package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cpa-session-archive/internal/archive"
)

func TestProcessingHeartbeatsRemainInterimUntilFinalResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeProcessing(w)
		writeProcessing(w)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()

	connection, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err = connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = io.WriteString(connection, "GET / HTTP/1.1\r\nHost: collector\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}

	reader := bufio.NewReader(connection)
	request := &http.Request{Method: http.MethodGet}
	for _, want := range []int{http.StatusProcessing, http.StatusProcessing, http.StatusOK} {
		response, err := http.ReadResponse(reader, request)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != want {
			t.Fatalf("status=%d, want %d", response.StatusCode, want)
		}
		if want == http.StatusOK {
			body, readErr := io.ReadAll(response.Body)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if string(body) != "ok" {
				t.Fatalf("body=%q", body)
			}
		}
		if err = response.Body.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReadinessWaitsForRepairsAndTurnBackfills(t *testing.T) {
	store, err := archive.OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.DB.Close() })

	repairStarted := make(chan struct{})
	repairDone := make(chan struct{})
	projectionStarted := make(chan struct{})
	projectionDone := make(chan struct{})
	facetStarted := make(chan struct{})
	facetDone := make(chan struct{})
	var startupPhase atomic.Int32
	var orderViolation atomic.Bool
	releaseRepair := releaseGate(repairDone)
	releaseProjection := releaseGate(projectionDone)
	releaseFacet := releaseGate(facetDone)
	defer releaseRepair()
	defer releaseProjection()
	defer releaseFacet()
	startupReady := startStartupTasks(
		startupTask{name: "repair chain", run: func() error {
			if !startupPhase.CompareAndSwap(0, 1) {
				orderViolation.Store(true)
			}
			close(repairStarted)
			<-repairDone
			startupPhase.Store(2)
			return nil
		}},
		startupTask{name: "turn projection", run: func() error {
			if !startupPhase.CompareAndSwap(2, 3) {
				orderViolation.Store(true)
			}
			close(projectionStarted)
			<-projectionDone
			startupPhase.Store(4)
			return nil
		}},
		startupTask{name: "turn facet projection", run: func() error {
			if !startupPhase.CompareAndSwap(4, 5) {
				orderViolation.Store(true)
			}
			close(facetStarted)
			<-facetDone
			startupPhase.Store(6)
			return nil
		}},
	)
	server := &server{s: store, startupReady: startupReady}

	<-repairStarted
	assertProbeStatus(t, server.health, http.StatusOK)
	assertProbeStatus(t, server.ready, http.StatusServiceUnavailable)

	releaseRepair()
	<-projectionStarted
	if orderViolation.Load() {
		t.Fatal("turn projection entered before repair chain completed")
	}
	assertProbeStatus(t, server.ready, http.StatusServiceUnavailable)

	releaseProjection()
	<-facetStarted
	if orderViolation.Load() {
		t.Fatal("turn facet projection entered before turn projection completed")
	}
	assertProbeStatus(t, server.ready, http.StatusServiceUnavailable)

	releaseFacet()
	<-startupReady
	if orderViolation.Load() || startupPhase.Load() != 6 {
		t.Fatalf("startup tasks completed out of order: phase=%d violation=%v", startupPhase.Load(), orderViolation.Load())
	}
	assertProbeStatus(t, server.ready, http.StatusOK)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil).WithContext(cancelled)
	response := httptest.NewRecorder()
	server.ready(response, request)
	if response.Code != http.StatusServiceUnavailable || strings.TrimSpace(response.Body.String()) != "not ready" {
		t.Fatalf("cancelled request status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestFailedStartupUsesBoundedRetriesAndNeverBecomesReady(t *testing.T) {
	attempts := 0
	taskFinished := make(chan struct{})
	ready := startStartupTasks(startupTask{name: "failing repair", run: func() error {
		defer close(taskFinished)
		return retryStartupWithPolicy(context.Background(), "failing repair", 3, 0, func(context.Context) error {
			attempts++
			return errors.New("forced failure")
		})
	}})
	<-taskFinished
	if attempts != 3 {
		t.Fatalf("attempts=%d, want 3", attempts)
	}
	assertNotSignaled(t, ready, "failed startup became ready")
}

func TestHealthFailureIsGeneric(t *testing.T) {
	store, err := archive.OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.DB.Close(); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	(&server{s: store}).health(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if response.Code != http.StatusServiceUnavailable || strings.TrimSpace(response.Body.String()) != "unhealthy" {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestReadyFailureIsGenericWhenDatabaseIsUnavailable(t *testing.T) {
	store, err := archive.OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.DB.Close(); err != nil {
		t.Fatal(err)
	}
	startupReady := make(chan struct{})
	close(startupReady)
	response := httptest.NewRecorder()
	(&server{s: store, startupReady: startupReady}).ready(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable || strings.TrimSpace(response.Body.String()) != "not ready" {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestPreviewPayloadFailureBlocksReadinessWithoutCompletionMarker(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, *archive.Store, string)
	}{
		{name: "missing", mutate: func(t *testing.T, store *archive.Store, ref string) {
			if _, err := store.DB.Exec(`DELETE FROM blobs WHERE hash=?`, ref); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "corrupt", mutate: func(t *testing.T, store *archive.Store, ref string) {
			if _, err := store.DB.Exec(`UPDATE blobs SET data=? WHERE hash=?`, []byte("not-a-gzip-stream"), ref); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			store, err := archive.OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.DB.Close() })
			now := time.Now()
			requestID := test.name + "-payload"
			if err = store.PutBatch([]archive.Record{{
				RequestID: requestID, SessionID: "session", StartedAt: now, CompletedAt: now,
				OriginalRequest: []byte(`{"input":[{"role":"user","content":"must not be marked complete"}]}`),
			}}); err != nil {
				t.Fatal(err)
			}
			var originalRef string
			if err = store.DB.QueryRow(`SELECT original_ref FROM records WHERE request_id=?`, requestID).Scan(&originalRef); err != nil {
				t.Fatal(err)
			}
			test.mutate(t, store, originalRef)

			taskFinished := make(chan struct{})
			var repairErr error
			ready := startStartupTasks(startupTask{name: "request preview repair", run: func() error {
				defer close(taskFinished)
				repairErr = retryStartupWithPolicy(context.Background(), "request preview repair", 1, 0, store.RepairRecordPreviews)
				return repairErr
			}})
			<-taskFinished
			if repairErr == nil {
				t.Fatal("invalid payload was accepted")
			}
			assertNotSignaled(t, ready, "payload failure became ready")
			var requestMarkers, repairMarkers int
			if err = store.DB.QueryRow(`SELECT COUNT(*) FROM previewed_requests WHERE request_id=?`, requestID).Scan(&requestMarkers); err != nil {
				t.Fatal(err)
			}
			if err = store.DB.QueryRow(`SELECT COUNT(*) FROM repair_versions WHERE name='record_preview'`).Scan(&repairMarkers); err != nil {
				t.Fatal(err)
			}
			if requestMarkers != 0 || repairMarkers != 0 {
				t.Fatalf("request markers=%d repair markers=%d", requestMarkers, repairMarkers)
			}
		})
	}
}

func releaseGate(gate chan struct{}) func() {
	var once sync.Once
	return func() {
		once.Do(func() { close(gate) })
	}
}

func assertNotSignaled(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
		t.Fatal(message)
	default:
	}
}

func assertProbeStatus(t *testing.T, handler http.HandlerFunc, want int) {
	t.Helper()
	response := httptest.NewRecorder()
	handler(response, httptest.NewRequest(http.MethodGet, "/probe", nil))
	if response.Code != want {
		t.Fatalf("status=%d body=%q, want %d", response.Code, response.Body.String(), want)
	}
	if response.Code == http.StatusServiceUnavailable && strings.TrimSpace(response.Body.String()) != "not ready" {
		t.Fatalf("unexpected error detail: %q", response.Body.String())
	}
}

func TestExportTicketHeadIsReusableWithoutStreamingDatabase(t *testing.T) {
	server := &server{tickets: map[string]exportTicket{
		"ticket": {SessionID: "session", Scope: "session", Format: "archive", Filename: "session.jsonl", ExpiresAt: time.Now().Add(time.Minute)},
	}}
	for attempt := 0; attempt < 2; attempt++ {
		request := httptest.NewRequest(http.MethodHead, "/archive-api/v1/exports/ticket", nil)
		response := httptest.NewRecorder()
		server.ticketedExport(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("attempt %d status=%d", attempt+1, response.Code)
		}
		if !strings.Contains(response.Header().Get("Content-Disposition"), "session.jsonl") {
			t.Fatalf("disposition=%q", response.Header().Get("Content-Disposition"))
		}
		if response.Header().Get("X-Accel-Buffering") != "no" {
			t.Fatalf("buffering header=%q", response.Header().Get("X-Accel-Buffering"))
		}
		if response.Body.Len() != 0 {
			t.Fatalf("HEAD streamed %d bytes", response.Body.Len())
		}
	}
}

func TestExpiredExportTicketReturnsMachineReadableGone(t *testing.T) {
	server := &server{tickets: map[string]exportTicket{
		"expired": {SessionID: "session", Scope: "session", Format: "archive", Filename: "session.jsonl", ExpiresAt: time.Now().Add(-time.Second)},
	}}
	for attempt := 0; attempt < 2; attempt++ {
		response := httptest.NewRecorder()
		server.ticketedExport(response, httptest.NewRequest(http.MethodGet, "/archive-api/v1/exports/expired", nil))
		if response.Code != http.StatusGone || response.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("attempt=%d status=%d content-type=%q body=%s", attempt+1, response.Code, response.Header().Get("Content-Type"), response.Body.String())
		}
		if response.Header().Get("Content-Disposition") != "" {
			t.Fatalf("expired ticket returned attachment headers: %+v", response.Header())
		}
		var body map[string]string
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body["error"] != "export ticket expired" {
			t.Fatalf("body=%s err=%v", response.Body.String(), err)
		}
	}
}
