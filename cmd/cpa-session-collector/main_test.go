package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cpa-session-archive/internal/archive"
)

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
	releaseRepair := releaseGate(repairDone)
	releaseProjection := releaseGate(projectionDone)
	releaseFacet := releaseGate(facetDone)
	defer releaseRepair()
	defer releaseProjection()
	defer releaseFacet()
	startupReady := startStartupTasks(
		startupTask{name: "repair chain", run: func() error {
			close(repairStarted)
			<-repairDone
			return nil
		}},
		startupTask{name: "turn projection", run: func() error {
			close(projectionStarted)
			<-projectionDone
			return nil
		}},
		startupTask{name: "turn facet projection", run: func() error {
			close(facetStarted)
			<-facetDone
			return nil
		}},
	)
	server := &server{s: store, startupReady: startupReady}

	<-repairStarted
	assertNotSignaled(t, projectionStarted, "turn projection started before repair chain completed")
	assertProbeStatus(t, server.health, http.StatusOK)
	assertProbeStatus(t, server.ready, http.StatusServiceUnavailable)

	releaseRepair()
	<-projectionStarted
	assertNotSignaled(t, facetStarted, "turn facet projection started before turn projection completed")
	assertProbeStatus(t, server.ready, http.StatusServiceUnavailable)

	releaseProjection()
	<-facetStarted
	assertProbeStatus(t, server.ready, http.StatusServiceUnavailable)

	releaseFacet()
	<-startupReady
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
