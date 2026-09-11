package main

import (
	"context"
	"encoding/json"
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
		func() {
			close(repairStarted)
			<-repairDone
		},
		func() {
			close(projectionStarted)
			<-projectionDone
			close(facetStarted)
			<-facetDone
		},
	)
	server := &server{s: store, startupReady: startupReady}

	<-repairStarted
	<-projectionStarted
	assertProbeStatus(t, server.health, http.StatusOK)
	assertProbeStatus(t, server.ready, http.StatusServiceUnavailable)

	releaseRepair()
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

func releaseGate(gate chan struct{}) func() {
	var once sync.Once
	return func() {
		once.Do(func() { close(gate) })
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
