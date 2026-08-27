package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

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
