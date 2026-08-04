package main

import (
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
