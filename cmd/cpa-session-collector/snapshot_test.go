package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cpa-session-archive/internal/archive"
)

type stablePageResponse struct {
	CursorProtocol   string                         `json:"cursor_protocol"`
	Snapshot         string                         `json:"snapshot"`
	IngestFence      string                         `json:"ingest_fence"`
	SessionCount     int                            `json:"session_count"`
	RequestCount     int                            `json:"request_count"`
	SessionSetSHA256 string                         `json:"session_set_sha256"`
	Sessions         []archive.StableSessionSummary `json:"sessions"`
	Complete         bool                           `json:"complete"`
	NextCursor       *string                        `json:"next_cursor"`
}

func refreshHTTPTestDigests(t *testing.T, store *archive.Store) {
	t.Helper()
	for {
		updated, err := store.RefreshSessionExportDigests(context.Background(), 64)
		if err != nil {
			t.Fatal(err)
		}
		if updated == 0 {
			return
		}
	}
}

func snapshotTestServer(t *testing.T, records []archive.Record, maxActive int) (*server, *stableSnapshotRegistry, func()) {
	t.Helper()
	store, err := archive.OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PutBatch(records); err != nil {
		store.DB.Close()
		t.Fatal(err)
	}
	refreshHTTPTestDigests(t, store)
	registry, err := newStableSnapshotRegistry(time.Minute, time.Minute, maxActive)
	if err != nil {
		store.DB.Close()
		t.Fatal(err)
	}
	server := &server{
		s:                store,
		tickets:          map[string]exportTicket{},
		snapshotRegistry: registry,
		allowOfflineFull: true,
	}
	server.snapshotOnce.Do(func() {})
	return server, registry, func() {
		registry.mu.Lock()
		for _, entry := range registry.snapshots {
			_ = entry.Snapshot.Close()
		}
		registry.snapshots = map[string]*stableSnapshotEntry{}
		registry.mu.Unlock()
		_ = store.DB.Close()
	}
}

func requestStablePage(t *testing.T, server *server, rawURL string) (*httptest.ResponseRecorder, stablePageResponse) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, rawURL, nil)
	response := httptest.NewRecorder()
	server.sessions(response, request)
	var payload stablePageResponse
	if response.Code == http.StatusOK {
		if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
			t.Fatal(err)
		}
	}
	return response, payload
}

func TestStableSessionsHTTPContractFailsClosedAndReplays(t *testing.T) {
	when := time.Date(2026, 8, 21, 1, 2, 3, 0, time.UTC)
	records := []archive.Record{}
	for index := 0; index < 3; index++ {
		records = append(records, archive.Record{
			RequestID:       fmt.Sprintf("request-%d", index),
			SessionID:       fmt.Sprintf("session-%d", index),
			KeyID:           "secret-key-value",
			StartedAt:       when,
			CompletedAt:     when,
			OriginalRequest: []byte(`{"path":"/private/archive.sqlite"}`),
			Outcome:         "succeeded",
		})
	}
	server, registry, closeServer := snapshotTestServer(t, records, 2)
	defer closeServer()
	now := time.Date(2026, 8, 21, 2, 0, 0, 0, time.UTC)
	registry.now = func() time.Time { return now }
	base := "/v1/sessions?cursor_protocol=" + url.QueryEscape(archive.StableCursorProtocol) +
		"&lower_bound_completed_at=" + url.QueryEscape("2030-01-01T00:00:00Z") + "&limit=2"
	firstResponse, first := requestStablePage(t, server, base)
	if firstResponse.Code != http.StatusOK {
		t.Fatalf("first status=%d body=%s", firstResponse.Code, firstResponse.Body.String())
	}
	if first.CursorProtocol != archive.StableCursorProtocol || first.SessionCount != 3 || first.RequestCount != 3 || first.NextCursor == nil || first.Complete {
		t.Fatalf("first page=%+v", first)
	}
	if len(first.SessionSetSHA256) != 64 || len(first.IngestFence) == 0 {
		t.Fatalf("metadata=%+v", first)
	}
	if strings.Contains(firstResponse.Body.String(), "secret-key-value") || strings.Contains(firstResponse.Body.String(), "/private/archive.sqlite") {
		t.Fatalf("stable projection leaked archive data: %s", firstResponse.Body.String())
	}
	if err := server.s.PutBatch([]archive.Record{
		{
			RequestID:   "request-0",
			SessionID:   "session-0",
			StartedAt:   when,
			CompletedAt: when,
			Response:    []byte(`{"changed":"after snapshot"}`),
			Outcome:     "succeeded",
		},
		{
			RequestID:   "request-new",
			SessionID:   "session-new",
			StartedAt:   when.Add(-time.Hour),
			CompletedAt: when.Add(-time.Hour),
			Outcome:     "succeeded",
		},
	}); err != nil {
		t.Fatal(err)
	}
	replayURL := base + "&snapshot=" + url.QueryEscape(first.Snapshot)
	replayResponse, replay := requestStablePage(t, server, replayURL)
	if replayResponse.Code != http.StatusOK || replayResponse.Body.String() != firstResponse.Body.String() {
		t.Fatalf("page replay changed\nfirst=%s\nreplay=%s", firstResponse.Body.String(), replayResponse.Body.String())
	}
	secondPageResponse, secondPage := requestStablePage(t, server, replayURL+"&cursor="+url.QueryEscape(*first.NextCursor))
	if secondPageResponse.Code != http.StatusOK || len(secondPage.Sessions) != 1 || !secondPage.Complete || secondPage.Sessions[0].SessionID != "session-2" {
		t.Fatalf("snapshot continuation changed: status=%d page=%+v", secondPageResponse.Code, secondPage)
	}
	if replay.NextCursor == nil || *replay.NextCursor != *first.NextCursor {
		t.Fatal("replayed next cursor changed")
	}
	tampered := *first.NextCursor + "A"
	response, _ := requestStablePage(t, server, replayURL+"&cursor="+url.QueryEscape(tampered))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("tampered cursor status=%d", response.Code)
	}
	response, _ = requestStablePage(t, server, strings.Replace(replayURL, "limit=2", "limit=1", 1))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("rewritten limit status=%d", response.Code)
	}

	refreshHTTPTestDigests(t, server.s)
	secondResponse, second := requestStablePage(t, server, base)
	if secondResponse.Code != http.StatusOK || second.Snapshot == first.Snapshot {
		t.Fatalf("second snapshot status=%d snapshot=%q", secondResponse.Code, second.Snapshot)
	}
	crossURL := base + "&snapshot=" + url.QueryEscape(second.Snapshot) + "&cursor=" + url.QueryEscape(*first.NextCursor)
	response, _ = requestStablePage(t, server, crossURL)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("cross-snapshot cursor status=%d", response.Code)
	}
	response, _ = requestStablePage(t, server, base)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("snapshot capacity status=%d", response.Code)
	}

	now = now.Add(2 * time.Minute)
	response, _ = requestStablePage(t, server, replayURL)
	if response.Code != http.StatusGone {
		t.Fatalf("expired snapshot status=%d", response.Code)
	}
	if _, err := registry.resolveCursor(&stableSnapshotEntry{ID: "missing"}, *first.NextCursor); err == nil {
		t.Fatal("expired cursor unexpectedly resolved")
	}
}

func TestStableSnapshotTicketExportsExactSnapshot(t *testing.T) {
	when := time.Date(2026, 8, 21, 1, 2, 3, 0, time.UTC)
	server, _, closeServer := snapshotTestServer(t, []archive.Record{{
		RequestID:       "request-old",
		SessionID:       "session",
		StartedAt:       when,
		CompletedAt:     when,
		OriginalRequest: []byte(`{"input":"old"}`),
		Response:        []byte(`{"output":"old"}`),
		Outcome:         "succeeded",
	}}, 2)
	defer closeServer()
	base := "/v1/sessions?cursor_protocol=" + url.QueryEscape(archive.StableCursorProtocol) +
		"&lower_bound_completed_at=" + url.QueryEscape("2030-01-01T00:00:00Z") + "&limit=10"
	response, page := requestStablePage(t, server, base)
	if response.Code != http.StatusOK || len(page.Sessions) != 1 {
		t.Fatalf("snapshot status=%d page=%+v", response.Code, page)
	}
	ticketRequest := httptest.NewRequest(http.MethodGet,
		"/v1/export-tickets?session_id=session&scope=session&format=archive&snapshot="+url.QueryEscape(page.Snapshot), nil)
	ticketResponse := httptest.NewRecorder()
	server.exportTicket(ticketResponse, ticketRequest)
	if ticketResponse.Code != http.StatusOK {
		t.Fatalf("ticket status=%d body=%s", ticketResponse.Code, ticketResponse.Body.String())
	}
	var ticket struct {
		URL            string `json:"url"`
		CursorProtocol string `json:"cursor_protocol"`
		Snapshot       string `json:"snapshot"`
		RecordsSHA256  string `json:"records_sha256"`
	}
	if err := json.Unmarshal(ticketResponse.Body.Bytes(), &ticket); err != nil {
		t.Fatal(err)
	}
	if ticket.CursorProtocol != archive.StableCursorProtocol || ticket.Snapshot != page.Snapshot || ticket.RecordsSHA256 != page.Sessions[0].RecordsSHA256 {
		t.Fatalf("ticket is not snapshot-bound: %+v", ticket)
	}
	if err := server.s.PutBatch([]archive.Record{
		{
			RequestID:   "request-old",
			SessionID:   "session",
			StartedAt:   when,
			CompletedAt: when,
			Response:    []byte(`{"output":"changed"}`),
			Outcome:     "succeeded",
		},
		{
			RequestID:   "request-new",
			SessionID:   "session",
			StartedAt:   when.Add(time.Second),
			CompletedAt: when.Add(time.Second),
			Outcome:     "succeeded",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.s.DB.Exec(`DELETE FROM records WHERE request_id='request-old'`); err != nil {
		t.Fatal(err)
	}
	downloadRequest := httptest.NewRequest(http.MethodGet, ticket.URL, nil)
	downloadResponse := httptest.NewRecorder()
	server.ticketedExport(downloadResponse, downloadRequest)
	if downloadResponse.Code != http.StatusOK {
		t.Fatalf("download status=%d body=%s", downloadResponse.Code, downloadResponse.Body.String())
	}
	if lines := strings.Count(downloadResponse.Body.String(), "\n"); lines != 1 {
		t.Fatalf("snapshot download included later records: lines=%d body=%s", lines, downloadResponse.Body.String())
	}
	sum := sha256.Sum256(downloadResponse.Body.Bytes())
	if hex.EncodeToString(sum[:]) != ticket.RecordsSHA256 {
		t.Fatalf("download digest=%s expected=%s", hex.EncodeToString(sum[:]), ticket.RecordsSHA256)
	}
}

func TestLegacySessionsResponseRemainsAList(t *testing.T) {
	when := time.Date(2026, 8, 21, 1, 2, 3, 0, time.UTC)
	server, _, closeServer := snapshotTestServer(t, []archive.Record{{
		RequestID: "request", SessionID: "session", StartedAt: when, CompletedAt: when,
	}}, 1)
	defer closeServer()
	request := httptest.NewRequest(http.MethodGet, "/v1/sessions?limit=1", nil)
	response := httptest.NewRecorder()
	server.sessions(response, request)
	if response.Code != http.StatusOK || !strings.HasPrefix(strings.TrimSpace(response.Body.String()), "[") {
		t.Fatalf("legacy response changed: status=%d body=%s", response.Code, response.Body.String())
	}
}
