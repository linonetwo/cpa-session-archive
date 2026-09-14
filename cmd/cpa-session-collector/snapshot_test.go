package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cpa-session-archive/internal/archive"
)

func TestWaitStableSnapshotExportSendsHeartbeatsUntilMaterialized(t *testing.T) {
	done := make(chan error, 1)
	release := make(chan struct{}, 1)
	var heartbeats atomic.Int32
	go func() {
		<-release
		done <- nil
	}()
	if err := waitStableSnapshotExport(done, time.Millisecond, func() {
		heartbeats.Add(1)
		select {
		case release <- struct{}{}:
		default:
		}
	}); err != nil {
		t.Fatal(err)
	}
	if heartbeats.Load() == 0 {
		t.Fatal("materializing stable export sent no informational heartbeat")
	}
}

func TestStableExportErrorClassDoesNotExposeErrorDetail(t *testing.T) {
	if got := stableExportErrorClass(context.DeadlineExceeded); got != "deadline" {
		t.Fatalf("deadline class=%q", got)
	}
	if got := stableExportErrorClass(errors.New("sensitive error detail")); got != "internal" {
		t.Fatalf("unknown class=%q", got)
	}
}

type stablePageResponse struct {
	SnapshotSchemaVersion         int                            `json:"snapshot_schema_version"`
	CursorProtocol                string                         `json:"cursor_protocol"`
	Snapshot                      string                         `json:"snapshot"`
	IngestFence                   string                         `json:"ingest_fence"`
	TombstoneSafeAfterIngestFence string                         `json:"tombstone_safe_after_ingest_fence"`
	SessionCount                  int                            `json:"session_count"`
	RequestCount                  int                            `json:"request_count"`
	DeletedSessionCount           int                            `json:"deleted_session_count"`
	SessionSetSHA256              string                         `json:"session_set_sha256"`
	Sessions                      []archive.StableSessionSummary `json:"sessions"`
	Complete                      bool                           `json:"complete"`
	NextCursor                    *string                        `json:"next_cursor"`
}

type blockedExportWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (writer *blockedExportWriter) Write(payload []byte) (int, error) {
	writer.once.Do(func() {
		close(writer.started)
		<-writer.release
	})
	return len(payload), nil
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
	if first.SnapshotSchemaVersion != archive.StableSnapshotSchemaVersion || first.CursorProtocol != archive.StableCursorProtocol || first.SessionCount != 3 || first.RequestCount != 3 || first.DeletedSessionCount != 0 || first.NextCursor == nil || first.Complete {
		t.Fatalf("first page=%+v", first)
	}
	if len(first.SessionSetSHA256) != 64 || len(first.IngestFence) == 0 || first.TombstoneSafeAfterIngestFence != "0" {
		t.Fatalf("metadata=%+v", first)
	}
	if strings.Contains(firstResponse.Body.String(), "secret-key-value") || strings.Contains(firstResponse.Body.String(), "/private/archive.sqlite") {
		t.Fatalf("stable projection leaked archive data: %s", firstResponse.Body.String())
	}
	registry.mu.Lock()
	expiringSnapshot := registry.snapshots[first.Snapshot].Snapshot
	registry.mu.Unlock()
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
	response, _ = requestStablePage(t, server, strings.Replace(replayURL, "2030-01-01T00%3A00%3A00Z", "2031-01-01T00%3A00%3A00Z", 1))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("rewritten lower bound status=%d", response.Code)
	}
	response, _ = requestStablePage(t, server, replayURL+"&unknown_stable_option=true")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unknown stable query status=%d", response.Code)
	}
	response, _ = requestStablePage(t, server, base+"&snapshot=unknown")
	if response.Code != http.StatusGone {
		t.Fatalf("unknown snapshot status=%d", response.Code)
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
	if _, _, err := expiringSnapshot.Page("", "", 1); !errors.Is(err, archive.ErrSnapshotCursor) {
		t.Fatalf("expired registry entry did not rollback its transaction: %v", err)
	}
	if _, err := registry.resolveCursor(&stableSnapshotEntry{ID: "missing"}, *first.NextCursor); err == nil {
		t.Fatal("expired cursor unexpectedly resolved")
	}
}

func TestStableSessionsRejectWrongFenceAndSnapshotBinding(t *testing.T) {
	when := time.Date(2026, 8, 21, 1, 2, 3, 0, time.UTC)
	server, _, closeServer := snapshotTestServer(t, []archive.Record{{
		RequestID: "request", SessionID: "session", StartedAt: when, CompletedAt: when,
	}}, 2)
	defer closeServer()
	base := "/v1/sessions?cursor_protocol=" + url.QueryEscape(archive.StableCursorProtocol) +
		"&lower_bound_completed_at=" + url.QueryEscape("2030-01-01T00:00:00Z") + "&after_ingest_fence=0&limit=10"
	response, page := requestStablePage(t, server, base)
	if response.Code != http.StatusOK {
		t.Fatalf("initial status=%d body=%s", response.Code, response.Body.String())
	}
	response, _ = requestStablePage(t, server, strings.Replace(base, "after_ingest_fence=0", "after_ingest_fence=1", 1)+"&snapshot="+url.QueryEscape(page.Snapshot))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("rewritten prior fence status=%d", response.Code)
	}
	response, _ = requestStablePage(t, server, strings.Replace(base, "after_ingest_fence=0", "after_ingest_fence=999999", 1))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("future prior fence status=%d", response.Code)
	}
	if _, err := server.s.DB.Exec(`UPDATE archive_snapshot_contract SET tombstone_safe_after_sequence=1 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	response, _ = requestStablePage(t, server, base)
	if response.Code != http.StatusConflict {
		t.Fatalf("pre-upgrade prior fence status=%d body=%s", response.Code, response.Body.String())
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

func TestStableSnapshotMetadataTouchDoesNotWaitForActiveExport(t *testing.T) {
	when := time.Date(2026, 8, 21, 1, 2, 3, 0, time.UTC)
	server, registry, closeServer := snapshotTestServer(t, []archive.Record{{
		RequestID: "request", SessionID: "session", StartedAt: when, CompletedAt: when,
		OriginalRequest: []byte(`{"input":"old"}`), Outcome: "succeeded",
	}}, 1)
	defer closeServer()
	var clock atomic.Int64
	clock.Store(time.Now().UTC().UnixNano())
	registry.now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	registry.ttl = 6 * time.Hour
	registry.idleTTL = time.Minute
	base := "/v1/sessions?cursor_protocol=" + url.QueryEscape(archive.StableCursorProtocol) +
		"&lower_bound_completed_at=" + url.QueryEscape("2030-01-01T00:00:00Z") + "&limit=10"
	response, page := requestStablePage(t, server, base)
	if response.Code != http.StatusOK || len(page.Sessions) != 1 {
		t.Fatalf("snapshot status=%d page=%+v", response.Code, page)
	}

	writer := &blockedExportWriter{started: make(chan struct{}), release: make(chan struct{})}
	released := false
	defer func() {
		if !released {
			close(writer.release)
		}
	}()
	exportDone := make(chan error, 1)
	go func() {
		exportDone <- registry.export(context.Background(), page.Snapshot, "session", page.Sessions[0].RecordsSHA256, writer)
	}()
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("stable export did not reach the deterministic writer barrier")
	}
	clock.Add(int64(2 * time.Minute))
	registry.cleanup()
	registry.mu.Lock()
	_, retained := registry.snapshots[page.Snapshot]
	registry.mu.Unlock()
	if !retained {
		t.Fatal("active export lost its registry pin at the idle lease boundary")
	}

	replayDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		replay := httptest.NewRecorder()
		server.sessions(replay, httptest.NewRequest(http.MethodGet, base+"&snapshot="+url.QueryEscape(page.Snapshot), nil))
		replayDone <- replay
	}()
	select {
	case replay := <-replayDone:
		if replay.Code != http.StatusOK {
			t.Fatalf("metadata touch status=%d body=%s", replay.Code, replay.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("metadata touch waited for the active archive materialization")
	}

	close(writer.release)
	released = true
	select {
	case err := <-exportDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("stable export did not finish after releasing the writer barrier")
	}
}

func TestExpiredStableSnapshotDownloadReturnsJSONBeforeAttachmentHeaders(t *testing.T) {
	when := time.Date(2026, 8, 21, 1, 2, 3, 0, time.UTC)
	server, registry, closeServer := snapshotTestServer(t, []archive.Record{{
		RequestID: "request", SessionID: "session", StartedAt: when, CompletedAt: when,
	}}, 1)
	defer closeServer()
	now := time.Now().UTC()
	registry.now = func() time.Time { return now }
	base := "/v1/sessions?cursor_protocol=" + url.QueryEscape(archive.StableCursorProtocol) +
		"&lower_bound_completed_at=" + url.QueryEscape("2030-01-01T00:00:00Z") + "&limit=10"
	response, page := requestStablePage(t, server, base)
	if response.Code != http.StatusOK {
		t.Fatalf("snapshot status=%d body=%s", response.Code, response.Body.String())
	}
	ticketRequest := httptest.NewRequest(http.MethodGet,
		"/v1/export-tickets?session_id=session&scope=session&format=archive&snapshot="+url.QueryEscape(page.Snapshot), nil)
	ticketResponse := httptest.NewRecorder()
	server.exportTicket(ticketResponse, ticketRequest)
	if ticketResponse.Code != http.StatusOK {
		t.Fatalf("ticket status=%d body=%s", ticketResponse.Code, ticketResponse.Body.String())
	}
	var ticket struct {
		URL                   string `json:"url"`
		SnapshotSchemaVersion int    `json:"snapshot_schema_version"`
	}
	if err := json.Unmarshal(ticketResponse.Body.Bytes(), &ticket); err != nil {
		t.Fatal(err)
	}
	if ticket.SnapshotSchemaVersion != archive.StableSnapshotSchemaVersion {
		t.Fatalf("ticket schema version=%d", ticket.SnapshotSchemaVersion)
	}
	now = now.Add(2 * time.Minute)
	downloadResponse := httptest.NewRecorder()
	server.ticketedExport(downloadResponse, httptest.NewRequest(http.MethodGet, ticket.URL, nil))
	if downloadResponse.Code != http.StatusGone || downloadResponse.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("expired download status=%d content-type=%q body=%s", downloadResponse.Code, downloadResponse.Header().Get("Content-Type"), downloadResponse.Body.String())
	}
	if downloadResponse.Header().Get("Content-Disposition") != "" || downloadResponse.Header().Get("X-Accel-Buffering") != "" {
		t.Fatalf("attachment headers committed before expiry check: %+v", downloadResponse.Header())
	}
	var failure map[string]string
	if err := json.Unmarshal(downloadResponse.Body.Bytes(), &failure); err != nil || failure["error"] != "stable session snapshot expired" {
		t.Fatalf("expired body=%s err=%v", downloadResponse.Body.String(), err)
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

func TestStatsAdvertisesStableSnapshotSchema(t *testing.T) {
	server, _, closeServer := snapshotTestServer(t, nil, 1)
	defer closeServer()
	response := httptest.NewRecorder()
	server.stats(response, httptest.NewRequest(http.MethodGet, "/v1/stats", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("stats status=%d body=%s", response.Code, response.Body.String())
	}
	var payload struct {
		SnapshotSchemaVersion         int      `json:"snapshot_schema_version"`
		TombstoneSafeAfterIngestFence string   `json:"tombstone_safe_after_ingest_fence"`
		SessionCursorProtocols        []string `json:"session_cursor_protocols"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.SnapshotSchemaVersion != archive.StableSnapshotSchemaVersion || payload.TombstoneSafeAfterIngestFence != "0" || len(payload.SessionCursorProtocols) != 1 || payload.SessionCursorProtocols[0] != archive.StableCursorProtocol {
		t.Fatalf("stats discovery=%+v", payload)
	}
}

func TestFullSnapshotRequiresOfflineOptIn(t *testing.T) {
	when := time.Date(2026, 8, 21, 1, 2, 3, 0, time.UTC)
	server, _, closeServer := snapshotTestServer(t, []archive.Record{{
		RequestID: "request", SessionID: "session", StartedAt: when, CompletedAt: when,
	}}, 1)
	defer closeServer()
	server.allowOfflineFull = false
	request := httptest.NewRequest(http.MethodGet,
		"/v1/sessions?cursor_protocol="+url.QueryEscape(archive.StableCursorProtocol)+
			"&lower_bound_completed_at="+url.QueryEscape("2026-08-21T00:00:00Z")+"&limit=100", nil)
	response := httptest.NewRecorder()
	server.sessions(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("live full snapshot status=%d body=%s", response.Code, response.Body.String())
	}
}
