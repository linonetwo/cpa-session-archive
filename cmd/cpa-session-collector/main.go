package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"cpa-session-archive/internal/archive"
)

type server struct {
	s                   *archive.Store
	q                   chan archive.Record
	startupReady        <-chan struct{}
	ticketMu            sync.Mutex
	tickets             map[string]exportTicket
	turnTextMu          sync.Mutex
	turnTextJobs        map[string]bool
	snapshotOnce        sync.Once
	snapshotRegistry    *stableSnapshotRegistry
	snapshotRegistryErr error
	allowOfflineFull    bool
}

type exportTicket struct {
	SessionID      string
	Scope          string
	Format         string
	Filename       string
	ExpiresAt      time.Time
	Snapshot       string
	RecordsSHA256  string
	CursorProtocol string
}

type startupTask struct {
	name string
	run  func() error
}

const (
	startupMaxAttempts = 3
	startupRetryDelay  = 5 * time.Second
)

func main() {
	dbPath := env("ARCHIVE_DB", "/data/archive.sqlite")
	storeUpstream := env("STORE_UPSTREAM_REQUEST", "false") == "true"
	st, e := archive.OpenStore(dbPath, storeUpstream)
	if e != nil {
		log.Fatal(e)
	}
	startupContext := context.Background()
	repairChain := func() error {
		if env("ARCHIVE_MIGRATE_LEGACY", "false") == "true" {
			if err := retryStartup(startupContext, "legacy archive migration", func(ctx context.Context) error {
				return st.MigrateLegacy(ctx)
			}); err != nil {
				return err
			}
		}
		if env("ARCHIVE_REPAIR_CANONICAL_SESSIONS", "true") == "true" {
			var changed int
			if err := retryStartup(startupContext, "canonical session repair", func(ctx context.Context) (err error) {
				changed, err = st.RepairCanonicalSessions(ctx)
				return err
			}); err != nil {
				return err
			}
			if changed > 0 {
				log.Printf("canonical session repair merged %d request records", changed)
			}
		}
		if env("ARCHIVE_BACKFILL_SESSION_INDEX", "true") == "true" {
			if err := retryStartup(startupContext, "session index backfill", st.BackfillSessionIndex); err != nil {
				return err
			}
		}
		if env("ARCHIVE_REPAIR_SESSION_SUMMARIES", "true") == "true" {
			if err := retryStartup(startupContext, "session summary repair", st.RepairSessionSummaries); err != nil {
				return err
			}
		}
		if env("ARCHIVE_REPAIR_RECORD_PREVIEWS", "true") == "true" {
			if err := retryStartup(startupContext, "request preview repair", st.RepairRecordPreviews); err != nil {
				return err
			}
		}
		if env("ARCHIVE_NORMALIZE_SSE", "true") == "true" {
			if err := retryStartup(startupContext, "historical SSE normalization", st.NormalizeHistoricalSSE); err != nil {
				return err
			}
		}
		return nil
	}
	var turnProjection, turnFacets func() error
	if env("ARCHIVE_BACKFILL_TURN_PROJECTION", "true") == "true" {
		turnProjection = func() error {
			if err := retryStartup(startupContext, "turn projection backfill", func(ctx context.Context) error {
				return st.BackfillTurnProjection(ctx, 64, 25*time.Millisecond)
			}); err != nil {
				return err
			}
			log.Printf("turn projection backfill complete")
			return nil
		}
		turnFacets = func() error {
			if err := retryStartup(startupContext, "turn facet projection backfill", func(ctx context.Context) error {
				return st.BackfillTurnFacetProjection(ctx, 64, 10*time.Millisecond)
			}); err != nil {
				return err
			}
			log.Printf("turn facet projection backfill complete")
			return nil
		}
	}
	s := &server{s: st, q: make(chan archive.Record, 4096), startupReady: startStartupTasks(
		startupTask{name: "repair chain", run: repairChain},
		startupTask{name: "turn projection", run: turnProjection},
		startupTask{name: "turn facet projection", run: turnFacets},
	), tickets: map[string]exportTicket{}, turnTextJobs: map[string]bool{}}
	go s.writer()
	go s.digestRefresher()
	http.HandleFunc("/healthz", s.health)
	http.HandleFunc("/readyz", s.ready)
	http.HandleFunc("/ingest", s.ingest)
	http.HandleFunc("/v1/stats", s.stats)
	http.HandleFunc("/v1/facets", s.facets)
	http.HandleFunc("/v1/identity-mappings", s.identityMappings)
	http.HandleFunc("/v1/sessions", s.sessions)
	http.HandleFunc("/v1/sessions/", s.session)
	http.HandleFunc("/v1/requests/", s.request)
	http.HandleFunc("/v1/request-context", s.requestContext)
	http.HandleFunc("/v1/request-view", s.requestView)
	http.HandleFunc("/v1/turns", s.turns)
	http.HandleFunc("/v1/turn-text", s.turnText)
	http.HandleFunc("/v1/export-tickets", s.exportTicket)
	http.HandleFunc("/archive-api/v1/exports/", s.ticketedExport)
	http.HandleFunc("/v1/maintenance/gc", s.gc)
	addr := env("LISTEN_ADDR", ":8080")
	log.Printf("archive collector v%s listening on %s, db=%s, store_upstream=%v", archive.Version, addr, dbPath, storeUpstream)
	log.Fatal(http.ListenAndServe(addr, nil))
}

func startStartupTasks(tasks ...startupTask) <-chan struct{} {
	ready := make(chan struct{})
	go func() {
		for _, task := range tasks {
			if task.run == nil {
				continue
			}
			if err := task.run(); err != nil {
				log.Printf("collector startup stopped at %s; readiness remains false: %v", task.name, err)
				return
			}
		}
		close(ready)
	}()
	return ready
}

func retryStartup(ctx context.Context, name string, run func(context.Context) error) error {
	return retryStartupWithPolicy(ctx, name, startupMaxAttempts, startupRetryDelay, run)
}

func retryStartupWithPolicy(ctx context.Context, name string, attempts int, delay time.Duration, run func(context.Context) error) error {
	if attempts < 1 {
		attempts = 1
	}
	var err error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = run(ctx); err == nil {
			return nil
		}
		if attempt == attempts {
			break
		}
		log.Printf("%s attempt %d/%d failed; will retry: %v", name, attempt, attempts, err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func (s *server) identityMappings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		items, err := s.s.CredentialPrincipals(r.Context())
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, map[string]any{"mappings": items})
	case http.MethodPut, http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, 4<<20)
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		items, err := archive.DecodeCredentialPrincipals(raw)
		if err != nil || len(items) == 0 {
			http.Error(w, "invalid or empty mappings", 400)
			return
		}
		if err = s.s.ApplyCredentialPrincipals(r.Context(), items); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		writeJSON(w, map[string]any{"updated": len(items)})
	default:
		http.Error(w, "method", 405)
	}
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func (s *server) health(w http.ResponseWriter, r *http.Request) {
	if e := s.s.DB.PingContext(r.Context()); e != nil {
		http.Error(w, "unhealthy", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ok"))
}

func (s *server) ready(w http.ResponseWriter, r *http.Request) {
	select {
	case <-s.startupReady:
	default:
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	if _, err := s.s.Stats(r.Context()); err != nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ok"))
}
func (s *server) ingest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", 405)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 192<<20)
	var rec archive.Record
	if e := json.NewDecoder(r.Body).Decode(&rec); e != nil || rec.RequestID == "" {
		http.Error(w, "invalid record", 400)
		return
	}
	if rec.SessionID == "" {
		rec.SessionID = "request:" + rec.RequestID
	}
	select {
	case s.q <- rec:
		w.WriteHeader(202)
	default:
		http.Error(w, "queue full", 503)
	}
}
func (s *server) writer() {
	batch := make([]archive.Record, 0, 64)
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case r := <-s.q:
			batch = append(batch, r)
			if len(batch) >= 64 {
				s.flush(batch)
				batch = batch[:0]
			}
		case <-tick.C:
			if len(batch) > 0 {
				s.flush(batch)
				batch = batch[:0]
			}
		}
	}
}
func (s *server) flush(batch []archive.Record) {
	for attempt := 1; ; attempt++ {
		e := s.s.PutBatch(batch)
		if e == nil {
			return
		}
		message := strings.ToLower(e.Error())
		if !strings.Contains(message, "database is locked") && !strings.Contains(message, "database is busy") {
			log.Printf("archive batch dropped after non-retryable error: %v", e)
			return
		}
		if attempt == 1 || attempt%10 == 0 {
			log.Printf("archive batch waiting for SQLite lock: attempt=%d records=%d", attempt, len(batch))
		}
		time.Sleep(250 * time.Millisecond)
	}
}
func (s *server) sessions(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Has("cursor_protocol") {
		s.stableSessions(w, r)
		return
	}
	limit := 100
	if v, e := strconv.Atoi(r.URL.Query().Get("limit")); e == nil && v > 0 && v <= 1000 {
		limit = v
	}
	filters := map[string]string{}
	for name, values := range r.URL.Query() {
		if name == "limit" || len(values) == 0 {
			continue
		}
		filters[name] = values[0]
	}
	out, e := s.s.SessionsFiltered(r.Context(), limit, filters)
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	writeJSON(w, out)
}
func (s *server) session(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/sessions/")
	if strings.HasSuffix(id, "/export") {
		id = strings.TrimSuffix(id, "/export")
		if id == "" {
			http.Error(w, "session required", 400)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
		w.Header().Set("Content-Disposition", `attachment; filename="session-`+safeFilename(id)+`.jsonl"`)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if e := s.s.ExportSessionJSONL(r.Context(), id, w); e != nil {
			log.Printf("session export failed id=%s: %v", id, e)
		}
		return
	}
	if id == "" {
		http.Error(w, "session required", 400)
		return
	}
	if r.URL.Query().Has("limit") || r.URL.Query().Has("offset") {
		limit, offset := 20, 0
		maxLimit := 100
		metadataOnly := r.URL.Query().Get("metadata_only") == "true"
		if metadataOnly {
			maxLimit = 1000
		}
		if v, e := strconv.Atoi(r.URL.Query().Get("limit")); e == nil && v > 0 && v <= maxLimit {
			limit = v
		}
		if v, e := strconv.Atoi(r.URL.Query().Get("offset")); e == nil && v >= 0 {
			offset = v
		}
		if metadataOnly {
			filters := map[string]string{}
			order := r.URL.Query().Get("order")
			for name, values := range r.URL.Query() {
				if name == "limit" || name == "offset" || name == "metadata_only" || name == "preview_bytes" || name == "order" || len(values) == 0 {
					continue
				}
				filters[name] = values[0]
			}
			out, e := s.s.SessionMetadataRange(r.Context(), id, limit, offset, order, filters)
			if e != nil {
				http.Error(w, e.Error(), 500)
				return
			}
			writeJSON(w, out)
			return
		}
		preview := 65536
		if v, e := strconv.Atoi(r.URL.Query().Get("preview_bytes")); e == nil && v >= 1024 && v <= 1048576 {
			preview = v
		}
		out, e := s.s.SessionRange(r.Context(), id, limit, offset, preview)
		if e != nil {
			http.Error(w, e.Error(), 500)
			return
		}
		writeJSON(w, out)
		return
	}
	out, e := s.s.Session(r.Context(), id)
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	writeJSON(w, out)
}
func (s *server) request(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/requests/")
	if id == "" {
		http.Error(w, "request required", 400)
		return
	}
	out, e := s.s.Request(r.Context(), id)
	if e != nil {
		if strings.Contains(strings.ToLower(e.Error()), "no rows") {
			http.Error(w, "not found", 404)
		} else {
			http.Error(w, e.Error(), 500)
		}
		return
	}
	writeJSON(w, out)
}
func (s *server) requestContext(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id == "" {
		http.Error(w, "request required", 400)
		return
	}
	limit := 16
	before := 0
	if v, e := strconv.Atoi(r.URL.Query().Get("limit")); e == nil && v > 0 && v <= 32 {
		limit = v
	}
	if v, e := strconv.Atoi(r.URL.Query().Get("before")); e == nil && v >= 0 && v <= 4 {
		before = v
	}
	out, e := s.s.RequestContext(r.Context(), id, before, limit)
	if e != nil {
		if strings.Contains(strings.ToLower(e.Error()), "no rows") {
			http.Error(w, "not found", 404)
		} else {
			http.Error(w, e.Error(), 500)
		}
		return
	}
	writeJSON(w, out)
}
func (s *server) requestView(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id == "" {
		http.Error(w, "request required", 400)
		return
	}
	out, e := s.s.RequestTimeline(r.Context(), id)
	if e != nil {
		if strings.Contains(strings.ToLower(e.Error()), "no rows") {
			http.Error(w, "not found", 404)
		} else {
			http.Error(w, e.Error(), 500)
		}
		return
	}
	writeJSON(w, out)
}
func (s *server) turns(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	if sessionID == "" {
		http.Error(w, "session required", 400)
		return
	}
	limit, offset := 20, 0
	if value, parseErr := strconv.Atoi(r.URL.Query().Get("limit")); parseErr == nil && value > 0 && value <= 100 {
		limit = value
	}
	if value, parseErr := strconv.Atoi(r.URL.Query().Get("offset")); parseErr == nil && value >= 0 {
		offset = value
	}
	turnID := strings.TrimSpace(r.URL.Query().Get("turn_id"))
	if turnID != "" {
		out, err := s.s.SessionTurnDetail(r.Context(), sessionID, turnID, limit, offset)
		if err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "not found") {
				http.Error(w, "not found", 404)
			} else {
				http.Error(w, err.Error(), 500)
			}
			return
		}
		writeJSON(w, out)
		return
	}
	out, err := s.s.SessionTurnPage(r.Context(), sessionID, limit, offset, r.URL.Query().Get("order"))
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, out)
}
func (s *server) turnText(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	turnID := strings.TrimSpace(r.URL.Query().Get("turn_id"))
	if sessionID == "" || turnID == "" {
		http.Error(w, "session and turn required", http.StatusBadRequest)
		return
	}
	if text, found, err := s.s.CachedTurnText(r.Context(), sessionID, turnID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	} else if found {
		writeJSON(w, map[string]any{"status": "ok", "text": text})
		return
	}
	jobID := sessionID + "\x00" + turnID
	s.turnTextMu.Lock()
	running := s.turnTextJobs[jobID]
	if !running {
		s.turnTextJobs[jobID] = true
	}
	s.turnTextMu.Unlock()
	if !running {
		go func() {
			defer func() {
				s.turnTextMu.Lock()
				delete(s.turnTextJobs, jobID)
				s.turnTextMu.Unlock()
			}()
			text, err := s.s.BuildTurnText(context.Background(), sessionID, turnID)
			if err != nil {
				log.Printf("turn text build failed session=%s turn=%s: %v", sessionID, turnID, err)
				return
			}
			if err = s.s.SaveTurnText(context.Background(), sessionID, turnID, text); err != nil {
				log.Printf("turn text save failed session=%s turn=%s: %v", sessionID, turnID, err)
			}
		}()
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	writeJSON(w, map[string]any{"status": "building"})
}
func (s *server) exportTicket(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", 405)
		return
	}
	sessionID := strings.TrimSpace(r.URL.Query().Get("session_id"))
	scope := strings.TrimSpace(r.URL.Query().Get("scope"))
	format := strings.TrimSpace(r.URL.Query().Get("format"))
	if scope == "" {
		scope = "session"
	}
	if format == "" {
		format = "archive"
	}
	if scope != "session" && scope != "all" {
		http.Error(w, "invalid scope", 400)
		return
	}
	if format != "archive" && format != "sft" {
		http.Error(w, "invalid format", 400)
		return
	}
	if scope == "session" && sessionID == "" {
		http.Error(w, "session required", 400)
		return
	}
	snapshotID := strings.TrimSpace(r.URL.Query().Get("snapshot"))
	recordsSHA256 := ""
	cursorProtocol := ""
	var stableEntry *stableSnapshotEntry
	if snapshotID != "" {
		if scope != "session" || format != "archive" || len(snapshotID) > 128 {
			stableError(w, http.StatusBadRequest, "invalid stable snapshot export")
			return
		}
		registry, err := s.stableRegistry()
		if err != nil {
			stableError(w, http.StatusServiceUnavailable, "stable session snapshots are unavailable")
			return
		}
		stableEntry, err = registry.getForExport(snapshotID)
		if err != nil {
			stableError(w, http.StatusGone, "stable session snapshot expired")
			return
		}
		summary, found := stableEntry.Snapshot.Summary(sessionID)
		if !found || summary.Deleted {
			stableError(w, http.StatusNotFound, "session not found in snapshot")
			return
		}
		recordsSHA256 = summary.RecordsSHA256
		cursorProtocol = archive.StableCursorProtocol
	}
	if scope == "session" {
		var exists int
		if snapshotID == "" {
			if err := s.s.DB.QueryRowContext(r.Context(), `SELECT 1 FROM records WHERE session_id=? LIMIT 1`, sessionID).Scan(&exists); err != nil {
				http.Error(w, "session not found", 404)
				return
			}
		} else if stableEntry == nil {
			http.Error(w, "session not found", 404)
			return
		}
	}
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		http.Error(w, "ticket unavailable", 500)
		return
	}
	token := hex.EncodeToString(random)
	s.ticketMu.Lock()
	now := time.Now()
	for key, item := range s.tickets {
		// Keep a short tombstone window so a client retry receives a
		// machine-readable 410 instead of becoming indistinguishable from an
		// invented capability. Old entries are still bounded to one hour total.
		if now.After(item.ExpiresAt.Add(30 * time.Minute)) {
			delete(s.tickets, key)
		}
	}
	name := "cpa-"
	if scope == "all" {
		name += "all-sessions"
	} else {
		name += "session-" + safeFilename(sessionID)
	}
	name += "-" + format + ".jsonl"
	expiresAt := now.Add(30 * time.Minute)
	if stableEntry != nil && stableEntry.ExpiresAt.Before(expiresAt) {
		expiresAt = stableEntry.ExpiresAt
	}
	s.tickets[token] = exportTicket{SessionID: sessionID, Scope: scope, Format: format, Filename: name, ExpiresAt: expiresAt, Snapshot: snapshotID, RecordsSHA256: recordsSHA256, CursorProtocol: cursorProtocol}
	s.ticketMu.Unlock()
	response := map[string]any{"url": "/archive-api/v1/exports/" + token, "filename": name, "content_type": "application/x-ndjson", "expires_at": expiresAt}
	if snapshotID != "" {
		response["snapshot_schema_version"] = archive.StableSnapshotSchemaVersion
		response["cursor_protocol"] = cursorProtocol
		response["snapshot"] = snapshotID
		response["records_sha256"] = recordsSHA256
	}
	writeJSON(w, response)
}
func (s *server) ticketedExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		stableError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	token := strings.TrimPrefix(r.URL.Path, "/archive-api/v1/exports/")
	s.ticketMu.Lock()
	ticket, ok := s.tickets[token]
	expired := ok && !time.Now().Before(ticket.ExpiresAt)
	if expired {
		ok = false
	}
	s.ticketMu.Unlock()
	if !ok {
		if expired {
			stableError(w, http.StatusGone, "export ticket expired")
		} else {
			stableError(w, http.StatusNotFound, "invalid export ticket")
		}
		return
	}
	var stableArtifact *os.File
	var stableArtifactSize int64
	if ticket.Snapshot != "" {
		var err error
		if r.Method == http.MethodHead {
			err = s.validateStableSnapshotExport(ticket)
		} else {
			stableArtifact, stableArtifactSize, err = s.prepareStableSnapshotExport(r, ticket)
		}
		if err != nil {
			switch {
			case errors.Is(err, errStableSnapshotExpired), errors.Is(err, context.DeadlineExceeded):
				stableError(w, http.StatusGone, "stable session snapshot expired")
			case errors.Is(err, archive.ErrSnapshotCursor):
				stableError(w, http.StatusBadRequest, "invalid stable snapshot export")
			default:
				stableError(w, http.StatusInternalServerError, "stable snapshot export failed")
			}
			return
		}
		if stableArtifact != nil {
			defer func() {
				_ = stableArtifact.Close()
				_ = os.Remove(stableArtifact.Name())
			}()
			w.Header().Set("Content-Length", strconv.FormatInt(stableArtifactSize, 10))
		}
	}
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+ticket.Filename+`"`)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Accel-Buffering", "no")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	// Stable snapshot exports are completely materialized and verified above,
	// before committing 200. This makes snapshot expiry a machine-readable 410
	// instead of a successful response with a truncated attachment.
	w.WriteHeader(http.StatusOK)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	var e error
	if stableArtifact != nil {
		_, e = io.Copy(w, stableArtifact)
	} else if ticket.Format == "sft" {
		e = s.s.ExportTrainingJSONL(r.Context(), map[bool]string{true: ticket.SessionID, false: ""}[ticket.Scope == "session"], w)
	} else {
		e = s.s.ExportArchiveJSONL(r.Context(), map[bool]string{true: ticket.SessionID, false: ""}[ticket.Scope == "session"], w)
	}
	if e != nil {
		log.Printf("ticketed session export failed id=%s: %v", ticket.SessionID, e)
	}
}
func safeFilename(v string) string {
	var b strings.Builder
	for _, r := range v {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "archive"
	}
	return b.String()
}
func (s *server) facets(w http.ResponseWriter, r *http.Request) {
	out, e := s.s.Facets(r.Context())
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	writeJSON(w, out)
}
func (s *server) stats(w http.ResponseWriter, r *http.Request) {
	out, e := s.s.Stats(r.Context())
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	registry, registryErr := s.stableRegistry()
	activeSnapshots, oldestSnapshotAge := 0, int64(0)
	if registryErr == nil {
		activeSnapshots, oldestSnapshotAge = registry.diagnostics()
	}
	pendingDigests, pendingDigestCapacity := s.s.PendingSessionExportDigests()
	snapshotSchemaVersion, tombstoneSafeAfter, contractErr := s.s.StableSnapshotContract(r.Context())
	if contractErr != nil {
		http.Error(w, contractErr.Error(), 500)
		return
	}
	writeJSON(w, struct {
		archive.Stats
		SessionCursorProtocols        []string `json:"session_cursor_protocols"`
		SnapshotSchemaVersion         int      `json:"snapshot_schema_version"`
		TombstoneSafeAfterIngestFence string   `json:"tombstone_safe_after_ingest_fence"`
		SnapshotTTLSeconds            int64    `json:"snapshot_ttl_seconds"`
		SnapshotIdleTTLSeconds        int64    `json:"snapshot_idle_ttl_seconds"`
		MaxActiveSnapshots            int      `json:"max_active_snapshots"`
		ActiveSnapshots               int      `json:"active_snapshots"`
		OldestSnapshotAge             int64    `json:"oldest_snapshot_age_seconds"`
		OfflineFullEnabled            bool     `json:"offline_full_snapshot_enabled"`
		PendingSessionDigests         int      `json:"pending_session_digests"`
		PendingDigestCapacity         int      `json:"pending_session_digest_capacity"`
	}{
		Stats:                         out,
		SessionCursorProtocols:        []string{archive.StableCursorProtocol},
		SnapshotSchemaVersion:         snapshotSchemaVersion,
		TombstoneSafeAfterIngestFence: strconv.FormatInt(tombstoneSafeAfter, 10),
		SnapshotTTLSeconds:            int64(registryTTL(s.allowOfflineFull).Seconds()),
		SnapshotIdleTTLSeconds:        int64(stableSnapshotIdleTTL.Seconds()),
		MaxActiveSnapshots:            stableMaxActiveSnapshots,
		ActiveSnapshots:               activeSnapshots,
		OldestSnapshotAge:             oldestSnapshotAge,
		OfflineFullEnabled:            s.allowOfflineFull,
		PendingSessionDigests:         pendingDigests,
		PendingDigestCapacity:         pendingDigestCapacity,
	})
}
func (s *server) gc(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method", 405)
		return
	}
	n, e := s.s.GC()
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	writeJSON(w, map[string]any{"deleted_blobs": n})
}
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
