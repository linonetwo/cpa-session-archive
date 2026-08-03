package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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
	s        *archive.Store
	q        chan archive.Record
	ticketMu sync.Mutex
	tickets  map[string]exportTicket
}

type exportTicket struct {
	SessionID string
	Scope     string
	Format    string
	Filename  string
	ExpiresAt time.Time
}

func main() {
	dbPath := env("ARCHIVE_DB", "/data/archive.sqlite")
	storeUpstream := env("STORE_UPSTREAM_REQUEST", "false") == "true"
	st, e := archive.OpenStore(dbPath, storeUpstream)
	if e != nil {
		log.Fatal(e)
	}
	if env("ARCHIVE_MIGRATE_LEGACY", "false") == "true" {
		go st.MigrateLegacy()
	}
	go func() {
		if env("ARCHIVE_REPAIR_CANONICAL_SESSIONS", "true") == "true" {
			for {
				if changed, err := st.RepairCanonicalSessions(context.Background()); err != nil {
					log.Printf("canonical session repair will retry: %v", err)
					time.Sleep(5 * time.Second)
					continue
				} else if changed > 0 {
					log.Printf("canonical session repair merged %d request records", changed)
				}
				break
			}
		}
		if env("ARCHIVE_BACKFILL_SESSION_INDEX", "true") == "true" {
			for {
				if err := st.BackfillSessionIndex(context.Background()); err != nil {
					log.Printf("session index backfill will retry: %v", err)
					time.Sleep(5 * time.Second)
					continue
				}
				break
			}
		}
		if env("ARCHIVE_REPAIR_SESSION_SUMMARIES", "true") == "true" {
			for {
				if err := st.RepairSessionSummaries(context.Background()); err != nil {
					log.Printf("session summary repair will retry: %v", err)
					time.Sleep(5 * time.Second)
					continue
				}
				break
			}
		}
		if env("ARCHIVE_REPAIR_RECORD_PREVIEWS", "true") == "true" {
			for {
				if err := st.RepairRecordPreviews(context.Background()); err != nil {
					log.Printf("request preview repair will retry: %v", err)
					time.Sleep(5 * time.Second)
					continue
				}
				break
			}
		}
		if env("ARCHIVE_NORMALIZE_SSE", "true") == "true" {
			for {
				if err := st.NormalizeHistoricalSSE(context.Background()); err != nil {
					log.Printf("historical SSE normalization will retry: %v", err)
					time.Sleep(5 * time.Second)
					continue
				}
				break
			}
		}
	}()
	if env("ARCHIVE_BACKFILL_TURN_PROJECTION", "true") == "true" {
		go func() {
			time.Sleep(2 * time.Second)
			for {
				if err := st.BackfillTurnProjection(context.Background(), 64, 25*time.Millisecond); err != nil {
					log.Printf("turn projection backfill will retry: %v", err)
					time.Sleep(5 * time.Second)
					continue
				}
				log.Printf("turn projection backfill complete")
				break
			}
		}()
	}
	s := &server{s: st, q: make(chan archive.Record, 4096), tickets: map[string]exportTicket{}}
	go s.writer()
	http.HandleFunc("/healthz", s.health)
	http.HandleFunc("/ingest", s.ingest)
	http.HandleFunc("/v1/stats", s.stats)
	http.HandleFunc("/v1/facets", s.facets)
	http.HandleFunc("/v1/sessions", s.sessions)
	http.HandleFunc("/v1/sessions/", s.session)
	http.HandleFunc("/v1/requests/", s.request)
	http.HandleFunc("/v1/request-context", s.requestContext)
	http.HandleFunc("/v1/request-view", s.requestView)
	http.HandleFunc("/v1/turns", s.turns)
	http.HandleFunc("/v1/export-tickets", s.exportTicket)
	http.HandleFunc("/archive-api/v1/exports/", s.ticketedExport)
	http.HandleFunc("/v1/maintenance/gc", s.gc)
	addr := env("LISTEN_ADDR", ":8080")
	log.Printf("archive collector v%s listening on %s, db=%s, store_upstream=%v", archive.Version, addr, dbPath, storeUpstream)
	log.Fatal(http.ListenAndServe(addr, nil))
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func (s *server) health(w http.ResponseWriter, r *http.Request) {
	if e := s.s.DB.PingContext(r.Context()); e != nil {
		http.Error(w, e.Error(), 503)
		return
	}
	w.Write([]byte("ok"))
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
	if scope == "session" {
		var exists int
		if err := s.s.DB.QueryRowContext(r.Context(), `SELECT 1 FROM records WHERE session_id=? LIMIT 1`, sessionID).Scan(&exists); err != nil {
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
		if now.After(item.ExpiresAt) {
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
	s.tickets[token] = exportTicket{SessionID: sessionID, Scope: scope, Format: format, Filename: name, ExpiresAt: expiresAt}
	s.ticketMu.Unlock()
	writeJSON(w, map[string]any{"url": "/archive-api/v1/exports/" + token, "filename": name, "content_type": "application/x-ndjson", "expires_at": expiresAt})
}
func (s *server) ticketedExport(w http.ResponseWriter, r *http.Request) {
	token := strings.TrimPrefix(r.URL.Path, "/archive-api/v1/exports/")
	s.ticketMu.Lock()
	ticket, ok := s.tickets[token]
	if ok && time.Now().After(ticket.ExpiresAt) {
		delete(s.tickets, token)
		ok = false
	}
	s.ticketMu.Unlock()
	if !ok {
		http.Error(w, "invalid or expired export ticket", 404)
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+ticket.Filename+`"`)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	var e error
	if ticket.Format == "sft" {
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
	writeJSON(w, out)
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
