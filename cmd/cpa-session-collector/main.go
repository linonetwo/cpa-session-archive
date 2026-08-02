package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"cpa-session-archive/internal/archive"
)

type server struct {
	s *archive.Store
	q chan archive.Record
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
	s := &server{s: st, q: make(chan archive.Record, 4096)}
	go s.writer()
	http.HandleFunc("/healthz", s.health)
	http.HandleFunc("/ingest", s.ingest)
	http.HandleFunc("/v1/stats", s.stats)
	http.HandleFunc("/v1/facets", s.facets)
	http.HandleFunc("/v1/sessions", s.sessions)
	http.HandleFunc("/v1/sessions/", s.session)
	http.HandleFunc("/v1/maintenance/gc", s.gc)
	addr := env("LISTEN_ADDR", ":8080")
	log.Printf("archive collector v0.4.5 listening on %s, db=%s, store_upstream=%v", addr, dbPath, storeUpstream)
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
		if e == nil { return }
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
	filters:=map[string]string{};for name,values:=range r.URL.Query(){if name=="limit"||len(values)==0{continue};filters[name]=values[0]}
	out, e := s.s.SessionsFiltered(r.Context(), limit, filters)
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
	writeJSON(w, out)
}
func (s *server) session(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/sessions/")
	if id == "" {
		http.Error(w, "session required", 400)
		return
	}
	if r.URL.Query().Has("limit") || r.URL.Query().Has("offset") {
		limit, offset := 20, 0
		maxLimit := 100
		metadataOnly := r.URL.Query().Get("metadata_only") == "true"
		if metadataOnly { maxLimit = 1000 }
		if v, e := strconv.Atoi(r.URL.Query().Get("limit")); e == nil && v > 0 && v <= maxLimit { limit = v }
		if v, e := strconv.Atoi(r.URL.Query().Get("offset")); e == nil && v >= 0 { offset = v }
		if metadataOnly {
			filters := map[string]string{}
			for name, values := range r.URL.Query() {
				if name == "limit" || name == "offset" || name == "metadata_only" || name == "preview_bytes" || len(values) == 0 { continue }
				filters[name] = values[0]
			}
			out, e := s.s.SessionMetadataRange(r.Context(), id, limit, offset, filters)
			if e != nil { http.Error(w, e.Error(), 500); return }
			writeJSON(w, out)
			return
		}
		preview := 65536
		if v, e := strconv.Atoi(r.URL.Query().Get("preview_bytes")); e == nil && v >= 1024 && v <= 1048576 { preview = v }
		out, e := s.s.SessionRange(r.Context(), id, limit, offset, preview)
		if e != nil { http.Error(w, e.Error(), 500); return }
		writeJSON(w, out)
		return
	}
	out, e := s.s.Session(r.Context(), id)
	if e != nil { http.Error(w, e.Error(), 500); return }
	writeJSON(w, out)
}
func (s *server) facets(w http.ResponseWriter,r *http.Request){out,e:=s.s.Facets(r.Context());if e!=nil{http.Error(w,e.Error(),500);return};writeJSON(w,out)}
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
