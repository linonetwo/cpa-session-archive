package main

import (
	"cpa-session-archive/internal/archive"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
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
	log.Printf("archive collector v0.3.1 listening on %s, db=%s, store_upstream=%v", addr, dbPath, storeUpstream)
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
	if e := s.s.PutBatch(batch); e != nil {
		log.Printf("archive batch: %v", e)
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
	out, e := s.s.Session(r.Context(), id)
	if e != nil {
		http.Error(w, e.Error(), 500)
		return
	}
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
