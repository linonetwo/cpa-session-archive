package archive

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

type Store struct {
	DB            *sql.DB
	DBPath        string
	StoreUpstream bool
}
type SessionSummary struct {
	SessionID string `json:"session_id"`
	Requests  int    `json:"requests"`
	FirstAt   string `json:"first_at"`
	LastAt    string `json:"last_at"`
	KeyID     string `json:"key_id,omitempty"`
	Model     string `json:"model,omitempty"`
	Project   string `json:"project,omitempty"`
	Summary   string `json:"summary,omitempty"`
}
type Stats struct {
	Records         int64 `json:"records"`
	Sessions        int64 `json:"sessions"`
	Blobs           int64 `json:"blobs"`
	LogicalBytes    int64 `json:"logical_bytes"`
	CompressedBytes int64 `json:"compressed_bytes"`
	SavedBytes      int64 `json:"saved_bytes"`
}

func OpenStore(path string, storeUpstream bool) (*Store, error) {
	db, e := sql.Open("sqlite3", path+"?_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL&_foreign_keys=on")
	if e != nil {
		return nil, e
	}
	schema := `CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,trace_id TEXT,session_id TEXT NOT NULL,key_id TEXT,source_format TEXT,requested_model TEXT,model TEXT,stream INTEGER,outcome TEXT,status_code INTEGER,error TEXT,started_at TEXT,completed_at TEXT,parent_response_id TEXT,response_id TEXT,original_request_gz BLOB,upstream_request_gz BLOB,response_gz BLOB,truncated INTEGER,metadata_json TEXT);CREATE TABLE IF NOT EXISTS record_facets(request_id TEXT NOT NULL,name TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(request_id,name,value));CREATE INDEX IF NOT EXISTS idx_facets_name_value ON record_facets(name,value);CREATE TABLE IF NOT EXISTS blobs(hash TEXT PRIMARY KEY,media_type TEXT,raw_size INTEGER NOT NULL,codec TEXT NOT NULL,data BLOB NOT NULL);CREATE INDEX IF NOT EXISTS idx_records_session_time ON records(session_id,started_at);CREATE INDEX IF NOT EXISTS idx_records_key_time ON records(key_id,started_at);CREATE INDEX IF NOT EXISTS idx_records_model_time ON records(requested_model,started_at);CREATE TABLE IF NOT EXISTS session_summaries(session_id TEXT PRIMARY KEY,requests INTEGER NOT NULL,first_at TEXT NOT NULL,last_at TEXT NOT NULL,key_id TEXT NOT NULL DEFAULT '',model TEXT NOT NULL DEFAULT '',project TEXT NOT NULL DEFAULT '',summary TEXT NOT NULL DEFAULT '',summary_at TEXT NOT NULL DEFAULT '');CREATE TABLE IF NOT EXISTS session_facets(session_id TEXT NOT NULL,name TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(session_id,name,value));CREATE INDEX IF NOT EXISTS idx_session_facets_name_value ON session_facets(name,value);CREATE TABLE IF NOT EXISTS session_indexed_requests(request_id TEXT PRIMARY KEY,session_id TEXT NOT NULL);CREATE TABLE IF NOT EXISTS normalized_response_requests(request_id TEXT PRIMARY KEY);`
	if _, e = db.Exec(schema); e != nil {
		return nil, e
	}
	for _, q := range []string{"ALTER TABLE records ADD COLUMN original_ref TEXT", "ALTER TABLE records ADD COLUMN upstream_ref TEXT", "ALTER TABLE records ADD COLUMN response_ref TEXT", "ALTER TABLE records ADD COLUMN facets_json TEXT", "ALTER TABLE records ADD COLUMN summary TEXT"} {
		_, _ = db.Exec(q)
	}
	s := &Store{DB: db, DBPath: path, StoreUpstream: storeUpstream}
	return s, nil
}
func gzipBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	var out bytes.Buffer
	z, _ := gzip.NewWriterLevel(&out, gzip.BestSpeed)
	_, _ = z.Write(b)
	_ = z.Close()
	return out.Bytes()
}
func gunzipBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	z, e := gzip.NewReader(bytes.NewReader(b))
	if e != nil {
		return nil
	}
	defer z.Close()
	v, _ := io.ReadAll(z)
	return v
}
func putBlob(tx *sql.Tx, b Blob) error {
	_, e := tx.Exec(`INSERT OR IGNORE INTO blobs(hash,media_type,raw_size,codec,data) VALUES(?,?,?,'gzip',?)`, b.Hash, b.MediaType, b.RawSize, gzipBytes(b.Data))
	return e
}
func putPayload(tx *sql.Tx, raw []byte) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	manifest, blobs, e := CompactPayload(raw)
	if e != nil {
		return "", e
	}
	for _, b := range blobs {
		if e = putBlob(tx, b); e != nil {
			return "", e
		}
	}
	sum := sha256.Sum256(manifest)
	mb := Blob{Hash: "sha256:" + hex.EncodeToString(sum[:]), RawSize: int64(len(manifest)), MediaType: "application/vnd.cpa.archive-manifest+json", Data: manifest}
	if e = putBlob(tx, mb); e != nil {
		return "", e
	}
	return mb.Hash, nil
}
func (s *Store) PutBatch(batch []Record) error {
	tx, e := s.DB.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	st, e := tx.Prepare(`INSERT INTO records(request_id,trace_id,session_id,key_id,summary,source_format,requested_model,model,stream,outcome,status_code,error,started_at,completed_at,parent_response_id,response_id,original_ref,upstream_ref,response_ref,truncated,metadata_json,facets_json,original_request_gz,upstream_request_gz,response_gz) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL) ON CONFLICT(request_id) DO UPDATE SET outcome=excluded.outcome,status_code=excluded.status_code,error=excluded.error,completed_at=excluded.completed_at,response_id=excluded.response_id,response_ref=excluded.response_ref,truncated=excluded.truncated,summary=CASE WHEN COALESCE(records.summary,'')='' THEN excluded.summary ELSE records.summary END`)
	if e != nil {
		return e
	}
	defer st.Close()
	for _, r := range batch {
		orig, e := putPayload(tx, r.OriginalRequest)
		if e != nil {
			return e
		}
		up := ""
		if s.StoreUpstream {
			up, e = putPayload(tx, r.UpstreamRequest)
			if e != nil {
				return e
			}
		}
		resp, e := putPayload(tx, r.Response)
		if e != nil {
			return e
		}
		m, _ := json.Marshal(r.Metadata)
		if _, e = st.Exec(r.RequestID, r.TraceID, r.SessionID, r.KeyID, r.Summary, r.SourceFormat, r.RequestedModel, r.Model, r.Stream, r.Outcome, r.StatusCode, r.Error, r.StartedAt.Format(time.RFC3339Nano), r.CompletedAt.Format(time.RFC3339Nano), r.ParentResponseID, r.ResponseID, orig, up, resp, r.Truncated, string(m), facetsJSON(r.Facets)); e != nil {
			return e
		}
		if _, e = tx.Exec(`DELETE FROM record_facets WHERE request_id=?`, r.RequestID); e != nil {
			return e
		}
		for name, values := range r.Facets {
			for _, value := range values {
				if _, e = tx.Exec(`INSERT OR IGNORE INTO record_facets(request_id,name,value) VALUES(?,?,?)`, r.RequestID, name, value); e != nil {
					return e
				}
			}
		}
		if e = indexSessionRecord(tx, r); e != nil {
			return e
		}
	}
	return tx.Commit()
}
func (s *Store) loadBlob(hash string) ([]byte, error) {
	if hash == "" {
		return nil, nil
	}
	var data []byte
	var codec string
	if e := s.DB.QueryRow(`SELECT codec,data FROM blobs WHERE hash=?`, hash).Scan(&codec, &data); e != nil {
		return nil, e
	}
	if codec == "gzip" {
		return gunzipBytes(data), nil
	}
	return data, nil
}
func (s *Store) LoadPayload(hash string) ([]byte, error) {
	m, e := s.loadBlob(hash)
	if e != nil {
		return nil, e
	}
	return ExpandPayload(m, s.loadBlob)
}
func facetsJSON(v map[string][]string) string { b, _ := json.Marshal(v); return string(b) }

type FacetCount struct {
	Name     string `json:"name"`
	Value    string `json:"value"`
	Sessions int    `json:"sessions"`
}

func (s *Store) Facets(ctx context.Context) ([]FacetCount, error) {
	rows, e := s.DB.QueryContext(ctx, `SELECT name,value,COUNT(*) FROM session_facets GROUP BY name,value ORDER BY name,COUNT(*) DESC LIMIT 5000`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []FacetCount{}
	for rows.Next() {
		var x FacetCount
		if e = rows.Scan(&x.Name, &x.Value, &x.Sessions); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func (s *Store) Sessions(ctx context.Context, limit int) ([]SessionSummary, error) {
	rows, e := s.DB.QueryContext(ctx, `SELECT session_id,requests,first_at,last_at,key_id,model,project,summary FROM session_summaries ORDER BY last_at DESC LIMIT ?`, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []SessionSummary{}
	for rows.Next() {
		var x SessionSummary
		if e = rows.Scan(&x.SessionID, &x.Requests, &x.FirstAt, &x.LastAt, &x.KeyID, &x.Model, &x.Project, &x.Summary); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

func (s *Store) SessionsFiltered(ctx context.Context, limit int, filters map[string]string) ([]SessionSummary, error) {
	where := []string{}
	args := []any{}
	for name, value := range filters {
		where = append(where, `EXISTS (SELECT 1 FROM session_facets f WHERE f.session_id=s.session_id AND f.name=? AND f.value=?)`)
		args = append(args, name, value)
	}
	q := `SELECT s.session_id,s.requests,s.first_at,s.last_at,s.key_id,s.model,s.project,s.summary FROM session_summaries s`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	q += ` ORDER BY s.last_at DESC LIMIT ?`
	args = append(args, limit)
	rows, e := s.DB.QueryContext(ctx, q, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []SessionSummary{}
	for rows.Next() {
		var x SessionSummary
		if e = rows.Scan(&x.SessionID, &x.Requests, &x.FirstAt, &x.LastAt, &x.KeyID, &x.Model, &x.Project, &x.Summary); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

type SessionPage struct {
	Records []Record `json:"records"`
	Total   int      `json:"total"`
	Limit   int      `json:"limit"`
	Offset  int      `json:"offset"`
}

func (s *Store) SessionMetadataRange(ctx context.Context, id string, limit, offset int, filters map[string]string) (SessionPage, error) {
	out := SessionPage{Records: []Record{}, Limit: limit, Offset: offset}
	where := []string{"session_id=?"}
	args := []any{id}
	for name, value := range filters {
		where = append(where, `EXISTS (SELECT 1 FROM record_facets f WHERE f.request_id=records.request_id AND f.name=? AND f.value=?)`)
		args = append(args, name, value)
	}
	clause := strings.Join(where, " AND ")
	if e := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM records WHERE `+clause, args...).Scan(&out.Total); e != nil {
		return out, e
	}
	queryArgs := append(append([]any{}, args...), limit, offset)
	rows, e := s.DB.QueryContext(ctx, `SELECT request_id,trace_id,COALESCE(key_id,''),COALESCE(summary,''),COALESCE(source_format,''),COALESCE(requested_model,''),COALESCE(model,''),stream,COALESCE(outcome,''),status_code,COALESCE(error,''),started_at,completed_at,COALESCE(parent_response_id,''),COALESCE(response_id,''),truncated,COALESCE(metadata_json,''),COALESCE(facets_json,'') FROM records WHERE `+clause+` ORDER BY started_at LIMIT ? OFFSET ?`, queryArgs...)
	if e != nil {
		return out, e
	}
	defer rows.Close()
	for rows.Next() {
		var x Record
		var stream, truncated int
		var started, completed, metadata, facets string
		x.SessionID = id
		if e = rows.Scan(&x.RequestID, &x.TraceID, &x.KeyID, &x.Summary, &x.SourceFormat, &x.RequestedModel, &x.Model, &stream, &x.Outcome, &x.StatusCode, &x.Error, &started, &completed, &x.ParentResponseID, &x.ResponseID, &truncated, &metadata, &facets); e != nil {
			return out, e
		}
		x.Stream = stream != 0
		x.Truncated = truncated != 0
		x.StartedAt, _ = time.Parse(time.RFC3339Nano, started)
		x.CompletedAt, _ = time.Parse(time.RFC3339Nano, completed)
		_ = json.Unmarshal([]byte(metadata), &x.Metadata)
		_ = json.Unmarshal([]byte(facets), &x.Facets)
		out.Records = append(out.Records, x)
	}
	return out, rows.Err()
}

func (s *Store) SessionRange(ctx context.Context, id string, limit, offset, previewBytes int) (SessionPage, error) {
	out := SessionPage{Records: []Record{}, Limit: limit, Offset: offset}
	if e := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM records WHERE session_id=?`, id).Scan(&out.Total); e != nil {
		return out, e
	}
	rows, e := s.DB.QueryContext(ctx, `SELECT request_id,trace_id,COALESCE(key_id,''),COALESCE(summary,''),COALESCE(source_format,''),COALESCE(requested_model,''),COALESCE(model,''),stream,COALESCE(outcome,''),status_code,COALESCE(error,''),started_at,completed_at,COALESCE(parent_response_id,''),COALESCE(response_id,''),COALESCE(original_ref,''),COALESCE(upstream_ref,''),COALESCE(response_ref,''),truncated,COALESCE(metadata_json,''),COALESCE(facets_json,''),original_request_gz,upstream_request_gz,response_gz FROM records WHERE session_id=? ORDER BY started_at LIMIT ? OFFSET ?`, id, limit, offset)
	if e != nil {
		return out, e
	}
	defer rows.Close()
	for rows.Next() {
		var x Record
		var stream, trunc int
		var started, done, meta, facets, or, ur, rr string
		var oldO, oldU, oldR []byte
		x.SessionID = id
		if e = rows.Scan(&x.RequestID, &x.TraceID, &x.KeyID, &x.Summary, &x.SourceFormat, &x.RequestedModel, &x.Model, &stream, &x.Outcome, &x.StatusCode, &x.Error, &started, &done, &x.ParentResponseID, &x.ResponseID, &or, &ur, &rr, &trunc, &meta, &facets, &oldO, &oldU, &oldR); e != nil {
			return out, e
		}
		x.Stream = stream != 0
		x.Truncated = trunc != 0
		x.StartedAt, _ = time.Parse(time.RFC3339Nano, started)
		x.CompletedAt, _ = time.Parse(time.RFC3339Nano, done)
		if or != "" {
			x.OriginalRequest, _ = s.LoadPayload(or)
		} else {
			x.OriginalRequest = gunzipBytes(oldO)
		}
		if ur != "" {
			x.UpstreamRequest, _ = s.LoadPayload(ur)
		} else if s.StoreUpstream {
			x.UpstreamRequest = gunzipBytes(oldU)
		}
		if rr != "" {
			x.Response, _ = s.LoadPayload(rr)
		} else {
			x.Response = gunzipBytes(oldR)
		}
		_ = json.Unmarshal([]byte(meta), &x.Metadata)
		_ = json.Unmarshal([]byte(facets), &x.Facets)
		if previewBytes > 0 {
			if len(x.OriginalRequest) > previewBytes {
				x.OriginalRequest = x.OriginalRequest[:previewBytes]
				x.Truncated = true
			}
			if len(x.Response) > previewBytes {
				x.Response = x.Response[:previewBytes]
				x.Truncated = true
			}
			x.UpstreamRequest = nil
		}
		out.Records = append(out.Records, x)
	}
	return out, rows.Err()
}
func (s *Store) Session(ctx context.Context, id string) ([]Record, error) {
	page, e := s.SessionRange(ctx, id, 1000000, 0, 0)
	return page.Records, e
}

func (s *Store) Request(ctx context.Context, id string) (Record, error) {
	var x Record
	var stream, trunc int
	var started, done, meta, facets, or, ur, rr, sessionID string
	var oldO, oldU, oldR []byte
	err := s.DB.QueryRowContext(ctx, `SELECT session_id,request_id,trace_id,COALESCE(key_id,''),COALESCE(summary,''),COALESCE(source_format,''),COALESCE(requested_model,''),COALESCE(model,''),stream,COALESCE(outcome,''),status_code,COALESCE(error,''),started_at,completed_at,COALESCE(parent_response_id,''),COALESCE(response_id,''),COALESCE(original_ref,''),COALESCE(upstream_ref,''),COALESCE(response_ref,''),truncated,COALESCE(metadata_json,''),COALESCE(facets_json,''),original_request_gz,upstream_request_gz,response_gz FROM records WHERE request_id=? LIMIT 1`, id).Scan(&sessionID, &x.RequestID, &x.TraceID, &x.KeyID, &x.Summary, &x.SourceFormat, &x.RequestedModel, &x.Model, &stream, &x.Outcome, &x.StatusCode, &x.Error, &started, &done, &x.ParentResponseID, &x.ResponseID, &or, &ur, &rr, &trunc, &meta, &facets, &oldO, &oldU, &oldR)
	if err != nil {
		return x, err
	}
	x.SessionID = sessionID
	x.Stream = stream != 0
	x.Truncated = trunc != 0
	x.StartedAt, _ = time.Parse(time.RFC3339Nano, started)
	x.CompletedAt, _ = time.Parse(time.RFC3339Nano, done)
	if or != "" {
		x.OriginalRequest, err = s.LoadPayload(or)
	} else {
		x.OriginalRequest = gunzipBytes(oldO)
	}
	if err != nil {
		return x, err
	}
	if ur != "" {
		x.UpstreamRequest, err = s.LoadPayload(ur)
	} else if s.StoreUpstream {
		x.UpstreamRequest = gunzipBytes(oldU)
	}
	if err != nil {
		return x, err
	}
	if rr != "" {
		x.Response, err = s.LoadPayload(rr)
	} else {
		x.Response = gunzipBytes(oldR)
	}
	if err != nil {
		return x, err
	}
	_ = json.Unmarshal([]byte(meta), &x.Metadata)
	_ = json.Unmarshal([]byte(facets), &x.Facets)
	return x, nil
}

type TrainingRecord struct {
	SchemaVersion  int                 `json:"schema_version"`
	SessionID      string              `json:"session_id"`
	RequestID      string              `json:"request_id"`
	StartedAt      time.Time           `json:"started_at"`
	CompletedAt    time.Time           `json:"completed_at"`
	KeyID          string              `json:"key_id,omitempty"`
	RequestedModel string              `json:"requested_model,omitempty"`
	Model          string              `json:"model,omitempty"`
	Outcome        string              `json:"outcome,omitempty"`
	StatusCode     int                 `json:"status_code,omitempty"`
	Metadata       map[string]any      `json:"metadata,omitempty"`
	Facets         map[string][]string `json:"facets,omitempty"`
	Request        any                 `json:"request,omitempty"`
	Response       any                 `json:"response,omitempty"`
}

// ExportSessionJSONL writes a complete session one record at a time. JSON
// payloads remain structured objects instead of opaque/base64 strings.
func (s *Store) ExportSessionJSONL(ctx context.Context, id string, dst io.Writer) error {
	rows, err := s.DB.QueryContext(ctx, `SELECT request_id FROM records WHERE session_id=? ORDER BY started_at,id`, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var requestID string
		if err = rows.Scan(&requestID); err != nil {
			return err
		}
		ids = append(ids, requestID)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	enc := json.NewEncoder(dst)
	enc.SetEscapeHTML(false)
	for _, requestID := range ids {
		if err = ctx.Err(); err != nil {
			return err
		}
		r, loadErr := s.Request(ctx, requestID)
		if loadErr != nil {
			return loadErr
		}
		item := TrainingRecord{SchemaVersion: 1, SessionID: r.SessionID, RequestID: r.RequestID, StartedAt: r.StartedAt, CompletedAt: r.CompletedAt, KeyID: r.KeyID, RequestedModel: r.RequestedModel, Model: r.Model, Outcome: r.Outcome, StatusCode: r.StatusCode, Metadata: r.Metadata, Facets: r.Facets, Request: decodedPayload(r.OriginalRequest), Response: decodedPayload(r.Response)}
		if err = enc.Encode(item); err != nil {
			return err
		}
		if f, ok := dst.(interface{ Flush() }); ok {
			f.Flush()
		}
	}
	return nil
}

func decodedPayload(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	var value any
	if json.Unmarshal(raw, &value) == nil {
		return value
	}
	return string(raw)
}
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var x Stats
	_ = s.DB.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(DISTINCT session_id) FROM records`).Scan(&x.Records, &x.Sessions)
	_ = s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM blobs`).Scan(&x.Blobs)
	// Report the storage a user can act on. SUM(LENGTH(data)) scans every large
	// blob and can block the management page for minutes while legacy rows are
	// being migrated. File metadata is O(1) and includes SQLite WAL usage.
	x.CompressedBytes = fileSize(s.DBPath) + fileSize(s.DBPath+"-wal") + fileSize(s.DBPath+"-shm")
	return x, nil
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
func (s *Store) MigrateLegacy() {
	for {
		rows, e := s.DB.Query(`SELECT request_id,original_request_gz,upstream_request_gz,response_gz FROM records WHERE (original_ref IS NULL OR original_ref='') AND (original_request_gz IS NOT NULL OR response_gz IS NOT NULL) LIMIT 8`)
		if e != nil {
			return
		}
		type old struct {
			id      string
			o, u, r []byte
		}
		var batch []old
		for rows.Next() {
			var x old
			if rows.Scan(&x.id, &x.o, &x.u, &x.r) == nil {
				batch = append(batch, x)
			}
		}
		rows.Close()
		if len(batch) == 0 {
			return
		}
		tx, e := s.DB.Begin()
		if e != nil {
			return
		}
		for _, x := range batch {
			or, e := putPayload(tx, gunzipBytes(x.o))
			if e != nil {
				_ = tx.Rollback()
				return
			}
			ur := ""
			if s.StoreUpstream {
				ur, e = putPayload(tx, gunzipBytes(x.u))
				if e != nil {
					_ = tx.Rollback()
					return
				}
			}
			rr, e := putPayload(tx, gunzipBytes(x.r))
			if e != nil {
				_ = tx.Rollback()
				return
			}
			if _, e = tx.Exec(`UPDATE records SET original_ref=?,upstream_ref=?,response_ref=?,original_request_gz=NULL,upstream_request_gz=NULL,response_gz=NULL WHERE request_id=?`, or, ur, rr, x.id); e != nil {
				_ = tx.Rollback()
				return
			}
		}
		if e = tx.Commit(); e != nil {
			return
		}
		log.Printf("migrated %d legacy archive records to CAS", len(batch))
	}
}
func (s *Store) GC() (int64, error) {
	res, e := s.DB.Exec(`DELETE FROM blobs WHERE hash NOT IN (SELECT original_ref FROM records WHERE original_ref!='' UNION SELECT upstream_ref FROM records WHERE upstream_ref!='' UNION SELECT response_ref FROM records WHERE response_ref!='')`)
	if e != nil {
		return 0, e
	}
	return res.RowsAffected()
}

var _ = errors.Is
