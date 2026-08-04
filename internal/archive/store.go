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
	SessionID     string   `json:"session_id"`
	Requests      int      `json:"requests"`
	FirstAt       string   `json:"first_at"`
	LastAt        string   `json:"last_at"`
	KeyID         string   `json:"key_id,omitempty"`
	Model         string   `json:"model,omitempty"`
	Project       string   `json:"project,omitempty"`
	Summary       string   `json:"summary,omitempty"`
	Kinds         []string `json:"kinds,omitempty"`
	ThreadSources []string `json:"thread_sources,omitempty"`
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
	schema := `CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,trace_id TEXT,session_id TEXT NOT NULL,key_id TEXT,source_format TEXT,requested_model TEXT,model TEXT,stream INTEGER,outcome TEXT,status_code INTEGER,error TEXT,started_at TEXT,completed_at TEXT,parent_response_id TEXT,response_id TEXT,original_request_gz BLOB,upstream_request_gz BLOB,response_gz BLOB,truncated INTEGER,metadata_json TEXT);CREATE TABLE IF NOT EXISTS record_facets(request_id TEXT NOT NULL,name TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(request_id,name,value));CREATE INDEX IF NOT EXISTS idx_facets_name_value ON record_facets(name,value);CREATE TABLE IF NOT EXISTS blobs(hash TEXT PRIMARY KEY,media_type TEXT,raw_size INTEGER NOT NULL,codec TEXT NOT NULL,data BLOB NOT NULL);CREATE INDEX IF NOT EXISTS idx_records_session_time ON records(session_id,started_at);CREATE INDEX IF NOT EXISTS idx_records_key_time ON records(key_id,started_at);CREATE INDEX IF NOT EXISTS idx_records_model_time ON records(requested_model,started_at);CREATE TABLE IF NOT EXISTS session_summaries(session_id TEXT PRIMARY KEY,requests INTEGER NOT NULL,first_at TEXT NOT NULL,last_at TEXT NOT NULL,key_id TEXT NOT NULL DEFAULT '',model TEXT NOT NULL DEFAULT '',project TEXT NOT NULL DEFAULT '',summary TEXT NOT NULL DEFAULT '',summary_at TEXT NOT NULL DEFAULT '');CREATE TABLE IF NOT EXISTS session_facets(session_id TEXT NOT NULL,name TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(session_id,name,value));CREATE INDEX IF NOT EXISTS idx_session_facets_name_value ON session_facets(name,value);CREATE TABLE IF NOT EXISTS session_indexed_requests(request_id TEXT PRIMARY KEY,session_id TEXT NOT NULL);CREATE TABLE IF NOT EXISTS normalized_response_requests(request_id TEXT PRIMARY KEY);CREATE TABLE IF NOT EXISTS previewed_requests(request_id TEXT PRIMARY KEY,version INTEGER NOT NULL DEFAULT 1);CREATE TABLE IF NOT EXISTS repair_versions(name TEXT PRIMARY KEY,version INTEGER NOT NULL);`
	if _, e = db.Exec(schema); e != nil {
		return nil, e
	}
	for _, q := range []string{"ALTER TABLE records ADD COLUMN original_ref TEXT", "ALTER TABLE records ADD COLUMN upstream_ref TEXT", "ALTER TABLE records ADD COLUMN response_ref TEXT", "ALTER TABLE records ADD COLUMN facets_json TEXT", "ALTER TABLE records ADD COLUMN summary TEXT", "ALTER TABLE records ADD COLUMN response_preview TEXT"} {
		_, _ = db.Exec(q)
	}
	if _, e = db.Exec(`CREATE TABLE IF NOT EXISTS turn_records(
		id INTEGER PRIMARY KEY,
		request_id TEXT NOT NULL UNIQUE,
		session_id TEXT NOT NULL,
		key_id TEXT,
		summary TEXT,
		response_preview TEXT,
		requested_model TEXT,
		model TEXT,
		outcome TEXT,
		status_code INTEGER,
		started_at TEXT,
		completed_at TEXT,
		facets_json TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_turn_records_session_time ON turn_records(session_id,started_at,id)`); e != nil {
		return nil, e
	}
	for _, q := range []string{
		"ALTER TABLE turn_records ADD COLUMN turn_id TEXT",
		"ALTER TABLE turn_records ADD COLUMN tool_names_json TEXT",
		"ALTER TABLE turn_records ADD COLUMN request_kinds_json TEXT",
	} {
		_, _ = db.Exec(q)
	}
	if _, e = db.Exec(`CREATE TABLE IF NOT EXISTS turn_texts(
		session_id TEXT NOT NULL,
		turn_id TEXT NOT NULL,
		text_gz BLOB NOT NULL,
		updated_at TEXT NOT NULL,
		PRIMARY KEY(session_id,turn_id)
	)`); e != nil {
		return nil, e
	}
	_, _ = db.Exec(`ALTER TABLE previewed_requests ADD COLUMN version INTEGER NOT NULL DEFAULT 1`)
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
	st, e := tx.Prepare(`INSERT INTO records(request_id,trace_id,session_id,key_id,summary,response_preview,source_format,requested_model,model,stream,outcome,status_code,error,started_at,completed_at,parent_response_id,response_id,original_ref,upstream_ref,response_ref,truncated,metadata_json,facets_json,original_request_gz,upstream_request_gz,response_gz) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL) ON CONFLICT(request_id) DO UPDATE SET outcome=excluded.outcome,status_code=excluded.status_code,error=excluded.error,completed_at=excluded.completed_at,response_id=excluded.response_id,response_ref=excluded.response_ref,truncated=excluded.truncated,summary=CASE WHEN COALESCE(records.summary,'')='' THEN excluded.summary ELSE records.summary END,response_preview=CASE WHEN COALESCE(records.response_preview,'')='' THEN excluded.response_preview ELSE records.response_preview END`)
	if e != nil {
		return e
	}
	defer st.Close()
	turnSt, e := tx.Prepare(`INSERT INTO turn_records(id,request_id,session_id,key_id,summary,response_preview,requested_model,model,outcome,status_code,started_at,completed_at,facets_json,turn_id,tool_names_json,request_kinds_json)
		SELECT id,request_id,session_id,key_id,summary,response_preview,requested_model,model,outcome,status_code,started_at,completed_at,facets_json,?,?,?
		FROM records WHERE request_id=?
		ON CONFLICT(request_id) DO UPDATE SET
			session_id=excluded.session_id,key_id=excluded.key_id,summary=excluded.summary,response_preview=excluded.response_preview,
			requested_model=excluded.requested_model,model=excluded.model,outcome=excluded.outcome,status_code=excluded.status_code,
			started_at=excluded.started_at,completed_at=excluded.completed_at,facets_json=excluded.facets_json,
			turn_id=excluded.turn_id,tool_names_json=excluded.tool_names_json,request_kinds_json=excluded.request_kinds_json`)
	if e != nil {
		return e
	}
	defer turnSt.Close()
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
		if _, e = st.Exec(r.RequestID, r.TraceID, r.SessionID, r.KeyID, r.Summary, r.ResponsePreview, r.SourceFormat, r.RequestedModel, r.Model, r.Stream, r.Outcome, r.StatusCode, r.Error, r.StartedAt.Format(time.RFC3339Nano), r.CompletedAt.Format(time.RFC3339Nano), r.ParentResponseID, r.ResponseID, orig, up, resp, r.Truncated, string(m), facetsJSON(r.Facets)); e != nil {
			return e
		}
		turnID, toolNames, requestKinds := compactTurnFacets(r.Facets)
		if _, e = turnSt.Exec(turnID, toolNames, requestKinds, r.RequestID); e != nil {
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
func facetsJSON(v map[string][]string) string {
	if v == nil {
		return "{}"
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func compactTurnFacets(facets map[string][]string) (string, string, string) {
	turnID := ""
	if values := facets["turn.id"]; len(values) > 0 {
		turnID = strings.TrimSpace(values[0])
	}
	toolValues := facets["tool.name"]
	if toolValues == nil {
		toolValues = []string{}
	}
	kindValues := facets["request.kind"]
	if kindValues == nil {
		kindValues = []string{}
	}
	toolNames, _ := json.Marshal(toolValues)
	requestKinds, _ := json.Marshal(kindValues)
	return turnID, string(toolNames), string(requestKinds)
}

type FacetCount struct {
	Name     string `json:"name"`
	Value    string `json:"value"`
	Sessions int    `json:"sessions"`
}

func (s *Store) Facets(ctx context.Context) ([]FacetCount, error) {
	rows, e := s.DB.QueryContext(ctx, `WITH ranked AS (
		SELECT name,value,COUNT(*) AS sessions,ROW_NUMBER() OVER(PARTITION BY name ORDER BY COUNT(*) DESC,value) AS rank
		FROM session_facets
		WHERE name NOT IN (
			'request.id','trace.id','response.id','turn.id','window.id','tool.call_id',
			'client.request_id','session.id','conversation.id','thread.id',
			'header.session-id','header.thread-id','execution.session.id'
		)
		GROUP BY name,value
	) SELECT name,value,sessions FROM ranked WHERE rank<=100 ORDER BY name,sessions DESC,value`)
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
	rows, e := s.DB.QueryContext(ctx, `SELECT s.session_id,s.requests,s.first_at,s.last_at,s.key_id,s.model,s.project,s.summary,
		COALESCE((SELECT GROUP_CONCAT(value,CHAR(31)) FROM session_facets f WHERE f.session_id=s.session_id AND f.name='request.kind'),''),
		COALESCE((SELECT GROUP_CONCAT(value,CHAR(31)) FROM session_facets f WHERE f.session_id=s.session_id AND f.name='thread.source'),'')
		FROM session_summaries s ORDER BY s.last_at DESC LIMIT ?`, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []SessionSummary{}
	for rows.Next() {
		var x SessionSummary
		var kinds, sources string
		if e = rows.Scan(&x.SessionID, &x.Requests, &x.FirstAt, &x.LastAt, &x.KeyID, &x.Model, &x.Project, &x.Summary, &kinds, &sources); e != nil {
			return nil, e
		}
		x.Kinds = splitProjectionValues(kinds)
		x.ThreadSources = splitProjectionValues(sources)
		x.Summary = cleanTurnText(x.Summary)
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
	q := `SELECT s.session_id,s.requests,s.first_at,s.last_at,s.key_id,s.model,s.project,s.summary,
		COALESCE((SELECT GROUP_CONCAT(value,CHAR(31)) FROM session_facets sf WHERE sf.session_id=s.session_id AND sf.name='request.kind'),''),
		COALESCE((SELECT GROUP_CONCAT(value,CHAR(31)) FROM session_facets sf WHERE sf.session_id=s.session_id AND sf.name='thread.source'),'')
		FROM session_summaries s`
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
		var kinds, sources string
		if e = rows.Scan(&x.SessionID, &x.Requests, &x.FirstAt, &x.LastAt, &x.KeyID, &x.Model, &x.Project, &x.Summary, &kinds, &sources); e != nil {
			return nil, e
		}
		x.Kinds = splitProjectionValues(kinds)
		x.ThreadSources = splitProjectionValues(sources)
		x.Summary = cleanTurnText(x.Summary)
		out = append(out, x)
	}
	return out, rows.Err()
}

func splitProjectionValues(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, string(rune(31)))
}

type SessionPage struct {
	Records []Record `json:"records"`
	Total   int      `json:"total"`
	Limit   int      `json:"limit"`
	Offset  int      `json:"offset"`
}

func (s *Store) SessionMetadataRange(ctx context.Context, id string, limit, offset int, order string, filters map[string]string) (SessionPage, error) {
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
	direction := "ASC"
	if strings.EqualFold(order, "desc") {
		direction = "DESC"
	}
	rows, e := s.DB.QueryContext(ctx, `SELECT request_id,trace_id,COALESCE(key_id,''),COALESCE(summary,''),COALESCE(response_preview,''),COALESCE(source_format,''),COALESCE(requested_model,''),COALESCE(model,''),stream,COALESCE(outcome,''),status_code,COALESCE(error,''),started_at,completed_at,COALESCE(parent_response_id,''),COALESCE(response_id,''),truncated,COALESCE(metadata_json,''),COALESCE(facets_json,'') FROM records WHERE `+clause+` ORDER BY started_at `+direction+`,id `+direction+` LIMIT ? OFFSET ?`, queryArgs...)
	if e != nil {
		return out, e
	}
	defer rows.Close()
	for rows.Next() {
		var x Record
		var stream, truncated int
		var started, completed, metadata, facets string
		x.SessionID = id
		if e = rows.Scan(&x.RequestID, &x.TraceID, &x.KeyID, &x.Summary, &x.ResponsePreview, &x.SourceFormat, &x.RequestedModel, &x.Model, &stream, &x.Outcome, &x.StatusCode, &x.Error, &started, &completed, &x.ParentResponseID, &x.ResponseID, &truncated, &metadata, &facets); e != nil {
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
	rows, e := s.DB.QueryContext(ctx, `SELECT request_id,trace_id,COALESCE(key_id,''),COALESCE(summary,''),COALESCE(response_preview,''),COALESCE(source_format,''),COALESCE(requested_model,''),COALESCE(model,''),stream,COALESCE(outcome,''),status_code,COALESCE(error,''),started_at,completed_at,COALESCE(parent_response_id,''),COALESCE(response_id,''),COALESCE(original_ref,''),COALESCE(upstream_ref,''),COALESCE(response_ref,''),truncated,COALESCE(metadata_json,''),COALESCE(facets_json,''),original_request_gz,upstream_request_gz,response_gz FROM records WHERE session_id=? ORDER BY started_at LIMIT ? OFFSET ?`, id, limit, offset)
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
		if e = rows.Scan(&x.RequestID, &x.TraceID, &x.KeyID, &x.Summary, &x.ResponsePreview, &x.SourceFormat, &x.RequestedModel, &x.Model, &stream, &x.Outcome, &x.StatusCode, &x.Error, &started, &done, &x.ParentResponseID, &x.ResponseID, &or, &ur, &rr, &trunc, &meta, &facets, &oldO, &oldU, &oldR); e != nil {
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
	err := s.DB.QueryRowContext(ctx, `SELECT session_id,request_id,trace_id,COALESCE(key_id,''),COALESCE(summary,''),COALESCE(response_preview,''),COALESCE(source_format,''),COALESCE(requested_model,''),COALESCE(model,''),stream,COALESCE(outcome,''),status_code,COALESCE(error,''),started_at,completed_at,COALESCE(parent_response_id,''),COALESCE(response_id,''),COALESCE(original_ref,''),COALESCE(upstream_ref,''),COALESCE(response_ref,''),truncated,COALESCE(metadata_json,''),COALESCE(facets_json,''),original_request_gz,upstream_request_gz,response_gz FROM records WHERE request_id=? LIMIT 1`, id).Scan(&sessionID, &x.RequestID, &x.TraceID, &x.KeyID, &x.Summary, &x.ResponsePreview, &x.SourceFormat, &x.RequestedModel, &x.Model, &stream, &x.Outcome, &x.StatusCode, &x.Error, &started, &done, &x.ParentResponseID, &x.ResponseID, &or, &ur, &rr, &trunc, &meta, &facets, &oldO, &oldU, &oldR)
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

// RequestContext returns the selected request and the following requests from
// the same durable session. Responses API tool outputs are commonly submitted
// in the next client request, so request-local inspection alone cannot pair a
// function_call with its function_call_output.
func (s *Store) RequestContext(ctx context.Context, id string, before, limit int) ([]Record, error) {
	if limit < 1 {
		limit = 8
	}
	if limit > 32 {
		limit = 32
	}
	if before < 0 {
		before = 0
	}
	if before > 4 {
		before = 4
	}
	var sessionID string
	var recordID int64
	if err := s.DB.QueryRowContext(ctx, `SELECT session_id,id FROM records WHERE request_id=? LIMIT 1`, id).Scan(&sessionID, &recordID); err != nil {
		return nil, err
	}
	ids := make([]string, 0, before+limit)
	if before > 0 {
		previousRows, previousErr := s.DB.QueryContext(ctx, `SELECT request_id FROM records WHERE session_id=? AND id<? ORDER BY id DESC LIMIT ?`, sessionID, recordID, before)
		if previousErr != nil {
			return nil, previousErr
		}
		var reversed []string
		for previousRows.Next() {
			var requestID string
			if previousErr = previousRows.Scan(&requestID); previousErr != nil {
				previousRows.Close()
				return nil, previousErr
			}
			reversed = append(reversed, requestID)
		}
		if previousErr = previousRows.Close(); previousErr != nil {
			return nil, previousErr
		}
		for index := len(reversed) - 1; index >= 0; index-- {
			ids = append(ids, reversed[index])
		}
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT request_id FROM records WHERE session_id=? AND id>=? ORDER BY id LIMIT ?`, sessionID, recordID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var requestID string
		if err = rows.Scan(&requestID); err != nil {
			return nil, err
		}
		ids = append(ids, requestID)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	out := make([]Record, 0, len(ids))
	for _, requestID := range ids {
		record, loadErr := s.Request(ctx, requestID)
		if loadErr != nil {
			return nil, loadErr
		}
		out = append(out, record)
	}
	return out, nil
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
	return s.ExportArchiveJSONL(ctx, id, dst)
}

// ExportArchiveJSONL streams the lossless normalized archive. An empty id
// exports the complete database in deterministic session/time order.
func (s *Store) ExportArchiveJSONL(ctx context.Context, id string, dst io.Writer) error {
	query := `SELECT request_id FROM records ORDER BY session_id,started_at,id`
	args := []any{}
	if id != "" {
		query = `SELECT request_id FROM records WHERE session_id=? ORDER BY started_at,id`
		args = append(args, id)
	}
	rows, err := s.DB.QueryContext(ctx, query, args...)
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
	// Read counts from the narrow projections maintained in the ingest
	// transaction. Scanning the payload-heavy records table is needlessly slow
	// on network-backed volumes after an attach or replica rebuild.
	_ = s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM session_indexed_requests`).Scan(&x.Records)
	_ = s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM session_summaries`).Scan(&x.Sessions)
	// blobs stores large payload chunks inline. COUNT(*) walks payload pages on
	// SQLite and becomes painfully slow on a degraded network volume. rowid is
	// monotonically allocated, so MAX(rowid) is an O(log n) operational
	// estimate; exact blob cardinality is not user-facing archive information.
	_ = s.DB.QueryRowContext(ctx, `SELECT COALESCE(MAX(rowid),0) FROM blobs`).Scan(&x.Blobs)
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
	tx, err := s.DB.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	reachable := map[string]bool{}
	rows, err := tx.Query(`SELECT original_ref FROM records WHERE original_ref!='' UNION SELECT upstream_ref FROM records WHERE upstream_ref!='' UNION SELECT response_ref FROM records WHERE response_ref!=''`)
	if err != nil {
		return 0, err
	}
	var queue []string
	for rows.Next() {
		var hash string
		if err = rows.Scan(&hash); err != nil {
			rows.Close()
			return 0, err
		}
		queue = append(queue, hash)
	}
	if err = rows.Close(); err != nil {
		return 0, err
	}
	for len(queue) > 0 {
		hash := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if reachable[hash] {
			continue
		}
		reachable[hash] = true
		var codec string
		var data []byte
		if err = tx.QueryRow(`SELECT codec,data FROM blobs WHERE hash=?`, hash).Scan(&codec, &data); err != nil {
			return 0, err
		}
		if codec == "gzip" {
			data = gunzipBytes(data)
		}
		queue = append(queue, nestedBlobRefs(data)...)
	}
	all, err := tx.Query(`SELECT hash FROM blobs`)
	if err != nil {
		return 0, err
	}
	var stale []string
	for all.Next() {
		var hash string
		if err = all.Scan(&hash); err != nil {
			all.Close()
			return 0, err
		}
		if !reachable[hash] {
			stale = append(stale, hash)
		}
	}
	if err = all.Close(); err != nil {
		return 0, err
	}
	for _, hash := range stale {
		if _, err = tx.Exec(`DELETE FROM blobs WHERE hash=?`, hash); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return int64(len(stale)), nil
}

func nestedBlobRefs(raw []byte) []string {
	var root any
	if json.Unmarshal(raw, &root) != nil {
		return nil
	}
	var out []string
	var walk func(any)
	walk = func(value any) {
		switch item := value.(type) {
		case []any:
			for _, child := range item {
				walk(child)
			}
		case map[string]any:
			if hash, ok := item["$cpa_blob"].(string); ok && hash != "" {
				out = append(out, hash)
				return
			}
			for _, child := range item {
				walk(child)
			}
		}
	}
	walk(root)
	return out
}

var _ = errors.Is
