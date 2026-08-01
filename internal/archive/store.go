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
	"strings"
	"time"
)

type Store struct {
	DB            *sql.DB
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
	schema := `CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,trace_id TEXT,session_id TEXT NOT NULL,key_id TEXT,source_format TEXT,requested_model TEXT,model TEXT,stream INTEGER,outcome TEXT,status_code INTEGER,error TEXT,started_at TEXT,completed_at TEXT,parent_response_id TEXT,response_id TEXT,original_request_gz BLOB,upstream_request_gz BLOB,response_gz BLOB,truncated INTEGER,metadata_json TEXT);CREATE TABLE IF NOT EXISTS record_facets(request_id TEXT NOT NULL,name TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(request_id,name,value));CREATE INDEX IF NOT EXISTS idx_facets_name_value ON record_facets(name,value);CREATE TABLE IF NOT EXISTS blobs(hash TEXT PRIMARY KEY,media_type TEXT,raw_size INTEGER NOT NULL,codec TEXT NOT NULL,data BLOB NOT NULL);CREATE INDEX IF NOT EXISTS idx_records_session_time ON records(session_id,started_at);CREATE INDEX IF NOT EXISTS idx_records_key_time ON records(key_id,started_at);CREATE INDEX IF NOT EXISTS idx_records_model_time ON records(requested_model,started_at);`
	if _, e = db.Exec(schema); e != nil {
		return nil, e
	}
	for _, q := range []string{"ALTER TABLE records ADD COLUMN original_ref TEXT", "ALTER TABLE records ADD COLUMN upstream_ref TEXT", "ALTER TABLE records ADD COLUMN response_ref TEXT", "ALTER TABLE records ADD COLUMN facets_json TEXT"} {
		_, _ = db.Exec(q)
	}
	s := &Store{DB: db, StoreUpstream: storeUpstream}
	go s.MigrateLegacy()
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
	st, e := tx.Prepare(`INSERT INTO records(request_id,trace_id,session_id,key_id,source_format,requested_model,model,stream,outcome,status_code,error,started_at,completed_at,parent_response_id,response_id,original_ref,upstream_ref,response_ref,truncated,metadata_json,facets_json,original_request_gz,upstream_request_gz,response_gz) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL) ON CONFLICT(request_id) DO UPDATE SET outcome=excluded.outcome,status_code=excluded.status_code,error=excluded.error,completed_at=excluded.completed_at,response_id=excluded.response_id,response_ref=excluded.response_ref,truncated=excluded.truncated`)
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
		if _, e = st.Exec(r.RequestID, r.TraceID, r.SessionID, r.KeyID, r.SourceFormat, r.RequestedModel, r.Model, r.Stream, r.Outcome, r.StatusCode, r.Error, r.StartedAt.Format(time.RFC3339Nano), r.CompletedAt.Format(time.RFC3339Nano), r.ParentResponseID, r.ResponseID, orig, up, resp, r.Truncated, string(m), facetsJSON(r.Facets)); e != nil {
			return e
		}
		if _, e = tx.Exec(`DELETE FROM record_facets WHERE request_id=?`, r.RequestID); e != nil { return e }
		for name, values := range r.Facets {
			for _, value := range values {
				if _, e = tx.Exec(`INSERT OR IGNORE INTO record_facets(request_id,name,value) VALUES(?,?,?)`, r.RequestID, name, value); e != nil { return e }
			}
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
func facetsJSON(v map[string][]string) string { b,_:=json.Marshal(v);return string(b) }

type FacetCount struct{Name string `json:"name"`;Value string `json:"value"`;Sessions int `json:"sessions"`}
func (s *Store) Facets(ctx context.Context)([]FacetCount,error){rows,e:=s.DB.QueryContext(ctx,`SELECT f.name,f.value,COUNT(DISTINCT r.session_id) FROM record_facets f JOIN records r ON r.request_id=f.request_id GROUP BY f.name,f.value ORDER BY f.name,COUNT(DISTINCT r.session_id) DESC LIMIT 5000`);if e!=nil{return nil,e};defer rows.Close();out:=[]FacetCount{};for rows.Next(){var x FacetCount;if e=rows.Scan(&x.Name,&x.Value,&x.Sessions);e!=nil{return nil,e};out=append(out,x)};return out,rows.Err()}
func (s *Store) Sessions(ctx context.Context, limit int) ([]SessionSummary, error) {
	rows, e := s.DB.QueryContext(ctx, `SELECT session_id,COUNT(*),MIN(started_at),MAX(completed_at),COALESCE(MAX(key_id),''),COALESCE(MAX(requested_model),'') FROM records GROUP BY session_id ORDER BY MAX(started_at) DESC LIMIT ?`, limit)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []SessionSummary{}
	for rows.Next() {
		var x SessionSummary
		if e = rows.Scan(&x.SessionID, &x.Requests, &x.FirstAt, &x.LastAt, &x.KeyID, &x.Model); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

func (s *Store) SessionsFiltered(ctx context.Context,limit int,filters map[string]string)([]SessionSummary,error){where:=[]string{};args:=[]any{};for name,value:=range filters{where=append(where,`EXISTS (SELECT 1 FROM record_facets f WHERE f.request_id=records.request_id AND f.name=? AND f.value=?)`);args=append(args,name,value)};q:=`SELECT session_id,COUNT(*),MIN(started_at),MAX(completed_at),COALESCE(MAX(key_id),''),COALESCE(MAX(requested_model),''),COALESCE((SELECT f.value FROM record_facets f JOIN records r2 ON r2.request_id=f.request_id WHERE r2.session_id=records.session_id AND f.name='project.name' LIMIT 1),'') FROM records`;if len(where)>0{q+=" WHERE "+strings.Join(where," AND ")};q+=` GROUP BY session_id ORDER BY MAX(started_at) DESC LIMIT ?`;args=append(args,limit);rows,e:=s.DB.QueryContext(ctx,q,args...);if e!=nil{return nil,e};defer rows.Close();out:=[]SessionSummary{};for rows.Next(){var x SessionSummary;if e=rows.Scan(&x.SessionID,&x.Requests,&x.FirstAt,&x.LastAt,&x.KeyID,&x.Model,&x.Project);e!=nil{return nil,e};out=append(out,x)};return out,rows.Err()}

func (s *Store) Session(ctx context.Context, id string) ([]Record, error) {
	rows, e := s.DB.QueryContext(ctx, `SELECT request_id,trace_id,COALESCE(key_id,''),COALESCE(source_format,''),COALESCE(requested_model,''),COALESCE(model,''),stream,COALESCE(outcome,''),status_code,COALESCE(error,''),started_at,completed_at,COALESCE(parent_response_id,''),COALESCE(response_id,''),COALESCE(original_ref,''),COALESCE(upstream_ref,''),COALESCE(response_ref,''),truncated,COALESCE(metadata_json,''),COALESCE(facets_json,''),original_request_gz,upstream_request_gz,response_gz FROM records WHERE session_id=? ORDER BY started_at`, id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Record{}
	for rows.Next() {
		var x Record
		var stream, trunc int
		var started, done, meta, facets, or, ur, rr string
		var oldO, oldU, oldR []byte
		x.SessionID = id
		if e = rows.Scan(&x.RequestID, &x.TraceID, &x.KeyID, &x.SourceFormat, &x.RequestedModel, &x.Model, &stream, &x.Outcome, &x.StatusCode, &x.Error, &started, &done, &x.ParentResponseID, &x.ResponseID, &or, &ur, &rr, &trunc, &meta, &facets, &oldO, &oldU, &oldR); e != nil {
			return nil, e
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
		out = append(out, x)
	}
	return out, rows.Err()
}
func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var x Stats
	_ = s.DB.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(DISTINCT session_id) FROM records`).Scan(&x.Records, &x.Sessions)
	_ = s.DB.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(raw_size),0),COALESCE(SUM(LENGTH(data)),0) FROM blobs`).Scan(&x.Blobs, &x.LogicalBytes, &x.CompressedBytes)
	var referenced int64
	_ = s.DB.QueryRowContext(ctx, `SELECT COALESCE(SUM(COALESCE(LENGTH(original_request_gz),0)+COALESCE(LENGTH(upstream_request_gz),0)+COALESCE(LENGTH(response_gz),0)),0) FROM records`).Scan(&referenced)
	x.SavedBytes = referenced + x.LogicalBytes - x.CompressedBytes
	return x, nil
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
