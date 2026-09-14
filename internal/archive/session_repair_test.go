package archive

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSessionIDPrefersDurableThreadOverExecution(t *testing.T) {
	rec := Record{ThreadID: "019f8c0c-66f7-7002-9fb6-7852ee3ca2cb"}
	got := sessionID(&rec, map[string]any{"execution_session_id": "f1a21cae-ae86-4a59-97b9-fbcbd8875e0e"})
	if got != rec.ThreadID {
		t.Fatalf("got %q want durable thread %q", got, rec.ThreadID)
	}
	h := http.Header{"X-Codex-Turn-Metadata": []string{`{"session_id":"stable-session","thread_id":"stable-thread"}`}}
	enrichDesktopMetadata(&rec, h)
	if rec.ThreadID != "stable-thread" {
		t.Fatalf("turn metadata thread not preferred: %#v", rec)
	}
}

func TestExportSessionJSONLIsCompleteAndStructured(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	for i := 0; i < 2; i++ {
		r := Record{RequestID: string(rune('a' + i)), SessionID: "session", StartedAt: now.Add(time.Duration(i) * time.Second), CompletedAt: now, OriginalRequest: []byte(`{"input":[{"role":"user","content":"hello"}]}`), Response: []byte(`{"output":[{"role":"assistant","content":"world"}]}`)}
		if err = store.PutBatch([]Record{r}); err != nil {
			t.Fatal(err)
		}
	}
	var out bytes.Buffer
	if err = store.ExportSessionJSONL(context.Background(), "session", &out); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(&out)
	count := 0
	for scanner.Scan() {
		var item map[string]any
		if err = json.Unmarshal(scanner.Bytes(), &item); err != nil {
			t.Fatal(err)
		}
		if _, ok := item["request"].(map[string]any); !ok {
			t.Fatalf("request was not structured JSON: %#v", item["request"])
		}
		if _, exists := item["original_request"]; exists {
			t.Fatal("legacy base64 field leaked into training export")
		}
		count++
	}
	if err = scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("exported %d records, want 2", count)
	}
}

func TestExportSessionJSONLSeeksAcrossEqualTimestampBatchBoundary(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now().UTC()
	for i := 0; i < archiveExportBatchSize+2; i++ {
		record := Record{RequestID: fmt.Sprintf("request-%03d", i), SessionID: "session", StartedAt: now, CompletedAt: now, OriginalRequest: []byte(`{"input":"request"}`), Response: []byte(`{"output":"response"}`)}
		if err = store.PutBatch([]Record{record}); err != nil {
			t.Fatal(err)
		}
	}
	var legacy bytes.Buffer
	if err = store.ExportArchiveJSONL(context.Background(), "", &legacy); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = store.ExportSessionJSONL(context.Background(), "session", &out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), legacy.Bytes()) {
		t.Fatal("bounded session export changed JSONL bytes")
	}
	seen := map[string]bool{}
	scanner := bufio.NewScanner(&out)
	for scanner.Scan() {
		var item struct {
			RequestID string `json:"request_id"`
		}
		if err = json.Unmarshal(scanner.Bytes(), &item); err != nil {
			t.Fatal(err)
		}
		if item.RequestID == "" || seen[item.RequestID] {
			t.Fatalf("duplicate or empty request id %q", item.RequestID)
		}
		seen[item.RequestID] = true
	}
	if err = scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != archiveExportBatchSize+2 {
		t.Fatalf("exported %d records, want %d", len(seen), archiveExportBatchSize+2)
	}
}

func TestArchiveExportMemoryBoundaries(t *testing.T) {
	if archiveExportBatchShouldStop(archiveExportBatchBytes - 1) {
		t.Fatal("batch stopped below byte waterline")
	}
	if !archiveExportBatchShouldStop(archiveExportBatchBytes) {
		t.Fatal("batch did not stop at byte waterline")
	}
	if !archivePayloadCacheCanStore(archiveExportCacheBytes-archiveExportCacheEntryBytes, archiveExportCacheEntryBytes) {
		t.Fatal("cache rejected an entry that exactly fits")
	}
	if archivePayloadCacheCanStore(archiveExportCacheBytes, 1) {
		t.Fatal("cache accepted bytes beyond its total bound")
	}
	if archivePayloadCacheCanStore(0, archiveExportCacheEntryBytes+1) {
		t.Fatal("cache accepted an oversized entry")
	}
}

type countingQueryContext struct {
	db    *sql.DB
	query int
}

func (q *countingQueryContext) QueryContext(ctx context.Context, statement string, args ...any) (*sql.Rows, error) {
	q.query++
	return q.db.QueryContext(ctx, statement, args...)
}

func (q *countingQueryContext) QueryRowContext(ctx context.Context, statement string, args ...any) *sql.Row {
	return q.db.QueryRowContext(ctx, statement, args...)
}

func TestStableSessionArchivePrefetchesReachableSmallCASBlobs(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()

	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		payload := []byte(`{"input":"` + strings.Repeat(string(rune('a'+i)), inlineThreshold+1) + `"}`)
		response := []byte(`{"output":"` + strings.Repeat(string(rune('d'+i)), inlineThreshold+1) + `"}`)
		if err = store.PutBatch([]Record{{
			RequestID:       fmt.Sprintf("prefetch-%d", i),
			SessionID:       "session",
			StartedAt:       now.Add(time.Duration(i) * time.Second),
			CompletedAt:     now.Add(time.Duration(i) * time.Second),
			OriginalRequest: payload,
			Response:        response,
		}}); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := store.DB.Query(`SELECT id,session_id,request_id,trace_id,COALESCE(key_id,''),COALESCE(principal_id,''),COALESCE(credential_hash,''),COALESCE((SELECT alias FROM credential_principals p WHERE p.principal_id=records.principal_id AND alias<>'' ORDER BY updated_at DESC LIMIT 1),''),COALESCE(summary,''),COALESCE(response_preview,''),COALESCE(source_format,''),COALESCE(requested_model,''),COALESCE(model,''),stream,COALESCE(outcome,''),status_code,COALESCE(error,''),started_at,completed_at,COALESCE(parent_response_id,''),COALESCE(response_id,''),COALESCE(original_ref,''),COALESCE(upstream_ref,''),COALESCE(response_ref,''),truncated,COALESCE(metadata_json,''),COALESCE(facets_json,''),original_request_gz,upstream_request_gz,response_gz FROM records WHERE session_id=? ORDER BY started_at,id`, "session")
	if err != nil {
		t.Fatal(err)
	}
	var batch []archiveExportRow
	for rows.Next() {
		item, scanErr := scanArchiveExportRow(rows)
		if scanErr != nil {
			_ = rows.Close()
			t.Fatal(scanErr)
		}
		batch = append(batch, item)
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}

	q := &countingQueryContext{db: store.DB}
	cache := archivePayloadCache{values: map[string][]byte{}}
	if err = prefetchArchivePayloads(context.Background(), q, batch, &cache); err != nil {
		t.Fatal(err)
	}
	// Roots and their nested small values use one metadata query plus one value
	// query per manifest depth, rather than a point query for every reference.
	if q.query > 4 {
		t.Fatalf("prefetch made %d queries, want at most four bounded batch queries", q.query)
	}
	if cache.bytes > archiveExportCacheBytes {
		t.Fatalf("cache retained %d bytes, bound is %d", cache.bytes, archiveExportCacheBytes)
	}

	var legacy, bounded bytes.Buffer
	if err = store.ExportArchiveJSONL(context.Background(), "", &legacy); err != nil {
		t.Fatal(err)
	}
	if err = store.ExportSessionJSONL(context.Background(), "session", &bounded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(legacy.Bytes(), bounded.Bytes()) {
		t.Fatal("prefetched bounded export changed JSONL bytes")
	}
}

func TestStableSessionArchiveDoesNotReadDiscardedUpstreamPayload(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now().UTC()
	if err = store.PutBatch([]Record{{RequestID: "no-upstream-read", SessionID: "session", StartedAt: now, CompletedAt: now, OriginalRequest: []byte(`{"input":"ok"}`), Response: []byte(`{"output":"ok"}`)}}); err != nil {
		t.Fatal(err)
	}
	// A historical upstream reference can be absent or corrupt without changing
	// the archive schema, because TrainingRecord does not emit it.
	if _, err = store.DB.Exec(`UPDATE records SET upstream_ref=? WHERE request_id=?`, "sha256:not-present", "no-upstream-read"); err != nil {
		t.Fatal(err)
	}
	if err = store.ExportSessionJSONL(context.Background(), "session", &bytes.Buffer{}); err != nil {
		t.Fatalf("unused upstream reference blocked export: %v", err)
	}
}

func TestStableSessionArchiveSeekPlanUsesSessionTimeIndexWithoutSort(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	rows, err := store.DB.Query(`EXPLAIN QUERY PLAN SELECT id,session_id,request_id,started_at FROM records WHERE session_id=? AND (started_at>? OR (started_at=? AND id>?)) ORDER BY started_at,id LIMIT ?`, "session", "", "", 0, archiveExportBatchSize)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	indexed := false
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(detail, "idx_records_session_time") {
			indexed = true
		}
		if strings.Contains(detail, "USE TEMP B-TREE") {
			t.Fatalf("stable session seek plan sorts with temp b-tree: %s", detail)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !indexed {
		t.Fatal("stable session seek plan did not use idx_records_session_time")
	}
}

func TestTrainingExportProducesConversationalToolCallingJSONL(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	request := []byte(`{"model":"gpt","input":[{"role":"user","content":[{"type":"input_text","text":"inspect the repo"}]},{"type":"function_call","call_id":"call-1","name":"shell","arguments":"{\"command\":\"rg TODO\"}"},{"type":"function_call_output","call_id":"call-1","output":"no matches"}],"tools":[{"type":"function","name":"shell","parameters":{"type":"object"}}]}`)
	response := []byte(`{"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"The repository is clean."}]}]}`)
	record := Record{RequestID: "training", SessionID: "session", Outcome: "succeeded", StatusCode: 200, StartedAt: now, CompletedAt: now, OriginalRequest: request, Response: response}
	if err = store.PutBatch([]Record{record}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = store.ExportTrainingJSONL(context.Background(), "session", &out); err != nil {
		t.Fatal(err)
	}
	var example SFTExample
	if err = json.Unmarshal(bytes.TrimSpace(out.Bytes()), &example); err != nil {
		t.Fatalf("invalid JSONL: %v\n%s", err, out.String())
	}
	if len(example.Messages) < 4 {
		t.Fatalf("missing tool conversation: %#v", example.Messages)
	}
	if example.Tools == nil {
		t.Fatal("tools schema missing")
	}
	if bytes.Contains(out.Bytes(), []byte("schema_version")) {
		t.Fatal("archive metadata leaked into SFT example")
	}
}

func TestTrainingExampleShapeWithoutStore(t *testing.T) {
	request := []byte(`{"input":[{"role":"user","content":"inspect"},{"type":"function_call","call_id":"call-1","name":"shell","arguments":"{}"},{"type":"function_call_output","call_id":"call-1","output":"ok"}],"tools":[{"type":"function","name":"shell","parameters":{"type":"object"}}]}`)
	response := []byte(`{"output":[{"type":"message","role":"assistant","content":"done"}]}`)
	example, ok := trainingExample(Record{OriginalRequest: request, Response: response})
	if !ok || len(example.Messages) != 4 {
		t.Fatalf("example=%#v ok=%v", example, ok)
	}
	tools, ok := example.Tools.([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("tools=%#v", example.Tools)
	}
	wrapped := tools[0].(map[string]any)
	if wrapped["function"] == nil {
		t.Fatalf("OpenAI function wrapper missing: %#v", wrapped)
	}
}

func TestRepairCanonicalSessionsMergesTransientExecutions(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	for i, id := range []string{"execution-a", "execution-b"} {
		r := Record{RequestID: id, SessionID: id, StartedAt: now.Add(time.Duration(i) * time.Second), CompletedAt: now.Add(time.Duration(i) * time.Second), Facets: map[string][]string{"thread.id": {"stable-thread"}, "session.id": {id}}}
		if err = store.PutBatch([]Record{r}); err != nil {
			t.Fatal(err)
		}
	}
	changed, err := store.RepairCanonicalSessions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if changed != 2 {
		t.Fatalf("changed=%d", changed)
	}
	sessions, err := store.Sessions(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].SessionID != "stable-thread" || sessions[0].Requests != 2 {
		t.Fatalf("unexpected merged projection: %#v", sessions)
	}
	var executions int
	if err = store.DB.QueryRow(`SELECT COUNT(*) FROM record_facets WHERE name='execution.session.id'`).Scan(&executions); err != nil {
		t.Fatal(err)
	}
	if executions != 2 {
		t.Fatalf("execution facets=%d", executions)
	}
	var version int
	if err = store.DB.QueryRow(`SELECT version FROM repair_versions WHERE name='canonical_session'`).Scan(&version); err != nil || version != 1 {
		t.Fatalf("canonical repair version=%d err=%v", version, err)
	}
	changed, err = store.RepairCanonicalSessions(context.Background())
	if err != nil || changed != 0 {
		t.Fatalf("completed repair reran: changed=%d err=%v", changed, err)
	}
}

func TestRepairRecordPreviewsResumesByExtractorVersion(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	records := []Record{
		{RequestID: "already-current", SessionID: "session", Summary: "keep current", StartedAt: now, CompletedAt: now, OriginalRequest: []byte(`{"input":[{"role":"user","content":"should not replace"}]}`)},
		{RequestID: "needs-repair", SessionID: "session", Summary: "old wrapper", StartedAt: now.Add(time.Second), CompletedAt: now.Add(time.Second), OriginalRequest: []byte(`{"input":[{"role":"user","content":"Generate a title.\n\nUser prompt:\nactual request"}]}`)},
	}
	if err = store.PutBatch(records); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec(`INSERT INTO previewed_requests(request_id,version) VALUES('already-current',4),('needs-repair',1)`); err != nil {
		t.Fatal(err)
	}
	if err = store.RepairRecordPreviews(context.Background()); err != nil {
		t.Fatal(err)
	}
	var kept, repaired string
	if err = store.DB.QueryRow(`SELECT summary FROM records WHERE request_id='already-current'`).Scan(&kept); err != nil {
		t.Fatal(err)
	}
	if err = store.DB.QueryRow(`SELECT summary FROM records WHERE request_id='needs-repair'`).Scan(&repaired); err != nil {
		t.Fatal(err)
	}
	if kept != "keep current" || repaired != "actual request" {
		t.Fatalf("kept=%q repaired=%q", kept, repaired)
	}
	var version int
	if err = store.DB.QueryRow(`SELECT version FROM previewed_requests WHERE request_id='needs-repair'`).Scan(&version); err != nil || version != 4 {
		t.Fatalf("version=%d err=%v", version, err)
	}
}

func TestRepairRecordPreviewsBackfillsThreadSource(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	body := []byte(`{"client_metadata":{"x-codex-turn-metadata":"{\"thread_source\":\"system\"}"},"input":[{"role":"user","content":"User prompt:\nreal task"}]}`)
	if err = store.PutBatch([]Record{{RequestID: "system", SessionID: "session", StartedAt: now, CompletedAt: now, OriginalRequest: body}}); err != nil {
		t.Fatal(err)
	}
	if err = store.RepairRecordPreviews(context.Background()); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = store.DB.QueryRow(`SELECT COUNT(*) FROM record_facets WHERE request_id='system' AND name='thread.source' AND value='system'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("record facet count=%d err=%v", count, err)
	}
	sessions, err := store.SessionsFiltered(context.Background(), 10, map[string]string{"thread.source": "system"})
	if err != nil || len(sessions) != 1 || len(sessions[0].ThreadSources) != 1 || sessions[0].ThreadSources[0] != "system" {
		t.Fatalf("sessions=%+v err=%v", sessions, err)
	}
}

func TestMigrateLegacyReturnsCorruptPayloadErrorWithoutMutation(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	if err = store.PutBatch([]Record{{
		RequestID: "corrupt-legacy", SessionID: "session", StartedAt: now, CompletedAt: now,
		OriginalRequest: []byte(`{"input":"valid before corruption"}`),
	}}); err != nil {
		t.Fatal(err)
	}
	corrupt := []byte("not-a-gzip-stream")
	if _, err = store.DB.Exec(`UPDATE records SET original_ref='',original_request_gz=? WHERE request_id='corrupt-legacy'`, corrupt); err != nil {
		t.Fatal(err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err = store.MigrateLegacy(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled migration error=%v", err)
	}
	if err = store.MigrateLegacy(context.Background()); err == nil {
		t.Fatal("corrupt legacy gzip was accepted")
	}
	var originalRef string
	var legacy []byte
	if err = store.DB.QueryRow(`SELECT COALESCE(original_ref,''),original_request_gz FROM records WHERE request_id='corrupt-legacy'`).Scan(&originalRef, &legacy); err != nil {
		t.Fatal(err)
	}
	if originalRef != "" || !bytes.Equal(legacy, corrupt) {
		t.Fatalf("failed migration mutated record: original_ref=%q legacy=%q", originalRef, legacy)
	}
}
