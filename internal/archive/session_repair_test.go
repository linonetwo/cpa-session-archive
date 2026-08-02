package archive

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
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
