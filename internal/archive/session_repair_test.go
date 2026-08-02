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
