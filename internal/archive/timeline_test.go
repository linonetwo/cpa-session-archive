package archive

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRequestTimelineOnlyShowsNewTurnDelta(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	records := []Record{
		{RequestID: "first", SessionID: "session", StartedAt: now, CompletedAt: now, OriginalRequest: []byte(`{"input":[{"role":"user","content":"hello"}]}`), Response: []byte(`{"output":[{"type":"message","role":"assistant","content":"first reply"},{"type":"function_call","call_id":"call-1","name":"shell","arguments":"{}"}]}`)},
		{RequestID: "second", SessionID: "session", StartedAt: now.Add(time.Second), CompletedAt: now.Add(time.Second), OriginalRequest: []byte(`{"input":[{"role":"user","content":"hello"},{"type":"message","role":"assistant","content":"first reply"},{"type":"function_call","call_id":"call-1","name":"shell","arguments":"{}"},{"type":"function_call_output","call_id":"call-1","output":"done"},{"role":"user","content":"continue"}]}`), Response: []byte(`{"output":[{"type":"message","role":"assistant","content":"second reply"}]}`)},
	}
	if err = store.PutBatch(records); err != nil {
		t.Fatal(err)
	}
	view, err := store.RequestTimeline(context.Background(), "second")
	if err != nil {
		t.Fatal(err)
	}
	joined := ""
	for _, entry := range view.Entries {
		joined += entry.Role + ":" + entry.Text + "\n"
	}
	if strings.Contains(joined, "hello") || strings.Contains(joined, "first reply") {
		t.Fatalf("replayed history leaked into delta:\n%s", joined)
	}
	for _, want := range []string{"tool_result:done", "user:continue", "assistant:second reply"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("timeline missing %q:\n%s", want, joined)
		}
	}
}

func TestCollectTimelineDecodesEscapedReadableText(t *testing.T) {
	var entries []TimelineEntry
	collectTimeline([]any{
		map[string]any{
			"type":    "message",
			"role":    "user",
			"content": `&amp;lt;environment_context&amp;gt;cwd&amp;lt;/environment_context&amp;gt;`,
		},
	}, "user", &entries)
	if len(entries) != 1 || entries[0].Text != `<environment_context>cwd</environment_context>` {
		t.Fatalf("entries=%#v", entries)
	}
}

func TestRequestTimelineMarksCompaction(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	record := Record{RequestID: "compact", SessionID: "session", StartedAt: now, CompletedAt: now, OriginalRequest: []byte(`{"input":[{"type":"compaction_trigger"}]}`)}
	if err = store.PutBatch([]Record{record}); err != nil {
		t.Fatal(err)
	}
	view, err := store.RequestTimeline(context.Background(), "compact")
	if err != nil {
		t.Fatal(err)
	}
	if view.Kind != "compaction" {
		t.Fatalf("kind=%q", view.Kind)
	}
}

func TestRequestTimelineDoesNotReuseHistoricalCompactionLabel(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	records := []Record{
		{RequestID: "compacted", SessionID: "session", StartedAt: now, CompletedAt: now, OriginalRequest: []byte(`{"input":[{"type":"compaction_trigger"}]}`)},
		{RequestID: "later", SessionID: "session", StartedAt: now.Add(time.Second), CompletedAt: now.Add(time.Second), OriginalRequest: []byte(`{"input":[{"type":"compaction_trigger"},{"role":"user","content":"new turn"}]}`)},
	}
	if err = store.PutBatch(records); err != nil {
		t.Fatal(err)
	}
	view, err := store.RequestTimeline(context.Background(), "later")
	if err != nil {
		t.Fatal(err)
	}
	if view.Kind != "turn" {
		t.Fatalf("kind=%q", view.Kind)
	}
}

func TestConversationSummaryOnlyUsesExplicitUserMessages(t *testing.T) {
	body := []byte(`{"input":[{"role":"user","content":"real user request"},{"role":"assistant","content":"wrong assistant title"}]}`)
	if got := extractConversationSummary(body); got != "real user request" {
		t.Fatalf("summary=%q", got)
	}
}

func TestConversationSummarySkipsContextWrappers(t *testing.T) {
	body := []byte(`{"input":[{"role":"user","content":"<context>machine metadata</context><environment_context>cwd</environment_context>calibre 有 API 吗，我这里有两个文件夹"}]}`)
	if got := extractConversationSummary(body); got != "calibre 有 API 吗，我这里有两个文件夹" {
		t.Fatalf("summary=%q", got)
	}
}

func TestConversationSummaryFirstKeepsStableSessionOpening(t *testing.T) {
	body := []byte(`{"input":[{"role":"user","content":"first real request"},{"role":"assistant","content":"reply"},{"role":"user","content":"latest turn"}]}`)
	if got := extractConversationSummary(body); got != "latest turn" {
		t.Fatalf("request summary=%q", got)
	}
	if got := extractConversationSummaryFirst(body); got != "first real request" {
		t.Fatalf("session summary=%q", got)
	}
}

func TestConversationSummaryExtractsEmbeddedTitlePrompt(t *testing.T) {
	body := []byte(`{"input":[{"role":"user","content":"Generate a concise UI title.\n\nUser prompt:\ncalibre 有 API 吗，我这里有两个文件夹"}]}`)
	if got := extractConversationSummary(body); got != "calibre 有 API 吗，我这里有两个文件夹" {
		t.Fatalf("summary=%q", got)
	}
}

func TestConversationSummaryUnwrapsEscapedUserRequest(t *testing.T) {
	body := []byte(`{"input":[{"role":"user","content":"&lt;userRequest&gt; 继续处理归档 &lt;/userRequest&gt;"}]}`)
	if got := extractConversationSummary(body); got != "继续处理归档" {
		t.Fatalf("summary=%q", got)
	}
}

func TestExtractThreadSourceFromEmbeddedTurnMetadata(t *testing.T) {
	body := []byte(`{"client_metadata":{"x-codex-turn-metadata":"{\"thread_source\":\"system\"}"}}`)
	if got := extractThreadSource(body); got != "system" {
		t.Fatalf("thread source=%q", got)
	}
}
