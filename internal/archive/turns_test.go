package archive

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSessionTurnPageGroupsCodexByTurnID(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	records := []Record{
		{RequestID: "r1", SessionID: "s", Summary: "First command", ResponsePreview: "工具调用：shell", StartedAt: now, CompletedAt: now, Facets: map[string][]string{"turn.id": {"turn-a"}, "tool.name": {"shell"}}},
		{RequestID: "r2", SessionID: "s", Summary: "First command", ResponsePreview: "Intermediate explanation", StartedAt: now.Add(time.Second), CompletedAt: now.Add(time.Second), Facets: map[string][]string{"turn.id": {"turn-a"}}},
		{RequestID: "r3", SessionID: "s", Summary: "First command", ResponsePreview: "Final answer", StartedAt: now.Add(2 * time.Second), CompletedAt: now.Add(2 * time.Second), Facets: map[string][]string{"turn.id": {"turn-a"}, "request.kind": {"compaction"}}},
		{RequestID: "r4", SessionID: "s", Summary: "Second command", ResponsePreview: "Second answer", StartedAt: now.Add(3 * time.Second), CompletedAt: now.Add(3 * time.Second), Facets: map[string][]string{"turn.id": {"turn-b"}}},
	}
	if err = store.PutBatch(records); err != nil {
		t.Fatal(err)
	}
	page, err := store.SessionTurnPage(context.Background(), "s", 20, 0, "asc")
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 2 || len(page.Turns) != 2 {
		t.Fatalf("page=%+v", page)
	}
	first := page.Turns[0]
	if first.TurnID != "turn-a" || first.Requests != 3 || first.UserText != "First command" || first.FinalText != "Final answer" || !first.HasCompaction {
		t.Fatalf("first=%+v", first)
	}
	if len(first.ToolNames) != 1 || first.ToolNames[0] != "shell" {
		t.Fatalf("tools=%v", first.ToolNames)
	}
}

func TestSessionTurnPageInfersKimiTurnsFromSummaryRuns(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	records := []Record{
		{RequestID: "k1", SessionID: "kimi", Summary: "&lt;userRequest&gt; Build feature A", ResponsePreview: "工具调用：read_file", StartedAt: now, CompletedAt: now},
		{RequestID: "k2", SessionID: "kimi", Summary: "<userRequest> Build feature A", ResponsePreview: "Feature A complete", StartedAt: now.Add(time.Second), CompletedAt: now.Add(time.Second)},
		{RequestID: "k3", SessionID: "kimi", Summary: "Review feature B", ResponsePreview: "Feature B reviewed", StartedAt: now.Add(2 * time.Second), CompletedAt: now.Add(2 * time.Second)},
	}
	if err = store.PutBatch(records); err != nil {
		t.Fatal(err)
	}
	page, err := store.SessionTurnPage(context.Background(), "kimi", 20, 0, "asc")
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 2 || page.Turns[0].Requests != 2 || page.Turns[0].FinalText != "Feature A complete" {
		t.Fatalf("page=%+v", page)
	}
	if !strings.HasPrefix(page.Turns[0].TurnID, "derived-") {
		t.Fatalf("turn id=%q", page.Turns[0].TurnID)
	}
}

func TestCleanTurnTextRemovesClientWrappers(t *testing.T) {
	for input, expected := range map[string]string{
		"## My request for Codex:\nDo the work": "Do the work",
		"&lt;userRequest&gt;Review this&lt;/userRequest&gt;": "Review this",
	} {
		if actual := cleanTurnText(input); actual != expected {
			t.Fatalf("cleanTurnText(%q)=%q, want %q", input, actual, expected)
		}
	}
}

func TestSessionTurnDetailRehydratesCompleteUserCommand(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	fullText := "You are an expert at upholding safety and compliance standards. " +
		strings.Repeat("This paragraph must remain available in the turn detail. ", 180) +
		"END-OF-COMPLETE-USER-COMMAND"
	body := []byte(`{"input":[{"role":"user","content":[{"type":"input_text","text":` + mustJSON(fullText) + `}]}]}`)
	now := time.Now()
	record := Record{
		RequestID:       "long-request",
		SessionID:       "long-session",
		Summary:         compactSummary(fullText),
		ResponsePreview: "Complete answer",
		OriginalRequest: body,
		StartedAt:       now,
		CompletedAt:     now,
		Facets:          map[string][]string{"turn.id": {"long-turn"}},
	}
	if err = store.PutBatch([]Record{record}); err != nil {
		t.Fatal(err)
	}
	page, err := store.SessionTurnPage(context.Background(), "long-session", 20, 0, "asc")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(page.Turns[0].UserText, "END-OF-COMPLETE-USER-COMMAND") {
		t.Fatal("list projection unexpectedly materialized the full command")
	}
	detail, err := store.SessionTurnDetail(context.Background(), "long-session", "long-turn", 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Turn.UserText != fullText {
		t.Fatalf("detail user text length=%d, want %d", len(detail.Turn.UserText), len(fullText))
	}
	if !strings.HasSuffix(detail.Turn.UserText, "END-OF-COMPLETE-USER-COMMAND") {
		t.Fatal("detail did not preserve the end of the complete user command")
	}
}

func mustJSON(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
