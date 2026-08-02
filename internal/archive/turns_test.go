package archive

import (
	"context"
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
