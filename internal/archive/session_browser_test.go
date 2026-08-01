package archive

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSessionBrowserSummaryKeyAndPagination(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil { t.Fatal(err) }
	defer store.DB.Close()
	now := time.Now()
	large := []byte(`{"input":[{"role":"user","content":[{"type":"input_text","text":"a long request body used for preview truncation"}]}]}`)
	records := []Record{
		{RequestID: "r1", SessionID: "s1", Summary: "First useful request", StartedAt: now, CompletedAt: now, OriginalRequest: large, Facets: map[string][]string{"project.name": {"repo"}, "caller.scope": {"hash-key"}}},
		{RequestID: "r2", SessionID: "s1", Summary: "Later request", StartedAt: now.Add(time.Second), CompletedAt: now.Add(time.Second), OriginalRequest: large, Facets: map[string][]string{"project.name": {"repo"}, "caller.scope": {"hash-key"}}},
	}
	if err = store.PutBatch(records); err != nil { t.Fatal(err) }
	sessions, err := store.SessionsFiltered(context.Background(), 10, map[string]string{"project.name": "repo"})
	if err != nil { t.Fatal(err) }
	if len(sessions) != 1 || sessions[0].Summary != "First useful request" || sessions[0].KeyID != "hash-key" { t.Fatalf("sessions=%+v", sessions) }
	page, err := store.SessionRange(context.Background(), "s1", 1, 0, 16)
	if err != nil { t.Fatal(err) }
	if page.Total != 2 || len(page.Records) != 1 || len(page.Records[0].OriginalRequest) != 16 || !page.Records[0].Truncated { t.Fatalf("page=%+v", page) }
}

func TestSessionsFilteredKeepsNewestSessionsAndFirstSummary(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil { t.Fatal(err) }
	defer store.DB.Close()
	now := time.Now()
	records := []Record{
		{RequestID: "old-1", SessionID: "old", Summary: "Old session first request", StartedAt: now, CompletedAt: now, Facets: map[string][]string{"request.kind": {"turn"}}},
		{RequestID: "old-2", SessionID: "old", Summary: "Old session later request", StartedAt: now.Add(time.Second), CompletedAt: now.Add(time.Second), Facets: map[string][]string{"request.kind": {"turn"}}},
		{RequestID: "new-1", SessionID: "new", Summary: "New session", StartedAt: now.Add(2 * time.Second), CompletedAt: now.Add(2 * time.Second), Facets: map[string][]string{"request.kind": {"compaction"}}},
	}
	if err = store.PutBatch(records); err != nil { t.Fatal(err) }
	sessions, err := store.SessionsFiltered(context.Background(), 1, map[string]string{})
	if err != nil { t.Fatal(err) }
	if len(sessions) != 1 || sessions[0].SessionID != "new" { t.Fatalf("newest sessions=%+v", sessions) }
	turns, err := store.SessionsFiltered(context.Background(), 10, map[string]string{"request.kind": "turn"})
	if err != nil { t.Fatal(err) }
	if len(turns) != 1 || turns[0].Requests != 2 || turns[0].Summary != "Old session first request" { t.Fatalf("turn sessions=%+v", turns) }
}

func TestExtractConversationSummary(t *testing.T) {
	body := []byte(`{"input":[{"role":"developer","content":[{"type":"input_text","text":"ignore"}]},{"role":"user","content":[{"type":"input_text","text":"  Build   the archive browser  "}]}]}`)
	if got := extractConversationSummary(body); got != "Build the archive browser" { t.Fatalf("summary=%q", got) }
}
