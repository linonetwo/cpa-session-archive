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

func TestExtractConversationSummary(t *testing.T) {
	body := []byte(`{"input":[{"role":"developer","content":[{"type":"input_text","text":"ignore"}]},{"role":"user","content":[{"type":"input_text","text":"  Build   the archive browser  "}]}]}`)
	if got := extractConversationSummary(body); got != "Build the archive browser" { t.Fatalf("summary=%q", got) }
}
