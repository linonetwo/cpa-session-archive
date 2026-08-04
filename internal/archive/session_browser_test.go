package archive

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSessionBrowserSummaryKeyAndPagination(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	large := []byte(`{"input":[{"role":"user","content":[{"type":"input_text","text":"a long request body used for preview truncation"}]}]}`)
	records := []Record{
		{RequestID: "r1", SessionID: "s1", Summary: "First useful request", StartedAt: now, CompletedAt: now, OriginalRequest: large, Facets: map[string][]string{"project.name": {"repo"}, "caller.scope": {"hash-key"}}},
		{RequestID: "r2", SessionID: "s1", Summary: "Later request", StartedAt: now.Add(time.Second), CompletedAt: now.Add(time.Second), OriginalRequest: large, Facets: map[string][]string{"project.name": {"repo"}, "caller.scope": {"hash-key"}}},
	}
	if err = store.PutBatch(records); err != nil {
		t.Fatal(err)
	}
	sessions, err := store.SessionsFiltered(context.Background(), 10, map[string]string{"project.name": "repo"})
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].Summary != "First useful request" || sessions[0].KeyID != "hash-key" {
		t.Fatalf("sessions=%+v", sessions)
	}
	page, err := store.SessionRange(context.Background(), "s1", 1, 0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 2 || len(page.Records) != 1 || len(page.Records[0].OriginalRequest) != 16 || !page.Records[0].Truncated {
		t.Fatalf("page=%+v", page)
	}
}

func TestSessionsFilteredKeepsNewestSessionsAndFirstSummary(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	records := []Record{
		{RequestID: "old-1", SessionID: "old", Summary: "Old session first request", StartedAt: now, CompletedAt: now, Facets: map[string][]string{"request.kind": {"turn"}, "thread.source": {"user"}}},
		{RequestID: "old-2", SessionID: "old", Summary: "Old session later request", StartedAt: now.Add(time.Second), CompletedAt: now.Add(time.Second), Facets: map[string][]string{"request.kind": {"turn"}}},
		{RequestID: "new-1", SessionID: "new", Summary: "New session", StartedAt: now.Add(2 * time.Second), CompletedAt: now.Add(2 * time.Second), Facets: map[string][]string{"request.kind": {"compaction"}}},
	}
	if err = store.PutBatch(records); err != nil {
		t.Fatal(err)
	}
	sessions, err := store.SessionsFiltered(context.Background(), 1, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].SessionID != "new" {
		t.Fatalf("newest sessions=%+v", sessions)
	}
	turns, err := store.SessionsFiltered(context.Background(), 10, map[string]string{"request.kind": "turn"})
	if err != nil {
		t.Fatal(err)
	}
	if len(turns) != 1 || turns[0].Requests != 2 || turns[0].Summary != "Old session first request" {
		t.Fatalf("turn sessions=%+v", turns)
	}
	if len(turns[0].Kinds) != 1 || turns[0].Kinds[0] != "turn" || len(turns[0].ThreadSources) != 1 || turns[0].ThreadSources[0] != "user" {
		t.Fatalf("session metadata=%+v", turns[0])
	}
}

func TestSessionsDecodeRepeatedHTMLEntitiesForDisplay(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	if err = store.PutBatch([]Record{{
		RequestID:   "encoded",
		SessionID:   "encoded-session",
		Summary:     `calibre &amp;#34;library&amp;#34;`,
		StartedAt:   now,
		CompletedAt: now,
	}}); err != nil {
		t.Fatal(err)
	}
	sessions, err := store.Sessions(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].Summary != `calibre "library"` {
		t.Fatalf("sessions=%+v", sessions)
	}
}

func TestExtractConversationSummary(t *testing.T) {
	body := []byte(`{"input":[{"role":"developer","content":[{"type":"input_text","text":"ignore"}]},{"role":"user","content":[{"type":"input_text","text":"  Build   the archive browser  "}]}]}`)
	if got := extractConversationSummary(body); got != "Build the archive browser" {
		t.Fatalf("summary=%q", got)
	}
}

func TestExtractConversationSummarySkipsEnvironmentBoilerplate(t *testing.T) {
	body := []byte(`{"input":[{"role":"user","content":[{"type":"input_text","text":"<environment_context>workspace</environment_context>"},{"type":"input_text","text":"Investigate the real failure"}]}]}`)
	if got := extractConversationSummary(body); got != "Investigate the real failure" {
		t.Fatalf("summary=%q", got)
	}
}

func TestExtractConversationSummaryFromStringInput(t *testing.T) {
	if got := extractConversationSummary([]byte(`{"input":"Summarize this repository"}`)); got != "Summarize this repository" {
		t.Fatalf("summary=%q", got)
	}
}

func TestExtractConversationSummaryStripsAmbientBrowserContext(t *testing.T) {
	body := []byte(`{"input":"<in-app-browser-context source=\"ambient-ui-state\">ignore this</in-app-browser-context>\n\nInvestigate the visible archive duplicates"}`)
	if got := extractConversationSummary(body); got != "Investigate the visible archive duplicates" {
		t.Fatalf("summary=%q", got)
	}
}

func TestExtractConversationSummaryStripsFilePreamble(t *testing.T) {
	body := []byte(`{"input":"# Files mentioned by the user:\n\n- screenshot.png\n\n## My request for Codex:\n\nExplain this failure"}`)
	if got := extractConversationSummary(body); got != "Explain this failure" {
		t.Fatalf("summary=%q", got)
	}
}

func TestRepairSessionSummaries(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	body := []byte(`{"input":[{"role":"user","content":[{"type":"input_text","text":"<environment_context>workspace</environment_context>"},{"type":"input_text","text":"Explain the actual outage"}]}]}`)
	if err = store.PutBatch([]Record{{RequestID: "bad-summary", SessionID: "session", Summary: "<environment_context>workspace</environment_context>", StartedAt: now, CompletedAt: now, OriginalRequest: body}}); err != nil {
		t.Fatal(err)
	}
	if err = store.RepairSessionSummaries(context.Background()); err != nil {
		t.Fatal(err)
	}
	sessions, err := store.SessionsFiltered(context.Background(), 10, map[string]string{})
	if err != nil || len(sessions) != 1 || sessions[0].Summary != "Explain the actual outage" {
		t.Fatalf("sessions=%+v err=%v", sessions, err)
	}
}

func TestBackfillSessionProjection(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	if err = store.PutBatch([]Record{{RequestID: "legacy", SessionID: "session", Summary: "Legacy session", KeyID: "key", RequestedModel: "model", StartedAt: now, CompletedAt: now, Facets: map[string][]string{"project.name": {"repo"}}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec(`DELETE FROM session_facets; DELETE FROM session_summaries; DELETE FROM session_indexed_requests`); err != nil {
		t.Fatal(err)
	}
	if err = store.BackfillSessionIndex(context.Background()); err != nil {
		t.Fatal(err)
	}
	sessions, err := store.SessionsFiltered(context.Background(), 10, map[string]string{"project.name": "repo"})
	if err != nil || len(sessions) != 1 || sessions[0].Summary != "Legacy session" || sessions[0].KeyID != "key" {
		t.Fatalf("sessions=%+v err=%v", sessions, err)
	}
}
