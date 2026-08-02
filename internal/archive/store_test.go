package archive

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestFacetIndexAndFiltering(t *testing.T) {
	s, e := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if e != nil {
		t.Fatal(e)
	}
	defer s.DB.Close()
	now := time.Now()
	records := []Record{{RequestID: "r1", SessionID: "s1", RequestedModel: "gpt", Outcome: "succeeded", StartedAt: now, CompletedAt: now, OriginalRequest: []byte(`{"input":"hello"}`), Facets: map[string][]string{"project.name": {"repo-a"}, "client": {"Codex Desktop"}, "source.format": {"openai-response"}}}, {RequestID: "r2", SessionID: "s2", RequestedModel: "claude", Outcome: "succeeded", StartedAt: now, CompletedAt: now, OriginalRequest: []byte(`{"input":"world"}`), Facets: map[string][]string{"project.name": {"repo-b"}, "client": {"Claude Code"}}}}
	if e = s.PutBatch(records); e != nil {
		t.Fatal(e)
	}
	facets, e := s.Facets(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if len(facets) < 4 {
		t.Fatalf("facet count=%d", len(facets))
	}
	sessions, e := s.SessionsFiltered(context.Background(), 10, map[string]string{"project.name": "repo-a", "client": "Codex Desktop"})
	if e != nil {
		t.Fatal(e)
	}
	if len(sessions) != 1 || sessions[0].SessionID != "s1" || sessions[0].Project != "repo-a" {
		t.Fatalf("sessions=%+v", sessions)
	}
	detail, e := s.Session(context.Background(), "s1")
	if e != nil {
		t.Fatal(e)
	}
	if len(detail) != 1 || detail[0].Facets["project.name"][0] != "repo-a" {
		t.Fatalf("detail=%+v", detail)
	}
}

func TestRequestContextIncludesFollowingToolResult(t *testing.T) {
	s, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.DB.Close()
	now := time.Now()
	records := []Record{
		{RequestID: "call", SessionID: "session", StartedAt: now, CompletedAt: now, OriginalRequest: []byte(`{"input":[]}`), Response: []byte(`{"output":[{"type":"function_call","call_id":"call-1","name":"exec","arguments":"{}"}]}`)},
		{RequestID: "result", SessionID: "session", StartedAt: now.Add(time.Second), CompletedAt: now.Add(time.Second), OriginalRequest: []byte(`{"input":[{"type":"function_call_output","call_id":"call-1","output":"done"}]}`)},
		{RequestID: "other", SessionID: "other", StartedAt: now.Add(2 * time.Second), CompletedAt: now.Add(2 * time.Second)},
	}
	if err = s.PutBatch(records); err != nil {
		t.Fatal(err)
	}
	contextRecords, err := s.RequestContext(context.Background(), "call", 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(contextRecords) != 2 || contextRecords[0].RequestID != "call" || contextRecords[1].RequestID != "result" {
		t.Fatalf("context=%+v", contextRecords)
	}
}
