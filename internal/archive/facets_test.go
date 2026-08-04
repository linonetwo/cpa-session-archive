package archive

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"testing"
)

func TestGenericFacetsAcrossCodingClients(t *testing.T) {
	rec := Record{}
	headers := http.Header{}
	headers.Set("Originator", "Claude Code")
	headers.Set("User-Agent", "claude-code/9.9")
	headers.Set("X-Claude-Code-Session-Id", "claude-session")
	headers.Set("X-Stainless-Lang", "typescript")
	headers.Set("X-Stainless-OS", "Linux")
	body := []byte(`{
		"session_id":"generic-session",
		"project":{"name":"agent-repo","path":"/workspace/agent-repo"},
		"repository":"https://example.test/org/agent-repo.git",
		"branch":"feature/facets",
		"reasoning":{"effort":"high"},
		"service_tier":"priority",
		"tools":[{"type":"function","function":{"name":"shell"}},{"type":"computer"}],
		"messages":[{"role":"user","content":[{"type":"text","text":"hello"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}]}]
	}`)

	enrichDesktopMetadata(&rec, headers)
	enrichGenericFacets(&rec, headers, body)

	want := map[string]string{
		"client": "Claude Code", "client.user_agent": "claude-code/9.9",
		"sdk.language": "typescript", "client.os": "Linux",
		"session.id": "generic-session", "project.name": "agent-repo",
		"project.path": "/workspace/agent-repo", "git.remote": "https://example.test/org/agent-repo.git",
		"git.branch": "feature/facets", "reasoning.effort": "high", "service.tier": "priority",
		"tool.name": "shell", "message.role": "user",
	}
	for name, value := range want {
		if !containsFacet(rec.Facets[name], value) {
			t.Errorf("facet %s missing %q: %#v", name, value, rec.Facets[name])
		}
	}
	if !containsFacet(rec.Facets["tool.type"], "function") || !containsFacet(rec.Facets["tool.type"], "computer") {
		t.Fatalf("tool types not indexed: %#v", rec.Facets["tool.type"])
	}
	if !containsFacet(rec.Facets["content.type"], "text") || !containsFacet(rec.Facets["content.type"], "image_url") {
		t.Fatalf("content types not indexed: %#v", rec.Facets["content.type"])
	}
	if rec.ProjectName != "agent-repo" || rec.ProjectPath != "/workspace/agent-repo" || rec.GitRemote == "" {
		t.Fatalf("canonical project metadata not populated: %#v", rec)
	}
}

func TestCompletionFacetsSkipMissingMetadata(t *testing.T) {
	rec := Record{SessionID: "s", Metadata: map[string]any{"target_provider": "codex"}}
	addCompletionFacets(&rec)
	if containsFacet(rec.Facets["auth.group"], "<nil>") || containsFacet(rec.Facets["auth.id"], "<nil>") {
		t.Fatalf("missing metadata became synthetic facet: %#v", rec.Facets)
	}
	if !containsFacet(rec.Facets["provider.target"], "codex") {
		t.Fatalf("target provider missing: %#v", rec.Facets)
	}
}

func TestArchiveKeyIDPrefersNativeHashOverCallerScope(t *testing.T) {
	metadata := map[string]any{
		"key_hash":     "sha256:native",
		"caller_scope": "salted-host-scope",
	}
	if got := archiveKeyID(metadata); got != "sha256:native" {
		t.Fatalf("archiveKeyID=%q", got)
	}
	sanitized := sanitizeMeta(metadata)
	if sanitized["key_hash"] != "sha256:native" {
		t.Fatalf("native key hash missing from sanitized metadata: %#v", sanitized)
	}
	rec := Record{KeyID: archiveKeyID(metadata), Metadata: metadata}
	addCompletionFacets(&rec)
	if containsFacet(rec.Facets["caller.scope"], "salted-host-scope") {
		t.Fatalf("salted caller scope duplicated native identity: %#v", rec.Facets)
	}
	if !containsFacet(rec.Facets["key.id"], "sha256:native") {
		t.Fatalf("native key identity missing: %#v", rec.Facets)
	}
}

func TestCompletionFacetsRetainCallerScopeAsFallback(t *testing.T) {
	metadata := map[string]any{"caller_scope": "salted-host-scope"}
	rec := Record{KeyID: archiveKeyID(metadata), Metadata: metadata}
	addCompletionFacets(&rec)
	if !containsFacet(rec.Facets["caller.scope"], "salted-host-scope") {
		t.Fatalf("caller scope fallback missing: %#v", rec.Facets)
	}
}

func TestNativeKeyHashUsesAuthorizationWithoutPersistingSecret(t *testing.T) {
	headers := http.Header{}
	headers.Set("Authorization", "Bearer native-secret")
	want := fmt.Sprintf("%x", sha256.Sum256([]byte("native-secret")))
	if got := nativeKeyHash(headers); got != want {
		t.Fatalf("nativeKeyHash=%q want=%q", got, want)
	}
}

func containsFacet(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
