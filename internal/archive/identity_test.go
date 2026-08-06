package archive

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestCredentialPrincipalProjectionKeepsRawAuditKey(t *testing.T) {
	store, err := OpenStore(t.TempDir()+"/archive.db", false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	const hash = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if err = store.ApplyCredentialPrincipals(context.Background(), []CredentialPrincipal{{
		CredentialHash: hash,
		PrincipalID:    "principal-user-one",
		Alias:          "User One",
	}}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err = store.PutBatch([]Record{
		{RequestID: "mapped", SessionID: "session", KeyID: hash, StartedAt: now, CompletedAt: now},
		{RequestID: "legacy-label", SessionID: "legacy", KeyID: "human-label", StartedAt: now, CompletedAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	mapped, err := store.Request(context.Background(), "mapped")
	if err != nil {
		t.Fatal(err)
	}
	if mapped.KeyID != hash || mapped.CredentialHash != hash ||
		mapped.PrincipalID != "principal-user-one" || mapped.PrincipalAlias != "User One" {
		t.Fatalf("unexpected mapped identity: %#v", mapped)
	}
	legacy, err := store.Request(context.Background(), "legacy-label")
	if err != nil {
		t.Fatal(err)
	}
	if legacy.KeyID != "human-label" || legacy.CredentialHash != "" || legacy.PrincipalID != "" {
		t.Fatalf("unmapped legacy audit value was reclassified as a credential: %#v", legacy)
	}
}

func TestIdentityMigrationBackfillsSearchAndTrainingExport(t *testing.T) {
	store, err := OpenStore(t.TempDir()+"/archive.db", false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	const hash = "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	now := time.Now().UTC()
	if err = store.PutBatch([]Record{{
		RequestID:       "legacy-request",
		SessionID:       "legacy-session",
		KeyID:           hash,
		StartedAt:       now,
		CompletedAt:     now,
		OriginalRequest: []byte(`{"input":"hello"}`),
		Response:        []byte(`{"output":"world"}`),
	}}); err != nil {
		t.Fatal(err)
	}
	if err = store.ApplyCredentialPrincipals(context.Background(), []CredentialPrincipal{{
		CredentialHash: hash,
		PrincipalID:    "principal-stable",
		Alias:          "Stable User",
		Status:         "retired",
	}}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Request(context.Background(), "legacy-request")
	if err != nil {
		t.Fatal(err)
	}
	if got.PrincipalID != "principal-stable" || got.CredentialHash != hash || got.PrincipalAlias != "Stable User" {
		t.Fatalf("migration did not backfill identity: %#v", got)
	}
	sessions, err := store.SessionsFiltered(context.Background(), 10, map[string]string{
		"principal.id": "principal-stable",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].PrincipalID != "principal-stable" {
		t.Fatalf("principal facet did not resolve migrated session: %#v", sessions)
	}
	var exported bytes.Buffer
	if err = store.ExportArchiveJSONL(context.Background(), "legacy-session", &exported); err != nil {
		t.Fatal(err)
	}
	text := exported.String()
	for _, want := range []string{`"schema_version":2`, `"principal_id":"principal-stable"`, `"credential_hash":"` + hash + `"`, `"principal_alias":"Stable User"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("training export missing %s: %s", want, text)
		}
	}
}
