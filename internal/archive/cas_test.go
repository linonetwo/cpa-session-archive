package archive

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCompactDeduplicatesAttachmentAndDropsNoise(t *testing.T) {
	img := []byte(strings.Repeat("image-bytes", 1000))
	data := "data:image/png;base64," + base64.StdEncoding.EncodeToString(img)
	raw, _ := json.Marshal(map[string]any{"input": []any{map[string]any{"image_url": data}, map[string]any{"image_url": data}}, "encrypted_content": strings.Repeat("noise", 5000)})
	m, blobs, e := CompactPayload(raw)
	if e != nil {
		t.Fatal(e)
	}
	if len(blobs) != 1 {
		t.Fatalf("blobs=%d want 1", len(blobs))
	}
	if bytes.Contains(m, []byte("encrypted_content")) {
		t.Fatal("noise retained")
	}
	out, e := ExpandPayload(m, func(h string) ([]byte, error) { return blobs[0].Data, nil })
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Count(out, []byte("data:image/png;base64,")) != 2 {
		t.Fatal("attachment was not restored twice")
	}
}
func TestCompactExternalizesRepeatedLargeText(t *testing.T) {
	text := strings.Repeat("tool schema ", 3000)
	raw, _ := json.Marshal(map[string]any{"instructions": text, "tools": []any{text, text}})
	m, blobs, e := CompactPayload(raw)
	if e != nil {
		t.Fatal(e)
	}
	if len(blobs) > 3 {
		t.Fatalf("unexpected duplicate blobs: %d", len(blobs))
	}
	if len(m) >= len(raw)/2 {
		t.Fatalf("manifest not compact: %d >= %d", len(m), len(raw)/2)
	}
}

func TestCompactKeepsOnlyCompletedSSETerminalEvent(t *testing.T) {
	raw := []byte(`event: response.createddata: {"type":"response.created","response":{"id":"r"}}event: response.output_text.deltadata: {"type":"response.output_text.delta","delta":"duplicate"}event: response.completeddata: {"type":"response.completed","response":{"id":"r","output":[{"text":"final"}]}}`)
	manifest, blobs, err := CompactPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	byHash := map[string][]byte{}
	for _, blob := range blobs {
		byHash[blob.Hash] = blob.Data
	}
	out, err := ExpandPayload(manifest, func(hash string) ([]byte, error) { return byHash[hash], nil })
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(out, []byte("duplicate")) || !bytes.Contains(out, []byte(`"type":"response.completed"`)) || !bytes.Contains(out, []byte("final")) {
		t.Fatalf("normalized response=%s", out)
	}
}

func TestCompactRetainsIncompleteSSE(t *testing.T) {
	raw := []byte(`event: response.output_text.deltadata: {"type":"response.output_text.delta","delta":"partial"}`)
	manifest, blobs, err := CompactPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(blobs) != 1 || blobs[0].MediaType != "" {
		t.Fatalf("blobs=%+v", blobs)
	}
	out, err := ExpandPayload(manifest, func(string) ([]byte, error) { return blobs[0].Data, nil })
	if err != nil || !bytes.Equal(out, raw) {
		t.Fatalf("out=%s err=%v", out, err)
	}
}

func TestNestedBlobRefsFindsManifestChildrenOnly(t *testing.T) {
	refs := nestedBlobRefs([]byte(`{"input":[{"$cpa_blob":"sha256:child","encoding":"utf8"}],"ordinary":"sha256:not-a-ref"}`))
	if len(refs) != 1 || refs[0] != "sha256:child" {
		t.Fatalf("refs=%v", refs)
	}
}

func TestGCKeepsNestedContentAddressedBlobs(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	now := time.Now()
	payload := []byte(`{"input":"` + strings.Repeat("important history ", 5000) + `"}`)
	if err = store.PutBatch([]Record{{RequestID: "request", SessionID: "session", StartedAt: now, CompletedAt: now, OriginalRequest: payload}}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec(`INSERT INTO blobs(hash,media_type,raw_size,codec,data) VALUES('sha256:orphan','',1,'raw',X'00')`); err != nil {
		t.Fatal(err)
	}
	deleted, err := store.GC()
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted=%d", deleted)
	}
	record, err := store.Request(context.Background(), "request")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(record.OriginalRequest, payload) {
		t.Fatal("GC removed a nested live blob")
	}
}
