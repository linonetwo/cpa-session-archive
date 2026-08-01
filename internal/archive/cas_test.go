package archive

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
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
