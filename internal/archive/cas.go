package archive

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const inlineThreshold = 1024
const subtreeThreshold = 16 << 10

type Blob struct {
	Hash      string
	RawSize   int64
	MediaType string
	Data      []byte
}
type blobRef struct {
	Blob      string `json:"$cpa_blob"`
	Encoding  string `json:"encoding"`
	Size      int    `json:"size"`
	MediaType string `json:"media_type,omitempty"`
}

var noiseKeys = map[string]bool{"encrypted_content": true, "internal_chat_message_metadata_passthrough": true}

func CompactPayload(raw []byte) ([]byte, []Blob, error) {
	if len(raw) == 0 {
		return nil, nil, nil
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err == nil {
		blobs := map[string]Blob{}
		v = compactValue(v, 0, blobs)
		m, err := json.Marshal(v)
		return m, blobSlice(blobs), err
	}
	b := newBlob(raw, "")
	ref := blobRef{Blob: b.Hash, Encoding: "raw", Size: len(raw)}
	m, _ := json.Marshal(ref)
	return m, []Blob{b}, nil
}
func ExpandPayload(manifest []byte, load func(string) ([]byte, error)) ([]byte, error) {
	if len(manifest) == 0 {
		return nil, nil
	}
	var v any
	if err := json.Unmarshal(manifest, &v); err != nil {
		return nil, err
	}
	x, err := expandValue(v, load)
	if err != nil {
		return nil, err
	}
	if s, ok := x.(rawPayload); ok {
		return []byte(s), nil
	}
	return json.Marshal(x)
}

type rawPayload string

func compactValue(v any, depth int, blobs map[string]Blob) any {
	switch x := v.(type) {
	case string:
		return compactString(x, blobs)
	case []any:
		for i := range x {
			x[i] = compactValue(x[i], depth+1, blobs)
		}
		return maybeExternalize(x, depth, blobs)
	case map[string]any:
		for k := range x {
			if noiseKeys[strings.ToLower(k)] {
				delete(x, k)
				continue
			}
			x[k] = compactValue(x[k], depth+1, blobs)
		}
		return maybeExternalize(x, depth, blobs)
	default:
		return v
	}
}
func compactString(s string, blobs map[string]Blob) any {
	if strings.HasPrefix(s, "data:") {
		if comma := strings.IndexByte(s, ','); comma > 5 && strings.Contains(s[:comma], ";base64") {
			if raw, err := base64.StdEncoding.DecodeString(s[comma+1:]); err == nil {
				media := strings.TrimPrefix(strings.Split(s[:comma], ";")[0], "data:")
				b := newBlob(raw, media)
				blobs[b.Hash] = b
				return blobRef{Blob: b.Hash, Encoding: "data-url", Size: len(raw), MediaType: media}
			}
		}
	}
	if len(s) < inlineThreshold {
		return s
	}
	b := newBlob([]byte(s), "text/plain; charset=utf-8")
	blobs[b.Hash] = b
	return blobRef{Blob: b.Hash, Encoding: "utf8", Size: len(s), MediaType: b.MediaType}
}
func maybeExternalize(v any, depth int, blobs map[string]Blob) any {
	if depth == 0 {
		return v
	}
	raw, _ := json.Marshal(v)
	if len(raw) < subtreeThreshold {
		return v
	}
	b := newBlob(raw, "application/json")
	blobs[b.Hash] = b
	return blobRef{Blob: b.Hash, Encoding: "json", Size: len(raw), MediaType: b.MediaType}
}
func expandValue(v any, load func(string) ([]byte, error)) (any, error) {
	switch x := v.(type) {
	case []any:
		for i := range x {
			y, e := expandValue(x[i], load)
			if e != nil {
				return nil, e
			}
			x[i] = y
		}
		return x, nil
	case map[string]any:
		if h, ok := x["$cpa_blob"].(string); ok {
			raw, e := load(h)
			if e != nil {
				return nil, e
			}
			enc, _ := x["encoding"].(string)
			switch enc {
			case "raw":
				return rawPayload(raw), nil
			case "utf8":
				return string(raw), nil
			case "data-url":
				media, _ := x["media_type"].(string)
				return "data:" + media + ";base64," + base64.StdEncoding.EncodeToString(raw), nil
			case "json":
				var nested any
				if e = json.Unmarshal(raw, &nested); e != nil {
					return nil, e
				}
				return expandValue(nested, load)
			default:
				return nil, fmt.Errorf("unknown blob encoding %q", enc)
			}
		}
		for k := range x {
			y, e := expandValue(x[k], load)
			if e != nil {
				return nil, e
			}
			x[k] = y
		}
		return x, nil
	default:
		return v, nil
	}
}
func newBlob(raw []byte, media string) Blob {
	sum := sha256.Sum256(raw)
	return Blob{Hash: "sha256:" + hex.EncodeToString(sum[:]), RawSize: int64(len(raw)), MediaType: media, Data: append([]byte(nil), raw...)}
}
func blobSlice(m map[string]Blob) []Blob {
	out := make([]Blob, 0, len(m))
	for _, b := range m {
		out = append(out, b)
	}
	return out
}
