package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"
)

type TimelineEntry struct {
	Role      string `json:"role"`
	Text      string `json:"text,omitempty"`
	Label     string `json:"label,omitempty"`
	CallID    string `json:"call_id,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
}

type RequestTimelineView struct {
	RequestID       string              `json:"request_id"`
	SessionID       string              `json:"session_id"`
	KeyID           string              `json:"key_id,omitempty"`
	Summary         string              `json:"summary,omitempty"`
	RequestedModel  string              `json:"requested_model,omitempty"`
	Model           string              `json:"model,omitempty"`
	Outcome         string              `json:"outcome,omitempty"`
	StatusCode      int                 `json:"status_code,omitempty"`
	Error           string              `json:"error,omitempty"`
	StartedAt       time.Time           `json:"started_at"`
	CompletedAt     time.Time           `json:"completed_at"`
	Facets          map[string][]string `json:"facets,omitempty"`
	Kind            string              `json:"kind"`
	InputItems      int                 `json:"input_items"`
	HistoryItems    int                 `json:"history_items"`
	SystemChars     int                 `json:"system_chars"`
	ToolDefinitions int                 `json:"tool_definitions"`
	Entries         []TimelineEntry     `json:"entries"`
}

func (s *Store) RequestTimeline(ctx context.Context, id string) (RequestTimelineView, error) {
	current, err := s.Request(ctx, id)
	if err != nil {
		return RequestTimelineView{}, err
	}
	var recordID int64
	if err = s.DB.QueryRowContext(ctx, `SELECT id FROM records WHERE request_id=?`, id).Scan(&recordID); err != nil {
		return RequestTimelineView{}, err
	}
	var previous *Record
	var previousID string
	if scanErr := s.DB.QueryRowContext(ctx, `SELECT request_id FROM records WHERE session_id=? AND id<? ORDER BY id DESC LIMIT 1`, current.SessionID, recordID).Scan(&previousID); scanErr == nil {
		item, loadErr := s.Request(ctx, previousID)
		if loadErr != nil {
			return RequestTimelineView{}, loadErr
		}
		previous = &item
	}

	currentRoot := decodedObject(current.OriginalRequest)
	currentInput := collectInputEntries(currentRoot)
	previousInput := []TimelineEntry{}
	previousResponse := []TimelineEntry{}
	if previous != nil {
		previousInput = collectInputEntries(decodedObject(previous.OriginalRequest))
		previousResponse = collectResponseEntries(decodedObject(previous.Response))
	}
	common := commonEntryPrefix(previousInput, currentInput)
	delta := append([]TimelineEntry(nil), currentInput[common:]...)
	// The next stateless Responses request repeats the previous assistant
	// output. Remove that transport replay while retaining new tool outputs.
	delta = subtractEntries(delta, previousResponse)

	kind := requestKind(currentRoot)
	if kind == "" {
		kind = firstTimelineFacet(current.Facets, "request.kind")
	}
	if kind == "compact" {
		kind = "compaction"
	}
	if kind == "" {
		kind = "turn"
	}
	if kind == "compaction" || containsRole(delta, "compaction") {
		kind = "compaction"
	} else if previous != nil && len(currentInput) > 0 && common == len(currentInput) {
		kind = "retry"
	} else if previous == nil && len(currentInput) > 24 {
		kind = "history_snapshot"
	} else if previous != nil && common == 0 && len(currentInput) > 24 {
		kind = "context_rebuild"
	} else if !containsRole(delta, "user") && containsRole(delta, "tool_result") {
		kind = "tool_continuation"
	}

	historyItems := common
	if (kind == "history_snapshot" || kind == "context_rebuild") && len(delta) > 24 {
		historyItems += len(delta) - 24
		delta = delta[len(delta)-24:]
	}
	entries := append(delta, collectResponseEntries(decodedObject(current.Response))...)
	for i := range entries {
		if len(entries[i].Text) > 40000 {
			entries[i].Text = entries[i].Text[:40000]
			entries[i].Truncated = true
		}
	}
	systemChars, toolDefinitions := requestContextCounts(currentRoot)
	return RequestTimelineView{
		RequestID: current.RequestID, SessionID: current.SessionID, KeyID: current.KeyID,
		Summary: current.Summary, RequestedModel: current.RequestedModel, Model: current.Model,
		Outcome: current.Outcome, StatusCode: current.StatusCode, Error: current.Error,
		StartedAt: current.StartedAt, CompletedAt: current.CompletedAt, Facets: current.Facets,
		Kind: kind, InputItems: len(currentInput), HistoryItems: historyItems,
		SystemChars: systemChars, ToolDefinitions: toolDefinitions, Entries: entries,
	}, nil
}

func firstTimelineFacet(facets map[string][]string, name string) string {
	if values := facets[name]; len(values) > 0 {
		return strings.ToLower(values[0])
	}
	return ""
}

func decodedObject(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	var value any
	_ = json.Unmarshal(raw, &value)
	return value
}

func collectInputEntries(root any) []TimelineEntry {
	object, _ := root.(map[string]any)
	if object == nil {
		return nil
	}
	value := object["input"]
	if value == nil {
		value = object["messages"]
	}
	var out []TimelineEntry
	collectTimeline(value, "user", &out)
	return out
}

func collectResponseEntries(root any) []TimelineEntry {
	object, _ := root.(map[string]any)
	if object == nil {
		return nil
	}
	if object["type"] == "response.completed" {
		if nested, ok := object["response"].(map[string]any); ok {
			object = nested
		}
	}
	value := object["output"]
	if value == nil {
		value = object
	}
	var out []TimelineEntry
	collectTimeline(value, "assistant", &out)
	return out
}

func collectTimeline(value any, fallback string, out *[]TimelineEntry) {
	switch item := value.(type) {
	case []any:
		for _, child := range item {
			collectTimeline(child, fallback, out)
		}
	case string:
		if strings.TrimSpace(item) != "" {
			*out = append(*out, TimelineEntry{Role: fallback, Text: item})
		}
	case map[string]any:
		typeName := strings.ToLower(stringValue(item["type"]))
		role := strings.ToLower(stringValue(item["role"]))
		if role == "" {
			role = fallback
		}
		switch typeName {
		case "function_call_output", "custom_tool_call_output":
			*out = append(*out, TimelineEntry{Role: "tool_result", Text: contentValue(firstValue(item, "output", "content")), Label: stringValue(item["name"]), CallID: stringValue(firstValue(item, "call_id", "tool_call_id", "id"))})
			return
		case "function_call", "custom_tool_call":
			*out = append(*out, TimelineEntry{Role: "tool_call", Text: contentValue(firstValue(item, "arguments", "input", "content")), Label: stringValue(item["name"]), CallID: stringValue(firstValue(item, "call_id", "id"))})
			return
		case "compaction", "compaction_trigger":
			*out = append(*out, TimelineEntry{Role: "compaction", Text: contentValue(firstValue(item, "summary", "content"))})
			return
		case "input_image", "image", "input_audio", "audio", "video":
			*out = append(*out, TimelineEntry{Role: role, Text: "[attachment]", Label: typeName})
			return
		case "reasoning":
			return
		}
		if typeName == "message" || stringValue(item["role"]) != "" {
			text := contentValue(firstValue(item, "content", "text"))
			if text == "" {
				return
			}
			switch role {
			case "assistant":
				role = "assistant"
			case "tool":
				role = "tool_result"
			case "system", "developer":
				role = "system"
			default:
				role = "user"
			}
			*out = append(*out, TimelineEntry{Role: role, Text: text})
			return
		}
		if nested := firstValue(item, "output", "content"); nested != nil {
			collectTimeline(nested, fallback, out)
		}
	}
}

func contentValue(value any) string {
	switch item := value.(type) {
	case nil:
		return ""
	case string:
		return item
	case []any:
		parts := make([]string, 0, len(item))
		for _, child := range item {
			if text := contentValue(child); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		if strings.Contains(strings.ToLower(stringValue(item["type"])), "image") {
			return "[attachment]"
		}
		return contentValue(firstValue(item, "text", "output_text", "input_text", "output", "content", "summary"))
	default:
		encoded, _ := json.Marshal(item)
		return string(encoded)
	}
}

func firstValue(item map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := item[key]; ok && value != nil {
			return value
		}
	}
	return nil
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func entrySignature(entry TimelineEntry) string {
	sum := sha256.Sum256([]byte(entry.Role + "\n" + entry.Label + "\n" + entry.CallID + "\n" + entry.Text))
	return hex.EncodeToString(sum[:])
}

func commonEntryPrefix(left, right []TimelineEntry) int {
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}
	index := 0
	for index < limit && entrySignature(left[index]) == entrySignature(right[index]) {
		index++
	}
	return index
}

func subtractEntries(entries, replay []TimelineEntry) []TimelineEntry {
	counts := map[string]int{}
	for _, entry := range replay {
		counts[entrySignature(entry)]++
	}
	out := make([]TimelineEntry, 0, len(entries))
	for _, entry := range entries {
		key := entrySignature(entry)
		if counts[key] > 0 {
			counts[key]--
			continue
		}
		out = append(out, entry)
	}
	return out
}

func containsRole(entries []TimelineEntry, role string) bool {
	for _, entry := range entries {
		if entry.Role == role {
			return true
		}
	}
	return false
}

func requestKind(root any) string {
	object, _ := root.(map[string]any)
	if object == nil {
		return ""
	}
	if metadata, ok := object["client_metadata"].(map[string]any); ok {
		return strings.ToLower(stringValue(metadata["request_kind"]))
	}
	return ""
}

func requestContextCounts(root any) (int, int) {
	object, _ := root.(map[string]any)
	if object == nil {
		return 0, 0
	}
	instructions := stringValue(object["instructions"])
	tools, _ := object["tools"].([]any)
	return utf8.RuneCountInString(instructions), len(tools)
}
