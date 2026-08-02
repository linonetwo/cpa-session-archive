package archive

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// SFTExample follows the conversational JSONL shape accepted by OpenAI SFT
// and Hugging Face TRL. One exported line represents one durable session so
// repeatedly resent Responses API context is not emitted as duplicate samples.
type SFTExample struct {
	Messages          []map[string]any `json:"messages"`
	Tools             any              `json:"tools,omitempty"`
	ParallelToolCalls *bool            `json:"parallel_tool_calls,omitempty"`
}

func (s *Store) ExportTrainingJSONL(ctx context.Context, sessionID string, dst io.Writer) error {
	where := `outcome='succeeded' AND status_code BETWEEN 200 AND 299`
	args := []any{}
	if sessionID != "" {
		where += ` AND session_id=?`
		args = append(args, sessionID)
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT session_id,request_id FROM records r WHERE `+where+` AND started_at=(SELECT MAX(r2.started_at) FROM records r2 WHERE r2.session_id=r.session_id AND r2.outcome='succeeded' AND r2.status_code BETWEEN 200 AND 299) ORDER BY session_id`, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	type selected struct{ sessionID, requestID string }
	var selectedRows []selected
	for rows.Next() {
		var item selected
		if err = rows.Scan(&item.sessionID, &item.requestID); err != nil {
			return err
		}
		selectedRows = append(selectedRows, item)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	enc := json.NewEncoder(dst)
	enc.SetEscapeHTML(false)
	for _, item := range selectedRows {
		if err = ctx.Err(); err != nil {
			return err
		}
		record, loadErr := s.Request(ctx, item.requestID)
		if loadErr != nil {
			return loadErr
		}
		example, ok := trainingExample(record)
		if !ok {
			continue
		}
		if err = enc.Encode(example); err != nil {
			return err
		}
		if f, ok := dst.(interface{ Flush() }); ok {
			f.Flush()
		}
	}
	return nil
}

func trainingExample(record Record) (SFTExample, bool) {
	var request, response any
	if json.Unmarshal(record.OriginalRequest, &request) != nil {
		return SFTExample{}, false
	}
	_ = json.Unmarshal(record.Response, &response)
	root, _ := request.(map[string]any)
	if root == nil {
		return SFTExample{}, false
	}
	result := SFTExample{Messages: []map[string]any{}}
	if instructions, ok := root["instructions"].(string); ok && strings.TrimSpace(instructions) != "" {
		result.Messages = append(result.Messages, map[string]any{"role": "system", "content": sanitizeTrainingText(instructions)})
	}
	input := root["input"]
	if input == nil {
		input = root["messages"]
	}
	appendTrainingItems(&result.Messages, input)
	responseRoot := response
	if wrapper, ok := response.(map[string]any); ok {
		if nested, exists := wrapper["response"]; exists {
			responseRoot = nested
		}
	}
	if payload, ok := responseRoot.(map[string]any); ok {
		appendTrainingItems(&result.Messages, payload["output"])
	}
	result.Messages = dedupeTrainingMessages(result.Messages)
	if tools, ok := root["tools"]; ok {
		result.Tools = normalizeTrainingTools(tools)
	}
	if parallel, ok := root["parallel_tool_calls"].(bool); ok {
		result.ParallelToolCalls = &parallel
	}
	hasUser, hasAssistant := false, false
	for _, message := range result.Messages {
		role, _ := message["role"].(string)
		hasUser = hasUser || role == "user"
		hasAssistant = hasAssistant || role == "assistant"
	}
	return result, hasUser && hasAssistant
}

func normalizeTrainingTools(value any) any {
	items, ok := value.([]any)
	if !ok {
		return sanitizeTrainingValue(value)
	}
	out := make([]any, 0, len(items))
	for _, raw := range items {
		tool, mapOK := raw.(map[string]any)
		if !mapOK {
			out = append(out, sanitizeTrainingValue(raw))
			continue
		}
		if strings.EqualFold(fmt.Sprint(tool["type"]), "function") && tool["function"] == nil {
			function := map[string]any{}
			for _, key := range []string{"name", "description", "parameters", "strict"} {
				if child, exists := tool[key]; exists {
					function[key] = sanitizeTrainingValue(child)
				}
			}
			out = append(out, map[string]any{"type": "function", "function": function})
		} else {
			out = append(out, sanitizeTrainingValue(tool))
		}
	}
	return out
}

func appendTrainingItems(dst *[]map[string]any, value any) {
	switch item := value.(type) {
	case []any:
		for _, child := range item {
			appendTrainingItems(dst, child)
		}
	case string:
		if text := strings.TrimSpace(item); text != "" {
			*dst = append(*dst, map[string]any{"role": "user", "content": sanitizeTrainingText(text)})
		}
	case map[string]any:
		typeName := strings.ToLower(fmt.Sprint(item["type"]))
		role := strings.ToLower(fmt.Sprint(item["role"]))
		switch typeName {
		case "function_call", "custom_tool_call":
			name := fmt.Sprint(item["name"])
			arguments := trainingString(firstAny(item["arguments"], item["input"], item["content"]))
			id := fmt.Sprint(firstAny(item["call_id"], item["id"]))
			*dst = append(*dst, map[string]any{"role": "assistant", "tool_calls": []any{map[string]any{"id": id, "type": "function", "function": map[string]any{"name": name, "arguments": arguments}}}})
			return
		case "function_call_output", "custom_tool_call_output":
			*dst = append(*dst, map[string]any{"role": "tool", "tool_call_id": fmt.Sprint(firstAny(item["call_id"], item["id"])), "content": trainingString(firstAny(item["output"], item["content"]))})
			return
		case "reasoning", "compaction", "compaction_trigger":
			return
		}
		if calls, ok := item["tool_calls"].([]any); ok {
			cleanCalls := sanitizeTrainingValue(calls)
			message := map[string]any{"role": "assistant", "tool_calls": cleanCalls}
			if text := trainingString(item["content"]); text != "" {
				message["content"] = text
			}
			*dst = append(*dst, message)
			return
		}
		if role == "tool" {
			message := map[string]any{"role": "tool", "content": trainingString(firstAny(item["content"], item["output"]))}
			if id := fmt.Sprint(firstAny(item["tool_call_id"], item["call_id"])); id != "<nil>" && id != "" {
				message["tool_call_id"] = id
			}
			if name := fmt.Sprint(item["name"]); name != "<nil>" && name != "" {
				message["name"] = name
			}
			*dst = append(*dst, message)
			return
		}
		if role != "" || typeName == "message" {
			if role == "" {
				role = "assistant"
			}
			content := trainingString(firstAny(item["content"], item["text"], item["output_text"], item["input_text"]))
			if content != "" && (role == "system" || role == "developer" || role == "user" || role == "assistant") {
				if role == "developer" {
					role = "system"
				}
				*dst = append(*dst, map[string]any{"role": role, "content": content})
			}
			return
		}
		if nested := firstAny(item["output"], item["content"]); nested != nil {
			appendTrainingItems(dst, nested)
		}
	}
}

func firstAny(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func trainingString(value any) string {
	switch item := value.(type) {
	case nil:
		return ""
	case string:
		return sanitizeTrainingText(item)
	case []any:
		parts := make([]string, 0, len(item))
		for _, child := range item {
			if text := trainingString(child); text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		if text := firstAny(item["text"], item["output_text"], item["input_text"]); text != nil {
			return trainingString(text)
		}
		if content := item["content"]; content != nil {
			return trainingString(content)
		}
		data, _ := json.Marshal(sanitizeTrainingValue(item))
		return string(data)
	default:
		return fmt.Sprint(item)
	}
}

func sanitizeTrainingText(value string) string {
	if !strings.HasPrefix(value, "data:") {
		return value
	}
	comma := strings.IndexByte(value, ',')
	if comma < 0 {
		return "[attachment omitted]"
	}
	header := value[5:comma]
	raw, err := base64.StdEncoding.DecodeString(value[comma+1:])
	if err != nil {
		return "[attachment omitted: " + header + "]"
	}
	sum := sha256.Sum256(raw)
	return "[attachment omitted: " + header + " sha256:" + hex.EncodeToString(sum[:]) + "]"
}

func sanitizeTrainingValue(value any) any {
	switch item := value.(type) {
	case string:
		return sanitizeTrainingText(item)
	case []any:
		out := make([]any, len(item))
		for i, child := range item {
			out[i] = sanitizeTrainingValue(child)
		}
		return out
	case map[string]any:
		out := map[string]any{}
		for key, child := range item {
			out[key] = sanitizeTrainingValue(child)
		}
		return out
	default:
		return value
	}
}

func dedupeTrainingMessages(messages []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(messages))
	last := ""
	for _, message := range messages {
		encoded, _ := json.Marshal(message)
		key := string(encoded)
		if key == last {
			continue
		}
		out = append(out, message)
		last = key
	}
	return out
}
