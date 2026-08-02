package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"html"
	"sort"
	"strings"
	"time"
)

type TurnSummary struct {
	TurnID       string   `json:"turn_id"`
	SessionID    string   `json:"session_id"`
	UserText     string   `json:"user_text,omitempty"`
	FinalText    string   `json:"final_text,omitempty"`
	Requests     int      `json:"requests"`
	FirstAt      time.Time `json:"first_at"`
	LastAt       time.Time `json:"last_at"`
	KeyID        string   `json:"key_id,omitempty"`
	Model        string   `json:"model,omitempty"`
	Outcome      string   `json:"outcome,omitempty"`
	StatusCode   int      `json:"status_code,omitempty"`
	ToolNames    []string `json:"tool_names,omitempty"`
	HasCompaction bool    `json:"has_compaction,omitempty"`
}

type TurnPage struct {
	Turns  []TurnSummary `json:"turns"`
	Total  int           `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
}

type TurnDetailPage struct {
	Turn    TurnSummary          `json:"turn"`
	Records []RequestTimelineView `json:"records"`
	Total   int                  `json:"total"`
	Limit   int                  `json:"limit"`
	Offset  int                  `json:"offset"`
}

type turnGroup struct {
	summary           TurnSummary
	requestIDs        []string
	explicitID        string
	normalizedSummary string
	toolNames         map[string]struct{}
}

func (s *Store) SessionTurnPage(ctx context.Context, sessionID string, limit, offset int, order string) (TurnPage, error) {
	groups, err := s.sessionTurnGroups(ctx, sessionID)
	if err != nil {
		return TurnPage{}, err
	}
	if strings.EqualFold(order, "desc") {
		for left, right := 0, len(groups)-1; left < right; left, right = left+1, right-1 {
			groups[left], groups[right] = groups[right], groups[left]
		}
	}
	out := TurnPage{Turns: []TurnSummary{}, Total: len(groups), Limit: limit, Offset: offset}
	if offset >= len(groups) {
		return out, nil
	}
	end := offset + limit
	if end > len(groups) {
		end = len(groups)
	}
	for _, group := range groups[offset:end] {
		out.Turns = append(out.Turns, group.summary)
	}
	return out, nil
}

func (s *Store) SessionTurnDetail(ctx context.Context, sessionID, turnID string, limit, offset int) (TurnDetailPage, error) {
	groups, err := s.sessionTurnGroups(ctx, sessionID)
	if err != nil {
		return TurnDetailPage{}, err
	}
	for _, group := range groups {
		if group.summary.TurnID != turnID {
			continue
		}
		out := TurnDetailPage{Turn: group.summary, Records: []RequestTimelineView{}, Total: len(group.requestIDs), Limit: limit, Offset: offset}
		if offset >= len(group.requestIDs) {
			return out, nil
		}
		end := offset + limit
		if end > len(group.requestIDs) {
			end = len(group.requestIDs)
		}
		for _, requestID := range group.requestIDs[offset:end] {
			view, loadErr := s.RequestTimeline(ctx, requestID)
			if loadErr != nil {
				return TurnDetailPage{}, loadErr
			}
			for index := range view.Entries {
				if view.Entries[index].Role == "tool_call" || view.Entries[index].Role == "tool_result" {
					view.Entries[index].Text = ""
				}
			}
			out.Records = append(out.Records, view)
		}
		return out, nil
	}
	return TurnDetailPage{}, errors.New("turn not found")
}

func (s *Store) sessionTurnGroups(ctx context.Context, sessionID string) ([]turnGroup, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT request_id,COALESCE(key_id,''),COALESCE(summary,''),COALESCE(response_preview,''),COALESCE(requested_model,''),COALESCE(model,''),COALESCE(outcome,''),status_code,started_at,completed_at,COALESCE(facets_json,'') FROM records WHERE session_id=? ORDER BY started_at,id`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	groups := []turnGroup{}
	for rows.Next() {
		var requestID, keyID, summary, responsePreview, requestedModel, model, outcome, startedRaw, completedRaw, facetsRaw string
		var statusCode int
		if err = rows.Scan(&requestID, &keyID, &summary, &responsePreview, &requestedModel, &model, &outcome, &statusCode, &startedRaw, &completedRaw, &facetsRaw); err != nil {
			return nil, err
		}
		startedAt, _ := time.Parse(time.RFC3339Nano, startedRaw)
		completedAt, _ := time.Parse(time.RFC3339Nano, completedRaw)
		facets := map[string][]string{}
		_ = json.Unmarshal([]byte(facetsRaw), &facets)
		explicitID := firstTurnFacet(facets, "turn.id")
		normalized := normalizeTurnSummary(summary)
		startNew := len(groups) == 0
		if !startNew {
			current := &groups[len(groups)-1]
			switch {
			case explicitID != "" && current.explicitID != "":
				startNew = explicitID != current.explicitID
			case explicitID != "" && current.explicitID == "":
				startNew = normalized != "" && current.normalizedSummary != "" && normalized != current.normalizedSummary
			case explicitID == "" && normalized != "" && current.normalizedSummary != "":
				startNew = normalized != current.normalizedSummary
			}
		}
		if startNew {
			turnID := explicitID
			if turnID == "" {
				turnID = derivedTurnID(sessionID, requestID)
			}
			groups = append(groups, turnGroup{
				summary: TurnSummary{TurnID: turnID, SessionID: sessionID, UserText: cleanTurnText(summary), FirstAt: startedAt, LastAt: completedAt, KeyID: keyID, Model: firstNonEmpty(requestedModel, model), ToolNames: []string{}},
				explicitID: explicitID, normalizedSummary: normalized, toolNames: map[string]struct{}{},
			})
		}
		current := &groups[len(groups)-1]
		if current.explicitID == "" && explicitID != "" {
			current.explicitID = explicitID
			current.summary.TurnID = explicitID
		}
		if current.normalizedSummary == "" && normalized != "" {
			current.normalizedSummary = normalized
			current.summary.UserText = cleanTurnText(summary)
		}
		current.requestIDs = append(current.requestIDs, requestID)
		current.summary.Requests++
		current.summary.LastAt = completedAt
		current.summary.Outcome = outcome
		current.summary.StatusCode = statusCode
		if keyID != "" {
			current.summary.KeyID = keyID
		}
		if selectedModel := firstNonEmpty(requestedModel, model); selectedModel != "" {
			current.summary.Model = selectedModel
		}
		if readableFinalPreview(responsePreview) {
			current.summary.FinalText = cleanTurnText(responsePreview)
		}
		for _, name := range facets["tool.name"] {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			if _, exists := current.toolNames[name]; !exists {
				current.toolNames[name] = struct{}{}
				current.summary.ToolNames = append(current.summary.ToolNames, name)
			}
		}
		for _, kind := range facets["request.kind"] {
			if strings.EqualFold(kind, "compaction") || strings.EqualFold(kind, "compact") {
				current.summary.HasCompaction = true
			}
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	for index := range groups {
		sort.Strings(groups[index].summary.ToolNames)
	}
	return groups, nil
}

func firstTurnFacet(facets map[string][]string, name string) string {
	if values := facets[name]; len(values) > 0 {
		return strings.TrimSpace(values[0])
	}
	return ""
}

func normalizeTurnSummary(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(cleanTurnText(value))), " ")
}

func cleanTurnText(value string) string {
	value = strings.TrimSpace(html.UnescapeString(value))
	lower := strings.ToLower(value)
	for _, prefix := range []string{"## my request for codex:", "# my request for codex:", "my request for codex:"} {
		if strings.HasPrefix(lower, prefix) {
			value = strings.TrimSpace(value[len(prefix):])
			lower = strings.ToLower(value)
			break
		}
	}
	if strings.HasPrefix(lower, "<userrequest>") {
		value = strings.TrimSpace(value[len("<userRequest>"):])
		lower = strings.ToLower(value)
	}
	if strings.HasSuffix(lower, "</userrequest>") {
		value = strings.TrimSpace(value[:len(value)-len("</userRequest>")])
	}
	return value
}

func readableFinalPreview(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	lower := strings.ToLower(value)
	for _, prefix := range []string{"工具调用：", "工具调用:", "tool call:", "tool call："} {
		if strings.HasPrefix(lower, prefix) {
			return false
		}
	}
	return true
}

func derivedTurnID(sessionID, requestID string) string {
	sum := sha256.Sum256([]byte(sessionID + "\n" + requestID))
	return "derived-" + hex.EncodeToString(sum[:16])
}
