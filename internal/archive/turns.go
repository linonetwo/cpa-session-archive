package archive

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"html"
	"sort"
	"strings"
	"time"
)

type TurnSummary struct {
	TurnID         string    `json:"turn_id"`
	SessionID      string    `json:"session_id"`
	UserText       string    `json:"user_text,omitempty"`
	FinalText      string    `json:"final_text,omitempty"`
	Requests       int       `json:"requests"`
	FirstAt        time.Time `json:"first_at"`
	LastAt         time.Time `json:"last_at"`
	KeyID          string    `json:"key_id,omitempty"`
	PrincipalID    string    `json:"principal_id,omitempty"`
	CredentialHash string    `json:"credential_hash,omitempty"`
	PrincipalAlias string    `json:"principal_alias,omitempty"`
	Model          string    `json:"model,omitempty"`
	Outcome        string    `json:"outcome,omitempty"`
	StatusCode     int       `json:"status_code,omitempty"`
	ToolNames      []string  `json:"tool_names,omitempty"`
	HasCompaction  bool      `json:"has_compaction,omitempty"`
}

type TurnPage struct {
	Turns  []TurnSummary `json:"turns"`
	Total  int           `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
}

type TurnDetailPage struct {
	Turn    TurnSummary           `json:"turn"`
	Records []RequestTimelineView `json:"records"`
	Total   int                   `json:"total"`
	Limit   int                   `json:"limit"`
	Offset  int                   `json:"offset"`
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
			view, loadErr := s.RequestTimelinePreview(ctx, requestID)
			if loadErr != nil {
				return TurnDetailPage{}, loadErr
			}
			out.Records = append(out.Records, view)
		}
		return out, nil
	}
	return TurnDetailPage{}, errors.New("turn not found")
}

func (s *Store) CachedTurnText(ctx context.Context, sessionID, turnID string) (string, bool, error) {
	var compressed []byte
	err := s.DB.QueryRowContext(ctx, `SELECT text_gz FROM turn_texts WHERE session_id=? AND turn_id=?`, sessionID, turnID).Scan(&compressed)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return string(gunzipBytes(compressed)), true, nil
}

func (s *Store) BuildTurnText(ctx context.Context, sessionID, turnID string) (string, error) {
	groups, err := s.sessionTurnGroups(ctx, sessionID)
	if err != nil {
		return "", err
	}
	for _, group := range groups {
		if group.summary.TurnID != turnID {
			continue
		}
		if fullText := s.fullTurnUserText(ctx, group); fullText != "" {
			return fullText, nil
		}
		return group.summary.UserText, nil
	}
	return "", errors.New("turn not found")
}

func (s *Store) SaveTurnText(ctx context.Context, sessionID, turnID, text string) error {
	_, err := s.DB.ExecContext(ctx, `INSERT INTO turn_texts(session_id,turn_id,text_gz,updated_at) VALUES(?,?,?,?)
		ON CONFLICT(session_id,turn_id) DO UPDATE SET text_gz=excluded.text_gz,updated_at=excluded.updated_at`,
		sessionID, turnID, gzipBytes([]byte(text)), time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// fullTurnUserText rehydrates only the original request payloads needed to
// recover one complete user command. The compact summary remains sufficient
// for list pages, avoiding a second long-text copy in SQLite.
func (s *Store) fullTurnUserText(ctx context.Context, group turnGroup) string {
	var fallback string
	for _, requestID := range group.requestIDs {
		var originalRef string
		var legacy []byte
		if err := s.DB.QueryRowContext(ctx, `SELECT COALESCE(original_ref,''),original_request_gz FROM records WHERE request_id=? LIMIT 1`, requestID).Scan(&originalRef, &legacy); err != nil {
			continue
		}
		var body []byte
		if originalRef != "" {
			body, _ = s.LoadPayload(originalRef)
		} else {
			body = gunzipBytes(legacy)
		}
		candidate := cleanTurnText(extractConversationUserText(body))
		if candidate == "" {
			continue
		}
		if fallback == "" {
			fallback = candidate
		}
		if group.normalizedSummary == "" || normalizeTurnSummary(compactSummary(candidate)) == group.normalizedSummary {
			return candidate
		}
	}
	return fallback
}

func (s *Store) sessionTurnGroups(ctx context.Context, sessionID string) ([]turnGroup, error) {
	table := "records"
	var projected int
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM turn_records WHERE session_id=?`, sessionID).Scan(&projected); err == nil && projected > 0 {
		// Prefer the narrow projection as soon as it contains this session.
		// Active sessions can gain a record between two COUNT queries; requiring
		// exact equality made a single in-flight row fall back to the wide
		// records table and forced SQLite to walk pages containing historical
		// request/response BLOBs. The writer updates turn_records in the same
		// transaction, so any difference is transient and the next refresh
		// naturally includes it.
		table = "turn_records"
	}
	facetColumns := `COALESCE(r.turn_id,''),COALESCE(r.tool_names_json,'[]'),COALESCE(r.request_kinds_json,'[]')`
	if table == "records" {
		facetColumns = `COALESCE(json_extract(r.facets_json,'$."turn.id"[0]'),''),
			COALESCE(json_extract(r.facets_json,'$."tool.name"'),'[]'),
			COALESCE(json_extract(r.facets_json,'$."request.kind"'),'[]')`
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT
		r.request_id,COALESCE(r.key_id,''),COALESCE(r.principal_id,''),COALESCE(r.credential_hash,''),
		COALESCE((SELECT alias FROM credential_principals p WHERE p.principal_id=r.principal_id AND alias<>'' ORDER BY updated_at DESC LIMIT 1),''),
		COALESCE(r.summary,''),COALESCE(r.response_preview,''),
		COALESCE(r.requested_model,''),COALESCE(r.model,''),COALESCE(r.outcome,''),r.status_code,
		r.started_at,r.completed_at,
		`+facetColumns+`
		FROM `+table+` r WHERE r.session_id=? ORDER BY r.started_at,r.id`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	groups := []turnGroup{}
	for rows.Next() {
		var requestID, keyID, principalID, credentialHash, principalAlias, summary, responsePreview, requestedModel, model, outcome, startedRaw, completedRaw string
		var explicitID, toolNamesRaw, requestKindsRaw string
		var statusCode int
		if err = rows.Scan(
			&requestID, &keyID, &principalID, &credentialHash, &principalAlias, &summary, &responsePreview, &requestedModel, &model,
			&outcome, &statusCode, &startedRaw, &completedRaw, &explicitID,
			&toolNamesRaw, &requestKindsRaw,
		); err != nil {
			return nil, err
		}
		startedAt, _ := time.Parse(time.RFC3339Nano, startedRaw)
		completedAt, _ := time.Parse(time.RFC3339Nano, completedRaw)
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
				summary:    TurnSummary{TurnID: turnID, SessionID: sessionID, UserText: cleanTurnText(summary), FirstAt: startedAt, LastAt: completedAt, KeyID: keyID, PrincipalID: principalID, CredentialHash: credentialHash, PrincipalAlias: principalAlias, Model: firstNonEmpty(requestedModel, model), ToolNames: []string{}},
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
		if principalID != "" {
			current.summary.PrincipalID = principalID
			current.summary.PrincipalAlias = principalAlias
		}
		if credentialHash != "" {
			current.summary.CredentialHash = credentialHash
		}
		if selectedModel := firstNonEmpty(requestedModel, model); selectedModel != "" {
			current.summary.Model = selectedModel
		}
		if readableFinalPreview(responsePreview) {
			current.summary.FinalText = cleanTurnText(responsePreview)
		}
		toolNames := []string{}
		_ = json.Unmarshal([]byte(toolNamesRaw), &toolNames)
		for _, name := range toolNames {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			if _, exists := current.toolNames[name]; !exists {
				current.toolNames[name] = struct{}{}
				current.summary.ToolNames = append(current.summary.ToolNames, name)
			}
		}
		requestKinds := []string{}
		_ = json.Unmarshal([]byte(requestKindsRaw), &requestKinds)
		for _, kind := range requestKinds {
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

func normalizeTurnSummary(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(cleanTurnText(value))), " ")
}

func cleanTurnText(value string) string {
	value = strings.TrimSpace(unescapeDisplayText(value))
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

// unescapeDisplayText handles payloads that were HTML-escaped more than once
// by an upstream client or an intermediate JSON serializer. It returns plain
// text only; the browser still escapes the result before inserting it into the
// DOM, so tags such as <environment_context> remain visible rather than being
// interpreted as markup.
func unescapeDisplayText(value string) string {
	for range 4 {
		next := html.UnescapeString(value)
		if next == value {
			break
		}
		value = next
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
