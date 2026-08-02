package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"
	"gopkg.in/yaml.v3"
)

type pluginConfig struct {
	Enabled              bool   `yaml:"enabled"`
	Endpoint             string `yaml:"endpoint"`
	QueueSize            int    `yaml:"queue_size"`
	MaxBodyBytes         int    `yaml:"max_body_bytes"`
	Timeout              string `yaml:"timeout"`
	StoreUpstreamRequest bool   `yaml:"store_upstream_request"`
}
type lifecycle struct {
	ConfigYAML []byte `json:"config_yaml"`
}
type state struct {
	Record
	chunks bytes.Buffer
}
type Plugin struct {
	mu            sync.Mutex
	active        map[string]*state
	q             chan Record
	stop          chan struct{}
	client        *http.Client
	endpoint      string
	max           int
	storeUpstream bool
	wg            sync.WaitGroup
}
type envelope struct {
	OK     bool `json:"ok"`
	Result any  `json:"result,omitempty"`
	Error  any  `json:"error,omitempty"`
}
type intercept struct {
	RequestID, TraceID, SourceFormat, Model, RequestedModel string
	Stream                                                  bool
	Headers                                                 http.Header
	RequestHeaders                                          http.Header
	OriginalRequest, RequestBody, Body                      []byte
	StatusCode                                              int
	Metadata                                                map[string]any
	ChunkIndex                                              int
}
type completion struct {
	RequestID, TraceID, SourceFormat, Model, RequestedModel, Outcome, Error string
	Stream                                                                  bool
	StatusCode                                                              int
	StartedAt, CompletedAt                                                  time.Time
	Metadata                                                                map[string]any
}

func NewPlugin() *Plugin {
	transport := &http.Transport{
		Proxy:       nil,
		DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
	}
	return &Plugin{active: map[string]*state{}, stop: make(chan struct{}), client: &http.Client{Timeout: 5 * time.Second, Transport: transport}, max: 64 << 20}
}
func ok(v any) ([]byte, error) { return json.Marshal(envelope{OK: true, Result: v}) }
func (p *Plugin) Handle(method string, raw []byte) ([]byte, error) {
	switch method {
	case "plugin.register", "plugin.reconfigure":
		return p.configure(raw)
	case "management.register":
		return ok(p.managementRegistration())
	case "management.handle":
		return p.handleManagement(raw)
	case "request.intercept_before":
		var r intercept
		if json.Unmarshal(raw, &r) != nil {
			return nil, errors.New("invalid request")
		}
		p.captureRequest(r, false)
		return ok(map[string]any{})
	case "request.intercept_after":
		var r intercept
		if json.Unmarshal(raw, &r) != nil {
			return nil, errors.New("invalid request")
		}
		p.captureRequest(r, true)
		return ok(map[string]any{})
	case "response.intercept_after":
		var r intercept
		if json.Unmarshal(raw, &r) != nil {
			return nil, errors.New("invalid response")
		}
		p.captureResponse(r)
		return ok(map[string]any{})
	case "response.intercept_stream_chunk":
		var r intercept
		if json.Unmarshal(raw, &r) != nil {
			return nil, errors.New("invalid chunk")
		}
		p.captureChunk(r)
		return ok(map[string]any{})
	case "request.complete":
		var r completion
		if json.Unmarshal(raw, &r) != nil {
			return nil, errors.New("invalid completion")
		}
		p.complete(r)
		return ok(map[string]any{})
	default:
		return nil, errors.New("unknown method")
	}
}
func (p *Plugin) configure(raw []byte) ([]byte, error) {
	var l lifecycle
	_ = json.Unmarshal(raw, &l)
	c := pluginConfig{Enabled: true, QueueSize: 2048, MaxBodyBytes: 64 << 20, Timeout: "5s"}
	_ = yaml.Unmarshal(l.ConfigYAML, &c)
	if c.QueueSize < 64 {
		c.QueueSize = 64
	}
	if c.MaxBodyBytes > 0 {
		p.max = c.MaxBodyBytes
	}
	if d, e := time.ParseDuration(c.Timeout); e == nil {
		p.client.Timeout = d
	}
	endpoint := strings.TrimRight(strings.TrimSpace(c.Endpoint), "/")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:8080"
	}
	p.endpoint = endpoint + "/ingest"
	p.storeUpstream = c.StoreUpstreamRequest
	if p.q == nil {
		p.q = make(chan Record, c.QueueSize)
		p.wg.Add(1)
		go p.sender()
	}
	reg := map[string]any{"schema_version": 2, "metadata": map[string]any{"Name": "cpa-session-archive", "Version": "0.6.1", "Author": "OneTwo", "GitHubRepository": "https://github.com/linonetwo/cpa-session-archive", "ConfigFields": []any{}}, "capabilities": map[string]any{"request_interceptor": true, "request_lifecycle_plugin": true, "response_interceptor": true, "response_stream_interceptor": true, "management_api": true}}
	return ok(reg)
}
func (p *Plugin) captureRequest(r intercept, afterAuth bool) {
	if r.RequestID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.active[r.RequestID]
	if s == nil {
		s = &state{}
		s.RequestID = r.RequestID
		p.active[r.RequestID] = s
	}
	s.TraceID = r.TraceID
	s.SourceFormat = r.SourceFormat
	s.Model = r.Model
	s.RequestedModel = r.RequestedModel
	s.Stream = r.Stream
	s.Metadata = sanitizeMeta(r.Metadata)
	if !afterAuth {
		enrichDesktopMetadata(&s.Record, r.Headers)
	}
	if afterAuth {
		if p.storeUpstream && len(r.Body) > 0 {
			s.UpstreamRequest = limit(r.Body, p.max, &s.Truncated)
		}
	} else {
		if len(r.Body) > 0 {
			s.OriginalRequest = limit(r.Body, p.max, &s.Truncated)
		}
		enrichGenericFacets(&s.Record, r.Headers, r.Body)
		if s.Summary == "" {
			s.Summary = extractConversationSummary(r.Body)
		}
	}
	s.SessionID = sessionID(&s.Record, r.Metadata, s.OriginalRequest, s.UpstreamRequest)
	s.ParentResponseID = firstJSON(s.OriginalRequest, s.UpstreamRequest, "previous_response_id")
	s.KeyID = firstNonEmpty(findString(r.Metadata, "key_name", "key_alias", "principal_name", "key_id", "principal"), findString(r.Metadata, "caller_scope"))
}
func (p *Plugin) captureResponse(r intercept) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s := p.active[r.RequestID]; s != nil {
		s.StatusCode = r.StatusCode
		s.Response = limit(r.Body, p.max, &s.Truncated)
		s.ResponseID = firstJSON(r.Body, "id")
	}
}
func (p *Plugin) captureChunk(r intercept) {
	if r.ChunkIndex < 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if s := p.active[r.RequestID]; s != nil {
		remaining := p.max - s.chunks.Len()
		if remaining <= 0 {
			s.Truncated = true
			return
		}
		b := r.Body
		if len(b) > remaining {
			b = b[:remaining]
			s.Truncated = true
		}
		s.chunks.Write(b)
	}
}
func (p *Plugin) complete(r completion) {
	p.mu.Lock()
	s := p.active[r.RequestID]
	delete(p.active, r.RequestID)
	p.mu.Unlock()
	if s == nil {
		s = &state{Record: Record{RequestID: r.RequestID}}
	}
	s.TraceID = r.TraceID
	s.SourceFormat = r.SourceFormat
	s.Model = r.Model
	s.RequestedModel = r.RequestedModel
	s.Stream = r.Stream
	s.Outcome = r.Outcome
	s.StatusCode = r.StatusCode
	s.Error = r.Error
	s.StartedAt = r.StartedAt
	s.CompletedAt = r.CompletedAt
	if s.SessionID == "" || s.SessionID == "request:" {
		s.SessionID = sessionID(&s.Record, r.Metadata, s.OriginalRequest, s.UpstreamRequest)
	}
	if s.SessionID == "" || s.SessionID == "request:" {
		s.SessionID = "request:" + r.RequestID
	}
	if len(s.Response) == 0 && s.chunks.Len() > 0 {
		s.Response = append([]byte(nil), s.chunks.Bytes()...)
	}
	if s.ResponseID == "" {
		s.ResponseID = responseID(s.Response)
	}
	if s.ResponsePreview == "" {
		s.ResponsePreview = extractResponsePreview(s.Response)
	}
	addCompletionFacets(&s.Record)
	select {
	case p.q <- s.Record:
	default:
	}
}

func extractResponsePreview(body []byte) string {
	if terminal, ok := terminalSSEPayload(body); ok {
		body = terminal
	}
	var root any
	if json.Unmarshal(body, &root) != nil {
		return ""
	}
	var candidates []string
	var walk func(any)
	walk = func(value any) {
		switch item := value.(type) {
		case []any:
			for _, child := range item {
				walk(child)
			}
		case map[string]any:
			typeName := strings.ToLower(fmt.Sprint(item["type"]))
			if typeName == "function_call" || typeName == "custom_tool_call" {
				if name := strings.TrimSpace(fmt.Sprint(item["name"])); name != "" && name != "<nil>" {
					candidates = append(candidates, "工具调用："+name)
				}
			}
			if role := strings.ToLower(fmt.Sprint(item["role"])); role == "assistant" || strings.Contains(typeName, "output_text") {
				for _, key := range []string{"text", "output_text", "content"} {
					walkPreviewText(item[key], &candidates)
				}
			}
			for _, key := range []string{"response", "output", "content"} {
				walk(item[key])
			}
		}
	}
	walk(root)
	if len(candidates) == 0 {
		return ""
	}
	return compactPreview(candidates[len(candidates)-1], 240)
}

func walkPreviewText(value any, out *[]string) {
	switch item := value.(type) {
	case string:
		if text := strings.TrimSpace(item); text != "" {
			*out = append(*out, text)
		}
	case []any:
		for _, child := range item {
			walkPreviewText(child, out)
		}
	case map[string]any:
		for _, key := range []string{"text", "output_text", "content"} {
			walkPreviewText(item[key], out)
		}
	}
}

func compactPreview(value string, limit int) string {
	value = strings.Join(strings.Fields(value), " ")
	runes := []rune(value)
	if len(runes) > limit {
		return string(runes[:limit]) + "…"
	}
	return value
}
func addCompletionFacets(rec *Record) {
	if rec.Facets == nil {
		rec.Facets = map[string][]string{}
	}
	add := func(k, v string) {
		if strings.TrimSpace(v) != "" {
			rec.Facets[k] = []string{v}
		}
	}
	add("session.id", rec.SessionID)
	add("model.requested", rec.RequestedModel)
	add("model.resolved", rec.Model)
	add("source.format", rec.SourceFormat)
	add("outcome", rec.Outcome)
	add("key.id", rec.KeyID)
	if rec.Metadata != nil {
		add("provider.target", findString(rec.Metadata, "target_provider"))
		add("model.target", findString(rec.Metadata, "target_model"))
		add("auth.group", findString(rec.Metadata, "group"))
		add("auth.id", findString(rec.Metadata, "selected_auth_id"))
		add("caller.scope", findString(rec.Metadata, "caller_scope"))
		add("request.path", findString(rec.Metadata, "request_path"))
	}
	rec.Facets["stream"] = []string{fmt.Sprint(rec.Stream)}
	if rec.StatusCode > 0 {
		rec.Facets["status.code"] = []string{fmt.Sprint(rec.StatusCode)}
	}
}
func (p *Plugin) sender() {
	defer p.wg.Done()
	for {
		select {
		case rec := <-p.q:
			p.sendWithRetry(rec)
		case <-p.stop:
			return
		}
	}
}

func (p *Plugin) sendWithRetry(rec Record) {
	b, _ := json.Marshal(rec)
	backoff := 250 * time.Millisecond
	for {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, p.endpoint, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		resp, err := p.client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return
			}
			err = fmt.Errorf("collector returned HTTP %d", resp.StatusCode)
		}
		log.Printf("archive delivery failed request_id=%s endpoint=%s: %v; retrying in %s", rec.RequestID, p.endpoint, err, backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-p.stop:
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}
func (p *Plugin) Shutdown() {
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
	p.wg.Wait()
}
func limit(b []byte, n int, t *bool) []byte {
	if len(b) > n {
		*t = true
		b = b[:n]
	}
	return append([]byte(nil), b...)
}
func firstJSON(bs ...any) string {
	var key string
	var arr [][]byte
	for _, x := range bs {
		switch v := x.(type) {
		case []byte:
			arr = append(arr, v)
		case string:
			key = v
		}
	}
	for _, b := range arr {
		if v := gjson.GetBytes(b, key).String(); v != "" {
			return v
		}
	}
	return ""
}
func responseID(b []byte) string {
	for _, line := range bytes.Split(b, []byte("\n")) {
		line = bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
		if v := gjson.GetBytes(line, "response.id").String(); v != "" {
			return v
		}
		if v := gjson.GetBytes(line, "id").String(); strings.HasPrefix(v, "resp_") {
			return v
		}
	}
	return ""
}
func sessionID(rec *Record, m map[string]any, bs ...[]byte) string {
	// Codex Desktop creates a new execution_session_id for retries and remote
	// executions inside one visible task.  The thread identity carried by the
	// client is the durable archive boundary; execution IDs remain metadata.
	if rec != nil {
		if v := firstNonEmpty(rec.ThreadID, stableWindowID(rec.WindowID)); v != "" {
			return v
		}
	}
	for _, b := range bs {
		for _, k := range []string{"thread_id", "session_id", "client_metadata.thread_id", "client_metadata.session_id", "prompt_cache_key", "client_metadata.x-codex-window-id"} {
			if v := gjson.GetBytes(b, k).String(); v != "" {
				return firstNonEmpty(stableWindowID(v), v)
			}
		}
		u := gjson.GetBytes(b, "metadata.user_id").String()
		if strings.HasPrefix(u, "{") {
			if v := gjson.Get(u, "session_id").String(); v != "" {
				return v
			}
		}
	}
	if v := findString(m, "thread_id", "session_id"); v != "" {
		return v
	}
	if v := findString(m, "execution_session_id", "derived_session_id"); v != "" {
		return v
	}
	return "request:" + findString(m, "request_id")
}

func stableWindowID(v string) string {
	v = strings.TrimSpace(v)
	if i := strings.IndexByte(v, ':'); i > 0 {
		return v[:i]
	}
	return v
}
func findString(v any, keys ...string) string {
	wanted := map[string]bool{}
	for _, k := range keys {
		wanted[k] = true
	}
	var walk func(any) string
	walk = func(x any) string {
		if m, ok := x.(map[string]any); ok {
			for k, v := range m {
				if wanted[strings.ToLower(k)] {
					if s, ok := v.(string); ok && s != "" {
						return s
					}
				}
			}
			for _, v := range m {
				if s := walk(v); s != "" {
					return s
				}
			}
		}
		return ""
	}
	return walk(v)
}
func enrichDesktopMetadata(rec *Record, h http.Header) {
	rec.Client = firstNonEmpty(h.Get("Originator"), h.Get("User-Agent"))
	rec.ThreadID = firstNonEmpty(h.Get("Thread-Id"), h.Get("X-Thread-Id"))
	rec.WindowID = h.Get("X-Codex-Window-Id")
	raw := h.Get("X-Codex-Turn-Metadata")
	if raw == "" {
		return
	}
	var meta struct {
		SessionID   string `json:"session_id"`
		ThreadID    string `json:"thread_id"`
		TurnID      string `json:"turn_id"`
		WindowID    string `json:"window_id"`
		RequestKind string `json:"request_kind"`
		Workspaces  map[string]struct {
			AssociatedRemoteURLs map[string]string `json:"associated_remote_urls"`
		} `json:"workspaces"`
	}
	if json.Unmarshal([]byte(raw), &meta) != nil {
		return
	}
	rec.ThreadID = firstNonEmpty(meta.ThreadID, meta.SessionID, rec.ThreadID)
	rec.TurnID = meta.TurnID
	rec.WindowID = firstNonEmpty(meta.WindowID, rec.WindowID)
	rec.RequestKind = meta.RequestKind
	paths := make([]string, 0, len(meta.Workspaces))
	for p := range meta.Workspaces {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	if len(paths) > 0 {
		rec.ProjectPath = paths[0]
		rec.ProjectName = filepath.Base(strings.ReplaceAll(paths[0], "\\", "/"))
		rem := meta.Workspaces[paths[0]].AssociatedRemoteURLs
		for _, k := range []string{"origin", "forgejo", "github", "llm"} {
			if rem[k] != "" {
				rec.GitRemote = rem[k]
				break
			}
		}
	}
}
func extractConversationSummary(body []byte) string {
	for _, path := range []string{"title", "conversation.title", "metadata.title", "client_metadata.title"} {
		if value := compactSummary(gjson.GetBytes(body, path).String()); value != "" {
			return value
		}
	}
	var root any
	if json.Unmarshal(body, &root) != nil {
		return ""
	}
	var candidates []string
	var walk func(any, bool)
	walk = func(value any, user bool) {
		switch item := value.(type) {
		case map[string]any:
			role, _ := item["role"].(string)
			isUser := user || strings.EqualFold(role, "user")
			if isUser {
				for _, key := range []string{"text", "prompt", "content", "input"} {
					walk(item[key], true)
				}
			}
			walk(item["input"], isUser || role == "")
			for _, key := range []string{"messages", "content"} {
				walk(item[key], isUser)
			}
		case []any:
			for _, child := range item {
				walk(child, user)
			}
		case string:
			if user {
				value := meaningfulSummary(item)
				if value != "" {
					candidates = append(candidates, value)
				}
			}
		}
	}
	walk(root, false)
	if len(candidates) == 0 {
		return ""
	}
	return candidates[len(candidates)-1]
}
func meaningfulSummary(value string) string {
	value = strings.TrimSpace(value)
	for {
		lower := strings.ToLower(value)
		matched := false
		for _, tag := range []string{"environment_context", "environment_info", "workspace_info", "in-app-browser-context", "app-context"} {
			open := "<" + tag
			if !strings.HasPrefix(lower, open) {
				continue
			}
			close := "</" + tag + ">"
			end := strings.Index(lower, close)
			if end < 0 {
				return ""
			}
			value = strings.TrimSpace(value[end+len(close):])
			matched = true
			break
		}
		if !matched {
			break
		}
	}
	lower := strings.ToLower(value)
	if strings.HasPrefix(lower, "# files mentioned by the user:") {
		marker := "## my request for codex:"
		if pos := strings.Index(lower, marker); pos >= 0 {
			value = strings.TrimSpace(value[pos+len(marker):])
		} else {
			return ""
		}
	}
	return compactSummary(value)
}
func compactSummary(value string) string {
	value = strings.Join(strings.Fields(value), " ")
	if value == "" {
		return ""
	}
	runes := []rune(value)
	if len(runes) > 160 {
		return string(runes[:160]) + "…"
	}
	return value
}
func enrichGenericFacets(rec *Record, h http.Header, body []byte) {
	if rec.Facets == nil {
		rec.Facets = map[string][]string{}
	}
	addFacet := func(k, v string) {
		v = strings.TrimSpace(v)
		if v == "" || len(v) > 2048 {
			return
		}
		for _, old := range rec.Facets[k] {
			if old == v {
				return
			}
		}
		rec.Facets[k] = append(rec.Facets[k], v)
	}
	var addResult func(string, gjson.Result)
	addResult = func(name string, result gjson.Result) {
		if result.IsArray() {
			for _, item := range result.Array() {
				addResult(name, item)
			}
			return
		}
		addFacet(name, result.String())
	}
	addPathValues := func(name, query string) {
		addResult(name, gjson.GetBytes(body, query))
	}
	for _, pair := range [][2]string{
		{"client", rec.Client}, {"client.user_agent", h.Get("User-Agent")},
		{"project.name", rec.ProjectName}, {"project.path", rec.ProjectPath}, {"git.remote", rec.GitRemote},
		{"session.id", rec.SessionID}, {"thread.id", rec.ThreadID}, {"turn.id", rec.TurnID}, {"window.id", rec.WindowID},
		{"request.kind", rec.RequestKind}, {"client.request_id", h.Get("X-Client-Request-Id")},
		{"request.id", h.Get("X-Request-Id")}, {"originator", h.Get("Originator")},
		{"sdk.language", h.Get("X-Stainless-Lang")}, {"sdk.package_version", h.Get("X-Stainless-Package-Version")},
		{"client.os", h.Get("X-Stainless-OS")}, {"client.arch", h.Get("X-Stainless-Arch")},
		{"runtime.name", h.Get("X-Stainless-Runtime")}, {"runtime.version", h.Get("X-Stainless-Runtime-Version")},
		{"anthropic.version", h.Get("Anthropic-Version")}, {"openai.organization", h.Get("OpenAI-Organization")},
		{"openai.project", h.Get("OpenAI-Project")},
	} {
		addFacet(pair[0], pair[1])
	}
	for _, name := range []string{"X-Claude-Code-Session-Id", "X-Session-Id", "Session-Id", "Thread-Id", "X-Thread-Id", "X-Conversation-Id", "X-Project-Id", "X-Workspace-Id"} {
		addFacet("header."+strings.ToLower(name), h.Get(name))
	}
	for _, spec := range [][2]string{
		{"session.id", "session_id"}, {"conversation.id", "conversation_id"}, {"thread.id", "thread_id"},
		{"project.name", "project.name"}, {"project.name", "project"}, {"project.id", "project_id"},
		{"workspace.name", "workspace.name"}, {"workspace.id", "workspace_id"},
		{"project.path", "project.path"}, {"project.path", "workspace.path"}, {"project.path", "cwd"},
		{"git.remote", "repository"}, {"git.remote", "repo"}, {"git.branch", "branch"},
		{"client.name", "metadata.client"}, {"client.name", "metadata.client_name"},
		{"client.version", "metadata.client_version"}, {"client.ide", "metadata.ide"},
		{"client.ide_version", "metadata.ide_version"}, {"client.os", "metadata.os"}, {"client.arch", "metadata.arch"},
		{"project.name", "metadata.project"}, {"workspace.name", "metadata.workspace"}, {"project.path", "metadata.cwd"},
		{"project.name", "client_metadata.project"}, {"workspace.name", "client_metadata.workspace"},
		{"reasoning.effort", "reasoning.effort"}, {"service.tier", "service_tier"}, {"tool.choice", "tool_choice"},
	} {
		addPathValues(spec[0], spec[1])
	}
	for _, spec := range [][2]string{
		{"tool.type", "tools.#.type"}, {"tool.name", "tools.#.name"}, {"tool.name", "tools.#.function.name"},
		{"input.type", "input.#.type"}, {"content.type", "input.#.content.#.type"},
		{"message.role", "messages.#.role"}, {"content.type", "messages.#.content.#.type"},
	} {
		addPathValues(spec[0], spec[1])
	}
	if rec.ProjectName == "" {
		rec.ProjectName = firstNonEmpty(firstFacet(rec.Facets, "project.name"), firstFacet(rec.Facets, "workspace.name"))
	}
	if rec.ProjectPath == "" {
		rec.ProjectPath = firstFacet(rec.Facets, "project.path")
	}
	if rec.GitRemote == "" {
		rec.GitRemote = firstFacet(rec.Facets, "git.remote")
	}
}
func firstFacet(facets map[string][]string, name string) string {
	if values := facets[name]; len(values) > 0 {
		return values[0]
	}
	return ""
}
func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}
func sanitizeMeta(m map[string]any) map[string]any {
	out := map[string]any{}
	for _, k := range []string{"key_id", "requested_model", "target_provider", "target_model", "group", "execution_session_id", "derived_session_id", "caller_scope", "request_path", "selected_auth_id"} {
		if v := findString(m, k); v != "" {
			out[k] = v
		}
	}
	return out
}
