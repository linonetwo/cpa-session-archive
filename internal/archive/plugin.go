package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
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
	return &Plugin{active: map[string]*state{}, stop: make(chan struct{}), client: &http.Client{Timeout: 5 * time.Second}, max: 64 << 20}
}
func ok(v any) ([]byte, error) { return json.Marshal(envelope{OK: true, Result: v}) }
func (p *Plugin) Handle(method string, raw []byte) ([]byte, error) {
	switch method {
	case "plugin.register", "plugin.reconfigure":
		return p.configure(raw)
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
	p.endpoint = strings.TrimRight(c.Endpoint, "/") + "/ingest"
	p.storeUpstream = c.StoreUpstreamRequest
	if p.q == nil {
		p.q = make(chan Record, c.QueueSize)
		p.wg.Add(1)
		go p.sender()
	}
	reg := map[string]any{"schema_version": 2, "metadata": map[string]any{"Name": "cpa-session-archive", "Version": "0.2.0", "Author": "OneTwo", "GitHubRepository": "https://github.com/linonetwo/cpa-session-archive", "ConfigFields": []any{}}, "capabilities": map[string]any{"request_interceptor": true, "request_lifecycle_plugin": true, "response_interceptor": true, "response_stream_interceptor": true}}
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
	if afterAuth {
		if p.storeUpstream && len(r.Body) > 0 {
			s.UpstreamRequest = limit(r.Body, p.max, &s.Truncated)
		}
	} else if len(r.Body) > 0 {
		s.OriginalRequest = limit(r.Body, p.max, &s.Truncated)
	}
	s.SessionID = sessionID(r.Metadata, s.OriginalRequest, s.UpstreamRequest)
	s.ParentResponseID = firstJSON(s.OriginalRequest, s.UpstreamRequest, "previous_response_id")
	s.KeyID = findString(r.Metadata, "key_id", "principal")
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
		s.SessionID = sessionID(r.Metadata, s.OriginalRequest, s.UpstreamRequest)
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
	select {
	case p.q <- s.Record:
	default:
	}
}
func (p *Plugin) sender() {
	defer p.wg.Done()
	for {
		select {
		case rec := <-p.q:
			b, _ := json.Marshal(rec)
			req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, p.endpoint, bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			resp, e := p.client.Do(req)
			if e == nil {
				resp.Body.Close()
			}
		case <-p.stop:
			return
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
func sessionID(m map[string]any, bs ...[]byte) string {
	if v := findString(m, "execution_session_id", "derived_session_id"); v != "" {
		return v
	}
	for _, b := range bs {
		for _, k := range []string{"session_id", "prompt_cache_key", "client_metadata.x-codex-window-id"} {
			if v := gjson.GetBytes(b, k).String(); v != "" {
				return v
			}
		}
		u := gjson.GetBytes(b, "metadata.user_id").String()
		if strings.HasPrefix(u, "{") {
			if v := gjson.Get(u, "session_id").String(); v != "" {
				return v
			}
		}
	}
	return "request:" + findString(m, "request_id")
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
func sanitizeMeta(m map[string]any) map[string]any {
	out := map[string]any{}
	for _, k := range []string{"key_id", "requested_model", "target_provider", "target_model", "group", "execution_session_id", "derived_session_id", "caller_scope", "request_path", "selected_auth_id"} {
		if v := findString(m, k); v != "" {
			out[k] = v
		}
	}
	return out
}
