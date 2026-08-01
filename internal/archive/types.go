package archive

import "time"

type Record struct {
	RequestID        string         `json:"request_id"`
	TraceID          string         `json:"trace_id,omitempty"`
	SessionID        string         `json:"session_id"`
	ParentResponseID string         `json:"parent_response_id,omitempty"`
	ResponseID       string         `json:"response_id,omitempty"`
	KeyID            string         `json:"key_id,omitempty"`
	Summary          string         `json:"summary,omitempty"`
	ProjectPath      string         `json:"project_path,omitempty"`
	ProjectName      string         `json:"project_name,omitempty"`
	GitRemote        string         `json:"git_remote,omitempty"`
	ThreadID         string         `json:"thread_id,omitempty"`
	TurnID           string         `json:"turn_id,omitempty"`
	WindowID         string         `json:"window_id,omitempty"`
	RequestKind      string         `json:"request_kind,omitempty"`
	Client           string         `json:"client,omitempty"`
	SourceFormat     string         `json:"source_format,omitempty"`
	RequestedModel   string         `json:"requested_model,omitempty"`
	Model            string         `json:"model,omitempty"`
	Stream           bool           `json:"stream"`
	Outcome          string         `json:"outcome"`
	StatusCode       int            `json:"status_code"`
	Error            string         `json:"error,omitempty"`
	StartedAt        time.Time      `json:"started_at"`
	CompletedAt      time.Time      `json:"completed_at"`
	OriginalRequest  []byte         `json:"original_request,omitempty"`
	UpstreamRequest  []byte         `json:"upstream_request,omitempty"`
	Response         []byte         `json:"response,omitempty"`
	Truncated        bool           `json:"truncated,omitempty"`
	Metadata         map[string]any `json:"metadata,omitempty"`
	Facets           map[string][]string `json:"facets,omitempty"`
}
