package archive

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const pluginID = "cpa-session-archive"

//go:embed web/index.html
var managementHTML []byte

type managementRequest struct {
	Method  string
	Path    string
	Headers http.Header
	Query   url.Values
	Body    []byte
}
type managementResponse struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
}

func (p *Plugin) managementRegistration() map[string]any {
	base := "/plugins/" + pluginID
	return map[string]any{"Routes": []map[string]string{{"Method": "GET", "Path": base + "/stats"}, {"Method": "GET", "Path": base + "/facets"}, {"Method": "GET", "Path": base + "/sessions"}, {"Method": "GET", "Path": base + "/turns"}, {"Method": "GET", "Path": base + "/requests"}, {"Method": "GET", "Path": base + "/request-context"}, {"Method": "GET", "Path": base + "/request-view"}, {"Method": "GET", "Path": base + "/export"}}, "Resources": []map[string]string{{"Path": "/index.html", "Menu": "会话归档", "Description": "Browse archived projects, sessions and training data."}}}
}
func (p *Plugin) handleManagement(raw []byte) ([]byte, error) {
	var r managementRequest
	if e := json.Unmarshal(raw, &r); e != nil {
		return nil, e
	}
	path := strings.TrimRight(r.Path, "/")
	resource := "/v0/resource/plugins/" + pluginID
	if r.Method == http.MethodGet && strings.HasPrefix(path, resource) {
		status := http.StatusNotFound
		body := []byte("not found")
		if strings.TrimPrefix(path, resource) == "/index.html" {
			status = http.StatusOK
			body = managementHTML
		}
		return ok(managementResponse{StatusCode: status, Headers: http.Header{"Content-Type": []string{"text/html; charset=utf-8"}}, Body: body})
	}
	base := "/v0/management/plugins/" + pluginID
	var target string
	drop := map[string]bool{}
	switch {
	case r.Method == http.MethodGet && path == base+"/stats":
		target = "/v1/stats"
	case r.Method == http.MethodGet && path == base+"/facets":
		target = "/v1/facets"
	case r.Method == http.MethodGet && path == base+"/sessions":
		if id := r.Query.Get("id"); id != "" {
			target = "/v1/sessions/" + url.PathEscape(id)
			drop["id"] = true
		} else {
			target = "/v1/sessions"
		}
	case r.Method == http.MethodGet && path == base+"/requests":
		target = "/v1/requests/" + url.PathEscape(r.Query.Get("id"))
		drop["id"] = true
	case r.Method == http.MethodGet && path == base+"/request-context":
		target = "/v1/request-context"
	case r.Method == http.MethodGet && path == base+"/request-view":
		target = "/v1/request-view"
	case r.Method == http.MethodGet && path == base+"/turns":
		target = "/v1/turns"
	case r.Method == http.MethodGet && path == base+"/export":
		target = "/v1/export-tickets"
		if id := r.Query.Get("id"); id != "" {
			r.Query.Set("session_id", id)
		}
		drop["id"] = true
	default:
		return ok(managementResponse{StatusCode: 404, Body: []byte("{\"error\":\"not found\"}")})
	}
	u := strings.TrimSuffix(p.endpoint, "/ingest") + target
	if len(r.Query) > 0 {
		q := url.Values{}
		for k, v := range r.Query {
			if !drop[k] {
				q[k] = v
			}
		}
		if len(q) > 0 {
			u += "?" + q.Encode()
		}
	}
	req, _ := http.NewRequest(http.MethodGet, u, nil)
	resp, e := p.client.Do(req)
	if e != nil {
		body, _ := json.Marshal(map[string]string{"error": "collector unavailable", "detail": fmt.Sprintf("%s: %v", u, e)})
		return ok(managementResponse{StatusCode: 502, Headers: http.Header{"Content-Type": []string{"application/json"}}, Body: body})
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	headers := http.Header{"Content-Type": []string{resp.Header.Get("Content-Type")}}
	if v := resp.Header.Get("Content-Disposition"); v != "" {
		headers.Set("Content-Disposition", v)
	}
	return ok(managementResponse{StatusCode: resp.StatusCode, Headers: headers, Body: body})
}
