package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"
)

func indexSessionRecord(tx *sql.Tx, r Record) error {
	if r.RequestID == "" || r.SessionID == "" {
		return nil
	}
	inserted, err := tx.Exec(`INSERT OR IGNORE INTO session_indexed_requests(request_id,session_id) VALUES(?,?)`, r.RequestID, r.SessionID)
	if err != nil {
		return err
	}
	n, err := inserted.RowsAffected()
	if err != nil || n == 0 {
		return err
	}
	started := r.StartedAt.Format(time.RFC3339Nano)
	completed := r.CompletedAt.Format(time.RFC3339Nano)
	project := firstNonEmpty(r.ProjectName, firstFacet(r.Facets, "project.name"), firstFacet(r.Facets, "workspace.name"))
	keyID := firstNonEmpty(r.KeyID, firstFacet(r.Facets, "caller.scope"))
	_, err = tx.Exec(`INSERT INTO session_summaries(session_id,requests,first_at,last_at,key_id,model,project,summary,summary_at)
		VALUES(?,1,?,?,?,?,?,?,?)
		ON CONFLICT(session_id) DO UPDATE SET
		requests=session_summaries.requests+1,
		first_at=MIN(session_summaries.first_at,excluded.first_at),
		last_at=MAX(session_summaries.last_at,excluded.last_at),
		key_id=CASE WHEN session_summaries.key_id='' THEN excluded.key_id ELSE session_summaries.key_id END,
		model=CASE WHEN excluded.last_at>=session_summaries.last_at AND excluded.model<>'' THEN excluded.model ELSE session_summaries.model END,
		project=CASE WHEN session_summaries.project='' THEN excluded.project ELSE session_summaries.project END,
		summary=CASE WHEN excluded.summary<>'' AND (session_summaries.summary='' OR excluded.summary_at<session_summaries.summary_at) THEN excluded.summary ELSE session_summaries.summary END,
		summary_at=CASE WHEN excluded.summary<>'' AND (session_summaries.summary='' OR excluded.summary_at<session_summaries.summary_at) THEN excluded.summary_at ELSE session_summaries.summary_at END`,
		r.SessionID, started, completed, keyID, r.RequestedModel, project, r.Summary, started)
	if err != nil {
		return err
	}
	for name, values := range r.Facets {
		for _, value := range values {
			if value == "" {
				continue
			}
			if _, err = tx.Exec(`INSERT OR IGNORE INTO session_facets(session_id,name,value) VALUES(?,?,?)`, r.SessionID, name, value); err != nil {
				return err
			}
		}
	}
	return nil
}

// BackfillSessionIndex reads request IDs from the small covering index and
// fetches only records that have not yet entered the projection. It is safe to
// resume after a restart and never rewrites archived payload blobs.
func (s *Store) BackfillSessionIndex(ctx context.Context) error {
	rows, err := s.DB.QueryContext(ctx, `SELECT r.request_id FROM records r LEFT JOIN session_indexed_requests i ON i.request_id=r.request_id WHERE i.request_id IS NULL`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Close()
	if err != nil || len(ids) == 0 {
		return err
	}
	log.Printf("session index backfill started: %d requests", len(ids))
	for offset := 0; offset < len(ids); offset += 8 {
		if err = ctx.Err(); err != nil {
			return err
		}
		end := offset + 8
		if end > len(ids) {
			end = len(ids)
		}
		tx, beginErr := s.DB.BeginTx(ctx, nil)
		if beginErr != nil {
			return beginErr
		}
		for _, id := range ids[offset:end] {
			var r Record
			var started, completed, facets string
			err = tx.QueryRowContext(ctx, `SELECT request_id,session_id,COALESCE(key_id,''),COALESCE(summary,''),COALESCE(requested_model,''),started_at,completed_at,COALESCE(facets_json,'') FROM records WHERE request_id=?`, id).
				Scan(&r.RequestID, &r.SessionID, &r.KeyID, &r.Summary, &r.RequestedModel, &started, &completed, &facets)
			if err != nil {
				tx.Rollback()
				return err
			}
			r.StartedAt, _ = time.Parse(time.RFC3339Nano, started)
			r.CompletedAt, _ = time.Parse(time.RFC3339Nano, completed)
			_ = json.Unmarshal([]byte(facets), &r.Facets)
			if err = indexSessionRecord(tx, r); err != nil {
				tx.Rollback()
				return err
			}
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		if end%320 == 0 || end == len(ids) {
			log.Printf("session index backfill: %d/%d", end, len(ids))
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// NormalizeHistoricalSSE rewrites only successful legacy SSE responses. It
// uses the small request projection as a work list, so it never scans the
// payload-heavy records table. Failed and partial streams are retained.
func (s *Store) NormalizeHistoricalSSE(ctx context.Context) error {
	rows, err := s.DB.QueryContext(ctx, `SELECT i.request_id FROM session_indexed_requests i LEFT JOIN normalized_response_requests n ON n.request_id=i.request_id WHERE n.request_id IS NULL ORDER BY i.request_id`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	changed := 0
	for offset := 0; offset < len(ids); offset += 32 {
		if err = ctx.Err(); err != nil {
			return err
		}
		tx, beginErr := s.DB.BeginTx(ctx, nil)
		if beginErr != nil {
			return beginErr
		}
		end := offset + 32
		if end > len(ids) {
			end = len(ids)
		}
		for _, id := range ids[offset:end] {
			var root string
			if err = tx.QueryRowContext(ctx, `SELECT COALESCE(response_ref,'') FROM records WHERE request_id=?`, id).Scan(&root); err != nil {
				tx.Rollback()
				return err
			}
			if root != "" {
				manifest, loadErr := loadBlobTx(tx, root)
				if loadErr != nil {
					tx.Rollback()
					return loadErr
				}
				var ref blobRef
				if json.Unmarshal(manifest, &ref) == nil && ref.Encoding == "raw" && ref.Blob != "" {
					raw, loadErr := loadBlobTx(tx, ref.Blob)
					if loadErr != nil {
						tx.Rollback()
						return loadErr
					}
					if terminal, ok := terminalSSEPayload(raw); ok {
						newRoot, putErr := putPayload(tx, terminal)
						if putErr != nil {
							tx.Rollback()
							return putErr
						}
						if _, putErr = tx.Exec(`UPDATE records SET response_ref=? WHERE request_id=? AND response_ref=?`, newRoot, id, root); putErr != nil {
							tx.Rollback()
							return putErr
						}
						changed++
					}
				}
			}
			if _, err = tx.Exec(`INSERT OR IGNORE INTO normalized_response_requests(request_id) VALUES(?)`, id); err != nil {
				tx.Rollback()
				return err
			}
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		if end%320 == 0 || end == len(ids) {
			log.Printf("historical SSE normalization: scanned=%d/%d rewritten=%d", end, len(ids), changed)
		}
		time.Sleep(25 * time.Millisecond)
	}
	log.Printf("historical SSE normalization complete: %d/%d responses rewritten", changed, len(ids))
	return nil
}

// RepairSessionSummaries replaces transport boilerplate chosen by older
// extractors with the last meaningful user message from the first request.
func (s *Store) RepairSessionSummaries(ctx context.Context) error {
	var version int
	_ = s.DB.QueryRowContext(ctx, `SELECT version FROM repair_versions WHERE name='session_summary'`).Scan(&version)
	where := ` WHERE s.summary='' OR lower(s.summary) LIKE '<environment_%' OR lower(s.summary) LIKE '<workspace_info>%' OR lower(s.summary) LIKE '<in-app-browser-context%' OR lower(s.summary) LIKE '<app-context>%' OR lower(s.summary) LIKE '<context>%' OR lower(s.summary) LIKE '&lt;context&gt;%' OR lower(s.summary) LIKE '# files mentioned by the user:%'`
	if version < 3 {
		where = ""
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT s.session_id,COALESCE((SELECT original_ref FROM records r WHERE r.session_id=s.session_id ORDER BY r.started_at LIMIT 1),'') FROM session_summaries s`+where)
	if err != nil {
		return err
	}
	type candidate struct{ sessionID, originalRef string }
	var candidates []candidate
	for rows.Next() {
		var item candidate
		if err = rows.Scan(&item.sessionID, &item.originalRef); err != nil {
			rows.Close()
			return err
		}
		candidates = append(candidates, item)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	repaired := 0
	for _, item := range candidates {
		if item.originalRef == "" {
			continue
		}
		body, loadErr := s.LoadPayload(item.originalRef)
		if loadErr != nil {
			return loadErr
		}
		summary := extractConversationSummaryFirst(body)
		if summary == "" {
			continue
		}
		if _, err = s.DB.ExecContext(ctx, `UPDATE session_summaries SET summary=?,summary_at=first_at WHERE session_id=?`, summary, item.sessionID); err != nil {
			return err
		}
		repaired++
	}
	_, _ = s.DB.ExecContext(ctx, `INSERT INTO repair_versions(name,version) VALUES('session_summary',3) ON CONFLICT(name) DO UPDATE SET version=excluded.version`)
	log.Printf("session summary repair complete: %d/%d updated", repaired, len(candidates))
	return nil
}

// RepairRecordPreviews fills lightweight per-request previews once. The
// marker table prevents non-conversational records from being re-expanded on
// every restart.
func (s *Store) RepairRecordPreviews(ctx context.Context) error {
	const previewVersion = 4
	rows, err := s.DB.QueryContext(ctx, `SELECT r.request_id,r.session_id,COALESCE(r.original_ref,''),COALESCE(r.response_ref,''),COALESCE(r.facets_json,'') FROM records r LEFT JOIN previewed_requests p ON p.request_id=r.request_id WHERE p.request_id IS NULL OR COALESCE(p.version,0)<? ORDER BY r.id`, previewVersion)
	if err != nil {
		return err
	}
	type candidate struct{ requestID, sessionID, originalRef, responseRef, facets string }
	var candidates []candidate
	for rows.Next() {
		var item candidate
		if err = rows.Scan(&item.requestID, &item.sessionID, &item.originalRef, &item.responseRef, &item.facets); err != nil {
			rows.Close()
			return err
		}
		candidates = append(candidates, item)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	for offset := 0; offset < len(candidates); offset += 16 {
		if err = ctx.Err(); err != nil {
			return err
		}
		end := offset + 16
		if end > len(candidates) {
			end = len(candidates)
		}
		tx, beginErr := s.DB.BeginTx(ctx, nil)
		if beginErr != nil {
			return beginErr
		}
		for _, item := range candidates[offset:end] {
			var summary, responsePreview, threadSource string
			if item.originalRef != "" {
				if body, loadErr := s.LoadPayload(item.originalRef); loadErr == nil {
					summary = extractConversationSummary(body)
					threadSource = extractThreadSource(body)
				}
			}
			if item.responseRef != "" {
				if body, loadErr := s.LoadPayload(item.responseRef); loadErr == nil {
					responsePreview = extractResponsePreview(body)
				}
			}
			facets := map[string][]string{}
			_ = json.Unmarshal([]byte(item.facets), &facets)
			if facets == nil {
				facets = map[string][]string{}
			}
			if threadSource != "" {
				facets["thread.source"] = appendUnique(facets["thread.source"], threadSource)
			}
			if _, err = tx.Exec(`UPDATE records SET summary=CASE WHEN ?<>'' THEN ? ELSE summary END,response_preview=CASE WHEN ?<>'' THEN ? ELSE response_preview END,facets_json=? WHERE request_id=?`, summary, summary, responsePreview, responsePreview, facetsJSON(facets), item.requestID); err != nil {
				tx.Rollback()
				return err
			}
			if threadSource != "" {
				if _, err = tx.Exec(`INSERT OR IGNORE INTO record_facets(request_id,name,value) VALUES(?,'thread.source',?)`, item.requestID, threadSource); err != nil {
					tx.Rollback()
					return err
				}
				if _, err = tx.Exec(`INSERT OR IGNORE INTO session_facets(session_id,name,value) VALUES(?,'thread.source',?)`, item.sessionID, threadSource); err != nil {
					tx.Rollback()
					return err
				}
			}
			if _, err = tx.Exec(`INSERT INTO previewed_requests(request_id,version) VALUES(?,?) ON CONFLICT(request_id) DO UPDATE SET version=excluded.version`, item.requestID, previewVersion); err != nil {
				tx.Rollback()
				return err
			}
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		if end%320 == 0 || end == len(candidates) {
			log.Printf("request preview repair: %d/%d", end, len(candidates))
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, _ = s.DB.ExecContext(ctx, `INSERT INTO repair_versions(name,version) VALUES('record_preview',?) ON CONFLICT(name) DO UPDATE SET version=excluded.version`, previewVersion)
	return nil
}

func loadBlobTx(tx *sql.Tx, hash string) ([]byte, error) {
	var data []byte
	var codec string
	if err := tx.QueryRow(`SELECT codec,data FROM blobs WHERE hash=?`, hash).Scan(&codec, &data); err != nil {
		return nil, err
	}
	if codec == "gzip" {
		return gunzipBytes(data), nil
	}
	return data, nil
}
