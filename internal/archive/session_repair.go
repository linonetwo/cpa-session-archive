package archive

import (
	"context"
	"encoding/json"
	"time"
)

type canonicalRepair struct{ requestID, oldID, newID, facets string }

// RepairCanonicalSessions merges records which older versions grouped by a
// transient execution_session_id.  It changes only searchable projections and
// session_id metadata; CAS payloads are never rewritten.
func (s *Store) RepairCanonicalSessions(ctx context.Context) (int, error) {
	const repairVersion = 1
	var version int
	_ = s.DB.QueryRowContext(ctx, `SELECT version FROM repair_versions WHERE name='canonical_session'`).Scan(&version)
	if version >= repairVersion {
		return 0, nil
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT r.request_id,r.session_id,COALESCE(r.facets_json,''),COALESCE(
		(SELECT value FROM record_facets f WHERE f.request_id=r.request_id AND f.name='thread.id' AND value<>'' LIMIT 1),
		(SELECT value FROM record_facets f WHERE f.request_id=r.request_id AND f.name='header.thread-id' AND value<>'' LIMIT 1),
		(SELECT value FROM record_facets f WHERE f.request_id=r.request_id AND f.name='header.session-id' AND value<>'' LIMIT 1), '') AS canonical
		FROM records r`)
	if err != nil {
		return 0, err
	}
	var repairs []canonicalRepair
	for rows.Next() {
		var x canonicalRepair
		if err = rows.Scan(&x.requestID, &x.oldID, &x.facets, &x.newID); err != nil {
			rows.Close()
			return 0, err
		}
		x.newID = stableWindowID(x.newID)
		if x.newID != "" && x.newID != x.oldID {
			repairs = append(repairs, x)
		}
	}
	if err = rows.Close(); err != nil {
		return 0, err
	}
	if len(repairs) == 0 {
		_, err = s.DB.ExecContext(ctx, `INSERT INTO repair_versions(name,version) VALUES('canonical_session',?) ON CONFLICT(name) DO UPDATE SET version=excluded.version`, repairVersion)
		return 0, err
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	for _, x := range repairs {
		facets := map[string][]string{}
		_ = json.Unmarshal([]byte(x.facets), &facets)
		facets["session.id"] = []string{x.newID}
		if x.oldID != "" && x.oldID != x.newID {
			facets["execution.session.id"] = appendUnique(facets["execution.session.id"], x.oldID)
		}
		if _, err = tx.ExecContext(ctx, `UPDATE records SET session_id=?,facets_json=? WHERE request_id=?`, x.newID, facetsJSON(facets), x.requestID); err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE turn_records SET session_id=?,facets_json=? WHERE request_id=?`, x.newID, facetsJSON(facets), x.requestID); err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM record_facets WHERE request_id=? AND name='session.id'`, x.requestID); err != nil {
			return 0, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO record_facets(request_id,name,value) VALUES(?,'session.id',?)`, x.requestID, x.newID); err != nil {
			return 0, err
		}
		if x.oldID != "" && x.oldID != x.newID {
			if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO record_facets(request_id,name,value) VALUES(?,'execution.session.id',?)`, x.requestID, x.oldID); err != nil {
				return 0, err
			}
		}
	}
	for _, table := range []string{"session_indexed_requests", "session_summaries", "session_facets"} {
		if _, err = tx.ExecContext(ctx, `DELETE FROM `+table); err != nil {
			return 0, err
		}
	}
	all, err := tx.QueryContext(ctx, `SELECT request_id,session_id,COALESCE(key_id,''),COALESCE(summary,''),COALESCE(requested_model,''),started_at,completed_at,COALESCE(facets_json,'') FROM records ORDER BY id`)
	if err != nil {
		return 0, err
	}
	var records []Record
	for all.Next() {
		var r Record
		var started, completed, facets string
		if err = all.Scan(&r.RequestID, &r.SessionID, &r.KeyID, &r.Summary, &r.RequestedModel, &started, &completed, &facets); err != nil {
			all.Close()
			return 0, err
		}
		r.StartedAt, _ = time.Parse(time.RFC3339Nano, started)
		r.CompletedAt, _ = time.Parse(time.RFC3339Nano, completed)
		_ = json.Unmarshal([]byte(facets), &r.Facets)
		records = append(records, r)
	}
	if err = all.Close(); err != nil {
		return 0, err
	}
	for _, r := range records {
		if err = indexSessionRecord(tx, r); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	_, err = s.DB.ExecContext(ctx, `INSERT INTO repair_versions(name,version) VALUES('canonical_session',?) ON CONFLICT(name) DO UPDATE SET version=excluded.version`, repairVersion)
	if err != nil {
		return 0, err
	}
	return len(repairs), nil
}

func appendUnique(values []string, value string) []string {
	for _, old := range values {
		if old == value {
			return values
		}
	}
	return append(values, value)
}
