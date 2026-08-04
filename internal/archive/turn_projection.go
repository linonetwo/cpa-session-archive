package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"
)

type turnFacetProjectionItem struct {
	id     int64
	turnID string
	tools  string
	kinds  string
}

type turnProjectionRecord struct {
	id              int64
	requestID       string
	sessionID       string
	keyID           sql.NullString
	summary         sql.NullString
	responsePreview sql.NullString
	requestedModel  sql.NullString
	model           sql.NullString
	outcome         sql.NullString
	statusCode      sql.NullInt64
	startedAt       sql.NullString
	completedAt     sql.NullString
	facetsJSON      sql.NullString
}

// BackfillTurnFacetProjection extracts only the three facets needed to group a
// human-readable conversation. It processes newest records first and commits
// small batches so ingestion remains available while an existing database is
// upgraded.
func (s *Store) BackfillTurnFacetProjection(ctx context.Context, batchSize int, pause time.Duration) error {
	if batchSize < 1 {
		batchSize = 64
	}
	beforeID := int64(^uint64(0) >> 1)
	for {
		rows, err := s.DB.QueryContext(ctx, `SELECT id,COALESCE(facets_json,'{}')
			FROM turn_records
			WHERE id<? AND (turn_id IS NULL OR tool_names_json IS NULL OR request_kinds_json IS NULL)
			ORDER BY id DESC LIMIT ?`, beforeID, batchSize)
		if err != nil {
			return err
		}
		batch := make([]turnFacetProjectionItem, 0, batchSize)
		for rows.Next() {
			var id int64
			var raw string
			if err = rows.Scan(&id, &raw); err != nil {
				rows.Close()
				return err
			}
			facets := map[string][]string{}
			_ = json.Unmarshal([]byte(raw), &facets)
			turnID, tools, kinds := compactTurnFacets(facets)
			batch = append(batch, turnFacetProjectionItem{id: id, turnID: turnID, tools: tools, kinds: kinds})
		}
		if err = rows.Close(); err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		if err = s.writeTurnFacetProjectionBatch(ctx, batch); err != nil {
			return err
		}
		beforeID = batch[len(batch)-1].id
		if pause > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pause):
			}
		}
	}
}

func (s *Store) writeTurnFacetProjectionBatch(ctx context.Context, batch []turnFacetProjectionItem) error {
	var err error
	for attempt := 0; attempt < 30; attempt++ {
		err = s.tryWriteTurnFacetProjectionBatch(ctx, batch)
		if err == nil {
			return nil
		}
		if !isSQLiteBusy(err) {
			return err
		}
		delay := time.Duration(attempt+1) * 50 * time.Millisecond
		if delay > time.Second {
			delay = time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return err
}

func (s *Store) tryWriteTurnFacetProjectionBatch(ctx context.Context, batch []turnFacetProjectionItem) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `UPDATE turn_records
		SET turn_id=?,tool_names_json=?,request_kinds_json=? WHERE id=?`)
	if err != nil {
		return err
	}
	for _, value := range batch {
		if _, err = stmt.ExecContext(ctx, value.turnID, value.tools, value.kinds, value.id); err != nil {
			stmt.Close()
			return err
		}
	}
	if err = stmt.Close(); err != nil {
		return err
	}
	return tx.Commit()
}

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") ||
		strings.Contains(message, "database is busy")
}

// BackfillTurnProjection copies only the compact fields used by the human
// timeline. Active sessions are processed first and each write transaction is
// deliberately small, so a historical database with inline BLOBs remains
// available for ingestion and health checks throughout the migration.
func (s *Store) BackfillTurnProjection(ctx context.Context, batchSize int, pause time.Duration) error {
	if batchSize < 1 {
		batchSize = 64
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT session_id FROM session_summaries ORDER BY last_at DESC`)
	if err != nil {
		return err
	}
	var sessionIDs []string
	for rows.Next() {
		var sessionID string
		if err = rows.Scan(&sessionID); err != nil {
			rows.Close()
			return err
		}
		sessionIDs = append(sessionIDs, sessionID)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	for _, sessionID := range sessionIDs {
		if err = s.backfillTurnSession(ctx, sessionID, batchSize, pause); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) backfillTurnSession(ctx context.Context, sessionID string, batchSize int, pause time.Duration) error {
	var projected, total int
	if err := s.DB.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM turn_records WHERE session_id=?),
		(SELECT COUNT(*) FROM records WHERE session_id=?)`, sessionID, sessionID).Scan(&projected, &total); err != nil {
		return err
	}
	if projected == total {
		return nil
	}
	var lastStarted string
	var lastID int64
	for {
		rows, err := s.DB.QueryContext(ctx, `SELECT
			id,request_id,session_id,key_id,summary,response_preview,requested_model,model,outcome,status_code,started_at,completed_at,facets_json
			FROM records
			WHERE session_id=? AND (started_at>? OR (started_at=? AND id>?))
			ORDER BY started_at,id LIMIT ?`, sessionID, lastStarted, lastStarted, lastID, batchSize)
		if err != nil {
			return err
		}
		batch := make([]turnProjectionRecord, 0, batchSize)
		for rows.Next() {
			var item turnProjectionRecord
			if err = rows.Scan(
				&item.id, &item.requestID, &item.sessionID, &item.keyID, &item.summary,
				&item.responsePreview, &item.requestedModel, &item.model, &item.outcome,
				&item.statusCode, &item.startedAt, &item.completedAt, &item.facetsJSON,
			); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, item)
		}
		if err = rows.Close(); err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil
		}
		tx, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO turn_records(
			id,request_id,session_id,key_id,summary,response_preview,requested_model,model,outcome,status_code,started_at,completed_at,facets_json
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(request_id) DO UPDATE SET
			session_id=excluded.session_id,key_id=excluded.key_id,summary=excluded.summary,response_preview=excluded.response_preview,
			requested_model=excluded.requested_model,model=excluded.model,outcome=excluded.outcome,status_code=excluded.status_code,
			started_at=excluded.started_at,completed_at=excluded.completed_at,facets_json=excluded.facets_json`)
		if err != nil {
			tx.Rollback()
			return err
		}
		for _, item := range batch {
			if _, err = stmt.ExecContext(
				ctx, item.id, item.requestID, item.sessionID, item.keyID, item.summary,
				item.responsePreview, item.requestedModel, item.model, item.outcome,
				item.statusCode, item.startedAt, item.completedAt, item.facetsJSON,
			); err != nil {
				stmt.Close()
				tx.Rollback()
				return err
			}
		}
		if err = stmt.Close(); err != nil {
			tx.Rollback()
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		last := batch[len(batch)-1]
		lastStarted = last.startedAt.String
		lastID = last.id
		if len(batch) < batchSize {
			return nil
		}
		if pause > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pause):
			}
		}
	}
}
