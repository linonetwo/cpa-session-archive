package archive

import (
	"context"
	"database/sql"
	"time"
)

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
