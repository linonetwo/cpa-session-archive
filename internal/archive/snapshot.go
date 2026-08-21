package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

const StableCursorProtocol = "session-snapshot-cursor-v1"

var (
	ErrSnapshotProjectionNotReady = errors.New("stable session projection is not ready")
	ErrSnapshotCursor             = errors.New("stable session cursor is invalid")
)

type StableSessionSummary struct {
	SessionID     string `json:"session_id"`
	Requests      int    `json:"requests"`
	FirstAt       string `json:"first_at"`
	LastAt        string `json:"last_at"`
	RecordsSHA256 string `json:"records_sha256"`
}

type StableSessionSnapshot struct {
	mu               sync.Mutex
	tx               *sql.Tx
	closed           bool
	ingestFence      int64
	sessions         []StableSessionSummary
	requestCount     int
	sessionSetSHA256 string
}

func (s *Store) BeginStableSessionSnapshot(lowerBound time.Time, afterIngestFence *int64) (*StableSessionSnapshot, error) {
	tx, err := s.DB.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()

	var ingestFence int64
	if err = tx.QueryRowContext(context.Background(), `SELECT sequence FROM archive_ingest_clock WHERE id=1`).Scan(&ingestFence); err != nil {
		return nil, err
	}
	useDelta := 0
	after := int64(0)
	if afterIngestFence != nil {
		if *afterIngestFence < 0 || *afterIngestFence > ingestFence {
			return nil, ErrSnapshotCursor
		}
		useDelta = 1
		after = *afterIngestFence
	}
	lower := canonicalTimestamp(lowerBound)
	rows, err := tx.QueryContext(context.Background(), `WITH selected AS (
		SELECT DISTINCT r.session_id
		FROM records r
		WHERE ?=0
		   OR julianday(r.completed_at)>=julianday(?)
		   OR EXISTS(
				SELECT 1 FROM archive_ingest_events changed
				WHERE changed.session_id=r.session_id AND changed.sequence>? AND changed.sequence<=?
		   )
	)
	SELECT r.session_id,COUNT(*),MIN(r.started_at),MAX(r.completed_at),
		COALESCE((SELECT MAX(e.sequence) FROM archive_ingest_events e WHERE e.session_id=r.session_id AND e.sequence<=?),0),
		COALESCE(d.records_sha256,''),COALESCE(d.max_ingest_sequence,-1)
	FROM records r
	JOIN selected x ON x.session_id=r.session_id
	LEFT JOIN session_export_digests d ON d.session_id=r.session_id
	GROUP BY r.session_id
	ORDER BY MAX(r.completed_at) DESC,r.session_id COLLATE BINARY ASC`, useDelta, lower, after, ingestFence, ingestFence)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sessions := make([]StableSessionSummary, 0)
	requestCount := 0
	for rows.Next() {
		var item StableSessionSummary
		var firstAt, lastAt string
		var sessionFence, digestFence int64
		if err = rows.Scan(&item.SessionID, &item.Requests, &firstAt, &lastAt, &sessionFence, &item.RecordsSHA256, &digestFence); err != nil {
			return nil, err
		}
		if !validStableSessionID(item.SessionID) {
			return nil, fmt.Errorf("stable session id is outside the protocol limits")
		}
		first, firstErr := time.Parse(time.RFC3339Nano, firstAt)
		last, lastErr := time.Parse(time.RFC3339Nano, lastAt)
		if firstErr != nil || lastErr != nil {
			return nil, fmt.Errorf("stable session timestamp is invalid")
		}
		item.FirstAt = canonicalTimestamp(first)
		item.LastAt = canonicalTimestamp(last)
		if digestFence != sessionFence || len(item.RecordsSHA256) != 64 {
			return nil, ErrSnapshotProjectionNotReady
		}
		if _, err = hex.DecodeString(item.RecordsSHA256); err != nil {
			return nil, ErrSnapshotProjectionNotReady
		}
		sessions = append(sessions, item)
		requestCount += item.Requests
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	sort.SliceStable(sessions, func(left, right int) bool {
		if sessions[left].LastAt != sessions[right].LastAt {
			return sessions[left].LastAt > sessions[right].LastAt
		}
		return sessions[left].SessionID < sessions[right].SessionID
	})
	setDigest, err := stableSessionSetDigest(sessions)
	if err != nil {
		return nil, err
	}
	rollback = false
	return &StableSessionSnapshot{
		tx:               tx,
		ingestFence:      ingestFence,
		sessions:         sessions,
		requestCount:     requestCount,
		sessionSetSHA256: setDigest,
	}, nil
}

func validStableSessionID(value string) bool {
	if value == "" || len(value) > 512 {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] < 0x20 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

func canonicalTimestamp(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000Z")
}

func stableSessionSetDigest(sessions []StableSessionSummary) (string, error) {
	type digestItem struct {
		FirstAt       string `json:"first_at"`
		LastAt        string `json:"last_at"`
		RecordsSHA256 string `json:"records_sha256"`
		Requests      int    `json:"requests"`
		SessionID     string `json:"session_id"`
	}
	items := make([]digestItem, 0, len(sessions))
	for _, item := range sessions {
		items = append(items, digestItem{
			FirstAt:       item.FirstAt,
			LastAt:        item.LastAt,
			RecordsSHA256: item.RecordsSHA256,
			Requests:      item.Requests,
			SessionID:     item.SessionID,
		})
	}
	sort.Slice(items, func(left, right int) bool { return items[left].SessionID < items[right].SessionID })
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(items); err != nil {
		return "", err
	}
	sum := sha256.Sum256(bytes.TrimSuffix(buffer.Bytes(), []byte("\n")))
	return hex.EncodeToString(sum[:]), nil
}

func (snapshot *StableSessionSnapshot) IngestFence() int64 {
	return snapshot.ingestFence
}

func (snapshot *StableSessionSnapshot) SessionCount() int {
	return len(snapshot.sessions)
}

func (snapshot *StableSessionSnapshot) RequestCount() int {
	return snapshot.requestCount
}

func (snapshot *StableSessionSnapshot) SessionSetSHA256() string {
	return snapshot.sessionSetSHA256
}

func (snapshot *StableSessionSnapshot) Page(afterLastAt, afterSessionID string, limit int) ([]StableSessionSummary, *StableSessionSummary, error) {
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed || limit < 1 {
		return nil, nil, ErrSnapshotCursor
	}
	start := 0
	if afterLastAt != "" || afterSessionID != "" {
		found := false
		for index := range snapshot.sessions {
			if snapshot.sessions[index].LastAt == afterLastAt && snapshot.sessions[index].SessionID == afterSessionID {
				start = index + 1
				found = true
				break
			}
		}
		if !found {
			return nil, nil, ErrSnapshotCursor
		}
	}
	end := start + limit
	if end > len(snapshot.sessions) {
		end = len(snapshot.sessions)
	}
	page := append([]StableSessionSummary(nil), snapshot.sessions[start:end]...)
	if end >= len(snapshot.sessions) {
		return page, nil, nil
	}
	next := snapshot.sessions[end-1]
	return page, &next, nil
}

func (snapshot *StableSessionSnapshot) Summary(sessionID string) (StableSessionSummary, bool) {
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed {
		return StableSessionSummary{}, false
	}
	for _, item := range snapshot.sessions {
		if item.SessionID == sessionID {
			return item, true
		}
	}
	return StableSessionSummary{}, false
}

func (snapshot *StableSessionSnapshot) ExportSessionJSONL(ctx context.Context, sessionID, expectedDigest string, destination io.Writer) error {
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed {
		return ErrSnapshotCursor
	}
	var expected StableSessionSummary
	found := false
	for _, item := range snapshot.sessions {
		if item.SessionID == sessionID {
			expected = item
			found = true
			break
		}
	}
	if !found || expected.RecordsSHA256 != expectedDigest {
		return ErrSnapshotCursor
	}
	digest := sha256.New()
	if err := exportStableSessionJSONL(ctx, snapshot.tx, sessionID, io.MultiWriter(destination, digest)); err != nil {
		return err
	}
	if hex.EncodeToString(digest.Sum(nil)) != expectedDigest {
		return fmt.Errorf("stable archive projection digest mismatch")
	}
	return nil
}

func (snapshot *StableSessionSnapshot) Close() error {
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed {
		return nil
	}
	snapshot.closed = true
	return snapshot.tx.Rollback()
}

func (s *Store) RefreshSessionExportDigests(ctx context.Context, limit int) (int, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 64 {
		limit = 64
	}
	rows, err := s.DB.QueryContext(ctx, `SELECT summaries.session_id
		FROM session_summaries summaries
		LEFT JOIN session_export_digests digests ON digests.session_id=summaries.session_id
		WHERE digests.session_id IS NULL OR digests.max_ingest_sequence<>COALESCE(
			(SELECT MAX(events.sequence) FROM archive_ingest_events events WHERE events.session_id=summaries.session_id),0
		)
		ORDER BY summaries.last_at DESC,summaries.session_id COLLATE BINARY ASC
		LIMIT ?`, limit)
	if err != nil {
		return 0, err
	}
	var sessionIDs []string
	for rows.Next() {
		var sessionID string
		if err = rows.Scan(&sessionID); err != nil {
			rows.Close()
			return 0, err
		}
		sessionIDs = append(sessionIDs, sessionID)
	}
	if err = rows.Close(); err != nil {
		return 0, err
	}
	updated := 0
	for _, sessionID := range sessionIDs {
		if err = ctx.Err(); err != nil {
			return updated, err
		}
		if err = s.refreshSessionExportDigest(ctx, sessionID); err != nil {
			return updated, err
		}
		updated++
	}
	return updated, nil
}

func (s *Store) refreshSessionExportDigest(ctx context.Context, sessionID string) error {
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	var requests int
	var firstAt, lastAt string
	var sessionFence int64
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*),MIN(started_at),MAX(completed_at),
		COALESCE((SELECT MAX(sequence) FROM archive_ingest_events WHERE session_id=?),0)
		FROM records WHERE session_id=?`, sessionID, sessionID).Scan(&requests, &firstAt, &lastAt, &sessionFence)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	digest := sha256.New()
	if err = exportStableSessionJSONL(ctx, tx, sessionID, digest); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err = tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		return err
	}
	recordsSHA256 := hex.EncodeToString(digest.Sum(nil))
	_, err = s.DB.ExecContext(ctx, `INSERT INTO session_export_digests(session_id,requests,first_at,last_at,records_sha256,max_ingest_sequence,updated_at)
		SELECT ?,?,?,?,?,?,? WHERE COALESCE((SELECT MAX(sequence) FROM archive_ingest_events WHERE session_id=?),0)=?
		ON CONFLICT(session_id) DO UPDATE SET
			requests=excluded.requests,first_at=excluded.first_at,last_at=excluded.last_at,
			records_sha256=excluded.records_sha256,max_ingest_sequence=excluded.max_ingest_sequence,updated_at=excluded.updated_at`,
		sessionID, requests, firstAt, lastAt, recordsSHA256, sessionFence, canonicalTimestamp(time.Now()), sessionID, sessionFence)
	return err
}

func exportStableSessionJSONL(ctx context.Context, q queryContext, sessionID string, destination io.Writer) error {
	rows, err := q.QueryContext(ctx, `SELECT request_id FROM records WHERE session_id=? ORDER BY request_id COLLATE BINARY ASC`, sessionID)
	if err != nil {
		return err
	}
	var requestIDs []string
	for rows.Next() {
		var requestID string
		if err = rows.Scan(&requestID); err != nil {
			rows.Close()
			return err
		}
		requestIDs = append(requestIDs, requestID)
	}
	if err = rows.Close(); err != nil {
		return err
	}
	encoder := json.NewEncoder(destination)
	encoder.SetEscapeHTML(false)
	for _, requestID := range requestIDs {
		if err = ctx.Err(); err != nil {
			return err
		}
		record, loadErr := (&Store{}).requestWithQuery(ctx, q, requestID)
		if loadErr != nil {
			return loadErr
		}
		if err = encoder.Encode(stableTrainingRecord(record)); err != nil {
			return err
		}
	}
	return nil
}

func stableTrainingRecord(record Record) map[string]any {
	item := map[string]any{
		"schema_version": 2,
		"session_id":     record.SessionID,
		"request_id":     record.RequestID,
		"started_at":     canonicalTimestamp(record.StartedAt),
		"completed_at":   canonicalTimestamp(record.CompletedAt),
	}
	optionalString := map[string]string{
		"key_id":          record.KeyID,
		"principal_id":    record.PrincipalID,
		"credential_hash": record.CredentialHash,
		"requested_model": record.RequestedModel,
		"model":           record.Model,
		"outcome":         record.Outcome,
	}
	for key, value := range optionalString {
		if strings.TrimSpace(value) != "" {
			item[key] = value
		}
	}
	if record.StatusCode != 0 {
		item["status_code"] = record.StatusCode
	}
	if record.Metadata != nil {
		item["metadata"] = record.Metadata
	}
	if record.Facets != nil {
		item["facets"] = record.Facets
	}
	if request := decodedPayload(record.OriginalRequest); request != nil {
		item["request"] = request
	}
	if response := decodedPayload(record.Response); response != nil {
		item["response"] = response
	}
	return item
}
