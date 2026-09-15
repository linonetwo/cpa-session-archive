package archive

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const stableFullSnapshotReferenceQuery = `WITH changed(session_id) AS (
	SELECT events.session_id
	FROM archive_ingest_events events
	WHERE ?=1 AND events.sequence>? AND events.sequence<=?
	UNION
	SELECT events.previous_session_id
	FROM archive_ingest_events events
	WHERE ?=1 AND events.sequence>? AND events.sequence<=? AND events.previous_session_id<>''
), selected(session_id) AS (
	SELECT DISTINCT records.session_id
	FROM records INDEXED BY idx_records_session_snapshot_metadata
	WHERE ?=0
	   OR julianday(records.completed_at)>=julianday(?)
	   OR EXISTS(SELECT 1 FROM changed WHERE changed.session_id=records.session_id)
	UNION
	SELECT changed.session_id
	FROM changed
	WHERE NOT EXISTS(SELECT 1 FROM records WHERE records.session_id=changed.session_id)
)
SELECT selected.session_id,COUNT(records.id),MIN(records.started_at),MAX(records.completed_at),
	COALESCE((SELECT MAX(events.sequence) FROM archive_ingest_events events
		WHERE (events.session_id=selected.session_id OR events.previous_session_id=selected.session_id) AND events.sequence<=?),0),
	COALESCE(d.records_sha256,''),COALESCE(d.max_ingest_sequence,-1),COUNT(records.id)=0,
	COALESCE((SELECT MAX(events.recorded_at) FROM archive_ingest_events events
		WHERE (events.session_id=selected.session_id OR events.previous_session_id=selected.session_id) AND events.sequence<=?),'')
FROM selected
LEFT JOIN records INDEXED BY idx_records_session_snapshot_metadata ON records.session_id=selected.session_id
LEFT JOIN session_export_digests d ON d.session_id=selected.session_id
GROUP BY selected.session_id
ORDER BY COALESCE(MAX(records.completed_at),(SELECT MAX(events.recorded_at) FROM archive_ingest_events events
	WHERE (events.session_id=selected.session_id OR events.previous_session_id=selected.session_id) AND events.sequence<=?)) DESC,
	selected.session_id COLLATE BINARY ASC`

type stableProjectionRow struct {
	sessionID    string
	requests     int
	firstAt      sql.NullString
	lastAt       sql.NullString
	sessionFence int64
	digest       string
	digestFence  int64
	deleted      bool
	changedAt    sql.NullString
}

func readStableProjectionRows(t *testing.T, db *sql.DB, query string, args ...any) []stableProjectionRow {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := make([]stableProjectionRow, 0)
	for rows.Next() {
		var row stableProjectionRow
		if err = rows.Scan(&row.sessionID, &row.requests, &row.firstAt, &row.lastAt, &row.sessionFence,
			&row.digest, &row.digestFence, &row.deleted, &row.changedAt); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestStableFullSnapshotQueryMatchesReferenceProjection(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	if _, err = store.DB.Exec(`
		DROP TRIGGER archive_records_insert;
		DROP TRIGGER archive_records_delete;
		DROP TRIGGER archive_records_update;
		INSERT INTO records(id,request_id,session_id,started_at,completed_at) VALUES
			(1,'request-z','session-Z','2026-09-14T01:00:00Z','2026-09-14T05:00:00Z'),
			(2,'request-unicode','session-Ω','2026-09-14T02:00:00Z','2026-09-14T05:00:00Z'),
			(3,'request-prev-a','session-prev','2026-09-14T03:00:00Z',NULL),
			(4,'request-prev-b','session-prev','2026-09-14T04:00:00Z',NULL),
			(5,'request-empty','session-Z-empty','2026-09-14T07:00:00Z',''),
			(6,'request-null','session-A-null','2026-09-14T08:00:00Z',NULL);
		INSERT INTO archive_ingest_events(sequence,request_id,session_id,previous_session_id,recorded_at) VALUES
			(10,'request-z','session-Z','','2026-09-14T04:00:00Z'),
			(11,'request-unicode','session-Ω','','2026-09-14T04:00:00Z'),
			(12,'request-moved','session-moved','session-prev','2026-09-14T06:00:00Z');
	`); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec(`
		INSERT INTO session_export_digests(session_id,requests,first_at,last_at,records_sha256,max_ingest_sequence,updated_at) VALUES
			('session-Z',1,'2026-09-14T01:00:00Z','2026-09-14T05:00:00Z',?,10,'2026-09-14T07:00:00Z')
	`, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec(`
		INSERT INTO session_export_digests(session_id,requests,first_at,last_at,records_sha256,max_ingest_sequence,updated_at) VALUES
			('session-Ω',1,'2026-09-14T02:00:00Z','2026-09-14T05:00:00Z',?,11,'2026-09-14T07:00:00Z'),
			('session-prev',2,'2026-09-14T03:00:00Z','',?,12,'2026-09-14T07:00:00Z'),
			('session-Z-empty',1,'2026-09-14T07:00:00Z','',?,0,'2026-09-14T09:00:00Z'),
			('session-A-null',1,'2026-09-14T08:00:00Z','',?,0,'2026-09-14T09:00:00Z')
	`, strings.Repeat("b", 64), strings.Repeat("c", 64), strings.Repeat("d", 64), strings.Repeat("e", 64)); err != nil {
		t.Fatal(err)
	}

	reference := readStableProjectionRows(t, store.DB, stableFullSnapshotReferenceQuery,
		0, 0, 12, 0, 0, 12, 0, "1970-01-01T00:00:00Z", 12, 12, 12)
	optimized := readStableProjectionRows(t, store.DB, stableFullSnapshotQuery, 12, 12)
	if !reflect.DeepEqual(optimized, reference) {
		t.Fatalf("optimized full projection differs from reference:\noptimized=%+v\nreference=%+v", optimized, reference)
	}
	if len(optimized) != 5 || optimized[0].sessionID != "session-prev" || optimized[1].sessionID != "session-Z" || optimized[2].sessionID != "session-Ω" || optimized[3].sessionID != "session-Z-empty" || optimized[4].sessionID != "session-A-null" {
		t.Fatalf("unexpected empty-time/event or binary tie ordering: %+v", optimized)
	}
	if optimized[0].sessionFence != 12 || optimized[0].changedAt.String != "2026-09-14T06:00:00Z" {
		t.Fatalf("previous-session event was not included: %+v", optimized[0])
	}
}

func TestStableFullSnapshotPlanAvoidsSelectedUnionMaterialization(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	rows, err := store.DB.Query(`EXPLAIN QUERY PLAN `+stableFullSnapshotQuery, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	details := make([]string, 0)
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(details, "\n")
	if strings.Contains(joined, "MERGE (UNION)") || strings.Contains(joined, "CO-ROUTINE selected") {
		t.Fatalf("full projection retained generic selected materialization:\n%s", joined)
	}
	if !strings.Contains(joined, "COVERING INDEX idx_records_session_snapshot_metadata") {
		t.Fatalf("full projection does not use the metadata covering index:\n%s", joined)
	}
	if got := strings.Count(joined, "USE TEMP B-TREE FOR ORDER BY"); got != 1 {
		t.Fatalf("full projection has %d ORDER BY temp trees, want 1 for the final session ordering:\n%s", got, joined)
	}
}
