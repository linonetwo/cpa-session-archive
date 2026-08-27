package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestStableSessionSetDigestPreservesV1PresentSummaryContract(t *testing.T) {
	item := StableSessionSummary{
		SessionID:     "session",
		Requests:      2,
		FirstAt:       "2026-08-21T01:02:03.000000Z",
		LastAt:        "2026-08-21T01:02:04.000000Z",
		RecordsSHA256: strings.Repeat("a", 64),
	}
	got, err := stableSessionSetDigest([]StableSessionSummary{item})
	if err != nil {
		t.Fatal(err)
	}
	legacyJSON, err := json.Marshal([]struct {
		FirstAt       string `json:"first_at"`
		LastAt        string `json:"last_at"`
		RecordsSHA256 string `json:"records_sha256"`
		Requests      int    `json:"requests"`
		SessionID     string `json:"session_id"`
	}{{item.FirstAt, item.LastAt, item.RecordsSHA256, item.Requests, item.SessionID}})
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(legacyJSON)
	if got != hex.EncodeToString(want[:]) {
		t.Fatalf("present-only v1 set digest changed: got=%s want=%s", got, hex.EncodeToString(want[:]))
	}
	tombstoneA := StableSessionSummary{SessionID: "deleted", LastAt: "2026-08-21T01:02:05.000000Z", Deleted: true, DeletedAt: "2026-08-21T01:02:05.000000Z"}
	tombstoneB := tombstoneA
	tombstoneB.DeletedAt = "2026-08-21T01:02:06.000000Z"
	digestA, err := stableSessionSetDigest([]StableSessionSummary{tombstoneA})
	if err != nil {
		t.Fatal(err)
	}
	digestB, err := stableSessionSetDigest([]StableSessionSummary{tombstoneB})
	if err != nil {
		t.Fatal(err)
	}
	if digestA == digestB {
		t.Fatal("tombstone semantics were omitted from the session set digest")
	}
}

func refreshAllSessionDigests(t *testing.T, store *Store) {
	t.Helper()
	for iteration := 0; iteration < 10000; iteration++ {
		updated, err := store.RefreshSessionExportDigests(context.Background(), 64)
		if err != nil {
			t.Fatal(err)
		}
		if updated == 0 {
			return
		}
	}
	t.Fatal("session digest projection did not converge")
}

func TestStableSnapshotPaginatesTiesAndExcludesLaterWrites(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	when := time.Date(2026, 8, 21, 5, 4, 3, 123456789, time.UTC)
	records := make([]Record, 0, 1001)
	for index := 0; index < 1001; index++ {
		records = append(records, Record{
			RequestID:   fmt.Sprintf("request-%04d", index),
			SessionID:   fmt.Sprintf("session-%04d", index),
			StartedAt:   when,
			CompletedAt: when,
			Outcome:     "succeeded",
		})
	}
	if err = store.PutBatch(records); err != nil {
		t.Fatal(err)
	}
	refreshAllSessionDigests(t, store)

	futureLowerBound := when.Add(365 * 24 * time.Hour)
	snapshot, err := store.BeginStableSessionSnapshot(futureLowerBound, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if snapshot.SessionCount() != 1001 || snapshot.RequestCount() != 1001 {
		t.Fatalf("counts sessions=%d requests=%d", snapshot.SessionCount(), snapshot.RequestCount())
	}
	firstPage, next, err := snapshot.Page("", "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	replayed, replayNext, err := snapshot.Page("", "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(firstPage, replayed) || !reflect.DeepEqual(next, replayNext) {
		t.Fatal("replayed first page changed")
	}
	if len(firstPage) != 1000 || next == nil {
		t.Fatalf("first page len=%d next=%+v", len(firstPage), next)
	}
	if firstPage[0].SessionID != "session-0000" || firstPage[999].SessionID != "session-0999" {
		t.Fatalf("tie ordering first=%s last=%s", firstPage[0].SessionID, firstPage[999].SessionID)
	}
	lastPage, finalCursor, err := snapshot.Page(next.LastAt, next.SessionID, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(lastPage) != 1 || lastPage[0].SessionID != "session-1000" || finalCursor != nil {
		t.Fatalf("last page=%+v cursor=%+v", lastPage, finalCursor)
	}

	oldFence := snapshot.IngestFence()
	if err = store.PutBatch([]Record{{
		RequestID:       "late-request",
		SessionID:       "late-session",
		StartedAt:       when.Add(-time.Hour),
		CompletedAt:     when.Add(-time.Hour),
		OriginalRequest: []byte(`{"input":"late"}`),
		Response:        []byte(`{"output":"included next time"}`),
		Outcome:         "succeeded",
	}}); err != nil {
		t.Fatal(err)
	}
	stillFirst, _, err := snapshot.Page("", "", 2000)
	if err != nil {
		t.Fatal(err)
	}
	if len(stillFirst) != 1001 {
		t.Fatalf("post-fence write entered active snapshot: %d", len(stillFirst))
	}
	refreshAllSessionDigests(t, store)
	nextSnapshot, err := store.BeginStableSessionSnapshot(futureLowerBound, &oldFence)
	if err != nil {
		t.Fatal(err)
	}
	defer nextSnapshot.Close()
	delta, _, err := nextSnapshot.Page("", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(delta) != 1 || delta[0].SessionID != "late-session" {
		t.Fatalf("late old-timestamp record missing from delta: %+v", delta)
	}
}

func TestStableDeltaIncludesDeleteAndOldIdentityTombstones(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	when := time.Date(2026, 8, 21, 5, 4, 3, 0, time.UTC)
	if err = store.PutBatch([]Record{
		{RequestID: "move", SessionID: "session-old", StartedAt: when, CompletedAt: when},
		{RequestID: "delete", SessionID: "session-deleted", StartedAt: when, CompletedAt: when},
		{RequestID: "partial-a", SessionID: "session-partial", StartedAt: when, CompletedAt: when},
		{RequestID: "partial-b", SessionID: "session-partial", StartedAt: when, CompletedAt: when},
	}); err != nil {
		t.Fatal(err)
	}
	refreshAllSessionDigests(t, store)
	initial, err := store.BeginStableSessionSnapshot(time.Unix(0, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	priorFence := initial.IngestFence()
	if err = initial.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err = store.DB.Exec(`UPDATE records SET session_id='session-new' WHERE request_id='move';
		DELETE FROM records WHERE request_id='delete';
		DELETE FROM records WHERE request_id='partial-a'`); err != nil {
		t.Fatal(err)
	}
	if _, err = store.BeginStableSessionSnapshot(when.Add(24*time.Hour), &priorFence); !errors.Is(err, ErrSnapshotProjectionNotReady) {
		t.Fatalf("changed present sessions did not require exact new digests: %v", err)
	}
	if updated, refreshErr := store.RefreshQueuedSessionExportDigests(context.Background(), 64); refreshErr != nil || updated != 2 {
		t.Fatalf("changed digest refresh updated=%d err=%v", updated, refreshErr)
	}

	delta, err := store.BeginStableSessionSnapshot(when.Add(24*time.Hour), &priorFence)
	if err != nil {
		t.Fatal(err)
	}
	defer delta.Close()
	items, next, err := delta.Page("", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if next != nil || len(items) != 4 || delta.SessionCount() != 4 || delta.RequestCount() != 2 || delta.DeletedSessionCount() != 2 {
		t.Fatalf("delta metadata items=%+v next=%+v sessions=%d requests=%d deleted=%d", items, next, delta.SessionCount(), delta.RequestCount(), delta.DeletedSessionCount())
	}
	byID := map[string]StableSessionSummary{}
	for _, item := range items {
		byID[item.SessionID] = item
	}
	for _, sessionID := range []string{"session-old", "session-deleted"} {
		item := byID[sessionID]
		if !item.Deleted || item.Requests != 0 || item.DeletedAt == "" || item.LastAt != item.DeletedAt || item.FirstAt != "" || item.RecordsSHA256 != "" {
			t.Fatalf("invalid tombstone for %s: %+v", sessionID, item)
		}
		if err = delta.ExportSessionJSONL(context.Background(), sessionID, "", &bytes.Buffer{}); !errors.Is(err, ErrSnapshotCursor) {
			t.Fatalf("tombstone %s was exportable: %v", sessionID, err)
		}
	}
	if moved := byID["session-new"]; moved.Deleted || moved.Requests != 1 || len(moved.RecordsSHA256) != 64 {
		t.Fatalf("new session identity is not a complete replacement: %+v", moved)
	}
	if partial := byID["session-partial"]; partial.Deleted || partial.Requests != 1 || len(partial.RecordsSHA256) != 64 {
		t.Fatalf("partially deleted session is not a replacement summary: %+v", partial)
	}
	var previous string
	if err = store.DB.QueryRow(`SELECT previous_session_id FROM archive_ingest_events WHERE request_id='move' ORDER BY sequence DESC LIMIT 1`).Scan(&previous); err != nil {
		t.Fatal(err)
	}
	if previous != "session-old" {
		t.Fatalf("session move lost old identity: %q", previous)
	}
	if updated, refreshErr := store.RefreshSessionExportDigests(context.Background(), 64); refreshErr != nil || updated != 0 {
		t.Fatalf("deleted stale summaries kept the offline digest sweep busy: updated=%d err=%v", updated, refreshErr)
	}

	full, err := store.BeginStableSessionSnapshot(time.Unix(0, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()
	if full.DeletedSessionCount() != 0 || full.SessionCount() != 2 {
		t.Fatalf("full snapshot leaked historical tombstones: sessions=%d deleted=%d", full.SessionCount(), full.DeletedSessionCount())
	}
}

func TestStableSnapshotExportIsCanonicalAndSnapshotBound(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	when := time.Date(2026, 8, 21, 5, 4, 3, 987654321, time.UTC)
	if err = store.PutBatch([]Record{{
		RequestID:       "request-b",
		SessionID:       "session",
		KeyID:           "private-key-id",
		StartedAt:       when,
		CompletedAt:     when,
		OriginalRequest: []byte(`{"z":1,"a":"request"}`),
		Response:        []byte(`{"z":2,"a":"response"}`),
		Outcome:         "succeeded",
	}}); err != nil {
		t.Fatal(err)
	}
	refreshAllSessionDigests(t, store)
	snapshot, err := store.BeginStableSessionSnapshot(time.Unix(0, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	summary, found := snapshot.Summary("session")
	if !found {
		t.Fatal("session missing")
	}
	if err = store.PutBatch([]Record{{
		RequestID:   "request-a",
		SessionID:   "session",
		StartedAt:   when.Add(time.Second),
		CompletedAt: when.Add(time.Second),
		Outcome:     "succeeded",
	}}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err = snapshot.ExportSessionJSONL(context.Background(), "session", summary.RecordsSHA256, &output); err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(output.String(), "\n"); lines != 1 {
		t.Fatalf("snapshot export included later write: lines=%d", lines)
	}
	sum := sha256.Sum256(output.Bytes())
	if hex.EncodeToString(sum[:]) != summary.RecordsSHA256 {
		t.Fatalf("digest mismatch output=%s summary=%s", hex.EncodeToString(sum[:]), summary.RecordsSHA256)
	}
	line := output.String()
	if !strings.Contains(line, `"completed_at":"2026-08-21T05:04:03.987654Z"`) {
		t.Fatalf("timestamp is not canonical: %s", line)
	}
	if strings.Index(line, `"completed_at"`) > strings.Index(line, `"request_id"`) {
		t.Fatalf("record keys are not lexicographically ordered: %s", line)
	}
	if err = snapshot.ExportSessionJSONL(context.Background(), "session", strings.Repeat("0", 64), &bytes.Buffer{}); !errors.Is(err, ErrSnapshotCursor) {
		t.Fatalf("altered digest was accepted: %v", err)
	}
}

func TestOpeningLegacyArchiveDoesNotBackfillEventRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.sqlite")
	store, err := OpenStore(path, false)
	if err != nil {
		t.Fatal(err)
	}
	when := time.Now().UTC()
	if err = store.PutBatch([]Record{{RequestID: "legacy", SessionID: "legacy-session", StartedAt: when, CompletedAt: when}}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec(`DROP TRIGGER archive_records_insert;
		DROP TRIGGER archive_records_update;
		DROP TRIGGER archive_records_delete;
		DROP TABLE archive_ingest_events;
		DROP TABLE archive_ingest_clock;
		DROP TABLE session_export_digests`); err != nil {
		t.Fatal(err)
	}
	if err = store.DB.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	var events, sequence int64
	if err = store.DB.QueryRow(`SELECT COUNT(*) FROM archive_ingest_events`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err = store.DB.QueryRow(`SELECT sequence FROM archive_ingest_clock WHERE id=1`).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if events != 0 || sequence != 1 {
		t.Fatalf("startup performed an event backfill: events=%d clock=%d", events, sequence)
	}
}

func TestOpeningLegacyEventSchemaCapturesOldSessionIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.sqlite")
	store, err := OpenStore(path, false)
	if err != nil {
		t.Fatal(err)
	}
	when := time.Now().UTC()
	if err = store.PutBatch([]Record{{RequestID: "legacy", SessionID: "legacy-old", StartedAt: when, CompletedAt: when}}); err != nil {
		t.Fatal(err)
	}
	var legacyFence int64
	if err = store.DB.QueryRow(`SELECT sequence FROM archive_ingest_clock WHERE id=1`).Scan(&legacyFence); err != nil {
		t.Fatal(err)
	}
	if _, err = store.DB.Exec(`DROP TRIGGER archive_records_insert;
		DROP TRIGGER archive_records_update;
		DROP TRIGGER archive_records_delete;
		DROP INDEX idx_archive_ingest_events_previous_session_sequence;
		DROP TABLE archive_snapshot_contract;
		ALTER TABLE archive_ingest_events DROP COLUMN previous_session_id`); err != nil {
		t.Fatal(err)
	}
	if err = store.DB.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	unsafeFence := legacyFence - 1
	if snapshot, snapshotErr := store.BeginStableSessionSnapshot(time.Unix(0, 0), &unsafeFence); !errors.Is(snapshotErr, ErrSnapshotTombstoneHistory) {
		if snapshot != nil {
			_ = snapshot.Close()
		}
		t.Fatalf("pre-upgrade delta fence did not fail closed: %v", snapshotErr)
	}
	if _, err = store.DB.Exec(`UPDATE records SET session_id='legacy-new' WHERE request_id='legacy'`); err != nil {
		t.Fatal(err)
	}
	var current, previous string
	if err = store.DB.QueryRow(`SELECT session_id,previous_session_id FROM archive_ingest_events WHERE request_id='legacy' ORDER BY sequence DESC LIMIT 1`).Scan(&current, &previous); err != nil {
		t.Fatal(err)
	}
	if current != "legacy-new" || previous != "legacy-old" {
		t.Fatalf("upgraded trigger current=%q previous=%q", current, previous)
	}
}

func TestClosingStableSnapshotReleasesWALCheckpoint(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	when := time.Date(2026, 8, 21, 1, 2, 3, 0, time.UTC)
	if err = store.PutBatch([]Record{{
		RequestID: "before", SessionID: "session-before", StartedAt: when, CompletedAt: when,
	}}); err != nil {
		t.Fatal(err)
	}
	refreshAllSessionDigests(t, store)
	snapshot, err := store.BeginStableSessionSnapshot(time.Unix(0, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.PutBatch([]Record{{
		RequestID: "after", SessionID: "session-after", StartedAt: when, CompletedAt: when,
	}}); err != nil {
		t.Fatal(err)
	}
	if err = snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = snapshot.Page("", "", 1); !errors.Is(err, ErrSnapshotCursor) {
		t.Fatalf("closed snapshot remained readable: %v", err)
	}
	var busy, logFrames, checkpointed int
	if err = store.DB.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &logFrames, &checkpointed); err != nil {
		t.Fatal(err)
	}
	if busy != 0 || logFrames != 0 {
		t.Fatalf("closed snapshot retained WAL reader: busy=%d log=%d checkpointed=%d", busy, logFrames, checkpointed)
	}
}

func TestStableSnapshotQueuesOnlyRequestedMissingDigests(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()
	when := time.Date(2026, 8, 21, 1, 2, 3, 0, time.UTC)
	if err = store.PutBatch([]Record{{
		RequestID: "request", SessionID: "session", StartedAt: when, CompletedAt: when,
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err = store.BeginStableSessionSnapshot(time.Unix(0, 0), nil); !errors.Is(err, ErrSnapshotProjectionNotReady) {
		t.Fatalf("missing digest did not return a retryable projection state: %v", err)
	}
	pending, capacity := store.PendingSessionExportDigests()
	if pending != 1 || capacity != maxQueuedSessionDigests {
		t.Fatalf("digest queue pending=%d capacity=%d", pending, capacity)
	}
	if updated, refreshErr := store.RefreshQueuedSessionExportDigests(context.Background(), 1); refreshErr != nil || updated != 1 {
		t.Fatalf("queued digest refresh updated=%d err=%v", updated, refreshErr)
	}
	pending, _ = store.PendingSessionExportDigests()
	if pending != 0 {
		t.Fatalf("digest queue did not drain: %d", pending)
	}
	snapshot, err := store.BeginStableSessionSnapshot(time.Unix(0, 0), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if snapshot.SessionCount() != 1 {
		t.Fatalf("session count=%d", snapshot.SessionCount())
	}
}
