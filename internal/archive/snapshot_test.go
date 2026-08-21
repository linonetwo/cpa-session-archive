package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

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
		RequestID:        "late-request",
		SessionID:        "late-session",
		StartedAt:        when.Add(-time.Hour),
		CompletedAt:      when.Add(-time.Hour),
		OriginalRequest:  []byte(`{"input":"late"}`),
		Response:         []byte(`{"output":"included next time"}`),
		Outcome:          "succeeded",
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
