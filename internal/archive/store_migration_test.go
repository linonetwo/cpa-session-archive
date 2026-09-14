package archive

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestStableSnapshotMetadataIndexMigrationIsNamedAndIdempotent(t *testing.T) {
	db, err := sql.Open("sqlite3", t.TempDir()+"/archive.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE records(
		id INTEGER PRIMARY KEY,
		session_id TEXT NOT NULL,
		started_at TEXT NOT NULL,
		completed_at TEXT NOT NULL
	)`); err != nil {
		t.Fatal(err)
	}

	var logs []string
	logf := func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	}
	if err = ensureStableSnapshotMetadataIndex(db, 0, logf); err != nil {
		t.Fatal(err)
	}
	if len(logs) != 2 || !strings.Contains(logs[0], "migration started: name="+stableSnapshotMetadataIndexName) || !strings.Contains(logs[1], "migration completed: name="+stableSnapshotMetadataIndexName) {
		t.Fatalf("unexpected migration logs: %q", logs)
	}

	var columns string
	rows, err := db.Query(`PRAGMA index_info(` + stableSnapshotMetadataIndexName + `)`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var sequence, columnID int
		var name string
		if err = rows.Scan(&sequence, &columnID, &name); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		columns += name + ","
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if columns != "session_id,started_at,completed_at," {
		t.Fatalf("unexpected index columns: %q", columns)
	}

	logs = nil
	if err = ensureStableSnapshotMetadataIndex(db, 0, logf); err != nil {
		t.Fatal(err)
	}
	if len(logs) != 0 {
		t.Fatalf("existing index should skip migration, got logs %q", logs)
	}
}

func TestArchiveMigrationProgressUsesSuppliedTicks(t *testing.T) {
	started := time.Date(2026, time.September, 14, 21, 0, 0, 0, time.UTC)
	ticks := make(chan time.Time)
	stop := make(chan struct{})
	stopped := make(chan struct{})
	logged := make(chan string, 1)
	go func() {
		reportArchiveMigrationProgress(started, ticks, stop, func(format string, args ...any) {
			logged <- fmt.Sprintf(format, args...)
		})
		close(stopped)
	}()

	ticks <- started.Add(30 * time.Second)
	if got := <-logged; got != "archive schema migration in progress: name="+stableSnapshotMetadataIndexName+" elapsed=30s" {
		t.Fatalf("unexpected progress log: %q", got)
	}
	close(stop)
	<-stopped
}
