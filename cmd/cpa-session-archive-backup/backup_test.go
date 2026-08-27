package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestCreateOnlineBackupIncludesLiveWALAndSafeSummary(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "active-source.sqlite")
	destination := filepath.Join(directory, "published-backup.sqlite")
	db := createTestArchive(t, source, "WAL")
	defer db.Close()
	insertTestRecord(t, db, "request-one", "session-one")
	insertTestRecord(t, db, "request-two", "session-one")
	insertTestRecord(t, db, "request-three", "session-two")
	if info, err := os.Stat(source + "-wal"); err != nil || info.Size() == 0 {
		t.Fatal("test setup did not retain an active WAL")
	}

	result, err := createOnlineBackup(context.Background(), testOptions(source, destination))
	if err != nil {
		t.Fatalf("online backup failed: %v", publicFailure(err))
	}
	if result.SchemaVersion != 1 || result.Records != 3 || result.Sessions != 2 || result.SourceIngestFence != 3 {
		t.Fatalf("unexpected safe summary: %+v", result)
	}
	if len(result.ContentSHA256) != 64 || result.SizeBytes <= 0 {
		t.Fatalf("invalid content evidence: %+v", result)
	}

	contents, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal("published backup is unreadable")
	}
	sum := sha256.Sum256(contents)
	if result.ContentSHA256 != hex.EncodeToString(sum[:]) || result.SizeBytes != int64(len(contents)) {
		t.Fatal("published backup content does not match the reported evidence")
	}
	verify := openTestDatabase(t, destination, "ro")
	defer verify.Close()
	var integrity string
	if err = verify.QueryRow(`PRAGMA integrity_check`).Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatal("published backup did not pass an independent integrity check")
	}
	var records int
	if err = verify.QueryRow(`SELECT COUNT(*) FROM records`).Scan(&records); err != nil || records != 3 {
		t.Fatal("published backup omitted WAL-resident records")
	}

	var output bytes.Buffer
	if err = encodeResult(&output, result); err != nil {
		t.Fatal("could not encode result")
	}
	assertDoesNotContainPaths(t, output.String(), source, destination, directory)
	var keys map[string]json.RawMessage
	if err = json.Unmarshal(output.Bytes(), &keys); err != nil {
		t.Fatal("result is not JSON")
	}
	allowed := map[string]bool{
		"schema_version": true, "records": true, "sessions": true,
		"source_ingest_fence": true, "content_sha256": true, "size_bytes": true,
	}
	if len(keys) != len(allowed) {
		t.Fatalf("unexpected JSON field count: %d", len(keys))
	}
	for key := range keys {
		if !allowed[key] {
			t.Fatalf("unsafe JSON field: %s", key)
		}
	}
}

func TestCreateOnlineBackupRefusesExistingDestinationWithoutChangingIt(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source.sqlite")
	destination := filepath.Join(directory, "do-not-overwrite.sqlite")
	db := createTestArchive(t, source, "WAL")
	insertTestRecord(t, db, "request-one", "session-one")
	if err := db.Close(); err != nil {
		t.Fatal("could not close test source")
	}
	original := []byte("must remain unchanged")
	if err := os.WriteFile(destination, original, 0o600); err != nil {
		t.Fatal("could not create protected destination")
	}

	_, err := createOnlineBackup(context.Background(), testOptions(source, destination))
	if !errors.Is(err, errUnsafeDestination) {
		t.Fatalf("expected a safe destination refusal, got %q", publicFailure(err))
	}
	after, readErr := os.ReadFile(destination)
	if readErr != nil || !bytes.Equal(after, original) {
		t.Fatal("existing destination was changed")
	}
	assertDoesNotContainPaths(t, publicFailure(err), source, destination, directory)
}

func TestRunNeverPrintsPathsOnFailure(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "missing-secret-source.sqlite")
	destination := filepath.Join(directory, "private-destination.sqlite")
	var stdout, stderr bytes.Buffer
	code := run([]string{"--source", source, "--destination", destination}, &stdout, &stderr)
	if code != 1 || stdout.Len() != 0 {
		t.Fatalf("unexpected command result: code=%d stdout-bytes=%d", code, stdout.Len())
	}
	assertDoesNotContainPaths(t, stderr.String(), source, destination, directory)
	if strings.TrimSpace(stderr.String()) != errInvalidSource.Error() {
		t.Fatalf("unexpected public error: %q", strings.TrimSpace(stderr.String()))
	}
}

func TestRunSuccessEmitsOnlySafeJSON(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "source-with-private-name.sqlite")
	destination := filepath.Join(directory, "destination-with-private-name.sqlite")
	db := createTestArchive(t, source, "WAL")
	insertTestRecord(t, db, "request-one", "session-one")
	defer db.Close()

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"--source", source,
		"--destination", destination,
		"--timeout", "2s",
		"--retry-interval", "1ms",
		"--pages-per-step", "1",
	}, &stdout, &stderr)
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("unexpected command result: code=%d stderr-bytes=%d", code, stderr.Len())
	}
	assertDoesNotContainPaths(t, stdout.String(), source, destination, directory)
	var result backupResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal("success output is not JSON")
	}
	if result.SchemaVersion != 1 || result.Records != 1 || result.Sessions != 1 || result.SourceIngestFence != 1 {
		t.Fatalf("unexpected safe command summary: %+v", result)
	}
	if _, err := os.Stat(destination); err != nil {
		t.Fatal("command did not publish the backup")
	}
}

func TestCreateOnlineBackupTimesOutWhileSourceIsBusy(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "busy.sqlite")
	destination := filepath.Join(directory, "should-not-exist.sqlite")
	db := createTestArchive(t, source, "DELETE")
	insertTestRecord(t, db, "request-one", "session-one")
	connection, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal("could not reserve source connection")
	}
	defer connection.Close()
	if _, err = connection.ExecContext(context.Background(), `BEGIN EXCLUSIVE`); err != nil {
		t.Fatal("could not lock source for timeout test")
	}
	defer connection.ExecContext(context.Background(), `ROLLBACK`)

	options := testOptions(source, destination)
	options.Timeout = 40 * time.Millisecond
	options.RetryInterval = 5 * time.Millisecond
	_, err = createOnlineBackup(context.Background(), options)
	if !errors.Is(err, errBackupTimeout) {
		t.Fatalf("expected bounded busy timeout, got %q", publicFailure(err))
	}
	if _, statErr := os.Stat(destination); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatal("timed-out backup published a destination")
	}
	assertDoesNotContainPaths(t, publicFailure(err), source, destination, directory)
}

func TestCreateOnlineBackupRetriesBusySourceThenSucceeds(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "temporarily-busy.sqlite")
	destination := filepath.Join(directory, "eventual-backup.sqlite")
	db := createTestArchive(t, source, "DELETE")
	defer db.Close()
	insertTestRecord(t, db, "request-one", "session-one")
	connection, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal("could not reserve source connection")
	}
	defer connection.Close()
	if _, err = connection.ExecContext(context.Background(), `BEGIN EXCLUSIVE`); err != nil {
		t.Fatal("could not temporarily lock source")
	}
	released := make(chan error, 1)
	go func() {
		time.Sleep(30 * time.Millisecond)
		_, releaseErr := connection.ExecContext(context.Background(), `ROLLBACK`)
		released <- releaseErr
	}()

	options := testOptions(source, destination)
	options.Timeout = time.Second
	options.RetryInterval = 5 * time.Millisecond
	result, err := createOnlineBackup(context.Background(), options)
	if releaseErr := <-released; releaseErr != nil {
		t.Fatal("could not release temporary source lock")
	}
	if err != nil {
		t.Fatalf("backup did not recover from temporary contention: %q", publicFailure(err))
	}
	if result.Records != 1 || result.Sessions != 1 || result.SourceIngestFence != 1 {
		t.Fatalf("unexpected backup after retry: %+v", result)
	}
}

func TestConcurrentBackupsCannotReplacePublishedDestination(t *testing.T) {
	directory := t.TempDir()
	source := filepath.Join(directory, "shared-source.sqlite")
	destination := filepath.Join(directory, "single-winner.sqlite")
	db := createTestArchive(t, source, "WAL")
	defer db.Close()
	insertTestRecord(t, db, "request-one", "session-one")

	start := make(chan struct{})
	results := make(chan error, 2)
	var workers sync.WaitGroup
	for index := 0; index < 2; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, backupErr := createOnlineBackup(context.Background(), testOptions(source, destination))
			results <- backupErr
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	successes := 0
	refusals := 0
	for resultErr := range results {
		switch {
		case resultErr == nil:
			successes++
		case errors.Is(resultErr, errUnsafeDestination):
			refusals++
		default:
			t.Fatalf("unexpected concurrent backup result: %q", publicFailure(resultErr))
		}
	}
	if successes != 1 || refusals != 1 {
		t.Fatalf("expected one no-replace winner and one refusal; successes=%d refusals=%d", successes, refusals)
	}
	verify := openTestDatabase(t, destination, "ro")
	defer verify.Close()
	var records int
	if err := verify.QueryRow(`SELECT COUNT(*) FROM records`).Scan(&records); err != nil || records != 1 {
		t.Fatal("concurrent publication did not preserve a valid winner")
	}
}

func TestInspectBackupRejectsCorruptDatabase(t *testing.T) {
	directory := t.TempDir()
	corrupt := filepath.Join(directory, "corrupt.sqlite")
	if err := os.WriteFile(corrupt, bytes.Repeat([]byte{0xff}, 8192), 0o600); err != nil {
		t.Fatal("could not create corrupt test input")
	}
	_, err := inspectBackup(context.Background(), corrupt)
	if !errors.Is(err, errIntegrityCheck) {
		t.Fatalf("corrupt database was not rejected by integrity gate: %q", publicFailure(err))
	}
	assertDoesNotContainPaths(t, publicFailure(err), corrupt, directory)
}

func TestCreateOnlineBackupRejectsUnboundedStepSize(t *testing.T) {
	options := testOptions("source", "destination")
	options.PagesPerStep = maxPagesPerStep + 1
	if err := validateOptions(options); !errors.Is(err, errInvalidArguments) {
		t.Fatal("unbounded SQLite backup step size was accepted")
	}
}

func testOptions(source, destination string) backupOptions {
	return backupOptions{
		Source:        source,
		Destination:   destination,
		Timeout:       2 * time.Second,
		RetryInterval: time.Millisecond,
		PagesPerStep:  1,
	}
}

func createTestArchive(t *testing.T, path, journalMode string) *sql.DB {
	t.Helper()
	db := openTestDatabase(t, path, "rwc")
	if _, err := db.Exec(`PRAGMA journal_mode=` + journalMode); err != nil {
		t.Fatal("could not set test journal mode")
	}
	if _, err := db.Exec(`PRAGMA wal_autocheckpoint=0`); err != nil {
		t.Fatal("could not disable automatic WAL checkpoint")
	}
	if _, err := db.Exec(`CREATE TABLE records(
		id INTEGER PRIMARY KEY,
		request_id TEXT NOT NULL UNIQUE,
		session_id TEXT NOT NULL
	);
	CREATE TABLE archive_ingest_clock(id INTEGER PRIMARY KEY, sequence INTEGER NOT NULL);
	INSERT INTO archive_ingest_clock(id, sequence) VALUES(1, 0);
	CREATE TRIGGER archive_records_insert AFTER INSERT ON records BEGIN
		UPDATE archive_ingest_clock SET sequence=sequence+1 WHERE id=1;
	END;`); err != nil {
		t.Fatal("could not create test archive")
	}
	return db
}

func openTestDatabase(t *testing.T, path, mode string) *sql.DB {
	t.Helper()
	dsn, err := sqliteFileDSN(path, mode)
	if err != nil {
		t.Fatal("could not construct test database DSN")
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatal("could not open test database")
	}
	db.SetMaxOpenConns(1)
	if err = db.Ping(); err != nil {
		db.Close()
		t.Fatal("could not connect to test database")
	}
	return db
}

func insertTestRecord(t *testing.T, db *sql.DB, requestID, sessionID string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO records(request_id, session_id) VALUES(?, ?)`, requestID, sessionID); err != nil {
		t.Fatal("could not insert test record")
	}
}

func assertDoesNotContainPaths(t *testing.T, output string, paths ...string) {
	t.Helper()
	for _, path := range paths {
		if path != "" && strings.Contains(output, path) {
			t.Fatal("command output leaked a filesystem path")
		}
	}
}
