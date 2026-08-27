package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/mattn/go-sqlite3"
)

const (
	backupSchemaVersion = 1
	maxPagesPerStep     = 4096
)

var (
	errInvalidArguments   = errors.New("invalid arguments")
	errInvalidSource      = errors.New("source database is unavailable or invalid")
	errUnsafeDestination  = errors.New("destination already exists or is unsafe")
	errBackupFailed       = errors.New("online backup failed")
	errBackupTimeout      = errors.New("online backup timed out")
	errIntegrityCheck     = errors.New("backup integrity check failed")
	errBackupVerification = errors.New("backup verification failed")
)

type backupOptions struct {
	Source        string
	Destination   string
	Timeout       time.Duration
	RetryInterval time.Duration
	PagesPerStep  int
}

type backupResult struct {
	SchemaVersion     int    `json:"schema_version"`
	Records           int64  `json:"records"`
	Sessions          int64  `json:"sessions"`
	SourceIngestFence int64  `json:"source_ingest_fence"`
	ContentSHA256     string `json:"content_sha256"`
	SizeBytes         int64  `json:"size_bytes"`
}

func createOnlineBackup(ctx context.Context, options backupOptions) (result backupResult, err error) {
	if err = validateOptions(options); err != nil {
		return backupResult{}, err
	}

	sourceInfo, err := os.Stat(options.Source)
	if err != nil || !sourceInfo.Mode().IsRegular() {
		return backupResult{}, errInvalidSource
	}
	if _, statErr := os.Lstat(options.Destination); statErr == nil {
		return backupResult{}, errUnsafeDestination
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return backupResult{}, errUnsafeDestination
	}
	if sameResolvedPath(options.Source, options.Destination) {
		return backupResult{}, errUnsafeDestination
	}

	destinationDir := filepath.Dir(options.Destination)
	temporary, err := os.CreateTemp(destinationDir, ".cpa-session-archive-backup-*.sqlite")
	if err != nil {
		return backupResult{}, errUnsafeDestination
	}
	temporaryPath := temporary.Name()
	cleanupTemporary := true
	defer func() {
		_ = temporary.Close()
		if cleanupTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err = temporary.Chmod(0o600); err != nil {
		return backupResult{}, errUnsafeDestination
	}
	if err = temporary.Close(); err != nil {
		return backupResult{}, errUnsafeDestination
	}

	backupContext, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()
	if err = copySQLiteOnline(backupContext, options.Source, temporaryPath, options.PagesPerStep, options.RetryInterval); err != nil {
		if backupContext.Err() != nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return backupResult{}, errBackupTimeout
		}
		return backupResult{}, errBackupFailed
	}

	result, err = inspectBackup(backupContext, temporaryPath)
	if err != nil {
		if errors.Is(err, errIntegrityCheck) {
			return backupResult{}, err
		}
		return backupResult{}, errBackupVerification
	}
	if err = syncFile(temporaryPath); err != nil {
		return backupResult{}, errBackupVerification
	}
	result.ContentSHA256, result.SizeBytes, err = hashFile(temporaryPath)
	if err != nil {
		return backupResult{}, errBackupVerification
	}

	// Linking is the publication primitive: unlike rename, it atomically fails
	// when another process creates the destination while this backup is running.
	if err = os.Link(temporaryPath, options.Destination); err != nil {
		return backupResult{}, errUnsafeDestination
	}
	// After publication, never remove the destination by path: another process
	// could unlink and replace that name between a check and cleanup. A rare
	// durability/cleanup error is therefore reported while leaving the safely
	// published no-replace target in place for operator inspection.
	if err = syncDirectory(destinationDir); err != nil {
		return backupResult{}, errBackupVerification
	}
	if err = os.Remove(temporaryPath); err != nil {
		return backupResult{}, errBackupVerification
	}
	cleanupTemporary = false
	if err = syncDirectory(destinationDir); err != nil {
		return backupResult{}, errBackupVerification
	}
	return result, nil
}

func validateOptions(options backupOptions) error {
	if options.Source == "" || options.Destination == "" || options.Timeout <= 0 || options.RetryInterval <= 0 || options.PagesPerStep <= 0 || options.PagesPerStep > maxPagesPerStep {
		return errInvalidArguments
	}
	return nil
}

func sameResolvedPath(source, destination string) bool {
	sourceAbsolute, sourceErr := filepath.Abs(source)
	destinationAbsolute, destinationErr := filepath.Abs(destination)
	if sourceErr != nil || destinationErr != nil {
		return true
	}
	return filepath.Clean(sourceAbsolute) == filepath.Clean(destinationAbsolute)
}

func sqliteFileDSN(path, mode string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	uri := &url.URL{Scheme: "file", Path: filepath.ToSlash(absolute)}
	query := uri.Query()
	query.Set("mode", mode)
	// The backup loop owns retry timing so a driver-level busy wait cannot run
	// past the caller's context deadline.
	query.Set("_busy_timeout", "0")
	uri.RawQuery = query.Encode()
	return uri.String(), nil
}

func copySQLiteOnline(ctx context.Context, sourcePath, destinationPath string, pagesPerStep int, retryInterval time.Duration) error {
	sourceDSN, err := sqliteFileDSN(sourcePath, "ro")
	if err != nil {
		return err
	}
	destinationDSN, err := sqliteFileDSN(destinationPath, "rw")
	if err != nil {
		return err
	}
	sourceDB, err := sql.Open("sqlite3", sourceDSN)
	if err != nil {
		return err
	}
	defer sourceDB.Close()
	sourceDB.SetMaxOpenConns(1)
	destinationDB, err := sql.Open("sqlite3", destinationDSN)
	if err != nil {
		return err
	}
	defer destinationDB.Close()
	destinationDB.SetMaxOpenConns(1)

	sourceConnection, err := acquireSQLiteConnection(ctx, sourceDB, retryInterval)
	if err != nil {
		return err
	}
	defer sourceConnection.Close()
	destinationConnection, err := acquireSQLiteConnection(ctx, destinationDB, retryInterval)
	if err != nil {
		return err
	}
	defer destinationConnection.Close()

	return sourceConnection.Raw(func(sourceDriver any) error {
		sourceSQLite, ok := sourceDriver.(*sqlite3.SQLiteConn)
		if !ok {
			return errBackupFailed
		}
		return destinationConnection.Raw(func(destinationDriver any) error {
			destinationSQLite, ok := destinationDriver.(*sqlite3.SQLiteConn)
			if !ok {
				return errBackupFailed
			}
			var backup *sqlite3.SQLiteBackup
			for {
				var backupErr error
				backup, backupErr = destinationSQLite.Backup("main", sourceSQLite, "main")
				if backupErr == nil {
					break
				}
				if !isSQLiteBusy(backupErr) {
					return backupErr
				}
				if waitErr := waitForRetry(ctx, retryInterval); waitErr != nil {
					return waitErr
				}
			}
			return stepSQLiteBackup(ctx, backup, pagesPerStep, retryInterval)
		})
	})
}

func acquireSQLiteConnection(ctx context.Context, db *sql.DB, retryInterval time.Duration) (*sql.Conn, error) {
	for {
		connection, err := db.Conn(ctx)
		if err == nil {
			return connection, nil
		}
		if !isSQLiteBusy(err) {
			return nil, err
		}
		if waitErr := waitForRetry(ctx, retryInterval); waitErr != nil {
			return nil, waitErr
		}
	}
}

func isSQLiteBusy(err error) bool {
	var sqliteErr sqlite3.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	return sqliteErr.Code == sqlite3.ErrBusy || sqliteErr.Code == sqlite3.ErrLocked
}

func waitForRetry(ctx context.Context, retryInterval time.Duration) error {
	timer := time.NewTimer(retryInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func stepSQLiteBackup(ctx context.Context, backup *sqlite3.SQLiteBackup, pagesPerStep int, retryInterval time.Duration) (err error) {
	defer func() {
		finishErr := backup.Finish()
		if err == nil {
			err = finishErr
		}
	}()
	for {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		remainingBefore := backup.Remaining()
		done, stepErr := backup.Step(pagesPerStep)
		if stepErr != nil {
			return stepErr
		}
		if done {
			return nil
		}
		remainingAfter := backup.Remaining()
		pageCountAfter := backup.PageCount()
		madeProgress := remainingAfter < remainingBefore || (remainingBefore == 0 && remainingAfter < pageCountAfter)
		if madeProgress {
			continue
		}
		if waitErr := waitForRetry(ctx, retryInterval); waitErr != nil {
			return waitErr
		}
	}
}

func inspectBackup(ctx context.Context, path string) (backupResult, error) {
	dsn, err := sqliteFileDSN(path, "ro")
	if err != nil {
		return backupResult{}, err
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return backupResult{}, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	rows, err := db.QueryContext(ctx, `PRAGMA integrity_check`)
	if err != nil {
		return backupResult{}, errIntegrityCheck
	}
	checks := 0
	for rows.Next() {
		var check string
		if err = rows.Scan(&check); err != nil {
			_ = rows.Close()
			return backupResult{}, errIntegrityCheck
		}
		checks++
		if check != "ok" {
			_ = rows.Close()
			return backupResult{}, errIntegrityCheck
		}
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return backupResult{}, errIntegrityCheck
	}
	if err = rows.Close(); err != nil || checks != 1 {
		return backupResult{}, errIntegrityCheck
	}

	result := backupResult{SchemaVersion: backupSchemaVersion}
	if err = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM records`).Scan(&result.Records); err != nil {
		return backupResult{}, err
	}
	if err = db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT session_id) FROM records`).Scan(&result.Sessions); err != nil {
		return backupResult{}, err
	}
	if err = db.QueryRowContext(ctx, `SELECT sequence FROM archive_ingest_clock WHERE id=1`).Scan(&result.SourceIngestFence); err != nil {
		return backupResult{}, err
	}
	return result, nil
}

func hashFile(path string) (string, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(hash.Sum(nil)), size, nil
}

func syncFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func encodeResult(writer io.Writer, result backupResult) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(result)
}

func publicFailure(err error) string {
	switch {
	case errors.Is(err, errInvalidArguments):
		return errInvalidArguments.Error()
	case errors.Is(err, errInvalidSource):
		return errInvalidSource.Error()
	case errors.Is(err, errUnsafeDestination):
		return errUnsafeDestination.Error()
	case errors.Is(err, errBackupTimeout):
		return errBackupTimeout.Error()
	case errors.Is(err, errIntegrityCheck):
		return errIntegrityCheck.Error()
	case errors.Is(err, errBackupVerification):
		return errBackupVerification.Error()
	default:
		return fmt.Sprint(errBackupFailed)
	}
}
