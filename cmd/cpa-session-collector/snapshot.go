package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"cpa-session-archive/internal/archive"
)

var (
	errStableSnapshotExpired = errors.New("stable session snapshot expired")
	errStableSnapshotLimit   = errors.New("stable session snapshot capacity reached")
)

const (
	stableSnapshotTTL        = 15 * time.Minute
	stableOfflineSnapshotTTL = 6 * time.Hour
	stableSnapshotIdleTTL    = 2 * time.Minute
	stableMaxActiveSnapshots = 1
	// Stable exports are materialized before their final response is committed.
	// Send an informational response well inside the migration client's socket
	// idle budget while that verification is in progress.
	stableExportHeartbeatInterval = 30 * time.Second
)

type stableCursorClaims struct {
	SnapshotID string `json:"snapshot"`
	LowerBound string `json:"lower_bound"`
	AfterFence string `json:"after_fence"`
	Limit      int    `json:"limit"`
	LastAt     string `json:"last_at"`
	SessionID  string `json:"session_id"`
}

type stableSnapshotEntry struct {
	ID          string
	LowerBound  string
	AfterFence  string
	Limit       int
	CreatedAt   time.Time
	LastUsedAt  time.Time
	ExpiresAt   time.Time
	Snapshot    *archive.StableSessionSnapshot
	CursorState map[string]stableCursorClaims
}

type stableSnapshotRegistry struct {
	mu        sync.Mutex
	secret    []byte
	ttl       time.Duration
	idleTTL   time.Duration
	maxActive int
	now       func() time.Time
	snapshots map[string]*stableSnapshotEntry
}

func newStableSnapshotRegistry(ttl, idleTTL time.Duration, maxActive int) (*stableSnapshotRegistry, error) {
	if ttl < time.Minute || ttl > 6*time.Hour {
		return nil, fmt.Errorf("snapshot ttl is outside the supported range")
	}
	if idleTTL < time.Minute || idleTTL > ttl {
		return nil, fmt.Errorf("snapshot idle ttl is outside the supported range")
	}
	if maxActive < 1 || maxActive > 8 {
		return nil, fmt.Errorf("snapshot capacity is outside the supported range")
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return nil, err
	}
	return &stableSnapshotRegistry{
		secret:    secret,
		ttl:       ttl,
		idleTTL:   idleTTL,
		maxActive: maxActive,
		now:       time.Now,
		snapshots: map[string]*stableSnapshotEntry{},
	}, nil
}

func (registry *stableSnapshotRegistry) cleanupLocked(now time.Time) {
	for id, entry := range registry.snapshots {
		if !now.Before(entry.ExpiresAt) || now.Sub(entry.LastUsedAt) >= registry.idleTTL {
			_ = entry.Snapshot.Close()
			delete(registry.snapshots, id)
		}
	}
}

func (registry *stableSnapshotRegistry) cleanup() {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.cleanupLocked(registry.now())
}

func (registry *stableSnapshotRegistry) create(store *archive.Store, lowerBound time.Time, afterFence *int64, limit int) (*stableSnapshotEntry, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	now := registry.now()
	registry.cleanupLocked(now)
	if len(registry.snapshots) >= registry.maxActive {
		return nil, errStableSnapshotLimit
	}
	snapshot, err := store.BeginStableSessionSnapshot(lowerBound, afterFence)
	if err != nil {
		return nil, err
	}
	random := make([]byte, 32)
	if _, err = rand.Read(random); err != nil {
		_ = snapshot.Close()
		return nil, err
	}
	afterValue := ""
	if afterFence != nil {
		afterValue = strconv.FormatInt(*afterFence, 10)
	}
	entry := &stableSnapshotEntry{
		ID:          base64.RawURLEncoding.EncodeToString(random),
		LowerBound:  lowerBound.UTC().Format(time.RFC3339Nano),
		AfterFence:  afterValue,
		Limit:       limit,
		CreatedAt:   now,
		LastUsedAt:  now,
		ExpiresAt:   now.Add(registry.ttl),
		Snapshot:    snapshot,
		CursorState: map[string]stableCursorClaims{},
	}
	registry.snapshots[entry.ID] = entry
	return entry, nil
}

func (registry *stableSnapshotRegistry) get(id, lowerBound, afterFence string, limit int) (*stableSnapshotEntry, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.cleanupLocked(registry.now())
	entry, ok := registry.snapshots[id]
	if !ok {
		return nil, errStableSnapshotExpired
	}
	if entry.LowerBound != lowerBound || entry.AfterFence != afterFence || entry.Limit != limit {
		return nil, archive.ErrSnapshotCursor
	}
	entry.LastUsedAt = registry.now()
	return entry, nil
}

func (registry *stableSnapshotRegistry) getForExport(id string) (*stableSnapshotEntry, error) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.cleanupLocked(registry.now())
	entry, ok := registry.snapshots[id]
	if !ok {
		return nil, errStableSnapshotExpired
	}
	entry.LastUsedAt = registry.now()
	return entry, nil
}

func (registry *stableSnapshotRegistry) export(ctx context.Context, id, sessionID, digest string, destination io.Writer) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	now := registry.now()
	registry.cleanupLocked(now)
	entry, ok := registry.snapshots[id]
	if !ok {
		return errStableSnapshotExpired
	}
	entry.LastUsedAt = now
	exportContext, cancel := context.WithDeadline(ctx, entry.ExpiresAt)
	defer cancel()
	err := entry.Snapshot.ExportSessionJSONL(exportContext, sessionID, digest, destination)
	entry.LastUsedAt = registry.now()
	if !entry.LastUsedAt.Before(entry.ExpiresAt) {
		_ = entry.Snapshot.Close()
		delete(registry.snapshots, id)
		if err == nil {
			err = errStableSnapshotExpired
		}
	}
	return err
}

func (registry *stableSnapshotRegistry) validateExport(id, sessionID, digest string) error {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	now := registry.now()
	registry.cleanupLocked(now)
	entry, ok := registry.snapshots[id]
	if !ok {
		return errStableSnapshotExpired
	}
	summary, found := entry.Snapshot.Summary(sessionID)
	if !found || summary.Deleted || summary.RecordsSHA256 != digest {
		return archive.ErrSnapshotCursor
	}
	entry.LastUsedAt = now
	return nil
}

func (registry *stableSnapshotRegistry) cursor(entry *stableSnapshotEntry, claims stableCursorClaims) (string, error) {
	raw, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, registry.secret)
	_, _ = mac.Write(raw)
	token := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	registry.mu.Lock()
	defer registry.mu.Unlock()
	current, ok := registry.snapshots[entry.ID]
	if !ok || current != entry || !registry.now().Before(entry.ExpiresAt) {
		return "", errStableSnapshotExpired
	}
	entry.LastUsedAt = registry.now()
	entry.CursorState[token] = claims
	return token, nil
}

func (registry *stableSnapshotRegistry) resolveCursor(entry *stableSnapshotEntry, token string) (stableCursorClaims, error) {
	if len(token) == 0 || len(token) > 128 {
		return stableCursorClaims{}, archive.ErrSnapshotCursor
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	current, ok := registry.snapshots[entry.ID]
	if !ok || current != entry || !registry.now().Before(entry.ExpiresAt) {
		return stableCursorClaims{}, errStableSnapshotExpired
	}
	entry.LastUsedAt = registry.now()
	claims, ok := entry.CursorState[token]
	if !ok {
		return stableCursorClaims{}, archive.ErrSnapshotCursor
	}
	return claims, nil
}

func (s *server) stableRegistry() (*stableSnapshotRegistry, error) {
	s.snapshotOnce.Do(func() {
		s.allowOfflineFull = env("ARCHIVE_ALLOW_OFFLINE_FULL_SNAPSHOT", "false") == "true"
		ttl := registryTTL(s.allowOfflineFull)
		s.snapshotRegistry, s.snapshotRegistryErr = newStableSnapshotRegistry(ttl, stableSnapshotIdleTTL, stableMaxActiveSnapshots)
		if s.snapshotRegistry != nil {
			go func() {
				ticker := time.NewTicker(30 * time.Second)
				defer ticker.Stop()
				for range ticker.C {
					s.snapshotRegistry.cleanup()
				}
			}()
		}
	})
	return s.snapshotRegistry, s.snapshotRegistryErr
}

func registryTTL(allowOfflineFull bool) time.Duration {
	if allowOfflineFull {
		return stableOfflineSnapshotTTL
	}
	return stableSnapshotTTL
}

func (registry *stableSnapshotRegistry) diagnostics() (int, int64) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	now := registry.now()
	registry.cleanupLocked(now)
	oldest := int64(0)
	for _, entry := range registry.snapshots {
		age := int64(now.Sub(entry.CreatedAt).Seconds())
		if age > oldest {
			oldest = age
		}
	}
	return len(registry.snapshots), oldest
}

func (s *server) stableSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	allowed := map[string]bool{
		"cursor_protocol":          true,
		"lower_bound_completed_at": true,
		"after_ingest_fence":       true,
		"limit":                    true,
		"snapshot":                 true,
		"cursor":                   true,
	}
	for key := range r.URL.Query() {
		if !allowed[key] {
			stableError(w, http.StatusBadRequest, "invalid stable session query")
			return
		}
	}
	if r.URL.Query().Get("cursor_protocol") != archive.StableCursorProtocol {
		stableError(w, http.StatusBadRequest, "unsupported cursor protocol")
		return
	}
	lowerRaw := strings.TrimSpace(r.URL.Query().Get("lower_bound_completed_at"))
	lowerBound, err := time.Parse(time.RFC3339Nano, lowerRaw)
	if err != nil {
		stableError(w, http.StatusBadRequest, "invalid lower bound")
		return
	}
	lowerCanonical := lowerBound.UTC().Format(time.RFC3339Nano)
	limit := 100
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		value, parseErr := strconv.Atoi(raw)
		if parseErr != nil || value < 1 || value > 1000 || raw != strconv.Itoa(value) {
			stableError(w, http.StatusBadRequest, "invalid page limit")
			return
		}
		limit = value
	}
	afterRaw := strings.TrimSpace(r.URL.Query().Get("after_ingest_fence"))
	var afterFence *int64
	if afterRaw != "" {
		value, parseErr := strconv.ParseInt(afterRaw, 10, 64)
		if parseErr != nil || value < 0 || afterRaw != strconv.FormatInt(value, 10) {
			stableError(w, http.StatusBadRequest, "invalid prior ingest fence")
			return
		}
		afterFence = &value
	}
	snapshotID := strings.TrimSpace(r.URL.Query().Get("snapshot"))
	cursor := strings.TrimSpace(r.URL.Query().Get("cursor"))
	if len(snapshotID) > 128 || (cursor != "" && snapshotID == "") {
		stableError(w, http.StatusBadRequest, "invalid stable session cursor")
		return
	}
	registry, err := s.stableRegistry()
	if err != nil {
		stableError(w, http.StatusServiceUnavailable, "stable session snapshots are unavailable")
		return
	}
	if snapshotID == "" && afterFence == nil && !s.allowOfflineFull {
		stableError(w, http.StatusForbidden, "full snapshots require an offline archive")
		return
	}
	var entry *stableSnapshotEntry
	if snapshotID == "" {
		entry, err = registry.create(s.s, lowerBound, afterFence, limit)
	} else {
		entry, err = registry.get(snapshotID, lowerCanonical, afterRaw, limit)
	}
	if err != nil {
		switch {
		case errors.Is(err, archive.ErrSnapshotProjectionNotReady):
			stableError(w, http.StatusServiceUnavailable, "stable session projection is preparing")
		case errors.Is(err, errStableSnapshotLimit):
			stableError(w, http.StatusTooManyRequests, "stable session snapshot capacity reached")
		case errors.Is(err, errStableSnapshotExpired):
			stableError(w, http.StatusGone, "stable session snapshot expired")
		case errors.Is(err, archive.ErrSnapshotCursor):
			stableError(w, http.StatusBadRequest, "invalid stable session cursor")
		case errors.Is(err, archive.ErrSnapshotTombstoneHistory):
			stableError(w, http.StatusConflict, "stable session tombstone history is unavailable before the upgrade fence")
		default:
			stableError(w, http.StatusInternalServerError, "stable session snapshot failed")
		}
		return
	}
	afterLastAt, afterSessionID := "", ""
	if cursor != "" {
		claims, cursorErr := registry.resolveCursor(entry, cursor)
		if cursorErr != nil {
			if errors.Is(cursorErr, errStableSnapshotExpired) {
				stableError(w, http.StatusGone, "stable session snapshot expired")
			} else {
				stableError(w, http.StatusBadRequest, "invalid stable session cursor")
			}
			return
		}
		if claims.SnapshotID != entry.ID || claims.LowerBound != entry.LowerBound || claims.AfterFence != entry.AfterFence || claims.Limit != entry.Limit {
			stableError(w, http.StatusBadRequest, "invalid stable session cursor")
			return
		}
		afterLastAt, afterSessionID = claims.LastAt, claims.SessionID
	}
	page, next, err := entry.Snapshot.Page(afterLastAt, afterSessionID, limit)
	if err != nil {
		stableError(w, http.StatusBadRequest, "invalid stable session cursor")
		return
	}
	var nextCursor any
	if next != nil {
		token, cursorErr := registry.cursor(entry, stableCursorClaims{
			SnapshotID: entry.ID,
			LowerBound: entry.LowerBound,
			AfterFence: entry.AfterFence,
			Limit:      entry.Limit,
			LastAt:     next.LastAt,
			SessionID:  next.SessionID,
		})
		if cursorErr != nil {
			stableError(w, http.StatusGone, "stable session snapshot expired")
			return
		}
		nextCursor = token
	}
	writeJSON(w, map[string]any{
		"snapshot_schema_version":           archive.StableSnapshotSchemaVersion,
		"cursor_protocol":                   archive.StableCursorProtocol,
		"snapshot":                          entry.ID,
		"ingest_fence":                      strconv.FormatInt(entry.Snapshot.IngestFence(), 10),
		"tombstone_safe_after_ingest_fence": strconv.FormatInt(entry.Snapshot.TombstoneSafeAfterIngestFence(), 10),
		"session_count":                     entry.Snapshot.SessionCount(),
		"request_count":                     entry.Snapshot.RequestCount(),
		"deleted_session_count":             entry.Snapshot.DeletedSessionCount(),
		"session_set_sha256":                entry.Snapshot.SessionSetSHA256(),
		"sessions":                          page,
		"complete":                          next == nil,
		"next_cursor":                       nextCursor,
	})
}

func stableError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func (s *server) digestRefresher() {
	const batch = 2
	const interval = 500 * time.Millisecond
	allowOfflineFull := env("ARCHIVE_ALLOW_OFFLINE_FULL_SNAPSHOT", "false") == "true"
	for {
		var updated int
		var err error
		if allowOfflineFull {
			updated, err = s.s.RefreshSessionExportDigests(context.Background(), batch)
		} else {
			updated, err = s.s.RefreshQueuedSessionExportDigests(context.Background(), batch)
		}
		if err != nil {
			log.Printf("stable session digest projection will retry: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if updated == 0 {
			time.Sleep(30 * time.Second)
			continue
		}
		time.Sleep(interval)
	}
}

func (s *server) validateStableSnapshotExport(ticket exportTicket) error {
	registry, err := s.stableRegistry()
	if err != nil {
		return err
	}
	return registry.validateExport(ticket.Snapshot, ticket.SessionID, ticket.RecordsSHA256)
}

func waitStableSnapshotExport(done <-chan error, interval time.Duration, heartbeat func()) error {
	if heartbeat == nil || interval <= 0 {
		return <-done
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			return err
		case <-ticker.C:
			heartbeat()
		}
	}
}

func (s *server) prepareStableSnapshotExport(r *http.Request, ticket exportTicket, heartbeat func()) (*os.File, int64, error) {
	registry, err := s.stableRegistry()
	if err != nil {
		return nil, 0, err
	}
	artifact, err := os.CreateTemp(filepath.Dir(s.s.DBPath), ".cpa-stable-session-*.jsonl")
	if err != nil {
		return nil, 0, err
	}
	cleanup := func() {
		_ = artifact.Close()
		_ = os.Remove(artifact.Name())
	}
	done := make(chan error, 1)
	go func() {
		done <- registry.export(r.Context(), ticket.Snapshot, ticket.SessionID, ticket.RecordsSHA256, artifact)
	}()
	if err = waitStableSnapshotExport(done, stableExportHeartbeatInterval, heartbeat); err != nil {
		cleanup()
		return nil, 0, err
	}
	info, err := artifact.Stat()
	if err != nil {
		cleanup()
		return nil, 0, err
	}
	if _, err = artifact.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, 0, err
	}
	return artifact, info.Size(), nil
}
