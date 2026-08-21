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
	"log"
	"net/http"
	"os"
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
	ExpiresAt   time.Time
	Snapshot    *archive.StableSessionSnapshot
	CursorState map[string]stableCursorClaims
}

type stableSnapshotRegistry struct {
	mu         sync.Mutex
	secret     []byte
	ttl        time.Duration
	maxActive  int
	now        func() time.Time
	snapshots  map[string]*stableSnapshotEntry
}

func newStableSnapshotRegistry(ttl time.Duration, maxActive int) (*stableSnapshotRegistry, error) {
	if ttl < time.Minute || ttl > 6*time.Hour {
		return nil, fmt.Errorf("snapshot ttl is outside the supported range")
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
		maxActive: maxActive,
		now:       time.Now,
		snapshots: map[string]*stableSnapshotEntry{},
	}, nil
}

func (registry *stableSnapshotRegistry) cleanupLocked(now time.Time) {
	for id, entry := range registry.snapshots {
		if !now.Before(entry.ExpiresAt) {
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
	return entry, nil
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
	claims, ok := entry.CursorState[token]
	if !ok {
		return stableCursorClaims{}, archive.ErrSnapshotCursor
	}
	return claims, nil
}

func (s *server) stableRegistry() (*stableSnapshotRegistry, error) {
	s.snapshotOnce.Do(func() {
		ttl := durationEnv("ARCHIVE_SNAPSHOT_TTL", 2*time.Hour, time.Minute, 6*time.Hour)
		maxActive := integerEnv("ARCHIVE_MAX_ACTIVE_SNAPSHOTS", 2, 1, 8)
		s.snapshotRegistry, s.snapshotRegistryErr = newStableSnapshotRegistry(ttl, maxActive)
		if s.snapshotRegistry != nil {
			interval := ttl / 4
			if interval > time.Minute {
				interval = time.Minute
			}
			go func() {
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				for range ticker.C {
					s.snapshotRegistry.cleanup()
				}
			}()
		}
	})
	return s.snapshotRegistry, s.snapshotRegistryErr
}

func durationEnv(name string, fallback, minimum, maximum time.Duration) time.Duration {
	value, err := time.ParseDuration(strings.TrimSpace(os.Getenv(name)))
	if err != nil || value < minimum || value > maximum {
		return fallback
	}
	return value
}

func integerEnv(name string, fallback, minimum, maximum int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name)))
	if err != nil || value < minimum || value > maximum {
		return fallback
	}
	return value
}

func (s *server) stableSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method", http.StatusMethodNotAllowed)
		return
	}
	allowed := map[string]bool{
		"cursor_protocol": true,
		"lower_bound_completed_at": true,
		"after_ingest_fence": true,
		"limit": true,
		"snapshot": true,
		"cursor": true,
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
		"cursor_protocol":   archive.StableCursorProtocol,
		"snapshot":          entry.ID,
		"ingest_fence":      strconv.FormatInt(entry.Snapshot.IngestFence(), 10),
		"session_count":     entry.Snapshot.SessionCount(),
		"request_count":     entry.Snapshot.RequestCount(),
		"session_set_sha256": entry.Snapshot.SessionSetSHA256(),
		"sessions":          page,
		"complete":          next == nil,
		"next_cursor":       nextCursor,
	})
}

func stableError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func (s *server) digestRefresher() {
	batch := integerEnv("ARCHIVE_DIGEST_BATCH", 2, 1, 64)
	interval := durationEnv("ARCHIVE_DIGEST_INTERVAL", 500*time.Millisecond, 100*time.Millisecond, time.Minute)
	for {
		updated, err := s.s.RefreshSessionExportDigests(context.Background(), batch)
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

func (s *server) exportStableSnapshot(w http.ResponseWriter, r *http.Request, ticket exportTicket) error {
	registry, err := s.stableRegistry()
	if err != nil {
		return err
	}
	entry, err := registry.getForExport(ticket.Snapshot)
	if err != nil {
		return err
	}
	return entry.Snapshot.ExportSessionJSONL(r.Context(), ticket.SessionID, ticket.RecordsSHA256, w)
}
