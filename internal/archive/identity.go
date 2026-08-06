package archive

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// CredentialPrincipal is a mutable projection from one credential version to
// its durable caller identity. Archived payloads and their CAS hashes are not
// changed when this projection is updated.
type CredentialPrincipal struct {
	CredentialHash string `json:"credential_hash"`
	PrincipalID    string `json:"principal_id"`
	Alias          string `json:"alias,omitempty"`
	Status         string `json:"status,omitempty"`
	UpdatedAt      string `json:"updated_at,omitempty"`
}

func identityString(metadata map[string]any, names ...string) string {
	for _, name := range names {
		if value, ok := metadata[name]; ok {
			if text := strings.TrimSpace(fmt.Sprint(value)); text != "" && text != "<nil>" {
				return text
			}
		}
	}
	return ""
}

func normalizeIdentityRecord(record *Record) {
	if record.Metadata == nil {
		record.Metadata = map[string]any{}
	}
	if record.PrincipalID == "" {
		record.PrincipalID = identityString(record.Metadata, "principal_id")
	}
	if record.CredentialHash == "" {
		record.CredentialHash = identityString(record.Metadata, "credential_hash", "key_hash", "api_key_hash", "key_id")
	}
	// key_id is the historical audit field. New producers should send the
	// credential hash, while old key_hash-only producers remain unchanged.
	if record.KeyID == "" {
		record.KeyID = record.CredentialHash
	}
	if record.Facets == nil {
		record.Facets = map[string][]string{}
	}
	if record.PrincipalID != "" {
		record.Facets["principal.id"] = []string{record.PrincipalID}
	}
	if record.CredentialHash != "" {
		record.Facets["credential.hash"] = []string{record.CredentialHash}
	}
}

type credentialPrincipalProjection struct {
	PrincipalID string
	Alias       string
}

func (s *Store) credentialPrincipalIndex(ctx context.Context) (map[string]credentialPrincipalProjection, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT credential_hash,principal_id,alias FROM credential_principals`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]credentialPrincipalProjection)
	for rows.Next() {
		var hash, principalID, alias string
		if err = rows.Scan(&hash, &principalID, &alias); err != nil {
			return nil, err
		}
		out[strings.ToLower(strings.TrimSpace(hash))] = credentialPrincipalProjection{
			PrincipalID: principalID,
			Alias:       alias,
		}
	}
	return out, rows.Err()
}

func looksLikeCredentialHash(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func resolveIdentity(record *Record, identities map[string]credentialPrincipalProjection) {
	normalizeIdentityRecord(record)
	candidate := strings.TrimSpace(record.CredentialHash)
	if candidate == "" {
		candidate = strings.TrimSpace(record.KeyID)
	}
	if candidate == "" {
		return
	}
	identity, mapped := identities[strings.ToLower(candidate)]
	if record.CredentialHash == "" && (mapped || looksLikeCredentialHash(candidate)) {
		record.CredentialHash = candidate
		record.Facets["credential.hash"] = []string{candidate}
	}
	if record.PrincipalID == "" {
		record.PrincipalID = identity.PrincipalID
	}
	if record.PrincipalAlias == "" {
		record.PrincipalAlias = identity.Alias
	}
	if record.PrincipalID != "" {
		record.Facets["principal.id"] = []string{record.PrincipalID}
	}
}

func (s *Store) CredentialPrincipals(ctx context.Context) ([]CredentialPrincipal, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT credential_hash,principal_id,alias,status,updated_at
		FROM credential_principals ORDER BY principal_id,updated_at,credential_hash`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []CredentialPrincipal{}
	for rows.Next() {
		var item CredentialPrincipal
		if err = rows.Scan(&item.CredentialHash, &item.PrincipalID, &item.Alias, &item.Status, &item.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// ApplyCredentialPrincipals updates only searchable identity projections.
// records.metadata_json and CAS blobs deliberately remain byte-for-byte intact.
func (s *Store) ApplyCredentialPrincipals(ctx context.Context, mappings []CredentialPrincipal) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	affected := map[string]bool{}
	for _, item := range mappings {
		item.CredentialHash = strings.TrimSpace(item.CredentialHash)
		item.PrincipalID = strings.TrimSpace(item.PrincipalID)
		if item.CredentialHash == "" || item.PrincipalID == "" {
			return fmt.Errorf("credential_hash and principal_id are required")
		}
		if item.Status == "" {
			item.Status = "active"
		}
		if item.UpdatedAt == "" {
			item.UpdatedAt = now
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO credential_principals(credential_hash,principal_id,alias,status,updated_at)
			VALUES(?,?,?,?,?) ON CONFLICT(credential_hash) DO UPDATE SET
			principal_id=excluded.principal_id,alias=excluded.alias,status=excluded.status,updated_at=excluded.updated_at`,
			item.CredentialHash, item.PrincipalID, item.Alias, item.Status, item.UpdatedAt); err != nil {
			return err
		}
		rows, queryErr := tx.QueryContext(ctx, `SELECT DISTINCT session_id FROM records
			WHERE credential_hash=? COLLATE NOCASE OR (COALESCE(credential_hash,'')='' AND key_id=? COLLATE NOCASE)`,
			item.CredentialHash, item.CredentialHash)
		if queryErr != nil {
			return queryErr
		}
		for rows.Next() {
			var sessionID string
			if queryErr = rows.Scan(&sessionID); queryErr != nil {
				rows.Close()
				return queryErr
			}
			affected[sessionID] = true
		}
		if queryErr = rows.Close(); queryErr != nil {
			return queryErr
		}
		if _, err = tx.ExecContext(ctx, `UPDATE records SET
			credential_hash=CASE WHEN COALESCE(credential_hash,'')='' THEN key_id ELSE credential_hash END,
			principal_id=? WHERE credential_hash=? COLLATE NOCASE
			OR (COALESCE(credential_hash,'')='' AND key_id=? COLLATE NOCASE)`,
			item.PrincipalID, item.CredentialHash, item.CredentialHash); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE turn_records SET principal_id=?,credential_hash=?
			WHERE request_id IN (SELECT request_id FROM records WHERE principal_id=? AND credential_hash=? COLLATE NOCASE)`,
			item.PrincipalID, item.CredentialHash, item.PrincipalID, item.CredentialHash); err != nil {
			return err
		}
	}
	for sessionID := range affected {
		if _, err = tx.ExecContext(ctx, `DELETE FROM record_facets WHERE request_id IN
			(SELECT request_id FROM records WHERE session_id=?) AND name IN ('principal.id','credential.hash')`, sessionID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO record_facets(request_id,name,value)
			SELECT request_id,'principal.id',principal_id FROM records WHERE session_id=? AND COALESCE(principal_id,'')<>''`, sessionID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO record_facets(request_id,name,value)
			SELECT request_id,'credential.hash',credential_hash FROM records WHERE session_id=? AND COALESCE(credential_hash,'')<>''`, sessionID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM session_facets WHERE session_id=? AND name IN ('principal.id','credential.hash')`, sessionID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO session_facets(session_id,name,value)
			SELECT DISTINCT session_id,'principal.id',principal_id FROM records WHERE session_id=? AND COALESCE(principal_id,'')<>''`, sessionID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO session_facets(session_id,name,value)
			SELECT DISTINCT session_id,'credential.hash',credential_hash FROM records WHERE session_id=? AND COALESCE(credential_hash,'')<>''`, sessionID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE session_summaries SET principal_id=COALESCE(
			(SELECT principal_id FROM records WHERE session_id=? AND COALESCE(principal_id,'')<>'' ORDER BY started_at LIMIT 1),'')
			WHERE session_id=?`, sessionID, sessionID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func DecodeCredentialPrincipals(raw []byte) ([]CredentialPrincipal, error) {
	var envelope struct {
		Mappings    []CredentialPrincipal `json:"mappings"`
		Credentials []struct {
			KeyHash        string `json:"key_hash"`
			CredentialHash string `json:"credential_hash"`
			PrincipalID    string `json:"principal_id"`
			Alias          string `json:"alias"`
			Status         string `json:"status"`
		} `json:"credentials"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, err
	}
	out := append([]CredentialPrincipal(nil), envelope.Mappings...)
	for _, item := range envelope.Credentials {
		hash := item.CredentialHash
		if hash == "" {
			hash = item.KeyHash
		}
		out = append(out, CredentialPrincipal{CredentialHash: hash, PrincipalID: item.PrincipalID, Alias: item.Alias, Status: item.Status})
	}
	return out, nil
}
