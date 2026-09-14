package archive

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestStableSnapshotMetadataIndexCoversAggregate(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()

	rows, err := store.DB.Query(`EXPLAIN QUERY PLAN
		SELECT records.session_id,COUNT(records.id),MIN(records.started_at),MAX(records.completed_at)
		FROM records INDEXED BY idx_records_session_snapshot_metadata
		GROUP BY records.session_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	covered := false
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		covered = covered || strings.Contains(detail, "COVERING INDEX idx_records_session_snapshot_metadata")
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !covered {
		t.Fatal("stable snapshot aggregate no longer uses its covering metadata index")
	}
}
