package archive

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestStableSnapshotMetadataPlanAvoidsPayloadTable(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "archive.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer store.DB.Close()

	rows, err := store.DB.Query(`EXPLAIN QUERY PLAN WITH selected(session_id) AS (
		SELECT DISTINCT records.session_id
		FROM records INDEXED BY idx_records_session_snapshot_metadata
	)
	SELECT selected.session_id,COUNT(records.id),MIN(records.started_at),MAX(records.completed_at)
	FROM selected
	LEFT JOIN records INDEXED BY idx_records_session_snapshot_metadata ON records.session_id=selected.session_id
	GROUP BY selected.session_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	covered := 0
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(detail, "COVERING INDEX idx_records_session_snapshot_metadata") {
			covered++
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if covered < 2 {
		t.Fatalf("stable snapshot metadata plan has %d covering accesses, want selected and aggregate paths", covered)
	}
}
