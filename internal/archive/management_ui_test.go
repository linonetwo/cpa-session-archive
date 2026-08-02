package archive

import (
	"bytes"
	"testing"
)

func TestManagementUIContainsBilingualFacetedExperience(t *testing.T) {
	checks := [][]byte{
		[]byte("Projects and repositories"),
		[]byte("项目与代码库"),
		[]byte("Faceted filters"),
		[]byte("分面筛选"),
		[]byte("MutationObserver"),
		[]byte("data-i18n"),
		[]byte("#/sessions/"),
		[]byte("metadata_only=true"),
		[]byte("Download this session"),
		[]byte("Parameterized SQL"),
		[]byte("previousPage"),
		[]byte("toolCallsAndResults"),
		[]byte("downloadExport('all')"),
		[]byte("#/sessions/"),
		[]byte("/raw/"),
		[]byte("/tools/"),
		[]byte("/request-context?id="),
		[]byte("extractToolEntriesFromValues"),
	}
	for _, check := range checks {
		if !bytes.Contains(managementHTML, check) {
			t.Errorf("management UI missing %q", check)
		}
	}
	for _, forbidden := range [][]byte{[]byte("sk-live-"), []byte("cpamp_actual_secret_")} {
		if bytes.Contains(managementHTML, forbidden) {
			t.Errorf("management UI contains credential-like literal %q", forbidden)
		}
	}
	if bytes.Contains(managementHTML, []byte("response.blob()")) {
		t.Error("complete exports must not be buffered in the browser")
	}
	if bytes.Contains(managementHTML, []byte("link.download")) {
		t.Error("download attribute overrides the server-provided JSONL filename")
	}
}
