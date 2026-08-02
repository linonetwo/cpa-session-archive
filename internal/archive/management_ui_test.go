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
		[]byte("Export complete session"),
		[]byte("Parameterized SQL"),
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
}
