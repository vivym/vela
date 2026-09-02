package h3campaignrunner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadManifestUsesStrictBoundedJSON(t *testing.T) {
	encoded, err := json.Marshal(validManifest())
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	path := filepath.Join(t.TempDir(), "campaign.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	manifest, err := LoadManifest(path)
	if err != nil || manifest.ProjectID != validManifest().ProjectID {
		t.Fatalf("LoadManifest = %#v error=%v", manifest, err)
	}

	for _, content := range []string{
		`{"schema_version":1,"schema_version":1}`,
		`{"schema_version":1,"unknown":true}`,
		`{"schema_version":1} {"schema_version":1}`,
	} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("rewrite manifest: %v", err)
		}
		if _, err := LoadManifest(path); err == nil {
			t.Fatalf("ambiguous manifest %q accepted", content)
		}
	}
}

func TestLoadManifestRejectsNonRegularAndOversizedInput(t *testing.T) {
	if _, err := LoadManifest(t.TempDir()); err == nil || !strings.Contains(err.Error(), "regular") {
		t.Fatalf("directory manifest error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "oversized.json")
	if err := os.WriteFile(path, make([]byte, maximumManifestBytes+1), 0o600); err != nil {
		t.Fatalf("write oversized manifest: %v", err)
	}
	if _, err := LoadManifest(path); err == nil || !strings.Contains(err.Error(), "size") {
		t.Fatalf("oversized manifest error = %v", err)
	}
}
