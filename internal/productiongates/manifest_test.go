package productiongates

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadManifestVerifiesCompleteReleaseEvidence(t *testing.T) {
	directory := t.TempDir()
	manifestPath := writeManifestFixture(t, directory, func(_ *Manifest) {})

	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		t.Fatalf("load complete manifest: %v", err)
	}
	if manifest.ReleaseDigest != fixtureDigest("release") ||
		manifest.ConfigurationRevision != "config-rev-1" ||
		manifest.Evaluation.Pass != len(AllGates()) ||
		manifest.Digest == "" {
		t.Fatalf("loaded manifest = %#v", manifest)
	}
}

func TestLoadManifestRejectsUnboundOrUnverifiedEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*Manifest)
		check  func(*testing.T, string)
	}{
		{
			name: "mixed release",
			mutate: func(manifest *Manifest) {
				manifest.Receipts[1].ReleaseDigest = fixtureDigest("other-release")
			},
		},
		{
			name: "mixed configuration",
			mutate: func(manifest *Manifest) {
				manifest.Receipts[1].ConfigurationRevision = "config-rev-2"
			},
		},
		{
			name: "missing gate",
			mutate: func(manifest *Manifest) {
				manifest.Receipts = manifest.Receipts[:len(manifest.Receipts)-1]
			},
		},
		{
			name: "evidence digest mismatch",
			mutate: func(manifest *Manifest) {
				manifest.Receipts[0].EvidenceDigest = fixtureDigest("not-the-evidence")
			},
		},
		{
			name: "path escape",
			mutate: func(manifest *Manifest) {
				manifest.Receipts[0].EvidenceRef = "../outside.json"
			},
			check: func(t *testing.T, directory string) {
				t.Helper()
				if err := os.WriteFile(filepath.Join(directory, "..", "outside.json"), []byte("outside"), 0o600); err != nil {
					t.Fatalf("write escaped evidence: %v", err)
				}
			},
		},
		{
			name: "non-regular evidence",
			mutate: func(manifest *Manifest) {
				manifest.Receipts[0].EvidenceRef = "evidence"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			manifestPath := writeManifestFixture(t, directory, test.mutate)
			if test.check != nil {
				test.check(t, directory)
			}
			if _, err := LoadManifest(manifestPath); !errors.Is(err, ErrInvalidManifest) {
				t.Fatalf("LoadManifest error = %v, want invalid manifest", err)
			}
		})
	}
}

func TestLoadManifestRejectsUnknownFieldAndTrailingDocument(t *testing.T) {
	for _, suffix := range []string{
		`,"waiver":"verbal"}`,
		`}\n{}`,
	} {
		directory := t.TempDir()
		manifestPath := writeManifestFixture(t, directory, func(_ *Manifest) {})
		encoded, err := os.ReadFile(manifestPath)
		if err != nil {
			t.Fatalf("read manifest fixture: %v", err)
		}
		encoded = []byte(strings.TrimSuffix(string(encoded), "}") + suffix)
		if err := os.WriteFile(manifestPath, encoded, 0o600); err != nil {
			t.Fatalf("rewrite malformed manifest: %v", err)
		}
		if _, err := LoadManifest(manifestPath); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("LoadManifest error = %v, want invalid manifest", err)
		}
	}
}

func TestLoadManifestRejectsDuplicateJSONKeys(t *testing.T) {
	for _, replacement := range []string{
		`"schema_version":1,"schema_version":1`,
		`"gate":"preset-certification","gate":"preset-certification"`,
	} {
		directory := t.TempDir()
		manifestPath := writeManifestFixture(t, directory, func(_ *Manifest) {})
		encoded, err := os.ReadFile(manifestPath)
		if err != nil {
			t.Fatalf("read manifest fixture: %v", err)
		}
		key := strings.SplitN(replacement, ",", 2)[0]
		encoded = []byte(strings.Replace(string(encoded), key, replacement, 1))
		if err := os.WriteFile(manifestPath, encoded, 0o600); err != nil {
			t.Fatalf("rewrite duplicate-key manifest: %v", err)
		}
		if _, err := LoadManifest(manifestPath); !errors.Is(err, ErrInvalidManifest) ||
			!strings.Contains(err.Error(), "duplicate JSON key") {
			t.Fatalf("LoadManifest error = %v, want duplicate-key rejection", err)
		}
	}
}

func TestLoadManifestRejectsEvidenceSymlinkOutsideRoot(t *testing.T) {
	directory := t.TempDir()
	manifestPath := writeManifestFixture(t, directory, func(manifest *Manifest) {
		manifest.Receipts[0].EvidenceRef = "evidence/outside-link.json"
	})
	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, []byte(`{"result":"independent production evidence"}`), 0o600); err != nil {
		t.Fatalf("write outside evidence: %v", err)
	}
	link := filepath.Join(directory, "evidence", "outside-link.json")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatalf("create outside evidence symlink: %v", err)
	}
	if _, err := LoadManifest(manifestPath); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("LoadManifest error = %v, want escaped symlink rejection", err)
	}
}

func TestLoadManifestWithinRejectsManifestSymlinkOutsideRoot(t *testing.T) {
	outside := t.TempDir()
	manifestPath := writeManifestFixture(t, outside, func(_ *Manifest) {})
	directory := t.TempDir()
	if err := os.Symlink(manifestPath, filepath.Join(directory, "launch-receipts.json")); err != nil {
		t.Fatalf("create outside manifest symlink: %v", err)
	}
	if _, err := LoadManifestWithin(directory, "launch-receipts.json"); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("LoadManifestWithin error = %v, want escaped manifest rejection", err)
	}
}

func writeManifestFixture(t *testing.T, directory string, mutate func(*Manifest)) string {
	t.Helper()
	evidence := []byte(`{"result":"independent production evidence"}`)
	evidenceDigest := sha256.Sum256(evidence)
	manifest := Manifest{SchemaVersion: 1, Receipts: make([]Receipt, 0, len(AllGates()))}
	for index, gate := range AllGates() {
		evidenceRef := filepath.ToSlash(filepath.Join("evidence", string(gate)+".json"))
		evidencePath := filepath.Join(directory, filepath.FromSlash(evidenceRef))
		if err := os.MkdirAll(filepath.Dir(evidencePath), 0o700); err != nil {
			t.Fatalf("create evidence directory: %v", err)
		}
		if err := os.WriteFile(evidencePath, evidence, 0o600); err != nil {
			t.Fatalf("write evidence fixture: %v", err)
		}
		startedAt := time.Date(2026, 8, 28, 1, index, 0, 0, time.UTC)
		manifest.Receipts = append(manifest.Receipts, Receipt{
			SchemaVersion:         1,
			Gate:                  gate,
			ReleaseDigest:         fixtureDigest("release"),
			ConfigurationRevision: "config-rev-1",
			ValidationEnvironment: "production-validation-rack-1",
			Result:                ResultPass,
			Owner:                 "platform-oncall@example.invalid",
			AcceptanceThreshold:   "all required assertions pass",
			ObservedResult:        "all required assertions passed",
			EvidenceRef:           evidenceRef,
			EvidenceDigest:        "sha256:" + hex.EncodeToString(evidenceDigest[:]),
			StartedAt:             startedAt,
			CompletedAt:           startedAt.Add(time.Minute),
			RecordedAt:            startedAt.Add(2 * time.Minute),
		})
	}
	mutate(&manifest)
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("encode manifest fixture: %v", err)
	}
	manifestPath := filepath.Join(directory, "launch-receipts.json")
	if err := os.WriteFile(manifestPath, encoded, 0o600); err != nil {
		t.Fatalf("write manifest fixture: %v", err)
	}
	return manifestPath
}

func fixtureDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(digest[:])
}
