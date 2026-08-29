package productiongates

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/slo"
	"github.com/vivym/vela/internal/sloevidence"
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

func TestManifestValidateBindingRequiresExactReleaseAndConfiguration(t *testing.T) {
	manifest := Manifest{
		ReleaseDigest:         fixtureDigest("release"),
		ConfigurationRevision: "config-rev-1",
	}
	if err := manifest.ValidateBinding(manifest.ReleaseDigest, manifest.ConfigurationRevision); err != nil {
		t.Fatalf("validate exact manifest binding: %v", err)
	}
	if err := manifest.ValidateBinding(fixtureDigest("other-release"), manifest.ConfigurationRevision); err == nil ||
		!strings.Contains(err.Error(), "want release=") {
		t.Fatalf("release mismatch error = %v", err)
	}
	if err := manifest.ValidateBinding(manifest.ReleaseDigest, "config-rev-2"); err == nil ||
		!strings.Contains(err.Error(), "configuration=config-rev-2") {
		t.Fatalf("configuration mismatch error = %v", err)
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
			name: "owner binding mismatch",
			mutate: func(manifest *Manifest) {
				manifest.Receipts[0].Owner = "other-owner@example.invalid"
			},
		},
		{
			name: "environment binding mismatch",
			mutate: func(manifest *Manifest) {
				manifest.Receipts[0].ValidationEnvironment = "other-validation-rack"
			},
		},
		{
			name: "time binding mismatch",
			mutate: func(manifest *Manifest) {
				manifest.Receipts[0].CompletedAt = manifest.Receipts[0].CompletedAt.Add(time.Second)
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

func TestLoadManifestRejectsOpaqueObservabilityEvidence(t *testing.T) {
	directory := t.TempDir()
	manifestPath := writeManifestFixture(t, directory, func(manifest *Manifest) {
		for index := range manifest.Receipts {
			if manifest.Receipts[index].Gate != GateObservabilityOnCall {
				continue
			}
			opaque := []byte(`{"result":"independent production evidence"}`)
			path := filepath.Join(directory, filepath.FromSlash(manifest.Receipts[index].EvidenceRef))
			if err := os.WriteFile(path, opaque, 0o600); err != nil {
				t.Fatalf("write opaque observability evidence: %v", err)
			}
			manifest.Receipts[index].EvidenceDigest = sloevidence.Digest(opaque)
		}
	})
	if _, err := LoadManifest(manifestPath); !errors.Is(err, ErrInvalidManifest) ||
		!strings.Contains(err.Error(), "observability evidence semantics") {
		t.Fatalf("LoadManifest opaque observability error = %v", err)
	}
}

func TestLoadManifestRejectsOpaqueTypedEvidence(t *testing.T) {
	directory := t.TempDir()
	manifestPath := writeManifestFixture(t, directory, func(manifest *Manifest) {
		receipt := receiptForGate(t, manifest, GateDataDisasterRecovery)
		opaque := []byte(`{"result":"PASS"}`)
		path := filepath.Join(directory, filepath.FromSlash(receipt.EvidenceRef))
		if err := os.WriteFile(path, opaque, 0o600); err != nil {
			t.Fatalf("write opaque typed evidence: %v", err)
		}
		receipt.EvidenceDigest = sloevidence.Digest(opaque)
	})
	if _, err := LoadManifest(manifestPath); !errors.Is(err, ErrInvalidManifest) ||
		!strings.Contains(err.Error(), "evidence semantics") {
		t.Fatalf("LoadManifest opaque typed evidence error = %v", err)
	}
}

func TestLoadManifestRejectsInvalidTypedArtifacts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, string, *Manifest)
	}{
		{
			name: "content tamper",
			mutate: func(t *testing.T, directory string, manifest *Manifest) {
				evidence := readTypedEvidenceFixture(t, directory, manifest, GateReleaseRollback)
				path := filepath.Join(directory, filepath.FromSlash(evidence.Artifacts[0].Ref))
				if err := os.WriteFile(path, []byte(`{"tampered":true}`), 0o600); err != nil {
					t.Fatalf("tamper typed artifact: %v", err)
				}
			},
		},
		{
			name: "path escape",
			mutate: func(t *testing.T, directory string, manifest *Manifest) {
				mutateTypedEvidenceFixture(t, directory, manifest, GateGPURemediation, func(evidence *TypedEvidence) {
					evidence.Artifacts[0].Ref = "../outside.json"
				})
			},
		},
		{
			name: "symlink escape",
			mutate: func(t *testing.T, directory string, manifest *Manifest) {
				outsideContent := []byte(`{"schema_version":1}`)
				outside := filepath.Join(t.TempDir(), "outside.json")
				if err := os.WriteFile(outside, outsideContent, 0o600); err != nil {
					t.Fatalf("write outside typed artifact: %v", err)
				}
				linkRef := filepath.ToSlash(filepath.Join("typed", "outside-link.json"))
				link := filepath.Join(directory, filepath.FromSlash(linkRef))
				if err := os.MkdirAll(filepath.Dir(link), 0o700); err != nil {
					t.Fatalf("create typed artifact link directory: %v", err)
				}
				if err := os.Symlink(outside, link); err != nil {
					t.Fatalf("create typed artifact symlink: %v", err)
				}
				mutateTypedEvidenceFixture(t, directory, manifest, GateCommercialLifecycle, func(evidence *TypedEvidence) {
					evidence.Artifacts[0].Ref = linkRef
					evidence.Artifacts[0].Digest = sloevidence.Digest(outsideContent)
				})
			},
		},
		{
			name: "oversized",
			mutate: func(t *testing.T, directory string, manifest *Manifest) {
				content := bytes.Repeat([]byte("x"), MaxEvidenceArtifactBytes+1)
				ref := filepath.ToSlash(filepath.Join("typed", "oversized.json"))
				if err := os.WriteFile(filepath.Join(directory, filepath.FromSlash(ref)), content, 0o600); err != nil {
					t.Fatalf("write oversized typed artifact: %v", err)
				}
				mutateTypedEvidenceFixture(t, directory, manifest, GateDataDisasterRecovery, func(evidence *TypedEvidence) {
					evidence.Artifacts[0].Ref = ref
					evidence.Artifacts[0].Digest = sloevidence.Digest(content)
				})
			},
		},
		{
			name: "artifact owner binding mismatch",
			mutate: func(t *testing.T, directory string, manifest *Manifest) {
				mutateTypedArtifactFixture(t, directory, manifest, GateOrganizationIsolation, 0, func(artifact *TypedEvidenceArtifact) {
					artifact.Owner = "other-owner@example.invalid"
				})
			},
		},
		{
			name: "artifact observation mismatch",
			mutate: func(t *testing.T, directory string, manifest *Manifest) {
				mutateTypedArtifactFixture(t, directory, manifest, GateStateEventFaultInjection, 0, func(artifact *TypedEvidenceArtifact) {
					artifact.Checks = artifact.Checks[1:]
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			manifestPath := writeManifestFixture(t, directory, func(manifest *Manifest) {
				test.mutate(t, directory, manifest)
			})
			if _, err := LoadManifest(manifestPath); !errors.Is(err, ErrInvalidManifest) {
				t.Fatalf("LoadManifest error = %v, want invalid manifest", err)
			}
		})
	}
}

func TestVerifyEvidenceRejectsExhaustedManifestArtifactBudget(t *testing.T) {
	directory := t.TempDir()
	manifestPath := writeManifestFixture(t, directory, func(_ *Manifest) {})
	encoded, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest fixture: %v", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(encoded, &manifest); err != nil {
		t.Fatalf("decode manifest fixture: %v", err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatalf("open manifest fixture root: %v", err)
	}
	defer func() { _ = root.Close() }()
	if _, _, err := verifyEvidence(
		root,
		*receiptForGate(t, &manifest, GateDataDisasterRecovery),
		0,
	); !errors.Is(err, ErrInvalidManifest) || !strings.Contains(err.Error(), "aggregate size limit") {
		t.Fatalf("verifyEvidence exhausted budget error = %v", err)
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
	manifest := Manifest{SchemaVersion: 1, Receipts: make([]Receipt, 0, len(AllGates()))}
	for index, gate := range AllGates() {
		startedAt := time.Date(2026, 8, 28, 1, index, 0, 0, time.UTC)
		completedAt := startedAt.Add(time.Minute)
		if gate == GateRealH3Soak {
			completedAt = startedAt.Add(72 * time.Hour)
		}
		acceptanceThreshold := "all required assertions pass"
		observedResult := "all required assertions passed"
		var gateEvidence []byte
		if gate == GateObservabilityOnCall {
			gateEvidence = observabilityEvidenceFixture(t, directory)
		} else {
			typed := passingTypedEvidenceFixture(t, directory, gate, startedAt, completedAt)
			acceptanceThreshold = typed.AcceptanceThreshold()
			observedResult = typed.ObservedResult()
			var err error
			gateEvidence, err = json.Marshal(typed)
			if err != nil {
				t.Fatalf("encode %s evidence fixture: %v", gate, err)
			}
		}
		gateEvidenceDigest := sha256.Sum256(gateEvidence)
		evidenceRef := filepath.ToSlash(filepath.Join("evidence", string(gate)+".json"))
		evidencePath := filepath.Join(directory, filepath.FromSlash(evidenceRef))
		if err := os.MkdirAll(filepath.Dir(evidencePath), 0o700); err != nil {
			t.Fatalf("create evidence directory: %v", err)
		}
		if err := os.WriteFile(evidencePath, gateEvidence, 0o600); err != nil {
			t.Fatalf("write evidence fixture: %v", err)
		}
		manifest.Receipts = append(manifest.Receipts, Receipt{
			SchemaVersion:         1,
			Gate:                  gate,
			ReleaseDigest:         fixtureDigest("release"),
			ConfigurationRevision: "config-rev-1",
			ValidationEnvironment: "production-validation-rack-1",
			Result:                ResultPass,
			Owner:                 "platform-oncall@example.invalid",
			AcceptanceThreshold:   acceptanceThreshold,
			ObservedResult:        observedResult,
			EvidenceRef:           evidenceRef,
			EvidenceDigest:        "sha256:" + hex.EncodeToString(gateEvidenceDigest[:]),
			StartedAt:             startedAt,
			CompletedAt:           completedAt,
			RecordedAt:            completedAt.Add(time.Minute),
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

func receiptForGate(t *testing.T, manifest *Manifest, gate Gate) *Receipt {
	t.Helper()
	for index := range manifest.Receipts {
		if manifest.Receipts[index].Gate == gate {
			return &manifest.Receipts[index]
		}
	}
	t.Fatalf("manifest fixture lacks receipt for %s", gate)
	return nil
}

func readTypedEvidenceFixture(
	t *testing.T,
	directory string,
	manifest *Manifest,
	gate Gate,
) TypedEvidence {
	t.Helper()
	receipt := receiptForGate(t, manifest, gate)
	encoded, err := os.ReadFile(filepath.Join(directory, filepath.FromSlash(receipt.EvidenceRef)))
	if err != nil {
		t.Fatalf("read %s typed evidence fixture: %v", gate, err)
	}
	var evidence TypedEvidence
	if err := json.Unmarshal(encoded, &evidence); err != nil {
		t.Fatalf("decode %s typed evidence fixture: %v", gate, err)
	}
	return evidence
}

func mutateTypedEvidenceFixture(
	t *testing.T,
	directory string,
	manifest *Manifest,
	gate Gate,
	mutate func(*TypedEvidence),
) {
	t.Helper()
	receipt := receiptForGate(t, manifest, gate)
	evidence := readTypedEvidenceFixture(t, directory, manifest, gate)
	mutate(&evidence)
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatalf("encode mutated %s typed evidence: %v", gate, err)
	}
	if err := os.WriteFile(
		filepath.Join(directory, filepath.FromSlash(receipt.EvidenceRef)),
		encoded,
		0o600,
	); err != nil {
		t.Fatalf("write mutated %s typed evidence: %v", gate, err)
	}
	receipt.EvidenceDigest = sloevidence.Digest(encoded)
}

func mutateTypedArtifactFixture(
	t *testing.T,
	directory string,
	manifest *Manifest,
	gate Gate,
	artifactIndex int,
	mutate func(*TypedEvidenceArtifact),
) {
	t.Helper()
	mutateTypedEvidenceFixture(t, directory, manifest, gate, func(evidence *TypedEvidence) {
		reference := &evidence.Artifacts[artifactIndex]
		path := filepath.Join(directory, filepath.FromSlash(reference.Ref))
		encoded, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s/%s typed artifact fixture: %v", gate, reference.Kind, err)
		}
		var artifact TypedEvidenceArtifact
		if err := json.Unmarshal(encoded, &artifact); err != nil {
			t.Fatalf("decode %s/%s typed artifact fixture: %v", gate, reference.Kind, err)
		}
		mutate(&artifact)
		encoded, err = json.Marshal(artifact)
		if err != nil {
			t.Fatalf("encode mutated %s/%s typed artifact: %v", gate, reference.Kind, err)
		}
		if err := os.WriteFile(path, encoded, 0o600); err != nil {
			t.Fatalf("write mutated %s/%s typed artifact: %v", gate, reference.Kind, err)
		}
		reference.Digest = sloevidence.Digest(encoded)
	})
}

func passingTypedEvidenceFixture(
	t *testing.T,
	directory string,
	gate Gate,
	startedAt,
	completedAt time.Time,
) TypedEvidence {
	t.Helper()
	contract, ok := TypedEvidenceContractForGate(gate)
	if !ok {
		t.Fatalf("typed evidence contract missing for %s", gate)
	}
	evidence := TypedEvidence{
		SchemaVersion: 1, Gate: gate, CriteriaRevision: contract.CriteriaRevision,
		ReleaseDigest: fixtureDigest("release"), ConfigurationRevision: "config-rev-1",
		ValidationEnvironment: "production-validation-rack-1",
		Owner:                 "platform-oncall@example.invalid",
		StartedAt:             startedAt, CompletedAt: completedAt,
		Checks:       make([]EvidenceCheck, 0, len(contract.CheckIDs)),
		Measurements: make([]EvidenceMeasurement, 0, len(contract.Measurements)),
		Artifacts:    make([]EvidenceArtifact, 0, len(contract.ArtifactKinds)),
	}
	for _, id := range contract.CheckIDs {
		evidence.Checks = append(evidence.Checks, EvidenceCheck{ID: id, Passed: true})
	}
	for _, requirement := range contract.Measurements {
		evidence.Measurements = append(evidence.Measurements, EvidenceMeasurement{
			ID: requirement.ID, Unit: requirement.Unit,
			Comparator: requirement.Comparator, Threshold: requirement.Threshold,
			Observed: requirement.Threshold,
		})
	}
	if gate == GatePresetCertification {
		claims := make([]PresetCertificationClaim, 0, 3)
		for index, preset := range []string{"quality", "balanced", "fast"} {
			claims = append(claims, PresetCertificationClaim{
				EvidenceID:                 fmt.Sprintf("35000000-0000-0000-0000-%012d", 401+index),
				ProfileCertificationID:     fmt.Sprintf("35000000-0000-0000-0000-%012d", 501+index),
				InferenceBackendRevisionID: "35000000-0000-0000-0000-000000000601",
				SaleableGroupID:            "model-v1/standard-v1/1080p-v1/CNY",
				StablePreset:               preset,
				HardwareDriverBaseline:     "h3-8gpu-driver-r1",
				BenchmarkCorpusRevision:    "h3-video-quality-v2",
				SampleCount:                100,
				QualityThresholdPPM:        820000, QualityObservedPPM: 850000,
				SuccessRateThresholdPPM: 990000, SuccessRateObservedPPM: 999000,
				P50Milliseconds: 900000, P95ThresholdMilliseconds: 1800000,
				P95ObservedMilliseconds: 1700000,
				CostThresholdMinor:      500000, CostObservedMinor: 450000, CostCurrency: "CNY",
				ConfidenceThresholdPPM: 950000, ConfidenceObservedPPM: 990000,
			})
		}
		evidence.PresetCertification = &PresetCertificationClaims{
			SaleableGroupIDs: []string{"model-v1/standard-v1/1080p-v1/CNY"},
			Certifications:   claims,
			RateCards: []RateCardPromotionClaim{{
				BindingID:          "35000000-0000-0000-0000-000000000701",
				RateCardRevisionID: "35000000-0000-0000-0000-000000000702",
			}},
		}
	}
	writeTypedEvidenceArtifacts(t, directory, &evidence, contract.ArtifactKinds)
	return evidence
}

func writeTypedEvidenceArtifacts(
	t *testing.T,
	directory string,
	evidence *TypedEvidence,
	kinds []string,
) {
	t.Helper()
	payloads := make([]TypedEvidenceArtifact, len(kinds))
	for index, kind := range kinds {
		payloads[index] = TypedEvidenceArtifact{
			SchemaVersion: 1, Gate: evidence.Gate, Kind: kind,
			ReleaseDigest: evidence.ReleaseDigest, ConfigurationRevision: evidence.ConfigurationRevision,
			ValidationEnvironment: evidence.ValidationEnvironment, Owner: evidence.Owner,
			StartedAt: evidence.StartedAt, CompletedAt: evidence.CompletedAt,
			Checks: make([]EvidenceCheck, 0), Measurements: make([]EvidenceMeasurement, 0),
		}
	}
	for index, check := range evidence.Checks {
		payloads[index%len(payloads)].Checks = append(payloads[index%len(payloads)].Checks, check)
	}
	for index, measurement := range evidence.Measurements {
		payloads[index%len(payloads)].Measurements = append(
			payloads[index%len(payloads)].Measurements,
			measurement,
		)
	}
	for index := range payloads {
		if evidence.Gate == GatePresetCertification {
			payloads[index].PresetCertification = evidence.PresetCertification
		}
		content, err := json.Marshal(payloads[index])
		if err != nil {
			t.Fatalf("encode %s/%s evidence artifact: %v", evidence.Gate, payloads[index].Kind, err)
		}
		ref := filepath.ToSlash(filepath.Join("typed", string(evidence.Gate), payloads[index].Kind+".json"))
		path := filepath.Join(directory, filepath.FromSlash(ref))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("create typed evidence artifact directory: %v", err)
		}
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("write typed evidence artifact: %v", err)
		}
		evidence.Artifacts = append(evidence.Artifacts, EvidenceArtifact{
			Kind: payloads[index].Kind, Ref: ref, Digest: sloevidence.Digest(content),
		})
	}
}

func observabilityEvidenceFixture(t *testing.T, directory string) []byte {
	t.Helper()
	window := slo.Window{
		Start: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
	}
	evaluatedAt := window.End.Add(time.Hour)
	cohorts := make([]sloevidence.CohortEvidence, 0, 3)
	contractIDs := make([]string, 0, 3)
	for _, preset := range []string{"quality", "balanced", "fast"} {
		contract := slo.Contract{
			RevisionID: "slo-" + preset + "-v1", ModelRevisionID: "model-v1",
			GenerationPreset: preset, GenerationPresetRevisionID: preset + "-v1",
			ServiceClassRevisionID: "standard-v1", OutputSpecID: "1080p-v1",
			GenerationCount: 1, P95TargetMilliseconds: 60_000,
			SuccessTargetPPM: 800_000, MinimumSample: 20,
			ConfidenceMethod:      slo.ConfidenceMethod,
			OneSidedConfidencePPM: slo.OneSidedConfidencePPM,
			CancellationPolicy:    slo.CancellationPolicy,
		}
		observations := make([]slo.Observation, 0, 20)
		for index := 0; index < 20; index++ {
			accepted := window.Start.Add(time.Duration(index+1) * time.Minute)
			completed := accepted.Add(time.Duration(index+1) * time.Second)
			observations = append(observations, slo.Observation{
				JobID:              fmt.Sprintf("%s-%02d", preset, index),
				ContractRevisionID: contract.RevisionID,
				AcceptedAt:         accepted, ExpiresAt: accepted.Add(time.Hour),
				Outcome: slo.OutcomeSucceeded, TerminalAt: &completed, VisibleCompletedAt: &completed,
			})
		}
		report, err := slo.Evaluate(contract, window, evaluatedAt, observations)
		if err != nil || report.Result != slo.ResultPass {
			t.Fatalf("build observability cohort: report=%#v error=%v", report, err)
		}
		cohorts = append(cohorts, sloevidence.CohortEvidence{
			Contract: contract, Observations: observations, Report: report,
		})
		contractIDs = append(contractIDs, contract.RevisionID)
	}
	apiReport, err := slo.EvaluateAvailability(1_000_000, 1_000_000, 10_000)
	if err != nil || apiReport.Result != slo.ResultPass {
		t.Fatalf("build observability API report: report=%#v error=%v", apiReport, err)
	}
	artifacts := make([]sloevidence.Artifact, 0, 7)
	for _, kind := range []sloevidence.ArtifactKind{
		sloevidence.ArtifactGatewayObservations, sloevidence.ArtifactSaleableSKUSnapshot,
		sloevidence.ArtifactDashboard, sloevidence.ArtifactAlertRules,
		sloevidence.ArtifactRuleTests, sloevidence.ArtifactRunbook,
		sloevidence.ArtifactPageEvents,
	} {
		content := []byte("fixture for " + kind)
		switch kind {
		case sloevidence.ArtifactGatewayObservations:
			content = observabilityGatewayArtifact(t, window, 1_000_000, 1_000_000)
		case sloevidence.ArtifactSaleableSKUSnapshot:
			content = observabilitySaleableSnapshotArtifact(t, evaluatedAt, cohorts)
		}
		ref := filepath.ToSlash(filepath.Join("observability", string(kind)+".json"))
		path := filepath.Join(directory, filepath.FromSlash(ref))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("create observability fixture directory: %v", err)
		}
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("write observability fixture: %v", err)
		}
		artifacts = append(artifacts, sloevidence.Artifact{
			Kind: kind, Ref: ref, Digest: sloevidence.Digest(content),
		})
	}
	fired := evaluatedAt.Add(time.Hour)
	evidence := sloevidence.Evidence{
		SchemaVersion: 1, ReleaseDigest: fixtureDigest("release"),
		ConfigurationRevision: "config-rev-1",
		ValidationEnvironment: "production-validation-rack-1",
		Owner:                 "platform-oncall@example.invalid", Coverage: "24x7",
		Window: window, EvaluatedAt: evaluatedAt,
		SaleableContractRevisionIDs: contractIDs,
		API: sloevidence.APIEvidence{
			EligibleCount: 1_000_000, GoodCount: 1_000_000,
			MinimumSample: 10_000, Report: apiReport,
		},
		Cohorts: cohorts, Artifacts: artifacts,
		Exercise: sloevidence.Exercise{
			AlertFiredAt: fired, AlertDeliveredAt: fired.Add(time.Minute),
			AlertAckedAt: fired.Add(2 * time.Minute), ResolvedAt: fired.Add(3 * time.Minute),
			Result: slo.ResultPass,
		},
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatalf("encode observability evidence fixture: %v", err)
	}
	return encoded
}

func observabilityGatewayArtifact(
	t *testing.T,
	window slo.Window,
	eligible,
	good int,
) []byte {
	t.Helper()
	streams := make([]sloevidence.GatewayObservationStream, 0, 2)
	for _, source := range []sloevidence.GatewayObservationSource{
		sloevidence.GatewaySourceExternalGateway,
		sloevidence.GatewaySourceSyntheticProbe,
	} {
		buckets := make([]sloevidence.GatewayObservationBucket, 0, 31)
		start := window.Start
		for start.Before(window.End) {
			end := start.Add(24 * time.Hour)
			if end.After(window.End) {
				end = window.End
			}
			bucket := sloevidence.GatewayObservationBucket{Start: start, End: end}
			if len(buckets) == 0 && source == sloevidence.GatewaySourceExternalGateway {
				bucket.EligibleCount = eligible
				bucket.GoodCount = good
			}
			buckets = append(buckets, bucket)
			start = end
		}
		streams = append(streams, sloevidence.GatewayObservationStream{
			Source: source, Buckets: buckets,
		})
	}
	encoded, err := json.Marshal(sloevidence.GatewayObservations{
		SchemaVersion: 1, Window: window, Streams: streams,
	})
	if err != nil {
		t.Fatalf("encode gateway observation artifact: %v", err)
	}
	return encoded
}

func observabilitySaleableSnapshotArtifact(
	t *testing.T,
	capturedAt time.Time,
	cohorts []sloevidence.CohortEvidence,
) []byte {
	t.Helper()
	contracts := make([]slo.Contract, 0, len(cohorts))
	for _, cohort := range cohorts {
		contracts = append(contracts, cohort.Contract)
	}
	encoded, err := json.Marshal(sloevidence.SaleableSKUSnapshot{
		SchemaVersion: 1, CapturedAt: capturedAt, Contracts: contracts,
	})
	if err != nil {
		t.Fatalf("encode saleable SKU snapshot artifact: %v", err)
	}
	return encoded
}

func fixtureDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(digest[:])
}
