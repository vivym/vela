package productiongates

import (
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
	manifest := Manifest{SchemaVersion: 1, Receipts: make([]Receipt, 0, len(AllGates()))}
	for index, gate := range AllGates() {
		gateEvidence := evidence
		if gate == GateObservabilityOnCall {
			gateEvidence = observabilityEvidenceFixture(t, directory)
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
			EvidenceDigest:        "sha256:" + hex.EncodeToString(gateEvidenceDigest[:]),
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
