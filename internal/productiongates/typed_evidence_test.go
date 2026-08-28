package productiongates

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTypedEvidenceContractsCoverEveryNonObservabilityGate(t *testing.T) {
	for _, gate := range AllGates() {
		_, present := TypedEvidenceContractForGate(gate)
		if gate == GateObservabilityOnCall {
			if present {
				t.Fatalf("observability gate unexpectedly has a typed evidence contract")
			}
			continue
		}
		if !present {
			t.Fatalf("typed evidence contract missing for %s", gate)
		}
		startedAt := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
		completedAt := startedAt.Add(time.Minute)
		if gate == GateRealH3Soak {
			completedAt = startedAt.Add(72 * time.Hour)
		}
		evidence := passingTypedEvidenceFixture(t, t.TempDir(), gate, startedAt, completedAt)
		receipt := receiptForTypedEvidence(evidence)
		if err := evidence.Validate(receipt); err != nil {
			t.Fatalf("validate %s evidence: %v", gate, err)
		}
	}
}

func TestTypedEvidenceRejectsIncompleteOrContradictorySemantics(t *testing.T) {
	tests := []struct {
		name   string
		gate   Gate
		mutate func(*TypedEvidence, *Receipt)
	}{
		{
			name: "criteria revision", gate: GateDataDisasterRecovery,
			mutate: func(evidence *TypedEvidence, _ *Receipt) { evidence.CriteriaRevision += "/waived" },
		},
		{
			name: "environment binding", gate: GateOrganizationIsolation,
			mutate: func(evidence *TypedEvidence, _ *Receipt) { evidence.ValidationEnvironment = "other-rack" },
		},
		{
			name: "receipt time binding", gate: GateReleaseRollback,
			mutate: func(_ *TypedEvidence, receipt *Receipt) { receipt.CompletedAt = receipt.CompletedAt.Add(time.Second) },
		},
		{
			name: "missing check", gate: GateStateEventFaultInjection,
			mutate: func(evidence *TypedEvidence, _ *Receipt) { evidence.Checks = evidence.Checks[1:] },
		},
		{
			name: "failed check", gate: GateGPURemediation,
			mutate: func(evidence *TypedEvidence, _ *Receipt) { evidence.Checks[0].Passed = false },
		},
		{
			name: "duplicate check", gate: GateCommercialLifecycle,
			mutate: func(evidence *TypedEvidence, _ *Receipt) { evidence.Checks[1].ID = evidence.Checks[0].ID },
		},
		{
			name: "measurement threshold drift", gate: GateDataDisasterRecovery,
			mutate: func(evidence *TypedEvidence, _ *Receipt) { evidence.Measurements[1].Threshold++ },
		},
		{
			name: "measurement comparator drift", gate: GateRealH3Soak,
			mutate: func(evidence *TypedEvidence, _ *Receipt) { evidence.Measurements[0].Comparator = EvidenceEqual },
		},
		{
			name: "measurement fails", gate: GateReleaseRollback,
			mutate: func(evidence *TypedEvidence, _ *Receipt) { evidence.Measurements[0].Observed = 0 },
		},
		{
			name: "duplicate measurement", gate: GateOrganizationIsolation,
			mutate: func(evidence *TypedEvidence, _ *Receipt) { evidence.Measurements[1].ID = evidence.Measurements[0].ID },
		},
		{
			name: "artifact path escape", gate: GateCommercialLifecycle,
			mutate: func(evidence *TypedEvidence, _ *Receipt) { evidence.Artifacts[0].Ref = "../outside.json" },
		},
		{
			name: "duplicate artifact reference", gate: GateGPURemediation,
			mutate: func(evidence *TypedEvidence, _ *Receipt) { evidence.Artifacts[1].Ref = evidence.Artifacts[0].Ref },
		},
		{
			name: "missing preset claims", gate: GatePresetCertification,
			mutate: func(evidence *TypedEvidence, _ *Receipt) { evidence.PresetCertification = nil },
		},
		{
			name: "preset threshold failure", gate: GatePresetCertification,
			mutate: func(evidence *TypedEvidence, _ *Receipt) {
				evidence.PresetCertification.Certifications[0].QualityObservedPPM = 1
			},
		},
		{
			name: "preset group incomplete", gate: GatePresetCertification,
			mutate: func(evidence *TypedEvidence, _ *Receipt) {
				evidence.PresetCertification.Certifications = evidence.PresetCertification.Certifications[:2]
			},
		},
		{
			name: "saleable group snapshot incomplete", gate: GatePresetCertification,
			mutate: func(evidence *TypedEvidence, _ *Receipt) {
				evidence.PresetCertification.SaleableGroupIDs = append(
					evidence.PresetCertification.SaleableGroupIDs,
					"model-v2/standard-v1/1080p-v1/CNY",
				)
			},
		},
		{
			name: "preset saleable group aggregate", gate: GatePresetCertification,
			mutate: func(evidence *TypedEvidence, _ *Receipt) {
				evidence.Measurements[0].Observed++
			},
		},
		{
			name: "RateCard binding missing", gate: GatePresetCertification,
			mutate: func(evidence *TypedEvidence, _ *Receipt) {
				evidence.PresetCertification.RateCards[0].BindingID = ""
			},
		},
		{
			name: "soak window mismatch", gate: GateRealH3Soak,
			mutate: func(evidence *TypedEvidence, receipt *Receipt) {
				evidence.CompletedAt = evidence.CompletedAt.Add(time.Second)
				receipt.CompletedAt = evidence.CompletedAt
				receipt.RecordedAt = receipt.CompletedAt.Add(time.Minute)
			},
		},
		{
			name: "free form receipt summary", gate: GateDataDisasterRecovery,
			mutate: func(_ *TypedEvidence, receipt *Receipt) { receipt.ObservedResult = "looks good" },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			startedAt := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
			completedAt := startedAt.Add(time.Minute)
			if test.gate == GateRealH3Soak {
				completedAt = startedAt.Add(72 * time.Hour)
			}
			evidence := passingTypedEvidenceFixture(t, t.TempDir(), test.gate, startedAt, completedAt)
			receipt := receiptForTypedEvidence(evidence)
			test.mutate(&evidence, &receipt)
			if err := evidence.Validate(receipt); !errors.Is(err, ErrInvalidTypedEvidence) {
				t.Fatalf("Validate error = %v, want invalid typed evidence", err)
			}
		})
	}
}

func TestTypedEvidenceArtifactsReproduceEnvelopeSemantics(t *testing.T) {
	directory := t.TempDir()
	startedAt := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	evidence := passingTypedEvidenceFixture(
		t,
		directory,
		GatePresetCertification,
		startedAt,
		startedAt.Add(time.Minute),
	)
	artifacts := make(map[string]TypedEvidenceArtifact, len(evidence.Artifacts))
	for _, reference := range evidence.Artifacts {
		encoded, err := os.ReadFile(filepath.Join(directory, filepath.FromSlash(reference.Ref)))
		if err != nil {
			t.Fatalf("read %s artifact: %v", reference.Kind, err)
		}
		artifact, err := DecodeTypedEvidenceArtifact(encoded, evidence, reference)
		if err != nil {
			t.Fatalf("decode %s artifact: %v", reference.Kind, err)
		}
		artifacts[reference.Kind] = artifact
	}
	if err := ValidateTypedEvidenceArtifacts(evidence, artifacts); err != nil {
		t.Fatalf("validate complete typed artifact set: %v", err)
	}

	for kind, artifact := range artifacts {
		if len(artifact.Checks) == 0 {
			continue
		}
		artifact.Checks = artifact.Checks[1:]
		artifacts[kind] = artifact
		if err := ValidateTypedEvidenceArtifacts(evidence, artifacts); !errors.Is(err, ErrInvalidTypedEvidence) {
			t.Fatalf("incomplete artifact aggregate error = %v", err)
		}
		return
	}
	t.Fatal("fixture has no artifact checks to remove")
}

func TestDecodeTypedEvidenceRejectsAmbiguousOrUnboundedJSON(t *testing.T) {
	startedAt := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	evidence := passingTypedEvidenceFixture(
		t,
		t.TempDir(),
		GateDataDisasterRecovery,
		startedAt,
		startedAt.Add(time.Minute),
	)
	receipt := receiptForTypedEvidence(evidence)
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatalf("marshal typed evidence: %v", err)
	}
	for _, test := range []struct {
		name    string
		encoded []byte
	}{
		{name: "opaque", encoded: []byte(`{"result":"PASS"}`)},
		{name: "unknown field", encoded: []byte(strings.TrimSuffix(string(encoded), "}") + `,"waiver":true}`)},
		{name: "duplicate key", encoded: []byte(strings.Replace(string(encoded), `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1))},
		{name: "trailing document", encoded: append(append([]byte(nil), encoded...), []byte(`{}`)...)},
		{name: "empty", encoded: nil},
		{name: "oversized", encoded: bytes.Repeat([]byte("x"), MaxTypedEvidenceBytes+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeTypedEvidence(test.encoded, receipt); !errors.Is(err, ErrInvalidTypedEvidence) {
				t.Fatalf("DecodeTypedEvidence error = %v, want invalid typed evidence", err)
			}
		})
	}
}

func receiptForTypedEvidence(evidence TypedEvidence) Receipt {
	return Receipt{
		SchemaVersion: 1, Gate: evidence.Gate,
		ReleaseDigest: evidence.ReleaseDigest, ConfigurationRevision: evidence.ConfigurationRevision,
		ValidationEnvironment: evidence.ValidationEnvironment, Result: ResultPass,
		Owner: evidence.Owner, AcceptanceThreshold: evidence.AcceptanceThreshold(),
		ObservedResult: evidence.ObservedResult(), EvidenceRef: "evidence.json",
		EvidenceDigest: digest("typed-evidence"), StartedAt: evidence.StartedAt,
		CompletedAt: evidence.CompletedAt, RecordedAt: evidence.CompletedAt.Add(time.Minute),
	}
}
