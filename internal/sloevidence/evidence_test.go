package sloevidence

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/slo"
)

func TestEvidenceRequiresRecomputedThreePresetAndAPIResults(t *testing.T) {
	evidence := validEvidence(t)
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatalf("marshal evidence: %v", err)
	}
	decoded, err := Decode(encoded, evidence.ReleaseDigest, evidence.ConfigurationRevision)
	if err != nil || len(decoded.Cohorts) != 3 {
		t.Fatalf("decode valid evidence: cohorts=%d error=%v", len(decoded.Cohorts), err)
	}
	gateway := gatewayArtifact(t, evidence)
	if err := decoded.ValidateArtifact(ArtifactGatewayObservations, gateway); err != nil {
		t.Fatalf("validate gateway observations: %v", err)
	}
	snapshot := saleableSnapshotArtifact(t, evidence)
	if err := decoded.ValidateArtifact(ArtifactSaleableSKUSnapshot, snapshot); err != nil {
		t.Fatalf("validate saleable SKU snapshot: %v", err)
	}

	tampered := evidence
	tampered.Cohorts = append([]CohortEvidence(nil), evidence.Cohorts...)
	tampered.Cohorts[0].Report.SuccessObservedPPM--
	encoded, _ = json.Marshal(tampered)
	if _, err := Decode(encoded, evidence.ReleaseDigest, evidence.ConfigurationRevision); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("tampered report error = %v", err)
	}
}

func TestEvidenceRejectsArtifactRecomputationMismatch(t *testing.T) {
	evidence := validEvidence(t)
	gateway := gatewayObservations(evidence)
	gateway.Streams[0].Buckets[0].EligibleCount--
	encoded, err := json.Marshal(gateway)
	if err != nil {
		t.Fatalf("marshal tampered gateway observations: %v", err)
	}
	if err := evidence.ValidateArtifact(ArtifactGatewayObservations, encoded); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("tampered gateway observation error = %v", err)
	}

	snapshot := saleableSnapshot(evidence)
	snapshot.Contracts[0].SuccessTargetPPM--
	encoded, err = json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal tampered saleable snapshot: %v", err)
	}
	if err := evidence.ValidateArtifact(ArtifactSaleableSKUSnapshot, encoded); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("tampered saleable snapshot error = %v", err)
	}
}

func TestEvidenceRejectsAmbiguousOrUnknownArtifactFields(t *testing.T) {
	evidence := validEvidence(t)
	gateway := gatewayArtifact(t, evidence)
	duplicate := []byte(strings.Replace(
		string(gateway),
		`"schema_version":1`,
		`"schema_version":1,"schema_version":1`,
		1,
	))
	if err := evidence.ValidateArtifact(ArtifactGatewayObservations, duplicate); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("duplicate gateway artifact key error = %v", err)
	}
	snapshot := saleableSnapshotArtifact(t, evidence)
	unknown := []byte(strings.Replace(
		string(snapshot),
		`"schema_version":1`,
		`"schema_version":1,"waiver":"none"`,
		1,
	))
	if err := evidence.ValidateArtifact(ArtifactSaleableSKUSnapshot, unknown); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("unknown saleable snapshot field error = %v", err)
	}
}

func TestEvidenceRejectsDuplicateKeysAndMissingArtifacts(t *testing.T) {
	evidence := validEvidence(t)
	encoded, _ := json.Marshal(evidence)
	duplicate := append([]byte(`{"schema_version":1,"schema_version":1,`), encoded[1:]...)
	if _, err := Decode(duplicate, evidence.ReleaseDigest, evidence.ConfigurationRevision); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("duplicate key error = %v", err)
	}
	evidence.Artifacts = evidence.Artifacts[:len(evidence.Artifacts)-1]
	encoded, _ = json.Marshal(evidence)
	if _, err := Decode(encoded, evidence.ReleaseDigest, evidence.ConfigurationRevision); !errors.Is(err, ErrInvalidEvidence) {
		t.Fatalf("missing artifact error = %v", err)
	}
}

func validEvidence(t *testing.T) Evidence {
	t.Helper()
	window := slo.Window{
		Start: time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC),
	}
	evaluatedAt := window.End.Add(time.Hour)
	cohorts := make([]CohortEvidence, 0, 3)
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
				Outcome:    slo.OutcomeSucceeded,
				TerminalAt: &completed, VisibleCompletedAt: &completed,
			})
		}
		report, err := slo.Evaluate(contract, window, evaluatedAt, observations)
		if err != nil || report.Result != slo.ResultPass {
			t.Fatalf("build %s report: %#v error=%v", preset, report, err)
		}
		cohorts = append(cohorts, CohortEvidence{Contract: contract, Observations: observations, Report: report})
		contractIDs = append(contractIDs, contract.RevisionID)
	}
	apiReport, err := slo.EvaluateAvailability(1_000_000, 1_000_000, 10_000)
	if err != nil || apiReport.Result != slo.ResultPass {
		t.Fatalf("build API report: %#v error=%v", apiReport, err)
	}
	artifacts := make([]Artifact, 0, 7)
	for _, kind := range []ArtifactKind{
		ArtifactGatewayObservations, ArtifactSaleableSKUSnapshot, ArtifactDashboard,
		ArtifactAlertRules, ArtifactRuleTests, ArtifactRunbook, ArtifactPageEvents,
	} {
		content := []byte(kind)
		switch kind {
		case ArtifactGatewayObservations:
			content = gatewayArtifact(t, Evidence{
				Window: window,
				API:    APIEvidence{EligibleCount: 1_000_000, GoodCount: 1_000_000},
			})
		case ArtifactSaleableSKUSnapshot:
			content = saleableSnapshotArtifact(t, Evidence{
				Window: window, EvaluatedAt: evaluatedAt, Cohorts: cohorts,
			})
		}
		artifacts = append(artifacts, Artifact{
			Kind: kind, Ref: string(kind) + ".json",
			Digest: Digest(content),
		})
	}
	fired := evaluatedAt.Add(time.Hour)
	delivered := fired.Add(time.Minute)
	acked := delivered.Add(time.Minute)
	resolved := acked.Add(time.Minute)
	return Evidence{
		SchemaVersion:         1,
		ReleaseDigest:         Digest([]byte("release")),
		ConfigurationRevision: "config-v1",
		ValidationEnvironment: "real-h3-production",
		Owner:                 "platform-oncall@example.invalid", Coverage: "24x7",
		Window: window, EvaluatedAt: evaluatedAt,
		SaleableContractRevisionIDs: contractIDs,
		API:                         APIEvidence{EligibleCount: 1_000_000, GoodCount: 1_000_000, MinimumSample: 10_000, Report: apiReport},
		Cohorts:                     cohorts, Artifacts: artifacts,
		Exercise: Exercise{
			AlertFiredAt: fired, AlertDeliveredAt: delivered,
			AlertAckedAt: acked, ResolvedAt: resolved, Result: slo.ResultPass,
		},
	}
}

func gatewayArtifact(t *testing.T, evidence Evidence) []byte {
	t.Helper()
	encoded, err := json.Marshal(gatewayObservations(evidence))
	if err != nil {
		t.Fatalf("marshal gateway observations: %v", err)
	}
	return encoded
}

func gatewayObservations(evidence Evidence) GatewayObservations {
	streams := make([]GatewayObservationStream, 0, 2)
	for _, source := range []GatewayObservationSource{
		GatewaySourceExternalGateway,
		GatewaySourceSyntheticProbe,
	} {
		buckets := make([]GatewayObservationBucket, 0, 31)
		start := evidence.Window.Start
		for start.Before(evidence.Window.End) {
			end := start.Add(24 * time.Hour)
			if end.After(evidence.Window.End) {
				end = evidence.Window.End
			}
			bucket := GatewayObservationBucket{Start: start, End: end}
			if len(buckets) == 0 && source == GatewaySourceExternalGateway {
				bucket.EligibleCount = evidence.API.EligibleCount
				bucket.GoodCount = evidence.API.GoodCount
			}
			buckets = append(buckets, bucket)
			start = end
		}
		streams = append(streams, GatewayObservationStream{Source: source, Buckets: buckets})
	}
	return GatewayObservations{SchemaVersion: 1, Window: evidence.Window, Streams: streams}
}

func saleableSnapshotArtifact(t *testing.T, evidence Evidence) []byte {
	t.Helper()
	encoded, err := json.Marshal(saleableSnapshot(evidence))
	if err != nil {
		t.Fatalf("marshal saleable SKU snapshot: %v", err)
	}
	return encoded
}

func saleableSnapshot(evidence Evidence) SaleableSKUSnapshot {
	contracts := make([]slo.Contract, 0, len(evidence.Cohorts))
	for _, cohort := range evidence.Cohorts {
		contracts = append(contracts, cohort.Contract)
	}
	return SaleableSKUSnapshot{
		SchemaVersion: 1,
		CapturedAt:    evidence.EvaluatedAt,
		Contracts:     contracts,
	}
}
