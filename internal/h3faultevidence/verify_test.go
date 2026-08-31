package h3faultevidence

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

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/productiongates"
)

func TestLoadAndBuildBundleRequiresAllFaultAndStaleAuthorityEvidence(t *testing.T) {
	path := writeCampaignFixture(t, nil)
	campaign, err := Load(path)
	if err != nil {
		t.Fatalf("load H3 fault campaign: %v", err)
	}
	if len(campaign.Receipts) != len(AllScenarios()) {
		t.Fatalf("campaign receipt count = %d", len(campaign.Receipts))
	}
	stale := campaign.Receipts[ScenarioStaleFenceLateCompletion]
	if len(stale.StaleAuthorityProbes) != len(AllStaleAuthorityKinds()) {
		t.Fatalf("stale-authority probe count = %d", len(stale.StaleAuthorityProbes))
	}

	bundle, err := campaign.BuildBundle()
	if err != nil {
		t.Fatalf("build H3 fault campaign bundle: %v", err)
	}
	if len(bundle.Artifacts) != 3 || len(bundle.ArtifactBytes) != 3 {
		t.Fatalf("fault campaign artifacts = %d/%d", len(bundle.Artifacts), len(bundle.ArtifactBytes))
	}
	receipt := productiongates.Receipt{
		SchemaVersion: 1, Gate: productiongates.GateStateEventFaultInjection,
		ReleaseDigest:         bundle.Evidence.ReleaseDigest,
		ConfigurationRevision: bundle.Evidence.ConfigurationRevision,
		ValidationEnvironment: bundle.Evidence.ValidationEnvironment,
		Result:                productiongates.ResultPass, Owner: bundle.Evidence.Owner,
		AcceptanceThreshold: bundle.Evidence.AcceptanceThreshold(),
		ObservedResult:      bundle.Evidence.ObservedResult(),
		EvidenceRef:         "state-event-fault-injection.json",
		EvidenceDigest:      digestBytes(bundle.EvidenceBytes),
		StartedAt:           bundle.Evidence.StartedAt, CompletedAt: bundle.Evidence.CompletedAt,
		RecordedAt: bundle.Evidence.CompletedAt.Add(time.Second),
	}
	decoded, err := productiongates.DecodeTypedEvidence(bundle.EvidenceBytes, receipt)
	if err != nil {
		t.Fatalf("decode assembled typed evidence: %v", err)
	}
	verifiedArtifacts := make(map[string]productiongates.TypedEvidenceArtifact, 3)
	for _, reference := range decoded.Artifacts {
		encoded := bundle.ArtifactBytes[reference.Kind]
		if digestBytes(encoded) != reference.Digest {
			t.Fatalf("artifact %s digest mismatch", reference.Kind)
		}
		artifact, decodeErr := productiongates.DecodeTypedEvidenceArtifact(
			encoded, decoded, reference,
		)
		if decodeErr != nil {
			t.Fatalf("decode assembled artifact %s: %v", reference.Kind, decodeErr)
		}
		verifiedArtifacts[reference.Kind] = artifact
	}
	if err := productiongates.ValidateTypedEvidenceArtifacts(decoded, verifiedArtifacts); err != nil {
		t.Fatalf("validate assembled artifact union: %v", err)
	}
}

func TestFaultCampaignRejectsIncompleteOrContradictoryReceipts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Manifest, map[Scenario]*ScenarioReceipt)
	}{
		{
			name: "missing scenario",
			mutate: func(manifest *Manifest, _ map[Scenario]*ScenarioReceipt) {
				manifest.Scenarios = manifest.Scenarios[1:]
			},
		},
		{
			name: "lost accepted Job",
			mutate: func(_ *Manifest, receipts map[Scenario]*ScenarioReceipt) {
				receipts[ScenarioProcessKill].Measurements.LostAcceptedJobCount = 1
			},
		},
		{
			name: "missing stale lease probe",
			mutate: func(_ *Manifest, receipts map[Scenario]*ScenarioReceipt) {
				probes := receipts[ScenarioStaleFenceLateCompletion].StaleAuthorityProbes
				receipts[ScenarioStaleFenceLateCompletion].StaleAuthorityProbes = probes[:3]
			},
		},
		{
			name: "accepted stale probe",
			mutate: func(_ *Manifest, receipts map[Scenario]*ScenarioReceipt) {
				receipts[ScenarioStaleFenceLateCompletion].StaleAuthorityProbes[0].Decision = "ACCEPTED"
			},
		},
		{
			name: "duplicate event identity",
			mutate: func(_ *Manifest, receipts map[Scenario]*ScenarioReceipt) {
				events := receipts[ScenarioNodeReboot].RawEvents
				receipts[ScenarioNodeReboot].RawEvents = append(events, events[0])
			},
		},
		{
			name: "unreconciled completion ledger",
			mutate: func(_ *Manifest, receipts map[Scenario]*ScenarioReceipt) {
				receipts[ScenarioConsumerPostDBPreAckCrash].AuthorityAfter.VisibleCompletionCount = 0
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeCampaignFixture(t, test.mutate)
			if _, err := Load(path); !errors.Is(err, ErrInvalidCampaign) {
				t.Fatalf("Load error = %v, want ErrInvalidCampaign", err)
			}
		})
	}
}

func TestFaultCampaignRejectsTamperPathEscapeAndAmbiguousJSON(t *testing.T) {
	t.Run("tamper", func(t *testing.T) {
		path := writeCampaignFixture(t, nil)
		directory := filepath.Dir(path)
		receiptPath := filepath.Join(directory, string(ScenarioProcessKill)+".json")
		if err := os.WriteFile(receiptPath, []byte("{}\n"), 0o600); err != nil {
			t.Fatalf("tamper receipt: %v", err)
		}
		if _, err := Load(path); !errors.Is(err, ErrInvalidCampaign) {
			t.Fatalf("tampered Load error = %v", err)
		}
	})
	t.Run("path escape", func(t *testing.T) {
		path := writeCampaignFixture(t, func(manifest *Manifest, _ map[Scenario]*ScenarioReceipt) {
			manifest.Scenarios[0].Ref = "../outside.json"
		})
		if _, err := Load(path); !errors.Is(err, ErrInvalidCampaign) {
			t.Fatalf("path escape Load error = %v", err)
		}
	})
	t.Run("duplicate key", func(t *testing.T) {
		path := writeCampaignFixture(t, nil)
		encoded, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read manifest: %v", err)
		}
		ambiguous := strings.Replace(
			string(encoded), `"schema_version": 1`, `"schema_version": 1, "schema_version": 1`, 1,
		)
		if err := os.WriteFile(path, []byte(ambiguous), 0o600); err != nil {
			t.Fatalf("write ambiguous manifest: %v", err)
		}
		if _, err := Load(path); !errors.Is(err, ErrInvalidCampaign) {
			t.Fatalf("ambiguous Load error = %v", err)
		}
	})
}

func writeCampaignFixture(
	t *testing.T,
	mutate func(*Manifest, map[Scenario]*ScenarioReceipt),
) string {
	t.Helper()
	directory := t.TempDir()
	started := time.Date(2026, 9, 1, 1, 0, 0, 0, time.UTC)
	manifest := Manifest{
		SchemaVersion: 1, ReleaseDigest: digest('a'),
		ConfigurationRevision: "h3-fault-campaign-r1",
		ValidationEnvironment: "repository-conformance",
		Owner:                 "platform-resilience/test", StartedAt: started,
		CompletedAt: started.Add(30 * time.Minute),
	}
	receipts := make(map[Scenario]*ScenarioReceipt, len(AllScenarios()))
	for index, scenario := range AllScenarios() {
		jobID := uuid.New()
		scenarioStarted := started.Add(time.Duration(index) * time.Minute)
		receipt := &ScenarioReceipt{
			SchemaVersion: 1, Scenario: scenario,
			ReleaseDigest:         manifest.ReleaseDigest,
			ConfigurationRevision: manifest.ConfigurationRevision,
			ValidationEnvironment: manifest.ValidationEnvironment, Owner: manifest.Owner,
			StartedAt: scenarioStarted, CompletedAt: scenarioStarted.Add(30 * time.Second),
			ExerciseID: uuid.New(), ControllerIdentity: "spiffe://vela/test/fault-controller",
			Target:         FaultTarget{Kind: "PROCESS", ID: "target-" + string(scenario)},
			AcceptedJobIDs: []uuid.UUID{jobID},
			AuthorityBefore: AuthorityObservation{
				CapturedAt: scenarioStarted.Add(time.Second), DatabaseSnapshotID: "10:20:",
				JobLedgerDigest: digest('b'), CompletionLedgerDigest: digest('c'),
				ChargeLedgerDigest: digest('d'), AcceptedJobCount: 1,
			},
			AuthorityAfter: AuthorityObservation{
				CapturedAt: scenarioStarted.Add(29 * time.Second), DatabaseSnapshotID: "20:30:",
				JobLedgerDigest: digest('e'), CompletionLedgerDigest: digest('f'),
				ChargeLedgerDigest: digest('1'), AcceptedJobCount: 1,
				VisibleCompletionCount: 1, ChargeCount: 1,
			},
			RawEvents: []RawEventObservation{{
				EventID: uuid.New(), AggregateType: "Job", AggregateID: jobID,
				AggregateVersion: 1, EventType: "job.succeeded", PayloadDigest: digest('2'),
				PublishedCount: 1, ConsumedCount: 1,
			}},
		}
		if scenario == ScenarioStaleFenceLateCompletion {
			for _, kind := range AllStaleAuthorityKinds() {
				receipt.StaleAuthorityProbes = append(receipt.StaleAuthorityProbes, StaleAuthorityProbe{
					ID: uuid.New(), Kind: kind, JobID: jobID, StageRunID: uuid.New(),
					WorkerInstanceID: uuid.New(), PresentedAuthorityDigest: digest('3'),
					CurrentAuthorityDigest: digest('4'), Decision: "REJECTED",
					ReasonCode: staleReason(kind), RejectedAt: scenarioStarted.Add(15 * time.Second),
				})
			}
		}
		receipts[scenario] = receipt
		manifest.Scenarios = append(manifest.Scenarios, ScenarioReference{
			Scenario: scenario, Ref: string(scenario) + ".json", Digest: digest('0'),
		})
	}
	if mutate != nil {
		mutate(&manifest, receipts)
	}
	for _, scenario := range AllScenarios() {
		receipt, present := receipts[scenario]
		if !present {
			continue
		}
		encoded, err := json.MarshalIndent(receipt, "", "  ")
		if err != nil {
			t.Fatalf("encode %s receipt: %v", scenario, err)
		}
		encoded = append(encoded, '\n')
		name := string(scenario) + ".json"
		if err := os.WriteFile(filepath.Join(directory, name), encoded, 0o600); err != nil {
			t.Fatalf("write %s receipt: %v", scenario, err)
		}
		for index := range manifest.Scenarios {
			if manifest.Scenarios[index].Scenario == scenario {
				manifest.Scenarios[index].Digest = digestBytes(encoded)
			}
		}
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	path := filepath.Join(directory, "manifest.json")
	if err := os.WriteFile(path, append(encoded, '\n'), 0o600); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return path
}

func staleReason(kind StaleAuthorityKind) string {
	switch kind {
	case StaleMemberEpoch:
		return "member-epoch-stale"
	case StaleDeviceEpoch:
		return "device-epoch-stale"
	case StaleModelRuntimeEpoch:
		return "model-runtime-epoch-stale"
	case StaleStageLease:
		return "stage-lease-stale"
	default:
		return ""
	}
}

func digest(character byte) string {
	return "sha256:" + strings.Repeat(string(character), 64)
}

func digestBytes(encoded []byte) string {
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}
