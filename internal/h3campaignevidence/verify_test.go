package h3campaignevidence

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestVerifyProducesEvidenceForEquivalentSameAndCrossNodeRuns(t *testing.T) {
	input := physicalCampaignInput()

	evidence, err := verify(input)
	if err != nil {
		t.Fatalf("Verify exact H3 campaign: %v", err)
	}
	if evidence.ReleaseDigest != input.ReleaseDigest ||
		evidence.ResidencyPlanRevisionID != input.ResidencyPlanRevisionID ||
		len(evidence.Runs) != 2 || evidence.Runs[0].Kind != RunSameNode ||
		evidence.Runs[1].Kind != RunCrossNode ||
		evidence.Runs[0].FinalOutputDigest != evidence.Runs[1].FinalOutputDigest ||
		len(evidence.CacheRun.Hits) != 2 {
		t.Fatalf("verified campaign evidence = %#v", evidence)
	}
}

func TestVerifyRejectsMismatchedSameAndCrossNodeEquivalenceBindings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*RunSnapshot)
	}{
		{name: "root input", mutate: func(run *RunSnapshot) {
			run.Stages[0].RootInputDigest = digest('2')
		}},
		{name: "stage profile", mutate: func(run *RunSnapshot) {
			run.Stages[1].StageProfileRevisionID = id(201)
		}},
		{name: "stage interface", mutate: func(run *RunSnapshot) {
			run.Stages[2].StageInterfaceRevisionID = id(202)
		}},
		{name: "connector", mutate: func(run *RunSnapshot) {
			run.Transfers[0].ConnectorRevisionID = id(203)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := physicalCampaignInput()
			test.mutate(&input.Runs[1])
			if _, err := verify(input); err == nil {
				t.Fatal("Verify accepted mismatched execution equivalence binding")
			}
		})
	}
}

func TestVerifyRejectsWorkerOutsideReleaseBoundResidencyPlan(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Input)
	}{
		{name: "physical run", mutate: func(input *Input) {
			input.Runs[0].Stages[1].ResidencyPlanRevisionID = id(200)
		}},
		{name: "cache source", mutate: func(input *Input) {
			input.CacheRun.Hits[0].SourceResidencyPlanRevisionID = id(200)
		}},
		{name: "cache target", mutate: func(input *Input) {
			input.CacheRun.PhysicalWorkers[0].ResidencyPlanRevisionID = id(200)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := physicalCampaignInput()
			test.mutate(&input)
			if _, err := verify(input); err == nil {
				t.Fatal("verify accepted Worker outside the release-bound ResidencyPlan")
			}
		})
	}
}

func physicalCampaignInput() Input {
	capturedAt := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	return Input{
		EvidenceBinding: EvidenceBinding{
			ReleaseDigest:           digest('a'),
			ConfigurationRevision:   digest('b'),
			ValidationEnvironment:   "h3-production-cn-north-1",
			CollectorIdentity:       "spiffe://vela/launch-evidence/collector",
			ResidencyPlanRevisionID: id(1),
		},
		CapturedAt: capturedAt,
		InitialDatabaseRead: DatabaseReadProvenance{
			DatabaseTime: capturedAt.Add(-2 * time.Second), BackendPID: "8420",
			SnapshotID: "00000003-0000001A-1",
		},
		FinalDatabaseRead: DatabaseReadProvenance{
			DatabaseTime: capturedAt.Add(-time.Second), BackendPID: "8421",
			SnapshotID: "00000003-0000001B-1",
		},
		Runs: []RunSnapshot{
			physicalRun(RunSameNode, 10, []string{"node-a", "node-a", "node-a"}),
			physicalRun(RunCrossNode, 20, []string{"node-b", "node-c", "node-d"}),
		},
		CacheRun: exactCacheRun(capturedAt),
	}
}

func exactCacheRun(capturedAt time.Time) CacheRunSnapshot {
	organizationID := id(100)
	projectID := id(101)
	targetJobID := id(102)
	return CacheRunSnapshot{
		OrganizationID: organizationID, ProjectID: projectID,
		JobID: targetJobID, AttemptID: id(103), AttemptFence: 1,
		JobState: "SUCCEEDED", AttemptState: "SUCCEEDED", GraphState: "SUCCEEDED",
		ArtifactSetCount: 1, VisibleCompletionCount: 1, ChargeCount: 1,
		Hits: []CacheHitSnapshot{
			exactCacheHit("encoder", 110, organizationID, projectID, targetJobID, capturedAt),
			exactCacheHit("dit", 120, organizationID, projectID, targetJobID, capturedAt),
		},
		PhysicalWorkers: []WorkerPlanSnapshot{{
			StageKey: "vae", WorkerInstanceID: id(104), ResidencyPlanRevisionID: id(1),
		}},
	}
}

func exactCacheHit(
	stageKey string,
	seed byte,
	organizationID, projectID, targetJobID uuid.UUID,
	capturedAt time.Time,
) CacheHitSnapshot {
	return CacheHitSnapshot{
		StageKey: stageKey, EntryID: id(seed), EntryState: "LIVE",
		Scope: "PROJECT", ScopeProjectID: projectID,
		OrganizationID: organizationID, SourceProjectID: projectID,
		SourceJobID: id(seed + 1), SourceStageRunID: id(seed + 2),
		SourceResidencyPlanRevisionID: id(1),
		TargetJobID:                   targetJobID, TargetStageRunID: id(seed + 3),
		StageArtifactID: id(seed + 4), ExactObjectVersion: "cache-version-" + stageKey,
		ArtifactDigest: digest('d'), CacheKeyDigest: digest('e'),
		CachePolicyRevisionID: id(seed + 5), ResultEquivalenceRevisionID: id(seed + 6),
		ReferenceID: id(seed + 7), ReferenceState: "ACTIVE",
		ReferenceAcquiredAt: capturedAt.Add(-time.Minute),
		ExecutionPinID:      id(seed + 8), PinKind: "EXECUTION", PinState: "ACTIVE",
		PinAcquiredAt:           capturedAt.Add(-time.Minute),
		OutputBindingSourceKind: "EXACT_CACHE",
		OutputBindingArtifactID: id(seed + 4),
	}
}

func physicalRun(kind RunKind, base byte, nodes []string) RunSnapshot {
	encoderArtifact := id(base + 4)
	ditArtifact := id(base + 5)
	vaeArtifact := id(base + 6)
	stages := []StageSnapshot{
		physicalStage("encoder", base, nodes[0], encoderArtifact, nil, digest('1')),
		physicalStage("dit", base+1, nodes[1], ditArtifact, []uuid.UUID{encoderArtifact}, ""),
		physicalStage("vae", base+2, nodes[2], vaeArtifact, []uuid.UUID{ditArtifact}, ""),
	}
	stages[2].OutputDigest = digest('f')
	return RunSnapshot{
		Kind: kind, JobID: id(base + 30), AttemptID: id(base + 31),
		AttemptFence: 1, ExecutionGraphRevisionID: id(90),
		JobState: "SUCCEEDED", AttemptState: "SUCCEEDED", GraphState: "SUCCEEDED",
		Stages: stages,
		Transfers: []TransferSnapshot{
			{ID: id(base + 40), SourceStageRunID: stages[0].StageRunID,
				DestinationStageRunID: stages[1].StageRunID, StageArtifactID: encoderArtifact,
				DestinationWorkerInstanceID:    stages[1].WorkerInstanceID,
				DestinationWorkerInstanceEpoch: stages[1].WorkerInstanceEpoch,
				ConnectorRevisionID:            id(80), State: "CONSUMED"},
			{ID: id(base + 41), SourceStageRunID: stages[1].StageRunID,
				DestinationStageRunID: stages[2].StageRunID, StageArtifactID: ditArtifact,
				DestinationWorkerInstanceID:    stages[2].WorkerInstanceID,
				DestinationWorkerInstanceEpoch: stages[2].WorkerInstanceEpoch,
				ConnectorRevisionID:            id(81), State: "CONSUMED"},
		},
		VisibleCompletionCount: 1, ChargeCount: 1, ArtifactSetCount: 1,
	}
}

func physicalStage(
	key string,
	seed byte,
	node string,
	artifactID uuid.UUID,
	inputs []uuid.UUID,
	rootDigest string,
) StageSnapshot {
	var profileID, interfaceID uuid.UUID
	switch key {
	case "encoder":
		profileID, interfaceID = id(210), id(220)
	case "dit":
		profileID, interfaceID = id(211), id(221)
	case "vae":
		profileID, interfaceID = id(212), id(222)
	}
	return StageSnapshot{
		StageKey: key, State: "SUCCEEDED", SourceKind: "PHYSICAL",
		StageRunID: id(seed + 50), StageAttemptID: id(seed + 51),
		StageProfileRevisionID: profileID, ArtifactID: artifactID,
		ObjectVersion: "version-" + key, OutputDigest: digest('c'),
		StageInterfaceRevisionID: interfaceID, InputStageArtifactIDs: inputs,
		RootInputDigest: rootDigest, NodeIdentity: node,
		WorkerInstanceID: id(seed + 54), WorkerInstanceEpoch: 7,
		ResidencyPlanRevisionID: id(1),
		WorkerMemberID:          id(seed + 55), MemberEpoch: 3,
		DeviceID: id(seed + 56), DeviceEpoch: 5,
		ModelResidencyID: id(seed + 57), ModelRuntimeEpoch: 11,
	}
}

func id(value byte) uuid.UUID {
	var result uuid.UUID
	result[15] = value
	return result
}

func digest(character byte) string {
	value := make([]byte, 64)
	for index := range value {
		value[index] = character
	}
	return "sha256:" + string(value)
}
