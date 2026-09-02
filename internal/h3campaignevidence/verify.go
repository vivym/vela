package h3campaignevidence

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidCampaignEvidence = errors.New("invalid H3 campaign evidence")
	sha256Pattern              = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

type runEquivalenceBinding struct {
	rootInputDigest           string
	stageProfileRevisionIDs   [3]uuid.UUID
	stageInterfaceRevisionIDs [3]uuid.UUID
	connectorRevisionIDs      [2]uuid.UUID
	finalOutputDigest         string
}

func verify(input Input) (Evidence, error) {
	if err := validateEnvelope(input); err != nil {
		return Evidence{}, err
	}
	if len(input.Runs) != 2 {
		return Evidence{}, invalid("physical conformance requires exactly same-node and cross-node runs")
	}
	byKind := make(map[RunKind]RunSnapshot, len(input.Runs))
	for _, run := range input.Runs {
		if _, duplicate := byKind[run.Kind]; duplicate {
			return Evidence{}, invalid("duplicate run kind %q", run.Kind)
		}
		byKind[run.Kind] = run
	}
	same, sameOK := byKind[RunSameNode]
	cross, crossOK := byKind[RunCrossNode]
	if !sameOK || !crossOK || same.ExecutionGraphRevisionID != cross.ExecutionGraphRevisionID {
		return Evidence{}, invalid("same-node and cross-node runs do not use one exact graph revision")
	}
	sameEvidence, sameBinding, err := verifyPhysicalRun(same, input.ResidencyPlanRevisionID)
	if err != nil {
		return Evidence{}, err
	}
	crossEvidence, crossBinding, err := verifyPhysicalRun(cross, input.ResidencyPlanRevisionID)
	if err != nil {
		return Evidence{}, err
	}
	if sameBinding != crossBinding {
		return Evidence{}, invalid("same-node and cross-node execution equivalence bindings differ")
	}
	cacheEvidence, err := verifyCacheRun(
		input.CacheRun, input.CapturedAt, input.ResidencyPlanRevisionID,
	)
	if err != nil {
		return Evidence{}, err
	}
	return Evidence{
		SchemaVersion: SchemaVersion, MediaType: MediaType,
		ReleaseDigest: input.ReleaseDigest, ConfigurationRevision: input.ConfigurationRevision,
		ValidationEnvironment: input.ValidationEnvironment, CollectorIdentity: input.CollectorIdentity,
		CapturedAt: input.CapturedAt, ResidencyPlanRevisionID: input.ResidencyPlanRevisionID,
		InitialDatabaseRead: input.InitialDatabaseRead,
		FinalDatabaseRead:   input.FinalDatabaseRead,
		Runs:                []RunEvidence{sameEvidence, crossEvidence}, CacheRun: cacheEvidence,
	}, nil
}

func verifyCacheRun(
	run CacheRunSnapshot,
	capturedAt time.Time,
	residencyPlanRevisionID uuid.UUID,
) (CacheRunEvidence, error) {
	if run.OrganizationID == uuid.Nil || run.ProjectID == uuid.Nil || run.JobID == uuid.Nil ||
		run.AttemptID == uuid.Nil || run.AttemptFence <= 0 || run.JobState != "SUCCEEDED" ||
		run.AttemptState != "SUCCEEDED" || run.GraphState != "SUCCEEDED" ||
		run.ArtifactSetCount != 1 || run.VisibleCompletionCount != 1 || run.ChargeCount != 1 ||
		len(run.Hits) != 2 || len(run.PhysicalWorkers) != 1 ||
		run.PhysicalWorkers[0].StageKey != "vae" ||
		run.PhysicalWorkers[0].WorkerInstanceID == uuid.Nil ||
		run.PhysicalWorkers[0].ResidencyPlanRevisionID != residencyPlanRevisionID {
		return CacheRunEvidence{}, invalid("cache run is not exactly-once successful with two exact hits")
	}
	byStage := make(map[string]CacheHitSnapshot, 2)
	seenEntries := make(map[uuid.UUID]struct{}, 2)
	seenArtifacts := make(map[uuid.UUID]struct{}, 2)
	for _, hit := range run.Hits {
		if _, duplicate := byStage[hit.StageKey]; duplicate {
			return CacheRunEvidence{}, invalid("cache run contains duplicate stage %q", hit.StageKey)
		}
		if err := validateCacheHit(hit, run, capturedAt, residencyPlanRevisionID); err != nil {
			return CacheRunEvidence{}, err
		}
		if _, duplicate := seenEntries[hit.EntryID]; duplicate {
			return CacheRunEvidence{}, invalid("cache run reuses one entry for multiple stages")
		}
		if _, duplicate := seenArtifacts[hit.StageArtifactID]; duplicate {
			return CacheRunEvidence{}, invalid("cache run reuses one Artifact for multiple stage interfaces")
		}
		seenEntries[hit.EntryID] = struct{}{}
		seenArtifacts[hit.StageArtifactID] = struct{}{}
		byStage[hit.StageKey] = hit
	}
	encoder, encoderOK := byStage["encoder"]
	dit, ditOK := byStage["dit"]
	if !encoderOK || !ditOK {
		return CacheRunEvidence{}, invalid("cache run does not contain exact Encoder and DiT hits")
	}
	return CacheRunEvidence{
		JobID: run.JobID, AttemptID: run.AttemptID, AttemptFence: run.AttemptFence,
		Hits: []CacheHitEvidence{cacheHitEvidence(encoder), cacheHitEvidence(dit)},
	}, nil
}

func validateCacheHit(
	hit CacheHitSnapshot,
	run CacheRunSnapshot,
	capturedAt time.Time,
	residencyPlanRevisionID uuid.UUID,
) error {
	if hit.StageKey != "encoder" && hit.StageKey != "dit" {
		return invalid("cache stage %q is not cacheable in this campaign", hit.StageKey)
	}
	if hit.EntryID == uuid.Nil || hit.EntryState != "LIVE" || hit.Scope != "PROJECT" ||
		hit.ScopeProjectID != run.ProjectID || hit.OrganizationID != run.OrganizationID ||
		hit.SourceProjectID != run.ProjectID || hit.SourceJobID == uuid.Nil ||
		hit.SourceJobID == run.JobID || hit.SourceStageRunID == uuid.Nil ||
		hit.SourceResidencyPlanRevisionID != residencyPlanRevisionID ||
		hit.TargetJobID != run.JobID || hit.TargetStageRunID == uuid.Nil ||
		hit.StageArtifactID == uuid.Nil || !validText(hit.ExactObjectVersion, 1000) ||
		!sha256Pattern.MatchString(hit.ArtifactDigest) || !sha256Pattern.MatchString(hit.CacheKeyDigest) ||
		hit.CachePolicyRevisionID == uuid.Nil || hit.ResultEquivalenceRevisionID == uuid.Nil ||
		hit.ReferenceID == uuid.Nil || hit.ReferenceState != "ACTIVE" ||
		hit.ReferenceAcquiredAt.IsZero() || hit.ReferenceAcquiredAt.After(capturedAt) ||
		hit.ExecutionPinID == uuid.Nil || hit.PinKind != "EXECUTION" || hit.PinState != "ACTIVE" ||
		hit.PinAcquiredAt.IsZero() || !hit.PinAcquiredAt.Equal(hit.ReferenceAcquiredAt) ||
		hit.OutputBindingSourceKind != "EXACT_CACHE" ||
		hit.OutputBindingArtifactID != hit.StageArtifactID {
		return invalid("%s cache hit lacks exact scope, Artifact, reference, pin, or output binding", hit.StageKey)
	}
	return nil
}

func cacheHitEvidence(hit CacheHitSnapshot) CacheHitEvidence {
	return CacheHitEvidence{
		StageKey: hit.StageKey, EntryID: hit.EntryID, SourceJobID: hit.SourceJobID,
		SourceStageRunID: hit.SourceStageRunID, TargetStageRunID: hit.TargetStageRunID,
		StageArtifactID: hit.StageArtifactID, ExactObjectVersion: hit.ExactObjectVersion,
		ArtifactDigest: hit.ArtifactDigest, CachePolicyRevisionID: hit.CachePolicyRevisionID,
		ResultEquivalenceRevisionID: hit.ResultEquivalenceRevisionID,
		ReferenceID:                 hit.ReferenceID, ExecutionPinID: hit.ExecutionPinID,
	}
}

func validateEnvelope(input Input) error {
	if !sha256Pattern.MatchString(input.ReleaseDigest) ||
		!sha256Pattern.MatchString(input.ConfigurationRevision) ||
		!validText(input.ValidationEnvironment, 500) ||
		!validText(input.CollectorIdentity, 500) || input.CapturedAt.IsZero() ||
		input.ResidencyPlanRevisionID == uuid.Nil ||
		!validDatabaseRead(input.InitialDatabaseRead, input.CapturedAt) ||
		!validDatabaseRead(input.FinalDatabaseRead, input.CapturedAt) ||
		input.InitialDatabaseRead.DatabaseTime.After(input.FinalDatabaseRead.DatabaseTime) {
		return invalid("release binding or database snapshot provenance is incomplete")
	}
	return nil
}

func validDatabaseRead(read DatabaseReadProvenance, capturedAt time.Time) bool {
	return !read.DatabaseTime.IsZero() && !read.DatabaseTime.After(capturedAt) &&
		capturedAt.Sub(read.DatabaseTime) <= 5*time.Minute && validText(read.BackendPID, 100) &&
		validText(read.SnapshotID, 200)
}

func verifyPhysicalRun(
	run RunSnapshot,
	residencyPlanRevisionID uuid.UUID,
) (RunEvidence, runEquivalenceBinding, error) {
	if run.Kind != RunSameNode && run.Kind != RunCrossNode {
		return RunEvidence{}, runEquivalenceBinding{}, invalid("unknown physical run kind %q", run.Kind)
	}
	if run.JobID == uuid.Nil || run.AttemptID == uuid.Nil || run.AttemptFence <= 0 ||
		run.ExecutionGraphRevisionID == uuid.Nil || run.JobState != "SUCCEEDED" ||
		run.AttemptState != "SUCCEEDED" || run.GraphState != "SUCCEEDED" ||
		run.ArtifactSetCount != 1 || run.VisibleCompletionCount != 1 || run.ChargeCount != 1 {
		return RunEvidence{}, runEquivalenceBinding{}, invalid("%s run is not exactly-once successful", run.Kind)
	}
	if len(run.Stages) != 3 || len(run.Transfers) != 2 {
		return RunEvidence{}, runEquivalenceBinding{}, invalid("%s run does not contain three stages and two transfers", run.Kind)
	}
	stages := make(map[string]StageSnapshot, 3)
	seenRuns := make(map[uuid.UUID]struct{}, 3)
	seenArtifacts := make(map[uuid.UUID]struct{}, 3)
	for _, stage := range run.Stages {
		if _, duplicate := stages[stage.StageKey]; duplicate {
			return RunEvidence{}, runEquivalenceBinding{}, invalid("%s run contains duplicate stage %q", run.Kind, stage.StageKey)
		}
		if err := validatePhysicalStage(stage, residencyPlanRevisionID); err != nil {
			return RunEvidence{}, runEquivalenceBinding{}, err
		}
		if _, duplicate := seenRuns[stage.StageRunID]; duplicate {
			return RunEvidence{}, runEquivalenceBinding{}, invalid("%s run contains duplicate StageRun identity", run.Kind)
		}
		if _, duplicate := seenArtifacts[stage.ArtifactID]; duplicate {
			return RunEvidence{}, runEquivalenceBinding{}, invalid("%s run contains duplicate StageArtifact identity", run.Kind)
		}
		seenRuns[stage.StageRunID] = struct{}{}
		seenArtifacts[stage.ArtifactID] = struct{}{}
		stages[stage.StageKey] = stage
	}
	encoder, encoderOK := stages["encoder"]
	dit, ditOK := stages["dit"]
	vae, vaeOK := stages["vae"]
	if !encoderOK || !ditOK || !vaeOK ||
		!sha256Pattern.MatchString(encoder.RootInputDigest) || len(encoder.InputStageArtifactIDs) != 0 ||
		encoder.RootInputDigest == "" || encoder.ArtifactID == uuid.Nil ||
		len(dit.InputStageArtifactIDs) != 1 || dit.InputStageArtifactIDs[0] != encoder.ArtifactID ||
		dit.RootInputDigest != "" || len(vae.InputStageArtifactIDs) != 1 ||
		vae.InputStageArtifactIDs[0] != dit.ArtifactID || vae.RootInputDigest != "" {
		return RunEvidence{}, runEquivalenceBinding{}, invalid("%s Encoder-DiT-VAE StageArtifact lineage is not exact", run.Kind)
	}
	if run.Kind == RunSameNode {
		if encoder.NodeIdentity != dit.NodeIdentity || dit.NodeIdentity != vae.NodeIdentity {
			return RunEvidence{}, runEquivalenceBinding{}, invalid("same-node run spans multiple nodes")
		}
	} else {
		nodes := map[string]struct{}{encoder.NodeIdentity: {}, dit.NodeIdentity: {}, vae.NodeIdentity: {}}
		if len(nodes) != 3 {
			return RunEvidence{}, runEquivalenceBinding{}, invalid("cross-node run does not place all stages on distinct nodes")
		}
	}
	orderedTransfers, err := verifyTransfers(run.Transfers, encoder, dit, vae)
	if err != nil {
		return RunEvidence{}, runEquivalenceBinding{}, err
	}
	binding := runEquivalenceBinding{
		rootInputDigest: encoder.RootInputDigest,
		stageProfileRevisionIDs: [3]uuid.UUID{
			encoder.StageProfileRevisionID, dit.StageProfileRevisionID, vae.StageProfileRevisionID,
		},
		stageInterfaceRevisionIDs: [3]uuid.UUID{
			encoder.StageInterfaceRevisionID, dit.StageInterfaceRevisionID, vae.StageInterfaceRevisionID,
		},
		connectorRevisionIDs: [2]uuid.UUID{
			orderedTransfers[0].ConnectorRevisionID, orderedTransfers[1].ConnectorRevisionID,
		},
		finalOutputDigest: vae.OutputDigest,
	}
	return RunEvidence{
		Kind: run.Kind, JobID: run.JobID, AttemptID: run.AttemptID,
		AttemptFence: run.AttemptFence, ExecutionGraphRevisionID: run.ExecutionGraphRevisionID,
		RootInputDigest: encoder.RootInputDigest,
		StageProfileRevisionIDs: []uuid.UUID{
			encoder.StageProfileRevisionID, dit.StageProfileRevisionID, vae.StageProfileRevisionID,
		},
		StageInterfaceRevisionIDs: []uuid.UUID{
			encoder.StageInterfaceRevisionID, dit.StageInterfaceRevisionID, vae.StageInterfaceRevisionID,
		},
		ConnectorRevisionIDs: []uuid.UUID{
			orderedTransfers[0].ConnectorRevisionID, orderedTransfers[1].ConnectorRevisionID,
		},
		StageRunIDs:       []uuid.UUID{encoder.StageRunID, dit.StageRunID, vae.StageRunID},
		NodeIdentities:    []string{encoder.NodeIdentity, dit.NodeIdentity, vae.NodeIdentity},
		TransferTicketIDs: []uuid.UUID{orderedTransfers[0].ID, orderedTransfers[1].ID},
		FinalOutputDigest: vae.OutputDigest,
	}, binding, nil
}

func validatePhysicalStage(stage StageSnapshot, residencyPlanRevisionID uuid.UUID) error {
	if stage.State != "SUCCEEDED" || stage.SourceKind != "PHYSICAL" ||
		stage.StageRunID == uuid.Nil || stage.StageAttemptID == uuid.Nil ||
		stage.StageProfileRevisionID == uuid.Nil || stage.ArtifactID == uuid.Nil ||
		!validText(stage.ObjectVersion, 1000) || !sha256Pattern.MatchString(stage.OutputDigest) ||
		stage.StageInterfaceRevisionID == uuid.Nil || !validText(stage.NodeIdentity, 300) ||
		stage.WorkerInstanceID == uuid.Nil || stage.WorkerInstanceEpoch <= 0 ||
		stage.ResidencyPlanRevisionID != residencyPlanRevisionID ||
		stage.WorkerMemberID == uuid.Nil || stage.MemberEpoch <= 0 ||
		stage.DeviceID == uuid.Nil || stage.DeviceEpoch <= 0 ||
		stage.ModelResidencyID == uuid.Nil || stage.ModelRuntimeEpoch <= 0 {
		return invalid("stage %q physical authority is incomplete", stage.StageKey)
	}
	return nil
}

func verifyTransfers(
	transfers []TransferSnapshot,
	encoder, dit, vae StageSnapshot,
) ([2]TransferSnapshot, error) {
	want := map[string]int{
		transferKey(encoder.StageRunID, dit.StageRunID, encoder.ArtifactID, dit.WorkerInstanceID, dit.WorkerInstanceEpoch): 0,
		transferKey(dit.StageRunID, vae.StageRunID, dit.ArtifactID, vae.WorkerInstanceID, vae.WorkerInstanceEpoch):         1,
	}
	seen := make(map[string]struct{}, 2)
	seenIDs := make(map[uuid.UUID]struct{}, 2)
	var ordered [2]TransferSnapshot
	for _, transfer := range transfers {
		key := transferKey(
			transfer.SourceStageRunID, transfer.DestinationStageRunID,
			transfer.StageArtifactID, transfer.DestinationWorkerInstanceID,
			transfer.DestinationWorkerInstanceEpoch,
		)
		if transfer.ID == uuid.Nil || transfer.ConnectorRevisionID == uuid.Nil ||
			transfer.State != "CONSUMED" {
			return [2]TransferSnapshot{}, invalid("transfer ticket identity or state is invalid")
		}
		index, expected := want[key]
		if !expected {
			return [2]TransferSnapshot{}, invalid("transfer ticket does not bind an exact adjacent stage edge")
		}
		if _, duplicate := seen[key]; duplicate {
			return [2]TransferSnapshot{}, invalid("duplicate transfer ticket edge")
		}
		if _, duplicate := seenIDs[transfer.ID]; duplicate {
			return [2]TransferSnapshot{}, invalid("duplicate transfer ticket identity")
		}
		seen[key] = struct{}{}
		seenIDs[transfer.ID] = struct{}{}
		ordered[index] = transfer
	}
	if len(seen) != len(want) {
		return [2]TransferSnapshot{}, invalid("transfer ticket edge set is incomplete")
	}
	return ordered, nil
}

func transferKey(source, destination, artifact, worker uuid.UUID, epoch int64) string {
	return fmt.Sprintf("%s/%s/%s/%s/%d", source, destination, artifact, worker, epoch)
}

func validText(value string, maximum int) bool {
	return len(value) > 0 && len(value) <= maximum && strings.TrimSpace(value) == value &&
		strings.IndexFunc(value, func(character rune) bool { return character < 0x20 || character == 0x7f }) == -1
}

func invalid(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidCampaignEvidence, fmt.Sprintf(format, arguments...))
}
