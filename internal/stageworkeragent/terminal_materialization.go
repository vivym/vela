package stageworkeragent

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

// RetireTerminalScratch serializes retirement with local sealing/publication.
// After durable RETIRED it discards matching obsolete materialization records;
// doing so never asserts that their COMMIT or SOURCE_LOST commands succeeded.
func (agent *StreamAgent) RetireTerminalScratch(ctx context.Context, disposition *velav1.StageTerminalDisposition, authorities map[string]*velav1.StageAuthority, targets map[string]*velav1.ModelRuntimeIdentity) (TerminalRetirementSnapshot, error) {
	if agent == nil || ctx == nil || agent.terminalRetirement == nil || agent.materialization == nil {
		return TerminalRetirementSnapshot{}, ErrScratchRetirementUnproven
	}
	agent.materializationMu.Lock()
	defer agent.materializationMu.Unlock()
	snapshot, _, err := agent.retireTerminalMaterializations(ctx, disposition, authorities, targets)
	return snapshot, err
}

func (agent *StreamAgent) retireTerminalMaterializations(ctx context.Context, disposition *velav1.StageTerminalDisposition, authorities map[string]*velav1.StageAuthority, targets map[string]*velav1.ModelRuntimeIdentity) (TerminalRetirementSnapshot, int, error) {
	verified, err := agent.admission.validator.ValidateTerminalDispositionEnvelope(disposition)
	if err != nil {
		return TerminalRetirementSnapshot{}, 0, err
	}
	records, err := agent.materialization.journal.List(ctx)
	if err != nil {
		return TerminalRetirementSnapshot{}, 0, err
	}
	selected, err := agent.terminalMaterializationRecords(verified.Disposition, records)
	if err != nil {
		return TerminalRetirementSnapshot{}, 0, err
	}
	snapshot, err := agent.terminalRetirement.Retire(ctx, verified.Disposition, authorities, targets)
	if err != nil {
		return snapshot, 0, err
	}
	count, err := agent.finishTerminalMaterializations(ctx, snapshot, selected)
	return snapshot, count, err
}

type terminalMaterializationPlan struct {
	snapshot TerminalRetirementSnapshot
	records  []PendingMaterialization
}

// Called under materializationMu before ordinary replay or opening local output.
// Validate all affected records before any cleanup, then recover every complete
// checkpoint. An INTENT cannot grant cleanup or ordinary materialization entry.
func (agent *StreamAgent) resumeTerminalMaterializations(ctx context.Context, prepareHistory func(context.Context) error) (int, error) {
	if agent.terminalRetirement == nil {
		return 0, nil
	}
	state, err := agent.admission.Snapshot(ctx)
	if err != nil {
		return 0, err
	}
	records, err := agent.materialization.journal.List(ctx)
	if err != nil {
		return 0, err
	}
	plans := make([]terminalMaterializationPlan, 0, len(state.Retirements))
	for _, snapshot := range state.Retirements {
		history, err := agent.admission.terminalRetirementHistory(ctx, snapshot.StageRunID)
		if err != nil {
			return 0, err
		}
		selected, err := agent.terminalMaterializationRecords(history, records)
		if err != nil {
			return 0, err
		}
		plans = append(plans, terminalMaterializationPlan{snapshot: snapshot, records: selected})
	}
	retired := 0
	var incomplete error
	for _, plan := range plans {
		if plan.snapshot.Phase == TerminalRetirementIntent {
			if agent.terminalHistory == nil {
				incomplete = errors.Join(incomplete, fmt.Errorf("terminal Stage %s needs fresh history and complete drain: %w", plan.snapshot.StageRunID, ErrScratchRetirementUnproven))
			}
			continue
		}
		snapshot, err := agent.terminalRetirement.Resume(ctx, plan.snapshot.StageRunID)
		if err != nil {
			return retired, errors.Join(incomplete, err)
		}
		count, err := agent.finishTerminalMaterializations(ctx, snapshot, plan.records)
		retired += count
		if err != nil {
			return retired, errors.Join(incomplete, err)
		}
	}
	if agent.terminalHistory != nil {
		count, err := agent.collectTerminalMaterializations(ctx, prepareHistory)
		return retired + count, errors.Join(incomplete, err)
	}
	return retired, incomplete
}

func (gate *FileAssignmentAdmission) terminalRetirementHistory(ctx context.Context, stageRunID uuid.UUID) (*velav1.StageTerminalDisposition, error) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return nil, err
	}
	index := retirementIndex(gate.state, stageRunID)
	if index < 0 {
		return nil, ErrScratchRetirementUnproven
	}
	return gate.retirementDisposition(gate.state.Retirements[index])
}

func (agent *StreamAgent) terminalMaterializationRecords(history *velav1.StageTerminalDisposition, records []PendingMaterialization) ([]PendingMaterialization, error) {
	var selected []PendingMaterialization
	for _, record := range records {
		if record.StageAuthority.GetStageRunId() != history.GetStageRunId() {
			continue
		}
		if err := agent.validateTerminalMaterialization(history, record); err != nil {
			return nil, fmt.Errorf("validate terminal materialization %s: %w", record.ID, err)
		}
		selected = append(selected, record)
	}
	return selected, nil
}

func (agent *StreamAgent) validateTerminalMaterialization(history *velav1.StageTerminalDisposition, record PendingMaterialization) error {
	if err := validatePendingMaterialization(record); err != nil {
		return err
	}
	if record.ConfirmedDisposition == MaterializationCommitted && history.GetTerminalState() != velav1.StageTerminalState_STAGE_TERMINAL_STATE_SUCCEEDED {
		return errors.New("confirmed COMMIT conflicts with terminal Stage state")
	}
	stage, err := agent.admission.validator.ValidateEnvelopeForReplay(record.StageAuthority, agent.admission.maxSkew)
	if err != nil {
		return err
	}
	if err := stageauthority.ValidateTerminalAllocation(history, stageauthority.FindTerminalAllocation(history, stage.Authority.GetStageAllocationId()), stage.Authority); err != nil {
		return err
	}
	manifest, err := stageartifact.ParseLocalOutputManifestV1(record.LocalReceipt.GetOutputManifestJson())
	if err != nil {
		return err
	}
	if err := validateAttemptOwnedScratchManifest(manifest); err != nil {
		return err
	}
	sealed := record.LocalReceipt.GetSealedAt().AsTime()
	// An in-flight seal may finish after Control observed the terminal state.
	// The later complete drain checkpoint, not timestamp order, excludes writers.
	if !manifest.MatchesStageAuthority(stage.Authority) || manifest.SizeBytes != record.LocalReceipt.GetTotalSizeBytes() ||
		sealed.Before(stage.Authority.GetIssuedAt().AsTime()) || !sealed.Before(stage.Authority.GetExpiresAt().AsTime()) {
		return errors.New("terminal materialization receipt conflicts with signed allocation history")
	}
	if record.MaterializationAuthority != nil {
		materialization, err := agent.materialization.validator.ValidateForReplay(record.MaterializationAuthority, agent.materialization.maxClockSkew)
		if err != nil {
			return err
		}
		if err := materializationMatchesPending(materialization, record); err != nil {
			return err
		}
	}
	return nil
}

func (agent *StreamAgent) finishTerminalMaterializations(ctx context.Context, snapshot TerminalRetirementSnapshot, records []PendingMaterialization) (int, error) {
	if snapshot.Phase != TerminalRetirementRetired {
		return 0, ErrScratchRetirementUnproven
	}
	agent.runtimeMu.Lock()
	if active := agent.activeAuthority(); active.GetStageRunId() == snapshot.StageRunID.String() {
		agent.clearActive(active)
	}
	agent.runtimeMu.Unlock()
	for index, record := range records {
		if err := agent.materialization.journal.Delete(ctx, record.ID); err != nil {
			return index, fmt.Errorf("clear retired Stage materialization record: %w", err)
		}
	}
	return len(records), nil
}
