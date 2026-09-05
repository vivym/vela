package stageworkeragent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// TerminalDispositionReader authenticates the current Control session and the
// complete response. The production Control transport implements this contract.
// Nil means RETAIN, not proof of non-admission or writer completion.
type TerminalDispositionReader interface {
	ReadTerminalDisposition(context.Context, *stageauthority.Validator, *velav1.StageAuthority, string, uuid.UUID) (*stageauthority.VerifiedTerminalDisposition, error)
}

type terminalRecoveryCandidate struct {
	stageRunID  uuid.UUID
	memberID    string
	intent      bool
	authorities map[string]*velav1.StageAuthority
	acquires    map[string]uuid.UUID
	anchor      *velav1.StageAuthority
}

// Called under materializationMu after all already complete proofs are resumed.
// One bounded pass covers discovery plus proof collection for retained history.
func (agent *StreamAgent) collectTerminalMaterializations(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, agent.runtime.floor.timeout)
	defer cancel()
	records, err := agent.materialization.journal.List(ctx)
	if err != nil {
		return 0, err
	}
	candidates, err := agent.admission.terminalRecoveryCandidates(ctx, records)
	if err != nil {
		return 0, err
	}
	retired := 0
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return retired, err
		}
		response, err := agent.terminalHistory.ReadTerminalDisposition(ctx, agent.admission.validator,
			proto.Clone(candidate.anchor).(*velav1.StageAuthority), candidate.memberID, candidate.acquires[candidate.anchor.GetStageAllocationId()])
		if err != nil {
			return retired, fmt.Errorf("read terminal Stage %s: %w", candidate.stageRunID, err)
		}
		if err := ctx.Err(); err != nil {
			return retired, err
		}
		if response == nil {
			if candidate.intent {
				return retired, fmt.Errorf("terminal Stage %s INTENT still requires complete history: %w", candidate.stageRunID, ErrScratchRetirementUnproven)
			}
			continue
		}
		// The transport checks its session; the consumer independently authenticates
		// the returned facts and their exact query binding before installing a floor.
		verified, err := agent.admission.validator.ValidateTerminalDisposition(response.Disposition, candidate.anchor,
			candidate.memberID, response.Disposition.GetControlSessionEpoch())
		if err != nil {
			return retired, err
		}
		if verified.Digest != response.Digest {
			return retired, errors.New("terminal history reader returned a mismatched verified digest")
		}
		targets, err := agent.runtime.executionFloorTargets(verified.Disposition)
		if err != nil {
			return retired, err
		}
		_, count, err := agent.retireTerminalMaterializations(ctx, verified.Disposition, candidate.authorities, targets)
		retired += count
		if err != nil {
			return retired, err
		}
	}
	return retired, nil
}

func (gate *FileAssignmentAdmission) terminalRecoveryCandidates(ctx context.Context, records []PendingMaterialization) ([]terminalRecoveryCandidate, error) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return nil, err
	}
	retirements := make(map[uuid.UUID]TerminalRetirementPhase)
	for _, entry := range gate.state.Retirements {
		retirements[entry.StageRunID] = entry.Phase
	}
	groups := make(map[uuid.UUID]*terminalRecoveryCandidate)
	add := func(authority *velav1.StageAuthority, acquireID uuid.UUID) error {
		verified, err := gate.validator.ValidateEnvelopeForReplay(authority, gate.maxSkew)
		if err != nil {
			return err
		}
		a := verified.Authority
		if a.GetSchemaVersion() != stageauthority.SchemaVersionV2 || a.GetWorkerInstanceId() != gate.state.WorkerInstanceID.String() ||
			a.GetWorkerInstanceEpoch() != gate.state.WorkerInstanceEpoch || !slices.ContainsFunc(a.GetMembers(), func(member *velav1.StageAuthorityMemberEpoch) bool {
			return member.GetWorkerMemberId() == gate.state.WorkerMemberID.String()
		}) {
			return ErrAdmissionClosed
		}
		id := uuid.MustParse(a.GetStageRunId())
		if retirements[id] == TerminalRetirementReady || retirements[id] == TerminalRetirementRetired {
			return nil
		}
		candidate := groups[id]
		if candidate == nil {
			candidate = &terminalRecoveryCandidate{stageRunID: id, memberID: gate.state.WorkerMemberID.String(),
				intent: retirements[id] == TerminalRetirementIntent, authorities: make(map[string]*velav1.StageAuthority), acquires: make(map[string]uuid.UUID)}
			groups[id] = candidate
		}
		allocation := a.GetStageAllocationId()
		if previous := candidate.authorities[allocation]; previous != nil && !proto.Equal(previous, a) {
			if stageauthority.ValidateRenewal(previous, a) != nil {
				if stageauthority.ValidateRenewal(a, previous) != nil {
					return errors.New("terminal recovery has conflicting execution envelopes")
				}
				a = previous
			}
		}
		if previous := candidate.acquires[allocation]; previous != uuid.Nil && acquireID != uuid.Nil && previous != acquireID {
			return errors.New("terminal recovery has conflicting Acquire command IDs")
		}
		candidate.authorities[allocation] = a
		if acquireID != uuid.Nil {
			candidate.acquires[allocation] = acquireID
		}
		if candidate.anchor == nil || a.GetExecutionSequence() >= candidate.anchor.GetExecutionSequence() {
			candidate.anchor = a
		}
		return nil
	}
	for _, entry := range admissionEntries(gate.state) {
		record, err := gate.record(entry)
		if err != nil {
			return nil, err
		}
		if err := add(record.Latest, record.AcquireCommandID); err != nil {
			return nil, err
		}
	}
	for _, record := range records {
		if err := add(record.StageAuthority, uuid.Nil); err != nil {
			return nil, err
		}
	}
	for id, phase := range retirements {
		if phase == TerminalRetirementIntent && groups[id] == nil {
			return nil, fmt.Errorf("terminal Stage %s has no retained query envelope: %w", id, ErrAdmissionRecoveryRequired)
		}
	}
	candidates := make([]terminalRecoveryCandidate, 0, len(groups))
	for _, candidate := range groups {
		candidates = append(candidates, *candidate)
	}
	slices.SortFunc(candidates, func(a, b terminalRecoveryCandidate) int {
		if a.anchor.GetExecutionSequence() < b.anchor.GetExecutionSequence() {
			return -1
		}
		if a.anchor.GetExecutionSequence() > b.anchor.GetExecutionSequence() {
			return 1
		}
		return bytes.Compare(a.stageRunID[:], b.stageRunID[:])
	})
	return candidates, nil
}
