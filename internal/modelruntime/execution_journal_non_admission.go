package modelruntime

import (
	"cmp"
	"slices"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// This is trusted owner assembly, not part of a client request. A future remote
// owner must select the approved live route independently of the signed history.
// Historical journal scope deliberately does not establish current residency.
type executionJournalRoute struct {
	binding      stageauthority.RuntimeBinding
	maxClockSkew time.Duration
}

func (service *Service) journalRoute() executionJournalRoute {
	return executionJournalRoute{binding: cloneBinding(service.binding), maxClockSkew: service.maxClockSkew}
}

func (store *executionStateFile) saveNonAdmission(authority stageauthority.Verified, route executionJournalRoute, observed time.Time) error {
	return store.transition(func(draft *executionJournalDraft) error {
		return draft.recordNonAdmission(authority, route, observed)
	})
}

func (draft *executionJournalDraft) recordNonAdmission(value stageauthority.Verified, route executionJournalRoute, observed time.Time) error {
	verified, err := draft.verifyMutationAuthority(value)
	if err != nil {
		return err
	}
	if _, err := draft.scope.floor.validator.ValidateEnvelopeForReplay(verified.Authority, route.maxClockSkew); err != nil {
		return err
	}
	if _, err := draft.scope.floor.validator.ValidateSignature(verified.Authority, route.binding); err != nil {
		return err
	}
	sequence := verified.Authority.GetExecutionSequence()
	if draft.state.Floor < sequence {
		return ErrExecutionNonAdmissionUnproven
	}
	if err := draft.matchTerminalNonAdmissionAuthority(verified.Authority); err != nil {
		return err
	}
	if saved, err := draft.nonAdmissionCheckpoint(verified); err != nil || saved != nil {
		return err
	}
	if err := draft.requireUnadmittedSequence(sequence); err != nil {
		return err
	}
	if len(draft.state.NonAdmissions) >= maxNonAdmissionCheckpoints {
		return ErrExecutionNonAdmissionHistoryFull
	}
	if observed.IsZero() || observed.Location() != time.UTC {
		return ErrExecutionNonAdmissionUnproven
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(verified.Authority)
	if err != nil {
		return err
	}
	next := draft.state
	next.NonAdmissions = append(slices.Clone(next.NonAdmissions), executionDiskNonAdmission{
		Authority: wire, AuthorityDigest: verified.Digest, ExecutionSequence: sequence,
		InstalledCutoff: next.Floor, Contract: ExecutionNonAdmissionContract, ObservedAt: observed,
	})
	slices.SortFunc(next.NonAdmissions, func(a, b executionDiskNonAdmission) int {
		return cmp.Compare(a.ExecutionSequence, b.ExecutionSequence)
	})
	return draft.replace(next)
}

func (store *executionStateFile) saveTerminalNonAdmission(disposition *velav1.StageTerminalDisposition, allocationID string, route executionJournalRoute, observed time.Time) error {
	return store.transition(func(draft *executionJournalDraft) error {
		return draft.recordTerminalNonAdmission(disposition, allocationID, route, observed)
	})
}

func (draft *executionJournalDraft) recordTerminalNonAdmission(disposition *velav1.StageTerminalDisposition, allocationID string, route executionJournalRoute, observed time.Time) error {
	if disposition == nil || proto.Size(disposition) > maxExecutionWireBytes {
		return ErrExecutionNonAdmissionUnproven
	}
	verified, err := draft.scope.floor.validator.ValidateTerminalDispositionEnvelope(disposition)
	if err != nil {
		return err
	}
	if err := draft.scope.matchExecutionFloorScope(verified.Disposition); err != nil {
		return err
	}
	allocation := stageauthority.FindTerminalAllocation(verified.Disposition, allocationID)
	if allocation == nil {
		return ErrExecutionNonAdmissionUnproven
	}
	if !route.matchesTerminalAllocation(draft.scope.binding, allocation) {
		return stageauthority.ErrRuntimeMismatch
	}
	sequence := allocation.GetExecutionSequence()
	if draft.state.Floor < sequence {
		return ErrExecutionNonAdmissionUnproven
	}
	if saved, err := draft.terminalNonAdmissionCheckpoint(verified.Disposition, allocation); err != nil || saved != nil {
		return err
	}
	if err := draft.requireUnadmittedSequence(sequence); err != nil {
		return err
	}
	for _, record := range draft.state.NonAdmissions {
		if record.ExecutionSequence == sequence {
			original, err := draft.retainedAuthority(record.Authority)
			if err != nil || stageauthority.ValidateTerminalAllocation(verified.Disposition, allocation, original.Authority) != nil {
				return ErrExecutionNonAdmissionUnproven
			}
		}
	}
	if len(draft.state.TerminalNonAdmissions) >= maxNonAdmissionCheckpoints {
		return ErrExecutionNonAdmissionHistoryFull
	}
	if observed.IsZero() || observed.Location() != time.UTC || observed.Before(verified.Disposition.GetObservedAt().AsTime()) {
		return ErrExecutionNonAdmissionUnproven
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(verified.Disposition)
	if err != nil {
		return err
	}
	next := draft.state
	next.TerminalNonAdmissions = append(slices.Clone(next.TerminalNonAdmissions), terminalDiskNonAdmission{
		Disposition: wire, DispositionDigest: verified.Digest, StageAllocationID: allocationID,
		ExecutionSequence: sequence, InstalledCutoff: next.Floor, Contract: TerminalNonAdmissionContract, ObservedAt: observed,
	})
	slices.SortFunc(next.TerminalNonAdmissions, func(a, b terminalDiskNonAdmission) int {
		return cmp.Compare(a.ExecutionSequence, b.ExecutionSequence)
	})
	return draft.replace(next)
}

func (route executionJournalRoute) matchesTerminalAllocation(scope stageauthority.RuntimeBinding, allocation *velav1.StageTerminalAllocation) bool {
	binding := route.binding
	if binding.WorkerInstanceID != scope.WorkerInstanceID || binding.WorkerInstanceEpoch != scope.WorkerInstanceEpoch ||
		binding.WorkerMemberID != scope.WorkerMemberID || binding.WorkerMemberEpoch != scope.WorkerMemberEpoch ||
		binding.ModelResidencyID != allocation.GetModelResidencyId() || binding.ModelRuntimeIdentity != allocation.GetModelRuntimeIdentity() ||
		binding.StageProfileRevisionID != allocation.GetStageProfileRevisionId() {
		return false
	}
	for _, member := range allocation.GetMembers() {
		if member.GetWorkerMemberId() == binding.WorkerMemberID {
			return member.GetMemberEpoch() == binding.WorkerMemberEpoch && member.GetModelRuntimeEpoch() == binding.ModelRuntimeEpoch
		}
	}
	return false
}
