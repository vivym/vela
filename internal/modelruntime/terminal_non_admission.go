package modelruntime

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const TerminalNonAdmissionContract = "vela-terminal-allocation-never-admitted-v1"

// TerminalNonAdmissionCheckpoint covers one allocation on one member, including
// allocations for which no execution envelope was ever issued. It proves neither
// backend drain nor Worker input exclusion and grants no execution authority.
type TerminalNonAdmissionCheckpoint struct {
	WorkerMemberID    string
	Disposition       *velav1.StageTerminalDisposition
	DispositionDigest [sha256.Size]byte
	StageAllocationID string
	ExecutionSequence int64
	InstalledCutoff   int64
	Contract          string
	ObservedAt        time.Time
}

type terminalDiskNonAdmission struct {
	Disposition       []byte            `json:"disposition"`
	DispositionDigest [sha256.Size]byte `json:"disposition_digest"`
	StageAllocationID string            `json:"stage_allocation_id"`
	ExecutionSequence int64             `json:"execution_sequence"`
	InstalledCutoff   int64             `json:"installed_cutoff"`
	Contract          string            `json:"contract"`
	ObservedAt        time.Time         `json:"observed_at"`
}

// CheckpointTerminalNonAdmission uses Control's signed historical allocation
// directly. New proof requires the selected original resident epoch/profile and
// an independently persisted floor. No execution envelope is synthesized.
func (supervisor *Supervisor) CheckpointTerminalNonAdmission(ctx context.Context, disposition *velav1.StageTerminalDisposition, allocationID string) (*TerminalNonAdmissionCheckpoint, error) {
	return supervisor.terminalNonAdmission(ctx, disposition, allocationID, true)
}

// InspectTerminalNonAdmission replays existing proof across profile/epoch changes.
// Missing history stays unknown even when a newer Runtime has an empty backend.
func (supervisor *Supervisor) InspectTerminalNonAdmission(ctx context.Context, disposition *velav1.StageTerminalDisposition, allocationID string) (*TerminalNonAdmissionCheckpoint, error) {
	return supervisor.terminalNonAdmission(ctx, disposition, allocationID, false)
}

func (supervisor *Supervisor) terminalNonAdmission(ctx context.Context, disposition *velav1.StageTerminalDisposition, allocationID string, checkpoint bool) (*TerminalNonAdmissionCheckpoint, error) {
	if supervisor == nil || supervisor.floor == nil || supervisor.admission == nil || ctx == nil {
		return nil, ErrExecutionNonAdmissionUnproven
	}
	admission := supervisor.admission
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := admission.checkStateLocked(); err != nil {
		return nil, err
	}
	if admission.store == nil || disposition == nil || proto.Size(disposition) > maxExecutionWireBytes {
		return nil, ErrExecutionNonAdmissionUnproven
	}
	verified, err := supervisor.floor.validator.ValidateTerminalDispositionSignature(disposition)
	if err != nil {
		return nil, err
	}
	// Inspection accepts expired historical evidence but never a future fact.
	if supervisor.services[0].clock.Now().Before(verified.Disposition.GetObservedAt().AsTime()) {
		return nil, stageauthority.ErrStale
	}
	if err := supervisor.matchExecutionFloorScope(verified.Disposition, false); err != nil {
		return nil, err
	}
	allocation := terminalAllocationByID(verified.Disposition, allocationID)
	if allocation == nil {
		return nil, ErrExecutionNonAdmissionUnproven
	}
	store := admission.store
	if !checkpoint {
		return store.terminalNonAdmissionCheckpoint(verified.Disposition, allocation)
	}
	// Validate freshness at the same boundary as execution admission and storage.
	if _, err := supervisor.floor.validator.ValidateTerminalDispositionEnvelope(verified.Disposition); err != nil {
		return nil, err
	}
	service := supervisor.terminalAllocationService(allocation)
	if service == nil {
		return nil, stageauthority.ErrRuntimeMismatch
	}
	sequence := allocation.GetExecutionSequence()
	if store.state.Floor < sequence {
		return nil, ErrExecutionNonAdmissionUnproven
	}
	if saved, err := store.terminalNonAdmissionCheckpoint(verified.Disposition, allocation); err != nil || saved != nil {
		return saved, err
	}
	if err := store.requireUnadmittedSequence(sequence); err != nil {
		return nil, err
	}
	for _, operation := range admission.active {
		if operation.sequence == sequence {
			return nil, ErrExecutionNonAdmissionUnproven
		}
	}
	for _, record := range store.state.NonAdmissions {
		if record.ExecutionSequence == sequence {
			original, err := store.retainedAuthority(record.Authority)
			if err != nil || stageauthority.ValidateTerminalAllocation(verified.Disposition, allocation, original.Authority) != nil {
				return nil, ErrExecutionNonAdmissionUnproven
			}
		}
	}
	if len(store.state.TerminalNonAdmissions) >= maxNonAdmissionCheckpoints {
		return nil, ErrExecutionNonAdmissionHistoryFull
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(verified.Disposition)
	if err != nil {
		return nil, err
	}
	observed := service.clock.Now().UTC()
	if observed.IsZero() || observed.Before(verified.Disposition.GetObservedAt().AsTime()) {
		return nil, ErrExecutionNonAdmissionUnproven
	}
	next := store.state
	next.TerminalNonAdmissions = append(slices.Clone(next.TerminalNonAdmissions), terminalDiskNonAdmission{
		Disposition: wire, DispositionDigest: verified.Digest, StageAllocationID: allocationID,
		ExecutionSequence: sequence, InstalledCutoff: next.Floor, Contract: TerminalNonAdmissionContract, ObservedAt: observed,
	})
	slices.SortFunc(next.TerminalNonAdmissions, func(a, b terminalDiskNonAdmission) int {
		return cmp.Compare(a.ExecutionSequence, b.ExecutionSequence)
	})
	if err := store.persist(next); err != nil {
		return nil, admission.failStateLocked(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return store.terminalNonAdmissionCheckpoint(verified.Disposition, allocation)
}

func (supervisor *Supervisor) terminalAllocationService(allocation *velav1.StageTerminalAllocation) *Service {
	for _, member := range allocation.GetMembers() {
		if member.GetWorkerMemberId() == supervisor.services[0].binding.WorkerMemberID {
			return supervisor.routes[runtimeRoute{
				modelResidencyID: allocation.GetModelResidencyId(), runtimeIdentity: allocation.GetModelRuntimeIdentity(),
				modelRuntimeEpoch: member.GetModelRuntimeEpoch(), stageProfileRevisionID: allocation.GetStageProfileRevisionId(),
			}]
		}
	}
	return nil
}

func terminalAllocationByID(disposition *velav1.StageTerminalDisposition, id string) *velav1.StageTerminalAllocation {
	for _, allocation := range disposition.GetAllocations() {
		if allocation.GetStageAllocationId() == id {
			return allocation
		}
	}
	return nil
}

func sameTerminalAllocation(a, b *velav1.StageTerminalDisposition, allocation *velav1.StageTerminalAllocation) bool {
	if a.GetOrganizationId() != b.GetOrganizationId() || a.GetProjectId() != b.GetProjectId() || a.GetJobId() != b.GetJobId() ||
		a.GetAttemptId() != b.GetAttemptId() || a.GetStageRunId() != b.GetStageRunId() || a.GetTerminalState() != b.GetTerminalState() ||
		a.GetStageFence() != b.GetStageFence() || a.GetStageVersion() != b.GetStageVersion() ||
		a.GetWorkerInstanceId() != b.GetWorkerInstanceId() || a.GetWorkerInstanceEpoch() != b.GetWorkerInstanceEpoch() ||
		!bytes.Equal(a.GetDeviceSetDigest(), b.GetDeviceSetDigest()) || !bytes.Equal(a.GetMembershipDigest(), b.GetMembershipDigest()) ||
		len(a.GetDevices()) != len(b.GetDevices()) || !proto.Equal(terminalAllocationByID(a, allocation.GetStageAllocationId()), allocation) {
		return false
	}
	for i, device := range a.GetDevices() {
		if !proto.Equal(device, b.GetDevices()[i]) {
			return false
		}
	}
	return true
}

func (store *executionStateFile) terminalNonAdmissionCheckpoint(disposition *velav1.StageTerminalDisposition, allocation *velav1.StageTerminalAllocation) (*TerminalNonAdmissionCheckpoint, error) {
	for _, record := range store.state.TerminalNonAdmissions {
		if record.ExecutionSequence != allocation.GetExecutionSequence() {
			continue
		}
		original, err := store.retainedTerminalDisposition(record.Disposition)
		if err != nil {
			return nil, err
		}
		if !sameTerminalAllocation(original.Disposition, disposition, allocation) {
			return nil, ErrExecutionNonAdmissionUnproven
		}
		return &TerminalNonAdmissionCheckpoint{
			WorkerMemberID: store.scope.services[0].binding.WorkerMemberID, Disposition: original.Disposition,
			DispositionDigest: record.DispositionDigest, StageAllocationID: record.StageAllocationID,
			ExecutionSequence: record.ExecutionSequence, InstalledCutoff: record.InstalledCutoff,
			Contract: record.Contract, ObservedAt: record.ObservedAt,
		}, nil
	}
	return nil, nil
}

func (store *executionStateFile) retainedTerminalDisposition(wire []byte) (stageauthority.VerifiedTerminalDisposition, error) {
	if len(wire) == 0 || len(wire) > maxExecutionWireBytes {
		return stageauthority.VerifiedTerminalDisposition{}, ErrExecutionNonAdmissionUnproven
	}
	var disposition velav1.StageTerminalDisposition
	if err := proto.Unmarshal(wire, &disposition); err != nil {
		return stageauthority.VerifiedTerminalDisposition{}, err
	}
	verified, err := store.scope.floor.validator.ValidateTerminalDispositionSignature(&disposition)
	if err != nil {
		return verified, err
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(verified.Disposition)
	if err != nil || !bytes.Equal(canonical, wire) {
		return stageauthority.VerifiedTerminalDisposition{}, ErrExecutionNonAdmissionUnproven
	}
	return verified, store.scope.matchExecutionFloorScope(verified.Disposition, false)
}

func (store *executionStateFile) requireUnadmittedSequence(sequence int64) error {
	for _, record := range store.state.Executions {
		original, err := store.retainedAuthority(record.Authority)
		if err != nil {
			return err
		}
		if original.Authority.GetExecutionSequence() == sequence {
			return ErrExecutionNonAdmissionUnproven
		}
	}
	return nil
}

func (store *executionStateFile) validateTerminalNonAdmissions() error {
	if len(store.state.TerminalNonAdmissions) > maxNonAdmissionCheckpoints {
		return ErrExecutionNonAdmissionHistoryFull
	}
	var previous int64
	for _, record := range store.state.TerminalNonAdmissions {
		original, err := store.retainedTerminalDisposition(record.Disposition)
		if err != nil {
			return err
		}
		allocation := terminalAllocationByID(original.Disposition, record.StageAllocationID)
		if allocation == nil || record.ExecutionSequence <= previous || record.ExecutionSequence != allocation.GetExecutionSequence() ||
			record.DispositionDigest != original.Digest || record.InstalledCutoff < record.ExecutionSequence || record.InstalledCutoff > store.state.Floor ||
			record.Contract != TerminalNonAdmissionContract || record.ObservedAt.IsZero() || record.ObservedAt.Location() != time.UTC ||
			record.ObservedAt.Before(original.Disposition.GetObservedAt().AsTime()) {
			return ErrExecutionNonAdmissionUnproven
		}
		if err := store.requireUnadmittedSequence(record.ExecutionSequence); err != nil {
			return err
		}
		previous = record.ExecutionSequence
	}
	// Both proof formats share one allocation sequence namespace.
	for _, record := range store.state.NonAdmissions {
		original, err := store.retainedAuthority(record.Authority)
		if err != nil {
			return err
		}
		if err := store.matchTerminalNonAdmissionAuthority(original.Authority); err != nil {
			return err
		}
	}
	return nil
}

func (store *executionStateFile) matchTerminalNonAdmissionAuthority(authority *velav1.StageAuthority) error {
	for _, record := range store.state.TerminalNonAdmissions {
		if record.ExecutionSequence == authority.GetExecutionSequence() {
			original, err := store.retainedTerminalDisposition(record.Disposition)
			if err != nil {
				return err
			}
			if err := stageauthority.ValidateTerminalAllocation(original.Disposition, terminalAllocationByID(original.Disposition, record.StageAllocationID), authority); err != nil {
				return errors.Join(ErrExecutionNonAdmissionUnproven, err)
			}
		}
	}
	return nil
}
