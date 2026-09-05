package modelruntime

import (
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

const (
	ExecutionNonAdmissionContract = "vela-execution-never-admitted-v1"
	maxNonAdmissionCheckpoints    = 32
)

var (
	ErrExecutionNonAdmissionUnproven    = errors.New("execution non-admission is unproven")
	ErrExecutionNonAdmissionHistoryFull = errors.New("execution non-admission history is full")
)

// ExecutionNonAdmissionCheckpoint proves this allocation never entered this
// member's backend, and its persisted floor excludes future entry. It covers
// neither other allocations nor Worker inputs and is not a writer-drain result.
type ExecutionNonAdmissionCheckpoint struct {
	WorkerMemberID    string
	Authority         *velav1.StageAuthority
	AuthorityDigest   [sha256.Size]byte
	ExecutionSequence int64
	InstalledCutoff   int64
	Contract          string
	ObservedAt        time.Time
}

type executionDiskNonAdmission struct {
	Authority         []byte            `json:"authority"`
	AuthorityDigest   [sha256.Size]byte `json:"authority_digest"`
	ExecutionSequence int64             `json:"execution_sequence"`
	InstalledCutoff   int64             `json:"installed_cutoff"`
	Contract          string            `json:"contract"`
	ObservedAt        time.Time         `json:"observed_at"`
}

// CheckpointNonAdmission can establish a new proof only for a currently resident
// Runtime epoch. Supervisor construction requires unused Services, and every
// Prepare intent is persisted at the same lock boundary before backend entry.
// An absent record from an older Runtime epoch remains unproven.
func (supervisor *Supervisor) CheckpointNonAdmission(ctx context.Context, authority *velav1.StageAuthority) (*ExecutionNonAdmissionCheckpoint, error) {
	if supervisor == nil || supervisor.floor == nil || ctx == nil {
		return nil, ErrExecutionNonAdmissionUnproven
	}
	service := supervisor.routeAuthority(authority)
	if service == nil {
		return nil, stageauthority.ErrRuntimeMismatch
	}
	verified, err := supervisor.floor.validator.ValidateEnvelopeForReplay(authority, service.maxClockSkew)
	if err != nil {
		return nil, err
	}
	if _, err := service.validator.ValidateSignature(verified.Authority, service.binding); err != nil {
		return nil, err
	}
	if err := supervisor.matchRetainedExecutionScope(verified.Authority); err != nil {
		return nil, err
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
	if admission.store == nil || verified.Authority.GetSchemaVersion() != stageauthority.SchemaVersionV2 ||
		proto.Size(verified.Authority) > maxExecutionWireBytes || admission.store.state.Floor < verified.Authority.GetExecutionSequence() {
		return nil, ErrExecutionNonAdmissionUnproven
	}
	store := admission.store
	if err := store.matchTerminalNonAdmissionAuthority(verified.Authority); err != nil {
		return nil, err
	}
	if saved, err := store.nonAdmissionCheckpoint(verified); err != nil || saved != nil {
		return saved, err
	}
	sequence := verified.Authority.GetExecutionSequence()
	// Any persisted intent at this sequence, even for a conflicting identity or
	// failed-before-entry operation, precludes an absence claim.
	for _, record := range store.state.Executions {
		original, err := store.retainedAuthority(record.Authority)
		if err != nil {
			return nil, err
		}
		if original.Authority.GetExecutionSequence() == sequence {
			return nil, ErrExecutionNonAdmissionUnproven
		}
	}
	for _, operation := range admission.active {
		if operation.sequence == sequence {
			return nil, ErrExecutionNonAdmissionUnproven
		}
	}
	if len(store.state.NonAdmissions) >= maxNonAdmissionCheckpoints {
		return nil, ErrExecutionNonAdmissionHistoryFull
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(verified.Authority)
	if err != nil {
		return nil, err
	}
	observed := service.clock.Now().UTC()
	if observed.IsZero() {
		return nil, ErrExecutionNonAdmissionUnproven
	}
	next := store.state
	next.NonAdmissions = append(slices.Clone(next.NonAdmissions), executionDiskNonAdmission{
		Authority: wire, AuthorityDigest: verified.Digest, ExecutionSequence: sequence,
		InstalledCutoff: next.Floor, Contract: ExecutionNonAdmissionContract, ObservedAt: observed,
	})
	slices.SortFunc(next.NonAdmissions, func(a, b executionDiskNonAdmission) int {
		return cmp.Compare(a.ExecutionSequence, b.ExecutionSequence)
	})
	if err := store.persist(next); err != nil {
		return nil, admission.failStateLocked(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return store.nonAdmissionCheckpoint(verified)
}

// InspectNonAdmission only replays an existing durable proof. It may cross Runtime
// epochs/profile retirement but never turns missing historical state into proof.
func (supervisor *Supervisor) InspectNonAdmission(ctx context.Context, authority *velav1.StageAuthority) (*ExecutionNonAdmissionCheckpoint, error) {
	if supervisor == nil || supervisor.floor == nil || ctx == nil {
		return nil, ErrExecutionNonAdmissionUnproven
	}
	verified, err := supervisor.floor.validator.ValidateEnvelopeForReplay(authority, supervisor.services[0].maxClockSkew)
	if err != nil {
		return nil, err
	}
	if verified.Authority.GetSchemaVersion() != stageauthority.SchemaVersionV2 || proto.Size(verified.Authority) > maxExecutionWireBytes {
		return nil, ErrExecutionNonAdmissionUnproven
	}
	if err := supervisor.matchRetainedExecutionScope(verified.Authority); err != nil {
		return nil, err
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
	if admission.store == nil {
		return nil, ErrExecutionNonAdmissionUnproven
	}
	return admission.store.nonAdmissionCheckpoint(verified)
}

func (store *executionStateFile) nonAdmissionCheckpoint(query stageauthority.Verified) (*ExecutionNonAdmissionCheckpoint, error) {
	for _, record := range store.state.NonAdmissions {
		if record.ExecutionSequence != query.Authority.GetExecutionSequence() {
			continue
		}
		original, err := store.retainedAuthority(record.Authority)
		if err != nil {
			return nil, err
		}
		if stageauthority.ValidateSameExecution(original.Authority, query.Authority) != nil {
			return nil, ErrExecutionNonAdmissionUnproven
		}
		return &ExecutionNonAdmissionCheckpoint{
			WorkerMemberID: store.scope.services[0].binding.WorkerMemberID, Authority: proto.Clone(original.Authority).(*velav1.StageAuthority),
			AuthorityDigest: record.AuthorityDigest, ExecutionSequence: record.ExecutionSequence,
			InstalledCutoff: record.InstalledCutoff, Contract: record.Contract, ObservedAt: record.ObservedAt,
		}, nil
	}
	return nil, nil
}

func (store *executionStateFile) validateNonAdmissions() error {
	if len(store.state.NonAdmissions) > maxNonAdmissionCheckpoints {
		return ErrExecutionNonAdmissionHistoryFull
	}
	var previous int64
	for _, record := range store.state.NonAdmissions {
		original, err := store.retainedAuthority(record.Authority)
		if err != nil {
			return err
		}
		if record.ExecutionSequence <= previous || record.ExecutionSequence != original.Authority.GetExecutionSequence() ||
			record.AuthorityDigest != original.Digest || record.InstalledCutoff < record.ExecutionSequence || record.InstalledCutoff > store.state.Floor ||
			record.Contract != ExecutionNonAdmissionContract || record.ObservedAt.IsZero() || record.ObservedAt.Location() != time.UTC {
			return ErrExecutionNonAdmissionUnproven
		}
		for _, admitted := range store.state.Executions {
			authority, err := store.retainedAuthority(admitted.Authority)
			if err != nil {
				return err
			}
			if authority.Authority.GetExecutionSequence() == record.ExecutionSequence {
				return ErrExecutionNonAdmissionUnproven
			}
		}
		previous = record.ExecutionSequence
	}
	return nil
}
