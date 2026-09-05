package stageworkeragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageassignment"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

var (
	ErrAdmissionClosed           = errors.New("assignment input admission is closed")
	ErrAdmissionRecoveryRequired = errors.New("assignment requires Runtime recovery before further admission")
	ErrAdmissionCapacity         = errors.New("assignment retirement history is at capacity")
)

type AssignmentAdmissionPhase string

const (
	AssignmentInputsPending  AssignmentAdmissionPhase = "INPUTS_PENDING"
	AssignmentRuntimeEntered AssignmentAdmissionPhase = "RUNTIME_ENTERED"
	AssignmentClosed         AssignmentAdmissionPhase = "CLOSED"
)

// AdmissionRuntimeBinding is trusted configuration/discovery, not assignment data.
type AdmissionRuntimeBinding struct {
	Runtime        stageauthority.RuntimeBinding
	IdentityDigest [sha256.Size]byte
}

type AssignmentAdmissionConfig struct {
	Directory           string
	InputRoot           string
	OutputRoot          string
	WorkerInstanceID    uuid.UUID
	WorkerInstanceEpoch int64
	WorkerMemberID      uuid.UUID
	Validator           *stageauthority.Validator
	Bindings            []AdmissionRuntimeBinding
	MaxRecords          int
	MaxClockSkew        time.Duration
}

// AssignmentAdmissionRecord keeps original lookup evidence without delivery content.
// CLOSED forbids execution reentry; it is never a writer-drain checkpoint.
type AssignmentAdmissionRecord struct {
	AcquireCommandID uuid.UUID
	Phase            AssignmentAdmissionPhase
	Original         *velav1.StageAuthority
	Latest           *velav1.StageAuthority
}

type AssignmentAdmissionSnapshot struct {
	Watermark int64
	Latest    *AssignmentAdmissionRecord
	Pending   []AssignmentAdmissionRecord
}

type FileAssignmentAdmission struct {
	mu        sync.Mutex
	files     *assignmentAdmissionFiles
	state     assignmentAdmissionState
	validator *stageauthority.Validator
	bindings  []AdmissionRuntimeBinding
	maxSkew   time.Duration
	active    *AssignmentAdmission
	failed    error
}

// AssignmentAdmission owns an in-process input writer slot. Release is called
// only after Resolve/download tasks have returned and closed their handles.
type AssignmentAdmission struct {
	gate      *FileAssignmentAdmission
	authority *velav1.StageAuthority
	identity  [sha256.Size]byte
	done      chan struct{}
}

func NewFileAssignmentAdmission(config AssignmentAdmissionConfig) (*FileAssignmentAdmission, error) {
	if config.WorkerInstanceID == uuid.Nil || config.WorkerInstanceEpoch <= 0 || config.WorkerMemberID == uuid.Nil ||
		config.Validator == nil || len(config.Bindings) == 0 || len(config.Bindings) > 16*64 ||
		config.MaxRecords < 1 || config.MaxRecords > 64 || config.MaxClockSkew < 0 || config.MaxClockSkew > time.Minute {
		return nil, errors.New("assignment admission configuration is invalid")
	}
	bindings := make([]AdmissionRuntimeBinding, 0, len(config.Bindings))
	for _, binding := range config.Bindings {
		if binding.Runtime.WorkerInstanceID != config.WorkerInstanceID.String() || binding.Runtime.WorkerInstanceEpoch != config.WorkerInstanceEpoch ||
			binding.Runtime.WorkerMemberID == "" || binding.IdentityDigest == ([sha256.Size]byte{}) {
			return nil, errors.New("assignment admission Runtime binding is invalid")
		}
		binding.Runtime.Devices = slices.Clone(binding.Runtime.Devices)
		binding.Runtime.Members = slices.Clone(binding.Runtime.Members)
		binding.Runtime.DeviceSetDigest = slices.Clone(binding.Runtime.DeviceSetDigest)
		binding.Runtime.MembershipDigest = slices.Clone(binding.Runtime.MembershipDigest)
		bindings = append(bindings, binding)
	}
	files, state, err := openAssignmentAdmissionFiles(config)
	if err != nil {
		return nil, err
	}
	gate := &FileAssignmentAdmission{files: files, state: state, validator: config.Validator, bindings: bindings, maxSkew: config.MaxClockSkew}
	if err := gate.validateState(state); err != nil {
		_ = files.close()
		return nil, err
	}
	return gate, nil
}

func (gate *FileAssignmentAdmission) Begin(ctx context.Context, assignment *velav1.StageAssignment, acquireID uuid.UUID) (*AssignmentAdmission, error) {
	if gate == nil || ctx == nil || acquireID == uuid.Nil {
		return nil, errors.New("assignment admission requires a context and original Acquire identity")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := stageassignment.Validate(assignment); err != nil {
		return nil, err
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return nil, err
	}
	if gate.active != nil {
		return nil, ErrStageWorkerBusy
	}
	verified, err := gate.verifyCurrent(assignment.GetAuthority())
	if err != nil {
		return nil, err
	}
	identity, err := assignmentExecutionIdentity(verified.Authority)
	if err != nil {
		return nil, err
	}
	wire, err := admissionAuthorityWire(verified.Authority)
	if err != nil {
		return nil, err
	}
	next := cloneAdmissionState(gate.state)
	sequence := verified.Authority.GetExecutionSequence()
	if sequence < next.Watermark {
		return nil, ErrAdmissionClosed
	}
	if sequence == next.Watermark {
		if next.Latest == nil || next.Latest.Identity != identity || next.Latest.AcquireCommandID != acquireID || next.Latest.Phase == AssignmentClosed {
			return nil, ErrAdmissionClosed
		}
		if next.Latest.Phase == AssignmentRuntimeEntered {
			return nil, ErrAdmissionRecoveryRequired
		}
		latest, err := gate.decodeAuthority(next.Latest.LatestWire)
		if err != nil {
			return nil, err
		}
		if !proto.Equal(latest, verified.Authority) {
			if err := stageauthority.ValidateRenewal(latest, verified.Authority); err != nil {
				return nil, err
			}
			next.Latest.LatestWire = wire
		}
	} else {
		if next.Latest != nil {
			if next.Latest.Phase == AssignmentRuntimeEntered {
				return nil, ErrAdmissionRecoveryRequired
			}
			previous := *next.Latest
			previous.Phase = AssignmentClosed
			next.Pending = append(next.Pending, previous)
		}
		if len(next.Pending) >= next.MaxRecords {
			return nil, ErrAdmissionCapacity
		}
		next.Watermark = sequence
		next.Latest = &assignmentAdmissionEntry{AcquireCommandID: acquireID, Identity: identity, Phase: AssignmentInputsPending, OriginalWire: wire, LatestWire: wire}
	}
	if err := gate.commit(ctx, next); err != nil {
		return nil, err
	}
	handle := &AssignmentAdmission{gate: gate, authority: verified.Authority, identity: identity, done: make(chan struct{})}
	gate.active = handle
	return handle, nil
}

func (handle *AssignmentAdmission) EnterRuntime(ctx context.Context) error {
	if handle == nil || handle.gate == nil || ctx == nil {
		return ErrAdmissionClosed
	}
	gate := handle.gate
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return err
	}
	if gate.active != handle || gate.state.Latest == nil || gate.state.Latest.Identity != handle.identity || gate.state.Latest.Phase != AssignmentInputsPending {
		return ErrAdmissionClosed
	}
	// Input resolution and waiting for the lock can outlive the execution window.
	if _, err := gate.verifyCurrent(handle.authority); err != nil {
		return err
	}
	next := cloneAdmissionState(gate.state)
	next.Latest.Phase = AssignmentRuntimeEntered
	return gate.commit(ctx, next)
}

// CloseExecution closes both initial and renewal envelopes of the same immutable
// execution. It does not cancel/join writers, release the handle or authorize cleanup.
func (gate *FileAssignmentAdmission) CloseExecution(ctx context.Context, authority *velav1.StageAuthority) error {
	if gate == nil || ctx == nil {
		return ErrAdmissionClosed
	}
	verified, err := gate.validator.ValidateEnvelopeForReplay(authority, gate.maxSkew)
	if err != nil {
		return err
	}
	identity, err := assignmentExecutionIdentity(verified.Authority)
	if err != nil {
		return err
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return err
	}
	if gate.state.Latest == nil || gate.state.Latest.Identity != identity {
		return ErrAdmissionClosed
	}
	if gate.state.Latest.Phase == AssignmentClosed {
		return nil
	}
	next := cloneAdmissionState(gate.state)
	next.Latest.Phase = AssignmentClosed
	return gate.commit(ctx, next)
}

func (handle *AssignmentAdmission) Release() {
	if handle == nil || handle.gate == nil {
		return
	}
	gate := handle.gate
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.active == handle {
		gate.active = nil
		close(handle.done)
	}
}

func (handle *AssignmentAdmission) WaitReleased(ctx context.Context) error {
	if handle == nil || handle.done == nil || ctx == nil {
		return ErrAdmissionClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-handle.done:
		return nil
	}
}

func (gate *FileAssignmentAdmission) Snapshot(ctx context.Context) (AssignmentAdmissionSnapshot, error) {
	if gate == nil || ctx == nil {
		return AssignmentAdmissionSnapshot{}, ErrAdmissionClosed
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return AssignmentAdmissionSnapshot{}, err
	}
	result := AssignmentAdmissionSnapshot{Watermark: gate.state.Watermark}
	if gate.state.Latest != nil {
		record, err := gate.record(*gate.state.Latest)
		if err != nil {
			return AssignmentAdmissionSnapshot{}, err
		}
		result.Latest = &record
	}
	for _, entry := range gate.state.Pending {
		record, err := gate.record(entry)
		if err != nil {
			return AssignmentAdmissionSnapshot{}, err
		}
		result.Pending = append(result.Pending, record)
	}
	return result, nil
}

func (gate *FileAssignmentAdmission) Close() error {
	if gate == nil {
		return nil
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.active != nil {
		return ErrStageWorkerBusy
	}
	if gate.files == nil {
		return nil
	}
	err := gate.files.close()
	gate.files = nil
	return err
}

func (gate *FileAssignmentAdmission) verifyCurrent(authority *velav1.StageAuthority) (stageauthority.Verified, error) {
	verified, err := gate.validator.ValidateEnvelopeWithClockSkew(authority, gate.maxSkew)
	if err != nil {
		return stageauthority.Verified{}, err
	}
	if verified.Authority.GetSchemaVersion() != stageauthority.SchemaVersionV2 {
		return stageauthority.Verified{}, ErrAdmissionClosed
	}
	for _, member := range verified.Authority.GetMembers() {
		matches := 0
		for _, binding := range gate.bindings {
			if binding.Runtime.WorkerMemberID != member.GetWorkerMemberId() || !bytes.Equal(binding.IdentityDigest[:], member.GetIdentityDigest()) {
				continue
			}
			if _, err := gate.validator.ValidateWithClockSkew(verified.Authority, binding.Runtime, gate.maxSkew); err == nil {
				matches++
			}
		}
		if matches != 1 {
			return stageauthority.Verified{}, stageauthority.ErrRuntimeMismatch
		}
	}
	return verified, nil
}

func (gate *FileAssignmentAdmission) available(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if gate.files == nil {
		return ErrAdmissionClosed
	}
	if gate.failed != nil {
		return gate.failed
	}
	if err := gate.files.validateBinding(); err != nil {
		gate.failed = err
		return err
	}
	return nil
}

func (gate *FileAssignmentAdmission) commit(ctx context.Context, next assignmentAdmissionState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := gate.validateState(next); err != nil {
		return err
	}
	if err := gate.files.persist(next); err != nil {
		gate.failed = fmt.Errorf("assignment admission persistence requires recovery: %w", err)
		return gate.failed
	}
	gate.state = next
	return nil
}

func (gate *FileAssignmentAdmission) record(entry assignmentAdmissionEntry) (AssignmentAdmissionRecord, error) {
	original, err := gate.decodeAuthority(entry.OriginalWire)
	if err != nil {
		return AssignmentAdmissionRecord{}, err
	}
	latest, err := gate.decodeAuthority(entry.LatestWire)
	if err != nil {
		return AssignmentAdmissionRecord{}, err
	}
	return AssignmentAdmissionRecord{AcquireCommandID: entry.AcquireCommandID, Phase: entry.Phase, Original: original, Latest: latest}, nil
}

func (gate *FileAssignmentAdmission) decodeAuthority(wire []byte) (*velav1.StageAuthority, error) {
	if len(wire) == 0 || len(wire) > maxAdmissionAuthorityBytes {
		return nil, ErrAdmissionClosed
	}
	var authority velav1.StageAuthority
	if err := proto.Unmarshal(wire, &authority); err != nil {
		return nil, ErrAdmissionClosed
	}
	verified, err := gate.validator.ValidateEnvelopeForReplay(&authority, gate.maxSkew)
	if err != nil {
		return nil, err
	}
	canonical, err := admissionAuthorityWire(verified.Authority)
	if err != nil || !bytes.Equal(wire, canonical) || verified.Authority.GetSchemaVersion() != stageauthority.SchemaVersionV2 {
		return nil, ErrAdmissionClosed
	}
	return verified.Authority, nil
}

func (gate *FileAssignmentAdmission) validateState(state assignmentAdmissionState) error {
	if state.SchemaVersion != 1 || state.ID == uuid.Nil || state.WorkerInstanceID == uuid.Nil || state.WorkerInstanceEpoch <= 0 || state.WorkerMemberID == uuid.Nil ||
		state.MaxRecords < 1 || state.MaxRecords > 64 || state.Watermark < 0 || len(state.Pending) >= state.MaxRecords ||
		(state.Latest == nil && (state.Watermark != 0 || len(state.Pending) != 0)) {
		return errors.New("assignment admission state is invalid")
	}
	entries := slices.Clone(state.Pending)
	if state.Latest != nil {
		entries = append(entries, *state.Latest)
	}
	var previous int64
	for index, entry := range entries {
		record, err := gate.record(entry)
		if err != nil {
			return err
		}
		identity, err := assignmentExecutionIdentity(record.Original)
		sequence := record.Original.GetExecutionSequence()
		if err != nil || entry.AcquireCommandID == uuid.Nil || identity != entry.Identity || sequence <= previous ||
			record.Original.GetWorkerInstanceId() != state.WorkerInstanceID.String() || record.Original.GetWorkerInstanceEpoch() != state.WorkerInstanceEpoch {
			return errors.New("assignment admission execution history is invalid")
		}
		memberFound := false
		for _, member := range record.Original.GetMembers() {
			memberFound = memberFound || member.GetWorkerMemberId() == state.WorkerMemberID.String()
		}
		if !memberFound {
			return errors.New("assignment admission member is absent from history")
		}
		if !proto.Equal(record.Original, record.Latest) && stageauthority.ValidateRenewal(record.Original, record.Latest) != nil {
			return errors.New("assignment admission renewal history is invalid")
		}
		if index < len(state.Pending) && entry.Phase != AssignmentClosed {
			return ErrAdmissionClosed
		}
		switch entry.Phase {
		case AssignmentInputsPending, AssignmentRuntimeEntered, AssignmentClosed:
		default:
			return ErrAdmissionClosed
		}
		previous = sequence
	}
	if previous != state.Watermark {
		return ErrAdmissionClosed
	}
	return nil
}

func assignmentExecutionIdentity(authority *velav1.StageAuthority) ([sha256.Size]byte, error) {
	if _, err := stageauthority.Digest(authority); err != nil {
		return [sha256.Size]byte{}, err
	}
	canonical := proto.Clone(authority).(*velav1.StageAuthority)
	canonical.StageVersion, canonical.SigningKeyId = 0, ""
	canonical.IssuedAt, canonical.ExpiresAt, canonical.MonotonicValidFor, canonical.Signature = nil, nil, nil, nil
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(canonical)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(wire), nil
}

func admissionAuthorityWire(authority *velav1.StageAuthority) ([]byte, error) {
	if proto.Size(authority) > maxAdmissionAuthorityBytes {
		return nil, errors.New("assignment admission authority exceeds its bound")
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(authority)
}
