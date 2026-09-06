package stageworkeragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
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
	Runtime            stageauthority.RuntimeBinding
	IdentityDigest     [sha256.Size]byte
	DeviceSubsetDigest [sha256.Size]byte
}

type AssignmentAdmissionConfig struct {
	// Initialize is a separate, explicitly authorized first bootstrap. Recovery
	// must leave it false even when every local directory has been replaced.
	Initialize bool
	// All upgrades require a retained signed floor proving the original topology.
	// Empty legacy journals cannot establish it from replacement configuration.
	// UpgradeV2 preserves schema-2 history without inventing input drain evidence.
	UpgradeV2 bool
	// UpgradeV3 preserves schema-3 history without creating retirement evidence.
	UpgradeV3 bool
	// UpgradeV4 preserves schema-4 history including retirement evidence.
	UpgradeV4           bool
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
	RegistryBinding     *velav1.WorkerBootstrapBinding
	RegistryVerifier    *journalbinding.Verifier
	// DeferRuntimeRoutes holds journal ownership during startup discovery while
	// refusing execution admission until BindRuntimeRoutes succeeds once.
	DeferRuntimeRoutes bool
}

// AssignmentAdmissionRecord keeps original lookup evidence without delivery content.
// CLOSED forbids execution reentry; it is never a writer-drain checkpoint.
type AssignmentAdmissionRecord struct {
	AcquireCommandID uuid.UUID
	Phase            AssignmentAdmissionPhase
	Original         *velav1.StageAuthority
	Latest           *velav1.StageAuthority
	InputDrain       *AssignmentInputDrainCheckpoint
}

type AssignmentAdmissionSnapshot struct {
	Watermark   int64
	Floor       int64
	Disposition *velav1.StageTerminalDisposition
	Latest      *AssignmentAdmissionRecord
	Pending     []AssignmentAdmissionRecord
	Retirements []TerminalRetirementSnapshot
}

type FileAssignmentAdmission struct {
	mu                         sync.Mutex
	files                      *assignmentAdmissionFiles
	state                      assignmentAdmissionState
	validator                  *stageauthority.Validator
	bindings                   []AdmissionRuntimeBinding
	routesBound                bool
	scope                      assignmentAdmissionScope
	scopeDigest                [sha256.Size]byte
	maxSkew                    time.Duration
	active                     *AssignmentAdmission
	failed                     error
	registryBinding            *velav1.WorkerBootstrapBinding
	registryVerifier           *journalbinding.Verifier
	retirementAfterDirectory   func(int) error
	retirementSyncAbsentParent func(*os.Root) error
}

// AssignmentAdmission owns an in-process input writer slot. Release is called
// only after Resolve/download tasks have returned and closed their handles.
type AssignmentAdmission struct {
	gate       *FileAssignmentAdmission
	authority  *velav1.StageAuthority
	identity   [sha256.Size]byte
	done       chan struct{}
	inputsDone chan struct{}
}

func NewFileAssignmentAdmission(config AssignmentAdmissionConfig) (*FileAssignmentAdmission, error) {
	if (config.RegistryBinding == nil) != (config.RegistryVerifier == nil) {
		return nil, errors.New("assignment journal Registry binding and verifier must be configured together")
	}
	if config.RegistryBinding != nil {
		if config.Initialize || config.UpgradeV2 || config.UpgradeV3 || config.UpgradeV4 {
			return nil, errors.New("assignment journal Registry binding requires recovery without initialization or upgrade")
		}
		verified, err := config.RegistryVerifier.Verify(config.RegistryBinding)
		if err != nil {
			return nil, err
		}
		config.RegistryBinding = verified
	}
	upgrades := 0
	for _, selected := range []bool{config.UpgradeV2, config.UpgradeV3, config.UpgradeV4} {
		if selected {
			upgrades++
		}
	}
	if config.Initialize && upgrades > 0 || upgrades > 1 {
		return nil, errors.New("assignment admission upgrade cannot initialize state")
	}
	if config.WorkerInstanceID == uuid.Nil || config.WorkerInstanceEpoch <= 0 || config.WorkerMemberID == uuid.Nil ||
		config.Validator == nil || len(config.Bindings) == 0 || len(config.Bindings) > 16*64 ||
		config.MaxRecords < 1 || config.MaxRecords > 64 || config.MaxClockSkew < 0 || config.MaxClockSkew > time.Minute {
		return nil, errors.New("assignment admission configuration is invalid")
	}
	bindings, err := cloneAdmissionRuntimeBindings(config.WorkerInstanceID.String(), config.WorkerInstanceEpoch, config.Bindings)
	if err != nil {
		return nil, err
	}
	scope, err := newAssignmentAdmissionScope(config, bindings)
	if err != nil {
		return nil, err
	}
	scopeDigest, err := scope.digest()
	if err != nil {
		return nil, err
	}
	files, state, err := openAssignmentAdmissionFiles(config, scopeDigest)
	if err != nil {
		return nil, err
	}
	gate := &FileAssignmentAdmission{files: files, state: state, validator: config.Validator, bindings: bindings,
		routesBound: !config.DeferRuntimeRoutes, scope: scope, scopeDigest: scopeDigest, maxSkew: config.MaxClockSkew,
		registryBinding: config.RegistryBinding, registryVerifier: config.RegistryVerifier}
	upgrade := config.UpgradeV2 && state.SchemaVersion == 2 || config.UpgradeV3 && state.SchemaVersion == 3 || config.UpgradeV4 && state.SchemaVersion == 4
	if upgrade {
		if state.SchemaVersion < 4 && len(state.Retirements) != 0 {
			_ = files.close()
			return nil, errors.New("legacy assignment admission cannot contain retirement evidence")
		}
		for _, entry := range admissionEntries(state) {
			if state.SchemaVersion == 2 && entry.InputDrain != nil {
				_ = files.close()
				return nil, errors.New("schema-2 assignment admission cannot contain input drain proof")
			}
		}
		if len(state.Scope) != 0 || state.Floor <= 0 {
			_ = files.close()
			return nil, errors.New("legacy assignment admission requires a retained signed topology witness")
		}
		if _, err := gate.decodeFloor(state); err != nil {
			_ = files.close()
			return nil, err
		}
		state.SchemaVersion, state.Scope = 5, bytes.Clone(scopeDigest[:])
	}
	if err := gate.validateState(state); err != nil {
		_ = files.close()
		return nil, err
	}
	if config.RegistryBinding != nil {
		if err := gate.verifyRegistryJournal(); err != nil {
			_ = files.close()
			return nil, fmt.Errorf("match locked assignment journal to Registry: %w", err)
		}
	}
	if !config.Initialize {
		if err := files.recoverDurability(); err != nil {
			_ = files.close()
			return nil, fmt.Errorf("recover assignment admission durability: %w", err)
		}
	}
	if upgrade {
		if err := files.persist(state); err != nil {
			_ = files.close()
			return nil, fmt.Errorf("upgrade assignment admission: %w", err)
		}
		gate.state = state
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
		if next.Latest.InputDrain == nil {
			return nil, ErrInputWritersUnproven
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
		next.Latest.InputDrain = nil
	} else {
		if next.Latest != nil {
			if next.Latest.Phase == AssignmentRuntimeEntered {
				return nil, ErrAdmissionRecoveryRequired
			}
			if next.Latest.InputDrain == nil {
				return nil, ErrInputWritersUnproven
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
	for _, pending := range next.Pending {
		if pending.InputDrain == nil {
			return nil, ErrInputWritersUnproven
		}
	}
	if err := gate.commit(ctx, next); err != nil {
		return nil, err
	}
	handle := &AssignmentAdmission{gate: gate, authority: verified.Authority, identity: identity, done: make(chan struct{}), inputsDone: make(chan struct{})}
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
	if gate.state.Latest.InputDrain == nil {
		return ErrInputWritersUnproven
	}
	next := cloneAdmissionState(gate.state)
	next.Latest.Phase = AssignmentRuntimeEntered
	return gate.commit(ctx, next)
}

// ObserveRuntimeAuthority persists a permitted renewal before a Runtime RPC can
// install it. It cannot create an execution or reopen a closed admission.
func (gate *FileAssignmentAdmission) ObserveRuntimeAuthority(ctx context.Context, authority *velav1.StageAuthority) error {
	if gate == nil || ctx == nil {
		return ErrAdmissionClosed
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if err := gate.available(ctx); err != nil {
		return err
	}
	verified, err := gate.verifyCurrent(authority)
	if err != nil {
		return err
	}
	identity, err := assignmentExecutionIdentity(verified.Authority)
	if err != nil {
		return err
	}
	if gate.state.Latest == nil || gate.state.Latest.Identity != identity || gate.state.Latest.Phase != AssignmentRuntimeEntered {
		return ErrAdmissionClosed
	}
	latest, err := gate.decodeAuthority(gate.state.Latest.LatestWire)
	if err != nil {
		return err
	}
	if proto.Equal(latest, verified.Authority) {
		return nil
	}
	if err := stageauthority.ValidateRenewal(latest, verified.Authority); err != nil {
		return err
	}
	wire, err := admissionAuthorityWire(verified.Authority)
	if err != nil {
		return err
	}
	next := cloneAdmissionState(gate.state)
	next.Latest.LatestWire = wire
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
		for _, entry := range gate.state.Pending {
			if entry.Identity == identity && entry.Phase == AssignmentClosed {
				return nil
			}
		}
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
	result := AssignmentAdmissionSnapshot{Watermark: gate.state.Watermark, Floor: gate.state.Floor}
	if gate.state.Floor > 0 {
		var err error
		result.Disposition, err = gate.decodeFloor(gate.state)
		if err != nil {
			return AssignmentAdmissionSnapshot{}, err
		}
	}
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
	for _, entry := range gate.state.Retirements {
		result.Retirements = append(result.Retirements, retirementSnapshot(entry))
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
	if !gate.routesBound {
		return stageauthority.Verified{}, ErrAdmissionClosed
	}
	verified, err := gate.validator.ValidateEnvelopeWithClockSkew(authority, gate.maxSkew)
	if err != nil {
		return stageauthority.Verified{}, err
	}
	if verified.Authority.GetSchemaVersion() != stageauthority.SchemaVersionV2 {
		return stageauthority.Verified{}, ErrAdmissionClosed
	}
	if err := gate.scope.matchAuthority(verified.Authority); err != nil {
		return stageauthority.Verified{}, err
	}
	for _, retirement := range gate.state.Retirements {
		if retirement.StageRunID.String() == verified.Authority.GetStageRunId() {
			return stageauthority.Verified{}, ErrAdmissionClosed
		}
	}
	if verified.Authority.GetExecutionSequence() <= gate.state.Floor {
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
	var inputDrain *AssignmentInputDrainCheckpoint
	if entry.InputDrain != nil {
		copy := *entry.InputDrain
		inputDrain = &copy
	}
	return AssignmentAdmissionRecord{AcquireCommandID: entry.AcquireCommandID, Phase: entry.Phase, Original: original, Latest: latest, InputDrain: inputDrain}, nil
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
	if state.SchemaVersion != 5 || !bytes.Equal(state.Scope, gate.scopeDigest[:]) || state.ID == uuid.Nil || state.WorkerInstanceID == uuid.Nil || state.WorkerInstanceEpoch <= 0 || state.WorkerMemberID == uuid.Nil ||
		state.MaxRecords < 1 || state.MaxRecords > 64 || state.Watermark < 0 || len(state.Pending) >= state.MaxRecords ||
		(state.Latest == nil && (state.Watermark != 0 || len(state.Pending) != 0)) {
		return errors.New("assignment admission state is invalid")
	}
	if state.Floor < 0 || (state.Floor == 0) != (len(state.FloorWire) == 0) {
		return errors.New("assignment admission floor witness is invalid")
	}
	if state.Floor > 0 {
		if _, err := gate.decodeFloor(state); err != nil {
			return err
		}
	}
	entries := admissionEntries(state)
	var previous int64
	for index, entry := range entries {
		if entry.InputDrain != nil && (entry.InputDrain.Contract != AssignmentInputDrainContract || entry.InputDrain.ObservedAt.IsZero() || entry.InputDrain.ObservedAt.Location() != time.UTC) {
			return ErrInputWritersUnproven
		}
		record, err := gate.record(entry)
		if err != nil {
			return err
		}
		if err := gate.scope.matchAuthority(record.Original); err != nil {
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
	return gate.validateRetirements(state)
}

func admissionEntries(state assignmentAdmissionState) []assignmentAdmissionEntry {
	entries := slices.Clone(state.Pending)
	if state.Latest != nil {
		entries = append(entries, *state.Latest)
	}
	return entries
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
