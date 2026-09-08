package modelruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

type JournalCallerRole string

const (
	JournalRuntimeRole JournalCallerRole = "RUNTIME"
	JournalWorkerRole  JournalCallerRole = "WORKER"
)

// ExecutionJournalOwnerConfig is trusted Node assembly. Routes must be the
// approved live epochs, not epochs learned from a command. State initialization
// and adoption still require independently established Registry/Node provenance.
type ExecutionJournalOwnerConfig struct {
	Manifest     LaunchManifest
	Validator    *stageauthority.Validator
	State        ExecutionFloorStateConfig
	Routes       []stageauthority.RuntimeBinding
	MaxClockSkew time.Duration
	Now          func() time.Time
}

// JournalMutationReceipt acknowledges restrictions/history only. It grants no
// backend startup, execution time, writer exclusion or scratch deletion rights.
// Replayed history is not permission to dispatch an operation a second time.
type JournalMutationReceipt struct {
	SchemaVersion int               `json:"schema_version"`
	RequestDigest [sha256.Size]byte `json:"request_digest"`
	JournalID     uuid.UUID         `json:"journal_id"`
	JournalScope  [sha256.Size]byte `json:"journal_scope"`
	StateDigest   [sha256.Size]byte `json:"state_digest"`
	Highest       int64             `json:"highest"`
	Floor         int64             `json:"floor"`
	Replayed      bool              `json:"replayed"`
}

// ExecutionJournalOwner exclusively owns a journal, independently of a backend.
// It is not an authenticator or a replacement for the live Runtime state machine.
type ExecutionJournalOwner struct {
	mu       sync.Mutex
	store    *executionStateFile
	manifest LaunchManifest
	routes   []executionJournalRoute
	now      func() time.Time
	failed   error
}

func OpenExecutionJournalOwner(config ExecutionJournalOwnerConfig) (*ExecutionJournalOwner, error) {
	if config.Now == nil || config.MaxClockSkew < 0 || config.MaxClockSkew > time.Minute {
		return nil, ErrJournalCommand
	}
	if _, err := EncodeLaunchManifest(config.Manifest); err != nil {
		return nil, err
	}
	manifest := cloneLaunchManifest(config.Manifest)
	scope, err := executionScopeForManifest(manifest, config.Validator)
	if err != nil {
		return nil, err
	}
	bindings, err := manifest.RuntimeBindings()
	if err != nil || len(bindings) != len(config.Routes) {
		return nil, ErrJournalCommand
	}
	owner := &ExecutionJournalOwner{manifest: manifest, now: config.Now}
	for i, binding := range bindings {
		route := cloneBinding(config.Routes[i])
		binding.ModelRuntimeEpoch = route.ModelRuntimeEpoch
		if route.ModelRuntimeEpoch <= 0 || route.ModelRuntimeEpoch < manifest.Runtimes[i].ModelRuntimeEpochFloor || !reflect.DeepEqual(binding, route) {
			return nil, stageauthority.ErrRuntimeMismatch
		}
		owner.routes = append(owner.routes, executionJournalRoute{binding: route, maxClockSkew: config.MaxClockSkew})
	}
	owner.store, err = openExecutionState(config.State, scope)
	if err != nil {
		return nil, err
	}
	return owner, nil
}

// RecordBackendStartupIntent is a trusted orchestration call, not a wire
// command. It does not clear an unresolved prior incarnation or issue a permit.
func (owner *ExecutionJournalOwner) RecordBackendStartupIntent(ctx context.Context) (BackendLifecycleStatus, error) {
	if owner == nil || ctx == nil {
		return BackendLifecycleStatus{}, ErrJournalCommand
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err := owner.check(ctx); err != nil {
		return BackendLifecycleStatus{}, err
	}
	if err := owner.store.transition(func(draft *executionJournalDraft) error {
		if owner.store.recoveryDrain {
			return ErrBackendIncarnationUnproven
		}
		return draft.recordBackendStartup(owner.manifest, uuid.New(), owner.now().UTC())
	}); err != nil {
		return BackendLifecycleStatus{}, owner.mutationError(err)
	}
	return *owner.store.state.BackendLifecycle, context.Cause(ctx)
}

func (owner *ExecutionJournalOwner) Status(ctx context.Context) (ExecutionJournalStatus, error) {
	if owner == nil || ctx == nil {
		return ExecutionJournalStatus{}, ErrJournalCommand
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err := owner.check(ctx); err != nil {
		return ExecutionJournalStatus{}, err
	}
	return owner.store.status(), nil
}

// InspectStartup compares the held journal and its actual routes with an
// independently approved manifest and first-startup declaration. It grants no
// startup permission and does not authenticate the process or filesystem owner.
func (owner *ExecutionJournalOwner) InspectStartup(ctx context.Context, manifest LaunchManifest, request BackendStartupRequest) (ExecutionJournalSnapshot, error) {
	if owner == nil || ctx == nil {
		return ExecutionJournalSnapshot{}, ErrJournalCommand
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err := owner.check(ctx); err != nil {
		return ExecutionJournalSnapshot{}, err
	}
	expected, err := EncodeLaunchManifest(manifest)
	if err != nil {
		return ExecutionJournalSnapshot{}, err
	}
	actual, err := EncodeLaunchManifest(owner.manifest)
	if err != nil || !bytes.Equal(actual, expected) {
		return ExecutionJournalSnapshot{}, ErrBackendStartupDenied
	}
	bindings, err := RemoteStartupBindings(manifest)
	if err != nil || len(bindings) != len(owner.routes) {
		return ExecutionJournalSnapshot{}, ErrBackendStartupDenied
	}
	for i, binding := range bindings {
		if !reflect.DeepEqual(binding, owner.routes[i].binding) {
			return ExecutionJournalSnapshot{}, stageauthority.ErrRuntimeMismatch
		}
	}
	state := owner.store.state
	// Reuse full snapshot validation, including every signature/history invariant.
	document, err := json.Marshal(state)
	if err != nil {
		return ExecutionJournalSnapshot{}, err
	}
	if sha256.Sum256(document) != owner.store.stateDigest {
		return ExecutionJournalSnapshot{}, ErrExecutionStateRecovery
	}
	snapshot := ExecutionJournalSnapshot{status: owner.store.status(), digest: owner.store.stateDigest,
		launchDigest: sha256.Sum256(expected), nonAdmissions: len(state.NonAdmissions),
		terminalNonAdmissions: len(state.TerminalNonAdmissions), verified: true}
	if err := errors.Join(owner.store.validateProofs(), snapshot.MatchStartup(request), context.Cause(ctx)); err != nil {
		return ExecutionJournalSnapshot{}, err
	}
	return snapshot, nil
}

func (owner *ExecutionJournalOwner) Apply(ctx context.Context, role JournalCallerRole, wire []byte) (JournalMutationReceipt, error) {
	if owner == nil || ctx == nil {
		return JournalMutationReceipt{}, ErrJournalCommand
	}
	command, err := ParseJournalCommand(wire)
	if err != nil {
		return JournalMutationReceipt{}, err
	}
	if command.Read != nil {
		return JournalMutationReceipt{}, ErrJournalCommand
	}
	workerOperation := command.Floor != nil || command.NonAdmission != nil || command.TerminalNonAdmission != nil
	if workerOperation && role != JournalWorkerRole || !workerOperation && role != JournalRuntimeRole {
		return JournalMutationReceipt{}, ErrJournalCommand
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err := owner.check(ctx); err != nil {
		return JournalMutationReceipt{}, err
	}
	before := owner.store.stateDigest
	if err := owner.store.transition(func(draft *executionJournalDraft) error { return owner.apply(draft, command) }); err != nil {
		return JournalMutationReceipt{}, owner.mutationError(err)
	}
	if err := context.Cause(ctx); err != nil {
		return JournalMutationReceipt{}, err // durable outcome is uncertain to the caller
	}
	return JournalMutationReceipt{SchemaVersion: 1, RequestDigest: sha256.Sum256(wire), JournalID: owner.store.state.ID,
		JournalScope: owner.store.state.Scope, StateDigest: owner.store.stateDigest, Highest: owner.store.state.Highest,
		Floor: owner.store.state.Floor, Replayed: before == owner.store.stateDigest}, nil
}

func (owner *ExecutionJournalOwner) check(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if owner.store == nil {
		return ErrExecutionStateRecovery
	}
	if owner.failed != nil {
		return owner.failed
	}
	if err := owner.store.check(); err != nil {
		owner.failed = errors.Join(ErrExecutionStateRecovery, err)
	}
	return owner.failed
}

func (owner *ExecutionJournalOwner) mutationError(err error) error {
	if !isExecutionJournalRejection(err) {
		owner.failed = errors.Join(ErrExecutionStateRecovery, err)
		return owner.failed
	}
	return err
}

func (owner *ExecutionJournalOwner) Close() error {
	if owner == nil {
		return nil
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.store == nil {
		return nil
	}
	err := owner.store.close()
	owner.store = nil
	return err
}

func decodeJournalProto(wire []byte, message proto.Message) error {
	if len(wire) == 0 || len(wire) > maxExecutionWireBytes || proto.Unmarshal(wire, message) != nil {
		return ErrJournalCommand
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil || !bytes.Equal(wire, canonical) {
		return ErrJournalCommand
	}
	return nil
}

func (owner *ExecutionJournalOwner) authority(draft *executionJournalDraft, wire []byte) (stageauthority.Verified, executionJournalRoute, error) {
	var authority velav1.StageAuthority
	if err := decodeJournalProto(wire, &authority); err != nil {
		return stageauthority.Verified{}, executionJournalRoute{}, err
	}
	verified, err := draft.verifyMutationAuthority(stageauthority.Verified{Authority: &authority})
	if err != nil {
		return verified, executionJournalRoute{}, err
	}
	for _, route := range owner.routes {
		if _, err := draft.scope.floor.validator.ValidateSignature(verified.Authority, route.binding); err == nil {
			return verified, route, nil
		}
	}
	return verified, executionJournalRoute{}, stageauthority.ErrRuntimeMismatch
}

func (owner *ExecutionJournalOwner) apply(draft *executionJournalDraft, command JournalCommand) error {
	if command.Floor != nil || command.TerminalNonAdmission != nil {
		return owner.applyDisposition(draft, command)
	}
	var wire []byte
	switch {
	case command.Admit != nil:
		wire = command.Admit.Authority
	case command.Candidates != nil:
		wire = command.Candidates.Authority
	case command.Seal != nil:
		wire = command.Seal.Authority
	case command.Drain != nil:
		wire = command.Drain.Authority
	case command.Health != nil:
		wire = command.Health.Authority
	case command.NonAdmission != nil:
		wire = command.NonAdmission.Authority
	}
	verified, route, err := owner.authority(draft, wire)
	if err != nil {
		return err
	}
	switch {
	case command.Admit != nil:
		for _, record := range draft.state.Executions {
			if bytes.Equal(record.Authority, wire) {
				return nil // exact retained intent only, never renewed execution time
			}
		}
		if err := owner.store.recoveryError(); err != nil {
			return err
		}
		if draft.state.BackendLifecycle.State != BackendLifecycleUnresolved {
			return ErrBackendIncarnationUnproven
		}
		return draft.admit(verified.Authority, route.maxClockSkew)
	case command.Candidates != nil:
		var confirmed *stageauthority.Verified
		if len(command.Candidates.Confirmed) != 0 {
			value, _, err := owner.authority(draft, command.Candidates.Confirmed)
			if err != nil {
				return err
			}
			confirmed = &value
		}
		return draft.recordCandidates(verified, confirmed)
	case command.Seal != nil:
		var receipt velav1.LocalMaterializationReceipt
		if err := decodeJournalProto(command.Seal.Receipt, &receipt); err != nil {
			return err
		}
		return draft.recordSeal(verified, &receipt)
	case command.Drain != nil:
		index, err := draft.retainedExecutionIndex(verified)
		if err != nil {
			return err
		}
		if saved := draft.state.Executions[index].Drain; saved != nil {
			if bytes.Equal(saved.Authority, wire) && saved.Result == command.Drain.Drain {
				return nil
			}
			return ErrExecutionDrainUnproven
		}
		return draft.recordDrain(verified, command.Drain.Drain, owner.now().UTC())
	case command.Health != nil:
		return draft.recordHealth(verified, command.Health.Evidence)
	case command.NonAdmission != nil:
		return draft.recordNonAdmission(verified, route, owner.now().UTC())
	default:
		return ErrJournalCommand
	}
}

func (owner *ExecutionJournalOwner) applyDisposition(draft *executionJournalDraft, command JournalCommand) error {
	var wire []byte
	if command.Floor != nil {
		wire = command.Floor.Disposition
	} else {
		wire = command.TerminalNonAdmission.Disposition
	}
	var disposition velav1.StageTerminalDisposition
	if err := decodeJournalProto(wire, &disposition); err != nil {
		return err
	}
	if command.Floor != nil {
		if bytes.Equal(draft.state.Disposition, wire) {
			return nil // exact history only; current floor is never lowered
		}
		return draft.installFloor(&disposition)
	}
	allocation := stageauthority.FindTerminalAllocation(&disposition, command.TerminalNonAdmission.Allocation)
	if allocation == nil {
		return ErrExecutionNonAdmissionUnproven
	}
	for _, route := range owner.routes {
		if route.matchesTerminalAllocation(draft.scope.binding, allocation) {
			return draft.recordTerminalNonAdmission(&disposition, command.TerminalNonAdmission.Allocation, route, owner.now().UTC())
		}
	}
	return stageauthority.ErrRuntimeMismatch
}
