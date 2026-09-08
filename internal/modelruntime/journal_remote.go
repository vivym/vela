package modelruntime

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

var ErrJournalRejected = errors.New("execution journal owner rejected the mutation")

// A canceled pure read published nothing and can be retried. Callers of apply
// still fence this error: its readback follows a possibly committed mutation.
var errJournalReadInterrupted = errors.New("execution journal read interrupted")

// RuntimeJournalTransport is supplied by trusted assembly. Implementations must
// authenticate the owner; implementing this interface is not startup permission.
// Read may return ErrJournalReadUnavailable only without a usable result. Owner
// recovery responses and invalid journal evidence must retain their fatal error.
type RuntimeJournalTransport interface {
	Apply(context.Context, JournalCommand) (JournalMutationReceipt, error)
	Read(context.Context) (JournalDocument, error)
}

type RemoteExecutionJournalConfig struct {
	Manifest  LaunchManifest
	Validator *stageauthority.Validator
	Identity  ExecutionJournalIdentity
	Startup   BackendLifecycleStatus
	Transport RuntimeJournalTransport
	Timeout   time.Duration
}

type remoteExecutionJournal struct {
	executionJournal
	config RemoteExecutionJournalConfig
	ctx    context.Context
	closed bool
}

// NewSupervisorWithRemoteExecutionJournal attaches unused Services to an
// already-started, independently approved original incarnation. It creates no
// local journal and cannot start a backend, allocate an epoch, authorize first
// use or adopt a replacement. Trusted startup assembly must precede this call.
func NewSupervisorWithRemoteExecutionJournal(ctx context.Context, config RemoteExecutionJournalConfig, services ...*Service) (*Supervisor, error) {
	remote, err := newRemoteExecutionJournal(ctx, config)
	if err != nil {
		return nil, err
	}
	config = remote.config
	bindings, err := config.Manifest.RuntimeBindings()
	if err != nil || len(bindings) != len(services) {
		return nil, ErrExecutionStateRecovery
	}
	for i, binding := range bindings {
		found := false
		for _, service := range services {
			if service == nil {
				continue
			}
			binding.ModelRuntimeEpoch = service.binding.ModelRuntimeEpoch
			if binding.ModelRuntimeEpoch >= config.Manifest.Runtimes[i].ModelRuntimeEpochFloor && reflect.DeepEqual(binding, service.binding) {
				found = true
			}
		}
		if !found {
			return nil, stageauthority.ErrRuntimeMismatch
		}
	}
	floor, err := config.Manifest.bindExecutionFloorConfig(ExecutionFloorConfig{}, config.Validator)
	if err != nil {
		return nil, err
	}
	return newSupervisorWithStores(floor, nil, remote, services...)
}

func newRemoteExecutionJournal(ctx context.Context, config RemoteExecutionJournalConfig) (*remoteExecutionJournal, error) {
	if ctx == nil || config.Transport == nil || config.Timeout <= 0 || config.Timeout > 45*time.Second ||
		config.Startup.State != BackendLifecycleUnresolved || !config.Identity.Storage.Valid() {
		return nil, ErrExecutionStateRecovery
	}
	config.Manifest = cloneLaunchManifest(config.Manifest)
	scope, err := executionScopeForManifest(config.Manifest, config.Validator)
	if err != nil {
		return nil, err
	}
	remote := &remoteExecutionJournal{executionJournal: executionJournal{scope: scope}, config: config, ctx: ctx}
	if err := remote.checkContext(ctx); err != nil {
		return nil, err
	}
	if err := remote.requireFreshStartup(); err != nil {
		return nil, err
	}
	return remote, nil
}

func (store *remoteExecutionJournal) requireFreshStartup() error {
	state := store.state
	if state.Highest != 0 || state.Floor != 0 || len(state.Executions) != 0 || len(state.NonAdmissions) != 0 || len(state.TerminalNonAdmissions) != 0 {
		return ErrBackendIncarnationUnproven
	}
	return nil
}

func (store *remoteExecutionJournal) view() *executionJournal { return &store.executionJournal }
func (store *remoteExecutionJournal) close() error            { store.closed = true; return nil }
func (store *remoteExecutionJournal) recoveryError() error    { return store.workerHealthError() }

// Every exchange observes the RPC, owner lifetime and configured timeout.
// Readback inherits the write's remaining budget, rather than starting anew.
func (store *remoteExecutionJournal) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parent, store.config.Timeout)
	stop := context.AfterFunc(store.ctx, cancel)
	if store.ctx.Err() != nil {
		cancel()
	}
	return ctx, func() { stop(); cancel() }
}

func (store *remoteExecutionJournal) readInterruption(ctx context.Context) error {
	if err := store.ctx.Err(); err != nil {
		return errors.Join(ErrExecutionStateRecovery, err, context.Cause(store.ctx))
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(errJournalReadInterrupted, err, context.Cause(ctx))
	}
	// A socket deadline can fire before the context timer at the same instant.
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return errors.Join(errJournalReadInterrupted, context.DeadlineExceeded)
	}
	return nil
}

func (store *remoteExecutionJournal) checkContext(requestCtx context.Context) error {
	if store.closed {
		return ErrExecutionStateRecovery
	}
	ctx, cancel := store.operationContext(requestCtx)
	defer cancel()
	if err := store.readInterruption(ctx); err != nil {
		return err
	}
	var document JournalDocument
	var err error
	for range 3 {
		document, err = store.config.Transport.Read(ctx)
		if !errors.Is(err, ErrJournalChanged) || errors.Is(err, ErrExecutionStateRecovery) || errors.Is(err, ErrJournalCommand) {
			break
		}
		if err := store.readInterruption(ctx); err != nil {
			return err
		}
	}
	if err != nil {
		// Explicit owner/integrity failure outranks transport/cancellation
		// markers, including errors joined by a transport implementation.
		if errors.Is(err, ErrExecutionStateRecovery) || errors.Is(err, ErrJournalCommand) {
			return errors.Join(ErrExecutionStateRecovery, err)
		}
		if interrupted := store.readInterruption(ctx); interrupted != nil {
			return errors.Join(interrupted, err)
		}
		if errors.Is(err, ErrJournalChanged) || errors.Is(err, ErrJournalReadUnavailable) {
			return err
		}
		return errors.Join(ErrExecutionStateRecovery, err)
	}
	snapshot, err := VerifyExecutionJournalSnapshot(document.Document, document.LockDocument, store.config.Manifest, store.config.Validator, store.config.Identity)
	if err != nil {
		return errors.Join(ErrExecutionStateRecovery, err)
	}
	if snapshot.status.BackendLifecycle != store.config.Startup || snapshot.status.BackendLifecycle.LaunchDigest != snapshot.launchDigest || snapshot.status.Highest < store.state.Highest || snapshot.status.Floor < store.state.Floor {
		return ErrExecutionStateRecovery
	}
	state, err := decodeExecutionJournal(document.Document)
	if err != nil {
		return errors.Join(ErrExecutionStateRecovery, err)
	}
	if err := store.readInterruption(ctx); err != nil {
		return err
	}
	store.state = state
	return nil
}

func (store *remoteExecutionJournal) apply(requestCtx context.Context, command JournalCommand) (JournalMutationReceipt, error) {
	command.SchemaVersion = 1
	wire, err := EncodeJournalCommand(command)
	if err != nil {
		return JournalMutationReceipt{}, err
	}
	ctx, cancel := store.operationContext(requestCtx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return JournalMutationReceipt{}, err
	}
	receipt, err := store.config.Transport.Apply(ctx, command)
	if err != nil {
		if errors.Is(err, ErrJournalRejected) {
			return JournalMutationReceipt{}, &executionJournalRejection{cause: err}
		}
		return JournalMutationReceipt{}, err
	}
	if receipt.SchemaVersion != 1 || receipt.RequestDigest != sha256.Sum256(wire) || receipt.JournalID != store.config.Identity.JournalID ||
		receipt.JournalScope != store.config.Identity.Scope || receipt.StateDigest == ([32]byte{}) || receipt.Highest < 0 || receipt.Floor < 0 {
		return JournalMutationReceipt{}, ErrExecutionStateRecovery
	}
	if err := context.Cause(ctx); err != nil {
		return JournalMutationReceipt{}, errors.Join(ErrExecutionStateRecovery, err)
	}
	if err := store.checkContext(ctx); err != nil {
		return JournalMutationReceipt{}, err
	}
	if store.state.Highest < receipt.Highest || store.state.Floor < receipt.Floor {
		return JournalMutationReceipt{}, ErrExecutionStateRecovery
	}
	return receipt, nil
}

func journalAuthorityWire(authority *velav1.StageAuthority) []byte {
	wire, _ := proto.MarshalOptions{Deterministic: true}.Marshal(authority)
	return wire
}

func (store *remoteExecutionJournal) saveHighestContext(ctx context.Context, authority *velav1.StageAuthority, _ time.Duration) error {
	receipt, err := store.apply(ctx, JournalCommand{Admit: &JournalAuthorityCommand{Authority: journalAuthorityWire(authority)}})
	if err == nil && receipt.Replayed {
		return ErrExecutionStateRecovery
	}
	return err
}
func (store *remoteExecutionJournal) saveCandidatesContext(ctx context.Context, accepted stageauthority.Verified, confirmed *stageauthority.Verified) error {
	command := &JournalCandidatesCommand{Authority: journalAuthorityWire(accepted.Authority)}
	if confirmed != nil {
		command.Confirmed = journalAuthorityWire(confirmed.Authority)
	}
	_, err := store.apply(ctx, JournalCommand{Candidates: command})
	return err
}
func (store *remoteExecutionJournal) saveSealContext(ctx context.Context, authority stageauthority.Verified, receipt *velav1.LocalMaterializationReceipt) error {
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(receipt)
	if err != nil {
		return err
	}
	_, err = store.apply(ctx, JournalCommand{Seal: &JournalSealCommand{Authority: journalAuthorityWire(authority.Authority), Receipt: wire}})
	return err
}
func (store *remoteExecutionJournal) saveHealthContext(ctx context.Context, authority stageauthority.Verified, evidence *FailureEvidence) error {
	_, err := store.apply(ctx, JournalCommand{Health: &JournalHealthCommand{Authority: journalAuthorityWire(authority.Authority), Evidence: evidence}})
	return err
}
func (store *remoteExecutionJournal) saveDrainContext(ctx context.Context, authority stageauthority.Verified, drain BackendDrain, _ time.Time) error {
	_, err := store.apply(ctx, JournalCommand{Drain: &JournalDrainCommand{Authority: journalAuthorityWire(authority.Authority), Drain: drain}})
	return err
}

// Worker mutations must already be durably recorded by the independently bound
// Worker. Runtime checks that history under its admission lock; it cannot borrow
// the Worker role or attest absence based only on a private cache.
func (store *remoteExecutionJournal) saveFloorContext(ctx context.Context, disposition *velav1.StageTerminalDisposition) error {
	if store.state.Floor < disposition.GetCutoff() {
		return &executionJournalRejection{cause: ErrJournalRejected}
	}
	return nil
}
func (store *remoteExecutionJournal) saveNonAdmissionContext(ctx context.Context, authority stageauthority.Verified, _ executionJournalRoute, _ time.Time) error {
	proof, err := store.nonAdmissionCheckpoint(authority)
	if err != nil || proof == nil {
		return &executionJournalRejection{cause: errors.Join(ErrExecutionNonAdmissionUnproven, err)}
	}
	return nil
}
func (store *remoteExecutionJournal) saveTerminalNonAdmissionContext(ctx context.Context, disposition *velav1.StageTerminalDisposition, id string, _ executionJournalRoute, _ time.Time) error {
	allocation := stageauthority.FindTerminalAllocation(disposition, id)
	if allocation == nil {
		return &executionJournalRejection{cause: ErrExecutionNonAdmissionUnproven}
	}
	proof, err := store.terminalNonAdmissionCheckpoint(disposition, allocation)
	if err != nil || proof == nil {
		return &executionJournalRejection{cause: errors.Join(ErrExecutionNonAdmissionUnproven, err)}
	}
	return nil
}
