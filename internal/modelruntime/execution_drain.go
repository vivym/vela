package modelruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const ExecutionDrainContract = "vela-execution-writer-drain-v1"

var (
	ErrExecutionDrainUnproven = errors.New("execution writer drain is unproven")
	ErrExecutionHistoryFull   = errors.New("retained execution history is full")
)

// BackendExecutionDrainer freezes task admission for this exact execution,
// joins all its tasks, closes writable handles, and reaps execution-owned child
// writers before returning success. It must preserve resident model state and
// sealed output. Status, Cancel acknowledgements and driver teardown are not
// substitutes. Errors/timeouts leave drain unproven and must not unload a driver.
type BackendExecutionDrainer interface {
	DrainExecution(context.Context, stageauthority.Verified) (BackendDrain, error)
}

type BackendDrain struct {
	Contract          string            `json:"contract"`
	AuthorityDigest   [sha256.Size]byte `json:"authority_digest"`
	ExecutionSequence int64             `json:"execution_sequence"`
}

// ExecutionDrainCheckpoint covers only this member's backend execution. It
// does not prove Worker input exclusion or close unseen allocations through a
// signed floor, and cannot by itself authorize scratch deletion.
type ExecutionDrainCheckpoint struct {
	WorkerMemberID string
	Authority      *velav1.StageAuthority
	Result         BackendDrain
	DrainedAt      time.Time
}

func validateBackendDrain(result BackendDrain, verified stageauthority.Verified) error {
	if result.Contract != ExecutionDrainContract || result.AuthorityDigest != verified.Digest ||
		result.ExecutionSequence <= 0 || result.ExecutionSequence != verified.Authority.GetExecutionSequence() {
		return ErrExecutionDrainUnproven
	}
	return nil
}

// The caller owns operationMu and an admitted operation throughout. Persistence
// is required before the caller releases the active identity or shared slot.
func (service *Service) checkpointExecutionDrain(ctx context.Context, verified stageauthority.Verified) error {
	admission := service.executionAdmission()
	admission.mu.Lock()
	if admission.store == nil {
		admission.mu.Unlock()
		return nil
	}
	if err := admission.checkStateLocked(); err != nil {
		admission.mu.Unlock()
		return err
	}
	index, err := admission.store.retainedExecutionIndex(verified)
	if err != nil {
		admission.mu.Unlock()
		return err
	}
	if saved := admission.store.state.Executions[index].Drain; saved != nil {
		err := validateBackendDrain(saved.Result, verified)
		admission.mu.Unlock()
		return err
	}
	admission.mu.Unlock()

	drainer, ok := service.backend.(BackendExecutionDrainer)
	if !ok {
		return ErrExecutionDrainUnproven
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, service.cancelTimeout)
	defer cancel()
	result, err := drainer.DrainExecution(ctx, verified)
	if err != nil {
		return errors.Join(ErrExecutionDrainUnproven, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateBackendDrain(result, verified); err != nil {
		return err
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if err := admission.checkStateLocked(); err != nil {
		return err
	}
	if err := admission.store.saveDrain(verified, result, service.clock.Now()); err != nil {
		return admission.failStateLocked(err)
	}
	return nil
}

// DrainExecution retries drain for an exact, terminal or canceling execution owned by
// this resident Service. Historical signatures can stop writers but never renew
// execution. Old Runtime epochs and missing active records cannot enter a backend.
func (supervisor *Supervisor) DrainExecution(ctx context.Context, authority *velav1.StageAuthority) (*ExecutionDrainCheckpoint, error) {
	if supervisor == nil || supervisor.floor == nil || ctx == nil {
		return nil, ErrExecutionDrainUnproven
	}
	service := supervisor.routeAuthority(authority)
	if service == nil {
		return nil, stageauthority.ErrRuntimeMismatch
	}
	verified, err := service.validator.ValidateSignature(authority, service.binding)
	if err == nil {
		_, err = service.validator.ValidateEnvelopeForReplay(authority, service.maxClockSkew)
	}
	if err != nil {
		return nil, err
	}
	service.operationMu.Lock()
	defer service.operationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	admission := service.executionAdmission()
	_, release, err := admission.begin(service, &verified, true)
	if err != nil {
		return nil, err
	}
	defer release()
	service.mu.Lock()
	exact := service.active != nil && service.active.verified.Digest == verified.Digest &&
		(terminalState(service.active.state) || service.active.state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING)
	state := velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED
	reusable := false
	if exact {
		state = service.active.state
		reusable = service.active.reuseAfterDrain
	}
	service.mu.Unlock()
	if !exact {
		return supervisor.InspectExecutionDrain(ctx, authority)
	}
	admission.mu.Lock()
	durable := admission.store != nil
	admission.mu.Unlock()
	if !durable {
		return nil, ErrExecutionDrainUnproven
	}
	if err := service.checkpointExecutionDrain(ctx, verified); err != nil {
		return nil, err
	}
	// A canceled execution may have finished after its authority expired. Its
	// explicit backend proof can be persisted without inventing terminal state
	// or WorkerReusable health that ordinary Status has not established.
	if state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED ||
		(state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED && reusable) {
		service.setActiveWorkerReusable(verified.Digest)
		service.stopWatchdog(verified.Digest)
	}
	return supervisor.InspectExecutionDrain(ctx, authority)
}

// InspectExecutionDrain reads an exact persisted checkpoint, including after a
// Runtime epoch change. Missing or pending history returns nil, never an inferred
// drain result. This local API neither enters a backend nor changes admission.
func (supervisor *Supervisor) InspectExecutionDrain(ctx context.Context, authority *velav1.StageAuthority) (*ExecutionDrainCheckpoint, error) {
	return supervisor.inspectExecutionDrain(ctx, authority, false)
}

// InspectAllocationDrain returns only an existing durable checkpoint, retaining
// the exact signed envelope actually drained, including partial renewal history.
func (supervisor *Supervisor) InspectAllocationDrain(ctx context.Context, authority *velav1.StageAuthority) (*ExecutionDrainCheckpoint, error) {
	return supervisor.inspectExecutionDrain(ctx, authority, true)
}

func (supervisor *Supervisor) inspectExecutionDrain(ctx context.Context, authority *velav1.StageAuthority, allocation bool) (*ExecutionDrainCheckpoint, error) {
	if supervisor == nil || supervisor.floor == nil || ctx == nil {
		return nil, ErrExecutionDrainUnproven
	}
	verified, err := supervisor.floor.validator.ValidateEnvelopeForReplay(authority, supervisor.services[0].maxClockSkew)
	if err != nil {
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
	if admission.store == nil {
		return nil, ErrExecutionDrainUnproven
	}
	for _, record := range admission.store.state.Executions {
		if record.Drain == nil {
			continue
		}
		drained, err := admission.store.retainedAuthority(record.Drain.Authority)
		if err != nil {
			return nil, err
		}
		if record.Drain.Result.AuthorityDigest == verified.Digest ||
			(allocation && stageauthority.ValidateSameExecution(verified.Authority, drained.Authority) == nil) {
			return &ExecutionDrainCheckpoint{WorkerMemberID: supervisor.services[0].binding.WorkerMemberID,
				Authority: proto.Clone(drained.Authority).(*velav1.StageAuthority),
				Result:    record.Drain.Result, DrainedAt: record.Drain.DrainedAt}, nil
		}
	}
	return nil, nil
}

func (store *executionStateFile) retainedAuthority(wire []byte) (stageauthority.Verified, error) {
	if len(wire) == 0 || len(wire) > maxExecutionWireBytes {
		return stageauthority.Verified{}, errors.New("retained execution authority exceeds its bound")
	}
	authority := &velav1.StageAuthority{}
	if err := proto.Unmarshal(wire, authority); err != nil {
		return stageauthority.Verified{}, err
	}
	verified, err := store.scope.floor.validator.ValidateEnvelopeSignature(authority)
	if err != nil {
		return stageauthority.Verified{}, err
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(verified.Authority)
	if err != nil || !bytes.Equal(wire, canonical) || verified.Authority.GetSchemaVersion() != stageauthority.SchemaVersionV2 {
		return stageauthority.Verified{}, errors.New("retained execution authority is not canonical V2")
	}
	return verified, store.scope.matchRetainedExecutionScope(verified.Authority)
}

func (store *executionStateFile) validateRetainedExecutions() error {
	if len(store.state.Executions) > maxRetainedExecutions || (store.state.Highest == 0) != (len(store.state.Executions) == 0) {
		return errors.New("retained execution history is incomplete or exceeds its bound")
	}
	var previous int64
	for _, record := range store.state.Executions {
		original, err := store.retainedAuthority(record.Authority)
		if err != nil {
			return err
		}
		sequence := original.Authority.GetExecutionSequence()
		if sequence <= previous || sequence > store.state.Highest {
			return errors.New("retained execution history is unordered")
		}
		previous = sequence
		if record.Drain != nil {
			verified, err := store.retainedAuthority(record.Drain.Authority)
			if err != nil {
				return err
			}
			if original.Digest != verified.Digest && stageauthority.ValidateRenewal(original.Authority, verified.Authority) != nil {
				return errors.New("retained drain does not belong to the original execution")
			}
			if err := validateBackendDrain(record.Drain.Result, verified); err != nil {
				return err
			}
			if record.Drain.DrainedAt.IsZero() || record.Drain.DrainedAt.Location() != time.UTC {
				return errors.New("retained drain timestamp is invalid")
			}
		}
	}
	if previous != store.state.Highest || (previous > 0 && !bytes.Equal(store.state.Executions[len(store.state.Executions)-1].Authority, store.state.Authority)) {
		return errors.New("retained execution history does not match the watermark")
	}
	return nil
}

func (store *executionStateFile) retainedExecutionIndex(verified stageauthority.Verified) (int, error) {
	for index, record := range store.state.Executions {
		original, err := store.retainedAuthority(record.Authority)
		if err != nil {
			return 0, err
		}
		if original.Authority.GetExecutionSequence() == verified.Authority.GetExecutionSequence() {
			if original.Digest == verified.Digest || stageauthority.ValidateRenewal(original.Authority, verified.Authority) == nil {
				return index, nil
			}
			return 0, ErrExecutionDrainUnproven
		}
	}
	return 0, ErrExecutionDrainUnproven
}

func (store *executionStateFile) saveDrain(verified stageauthority.Verified, result BackendDrain, observed time.Time) error {
	index, err := store.retainedExecutionIndex(verified)
	if err != nil {
		return err
	}
	if err := validateBackendDrain(result, verified); err != nil {
		return err
	}
	if store.state.Executions[index].Drain != nil || observed.IsZero() {
		return errors.New("execution drain checkpoint is immutable or invalid")
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(verified.Authority)
	if err != nil || len(wire) > maxExecutionWireBytes {
		return errors.New("drained execution authority exceeds its persistence bound")
	}
	state := store.state
	state.Executions = slices.Clone(state.Executions)
	state.Executions[index].Drain = &executionDiskDrain{Authority: wire, Result: result, DrainedAt: observed.UTC()}
	return store.persist(state)
}

func (runtime *FakeRuntime) DrainExecution(ctx context.Context, verified stageauthority.Verified) (BackendDrain, error) {
	if runtime == nil || ctx == nil {
		return BackendDrain{}, ErrExecutionDrainUnproven
	}
	if err := ctx.Err(); err != nil {
		return BackendDrain{}, err
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if verified.Authority.GetExecutionSequence() <= 0 || runtime.activeDigest == ([32]byte{}) || runtime.activeDigest != verified.Digest ||
		(runtime.state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED &&
			runtime.state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED &&
			runtime.state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED) {
		return BackendDrain{}, ErrExecutionDrainUnproven
	}
	// FakeRuntime owns no files, execution goroutines or child processes. This
	// lock joins its synchronous state changes; the watermark closes reentry.
	runtime.drainedDigest = verified.Digest
	runtime.drainedThrough = max(runtime.drainedThrough, verified.Authority.GetExecutionSequence())
	return BackendDrain{Contract: ExecutionDrainContract, AuthorityDigest: verified.Digest,
		ExecutionSequence: verified.Authority.GetExecutionSequence()}, nil
}
