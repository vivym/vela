package modelruntime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

type ProbeResult struct {
	Ready    bool
	Evidence []byte
	Detail   string
}

type BackendStatus struct {
	State              velav1.ModelRuntimeExecutionState
	Sequence           int64
	BackendStage       string
	Progress           *float64
	BoundedStatusJSON  []byte
	LocalReceiptID     string
	LocalReceiptDigest []byte
	Detail             string
	FailureEvidence    *FailureEvidence
}

type FailureEvidence struct {
	FailureClass          string
	FailureFingerprint    []byte
	Detail                string
	WorkerReusable        bool
	ConsumedResourceUnits int64
	FailedAt              time.Time
	RetryAt               time.Time
}

type SealedOutput struct {
	OutputManifestJSON []byte
	TotalSizeBytes     int64
}

type Backend interface {
	Probe(context.Context, velav1.ModelRuntimeReadinessCheck) (ProbeResult, error)
	Prepare(context.Context, stageauthority.Verified, *velav1.StageExecutionSpec) error
	Start(context.Context, stageauthority.Verified) error
	Cancel(context.Context, stageauthority.Verified, velav1.ModelRuntimeCancelReason) error
	Status(context.Context, stageauthority.Verified) (BackendStatus, error)
	Seal(context.Context, stageauthority.Verified) (SealedOutput, error)
}

type FakeRuntime struct {
	mu              sync.Mutex
	component       string
	state           velav1.ModelRuntimeExecutionState
	sequence        int64
	activeDigest    [32]byte
	cancelRequested bool
	outputManifest  []byte
	outputSize      int64
}

func NewFakeEncoderRuntime() *FakeRuntime {
	return newFakeRuntime("encoder")
}

func NewFakeDiTRuntime() *FakeRuntime {
	return newFakeRuntime("dit")
}

func NewFakeVAERuntime() *FakeRuntime {
	return newFakeRuntime("vae_decoder")
}

func newFakeRuntime(component string) *FakeRuntime {
	return &FakeRuntime{
		component: component,
		state:     velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARING,
	}
}

func (runtime *FakeRuntime) Probe(
	_ context.Context,
	check velav1.ModelRuntimeReadinessCheck,
) (ProbeResult, error) {
	if runtime == nil {
		return ProbeResult{}, errors.New("fake ModelRuntime is not configured")
	}
	if check == velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_UNSPECIFIED {
		return ProbeResult{}, errors.New("ModelRuntime readiness check is unspecified")
	}
	evidence, err := json.Marshal(map[string]any{
		"component": runtime.component,
		"check":     check.String(),
		"ready":     true,
	})
	if err != nil {
		return ProbeResult{}, err
	}
	return ProbeResult{Ready: true, Evidence: evidence, Detail: "resident runtime ready"}, nil
}

func (runtime *FakeRuntime) Prepare(
	_ context.Context,
	authority stageauthority.Verified,
	_ *velav1.StageExecutionSpec,
) error {
	if runtime == nil {
		return errors.New("fake ModelRuntime is not configured")
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.activeDigest != ([32]byte{}) && runtime.activeDigest != authority.Digest &&
		runtime.state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED {
		return errors.New("fake ModelRuntime already owns another StageAttempt")
	}
	runtime.activeDigest = authority.Digest
	runtime.cancelRequested = false
	runtime.outputManifest = nil
	runtime.outputSize = 0
	runtime.state = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED
	runtime.sequence++
	return nil
}

func (runtime *FakeRuntime) Start(
	_ context.Context,
	authority stageauthority.Verified,
) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.activeDigest != authority.Digest ||
		runtime.state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED {
		return errors.New("fake ModelRuntime StageAttempt is not prepared")
	}
	runtime.state = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING
	runtime.sequence++
	return nil
}

func (runtime *FakeRuntime) Cancel(
	_ context.Context,
	authority stageauthority.Verified,
	reason velav1.ModelRuntimeCancelReason,
) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.activeDigest != authority.Digest {
		return errors.New("fake ModelRuntime cancellation authority is stale")
	}
	if reason == velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_UNSPECIFIED {
		return errors.New("fake ModelRuntime cancellation reason is unspecified")
	}
	runtime.cancelRequested = true
	if runtime.state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED {
		runtime.state = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING
		runtime.sequence++
	}
	return nil
}

func (runtime *FakeRuntime) Status(
	_ context.Context,
	authority stageauthority.Verified,
) (BackendStatus, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.activeDigest != authority.Digest && runtime.activeDigest != stableDigest(authority) {
		// Authority renewal changes the signed envelope digest. The Service already
		// proves the stable execution identity before presenting it to the backend.
		runtime.activeDigest = authority.Digest
	}
	progress := fakeProgress(runtime.state)
	statusJSON, err := json.Marshal(map[string]any{
		"component": runtime.component,
		"state":     runtime.state.String(),
	})
	if err != nil {
		return BackendStatus{}, err
	}
	return BackendStatus{
		State: runtime.state, Sequence: runtime.sequence, BackendStage: runtime.component,
		Progress: progress, BoundedStatusJSON: statusJSON,
	}, nil
}

func (runtime *FakeRuntime) Seal(
	_ context.Context,
	authority stageauthority.Verified,
) (SealedOutput, error) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.activeDigest != authority.Digest {
		runtime.activeDigest = authority.Digest
	}
	if runtime.state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_READY ||
		len(runtime.outputManifest) == 0 {
		return SealedOutput{}, errors.New("fake ModelRuntime output is not ready")
	}
	runtime.state = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED
	runtime.sequence++
	return SealedOutput{
		OutputManifestJSON: append([]byte(nil), runtime.outputManifest...),
		TotalSizeBytes:     runtime.outputSize,
	}, nil
}

func (runtime *FakeRuntime) MarkOutputReady(outputManifestJSON []byte) {
	runtime.MarkOutputReadyWithSize(outputManifestJSON, int64(len(outputManifestJSON)))
}

func (runtime *FakeRuntime) MarkOutputReadyWithSize(outputManifestJSON []byte, sizeBytes int64) {
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING ||
		sizeBytes <= 0 {
		return
	}
	runtime.outputManifest = append([]byte(nil), outputManifestJSON...)
	runtime.outputSize = sizeBytes
	runtime.state = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_READY
	runtime.sequence++
}

func (runtime *FakeRuntime) FinishStop() {
	if runtime == nil {
		return
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if !runtime.cancelRequested {
		return
	}
	runtime.state = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED
	runtime.sequence++
}

func (runtime *FakeRuntime) Component() string {
	if runtime == nil {
		return ""
	}
	return runtime.component
}

func fakeProgress(state velav1.ModelRuntimeExecutionState) *float64 {
	value := 0.0
	switch state {
	case velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING:
		value = 0.5
	case velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_READY,
		velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED:
		value = 1
	}
	return &value
}

func stableDigest(authority stageauthority.Verified) [32]byte {
	if authority.Authority == nil {
		return [32]byte{}
	}
	payload := fmt.Sprintf(
		"%s\x00%s\x00%s",
		authority.Authority.GetStageLeaseId(),
		authority.Authority.GetExecutionNonce(),
		authority.Authority.GetModelRuntimeIdentity(),
	)
	return sha256.Sum256([]byte(payload))
}
