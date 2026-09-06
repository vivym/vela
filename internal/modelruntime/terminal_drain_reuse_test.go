package modelruntime_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestTerminalDrainRestoresStoppedSlotAfterUnacknowledgedRenewal(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "not-applied", true: "applied"}[applied], func(t *testing.T) {
			backend := &renewalRecoveryBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), failStatus: true,
				applyRenewal: applied, calls: make(chan cancellationAuthorityCall, 2)}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			first := f.authorities[0]
			prepareFloorRuntime(t, f.supervisor, first)
			f.clock.Advance(time.Second)
			latest := renewWatchdogAuthority(t, f.signer, first, f.clock.Now())
			if response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: latest}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("renewal fault: %v %v", response, err)
			}
			if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
				t.Fatal(err)
			}
			if response, err := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{Authority: latest,
				Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP}); err != nil || !response.GetCancellationAcknowledged() {
				t.Fatalf("cancel: %v %v", response, err)
			}
			backend.FinishStop()
			request := &velav1.ModelRuntimeServiceProbeReadinessRequest{Identity: inspectionIdentity(first), Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP}
			if response, err := f.supervisor.ProbeReadiness(t.Context(), request); err != nil || response.GetReady() {
				t.Fatalf("terminal allocation still held its slot but reported ready: %v %v", response, err)
			}
			actual := first
			if applied {
				actual = latest
			}
			checkpoint, err := f.supervisor.DrainExecution(t.Context(), actual)
			if err != nil || checkpoint == nil || !proto.Equal(checkpoint.Authority, actual) {
				t.Fatalf("exact terminal drain: %+v %v", checkpoint, err)
			}
			if response, err := f.supervisor.ProbeReadiness(t.Context(), request); err != nil || !response.GetReady() {
				t.Fatalf("drained stopped Runtime did not restore readiness: %v %v", response, err)
			}
			next := f.authority(t, 1, 12)
			if response, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: next, ExecutionSpec: runtimeExecutionSpec()}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("terminal drain left the shared execution slot occupied: %v %v", response, err)
			}
		})
	}
}

func TestTerminalDrainRetriesStopInspectionFromPersistedCheckpoint(t *testing.T) {
	for index, fault := range []string{"error", "unknown", "still-prepared", "malformed", "timeout"} {
		t.Run(fault, func(t *testing.T) {
			backend := &renewalRecoveryBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), calls: make(chan cancellationAuthorityCall, 2)}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			authority := f.authorities[0]
			prepareFloorRuntime(t, f.supervisor, authority)
			if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
				t.Fatal(err)
			}
			if response, err := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{Authority: authority,
				Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP}); err != nil || !response.GetCancellationAcknowledged() {
				t.Fatalf("cancel: %v %v", response, err)
			}
			backend.FinishStop()
			backend.inspectFault.Store(int32(index + 1))
			if checkpoint, err := f.supervisor.DrainExecution(t.Context(), authority); err == nil || checkpoint != nil {
				t.Fatalf("unproven stop released slot: %+v %v", checkpoint, err)
			}
			assertExecutionDrainCheckpoint(t, f.supervisor, authority, true)
			probe := &velav1.ModelRuntimeServiceProbeReadinessRequest{Identity: inspectionIdentity(authority), Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP}
			if response, err := f.supervisor.ProbeReadiness(t.Context(), probe); err != nil || response.GetReady() {
				t.Fatalf("checkpoint alone restored readiness: %v %v", response, err)
			}
			next := f.authority(t, 1, 12)
			if response, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: next, ExecutionSpec: runtimeExecutionSpec()}); err != nil || response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("checkpoint alone admitted next execution: %v %v", response, err)
			}
			backend.inspectFault.Store(0)
			if checkpoint, err := f.supervisor.DrainExecution(t.Context(), authority); err != nil || checkpoint == nil {
				t.Fatalf("stop observation retry: %+v %v", checkpoint, err)
			}
			if response, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: next, ExecutionSpec: runtimeExecutionSpec()}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("retry left slot occupied: %v %v", response, err)
			}
		})
	}
}

func TestTerminalDrainAfterCancellationPreservesExplicitWorkerHealth(t *testing.T) {
	for _, reusable := range []bool{false, true} {
		t.Run(map[bool]string{false: "unhealthy", true: "reusable"}[reusable], func(t *testing.T) {
			backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), failure: "error"}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			authority := f.authorities[0]
			prepareFloorRuntime(t, f.supervisor, authority)
			backend.failureEvidence = &modelruntime.FailureEvidence{FailureClass: "backend_oom", FailureFingerprint: bytes.Repeat([]byte{0x91}, 32),
				WorkerReusable: reusable, ConsumedResourceUnits: 1, FailedAt: f.clock.Now(), RetryAt: f.clock.Now().Add(time.Second)}
			if response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: authority}); err != nil || response.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED {
				t.Fatalf("failed status: %v %v", response, err)
			}
			if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
				t.Fatal(err)
			}
			if response, err := f.supervisor.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{Authority: authority,
				Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP}); err != nil || !response.GetCancellationAcknowledged() {
				t.Fatalf("cancel failed backend: %v %v", response, err)
			}
			backend.failure = ""
			if checkpoint, err := f.supervisor.DrainExecution(t.Context(), authority); err != nil || checkpoint == nil {
				t.Fatalf("drain canceled backend: %+v %v", checkpoint, err)
			}
			probe := &velav1.ModelRuntimeServiceProbeReadinessRequest{Identity: inspectionIdentity(authority), Check: velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP}
			if response, err := f.supervisor.ProbeReadiness(t.Context(), probe); err != nil || response.GetReady() != reusable {
				t.Fatalf("cancel/drain readiness lost WorkerReusable=%t: %v %v", reusable, response, err)
			}
			if response, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority(t, 1, 12), ExecutionSpec: runtimeExecutionSpec()}); err != nil || (response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED) != reusable {
				t.Fatalf("cancel/drain admission lost WorkerReusable=%t: %v %v", reusable, response, err)
			}
		})
	}
}
