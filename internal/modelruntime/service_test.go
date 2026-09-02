package modelruntime_test

import (
	"bytes"
	"context"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestModelRuntimeServiceReattachesOnlyToTheSameResidentRuntime(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 30, 6, 0, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	authority := signRuntimeAuthority(t, signer, clock.Now())
	backend := modelruntime.NewFakeDiTRuntime()
	service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
	client, redial := serveRuntime(t, service)

	prepared, err := client.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{
		Authority: authority, ExecutionSpec: runtimeExecutionSpec(),
	})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
		prepared.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED {
		t.Fatalf("PrepareStage = %#v error=%v", prepared, err)
	}
	started, err := client.StartStage(context.Background(), &velav1.ModelRuntimeServiceStartStageRequest{
		Authority: authority,
	})
	if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
		started.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING {
		t.Fatalf("StartStage = %#v error=%v", started, err)
	}

	reattached := redial()
	statusResponse, err := reattached.Status(context.Background(), &velav1.ModelRuntimeServiceStatusRequest{
		Authority: authority,
	})
	if err != nil || statusResponse.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
		statusResponse.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING {
		t.Fatalf("reattached Status = %#v error=%v", statusResponse, err)
	}

	changedBinding := runtimeBinding()
	changedBinding.ModelRuntimeEpoch++
	changedService := newRuntimeService(
		t, clock, validator, changedBinding, modelruntime.NewFakeDiTRuntime(),
	)
	changedClient, _ := serveRuntime(t, changedService)
	rejected, err := changedClient.PrepareStage(
		context.Background(),
		&velav1.ModelRuntimeServicePrepareStageRequest{Authority: authority},
	)
	if err != nil || rejected.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("changed-runtime PrepareStage = %#v error=%v", rejected, err)
	}
}

func TestModelRuntimeRejectsExecutionSpecNotBoundByAuthority(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 30, 6, 10, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	authority := signRuntimeAuthority(t, signer, clock.Now())
	service := newRuntimeService(
		t, clock, validator, runtimeBinding(), modelruntime.NewFakeDiTRuntime(),
	)
	client, _ := serveRuntime(t, service)
	changed := runtimeExecutionSpec()
	changed.Inputs[0].ObjectVersion = "object-version-2"
	prepared, err := client.PrepareStage(
		context.Background(),
		&velav1.ModelRuntimeServicePrepareStageRequest{
			Authority: authority, ExecutionSpec: changed,
		},
	)
	if err != nil || prepared.GetDecision() !=
		velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("PrepareStage with altered spec = %#v error=%v", prepared, err)
	}
}

func TestFileEpochStoreAdvancesEpochAndRejectsPreRestartAuthority(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 30, 6, 12, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	store, err := modelruntime.NewFileEpochStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileEpochStore: %v", err)
	}
	newService := func() *modelruntime.Service {
		binding := runtimeBinding()
		binding.ModelRuntimeEpoch = 0
		service, serviceErr := modelruntime.NewService(modelruntime.Config{
			Binding: binding, EpochStore: store, Validator: validator,
			Backend: modelruntime.NewFakeDiTRuntime(), Clock: clock, CancelTimeout: time.Second,
		})
		if serviceErr != nil {
			t.Fatalf("NewService: %v", serviceErr)
		}
		t.Cleanup(service.Close)
		return service
	}
	first := newService()
	firstProbe, err := first.ProbeReadiness(
		context.Background(), &velav1.ModelRuntimeServiceProbeReadinessRequest{},
	)
	if err != nil {
		t.Fatalf("first ProbeReadiness: %v", err)
	}
	second := newService()
	secondProbe, err := second.ProbeReadiness(
		context.Background(), &velav1.ModelRuntimeServiceProbeReadinessRequest{},
	)
	if err != nil {
		t.Fatalf("second ProbeReadiness: %v", err)
	}
	firstEpoch := firstProbe.GetIdentity().GetModelRuntimeEpoch()
	secondEpoch := secondProbe.GetIdentity().GetModelRuntimeEpoch()
	if firstEpoch <= 0 || secondEpoch != firstEpoch+1 {
		t.Fatalf("ModelRuntime epochs = %d, %d", firstEpoch, secondEpoch)
	}
	oldAuthority := signRuntimeAuthorityForEpoch(t, signer, clock.Now(), firstEpoch)
	rejected, err := second.PrepareStage(
		context.Background(),
		&velav1.ModelRuntimeServicePrepareStageRequest{
			Authority: oldAuthority, ExecutionSpec: runtimeExecutionSpec(),
		},
	)
	if err != nil || rejected.GetDecision() !=
		velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("post-restart PrepareStage = %#v error=%v", rejected, err)
	}
}

func TestFileEpochStoreAdvancesPastDurableEpochFloor(t *testing.T) {
	store, err := modelruntime.NewFileEpochStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileEpochStore: %v", err)
	}
	binding := runtimeBinding()
	binding.ModelRuntimeEpoch = 0
	first, err := modelruntime.NewService(modelruntime.Config{
		Binding: binding, EpochStore: store, EpochFloor: 41,
		Validator: mustRuntimeValidator(t), Backend: modelruntime.NewFakeDiTRuntime(),
		CancelTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewService above floor: %v", err)
	}
	t.Cleanup(first.Close)
	identity, err := first.ProbeReadiness(
		context.Background(), &velav1.ModelRuntimeServiceProbeReadinessRequest{},
	)
	if err != nil || identity.GetIdentity().GetModelRuntimeEpoch() != 42 {
		t.Fatalf("allocated epoch = %#v error=%v, want 42", identity, err)
	}
	second, err := modelruntime.NewService(modelruntime.Config{
		Binding: binding, EpochStore: store, EpochFloor: 41,
		Validator: mustRuntimeValidator(t), Backend: modelruntime.NewFakeDiTRuntime(),
		CancelTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewService after persisted epoch: %v", err)
	}
	t.Cleanup(second.Close)
	identity, err = second.ProbeReadiness(
		context.Background(), &velav1.ModelRuntimeServiceProbeReadinessRequest{},
	)
	if err != nil || identity.GetIdentity().GetModelRuntimeEpoch() != 43 {
		t.Fatalf("allocated epoch = %#v error=%v, want 43", identity, err)
	}
}

func TestFileEpochStoreKeepsAUXResidencyEpochsIndependent(t *testing.T) {
	store, err := modelruntime.NewFileEpochStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileEpochStore: %v", err)
	}
	validator := mustRuntimeValidator(t)
	encoderBinding := runtimeBinding()
	encoderBinding.ModelResidencyID = "51000000-0000-0000-0000-000000000011"
	encoderBinding.ModelRuntimeIdentity = "h3-encoder-runtime-1"
	encoderBinding.ModelRuntimeEpoch = 0
	vaeBinding := runtimeBinding()
	vaeBinding.ModelResidencyID = "51000000-0000-0000-0000-000000000012"
	vaeBinding.ModelRuntimeIdentity = "h3-vae-runtime-1"
	vaeBinding.ModelRuntimeEpoch = 0
	for _, test := range []struct {
		name    string
		binding stageauthority.RuntimeBinding
		floor   int64
		want    int64
	}{
		{name: "encoder", binding: encoderBinding, floor: 40, want: 41},
		{name: "vae", binding: vaeBinding, floor: 90, want: 91},
		{name: "encoder restart", binding: encoderBinding, floor: 40, want: 42},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, serviceErr := modelruntime.NewService(modelruntime.Config{
				Binding: test.binding, EpochStore: store, EpochFloor: test.floor,
				Validator: validator, Backend: modelruntime.NewFakeEncoderRuntime(),
				CancelTimeout: time.Second,
			})
			if serviceErr != nil {
				t.Fatalf("NewService: %v", serviceErr)
			}
			t.Cleanup(service.Close)
			identity, probeErr := service.ProbeReadiness(
				context.Background(), &velav1.ModelRuntimeServiceProbeReadinessRequest{},
			)
			if probeErr != nil || identity.GetIdentity().GetModelRuntimeEpoch() != test.want {
				t.Fatalf("allocated epoch = %#v error=%v, want %d", identity, probeErr, test.want)
			}
		})
	}
}

func mustRuntimeValidator(t *testing.T) *stageauthority.Validator {
	t.Helper()
	validator, err := stageauthority.NewValidator(
		map[string][]byte{"stage-key-9": bytes.Repeat([]byte{0x5a}, 32)}, time.Now,
	)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	return validator
}

func TestModelRuntimeStartCannotInstallAuthorityBeforePrepare(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 30, 6, 15, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	authority := signRuntimeAuthority(t, signer, clock.Now())
	service := newRuntimeService(
		t, clock, validator, runtimeBinding(), modelruntime.NewFakeDiTRuntime(),
	)
	client, _ := serveRuntime(t, service)
	started, err := client.StartStage(
		context.Background(),
		&velav1.ModelRuntimeServiceStartStageRequest{Authority: authority},
	)
	if err != nil || started.GetDecision() !=
		velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("StartStage before Prepare = %#v error=%v", started, err)
	}
	prepared, err := client.PrepareStage(
		context.Background(),
		&velav1.ModelRuntimeServicePrepareStageRequest{
			Authority: authority, ExecutionSpec: runtimeExecutionSpec(),
		},
	)
	if err != nil || prepared.GetDecision() !=
		velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("PrepareStage after rejected Start = %#v error=%v", prepared, err)
	}
}

func TestModelRuntimeMonotonicWatchdogStopsUnrenewedWork(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 30, 6, 30, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	authority := signRuntimeAuthority(t, signer, clock.Now())
	backend := modelruntime.NewFakeEncoderRuntime()
	service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
	client, _ := serveRuntime(t, service)
	prepareAndStart(t, client, authority)

	clock.Advance(30 * time.Second)
	renewed := signRuntimeAuthority(t, signer, clock.Now())
	deadline := time.Now().Add(time.Second)
	for {
		statusResponse, err := client.Status(
			context.Background(),
			&velav1.ModelRuntimeServiceStatusRequest{Authority: renewed},
		)
		if err == nil &&
			statusResponse.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED &&
			statusResponse.GetState() == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("watchdog Status = %#v error=%v", statusResponse, err)
		}
		runtime.Gosched()
	}
}

func TestModelRuntimeCancellationAcknowledgementPrecedesActualStop(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 30, 7, 0, 0, 0, time.UTC))
	_, validator := runtimeAuthorityCrypto(t, clock)
	signer, err := stageauthority.NewSigner(map[string][]byte{
		"stage-key-9": bytes.Repeat([]byte{0x5a}, 32),
	})
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	authority := signRuntimeAuthority(t, signer, clock.Now())
	backend := modelruntime.NewFakeVAERuntime()
	service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
	client, _ := serveRuntime(t, service)
	prepareAndStart(t, client, authority)

	canceled, err := client.CancelStage(context.Background(), &velav1.ModelRuntimeServiceCancelStageRequest{
		Authority: authority,
		Reason:    velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	})
	if err != nil || !canceled.GetCancellationAcknowledged() ||
		canceled.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING {
		t.Fatalf("CancelStage = %#v error=%v", canceled, err)
	}
	statusResponse, err := client.Status(
		context.Background(),
		&velav1.ModelRuntimeServiceStatusRequest{Authority: authority},
	)
	if err != nil || statusResponse.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING {
		t.Fatalf("Status before stop = %#v error=%v", statusResponse, err)
	}
	backend.FinishStop()
	statusResponse, err = client.Status(
		context.Background(),
		&velav1.ModelRuntimeServiceStatusRequest{Authority: authority},
	)
	if err != nil || statusResponse.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED {
		t.Fatalf("Status after stop = %#v error=%v", statusResponse, err)
	}
}

func TestModelRuntimeReusesResidentBackendAfterCanceledStageStops(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 30, 7, 10, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	first := signRuntimeAuthority(t, signer, clock.Now())
	backend := modelruntime.NewFakeEncoderRuntime()
	service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
	client, _ := serveRuntime(t, service)
	prepareAndStart(t, client, first)

	canceled, err := client.CancelStage(
		context.Background(),
		&velav1.ModelRuntimeServiceCancelStageRequest{
			Authority: first,
			Reason:    velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
		},
	)
	if err != nil || !canceled.GetCancellationAcknowledged() {
		t.Fatalf("CancelStage = %#v error=%v", canceled, err)
	}
	backend.FinishStop()
	stopped, err := client.Status(
		context.Background(),
		&velav1.ModelRuntimeServiceStatusRequest{Authority: first},
	)
	if err != nil || stopped.GetState() !=
		velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED {
		t.Fatalf("stopped Status = %#v error=%v", stopped, err)
	}

	secondUnsigned := proto.Clone(first).(*velav1.StageAuthority)
	secondUnsigned.StageRunId = "11000000-0000-0000-0000-000000000023"
	secondUnsigned.StageAttemptId = "11000000-0000-0000-0000-000000000024"
	secondUnsigned.StageAllocationId = "11000000-0000-0000-0000-000000000025"
	secondUnsigned.StageLeaseId = "11000000-0000-0000-0000-000000000026"
	secondUnsigned.StageFence++
	secondUnsigned.StageVersion++
	secondUnsigned.ExecutionNonce = bytes.Repeat([]byte{0x75}, 32)
	secondUnsigned.Signature = nil
	second, err := signer.Sign(secondUnsigned)
	if err != nil {
		t.Fatalf("sign second StageAuthority: %v", err)
	}
	prepared, err := client.PrepareStage(
		context.Background(),
		&velav1.ModelRuntimeServicePrepareStageRequest{
			Authority: second, ExecutionSpec: runtimeExecutionSpec(),
		},
	)
	if err != nil || prepared.GetDecision() !=
		velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
		prepared.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED ||
		backend.Component() != "encoder" {
		t.Fatalf("second PrepareStage = %#v component=%q error=%v", prepared, backend.Component(), err)
	}
}

func TestModelRuntimeRejectsFailedStatusWithoutStructuredEvidence(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 30, 7, 15, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	authority := signRuntimeAuthority(t, signer, clock.Now())
	backend := &fixedStatusBackend{
		Backend: modelruntime.NewFakeDiTRuntime(),
		status: modelruntime.BackendStatus{
			State:             velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED,
			Sequence:          3,
			BackendStage:      "dit",
			BoundedStatusJSON: []byte(`{"state":"failed"}`),
		},
	}
	client, _ := serveRuntime(t, newRuntimeService(t, clock, validator, runtimeBinding(), backend))
	prepareAndStart(t, client, authority)

	response, err := client.Status(
		context.Background(),
		&velav1.ModelRuntimeServiceStatusRequest{Authority: authority},
	)
	if err != nil || response.GetDecision() !=
		velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
		t.Fatalf("Status without failure evidence = %#v error=%v", response, err)
	}
}

func TestModelRuntimeReturnsStructuredFailureEvidence(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 30, 7, 20, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	authority := signRuntimeAuthority(t, signer, clock.Now())
	failedAt := clock.Now().Add(5 * time.Second)
	retryAt := failedAt.Add(30 * time.Second)
	fingerprint := bytes.Repeat([]byte{0x91}, 32)
	backend := &fixedStatusBackend{
		Backend: modelruntime.NewFakeDiTRuntime(),
		status: modelruntime.BackendStatus{
			State:             velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED,
			Sequence:          3,
			BackendStage:      "dit",
			BoundedStatusJSON: []byte(`{"state":"failed"}`),
			FailureEvidence: &modelruntime.FailureEvidence{
				FailureClass: "backend_oom", FailureFingerprint: fingerprint,
				Detail: "DiT allocation failed", WorkerReusable: true,
				ConsumedResourceUnits: 87, FailedAt: failedAt, RetryAt: retryAt,
			},
		},
	}
	client, _ := serveRuntime(t, newRuntimeService(t, clock, validator, runtimeBinding(), backend))
	prepareAndStart(t, client, authority)

	response, err := client.Status(
		context.Background(),
		&velav1.ModelRuntimeServiceStatusRequest{Authority: authority},
	)
	evidence := response.GetFailureEvidence()
	if err != nil || response.GetDecision() !=
		velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
		response.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED ||
		evidence.GetFailureClass() != "backend_oom" ||
		!bytes.Equal(evidence.GetFailureFingerprint(), fingerprint) ||
		evidence.GetDetail() != "DiT allocation failed" || !evidence.GetWorkerReusable() ||
		evidence.GetConsumedResourceUnits() != 87 ||
		!evidence.GetFailedAt().AsTime().Equal(failedAt) ||
		!evidence.GetRetryAt().AsTime().Equal(retryAt) {
		t.Fatalf("Status failure evidence = %#v response=%#v error=%v", evidence, response, err)
	}
}

func TestModelRuntimeSealsOnlyTheExactActiveAuthority(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 30, 7, 30, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	authority := signRuntimeAuthority(t, signer, clock.Now())
	backend := modelruntime.NewFakeDiTRuntime()
	service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
	client, _ := serveRuntime(t, service)
	prepareAndStart(t, client, authority)
	backend.MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/dit.bin"}`))

	sealed, err := client.SealOutput(context.Background(), &velav1.ModelRuntimeServiceSealOutputRequest{
		Authority: authority,
	})
	if err != nil || sealed.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
		sealed.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED ||
		sealed.GetReceipt().GetReceiptId() == "" || len(sealed.GetReceipt().GetManifestSha256()) != 32 {
		t.Fatalf("SealOutput = %#v error=%v", sealed, err)
	}
	statusResponse, err := client.Status(
		context.Background(),
		&velav1.ModelRuntimeServiceStatusRequest{Authority: authority},
	)
	if err != nil || statusResponse.GetLocalReceiptId() != sealed.GetReceipt().GetReceiptId() ||
		!bytes.Equal(statusResponse.GetLocalReceiptDigest(), sealed.GetReceipt().GetManifestSha256()) {
		t.Fatalf("Status after seal = %#v error=%v", statusResponse, err)
	}

	stale := proto.Clone(authority).(*velav1.StageAuthority)
	stale.StageFence++
	rejected, err := client.SealOutput(context.Background(), &velav1.ModelRuntimeServiceSealOutputRequest{
		Authority: stale,
	})
	if err != nil || rejected.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("stale SealOutput = %#v error=%v", rejected, err)
	}
}

func TestModelRuntimeSealReplayReturnsReceiptAfterComputeAuthorityRelease(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 30, 7, 30, 0, 0, time.UTC))
	signer, validator := runtimeAuthorityCrypto(t, clock)
	first := signRuntimeAuthority(t, signer, clock.Now())
	backend := modelruntime.NewFakeDiTRuntime()
	service := newRuntimeService(t, clock, validator, runtimeBinding(), backend)
	client, _ := serveRuntime(t, service)
	prepareAndStart(t, client, first)
	backend.MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/dit.bin"}`))

	sealed, err := client.SealOutput(context.Background(), &velav1.ModelRuntimeServiceSealOutputRequest{
		Authority: first,
	})
	if err != nil || sealed.GetDecision() !=
		velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
		sealed.GetReceipt() == nil {
		t.Fatalf("first SealOutput = %#v error=%v", sealed, err)
	}
	replayed, err := client.SealOutput(
		context.Background(),
		&velav1.ModelRuntimeServiceSealOutputRequest{Authority: first},
	)
	if err != nil || replayed.GetDecision() !=
		velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED ||
		!proto.Equal(replayed.GetReceipt(), sealed.GetReceipt()) {
		t.Fatalf("replayed SealOutput = %#v error=%v want receipt=%#v", replayed, err, sealed.GetReceipt())
	}

	secondUnsigned := proto.Clone(first).(*velav1.StageAuthority)
	secondUnsigned.StageRunId = "11000000-0000-0000-0000-000000000013"
	secondUnsigned.StageAttemptId = "11000000-0000-0000-0000-000000000014"
	secondUnsigned.StageAllocationId = "11000000-0000-0000-0000-000000000015"
	secondUnsigned.StageLeaseId = "11000000-0000-0000-0000-000000000016"
	secondUnsigned.StageFence++
	secondUnsigned.StageVersion++
	secondUnsigned.ExecutionNonce = bytes.Repeat([]byte{0x74}, 32)
	secondUnsigned.Signature = nil
	second, err := signer.Sign(secondUnsigned)
	if err != nil {
		t.Fatalf("sign second StageAuthority: %v", err)
	}
	prepareAndStart(t, client, second)
}

func newRuntimeService(
	t *testing.T,
	clock *manualClock,
	validator *stageauthority.Validator,
	binding stageauthority.RuntimeBinding,
	backend modelruntime.Backend,
) *modelruntime.Service {
	t.Helper()
	service, err := modelruntime.NewService(modelruntime.Config{
		Binding: func() stageauthority.RuntimeBinding {
			template := binding
			template.ModelRuntimeEpoch = 0
			return template
		}(),
		EpochStore: modelruntime.EpochStoreFunc(func(stageauthority.RuntimeBinding) (int64, error) {
			return binding.ModelRuntimeEpoch, nil
		}),
		Validator:     validator,
		Backend:       backend,
		Clock:         clock,
		CancelTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	t.Cleanup(service.Close)
	return service
}

type fixedStatusBackend struct {
	modelruntime.Backend
	status modelruntime.BackendStatus
}

func (backend *fixedStatusBackend) Status(
	context.Context,
	stageauthority.Verified,
) (modelruntime.BackendStatus, error) {
	return backend.status, nil
}

func serveRuntime(
	t *testing.T,
	service *modelruntime.Service,
) (velav1.ModelRuntimeServiceClient, func() velav1.ModelRuntimeServiceClient) {
	return serveRuntimeServer(t, service)
}

func serveRuntimeServer(
	t *testing.T,
	service velav1.ModelRuntimeServiceServer,
) (velav1.ModelRuntimeServiceClient, func() velav1.ModelRuntimeServiceClient) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	velav1.RegisterModelRuntimeServiceServer(server, service)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		if err := <-done; err != nil && err != grpc.ErrServerStopped {
			t.Errorf("serve ModelRuntime: %v", err)
		}
	})
	connections := make([]*grpc.ClientConn, 0, 2)
	dial := func() velav1.ModelRuntimeServiceClient {
		connection, err := grpc.NewClient(
			"passthrough:///bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("dial ModelRuntime: %v", err)
		}
		connections = append(connections, connection)
		return velav1.NewModelRuntimeServiceClient(connection)
	}
	t.Cleanup(func() {
		for _, connection := range connections {
			_ = connection.Close()
		}
	})
	return dial(), dial
}

func prepareAndStart(
	t *testing.T,
	client velav1.ModelRuntimeServiceClient,
	authority *velav1.StageAuthority,
) {
	t.Helper()
	prepared, err := client.PrepareStage(
		context.Background(),
		&velav1.ModelRuntimeServicePrepareStageRequest{
			Authority: authority, ExecutionSpec: runtimeExecutionSpec(),
		},
	)
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("PrepareStage = %#v error=%v", prepared, err)
	}
	started, err := client.StartStage(
		context.Background(),
		&velav1.ModelRuntimeServiceStartStageRequest{Authority: authority},
	)
	if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("StartStage = %#v error=%v", started, err)
	}
}

func runtimeAuthorityCrypto(
	t *testing.T,
	clock *manualClock,
) (*stageauthority.Signer, *stageauthority.Validator) {
	t.Helper()
	keys := map[string][]byte{"stage-key-9": bytes.Repeat([]byte{0x5a}, 32)}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	validator, err := stageauthority.NewValidator(keys, clock.Now)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	return signer, validator
}

func signRuntimeAuthority(
	t *testing.T,
	signer *stageauthority.Signer,
	now time.Time,
) *velav1.StageAuthority {
	return signRuntimeAuthorityForEpoch(t, signer, now, 9)
}

func signRuntimeAuthorityForEpoch(
	t *testing.T,
	signer *stageauthority.Signer,
	now time.Time,
	epoch int64,
) *velav1.StageAuthority {
	t.Helper()
	executionSpecDigest, err := stageauthority.ExecutionSpecDigest(runtimeExecutionSpec())
	if err != nil {
		t.Fatalf("ExecutionSpecDigest: %v", err)
	}
	authority := &velav1.StageAuthority{
		SchemaVersion:     1,
		JobId:             "11000000-0000-0000-0000-000000000001",
		AttemptId:         "11000000-0000-0000-0000-000000000002",
		StageRunId:        "11000000-0000-0000-0000-000000000003",
		StageAttemptId:    "11000000-0000-0000-0000-000000000004",
		StageAllocationId: "11000000-0000-0000-0000-000000000005",
		StageLeaseId:      "11000000-0000-0000-0000-000000000006",
		AttemptFence:      2, StageFence: 3, StageVersion: 4,
		WorkerInstanceId:    "21000000-0000-0000-0000-000000000001",
		WorkerInstanceEpoch: 5,
		DeviceSetDigest:     bytes.Repeat([]byte{0x61}, 32),
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "31000000-0000-0000-0000-000000000001", DeviceEpoch: 7,
		}},
		MembershipDigest: bytes.Repeat([]byte{0x62}, 32),
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: "41000000-0000-0000-0000-000000000001", MemberEpoch: 8,
			ModelRuntimeEpoch: epoch, IdentityDigest: bytes.Repeat([]byte{0x66}, 32),
		}},
		ModelResidencyId:              "51000000-0000-0000-0000-000000000001",
		ModelRuntimeIdentity:          "h3-runtime-1",
		ModelRuntimeBarrierGeneration: epoch,
		StageProfileRevisionId:        "61000000-0000-0000-0000-000000000001",
		CapacityObservationSequence:   10,
		CapacityVector:                map[string]int64{"active_stage_slots": 1, "gpu_count": 1},
		LeaseToken:                    bytes.Repeat([]byte{0x63}, 32), ExecutionNonce: bytes.Repeat([]byte{0x64}, 32),
		ExecutionSpecDigest: executionSpecDigest[:],
		SigningKeyId:        "stage-key-9", IssuedAt: timestamppb.New(now),
		ExpiresAt:         timestamppb.New(now.Add(30 * time.Second)),
		MonotonicValidFor: durationpb.New(30 * time.Second),
	}
	signed, err := signer.Sign(authority)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return signed
}

func runtimeBinding() stageauthority.RuntimeBinding {
	return stageauthority.RuntimeBinding{
		WorkerInstanceID: "21000000-0000-0000-0000-000000000001", WorkerInstanceEpoch: 5,
		WorkerMemberID: "41000000-0000-0000-0000-000000000001", WorkerMemberEpoch: 8,
		DeviceSetDigest:  bytes.Repeat([]byte{0x61}, 32),
		Devices:          []stageauthority.DeviceEpoch{{ID: "31000000-0000-0000-0000-000000000001", Epoch: 7}},
		MembershipDigest: bytes.Repeat([]byte{0x62}, 32),
		Members:          []stageauthority.MemberEpoch{{ID: "41000000-0000-0000-0000-000000000001", Epoch: 8}},
		ModelResidencyID: "51000000-0000-0000-0000-000000000001", ModelRuntimeIdentity: "h3-runtime-1",
		ModelRuntimeEpoch: 9, StageProfileRevisionID: "61000000-0000-0000-0000-000000000001",
	}
}

func runtimeExecutionSpec() *velav1.StageExecutionSpec {
	return &velav1.StageExecutionSpec{
		Inputs: []*velav1.StageInputArtifact{{
			StageArtifactId:          "71000000-0000-0000-0000-000000000001",
			ObjectVersion:            "object-version-1",
			Sha256:                   bytes.Repeat([]byte{0x65}, 32),
			SizeBytes:                4096,
			StageInterfaceRevisionId: "61000000-0000-0000-0000-000000000002",
		}},
		ParametersJson:             []byte(`{"steps":30}`),
		ExpectedOutputManifestJson: []byte(`{"kind":"LATENT"}`),
	}
}

type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*manualTimer
}

type manualTimer struct {
	clock    *manualClock
	deadline time.Time
	channel  chan time.Time
	stopped  bool
	fired    bool
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{now: now}
}

func (clock *manualClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *manualClock) NewTimer(duration time.Duration) modelruntime.Timer {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	timer := &manualTimer{
		clock: clock, deadline: clock.now.Add(duration), channel: make(chan time.Time, 1),
	}
	clock.timers = append(clock.timers, timer)
	return timer
}

func (clock *manualClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(duration)
	for _, timer := range clock.timers {
		if !timer.stopped && !timer.fired && !clock.now.Before(timer.deadline) {
			timer.fired = true
			timer.channel <- clock.now
		}
	}
}

func (timer *manualTimer) C() <-chan time.Time {
	return timer.channel
}

func (timer *manualTimer) Stop() bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	if timer.stopped || timer.fired {
		return false
	}
	timer.stopped = true
	return true
}
