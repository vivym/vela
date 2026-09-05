package modelruntime_test

import (
	"context"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestExecutionDrainRPCPersistsBeforeReplyAndReadsAcrossEpochs(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime(), failure: "error"}
	f := newExecutionDrainFixture(t, directory, backend)
	client := dialExecutionFloorServer(t, f.supervisor)
	scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: discoverExecutionFloorIdentity(t, client, f.bindings[0]), Authority: f.authorities[0]}
	readyDrainOutput(t, f, backend.FakeRuntime, scope.Authority)
	sealed, err := client.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: scope.Authority})
	if err != nil || sealed.GetReceipt() != nil {
		t.Fatalf("unproven Seal: %v %v", sealed, err)
	}
	before := backend.drainCalls.Load()
	read, err := client.InspectStageExecutionDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: scope})
	if err != nil || read.GetResult().GetCheckpoint() != nil || backend.drainCalls.Load() != before {
		t.Fatalf("inspection entered backend: %v %v", read, err)
	}
	if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(2 * time.Minute)
	backend.failure = ""
	for range 2 {
		response, err := client.DrainStageExecution(t.Context(), &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope})
		if err != nil || response.GetResult().GetCheckpoint() == nil {
			t.Fatalf("historical terminal retry: %v %v", response, err)
		}
		verified, err := modelruntimetransport.ValidateExecutionDrainScope(f.validator, scope, 0, false)
		if err != nil || modelruntimetransport.ValidateExecutionDrainResult(scope, verified.Digest, response.GetResult()) != nil {
			t.Fatalf("invalid RPC result: %v", err)
		}
		assertExecutionDrainCheckpoint(t, f.supervisor, scope.Authority, true)
	}
	if backend.drainCalls.Load() != before+1 || backend.closed.Load() {
		t.Fatal("checkpoint retry repeated physical drain or unloaded model")
	}
	f.supervisor.Close()
	recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now())
	client = dialExecutionFloorServer(t, recovered.supervisor)
	scope.Identity = discoverExecutionFloorIdentity(t, client, recovered.bindings[0])
	read, err = client.InspectStageExecutionDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: scope})
	if err != nil || read.GetResult().GetCheckpoint() == nil || read.GetResult().GetIdentity().GetModelRuntimeEpoch() != 10 ||
		read.GetResult().GetCheckpoint().GetAuthority().GetMembers()[0].GetModelRuntimeEpoch() != 9 {
		t.Fatalf("recovery mixed owner and execution epochs: %v %v", read, err)
	}
	if response, err := client.DrainStageExecution(t.Context(), &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope}); response != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("historical epoch entered backend: %v %v", response, err)
	}
	scope.Authority = recovered.authority(t, 0, 12)
	read, err = client.InspectStageExecutionDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: scope})
	if err != nil || read.GetResult().GetCheckpoint() != nil {
		t.Fatalf("missing history inferred drained: %v %v", read, err)
	}
}

func TestExecutionDrainRPCAfterExpiredCancellationDoesNotInventReuse(t *testing.T) {
	backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime()}
	f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
	client := dialExecutionFloorServer(t, f.supervisor)
	prepareFloorRuntime(t, f.supervisor, f.authorities[0])
	canceled, err := client.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{Authority: f.authorities[0], Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP})
	if err != nil || !canceled.GetCancellationAcknowledged() {
		t.Fatalf("cancel: %v %v", canceled, err)
	}
	scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: discoverExecutionFloorIdentity(t, client, f.bindings[0]), Authority: f.authorities[0]}
	response, err := client.DrainStageExecution(t.Context(), &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope})
	if err != nil || response.GetResult().GetCheckpoint() != nil {
		t.Fatalf("cancel acknowledgement inferred drain: %v %v", response, err)
	}
	backend.FinishStop()
	f.clock.Advance(2 * time.Minute)
	response, err = client.DrainStageExecution(t.Context(), &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope})
	if err != nil || response.GetResult().GetCheckpoint() == nil {
		t.Fatalf("expired cancellation could not checkpoint real drain: %v %v", response, err)
	}
	prepared, err := f.supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority(t, 1, 12), ExecutionSpec: runtimeExecutionSpec()})
	if err != nil || prepared.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("drain invented healthy reusable slot: %v %v", prepared, err)
	}
	if backend.closed.Load() {
		t.Fatal("drain unloaded residency")
	}
}

func TestExecutionDrainRPCRejectsInvalidScopesWithoutBackendEntry(t *testing.T) {
	for _, mutation := range []string{"scope", "schema", "unknown", "identity", "epoch", "member", "signature", "future", "v1", "size", "cancel"} {
		t.Run(mutation, func(t *testing.T) {
			backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime()}
			f := newExecutionDrainFixture(t, privateExecutionStateDirectory(t), backend)
			client := dialExecutionFloorServer(t, f.supervisor)
			scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: discoverExecutionFloorIdentity(t, client, f.bindings[0]), Authority: proto.Clone(f.authorities[0]).(*velav1.StageAuthority)}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch mutation {
			case "scope":
				scope = nil
			case "schema":
				scope.SchemaVersion++
			case "unknown":
				scope.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "identity":
				scope.Identity.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "epoch":
				scope.Identity.ModelRuntimeEpoch++
			case "member":
				scope.Identity.WorkerMemberId = "41000000-0000-0000-0000-000000000002"
			case "signature":
				scope.Authority.Signature[0] ^= 1
			case "future":
				scope.Authority = signRuntimeAuthority(t, f.signer, f.clock.Now().Add(time.Hour))
			case "v1":
				scope.Authority.SchemaVersion = 1
			case "size":
				scope.Authority.Signature = make([]byte, 65<<10)
			case "cancel":
				cancel()
			}
			response, err := client.DrainStageExecution(ctx, &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope})
			if err == nil || response != nil || backend.drainCalls.Load() != 0 {
				t.Fatalf("invalid drain reached backend: %v %v", response, err)
			}
			read, err := client.InspectStageExecutionDrain(ctx, &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: scope})
			if err == nil || read != nil {
				t.Fatalf("invalid inspection accepted: %v %v", read, err)
			}
		})
	}
}
