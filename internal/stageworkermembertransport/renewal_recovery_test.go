package stageworkermembertransport

import (
	"testing"
	"time"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMemberRenewalRecoveryTLSUnixRetainsExactBackendDrain(t *testing.T) {
	for index, outcome := range []string{"not-applied", "applied-response-lost"} {
		t.Run(outcome, func(t *testing.T) {
			f, floor, _ := newMemberFloorFixture(t)
			gate, _ := commandWorkerJournal(t, f, true)
			chain := startMemberFloorChainWithWorkerJournal(t, f, floor.Command.Disposition, privateMemberFloorDirectory(t), true, false, gate)
			if response, err := chain.client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority, ExecutionSpec: f.spec}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("prepare renewal recovery: %v %v", response, err)
			}
			renewed := proto.Clone(f.authority).(*velav1.StageAuthority)
			renewed.StageVersion++
			renewed.IssuedAt = timestamppb.New(renewed.IssuedAt.AsTime().Add(time.Second))
			renewed.ExpiresAt = timestamppb.New(renewed.ExpiresAt.AsTime().Add(time.Second))
			renewed, err := f.signer.Sign(renewed)
			if err != nil {
				t.Fatal(err)
			}
			chain.backend.renewalResponseFault.Store(int32(index + 1))
			if response, err := chain.client.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: renewed}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED {
				t.Fatalf("renewal did not report its lost response: %v %v", response, err)
			}
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := chain.client.InstallStageExecutionFloor(t.Context(), floor.Command); err != nil {
				t.Fatal(err)
			}
			request := &velav1.ModelRuntimeServiceCancelStageRequest{Authority: renewed, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP}
			nonleader := chain.dial(t, chain.followerCredentials)
			query := &velav1.ModelRuntimeServiceInspectAllocationExecutionRequest{SchemaVersion: 1, Authority: renewed}
			if _, err := nonleader.InspectAllocationExecution(t.Context(), query); status.Code(err) != codes.PermissionDenied {
				t.Fatalf("nonleader discovered backend authority: %v", err)
			}
			if _, err := nonleader.CancelStage(t.Context(), request); status.Code(err) != codes.PermissionDenied {
				t.Fatalf("nonleader bypassed renewal recovery authorization: %v", err)
			}
			if response, err := chain.client.CancelStage(t.Context(), request); err != nil || !response.GetCancellationAcknowledged() || chain.backend.cancelCalls.Load() != 1 {
				t.Fatalf("exact cancellation failed after journal closure and floor: %v %v", response, err)
			}
			actual := f.authority
			if index == 1 {
				actual = renewed
			}
			discovered, err := chain.client.InspectAllocationExecution(t.Context(), query)
			if err != nil || !proto.Equal(discovered.GetObservedAuthority(), actual) {
				t.Fatalf("latest-only caller could not discover exact backend identity: %v %v", discovered, err)
			}
			actual = discovered.GetObservedAuthority()
			read, err := chain.client.InspectExecution(t.Context(), &velav1.ModelRuntimeServiceInspectExecutionRequest{SchemaVersion: 1, Authority: actual})
			if err != nil || !read.GetKnown() || read.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING {
				t.Fatalf("recovery hid the actual backend identity: %v %v", read, err)
			}
			chain.backend.FinishStop()
			scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity), Authority: actual}
			drained, err := chain.client.DrainStageExecution(t.Context(), &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope})
			if err != nil || drained.GetResult().GetCheckpoint() == nil || !proto.Equal(drained.GetResult().GetCheckpoint().GetAuthority(), actual) || chain.backend.closed.Load() {
				t.Fatalf("recovery lost its exact durable drain: %v %v", drained, err)
			}
			scope.Authority = renewed
			allocation, err := chain.client.InspectStageAllocationDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageAllocationDrainRequest{Scope: scope})
			if err != nil || !proto.Equal(allocation.GetResult().GetCheckpoint(), drained.GetResult().GetCheckpoint()) {
				t.Fatalf("allocation inspection rewrote the recovered identity: %v %v", allocation, err)
			}
		})
	}
}
