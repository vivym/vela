package stageworkermembertransport

import (
	"bytes"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMemberCancellationTLSUnixCannotRenewAfterWorkerJournalFailure(t *testing.T) {
	for _, outcome := range []string{"acknowledged", "failed"} {
		t.Run(outcome, func(t *testing.T) {
			f, floor, _ := newMemberFloorFixture(t)
			gate, _ := commandWorkerJournal(t, f, true)
			chain := startMemberFloorChainWithWorkerJournal(t, f, floor.Command.Disposition, privateMemberFloorDirectory(t), true, false, gate)
			prepared, err := chain.client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority, ExecutionSpec: f.spec})
			if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("prepare cancellation fixture: %v %v", prepared, err)
			}
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			successor := proto.Clone(f.authority).(*velav1.StageAuthority)
			successor.StageVersion++
			successor.IssuedAt = timestamppb.New(successor.IssuedAt.AsTime().Add(time.Second))
			successor.ExpiresAt = timestamppb.New(successor.ExpiresAt.AsTime().Add(time.Second))
			successor, err = f.signer.Sign(successor)
			if err != nil {
				t.Fatal(err)
			}
			chain.backend.failCancel.Store(outcome == "failed")
			canceled, err := chain.client.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
				Authority: successor, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
			})
			want := velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
			state := velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING
			if outcome == "failed" {
				want, state = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED
			}
			requestDigest, errDigest := stageauthority.Digest(successor)
			if err != nil || errDigest != nil || canceled.GetDecision() != want || canceled.GetCancellationAcknowledged() != (outcome == "acknowledged") || !bytes.Equal(canceled.GetAuthorityDigest(), requestDigest[:]) {
				t.Fatalf("cancel with successor after Worker journal failure: %v %v", canceled, err)
			}
			for _, authority := range []*velav1.StageAuthority{f.authority, successor} {
				read, err := chain.client.InspectExecution(t.Context(), &velav1.ModelRuntimeServiceInspectExecutionRequest{SchemaVersion: 1, Authority: authority})
				known := authority == f.authority
				if err != nil || read.GetKnown() != known || known && read.GetState() != state {
					t.Fatalf("cancellation changed installed inspection identity: %v %v", read, err)
				}
			}
			if _, err := chain.client.InstallStageExecutionFloor(t.Context(), floor.Command); err != nil {
				t.Fatal(err)
			}
			before := chain.backend.cancelCalls.Load()
			if response, err := chain.client.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
				Authority: successor, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
			}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE || chain.backend.cancelCalls.Load() != before {
				t.Fatalf("floor permitted an uninstalled cancellation envelope: %v %v", response, err)
			}
			chain.backend.failCancel.Store(false)
			if response, err := chain.client.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{
				Authority: f.authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
			}); err != nil || !response.GetCancellationAcknowledged() {
				t.Fatalf("original exact authority could not recover cancellation: %v %v", response, err)
			}
			chain.backend.FinishStop()
			scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity), Authority: f.authority}
			drained, err := chain.client.DrainStageExecution(t.Context(), &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope})
			if err != nil || drained.GetResult().GetCheckpoint() == nil || !proto.Equal(drained.GetResult().GetCheckpoint().GetAuthority(), f.authority) || chain.backend.closed.Load() {
				t.Fatalf("cancellation lost original drain authority: %v %v", drained, err)
			}
			scope.Authority = successor
			read, err := chain.client.InspectStageExecutionDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: scope})
			if err != nil || read.GetResult().GetCheckpoint() != nil {
				t.Fatalf("uninstalled successor inherited exact drain: %v %v", read, err)
			}
			allocation, err := chain.client.InspectStageAllocationDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageAllocationDrainRequest{Scope: scope})
			if err != nil || !proto.Equal(allocation.GetResult().GetCheckpoint(), drained.GetResult().GetCheckpoint()) {
				t.Fatalf("allocation inspection lost actual original checkpoint: %v %v", allocation, err)
			}
		})
	}
}
