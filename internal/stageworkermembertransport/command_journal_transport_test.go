package stageworkermembertransport

import (
	"testing"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestMemberWorkerJournalFailureTLSUnixPreservesExecutionRecovery(t *testing.T) {
	for _, fault := range []string{"closed", "replaced"} {
		t.Run(fault, func(t *testing.T) {
			f, floor, _ := newMemberFloorFixture(t)
			gate, config := commandWorkerJournal(t, f, true)
			chain := startMemberFloorChainWithWorkerJournal(t, f, floor.Command.Disposition, privateMemberFloorDirectory(t), true, false, gate)
			scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity), Authority: f.authority}
			prepared, err := chain.client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority, ExecutionSpec: f.spec})
			if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("prepare with live Worker journal: %v %v", prepared, err)
			}
			if fault == "closed" {
				if err := gate.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				replaceCommandJournalState(t, config.Directory)
			}
			_, prepareErr := chain.client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority, ExecutionSpec: f.spec})
			_, startErr := chain.client.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: f.authority})
			_, statusErr := chain.client.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: f.authority})
			for _, err := range []error{prepareErr, startErr, statusErr} {
				if status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("lost Worker ownership allowed execution/renewal: %v", err)
				}
			}
			inspection, err := chain.client.InspectExecution(t.Context(), &velav1.ModelRuntimeServiceInspectExecutionRequest{SchemaVersion: 1, Authority: f.authority})
			if err != nil || !inspection.GetKnown() || inspection.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED {
				t.Fatalf("journal failure blocked nonrenewing inspection: %v %v", inspection, err)
			}
			if response, err := chain.client.InstallStageExecutionFloor(t.Context(), floor.Command); err != nil || !response.GetDurable() {
				t.Fatalf("journal failure blocked restriction: %v %v", response, err)
			}
			cancelRequest := &velav1.ModelRuntimeServiceCancelStageRequest{Authority: f.authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP}
			nonleader := chain.dial(t, chain.followerCredentials)
			if _, err := nonleader.CancelStage(t.Context(), cancelRequest); status.Code(err) != codes.PermissionDenied || chain.backend.cancelCalls.Load() != 0 {
				t.Fatalf("recovery bypassed peer authorization: %v", err)
			}
			canceled, err := chain.client.CancelStage(t.Context(), cancelRequest)
			if err != nil || !canceled.GetCancellationAcknowledged() || chain.backend.cancelCalls.Load() != 1 {
				t.Fatalf("journal failure blocked exact cancellation: %v %v", canceled, err)
			}
			chain.backend.FinishStop()
			read, err := chain.client.InspectStageExecutionDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: scope})
			if err != nil || read.GetResult().GetCheckpoint() != nil {
				t.Fatalf("STOPPED alone created a drain proof: %v %v", read, err)
			}
			if _, err := nonleader.DrainStageExecution(t.Context(), &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope}); status.Code(err) != codes.PermissionDenied {
				t.Fatalf("journal failure bypassed drain authorization: %v", err)
			}
			drained, err := chain.client.DrainStageExecution(t.Context(), &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope})
			if err != nil || drained.GetResult().GetCheckpoint() == nil || chain.backend.closed.Load() {
				t.Fatalf("journal failure blocked durable drain: %v %v", drained, err)
			}
			read, err = chain.client.InspectStageExecutionDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: scope})
			if err != nil || !proto.Equal(read.GetResult().GetCheckpoint(), drained.GetResult().GetCheckpoint()) {
				t.Fatalf("exact checkpoint inspection: %v %v", read, err)
			}
			allocation, err := chain.client.InspectStageAllocationDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageAllocationDrainRequest{Scope: scope})
			if err != nil || !proto.Equal(allocation.GetResult().GetCheckpoint(), drained.GetResult().GetCheckpoint()) {
				t.Fatalf("allocation checkpoint inspection: %v %v", allocation, err)
			}
		})
	}
}

func TestMemberWorkerJournalFailureTLSUnixPreservesNonAdmissionRecovery(t *testing.T) {
	f, floor, _ := newMemberFloorFixture(t)
	gate, _ := commandWorkerJournal(t, f, true)
	chain := startMemberFloorChainWithWorkerJournal(t, f, floor.Command.Disposition, privateMemberFloorDirectory(t), true, false, gate)
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	if response, err := chain.client.InstallStageExecutionFloor(t.Context(), floor.Command); err != nil || !response.GetDurable() {
		t.Fatalf("closed Worker journal blocked restriction: %v %v", response, err)
	}
	scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity), Authority: f.authority}
	read, err := chain.client.InspectStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceInspectStageNonAdmissionRequest{Scope: scope})
	if err != nil || read.GetResult().GetCheckpoint() != nil {
		t.Fatalf("inspection inferred non-admission: %v %v", read, err)
	}
	checkpoint, err := chain.client.CheckpointStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: scope})
	if err != nil || checkpoint.GetResult().GetCheckpoint() == nil || chain.backend.cancelCalls.Load() != 0 || chain.backend.closed.Load() {
		t.Fatalf("closed Worker journal blocked non-admission checkpoint: %v %v", checkpoint, err)
	}
	read, err = chain.client.InspectStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceInspectStageNonAdmissionRequest{Scope: scope})
	if err != nil || !proto.Equal(read.GetResult().GetCheckpoint(), checkpoint.GetResult().GetCheckpoint()) {
		t.Fatalf("non-admission checkpoint inspection: %v %v", read, err)
	}
	nonleader := chain.dial(t, chain.followerCredentials)
	if _, err := nonleader.CheckpointStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: scope}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("non-admission recovery bypassed peer authorization: %v", err)
	}
}
