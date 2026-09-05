package modelruntime_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTerminalNonAdmissionRPCRejectsInvalidScopes(t *testing.T) {
	for _, fault := range []string{"nil scope", "schema", "unknown", "identity unknown", "missing history", "signature", "future", "size", "allocation", "target member", "target epoch", "member identity", "subset", "non-durable"} {
		t.Run(fault, func(t *testing.T) {
			f := durableExecutionFixture(t, privateExecutionStateDirectory(t), true, "", 9, time.Time{})
			if fault == "non-durable" {
				f = newExecutionFloorFixture(t, "")
			}
			client := dialExecutionFloorServer(t, f.supervisor)
			disposition := unsignedTerminalAllocation(t, f)
			scope := &velav1.ModelRuntimeTerminalAllocationScope{SchemaVersion: 1, Identity: discoverExecutionFloorIdentity(t, client, f.bindings[1]),
				Disposition: disposition, StageAllocationId: disposition.Allocations[1].StageAllocationId}
			switch fault {
			case "nil scope":
				scope = nil
			case "schema":
				scope.SchemaVersion++
			case "unknown":
				scope.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "identity unknown":
				scope.Identity.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "missing history":
				scope.Disposition = nil
			case "signature":
				scope.Disposition.Signature[0] ^= 1
			case "future":
				scope.Disposition.ObservedAt = timestamppb.New(f.clock.Now().Add(time.Second))
				scope.Disposition = signTerminalNonAdmission(t, f, scope.Disposition)
			case "size":
				scope.Disposition.Signature = make([]byte, 65<<10)
			case "allocation":
				scope.StageAllocationId = uuid.NewString()
			case "target member":
				scope.Identity.WorkerMemberId = uuid.NewString()
			case "target epoch":
				scope.Identity.ModelRuntimeEpoch++
			case "member identity", "subset":
				for _, allocation := range scope.Disposition.Allocations {
					if fault == "member identity" {
						allocation.Members[0].IdentityDigest[0] ^= 1
					} else {
						allocation.Members[0].DeviceSubsetDigest[0] ^= 1
					}
				}
				scope.Disposition = signTerminalNonAdmission(t, f, scope.Disposition)
			}
			if response, err := client.CheckpointStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{Scope: scope}); err == nil || response != nil {
				t.Fatalf("invalid checkpoint scope accepted: %v %v", response, err)
			}
			if response, err := client.InspectStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest{Scope: scope}); err == nil || response != nil {
				t.Fatalf("invalid inspection scope accepted: %v %v", response, err)
			}
			if f.backend.calls.Load() != 0 || f.backend.closed.Load() {
				t.Fatal("invalid query touched backend")
			}
		})
	}
}

func TestTerminalNonAdmissionRPCSeparatesCurrentReaderFromOriginalResidency(t *testing.T) {
	f := durableExecutionFixture(t, privateExecutionStateDirectory(t), true, "", 9, time.Time{})
	client := dialExecutionFloorServer(t, f.supervisor)
	disposition := unsignedTerminalAllocation(t, f)
	scope := &velav1.ModelRuntimeTerminalAllocationScope{SchemaVersion: 1, Identity: discoverExecutionFloorIdentity(t, client, f.bindings[0]),
		Disposition: disposition, StageAllocationId: disposition.Allocations[1].StageAllocationId}
	if _, err := client.InstallExecutionFloor(t.Context(), f.validator, scope.Identity, disposition); err != nil {
		t.Fatal(err)
	}
	if response, err := client.CheckpointStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{Scope: scope}); err == nil || response != nil {
		t.Fatalf("alternate profile created new proof: %v %v", response, err)
	}
	if response, err := client.InspectStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest{Scope: scope}); err != nil || response.GetResult().GetCheckpoint() != nil {
		t.Fatalf("alternate reader inferred proof: %v %v", response, err)
	}
	original := proto.Clone(scope).(*velav1.ModelRuntimeTerminalAllocationScope)
	original.Identity = discoverExecutionFloorIdentity(t, client, f.bindings[1])
	created, err := client.CheckpointStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{Scope: original})
	if err != nil || created.GetResult().GetCheckpoint() == nil {
		t.Fatalf("original resident checkpoint: %v %v", created, err)
	}
	read, err := client.InspectStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest{Scope: scope})
	if err != nil || !proto.Equal(read.GetResult().GetCheckpoint(), created.GetResult().GetCheckpoint()) || !proto.Equal(read.GetResult().GetIdentity(), scope.Identity) {
		t.Fatalf("alternate trusted reader changed proof: %v %v", read, err)
	}
	// Fresh terminal queries after checkpointing keep the stored signed witness.
	f.clock.Advance(time.Second)
	fresh := proto.Clone(scope).(*velav1.ModelRuntimeTerminalAllocationScope)
	fresh.Disposition.ObservedAt = timestamppb.New(f.clock.Now())
	fresh.Disposition.ExpiresAt = timestamppb.New(f.clock.Now().Add(time.Minute))
	fresh.Disposition.ControlSessionEpoch++
	fresh.Disposition = signTerminalNonAdmission(t, f, fresh.Disposition)
	read, err = client.InspectStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest{Scope: fresh})
	if err != nil || !proto.Equal(read.GetResult().GetCheckpoint(), created.GetResult().GetCheckpoint()) {
		t.Fatalf("fresh query replaced historical witness: %v %v", read, err)
	}
}
