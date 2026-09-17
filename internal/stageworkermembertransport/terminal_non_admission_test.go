package stageworkermembertransport

import (
	"context"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntimetransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMemberTerminalNonAdmissionTLSUnixUnsignedAllocationRecovery(t *testing.T) {
	f, floor, _ := newMemberFloorFixture(t)
	disposition := floor.Command.Disposition
	directory := privateMemberFloorDirectory(t)
	chain := startMemberFloorChain(t, f, disposition, directory, true, false)
	scope := &velav1.ModelRuntimeTerminalAllocationScope{SchemaVersion: 1, Identity: proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity),
		Disposition: disposition, StageAllocationId: disposition.Allocations[1].StageAllocationId}
	if response, err := chain.client.CheckpointStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{Scope: scope}); err != nil || response.GetResult().GetCheckpoint() != nil {
		t.Fatalf("no floor proved terminal absence: %v %v", response, err)
	}
	prepared, err := chain.client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority, ExecutionSpec: f.spec})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("prepare: %v %v", prepared, err)
	}
	if _, err := chain.client.InstallStageExecutionFloor(t.Context(), floor.Command); err != nil {
		t.Fatal(err)
	}
	admitted := proto.Clone(scope).(*velav1.ModelRuntimeTerminalAllocationScope)
	admitted.StageAllocationId = f.authority.GetStageAllocationId()
	if response, err := chain.client.CheckpointStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{Scope: admitted}); err != nil || response.GetResult().GetCheckpoint() != nil {
		t.Fatalf("persisted Prepare claimed never admitted: %v %v", response, err)
	}
	if read, err := chain.client.InspectStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest{Scope: scope}); err != nil || read.GetResult().GetCheckpoint() != nil {
		t.Fatalf("read created proof: %v %v", read, err)
	}
	nonleader := chain.dial(t, chain.followerCredentials)
	if response, err := nonleader.CheckpointStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{Scope: scope}); response != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("nonleader checkpointed: %v %v", response, err)
	}
	if response, err := nonleader.InspectStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest{Scope: scope}); response != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("nonleader inspected: %v %v", response, err)
	}
	chain.dropNonAdmissionResponse.Store(true)
	if response, err := chain.client.CheckpointStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{Scope: scope}); response != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("lost reply returned proof: %v %v", response, err)
	}
	read, err := chain.client.InspectStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest{Scope: scope})
	if err != nil || read.GetResult().GetCheckpoint() == nil || chain.backend.cancelCalls.Load() != 0 || chain.backend.closed.Load() {
		t.Fatalf("proof read or backend isolation: %v %v", read, err)
	}
	saved := proto.Clone(read.GetResult().GetCheckpoint()).(*velav1.ModelRuntimeTerminalNonAdmissionCheckpoint)
	response, err := chain.client.CheckpointStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{Scope: scope})
	if err != nil || !proto.Equal(saved, response.GetResult().GetCheckpoint()) {
		t.Fatalf("checkpoint replay: %v %v", response, err)
	}
	chain.close()
	f.runtime.identity.ModelRuntimeEpoch++
	f.server.validator = floorValidatorAt(t, campaignNow().Add(2*time.Minute))
	chain = startMemberFloorChain(t, f, disposition, directory, false, false)
	scope.Identity = proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity)
	read, err = chain.client.InspectStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest{Scope: scope})
	if err != nil || !proto.Equal(saved, read.GetResult().GetCheckpoint()) {
		t.Fatalf("expired signed history at next epoch: %v %v", read, err)
	}
	if response, err := chain.client.CheckpointStageTerminalNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{Scope: scope}); response != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expired checkpoint request: %v %v", response, err)
	}
}

func terminalMemberResult(t *testing.T, f *serverFixture, scope *velav1.ModelRuntimeTerminalAllocationScope) *velav1.ModelRuntimeTerminalNonAdmissionResult {
	t.Helper()
	verified, err := modelruntimetransport.ValidateTerminalAllocationScope(f.server.validator, scope, true)
	if err != nil {
		t.Fatal(err)
	}
	return &velav1.ModelRuntimeTerminalNonAdmissionResult{SchemaVersion: 1, Identity: proto.Clone(scope.Identity).(*velav1.ModelRuntimeIdentity),
		DispositionDigest: append([]byte(nil), verified.Digest[:]...), StageAllocationId: scope.StageAllocationId, Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
		Checkpoint: &velav1.ModelRuntimeTerminalNonAdmissionCheckpoint{SchemaVersion: 1, Disposition: proto.Clone(scope.Disposition).(*velav1.StageTerminalDisposition),
			DispositionDigest: append([]byte(nil), verified.Digest[:]...), WorkerMemberId: scope.Identity.WorkerMemberId, StageAllocationId: scope.StageAllocationId,
			ExecutionSequence: scope.Disposition.Allocations[1].ExecutionSequence, InstalledCutoff: scope.Disposition.Cutoff,
			Contract: modelruntimetransport.TerminalNonAdmissionContract, ObservedAt: timestamppb.New(campaignNow())}}
}

func TestMemberTerminalNonAdmissionRejectsMalformedProofAtBothHops(t *testing.T) {
	for _, historical := range []bool{false, true} {
		for _, hop := range []string{"runtime", "member"} {
			for _, fault := range []string{"nil", "schema", "digest", "allocation", "owner", "unknown", "checkpoint-schema", "signature", "checkpoint-digest", "member", "sequence", "floor", "contract", "timestamp", "early-time", "rejected", "decision", "future", "project", "mutation", "late"} {
				t.Run(hop+"/"+fault+"/"+map[bool]string{false: "checkpoint", true: "read"}[historical], func(t *testing.T) {
					f, floor, _ := newMemberFloorFixture(t)
					scope := &velav1.ModelRuntimeTerminalAllocationScope{SchemaVersion: 1, Identity: proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity),
						Disposition: floor.Command.Disposition, StageAllocationId: floor.Command.Disposition.Allocations[1].StageAllocationId}
					result := terminalMemberResult(t, f, scope)
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					switch fault {
					case "nil":
						result = nil
					case "schema":
						result.SchemaVersion++
					case "digest":
						result.DispositionDigest[0] ^= 1
					case "allocation":
						result.StageAllocationId = scope.Disposition.StageAllocationId
					case "owner":
						result.Identity.ModelRuntimeEpoch++
					case "unknown":
						result.ProtoReflect().SetUnknown([]byte{0x78, 1})
					case "checkpoint-schema":
						result.Checkpoint.SchemaVersion++
					case "signature":
						result.Checkpoint.Disposition.Signature[0] ^= 1
					case "checkpoint-digest":
						result.Checkpoint.DispositionDigest[0] ^= 1
					case "member":
						result.Checkpoint.WorkerMemberId = f.leader.ID
					case "sequence":
						result.Checkpoint.ExecutionSequence++
					case "floor":
						result.Checkpoint.InstalledCutoff = 0
					case "contract":
						result.Checkpoint.Contract = modelruntimetransport.ExecutionDrainContract
					case "timestamp":
						result.Checkpoint.ObservedAt = nil
					case "early-time":
						result.Checkpoint.ObservedAt.Seconds -= 2
					case "rejected":
						result.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
					case "decision":
						result.Decision = 99
					case "future", "project":
						if fault == "future" {
							result.Checkpoint.Disposition.ObservedAt.Seconds += 3600
							result.Checkpoint.Disposition.ExpiresAt.Seconds += 3600
							result.Checkpoint.ObservedAt.Seconds += 3600
						} else {
							result.Checkpoint.Disposition.ProjectId = result.Checkpoint.Disposition.OrganizationId
						}
						var err error
						result.Checkpoint.Disposition, err = f.signer.SignTerminalDisposition(result.Checkpoint.Disposition)
						if err != nil {
							t.Fatal(err)
						}
						verified, err := f.server.validator.ValidateTerminalDispositionSignature(result.Checkpoint.Disposition)
						if err != nil {
							t.Fatal(err)
						}
						result.Checkpoint.DispositionDigest = append([]byte(nil), verified.Digest[:]...)
					}
					reply := func(received *velav1.ModelRuntimeTerminalAllocationScope) *velav1.ModelRuntimeTerminalNonAdmissionResult {
						if fault == "late" {
							cancel()
						}
						if fault == "mutation" {
							received.Identity.ModelRuntimeEpoch++
							result.Identity.ModelRuntimeEpoch++
						}
						return result
					}
					var err error
					if hop == "runtime" {
						f.server.runtime = &terminalNonAdmissionRuntimeReply{reply: reply}
						if historical {
							_, err = f.server.InspectStageTerminalNonAdmission(ctx, &velav1.StageWorkerMemberServiceInspectStageTerminalNonAdmissionRequest{TargetWorkerMemberId: f.local.ID, Command: &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest{Scope: scope}})
						} else {
							_, err = f.server.CheckpointStageTerminalNonAdmission(ctx, &velav1.StageWorkerMemberServiceCheckpointStageTerminalNonAdmissionRequest{TargetWorkerMemberId: f.local.ID, Command: &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{Scope: scope}})
						}
					} else {
						client := floorTestClient(f, &terminalNonAdmissionMemberReply{reply: reply})
						if historical {
							_, err = client.InspectStageTerminalNonAdmission(ctx, &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest{Scope: scope})
						} else {
							_, err = client.CheckpointStageTerminalNonAdmission(ctx, &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest{Scope: scope})
						}
					}
					want := codes.DataLoss
					if fault == "late" {
						want = codes.Canceled
					}
					if status.Code(err) != want {
						t.Fatalf("invalid terminal proof escaped: %v", err)
					}
				})
			}
		}
	}
}

type terminalNonAdmissionRuntimeReply struct {
	velav1.ModelRuntimeServiceClient
	reply func(*velav1.ModelRuntimeTerminalAllocationScope) *velav1.ModelRuntimeTerminalNonAdmissionResult
}

func (r *terminalNonAdmissionRuntimeReply) CheckpointStageTerminalNonAdmission(_ context.Context, request *velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionResponse, error) {
	return &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionResponse{Result: r.reply(request.Scope)}, nil
}
func (r *terminalNonAdmissionRuntimeReply) InspectStageTerminalNonAdmission(_ context.Context, request *velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionResponse, error) {
	return &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionResponse{Result: r.reply(request.Scope)}, nil
}

type terminalNonAdmissionMemberReply struct {
	velav1.StageWorkerMemberServiceClient
	reply func(*velav1.ModelRuntimeTerminalAllocationScope) *velav1.ModelRuntimeTerminalNonAdmissionResult
}

func (r *terminalNonAdmissionMemberReply) CheckpointStageTerminalNonAdmission(_ context.Context, request *velav1.StageWorkerMemberServiceCheckpointStageTerminalNonAdmissionRequest, _ ...grpc.CallOption) (*velav1.StageWorkerMemberServiceCheckpointStageTerminalNonAdmissionResponse, error) {
	return &velav1.StageWorkerMemberServiceCheckpointStageTerminalNonAdmissionResponse{Result: &velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionResponse{Result: r.reply(request.Command.Scope)}}, nil
}
func (r *terminalNonAdmissionMemberReply) InspectStageTerminalNonAdmission(_ context.Context, request *velav1.StageWorkerMemberServiceInspectStageTerminalNonAdmissionRequest, _ ...grpc.CallOption) (*velav1.StageWorkerMemberServiceInspectStageTerminalNonAdmissionResponse, error) {
	return &velav1.StageWorkerMemberServiceInspectStageTerminalNonAdmissionResponse{Result: &velav1.ModelRuntimeServiceInspectStageTerminalNonAdmissionResponse{Result: r.reply(request.Command.Scope)}}, nil
}
