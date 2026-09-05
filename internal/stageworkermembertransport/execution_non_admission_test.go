package stageworkermembertransport

import (
	"context"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestMemberNonAdmissionTLSUnixPersistsBeforeReplyAndRecovers(t *testing.T) {
	f, floor, _ := newMemberFloorFixture(t)
	directory := privateMemberFloorDirectory(t)
	chain := startMemberFloorChain(t, f, floor.Command.Disposition, directory, true, false)
	scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity), Authority: f.authority}
	if response, err := chain.client.CheckpointStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: scope}); err != nil || response.GetResult().GetCheckpoint() != nil {
		t.Fatalf("no floor proved absence: %v %v", response, err)
	}
	if _, err := chain.client.InstallStageExecutionFloor(t.Context(), floor.Command); err != nil {
		t.Fatal(err)
	}
	read, err := chain.client.InspectStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceInspectStageNonAdmissionRequest{Scope: scope})
	if err != nil || read.GetResult().GetCheckpoint() != nil {
		t.Fatalf("inspection inferred absence: %v %v", read, err)
	}
	nonleader := chain.dial(t, chain.followerCredentials)
	if response, err := nonleader.CheckpointStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: scope}); response != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("nonleader checkpointed: %v %v", response, err)
	}
	if response, err := nonleader.InspectStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceInspectStageNonAdmissionRequest{Scope: scope}); response != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("nonleader inspected: %v %v", response, err)
	}
	chain.dropNonAdmissionResponse.Store(true)
	if response, err := chain.client.CheckpointStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: scope}); response != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("lost reply claimed proof: %v %v", response, err)
	}
	read, err = chain.client.InspectStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceInspectStageNonAdmissionRequest{Scope: scope})
	if err != nil || read.GetResult().GetCheckpoint() == nil || chain.backend.cancelCalls.Load() != 0 || chain.backend.closed.Load() {
		t.Fatalf("read durable absence touched backend: %v %v", read, err)
	}
	saved := proto.Clone(read.Result.Checkpoint).(*velav1.ModelRuntimeExecutionNonAdmissionCheckpoint)
	checkpoint, err := chain.client.CheckpointStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: scope})
	if err != nil || !proto.Equal(saved, checkpoint.GetResult().GetCheckpoint()) {
		t.Fatalf("retry replaced immutable absence: %v %v", checkpoint, err)
	}
	chain.close()
	f.runtime.identity.ModelRuntimeEpoch++
	chain = startMemberFloorChain(t, f, floor.Command.Disposition, directory, false, false)
	scope.Identity = proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity)
	read, err = chain.client.InspectStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceInspectStageNonAdmissionRequest{Scope: scope})
	if err != nil || !proto.Equal(saved, read.GetResult().GetCheckpoint()) {
		t.Fatalf("absence recovery: %v %v", read, err)
	}
	if response, err := chain.client.CheckpointStageNonAdmission(t.Context(), &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: scope}); response != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old epoch could form new absence: %v %v", response, err)
	}
}

func TestMemberNonAdmissionRejectsMalformedProofAtBothHops(t *testing.T) {
	for _, historical := range []bool{false, true} {
		for _, hop := range []string{"runtime", "member"} {
			for _, fault := range []string{"nil", "schema", "digest", "owner", "unknown", "checkpoint-schema", "signature", "member", "sequence", "floor", "contract", "timestamp", "rejected", "decision", "future", "mutation", "late"} {
				t.Run(hop+"/"+fault+"/"+map[bool]string{false: "checkpoint", true: "read"}[historical], func(t *testing.T) {
					f, _, _ := newMemberFloorFixture(t)
					scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity), Authority: proto.Clone(f.authority).(*velav1.StageAuthority)}
					drain := memberDrainResult(t, scope)
					result := &velav1.ModelRuntimeExecutionNonAdmissionResult{SchemaVersion: 1, Identity: drain.Identity, AuthorityDigest: drain.AuthorityDigest, Decision: drain.Decision,
						Checkpoint: &velav1.ModelRuntimeExecutionNonAdmissionCheckpoint{SchemaVersion: 1, Authority: drain.Checkpoint.Authority, AuthorityDigest: drain.Checkpoint.AuthorityDigest,
							WorkerMemberId: scope.Identity.WorkerMemberId, ExecutionSequence: scope.Authority.ExecutionSequence, InstalledCutoff: scope.Authority.ExecutionSequence,
							Contract: modelruntimetransport.ExecutionNonAdmissionContract, ObservedAt: drain.Checkpoint.DrainedAt}}
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					switch fault {
					case "nil":
						result = nil
					case "schema":
						result.SchemaVersion++
					case "digest":
						result.AuthorityDigest[0] ^= 1
					case "owner":
						result.Identity.ModelRuntimeEpoch++
					case "unknown":
						result.ProtoReflect().SetUnknown([]byte{0x78, 1})
					case "checkpoint-schema":
						result.Checkpoint.SchemaVersion++
					case "signature":
						result.Checkpoint.Authority.Signature[0] ^= 1
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
					case "rejected":
						result.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
					case "decision":
						result.Decision = 99
					case "future":
						result.Checkpoint.Authority.IssuedAt.Seconds += int64(time.Hour / time.Second)
						result.Checkpoint.Authority.ExpiresAt.Seconds += int64(time.Hour / time.Second)
						var err error
						result.Checkpoint.Authority, err = f.signer.Sign(result.Checkpoint.Authority)
						if err != nil {
							t.Fatal(err)
						}
						digest, err := stageauthority.Digest(result.Checkpoint.Authority)
						if err != nil {
							t.Fatal(err)
						}
						result.Checkpoint.AuthorityDigest = digest[:]
					}
					reply := func(received *velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeExecutionNonAdmissionResult {
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
						f.server.runtime = &nonAdmissionRuntimeReply{reply: reply}
						if historical {
							_, err = f.server.InspectStageNonAdmission(ctx, &velav1.StageWorkerMemberServiceInspectStageNonAdmissionRequest{TargetWorkerMemberId: f.local.ID, Command: &velav1.ModelRuntimeServiceInspectStageNonAdmissionRequest{Scope: scope}})
						} else {
							_, err = f.server.CheckpointStageNonAdmission(ctx, &velav1.StageWorkerMemberServiceCheckpointStageNonAdmissionRequest{TargetWorkerMemberId: f.local.ID, Command: &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: scope}})
						}
					} else {
						client := floorTestClient(f, &nonAdmissionMemberReply{reply: reply})
						if historical {
							_, err = client.InspectStageNonAdmission(ctx, &velav1.ModelRuntimeServiceInspectStageNonAdmissionRequest{Scope: scope})
						} else {
							_, err = client.CheckpointStageNonAdmission(ctx, &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: scope})
						}
					}
					want := codes.DataLoss
					if fault == "late" {
						want = codes.Canceled
					}
					if status.Code(err) != want {
						t.Fatalf("invalid proof escaped: %v", err)
					}
				})
			}
		}
	}
}

type nonAdmissionRuntimeReply struct {
	velav1.ModelRuntimeServiceClient
	reply func(*velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeExecutionNonAdmissionResult
}

func (r *nonAdmissionRuntimeReply) CheckpointStageNonAdmission(_ context.Context, request *velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceCheckpointStageNonAdmissionResponse, error) {
	return &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionResponse{Result: r.reply(request.Scope)}, nil
}
func (r *nonAdmissionRuntimeReply) InspectStageNonAdmission(_ context.Context, request *velav1.ModelRuntimeServiceInspectStageNonAdmissionRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectStageNonAdmissionResponse, error) {
	return &velav1.ModelRuntimeServiceInspectStageNonAdmissionResponse{Result: r.reply(request.Scope)}, nil
}

type nonAdmissionMemberReply struct {
	velav1.StageWorkerMemberServiceClient
	reply func(*velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeExecutionNonAdmissionResult
}

func (r *nonAdmissionMemberReply) CheckpointStageNonAdmission(_ context.Context, request *velav1.StageWorkerMemberServiceCheckpointStageNonAdmissionRequest, _ ...grpc.CallOption) (*velav1.StageWorkerMemberServiceCheckpointStageNonAdmissionResponse, error) {
	return &velav1.StageWorkerMemberServiceCheckpointStageNonAdmissionResponse{Result: &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionResponse{Result: r.reply(request.Command.Scope)}}, nil
}
func (r *nonAdmissionMemberReply) InspectStageNonAdmission(_ context.Context, request *velav1.StageWorkerMemberServiceInspectStageNonAdmissionRequest, _ ...grpc.CallOption) (*velav1.StageWorkerMemberServiceInspectStageNonAdmissionResponse, error) {
	return &velav1.StageWorkerMemberServiceInspectStageNonAdmissionResponse{Result: &velav1.ModelRuntimeServiceInspectStageNonAdmissionResponse{Result: r.reply(request.Command.Scope)}}, nil
}
