package stageworkermembertransport

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMemberDrainTLSUnixLostReplyAndHistoricalRecovery(t *testing.T) {
	f, floorRequest, _ := newMemberFloorFixture(t)
	var wall atomic.Int64
	wall.Store(campaignNow().UnixNano())
	validator, err := stageauthority.NewValidator(map[string][]byte{"authority-v1": []byte("0123456789abcdef0123456789abcdef")}, func() time.Time { return time.Unix(0, wall.Load()) })
	if err != nil {
		t.Fatal(err)
	}
	f.server.validator = validator
	directory := privateMemberFloorDirectory(t)
	chain := startMemberFloorChain(t, f, floorRequest.Command.Disposition, directory, true, false)
	scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity), Authority: f.authority}
	prepared, err := chain.client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority, ExecutionSpec: f.spec})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("prepare: %v %v", prepared, err)
	}
	if _, err := chain.client.InstallStageExecutionFloor(t.Context(), floorRequest.Command); err != nil {
		t.Fatal(err)
	}
	wall.Add(int64(10 * time.Minute))
	canceled, err := chain.client.CancelStage(t.Context(), &velav1.ModelRuntimeServiceCancelStageRequest{Authority: f.authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP})
	if err != nil || !canceled.GetCancellationAcknowledged() {
		t.Fatalf("cancel: %v %v", canceled, err)
	}
	chain.backend.FinishStop()
	read, err := chain.client.InspectStageExecutionDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: scope})
	if err != nil || read.GetResult().GetCheckpoint() != nil {
		t.Fatalf("read inferred drain from STOPPED: %v %v", read, err)
	}
	nonleader := chain.dial(t, chain.followerCredentials)
	if response, err := nonleader.DrainStageExecution(t.Context(), &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope}); response != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("nonleader drained remote member: %v %v", response, err)
	}
	if response, err := nonleader.InspectStageExecutionDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: scope}); response != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("nonleader read remote checkpoint: %v %v", response, err)
	}
	chain.dropDrainResponse.Store(true)
	if response, err := chain.client.DrainStageExecution(t.Context(), &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope}); response != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("lost acknowledgement claimed proof: %v %v", response, err)
	}
	read, err = chain.client.InspectStageExecutionDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: scope})
	if err != nil || read.GetResult().GetCheckpoint() == nil {
		t.Fatalf("lost reply was not durable: %v %v", read, err)
	}
	saved := proto.Clone(read.GetResult().GetCheckpoint()).(*velav1.ModelRuntimeExecutionDrainCheckpoint)
	response, err := chain.client.DrainStageExecution(t.Context(), &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope})
	if err != nil || !proto.Equal(saved, response.GetResult().GetCheckpoint()) || chain.backend.closed.Load() {
		t.Fatalf("drain retry: %v %v", response, err)
	}
	chain.close()
	f.runtime.identity.ModelRuntimeEpoch++
	chain = startMemberFloorChain(t, f, floorRequest.Command.Disposition, directory, false, false)
	scope.Identity = proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity)
	read, err = chain.client.InspectStageExecutionDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: scope})
	if err != nil || !proto.Equal(saved, read.GetResult().GetCheckpoint()) {
		t.Fatalf("historical checkpoint recovery: %v %v", read, err)
	}
	if response, err := chain.client.DrainStageExecution(t.Context(), &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope}); response != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old epoch entered new backend: %v %v", response, err)
	}
	unseen := proto.Clone(scope).(*velav1.ModelRuntimeExecutionDrainScope)
	unseen.Authority.StageVersion++
	unseen.Authority, err = f.signer.Sign(unseen.Authority)
	if err != nil {
		t.Fatal(err)
	}
	read, err = chain.client.InspectStageExecutionDrain(t.Context(), &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: unseen})
	if err != nil || read.GetResult().GetCheckpoint() != nil {
		t.Fatalf("unseen renewal inherited proof: %v %v", read, err)
	}
}

func TestMemberDrainRejectsMalformedEvidenceAtBothHops(t *testing.T) {
	for _, historical := range []bool{false, true} {
		for _, boundary := range []string{"runtime", "member"} {
			for _, fault := range []string{"missing", "schema", "digest", "owner", "unknown", "checkpoint-schema", "checkpoint-digest", "checkpoint-authority", "checkpoint-sequence", "checkpoint-member", "contract", "timestamp", "unknown-time", "rejected-proof", "decision", "late", "request-mutation"} {
				t.Run(boundary+"/"+fault+"/historical="+map[bool]string{false: "false", true: "true"}[historical], func(t *testing.T) {
					f, _, _ := newMemberFloorFixture(t)
					scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity), Authority: proto.Clone(f.authority).(*velav1.StageAuthority)}
					result := memberDrainResult(t, scope)
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					switch fault {
					case "missing":
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
					case "checkpoint-digest":
						result.Checkpoint.AuthorityDigest[0] ^= 1
					case "checkpoint-authority":
						result.Checkpoint.Authority.StageVersion++
					case "checkpoint-sequence":
						result.Checkpoint.ExecutionSequence++
					case "checkpoint-member":
						result.Checkpoint.WorkerMemberId = f.leader.ID
					case "contract":
						result.Checkpoint.Contract = "STOPPED"
					case "timestamp":
						result.Checkpoint.DrainedAt = nil
					case "unknown-time":
						result.Checkpoint.DrainedAt.ProtoReflect().SetUnknown([]byte{0x78, 1})
					case "rejected-proof":
						result.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
					case "decision":
						result.Decision = 99
					}
					reply := func(received *velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeExecutionDrainResult {
						if fault == "late" {
							cancel()
						}
						if fault == "request-mutation" {
							received.Identity.ModelRuntimeEpoch++
							result.Identity = proto.Clone(received.Identity).(*velav1.ModelRuntimeIdentity)
						}
						return result
					}
					var err error
					if boundary == "runtime" {
						f.server.runtime = &drainRuntimeReply{reply: reply}
						if historical {
							_, err = f.server.InspectStageExecutionDrain(ctx, &velav1.StageWorkerMemberServiceInspectStageExecutionDrainRequest{TargetWorkerMemberId: f.local.ID, Command: &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: scope}})
						} else {
							_, err = f.server.DrainStageExecution(ctx, &velav1.StageWorkerMemberServiceDrainStageExecutionRequest{TargetWorkerMemberId: f.local.ID, Command: &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope}})
						}
					} else {
						client := floorTestClient(f, &drainMemberReply{reply: reply})
						if historical {
							_, err = client.InspectStageExecutionDrain(ctx, &velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest{Scope: scope})
						} else {
							_, err = client.DrainStageExecution(ctx, &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope})
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

func memberDrainResult(t *testing.T, scope *velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeExecutionDrainResult {
	t.Helper()
	digest, err := stageauthority.Digest(scope.Authority)
	if err != nil {
		t.Fatal(err)
	}
	return &velav1.ModelRuntimeExecutionDrainResult{SchemaVersion: 1, Identity: proto.Clone(scope.Identity).(*velav1.ModelRuntimeIdentity), AuthorityDigest: append([]byte(nil), digest[:]...), Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
		Checkpoint: &velav1.ModelRuntimeExecutionDrainCheckpoint{SchemaVersion: 1, Authority: proto.Clone(scope.Authority).(*velav1.StageAuthority), AuthorityDigest: append([]byte(nil), digest[:]...), WorkerMemberId: scope.Identity.WorkerMemberId, ExecutionSequence: scope.Authority.ExecutionSequence, Contract: modelruntimetransport.ExecutionDrainContract, DrainedAt: timestamppb.New(campaignNow())}}
}

type drainRuntimeReply struct {
	velav1.ModelRuntimeServiceClient
	reply func(*velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeExecutionDrainResult
}

func (r *drainRuntimeReply) DrainStageExecution(_ context.Context, request *velav1.ModelRuntimeServiceDrainStageExecutionRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceDrainStageExecutionResponse, error) {
	return &velav1.ModelRuntimeServiceDrainStageExecutionResponse{Result: r.reply(request.Scope)}, nil
}
func (r *drainRuntimeReply) InspectStageExecutionDrain(_ context.Context, request *velav1.ModelRuntimeServiceInspectStageExecutionDrainRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectStageExecutionDrainResponse, error) {
	return &velav1.ModelRuntimeServiceInspectStageExecutionDrainResponse{Result: r.reply(request.Scope)}, nil
}

type drainMemberReply struct {
	velav1.StageWorkerMemberServiceClient
	reply func(*velav1.ModelRuntimeExecutionDrainScope) *velav1.ModelRuntimeExecutionDrainResult
}

func (r *drainMemberReply) DrainStageExecution(_ context.Context, request *velav1.StageWorkerMemberServiceDrainStageExecutionRequest, _ ...grpc.CallOption) (*velav1.StageWorkerMemberServiceDrainStageExecutionResponse, error) {
	return &velav1.StageWorkerMemberServiceDrainStageExecutionResponse{Result: &velav1.ModelRuntimeServiceDrainStageExecutionResponse{Result: r.reply(request.Command.Scope)}}, nil
}
func (r *drainMemberReply) InspectStageExecutionDrain(_ context.Context, request *velav1.StageWorkerMemberServiceInspectStageExecutionDrainRequest, _ ...grpc.CallOption) (*velav1.StageWorkerMemberServiceInspectStageExecutionDrainResponse, error) {
	return &velav1.StageWorkerMemberServiceInspectStageExecutionDrainResponse{Result: &velav1.ModelRuntimeServiceInspectStageExecutionDrainResponse{Result: r.reply(request.Command.Scope)}}, nil
}
