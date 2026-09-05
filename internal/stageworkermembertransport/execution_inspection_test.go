package stageworkermembertransport

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMemberInspectionTLSAndUnixAfterFloorAndExpiry(t *testing.T) {
	f, floorRequest, _ := newMemberFloorFixture(t)
	var wall atomic.Int64
	wall.Store(campaignNow().UnixNano())
	validator, err := stageauthority.NewValidator(map[string][]byte{"authority-v1": []byte("0123456789abcdef0123456789abcdef")}, func() time.Time {
		return time.Unix(0, wall.Load())
	})
	if err != nil {
		t.Fatal(err)
	}
	f.server.validator = validator
	directory := privateMemberFloorDirectory(t)
	chain := startMemberFloorChain(t, f, floorRequest.Command.Disposition, directory, true, false)
	request := &velav1.ModelRuntimeServiceInspectExecutionRequest{SchemaVersion: 1, Authority: f.authority}
	assert := func(known bool, state velav1.ModelRuntimeExecutionState) {
		t.Helper()
		response, err := chain.client.InspectExecution(context.Background(), request)
		if err != nil || response.GetKnown() != known || response.GetState() != state ||
			response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
			t.Fatalf("TLS/UDS inspection: %v %v, want known=%v state=%s", response, err, known, state)
		}
	}
	assert(false, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED)
	prepared, err := chain.client.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority, ExecutionSpec: f.spec})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("prepare inspection fixture: %v %v", prepared, err)
	}
	if _, err := chain.client.InstallStageExecutionFloor(context.Background(), floorRequest.Command); err != nil {
		t.Fatal(err)
	}
	wall.Add(int64(10 * time.Minute))
	assert(true, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED)
	if _, err := chain.client.Status(context.Background(), &velav1.ModelRuntimeServiceStatusRequest{Authority: f.authority}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ordinary Status accepted elapsed authority: %v", err)
	}
	unseen := proto.Clone(f.authority).(*velav1.StageAuthority)
	unseen.StageVersion++
	unseen, err = f.signer.Sign(unseen)
	if err != nil {
		t.Fatal(err)
	}
	response, err := chain.client.InspectExecution(context.Background(), &velav1.ModelRuntimeServiceInspectExecutionRequest{SchemaVersion: 1, Authority: unseen})
	if err != nil || response.GetKnown() || response.GetState() != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED {
		t.Fatalf("inspection installed unseen renewal: %v %v", response, err)
	}
	canceled, err := chain.client.CancelStage(context.Background(), &velav1.ModelRuntimeServiceCancelStageRequest{
		Authority: f.authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	})
	if err != nil || !canceled.GetCancellationAcknowledged() {
		t.Fatalf("expired cancellation: %v %v", canceled, err)
	}
	assert(true, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING)
	chain.backend.FinishStop()
	assert(true, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED)
	if chain.backend.closed.Load() || chain.backend.cancelCalls.Load() != 1 {
		t.Fatal("inspection stopped or unloaded the resident backend")
	}
	nonleader := chain.dial(t, chain.followerCredentials)
	if _, err := nonleader.InspectExecution(context.Background(), request); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("nonleader read remote execution: %v", err)
	}
	chain.close()
	chain = startMemberFloorChain(t, f, floorRequest.Command.Disposition, directory, false, false)
	assert(false, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED)
}

func TestMemberInspectionRejectsMalformedResultsAtBothBoundaries(t *testing.T) {
	for _, boundary := range []string{"runtime", "member"} {
		for _, mutation := range []string{"missing", "schema", "authority digest", "runtime epoch", "member", "member epoch", "worker epoch", "profile", "unknown field", "unknown identity field", "unknown stopped", "unknown sequence", "negative sequence", "missing timestamp", "invalid timestamp", "rejected stopped", "unrecognized decision", "unrecognized state"} {
			t.Run(boundary+"/"+mutation, func(t *testing.T) {
				f, _, _ := newMemberFloorFixture(t)
				digest, err := stageauthority.Digest(f.authority)
				if err != nil {
					t.Fatal(err)
				}
				result := &velav1.ModelRuntimeServiceInspectExecutionResponse{
					SchemaVersion: 1, AuthorityDigest: digest[:], RuntimeIdentity: proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity),
					Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
					Known:    true, State: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED,
					ObservedAt: timestamppb.New(campaignNow()),
				}
				switch mutation {
				case "missing":
					result = nil
				case "schema":
					result.SchemaVersion++
				case "authority digest":
					result.AuthorityDigest[0] ^= 1
				case "runtime epoch":
					result.RuntimeIdentity.ModelRuntimeEpoch++
				case "member":
					result.RuntimeIdentity.WorkerMemberId = f.leader.ID
				case "member epoch":
					result.RuntimeIdentity.WorkerMemberEpoch++
				case "worker epoch":
					result.RuntimeIdentity.WorkerInstanceEpoch++
				case "profile":
					result.RuntimeIdentity.StageProfileRevisionId = f.leader.ID
				case "unknown field":
					result.ProtoReflect().SetUnknown([]byte{0x78, 1})
				case "unknown identity field":
					result.RuntimeIdentity.ProtoReflect().SetUnknown([]byte{0x78, 1})
				case "unknown stopped":
					result.Known = false
				case "unknown sequence":
					result.Known, result.State, result.Sequence = false, 0, 1
				case "negative sequence":
					result.Sequence = -1
				case "missing timestamp":
					result.ObservedAt = nil
				case "invalid timestamp":
					result.ObservedAt.Nanos = -1
				case "rejected stopped":
					result.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
				case "unrecognized decision":
					result.Decision = 99
				case "unrecognized state":
					result.State = 99
				}
				request := &velav1.ModelRuntimeServiceInspectExecutionRequest{SchemaVersion: 1, Authority: f.authority}
				if boundary == "runtime" {
					f.server.runtime = &inspectionRuntimeResult{result: result}
					response, err := f.server.InspectExecution(context.Background(), &velav1.StageWorkerMemberServiceInspectExecutionRequest{
						TargetWorkerMemberId: f.local.ID, Command: request,
					})
					if response != nil || status.Code(err) != codes.DataLoss {
						t.Fatalf("malformed Runtime response escaped: %v %v", response, err)
					}
				} else {
					client := &Client{service: &inspectionMemberResult{result: result}, targetID: f.local.ID}
					for _, member := range f.authority.GetMembers() {
						if member.GetWorkerMemberId() == f.local.ID {
							copy(client.targetIdentityDigest[:], member.GetIdentityDigest())
						}
					}
					response, err := client.InspectExecution(context.Background(), request)
					if response != nil || status.Code(err) != codes.DataLoss {
						t.Fatalf("malformed member response escaped: %v %v", response, err)
					}
				}
			})
		}
	}
}

type inspectionRuntimeResult struct {
	velav1.ModelRuntimeServiceClient
	result *velav1.ModelRuntimeServiceInspectExecutionResponse
}

func (runtime *inspectionRuntimeResult) InspectExecution(context.Context, *velav1.ModelRuntimeServiceInspectExecutionRequest, ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectExecutionResponse, error) {
	return runtime.result, nil
}

type inspectionMemberResult struct {
	velav1.StageWorkerMemberServiceClient
	result *velav1.ModelRuntimeServiceInspectExecutionResponse
}

func (member *inspectionMemberResult) InspectExecution(context.Context, *velav1.StageWorkerMemberServiceInspectExecutionRequest, ...grpc.CallOption) (*velav1.StageWorkerMemberServiceInspectExecutionResponse, error) {
	return &velav1.StageWorkerMemberServiceInspectExecutionResponse{Result: member.result}, nil
}
