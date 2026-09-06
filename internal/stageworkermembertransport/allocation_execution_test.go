package stageworkermembertransport

import (
	"context"
	"testing"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMemberAllocationExecutionRejectsMalformedResultsAtBothBoundaries(t *testing.T) {
	for _, boundary := range []string{"runtime", "member"} {
		for _, mutation := range []string{"missing", "schema", "query digest", "runtime epoch", "member", "unknown field", "unknown identity field", "missing authority", "missing inspection", "bad signature", "other allocation", "observed digest", "inspection identity", "unknown observation", "invalid timestamp", "unknown inspection field", "negative sequence", "rejected observation", "unrecognized decision", "partial absence", "rejected wrapper"} {
			t.Run(boundary+"/"+mutation, func(t *testing.T) {
				f, _, _ := newMemberFloorFixture(t)
				digest, err := stageauthority.Digest(f.authority)
				if err != nil {
					t.Fatal(err)
				}
				result := &velav1.ModelRuntimeServiceInspectAllocationExecutionResponse{
					SchemaVersion: 1, AuthorityDigest: append([]byte(nil), digest[:]...),
					RuntimeIdentity:   proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity),
					Decision:          velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
					ObservedAuthority: proto.Clone(f.authority).(*velav1.StageAuthority),
					Inspection: &velav1.ModelRuntimeServiceInspectExecutionResponse{
						SchemaVersion: 1, AuthorityDigest: append([]byte(nil), digest[:]...),
						RuntimeIdentity: proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity),
						Decision:        velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
						Known:           true, State: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED,
						ObservedAt: timestamppb.New(campaignNow()),
					},
				}
				switch mutation {
				case "missing":
					result = nil
				case "schema":
					result.SchemaVersion++
				case "query digest":
					result.AuthorityDigest[0] ^= 1
				case "runtime epoch":
					result.RuntimeIdentity.ModelRuntimeEpoch++
				case "member":
					result.RuntimeIdentity.WorkerMemberId = f.leader.ID
				case "unknown field":
					result.ProtoReflect().SetUnknown([]byte{0x78, 1})
				case "unknown identity field":
					result.RuntimeIdentity.ProtoReflect().SetUnknown([]byte{0x78, 1})
				case "missing authority":
					result.ObservedAuthority = nil
				case "missing inspection":
					result.Inspection = nil
				case "bad signature":
					result.ObservedAuthority.Signature[0] ^= 1
				case "other allocation":
					result.ObservedAuthority.ExecutionSequence++
					result.ObservedAuthority, err = f.signer.Sign(result.ObservedAuthority)
					if err != nil {
						t.Fatal(err)
					}
					changed, err := stageauthority.Digest(result.ObservedAuthority)
					if err != nil {
						t.Fatal(err)
					}
					result.Inspection.AuthorityDigest = changed[:]
				case "observed digest":
					result.Inspection.AuthorityDigest[0] ^= 1
				case "inspection identity":
					result.Inspection.RuntimeIdentity.ModelRuntimeEpoch++
				case "unknown observation":
					result.Inspection.Known = false
					result.Inspection.State = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED
				case "invalid timestamp":
					result.Inspection.ObservedAt.Nanos = -1
				case "unknown inspection field":
					result.Inspection.ProtoReflect().SetUnknown([]byte{0x78, 1})
				case "negative sequence":
					result.Inspection.Sequence = -1
				case "rejected observation":
					result.Inspection.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
				case "unrecognized decision":
					result.Decision = 99
				case "partial absence":
					result.ObservedAuthority = nil
					result.Inspection.Known = false
					result.Inspection.State = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED
				case "rejected wrapper":
					result.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
				}
				request := &velav1.ModelRuntimeServiceInspectAllocationExecutionRequest{SchemaVersion: 1, Authority: f.authority}
				if boundary == "runtime" {
					f.server.runtime = &allocationInspectionRuntimeResult{result: result}
					response, err := f.server.InspectAllocationExecution(t.Context(), &velav1.StageWorkerMemberServiceInspectAllocationExecutionRequest{
						TargetWorkerMemberId: f.local.ID, Command: request,
					})
					if response != nil || status.Code(err) != codes.DataLoss {
						t.Fatalf("malformed Runtime response escaped: %v %v", response, err)
					}
				} else {
					client := &Client{service: &allocationInspectionMemberResult{result: result}, targetID: f.local.ID, floorValidator: f.server.validator}
					for _, member := range f.authority.GetMembers() {
						if member.GetWorkerMemberId() == f.local.ID {
							copy(client.targetIdentityDigest[:], member.GetIdentityDigest())
						}
					}
					response, err := client.InspectAllocationExecution(t.Context(), request)
					if response != nil || status.Code(err) != codes.DataLoss {
						t.Fatalf("malformed member response escaped: %v %v", response, err)
					}
				}
			})
		}
	}
}

type allocationInspectionRuntimeResult struct {
	velav1.ModelRuntimeServiceClient
	result *velav1.ModelRuntimeServiceInspectAllocationExecutionResponse
}

func (runtime *allocationInspectionRuntimeResult) InspectAllocationExecution(context.Context, *velav1.ModelRuntimeServiceInspectAllocationExecutionRequest, ...grpc.CallOption) (*velav1.ModelRuntimeServiceInspectAllocationExecutionResponse, error) {
	return runtime.result, nil
}

type allocationInspectionMemberResult struct {
	velav1.StageWorkerMemberServiceClient
	result *velav1.ModelRuntimeServiceInspectAllocationExecutionResponse
}

func (member *allocationInspectionMemberResult) InspectAllocationExecution(context.Context, *velav1.StageWorkerMemberServiceInspectAllocationExecutionRequest, ...grpc.CallOption) (*velav1.StageWorkerMemberServiceInspectAllocationExecutionResponse, error) {
	return &velav1.StageWorkerMemberServiceInspectAllocationExecutionResponse{Result: member.result}, nil
}
