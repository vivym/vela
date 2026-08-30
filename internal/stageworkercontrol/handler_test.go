package stageworkercontrol_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestHandlerRejectsStaleHeartbeatAndReattachBeforeExecution(t *testing.T) {
	now := time.Date(2026, 8, 30, 8, 0, 0, 0, time.UTC)
	keys := map[string][]byte{"control-key": bytes.Repeat([]byte{0x7c}, 32)}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	validator, err := stageauthority.NewValidator(keys, func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	active, err := signer.Sign(controlAuthority(now))
	if err != nil {
		t.Fatalf("Sign active: %v", err)
	}
	activeDigest, err := stageauthority.Digest(active)
	if err != nil {
		t.Fatalf("Digest active: %v", err)
	}
	authorizer := &exactActiveAuthorizer{digest: activeDigest}
	executor := &recordingControlExecutor{}
	handler, err := stageworkercontrol.NewHandler(stageworkercontrol.Config{
		Validator: validator, Authorizer: authorizer, Executor: executor,
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	identity := stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/member-1"}

	for _, operation := range []struct {
		name    string
		request func(*velav1.StageAuthority) *velav1.StageWorkerControlServiceConnectRequest
	}{
		{
			name: "HeartbeatStage",
			request: func(authority *velav1.StageAuthority) *velav1.StageWorkerControlServiceConnectRequest {
				return &velav1.StageWorkerControlServiceConnectRequest{
					RequestId: "81000000-0000-0000-0000-000000000001",
					Operation: &velav1.StageWorkerControlServiceConnectRequest_HeartbeatStage{
						HeartbeatStage: &velav1.HeartbeatStageRequest{Authority: authority, Sequence: 1},
					},
				}
			},
		},
		{
			name: "ReattachStage",
			request: func(authority *velav1.StageAuthority) *velav1.StageWorkerControlServiceConnectRequest {
				return &velav1.StageWorkerControlServiceConnectRequest{
					RequestId: "81000000-0000-0000-0000-000000000002",
					Operation: &velav1.StageWorkerControlServiceConnectRequest_ReattachStage{
						ReattachStage: &velav1.ReattachStageRequest{Authority: authority},
					},
				}
			},
		},
	} {
		t.Run(operation.name, func(t *testing.T) {
			before := executor.calls
			response, handleErr := handler.Handle(
				context.Background(), identity, 11, operation.request(active),
			)
			if handleErr != nil || response.GetStageCommandResult().GetDecision() !=
				velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("active Handle = %#v error=%v", response, handleErr)
			}
			if executor.calls != before+1 {
				t.Fatalf("active executor calls = %d, want %d", executor.calls, before+1)
			}

			stale := proto.Clone(active).(*velav1.StageAuthority)
			stale.Members[0].ModelRuntimeEpoch++
			stale.Signature = nil
			stale, err = signer.Sign(stale)
			if err != nil {
				t.Fatalf("Sign stale: %v", err)
			}
			response, handleErr = handler.Handle(
				context.Background(), identity, 12, operation.request(stale),
			)
			if handleErr != nil || response.GetStageCommandResult().GetDecision() !=
				velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE {
				t.Fatalf("stale Handle = %#v error=%v", response, handleErr)
			}
			if executor.calls != before+1 {
				t.Fatalf("stale authority reached executor: calls=%d", executor.calls)
			}

			tampered := proto.Clone(active).(*velav1.StageAuthority)
			tampered.StageFence++
			response, handleErr = handler.Handle(
				context.Background(), identity, 12, operation.request(tampered),
			)
			if handleErr != nil || response.GetStageCommandResult().GetDecision() !=
				velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE {
				t.Fatalf("tampered Handle = %#v error=%v", response, handleErr)
			}
			if executor.calls != before+1 {
				t.Fatalf("tampered authority reached executor: calls=%d", executor.calls)
			}
		})
	}
}

type exactActiveAuthorizer struct {
	digest [32]byte
}

func (authorizer *exactActiveAuthorizer) IsActive(
	_ context.Context,
	_ stageworkertransport.Identity,
	_ int64,
	_ stageworkercontrol.Operation,
	authority stageauthority.Verified,
) (bool, error) {
	return authority.Digest == authorizer.digest, nil
}

type recordingControlExecutor struct {
	calls int
}

func (executor *recordingControlExecutor) Execute(
	_ context.Context,
	_ stageworkertransport.Identity,
	_ int64,
	operation stageworkercontrol.Operation,
	request *velav1.StageWorkerControlServiceConnectRequest,
	_ *stageauthority.Verified,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	executor.calls++
	return &velav1.StageWorkerControlServiceConnectResponse{
		RequestId: request.GetRequestId(),
		Result: &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{
			StageCommandResult: &velav1.StageCommandResult{
				Decision:  velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
				Operation: operation,
			},
		},
	}, nil
}

func controlAuthority(now time.Time) *velav1.StageAuthority {
	return &velav1.StageAuthority{
		SchemaVersion: 1,
		JobId:         "13000000-0000-0000-0000-000000000001", AttemptId: "13000000-0000-0000-0000-000000000002",
		StageRunId: "13000000-0000-0000-0000-000000000003", StageAttemptId: "13000000-0000-0000-0000-000000000004",
		StageAllocationId: "13000000-0000-0000-0000-000000000005", StageLeaseId: "13000000-0000-0000-0000-000000000006",
		AttemptFence: 2, StageFence: 3, StageVersion: 4,
		WorkerInstanceId: "23000000-0000-0000-0000-000000000001", WorkerInstanceEpoch: 5,
		DeviceSetDigest: bytes.Repeat([]byte{0x81}, 32),
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "33000000-0000-0000-0000-000000000001", DeviceEpoch: 7,
		}},
		MembershipDigest: bytes.Repeat([]byte{0x82}, 32),
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: "43000000-0000-0000-0000-000000000001", MemberEpoch: 8, ModelRuntimeEpoch: 9,
		}},
		ModelResidencyId: "53000000-0000-0000-0000-000000000001", ModelRuntimeIdentity: "dit-runtime-3",
		StageProfileRevisionId:      "63000000-0000-0000-0000-000000000001",
		CapacityObservationSequence: 10,
		CapacityVector:              map[string]int64{"active_stage_slots": 1, "gpu_count": 1},
		LeaseToken:                  bytes.Repeat([]byte{0x83}, 32), ExecutionNonce: bytes.Repeat([]byte{0x84}, 32),
		ExecutionSpecDigest: bytes.Repeat([]byte{0x85}, 32),
		SigningKeyId:        "control-key", IssuedAt: timestamppb.New(now),
		ExpiresAt: timestamppb.New(now.Add(time.Minute)), MonotonicValidFor: durationpb.New(time.Minute),
	}
}
