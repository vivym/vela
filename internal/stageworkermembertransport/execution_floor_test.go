package stageworkermembertransport

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMemberFloorServerAuthenticatesAndPreservesSignedHistory(t *testing.T) {
	f, request, runtime := newMemberFloorFixture(t)
	for range 2 {
		response, err := f.server.InstallStageExecutionFloor(context.Background(), request)
		if err != nil || !response.GetResult().GetDurable() || response.GetResult().GetInstalledCutoff() != 11 {
			t.Fatalf("install member floor: %v %v", response, err)
		}
		if !proto.Equal(runtime.request, request.GetCommand()) {
			t.Fatal("member forwarding changed the signed history or target")
		}
	}
	if runtime.calls != 2 || f.runtime.prepareCalls != 0 || f.runtime.startCalls != 0 || f.runtime.statusCalls != 0 {
		t.Fatal("floor installation crossed an execution or renewal operation")
	}
}

func TestMemberFloorServerRejectsUntrustedScopeBeforeRuntime(t *testing.T) {
	for _, mutation := range []string{"target", "schema", "unknown wrapper", "unknown command", "unknown identity", "missing command", "signature", "expired", "future", "unauthenticated", "nonleader", "missing member", "member epoch", "member identity", "worker epoch", "runtime epoch", "historical profile", "target profile"} {
		t.Run(mutation, func(t *testing.T) {
			f, request, runtime := newMemberFloorFixture(t)
			resign := false
			switch mutation {
			case "target":
				request.TargetWorkerMemberId = f.leader.ID
			case "schema":
				request.Command.SchemaVersion++
			case "unknown wrapper":
				request.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
			case "unknown command":
				request.Command.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
			case "unknown identity":
				request.Command.Identity.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
			case "missing command":
				request.Command = nil
			case "signature":
				request.Command.Disposition.Signature[0] ^= 1
			case "expired", "future":
				now := campaignNow().Add(2 * time.Minute)
				if mutation == "future" {
					now = campaignNow().Add(-time.Second)
				}
				f.server.validator = floorValidatorAt(t, now)
			case "unauthenticated":
				f.server.authenticator = stageworkertransport.PeerAuthenticator{}
			case "nonleader":
				f.auth.identity.SPIFFEID = f.localSPIFFE
			case "missing member":
				f.server.membersByID[uuid.NewString()] = MemberBinding{Epoch: 1}
			case "member epoch":
				for _, allocation := range request.Command.Disposition.Allocations {
					allocation.Members[0].MemberEpoch++
				}
				resign = true
			case "member identity":
				for _, allocation := range request.Command.Disposition.Allocations {
					allocation.Members[0].IdentityDigest = bytesOf('x')
				}
				resign = true
			case "worker epoch":
				request.Command.Disposition.WorkerInstanceEpoch++
				resign = true
			case "runtime epoch":
				request.Command.Disposition.Allocations[1].Members[1].ModelRuntimeEpoch++
				resign = true
			case "historical profile":
				request.Command.Disposition.Allocations[1].StageProfileRevisionId = uuid.NewString()
				resign = true
			case "target profile":
				request.Command.Identity.StageProfileRevisionId = uuid.NewString()
			}
			if resign {
				var err error
				request.Command.Disposition, err = f.signer.SignTerminalDisposition(request.Command.Disposition)
				if err != nil {
					t.Fatal(err)
				}
			}
			response, err := f.server.InstallStageExecutionFloor(context.Background(), request)
			if response != nil || err == nil || runtime.calls != 0 {
				t.Fatalf("invalid scope reached Runtime: %v %v calls=%d", response, err, runtime.calls)
			}
			if mutation == "unauthenticated" && status.Code(err) != codes.Unauthenticated || mutation == "nonleader" && status.Code(err) != codes.PermissionDenied {
				t.Fatalf("unexpected authentication result: %v", err)
			}
		})
	}
}

func TestMemberFloorRejectsUnboundAcknowledgementsAtBothHops(t *testing.T) {
	for _, hop := range []string{"runtime", "member"} {
		for _, mutation := range []string{"nil", "schema", "unknown", "identity", "digest", "cutoff", "durable", "decision"} {
			t.Run(hop+"/"+mutation, func(t *testing.T) {
				f, request, runtime := newMemberFloorFixture(t)
				runtime.mutate = func(response *velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse) *velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse {
					switch mutation {
					case "nil":
						return nil
					case "schema":
						response.SchemaVersion++
					case "unknown":
						response.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
					case "identity":
						response.Identity.ModelRuntimeEpoch++
					case "digest":
						response.DispositionDigest[0] ^= 1
					case "cutoff":
						response.InstalledCutoff--
					case "durable":
						response.Durable = false
					case "decision":
						response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
					}
					return response
				}
				var err error
				if hop == "runtime" {
					_, err = f.server.InstallStageExecutionFloor(context.Background(), request)
				} else {
					client := floorTestClient(f, &memberFloorReplyService{runtime: runtime})
					_, err = client.InstallStageExecutionFloor(context.Background(), request.Command)
				}
				if status.Code(err) != codes.DataLoss || runtime.calls != 1 {
					t.Fatalf("unbound acknowledgement accepted: %v calls=%d", err, runtime.calls)
				}
			})
		}
	}
}

func TestMemberFloorClientPinsTargetAndValidatesBeforeSending(t *testing.T) {
	for _, mutation := range []string{"target", "identity", "signature", "expired", "validator", "canceled"} {
		t.Run(mutation, func(t *testing.T) {
			f, request, runtime := newMemberFloorFixture(t)
			client := floorTestClient(f, &memberFloorReplyService{runtime: runtime})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mutation {
			case "target":
				client.targetID = f.leader.ID
			case "identity":
				client.targetIdentityDigest[0] ^= 1
			case "signature":
				request.Command.Disposition.Signature[0] ^= 1
			case "expired":
				client.floorValidator = floorValidatorAt(t, campaignNow().Add(time.Hour))
			case "validator":
				client.floorValidator = nil
			case "canceled":
				cancel()
			}
			response, err := client.InstallStageExecutionFloor(ctx, request.Command)
			if response != nil || err == nil || runtime.calls != 0 {
				t.Fatalf("invalid floor reached member: %v %v calls=%d", response, err, runtime.calls)
			}
		})
	}
}

func TestMemberFloorPropagatesCancellationAndRejectsLateSuccess(t *testing.T) {
	for _, hop := range []string{"runtime", "member"} {
		f, request, runtime := newMemberFloorFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		runtime.mutate = func(response *velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse) *velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse {
			cancel()
			return response
		}
		var err error
		if hop == "runtime" {
			_, err = f.server.InstallStageExecutionFloor(ctx, request)
		} else {
			_, err = floorTestClient(f, &memberFloorReplyService{runtime: runtime}).InstallStageExecutionFloor(ctx, request.Command)
		}
		if status.Code(err) != codes.Canceled {
			t.Fatalf("late success survived cancellation at %s: %v", hop, err)
		}
		cancel()
	}
	f, request, runtime := newMemberFloorFixture(t)
	runtime.failure = status.Error(codes.FailedPrecondition, "journal unavailable")
	if response, err := f.server.InstallStageExecutionFloor(context.Background(), request); response != nil || !errors.Is(err, runtime.failure) {
		t.Fatalf("journal failure was hidden: %v %v", response, err)
	}
}

func newMemberFloorFixture(t *testing.T) (*serverFixture, *velav1.StageWorkerMemberServiceInstallStageExecutionFloorRequest, *memberFloorRuntimeClient) {
	t.Helper()
	f := newServerFixture(t, time.Time{})
	a := proto.Clone(f.authority).(*velav1.StageAuthority)
	a.SchemaVersion, a.ExecutionSequence = stageauthority.SchemaVersionV2, 10
	var err error
	f.authority, err = f.signer.Sign(a)
	if err != nil {
		t.Fatal(err)
	}
	a = f.authority
	digest, err := stageauthority.Digest(a)
	if err != nil {
		t.Fatal(err)
	}
	d := &velav1.StageTerminalDisposition{
		SchemaVersion: 1, InputDisposition: velav1.StageInputDisposition_STAGE_INPUT_DISPOSITION_INPUTS_UNUSED,
		OriginalAuthorityDigest: digest[:], OrganizationId: uuid.NewString(), ProjectId: uuid.NewString(),
		JobId: a.GetJobId(), AttemptId: a.GetAttemptId(), StageRunId: a.GetStageRunId(),
		StageAttemptId: a.GetStageAttemptId(), StageAllocationId: a.GetStageAllocationId(), StageLeaseId: a.GetStageLeaseId(),
		TerminalState: velav1.StageTerminalState_STAGE_TERMINAL_STATE_FAILED, StageFence: 3, StageVersion: 4,
		WorkerInstanceId: a.GetWorkerInstanceId(), WorkerInstanceEpoch: a.GetWorkerInstanceEpoch(),
		WorkerMemberId: f.leader.ID, ControlSessionEpoch: 7, DeviceSetDigest: bytes.Clone(a.GetDeviceSetDigest()), MembershipDigest: bytes.Clone(a.GetMembershipDigest()),
		Devices: a.GetDevices(), Cutoff: 11, ObservedAt: timestamppb.New(campaignNow()), ExpiresAt: timestamppb.New(campaignNow().Add(time.Minute)), SigningKeyId: a.GetSigningKeyId(),
	}
	for sequence := int64(10); sequence <= 11; sequence++ {
		allocation := &velav1.StageTerminalAllocation{
			StageAttemptId: a.GetStageAttemptId(), StageAllocationId: a.GetStageAllocationId(), StageLeaseId: a.GetStageLeaseId(),
			ExecutionSequence: sequence, ExecutionNonce: bytes.Clone(a.GetExecutionNonce()), ModelResidencyId: a.GetModelResidencyId(),
			ModelRuntimeIdentity: a.GetModelRuntimeIdentity(), BarrierGeneration: a.GetModelRuntimeBarrierGeneration(), StageProfileRevisionId: a.GetStageProfileRevisionId(),
		}
		if sequence > 10 {
			allocation.StageAttemptId, allocation.StageAllocationId, allocation.StageLeaseId = uuid.NewString(), uuid.NewString(), uuid.NewString()
		}
		for _, member := range a.GetMembers() {
			allocation.Members = append(allocation.Members, &velav1.StageTerminalMember{
				WorkerMemberId: member.GetWorkerMemberId(), MemberEpoch: member.GetMemberEpoch(), ModelRuntimeEpoch: member.GetModelRuntimeEpoch(),
				IdentityDigest: bytes.Clone(member.GetIdentityDigest()), DeviceSubsetDigest: bytesOf('s'),
			})
		}
		d.Allocations = append(d.Allocations, allocation)
	}
	d, err = f.signer.SignTerminalDisposition(d)
	if err != nil {
		t.Fatal(err)
	}
	request := &velav1.StageWorkerMemberServiceInstallStageExecutionFloorRequest{
		TargetWorkerMemberId: f.local.ID, Command: &velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest{
			SchemaVersion: 1, Identity: proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity), Disposition: d,
		},
	}
	runtime := &memberFloorRuntimeClient{validator: f.server.validator}
	f.server.runtime = runtime
	return f, request, runtime
}

func floorValidatorAt(t *testing.T, now time.Time) *stageauthority.Validator {
	t.Helper()
	validator, err := stageauthority.NewValidator(map[string][]byte{"authority-v1": []byte("0123456789abcdef0123456789abcdef")}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	return validator
}

func floorTestClient(f *serverFixture, service velav1.StageWorkerMemberServiceClient) *Client {
	return &Client{service: service, targetID: f.local.ID, targetIdentityDigest: [32]byte(f.authority.GetMembers()[1].GetIdentityDigest()), floorValidator: f.server.validator}
}

type memberFloorRuntimeClient struct {
	velav1.ModelRuntimeServiceClient
	validator *stageauthority.Validator
	request   *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest
	calls     int
	failure   error
	mutate    func(*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse) *velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse
}

func (runtime *memberFloorRuntimeClient) InstallStageExecutionFloor(_ context.Context, request *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest, _ ...grpc.CallOption) (*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse, error) {
	runtime.calls++
	if runtime.failure != nil {
		return nil, runtime.failure
	}
	runtime.request = proto.Clone(request).(*velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest)
	verified, err := runtime.validator.ValidateTerminalDispositionSignature(request.GetDisposition())
	if err != nil {
		return nil, err
	}
	response := &velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse{
		SchemaVersion: 1, Identity: proto.Clone(request.GetIdentity()).(*velav1.ModelRuntimeIdentity),
		Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED, DispositionDigest: bytes.Clone(verified.Digest[:]),
		InstalledCutoff: request.GetDisposition().GetCutoff(), Durable: true,
	}
	if runtime.mutate != nil {
		response = runtime.mutate(response)
	}
	return response, nil
}

type memberFloorReplyService struct {
	velav1.StageWorkerMemberServiceClient
	runtime *memberFloorRuntimeClient
}

func (service *memberFloorReplyService) InstallStageExecutionFloor(ctx context.Context, request *velav1.StageWorkerMemberServiceInstallStageExecutionFloorRequest, options ...grpc.CallOption) (*velav1.StageWorkerMemberServiceInstallStageExecutionFloorResponse, error) {
	response, err := service.runtime.InstallStageExecutionFloor(ctx, request.GetCommand(), options...)
	return &velav1.StageWorkerMemberServiceInstallStageExecutionFloorResponse{Result: response}, err
}
