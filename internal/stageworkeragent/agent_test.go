package stageworkeragent_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMultiMemberStartBarrierStartsOnlyAfterEveryMemberPrepares(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	agent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: fixture.clients[1]},
		},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	result, err := agent.PrepareAndStart(context.Background(), fixture.assignment)
	if err != nil {
		t.Fatalf("PrepareAndStart: %v", err)
	}
	if result.PreparedMembers != 2 || result.StartedMembers != 2 || !result.BarrierPassed ||
		result.StartedAt.IsZero() {
		t.Fatalf("barrier result = %#v", result)
	}
	for index, client := range fixture.clients {
		statusResponse, statusErr := client.Status(
			context.Background(),
			&velav1.ModelRuntimeServiceStatusRequest{Authority: fixture.authority},
		)
		if statusErr != nil || statusResponse.GetState() !=
			velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING {
			t.Fatalf("member %d Status = %#v error=%v", index, statusResponse, statusErr)
		}
	}
}

func TestMultiMemberStartBarrierCancelsEntireAllocationWhenOneMemberFails(t *testing.T) {
	fixture := newBarrierFixture(t, true)
	agent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: fixture.clients[1]},
		},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	result, err := agent.PrepareAndStart(context.Background(), fixture.assignment)
	if err == nil || result.BarrierPassed || result.CancellationAcknowledgedMembers != 2 {
		t.Fatalf("failed barrier result = %#v error=%v", result, err)
	}
	for index, client := range fixture.clients {
		statusResponse, statusErr := client.Status(
			context.Background(),
			&velav1.ModelRuntimeServiceStatusRequest{Authority: fixture.authority},
		)
		if statusErr != nil || statusResponse.GetState() !=
			velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING {
			t.Fatalf("member %d Status = %#v error=%v", index, statusResponse, statusErr)
		}
	}
}

func TestMultiMemberCancellationAckAndActuallyStoppedRemainDistinct(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	agent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: fixture.clients[1]},
		},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	if _, err := agent.PrepareAndStart(context.Background(), fixture.assignment); err != nil {
		t.Fatalf("PrepareAndStart: %v", err)
	}
	canceled, err := agent.Cancel(
		context.Background(),
		fixture.authority,
		velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	)
	if err != nil || canceled.AcknowledgedMembers != 2 || canceled.AllStopped {
		t.Fatalf("Cancel = %#v error=%v", canceled, err)
	}
	for _, backend := range fixture.backends {
		backend.FinishStop()
	}
	status, err := agent.Status(context.Background(), fixture.authority)
	if err != nil || status.ReportingMembers != 2 || !status.AllStopped {
		t.Fatalf("Status = %#v error=%v", status, err)
	}
}

func TestMultiMemberStatusCollectsStructuredFailureEvidenceByMember(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	digest, err := stageauthority.Digest(fixture.authority)
	if err != nil {
		t.Fatalf("Digest StageAuthority: %v", err)
	}
	failedAt := time.Date(2026, 8, 30, 9, 0, 0, 0, time.UTC)
	clients := make([]stageworkeragent.RuntimeMember, 0, len(fixture.memberIDs))
	for index, memberID := range fixture.memberIDs {
		clients = append(clients, stageworkeragent.RuntimeMember{
			ID: memberID,
			Client: fixedStatusRuntimeClient{response: &velav1.ModelRuntimeServiceStatusResponse{
				AuthorityDigest: digest[:],
				Decision:        velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
				State:           velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED,
				RuntimeIdentity: runtimeIdentityForMember(fixture.authority, memberID),
				FailureEvidence: &velav1.ModelRuntimeFailureEvidence{
					FailureClass:          "backend_failure",
					FailureFingerprint:    bytes.Repeat([]byte{byte(0x91 + index)}, 32),
					Detail:                "rank failed",
					WorkerReusable:        index == 0,
					ConsumedResourceUnits: int64(10 + index),
					FailedAt:              timestamppb.New(failedAt.Add(time.Duration(index) * time.Second)),
					RetryAt:               timestamppb.New(failedAt.Add(time.Minute)),
				},
			}},
		})
	}
	agent, err := stageworkeragent.New(stageworkeragent.Config{Members: clients})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}

	status, err := agent.Status(context.Background(), fixture.authority)
	if err != nil || status.ReportingMembers != 2 || len(status.Failures) != 2 {
		t.Fatalf("Status = %#v error=%v", status, err)
	}
	for index, memberID := range fixture.memberIDs {
		failure := status.Failures[memberID]
		if failure.GetFailureClass() != "backend_failure" ||
			failure.GetConsumedResourceUnits() != int64(10+index) ||
			failure.GetWorkerReusable() != (index == 0) {
			t.Fatalf("member %s failure = %#v", memberID, failure)
		}
	}
}

func TestMultiMemberStartBarrierRejectsMissingMemberBeforePreparing(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	agent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{{
			ID: fixture.memberIDs[0], Client: fixture.clients[0],
		}},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	result, err := agent.PrepareAndStart(context.Background(), fixture.assignment)
	if err == nil || result.PreparedMembers != 0 || result.StartedMembers != 0 {
		t.Fatalf("missing-member barrier result = %#v error=%v", result, err)
	}
}

func TestMultiMemberStartBarrierRejectsOneRuntimeCountedAsTwoMembers(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	agent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: fixture.clients[0]},
		},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	result, err := agent.PrepareAndStart(context.Background(), fixture.assignment)
	if err == nil || result.BarrierPassed || result.PreparedMembers == 2 {
		t.Fatalf("duplicated-runtime barrier result = %#v error=%v", result, err)
	}
}

func TestStageAssignmentRejectsTransferTicketWithoutExactInputVersion(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	fixture.assignment.InputTransferTickets = []*velav1.StageInputTransferTicket{{
		StageArtifactId: "72000000-0000-0000-0000-000000000001",
		ObjectVersion:   "object-version-1",
		TransferTicket:  []byte("ticket"),
	}}
	agent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: fixture.clients[1]},
		},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	result, err := agent.PrepareAndStart(context.Background(), fixture.assignment)
	if err == nil || result.PreparedMembers != 0 || result.StartedMembers != 0 {
		t.Fatalf("unbound TransferTicket result = %#v error=%v", result, err)
	}
}

func TestMemberBarrierRollbackIsBoundedWhenMemberRemainsUnreachable(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	fixture.assignment.MemberStartTimeout = durationpb.New(20 * time.Millisecond)
	agent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: unreachableRuntimeClient{
				ModelRuntimeServiceClient: fixture.clients[1],
			}},
		},
		CancellationTimeout: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	startedAt := time.Now()
	result, err := agent.PrepareAndStart(context.Background(), fixture.assignment)
	if err == nil || result.BarrierPassed {
		t.Fatalf("unreachable-member barrier result = %#v error=%v", result, err)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("barrier rollback took %s, want bounded cleanup", elapsed)
	}
}

type barrierFixture struct {
	authority  *velav1.StageAuthority
	assignment *velav1.StageAssignment
	memberIDs  []string
	clients    []velav1.ModelRuntimeServiceClient
	backends   []*modelruntime.FakeRuntime
}

func newBarrierFixture(t *testing.T, failSecondStart bool) barrierFixture {
	t.Helper()
	now := time.Now().UTC()
	memberIDs := []string{
		"42000000-0000-0000-0000-000000000001",
		"42000000-0000-0000-0000-000000000002",
	}
	keys := map[string][]byte{"barrier-key": bytes.Repeat([]byte{0x6b}, 32)}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	validator, err := stageauthority.NewValidator(keys, time.Now)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	executionSpec := &velav1.StageExecutionSpec{
		ParametersJson: []byte(`{"tensor_parallel":2}`),
	}
	authority, err := signer.Sign(barrierAuthority(now, memberIDs, executionSpec))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	backends := []*modelruntime.FakeRuntime{
		modelruntime.NewFakeDiTRuntime(), modelruntime.NewFakeDiTRuntime(),
	}
	backendServices := []modelruntime.Backend{backends[0], backends[1]}
	if failSecondStart {
		backendServices[1] = failingStartBackend{Backend: backends[1]}
	}
	clients := make([]velav1.ModelRuntimeServiceClient, 0, len(backendServices))
	for index, backend := range backendServices {
		binding := barrierBinding(memberIDs, memberIDs[index])
		service, serviceErr := modelruntime.NewService(modelruntime.Config{
			Binding: binding,
			EpochStore: modelruntime.EpochStoreFunc(func(stageauthority.RuntimeBinding) (int64, error) {
				return 9, nil
			}),
			Validator: validator, Backend: backend,
			CancelTimeout: time.Second,
		})
		if serviceErr != nil {
			t.Fatalf("NewService: %v", serviceErr)
		}
		t.Cleanup(service.Close)
		clients = append(clients, serveBarrierRuntime(t, service))
	}
	return barrierFixture{
		authority: authority,
		assignment: &velav1.StageAssignment{
			Authority:               authority,
			ExecutionSpec:           executionSpec,
			RequiredWorkerMemberIds: memberIDs,
			MemberStartTimeout:      durationpb.New(5 * time.Second),
		},
		memberIDs: memberIDs,
		clients:   clients,
		backends:  backends,
	}
}

func serveBarrierRuntime(
	t *testing.T,
	service *modelruntime.Service,
) velav1.ModelRuntimeServiceClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	velav1.RegisterModelRuntimeServiceServer(server, service)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial ModelRuntime: %v", err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		server.Stop()
		_ = listener.Close()
		if serveErr := <-done; serveErr != nil && serveErr != grpc.ErrServerStopped {
			t.Errorf("serve ModelRuntime: %v", serveErr)
		}
	})
	return velav1.NewModelRuntimeServiceClient(connection)
}

func barrierBinding(memberIDs []string, memberID string) stageauthority.RuntimeBinding {
	return stageauthority.RuntimeBinding{
		WorkerInstanceID: "22000000-0000-0000-0000-000000000001", WorkerInstanceEpoch: 5,
		WorkerMemberID: memberID, WorkerMemberEpoch: 8,
		DeviceSetDigest: bytes.Repeat([]byte{0x71}, 32),
		Devices: []stageauthority.DeviceEpoch{
			{ID: "32000000-0000-0000-0000-000000000001", Epoch: 7},
			{ID: "32000000-0000-0000-0000-000000000002", Epoch: 7},
		},
		MembershipDigest: bytes.Repeat([]byte{0x72}, 32),
		Members: []stageauthority.MemberEpoch{
			{ID: memberIDs[0], Epoch: 8}, {ID: memberIDs[1], Epoch: 8},
		},
		ModelResidencyID: "52000000-0000-0000-0000-000000000001", ModelRuntimeIdentity: "llm-runtime-1",
		StageProfileRevisionID: "62000000-0000-0000-0000-000000000001",
	}
}

func barrierAuthority(
	now time.Time,
	memberIDs []string,
	executionSpec *velav1.StageExecutionSpec,
) *velav1.StageAuthority {
	executionSpecDigest, err := stageauthority.ExecutionSpecDigest(executionSpec)
	if err != nil {
		panic(err)
	}
	return &velav1.StageAuthority{
		SchemaVersion: 1,
		JobId:         "12000000-0000-0000-0000-000000000001", AttemptId: "12000000-0000-0000-0000-000000000002",
		StageRunId: "12000000-0000-0000-0000-000000000003", StageAttemptId: "12000000-0000-0000-0000-000000000004",
		StageAllocationId: "12000000-0000-0000-0000-000000000005", StageLeaseId: "12000000-0000-0000-0000-000000000006",
		AttemptFence: 2, StageFence: 3, StageVersion: 4,
		WorkerInstanceId: "22000000-0000-0000-0000-000000000001", WorkerInstanceEpoch: 5,
		DeviceSetDigest: bytes.Repeat([]byte{0x71}, 32),
		Devices: []*velav1.StageAuthorityDeviceEpoch{
			{DeviceId: "32000000-0000-0000-0000-000000000001", DeviceEpoch: 7},
			{DeviceId: "32000000-0000-0000-0000-000000000002", DeviceEpoch: 7},
		},
		MembershipDigest: bytes.Repeat([]byte{0x72}, 32),
		Members: []*velav1.StageAuthorityMemberEpoch{
			{WorkerMemberId: memberIDs[0], MemberEpoch: 8, ModelRuntimeEpoch: 9},
			{WorkerMemberId: memberIDs[1], MemberEpoch: 8, ModelRuntimeEpoch: 9},
		},
		ModelResidencyId: "52000000-0000-0000-0000-000000000001", ModelRuntimeIdentity: "llm-runtime-1",
		StageProfileRevisionId:      "62000000-0000-0000-0000-000000000001",
		CapacityObservationSequence: 10,
		CapacityVector:              map[string]int64{"active_stage_slots": 1, "gpu_count": 2},
		LeaseToken:                  bytes.Repeat([]byte{0x73}, 32), ExecutionNonce: bytes.Repeat([]byte{0x74}, 32),
		ExecutionSpecDigest: executionSpecDigest[:],
		SigningKeyId:        "barrier-key", IssuedAt: timestamppb.New(now),
		ExpiresAt: timestamppb.New(now.Add(5 * time.Minute)), MonotonicValidFor: durationpb.New(5 * time.Minute),
	}
}

type failingStartBackend struct {
	modelruntime.Backend
}

type unreachableRuntimeClient struct {
	velav1.ModelRuntimeServiceClient
}

type fixedStatusRuntimeClient struct {
	velav1.ModelRuntimeServiceClient
	response *velav1.ModelRuntimeServiceStatusResponse
}

func (client fixedStatusRuntimeClient) Status(
	context.Context,
	*velav1.ModelRuntimeServiceStatusRequest,
	...grpc.CallOption,
) (*velav1.ModelRuntimeServiceStatusResponse, error) {
	return client.response, nil
}

func runtimeIdentityForMember(
	authority *velav1.StageAuthority,
	memberID string,
) *velav1.ModelRuntimeIdentity {
	identity := &velav1.ModelRuntimeIdentity{
		WorkerInstanceId:       authority.GetWorkerInstanceId(),
		WorkerInstanceEpoch:    authority.GetWorkerInstanceEpoch(),
		DeviceSetDigest:        append([]byte(nil), authority.GetDeviceSetDigest()...),
		MembershipDigest:       append([]byte(nil), authority.GetMembershipDigest()...),
		ModelResidencyId:       authority.GetModelResidencyId(),
		RuntimeIdentity:        authority.GetModelRuntimeIdentity(),
		StageProfileRevisionId: authority.GetStageProfileRevisionId(),
		WorkerMemberId:         memberID,
	}
	for _, member := range authority.GetMembers() {
		if member.GetWorkerMemberId() == memberID {
			identity.WorkerMemberEpoch = member.GetMemberEpoch()
			identity.ModelRuntimeEpoch = member.GetModelRuntimeEpoch()
			break
		}
	}
	return identity
}

func (unreachableRuntimeClient) PrepareStage(
	ctx context.Context,
	_ *velav1.ModelRuntimeServicePrepareStageRequest,
	_ ...grpc.CallOption,
) (*velav1.ModelRuntimeServicePrepareStageResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (unreachableRuntimeClient) CancelStage(
	ctx context.Context,
	_ *velav1.ModelRuntimeServiceCancelStageRequest,
	_ ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceCancelStageResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (backend failingStartBackend) Start(context.Context, stageauthority.Verified) error {
	return errors.New("required member failed backend start")
}
