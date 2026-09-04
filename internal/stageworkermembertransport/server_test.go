package stageworkermembertransport

import (
	"context"
	"crypto/sha256"
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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestServerAuthenticatesLeaderAndPreservesRuntimeReplay(t *testing.T) {
	fixture := newServerFixture(t, time.Time{})
	request := &velav1.StageWorkerMemberServicePrepareStageRequest{
		TargetWorkerMemberId: fixture.local.ID,
		Command: &velav1.ModelRuntimeServicePrepareStageRequest{
			Authority: fixture.authority, ExecutionSpec: fixture.spec,
		},
	}
	first, err := fixture.server.PrepareStage(context.Background(), request)
	if err != nil || first.GetResult().GetDecision() !=
		velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("first PrepareStage = %#v error=%v", first, err)
	}
	second, err := fixture.server.PrepareStage(context.Background(), request)
	if err != nil || second.GetResult().GetDecision() !=
		velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED ||
		fixture.runtime.prepareCalls != 2 {
		t.Fatalf("replayed PrepareStage = %#v calls=%d error=%v", second, fixture.runtime.prepareCalls, err)
	}
}

func TestServerRejectsWrongTargetLeaderAuthorityAndEpochBeforeRuntime(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*serverFixture, *velav1.StageWorkerMemberServiceStartStageRequest)
		wantCode codes.Code
	}{
		{name: "wrong target", wantCode: codes.InvalidArgument, mutate: func(
			_ *serverFixture, request *velav1.StageWorkerMemberServiceStartStageRequest,
		) {
			request.TargetWorkerMemberId = uuid.NewString()
		}},
		{name: "non leader peer", wantCode: codes.PermissionDenied, mutate: func(
			fixture *serverFixture, _ *velav1.StageWorkerMemberServiceStartStageRequest,
		) {
			fixture.auth.identity = stageworkertransport.Identity{SPIFFEID: fixture.localSPIFFE}
		}},
		{name: "tampered signature", wantCode: codes.FailedPrecondition, mutate: func(
			_ *serverFixture, request *velav1.StageWorkerMemberServiceStartStageRequest,
		) {
			request.Command.Authority.StageFence++
		}},
		{name: "stale local epoch", wantCode: codes.FailedPrecondition, mutate: func(
			fixture *serverFixture, request *velav1.StageWorkerMemberServiceStartStageRequest,
		) {
			unsigned := proto.Clone(request.Command.Authority).(*velav1.StageAuthority)
			unsigned.Signature = nil
			unsigned.Members[1].MemberEpoch++
			signed, err := fixture.signer.Sign(unsigned)
			if err != nil {
				fixture.t.Fatalf("resign stale authority: %v", err)
			}
			request.Command.Authority = signed
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newServerFixture(t, time.Time{})
			request := &velav1.StageWorkerMemberServiceStartStageRequest{
				TargetWorkerMemberId: fixture.local.ID,
				Command:              &velav1.ModelRuntimeServiceStartStageRequest{Authority: fixture.authority},
			}
			test.mutate(fixture, request)
			_, err := fixture.server.StartStage(context.Background(), request)
			if status.Code(err) != test.wantCode || fixture.runtime.startCalls != 0 {
				t.Fatalf("StartStage error=%v code=%s runtime calls=%d", err, status.Code(err), fixture.runtime.startCalls)
			}
		})
	}
}

func TestServerRejectsExpiredAuthorityAndPropagatesDeadline(t *testing.T) {
	fixture := newServerFixture(t, campaignNow().Add(time.Hour))
	_, err := fixture.server.Status(context.Background(), &velav1.StageWorkerMemberServiceStatusRequest{
		TargetWorkerMemberId: fixture.local.ID,
		Command:              &velav1.ModelRuntimeServiceStatusRequest{Authority: fixture.authority},
	})
	if status.Code(err) != codes.FailedPrecondition || fixture.runtime.statusCalls != 0 {
		t.Fatalf("expired Status error=%v calls=%d", err, fixture.runtime.statusCalls)
	}

	fixture = newServerFixture(t, time.Time{})
	fixture.runtime.blockStatus = true
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	_, err = fixture.server.Status(ctx, &velav1.StageWorkerMemberServiceStatusRequest{
		TargetWorkerMemberId: fixture.local.ID,
		Command:              &velav1.ModelRuntimeServiceStatusRequest{Authority: fixture.authority},
	})
	if !errors.Is(err, context.DeadlineExceeded) || fixture.runtime.statusCalls != 1 {
		t.Fatalf("deadline Status error=%v calls=%d", err, fixture.runtime.statusCalls)
	}
}

func TestServerAcceptsAuthorityWithinConfiguredClockSkew(t *testing.T) {
	fixture := newServerFixtureWithClockSkew(
		t,
		campaignNow().Add(-time.Second-3200*time.Microsecond),
		30*time.Second,
	)
	response, err := fixture.server.Status(
		context.Background(),
		&velav1.StageWorkerMemberServiceStatusRequest{
			TargetWorkerMemberId: fixture.local.ID,
			Command:              &velav1.ModelRuntimeServiceStatusRequest{Authority: fixture.authority},
		},
	)
	if err != nil || response.GetResult().GetDecision() !=
		velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
		fixture.runtime.statusCalls != 1 {
		t.Fatalf("future-issued Status = %#v calls=%d error=%v", response, fixture.runtime.statusCalls, err)
	}
}

func TestServerRejectsAuthorityBeyondConfiguredClockSkew(t *testing.T) {
	fixture := newServerFixtureWithClockSkew(
		t,
		campaignNow().Add(-time.Second-30*time.Second-time.Millisecond),
		30*time.Second,
	)
	_, err := fixture.server.Status(
		context.Background(),
		&velav1.StageWorkerMemberServiceStatusRequest{
			TargetWorkerMemberId: fixture.local.ID,
			Command:              &velav1.ModelRuntimeServiceStatusRequest{Authority: fixture.authority},
		},
	)
	if status.Code(err) != codes.FailedPrecondition || fixture.runtime.statusCalls != 0 {
		t.Fatalf("over-skew Status error=%v calls=%d", err, fixture.runtime.statusCalls)
	}
}

func TestNewServerRejectsInvalidClockSkew(t *testing.T) {
	fixture := newServerFixture(t, time.Time{})
	for _, maxClockSkew := range []time.Duration{-time.Nanosecond, time.Minute + time.Nanosecond} {
		t.Run(maxClockSkew.String(), func(t *testing.T) {
			server, err := NewServer(ServerConfig{
				Authenticator: fixture.auth,
				Validator:     fixture.server.validator,
				Runtime:       fixture.runtime,
				LocalIdentities: []*velav1.ModelRuntimeIdentity{
					proto.Clone(fixture.server.localIdentities[0]).(*velav1.ModelRuntimeIdentity),
				},
				Members:      []MemberBinding{fixture.leader, fixture.local},
				MaxClockSkew: maxClockSkew,
			})
			if server != nil || err == nil || err.Error() != "stage worker member server clock skew is invalid" {
				t.Fatalf("NewServer skew=%s server=%v error=%v", maxClockSkew, server, err)
			}
		})
	}
}

type serverFixture struct {
	t            *testing.T
	server       *Server
	auth         *mutableAuthenticator
	runtime      *runtimeClient
	signer       *stageauthority.Signer
	authority    *velav1.StageAuthority
	spec         *velav1.StageExecutionSpec
	leader       MemberBinding
	local        MemberBinding
	leaderSPIFFE string
	localSPIFFE  string
}

func newServerFixture(t *testing.T, validatorNow time.Time) *serverFixture {
	return newServerFixtureWithClockSkew(t, validatorNow, 0)
}

func newServerFixtureWithClockSkew(
	t *testing.T,
	validatorNow time.Time,
	maxClockSkew time.Duration,
) *serverFixture {
	t.Helper()
	now := campaignNow()
	keys := map[string][]byte{"authority-v1": []byte("0123456789abcdef0123456789abcdef")}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	if validatorNow.IsZero() {
		validatorNow = now
	}
	validator, err := stageauthority.NewValidator(keys, func() time.Time { return validatorNow })
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	leader := MemberBinding{ID: "49360000-0000-0000-0000-000000000010", Epoch: 7}
	local := MemberBinding{ID: "49360000-0000-0000-0000-000000000020", Epoch: 8}
	leaderSPIFFE := "spiffe://vela.test/stage-worker/member-0"
	localSPIFFE := "spiffe://vela.test/stage-worker/member-1"
	leaderIdentityDigest := sha256.Sum256([]byte(leaderSPIFFE))
	localIdentityDigest := sha256.Sum256([]byte(localSPIFFE))
	spec := &velav1.StageExecutionSpec{ParametersJson: []byte(`{"batch":1}`)}
	specDigest, err := stageauthority.ExecutionSpecDigest(spec)
	if err != nil {
		t.Fatalf("ExecutionSpecDigest: %v", err)
	}
	identity := &velav1.ModelRuntimeIdentity{
		WorkerInstanceId: "49360000-0000-0000-0000-000000000001", WorkerInstanceEpoch: 3,
		WorkerMemberId: local.ID, WorkerMemberEpoch: local.Epoch,
		DeviceSetDigest: bytesOf('d'), MembershipDigest: bytesOf('m'),
		ModelResidencyId: "49360000-0000-0000-0000-000000000002",
		RuntimeIdentity:  "future-llm-runtime-v1", ModelRuntimeEpoch: 12,
		StageProfileRevisionId: "49360000-0000-0000-0000-000000000003",
	}
	unsigned := &velav1.StageAuthority{
		SchemaVersion:     1,
		JobId:             "49360000-0000-0000-0000-000000000101",
		AttemptId:         "49360000-0000-0000-0000-000000000102",
		StageRunId:        "49360000-0000-0000-0000-000000000103",
		StageAttemptId:    "49360000-0000-0000-0000-000000000104",
		StageAllocationId: "49360000-0000-0000-0000-000000000105",
		StageLeaseId:      "49360000-0000-0000-0000-000000000106",
		AttemptFence:      1, StageFence: 2, StageVersion: 3,
		WorkerInstanceId: identity.WorkerInstanceId, WorkerInstanceEpoch: identity.WorkerInstanceEpoch,
		DeviceSetDigest: identity.DeviceSetDigest,
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "49360000-0000-0000-0000-000000000201", DeviceEpoch: 5,
		}},
		MembershipDigest: identity.MembershipDigest,
		Members: []*velav1.StageAuthorityMemberEpoch{
			{WorkerMemberId: leader.ID, MemberEpoch: leader.Epoch, ModelRuntimeEpoch: 11,
				IdentityDigest: leaderIdentityDigest[:]},
			{WorkerMemberId: local.ID, MemberEpoch: local.Epoch, ModelRuntimeEpoch: identity.ModelRuntimeEpoch,
				IdentityDigest: localIdentityDigest[:]},
		},
		ModelResidencyId: identity.ModelResidencyId, ModelRuntimeIdentity: identity.RuntimeIdentity,
		StageProfileRevisionId:      identity.StageProfileRevisionId,
		CapacityObservationSequence: 9, CapacityVector: map[string]int64{"slots": 1},
		LeaseToken: bytesOf('l'), ExecutionNonce: bytesOf('n'),
		SigningKeyId: "authority-v1", IssuedAt: timestamppb.New(now.Add(-time.Second)),
		ExpiresAt:           timestamppb.New(now.Add(5 * time.Minute)),
		MonotonicValidFor:   durationpb.New(5*time.Minute + time.Second),
		ExecutionSpecDigest: specDigest[:], ModelRuntimeBarrierGeneration: 14,
	}
	authority, err := signer.Sign(unsigned)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	auth := &mutableAuthenticator{identity: stageworkertransport.Identity{SPIFFEID: leaderSPIFFE}}
	runtime := &runtimeClient{identity: identity}
	server, err := NewServer(ServerConfig{
		Authenticator: auth, Validator: validator, Runtime: runtime,
		LocalIdentities: []*velav1.ModelRuntimeIdentity{identity},
		Members:         []MemberBinding{leader, local}, MaxClockSkew: maxClockSkew,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return &serverFixture{
		t: t, server: server, auth: auth, runtime: runtime, signer: signer,
		authority: authority, spec: spec, leader: leader, local: local,
		leaderSPIFFE: leaderSPIFFE, localSPIFFE: localSPIFFE,
	}
}

type mutableAuthenticator struct {
	identity stageworkertransport.Identity
}

func (authenticator *mutableAuthenticator) Authenticate(context.Context) (stageworkertransport.Identity, error) {
	return authenticator.identity, nil
}

type runtimeClient struct {
	velav1.ModelRuntimeServiceClient
	identity     *velav1.ModelRuntimeIdentity
	prepareCalls int
	startCalls   int
	statusCalls  int
	blockStatus  bool
}

func (client *runtimeClient) PrepareStage(
	_ context.Context,
	request *velav1.ModelRuntimeServicePrepareStageRequest,
	_ ...grpc.CallOption,
) (*velav1.ModelRuntimeServicePrepareStageResponse, error) {
	client.prepareCalls++
	digest, _ := stageauthority.Digest(request.GetAuthority())
	decision := velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
	if client.prepareCalls > 1 {
		decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED
	}
	return &velav1.ModelRuntimeServicePrepareStageResponse{
		AuthorityDigest: digest[:], Decision: decision,
		State:           velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED,
		RuntimeIdentity: proto.Clone(client.identity).(*velav1.ModelRuntimeIdentity),
	}, nil
}

func (client *runtimeClient) StartStage(
	_ context.Context,
	request *velav1.ModelRuntimeServiceStartStageRequest,
	_ ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceStartStageResponse, error) {
	client.startCalls++
	digest, _ := stageauthority.Digest(request.GetAuthority())
	return &velav1.ModelRuntimeServiceStartStageResponse{
		AuthorityDigest: digest[:],
		Decision:        velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
		State:           velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING,
		StartedAt:       timestamppb.New(campaignNow()),
		RuntimeIdentity: proto.Clone(client.identity).(*velav1.ModelRuntimeIdentity),
	}, nil
}

func (client *runtimeClient) Status(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceStatusRequest,
	_ ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceStatusResponse, error) {
	client.statusCalls++
	if client.blockStatus {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	digest, _ := stageauthority.Digest(request.GetAuthority())
	return &velav1.ModelRuntimeServiceStatusResponse{
		AuthorityDigest: digest[:],
		Decision:        velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
		State:           velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING,
		RuntimeIdentity: proto.Clone(client.identity).(*velav1.ModelRuntimeIdentity),
	}, nil
}

func campaignNow() time.Time {
	return time.Date(2026, 9, 2, 4, 0, 0, 0, time.UTC)
}

func bytesOf(value byte) []byte {
	result := make([]byte, 32)
	for index := range result {
		result[index] = value
	}
	return result
}
