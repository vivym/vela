package h3membercampaign

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/stageworkermembertransport"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	SchemaVersion                 = 1
	campaignServerShutdownTimeout = 5 * time.Second
)

type Outcome string

const (
	OutcomePass          Outcome = "PASS"
	OutcomeFaultRejected Outcome = "FAULT_REJECTED"
)

type Member struct {
	ID           string
	Epoch        int64
	RuntimeEpoch int64
	SPIFFEID     string
	ServerName   string
}

type Topology struct {
	Leader   Member
	Follower Member
}

func FixedTopology() Topology {
	return Topology{
		Leader: Member{
			ID: "49370000-0000-0000-0000-000000000010", Epoch: 7, RuntimeEpoch: 11,
			SPIFFEID: "spiffe://vela.internal/stage-worker/49370000-0000-0000-0000-000000000010",
		},
		Follower: Member{
			ID: "49370000-0000-0000-0000-000000000020", Epoch: 8, RuntimeEpoch: 12,
			SPIFFEID:   "spiffe://vela.internal/stage-worker/49370000-0000-0000-0000-000000000020",
			ServerName: "follower.vela-h3-disposable.svc",
		},
	}
}

type ServerTLSFiles struct {
	Certificate string
	PrivateKey  string
	ClientCA    string
}

type ClientTLSFiles struct {
	Certificate string
	PrivateKey  string
	ServerCA    string
}

type ServerConfig struct {
	ListenAddress  string
	TLS            ServerTLSFiles
	AuthorityKeyID string
	AuthorityKey   []byte
	Ready          chan<- struct{}
	Logf           func(string, ...any)
}

type ClientConfig struct {
	Address              string
	ServerName           string
	TLS                  ClientTLSFiles
	AuthorityKeyID       string
	AuthorityKey         []byte
	DialTimeout          time.Duration
	CommandTimeout       time.Duration
	RemoteStartDelay     time.Duration
	ExpectedStartFailure bool
}

type Receipt struct {
	SchemaVersion                   int       `json:"schema_version"`
	ExerciseID                      string    `json:"exercise_id"`
	AuthorityDigest                 string    `json:"authority_digest"`
	LeaderIdentityDigest            string    `json:"leader_identity_digest"`
	FollowerIdentityDigest          string    `json:"follower_identity_digest"`
	Outcome                         Outcome   `json:"outcome"`
	BarrierPassed                   bool      `json:"barrier_passed"`
	PreparedMembers                 int       `json:"prepared_members"`
	StartedMembers                  int       `json:"started_members"`
	ReportingMembers                int       `json:"reporting_members"`
	CancellationAcknowledgedMembers int       `json:"cancellation_acknowledged_members"`
	AllStopped                      bool      `json:"all_stopped"`
	LocalMemberStopped              bool      `json:"local_member_stopped"`
	RemoteMemberUnavailable         bool      `json:"remote_member_unavailable"`
	FaultPhase                      string    `json:"fault_phase,omitempty"`
	LeaderMemberID                  string    `json:"leader_member_id"`
	FollowerMemberID                string    `json:"follower_member_id"`
	StartedAt                       time.Time `json:"started_at"`
	CompletedAt                     time.Time `json:"completed_at"`
}

func Serve(ctx context.Context, config ServerConfig) error {
	if ctx == nil || config.ListenAddress == "" ||
		config.TLS.Certificate == "" || config.TLS.PrivateKey == "" || config.TLS.ClientCA == "" ||
		config.AuthorityKeyID == "" || len(config.AuthorityKey) < 32 {
		return errors.New("disposable member campaign server configuration is incomplete")
	}
	keys := map[string][]byte{config.AuthorityKeyID: append([]byte(nil), config.AuthorityKey...)}
	validator, err := stageauthority.NewValidator(keys, time.Now)
	stageauthority.ClearKeyring(keys)
	if err != nil {
		return fmt.Errorf("configure disposable member authority validator: %w", err)
	}
	topology := FixedTopology()
	identity := runtimeIdentity(topology.Follower)
	runtime := newCampaignRuntime(identity, config.Logf)
	service, err := stageworkermembertransport.NewServer(
		stageworkermembertransport.ServerConfig{
			Authenticator: stageworkertransport.PeerAuthenticator{},
			Validator:     validator,
			Runtime:       runtime,
			LocalIdentities: []*velav1.ModelRuntimeIdentity{
				identity,
			},
			Members: []stageworkermembertransport.MemberBinding{
				{ID: topology.Leader.ID, Epoch: topology.Leader.Epoch},
				{ID: topology.Follower.ID, Epoch: topology.Follower.Epoch},
			},
		},
	)
	if err != nil {
		return fmt.Errorf("configure disposable member service: %w", err)
	}
	credentials, err := stageworkertransport.NewServerTLSCredentials(
		config.TLS.Certificate, config.TLS.PrivateKey, config.TLS.ClientCA,
	)
	if err != nil {
		return fmt.Errorf("configure disposable member server mTLS: %w", err)
	}
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen for disposable member service: %w", err)
	}
	defer func() { _ = listener.Close() }()
	server := grpc.NewServer(
		grpc.Creds(credentials), grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(4<<20),
	)
	velav1.RegisterStageWorkerMemberServiceServer(server, service)
	stopped := make(chan struct{})
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		select {
		case <-ctx.Done():
			stopServerWithin(server, campaignServerShutdownTimeout)
		case <-stopped:
		}
	}()
	if config.Ready != nil {
		close(config.Ready)
	}
	err = server.Serve(listener)
	close(stopped)
	<-shutdownDone
	if errors.Is(err, grpc.ErrServerStopped) || ctx.Err() != nil {
		return nil
	}
	return err
}

func Run(ctx context.Context, config ClientConfig) (Receipt, error) {
	if ctx == nil || config.Address == "" || config.ServerName == "" ||
		config.TLS.Certificate == "" || config.TLS.PrivateKey == "" || config.TLS.ServerCA == "" ||
		config.AuthorityKeyID == "" || len(config.AuthorityKey) < 32 ||
		config.DialTimeout <= 0 || config.DialTimeout > time.Minute ||
		config.CommandTimeout <= 0 || config.CommandTimeout > 10*time.Minute ||
		config.RemoteStartDelay < 0 || config.RemoteStartDelay > config.CommandTimeout ||
		(config.ExpectedStartFailure && config.RemoteStartDelay == 0) {
		return Receipt{}, errors.New("disposable member campaign client configuration is incomplete")
	}
	topology := FixedTopology()
	credentials, err := stageworkertransport.NewClientTLSCredentials(
		config.TLS.Certificate, config.TLS.PrivateKey, config.TLS.ServerCA, config.ServerName,
	)
	if err != nil {
		return Receipt{}, fmt.Errorf("configure disposable member client mTLS: %w", err)
	}
	dialContext, cancelDial := context.WithTimeout(ctx, config.DialTimeout)
	remote, err := stageworkermembertransport.Dial(
		dialContext,
		stageworkermembertransport.ClientConfig{
			Address: config.Address, TargetWorkerMemberID: topology.Follower.ID,
			TargetIdentityDigest: memberIdentityDigest(topology.Follower),
			TransportCredentials: credentials,
		},
	)
	cancelDial()
	if err != nil {
		return Receipt{}, fmt.Errorf("connect disposable member follower: %w", err)
	}
	defer func() { _ = remote.Close() }()
	local := newCampaignRuntime(runtimeIdentity(topology.Leader), nil)
	var remoteRuntime velav1.ModelRuntimeServiceClient = remote
	if config.RemoteStartDelay > 0 {
		remoteRuntime = &delayedStartClient{
			ModelRuntimeServiceClient: remote,
			delay:                     config.RemoteStartDelay,
		}
	}
	agent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: topology.Leader.ID, Client: local},
			{ID: topology.Follower.ID, Client: remoteRuntime},
		},
		CancellationTimeout: config.CommandTimeout,
	})
	if err != nil {
		return Receipt{}, fmt.Errorf("configure disposable member barrier: %w", err)
	}
	exerciseID, err := uuid.NewRandom()
	if err != nil {
		return Receipt{}, errors.New("generate disposable member exercise identity")
	}
	random := make([]byte, 2*sha256.Size)
	if _, err := rand.Read(random); err != nil {
		return Receipt{}, errors.New("generate disposable member authority secrets")
	}
	assignment, err := campaignAssignment(
		topology,
		config.AuthorityKeyID,
		config.AuthorityKey,
		exerciseID.String(),
		random[:sha256.Size],
		random[sha256.Size:],
	)
	clear(random)
	if err != nil {
		return Receipt{}, err
	}
	authorityDigest, err := stageauthority.Digest(assignment.GetAuthority())
	if err != nil {
		return Receipt{}, fmt.Errorf("digest disposable member authority: %w", err)
	}
	receipt := Receipt{
		SchemaVersion:          SchemaVersion,
		ExerciseID:             exerciseID.String(),
		AuthorityDigest:        hex.EncodeToString(authorityDigest[:]),
		LeaderIdentityDigest:   hex.EncodeToString(memberIdentityDigest(topology.Leader)),
		FollowerIdentityDigest: hex.EncodeToString(memberIdentityDigest(topology.Follower)),
		LeaderMemberID:         topology.Leader.ID,
		FollowerMemberID:       topology.Follower.ID,
	}
	receipt.StartedAt = time.Now().UTC()
	commandContext, cancelCommand := context.WithTimeout(ctx, config.CommandTimeout)
	barrier, err := agent.PrepareAndStart(commandContext, assignment)
	cancelCommand()
	if config.ExpectedStartFailure {
		if err == nil {
			return Receipt{}, errors.New("disposable member start fault was not observed")
		}
		localStopped := local.currentState() ==
			velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED
		if barrier.BarrierPassed || barrier.PreparedMembers != 2 || barrier.StartedMembers != 1 ||
			barrier.CancellationAcknowledgedMembers != 1 || !localStopped {
			return Receipt{}, fmt.Errorf(
				"disposable member start fault did not fail closed: prepared=%d started=%d canceled=%d local_stopped=%t: %w",
				barrier.PreparedMembers, barrier.StartedMembers,
				barrier.CancellationAcknowledgedMembers, localStopped, err,
			)
		}
		receipt.Outcome = OutcomeFaultRejected
		receipt.PreparedMembers = barrier.PreparedMembers
		receipt.StartedMembers = barrier.StartedMembers
		receipt.CancellationAcknowledgedMembers = barrier.CancellationAcknowledgedMembers
		receipt.LocalMemberStopped = true
		receipt.RemoteMemberUnavailable = true
		receipt.FaultPhase = "START"
		completeReceipt(&receipt)
		return receipt, nil
	}
	if err != nil {
		return Receipt{}, fmt.Errorf("run disposable member start barrier: %w", err)
	}
	statusContext, cancelStatus := context.WithTimeout(ctx, config.CommandTimeout)
	status, err := agent.Status(statusContext, assignment.GetAuthority())
	cancelStatus()
	if err != nil || status.ReportingMembers != 2 || status.AllStopped {
		return Receipt{}, errors.New("disposable member running status is incomplete")
	}
	cancelContext, cancelExecution := context.WithTimeout(ctx, config.CommandTimeout)
	cancellation, err := agent.Cancel(
		cancelContext,
		assignment.GetAuthority(),
		velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	)
	cancelExecution()
	if err != nil {
		return Receipt{}, fmt.Errorf("cancel disposable member execution: %w", err)
	}
	finalContext, cancelFinal := context.WithTimeout(ctx, config.CommandTimeout)
	finalStatus, err := agent.Status(finalContext, assignment.GetAuthority())
	cancelFinal()
	if err != nil || finalStatus.ReportingMembers != 2 || !finalStatus.AllStopped {
		return Receipt{}, errors.New("disposable member final status is incomplete")
	}
	receipt.Outcome = OutcomePass
	receipt.BarrierPassed = barrier.BarrierPassed
	receipt.PreparedMembers = barrier.PreparedMembers
	receipt.StartedMembers = barrier.StartedMembers
	receipt.ReportingMembers = status.ReportingMembers
	receipt.CancellationAcknowledgedMembers = cancellation.AcknowledgedMembers
	receipt.AllStopped = finalStatus.AllStopped
	receipt.LocalMemberStopped = finalStatus.AllStopped
	completeReceipt(&receipt)
	return receipt, nil
}

func completeReceipt(receipt *Receipt) {
	receipt.CompletedAt = time.Now().UTC()
	if receipt.CompletedAt.Before(receipt.StartedAt) {
		receipt.CompletedAt = receipt.StartedAt
	}
}

func campaignAssignment(
	topology Topology,
	keyID string,
	key []byte,
	exerciseID string,
	leaseToken []byte,
	executionNonce []byte,
) (*velav1.StageAssignment, error) {
	now := time.Now().UTC()
	spec := &velav1.StageExecutionSpec{ParametersJson: []byte(`{"campaign":"disposable-member-smoke"}`)}
	specDigest, err := stageauthority.ExecutionSpecDigest(spec)
	if err != nil {
		return nil, err
	}
	deviceSetDigest := sha256.Sum256([]byte("vela-disposable-member-device-set-v1"))
	membershipDigest := sha256.Sum256([]byte("vela-disposable-member-membership-v1"))
	unsigned := &velav1.StageAuthority{
		SchemaVersion:     1,
		JobId:             "49370000-0000-0000-0000-000000000101",
		AttemptId:         "49370000-0000-0000-0000-000000000102",
		StageRunId:        "49370000-0000-0000-0000-000000000103",
		StageAttemptId:    "49370000-0000-0000-0000-000000000104",
		StageAllocationId: "49370000-0000-0000-0000-000000000105",
		StageLeaseId:      exerciseID,
		AttemptFence:      1, StageFence: 1, StageVersion: 1,
		WorkerInstanceId:    "49370000-0000-0000-0000-000000000001",
		WorkerInstanceEpoch: 1,
		DeviceSetDigest:     deviceSetDigest[:],
		Devices: []*velav1.StageAuthorityDeviceEpoch{
			{DeviceId: "49370000-0000-0000-0000-000000000201", DeviceEpoch: 1},
			{DeviceId: "49370000-0000-0000-0000-000000000202", DeviceEpoch: 1},
		},
		MembershipDigest: membershipDigest[:],
		Members: []*velav1.StageAuthorityMemberEpoch{
			{WorkerMemberId: topology.Leader.ID, MemberEpoch: topology.Leader.Epoch,
				ModelRuntimeEpoch: topology.Leader.RuntimeEpoch,
				IdentityDigest:    memberIdentityDigest(topology.Leader)},
			{WorkerMemberId: topology.Follower.ID, MemberEpoch: topology.Follower.Epoch,
				ModelRuntimeEpoch: topology.Follower.RuntimeEpoch,
				IdentityDigest:    memberIdentityDigest(topology.Follower)},
		},
		ModelResidencyId:            "49370000-0000-0000-0000-000000000002",
		ModelRuntimeIdentity:        "vela-disposable-multi-member-runtime-v1",
		StageProfileRevisionId:      "49370000-0000-0000-0000-000000000003",
		CapacityObservationSequence: 1,
		CapacityVector:              map[string]int64{"active_stage_slots": 1, "gpu_count": 2},
		LeaseToken:                  append([]byte(nil), leaseToken...),
		ExecutionNonce:              append([]byte(nil), executionNonce...),
		SigningKeyId:                keyID, IssuedAt: timestamppb.New(now.Add(-time.Second)),
		ExpiresAt:           timestamppb.New(now.Add(5 * time.Minute)),
		MonotonicValidFor:   durationpb.New(5 * time.Minute),
		ExecutionSpecDigest: specDigest[:], ModelRuntimeBarrierGeneration: 1,
	}
	keys := map[string][]byte{keyID: append([]byte(nil), key...)}
	signer, err := stageauthority.NewSigner(keys)
	stageauthority.ClearKeyring(keys)
	if err != nil {
		return nil, fmt.Errorf("configure disposable member authority signer: %w", err)
	}
	authority, err := signer.Sign(unsigned)
	if err != nil {
		return nil, fmt.Errorf("sign disposable member authority: %w", err)
	}
	return &velav1.StageAssignment{
		Authority: authority, ExecutionSpec: spec,
		RequiredWorkerMemberIds: []string{topology.Leader.ID, topology.Follower.ID},
		MemberStartTimeout:      durationpb.New(time.Minute),
	}, nil
}

func stopServerWithin(server *grpc.Server, timeout time.Duration) {
	gracefulStopDone := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(gracefulStopDone)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-gracefulStopDone:
	case <-timer.C:
		server.Stop()
		<-gracefulStopDone
	}
}

func runtimeIdentity(member Member) *velav1.ModelRuntimeIdentity {
	deviceSetDigest := sha256.Sum256([]byte("vela-disposable-member-device-set-v1"))
	membershipDigest := sha256.Sum256([]byte("vela-disposable-member-membership-v1"))
	return &velav1.ModelRuntimeIdentity{
		WorkerInstanceId:    "49370000-0000-0000-0000-000000000001",
		WorkerInstanceEpoch: 1, DeviceSetDigest: deviceSetDigest[:],
		MembershipDigest:       membershipDigest[:],
		ModelResidencyId:       "49370000-0000-0000-0000-000000000002",
		RuntimeIdentity:        "vela-disposable-multi-member-runtime-v1",
		ModelRuntimeEpoch:      member.RuntimeEpoch,
		StageProfileRevisionId: "49370000-0000-0000-0000-000000000003",
		WorkerMemberId:         member.ID, WorkerMemberEpoch: member.Epoch,
	}
}

func memberIdentityDigest(member Member) []byte {
	digest := sha256.Sum256([]byte(member.SPIFFEID))
	return digest[:]
}

type campaignRuntime struct {
	velav1.ModelRuntimeServiceClient
	identity *velav1.ModelRuntimeIdentity
	logf     func(string, ...any)
	mu       sync.Mutex
	state    velav1.ModelRuntimeExecutionState
}

func newCampaignRuntime(identity *velav1.ModelRuntimeIdentity, logf func(string, ...any)) *campaignRuntime {
	return &campaignRuntime{identity: proto.Clone(identity).(*velav1.ModelRuntimeIdentity), logf: logf}
}

func (runtime *campaignRuntime) PrepareStage(
	_ context.Context,
	request *velav1.ModelRuntimeServicePrepareStageRequest,
	_ ...grpc.CallOption,
) (*velav1.ModelRuntimeServicePrepareStageResponse, error) {
	digest, err := stageauthority.Digest(request.GetAuthority())
	if err != nil {
		return nil, err
	}
	runtime.mu.Lock()
	runtime.state = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED
	runtime.mu.Unlock()
	if runtime.logf != nil {
		runtime.logf("prepared member %s", runtime.identity.GetWorkerMemberId())
	}
	return &velav1.ModelRuntimeServicePrepareStageResponse{
		AuthorityDigest: digest[:],
		Decision:        velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
		State:           velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED,
		RuntimeIdentity: proto.Clone(runtime.identity).(*velav1.ModelRuntimeIdentity),
	}, nil
}

func (runtime *campaignRuntime) StartStage(
	_ context.Context,
	request *velav1.ModelRuntimeServiceStartStageRequest,
	_ ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceStartStageResponse, error) {
	digest, err := stageauthority.Digest(request.GetAuthority())
	if err != nil {
		return nil, err
	}
	runtime.mu.Lock()
	runtime.state = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING
	runtime.mu.Unlock()
	return &velav1.ModelRuntimeServiceStartStageResponse{
		AuthorityDigest: digest[:],
		Decision:        velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
		State:           velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING,
		StartedAt:       timestamppb.Now(),
		RuntimeIdentity: proto.Clone(runtime.identity).(*velav1.ModelRuntimeIdentity),
	}, nil
}

func (runtime *campaignRuntime) CancelStage(
	_ context.Context,
	request *velav1.ModelRuntimeServiceCancelStageRequest,
	_ ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceCancelStageResponse, error) {
	digest, err := stageauthority.Digest(request.GetAuthority())
	if err != nil {
		return nil, err
	}
	runtime.mu.Lock()
	runtime.state = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED
	runtime.mu.Unlock()
	return &velav1.ModelRuntimeServiceCancelStageResponse{
		AuthorityDigest:          digest[:],
		Decision:                 velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
		CancellationAcknowledged: true,
		State:                    velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED,
		RuntimeIdentity:          proto.Clone(runtime.identity).(*velav1.ModelRuntimeIdentity),
	}, nil
}

func (runtime *campaignRuntime) Status(
	_ context.Context,
	request *velav1.ModelRuntimeServiceStatusRequest,
	_ ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceStatusResponse, error) {
	digest, err := stageauthority.Digest(request.GetAuthority())
	if err != nil {
		return nil, err
	}
	runtime.mu.Lock()
	state := runtime.state
	runtime.mu.Unlock()
	return &velav1.ModelRuntimeServiceStatusResponse{
		AuthorityDigest: digest[:],
		Decision:        velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
		State:           state, Sequence: 1,
		RuntimeIdentity: proto.Clone(runtime.identity).(*velav1.ModelRuntimeIdentity),
	}, nil
}

func (runtime *campaignRuntime) currentState() velav1.ModelRuntimeExecutionState {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.state
}

type delayedStartClient struct {
	velav1.ModelRuntimeServiceClient
	delay time.Duration
}

func (client *delayedStartClient) StartStage(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceStartStageRequest,
	options ...grpc.CallOption,
) (*velav1.ModelRuntimeServiceStartStageResponse, error) {
	timer := time.NewTimer(client.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return client.ModelRuntimeServiceClient.StartStage(ctx, request, options...)
	}
}

var _ velav1.ModelRuntimeServiceClient = (*campaignRuntime)(nil)
var _ velav1.ModelRuntimeServiceClient = (*delayedStartClient)(nil)
