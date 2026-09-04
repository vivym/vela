package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/authoritypolicy"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/stageworkermembertransport"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestSingleGPUProductionCompositionSmoke(t *testing.T) {
	identity := &velav1.ModelRuntimeIdentity{
		WorkerInstanceId:       "49800000-0000-0000-0000-000000000001",
		WorkerInstanceEpoch:    5,
		DeviceSetDigest:        bytes.Repeat([]byte{0x41}, sha256.Size),
		MembershipDigest:       bytes.Repeat([]byte{0x42}, sha256.Size),
		ModelResidencyId:       "49800000-0000-0000-0000-000000000002",
		RuntimeIdentity:        "h3-dit-runtime-smoke-v1",
		ModelRuntimeEpoch:      7,
		StageProfileRevisionId: "49800000-0000-0000-0000-000000000003",
		WorkerMemberId:         "49800000-0000-0000-0000-000000000004",
		WorkerMemberEpoch:      11,
	}
	runtimeSocket, runtimeProbe := serveSingleGPUModelRuntime(t, identity)
	controlAddress, controlFiles, control := serveStageWorkerControlSmoke(t, identity)
	artifactEndpoint, artifactCA := serveArtifactStoreSmoke(t)

	directory := t.TempDir()
	authorityKey := bytes.Repeat([]byte{0x71}, 32)
	authorityKeyring := []byte(fmt.Sprintf(
		`{"stage-authority-smoke-v1":%q}`,
		base64.StdEncoding.EncodeToString(authorityKey),
	))
	authorityPath := writeSmokeSecret(t, directory, "authority-keyring.json", authorityKeyring)
	accessKeyPath := writeSmokeSecret(t, directory, "access-key-id", []byte("stage-smoke-access"))
	secretKeyPath := writeSmokeSecret(t, directory, "secret-access-key", []byte("stage-smoke-secret"))
	artifactCAPath := writeSmokeSecret(t, directory, "artifact-ca.crt", artifactCA)
	scratchRoot := filepath.Join(directory, "scratch")
	configuration := config{
		workerInstanceID:    uuid.MustParse(identity.GetWorkerInstanceId()),
		workerInstanceEpoch: identity.GetWorkerInstanceEpoch(),
		workerMemberID:      uuid.MustParse(identity.GetWorkerMemberId()),
		workerMemberEpoch:   identity.GetWorkerMemberEpoch(),
		members: []memberConfig{{
			workerMemberID: uuid.MustParse(identity.GetWorkerMemberId()),
			memberEpoch:    identity.GetWorkerMemberEpoch(),
			identityDigest: sha256.Sum256([]byte("spiffe://vela.internal/stage-worker/smoke-member")),
		}},
		devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "49800000-0000-0000-0000-000000000005", DeviceEpoch: 3,
		}},
		capacityVector:                  map[string]int64{"active_stage_slots": 1, "gpu_count": 1},
		controlAddress:                  controlAddress,
		controlServerName:               "stage-worker-control.internal",
		tlsCertificateFile:              controlFiles.clientCertificate,
		tlsPrivateKeyFile:               controlFiles.clientPrivateKey,
		controlCAFile:                   controlFiles.ca,
		runtimeSocket:                   runtimeSocket,
		runtimeExpectedUID:              uint32(os.Geteuid()),
		scratchRoot:                     scratchRoot,
		productionStateRoot:             filepath.Join(scratchRoot, "production-state"),
		inputRoot:                       filepath.Join(scratchRoot, "inputs"),
		inputTransferJournalRoot:        filepath.Join(scratchRoot, "input-transfer-journal"),
		outputRoot:                      filepath.Join(scratchRoot, "outputs"),
		materializationJournalRoot:      filepath.Join(scratchRoot, "materialization-journal"),
		materializationJournalLimit:     16,
		authorityKeyringFile:            authorityPath,
		authorityActiveKeyID:            "stage-authority-smoke-v1",
		connectorRevisionID:             uuid.MustParse("49800000-0000-0000-0000-000000000006"),
		capacityTTL:                     2 * time.Minute,
		heartbeatInterval:               20 * time.Second,
		retryMinimum:                    time.Second,
		retryMaximum:                    30 * time.Second,
		artifactS3Endpoint:              artifactEndpoint,
		artifactS3Region:                "us-smoke-1",
		artifactS3Bucket:                "vela-stage-artifacts",
		artifactS3AccessKeyFile:         accessKeyPath,
		artifactS3SecretKeyFile:         secretKeyPath,
		artifactS3CAFile:                artifactCAPath,
		artifactS3PathStyle:             true,
		artifactSignedGETTTL:            5 * time.Minute,
		sourceLossRetry:                 30 * time.Second,
		sourceLossConsumedResourceUnits: 1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan error, 1)
	go func() { done <- runWithContext(ctx, configuration) }()
	select {
	case <-control.acquired:
		cancel()
	case err := <-done:
		t.Fatalf("Stage Worker stopped before acquire: %v", err)
	case <-ctx.Done():
		t.Fatal("Stage Worker did not reach acquire before timeout")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runWithContext: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stage Worker did not close after cancellation")
	}

	operations, registration, capacity, acquire := control.snapshot()
	if !reflect.DeepEqual(operations, []string{"capacity", "register", "capacity", "acquire"}) {
		t.Fatalf("Stage Worker control operations = %v", operations)
	}
	if registration.GetRuntimeIdentity().GetModelRuntimeEpoch() != identity.GetModelRuntimeEpoch() ||
		len(registration.GetDevices()) != 1 || registration.GetDevices()[0].GetDeviceEpoch() != 3 ||
		len(registration.GetMembers()) != 1 || registration.GetMembers()[0].GetMemberEpoch() != 11 {
		t.Fatalf("Stage Worker registration = %#v", registration)
	}
	if capacity.GetCapacityVector()["gpu_count"] != 1 ||
		acquire.GetModelResidencyId() != identity.GetModelResidencyId() ||
		acquire.GetModelRuntimeEpoch() != identity.GetModelRuntimeEpoch() {
		t.Fatalf("Stage Worker capacity/acquire = %#v/%#v", capacity, acquire)
	}
	if checks := runtimeProbe.snapshot(); !reflect.DeepEqual(checks, []velav1.ModelRuntimeReadinessCheck{
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_DEVICE,
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_BACKEND,
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP,
		velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_CANARY,
	}) {
		t.Fatalf("ModelRuntime readiness checks = %v", checks)
	}
	state, err := stageworkeragent.NewFileProductionState(stageworkeragent.FileProductionStateConfig{
		Directory:        configuration.productionStateRoot,
		WorkerInstanceID: configuration.workerInstanceID, WorkerInstanceEpoch: configuration.workerInstanceEpoch,
		WorkerMemberID: configuration.workerMemberID,
	})
	if err != nil {
		t.Fatalf("reopen closed production state: %v", err)
	}
	if err := state.Close(); err != nil {
		t.Fatalf("close reopened production state: %v", err)
	}
	journal, err := stageworkeragent.NewFileInputTransferJournal(configuration.inputTransferJournalRoot)
	if err != nil {
		t.Fatalf("reopen closed input transfer journal: %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("close reopened input transfer journal: %v", err)
	}
}

func TestMultiMemberFollowerStartsServerWithoutDialingLeader(t *testing.T) {
	identity := productionSmokeIdentity("49800000-0000-0000-0000-000000000004", 11)
	configuration := productionSmokeConfig(t, identity)
	listenAddress := unusedSmokeTCPAddress(t)
	configureMemberTransportSmoke(
		t,
		&configuration,
		"49800000-0000-0000-0000-000000000003",
		9,
		"127.0.0.1:1",
		listenAddress,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runtime, err := newProductionRuntime(ctx, configuration)
	if err != nil {
		t.Fatalf("build follower production runtime: %v", err)
	}
	defer func() {
		if closeErr := runtime.Close(); closeErr != nil {
			t.Errorf("close follower production runtime: %v", closeErr)
		}
	}()
	connection, err := net.DialTimeout("tcp", listenAddress, time.Second)
	if err != nil {
		t.Fatalf("follower member server was not listening before peer dial: %v", err)
	}
	_ = connection.Close()
}

func TestProductionRuntimePropagatesAuthorityClockSkew(t *testing.T) {
	identity := productionSmokeIdentity("49800000-0000-0000-0000-000000000004", 11)
	configuration := productionSmokeConfig(t, identity)
	configureMemberTransportSmoke(
		t,
		&configuration,
		"49800000-0000-0000-0000-000000000003",
		9,
		"127.0.0.1:1",
		unusedSmokeTCPAddress(t),
	)
	var memberClockSkew, materializationClockSkew time.Duration
	consumers := productionAuthorityConsumers{
		newMemberServer: func(
			config stageworkermembertransport.ServerConfig,
		) (*stageworkermembertransport.Server, error) {
			memberClockSkew = config.MaxClockSkew
			return stageworkermembertransport.NewServer(config)
		},
		newMaterializingAgent: func(
			runtime *stageworkeragent.Agent,
			control stageworkeragent.ControlClient,
			config stageworkeragent.MaterializationConfig,
			resolver stageworkeragent.InputResolver,
		) (*stageworkeragent.StreamAgent, error) {
			materializationClockSkew = config.MaxClockSkew
			return stageworkeragent.NewInputResolvingMaterializingStreamAgent(
				runtime,
				control,
				config,
				resolver,
			)
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runtime, err := newProductionRuntimeUsing(ctx, configuration, consumers)
	if err != nil {
		t.Fatalf("build production runtime: %v", err)
	}
	defer func() {
		if closeErr := runtime.Close(); closeErr != nil {
			t.Errorf("close production runtime: %v", closeErr)
		}
	}()
	if memberClockSkew != authoritypolicy.ProductionMaxClockSkew ||
		materializationClockSkew != authoritypolicy.ProductionMaxClockSkew {
		t.Fatalf(
			"production authority clock skew member/materialization = %s/%s, want %s",
			memberClockSkew,
			materializationClockSkew,
			authoritypolicy.ProductionMaxClockSkew,
		)
	}
}

func TestMultiMemberLeaderRequiresReachableFollower(t *testing.T) {
	identity := productionSmokeIdentity("49800000-0000-0000-0000-000000000003", 9)
	configuration := productionSmokeConfig(t, identity)
	configureMemberTransportSmoke(
		t,
		&configuration,
		"49800000-0000-0000-0000-000000000004",
		11,
		"127.0.0.1:1",
		unusedSmokeTCPAddress(t),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	runtime, err := newProductionRuntime(ctx, configuration)
	if runtime != nil || err == nil || !strings.Contains(err.Error(), "connect to Stage Worker member") {
		t.Fatalf("unreachable follower runtime=%T error=%v", runtime, err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("member dial was not bounded: %s", elapsed)
	}
}

func productionSmokeIdentity(memberID string, memberEpoch int64) *velav1.ModelRuntimeIdentity {
	return &velav1.ModelRuntimeIdentity{
		WorkerInstanceId:       "49800000-0000-0000-0000-000000000001",
		WorkerInstanceEpoch:    5,
		DeviceSetDigest:        bytes.Repeat([]byte{0x41}, sha256.Size),
		MembershipDigest:       bytes.Repeat([]byte{0x42}, sha256.Size),
		ModelResidencyId:       "49800000-0000-0000-0000-000000000002",
		RuntimeIdentity:        "h3-distributed-runtime-smoke-v1",
		ModelRuntimeEpoch:      7,
		StageProfileRevisionId: "49800000-0000-0000-0000-000000000003",
		WorkerMemberId:         memberID,
		WorkerMemberEpoch:      memberEpoch,
	}
}

func productionSmokeConfig(t *testing.T, identity *velav1.ModelRuntimeIdentity) config {
	t.Helper()
	runtimeSocket, _ := serveSingleGPUModelRuntime(t, identity)
	controlAddress, controlFiles, _ := serveStageWorkerControlSmoke(t, identity)
	artifactEndpoint, artifactCA := serveArtifactStoreSmoke(t)
	directory := t.TempDir()
	authorityKeyring := []byte(fmt.Sprintf(
		`{"stage-authority-smoke-v1":%q}`,
		base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x71}, 32)),
	))
	scratchRoot := filepath.Join(directory, "scratch")
	return config{
		workerInstanceID:    uuid.MustParse(identity.GetWorkerInstanceId()),
		workerInstanceEpoch: identity.GetWorkerInstanceEpoch(),
		workerMemberID:      uuid.MustParse(identity.GetWorkerMemberId()),
		workerMemberEpoch:   identity.GetWorkerMemberEpoch(),
		devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: "49800000-0000-0000-0000-000000000005", DeviceEpoch: 3,
		}},
		capacityVector:                  map[string]int64{"active_stage_slots": 1, "gpu_count": 1},
		controlAddress:                  controlAddress,
		controlServerName:               "stage-worker-control.internal",
		tlsCertificateFile:              controlFiles.clientCertificate,
		tlsPrivateKeyFile:               controlFiles.clientPrivateKey,
		controlCAFile:                   controlFiles.ca,
		runtimeSocket:                   runtimeSocket,
		runtimeExpectedUID:              uint32(os.Geteuid()),
		scratchRoot:                     scratchRoot,
		productionStateRoot:             filepath.Join(scratchRoot, "production-state"),
		inputRoot:                       filepath.Join(scratchRoot, "inputs"),
		inputTransferJournalRoot:        filepath.Join(scratchRoot, "input-transfer-journal"),
		outputRoot:                      filepath.Join(scratchRoot, "outputs"),
		materializationJournalRoot:      filepath.Join(scratchRoot, "materialization-journal"),
		materializationJournalLimit:     16,
		authorityKeyringFile:            writeSmokeSecret(t, directory, "authority-keyring.json", authorityKeyring),
		authorityActiveKeyID:            "stage-authority-smoke-v1",
		connectorRevisionID:             uuid.MustParse("49800000-0000-0000-0000-000000000006"),
		capacityTTL:                     2 * time.Minute,
		heartbeatInterval:               20 * time.Second,
		retryMinimum:                    time.Second,
		retryMaximum:                    30 * time.Second,
		artifactS3Endpoint:              artifactEndpoint,
		artifactS3Region:                "us-smoke-1",
		artifactS3Bucket:                "vela-stage-artifacts",
		artifactS3AccessKeyFile:         writeSmokeSecret(t, directory, "access-key-id", []byte("stage-smoke-access")),
		artifactS3SecretKeyFile:         writeSmokeSecret(t, directory, "secret-access-key", []byte("stage-smoke-secret")),
		artifactS3CAFile:                writeSmokeSecret(t, directory, "artifact-ca.crt", artifactCA),
		artifactS3PathStyle:             true,
		artifactSignedGETTTL:            5 * time.Minute,
		sourceLossRetry:                 30 * time.Second,
		sourceLossConsumedResourceUnits: 1,
	}
}

func configureMemberTransportSmoke(
	t *testing.T,
	configuration *config,
	peerID string,
	peerEpoch int64,
	peerAddress string,
	localAddress string,
) {
	t.Helper()
	ca, caKey, caPEM := issueSmokeCA(t)
	localSPIFFE, err := url.Parse("spiffe://vela.internal/stage-worker/" + configuration.workerMemberID.String())
	if err != nil {
		t.Fatalf("parse local member SPIFFE identity: %v", err)
	}
	localDigest := sha256.Sum256([]byte(localSPIFFE.String()))
	peerSPIFFE := "spiffe://vela.internal/stage-worker/" + peerID
	peerDigest := sha256.Sum256([]byte(peerSPIFFE))
	serverName := "local-member.internal"
	serverCertificate, serverKey := issueSmokeCertificate(
		t, ca, caKey, serverName, []string{serverName}, []*url.URL{localSPIFFE},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	)
	clientCertificate, clientKey := issueSmokeCertificate(
		t, ca, caKey, configuration.workerMemberID.String(), nil, []*url.URL{localSPIFFE},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	directory := t.TempDir()
	configuration.memberListenAddress = localAddress
	configuration.memberClientCertificateFile = writeSmokeSecret(t, directory, "client.crt", clientCertificate)
	configuration.memberClientPrivateKeyFile = writeSmokeSecret(t, directory, "client.key", clientKey)
	configuration.memberServerCAFile = writeSmokeSecret(t, directory, "server-ca.crt", caPEM)
	configuration.memberServerCertificateFile = writeSmokeSecret(t, directory, "server.crt", serverCertificate)
	configuration.memberServerPrivateKeyFile = writeSmokeSecret(t, directory, "server.key", serverKey)
	configuration.memberClientCAFile = writeSmokeSecret(t, directory, "client-ca.crt", caPEM)
	configuration.memberDialTimeout = time.Second
	configuration.memberShutdownTimeout = time.Second
	local := memberConfig{
		workerMemberID: configuration.workerMemberID,
		memberEpoch:    configuration.workerMemberEpoch,
		identityDigest: localDigest,
		address:        localAddress,
		serverName:     serverName,
	}
	peer := memberConfig{
		workerMemberID: uuid.MustParse(peerID),
		memberEpoch:    peerEpoch,
		identityDigest: peerDigest,
		address:        peerAddress,
		serverName:     "peer-member.internal",
	}
	configuration.members = []memberConfig{local, peer}
	if peer.workerMemberID.String() < local.workerMemberID.String() {
		configuration.members[0], configuration.members[1] = peer, local
	}
}

func unusedSmokeTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve smoke TCP address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release smoke TCP address: %v", err)
	}
	return address
}

type smokeRuntimeProbe struct {
	velav1.UnimplementedModelRuntimeServiceServer
	identity *velav1.ModelRuntimeIdentity
	mu       sync.Mutex
	checks   []velav1.ModelRuntimeReadinessCheck
}

func (server *smokeRuntimeProbe) DiscoverRuntimeIdentities(
	_ context.Context,
	request *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest,
) (*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse, error) {
	if request.GetWorkerInstanceId() != server.identity.GetWorkerInstanceId() ||
		request.GetWorkerInstanceEpoch() != server.identity.GetWorkerInstanceEpoch() ||
		request.GetWorkerMemberId() != server.identity.GetWorkerMemberId() ||
		request.GetWorkerMemberEpoch() != server.identity.GetWorkerMemberEpoch() {
		return &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse{
			Detail: "runtime identity expectation mismatch",
		}, nil
	}
	return &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse{
		Identities: []*velav1.ModelRuntimeIdentity{
			proto.Clone(server.identity).(*velav1.ModelRuntimeIdentity),
		},
		Detail: "runtime identity discovered",
	}, nil
}

func (server *smokeRuntimeProbe) ProbeReadiness(
	_ context.Context,
	request *velav1.ModelRuntimeServiceProbeReadinessRequest,
) (*velav1.ModelRuntimeServiceProbeReadinessResponse, error) {
	server.mu.Lock()
	server.checks = append(server.checks, request.GetCheck())
	server.mu.Unlock()
	return &velav1.ModelRuntimeServiceProbeReadinessResponse{
		Identity: proto.Clone(server.identity).(*velav1.ModelRuntimeIdentity),
		Check:    request.GetCheck(), Ready: true,
		Evidence: []byte("single-gpu-smoke:" + request.GetCheck().String()), Detail: "ready",
	}, nil
}

func (server *smokeRuntimeProbe) snapshot() []velav1.ModelRuntimeReadinessCheck {
	server.mu.Lock()
	defer server.mu.Unlock()
	return append([]velav1.ModelRuntimeReadinessCheck(nil), server.checks...)
}

func serveSingleGPUModelRuntime(
	t *testing.T,
	identity *velav1.ModelRuntimeIdentity,
) (string, *smokeRuntimeProbe) {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "vela-stage-smoke-")
	if err != nil {
		t.Fatalf("create ModelRuntime smoke directory: %v", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("protect ModelRuntime smoke directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socketPath := filepath.Join(directory, "runtime.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen on ModelRuntime smoke socket: %v", err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		t.Fatalf("protect ModelRuntime smoke socket: %v", err)
	}
	probe := &smokeRuntimeProbe{identity: proto.Clone(identity).(*velav1.ModelRuntimeIdentity)}
	server := grpc.NewServer()
	velav1.RegisterModelRuntimeServiceServer(server, probe)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		if err := <-done; err != nil && err != grpc.ErrServerStopped {
			t.Errorf("serve ModelRuntime smoke: %v", err)
		}
	})
	return socketPath, probe
}

type smokeControlFiles struct {
	clientCertificate string
	clientPrivateKey  string
	ca                string
}

type smokeControlHandler struct {
	identity     *velav1.ModelRuntimeIdentity
	acquired     chan struct{}
	once         sync.Once
	mu           sync.Mutex
	operations   []string
	registration *velav1.RegisterWorkerEvidenceRequest
	capacity     *velav1.ReportStageCapacityObservationRequest
	acquire      *velav1.AcquireStageRequest
}

func (handler *smokeControlHandler) Handle(
	_ context.Context,
	identity stageworkertransport.Identity,
	_ int64,
	request *velav1.StageWorkerControlServiceConnectRequest,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	if identity.SPIFFEID != "spiffe://vela.internal/stage-worker/smoke-member" {
		return nil, fmt.Errorf("unexpected smoke Stage Worker identity %q", identity.SPIFFEID)
	}
	response := &velav1.StageWorkerControlServiceConnectResponse{RequestId: request.GetRequestId()}
	handler.mu.Lock()
	switch operation := request.GetOperation().(type) {
	case *velav1.StageWorkerControlServiceConnectRequest_RegisterWorkerEvidence:
		handler.operations = append(handler.operations, "register")
		handler.registration = proto.Clone(operation.RegisterWorkerEvidence).(*velav1.RegisterWorkerEvidenceRequest)
		response.Result = &velav1.StageWorkerControlServiceConnectResponse_WorkerReadinessDecision{
			WorkerReadinessDecision: handler.readiness(),
		}
	case *velav1.StageWorkerControlServiceConnectRequest_ReportCapacityObservation:
		handler.operations = append(handler.operations, "capacity")
		handler.capacity = proto.Clone(operation.ReportCapacityObservation).(*velav1.ReportStageCapacityObservationRequest)
		response.Result = &velav1.StageWorkerControlServiceConnectResponse_WorkerReadinessDecision{
			WorkerReadinessDecision: handler.readiness(),
		}
		response.GetWorkerReadinessDecision().CapacityObservationSequence =
			operation.ReportCapacityObservation.GetObservationSequence()
	case *velav1.StageWorkerControlServiceConnectRequest_AcquireStage:
		handler.operations = append(handler.operations, "acquire")
		handler.acquire = proto.Clone(operation.AcquireStage).(*velav1.AcquireStageRequest)
		response.Result = &velav1.StageWorkerControlServiceConnectResponse_NoWork{
			NoWork: &velav1.NoStageWork{RetryAfter: durationpb.New(time.Hour)},
		}
		handler.once.Do(func() { close(handler.acquired) })
	default:
		handler.mu.Unlock()
		return nil, fmt.Errorf("unexpected smoke Stage Worker operation %T", operation)
	}
	handler.mu.Unlock()
	return response, nil
}

func (handler *smokeControlHandler) readiness() *velav1.WorkerReadinessDecision {
	return &velav1.WorkerReadinessDecision{
		WorkerInstanceId:    handler.identity.GetWorkerInstanceId(),
		WorkerInstanceEpoch: handler.identity.GetWorkerInstanceEpoch(), Ready: true, Reason: "READY",
		ModelRuntimeBarrierGeneration: handler.identity.GetModelRuntimeEpoch(),
		LeaderWorkerMemberId:          handler.identity.GetWorkerMemberId(),
	}
}

func (handler *smokeControlHandler) snapshot() (
	[]string,
	*velav1.RegisterWorkerEvidenceRequest,
	*velav1.ReportStageCapacityObservationRequest,
	*velav1.AcquireStageRequest,
) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	return append([]string(nil), handler.operations...), handler.registration, handler.capacity, handler.acquire
}

func serveStageWorkerControlSmoke(
	t *testing.T,
	identity *velav1.ModelRuntimeIdentity,
) (string, smokeControlFiles, *smokeControlHandler) {
	t.Helper()
	ca, caKey, caPEM := issueSmokeCA(t)
	serverPEM, serverKeyPEM := issueSmokeCertificate(
		t, ca, caKey, "stage-worker-control.internal",
		[]string{"stage-worker-control.internal"}, nil, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	)
	spiffeID, err := url.Parse("spiffe://vela.internal/stage-worker/smoke-member")
	if err != nil {
		t.Fatalf("parse smoke Stage Worker identity: %v", err)
	}
	clientPEM, clientKeyPEM := issueSmokeCertificate(
		t, ca, caKey, "smoke-member", nil, []*url.URL{spiffeID},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	directory := t.TempDir()
	serverCertificate := writeSmokeSecret(t, directory, "server.crt", serverPEM)
	serverPrivateKey := writeSmokeSecret(t, directory, "server.key", serverKeyPEM)
	clientCertificate := writeSmokeSecret(t, directory, "client.crt", clientPEM)
	clientPrivateKey := writeSmokeSecret(t, directory, "client.key", clientKeyPEM)
	caPath := writeSmokeSecret(t, directory, "ca.crt", caPEM)
	serverCredentials, err := stageworkertransport.NewServerTLSCredentials(
		serverCertificate, serverPrivateKey, caPath,
	)
	if err != nil {
		t.Fatalf("configure smoke Stage Worker control mTLS: %v", err)
	}
	handler := &smokeControlHandler{
		identity: proto.Clone(identity).(*velav1.ModelRuntimeIdentity), acquired: make(chan struct{}),
	}
	service, err := stageworkertransport.NewServer(stageworkertransport.ServerConfig{
		Authenticator: stageworkertransport.PeerAuthenticator{}, Handler: handler,
		StopSource: stageworkertransport.StopSourceFunc(func(
			context.Context, stageworkertransport.Identity, int64,
		) <-chan *velav1.StopStage {
			return nil
		}),
	})
	if err != nil {
		t.Fatalf("configure smoke Stage Worker control service: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for smoke Stage Worker control: %v", err)
	}
	server := grpc.NewServer(grpc.Creds(serverCredentials))
	velav1.RegisterStageWorkerControlServiceServer(server, service)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		if err := <-done; err != nil && err != grpc.ErrServerStopped {
			t.Errorf("serve smoke Stage Worker control: %v", err)
		}
	})
	return listener.Addr().String(), smokeControlFiles{
		clientCertificate: clientCertificate, clientPrivateKey: clientPrivateKey, ca: caPath,
	}, handler
}

func serveArtifactStoreSmoke(t *testing.T) (string, []byte) {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/vela-stage-artifacts" {
			t.Errorf("unexpected Artifact Store smoke request = %s %s", request.Method, request.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case request.URL.Query().Has("versioning"):
			_, _ = w.Write([]byte(`<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`))
		case request.URL.Query().Has("acl"):
			_, _ = w.Write([]byte(`<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Owner><ID>smoke-owner</ID></Owner><AccessControlList><Grant><Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"><ID>smoke-owner</ID></Grantee><Permission>FULL_CONTROL</Permission></Grant></AccessControlList></AccessControlPolicy>`))
		case request.URL.Query().Has("policy"):
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"Version":"2012-10-17","Statement":[]}`))
		default:
			http.Error(w, "missing bucket subresource", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
}

func writeSmokeSecret(t *testing.T, directory, name string, payload []byte) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write smoke secret %q: %v", name, err)
	}
	return path
}

func issueSmokeCA(t *testing.T) (*x509.Certificate, ed25519.PrivateKey, []byte) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate smoke CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Vela Stage Worker Smoke CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatalf("issue smoke CA: %v", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse smoke CA: %v", err)
	}
	return certificate, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func issueSmokeCertificate(
	t *testing.T,
	ca *x509.Certificate,
	caKey ed25519.PrivateKey,
	commonName string,
	dnsNames []string,
	identities []*url.URL,
	usage []x509.ExtKeyUsage,
) ([]byte, []byte) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate smoke certificate key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatalf("generate smoke certificate serial: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: commonName},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usage,
		DNSNames: dnsNames, URIs: identities,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, key.Public(), caKey)
	if err != nil {
		t.Fatalf("issue smoke certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal smoke certificate key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
}

var _ stageworkertransport.Handler = (*smokeControlHandler)(nil)
var _ velav1.ModelRuntimeServiceServer = (*smokeRuntimeProbe)(nil)
