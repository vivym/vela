package stageworkermembertransport

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMemberFloorRecoveryTLSAndUnixUseCurrentJournalOwner(t *testing.T) {
	f, request, _ := newMemberFloorFixture(t)
	directory := privateMemberFloorDirectory(t)
	chain := startMemberFloorChain(t, f, request.Command.Disposition, directory, true, false)
	chain.close()
	// Old allocation routes are gone; the same member-wide journal persists.
	f.runtime.identity.ModelRuntimeEpoch++
	f.runtime.identity.ModelResidencyId, f.runtime.identity.StageProfileRevisionId = uuid.NewString(), uuid.NewString()
	f.runtime.identity.RuntimeIdentity = "replacement-runtime"
	request.Command.Identity = proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity)
	chain = startMemberFloorChain(t, f, request.Command.Disposition, directory, false, true)
	if response, err := chain.client.InstallStageExecutionFloor(t.Context(), request.Command); response != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("v1 accepted nonresident history: %v %v", response, err)
	}
	request.Command.SchemaVersion = 2
	if response, err := chain.client.InstallStageExecutionFloor(t.Context(), request.Command); response != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("v2 response loss was not exercised: %v %v", response, err)
	}
	response, err := chain.client.InstallStageExecutionFloor(t.Context(), request.Command)
	if err != nil || response.GetSchemaVersion() != 2 || !response.GetDurable() || response.GetInstalledCutoff() != 11 {
		t.Fatalf("v2 current-owner retry: %v %v", response, err)
	}
	nonleader := chain.dial(t, chain.followerCredentials)
	if response, err := nonleader.InstallStageExecutionFloor(t.Context(), request.Command); response != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("v2 bypassed mTLS leader authorization: %v %v", response, err)
	}
	chain.close()
	f.runtime.identity.ModelRuntimeEpoch++
	request.Command.Identity = proto.Clone(f.runtime.identity).(*velav1.ModelRuntimeIdentity)
	chain = startMemberFloorChain(t, f, request.Command.Disposition, directory, false, false)
	if response, err := chain.client.InstallStageExecutionFloor(t.Context(), request.Command); err != nil || response.GetSchemaVersion() != 2 || response.GetInstalledCutoff() != 11 {
		t.Fatalf("v2 floor did not survive another restart: %v %v", response, err)
	}
}

func TestMemberFloorTLSAndUnixJournalSurviveResponseLossAndRestart(t *testing.T) {
	f, request, _ := newMemberFloorFixture(t)
	directory := privateMemberFloorDirectory(t)
	chain := startMemberFloorChain(t, f, request.Command.Disposition, directory, true, true)
	prepared, err := chain.client.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority, ExecutionSpec: f.spec})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("pre-floor execution did not reach Runtime: %v %v", prepared, err)
	}
	if response, err := chain.client.InstallStageExecutionFloor(context.Background(), request.Command); response != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("response-loss injection failed: %v %v", response, err)
	}
	started, err := chain.client.StartStage(context.Background(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: f.authority})
	if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("lost acknowledgement reopened execution: %v %v", started, err)
	}
	for range 2 {
		response, err := chain.client.InstallStageExecutionFloor(context.Background(), request.Command)
		if err != nil || !response.GetDurable() || response.GetInstalledCutoff() != 11 {
			t.Fatalf("replay after lost acknowledgement: %v %v", response, err)
		}
	}
	canceled, err := chain.client.CancelStage(context.Background(), &velav1.ModelRuntimeServiceCancelStageRequest{
		Authority: f.authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	})
	if err != nil || !canceled.GetCancellationAcknowledged() {
		t.Fatalf("floor prevented exact stop signaling: %v %v", canceled, err)
	}
	if chain.backend.closed.Load() {
		t.Fatal("floor installation unloaded resident backend")
	}
	// A valid nonleader certificate can connect but cannot install a restriction.
	nonleader := chain.dial(t, chain.followerCredentials)
	if response, err := nonleader.InstallStageExecutionFloor(context.Background(), request.Command); response != nil || status.Code(err) != codes.PermissionDenied {
		t.Fatalf("nonleader mTLS peer installed a floor: %v %v", response, err)
	}
	chain.close()
	chain = startMemberFloorChain(t, f, request.Command.Disposition, directory, false, false)
	if response, err := chain.client.InstallStageExecutionFloor(context.Background(), request.Command); err != nil || response.GetInstalledCutoff() != 11 {
		t.Fatalf("member/runtime restart lost persisted floor: %v %v", response, err)
	}
	for _, sequence := range []int64{11, 12} {
		a := proto.Clone(f.authority).(*velav1.StageAuthority)
		a.ExecutionSequence = sequence
		a.StageAttemptId, a.StageAllocationId, a.StageLeaseId = uuid.NewString(), uuid.NewString(), uuid.NewString()
		a, err = f.signer.Sign(a)
		if err != nil {
			t.Fatal(err)
		}
		response, err := chain.client.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: a, ExecutionSpec: f.spec})
		want := velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		if sequence > 11 {
			want = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
			if !strings.Contains(response.GetDetail(), modelruntime.ErrExecutionDrainUnproven.Error()) {
				t.Fatalf("recovered member omitted unresolved writer drain: %v %v", response, err)
			}
		}
		if err != nil || response.GetDecision() != want {
			t.Fatalf("recovered admission sequence=%d: %v %v", sequence, response, err)
		}
	}
}

func TestMemberFloorTLSForwardsCompleteLargeHistory(t *testing.T) {
	f, request, _ := newMemberFloorFixture(t)
	d := request.Command.Disposition
	members := d.Allocations[0].Members
	for len(members) < 64 {
		member := proto.Clone(members[0]).(*velav1.StageTerminalMember)
		member.WorkerMemberId = fmt.Sprintf("f0000000-0000-0000-0000-%012d", len(members))
		members = append(members, member)
	}
	for len(d.Allocations) < 256 {
		allocation := proto.Clone(d.Allocations[0]).(*velav1.StageTerminalAllocation)
		allocation.StageAttemptId, allocation.StageAllocationId, allocation.StageLeaseId = uuid.NewString(), uuid.NewString(), uuid.NewString()
		allocation.ExecutionSequence = int64(10 + len(d.Allocations))
		d.Allocations = append(d.Allocations, allocation)
	}
	for _, allocation := range d.Allocations {
		allocation.Members = members
	}
	d.Cutoff = d.Allocations[len(d.Allocations)-1].ExecutionSequence
	var err error
	request.Command.Disposition, err = f.signer.SignTerminalDisposition(d)
	if err != nil || proto.Size(request.Command.Disposition) <= 1<<20 {
		t.Fatalf("large terminal history: size=%d err=%v", proto.Size(request.Command.Disposition), err)
	}
	directory := privateMemberFloorDirectory(t)
	chain := startMemberFloorChain(t, f, request.Command.Disposition, directory, true, false)
	if response, err := chain.client.InstallStageExecutionFloor(context.Background(), request.Command); err != nil || response.GetInstalledCutoff() != d.GetCutoff() {
		t.Fatalf("large history failed across TLS/UDS: %v %v", response, err)
	}
	chain.close()
	chain = startMemberFloorChain(t, f, request.Command.Disposition, directory, false, false)
	if response, err := chain.client.InstallStageExecutionFloor(context.Background(), request.Command); err != nil || response.GetInstalledCutoff() != d.GetCutoff() {
		t.Fatalf("large history failed after journal recovery: %v %v", response, err)
	}
}

func TestMemberCancellationAfterFloorAllowsOnlyInstalledExpiredAuthority(t *testing.T) {
	f, request, _ := newMemberFloorFixture(t)
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
	chain := startMemberFloorChain(t, f, request.Command.Disposition, directory, true, false)
	prepared, err := chain.client.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority, ExecutionSpec: f.spec})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("prepare cancellation fixture: %v %v", prepared, err)
	}
	if _, err := chain.client.InstallStageExecutionFloor(context.Background(), request.Command); err != nil {
		t.Fatal(err)
	}
	wall.Add(int64(10 * time.Minute))
	_, prepareErr := chain.client.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.authority, ExecutionSpec: f.spec})
	_, startErr := chain.client.StartStage(context.Background(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: f.authority})
	_, statusErr := chain.client.Status(context.Background(), &velav1.ModelRuntimeServiceStatusRequest{Authority: f.authority})
	for _, err := range []error{prepareErr, startErr, statusErr} {
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("execution or inspection accepted expired authority: %v", err)
		}
	}
	for _, mutation := range []string{"unseen renewal", "unseen allocation", "future", "runtime epoch", "signature"} {
		a := proto.Clone(f.authority).(*velav1.StageAuthority)
		switch mutation {
		case "unseen renewal":
			a.IssuedAt = timestamppb.New(a.IssuedAt.AsTime().Add(time.Second))
			a.ExpiresAt = timestamppb.New(a.ExpiresAt.AsTime().Add(time.Second))
		case "unseen allocation":
			a.StageAttemptId, a.StageAllocationId, a.StageLeaseId = uuid.NewString(), uuid.NewString(), uuid.NewString()
		case "future":
			a.IssuedAt = timestamppb.New(time.Unix(0, wall.Load()).Add(time.Hour))
			a.ExpiresAt = timestamppb.New(a.IssuedAt.AsTime().Add(a.MonotonicValidFor.AsDuration()))
		case "runtime epoch":
			a.Members[1].ModelRuntimeEpoch++
		}
		a, err = f.signer.Sign(a)
		if err != nil {
			t.Fatal(err)
		}
		if mutation == "signature" {
			a.Signature[0] ^= 1
		}
		response, err := chain.client.CancelStage(context.Background(), &velav1.ModelRuntimeServiceCancelStageRequest{
			Authority: a, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
		})
		if response.GetCancellationAcknowledged() || chain.backend.cancelCalls.Load() != 0 || err == nil && response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
			t.Fatalf("%s canceled or renewed installed execution: %v %v", mutation, response, err)
		}
	}
	response, err := chain.client.CancelStage(context.Background(), &velav1.ModelRuntimeServiceCancelStageRequest{
		Authority: f.authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	})
	if err != nil || !response.GetCancellationAcknowledged() || chain.backend.cancelCalls.Load() != 1 || chain.backend.closed.Load() {
		t.Fatalf("expired exact authority could not signal installed execution: %v %v calls=%d", response, err, chain.backend.cancelCalls.Load())
	}
	nonleader := chain.dial(t, chain.followerCredentials)
	if response, err := nonleader.CancelStage(context.Background(), &velav1.ModelRuntimeServiceCancelStageRequest{
		Authority: f.authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	}); response.GetCancellationAcknowledged() || status.Code(err) != codes.PermissionDenied || chain.backend.cancelCalls.Load() != 1 {
		t.Fatalf("historical cancellation bypassed leader authorization: %v %v", response, err)
	}
	chain.close()
	chain = startMemberFloorChain(t, f, request.Command.Disposition, directory, false, false)
	response, err = chain.client.CancelStage(context.Background(), &velav1.ModelRuntimeServiceCancelStageRequest{
		Authority: f.authority, Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	})
	if err != nil || response.GetCancellationAcknowledged() || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE || chain.backend.cancelCalls.Load() != 0 {
		t.Fatalf("empty replacement Runtime acknowledged unknown historical execution: %v %v", response, err)
	}
}

type memberFloorChain struct {
	client                   *Client
	supervisor               *modelruntime.Supervisor
	backend                  *memberFloorBackend
	address                  string
	f                        *serverFixture
	followerCredentials      credentials.TransportCredentials
	close                    func()
	dropDrainResponse        *atomic.Bool
	dropNonAdmissionResponse *atomic.Bool
}

type memberFloorBackend struct {
	*modelruntime.FakeRuntime
	closed          atomic.Bool
	cancelCalls     atomic.Int64
	failCancel      atomic.Bool
	prepareEntered  chan struct{}
	prepareCanceled chan struct{}
}

func (backend *memberFloorBackend) Prepare(ctx context.Context, verified stageauthority.Verified, spec *velav1.StageExecutionSpec) error {
	if err := backend.FakeRuntime.Prepare(ctx, verified, spec); err != nil {
		return err
	}
	if backend.prepareEntered != nil {
		close(backend.prepareEntered)
		<-ctx.Done()
		close(backend.prepareCanceled)
		return ctx.Err()
	}
	return nil
}

func (backend *memberFloorBackend) Cancel(ctx context.Context, verified stageauthority.Verified, reason velav1.ModelRuntimeCancelReason) error {
	backend.cancelCalls.Add(1)
	if backend.failCancel.Load() {
		return errors.New("injected CPU cancellation failure")
	}
	return backend.FakeRuntime.Cancel(ctx, verified, reason)
}

func (backend *memberFloorBackend) Close() error {
	backend.closed.Store(true)
	return nil
}

func startMemberFloorChain(t *testing.T, f *serverFixture, disposition *velav1.StageTerminalDisposition, directory string, initialize, loseResponse bool) *memberFloorChain {
	t.Helper()
	return startMemberFloorChainWithWorkerJournal(t, f, disposition, directory, initialize, loseResponse, nil)
}

func startMemberFloorChainWithWorkerJournal(t *testing.T, f *serverFixture, disposition *velav1.StageTerminalDisposition, directory string, initialize, loseResponse bool, workerJournal WorkerJournalBindingObserver) *memberFloorChain {
	t.Helper()
	identity := f.runtime.identity
	binding := stageauthority.RuntimeBinding{
		WorkerInstanceID: identity.WorkerInstanceId, WorkerInstanceEpoch: identity.WorkerInstanceEpoch,
		WorkerMemberID: identity.WorkerMemberId, WorkerMemberEpoch: identity.WorkerMemberEpoch,
		DeviceSetDigest: identity.DeviceSetDigest, MembershipDigest: identity.MembershipDigest,
		ModelResidencyID: identity.ModelResidencyId, ModelRuntimeIdentity: identity.RuntimeIdentity,
		StageProfileRevisionID: identity.StageProfileRevisionId,
	}
	for _, device := range disposition.Devices {
		binding.Devices = append(binding.Devices, stageauthority.DeviceEpoch{ID: device.DeviceId, Epoch: device.DeviceEpoch})
	}
	floor := modelruntime.ExecutionFloorConfig{Validator: f.server.validator, State: &modelruntime.ExecutionFloorStateConfig{Directory: directory, Initialize: initialize}}
	var memberBindings []MemberBinding
	for _, member := range disposition.Allocations[0].Members {
		binding.Members = append(binding.Members, stageauthority.MemberEpoch{ID: member.WorkerMemberId, Epoch: member.MemberEpoch})
		memberBindings = append(memberBindings, MemberBinding{ID: member.WorkerMemberId, Epoch: member.MemberEpoch, IdentityDigest: member.IdentityDigest})
		floor.Members = append(floor.Members, modelruntime.ExecutionFloorMember{
			WorkerMemberID: member.WorkerMemberId, MemberEpoch: member.MemberEpoch,
			IdentityDigest: member.IdentityDigest, DeviceSubsetDigest: member.DeviceSubsetDigest,
		})
	}
	backend := &memberFloorBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime()}
	service, err := modelruntime.NewService(modelruntime.Config{
		Binding: binding, Backend: backend, Validator: f.server.validator, CancelTimeout: time.Second,
		EpochStore: modelruntime.EpochStoreFunc(func(stageauthority.RuntimeBinding) (int64, error) { return identity.ModelRuntimeEpoch, nil }),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(service.Close)
	supervisor, err := modelruntime.NewSupervisorWithExecutionFloor(floor, service)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(supervisor.Close)
	root, err := os.MkdirTemp("/tmp", "vela-floor-uds-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	path := filepath.Join(root, "runtime.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	localServer := grpc.NewServer(grpc.MaxRecvMsgSize(4 << 20))
	velav1.RegisterModelRuntimeServiceServer(localServer, supervisor)
	localDone := make(chan error, 1)
	go func() { localDone <- localServer.Serve(listener) }()
	t.Cleanup(localServer.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	localClient, err := modelruntimetransport.Dial(ctx, modelruntimetransport.Config{SocketPath: path, ExpectedUID: uint32(os.Geteuid())})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = localClient.Close() })
	member, err := NewServer(ServerConfig{
		Authenticator: stageworkertransport.PeerAuthenticator{}, Validator: f.server.validator, Runtime: localClient,
		LocalIdentities: []*velav1.ModelRuntimeIdentity{identity}, Members: memberBindings,
		WorkerJournal: workerJournal,
	})
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, leaderTLS, followerTLS := memberFloorTLS(t, f)
	var lost atomic.Bool
	var dropDrainResponse atomic.Bool
	var dropNonAdmissionResponse atomic.Bool
	memberServer := grpc.NewServer(grpc.Creds(serverTLS), grpc.MaxRecvMsgSize(4<<20), grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		response, err := next(ctx, request)
		if err == nil && info.FullMethod == velav1.StageWorkerMemberService_DrainStageExecution_FullMethodName && dropDrainResponse.CompareAndSwap(true, false) {
			return nil, status.Error(codes.Unavailable, "injected lost durable drain response")
		}
		if err == nil && (info.FullMethod == velav1.StageWorkerMemberService_CheckpointStageNonAdmission_FullMethodName || info.FullMethod == velav1.StageWorkerMemberService_CheckpointStageTerminalNonAdmission_FullMethodName) && dropNonAdmissionResponse.CompareAndSwap(true, false) {
			return nil, status.Error(codes.Unavailable, "injected lost non-admission response")
		}
		if err == nil && loseResponse && info.FullMethod == velav1.StageWorkerMemberService_InstallStageExecutionFloor_FullMethodName && lost.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "injected lost floor acknowledgement")
		}
		return response, err
	}))
	velav1.RegisterStageWorkerMemberServiceServer(memberServer, member)
	memberListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	memberDone := make(chan error, 1)
	go func() { memberDone <- memberServer.Serve(memberListener) }()
	t.Cleanup(memberServer.Stop)
	chain := &memberFloorChain{backend: backend, supervisor: supervisor, address: memberListener.Addr().String(), f: f, followerCredentials: followerTLS, dropDrainResponse: &dropDrainResponse, dropNonAdmissionResponse: &dropNonAdmissionResponse}
	chain.client = chain.dial(t, leaderTLS)
	var once sync.Once
	chain.close = func() {
		once.Do(func() {
			_ = chain.client.Close()
			memberServer.Stop()
			_ = memberListener.Close()
			<-memberDone
			_ = localClient.Close()
			localServer.Stop()
			_ = listener.Close()
			<-localDone
			if err := supervisor.Shutdown(); err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(chain.close)
	return chain
}

func (chain *memberFloorChain) dial(t *testing.T, transport credentials.TransportCredentials) *Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Dial(ctx, ClientConfig{
		Address: chain.address, TargetWorkerMemberID: chain.f.local.ID, TargetIdentityDigest: chain.f.authority.Members[1].IdentityDigest,
		TransportCredentials: transport, FloorValidator: chain.f.server.validator,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func memberFloorTLS(t *testing.T, f *serverFixture) (credentials.TransportCredentials, credentials.TransportCredentials, credentials.TransportCredentials) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Vela floor test CA"}, IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	certificate := func(serial int64, spiffe string) tls.Certificate {
		_, leafKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		identity, err := url.Parse(spiffe)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "floor-member.test"}, DNSNames: []string{"floor-member.test"},
			URIs: []*url.URL{identity}, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
			NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
		}
		der, err := x509.CreateCertificate(rand.Reader, leaf, ca, leafKey.Public(), key)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: leafKey}
	}
	leader, follower := certificate(2, f.leaderSPIFFE), certificate(3, f.localSPIFFE)
	server := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{follower}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert})
	client := func(certificate tls.Certificate) credentials.TransportCredentials {
		return credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, RootCAs: roots, ServerName: "floor-member.test"})
	}
	return server, client(leader), client(follower)
}

func privateMemberFloorDirectory(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return directory
}
