package modelruntime_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestExecutionFloorRPCPersistsWhileBackendIsBlockedAndReplays(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "prepare", 9, time.Time{})
	client := dialExecutionFloorServer(t, f.supervisor)
	identity := discoverExecutionFloorIdentity(t, client, f.bindings[0])
	finished := make(chan error, 1)
	go func() { finished <- runFloorOperation(f, "prepare") }()
	select {
	case <-f.backend.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Prepare did not enter backend")
	}
	disposition := f.disposition(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for range 2 {
		response, err := client.InstallExecutionFloor(ctx, f.validator, identity, disposition)
		if err != nil || !response.GetDurable() || response.GetInstalledCutoff() != disposition.GetCutoff() {
			t.Fatalf("floor RPC while Prepare blocked: %v %v", response, err)
		}
	}
	if state := readDurableExecutionState(t, directory); state.Floor != 11 || state.Highest != 10 {
		t.Fatalf("RPC success preceded persistence: %+v", state)
	}
	select {
	case err := <-finished:
		t.Fatalf("floor RPC unexpectedly drained backend: %v", err)
	default:
	}
	lower := proto.Clone(disposition).(*velav1.StageTerminalDisposition)
	lower.Allocations, lower.Cutoff = lower.Allocations[:1], 10
	lower, err := f.signer.SignTerminalDisposition(lower)
	if err != nil {
		t.Fatal(err)
	}
	if response, err := client.InstallExecutionFloor(ctx, f.validator, identity, lower); err != nil || response.GetInstalledCutoff() != 11 {
		t.Fatalf("lower request lost installed floor or request digest: %v %v", response, err)
	}
	f.backend.unblock()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	assertFloorCommandsRejected(t, f.supervisor, f.authorities[0])
	if err := f.supervisor.Shutdown(); err != nil {
		t.Fatal(err)
	}
	recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now())
	assertFloorCommandsRejected(t, recovered.supervisor, recovered.authority(t, 1, 11))
	assertRecoveryDrainBlocks(t, recovered, recovered.authority(t, 1, 12))
}

func TestExecutionFloorRPCRejectsUntrustedAndNonDurableInstallation(t *testing.T) {
	for _, mutation := range []string{"schema", "unknown", "identity unknown", "target member", "target epoch", "signature", "scope", "expired", "missing disposition", "non-durable"} {
		t.Run(mutation, func(t *testing.T) {
			f := durableExecutionFixture(t, privateExecutionStateDirectory(t), true, "", 9, time.Time{})
			if mutation == "non-durable" {
				f = newExecutionFloorFixture(t, "")
			}
			client := dialExecutionFloorServer(t, f.supervisor)
			request := &velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest{
				SchemaVersion: 1, Identity: discoverExecutionFloorIdentity(t, client, f.bindings[0]), Disposition: f.disposition(t),
			}
			switch mutation {
			case "schema":
				request.SchemaVersion++
			case "unknown":
				request.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
			case "identity unknown":
				request.Identity.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
			case "target member":
				request.Identity.WorkerMemberId = uuid.NewString()
			case "target epoch":
				request.Identity.ModelRuntimeEpoch++
			case "signature":
				request.Disposition.Signature[0] ^= 1
			case "scope":
				request.Disposition.Allocations[0].Members[0].ModelRuntimeEpoch++
				var err error
				request.Disposition, err = f.signer.SignTerminalDisposition(request.Disposition)
				if err != nil {
					t.Fatal(err)
				}
			case "expired":
				f.clock.Advance(2 * time.Minute)
			case "missing disposition":
				request.Disposition = nil
			}
			response, err := client.InstallStageExecutionFloor(context.Background(), request)
			if err != nil || response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
				response.GetDurable() || response.GetInstalledCutoff() != 0 || len(response.GetDispositionDigest()) != 0 {
				t.Fatalf("invalid installation acquired a receipt: %v %v", response, err)
			}
			prepareFloorRuntime(t, f.supervisor, f.authority(t, 0, 10))
		})
	}
}

func TestExecutionFloorRPCStateFailureReturnsNoAcknowledgement(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	client := dialExecutionFloorServer(t, f.supervisor)
	identity := discoverExecutionFloorIdentity(t, client, f.bindings[0])
	path := filepath.Join(directory, durableStateFileName)
	if err := os.Rename(path, path+".retained"); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		response, err := client.InstallExecutionFloor(context.Background(), f.validator, identity, f.disposition(t))
		if response != nil || status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("state failure returned acknowledgement: %v %v", response, err)
		}
	}
}

func TestExecutionFloorClientRejectsMismatchedAcknowledgement(t *testing.T) {
	f := durableExecutionFixture(t, privateExecutionStateDirectory(t), true, "", 9, time.Time{})
	identity := discoverExecutionFloorIdentity(t, dialExecutionFloorServer(t, f.supervisor), f.bindings[0])
	disposition := f.disposition(t)
	verified, err := f.validator.ValidateTerminalDispositionEnvelope(disposition)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"schema", "unknown", "identity unknown", "member", "runtime epoch", "digest", "cutoff", "durability", "decision", "nil", "canceled"} {
		t.Run(mutation, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stub := &executionFloorReplyClient{reply: func() *velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse {
				response := &velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse{
					SchemaVersion: 1, Identity: proto.Clone(identity).(*velav1.ModelRuntimeIdentity),
					Decision:          velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
					DispositionDigest: bytes.Clone(verified.Digest[:]), InstalledCutoff: disposition.GetCutoff(), Durable: true,
				}
				switch mutation {
				case "schema":
					response.SchemaVersion++
				case "unknown":
					response.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
				case "identity unknown":
					response.Identity.ProtoReflect().SetUnknown([]byte{0x78, 0x01})
				case "member":
					response.Identity.WorkerMemberId = uuid.NewString()
				case "runtime epoch":
					response.Identity.ModelRuntimeEpoch++
				case "digest":
					response.DispositionDigest[0] ^= 1
				case "cutoff":
					response.InstalledCutoff--
				case "durability":
					response.Durable = false
				case "decision":
					response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
				case "nil":
					return nil
				case "canceled":
					cancel()
				}
				return response
			}}
			client := &modelruntimetransport.Client{ModelRuntimeServiceClient: stub}
			if response, err := client.InstallExecutionFloor(ctx, f.validator, identity, disposition); response != nil || err == nil {
				t.Fatalf("unbound acknowledgement accepted: %v %v", response, err)
			}
		})
	}
	stub := &executionFloorReplyClient{reply: func() *velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse {
		t.Fatal("invalid target was sent to Runtime")
		return nil
	}}
	client := &modelruntimetransport.Client{ModelRuntimeServiceClient: stub}
	for _, member := range []bool{false, true} {
		changed := proto.Clone(identity).(*velav1.ModelRuntimeIdentity)
		if member {
			changed.WorkerMemberId = uuid.NewString()
		} else {
			changed.WorkerInstanceId = uuid.NewString()
		}
		if response, err := client.InstallExecutionFloor(context.Background(), f.validator, changed, disposition); response != nil || err == nil {
			t.Fatalf("mismatched target accepted: %v %v", response, err)
		}
	}
}

func TestExecutionFloorServerAssemblyRecoversAndPreservesMessageBounds(t *testing.T) {
	directory, socketRoot := privateExecutionStateDirectory(t), privateSocketRoot(t)
	f := &executionFloorFixture{clock: newManualClock(time.Date(2026, 9, 5, 8, 0, 0, 0, time.UTC))}
	f.signer, f.validator = runtimeAuthorityCrypto(t, f.clock)
	manifest := runtimeServerManifest(t.TempDir())
	manifest.Members[0].IdentityDigest = fmt.Sprintf("%x", bytes.Repeat([]byte{0x66}, 32))
	manifest.Members[0].DeviceSubsetDigest = fmt.Sprintf("%x", bytes.Repeat([]byte{0x67}, 32))
	var err error
	f.bindings, err = manifest.RuntimeBindings()
	if err != nil {
		t.Fatal(err)
	}
	for i := range f.bindings {
		f.bindings[i].ModelRuntimeEpoch = 9
	}
	// Build authority matching the manifest, including all trusted epochs/digests.
	f.authorities = make([]*velav1.StageAuthority, len(f.bindings))
	for i := range f.authorities {
		a := f.authority(t, i, int64(10+i))
		a.WorkerInstanceId, a.WorkerInstanceEpoch = manifest.WorkerInstanceID, manifest.WorkerInstanceEpoch
		a.DeviceSetDigest, a.MembershipDigest = bytes.Clone(f.bindings[i].DeviceSetDigest), bytes.Clone(f.bindings[i].MembershipDigest)
		a.Devices = []*velav1.StageAuthorityDeviceEpoch{{DeviceId: manifest.Devices[0].ID, DeviceEpoch: manifest.Devices[0].Epoch}}
		a.Members[0].WorkerMemberId, a.Members[0].MemberEpoch = manifest.WorkerMemberID, manifest.WorkerMemberEpoch
		f.authorities[i], err = f.signer.Sign(a)
		if err != nil {
			t.Fatal(err)
		}
	}
	config := modelruntime.RuntimeServerConfig{
		Manifest: manifest, Validator: f.validator, CancelTimeout: time.Second,
		EpochStore: modelruntime.EpochStoreFunc(func(stageauthority.RuntimeBinding) (int64, error) { return 9, nil }),
		SocketPath: filepath.Join(socketRoot, "runtime.sock"),
		ExecutionFloor: &modelruntime.ExecutionFloorConfig{
			State: &modelruntime.ExecutionFloorStateConfig{Directory: directory, Initialize: true},
		},
		BackendFactory: func(context.Context, modelruntime.LaunchRuntime, stageauthority.RuntimeBinding, modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
			return modelruntime.NewFakeEncoderRuntime(), nil
		},
	}
	server, err := modelruntime.StartRuntimeServer(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	client := dialExecutionFloorSocket(t, config.SocketPath)
	identity := discoverExecutionFloorIdentity(t, client, f.bindings[0])
	if _, err := client.InstallExecutionFloor(context.Background(), f.validator, identity, f.disposition(t)); err != nil {
		t.Fatal(err)
	}
	large := largeExecutionDisposition(t, f)
	response, err := client.InstallStageExecutionFloor(context.Background(), &velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest{
		SchemaVersion: 1, Identity: identity, Disposition: large,
	}, grpc.MaxCallSendMsgSize(4<<20))
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("large signed history did not reach scope verification: %v %v", response, err)
	}
	_, err = client.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{
		ExecutionSpec: &velav1.StageExecutionSpec{ParametersJson: bytes.Repeat([]byte{'x'}, (1<<20)+1)},
	}, grpc.MaxCallSendMsgSize(4<<20))
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("ordinary execution message limit widened: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	config.ExecutionFloor.State.Initialize = false
	server, err = modelruntime.StartRuntimeServer(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	client = dialExecutionFloorSocket(t, config.SocketPath)
	if response, err := client.InstallExecutionFloor(context.Background(), f.validator, identity, f.disposition(t)); err != nil || response.GetInstalledCutoff() != 11 {
		t.Fatalf("assembled server lost recovered floor: %v %v", response, err)
	}
	prepared, err := client.PrepareStage(context.Background(), &velav1.ModelRuntimeServicePrepareStageRequest{
		Authority: f.authorities[1], ExecutionSpec: runtimeExecutionSpec(),
	})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("restart reopened retired allocation: %v %v", prepared, err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	// A separately bootstrapped 64-member scope accepts the complete large history.
	config.Manifest.Members = nil
	config.ExecutionFloor.Members = nil
	for _, member := range large.Allocations[0].Members {
		config.Manifest.Members = append(config.Manifest.Members, modelruntime.LaunchMemberEpoch{
			ID: member.WorkerMemberId, Epoch: member.MemberEpoch,
			IdentityDigest: fmt.Sprintf("%x", member.IdentityDigest), DeviceSubsetDigest: fmt.Sprintf("%x", member.DeviceSubsetDigest),
		})
		config.ExecutionFloor.Members = append(config.ExecutionFloor.Members, modelruntime.ExecutionFloorMember{
			WorkerMemberID: member.WorkerMemberId, MemberEpoch: member.MemberEpoch,
			IdentityDigest: bytes.Clone(member.IdentityDigest), DeviceSubsetDigest: bytes.Clone(member.DeviceSubsetDigest),
		})
	}
	config.ExecutionFloor.State = &modelruntime.ExecutionFloorStateConfig{Directory: privateExecutionStateDirectory(t), Initialize: true}
	config.Manifest.WorkerRole, config.Manifest.SharedSlotException = "llm", ""
	config.Manifest.Runtimes = config.Manifest.Runtimes[:1]
	config.Manifest.Runtimes[0].Component = "LLM"
	for _, allocation := range large.Allocations {
		allocation.ModelResidencyId = config.Manifest.Runtimes[0].ModelResidencyID
		allocation.ModelRuntimeIdentity = config.Manifest.Runtimes[0].RuntimeIdentity
		allocation.StageProfileRevisionId = config.Manifest.Runtimes[0].StageProfileRevisionID
	}
	large, err = f.signer.SignTerminalDisposition(large)
	if err != nil {
		t.Fatal(err)
	}
	server, err = modelruntime.StartRuntimeServer(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	client = dialExecutionFloorSocket(t, config.SocketPath)
	identity = discoverExecutionFloorIdentity(t, client, f.bindings[0])
	if response, err := client.InstallExecutionFloor(context.Background(), f.validator, identity, large); err != nil || response.GetInstalledCutoff() != large.GetCutoff() {
		t.Fatalf("complete large signed history failed installation: %v %v", response, err)
	}
	if state := readDurableExecutionState(t, config.ExecutionFloor.State.Directory); state.Floor != large.GetCutoff() {
		t.Fatalf("large history was acknowledged before persistence: floor=%d", state.Floor)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	config.ExecutionFloor.State.Initialize = false
	factory := config.BackendFactory
	config.BackendFactory = func(ctx context.Context, runtime modelruntime.LaunchRuntime, binding stageauthority.RuntimeBinding, backendConfig modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
		// The conflict appears after target validation but before socket publication.
		if err := os.WriteFile(config.SocketPath, []byte("publication conflict"), 0o600); err != nil {
			return nil, err
		}
		return factory(ctx, runtime, binding, backendConfig)
	}
	if failed, err := modelruntime.StartRuntimeServer(context.Background(), config); failed != nil || err == nil {
		t.Fatalf("socket publication conflict started server: %v %v", failed, err)
	}
	if content, err := os.ReadFile(config.SocketPath); err != nil || string(content) != "publication conflict" {
		t.Fatalf("startup rollback removed another socket target owner: %q %v", content, err)
	}
	if err := os.Remove(config.SocketPath); err != nil {
		t.Fatal(err)
	}
	config.BackendFactory = factory
	server, err = modelruntime.StartRuntimeServer(context.Background(), config)
	if err != nil {
		t.Fatalf("failed server publication retained journal lock: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })
}

func largeExecutionDisposition(t *testing.T, f *executionFloorFixture) *velav1.StageTerminalDisposition {
	t.Helper()
	d := f.disposition(t)
	for len(d.Allocations) < 256 {
		a := proto.Clone(d.Allocations[0]).(*velav1.StageTerminalAllocation)
		a.StageAttemptId, a.StageAllocationId, a.StageLeaseId = uuid.NewString(), uuid.NewString(), uuid.NewString()
		a.ExecutionSequence = int64(10 + len(d.Allocations))
		d.Allocations = append(d.Allocations, a)
	}
	var members []*velav1.StageTerminalMember
	members = append(members, d.Allocations[0].Members[0])
	for len(members) < 64 {
		member := proto.Clone(members[0]).(*velav1.StageTerminalMember)
		member.WorkerMemberId = uuid.NewString()
		members = append(members, member)
	}
	for _, allocation := range d.Allocations {
		allocation.Members = members
	}
	d.Cutoff = d.Allocations[len(d.Allocations)-1].GetExecutionSequence()
	signed, err := f.signer.SignTerminalDisposition(d)
	if err != nil || proto.Size(signed) <= 1<<20 {
		t.Fatalf("large signed fixture: size=%d err=%v", proto.Size(signed), err)
	}
	return signed
}

type executionFloorReplyClient struct {
	velav1.ModelRuntimeServiceClient
	reply func() *velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse
}

func (client *executionFloorReplyClient) InstallStageExecutionFloor(context.Context, *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest, ...grpc.CallOption) (*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse, error) {
	return client.reply(), nil
}

func dialExecutionFloorServer(t *testing.T, supervisor *modelruntime.Supervisor) *modelruntimetransport.Client {
	t.Helper()
	path := filepath.Join(privateSocketRoot(t), "runtime.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(4 << 20))
	velav1.RegisterModelRuntimeServiceServer(server, supervisor)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		if err := <-done; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	})
	return dialExecutionFloorSocket(t, path)
}

func dialExecutionFloorSocket(t *testing.T, path string) *modelruntimetransport.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := modelruntimetransport.Dial(ctx, modelruntimetransport.Config{SocketPath: path, ExpectedUID: uint32(os.Geteuid())})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func discoverExecutionFloorIdentity(t *testing.T, client *modelruntimetransport.Client, binding stageauthority.RuntimeBinding) *velav1.ModelRuntimeIdentity {
	t.Helper()
	response, err := client.DiscoverRuntimeIdentities(context.Background(), &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
		WorkerInstanceId: binding.WorkerInstanceID, WorkerInstanceEpoch: binding.WorkerInstanceEpoch,
		WorkerMemberId: binding.WorkerMemberID, WorkerMemberEpoch: binding.WorkerMemberEpoch,
	})
	if err != nil || len(response.GetIdentities()) == 0 {
		t.Fatalf("discover floor identity: %v %v", response, err)
	}
	for _, identity := range response.GetIdentities() {
		if identity.GetModelResidencyId() == binding.ModelResidencyID && identity.GetRuntimeIdentity() == binding.ModelRuntimeIdentity &&
			identity.GetModelRuntimeEpoch() == binding.ModelRuntimeEpoch && identity.GetStageProfileRevisionId() == binding.StageProfileRevisionID {
			return identity
		}
	}
	t.Fatal("discovery omitted the requested resident Runtime binding")
	return nil
}
