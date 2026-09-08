package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/runtimechannel"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type journalEndpointControl struct {
	Identity              modelruntime.ExecutionJournalIdentity
	Command               modelruntime.JournalCommand
	Manifest              *modelruntime.LaunchManifest
	Startup               modelruntime.BackendLifecycleStatus
	Authority             []byte
	IdleConnections       int
	CloseIdle             bool
	Read                  bool
	InteractiveSupervisor bool
	SupervisorAction      string
}

type journalEndpointReport struct {
	Receipt             modelruntime.JournalMutationReceipt
	Error               string
	OwnerError          string
	SupervisorCompleted bool
	IdleReady           int
	ReadCompleted       bool
	SupervisorReady     bool
	Decision            velav1.ModelRuntimeCommandDecision
	StartCalls          int64
}

type journalEndpointChild struct {
	process *os.Process
	input   *json.Encoder
	reader  *os.File
	reports *json.Decoder
	owner   *RuntimeNamespaceOwner
}

func TestJournalEndpointProcessHelper(t *testing.T) {
	if os.Getenv("VELA_JOURNAL_ENDPOINT_HELPER") != "client" {
		t.Skip("journal endpoint subprocess helper")
	}
	if os.Geteuid() != 65532 || os.Getegid() != 65532 || os.Getpid() != 1 {
		t.Fatal("expected independent non-root namespace PID 1")
	}
	root := os.Getenv("VELA_JOURNAL_ENDPOINT_ROOT")
	for _, name := range []string{"execution-admission.json", "execution-admission.lock", "forged-state"} {
		file, err := os.OpenFile(filepath.Join(root, name), os.O_RDWR|os.O_CREATE, 0o600)
		if file != nil {
			_ = file.Close()
		}
		if !errors.Is(err, os.ErrPermission) {
			t.Fatalf("workload could access root journal %s: %v", name, err)
		}
	}
	socket := os.Getenv("VELA_JOURNAL_ENDPOINT_SOCKET")
	if reply, err := runtimechannel.Exchange(t.Context(), socket, []byte("enroll")); err != nil || string(reply) != "enrolled" {
		t.Fatalf("test enrollment: %q %v", reply, err)
	}
	output := os.NewFile(3, "journal-report")
	defer func() { _ = output.Close() }()
	decoder, encoder := json.NewDecoder(os.Stdin), json.NewEncoder(output)
	var idle []*net.UnixConn
	defer func() {
		for _, connection := range idle {
			_ = connection.Close()
		}
	}()
	for {
		var request journalEndpointControl
		if err := decoder.Decode(&request); errors.Is(err, io.EOF) {
			return
		} else if err != nil {
			t.Fatal(err)
		}
		if request.IdleConnections != 0 || request.CloseIdle {
			for _, connection := range idle {
				_ = connection.Close()
			}
			idle = nil
			for range request.IdleConnections {
				connection, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: socket, Net: "unixpacket"})
				if err != nil {
					t.Fatal(err)
				}
				idle = append(idle, connection)
				if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatal(err)
				}
				challenge := make([]byte, runtimechannel.ChallengeSize)
				if n, err := connection.Read(challenge); err != nil || n != len(challenge) {
					t.Fatalf("idle handshake: %d %v", n, err)
				}
			}
			if err := encoder.Encode(journalEndpointReport{IdleReady: len(idle)}); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if request.Read {
			_, err := (modelruntime.UnixRuntimeJournalTransport{Socket: socket, Identity: request.Identity}).Read(t.Context())
			report := journalEndpointReport{ReadCompleted: err == nil}
			if err != nil {
				report.Error = err.Error()
			}
			if err := encoder.Encode(report); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if request.Manifest != nil {
			journalEndpointRunSupervisor(t, socket, request, decoder, encoder)
			if err := encoder.Encode(journalEndpointReport{SupervisorCompleted: true}); err != nil {
				t.Fatal(err)
			}
			continue
		}
		receipt, err := modelruntime.ExchangeJournalCommand(t.Context(), socket, request.Identity, request.Command)
		report := journalEndpointReport{Receipt: receipt}
		if err != nil {
			report.Error = err.Error()
		}
		if err := encoder.Encode(report); err != nil {
			t.Fatal(err)
		}
	}
}

func journalEndpointStart(t *testing.T, listener *net.UnixListener, root string) *journalEndpointChild {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), binary, "-test.run=^TestJournalEndpointProcessHelper$", "-test.timeout=45s")
	command.Env = []string{"VELA_JOURNAL_ENDPOINT_HELPER=client", "VELA_JOURNAL_ENDPOINT_ROOT=" + root,
		"VELA_JOURNAL_ENDPOINT_SOCKET=" + listener.Addr().String()}
	command.SysProcAttr = &syscall.SysProcAttr{Cloneflags: unix.CLONE_NEWPID, Credential: &syscall.Credential{Uid: 65532, Gid: 65532}}
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	command.ExtraFiles = []*os.File{writer}
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	done := make(chan struct{})
	go func() { _ = command.Wait(); close(done) }()
	t.Cleanup(func() { _ = input.Close(); _ = command.Process.Kill(); <-done; _ = reader.Close() })
	connection := journalEndpointAccept(t, listener)
	defer func() { _ = connection.Close() }()
	caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = caller.Close() }()
	observed, err := caller.Inspect(t.Context())
	if err != nil || string(caller.Payload()) != "enroll" {
		t.Fatalf("enrollment identity: %+v %v", observed, err)
	}
	// CRI/native inventory is a fixture; process and PID namespace handles are
	// actual kernel observations. This does not establish Registry/Fleet approval.
	cri, _, observer := runtimeCallerObserverFixture(t, observed)
	owner, err := observer.RetainNamespaceOwner(t.Context(), cri.target, caller)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if err := errors.Join(caller.Reply(t.Context(), []byte("enrolled")), observer.Close()); err != nil {
		t.Fatal(err)
	}
	return &journalEndpointChild{process: command.Process, input: json.NewEncoder(input), reader: reader, reports: json.NewDecoder(reader), owner: owner}
}

func journalEndpointAccept(t *testing.T, listener *net.UnixListener) *net.UnixConn {
	t.Helper()
	if err := listener.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	connection, err := listener.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	return connection
}

func journalEndpointExchange(t *testing.T, endpoint *JournalEndpoint, listener *net.UnixListener, child *journalEndpointChild, identity modelruntime.ExecutionJournalIdentity, command modelruntime.JournalCommand, lost bool) journalEndpointReport {
	t.Helper()
	command.SchemaVersion = 1
	if err := child.input.Encode(journalEndpointControl{Identity: identity, Command: command}); err != nil {
		t.Fatal(err)
	}
	connection := journalEndpointAccept(t, listener)
	caller, err := ReceiveRuntimeCallerWithRequestLimit(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532}, modelruntime.MaximumJournalCommandBytes)
	if err != nil {
		t.Fatal(err)
	}
	reply, handleErr := endpoint.Handle(t.Context(), caller)
	if handleErr == nil && !lost {
		if err := caller.Reply(t.Context(), reply); err != nil {
			t.Fatal(err)
		}
	}
	_ = caller.Close()
	_ = connection.Close()
	if err := child.reader.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var report journalEndpointReport
	if err := child.reports.Decode(&report); err != nil {
		t.Fatal(err)
	}
	if handleErr != nil {
		report.OwnerError = handleErr.Error()
	}
	return report
}

type journalEndpointFixture struct {
	owner                    *modelruntime.ExecutionJournalOwner
	endpoint                 *JournalEndpoint
	listener                 *net.UnixListener
	runtime, worker, sibling *journalEndpointChild
	identity                 modelruntime.ExecutionJournalIdentity
	manifest                 modelruntime.LaunchManifest
	startup                  modelruntime.BackendLifecycleStatus
	authority, floor         []byte
}

func newJournalEndpointFixture(t *testing.T) journalEndpointFixture {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("requires root Node and independent non-root PID namespaces")
	}
	root, err := os.MkdirTemp("/run", "vela-journal-endpoint-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(root, "state")
	if err := os.Mkdir(state, 0o700); err != nil {
		t.Fatal(err)
	}
	fixture := runtimeLaunchFixture(t)
	now := time.Now().UTC()
	clock := func() time.Time { return now }
	keys := map[string][]byte{"journal-test": bytes.Repeat([]byte{73}, 32)}
	validator, err := stageauthority.NewValidator(keys, clock)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := fixture.launch.RuntimeBindings()
	if err != nil {
		t.Fatal(err)
	}
	for i := range routes {
		routes[i].ModelRuntimeEpoch = fixture.launch.Runtimes[i].ModelRuntimeEpochFloor
	}
	owner, err := modelruntime.OpenExecutionJournalOwner(modelruntime.ExecutionJournalOwnerConfig{Manifest: fixture.launch,
		Validator: validator, Routes: routes, State: modelruntime.ExecutionFloorStateConfig{Directory: state, Initialize: true}, Now: clock})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	startup, err := owner.RecordBackendStartupIntent(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	status, err := owner.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	identity := modelruntime.ExecutionJournalIdentity{JournalID: status.JournalID, Scope: status.Scope, Storage: status.Storage}
	socket := filepath.Join(root, "node.sock")
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socket, Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	if err := errors.Join(os.Chown(socket, 0, 65532), os.Chmod(socket, 0o660)); err != nil {
		t.Fatal(err)
	}
	runtime := journalEndpointStart(t, listener, state)
	worker := journalEndpointStart(t, listener, state)
	sibling := journalEndpointStart(t, listener, state)
	if endpoint, err := NewJournalEndpoint(t.Context(), owner, runtime.owner, runtime.owner); err == nil || endpoint != nil {
		t.Fatal("same original process acquired both roles")
	}
	endpoint, err := NewJournalEndpoint(t.Context(), owner, runtime.owner, worker.owner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	// Endpoint keeps independent original pidfds after enrollment is discarded.
	if err := errors.Join(runtime.owner.Close(), worker.owner.Close(), sibling.owner.Close()); err != nil {
		t.Fatal(err)
	}
	authority, floor := journalEndpointCommands(t, fixture.launch, routes[0], signer, now)
	return journalEndpointFixture{owner: owner, endpoint: endpoint, listener: listener, runtime: runtime, worker: worker, sibling: sibling, identity: identity, manifest: fixture.launch, startup: startup, authority: authority, floor: floor}
}

func TestJournalEndpoint(t *testing.T) {
	f := newJournalEndpointFixture(t)
	owner, endpoint, listener := f.owner, f.endpoint, f.listener
	runtime, worker, sibling := f.runtime, f.worker, f.sibling
	identity, authority, floor := f.identity, f.authority, f.floor

	journalEndpointDriveSupervisor(t, endpoint, listener, runtime, journalEndpointControl{Identity: identity, Manifest: &f.manifest, Startup: f.startup, Authority: authority})
	if status, err := owner.Status(t.Context()); err != nil || status.Highest != 1 || status.PendingExecutions != 0 {
		t.Fatalf("Supervisor did not persist sealed/drained execution: %+v %v", status, err)
	}
	admit := modelruntime.JournalCommand{Admit: &modelruntime.JournalAuthorityCommand{Authority: authority}}
	for _, child := range []*journalEndpointChild{worker, sibling} {
		if report := journalEndpointExchange(t, endpoint, listener, child, identity, admit, false); report.Error == "" {
			t.Fatal("wrong role or same-UID sibling admitted execution")
		}
	}
	if report := journalEndpointExchange(t, endpoint, listener, runtime, identity, admit, true); report.Error == "" {
		t.Fatal("lost reply falsely acknowledged")
	}
	replay := journalEndpointExchange(t, endpoint, listener, runtime, identity, admit, false)
	if replay.Error != "" || !replay.Receipt.Replayed || replay.Receipt.Highest != 1 {
		t.Fatalf("same original process failed durable retry: %+v", replay)
	}
	wrongIdentity := identity
	wrongIdentity.JournalID = uuid.New()
	if report := journalEndpointExchange(t, endpoint, listener, runtime, wrongIdentity, admit, false); report.Error == "" {
		t.Fatal("client trusted receipt for another journal")
	}
	floorCommand := modelruntime.JournalCommand{Floor: &modelruntime.JournalFloorCommand{Disposition: floor}}
	if report := journalEndpointExchange(t, endpoint, listener, runtime, identity, floorCommand, false); report.Error == "" {
		t.Fatal("Runtime obtained Worker floor role")
	}
	if err := runtime.process.Kill(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for runtimechannel.SameLiveProcess(int(endpoint.runtime.Fd()), int(endpoint.runtime.Fd())) == nil {
		if context.Cause(ctx) != nil {
			t.Fatal("Runtime did not exit")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if report := journalEndpointExchange(t, endpoint, listener, worker, identity, floorCommand, false); report.Error != "" || report.Receipt.Floor != 1 {
		t.Fatalf("Runtime exit prevented safe floor restriction: %+v", report)
	}
	checkpoint := modelruntime.JournalCommand{NonAdmission: &modelruntime.JournalAuthorityCommand{Authority: authority}}
	if report := journalEndpointExchange(t, endpoint, listener, worker, identity, checkpoint, false); report.Error == "" || report.OwnerError != ErrRuntimeNamespaceOwnerLost.Error() {
		t.Fatal("dead Runtime still authorized non-admission")
	}
	if err := endpoint.Close(); err != nil {
		t.Fatal(err)
	}
	if report := journalEndpointExchange(t, endpoint, listener, worker, identity, floorCommand, false); report.Error == "" {
		t.Fatal("closed endpoint retained authority")
	}
	t.Log("real root owner / non-root PID-1 roles; direct filesystem access denied; sibling rejected; lost reply replayed; exit and endpoint closure fenced")
}

func journalEndpointCommands(t *testing.T, manifest modelruntime.LaunchManifest, binding stageauthority.RuntimeBinding, signer *stageauthority.Signer, now time.Time) ([]byte, []byte) {
	t.Helper()
	identity, err := hex.DecodeString(manifest.Members[0].IdentityDigest)
	if err != nil {
		t.Fatal(err)
	}
	subset, err := hex.DecodeString(manifest.Members[0].DeviceSubsetDigest)
	if err != nil {
		t.Fatal(err)
	}
	digest := bytes.Repeat([]byte{11}, sha256.Size)
	a := &velav1.StageAuthority{SchemaVersion: 2, JobId: uuid.NewString(), AttemptId: uuid.NewString(), StageRunId: uuid.NewString(),
		StageAttemptId: uuid.NewString(), StageAllocationId: uuid.NewString(), StageLeaseId: uuid.NewString(), AttemptFence: 1, StageFence: 1, StageVersion: 1,
		WorkerInstanceId: binding.WorkerInstanceID, WorkerInstanceEpoch: binding.WorkerInstanceEpoch, DeviceSetDigest: binding.DeviceSetDigest,
		MembershipDigest: binding.MembershipDigest, ModelResidencyId: binding.ModelResidencyID, ModelRuntimeIdentity: binding.ModelRuntimeIdentity,
		StageProfileRevisionId: binding.StageProfileRevisionID, ModelRuntimeBarrierGeneration: 1, CapacityObservationSequence: 1,
		LeaseToken: digest, ExecutionNonce: digest, ExecutionSequence: 1,
		CapacityVector: map[string]int64{"slots": 1}, SigningKeyId: "journal-test", IssuedAt: timestamppb.New(now),
		ExpiresAt: timestamppb.New(now.Add(time.Minute)), MonotonicValidFor: durationpb.New(time.Minute),
		Members: []*velav1.StageAuthorityMemberEpoch{{WorkerMemberId: binding.WorkerMemberID, MemberEpoch: binding.WorkerMemberEpoch,
			ModelRuntimeEpoch: binding.ModelRuntimeEpoch, IdentityDigest: identity}}}
	for _, device := range binding.Devices {
		a.Devices = append(a.Devices, &velav1.StageAuthorityDeviceEpoch{DeviceId: device.ID, DeviceEpoch: device.Epoch})
	}
	specDigest, err := stageauthority.ExecutionSpecDigest(journalEndpointSpec())
	if err != nil {
		t.Fatal(err)
	}
	a.ExecutionSpecDigest = specDigest[:]
	a, err = signer.Sign(a)
	if err != nil {
		t.Fatal(err)
	}
	aDigest, err := stageauthority.Digest(a)
	if err != nil {
		t.Fatal(err)
	}
	d := &velav1.StageTerminalDisposition{SchemaVersion: 1, InputDisposition: velav1.StageInputDisposition_STAGE_INPUT_DISPOSITION_INPUTS_UNUSED,
		OriginalAuthorityDigest: aDigest[:], OrganizationId: uuid.NewString(), ProjectId: uuid.NewString(), JobId: a.JobId, AttemptId: a.AttemptId,
		StageRunId: a.StageRunId, StageAttemptId: a.StageAttemptId, StageAllocationId: a.StageAllocationId, StageLeaseId: a.StageLeaseId,
		TerminalState: velav1.StageTerminalState_STAGE_TERMINAL_STATE_FAILED, StageFence: 2, StageVersion: 2, WorkerInstanceId: a.WorkerInstanceId,
		WorkerInstanceEpoch: a.WorkerInstanceEpoch, WorkerMemberId: binding.WorkerMemberID, ControlSessionEpoch: 1,
		DeviceSetDigest: a.DeviceSetDigest, MembershipDigest: a.MembershipDigest, Devices: a.Devices, Cutoff: 1,
		ObservedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(time.Minute)), SigningKeyId: "journal-test",
		Allocations: []*velav1.StageTerminalAllocation{{StageAttemptId: a.StageAttemptId, StageAllocationId: a.StageAllocationId, StageLeaseId: a.StageLeaseId,
			ExecutionSequence: 1, ExecutionNonce: a.ExecutionNonce, ModelResidencyId: a.ModelResidencyId, ModelRuntimeIdentity: a.ModelRuntimeIdentity,
			StageProfileRevisionId: a.StageProfileRevisionId, BarrierGeneration: 1, Members: []*velav1.StageTerminalMember{{
				WorkerMemberId: binding.WorkerMemberID, MemberEpoch: binding.WorkerMemberEpoch, ModelRuntimeEpoch: binding.ModelRuntimeEpoch,
				IdentityDigest: identity, DeviceSubsetDigest: subset}}}}}
	d, err = signer.SignTerminalDisposition(d)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	floor, err := proto.MarshalOptions{Deterministic: true}.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return wire, floor
}

func journalEndpointSpec() *velav1.StageExecutionSpec {
	return &velav1.StageExecutionSpec{ParametersJson: []byte(`{}`), ExpectedOutputManifestJson: []byte(`{"kind":"LATENT"}`)}
}

type journalEndpointBackend struct {
	*modelruntime.FakeRuntime
	startCalls atomic.Int64
}

func (backend *journalEndpointBackend) Start(ctx context.Context, authority stageauthority.Verified) error {
	backend.startCalls.Add(1)
	return backend.FakeRuntime.Start(ctx, authority)
}

func journalEndpointRunSupervisor(t *testing.T, socket string, request journalEndpointControl, decoder *json.Decoder, encoder *json.Encoder) {
	t.Helper()
	validator, err := stageauthority.NewValidator(map[string][]byte{"journal-test": bytes.Repeat([]byte{73}, 32)}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	bindings, err := request.Manifest.RuntimeBindings()
	if err != nil {
		t.Fatal(err)
	}
	var services []*modelruntime.Service
	var backends []*journalEndpointBackend
	for i, binding := range bindings {
		backend := &journalEndpointBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime()}
		epoch := request.Manifest.Runtimes[i].ModelRuntimeEpochFloor
		service, err := modelruntime.NewService(modelruntime.Config{Binding: binding, EpochStore: modelruntime.EpochStoreFunc(func(stageauthority.RuntimeBinding) (int64, error) { return epoch, nil }),
			Validator: validator, Backend: backend, CancelTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		services = append(services, service)
		backends = append(backends, backend)
	}
	supervisor, err := modelruntime.NewSupervisorWithRemoteExecutionJournal(t.Context(), modelruntime.RemoteExecutionJournalConfig{
		Manifest: *request.Manifest, Validator: validator, Identity: request.Identity, Startup: request.Startup,
		Transport: modelruntime.UnixRuntimeJournalTransport{Socket: socket, Identity: request.Identity}, Timeout: 5 * time.Second}, services...)
	if err != nil {
		t.Fatal(err)
	}
	defer supervisor.Close()
	var authority velav1.StageAuthority
	if err := proto.Unmarshal(request.Authority, &authority); err != nil {
		t.Fatal(err)
	}
	prepared, err := supervisor.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: &authority, ExecutionSpec: journalEndpointSpec()})
	if err != nil || prepared.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("native remote Prepare: %v %v", prepared, err)
	}
	if request.InteractiveSupervisor {
		if err := encoder.Encode(journalEndpointReport{SupervisorReady: true}); err != nil {
			t.Fatal(err)
		}
		for {
			var control journalEndpointControl
			if err := decoder.Decode(&control); err != nil {
				t.Fatal(err)
			}
			if control.SupervisorAction == "finish" {
				break
			}
			if control.SupervisorAction != "start" {
				t.Fatalf("unknown Supervisor control: %q", control.SupervisorAction)
			}
			started, err := supervisor.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: &authority})
			report := journalEndpointReport{Decision: started.GetDecision(), Error: started.GetDetail(), StartCalls: backends[0].startCalls.Load()}
			if err != nil {
				report.Error = err.Error()
			}
			if err := encoder.Encode(report); err != nil {
				t.Fatal(err)
			}
		}
		if backends[0].startCalls.Load() != 1 {
			t.Fatal("interactive Supervisor did not start exactly once")
		}
	} else {
		started, err := supervisor.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: &authority})
		if err != nil || started.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
			t.Fatalf("native remote Start: %v %v", started, err)
		}
	}
	backends[0].MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/native.bin"}`))
	sealed, err := supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: &authority})
	if err != nil || sealed.GetReceipt() == nil {
		t.Fatalf("native remote seal/drain: %v %v", sealed, err)
	}
	if err := supervisor.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func journalEndpointDriveSupervisor(t *testing.T, endpoint *JournalEndpoint, listener *net.UnixListener, child *journalEndpointChild, request journalEndpointControl) {
	t.Helper()
	if err := child.reader.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := listener.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	done := make(chan journalEndpointReport, 1)
	go func() {
		var report journalEndpointReport
		if err := child.reports.Decode(&report); err != nil {
			report.Error = err.Error()
		}
		done <- report
		_ = listener.SetDeadline(time.Now())
	}()
	if err := child.input.Encode(request); err != nil {
		t.Fatal(err)
	}
	exchanges := 0
	for {
		connection, err := listener.AcceptUnix()
		if err != nil {
			break
		}
		caller, err := ReceiveRuntimeCallerWithRequestLimit(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532}, modelruntime.MaximumJournalCommandBytes)
		if err != nil {
			_ = connection.Close()
			t.Fatal(err)
		}
		reply, err := endpoint.Handle(t.Context(), caller)
		if err == nil {
			err = caller.Reply(t.Context(), reply)
		}
		_ = caller.Close()
		_ = connection.Close()
		if err != nil {
			t.Fatal(err)
		}
		exchanges++
	}
	report := <-done
	if report.Error != "" || !report.SupervisorCompleted || exchanges < 10 {
		t.Fatalf("native Supervisor drive: %+v exchanges=%d", report, exchanges)
	}
	t.Logf("actual Supervisor prepare/start/seal/drain across %d authenticated Node exchanges; Runtime opened no journal files", exchanges)
}
