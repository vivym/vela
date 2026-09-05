package stageworkeragent_test

import (
	"context"
	"net"
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
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestExecutionFloorCollectionRPCPersistsCompleteMemberHistoryWithoutDrain(t *testing.T) {
	f := newAssignmentFloorFixture(t)
	gate := f.open(t)
	base := t.TempDir()
	group := startFloorCollectorRuntimes(t, f, base, true, true)
	agent := f.agent(t)
	handle := beginAdmission(t, gate, f.assignment, f.acquireID)
	if err := handle.EnterRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	handle.Release()
	stream, err := stageworkeragent.NewDurableStreamAgent(stageworkeragent.DurableStreamConfig{
		Runtime: agent, Admission: gate, Control: &recordingStreamControl{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if started, err := agent.PrepareAndStart(t.Context(), f.assignment); err != nil || !started.BarrierPassed {
		t.Fatalf("start two-member fixture: %+v %v", started, err)
	}
	first, err := stream.InstallExecutionFloor(t.Context(), f.disposition)
	if err == nil || first.Input == nil || first.Runtimes.AllInstalled || len(first.Runtimes.Acknowledgements) != 1 {
		t.Fatalf("lost member response became complete floor: %+v %v", first, err)
	}
	// Even the member whose response was lost must retain its restriction.
	assertCollectorHistoryRejected(t, f, group.clients)
	if result, err := stream.InstallExecutionFloor(t.Context(), f.disposition); err != nil || result.Input.Cutoff != 7 || !result.Runtimes.AllInstalled || len(result.Runtimes.Acknowledgements) != 2 {
		t.Fatalf("durable all-member retry: %+v %v", result, err)
	}
	verified, err := f.admissionFixture.config.Validator.ValidateEnvelopeForReplay(f.assignment.Authority, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, backend := range group.activeBackends {
		state, err := backend.Status(t.Context(), verified)
		if err != nil || state.State != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING || backend.closed.Load() {
			t.Fatalf("floor installation stopped or unloaded execution: %+v %v", state, err)
		}
	}
	// Test process teardown is separate from Stage drain. Recover the same
	// synthetic topology with no execution entry and without initializing state.
	group.close()
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = f.open(t)
	if snapshot := admissionSnapshot(t, gate); snapshot.Floor != 7 || snapshot.Latest.Phase != stageworkeragent.AssignmentRuntimeEntered {
		t.Fatalf("Worker restart lost floor or runtime recovery intent: %+v", snapshot)
	}
	group = startFloorCollectorRuntimes(t, f, base, false, false)
	assertCollectorHistoryRejected(t, f, group.clients)
	stream, err = stageworkeragent.NewDurableStreamAgent(stageworkeragent.DurableStreamConfig{
		Runtime: f.agent(t), Admission: gate, Control: &recordingStreamControl{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := stream.InstallExecutionFloor(t.Context(), f.disposition); err != nil || result.Input.Cutoff != 7 || !result.Runtimes.AllInstalled {
		t.Fatalf("recovered all-member floor: %+v %v", result, err)
	}
	next := collectorHistoryAuthority(t, f, f.disposition.Allocations[1])
	next.ExecutionSequence = f.disposition.Cutoff + 1
	next.StageAttemptId, next.StageAllocationId, next.StageLeaseId = uuid.NewString(), uuid.NewString(), uuid.NewString()
	next, err = f.signer.Sign(next)
	if err != nil {
		t.Fatal(err)
	}
	for _, client := range group.clients {
		response, err := client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: next, ExecutionSpec: f.assignment.ExecutionSpec})
		if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED ||
			!strings.Contains(response.GetDetail(), modelruntime.ErrExecutionDrainUnproven.Error()) {
			t.Fatalf("recovered floor admitted new work without historical writer drain: %v %v", response, err)
		}
	}
}

func assertCollectorHistoryRejected(t *testing.T, f *floorCollectorFixture, clients []velav1.ModelRuntimeServiceClient) {
	t.Helper()
	for _, allocation := range f.disposition.Allocations {
		authority := collectorHistoryAuthority(t, f, allocation)
		for _, client := range clients {
			prepare, prepareErr := client.PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: authority, ExecutionSpec: f.assignment.ExecutionSpec})
			start, startErr := client.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: authority})
			if prepareErr != nil || startErr != nil || prepare.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE || start.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
				t.Fatalf("historical allocation reopened after floor: %v %v %v %v", prepare, prepareErr, start, startErr)
			}
		}
	}
}

func collectorHistoryAuthority(t *testing.T, f *floorCollectorFixture, allocation *velav1.StageTerminalAllocation) *velav1.StageAuthority {
	t.Helper()
	a := proto.Clone(f.assignment.Authority).(*velav1.StageAuthority)
	a.StageAttemptId, a.StageAllocationId, a.StageLeaseId = allocation.StageAttemptId, allocation.StageAllocationId, allocation.StageLeaseId
	a.ExecutionSequence, a.ModelRuntimeBarrierGeneration = allocation.ExecutionSequence, allocation.BarrierGeneration
	a.ModelResidencyId, a.StageProfileRevisionId, a.ModelRuntimeIdentity = allocation.ModelResidencyId, allocation.StageProfileRevisionId, allocation.ModelRuntimeIdentity
	for index, member := range allocation.Members {
		a.Members[index].ModelRuntimeEpoch = member.ModelRuntimeEpoch
	}
	a, err := f.signer.Sign(a)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

type floorCollectorRuntimes struct {
	clients        []velav1.ModelRuntimeServiceClient
	activeBackends []*floorCollectorBackend
	backends       map[string]map[string]*floorCollectorBackend
	close          func()
}

type floorCollectorBackend struct {
	*modelruntime.FakeRuntime
	closed atomic.Bool
}

func (backend *floorCollectorBackend) Close() error {
	backend.closed.Store(true)
	return nil
}

func startFloorCollectorRuntimes(t *testing.T, f *floorCollectorFixture, base string, initialize, loseResponse bool) *floorCollectorRuntimes {
	t.Helper()
	group := &floorCollectorRuntimes{backends: make(map[string]map[string]*floorCollectorBackend)}
	var closers []func()
	var once sync.Once
	group.close = func() {
		once.Do(func() {
			for _, closeRuntime := range closers {
				closeRuntime()
			}
		})
	}
	t.Cleanup(group.close)
	for index, member := range f.config.Members {
		group.backends[member.ID] = make(map[string]*floorCollectorBackend)
		directory := filepath.Join(base, member.ID)
		if initialize {
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatal(err)
			}
		}
		floor := modelruntime.ExecutionFloorConfig{
			Validator: f.admissionFixture.config.Validator,
			State:     &modelruntime.ExecutionFloorStateConfig{Directory: directory, Initialize: initialize},
		}
		for _, binding := range f.config.ExecutionFloor.Bindings[:len(f.config.Members)] {
			floor.Members = append(floor.Members, modelruntime.ExecutionFloorMember{
				WorkerMemberID: binding.Runtime.WorkerMemberID, MemberEpoch: binding.Runtime.WorkerMemberEpoch,
				IdentityDigest: binding.IdentityDigest[:], DeviceSubsetDigest: binding.DeviceSubsetDigest[:],
			})
		}
		var services []*modelruntime.Service
		for _, binding := range f.config.ExecutionFloor.Bindings {
			if binding.Runtime.WorkerMemberID != member.ID {
				continue
			}
			backend := &floorCollectorBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime()}
			group.backends[member.ID][binding.Runtime.ModelResidencyID] = backend
			if binding.Runtime.ModelResidencyID == f.assignment.Authority.ModelResidencyId {
				group.activeBackends = append(group.activeBackends, backend)
			}
			epoch := binding.Runtime.ModelRuntimeEpoch
			binding.Runtime.ModelRuntimeEpoch = 0
			service, err := modelruntime.NewService(modelruntime.Config{
				Binding: binding.Runtime, Validator: floor.Validator, Backend: backend, CancelTimeout: time.Second,
				EpochStore: modelruntime.EpochStoreFunc(func(stageauthority.RuntimeBinding) (int64, error) { return epoch, nil }),
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(service.Close)
			services = append(services, service)
		}
		supervisor, err := modelruntime.NewSupervisorWithExecutionFloor(floor, services...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(supervisor.Close)
		root, err := os.MkdirTemp("/tmp", "vela-collector-")
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
		var lost atomic.Bool
		server := grpc.NewServer(grpc.MaxRecvMsgSize(4<<20), grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			response, err := next(ctx, request)
			if err == nil && loseResponse && index == 1 && info.FullMethod == velav1.ModelRuntimeService_InstallStageExecutionFloor_FullMethodName && !lost.Swap(true) {
				return nil, status.Error(codes.Unavailable, "injected floor response loss after persistence")
			}
			return response, err
		}))
		velav1.RegisterModelRuntimeServiceServer(server, supervisor)
		done := make(chan error, 1)
		go func() { done <- server.Serve(listener) }()
		t.Cleanup(server.Stop)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		client, err := modelruntimetransport.Dial(ctx, modelruntimetransport.Config{SocketPath: path, ExpectedUID: uint32(os.Geteuid())})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		group.clients = append(group.clients, client)
		f.config.Members[index].Client = client
		closers = append(closers, func() {
			_ = client.Close()
			server.Stop()
			_ = listener.Close()
			<-done
			if err := supervisor.Shutdown(); err != nil {
				t.Error(err)
			}
		})
	}
	return group
}
