package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// This harness keeps actual Worker RPCs and Runtime admission in distinct
// non-root PID-1 processes. Only fault timing and fake backend completion are
// controlled by the parent; Node ownership uses retained kernel identities.
func journalEndpointServeSupervisor(t *testing.T, supervisor *modelruntime.Supervisor, backend *journalEndpointBackend, socket string, clock *journalEndpointClock, decoder *json.Decoder, encoder *json.Encoder) {
	t.Helper()
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	if err := os.Chmod(socket, 0o600); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	velav1.RegisterModelRuntimeServiceServer(server, supervisor)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() {
		server.Stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	journalBarrierReport(t, encoder, journalEndpointReport{SupervisorReady: true})
	for {
		var request journalEndpointControl
		if err := decoder.Decode(&request); err != nil {
			t.Fatal(err)
		}
		switch request.SupervisorAction {
		case "stats":
		case "stop":
			backend.FinishStop()
		case "expire":
			if clock == nil {
				t.Fatal("expiry requires controlled test clock")
			}
			clock.Advance(2 * time.Minute)
		case "output":
			backend.MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/native.bin"}`))
		case "close":
			if err := supervisor.Shutdown(); err != nil {
				t.Fatal(err)
			}
			return
		default:
			t.Fatalf("unknown served Supervisor control: %q", request.SupervisorAction)
		}
		journalBarrierReport(t, encoder, journalEndpointReport{PrepareCalls: backend.prepareCalls.Load(), StartCalls: backend.startCalls.Load(), CancelCalls: backend.cancelCalls.Load(), CancelReason: velav1.ModelRuntimeCancelReason(backend.cancelReason.Load())})
	}
}

type journalBarrierClient struct {
	velav1.ModelRuntimeServiceClient
	before func(string)
}

func (client *journalBarrierClient) PrepareStage(ctx context.Context, request *velav1.ModelRuntimeServicePrepareStageRequest, opts ...grpc.CallOption) (*velav1.ModelRuntimeServicePrepareStageResponse, error) {
	client.before("prepare")
	return client.ModelRuntimeServiceClient.PrepareStage(ctx, request, opts...)
}

func (client *journalBarrierClient) StartStage(ctx context.Context, request *velav1.ModelRuntimeServiceStartStageRequest, opts ...grpc.CallOption) (*velav1.ModelRuntimeServiceStartStageResponse, error) {
	client.before("start")
	return client.ModelRuntimeServiceClient.StartStage(ctx, request, opts...)
}

func journalBarrierReport(t *testing.T, encoder *json.Encoder, report journalEndpointReport) {
	t.Helper()
	if err := encoder.Encode(report); err != nil {
		t.Fatal(err)
	}
}

func journalEndpointRunWorker(t *testing.T, socket string, request journalEndpointControl, decoder *json.Decoder, encoder *json.Encoder) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	connection, err := modelruntimetransport.Dial(ctx, modelruntimetransport.Config{SocketPath: request.WorkerSocket, ExpectedUID: 65532})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	client, err := modelruntime.NewJournalWorkerClient(connection, modelruntime.UnixRuntimeJournalTransport{Socket: socket, Identity: request.Identity})
	if err != nil {
		t.Fatal(err)
	}
	var original velav1.StageAuthority
	if err := proto.Unmarshal(request.Authority, &original); err != nil {
		t.Fatal(err)
	}
	member := original.Members[0]
	discovery, err := client.DiscoverRuntimeIdentities(ctx, &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
		WorkerInstanceId: original.WorkerInstanceId, WorkerInstanceEpoch: original.WorkerInstanceEpoch,
		WorkerMemberId: member.WorkerMemberId, WorkerMemberEpoch: member.MemberEpoch})
	if err != nil || len(discovery.GetIdentities()) != 1 {
		t.Fatalf("Worker discovery: %v %v", discovery, err)
	}
	identity := discovery.Identities[0]
	validator, err := stageauthority.NewValidator(map[string][]byte{"journal-test": bytes.Repeat([]byte{73}, 32)}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	hooked := &journalBarrierClient{ModelRuntimeServiceClient: client, before: func(phase string) {
		if request.PauseBefore != phase {
			return
		}
		journalBarrierReport(t, encoder, journalEndpointReport{PausedBefore: phase})
		var resume journalEndpointControl
		if err := decoder.Decode(&resume); err != nil || resume.WorkerAction != "resume" {
			t.Fatalf("Worker barrier resume: %+v %v", resume, err)
		}
	}}
	agent, err := stageworkeragent.New(stageworkeragent.Config{Members: []stageworkeragent.RuntimeMember{{ID: member.WorkerMemberId, Client: hooked}}, CancellationTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for {
		var authority velav1.StageAuthority
		if err := proto.Unmarshal(request.Authority, &authority); err != nil {
			t.Fatal(err)
		}
		scope := &velav1.ModelRuntimeExecutionDrainScope{SchemaVersion: 1, Identity: identity, Authority: &authority}
		digest, err := stageauthority.Digest(&authority)
		if err != nil {
			t.Fatal(err)
		}
		report := journalEndpointReport{}
		switch request.WorkerAction {
		case "execute":
			result, err := agent.PrepareAndStart(ctx, &velav1.StageAssignment{Authority: &authority, ExecutionSpec: journalEndpointSpec(),
				RequiredWorkerMemberIds: []string{member.WorkerMemberId}, MemberStartTimeout: durationpb.New(10 * time.Second)})
			report.Barrier = &result
			if err != nil {
				report.Error = err.Error()
			}
		case "floor":
			var floor velav1.StageTerminalDisposition
			if err := proto.Unmarshal(request.Floor, &floor); err != nil {
				t.Fatal(err)
			}
			result, err := client.InstallStageExecutionFloor(ctx, &velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest{SchemaVersion: 1, Identity: identity, Disposition: &floor})
			if err != nil || !result.GetDurable() || result.GetInstalledCutoff() != 1 || result.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("Worker floor: %v %v", result, err)
			}
			report.Decision = result.Decision
		case "non-admission":
			result, err := client.CheckpointStageNonAdmission(ctx, &velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest{Scope: scope})
			if err != nil || modelruntimetransport.ValidateExecutionNonAdmissionResult(validator, scope, digest, result.GetResult()) != nil {
				t.Fatalf("Worker non-admission: %v %v", result, err)
			}
			report.Checkpoint = result.GetResult().GetCheckpoint() != nil
		case "drain":
			result, err := client.DrainStageExecution(ctx, &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope})
			if err != nil || modelruntimetransport.ValidateExecutionDrainResult(scope, digest, result.GetResult()) != nil {
				t.Fatalf("Worker drain: %v %v", result, err)
			}
			report.Checkpoint = result.GetResult().GetCheckpoint() != nil
		case "drain-unavailable":
			readCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			result, err := client.DrainStageExecution(readCtx, &velav1.ModelRuntimeServiceDrainStageExecutionRequest{Scope: scope})
			cancel()
			if err == nil || result.GetResult().GetCheckpoint() != nil {
				t.Fatalf("unavailable Node allowed drain: %v %v", result, err)
			}
			report.Error = err.Error()
		case "cancel":
			callCtx, cancel := context.WithTimeout(ctx, time.Second)
			result, err := client.CancelStage(callCtx, &velav1.ModelRuntimeServiceCancelStageRequest{Authority: &authority,
				Reason: velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP})
			cancel()
			if err != nil || !result.GetCancellationAcknowledged() {
				t.Fatalf("Worker could not cancel during journal outage: %v %v", result, err)
			}
			report.Decision = result.Decision
		case "seal":
			result, err := client.SealOutput(ctx, &velav1.ModelRuntimeServiceSealOutputRequest{Authority: &authority})
			if err != nil || result.GetReceipt() == nil {
				t.Fatalf("Worker seal: %v %v", result, err)
			}
			report.Decision = result.Decision
		case "close":
			journalBarrierReport(t, encoder, report)
			return
		default:
			t.Fatalf("unknown Worker action: %q", request.WorkerAction)
		}
		journalBarrierReport(t, encoder, report)
		if err := decoder.Decode(&request); err != nil {
			t.Fatal(err)
		}
	}
}

func TestJournalServerWorkerBarrierRecoversAfterOverload(t *testing.T) {
	for _, phase := range []string{"prepare", "start"} {
		t.Run(phase, func(t *testing.T) {
			f := newJournalEndpointFixture(t)
			server, done := startJournalTestServer(t, f, 30*time.Second)
			directory := filepath.Join(filepath.Dir(f.listener.Addr().String()), "workload")
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chown(directory, 65532, 65532); err != nil {
				t.Fatal(err)
			}
			socket := filepath.Join(directory, "runtime.sock")
			if ready := journalServerRequest(t, f.runtime, journalEndpointControl{Identity: f.identity, Manifest: &f.manifest, Startup: f.startup, RuntimeSocket: socket}); !ready.SupervisorReady {
				t.Fatalf("Runtime gRPC not ready: %+v", ready)
			}
			paused := journalServerRequest(t, f.worker, journalEndpointControl{Identity: f.identity, Authority: f.authority, WorkerSocket: socket, WorkerAction: "execute", PauseBefore: phase})
			if paused.PausedBefore != phase {
				t.Fatalf("Worker missed barrier pause: %+v", paused)
			}
			waitJournalServer(t, server, func(s JournalServerStats) bool { return s.InFlight == 0 })
			statePath := filepath.Join(filepath.Dir(directory), "state", "execution-admission.json")
			before, err := os.ReadFile(statePath)
			if err != nil {
				t.Fatal(err)
			}
			journalServerRequest(t, f.sibling, journalEndpointControl{IdleConnections: 2})
			failed := journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: "resume"})
			prepared, canceled := 0, 0
			if phase == "start" {
				prepared, canceled = 1, 1
			}
			if failed.Error == "" || failed.Barrier == nil || failed.Barrier.BarrierPassed || failed.Barrier.PreparedMembers != prepared ||
				failed.Barrier.StartedMembers != 0 || failed.Barrier.CancellationAcknowledgedMembers != canceled {
				t.Fatalf("Worker barrier falsely passed or cancellation changed: %+v", failed)
			}
			calls := journalServerRequest(t, f.runtime, journalEndpointControl{SupervisorAction: "stats"})
			// Cancellation also inspects admission state. Count overload as an
			// observed fault, without assuming one read per Worker operation.
			if calls.PrepareCalls != int64(prepared) || calls.StartCalls != 0 || calls.CancelCalls != int64(canceled) || server.Stats().Overloaded == 0 {
				t.Fatalf("overload backend calls: %+v server=%+v", calls, server.Stats())
			}
			after, err := os.ReadFile(statePath)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("barrier rejection changed durable history: %v", err)
			}
			journalServerRequest(t, f.sibling, journalEndpointControl{CloseIdle: true})
			waitJournalServer(t, server, func(s JournalServerStats) bool { return s.InFlight == 0 })
			journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: "floor", Authority: f.authority, Floor: f.floor})
			proofAction := "non-admission"
			if phase == "start" {
				proofAction = "drain"
				if report := journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: "drain", Authority: f.authority}); report.Checkpoint {
					t.Fatal("cancellation acknowledgement falsely proved writer drain")
				}
				journalServerRequest(t, f.runtime, journalEndpointControl{SupervisorAction: "stop"})
			}
			if report := journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: proofAction, Authority: f.authority}); !report.Checkpoint {
				t.Fatalf("old allocation remains unproven: %+v", report)
			}
			next := journalBarrierNextAuthority(t, f.authority)
			recovered := journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: "execute", Authority: next})
			if recovered.Error != "" || recovered.Barrier == nil || !recovered.Barrier.BarrierPassed || recovered.Barrier.PreparedMembers != 1 || recovered.Barrier.StartedMembers != 1 {
				t.Fatalf("same Worker/Runtime could not execute next authority: %+v", recovered)
			}
			calls = journalServerRequest(t, f.runtime, journalEndpointControl{SupervisorAction: "output"})
			if calls.PrepareCalls != int64(prepared+1) || calls.StartCalls != 1 || calls.CancelCalls != int64(canceled) {
				t.Fatalf("recovery repeated backend dispatch: %+v", calls)
			}
			journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: "seal", Authority: next})
			if report := journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: "drain", Authority: next}); !report.Checkpoint {
				t.Fatal("new execution seal omitted durable exact drain")
			}
			status, err := f.owner.Status(t.Context())
			if err != nil || status.Floor != 1 || status.Highest != 2 || status.PendingExecutions != 0 {
				t.Fatalf("root-owned final history: %+v %v", status, err)
			}
			journalServerRequest(t, f.worker, journalEndpointControl{WorkerAction: "close", Authority: next})
			if report := journalServerRequest(t, f.runtime, journalEndpointControl{SupervisorAction: "close"}); !report.SupervisorCompleted {
				t.Fatalf("Runtime did not close cleanly: %+v", report)
			}
			if err := server.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			assertJournalServerJoined(t, server, done)
			t.Logf("independent Worker barrier %s overload: prepared=%d started=0 canceled=%d; %s checkpoint; next sequence=2 started once and drained", phase, prepared, canceled, proofAction)
		})
	}
}

func journalBarrierNextAuthority(t *testing.T, wire []byte) []byte {
	t.Helper()
	var authority velav1.StageAuthority
	if err := proto.Unmarshal(wire, &authority); err != nil {
		t.Fatal(err)
	}
	authority.StageAttemptId, authority.StageAllocationId, authority.StageLeaseId = uuid.NewString(), uuid.NewString(), uuid.NewString()
	authority.ExecutionSequence = 2
	authority.ExecutionNonce = bytes.Repeat([]byte{42}, 32)
	authority.StageFence, authority.StageVersion = 3, 3
	signer, err := stageauthority.NewSigner(map[string][]byte{"journal-test": bytes.Repeat([]byte{73}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	next, err := signer.Sign(&authority)
	if err != nil {
		t.Fatal(err)
	}
	result, err := proto.MarshalOptions{Deterministic: true}.Marshal(next)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
