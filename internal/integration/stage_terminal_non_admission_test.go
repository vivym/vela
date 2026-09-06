//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/modelruntimetransport"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/stageworkercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func assertUnsignedTerminalAllocationNonAdmission(t *testing.T, fixture stageSchedulerFixture, validator *stageauthority.Validator, assignment *velav1.StageAssignment, command stageworkercontrol.CommandContext, disposition *velav1.StageTerminalDisposition, allocationID string, replaceRuntime bool) {
	t.Helper()
	original := assignment.GetAuthority()
	var issued int
	if err := fixture.database.Admin.QueryRow(`SELECT count(*) FROM stage_assignment_authority_receipts AS receipt
		JOIN stage_leases AS lease ON lease.id = receipt.stage_lease_id WHERE lease.stage_allocation_id = $1`, allocationID).Scan(&issued); err != nil || issued != 0 {
		t.Fatalf("fixture must have no signed assignment for the retry: count=%d error=%v", issued, err)
	}
	member := original.GetMembers()[0]
	// Independently configure the Runtime from the existing assignment and database
	// registration. The terminal query is not its source of trusted topology.
	var subset []byte
	if err := fixture.database.Admin.QueryRow(`SELECT device_subset_digest FROM model_runtime_epoch_registrations
		WHERE model_residency_id = $1 AND barrier_generation = $2 AND worker_member_id = $3`,
		original.GetModelResidencyId(), original.GetModelRuntimeBarrierGeneration(), member.GetWorkerMemberId()).Scan(&subset); err != nil {
		t.Fatal(err)
	}
	binding := stageauthority.RuntimeBinding{
		WorkerInstanceID: original.GetWorkerInstanceId(), WorkerInstanceEpoch: original.GetWorkerInstanceEpoch(),
		WorkerMemberID: member.GetWorkerMemberId(), WorkerMemberEpoch: member.GetMemberEpoch(),
		DeviceSetDigest: bytes.Clone(original.GetDeviceSetDigest()), MembershipDigest: bytes.Clone(original.GetMembershipDigest()),
		ModelResidencyID: original.GetModelResidencyId(), ModelRuntimeIdentity: original.GetModelRuntimeIdentity(),
		StageProfileRevisionID: original.GetStageProfileRevisionId(),
	}
	for _, device := range original.GetDevices() {
		binding.Devices = append(binding.Devices, stageauthority.DeviceEpoch{ID: device.GetDeviceId(), Epoch: device.GetDeviceEpoch()})
	}
	for _, value := range original.GetMembers() {
		binding.Members = append(binding.Members, stageauthority.MemberEpoch{ID: value.GetWorkerMemberId(), Epoch: value.GetMemberEpoch()})
	}
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	open := func(initialize bool, epoch int64) *modelruntime.Supervisor {
		t.Helper()
		service, err := modelruntime.NewService(modelruntime.Config{
			Binding: binding, EpochStore: modelruntime.EpochStoreFunc(func(stageauthority.RuntimeBinding) (int64, error) { return epoch, nil }),
			Validator: validator, Backend: modelruntime.NewFakeDiTRuntime(), CancelTimeout: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(service.Close)
		supervisor, err := modelruntime.NewSupervisorWithExecutionFloor(modelruntime.ExecutionFloorConfig{
			Validator: validator, State: &modelruntime.ExecutionFloorStateConfig{Directory: directory, Initialize: initialize},
			Members: []modelruntime.ExecutionFloorMember{{WorkerMemberID: member.GetWorkerMemberId(), MemberEpoch: member.GetMemberEpoch(),
				IdentityDigest: bytes.Clone(member.GetIdentityDigest()), DeviceSubsetDigest: subset}},
		}, service)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(supervisor.Close)
		return supervisor
	}
	supervisor := open(true, member.GetModelRuntimeEpoch())
	if proof, err := supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, allocationID); err == nil || proof != nil {
		t.Fatalf("terminal SQL history alone proved exclusion: %+v %v", proof, err)
	}
	if floor, err := supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil || !floor.Durable {
		t.Fatalf("persist Runtime floor: %v", err)
	}
	proof, err := supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, allocationID)
	if err != nil || proof == nil || proof.ExecutionSequence != disposition.GetCutoff() || proof.StageAllocationID != allocationID ||
		proof.Contract != modelruntime.TerminalNonAdmissionContract || !proto.Equal(proof.Disposition, disposition) {
		t.Fatalf("unsigned database allocation was not checkpointed: %+v %v", proof, err)
	}
	epoch := member.GetModelRuntimeEpoch()
	if replaceRuntime {
		// Independent Fleet test setup approves the known CPU mock probe payloads.
		// Runtime registration still validates its actual UDS responses against this digest.
		var checks []stageworkeragent.ReadinessEvidenceCheck
		for _, check := range []velav1.ModelRuntimeReadinessCheck{
			velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_DEVICE,
			velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_BACKEND,
			velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP,
			velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_CANARY,
		} {
			evidence, err := json.Marshal(map[string]any{"component": "dit", "check": check.String(), "ready": true})
			if err != nil {
				t.Fatal(err)
			}
			checks = append(checks, stageworkeragent.ReadinessEvidenceCheck{
				Check: check.String(), Evidence: evidence, Detail: "resident runtime ready",
			})
		}
		evidence, err := stageworkeragent.EncodeReadinessEvidence(checks)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(evidence)
		if _, err := fixture.database.Admin.Exec(`UPDATE model_residencies SET canary_evidence_digest = $2 WHERE id = $1`, binding.ModelResidencyID, digest[:]); err != nil {
			t.Fatal(err)
		}
		// Only the original owner may establish missing old-epoch non-admission.
		// The replacement must recover these actual checkpoints from the same journal.
		for _, allocation := range disposition.GetAllocations() {
			if _, err := supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, allocation.GetStageAllocationId()); err != nil {
				t.Fatal(err)
			}
		}
		if err := supervisor.Shutdown(); err != nil {
			t.Fatal(err)
		}
		epoch++
		supervisor = open(false, epoch)
	}
	assertDatabaseTerminalScratchRetirement(t, fixture, supervisor, validator, binding, assignment, command, disposition, subset, replaceRuntime)
	supervisor.Close()
	recovered := open(false, epoch+1)
	read, err := recovered.InspectTerminalNonAdmission(t.Context(), disposition, allocationID)
	if err != nil || read == nil || read.DispositionDigest != proof.DispositionDigest || read.StageAllocationID != allocationID || !read.ObservedAt.Equal(proof.ObservedAt) {
		t.Fatalf("database allocation checkpoint recovery: %+v %v", read, err)
	}
	t.Log("unsigned retry: PostgreSQL history -> authenticated Control disposition -> durable Runtime floor and non-admission -> next-epoch proof recovery")
}

func assertDatabaseTerminalScratchRetirement(t *testing.T, fixture stageSchedulerFixture, supervisor *modelruntime.Supervisor, validator *stageauthority.Validator, binding stageauthority.RuntimeBinding, assignment *velav1.StageAssignment, command stageworkercontrol.CommandContext, disposition *velav1.StageTerminalDisposition, subset []byte, replaceRuntime bool) {
	t.Helper()
	original := assignment.GetAuthority()
	socketDirectory, err := os.MkdirTemp("/tmp", "vela-retirement-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDirectory) })
	socket := filepath.Join(socketDirectory, "runtime.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	velav1.RegisterModelRuntimeServiceServer(server, supervisor)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	client, err := modelruntimetransport.Dial(t.Context(), modelruntimetransport.Config{SocketPath: socket, ExpectedUID: uint32(os.Geteuid())})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	identities, err := client.DiscoverRuntimeIdentities(t.Context(), &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest{
		WorkerInstanceId: binding.WorkerInstanceID, WorkerInstanceEpoch: binding.WorkerInstanceEpoch,
		WorkerMemberId: binding.WorkerMemberID, WorkerMemberEpoch: binding.WorkerMemberEpoch,
	})
	if err != nil || len(identities.GetIdentities()) != 1 {
		t.Fatalf("discover Runtime: %v %v", identities, err)
	}
	member := original.GetMembers()[0]
	binding.ModelRuntimeEpoch = identities.Identities[0].GetModelRuntimeEpoch()
	if replaceRuntime && binding.ModelRuntimeEpoch != member.GetModelRuntimeEpoch()+1 {
		t.Fatal("recovery did not discover the replacement Runtime epoch")
	}
	trusted := stageworkeragent.ExecutionFloorBinding{Runtime: binding, IdentityDigest: [sha256.Size]byte(member.GetIdentityDigest()), DeviceSubsetDigest: [sha256.Size]byte(subset)}
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{{ID: binding.WorkerMemberID, Client: client}},
		ExecutionFloor: &stageworkeragent.ExecutionFloorConfig{
			Validator: validator, Bindings: []stageworkeragent.ExecutionFloorBinding{trusted},
			CurrentReaders: map[string]*velav1.ModelRuntimeIdentity{binding.WorkerMemberID: identities.Identities[0]},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	for _, name := range []string{"state", "inputs", "outputs"} {
		if err := os.Mkdir(filepath.Join(base, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// Persist the actual issued assignment at its issuance time, then reopen with
	// the expired-envelope validator used by the authenticated recovery transport.
	keys := map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)}
	bootstrapValidator, err := stageauthority.NewValidator(keys, func() time.Time { return original.GetIssuedAt().AsTime() })
	if err != nil {
		t.Fatal(err)
	}
	originalTrusted := stageworkeragent.AdmissionRuntimeBinding(trusted)
	originalTrusted.Runtime.ModelRuntimeEpoch = member.GetModelRuntimeEpoch()
	admissionConfig := stageworkeragent.AssignmentAdmissionConfig{
		Initialize: true, Directory: filepath.Join(base, "state"), InputRoot: filepath.Join(base, "inputs"), OutputRoot: filepath.Join(base, "outputs"),
		WorkerInstanceID: uuid.MustParse(binding.WorkerInstanceID), WorkerInstanceEpoch: binding.WorkerInstanceEpoch, WorkerMemberID: uuid.MustParse(binding.WorkerMemberID),
		Validator: bootstrapValidator, Bindings: []stageworkeragent.AdmissionRuntimeBinding{originalTrusted}, MaxRecords: 4,
	}
	gate, err := stageworkeragent.NewFileAssignmentAdmission(admissionConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gate.Close() })
	admitted, err := gate.Begin(t.Context(), assignment, command.CommandID)
	if err != nil {
		t.Fatal(err)
	}
	if err := admitted.CompleteInputs(t.Context()); err != nil {
		admitted.Release()
		t.Fatal(err)
	}
	admitted.Release()
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	admissionConfig.Initialize, admissionConfig.Validator = false, validator
	admissionConfig.Bindings = []stageworkeragent.AdmissionRuntimeBinding{stageworkeragent.AdmissionRuntimeBinding(trusted)}
	gate, err = stageworkeragent.NewFileAssignmentAdmission(admissionConfig)
	if err != nil {
		t.Fatal(err)
	}
	paths := []string{filepath.Join(base, "inputs", "stage-runs", disposition.StageRunId, "inputs", "payload")}
	for _, allocation := range disposition.Allocations {
		paths = append(paths, filepath.Join(base, "outputs", allocation.StageAttemptId, "output"))
	}
	for _, name := range append(paths, filepath.Join(base, "outputs", "unrelated", "keep")) {
		if err := os.MkdirAll(filepath.Dir(name), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, []byte("CPU scratch fixture"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	retirer, err := stageworkeragent.NewTerminalScratchRetirement(gate, runtimeAgent, stageworkeragent.AttemptOwnedFilesystemScratchV1)
	if err != nil {
		t.Fatal(err)
	}
	var assignments stageworkercontrol.AssignmentOperations = unusedMaterializationReplayDependencies{}
	if replaceRuntime {
		assignments = newPostgresAssignmentTestBackend(t, fixture)
	}
	handler, _, _ := terminalDispositionControlWithAssignments(t, fixture, assignments)
	productionState, err := stageworkeragent.NewFileProductionState(stageworkeragent.FileProductionStateConfig{
		Directory: filepath.Join(base, "production-state"), WorkerInstanceID: admissionConfig.WorkerInstanceID,
		WorkerInstanceEpoch: admissionConfig.WorkerInstanceEpoch, WorkerMemberID: admissionConfig.WorkerMemberID,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = productionState.Close() })
	control := terminalDispositionDialer(t, handler, productionState)(command.Identity.SPIFFEID, command.ControlSessionEpoch)
	journal, err := stageworkeragent.NewMemoryMaterializationJournal(4)
	if err != nil {
		t.Fatal(err)
	}
	source, err := stageartifact.NewFilesystemLocalOutputSource(admissionConfig.OutputRoot)
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := stageartifact.NewObjectStorePublisher(artifactstore.NewLocal(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	materializationValidator, err := materializationauthority.NewValidator(keys, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := stageworkeragent.NewDurableStreamAgent(stageworkeragent.DurableStreamConfig{
		Runtime: runtimeAgent, Control: control, Admission: gate, TerminalRetirement: retirer, TerminalHistory: control,
		Materialization: &stageworkeragent.MaterializationConfig{
			Validator: materializationValidator, Source: source, Publisher: publisher, Journal: journal,
			ScratchRetirer: stageworkeragent.RetainScratchRetirer{}, OutputOwnershipContract: stageworkeragent.AttemptOwnedFilesystemScratchV1,
			SourceLossEvidence: stageworkeragent.MaterializationSourceLossEvidenceFunc(func(context.Context, stageworkeragent.PendingMaterialization) (stageworkeragent.MaterializationSourceLossEvidence, error) {
				return stageworkeragent.MaterializationSourceLossEvidence{}, errors.New("terminal retirement must not report source loss")
			}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	readiness := &terminalRecoveryReadiness{}
	if replaceRuntime {
		readiness.runtime = client
	}
	readiness.beforeProbe = func() {
		state, err := gate.Snapshot(t.Context())
		if err != nil || len(state.Retirements) != 1 || state.Retirements[0].Phase != stageworkeragent.TerminalRetirementRetired {
			t.Fatalf("readiness preceded durable retirement: %+v %v", state, err)
		}
		capacity, session := terminalRecoveryCapacity(t, fixture, admissionConfig.WorkerInstanceID)
		if session != control.CurrentControlSessionEpoch() {
			t.Fatal("readiness preceded current Control session confirmation")
		}
		for _, quantity := range capacity {
			if quantity != 0 {
				t.Fatal("capacity reopened before replacement Runtime readiness")
			}
		}
	}
	waits := 0
	production, err := stageworkeragent.NewProductionAgent(stageworkeragent.ProductionConfig{
		Control: control, Runtime: readiness, Stream: stream, RuntimeIdentity: identities.Identities[0],
		Devices: original.Devices, Members: original.Members, CapacityVector: original.CapacityVector,
		CapacityTTL: time.Minute, HeartbeatInterval: time.Second, RetryMinimum: time.Millisecond, RetryMaximum: time.Second,
		ObservationSequenceSource: productionState, Now: time.Now,
		RetryObserver: func(operation string, err error) { t.Logf("recovery retry %s: %v", operation, err) },
		Wait: func(context.Context, time.Duration) error {
			waits++
			state, err := gate.Snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if len(state.Retirements) == 1 && state.Retirements[0].Phase == stageworkeragent.TerminalRetirementRetired || waits > 5 {
				cancel()
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := production.Run(ctx); err != nil {
		t.Fatal(err)
	}
	state, err := gate.Snapshot(t.Context())
	if err != nil || len(state.Retirements) != 1 || state.Retirements[0].Phase != stageworkeragent.TerminalRetirementRetired ||
		state.Floor != disposition.GetCutoff() || state.Latest.Phase != stageworkeragent.AssignmentClosed || state.Latest.AcquireCommandID != command.CommandID {
		t.Fatalf("automatic recovery lost durable proof or original Acquire: %+v %v", state, err)
	}
	capacity, session := terminalRecoveryCapacity(t, fixture, admissionConfig.WorkerInstanceID)
	wantProbes := 1
	if replaceRuntime {
		wantProbes = 4
	}
	if len(capacity) != len(original.CapacityVector) || session != control.CurrentControlSessionEpoch() || readiness.calls != wantProbes {
		t.Fatalf("recovery capacity/session/readiness: %v epoch=%d probes=%d", capacity, session, readiness.calls)
	}
	for resource, quantity := range capacity {
		want := int64(0)
		if replaceRuntime {
			want = original.CapacityVector[resource]
		}
		if quantity != want {
			t.Fatalf("recovery capacity %s: got %d, want %d", resource, quantity, want)
		}
	}
	if replaceRuntime {
		var registered int64
		if err := fixture.database.Admin.QueryRow(`SELECT max(local_model_runtime_epoch) FROM model_runtime_epoch_registrations
			WHERE model_residency_id = $1 AND worker_member_id = $2`, binding.ModelResidencyID, binding.WorkerMemberID).Scan(&registered); err != nil || registered != binding.ModelRuntimeEpoch {
			t.Fatalf("replacement readiness registration: epoch=%d error=%v", registered, err)
		}
		var allocations int
		if err := fixture.database.Admin.QueryRow(`SELECT count(*) FROM stage_allocations WHERE stage_run_id = $1`, original.GetStageRunId()).Scan(&allocations); err != nil || allocations != len(disposition.GetAllocations()) {
			t.Fatalf("recovery reassigned a terminal StageRun: allocations=%d error=%v", allocations, err)
		}
		var noWork int
		if err := fixture.database.Admin.QueryRow(`SELECT count(*) FROM stage_worker_acquire_intents AS intent
			JOIN stage_worker_acquire_results AS result USING (command_id)
			WHERE intent.worker_instance_id = $1 AND intent.control_session_epoch = $2 AND result.result_kind = 'NO_WORK'`,
			binding.WorkerInstanceID, session).Scan(&noWork); err != nil || noWork != 1 {
			t.Fatalf("recovery did not complete normal acquisition: no_work=%d error=%v", noWork, err)
		}
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.ResumeMaterializations(t.Context()); err != nil {
		t.Fatalf("RETIRED recovery still needs Control: %v", err)
	}
	for _, name := range paths {
		if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("authorized scratch remained: %s %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(base, "outputs", "unrelated", "keep")); err != nil {
		t.Fatal("retirement changed unrelated scratch")
	}
	t.Logf("replacement=%t: zero capacity/current PostgreSQL session -> automatic authenticated history -> UDS proof -> durable RETIRED -> readiness/capacity -> offline replay", replaceRuntime)
}

func terminalRecoveryCapacity(t *testing.T, fixture stageSchedulerFixture, workerID uuid.UUID) (map[string]int64, int64) {
	t.Helper()
	var vector []byte
	var session int64
	if err := fixture.database.Admin.QueryRow(`SELECT capacity_vector, stage_worker_control_session_epoch
		FROM capacity_observations WHERE worker_instance_id = $1 AND stage_worker_control_session_epoch IS NOT NULL
		ORDER BY observation_sequence DESC LIMIT 1`, workerID).Scan(&vector, &session); err != nil {
		t.Fatal(err)
	}
	var capacity map[string]int64
	if err := json.Unmarshal(vector, &capacity); err != nil || len(capacity) == 0 {
		t.Fatalf("recovery capacity: %s error=%v", vector, err)
	}
	return capacity, session
}

type terminalRecoveryReadiness struct {
	calls       int
	runtime     stageworkeragent.RuntimeReadinessClient
	beforeProbe func()
}

func (runtime *terminalRecoveryReadiness) ProbeReadiness(ctx context.Context, request *velav1.ModelRuntimeServiceProbeReadinessRequest, options ...grpc.CallOption) (*velav1.ModelRuntimeServiceProbeReadinessResponse, error) {
	runtime.calls++
	if runtime.beforeProbe != nil {
		runtime.beforeProbe()
	}
	if runtime.runtime != nil {
		return runtime.runtime.ProbeReadiness(ctx, request, options...)
	}
	return nil, errors.New("Runtime readiness remains unavailable during this recovery fixture")
}
