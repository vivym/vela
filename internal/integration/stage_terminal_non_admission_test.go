//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
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

func assertUnsignedTerminalAllocationNonAdmission(t *testing.T, fixture stageSchedulerFixture, validator *stageauthority.Validator, assignment *velav1.StageAssignment, command stageworkercontrol.CommandContext, disposition *velav1.StageTerminalDisposition, allocationID string) {
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
	assertDatabaseTerminalScratchRetirement(t, fixture, supervisor, validator, binding, assignment, command, disposition, subset)
	supervisor.Close()
	recovered := open(false, member.GetModelRuntimeEpoch()+1)
	read, err := recovered.InspectTerminalNonAdmission(t.Context(), disposition, allocationID)
	if err != nil || read == nil || read.DispositionDigest != proof.DispositionDigest || read.StageAllocationID != allocationID || !read.ObservedAt.Equal(proof.ObservedAt) {
		t.Fatalf("database allocation checkpoint recovery: %+v %v", read, err)
	}
	t.Log("unsigned retry: PostgreSQL history -> authenticated Control disposition -> durable Runtime floor and non-admission -> next-epoch proof recovery")
}

func assertDatabaseTerminalScratchRetirement(t *testing.T, fixture stageSchedulerFixture, supervisor *modelruntime.Supervisor, validator *stageauthority.Validator, binding stageauthority.RuntimeBinding, assignment *velav1.StageAssignment, command stageworkercontrol.CommandContext, disposition *velav1.StageTerminalDisposition, subset []byte) {
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
	binding.ModelRuntimeEpoch = member.GetModelRuntimeEpoch()
	trusted := stageworkeragent.ExecutionFloorBinding{Runtime: binding, IdentityDigest: [sha256.Size]byte(member.GetIdentityDigest()), DeviceSubsetDigest: [sha256.Size]byte(subset)}
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members:        []stageworkeragent.RuntimeMember{{ID: binding.WorkerMemberID, Client: client}},
		ExecutionFloor: &stageworkeragent.ExecutionFloorConfig{Validator: validator, Bindings: []stageworkeragent.ExecutionFloorBinding{trusted}},
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
	admissionConfig := stageworkeragent.AssignmentAdmissionConfig{
		Initialize: true, Directory: filepath.Join(base, "state"), InputRoot: filepath.Join(base, "inputs"), OutputRoot: filepath.Join(base, "outputs"),
		WorkerInstanceID: uuid.MustParse(binding.WorkerInstanceID), WorkerInstanceEpoch: binding.WorkerInstanceEpoch, WorkerMemberID: uuid.MustParse(binding.WorkerMemberID),
		Validator: bootstrapValidator, Bindings: []stageworkeragent.AdmissionRuntimeBinding{stageworkeragent.AdmissionRuntimeBinding(trusted)}, MaxRecords: 4,
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
	handler, _, _ := terminalDispositionControl(t, fixture)
	control := terminalDispositionDialer(t, handler)(command.Identity.SPIFFEID, command.ControlSessionEpoch)
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
	result, err := stream.ResumeMaterializations(t.Context())
	if err != nil || result.TerminalRecordsRetired != 0 || result.Committed || result.SourceLostReported || result.L2Published {
		t.Fatalf("automatic database-authorized scratch retirement: %+v %v", result, err)
	}
	state, err := gate.Snapshot(t.Context())
	if err != nil || len(state.Retirements) != 1 || state.Retirements[0].Phase != stageworkeragent.TerminalRetirementRetired ||
		state.Floor != disposition.GetCutoff() || state.Latest.Phase != stageworkeragent.AssignmentClosed || state.Latest.AcquireCommandID != command.CommandID {
		t.Fatalf("automatic recovery lost durable proof or original Acquire: %+v %v", state, err)
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
	t.Log("persisted original Acquire -> automatic authenticated PostgreSQL history query -> Worker INTENT/floor -> UDS Runtime proof -> durable READY/RETIRED -> offline replay")
}
