//go:build integration

package integration_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
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
)

func assertUnsignedTerminalAllocationNonAdmission(t *testing.T, fixture stageSchedulerFixture, validator *stageauthority.Validator, original *velav1.StageAuthority, disposition *velav1.StageTerminalDisposition, allocationID string) {
	t.Helper()
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
	assertDatabaseTerminalScratchRetirement(t, supervisor, validator, binding, original, disposition, subset)
	supervisor.Close()
	recovered := open(false, member.GetModelRuntimeEpoch()+1)
	read, err := recovered.InspectTerminalNonAdmission(t.Context(), disposition, allocationID)
	if err != nil || read == nil || read.DispositionDigest != proof.DispositionDigest || read.StageAllocationID != allocationID || !read.ObservedAt.Equal(proof.ObservedAt) {
		t.Fatalf("database allocation checkpoint recovery: %+v %v", read, err)
	}
	t.Log("unsigned retry: PostgreSQL history -> authenticated Control disposition -> durable Runtime floor and non-admission -> next-epoch proof recovery")
}

func assertDatabaseTerminalScratchRetirement(t *testing.T, supervisor *modelruntime.Supervisor, validator *stageauthority.Validator, binding stageauthority.RuntimeBinding, original *velav1.StageAuthority, disposition *velav1.StageTerminalDisposition, subset []byte) {
	t.Helper()
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
	gate, err := stageworkeragent.NewFileAssignmentAdmission(stageworkeragent.AssignmentAdmissionConfig{
		Initialize: true, Directory: filepath.Join(base, "state"), InputRoot: filepath.Join(base, "inputs"), OutputRoot: filepath.Join(base, "outputs"),
		WorkerInstanceID: uuid.MustParse(binding.WorkerInstanceID), WorkerInstanceEpoch: binding.WorkerInstanceEpoch, WorkerMemberID: uuid.MustParse(binding.WorkerMemberID),
		Validator: validator, Bindings: []stageworkeragent.AdmissionRuntimeBinding{stageworkeragent.AdmissionRuntimeBinding(trusted)}, MaxRecords: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gate.Close() })
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
	result, err := retirer.Retire(t.Context(), disposition, map[string]*velav1.StageAuthority{original.StageAllocationId: original}, map[string]*velav1.ModelRuntimeIdentity{binding.WorkerMemberID: identities.Identities[0]})
	if err != nil || result.Phase != stageworkeragent.TerminalRetirementRetired {
		t.Fatalf("database-authorized scratch retirement: %+v %v", result, err)
	}
	for _, name := range paths {
		if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("authorized scratch remained: %s %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(base, "outputs", "unrelated", "keep")); err != nil {
		t.Fatal("retirement changed unrelated scratch")
	}
	t.Log("authenticated PostgreSQL terminal history -> Worker intent/floor -> UDS Runtime proof collection -> durable READY -> exact scratch retirement")
}
