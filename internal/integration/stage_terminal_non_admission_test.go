//go:build integration

package integration_test

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
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
	supervisor.Close()
	recovered := open(false, member.GetModelRuntimeEpoch()+1)
	read, err := recovered.InspectTerminalNonAdmission(t.Context(), disposition, allocationID)
	if err != nil || read == nil || read.DispositionDigest != proof.DispositionDigest || read.StageAllocationID != allocationID || !read.ObservedAt.Equal(proof.ObservedAt) {
		t.Fatalf("database allocation checkpoint recovery: %+v %v", read, err)
	}
	t.Log("unsigned retry: PostgreSQL history -> authenticated Control disposition -> durable Runtime floor and non-admission -> next-epoch proof recovery")
}
