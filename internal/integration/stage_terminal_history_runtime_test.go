//go:build integration

package integration_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/stageworkercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestStageTerminalHistoryPreservesHistoricalRuntimeScopesWhileDraining(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "terminal-history-runtime")
	ctx := context.Background()
	command := stageWorkerAcquireCommand(fixture)
	backend := newPostgresAssignmentTestBackend(t, fixture)
	first, err := backend.AcquireStage(ctx, command, stageWorkerAcquireRequest(fixture))
	if err != nil || first.Assignment == nil {
		t.Fatalf("first Acquire = %+v error=%v", first, err)
	}
	old := first.Assignment.GetAuthority()
	request := terminalHistoryRequest(t, command, old)
	if len(old.GetMembers()) != 1 {
		t.Fatalf("single-member fixture returned %d members", len(old.GetMembers()))
	}
	member := proto.Clone(old.GetMembers()[0]).(*velav1.StageAuthorityMemberEpoch)
	member.ModelRuntimeEpoch += 7
	evidenceBackend, err := stageworkercontrol.NewPostgresWorkerEvidenceBackend(newRolePool(
		t, fixture.database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password",
	))
	if err != nil {
		t.Fatal(err)
	}
	registrationCommand := command
	registrationCommand.CommandID = uuid.New()
	registered, err := evidenceBackend.RegisterWorkerEvidence(ctx, registrationCommand, &velav1.RegisterWorkerEvidenceRequest{
		RuntimeIdentity: &velav1.ModelRuntimeIdentity{
			WorkerInstanceId: old.GetWorkerInstanceId(), WorkerInstanceEpoch: old.GetWorkerInstanceEpoch(),
			DeviceSetDigest: old.GetDeviceSetDigest(), MembershipDigest: old.GetMembershipDigest(),
			ModelResidencyId: old.GetModelResidencyId(), RuntimeIdentity: old.GetModelRuntimeIdentity(),
			ModelRuntimeEpoch: member.GetModelRuntimeEpoch(), StageProfileRevisionId: old.GetStageProfileRevisionId(),
			WorkerMemberId: member.GetWorkerMemberId(), WorkerMemberEpoch: member.GetMemberEpoch(),
		},
		CapacityObservationSequence: fixture.observation.Sequence,
		Devices:                     old.GetDevices(),
		Members:                     []*velav1.StageAuthorityMemberEpoch{member},
		ReadinessEvidence:           []byte("stage-scheduler-runtime-ready"),
	})
	if err != nil || !registered.Ready ||
		registered.ModelRuntimeBarrierGeneration <= old.GetModelRuntimeBarrierGeneration() ||
		registered.ModelRuntimeBarrierGeneration == member.GetModelRuntimeEpoch() {
		t.Fatalf("advance local Runtime epoch with distinct barrier generation = %+v error=%v", registered, err)
	}
	var state string
	if err := fixture.database.Admin.QueryRow(`SELECT state::text FROM stage_runs WHERE id = $1`, fixture.stageRunID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "RETRY_WAIT" {
		t.Fatalf("Runtime restart left StageRun %s, want RETRY_WAIT", state)
	}
	deadline := time.Now().Add(5 * time.Second)
	for state == "RETRY_WAIT" && time.Now().Before(deadline) {
		if _, err := fixture.coordinator.Reconcile(ctx, 10); err != nil {
			t.Fatal(err)
		}
		if err := fixture.database.Admin.QueryRow(`SELECT state::text FROM stage_runs WHERE id = $1`, fixture.stageRunID).Scan(&state); err != nil {
			t.Fatal(err)
		}
		if state == "RETRY_WAIT" {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if state != "READY" {
		t.Fatalf("Runtime restart retry did not become READY: %s", state)
	}
	fixture.authority.ModelRuntimeEpoch = registered.ModelRuntimeBarrierGeneration
	retryCommand := stageWorkerAcquireCommand(fixture)
	retried, err := backend.AcquireStage(ctx, retryCommand, stageWorkerAcquireRequest(fixture))
	if err != nil || retried.Assignment == nil {
		t.Fatalf("retry Acquire = %+v error=%v", retried, err)
	}
	current := retried.Assignment.GetAuthority()
	if current.GetStageRunId() != old.GetStageRunId() || current.GetStageAttemptId() == old.GetStageAttemptId() ||
		current.GetModelRuntimeBarrierGeneration() != registered.ModelRuntimeBarrierGeneration ||
		current.GetMembers()[0].GetModelRuntimeEpoch() != member.GetModelRuntimeEpoch() ||
		current.GetExecutionSequence() <= old.GetExecutionSequence() {
		t.Fatal("retry did not bind the same StageRun to the new Runtime scope")
	}
	failTerminalHistoryAssignment(t, fixture, current)
	reader := newTerminalHistoryReaderForTest(t, fixture, old)
	assertHistory := func() {
		t.Helper()
		history, err := reader.Read(ctx, command, old, command.CommandID)
		if err != nil || history == nil || history.Cutoff != current.GetExecutionSequence() || len(history.Allocations) != 2 ||
			history.Allocations[0].Members[0].ModelRuntimeEpoch != old.GetMembers()[0].GetModelRuntimeEpoch() ||
			history.Allocations[1].Members[0].ModelRuntimeEpoch != member.GetModelRuntimeEpoch() {
			t.Fatalf("verified history did not retain both Runtime epochs: history=%v error=%v", history, err)
		}
		snapshot := readTerminalHistory(t, fixture, request)
		if !snapshot.Eligible || snapshot.TerminalState != "FAILED" ||
			snapshot.Cutoff != current.GetExecutionSequence() || len(snapshot.Allocations) != 2 {
			t.Fatalf("old authority lost historical Runtime scopes: %+v", snapshot)
		}
		for index, authority := range []*velav1.StageAuthority{old, current} {
			var allocation struct {
				StageAttemptID       string `json:"stage_attempt_id"`
				StageAllocationID    string `json:"stage_allocation_id"`
				StageLeaseID         string `json:"stage_lease_id"`
				Sequence             int64  `json:"execution_sequence"`
				ModelResidencyID     string `json:"model_residency_id"`
				ModelRuntimeIdentity string `json:"model_runtime_identity"`
				BarrierGeneration    int64  `json:"barrier_generation"`
				Members              []struct {
					ID                 string `json:"worker_member_id"`
					Epoch              int64  `json:"member_epoch"`
					LocalEpoch         int64  `json:"model_runtime_epoch"`
					IdentityDigest     string `json:"identity_digest"`
					DeviceSubsetDigest string `json:"device_subset_digest"`
				} `json:"members"`
			}
			if err := json.Unmarshal(snapshot.Allocations[index], &allocation); err != nil {
				t.Fatal(err)
			}
			if allocation.StageAttemptID != authority.GetStageAttemptId() ||
				allocation.StageAllocationID != authority.GetStageAllocationId() ||
				allocation.StageLeaseID != authority.GetStageLeaseId() || allocation.Sequence != authority.GetExecutionSequence() ||
				allocation.ModelResidencyID != authority.GetModelResidencyId() ||
				allocation.ModelRuntimeIdentity != authority.GetModelRuntimeIdentity() ||
				allocation.BarrierGeneration != authority.GetModelRuntimeBarrierGeneration() || len(allocation.Members) != 1 {
				t.Fatalf("historical allocation %d = %+v", index, allocation)
			}
			actualMember, expectedMember := allocation.Members[0], authority.GetMembers()[0]
			var subsetDigest string
			if err := fixture.database.Admin.QueryRow(`SELECT encode(device_subset_digest, 'hex')
				FROM model_runtime_epoch_registrations WHERE model_residency_id = $1
				AND barrier_generation = $2 AND worker_member_id = $3`, allocation.ModelResidencyID,
				allocation.BarrierGeneration, expectedMember.GetWorkerMemberId()).Scan(&subsetDigest); err != nil {
				t.Fatal(err)
			}
			if actualMember.ID != expectedMember.GetWorkerMemberId() || actualMember.Epoch != expectedMember.GetMemberEpoch() ||
				actualMember.LocalEpoch != expectedMember.GetModelRuntimeEpoch() ||
				actualMember.IdentityDigest != hex.EncodeToString(expectedMember.GetIdentityDigest()) ||
				actualMember.DeviceSubsetDigest != subsetDigest {
				t.Fatalf("historical member %d = %+v", index, actualMember)
			}
		}
		wire, err := hex.DecodeString(snapshot.AssignmentWire)
		if err != nil {
			t.Fatal(err)
		}
		var recorded velav1.StageAssignment
		if err := proto.Unmarshal(wire, &recorded); err != nil || !proto.Equal(recorded.GetAuthority(), old) {
			t.Fatalf("history substituted latest authority for the original: %v", err)
		}
	}
	assertHistory()
	registry, err := fleet.NewService(newRolePool(t, fixture.database.DSN, "vela_fleet_login", "vela-fleet-password"))
	if err != nil {
		t.Fatal(err)
	}
	drained, err := registry.Drain(ctx, fleet.WorkerInstanceDrainRequest{
		WorkerInstanceID: fixture.authority.WorkerInstanceID, ExpectedInstanceEpoch: fixture.authority.WorkerInstanceEpoch,
		Reason: "terminal history during runtime drain", RequestedBy: "integration/terminal-history",
	})
	if err != nil || drained.Lifecycle != fleet.WorkerInstanceDraining {
		t.Fatalf("drain terminal Worker = %+v error=%v", drained, err)
	}
	assertHistory()
	t.Logf("old/new barriers=%d/%d, local epochs=%d/%d, cutoff=%d; historical query survives DRAINING",
		old.GetModelRuntimeBarrierGeneration(), current.GetModelRuntimeBarrierGeneration(),
		old.GetMembers()[0].GetModelRuntimeEpoch(), current.GetMembers()[0].GetModelRuntimeEpoch(), current.GetExecutionSequence())
}
