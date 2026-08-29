//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/fleet"
)

const (
	workerRegistryProfileID = "49000000-0000-0000-0000-000000000022"
	workerRegistryPoolID    = "49200000-0000-0000-0000-000000000001"
	workerRegistryBundleID  = "49200000-0000-0000-0000-000000000002"
	workerRegistryNodeID    = "49200000-0000-0000-0000-000000000003"
	workerRegistryDeviceID  = "49200000-0000-0000-0000-000000000004"
	multiWorkerProfileID    = "49200000-0000-0000-0000-000000000100"
	multiStageProfileID     = "49200000-0000-0000-0000-000000000101"
	multiCapacityPoolID     = "49200000-0000-0000-0000-000000000102"
)

func TestWorkerRegistryRejectsSharedGPUAcrossWorkerInstances(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedWorkerRegistryPlan(t, database.Admin)
	fleetPool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")

	firstWorkerID := uuid.MustParse("49200000-0000-0000-0000-000000000010")
	secondWorkerID := uuid.MustParse("49200000-0000-0000-0000-000000000011")
	for _, workerID := range []uuid.UUID{firstWorkerID, secondWorkerID} {
		if _, err := database.Admin.Exec(`
			INSERT INTO worker_instances (
				id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
				lifecycle_state, reachability_state, instance_epoch,
				control_session_epoch, desired_member_count, desired_device_count
			) VALUES ($1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 1, 1)
		`, workerID, workerRegistryProfileID, workerRegistryPoolID, workerRegistryBundleID); err != nil {
			t.Fatalf("seed WorkerInstance %s: %v", workerID, err)
		}
	}

	firstEvidence := workerRegistryEvidence(firstWorkerID, 0x10)
	if _, err := fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		firstEvidence,
	); err != nil {
		t.Fatalf("observe first WorkerInstance: %v", err)
	}

	secondEvidence := workerRegistryEvidence(secondWorkerID, 0x11)
	_, err := fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		secondEvidence,
	)
	assertPostgresConstraint(t, err, "device_already_bound_to_worker_instance")
}

func TestWorkerRegistryReconnectPreservesRuntimeAndFenceInvalidatesAuthority(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedWorkerRegistryPlan(t, database.Admin)
	fleetPool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000020")
	seedWorkerInstance(t, database.Admin, workerID, workerRegistryProfileID, 1, 1)
	if _, err := fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		workerRegistryEvidence(workerID, 0x20),
	); err != nil {
		t.Fatalf("observe WorkerInstance before reconnect: %v", err)
	}

	var (
		deviceSetDigest  []byte
		membershipDigest []byte
		residencyID      uuid.UUID
		modelEpoch       int64
	)
	if err := database.Admin.QueryRow(`
		SELECT worker.device_set_digest, worker.membership_digest,
			residency.id, residency.model_runtime_epoch
		FROM worker_instances AS worker
		JOIN model_residencies AS residency ON residency.worker_instance_id = worker.id
		WHERE worker.id = $1 AND residency.state = 'READY'
	`, workerID).Scan(
		&deviceSetDigest, &membershipDigest, &residencyID, &modelEpoch,
	); err != nil {
		t.Fatalf("read WorkerInstance authority: %v", err)
	}
	assertWorkerInstanceAuthority(
		t, fleetPool, workerID, 1, deviceSetDigest, membershipDigest,
		residencyID, modelEpoch, true,
	)

	var instanceEpoch, controlSessionEpoch, reconnectModelEpoch int64
	if err := fleetPool.QueryRow(context.Background(), `
		SELECT instance_epoch, control_session_epoch, model_runtime_epoch
		FROM vela_reconnect_worker_instance($1, 1, 1, 'control-session-2', $2, $3)
	`, workerID, time.Now().UTC(),
		"worker-agent/h3-node-01").Scan(
		&instanceEpoch, &controlSessionEpoch, &reconnectModelEpoch,
	); err != nil {
		t.Fatalf("reconnect WorkerInstance Agent: %v", err)
	}
	if instanceEpoch != 1 || controlSessionEpoch != 2 || reconnectModelEpoch != modelEpoch {
		t.Fatalf(
			"reconnect epochs = instance %d control %d model %d, want 1/2/%d",
			instanceEpoch, controlSessionEpoch, reconnectModelEpoch, modelEpoch,
		)
	}
	assertWorkerInstanceAuthority(
		t, fleetPool, workerID, 1, deviceSetDigest, membershipDigest,
		residencyID, modelEpoch, true,
	)

	var fencedEpoch int64
	var lifecycle string
	if err := fleetPool.QueryRow(context.Background(), `
		SELECT instance_epoch, lifecycle_state::text
		FROM vela_fence_worker_instance($1, 1, 'device epoch changed', $2)
	`, workerID, "node-agent/h3-node-01").Scan(&fencedEpoch, &lifecycle); err != nil {
		t.Fatalf("fence WorkerInstance: %v", err)
	}
	if fencedEpoch != 2 || lifecycle != "FENCED" {
		t.Fatalf("fenced WorkerInstance = epoch %d lifecycle %q, want 2/FENCED", fencedEpoch, lifecycle)
	}
	assertWorkerInstanceAuthority(
		t, fleetPool, workerID, 1, deviceSetDigest, membershipDigest,
		residencyID, modelEpoch, false,
	)
	var bindings int
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM active_device_bindings WHERE worker_instance_id = $1
	`, workerID).Scan(&bindings); err != nil || bindings != 0 {
		t.Fatalf("active bindings after fence = %d error=%v, want zero", bindings, err)
	}
	var residencyState string
	if err := database.Admin.QueryRow(`
		SELECT state::text FROM model_residencies WHERE id = $1
	`, residencyID).Scan(&residencyState); err != nil || residencyState != "DRAINING" {
		t.Fatalf(
			"ModelResidency after fence = %q error=%v, want DRAINING not RELEASED",
			residencyState, err,
		)
	}
}

func TestWorkerRegistryReleaseRequiresApprovalAndBreakEvenEvidence(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedWorkerRegistryPlan(t, database.Admin)
	fleetPool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	schedulerPool := newRolePool(
		t, database.DSN, "vela_scheduler_login", "vela-scheduler-password",
	)
	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000030")
	seedWorkerInstance(t, database.Admin, workerID, workerRegistryProfileID, 1, 1)
	if _, err := fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		workerRegistryEvidence(workerID, 0x30),
	); err != nil {
		t.Fatalf("observe WorkerInstance before release: %v", err)
	}
	var residencyID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id FROM model_residencies
		WHERE worker_instance_id = $1 AND state = 'READY'
	`, workerID).Scan(&residencyID); err != nil {
		t.Fatalf("read ModelResidency before release: %v", err)
	}

	if _, err := schedulerPool.Exec(context.Background(), `
		UPDATE model_residencies SET state = 'RELEASED' WHERE id = $1
	`, residencyID); !isPermissionDenied(err) {
		t.Fatalf("Scheduler direct ModelResidency mutation error = %v, want permission denied", err)
	}
	if _, err := fleetPool.Exec(context.Background(), `
		SELECT * FROM vela_begin_worker_instance_drain($1, 1, 'approved capacity change', $2)
	`, workerID, "fleet/operator-1"); err != nil {
		t.Fatalf("begin WorkerInstance drain: %v", err)
	}

	operationID := uuid.MustParse("49200000-0000-0000-0000-000000000031")
	_, err := fleetPool.Exec(context.Background(), `
		SELECT * FROM vela_approve_model_residency_release(
			$1, $2, 1, 'CAPACITY_CHANGE', 'residency-plan-v2', 'fleet/operator-1',
			60, 3600, decode(repeat('61', 32), 'hex')
		)
	`, operationID, residencyID)
	assertPostgresConstraint(t, err, "model_residency_release_below_break_even")

	if _, err := schedulerPool.Exec(context.Background(), `
		SELECT * FROM vela_approve_model_residency_release(
			$1, $2, 1, 'CAPACITY_CHANGE', 'residency-plan-v2', 'scheduler',
			7200, 3600, decode(repeat('62', 32), 'hex')
		)
	`, operationID, residencyID); !isPermissionDenied(err) {
		t.Fatalf("Scheduler release approval error = %v, want permission denied", err)
	}

	var state string
	if err := fleetPool.QueryRow(context.Background(), `
		SELECT state::text FROM vela_approve_model_residency_release(
			$1, $2, 1, 'CAPACITY_CHANGE', 'residency-plan-v2', 'fleet/operator-1',
			7200, 3600, decode(repeat('63', 32), 'hex')
		)
	`, operationID, residencyID).Scan(&state); err != nil {
		t.Fatalf("approve ModelResidency release: %v", err)
	}
	if state != "APPROVED" {
		t.Fatalf("release approval state = %q, want APPROVED", state)
	}
	if err := database.Admin.QueryRow(`
		SELECT state::text FROM model_residencies WHERE id = $1
	`, residencyID).Scan(&state); err != nil || state != "READY" {
		t.Fatalf("residency after approval = %q error=%v, want READY", state, err)
	}

	if err := fleetPool.QueryRow(context.Background(), `
		SELECT state::text
		FROM vela_complete_model_residency_release(
			$1, 1, decode(repeat('64', 32), 'hex'), 'node-agent/h3-node-01'
		)
	`, operationID).Scan(&state); err != nil {
		t.Fatalf("complete ModelResidency release: %v", err)
	}
	if state != "COMPLETED" {
		t.Fatalf("release completion state = %q, want COMPLETED", state)
	}
	var releasedAt sql.NullTime
	if err := database.Admin.QueryRow(`
		SELECT state::text, released_at FROM model_residencies WHERE id = $1
	`, residencyID).Scan(&state, &releasedAt); err != nil || state != "RELEASED" || !releasedAt.Valid {
		t.Fatalf(
			"completed residency = state %q released_at %v error=%v",
			state, releasedAt, err,
		)
	}
}

func TestWorkerRegistryRequiresCompleteMultiMemberIdentity(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedWorkerRegistryPlan(t, database.Admin)
	seedMultiMemberProfile(t, database.Admin)
	fleetPool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000103")
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_instances (
			id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
			lifecycle_state, reachability_state, instance_epoch,
			control_session_epoch, desired_member_count, desired_device_count
		) VALUES (
			$1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 2, 4
		)
	`, workerID, multiWorkerProfileID, multiCapacityPoolID,
		workerRegistryBundleID); err != nil {
		t.Fatalf("seed multi-member WorkerInstance: %v", err)
	}

	_, err := fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		multiMemberWorkerEvidence(workerID, false),
	)
	assertPostgresConstraint(t, err, "worker_instance_membership_incomplete")

	if _, err := fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		multiMemberWorkerEvidence(workerID, true),
	); err != nil {
		t.Fatalf("observe complete multi-member WorkerInstance: %v", err)
	}
	var memberCount, bindingCount int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM worker_members
			 WHERE worker_instance_id = $1 AND readiness = 'READY'),
			(SELECT count(*) FROM active_device_bindings
			 WHERE worker_instance_id = $1)
	`, workerID).Scan(&memberCount, &bindingCount); err != nil {
		t.Fatalf("read multi-member registry coverage: %v", err)
	}
	if memberCount != 2 || bindingCount != 4 {
		t.Fatalf(
			"multi-member coverage = %d members/%d bindings, want 2/4",
			memberCount, bindingCount,
		)
	}

	if _, err := fleetPool.Exec(context.Background(), `
		SELECT * FROM vela_fence_worker_instance($1, 1, 'member-1 epoch changed', $2)
	`, workerID, "node-agent/llm-node-a"); err != nil {
		t.Fatalf("fence multi-member WorkerInstance: %v", err)
	}
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM active_device_bindings WHERE worker_instance_id = $1
	`, workerID).Scan(&bindingCount); err != nil || bindingCount != 0 {
		t.Fatalf("multi-member bindings after fence = %d error=%v, want zero", bindingCount, err)
	}
}

func TestWorkerRegistryProposalIsAdvisoryUntilApprovedPlan(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	fleetPool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	schedulerPool := newRolePool(
		t, database.DSN, "vela_scheduler_login", "vela-scheduler-password",
	)
	proposalID := uuid.MustParse("49200000-0000-0000-0000-000000000200")
	proposal := map[string]any{
		"schema_version":     1,
		"id":                 proposalID.String(),
		"input_digest":       digestHex(0xc0),
		"confidence_ppm":     900000,
		"expires_at":         time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
		"min_capacity":       map[string]any{"dit": 7},
		"desired_capacity":   map[string]any{"dit": 14},
		"max_capacity":       map[string]any{"dit": 21},
		"cooldown_seconds":   86400,
		"budget_micro_units": 1000000,
		"reason_codes":       []string{"QUEUE_PRESSURE", "WARM_RESIDENCY_FLOOR"},
		"proposed_by":        "capacity-simulator/shadow",
	}
	encodedProposal, err := json.Marshal(proposal)
	if err != nil {
		t.Fatalf("encode ResidencyProposal: %v", err)
	}
	var recordedProposalID uuid.UUID
	if err := fleetPool.QueryRow(context.Background(), `
		SELECT proposal_id FROM vela_record_residency_proposal($1::jsonb)
	`, encodedProposal).Scan(&recordedProposalID); err != nil {
		t.Fatalf("record ResidencyProposal: %v", err)
	}
	if recordedProposalID != proposalID {
		t.Fatalf("recorded proposal id = %s, want %s", recordedProposalID, proposalID)
	}
	var workerCount int
	if err := database.Admin.QueryRow(`SELECT count(*) FROM worker_instances`).Scan(&workerCount); err != nil {
		t.Fatalf("count Workers after proposal: %v", err)
	}
	if workerCount != 0 {
		t.Fatalf("advisory proposal created %d WorkerInstances", workerCount)
	}

	plan := approvedResidencyPlanFixture(proposalID)
	delete(plan, "approval_evidence_digest")
	encodedPlan, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("encode unapproved ResidencyPlan: %v", err)
	}
	_, err = fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_apply_residency_plan($1::jsonb)",
		encodedPlan,
	)
	assertPostgresConstraint(t, err, "residency_plan_approval_required")

	plan = approvedResidencyPlanFixture(proposalID)
	encodedPlan, err = json.Marshal(plan)
	if err != nil {
		t.Fatalf("encode approved ResidencyPlan: %v", err)
	}
	if _, err := schedulerPool.Exec(
		context.Background(),
		"SELECT * FROM vela_apply_residency_plan($1::jsonb)",
		encodedPlan,
	); !isPermissionDenied(err) {
		t.Fatalf("Scheduler ResidencyPlan apply error = %v, want permission denied", err)
	}
	var appliedWorkers int
	if err := fleetPool.QueryRow(context.Background(), `
		SELECT worker_instance_count FROM vela_apply_residency_plan($1::jsonb)
	`, encodedPlan).Scan(&appliedWorkers); err != nil {
		t.Fatalf("apply approved ResidencyPlan: %v", err)
	}
	if appliedWorkers != 1 {
		t.Fatalf("applied WorkerInstance count = %d, want 1", appliedWorkers)
	}
	var lifecycle string
	if err := database.Admin.QueryRow(`
		SELECT lifecycle_state::text FROM worker_instances
		WHERE id = '49200000-0000-0000-0000-000000000204'
	`).Scan(&lifecycle); err != nil || lifecycle != "PROVISIONING" {
		t.Fatalf("planned WorkerInstance lifecycle = %q error=%v", lifecycle, err)
	}
}

func TestWorkerRegistryAndFleetGoInterfaceOwnsCommandSequence(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	pool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	service, err := fleet.NewService(pool)
	if err != nil {
		t.Fatalf("construct WorkerRegistryAndFleet service: %v", err)
	}
	var registry fleet.WorkerRegistryAndFleet = service

	proposalID := uuid.MustParse("49200000-0000-0000-0000-000000000210")
	proposal, err := registry.Propose(context.Background(), fleet.ResidencyPlanInputs{
		ProposalID:       proposalID,
		InputDigest:      mustDigestBytes(t, digestHex(0xd0)),
		ConfidencePPM:    900000,
		ExpiresAt:        time.Now().UTC().Add(time.Hour),
		MinCapacity:      map[string]int64{"dit": 7},
		DesiredCapacity:  map[string]int64{"dit": 14},
		MaxCapacity:      map[string]int64{"dit": 21},
		Cooldown:         24 * time.Hour,
		BudgetMicroUnits: 1000000,
		ReasonCodes:      []string{"QUEUE_PRESSURE", "WARM_RESIDENCY_FLOOR"},
		ProposedBy:       "capacity-simulator/shadow",
	})
	if err != nil || proposal.ID != proposalID {
		t.Fatalf("propose residency = %#v error=%v", proposal, err)
	}

	encodedPlan, err := json.Marshal(approvedResidencyPlanFixture(proposalID))
	if err != nil {
		t.Fatalf("encode Go Interface ResidencyPlan: %v", err)
	}
	var approvedPlan fleet.ApprovedResidencyPlan
	if err := json.Unmarshal(encodedPlan, &approvedPlan); err != nil {
		t.Fatalf("decode Go Interface ResidencyPlan: %v", err)
	}
	actuation, err := registry.Apply(context.Background(), approvedPlan)
	if err != nil || actuation.WorkerInstanceCount != 1 {
		t.Fatalf("apply residency plan = %#v error=%v", actuation, err)
	}

	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000204")
	var evidence fleet.WorkerInstanceEvidence
	if err := json.Unmarshal(workerRegistryEvidence(workerID, 0xd1), &evidence); err != nil {
		t.Fatalf("decode WorkerInstance evidence: %v", err)
	}
	decision, err := registry.Observe(context.Background(), evidence)
	if err != nil || decision.Readiness != fleet.WorkerInstanceReady {
		t.Fatalf("observe WorkerInstance = %#v error=%v", decision, err)
	}
	transition, err := registry.Drain(context.Background(), fleet.WorkerInstanceDrainRequest{
		WorkerInstanceID:      workerID,
		ExpectedInstanceEpoch: 1,
		Reason:                "approved rollout",
		RequestedBy:           "fleet/operator-3",
	})
	if err != nil || transition.Lifecycle != fleet.WorkerInstanceDraining {
		t.Fatalf("drain WorkerInstance = %#v error=%v", transition, err)
	}
}

func mustDigestBytes(t *testing.T, encoded string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode fixture digest: %v", err)
	}
	return decoded
}

func approvedResidencyPlanFixture(proposalID uuid.UUID) map[string]any {
	now := time.Now().UTC().Truncate(time.Millisecond)
	return map[string]any{
		"schema_version":           1,
		"id":                       "49200000-0000-0000-0000-000000000201",
		"stable_id":                "h3-dit-residency",
		"revision":                 1,
		"source_proposal_id":       proposalID.String(),
		"content_digest":           digestHex(0xc1),
		"approval_evidence_digest": digestHex(0xc2),
		"approved_at":              now.Format(time.RFC3339Nano),
		"approved_by":              "fleet/operator-2",
		"capacity_pools": []map[string]any{{
			"id":                        "49200000-0000-0000-0000-000000000202",
			"stable_id":                 "h3-dit-plan-pool",
			"stage_profile_revision_id": "49000000-0000-0000-0000-000000000041",
			"resource_class":            "GPU",
			"security_class":            "INTERNAL",
			"region":                    "cn-shanghai",
			"max_ready_queue_depth":     1024,
		}},
		"worker_bundles": []map[string]any{{
			"id":                 "49200000-0000-0000-0000-000000000203",
			"stable_id":          "h3-node-plan-bundle",
			"desired_generation": 1,
			"layout_digest":      digestHex(0xc3),
		}},
		"worker_instances": []map[string]any{{
			"id":                         "49200000-0000-0000-0000-000000000204",
			"worker_profile_revision_id": workerRegistryProfileID,
			"capacity_pool_id":           "49200000-0000-0000-0000-000000000202",
			"worker_bundle_id":           "49200000-0000-0000-0000-000000000203",
			"desired_member_count":       1,
			"desired_device_count":       1,
		}},
	}
}

func assertWorkerInstanceAuthority(
	t *testing.T,
	database *pgxpool.Pool,
	workerID uuid.UUID,
	instanceEpoch int64,
	deviceSetDigest []byte,
	membershipDigest []byte,
	residencyID uuid.UUID,
	modelRuntimeEpoch int64,
	want bool,
) {
	t.Helper()
	var matches bool
	if err := database.QueryRow(context.Background(), `
		SELECT vela_worker_instance_authority_matches($1, $2, $3, $4, $5, $6)
	`, workerID, instanceEpoch, deviceSetDigest, membershipDigest,
		residencyID, modelRuntimeEpoch).Scan(&matches); err != nil {
		t.Fatalf("match WorkerInstance authority: %v", err)
	}
	if matches != want {
		t.Fatalf("WorkerInstance authority match = %t, want %t", matches, want)
	}
}

func seedWorkerRegistryPlan(t *testing.T, database *sql.DB) {
	t.Helper()
	if _, err := database.Exec(`
		INSERT INTO capacity_pools (
			id, stable_id, stage_profile_revision_id, resource_class,
			security_class, region, max_ready_queue_depth, state
		) VALUES (
			$1, 'h3-dit-shanghai', '49000000-0000-0000-0000-000000000041',
			'GPU', 'INTERNAL', 'cn-shanghai', 1024, 'ACTIVE'
		)
	`, workerRegistryPoolID); err != nil {
		t.Fatalf("seed Worker Registry CapacityPool: %v", err)
	}
	if _, err := database.Exec(`
		INSERT INTO worker_bundles (
			id, stable_id, plan_revision, desired_generation, observed_generation,
			lifecycle_state, layout_digest, approved_by
		) VALUES (
			$1, 'h3-node-01-layout', 'plan-h3-v1', 1, 0, 'APPLYING',
			decode(repeat('42', 32), 'hex'), 'fleet-test'
		)
	`, workerRegistryBundleID); err != nil {
		t.Fatalf("seed Worker Registry WorkerBundle: %v", err)
	}
}

func seedWorkerInstance(
	t *testing.T,
	database *sql.DB,
	workerID uuid.UUID,
	profileID string,
	memberCount int,
	deviceCount int,
) {
	t.Helper()
	if _, err := database.Exec(`
		INSERT INTO worker_instances (
			id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
			lifecycle_state, reachability_state, instance_epoch,
			control_session_epoch, desired_member_count, desired_device_count
		) VALUES ($1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, $5, $6)
	`, workerID, profileID, workerRegistryPoolID, workerRegistryBundleID,
		memberCount, deviceCount); err != nil {
		t.Fatalf("seed WorkerInstance %s: %v", workerID, err)
	}
}

func seedMultiMemberProfile(t *testing.T, database *sql.DB) {
	t.Helper()
	if _, err := database.Exec(`
		INSERT INTO worker_profile_revisions (
			id, stable_id, revision, state, device_count, member_count,
			device_set_shape, resident_model_revisions, capacity_limits,
			readiness_checks, content_digest
		) VALUES (
			$1, 'future-llm-4gpu-2node', 1, 'CERTIFIED', 4, 2,
			'{"kind":"certified-multi-node","devices":4,"members":2}',
			'["future-llm-component-v1"]', '{"concurrency":1}',
			'{"membership_barrier":true,"warmup":true}',
			decode(repeat('70', 32), 'hex')
		)
	`, multiWorkerProfileID); err != nil {
		t.Fatalf("seed multi-member WorkerProfile: %v", err)
	}
	if _, err := database.Exec(`
		INSERT INTO stage_profile_revisions (
			id, stable_id, revision, state, stage_definition_revision_id,
			model_component_revision, runtime_image_digest,
			worker_profile_revision_id, result_equivalence_revision_id,
			certified_capacity_vector, content_digest
		) VALUES (
			$1, 'future-llm-stage', 1, 'CERTIFIED',
			'49000000-0000-0000-0000-000000000031', 'future-llm-component-v1',
			'sha256:2222222222222222222222222222222222222222222222222222222222222222',
			$2, '49000000-0000-0000-0000-000000000024', '{"concurrency":1}',
			decode(repeat('71', 32), 'hex')
		)
	`, multiStageProfileID, multiWorkerProfileID); err != nil {
		t.Fatalf("seed multi-member StageProfile: %v", err)
	}
	if _, err := database.Exec(`
		INSERT INTO capacity_pools (
			id, stable_id, stage_profile_revision_id, resource_class,
			security_class, region, max_ready_queue_depth, state
		) VALUES (
			$1, 'future-llm-multinode', $2, 'GPU', 'INTERNAL',
			'cn-shanghai', 128, 'ACTIVE'
		)
	`, multiCapacityPoolID, multiStageProfileID); err != nil {
		t.Fatalf("seed multi-member CapacityPool: %v", err)
	}
}

func multiMemberWorkerEvidence(workerID uuid.UUID, complete bool) []byte {
	observedAt := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	nodeIDs := []string{
		"49200000-0000-0000-0000-000000000110",
		"49200000-0000-0000-0000-000000000111",
	}
	devices := make([]map[string]any, 0, 4)
	deviceIDs := make([]string, 0, 4)
	for index := 0; index < 4; index++ {
		deviceID := uuid.MustParse(
			"49200000-0000-0000-0000-00000000011" + string(rune('2'+index)),
		).String()
		deviceIDs = append(deviceIDs, deviceID)
		nodeIndex := index / 2
		devices = append(devices, map[string]any{
			"id":                      deviceID,
			"compute_node_id":         nodeIDs[nodeIndex],
			"node_identity":           "llm-node-" + string(rune('a'+nodeIndex)),
			"region":                  "cn-shanghai",
			"network_domain":          "ib-fabric-a",
			"fault_domain":            "power-" + string(rune('a'+nodeIndex)),
			"node_epoch":              1,
			"agent_session_epoch":     1,
			"node_attestation_digest": digestHex(byte(0x80 + nodeIndex)),
			"kind":                    "GPU",
			"gpu_uuid": "GPU-00000000-0000-0000-0000-00000000010" +
				string([]byte{'0' + byte(index)}),
			"pci_bdf":            "0000:4" + string([]byte{'1' + byte(index)}) + ":00.0",
			"device_epoch":       1,
			"ordinal":            index,
			"health":             "HEALTHY",
			"attestation_digest": digestHex(byte(0x90 + index)),
		})
	}
	members := []map[string]any{
		{
			"id":                   "49200000-0000-0000-0000-000000000120",
			"member_key":           "member-0",
			"compute_node_id":      nodeIDs[0],
			"member_epoch":         1,
			"device_ids":           deviceIDs[:2],
			"device_subset_digest": digestHex(0xa0),
			"identity_digest":      digestHex(0xa1),
			"readiness":            "READY",
		},
		{
			"id":                   "49200000-0000-0000-0000-000000000121",
			"member_key":           "member-1",
			"compute_node_id":      nodeIDs[1],
			"member_epoch":         1,
			"device_ids":           deviceIDs[2:],
			"device_subset_digest": digestHex(0xa2),
			"identity_digest":      digestHex(0xa3),
			"readiness":            "READY",
		},
	}
	if !complete {
		members = members[:1]
	}
	evidence := map[string]any{
		"schema_version":        1,
		"worker_instance_id":    workerID.String(),
		"instance_epoch":        1,
		"control_session_epoch": 1,
		"device_set": map[string]any{
			"id":                "49200000-0000-0000-0000-000000000122",
			"membership_digest": digestHex(0xb0),
			"topology_digest":   digestHex(0xb1),
			"devices":           devices,
		},
		"members": members,
		"residencies": []map[string]any{{
			"id":                       "49200000-0000-0000-0000-000000000123",
			"model_component_revision": "future-llm-component-v1",
			"runtime_identity":         "future-llm-runtime@sha256:runtime-v1",
			"runtime_image_digest":     "sha256:2222222222222222222222222222222222222222222222222222222222222222",
			"model_runtime_epoch":      1,
			"state":                    "READY",
			"warmup_evidence_digest":   digestHex(0xb2),
			"canary_evidence_digest":   digestHex(0xb3),
		}},
		"capacity": map[string]any{
			"sequence":    1,
			"vector":      map[string]any{"concurrency": 1},
			"observed_at": observedAt.Format(time.RFC3339Nano),
			"expires_at":  observedAt.Add(2 * time.Minute).Format(time.RFC3339Nano),
		},
		"observed_at": observedAt.Format(time.RFC3339Nano),
		"observed_by": "node-agent/llm-members",
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		panic(err)
	}
	return encoded
}

func workerRegistryEvidence(workerID uuid.UUID, identityByte byte) []byte {
	observedAt := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	evidence := map[string]any{
		"schema_version":        1,
		"worker_instance_id":    workerID.String(),
		"instance_epoch":        1,
		"control_session_epoch": 1,
		"device_set": map[string]any{
			"id":                uuid.NewSHA1(workerID, []byte("device-set")).String(),
			"membership_digest": digestHex(identityByte),
			"topology_digest":   digestHex(identityByte + 1),
			"devices": []map[string]any{{
				"id":                      workerRegistryDeviceID,
				"compute_node_id":         workerRegistryNodeID,
				"node_identity":           "h3-node-01",
				"region":                  "cn-shanghai",
				"network_domain":          "rack-a",
				"fault_domain":            "power-a",
				"node_epoch":              1,
				"agent_session_epoch":     1,
				"node_attestation_digest": digestHex(0x43),
				"kind":                    "GPU",
				"gpu_uuid":                "GPU-00000000-0000-0000-0000-000000000004",
				"pci_bdf":                 "0000:41:00.0",
				"device_epoch":            1,
				"ordinal":                 0,
				"health":                  "HEALTHY",
				"attestation_digest":      digestHex(0x44),
			}},
		},
		"members": []map[string]any{{
			"id":                   uuid.NewSHA1(workerID, []byte("member-0")).String(),
			"member_key":           "member-0",
			"compute_node_id":      workerRegistryNodeID,
			"member_epoch":         1,
			"device_ids":           []string{workerRegistryDeviceID},
			"device_subset_digest": digestHex(identityByte + 3),
			"identity_digest":      digestHex(identityByte + 4),
			"readiness":            "READY",
		}},
		"residencies": []map[string]any{{
			"id":                       uuid.NewSHA1(workerID, []byte("residency")).String(),
			"model_component_revision": "h3-component-v1",
			"runtime_identity":         "h3-dit-runtime@sha256:runtime-v1",
			"runtime_image_digest":     "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			"model_runtime_epoch":      1,
			"state":                    "READY",
			"warmup_evidence_digest":   digestHex(identityByte + 5),
			"canary_evidence_digest":   digestHex(identityByte + 6),
		}},
		"capacity": map[string]any{
			"sequence":    1,
			"vector":      map[string]any{"concurrency": 1},
			"observed_at": observedAt.Format(time.RFC3339Nano),
			"expires_at":  observedAt.Add(2 * time.Minute).Format(time.RFC3339Nano),
		},
		"observed_at": observedAt.Format(time.RFC3339Nano),
		"observed_by": "node-agent/h3-node-01",
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		panic(err)
	}
	return encoded
}

func digestHex(value byte) string {
	encoded := make([]byte, 64)
	const digits = "0123456789abcdef"
	for index := 0; index < len(encoded); index += 2 {
		encoded[index] = digits[value>>4]
		encoded[index+1] = digits[value&0x0f]
	}
	return string(encoded)
}
