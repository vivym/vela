//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/workercontrol"
)

func TestFleetProtocolRequiresZeroLegacyWritersAndImmutableOperatorReceipts(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)

	var enforced bool
	var version int
	if err := database.Admin.QueryRow(`
		SELECT enforced, protocol_version
		FROM fleet_assignment_protocol_state
		WHERE singleton
	`).Scan(&enforced, &version); err != nil {
		t.Fatalf("read expanded Fleet protocol state: %v", err)
	}
	if enforced || version != 1 {
		t.Fatalf("expanded Fleet protocol = enforced %t version %d", enforced, version)
	}
	assertFleetProtocolCallDisabled(t, database)

	_, err := database.Admin.Exec(`
		SELECT vela_transition_fleet_assignment_protocol(
			true, 'legacy Assignment writer still active', 1
		)
	`)
	assertFleetSQLState(t, err, "55000")
	if _, err := database.Admin.Exec(`
		SELECT vela_transition_fleet_assignment_protocol(
			true, 'operator verified zero legacy Assignment writers', 0
		)
	`); err != nil {
		t.Fatalf("enforce Fleet protocol: %v", err)
	}
	if err := database.Admin.QueryRow(`
		SELECT enforced, protocol_version
		FROM fleet_assignment_protocol_state
		WHERE singleton
	`).Scan(&enforced, &version); err != nil {
		t.Fatalf("read enforced Fleet protocol state: %v", err)
	}
	if !enforced || version != 2 {
		t.Fatalf("enforced Fleet protocol = enforced %t version %d", enforced, version)
	}
	_, err = database.Admin.Exec(`
		SELECT * FROM vela_request_worker_drain(
			NULL, NULL, NULL, NULL, NULL, NULL
		)
	`)
	assertFleetSQLState(t, err, "22023")

	_, err = database.Admin.Exec(`
		UPDATE fleet_assignment_protocol_transitions
		SET transition_receipt = 'rewritten'
	`)
	assertFleetSQLState(t, err, "55000")
	_, err = database.Admin.Exec(`
		UPDATE fleet_assignment_protocol_state
		SET enforced = false
		WHERE singleton
	`)
	assertFleetSQLState(t, err, "55000")

	if _, err := database.Admin.Exec(`
		SELECT vela_transition_fleet_assignment_protocol(
			false, 'operator rollback receipt', 0
		)
	`); err != nil {
		t.Fatalf("roll back Fleet protocol: %v", err)
	}
	var receipts string
	if err := database.Admin.QueryRow(`
		SELECT string_agg(transition_receipt, ',' ORDER BY protocol_version)
		FROM fleet_assignment_protocol_transitions
	`).Scan(&receipts); err != nil {
		t.Fatalf("read Fleet protocol receipts: %v", err)
	}
	if receipts != "operator verified zero legacy Assignment writers,operator rollback receipt" {
		t.Fatalf("Fleet protocol receipts = %q", receipts)
	}
	assertFleetProtocolCallDisabled(t, database)
}

func TestFleetRuntimeRoleCannotReadFleetTablesDirectly(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	pool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	for _, table := range []string{
		"worker_capacity_conditions",
		"worker_pool_capacity_conditions",
		"worker_pool_capacity_policies",
		"worker_readiness_cycles",
		"worker_readiness_evidence",
		"worker_drain_operations",
		"fleet_mutation_authorizations",
		"fleet_retirement_completions",
		"fleet_assignment_protocol_state",
		"fleet_assignment_protocol_transitions",
	} {
		_, err := pool.Exec(context.Background(), "SELECT * FROM "+table)
		assertFleetSQLState(t, err, "42501")
	}
}

func TestFleetPoolCapacityRequiresAtLeastOneFreshAvailableWorker(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23000000-0000-0000-0000-0000000000b1")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/fleet-pool-freshness', 1,
			'READY', 'HEALTHY', 'node-fleet-pool-freshness'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed pool-freshness Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	if _, err := service.ConfigureCapacityPolicy(
		context.Background(), testCapacityPolicy(poolID),
	); err != nil {
		t.Fatalf("configure pool-freshness capacity policy: %v", err)
	}
	result, err := service.ObserveCapacity(
		context.Background(), capacityObservation(workerID, poolID, 1, 1, 300, true),
	)
	if err != nil || !result.PoolAssignmentAllowed {
		t.Fatalf("fresh available Worker pool capacity = %#v error=%v", result, err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE workers SET lifecycle_state = 'DRAINING' WHERE id = $1
	`, workerID); err != nil {
		t.Fatalf("remove last available Worker from pool: %v", err)
	}
	pool, err := service.GetPoolCapacity(context.Background(), poolID)
	if err != nil || pool.PoolAssignmentAllowed {
		t.Fatalf("pool without fresh available Worker = %#v error=%v", pool, err)
	}
}

func TestFleetOwnerPrivilegesMatchFunctionReadWriteSet(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	tests := []struct {
		relation  string
		privilege string
		allowed   bool
	}{
		{relation: "worker_readiness_cycles", privilege: "SELECT", allowed: true},
		{relation: "worker_readiness_cycles", privilege: "INSERT", allowed: true},
		{relation: "worker_readiness_cycles", privilege: "UPDATE", allowed: true},
		{relation: "worker_readiness_cycles", privilege: "DELETE", allowed: false},
		{relation: "worker_readiness_evidence", privilege: "SELECT", allowed: true},
		{relation: "worker_readiness_evidence", privilege: "INSERT", allowed: true},
		{relation: "worker_readiness_evidence", privilege: "UPDATE", allowed: false},
		{relation: "worker_readiness_evidence", privilege: "DELETE", allowed: false},
		{relation: "fleet_mutation_authorizations", privilege: "SELECT", allowed: true},
		{relation: "fleet_mutation_authorizations", privilege: "INSERT", allowed: true},
		{relation: "fleet_mutation_authorizations", privilege: "UPDATE", allowed: false},
		{relation: "fleet_mutation_authorizations", privilege: "DELETE", allowed: false},
		{relation: "fleet_retirement_completions", privilege: "SELECT", allowed: true},
		{relation: "fleet_retirement_completions", privilege: "INSERT", allowed: true},
		{relation: "fleet_retirement_completions", privilege: "UPDATE", allowed: false},
		{relation: "fleet_retirement_completions", privilege: "DELETE", allowed: false},
		{relation: "worker_pools", privilege: "SELECT", allowed: true},
		{relation: "worker_pools", privilege: "UPDATE", allowed: false},
		{relation: "execution_profile_revisions", privilege: "SELECT", allowed: true},
		{relation: "execution_profile_revisions", privilege: "UPDATE", allowed: false},
		{relation: "workers", privilege: "SELECT", allowed: true},
		{relation: "workers", privilege: "UPDATE", allowed: true},
		{relation: "worker_profile_readiness", privilege: "SELECT", allowed: true},
		{relation: "worker_profile_readiness", privilege: "INSERT", allowed: true},
		{relation: "worker_profile_readiness", privilege: "UPDATE", allowed: true},
		{relation: "worker_profile_readiness", privilege: "DELETE", allowed: true},
	}
	for _, test := range tests {
		var allowed bool
		if err := database.Admin.QueryRow(`
			SELECT has_table_privilege(
				'vela_fleet_owner', 'public.' || $1, $2
			)
		`, test.relation, test.privilege).Scan(&allowed); err != nil {
			t.Fatalf("inspect Fleet owner %s on %s: %v", test.privilege, test.relation, err)
		}
		if allowed != test.allowed {
			t.Fatalf(
				"Fleet owner %s on %s = %t, want %t",
				test.privilege,
				test.relation,
				allowed,
				test.allowed,
			)
		}
	}
	for _, lockPrivilege := range []struct {
		relation string
		column   string
	}{
		{relation: "worker_pools", column: "id"},
		{relation: "execution_profile_revisions", column: "id"},
		{relation: "worker_readiness_evidence", column: "cycle_id"},
	} {
		var allowed bool
		if err := database.Admin.QueryRow(`
			SELECT has_column_privilege(
				'vela_fleet_owner', 'public.' || $1, $2, 'UPDATE'
			)
		`, lockPrivilege.relation, lockPrivilege.column).Scan(&allowed); err != nil {
			t.Fatalf(
				"inspect Fleet owner lock privilege on %s.%s: %v",
				lockPrivilege.relation,
				lockPrivilege.column,
				err,
			)
		}
		if !allowed {
			t.Fatalf(
				"Fleet owner lacks row-lock privilege on %s.%s",
				lockPrivilege.relation,
				lockPrivilege.column,
			)
		}
	}
}

func TestFleetMigrationEmptyDownUpRestoresNMinusOneSurface(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 22); err != nil {
		t.Fatalf("Fleet migration down: %v", err)
	}
	assertTableDoesNotExist(t, database.Admin, "fleet_assignment_protocol_state")
	assertTableDoesNotExist(t, database.Admin, "worker_capacity_conditions")
	var fleetColumnExists bool
	if err := database.Admin.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public' AND table_name = 'attempts'
			  AND column_name = 'fleet_protocol_version'
		)
	`).Scan(&fleetColumnExists); err != nil {
		t.Fatalf("inspect contracted Fleet Assignment column: %v", err)
	}
	if fleetColumnExists {
		t.Fatal("Fleet Assignment protocol column survived migration down")
	}
	if err := goose.Up(database.Admin, migrations); err != nil {
		t.Fatalf("Fleet migration up after down: %v", err)
	}
	if err := database.Admin.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = 'public' AND table_name = 'fleet_assignment_protocol_state'
		)
	`).Scan(&fleetColumnExists); err != nil || !fleetColumnExists {
		t.Fatalf("Fleet protocol state after migration up = %t error=%v", fleetColumnExists, err)
	}
}

func TestFleetMigrationDownRefusesDurableEvidence(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err := goose.DownTo(database.Admin, migrations, 22)
	if err == nil {
		t.Fatal("Fleet migration down removed durable transition evidence")
	}
	var version int
	if scanErr := database.Admin.QueryRow(`
		SELECT protocol_version
		FROM fleet_assignment_protocol_state
		WHERE singleton
	`).Scan(&version); scanErr != nil || version != 2 {
		t.Fatalf("Fleet protocol after refused down = version %d error=%v", version, scanErr)
	}
}

func TestFleetResolvesCurrentWorkerIdentityByExactNodeAndPool(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000020")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/fleet-node-binding', 7,
			'WARMING', 'SUSPECT', 'h3-node-01'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed Node-bound Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}

	_, err = service.ResolveWorkerIdentity(
		context.Background(),
		fleet.WorkerIdentityRequest{
			NodeIdentity: "h3-node-01", WorkerPoolID: poolID,
			KubernetesUID: "kubernetes-worker-pod-uid-1",
			Namespace:     "vela-system", Name: "h3-worker-node-1",
		},
	)
	assertFleetFailure(t, err, fleet.FailureConflict)

	enforceFleetProtocol(t, database.Admin)
	identity, err := service.ResolveWorkerIdentity(
		context.Background(),
		fleet.WorkerIdentityRequest{
			NodeIdentity: "h3-node-01", WorkerPoolID: poolID,
			KubernetesUID: "kubernetes-worker-pod-uid-1",
			Namespace:     "vela-system", Name: "h3-worker-node-1",
		},
	)
	if err != nil || identity.WorkerID != workerID || identity.WorkerPoolID != poolID ||
		identity.WorkerEpoch != 8 || identity.NodeIdentity != "h3-node-01" {
		t.Fatalf("resolved Worker identity = %#v error=%v", identity, err)
	}
	replayed, err := service.ResolveWorkerIdentity(
		context.Background(),
		fleet.WorkerIdentityRequest{
			NodeIdentity: "h3-node-01", WorkerPoolID: poolID,
			KubernetesUID: "kubernetes-worker-pod-uid-1",
			Namespace:     "vela-system", Name: "h3-worker-node-1",
		},
	)
	if err != nil || replayed != identity {
		t.Fatalf("replayed Worker Pod identity = %#v error=%v, want %#v", replayed, err, identity)
	}
	replacement, err := service.ResolveWorkerIdentity(
		context.Background(),
		fleet.WorkerIdentityRequest{
			NodeIdentity: "h3-node-01", WorkerPoolID: poolID,
			KubernetesUID: "kubernetes-worker-pod-uid-2",
			Namespace:     "vela-system", Name: "h3-worker-node-2",
		},
	)
	if err != nil || replacement.WorkerID != workerID || replacement.WorkerEpoch != 9 {
		t.Fatalf("replacement Worker Pod identity = %#v error=%v", replacement, err)
	}
	_, err = service.ResolveWorkerIdentity(
		context.Background(),
		fleet.WorkerIdentityRequest{
			NodeIdentity: "h3-node-01", WorkerPoolID: poolID,
			KubernetesUID: "kubernetes-worker-pod-uid-1",
			Namespace:     "vela-system", Name: "h3-worker-node-1",
		},
	)
	assertFleetFailure(t, err, fleet.FailureConflict)

	_, err = service.ResolveWorkerIdentity(
		context.Background(),
		fleet.WorkerIdentityRequest{
			NodeIdentity:  "h3-node-01",
			WorkerPoolID:  uuid.MustParse("00000000-0000-0000-0000-000000000105"),
			KubernetesUID: "kubernetes-worker-pod-uid-3",
			Namespace:     "vela-system", Name: "h3-worker-node-3",
		},
	)
	assertFleetFailure(t, err, fleet.FailureNotFound)
	_, err = service.ResolveWorkerIdentity(
		context.Background(),
		fleet.WorkerIdentityRequest{
			NodeIdentity: "h3-node-02", WorkerPoolID: poolID,
			KubernetesUID: "kubernetes-worker-pod-uid-4",
			Namespace:     "vela-system", Name: "h3-worker-node-4",
		},
	)
	assertFleetFailure(t, err, fleet.FailureNotFound)
}

func TestFleetReadinessRequiresFiveOrderedChecksBeforeHealthyReady(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000021")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/fleet-readiness', 3,
			'RECOVERING', 'OFFLINE', 'node-fleet-readiness'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed recovering Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	configureTestCapacityPolicies(t, service, poolID)
	if _, err := service.ObserveCapacity(
		context.Background(), capacityObservation(workerID, poolID, 3, 1, 300, true),
	); err != nil {
		t.Fatalf("record recovering Worker capacity: %v", err)
	}

	cycleID := uuid.MustParse("23000000-0000-0000-0000-000000000022")
	request := fleet.ReadinessRequest{
		CycleID: cycleID, WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 3,
		NodeIdentity: "node-fleet-readiness", ExecutionProfileRevisionID: profileID,
		InferenceBackendRevision: "sglang-h3-v3", RequestedBy: "fleet-controller/primary",
		Deadline: time.Now().Add(time.Hour),
	}
	started, err := service.BeginReadiness(context.Background(), request)
	if err != nil || started.Replayed || started.State != fleet.ReadinessChecking ||
		started.NextCheck != fleet.ReadinessIdentity || started.WorkerLifecycle != "WARMING" ||
		started.WorkerReachability != "SUSPECT" {
		t.Fatalf("begin readiness = %#v error=%v", started, err)
	}
	replayedStart, err := service.BeginReadiness(context.Background(), request)
	if err != nil || !replayedStart.Replayed || replayedStart.State != fleet.ReadinessChecking ||
		replayedStart.NextCheck != fleet.ReadinessIdentity {
		t.Fatalf("replay readiness begin = %#v error=%v", replayedStart, err)
	}

	earlyCanary := readinessEvidence(cycleID, fleet.ReadinessCanary, "early canary")
	_, err = service.ReportReadiness(context.Background(), earlyCanary)
	var conflict *fleet.Failure
	if !errors.As(err, &conflict) || conflict.Code != fleet.FailureConflict {
		t.Fatalf("out-of-order canary error = %v, want conflict", err)
	}

	checks := []fleet.ReadinessCheck{
		fleet.ReadinessIdentity,
		fleet.ReadinessDevice,
		fleet.ReadinessInferenceBackend,
		fleet.ReadinessModelWarmup,
		fleet.ReadinessCanary,
	}
	for index, check := range checks {
		evidence := readinessEvidence(cycleID, check, "passed "+string(check))
		result, reportErr := service.ReportReadiness(context.Background(), evidence)
		if reportErr != nil {
			t.Fatalf("report %s readiness: %v", check, reportErr)
		}
		replayed, replayErr := service.ReportReadiness(context.Background(), evidence)
		if replayErr != nil || !replayed.Replayed || replayed.State != result.State ||
			replayed.NextCheck != result.NextCheck {
			t.Fatalf("replay %s readiness = %#v error=%v, want replay of %#v", check, replayed, replayErr, result)
		}
		if index < len(checks)-1 {
			if result.State != fleet.ReadinessChecking || result.NextCheck != checks[index+1] ||
				result.WorkerLifecycle == "READY" || result.WorkerReachability == "HEALTHY" {
				t.Fatalf("intermediate %s readiness = %#v", check, result)
			}
			continue
		}
		if result.State != fleet.ReadinessReady || result.NextCheck != "" ||
			result.WorkerLifecycle != "READY" || result.WorkerReachability != "HEALTHY" {
			t.Fatalf("completed readiness = %#v", result)
		}
	}

	completed, err := service.GetReadiness(context.Background(), cycleID)
	if err != nil || completed.State != fleet.ReadinessReady ||
		completed.WorkerLifecycle != "READY" || completed.WorkerReachability != "HEALTHY" {
		t.Fatalf("get completed readiness = %#v error=%v", completed, err)
	}
}

func TestFleetReadinessBootstrapBeginsBeforeFirstCapacityObservation(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23000000-0000-0000-0000-0000000000b1")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/fleet-bootstrap', 1,
			'REGISTERING', 'SUSPECT', 'node-fleet-bootstrap'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed registering Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	configureTestCapacityPolicies(t, service, poolID)

	cycleID := uuid.MustParse("23000000-0000-0000-0000-0000000000b2")
	started, err := service.BeginReadiness(context.Background(), fleet.ReadinessRequest{
		CycleID: cycleID, WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 1,
		NodeIdentity: "node-fleet-bootstrap", ExecutionProfileRevisionID: profileID,
		InferenceBackendRevision: "sglang-h3-v3",
		RequestedBy:              "fleet-controller/primary",
		Deadline:                 time.Now().Add(time.Hour),
	})
	if err != nil || started.State != fleet.ReadinessChecking ||
		started.NextCheck != fleet.ReadinessIdentity || started.WorkerLifecycle != "WARMING" ||
		started.WorkerReachability != "SUSPECT" {
		t.Fatalf("bootstrap readiness = %#v error=%v", started, err)
	}
	var observations int
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM worker_capacity_conditions WHERE worker_id = $1
	`, workerID).Scan(&observations); err != nil {
		t.Fatalf("count bootstrap capacity observations: %v", err)
	}
	if observations != 0 {
		t.Fatalf("bootstrap readiness fabricated %d capacity observations", observations)
	}
}

func TestFleetReturnsExactActiveReadinessWorkForCurrentWorkerEpoch(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23010000-0000-0000-0000-000000000021")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/readiness-work', 4,
			'RECOVERING', 'OFFLINE', 'node-readiness-work'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed readiness Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	configureTestCapacityPolicies(t, service, poolID)
	if _, err := service.ObserveCapacity(
		context.Background(), capacityObservation(workerID, poolID, 4, 1, 300, true),
	); err != nil {
		t.Fatalf("record readiness capacity: %v", err)
	}
	cycleID := uuid.MustParse("23010000-0000-0000-0000-000000000022")
	deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	if _, err := service.BeginReadiness(context.Background(), fleet.ReadinessRequest{
		CycleID: cycleID, WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 4,
		NodeIdentity: "node-readiness-work", ExecutionProfileRevisionID: profileID,
		InferenceBackendRevision: "sglang-h3-v3", RequestedBy: "fleet-controller/primary",
		Deadline: deadline,
	}); err != nil {
		t.Fatalf("begin readiness: %v", err)
	}

	work, err := service.GetWorkerReadinessWork(context.Background(), workerID, 4)
	if err != nil || !work.Available || work.CycleID != cycleID ||
		work.Check != fleet.ReadinessIdentity || work.WorkerID != workerID ||
		work.WorkerPoolID != poolID || work.WorkerEpoch != 4 ||
		work.NodeIdentity != "node-readiness-work" ||
		work.ExecutionProfileRevisionID != profileID ||
		work.InferenceBackendRevision != "sglang-h3-v3" || !work.Deadline.Equal(deadline) {
		t.Fatalf("readiness work = %#v error=%v", work, err)
	}
	missing, err := service.GetWorkerReadinessWork(context.Background(), workerID, 5)
	assertFleetFailure(t, err, fleet.FailureConflict)
	if missing.Available {
		t.Fatalf("stale epoch returned readiness work = %#v", missing)
	}
}

func TestFleetReadinessWorkAtomicallyExpiresElapsedCycle(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23020000-0000-0000-0000-000000000021")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/readiness-work-expiry', 6,
			'RECOVERING', 'OFFLINE', 'node-readiness-work-expiry'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed readiness Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	configureTestCapacityPolicies(t, service, poolID)
	if _, err := service.ObserveCapacity(
		context.Background(), capacityObservation(workerID, poolID, 6, 1, 300, true),
	); err != nil {
		t.Fatalf("record readiness capacity: %v", err)
	}
	cycleID := uuid.MustParse("23020000-0000-0000-0000-000000000022")
	if _, err := service.BeginReadiness(context.Background(), fleet.ReadinessRequest{
		CycleID: cycleID, WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 6,
		NodeIdentity: "node-readiness-work-expiry", ExecutionProfileRevisionID: profileID,
		InferenceBackendRevision: "sglang-h3-v3", RequestedBy: "fleet-controller/primary",
		Deadline: time.Now().UTC().Add(time.Hour),
	}); err != nil {
		t.Fatalf("begin readiness: %v", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE worker_readiness_cycles
		SET started_at = clock_timestamp() - interval '2 seconds',
			deadline_at = clock_timestamp() - interval '1 second'
		WHERE id = $1
	`, cycleID); err != nil {
		t.Fatalf("advance readiness cycle beyond its deadline: %v", err)
	}

	work, err := service.GetWorkerReadinessWork(context.Background(), workerID, 6)
	if err != nil {
		t.Fatalf("get elapsed readiness work: %v", err)
	}
	if work.Available {
		t.Fatalf("elapsed readiness cycle returned work = %#v", work)
	}
	expired, err := service.GetReadiness(context.Background(), cycleID)
	if err != nil || expired.State != fleet.ReadinessExpired || expired.NextCheck != "" ||
		expired.WorkerLifecycle != "DRAINING" || expired.WorkerReachability != "SUSPECT" {
		t.Fatalf("elapsed readiness cycle = %#v error=%v", expired, err)
	}
}

func TestFleetReadinessRejectsEvidenceWhileExecutionAuthorityIsActive(t *testing.T) {
	fixture := newAssignmentFixture(t, "fleet-readiness-active-authority", 4)
	enforceFleetProtocol(t, fixture.database.Admin)
	workerID := fixture.worker.ID
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := fixture.candidate.ExecutionProfileRevisionID
	nodeIdentity := "node-fleet-readiness-active-authority"
	if _, err := fixture.database.Admin.Exec(`
		UPDATE workers SET epoch = 5, node_identity = $2 WHERE id = $1
	`, workerID, nodeIdentity); err != nil {
		t.Fatalf("bind readiness Worker identity: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, fixture.database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	configureTestCapacityPolicies(t, service, poolID)
	if _, err := service.ObserveCapacity(
		context.Background(), capacityObservation(workerID, poolID, 5, 1, 300, true),
	); err != nil {
		t.Fatalf("record readiness Worker capacity: %v", err)
	}
	cycleID := uuid.MustParse("23000000-0000-0000-0000-000000000023")
	if _, err := service.BeginReadiness(context.Background(), fleet.ReadinessRequest{
		CycleID: cycleID, WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 5,
		NodeIdentity: nodeIdentity, ExecutionProfileRevisionID: profileID,
		InferenceBackendRevision: "sglang-h3-v3", RequestedBy: "fleet-controller/primary",
		Deadline: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("begin Worker readiness: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE workers
		SET lifecycle_state = 'READY', reachability_condition = 'HEALTHY'
		WHERE id = $1
	`, workerID); err != nil {
		t.Fatalf("prepare concurrent Assignment Worker: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO worker_profile_readiness (
			worker_id, worker_epoch, execution_profile_revision_id, readiness
		) VALUES ($1, 5, $2, 'WARM')
	`, workerID, profileID); err != nil {
		t.Fatalf("prepare concurrent Assignment readiness: %v", err)
	}
	if _, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 5, &fixture.candidate,
	); err != nil {
		t.Fatalf("acquire concurrent readiness Assignment: %v", err)
	}

	_, err = service.ReportReadiness(
		context.Background(), readinessEvidence(cycleID, fleet.ReadinessIdentity, "identity while assigned"),
	)
	assertFleetFailure(t, err, fleet.FailureConflict)
	result, err := service.GetReadiness(context.Background(), cycleID)
	if err != nil || result.State != fleet.ReadinessChecking ||
		result.NextCheck != fleet.ReadinessIdentity || result.WorkerLifecycle != "BUSY" {
		t.Fatalf("readiness after active authority = %#v error=%v", result, err)
	}
}

func TestFleetReadinessRejectsEvidenceWhileCapacityIsBlocked(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000024")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/fleet-readiness-capacity', 6,
			'RECOVERING', 'OFFLINE', 'node-fleet-readiness-capacity'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed capacity-blocked readiness Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	configureTestCapacityPolicies(t, service, poolID)
	if _, err := service.ObserveCapacity(
		context.Background(), capacityObservation(workerID, poolID, 6, 1, 300, true),
	); err != nil {
		t.Fatalf("record initial readiness capacity: %v", err)
	}
	cycleID := uuid.MustParse("23000000-0000-0000-0000-000000000025")
	if _, err := service.BeginReadiness(context.Background(), fleet.ReadinessRequest{
		CycleID: cycleID, WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 6,
		NodeIdentity: "node-fleet-readiness-capacity", ExecutionProfileRevisionID: profileID,
		InferenceBackendRevision: "sglang-h3-v3", RequestedBy: "fleet-controller/primary",
		Deadline: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("begin capacity-blocked readiness: %v", err)
	}
	for _, check := range []fleet.ReadinessCheck{
		fleet.ReadinessIdentity,
		fleet.ReadinessDevice,
		fleet.ReadinessInferenceBackend,
		fleet.ReadinessModelWarmup,
	} {
		if _, err := service.ReportReadiness(
			context.Background(), readinessEvidence(cycleID, check, "capacity "+string(check)),
		); err != nil {
			t.Fatalf("report %s before capacity pressure: %v", check, err)
		}
	}
	if _, err := service.ObserveCapacity(
		context.Background(), capacityObservation(workerID, poolID, 6, 2, 850, true),
	); err != nil {
		t.Fatalf("close readiness capacity: %v", err)
	}

	canary := readinessEvidence(cycleID, fleet.ReadinessCanary, "capacity-gated canary")
	_, err = service.ReportReadiness(context.Background(), canary)
	assertFleetFailure(t, err, fleet.FailureConflict)
	result, err := service.GetReadiness(context.Background(), cycleID)
	if err != nil || result.State != fleet.ReadinessChecking ||
		result.NextCheck != fleet.ReadinessCanary || result.WorkerLifecycle != "WARMING" {
		t.Fatalf("readiness after capacity blocker = %#v error=%v", result, err)
	}

	staleRecovery := capacityObservation(workerID, poolID, 6, 3, 300, true)
	staleRecovery.ObservedAt = time.Now().UTC().Add(-10 * time.Minute)
	staleResult, err := service.ObserveCapacity(context.Background(), staleRecovery)
	if err != nil || staleResult.WorkerState != fleet.CapacityAdmittable ||
		staleResult.PoolState != fleet.CapacityAdmittable ||
		staleResult.WorkerAssignmentAllowed || staleResult.PoolAssignmentAllowed {
		t.Fatalf("stale readiness recovery capacity = %#v error=%v", staleResult, err)
	}
	_, err = service.ReportReadiness(context.Background(), canary)
	assertFleetFailure(t, err, fleet.FailureConflict)

	replayed, err := service.ObserveCapacity(context.Background(), staleRecovery)
	if err != nil || !replayed.Replayed || replayed.WorkerAssignmentAllowed ||
		replayed.PoolAssignmentAllowed {
		t.Fatalf("replayed stale readiness capacity = %#v error=%v", replayed, err)
	}
	freshRecovery := capacityObservation(workerID, poolID, 6, 4, 300, true)
	freshResult, err := service.ObserveCapacity(context.Background(), freshRecovery)
	if err != nil || !freshResult.WorkerAssignmentAllowed ||
		freshResult.PoolAssignmentAllowed {
		t.Fatalf("fresh readiness recovery capacity = %#v error=%v", freshResult, err)
	}
	promoted, err := service.ReportReadiness(context.Background(), canary)
	if err != nil || promoted.State != fleet.ReadinessReady ||
		promoted.WorkerLifecycle != "READY" || promoted.WorkerReachability != "HEALTHY" {
		t.Fatalf("fresh capacity readiness promotion = %#v error=%v", promoted, err)
	}
}

func TestFleetReadinessRejectsEvidenceAfterProfileBecomesIneligible(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000026")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/fleet-readiness-profile', 7,
			'RECOVERING', 'OFFLINE', 'node-fleet-readiness-profile'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed profile-bound readiness Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	configureTestCapacityPolicies(t, service, poolID)
	if _, err := service.ObserveCapacity(
		context.Background(), capacityObservation(workerID, poolID, 7, 1, 300, true),
	); err != nil {
		t.Fatalf("record profile-bound readiness capacity: %v", err)
	}
	cycleID := uuid.MustParse("23000000-0000-0000-0000-000000000027")
	if _, err := service.BeginReadiness(context.Background(), fleet.ReadinessRequest{
		CycleID: cycleID, WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 7,
		NodeIdentity: "node-fleet-readiness-profile", ExecutionProfileRevisionID: profileID,
		InferenceBackendRevision: "sglang-h3-v3", RequestedBy: "fleet-controller/primary",
		Deadline: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("begin profile-bound readiness: %v", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE execution_profile_revisions SET state = 'RETIRED' WHERE id = $1
	`, profileID); err != nil {
		t.Fatalf("retire readiness profile: %v", err)
	}

	_, err = service.ReportReadiness(
		context.Background(), readinessEvidence(cycleID, fleet.ReadinessIdentity, "identity after retirement"),
	)
	assertFleetFailure(t, err, fleet.FailureConflict)
	result, err := service.GetReadiness(context.Background(), cycleID)
	if err != nil || result.State != fleet.ReadinessChecking ||
		result.NextCheck != fleet.ReadinessIdentity || result.WorkerLifecycle != "WARMING" {
		t.Fatalf("readiness after profile retirement = %#v error=%v", result, err)
	}
}

func TestFleetReadinessFailedCheckIsTerminalAndReplaySafe(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000028")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/fleet-readiness-failed', 8,
			'RECOVERING', 'OFFLINE', 'node-fleet-readiness-failed'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed failing readiness Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	configureTestCapacityPolicies(t, service, poolID)
	if _, err := service.ObserveCapacity(
		context.Background(), capacityObservation(workerID, poolID, 8, 1, 300, true),
	); err != nil {
		t.Fatalf("record failing readiness capacity: %v", err)
	}
	cycleID := uuid.MustParse("23000000-0000-0000-0000-000000000029")
	if _, err := service.BeginReadiness(context.Background(), fleet.ReadinessRequest{
		CycleID: cycleID, WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 8,
		NodeIdentity: "node-fleet-readiness-failed", ExecutionProfileRevisionID: profileID,
		InferenceBackendRevision: "sglang-h3-v3", RequestedBy: "fleet-controller/primary",
		Deadline: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("begin failing readiness: %v", err)
	}
	evidence := readinessEvidence(cycleID, fleet.ReadinessIdentity, "identity check failed")
	evidence.Passed = false
	failed, err := service.ReportReadiness(context.Background(), evidence)
	if err != nil || failed.Replayed || failed.State != fleet.ReadinessFailed ||
		failed.NextCheck != "" || failed.WorkerLifecycle != "DRAINING" ||
		failed.WorkerReachability != "SUSPECT" {
		t.Fatalf("failed readiness = %#v error=%v", failed, err)
	}
	replayed, err := service.ReportReadiness(context.Background(), evidence)
	if err != nil || !replayed.Replayed || replayed.State != fleet.ReadinessFailed ||
		replayed.WorkerLifecycle != "DRAINING" {
		t.Fatalf("replayed failed readiness = %#v error=%v", replayed, err)
	}
	conflicting := evidence
	conflicting.ObservedBy = "fleet-controller/conflict"
	_, err = service.ReportReadiness(context.Background(), conflicting)
	assertFleetFailure(t, err, fleet.FailureConflict)
	terminal, err := service.GetReadiness(context.Background(), cycleID)
	if err != nil || terminal.State != fleet.ReadinessFailed || terminal.NextCheck != "" ||
		terminal.WorkerLifecycle != "DRAINING" || terminal.WorkerReachability != "SUSPECT" {
		t.Fatalf("terminal failed readiness = %#v error=%v", terminal, err)
	}
}

func TestFleetReadinessDeadlineExpiryIsTerminalAndReplaySafe(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000030")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/fleet-readiness-expired', 9,
			'RECOVERING', 'OFFLINE', 'node-fleet-readiness-expired'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed expiring readiness Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	configureTestCapacityPolicies(t, service, poolID)
	if _, err := service.ObserveCapacity(
		context.Background(), capacityObservation(workerID, poolID, 9, 1, 300, true),
	); err != nil {
		t.Fatalf("record expiring readiness capacity: %v", err)
	}
	cycleID := uuid.MustParse("23000000-0000-0000-0000-000000000031")
	request := fleet.ReadinessRequest{
		CycleID: cycleID, WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 9,
		NodeIdentity: "node-fleet-readiness-expired", ExecutionProfileRevisionID: profileID,
		InferenceBackendRevision: "sglang-h3-v3", RequestedBy: "fleet-controller/primary",
		Deadline: time.Now().Add(time.Hour),
	}
	if _, err := service.BeginReadiness(context.Background(), request); err != nil {
		t.Fatalf("begin expiring readiness: %v", err)
	}
	if err := database.Admin.QueryRow(`
		UPDATE worker_readiness_cycles
		SET deadline_at = started_at + interval '1 microsecond'
		WHERE id = $1
		RETURNING deadline_at
	`, cycleID).Scan(&request.Deadline); err != nil {
		t.Fatalf("expire readiness cycle: %v", err)
	}
	replayedBegin, err := service.BeginReadiness(context.Background(), request)
	if err != nil || !replayedBegin.Replayed || replayedBegin.State != fleet.ReadinessChecking ||
		replayedBegin.NextCheck != fleet.ReadinessIdentity ||
		replayedBegin.WorkerLifecycle != "WARMING" ||
		replayedBegin.WorkerReachability != "SUSPECT" {
		t.Fatalf("replay readiness begin after deadline = %#v error=%v", replayedBegin, err)
	}
	evidence := readinessEvidence(cycleID, fleet.ReadinessIdentity, "late identity evidence")
	expired, err := service.ReportReadiness(context.Background(), evidence)
	if err != nil || expired.Replayed || expired.State != fleet.ReadinessExpired ||
		expired.NextCheck != "" || expired.WorkerLifecycle != "DRAINING" ||
		expired.WorkerReachability != "SUSPECT" {
		t.Fatalf("expired readiness = %#v error=%v", expired, err)
	}
	replayed, err := service.ReportReadiness(context.Background(), evidence)
	if err != nil || !replayed.Replayed || replayed.State != fleet.ReadinessExpired ||
		replayed.WorkerLifecycle != "DRAINING" || replayed.WorkerReachability != "SUSPECT" {
		t.Fatalf("replayed expired readiness = %#v error=%v", replayed, err)
	}
}

func TestFleetReadinessFencesQuarantineEpochProfileAndBackendIdentity(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000032")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/fleet-readiness-fenced', 10,
			'QUARANTINED', 'SUSPECT', 'node-fleet-readiness-fenced'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed quarantined readiness Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	configureTestCapacityPolicies(t, service, poolID)
	if _, err := service.ObserveCapacity(
		context.Background(), capacityObservation(workerID, poolID, 10, 1, 300, true),
	); err != nil {
		t.Fatalf("record quarantined readiness capacity: %v", err)
	}
	cycleID := uuid.MustParse("23000000-0000-0000-0000-000000000033")
	request := fleet.ReadinessRequest{
		CycleID: cycleID, WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 10,
		NodeIdentity: "node-fleet-readiness-fenced", ExecutionProfileRevisionID: profileID,
		InferenceBackendRevision: "sglang-h3-v3", RequestedBy: "fleet-controller/primary",
		Deadline: time.Now().Add(time.Hour),
	}
	_, err = service.BeginReadiness(context.Background(), request)
	assertFleetFailure(t, err, fleet.FailureConflict)
	if _, err := database.Admin.Exec(`
		UPDATE workers
		SET epoch = 11, lifecycle_state = 'RECOVERING', reachability_condition = 'OFFLINE'
		WHERE id = $1
	`, workerID); err != nil {
		t.Fatalf("advance quarantined Worker epoch: %v", err)
	}
	if _, err := service.ObserveCapacity(
		context.Background(), capacityObservation(workerID, poolID, 11, 2, 300, true),
	); err != nil {
		t.Fatalf("record higher-epoch readiness capacity: %v", err)
	}
	request.WorkerEpoch = 11
	started, err := service.BeginReadiness(context.Background(), request)
	if err != nil || started.State != fleet.ReadinessChecking {
		t.Fatalf("begin higher-epoch readiness = %#v error=%v", started, err)
	}
	conflictingBackend := request
	conflictingBackend.InferenceBackendRevision = "sglang-h3-v4"
	_, err = service.BeginReadiness(context.Background(), conflictingBackend)
	assertFleetFailure(t, err, fleet.FailureConflict)
	conflictingProfile := request
	conflictingProfile.ExecutionProfileRevisionID = uuid.New()
	_, err = service.BeginReadiness(context.Background(), conflictingProfile)
	assertFleetFailure(t, err, fleet.FailureConflict)
	if _, err := database.Admin.Exec(`
		UPDATE workers SET epoch = 12 WHERE id = $1
	`, workerID); err != nil {
		t.Fatalf("advance active readiness Worker epoch: %v", err)
	}
	_, err = service.ReportReadiness(
		context.Background(), readinessEvidence(cycleID, fleet.ReadinessIdentity, "stale epoch identity"),
	)
	assertFleetFailure(t, err, fleet.FailureConflict)
	result, err := service.GetReadiness(context.Background(), cycleID)
	if err != nil || result.State != fleet.ReadinessChecking ||
		result.NextCheck != fleet.ReadinessIdentity || result.WorkerLifecycle != "WARMING" {
		t.Fatalf("stale-epoch readiness = %#v error=%v", result, err)
	}
}

func TestFleetReadinessCannotBeginWithActiveLease(t *testing.T) {
	fixture := newAssignmentFixture(t, "fleet-readiness-begin-active-lease", 13)
	enforceFleetProtocol(t, fixture.database.Admin)
	workerID := fixture.worker.ID
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := fixture.candidate.ExecutionProfileRevisionID
	if _, err := fixture.database.Admin.Exec(`
		UPDATE workers
		SET epoch = 14, node_identity = 'node-fleet-readiness-active-lease'
		WHERE id = $1
	`, workerID); err != nil {
		t.Fatalf("bind active-Lease Worker identity: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, fixture.database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	configureTestCapacityPolicies(t, service, poolID)
	if _, err := service.ObserveCapacity(
		context.Background(), capacityObservation(workerID, poolID, 14, 1, 300, true),
	); err != nil {
		t.Fatalf("record active-Lease Worker capacity: %v", err)
	}
	if _, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 14, &fixture.candidate,
	); err != nil {
		t.Fatalf("acquire readiness-blocking Lease: %v", err)
	}
	_, err = service.BeginReadiness(context.Background(), fleet.ReadinessRequest{
		CycleID:  uuid.MustParse("23000000-0000-0000-0000-000000000034"),
		WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 14,
		NodeIdentity:               "node-fleet-readiness-active-lease",
		ExecutionProfileRevisionID: profileID, InferenceBackendRevision: "sglang-h3-v3",
		RequestedBy: "fleet-controller/primary", Deadline: time.Now().Add(time.Hour),
	})
	assertFleetFailure(t, err, fleet.FailureConflict)
}

func TestFleetDrainRequestCompletesIdleWorkerAndReplaysExactly(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000035")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/fleet-drain-idle', 15,
			'READY', 'HEALTHY', 'node-fleet-drain-idle'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed idle drain Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	request := fleet.DrainRequest{
		OperationID: uuid.MustParse("23000000-0000-0000-0000-000000000036"),
		WorkerID:    workerID, ExpectedEpoch: 15, Reason: "roll out worker-agent revision 24",
		Deadline: time.Now().Add(time.Hour), RequestedBy: "fleet-controller/primary",
	}
	completed, err := service.RequestDrain(context.Background(), request)
	if err != nil || completed.Replayed || completed.State != fleet.DrainComplete ||
		completed.WorkerID != workerID || completed.WorkerEpoch != 15 ||
		completed.WorkerLifecycle != "DRAINING" || completed.WorkerReachability != "HEALTHY" {
		t.Fatalf("request idle Worker drain = %#v error=%v", completed, err)
	}
	replayed, err := service.RequestDrain(context.Background(), request)
	if err != nil || !replayed.Replayed || replayed.State != fleet.DrainComplete ||
		replayed.WorkerID != workerID || replayed.WorkerLifecycle != "DRAINING" {
		t.Fatalf("replay idle Worker drain = %#v error=%v", replayed, err)
	}
	conflicting := request
	conflicting.Reason = "conflicting rollout"
	_, err = service.RequestDrain(context.Background(), conflicting)
	assertFleetFailure(t, err, fleet.FailureConflict)
	stored, err := service.GetDrain(context.Background(), request.OperationID)
	if err != nil || stored.State != fleet.DrainComplete || stored.WorkerID != workerID ||
		stored.WorkerEpoch != 15 || stored.WorkerLifecycle != "DRAINING" {
		t.Fatalf("get idle Worker drain = %#v error=%v", stored, err)
	}
}

func TestFleetDrainPreservesActiveJobUntilNormalCompletion(t *testing.T) {
	fixture := newAssignmentFixture(t, "fleet-drain-active-job", 16)
	enforceFleetProtocol(t, fixture.database.Admin)
	service := newFleetServiceWithFreshAssignmentCapacity(t, fixture, 16)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 16, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create drain-protected Assignment: %v", err)
	}
	request := fleet.DrainRequest{
		OperationID: uuid.MustParse("23000000-0000-0000-0000-000000000037"),
		WorkerID:    fixture.worker.ID, ExpectedEpoch: 16,
		Reason: "routine worker-agent rollout", Deadline: time.Now().Add(time.Hour),
		RequestedBy: "fleet-controller/primary",
	}
	draining, err := service.RequestDrain(context.Background(), request)
	if err != nil || draining.State != fleet.DrainDraining ||
		draining.WorkerLifecycle != "DRAINING" {
		t.Fatalf("request active Worker drain = %#v error=%v", draining, err)
	}
	credentials := workercontrol.LeaseCredentials{
		AttemptID: assignment.AttemptID, WorkerEpoch: assignment.WorkerEpoch,
		Fence: assignment.LeaseFence, Token: assignment.LeaseToken,
	}
	started, err := fixture.service.Start(context.Background(), fixture.worker, credentials)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start during Fleet drain = %#v error=%v", started, err)
	}
	stillDraining, err := service.ReconcileDrain(
		context.Background(), request.OperationID, "fleet-controller/primary",
	)
	if err != nil || stillDraining.State != fleet.DrainDraining ||
		stillDraining.WorkerLifecycle != "DRAINING" {
		t.Fatalf("reconcile active Worker drain = %#v error=%v", stillDraining, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE attempt_leases
		SET revoked_at = clock_timestamp(), updated_at = clock_timestamp()
		WHERE attempt_id = $1
	`, assignment.AttemptID); err != nil {
		t.Fatalf("finish drain-protected Lease: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE attempts
		SET state = 'FAILED', ended_at = clock_timestamp(), updated_at = clock_timestamp()
		WHERE id = $1
	`, assignment.AttemptID); err != nil {
		t.Fatalf("finish drain-protected Attempt: %v", err)
	}
	completed, err := service.ReconcileDrain(
		context.Background(), request.OperationID, "fleet-controller/primary",
	)
	if err != nil || completed.Replayed || completed.State != fleet.DrainComplete ||
		completed.WorkerLifecycle != "DRAINING" {
		t.Fatalf("complete active Worker drain = %#v error=%v", completed, err)
	}
	replayed, err := service.ReconcileDrain(
		context.Background(), request.OperationID, "fleet-controller/secondary",
	)
	if err != nil || !replayed.Replayed || replayed.State != fleet.DrainComplete ||
		replayed.WorkerLifecycle != "DRAINING" {
		t.Fatalf("replay completed Worker drain = %#v error=%v", replayed, err)
	}
}

func TestFleetDrainDeadlineExpiresWithoutFencingActiveJob(t *testing.T) {
	fixture := newAssignmentFixture(t, "fleet-drain-deadline", 17)
	enforceFleetProtocol(t, fixture.database.Admin)
	service := newFleetServiceWithFreshAssignmentCapacity(t, fixture, 17)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 17, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create expiring drain Assignment: %v", err)
	}
	request := fleet.DrainRequest{
		OperationID: uuid.MustParse("23000000-0000-0000-0000-000000000038"),
		WorkerID:    fixture.worker.ID, ExpectedEpoch: 17,
		Reason: "bounded routine rollout", Deadline: time.Now().Add(time.Hour),
		RequestedBy: "fleet-controller/primary",
	}
	if result, err := service.RequestDrain(context.Background(), request); err != nil ||
		result.State != fleet.DrainDraining {
		t.Fatalf("request expiring Worker drain = %#v error=%v", result, err)
	}
	if err := fixture.database.Admin.QueryRow(`
		UPDATE worker_drain_operations
		SET deadline_at = requested_at + interval '1 microsecond'
		WHERE id = $1
		RETURNING deadline_at
	`, request.OperationID).Scan(&request.Deadline); err != nil {
		t.Fatalf("expire Worker drain operation: %v", err)
	}
	replayedRequest, err := service.RequestDrain(context.Background(), request)
	if err != nil || !replayedRequest.Replayed || replayedRequest.State != fleet.DrainDraining ||
		replayedRequest.WorkerID != fixture.worker.ID || replayedRequest.WorkerEpoch != 17 {
		t.Fatalf("replay Worker drain request after deadline = %#v error=%v", replayedRequest, err)
	}
	expired, err := service.ReconcileDrain(
		context.Background(), request.OperationID, "fleet-controller/primary",
	)
	if err != nil || expired.Replayed || expired.State != fleet.DrainExpired ||
		expired.WorkerLifecycle != "DRAINING" {
		t.Fatalf("expire active Worker drain = %#v error=%v", expired, err)
	}
	credentials := workercontrol.LeaseCredentials{
		AttemptID: assignment.AttemptID, WorkerEpoch: assignment.WorkerEpoch,
		Fence: assignment.LeaseFence, Token: assignment.LeaseToken,
	}
	started, err := fixture.service.Start(context.Background(), fixture.worker, credentials)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start after routine drain expiry = %#v error=%v", started, err)
	}
	replayed, err := service.ReconcileDrain(
		context.Background(), request.OperationID, "fleet-controller/secondary",
	)
	if err != nil || !replayed.Replayed || replayed.State != fleet.DrainExpired ||
		replayed.WorkerLifecycle != "DRAINING" {
		t.Fatalf("replay expired Worker drain = %#v error=%v", replayed, err)
	}
}

func TestFleetDrainRejectsStaleWorkerEpoch(t *testing.T) {
	fixture := newAssignmentFixture(t, "fleet-drain-stale-epoch", 18)
	enforceFleetProtocol(t, fixture.database.Admin)
	service := newFleetServiceWithFreshAssignmentCapacity(t, fixture, 18)
	if _, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 18, &fixture.candidate,
	); err != nil {
		t.Fatalf("create stale-epoch drain Assignment: %v", err)
	}
	request := fleet.DrainRequest{
		OperationID: uuid.MustParse("23000000-0000-0000-0000-000000000039"),
		WorkerID:    fixture.worker.ID, ExpectedEpoch: 17,
		Reason: "stale epoch rollout", Deadline: time.Now().Add(time.Hour),
		RequestedBy: "fleet-controller/primary",
	}
	_, err := service.RequestDrain(context.Background(), request)
	assertFleetFailure(t, err, fleet.FailureConflict)
	request.OperationID = uuid.MustParse("23000000-0000-0000-0000-000000000040")
	request.ExpectedEpoch = 18
	if result, err := service.RequestDrain(context.Background(), request); err != nil ||
		result.State != fleet.DrainDraining {
		t.Fatalf("request current-epoch Worker drain = %#v error=%v", result, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE workers SET epoch = 19 WHERE id = $1
	`, fixture.worker.ID); err != nil {
		t.Fatalf("advance draining Worker epoch: %v", err)
	}
	_, err = service.ReconcileDrain(
		context.Background(), request.OperationID, "fleet-controller/primary",
	)
	assertFleetFailure(t, err, fleet.FailureConflict)
	stored, err := service.GetDrain(context.Background(), request.OperationID)
	if err != nil || stored.State != fleet.DrainDraining || stored.WorkerEpoch != 18 ||
		stored.WorkerLifecycle != "DRAINING" {
		t.Fatalf("stale-epoch Worker drain = %#v error=%v", stored, err)
	}
}

func TestFleetMutationAuthorizationRequiresCompletedExactWorkerDrain(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000041")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/fleet-mutation', 20,
			'READY', 'HEALTHY', 'node-fleet-mutation'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed mutation Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	drainID := uuid.MustParse("23000000-0000-0000-0000-000000000042")
	if result, err := service.RequestDrain(context.Background(), fleet.DrainRequest{
		OperationID: drainID, WorkerID: workerID, ExpectedEpoch: 20,
		Reason: "replace protected Worker Pod", Deadline: time.Now().Add(time.Hour),
		RequestedBy: "fleet-controller/primary",
	}); err != nil || result.State != fleet.DrainComplete {
		t.Fatalf("complete mutation Worker drain = %#v error=%v", result, err)
	}
	digest := sha256.Sum256([]byte("normalized pod delete request"))
	request := fleet.MutationAuthorizationRequest{
		RequestUID: "admission-request-0001", ActorIdentity: "fleet-controller/primary",
		ResourceKind: fleet.ProtectedPod, Operation: fleet.MutationDelete,
		KubernetesUID: "kubernetes-worker-pod-uid-1", Namespace: "vela-workers",
		Name: "h3-worker-node-fleet-mutation", WorkerPoolID: poolID,
		WorkerID: workerID, WorkerEpoch: 20, DrainOperationIDs: []uuid.UUID{drainID},
		RequestDigest: digest[:],
	}
	authorized, err := service.AuthorizeMutation(context.Background(), request)
	if err != nil || authorized.Replayed || !authorized.Authorized ||
		authorized.RequestUID != request.RequestUID {
		t.Fatalf("authorize protected Pod mutation = %#v error=%v", authorized, err)
	}
	replayed, err := service.AuthorizeMutation(context.Background(), request)
	if err != nil || !replayed.Replayed || !replayed.Authorized ||
		replayed.RequestUID != request.RequestUID {
		t.Fatalf("replay protected Pod mutation = %#v error=%v", replayed, err)
	}
	retirementRequest := fleet.RetirementAuthorizationRequest{
		ResourceKind: request.ResourceKind, KubernetesUID: request.KubernetesUID,
		Namespace: request.Namespace, Name: request.Name, WorkerPoolID: request.WorkerPoolID,
		WorkerID: request.WorkerID, WorkerEpoch: request.WorkerEpoch,
		DrainOperationIDs: append([]uuid.UUID(nil), request.DrainOperationIDs...),
	}
	if authorized, err := service.HasRetirementAuthorization(
		context.Background(), retirementRequest,
	); err != nil || authorized {
		t.Fatalf("DELETE-only Pod retirement authorization = %t error=%v", authorized, err)
	}
	completionRequest := fleet.RetirementCompletionRequest{
		RetirementAuthorizationRequest: retirementRequest,
		ObservedBy:                     "fleet-controller/primary",
	}
	_, err = service.RecordRetirementCompletion(context.Background(), completionRequest)
	assertFleetFailure(t, err, fleet.FailureConflict)
	finalizerRequest := request
	finalizerRequest.RequestUID = "admission-request-0001-finalizer"
	finalizerRequest.Operation = fleet.MutationRemoveFinalizer
	finalizerDigest := sha256.Sum256([]byte("normalized pod finalizer removal request"))
	finalizerRequest.RequestDigest = finalizerDigest[:]
	if finalized, err := service.AuthorizeMutation(
		context.Background(), finalizerRequest,
	); err != nil || !finalized.Authorized {
		t.Fatalf("authorize protected Pod finalizer removal = %#v error=%v", finalized, err)
	}
	if authorized, err := service.HasRetirementAuthorization(
		context.Background(), retirementRequest,
	); err != nil || !authorized {
		t.Fatalf("complete Pod retirement authorization = %t error=%v", authorized, err)
	}
	completion, err := service.RecordRetirementCompletion(context.Background(), completionRequest)
	if err != nil || completion.Replayed || completion.CompletedAt.IsZero() {
		t.Fatalf("record Pod retirement completion = %#v error=%v", completion, err)
	}
	replayedCompletion, err := service.RecordRetirementCompletion(
		context.Background(), completionRequest,
	)
	if err != nil || !replayedCompletion.Replayed ||
		!replayedCompletion.CompletedAt.Equal(completion.CompletedAt) {
		t.Fatalf("replay Pod retirement completion = %#v error=%v", replayedCompletion, err)
	}
	if completed, err := service.HasRetirementCompletion(
		context.Background(), retirementRequest,
	); err != nil || !completed {
		t.Fatalf("lookup Pod retirement completion = %t error=%v", completed, err)
	}
	mismatchedRetirement := retirementRequest
	mismatchedRetirement.WorkerEpoch++
	if authorized, err := service.HasRetirementAuthorization(
		context.Background(), mismatchedRetirement,
	); err != nil || authorized {
		t.Fatalf("mismatched Pod retirement authorization = %t error=%v", authorized, err)
	}
	if completed, err := service.HasRetirementCompletion(
		context.Background(), mismatchedRetirement,
	); err != nil || completed {
		t.Fatalf("mismatched Pod retirement completion = %t error=%v", completed, err)
	}
	conflicting := request
	conflictingDigest := sha256.Sum256([]byte("conflicting pod delete request"))
	conflicting.RequestDigest = conflictingDigest[:]
	_, err = service.AuthorizeMutation(context.Background(), conflicting)
	assertFleetFailure(t, err, fleet.FailureConflict)

	_, err = database.Admin.Exec(`
		UPDATE fleet_mutation_authorizations
		SET actor_identity = 'fleet-controller/rewritten'
		WHERE request_uid = $1
	`, request.RequestUID)
	assertFleetSQLState(t, err, "55000")
	_, err = database.Admin.Exec(`
		DELETE FROM fleet_mutation_authorizations WHERE request_uid = $1
	`, request.RequestUID)
	assertFleetSQLState(t, err, "55000")
	_, err = database.Admin.Exec(`
		UPDATE fleet_retirement_completions
		SET observed_by = 'fleet-controller/rewritten'
		WHERE resource_kind = $1 AND kubernetes_uid = $2
	`, retirementRequest.ResourceKind, retirementRequest.KubernetesUID)
	assertFleetSQLState(t, err, "55000")
	_, err = database.Admin.Exec(`
		DELETE FROM fleet_retirement_completions
		WHERE resource_kind = $1 AND kubernetes_uid = $2
	`, retirementRequest.ResourceKind, retirementRequest.KubernetesUID)
	assertFleetSQLState(t, err, "55000")
}

func TestFleetMutationAuthorizationRequiresCompleteCurrentPoolDrainSet(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	workers := []struct {
		id      uuid.UUID
		epoch   int64
		spiffe  string
		node    string
		drainID uuid.UUID
	}{
		{
			id: uuid.MustParse("23000000-0000-0000-0000-000000000043"), epoch: 21,
			spiffe:  "spiffe://vela.internal/worker/fleet-pool-mutation-1",
			node:    "node-fleet-pool-mutation-1",
			drainID: uuid.MustParse("23000000-0000-0000-0000-000000000045"),
		},
		{
			id: uuid.MustParse("23000000-0000-0000-0000-000000000044"), epoch: 22,
			spiffe:  "spiffe://vela.internal/worker/fleet-pool-mutation-2",
			node:    "node-fleet-pool-mutation-2",
			drainID: uuid.MustParse("23000000-0000-0000-0000-000000000046"),
		},
	}
	for _, worker := range workers {
		if _, err := database.Admin.Exec(`
			INSERT INTO workers (
				id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
				reachability_condition, node_identity
			) VALUES ($1, $2, $3, $4, 'READY', 'HEALTHY', $5)
		`, worker.id, poolID, worker.spiffe, worker.epoch, worker.node); err != nil {
			t.Fatalf("seed pool mutation Worker %s: %v", worker.id, err)
		}
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	for _, worker := range workers {
		if result, err := service.RequestDrain(context.Background(), fleet.DrainRequest{
			OperationID: worker.drainID, WorkerID: worker.id, ExpectedEpoch: worker.epoch,
			Reason: "replace protected pool revision", Deadline: time.Now().Add(time.Hour),
			RequestedBy: "fleet-controller/primary",
		}); err != nil || result.State != fleet.DrainComplete {
			t.Fatalf("complete pool Worker %s drain = %#v error=%v", worker.id, result, err)
		}
	}
	digest := sha256.Sum256([]byte("normalized daemonset image patch"))
	request := fleet.MutationAuthorizationRequest{
		RequestUID:    "admission-request-pool-0001",
		ActorIdentity: "fleet-controller/primary", ResourceKind: fleet.ProtectedDaemonSet,
		Operation: fleet.MutationPatchImage, KubernetesUID: "kubernetes-daemonset-uid-1",
		Namespace: "vela-workers", Name: "h3-worker-pool-primary", WorkerPoolID: poolID,
		DrainOperationIDs: []uuid.UUID{workers[0].drainID, workers[1].drainID},
		RequestDigest:     digest[:],
	}
	authorized, err := service.AuthorizeMutation(context.Background(), request)
	if err != nil || authorized.Replayed || !authorized.Authorized {
		t.Fatalf("authorize complete pool mutation = %#v error=%v", authorized, err)
	}
	deleteRequest := request
	deleteRequest.RequestUID = "admission-request-pool-delete"
	deleteRequest.Operation = fleet.MutationDelete
	deleteDigest := sha256.Sum256([]byte("normalized daemonset delete"))
	deleteRequest.RequestDigest = deleteDigest[:]
	if deleted, err := service.AuthorizeMutation(
		context.Background(), deleteRequest,
	); err != nil || !deleted.Authorized {
		t.Fatalf("authorize complete pool delete = %#v error=%v", deleted, err)
	}
	finalizerRequest := deleteRequest
	finalizerRequest.RequestUID = "admission-request-pool-finalizer"
	finalizerRequest.Operation = fleet.MutationRemoveFinalizer
	finalizerDigest := sha256.Sum256([]byte("normalized daemonset finalizer removal"))
	finalizerRequest.RequestDigest = finalizerDigest[:]
	if finalized, err := service.AuthorizeMutation(
		context.Background(), finalizerRequest,
	); err != nil || !finalized.Authorized {
		t.Fatalf("authorize complete pool finalizer removal = %#v error=%v", finalized, err)
	}
	retirementRequest := fleet.RetirementAuthorizationRequest{
		ResourceKind: request.ResourceKind, KubernetesUID: request.KubernetesUID,
		Namespace: request.Namespace, Name: request.Name, WorkerPoolID: request.WorkerPoolID,
		DrainOperationIDs: []uuid.UUID{workers[1].drainID, workers[0].drainID},
	}
	if retirementAuthorized, err := service.HasRetirementAuthorization(
		context.Background(), retirementRequest,
	); err != nil || !retirementAuthorized {
		t.Fatalf("complete pool retirement authorization = %t error=%v", retirementAuthorized, err)
	}
	retirementRequest.KubernetesUID = "kubernetes-daemonset-uid-conflict"
	if retirementAuthorized, err := service.HasRetirementAuthorization(
		context.Background(), retirementRequest,
	); err != nil || retirementAuthorized {
		t.Fatalf("mismatched pool retirement authorization = %t error=%v", retirementAuthorized, err)
	}
	missing := request
	missing.RequestUID = "admission-request-pool-0002"
	missing.DrainOperationIDs = missing.DrainOperationIDs[:1]
	_, err = service.AuthorizeMutation(context.Background(), missing)
	assertFleetFailure(t, err, fleet.FailureConflict)
}

func readinessEvidence(
	cycleID uuid.UUID,
	check fleet.ReadinessCheck,
	detail string,
) fleet.ReadinessEvidence {
	digest := sha256.Sum256([]byte(detail))
	return fleet.ReadinessEvidence{
		CycleID: cycleID, Check: check, Passed: true, EvidenceDigest: digest[:],
		ObservedBy: "fleet-controller/primary",
	}
}

func TestFleetCapacityIsRecheckedInsideAssignmentTransaction(t *testing.T) {
	fixture := newAssignmentFixture(t, "fleet-capacity-assignment", 7)
	enforceFleetProtocol(t, fixture.database.Admin)
	service, err := fleet.NewService(newRolePool(
		t, fixture.database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	configureTestCapacityPolicies(t, service, poolID)
	blocked := capacityObservation(fixture.worker.ID, poolID, 7, 1, 850, true)
	if _, err := service.ObserveCapacity(context.Background(), blocked); err != nil {
		t.Fatalf("block Assignment Worker: %v", err)
	}

	_, err = fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	var unavailable *workercontrol.Failure
	if !errors.As(err, &unavailable) || unavailable.Code != workercontrol.FailureWorkerUnavailable {
		t.Fatalf("blocked Worker Acquire error = %v, want worker_unavailable", err)
	}

	recovered := capacityObservation(fixture.worker.ID, poolID, 7, 2, 400, true)
	if _, err := service.ObserveCapacity(context.Background(), recovered); err != nil {
		t.Fatalf("recover Assignment Worker: %v", err)
	}
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil || assignment.AttemptID == uuid.Nil || assignment.JobID != fixture.candidate.JobID {
		t.Fatalf("recovered Worker Acquire = %#v error=%v", assignment, err)
	}
}

func TestFleetCapacityMissingObservationFailsClosedInsideAssignment(t *testing.T) {
	fixture := newAssignmentFixture(t, "fleet-capacity-missing", 7)
	enforceFleetProtocol(t, fixture.database.Admin)

	_, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	var unavailable *workercontrol.Failure
	if !errors.As(err, &unavailable) || unavailable.Code != workercontrol.FailureWorkerUnavailable {
		t.Fatalf("missing-capacity Worker Acquire error = %v, want worker_unavailable", err)
	}
}

func TestFleetCapacityStaleObservationFailsClosedInsideAssignmentUntilFreshReport(t *testing.T) {
	fixture := newAssignmentFixture(t, "fleet-capacity-stale-assignment", 8)
	enforceFleetProtocol(t, fixture.database.Admin)
	service, err := fleet.NewService(newRolePool(
		t, fixture.database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	configureTestCapacityPolicies(t, service, poolID)
	stale := capacityObservation(fixture.worker.ID, poolID, 8, 1, 300, true)
	stale.ObservedAt = time.Now().UTC().Add(-10 * time.Minute)
	if _, err := service.ObserveCapacity(context.Background(), stale); err != nil {
		t.Fatalf("record stale Assignment capacity: %v", err)
	}

	_, err = fixture.service.Acquire(
		context.Background(), fixture.worker, 8, &fixture.candidate,
	)
	var unavailable *workercontrol.Failure
	if !errors.As(err, &unavailable) || unavailable.Code != workercontrol.FailureWorkerUnavailable {
		t.Fatalf("stale-capacity Worker Acquire error = %v, want worker_unavailable", err)
	}

	fresh := capacityObservation(fixture.worker.ID, poolID, 8, 2, 300, true)
	if _, err := service.ObserveCapacity(context.Background(), fresh); err != nil {
		t.Fatalf("record fresh Assignment capacity: %v", err)
	}
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 8, &fixture.candidate,
	)
	if err != nil || assignment.AttemptID == uuid.Nil || assignment.JobID != fixture.candidate.JobID {
		t.Fatalf("fresh-capacity Worker Acquire = %#v error=%v", assignment, err)
	}
}

func TestFleetCapacityMissingPoolConditionFailsClosed(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	var allowed bool
	if err := database.Admin.QueryRow(`
		SELECT vela_pool_capacity_allows_assignment(
			'00000000-0000-0000-0000-000000000005'
		)
	`).Scan(&allowed); err != nil {
		t.Fatalf("read missing pool capacity authority: %v", err)
	}
	if allowed {
		t.Fatal("missing Worker pool capacity condition allowed Assignment")
	}
}

func TestFleetCapacityPolicyAcceptsExactReplayAndRejectsConflictingRevision(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	policy := fleet.CapacityPolicy{
		WorkerPoolID:             uuid.MustParse("00000000-0000-0000-0000-000000000005"),
		Revision:                 strings.Repeat("a", 64),
		WorkerHighWatermarkBytes: 800, WorkerLowWatermarkBytes: 400,
		WorkerCriticalFreeBytes: 50, PoolHighWatermarkBytes: 1200,
		PoolLowWatermarkBytes: 800, ObservationMaxAge: 2 * time.Minute,
		ConfiguredBy: "fleet-controller/primary",
	}

	configured, err := service.ConfigureCapacityPolicy(context.Background(), policy)
	if err != nil || configured.WorkerPoolID != policy.WorkerPoolID ||
		configured.Revision != policy.Revision || configured.Replayed {
		t.Fatalf("configure Fleet capacity policy = %#v error=%v", configured, err)
	}
	replayed, err := service.ConfigureCapacityPolicy(context.Background(), policy)
	if err != nil || !replayed.Replayed || replayed.WorkerPoolID != policy.WorkerPoolID ||
		replayed.Revision != policy.Revision {
		t.Fatalf("replay Fleet capacity policy = %#v error=%v", replayed, err)
	}
	conflicting := policy
	conflicting.PoolLowWatermarkBytes--
	_, err = service.ConfigureCapacityPolicy(context.Background(), conflicting)
	assertFleetFailure(t, err, fleet.FailureConflict)
}

func TestFleetCapacityPoolUsesAggregateHighLowHysteresis(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	workers := []uuid.UUID{
		uuid.MustParse("23000000-0000-0000-0000-000000000041"),
		uuid.MustParse("23000000-0000-0000-0000-000000000042"),
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES
			($1, $3, 'spiffe://vela.internal/worker/fleet-aggregate-a', 1,
			 'READY', 'HEALTHY', 'node-fleet-aggregate-a'),
			($2, $3, 'spiffe://vela.internal/worker/fleet-aggregate-b', 1,
			 'READY', 'HEALTHY', 'node-fleet-aggregate-b')
	`, workers[0], workers[1], poolID); err != nil {
		t.Fatalf("seed aggregate-capacity Workers: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	policy := testCapacityPolicy(poolID)
	if _, err := service.ConfigureCapacityPolicy(context.Background(), policy); err != nil {
		t.Fatalf("configure aggregate capacity policy: %v", err)
	}

	first := capacityObservation(workers[0], poolID, 1, 1, 700, true)
	result, err := service.ObserveCapacity(context.Background(), first)
	if err != nil || result.WorkerState != fleet.CapacityAdmittable ||
		result.PoolState != fleet.CapacityAdmittable || result.PoolAssignmentAllowed {
		t.Fatalf("first aggregate observation = %#v error=%v", result, err)
	}
	second := capacityObservation(workers[1], poolID, 1, 1, 700, true)
	result, err = service.ObserveCapacity(context.Background(), second)
	if err != nil || result.WorkerState != fleet.CapacityAdmittable ||
		result.PoolState != fleet.CapacityScratchPressured || result.PoolAssignmentAllowed {
		t.Fatalf("aggregate high-watermark observation = %#v error=%v", result, err)
	}

	between := capacityObservation(workers[0], poolID, 1, 2, 300, true)
	result, err = service.ObserveCapacity(context.Background(), between)
	if err != nil || result.PoolState != fleet.CapacityScratchPressured ||
		result.PoolAssignmentAllowed {
		t.Fatalf("aggregate hysteresis observation = %#v error=%v", result, err)
	}
	recovered := capacityObservation(workers[1], poolID, 1, 2, 400, true)
	result, err = service.ObserveCapacity(context.Background(), recovered)
	if err != nil || result.PoolState != fleet.CapacityAdmittable ||
		!result.PoolAssignmentAllowed {
		t.Fatalf("aggregate low-watermark recovery = %#v error=%v", result, err)
	}
}

func TestFleetCapacityKeepsWorkerWatermarkStateSeparateFromPoolBlockers(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000071")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/fleet-separate-blockers', 1,
			'READY', 'HEALTHY', 'node-fleet-separate-blockers'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed capacity Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	policy := testCapacityPolicy(poolID)
	policy.PoolHighWatermarkBytes = 800
	policy.PoolLowWatermarkBytes = 400
	if _, err := service.ConfigureCapacityPolicy(context.Background(), policy); err != nil {
		t.Fatalf("configure capacity policy: %v", err)
	}
	observation := capacityObservation(workerID, poolID, 1, 1, 850, false)
	observation.WatermarkState = fleet.ScratchWatermarkPressured

	result, err := service.ObserveCapacity(context.Background(), observation)
	if err != nil || result.WorkerState != fleet.CapacityScratchPressured ||
		result.PoolState != fleet.CapacityMultipleBlockers ||
		result.WorkerAssignmentAllowed || result.PoolAssignmentAllowed {
		t.Fatalf("pressure plus storage capacity = %#v error=%v", result, err)
	}
	replayed, err := service.ObserveCapacity(context.Background(), observation)
	if err != nil || !replayed.Replayed || replayed.WorkerState != result.WorkerState ||
		replayed.PoolState != result.PoolState {
		t.Fatalf("replayed pressure plus storage capacity = %#v error=%v", replayed, err)
	}
	contradictory := observation
	contradictory.Sequence++
	contradictory.WatermarkState = fleet.ScratchWatermarkNormal
	_, err = service.ObserveCapacity(context.Background(), contradictory)
	assertFleetFailure(t, err, fleet.FailureInvalid)
}

func TestFleetCapacityStaleDrainedWorkerDoesNotCloseAvailablePool(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	readyWorkerID := uuid.MustParse("23000000-0000-0000-0000-000000000061")
	drainingWorkerID := uuid.MustParse("23000000-0000-0000-0000-000000000062")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES
			($1, $3, 'spiffe://vela.internal/worker/fleet-fresh-ready', 1,
			 'READY', 'HEALTHY', 'node-fleet-fresh-ready'),
			($2, $3, 'spiffe://vela.internal/worker/fleet-stale-draining', 1,
			 'WARMING', 'SUSPECT', 'node-fleet-stale-draining')
	`, readyWorkerID, drainingWorkerID, poolID); err != nil {
		t.Fatalf("seed available and draining capacity Workers: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	if _, err := service.ConfigureCapacityPolicy(
		context.Background(), testCapacityPolicy(poolID),
	); err != nil {
		t.Fatalf("configure drained-Worker capacity policy: %v", err)
	}
	if result, err := service.ObserveCapacity(
		context.Background(),
		capacityObservation(readyWorkerID, poolID, 1, 1, 300, true),
	); err != nil || !result.PoolAssignmentAllowed {
		t.Fatalf("fresh available Worker capacity = %#v error=%v", result, err)
	}
	stale := capacityObservation(drainingWorkerID, poolID, 1, 1, 300, true)
	stale.ObservedAt = time.Now().UTC().Add(-10 * time.Minute)
	if _, err := service.ObserveCapacity(context.Background(), stale); err != nil {
		t.Fatalf("record stale non-ready Worker capacity: %v", err)
	}
	if drain, err := service.RequestDrain(context.Background(), fleet.DrainRequest{
		OperationID: uuid.MustParse("23000000-0000-0000-0000-000000000063"),
		WorkerID:    drainingWorkerID, ExpectedEpoch: 1,
		Reason: "retire stale non-ready Worker", Deadline: time.Now().Add(time.Hour),
		RequestedBy: "fleet-controller/primary",
	}); err != nil || drain.State != fleet.DrainComplete || drain.WorkerLifecycle != "DRAINING" {
		t.Fatalf("drain stale non-ready Worker = %#v error=%v", drain, err)
	}
	result, err := service.ObserveCapacity(
		context.Background(),
		capacityObservation(readyWorkerID, poolID, 1, 2, 300, true),
	)
	if err != nil || result.PoolState != fleet.CapacityAdmittable ||
		!result.PoolAssignmentAllowed {
		t.Fatalf("pool after stale Worker drained = %#v error=%v", result, err)
	}
}

func TestFleetCapacityPoolFailsClosedWhenCurrentReadyWorkerHasNoObservation(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	observedWorkerID := uuid.MustParse("23000000-0000-0000-0000-000000000051")
	missingWorkerID := uuid.MustParse("23000000-0000-0000-0000-000000000052")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES
			($1, $3, 'spiffe://vela.internal/worker/fleet-observed-ready', 1,
			 'READY', 'HEALTHY', 'node-fleet-observed-ready'),
			($2, $3, 'spiffe://vela.internal/worker/fleet-missing-ready', 1,
			 'READY', 'HEALTHY', 'node-fleet-missing-ready')
	`, observedWorkerID, missingWorkerID, poolID); err != nil {
		t.Fatalf("seed current READY capacity Workers: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	if _, err := service.ConfigureCapacityPolicy(
		context.Background(), testCapacityPolicy(poolID),
	); err != nil {
		t.Fatalf("configure missing-observation capacity policy: %v", err)
	}

	result, err := service.ObserveCapacity(
		context.Background(),
		capacityObservation(observedWorkerID, poolID, 1, 1, 300, true),
	)
	if err != nil || result.WorkerState != fleet.CapacityAdmittable ||
		!result.WorkerAssignmentAllowed || result.PoolAssignmentAllowed {
		t.Fatalf("pool with missing current READY observation = %#v error=%v", result, err)
	}
	pool, err := service.GetPoolCapacity(context.Background(), poolID)
	if err != nil || pool.PoolState != fleet.CapacityAdmittable || pool.PoolAssignmentAllowed {
		t.Fatalf("stored pool with missing current READY observation = %#v error=%v", pool, err)
	}
}

func testCapacityPolicy(poolID uuid.UUID) fleet.CapacityPolicy {
	return fleet.CapacityPolicy{
		WorkerPoolID: poolID, Revision: strings.Repeat("a", 64),
		WorkerHighWatermarkBytes: 800, WorkerLowWatermarkBytes: 400,
		WorkerCriticalFreeBytes: 50, PoolHighWatermarkBytes: 1200,
		PoolLowWatermarkBytes: 800, ObservationMaxAge: 2 * time.Minute,
		ConfiguredBy: "fleet-controller/primary",
	}
}

func configureTestCapacityPolicies(
	t *testing.T,
	service *fleet.Service,
	poolIDs ...uuid.UUID,
) {
	t.Helper()
	for _, poolID := range poolIDs {
		if _, err := service.ConfigureCapacityPolicy(
			context.Background(), testCapacityPolicy(poolID),
		); err != nil {
			t.Fatalf("configure test capacity policy for WorkerPool %s: %v", poolID, err)
		}
	}
}

func newFleetServiceWithFreshAssignmentCapacity(
	t *testing.T,
	fixture assignmentFixture,
	workerEpoch int64,
) *fleet.Service {
	t.Helper()
	service, err := fleet.NewService(newRolePool(
		t, fixture.database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	configureTestCapacityPolicies(t, service, poolID)
	if _, err := service.ObserveCapacity(
		context.Background(),
		capacityObservation(fixture.worker.ID, poolID, workerEpoch, 1, 300, true),
	); err != nil {
		t.Fatalf("record fresh Assignment capacity: %v", err)
	}
	return service
}

func TestFleetCapacityConditionBlocksOnlyAffectedPoolAdmission(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	primaryWorker := uuid.MustParse("23000000-0000-0000-0000-000000000011")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/fleet-admission-primary', 1,
			'READY', 'HEALTHY', 'node-fleet-admission-primary'
		)
	`, primaryWorker); err != nil {
		t.Fatalf("seed primary Admission Worker: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_profile_readiness (
			worker_id, worker_epoch, execution_profile_revision_id, readiness
		) VALUES ($1, 1, '00000000-0000-0000-0000-000000000014', 'WARM')
	`, primaryWorker); err != nil {
		t.Fatalf("seed primary profile readiness: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	configureTestCapacityPolicies(
		t, service, uuid.MustParse("00000000-0000-0000-0000-000000000005"),
	)
	server := schedulerAdmissionServerForDatabase(t, database)
	missing := submitJob(t, server.URL, "fleet-capacity-missing-admission", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"reject missing capacity before Admission"
	}`))
	if missing.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("missing-capacity Admission status = %d, want 503; body=%s", missing.StatusCode, missing.Body)
	}
	assertNoAdmissionEffects(t, database.Admin)

	stale := capacityObservation(
		primaryWorker,
		uuid.MustParse("00000000-0000-0000-0000-000000000005"),
		1, 1, 300, true,
	)
	stale.ObservedAt = time.Now().UTC().Add(-10 * time.Minute)
	if _, err := service.ObserveCapacity(context.Background(), stale); err != nil {
		t.Fatalf("record stale primary Worker capacity: %v", err)
	}
	staleAdmission := submitJob(t, server.URL, "fleet-capacity-stale-admission", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"reject stale capacity before Admission"
	}`))
	if staleAdmission.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("stale-capacity Admission status = %d, want 503; body=%s", staleAdmission.StatusCode, staleAdmission.Body)
	}
	assertNoAdmissionEffects(t, database.Admin)

	blocked := capacityObservation(
		primaryWorker,
		uuid.MustParse("00000000-0000-0000-0000-000000000005"),
		1, 2, 850, true,
	)
	if _, err := service.ObserveCapacity(context.Background(), blocked); err != nil {
		t.Fatalf("block primary Worker pool: %v", err)
	}

	deferred := submitJob(t, server.URL, "fleet-capacity-retry", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"wait for an unaffected Worker pool"
	}`))
	if deferred.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("blocked-pool Admission status = %d, want 503; body=%s", deferred.StatusCode, deferred.Body)
	}
	assertNoAdmissionEffects(t, database.Admin)

	seedSecondProjectAndPool(t, database.Admin)
	configureTestCapacityPolicies(
		t, service, uuid.MustParse("00000000-0000-0000-0000-000000000105"),
	)
	secondaryWorker := uuid.MustParse("23000000-0000-0000-0000-000000000012")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000105',
			'spiffe://vela.internal/worker/fleet-admission-secondary', 1,
			'READY', 'HEALTHY', 'node-fleet-admission-secondary'
		)
	`, secondaryWorker); err != nil {
		t.Fatalf("seed secondary Admission Worker: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_profile_readiness (
			worker_id, worker_epoch, execution_profile_revision_id, readiness
		) VALUES ($1, 1, '00000000-0000-0000-0000-000000000106', 'WARM')
	`, secondaryWorker); err != nil {
		t.Fatalf("seed secondary profile readiness: %v", err)
	}
	healthy := capacityObservation(
		secondaryWorker,
		uuid.MustParse("00000000-0000-0000-0000-000000000105"),
		1, 1, 300, true,
	)
	if _, err := service.ObserveCapacity(context.Background(), healthy); err != nil {
		t.Fatalf("open secondary Worker pool: %v", err)
	}
	accepted := submitJob(t, server.URL, "fleet-capacity-retry", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"wait for an unaffected Worker pool"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("unaffected-pool Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
}

func TestFleetCapacityHysteresisIsolatesPoolAndRequiresLowWatermarkRecovery(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedSecondProjectAndPool(t, database.Admin)

	primaryWorker := uuid.MustParse("23000000-0000-0000-0000-000000000001")
	secondaryWorker := uuid.MustParse("23000000-0000-0000-0000-000000000002")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES
			($1, '00000000-0000-0000-0000-000000000005',
			 'spiffe://vela.internal/worker/fleet-primary', 1, 'READY', 'HEALTHY',
			 'node-fleet-primary'),
			($2, '00000000-0000-0000-0000-000000000105',
			 'spiffe://vela.internal/worker/fleet-secondary', 1, 'READY', 'HEALTHY',
			 'node-fleet-secondary')
	`, primaryWorker, secondaryWorker); err != nil {
		t.Fatalf("seed Fleet Workers: %v", err)
	}

	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	configureTestCapacityPolicies(
		t,
		service,
		uuid.MustParse("00000000-0000-0000-0000-000000000005"),
		uuid.MustParse("00000000-0000-0000-0000-000000000105"),
	)

	primary := capacityObservation(
		primaryWorker,
		uuid.MustParse("00000000-0000-0000-0000-000000000005"),
		1, 1, 300, true,
	)
	result, err := service.ObserveCapacity(context.Background(), primary)
	if err != nil || result.Replayed || result.WorkerState != fleet.CapacityAdmittable ||
		result.PoolState != fleet.CapacityAdmittable || !result.WorkerAssignmentAllowed ||
		!result.PoolAssignmentAllowed {
		t.Fatalf("initial primary capacity = %#v error=%v", result, err)
	}
	secondary := capacityObservation(
		secondaryWorker,
		uuid.MustParse("00000000-0000-0000-0000-000000000105"),
		1, 1, 300, true,
	)
	secondaryResult, err := service.ObserveCapacity(context.Background(), secondary)
	if err != nil || secondaryResult.PoolState != fleet.CapacityAdmittable ||
		!secondaryResult.PoolAssignmentAllowed {
		t.Fatalf("initial secondary capacity = %#v error=%v", secondaryResult, err)
	}

	pressured := capacityObservation(primaryWorker, primary.WorkerPoolID, 1, 2, 850, true)
	result, err = service.ObserveCapacity(context.Background(), pressured)
	if err != nil || result.WorkerState != fleet.CapacityScratchPressured ||
		result.PoolState != fleet.CapacityAdmittable || result.WorkerAssignmentAllowed ||
		!result.PoolAssignmentAllowed {
		t.Fatalf("high-watermark capacity = %#v error=%v", result, err)
	}

	betweenWatermarks := capacityObservation(primaryWorker, primary.WorkerPoolID, 1, 3, 600, true)
	result, err = service.ObserveCapacity(context.Background(), betweenWatermarks)
	if err != nil || result.WorkerState != fleet.CapacityScratchPressured ||
		result.PoolState != fleet.CapacityAdmittable || result.WorkerAssignmentAllowed ||
		!result.PoolAssignmentAllowed {
		t.Fatalf("latched capacity = %#v error=%v", result, err)
	}

	recovered := capacityObservation(primaryWorker, primary.WorkerPoolID, 1, 4, 400, true)
	result, err = service.ObserveCapacity(context.Background(), recovered)
	if err != nil || result.WorkerState != fleet.CapacityAdmittable ||
		result.PoolState != fleet.CapacityAdmittable || !result.WorkerAssignmentAllowed ||
		!result.PoolAssignmentAllowed {
		t.Fatalf("low-watermark recovery = %#v error=%v", result, err)
	}

	storageDown := capacityObservation(primaryWorker, primary.WorkerPoolID, 1, 5, 300, false)
	result, err = service.ObserveCapacity(context.Background(), storageDown)
	if err != nil || result.WorkerState != fleet.CapacityStorageUnavailable ||
		result.PoolState != fleet.CapacityStorageUnavailable || result.WorkerAssignmentAllowed ||
		result.PoolAssignmentAllowed {
		t.Fatalf("Artifact Store outage = %#v error=%v", result, err)
	}
	storageRecovered := capacityObservation(primaryWorker, primary.WorkerPoolID, 1, 6, 300, true)
	result, err = service.ObserveCapacity(context.Background(), storageRecovered)
	if err != nil || result.WorkerState != fleet.CapacityAdmittable ||
		result.PoolState != fleet.CapacityAdmittable || !result.WorkerAssignmentAllowed ||
		!result.PoolAssignmentAllowed {
		t.Fatalf("Artifact Store recovery = %#v error=%v", result, err)
	}

	replayed, err := service.ObserveCapacity(context.Background(), storageRecovered)
	if err != nil || !replayed.Replayed || replayed != resultWithReplay(result) {
		t.Fatalf("capacity replay = %#v error=%v, want replay of %#v", replayed, err, result)
	}
	conflictingReplay := storageRecovered
	conflictingReplay.FreeBytes--
	_, err = service.ObserveCapacity(context.Background(), conflictingReplay)
	assertFleetFailure(t, err, fleet.FailureConflict)
	stale := storageRecovered
	stale.Sequence--
	_, err = service.ObserveCapacity(context.Background(), stale)
	assertFleetFailure(t, err, fleet.FailureConflict)
	secondaryResult, err = service.GetPoolCapacity(context.Background(), secondary.WorkerPoolID)
	if err != nil || secondaryResult.PoolState != fleet.CapacityAdmittable ||
		!secondaryResult.PoolAssignmentAllowed {
		t.Fatalf("unrelated pool after primary pressure = %#v error=%v", secondaryResult, err)
	}
}

func TestFleetCapacityStaleObservationFailsClosedUntilFreshReport(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000031")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/fleet-stale-capacity', 1,
			'READY', 'HEALTHY', 'node-fleet-stale-capacity'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed stale-capacity Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	if _, err := service.ConfigureCapacityPolicy(
		context.Background(), testCapacityPolicy(poolID),
	); err != nil {
		t.Fatalf("configure stale-observation capacity policy: %v", err)
	}
	mismatched := capacityObservation(workerID, poolID, 1, 1, 300, true)
	mismatched.HighWatermarkBytes--
	_, err = service.ObserveCapacity(context.Background(), mismatched)
	assertFleetFailure(t, err, fleet.FailureConflict)
	future := capacityObservation(workerID, poolID, 1, 1, 300, true)
	future.ObservedAt = time.Now().UTC().Add(31 * time.Second)
	_, err = service.ObserveCapacity(context.Background(), future)
	assertFleetFailure(t, err, fleet.FailureInvalid)

	stale := capacityObservation(workerID, poolID, 1, 1, 300, true)
	stale.ObservedAt = time.Now().UTC().Add(-10 * time.Minute)
	result, err := service.ObserveCapacity(context.Background(), stale)
	if err != nil || result.WorkerState != fleet.CapacityAdmittable ||
		result.PoolState != fleet.CapacityAdmittable || result.WorkerAssignmentAllowed ||
		result.PoolAssignmentAllowed {
		t.Fatalf("stale capacity observation = %#v error=%v", result, err)
	}
	replayed, err := service.ObserveCapacity(context.Background(), stale)
	if err != nil || !replayed.Replayed || replayed.WorkerAssignmentAllowed ||
		replayed.PoolAssignmentAllowed {
		t.Fatalf("exact stale capacity replay = %#v error=%v", replayed, err)
	}

	fresh := capacityObservation(workerID, poolID, 1, 2, 300, true)
	result, err = service.ObserveCapacity(context.Background(), fresh)
	if err != nil || !result.WorkerAssignmentAllowed || !result.PoolAssignmentAllowed {
		t.Fatalf("fresh capacity observation = %#v error=%v", result, err)
	}
}

func TestFleetCapacityExactReplayFailsClosedAfterObservationBecomesStale(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000053")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
			reachability_condition, node_identity
		) VALUES (
			$1, $2, 'spiffe://vela.internal/worker/fleet-aging-capacity', 1,
			'READY', 'HEALTHY', 'node-fleet-aging-capacity'
		)
	`, workerID, poolID); err != nil {
		t.Fatalf("seed aging-capacity Worker: %v", err)
	}
	service, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	policy := testCapacityPolicy(poolID)
	policy.ObservationMaxAge = 10 * time.Second
	if _, err := service.ConfigureCapacityPolicy(context.Background(), policy); err != nil {
		t.Fatalf("configure aging-observation capacity policy: %v", err)
	}
	observation := capacityObservation(workerID, poolID, 1, 1, 300, true)
	observation.ObservedAt = time.Now().UTC().Add(-8 * time.Second)
	initial, err := service.ObserveCapacity(context.Background(), observation)
	if err != nil || !initial.WorkerAssignmentAllowed || !initial.PoolAssignmentAllowed {
		t.Fatalf("initially fresh capacity observation = %#v error=%v", initial, err)
	}

	time.Sleep(3 * time.Second)
	replayed, err := service.ObserveCapacity(context.Background(), observation)
	if err != nil || !replayed.Replayed || replayed.WorkerAssignmentAllowed ||
		replayed.PoolAssignmentAllowed {
		t.Fatalf("aged exact capacity replay = %#v error=%v", replayed, err)
	}
}

func capacityObservation(
	workerID uuid.UUID,
	poolID uuid.UUID,
	epoch int64,
	sequence int64,
	usedBytes int64,
	artifactStoreReachable bool,
) fleet.CapacityObservation {
	watermarkState := fleet.ScratchWatermarkNormal
	if usedBytes >= 800 {
		watermarkState = fleet.ScratchWatermarkPressured
	}
	if 1000-usedBytes <= 50 {
		watermarkState = fleet.ScratchWatermarkCritical
	}
	return fleet.CapacityObservation{
		WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: epoch, Sequence: sequence,
		ObservedAt: time.Now().UTC(), WatermarkState: watermarkState,
		TotalBytes: 1000, FreeBytes: 1000 - usedBytes,
		HighWatermarkBytes: 800, LowWatermarkBytes: 400, CriticalFreeBytes: 50,
		ArtifactStoreReachable: artifactStoreReachable,
		ObservedBy:             "fleet-controller/primary",
	}
}

func resultWithReplay(result fleet.CapacityResult) fleet.CapacityResult {
	result.Replayed = true
	return result
}

func assertFleetFailure(t *testing.T, err error, code fleet.FailureCode) {
	t.Helper()
	var failure *fleet.Failure
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("Fleet error = %v, want %s", err, code)
	}
}

func assertFleetSQLState(t *testing.T, err error, code string) {
	t.Helper()
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != code {
		t.Fatalf("Fleet SQL error = %v, want SQLSTATE %s", err, code)
	}
}

func assertFleetProtocolCallDisabled(t *testing.T, database testDatabase) {
	t.Helper()
	_, err := database.Admin.Exec(`
		SELECT * FROM vela_request_worker_drain(
			NULL, NULL, NULL, NULL, NULL, NULL
		)
	`)
	assertFleetSQLState(t, err, "55000")
}

func enforceFleetProtocol(t *testing.T, database *sql.DB) {
	t.Helper()
	if _, err := database.Exec(`
		SELECT vela_transition_fleet_assignment_protocol(
			true, 'integration fixture verified zero legacy Assignment writers', 0
		)
	`); err != nil {
		t.Fatalf("enforce Fleet protocol for integration fixture: %v", err)
	}
}
