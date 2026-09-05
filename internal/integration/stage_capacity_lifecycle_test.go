//go:build integration

package integration_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/stageworkercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageCapacityObservationLifecycleReclaimsExpiredSupersededReports(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "capacity-lifecycle")
	backend, err := stageworkercontrol.NewPostgresWorkerEvidenceBackend(newRolePool(
		t, fixture.database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password",
	))
	if err != nil {
		t.Fatal(err)
	}
	command := stageWorkerAcquireCommand(fixture)
	request := &velav1.ReportStageCapacityObservationRequest{
		WorkerInstanceId:    fixture.authority.WorkerInstanceID.String(),
		WorkerInstanceEpoch: fixture.authority.WorkerInstanceEpoch,
		ObservationSequence: fixture.authority.WorkerInstanceEpoch << 32,
		CapacityVector:      map[string]int64{},
	}
	for resource, quantity := range fixture.authority.CapacityVector {
		request.CapacityVector[resource] = quantity
	}
	count := func() int {
		t.Helper()
		var rows int
		if err := fixture.database.Admin.QueryRow(`
			SELECT count(*) FROM capacity_observations WHERE worker_instance_id = $1
		`, fixture.authority.WorkerInstanceID).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		return rows
	}
	report := func() {
		t.Helper()
		result, err := backend.ReportCapacityObservation(context.Background(), command, request)
		if err == nil && !result.Ready && result.ControlSessionEpoch > command.ControlSessionEpoch {
			command.ControlSessionEpoch = result.ControlSessionEpoch
			result, err = backend.ReportCapacityObservation(context.Background(), command, request)
		}
		if err != nil || !result.Ready || result.CapacityObservationSequence != request.ObservationSequence {
			t.Fatalf("capacity report: result=%#v error=%v", result, err)
		}
	}
	baseline := count()
	for index := range 64 {
		request.ObservationSequence++
		for resource, quantity := range fixture.authority.CapacityVector {
			request.CapacityVector[resource] = quantity * int64(index%2)
		}
		now := time.Now().UTC().Truncate(time.Microsecond)
		request.ObservedAt = timestamppb.New(now)
		request.ExpiresAt = timestamppb.New(now.Add(2 * time.Second))
		report()
	}
	live := count()
	if live != baseline+64 {
		t.Fatalf("live capacity reports were removed: rows=%d want=%d", live, baseline+64)
	}
	if delay := time.Until(request.ExpiresAt.AsTime().Add(20 * time.Millisecond)); delay > 0 {
		time.Sleep(delay)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	request.ObservedAt = timestamppb.New(now)
	request.ExpiresAt = timestamppb.New(now.Add(time.Minute))
	report()
	after := count()
	t.Logf("64 changing capacity reports: baseline=%d live=%d after expiry and renewal=%d", baseline, live, after)
	if after != baseline+1 {
		t.Fatalf("expired superseded observations remain: rows=%d want=%d", after, baseline+1)
	}
	for range 64 {
		now = time.Now().UTC().Truncate(time.Microsecond)
		request.ObservedAt = timestamppb.New(now)
		request.ExpiresAt = timestamppb.New(now.Add(time.Minute))
		report()
	}
	if got := count(); got != after {
		t.Fatalf("steady renewal appended capacity observations: rows=%d want=%d", got, after)
	}
	stale := proto.Clone(request).(*velav1.ReportStageCapacityObservationRequest)
	stale.ObservationSequence--
	_, err = backend.ReportCapacityObservation(context.Background(), command, stale)
	assertPostgresConstraint(t, err, "stage_worker_capacity_sequence_stale")
	backward := proto.Clone(request).(*velav1.ReportStageCapacityObservationRequest)
	backward.ObservedAt = timestamppb.New(request.ObservedAt.AsTime().Add(-time.Microsecond))
	_, err = backend.ReportCapacityObservation(context.Background(), command, backward)
	assertPostgresConstraint(t, err, "stage_worker_capacity_renewal_stale")
}

func TestBlockedStageWorkerAcquireHistoryGrowthEvidence(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "blocked-history-lifecycle")
	if _, err := fixture.database.Admin.Exec(`
		UPDATE projects SET running_count = running_limit WHERE id = $1
	`, testProjectID); err != nil {
		t.Fatal(err)
	}
	backend := newPostgresAssignmentTestBackend(t, fixture)
	request := stageWorkerAcquireRequest(fixture)
	for range 64 {
		result, err := backend.AcquireStage(context.Background(), stageWorkerAcquireCommand(fixture), request)
		if err != nil || result.Assignment != nil || result.Command != nil || result.RetryAfter != 250*time.Millisecond {
			t.Fatalf("blocked acquisition: result=%#v error=%v", result, err)
		}
	}
	var intents, results, snapshots, decisions, attempts int
	if err := fixture.database.Admin.QueryRow(`
		SELECT (SELECT count(*) FROM stage_worker_acquire_intents),
		       (SELECT count(*) FROM stage_worker_acquire_results),
		       (SELECT count(*) FROM stage_scheduler_snapshot_traces),
		       (SELECT count(*) FROM stage_decision_evidence),
		       (SELECT count(*) FROM stage_attempts)
	`).Scan(&intents, &results, &snapshots, &decisions, &attempts); err != nil {
		t.Fatal(err)
	}
	t.Logf("64 public AcquireStage calls with blocked ready work: intents=%d results=%d snapshots=%d decisions=%d attempts=%d",
		intents, results, snapshots, decisions, attempts)
	if decisions != 0 || attempts != 0 {
		t.Fatal("blocked acquisition created execution authority")
	}
	if intents != 0 || results != 0 || snapshots != 0 {
		t.Fatal("blocked advisory acquisition appended durable history")
	}
	if _, err := fixture.database.Admin.Exec(`UPDATE projects SET running_count = 0 WHERE id = $1`, testProjectID); err != nil {
		t.Fatal(err)
	}
	command := stageWorkerAcquireCommand(fixture)
	assigned, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || assigned.Assignment == nil {
		t.Fatalf("capacity released after blocked advisory polls: result=%#v error=%v", assigned, err)
	}
	replayed, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || !proto.Equal(replayed.Assignment, assigned.Assignment) {
		t.Fatalf("assignment after blocked advisory polls did not replay: result=%#v error=%v", replayed, err)
	}
}

func TestStageCapacityObservationLifecyclePreservesEpochSourceWatermarksAndBoundsBatch(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "capacity-lifecycle-watermarks")
	workerID := fixture.authority.WorkerInstanceID
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO worker_instance_epochs (
			worker_instance_id, epoch, device_set_id, membership_digest, device_set_digest, started_at
		)
		SELECT worker_instance_id, 2, device_set_id, membership_digest, device_set_digest, started_at
		FROM worker_instance_epochs WHERE worker_instance_id = $1 AND epoch = 1
	`, workerID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO capacity_observations (
			worker_instance_id, worker_instance_epoch, observation_sequence,
			capacity_vector, observed_at, expires_at, observed_by
		)
		SELECT $1, epoch, sequence, '{"concurrency":0}'::jsonb,
		       clock_timestamp() - interval '2 hours',
		       clock_timestamp() - interval '1 hour', 'integration/lifecycle'
		FROM (SELECT 1 AS epoch, 4294967296 + value AS sequence FROM generate_series(1, 514) AS value
		      UNION ALL SELECT 1, 1000
		      UNION ALL SELECT 2, 1001
		      UNION ALL SELECT 2, 8589934593
		      UNION ALL SELECT 2, 8589934594) AS history
	`, workerID); err != nil {
		t.Fatal(err)
	}
	prune := func(want int) {
		t.Helper()
		tx, err := fixture.database.Admin.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.Exec(`SELECT id FROM worker_instances WHERE id = $1 FOR UPDATE`, workerID); err != nil {
			t.Fatal(err)
		}
		var deleted int
		if err := tx.QueryRow(`SELECT vela_prune_worker_capacity_observations($1)`, workerID).Scan(&deleted); err != nil {
			t.Fatal(err)
		}
		if deleted != want {
			t.Fatalf("prune deleted=%d want=%d", deleted, want)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	prune(256)
	prune(256)
	prune(2)
	prune(0)
	var sequences []int64
	rows, err := fixture.database.Admin.Query(`
		SELECT observation_sequence FROM capacity_observations WHERE worker_instance_id = $1 ORDER BY observation_sequence
	`, workerID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var sequence int64
		if err := rows.Scan(&sequence); err != nil {
			t.Fatal(err)
		}
		sequences = append(sequences, sequence)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []int64{fixture.observation.Sequence, 1000, 1001, 4294967810, 8589934594}
	if len(sequences) != len(want) {
		t.Fatalf("retained sequences=%v want=%v", sequences, want)
	}
	for index := range want {
		if sequences[index] != want[index] {
			t.Fatalf("retained sequences=%v want=%v", sequences, want)
		}
	}
}

func TestStageCapacityObservationLifecycleMigrationPreservesRoleAndFunctionIdentity(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 82)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	identity := func() string {
		t.Helper()
		var result string
		if err := database.Admin.QueryRow(`
			SELECT jsonb_agg(jsonb_build_array(oid, proowner, proacl) ORDER BY oid)::text
			FROM pg_proc WHERE oid IN (
				'vela_observe_worker_instance(jsonb)'::regprocedure,
				'vela_report_stage_worker_capacity_v66(jsonb)'::regprocedure,
				'vela_verify_stage_capacity_observation(jsonb)'::regprocedure)
		`).Scan(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	want := identity()
	for range 2 {
		if err := goose.UpTo(database.Admin, migrations, 83); err != nil {
			t.Fatal(err)
		}
		if got := identity(); got != want {
			t.Fatalf("capacity function identity/ACL changed: got=%s want=%s", got, want)
		}
		var exposed bool
		if err := database.Admin.QueryRow(`
			SELECT EXISTS (
				SELECT 1 FROM pg_roles WHERE rolcanlogin AND NOT rolsuper
				AND has_function_privilege(oid, 'vela_prune_worker_capacity_observations(uuid)', 'EXECUTE')
			)
		`).Scan(&exposed); err != nil {
			t.Fatal(err)
		}
		if exposed {
			t.Fatal("capacity maintenance became directly callable by a login role")
		}
		if err := goose.DownTo(database.Admin, migrations, 82); err != nil {
			t.Fatal(err)
		}
		if got := identity(); got != want {
			t.Fatalf("capacity rollback identity/ACL changed: got=%s want=%s", got, want)
		}
	}
}
