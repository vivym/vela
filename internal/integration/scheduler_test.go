//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/scheduler"
	"github.com/vivym/vela/internal/workercontrol"
)

func TestSchedulerDispatchesWeightedOrganizationsThroughAcquire(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Scheduler integration fixture")
	seedAdmissionFixture(t, database.Admin)
	seedOtherOrganization(t, database.Admin)
	server := admissionServerForDatabase(t, database)

	primary := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-weighted-primary",
	)
	otherFirst := submitSchedulerJob(
		t,
		server.URL,
		testOtherProjectID,
		bearerCredential(testOtherCredentialID, testOtherSecret),
		"scheduler-weighted-other-first",
	)
	otherSecond := submitSchedulerJob(
		t,
		server.URL,
		testOtherProjectID,
		bearerCredential(testOtherCredentialID, testOtherSecret),
		"scheduler-weighted-other-second",
	)

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	seedSchedulerCapacityShares(t, database.Admin, poolID,
		schedulerCapacityShare{
			OrganizationID: testOrganizationID,
			ProjectID:      testProjectID,
			Weight:         1,
			RunningLimit:   2,
			ProjectWeight:  1,
		},
		schedulerCapacityShare{
			OrganizationID: testOtherOrganizationID,
			ProjectID:      testOtherProjectID,
			Weight:         2,
			RunningLimit:   2,
			ProjectWeight:  1,
		},
	)

	workerIDs := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000320"),
		uuid.MustParse("00000000-0000-0000-0000-000000000321"),
		uuid.MustParse("00000000-0000-0000-0000-000000000322"),
	}
	workers := make([]schedulerWorker, len(workerIDs))
	for index, workerID := range workerIDs {
		workers[index] = schedulerWorker{
			ID:       workerID,
			SPIFFEID: fmt.Sprintf("spiffe://vela.internal/worker/scheduler-%d", index),
			Epoch:    7,
		}
	}
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, workers...)

	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create Worker coordinator: %v", err)
	}
	schedulerPool := newRolePool(t, database.DSN, "vela_scheduler_login", "vela-scheduler-password")
	scheduling, err := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "scheduler-weighted-integration",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create Scheduler: %v", err)
	}

	wantJobs := []uuid.UUID{otherFirst, primary, otherSecond}
	for index, wantJob := range wantJobs {
		if index == 1 {
			scheduling, err = scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
				SchedulerID:       "scheduler-weighted-integration-restarted",
				ClaimTTL:          30 * time.Second,
				CandidateAttempts: 3,
			})
			if err != nil {
				t.Fatalf("restart weighted Scheduler: %v", err)
			}
		}
		dispatch, ok, err := scheduling.RunOnce(context.Background(), poolID)
		if err != nil {
			t.Fatalf("Scheduler RunOnce %d: %v", index, err)
		}
		if !ok {
			t.Fatalf("Scheduler RunOnce %d found no dispatch", index)
		}
		if dispatch.Assignment.JobID != wantJob {
			t.Fatalf(
				"Scheduler dispatch %d Job = %s, want %s",
				index,
				dispatch.Assignment.JobID,
				wantJob,
			)
		}
		var (
			state       string
			attemptJob  uuid.UUID
			attempts    int
			assignments int
		)
		if err := database.Admin.QueryRow(`
			SELECT
				d.state::text,
				a.job_id,
				(SELECT count(*) FROM attempts WHERE scheduler_dispatch_intent_id = d.id),
				(SELECT count(*) FROM outbox_events
				 WHERE aggregate_id = a.job_id AND event_type = 'job.assigned')
			FROM scheduler_dispatch_intents AS d
			JOIN attempts AS a ON a.scheduler_dispatch_intent_id = d.id
			WHERE d.id = $1
		`, dispatch.IntentID).Scan(&state, &attemptJob, &attempts, &assignments); err != nil {
			t.Fatalf("read committed dispatch %d: %v", index, err)
		}
		if state != "COMMITTED" || attemptJob != wantJob || attempts != 1 || assignments != 1 {
			t.Fatalf(
				"dispatch %d receipt = state %s Job %s Attempts %d events %d",
				index,
				state,
				attemptJob,
				attempts,
				assignments,
			)
		}
	}
}

func TestSchedulerWeightedDeficitSurvivesSaturationWithoutStarvation(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedOtherOrganization(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	primaryJob := submitSchedulerJob(
		t, server.URL, testProjectID, testBearerCredential(), "scheduler-saturation-primary",
	)
	otherJob := submitSchedulerJob(
		t,
		server.URL,
		testOtherProjectID,
		bearerCredential(testOtherCredentialID, testOtherSecret),
		"scheduler-saturation-other",
	)

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	seedSchedulerCapacityShares(t, database.Admin, poolID,
		schedulerCapacityShare{
			OrganizationID: testOrganizationID,
			ProjectID:      testProjectID,
			Weight:         1,
			RunningLimit:   1,
			ProjectWeight:  1,
		},
		schedulerCapacityShare{
			OrganizationID: testOtherOrganizationID,
			ProjectID:      testOtherProjectID,
			Weight:         2,
			RunningLimit:   1,
			ProjectWeight:  1,
		},
	)
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, schedulerWorker{
		ID:       uuid.MustParse("00000000-0000-0000-0000-000000000319"),
		SPIFFEID: "spiffe://vela.internal/worker/scheduler-saturation",
		Epoch:    7,
	})
	if _, err := database.Admin.Exec(`
		UPDATE worker_pools
		SET scheduler_quantum_seconds = 1, scheduler_max_deficit_seconds = 1
		WHERE id = $1
	`, poolID); err != nil {
		t.Fatalf("seed saturation Scheduler policy: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO job_runtime_predictions (
			job_id, organization_id, project_id, predicted_runtime_seconds,
			source, source_revision
		)
		SELECT id, organization_id, project_id, 1, 'integration', 'saturation-v1'
		FROM jobs WHERE id = ANY($1::uuid[])
	`, []uuid.UUID{primaryJob, otherJob}); err != nil {
		t.Fatalf("seed saturation runtime predictions: %v", err)
	}

	schedulerPool := newRolePool(t, database.DSN, "vela_scheduler_login", "vela-scheduler-password")
	counts := map[uuid.UUID]int{}
	for index := 0; index < 120; index++ {
		var intentID, organizationID uuid.UUID
		if err := schedulerPool.QueryRow(context.Background(), `
			SELECT intent_id, organization_id
			FROM vela_claim_scheduler_dispatch($1, 'scheduler-saturation', 30)
		`, poolID).Scan(&intentID, &organizationID); err != nil {
			t.Fatalf("claim saturation dispatch %d: %v", index, err)
		}
		counts[organizationID]++
		var abandoned bool
		if err := schedulerPool.QueryRow(context.Background(), `
			SELECT vela_abandon_scheduler_dispatch($1, 'scheduler-saturation', 'test_turn')
		`, intentID).Scan(&abandoned); err != nil || !abandoned {
			t.Fatalf("abandon saturation dispatch %d = %t error=%v", index, abandoned, err)
		}
	}
	primaryCount := counts[uuid.MustParse(testOrganizationID)]
	otherCount := counts[uuid.MustParse(testOtherOrganizationID)]
	if primaryCount < 36 || primaryCount > 44 || otherCount < 76 || otherCount > 84 {
		t.Fatalf(
			"saturated weighted service = primary %d other %d, want bounded 1:2 service",
			primaryCount,
			otherCount,
		)
	}
}

func TestSchedulerRunCycleDiscoversPoolsWithoutBroadTablePrivileges(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Scheduler cycle fixture")
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	jobID := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-cycle",
	)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	workerID := uuid.MustParse("00000000-0000-0000-0000-000000000323")
	seedSchedulerCapacityShares(t, database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   2,
		ProjectWeight:  1,
	})
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, schedulerWorker{
		ID:       workerID,
		SPIFFEID: "spiffe://vela.internal/worker/scheduler-cycle",
		Epoch:    7,
	})

	schedulerPool := newRolePool(t, database.DSN, "vela_scheduler_login", "vela-scheduler-password")
	if err := schedulerPool.QueryRow(context.Background(), "SELECT id FROM jobs LIMIT 1").Scan(new(uuid.UUID)); err == nil {
		t.Fatal("Scheduler role unexpectedly read Jobs directly")
	}
	if err := schedulerPool.QueryRow(context.Background(), "SELECT id FROM workers LIMIT 1").Scan(new(uuid.UUID)); err == nil {
		t.Fatal("Scheduler role unexpectedly read Workers directly")
	}
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create cycle Worker coordinator: %v", err)
	}
	scheduling, err := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "scheduler-cycle-integration",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create cycle Scheduler: %v", err)
	}
	dispatches, err := scheduling.RunCycle(context.Background())
	if err != nil {
		t.Fatalf("run Scheduler cycle: %v", err)
	}
	if len(dispatches) != 1 || dispatches[0].Assignment.JobID != jobID {
		t.Fatalf("Scheduler cycle dispatches = %#v, want Job %s", dispatches, jobID)
	}
}

func TestSchedulerDispatchesWeightedProjectsInsideOrganization(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Scheduler Project fairness fixture")
	seedAdmissionFixture(t, database.Admin)
	seedSecondProjectAndPool(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE worker_pools
		SET admission_open = false
		WHERE id = '00000000-0000-0000-0000-000000000105'
	`); err != nil {
		t.Fatalf("close secondary pool Admission: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	primary := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-project-primary",
	)
	secondFirst := submitSchedulerJob(
		t,
		server.URL,
		testProjectTwoID,
		bearerCredential(testCredentialTwoID, testCredentialTwoSecret),
		"scheduler-project-second-first",
	)
	secondSecond := submitSchedulerJob(
		t,
		server.URL,
		testProjectTwoID,
		bearerCredential(testCredentialTwoID, testCredentialTwoSecret),
		"scheduler-project-second-second",
	)

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	seedSchedulerCapacityShares(t, database.Admin, poolID,
		schedulerCapacityShare{
			OrganizationID: testOrganizationID,
			ProjectID:      testProjectID,
			Weight:         1,
			RunningLimit:   3,
			ProjectWeight:  1,
		},
		schedulerCapacityShare{
			OrganizationID: testOrganizationID,
			ProjectID:      testProjectTwoID,
			Weight:         1,
			RunningLimit:   3,
			ProjectWeight:  2,
		},
	)
	workers := make([]schedulerWorker, 3)
	for index := range 3 {
		workerID := uuid.MustParse(fmt.Sprintf(
			"00000000-0000-0000-0000-%012d",
			330+index,
		))
		workers[index] = schedulerWorker{
			ID:       workerID,
			SPIFFEID: fmt.Sprintf("spiffe://vela.internal/worker/project-%d", index),
			Epoch:    7,
		}
	}
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, workers[0])
	rows, err := database.Admin.Query(`
		SELECT job_id
		FROM vela_scheduler_queue_projection()
		WHERE worker_pool_id = $1
		GROUP BY job_id, predicted_start_at
		ORDER BY predicted_start_at, job_id
	`, poolID)
	if err != nil {
		t.Fatalf("read Project fairness queue projection: %v", err)
	}
	projectedJobs := make([]uuid.UUID, 0, 3)
	for rows.Next() {
		var jobID uuid.UUID
		if err := rows.Scan(&jobID); err != nil {
			rows.Close()
			t.Fatalf("scan Project fairness queue projection: %v", err)
		}
		projectedJobs = append(projectedJobs, jobID)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close Project fairness queue projection: %v", err)
	}
	wantJobs := []uuid.UUID{secondFirst, primary, secondSecond}
	if len(projectedJobs) != len(wantJobs) {
		t.Fatalf("Project fairness projection = %v, want %v", projectedJobs, wantJobs)
	}
	for index := range wantJobs {
		if projectedJobs[index] != wantJobs[index] {
			t.Fatalf(
				"Project fairness projection %d Job = %s, want %s",
				index,
				projectedJobs[index],
				wantJobs[index],
			)
		}
	}
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, workers[1:]...)

	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create Project fairness coordinator: %v", err)
	}
	schedulerPool := newRolePool(t, database.DSN, "vela_scheduler_login", "vela-scheduler-password")
	scheduling, err := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "scheduler-project-fairness",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create Project fairness Scheduler: %v", err)
	}
	for index, wantJob := range wantJobs {
		if index == 1 {
			scheduling, err = scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
				SchedulerID:       "scheduler-project-fairness-restarted",
				ClaimTTL:          30 * time.Second,
				CandidateAttempts: 3,
			})
			if err != nil {
				t.Fatalf("restart Project fairness Scheduler: %v", err)
			}
		}
		dispatch, ok, err := scheduling.RunOnce(context.Background(), poolID)
		if err != nil || !ok {
			t.Fatalf("Project fairness dispatch %d = ok %t error %v", index, ok, err)
		}
		if dispatch.Assignment.JobID != wantJob {
			t.Fatalf(
				"Project fairness dispatch %d Job = %s, want %s",
				index,
				dispatch.Assignment.JobID,
				wantJob,
			)
		}
	}
}

func TestSchedulerWeightedProjectDeficitSurvivesSaturation(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedSecondProjectAndPool(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE worker_pools
		SET admission_open = false
		WHERE id = '00000000-0000-0000-0000-000000000105'
	`); err != nil {
		t.Fatalf("close secondary pool Admission: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	primaryJob := submitSchedulerJob(
		t, server.URL, testProjectID, testBearerCredential(), "scheduler-project-saturation-primary",
	)
	secondJob := submitSchedulerJob(
		t,
		server.URL,
		testProjectTwoID,
		bearerCredential(testCredentialTwoID, testCredentialTwoSecret),
		"scheduler-project-saturation-second",
	)

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	seedSchedulerCapacityShares(t, database.Admin, poolID,
		schedulerCapacityShare{
			OrganizationID: testOrganizationID,
			ProjectID:      testProjectID,
			Weight:         1,
			RunningLimit:   1,
			ProjectWeight:  1,
		},
		schedulerCapacityShare{
			OrganizationID: testOrganizationID,
			ProjectID:      testProjectTwoID,
			Weight:         1,
			RunningLimit:   1,
			ProjectWeight:  2,
		},
	)
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, schedulerWorker{
		ID:       uuid.MustParse("00000000-0000-0000-0000-000000000329"),
		SPIFFEID: "spiffe://vela.internal/worker/scheduler-project-saturation",
		Epoch:    7,
	})
	if _, err := database.Admin.Exec(`
		UPDATE worker_pools
		SET scheduler_quantum_seconds = 1, scheduler_max_deficit_seconds = 1
		WHERE id = $1
	`, poolID); err != nil {
		t.Fatalf("seed Project saturation Scheduler policy: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO job_runtime_predictions (
			job_id, organization_id, project_id, predicted_runtime_seconds,
			source, source_revision
		)
		SELECT id, organization_id, project_id, 1, 'integration', 'project-saturation-v1'
		FROM jobs WHERE id = ANY($1::uuid[])
	`, []uuid.UUID{primaryJob, secondJob}); err != nil {
		t.Fatalf("seed Project saturation runtime predictions: %v", err)
	}

	schedulerPool := newRolePool(t, database.DSN, "vela_scheduler_login", "vela-scheduler-password")
	counts := map[uuid.UUID]int{}
	for index := 0; index < 120; index++ {
		var intentID, projectID uuid.UUID
		if err := schedulerPool.QueryRow(context.Background(), `
			SELECT intent_id, project_id
			FROM vela_claim_scheduler_dispatch($1, 'scheduler-project-saturation', 30)
		`, poolID).Scan(&intentID, &projectID); err != nil {
			t.Fatalf("claim Project saturation dispatch %d: %v", index, err)
		}
		counts[projectID]++
		var abandoned bool
		if err := schedulerPool.QueryRow(context.Background(), `
			SELECT vela_abandon_scheduler_dispatch(
				$1, 'scheduler-project-saturation', 'test_turn'
			)
		`, intentID).Scan(&abandoned); err != nil || !abandoned {
			t.Fatalf(
				"abandon Project saturation dispatch %d = %t error=%v",
				index,
				abandoned,
				err,
			)
		}
	}
	primaryCount := counts[uuid.MustParse(testProjectID)]
	secondCount := counts[uuid.MustParse(testProjectTwoID)]
	if primaryCount < 36 || primaryCount > 44 || secondCount < 76 || secondCount > 84 {
		t.Fatalf(
			"saturated weighted Project service = primary %d second %d, want bounded 1:2 service",
			primaryCount,
			secondCount,
		)
	}
}

func TestSchedulerDispatchesWeightedServiceClassesBeforeProjectJobs(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Scheduler Service Class fixture")
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE projects SET running_limit = 3 WHERE id = $1
	`, testProjectID); err != nil {
		t.Fatalf("raise Service Class fixture Project running limit: %v", err)
	}
	acceleratedServiceClassID := uuid.MustParse("00000000-0000-0000-0000-000000000350")
	if _, err := database.Admin.Exec(`
		INSERT INTO service_class_revisions (
			id, stable_id, revision, state, queue_retry_allowance_seconds, max_attempts,
			max_total_compute_multiplier_milli, max_finalization_seconds_per_attempt,
			retry_backoff_policy, retryable_failure_classes, circuit_breaker_policy,
			queue_weight
		) VALUES (
			$1, 'accelerated', 1, 'ACTIVE', 7200, 3, 2000, 600,
			'{"kind":"exponential","initial_seconds":30,"max_seconds":300}',
			ARRAY['WORKER_LOST', 'TRANSIENT_BACKEND'],
			'{"policy_revision":"h3-accelerated-v1"}', 2
		)
	`, acceleratedServiceClassID); err != nil {
		t.Fatalf("seed accelerated ServiceClassRevision: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO rate_card_lines (
			id, rate_card_revision_id, model_revision_id, generation_preset_revision_id,
			service_class_revision_id, output_spec_id, unit_amount_minor, currency
		) VALUES (
			'00000000-0000-0000-0000-000000000351',
			'00000000-0000-0000-0000-000000000016',
			'00000000-0000-0000-0000-000000000010',
			'00000000-0000-0000-0000-000000000011', $1,
			'00000000-0000-0000-0000-000000000013', 1250, 'CNY'
		)
	`, acceleratedServiceClassID); err != nil {
		t.Fatalf("seed accelerated RateCard line: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	standard := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-service-class-standard",
	)
	acceleratedFirst := seedSchedulerJobForServiceClass(
		t,
		database.Admin,
		standard,
		acceleratedServiceClassID,
		uuid.MustParse("00000000-0000-0000-0000-000000000351"),
		"accelerated",
		time.Now().UTC(),
		nil,
	)
	acceleratedSecond := seedSchedulerJobForServiceClass(
		t,
		database.Admin,
		standard,
		acceleratedServiceClassID,
		uuid.MustParse("00000000-0000-0000-0000-000000000351"),
		"accelerated",
		time.Now().UTC(),
		nil,
	)

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	seedSchedulerCapacityShares(t, database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   3,
		ProjectWeight:  1,
	})
	workers := make([]schedulerWorker, 3)
	for index := range 3 {
		workerID := uuid.MustParse(fmt.Sprintf(
			"00000000-0000-0000-0000-%012d",
			360+index,
		))
		workers[index] = schedulerWorker{
			ID:       workerID,
			SPIFFEID: fmt.Sprintf("spiffe://vela.internal/worker/service-class-%d", index),
			Epoch:    7,
		}
	}
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, workers...)

	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create Service Class coordinator: %v", err)
	}
	schedulerPool := newRolePool(t, database.DSN, "vela_scheduler_login", "vela-scheduler-password")
	scheduling, err := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "scheduler-service-class-fairness",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create Service Class Scheduler: %v", err)
	}
	wantJobs := []uuid.UUID{acceleratedFirst, standard, acceleratedSecond}
	for index, wantJob := range wantJobs {
		dispatch, ok, err := scheduling.RunOnce(context.Background(), poolID)
		if err != nil || !ok {
			t.Fatalf("Service Class dispatch %d = ok %t error %v", index, ok, err)
		}
		if dispatch.Assignment.JobID != wantJob {
			t.Fatalf(
				"Service Class dispatch %d Job = %s, want %s",
				index,
				dispatch.Assignment.JobID,
				wantJob,
			)
		}
	}
}

func TestSchedulerWaitingAgeCannotBeRewritten(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	jobID := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-age-immutable",
	)
	_, err := database.Admin.Exec(`
		UPDATE jobs SET created_at = created_at - interval '1 hour' WHERE id = $1
	`, jobID)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) ||
		postgresError.Code != "55000" ||
		postgresError.ConstraintName != "jobs_scheduler_waiting_age_immutable" {
		t.Fatalf("rewrite Scheduler waiting age error = %v", err)
	}
}

func TestSchedulerProtectedLanePreventsLongJobStarvation(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Scheduler Protected Lane fixture")
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	templateJob := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-protected-template",
	)
	protectedServiceClassID := uuid.MustParse("00000000-0000-0000-0000-000000000370")
	protectedRateLineID := uuid.MustParse("00000000-0000-0000-0000-000000000371")
	if _, err := database.Admin.Exec(`
		INSERT INTO service_class_revisions (
			id, stable_id, revision, state, queue_retry_allowance_seconds, max_attempts,
			max_total_compute_multiplier_milli, max_finalization_seconds_per_attempt,
			retry_backoff_policy, retryable_failure_classes, circuit_breaker_policy,
			queue_weight, max_queue_wait_before_protection_seconds,
			max_aging_credit_seconds, max_expiry_urgency_credit_seconds
		) VALUES (
			$1, 'protected-test', 1, 'ACTIVE', 7200, 3, 2000, 600,
			'{"kind":"exponential","initial_seconds":30,"max_seconds":300}',
			ARRAY['WORKER_LOST', 'TRANSIENT_BACKEND'],
			'{"policy_revision":"h3-protected-v1"}', 1000, 60, 10, 10
		)
	`, protectedServiceClassID); err != nil {
		t.Fatalf("seed Protected Lane ServiceClassRevision: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO rate_card_lines (
			id, rate_card_revision_id, model_revision_id, generation_preset_revision_id,
			service_class_revision_id, output_spec_id, unit_amount_minor, currency
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000016',
			'00000000-0000-0000-0000-000000000010',
			'00000000-0000-0000-0000-000000000011', $2,
			'00000000-0000-0000-0000-000000000013', 1250, 'CNY'
		)
	`, protectedRateLineID, protectedServiceClassID); err != nil {
		t.Fatalf("seed Protected Lane RateCard line: %v", err)
	}
	now := time.Now().UTC()
	farExpiry := now.Add(30 * time.Minute)
	earlyExpiry := now.Add(10 * time.Minute)
	oldLongJob := seedSchedulerJobForServiceClass(
		t,
		database.Admin,
		templateJob,
		protectedServiceClassID,
		protectedRateLineID,
		"protected-test",
		now.Add(-3*time.Minute),
		&farExpiry,
	)
	earlyExpiryJob := seedSchedulerJobForServiceClass(
		t,
		database.Admin,
		templateJob,
		protectedServiceClassID,
		protectedRateLineID,
		"protected-test",
		now.Add(-2*time.Minute),
		&earlyExpiry,
	)
	newerFIFOJob := seedSchedulerJobForServiceClass(
		t,
		database.Admin,
		templateJob,
		protectedServiceClassID,
		protectedRateLineID,
		"protected-test",
		now.Add(-2*time.Minute),
		&farExpiry,
	)
	freshShortJob := seedSchedulerJobForServiceClass(
		t,
		database.Admin,
		templateJob,
		protectedServiceClassID,
		protectedRateLineID,
		"protected-test",
		now,
		&farExpiry,
	)
	if _, err := database.Admin.Exec(`
		INSERT INTO job_runtime_predictions (
			job_id, organization_id, project_id, predicted_runtime_seconds,
			source, source_revision
		) VALUES
			($1, $5, $6, 100, 'integration-receipt', 'protected-old-v1'),
			($2, $5, $6, 100, 'integration-receipt', 'protected-expiry-v1'),
			($3, $5, $6, 100, 'integration-receipt', 'protected-fifo-v1'),
			($4, $5, $6, 1, 'integration-receipt', 'protected-short-v1')
	`, oldLongJob, earlyExpiryJob, newerFIFOJob, freshShortJob, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("seed Protected Lane runtime predictions: %v", err)
	}

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	if _, err := database.Admin.Exec(`
		UPDATE worker_pools
		SET scheduler_quantum_seconds = 1, scheduler_max_deficit_seconds = 100000
		WHERE id = $1
	`, poolID); err != nil {
		t.Fatalf("configure Protected Lane Scheduler quantum: %v", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE projects SET running_limit = 3 WHERE id = $1
	`, testProjectID); err != nil {
		t.Fatalf("configure Protected Lane Project capacity: %v", err)
	}
	seedSchedulerCapacityShares(t, database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   3,
		ProjectWeight:  1,
	})
	workerIDs := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000372"),
		uuid.MustParse("00000000-0000-0000-0000-000000000377"),
		uuid.MustParse("00000000-0000-0000-0000-000000000378"),
	}
	workers := make([]schedulerWorker, len(workerIDs))
	for index, workerID := range workerIDs {
		workers[index] = schedulerWorker{
			ID:       workerID,
			SPIFFEID: fmt.Sprintf("spiffe://vela.internal/worker/protected-lane-%d", index),
			Epoch:    7,
		}
	}
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, workers...)
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create Protected Lane coordinator: %v", err)
	}
	schedulerPool := newRolePool(t, database.DSN, "vela_scheduler_login", "vela-scheduler-password")
	scheduling, err := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "scheduler-protected-lane",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create Protected Lane Scheduler: %v", err)
	}
	for index, wantJob := range []uuid.UUID{earlyExpiryJob, oldLongJob, newerFIFOJob} {
		dispatch, ok, runErr := scheduling.RunOnce(context.Background(), poolID)
		if runErr != nil || !ok {
			t.Fatalf("Protected Lane dispatch %d = ok %t error %v", index, ok, runErr)
		}
		if dispatch.Assignment.JobID != wantJob || dispatch.Lane != "PROTECTED" {
			t.Fatalf(
				"Protected Lane dispatch %d = Job %s lane %s, want Job %s lane PROTECTED; fresh short Job %s",
				index,
				dispatch.Assignment.JobID,
				dispatch.Lane,
				wantJob,
				freshShortJob,
			)
		}
	}
}

func TestSchedulerNormalLaneUsesBoundedAgingAndExpiryUrgency(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Scheduler normal score fixture")
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	templateJob := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-normal-score-template",
	)
	serviceClassID := uuid.MustParse("00000000-0000-0000-0000-000000000373")
	rateLineID := uuid.MustParse("00000000-0000-0000-0000-000000000374")
	if _, err := database.Admin.Exec(`
		INSERT INTO service_class_revisions (
			id, stable_id, revision, state, queue_retry_allowance_seconds, max_attempts,
			max_total_compute_multiplier_milli, max_finalization_seconds_per_attempt,
			retry_backoff_policy, retryable_failure_classes, circuit_breaker_policy,
			queue_weight, max_queue_wait_before_protection_seconds,
			max_aging_credit_seconds, max_expiry_urgency_credit_seconds
		) VALUES (
			$1, 'normal-score-test', 1, 'ACTIVE', 7200, 3, 2000, 600,
			'{"kind":"exponential","initial_seconds":30,"max_seconds":300}',
			ARRAY['WORKER_LOST', 'TRANSIENT_BACKEND'],
			'{"policy_revision":"h3-normal-score-v1"}', 1000, 3600, 300, 300
		)
	`, serviceClassID); err != nil {
		t.Fatalf("seed normal-score ServiceClassRevision: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO rate_card_lines (
			id, rate_card_revision_id, model_revision_id, generation_preset_revision_id,
			service_class_revision_id, output_spec_id, unit_amount_minor, currency
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000016',
			'00000000-0000-0000-0000-000000000010',
			'00000000-0000-0000-0000-000000000011', $2,
			'00000000-0000-0000-0000-000000000013', 1250, 'CNY'
		)
	`, rateLineID, serviceClassID); err != nil {
		t.Fatalf("seed normal-score RateCard line: %v", err)
	}

	now := time.Now().UTC()
	farExpiry := now.Add(30 * time.Minute)
	urgentExpiry := now.Add(2 * time.Minute)
	agedJob := seedSchedulerJobForServiceClass(
		t,
		database.Admin,
		templateJob,
		serviceClassID,
		rateLineID,
		"normal-score-test",
		now.Add(-2*time.Minute),
		&farExpiry,
	)
	urgentJob := seedSchedulerJobForServiceClass(
		t,
		database.Admin,
		templateJob,
		serviceClassID,
		rateLineID,
		"normal-score-test",
		now,
		&urgentExpiry,
	)
	controlJob := seedSchedulerJobForServiceClass(
		t,
		database.Admin,
		templateJob,
		serviceClassID,
		rateLineID,
		"normal-score-test",
		now,
		&farExpiry,
	)
	if _, err := database.Admin.Exec(`
		INSERT INTO job_runtime_predictions (
			job_id, organization_id, project_id, predicted_runtime_seconds,
			source, source_revision
		) VALUES
			($1, $4, $5, 300, 'integration-receipt', 'normal-aged-v1'),
			($2, $4, $5, 300, 'integration-receipt', 'normal-urgent-v1'),
			($3, $4, $5, 200, 'integration-receipt', 'normal-control-v1')
	`, agedJob, urgentJob, controlJob, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("seed normal-score runtime predictions: %v", err)
	}

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	if _, err := database.Admin.Exec(`
		UPDATE worker_pools
		SET scheduler_quantum_seconds = 1, scheduler_max_deficit_seconds = 100000
		WHERE id = $1
	`, poolID); err != nil {
		t.Fatalf("configure normal-score Scheduler quantum: %v", err)
	}
	seedSchedulerCapacityShares(t, database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   2,
		ProjectWeight:  1,
	})
	workerIDs := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000375"),
		uuid.MustParse("00000000-0000-0000-0000-000000000376"),
	}
	workers := make([]schedulerWorker, len(workerIDs))
	for index, workerID := range workerIDs {
		workers[index] = schedulerWorker{
			ID:       workerID,
			SPIFFEID: fmt.Sprintf("spiffe://vela.internal/worker/normal-score-%d", index),
			Epoch:    7,
		}
	}
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, workers...)

	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create normal-score coordinator: %v", err)
	}
	schedulerPool := newRolePool(t, database.DSN, "vela_scheduler_login", "vela-scheduler-password")
	scheduling, err := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "scheduler-normal-score",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create normal-score Scheduler: %v", err)
	}
	for index, wantJob := range []uuid.UUID{urgentJob, agedJob} {
		dispatch, ok, runErr := scheduling.RunOnce(context.Background(), poolID)
		if runErr != nil || !ok {
			t.Fatalf("normal-score dispatch %d = ok %t error %v", index, ok, runErr)
		}
		if dispatch.Assignment.JobID != wantJob || dispatch.Lane != "NORMAL" {
			t.Fatalf(
				"normal-score dispatch %d = Job %s lane %s, want Job %s lane NORMAL; control Job %s",
				index,
				dispatch.Assignment.JobID,
				dispatch.Lane,
				wantJob,
				controlJob,
			)
		}
	}
}

func TestSchedulerNormalLaneUsesBoundedPerJobRetryRisk(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Scheduler retry-risk score fixture")
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	templateJob := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-retry-risk-template",
	)
	serviceClassID := uuid.MustParse("00000000-0000-0000-0000-000000000379")
	rateLineID := uuid.MustParse("00000000-0000-0000-0000-000000000380")
	if _, err := database.Admin.Exec(`
		INSERT INTO service_class_revisions (
			id, stable_id, revision, state, queue_retry_allowance_seconds, max_attempts,
			max_total_compute_multiplier_milli, max_finalization_seconds_per_attempt,
			retry_backoff_policy, retryable_failure_classes, circuit_breaker_policy,
			queue_weight, max_queue_wait_before_protection_seconds,
			max_aging_credit_seconds, max_expiry_urgency_credit_seconds,
			max_retry_risk_penalty_seconds
		) VALUES (
			$1, 'retry-risk-score-test', 1, 'ACTIVE', 7200, 3, 2000, 600,
			'{"kind":"exponential","initial_seconds":30,"max_seconds":300}',
			ARRAY['WORKER_LOST', 'TRANSIENT_BACKEND'],
			'{"policy_revision":"h3-retry-risk-score-v1"}', 1000, 3600, 0, 0, 150
		)
	`, serviceClassID); err != nil {
		t.Fatalf("seed retry-risk score ServiceClassRevision: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO rate_card_lines (
			id, rate_card_revision_id, model_revision_id, generation_preset_revision_id,
			service_class_revision_id, output_spec_id, unit_amount_minor, currency
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000016',
			'00000000-0000-0000-0000-000000000010',
			'00000000-0000-0000-0000-000000000011', $2,
			'00000000-0000-0000-0000-000000000013', 1250, 'CNY'
		)
	`, rateLineID, serviceClassID); err != nil {
		t.Fatalf("seed retry-risk score RateCard line: %v", err)
	}

	now := time.Now().UTC()
	createdAt := now.Add(-2 * time.Minute)
	jobExpiresAt := now.Add(30 * time.Minute)
	riskyShortJob := seedSchedulerJobForServiceClass(
		t,
		database.Admin,
		templateJob,
		serviceClassID,
		rateLineID,
		"retry-risk-score-test",
		createdAt,
		&jobExpiresAt,
	)
	safeLongJob := seedSchedulerJobForServiceClass(
		t,
		database.Admin,
		templateJob,
		serviceClassID,
		rateLineID,
		"retry-risk-score-test",
		createdAt,
		&jobExpiresAt,
	)
	safeLongestJob := seedSchedulerJobForServiceClass(
		t,
		database.Admin,
		templateJob,
		serviceClassID,
		rateLineID,
		"retry-risk-score-test",
		createdAt,
		&jobExpiresAt,
	)
	if _, err := database.Admin.Exec(`
		INSERT INTO job_runtime_predictions (
			job_id, organization_id, project_id, predicted_runtime_seconds,
			retry_risk_penalty_seconds, source, source_revision
		) VALUES
			($1, $4, $5, 100, 500, 'integration-receipt', 'retry-risk-short-v1'),
			($2, $4, $5, 200, 0, 'integration-receipt', 'retry-risk-safe-v1'),
			($3, $4, $5, 300, 0, 'integration-receipt', 'retry-risk-safest-v1')
	`, riskyShortJob, safeLongJob, safeLongestJob, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("seed per-Job retry-risk predictions: %v", err)
	}

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	if _, err := database.Admin.Exec(`
		UPDATE worker_pools
		SET scheduler_quantum_seconds = 1, scheduler_max_deficit_seconds = 100000
		WHERE id = $1
	`, poolID); err != nil {
		t.Fatalf("configure retry-risk score Scheduler quantum: %v", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE projects SET running_limit = 3 WHERE id = $1
	`, testProjectID); err != nil {
		t.Fatalf("configure retry-risk score Project capacity: %v", err)
	}
	seedSchedulerCapacityShares(t, database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   3,
		ProjectWeight:  1,
	})
	workerIDs := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000381"),
		uuid.MustParse("00000000-0000-0000-0000-000000000382"),
		uuid.MustParse("00000000-0000-0000-0000-000000000383"),
	}
	workers := make([]schedulerWorker, len(workerIDs))
	for index, workerID := range workerIDs {
		workers[index] = schedulerWorker{
			ID:       workerID,
			SPIFFEID: fmt.Sprintf("spiffe://vela.internal/worker/retry-risk-score-%d", index),
			Epoch:    7,
		}
	}
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, workers...)

	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create retry-risk score coordinator: %v", err)
	}
	schedulerPool := newRolePool(t, database.DSN, "vela_scheduler_login", "vela-scheduler-password")
	scheduling, err := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "scheduler-retry-risk-score",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create retry-risk score Scheduler: %v", err)
	}
	for index, wantJob := range []uuid.UUID{safeLongJob, riskyShortJob, safeLongestJob} {
		dispatch, ok, runErr := scheduling.RunOnce(context.Background(), poolID)
		if runErr != nil || !ok {
			t.Fatalf("retry-risk score dispatch %d = ok %t error %v", index, ok, runErr)
		}
		if dispatch.Assignment.JobID != wantJob || dispatch.Lane != "NORMAL" {
			t.Fatalf(
				"retry-risk score dispatch %d = Job %s lane %s, want Job %s lane NORMAL",
				index,
				dispatch.Assignment.JobID,
				dispatch.Lane,
				wantJob,
			)
		}
	}
}

func TestSchedulerRetryLaneIsPrioritizedAndCappedAcrossReplicas(t *testing.T) {
	fixture := newAssignmentFixture(t, "scheduler-retry-first", 7)
	firstAssignment, err := fixture.service.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create first retry fixture Assignment: %v", err)
	}
	if started, startErr := fixture.service.Start(
		context.Background(),
		fixture.worker,
		leaseCredentials(firstAssignment),
	); startErr != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start first retry fixture = %#v error=%v", started, startErr)
	}
	if decision, failErr := fixture.service.Fail(
		context.Background(),
		fixture.worker,
		leaseCredentials(firstAssignment),
		validFailureObservation(),
	); failErr != nil || decision.Disposition != workercontrol.RetryDispositionRetryWait {
		t.Fatalf("fail first retry fixture = %#v error=%v", decision, failErr)
	}

	server := admissionServerForDatabase(t, fixture.database)
	secondRetryJob := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-retry-second",
	)
	secondCandidate := workercontrol.AssignmentCandidate{
		JobID:                      secondRetryJob,
		ExpectedJobVersion:         1,
		ExecutionProfileRevisionID: uuid.MustParse("00000000-0000-0000-0000-000000000014"),
	}
	secondAssignment, err := fixture.service.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&secondCandidate,
	)
	if err != nil {
		t.Fatalf("create second retry fixture Assignment: %v", err)
	}
	if started, startErr := fixture.service.Start(
		context.Background(),
		fixture.worker,
		leaseCredentials(secondAssignment),
	); startErr != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start second retry fixture = %#v error=%v", started, startErr)
	}
	if decision, failErr := fixture.service.Fail(
		context.Background(),
		fixture.worker,
		leaseCredentials(secondAssignment),
		validFailureObservation(),
	); failErr != nil || decision.Disposition != workercontrol.RetryDispositionRetryWait {
		t.Fatalf("fail second retry fixture = %#v error=%v", decision, failErr)
	}
	ordinaryJob := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-retry-ordinary",
	)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE retry_runtime_states
		SET next_retry_at = clock_timestamp() - interval '1 second'
		WHERE job_id = ANY($1)
	`, []uuid.UUID{firstAssignment.JobID, secondRetryJob}); err != nil {
		t.Fatalf("make Retry Jobs schedulable: %v", err)
	}

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	if _, err := fixture.database.Admin.Exec(`
		UPDATE worker_pools SET retry_running_limit = 1 WHERE id = $1
	`, poolID); err != nil {
		t.Fatalf("set retry lane cap: %v", err)
	}
	seedSchedulerCapacityShares(t, fixture.database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   2,
		ProjectWeight:  1,
	})
	workers := make([]schedulerWorker, 2)
	for index := range 2 {
		workerID := uuid.MustParse(fmt.Sprintf(
			"00000000-0000-0000-0000-%012d",
			380+index,
		))
		workers[index] = schedulerWorker{
			ID:       workerID,
			SPIFFEID: fmt.Sprintf("spiffe://vela.internal/worker/retry-%d", index),
			Epoch:    7,
		}
	}
	seedSchedulerWorkers(t, fixture.database.Admin, poolID, profileID, workers...)
	internalPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create retry Scheduler coordinator: %v", err)
	}
	schedulers := make([]*scheduler.Service, 0, 2)
	for index := range 2 {
		schedulerPool := newRolePool(
			t,
			fixture.database.DSN,
			"vela_scheduler_login",
			"vela-scheduler-password",
		)
		scheduling, serviceErr := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
			SchedulerID:       fmt.Sprintf("scheduler-retry-replica-%d", index),
			ClaimTTL:          30 * time.Second,
			CandidateAttempts: 3,
		})
		if serviceErr != nil {
			t.Fatalf("create retry Scheduler replica %d: %v", index, serviceErr)
		}
		schedulers = append(schedulers, scheduling)
	}

	type dispatchResult struct {
		dispatch scheduler.Dispatch
		ok       bool
		err      error
	}
	start := make(chan struct{})
	results := make(chan dispatchResult, 2)
	var wait sync.WaitGroup
	for _, scheduling := range schedulers {
		wait.Add(1)
		go func(service *scheduler.Service) {
			defer wait.Done()
			<-start
			dispatch, ok, runErr := service.RunOnce(context.Background(), poolID)
			results <- dispatchResult{dispatch: dispatch, ok: ok, err: runErr}
		}(scheduling)
	}
	close(start)
	wait.Wait()
	close(results)
	lanes := map[string]int{}
	dispatchedJobs := map[uuid.UUID]bool{}
	for result := range results {
		if result.err != nil || !result.ok {
			var postgresError *pgconn.PgError
			if errors.As(result.err, &postgresError) {
				t.Fatalf(
					"concurrent retry dispatch = ok %t PostgreSQL %s detail=%s where=%s",
					result.ok,
					postgresError.Code,
					postgresError.Detail,
					postgresError.Where,
				)
			}
			t.Fatalf("concurrent retry dispatch = ok %t error %v", result.ok, result.err)
		}
		lanes[string(result.dispatch.Lane)]++
		dispatchedJobs[result.dispatch.Assignment.JobID] = true
	}
	if lanes["RETRY"] != 1 || lanes["NORMAL"] != 1 || !dispatchedJobs[ordinaryJob] {
		t.Fatalf("concurrent retry lanes = %v Jobs=%v", lanes, dispatchedJobs)
	}
	var activeRetries int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*)
		FROM attempts
		WHERE worker_pool_id = $1
		  AND attempt_number > 1
		  AND state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
	`, poolID).Scan(&activeRetries); err != nil {
		t.Fatalf("read active retry lane: %v", err)
	}
	if activeRetries != 1 {
		t.Fatalf("active retry Attempts = %d, want 1", activeRetries)
	}
}

func TestSchedulerProjectionSkipsRetryWhenLaneIsFull(t *testing.T) {
	fixture := newAssignmentFixture(t, "scheduler-projection-retry-cap-first", 7)
	firstAssignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create first projection retry Assignment: %v", err)
	}
	if started, startErr := fixture.service.Start(
		context.Background(), fixture.worker, leaseCredentials(firstAssignment),
	); startErr != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start first projection retry = %#v error=%v", started, startErr)
	}
	if decision, failErr := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(firstAssignment), validFailureObservation(),
	); failErr != nil || decision.Disposition != workercontrol.RetryDispositionRetryWait {
		t.Fatalf("fail first projection retry = %#v error=%v", decision, failErr)
	}

	server := admissionServerForDatabase(t, fixture.database)
	waitingRetryJobID := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-projection-retry-cap-waiting",
	)
	waitingCandidate := workercontrol.AssignmentCandidate{
		JobID:                      waitingRetryJobID,
		ExpectedJobVersion:         1,
		ExecutionProfileRevisionID: fixture.candidate.ExecutionProfileRevisionID,
	}
	waitingAssignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &waitingCandidate,
	)
	if err != nil {
		t.Fatalf("create waiting projection retry Assignment: %v", err)
	}
	if started, startErr := fixture.service.Start(
		context.Background(), fixture.worker, leaseCredentials(waitingAssignment),
	); startErr != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start waiting projection retry = %#v error=%v", started, startErr)
	}
	if decision, failErr := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(waitingAssignment), validFailureObservation(),
	); failErr != nil || decision.Disposition != workercontrol.RetryDispositionRetryWait {
		t.Fatalf("fail waiting projection retry = %#v error=%v", decision, failErr)
	}
	ordinaryJobID := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-projection-retry-cap-ordinary",
	)

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	if _, err := fixture.database.Admin.Exec(`
		UPDATE worker_pools SET retry_running_limit = 1 WHERE id = $1
	`, poolID); err != nil {
		t.Fatalf("set full projection retry lane limit: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE retry_runtime_states
		SET next_retry_at = clock_timestamp() - interval '1 second'
		WHERE job_id IN ($1, $2)
	`, firstAssignment.JobID, waitingRetryJobID); err != nil {
		t.Fatalf("make projection Retry Jobs schedulable: %v", err)
	}
	seedSchedulerCapacityShares(t, fixture.database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   3,
		ProjectWeight:  1,
	})
	seedSchedulerWorkerReadiness(
		t,
		fixture.database.Admin,
		fixture.worker.ID,
		7,
		fixture.candidate.ExecutionProfileRevisionID,
	)
	activeRetryWorkerID := uuid.MustParse("00000000-0000-0000-0000-000000000386")
	seedSchedulerWorkers(
		t,
		fixture.database.Admin,
		poolID,
		fixture.candidate.ExecutionProfileRevisionID,
		schedulerWorker{
			ID:       activeRetryWorkerID,
			SPIFFEID: "spiffe://vela.internal/worker/projection-active-retry",
			Epoch:    7,
		},
	)
	var firstRetryVersion int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT version FROM jobs WHERE id = $1
	`, firstAssignment.JobID).Scan(&firstRetryVersion); err != nil {
		t.Fatalf("read first projection retry version: %v", err)
	}
	activeRetryCandidate := workercontrol.AssignmentCandidate{
		JobID:                      firstAssignment.JobID,
		ExpectedJobVersion:         firstRetryVersion,
		ExecutionProfileRevisionID: fixture.candidate.ExecutionProfileRevisionID,
	}
	if _, err := fixture.service.Acquire(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: activeRetryWorkerID},
		7,
		&activeRetryCandidate,
	); err != nil {
		t.Fatalf("fill projection retry lane: %v", err)
	}

	var waitingRetryRows, ordinaryRows int
	var ordinaryStart, projectedAt time.Time
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			count(*) FILTER (WHERE job_id = $1),
			count(*) FILTER (WHERE job_id = $2),
			min(predicted_start_at) FILTER (WHERE job_id = $2),
			min(projected_at) FILTER (WHERE job_id = $2)
		FROM vela_scheduler_queue_projection()
		WHERE job_id IN ($1, $2)
	`, waitingRetryJobID, ordinaryJobID).Scan(
		&waitingRetryRows,
		&ordinaryRows,
		&ordinaryStart,
		&projectedAt,
	); err != nil {
		t.Fatalf("read full retry-lane queue projection: %v", err)
	}
	if waitingRetryRows != 0 || ordinaryRows != 1 || ordinaryStart.Sub(projectedAt) > time.Second {
		t.Fatalf(
			"full retry-lane projection = waiting retry rows %d ordinary rows %d ordinary wait %s",
			waitingRetryRows,
			ordinaryRows,
			ordinaryStart.Sub(projectedAt),
		)
	}
}

func TestSchedulerProjectionDoesNotPlaceRetryOnExcludedWorker(t *testing.T) {
	fixture := newAssignmentFixture(t, "scheduler-projection-excluded-worker", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create excluded-Worker projection Assignment: %v", err)
	}
	if started, startErr := fixture.service.Start(
		context.Background(), fixture.worker, leaseCredentials(assignment),
	); startErr != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start excluded-Worker projection Assignment = %#v error=%v", started, startErr)
	}
	if decision, failErr := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(assignment), validFailureObservation(),
	); failErr != nil || decision.Disposition != workercontrol.RetryDispositionRetryWait {
		t.Fatalf("fail excluded-Worker projection Assignment = %#v error=%v", decision, failErr)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE retry_runtime_states
		SET next_retry_at = clock_timestamp() - interval '1 second'
		WHERE job_id = $1
	`, assignment.JobID); err != nil {
		t.Fatalf("make excluded-Worker Retry Job schedulable: %v", err)
	}

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := fixture.candidate.ExecutionProfileRevisionID
	seedSchedulerCapacityShares(t, fixture.database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   2,
		ProjectWeight:  1,
	})
	seedSchedulerWorkerReadiness(
		t,
		fixture.database.Admin,
		fixture.worker.ID,
		7,
		profileID,
	)
	compatibleWorkerID := uuid.MustParse("00000000-0000-0000-0000-000000000387")
	seedSchedulerWorkers(
		t,
		fixture.database.Admin,
		poolID,
		profileID,
		schedulerWorker{
			ID:       compatibleWorkerID,
			SPIFFEID: "spiffe://vela.internal/worker/projection-retry-compatible",
			Epoch:    7,
		},
	)

	var projectedWorkerID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT projected_worker_id
		FROM vela_scheduler_queue_projection()
		WHERE job_id = $1
		LIMIT 1
	`, assignment.JobID).Scan(&projectedWorkerID); err != nil {
		t.Fatalf("read excluded-Worker retry projection: %v", err)
	}
	if projectedWorkerID != compatibleWorkerID {
		t.Fatalf(
			"Retry Job projected Worker = %s, want non-excluded Worker %s",
			projectedWorkerID,
			compatibleWorkerID,
		)
	}
}

func TestSchedulerCandidateSnapshotFailsClosedAbovePoolWaitingBound(t *testing.T) {
	fixture := newAssignmentFixture(t, "scheduler-candidate-snapshot-bound", 7)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := fixture.candidate.ExecutionProfileRevisionID
	seedSchedulerCapacityShares(t, fixture.database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   2,
		ProjectWeight:  1,
	})
	seedSchedulerWorkerReadiness(
		t,
		fixture.database.Admin,
		fixture.worker.ID,
		7,
		profileID,
	)
	seedSchedulerJobForServiceClass(
		t,
		fixture.database.Admin,
		fixture.candidate.JobID,
		uuid.MustParse("00000000-0000-0000-0000-000000000012"),
		uuid.MustParse("00000000-0000-0000-0000-000000000017"),
		"standard",
		time.Now().UTC(),
		nil,
	)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE worker_pools
		SET queued_limit = 1,
			queued_count = 1
		WHERE id = $1
	`, poolID); err != nil {
		t.Fatalf("create impossible Scheduler pool waiting-count state: %v", err)
	}

	schedulerPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_scheduler_login",
		"vela-scheduler-password",
	)
	var claimedJobID uuid.UUID
	err := schedulerPool.QueryRow(context.Background(), `
		SELECT job_id
		FROM vela_claim_scheduler_dispatch($1, 'scheduler-candidate-snapshot-bound', 30)
	`, poolID).Scan(&claimedJobID)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) ||
		postgresError.Code != "55000" ||
		postgresError.ConstraintName != "scheduler_candidate_snapshot_exceeds_pool_waiting_bound" {
		t.Fatalf(
			"Scheduler candidate snapshot overflow = Job %s error %v",
			claimedJobID,
			err,
		)
	}
}

func TestSchedulerPoolScopedOperationsAndRunCycleIsolateUnrelatedWorkerPools(t *testing.T) {
	fixture := newAssignmentFixture(t, "scheduler-per-pool-candidate-projection", 7)
	primaryPoolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	primaryProfileID := fixture.candidate.ExecutionProfileRevisionID
	seedSchedulerCapacityShares(t, fixture.database.Admin, primaryPoolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   2,
		ProjectWeight:  1,
	})
	seedSchedulerWorkerReadiness(
		t,
		fixture.database.Admin,
		fixture.worker.ID,
		7,
		primaryProfileID,
	)

	unrelatedPoolID := uuid.MustParse("00000000-0000-0000-0000-000000000004")
	unrelatedProfileID := uuid.MustParse("00000000-0000-0000-0000-000000000491")
	unrelatedCertificationID := uuid.MustParse("00000000-0000-0000-0000-000000000492")
	unrelatedWorkerID := uuid.MustParse("00000000-0000-0000-0000-000000000493")
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO worker_pools (id, stable_id, queued_limit, queued_count)
		VALUES ($1, 'scheduler-unrelated-overflow', 10, 0)
	`, unrelatedPoolID); err != nil {
		t.Fatalf("seed unrelated Scheduler pool: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO execution_profile_revisions (
			id, model_revision_id, worker_pool_id, stable_id, revision, state
		) VALUES (
			$2, '00000000-0000-0000-0000-000000000010', $1,
			'h3-unrelated-overflow', 1, 'ACTIVE'
		)
	`, unrelatedPoolID, unrelatedProfileID); err != nil {
		t.Fatalf("seed unrelated Scheduler profile: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO profile_certifications (
			id, model_revision_id, generation_preset_revision_id, output_spec_id,
			execution_profile_revision_id, state, evidence_digest, certified_at
		) VALUES (
			$1,
			'00000000-0000-0000-0000-000000000010',
			'00000000-0000-0000-0000-000000000011',
			'00000000-0000-0000-0000-000000000013',
			$2,
			'ACTIVE',
			'unrelated-pool-overflow-v1',
			clock_timestamp()
		)
	`, unrelatedCertificationID, unrelatedProfileID); err != nil {
		t.Fatalf("seed unrelated Scheduler certification: %v", err)
	}
	seedSchedulerCapacityShares(t, fixture.database.Admin, unrelatedPoolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   2,
		ProjectWeight:  1,
	})
	seedSchedulerWorkers(
		t,
		fixture.database.Admin,
		unrelatedPoolID,
		unrelatedProfileID,
		schedulerWorker{
			ID:       unrelatedWorkerID,
			SPIFFEID: "spiffe://vela.internal/worker/scheduler-unrelated-overflow",
			Epoch:    7,
		},
	)
	unrelatedJobID := seedSchedulerJobForWorkerPool(
		t,
		fixture.database.Admin,
		fixture.candidate.JobID,
		unrelatedPoolID,
		uuid.MustParse("00000000-0000-0000-0000-000000000012"),
		uuid.MustParse("00000000-0000-0000-0000-000000000017"),
		"standard",
		time.Now().UTC(),
		nil,
	)
	seedSchedulerJobForWorkerPool(
		t,
		fixture.database.Admin,
		fixture.candidate.JobID,
		unrelatedPoolID,
		uuid.MustParse("00000000-0000-0000-0000-000000000012"),
		uuid.MustParse("00000000-0000-0000-0000-000000000017"),
		"standard",
		time.Now().UTC(),
		nil,
	)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE worker_pools
		SET queued_limit = 1, queued_count = 1
		WHERE id = $1
	`, unrelatedPoolID); err != nil {
		t.Fatalf("create unrelated Scheduler counter drift: %v", err)
	}

	schedulerPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_scheduler_login",
		"vela-scheduler-password",
	)
	var dynamicETA time.Time
	if err := schedulerPool.QueryRow(context.Background(), `
		SELECT predicted_finish_at
		FROM vela_predict_job_dynamic_eta($1)
	`, fixture.candidate.JobID).Scan(&dynamicETA); err != nil {
		t.Fatalf("predict target-pool Dynamic ETA with unrelated projection overflow: %v", err)
	}
	var admissionFinish time.Time
	if err := schedulerPool.QueryRow(context.Background(), `
		SELECT predicted_finish_at
		FROM vela_predict_admission_capacity($1, $2, $3, $4, $5, 1)
	`,
		primaryPoolID,
		uuid.MustParse("00000000-0000-0000-0000-000000000010"),
		uuid.MustParse("00000000-0000-0000-0000-000000000011"),
		uuid.MustParse("00000000-0000-0000-0000-000000000012"),
		uuid.MustParse("00000000-0000-0000-0000-000000000013"),
	).Scan(&admissionFinish); err != nil {
		t.Fatalf("predict target-pool Admission with unrelated projection overflow: %v", err)
	}
	coordinator, err := newWorkerControlService(newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	))
	if err != nil {
		t.Fatalf("create per-pool Scheduler coordinator: %v", err)
	}
	scheduling, err := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "scheduler-per-pool-candidate-projection",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create per-pool Scheduler: %v", err)
	}
	dispatches, cycleErr := scheduling.RunCycle(context.Background())
	var postgresError *pgconn.PgError
	if !errors.As(cycleErr, &postgresError) ||
		postgresError.Code != "55000" ||
		postgresError.ConstraintName != "scheduler_candidate_snapshot_exceeds_pool_waiting_bound" {
		t.Fatalf(
			"per-pool Scheduler cycle error = %v, want pool-local candidate bound error",
			cycleErr,
		)
	}
	if len(dispatches) != 1 || dispatches[0].Assignment.JobID != fixture.candidate.JobID {
		t.Fatalf(
			"per-pool Scheduler cycle dispatches = %#v, want healthy-pool Job %s despite bad pool Job %s",
			dispatches,
			fixture.candidate.JobID,
			unrelatedJobID,
		)
	}
}

func TestScheduledAcquireCountsOrganizationCapacityAfterWaitingForAuthorityLock(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Scheduler capacity serialization fixture")
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	firstJobID := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-capacity-serialization-first",
	)
	secondJobID := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-capacity-serialization-second",
	)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	seedSchedulerCapacityShares(t, database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   1,
		ProjectWeight:  1,
	})
	seedSchedulerWorkers(
		t,
		database.Admin,
		poolID,
		profileID,
		schedulerWorker{
			ID:       uuid.MustParse("00000000-0000-0000-0000-000000000390"),
			SPIFFEID: "spiffe://vela.internal/worker/capacity-serialization-first",
			Epoch:    7,
		},
		schedulerWorker{
			ID:       uuid.MustParse("00000000-0000-0000-0000-000000000391"),
			SPIFFEID: "spiffe://vela.internal/worker/capacity-serialization-second",
			Epoch:    7,
		},
	)
	if _, err := database.Admin.Exec(`
		SELECT vela_transition_scheduler_dispatch_protocol(
			true,
			'capacity authority concurrency fixture uses exact Scheduler claims'
		)
	`); err != nil {
		t.Fatalf("enable Scheduler protocol for capacity serialization: %v", err)
	}

	type claimedAssignment struct {
		intentID                   uuid.UUID
		jobID                      uuid.UUID
		expectedJobVersion         int64
		workerID                   uuid.UUID
		workerEpoch                int64
		executionProfileRevisionID uuid.UUID
	}
	schedulerPool := newRolePool(
		t,
		database.DSN,
		"vela_scheduler_login",
		"vela-scheduler-password",
	)
	claim := func(schedulerID string) claimedAssignment {
		t.Helper()
		var result claimedAssignment
		if err := schedulerPool.QueryRow(context.Background(), `
			SELECT
				intent_id,
				job_id,
				expected_job_version,
				worker_id,
				worker_epoch,
				execution_profile_revision_id
			FROM vela_claim_scheduler_dispatch($1, $2, 30)
		`, poolID, schedulerID).Scan(
			&result.intentID,
			&result.jobID,
			&result.expectedJobVersion,
			&result.workerID,
			&result.workerEpoch,
			&result.executionProfileRevisionID,
		); err != nil {
			t.Fatalf("claim %s capacity serialization Assignment: %v", schedulerID, err)
		}
		return result
	}
	firstClaim := claim("scheduler-capacity-serialization-first")
	secondClaim := claim("scheduler-capacity-serialization-second")
	if firstClaim.jobID == secondClaim.jobID || firstClaim.workerID == secondClaim.workerID ||
		!map[uuid.UUID]bool{firstJobID: true, secondJobID: true}[firstClaim.jobID] ||
		!map[uuid.UUID]bool{firstJobID: true, secondJobID: true}[secondClaim.jobID] {
		t.Fatalf("capacity serialization claims = first %#v second %#v", firstClaim, secondClaim)
	}

	const advisoryLockKey int64 = 580010
	if _, err := database.Admin.Exec(`
		CREATE FUNCTION vela_test_pause_capacity_assignment() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock(580010);
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER attempts_pause_capacity_assignment
		BEFORE INSERT ON attempts
		FOR EACH ROW EXECUTE FUNCTION vela_test_pause_capacity_assignment();
	`); err != nil {
		t.Fatalf("install capacity serialization pause trigger: %v", err)
	}
	blocker, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin capacity serialization blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	if _, err := blocker.Exec("SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		t.Fatalf("acquire capacity serialization blocker: %v", err)
	}

	internalPool := newRolePool(
		t,
		database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create capacity serialization coordinator: %v", err)
	}
	type acquireResult struct {
		assignment workercontrol.Assignment
		err        error
	}
	acquire := func(claimed claimedAssignment) acquireResult {
		candidate := workercontrol.AssignmentCandidate{
			JobID:                      claimed.jobID,
			ExpectedJobVersion:         claimed.expectedJobVersion,
			ExecutionProfileRevisionID: claimed.executionProfileRevisionID,
			SchedulerClaim: &workercontrol.SchedulerClaim{
				IntentID:     claimed.intentID,
				WorkerPoolID: poolID,
			},
		}
		assignment, acquireErr := coordinator.Acquire(
			context.Background(),
			workercontrol.AuthenticatedWorker{ID: claimed.workerID},
			claimed.workerEpoch,
			&candidate,
		)
		return acquireResult{assignment: assignment, err: acquireErr}
	}
	firstResults := make(chan acquireResult, 1)
	go func() { firstResults <- acquire(firstClaim) }()
	firstWaitDeadline := time.Now().Add(6 * time.Second)
	for {
		select {
		case firstResult := <-firstResults:
			t.Fatalf(
				"first capacity Acquire completed before pause = %#v error=%v",
				firstResult.assignment,
				firstResult.err,
			)
		default:
		}
		var waiting bool
		if err := database.Admin.QueryRow(`
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE usename = 'vela_internal_login' AND wait_event_type = 'Lock'
			)
		`).Scan(&waiting); err != nil {
			t.Fatalf("inspect first capacity lock wait: %v", err)
		}
		if waiting {
			break
		}
		if time.Now().After(firstWaitDeadline) {
			t.Fatal("first capacity Acquire did not reach the pause trigger")
		}
		time.Sleep(10 * time.Millisecond)
	}
	secondResults := make(chan acquireResult, 1)
	go func() { secondResults <- acquire(secondClaim) }()

	deadline := time.Now().Add(6 * time.Second)
	for {
		var waiting int
		if err := database.Admin.QueryRow(`
			SELECT count(*)
			FROM pg_stat_activity
			WHERE usename = 'vela_internal_login' AND wait_event_type = 'Lock'
		`).Scan(&waiting); err != nil {
			t.Fatalf("inspect concurrent capacity lock waits: %v", err)
		}
		if waiting >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("capacity Assignments waiting for locks = %d, want 2", waiting)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var unlocked bool
	if err := blocker.QueryRow("SELECT pg_advisory_unlock($1)", advisoryLockKey).Scan(&unlocked); err != nil {
		t.Fatalf("release capacity serialization blocker: %v", err)
	}
	if !unlocked {
		t.Fatal("capacity serialization blocker was not held")
	}
	if err := blocker.Commit(); err != nil {
		t.Fatalf("commit capacity serialization blocker: %v", err)
	}

	firstResult := <-firstResults
	if firstResult.err != nil || firstResult.assignment.JobID != firstClaim.jobID {
		t.Fatalf("first serialized capacity Acquire = %#v error=%v", firstResult.assignment, firstResult.err)
	}
	secondResult := <-secondResults
	var failure *workercontrol.Failure
	if !errors.As(secondResult.err, &failure) || failure.Code != workercontrol.FailureCandidateUnavailable {
		t.Fatalf("second serialized capacity Acquire = %#v error=%v", secondResult.assignment, secondResult.err)
	}
	var activeAssignments int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM attempts
		WHERE organization_id = $1
		  AND worker_pool_id = $2
		  AND state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
	`, testOrganizationID, poolID).Scan(&activeAssignments); err != nil {
		t.Fatalf("read serialized Organization capacity: %v", err)
	}
	if activeAssignments != 1 {
		t.Fatalf("active Organization Assignments = %d, want hard limit 1", activeAssignments)
	}
}

func TestSchedulerReclaimsCrashBeforeAcquireAndReplicasAssignOnce(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Scheduler crash recovery fixture")
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	jobID := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-crash-before-acquire",
	)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	workerID := uuid.MustParse("00000000-0000-0000-0000-000000000390")
	seedSchedulerCapacityShares(t, database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   1,
		ProjectWeight:  1,
	})
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, schedulerWorker{
		ID:       workerID,
		SPIFFEID: "spiffe://vela.internal/worker/scheduler-crash",
		Epoch:    7,
	})

	crashedSchedulerPool := newRolePool(
		t,
		database.DSN,
		"vela_scheduler_login",
		"vela-scheduler-password",
	)
	var crashedIntentID uuid.UUID
	if err := crashedSchedulerPool.QueryRow(context.Background(), `
		SELECT intent_id
		FROM vela_claim_scheduler_dispatch($1, 'scheduler-crashed', 30)
	`, poolID).Scan(&crashedIntentID); err != nil {
		t.Fatalf("claim dispatch before simulated Scheduler crash: %v", err)
	}
	var (
		jobState    string
		workerState string
		attempts    int
	)
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT state::text FROM jobs WHERE id = $1),
			(SELECT lifecycle_state::text FROM workers WHERE id = $2),
			(SELECT count(*) FROM attempts WHERE job_id = $1)
	`, jobID, workerID).Scan(&jobState, &workerState, &attempts); err != nil {
		t.Fatalf("read state after crash-before-Acquire claim: %v", err)
	}
	if jobState != "QUEUED" || workerState != "READY" || attempts != 0 {
		t.Fatalf(
			"crash-before-Acquire state = Job %s Worker %s Attempts %d",
			jobState,
			workerState,
			attempts,
		)
	}

	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create crash recovery coordinator: %v", err)
	}
	blockedScheduler, err := scheduler.NewService(crashedSchedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "scheduler-before-expiry",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 2,
	})
	if err != nil {
		t.Fatalf("create pre-expiry Scheduler: %v", err)
	}
	if dispatch, ok, runErr := blockedScheduler.RunOnce(context.Background(), poolID); runErr != nil || ok {
		t.Fatalf("pre-expiry dispatch = %#v ok=%t error=%v", dispatch, ok, runErr)
	}
	if _, err := database.Admin.Exec(`
		UPDATE scheduler_dispatch_intents
		SET claimed_at = clock_timestamp() - interval '2 seconds',
			claim_expires_at = clock_timestamp() - interval '1 second'
		WHERE id = $1 AND state = 'CLAIMED'
	`, crashedIntentID); err != nil {
		t.Fatalf("expire crashed Scheduler claim: %v", err)
	}
	if reconciled, reconcileErr := blockedScheduler.ReconcileExpired(context.Background()); reconcileErr != nil || reconciled != 1 {
		t.Fatalf("reconcile expired Scheduler claim = %d error=%v", reconciled, reconcileErr)
	}

	schedulers := make([]*scheduler.Service, 0, 2)
	for index := range 2 {
		schedulerPool := newRolePool(
			t,
			database.DSN,
			"vela_scheduler_login",
			"vela-scheduler-password",
		)
		scheduling, serviceErr := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
			SchedulerID:       fmt.Sprintf("scheduler-crash-recovery-%d", index),
			ClaimTTL:          30 * time.Second,
			CandidateAttempts: 2,
		})
		if serviceErr != nil {
			t.Fatalf("create crash recovery Scheduler %d: %v", index, serviceErr)
		}
		schedulers = append(schedulers, scheduling)
	}
	type crashDispatchResult struct {
		dispatch scheduler.Dispatch
		ok       bool
		err      error
	}
	start := make(chan struct{})
	results := make(chan crashDispatchResult, 2)
	var wait sync.WaitGroup
	for _, scheduling := range schedulers {
		wait.Add(1)
		go func(service *scheduler.Service) {
			defer wait.Done()
			<-start
			dispatch, ok, runErr := service.RunOnce(context.Background(), poolID)
			results <- crashDispatchResult{dispatch: dispatch, ok: ok, err: runErr}
		}(scheduling)
	}
	close(start)
	wait.Wait()
	close(results)
	successes := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("crash recovery Scheduler error: %v", result.err)
		}
		if result.ok {
			successes++
			if result.dispatch.Assignment.JobID != jobID {
				t.Fatalf("crash recovery assigned Job %s, want %s", result.dispatch.Assignment.JobID, jobID)
			}
		}
	}
	if successes != 1 {
		t.Fatalf("crash recovery successful dispatches = %d, want 1", successes)
	}
	var (
		crashedState string
		committed    int
		jobAttempts  int
		jobEvents    int
	)
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT state::text FROM scheduler_dispatch_intents WHERE id = $1),
			(SELECT count(*) FROM scheduler_dispatch_intents
			 WHERE job_id = $2 AND state = 'COMMITTED'),
			(SELECT count(*) FROM attempts WHERE job_id = $2),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = $2 AND event_type = 'job.assigned')
	`, crashedIntentID, jobID).Scan(
		&crashedState,
		&committed,
		&jobAttempts,
		&jobEvents,
	); err != nil {
		t.Fatalf("read crash recovery receipts: %v", err)
	}
	if crashedState != "ABANDONED" || committed != 1 || jobAttempts != 1 || jobEvents != 1 {
		t.Fatalf(
			"crash recovery receipts = crashed %s committed %d Attempts %d events %d",
			crashedState,
			committed,
			jobAttempts,
			jobEvents,
		)
	}
}

func TestSchedulerCrashBeforeClaimCommitAndAfterAssignmentCommitLeavesOneAssignment(t *testing.T) {
	fixture := newAssignmentFixture(t, "scheduler-crash-commit-boundaries", 7)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	seedSchedulerCapacityShares(t, fixture.database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   1,
		ProjectWeight:  1,
	})
	seedSchedulerWorkerReadiness(
		t,
		fixture.database.Admin,
		fixture.worker.ID,
		7,
		fixture.candidate.ExecutionProfileRevisionID,
	)

	crashedSchedulerPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_scheduler_login",
		"vela-scheduler-password",
	)
	tx, err := crashedSchedulerPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin crash-before-claim-commit transaction: %v", err)
	}
	var rolledBackIntentID uuid.UUID
	if err := tx.QueryRow(context.Background(), `
		SELECT intent_id
		FROM vela_claim_scheduler_dispatch($1, 'scheduler-crash-before-claim-commit', 30)
	`, poolID).Scan(&rolledBackIntentID); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatalf("claim before simulated pre-commit crash: %v", err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback simulated pre-commit Scheduler crash: %v", err)
	}
	var persistedRolledBackClaims int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*) FROM scheduler_dispatch_intents WHERE id = $1
	`, rolledBackIntentID).Scan(&persistedRolledBackClaims); err != nil {
		t.Fatalf("read rolled-back Scheduler claim: %v", err)
	}
	if persistedRolledBackClaims != 0 {
		t.Fatalf("pre-commit Scheduler crash persisted %d claims", persistedRolledBackClaims)
	}
	if _, err := fixture.database.Admin.Exec(`
		SELECT vela_transition_scheduler_dispatch_protocol(
			true,
			'pre-commit rollback verified; N-1 drained before commit-boundary receipt'
		)
	`); err != nil {
		t.Fatalf("enable Scheduler protocol for commit-boundary receipt: %v", err)
	}

	firstScheduler, err := scheduler.NewService(crashedSchedulerPool, fixture.service, scheduler.Config{
		SchedulerID:       "scheduler-before-assignment-commit",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 2,
	})
	if err != nil {
		t.Fatalf("create commit-boundary Scheduler: %v", err)
	}
	dispatch, ok, err := firstScheduler.RunOnce(context.Background(), poolID)
	if err != nil || !ok || dispatch.Assignment.JobID != fixture.candidate.JobID {
		t.Fatalf("commit-boundary dispatch = %#v ok=%t error=%v", dispatch, ok, err)
	}

	restartedSchedulerPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_scheduler_login",
		"vela-scheduler-password",
	)
	restartedScheduler, err := scheduler.NewService(
		restartedSchedulerPool,
		fixture.service,
		scheduler.Config{
			SchedulerID:       "scheduler-after-assignment-commit",
			ClaimTTL:          30 * time.Second,
			CandidateAttempts: 2,
		},
	)
	if err != nil {
		t.Fatalf("create restarted commit-boundary Scheduler: %v", err)
	}
	if duplicate, duplicateOK, runErr := restartedScheduler.RunOnce(
		context.Background(),
		poolID,
	); runErr != nil || duplicateOK {
		t.Fatalf("post-commit redispatch = %#v ok=%t error=%v", duplicate, duplicateOK, runErr)
	}

	var (
		committedClaims  int
		linkedAttempts   int
		attempts         int
		leases           int
		assignmentEvents int
		jobFence         int64
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM scheduler_dispatch_intents
			 WHERE job_id = $1 AND state = 'COMMITTED'),
			(SELECT count(*) FROM attempts
			 WHERE job_id = $1 AND scheduler_dispatch_intent_id = $2),
			(SELECT count(*) FROM attempts WHERE job_id = $1),
			(SELECT count(*) FROM attempt_leases AS lease
			 JOIN attempts AS attempt ON attempt.id = lease.attempt_id
			 WHERE attempt.job_id = $1),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = $1 AND event_type = 'job.assigned'),
			(SELECT current_fence FROM jobs WHERE id = $1)
	`, fixture.candidate.JobID, dispatch.IntentID).Scan(
		&committedClaims,
		&linkedAttempts,
		&attempts,
		&leases,
		&assignmentEvents,
		&jobFence,
	); err != nil {
		t.Fatalf("read Scheduler commit-boundary receipts: %v", err)
	}
	if committedClaims != 1 || linkedAttempts != 1 || attempts != 1 || leases != 1 ||
		assignmentEvents != 1 || jobFence != 1 {
		t.Fatalf(
			"commit-boundary receipts = claims %d linked Attempts %d Attempts %d Leases %d events %d fence %d",
			committedClaims,
			linkedAttempts,
			attempts,
			leases,
			assignmentEvents,
			jobFence,
		)
	}
}

func TestSchedulerUsesEveryCompatibleReadyWorkerByWorkerScore(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Scheduler Worker score fixture")
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	firstJob := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-worker-score-first",
	)
	secondJob := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-worker-score-second",
	)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	seedSchedulerCapacityShares(t, database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   2,
		ProjectWeight:  1,
	})
	higherPenaltyWorker := uuid.MustParse("00000000-0000-0000-0000-000000000400")
	lowerPenaltyWorker := uuid.MustParse("00000000-0000-0000-0000-000000000401")
	busyWorker := uuid.MustParse("00000000-0000-0000-0000-000000000402")
	suspectWorker := uuid.MustParse("00000000-0000-0000-0000-000000000404")
	uncertifiedWorker := uuid.MustParse("00000000-0000-0000-0000-000000000405")
	invalidatedWorker := uuid.MustParse("00000000-0000-0000-0000-000000000406")
	excludedWorker := uuid.MustParse("00000000-0000-0000-0000-000000000407")
	incompatibleWorker := uuid.MustParse("00000000-0000-0000-0000-000000000408")
	uncertifiedProfileID := uuid.MustParse("00000000-0000-0000-0000-000000000410")
	invalidatedProfileID := uuid.MustParse("00000000-0000-0000-0000-000000000411")
	incompatibleProfileID := uuid.MustParse("00000000-0000-0000-0000-000000000414")
	if _, err := database.Admin.Exec(`
		INSERT INTO execution_profile_revisions (
			id, model_revision_id, worker_pool_id, stable_id, revision, state
		) VALUES
			($1, '00000000-0000-0000-0000-000000000010', $4, 'h3-uncertified', 1, 'ACTIVE'),
			($2, '00000000-0000-0000-0000-000000000010', $4, 'h3-invalidated', 1, 'ACTIVE'),
			($3, '00000000-0000-0000-0000-000000000010', $4, 'h3-quality-only', 1, 'ACTIVE')
	`, uncertifiedProfileID, invalidatedProfileID, incompatibleProfileID, poolID); err != nil {
		t.Fatalf("seed ineligible ExecutionProfileRevisions: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO generation_preset_revisions (
			id, model_revision_id, stable_id, revision, state, certified_p95_compute_seconds
		) VALUES (
			'00000000-0000-0000-0000-000000000413',
			'00000000-0000-0000-0000-000000000010',
			'quality', 1, 'ACTIVE', 1200
		)
	`); err != nil {
		t.Fatalf("seed incompatible GenerationPresetRevision: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO profile_certifications (
			id, model_revision_id, generation_preset_revision_id, output_spec_id,
			execution_profile_revision_id, state, evidence_digest, certified_at, invalidated_at
		) VALUES
			(
				'00000000-0000-0000-0000-000000000412',
				'00000000-0000-0000-0000-000000000010',
				'00000000-0000-0000-0000-000000000011',
				'00000000-0000-0000-0000-000000000013',
				$1, 'INVALID', 'invalidated-profile-evidence', clock_timestamp(), clock_timestamp()
			),
			(
				'00000000-0000-0000-0000-000000000415',
				'00000000-0000-0000-0000-000000000010',
				'00000000-0000-0000-0000-000000000413',
				'00000000-0000-0000-0000-000000000013',
				$2, 'ACTIVE', 'incompatible-profile-evidence', clock_timestamp(), NULL
			)
	`, invalidatedProfileID, incompatibleProfileID); err != nil {
		t.Fatalf("seed ineligible ProfileCertifications: %v", err)
	}
	for index, worker := range []struct {
		id           uuid.UUID
		profileID    uuid.UUID
		lifecycle    string
		reachability string
		penalty      int
	}{
		{id: higherPenaltyWorker, profileID: profileID, lifecycle: "READY", reachability: "HEALTHY", penalty: 90},
		{id: lowerPenaltyWorker, profileID: profileID, lifecycle: "READY", reachability: "HEALTHY", penalty: 1},
		{id: busyWorker, profileID: profileID, lifecycle: "BUSY", reachability: "HEALTHY", penalty: 0},
		{id: suspectWorker, profileID: profileID, lifecycle: "READY", reachability: "SUSPECT", penalty: 0},
		{id: uncertifiedWorker, profileID: uncertifiedProfileID, lifecycle: "READY", reachability: "HEALTHY", penalty: 0},
		{id: invalidatedWorker, profileID: invalidatedProfileID, lifecycle: "READY", reachability: "HEALTHY", penalty: 0},
		{id: excludedWorker, profileID: profileID, lifecycle: "READY", reachability: "HEALTHY", penalty: 0},
		{id: incompatibleWorker, profileID: incompatibleProfileID, lifecycle: "READY", reachability: "HEALTHY", penalty: 0},
	} {
		if _, err := database.Admin.Exec(`
			INSERT INTO workers (
				id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
				reachability_condition
			) VALUES ($1, $2, $3, 7, $4, $5)
		`,
			worker.id,
			poolID,
			fmt.Sprintf("spiffe://vela.internal/worker/score-%d", index),
			worker.lifecycle,
			worker.reachability,
		); err != nil {
			t.Fatalf("seed scored Worker %d: %v", index, err)
		}
		if _, err := database.Admin.Exec(`
			INSERT INTO worker_profile_readiness (
				worker_id, worker_epoch, execution_profile_revision_id, readiness,
				model_cold_start_penalty_seconds
			) VALUES ($1, 7, $2, 'WARM', $3)
		`, worker.id, worker.profileID, worker.penalty); err != nil {
			t.Fatalf("seed scored Worker readiness %d: %v", index, err)
		}
	}
	staleReadinessWorker := uuid.MustParse("00000000-0000-0000-0000-000000000403")
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES ($1, $2, 'spiffe://vela.internal/worker/stale-readiness', 6, 'READY', 'HEALTHY')
	`, staleReadinessWorker, poolID); err != nil {
		t.Fatalf("seed stale-readiness Worker: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_profile_readiness (
			worker_id, worker_epoch, execution_profile_revision_id, readiness
		) VALUES ($1, 6, $2, 'WARM')
	`, staleReadinessWorker, profileID); err != nil {
		t.Fatalf("seed stale Worker readiness: %v", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE workers SET epoch = 7 WHERE id = $1
	`, staleReadinessWorker); err != nil {
		t.Fatalf("advance Worker beyond readiness epoch: %v", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE execution_retry_evidence
		SET excluded_workers = jsonb_build_array(jsonb_build_object(
			'worker_id', $1::text,
			'expires_at', clock_timestamp() + interval '1 hour'
		))
		WHERE job_id IN ($2, $3)
	`, excludedWorker, firstJob, secondJob); err != nil {
		t.Fatalf("exclude Worker from scheduled Jobs: %v", err)
	}

	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create Worker score coordinator: %v", err)
	}
	schedulerPool := newRolePool(t, database.DSN, "vela_scheduler_login", "vela-scheduler-password")
	scheduling, err := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "scheduler-worker-score",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 16,
	})
	if err != nil {
		t.Fatalf("create Worker score Scheduler: %v", err)
	}
	firstDispatch, ok, err := scheduling.RunOnce(context.Background(), poolID)
	if err != nil || !ok {
		t.Fatalf("first Worker score dispatch = ok %t error %v", ok, err)
	}
	if firstDispatch.Assignment.JobID != firstJob ||
		firstDispatch.Assignment.WorkerID != lowerPenaltyWorker {
		t.Fatalf(
			"first Worker score dispatch = Job %s Worker %s, want Job %s Worker %s",
			firstDispatch.Assignment.JobID,
			firstDispatch.Assignment.WorkerID,
			firstJob,
			lowerPenaltyWorker,
		)
	}
	secondDispatch, ok, err := scheduling.RunOnce(context.Background(), poolID)
	if err != nil || !ok {
		t.Fatalf("second Worker score dispatch = ok %t error %v", ok, err)
	}
	if secondDispatch.Assignment.JobID != secondJob ||
		secondDispatch.Assignment.WorkerID != higherPenaltyWorker {
		t.Fatalf(
			"second Worker score dispatch = Job %s Worker %s, want Job %s Worker %s",
			secondDispatch.Assignment.JobID,
			secondDispatch.Assignment.WorkerID,
			secondJob,
			higherPenaltyWorker,
		)
	}
	for label, workerID := range map[string]uuid.UUID{
		"BUSY":                 busyWorker,
		"SUSPECT":              suspectWorker,
		"stale-epoch":          staleReadinessWorker,
		"uncertified":          uncertifiedWorker,
		"invalidated":          invalidatedWorker,
		"excluded":             excludedWorker,
		"profile-incompatible": incompatibleWorker,
	} {
		var claims int
		var attempts int
		if err := database.Admin.QueryRow(`
			SELECT
				(SELECT count(*) FROM scheduler_dispatch_intents WHERE worker_id = $1),
				(SELECT count(*) FROM attempts WHERE worker_id = $1)
		`, workerID).Scan(&claims, &attempts); err != nil {
			t.Fatalf("read %s Worker scheduling evidence: %v", label, err)
		}
		if claims != 0 || attempts != 0 {
			t.Fatalf("%s Worker received %d claims and %d Attempts", label, claims, attempts)
		}
	}
}

func TestScheduledAcquireRejectsClaimWithMismatchedServiceClass(t *testing.T) {
	fixture := newAssignmentFixture(t, "scheduler-service-class-recheck", 7)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := fixture.candidate.ExecutionProfileRevisionID
	seedSchedulerCapacityShares(t, fixture.database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   1,
		ProjectWeight:  1,
	})
	seedSchedulerWorkerReadiness(t, fixture.database.Admin, fixture.worker.ID, 7, profileID)
	schedulerPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_scheduler_login",
		"vela-scheduler-password",
	)
	var intentID uuid.UUID
	if err := schedulerPool.QueryRow(context.Background(), `
		SELECT intent_id
		FROM vela_claim_scheduler_dispatch($1, 'scheduler-service-class-recheck', 30)
	`, poolID).Scan(&intentID); err != nil {
		t.Fatalf("claim scheduled Assignment: %v", err)
	}

	tamperedServiceClassID := uuid.MustParse("00000000-0000-0000-0000-000000000420")
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO service_class_revisions (
			id, stable_id, revision, state, queue_retry_allowance_seconds, max_attempts,
			max_total_compute_multiplier_milli, max_finalization_seconds_per_attempt,
			retry_backoff_policy, retryable_failure_classes, circuit_breaker_policy
		)
		SELECT
			$1, 'tampered-scheduler-class', 1, state, queue_retry_allowance_seconds,
			max_attempts, max_total_compute_multiplier_milli,
			max_finalization_seconds_per_attempt, retry_backoff_policy,
			retryable_failure_classes, circuit_breaker_policy
		FROM service_class_revisions
		WHERE id = '00000000-0000-0000-0000-000000000012'
	`, tamperedServiceClassID); err != nil {
		t.Fatalf("seed tampered ServiceClassRevision: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE scheduler_dispatch_intents
		SET service_class_revision_id = $1
		WHERE id = $2
	`, tamperedServiceClassID, intentID); err != nil {
		t.Fatalf("tamper claimed ServiceClassRevision: %v", err)
	}

	before := readAssignmentState(
		t,
		fixture.database.Admin,
		fixture.candidate.JobID,
		fixture.worker.ID,
	)
	candidate := fixture.candidate
	candidate.SchedulerClaim = &workercontrol.SchedulerClaim{
		IntentID:     intentID,
		WorkerPoolID: poolID,
	}
	_, err := fixture.service.Acquire(context.Background(), fixture.worker, 7, &candidate)
	var failure *workercontrol.Failure
	if !errors.As(err, &failure) || failure.Code != workercontrol.FailureCandidateUnavailable {
		t.Fatalf("Acquire with mismatched ServiceClassRevision claim error = %v", err)
	}
	after := readAssignmentState(
		t,
		fixture.database.Admin,
		fixture.candidate.JobID,
		fixture.worker.ID,
	)
	if after != before {
		t.Fatalf("rejected ServiceClassRevision claim changed Assignment state: before=%+v after=%+v", before, after)
	}
}

func TestScheduledAcquireRechecksMutableAuthoritiesAfterClaim(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*testing.T, scheduledAcquireFixture)
		wantCode workercontrol.FailureCode
	}{
		{
			name: "invalidated ProfileCertification",
			mutate: func(t *testing.T, fixture scheduledAcquireFixture) {
				if _, err := fixture.database.Admin.Exec(`
					UPDATE profile_certifications
					SET state = 'INVALID', invalidated_at = clock_timestamp()
					WHERE execution_profile_revision_id = $1
				`, fixture.candidate.ExecutionProfileRevisionID); err != nil {
					t.Fatalf("invalidate claimed ProfileCertification: %v", err)
				}
			},
			wantCode: workercontrol.FailureCandidateUnavailable,
		},
		{
			name: "advanced Worker epoch",
			mutate: func(t *testing.T, fixture scheduledAcquireFixture) {
				if _, err := fixture.database.Admin.Exec(`
					UPDATE workers SET epoch = 8, updated_at = clock_timestamp() WHERE id = $1
				`, fixture.worker.ID); err != nil {
					t.Fatalf("advance claimed Worker epoch: %v", err)
				}
			},
			wantCode: workercontrol.FailureStaleWorkerEpoch,
		},
		{
			name: "exhausted Organization running limit",
			mutate: func(t *testing.T, fixture scheduledAcquireFixture) {
				if _, err := fixture.database.Admin.Exec(`
					UPDATE organization_capacity_shares SET running_limit = 0
					WHERE worker_pool_id = $1 AND organization_id = $2
				`, fixture.poolID, testOrganizationID); err != nil {
					t.Fatalf("exhaust claimed Organization capacity: %v", err)
				}
			},
			wantCode: workercontrol.FailureCandidateUnavailable,
		},
		{
			name: "removed Worker profile readiness",
			mutate: func(t *testing.T, fixture scheduledAcquireFixture) {
				if _, err := fixture.database.Admin.Exec(`
					DELETE FROM worker_profile_readiness
					WHERE worker_id = $1 AND worker_epoch = 7
					  AND execution_profile_revision_id = $2
				`, fixture.worker.ID, fixture.candidate.ExecutionProfileRevisionID); err != nil {
					t.Fatalf("remove claimed Worker profile readiness: %v", err)
				}
			},
			wantCode: workercontrol.FailureCandidateUnavailable,
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newScheduledAcquireFixture(t, fmt.Sprintf("scheduler-recheck-%d", index))
			test.mutate(t, fixture)
			before := readAssignmentState(
				t,
				fixture.database.Admin,
				fixture.candidate.JobID,
				fixture.worker.ID,
			)
			candidate := fixture.candidate
			candidate.SchedulerClaim = &workercontrol.SchedulerClaim{
				IntentID:     fixture.intentID,
				WorkerPoolID: fixture.poolID,
			}
			_, err := fixture.service.Acquire(context.Background(), fixture.worker, 7, &candidate)
			var failure *workercontrol.Failure
			if !errors.As(err, &failure) || failure.Code != test.wantCode {
				t.Fatalf("Acquire after %s error = %v, want %s", test.name, err, test.wantCode)
			}
			after := readAssignmentState(
				t,
				fixture.database.Admin,
				fixture.candidate.JobID,
				fixture.worker.ID,
			)
			if after != before {
				t.Fatalf("rejected %s changed Assignment state: before=%+v after=%+v", test.name, before, after)
			}
		})
	}
}

func TestSchedulerChoosesBestWorkerAcrossEveryCertifiedProfile(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Scheduler cross-profile fixture")
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	jobID := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-cross-profile-worker-score",
	)

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	defaultProfileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	alternateProfileID := uuid.MustParse("00000000-0000-0000-0000-000000000416")
	if _, err := database.Admin.Exec(`
		INSERT INTO execution_profile_revisions (
			id, model_revision_id, worker_pool_id, stable_id, revision, state
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000010', $2,
			'h3-balanced-alternate', 1, 'ACTIVE'
		)
	`, alternateProfileID, poolID); err != nil {
		t.Fatalf("seed alternate Execution Profile: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO profile_certifications (
			id, model_revision_id, generation_preset_revision_id, output_spec_id,
			execution_profile_revision_id, state, evidence_digest, certified_at
		) VALUES (
			'00000000-0000-0000-0000-000000000417',
			'00000000-0000-0000-0000-000000000010',
			'00000000-0000-0000-0000-000000000011',
			'00000000-0000-0000-0000-000000000013',
			$1, 'ACTIVE', 'cross-profile-evidence', clock_timestamp()
		)
	`, alternateProfileID); err != nil {
		t.Fatalf("certify alternate Execution Profile: %v", err)
	}
	seedSchedulerCapacityShares(t, database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   1,
		ProjectWeight:  1,
	})

	highPenaltyWorkerID := uuid.MustParse("00000000-0000-0000-0000-000000000418")
	lowPenaltyWorkerID := uuid.MustParse("00000000-0000-0000-0000-000000000419")
	for _, worker := range []struct {
		id        uuid.UUID
		profileID uuid.UUID
		penalty   int
	}{
		{id: highPenaltyWorkerID, profileID: defaultProfileID, penalty: 90},
		{id: lowPenaltyWorkerID, profileID: alternateProfileID, penalty: 1},
	} {
		if _, err := database.Admin.Exec(`
			INSERT INTO workers (
				id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
				reachability_condition
			) VALUES (
				$1::uuid, $2, 'spiffe://vela.internal/worker/' || ($1::uuid)::text,
				7, 'READY', 'HEALTHY'
			)
		`, worker.id, poolID); err != nil {
			t.Fatalf("seed cross-profile Worker %s: %v", worker.id, err)
		}
		if _, err := database.Admin.Exec(`
			INSERT INTO worker_profile_readiness (
				worker_id, worker_epoch, execution_profile_revision_id, readiness,
				model_cold_start_penalty_seconds
			) VALUES ($1, 7, $2, 'WARM', $3)
		`, worker.id, worker.profileID, worker.penalty); err != nil {
			t.Fatalf("seed cross-profile Worker readiness %s: %v", worker.id, err)
		}
	}
	schedulerPool := newRolePool(
		t, database.DSN, "vela_scheduler_login", "vela-scheduler-password",
	)
	predictor, err := scheduler.NewCapacityPredictor(schedulerPool)
	if err != nil {
		t.Fatalf("create cross-profile Dynamic ETA predictor: %v", err)
	}
	if _, err := predictor.PredictJobDynamicETA(context.Background(), jobID); err != nil {
		t.Fatalf("predict cross-profile Dynamic ETA: %v", err)
	}
	var claimedJobID, claimedWorkerID, claimedProfileID uuid.UUID
	if err := schedulerPool.QueryRow(context.Background(), `
		SELECT job_id, worker_id, execution_profile_revision_id
		FROM vela_claim_scheduler_dispatch($1, 'scheduler-cross-profile', 30)
	`, poolID).Scan(&claimedJobID, &claimedWorkerID, &claimedProfileID); err != nil {
		t.Fatalf("claim cross-profile Job: %v", err)
	}
	if claimedJobID != jobID || claimedWorkerID != lowPenaltyWorkerID ||
		claimedProfileID != alternateProfileID {
		t.Fatalf(
			"cross-profile claim = Job %s Worker %s profile %s, want Job %s Worker %s profile %s",
			claimedJobID,
			claimedWorkerID,
			claimedProfileID,
			jobID,
			lowPenaltyWorkerID,
			alternateProfileID,
		)
	}
}

func TestSchedulerProjectionCountsMultiProfileWorkerOnce(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Scheduler multi-profile capacity fixture")
	seedAdmissionFixture(t, database.Admin)

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	defaultProfileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	alternateOutputSpecID := uuid.MustParse("00000000-0000-0000-0000-000000000420")
	alternateProfileID := uuid.MustParse("00000000-0000-0000-0000-000000000421")
	if _, err := database.Admin.Exec(`
		INSERT INTO output_specs (
			id, stable_id, revision, state, width, height, duration_milliseconds,
			frame_rate_milli, codec
		) VALUES ($1, 'video-720p-5s-24fps', 1, 'ACTIVE', 1280, 720, 5000, 24000, 'h264')
	`, alternateOutputSpecID); err != nil {
		t.Fatalf("seed alternate OutputSpec: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO execution_profile_revisions (
			id, model_revision_id, worker_pool_id, stable_id, revision, state
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000010', $2,
			'h3-balanced-720p', 1, 'ACTIVE'
		)
	`, alternateProfileID, poolID); err != nil {
		t.Fatalf("seed alternate capacity Execution Profile: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO profile_certifications (
			id, model_revision_id, generation_preset_revision_id, output_spec_id,
			execution_profile_revision_id, state, evidence_digest, certified_at
		) VALUES (
			'00000000-0000-0000-0000-000000000422',
			'00000000-0000-0000-0000-000000000010',
			'00000000-0000-0000-0000-000000000011', $1, $2,
			'ACTIVE', 'multi-profile-capacity-evidence', clock_timestamp()
		)
	`, alternateOutputSpecID, alternateProfileID); err != nil {
		t.Fatalf("certify alternate capacity Execution Profile: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO rate_card_lines (
			id, rate_card_revision_id, model_revision_id, generation_preset_revision_id,
			service_class_revision_id, output_spec_id, unit_amount_minor, currency
		) VALUES (
			'00000000-0000-0000-0000-000000000423',
			'00000000-0000-0000-0000-000000000016',
			'00000000-0000-0000-0000-000000000010',
			'00000000-0000-0000-0000-000000000011',
			'00000000-0000-0000-0000-000000000012', $1, 900, 'CNY'
		)
	`, alternateOutputSpecID); err != nil {
		t.Fatalf("price alternate OutputSpec: %v", err)
	}

	server := admissionServerForDatabase(t, database)
	firstJobID := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-multi-profile-capacity-first",
	)
	alternateBody := []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-720p-5s-24fps",
		"generation_count":1,
		"prompt":"exercise one physical Worker across profiles"
	}`)
	result, err := doSubmitJob(
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-multi-profile-capacity-second",
		alternateBody,
	)
	if err != nil || result.StatusCode != http.StatusAccepted {
		t.Fatalf("submit alternate-profile Job status=%d body=%s error=%v", result.StatusCode, result.Body, err)
	}
	var alternateJob jobResponse
	if err := json.Unmarshal(result.Body, &alternateJob); err != nil {
		t.Fatalf("decode alternate-profile Job: %v", err)
	}
	secondJobID := uuid.MustParse(alternateJob.JobID)

	seedSchedulerCapacityShares(t, database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   2,
		ProjectWeight:  1,
	})
	workerID := uuid.MustParse("00000000-0000-0000-0000-000000000424")
	seedSchedulerWorkers(t, database.Admin, poolID, defaultProfileID, schedulerWorker{
		ID:       workerID,
		SPIFFEID: "spiffe://vela.internal/worker/multi-profile-capacity",
		Epoch:    7,
	})
	seedSchedulerWorkerReadiness(t, database.Admin, workerID, 7, alternateProfileID)

	var firstFinish, secondFinish time.Time
	if err := database.Admin.QueryRow(`
		SELECT
			max(predicted_finish_at) FILTER (WHERE job_id = $1),
			max(predicted_finish_at) FILTER (WHERE job_id = $2)
		FROM vela_scheduler_queue_projection()
	`, firstJobID, secondJobID).Scan(&firstFinish, &secondFinish); err != nil {
		t.Fatalf("read multi-profile queue projection: %v", err)
	}
	if secondFinish.Sub(firstFinish) != 1200*time.Second {
		t.Fatalf(
			"multi-profile projected finishes = first %s second %s delta %s, want one 1200s runtime",
			firstFinish,
			secondFinish,
			secondFinish.Sub(firstFinish),
		)
	}
}

func TestSchedulerDynamicETAIsProjectScopedAndProjectionFunctionsAreDenied(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedOtherOrganization(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	primaryJobID := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-request-projection-primary",
	)
	otherJobID := submitSchedulerJob(
		t,
		server.URL,
		testOtherProjectID,
		bearerCredential(testOtherCredentialID, testOtherSecret),
		"scheduler-request-projection-other",
	)

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	seedSchedulerCapacityShares(t, database.Admin, poolID,
		schedulerCapacityShare{
			OrganizationID: testOrganizationID,
			ProjectID:      testProjectID,
			Weight:         1,
			RunningLimit:   2,
			ProjectWeight:  1,
		},
		schedulerCapacityShare{
			OrganizationID: testOtherOrganizationID,
			ProjectID:      testOtherProjectID,
			Weight:         1,
			RunningLimit:   2,
			ProjectWeight:  1,
		},
	)
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, schedulerWorker{
		ID:       uuid.MustParse("00000000-0000-0000-0000-000000000425"),
		SPIFFEID: "spiffe://vela.internal/worker/request-scoped-projection",
		Epoch:    7,
	})
	productionServer := schedulerAdmissionServerForDatabase(t, database)
	primaryView, _ := fetchJobView(t, productionServer.URL, primaryJobID)
	if primaryView.EstimatedFinishAt == nil {
		t.Fatal("authorized Project Dynamic ETA = nil")
	}
	request, err := http.NewRequest(
		http.MethodGet,
		productionServer.URL+"/v1/projects/"+testProjectID+"/jobs/"+otherJobID.String(),
		nil,
	)
	if err != nil {
		t.Fatalf("create cross-Project Dynamic ETA request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+testBearerCredential())
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("GET cross-Project Dynamic ETA: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("cross-Project Dynamic ETA status = %d, want 404", response.StatusCode)
	}

	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	projectionTx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin projection function denial transaction: %v", err)
	}
	var requestProjectionRows int
	err = projectionTx.QueryRow(context.Background(), `
		SELECT count(*)
		FROM vela_scheduler_queue_projection()
	`).Scan(&requestProjectionRows)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
		t.Fatalf("request projection function error = %v, want SQLSTATE 42501", err)
	}
	if err := projectionTx.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback denied request projection transaction: %v", err)
	}

	rawProjectionTx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin raw projection denial transaction: %v", err)
	}
	var rawProjectionRows int
	err = rawProjectionTx.QueryRow(context.Background(), `
		SELECT count(*)
		FROM vela_scheduler_queue_projection_for_pool($1, clock_timestamp())
	`, poolID).Scan(&rawProjectionRows)
	if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
		t.Fatalf("request raw per-pool projection error = %v, want SQLSTATE 42501", err)
	}
	if err := rawProjectionTx.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback denied raw projection transaction: %v", err)
	}
}

func TestSchedulerPredictsAdmissionWaitFromGlobalCapacity(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Scheduler Admission prediction fixture")
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-admission-prediction-existing",
	)

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	seedSchedulerCapacityShares(t, database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   2,
		ProjectWeight:  1,
	})
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, schedulerWorker{
		ID:       uuid.MustParse("00000000-0000-0000-0000-000000000425"),
		SPIFFEID: "spiffe://vela.internal/worker/admission-prediction",
		Epoch:    7,
	})

	schedulerPool := newRolePool(
		t, database.DSN, "vela_scheduler_login", "vela-scheduler-password",
	)
	predictor, err := scheduler.NewCapacityPredictor(schedulerPool)
	if err != nil {
		t.Fatalf("create Scheduler capacity predictor: %v", err)
	}
	prediction, err := predictor.PredictCapacity(context.Background(), admission.CapacityPredictionRequest{
		WorkerPoolID:               poolID,
		ModelRevisionID:            uuid.MustParse("00000000-0000-0000-0000-000000000010"),
		GenerationPresetRevisionID: uuid.MustParse("00000000-0000-0000-0000-000000000011"),
		ServiceClassRevisionID:     uuid.MustParse("00000000-0000-0000-0000-000000000012"),
		OutputSpecID:               uuid.MustParse("00000000-0000-0000-0000-000000000013"),
		GenerationCount:            1,
	})
	if err != nil {
		t.Fatalf("predict Scheduler Admission wait: %v", err)
	}
	if prediction.QueueWait < 1199*time.Second || prediction.QueueWait > 1201*time.Second {
		t.Fatalf(
			"Scheduler Admission queue wait = %s, want one 1200s queued runtime",
			prediction.QueueWait,
		)
	}
	if _, err := database.Admin.Exec(`
		UPDATE workers SET reachability_condition = 'SUSPECT' WHERE worker_pool_id = $1
	`, poolID); err != nil {
		t.Fatalf("remove healthy Admission capacity: %v", err)
	}
	if _, err := predictor.PredictCapacity(
		context.Background(),
		admission.CapacityPredictionRequest{
			WorkerPoolID:               poolID,
			ModelRevisionID:            uuid.MustParse("00000000-0000-0000-0000-000000000010"),
			GenerationPresetRevisionID: uuid.MustParse("00000000-0000-0000-0000-000000000011"),
			ServiceClassRevisionID:     uuid.MustParse("00000000-0000-0000-0000-000000000012"),
			OutputSpecID:               uuid.MustParse("00000000-0000-0000-0000-000000000013"),
			GenerationCount:            1,
		},
	); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("Scheduler Admission prediction without capacity error = %v, want no rows", err)
	}
}

func TestSchedulerProtocolGateSupportsAuditedOperationalRollback(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Scheduler protocol fixture")
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	jobID := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-protocol-gate",
	)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	workerID := uuid.MustParse("00000000-0000-0000-0000-000000000410")
	seedSchedulerCapacityShares(t, database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   1,
		ProjectWeight:  1,
	})
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, schedulerWorker{
		ID:       workerID,
		SPIFFEID: "spiffe://vela.internal/worker/scheduler-protocol",
		Epoch:    7,
	})
	_, err := database.Admin.Exec(`
		SELECT vela_transition_scheduler_dispatch_protocol(true, '   ')
	`)
	var receiptError *pgconn.PgError
	if !errors.As(err, &receiptError) || receiptError.Code != "22023" {
		t.Fatalf("blank Scheduler protocol receipt error = %v", err)
	}
	var (
		requiredBeforeReceipt bool
		versionBeforeReceipt  int64
	)
	if err := database.Admin.QueryRow(`
		SELECT require_dispatch_intent, protocol_version
		FROM scheduler_dispatch_protocol_state WHERE singleton
	`).Scan(&requiredBeforeReceipt, &versionBeforeReceipt); err != nil {
		t.Fatalf("read protocol state after blank receipt: %v", err)
	}
	if requiredBeforeReceipt || versionBeforeReceipt != 1 {
		t.Fatalf(
			"blank receipt changed protocol state = required %t version %d",
			requiredBeforeReceipt,
			versionBeforeReceipt,
		)
	}
	if _, err := database.Admin.Exec(`
		SELECT vela_transition_scheduler_dispatch_protocol(
			true,
			'N-1 drained; Scheduler replicas and rollback receipt verified'
		)
	`); err != nil {
		t.Fatalf("enable Scheduler dispatch protocol: %v", err)
	}
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create protocol coordinator: %v", err)
	}
	_, err = coordinator.Acquire(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: workerID},
		7,
		&workercontrol.AssignmentCandidate{
			JobID:                      jobID,
			ExpectedJobVersion:         1,
			ExecutionProfileRevisionID: profileID,
		},
	)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) ||
		postgresError.Code != "55000" ||
		postgresError.ConstraintName != "attempts_scheduler_dispatch_required" {
		t.Fatalf("direct Acquire after Scheduler protocol gate error = %v", err)
	}
	var (
		jobState    string
		workerState string
		attempts    int
	)
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT state::text FROM jobs WHERE id = $1),
			(SELECT lifecycle_state::text FROM workers WHERE id = $2),
			(SELECT count(*) FROM attempts WHERE job_id = $1)
	`, jobID, workerID).Scan(&jobState, &workerState, &attempts); err != nil {
		t.Fatalf("read rejected direct Acquire effects: %v", err)
	}
	if jobState != "QUEUED" || workerState != "READY" || attempts != 0 {
		t.Fatalf(
			"rejected direct Acquire state = Job %s Worker %s Attempts %d",
			jobState,
			workerState,
			attempts,
		)
	}
	schedulerPool := newRolePool(t, database.DSN, "vela_scheduler_login", "vela-scheduler-password")
	scheduling, err := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "scheduler-protocol-gate",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create protocol Scheduler: %v", err)
	}
	dispatch, ok, err := scheduling.RunOnce(context.Background(), poolID)
	if err != nil || !ok || dispatch.Assignment.JobID != jobID {
		t.Fatalf("scheduled Acquire after protocol gate = %#v ok=%t error=%v", dispatch, ok, err)
	}
	_, err = database.Admin.Exec(`
		SELECT vela_transition_scheduler_dispatch_protocol(false, NULL)
	`)
	if !errors.As(err, &receiptError) || receiptError.Code != "22023" {
		t.Fatalf("missing operational rollback receipt error = %v", err)
	}
	if _, err := database.Admin.Exec(`
		SELECT vela_transition_scheduler_dispatch_protocol(
			false,
			'operator drained Scheduler writers and verified N-1 binary rollback'
		)
	`); err != nil {
		t.Fatalf("disable Scheduler protocol after scheduled Assignment: %v", err)
	}
	var (
		requireDispatchIntent bool
		protocolVersion       int64
		transitionReceipt     string
		transitionRows        int
	)
	if err := database.Admin.QueryRow(`
		SELECT
			state.require_dispatch_intent,
			state.protocol_version,
			state.transition_receipt,
			(SELECT count(*) FROM scheduler_dispatch_protocol_transitions)
		FROM scheduler_dispatch_protocol_state AS state
		WHERE state.singleton
	`).Scan(
		&requireDispatchIntent,
		&protocolVersion,
		&transitionReceipt,
		&transitionRows,
	); err != nil {
		t.Fatalf("read rolled-back Scheduler protocol state: %v", err)
	}
	if requireDispatchIntent || protocolVersion != 3 ||
		transitionReceipt != "operator drained Scheduler writers and verified N-1 binary rollback" ||
		transitionRows != 2 {
		t.Fatalf(
			"rolled-back Scheduler protocol = required %t version %d receipt %q history %d",
			requireDispatchIntent,
			protocolVersion,
			transitionReceipt,
			transitionRows,
		)
	}

	rollbackJobID := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-protocol-rollback-direct",
	)
	rollbackWorkerID := uuid.MustParse("00000000-0000-0000-0000-000000000412")
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, schedulerWorker{
		ID:       rollbackWorkerID,
		SPIFFEID: "spiffe://vela.internal/worker/scheduler-protocol-rollback",
		Epoch:    7,
	})
	rollbackAssignment, err := coordinator.Acquire(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: rollbackWorkerID},
		7,
		&workercontrol.AssignmentCandidate{
			JobID:                      rollbackJobID,
			ExpectedJobVersion:         1,
			ExecutionProfileRevisionID: profileID,
		},
	)
	if err != nil || rollbackAssignment.JobID != rollbackJobID ||
		rollbackAssignment.SchedulerDispatchIntentID != uuid.Nil {
		t.Fatalf("N-1 direct Acquire after protocol rollback = %#v error=%v", rollbackAssignment, err)
	}
}

func TestSchedulerProtocolStateRejectsDirectUpdateAndRecordsFunctionTransition(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)

	_, err := database.Admin.Exec(`
		UPDATE scheduler_dispatch_protocol_state
		SET require_dispatch_intent = true,
			protocol_version = protocol_version + 1,
			transition_receipt = 'direct update without transition history',
			transitioned_at = clock_timestamp()
		WHERE singleton
	`)
	var directUpdateError *pgconn.PgError
	if !errors.As(err, &directUpdateError) ||
		directUpdateError.Code != "55000" ||
		directUpdateError.ConstraintName != "scheduler_dispatch_protocol_state_transition_required" {
		t.Fatalf("direct Scheduler protocol state update error = %v", err)
	}

	forgedTransition, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin forged Scheduler protocol transition: %v", err)
	}
	defer func() { _ = forgedTransition.Rollback() }()
	if _, err := forgedTransition.Exec(`
		SET LOCAL vela.scheduler_dispatch_protocol_state_transition_guard =
			'vela-transition-8f9099fe-52d4-46e6-865a-e79ccc405f94'
	`); err != nil {
		t.Fatalf("set forged Scheduler protocol transition guard: %v", err)
	}
	_, err = forgedTransition.Exec(`
		UPDATE scheduler_dispatch_protocol_state
		SET require_dispatch_intent = true,
			protocol_version = protocol_version + 1,
			transition_receipt = 'forged transition without immutable history',
			transitioned_at = clock_timestamp()
		WHERE singleton
	`)
	var forgedUpdateError *pgconn.PgError
	if !errors.As(err, &forgedUpdateError) ||
		forgedUpdateError.Code != "55000" ||
		forgedUpdateError.ConstraintName !=
			"scheduler_dispatch_protocol_state_transition_required" {
		t.Fatalf("forged Scheduler protocol state update error = %v", err)
	}
	if err := forgedTransition.Rollback(); err != nil {
		t.Fatalf("roll back forged Scheduler protocol transition: %v", err)
	}

	_, err = database.Admin.Exec(`
		INSERT INTO scheduler_dispatch_protocol_transitions (
			protocol_version,
			require_dispatch_intent,
			transition_receipt,
			transitioned_at
		) VALUES (
			3,
			true,
			'non-contiguous transition history',
			clock_timestamp()
		)
	`)
	var nonContiguousHistoryError *pgconn.PgError
	if !errors.As(err, &nonContiguousHistoryError) ||
		nonContiguousHistoryError.Code != "55000" ||
		nonContiguousHistoryError.ConstraintName !=
			"scheduler_dispatch_protocol_history_contiguous" {
		t.Fatalf("non-contiguous Scheduler protocol history error = %v", err)
	}

	const receipt = "Scheduler protocol transition records its immutable receipt"
	if _, err := database.Admin.Exec(`
		SELECT vela_transition_scheduler_dispatch_protocol(true, $1)
	`, receipt); err != nil {
		t.Fatalf("transition Scheduler dispatch protocol: %v", err)
	}

	var (
		requireDispatchIntent bool
		protocolVersion       int64
		transitionReceipt     string
		matchingHistoryRows   int
		totalHistoryRows      int
	)
	if err := database.Admin.QueryRow(`
		SELECT
			state.require_dispatch_intent,
			state.protocol_version,
			state.transition_receipt,
			(
				SELECT count(*)
				FROM scheduler_dispatch_protocol_transitions AS history
				WHERE history.protocol_version = state.protocol_version
				  AND history.require_dispatch_intent = state.require_dispatch_intent
				  AND history.transition_receipt = state.transition_receipt
				  AND history.transitioned_at = state.transitioned_at
			),
			(SELECT count(*) FROM scheduler_dispatch_protocol_transitions)
		FROM scheduler_dispatch_protocol_state AS state
		WHERE state.singleton
	`).Scan(
		&requireDispatchIntent,
		&protocolVersion,
		&transitionReceipt,
		&matchingHistoryRows,
		&totalHistoryRows,
	); err != nil {
		t.Fatalf("read Scheduler protocol state and history: %v", err)
	}
	if !requireDispatchIntent || protocolVersion != 2 || transitionReceipt != receipt ||
		matchingHistoryRows != 1 || totalHistoryRows != 1 {
		t.Fatalf(
			"Scheduler protocol transition = required %t version %d receipt %q matching history %d total history %d",
			requireDispatchIntent,
			protocolVersion,
			transitionReceipt,
			matchingHistoryRows,
			totalHistoryRows,
		)
	}
}

func TestSchedulerProtocolStateIsIndelibleAndMissingStateFailsClosed(t *testing.T) {
	fixture := newAssignmentFixture(t, "scheduler-protocol-missing-state", 7)
	_, err := fixture.database.Admin.Exec(`DELETE FROM scheduler_dispatch_protocol_state`)
	var deleteError *pgconn.PgError
	if !errors.As(err, &deleteError) ||
		deleteError.Code != "55000" ||
		deleteError.ConstraintName != "scheduler_dispatch_protocol_state_required" {
		t.Fatalf("delete Scheduler protocol state error = %v", err)
	}

	if _, err := fixture.database.Admin.Exec(`
		ALTER TABLE scheduler_dispatch_protocol_state
			DISABLE TRIGGER scheduler_dispatch_protocol_reject_delete;
		DELETE FROM scheduler_dispatch_protocol_state;
	`); err != nil {
		t.Fatalf("simulate missing Scheduler protocol state: %v", err)
	}
	before := readAssignmentState(
		t,
		fixture.database.Admin,
		fixture.candidate.JobID,
		fixture.worker.ID,
	)
	_, err = fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	var acquireError *pgconn.PgError
	if !errors.As(err, &acquireError) || acquireError.Code != "P0002" {
		t.Fatalf("direct Acquire with missing Scheduler protocol state error = %v", err)
	}
	after := readAssignmentState(
		t,
		fixture.database.Admin,
		fixture.candidate.JobID,
		fixture.worker.ID,
	)
	if after != before {
		t.Fatalf("missing protocol state changed Assignment state: before=%+v after=%+v", before, after)
	}
	_, err = fixture.database.Admin.Exec(`
		SELECT vela_transition_scheduler_dispatch_protocol(
			true,
			'missing-state transition must fail closed'
		)
	`)
	var transitionError *pgconn.PgError
	if !errors.As(err, &transitionError) ||
		transitionError.Code != "55000" ||
		transitionError.ConstraintName != "scheduler_dispatch_protocol_state_required" {
		t.Fatalf("transition with missing Scheduler protocol state error = %v", err)
	}
}

func TestSchedulerRejectsNMinusOneAssignmentThatWinsAfterClaim(t *testing.T) {
	fixture := newAssignmentFixture(t, "scheduler-n-minus-one-claim-race", 7)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := fixture.candidate.ExecutionProfileRevisionID
	seedSchedulerCapacityShares(t, fixture.database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   1,
		ProjectWeight:  1,
	})
	seedSchedulerWorkerReadiness(
		t, fixture.database.Admin, fixture.worker.ID, 7, profileID,
	)

	racingCoordinator := &nMinusOneRacingCoordinator{delegate: fixture.service}
	schedulerPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_scheduler_login",
		"vela-scheduler-password",
	)
	scheduling, err := scheduler.NewService(schedulerPool, racingCoordinator, scheduler.Config{
		SchedulerID:       "scheduler-n-minus-one-claim-race",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create N-1 race Scheduler: %v", err)
	}
	dispatch, ok, err := scheduling.RunOnce(context.Background(), poolID)
	if err != nil || ok {
		t.Fatalf("N-1 race Scheduler result = %#v ok=%t error=%v", dispatch, ok, err)
	}
	if racingCoordinator.directErr != nil {
		t.Fatalf("N-1 direct Acquire: %v", racingCoordinator.directErr)
	}

	var directAttempts, committedClaims, abandonedClaims int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM attempts
			 WHERE job_id = $1 AND scheduler_dispatch_intent_id IS NULL),
			(SELECT count(*) FROM scheduler_dispatch_intents
			 WHERE job_id = $1 AND state = 'COMMITTED'),
			(SELECT count(*) FROM scheduler_dispatch_intents
			 WHERE job_id = $1 AND state = 'ABANDONED')
	`, fixture.candidate.JobID).Scan(
		&directAttempts,
		&committedClaims,
		&abandonedClaims,
	); err != nil {
		t.Fatalf("read N-1 claim-race receipt: %v", err)
	}
	if directAttempts != 1 || committedClaims != 0 || abandonedClaims != 1 {
		t.Fatalf(
			"N-1 claim-race receipt = direct Attempts %d committed %d abandoned %d",
			directAttempts,
			committedClaims,
			abandonedClaims,
		)
	}
}

func TestSchedulerCapacityTimelineUsesReadyAndBusyWorkerEvidence(t *testing.T) {
	fixture := newAssignmentFixture(t, "scheduler-capacity-timeline-busy", 7)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := fixture.candidate.ExecutionProfileRevisionID
	seedSchedulerCapacityShares(t, fixture.database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   2,
		ProjectWeight:  1,
	})
	seedSchedulerWorkerReadiness(
		t, fixture.database.Admin, fixture.worker.ID, 7, profileID,
	)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create BUSY timeline Assignment: %v", err)
	}
	if started, startErr := fixture.service.Start(
		context.Background(), fixture.worker, leaseCredentials(assignment),
	); startErr != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start BUSY timeline Assignment = %#v error=%v", started, startErr)
	}
	remainingSeconds := int64(90)
	heartbeat := validHeartbeatObservation(1)
	heartbeat.EstimatedRemainingSeconds = &remainingSeconds
	if result, heartbeatErr := fixture.service.Heartbeat(
		context.Background(), fixture.worker, leaseCredentials(assignment), heartbeat,
	); heartbeatErr != nil || result.Decision != workercontrol.HeartbeatContinue {
		t.Fatalf("record BUSY timeline Heartbeat = %#v error=%v", result, heartbeatErr)
	}

	var busyAvailableAt, databaseNow time.Time
	if err := fixture.database.Admin.QueryRow(`
		SELECT timeline.available_at, clock_timestamp()
		FROM vela_scheduler_capacity_timeline($1, clock_timestamp()) AS timeline
		WHERE timeline.worker_id = $2
		  AND timeline.execution_profile_revision_id = $3
		  AND timeline.capacity_state = 'BUSY'
	`, poolID, fixture.worker.ID, profileID).Scan(&busyAvailableAt, &databaseNow); err != nil {
		t.Fatalf("read BUSY Worker capacity timeline: %v", err)
	}
	busyDelay := busyAvailableAt.Sub(databaseNow)
	if busyDelay < 80*time.Second || busyDelay > 90*time.Second {
		t.Fatalf("BUSY Worker availability delay = %s, want current heartbeat estimate", busyDelay)
	}

	readyWorkerID := uuid.MustParse("00000000-0000-0000-0000-000000000411")
	seedSchedulerWorkers(t, fixture.database.Admin, poolID, profileID, schedulerWorker{
		ID:       readyWorkerID,
		SPIFFEID: "spiffe://vela.internal/worker/scheduler-capacity-ready",
		Epoch:    7,
	})
	server := schedulerAdmissionServerForDatabase(t, fixture.database)
	jobID := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-capacity-timeline-ready",
	)
	schedulerPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_scheduler_login",
		"vela-scheduler-password",
	)
	var intentID, claimedJobID uuid.UUID
	var predictedStartAt, predictedFinishAt, claimedAt time.Time
	var predictedRuntime int64
	if err := schedulerPool.QueryRow(context.Background(), `
		SELECT
			intent_id,
			job_id,
			predicted_runtime_seconds,
			predicted_start_at,
			predicted_finish_at,
			clock_timestamp()
		FROM vela_claim_scheduler_dispatch($1, 'scheduler-capacity-timeline', 30)
	`, poolID).Scan(
		&intentID,
		&claimedJobID,
		&predictedRuntime,
		&predictedStartAt,
		&predictedFinishAt,
		&claimedAt,
	); err != nil {
		t.Fatalf("claim READY capacity timeline Job: %v", err)
	}
	if claimedJobID != jobID || predictedStartAt.After(claimedAt) ||
		claimedAt.Sub(predictedStartAt) > 2*time.Second ||
		predictedFinishAt.Sub(predictedStartAt) != time.Duration(predictedRuntime)*time.Second {
		t.Fatalf(
			"capacity claim = Job %s runtime %d start %s finish %s observed %s",
			claimedJobID,
			predictedRuntime,
			predictedStartAt,
			predictedFinishAt,
			claimedAt,
		)
	}
	var abandoned bool
	if err := schedulerPool.QueryRow(context.Background(), `
		SELECT vela_abandon_scheduler_dispatch(
			$1, 'scheduler-capacity-timeline', 'timeline_verified'
		)
	`, intentID).Scan(&abandoned); err != nil || !abandoned {
		t.Fatalf("abandon capacity timeline claim = %t error=%v", abandoned, err)
	}
}

func TestSchedulerBusyTimelineDrivesQueuedDynamicETAWithoutPreassignment(t *testing.T) {
	fixture := newAssignmentFixture(t, "scheduler-busy-queue-projection", 7)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := fixture.candidate.ExecutionProfileRevisionID
	seedSchedulerCapacityShares(t, fixture.database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   2,
		ProjectWeight:  1,
	})
	seedSchedulerWorkerReadiness(
		t, fixture.database.Admin, fixture.worker.ID, 7, profileID,
	)
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create BUSY projection Assignment: %v", err)
	}
	if started, startErr := fixture.service.Start(
		context.Background(), fixture.worker, leaseCredentials(assignment),
	); startErr != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start BUSY projection Assignment = %#v error=%v", started, startErr)
	}

	recordHeartbeat := func(sequence int64, remainingSeconds int64) workercontrol.HeartbeatResult {
		t.Helper()
		heartbeat := validHeartbeatObservation(sequence)
		heartbeat.EstimatedRemainingSeconds = &remainingSeconds
		result, heartbeatErr := fixture.service.Heartbeat(
			context.Background(), fixture.worker, leaseCredentials(assignment), heartbeat,
		)
		if heartbeatErr != nil || result.Decision != workercontrol.HeartbeatContinue {
			t.Fatalf("record BUSY projection Heartbeat = %#v error=%v", result, heartbeatErr)
		}
		return result
	}
	firstHeartbeat := recordHeartbeat(1, 90)

	server := schedulerAdmissionServerForDatabase(t, fixture.database)
	queuedJobID := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-busy-queue-projection-waiter",
	)
	queuedView, _ := fetchJobView(t, server.URL, queuedJobID)
	if queuedView.EstimatedFinishAt == nil {
		t.Fatalf("queued Job Dynamic ETA = nil, want BUSY capacity projection")
	}
	firstExpectedFinish := firstHeartbeat.ProgressUpdatedAt.Add((90 + 1200) * time.Second)
	if !queuedView.EstimatedFinishAt.Equal(firstExpectedFinish) {
		t.Fatalf(
			"queued Job Dynamic ETA = %s, want %s",
			queuedView.EstimatedFinishAt,
			firstExpectedFinish,
		)
	}

	schedulerPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_scheduler_login",
		"vela-scheduler-password",
	)
	var claimedJobID uuid.UUID
	if claimErr := schedulerPool.QueryRow(context.Background(), `
		SELECT job_id
		FROM vela_claim_scheduler_dispatch($1, 'scheduler-busy-projection-no-preassignment', 30)
	`, poolID).Scan(&claimedJobID); !errors.Is(claimErr, pgx.ErrNoRows) {
		t.Fatalf("BUSY-only Scheduler claim = Job %s error %v, want no rows", claimedJobID, claimErr)
	}

	secondHeartbeat := recordHeartbeat(2, 180)
	updatedView, _ := fetchJobView(t, server.URL, queuedJobID)
	if updatedView.EstimatedFinishAt == nil {
		t.Fatalf("updated queued Job Dynamic ETA = nil, want BUSY capacity projection")
	}
	secondExpectedFinish := secondHeartbeat.ProgressUpdatedAt.Add((180 + 1200) * time.Second)
	if !updatedView.EstimatedFinishAt.Equal(secondExpectedFinish) {
		t.Fatalf(
			"updated queued Job Dynamic ETA = %s, want %s",
			updatedView.EstimatedFinishAt,
			secondExpectedFinish,
		)
	}
	if !updatedView.EstimatedFinishAt.After(*queuedView.EstimatedFinishAt) {
		t.Fatalf(
			"updated queued Job Dynamic ETA = %s, want later than %s",
			updatedView.EstimatedFinishAt,
			queuedView.EstimatedFinishAt,
		)
	}
}

func TestSchedulerCapacityTimelineExcludesWorkerWithOlderEpochAuthority(t *testing.T) {
	const currentEpoch int64 = 7
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")

	assertNoCapacity := func(t *testing.T, database *sql.DB, workerID uuid.UUID) {
		t.Helper()
		var capacityRows int
		if err := database.QueryRow(`
			SELECT count(*)
			FROM vela_scheduler_capacity_timeline($1, clock_timestamp()) AS timeline
			WHERE timeline.worker_id = $2
		`, poolID, workerID).Scan(&capacityRows); err != nil {
			t.Fatalf("read Worker-wide Scheduler capacity timeline: %v", err)
		}
		if capacityRows != 0 {
			t.Fatalf("Worker with older-epoch active authority exposed %d capacity slots", capacityRows)
		}
	}
	advanceWorkerEpoch := func(
		t *testing.T,
		database *sql.DB,
		workerID uuid.UUID,
		profileID uuid.UUID,
	) {
		t.Helper()
		if _, err := database.Exec(`
			UPDATE workers
			SET epoch = $2, lifecycle_state = 'READY', updated_at = clock_timestamp()
			WHERE id = $1
		`, workerID, currentEpoch); err != nil {
			t.Fatalf("advance Worker epoch with older active authority: %v", err)
		}
		seedSchedulerWorkerReadiness(t, database, workerID, currentEpoch, profileID)
	}

	t.Run("active Attempt", func(t *testing.T) {
		fixture := newAssignmentFixture(t, "scheduler-capacity-old-attempt", 6)
		profileID := fixture.candidate.ExecutionProfileRevisionID
		if _, err := fixture.service.Acquire(
			context.Background(), fixture.worker, 6, &fixture.candidate,
		); err != nil {
			t.Fatalf("create older-epoch active Assignment: %v", err)
		}
		advanceWorkerEpoch(t, fixture.database.Admin, fixture.worker.ID, profileID)
		assertNoCapacity(t, fixture.database.Admin, fixture.worker.ID)
	})

	t.Run("live dispatch claim", func(t *testing.T) {
		fixture := newAssignmentFixture(t, "scheduler-capacity-old-claim", 6)
		profileID := fixture.candidate.ExecutionProfileRevisionID
		seedSchedulerCapacityShares(t, fixture.database.Admin, poolID, schedulerCapacityShare{
			OrganizationID: testOrganizationID,
			ProjectID:      testProjectID,
			Weight:         1,
			RunningLimit:   1,
			ProjectWeight:  1,
		})
		seedSchedulerWorkerReadiness(t, fixture.database.Admin, fixture.worker.ID, 6, profileID)
		schedulerPool := newRolePool(
			t,
			fixture.database.DSN,
			"vela_scheduler_login",
			"vela-scheduler-password",
		)
		var intentID uuid.UUID
		if err := schedulerPool.QueryRow(context.Background(), `
			SELECT intent_id
			FROM vela_claim_scheduler_dispatch($1, 'scheduler-capacity-old-claim', 30)
		`, poolID).Scan(&intentID); err != nil {
			t.Fatalf("create older-epoch live dispatch claim: %v", err)
		}
		if intentID == uuid.Nil {
			t.Fatal("older-epoch live dispatch claim has a nil intent id")
		}
		advanceWorkerEpoch(t, fixture.database.Admin, fixture.worker.ID, profileID)
		assertNoCapacity(t, fixture.database.Admin, fixture.worker.ID)
	})
}

func TestSchedulerProtocolTransitionSerializesWithInFlightDirectAcquire(t *testing.T) {
	fixture := newAssignmentFixture(t, "scheduler-protocol-transition-race", 7)
	const advisoryLockKey int64 = 580009
	if _, err := fixture.database.Admin.Exec(`
		CREATE FUNCTION vela_test_pause_after_scheduler_protocol() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock(580009);
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER attempts_pause_after_scheduler_protocol
		BEFORE INSERT ON attempts
		FOR EACH ROW EXECUTE FUNCTION vela_test_pause_after_scheduler_protocol();
	`); err != nil {
		t.Fatalf("install in-flight direct Acquire pause trigger: %v", err)
	}
	blocker, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin protocol-transition advisory-lock blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	if _, err := blocker.Exec("SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		t.Fatalf("acquire protocol-transition advisory-lock blocker: %v", err)
	}

	type acquireResult struct {
		assignment workercontrol.Assignment
		err        error
	}
	acquireResults := make(chan acquireResult, 1)
	go func() {
		assignment, acquireErr := fixture.service.Acquire(
			context.Background(),
			fixture.worker,
			7,
			&fixture.candidate,
		)
		acquireResults <- acquireResult{assignment: assignment, err: acquireErr}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_internal_login")

	transitionResults := make(chan error, 1)
	go func() {
		_, transitionErr := fixture.database.Admin.Exec(`
			SELECT vela_transition_scheduler_dispatch_protocol(
				true,
				'in-flight N-1 writer serialized before Scheduler protocol switch'
			)
		`)
		transitionResults <- transitionErr
	}()
	transitionBlocked := false
	transitionCompletedEarly := false
	var transitionErr error
	deadline := time.Now().Add(6 * time.Second)
	for !transitionBlocked && !transitionCompletedEarly && time.Now().Before(deadline) {
		select {
		case transitionErr = <-transitionResults:
			transitionCompletedEarly = true
		default:
			if err := fixture.database.Admin.QueryRow(`
				SELECT EXISTS (
					SELECT 1
					FROM pg_stat_activity
					WHERE usename = 'postgres'
					  AND wait_event_type = 'Lock'
					  AND query LIKE '%vela_transition_scheduler_dispatch_protocol%'
				)
			`).Scan(&transitionBlocked); err != nil {
				t.Fatalf("inspect Scheduler protocol-transition lock wait: %v", err)
			}
			if !transitionBlocked {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
	var unlocked bool
	if err := blocker.QueryRow("SELECT pg_advisory_unlock($1)", advisoryLockKey).Scan(&unlocked); err != nil {
		t.Fatalf("release protocol-transition advisory-lock blocker: %v", err)
	}
	if !unlocked {
		t.Fatal("protocol-transition advisory-lock blocker was not held")
	}
	if err := blocker.Commit(); err != nil {
		t.Fatalf("commit protocol-transition advisory-lock blocker: %v", err)
	}

	select {
	case acquired := <-acquireResults:
		if acquired.err != nil || acquired.assignment.JobID != fixture.candidate.JobID {
			t.Fatalf("in-flight direct Acquire = %#v error=%v", acquired.assignment, acquired.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight direct Acquire did not finish")
	}
	if !transitionCompletedEarly {
		select {
		case transitionErr = <-transitionResults:
		case <-time.After(10 * time.Second):
			t.Fatal("Scheduler protocol transition did not finish")
		}
	}
	if transitionErr != nil {
		t.Fatalf("Scheduler protocol transition error = %v", transitionErr)
	}
	if transitionCompletedEarly || !transitionBlocked {
		t.Fatal("Scheduler protocol transition did not serialize with an in-flight direct Acquire")
	}

	var (
		requireDispatchIntent bool
		protocolVersion       int64
		receipt               string
		directAttempts        int
		transitionedAfter     bool
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			protocol.require_dispatch_intent,
			protocol.protocol_version,
			protocol.transition_receipt,
			(SELECT count(*) FROM attempts
			 WHERE job_id = $1 AND scheduler_dispatch_intent_id IS NULL),
			protocol.transitioned_at > (
				SELECT assigned_at FROM attempts WHERE job_id = $1
			)
		FROM scheduler_dispatch_protocol_state AS protocol
		WHERE protocol.singleton
	`, fixture.candidate.JobID).Scan(
		&requireDispatchIntent,
		&protocolVersion,
		&receipt,
		&directAttempts,
		&transitionedAfter,
	); err != nil {
		t.Fatalf("read Scheduler protocol-transition receipt: %v", err)
	}
	if !requireDispatchIntent || protocolVersion != 2 ||
		receipt != "in-flight N-1 writer serialized before Scheduler protocol switch" ||
		directAttempts != 1 || !transitionedAfter {
		t.Fatalf(
			"protocol-transition receipt = required %t version %d receipt %q direct Attempts %d ordered %t",
			requireDispatchIntent,
			protocolVersion,
			receipt,
			directAttempts,
			transitionedAfter,
		)
	}
}

type scheduledAcquireFixture struct {
	assignmentFixture
	poolID   uuid.UUID
	intentID uuid.UUID
}

type nMinusOneRacingCoordinator struct {
	delegate  *workercontrol.Service
	once      sync.Once
	directErr error
}

func (coordinator *nMinusOneRacingCoordinator) Acquire(
	ctx context.Context,
	worker workercontrol.AuthenticatedWorker,
	workerEpoch int64,
	candidate *workercontrol.AssignmentCandidate,
) (workercontrol.Assignment, error) {
	coordinator.once.Do(func() {
		directCandidate := *candidate
		directCandidate.SchedulerClaim = nil
		_, coordinator.directErr = coordinator.delegate.Acquire(
			ctx,
			worker,
			workerEpoch,
			&directCandidate,
		)
	})
	if coordinator.directErr != nil {
		return workercontrol.Assignment{}, coordinator.directErr
	}
	return coordinator.delegate.Acquire(ctx, worker, workerEpoch, candidate)
}

type schedulerCapacityShare struct {
	OrganizationID string
	ProjectID      string
	Weight         int
	RunningLimit   int
	ProjectWeight  int
}

func seedSchedulerCapacityShares(
	t *testing.T,
	database *sql.DB,
	workerPoolID uuid.UUID,
	shares ...schedulerCapacityShare,
) {
	t.Helper()
	for _, share := range shares {
		if _, err := database.Exec(`
			INSERT INTO organization_capacity_shares (
				worker_pool_id, organization_id, weight, running_limit
			) VALUES ($1, $2, $3, $4)
			ON CONFLICT (worker_pool_id, organization_id) DO NOTHING
		`,
			workerPoolID,
			share.OrganizationID,
			share.Weight,
			share.RunningLimit,
		); err != nil {
			t.Fatalf(
				"seed Scheduler Organization Capacity Share %s: %v",
				share.OrganizationID,
				err,
			)
		}
		if _, err := database.Exec(`
			INSERT INTO project_capacity_shares (
				worker_pool_id, organization_id, project_id, weight
			) VALUES ($1, $2, $3, $4)
		`,
			workerPoolID,
			share.OrganizationID,
			share.ProjectID,
			share.ProjectWeight,
		); err != nil {
			t.Fatalf(
				"seed Scheduler Capacity Share for Organization %s Project %s: %v",
				share.OrganizationID,
				share.ProjectID,
				err,
			)
		}
	}
}

type schedulerWorker struct {
	ID       uuid.UUID
	SPIFFEID string
	Epoch    int64
	State    string
}

func seedSchedulerWorkers(
	t *testing.T,
	database *sql.DB,
	workerPoolID uuid.UUID,
	executionProfileRevisionID uuid.UUID,
	workers ...schedulerWorker,
) {
	t.Helper()
	for _, worker := range workers {
		state := worker.State
		if state == "" {
			state = "READY"
		}
		if _, err := database.Exec(`
			INSERT INTO workers (
				id, worker_pool_id, spiffe_id, epoch, lifecycle_state,
				reachability_condition
			) VALUES ($1, $2, $3, $4, $5, 'HEALTHY')
		`, worker.ID, workerPoolID, worker.SPIFFEID, worker.Epoch, state); err != nil {
			t.Fatalf("seed Scheduler Worker %s: %v", worker.ID, err)
		}
		seedSchedulerWorkerReadiness(
			t,
			database,
			worker.ID,
			worker.Epoch,
			executionProfileRevisionID,
		)
	}
}

func seedSchedulerWorkerReadiness(
	t *testing.T,
	database *sql.DB,
	workerID uuid.UUID,
	workerEpoch int64,
	executionProfileRevisionID uuid.UUID,
) {
	t.Helper()
	if _, err := database.Exec(`
		INSERT INTO worker_profile_readiness (
			worker_id, worker_epoch, execution_profile_revision_id, readiness,
			model_cold_start_penalty_seconds, locality_penalty_seconds,
			health_risk_penalty_seconds
		) VALUES ($1, $2, $3, 'WARM', 0, 0, 0)
	`, workerID, workerEpoch, executionProfileRevisionID); err != nil {
		t.Fatalf("seed Scheduler Worker profile readiness for %s: %v", workerID, err)
	}
}

func newScheduledAcquireFixture(t *testing.T, key string) scheduledAcquireFixture {
	t.Helper()
	fixture := newAssignmentFixture(t, key, 7)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	seedSchedulerCapacityShares(t, fixture.database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   1,
		ProjectWeight:  1,
	})
	seedSchedulerWorkerReadiness(
		t,
		fixture.database.Admin,
		fixture.worker.ID,
		7,
		fixture.candidate.ExecutionProfileRevisionID,
	)
	schedulerPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_scheduler_login",
		"vela-scheduler-password",
	)
	var intentID uuid.UUID
	if err := schedulerPool.QueryRow(context.Background(), `
		SELECT intent_id FROM vela_claim_scheduler_dispatch($1, $2, 30)
	`, poolID, key).Scan(&intentID); err != nil {
		t.Fatalf("claim scheduled Assignment: %v", err)
	}
	return scheduledAcquireFixture{
		assignmentFixture: fixture,
		poolID:            poolID,
		intentID:          intentID,
	}
}

func submitSchedulerJob(
	t *testing.T,
	serverURL string,
	projectID string,
	credential string,
	idempotencyKey string,
) uuid.UUID {
	t.Helper()
	body := []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"exercise hierarchical scheduling"
	}`)
	result, err := doSubmitJob(serverURL, projectID, credential, idempotencyKey, body)
	if err != nil {
		t.Fatalf("submit Scheduler Job: %v", err)
	}
	if result.StatusCode != 202 {
		t.Fatalf("submit Scheduler Job status = %d body=%s", result.StatusCode, result.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(result.Body, &job); err != nil {
		t.Fatalf("decode Scheduler Job: %v", err)
	}
	return uuid.MustParse(job.JobID)
}

func seedSchedulerJobForServiceClass(
	t *testing.T,
	database *sql.DB,
	templateJobID uuid.UUID,
	serviceClassRevisionID uuid.UUID,
	rateLineID uuid.UUID,
	serviceClass string,
	createdAt time.Time,
	jobExpiresAt *time.Time,
) uuid.UUID {
	t.Helper()
	return seedSchedulerJobForWorkerPool(
		t,
		database,
		templateJobID,
		uuid.MustParse("00000000-0000-0000-0000-000000000005"),
		serviceClassRevisionID,
		rateLineID,
		serviceClass,
		createdAt,
		jobExpiresAt,
	)
}

func seedSchedulerJobForWorkerPool(
	t *testing.T,
	database *sql.DB,
	templateJobID uuid.UUID,
	workerPoolID uuid.UUID,
	serviceClassRevisionID uuid.UUID,
	rateLineID uuid.UUID,
	serviceClass string,
	createdAt time.Time,
	jobExpiresAt *time.Time,
) uuid.UUID {
	t.Helper()
	jobID := uuid.New()
	reservationID := uuid.New()
	tx, err := database.Begin()
	if err != nil {
		t.Fatalf("begin Service Class fixture transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`
		INSERT INTO jobs (
			id, organization_id, project_id, created_by_principal_id, state, version,
			model_revision_id, generation_preset_revision_id, service_class_revision_id,
			output_spec_id, worker_pool_id, request_hash, request_content,
			request_content_expires_at, pricing_rate_card_revision_id,
			pricing_rate_line_id, pricing_unit_amount_minor, pricing_quantity,
			pricing_quoted_amount_minor, pricing_currency, execution_max_attempts,
			execution_max_total_compute_seconds,
			execution_max_finalization_seconds_per_attempt,
			execution_retry_backoff_policy, execution_retryable_failure_classes,
			execution_circuit_breaker_policy, job_expires_at, created_at, updated_at
		)
		SELECT
			$1::uuid, organization_id, project_id, created_by_principal_id, 'QUEUED', 1,
			model_revision_id, generation_preset_revision_id, $2,
			output_spec_id, $8, sha256(convert_to(($1::uuid)::text, 'UTF8')),
			jsonb_set(request_content, '{service_class}', to_jsonb($4::text)),
			request_content_expires_at, pricing_rate_card_revision_id,
			$3, pricing_unit_amount_minor, pricing_quantity,
			pricing_quoted_amount_minor, pricing_currency, execution_max_attempts,
			execution_max_total_compute_seconds,
			execution_max_finalization_seconds_per_attempt,
			execution_retry_backoff_policy, execution_retryable_failure_classes,
			execution_circuit_breaker_policy, COALESCE($7::timestamptz, job_expires_at),
			$6, $6
		FROM jobs
		WHERE id = $5
		`,
		jobID,
		serviceClassRevisionID,
		rateLineID,
		serviceClass,
		templateJobID,
		createdAt,
		jobExpiresAt,
		workerPoolID,
	); err != nil {
		t.Fatalf("insert Service Class fixture Job: %v", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO credit_reservations (
			id, organization_id, project_id, job_id, amount_minor, currency
		)
		SELECT $1, organization_id, project_id, id, pricing_quoted_amount_minor,
			pricing_currency
		FROM jobs WHERE id = $2
	`, reservationID, jobID); err != nil {
		t.Fatalf("insert Service Class fixture CreditReservation: %v", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO retry_runtime_states (job_id, organization_id, project_id)
		SELECT id, organization_id, project_id FROM jobs WHERE id = $1
	`, jobID); err != nil {
		t.Fatalf("insert Service Class fixture RetryRuntimeState: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE projects SET queued_count = queued_count + 1 WHERE id = $1
	`, testProjectID); err != nil {
		t.Fatalf("update Service Class fixture Project counter: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE worker_pools SET queued_count = queued_count + 1
		WHERE id = $1
	`, workerPoolID); err != nil {
		t.Fatalf("update Service Class fixture pool counter: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE organization_credit_accounts
		SET reserved_minor = reserved_minor + 1250,
			version = version + 1,
			updated_at = clock_timestamp()
		WHERE organization_id = $1
	`, testOrganizationID); err != nil {
		t.Fatalf("update Service Class fixture credit counter: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit Service Class fixture Job: %v", err)
	}
	return jobID
}
