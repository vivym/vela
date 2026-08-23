//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/scheduler"
	"github.com/vivym/vela/internal/workercontrol"
)

func TestCrossJobFailureCircuitInvalidatesCertificationAndUsesAlternateProfile(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "profile circuit integration fixture")
	setProfileCircuitProtocolGate(t, database.Admin, true, "N-1 failure writers drained for circuit test")
	seedAdmissionFixture(t, database.Admin)

	const (
		primaryProfileID     = "00000000-0000-0000-0000-000000000014"
		primaryCertification = "00000000-0000-0000-0000-000000000015"
		alternateProfileID   = "00000000-0000-0000-0000-000000000510"
		workerOneID          = "00000000-0000-0000-0000-000000000511"
		workerTwoID          = "00000000-0000-0000-0000-000000000512"
		workerThreeID        = "00000000-0000-0000-0000-000000000513"
	)
	if _, err := database.Admin.Exec(`
		INSERT INTO execution_profile_revisions (
			id, model_revision_id, worker_pool_id, stable_id, revision, state
		) VALUES (
			$1,
			'00000000-0000-0000-0000-000000000010',
			'00000000-0000-0000-0000-000000000005',
			'h3-balanced-circuit-alternate', 1, 'ACTIVE'
		)
	`, alternateProfileID); err != nil {
		t.Fatalf("seed alternate ExecutionProfileRevision: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO profile_certifications (
			id, model_revision_id, generation_preset_revision_id, output_spec_id,
			execution_profile_revision_id, state, evidence_digest, certified_at
		) VALUES (
			'00000000-0000-0000-0000-000000000514',
			'00000000-0000-0000-0000-000000000010',
			'00000000-0000-0000-0000-000000000011',
			'00000000-0000-0000-0000-000000000013',
			$1, 'ACTIVE', 'circuit-alternate-evidence', clock_timestamp()
		)
	`, alternateProfileID); err != nil {
		t.Fatalf("seed alternate ProfileCertification: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES
			($1, '00000000-0000-0000-0000-000000000005',
			 'spiffe://vela.internal/worker/profile-circuit-one', 7, 'READY', 'HEALTHY'),
			($2, '00000000-0000-0000-0000-000000000005',
			 'spiffe://vela.internal/worker/profile-circuit-two', 7, 'READY', 'HEALTHY'),
			($3, '00000000-0000-0000-0000-000000000005',
			 'spiffe://vela.internal/worker/profile-circuit-three', 7, 'READY', 'HEALTHY')
	`, workerOneID, workerTwoID, workerThreeID); err != nil {
		t.Fatalf("seed profile circuit Workers: %v", err)
	}

	server := admissionServerForDatabase(t, database)
	jobIDs := make([]uuid.UUID, 0, 2)
	for index, key := range []string{"profile-circuit-first", "profile-circuit-threshold"} {
		accepted := submitJob(t, server.URL, key, []byte(`{
			"model":"minimax-h3",
			"generation_preset":"balanced",
			"service_class":"standard",
			"output_spec":"video-1080p-5s-24fps",
			"generation_count":1,
			"prompt":"exercise cross-Job ProfileCertification circuit"
		}`))
		if accepted.StatusCode != 202 {
			t.Fatalf("submit circuit Job %d status = %d; body=%s", index, accepted.StatusCode, accepted.Body)
		}
		var response jobResponse
		if err := json.Unmarshal(accepted.Body, &response); err != nil {
			t.Fatalf("decode circuit Job %d: %v", index, err)
		}
		jobIDs = append(jobIDs, uuid.MustParse(response.JobID))
		forceJobPolicySnapshot(
			t,
			database.Admin,
			jobIDs[index],
			`execution_retry_backoff_policy = '{"kind":"exponential","initial_seconds":1,"max_seconds":1}'::jsonb`,
		)
	}

	var windowSeconds, threshold int
	if err := database.Admin.QueryRow(`
		SELECT
			execution_circuit_fingerprint_window_seconds,
			execution_circuit_min_distinct_healthy_workers
		FROM jobs
		WHERE id = $1
	`, jobIDs[0]).Scan(&windowSeconds, &threshold); err != nil {
		t.Fatalf("read circuit policy snapshot: %v", err)
	}
	if windowSeconds != 3600 || threshold != 2 {
		t.Fatalf("circuit policy snapshot = window %d threshold %d", windowSeconds, threshold)
	}

	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	service, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create profile circuit Worker coordinator: %v", err)
	}
	workers := []workercontrol.AuthenticatedWorker{
		{ID: uuid.MustParse(workerOneID)},
		{ID: uuid.MustParse(workerTwoID)},
	}
	assignments := make([]workercontrol.Assignment, 0, 2)
	decisions := make([]workercontrol.RetryDecision, 0, 2)
	observation := validFailureObservation()
	for index, worker := range workers {
		assignment, acquireErr := service.Acquire(
			context.Background(),
			worker,
			7,
			&workercontrol.AssignmentCandidate{
				JobID:                      jobIDs[index],
				ExpectedJobVersion:         1,
				ExecutionProfileRevisionID: uuid.MustParse(primaryProfileID),
			},
		)
		if acquireErr != nil {
			t.Fatalf("Acquire circuit Attempt %d: %v", index, acquireErr)
		}
		if started, startErr := service.Start(
			context.Background(), worker, leaseCredentials(assignment),
		); startErr != nil || started.Decision != workercontrol.StartGranted {
			t.Fatalf("Start circuit Attempt %d = %#v error=%v", index, started, startErr)
		}
		decision, failErr := service.Fail(
			context.Background(), worker, leaseCredentials(assignment), observation,
		)
		if failErr != nil {
			t.Fatalf("Fail circuit Attempt %d: %v", index, failErr)
		}
		if decision.Disposition != workercontrol.RetryDispositionRetryWait {
			t.Fatalf("circuit RetryDecision %d = %#v", index, decision)
		}
		assignments = append(assignments, assignment)
		decisions = append(decisions, decision)

		var certificationState string
		var openingCount int
		if err := database.Admin.QueryRow(`
			SELECT
				state::text,
				(SELECT count(*) FROM profile_certification_circuit_openings
				 WHERE profile_certification_id = $1)
			FROM profile_certifications
			WHERE id = $1
		`, primaryCertification).Scan(&certificationState, &openingCount); err != nil {
			t.Fatalf("read circuit state after failure %d: %v", index, err)
		}
		if index == 0 && (certificationState != "ACTIVE" || openingCount != 0) {
			t.Fatalf("first failure opened circuit: state=%s openings=%d", certificationState, openingCount)
		}
		if index == 1 && (certificationState != "INVALID" || openingCount != 1) {
			t.Fatalf("threshold failure circuit = state %s openings %d", certificationState, openingCount)
		}
	}

	replayed, err := service.Fail(
		context.Background(), workers[1], leaseCredentials(assignments[1]), observation,
	)
	if err != nil {
		t.Fatalf("replay threshold Fail: %v", err)
	}
	if !reflect.DeepEqual(replayed, decisions[1]) {
		t.Fatalf("replayed threshold decision = %#v, want %#v", replayed, decisions[1])
	}

	var openingCount, observedWorkers, receiptThreshold, receiptWindow, protocolVersion int
	var retryCircuitState string
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM profile_certification_circuit_openings
			 WHERE profile_certification_id = $1),
			opening.observed_distinct_healthy_workers,
			opening.policy_min_distinct_healthy_workers,
			opening.policy_fingerprint_window_seconds,
			decision.circuit_protocol_version,
			runtime.circuit_breaker_state ->> 'state'
		FROM profile_certification_circuit_openings AS opening
		JOIN execution_failure_decisions AS decision
		  ON decision.id = opening.triggering_execution_failure_decision_id
		JOIN retry_runtime_states AS runtime ON runtime.job_id = opening.triggering_job_id
		WHERE opening.profile_certification_id = $1
	`, primaryCertification).Scan(
		&openingCount,
		&observedWorkers,
		&receiptThreshold,
		&receiptWindow,
		&protocolVersion,
		&retryCircuitState,
	); err != nil {
		t.Fatalf("read ProfileCertification circuit receipt: %v", err)
	}
	if openingCount != 1 || observedWorkers != 2 || receiptThreshold != 2 ||
		receiptWindow != 3600 || protocolVersion != 2 || retryCircuitState != "OPEN" {
		t.Fatalf(
			"circuit receipt = count %d workers %d threshold %d window %d protocol %d state %q",
			openingCount,
			observedWorkers,
			receiptThreshold,
			receiptWindow,
			protocolVersion,
			retryCircuitState,
		)
	}

	waitForDatabaseTimeAfter(t, database.Admin, *decisions[1].NextRetryAt)
	_, err = service.Acquire(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: uuid.MustParse(workerThreeID)},
		7,
		&workercontrol.AssignmentCandidate{
			JobID:                      jobIDs[1],
			ExpectedJobVersion:         decisions[1].JobVersion,
			ExecutionProfileRevisionID: uuid.MustParse(primaryProfileID),
		},
	)
	var unavailable *workercontrol.Failure
	if !errors.As(err, &unavailable) || unavailable.Code != workercontrol.FailureCandidateUnavailable {
		t.Fatalf("Acquire invalidated circuit profile error = %v", err)
	}

	replacement, err := service.Acquire(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: uuid.MustParse(workerThreeID)},
		7,
		&workercontrol.AssignmentCandidate{
			JobID:                      jobIDs[1],
			ExpectedJobVersion:         decisions[1].JobVersion,
			ExecutionProfileRevisionID: uuid.MustParse(alternateProfileID),
		},
	)
	if err != nil {
		t.Fatalf("Acquire alternate certified profile: %v", err)
	}
	if replacement.ExecutionProfileRevisionID != uuid.MustParse(alternateProfileID) ||
		replacement.LeaseFence <= decisions[1].JobFence {
		t.Fatalf("alternate replacement Assignment = %#v", replacement)
	}
}

func TestProfileCircuitWithoutAlternateFailsTriggeringJobAndReleasesCredit(t *testing.T) {
	fixture := newProfileCircuitFixture(t, 2, false)
	observation := validFailureObservation()

	firstJob, firstAssignment := fixture.assignAndStart(t, 0, "profile-circuit-no-alternate-first")
	first, err := fixture.service.Fail(
		context.Background(),
		fixture.workers[0],
		leaseCredentials(firstAssignment),
		observation,
	)
	if err != nil || first.Disposition != workercontrol.RetryDispositionRetryWait {
		t.Fatalf("first circuit Fail = %#v error=%v", first, err)
	}

	secondJob, secondAssignment := fixture.assignAndStart(t, 1, "profile-circuit-no-alternate-threshold")
	threshold, err := fixture.service.Fail(
		context.Background(),
		fixture.workers[1],
		leaseCredentials(secondAssignment),
		observation,
	)
	if err != nil {
		t.Fatalf("threshold circuit Fail without alternate: %v", err)
	}
	if threshold.Disposition != workercontrol.RetryDispositionFailed || threshold.NextRetryAt != nil {
		t.Fatalf("threshold decision without alternate = %#v", threshold)
	}

	var (
		certificationState string
		openingCount       int
		jobState           string
		circuitState       string
		reservationState   string
		chargeCount        int
		reservedMinor      int64
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT state::text FROM profile_certifications WHERE id = $1),
			(SELECT count(*) FROM profile_certification_circuit_openings
			 WHERE profile_certification_id = $1),
			job.state::text,
			runtime.circuit_breaker_state ->> 'state',
			reservation.state::text,
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT reserved_minor FROM organization_credit_accounts
			 WHERE organization_id = job.organization_id)
		FROM jobs AS job
		JOIN retry_runtime_states AS runtime ON runtime.job_id = job.id
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		WHERE job.id = $2
	`, fixture.primaryCertificationID, secondJob).Scan(
		&certificationState,
		&openingCount,
		&jobState,
		&circuitState,
		&reservationState,
		&chargeCount,
		&reservedMinor,
	); err != nil {
		t.Fatalf("read terminal circuit state: %v", err)
	}
	if certificationState != "INVALID" || openingCount != 1 || jobState != "FAILED" ||
		circuitState != "OPEN" || reservationState != "RELEASED" || chargeCount != 0 ||
		reservedMinor != 1250 {
		t.Fatalf(
			"terminal circuit state = certification %s openings %d job %s circuit %s reservation %s charges %d reserved %d",
			certificationState,
			openingCount,
			jobState,
			circuitState,
			reservationState,
			chargeCount,
			reservedMinor,
		)
	}

	var firstState, firstReservation string
	if err := fixture.database.Admin.QueryRow(`
		SELECT job.state::text, reservation.state::text
		FROM jobs AS job
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		WHERE job.id = $1
	`, firstJob).Scan(&firstState, &firstReservation); err != nil {
		t.Fatalf("read below-threshold Job state: %v", err)
	}
	if firstState != "RETRY_WAIT" || firstReservation != "RESERVED" {
		t.Fatalf("below-threshold Job = state %s reservation %s", firstState, firstReservation)
	}
}

func TestProfileCircuitAlreadyRunningAttemptUsesAlternateOrTerminates(t *testing.T) {
	tests := []struct {
		name            string
		keySuffix       string
		addAlternate    bool
		wantDisposition workercontrol.RetryDisposition
		wantJobState    string
		wantReservation string
	}{
		{
			name:            "alternate remains certified",
			keySuffix:       "alternate",
			addAlternate:    true,
			wantDisposition: workercontrol.RetryDispositionRetryWait,
			wantJobState:    "RETRY_WAIT",
			wantReservation: "RESERVED",
		},
		{
			name:            "no alternate remains certified",
			keySuffix:       "terminal",
			addAlternate:    false,
			wantDisposition: workercontrol.RetryDispositionFailed,
			wantJobState:    "FAILED",
			wantReservation: "RELEASED",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newProfileCircuitFixture(t, 3, test.addAlternate)
			observation := validFailureObservation()

			_, firstAssignment := fixture.assignAndStart(
				t, 0, "profile-circuit-running-first-"+test.keySuffix,
			)
			first, err := fixture.service.Fail(
				context.Background(),
				fixture.workers[0],
				leaseCredentials(firstAssignment),
				observation,
			)
			if err != nil || first.Disposition != workercontrol.RetryDispositionRetryWait {
				t.Fatalf("first circuit evidence = %#v error=%v", first, err)
			}

			_, thresholdAssignment := fixture.assignAndStart(
				t, 1, "profile-circuit-running-threshold-"+test.keySuffix,
			)
			alreadyRunningJob, alreadyRunningAssignment := fixture.assignAndStart(
				t, 2, "profile-circuit-already-running-"+test.keySuffix,
			)
			threshold, err := fixture.service.Fail(
				context.Background(),
				fixture.workers[1],
				leaseCredentials(thresholdAssignment),
				observation,
			)
			if err != nil {
				t.Fatalf("open circuit while another Attempt is RUNNING: %v", err)
			}
			if threshold.Disposition != test.wantDisposition {
				t.Fatalf("threshold decision = %#v, want %s", threshold, test.wantDisposition)
			}

			later, err := fixture.service.Fail(
				context.Background(),
				fixture.workers[2],
				leaseCredentials(alreadyRunningAssignment),
				observation,
			)
			if err != nil {
				t.Fatalf("Fail Attempt that was RUNNING when circuit opened: %v", err)
			}
			if later.Disposition != test.wantDisposition {
				t.Fatalf("already-running decision = %#v, want %s", later, test.wantDisposition)
			}

			var (
				certificationState  string
				openingCount        int
				attemptPredatesOpen bool
				protocolVersion     int
				workerWasHealthy    bool
				jobState            string
				circuitState        string
				reservationState    string
			)
			if err := fixture.database.Admin.QueryRow(`
				SELECT
					certification.state::text,
					(SELECT count(*) FROM profile_certification_circuit_openings
					 WHERE profile_certification_id = certification.id),
					attempt.started_at < certification.invalidated_at,
					decision.circuit_protocol_version,
					decision.worker_was_healthy,
					job.state::text,
					runtime.circuit_breaker_state ->> 'state',
					reservation.state::text
				FROM attempts AS attempt
				JOIN jobs AS job ON job.id = attempt.job_id
				JOIN retry_runtime_states AS runtime ON runtime.job_id = job.id
				JOIN credit_reservations AS reservation ON reservation.job_id = job.id
				JOIN profile_certifications AS certification
				  ON certification.id = attempt.profile_certification_id
				JOIN execution_failure_decisions AS decision ON decision.attempt_id = attempt.id
				WHERE attempt.id = $1 AND job.id = $2
			`, alreadyRunningAssignment.AttemptID, alreadyRunningJob).Scan(
				&certificationState,
				&openingCount,
				&attemptPredatesOpen,
				&protocolVersion,
				&workerWasHealthy,
				&jobState,
				&circuitState,
				&reservationState,
			); err != nil {
				t.Fatalf("read already-running circuit result: %v", err)
			}
			if certificationState != "INVALID" || openingCount != 1 || !attemptPredatesOpen ||
				protocolVersion != 2 || !workerWasHealthy || jobState != test.wantJobState ||
				circuitState != "OPEN" || reservationState != test.wantReservation {
				t.Fatalf(
					"already-running circuit = certification %s openings %d predates %t protocol %d healthy %t job %s circuit %s reservation %s",
					certificationState,
					openingCount,
					attemptPredatesOpen,
					protocolVersion,
					workerWasHealthy,
					jobState,
					circuitState,
					reservationState,
				)
			}

			if test.addAlternate {
				waitForDatabaseTimeAfter(t, fixture.database.Admin, *later.NextRetryAt)
				replacement, acquireErr := fixture.service.Acquire(
					context.Background(),
					fixture.workers[1],
					7,
					&workercontrol.AssignmentCandidate{
						JobID:                      alreadyRunningJob,
						ExpectedJobVersion:         later.JobVersion,
						ExecutionProfileRevisionID: fixture.alternateProfileID,
					},
				)
				if acquireErr != nil {
					t.Fatalf("Acquire alternate for already-running Job: %v", acquireErr)
				}
				if replacement.ExecutionProfileRevisionID != fixture.alternateProfileID ||
					replacement.LeaseFence <= later.JobFence {
					t.Fatalf("already-running alternate replacement = %#v", replacement)
				}
			}
		})
	}
}

func TestSchedulerAbandonsClaimWhenProfileCircuitOpensBeforeAcquire(t *testing.T) {
	fixture := newProfileCircuitFixture(t, 3, false)
	observation := validFailureObservation()

	firstJob, firstAssignment := fixture.assignAndStart(
		t, 0, "profile-circuit-scheduler-race-first",
	)
	first, err := fixture.service.Fail(
		context.Background(), fixture.workers[0], leaseCredentials(firstAssignment), observation,
	)
	if err != nil || first.Disposition != workercontrol.RetryDispositionRetryWait {
		t.Fatalf("first Scheduler-race circuit evidence = %#v error=%v", first, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE retry_runtime_states
		SET next_retry_at = clock_timestamp() + interval '1 hour'
		WHERE job_id = $1
	`, firstJob); err != nil {
		t.Fatalf("defer first circuit retry beyond Scheduler race: %v", err)
	}

	_, thresholdAssignment := fixture.assignAndStart(
		t, 1, "profile-circuit-scheduler-race-threshold",
	)
	queuedJob := submitSchedulerJob(
		t,
		fixture.serverURL,
		testProjectID,
		testBearerCredential(),
		"profile-circuit-scheduler-race-queued",
	)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
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
		fixture.workers[2].ID,
		7,
		fixture.primaryProfileID,
	)

	coordinator := &profileCircuitOpeningCoordinator{
		delegate: fixture.service,
		open: func() error {
			decision, failErr := fixture.service.Fail(
				context.Background(),
				fixture.workers[1],
				leaseCredentials(thresholdAssignment),
				observation,
			)
			if failErr == nil && decision.Disposition != workercontrol.RetryDispositionFailed {
				return fmt.Errorf("threshold decision disposition = %s", decision.Disposition)
			}
			return failErr
		},
	}
	schedulerPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_scheduler_login",
		"vela-scheduler-password",
	)
	scheduling, err := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "profile-circuit-claim-race",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 2,
	})
	if err != nil {
		t.Fatalf("create Profile circuit race Scheduler: %v", err)
	}
	dispatch, ok, err := scheduling.RunOnce(context.Background(), poolID)
	if err != nil || ok {
		t.Fatalf("Profile circuit race Scheduler = %#v ok=%t error=%v", dispatch, ok, err)
	}
	if coordinator.openErr != nil {
		t.Fatalf("open Profile circuit between claim and Acquire: %v", coordinator.openErr)
	}

	var jobState, certificationState, intentState, abandonReason string
	var attempts, openings int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state::text,
			certification.state::text,
			intent.state::text,
			intent.abandon_reason,
			(SELECT count(*) FROM attempts WHERE job_id = job.id),
			(SELECT count(*) FROM profile_certification_circuit_openings
			 WHERE profile_certification_id = certification.id)
		FROM jobs AS job
		JOIN scheduler_dispatch_intents AS intent ON intent.job_id = job.id
		JOIN profile_certifications AS certification ON certification.id = $2
		WHERE job.id = $1
	`, queuedJob, fixture.primaryCertificationID).Scan(
		&jobState,
		&certificationState,
		&intentState,
		&abandonReason,
		&attempts,
		&openings,
	); err != nil {
		t.Fatalf("read Profile circuit Scheduler race receipt: %v", err)
	}
	if jobState != "QUEUED" || certificationState != "INVALID" || intentState != "ABANDONED" ||
		!strings.Contains(abandonReason, "candidate_unavailable") || attempts != 0 || openings != 1 {
		t.Fatalf(
			"Profile circuit Scheduler race = job %s certification %s intent %s reason %q Attempts %d openings %d",
			jobState,
			certificationState,
			intentState,
			abandonReason,
			attempts,
			openings,
		)
	}
}

func TestProfileCircuitCrossProjectAcquireFirstLinearizesWithoutDeadlock(t *testing.T) {
	fixture := newProfileCircuitFixture(t, 3, false)
	observation := validFailureObservation()

	firstJob, firstAssignment := fixture.assignAndStart(
		t, 0, "profile-circuit-acquire-first-evidence",
	)
	first, err := fixture.service.Fail(
		context.Background(), fixture.workers[0], leaseCredentials(firstAssignment), observation,
	)
	if err != nil || first.Disposition != workercontrol.RetryDispositionRetryWait {
		t.Fatalf("first Acquire-first circuit evidence = %#v error=%v", first, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE retry_runtime_states
		SET next_retry_at = clock_timestamp() + interval '1 hour'
		WHERE job_id = $1
	`, firstJob); err != nil {
		t.Fatalf("defer first circuit retry beyond Acquire-first race: %v", err)
	}

	_, thresholdAssignment := fixture.assignAndStart(
		t, 1, "profile-circuit-acquire-first-threshold",
	)
	seedProfileCircuitSecondProject(t, fixture.database.Admin)
	queuedJob := submitSchedulerJob(
		t,
		fixture.serverURL,
		testProjectTwoID,
		bearerCredential(testCredentialTwoID, testCredentialTwoSecret),
		"profile-circuit-acquire-first-queued",
	)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	seedSchedulerCapacityShares(
		t,
		fixture.database.Admin,
		poolID,
		schedulerCapacityShare{
			OrganizationID: testOrganizationID,
			ProjectID:      testProjectID,
			Weight:         1,
			RunningLimit:   2,
			ProjectWeight:  1,
		},
		schedulerCapacityShare{
			OrganizationID: testOrganizationID,
			ProjectID:      testProjectTwoID,
			Weight:         1,
			RunningLimit:   2,
			ProjectWeight:  1,
		},
	)
	seedSchedulerWorkerReadiness(
		t,
		fixture.database.Admin,
		fixture.workers[2].ID,
		7,
		fixture.primaryProfileID,
	)

	schedulerPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_scheduler_login",
		"vela-scheduler-password",
	)
	var intentID, claimedJobID, workerID, profileID uuid.UUID
	var expectedJobVersion, workerEpoch int64
	if err := schedulerPool.QueryRow(context.Background(), `
		SELECT
			intent_id,
			job_id,
			expected_job_version,
			worker_id,
			worker_epoch,
			execution_profile_revision_id
		FROM vela_claim_scheduler_dispatch($1, 'profile-circuit-acquire-first', 30)
	`, poolID).Scan(
		&intentID,
		&claimedJobID,
		&expectedJobVersion,
		&workerID,
		&workerEpoch,
		&profileID,
	); err != nil {
		t.Fatalf("claim Acquire-first Scheduler dispatch: %v", err)
	}
	if claimedJobID != queuedJob || workerID != fixture.workers[2].ID ||
		profileID != fixture.primaryProfileID {
		t.Fatalf(
			"Acquire-first claim = job %s Worker %s profile %s",
			claimedJobID,
			workerID,
			profileID,
		)
	}

	const advisoryLockKey int64 = 580011
	if _, err := fixture.database.Admin.Exec(`
		CREATE FUNCTION vela_test_pause_cross_project_circuit_assignment() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock(580011);
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER attempts_pause_cross_project_circuit_assignment
		BEFORE INSERT ON attempts
		FOR EACH ROW EXECUTE FUNCTION vela_test_pause_cross_project_circuit_assignment();
	`); err != nil {
		t.Fatalf("install cross-Project circuit Assignment pause: %v", err)
	}
	blocker, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin cross-Project circuit Assignment blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	if _, err := blocker.Exec("SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		t.Fatalf("acquire cross-Project circuit Assignment blocker: %v", err)
	}

	type acquireResult struct {
		assignment workercontrol.Assignment
		err        error
	}
	acquireResults := make(chan acquireResult, 1)
	go func() {
		assignment, acquireErr := fixture.service.Acquire(
			context.Background(),
			fixture.workers[2],
			workerEpoch,
			&workercontrol.AssignmentCandidate{
				JobID:                      claimedJobID,
				ExpectedJobVersion:         expectedJobVersion,
				ExecutionProfileRevisionID: profileID,
				SchedulerClaim: &workercontrol.SchedulerClaim{
					IntentID:     intentID,
					WorkerPoolID: poolID,
				},
			},
		)
		acquireResults <- acquireResult{assignment: assignment, err: acquireErr}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_internal_login")

	type failResult struct {
		decision workercontrol.RetryDecision
		err      error
	}
	failResults := make(chan failResult, 1)
	go func() {
		decision, failErr := fixture.service.Fail(
			context.Background(),
			fixture.workers[1],
			leaseCredentials(thresholdAssignment),
			observation,
		)
		failResults <- failResult{decision: decision, err: failErr}
	}()

	deadline := time.Now().Add(6 * time.Second)
	for {
		var waiting int
		if err := fixture.database.Admin.QueryRow(`
			SELECT count(*)
			FROM pg_stat_activity
			WHERE usename = 'vela_internal_login' AND wait_event_type = 'Lock'
		`).Scan(&waiting); err != nil {
			t.Fatalf("inspect Acquire/Fail lock waits: %v", err)
		}
		if waiting >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Acquire/Fail lock waits = %d, want 2", waiting)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var unlocked bool
	if err := blocker.QueryRow("SELECT pg_advisory_unlock($1)", advisoryLockKey).Scan(&unlocked); err != nil {
		t.Fatalf("release cross-Project circuit Assignment blocker: %v", err)
	}
	if !unlocked {
		t.Fatal("cross-Project circuit Assignment blocker was not held")
	}
	if err := blocker.Commit(); err != nil {
		t.Fatalf("commit cross-Project circuit Assignment blocker: %v", err)
	}

	select {
	case acquired := <-acquireResults:
		if acquired.err != nil || acquired.assignment.JobID != queuedJob {
			t.Fatalf("Acquire-first scheduled Assignment = %#v error=%v", acquired.assignment, acquired.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Acquire-first scheduled Assignment did not finish")
	}
	select {
	case failed := <-failResults:
		if failed.err != nil || failed.decision.Disposition != workercontrol.RetryDispositionFailed {
			t.Fatalf("Acquire-first threshold Fail = %#v error=%v", failed.decision, failed.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Acquire-first threshold Fail did not finish")
	}

	var certificationState string
	var assignedAttempts, openings int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			certification.state::text,
			(SELECT count(*) FROM attempts WHERE job_id = $2),
			(SELECT count(*) FROM profile_certification_circuit_openings
			 WHERE profile_certification_id = certification.id)
		FROM profile_certifications AS certification
		WHERE certification.id = $1
	`, fixture.primaryCertificationID, queuedJob).Scan(
		&certificationState,
		&assignedAttempts,
		&openings,
	); err != nil {
		t.Fatalf("read Acquire-first circuit result: %v", err)
	}
	if certificationState != "INVALID" || assignedAttempts != 1 || openings != 1 {
		t.Fatalf(
			"Acquire-first circuit = certification %s Attempts %d openings %d",
			certificationState,
			assignedAttempts,
			openings,
		)
	}
}

func TestProfileCircuitOpeningSerializesBeforeCrossProjectAdmission(t *testing.T) {
	fixture := newProfileCircuitFixture(t, 2, false)
	seedProfileCircuitSecondProject(t, fixture.database.Admin)
	observation := validFailureObservation()

	firstJob, firstAssignment := fixture.assignAndStart(
		t, 0, "profile-circuit-admission-race-evidence",
	)
	first, err := fixture.service.Fail(
		context.Background(), fixture.workers[0], leaseCredentials(firstAssignment), observation,
	)
	if err != nil || first.Disposition != workercontrol.RetryDispositionRetryWait {
		t.Fatalf("first Admission-race circuit evidence = %#v error=%v", first, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE retry_runtime_states
		SET next_retry_at = clock_timestamp() + interval '1 hour'
		WHERE job_id = $1
	`, firstJob); err != nil {
		t.Fatalf("defer first circuit retry beyond Admission race: %v", err)
	}
	_, thresholdAssignment := fixture.assignAndStart(
		t, 1, "profile-circuit-admission-race-threshold",
	)

	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	blocker, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Admission circuit pool blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	var lockedPoolID uuid.UUID
	if err := blocker.QueryRow(`
		SELECT id FROM worker_pools WHERE id = $1 FOR UPDATE
	`, poolID).Scan(&lockedPoolID); err != nil {
		t.Fatalf("lock Admission circuit Worker pool: %v", err)
	}

	type failResult struct {
		decision workercontrol.RetryDecision
		err      error
	}
	failResults := make(chan failResult, 1)
	go func() {
		decision, failErr := fixture.service.Fail(
			context.Background(),
			fixture.workers[1],
			leaseCredentials(thresholdAssignment),
			observation,
		)
		failResults <- failResult{decision: decision, err: failErr}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_internal_login")

	type admissionResult struct {
		response httpResult
		err      error
	}
	admissionResults := make(chan admissionResult, 1)
	go func() {
		response, submitErr := doSubmitJob(
			fixture.serverURL,
			testProjectTwoID,
			bearerCredential(testCredentialTwoID, testCredentialTwoSecret),
			"profile-circuit-admission-race",
			[]byte(`{
				"model":"minimax-h3",
				"generation_preset":"balanced",
				"service_class":"standard",
				"output_spec":"video-1080p-5s-24fps",
				"generation_count":1,
				"prompt":"exercise Profile circuit Admission serialization"
			}`),
		)
		admissionResults <- admissionResult{response: response, err: submitErr}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_request_login")

	if err := blocker.Commit(); err != nil {
		t.Fatalf("release Admission circuit Worker pool blocker: %v", err)
	}
	select {
	case failed := <-failResults:
		if failed.err != nil || failed.decision.Disposition != workercontrol.RetryDispositionFailed {
			t.Fatalf("Admission-race threshold Fail = %#v error=%v", failed.decision, failed.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Admission-race threshold Fail did not finish")
	}
	select {
	case admitted := <-admissionResults:
		if admitted.err != nil || admitted.response.StatusCode != 503 {
			t.Fatalf(
				"cross-Project Admission after circuit = status %d body=%s error=%v",
				admitted.response.StatusCode,
				admitted.response.Body,
				admitted.err,
			)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cross-Project Admission after circuit did not finish")
	}

	var certificationState string
	var openings, secondProjectJobs, secondProjectQueued int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			certification.state::text,
			(SELECT count(*) FROM profile_certification_circuit_openings
			 WHERE profile_certification_id = certification.id),
			(SELECT count(*) FROM jobs WHERE project_id = $2),
			(SELECT queued_count FROM projects WHERE id = $2)
		FROM profile_certifications AS certification
		WHERE certification.id = $1
	`, fixture.primaryCertificationID, testProjectTwoID).Scan(
		&certificationState,
		&openings,
		&secondProjectJobs,
		&secondProjectQueued,
	); err != nil {
		t.Fatalf("read cross-Project Admission circuit result: %v", err)
	}
	if certificationState != "INVALID" || openings != 1 ||
		secondProjectJobs != 0 || secondProjectQueued != 0 {
		t.Fatalf(
			"cross-Project Admission circuit = certification %s openings %d Jobs %d queued %d",
			certificationState,
			openings,
			secondProjectJobs,
			secondProjectQueued,
		)
	}
}

func TestProfileCircuitGateOnJobExpiryWritesProtocolTwoWithoutOpening(t *testing.T) {
	fixture := newAssignmentFixture(t, "profile-circuit-gate-on-job-expiry", 7)
	setProfileCircuitProtocolGate(
		t,
		fixture.database.Admin,
		true,
		"N writer active for Job Expiry protocol evidence",
	)
	forceJobPolicySnapshot(
		t,
		fixture.database.Admin,
		fixture.candidate.JobID,
		"job_expires_at = clock_timestamp()",
	)

	result, err := fixture.service.ReconcileNextExecutionFailure(context.Background())
	if err != nil {
		t.Fatalf("reconcile gate-on queued Job Expiry: %v", err)
	}
	if !result.Processed || result.Source != workercontrol.FailureSourceJobExpired ||
		result.Decision.Disposition != workercontrol.RetryDispositionFailed ||
		result.Decision.AttemptID != uuid.Nil {
		t.Fatalf("gate-on Job Expiry result = %#v", result)
	}

	var protocolVersion int
	var workerWasHealthy bool
	var openingCount int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			decision.circuit_protocol_version,
			decision.worker_was_healthy,
			(SELECT count(*) FROM profile_certification_circuit_openings)
		FROM execution_failure_decisions AS decision
		WHERE decision.job_id = $1
	`, fixture.candidate.JobID).Scan(
		&protocolVersion,
		&workerWasHealthy,
		&openingCount,
	); err != nil {
		t.Fatalf("read gate-on Job Expiry protocol evidence: %v", err)
	}
	if protocolVersion != 2 || workerWasHealthy || openingCount != 0 {
		t.Fatalf(
			"gate-on Job Expiry protocol = %d healthy=%t openings=%d",
			protocolVersion,
			workerWasHealthy,
			openingCount,
		)
	}
}

func TestProfileCircuitCountsOnlyIndependentHealthyWorkerEvidence(t *testing.T) {
	t.Run("same Worker", func(t *testing.T) {
		fixture := newProfileCircuitFixture(t, 1, false)
		observation := validFailureObservation()
		for index := range 2 {
			_, assignment := fixture.assignAndStart(
				t,
				0,
				fmt.Sprintf("profile-circuit-same-worker-%d", index),
			)
			decision, err := fixture.service.Fail(
				context.Background(),
				fixture.workers[0],
				leaseCredentials(assignment),
				observation,
			)
			if err != nil || decision.Disposition != workercontrol.RetryDispositionRetryWait {
				t.Fatalf("same-Worker circuit Fail %d = %#v error=%v", index, decision, err)
			}
		}
		assertProfileCircuitClosed(t, fixture.database.Admin, fixture.primaryCertificationID)
	})

	t.Run("SUSPECT Worker", func(t *testing.T) {
		fixture := newProfileCircuitFixture(t, 2, false)
		observation := validFailureObservation()
		_, firstAssignment := fixture.assignAndStart(t, 0, "profile-circuit-healthy-before-suspect")
		if _, err := fixture.service.Fail(
			context.Background(),
			fixture.workers[0],
			leaseCredentials(firstAssignment),
			observation,
		); err != nil {
			t.Fatalf("record first healthy circuit failure: %v", err)
		}
		_, suspectAssignment := fixture.assignAndStart(t, 1, "profile-circuit-suspect-worker")
		if _, err := fixture.database.Admin.Exec(
			"UPDATE workers SET reachability_condition = 'SUSPECT' WHERE id = $1",
			fixture.workers[1].ID,
		); err != nil {
			t.Fatalf("mark circuit Worker SUSPECT: %v", err)
		}
		if _, err := fixture.service.Fail(
			context.Background(),
			fixture.workers[1],
			leaseCredentials(suspectAssignment),
			observation,
		); err != nil {
			t.Fatalf("record SUSPECT Worker failure: %v", err)
		}
		assertProfileCircuitClosed(t, fixture.database.Admin, fixture.primaryCertificationID)
		var workerWasHealthy bool
		if err := fixture.database.Admin.QueryRow(`
			SELECT worker_was_healthy
			FROM execution_failure_decisions
			WHERE attempt_id = $1
		`, suspectAssignment.AttemptID).Scan(&workerWasHealthy); err != nil {
			t.Fatalf("read SUSPECT Worker failure evidence: %v", err)
		}
		if workerWasHealthy {
			t.Fatal("SUSPECT Worker failure was recorded as healthy circuit evidence")
		}
	})

	t.Run("different fingerprint", func(t *testing.T) {
		fixture := newProfileCircuitFixture(t, 2, false)
		firstObservation := validFailureObservation()
		_, firstAssignment := fixture.assignAndStart(t, 0, "profile-circuit-fingerprint-first")
		if _, err := fixture.service.Fail(
			context.Background(),
			fixture.workers[0],
			leaseCredentials(firstAssignment),
			firstObservation,
		); err != nil {
			t.Fatalf("record first fingerprint: %v", err)
		}
		secondObservation := firstObservation
		secondObservation.FailureFingerprint = "sglang.process.exit.143"
		_, secondAssignment := fixture.assignAndStart(t, 1, "profile-circuit-fingerprint-second")
		if _, err := fixture.service.Fail(
			context.Background(),
			fixture.workers[1],
			leaseCredentials(secondAssignment),
			secondObservation,
		); err != nil {
			t.Fatalf("record different fingerprint: %v", err)
		}
		assertProfileCircuitClosed(t, fixture.database.Admin, fixture.primaryCertificationID)
	})

	t.Run("different certification", func(t *testing.T) {
		fixture := newProfileCircuitFixture(t, 2, true)
		observation := validFailureObservation()
		_, primaryAssignment := fixture.assignAndStart(t, 0, "profile-circuit-certification-primary")
		if _, err := fixture.service.Fail(
			context.Background(),
			fixture.workers[0],
			leaseCredentials(primaryAssignment),
			observation,
		); err != nil {
			t.Fatalf("record primary certification failure: %v", err)
		}
		_, alternateAssignment := fixture.assignAndStartOnProfile(
			t,
			1,
			"profile-circuit-certification-alternate",
			fixture.alternateProfileID,
		)
		if _, err := fixture.service.Fail(
			context.Background(),
			fixture.workers[1],
			leaseCredentials(alternateAssignment),
			observation,
		); err != nil {
			t.Fatalf("record alternate certification failure: %v", err)
		}
		assertProfileCircuitClosed(t, fixture.database.Admin, fixture.primaryCertificationID)
		assertProfileCircuitClosed(t, fixture.database.Admin, fixture.alternateCertificationID)
	})

	t.Run("expired window", func(t *testing.T) {
		fixture := newProfileCircuitFixture(t, 2, false)
		observation := validFailureObservation()
		_, firstAssignment := fixture.assignAndStart(t, 0, "profile-circuit-window-first")
		first, err := fixture.service.Fail(
			context.Background(),
			fixture.workers[0],
			leaseCredentials(firstAssignment),
			observation,
		)
		if err != nil {
			t.Fatalf("record first windowed failure: %v", err)
		}
		waitForDatabaseTimeAfter(t, fixture.database.Admin, first.DecidedAt.Add(time.Second))
		secondJob, secondAssignment := fixture.assignAndStart(t, 1, "profile-circuit-window-second")
		forceJobPolicySnapshot(
			t,
			fixture.database.Admin,
			secondJob,
			"execution_circuit_fingerprint_window_seconds = 1",
		)
		if _, err := fixture.service.Fail(
			context.Background(),
			fixture.workers[1],
			leaseCredentials(secondAssignment),
			observation,
		); err != nil {
			t.Fatalf("record failure after circuit window: %v", err)
		}
		assertProfileCircuitClosed(t, fixture.database.Admin, fixture.primaryCertificationID)
	})

	t.Run("Lease loss reconciliation", func(t *testing.T) {
		fixture := newProfileCircuitFixture(t, 2, false)
		observation := validFailureObservation()
		_, firstAssignment := fixture.assignAndStart(t, 0, "profile-circuit-reconcile-first")
		if _, err := fixture.service.Fail(
			context.Background(),
			fixture.workers[0],
			leaseCredentials(firstAssignment),
			observation,
		); err != nil {
			t.Fatalf("record first failure before Lease loss: %v", err)
		}
		_, lostAssignment := fixture.assign(t, 1, "profile-circuit-reconcile-lost")
		forceLeaseExpiry(t, fixture.database.Admin, lostAssignment.AttemptID, 31*time.Second)
		result, err := fixture.service.ReconcileNextExecutionFailure(context.Background())
		if err != nil {
			t.Fatalf("reconcile circuit Lease loss: %v", err)
		}
		if !result.Processed || result.Source != workercontrol.FailureSourceExecutionLeaseExpired {
			t.Fatalf("circuit Lease loss result = %#v", result)
		}
		assertProfileCircuitClosed(t, fixture.database.Admin, fixture.primaryCertificationID)
		var workerWasHealthy bool
		if err := fixture.database.Admin.QueryRow(`
			SELECT worker_was_healthy
			FROM execution_failure_decisions
			WHERE attempt_id = $1
		`, lostAssignment.AttemptID).Scan(&workerWasHealthy); err != nil {
			t.Fatalf("read Lease loss circuit evidence: %v", err)
		}
		if workerWasHealthy {
			t.Fatal("Lease loss reconciliation was recorded as healthy Worker evidence")
		}
	})
}

func TestConcurrentProfileCircuitThresholdFailuresOpenOnceAndPreserveResults(t *testing.T) {
	fixture := newProfileCircuitFixture(t, 2, true)
	assignments := make([]workercontrol.Assignment, 0, 2)
	for index := range 2 {
		_, assignment := fixture.assignAndStart(
			t,
			index,
			fmt.Sprintf("profile-circuit-concurrent-%d", index),
		)
		assignments = append(assignments, assignment)
	}

	type failureResult struct {
		decision workercontrol.RetryDecision
		err      error
	}
	start := make(chan struct{})
	results := make(chan failureResult, 2)
	var wait sync.WaitGroup
	for index := range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			decision, err := fixture.service.Fail(
				context.Background(),
				fixture.workers[index],
				leaseCredentials(assignments[index]),
				validFailureObservation(),
			)
			results <- failureResult{decision: decision, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent threshold Fail: %v", result.err)
		}
		if result.decision.Disposition != workercontrol.RetryDispositionRetryWait ||
			result.decision.NextRetryAt == nil {
			t.Fatalf("concurrent threshold decision = %#v", result.decision)
		}
	}

	var (
		certificationState string
		openingCount       int
		decisionCount      int
		failedAttempts     int
		revokedLeases      int
		retryJobs          int
		openRuntimeStates  int
		projectQueued      int
		projectRetryWait   int
		projectRunning     int
		poolQueued         int
		poolRetryWait      int
		reservedJobs       int
		reservedMinor      int64
		retryEvents        int
		readyWorkers       int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT state::text FROM profile_certifications WHERE id = $1),
			(SELECT count(*) FROM profile_certification_circuit_openings
			 WHERE profile_certification_id = $1),
			(SELECT count(*) FROM execution_failure_decisions WHERE attempt_id = ANY($2::uuid[])),
			(SELECT count(*) FROM attempts WHERE id = ANY($2::uuid[]) AND state = 'FAILED'),
			(SELECT count(*) FROM attempt_leases WHERE attempt_id = ANY($2::uuid[]) AND revoked_at IS NOT NULL),
			(SELECT count(*) FROM jobs WHERE id = ANY($3::uuid[]) AND state = 'RETRY_WAIT'),
			(SELECT count(*) FROM retry_runtime_states
			 WHERE job_id = ANY($3::uuid[]) AND circuit_breaker_state ->> 'state' = 'OPEN'),
			(SELECT queued_count FROM projects WHERE id = $4),
			(SELECT retry_wait_count FROM projects WHERE id = $4),
			(SELECT running_count FROM projects WHERE id = $4),
			(SELECT queued_count FROM worker_pools WHERE id = $5),
			(SELECT retry_wait_count FROM worker_pools WHERE id = $5),
			(SELECT count(*) FROM credit_reservations
			 WHERE job_id = ANY($3::uuid[]) AND state = 'RESERVED'),
			(SELECT reserved_minor FROM organization_credit_accounts WHERE organization_id = $6),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = ANY($3::uuid[]) AND event_type = 'job.retry_wait'),
			(SELECT count(*) FROM workers
			 WHERE id = ANY($7::uuid[]) AND lifecycle_state = 'READY'
			   AND reachability_condition = 'HEALTHY')
	`,
		fixture.primaryCertificationID,
		[]uuid.UUID{assignments[0].AttemptID, assignments[1].AttemptID},
		[]uuid.UUID{assignments[0].JobID, assignments[1].JobID},
		testProjectID,
		uuid.MustParse("00000000-0000-0000-0000-000000000005"),
		uuid.MustParse(testOrganizationID),
		[]uuid.UUID{fixture.workers[0].ID, fixture.workers[1].ID},
	).Scan(
		&certificationState,
		&openingCount,
		&decisionCount,
		&failedAttempts,
		&revokedLeases,
		&retryJobs,
		&openRuntimeStates,
		&projectQueued,
		&projectRetryWait,
		&projectRunning,
		&poolQueued,
		&poolRetryWait,
		&reservedJobs,
		&reservedMinor,
		&retryEvents,
		&readyWorkers,
	); err != nil {
		t.Fatalf("read concurrent ProfileCertification circuit result: %v", err)
	}
	if certificationState != "INVALID" || openingCount != 1 || decisionCount != 2 ||
		failedAttempts != 2 || revokedLeases != 2 || retryJobs != 2 || openRuntimeStates != 1 ||
		projectQueued != 2 || projectRetryWait != 2 || projectRunning != 0 ||
		poolQueued != 2 || poolRetryWait != 2 || reservedJobs != 2 || reservedMinor != 2500 ||
		retryEvents != 2 || readyWorkers != 2 {
		t.Fatalf(
			"concurrent circuit result = certification %s openings %d decisions %d attempts %d leases %d jobs %d open states %d project %d/%d/%d pool %d/%d reservations %d/%d events %d workers %d",
			certificationState,
			openingCount,
			decisionCount,
			failedAttempts,
			revokedLeases,
			retryJobs,
			openRuntimeStates,
			projectQueued,
			projectRetryWait,
			projectRunning,
			poolQueued,
			poolRetryWait,
			reservedJobs,
			reservedMinor,
			retryEvents,
			readyWorkers,
		)
	}
}

func TestProfileCircuitThresholdFailureRollsBackEveryOpeningBoundary(t *testing.T) {
	tests := []struct {
		name   string
		table  string
		action string
	}{
		{name: "certification invalidation", table: "profile_certifications", action: "UPDATE"},
		{name: "failure decision", table: "execution_failure_decisions", action: "INSERT"},
		{name: "opening receipt", table: "profile_certification_circuit_openings", action: "INSERT"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newProfileCircuitFixture(t, 2, true)
			observation := validFailureObservation()
			_, firstAssignment := fixture.assignAndStart(
				t,
				0,
				"profile-circuit-rollback-first-"+test.table,
			)
			if _, err := fixture.service.Fail(
				context.Background(),
				fixture.workers[0],
				leaseCredentials(firstAssignment),
				observation,
			); err != nil {
				t.Fatalf("record below-threshold rollback fixture: %v", err)
			}
			_, thresholdAssignment := fixture.assignAndStart(
				t,
				1,
				"profile-circuit-rollback-threshold-"+test.table,
			)
			before := readProfileCircuitRollbackState(
				t,
				fixture.database.Admin,
				fixture.primaryCertificationID,
				thresholdAssignment,
				fixture.workers[1].ID,
			)
			if _, err := fixture.database.Admin.Exec(fmt.Sprintf(`
				CREATE FUNCTION vela_test_reject_profile_circuit_write() RETURNS trigger
				LANGUAGE plpgsql AS $$
				BEGIN
					RAISE EXCEPTION 'injected ProfileCertification circuit failure';
				END
				$$;
				CREATE TRIGGER vela_test_reject_profile_circuit_write
				BEFORE %s ON %s
				FOR EACH ROW EXECUTE FUNCTION vela_test_reject_profile_circuit_write();
			`, test.action, test.table)); err != nil {
				t.Fatalf("install %s rollback trigger: %v", test.name, err)
			}
			decision, failErr := fixture.service.Fail(
				context.Background(),
				fixture.workers[1],
				leaseCredentials(thresholdAssignment),
				observation,
			)
			if failErr == nil || decision.Disposition != "" {
				t.Fatalf("threshold Fail with rejected %s = %#v error=%v", test.name, decision, failErr)
			}
			after := readProfileCircuitRollbackState(
				t,
				fixture.database.Admin,
				fixture.primaryCertificationID,
				thresholdAssignment,
				fixture.workers[1].ID,
			)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf(
					"rejected %s committed partial circuit state: before=%#v after=%#v",
					test.name,
					before,
					after,
				)
			}
		})
	}
}

func TestProfileCircuitDatabaseInvariantsAndRoleIsolation(t *testing.T) {
	fixture := newProfileCircuitFixture(t, 2, true)
	observation := validFailureObservation()
	assignments := make([]workercontrol.Assignment, 0, 2)
	for index := range 2 {
		_, assignment := fixture.assignAndStart(
			t,
			index,
			fmt.Sprintf("profile-circuit-invariants-%d", index),
		)
		assignments = append(assignments, assignment)
		if _, err := fixture.service.Fail(
			context.Background(),
			fixture.workers[index],
			leaseCredentials(assignment),
			observation,
		); err != nil {
			t.Fatalf("open ProfileCertification circuit for invariant test: %v", err)
		}
	}

	assertDatabaseError := func(label, statement, code, constraint string, arguments ...any) {
		t.Helper()
		_, err := fixture.database.Admin.Exec(statement, arguments...)
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != code ||
			(constraint != "" && postgresError.ConstraintName != constraint) {
			t.Fatalf(
				"%s error = %v, want SQLSTATE %s constraint %s",
				label,
				err,
				code,
				constraint,
			)
		}
	}

	assertDatabaseError(
		"ProfileCertification reactivation",
		"UPDATE profile_certifications SET state = 'ACTIVE', invalidated_at = NULL WHERE id = $1",
		"55000",
		"profile_certifications_invalidation_immutable",
		fixture.primaryCertificationID,
	)
	assertDatabaseError(
		"ProfileCertification invalidation retimestamp",
		"UPDATE profile_certifications SET invalidated_at = invalidated_at + interval '1 second' WHERE id = $1",
		"55000",
		"profile_certifications_invalidation_immutable",
		fixture.primaryCertificationID,
	)
	assertDatabaseError(
		"referenced ProfileCertification delete",
		"DELETE FROM profile_certifications WHERE id = $1",
		"23503",
		"",
		fixture.primaryCertificationID,
	)
	for label, statement := range map[string]string{
		"opening update":   "UPDATE profile_certification_circuit_openings SET observed_distinct_healthy_workers = observed_distinct_healthy_workers + 1",
		"opening delete":   "DELETE FROM profile_certification_circuit_openings",
		"opening truncate": "TRUNCATE profile_certification_circuit_openings",
	} {
		assertDatabaseError(
			label,
			statement,
			"55000",
			"profile_certification_circuit_openings_immutable",
		)
	}
	assertDatabaseError(
		"Job circuit policy mutation",
		"UPDATE jobs SET execution_circuit_fingerprint_window_seconds = 2 WHERE id = $1",
		"55000",
		"jobs_circuit_policy_snapshot_immutable",
		assignments[1].JobID,
	)
	assertDatabaseError(
		"ServiceClassRevision circuit policy mutation",
		"UPDATE service_class_revisions SET circuit_fingerprint_window_seconds = 2 WHERE id = '00000000-0000-0000-0000-000000000012'",
		"P0001",
		"",
	)
	assertDatabaseError(
		"Attempt ProfileCertification mutation",
		"UPDATE attempts SET profile_certification_id = $1 WHERE id = $2",
		"55000",
		"attempts_profile_certification_immutable",
		fixture.alternateCertificationID,
		assignments[1].AttemptID,
	)
	assertDatabaseError(
		"cross-identity opening receipt",
		`INSERT INTO profile_certification_circuit_openings (
			id,
			organization_id,
			project_id,
			profile_certification_id,
			execution_profile_revision_id,
			triggering_execution_failure_decision_id,
			triggering_job_id,
			triggering_attempt_id,
			triggering_worker_id,
			triggering_worker_epoch,
			triggering_attempt_fence,
			failure_class,
			failure_fingerprint,
			inference_backend_revision,
			policy_fingerprint_window_seconds,
			policy_min_distinct_healthy_workers,
			observed_distinct_healthy_workers,
			evidence_window_started_at,
			opened_at
		)
		SELECT
			$1,
			organization_id,
			project_id,
			$2,
			$3,
			triggering_execution_failure_decision_id,
			triggering_job_id,
			triggering_attempt_id,
			triggering_worker_id,
			triggering_worker_epoch,
			triggering_attempt_fence,
			failure_class,
			failure_fingerprint,
			inference_backend_revision,
			policy_fingerprint_window_seconds,
			policy_min_distinct_healthy_workers,
			observed_distinct_healthy_workers,
			evidence_window_started_at,
			opened_at
		FROM profile_certification_circuit_openings
		WHERE profile_certification_id = $4`,
		"23514",
		"profile_certification_circuit_opening_identity",
		uuid.New(),
		fixture.alternateCertificationID,
		fixture.alternateProfileID,
		fixture.primaryCertificationID,
	)
	assertDatabaseError(
		"legacy failure writer after protocol activation",
		`INSERT INTO execution_failure_decisions (
			id, organization_id, project_id, job_id, attempt_id, worker_id,
			worker_epoch, attempt_fence, source, disposition, attempt_state,
			failure_class, failure_fingerprint, request_hash, error_summary,
			backend_stage, gpu_uuids, inference_backend_revision,
			retry_recommended, worker_reusable, attempt_compute_seconds,
			total_compute_seconds, attempt_finalization_seconds,
			total_finalization_seconds, artifact_id, artifact_upload_id,
			finalization_failure_code, next_retry_at, job_fence, job_version, decided_at
		)
		SELECT
			$1, organization_id, project_id, job_id, attempt_id, worker_id,
			worker_epoch, attempt_fence, source, disposition, attempt_state,
			failure_class, failure_fingerprint, request_hash, error_summary,
			backend_stage, gpu_uuids, inference_backend_revision,
			retry_recommended, worker_reusable, attempt_compute_seconds,
			total_compute_seconds, attempt_finalization_seconds,
			total_finalization_seconds, artifact_id, artifact_upload_id,
			finalization_failure_code, next_retry_at, job_fence, job_version, decided_at
		FROM execution_failure_decisions
		WHERE attempt_id = $2`,
		"55000",
		"execution_failure_decisions_profile_circuit_protocol",
		uuid.New(),
		assignments[0].AttemptID,
	)

	for _, relation := range []string{
		"profile_circuit_protocol_state",
		"profile_circuit_protocol_transitions",
		"profile_certification_circuit_openings",
	} {
		var rowSecurity, forceRowSecurity bool
		if err := fixture.database.Admin.QueryRow(`
			SELECT relrowsecurity, relforcerowsecurity
			FROM pg_class
			WHERE oid = $1::regclass
		`, relation).Scan(&rowSecurity, &forceRowSecurity); err != nil {
			t.Fatalf("read %s RLS flags: %v", relation, err)
		}
		if !rowSecurity || !forceRowSecurity {
			t.Fatalf("%s RLS flags = enabled %t forced %t", relation, rowSecurity, forceRowSecurity)
		}
	}

	for _, login := range []struct {
		name     string
		password string
	}{
		{name: "vela_request_login", password: "vela-request-password"},
		{name: "vela_auth_login", password: "vela-auth-password"},
		{name: "vela_cancel_login", password: "vela-cancel-password"},
		{name: "vela_artifact_request_login", password: "vela-artifact-request-password"},
		{name: "vela_scheduler_login", password: "vela-scheduler-password"},
		{name: "vela_billing_login", password: "vela-billing-password"},
		{name: "vela_internal_login", password: "vela-internal-password"},
	} {
		pool := newRolePool(t, fixture.database.DSN, login.name, login.password)
		var count int
		err := pool.QueryRow(
			context.Background(),
			"SELECT count(*) FROM profile_certification_circuit_openings",
		).Scan(&count)
		var postgresError *pgconn.PgError
		if login.name == "vela_internal_login" {
			if err != nil || count != 1 {
				t.Fatalf("internal circuit receipt read = %d error=%v", count, err)
			}
		} else if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
			t.Fatalf("%s circuit receipt read error = %v, want SQLSTATE 42501", login.name, err)
		}
		_, err = pool.Exec(
			context.Background(),
			"SELECT vela_transition_profile_circuit_protocol(false, 'unauthorized runtime transition')",
		)
		if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
			t.Fatalf("%s protocol transition error = %v, want SQLSTATE 42501", login.name, err)
		}
	}
}

func TestProfileCircuitProtocolTransitionHistoryIsContiguousAndImmutable(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)

	assertProtocolState := func(
		wantRequired bool,
		wantVersion int,
		wantReceipt sql.NullString,
		wantTransitioned bool,
	) {
		t.Helper()
		var (
			required     bool
			version      int
			receipt      sql.NullString
			transitioned sql.NullTime
		)
		if err := database.Admin.QueryRow(`
			SELECT require_circuit_aggregation, protocol_version,
				transition_receipt, transitioned_at
			FROM profile_circuit_protocol_state
			WHERE singleton
		`).Scan(&required, &version, &receipt, &transitioned); err != nil {
			t.Fatalf("read Profile circuit protocol state: %v", err)
		}
		if required != wantRequired || version != wantVersion || receipt != wantReceipt ||
			transitioned.Valid != wantTransitioned {
			t.Fatalf(
				"Profile circuit protocol = required %t version %d receipt %#v transitioned %t",
				required,
				version,
				receipt,
				transitioned.Valid,
			)
		}
	}
	assertProtocolError := func(label, statement, code, constraint string) {
		t.Helper()
		_, err := database.Admin.Exec(statement)
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != code ||
			(constraint != "" && postgresError.ConstraintName != constraint) {
			t.Fatalf("%s error = %v, want SQLSTATE %s constraint %s", label, err, code, constraint)
		}
	}

	assertProtocolState(false, 1, sql.NullString{}, false)
	assertProtocolError(
		"blank Profile circuit receipt",
		"SELECT vela_transition_profile_circuit_protocol(true, '   ')",
		"22023",
		"",
	)
	assertProtocolState(false, 1, sql.NullString{}, false)
	setProfileCircuitProtocolGate(t, database.Admin, true, "N-1 failure writers drained")
	assertProtocolState(
		true,
		2,
		sql.NullString{String: "N-1 failure writers drained", Valid: true},
		true,
	)
	assertProtocolError(
		"direct Profile circuit state update",
		`UPDATE profile_circuit_protocol_state
		 SET protocol_version = 3,
		     require_circuit_aggregation = false,
		     transition_receipt = 'forged state',
		     transitioned_at = clock_timestamp()`,
		"55000",
		"profile_circuit_protocol_state_transition_required",
	)
	assertProtocolError(
		"non-contiguous Profile circuit history",
		`INSERT INTO profile_circuit_protocol_transitions (
			protocol_version, require_circuit_aggregation, transition_receipt, transitioned_at
		 ) VALUES (4, false, 'skipped protocol version', clock_timestamp())`,
		"55000",
		"profile_circuit_protocol_history_contiguous",
	)
	setProfileCircuitProtocolGate(t, database.Admin, false, "operator verified N-1 binary rollback")
	assertProtocolState(
		false,
		3,
		sql.NullString{String: "operator verified N-1 binary rollback", Valid: true},
		true,
	)

	var versions, requirements, receipts string
	if err := database.Admin.QueryRow(`
		SELECT
			string_agg(protocol_version::text, ',' ORDER BY protocol_version),
			string_agg(require_circuit_aggregation::text, ',' ORDER BY protocol_version),
			string_agg(transition_receipt, ',' ORDER BY protocol_version)
		FROM profile_circuit_protocol_transitions
	`).Scan(&versions, &requirements, &receipts); err != nil {
		t.Fatalf("read Profile circuit protocol history: %v", err)
	}
	if versions != "2,3" || requirements != "true,false" ||
		receipts != "N-1 failure writers drained,operator verified N-1 binary rollback" {
		t.Fatalf(
			"Profile circuit history = versions %q requirements %q receipts %q",
			versions,
			requirements,
			receipts,
		)
	}
	for label, statement := range map[string]string{
		"history update":   "UPDATE profile_circuit_protocol_transitions SET transition_receipt = 'rewritten'",
		"history delete":   "DELETE FROM profile_circuit_protocol_transitions",
		"history truncate": "TRUNCATE profile_circuit_protocol_transitions",
	} {
		assertProtocolError(
			label,
			statement,
			"55000",
			"profile_circuit_protocol_history_immutable",
		)
	}
	for label, statement := range map[string]string{
		"state delete":   "DELETE FROM profile_circuit_protocol_state",
		"state truncate": "TRUNCATE profile_circuit_protocol_state",
	} {
		assertProtocolError(
			label,
			statement,
			"55000",
			"profile_circuit_protocol_state_required",
		)
	}
}

func TestProfileCircuitProtocolTransitionSerializesWithFail(t *testing.T) {
	fixture := newProfileCircuitFixture(t, 1, false)
	setProfileCircuitProtocolGate(
		t,
		fixture.database.Admin,
		false,
		"exercise N-1 failure writer coexistence",
	)
	_, assignment := fixture.assignAndStart(t, 0, "profile-circuit-protocol-serialization")

	const advisoryKey = int64(8675309)
	blocker, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Profile circuit writer blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	if _, err := blocker.Exec("SELECT pg_advisory_xact_lock($1)", advisoryKey); err != nil {
		t.Fatalf("hold Profile circuit writer advisory lock: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(fmt.Sprintf(`
		CREATE FUNCTION vela_test_pause_profile_circuit_writer() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock(%d);
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER vela_test_pause_profile_circuit_writer
		BEFORE INSERT ON execution_failure_decisions
		FOR EACH ROW EXECUTE FUNCTION vela_test_pause_profile_circuit_writer();
	`, advisoryKey)); err != nil {
		t.Fatalf("install Profile circuit writer pause: %v", err)
	}

	type failResult struct {
		decision workercontrol.RetryDecision
		err      error
	}
	failResults := make(chan failResult, 1)
	go func() {
		decision, failErr := fixture.service.Fail(
			context.Background(),
			fixture.workers[0],
			leaseCredentials(assignment),
			validFailureObservation(),
		)
		failResults <- failResult{decision: decision, err: failErr}
	}()
	waitForProfileCircuitRelationLock(
		t,
		fixture.database.Admin,
		"vela_internal_login",
		"RowExclusiveLock",
		true,
	)

	transitionResults := make(chan error, 1)
	go func() {
		_, transitionErr := fixture.database.Admin.Exec(
			"SELECT vela_transition_profile_circuit_protocol(true, 'in-flight N-1 writer serialized')",
		)
		transitionResults <- transitionErr
	}()
	waitForProfileCircuitRelationLock(
		t,
		fixture.database.Admin,
		"postgres",
		"ShareRowExclusiveLock",
		false,
	)
	select {
	case transitionErr := <-transitionResults:
		t.Fatalf("Profile circuit transition bypassed in-flight Fail: %v", transitionErr)
	default:
	}
	if err := blocker.Rollback(); err != nil {
		t.Fatalf("release Profile circuit writer pause: %v", err)
	}

	result := <-failResults
	if result.err != nil || result.decision.Disposition != workercontrol.RetryDispositionRetryWait {
		t.Fatalf("serialized gate-off Fail = %#v error=%v", result.decision, result.err)
	}
	if err := <-transitionResults; err != nil {
		t.Fatalf("enable Profile circuit protocol after in-flight Fail: %v", err)
	}
	var decisionProtocol, stateProtocol int
	var required bool
	var receipt string
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			decision.circuit_protocol_version,
			state.protocol_version,
			state.require_circuit_aggregation,
			state.transition_receipt
		FROM execution_failure_decisions AS decision
		CROSS JOIN profile_circuit_protocol_state AS state
		WHERE decision.attempt_id = $1 AND state.singleton
	`, assignment.AttemptID).Scan(
		&decisionProtocol,
		&stateProtocol,
		&required,
		&receipt,
	); err != nil {
		t.Fatalf("read serialized Profile circuit protocol result: %v", err)
	}
	if decisionProtocol != 1 || stateProtocol != 4 || !required ||
		receipt != "in-flight N-1 writer serialized" {
		t.Fatalf(
			"serialized Profile circuit result = decision protocol %d state %d required %t receipt %q",
			decisionProtocol,
			stateProtocol,
			required,
			receipt,
		)
	}
}

func TestProfileCircuitMigrationEmptyDownUpRestoresDefaultSurface(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 9); err != nil {
		t.Fatalf("contract empty Profile circuit migration: %v", err)
	}
	for _, table := range []string{
		"profile_circuit_protocol_state",
		"profile_circuit_protocol_transitions",
		"profile_certification_circuit_openings",
	} {
		assertTableDoesNotExist(t, database.Admin, table)
	}
	for _, column := range []struct {
		table string
		name  string
	}{
		{table: "service_class_revisions", name: "circuit_fingerprint_window_seconds"},
		{table: "service_class_revisions", name: "circuit_min_distinct_healthy_workers"},
		{table: "jobs", name: "execution_circuit_fingerprint_window_seconds"},
		{table: "jobs", name: "execution_circuit_min_distinct_healthy_workers"},
		{table: "attempts", name: "profile_certification_id"},
		{table: "execution_failure_decisions", name: "circuit_protocol_version"},
		{table: "execution_failure_decisions", name: "worker_was_healthy"},
	} {
		var exists bool
		if err := database.Admin.QueryRow(`
			SELECT EXISTS (
				SELECT 1
				FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2
			)
		`, column.table, column.name).Scan(&exists); err != nil {
			t.Fatalf("inspect contracted %s.%s: %v", column.table, column.name, err)
		}
		if exists {
			t.Fatalf("contracted Profile circuit column remains: %s.%s", column.table, column.name)
		}
	}
	if err := goose.Up(database.Admin, migrations); err != nil {
		t.Fatalf("re-expand empty Profile circuit migration: %v", err)
	}
	for _, table := range []string{
		"profile_circuit_protocol_state",
		"profile_circuit_protocol_transitions",
		"profile_certification_circuit_openings",
	} {
		assertTableExists(t, database.Admin, table)
	}
	var window, threshold, protocol int
	var required bool
	if err := database.Admin.QueryRow(`
		SELECT
			revision.circuit_fingerprint_window_seconds,
			revision.circuit_min_distinct_healthy_workers,
			state.protocol_version,
			state.require_circuit_aggregation
		FROM service_class_revisions AS revision
		CROSS JOIN profile_circuit_protocol_state AS state
		WHERE revision.id = '00000000-0000-0000-0000-000000000012'
		  AND state.singleton
	`).Scan(&window, &threshold, &protocol, &required); err != nil {
		t.Fatalf("read re-expanded Profile circuit defaults: %v", err)
	}
	if window != 3600 || threshold != 2 || protocol != 1 || required {
		t.Fatalf(
			"re-expanded Profile circuit defaults = window %d threshold %d protocol %d required %t",
			window,
			threshold,
			protocol,
			required,
		)
	}
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "profile-circuit-down-up-admission", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"prove Profile circuit migration Down Up Admission"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("re-expanded Profile circuit Admission status = %d; body=%s", accepted.StatusCode, accepted.Body)
	}
	var response jobResponse
	if err := json.Unmarshal(accepted.Body, &response); err != nil {
		t.Fatalf("decode re-expanded Profile circuit Admission: %v", err)
	}
	if err := database.Admin.QueryRow(`
		SELECT
			execution_circuit_fingerprint_window_seconds,
			execution_circuit_min_distinct_healthy_workers
		FROM jobs
		WHERE id = $1
	`, response.JobID).Scan(&window, &threshold); err != nil {
		t.Fatalf("read re-expanded Job circuit snapshot: %v", err)
	}
	if window != 3600 || threshold != 2 {
		t.Fatalf("re-expanded Job circuit snapshot = window %d threshold %d", window, threshold)
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 16 {
		t.Fatalf("Profile circuit migration version after Down/Up = %d error=%v", version, err)
	}
}

func TestProfileCircuitMigrationDownRefusesPreservedEvidence(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*testing.T, testDatabase)
	}{
		{
			name: "protocol transition",
			prepare: func(t *testing.T, database testDatabase) {
				setProfileCircuitProtocolGate(t, database.Admin, true, "preserve protocol transition receipt")
			},
		},
		{
			name: "custom ServiceClassRevision policy",
			prepare: func(t *testing.T, database testDatabase) {
				seedAdmissionFixture(t, database.Admin)
				tx, err := database.Admin.Begin()
				if err != nil {
					t.Fatalf("begin custom ServiceClassRevision policy fixture: %v", err)
				}
				defer func() { _ = tx.Rollback() }()
				if _, err := tx.Exec("SET LOCAL session_replication_role = replica"); err != nil {
					t.Fatalf("disable revision immutability for custom circuit policy: %v", err)
				}
				if _, err := tx.Exec(`
					UPDATE service_class_revisions
					SET circuit_fingerprint_window_seconds = 120
					WHERE id = '00000000-0000-0000-0000-000000000012'
				`); err != nil {
					t.Fatalf("set custom ServiceClassRevision circuit policy: %v", err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatalf("commit custom ServiceClassRevision circuit policy: %v", err)
				}
			},
		},
		{
			name: "custom Job policy",
			prepare: func(t *testing.T, database testDatabase) {
				seedAdmissionFixture(t, database.Admin)
				server := admissionServerForDatabase(t, database)
				accepted := submitJob(t, server.URL, "profile-circuit-custom-job-policy", []byte(`{
					"model":"minimax-h3",
					"generation_preset":"balanced",
					"service_class":"standard",
					"output_spec":"video-1080p-5s-24fps",
					"generation_count":1,
					"prompt":"preserve custom Job circuit policy"
				}`))
				if accepted.StatusCode != 202 {
					t.Fatalf("custom Job policy Admission status = %d; body=%s", accepted.StatusCode, accepted.Body)
				}
				var response jobResponse
				if err := json.Unmarshal(accepted.Body, &response); err != nil {
					t.Fatalf("decode custom Job policy Admission: %v", err)
				}
				forceJobPolicySnapshot(
					t,
					database.Admin,
					uuid.MustParse(response.JobID),
					"execution_circuit_min_distinct_healthy_workers = 3",
				)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database := newPostgres(t)
			applyFoundation(t, database.Admin)
			test.prepare(t, database)
			migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
			err := goose.DownTo(database.Admin, migrations, 9)
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
				postgresError.ConstraintName !=
					"profile_certification_circuit_contract_requires_preserved_evidence" {
				t.Fatalf("Profile circuit migration Down with %s error = %v", test.name, err)
			}
			version, versionErr := goose.GetDBVersion(database.Admin)
			if versionErr != nil || version != 10 {
				t.Fatalf("migration version after refused %s Down = %d error=%v", test.name, version, versionErr)
			}
			assertTableExists(t, database.Admin, "profile_circuit_protocol_state")
		})
	}
}

func TestProfileCircuitMigrationDownRefusesOpeningReceipt(t *testing.T) {
	fixture := newProfileCircuitFixture(t, 2, true)
	for index := range 2 {
		_, assignment := fixture.assignAndStart(
			t,
			index,
			fmt.Sprintf("profile-circuit-down-opening-%d", index),
		)
		if _, err := fixture.service.Fail(
			context.Background(),
			fixture.workers[index],
			leaseCredentials(assignment),
			validFailureObservation(),
		); err != nil {
			t.Fatalf("create durable Profile circuit opening: %v", err)
		}
	}
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err := goose.DownTo(fixture.database.Admin, migrations, 9)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
		postgresError.ConstraintName !=
			"profile_certification_circuit_contract_requires_preserved_evidence" {
		t.Fatalf("Profile circuit migration Down with opening receipt error = %v", err)
	}
	version, versionErr := goose.GetDBVersion(fixture.database.Admin)
	if versionErr != nil || version != 10 {
		t.Fatalf("migration version after opening Down refusal = %d error=%v", version, versionErr)
	}
}

func waitForProfileCircuitRelationLock(
	t *testing.T,
	database *sql.DB,
	user string,
	mode string,
	granted bool,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var found bool
		if err := database.QueryRow(`
			SELECT EXISTS (
				SELECT 1
				FROM pg_locks AS lock
				JOIN pg_class AS relation ON relation.oid = lock.relation
				JOIN pg_stat_activity AS activity ON activity.pid = lock.pid
				WHERE relation.relname = 'execution_failure_decisions'
				  AND activity.usename = $1
				  AND lock.mode = $2
				  AND lock.granted = $3
			)
		`, user, mode, granted).Scan(&found); err != nil {
			t.Fatalf("inspect Profile circuit relation lock: %v", err)
		}
		if found {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s %s granted=%t", user, mode, granted)
}

type profileCircuitRollbackState struct {
	CertificationState string
	InvalidatedAt      sql.NullTime
	OpeningCount       int
	AttemptState       string
	AttemptEndedAt     sql.NullTime
	LeaseRevokedAt     sql.NullTime
	JobState           string
	JobVersion         int64
	JobFence           int64
	RuntimeVersion     int64
	CircuitState       string
	ProjectQueued      int
	ProjectRetryWait   int
	ProjectRunning     int
	PoolQueued         int
	PoolRetryWait      int
	ReservationState   string
	ReservedMinor      int64
	WorkerState        string
	WorkerReachability string
	DecisionCount      int
	OutboxCount        int
}

func readProfileCircuitRollbackState(
	t *testing.T,
	database *sql.DB,
	certificationID uuid.UUID,
	assignment workercontrol.Assignment,
	workerID uuid.UUID,
) profileCircuitRollbackState {
	t.Helper()
	var state profileCircuitRollbackState
	if err := database.QueryRow(`
		SELECT
			certification.state::text,
			certification.invalidated_at,
			(SELECT count(*) FROM profile_certification_circuit_openings
			 WHERE profile_certification_id = certification.id),
			attempt.state::text,
			attempt.ended_at,
			lease.revoked_at,
			job.state::text,
			job.version,
			job.current_fence,
			runtime.version,
			runtime.circuit_breaker_state::text,
			project.queued_count,
			project.retry_wait_count,
			project.running_count,
			pool.queued_count,
			pool.retry_wait_count,
			reservation.state::text,
			credit.reserved_minor,
			worker.lifecycle_state::text,
			worker.reachability_condition::text,
			(SELECT count(*) FROM execution_failure_decisions WHERE attempt_id = attempt.id),
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id)
		FROM profile_certifications AS certification
		JOIN attempts AS attempt ON attempt.id = $2
		JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id
		JOIN jobs AS job ON job.id = attempt.job_id
		JOIN retry_runtime_states AS runtime ON runtime.job_id = job.id
		JOIN projects AS project ON project.id = job.project_id
		JOIN worker_pools AS pool ON pool.id = job.worker_pool_id
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN organization_credit_accounts AS credit ON credit.organization_id = job.organization_id
		JOIN workers AS worker ON worker.id = $3
		WHERE certification.id = $1
	`, certificationID, assignment.AttemptID, workerID).Scan(
		&state.CertificationState,
		&state.InvalidatedAt,
		&state.OpeningCount,
		&state.AttemptState,
		&state.AttemptEndedAt,
		&state.LeaseRevokedAt,
		&state.JobState,
		&state.JobVersion,
		&state.JobFence,
		&state.RuntimeVersion,
		&state.CircuitState,
		&state.ProjectQueued,
		&state.ProjectRetryWait,
		&state.ProjectRunning,
		&state.PoolQueued,
		&state.PoolRetryWait,
		&state.ReservationState,
		&state.ReservedMinor,
		&state.WorkerState,
		&state.WorkerReachability,
		&state.DecisionCount,
		&state.OutboxCount,
	); err != nil {
		t.Fatalf("read ProfileCertification circuit rollback state: %v", err)
	}
	return state
}

type profileCircuitFixture struct {
	database                 testDatabase
	service                  *workercontrol.Service
	serverURL                string
	workers                  []workercontrol.AuthenticatedWorker
	primaryProfileID         uuid.UUID
	primaryCertificationID   uuid.UUID
	alternateProfileID       uuid.UUID
	alternateCertificationID uuid.UUID
}

type profileCircuitOpeningCoordinator struct {
	delegate *workercontrol.Service
	open     func() error
	once     sync.Once
	openErr  error
}

func seedProfileCircuitSecondProject(t *testing.T, database *sql.DB) {
	t.Helper()
	if _, err := database.Exec(`
		INSERT INTO projects (
			id, organization_id, display_name, queued_limit, running_limit
		) VALUES ($1, $2, 'Profile Circuit Project Two', 10, 2)
	`, testProjectTwoID, testOrganizationID); err != nil {
		t.Fatalf("seed cross-Project circuit Project: %v", err)
	}
	if _, err := database.Exec(`
		INSERT INTO principals (id, organization_id, kind, display_name)
		VALUES ($1, $2, 'SERVICE', 'Profile Circuit Principal Two')
	`, testPrincipalTwoID, testOrganizationID); err != nil {
		t.Fatalf("seed cross-Project circuit Principal: %v", err)
	}
	if _, err := database.Exec(`
		INSERT INTO service_principals (principal_id, organization_id, project_id)
		VALUES ($1, $2, $3)
	`, testPrincipalTwoID, testOrganizationID, testProjectTwoID); err != nil {
		t.Fatalf("seed cross-Project circuit Service Principal: %v", err)
	}
	if _, err := database.Exec(`
		INSERT INTO credentials (
			id, organization_id, project_id, principal_id, secret_digest, scopes, expires_at,
			created_by_principal_id
		) VALUES (
			$1, $2, $3, $4, $5,
			ARRAY['jobs:submit', 'jobs:read'], clock_timestamp() + interval '1 day', $4
		)
	`,
		testCredentialTwoID,
		testOrganizationID,
		testProjectTwoID,
		testPrincipalTwoID,
		credentialDigest([]byte(testCredentialTwoSecret)),
	); err != nil {
		t.Fatalf("seed cross-Project circuit Credential: %v", err)
	}
}

func (coordinator *profileCircuitOpeningCoordinator) Acquire(
	ctx context.Context,
	worker workercontrol.AuthenticatedWorker,
	workerEpoch int64,
	candidate *workercontrol.AssignmentCandidate,
) (workercontrol.Assignment, error) {
	coordinator.once.Do(func() {
		coordinator.openErr = coordinator.open()
	})
	if coordinator.openErr != nil {
		return workercontrol.Assignment{}, coordinator.openErr
	}
	return coordinator.delegate.Acquire(ctx, worker, workerEpoch, candidate)
}

func newProfileCircuitFixture(t *testing.T, workerCount int, addAlternate bool) profileCircuitFixture {
	t.Helper()
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "profile circuit fixture")
	setProfileCircuitProtocolGate(t, database.Admin, true, "N-1 failure writers drained for circuit fixture")
	seedAdmissionFixture(t, database.Admin)

	fixture := profileCircuitFixture{
		database:                 database,
		serverURL:                admissionServerForDatabase(t, database).URL,
		primaryProfileID:         uuid.MustParse("00000000-0000-0000-0000-000000000014"),
		primaryCertificationID:   uuid.MustParse("00000000-0000-0000-0000-000000000015"),
		alternateProfileID:       uuid.New(),
		alternateCertificationID: uuid.New(),
	}
	if addAlternate {
		if _, err := database.Admin.Exec(`
			INSERT INTO execution_profile_revisions (
				id, model_revision_id, worker_pool_id, stable_id, revision, state
			) VALUES (
				$1,
				'00000000-0000-0000-0000-000000000010',
				'00000000-0000-0000-0000-000000000005',
				$2, 1, 'ACTIVE'
			)
		`, fixture.alternateProfileID, "profile-circuit-alternate-"+fixture.alternateProfileID.String()); err != nil {
			t.Fatalf("seed alternate circuit profile: %v", err)
		}
		if _, err := database.Admin.Exec(`
			INSERT INTO profile_certifications (
				id, model_revision_id, generation_preset_revision_id, output_spec_id,
				execution_profile_revision_id, state, evidence_digest, certified_at
			) VALUES (
				$2,
				'00000000-0000-0000-0000-000000000010',
				'00000000-0000-0000-0000-000000000011',
				'00000000-0000-0000-0000-000000000013',
				$1, 'ACTIVE', 'profile-circuit-alternate-evidence', clock_timestamp()
			)
		`, fixture.alternateProfileID, fixture.alternateCertificationID); err != nil {
			t.Fatalf("seed alternate circuit certification: %v", err)
		}
	}

	fixture.workers = make([]workercontrol.AuthenticatedWorker, 0, workerCount)
	for index := range workerCount {
		workerID := uuid.New()
		if _, err := database.Admin.Exec(`
			INSERT INTO workers (
				id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
			) VALUES (
				$1, '00000000-0000-0000-0000-000000000005', $2, 7, 'READY', 'HEALTHY'
			)
		`, workerID, fmt.Sprintf("spiffe://vela.internal/worker/profile-circuit-%d-%s", index, workerID)); err != nil {
			t.Fatalf("seed profile circuit Worker %d: %v", index, err)
		}
		fixture.workers = append(fixture.workers, workercontrol.AuthenticatedWorker{ID: workerID})
	}
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	service, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create profile circuit Worker coordinator: %v", err)
	}
	fixture.service = service
	return fixture
}

func (fixture profileCircuitFixture) assignAndStart(
	t *testing.T,
	workerIndex int,
	idempotencyKey string,
) (uuid.UUID, workercontrol.Assignment) {
	return fixture.assignAndStartOnProfile(t, workerIndex, idempotencyKey, fixture.primaryProfileID)
}

func (fixture profileCircuitFixture) assignAndStartOnProfile(
	t *testing.T,
	workerIndex int,
	idempotencyKey string,
	profileID uuid.UUID,
) (uuid.UUID, workercontrol.Assignment) {
	jobID, assignment := fixture.assignOnProfile(t, workerIndex, idempotencyKey, profileID)
	started, err := fixture.service.Start(
		context.Background(),
		fixture.workers[workerIndex],
		leaseCredentials(assignment),
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start circuit Attempt = %#v error=%v", started, err)
	}
	return jobID, assignment
}

func (fixture profileCircuitFixture) assign(
	t *testing.T,
	workerIndex int,
	idempotencyKey string,
) (uuid.UUID, workercontrol.Assignment) {
	return fixture.assignOnProfile(t, workerIndex, idempotencyKey, fixture.primaryProfileID)
}

func (fixture profileCircuitFixture) assignOnProfile(
	t *testing.T,
	workerIndex int,
	idempotencyKey string,
	profileID uuid.UUID,
) (uuid.UUID, workercontrol.Assignment) {
	t.Helper()
	accepted := submitJob(t, fixture.serverURL, idempotencyKey, []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"exercise ProfileCertification circuit behavior"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit circuit Job status = %d; body=%s", accepted.StatusCode, accepted.Body)
	}
	var response jobResponse
	if err := json.Unmarshal(accepted.Body, &response); err != nil {
		t.Fatalf("decode circuit Job: %v", err)
	}
	jobID := uuid.MustParse(response.JobID)
	forceJobPolicySnapshot(
		t,
		fixture.database.Admin,
		jobID,
		`execution_retry_backoff_policy = '{"kind":"exponential","initial_seconds":1,"max_seconds":1}'::jsonb`,
	)
	assignment, err := fixture.service.Acquire(
		context.Background(),
		fixture.workers[workerIndex],
		7,
		&workercontrol.AssignmentCandidate{
			JobID:                      jobID,
			ExpectedJobVersion:         1,
			ExecutionProfileRevisionID: profileID,
		},
	)
	if err != nil {
		t.Fatalf("Acquire circuit Attempt: %v", err)
	}
	return jobID, assignment
}

func assertProfileCircuitClosed(t *testing.T, database *sql.DB, certificationID uuid.UUID) {
	t.Helper()
	var state string
	var invalidatedAt sql.NullTime
	var openings int
	if err := database.QueryRow(`
		SELECT
			state::text,
			invalidated_at,
			(SELECT count(*) FROM profile_certification_circuit_openings
			 WHERE profile_certification_id = $1)
		FROM profile_certifications
		WHERE id = $1
	`, certificationID).Scan(&state, &invalidatedAt, &openings); err != nil {
		t.Fatalf("read ProfileCertification circuit state: %v", err)
	}
	if state != "ACTIVE" || invalidatedAt.Valid || openings != 0 {
		t.Fatalf(
			"ProfileCertification circuit unexpectedly opened: state=%s invalidated=%v openings=%d",
			state,
			invalidatedAt,
			openings,
		)
	}
}

func setProfileCircuitProtocolGate(t *testing.T, database *sql.DB, enabled bool, receipt string) {
	t.Helper()
	result, err := database.Exec(
		"SELECT vela_transition_profile_circuit_protocol($1, $2)",
		enabled,
		receipt,
	)
	if err != nil {
		t.Fatalf("set ProfileCertification circuit protocol gate to %t: %v", enabled, err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("ProfileCertification circuit gate rows = %d error=%v", rows, rowsErr)
	}
}
