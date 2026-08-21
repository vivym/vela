//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestStartRejectsInvalidAuthorityWithoutSideEffects(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *startFixture, *workercontrol.AuthenticatedWorker, *workercontrol.LeaseCredentials)
	}{
		{
			name: "different authenticated Worker",
			mutate: func(t *testing.T, fixture *startFixture, worker *workercontrol.AuthenticatedWorker, _ *workercontrol.LeaseCredentials) {
				t.Helper()
				worker.ID = uuid.New()
				if _, err := fixture.database.Admin.Exec(`
					INSERT INTO workers (
						id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
					) VALUES (
						$1, '00000000-0000-0000-0000-000000000005', $2, 7, 'READY', 'HEALTHY'
					)
				`, worker.ID, "spiffe://vela.internal/worker/"+worker.ID.String()); err != nil {
					t.Fatalf("seed different Worker: %v", err)
				}
			},
		},
		{
			name: "stale Worker epoch",
			mutate: func(_ *testing.T, _ *startFixture, _ *workercontrol.AuthenticatedWorker, credentials *workercontrol.LeaseCredentials) {
				credentials.WorkerEpoch++
			},
		},
		{
			name: "different Attempt",
			mutate: func(_ *testing.T, _ *startFixture, _ *workercontrol.AuthenticatedWorker, credentials *workercontrol.LeaseCredentials) {
				credentials.AttemptID = uuid.New()
			},
		},
		{
			name: "stale fence",
			mutate: func(_ *testing.T, _ *startFixture, _ *workercontrol.AuthenticatedWorker, credentials *workercontrol.LeaseCredentials) {
				credentials.Fence++
			},
		},
		{
			name: "incorrect opaque token",
			mutate: func(_ *testing.T, _ *startFixture, _ *workercontrol.AuthenticatedWorker, credentials *workercontrol.LeaseCredentials) {
				credentials.Token = "not-the-issued-lease-token"
			},
		},
		{
			name: "revoked Lease",
			mutate: func(t *testing.T, fixture *startFixture, _ *workercontrol.AuthenticatedWorker, _ *workercontrol.LeaseCredentials) {
				t.Helper()
				if _, err := fixture.database.Admin.Exec(`
					UPDATE attempt_leases
					SET revoked_at = clock_timestamp()
					WHERE attempt_id = $1
				`, fixture.assignment.AttemptID); err != nil {
					t.Fatalf("revoke Lease: %v", err)
				}
			},
		},
		{
			name: "FINALIZATION Lease",
			mutate: func(t *testing.T, fixture *startFixture, _ *workercontrol.AuthenticatedWorker, _ *workercontrol.LeaseCredentials) {
				t.Helper()
				if _, err := fixture.database.Admin.Exec(`
					UPDATE attempt_leases
					SET phase = 'FINALIZATION'
					WHERE attempt_id = $1
				`, fixture.assignment.AttemptID); err != nil {
					t.Fatalf("change Lease to FINALIZATION: %v", err)
				}
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newStartFixture(t, fmt.Sprintf("invalid-start-authority-%d", index), 7)
			worker := fixture.worker
			credentials := fixture.credentials
			test.mutate(t, &fixture, &worker, &credentials)
			before := readStartState(t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID)

			result, err := fixture.service.Start(context.Background(), worker, credentials)
			if err != nil {
				t.Fatalf("Start with invalid authority: %v", err)
			}
			if result.Decision != workercontrol.Stop || result.StopReason != workercontrol.StopInvalidAuthority {
				t.Fatalf("invalid authority result = %#v", result)
			}
			after := readStartState(t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("invalid Start mutated state: before=%#v after=%#v", before, after)
			}
		})
	}
}

func TestStartStopsWhenAssignmentIsNoLongerStartable(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *startFixture)
	}{
		{
			name: "Job is terminal",
			mutate: func(t *testing.T, fixture *startFixture) {
				t.Helper()
				if _, err := fixture.database.Admin.Exec(`
					UPDATE jobs
					SET state = 'CANCELED', version = version + 1, updated_at = clock_timestamp()
					WHERE id = $1
				`, fixture.assignment.JobID); err != nil {
					t.Fatalf("cancel Job fixture: %v", err)
				}
			},
		},
		{
			name: "Attempt is terminal",
			mutate: func(t *testing.T, fixture *startFixture) {
				t.Helper()
				if _, err := fixture.database.Admin.Exec(`
					UPDATE attempts
					SET state = 'FAILED', ended_at = clock_timestamp(), updated_at = clock_timestamp()
					WHERE id = $1
				`, fixture.assignment.AttemptID); err != nil {
					t.Fatalf("fail Attempt fixture: %v", err)
				}
			},
		},
		{
			name: "Credit Reservation was released",
			mutate: func(t *testing.T, fixture *startFixture) {
				t.Helper()
				if _, err := fixture.database.Admin.Exec(`
					UPDATE credit_reservations
					SET state = 'RELEASED', updated_at = clock_timestamp()
					WHERE job_id = $1
				`, fixture.assignment.JobID); err != nil {
					t.Fatalf("release Credit Reservation fixture: %v", err)
				}
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newStartFixture(t, fmt.Sprintf("not-startable-%d", index), 7)
			test.mutate(t, &fixture)
			before := readStartState(t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID)

			result, err := fixture.service.Start(context.Background(), fixture.worker, fixture.credentials)
			if err != nil {
				t.Fatalf("Start stale Assignment: %v", err)
			}
			if result.Decision != workercontrol.Stop || result.StopReason != workercontrol.StopNotStartable {
				t.Fatalf("stale Assignment result = %#v", result)
			}
			after := readStartState(t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("stale Start mutated state: before=%#v after=%#v", before, after)
			}
		})
	}
}

func TestStartRejectsExpiredLeaseAndJobWithoutSideEffects(t *testing.T) {
	tests := []struct {
		name       string
		stopReason workercontrol.StopReason
		expire     func(*testing.T, *startFixture)
	}{
		{
			name:       "Lease",
			stopReason: workercontrol.StopLeaseExpired,
			expire: func(t *testing.T, fixture *startFixture) {
				t.Helper()
				if _, err := fixture.database.Admin.Exec(`
					UPDATE attempt_leases
					SET expires_at = issued_at + interval '1 microsecond'
					WHERE attempt_id = $1
				`, fixture.assignment.AttemptID); err != nil {
					t.Fatalf("expire Lease: %v", err)
				}
			},
		},
		{
			name:       "Job",
			stopReason: workercontrol.StopJobExpired,
			expire: func(t *testing.T, fixture *startFixture) {
				t.Helper()
				tx, err := fixture.database.Admin.Begin()
				if err != nil {
					t.Fatalf("begin expired Job fixture: %v", err)
				}
				defer func() { _ = tx.Rollback() }()
				if _, err := tx.Exec("SET LOCAL session_replication_role = replica"); err != nil {
					t.Fatalf("disable immutable snapshot trigger: %v", err)
				}
				if _, err := tx.Exec(`
					UPDATE jobs
					SET job_expires_at = created_at + interval '1 microsecond'
					WHERE id = $1
				`, fixture.assignment.JobID); err != nil {
					t.Fatalf("expire Job: %v", err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatalf("commit expired Job fixture: %v", err)
				}
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newStartFixture(t, fmt.Sprintf("expired-start-%d", index), 7)
			test.expire(t, &fixture)
			before := readStartState(t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID)

			result, err := fixture.service.Start(context.Background(), fixture.worker, fixture.credentials)
			if err != nil {
				t.Fatalf("Start expired %s: %v", test.name, err)
			}
			if result.Decision != workercontrol.Stop || result.StopReason != test.stopReason {
				t.Fatalf("expired %s result = %#v", test.name, result)
			}
			after := readStartState(t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("expired Start mutated state: before=%#v after=%#v", before, after)
			}
		})
	}
}

func TestStartRechecksLeaseExpiryAfterRowLockWait(t *testing.T) {
	fixture := newAssignmentFixture(t, "start-lease-expiry-lock-wait", 7)
	internalPool := newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password")
	service, err := workercontrol.NewService(context.Background(), internalPool, workercontrol.Config{
		LeaseTTL:         time.Second,
		ActiveLeaseKeyID: "lease-key-v1",
		LeaseKeys: map[string][]byte{
			"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
		},
	})
	if err != nil {
		t.Fatalf("create short-Lease Worker coordinator: %v", err)
	}
	assignment, err := service.Acquire(context.Background(), fixture.worker, 7, &fixture.candidate)
	if err != nil {
		t.Fatalf("create short Assignment: %v", err)
	}
	credentials := leaseCredentials(assignment)
	before := readStartState(t, fixture.database.Admin, assignment.JobID, assignment.AttemptID)

	holder, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Lease lock holder: %v", err)
	}
	defer func() { _ = holder.Rollback() }()
	if _, err := holder.Exec(`
		SELECT id FROM attempt_leases WHERE attempt_id = $1 FOR UPDATE
	`, assignment.AttemptID); err != nil {
		t.Fatalf("lock Lease: %v", err)
	}

	result := make(chan startCallResult, 1)
	go func() {
		started, startErr := service.Start(context.Background(), fixture.worker, credentials)
		result <- startCallResult{result: started, err: startErr}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_internal_login")
	waitForDatabaseTimeAfter(t, fixture.database.Admin, assignment.LeaseExpiresAt)
	if err := holder.Commit(); err != nil {
		t.Fatalf("release expired Lease row: %v", err)
	}

	call := <-result
	if call.err != nil || call.result.Decision != workercontrol.Stop ||
		call.result.StopReason != workercontrol.StopLeaseExpired {
		t.Fatalf("post-lock Lease expiry result = %#v error=%v", call.result, call.err)
	}
	after := readStartState(t, fixture.database.Admin, assignment.JobID, assignment.AttemptID)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("post-lock Lease expiry mutated state: before=%#v after=%#v", before, after)
	}
}

func TestStartRechecksJobExpiryAfterRowLockWait(t *testing.T) {
	fixture := newStartFixture(t, "start-job-expiry-lock-wait", 7)
	before := readStartState(t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID)
	holder, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Job lock holder: %v", err)
	}
	defer func() { _ = holder.Rollback() }()
	if _, err := holder.Exec("SET LOCAL session_replication_role = replica"); err != nil {
		t.Fatalf("disable immutable snapshot trigger: %v", err)
	}
	var expiresAt time.Time
	if err := holder.QueryRow(`
		UPDATE jobs
		SET job_expires_at = clock_timestamp() + interval '1 second'
		WHERE id = $1
		RETURNING job_expires_at
	`, fixture.assignment.JobID).Scan(&expiresAt); err != nil {
		t.Fatalf("set near Job Expiry: %v", err)
	}

	result := make(chan startCallResult, 1)
	go func() {
		started, startErr := fixture.service.Start(
			context.Background(),
			fixture.worker,
			fixture.credentials,
		)
		result <- startCallResult{result: started, err: startErr}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_internal_login")
	waitForDatabaseTimeAfter(t, fixture.database.Admin, expiresAt)
	if err := holder.Commit(); err != nil {
		t.Fatalf("release expired Job row: %v", err)
	}

	call := <-result
	if call.err != nil || call.result.Decision != workercontrol.Stop ||
		call.result.StopReason != workercontrol.StopJobExpired {
		t.Fatalf("post-lock Job expiry result = %#v error=%v", call.result, call.err)
	}
	after := readStartState(t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("post-lock Job expiry mutated state: before=%#v after=%#v", before, after)
	}
}

func TestStartRollsBackWhenAuthorityExpiresDuringOutboxWrite(t *testing.T) {
	tests := []struct {
		name       string
		stopReason workercontrol.StopReason
		prepare    func(*testing.T, assignmentFixture) (*workercontrol.Service, workercontrol.Assignment, time.Time)
	}{
		{
			name:       "Lease",
			stopReason: workercontrol.StopLeaseExpired,
			prepare: func(t *testing.T, fixture assignmentFixture) (*workercontrol.Service, workercontrol.Assignment, time.Time) {
				t.Helper()
				pool := newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password")
				service, err := workercontrol.NewService(context.Background(), pool, workercontrol.Config{
					LeaseTTL:         3 * time.Second,
					ActiveLeaseKeyID: "lease-key-v1",
					LeaseKeys: map[string][]byte{
						"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
					},
				})
				if err != nil {
					t.Fatalf("create short-Lease Worker coordinator: %v", err)
				}
				assignment, err := service.Acquire(context.Background(), fixture.worker, 7, &fixture.candidate)
				if err != nil {
					t.Fatalf("create short Assignment: %v", err)
				}
				return service, assignment, assignment.LeaseExpiresAt
			},
		},
		{
			name:       "Job",
			stopReason: workercontrol.StopJobExpired,
			prepare: func(t *testing.T, fixture assignmentFixture) (*workercontrol.Service, workercontrol.Assignment, time.Time) {
				t.Helper()
				assignment, err := fixture.service.Acquire(context.Background(), fixture.worker, 7, &fixture.candidate)
				if err != nil {
					t.Fatalf("create Assignment: %v", err)
				}
				tx, err := fixture.database.Admin.Begin()
				if err != nil {
					t.Fatalf("begin near-expiry Job fixture: %v", err)
				}
				defer func() { _ = tx.Rollback() }()
				if _, err := tx.Exec("SET LOCAL session_replication_role = replica"); err != nil {
					t.Fatalf("disable immutable snapshot trigger: %v", err)
				}
				var expiresAt time.Time
				if err := tx.QueryRow(`
					UPDATE jobs
					SET job_expires_at = clock_timestamp() + interval '3 seconds'
					WHERE id = $1
					RETURNING job_expires_at
				`, assignment.JobID).Scan(&expiresAt); err != nil {
					t.Fatalf("set near Job Expiry: %v", err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatalf("commit near Job Expiry: %v", err)
				}
				return fixture.service, assignment, expiresAt
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAssignmentFixture(t, fmt.Sprintf("start-outbox-expiry-%d", index), 7)
			service, assignment, expiresAt := test.prepare(t, fixture)
			credentials := leaseCredentials(assignment)
			if _, err := fixture.database.Admin.Exec(`
				CREATE FUNCTION delay_start_outbox_past_expiry() RETURNS trigger
				LANGUAGE plpgsql
				AS $$
				BEGIN
					PERFORM pg_advisory_xact_lock(24681357);
					RETURN NEW;
				END
				$$;
				CREATE TRIGGER delay_start_outbox
				BEFORE INSERT ON outbox_events
				FOR EACH ROW
				WHEN (NEW.event_type = 'job.started')
				EXECUTE FUNCTION delay_start_outbox_past_expiry();
			`); err != nil {
				t.Fatalf("create delayed Start Outbox trigger: %v", err)
			}
			latch, err := fixture.database.Admin.Begin()
			if err != nil {
				t.Fatalf("begin Outbox latch: %v", err)
			}
			defer func() { _ = latch.Rollback() }()
			if _, err := latch.Exec("SELECT pg_advisory_xact_lock(24681357)"); err != nil {
				t.Fatalf("acquire Outbox latch: %v", err)
			}
			before := readStartState(t, fixture.database.Admin, assignment.JobID, assignment.AttemptID)

			result := make(chan startCallResult, 1)
			go func() {
				started, startErr := service.Start(context.Background(), fixture.worker, credentials)
				result <- startCallResult{result: started, err: startErr}
			}()
			waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_internal_login")
			var beforeExpiry bool
			if err := fixture.database.Admin.QueryRow(
				"SELECT clock_timestamp() < $1",
				expiresAt,
			).Scan(&beforeExpiry); err != nil {
				t.Fatalf("compare authority expiry at Outbox latch: %v", err)
			}
			if !beforeExpiry {
				t.Fatal("authority expired before Start reached the delayed Outbox write")
			}
			waitForDatabaseTimeAfter(t, fixture.database.Admin, expiresAt)
			if err := latch.Commit(); err != nil {
				t.Fatalf("release expired Outbox write: %v", err)
			}

			call := <-result
			if call.err != nil || call.result.Decision != workercontrol.Stop ||
				call.result.StopReason != test.stopReason {
				t.Fatalf("expiry during Outbox result = %#v error=%v", call.result, call.err)
			}
			after := readStartState(t, fixture.database.Admin, assignment.JobID, assignment.AttemptID)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("expired Outbox transaction was partial: before=%#v after=%#v", before, after)
			}
		})
	}
}

func TestStartRollsBackWhenOutboxInsertFails(t *testing.T) {
	fixture := newStartFixture(t, "start-outbox-failure", 7)
	if _, err := fixture.database.Admin.Exec(`
		CREATE FUNCTION reject_start_outbox() RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			RAISE EXCEPTION 'injected job.started Outbox failure';
		END
		$$;
		CREATE TRIGGER reject_start_outbox
		BEFORE INSERT ON outbox_events
		FOR EACH ROW
		WHEN (NEW.event_type = 'job.started')
		EXECUTE FUNCTION reject_start_outbox();
	`); err != nil {
		t.Fatalf("create rejecting Start Outbox trigger: %v", err)
	}
	before := readStartState(t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID)

	result, err := fixture.service.Start(context.Background(), fixture.worker, fixture.credentials)
	if err == nil || result.Decision != "" {
		t.Fatalf("Start with failed Outbox = %#v error=%v", result, err)
	}
	after := readStartState(t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("failed Outbox transaction was partial: before=%#v after=%#v", before, after)
	}
}

func TestStartDoesNotInterruptAssignedJobForRoutineDrain(t *testing.T) {
	fixture := newStartFixture(t, "start-during-routine-drain", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE workers
		SET lifecycle_state = 'DRAINING', updated_at = clock_timestamp()
		WHERE id = $1
	`, fixture.worker.ID); err != nil {
		t.Fatalf("drain assigned Worker: %v", err)
	}

	result, err := fixture.service.Start(context.Background(), fixture.worker, fixture.credentials)
	if err != nil || result.Decision != workercontrol.StartGranted {
		t.Fatalf("Start during routine drain = %#v error=%v", result, err)
	}
}

func TestStartTimestampsRequireRunningTransition(t *testing.T) {
	fixture := newStartFixture(t, "start-timestamps-require-transition", 7)
	tests := []struct {
		name  string
		query string
		id    uuid.UUID
	}{
		{
			name:  "Billable Start",
			query: "UPDATE jobs SET billable_started_at = clock_timestamp() WHERE id = $1",
			id:    fixture.assignment.JobID,
		},
		{
			name:  "Attempt start",
			query: "UPDATE attempts SET started_at = clock_timestamp() WHERE id = $1",
			id:    fixture.assignment.AttemptID,
		},
	}
	before := readStartState(t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := fixture.database.Admin.Exec(test.query, test.id); err == nil {
				t.Fatalf("setting %s without RUNNING transition was accepted", test.name)
			}
		})
	}
	after := readStartState(t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected timestamp mutation changed state: before=%#v after=%#v", before, after)
	}
}

func TestJobRunningTransitionRequiresBillableStart(t *testing.T) {
	fixture := newStartFixture(t, "running-requires-billable-start", 7)
	before := readStartState(t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID)

	if _, err := fixture.database.Admin.Exec(`
		UPDATE jobs
		SET state = 'RUNNING', version = version + 1, updated_at = clock_timestamp()
		WHERE id = $1
	`, fixture.assignment.JobID); err == nil {
		t.Fatal("ASSIGNED to RUNNING without Billable Start was accepted")
	}

	after := readStartState(t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("rejected RUNNING transition changed state: before=%#v after=%#v", before, after)
	}
}

func TestJobCannotBeCreatedRunningWithoutBillableStart(t *testing.T) {
	fixture := newStartFixture(t, "created-running-requires-billable-start", 7)
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin invalid Job insert: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	_, err = tx.Exec(`
		INSERT INTO jobs (
			id, organization_id, project_id, created_by_principal_id, state, version,
			model_revision_id, generation_preset_revision_id, service_class_revision_id,
			output_spec_id, worker_pool_id, request_hash, request_content,
			request_content_expires_at, pricing_rate_card_revision_id, pricing_rate_line_id,
			pricing_unit_amount_minor, pricing_quantity, pricing_quoted_amount_minor,
			pricing_currency, execution_max_attempts, execution_max_total_compute_seconds,
			execution_max_finalization_seconds_per_attempt, execution_retry_backoff_policy,
			execution_retryable_failure_classes, execution_circuit_breaker_policy,
			job_expires_at
		)
		SELECT
			$1, organization_id, project_id, created_by_principal_id, 'RUNNING', 1,
			model_revision_id, generation_preset_revision_id, service_class_revision_id,
			output_spec_id, worker_pool_id, request_hash, request_content,
			request_content_expires_at, pricing_rate_card_revision_id, pricing_rate_line_id,
			pricing_unit_amount_minor, pricing_quantity, pricing_quoted_amount_minor,
			pricing_currency, execution_max_attempts, execution_max_total_compute_seconds,
			execution_max_finalization_seconds_per_attempt, execution_retry_backoff_policy,
			execution_retryable_failure_classes, execution_circuit_breaker_policy,
			job_expires_at
		FROM jobs
		WHERE id = $2
	`, uuid.New(), fixture.assignment.JobID)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23514" ||
		postgresError.ConstraintName != "jobs_running_requires_billable_start" {
		t.Fatalf("RUNNING Job insert error = %v, want jobs_running_requires_billable_start check violation", err)
	}
}

func TestRequestRoleCannotForgeBillableOrAttemptStart(t *testing.T) {
	fixture := newStartFixture(t, "request-role-cannot-forge-start", 7)
	requestPool := newRolePool(t, fixture.database.DSN, "vela_request_login", "vela-request-password")
	tests := []struct {
		name  string
		query string
		id    uuid.UUID
	}{
		{
			name:  "Billable Start",
			query: "UPDATE jobs SET billable_started_at = clock_timestamp() WHERE id = $1",
			id:    fixture.assignment.JobID,
		},
		{
			name:  "Attempt start",
			query: "UPDATE attempts SET started_at = clock_timestamp() WHERE id = $1",
			id:    fixture.assignment.AttemptID,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx, err := requestPool.Begin(context.Background())
			if err != nil {
				t.Fatalf("begin request transaction: %v", err)
			}
			defer func() { _ = tx.Rollback(context.Background()) }()
			if _, err := tx.Exec(
				context.Background(),
				"SELECT * FROM vela_set_request_context($1, $2, $3)",
				testCredentialID,
				credentialDigest([]byte(testCredentialSecret)),
				"jobs:submit",
			); err != nil {
				t.Fatalf("establish request context: %v", err)
			}
			_, err = tx.Exec(context.Background(), test.query, test.id)
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
				t.Fatalf("forged %s error = %v, want SQLSTATE 42501", test.name, err)
			}
		})
	}
}

func TestBillableStartSurvivesLaterRetry(t *testing.T) {
	fixture := newStartFixture(t, "billable-start-survives-retry", 7)
	firstStart, err := fixture.service.Start(context.Background(), fixture.worker, fixture.credentials)
	if err != nil || firstStart.Decision != workercontrol.StartGranted {
		t.Fatalf("start first Attempt = %#v error=%v", firstStart, err)
	}

	retryFixture, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin retry fixture: %v", err)
	}
	defer func() { _ = retryFixture.Rollback() }()
	retryStatements := []struct {
		query string
		args  []any
	}{
		{
			query: `UPDATE attempt_leases
				SET revoked_at = clock_timestamp(), updated_at = clock_timestamp()
				WHERE attempt_id = $1`,
			args: []any{fixture.assignment.AttemptID},
		},
		{
			query: `UPDATE attempts
				SET state = 'FAILED', ended_at = clock_timestamp(), updated_at = clock_timestamp()
				WHERE id = $1`,
			args: []any{fixture.assignment.AttemptID},
		},
		{
			query: `UPDATE jobs
				SET state = 'RETRY_WAIT', version = version + 1, updated_at = clock_timestamp()
				WHERE id = $1`,
			args: []any{fixture.assignment.JobID},
		},
		{
			query: `UPDATE workers
				SET lifecycle_state = 'READY', updated_at = clock_timestamp()
				WHERE id = $1`,
			args: []any{fixture.worker.ID},
		},
		{
			query: `UPDATE projects
				SET queued_count = queued_count + 1,
					retry_wait_count = retry_wait_count + 1,
					running_count = running_count - 1
				WHERE id = $1`,
			args: []any{testProjectID},
		},
		{
			query: `UPDATE worker_pools
				SET queued_count = queued_count + 1,
					retry_wait_count = retry_wait_count + 1
				WHERE id = '00000000-0000-0000-0000-000000000005'`,
		},
		{
			query: `UPDATE retry_runtime_states
				SET next_retry_at = clock_timestamp() - interval '1 second',
					version = version + 1,
					updated_at = clock_timestamp()
				WHERE job_id = $1`,
			args: []any{fixture.assignment.JobID},
		},
	}
	for _, statement := range retryStatements {
		if _, err := retryFixture.Exec(statement.query, statement.args...); err != nil {
			t.Fatalf("stage Retry Wait: %v", err)
		}
	}
	if err := retryFixture.Commit(); err != nil {
		t.Fatalf("commit Retry Wait: %v", err)
	}

	retryCandidate := fixture.candidate
	retryCandidate.ExpectedJobVersion = 4
	secondAssignment, err := fixture.service.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&retryCandidate,
	)
	if err != nil {
		t.Fatalf("create retry Assignment: %v", err)
	}
	if secondAssignment.AttemptNumber != 2 || secondAssignment.LeaseFence != 2 {
		t.Fatalf("retry Assignment = %#v", secondAssignment)
	}
	secondStart, err := fixture.service.Start(
		context.Background(),
		fixture.worker,
		leaseCredentials(secondAssignment),
	)
	if err != nil || secondStart.Decision != workercontrol.StartGranted {
		t.Fatalf("start retry Attempt = %#v error=%v", secondStart, err)
	}

	var (
		jobState, reservationState                         string
		jobVersion, startEvents                            int64
		billableStartedAt, firstStartedAt, secondStartedAt time.Time
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state::text,
			j.version,
			j.billable_started_at,
			(SELECT started_at FROM attempts WHERE id = $2),
			(SELECT started_at FROM attempts WHERE id = $3),
			cr.state::text,
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = j.id AND event_type = 'job.started')
		FROM jobs AS j
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		WHERE j.id = $1
	`, fixture.assignment.JobID, fixture.assignment.AttemptID, secondAssignment.AttemptID).Scan(
		&jobState,
		&jobVersion,
		&billableStartedAt,
		&firstStartedAt,
		&secondStartedAt,
		&reservationState,
		&startEvents,
	); err != nil {
		t.Fatalf("read retry Billable Start history: %v", err)
	}
	if jobState != "RUNNING" || jobVersion != 6 || reservationState != "RESERVED" || startEvents != 2 {
		t.Fatalf(
			"retry Start state = Job %s v%d reservation %s events %d",
			jobState,
			jobVersion,
			reservationState,
			startEvents,
		)
	}
	if !billableStartedAt.Equal(firstStart.StartedAt) || !firstStartedAt.Equal(firstStart.StartedAt) {
		t.Fatalf(
			"first Billable Start changed: marker=%s Attempt=%s want=%s",
			billableStartedAt,
			firstStartedAt,
			firstStart.StartedAt,
		)
	}
	if !secondStartedAt.Equal(secondStart.StartedAt) || secondStartedAt.Before(firstStartedAt) {
		t.Fatalf("retry Attempt start = %s, want result %s after first %s", secondStartedAt, secondStart.StartedAt, firstStartedAt)
	}

	if _, err := fixture.database.Admin.Exec(
		"UPDATE jobs SET billable_started_at = NULL WHERE id = $1",
		fixture.assignment.JobID,
	); err == nil {
		t.Fatal("clearing Billable Start was accepted")
	}
	if _, err := fixture.database.Admin.Exec(
		"UPDATE attempts SET started_at = started_at + interval '1 second' WHERE id = $1",
		fixture.assignment.AttemptID,
	); err == nil {
		t.Fatal("changing Attempt start was accepted")
	}
}

func TestStartCommitsOneBillableStartAndReplays(t *testing.T) {
	fixture := newAssignmentFixture(t, "worker-start-replay", 7)
	assignment, err := fixture.service.Acquire(
		context.Background(),
		fixture.worker,
		7,
		&fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	credentials := workercontrol.LeaseCredentials{
		AttemptID:   assignment.AttemptID,
		WorkerEpoch: assignment.WorkerEpoch,
		Fence:       assignment.LeaseFence,
		Token:       assignment.LeaseToken,
	}

	start := make(chan struct{})
	results := make(chan workercontrol.StartResult, 2)
	startErrors := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, startErr := fixture.service.Start(context.Background(), fixture.worker, credentials)
			results <- result
			startErrors <- startErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(startErrors)
	for startErr := range startErrors {
		if startErr != nil {
			t.Fatalf("concurrent Start: %v", startErr)
		}
	}
	starts := make([]workercontrol.StartResult, 0, 2)
	for result := range results {
		starts = append(starts, result)
	}
	if len(starts) != 2 {
		t.Fatalf("Start results = %d, want 2", len(starts))
	}
	if !reflect.DeepEqual(starts[0], starts[1]) {
		t.Fatalf("concurrent Start replay differs: first=%#v second=%#v", starts[0], starts[1])
	}
	granted := starts[0]
	if granted.Decision != workercontrol.StartGranted ||
		granted.AttemptID != assignment.AttemptID ||
		granted.JobID != assignment.JobID ||
		granted.StartedAt.IsZero() {
		t.Fatalf("Start result = %#v", granted)
	}

	replayed, err := fixture.service.Start(context.Background(), fixture.worker, credentials)
	if err != nil {
		t.Fatalf("replay Start: %v", err)
	}
	if !reflect.DeepEqual(replayed, granted) {
		t.Fatalf("lost-response Start replay = %#v, want %#v", replayed, granted)
	}
	internalPool := newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password")
	restartedService, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("restart Worker coordinator: %v", err)
	}
	replayedAfterRestart, err := restartedService.Start(context.Background(), fixture.worker, credentials)
	if err != nil {
		t.Fatalf("replay Start after coordinator restart: %v", err)
	}
	if !reflect.DeepEqual(replayedAfterRestart, granted) {
		t.Fatalf("post-restart Start replay = %#v, want %#v", replayedAfterRestart, granted)
	}

	var (
		jobState, attemptState, reservationState string
		jobVersion                               int64
		billableStartedAt, attemptStartedAt      time.Time
		startEvents                              int
		reservedMinor                            int64
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state::text,
			j.version,
			j.billable_started_at,
			a.state::text,
			a.started_at,
			cr.state::text,
			(
				SELECT count(*)
				FROM outbox_events
				WHERE aggregate_id = j.id AND event_type = 'job.started'
			),
			oac.reserved_minor
		FROM jobs AS j
		JOIN attempts AS a ON a.job_id = j.id
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		JOIN organization_credit_accounts AS oac ON oac.organization_id = j.organization_id
		WHERE j.id = $1 AND a.id = $2
	`, assignment.JobID, assignment.AttemptID).Scan(
		&jobState,
		&jobVersion,
		&billableStartedAt,
		&attemptState,
		&attemptStartedAt,
		&reservationState,
		&startEvents,
		&reservedMinor,
	); err != nil {
		t.Fatalf("read Billable Start transaction: %v", err)
	}
	if jobState != "RUNNING" || attemptState != "RUNNING" || jobVersion != 3 {
		t.Fatalf("started state = Job %s v%d, Attempt %s", jobState, jobVersion, attemptState)
	}
	if !billableStartedAt.Equal(granted.StartedAt) || !attemptStartedAt.Equal(granted.StartedAt) {
		t.Fatalf(
			"started timestamps = Billable %s Attempt %s, want %s",
			billableStartedAt,
			attemptStartedAt,
			granted.StartedAt,
		)
	}
	if reservationState != "RESERVED" || reservedMinor != 1250 {
		t.Fatalf("Start changed reserved credit: state=%s amount=%d", reservationState, reservedMinor)
	}
	if startEvents != 1 {
		t.Fatalf("job.started events = %d, want 1", startEvents)
	}

	var eventPayload []byte
	if err := fixture.database.Admin.QueryRow(`
		SELECT payload
		FROM outbox_events
		WHERE aggregate_id = $1 AND event_type = 'job.started'
	`, assignment.JobID).Scan(&eventPayload); err != nil {
		t.Fatalf("read job.started payload: %v", err)
	}
	var envelope velav1.EventEnvelope
	if err := proto.Unmarshal(eventPayload, &envelope); err != nil {
		t.Fatalf("decode job.started payload: %v", err)
	}
	startedEvent := envelope.GetJobStarted()
	if envelope.GetAggregateVersion() != 3 || startedEvent == nil ||
		startedEvent.GetJobId() != assignment.JobID.String() ||
		startedEvent.GetAttemptId() != assignment.AttemptID.String() ||
		startedEvent.GetWorkerId() != assignment.WorkerID.String() ||
		startedEvent.GetLeaseFence() != uint64(assignment.LeaseFence) ||
		!startedEvent.GetStartedAt().AsTime().Equal(granted.StartedAt) {
		t.Fatalf("job.started envelope = %#v", &envelope)
	}
}

type startFixture struct {
	assignmentFixture
	assignment  workercontrol.Assignment
	credentials workercontrol.LeaseCredentials
}

type startCallResult struct {
	result workercontrol.StartResult
	err    error
}

func leaseCredentials(assignment workercontrol.Assignment) workercontrol.LeaseCredentials {
	return workercontrol.LeaseCredentials{
		AttemptID:   assignment.AttemptID,
		WorkerEpoch: assignment.WorkerEpoch,
		Fence:       assignment.LeaseFence,
		Token:       assignment.LeaseToken,
	}
}

func newStartFixture(t *testing.T, key string, epoch int64) startFixture {
	t.Helper()
	fixture := newAssignmentFixture(t, key, epoch)
	assignment, err := fixture.service.Acquire(
		context.Background(),
		fixture.worker,
		epoch,
		&fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	return startFixture{
		assignmentFixture: fixture,
		assignment:        assignment,
		credentials:       leaseCredentials(assignment),
	}
}

type startState struct {
	JobState          string
	JobVersion        int64
	BillableStartedAt sql.NullTime
	AttemptState      string
	AttemptStartedAt  sql.NullTime
	ReservationState  string
	StartEvents       int
}

func readStartState(t *testing.T, db *sql.DB, jobID, attemptID uuid.UUID) startState {
	t.Helper()
	var state startState
	if err := db.QueryRow(`
		SELECT
			j.state::text,
			j.version,
			j.billable_started_at,
			a.state::text,
			a.started_at,
			cr.state::text,
			(
				SELECT count(*)
				FROM outbox_events
				WHERE aggregate_id = j.id AND event_type = 'job.started'
			)
		FROM jobs AS j
		JOIN attempts AS a ON a.job_id = j.id
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		WHERE j.id = $1 AND a.id = $2
	`, jobID, attemptID).Scan(
		&state.JobState,
		&state.JobVersion,
		&state.BillableStartedAt,
		&state.AttemptState,
		&state.AttemptStartedAt,
		&state.ReservationState,
		&state.StartEvents,
	); err != nil {
		t.Fatalf("read Start state: %v", err)
	}
	return state
}
