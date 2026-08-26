//go:build integration

package integration_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivym/vela/internal/scheduler"
)

func TestSynchronousQuorumGuardIsExplicitAndProtectsSchedulerClaims(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	defaultServer := admissionServerForDatabase(t, database)
	queuedJob := submitSchedulerJob(
		t,
		defaultServer.URL,
		testProjectID,
		testBearerCredential(),
		"quorum-guard-default-off",
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
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, schedulerWorker{
		ID:       uuid.MustParse("00000000-0000-0000-0000-000000000390"),
		SPIFFEID: "spiffe://vela.internal/worker/quorum-guard",
		Epoch:    7,
	})

	if _, err := database.Admin.Exec(`
		ALTER ROLE vela_request_login SET "vela.require_synchronous_quorum" = 'on';
		ALTER ROLE vela_scheduler_login SET "vela.require_synchronous_quorum" = 'on';
	`); err != nil {
		t.Fatalf("enable synchronous quorum guard for application roles: %v", err)
	}

	guardedServer := admissionServerForDatabase(t, database)
	blockedAdmission := submitJob(
		t,
		guardedServer.URL,
		"quorum-guard-no-synchronous-configuration",
		[]byte(`{
			"model":"minimax-h3",
			"generation_preset":"balanced",
			"service_class":"standard",
			"output_spec":"video-1080p-5s-24fps",
			"generation_count":1,
			"prompt":"the explicit quorum guard must reject this Job"
		}`),
	)
	if blockedAdmission.StatusCode != http.StatusInternalServerError {
		t.Fatalf(
			"guarded single-node Admission status = %d body=%s, want 500",
			blockedAdmission.StatusCode,
			blockedAdmission.Body,
		)
	}

	internalPool := newRolePool(
		t, database.DSN, "vela_internal_login", "vela-internal-password",
	)
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create guarded Assignment coordinator: %v", err)
	}
	schedulerPool := newRolePool(
		t, database.DSN, "vela_scheduler_login", "vela-scheduler-password",
	)
	scheduling, err := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "quorum-guard-integration",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 1,
	})
	if err != nil {
		t.Fatalf("create guarded Scheduler: %v", err)
	}
	dispatch, ok, schedulerErr := scheduling.RunOnce(context.Background(), poolID)
	var postgresError *pgconn.PgError
	if schedulerErr == nil || ok ||
		!errors.As(schedulerErr, &postgresError) || postgresError.Code != "55000" {
		t.Fatalf(
			"guarded Scheduler RunOnce = %#v ok=%t error=%v, want SQLSTATE 55000",
			dispatch,
			ok,
			schedulerErr,
		)
	}

	var jobs, dispatches, attempts int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM jobs),
			(SELECT count(*) FROM scheduler_dispatch_intents),
			(SELECT count(*) FROM attempts)
	`).Scan(&jobs, &dispatches, &attempts); err != nil {
		t.Fatalf("read guarded authority counts: %v", err)
	}
	if jobs != 1 || dispatches != 0 || attempts != 0 {
		t.Fatalf(
			"guarded authority = Jobs %d dispatches %d Attempts %d; want 1/0/0 for queued Job %s",
			jobs,
			dispatches,
			attempts,
			queuedJob,
		)
	}
}
