//go:build integration

package integration_test

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/inbox"
	"github.com/vivym/vela/internal/scheduler"
)

func TestSchedulerInboxReceiptUsesOnlyDedicatedFunction(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "scheduler-inbox-role", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"exercise the dedicated Scheduler Inbox receipt boundary"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	schedulerInboxPool := newRolePool(
		t,
		database.DSN,
		"vela_scheduler_inbox_login",
		"vela-scheduler-inbox-password",
	)

	var organizationID, projectID, eventID, jobID uuid.UUID
	var aggregateVersion int64
	if err := database.Admin.QueryRow(`
		SELECT organization_id, project_id, event_id, aggregate_id, aggregate_version
		FROM outbox_events
		LIMIT 1
	`).Scan(&organizationID, &projectID, &eventID, &jobID, &aggregateVersion); err != nil {
		t.Fatalf("read Scheduler Inbox fixture: %v", err)
	}
	otherOrganizationID := uuid.New()
	otherProjectID := uuid.New()
	if _, err := database.Admin.Exec(
		"INSERT INTO customer_organizations (id, display_name) VALUES ($1, 'Other Inbox Organization')",
		otherOrganizationID,
	); err != nil {
		t.Fatalf("seed mismatched Scheduler Inbox Organization: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO projects (id, organization_id, display_name, queued_limit, running_limit)
		VALUES ($1, $2, 'Other Inbox Project', 1, 1)
	`, otherProjectID, otherOrganizationID); err != nil {
		t.Fatalf("seed mismatched Scheduler Inbox Project: %v", err)
	}

	for _, function := range []struct {
		name  string
		query string
	}{
		{
			name:  "prepare",
			query: "SELECT vela_prepare_scheduler_inbox_receipt($1, $2, $3, $4, $5)",
		},
		{
			name:  "record",
			query: "SELECT vela_record_scheduler_inbox_receipt($1, $2, $3, $4, $5)",
		},
	} {
		for _, test := range []struct {
			name             string
			eventID          uuid.UUID
			organizationID   uuid.UUID
			projectID        uuid.UUID
			jobID            uuid.UUID
			aggregateVersion int64
		}{
			{
				name: "event id", eventID: uuid.New(), organizationID: organizationID,
				projectID: projectID, jobID: jobID, aggregateVersion: aggregateVersion,
			},
			{
				name: "Organization and Project", eventID: eventID, organizationID: otherOrganizationID,
				projectID: otherProjectID, jobID: jobID, aggregateVersion: aggregateVersion,
			},
			{
				name: "Job", eventID: eventID, organizationID: organizationID,
				projectID: projectID, jobID: uuid.New(), aggregateVersion: aggregateVersion,
			},
			{
				name: "aggregate version", eventID: eventID, organizationID: organizationID,
				projectID: projectID, jobID: jobID, aggregateVersion: aggregateVersion + 1,
			},
		} {
			t.Run(function.name+" rejects mismatched "+test.name, func(t *testing.T) {
				var mismatchedApplied bool
				err := schedulerInboxPool.QueryRow(
					context.Background(),
					function.query,
					test.eventID,
					test.organizationID,
					test.projectID,
					test.jobID,
					test.aggregateVersion,
				).Scan(&mismatchedApplied)
				if err == nil {
					t.Errorf(
						"mismatched %s receipt = %v, nil; want canonical Outbox rejection",
						test.name,
						mismatchedApplied,
					)
				}
				if _, cleanupErr := database.Admin.Exec(
					"DELETE FROM inbox_receipts WHERE consumer_name = 'scheduler'",
				); cleanupErr != nil {
					t.Fatalf("clean mismatched Scheduler Inbox receipt: %v", cleanupErr)
				}
			})
		}
	}

	var prepared, applied bool
	if err := schedulerInboxPool.QueryRow(
		context.Background(),
		"SELECT vela_prepare_scheduler_inbox_receipt($1, $2, $3, $4, $5)",
		eventID,
		organizationID,
		projectID,
		jobID,
		aggregateVersion,
	).Scan(&prepared); err != nil || !prepared {
		t.Fatalf("prepare Scheduler Inbox receipt = %v, %v; want true, nil", prepared, err)
	}
	if err := schedulerInboxPool.QueryRow(
		context.Background(),
		"SELECT vela_record_scheduler_inbox_receipt($1, $2, $3, $4, $5)",
		eventID,
		organizationID,
		projectID,
		jobID,
		aggregateVersion,
	).Scan(&applied); err != nil || !applied {
		t.Fatalf("record Scheduler Inbox receipt = %v, %v; want true, nil", applied, err)
	}
	if err := schedulerInboxPool.QueryRow(
		context.Background(),
		"SELECT vela_record_scheduler_inbox_receipt($1, $2, $3, $4, $5)",
		eventID,
		organizationID,
		projectID,
		jobID,
		aggregateVersion,
	).Scan(&applied); err != nil || applied {
		t.Fatalf("duplicate Scheduler Inbox receipt = %v, %v; want false, nil", applied, err)
	}
	if err := schedulerInboxPool.QueryRow(
		context.Background(),
		"SELECT vela_prepare_scheduler_inbox_receipt($1, $2, $3, $4, $5)",
		eventID,
		organizationID,
		projectID,
		jobID,
		aggregateVersion,
	).Scan(&prepared); err != nil || prepared {
		t.Fatalf("prepare committed Scheduler Inbox receipt = %v, %v; want false, nil", prepared, err)
	}

	if _, err := schedulerInboxPool.Exec(
		context.Background(),
		"INSERT INTO inbox_receipts (consumer_name, event_id, organization_id, project_id, aggregate_type, aggregate_id, aggregate_version, event_type) VALUES ('scheduler', $1, $2, $3, 'Job', $4, $5, 'job.ready')",
		uuid.New(),
		organizationID,
		projectID,
		jobID,
		aggregateVersion+1,
	); !isPermissionDenied(err) {
		t.Fatalf("Scheduler Inbox direct write error = %v, want permission denied", err)
	}

	schedulerPool := newRolePool(t, database.DSN, "vela_scheduler_login", "vela-scheduler-password")
	for _, query := range []string{
		"SELECT vela_prepare_scheduler_inbox_receipt($1, $2, $3, $4, $5)",
		"SELECT vela_record_scheduler_inbox_receipt($1, $2, $3, $4, $5)",
	} {
		if err := schedulerPool.QueryRow(
			context.Background(),
			query,
			uuid.New(),
			organizationID,
			projectID,
			jobID,
			aggregateVersion+1,
		).Scan(&applied); !isPermissionDenied(err) {
			t.Fatalf("Scheduler receipt call error = %v, want permission denied", err)
		}
	}
}

func TestSchedulerInboxPersistsAssignmentBeforeReceiptWithoutJobLockDeadlock(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Scheduler Inbox lock-order fixture")
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	jobID := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-inbox-lock-order",
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
		ID:       uuid.MustParse("00000000-0000-0000-0000-000000000324"),
		SPIFFEID: "spiffe://vela.internal/worker/scheduler-inbox-lock-order",
		Epoch:    7,
	})

	var event inbox.Event
	if err := database.Admin.QueryRow(`
		SELECT event_id, organization_id, project_id, aggregate_type,
		       aggregate_id, aggregate_version, event_type
		FROM outbox_events
		WHERE aggregate_id = $1 AND event_type = 'job.ready'
	`, jobID).Scan(
		&event.ID,
		&event.OrganizationID,
		&event.ProjectID,
		&event.AggregateType,
		&event.AggregateID,
		&event.AggregateVersion,
		&event.Type,
	); err != nil {
		t.Fatalf("read Scheduler Inbox event: %v", err)
	}

	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create Scheduler Inbox Worker coordinator: %v", err)
	}
	schedulerPool := newRolePool(t, database.DSN, "vela_scheduler_login", "vela-scheduler-password")
	scheduling, err := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "scheduler-inbox-lock-order",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 3,
	})
	if err != nil {
		t.Fatalf("create Scheduler Inbox Scheduler: %v", err)
	}
	schedulerInboxPool := newRolePool(
		t,
		database.DSN,
		"vela_scheduler_inbox_login",
		"vela-scheduler-inbox-password",
	)
	processor, err := inbox.NewSchedulerProcessor(schedulerInboxPool)
	if err != nil {
		t.Fatalf("create Scheduler Inbox processor: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var dispatches []scheduler.Dispatch
	applied, err := processor.ProcessOnce(ctx, event, func(handlerContext context.Context, _ pgx.Tx) error {
		var cycleErr error
		dispatches, cycleErr = scheduling.RunCycle(handlerContext)
		return cycleErr
	})
	if err != nil || !applied {
		t.Fatalf("process Scheduler Inbox event = applied %v error %v", applied, err)
	}
	if len(dispatches) != 1 || dispatches[0].Assignment.JobID != jobID {
		t.Fatalf("Scheduler Inbox dispatches = %#v, want Job %s", dispatches, jobID)
	}

	var jobState string
	var receiptCount int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT state::text FROM jobs WHERE id = $1),
			(SELECT count(*) FROM inbox_receipts
			 WHERE consumer_name = 'scheduler' AND event_id = $2)
	`, jobID, event.ID).Scan(&jobState, &receiptCount); err != nil {
		t.Fatalf("read Scheduler Inbox lock-order result: %v", err)
	}
	if jobState != "ASSIGNED" || receiptCount != 1 {
		t.Fatalf("Scheduler Inbox result = Job %s receipts %d", jobState, receiptCount)
	}

	applied, err = processor.ProcessOnce(
		context.Background(),
		event,
		func(context.Context, pgx.Tx) error {
			t.Fatal("committed Scheduler Inbox receipt invoked the handler twice")
			return nil
		},
	)
	if err != nil || applied {
		t.Fatalf("duplicate Scheduler Inbox event = applied %v error %v", applied, err)
	}
}

func TestSchedulerInboxSerializesConcurrentRedeliveryBeforeHandler(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "scheduler-inbox-concurrent-redelivery", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"serialize concurrent Scheduler Inbox redelivery"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}

	var event inbox.Event
	if err := database.Admin.QueryRow(`
		SELECT event_id, organization_id, project_id, aggregate_type,
		       aggregate_id, aggregate_version, event_type
		FROM outbox_events
		LIMIT 1
	`).Scan(
		&event.ID,
		&event.OrganizationID,
		&event.ProjectID,
		&event.AggregateType,
		&event.AggregateID,
		&event.AggregateVersion,
		&event.Type,
	); err != nil {
		t.Fatalf("read concurrent Scheduler Inbox event: %v", err)
	}

	newProcessor := func() *inbox.Processor {
		processor, err := inbox.NewSchedulerProcessor(newRolePool(
			t,
			database.DSN,
			"vela_scheduler_inbox_login",
			"vela-scheduler-inbox-password",
		))
		if err != nil {
			t.Fatalf("create concurrent Scheduler Inbox processor: %v", err)
		}
		return processor
	}
	firstProcessor := newProcessor()
	secondProcessor := newProcessor()

	type processResult struct {
		applied bool
		err     error
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstResult := make(chan processResult, 1)
	go func() {
		applied, err := firstProcessor.ProcessOnce(ctx, event, func(handlerContext context.Context, _ pgx.Tx) error {
			close(firstStarted)
			select {
			case <-releaseFirst:
				return nil
			case <-handlerContext.Done():
				return handlerContext.Err()
			}
		})
		firstResult <- processResult{applied: applied, err: err}
	}()
	select {
	case <-firstStarted:
	case <-ctx.Done():
		t.Fatalf("first Scheduler Inbox handler did not start: %v", ctx.Err())
	}

	secondHandlerCalled := make(chan struct{}, 1)
	secondResult := make(chan processResult, 1)
	go func() {
		applied, err := secondProcessor.ProcessOnce(ctx, event, func(context.Context, pgx.Tx) error {
			secondHandlerCalled <- struct{}{}
			return nil
		})
		secondResult <- processResult{applied: applied, err: err}
	}()
	select {
	case <-secondHandlerCalled:
		close(releaseFirst)
		t.Fatal("concurrent Scheduler Inbox redelivery entered the handler before the first receipt committed")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)

	first := <-firstResult
	second := <-secondResult
	if first.err != nil || !first.applied {
		t.Fatalf("first concurrent Scheduler Inbox result = applied %v error %v", first.applied, first.err)
	}
	if second.err != nil || second.applied {
		t.Fatalf("second concurrent Scheduler Inbox result = applied %v error %v", second.applied, second.err)
	}
	select {
	case <-secondHandlerCalled:
		t.Fatal("concurrent Scheduler Inbox redelivery invoked the handler after receipt commit")
	default:
	}

	var receipts int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM inbox_receipts
		WHERE consumer_name = 'scheduler' AND event_id = $1
	`, event.ID).Scan(&receipts); err != nil {
		t.Fatalf("count concurrent Scheduler Inbox receipts: %v", err)
	}
	if receipts != 1 {
		t.Fatalf("concurrent Scheduler Inbox receipts = %d, want 1", receipts)
	}
}

func TestSchedulerInboxMigrationRoundTripPreservesReceipts(t *testing.T) {
	database := newPostgres(t)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	applyFoundation(t, database.Admin)
	if err := goose.DownTo(database.Admin, migrations, 24); err != nil {
		t.Fatalf("migrate Scheduler Inbox fixture to N-1: %v", err)
	}
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "scheduler-inbox-migration", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"preserve Scheduler Inbox receipts across migration round trips"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}

	var organizationID, projectID, eventID, jobID uuid.UUID
	var aggregateVersion int64
	var tableOID uint32
	if err := database.Admin.QueryRow(`
		SELECT organization_id, project_id, event_id, aggregate_id, aggregate_version,
		       'public.inbox_receipts'::regclass::oid
		FROM outbox_events
		LIMIT 1
	`).Scan(
		&organizationID,
		&projectID,
		&eventID,
		&jobID,
		&aggregateVersion,
		&tableOID,
	); err != nil {
		t.Fatalf("read migration fixture: %v", err)
	}
	if err := goose.UpTo(database.Admin, migrations, 25); err != nil {
		t.Fatalf("expand Scheduler Inbox migration: %v", err)
	}
	schedulerInboxPool := newRolePool(
		t,
		database.DSN,
		"vela_scheduler_inbox_login",
		"vela-scheduler-inbox-password",
	)
	var prepared, applied bool
	if err := schedulerInboxPool.QueryRow(
		context.Background(),
		"SELECT vela_prepare_scheduler_inbox_receipt($1, $2, $3, $4, $5)",
		eventID,
		organizationID,
		projectID,
		jobID,
		aggregateVersion,
	).Scan(&prepared); err != nil || !prepared {
		t.Fatalf("prepare receipt before contraction = %v, %v", prepared, err)
	}
	if err := schedulerInboxPool.QueryRow(
		context.Background(),
		"SELECT vela_record_scheduler_inbox_receipt($1, $2, $3, $4, $5)",
		eventID,
		organizationID,
		projectID,
		jobID,
		aggregateVersion,
	).Scan(&applied); err != nil || !applied {
		t.Fatalf("record receipt before contraction = %v, %v", applied, err)
	}
	schedulerInboxPool.Close()

	if err := goose.DownTo(database.Admin, migrations, 24); err != nil {
		t.Fatalf("contract Scheduler Inbox migration: %v", err)
	}
	var currentTableOID uint32
	var receipts int
	var prepareFunctionExists, recordFunctionExists bool
	if err := database.Admin.QueryRow(`
		SELECT
			'public.inbox_receipts'::regclass::oid,
			(SELECT count(*) FROM inbox_receipts WHERE event_id = $1),
			to_regprocedure('public.vela_prepare_scheduler_inbox_receipt(uuid,uuid,uuid,uuid,bigint)') IS NOT NULL,
			to_regprocedure('public.vela_record_scheduler_inbox_receipt(uuid,uuid,uuid,uuid,bigint)') IS NOT NULL
	`, eventID).Scan(
		&currentTableOID,
		&receipts,
		&prepareFunctionExists,
		&recordFunctionExists,
	); err != nil {
		t.Fatalf("inspect contracted Scheduler Inbox surface: %v", err)
	}
	if currentTableOID != tableOID || receipts != 1 || prepareFunctionExists || recordFunctionExists {
		t.Fatalf(
			"contracted Scheduler Inbox = table %d/%d receipts %d functions %v/%v",
			currentTableOID,
			tableOID,
			receipts,
			prepareFunctionExists,
			recordFunctionExists,
		)
	}

	if err := goose.UpTo(database.Admin, migrations, 25); err != nil {
		t.Fatalf("re-expand Scheduler Inbox migration: %v", err)
	}
	schedulerInboxPool = newRolePool(
		t,
		database.DSN,
		"vela_scheduler_inbox_login",
		"vela-scheduler-inbox-password",
	)
	if err := schedulerInboxPool.QueryRow(
		context.Background(),
		"SELECT vela_prepare_scheduler_inbox_receipt($1, $2, $3, $4, $5)",
		eventID,
		organizationID,
		projectID,
		jobID,
		aggregateVersion,
	).Scan(&prepared); err != nil || prepared {
		t.Fatalf("prepare duplicate receipt after re-expansion = %v, %v", prepared, err)
	}
	if err := schedulerInboxPool.QueryRow(
		context.Background(),
		"SELECT vela_record_scheduler_inbox_receipt($1, $2, $3, $4, $5)",
		eventID,
		organizationID,
		projectID,
		jobID,
		aggregateVersion,
	).Scan(&applied); err != nil || applied {
		t.Fatalf("duplicate receipt after re-expansion = %v, %v", applied, err)
	}
	schedulerInboxPool.Close()
}
