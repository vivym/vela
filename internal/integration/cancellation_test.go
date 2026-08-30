//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestQueuedJobCancellationReleasesCreditWithoutCharge(t *testing.T) {
	server, admin := newAdmissionServer(t)
	if _, err := admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	accepted := submitJob(t, server.URL, "cancel-queued", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"cancel this Accepted Job before Billable Start"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Accepted Job: %v", err)
	}

	canceled := cancelJob(t, server.URL, testProjectID, job.JobID, testBearerCredential())
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200; body=%s", canceled.StatusCode, canceled.Body)
	}
	var result cancelResponse
	if err := json.Unmarshal(canceled.Body, &result); err != nil {
		t.Fatalf("decode cancellation response: %v", err)
	}
	if result.CancellationID == "" || result.JobID != job.JobID ||
		result.Decision != "CANCELED" || result.State != "CANCELED" ||
		result.JobVersion != 2 || result.Billable || result.Charge != nil ||
		result.DecidedAt.IsZero() {
		t.Fatalf("cancellation result = %#v", result)
	}

	var (
		state                                          string
		jobVersion, projectQueued, poolQueued          int64
		reservedMinor, unsettledPostedMinor            int64
		reservationState                               string
		cancellationCount, chargeCount, canceledEvents int64
	)
	if err := admin.QueryRow(`
		SELECT
			j.state,
			j.version,
			p.queued_count,
			wp.queued_count,
			oca.reserved_minor,
			oca.unsettled_posted_minor,
			cr.state,
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = j.id),
			(SELECT count(*) FROM charges WHERE job_id = j.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id AND event_type = 'job.canceled')
		FROM jobs AS j
		JOIN projects AS p ON p.id = j.project_id
		JOIN worker_pools AS wp ON wp.id = j.worker_pool_id
		JOIN organization_credit_accounts AS oca ON oca.organization_id = j.organization_id
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		WHERE j.id = $1
	`, job.JobID).Scan(
		&state,
		&jobVersion,
		&projectQueued,
		&poolQueued,
		&reservedMinor,
		&unsettledPostedMinor,
		&reservationState,
		&cancellationCount,
		&chargeCount,
		&canceledEvents,
	); err != nil {
		t.Fatalf("read queued cancellation state: %v", err)
	}
	if state != "CANCELED" || jobVersion != 2 || projectQueued != 0 || poolQueued != 0 ||
		reservedMinor != 0 || unsettledPostedMinor != 0 || reservationState != "RELEASED" ||
		cancellationCount != 1 || chargeCount != 0 || canceledEvents != 1 {
		t.Fatalf(
			"queued cancellation state = state %s version %d project/pool %d/%d credit %d/%d reservation %s decisions/charges/events %d/%d/%d",
			state,
			jobVersion,
			projectQueued,
			poolQueued,
			reservedMinor,
			unsettledPostedMinor,
			reservationState,
			cancellationCount,
			chargeCount,
			canceledEvents,
		)
	}
}

func TestAssignedJobCancellationFencesAttemptWithoutCharge(t *testing.T) {
	fixture := newAssignmentFixture(t, "cancel-assigned", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)

	canceled := cancelJob(
		t,
		server.URL,
		testProjectID,
		assignment.JobID.String(),
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel ASSIGNED status = %d, want 200; body=%s", canceled.StatusCode, canceled.Body)
	}
	var result cancelResponse
	if err := json.Unmarshal(canceled.Body, &result); err != nil {
		t.Fatalf("decode ASSIGNED cancellation: %v", err)
	}
	if result.Decision != "CANCELED" || result.State != "CANCELED" ||
		result.JobVersion != 3 || result.Billable || result.Charge != nil {
		t.Fatalf("ASSIGNED cancellation result = %#v", result)
	}

	var (
		jobState, attemptState, workerState, reservationState string
		jobVersion, currentFence, projectRunning              int64
		reservedMinor, unsettledPostedMinor                   int64
		ended, revoked                                        bool
		decisionAttempt, decisionWorker                       string
		decisionEpoch, decisionFence, cancellationFence       int64
		chargeCount, stopEvents, canceledEvents               int64
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state,
			j.version,
			j.current_fence,
			a.state,
			a.ended_at IS NOT NULL,
			l.revoked_at IS NOT NULL,
			w.lifecycle_state,
			p.running_count,
			cr.state,
			oca.reserved_minor,
			oca.unsettled_posted_minor,
			d.attempt_id::text,
			d.worker_id::text,
			d.worker_epoch,
			d.attempt_fence,
			d.cancellation_fence,
			(SELECT count(*) FROM charges WHERE job_id = j.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id AND event_type = 'job.cancel_requested'),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id AND event_type = 'job.canceled')
		FROM jobs AS j
		JOIN attempts AS a ON a.job_id = j.id
		JOIN attempt_leases AS l ON l.attempt_id = a.id
		JOIN workers AS w ON w.id = a.worker_id
		JOIN projects AS p ON p.id = j.project_id
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		JOIN organization_credit_accounts AS oca ON oca.organization_id = j.organization_id
		JOIN job_cancellation_decisions AS d ON d.job_id = j.id
		WHERE j.id = $1
	`, assignment.JobID).Scan(
		&jobState,
		&jobVersion,
		&currentFence,
		&attemptState,
		&ended,
		&revoked,
		&workerState,
		&projectRunning,
		&reservationState,
		&reservedMinor,
		&unsettledPostedMinor,
		&decisionAttempt,
		&decisionWorker,
		&decisionEpoch,
		&decisionFence,
		&cancellationFence,
		&chargeCount,
		&stopEvents,
		&canceledEvents,
	); err != nil {
		t.Fatalf("read ASSIGNED cancellation state: %v", err)
	}
	if jobState != "CANCELED" || jobVersion != 3 ||
		currentFence != assignment.LeaseFence+1 || attemptState != "CANCELED" ||
		!ended || !revoked || workerState != "DRAINING" || projectRunning != 0 ||
		reservationState != "RELEASED" || reservedMinor != 0 || unsettledPostedMinor != 0 ||
		decisionAttempt != assignment.AttemptID.String() ||
		decisionWorker != assignment.WorkerID.String() || decisionEpoch != assignment.WorkerEpoch ||
		decisionFence != assignment.LeaseFence || cancellationFence != assignment.LeaseFence+1 ||
		chargeCount != 0 || stopEvents != 1 || canceledEvents != 1 {
		t.Fatalf(
			"ASSIGNED cancellation state = job %s/%d fence %d attempt %s ended/revoked %t/%t worker %s running %d reservation %s credit %d/%d decision %s/%s/%d/%d/%d charges/events %d/%d/%d",
			jobState,
			jobVersion,
			currentFence,
			attemptState,
			ended,
			revoked,
			workerState,
			projectRunning,
			reservationState,
			reservedMinor,
			unsettledPostedMinor,
			decisionAttempt,
			decisionWorker,
			decisionEpoch,
			decisionFence,
			cancellationFence,
			chargeCount,
			stopEvents,
			canceledEvents,
		)
	}

	coordinator := cancellation.NewService(
		newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password"),
		newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password"),
	)
	firstStop, err := coordinator.AcknowledgeCancellationStop(
		context.Background(),
		fixture.worker,
		leaseCredentials(assignment),
		uuid.MustParse(result.CancellationID),
	)
	if err != nil {
		t.Fatalf("acknowledge ASSIGNED cancellation stop: %v", err)
	}
	replayedStop, err := coordinator.AcknowledgeCancellationStop(
		context.Background(),
		fixture.worker,
		leaseCredentials(assignment),
		uuid.MustParse(result.CancellationID),
	)
	if err != nil {
		t.Fatalf("replay ASSIGNED cancellation stop: %v", err)
	}
	if firstStop.Decision != cancellation.StopAcknowledged || firstStop.ReceiptID == uuid.Nil ||
		firstStop.State != "CANCELED" || firstStop.JobVersion != 3 ||
		!reflect.DeepEqual(replayedStop, firstStop) {
		t.Fatalf("ASSIGNED stop acknowledgement/replay = %#v / %#v", firstStop, replayedStop)
	}

	var workerAfterStop, receiptSource string
	var jobVersionAfterStop, receiptVersion, receiptCountAfterStop int64
	var canceledEventsAfterStop, chargesAfterStop int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.version,
			worker.lifecycle_state,
			receipt.source,
			receipt.terminal_job_version,
			(SELECT count(*) FROM cancellation_stop_receipts WHERE cancellation_id = decision.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = job.id AND event_type = 'job.canceled'),
			(SELECT count(*) FROM charges WHERE job_id = job.id)
		FROM jobs AS job
		JOIN job_cancellation_decisions AS decision ON decision.job_id = job.id
		JOIN cancellation_stop_receipts AS receipt ON receipt.cancellation_id = decision.id
		JOIN workers AS worker ON worker.id = decision.worker_id
		WHERE job.id = $1
	`, assignment.JobID).Scan(
		&jobVersionAfterStop,
		&workerAfterStop,
		&receiptSource,
		&receiptVersion,
		&receiptCountAfterStop,
		&canceledEventsAfterStop,
		&chargesAfterStop,
	); err != nil {
		t.Fatalf("read ASSIGNED stop acknowledgement state: %v", err)
	}
	if jobVersionAfterStop != 3 || workerAfterStop != "READY" || receiptSource != "ACKNOWLEDGED" ||
		receiptVersion != 3 || receiptCountAfterStop != 1 || canceledEventsAfterStop != 1 ||
		chargesAfterStop != 0 {
		t.Fatalf(
			"ASSIGNED stop state = version %d worker %s receipt %s/%d/%d canceled events %d charges %d",
			jobVersionAfterStop,
			workerAfterStop,
			receiptSource,
			receiptVersion,
			receiptCountAfterStop,
			canceledEventsAfterStop,
			chargesAfterStop,
		)
	}
}

func TestRetryWaitCancellationReplaysDecisionAndReleasesRetryCounterWithoutCharge(t *testing.T) {
	fixture := newAssignmentFixture(t, "cancel-retry-wait", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	retry, err := fixture.service.Fail(
		context.Background(),
		fixture.worker,
		leaseCredentials(assignment),
		validFailureObservation(),
	)
	if err != nil {
		t.Fatalf("move Job to RETRY_WAIT: %v", err)
	}
	if retry.Disposition != workercontrol.RetryDispositionRetryWait {
		t.Fatalf("failure disposition = %s, want RETRY_WAIT", retry.Disposition)
	}
	server := admissionServerForDatabase(t, fixture.database)

	first := cancelJob(
		t,
		server.URL,
		testProjectID,
		assignment.JobID.String(),
		testBearerCredential(),
	)
	if first.StatusCode != http.StatusOK {
		t.Fatalf("first RETRY_WAIT cancel status = %d, want 200; body=%s", first.StatusCode, first.Body)
	}
	second := cancelJob(
		t,
		server.URL,
		testProjectID,
		assignment.JobID.String(),
		testBearerCredential(),
	)
	if second.StatusCode != http.StatusOK {
		t.Fatalf("replayed RETRY_WAIT cancel status = %d, want 200; body=%s", second.StatusCode, second.Body)
	}
	var firstResult, replayedResult cancelResponse
	if err := json.Unmarshal(first.Body, &firstResult); err != nil {
		t.Fatalf("decode first RETRY_WAIT cancellation: %v", err)
	}
	if err := json.Unmarshal(second.Body, &replayedResult); err != nil {
		t.Fatalf("decode replayed RETRY_WAIT cancellation: %v", err)
	}
	if firstResult.CancellationID == "" ||
		firstResult.CancellationID != replayedResult.CancellationID ||
		firstResult.Decision != "CANCELED" || replayedResult.Decision != "CANCELED" ||
		firstResult.State != "CANCELED" || replayedResult.State != "CANCELED" ||
		firstResult.JobVersion != retry.JobVersion+1 ||
		replayedResult.JobVersion != firstResult.JobVersion ||
		firstResult.Billable || replayedResult.Billable ||
		firstResult.Charge != nil || replayedResult.Charge != nil ||
		!firstResult.DecidedAt.Equal(replayedResult.DecidedAt) {
		t.Fatalf("RETRY_WAIT first/replayed cancellation = %#v / %#v", firstResult, replayedResult)
	}

	var (
		jobState, reservationState                             string
		projectQueued, projectRetryWait, poolQueued, poolRetry int64
		reservedMinor, unsettledPostedMinor                    int64
		decisionCount, chargeCount, requestedEvents            int64
		canceledEvents                                         int64
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state,
			p.queued_count,
			p.retry_wait_count,
			wp.queued_count,
			wp.retry_wait_count,
			cr.state,
			oca.reserved_minor,
			oca.unsettled_posted_minor,
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = j.id),
			(SELECT count(*) FROM charges WHERE job_id = j.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id AND event_type = 'job.cancel_requested'),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id AND event_type = 'job.canceled')
		FROM jobs AS j
		JOIN projects AS p ON p.id = j.project_id
		JOIN worker_pools AS wp ON wp.id = j.worker_pool_id
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		JOIN organization_credit_accounts AS oca ON oca.organization_id = j.organization_id
		WHERE j.id = $1
	`, assignment.JobID).Scan(
		&jobState,
		&projectQueued,
		&projectRetryWait,
		&poolQueued,
		&poolRetry,
		&reservationState,
		&reservedMinor,
		&unsettledPostedMinor,
		&decisionCount,
		&chargeCount,
		&requestedEvents,
		&canceledEvents,
	); err != nil {
		t.Fatalf("read RETRY_WAIT cancellation state: %v", err)
	}
	if jobState != "CANCELED" || projectQueued != 0 || projectRetryWait != 0 ||
		poolQueued != 0 || poolRetry != 0 || reservationState != "RELEASED" ||
		reservedMinor != 0 || unsettledPostedMinor != 0 || decisionCount != 1 ||
		chargeCount != 0 || requestedEvents != 0 || canceledEvents != 1 {
		t.Fatalf(
			"RETRY_WAIT cancellation state = job %s project %d/%d pool %d/%d reservation %s credit %d/%d decisions/charges/events %d/%d/%d/%d",
			jobState,
			projectQueued,
			projectRetryWait,
			poolQueued,
			poolRetry,
			reservationState,
			reservedMinor,
			unsettledPostedMinor,
			decisionCount,
			chargeCount,
			requestedEvents,
			canceledEvents,
		)
	}
}

func TestRunningJobCancellationPostsFullQuoteBeforeWorkerStops(t *testing.T) {
	fixture := newStartFixture(t, "cancel-running-charge", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	server := admissionServerForDatabase(t, fixture.database)

	first := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if first.StatusCode != http.StatusOK {
		t.Fatalf("cancel RUNNING status = %d, want 200; body=%s", first.StatusCode, first.Body)
	}
	replayed := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if replayed.StatusCode != http.StatusOK {
		t.Fatalf("replay RUNNING cancellation status = %d, want 200; body=%s", replayed.StatusCode, replayed.Body)
	}
	var firstResult, replayedResult cancelResponse
	if err := json.Unmarshal(first.Body, &firstResult); err != nil {
		t.Fatalf("decode RUNNING cancellation: %v", err)
	}
	if err := json.Unmarshal(replayed.Body, &replayedResult); err != nil {
		t.Fatalf("decode replayed RUNNING cancellation: %v", err)
	}
	if firstResult.CancellationID == "" || firstResult.CancellationID != replayedResult.CancellationID ||
		firstResult.Decision != "CANCELING" || replayedResult.Decision != "CANCELING" ||
		firstResult.State != "CANCELING" || replayedResult.State != "CANCELING" ||
		firstResult.JobVersion != 4 || replayedResult.JobVersion != firstResult.JobVersion ||
		!firstResult.Billable || !replayedResult.Billable ||
		firstResult.Charge == nil || replayedResult.Charge == nil ||
		!firstResult.DecidedAt.Equal(replayedResult.DecidedAt) {
		t.Fatalf("RUNNING first/replayed cancellation = %#v / %#v", firstResult, replayedResult)
	}
	if firstResult.Charge.ChargeID == "" ||
		firstResult.Charge.ChargeID != replayedResult.Charge.ChargeID ||
		firstResult.Charge.Amount != 1250 || replayedResult.Charge.Amount != 1250 ||
		firstResult.Charge.Currency != "CNY" || replayedResult.Charge.Currency != "CNY" ||
		firstResult.Charge.Reason != "CUSTOMER_CANCELLATION" ||
		replayedResult.Charge.Reason != "CUSTOMER_CANCELLATION" ||
		!firstResult.Charge.PostedAt.Equal(replayedResult.Charge.PostedAt) {
		t.Fatalf("RUNNING first/replayed Charge = %#v / %#v", *firstResult.Charge, *replayedResult.Charge)
	}

	var (
		jobState, attemptState, workerState, reservationState string
		chargeID, chargeCurrency, chargeReason                string
		previousJobState, storedDecision                      string
		jobVersion, currentFence, projectRunning              int64
		reservedMinor, unsettledPostedMinor, chargeAmount     int64
		ended, revoked, billable                              bool
		billableStartedAt, chargePostedAt                     time.Time
		decisionCount, chargeCount                            int64
		cancelRequestedEvents, cancelingEvents                int64
		chargePostedEvents, invoiceExportEvents               int64
		canceledEvents                                        int64
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state,
			j.version,
			j.current_fence,
			j.billable_started_at,
			a.state,
			a.ended_at IS NOT NULL,
			l.revoked_at IS NOT NULL,
			w.lifecycle_state,
			p.running_count,
			cr.state,
			oca.reserved_minor,
			oca.unsettled_posted_minor,
			d.previous_job_state,
			d.decision,
			d.billable,
			c.id::text,
			c.amount_minor,
			c.currency,
			c.reason,
			c.posted_at,
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = j.id),
			(SELECT count(*) FROM charges WHERE job_id = j.id),
			(SELECT count(*) FROM outbox_events WHERE event_type = 'job.cancel_requested'),
			(SELECT count(*) FROM outbox_events WHERE event_type = 'job.canceling'),
			(SELECT count(*) FROM outbox_events WHERE event_type = 'charge.posted'),
			(SELECT count(*) FROM outbox_events WHERE event_type = 'invoice.export_requested'),
			(SELECT count(*) FROM outbox_events WHERE event_type = 'job.canceled')
		FROM jobs AS j
		JOIN attempts AS a ON a.job_id = j.id
		JOIN attempt_leases AS l ON l.attempt_id = a.id
		JOIN workers AS w ON w.id = a.worker_id
		JOIN projects AS p ON p.id = j.project_id
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		JOIN organization_credit_accounts AS oca ON oca.organization_id = j.organization_id
		JOIN job_cancellation_decisions AS d ON d.job_id = j.id
		JOIN charges AS c ON c.job_id = j.id
		WHERE j.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobState,
		&jobVersion,
		&currentFence,
		&billableStartedAt,
		&attemptState,
		&ended,
		&revoked,
		&workerState,
		&projectRunning,
		&reservationState,
		&reservedMinor,
		&unsettledPostedMinor,
		&previousJobState,
		&storedDecision,
		&billable,
		&chargeID,
		&chargeAmount,
		&chargeCurrency,
		&chargeReason,
		&chargePostedAt,
		&decisionCount,
		&chargeCount,
		&cancelRequestedEvents,
		&cancelingEvents,
		&chargePostedEvents,
		&invoiceExportEvents,
		&canceledEvents,
	); err != nil {
		t.Fatalf("read RUNNING cancellation state: %v", err)
	}
	if jobState != "CANCELING" || jobVersion != 4 ||
		currentFence != fixture.assignment.LeaseFence+1 || billableStartedAt.IsZero() ||
		attemptState != "CANCELED" || !ended || !revoked || workerState != "DRAINING" ||
		projectRunning != 0 || reservationState != "CONSUMED" || reservedMinor != 0 ||
		unsettledPostedMinor != 1250 || previousJobState != "RUNNING" ||
		storedDecision != "CANCELING" || !billable ||
		chargeID != firstResult.Charge.ChargeID || chargeAmount != 1250 ||
		chargeCurrency != "CNY" || chargeReason != "CUSTOMER_CANCELLATION" ||
		!chargePostedAt.Equal(firstResult.Charge.PostedAt) ||
		decisionCount != 1 || chargeCount != 1 || cancelRequestedEvents != 1 ||
		cancelingEvents != 1 || chargePostedEvents != 1 || invoiceExportEvents != 1 ||
		canceledEvents != 0 {
		t.Fatalf(
			"RUNNING cancellation state = job %s/%d/%d attempt %s ended/revoked %t/%t worker %s running %d reservation %s credit %d/%d decision %s/%s/%t charge %s/%d/%s/%s events %d/%d/%d/%d/%d/%d/%d",
			jobState,
			jobVersion,
			currentFence,
			attemptState,
			ended,
			revoked,
			workerState,
			projectRunning,
			reservationState,
			reservedMinor,
			unsettledPostedMinor,
			previousJobState,
			storedDecision,
			billable,
			chargeID,
			chargeAmount,
			chargeCurrency,
			chargeReason,
			decisionCount,
			chargeCount,
			cancelRequestedEvents,
			cancelingEvents,
			chargePostedEvents,
			invoiceExportEvents,
			canceledEvents,
		)
	}
}

func TestFinalizingJobCancellationPostsFullQuote(t *testing.T) {
	fixture := newStartFixture(t, "cancel-finalizing-charge", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin FINALIZING fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`
		UPDATE attempt_leases
		SET revoked_at = clock_timestamp(), updated_at = clock_timestamp()
		WHERE attempt_id = $1
		  AND phase = 'EXECUTION'
		  AND owner_kind = 'WORKER'
	`, fixture.assignment.AttemptID); err != nil {
		t.Fatalf("revoke EXECUTION Lease before FINALIZATION takeover: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE attempts SET state = 'FINALIZING', updated_at = clock_timestamp()
		WHERE id = $1
	`, fixture.assignment.AttemptID); err != nil {
		t.Fatalf("move Attempt to FINALIZING: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE jobs SET state = 'FINALIZING', version = version + 1, updated_at = clock_timestamp()
		WHERE id = $1
	`, fixture.assignment.JobID); err != nil {
		t.Fatalf("move Job to FINALIZING: %v", err)
	}
	finalizationLeaseID := uuid.New()
	const finalizationOwnerID = "spiffe://vela.internal/reconciler/cancel-finalizing-charge"
	var finalizationLeaseExpiry time.Time
	if err := tx.QueryRow(`
		INSERT INTO attempt_leases (
			id, organization_id, project_id, attempt_id, worker_id, worker_epoch,
			phase, owner_kind, owner_id, fence, token_digest, signing_key_id,
			issued_at, expires_at
		)
		SELECT
			$1, organization_id, project_id, id, worker_id, worker_epoch,
			'FINALIZATION', 'RECONCILER',
			$3, fence,
			decode(repeat('ab', 32), 'hex'), 'lease-key-v1',
			clock_timestamp(), clock_timestamp() + interval '2 seconds'
		FROM attempts
		WHERE id = $2
		RETURNING expires_at
	`, finalizationLeaseID, fixture.assignment.AttemptID, finalizationOwnerID).Scan(&finalizationLeaseExpiry); err != nil {
		t.Fatalf("insert Reconciler FINALIZATION Lease: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit FINALIZING fixture: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)

	canceled := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel FINALIZING status = %d, want 200; body=%s", canceled.StatusCode, canceled.Body)
	}
	var result cancelResponse
	if err := json.Unmarshal(canceled.Body, &result); err != nil {
		t.Fatalf("decode FINALIZING cancellation: %v", err)
	}
	if result.Decision != "CANCELING" || result.State != "CANCELING" ||
		result.JobVersion != 5 || !result.Billable || result.Charge == nil ||
		result.Charge.Amount != 1250 || result.Charge.Currency != "CNY" ||
		result.Charge.Reason != "CUSTOMER_CANCELLATION" {
		t.Fatalf("FINALIZING cancellation = %#v", result)
	}
	var cancelRequestedPayload []byte
	if err := fixture.database.Admin.QueryRow(`
		SELECT payload
		FROM outbox_events
		WHERE aggregate_id = $1 AND event_type = 'job.cancel_requested'
	`, fixture.assignment.JobID).Scan(&cancelRequestedPayload); err != nil {
		t.Fatalf("read FINALIZING cancel-requested payload: %v", err)
	}
	for label, value := range map[string]string{
		"Lease id":   finalizationLeaseID.String(),
		"phase":      "FINALIZATION",
		"owner kind": "RECONCILER",
		"owner id":   finalizationOwnerID,
	} {
		if !bytes.Contains(cancelRequestedPayload, []byte(value)) {
			t.Fatalf("FINALIZING cancel-requested payload lacks authority %s %q", label, value)
		}
	}
	var cancelRequestedEnvelope velav1.EventEnvelope
	if err := proto.Unmarshal(cancelRequestedPayload, &cancelRequestedEnvelope); err != nil {
		t.Fatalf("decode FINALIZING cancel-requested payload: %v", err)
	}
	requested := cancelRequestedEnvelope.GetJobCancelRequested()
	if requested == nil || requested.GetAuthorityLeaseId() != finalizationLeaseID.String() ||
		requested.GetAuthorityLeasePhase() != "FINALIZATION" ||
		requested.GetAuthorityLeaseOwnerKind() != "RECONCILER" ||
		requested.GetAuthorityLeaseOwnerId() != finalizationOwnerID ||
		requested.GetAuthorityLeaseExpiresAt() == nil ||
		!requested.GetAuthorityLeaseExpiresAt().AsTime().Equal(finalizationLeaseExpiry) {
		t.Fatalf("FINALIZING cancel-requested authority = %#v", requested)
	}

	var (
		previousState, reservationState, leasePhase      string
		leaseOwnerKind                                   string
		activeLeaseID                                    uuid.UUID
		activeLeaseRevoked                               bool
		decisionLeaseID                                  uuid.UUID
		decisionLeasePhase, decisionLeaseOwnerKind       string
		decisionLeaseOwnerID                             string
		decisionLeaseExpiry                              time.Time
		reservedMinor, unsettledPostedMinor              int64
		chargeCount, cancelingEvents, chargePostedEvents int64
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			d.previous_job_state,
			cr.state,
			oca.reserved_minor,
			oca.unsettled_posted_minor,
			l.id,
			l.phase,
			l.owner_kind,
			l.revoked_at IS NOT NULL,
			d.authority_lease_id,
				d.authority_lease_phase,
				d.authority_lease_owner_kind,
				d.authority_lease_owner_id,
				d.authority_lease_expires_at,
			(SELECT count(*) FROM charges WHERE job_id = j.id),
			(SELECT count(*) FROM outbox_events WHERE event_type = 'job.canceling'),
			(SELECT count(*) FROM outbox_events WHERE event_type = 'charge.posted')
		FROM jobs AS j
		JOIN job_cancellation_decisions AS d ON d.job_id = j.id
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		JOIN organization_credit_accounts AS oca ON oca.organization_id = j.organization_id
		JOIN attempt_leases AS l
		  ON l.attempt_id = d.attempt_id
		 AND l.phase = 'FINALIZATION'
		 AND l.owner_kind = 'RECONCILER'
		WHERE j.id = $1
	`, fixture.assignment.JobID).Scan(
		&previousState,
		&reservationState,
		&reservedMinor,
		&unsettledPostedMinor,
		&activeLeaseID,
		&leasePhase,
		&leaseOwnerKind,
		&activeLeaseRevoked,
		&decisionLeaseID,
		&decisionLeasePhase,
		&decisionLeaseOwnerKind,
		&decisionLeaseOwnerID,
		&decisionLeaseExpiry,
		&chargeCount,
		&cancelingEvents,
		&chargePostedEvents,
	); err != nil {
		t.Fatalf("read FINALIZING cancellation state: %v", err)
	}
	if previousState != "FINALIZING" || reservationState != "CONSUMED" ||
		reservedMinor != 0 || unsettledPostedMinor != 1250 || chargeCount != 1 ||
		cancelingEvents != 1 || chargePostedEvents != 1 ||
		activeLeaseID != finalizationLeaseID || leasePhase != "FINALIZATION" ||
		leaseOwnerKind != "RECONCILER" || !activeLeaseRevoked ||
		decisionLeaseID != finalizationLeaseID || decisionLeasePhase != "FINALIZATION" ||
		decisionLeaseOwnerKind != "RECONCILER" || decisionLeaseOwnerID != finalizationOwnerID ||
		!decisionLeaseExpiry.Equal(finalizationLeaseExpiry) {
		t.Fatalf(
			"FINALIZING cancellation state = previous %s reservation %s credit %d/%d Lease %s/%s/%s revoked=%t decision Lease %s/%s/%s/%s/%s charge/events %d/%d/%d",
			previousState,
			reservationState,
			reservedMinor,
			unsettledPostedMinor,
			activeLeaseID,
			leasePhase,
			leaseOwnerKind,
			activeLeaseRevoked,
			decisionLeaseID,
			decisionLeasePhase,
			decisionLeaseOwnerKind,
			decisionLeaseOwnerID,
			decisionLeaseExpiry,
			chargeCount,
			cancelingEvents,
			chargePostedEvents,
		)
	}
	waitForDatabaseTimeAfter(t, fixture.database.Admin, finalizationLeaseExpiry)
	coordinator := cancellation.NewService(
		newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password"),
		newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password"),
	)
	reconciled, err := coordinator.ReconcileNextCancellationStop(context.Background())
	if err != nil {
		t.Fatalf("reconcile expired Reconciler FINALIZATION Lease: %v", err)
	}
	if reconciled.Decision != cancellation.StopReconciled ||
		reconciled.CancellationID != uuid.MustParse(result.CancellationID) ||
		reconciled.JobID != fixture.assignment.JobID || reconciled.State != "CANCELED" ||
		reconciled.JobVersion != 6 || reconciled.StoppedAt.IsZero() {
		t.Fatalf("reconciled FINALIZATION cancellation stop = %#v", reconciled)
	}
}

func TestWorkerOwnedFinalizationCancellationAcknowledgesStop(t *testing.T) {
	fixture := newStartFixture(t, "cancel-finalizing-worker-ack", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Worker FINALIZATION fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`
		UPDATE attempt_leases
		SET phase = 'FINALIZATION', updated_at = clock_timestamp()
		WHERE attempt_id = $1
		  AND phase = 'EXECUTION'
		  AND owner_kind = 'WORKER'
		  AND revoked_at IS NULL
	`, fixture.assignment.AttemptID); err != nil {
		t.Fatalf("switch Worker Lease to FINALIZATION: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE attempts SET state = 'FINALIZING', updated_at = clock_timestamp()
		WHERE id = $1
	`, fixture.assignment.AttemptID); err != nil {
		t.Fatalf("move Worker Attempt to FINALIZING: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE jobs SET state = 'FINALIZING', version = version + 1, updated_at = clock_timestamp()
		WHERE id = $1
	`, fixture.assignment.JobID); err != nil {
		t.Fatalf("move Worker Job to FINALIZING: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit Worker FINALIZATION fixture: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	canceled := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel Worker FINALIZING status = %d, want 200; body=%s", canceled.StatusCode, canceled.Body)
	}
	var cancellationResult cancelResponse
	if err := json.Unmarshal(canceled.Body, &cancellationResult); err != nil {
		t.Fatalf("decode Worker FINALIZING cancellation: %v", err)
	}
	if cancellationResult.Decision != "CANCELING" || cancellationResult.JobVersion != 5 ||
		cancellationResult.Charge == nil {
		t.Fatalf("Worker FINALIZING cancellation = %#v", cancellationResult)
	}

	coordinator := cancellation.NewService(
		newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password"),
		newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password"),
	)
	acknowledged, err := coordinator.AcknowledgeCancellationStop(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		uuid.MustParse(cancellationResult.CancellationID),
	)
	if err != nil {
		t.Fatalf("acknowledge Worker FINALIZATION cancellation stop: %v", err)
	}
	if acknowledged.Decision != cancellation.StopAcknowledged ||
		acknowledged.State != "CANCELED" || acknowledged.JobVersion != 6 ||
		acknowledged.Source != "ACKNOWLEDGED" || acknowledged.StoppedAt.IsZero() {
		t.Fatalf("Worker FINALIZATION stop acknowledgement = %#v", acknowledged)
	}

	var jobState, workerState, leasePhase, leaseOwnerKind, receiptSource string
	var receipts, canceledEvents int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state,
			w.lifecycle_state,
			l.phase,
			l.owner_kind,
			r.source,
			(SELECT count(*) FROM cancellation_stop_receipts WHERE cancellation_id = d.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id AND event_type = 'job.canceled')
		FROM jobs AS j
		JOIN job_cancellation_decisions AS d ON d.job_id = j.id
		JOIN attempts AS a ON a.id = d.attempt_id
		JOIN attempt_leases AS l ON l.id = d.authority_lease_id
		JOIN workers AS w ON w.id = d.worker_id
		JOIN cancellation_stop_receipts AS r ON r.cancellation_id = d.id
		WHERE j.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobState,
		&workerState,
		&leasePhase,
		&leaseOwnerKind,
		&receiptSource,
		&receipts,
		&canceledEvents,
	); err != nil {
		t.Fatalf("read acknowledged Worker FINALIZATION cancellation: %v", err)
	}
	if jobState != "CANCELED" || workerState != "READY" ||
		leasePhase != "FINALIZATION" || leaseOwnerKind != "WORKER" ||
		receiptSource != "ACKNOWLEDGED" || receipts != 1 || canceledEvents != 1 {
		t.Fatalf(
			"acknowledged Worker FINALIZATION state = job %s worker %s Lease %s/%s receipt %s/%d events %d",
			jobState,
			workerState,
			leasePhase,
			leaseOwnerKind,
			receiptSource,
			receipts,
			canceledEvents,
		)
	}
}

func TestAcknowledgeCancellationStopTerminalizesOnceAndReplaysReceipt(t *testing.T) {
	fixture := newStartFixture(t, "cancel-stop-ack", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	canceled := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel RUNNING status = %d, want 200; body=%s", canceled.StatusCode, canceled.Body)
	}
	var cancellationResult cancelResponse
	if err := json.Unmarshal(canceled.Body, &cancellationResult); err != nil {
		t.Fatalf("decode cancellation before stop acknowledgement: %v", err)
	}
	if cancellationResult.Decision != "CANCELING" || cancellationResult.Charge == nil {
		t.Fatalf("cancellation before stop acknowledgement = %#v", cancellationResult)
	}

	cancelPool := newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password")
	internalPool := newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password")
	coordinator := cancellation.NewService(cancelPool, internalPool)
	cancellationID := uuid.MustParse(cancellationResult.CancellationID)
	first, err := coordinator.AcknowledgeCancellationStop(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		cancellationID,
	)
	if err != nil {
		t.Fatalf("acknowledge cancellation stop: %v", err)
	}
	if first.Decision != cancellation.StopAcknowledged || first.ReceiptID == uuid.Nil ||
		first.CancellationID != cancellationID || first.JobID != fixture.assignment.JobID ||
		first.State != "CANCELED" || first.JobVersion != 5 || first.StoppedAt.IsZero() {
		t.Fatalf("stop acknowledgement = %#v", first)
	}
	replayed, err := coordinator.AcknowledgeCancellationStop(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		cancellationID,
	)
	if err != nil {
		t.Fatalf("replay cancellation stop acknowledgement: %v", err)
	}
	if !reflect.DeepEqual(replayed, first) {
		t.Fatalf("replayed stop acknowledgement = %#v, want %#v", replayed, first)
	}

	var (
		jobState, workerState, reservationState string
		jobVersion, receiptCount                int64
		chargeCount, canceledEvents             int64
		reservedMinor, unsettledPostedMinor     int64
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state,
			j.version,
			w.lifecycle_state,
			cr.state,
			oca.reserved_minor,
			oca.unsettled_posted_minor,
			(SELECT count(*) FROM cancellation_stop_receipts WHERE cancellation_id = d.id),
			(SELECT count(*) FROM charges WHERE cancellation_id = d.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id AND event_type = 'job.canceled')
		FROM jobs AS j
		JOIN job_cancellation_decisions AS d ON d.job_id = j.id
		JOIN attempts AS a ON a.id = d.attempt_id
		JOIN workers AS w ON w.id = a.worker_id
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		JOIN organization_credit_accounts AS oca ON oca.organization_id = j.organization_id
		WHERE j.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobState,
		&jobVersion,
		&workerState,
		&reservationState,
		&reservedMinor,
		&unsettledPostedMinor,
		&receiptCount,
		&chargeCount,
		&canceledEvents,
	); err != nil {
		t.Fatalf("read acknowledged cancellation state: %v", err)
	}
	if jobState != "CANCELED" || jobVersion != 5 || workerState != "READY" ||
		reservationState != "CONSUMED" || reservedMinor != 0 || unsettledPostedMinor != 1250 ||
		receiptCount != 1 || chargeCount != 1 || canceledEvents != 1 {
		t.Fatalf(
			"acknowledged cancellation state = job %s/%d worker %s reservation %s credit %d/%d receipts/charges/events %d/%d/%d",
			jobState,
			jobVersion,
			workerState,
			reservationState,
			reservedMinor,
			unsettledPostedMinor,
			receiptCount,
			chargeCount,
			canceledEvents,
		)
	}
}

func TestCancellationOutboxPayloadsAreTypedAndExcludeSensitiveData(t *testing.T) {
	fixture := newStartFixture(t, "cancel-outbox-contract", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	if _, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil {
		t.Fatalf("start cancellation Outbox fixture: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	canceled := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel Outbox fixture status = %d; body=%s", canceled.StatusCode, canceled.Body)
	}
	var cancellationResult cancelResponse
	if err := json.Unmarshal(canceled.Body, &cancellationResult); err != nil {
		t.Fatalf("decode Outbox cancellation response: %v", err)
	}
	coordinator := cancellation.NewService(
		newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password"),
		newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password"),
	)
	stopped, err := coordinator.AcknowledgeCancellationStop(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		uuid.MustParse(cancellationResult.CancellationID),
	)
	if err != nil {
		t.Fatalf("acknowledge Outbox cancellation stop: %v", err)
	}

	rows, err := fixture.database.Admin.Query(`
		SELECT event_type, payload
		FROM outbox_events
		WHERE aggregate_id = $1
		  AND event_type IN (
			'job.cancel_requested',
			'job.canceling',
			'charge.posted',
			'invoice.export_requested',
			'job.canceled'
		  )
	`, fixture.assignment.JobID)
	if err != nil {
		t.Fatalf("query cancellation Outbox payloads: %v", err)
	}
	defer rows.Close()

	seen := make(map[string]bool)
	for rows.Next() {
		var eventType string
		var payload []byte
		if err := rows.Scan(&eventType, &payload); err != nil {
			t.Fatalf("scan %s Outbox payload: %v", eventType, err)
		}
		if seen[eventType] {
			t.Fatalf("duplicate %s Outbox event", eventType)
		}
		seen[eventType] = true
		for label, sensitive := range map[string]string{
			"Customer Content": "exercise Assignment rejection invariants",
			"Lease token":      fixture.credentials.Token,
		} {
			if bytes.Contains(payload, []byte(sensitive)) {
				t.Fatalf("%s payload contains %s", eventType, label)
			}
		}
		if eventType != "job.cancel_requested" &&
			bytes.Contains(payload, []byte(fixture.worker.ID.String())) {
			t.Fatalf("customer-facing %s payload contains Worker identity", eventType)
		}

		var envelope velav1.EventEnvelope
		if err := proto.Unmarshal(payload, &envelope); err != nil {
			t.Fatalf("decode %s payload: %v", eventType, err)
		}
		if envelope.GetEventType() != eventType || envelope.GetAggregateType() != "Job" ||
			envelope.GetAggregateId() != fixture.assignment.JobID.String() ||
			envelope.GetSchemaVersion() != 1 || envelope.GetOccurredAt() == nil {
			t.Fatalf("%s envelope = %#v", eventType, &envelope)
		}

		switch eventType {
		case "job.cancel_requested":
			requested := envelope.GetJobCancelRequested()
			if requested == nil || envelope.GetAggregateVersion() != 4 ||
				requested.GetCancellationId() != cancellationResult.CancellationID ||
				requested.GetAttemptId() != fixture.assignment.AttemptID.String() ||
				requested.GetWorkerId() != fixture.worker.ID.String() ||
				requested.GetWorkerEpoch() != 7 || requested.GetAttemptFence() != 1 ||
				requested.GetCancellationFence() != 2 || requested.GetDecidedAt() == nil ||
				!requested.GetDecidedAt().AsTime().Equal(cancellationResult.DecidedAt) {
				t.Fatalf("job.cancel_requested typed payload = %#v", requested)
			}
		case "job.canceling":
			canceling := envelope.GetJobCanceling()
			if canceling == nil || envelope.GetAggregateVersion() != 4 ||
				canceling.GetCancellationId() != cancellationResult.CancellationID ||
				canceling.GetChargeId() != cancellationResult.Charge.ChargeID ||
				canceling.GetCancellationFence() != 2 || canceling.GetDecidedAt() == nil ||
				!canceling.GetDecidedAt().AsTime().Equal(cancellationResult.DecidedAt) {
				t.Fatalf("job.canceling typed payload = %#v", canceling)
			}
		case "charge.posted":
			posted := envelope.GetChargePosted()
			if posted == nil || envelope.GetAggregateVersion() != 4 ||
				posted.GetChargeId() != cancellationResult.Charge.ChargeID ||
				posted.GetCancellationId() != cancellationResult.CancellationID ||
				posted.GetAmountMinor() != 1250 || posted.GetCurrency() != "CNY" ||
				posted.GetReason() != "CUSTOMER_CANCELLATION" || posted.GetPostedAt() == nil ||
				!posted.GetPostedAt().AsTime().Equal(cancellationResult.Charge.PostedAt) {
				t.Fatalf("charge.posted typed payload = %#v", posted)
			}
		case "invoice.export_requested":
			export := envelope.GetInvoiceExportRequested()
			if export == nil || envelope.GetAggregateVersion() != 4 ||
				export.GetChargeId() != cancellationResult.Charge.ChargeID ||
				export.GetRequestedAt() == nil ||
				!export.GetRequestedAt().AsTime().Equal(cancellationResult.DecidedAt) {
				t.Fatalf("invoice.export_requested typed payload = %#v", export)
			}
		case "job.canceled":
			terminal := envelope.GetJobCanceled()
			if terminal == nil || envelope.GetAggregateVersion() != 5 ||
				terminal.GetCancellationId() != cancellationResult.CancellationID ||
				terminal.GetJobFence() != 2 || !terminal.GetBillable() ||
				terminal.GetChargeId() != cancellationResult.Charge.ChargeID ||
				terminal.GetDecidedAt() == nil ||
				!terminal.GetDecidedAt().AsTime().Equal(cancellationResult.DecidedAt) ||
				!envelope.GetOccurredAt().AsTime().Equal(stopped.StoppedAt) {
				t.Fatalf("job.canceled typed payload = %#v envelope=%#v", terminal, &envelope)
			}
		default:
			t.Fatalf("unexpected cancellation event type %q", eventType)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate cancellation Outbox payloads: %v", err)
	}
	for _, eventType := range []string{
		"job.cancel_requested",
		"job.canceling",
		"charge.posted",
		"invoice.export_requested",
		"job.canceled",
	} {
		if !seen[eventType] {
			t.Fatalf("missing %s Outbox event", eventType)
		}
	}
}

func TestCancellationDecisionChargeAndStopReceiptAreImmutable(t *testing.T) {
	fixture := newStartFixture(t, "cancel-immutable-evidence", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	if _, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil {
		t.Fatalf("start immutable cancellation fixture: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	canceled := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel immutable fixture status = %d; body=%s", canceled.StatusCode, canceled.Body)
	}
	var cancellationResult cancelResponse
	if err := json.Unmarshal(canceled.Body, &cancellationResult); err != nil {
		t.Fatalf("decode immutable cancellation response: %v", err)
	}
	coordinator := cancellation.NewService(
		newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password"),
		newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password"),
	)
	if _, err := coordinator.AcknowledgeCancellationStop(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		uuid.MustParse(cancellationResult.CancellationID),
	); err != nil {
		t.Fatalf("acknowledge immutable cancellation stop: %v", err)
	}

	for name, statements := range map[string][]string{
		"Customer Cancellation decision": {
			"UPDATE job_cancellation_decisions SET created_at = created_at WHERE job_id = $1",
			"DELETE FROM job_cancellation_decisions WHERE job_id = $1",
		},
		"Charge": {
			"UPDATE charges SET created_at = created_at WHERE job_id = $1",
			"DELETE FROM charges WHERE job_id = $1",
		},
		"stop receipt": {
			"UPDATE cancellation_stop_receipts SET created_at = created_at WHERE job_id = $1",
			"DELETE FROM cancellation_stop_receipts WHERE job_id = $1",
		},
	} {
		for _, statement := range statements {
			if _, err := fixture.database.Admin.Exec(statement, fixture.assignment.JobID); err == nil {
				t.Fatalf("immutable %s accepted %q", name, statement)
			} else {
				var postgresError *pgconn.PgError
				if !errors.As(err, &postgresError) || postgresError.Code != "P0001" {
					t.Fatalf("%s mutation error = %v, want SQLSTATE P0001", name, err)
				}
			}
		}
	}

	var decisions, charges, receipts int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = $1),
			(SELECT count(*) FROM charges WHERE job_id = $1),
			(SELECT count(*) FROM cancellation_stop_receipts WHERE job_id = $1)
	`, fixture.assignment.JobID).Scan(&decisions, &charges, &receipts); err != nil {
		t.Fatalf("count immutable cancellation evidence: %v", err)
	}
	if decisions != 1 || charges != 1 || receipts != 1 {
		t.Fatalf("immutable evidence counts = decisions %d charges %d receipts %d", decisions, charges, receipts)
	}
}

func TestReconcileCancellationStopWaitsForPostgresLeaseExpiry(t *testing.T) {
	fixture := newStartFixture(t, "cancel-stop-reconcile", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	var originalLeaseExpiry time.Time
	if err := fixture.database.Admin.QueryRow(`
		UPDATE attempt_leases
		SET expires_at = clock_timestamp() + interval '3 seconds',
			updated_at = clock_timestamp()
		WHERE attempt_id = $1
		  AND phase = 'EXECUTION'
		  AND owner_kind = 'WORKER'
		  AND revoked_at IS NULL
		RETURNING expires_at
	`, fixture.assignment.AttemptID).Scan(&originalLeaseExpiry); err != nil {
		t.Fatalf("set original cancellation Lease expiry: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	canceled := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel RUNNING status = %d, want 200; body=%s", canceled.StatusCode, canceled.Body)
	}
	var cancellationResult cancelResponse
	if err := json.Unmarshal(canceled.Body, &cancellationResult); err != nil {
		t.Fatalf("decode cancellation before reconciliation: %v", err)
	}

	cancelPool := newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password")
	internalPool := newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password")
	coordinator := cancellation.NewService(cancelPool, internalPool)
	beforeExpiry, err := coordinator.ReconcileNextCancellationStop(context.Background())
	if err != nil {
		t.Fatalf("reconcile before Lease expiry: %v", err)
	}
	if beforeExpiry.Decision != cancellation.StopNoWork {
		t.Fatalf("reconcile before Lease expiry = %#v, want NO_WORK", beforeExpiry)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE attempt_leases
		SET expires_at = issued_at + interval '1 microsecond', updated_at = clock_timestamp()
		WHERE attempt_id = $1 AND revoked_at IS NOT NULL
	`, fixture.assignment.AttemptID); err != nil {
		t.Fatalf("tamper with revoked Lease expiry: %v", err)
	}
	tamperedEarly, err := coordinator.ReconcileNextCancellationStop(context.Background())
	if err != nil {
		t.Fatalf("reconcile before original Lease expiry after row tampering: %v", err)
	}
	if tamperedEarly.Decision != cancellation.StopNoWork {
		t.Fatalf(
			"reconcile before original Lease expiry after row tampering = %#v, want NO_WORK",
			tamperedEarly,
		)
	}
	waitForDatabaseTimeAfter(t, fixture.database.Admin, originalLeaseExpiry)

	reconciled, err := coordinator.ReconcileNextCancellationStop(context.Background())
	if err != nil {
		t.Fatalf("reconcile expired cancellation stop: %v", err)
	}
	if reconciled.Decision != cancellation.StopReconciled || reconciled.ReceiptID == uuid.Nil ||
		reconciled.CancellationID != uuid.MustParse(cancellationResult.CancellationID) ||
		reconciled.JobID != fixture.assignment.JobID || reconciled.State != "CANCELED" ||
		reconciled.JobVersion != 5 ||
		reconciled.Source != "LEASE_EXPIRED_RECONCILIATION" || reconciled.StoppedAt.IsZero() {
		t.Fatalf("reconciled cancellation stop = %#v", reconciled)
	}
	replayed, err := coordinator.ReconcileNextCancellationStop(context.Background())
	if err != nil {
		t.Fatalf("reconcile after receipt: %v", err)
	}
	if replayed.Decision != cancellation.StopNoWork {
		t.Fatalf("reconcile after receipt = %#v, want NO_WORK", replayed)
	}
	acknowledgementReplay, err := coordinator.AcknowledgeCancellationStop(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		uuid.MustParse(cancellationResult.CancellationID),
	)
	if err != nil {
		t.Fatalf("replay reconciled stop through acknowledgement seam: %v", err)
	}
	if !reflect.DeepEqual(acknowledgementReplay, reconciled) {
		t.Fatalf(
			"acknowledgement replay of reconciled stop = %#v, want %#v",
			acknowledgementReplay,
			reconciled,
		)
	}

	var jobState, workerState, receiptSource string
	var jobVersion, receiptCount, canceledEvents int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state,
			j.version,
			w.lifecycle_state,
			r.source,
			(SELECT count(*) FROM cancellation_stop_receipts WHERE cancellation_id = d.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id AND event_type = 'job.canceled')
		FROM jobs AS j
		JOIN job_cancellation_decisions AS d ON d.job_id = j.id
		JOIN cancellation_stop_receipts AS r ON r.cancellation_id = d.id
		JOIN workers AS w ON w.id = d.worker_id
		WHERE j.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobState,
		&jobVersion,
		&workerState,
		&receiptSource,
		&receiptCount,
		&canceledEvents,
	); err != nil {
		t.Fatalf("read reconciled cancellation state: %v", err)
	}
	if jobState != "CANCELED" || jobVersion != 5 || workerState != "DRAINING" ||
		receiptSource != "LEASE_EXPIRED_RECONCILIATION" || receiptCount != 1 ||
		canceledEvents != 1 {
		t.Fatalf(
			"reconciled cancellation state = job %s/%d worker %s receipt %s/%d events %d",
			jobState,
			jobVersion,
			workerState,
			receiptSource,
			receiptCount,
			canceledEvents,
		)
	}
}

func TestCancellationCredentialChangeAfterAuthenticationFailsClosedWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		name        string
		mutation    string
		wantFailure cancellation.FailureCode
	}{
		{
			name:        "scope removed",
			mutation:    "scopes = ARRAY['jobs:submit', 'jobs:read']",
			wantFailure: cancellation.FailureForbidden,
		},
		{
			name:        "credential revoked",
			mutation:    "revoked_at = clock_timestamp()",
			wantFailure: cancellation.FailureUnauthorized,
		},
		{
			name:        "credential expired",
			mutation:    "expires_at = clock_timestamp() - interval '1 second'",
			wantFailure: cancellation.FailureUnauthorized,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := newPostgres(t)
			applyFoundation(t, database.Admin)
			seedAdmissionFixture(t, database.Admin)
			server := admissionServerForDatabase(t, database)
			accepted := submitJob(t, server.URL, "cancel-credential-changed", []byte(`{
				"model":"minimax-h3",
				"generation_preset":"balanced",
				"service_class":"standard",
				"output_spec":"video-1080p-5s-24fps",
				"generation_count":1,
				"prompt":"credential changes must fail closed in cancellation transaction"
			}`))
			if accepted.StatusCode != http.StatusAccepted {
				t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
			}
			var job jobResponse
			if err := json.Unmarshal(accepted.Body, &job); err != nil {
				t.Fatalf("decode Accepted Job: %v", err)
			}
			if _, err := database.Admin.Exec(`
				UPDATE credentials
				SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
				WHERE id = $1
			`, testCredentialID); err != nil {
				t.Fatalf("grant cancellation scope before authentication: %v", err)
			}
			authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
			principal, err := identity.NewAuthenticator(authPool, testCredentialPepper).Authenticate(
				context.Background(), testBearerCredential(),
			)
			if err != nil {
				t.Fatalf("authenticate before credential mutation: %v", err)
			}
			if _, err := database.Admin.Exec(
				"UPDATE credentials SET "+test.mutation+" WHERE id = $1",
				testCredentialID,
			); err != nil {
				t.Fatalf("mutate authenticated credential: %v", err)
			}
			cancelPool := newRolePool(t, database.DSN, "vela_cancel_login", "vela-cancel-password")
			internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
			_, err = cancellation.NewService(cancelPool, internalPool).Cancel(
				context.Background(),
				principal,
				uuid.MustParse(testProjectID),
				uuid.MustParse(job.JobID),
			)
			var failure *cancellation.Failure
			if !errors.As(err, &failure) || failure.Code != test.wantFailure {
				t.Fatalf("cancellation error = %v, want %s", err, test.wantFailure)
			}

			var jobState, reservationState string
			var jobVersion, projectQueued, poolQueued int64
			var reservedMinor, unsettledPostedMinor int64
			var decisionCount, chargeCount, outboxCount int64
			if err := database.Admin.QueryRow(`
				SELECT
					job.state,
					job.version,
					project.queued_count,
					pool.queued_count,
					reservation.state,
					credit.reserved_minor,
					credit.unsettled_posted_minor,
					(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
					(SELECT count(*) FROM charges WHERE job_id = job.id),
					(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id)
				FROM jobs AS job
				JOIN projects AS project ON project.id = job.project_id
				JOIN worker_pools AS pool ON pool.id = job.worker_pool_id
				JOIN credit_reservations AS reservation ON reservation.job_id = job.id
				JOIN organization_credit_accounts AS credit
				  ON credit.organization_id = job.organization_id
				WHERE job.id = $1
			`, job.JobID).Scan(
				&jobState,
				&jobVersion,
				&projectQueued,
				&poolQueued,
				&reservationState,
				&reservedMinor,
				&unsettledPostedMinor,
				&decisionCount,
				&chargeCount,
				&outboxCount,
			); err != nil {
				t.Fatalf("read rejected cancellation state: %v", err)
			}
			if jobState != "QUEUED" || jobVersion != 1 || projectQueued != 1 || poolQueued != 1 ||
				reservationState != "RESERVED" || reservedMinor != 1250 || unsettledPostedMinor != 0 ||
				decisionCount != 0 || chargeCount != 0 || outboxCount != 1 {
				t.Fatalf(
					"rejected cancellation state = job %s/%d queued %d/%d reservation %s credit %d/%d decisions/charges/outbox %d/%d/%d",
					jobState,
					jobVersion,
					projectQueued,
					poolQueued,
					reservationState,
					reservedMinor,
					unsettledPostedMinor,
					decisionCount,
					chargeCount,
					outboxCount,
				)
			}
		})
	}
}

func TestCancellationHTTPCredentialChangeAfterAuthenticationFailsClosed(t *testing.T) {
	const authenticationPauseLock int64 = 580007
	for _, test := range []struct {
		name       string
		mutation   string
		wantStatus int
		wantCode   string
	}{
		{
			name:       "scope removed",
			mutation:   "scopes = ARRAY['jobs:submit', 'jobs:read']",
			wantStatus: http.StatusForbidden,
			wantCode:   "forbidden",
		},
		{
			name:       "credential revoked",
			mutation:   "revoked_at = clock_timestamp()",
			wantStatus: http.StatusUnauthorized,
			wantCode:   "unauthorized",
		},
		{
			name:       "credential expired",
			mutation:   "expires_at = clock_timestamp() - interval '1 second'",
			wantStatus: http.StatusUnauthorized,
			wantCode:   "unauthorized",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := newPostgres(t)
			applyFoundation(t, database.Admin)
			seedAdmissionFixture(t, database.Admin)
			server := admissionServerForDatabase(t, database)
			accepted := submitJob(t, server.URL, "cancel-http-credential-changed", []byte(`{
				"model":"minimax-h3",
				"generation_preset":"balanced",
				"service_class":"standard",
				"output_spec":"video-1080p-5s-24fps",
				"generation_count":1,
				"prompt":"HTTP cancellation must revalidate authenticated credentials"
			}`))
			if accepted.StatusCode != http.StatusAccepted {
				t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
			}
			var job jobResponse
			if err := json.Unmarshal(accepted.Body, &job); err != nil {
				t.Fatalf("decode Accepted Job: %v", err)
			}
			if _, err := database.Admin.Exec(`
				UPDATE credentials
				SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
				WHERE id = $1
			`, testCredentialID); err != nil {
				t.Fatalf("grant cancellation scope before HTTP authentication: %v", err)
			}
			installPausedCredentialAuthenticator(t, database.Admin, authenticationPauseLock)

			blocker, err := database.Admin.Conn(context.Background())
			if err != nil {
				t.Fatalf("open authentication pause connection: %v", err)
			}
			defer blocker.Close()
			if _, err := blocker.ExecContext(
				context.Background(), "SELECT pg_advisory_lock($1)", authenticationPauseLock,
			); err != nil {
				t.Fatalf("acquire authentication pause lock: %v", err)
			}
			lockHeld := true
			defer func() {
				if lockHeld {
					_, _ = blocker.ExecContext(
						context.Background(), "SELECT pg_advisory_unlock($1)", authenticationPauseLock,
					)
				}
			}()

			type cancelCall struct {
				result httpResult
				err    error
			}
			cancelResult := make(chan cancelCall, 1)
			go func() {
				result, cancelErr := doCancelJob(
					server.URL,
					testProjectID,
					job.JobID,
					testBearerCredential(),
				)
				cancelResult <- cancelCall{result: result, err: cancelErr}
			}()
			waitForRoleDatabaseLock(t, database.Admin, "vela_auth_login")

			if _, err := database.Admin.Exec(
				"UPDATE credentials SET "+test.mutation+" WHERE id = $1",
				testCredentialID,
			); err != nil {
				t.Fatalf("mutate HTTP-authenticated credential: %v", err)
			}
			var unlocked bool
			if err := blocker.QueryRowContext(
				context.Background(), "SELECT pg_advisory_unlock($1)", authenticationPauseLock,
			).Scan(&unlocked); err != nil {
				t.Fatalf("release authentication pause lock: %v", err)
			}
			if !unlocked {
				t.Fatal("authentication pause lock was not held")
			}
			lockHeld = false

			var response httpResult
			select {
			case call := <-cancelResult:
				if call.err != nil {
					t.Fatalf("cancel Job after credential mutation: %v", call.err)
				}
				response = call.result
			case <-time.After(10 * time.Second):
				t.Fatal("HTTP cancellation did not return after authentication resumed")
			}
			if response.StatusCode != test.wantStatus {
				t.Fatalf(
					"post-authentication credential mutation status = %d, want %d; body=%s",
					response.StatusCode,
					test.wantStatus,
					response.Body,
				)
			}
			if contentType := response.Header.Get("Content-Type"); contentType != "application/json" {
				t.Fatalf("post-authentication credential mutation content type = %q", contentType)
			}
			var responseError struct {
				Code string `json:"code"`
			}
			if err := json.Unmarshal(response.Body, &responseError); err != nil {
				t.Fatalf("decode credential mutation response: %v", err)
			}
			if responseError.Code != test.wantCode {
				t.Fatalf("credential mutation error code = %q, want %q", responseError.Code, test.wantCode)
			}

			var jobState, reservationState string
			var jobVersion, projectQueued, poolQueued int64
			var reservedMinor, unsettledPostedMinor int64
			var decisionCount, chargeCount, outboxCount int64
			if err := database.Admin.QueryRow(`
				SELECT
					job.state,
					job.version,
					project.queued_count,
					pool.queued_count,
					reservation.state,
					credit.reserved_minor,
					credit.unsettled_posted_minor,
					(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
					(SELECT count(*) FROM charges WHERE job_id = job.id),
					(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id)
				FROM jobs AS job
				JOIN projects AS project ON project.id = job.project_id
				JOIN worker_pools AS pool ON pool.id = job.worker_pool_id
				JOIN credit_reservations AS reservation ON reservation.job_id = job.id
				JOIN organization_credit_accounts AS credit
				  ON credit.organization_id = job.organization_id
				WHERE job.id = $1
			`, job.JobID).Scan(
				&jobState,
				&jobVersion,
				&projectQueued,
				&poolQueued,
				&reservationState,
				&reservedMinor,
				&unsettledPostedMinor,
				&decisionCount,
				&chargeCount,
				&outboxCount,
			); err != nil {
				t.Fatalf("read HTTP credential mutation state: %v", err)
			}
			if jobState != "QUEUED" || jobVersion != 1 || projectQueued != 1 || poolQueued != 1 ||
				reservationState != "RESERVED" || reservedMinor != 1250 || unsettledPostedMinor != 0 ||
				decisionCount != 0 || chargeCount != 0 || outboxCount != 1 {
				t.Fatalf(
					"HTTP credential mutation state = job %s/%d queued %d/%d reservation %s credit %d/%d decisions/charges/outbox %d/%d/%d",
					jobState,
					jobVersion,
					projectQueued,
					poolQueued,
					reservationState,
					reservedMinor,
					unsettledPostedMinor,
					decisionCount,
					chargeCount,
					outboxCount,
				)
			}
		})
	}
}

func TestCancellationHTTPAuthorizationAndVisibilityFailClosed(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "cancel-http-authorization", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"authorization failures must not mutate cancellation state"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode authorization fixture Job: %v", err)
	}

	missingScope := cancelJob(
		t, server.URL, testProjectID, job.JobID, testBearerCredential(),
	)
	if missingScope.StatusCode != http.StatusForbidden {
		t.Fatalf("missing cancellation scope status = %d, want 403; body=%s", missingScope.StatusCode, missingScope.Body)
	}
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}

	wrongProject := cancelJob(
		t, server.URL, uuid.NewString(), job.JobID, testBearerCredential(),
	)
	if wrongProject.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong Project cancellation status = %d, want 403; body=%s", wrongProject.StatusCode, wrongProject.Body)
	}
	unknownJob := cancelJob(
		t, server.URL, testProjectID, uuid.NewString(), testBearerCredential(),
	)
	if unknownJob.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown Job cancellation status = %d, want 404; body=%s", unknownJob.StatusCode, unknownJob.Body)
	}

	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET expires_at = clock_timestamp() - interval '1 second'
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("expire cancellation credential: %v", err)
	}
	expired := cancelJob(
		t, server.URL, testProjectID, job.JobID, testBearerCredential(),
	)
	if expired.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expired cancellation credential status = %d, want 401; body=%s", expired.StatusCode, expired.Body)
	}
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET expires_at = clock_timestamp() + interval '1 hour',
			revoked_at = clock_timestamp()
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("revoke cancellation credential: %v", err)
	}
	revoked := cancelJob(
		t, server.URL, testProjectID, job.JobID, testBearerCredential(),
	)
	if revoked.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked cancellation credential status = %d, want 401; body=%s", revoked.StatusCode, revoked.Body)
	}

	var jobState, reservationState string
	var queuedCount, reservedMinor, decisionCount, chargeCount int64
	if err := database.Admin.QueryRow(`
		SELECT
			job.state,
			project.queued_count,
			reservation.state,
			credit.reserved_minor,
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
			(SELECT count(*) FROM charges WHERE job_id = job.id)
		FROM jobs AS job
		JOIN projects AS project ON project.id = job.project_id
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN organization_credit_accounts AS credit
		  ON credit.organization_id = job.organization_id
		WHERE job.id = $1
	`, job.JobID).Scan(
		&jobState,
		&queuedCount,
		&reservationState,
		&reservedMinor,
		&decisionCount,
		&chargeCount,
	); err != nil {
		t.Fatalf("read authorization failure state: %v", err)
	}
	if jobState != "QUEUED" || queuedCount != 1 || reservationState != "RESERVED" ||
		reservedMinor != 1250 || decisionCount != 0 || chargeCount != 0 {
		t.Fatalf(
			"authorization failure state = job %s queued %d reservation %s credit %d decisions/charges %d/%d",
			jobState,
			queuedCount,
			reservationState,
			reservedMinor,
			decisionCount,
			chargeCount,
		)
	}
}

func installPausedCredentialAuthenticator(t *testing.T, db *sql.DB, advisoryLockKey int64) {
	t.Helper()
	if _, err := db.Exec(fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION vela_authenticate_service_credential(p_credential_id uuid)
		RETURNS TABLE (
			organization_id uuid,
			project_id uuid,
			principal_id uuid,
			secret_digest bytea,
			scopes text[],
			expires_at timestamptz,
			revoked_at timestamptz
		)
		LANGUAGE plpgsql
		SECURITY DEFINER
		SET search_path = pg_catalog, public
		AS $function$
		DECLARE
			v_organization_id uuid;
			v_project_id uuid;
			v_principal_id uuid;
			v_secret_digest bytea;
			v_scopes text[];
			v_expires_at timestamptz;
			v_revoked_at timestamptz;
		BEGIN
			SELECT
				credential.organization_id,
				credential.project_id,
				credential.principal_id,
				credential.secret_digest,
				credential.scopes,
				credential.expires_at,
				credential.revoked_at
			INTO
				v_organization_id,
				v_project_id,
				v_principal_id,
				v_secret_digest,
				v_scopes,
				v_expires_at,
				v_revoked_at
			FROM public.credentials AS credential
			WHERE credential.id = p_credential_id
			  AND credential.revoked_at IS NULL
			  AND credential.expires_at > clock_timestamp();

			IF FOUND THEN
				PERFORM pg_catalog.pg_advisory_xact_lock(%d);
				RETURN QUERY SELECT
					v_organization_id,
					v_project_id,
					v_principal_id,
					v_secret_digest,
					v_scopes,
					v_expires_at,
					v_revoked_at;
			END IF;
		END
		$function$;
	`, advisoryLockKey)); err != nil {
		t.Fatalf("install paused credential authenticator: %v", err)
	}
}

func TestCancellationStopRejectsMismatchedAuthorityWithoutMutation(t *testing.T) {
	fixture := newStartFixture(t, "cancel-stop-stale-authority", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	canceled := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel RUNNING status = %d, want 200; body=%s", canceled.StatusCode, canceled.Body)
	}
	var cancellationResult cancelResponse
	if err := json.Unmarshal(canceled.Body, &cancellationResult); err != nil {
		t.Fatalf("decode cancellation before stale stop acknowledgement: %v", err)
	}
	cancellationID := uuid.MustParse(cancellationResult.CancellationID)
	cancelPool := newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password")
	internalPool := newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password")
	coordinator := cancellation.NewService(cancelPool, internalPool)

	tests := []struct {
		name           string
		worker         workercontrol.AuthenticatedWorker
		credentials    workercontrol.LeaseCredentials
		cancellationID uuid.UUID
	}{
		{
			name:           "Worker identity",
			worker:         workercontrol.AuthenticatedWorker{ID: uuid.New()},
			credentials:    fixture.credentials,
			cancellationID: cancellationID,
		},
		{
			name:   "Worker epoch",
			worker: fixture.worker,
			credentials: workercontrol.LeaseCredentials{
				AttemptID:   fixture.credentials.AttemptID,
				WorkerEpoch: fixture.credentials.WorkerEpoch + 1,
				Fence:       fixture.credentials.Fence,
				Token:       fixture.credentials.Token,
			},
			cancellationID: cancellationID,
		},
		{
			name:   "Attempt fence",
			worker: fixture.worker,
			credentials: workercontrol.LeaseCredentials{
				AttemptID:   fixture.credentials.AttemptID,
				WorkerEpoch: fixture.credentials.WorkerEpoch,
				Fence:       fixture.credentials.Fence + 1,
				Token:       fixture.credentials.Token,
			},
			cancellationID: cancellationID,
		},
		{
			name:   "Lease token",
			worker: fixture.worker,
			credentials: workercontrol.LeaseCredentials{
				AttemptID:   fixture.credentials.AttemptID,
				WorkerEpoch: fixture.credentials.WorkerEpoch,
				Fence:       fixture.credentials.Fence,
				Token:       "different-revoked-lease-token",
			},
			cancellationID: cancellationID,
		},
		{
			name:           "Cancellation identity",
			worker:         fixture.worker,
			credentials:    fixture.credentials,
			cancellationID: uuid.New(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := coordinator.AcknowledgeCancellationStop(
				context.Background(), test.worker, test.credentials, test.cancellationID,
			)
			if err != nil {
				t.Fatalf("reject mismatched stop authority: %v", err)
			}
			if result.Decision != cancellation.StopRejectedStaleAuthority {
				t.Fatalf("mismatched stop authority result = %#v", result)
			}
		})
	}

	var jobState string
	var receiptCount, canceledEvents, chargeCount int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state,
			(SELECT count(*) FROM cancellation_stop_receipts WHERE cancellation_id = d.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id AND event_type = 'job.canceled'),
			(SELECT count(*) FROM charges WHERE cancellation_id = d.id)
		FROM jobs AS j
		JOIN job_cancellation_decisions AS d ON d.job_id = j.id
		WHERE j.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobState,
		&receiptCount,
		&canceledEvents,
		&chargeCount,
	); err != nil {
		t.Fatalf("read stale stop authority state: %v", err)
	}
	if jobState != "CANCELING" || receiptCount != 0 || canceledEvents != 0 || chargeCount != 1 {
		t.Fatalf(
			"stale stop authority state = job %s receipts/events/charges %d/%d/%d",
			jobState,
			receiptCount,
			canceledEvents,
			chargeCount,
		)
	}
}

func TestFailedJobCancellationReturnsAlreadyFailedWithoutHistory(t *testing.T) {
	fixture := newAssignmentFixture(t, "cancel-already-failed", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	observation := validFailureObservation()
	observation.FailureClass = "FATAL_BACKEND"
	observation.FailureFingerprint = "sglang.invalid.model.revision.cancel"
	failed, err := fixture.service.Fail(
		context.Background(), fixture.worker, leaseCredentials(assignment), observation,
	)
	if err != nil || failed.Disposition != workercontrol.RetryDispositionFailed {
		t.Fatalf("terminal Fail = %#v error=%v", failed, err)
	}
	server := admissionServerForDatabase(t, fixture.database)

	first := cancelJob(
		t,
		server.URL,
		testProjectID,
		assignment.JobID.String(),
		testBearerCredential(),
	)
	replayed := cancelJob(
		t,
		server.URL,
		testProjectID,
		assignment.JobID.String(),
		testBearerCredential(),
	)
	if first.StatusCode != http.StatusOK || replayed.StatusCode != http.StatusOK {
		t.Fatalf(
			"FAILED cancellation statuses = %d/%d, want 200/200; bodies=%s / %s",
			first.StatusCode,
			replayed.StatusCode,
			first.Body,
			replayed.Body,
		)
	}
	var firstResult, replayedResult cancelResponse
	if err := json.Unmarshal(first.Body, &firstResult); err != nil {
		t.Fatalf("decode FAILED cancellation: %v", err)
	}
	if err := json.Unmarshal(replayed.Body, &replayedResult); err != nil {
		t.Fatalf("decode replayed FAILED cancellation: %v", err)
	}
	if firstResult.CancellationID != uuid.Nil.String() ||
		replayedResult.CancellationID != uuid.Nil.String() ||
		firstResult.Decision != "ALREADY_FAILED" || replayedResult.Decision != "ALREADY_FAILED" ||
		firstResult.State != "FAILED" || replayedResult.State != "FAILED" ||
		firstResult.JobVersion != failed.JobVersion || replayedResult.JobVersion != failed.JobVersion ||
		firstResult.Billable || replayedResult.Billable ||
		firstResult.Charge != nil || replayedResult.Charge != nil ||
		!firstResult.DecidedAt.Equal(replayedResult.DecidedAt) {
		t.Fatalf("FAILED first/replayed cancellation = %#v / %#v", firstResult, replayedResult)
	}

	var decisionCount, chargeCount, cancellationEvents int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = $1),
			(SELECT count(*) FROM charges WHERE job_id = $1),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = $1
			   AND event_type IN ('job.cancel_requested', 'job.canceling', 'job.canceled'))
	`, assignment.JobID).Scan(&decisionCount, &chargeCount, &cancellationEvents); err != nil {
		t.Fatalf("read FAILED cancellation history: %v", err)
	}
	if decisionCount != 0 || chargeCount != 0 || cancellationEvents != 0 {
		t.Fatalf(
			"FAILED cancellation history = decisions/charges/events %d/%d/%d",
			decisionCount,
			chargeCount,
			cancellationEvents,
		)
	}
}

func TestLegacyCanceledJobCancellationReturnsTerminalWithoutDecisionHistory(t *testing.T) {
	server, admin := newAdmissionServer(t)
	if _, err := admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	accepted := submitJob(t, server.URL, "cancel-legacy-canceled", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"return an already canceled legacy Job without rewriting history"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode legacy CANCELED Job: %v", err)
	}
	jobID := uuid.MustParse(job.JobID)

	tx, err := admin.Begin()
	if err != nil {
		t.Fatalf("begin legacy CANCELED fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	terminalizeJobWithCanonicalEvent(t, tx, jobID, "CANCELED", nil)
	if _, err := tx.Exec(`
		UPDATE credit_reservations
		SET state = 'RELEASED', updated_at = clock_timestamp()
		WHERE job_id = $1
	`, jobID); err != nil {
		t.Fatalf("release legacy CANCELED reservation: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE projects SET queued_count = queued_count - 1
		WHERE id = $1
	`, testProjectID); err != nil {
		t.Fatalf("release legacy CANCELED Project queue count: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE worker_pools SET queued_count = queued_count - 1
		WHERE id = (SELECT worker_pool_id FROM jobs WHERE id = $1)
	`, jobID); err != nil {
		t.Fatalf("release legacy CANCELED pool queue count: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE organization_credit_accounts
		SET reserved_minor = reserved_minor - 1250,
			version = version + 1,
			updated_at = clock_timestamp()
		WHERE organization_id = $1
	`, testOrganizationID); err != nil {
		t.Fatalf("release legacy CANCELED organization credit: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit legacy CANCELED fixture: %v", err)
	}

	first := cancelJob(t, server.URL, testProjectID, job.JobID, testBearerCredential())
	replayed := cancelJob(t, server.URL, testProjectID, job.JobID, testBearerCredential())
	if first.StatusCode != http.StatusOK || replayed.StatusCode != http.StatusOK {
		t.Fatalf(
			"legacy CANCELED cancellation statuses = %d/%d, want 200/200; bodies=%s / %s",
			first.StatusCode,
			replayed.StatusCode,
			first.Body,
			replayed.Body,
		)
	}
	var firstResult, replayedResult cancelResponse
	if err := json.Unmarshal(first.Body, &firstResult); err != nil {
		t.Fatalf("decode legacy CANCELED response: %v", err)
	}
	if err := json.Unmarshal(replayed.Body, &replayedResult); err != nil {
		t.Fatalf("decode replayed legacy CANCELED response: %v", err)
	}
	if firstResult.CancellationID != uuid.Nil.String() ||
		replayedResult.CancellationID != uuid.Nil.String() ||
		firstResult.Decision != "CANCELED" || replayedResult.Decision != "CANCELED" ||
		firstResult.State != "CANCELED" || replayedResult.State != "CANCELED" ||
		firstResult.JobVersion != 2 || replayedResult.JobVersion != 2 ||
		firstResult.Billable || replayedResult.Billable ||
		firstResult.Charge != nil || replayedResult.Charge != nil ||
		firstResult.DecidedAt.IsZero() || !firstResult.DecidedAt.Equal(replayedResult.DecidedAt) {
		t.Fatalf("legacy CANCELED first/replayed response = %#v / %#v", firstResult, replayedResult)
	}

	var (
		state, reservationState                           string
		version, projectQueued, poolQueued, reservedMinor int64
		decisions, charges, cancellationEvents            int64
	)
	if err := admin.QueryRow(`
		SELECT
			j.state,
			j.version,
			cr.state,
			p.queued_count,
			wp.queued_count,
			oca.reserved_minor,
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = j.id),
			(SELECT count(*) FROM charges WHERE job_id = j.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id
			   AND event_type IN ('job.cancel_requested', 'job.canceling', 'job.canceled'))
		FROM jobs AS j
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		JOIN projects AS p ON p.id = j.project_id
		JOIN worker_pools AS wp ON wp.id = j.worker_pool_id
		JOIN organization_credit_accounts AS oca ON oca.organization_id = j.organization_id
		WHERE j.id = $1
	`, jobID).Scan(
		&state,
		&version,
		&reservationState,
		&projectQueued,
		&poolQueued,
		&reservedMinor,
		&decisions,
		&charges,
		&cancellationEvents,
	); err != nil {
		t.Fatalf("read legacy CANCELED state: %v", err)
	}
	if state != "CANCELED" || version != 2 || reservationState != "RELEASED" ||
		projectQueued != 0 || poolQueued != 0 || reservedMinor != 0 ||
		decisions != 0 || charges != 0 || cancellationEvents != 1 {
		t.Fatalf(
			"legacy CANCELED state = %s/%d reservation %s queue %d/%d credit %d history %d/%d/%d",
			state,
			version,
			reservationState,
			projectQueued,
			poolQueued,
			reservedMinor,
			decisions,
			charges,
			cancellationEvents,
		)
	}
}

func TestLegacyCancelingJobCancellationReturnsCurrentStateWithoutHistory(t *testing.T) {
	fixture := newStartFixture(t, "cancel-legacy-canceling", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE jobs
		SET state = 'CANCELING', version = version + 1, updated_at = clock_timestamp()
		WHERE id = $1
	`, fixture.assignment.JobID); err != nil {
		t.Fatalf("create legacy CANCELING Job: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)

	first := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	replayed := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if first.StatusCode != http.StatusOK || replayed.StatusCode != http.StatusOK {
		t.Fatalf(
			"legacy CANCELING cancellation statuses = %d/%d, want 200/200; bodies=%s / %s",
			first.StatusCode,
			replayed.StatusCode,
			first.Body,
			replayed.Body,
		)
	}
	var firstResult, replayedResult cancelResponse
	if err := json.Unmarshal(first.Body, &firstResult); err != nil {
		t.Fatalf("decode legacy CANCELING response: %v", err)
	}
	if err := json.Unmarshal(replayed.Body, &replayedResult); err != nil {
		t.Fatalf("decode replayed legacy CANCELING response: %v", err)
	}
	if firstResult.CancellationID != uuid.Nil.String() ||
		replayedResult.CancellationID != uuid.Nil.String() ||
		firstResult.Decision != "CANCELING" || replayedResult.Decision != "CANCELING" ||
		firstResult.State != "CANCELING" || replayedResult.State != "CANCELING" ||
		firstResult.JobVersion != 4 || replayedResult.JobVersion != 4 ||
		firstResult.Billable || replayedResult.Billable ||
		firstResult.Charge != nil || replayedResult.Charge != nil ||
		firstResult.DecidedAt.IsZero() || !firstResult.DecidedAt.Equal(replayedResult.DecidedAt) {
		t.Fatalf("legacy CANCELING first/replayed response = %#v / %#v", firstResult, replayedResult)
	}

	var (
		jobState, attemptState, reservationState string
		jobVersion, jobFence                     int64
		leaseRevoked                             bool
		decisions, charges, cancellationEvents   int64
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state,
			j.version,
			j.current_fence,
			a.state,
			l.revoked_at IS NOT NULL,
			cr.state,
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = j.id),
			(SELECT count(*) FROM charges WHERE job_id = j.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id
			   AND event_type IN ('job.cancel_requested', 'job.canceling', 'job.canceled'))
		FROM jobs AS j
		JOIN attempts AS a ON a.job_id = j.id
		JOIN attempt_leases AS l ON l.attempt_id = a.id
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		WHERE j.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobState,
		&jobVersion,
		&jobFence,
		&attemptState,
		&leaseRevoked,
		&reservationState,
		&decisions,
		&charges,
		&cancellationEvents,
	); err != nil {
		t.Fatalf("read legacy CANCELING state: %v", err)
	}
	if jobState != "CANCELING" || jobVersion != 4 ||
		jobFence != fixture.assignment.LeaseFence || attemptState != "RUNNING" ||
		leaseRevoked || reservationState != "RESERVED" ||
		decisions != 0 || charges != 0 || cancellationEvents != 0 {
		t.Fatalf(
			"legacy CANCELING state = job %s/%d/%d attempt %s lease revoked=%t reservation %s history %d/%d/%d",
			jobState,
			jobVersion,
			jobFence,
			attemptState,
			leaseRevoked,
			reservationState,
			decisions,
			charges,
			cancellationEvents,
		)
	}
}

func TestSucceededJobCancellationReturnsAlreadySucceededWithoutSecondCharge(t *testing.T) {
	fixture := newStartFixture(t, "cancel-already-succeeded", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	completionService := visibleCompletionService(t, fixture.database.DSN)
	plan, err := completionService.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t, completionService, fixture.worker, fixture.credentials, plan,
	)
	completed, err := completionService.CompleteVisibleCompletion(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		workercontrol.VisibleCompletionCandidate{
			CompletionID:       uuid.New(),
			ExpectedJobVersion: plan.JobVersion,
			ArtifactIDs:        artifactIDs,
		},
	)
	if err != nil || completed.Decision != workercontrol.VisibleCompletionCommitted {
		t.Fatalf("CompleteVisibleCompletion = %#v error=%v", completed, err)
	}
	server := admissionServerForDatabase(t, fixture.database)

	first := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	replayed := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if first.StatusCode != http.StatusOK || replayed.StatusCode != http.StatusOK {
		t.Fatalf(
			"SUCCEEDED cancellation statuses = %d/%d, want 200/200; bodies=%s / %s",
			first.StatusCode,
			replayed.StatusCode,
			first.Body,
			replayed.Body,
		)
	}
	var firstResult, replayedResult cancelResponse
	if err := json.Unmarshal(first.Body, &firstResult); err != nil {
		t.Fatalf("decode SUCCEEDED cancellation: %v", err)
	}
	if err := json.Unmarshal(replayed.Body, &replayedResult); err != nil {
		t.Fatalf("decode replayed SUCCEEDED cancellation: %v", err)
	}
	if firstResult.CancellationID != uuid.Nil.String() ||
		replayedResult.CancellationID != uuid.Nil.String() ||
		firstResult.Decision != "ALREADY_SUCCEEDED" ||
		replayedResult.Decision != "ALREADY_SUCCEEDED" ||
		firstResult.State != "SUCCEEDED" || replayedResult.State != "SUCCEEDED" ||
		firstResult.JobVersion != 5 || replayedResult.JobVersion != 5 ||
		firstResult.Billable || replayedResult.Billable ||
		firstResult.Charge != nil || replayedResult.Charge != nil ||
		!firstResult.DecidedAt.Equal(replayedResult.DecidedAt) {
		t.Fatalf("SUCCEEDED first/replayed cancellation = %#v / %#v", firstResult, replayedResult)
	}

	var cancellationCount, chargeCount, visibleChargeCount, cancellationEvents int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = $1),
			(SELECT count(*) FROM charges WHERE job_id = $1),
			(SELECT count(*) FROM charges
			 WHERE job_id = $1 AND reason = 'VISIBLE_COMPLETION' AND cancellation_id IS NULL),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = $1
			   AND event_type IN ('job.cancel_requested', 'job.canceling', 'job.canceled'))
	`, fixture.assignment.JobID).Scan(
		&cancellationCount,
		&chargeCount,
		&visibleChargeCount,
		&cancellationEvents,
	); err != nil {
		t.Fatalf("read SUCCEEDED cancellation history: %v", err)
	}
	if cancellationCount != 0 || chargeCount != 1 || visibleChargeCount != 1 ||
		cancellationEvents != 0 {
		t.Fatalf(
			"SUCCEEDED cancellation history = decisions/charges/visible/events %d/%d/%d/%d",
			cancellationCount,
			chargeCount,
			visibleChargeCount,
			cancellationEvents,
		)
	}
}

func TestCancellationRollsBackEveryTransactionalWriteBoundary(t *testing.T) {
	tests := []struct {
		name      string
		table     string
		action    string
		condition string
	}{
		{name: "decision", table: "job_cancellation_decisions", action: "INSERT"},
		{name: "Lease", table: "attempt_leases", action: "UPDATE"},
		{name: "Attempt", table: "attempts", action: "UPDATE"},
		{name: "Job", table: "jobs", action: "UPDATE"},
		{name: "Worker", table: "workers", action: "UPDATE"},
		{name: "Project counter", table: "projects", action: "UPDATE"},
		{name: "CreditReservation", table: "credit_reservations", action: "UPDATE"},
		{name: "Organization credit", table: "organization_credit_accounts", action: "UPDATE"},
		{name: "Charge", table: "charges", action: "INSERT"},
		{
			name:      "job.cancel_requested Outbox",
			table:     "outbox_events",
			action:    "INSERT",
			condition: "NEW.event_type = 'job.cancel_requested'",
		},
		{
			name:      "job.canceling Outbox",
			table:     "outbox_events",
			action:    "INSERT",
			condition: "NEW.event_type = 'job.canceling'",
		},
		{
			name:      "charge.posted Outbox",
			table:     "outbox_events",
			action:    "INSERT",
			condition: "NEW.event_type = 'charge.posted'",
		},
		{
			name:      "invoice.export_requested Outbox",
			table:     "outbox_events",
			action:    "INSERT",
			condition: "NEW.event_type = 'invoice.export_requested'",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newStartFixture(t, "cancel-rollback-"+test.table+"-"+test.action, 7)
			if _, err := fixture.database.Admin.Exec(`
				UPDATE credentials
				SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
				WHERE id = $1
			`, testCredentialID); err != nil {
				t.Fatalf("grant cancellation scope: %v", err)
			}
			if _, err := fixture.service.Start(
				context.Background(), fixture.worker, fixture.credentials,
			); err != nil {
				t.Fatalf("start cancellation rollback fixture: %v", err)
			}
			before := readCancellationMutationState(
				t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID,
			)
			whenClause := ""
			if test.condition != "" {
				whenClause = "WHEN (" + test.condition + ")"
			}
			if _, err := fixture.database.Admin.Exec(fmt.Sprintf(`
				CREATE FUNCTION vela_test_reject_cancellation_write() RETURNS trigger
				LANGUAGE plpgsql AS $$
				BEGIN
					RAISE EXCEPTION 'injected cancellation write rejection';
				END
				$$;
				CREATE TRIGGER vela_test_reject_cancellation_write
				BEFORE %s ON %s
				FOR EACH ROW %s EXECUTE FUNCTION vela_test_reject_cancellation_write();
			`, test.action, test.table, whenClause)); err != nil {
				t.Fatalf("install %s rejection trigger: %v", test.name, err)
			}

			server := admissionServerForDatabase(t, fixture.database)
			result := cancelJob(
				t,
				server.URL,
				testProjectID,
				fixture.assignment.JobID.String(),
				testBearerCredential(),
			)
			if result.StatusCode != http.StatusInternalServerError {
				t.Fatalf("cancellation with rejected %s write status = %d; body=%s", test.name, result.StatusCode, result.Body)
			}
			after := readCancellationMutationState(
				t, fixture.database.Admin, fixture.assignment.JobID, fixture.assignment.AttemptID,
			)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf(
					"rejected %s write committed partial cancellation: before=%#v after=%#v",
					test.name,
					before,
					after,
				)
			}
		})
	}
}

func TestQueuedAndRetryWaitCancellationRollsBackEveryTransactionalWriteBoundary(t *testing.T) {
	boundaries := []struct {
		name      string
		table     string
		action    string
		condition string
	}{
		{name: "decision", table: "job_cancellation_decisions", action: "INSERT"},
		{name: "Job", table: "jobs", action: "UPDATE"},
		{name: "Project counters", table: "projects", action: "UPDATE"},
		{name: "WorkerPool counters", table: "worker_pools", action: "UPDATE"},
		{name: "CreditReservation", table: "credit_reservations", action: "UPDATE"},
		{name: "Organization credit", table: "organization_credit_accounts", action: "UPDATE"},
		{
			name:      "job.canceled Outbox",
			table:     "outbox_events",
			action:    "INSERT",
			condition: "NEW.event_type = 'job.canceled'",
		},
	}
	for _, jobState := range []string{"QUEUED", "RETRY_WAIT"} {
		t.Run(jobState, func(t *testing.T) {
			database, serverURL, jobID := newQueueCancellationRollbackFixture(
				t, jobState, "cancel-queue-rollback-"+jobState,
			)
			before := readQueueCancellationMutationState(t, database.Admin, jobID)
			for _, boundary := range boundaries {
				t.Run(boundary.name, func(t *testing.T) {
					whenClause := ""
					if boundary.condition != "" {
						whenClause = "WHEN (" + boundary.condition + ")"
					}
					if _, err := database.Admin.Exec(fmt.Sprintf(`
						CREATE FUNCTION vela_test_reject_queue_cancellation_write() RETURNS trigger
						LANGUAGE plpgsql AS $$
						BEGIN
							RAISE EXCEPTION 'injected queue cancellation write rejection';
						END
						$$;
						CREATE TRIGGER vela_test_reject_queue_cancellation_write
						BEFORE %s ON %s
						FOR EACH ROW %s EXECUTE FUNCTION vela_test_reject_queue_cancellation_write();
					`, boundary.action, boundary.table, whenClause)); err != nil {
						t.Fatalf("install %s rejection trigger: %v", boundary.name, err)
					}
					t.Cleanup(func() {
						if _, err := database.Admin.Exec(fmt.Sprintf(`
							DROP TRIGGER IF EXISTS vela_test_reject_queue_cancellation_write ON %s;
							DROP FUNCTION IF EXISTS vela_test_reject_queue_cancellation_write();
						`, boundary.table)); err != nil {
							t.Errorf("drop %s rejection trigger: %v", boundary.name, err)
						}
					})

					result := cancelJob(
						t,
						serverURL,
						testProjectID,
						jobID.String(),
						testBearerCredential(),
					)
					if result.StatusCode != http.StatusInternalServerError {
						t.Fatalf(
							"%s cancellation with rejected %s write status = %d; body=%s",
							jobState,
							boundary.name,
							result.StatusCode,
							result.Body,
						)
					}
					after := readQueueCancellationMutationState(t, database.Admin, jobID)
					if !reflect.DeepEqual(after, before) {
						t.Fatalf(
							"rejected %s write committed partial %s cancellation: before=%#v after=%#v",
							boundary.name,
							jobState,
							before,
							after,
						)
					}
				})
			}
		})
	}
}

func TestCancellationStopCompletionRollsBackEveryTransactionalWriteBoundary(t *testing.T) {
	tests := []struct {
		name      string
		mode      string
		table     string
		action    string
		condition string
	}{
		{name: "acknowledgement Job", mode: "ack", table: "jobs", action: "UPDATE"},
		{name: "acknowledgement receipt", mode: "ack", table: "cancellation_stop_receipts", action: "INSERT"},
		{name: "acknowledgement Worker", mode: "ack", table: "workers", action: "UPDATE"},
		{
			name:      "acknowledgement terminal Outbox",
			mode:      "ack",
			table:     "outbox_events",
			action:    "INSERT",
			condition: "NEW.event_type = 'job.canceled'",
		},
		{name: "reconciliation Job", mode: "reconcile", table: "jobs", action: "UPDATE"},
		{name: "reconciliation receipt", mode: "reconcile", table: "cancellation_stop_receipts", action: "INSERT"},
		{
			name:      "reconciliation terminal Outbox",
			mode:      "reconcile",
			table:     "outbox_events",
			action:    "INSERT",
			condition: "NEW.event_type = 'job.canceled'",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, cancellationResult, coordinator := newCancelingTestFixture(
				t,
				"cancel-stop-rollback-"+test.mode+"-"+test.table,
				test.mode == "reconcile",
			)
			before := readCancellationStopMutationState(
				t, fixture.database.Admin, fixture.assignment.JobID,
			)
			whenClause := ""
			if test.condition != "" {
				whenClause = "WHEN (" + test.condition + ")"
			}
			if _, err := fixture.database.Admin.Exec(fmt.Sprintf(`
				CREATE FUNCTION vela_test_reject_cancellation_stop_write() RETURNS trigger
				LANGUAGE plpgsql AS $$
				BEGIN
					RAISE EXCEPTION 'injected cancellation stop write rejection';
				END
				$$;
				CREATE TRIGGER vela_test_reject_cancellation_stop_write
				BEFORE %s ON %s
				FOR EACH ROW %s EXECUTE FUNCTION vela_test_reject_cancellation_stop_write();
			`, test.action, test.table, whenClause)); err != nil {
				t.Fatalf("install %s rejection trigger: %v", test.name, err)
			}

			var result cancellation.StopResult
			var stopErr error
			if test.mode == "ack" {
				result, stopErr = coordinator.AcknowledgeCancellationStop(
					context.Background(),
					fixture.worker,
					fixture.credentials,
					uuid.MustParse(cancellationResult.CancellationID),
				)
			} else {
				result, stopErr = coordinator.ReconcileNextCancellationStop(context.Background())
			}
			if stopErr == nil || result.Decision != "" {
				t.Fatalf("%s with rejected write = %#v error=%v", test.name, result, stopErr)
			}
			after := readCancellationStopMutationState(
				t, fixture.database.Admin, fixture.assignment.JobID,
			)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf(
					"rejected %s write committed partial stop completion: before=%#v after=%#v",
					test.name,
					before,
					after,
				)
			}
		})
	}
}

func TestConcurrentQueuedCancellationReplaysOneDecision(t *testing.T) {
	server, admin := newAdmissionServer(t)
	if _, err := admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	accepted := submitJob(t, server.URL, "cancel-concurrent-queued", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"concurrent cancellation must replay one decision"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Accepted Job: %v", err)
	}

	start := make(chan struct{})
	results := make(chan httpResult, 2)
	errorsChannel := make(chan error, 2)
	var waitGroup sync.WaitGroup
	for range 2 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			result, err := doCancelJob(
				server.URL,
				testProjectID,
				job.JobID,
				testBearerCredential(),
			)
			if err != nil {
				errorsChannel <- err
				return
			}
			results <- result
		}()
	}
	close(start)
	waitGroup.Wait()
	close(results)
	close(errorsChannel)
	for err := range errorsChannel {
		t.Fatalf("concurrent queued cancellation request: %v", err)
	}
	var responses []cancelResponse
	for result := range results {
		if result.StatusCode != http.StatusOK {
			t.Fatalf("concurrent queued cancellation status = %d, want 200; body=%s", result.StatusCode, result.Body)
		}
		var response cancelResponse
		if err := json.Unmarshal(result.Body, &response); err != nil {
			t.Fatalf("decode concurrent queued cancellation: %v", err)
		}
		responses = append(responses, response)
	}
	if len(responses) != 2 || responses[0].CancellationID == "" ||
		responses[0].CancellationID != responses[1].CancellationID ||
		responses[0].Decision != "CANCELED" || responses[1].Decision != "CANCELED" ||
		responses[0].JobVersion != 2 || responses[1].JobVersion != 2 ||
		!responses[0].DecidedAt.Equal(responses[1].DecidedAt) {
		t.Fatalf("concurrent queued cancellation responses = %#v", responses)
	}

	var decisionCount, chargeCount, canceledEvents, reservedMinor int64
	if err := admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = $1),
			(SELECT count(*) FROM charges WHERE job_id = $1),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = $1 AND event_type = 'job.canceled'),
			(SELECT reserved_minor FROM organization_credit_accounts WHERE organization_id = $2)
	`, job.JobID, testOrganizationID).Scan(
		&decisionCount,
		&chargeCount,
		&canceledEvents,
		&reservedMinor,
	); err != nil {
		t.Fatalf("read concurrent queued cancellation state: %v", err)
	}
	if decisionCount != 1 || chargeCount != 0 || canceledEvents != 1 || reservedMinor != 0 {
		t.Fatalf(
			"concurrent queued cancellation state = decisions/charges/events/credit %d/%d/%d/%d",
			decisionCount,
			chargeCount,
			canceledEvents,
			reservedMinor,
		)
	}
}

func TestConcurrentRunningCancellationPostsOneChargeAndReplaysDecision(t *testing.T) {
	fixture := newStartFixture(t, "cancel-concurrent-running", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	server := admissionServerForDatabase(t, fixture.database)

	start := make(chan struct{})
	results := make(chan httpResult, 2)
	errorsChannel := make(chan error, 2)
	var waitGroup sync.WaitGroup
	for range 2 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			result, cancelErr := doCancelJob(
				server.URL,
				testProjectID,
				fixture.assignment.JobID.String(),
				testBearerCredential(),
			)
			if cancelErr != nil {
				errorsChannel <- cancelErr
				return
			}
			results <- result
		}()
	}
	close(start)
	waitGroup.Wait()
	close(results)
	close(errorsChannel)
	for cancelErr := range errorsChannel {
		t.Fatalf("concurrent RUNNING cancellation request: %v", cancelErr)
	}
	var responses []cancelResponse
	for result := range results {
		if result.StatusCode != http.StatusOK {
			t.Fatalf("concurrent RUNNING cancellation status = %d; body=%s", result.StatusCode, result.Body)
		}
		var response cancelResponse
		if err := json.Unmarshal(result.Body, &response); err != nil {
			t.Fatalf("decode concurrent RUNNING cancellation: %v", err)
		}
		responses = append(responses, response)
	}
	if len(responses) != 2 || responses[0].CancellationID == "" ||
		responses[0].CancellationID != responses[1].CancellationID ||
		responses[0].Decision != "CANCELING" || responses[1].Decision != "CANCELING" ||
		responses[0].JobVersion != 4 || responses[1].JobVersion != 4 ||
		responses[0].Charge == nil || responses[1].Charge == nil ||
		responses[0].Charge.ChargeID != responses[1].Charge.ChargeID ||
		responses[0].Charge.Amount != 1250 || responses[1].Charge.Amount != 1250 ||
		!responses[0].DecidedAt.Equal(responses[1].DecidedAt) {
		t.Fatalf("concurrent RUNNING cancellation responses = %#v", responses)
	}

	var jobState, attemptState, workerState, reservationState string
	var jobVersion, jobFence, decisionCount, chargeCount int64
	var cancelRequestedEvents, cancelingEvents, chargeEvents, invoiceEvents int64
	var projectRunning, reservedMinor, unsettledPostedMinor int64
	var leaseRevoked bool
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state,
			job.version,
			job.current_fence,
			attempt.state,
			lease.revoked_at IS NOT NULL,
			worker.lifecycle_state,
			project.running_count,
			reservation.state,
			credit.reserved_minor,
			credit.unsettled_posted_minor,
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id AND event_type = 'job.cancel_requested'),
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id AND event_type = 'job.canceling'),
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id AND event_type = 'charge.posted'),
			(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id AND event_type = 'invoice.export_requested')
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.job_id = job.id
		JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id AND lease.phase = 'EXECUTION'
		JOIN workers AS worker ON worker.id = attempt.worker_id
		JOIN projects AS project ON project.id = job.project_id
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN organization_credit_accounts AS credit ON credit.organization_id = job.organization_id
		WHERE job.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobState,
		&jobVersion,
		&jobFence,
		&attemptState,
		&leaseRevoked,
		&workerState,
		&projectRunning,
		&reservationState,
		&reservedMinor,
		&unsettledPostedMinor,
		&decisionCount,
		&chargeCount,
		&cancelRequestedEvents,
		&cancelingEvents,
		&chargeEvents,
		&invoiceEvents,
	); err != nil {
		t.Fatalf("read concurrent RUNNING cancellation state: %v", err)
	}
	if jobState != "CANCELING" || jobVersion != 4 || jobFence != 2 ||
		attemptState != "CANCELED" || !leaseRevoked || workerState != "DRAINING" ||
		projectRunning != 0 || reservationState != "CONSUMED" ||
		reservedMinor != 0 || unsettledPostedMinor != 1250 || decisionCount != 1 ||
		chargeCount != 1 || cancelRequestedEvents != 1 || cancelingEvents != 1 ||
		chargeEvents != 1 || invoiceEvents != 1 {
		t.Fatalf(
			"concurrent RUNNING cancellation state = job %s/%d/%d attempt %s lease_revoked=%t worker %s running %d reservation %s credit %d/%d decisions/charges/events %d/%d/%d/%d/%d/%d",
			jobState,
			jobVersion,
			jobFence,
			attemptState,
			leaseRevoked,
			workerState,
			projectRunning,
			reservationState,
			reservedMinor,
			unsettledPostedMinor,
			decisionCount,
			chargeCount,
			cancelRequestedEvents,
			cancelingEvents,
			chargeEvents,
			invoiceEvents,
		)
	}
}

func TestAssignmentWinsQueuedDiscoveryAndCancellationRetriesActiveAuthority(t *testing.T) {
	fixture := newAssignmentFixture(t, "cancel-queued-discovery-assigned", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	holder, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin queued Job lock holder: %v", err)
	}
	defer func() { _ = holder.Rollback() }()
	if _, err := holder.Exec(
		"SELECT id FROM jobs WHERE id = $1 FOR UPDATE",
		fixture.candidate.JobID,
	); err != nil {
		t.Fatalf("lock queued Job before Assignment/cancel race: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type assignmentCall struct {
		assignment workercontrol.Assignment
		err        error
	}
	type cancelCall struct {
		result httpResult
		err    error
	}
	assignmentResult := make(chan assignmentCall, 1)
	go func() {
		assignment, acquireErr := fixture.service.Acquire(
			ctx,
			fixture.worker,
			7,
			&fixture.candidate,
		)
		assignmentResult <- assignmentCall{assignment: assignment, err: acquireErr}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_internal_login")

	cancelResult := make(chan cancelCall, 1)
	go func() {
		result, cancelErr := doCancelJob(
			server.URL,
			testProjectID,
			fixture.candidate.JobID.String(),
			testBearerCredential(),
		)
		cancelResult <- cancelCall{result: result, err: cancelErr}
	}()
	waitForRoleDatabaseLockCount(t, fixture.database.Admin, "vela_internal_login", 2)
	if err := holder.Commit(); err != nil {
		t.Fatalf("release queued Job for Assignment/cancel race: %v", err)
	}

	assigned := <-assignmentResult
	if assigned.err != nil || assigned.assignment.AttemptID == uuid.Nil {
		t.Fatalf("winning Assignment = %#v error=%v", assigned.assignment, assigned.err)
	}
	canceled := <-cancelResult
	if canceled.err != nil || canceled.result.StatusCode != http.StatusOK {
		t.Fatalf(
			"post-Assignment cancellation = status %d body=%s error=%v",
			canceled.result.StatusCode,
			canceled.result.Body,
			canceled.err,
		)
	}
	var response cancelResponse
	if err := json.Unmarshal(canceled.result.Body, &response); err != nil {
		t.Fatalf("decode post-Assignment cancellation: %v", err)
	}
	if response.Decision != "CANCELED" || response.State != "CANCELED" ||
		response.JobVersion != fixture.candidate.ExpectedJobVersion+2 || response.Billable ||
		response.Charge != nil {
		t.Fatalf("post-Assignment cancellation = %#v", response)
	}

	var (
		jobState, attemptState, workerState, reservationState string
		leaseRevoked                                          bool
		projectQueued, projectRunning, poolQueued             int64
		reservedMinor, decisions, charges                     int64
		cancelRequestedEvents, canceledEvents                 int64
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state,
			a.state,
			l.revoked_at IS NOT NULL,
			w.lifecycle_state,
			p.queued_count,
			p.running_count,
			wp.queued_count,
			cr.state,
			oca.reserved_minor,
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = j.id),
			(SELECT count(*) FROM charges WHERE job_id = j.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id AND event_type = 'job.cancel_requested'),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id AND event_type = 'job.canceled')
		FROM jobs AS j
		JOIN attempts AS a ON a.job_id = j.id
		JOIN attempt_leases AS l ON l.attempt_id = a.id
		JOIN workers AS w ON w.id = a.worker_id
		JOIN projects AS p ON p.id = j.project_id
		JOIN worker_pools AS wp ON wp.id = j.worker_pool_id
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		JOIN organization_credit_accounts AS oca ON oca.organization_id = j.organization_id
		WHERE j.id = $1
	`, fixture.candidate.JobID).Scan(
		&jobState,
		&attemptState,
		&leaseRevoked,
		&workerState,
		&projectQueued,
		&projectRunning,
		&poolQueued,
		&reservationState,
		&reservedMinor,
		&decisions,
		&charges,
		&cancelRequestedEvents,
		&canceledEvents,
	); err != nil {
		t.Fatalf("read Assignment/cancel race state: %v", err)
	}
	if jobState != "CANCELED" || attemptState != "CANCELED" || !leaseRevoked ||
		workerState != "DRAINING" || projectQueued != 0 || projectRunning != 0 ||
		poolQueued != 0 || reservationState != "RELEASED" || reservedMinor != 0 ||
		decisions != 1 || charges != 0 || cancelRequestedEvents != 1 || canceledEvents != 1 {
		t.Fatalf(
			"Assignment/cancel race state = job %s attempt %s lease revoked=%t worker %s counters %d/%d/%d reservation %s credit %d history %d/%d events %d/%d",
			jobState,
			attemptState,
			leaseRevoked,
			workerState,
			projectQueued,
			projectRunning,
			poolQueued,
			reservationState,
			reservedMinor,
			decisions,
			charges,
			cancelRequestedEvents,
			canceledEvents,
		)
	}
}

func TestFailWinsActiveAuthorityChangeAndCancellationRetriesWithoutDeadlock(t *testing.T) {
	fixture := newAssignmentFixture(t, "cancel-fail-authority-race", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	assignment, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	holder, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Worker lock holder: %v", err)
	}
	defer func() { _ = holder.Rollback() }()
	if _, err := holder.Exec("SELECT id FROM workers WHERE id = $1 FOR UPDATE", assignment.WorkerID); err != nil {
		t.Fatalf("lock Worker before Fail/cancel race: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type failCall struct {
		decision workercontrol.RetryDecision
		err      error
	}
	type cancelCall struct {
		result httpResult
		err    error
	}
	failResult := make(chan failCall, 1)
	go func() {
		decision, failErr := fixture.service.Fail(
			ctx,
			fixture.worker,
			leaseCredentials(assignment),
			validFailureObservation(),
		)
		failResult <- failCall{decision: decision, err: failErr}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_internal_login")
	cancelResult := make(chan cancelCall, 1)
	go func() {
		result, cancelErr := doCancelJob(
			server.URL,
			testProjectID,
			assignment.JobID.String(),
			testBearerCredential(),
		)
		cancelResult <- cancelCall{result: result, err: cancelErr}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_cancel_login")
	if err := holder.Commit(); err != nil {
		t.Fatalf("release Worker for Fail/cancel race: %v", err)
	}

	failed := <-failResult
	if failed.err != nil || failed.decision.Disposition != workercontrol.RetryDispositionRetryWait {
		t.Fatalf("winning Fail = %#v error=%v", failed.decision, failed.err)
	}
	canceled := <-cancelResult
	if canceled.err != nil || canceled.result.StatusCode != http.StatusOK {
		t.Fatalf(
			"post-Fail cancellation = status %d body=%s error=%v",
			canceled.result.StatusCode,
			canceled.result.Body,
			canceled.err,
		)
	}
	var response cancelResponse
	if err := json.Unmarshal(canceled.result.Body, &response); err != nil {
		t.Fatalf("decode post-Fail cancellation: %v", err)
	}
	if response.Decision != "CANCELED" || response.State != "CANCELED" ||
		response.JobVersion != failed.decision.JobVersion+1 || response.Billable ||
		response.Charge != nil {
		t.Fatalf("post-Fail cancellation = %#v", response)
	}

	var (
		jobState, reservationState                      string
		projectQueued, projectRetryWait, projectRunning int64
		poolQueued, poolRetryWait, reservedMinor        int64
		failureDecisions, cancellationDecisions         int64
		charges, retryEvents, canceledEvents            int64
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state,
			p.queued_count,
			p.retry_wait_count,
			p.running_count,
			wp.queued_count,
			wp.retry_wait_count,
			cr.state,
			oca.reserved_minor,
			(SELECT count(*) FROM execution_failure_decisions WHERE job_id = j.id),
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = j.id),
			(SELECT count(*) FROM charges WHERE job_id = j.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id AND event_type = 'job.retry_wait'),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id AND event_type = 'job.canceled')
		FROM jobs AS j
		JOIN projects AS p ON p.id = j.project_id
		JOIN worker_pools AS wp ON wp.id = j.worker_pool_id
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		JOIN organization_credit_accounts AS oca ON oca.organization_id = j.organization_id
		WHERE j.id = $1
	`, assignment.JobID).Scan(
		&jobState,
		&projectQueued,
		&projectRetryWait,
		&projectRunning,
		&poolQueued,
		&poolRetryWait,
		&reservationState,
		&reservedMinor,
		&failureDecisions,
		&cancellationDecisions,
		&charges,
		&retryEvents,
		&canceledEvents,
	); err != nil {
		t.Fatalf("read Fail/cancel race state: %v", err)
	}
	if jobState != "CANCELED" || projectQueued != 0 || projectRetryWait != 0 ||
		projectRunning != 0 || poolQueued != 0 || poolRetryWait != 0 ||
		reservationState != "RELEASED" || reservedMinor != 0 ||
		failureDecisions != 1 || cancellationDecisions != 1 || charges != 0 ||
		retryEvents != 1 || canceledEvents != 1 {
		t.Fatalf(
			"Fail/cancel race state = job %s project %d/%d/%d pool %d/%d reservation %s credit %d decisions %d/%d charges/events %d/%d/%d",
			jobState,
			projectQueued,
			projectRetryWait,
			projectRunning,
			poolQueued,
			poolRetryWait,
			reservationState,
			reservedMinor,
			failureDecisions,
			cancellationDecisions,
			charges,
			retryEvents,
			canceledEvents,
		)
	}
}

func TestCancellationAndStartProduceOneBillingOutcome(t *testing.T) {
	fixture := newStartFixture(t, "cancel-start-race", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	type startCall struct {
		result workercontrol.StartResult
		err    error
	}
	type cancelCall struct {
		result httpResult
		err    error
	}
	startResult := make(chan startCall, 1)
	cancelResult := make(chan cancelCall, 1)
	go func() {
		<-start
		result, startErr := fixture.service.Start(
			ctx, fixture.worker, fixture.credentials,
		)
		startResult <- startCall{result: result, err: startErr}
	}()
	go func() {
		<-start
		result, cancelErr := doCancelJob(
			server.URL,
			testProjectID,
			fixture.assignment.JobID.String(),
			testBearerCredential(),
		)
		cancelResult <- cancelCall{result: result, err: cancelErr}
	}()
	close(start)
	started := <-startResult
	canceled := <-cancelResult
	if started.err != nil ||
		(started.result.Decision != workercontrol.StartGranted &&
			started.result.Decision != workercontrol.Stop) {
		t.Fatalf("concurrent Start = %#v error=%v", started.result, started.err)
	}
	if canceled.err != nil || canceled.result.StatusCode != http.StatusOK {
		t.Fatalf(
			"concurrent Start cancellation = status %d body=%s error=%v",
			canceled.result.StatusCode,
			canceled.result.Body,
			canceled.err,
		)
	}
	var response cancelResponse
	if err := json.Unmarshal(canceled.result.Body, &response); err != nil {
		t.Fatalf("decode Start race cancellation: %v", err)
	}

	var jobState, reservationState string
	var reservedMinor, unsettledPostedMinor, chargeCount int64
	var cancellationCount, canceledEvents, cancelingEvents int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state,
			reservation.state,
			credit.reserved_minor,
			credit.unsettled_posted_minor,
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = job.id AND event_type = 'job.canceled'),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = job.id AND event_type = 'job.canceling')
		FROM jobs AS job
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN organization_credit_accounts AS credit
		  ON credit.organization_id = job.organization_id
		WHERE job.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobState,
		&reservationState,
		&reservedMinor,
		&unsettledPostedMinor,
		&chargeCount,
		&cancellationCount,
		&canceledEvents,
		&cancelingEvents,
	); err != nil {
		t.Fatalf("read cancel/Start race state: %v", err)
	}
	if cancellationCount != 1 {
		t.Fatalf("cancel/Start race cancellation decisions = %d, want 1", cancellationCount)
	}
	switch jobState {
	case "CANCELED":
		if started.result.Decision != workercontrol.Stop || response.Decision != "CANCELED" ||
			response.Billable || response.Charge != nil || reservationState != "RELEASED" ||
			reservedMinor != 0 || unsettledPostedMinor != 0 || chargeCount != 0 ||
			canceledEvents != 1 || cancelingEvents != 0 {
			t.Fatalf(
				"cancellation-winning Start race = start %#v response %#v reservation %s credit %d/%d charges/events %d/%d/%d",
				started.result,
				response,
				reservationState,
				reservedMinor,
				unsettledPostedMinor,
				chargeCount,
				canceledEvents,
				cancelingEvents,
			)
		}
	case "CANCELING":
		if started.result.Decision != workercontrol.StartGranted || response.Decision != "CANCELING" ||
			!response.Billable || response.Charge == nil || response.Charge.Amount != 1250 ||
			reservationState != "CONSUMED" || reservedMinor != 0 || unsettledPostedMinor != 1250 ||
			chargeCount != 1 || canceledEvents != 0 || cancelingEvents != 1 {
			t.Fatalf(
				"Start-winning cancellation race = start %#v response %#v reservation %s credit %d/%d charges/events %d/%d/%d",
				started.result,
				response,
				reservationState,
				reservedMinor,
				unsettledPostedMinor,
				chargeCount,
				canceledEvents,
				cancelingEvents,
			)
		}
	default:
		t.Fatalf("cancel/Start race Job state = %s", jobState)
	}
}

func TestCancellationAndFailProduceOneBillingOutcome(t *testing.T) {
	fixture := newStartFixture(t, "cancel-fail-race", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	if _, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil {
		t.Fatalf("start cancel/Fail race fixture: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	type failCall struct {
		result workercontrol.RetryDecision
		err    error
	}
	type cancelCall struct {
		result httpResult
		err    error
	}
	failResult := make(chan failCall, 1)
	cancelResult := make(chan cancelCall, 1)
	go func() {
		<-start
		result, failErr := fixture.service.Fail(
			ctx,
			fixture.worker,
			fixture.credentials,
			validFailureObservation(),
		)
		failResult <- failCall{result: result, err: failErr}
	}()
	go func() {
		<-start
		result, cancelErr := doCancelJob(
			server.URL,
			testProjectID,
			fixture.assignment.JobID.String(),
			testBearerCredential(),
		)
		cancelResult <- cancelCall{result: result, err: cancelErr}
	}()
	close(start)
	failed := <-failResult
	canceled := <-cancelResult
	if failed.err != nil ||
		(failed.result.Disposition != workercontrol.RetryDispositionRetryWait &&
			failed.result.Disposition != workercontrol.RetryDispositionRejectedStaleLease) {
		t.Fatalf("concurrent Fail = %#v error=%v", failed.result, failed.err)
	}
	if canceled.err != nil || canceled.result.StatusCode != http.StatusOK {
		t.Fatalf(
			"concurrent Fail cancellation = status %d body=%s error=%v",
			canceled.result.StatusCode,
			canceled.result.Body,
			canceled.err,
		)
	}
	var response cancelResponse
	if err := json.Unmarshal(canceled.result.Body, &response); err != nil {
		t.Fatalf("decode Fail race cancellation: %v", err)
	}

	var jobState, reservationState string
	var reservedMinor, unsettledPostedMinor, chargeCount int64
	var cancellationCount, failureCount, retryEvents, canceledEvents int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state,
			reservation.state,
			credit.reserved_minor,
			credit.unsettled_posted_minor,
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
			(SELECT count(*) FROM execution_failure_decisions WHERE job_id = job.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = job.id AND event_type = 'job.retry_wait'),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = job.id AND event_type = 'job.canceled')
		FROM jobs AS job
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN organization_credit_accounts AS credit
		  ON credit.organization_id = job.organization_id
		WHERE job.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobState,
		&reservationState,
		&reservedMinor,
		&unsettledPostedMinor,
		&chargeCount,
		&cancellationCount,
		&failureCount,
		&retryEvents,
		&canceledEvents,
	); err != nil {
		t.Fatalf("read cancel/Fail race state: %v", err)
	}
	if cancellationCount != 1 {
		t.Fatalf("cancel/Fail race cancellation decisions = %d, want 1", cancellationCount)
	}
	switch jobState {
	case "CANCELING":
		if failed.result.Disposition != workercontrol.RetryDispositionRejectedStaleLease ||
			response.Decision != "CANCELING" || !response.Billable || response.Charge == nil ||
			response.Charge.Amount != 1250 || reservationState != "CONSUMED" || reservedMinor != 0 ||
			unsettledPostedMinor != 1250 || chargeCount != 1 || failureCount != 0 ||
			retryEvents != 0 || canceledEvents != 0 {
			t.Fatalf(
				"cancellation-winning Fail race = fail %#v response %#v reservation %s credit %d/%d charges/decisions/events %d/%d/%d/%d",
				failed.result,
				response,
				reservationState,
				reservedMinor,
				unsettledPostedMinor,
				chargeCount,
				failureCount,
				retryEvents,
				canceledEvents,
			)
		}
	case "CANCELED":
		if failed.result.Disposition != workercontrol.RetryDispositionRetryWait ||
			response.Decision != "CANCELED" || response.Billable || response.Charge != nil ||
			reservationState != "RELEASED" || reservedMinor != 0 || unsettledPostedMinor != 0 ||
			chargeCount != 0 || failureCount != 1 || retryEvents != 1 || canceledEvents != 1 {
			t.Fatalf(
				"Fail-winning cancellation race = fail %#v response %#v reservation %s credit %d/%d charges/decisions/events %d/%d/%d/%d",
				failed.result,
				response,
				reservationState,
				reservedMinor,
				unsettledPostedMinor,
				chargeCount,
				failureCount,
				retryEvents,
				canceledEvents,
			)
		}
	default:
		t.Fatalf("cancel/Fail race Job state = %s", jobState)
	}
}

func TestCancellationAndHeartbeatSerializeWithoutDeadlock(t *testing.T) {
	fixture := newStartFixture(t, "cancel-heartbeat-race", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	type heartbeatCall struct {
		result workercontrol.HeartbeatResult
		err    error
	}
	type cancelCall struct {
		result httpResult
		err    error
	}
	heartbeatResult := make(chan heartbeatCall, 1)
	cancelResult := make(chan cancelCall, 1)
	go func() {
		<-start
		result, heartbeatErr := fixture.service.Heartbeat(
			ctx,
			fixture.worker,
			fixture.credentials,
			validHeartbeatObservation(1),
		)
		heartbeatResult <- heartbeatCall{result: result, err: heartbeatErr}
	}()
	go func() {
		<-start
		result, cancelErr := doCancelJob(
			server.URL,
			testProjectID,
			fixture.assignment.JobID.String(),
			testBearerCredential(),
		)
		cancelResult <- cancelCall{result: result, err: cancelErr}
	}()
	close(start)
	heartbeat := <-heartbeatResult
	canceled := <-cancelResult
	if heartbeat.err != nil ||
		(heartbeat.result.Decision != workercontrol.HeartbeatContinue &&
			heartbeat.result.Decision != workercontrol.HeartbeatStop) {
		t.Fatalf("concurrent Heartbeat = %#v error=%v", heartbeat.result, heartbeat.err)
	}
	if canceled.err != nil || canceled.result.StatusCode != http.StatusOK {
		t.Fatalf(
			"concurrent cancellation = status %d body=%s error=%v",
			canceled.result.StatusCode,
			canceled.result.Body,
			canceled.err,
		)
	}
	var response cancelResponse
	if err := json.Unmarshal(canceled.result.Body, &response); err != nil {
		t.Fatalf("decode concurrent cancellation: %v", err)
	}
	if response.Decision != "CANCELING" || response.Charge == nil ||
		response.Charge.Amount != 1250 {
		t.Fatalf("concurrent cancellation response = %#v", response)
	}
	var jobState, reservationState string
	var chargeCount, decisionCount, reservedMinor, unsettledPostedMinor int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state,
			cr.state,
			oca.reserved_minor,
			oca.unsettled_posted_minor,
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = j.id),
			(SELECT count(*) FROM charges WHERE job_id = j.id)
		FROM jobs AS j
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		JOIN organization_credit_accounts AS oca ON oca.organization_id = j.organization_id
		WHERE j.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobState,
		&reservationState,
		&reservedMinor,
		&unsettledPostedMinor,
		&decisionCount,
		&chargeCount,
	); err != nil {
		t.Fatalf("read cancel/Heartbeat race state: %v", err)
	}
	if jobState != "CANCELING" || reservationState != "CONSUMED" ||
		reservedMinor != 0 || unsettledPostedMinor != 1250 ||
		decisionCount != 1 || chargeCount != 1 {
		t.Fatalf(
			"cancel/Heartbeat race state = job %s reservation %s credit %d/%d decisions/charges %d/%d",
			jobState,
			reservationState,
			reservedMinor,
			unsettledPostedMinor,
			decisionCount,
			chargeCount,
		)
	}
}

func TestCancellationAndJobExpiryProduceOneBillingOutcome(t *testing.T) {
	fixture := newStartFixture(t, "cancel-job-expiry-race", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	forceJobPolicySnapshot(
		t,
		fixture.database.Admin,
		fixture.assignment.JobID,
		"job_expires_at = clock_timestamp()",
	)
	server := admissionServerForDatabase(t, fixture.database)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	type reconcileCall struct {
		result workercontrol.ReconciliationResult
		err    error
	}
	type cancelCall struct {
		result httpResult
		err    error
	}
	reconcileResult := make(chan reconcileCall, 1)
	cancelResult := make(chan cancelCall, 1)
	go func() {
		<-start
		result, reconcileErr := fixture.service.ReconcileNextExecutionFailure(ctx)
		reconcileResult <- reconcileCall{result: result, err: reconcileErr}
	}()
	go func() {
		<-start
		result, cancelErr := doCancelJob(
			server.URL,
			testProjectID,
			fixture.assignment.JobID.String(),
			testBearerCredential(),
		)
		cancelResult <- cancelCall{result: result, err: cancelErr}
	}()
	close(start)
	reconciled := <-reconcileResult
	canceled := <-cancelResult
	if reconciled.err != nil {
		t.Fatalf("concurrent Job Expiry reconciliation: %v", reconciled.err)
	}
	if canceled.err != nil || canceled.result.StatusCode != http.StatusOK {
		t.Fatalf(
			"concurrent Job Expiry cancellation = status %d body=%s error=%v",
			canceled.result.StatusCode,
			canceled.result.Body,
			canceled.err,
		)
	}
	var response cancelResponse
	if err := json.Unmarshal(canceled.result.Body, &response); err != nil {
		t.Fatalf("decode Job Expiry race cancellation: %v", err)
	}
	if response.Decision != "CANCELING" && response.Decision != "ALREADY_FAILED" {
		t.Fatalf("Job Expiry race cancellation response = %#v", response)
	}

	var jobState, reservationState string
	var reservedMinor, unsettledPostedMinor, chargeCount int64
	var cancellationCount, failureCount int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state,
			cr.state,
			oca.reserved_minor,
			oca.unsettled_posted_minor,
			(SELECT count(*) FROM charges WHERE job_id = j.id),
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = j.id),
			(SELECT count(*) FROM execution_failure_decisions WHERE job_id = j.id)
		FROM jobs AS j
		JOIN credit_reservations AS cr ON cr.job_id = j.id
		JOIN organization_credit_accounts AS oca ON oca.organization_id = j.organization_id
		WHERE j.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobState,
		&reservationState,
		&reservedMinor,
		&unsettledPostedMinor,
		&chargeCount,
		&cancellationCount,
		&failureCount,
	); err != nil {
		t.Fatalf("read cancel/Job Expiry race state: %v", err)
	}
	switch jobState {
	case "CANCELING":
		if response.Decision != "CANCELING" || reservationState != "CONSUMED" ||
			reservedMinor != 0 || unsettledPostedMinor != 1250 || chargeCount != 1 ||
			cancellationCount != 1 || failureCount != 0 {
			t.Fatalf(
				"cancellation-winning Job Expiry race = response %#v state %s credit %d/%d charges/decisions %d/%d/%d reconcile %#v",
				response,
				reservationState,
				reservedMinor,
				unsettledPostedMinor,
				chargeCount,
				cancellationCount,
				failureCount,
				reconciled.result,
			)
		}
	case "FAILED":
		if response.Decision != "ALREADY_FAILED" || reservationState != "RELEASED" ||
			reservedMinor != 0 || unsettledPostedMinor != 0 || chargeCount != 0 ||
			cancellationCount != 0 || failureCount != 1 {
			t.Fatalf(
				"expiry-winning cancellation race = response %#v state %s credit %d/%d charges/decisions %d/%d/%d reconcile %#v",
				response,
				reservationState,
				reservedMinor,
				unsettledPostedMinor,
				chargeCount,
				cancellationCount,
				failureCount,
				reconciled.result,
			)
		}
	default:
		t.Fatalf("cancel/Job Expiry race terminal state = %s", jobState)
	}
}

func TestCancellationAcknowledgementAndReconciliationCommitOneReceipt(t *testing.T) {
	fixture := newStartFixture(t, "cancel-ack-reconcile-race", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	var originalLeaseExpiry time.Time
	if err := fixture.database.Admin.QueryRow(`
		UPDATE attempt_leases
		SET expires_at = greatest(
			clock_timestamp() + interval '500 milliseconds',
			issued_at + interval '500 milliseconds'
		),
			updated_at = clock_timestamp()
		WHERE attempt_id = $1 AND revoked_at IS NULL
		RETURNING expires_at
	`, fixture.assignment.AttemptID).Scan(&originalLeaseExpiry); err != nil {
		t.Fatalf("shorten active Lease before ack/reconcile race: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	canceled := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel RUNNING status = %d, want 200; body=%s", canceled.StatusCode, canceled.Body)
	}
	var response cancelResponse
	if err := json.Unmarshal(canceled.Body, &response); err != nil {
		t.Fatalf("decode cancellation before ack/reconcile race: %v", err)
	}
	var snapshottedLeaseExpiry time.Time
	if err := fixture.database.Admin.QueryRow(`
		SELECT authority_lease_expires_at
		FROM job_cancellation_decisions
		WHERE id = $1
	`, response.CancellationID).Scan(&snapshottedLeaseExpiry); err != nil {
		t.Fatalf("read original Lease expiry from cancellation decision: %v", err)
	}
	if !snapshottedLeaseExpiry.Equal(originalLeaseExpiry) {
		t.Fatalf("snapshotted Lease expiry = %s, want %s", snapshottedLeaseExpiry, originalLeaseExpiry)
	}
	waitForDatabaseTimeAfter(t, fixture.database.Admin, snapshottedLeaseExpiry)
	var reconciliationCandidates int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*)
		FROM job_cancellation_decisions AS decision
		JOIN attempt_leases AS lease ON lease.id = decision.authority_lease_id
		LEFT JOIN cancellation_stop_receipts AS receipt ON receipt.cancellation_id = decision.id
		WHERE decision.id = $1
		  AND lease.revoked_at IS NOT NULL
		  AND decision.authority_lease_expires_at <= clock_timestamp()
		  AND receipt.id IS NULL
	`, response.CancellationID).Scan(&reconciliationCandidates); err != nil {
		t.Fatalf("prove reconciliation candidate before race: %v", err)
	}
	if reconciliationCandidates != 1 {
		t.Fatalf("reconciliation candidates before race = %d, want 1", reconciliationCandidates)
	}
	cancelPool := newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password")
	internalPool := newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password")
	coordinator := cancellation.NewService(cancelPool, internalPool)
	ctx, cancelRace := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelRace()
	start := make(chan struct{})
	type stopCall struct {
		result cancellation.StopResult
		err    error
	}
	ackResult := make(chan stopCall, 1)
	reconcileResult := make(chan stopCall, 1)
	go func() {
		<-start
		result, ackErr := coordinator.AcknowledgeCancellationStop(
			ctx,
			fixture.worker,
			fixture.credentials,
			uuid.MustParse(response.CancellationID),
		)
		ackResult <- stopCall{result: result, err: ackErr}
	}()
	go func() {
		<-start
		result, reconcileErr := coordinator.ReconcileNextCancellationStop(ctx)
		reconcileResult <- stopCall{result: result, err: reconcileErr}
	}()
	close(start)
	acknowledged := <-ackResult
	reconciled := <-reconcileResult
	if acknowledged.err != nil ||
		(acknowledged.result.Decision != cancellation.StopAcknowledged &&
			acknowledged.result.Decision != cancellation.StopReconciled) {
		t.Fatalf("concurrent stop acknowledgement = %#v error=%v", acknowledged.result, acknowledged.err)
	}
	if reconciled.err != nil ||
		(reconciled.result.Decision != cancellation.StopReconciled &&
			reconciled.result.Decision != cancellation.StopNoWork) {
		t.Fatalf("concurrent stop reconciliation = %#v error=%v", reconciled.result, reconciled.err)
	}

	var jobState, workerState, receiptSource string
	var jobVersion, receiptCount, canceledEvents int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state,
			j.version,
			w.lifecycle_state,
			r.source,
			(SELECT count(*) FROM cancellation_stop_receipts WHERE cancellation_id = d.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = j.id AND event_type = 'job.canceled')
		FROM jobs AS j
		JOIN job_cancellation_decisions AS d ON d.job_id = j.id
		JOIN cancellation_stop_receipts AS r ON r.cancellation_id = d.id
		JOIN workers AS w ON w.id = d.worker_id
		WHERE j.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobState,
		&jobVersion,
		&workerState,
		&receiptSource,
		&receiptCount,
		&canceledEvents,
	); err != nil {
		t.Fatalf("read ack/reconcile race state: %v", err)
	}
	expectedWorkerState := "DRAINING"
	if receiptSource == "ACKNOWLEDGED" {
		expectedWorkerState = "READY"
	}
	if jobState != "CANCELED" || jobVersion != 5 || workerState != expectedWorkerState ||
		receiptCount != 1 || canceledEvents != 1 || acknowledged.result.Source != receiptSource {
		t.Fatalf(
			"ack/reconcile race state = job %s/%d worker %s receipt %s/%d events %d ack %#v reconcile %#v",
			jobState,
			jobVersion,
			workerState,
			receiptSource,
			receiptCount,
			canceledEvents,
			acknowledged.result,
			reconciled.result,
		)
	}
	if receiptSource == "ACKNOWLEDGED" {
		if acknowledged.result.Decision != cancellation.StopAcknowledged ||
			reconciled.result.Decision != cancellation.StopNoWork {
			t.Fatalf("acknowledgement-winning race = ack %#v reconcile %#v", acknowledged.result, reconciled.result)
		}
	} else if receiptSource == "LEASE_EXPIRED_RECONCILIATION" {
		if acknowledged.result.Decision != cancellation.StopReconciled ||
			reconciled.result.Decision != cancellation.StopReconciled ||
			reconciled.result.Source != receiptSource {
			t.Fatalf("reconciliation-winning race = ack %#v reconcile %#v", acknowledged.result, reconciled.result)
		}
	} else {
		t.Fatalf("ack/reconcile race stored unexpected receipt source %q", receiptSource)
	}
}

type cancellationMutationState struct {
	JobState            string
	JobVersion          int64
	JobFence            int64
	AttemptState        string
	AttemptEnded        bool
	LeaseRevoked        bool
	WorkerState         string
	ProjectRunning      int64
	ReservationState    string
	ReservedMinor       int64
	UnsettledPosted     int64
	CancellationRecords int64
	Charges             int64
	CancellationEvents  int64
}

type queueCancellationMutationState struct {
	JobState            string
	JobVersion          int64
	JobFence            int64
	ProjectQueued       int64
	ProjectRetryWait    int64
	PoolQueued          int64
	PoolRetryWait       int64
	ReservationState    string
	ReservedMinor       int64
	UnsettledPosted     int64
	CancellationRecords int64
	Charges             int64
	CancellationEvents  int64
}

type cancellationStopMutationState struct {
	JobState         string
	JobVersion       int64
	WorkerState      string
	ReservationState string
	ReservedMinor    int64
	UnsettledPosted  int64
	Receipts         int64
	Charges          int64
	CanceledEvents   int64
}

func newCancelingTestFixture(
	t *testing.T,
	key string,
	originalLeaseExpired bool,
) (startFixture, cancelResponse, *cancellation.Service) {
	t.Helper()
	fixture := newStartFixture(t, key, 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	if _, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil {
		t.Fatalf("start cancellation stop fixture: %v", err)
	}
	if originalLeaseExpired {
		if _, err := fixture.database.Admin.Exec(`
			UPDATE attempt_leases
			SET expires_at = issued_at + interval '1 microsecond',
				updated_at = clock_timestamp()
			WHERE attempt_id = $1 AND revoked_at IS NULL
		`, fixture.assignment.AttemptID); err != nil {
			t.Fatalf("expire original Lease before cancellation: %v", err)
		}
	}
	server := admissionServerForDatabase(t, fixture.database)
	canceled := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel stop fixture status = %d; body=%s", canceled.StatusCode, canceled.Body)
	}
	var cancellationResult cancelResponse
	if err := json.Unmarshal(canceled.Body, &cancellationResult); err != nil {
		t.Fatalf("decode stop fixture cancellation: %v", err)
	}
	if cancellationResult.Decision != "CANCELING" || cancellationResult.Charge == nil {
		t.Fatalf("stop fixture cancellation = %#v", cancellationResult)
	}
	coordinator := cancellation.NewService(
		newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password"),
		newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password"),
	)
	return fixture, cancellationResult, coordinator
}

func newQueueCancellationRollbackFixture(
	t *testing.T,
	jobState, key string,
) (testDatabase, string, uuid.UUID) {
	t.Helper()
	if jobState == "RETRY_WAIT" {
		fixture := newAssignmentFixture(t, key, 7)
		if _, err := fixture.database.Admin.Exec(`
			UPDATE credentials
			SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
			WHERE id = $1
		`, testCredentialID); err != nil {
			t.Fatalf("grant cancellation scope: %v", err)
		}
		assignment, err := fixture.service.Acquire(
			context.Background(), fixture.worker, 7, &fixture.candidate,
		)
		if err != nil {
			t.Fatalf("create RETRY_WAIT rollback Assignment: %v", err)
		}
		failed, err := fixture.service.Fail(
			context.Background(),
			fixture.worker,
			leaseCredentials(assignment),
			validFailureObservation(),
		)
		if err != nil {
			t.Fatalf("create RETRY_WAIT rollback state: %v", err)
		}
		if failed.Disposition != workercontrol.RetryDispositionRetryWait {
			t.Fatalf("rollback fixture failure disposition = %s, want RETRY_WAIT", failed.Disposition)
		}
		server := admissionServerForDatabase(t, fixture.database)
		return fixture.database, server.URL, assignment.JobID
	}
	if jobState != "QUEUED" {
		t.Fatalf("unsupported queue cancellation rollback state %q", jobState)
	}

	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, key, []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"queue cancellation must roll back every write"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode queue rollback Job: %v", err)
	}
	return database, server.URL, uuid.MustParse(job.JobID)
}

func readQueueCancellationMutationState(
	t *testing.T,
	db interface {
		QueryRow(query string, args ...any) *sql.Row
	},
	jobID uuid.UUID,
) queueCancellationMutationState {
	t.Helper()
	var state queueCancellationMutationState
	if err := db.QueryRow(`
		SELECT
			job.state,
			job.version,
			job.current_fence,
			project.queued_count,
			project.retry_wait_count,
			pool.queued_count,
			pool.retry_wait_count,
			reservation.state,
			credit.reserved_minor,
			credit.unsettled_posted_minor,
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = job.id
			   AND event_type IN (
				'job.cancel_requested',
				'job.canceling',
				'charge.posted',
				'invoice.export_requested',
				'job.canceled'
			   ))
		FROM jobs AS job
		JOIN projects AS project ON project.id = job.project_id
		JOIN worker_pools AS pool ON pool.id = job.worker_pool_id
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN organization_credit_accounts AS credit
		  ON credit.organization_id = job.organization_id
		WHERE job.id = $1
	`, jobID).Scan(
		&state.JobState,
		&state.JobVersion,
		&state.JobFence,
		&state.ProjectQueued,
		&state.ProjectRetryWait,
		&state.PoolQueued,
		&state.PoolRetryWait,
		&state.ReservationState,
		&state.ReservedMinor,
		&state.UnsettledPosted,
		&state.CancellationRecords,
		&state.Charges,
		&state.CancellationEvents,
	); err != nil {
		t.Fatalf("read queue cancellation mutation state: %v", err)
	}
	return state
}

func readCancellationStopMutationState(
	t *testing.T,
	db interface {
		QueryRow(query string, args ...any) *sql.Row
	},
	jobID uuid.UUID,
) cancellationStopMutationState {
	t.Helper()
	var state cancellationStopMutationState
	if err := db.QueryRow(`
		SELECT
			job.state,
			job.version,
			worker.lifecycle_state,
			reservation.state,
			credit.reserved_minor,
			credit.unsettled_posted_minor,
			(SELECT count(*) FROM cancellation_stop_receipts WHERE job_id = job.id),
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = job.id AND event_type = 'job.canceled')
		FROM jobs AS job
		JOIN job_cancellation_decisions AS decision ON decision.job_id = job.id
		JOIN workers AS worker ON worker.id = decision.worker_id
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN organization_credit_accounts AS credit
		  ON credit.organization_id = job.organization_id
		WHERE job.id = $1
	`, jobID).Scan(
		&state.JobState,
		&state.JobVersion,
		&state.WorkerState,
		&state.ReservationState,
		&state.ReservedMinor,
		&state.UnsettledPosted,
		&state.Receipts,
		&state.Charges,
		&state.CanceledEvents,
	); err != nil {
		t.Fatalf("read cancellation stop mutation state: %v", err)
	}
	return state
}

func readCancellationMutationState(
	t *testing.T,
	db interface {
		QueryRow(query string, args ...any) *sql.Row
	},
	jobID, attemptID uuid.UUID,
) cancellationMutationState {
	t.Helper()
	var state cancellationMutationState
	if err := db.QueryRow(`
		SELECT
			job.state,
			job.version,
			job.current_fence,
			attempt.state,
			attempt.ended_at IS NOT NULL,
			lease.revoked_at IS NOT NULL,
			worker.lifecycle_state,
			project.running_count,
			reservation.state,
			credit.reserved_minor,
			credit.unsettled_posted_minor,
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = job.id
			   AND event_type IN (
				'job.cancel_requested',
				'job.canceling',
				'charge.posted',
				'invoice.export_requested',
				'job.canceled'
			   ))
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.id = $2
		JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id
		JOIN workers AS worker ON worker.id = attempt.worker_id
		JOIN projects AS project ON project.id = job.project_id
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN organization_credit_accounts AS credit
		  ON credit.organization_id = job.organization_id
		WHERE job.id = $1
	`, jobID, attemptID).Scan(
		&state.JobState,
		&state.JobVersion,
		&state.JobFence,
		&state.AttemptState,
		&state.AttemptEnded,
		&state.LeaseRevoked,
		&state.WorkerState,
		&state.ProjectRunning,
		&state.ReservationState,
		&state.ReservedMinor,
		&state.UnsettledPosted,
		&state.CancellationRecords,
		&state.Charges,
		&state.CancellationEvents,
	); err != nil {
		t.Fatalf("read cancellation mutation state: %v", err)
	}
	return state
}

type cancelResponse struct {
	CancellationID string          `json:"cancellation_id"`
	JobID          string          `json:"job_id"`
	Decision       string          `json:"decision"`
	State          string          `json:"state"`
	JobVersion     int64           `json:"job_version"`
	Billable       bool            `json:"billable"`
	Charge         *chargeResponse `json:"charge"`
	DecidedAt      time.Time       `json:"decided_at"`
}

type chargeResponse struct {
	ChargeID string    `json:"charge_id"`
	Amount   int64     `json:"amount_minor"`
	Currency string    `json:"currency"`
	Reason   string    `json:"reason"`
	PostedAt time.Time `json:"posted_at"`
}

func cancelJob(
	t *testing.T,
	serverURL, projectID, jobID, bearerCredential string,
) httpResult {
	t.Helper()
	result, err := doCancelJob(serverURL, projectID, jobID, bearerCredential)
	if err != nil {
		t.Fatalf("cancel Job: %v", err)
	}
	return result
}

func doCancelJob(
	serverURL, projectID, jobID, bearerCredential string,
) (httpResult, error) {
	request, err := http.NewRequest(
		http.MethodPost,
		serverURL+"/v1/projects/"+projectID+"/jobs/"+jobID+"/cancel",
		nil,
	)
	if err != nil {
		return httpResult{}, fmt.Errorf("create cancellation request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+bearerCredential)
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return httpResult{}, fmt.Errorf("send cancellation request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return httpResult{}, fmt.Errorf("read cancellation response: %w", err)
	}
	return httpResult{StatusCode: response.StatusCode, Header: response.Header.Clone(), Body: body}, nil
}
