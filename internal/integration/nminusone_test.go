//go:build integration

package integration_test

import (
	"archive/tar"
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const (
	nMinusOneRenewalControlCommit        = "450dd5c379ed7d26588e2a76140f0b3281acfbb2"
	nMinusOneFailureControlCommit        = "9cb1a522e20490ef41bab535fde206a947118d11"
	nMinusOneCancellationCommit          = "d0a8c0105a09b7f538e79400a7affd2a6c700744"
	finalizationFixedPointCommit         = "c94e140c8e841e88bdfcc41725bd7aa5ea7ac068"
	hierarchicalSchedulerNMinusOneCommit = "8d4fd9199348d5ccdca48c40ef3b4a19ee5c5284"
	invoiceExportNMinusOneCommit         = "96760832c3b652827a9c5ce1732fbfa773ed0759"
	profileCircuitNMinusOneCommit        = "e1c50543d854cbad0cc5a98fdaac51e78e2c837c"
	webhookNMinusOneCommit               = "53f5d650adc30bacdcf9478786d71c1dcf6c1def"
	humanOIDCNMinusOneCommit             = "cedd0e8031d013a9a12327259b75fdc9749053ee"
)

func TestExactHumanOIDCNMinusOneControlAndServiceRequestRemainCompatible(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, humanOIDCNMinusOneCommit)
	assertWebhookNMinusOneDatabaseStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)
	seedAdmissionFixture(t, database.Admin)
	seedNMinusOneProfileCircuitWorker(t, database.Admin, uuid.New(), "human-oidc")
	jobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	var createdByPrincipalID, actorKind string
	if err := database.Admin.QueryRow(`
		SELECT job.created_by_principal_id::text, attribution.principal_kind::text
		FROM jobs AS job
		JOIN project_principal_attributions AS attribution
		  ON attribution.organization_id = job.organization_id
		 AND attribution.project_id = job.project_id
		 AND attribution.principal_id = job.created_by_principal_id
		WHERE job.id = $1
	`, jobID).Scan(&createdByPrincipalID, &actorKind); err != nil {
		t.Fatalf("read Human-slice N-1 Service Job: %v", err)
	}
	if createdByPrincipalID != testPrincipalID || actorKind != "SERVICE" {
		t.Fatalf(
			"Human-slice N-1 Job attribution = principal %s kind %s",
			createdByPrincipalID,
			actorKind,
		)
	}
}

func TestExactWebhookNMinusOneControlAndTerminalWriterRemainCompatible(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, webhookNMinusOneCommit)
	assertWebhookNMinusOneDatabaseStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)
	seedAdmissionFixture(t, database.Admin)
	seedNMinusOneProfileCircuitWorker(t, database.Admin, uuid.New(), "webhook")
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope for webhook N-1 writer: %v", err)
	}
	subscriptionID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO webhook_subscriptions (
			id, organization_id, project_id, endpoint_url, event_types,
			created_by_principal_id, created_by_credential_id
		) VALUES (
			$1, $2, $3, 'https://hooks.example.com/n-minus-one',
			ARRAY['job.canceled']::webhook_event_type[], $4, $5
		)
	`, subscriptionID, testOrganizationID, testProjectID, testPrincipalID, testCredentialID); err != nil {
		t.Fatalf("seed webhook N-1 Subscription: %v", err)
	}

	jobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	result := runNMinusOneInvoiceChargeProbe(
		t,
		nMinusOne.InvoiceChargeProbe,
		database.DSN,
		uuid.MustParse(jobID),
	)
	if result.Decision != "CANCELED" || result.ChargeID != "" {
		t.Fatalf("webhook N-1 cancellation result = %#v", result)
	}

	var eventType, deliveryState, deliveryJobID string
	var deliveryCount int
	if err := database.Admin.QueryRow(`
		SELECT count(*), min(delivery.event_type), min(delivery.state::text),
			min(delivery.job_id::text)
		FROM webhook_deliveries AS delivery
		WHERE delivery.subscription_id = $1
	`, subscriptionID).Scan(&deliveryCount, &eventType, &deliveryState, &deliveryJobID); err != nil {
		t.Fatalf("read webhook N-1 Delivery: %v", err)
	}
	if deliveryCount != 1 || eventType != "job.canceled" || deliveryState != "PENDING" ||
		deliveryJobID != jobID {
		t.Fatalf(
			"webhook N-1 Delivery count=%d type=%s state=%s job=%s",
			deliveryCount,
			eventType,
			deliveryState,
			deliveryJobID,
		)
	}
}

func assertWebhookNMinusOneDatabaseStartupPassed(t *testing.T, output string) {
	t.Helper()
	if strings.Contains(output, "open auth database pool") ||
		strings.Contains(output, "open request database pool") ||
		strings.Contains(output, "open Artifact request database pool") ||
		strings.Contains(output, "open cancellation database pool") ||
		strings.Contains(output, "open internal database pool") ||
		strings.Contains(output, "open Scheduler database pool") ||
		strings.Contains(output, "open billing database pool") ||
		!strings.Contains(output, "open Invoice export bearer token file") {
		t.Fatalf("webhook N-1 startup did not reach Invoice token sentinel:\n%s", output)
	}
}

func TestExactProfileCircuitNMinusOneWriterAcrossProtocolGate(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, profileCircuitNMinusOneCommit)
	assertProfileCircuitNMinusOneDatabaseStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)
	seedAdmissionFixture(t, database.Admin)

	gateOffWorkerID := uuid.New()
	seedNMinusOneProfileCircuitWorker(t, database.Admin, gateOffWorkerID, "gate-off")
	gateOffJobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	gateOff, err := runNMinusOneFailureProbeProcess(
		t,
		nMinusOne.FailureProbe,
		database.DSN,
		gateOffJobID,
		gateOffWorkerID.String(),
	)
	if err != nil {
		t.Fatalf("run gate-off Profile circuit N-1 writer: %v\n%s", err, gateOff)
	}
	var gateOffResult nMinusOneFailureProbeResult
	if err := json.Unmarshal(gateOff, &gateOffResult); err != nil {
		t.Fatalf("decode gate-off Profile circuit N-1 result: %v\n%s", err, gateOff)
	}
	if gateOffResult.Disposition != "RETRY_WAIT" || gateOffResult.AttemptID == "" {
		t.Fatalf("gate-off Profile circuit N-1 result = %#v", gateOffResult)
	}
	var protocol int
	var workerWasHealthy bool
	var certificationState string
	var openingCount int
	if err := database.Admin.QueryRow(`
		SELECT
			decision.circuit_protocol_version,
			decision.worker_was_healthy,
			certification.state::text,
			(SELECT count(*) FROM profile_certification_circuit_openings)
		FROM execution_failure_decisions AS decision
		JOIN attempts AS attempt ON attempt.id = decision.attempt_id
		JOIN profile_certifications AS certification
		  ON certification.id = attempt.profile_certification_id
		WHERE decision.attempt_id = $1
	`, gateOffResult.AttemptID).Scan(
		&protocol,
		&workerWasHealthy,
		&certificationState,
		&openingCount,
	); err != nil {
		t.Fatalf("read gate-off Profile circuit N-1 decision: %v", err)
	}
	if protocol != 1 || workerWasHealthy || certificationState != "ACTIVE" || openingCount != 0 {
		t.Fatalf(
			"gate-off N-1 decision = protocol %d healthy %t certification %s openings %d",
			protocol,
			workerWasHealthy,
			certificationState,
			openingCount,
		)
	}

	setProfileCircuitProtocolGate(
		t,
		database.Admin,
		true,
		"exact e1c5054 failure writers drained",
	)
	assertProfileCircuitNMinusOneDatabaseStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)
	gateOnWorkerID := uuid.New()
	seedNMinusOneProfileCircuitWorker(t, database.Admin, gateOnWorkerID, "gate-on")
	gateOnJobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	gateOn, gateOnErr := runNMinusOneFailureProbeProcess(
		t,
		nMinusOne.FailureProbe,
		database.DSN,
		gateOnJobID,
		gateOnWorkerID.String(),
	)
	if gateOnErr == nil ||
		!strings.Contains(string(gateOn), "failure decision writer does not match the active Profile circuit protocol") ||
		!strings.Contains(string(gateOn), "SQLSTATE 55000") {
		t.Fatalf("gate-on Profile circuit N-1 writer error=%v\n%s", gateOnErr, gateOn)
	}

	var (
		jobState          string
		attemptState      string
		leaseRevoked      bool
		workerState       string
		decisionCount     int
		jobOpeningCount   int
		circuitState      string
		projectRunning    int
		creditReservation string
	)
	if err := database.Admin.QueryRow(`
		SELECT
			job.state::text,
			attempt.state::text,
			lease.revoked_at IS NOT NULL,
			worker.lifecycle_state::text,
			(SELECT count(*) FROM execution_failure_decisions WHERE job_id = job.id),
			(SELECT count(*) FROM profile_certification_circuit_openings
			 WHERE triggering_job_id = job.id),
			runtime.circuit_breaker_state::text,
			project.running_count,
			reservation.state::text
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.job_id = job.id
		JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id
		JOIN workers AS worker ON worker.id = attempt.worker_id
		JOIN retry_runtime_states AS runtime ON runtime.job_id = job.id
		JOIN projects AS project ON project.id = job.project_id
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		WHERE job.id = $1
	`, gateOnJobID).Scan(
		&jobState,
		&attemptState,
		&leaseRevoked,
		&workerState,
		&decisionCount,
		&jobOpeningCount,
		&circuitState,
		&projectRunning,
		&creditReservation,
	); err != nil {
		t.Fatalf("read rejected gate-on Profile circuit N-1 state: %v", err)
	}
	if jobState != "RUNNING" || attemptState != "RUNNING" || leaseRevoked ||
		workerState != "BUSY" || decisionCount != 0 || jobOpeningCount != 0 ||
		circuitState != "{}" || projectRunning != 1 || creditReservation != "RESERVED" {
		t.Fatalf(
			"rejected gate-on N-1 state = job %s attempt %s revoked %t worker %s decisions/openings %d/%d circuit %s running %d reservation %s",
			jobState,
			attemptState,
			leaseRevoked,
			workerState,
			decisionCount,
			jobOpeningCount,
			circuitState,
			projectRunning,
			creditReservation,
		)
	}
}

func assertProfileCircuitNMinusOneDatabaseStartupPassed(t *testing.T, output string) {
	t.Helper()
	if strings.Contains(output, "open auth database pool") ||
		strings.Contains(output, "open request database pool") ||
		strings.Contains(output, "open Artifact request database pool") ||
		strings.Contains(output, "open cancellation database pool") ||
		strings.Contains(output, "open internal database pool") ||
		strings.Contains(output, "open Scheduler database pool") ||
		strings.Contains(output, "open billing database pool") ||
		!strings.Contains(output, "open Invoice export bearer token file") {
		t.Fatalf("Profile circuit N-1 startup did not reach Invoice token sentinel:\n%s", output)
	}
}

func seedNMinusOneProfileCircuitWorker(t *testing.T, database *sql.DB, workerID uuid.UUID, label string) {
	t.Helper()
	if _, err := database.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1,
			'00000000-0000-0000-0000-000000000005',
			$2,
			7,
			'READY',
			'HEALTHY'
		)
	`, workerID, "spiffe://vela.internal/worker/profile-circuit-n-minus-one-"+label); err != nil {
		t.Fatalf("seed %s Profile circuit N-1 Worker: %v", label, err)
	}
	if _, err := database.Exec(`
		INSERT INTO worker_profile_readiness (
			worker_id, worker_epoch, execution_profile_revision_id, readiness
		) VALUES (
			$1, 7, '00000000-0000-0000-0000-000000000014', 'WARM'
		)
	`, workerID); err != nil {
		t.Fatalf("seed %s Profile circuit N-1 Worker readiness: %v", label, err)
	}
}

func TestExactInvoiceExportNMinusOneWriterCreatesExportAuthority(t *testing.T) {
	fixture := newStartFixture(t, "invoice-export-n-minus-one-writer", 7)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope for Invoice N-1 writer: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start Invoice N-1 writer fixture = %#v error=%v", started, err)
	}
	nMinusOne := buildNMinusOneBinaries(t, invoiceExportNMinusOneCommit)
	result := runNMinusOneInvoiceChargeProbe(
		t,
		nMinusOne.InvoiceChargeProbe,
		fixture.database.DSN,
		fixture.assignment.JobID,
	)
	if result.Decision != "CANCELING" || result.ChargeID == "" {
		t.Fatalf("Invoice N-1 Charge result = %#v", result)
	}
	chargeID, err := uuid.Parse(result.ChargeID)
	if err != nil {
		t.Fatalf("parse Invoice N-1 Charge id: %v", err)
	}
	var state string
	var attempts, exports, invoiceEvents int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			export.state,
			export.attempts,
			(SELECT count(*) FROM invoice_exports WHERE charge_id = export.charge_id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = export.job_id AND event_type = 'invoice.export_requested')
		FROM invoice_exports AS export
		WHERE export.charge_id = $1
	`, chargeID).Scan(&state, &attempts, &exports, &invoiceEvents); err != nil {
		t.Fatalf("read Invoice N-1 export authority: %v", err)
	}
	if state != "PENDING" || attempts != 0 || exports != 1 || invoiceEvents != 1 {
		t.Fatalf(
			"Invoice N-1 authority = state %s attempts %d exports/events %d/%d",
			state,
			attempts,
			exports,
			invoiceEvents,
		)
	}
}

func TestNMinusOneControlStartupAcrossRenewalProtocolTransition(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, nMinusOneRenewalControlCommit)
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")

	assertNMinusOneDatabaseStartupPassed(t, runNMinusOneControl(t, nMinusOne.Control, database.DSN))
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err != nil {
		t.Fatalf("verify new request role during expand: %v", err)
	}
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "n-minus-one-writer-replay", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"prove the fixed-point writer and replay against the expanded schema"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit N-1 probe Job status = %d; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode N-1 probe Job: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/n-minus-one-probe', 7, 'READY', 'HEALTHY'
		)
	`, testWorkerID); err != nil {
		t.Fatalf("seed N-1 probe Worker: %v", err)
	}
	attemptID := runNMinusOneAssignmentProbe(
		t, nMinusOne.AssignmentProbe, database.DSN, job.JobID, testWorkerID, 7, 1,
	)
	var protocolVersion int16
	var claimMatchesExpiry bool
	if err := database.Admin.QueryRow(`
		SELECT renewal_protocol_version, token_claim_expires_at = expires_at
		FROM attempt_leases
		WHERE attempt_id = $1
	`, attemptID).Scan(&protocolVersion, &claimMatchesExpiry); err != nil {
		t.Fatalf("read N-1 Assignment protocol state: %v", err)
	}
	if protocolVersion != 1 || !claimMatchesExpiry {
		t.Fatalf("N-1 Assignment protocol version=%d claim_matches_expiry=%t", protocolVersion, claimMatchesExpiry)
	}
	if _, err := database.Admin.Exec(
		"UPDATE attempt_leases SET revoked_at = clock_timestamp() WHERE attempt_id = $1",
		attemptID,
	); err != nil {
		t.Fatalf("drain N-1 probe Lease before switch: %v", err)
	}

	if _, err := database.Admin.Exec(
		"SELECT vela_transition_execution_lease_renewal_protocol(true, 'N-1 startup integration switch')",
	); err != nil {
		t.Fatalf("enable renewal protocol: %v", err)
	}
	assertNMinusOneRequestStartupRejected(t, runNMinusOneControl(t, nMinusOne.Control, database.DSN))
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err != nil {
		t.Fatalf("verify new request role after switch: %v", err)
	}
	if _, err := database.Admin.Exec("GRANT SELECT ON retry_runtime_states TO vela_request"); err != nil {
		t.Fatalf("inject legacy request privilege after switch: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err == nil {
		t.Fatal("new request-role verification accepted legacy table access after switch")
	}
	if _, err := database.Admin.Exec(
		"SELECT vela_transition_execution_lease_renewal_protocol(true, 'repair switched privilege boundary')",
	); err != nil {
		t.Fatalf("repair switched request privilege boundary: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err != nil {
		t.Fatalf("verify repaired new request role after switch: %v", err)
	}

	if _, err := database.Admin.Exec(
		"SELECT vela_transition_execution_lease_renewal_protocol(false, 'N-1 startup integration rollback')",
	); err != nil {
		t.Fatalf("disable renewal protocol: %v", err)
	}
	assertNMinusOneDatabaseStartupPassed(t, runNMinusOneControl(t, nMinusOne.Control, database.DSN))
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err != nil {
		t.Fatalf("verify new request role after rollback: %v", err)
	}
}

func TestNMinusOneControlStartsAndWritesAfterExecutionFailureExpansion(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, nMinusOneFailureControlCommit)

	assertNMinusOneDatabaseStartupPassed(t, runNMinusOneControl(t, nMinusOne.Control, database.DSN))
	seedAdmissionFixture(t, database.Admin)
	jobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/n-minus-one-failure-probe', 7, 'READY', 'HEALTHY'
		)
	`, testWorkerID); err != nil {
		t.Fatalf("seed N-1 failure-expansion Worker: %v", err)
	}
	attemptID := runNMinusOneAssignmentProbe(
		t, nMinusOne.AssignmentProbe, database.DSN, jobID, testWorkerID, 7, 1,
	)

	var jobState, attemptState string
	var decisionCount int
	if err := database.Admin.QueryRow(`
		SELECT j.state, a.state, (SELECT count(*) FROM execution_failure_decisions)
		FROM jobs AS j
		JOIN attempts AS a ON a.job_id = j.id
		WHERE j.id = $1 AND a.id = $2
	`, jobID, attemptID).Scan(&jobState, &attemptState, &decisionCount); err != nil {
		t.Fatalf("read N-1 failure-expansion writes: %v", err)
	}
	if jobState != "ASSIGNED" || attemptState != "ASSIGNED" || decisionCount != 0 {
		t.Fatalf(
			"N-1 failure-expansion state = job %s attempt %s decisions %d",
			jobState, attemptState, decisionCount,
		)
	}
}

func TestNMinusOneControlStartsAndWritesAfterCustomerCancellationExpansion(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, nMinusOneCancellationCommit)

	assertNMinusOneDatabaseStartupPassed(t, runNMinusOneControl(t, nMinusOne.Control, database.DSN))
	seedAdmissionFixture(t, database.Admin)
	jobID := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/n-minus-one-cancellation-probe', 7, 'READY', 'HEALTHY'
		)
	`, testWorkerID); err != nil {
		t.Fatalf("seed N-1 cancellation-expansion Worker: %v", err)
	}
	attemptID := runNMinusOneAssignmentProbe(
		t, nMinusOne.AssignmentProbe, database.DSN, jobID, testWorkerID, 7, 1,
	)

	var jobState, attemptState string
	var cancellationDecisions, charges, stopReceipts int
	if err := database.Admin.QueryRow(`
		SELECT
			job.state,
			attempt.state,
			(SELECT count(*) FROM job_cancellation_decisions),
			(SELECT count(*) FROM charges),
			(SELECT count(*) FROM cancellation_stop_receipts)
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.job_id = job.id
		WHERE job.id = $1 AND attempt.id = $2
	`, jobID, attemptID).Scan(
		&jobState,
		&attemptState,
		&cancellationDecisions,
		&charges,
		&stopReceipts,
	); err != nil {
		t.Fatalf("read N-1 cancellation-expansion writes: %v", err)
	}
	if jobState != "ASSIGNED" || attemptState != "ASSIGNED" ||
		cancellationDecisions != 0 || charges != 0 || stopReceipts != 0 {
		t.Fatalf(
			"N-1 cancellation-expansion state = job %s attempt %s decisions/charges/receipts %d/%d/%d",
			jobState,
			attemptState,
			cancellationDecisions,
			charges,
			stopReceipts,
		)
	}
}

func TestExactFinalizationFixedPointStartsWritesAndForwardsUnknownSuccess(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, finalizationFixedPointCommit)

	assertNMinusOneDatabaseStartupPassed(t, runNMinusOneControl(t, nMinusOne.Control, database.DSN))
	seedAdmissionFixture(t, database.Admin)
	jobIDText := runNMinusOneAdmissionProbe(t, nMinusOne.AdmissionProbe, database.DSN)
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/n-minus-one-finalization-probe', 7, 'READY', 'HEALTHY'
		)
	`, testWorkerID); err != nil {
		t.Fatalf("seed exact fixed-point Worker: %v", err)
	}
	attemptID := runNMinusOneAssignmentProbe(
		t,
		nMinusOne.AssignmentProbe,
		database.DSN,
		jobIDText,
		testWorkerID,
		7,
		1,
	)
	jobID := uuid.MustParse(jobIDText)
	var jobState, attemptState string
	var artifacts, artifactSets int
	if err := database.Admin.QueryRow(`
		SELECT
			job.state,
			attempt.state,
			(SELECT count(*) FROM artifacts),
			(SELECT count(*) FROM artifact_sets)
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.job_id = job.id
		WHERE job.id = $1 AND attempt.id = $2
	`, jobID, attemptID).Scan(&jobState, &attemptState, &artifacts, &artifactSets); err != nil {
		t.Fatalf("read exact fixed-point expansion writes: %v", err)
	}
	if jobState != "ASSIGNED" || attemptState != "ASSIGNED" || artifacts != 0 || artifactSets != 0 {
		t.Fatalf(
			"exact fixed-point expansion state = %s/%s artifacts/sets %d/%d",
			jobState,
			attemptState,
			artifacts,
			artifactSets,
		)
	}

	eventID := uuid.New()
	successPayload, err := proto.Marshal(&velav1.EventEnvelope{
		EventId:          eventID.String(),
		AggregateType:    "Job",
		AggregateId:      jobID.String(),
		AggregateVersion: 3,
		EventType:        "job.succeeded",
		SchemaVersion:    1,
		Payload: &velav1.EventEnvelope_JobSucceeded{JobSucceeded: &velav1.JobSucceeded{
			OrganizationId: testOrganizationID,
			ProjectId:      testProjectID,
			JobId:          jobID.String(),
			AttemptId:      attemptID,
			AttemptFence:   1,
			ArtifactSetId:  uuid.NewString(),
			ManifestSha256: bytes.Repeat([]byte{0x2a}, 32),
			ChargeId:       uuid.NewString(),
			Artifacts: []*velav1.ArtifactSnapshot{{
				ArtifactId:      uuid.NewString(),
				Kind:            "VIDEO",
				Ordinal:         0,
				ObjectKey:       "artifacts/org/project/job/attempt/artifact/video.mp4",
				ObjectVersionId: "version-0001",
				SizeBytes:       1024,
				Sha256:          bytes.Repeat([]byte{0x7b}, 32),
				ContentType:     "video/mp4",
			}},
		}},
	})
	if err != nil {
		t.Fatalf("marshal fixed-point unknown success event: %v", err)
	}
	if _, err := database.Admin.Exec("DELETE FROM outbox_events WHERE aggregate_id = $1", jobID); err != nil {
		t.Fatalf("remove fixed-point Admission event: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO outbox_events (
			event_id, organization_id, project_id, aggregate_type, aggregate_id,
			aggregate_version, event_type, schema_version, payload, occurred_at
		) VALUES ($1, $2, $3, 'Job', $4, 3, 'job.succeeded', 1, $5, clock_timestamp())
	`, eventID, testOrganizationID, testProjectID, jobID, successPayload); err != nil {
		t.Fatalf("insert fixed-point unknown success event: %v", err)
	}
	forwarded := runNMinusOneOutboxProbe(t, nMinusOne.OutboxProbe, database.DSN)
	if forwarded.Subject != "vela.events.job.succeeded" ||
		forwarded.MessageID != eventID.String() || forwarded.KnownPayload ||
		forwarded.UnknownBytes == 0 || !bytes.Equal(forwarded.Payload, successPayload) {
		t.Fatalf("fixed-point success forwarding = %#v", forwarded)
	}
	var published bool
	var stream string
	var sequence int64
	var attempts int
	if err := database.Admin.QueryRow(`
		SELECT published_at IS NOT NULL, broker_stream, broker_sequence, publish_attempts
		FROM outbox_events
		WHERE event_id = $1
	`, eventID).Scan(&published, &stream, &sequence, &attempts); err != nil {
		t.Fatalf("read fixed-point success PubAck receipt: %v", err)
	}
	if !published || stream != "N_MINUS_ONE_PROBE" || sequence != 1 || attempts != 1 {
		t.Fatalf(
			"fixed-point success PubAck = published %t stream %s sequence %d attempts %d",
			published,
			stream,
			sequence,
			attempts,
		)
	}
}

func TestExactSchedulerNMinusOneAssignmentWriterAcrossProtocolGate(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, hierarchicalSchedulerNMinusOneCommit)
	startupOutput := runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN)
	if strings.Contains(startupOutput, "VELA_SCHEDULER_") ||
		strings.Contains(startupOutput, "open auth database pool") ||
		strings.Contains(startupOutput, "open request database pool") ||
		strings.Contains(startupOutput, "open Artifact request database pool") ||
		strings.Contains(startupOutput, "open cancellation database pool") ||
		strings.Contains(startupOutput, "open internal database pool") ||
		!strings.Contains(startupOutput, "open Artifact S3 access key ID file") {
		t.Fatalf("exact Scheduler N-1 startup did not reach Artifact sentinel:\n%s", startupOutput)
	}
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	firstJob := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-n-minus-one-gate-off",
	)
	secondJob := submitSchedulerJob(
		t,
		server.URL,
		testProjectID,
		testBearerCredential(),
		"scheduler-n-minus-one-gate-on",
	)
	firstWorker := uuid.MustParse("00000000-0000-0000-0000-000000000430")
	secondWorker := uuid.MustParse("00000000-0000-0000-0000-000000000431")
	for index, workerID := range []uuid.UUID{firstWorker, secondWorker} {
		if _, err := database.Admin.Exec(`
			INSERT INTO workers (
				id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
			) VALUES (
				$1, '00000000-0000-0000-0000-000000000005', $2, 7, 'READY', 'HEALTHY'
			)
		`, workerID, fmt.Sprintf("spiffe://vela.internal/worker/scheduler-n-minus-one-%d", index)); err != nil {
			t.Fatalf("seed Scheduler N-1 Worker %d: %v", index, err)
		}
	}

	firstAttemptID := runNMinusOneAssignmentProbe(
		t,
		nMinusOne.AssignmentProbe,
		database.DSN,
		firstJob.String(),
		firstWorker.String(),
		7,
		1,
	)
	var hasDispatchIntent bool
	if err := database.Admin.QueryRow(`
		SELECT scheduler_dispatch_intent_id IS NOT NULL
		FROM attempts WHERE id = $1
	`, firstAttemptID).Scan(&hasDispatchIntent); err != nil {
		t.Fatalf("read gate-off N-1 Attempt: %v", err)
	}
	if hasDispatchIntent {
		t.Fatal("gate-off N-1 Attempt unexpectedly has a Scheduler dispatch intent")
	}

	if _, err := database.Admin.Exec(`
		SELECT vela_transition_scheduler_dispatch_protocol(
			true,
			'exact 8d4fd91 writer drained before Scheduler protocol switch'
		)
	`); err != nil {
		t.Fatalf("enable Scheduler dispatch protocol: %v", err)
	}
	before := readAssignmentState(t, database.Admin, secondJob, secondWorker)
	output, probeErr := runNMinusOneAssignmentProbeProcess(
		t,
		nMinusOne.AssignmentProbe,
		database.DSN,
		secondJob.String(),
		secondWorker.String(),
		7,
		1,
	)
	if probeErr == nil || !strings.Contains(
		string(output),
		"Assignment requires a live Scheduler dispatch claim",
	) {
		t.Fatalf("gate-on Scheduler N-1 writer error=%v\n%s", probeErr, output)
	}
	after := readAssignmentState(t, database.Admin, secondJob, secondWorker)
	if after != before {
		t.Fatalf("rejected Scheduler N-1 writer changed state: before=%+v after=%+v", before, after)
	}
}

func TestNMinusOneOutboxPublisherForwardsUnknownCancellationPayloadBytes(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t, nMinusOneCancellationCommit)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "n-minus-one-unknown-cancellation-event", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"old publisher must preserve the new cancellation event bytes"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit N-1 Outbox probe Job status = %d; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode N-1 Outbox probe Job: %v", err)
	}
	jobID := uuid.MustParse(job.JobID)
	eventID := uuid.New()
	cancellationID := uuid.New()
	chargeID := uuid.New()
	payload, err := proto.Marshal(&velav1.EventEnvelope{
		EventId:          eventID.String(),
		AggregateType:    "Job",
		AggregateId:      jobID.String(),
		AggregateVersion: 2,
		EventType:        "job.canceling",
		SchemaVersion:    1,
		Payload: &velav1.EventEnvelope_JobCanceling{JobCanceling: &velav1.JobCanceling{
			OrganizationId:    testOrganizationID,
			ProjectId:         testProjectID,
			JobId:             jobID.String(),
			CancellationId:    cancellationID.String(),
			CancellationFence: 8,
			ChargeId:          chargeID.String(),
		}},
	})
	if err != nil {
		t.Fatalf("marshal unknown-to-N-1 cancellation event: %v", err)
	}
	if _, err := database.Admin.Exec("DELETE FROM outbox_events WHERE aggregate_id = $1", jobID); err != nil {
		t.Fatalf("remove Admission event before N-1 Outbox probe: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO outbox_events (
			event_id,
			organization_id,
			project_id,
			aggregate_type,
			aggregate_id,
			aggregate_version,
			event_type,
			schema_version,
			payload,
			occurred_at
		) VALUES ($1, $2, $3, 'Job', $4, 2, 'job.canceling', 1, $5, clock_timestamp())
	`, eventID, testOrganizationID, testProjectID, jobID, payload); err != nil {
		t.Fatalf("insert unknown-to-N-1 cancellation event: %v", err)
	}

	forwarded := runNMinusOneOutboxProbe(t, nMinusOne.OutboxProbe, database.DSN)
	if forwarded.Subject != "vela.events.job.canceling" || forwarded.MessageID != eventID.String() {
		t.Fatalf("N-1 publisher metadata = %q/%q", forwarded.Subject, forwarded.MessageID)
	}
	if forwarded.KnownPayload || forwarded.UnknownBytes == 0 {
		t.Fatalf(
			"N-1 descriptor classified cancellation payload as known=%t unknown_bytes=%d",
			forwarded.KnownPayload,
			forwarded.UnknownBytes,
		)
	}
	if !bytes.Equal(forwarded.Payload, payload) {
		t.Fatalf("N-1 publisher changed unknown cancellation payload bytes: got %x want %x", forwarded.Payload, payload)
	}

	var published bool
	var stream string
	var sequence int64
	var attempts int
	if err := database.Admin.QueryRow(`
		SELECT published_at IS NOT NULL, broker_stream, broker_sequence, publish_attempts
		FROM outbox_events
		WHERE event_id = $1
	`, eventID).Scan(&published, &stream, &sequence, &attempts); err != nil {
		t.Fatalf("read N-1 Outbox publish receipt: %v", err)
	}
	if !published || stream != "N_MINUS_ONE_PROBE" || sequence != 1 || attempts != 1 {
		t.Fatalf("N-1 Outbox receipt = published %t stream %s sequence %d attempts %d", published, stream, sequence, attempts)
	}

	successEventID := uuid.New()
	artifactSetID := uuid.New()
	successChargeID := uuid.New()
	artifactID := uuid.New()
	successPayload, err := proto.Marshal(&velav1.EventEnvelope{
		EventId:          successEventID.String(),
		AggregateType:    "Job",
		AggregateId:      jobID.String(),
		AggregateVersion: 3,
		EventType:        "job.succeeded",
		SchemaVersion:    1,
		Payload: &velav1.EventEnvelope_JobSucceeded{JobSucceeded: &velav1.JobSucceeded{
			OrganizationId: testOrganizationID,
			ProjectId:      testProjectID,
			JobId:          jobID.String(),
			AttemptId:      uuid.NewString(),
			AttemptFence:   8,
			ArtifactSetId:  artifactSetID.String(),
			ManifestSha256: bytes.Repeat([]byte{0x2a}, 32),
			ChargeId:       successChargeID.String(),
			Artifacts: []*velav1.ArtifactSnapshot{{
				ArtifactId:      artifactID.String(),
				Kind:            "VIDEO",
				Ordinal:         0,
				ObjectKey:       "artifacts/org/project/job/attempt/artifact/video.mp4",
				ObjectVersionId: "version-0001",
				SizeBytes:       1024,
				Sha256:          bytes.Repeat([]byte{0x7b}, 32),
				ContentType:     "video/mp4",
			}},
		},
		},
	})
	if err != nil {
		t.Fatalf("marshal unknown-to-N-1 success event: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO outbox_events (
			event_id,
			organization_id,
			project_id,
			aggregate_type,
			aggregate_id,
			aggregate_version,
			event_type,
			schema_version,
			payload,
			occurred_at
		) VALUES ($1, $2, $3, 'Job', $4, 3, 'job.succeeded', 1, $5, clock_timestamp())
	`, successEventID, testOrganizationID, testProjectID, jobID, successPayload); err != nil {
		t.Fatalf("insert unknown-to-N-1 success event: %v", err)
	}
	forwarded = runNMinusOneOutboxProbe(t, nMinusOne.OutboxProbe, database.DSN)
	if forwarded.Subject != "vela.events.job.succeeded" ||
		forwarded.MessageID != successEventID.String() {
		t.Fatalf("N-1 success publisher metadata = %q/%q", forwarded.Subject, forwarded.MessageID)
	}
	if forwarded.KnownPayload || forwarded.UnknownBytes == 0 {
		t.Fatalf(
			"N-1 descriptor classified success payload as known=%t unknown_bytes=%d",
			forwarded.KnownPayload,
			forwarded.UnknownBytes,
		)
	}
	if !bytes.Equal(forwarded.Payload, successPayload) {
		t.Fatalf(
			"N-1 publisher changed unknown success payload bytes: got %x want %x",
			forwarded.Payload,
			successPayload,
		)
	}
	if err := database.Admin.QueryRow(`
		SELECT published_at IS NOT NULL, broker_stream, broker_sequence, publish_attempts
		FROM outbox_events
		WHERE event_id = $1
	`, successEventID).Scan(&published, &stream, &sequence, &attempts); err != nil {
		t.Fatalf("read N-1 success Outbox publish receipt: %v", err)
	}
	if !published || stream != "N_MINUS_ONE_PROBE" || sequence != 1 || attempts != 1 {
		t.Fatalf(
			"N-1 success Outbox receipt = published %t stream %s sequence %d attempts %d",
			published,
			stream,
			sequence,
			attempts,
		)
	}
}

func TestNMinusOneAssignmentConsumesRetryWaitCreatedByNewControl(t *testing.T) {
	fixture := newAssignmentFixture(t, "n-minus-one-consumes-new-retry", 7)
	nMinusOne := buildNMinusOneBinaries(t, nMinusOneFailureControlCommit)
	first, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create first Assignment: %v", err)
	}
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, leaseCredentials(first),
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start first Assignment = %#v error=%v", started, err)
	}
	forceJobPolicySnapshot(
		t,
		fixture.database.Admin,
		first.JobID,
		`execution_retry_backoff_policy = '{"kind":"exponential","initial_seconds":1,"max_seconds":1}'::jsonb`,
	)
	decision, err := fixture.service.Fail(
		context.Background(),
		fixture.worker,
		leaseCredentials(first),
		validFailureObservation(),
	)
	if err != nil || decision.Disposition != workercontrol.RetryDispositionRetryWait ||
		decision.NextRetryAt == nil {
		t.Fatalf("new control RetryDecision = %#v error=%v", decision, err)
	}
	assertWaitingCompatibilityCounters(t, fixture.database.Admin, first.JobID, 1, 1, 1, 1)
	waitForDatabaseTimeAfter(t, fixture.database.Admin, *decision.NextRetryAt)

	output, probeErr := runNMinusOneAssignmentProbeProcess(
		t,
		nMinusOne.AssignmentProbe,
		fixture.database.DSN,
		first.JobID.String(),
		first.WorkerID.String(),
		7,
		decision.JobVersion,
	)
	if probeErr == nil || !strings.Contains(string(output), "Worker is excluded by protected Job retry evidence") {
		t.Fatalf("N-1 excluded Worker probe error=%v\n%s", probeErr, output)
	}
	assertWaitingCompatibilityCounters(t, fixture.database.Admin, first.JobID, 1, 1, 1, 1)
	server := admissionServerForDatabase(t, fixture.database)
	for index := range 10 {
		accepted := submitJob(t, server.URL, fmt.Sprintf("n-minus-one-full-normal-queue-%d", index), []byte(`{
			"model":"minimax-h3",
			"generation_preset":"balanced",
			"service_class":"standard",
			"output_spec":"video-1080p-5s-24fps",
			"generation_count":1,
			"prompt":"fill the real normal queue while an N-created Retry waits"
		}`))
		if accepted.StatusCode != 202 {
			t.Fatalf("submit normal queued Job %d status = %d; body=%s", index, accepted.StatusCode, accepted.Body)
		}
	}
	var normalQueuedJobs int
	if err := fixture.database.Admin.QueryRow(
		"SELECT count(*) FROM jobs WHERE state = 'QUEUED'",
	).Scan(&normalQueuedJobs); err != nil {
		t.Fatalf("count real normal queued Jobs: %v", err)
	}
	if normalQueuedJobs != 10 {
		t.Fatalf("normal queued Jobs = %d, want 10", normalQueuedJobs)
	}
	assertWaitingCompatibilityCounters(t, fixture.database.Admin, first.JobID, 11, 1, 11, 1)

	const replacementWorkerID = "00000000-0000-0000-0000-000000000291"
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/n-minus-one-retry-replacement',
			7, 'READY', 'HEALTHY'
		)
	`, replacementWorkerID); err != nil {
		t.Fatalf("seed N-1 replacement Worker: %v", err)
	}
	replacementAttemptID := runNMinusOneAssignmentProbe(
		t,
		nMinusOne.AssignmentProbe,
		fixture.database.DSN,
		first.JobID.String(),
		replacementWorkerID,
		7,
		decision.JobVersion,
	)

	var (
		jobState                string
		replacementWorker       string
		replacementFence        int64
		attemptsStarted         int
		projectQueued           int
		projectQueuedLimit      int
		projectRetryWait        int
		projectRunning          int
		poolQueued              int
		poolQueuedLimit         int
		poolRetryWait           int
		billableStartPreserved  bool
		visibleProgressRowCount int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			j.state,
			a.worker_id::text,
			a.fence,
			runtime.attempts_started,
			project.queued_count,
			project.queued_limit,
			project.retry_wait_count,
			project.running_count,
			pool.queued_count,
			pool.queued_limit,
			pool.retry_wait_count,
			j.billable_started_at = $3,
			(
				SELECT count(*)
				FROM attempt_progress AS progress
				WHERE progress.job_id = j.id
				  AND progress.fence = j.current_fence
			)
		FROM jobs AS j
		JOIN attempts AS a ON a.id = $2
		JOIN retry_runtime_states AS runtime ON runtime.job_id = j.id
		JOIN projects AS project ON project.id = j.project_id
		JOIN worker_pools AS pool ON pool.id = j.worker_pool_id
		WHERE j.id = $1
	`, first.JobID, replacementAttemptID, started.StartedAt).Scan(
		&jobState,
		&replacementWorker,
		&replacementFence,
		&attemptsStarted,
		&projectQueued,
		&projectQueuedLimit,
		&projectRetryWait,
		&projectRunning,
		&poolQueued,
		&poolQueuedLimit,
		&poolRetryWait,
		&billableStartPreserved,
		&visibleProgressRowCount,
	); err != nil {
		t.Fatalf("read N-1 retry replacement state: %v", err)
	}
	if jobState != "ASSIGNED" || replacementWorker != replacementWorkerID ||
		replacementFence <= decision.JobFence || attemptsStarted != 2 ||
		projectQueued != projectQueuedLimit || projectRetryWait != 0 || projectRunning != 1 ||
		poolQueued != poolQueuedLimit || poolRetryWait != 0 || !billableStartPreserved ||
		visibleProgressRowCount != 0 {
		t.Fatalf(
			"N-1 retry replacement = state %s worker %s fence %d attempts %d project %d(limit %d)/%d/%d pool %d(limit %d)/%d billable %t progress %d",
			jobState,
			replacementWorker,
			replacementFence,
			attemptsStarted,
			projectQueued,
			projectQueuedLimit,
			projectRetryWait,
			projectRunning,
			poolQueued,
			poolQueuedLimit,
			poolRetryWait,
			billableStartPreserved,
			visibleProgressRowCount,
		)
	}
}

func assertWaitingCompatibilityCounters(
	t *testing.T,
	database *sql.DB,
	jobID uuid.UUID,
	projectQueued int,
	projectRetryWait int,
	poolQueued int,
	poolRetryWait int,
) {
	t.Helper()
	var actualProjectQueued, actualProjectRetryWait, actualPoolQueued, actualPoolRetryWait int
	if err := database.QueryRow(`
		SELECT
			project.queued_count,
			project.retry_wait_count,
			pool.queued_count,
			pool.retry_wait_count
		FROM jobs AS job
		JOIN projects AS project ON project.id = job.project_id
		JOIN worker_pools AS pool ON pool.id = job.worker_pool_id
		WHERE job.id = $1
	`, jobID).Scan(
		&actualProjectQueued,
		&actualProjectRetryWait,
		&actualPoolQueued,
		&actualPoolRetryWait,
	); err != nil {
		t.Fatalf("read waiting compatibility counters: %v", err)
	}
	if actualProjectQueued != projectQueued || actualProjectRetryWait != projectRetryWait ||
		actualPoolQueued != poolQueued || actualPoolRetryWait != poolRetryWait {
		t.Fatalf(
			"waiting compatibility counters = project %d/%d pool %d/%d, want project %d/%d pool %d/%d",
			actualProjectQueued,
			actualProjectRetryWait,
			actualPoolQueued,
			actualPoolRetryWait,
			projectQueued,
			projectRetryWait,
			poolQueued,
			poolRetryWait,
		)
	}
}

type nMinusOneBinaries struct {
	Control            string
	AdmissionProbe     string
	AssignmentProbe    string
	OutboxProbe        string
	InvoiceChargeProbe string
	FailureProbe       string
}

type nMinusOneFailureProbeResult struct {
	Disposition string `json:"disposition"`
	AttemptID   string `json:"attempt_id"`
}

type nMinusOneInvoiceChargeResult struct {
	Decision string `json:"decision"`
	ChargeID string `json:"charge_id"`
}

type nMinusOneOutboxProbeResult struct {
	Subject      string `json:"subject"`
	MessageID    string `json:"message_id"`
	Payload      []byte `json:"payload"`
	KnownPayload bool   `json:"known_payload"`
	UnknownBytes int    `json:"unknown_bytes"`
}

func buildNMinusOneBinaries(t *testing.T, commit string) nMinusOneBinaries {
	t.Helper()
	sourceRoot := t.TempDir()
	archive := exec.Command("git", "archive", "--format=tar", commit)
	archive.Dir = repositoryRoot(t)
	stdout, err := archive.StdoutPipe()
	if err != nil {
		t.Fatalf("open N-1 source archive: %v", err)
	}
	if err := archive.Start(); err != nil {
		t.Fatalf("start N-1 source archive: %v", err)
	}
	reader := tar.NewReader(stdout)
	for {
		header, readErr := reader.Next()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			t.Fatalf("read N-1 source archive: %v", readErr)
		}
		name := filepath.Clean(header.Name)
		if filepath.IsAbs(name) || name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator)) {
			t.Fatalf("N-1 source archive contains unsafe path %q", header.Name)
		}
		target := filepath.Join(sourceRoot, name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				t.Fatalf("create N-1 source directory: %v", err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				t.Fatalf("create N-1 source parent: %v", err)
			}
			file, openErr := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_EXCL, header.FileInfo().Mode())
			if openErr != nil {
				t.Fatalf("create N-1 source file: %v", openErr)
			}
			_, copyErr := io.Copy(file, reader)
			closeErr := file.Close()
			if copyErr != nil || closeErr != nil {
				t.Fatalf("extract N-1 source file: copy=%v close=%v", copyErr, closeErr)
			}
		}
	}
	if err := archive.Wait(); err != nil {
		t.Fatalf("archive N-1 source: %v", err)
	}

	assignmentProbeSource, err := os.ReadFile(filepath.Join(
		repositoryRoot(t), "internal", "integration", "testdata", "nminusone_assignment_probe.go.txt",
	))
	if err != nil {
		t.Fatalf("read N-1 Assignment probe: %v", err)
	}
	probeDirectory := filepath.Join(sourceRoot, "cmd", "vela-nminusone-assignment-probe")
	if err := os.MkdirAll(probeDirectory, 0o755); err != nil {
		t.Fatalf("create N-1 Assignment probe directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(probeDirectory, "main.go"), assignmentProbeSource, 0o600); err != nil {
		t.Fatalf("write N-1 Assignment probe: %v", err)
	}
	admissionProbeSourceName := "nminusone_admission_probe.go.txt"
	if commit == profileCircuitNMinusOneCommit || commit == webhookNMinusOneCommit ||
		commit == humanOIDCNMinusOneCommit {
		admissionProbeSourceName = "nminusone_profile_circuit_admission_probe.go.txt"
	}
	admissionProbeSource, err := os.ReadFile(filepath.Join(
		repositoryRoot(t), "internal", "integration", "testdata", admissionProbeSourceName,
	))
	if err != nil {
		t.Fatalf("read N-1 Admission probe: %v", err)
	}
	admissionProbeDirectory := filepath.Join(sourceRoot, "cmd", "vela-nminusone-admission-probe")
	if err := os.MkdirAll(admissionProbeDirectory, 0o755); err != nil {
		t.Fatalf("create N-1 Admission probe directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(admissionProbeDirectory, "main.go"), admissionProbeSource, 0o600); err != nil {
		t.Fatalf("write N-1 Admission probe: %v", err)
	}
	outboxProbeSource, err := os.ReadFile(filepath.Join(
		repositoryRoot(t), "internal", "integration", "testdata", "nminusone_outbox_probe.go.txt",
	))
	if err != nil {
		t.Fatalf("read N-1 Outbox probe: %v", err)
	}
	outboxProbeDirectory := filepath.Join(sourceRoot, "cmd", "vela-nminusone-outbox-probe")
	if err := os.MkdirAll(outboxProbeDirectory, 0o755); err != nil {
		t.Fatalf("create N-1 Outbox probe directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outboxProbeDirectory, "main.go"), outboxProbeSource, 0o600); err != nil {
		t.Fatalf("write N-1 Outbox probe: %v", err)
	}
	var invoiceChargeProbeDirectory string
	if commit == invoiceExportNMinusOneCommit || commit == webhookNMinusOneCommit {
		invoiceChargeProbeSource, err := os.ReadFile(filepath.Join(
			repositoryRoot(t),
			"internal",
			"integration",
			"testdata",
			"nminusone_invoice_charge_probe.go.txt",
		))
		if err != nil {
			t.Fatalf("read N-1 Invoice Charge probe: %v", err)
		}
		invoiceChargeProbeDirectory = filepath.Join(sourceRoot, "cmd", "vela-nminusone-invoice-charge-probe")
		if err := os.MkdirAll(invoiceChargeProbeDirectory, 0o755); err != nil {
			t.Fatalf("create N-1 Invoice Charge probe directory: %v", err)
		}
		if err := os.WriteFile(
			filepath.Join(invoiceChargeProbeDirectory, "main.go"),
			invoiceChargeProbeSource,
			0o600,
		); err != nil {
			t.Fatalf("write N-1 Invoice Charge probe: %v", err)
		}
	}
	var failureProbeDirectory string
	if commit == profileCircuitNMinusOneCommit {
		failureProbeSource, err := os.ReadFile(filepath.Join(
			repositoryRoot(t),
			"internal",
			"integration",
			"testdata",
			"nminusone_failure_probe.go.txt",
		))
		if err != nil {
			t.Fatalf("read N-1 failure probe: %v", err)
		}
		failureProbeDirectory = filepath.Join(sourceRoot, "cmd", "vela-nminusone-failure-probe")
		if err := os.MkdirAll(failureProbeDirectory, 0o755); err != nil {
			t.Fatalf("create N-1 failure probe directory: %v", err)
		}
		if err := os.WriteFile(
			filepath.Join(failureProbeDirectory, "main.go"),
			failureProbeSource,
			0o600,
		); err != nil {
			t.Fatalf("write N-1 failure probe: %v", err)
		}
	}

	binaryDirectory := t.TempDir()
	binaries := nMinusOneBinaries{
		Control:         filepath.Join(binaryDirectory, "vela-control-n-minus-one"),
		AdmissionProbe:  filepath.Join(binaryDirectory, "vela-admission-probe-n-minus-one"),
		AssignmentProbe: filepath.Join(binaryDirectory, "vela-assignment-probe-n-minus-one"),
		OutboxProbe:     filepath.Join(binaryDirectory, "vela-outbox-probe-n-minus-one"),
	}
	if commit == invoiceExportNMinusOneCommit || commit == webhookNMinusOneCommit {
		binaries.InvoiceChargeProbe = filepath.Join(
			binaryDirectory,
			"vela-invoice-charge-probe-n-minus-one",
		)
	}
	if commit == profileCircuitNMinusOneCommit {
		binaries.FailureProbe = filepath.Join(binaryDirectory, "vela-failure-probe-n-minus-one")
	}
	build := exec.Command(
		"go", "build",
		"-o", binaries.Control, "./cmd/vela-control",
	)
	build.Dir = sourceRoot
	build.Env = environmentWith(map[string]string{"GOWORK": "off"})
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build N-1 vela-control: %v\n%s", err, output)
	}
	if commit != invoiceExportNMinusOneCommit {
		build = exec.Command(
			"go", "build",
			"-o", binaries.AdmissionProbe, "./cmd/vela-nminusone-admission-probe",
		)
		build.Dir = sourceRoot
		build.Env = environmentWith(map[string]string{"GOWORK": "off"})
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build N-1 Admission probe: %v\n%s", err, output)
		}
		build = exec.Command(
			"go", "build",
			"-o", binaries.AssignmentProbe, "./cmd/vela-nminusone-assignment-probe",
		)
		build.Dir = sourceRoot
		build.Env = environmentWith(map[string]string{"GOWORK": "off"})
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build N-1 Assignment probe: %v\n%s", err, output)
		}
		build = exec.Command(
			"go", "build",
			"-o", binaries.OutboxProbe, "./cmd/vela-nminusone-outbox-probe",
		)
		build.Dir = sourceRoot
		build.Env = environmentWith(map[string]string{"GOWORK": "off"})
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build N-1 Outbox probe: %v\n%s", err, output)
		}
	}
	if commit == invoiceExportNMinusOneCommit || commit == webhookNMinusOneCommit {
		build = exec.Command(
			"go",
			"build",
			"-o",
			binaries.InvoiceChargeProbe,
			"./cmd/vela-nminusone-invoice-charge-probe",
		)
		build.Dir = sourceRoot
		build.Env = environmentWith(map[string]string{"GOWORK": "off"})
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build N-1 Invoice Charge probe: %v\n%s", err, output)
		}
	}
	if commit == profileCircuitNMinusOneCommit {
		build = exec.Command(
			"go",
			"build",
			"-o",
			binaries.FailureProbe,
			"./cmd/vela-nminusone-failure-probe",
		)
		build.Dir = sourceRoot
		build.Env = environmentWith(map[string]string{"GOWORK": "off"})
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build N-1 failure probe: %v\n%s", err, output)
		}
	}
	return binaries
}

func runNMinusOneFailureProbeProcess(
	t *testing.T,
	binary string,
	adminDSN string,
	jobID string,
	workerID string,
) ([]byte, error) {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(
			t,
			adminDSN,
			"vela_internal_login",
			"vela-internal-password",
		),
		"VELA_JOB_ID":    jobID,
		"VELA_WORKER_ID": workerID,
	})
	return command.CombinedOutput()
}

func runNMinusOneInvoiceChargeProbe(
	t *testing.T,
	binary string,
	adminDSN string,
	jobID uuid.UUID,
) nMinusOneInvoiceChargeResult {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_AUTH_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_auth_login", "vela-auth-password",
		),
		"VELA_CANCEL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_cancel_login", "vela-cancel-password",
		),
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_internal_login", "vela-internal-password",
		),
		"VELA_CREDENTIAL_PEPPER_BASE64": base64.StdEncoding.EncodeToString(testCredentialPepper),
		"VELA_BEARER_CREDENTIAL":        testBearerCredential(),
		"VELA_PROJECT_ID":               testProjectID,
		"VELA_JOB_ID":                   jobID.String(),
	})
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run N-1 Invoice Charge writer probe: %v\n%s", err, output)
	}
	var result nMinusOneInvoiceChargeResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode N-1 Invoice Charge result: %v\n%s", err, output)
	}
	return result
}

func runNMinusOneAdmissionProbe(t *testing.T, binary, adminDSN string) string {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_AUTH_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_auth_login", "vela-auth-password",
		),
		"VELA_REQUEST_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_request_login", "vela-request-password",
		),
		"VELA_SCHEDULER_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_scheduler_login", "vela-scheduler-password",
		),
		"VELA_CREDENTIAL_PEPPER_BASE64": base64.StdEncoding.EncodeToString(testCredentialPepper),
		"VELA_BEARER_CREDENTIAL":        testBearerCredential(),
		"VELA_PROJECT_ID":               testProjectID,
	})
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run N-1 Admission writer probe: %v\n%s", err, output)
	}
	var result struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode N-1 Admission probe output: %v\n%s", err, output)
	}
	if _, err := uuid.Parse(result.JobID); err != nil {
		t.Fatalf("N-1 Admission probe returned invalid Job id %q", result.JobID)
	}
	return result.JobID
}

func runNMinusOneAssignmentProbe(
	t *testing.T,
	binary string,
	adminDSN string,
	jobID string,
	workerID string,
	workerEpoch int64,
	expectedJobVersion int64,
) string {
	t.Helper()
	output, err := runNMinusOneAssignmentProbeProcess(
		t, binary, adminDSN, jobID, workerID, workerEpoch, expectedJobVersion,
	)
	if err != nil {
		t.Fatalf("run N-1 Assignment writer/replay probe: %v\n%s", err, output)
	}
	var result struct {
		AttemptID string `json:"attempt_id"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode N-1 Assignment probe output: %v\n%s", err, output)
	}
	if _, err := uuid.Parse(result.AttemptID); err != nil {
		t.Fatalf("N-1 Assignment probe returned invalid Attempt id %q", result.AttemptID)
	}
	return result.AttemptID
}

func runNMinusOneAssignmentProbeProcess(
	t *testing.T,
	binary string,
	adminDSN string,
	jobID string,
	workerID string,
	workerEpoch int64,
	expectedJobVersion int64,
) ([]byte, error) {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_internal_login", "vela-internal-password",
		),
		"VELA_WORKER_ID":            workerID,
		"VELA_WORKER_EPOCH":         fmt.Sprintf("%d", workerEpoch),
		"VELA_EXPECTED_JOB_VERSION": fmt.Sprintf("%d", expectedJobVersion),
		"VELA_JOB_ID":               jobID,
		"VELA_PROFILE_REVISION_ID":  "00000000-0000-0000-0000-000000000014",
	})
	return command.CombinedOutput()
}

func runNMinusOneOutboxProbe(
	t *testing.T,
	binary, adminDSN string,
) nMinusOneOutboxProbeResult {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_internal_login", "vela-internal-password",
		),
	})
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run N-1 Outbox publisher probe: %v\n%s", err, output)
	}
	var result nMinusOneOutboxProbeResult
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode N-1 Outbox probe output: %v\n%s", err, output)
	}
	return result
}

func runNMinusOneControl(t *testing.T, binary, adminDSN string) string {
	t.Helper()
	temporary := t.TempDir()
	credentialsFile := filepath.Join(temporary, "invalid.creds")
	rootCAFile := filepath.Join(temporary, "invalid-ca.pem")
	if err := os.WriteFile(credentialsFile, []byte("not-a-nats-credential\n"), 0o600); err != nil {
		t.Fatalf("write NATS credential sentinel: %v", err)
	}
	if err := os.WriteFile(rootCAFile, []byte("not-a-ca\n"), 0o600); err != nil {
		t.Fatalf("write NATS root CA sentinel: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	command.Env = environmentWith(map[string]string{
		"VELA_HTTP_ADDRESS":          "127.0.0.1:0",
		"VELA_AUTH_DATABASE_URL":     roleDatabaseURL(t, adminDSN, "vela_auth_login", "vela-auth-password"),
		"VELA_REQUEST_DATABASE_URL":  roleDatabaseURL(t, adminDSN, "vela_request_login", "vela-request-password"),
		"VELA_CANCEL_DATABASE_URL":   roleDatabaseURL(t, adminDSN, "vela_cancel_login", "vela-cancel-password"),
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(t, adminDSN, "vela_internal_login", "vela-internal-password"),
		"VELA_CREDENTIAL_PEPPER_BASE64": base64.StdEncoding.EncodeToString(
			[]byte("0123456789abcdef0123456789abcdef"),
		),
		"VELA_NATS_URL":              "nats://127.0.0.1:1",
		"VELA_NATS_CREDENTIALS_FILE": credentialsFile,
		"VELA_NATS_ROOT_CA_FILE":     rootCAFile,
	})
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("N-1 vela-control startup timed out: %v\n%s", ctx.Err(), output)
	}
	if err == nil {
		t.Fatalf("N-1 vela-control unexpectedly passed the deliberate NATS sentinel")
	}
	return string(output)
}

func runSchedulerNMinusOneStartupProbe(t *testing.T, binary, adminDSN string) string {
	t.Helper()
	webhookKeyringFile := filepath.Join(t.TempDir(), "webhook-keyring.json")
	webhookKey := base64.StdEncoding.EncodeToString(
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err := os.WriteFile(
		webhookKeyringFile,
		[]byte(`{"webhook-key-v1":"`+webhookKey+`"}`),
		0o600,
	); err != nil {
		t.Fatalf("write N-1 Webhook keyring: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	command.Env = environmentWith(map[string]string{
		"VELA_HTTP_ADDRESS": "127.0.0.1:0",
		"VELA_AUTH_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_auth_login", "vela-auth-password",
		),
		"VELA_REQUEST_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_request_login", "vela-request-password",
		),
		"VELA_ARTIFACT_REQUEST_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_artifact_request_login", "vela-artifact-request-password",
		),
		"VELA_CANCEL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_cancel_login", "vela-cancel-password",
		),
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_internal_login", "vela-internal-password",
		),
		"VELA_SCHEDULER_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_scheduler_login", "vela-scheduler-password",
		),
		"VELA_SCHEDULER_ID": "n-minus-one-startup-probe",
		"VELA_BILLING_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_billing_login", "vela-billing-password",
		),
		"VELA_WEBHOOK_REQUEST_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_webhook_request_login", "vela-webhook-request-password",
		),
		"VELA_WEBHOOK_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_webhook_login", "vela-webhook-password",
		),
		"VELA_WEBHOOK_ENCRYPTION_ACTIVE_KEY_ID":   "webhook-key-v1",
		"VELA_WEBHOOK_ENCRYPTION_KEYRING_FILE":    webhookKeyringFile,
		"VELA_WEBHOOK_DISPATCHER_ID":              "n-minus-one-startup-probe",
		"VELA_INVOICE_EXPORTER_ID":                "n-minus-one-startup-probe",
		"VELA_INVOICE_EXPORT_ENDPOINT":            "https://127.0.0.1:1/invoices",
		"VELA_INVOICE_EXPORT_BEARER_TOKEN_FILE":   "/missing/invoice-export-token",
		"VELA_CREDENTIAL_PEPPER_BASE64":           base64.StdEncoding.EncodeToString(make([]byte, 32)),
		"VELA_NATS_URL":                           "nats://127.0.0.1:1",
		"VELA_NATS_CREDENTIALS_FILE":              "/missing/nats.creds",
		"VELA_NATS_ROOT_CA_FILE":                  "/missing/nats-ca.pem",
		"VELA_ARTIFACT_S3_ENDPOINT":               "http://127.0.0.1:1",
		"VELA_ARTIFACT_S3_REGION":                 "us-east-1",
		"VELA_ARTIFACT_S3_BUCKET":                 "vela-artifacts",
		"VELA_ARTIFACT_S3_ACCESS_KEY_ID_FILE":     "/missing/s3-access-key-id",
		"VELA_ARTIFACT_S3_SECRET_ACCESS_KEY_FILE": "/missing/s3-secret-access-key",
		"VELA_LEASE_ACTIVE_KEY_ID":                "lease-key-v1",
		"VELA_LEASE_KEYRING_FILE":                 "/missing/lease-keyring.json",
		"VELA_ARTIFACT_VALIDATOR_HELPER_PATH":     "/missing/vela-artifact-validator",
		"VELA_ARTIFACT_FFPROBE_PATH":              "/missing/ffprobe",
		"VELA_ARTIFACT_SANDBOX_ROOT":              "/missing/sandbox",
		"VELA_ARTIFACT_SPOOL_DIRECTORY":           "/missing/spool",
		"VELA_ARTIFACT_FFPROBE_VERSION":           "8.0.1",
		"VELA_ARTIFACT_VALIDATOR_REVISION":        "ffprobe-8.0.1-sandbox-v1",
		"VELA_ARTIFACT_RECONCILER_ID":             "spiffe://vela.internal/reconciler/artifact-finalization",
		"VELA_WORKER_GRPC_TLS_CERT_FILE":          "/missing/worker-control.crt",
		"VELA_WORKER_GRPC_TLS_KEY_FILE":           "/missing/worker-control.key",
		"VELA_WORKER_GRPC_CLIENT_CA_FILE":         "/missing/worker-client-ca.crt",
	})
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("exact Scheduler N-1 startup timed out: %v\n%s", ctx.Err(), output)
	}
	if err == nil {
		t.Fatalf("exact Scheduler N-1 startup unexpectedly passed the Artifact sentinel")
	}
	return string(output)
}

func assertNMinusOneDatabaseStartupPassed(t *testing.T, output string) {
	t.Helper()
	if strings.Contains(output, "open auth database pool") ||
		strings.Contains(output, "open request database pool") ||
		strings.Contains(output, "open internal database pool") {
		t.Fatalf("N-1 database startup failed before NATS sentinel:\n%s", output)
	}
	if !strings.Contains(output, "connect NATS") {
		t.Fatalf("N-1 startup did not reach the NATS sentinel:\n%s", output)
	}
}

func assertNMinusOneRequestStartupRejected(t *testing.T, output string) {
	t.Helper()
	if !strings.Contains(output, "open request database pool") ||
		!strings.Contains(output, "request transaction privilege boundary") {
		t.Fatalf("N-1 startup was not rejected at the request-role boundary:\n%s", output)
	}
}

func roleDatabaseURL(t *testing.T, adminDSN, username, password string) string {
	t.Helper()
	dsn, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	dsn.User = url.UserPassword(username, password)
	return dsn.String()
}

func environmentWith(overrides map[string]string) []string {
	environment := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		name, _, found := strings.Cut(entry, "=")
		if !found {
			continue
		}
		if _, overridden := overrides[name]; !overridden {
			environment = append(environment, entry)
		}
	}
	for name, value := range overrides {
		environment = append(environment, fmt.Sprintf("%s=%s", name, value))
	}
	return environment
}
