//go:build integration

package integration_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/billingexport"
	"github.com/vivym/vela/internal/stagefinalization"
)

type startFixture struct {
	database   testDatabase
	assignment struct {
		JobID     uuid.UUID
		AttemptID uuid.UUID
	}
}

func assertRejectedInvoiceChargeEffects(t *testing.T, fixture startFixture, label string) {
	t.Helper()
	var decisions, charges, exports, invoiceEvents int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = $1),
			(SELECT count(*) FROM charges WHERE job_id = $1),
			(SELECT count(*) FROM invoice_exports WHERE job_id = $1),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = $1 AND event_type = 'invoice.export_requested')
	`, fixture.assignment.JobID).Scan(&decisions, &charges, &exports, &invoiceEvents); err != nil {
		t.Fatalf("read rejected Charge effects for %s: %v", label, err)
	}
	if decisions != 0 || charges != 0 || exports != 0 || invoiceEvents != 0 {
		t.Fatalf(
			"%s left decisions/charges/exports/events = %d/%d/%d/%d",
			label,
			decisions,
			charges,
			exports,
			invoiceEvents,
		)
	}
}

func TestInvoiceExportMigrationDownUpReconstructsPendingAuthority(t *testing.T) {
	fixture := newInvoiceExportMigrationFixture(t, "invoice-export-down-up-pending")
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(fixture.database.Admin, migrations, 8); err != nil {
		t.Fatalf("migrate pending Invoice authority down: %v", err)
	}
	assertTableDoesNotExist(t, fixture.database.Admin, "invoice_exports")
	if err := goose.UpTo(fixture.database.Admin, migrations, 9); err != nil {
		t.Fatalf("migrate pending Invoice authority back up: %v", err)
	}
	var state string
	var attempts int
	if err := fixture.database.Admin.QueryRow(`
		SELECT state, attempts
		FROM invoice_exports
		WHERE charge_id = $1
	`, fixture.chargeID).Scan(&state, &attempts); err != nil {
		t.Fatalf("read reconstructed pending Invoice authority: %v", err)
	}
	if state != "PENDING" || attempts != 0 {
		t.Fatalf("reconstructed Invoice authority = state %s attempts %d", state, attempts)
	}
}

func TestInvoiceExportMigrationDownRefusesDurableEvidence(t *testing.T) {
	fixture := newInvoiceExportMigrationFixture(t, "invoice-export-down-refuses-receipt")
	adapter := &recordingInvoiceAdapter{receipt: billingexport.Receipt{
		InvoiceReference: "invoice-down-refusal",
		LineReference:    "line-down-refusal",
	}}
	service, err := billingexport.NewService(
		newRolePool(t, fixture.database.DSN, "vela_billing_login", "vela-billing-password"),
		adapter,
		billingexport.Config{
			ExporterID: "invoice-exporter-down-refusal",
			BatchSize:  1,
			ClaimTTL:   30 * time.Second,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("configure Invoice exporter for Down refusal: %v", err)
	}
	if result, err := service.ExportBatch(context.Background()); err != nil || result.Exported != 1 {
		t.Fatalf("create durable Invoice receipt = %#v error=%v", result, err)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err = goose.DownTo(fixture.database.Admin, migrations, 8)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
		postgresError.ConstraintName != "invoice_export_contract_has_durable_evidence" {
		t.Fatalf("Invoice migration Down with receipt error = %v", err)
	}
	var version int64
	if err := fixture.database.Admin.QueryRow("SELECT max(version_id) FROM goose_db_version WHERE is_applied").Scan(&version); err != nil {
		t.Fatalf("read migration version after refused Invoice Down: %v", err)
	}
	if version != 9 {
		t.Fatalf("migration version after refused Invoice Down = %d, want 9", version)
	}
}

func TestInvoiceExportReceiptCannotCommitWithoutExportedAuthority(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "invoice-export-partial-receipt")
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin partial Invoice receipt transaction: %v", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO invoice_export_receipts (
			id,
			organization_id,
			project_id,
			job_id,
			charge_id,
			external_invoice_reference,
			external_line_reference,
			exported_at
		)
		SELECT
			$1,
			export.organization_id,
			export.project_id,
			export.job_id,
			export.charge_id,
			'invoice-partial',
			'line-partial',
			clock_timestamp()
		FROM invoice_exports AS export
		WHERE export.charge_id = $2
	`, uuid.New(), fixture.chargeID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert partial Invoice receipt: %v", err)
	}
	err = tx.Commit()
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23514" ||
		postgresError.ConstraintName != "invoice_export_receipt_consistency" {
		t.Fatalf("partial Invoice receipt commit error = %v", err)
	}
	var receipts int
	if err := fixture.database.Admin.QueryRow(
		"SELECT count(*) FROM invoice_export_receipts WHERE charge_id = $1",
		fixture.chargeID,
	).Scan(&receipts); err != nil {
		t.Fatalf("read rejected partial Invoice receipt: %v", err)
	}
	if receipts != 0 {
		t.Fatalf("partial Invoice receipt count = %d, want 0", receipts)
	}
}

func TestInvoiceExportReceiptAndCompletedAuthorityAreImmutable(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "invoice-export-immutable-receipt")
	adapter := &recordingInvoiceAdapter{receipt: billingexport.Receipt{
		InvoiceReference: "invoice-immutable",
		LineReference:    "line-immutable",
	}}
	service, err := billingexport.NewService(
		newRolePool(t, fixture.database.DSN, "vela_billing_login", "vela-billing-password"),
		adapter,
		billingexport.Config{
			ExporterID: "invoice-exporter-immutable",
			BatchSize:  1,
			ClaimTTL:   30 * time.Second,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("configure immutable Invoice exporter: %v", err)
	}
	if result, err := service.ExportBatch(context.Background()); err != nil || result.Exported != 1 {
		t.Fatalf("create immutable Invoice receipt = %#v error=%v", result, err)
	}
	for _, mutation := range []string{
		"UPDATE invoice_export_receipts SET external_line_reference = 'forged' WHERE charge_id = $1",
		"DELETE FROM invoice_export_receipts WHERE charge_id = $1",
		"UPDATE invoice_exports SET last_error = 'forged' WHERE charge_id = $1",
		"DELETE FROM invoice_exports WHERE charge_id = $1",
	} {
		if _, err := fixture.database.Admin.Exec(mutation, fixture.chargeID); err == nil {
			t.Fatalf("completed Invoice evidence mutation succeeded: %s", mutation)
		}
	}
	var state, invoiceReference, lineReference string
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			export.state,
			receipt.external_invoice_reference,
			receipt.external_line_reference
		FROM invoice_exports AS export
		JOIN invoice_export_receipts AS receipt USING (charge_id)
		WHERE export.charge_id = $1
	`, fixture.chargeID).Scan(&state, &invoiceReference, &lineReference); err != nil {
		t.Fatalf("read immutable Invoice evidence: %v", err)
	}
	if state != "EXPORTED" || invoiceReference != "invoice-immutable" ||
		lineReference != "line-immutable" {
		t.Fatalf(
			"immutable Invoice evidence = %s/%s/%s",
			state,
			invoiceReference,
			lineReference,
		)
	}
}

func TestInvoiceExporterRetriesFailureWithTheSameChargeID(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "invoice-export-retry")
	adapter := &recordingInvoiceAdapter{
		receipt: billingexport.Receipt{
			InvoiceReference: "invoice-2026-08-organization-1",
			LineReference:    "line-retried-charge",
		},
		err: errors.New("external Invoice service unavailable"),
	}
	service, err := billingexport.NewService(
		newRolePool(t, fixture.database.DSN, "vela_billing_login", "vela-billing-password"),
		adapter,
		billingexport.Config{
			ExporterID: "invoice-exporter-retry",
			BatchSize:  10,
			ClaimTTL:   30 * time.Second,
			RetryDelay: 0,
		},
	)
	if err != nil {
		t.Fatalf("configure retrying Invoice exporter: %v", err)
	}

	first, firstErr := service.ExportBatch(context.Background())
	if firstErr == nil || first.Claimed != 1 || first.Exported != 0 {
		t.Fatalf("failed Invoice export = %#v error=%v", first, firstErr)
	}
	adapter.mu.Lock()
	adapter.err = nil
	adapter.mu.Unlock()
	second, secondErr := service.ExportBatch(context.Background())
	if secondErr != nil || second.Claimed != 1 || second.Exported != 1 {
		t.Fatalf("retried Invoice export = %#v error=%v", second, secondErr)
	}

	calls := adapter.Calls()
	if len(calls) != 2 || calls[0].ChargeID != fixture.chargeID ||
		calls[1].ChargeID != fixture.chargeID ||
		calls[0].IdempotencyKey != fixture.chargeID.String() ||
		calls[1].IdempotencyKey != fixture.chargeID.String() {
		t.Fatalf("retried Invoice adapter calls = %#v", calls)
	}
	var state string
	var attempts, receipts int
	var lastError any
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			export.state,
			export.attempts,
			export.last_error,
			(SELECT count(*) FROM invoice_export_receipts WHERE charge_id = export.charge_id)
		FROM invoice_exports AS export
		WHERE export.charge_id = $1
	`, fixture.chargeID).Scan(&state, &attempts, &lastError, &receipts); err != nil {
		t.Fatalf("read retried Invoice export: %v", err)
	}
	if state != "EXPORTED" || attempts != 2 || lastError != nil || receipts != 1 {
		t.Fatalf(
			"retried Invoice export = state %s attempts %d error=%v receipts=%d",
			state,
			attempts,
			lastError,
			receipts,
		)
	}
}

func TestInvoiceExporterRecoversRemoteSuccessBeforeLocalReceipt(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "invoice-export-remote-success-crash")
	installSingleInvoiceClaimAssertion(t, fixture.database)
	adapter := &expiringFirstInvoiceAdapter{
		receipt: billingexport.Receipt{
			InvoiceReference: "invoice-2026-08-organization-1",
			LineReference:    "line-idempotent-after-crash",
		},
	}
	service, err := billingexport.NewService(
		newRolePool(t, fixture.database.DSN, "vela_billing_login", "vela-billing-password"),
		adapter,
		billingexport.Config{
			ExporterID: "invoice-exporter-crash-recovery",
			BatchSize:  10,
			ClaimTTL:   time.Second,
			RetryDelay: 0,
		},
	)
	if err != nil {
		t.Fatalf("configure crash-recovering Invoice exporter: %v", err)
	}

	first, firstErr := service.ExportBatch(context.Background())
	if firstErr == nil || first.Claimed != 1 || first.Exported != 0 {
		t.Fatalf("expired post-remote-success claim = %#v error=%v", first, firstErr)
	}
	second, secondErr := service.ExportBatch(context.Background())
	if secondErr != nil || second.Claimed != 1 || second.Exported != 1 {
		t.Fatalf("recovered post-remote-success claim = %#v error=%v", second, secondErr)
	}

	calls := adapter.Calls()
	if len(calls) != 2 || calls[0].IdempotencyKey != fixture.chargeID.String() ||
		calls[1].IdempotencyKey != fixture.chargeID.String() {
		t.Fatalf("post-crash Invoice adapter calls = %#v", calls)
	}
	var state string
	var attempts, receipts int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			export.state,
			export.attempts,
			(SELECT count(*) FROM invoice_export_receipts WHERE charge_id = export.charge_id)
		FROM invoice_exports AS export
		WHERE export.charge_id = $1
	`, fixture.chargeID).Scan(&state, &attempts, &receipts); err != nil {
		t.Fatalf("read recovered post-remote-success Invoice export: %v", err)
	}
	if state != "EXPORTED" || attempts != 2 || receipts != 1 {
		t.Fatalf(
			"post-crash Invoice export = state %s attempts %d receipts=%d",
			state,
			attempts,
			receipts,
		)
	}
}

func installSingleInvoiceClaimAssertion(t *testing.T, database testDatabase) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		ALTER FUNCTION vela_claim_invoice_exports(text, uuid, integer, integer)
			RENAME TO vela_claim_invoice_exports_impl;
		CREATE FUNCTION vela_claim_invoice_exports(
			p_claimed_by text,
			p_claim_token uuid,
			p_claim_seconds integer,
			p_batch_size integer
		) RETURNS TABLE (
			charge_id uuid,
			organization_id uuid,
			project_id uuid,
			job_id uuid,
			reason text,
			amount_minor bigint,
			currency text,
			posted_at timestamptz
		)
		LANGUAGE plpgsql
		SECURITY DEFINER
		SET search_path = pg_catalog, public
		AS $$
		BEGIN
			IF p_batch_size <> 1 THEN
				RAISE EXCEPTION 'exporter claimed more than one Invoice line before remote delivery';
			END IF;
			RETURN QUERY
			SELECT *
			FROM public.vela_claim_invoice_exports_impl(
				p_claimed_by,
				p_claim_token,
				p_claim_seconds,
				p_batch_size
			);
		END
		$$;
		REVOKE ALL ON FUNCTION vela_claim_invoice_exports(text, uuid, integer, integer)
			FROM PUBLIC;
		GRANT EXECUTE ON FUNCTION vela_claim_invoice_exports(text, uuid, integer, integer)
			TO vela_billing;
	`); err != nil {
		t.Fatalf("install single Invoice claim assertion: %v", err)
	}
}

func TestConcurrentInvoiceExportersCallAdapterOnce(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "invoice-export-concurrent-claim")
	adapter := &blockingInvoiceAdapter{
		receipt: billingexport.Receipt{
			InvoiceReference: "invoice-concurrent",
			LineReference:    "line-concurrent",
		},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	newExporter := func(id string) *billingexport.Service {
		t.Helper()
		service, err := billingexport.NewService(
			newRolePool(t, fixture.database.DSN, "vela_billing_login", "vela-billing-password"),
			adapter,
			billingexport.Config{
				ExporterID: id,
				BatchSize:  1,
				ClaimTTL:   30 * time.Second,
				RetryDelay: time.Minute,
			},
		)
		if err != nil {
			t.Fatalf("configure concurrent Invoice exporter %s: %v", id, err)
		}
		return service
	}
	first := newExporter("invoice-exporter-concurrent-a")
	second := newExporter("invoice-exporter-concurrent-b")
	type callResult struct {
		result billingexport.BatchResult
		err    error
	}
	firstDone := make(chan callResult, 1)
	go func() {
		result, err := first.ExportBatch(context.Background())
		firstDone <- callResult{result: result, err: err}
	}()
	select {
	case <-adapter.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first Invoice exporter did not reach adapter")
	}
	secondResult, secondErr := second.ExportBatch(context.Background())
	if secondErr != nil || secondResult.Claimed != 0 || secondResult.Exported != 0 {
		t.Fatalf("competing Invoice exporter = %#v error=%v", secondResult, secondErr)
	}
	close(adapter.release)
	select {
	case outcome := <-firstDone:
		if outcome.err != nil || outcome.result.Claimed != 1 || outcome.result.Exported != 1 {
			t.Fatalf("winning Invoice exporter = %#v error=%v", outcome.result, outcome.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("winning Invoice exporter did not finish")
	}
	if calls := adapter.Calls(); len(calls) != 1 || calls[0].ChargeID != fixture.chargeID {
		t.Fatalf("concurrent Invoice adapter calls = %#v", calls)
	}
}

func TestBillingClaimFunctionRejectsUnboundedParameters(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "invoice-export-unbounded-claim")
	billingPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_billing_login",
		"vela-billing-password",
	)
	_, err := billingPool.Exec(
		context.Background(),
		"SELECT * FROM vela_claim_invoice_exports($1, $2, $3, $4)",
		"invoice-exporter-unbounded",
		uuid.New(),
		301,
		1001,
	)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "22023" {
		t.Fatalf("unbounded Invoice claim error = %v, want SQLSTATE 22023", err)
	}
	var state string
	var attempts int
	if err := fixture.database.Admin.QueryRow(`
		SELECT state, attempts
		FROM invoice_exports
		WHERE charge_id = $1
	`, fixture.chargeID).Scan(&state, &attempts); err != nil {
		t.Fatalf("read rejected unbounded Invoice claim: %v", err)
	}
	if state != "PENDING" || attempts != 0 {
		t.Fatalf("unbounded Invoice claim changed state = %s/%d", state, attempts)
	}
}

func TestInvoiceExporterPersistsReceiptAndUsesChargeIDAsIdempotencyKey(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "invoice-export-happy-path")
	adapter := &recordingInvoiceAdapter{
		receipt: billingexport.Receipt{
			InvoiceReference: "invoice-2026-08-organization-1",
			LineReference:    "line-charge-1",
		},
	}
	service, err := billingexport.NewService(
		newRolePool(t, fixture.database.DSN, "vela_billing_login", "vela-billing-password"),
		adapter,
		billingexport.Config{
			ExporterID: "invoice-exporter-a",
			BatchSize:  10,
			ClaimTTL:   30 * time.Second,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("configure Invoice exporter: %v", err)
	}

	result, err := service.ExportBatch(context.Background())
	if err != nil {
		t.Fatalf("export Invoice batch: %v", err)
	}
	if result.Claimed != 1 || result.Exported != 1 {
		t.Fatalf("Invoice export result = %#v, want one claimed and exported line", result)
	}

	calls := adapter.Calls()
	if len(calls) != 1 {
		t.Fatalf("Invoice adapter calls = %d, want 1", len(calls))
	}
	line := calls[0]
	if line.ChargeID != fixture.chargeID ||
		line.IdempotencyKey != fixture.chargeID.String() ||
		line.OrganizationID != uuid.MustParse(testOrganizationID) ||
		line.ProjectID != uuid.MustParse(testProjectID) ||
		line.JobID != fixture.assignment.JobID ||
		line.AmountMinor != 1250 || line.Currency != "CNY" ||
		line.Reason != "VISIBLE_COMPLETION" || line.PostedAt.IsZero() {
		t.Fatalf("exported Invoice line = %#v", line)
	}

	var (
		state                             string
		attempts                          int
		externalInvoice, externalLine     string
		receiptChargeID                   uuid.UUID
		exportedAt                        time.Time
		claimToken, claimOwner, lastError any
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			export.state,
			export.attempts,
			export.claim_token,
			export.claimed_by,
			export.last_error,
			receipt.charge_id,
			receipt.external_invoice_reference,
			receipt.external_line_reference,
			receipt.exported_at
		FROM invoice_exports AS export
		JOIN invoice_export_receipts AS receipt
		  ON receipt.charge_id = export.charge_id
		WHERE export.charge_id = $1
	`, fixture.chargeID).Scan(
		&state,
		&attempts,
		&claimToken,
		&claimOwner,
		&lastError,
		&receiptChargeID,
		&externalInvoice,
		&externalLine,
		&exportedAt,
	); err != nil {
		t.Fatalf("read durable Invoice export receipt: %v", err)
	}
	if state != "EXPORTED" || attempts != 1 || claimToken != nil || claimOwner != nil ||
		lastError != nil || receiptChargeID != fixture.chargeID ||
		externalInvoice != adapter.receipt.InvoiceReference ||
		externalLine != adapter.receipt.LineReference || exportedAt.IsZero() {
		t.Fatalf(
			"durable Invoice export = state %s attempts %d claim=%v/%v error=%v receipt %s/%s/%s at %s",
			state,
			attempts,
			claimOwner,
			claimToken,
			lastError,
			receiptChargeID,
			externalInvoice,
			externalLine,
			exportedAt,
		)
	}
}

type invoiceExportFixture struct {
	startFixture
	chargeID uuid.UUID
}

type invoiceExportMigrationFixture struct {
	database testDatabase
	jobID    uuid.UUID
	chargeID uuid.UUID
}

func newInvoiceExportMigrationFixture(t *testing.T, key string) invoiceExportMigrationFixture {
	t.Helper()
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 9)
	seedAdmissionFixture(t, database.Admin)

	jobID := uuid.New()
	reservationID := uuid.New()
	cancellationID := uuid.New()
	chargeID := uuid.New()
	eventID := uuid.New()
	postedAt := time.Now().UTC().Truncate(time.Microsecond)
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin historical Invoice export fixture: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if _, err := tx.Exec(`
		INSERT INTO jobs (
			id, organization_id, project_id, created_by_principal_id,
			model_revision_id, generation_preset_revision_id,
			service_class_revision_id, output_spec_id, worker_pool_id,
			request_hash, request_content, request_content_expires_at,
			pricing_rate_card_revision_id, pricing_rate_line_id,
			pricing_unit_amount_minor, pricing_quantity,
			pricing_quoted_amount_minor, pricing_currency,
			execution_max_attempts, execution_max_total_compute_seconds,
			execution_max_finalization_seconds_per_attempt,
			execution_retry_backoff_policy,
			execution_retryable_failure_classes,
			execution_circuit_breaker_policy, job_expires_at
		) VALUES (
			$1, $2, $3, $4,
			'00000000-0000-0000-0000-000000000010',
			'00000000-0000-0000-0000-000000000011',
			'00000000-0000-0000-0000-000000000012',
			'00000000-0000-0000-0000-000000000013',
			'00000000-0000-0000-0000-000000000005',
			sha256(convert_to($1::uuid::text, 'UTF8')),
			jsonb_build_object('model', 'minimax-h3', 'prompt', $5::text),
			$6::timestamptz + interval '2 days',
			'00000000-0000-0000-0000-000000000016',
			'00000000-0000-0000-0000-000000000017',
			1250, 1, 1250, 'CNY', 3, 2400, 600,
			'{"kind":"exponential","initial_seconds":30,"max_seconds":300}'::jsonb,
			ARRAY['WORKER_LOST', 'TRANSIENT_BACKEND'],
			'{"policy_revision":"h3-standard-v1"}'::jsonb,
			$6::timestamptz + interval '1 day'
		)
	`, jobID, testOrganizationID, testProjectID, testPrincipalID, key, postedAt); err != nil {
		t.Fatalf("insert historical Invoice export Job: %v", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO credit_reservations (
			id, organization_id, project_id, job_id, amount_minor, currency
		) VALUES ($1, $2, $3, $4, 1250, 'CNY')
	`, reservationID, testOrganizationID, testProjectID, jobID); err != nil {
		t.Fatalf("insert historical Invoice export CreditReservation: %v", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO retry_runtime_states (job_id, organization_id, project_id)
		VALUES ($1, $2, $3)
	`, jobID, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("insert historical Invoice export RetryRuntimeState: %v", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO job_cancellation_decisions (
			id, organization_id, project_id, job_id, requested_by_principal_id,
			previous_job_state, decision, billable, cancellation_fence,
			job_version, decided_at
		) VALUES (
			$1, $2, $3, $4, $5, 'QUEUED', 'CANCELED', false, 0, 1, $6
		)
	`, cancellationID, testOrganizationID, testProjectID, jobID, testPrincipalID, postedAt); err != nil {
		t.Fatalf("insert historical Invoice export cancellation decision: %v", err)
	}
	if _, err := tx.Exec(`
		SELECT vela_private.vela_insert_canonical_cancellation_event(
			$1::uuid, $2::uuid, $3::uuid, $4::uuid, 1,
			'invoice.export_requested', $5::timestamptz, 29,
			vela_private.vela_proto_string(1, $2::uuid::text)
				|| vela_private.vela_proto_string(2, $3::uuid::text)
				|| vela_private.vela_proto_string(3, $4::uuid::text)
				|| vela_private.vela_proto_string(4, $6::uuid::text)
				|| vela_private.vela_proto_bytes(
					5, vela_private.vela_proto_timestamp($5::timestamptz)
				)
		)
	`, eventID, testOrganizationID, testProjectID, jobID, postedAt, chargeID); err != nil {
		t.Fatalf("insert historical Invoice export intent: %v", err)
	}
	if _, err := tx.Exec(`
		INSERT INTO charges (
			id, organization_id, project_id, job_id, credit_reservation_id,
			cancellation_id, reason, amount_minor, currency, posted_at
		) VALUES ($1, $2, $3, $4, $5, $6, 'CUSTOMER_CANCELLATION', 1250, 'CNY', $7)
	`,
		chargeID,
		testOrganizationID,
		testProjectID,
		jobID,
		reservationID,
		cancellationID,
		postedAt,
	); err != nil {
		t.Fatalf("insert historical Invoice export Charge: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit historical Invoice export fixture: %v", err)
	}
	return invoiceExportMigrationFixture{
		database: database,
		jobID:    jobID,
		chargeID: chargeID,
	}
}

func newInvoiceExportChargeFixture(t *testing.T, key string) invoiceExportFixture {
	t.Helper()
	outcome := runCPUMediaH3GraphWithKey(t, key)
	service := visibleCompletionService(t, outcome.database.DSN)
	finalizer := stagefinalization.AuthenticatedFinalizer{
		ID: "spiffe://vela.internal/finalizer/invoice-" + outcome.jobID.String(),
	}
	claim, err := service.ClaimNextStageGraphFinalization(context.Background(), finalizer)
	if err != nil || claim.Decision != stagefinalization.StageGraphFinalizationGranted ||
		claim.JobID != outcome.jobID {
		t.Fatalf("claim Invoice Stage graph finalization = %#v error=%v", claim, err)
	}
	completed, err := service.CompleteStageGraphVisibleCompletion(
		context.Background(),
		finalizer,
		claim.Credentials,
		stagefinalization.StageGraphVisibleCompletionCandidate{
			CompletionID: uuid.New(), ExpectedJobVersion: claim.JobVersion,
		},
	)
	if err != nil || completed.Decision != stagefinalization.VisibleCompletionCommitted {
		t.Fatalf("complete Invoice Stage graph = %#v error=%v", completed, err)
	}
	var chargeID uuid.UUID
	if err := outcome.database.Admin.QueryRow(
		"SELECT id FROM charges WHERE job_id = $1",
		outcome.jobID,
	).Scan(&chargeID); err != nil {
		t.Fatalf("read Invoice Stage graph Charge: %v", err)
	}
	fixture := invoiceExportFixture{chargeID: chargeID}
	fixture.database = outcome.database
	fixture.assignment.JobID = outcome.jobID
	fixture.assignment.AttemptID = outcome.attemptID
	return fixture
}

type recordingInvoiceAdapter struct {
	mu      sync.Mutex
	receipt billingexport.Receipt
	err     error
	calls   []billingexport.Line
}

func (a *recordingInvoiceAdapter) ExportLine(
	_ context.Context,
	line billingexport.Line,
) (billingexport.Receipt, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, line)
	return a.receipt, a.err
}

func (a *recordingInvoiceAdapter) Calls() []billingexport.Line {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]billingexport.Line(nil), a.calls...)
}

type expiringFirstInvoiceAdapter struct {
	mu      sync.Mutex
	receipt billingexport.Receipt
	calls   []billingexport.Line
}

type blockingInvoiceAdapter struct {
	mu      sync.Mutex
	receipt billingexport.Receipt
	entered chan struct{}
	release chan struct{}
	calls   []billingexport.Line
}

func (a *blockingInvoiceAdapter) ExportLine(
	ctx context.Context,
	line billingexport.Line,
) (billingexport.Receipt, error) {
	a.mu.Lock()
	a.calls = append(a.calls, line)
	a.mu.Unlock()
	close(a.entered)
	select {
	case <-ctx.Done():
		return billingexport.Receipt{}, ctx.Err()
	case <-a.release:
		return a.receipt, nil
	}
}

func (a *blockingInvoiceAdapter) Calls() []billingexport.Line {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]billingexport.Line(nil), a.calls...)
}

func (a *expiringFirstInvoiceAdapter) ExportLine(
	ctx context.Context,
	line billingexport.Line,
) (billingexport.Receipt, error) {
	a.mu.Lock()
	a.calls = append(a.calls, line)
	first := len(a.calls) == 1
	a.mu.Unlock()
	if first {
		timer := time.NewTimer(1100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return billingexport.Receipt{}, ctx.Err()
		case <-timer.C:
		}
	}
	return a.receipt, nil
}

func (a *expiringFirstInvoiceAdapter) Calls() []billingexport.Line {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]billingexport.Line(nil), a.calls...)
}
