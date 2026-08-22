//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/artifactaccess"
	"github.com/vivym/vela/internal/billingexport"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/workercontrol"
)

func TestInvoiceFailureDoesNotBlockVisibleCompletionOrArtifactAccess(t *testing.T) {
	fixture := newStartFixture(t, "invoice-failure-visible-completion", 7)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start Visible Completion Invoice fixture = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("begin Visible Completion Invoice fixture = %#v error=%v", plan, err)
	}
	internalPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	completionService, err := workercontrol.NewService(
		context.Background(),
		internalPool,
		workercontrol.Config{
			LeaseTTL:         2 * time.Minute,
			ActiveLeaseKeyID: "lease-key-v1",
			LeaseKeys: map[string][]byte{
				"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
			},
			ArtifactInspector: artifactInspectorFunc(func(
				_ context.Context,
				request workercontrol.ArtifactInspectionRequest,
			) (workercontrol.ArtifactInspection, error) {
				return validInspectionForRequest(request), nil
			}),
		},
	)
	if err != nil {
		t.Fatalf("configure Visible Completion Invoice fixture: %v", err)
	}
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t,
		completionService,
		fixture.worker,
		fixture.credentials,
		plan,
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
	if err != nil || completed.Decision != workercontrol.VisibleCompletionCommitted ||
		completed.ChargeID == uuid.Nil || completed.ArtifactSetID == uuid.Nil {
		t.Fatalf("Visible Completion before Invoice outage = %#v error=%v", completed, err)
	}

	adapter := &recordingInvoiceAdapter{err: errors.New("external Invoice outage")}
	exporter, err := billingexport.NewService(
		newRolePool(t, fixture.database.DSN, "vela_billing_login", "vela-billing-password"),
		adapter,
		billingexport.Config{
			ExporterID: "invoice-exporter-visible-completion-outage",
			BatchSize:  1,
			ClaimTTL:   30 * time.Second,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("configure unavailable Invoice exporter: %v", err)
	}
	if result, err := exporter.ExportBatch(context.Background()); err == nil ||
		result.Claimed != 1 || result.Exported != 0 {
		t.Fatalf("unavailable Invoice export = %#v error=%v", result, err)
	}

	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'artifacts:read')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant Artifact read after Invoice outage: %v", err)
	}
	principal, err := identity.NewAuthenticator(
		newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate Artifact reader after Invoice outage: %v", err)
	}
	artifactSet, err := artifactaccess.NewService(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_artifact_request_login",
			"vela-artifact-request-password",
		),
		&recordingArtifactSigner{},
	).Get(
		context.Background(),
		principal,
		principal.ProjectID,
		fixture.assignment.JobID,
	)
	if err != nil || artifactSet.ID != completed.ArtifactSetID || len(artifactSet.Artifacts) == 0 {
		t.Fatalf("Artifact access after Invoice outage = %#v error=%v", artifactSet, err)
	}
	var jobState string
	if err := fixture.database.Admin.QueryRow(
		"SELECT state FROM jobs WHERE id = $1",
		fixture.assignment.JobID,
	).Scan(&jobState); err != nil {
		t.Fatalf("read Job after Invoice outage: %v", err)
	}
	if jobState != "SUCCEEDED" {
		t.Fatalf("Job state after Invoice outage = %s", jobState)
	}
}

func TestChargeCommitRequiresMatchingInvoiceExportIntent(t *testing.T) {
	fixture := newStartFixture(t, "charge-without-invoice-intent", 7)
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
		t.Fatalf("start missing Invoice intent fixture = %#v error=%v", started, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		CREATE FUNCTION vela_test_drop_invoice_export_intent() RETURNS trigger
		LANGUAGE plpgsql
		AS $$
		BEGIN
			IF NEW.event_type = 'invoice.export_requested' THEN
				RETURN NULL;
			END IF;
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER vela_test_drop_invoice_export_intent
		BEFORE INSERT ON outbox_events
		FOR EACH ROW EXECUTE FUNCTION vela_test_drop_invoice_export_intent();
	`); err != nil {
		t.Fatalf("install missing Invoice intent fault: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	canceled := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusInternalServerError {
		t.Fatalf(
			"cancel without Invoice intent status = %d, want 500; body=%s",
			canceled.StatusCode,
			canceled.Body,
		)
	}
	var decisions, charges, exports, invoiceEvents int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = $1),
			(SELECT count(*) FROM charges WHERE job_id = $1),
			(SELECT count(*) FROM invoice_exports WHERE job_id = $1),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = $1 AND event_type = 'invoice.export_requested')
	`, fixture.assignment.JobID).Scan(&decisions, &charges, &exports, &invoiceEvents); err != nil {
		t.Fatalf("read rejected Charge effects: %v", err)
	}
	if decisions != 0 || charges != 0 || exports != 0 || invoiceEvents != 0 {
		t.Fatalf(
			"rejected Charge left decisions/charges/exports/events = %d/%d/%d/%d",
			decisions,
			charges,
			exports,
			invoiceEvents,
		)
	}
}

func TestChargeCommitRejectsNoncanonicalInvoiceExportPayload(t *testing.T) {
	fixture := newStartFixture(t, "charge-noncanonical-invoice-intent", 7)
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
		t.Fatalf("start noncanonical Invoice intent fixture = %#v error=%v", started, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		CREATE FUNCTION vela_test_corrupt_invoice_export_intent() RETURNS trigger
		LANGUAGE plpgsql
		SECURITY DEFINER
		SET search_path = pg_catalog, public
		AS $$
		DECLARE
			v_charge_id uuid;
		BEGIN
			IF NEW.event_type = 'invoice.export_requested' THEN
				SELECT charge.id INTO STRICT v_charge_id
				FROM public.charges AS charge
				WHERE charge.organization_id = NEW.organization_id
				  AND charge.project_id = NEW.project_id
				  AND charge.job_id = NEW.aggregate_id;
				NEW.payload := vela_private.vela_cancellation_event_envelope(
					NEW.event_id,
					NEW.aggregate_id,
					NEW.aggregate_version,
					NEW.event_type,
					NEW.occurred_at,
					29,
					vela_private.vela_proto_string(1, NEW.organization_id::text)
						|| vela_private.vela_proto_string(2, NEW.project_id::text)
						|| vela_private.vela_proto_string(3, NEW.aggregate_id::text)
						|| vela_private.vela_proto_string(6, v_charge_id::text)
						|| vela_private.vela_proto_bytes(
							5,
							vela_private.vela_proto_timestamp(NEW.occurred_at)
						)
				);
			END IF;
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER vela_test_corrupt_invoice_export_intent
		BEFORE INSERT ON outbox_events
		FOR EACH ROW EXECUTE FUNCTION vela_test_corrupt_invoice_export_intent();
	`); err != nil {
		t.Fatalf("install noncanonical Invoice intent fault: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	canceled := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusInternalServerError {
		t.Fatalf(
			"cancel with noncanonical Invoice intent status = %d, want 500; body=%s",
			canceled.StatusCode,
			canceled.Body,
		)
	}
	assertRejectedInvoiceChargeEffects(t, fixture, "noncanonical Invoice intent")
}

func TestChargeCommitRejectsPreexistingInvoiceExportAuthority(t *testing.T) {
	fixture := newStartFixture(t, "charge-preexisting-invoice-authority", 7)
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
		t.Fatalf("start preexisting Invoice authority fixture = %#v error=%v", started, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		CREATE FUNCTION vela_test_preinsert_invoice_export_authority() RETURNS trigger
		LANGUAGE plpgsql
		SECURITY DEFINER
		SET search_path = pg_catalog, public
		AS $$
		BEGIN
			IF NEW.event_type = 'invoice.export_requested' THEN
				INSERT INTO public.invoice_exports (
					charge_id,
					organization_id,
					project_id,
					job_id,
					requested_event_id
				)
				SELECT
					charge.id,
					charge.organization_id,
					charge.project_id,
					charge.job_id,
					NEW.event_id
				FROM public.charges AS charge
				WHERE charge.organization_id = NEW.organization_id
				  AND charge.project_id = NEW.project_id
				  AND charge.job_id = NEW.aggregate_id;
			END IF;
			RETURN NULL;
		END
		$$;
		CREATE TRIGGER vela_test_preinsert_invoice_export_authority
		AFTER INSERT ON outbox_events
		FOR EACH ROW EXECUTE FUNCTION vela_test_preinsert_invoice_export_authority();
	`); err != nil {
		t.Fatalf("install preexisting Invoice authority fault: %v", err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	canceled := cancelJob(
		t,
		server.URL,
		testProjectID,
		fixture.assignment.JobID.String(),
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusInternalServerError {
		t.Fatalf(
			"cancel with preexisting Invoice authority status = %d, want 500; body=%s",
			canceled.StatusCode,
			canceled.Body,
		)
	}
	assertRejectedInvoiceChargeEffects(t, fixture, "preexisting Invoice authority")
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
	fixture := newInvoiceExportChargeFixture(t, "invoice-export-down-up-pending")
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(fixture.database.Admin, migrations, 8); err != nil {
		t.Fatalf("migrate pending Invoice authority down: %v", err)
	}
	assertTableDoesNotExist(t, fixture.database.Admin, "invoice_exports")
	if err := goose.Up(fixture.database.Admin, migrations); err != nil {
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
	fixture := newInvoiceExportChargeFixture(t, "invoice-export-down-refuses-receipt")
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
		line.Reason != "CUSTOMER_CANCELLATION" || line.PostedAt.IsZero() {
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

func newInvoiceExportChargeFixture(t *testing.T, key string) invoiceExportFixture {
	t.Helper()
	fixture := newStartFixture(t, key, 7)
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
		t.Fatalf("start billable Invoice fixture = %#v error=%v", started, err)
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
		t.Fatalf("cancel Invoice fixture status = %d; body=%s", canceled.StatusCode, canceled.Body)
	}
	var response cancelResponse
	if err := json.Unmarshal(canceled.Body, &response); err != nil {
		t.Fatalf("decode Invoice fixture cancellation: %v", err)
	}
	if response.Charge == nil {
		t.Fatal("Invoice fixture cancellation created no Charge")
	}
	chargeID, err := uuid.Parse(response.Charge.ChargeID)
	if err != nil {
		t.Fatalf("parse Invoice fixture Charge id: %v", err)
	}
	return invoiceExportFixture{startFixture: fixture, chargeID: chargeID}
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
