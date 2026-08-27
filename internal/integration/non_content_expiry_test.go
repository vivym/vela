//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/billingexport"
	"github.com/vivym/vela/internal/cancellation"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/financereconciliation"
	"github.com/vivym/vela/internal/legalhold"
	"github.com/vivym/vela/internal/noncontentexpiry"
	"github.com/vivym/vela/internal/workercontrol"
)

func migrateToSchema30BeforeTerminalEvidence(t *testing.T, database testDatabase) {
	t.Helper()
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 30); err != nil {
		t.Fatalf("contract fixture to schema 30 before terminal evidence: %v", err)
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 30 {
		t.Fatalf("fixture schema version = %d error=%v, want 30", version, err)
	}
}

func TestNonContentExpiryPhysicallyDeletesSourcesAndHonorsPostMetadataHold(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "non-content-expiry-physical")
	acknowledgeNonContentExpiryCancellation(t, fixture)
	exportNonContentExpiryInvoice(t, fixture)
	database := fixture.database
	jobID := fixture.assignment.JobID
	expiryPool := newRolePool(
		t,
		database.DSN,
		"vela_non_content_expiry_login",
		"vela-non-content-expiry-password",
	)
	reconciler, err := noncontentexpiry.New(expiryPool, noncontentexpiry.Config{
		InstanceID: "non-content-expiry-integration-1",
		BatchSize:  1,
		ClaimTTL:   30 * time.Second,
		HeldRetry:  time.Minute,
	})
	if err != nil {
		t.Fatalf("configure non-content expiry Reconciler: %v", err)
	}

	makeJobExpiryDue(t, database, jobID, "JOB_METADATA")
	result, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || result != (noncontentexpiry.Result{Claimed: 1, Expired: 1}) {
		t.Fatalf("metadata ReconcileBatch() = %#v, %v", result, err)
	}
	var (
		jobs, attempts, reservations, charges, invoiceExports, invoiceReceipts int64
		decisions, stopReceipts, roots, attemptRoots, metadataReceipts         int64
	)
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM jobs WHERE id = $1),
			(SELECT count(*) FROM attempts WHERE job_id = $1),
			(SELECT count(*) FROM credit_reservations WHERE job_id = $1),
			(SELECT count(*) FROM charges WHERE job_id = $1),
			(SELECT count(*) FROM invoice_exports WHERE job_id = $1),
			(SELECT count(*) FROM invoice_export_receipts WHERE job_id = $1),
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = $1),
			(SELECT count(*) FROM cancellation_stop_receipts WHERE job_id = $1),
			(SELECT count(*) FROM non_content_job_roots WHERE id = $1),
			(SELECT count(*) FROM non_content_attempt_roots WHERE job_id = $1),
			(SELECT count(*) FROM non_content_expiry_receipts
			 WHERE kind = 'JOB_METADATA' AND source_id = $1)
	`, jobID).Scan(
		&jobs, &attempts, &reservations, &charges, &invoiceExports, &invoiceReceipts,
		&decisions, &stopReceipts, &roots, &attemptRoots, &metadataReceipts,
	); err != nil {
		t.Fatalf("read metadata expiry result: %v", err)
	}
	if jobs != 0 || attempts != 0 || reservations != 1 || charges != 1 ||
		invoiceExports != 1 || invoiceReceipts != 1 || decisions != 1 || stopReceipts != 1 ||
		roots != 1 || attemptRoots != 1 || metadataReceipts != 1 {
		t.Fatalf(
			"metadata expiry counts jobs/attempts/reservations/charges/exports/invoice-receipts/"+
				"decisions/stop-receipts/job-roots/attempt-roots/expiry-receipts = "+
				"%d/%d/%d/%d/%d/%d/%d/%d/%d/%d/%d",
			jobs, attempts, reservations, charges, invoiceExports, invoiceReceipts,
			decisions, stopReceipts, roots, attemptRoots, metadataReceipts,
		)
	}

	seedCompliancePrincipal(t, database)
	compliancePool := newRolePool(
		t,
		database.DSN,
		"vela_compliance_login",
		"vela-compliance-password",
	)
	compliance, err := legalhold.NewService(context.Background(), compliancePool)
	if err != nil {
		t.Fatalf("configure Compliance service after metadata expiry: %v", err)
	}
	projectID := uuid.MustParse(testProjectID)
	effectiveAt := time.Now().UTC().Truncate(time.Microsecond)
	holdID := uuid.New()
	placed, err := compliance.Apply(context.Background(), legalhold.Request{
		IdempotencyKey:    "non-content-expiry-financial-hold-place",
		SourceSequence:    1,
		HoldID:            holdID,
		Kind:              legalhold.KindHoldPlaced,
		Scope:             legalhold.ScopeJob,
		OrganizationID:    uuid.MustParse(testOrganizationID),
		ProjectID:         &projectID,
		JobID:             &jobID,
		RecordClasses:     []legalhold.RecordClass{legalhold.RecordClassFinancial},
		ReasonCode:        "LITIGATION",
		ExternalReference: "non-content-expiry/place",
		EffectiveAt:       effectiveAt,
	})
	if err != nil || placed.State != legalhold.StateActive {
		t.Fatalf("place post-metadata Financial hold = %#v, %v", placed, err)
	}

	makeJobExpiryDue(t, database, jobID, "JOB_FINANCIAL")
	result, err = reconciler.ReconcileBatch(context.Background())
	if err != nil || result != (noncontentexpiry.Result{Claimed: 1, Held: 1}) {
		t.Fatalf("held financial ReconcileBatch() = %#v, %v", result, err)
	}
	if _, err := expiryPool.Exec(
		context.Background(),
		"DELETE FROM non_content_expiry_candidates WHERE source_id = $1",
		jobID,
	); err == nil {
		t.Fatal("non-content expiry runtime role deleted a candidate table row directly")
	} else {
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
			t.Fatalf("direct candidate deletion error = %v, want SQLSTATE 42501", err)
		}
	}

	released, err := compliance.Apply(context.Background(), legalhold.Request{
		IdempotencyKey:    "non-content-expiry-financial-hold-release",
		SourceSequence:    2,
		HoldID:            holdID,
		Kind:              legalhold.KindHoldReleased,
		ReasonCode:        "ORDER_LIFTED",
		ExternalReference: "non-content-expiry/release",
		EffectiveAt:       effectiveAt.Add(time.Minute),
	})
	if err != nil || released.State != legalhold.StateReleased {
		t.Fatalf("release post-metadata Financial hold = %#v, %v", released, err)
	}
	makeJobExpiryDue(t, database, jobID, "JOB_FINANCIAL")

	var beforeLimit, beforeReserved, beforeUnsettled, beforeVersion int64
	if err := database.Admin.QueryRow(`
		SELECT contract_credit_limit_minor, reserved_minor,
			unsettled_posted_minor, version
		FROM organization_credit_accounts WHERE organization_id = $1
	`, testOrganizationID).Scan(
		&beforeLimit, &beforeReserved, &beforeUnsettled, &beforeVersion,
	); err != nil {
		t.Fatalf("read credit aggregate before Financial expiry: %v", err)
	}
	result, err = reconciler.ReconcileBatch(context.Background())
	if err != nil || result != (noncontentexpiry.Result{Claimed: 1, Expired: 1}) {
		t.Fatalf("financial ReconcileBatch() = %#v, %v", result, err)
	}

	var afterLimit, afterReserved, afterUnsettled, afterVersion int64
	var (
		remainingReservations, remainingCharges, remainingExports, remainingInvoiceReceipts int64
		holds, receipts, expiredCandidates                                                  int64
		metadataDeletedJobs, metadataDeletedAttempts                                        int64
		financialDeletedReservations, financialDeletedCharges                               int64
		financialDeletedExports, financialDeletedInvoiceReceipts                            int64
	)
	var rootJSON, attemptRootJSON string
	if err := database.Admin.QueryRow(`
		SELECT
			account.contract_credit_limit_minor,
			account.reserved_minor,
			account.unsettled_posted_minor,
			account.version,
			(SELECT count(*) FROM credit_reservations WHERE job_id = $2),
			(SELECT count(*) FROM charges WHERE job_id = $2),
			(SELECT count(*) FROM invoice_exports WHERE job_id = $2),
			(SELECT count(*) FROM invoice_export_receipts WHERE job_id = $2),
			(SELECT count(*) FROM legal_holds WHERE job_id = $2),
			(SELECT count(*) FROM non_content_expiry_receipts WHERE source_id = $2),
			(SELECT count(*) FROM non_content_expiry_candidates
			 WHERE source_id = $2 AND state = 'EXPIRED'),
			(SELECT deleted_job_count FROM non_content_expiry_receipts
			 WHERE kind = 'JOB_METADATA' AND source_id = $2),
			(SELECT deleted_attempt_count FROM non_content_expiry_receipts
			 WHERE kind = 'JOB_METADATA' AND source_id = $2),
			(SELECT deleted_credit_reservation_count FROM non_content_expiry_receipts
			 WHERE kind = 'JOB_FINANCIAL' AND source_id = $2),
			(SELECT deleted_charge_count FROM non_content_expiry_receipts
			 WHERE kind = 'JOB_FINANCIAL' AND source_id = $2),
			(SELECT deleted_invoice_export_count FROM non_content_expiry_receipts
			 WHERE kind = 'JOB_FINANCIAL' AND source_id = $2),
			(SELECT deleted_invoice_receipt_count FROM non_content_expiry_receipts
			 WHERE kind = 'JOB_FINANCIAL' AND source_id = $2),
			(SELECT to_jsonb(root)::text FROM non_content_job_roots AS root WHERE id = $2),
			(SELECT to_jsonb(root)::text FROM non_content_attempt_roots AS root WHERE job_id = $2)
		FROM organization_credit_accounts AS account
		WHERE account.organization_id = $1
	`, testOrganizationID, jobID).Scan(
		&afterLimit, &afterReserved, &afterUnsettled, &afterVersion,
		&remainingReservations, &remainingCharges, &remainingExports, &remainingInvoiceReceipts,
		&holds, &receipts, &expiredCandidates,
		&metadataDeletedJobs, &metadataDeletedAttempts,
		&financialDeletedReservations, &financialDeletedCharges,
		&financialDeletedExports, &financialDeletedInvoiceReceipts,
		&rootJSON, &attemptRootJSON,
	); err != nil {
		t.Fatalf("read Financial expiry result: %v", err)
	}
	if [4]int64{afterLimit, afterReserved, afterUnsettled, afterVersion} !=
		([4]int64{beforeLimit, beforeReserved, beforeUnsettled, beforeVersion}) {
		t.Fatalf(
			"Financial expiry changed credit aggregate from %d/%d/%d/%d to %d/%d/%d/%d",
			beforeLimit, beforeReserved, beforeUnsettled, beforeVersion,
			afterLimit, afterReserved, afterUnsettled, afterVersion,
		)
	}
	if remainingReservations != 0 || remainingCharges != 0 || remainingExports != 0 ||
		remainingInvoiceReceipts != 0 || holds != 1 || receipts != 2 || expiredCandidates != 2 {
		t.Fatalf(
			"Financial expiry counts reservations/charges/exports/invoice-receipts/holds/"+
				"receipts/candidates = %d/%d/%d/%d/%d/%d/%d",
			remainingReservations, remainingCharges, remainingExports,
			remainingInvoiceReceipts, holds, receipts, expiredCandidates,
		)
	}
	if metadataDeletedJobs != 1 || metadataDeletedAttempts != 1 ||
		financialDeletedReservations != 1 || financialDeletedCharges != 1 ||
		financialDeletedExports != 1 || financialDeletedInvoiceReceipts != 1 {
		t.Fatalf(
			"expiry receipt deletion counts jobs/attempts/reservations/charges/exports/"+
				"invoice-receipts = %d/%d/%d/%d/%d/%d",
			metadataDeletedJobs, metadataDeletedAttempts, financialDeletedReservations,
			financialDeletedCharges, financialDeletedExports, financialDeletedInvoiceReceipts,
		)
	}
	for _, forbidden := range []string{"amount", "currency", "external_reference", "prompt"} {
		if strings.Contains(rootJSON, forbidden) {
			t.Fatalf("non-content root retained forbidden field %q: %s", forbidden, rootJSON)
		}
	}
	for _, forbidden := range []string{"worker_id", "worker_epoch", "fence"} {
		if strings.Contains(attemptRootJSON, forbidden) {
			t.Fatalf("non-content Attempt root retained forbidden field %q: %s", forbidden, attemptRootJSON)
		}
	}
}

func TestNonContentExpiryRootsAreMinimalImmutableAndFillOnce(t *testing.T) {
	fixture := newInvoiceExportChargeFixture(t, "non-content-expiry-root-contract")
	acknowledgeNonContentExpiryCancellation(t, fixture)
	exportNonContentExpiryInvoice(t, fixture)

	var jobRootJSON, attemptRootJSON string
	var rootChargeID, invoiceEventID uuid.UUID
	var terminalAt time.Time
	if err := fixture.database.Admin.QueryRow(`
		SELECT to_jsonb(job_root)::text, to_jsonb(attempt_root)::text,
			job_root.charge_id, job_root.invoice_requested_event_id, job_root.terminal_at
		FROM non_content_job_roots AS job_root
		JOIN non_content_attempt_roots AS attempt_root ON attempt_root.job_id = job_root.id
		WHERE job_root.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobRootJSON, &attemptRootJSON, &rootChargeID, &invoiceEventID, &terminalAt,
	); err != nil {
		t.Fatalf("read filled non-content roots: %v", err)
	}
	if rootChargeID != fixture.chargeID || invoiceEventID == uuid.Nil || terminalAt.IsZero() {
		t.Fatalf(
			"filled non-content Job root = charge %s invoice event %s terminal %s",
			rootChargeID, invoiceEventID, terminalAt,
		)
	}
	for _, forbidden := range []string{
		"request_content", "request_hash", "worker_id", "worker_epoch", "fence",
		"amount_minor", "currency", "external_reference",
	} {
		if strings.Contains(jobRootJSON, forbidden) || strings.Contains(attemptRootJSON, forbidden) {
			t.Fatalf(
				"non-content root retained forbidden field %q: job=%s attempt=%s",
				forbidden, jobRootJSON, attemptRootJSON,
			)
		}
	}

	for _, mutation := range []struct {
		name       string
		statement  string
		constraint string
	}{
		{
			name: "Job root identity update",
			statement: `UPDATE non_content_job_roots
				SET organization_id = gen_random_uuid() WHERE id = $1`,
			constraint: "non_content_job_root_is_immutable",
		},
		{
			name: "Job root terminal clock update",
			statement: `UPDATE non_content_job_roots
				SET terminal_at = terminal_at + interval '1 microsecond' WHERE id = $1`,
			constraint: "non_content_job_root_is_immutable",
		},
		{
			name:       "Job root delete",
			statement:  "DELETE FROM non_content_job_roots WHERE id = $1",
			constraint: "non_content_job_root_is_immutable",
		},
		{
			name: "Attempt root update",
			statement: `UPDATE non_content_attempt_roots
				SET created_at = created_at + interval '1 microsecond' WHERE job_id = $1`,
			constraint: "non_content_attempt_root_is_immutable",
		},
		{
			name:       "Attempt root delete",
			statement:  "DELETE FROM non_content_attempt_roots WHERE job_id = $1",
			constraint: "non_content_attempt_root_is_immutable",
		},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			_, err := fixture.database.Admin.Exec(mutation.statement, fixture.assignment.JobID)
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
				postgresError.ConstraintName != mutation.constraint {
				t.Fatalf("root mutation error = %v, want %s SQLSTATE 55000", err, mutation.constraint)
			}
		})
	}
}

func TestNonContentAttemptDependentsPreserveLiveCompositeIdentity(t *testing.T) {
	fixture := newStartFixture(t, "non-content-expiry-live-attempt-identity", 7)
	type triggerContract struct {
		trigger  string
		function string
	}
	expectedTriggers := map[string]triggerContract{
		"artifact_sets": {
			trigger:  "artifact_sets_validate_live_attempt_identity",
			function: "validate_live_artifact_attempt_identity",
		},
		"artifacts": {
			trigger:  "artifacts_validate_live_attempt_identity",
			function: "validate_live_artifact_attempt_identity",
		},
		"attempt_leases": {
			trigger:  "attempt_leases_validate_live_attempt_identity",
			function: "validate_live_lease_attempt_identity",
		},
		"attempt_progress": {
			trigger:  "attempt_progress_validate_live_attempt_identity",
			function: "validate_live_progress_attempt_identity",
		},
		"cancellation_stop_receipts": {
			trigger:  "cancellation_stop_receipts_validate_live_attempt_identity",
			function: "validate_live_evidence_attempt_identity",
		},
		"execution_failure_decisions": {
			trigger:  "execution_failure_decisions_validate_live_attempt_identity",
			function: "validate_live_evidence_attempt_identity",
		},
		"job_cancellation_decisions": {
			trigger:  "job_cancellation_decisions_validate_live_attempt_identity",
			function: "validate_live_evidence_attempt_identity",
		},
		"profile_certification_circuit_openings": {
			trigger:  "profile_circuit_openings_validate_live_attempt_identity",
			function: "validate_live_profile_circuit_attempt_identity",
		},
	}
	rows, err := fixture.database.Admin.Query(`
		SELECT relation.relname, trigger.tgname, procedure.proname
		FROM pg_catalog.pg_trigger AS trigger
		JOIN pg_catalog.pg_class AS relation ON relation.oid = trigger.tgrelid
		JOIN pg_catalog.pg_proc AS procedure ON procedure.oid = trigger.tgfoid
		JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = procedure.pronamespace
		WHERE namespace.nspname = 'vela_private'
		  AND procedure.proname IN (
			'validate_live_artifact_attempt_identity',
			'validate_live_lease_attempt_identity',
			'validate_live_progress_attempt_identity',
			'validate_live_evidence_attempt_identity',
			'validate_live_profile_circuit_attempt_identity'
		  )
		  AND NOT trigger.tgisinternal
	`)
	if err != nil {
		t.Fatalf("list live Attempt identity triggers: %v", err)
	}
	defer rows.Close()
	seen := map[string]triggerContract{}
	for rows.Next() {
		var table string
		var contract triggerContract
		if err := rows.Scan(&table, &contract.trigger, &contract.function); err != nil {
			t.Fatalf("scan live Attempt identity trigger: %v", err)
		}
		seen[table] = contract
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate live Attempt identity triggers: %v", err)
	}
	if len(seen) != len(expectedTriggers) {
		t.Fatalf("live Attempt identity trigger count = %d, want %d: %#v", len(seen), len(expectedTriggers), seen)
	}
	for table, contract := range expectedTriggers {
		if seen[table] != contract {
			t.Fatalf(
				"live Attempt identity trigger for %s = %#v, want %#v",
				table, seen[table], contract,
			)
		}
	}

	_, err = fixture.database.Admin.Exec(`
		INSERT INTO artifacts (
			id, organization_id, project_id, job_id, attempt_id, attempt_fence,
			kind, ordinal, object_key, expected_content_type, expires_at
		)
		SELECT $2::uuid, organization_id, project_id, job_id, id, fence + 1,
			'VIDEO', 1000, 'non-content-expiry/' || ($2::uuid)::text || '.mp4',
			'video/mp4', clock_timestamp() + interval '1 hour'
		FROM attempts
		WHERE id = $1
	`, fixture.assignment.AttemptID, uuid.New())
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23503" ||
		postgresError.ConstraintName != "artifact_live_attempt_identity" {
		t.Fatalf("mismatched live Attempt dependent error = %v, want named SQLSTATE 23503", err)
	}
}

func TestNonContentLegalHoldsDoNotBlockCustomerContentDeletion(t *testing.T) {
	database, jobID := newCanceledNonContentExpiryFixture(t, "non-content-expiry-content-deletion")
	seedCompliancePrincipal(t, database)
	compliance, err := legalhold.NewService(context.Background(), newRolePool(
		t,
		database.DSN,
		"vela_compliance_login",
		"vela-compliance-password",
	))
	if err != nil {
		t.Fatalf("configure Compliance service: %v", err)
	}
	projectID := uuid.MustParse(testProjectID)
	holdID := uuid.New()
	if _, err := compliance.Apply(context.Background(), legalhold.Request{
		IdempotencyKey: "non-content-expiry-content-deletion-hold",
		SourceSequence: 1,
		HoldID:         holdID,
		Kind:           legalhold.KindHoldPlaced,
		Scope:          legalhold.ScopeJob,
		OrganizationID: uuid.MustParse(testOrganizationID),
		ProjectID:      &projectID,
		JobID:          &jobID,
		RecordClasses: []legalhold.RecordClass{
			legalhold.RecordClassMetadata,
			legalhold.RecordClassFinancial,
		},
		ReasonCode:        "LITIGATION",
		ExternalReference: "non-content-expiry/content-deletion-hold",
		EffectiveAt:       time.Now().UTC().Truncate(time.Microsecond),
	}); err != nil {
		t.Fatalf("place non-content Legal Hold before Customer Content deletion: %v", err)
	}

	deletionService, principal := contentDeletionAuthority(t, database)
	deletion, err := deletionService.AcceptContentDeletion(
		context.Background(),
		principal,
		projectID,
		jobID,
		"non-content-expiry-content-deletion-request",
	)
	if err != nil {
		t.Fatalf("accept Customer Content deletion under non-content Legal Hold: %v", err)
	}
	if deletion.RequestID == uuid.Nil || deletion.JobID != jobID || deletion.ProjectID != projectID {
		t.Fatalf("Customer Content deletion under non-content Legal Hold = %#v", deletion)
	}

	var contentDeleted, holdActive, deletionPersisted bool
	if err := database.Admin.QueryRow(`
		SELECT
			job.request_content = '{"deleted":true}'::jsonb
				AND job.request_content_deleted_at IS NOT NULL,
			EXISTS (
				SELECT 1 FROM legal_holds AS hold
				WHERE hold.id = $2 AND hold.state = 'ACTIVE'
			),
			EXISTS (
				SELECT 1 FROM content_deletion_requests AS request
				WHERE request.id = $3
			)
		FROM jobs AS job
		WHERE job.id = $1
	`, jobID, holdID, deletion.RequestID).Scan(
		&contentDeleted,
		&holdActive,
		&deletionPersisted,
	); err != nil {
		t.Fatalf("read Customer Content deletion under non-content Legal Hold: %v", err)
	}
	if !contentDeleted || !holdActive || !deletionPersisted {
		t.Fatalf(
			"Customer Content deletion/active hold/persisted request = %t/%t/%t",
			contentDeleted,
			holdActive,
			deletionPersisted,
		)
	}
}

func TestOrganizationFinancialExpiryMatchesOnlyOrganizationHold(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	finance := newFinanceReconciliationService(t, database)
	applyReconciliation := func(sequence int64, key string) financereconciliation.Result {
		t.Helper()
		creditLimit := int64(100000) + sequence
		result, err := finance.Apply(context.Background(), financereconciliation.Request{
			IdempotencyKey:           "non-content-expiry-" + key,
			SourceSequence:           sequence,
			OrganizationID:           uuid.MustParse(testOrganizationID),
			Kind:                     financereconciliation.KindContractCreditLimitChanged,
			Currency:                 "CNY",
			ContractCreditLimitMinor: &creditLimit,
			ExternalReference:        "non-content-expiry/" + key,
			EffectiveAt:              time.Date(2026, 8, 27, 12, int(sequence), 0, 0, time.UTC),
		})
		if err != nil {
			t.Fatalf("apply %s Finance Reconciliation: %v", key, err)
		}
		return result
	}
	first := applyReconciliation(1, "organization-financial-first")
	second := applyReconciliation(2, "organization-financial-second")

	seedCompliancePrincipal(t, database)
	compliancePool := newRolePool(
		t, database.DSN, "vela_compliance_login", "vela-compliance-password",
	)
	compliance, err := legalhold.NewService(context.Background(), compliancePool)
	if err != nil {
		t.Fatalf("configure Compliance service: %v", err)
	}
	projectID := uuid.MustParse(testProjectID)
	effectiveAt := time.Now().UTC().Truncate(time.Microsecond)
	projectHoldID := uuid.New()
	if _, err := compliance.Apply(context.Background(), legalhold.Request{
		IdempotencyKey: "non-content-expiry-project-financial-place", SourceSequence: 1,
		HoldID: projectHoldID, Kind: legalhold.KindHoldPlaced,
		Scope: legalhold.ScopeProject, OrganizationID: uuid.MustParse(testOrganizationID),
		ProjectID: &projectID, RecordClasses: []legalhold.RecordClass{legalhold.RecordClassFinancial},
		ReasonCode: "TAX", ExternalReference: "non-content-expiry/project-financial",
		EffectiveAt: effectiveAt,
	}); err != nil {
		t.Fatalf("place Project Financial hold: %v", err)
	}
	organizationHoldID := uuid.New()
	if _, err := compliance.Apply(context.Background(), legalhold.Request{
		IdempotencyKey: "non-content-expiry-organization-financial-place", SourceSequence: 2,
		HoldID: organizationHoldID, Kind: legalhold.KindHoldPlaced,
		Scope: legalhold.ScopeOrganization, OrganizationID: uuid.MustParse(testOrganizationID),
		RecordClasses: []legalhold.RecordClass{legalhold.RecordClassFinancial},
		ReasonCode:    "REGULATORY", ExternalReference: "non-content-expiry/organization-financial",
		EffectiveAt: effectiveAt,
	}); err != nil {
		t.Fatalf("place Organization Financial hold: %v", err)
	}

	expiryPool := newRolePool(
		t, database.DSN, "vela_non_content_expiry_login", "vela-non-content-expiry-password",
	)
	reconciler, err := noncontentexpiry.New(expiryPool, noncontentexpiry.Config{
		InstanceID: "organization-financial-expiry-1", BatchSize: 1,
		ClaimTTL: 30 * time.Second, HeldRetry: time.Minute,
	})
	if err != nil {
		t.Fatalf("configure Organization Financial expiry Reconciler: %v", err)
	}
	makeOrganizationFinancialExpiryDue(t, database, first.RecordID)
	result, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || result != (noncontentexpiry.Result{Claimed: 1, Held: 1}) {
		t.Fatalf("held Organization Financial ReconcileBatch() = %#v, %v", result, err)
	}
	if _, err := compliance.Apply(context.Background(), legalhold.Request{
		IdempotencyKey: "non-content-expiry-organization-financial-release", SourceSequence: 3,
		HoldID: organizationHoldID, Kind: legalhold.KindHoldReleased,
		ReasonCode: "ORDER_LIFTED", ExternalReference: "non-content-expiry/organization-financial-release",
		EffectiveAt: effectiveAt.Add(time.Minute),
	}); err != nil {
		t.Fatalf("release Organization Financial hold: %v", err)
	}

	var beforeLimit, beforeReserved, beforeUnsettled, beforeVersion int64
	if err := database.Admin.QueryRow(`
		SELECT contract_credit_limit_minor, reserved_minor, unsettled_posted_minor, version
		FROM organization_credit_accounts WHERE organization_id = $1
	`, testOrganizationID).Scan(
		&beforeLimit, &beforeReserved, &beforeUnsettled, &beforeVersion,
	); err != nil {
		t.Fatalf("read credit aggregate before Organization Financial expiry: %v", err)
	}
	makeOrganizationFinancialExpiryDue(t, database, first.RecordID)
	result, err = reconciler.ReconcileBatch(context.Background())
	if err != nil || result != (noncontentexpiry.Result{Claimed: 1, Expired: 1}) {
		t.Fatalf("released Organization Financial ReconcileBatch() = %#v, %v", result, err)
	}
	makeOrganizationFinancialExpiryDue(t, database, second.RecordID)
	result, err = reconciler.ReconcileBatch(context.Background())
	if err != nil || result != (noncontentexpiry.Result{Claimed: 1, Expired: 1}) {
		t.Fatalf("Project-held Organization Financial ReconcileBatch() = %#v, %v", result, err)
	}

	var records, receipts int64
	var afterLimit, afterReserved, afterUnsettled, afterVersion int64
	var receiptJSON []string
	if err := database.Admin.QueryRow(`
		SELECT account.contract_credit_limit_minor, account.reserved_minor,
			account.unsettled_posted_minor, account.version,
			(SELECT count(*) FROM finance_reconciliation_records),
			(SELECT count(*) FROM non_content_expiry_receipts
			 WHERE kind = 'ORGANIZATION_FINANCIAL')
		FROM organization_credit_accounts AS account WHERE account.organization_id = $1
	`, testOrganizationID).Scan(
		&afterLimit, &afterReserved, &afterUnsettled, &afterVersion, &records, &receipts,
	); err != nil {
		t.Fatalf("read Organization Financial expiry result: %v", err)
	}
	rows, err := database.Admin.Query(`
		SELECT to_jsonb(receipt)::text FROM non_content_expiry_receipts AS receipt
		WHERE kind = 'ORGANIZATION_FINANCIAL' ORDER BY source_id
	`)
	if err != nil {
		t.Fatalf("read Organization Financial receipts: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var document string
		if err := rows.Scan(&document); err != nil {
			t.Fatalf("scan Organization Financial receipt: %v", err)
		}
		receiptJSON = append(receiptJSON, document)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate Organization Financial receipts: %v", err)
	}
	if [4]int64{afterLimit, afterReserved, afterUnsettled, afterVersion} !=
		([4]int64{beforeLimit, beforeReserved, beforeUnsettled, beforeVersion}) {
		t.Fatal("Organization Financial expiry changed the current credit aggregate")
	}
	if records != 0 || receipts != 2 || len(receiptJSON) != 2 {
		t.Fatalf("Organization Financial expiry records/receipts = %d/%d", records, receipts)
	}
	for _, document := range receiptJSON {
		for _, forbidden := range []string{"amount", "currency", "external_reference", "settlement"} {
			if strings.Contains(document, forbidden) {
				t.Fatalf("Organization Financial receipt retained forbidden field %q: %s", forbidden, document)
			}
		}
	}
}

func TestUncommittedFinanceReconciliationCannotBeClaimed(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	_ = newFinanceReconciliationService(t, database)
	financePool := newRolePool(
		t,
		database.DSN,
		"vela_finance_reconciliation_login",
		"vela-finance-reconciliation-password",
	)
	transaction, err := financePool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin uncommitted Finance Reconciliation: %v", err)
	}
	defer func() { _ = transaction.Rollback(context.Background()) }()
	recordID := uuid.New()
	creditLimit := int64(100001)
	var returnedRecordID uuid.UUID
	if err := transaction.QueryRow(context.Background(), `
		SELECT record_id
		FROM vela_apply_finance_reconciliation(
			$1, $2, $3, $4, 'CONTRACT_CREDIT_LIMIT_CHANGED', 'CNY',
			NULL, NULL, $5, $6, $7
		)
	`,
		recordID,
		"non-content-expiry-uncommitted-finance",
		int64(1),
		uuid.MustParse(testOrganizationID),
		creditLimit,
		"non-content-expiry/uncommitted-finance",
		time.Date(2026, 8, 27, 13, 0, 0, 0, time.UTC),
	).Scan(&returnedRecordID); err != nil {
		t.Fatalf("apply uncommitted Finance Reconciliation: %v", err)
	}
	if returnedRecordID != recordID {
		t.Fatalf("uncommitted Finance record id = %s, want %s", returnedRecordID, recordID)
	}
	var visibleRecords, visibleCandidates int64
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM finance_reconciliation_records WHERE id = $1),
			(SELECT count(*) FROM non_content_expiry_candidates
			 WHERE kind = 'ORGANIZATION_FINANCIAL' AND source_id = $1)
	`, recordID).Scan(&visibleRecords, &visibleCandidates); err != nil {
		t.Fatalf("inspect uncommitted Finance visibility: %v", err)
	}
	if visibleRecords != 0 || visibleCandidates != 0 {
		t.Fatalf("uncommitted Finance records/candidates visible = %d/%d", visibleRecords, visibleCandidates)
	}
	expiryPool := newRolePool(
		t, database.DSN, "vela_non_content_expiry_login", "vela-non-content-expiry-password",
	)
	if err := expiryPool.QueryRow(context.Background(), `
		SELECT kind::text FROM vela_claim_non_content_expiry($1, $2, $3)
	`, "uncommitted-finance-claim", uuid.New(), 30).Scan(new(string)); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("uncommitted Finance claim error = %v, want no rows", err)
	}
	if err := transaction.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback uncommitted Finance Reconciliation: %v", err)
	}
}

func TestNonContentExpiryMatchesAncestryAndRecordClass(t *testing.T) {
	database, jobID := newCanceledNonContentExpiryFixture(t, "non-content-expiry-hold-matrix")
	seedCompliancePrincipal(t, database)
	compliancePool := newRolePool(
		t, database.DSN, "vela_compliance_login", "vela-compliance-password",
	)
	compliance, err := legalhold.NewService(context.Background(), compliancePool)
	if err != nil {
		t.Fatalf("configure Compliance service: %v", err)
	}
	expiryPool := newRolePool(
		t, database.DSN, "vela_non_content_expiry_login", "vela-non-content-expiry-password",
	)
	reconciler, err := noncontentexpiry.New(expiryPool, noncontentexpiry.Config{
		InstanceID: "hold-matrix-expiry-1", BatchSize: 1,
		ClaimTTL: 30 * time.Second, HeldRetry: time.Minute,
	})
	if err != nil {
		t.Fatalf("configure hold matrix expiry Reconciler: %v", err)
	}
	organizationID := uuid.MustParse(testOrganizationID)
	projectID := uuid.MustParse(testProjectID)
	effectiveAt := time.Now().UTC().Truncate(time.Microsecond)
	organizationMetadataHold := uuid.New()
	if _, err := compliance.Apply(context.Background(), legalhold.Request{
		IdempotencyKey: "non-content-expiry-organization-metadata", SourceSequence: 1,
		HoldID: organizationMetadataHold, Kind: legalhold.KindHoldPlaced,
		Scope: legalhold.ScopeOrganization, OrganizationID: organizationID,
		RecordClasses: []legalhold.RecordClass{legalhold.RecordClassMetadata},
		ReasonCode:    "REGULATORY", ExternalReference: "non-content-expiry/organization-metadata",
		EffectiveAt: effectiveAt,
	}); err != nil {
		t.Fatalf("place Organization Metadata hold: %v", err)
	}
	makeJobExpiryDue(t, database, jobID, "JOB_METADATA")
	result, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || result != (noncontentexpiry.Result{Claimed: 1, Held: 1}) {
		t.Fatalf("Organization-held Metadata ReconcileBatch() = %#v, %v", result, err)
	}
	if _, err := compliance.Apply(context.Background(), legalhold.Request{
		IdempotencyKey: "non-content-expiry-organization-metadata-release", SourceSequence: 2,
		HoldID: organizationMetadataHold, Kind: legalhold.KindHoldReleased,
		ReasonCode: "ORDER_LIFTED", ExternalReference: "non-content-expiry/organization-metadata-release",
		EffectiveAt: effectiveAt.Add(time.Minute),
	}); err != nil {
		t.Fatalf("release Organization Metadata hold: %v", err)
	}
	projectFinancialHold := uuid.New()
	if _, err := compliance.Apply(context.Background(), legalhold.Request{
		IdempotencyKey: "non-content-expiry-project-financial", SourceSequence: 3,
		HoldID: projectFinancialHold, Kind: legalhold.KindHoldPlaced,
		Scope: legalhold.ScopeProject, OrganizationID: organizationID, ProjectID: &projectID,
		RecordClasses: []legalhold.RecordClass{legalhold.RecordClassFinancial},
		ReasonCode:    "TAX", ExternalReference: "non-content-expiry/project-financial-matrix",
		EffectiveAt: effectiveAt,
	}); err != nil {
		t.Fatalf("place Project Financial hold: %v", err)
	}
	makeJobExpiryDue(t, database, jobID, "JOB_METADATA")
	result, err = reconciler.ReconcileBatch(context.Background())
	if err != nil || result != (noncontentexpiry.Result{Claimed: 1, Expired: 1}) {
		t.Fatalf("class-mismatched Metadata ReconcileBatch() = %#v, %v", result, err)
	}
	makeJobExpiryDue(t, database, jobID, "JOB_FINANCIAL")
	result, err = reconciler.ReconcileBatch(context.Background())
	if err != nil || result != (noncontentexpiry.Result{Claimed: 1, Held: 1}) {
		t.Fatalf("Project-held Financial ReconcileBatch() = %#v, %v", result, err)
	}
	if _, err := compliance.Apply(context.Background(), legalhold.Request{
		IdempotencyKey: "non-content-expiry-project-financial-release", SourceSequence: 4,
		HoldID: projectFinancialHold, Kind: legalhold.KindHoldReleased,
		ReasonCode: "ORDER_LIFTED", ExternalReference: "non-content-expiry/project-financial-release",
		EffectiveAt: effectiveAt.Add(time.Minute),
	}); err != nil {
		t.Fatalf("release Project Financial hold: %v", err)
	}
	if _, err := compliance.Apply(context.Background(), legalhold.Request{
		IdempotencyKey: "non-content-expiry-job-metadata", SourceSequence: 5,
		HoldID: uuid.New(), Kind: legalhold.KindHoldPlaced,
		Scope: legalhold.ScopeJob, OrganizationID: organizationID, ProjectID: &projectID, JobID: &jobID,
		RecordClasses: []legalhold.RecordClass{legalhold.RecordClassMetadata},
		ReasonCode:    "LITIGATION", ExternalReference: "non-content-expiry/job-metadata",
		EffectiveAt: effectiveAt,
	}); err != nil {
		t.Fatalf("place post-Metadata Job hold: %v", err)
	}
	makeJobExpiryDue(t, database, jobID, "JOB_FINANCIAL")
	result, err = reconciler.ReconcileBatch(context.Background())
	if err != nil || result != (noncontentexpiry.Result{Claimed: 1, Expired: 1}) {
		t.Fatalf("class-mismatched Financial ReconcileBatch() = %#v, %v", result, err)
	}
}

func TestNonContentExpiryRecoversExpiredClaimAndCompletesOnce(t *testing.T) {
	database, jobID := newCanceledNonContentExpiryFixture(t, "non-content-expiry-claim-recovery")
	expiryPool := newRolePool(
		t, database.DSN, "vela_non_content_expiry_login", "vela-non-content-expiry-password",
	)
	makeJobExpiryDue(t, database, jobID, "JOB_METADATA")
	firstClaimID := uuid.New()
	var firstKind string
	var firstSourceID, returnedFirstClaimID uuid.UUID
	var firstAttempts int
	if err := expiryPool.QueryRow(context.Background(), `
		SELECT kind::text, source_id, claim_id, attempts
		FROM vela_claim_non_content_expiry($1, $2, $3)
	`, "claim-recovery-first", firstClaimID, 30).Scan(
		&firstKind, &firstSourceID, &returnedFirstClaimID, &firstAttempts,
	); err != nil {
		t.Fatalf("claim initial non-content expiry: %v", err)
	}
	if firstKind != "JOB_METADATA" || firstSourceID != jobID ||
		returnedFirstClaimID != firstClaimID || firstAttempts != 1 {
		t.Fatalf("initial claim = %s/%s/%s attempts %d", firstKind, firstSourceID, returnedFirstClaimID, firstAttempts)
	}
	expireNonContentClaim(t, database, "JOB_METADATA", jobID)
	secondClaimID := uuid.New()
	var secondKind string
	var secondSourceID, returnedSecondClaimID uuid.UUID
	var secondAttempts int
	if err := expiryPool.QueryRow(context.Background(), `
		SELECT kind::text, source_id, claim_id, attempts
		FROM vela_claim_non_content_expiry($1, $2, $3)
	`, "claim-recovery-second", secondClaimID, 30).Scan(
		&secondKind, &secondSourceID, &returnedSecondClaimID, &secondAttempts,
	); err != nil {
		t.Fatalf("claim replacement non-content expiry: %v", err)
	}
	if secondKind != "JOB_METADATA" || secondSourceID != jobID ||
		returnedSecondClaimID != secondClaimID || secondAttempts != 2 {
		t.Fatalf("replacement claim = %s/%s/%s attempts %d", secondKind, secondSourceID, returnedSecondClaimID, secondAttempts)
	}
	var staleOutcome string
	var staleReceiptID *uuid.UUID
	var staleDeleted int
	if err := expiryPool.QueryRow(context.Background(), `
		SELECT outcome, receipt_id, deleted_source_count
		FROM vela_complete_non_content_expiry($1, $2, $3, $4)
	`, "JOB_METADATA", jobID, firstClaimID, 60).Scan(
		&staleOutcome, &staleReceiptID, &staleDeleted,
	); err != nil {
		t.Fatalf("complete stale non-content expiry claim: %v", err)
	}
	if staleOutcome != "STALE" || staleReceiptID != nil || staleDeleted != 0 {
		t.Fatalf("stale completion = %s/%v/%d", staleOutcome, staleReceiptID, staleDeleted)
	}

	type completion struct {
		outcome   string
		receiptID *uuid.UUID
		deleted   int
		err       error
	}
	start := make(chan struct{})
	results := make(chan completion, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			var result completion
			result.err = expiryPool.QueryRow(context.Background(), `
				SELECT outcome, receipt_id, deleted_source_count
				FROM vela_complete_non_content_expiry($1, $2, $3, $4)
			`, "JOB_METADATA", jobID, secondClaimID, 60).Scan(
				&result.outcome, &result.receiptID, &result.deleted,
			)
			results <- result
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	expired, stale := 0, 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent non-content expiry completion: %v", result.err)
		}
		switch result.outcome {
		case "EXPIRED":
			expired++
			if result.receiptID == nil || *result.receiptID == uuid.Nil || result.deleted < 1 {
				t.Fatalf("expired completion = %#v", result)
			}
		case "STALE":
			stale++
			if result.receiptID != nil || result.deleted != 0 {
				t.Fatalf("stale concurrent completion = %#v", result)
			}
		default:
			t.Fatalf("unexpected concurrent completion = %#v", result)
		}
	}
	var jobs, receipts int64
	if err := database.Admin.QueryRow(`
		SELECT (SELECT count(*) FROM jobs WHERE id = $1),
			(SELECT count(*) FROM non_content_expiry_receipts
			 WHERE kind = 'JOB_METADATA' AND source_id = $1)
	`, jobID).Scan(&jobs, &receipts); err != nil {
		t.Fatalf("read claim recovery result: %v", err)
	}
	if expired != 1 || stale != 1 || jobs != 0 || receipts != 1 {
		t.Fatalf("claim recovery expired/stale/jobs/receipts = %d/%d/%d/%d", expired, stale, jobs, receipts)
	}
}

func TestNonContentExpiryMigrationEmptyRoundTripAndDurableEvidenceRefusal(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up restores runtime boundary", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		if err := goose.DownTo(database.Admin, migrations, 30); err != nil {
			t.Fatalf("migrate empty non-content expiry schema down: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 30 {
			t.Fatalf("non-content expiry version after Down = %d error=%v", version, err)
		}
		for _, table := range []string{
			"non_content_job_roots",
			"non_content_attempt_roots",
			"non_content_expiry_candidates",
			"non_content_expiry_receipts",
		} {
			assertTableDoesNotExist(t, database.Admin, table)
		}
		if err := goose.UpTo(database.Admin, migrations, 31); err != nil {
			t.Fatalf("migrate non-content expiry schema up: %v", err)
		}
		pool := newRolePool(
			t, database.DSN, "vela_non_content_expiry_login", "vela-non-content-expiry-password",
		)
		if err := veladb.VerifyRole(context.Background(), pool, veladb.RoleNonContentExpiry); err != nil {
			t.Fatalf("verify non-content expiry role after Down Up: %v", err)
		}
		if _, err := noncontentexpiry.New(pool, noncontentexpiry.Config{
			InstanceID: "non-content-expiry-round-trip", BatchSize: 1,
			ClaimTTL: time.Second, HeldRetry: time.Second,
		}); err != nil {
			t.Fatalf("configure non-content expiry after Down Up: %v", err)
		}

		seedAdmissionFixture(t, database.Admin)
		workerID := uuid.New()
		seedNMinusOneProfileCircuitWorker(t, database.Admin, workerID, "non-content-round-trip")
		if _, err := database.Admin.Exec(`
				UPDATE credentials
				SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
				WHERE id = $1
			`, testCredentialID); err != nil {
			t.Fatalf("grant cancellation scope after non-content expiry Down Up: %v", err)
		}
		server := admissionServerForDatabase(t, database)
		accepted := submitJob(t, server.URL, "non-content-expiry-round-trip-writes", []byte(`{
				"model":"minimax-h3",
				"generation_preset":"balanced",
				"service_class":"standard",
				"output_spec":"video-1080p-5s-24fps",
				"generation_count":1,
				"prompt":"prove all source writes after empty non-content expiry round trip"
			}`))
		if accepted.StatusCode != http.StatusAccepted {
			t.Fatalf(
				"Admission after non-content expiry Down Up = %d body=%s",
				accepted.StatusCode, accepted.Body,
			)
		}
		var job jobResponse
		if err := json.Unmarshal(accepted.Body, &job); err != nil {
			t.Fatalf("decode Admission after non-content expiry Down Up: %v", err)
		}
		jobID := uuid.MustParse(job.JobID)
		internalPool := newRolePool(
			t, database.DSN, "vela_internal_login", "vela-internal-password",
		)
		workerService, err := workercontrol.NewService(
			context.Background(),
			internalPool,
			workercontrol.Config{
				LeaseTTL:         2 * time.Minute,
				ActiveLeaseKeyID: "lease-key-v1",
				LeaseKeys: map[string][]byte{
					"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
				},
			},
		)
		if err != nil {
			t.Fatalf("configure Worker service after non-content expiry Down Up: %v", err)
		}
		worker := workercontrol.AuthenticatedWorker{ID: workerID}
		assignment, err := workerService.Acquire(
			context.Background(),
			worker,
			7,
			&workercontrol.AssignmentCandidate{
				JobID:                      jobID,
				ExpectedJobVersion:         1,
				ExecutionProfileRevisionID: uuid.MustParse("00000000-0000-0000-0000-000000000014"),
			},
		)
		if err != nil {
			t.Fatalf("assign Job after non-content expiry Down Up: %v", err)
		}
		if started, err := workerService.Start(
			context.Background(), worker, leaseCredentials(assignment),
		); err != nil || started.Decision != workercontrol.StartGranted {
			t.Fatalf("start Job after non-content expiry Down Up = %#v error=%v", started, err)
		}
		canceled := cancelJob(
			t, server.URL, testProjectID, jobID.String(), testBearerCredential(),
		)
		if canceled.StatusCode != http.StatusOK {
			t.Fatalf(
				"cancel Job after non-content expiry Down Up = %d body=%s",
				canceled.StatusCode, canceled.Body,
			)
		}
		var cancellationResult cancelResponse
		if err := json.Unmarshal(canceled.Body, &cancellationResult); err != nil {
			t.Fatalf("decode cancellation after non-content expiry Down Up: %v", err)
		}
		coordinator := cancellation.NewService(
			newRolePool(t, database.DSN, "vela_cancel_login", "vela-cancel-password"),
			internalPool,
		)
		stopped, err := coordinator.AcknowledgeCancellationStop(
			context.Background(),
			worker,
			leaseCredentials(assignment),
			uuid.MustParse(cancellationResult.CancellationID),
		)
		if err != nil || stopped.Decision != cancellation.StopAcknowledged ||
			stopped.State != "CANCELED" {
			t.Fatalf(
				"terminalize Job after non-content expiry Down Up = %#v error=%v",
				stopped, err,
			)
		}

		finance := newFinanceReconciliationService(t, database)
		creditLimit := int64(100001)
		reconciliation, err := finance.Apply(
			context.Background(),
			financereconciliation.Request{
				IdempotencyKey: "non-content-expiry-round-trip-finance", SourceSequence: 1,
				OrganizationID: uuid.MustParse(testOrganizationID),
				Kind:           financereconciliation.KindContractCreditLimitChanged, Currency: "CNY",
				ContractCreditLimitMinor: &creditLimit,
				ExternalReference:        "non-content-expiry/round-trip/finance",
				EffectiveAt:              time.Date(2026, 8, 27, 12, 30, 0, 0, time.UTC),
			},
		)
		if err != nil {
			t.Fatalf("apply Finance Reconciliation after non-content expiry Down Up: %v", err)
		}

		var jobRoots, attemptRoots, terminalCandidates, invoiceExports, financeCandidates int64
		var chargeID, invoiceEventID uuid.UUID
		var terminalAt time.Time
		if err := database.Admin.QueryRow(`
				SELECT
					(SELECT count(*) FROM non_content_job_roots WHERE id = $1),
					(SELECT count(*) FROM non_content_attempt_roots
					 WHERE id = $2 AND job_id = $1),
					(SELECT count(*) FROM non_content_expiry_candidates
					 WHERE source_id = $1 AND kind IN ('JOB_METADATA', 'JOB_FINANCIAL')),
					(SELECT count(*) FROM invoice_exports WHERE job_id = $1),
					(SELECT count(*) FROM non_content_expiry_candidates
					 WHERE kind = 'ORGANIZATION_FINANCIAL' AND source_id = $3),
					root.charge_id, root.invoice_requested_event_id, root.terminal_at
				FROM non_content_job_roots AS root WHERE root.id = $1
			`, jobID, assignment.AttemptID, reconciliation.RecordID).Scan(
			&jobRoots, &attemptRoots, &terminalCandidates, &invoiceExports,
			&financeCandidates, &chargeID, &invoiceEventID, &terminalAt,
		); err != nil {
			t.Fatalf("read source-write roots after non-content expiry Down Up: %v", err)
		}
		if jobRoots != 1 || attemptRoots != 1 || terminalCandidates != 2 ||
			invoiceExports != 1 || financeCandidates != 1 || chargeID == uuid.Nil ||
			invoiceEventID == uuid.Nil || terminalAt.IsZero() {
			t.Fatalf(
				"source writes after Down Up = roots %d/%d candidates %d/%d exports %d "+
					"linkage %s/%s terminal %s",
				jobRoots, attemptRoots, terminalCandidates, financeCandidates, invoiceExports,
				chargeID, invoiceEventID, terminalAt,
			)
		}
	})

	t.Run("terminal candidate refuses Down", func(t *testing.T) {
		database, _ := newCanceledNonContentExpiryFixture(t, "non-content-expiry-down-refusal")
		err := goose.DownTo(database.Admin, migrations, 30)
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
			postgresError.ConstraintName != "non_content_expiry_rollback_is_unsafe" {
			t.Fatalf("non-content expiry Down refusal = %v, want named SQLSTATE 55000", err)
		}
		version, versionErr := goose.GetDBVersion(database.Admin)
		if versionErr != nil || version != 31 {
			t.Fatalf("non-content expiry version after refused Down = %d error=%v", version, versionErr)
		}
	})
}

func TestNonContentExpiryMigrationRejectsAmbiguousTerminalBackfill(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, testDatabase, uuid.UUID)
	}{
		{
			name: "missing terminal event",
			mutate: func(t *testing.T, database testDatabase, jobID uuid.UUID) {
				t.Helper()
				if _, err := database.Admin.Exec(`
					DELETE FROM outbox_events
					WHERE aggregate_id = $1 AND event_type = 'job.canceled'
				`, jobID); err != nil {
					t.Fatalf("remove canonical terminal event: %v", err)
				}
			},
		},
		{
			name: "duplicate terminal event",
			mutate: func(t *testing.T, database testDatabase, jobID uuid.UUID) {
				t.Helper()
				if _, err := database.Admin.Exec(`
					ALTER TABLE outbox_events DROP CONSTRAINT
						outbox_events_aggregate_type_aggregate_id_aggregate_version_key
				`); err != nil {
					t.Fatalf("remove canonical terminal event uniqueness: %v", err)
				}
				if _, err := database.Admin.Exec(`
					INSERT INTO outbox_events (
						event_id, organization_id, project_id, aggregate_type, aggregate_id,
						aggregate_version, event_type, schema_version, payload, occurred_at
					)
					SELECT gen_random_uuid(), organization_id, project_id, aggregate_type,
						aggregate_id, aggregate_version, event_type, schema_version, payload,
						occurred_at + interval '1 microsecond'
					FROM outbox_events
					WHERE aggregate_id = $1 AND event_type = 'job.canceled'
				`, jobID); err != nil {
					t.Fatalf("duplicate canonical terminal event: %v", err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := newPostgres(t)
			applyFoundation(t, database.Admin)
			if err := goose.DownTo(database.Admin, migrations, 30); err != nil {
				t.Fatalf("contract non-content expiry schema: %v", err)
			}
			seedAdmissionFixture(t, database.Admin)
			server := admissionServerForDatabase(t, database)
			if _, err := database.Admin.Exec(`
				UPDATE credentials
				SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
				WHERE id = $1
			`, testCredentialID); err != nil {
				t.Fatalf("grant cancellation scope: %v", err)
			}
			accepted := submitJob(
				t,
				server.URL,
				"non-content-expiry-backfill-"+strings.ReplaceAll(test.name, " ", "-"),
				[]byte(`{
				"model":"minimax-h3",
				"generation_preset":"balanced",
				"service_class":"standard",
				"output_spec":"video-1080p-5s-24fps",
				"generation_count":1,
				"prompt":"ambiguous terminal event backfill must fail closed"
			}`),
			)
			if accepted.StatusCode != http.StatusAccepted {
				t.Fatalf("submit backfill fixture = %d body=%s", accepted.StatusCode, accepted.Body)
			}
			var job jobResponse
			if err := json.Unmarshal(accepted.Body, &job); err != nil {
				t.Fatalf("decode backfill fixture Job: %v", err)
			}
			jobID := uuid.MustParse(job.JobID)
			canceled := cancelJob(t, server.URL, testProjectID, job.JobID, testBearerCredential())
			if canceled.StatusCode != http.StatusOK {
				t.Fatalf("cancel backfill fixture = %d body=%s", canceled.StatusCode, canceled.Body)
			}
			test.mutate(t, database, jobID)
			err := goose.UpTo(database.Admin, migrations, 31)
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
				postgresError.ConstraintName != "terminal_job_requires_canonical_event" {
				t.Fatalf("ambiguous terminal backfill = %v, want named SQLSTATE 55000", err)
			}
			version, versionErr := goose.GetDBVersion(database.Admin)
			if versionErr != nil || version != 30 {
				t.Fatalf("version after refused terminal backfill = %d error=%v", version, versionErr)
			}
			assertTableDoesNotExist(t, database.Admin, "non_content_expiry_candidates")
		})
	}
}

func TestNonContentExpiryTerminalStatesCreateExactIndependentClocks(t *testing.T) {
	t.Run("canceled", func(t *testing.T) {
		database, jobID := newCanceledNonContentExpiryFixture(t, "non-content-expiry-clock-canceled")
		assertNonContentTerminalClocks(t, database, jobID, "job.canceled")
	})

	t.Run("failed", func(t *testing.T) {
		fixture := newAssignmentFixture(t, "non-content-expiry-clock-failed", 7)
		assignment, err := fixture.service.Acquire(
			context.Background(), fixture.worker, 7, &fixture.candidate,
		)
		if err != nil {
			t.Fatalf("create Failure clock Assignment: %v", err)
		}
		credentials := leaseCredentials(assignment)
		if started, err := fixture.service.Start(
			context.Background(), fixture.worker, credentials,
		); err != nil || started.Decision != workercontrol.StartGranted {
			t.Fatalf("start Failure clock Job = %#v error=%v", started, err)
		}
		expiryPool := newRolePool(
			t, fixture.database.DSN,
			"vela_non_content_expiry_login", "vela-non-content-expiry-password",
		)
		if err := expiryPool.QueryRow(context.Background(), `
			SELECT kind::text FROM vela_claim_non_content_expiry($1, $2, $3)
		`, "nonterminal-failed-clock", uuid.New(), 30).Scan(new(string)); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("nonterminal Job claim error = %v, want no rows", err)
		}
		observation := validFailureObservation()
		observation.FailureClass = "FATAL_BACKEND"
		observation.FailureFingerprint = "non-content-expiry.clock.failed"
		decision, err := fixture.service.Fail(
			context.Background(), fixture.worker, credentials, observation,
		)
		if err != nil || decision.Disposition != workercontrol.RetryDispositionFailed {
			t.Fatalf("terminalize Failure clock Job = %#v error=%v", decision, err)
		}
		assertNonContentTerminalClocks(t, fixture.database, assignment.JobID, "job.failed")
	})

	t.Run("succeeded", func(t *testing.T) {
		fixture := newStartFixture(t, "non-content-expiry-clock-succeeded", 7)
		if started, err := fixture.service.Start(
			context.Background(), fixture.worker, fixture.credentials,
		); err != nil || started.Decision != workercontrol.StartGranted {
			t.Fatalf("start Success clock Job = %#v error=%v", started, err)
		}
		plan, err := fixture.service.BeginFinalization(
			context.Background(), fixture.worker, fixture.credentials,
		)
		if err != nil || plan.Decision != workercontrol.FinalizationGranted {
			t.Fatalf("begin Success clock finalization = %#v error=%v", plan, err)
		}
		completionService := visibleCompletionService(t, fixture.database.DSN)
		artifactIDs := uploadAndVerifyFinalizationPlan(
			t, completionService, fixture.worker, fixture.credentials, plan,
		)
		completed, err := completionService.CompleteVisibleCompletion(
			context.Background(),
			fixture.worker,
			fixture.credentials,
			workercontrol.VisibleCompletionCandidate{
				CompletionID: uuid.New(), ExpectedJobVersion: plan.JobVersion,
				ArtifactIDs: artifactIDs,
			},
		)
		if err != nil || completed.Decision != workercontrol.VisibleCompletionCommitted {
			t.Fatalf("complete Success clock Job = %#v error=%v", completed, err)
		}
		assertNonContentTerminalClocks(
			t, fixture.database, fixture.assignment.JobID, "job.succeeded",
		)
	})
}

func TestNonContentExpirySerializesWithConcurrentHoldPlacement(t *testing.T) {
	t.Run("hold commits first", func(t *testing.T) {
		database, jobID := newCanceledNonContentExpiryFixture(t, "non-content-expiry-hold-first")
		seedCompliancePrincipal(t, database)
		compliancePool := newRolePool(
			t, database.DSN, "vela_compliance_login", "vela-compliance-password",
		)
		expiryPool := newRolePool(
			t, database.DSN, "vela_non_content_expiry_login", "vela-non-content-expiry-password",
		)
		makeJobExpiryDue(t, database, jobID, "JOB_METADATA")
		claimID := claimNonContentExpiry(t, expiryPool, "hold-first-claim", "JOB_METADATA", jobID)

		placementTx, err := compliancePool.Begin(context.Background())
		if err != nil {
			t.Fatalf("begin hold-first placement: %v", err)
		}
		defer func() { _ = placementTx.Rollback(context.Background()) }()
		holdID := uuid.New()
		if err := placementTx.QueryRow(context.Background(), `
			SELECT event_id FROM vela_apply_legal_hold_event(
				$1, $2, $3, $4, 'HOLD_PLACED', 'JOB', $5, $6, $7,
				$8::legal_hold_record_class[], 'LITIGATION', $9, $10
			)
		`, uuid.New(), "non-content-expiry-hold-first", int64(1), holdID,
			uuid.MustParse(testOrganizationID), uuid.MustParse(testProjectID), jobID,
			[]string{"METADATA"}, "non-content-expiry/hold-first",
			time.Now().UTC().Truncate(time.Microsecond),
		).Scan(new(uuid.UUID)); err != nil {
			t.Fatalf("place uncommitted hold-first Legal Hold: %v", err)
		}

		type completion struct {
			outcome string
			err     error
		}
		started := make(chan struct{})
		completed := make(chan completion, 1)
		go func() {
			close(started)
			var result completion
			result.err = expiryPool.QueryRow(context.Background(), `
				SELECT outcome FROM vela_complete_non_content_expiry($1, $2, $3, $4)
			`, "JOB_METADATA", jobID, claimID, 60).Scan(&result.outcome)
			completed <- result
		}()
		<-started
		select {
		case result := <-completed:
			t.Fatalf("expiry bypassed uncommitted Legal Hold gates: %#v", result)
		case <-time.After(100 * time.Millisecond):
		}
		if err := placementTx.Commit(context.Background()); err != nil {
			t.Fatalf("commit hold-first placement: %v", err)
		}
		select {
		case result := <-completed:
			if result.err != nil || result.outcome != "HELD" {
				t.Fatalf("hold-first completion = %#v", result)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("hold-first expiry did not resume")
		}
		var jobs, receipts int64
		if err := database.Admin.QueryRow(`
			SELECT (SELECT count(*) FROM jobs WHERE id = $1),
				(SELECT count(*) FROM non_content_expiry_receipts WHERE source_id = $1)
		`, jobID).Scan(&jobs, &receipts); err != nil {
			t.Fatalf("read hold-first result: %v", err)
		}
		if jobs != 1 || receipts != 0 {
			t.Fatalf("hold-first jobs/receipts = %d/%d", jobs, receipts)
		}
	})

	t.Run("expiry commits first", func(t *testing.T) {
		database, jobID := newCanceledNonContentExpiryFixture(t, "non-content-expiry-expiry-first")
		seedCompliancePrincipal(t, database)
		compliancePool := newRolePool(
			t, database.DSN, "vela_compliance_login", "vela-compliance-password",
		)
		compliance, err := legalhold.NewService(context.Background(), compliancePool)
		if err != nil {
			t.Fatalf("configure expiry-first Compliance service: %v", err)
		}
		expiryPool := newRolePool(
			t, database.DSN, "vela_non_content_expiry_login", "vela-non-content-expiry-password",
		)
		makeJobExpiryDue(t, database, jobID, "JOB_METADATA")
		claimID := claimNonContentExpiry(t, expiryPool, "expiry-first-claim", "JOB_METADATA", jobID)
		expiryTx, err := expiryPool.Begin(context.Background())
		if err != nil {
			t.Fatalf("begin expiry-first completion: %v", err)
		}
		defer func() { _ = expiryTx.Rollback(context.Background()) }()
		var outcome string
		if err := expiryTx.QueryRow(context.Background(), `
			SELECT outcome FROM vela_complete_non_content_expiry($1, $2, $3, $4)
		`, "JOB_METADATA", jobID, claimID, 60).Scan(&outcome); err != nil || outcome != "EXPIRED" {
			t.Fatalf("execute uncommitted expiry-first completion = %s error=%v", outcome, err)
		}

		placementStarted := make(chan struct{})
		placementDone := make(chan error, 1)
		projectID := uuid.MustParse(testProjectID)
		go func() {
			close(placementStarted)
			_, applyErr := compliance.Apply(context.Background(), legalhold.Request{
				IdempotencyKey: "non-content-expiry-expiry-first", SourceSequence: 1,
				HoldID: uuid.New(), Kind: legalhold.KindHoldPlaced,
				Scope: legalhold.ScopeJob, OrganizationID: uuid.MustParse(testOrganizationID),
				ProjectID: &projectID, JobID: &jobID,
				RecordClasses: []legalhold.RecordClass{legalhold.RecordClassMetadata},
				ReasonCode:    "LITIGATION", ExternalReference: "non-content-expiry/expiry-first",
				EffectiveAt: time.Now().UTC().Truncate(time.Microsecond),
			})
			placementDone <- applyErr
		}()
		<-placementStarted
		select {
		case err := <-placementDone:
			t.Fatalf("Legal Hold bypassed uncommitted expiry gates: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		if err := expiryTx.Commit(context.Background()); err != nil {
			t.Fatalf("commit expiry-first completion: %v", err)
		}
		select {
		case err := <-placementDone:
			if err != nil {
				t.Fatalf("place post-expiry Legal Hold: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("expiry-first Legal Hold placement did not resume")
		}
		var jobs, receipts, activeHolds int64
		if err := database.Admin.QueryRow(`
			SELECT (SELECT count(*) FROM jobs WHERE id = $1),
				(SELECT count(*) FROM non_content_expiry_receipts WHERE source_id = $1),
				(SELECT count(*) FROM legal_holds WHERE job_id = $1 AND state = 'ACTIVE')
		`, jobID).Scan(&jobs, &receipts, &activeHolds); err != nil {
			t.Fatalf("read expiry-first result: %v", err)
		}
		if jobs != 0 || receipts != 1 || activeHolds != 1 {
			t.Fatalf("expiry-first jobs/receipts/holds = %d/%d/%d", jobs, receipts, activeHolds)
		}
	})

	t.Run("release commits before retry completion", func(t *testing.T) {
		database, jobID := newCanceledNonContentExpiryFixture(t, "non-content-expiry-release-retry")
		seedCompliancePrincipal(t, database)
		compliancePool := newRolePool(
			t, database.DSN, "vela_compliance_login", "vela-compliance-password",
		)
		compliance, err := legalhold.NewService(context.Background(), compliancePool)
		if err != nil {
			t.Fatalf("configure release-retry Compliance service: %v", err)
		}
		expiryPool := newRolePool(
			t, database.DSN, "vela_non_content_expiry_login", "vela-non-content-expiry-password",
		)
		projectID := uuid.MustParse(testProjectID)
		holdID := uuid.New()
		effectiveAt := time.Now().UTC().Truncate(time.Microsecond)
		if _, err := compliance.Apply(context.Background(), legalhold.Request{
			IdempotencyKey: "non-content-expiry-release-retry-place", SourceSequence: 1,
			HoldID: holdID, Kind: legalhold.KindHoldPlaced,
			Scope: legalhold.ScopeJob, OrganizationID: uuid.MustParse(testOrganizationID),
			ProjectID: &projectID, JobID: &jobID,
			RecordClasses: []legalhold.RecordClass{legalhold.RecordClassMetadata},
			ReasonCode:    "LITIGATION", ExternalReference: "non-content-expiry/release-retry-place",
			EffectiveAt: effectiveAt,
		}); err != nil {
			t.Fatalf("place release-retry Legal Hold: %v", err)
		}

		makeJobExpiryDue(t, database, jobID, "JOB_METADATA")
		firstClaimID := claimNonContentExpiry(
			t, expiryPool, "release-retry-first", "JOB_METADATA", jobID,
		)
		var firstOutcome string
		if err := expiryPool.QueryRow(context.Background(), `
			SELECT outcome FROM vela_complete_non_content_expiry($1, $2, $3, $4)
		`, "JOB_METADATA", jobID, firstClaimID, 1).Scan(&firstOutcome); err != nil || firstOutcome != "HELD" {
			t.Fatalf("held release-retry completion = %s error=%v", firstOutcome, err)
		}

		if err := expiryPool.QueryRow(context.Background(), `
			SELECT kind::text FROM vela_claim_non_content_expiry($1, $2, $3)
		`, "release-retry-too-early", uuid.New(), 30).Scan(new(string)); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("early held retry claim error = %v, want no rows", err)
		}
		retryClaimID := uuid.New()
		var retryKind string
		var retrySourceID, returnedRetryClaimID uuid.UUID
		retryDeadline := time.Now().Add(5 * time.Second)
		for {
			err := expiryPool.QueryRow(context.Background(), `
				SELECT kind::text, source_id, claim_id
				FROM vela_claim_non_content_expiry($1, $2, $3)
			`, "release-retry-second", retryClaimID, 30).Scan(
				&retryKind, &retrySourceID, &returnedRetryClaimID,
			)
			if err == nil {
				break
			}
			if !errors.Is(err, pgx.ErrNoRows) || time.Now().After(retryDeadline) {
				t.Fatalf("claim held retry after delay: %v", err)
			}
			time.Sleep(25 * time.Millisecond)
		}
		if retryKind != "JOB_METADATA" || retrySourceID != jobID ||
			returnedRetryClaimID != retryClaimID {
			t.Fatalf("held retry claim = %s/%s/%s", retryKind, retrySourceID, returnedRetryClaimID)
		}
		releaseTx, err := compliancePool.Begin(context.Background())
		if err != nil {
			t.Fatalf("begin release-retry Legal Hold release: %v", err)
		}
		defer func() { _ = releaseTx.Rollback(context.Background()) }()
		if err := releaseTx.QueryRow(context.Background(), `
			SELECT event_id FROM vela_apply_legal_hold_event(
				$1, $2, $3, $4, 'HOLD_RELEASED', NULL::legal_hold_scope,
				NULL::uuid, NULL::uuid, NULL::uuid, NULL::legal_hold_record_class[],
				'ORDER_LIFTED', $5, $6
			)
		`, uuid.New(), "non-content-expiry-release-retry-release", int64(2), holdID,
			"non-content-expiry/release-retry-release", effectiveAt.Add(time.Minute),
		).Scan(new(uuid.UUID)); err != nil {
			t.Fatalf("apply uncommitted release-retry Legal Hold release: %v", err)
		}

		type completion struct {
			outcome string
			err     error
		}
		started := make(chan struct{})
		completed := make(chan completion, 1)
		go func() {
			close(started)
			var result completion
			result.err = expiryPool.QueryRow(context.Background(), `
				SELECT outcome FROM vela_complete_non_content_expiry($1, $2, $3, $4)
			`, "JOB_METADATA", jobID, retryClaimID, 60).Scan(&result.outcome)
			completed <- result
		}()
		<-started
		select {
		case result := <-completed:
			t.Fatalf("retry completion bypassed uncommitted Legal Hold release: %#v", result)
		case <-time.After(100 * time.Millisecond):
		}
		if err := releaseTx.Commit(context.Background()); err != nil {
			t.Fatalf("commit release-retry Legal Hold release: %v", err)
		}
		select {
		case result := <-completed:
			if result.err != nil || result.outcome != "EXPIRED" {
				t.Fatalf("released retry completion = %#v", result)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("released retry completion did not resume")
		}

		for label, staleClaimID := range map[string]uuid.UUID{
			"held claim":            firstClaimID,
			"completed retry claim": retryClaimID,
		} {
			var staleOutcome string
			if err := expiryPool.QueryRow(context.Background(), `
				SELECT outcome FROM vela_complete_non_content_expiry($1, $2, $3, $4)
			`, "JOB_METADATA", jobID, staleClaimID, 60).Scan(&staleOutcome); err != nil ||
				staleOutcome != "STALE" {
				t.Fatalf("%s completion = %s error=%v, want STALE", label, staleOutcome, err)
			}
		}
		var jobs, receipts, expiredCandidates int64
		if err := database.Admin.QueryRow(`
			SELECT (SELECT count(*) FROM jobs WHERE id = $1),
				(SELECT count(*) FROM non_content_expiry_receipts
				 WHERE kind = 'JOB_METADATA' AND source_id = $1),
				(SELECT count(*) FROM non_content_expiry_candidates
				 WHERE kind = 'JOB_METADATA' AND source_id = $1 AND state = 'EXPIRED')
		`, jobID).Scan(&jobs, &receipts, &expiredCandidates); err != nil {
			t.Fatalf("read release-retry result: %v", err)
		}
		if jobs != 0 || receipts != 1 || expiredCandidates != 1 {
			t.Fatalf(
				"release-retry jobs/receipts/expired-candidates = %d/%d/%d",
				jobs, receipts, expiredCandidates,
			)
		}
	})
}

func acknowledgeNonContentExpiryCancellation(t *testing.T, fixture invoiceExportFixture) {
	t.Helper()
	coordinator := cancellation.NewService(
		newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password"),
		newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password"),
	)
	result, err := coordinator.AcknowledgeCancellationStop(
		context.Background(), fixture.worker, fixture.credentials, fixture.cancellationID,
	)
	if err != nil || result.Decision != cancellation.StopAcknowledged ||
		result.State != "CANCELED" || result.ReceiptID == uuid.Nil {
		t.Fatalf("acknowledge non-content expiry cancellation = %#v error=%v", result, err)
	}
}

func exportNonContentExpiryInvoice(t *testing.T, fixture invoiceExportFixture) {
	t.Helper()
	adapter := &recordingInvoiceAdapter{receipt: billingexport.Receipt{
		InvoiceReference: "non-content-expiry-invoice",
		LineReference:    "non-content-expiry-line",
	}}
	exporter, err := billingexport.NewService(
		newRolePool(t, fixture.database.DSN, "vela_billing_login", "vela-billing-password"),
		adapter,
		billingexport.Config{
			ExporterID: "non-content-expiry-exporter", BatchSize: 1,
			ClaimTTL: 30 * time.Second, RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("configure non-content expiry Invoice exporter: %v", err)
	}
	result, err := exporter.ExportBatch(context.Background())
	if err != nil || result.Claimed != 1 || result.Exported != 1 {
		t.Fatalf("export non-content expiry Invoice = %#v error=%v", result, err)
	}
}

func newCanceledNonContentExpiryFixture(t *testing.T, key string) (testDatabase, uuid.UUID) {
	t.Helper()
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	accepted := submitJob(t, server.URL, key, []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"expire non-content records without retaining this content"
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
		t.Fatalf("cancel Job status = %d, want 200; body=%s", canceled.StatusCode, canceled.Body)
	}
	return database, uuid.MustParse(job.JobID)
}

func makeJobExpiryDue(t *testing.T, database testDatabase, jobID uuid.UUID, kind string) {
	t.Helper()
	transaction, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin non-content expiry clock adjustment: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	if _, err := transaction.Exec("SET LOCAL session_replication_role = 'replica'"); err != nil {
		t.Fatalf("disable triggers for non-content expiry clock adjustment: %v", err)
	}
	switch kind {
	case "JOB_METADATA":
		if _, err := transaction.Exec(`
			UPDATE non_content_job_roots
			SET terminal_at = clock_timestamp() - interval '3000 days',
				metadata_expires_at = clock_timestamp() - interval '2000 days',
				financial_expires_at = clock_timestamp() + interval '1 day'
			WHERE id = $1
		`, jobID); err != nil {
			t.Fatalf("make Metadata root expiry due: %v", err)
		}
		if _, err := transaction.Exec(`
			UPDATE non_content_expiry_candidates
			SET expires_at = clock_timestamp() - interval '2000 days',
				next_attempt_at = clock_timestamp() - interval '2000 days'
			WHERE kind = 'JOB_METADATA' AND source_id = $1
		`, jobID); err != nil {
			t.Fatalf("make Metadata candidate expiry due: %v", err)
		}
	case "JOB_FINANCIAL":
		if _, err := transaction.Exec(`
			UPDATE non_content_job_roots
			SET financial_expires_at = clock_timestamp() - interval '1 day'
			WHERE id = $1
		`, jobID); err != nil {
			t.Fatalf("make Financial root expiry due: %v", err)
		}
		if _, err := transaction.Exec(`
			UPDATE non_content_expiry_candidates
			SET expires_at = clock_timestamp() - interval '1 day',
				next_attempt_at = clock_timestamp() - interval '1 second'
			WHERE kind = 'JOB_FINANCIAL' AND source_id = $1
		`, jobID); err != nil {
			t.Fatalf("make Financial candidate expiry due: %v", err)
		}
	default:
		t.Fatalf("unsupported expiry kind %q", kind)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit non-content expiry clock adjustment: %v", err)
	}
}

func makeOrganizationFinancialExpiryDue(
	t *testing.T,
	database testDatabase,
	recordID uuid.UUID,
) {
	t.Helper()
	transaction, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Organization Financial expiry clock adjustment: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	if _, err := transaction.Exec("SET LOCAL session_replication_role = 'replica'"); err != nil {
		t.Fatalf("disable triggers for Organization Financial expiry clock adjustment: %v", err)
	}
	if _, err := transaction.Exec(`
		UPDATE non_content_expiry_candidates
		SET expires_at = clock_timestamp() - interval '1 day',
			next_attempt_at = clock_timestamp() - interval '1 second'
		WHERE kind = 'ORGANIZATION_FINANCIAL' AND source_id = $1
	`, recordID); err != nil {
		t.Fatalf("make Organization Financial expiry due: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit Organization Financial expiry clock adjustment: %v", err)
	}
}

func expireNonContentClaim(t *testing.T, database testDatabase, kind string, sourceID uuid.UUID) {
	t.Helper()
	transaction, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin non-content claim expiry adjustment: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	if _, err := transaction.Exec("SET LOCAL session_replication_role = 'replica'"); err != nil {
		t.Fatalf("disable triggers for non-content claim expiry adjustment: %v", err)
	}
	if _, err := transaction.Exec(`
		UPDATE non_content_expiry_candidates
		SET claim_expires_at = clock_timestamp() - interval '1 second'
		WHERE kind = $1 AND source_id = $2 AND state = 'CLAIMED'
	`, kind, sourceID); err != nil {
		t.Fatalf("expire non-content claim: %v", err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit non-content claim expiry adjustment: %v", err)
	}
}

func assertNonContentTerminalClocks(
	t *testing.T,
	database testDatabase,
	jobID uuid.UUID,
	eventType string,
) {
	t.Helper()
	var terminalAt, occurredAt, metadataExpiresAt, financialExpiresAt time.Time
	var metadataCandidateExpiresAt, financialCandidateExpiresAt time.Time
	var metadataCandidates, financialCandidates int64
	if err := database.Admin.QueryRow(`
		SELECT root.terminal_at, event.occurred_at,
			root.metadata_expires_at, root.financial_expires_at,
			metadata.expires_at, financial.expires_at,
			(SELECT count(*) FROM non_content_expiry_candidates
			 WHERE kind = 'JOB_METADATA' AND source_id = $1),
			(SELECT count(*) FROM non_content_expiry_candidates
			 WHERE kind = 'JOB_FINANCIAL' AND source_id = $1)
		FROM non_content_job_roots AS root
		JOIN outbox_events AS event
		  ON event.aggregate_type = 'Job'
		 AND event.aggregate_id = root.id
		 AND event.event_type = $2
		JOIN non_content_expiry_candidates AS metadata
		  ON metadata.kind = 'JOB_METADATA' AND metadata.source_id = root.id
		JOIN non_content_expiry_candidates AS financial
		  ON financial.kind = 'JOB_FINANCIAL' AND financial.source_id = root.id
		WHERE root.id = $1
	`, jobID, eventType).Scan(
		&terminalAt, &occurredAt, &metadataExpiresAt, &financialExpiresAt,
		&metadataCandidateExpiresAt, &financialCandidateExpiresAt,
		&metadataCandidates, &financialCandidates,
	); err != nil {
		t.Fatalf("read %s non-content expiry clocks: %v", eventType, err)
	}
	if !terminalAt.Equal(occurredAt) ||
		!metadataExpiresAt.Equal(occurredAt.Add(365*24*time.Hour)) ||
		!financialExpiresAt.Equal(occurredAt.Add(2557*24*time.Hour)) ||
		!metadataCandidateExpiresAt.Equal(metadataExpiresAt) ||
		!financialCandidateExpiresAt.Equal(financialExpiresAt) ||
		metadataCandidates != 1 || financialCandidates != 1 {
		t.Fatalf(
			"%s clocks terminal/event/metadata/financial/candidates = %s/%s/%s/%s/%d/%d",
			eventType, terminalAt, occurredAt, metadataExpiresAt, financialExpiresAt,
			metadataCandidates, financialCandidates,
		)
	}
}

func claimNonContentExpiry(
	t *testing.T,
	pool interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	},
	instanceID string,
	wantKind string,
	wantSourceID uuid.UUID,
) uuid.UUID {
	t.Helper()
	claimID := uuid.New()
	var kind string
	var sourceID, returnedClaimID uuid.UUID
	if err := pool.QueryRow(context.Background(), `
		SELECT kind::text, source_id, claim_id
		FROM vela_claim_non_content_expiry($1, $2, $3)
	`, instanceID, claimID, 30).Scan(&kind, &sourceID, &returnedClaimID); err != nil {
		t.Fatalf("claim non-content expiry: %v", err)
	}
	if kind != wantKind || sourceID != wantSourceID || returnedClaimID != claimID {
		t.Fatalf("claimed non-content expiry = %s/%s/%s", kind, sourceID, returnedClaimID)
	}
	return claimID
}
