//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/usagecostledger"
)

func TestUsageCostLedgerIsIdempotentRevaluableAndContentFree(t *testing.T) {
	database, coordinator, serverURL := newH3IntegrationEnvironment(t)
	seedWorkerRegistryPlan(t, database.Admin)
	registry, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_internal_login", "vela-internal-password",
	))
	if err != nil {
		t.Fatalf("construct Usage/Cost fixture Worker Registry: %v", err)
	}
	job, attemptID := instantiateH3IntegrationGraph(
		t, database, serverURL, "usage-cost-ledger",
	)
	jobID := uuid.MustParse(job.JobID)
	var encoderRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'
	`, attemptID).Scan(&encoderRunID); err != nil {
		t.Fatalf("read Encoder StageRun: %v", err)
	}
	encoder := h3IntegrationStage{
		key: "encoder", stage: "ENCODER",
		profileStableID: "h3-encoder-single-gpu", profileID: encoderStageProfileID,
		workerProfileID: encoderWorkerProfileID, component: "h3-encoder-v1",
		outputPort:      "conditioning",
		outputInterface: "49000000-0000-0000-0000-000000000011",
		nodeIdentity:    "usage-cost-encoder-node",
	}
	assignment := assignH3IntegrationStage(
		t, database, coordinator, registry, attemptID, encoderRunID, 1, encoder, 0xe1,
	)

	ledgerPool := newRolePool(
		t, database.DSN, "vela_usage_cost_login", "vela-usage-cost-password",
	)
	ledger, err := usagecostledger.NewPostgres(ledgerPool)
	if err != nil {
		t.Fatalf("construct Usage/Cost Ledger: %v", err)
	}
	modelOneID := uuid.New()
	modelTwoID := uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)
	seedCostModelRevision(t, database, modelOneID, "usage-cost-fixture-v1", 2, now)
	seedCostModelRevision(t, database, modelTwoID, "usage-cost-fixture-v2", 3, now)

	var quotedBefore int64
	var currencyBefore string
	var chargesBefore int64
	if err := database.Admin.QueryRow(`
		SELECT job.pricing_quoted_amount_minor, job.pricing_currency,
		       (SELECT count(*) FROM charges WHERE job_id = job.id)
		FROM jobs AS job WHERE job.id = $1
	`, jobID).Scan(&quotedBefore, &currencyBefore, &chargesBefore); err != nil {
		t.Fatalf("read fixed customer price before Usage ingestion: %v", err)
	}

	direct := usagecostledger.RecordUsageCommand{
		ID: uuid.New(), SchemaVersion: 1,
		SourceKind:        usagecostledger.SourceStageAttempt,
		SourceAuthorityID: assignment.StageAttemptID,
		ReceiptDigest:     sha256.Sum256([]byte("encoder-stage-runtime-receipt")),
		Attribution:       usagecostledger.AttributionDirect,
		UsageClass:        usagecostledger.UsageExecution,
		ResourceKind:      usagecostledger.ResourceGPUNanosecond,
		Quantity:          8_000_000_000,
		OrganizationID:    uuid.MustParse(testOrganizationID),
		ProjectID:         uuid.MustParse(testProjectID), JobID: jobID,
		AttemptID: attemptID, StageAttemptID: assignment.StageAttemptID,
		IntervalStart: now, IntervalEnd: now.Add(8 * time.Second),
		RecordedAt: now.Add(9 * time.Second),
	}
	first, err := ledger.RecordUsage(context.Background(), direct)
	if err != nil {
		t.Fatalf("record direct GPU usage: %v", err)
	}
	replayed, err := ledger.RecordUsage(context.Background(), direct)
	if err != nil {
		t.Fatalf("replay direct GPU usage: %v", err)
	}
	if first.ID != direct.ID || first.Replayed || replayed.ID != first.ID || !replayed.Replayed {
		t.Fatalf("direct usage first=%#v replay=%#v", first, replayed)
	}
	changed := direct
	changed.Quantity++
	if _, err := ledger.RecordUsage(context.Background(), changed); !errors.Is(
		err, usagecostledger.ErrReplayMismatch,
	) {
		t.Fatalf("changed Resource Usage replay error = %v", err)
	}

	shared := usagecostledger.RecordUsageCommand{
		ID: uuid.New(), SchemaVersion: 1,
		SourceKind:        usagecostledger.SourceCapacityPool,
		SourceAuthorityID: assignment.CapacityPoolID,
		ReceiptDigest:     sha256.Sum256([]byte("encoder-pool-residency-receipt")),
		Attribution:       usagecostledger.AttributionShared,
		UsageClass:        usagecostledger.UsageResidency,
		ResourceKind:      usagecostledger.ResourceGPUNanosecond,
		Quantity:          4_000_000_000, CapacityPoolID: assignment.CapacityPoolID,
		IntervalStart: now, IntervalEnd: now.Add(4 * time.Second),
		RecordedAt: now.Add(9 * time.Second),
	}
	if _, err := ledger.RecordUsage(context.Background(), shared); err != nil {
		t.Fatalf("record shared residency usage: %v", err)
	}

	counterfactual := usagecostledger.RecordUsageCommand{
		ID: uuid.New(), SchemaVersion: 1,
		SourceKind: usagecostledger.SourceStageCache, SourceAuthorityID: uuid.New(),
		ReceiptDigest:  sha256.Sum256([]byte("exact-cache-avoided-encoder-compute")),
		Attribution:    usagecostledger.AttributionCounterfactual,
		UsageClass:     usagecostledger.UsageCacheAvoidedCompute,
		ResourceKind:   usagecostledger.ResourceGPUNanosecond,
		Quantity:       5_000_000_000,
		OrganizationID: uuid.MustParse(testOrganizationID),
		ProjectID:      uuid.MustParse(testProjectID), JobID: jobID, AttemptID: attemptID,
		IntervalStart: now, IntervalEnd: now,
		RecordedAt: now.Add(9 * time.Second),
	}
	if _, err := ledger.RecordUsage(context.Background(), counterfactual); err != nil {
		t.Fatalf("record cache counterfactual: %v", err)
	}

	firstAllocationCommand := usagecostledger.ValueCommand{
		AllocationID: uuid.New(), UsageID: direct.ID,
		CostModelRevisionID: modelOneID, ValuedAt: now.Add(10 * time.Second),
	}
	firstAllocation, err := ledger.Value(context.Background(), firstAllocationCommand)
	if err != nil {
		t.Fatalf("value direct usage: %v", err)
	}
	firstAllocationReplay, err := ledger.Value(context.Background(), firstAllocationCommand)
	if err != nil {
		t.Fatalf("replay direct valuation: %v", err)
	}
	if firstAllocation.CostMicroUnits != 16 || firstAllocation.Replayed ||
		firstAllocationReplay.ID != firstAllocation.ID || !firstAllocationReplay.Replayed {
		t.Fatalf("direct allocations first=%#v replay=%#v", firstAllocation, firstAllocationReplay)
	}
	changedValuation := firstAllocationCommand
	changedValuation.AllocationID = uuid.New()
	if _, err := ledger.Value(context.Background(), changedValuation); !errors.Is(
		err, usagecostledger.ErrReplayMismatch,
	) {
		t.Fatalf("changed Cost allocation replay error = %v", err)
	}

	for _, usageID := range []uuid.UUID{shared.ID, counterfactual.ID} {
		if _, err := ledger.Value(context.Background(), usagecostledger.ValueCommand{
			AllocationID: uuid.New(), UsageID: usageID,
			CostModelRevisionID: modelOneID, ValuedAt: now.Add(10 * time.Second),
		}); err != nil {
			t.Fatalf("value usage %s: %v", usageID, err)
		}
	}
	revalued, err := ledger.Value(context.Background(), usagecostledger.ValueCommand{
		AllocationID: uuid.New(), UsageID: direct.ID,
		CostModelRevisionID: modelTwoID, ValuedAt: now.Add(11 * time.Second),
	})
	if err != nil {
		t.Fatalf("revalue direct usage: %v", err)
	}
	if revalued.CostMicroUnits != 24 ||
		revalued.SupersedesAllocationID != firstAllocation.ID {
		t.Fatalf("revalued allocation = %#v", revalued)
	}

	summary, err := ledger.Summarize(context.Background(), usagecostledger.SummaryQuery{
		CostModelRevisionID: modelOneID,
		From:                now.Add(-time.Second), To: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("summarize Usage/Cost Ledger: %v", err)
	}
	if summary.DirectCostMicroUnits != 16 || summary.SharedCostMicroUnits != 8 ||
		summary.CounterfactualAvoidedCostMicroUnits != 10 ||
		summary.UnvaluedRecordCount != 0 || len(summary.Buckets) != 3 {
		t.Fatalf("operator summary = %#v", summary)
	}
	secondSummary, err := ledger.Summarize(context.Background(), usagecostledger.SummaryQuery{
		CostModelRevisionID: modelTwoID,
		From:                now.Add(-time.Second), To: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("summarize revalued Usage/Cost Ledger: %v", err)
	}
	if secondSummary.DirectCostMicroUnits != 24 || secondSummary.SharedCostMicroUnits != 0 ||
		secondSummary.CounterfactualAvoidedCostMicroUnits != 0 ||
		secondSummary.UnvaluedRecordCount != 2 {
		t.Fatalf("revalued operator summary = %#v", secondSummary)
	}

	var usageCount, allocationCount, directQuantity int64
	if err := database.Admin.QueryRow(`
		SELECT (SELECT count(*) FROM resource_usage_records),
		       (SELECT count(*) FROM cost_allocation_records),
		       (SELECT quantity FROM resource_usage_records WHERE id = $1)
	`, direct.ID).Scan(&usageCount, &allocationCount, &directQuantity); err != nil {
		t.Fatalf("inspect immutable Usage/Cost rows: %v", err)
	}
	if usageCount != 3 || allocationCount != 4 || directQuantity != direct.Quantity {
		t.Fatalf(
			"Usage/Cost rows usage=%d allocations=%d direct_quantity=%d",
			usageCount, allocationCount, directQuantity,
		)
	}
	if _, err := database.Admin.Exec(
		"UPDATE resource_usage_records SET quantity = quantity + 1 WHERE id = $1", direct.ID,
	); err == nil {
		t.Fatal("immutable Resource Usage row accepted UPDATE")
	}
	if _, err := database.Admin.Exec(
		"DELETE FROM cost_allocation_records WHERE id = $1", firstAllocation.ID,
	); err == nil {
		t.Fatal("append-only Cost allocation accepted DELETE")
	}

	var quotedAfter int64
	var currencyAfter string
	var chargesAfter int64
	if err := database.Admin.QueryRow(`
		SELECT job.pricing_quoted_amount_minor, job.pricing_currency,
		       (SELECT count(*) FROM charges WHERE job_id = job.id)
		FROM jobs AS job WHERE job.id = $1
	`, jobID).Scan(&quotedAfter, &currencyAfter, &chargesAfter); err != nil {
		t.Fatalf("read fixed customer price after Usage valuation: %v", err)
	}
	if quotedAfter != quotedBefore || currencyAfter != currencyBefore || chargesAfter != chargesBefore {
		t.Fatalf(
			"customer billing changed quote %d/%s -> %d/%s charges %d -> %d",
			quotedBefore, currencyBefore, quotedAfter, currencyAfter, chargesBefore, chargesAfter,
		)
	}
	if _, err := ledgerPool.Exec(context.Background(), "SELECT request_content FROM jobs LIMIT 1"); err == nil {
		t.Fatal("Usage/Cost role can read Customer Content")
	}
	if _, err := ledgerPool.Exec(context.Background(), "SELECT * FROM charges LIMIT 1"); err == nil {
		t.Fatal("Usage/Cost role can read customer Charges")
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 46); err == nil {
		t.Fatal("migration 47 removed durable Usage/Cost evidence")
	}
}

func TestUsageCostLedgerMigrationAllowsEmptyRoundTrip(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 46); err != nil {
		t.Fatalf("remove empty Usage/Cost Ledger migration: %v", err)
	}
	if err := goose.Up(database.Admin, migrations); err != nil {
		t.Fatalf("restore Usage/Cost Ledger migration: %v", err)
	}
	var tablesReady bool
	if err := database.Admin.QueryRow(`
		SELECT to_regclass('public.resource_usage_records') IS NOT NULL
		   AND to_regclass('public.cost_allocation_records') IS NOT NULL
	`).Scan(&tablesReady); err != nil {
		t.Fatalf("inspect restored Usage/Cost tables: %v", err)
	}
	if !tablesReady {
		t.Fatal("Usage/Cost tables were not restored")
	}
}

func seedCostModelRevision(
	t *testing.T,
	database testDatabase,
	id uuid.UUID,
	stableID string,
	numerator int64,
	effectiveAt time.Time,
) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		INSERT INTO cost_model_revisions (
			id, stable_id, revision, state, effective_at, resource_valuations,
			allocation_method, evidence_digest, content_digest
		) VALUES (
			$1, $2, 1, 'ACTIVE', $3,
			jsonb_build_object(
				'GPU_NANOSECOND', jsonb_build_object(
					'numerator_micro_units', $4::bigint,
					'denominator_units', 1000000000::bigint
				)
			),
			'DIRECT_PLUS_SEPARATE_SHARED',
			decode(repeat('d1', 32), 'hex'), decode(repeat('d2', 32), 'hex')
		)
	`, id, stableID, effectiveAt.Add(-time.Hour), numerator); err != nil {
		t.Fatalf("seed CostModelRevision %s: %v", stableID, err)
	}
}
