//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/usagecostledger"
)

func TestRuntimeUsageRecordsAllocationAndCacheWithoutInventingGPUTime(t *testing.T) {
	fixture := runH3CampaignEvidenceFixture(t)
	rows, err := fixture.database.Admin.Query(`
		SELECT allocation.allocated_at, allocation.released_at, usage.quantity,
		       usage.interval_start, usage.interval_end
		FROM stage_allocations AS allocation
		LEFT JOIN resource_usage_records AS usage
		  ON usage.source_authority_id = allocation.stage_attempt_id
		 AND usage.source_kind = 'STAGE_ATTEMPT'
		 AND usage.resource_kind::text = 'ALLOCATION_NANOSECOND'
		WHERE allocation.state = 'RELEASED'
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var allocated, released, start, end time.Time
		var quantity *int64
		var recordedStart, recordedEnd *time.Time
		if err := rows.Scan(&allocated, &released, &quantity, &recordedStart, &recordedEnd); err != nil {
			t.Fatal(err)
		}
		if quantity == nil || recordedStart == nil || recordedEnd == nil {
			t.Fatal("released runtime allocation has no durable usage receipt")
		}
		start, end = *recordedStart, *recordedEnd
		if *quantity != released.Sub(allocated).Nanoseconds() || !start.Equal(allocated) || !end.Equal(released) {
			t.Fatalf("allocation receipt: quantity=%d, interval=%s..%s; authority=%s..%s", *quantity, start, end, allocated, released)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("campaign did not release any allocations")
	}
	var cacheCount, cacheWithoutPhysicalAncestor, inventedCompute, chargeCount int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
		 (SELECT count(*) FROM resource_usage_records
		  WHERE job_id = $1 AND attribution = 'COUNTERFACTUAL'
		    AND resource_kind::text = 'ALLOCATION_NANOSECOND'),
		 (SELECT count(*) FROM stage_cache_references AS reference
		  WHERE reference.owner_job_id = $1 AND reference.owner_stage_run_id IS NOT NULL
		    AND NOT EXISTS (
		      SELECT 1 FROM resource_usage_records AS usage
		      JOIN stage_artifacts AS artifact ON artifact.id = reference.stage_artifact_id
		      JOIN resource_usage_records AS source
		        ON source.source_authority_id = artifact.producer_stage_attempt_id
		       AND source.resource_kind::text = 'ALLOCATION_NANOSECOND'
		       AND source.attribution = 'DIRECT'
		      WHERE usage.source_authority_id = reference.id
		        AND usage.attribution = 'COUNTERFACTUAL'
		        AND usage.quantity = source.quantity
		        AND usage.job_id = reference.owner_job_id)),
		 (SELECT count(*) FROM resource_usage_records
		  WHERE resource_kind IN ('GPU_NANOSECOND', 'CPU_NANOSECOND')),
		 (SELECT count(*) FROM charges WHERE job_id = $1)
	`, fixture.cacheJobID).Scan(&cacheCount, &cacheWithoutPhysicalAncestor, &inventedCompute, &chargeCount); err != nil {
		t.Fatal(err)
	}
	if cacheCount != 2 || cacheWithoutPhysicalAncestor != 0 || inventedCompute != 0 || chargeCount != 1 {
		t.Fatalf("cache receipts=%d missing ancestor=%d invented compute=%d charges=%d", cacheCount, cacheWithoutPhysicalAncestor, inventedCompute, chargeCount)
	}
	var consumed, measured, wrongDestination int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*), count(usage.id),
		       count(*) FILTER (WHERE usage.job_id IS DISTINCT FROM pin.owner_job_id
		          OR usage.quantity IS DISTINCT FROM ticket.size_bytes)
		FROM transfer_tickets AS ticket
		JOIN stage_artifact_pins AS pin ON pin.id = ticket.stage_artifact_pin_id
		LEFT JOIN resource_usage_records AS usage
		  ON usage.source_kind = 'TRANSFER_TICKET' AND usage.source_authority_id = ticket.id
		 AND usage.resource_kind = 'BYTE' AND usage.attribution = 'DIRECT'
		WHERE ticket.state = 'CONSUMED'
	`).Scan(&consumed, &measured, &wrongDestination); err != nil {
		t.Fatal(err)
	}
	if consumed != 4 || measured != 4 || wrongDestination != 0 {
		t.Fatalf("successful payload receipts: consumed=%d measured=%d wrong attribution=%d", consumed, measured, wrongDestination)
	}
	verifyRuntimeUsageValuation(t, fixture.database)
}

func verifyRuntimeUsageValuation(t *testing.T, database testDatabase) {
	t.Helper()
	ledger, err := usagecostledger.NewPostgres(newRolePool(
		t, database.DSN, "vela_usage_cost_login", "vela-usage-cost-password",
	))
	if err != nil {
		t.Fatal(err)
	}
	modelID := uuid.New()
	// Unit rates are an exact valuation oracle, not a calibrated economic claim.
	if _, err := database.Admin.Exec(`
		INSERT INTO cost_model_revisions (
		 id, stable_id, revision, state, effective_at, resource_valuations,
		 allocation_method, evidence_digest, content_digest
		) VALUES ($1, 'synthetic-runtime-unit-rates', 1, 'ACTIVE', now() - interval '1 day',
		 '{"ALLOCATION_NANOSECOND":{"numerator_micro_units":1,"denominator_units":1},
		   "BYTE":{"numerator_micro_units":1,"denominator_units":1}}',
		 'DIRECT_PLUS_SEPARATE_SHARED', decode(repeat('b1',32),'hex'), decode(repeat('b2',32),'hex'))
	`, modelID); err != nil {
		t.Fatal(err)
	}
	var chargeBefore string
	if err := database.Admin.QueryRow(`
		SELECT coalesce(jsonb_agg(to_jsonb(charge) ORDER BY id)::text, '[]') FROM charges AS charge
	`).Scan(&chargeBefore); err != nil {
		t.Fatal(err)
	}
	rows, err := database.Admin.Query(`
		SELECT id, attribution, quantity, recorded_at FROM resource_usage_records ORDER BY id
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var direct, counterfactual int64
	var first, last time.Time
	var receiptCount int64
	for rows.Next() {
		var id uuid.UUID
		var attribution usagecostledger.Attribution
		var quantity int64
		var recordedAt time.Time
		if err := rows.Scan(&id, &attribution, &quantity, &recordedAt); err != nil {
			t.Fatal(err)
		}
		if first.IsZero() || recordedAt.Before(first) {
			first = recordedAt
		}
		if recordedAt.After(last) {
			last = recordedAt
		}
		switch attribution {
		case usagecostledger.AttributionDirect:
			direct += quantity
		case usagecostledger.AttributionCounterfactual:
			counterfactual += quantity
		default:
			t.Fatalf("physical/cache fixture produced unexpected %s attribution", attribution)
		}
		command := usagecostledger.ValueCommand{
			AllocationID: uuid.New(), UsageID: id, CostModelRevisionID: modelID,
			ValuedAt: recordedAt.Add(time.Second),
		}
		value, err := ledger.Value(context.Background(), command)
		if err != nil || value.CostMicroUnits != quantity || value.Quantity != quantity || value.Replayed {
			t.Fatalf("runtime receipt unit valuation = %+v error=%v; quantity=%d", value, err, quantity)
		}
		replayed, err := ledger.Value(context.Background(), command)
		if err != nil || !replayed.Replayed || replayed.ID != value.ID || replayed.CostMicroUnits != quantity {
			t.Fatalf("runtime receipt valuation replay = %+v error=%v", replayed, err)
		}
		receiptCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if receiptCount == 0 || direct == 0 || counterfactual == 0 {
		t.Fatal("runtime valuation fixture did not exercise physical and cache usage")
	}
	summary, err := ledger.Summarize(context.Background(), usagecostledger.SummaryQuery{
		CostModelRevisionID: modelID, From: first.Add(-time.Second), To: last.Add(time.Second),
	})
	if err != nil || summary.DirectCostMicroUnits != direct ||
		summary.CounterfactualAvoidedCostMicroUnits != counterfactual ||
		summary.SharedCostMicroUnits != 0 || summary.UnvaluedRecordCount != 0 {
		t.Fatalf("runtime valuation summary = %+v error=%v; direct=%d counterfactual=%d", summary, err, direct, counterfactual)
	}
	var chargeAfter string
	var valuations int64
	if err := database.Admin.QueryRow(`
		SELECT (SELECT coalesce(jsonb_agg(to_jsonb(charge) ORDER BY id)::text, '[]') FROM charges AS charge),
		       (SELECT count(*) FROM cost_allocation_records WHERE cost_model_revision_id = $1)
	`, modelID).Scan(&chargeAfter, &valuations); err != nil {
		t.Fatal(err)
	}
	if chargeBefore != chargeAfter || valuations != receiptCount {
		t.Fatalf("runtime valuation changed Charge or duplicated receipts: valuations=%d receipts=%d", valuations, receiptCount)
	}
}
