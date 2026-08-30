package usagecostledger

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresBackend struct {
	pool *pgxpool.Pool
}

func NewPostgres(pool *pgxpool.Pool) (*UsageCostLedger, error) {
	if pool == nil {
		return nil, errors.New("Usage/Cost Ledger database pool is required")
	}
	return New(&PostgresBackend{pool: pool})
}

func (backend *PostgresBackend) RecordUsage(
	ctx context.Context,
	command RecordUsageCommand,
) (UsageIdentity, error) {
	payload, err := json.Marshal(map[string]any{
		"id":                  command.ID,
		"schema_version":      command.SchemaVersion,
		"source_kind":         command.SourceKind,
		"source_authority_id": command.SourceAuthorityID,
		"receipt_digest":      hex.EncodeToString(command.ReceiptDigest[:]),
		"attribution":         command.Attribution,
		"usage_class":         command.UsageClass,
		"resource_kind":       command.ResourceKind,
		"quantity":            command.Quantity,
		"organization_id":     nullableUUID(command.OrganizationID),
		"project_id":          nullableUUID(command.ProjectID),
		"job_id":              nullableUUID(command.JobID),
		"attempt_id":          nullableUUID(command.AttemptID),
		"stage_attempt_id":    nullableUUID(command.StageAttemptID),
		"capacity_pool_id":    nullableUUID(command.CapacityPoolID),
		"interval_start":      command.IntervalStart.UTC().Format(time.RFC3339Nano),
		"interval_end":        command.IntervalEnd.UTC().Format(time.RFC3339Nano),
		"recorded_at":         command.RecordedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return UsageIdentity{}, fmt.Errorf("encode Resource Usage receipt: %w", err)
	}
	var result UsageIdentity
	err = backend.pool.QueryRow(ctx, `
		SELECT usage_record_id, replayed
		FROM vela_record_resource_usage($1::jsonb)
	`, payload).Scan(&result.ID, &result.Replayed)
	if err != nil {
		return UsageIdentity{}, mapReplayMismatch("record Resource Usage", err)
	}
	return result, nil
}

func (backend *PostgresBackend) Value(
	ctx context.Context,
	command ValueCommand,
) (CostAllocation, error) {
	payload, err := json.Marshal(map[string]any{
		"allocation_id":          command.AllocationID,
		"usage_record_id":        command.UsageID,
		"cost_model_revision_id": command.CostModelRevisionID,
		"valued_at":              command.ValuedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return CostAllocation{}, fmt.Errorf("encode Usage Cost valuation: %w", err)
	}
	var (
		allocation CostAllocation
		supersedes *uuid.UUID
	)
	err = backend.pool.QueryRow(ctx, `
		SELECT allocation_id, usage_record_id, cost_model_revision_id,
		       supersedes_allocation_id, attribution::text, resource_kind::text,
		       quantity, rate_numerator_micro_units, rate_denominator_units,
		       cost_micro_units, valued_at, replayed
		FROM vela_value_resource_usage($1::jsonb)
	`, payload).Scan(
		&allocation.ID,
		&allocation.UsageID,
		&allocation.CostModelRevisionID,
		&supersedes,
		&allocation.Attribution,
		&allocation.ResourceKind,
		&allocation.Quantity,
		&allocation.RateNumeratorMicroUnits,
		&allocation.RateDenominatorUnits,
		&allocation.CostMicroUnits,
		&allocation.ValuedAt,
		&allocation.Replayed,
	)
	if err != nil {
		return CostAllocation{}, mapReplayMismatch("value Resource Usage", err)
	}
	if supersedes != nil {
		allocation.SupersedesAllocationID = *supersedes
	}
	return allocation, nil
}

func (backend *PostgresBackend) Summarize(
	ctx context.Context,
	query SummaryQuery,
) (OperatorSummary, error) {
	rows, err := backend.pool.Query(ctx, `
		SELECT attribution::text, usage_class::text, resource_kind::text,
		       quantity, cost_micro_units, usage_record_count, unvalued_record_count
		FROM vela_summarize_usage_cost($1, $2, $3)
	`, query.CostModelRevisionID, query.From.UTC(), query.To.UTC())
	if err != nil {
		return OperatorSummary{}, fmt.Errorf("summarize Usage Cost: %w", err)
	}
	defer rows.Close()

	var summary OperatorSummary
	for rows.Next() {
		var bucket SummaryBucket
		if err := rows.Scan(
			&bucket.Attribution,
			&bucket.UsageClass,
			&bucket.ResourceKind,
			&bucket.Quantity,
			&bucket.CostMicroUnits,
			&bucket.UsageRecordCount,
			&bucket.UnvaluedRecordCount,
		); err != nil {
			return OperatorSummary{}, fmt.Errorf("scan Usage Cost summary: %w", err)
		}
		summary.Buckets = append(summary.Buckets, bucket)
	}
	if err := rows.Err(); err != nil {
		return OperatorSummary{}, fmt.Errorf("read Usage Cost summary: %w", err)
	}
	return summary, nil
}

func nullableUUID(value uuid.UUID) any {
	if value == uuid.Nil {
		return nil
	}
	return value
}

func mapReplayMismatch(operation string, err error) error {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) &&
		(postgresError.ConstraintName == "resource_usage_record_replay_mismatch" ||
			postgresError.ConstraintName == "cost_allocation_record_replay_mismatch") {
		return fmt.Errorf("%s: %w", operation, ErrReplayMismatch)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
