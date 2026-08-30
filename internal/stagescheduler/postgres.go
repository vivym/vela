package stagescheduler

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/attemptcoordinator"
)

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) (*PostgresRepository, error) {
	if pool == nil {
		return nil, errors.New("StageScheduler database pool is required")
	}
	return &PostgresRepository{pool: pool}, nil
}

func (repository *PostgresRepository) Capture(
	ctx context.Context,
	authority WorkerAuthority,
	observation CapacityObservation,
) (CapturedSnapshot, error) {
	if repository == nil || repository.pool == nil {
		return CapturedSnapshot{}, errors.New("StageScheduler repository is not configured")
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version":            1,
		"capacity_pool_id":          authority.CapacityPoolID,
		"stage_profile_revision_id": authority.StageProfileRevisionID,
		"worker_instance_id":        authority.WorkerInstanceID,
		"worker_instance_epoch":     authority.WorkerInstanceEpoch,
		"device_set_digest":         hex.EncodeToString(authority.DeviceSetDigest),
		"membership_digest":         hex.EncodeToString(authority.MembershipDigest),
		"model_residency_id":        authority.ModelResidencyID,
		"model_runtime_epoch":       authority.ModelRuntimeEpoch,
		"capacity_vector":           authority.CapacityVector,
		"observation_sequence":      observation.Sequence,
	})
	if err != nil {
		return CapturedSnapshot{}, fmt.Errorf("encode StageScheduler Worker authority: %w", err)
	}
	var captured CapturedSnapshot
	var encodedSnapshot []byte
	if err := repository.pool.QueryRow(ctx, `
		SELECT snapshot_id, snapshot
		FROM vela_capture_stage_scheduler_snapshot($1::jsonb)
	`, payload).Scan(&captured.ID, &encodedSnapshot); err != nil {
		return CapturedSnapshot{}, fmt.Errorf("capture StageScheduler snapshot: %w", err)
	}
	if err := json.Unmarshal(encodedSnapshot, &captured.Snapshot); err != nil {
		return CapturedSnapshot{}, fmt.Errorf("decode StageScheduler snapshot: %w", err)
	}
	return captured, nil
}

func (repository *PostgresRepository) Claim(
	ctx context.Context,
	request ClaimRequest,
) (ClaimResult, error) {
	if repository == nil || repository.pool == nil {
		return ClaimResult{}, errors.New("StageScheduler repository is not configured")
	}
	payload, err := encodeClaimRequest(request)
	if err != nil {
		return ClaimResult{}, err
	}
	var result ClaimResult
	if err := repository.pool.QueryRow(ctx, `
		SELECT claim_id, replayed
		FROM vela_claim_stage_scheduler_decision($1::jsonb)
	`, payload).Scan(&result.ClaimID, &result.Replayed); err != nil {
		return ClaimResult{}, fmt.Errorf("persist StageScheduler claim: %w", err)
	}
	return result, nil
}

func (repository *PostgresRepository) Commit(
	ctx context.Context,
	claimID uuid.UUID,
	stageAttemptID uuid.UUID,
) error {
	if repository == nil || repository.pool == nil {
		return errors.New("StageScheduler repository is not configured")
	}
	if claimID == uuid.Nil || stageAttemptID == uuid.Nil {
		return errors.New("StageScheduler commit identities are required")
	}
	var committedID uuid.UUID
	var replayed bool
	if err := repository.pool.QueryRow(ctx, `
		SELECT claim_id, replayed
		FROM vela_commit_stage_scheduler_claim($1, $2)
	`, claimID, stageAttemptID).Scan(&committedID, &replayed); err != nil {
		return fmt.Errorf("commit StageScheduler claim: %w", err)
	}
	if committedID != claimID {
		return errors.New("StageScheduler committed claim identity changed")
	}
	return nil
}

func (repository *PostgresRepository) Abandon(
	ctx context.Context,
	claimID uuid.UUID,
	reason string,
) error {
	if repository == nil || repository.pool == nil {
		return errors.New("StageScheduler repository is not configured")
	}
	if claimID == uuid.Nil || reason == "" {
		return errors.New("StageScheduler abandon authority is required")
	}
	var abandonedID uuid.UUID
	var replayed bool
	if err := repository.pool.QueryRow(ctx, `
		SELECT claim_id, replayed
		FROM vela_abandon_stage_scheduler_claim($1, $2)
	`, claimID, reason).Scan(&abandonedID, &replayed); err != nil {
		return fmt.Errorf("abandon StageScheduler claim: %w", err)
	}
	if abandonedID != claimID {
		return errors.New("StageScheduler abandoned claim identity changed")
	}
	return nil
}

func (repository *PostgresRepository) ReconcileExpired(
	ctx context.Context,
	limit int,
) (int64, error) {
	if repository == nil || repository.pool == nil {
		return 0, errors.New("StageScheduler repository is not configured")
	}
	if limit < 1 || limit > 1000 {
		return 0, errors.New("StageScheduler reconcile limit is invalid")
	}
	var processed int64
	if err := repository.pool.QueryRow(ctx, `
		SELECT processed
		FROM vela_reconcile_expired_stage_scheduler_claims($1)
	`, limit).Scan(&processed); err != nil {
		return 0, fmt.Errorf("reconcile StageScheduler claims: %w", err)
	}
	return processed, nil
}

func encodeClaimRequest(request ClaimRequest) ([]byte, error) {
	if request.ClaimID == uuid.Nil || request.DecisionID == uuid.Nil ||
		request.CapturedSnapshotID == uuid.Nil || request.SchedulerID == "" ||
		request.ClaimExpiresAt.IsZero() {
		return nil, errors.New("StageScheduler claim identity is incomplete")
	}
	command := request.Command
	payload := map[string]any{
		"schema_version":       1,
		"claim_id":             request.ClaimID,
		"decision_id":          request.DecisionID,
		"captured_snapshot_id": request.CapturedSnapshotID,
		"scheduler_id":         request.SchedulerID,
		"claim_expires_at":     request.ClaimExpiresAt.UTC().Format(time.RFC3339Nano),
		"evidence": map[string]any{
			"algorithm_revision":                 request.Evidence.AlgorithmRevision,
			"input_digest":                       hex.EncodeToString(request.Evidence.InputDigest[:]),
			"evidence_digest":                    hex.EncodeToString(request.Evidence.EvidenceDigest[:]),
			"capacity_pool_id":                   request.Evidence.CapacityPoolID,
			"worker_instance_id":                 request.Evidence.WorkerInstanceID,
			"selected_stage_run_id":              request.Evidence.SelectedStageRunID,
			"selected_attempt_id":                request.Evidence.SelectedAttemptID,
			"selected_stage_profile_revision_id": request.Evidence.SelectedStageProfileRevisionID,
			"organization_id":                    request.Evidence.OrganizationID,
			"service_class_revision_id":          request.Evidence.ServiceClassRevisionID,
			"project_id":                         request.Evidence.ProjectID,
			"attempt_fence":                      request.Evidence.AttemptFence,
			"stage_fence":                        request.Evidence.StageFence,
			"stage_version":                      request.Evidence.StageVersion,
			"lane":                               request.Evidence.Lane,
			"resource_millis":                    request.Evidence.ResourceMillis,
			"organization_deficit_millis":        request.Evidence.OrganizationDeficitMillis,
			"service_class_deficit_millis":       request.Evidence.ServiceClassDeficitMillis,
			"project_deficit_millis":             request.Evidence.ProjectDeficitMillis,
			"score":                              request.Evidence.Score,
			"score_total_millis":                 request.Evidence.ScoreTotalMillis,
			"filter_reason_counts":               request.Evidence.FilterReasonCounts,
			"tie_break":                          request.Evidence.TieBreak,
		},
		"command": encodeAssignmentCommand(command),
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode StageScheduler claim: %w", err)
	}
	return encoded, nil
}

func encodeAssignmentCommand(command attemptcoordinator.AssignStageCommand) map[string]any {
	return map[string]any{
		"schema_version":            1,
		"command_kind":              "ASSIGN",
		"command_id":                command.CommandID,
		"attempt_id":                command.AttemptID,
		"stage_run_id":              command.StageRunID,
		"expected_attempt_fence":    command.ExpectedAttemptFence,
		"expected_stage_fence":      command.ExpectedStageFence,
		"expected_stage_version":    command.ExpectedStageVersion,
		"stage_attempt_id":          command.StageAttemptID,
		"stage_allocation_id":       command.StageAllocationID,
		"stage_lease_id":            command.StageLeaseID,
		"stage_profile_revision_id": command.StageProfileRevisionID,
		"capacity_pool_id":          command.CapacityPoolID,
		"worker_instance_id":        command.WorkerInstanceID,
		"worker_instance_epoch":     command.WorkerInstanceEpoch,
		"device_set_digest":         hex.EncodeToString(command.DeviceSetDigest),
		"membership_digest":         hex.EncodeToString(command.MembershipDigest),
		"model_residency_id":        command.ModelResidencyID,
		"model_runtime_epoch":       command.ModelRuntimeEpoch,
		"capacity_vector":           command.CapacityVector,
		"token_digest":              hex.EncodeToString(command.TokenDigest),
		"signing_key_id":            command.SigningKeyID,
		"execution_nonce":           hex.EncodeToString(command.ExecutionNonce),
		"issued_at":                 command.IssuedAt.UTC().Format(time.RFC3339Nano),
		"expires_at":                command.ExpiresAt.UTC().Format(time.RFC3339Nano),
		"local_deadline_at":         command.LocalDeadlineAt.UTC().Format(time.RFC3339Nano),
	}
}
