package stagescheduler

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

func (repository *PostgresRepository) Recover(
	ctx context.Context,
	claimID uuid.UUID,
) (RecoveredClaim, bool, error) {
	if repository == nil || repository.pool == nil {
		return RecoveredClaim{}, false, errors.New("StageScheduler repository is not configured")
	}
	if claimID == uuid.Nil {
		return RecoveredClaim{}, false, errors.New("StageScheduler recovery claim identity is required")
	}
	var recovered RecoveredClaim
	var returnedClaimID, decisionID uuid.UUID
	var encoded []byte
	err := repository.pool.QueryRow(ctx, `
		SELECT claim_id, decision_id, claim_state, command_payload
		FROM vela_read_stage_scheduler_claim($1)
	`, claimID).Scan(&returnedClaimID, &decisionID, &recovered.State, &encoded)
	if errors.Is(err, pgx.ErrNoRows) {
		return RecoveredClaim{}, false, nil
	}
	if err != nil {
		return RecoveredClaim{}, false, fmt.Errorf("read StageScheduler claim recovery: %w", err)
	}
	if returnedClaimID != claimID || decisionID == uuid.Nil {
		return RecoveredClaim{}, false, errors.New("StageScheduler recovered claim identity changed")
	}
	command, err := decodeAssignmentCommand(encoded)
	if err != nil {
		return RecoveredClaim{}, false, err
	}
	recovered.Command = command
	recovered.Assignment = Assignment{
		ClaimID: returnedClaimID, DecisionID: decisionID,
		StageRunID: command.StageRunID, StageAttemptID: command.StageAttemptID,
		StageAllocationID: command.StageAllocationID, StageLeaseID: command.StageLeaseID,
		LeaseExpiresAt: command.ExpiresAt, LocalDeadlineAt: command.LocalDeadlineAt,
	}
	return recovered, true, nil
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

func (repository *PostgresRepository) ListShadowSnapshots(
	ctx context.Context,
	limit int,
) ([]ShadowSnapshot, error) {
	if repository == nil || repository.pool == nil {
		return nil, errors.New("StageScheduler repository is not configured")
	}
	if limit < 1 || limit > 1000 {
		return nil, errors.New("StageScheduler shadow replay limit is invalid")
	}
	rows, err := repository.pool.Query(ctx, `
		SELECT snapshot_id, snapshot, expected_evidence_digest
		FROM vela_list_stage_scheduler_shadow_snapshots($1)
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list StageScheduler shadow snapshots: %w", err)
	}
	defer rows.Close()

	snapshots := make([]ShadowSnapshot, 0, limit)
	for rows.Next() {
		var snapshot ShadowSnapshot
		var encodedSnapshot, expectedDigest []byte
		if err := rows.Scan(&snapshot.ID, &encodedSnapshot, &expectedDigest); err != nil {
			return nil, fmt.Errorf("scan StageScheduler shadow snapshot: %w", err)
		}
		if len(expectedDigest) != len(snapshot.ExpectedEvidenceDigest) {
			return nil, errors.New("StageScheduler shadow evidence digest is invalid")
		}
		copy(snapshot.ExpectedEvidenceDigest[:], expectedDigest)
		if err := json.Unmarshal(encodedSnapshot, &snapshot.Snapshot); err != nil {
			return nil, fmt.Errorf("decode StageScheduler shadow snapshot: %w", err)
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate StageScheduler shadow snapshots: %w", err)
	}
	return snapshots, nil
}

func (repository *PostgresRepository) RecordShadowReplay(
	ctx context.Context,
	receipt ShadowReplayReceipt,
) error {
	if repository == nil || repository.pool == nil {
		return errors.New("StageScheduler repository is not configured")
	}
	if receipt.ID == uuid.Nil || receipt.SnapshotID == uuid.Nil ||
		receipt.AlgorithmRevision == "" || receipt.ReplayedAt.IsZero() ||
		receipt.ReplayedBy == "" {
		return errors.New("StageScheduler shadow replay receipt is incomplete")
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version":           1,
		"receipt_id":               receipt.ID,
		"snapshot_id":              receipt.SnapshotID,
		"algorithm_revision":       receipt.AlgorithmRevision,
		"expected_evidence_digest": hex.EncodeToString(receipt.ExpectedEvidenceDigest[:]),
		"replayed_evidence_digest": hex.EncodeToString(receipt.ReplayedEvidenceDigest[:]),
		"matched":                  receipt.Matched,
		"replayed_at":              receipt.ReplayedAt.UTC().Format(time.RFC3339Nano),
		"replayed_by":              receipt.ReplayedBy,
	})
	if err != nil {
		return fmt.Errorf("encode StageScheduler shadow replay receipt: %w", err)
	}
	var receiptID uuid.UUID
	var replayed bool
	if err := repository.pool.QueryRow(ctx, `
		SELECT receipt_id, replayed
		FROM vela_record_stage_scheduler_shadow_replay($1::jsonb)
	`, payload).Scan(&receiptID, &replayed); err != nil {
		return fmt.Errorf("record StageScheduler shadow replay receipt: %w", err)
	}
	if receiptID != receipt.ID {
		return errors.New("StageScheduler shadow replay receipt identity changed")
	}
	return nil
}

func encodeClaimRequest(request ClaimRequest) ([]byte, error) {
	if request.ClaimID == uuid.Nil || request.DecisionID == uuid.Nil ||
		request.CapturedSnapshotID == uuid.Nil || request.SchedulerID == "" ||
		request.ClaimExpiresAt.IsZero() {
		return nil, errors.New("StageScheduler claim identity is incomplete")
	}
	if len(request.Evidence.inputPayload) == 0 || len(request.Evidence.evidencePayload) == 0 {
		return nil, errors.New("StageScheduler digest payloads are missing")
	}
	command := request.Command
	evidence := decisionEvidenceDigestPayload(request.Evidence)
	evidence["evidence_digest"] = hex.EncodeToString(request.Evidence.EvidenceDigest[:])
	evidence["input_payload"] = hex.EncodeToString(request.Evidence.inputPayload)
	evidence["evidence_payload"] = hex.EncodeToString(request.Evidence.evidencePayload)
	payload := map[string]any{
		"schema_version":       1,
		"claim_id":             request.ClaimID,
		"decision_id":          request.DecisionID,
		"captured_snapshot_id": request.CapturedSnapshotID,
		"scheduler_id":         request.SchedulerID,
		"claim_expires_at":     request.ClaimExpiresAt.UTC().Format(time.RFC3339Nano),
		"evidence":             evidence,
		"command":              encodeAssignmentCommand(command),
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode StageScheduler claim: %w", err)
	}
	return encoded, nil
}

func encodeAssignmentCommand(command attemptcoordinator.AssignStageCommand) map[string]any {
	return map[string]any{
		"schema_version":                1,
		"command_kind":                  "ASSIGN",
		"command_id":                    command.CommandID,
		"attempt_id":                    command.AttemptID,
		"stage_run_id":                  command.StageRunID,
		"expected_attempt_fence":        command.ExpectedAttemptFence,
		"expected_stage_fence":          command.ExpectedStageFence,
		"expected_stage_version":        command.ExpectedStageVersion,
		"stage_attempt_id":              command.StageAttemptID,
		"stage_allocation_id":           command.StageAllocationID,
		"stage_lease_id":                command.StageLeaseID,
		"stage_profile_revision_id":     command.StageProfileRevisionID,
		"capacity_pool_id":              command.CapacityPoolID,
		"worker_instance_id":            command.WorkerInstanceID,
		"worker_instance_epoch":         command.WorkerInstanceEpoch,
		"capacity_observation_sequence": command.ObservationSequence,
		"device_set_digest":             hex.EncodeToString(command.DeviceSetDigest),
		"membership_digest":             hex.EncodeToString(command.MembershipDigest),
		"model_residency_id":            command.ModelResidencyID,
		"model_runtime_epoch":           command.ModelRuntimeEpoch,
		"capacity_vector":               command.CapacityVector,
		"token_digest":                  hex.EncodeToString(command.TokenDigest),
		"signing_key_id":                command.SigningKeyID,
		"execution_nonce":               hex.EncodeToString(command.ExecutionNonce),
		"issued_at":                     command.IssuedAt.UTC().Format(time.RFC3339Nano),
		"expires_at":                    command.ExpiresAt.UTC().Format(time.RFC3339Nano),
		"local_deadline_at":             command.LocalDeadlineAt.UTC().Format(time.RFC3339Nano),
	}
}

type persistedAssignmentCommand struct {
	SchemaVersion          int              `json:"schema_version"`
	CommandKind            string           `json:"command_kind"`
	CommandID              uuid.UUID        `json:"command_id"`
	AttemptID              uuid.UUID        `json:"attempt_id"`
	StageRunID             uuid.UUID        `json:"stage_run_id"`
	ExpectedAttemptFence   int64            `json:"expected_attempt_fence"`
	ExpectedStageFence     int64            `json:"expected_stage_fence"`
	ExpectedStageVersion   int64            `json:"expected_stage_version"`
	StageAttemptID         uuid.UUID        `json:"stage_attempt_id"`
	StageAllocationID      uuid.UUID        `json:"stage_allocation_id"`
	StageLeaseID           uuid.UUID        `json:"stage_lease_id"`
	StageProfileRevisionID uuid.UUID        `json:"stage_profile_revision_id"`
	CapacityPoolID         uuid.UUID        `json:"capacity_pool_id"`
	WorkerInstanceID       uuid.UUID        `json:"worker_instance_id"`
	WorkerInstanceEpoch    int64            `json:"worker_instance_epoch"`
	ObservationSequence    int64            `json:"capacity_observation_sequence"`
	DeviceSetDigest        string           `json:"device_set_digest"`
	MembershipDigest       string           `json:"membership_digest"`
	ModelResidencyID       uuid.UUID        `json:"model_residency_id"`
	ModelRuntimeEpoch      int64            `json:"model_runtime_epoch"`
	CapacityVector         map[string]int64 `json:"capacity_vector"`
	TokenDigest            string           `json:"token_digest"`
	SigningKeyID           string           `json:"signing_key_id"`
	ExecutionNonce         string           `json:"execution_nonce"`
	IssuedAt               time.Time        `json:"issued_at"`
	ExpiresAt              time.Time        `json:"expires_at"`
	LocalDeadlineAt        time.Time        `json:"local_deadline_at"`
}

func decodeAssignmentCommand(encoded []byte) (attemptcoordinator.AssignStageCommand, error) {
	var persisted persistedAssignmentCommand
	if err := json.Unmarshal(encoded, &persisted); err != nil {
		return attemptcoordinator.AssignStageCommand{}, fmt.Errorf(
			"decode recovered StageScheduler assignment: %w", err,
		)
	}
	deviceSetDigest, err := hex.DecodeString(persisted.DeviceSetDigest)
	if err != nil {
		return attemptcoordinator.AssignStageCommand{}, errors.New(
			"recovered StageScheduler device digest is malformed",
		)
	}
	membershipDigest, err := hex.DecodeString(persisted.MembershipDigest)
	if err != nil {
		return attemptcoordinator.AssignStageCommand{}, errors.New(
			"recovered StageScheduler membership digest is malformed",
		)
	}
	tokenDigest, err := hex.DecodeString(persisted.TokenDigest)
	if err != nil {
		return attemptcoordinator.AssignStageCommand{}, errors.New(
			"recovered StageScheduler token digest is malformed",
		)
	}
	executionNonce, err := hex.DecodeString(persisted.ExecutionNonce)
	if err != nil {
		return attemptcoordinator.AssignStageCommand{}, errors.New(
			"recovered StageScheduler execution nonce is malformed",
		)
	}
	command := attemptcoordinator.AssignStageCommand{
		CommandID: persisted.CommandID, AttemptID: persisted.AttemptID,
		StageRunID: persisted.StageRunID, ExpectedAttemptFence: persisted.ExpectedAttemptFence,
		ExpectedStageFence:   persisted.ExpectedStageFence,
		ExpectedStageVersion: persisted.ExpectedStageVersion,
		StageAttemptID:       persisted.StageAttemptID,
		StageAllocationID:    persisted.StageAllocationID, StageLeaseID: persisted.StageLeaseID,
		StageProfileRevisionID: persisted.StageProfileRevisionID,
		CapacityPoolID:         persisted.CapacityPoolID, WorkerInstanceID: persisted.WorkerInstanceID,
		WorkerInstanceEpoch: persisted.WorkerInstanceEpoch,
		ObservationSequence: persisted.ObservationSequence,
		DeviceSetDigest:     deviceSetDigest, MembershipDigest: membershipDigest,
		ModelResidencyID:  persisted.ModelResidencyID,
		ModelRuntimeEpoch: persisted.ModelRuntimeEpoch,
		CapacityVector:    persisted.CapacityVector, TokenDigest: tokenDigest,
		SigningKeyID: persisted.SigningKeyID, ExecutionNonce: executionNonce,
		IssuedAt: persisted.IssuedAt, ExpiresAt: persisted.ExpiresAt,
		LocalDeadlineAt: persisted.LocalDeadlineAt,
	}
	if persisted.SchemaVersion != 1 || persisted.CommandKind != "ASSIGN" ||
		validateRecoveredAssignment(command) != nil {
		return attemptcoordinator.AssignStageCommand{}, errors.New(
			"recovered StageScheduler assignment is invalid",
		)
	}
	return command, nil
}

func validateRecoveredAssignment(command attemptcoordinator.AssignStageCommand) error {
	if command.CommandID == uuid.Nil || command.AttemptID == uuid.Nil ||
		command.StageRunID == uuid.Nil || command.StageAttemptID == uuid.Nil ||
		command.StageAllocationID == uuid.Nil || command.StageLeaseID == uuid.Nil ||
		command.StageProfileRevisionID == uuid.Nil || command.CapacityPoolID == uuid.Nil ||
		command.WorkerInstanceID == uuid.Nil || command.ModelResidencyID == uuid.Nil ||
		command.ExpectedAttemptFence <= 0 || command.ExpectedStageFence <= 0 ||
		command.ExpectedStageVersion <= 0 || command.WorkerInstanceEpoch <= 0 ||
		command.ObservationSequence <= 0 || command.ModelRuntimeEpoch <= 0 ||
		len(command.DeviceSetDigest) != 32 || len(command.MembershipDigest) != 32 ||
		len(command.TokenDigest) != 32 || len(command.ExecutionNonce) != 32 ||
		len(command.CapacityVector) == 0 || command.SigningKeyID == "" ||
		command.IssuedAt.IsZero() || !command.ExpiresAt.After(command.IssuedAt) ||
		!command.LocalDeadlineAt.After(command.IssuedAt) ||
		command.LocalDeadlineAt.After(command.ExpiresAt) {
		return errors.New("incomplete recovered assignment")
	}
	return nil
}
