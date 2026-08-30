package attemptcoordinator

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type InstantiateCommand struct {
	CommandID                  uuid.UUID `json:"command_id"`
	JobID                      uuid.UUID `json:"job_id"`
	ExpectedJobVersion         int64     `json:"expected_job_version"`
	ExpectedJobFence           int64     `json:"expected_job_fence"`
	ExecutionGraphSnapshotID   uuid.UUID `json:"execution_graph_snapshot_id"`
	ExecutionGraphRevisionID   uuid.UUID `json:"execution_graph_revision_id"`
	ExecutionProfileRevisionID uuid.UUID `json:"execution_profile_revision_id"`
	AttemptID                  uuid.UUID `json:"attempt_id"`
	StorageReservationID       uuid.UUID `json:"storage_reservation_id"`
	ReservedStorageBytes       int64     `json:"reserved_storage_bytes"`
}

type AttemptHandle struct {
	SnapshotID    uuid.UUID
	AttemptID     uuid.UUID
	AttemptFence  int64
	StageRunCount int
	Replayed      bool
}

type StageCommand interface {
	stageCommand()
}

type AssignStageCommand struct {
	CommandID              uuid.UUID
	AttemptID              uuid.UUID
	StageRunID             uuid.UUID
	ExpectedAttemptFence   int64
	ExpectedStageFence     int64
	ExpectedStageVersion   int64
	StageAttemptID         uuid.UUID
	StageAllocationID      uuid.UUID
	StageLeaseID           uuid.UUID
	StageProfileRevisionID uuid.UUID
	CapacityPoolID         uuid.UUID
	WorkerInstanceID       uuid.UUID
	WorkerInstanceEpoch    int64
	DeviceSetDigest        []byte
	MembershipDigest       []byte
	ModelResidencyID       uuid.UUID
	ModelRuntimeEpoch      int64
	CapacityVector         map[string]int64
	TokenDigest            []byte
	SigningKeyID           string
	ExecutionNonce         []byte
	IssuedAt               time.Time
	ExpiresAt              time.Time
	LocalDeadlineAt        time.Time
}

func (AssignStageCommand) stageCommand() {}

type StartStageCommand struct {
	CommandID            uuid.UUID
	AttemptID            uuid.UUID
	StageRunID           uuid.UUID
	StageAttemptID       uuid.UUID
	StageLeaseID         uuid.UUID
	ExpectedAttemptFence int64
	ExpectedStageFence   int64
	ExpectedStageVersion int64
	StartedAt            time.Time
}

func (StartStageCommand) stageCommand() {}

type ExactCacheAdvanceCommand struct {
	CommandID            uuid.UUID
	AttemptID            uuid.UUID
	StageRunID           uuid.UUID
	ExpectedAttemptFence int64
	ExpectedStageFence   int64
	ExpectedStageVersion int64
	ProgressReceiptID    uuid.UUID
	CacheSourceIdentity  string
	OutputDigest         []byte
	AdvancedAt           time.Time
}

func (ExactCacheAdvanceCommand) stageCommand() {}

type CompleteStageCommand struct {
	CommandID            uuid.UUID
	AttemptID            uuid.UUID
	StageRunID           uuid.UUID
	StageAttemptID       uuid.UUID
	StageLeaseID         uuid.UUID
	ExpectedAttemptFence int64
	ExpectedStageFence   int64
	ExpectedStageVersion int64
	ProgressReceiptID    uuid.UUID
	OutputIdentity       string
	OutputDigest         []byte
	CompletedAt          time.Time
}

func (CompleteStageCommand) stageCommand() {}

type FailStageCommand struct {
	CommandID             uuid.UUID
	AttemptID             uuid.UUID
	StageRunID            uuid.UUID
	StageAttemptID        uuid.UUID
	StageLeaseID          uuid.UUID
	ExpectedAttemptFence  int64
	ExpectedStageFence    int64
	ExpectedStageVersion  int64
	FailureClass          string
	FailureFingerprint    []byte
	ConsumedResourceUnits int64
	FailedAt              time.Time
	RetryAt               time.Time
}

func (FailStageCommand) stageCommand() {}

type StageDecision struct {
	StageRunID     uuid.UUID
	StageAttemptID uuid.UUID
	State          string
	StageFence     int64
	StageVersion   int64
	Replayed       bool
}

type ReconcileDecision struct {
	AttemptID    uuid.UUID
	StageRunID   uuid.UUID
	State        string
	StageFence   int64
	StageVersion int64
	Reason       string
}

type Service struct {
	pool             *pgxpool.Pool
	cancellationPool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool, cancellationPools ...*pgxpool.Pool) (*Service, error) {
	if pool == nil {
		return nil, errors.New("AttemptCoordinator database pool is required")
	}
	if len(cancellationPools) > 1 ||
		(len(cancellationPools) == 1 && cancellationPools[0] == nil) {
		return nil, errors.New("AttemptCoordinator cancellation pool is invalid")
	}
	service := &Service{pool: pool}
	if len(cancellationPools) == 1 {
		service.cancellationPool = cancellationPools[0]
	}
	return service, nil
}

func NewCancellationService(pool *pgxpool.Pool) (*Service, error) {
	if pool == nil {
		return nil, errors.New("AttemptCoordinator cancellation pool is required")
	}
	return &Service{cancellationPool: pool}, nil
}

func (service *Service) Instantiate(
	ctx context.Context,
	command InstantiateCommand,
) (AttemptHandle, error) {
	if service == nil || service.pool == nil {
		return AttemptHandle{}, errors.New("AttemptCoordinator is not configured")
	}
	if err := validateInstantiate(command); err != nil {
		return AttemptHandle{}, err
	}
	payload, err := json.Marshal(struct {
		SchemaVersion int `json:"schema_version"`
		InstantiateCommand
	}{SchemaVersion: 1, InstantiateCommand: command})
	if err != nil {
		return AttemptHandle{}, fmt.Errorf("encode graph instantiation command: %w", err)
	}
	var result AttemptHandle
	err = service.pool.QueryRow(ctx, `
		SELECT snapshot_id, attempt_id, attempt_fence, stage_run_count, replayed
		FROM vela_instantiate_stage_graph($1::jsonb)
	`, payload).Scan(
		&result.SnapshotID,
		&result.AttemptID,
		&result.AttemptFence,
		&result.StageRunCount,
		&result.Replayed,
	)
	if err != nil {
		return AttemptHandle{}, fmt.Errorf("instantiate Stage graph: %w", err)
	}
	return result, nil
}

func (service *Service) Apply(
	ctx context.Context,
	command StageCommand,
) (StageDecision, error) {
	if service == nil || service.pool == nil {
		return StageDecision{}, errors.New("AttemptCoordinator is not configured")
	}
	var payload []byte
	var err error
	switch typed := command.(type) {
	case AssignStageCommand:
		payload, err = encodeAssign(typed)
	case *AssignStageCommand:
		if typed == nil {
			return StageDecision{}, errors.New("AttemptCoordinator Stage command is required")
		}
		payload, err = encodeAssign(*typed)
	case StartStageCommand:
		payload, err = encodeStart(typed)
	case *StartStageCommand:
		if typed == nil {
			return StageDecision{}, errors.New("AttemptCoordinator Stage command is required")
		}
		payload, err = encodeStart(*typed)
	case ExactCacheAdvanceCommand:
		payload, err = encodeExactCacheAdvance(typed)
	case *ExactCacheAdvanceCommand:
		if typed == nil {
			return StageDecision{}, errors.New("AttemptCoordinator Stage command is required")
		}
		payload, err = encodeExactCacheAdvance(*typed)
	case CompleteStageCommand:
		payload, err = encodeComplete(typed)
	case *CompleteStageCommand:
		if typed == nil {
			return StageDecision{}, errors.New("AttemptCoordinator Stage command is required")
		}
		payload, err = encodeComplete(*typed)
	case FailStageCommand:
		payload, err = encodeFail(typed)
	case *FailStageCommand:
		if typed == nil {
			return StageDecision{}, errors.New("AttemptCoordinator Stage command is required")
		}
		payload, err = encodeFail(*typed)
	default:
		return StageDecision{}, errors.New("AttemptCoordinator Stage command kind is unsupported")
	}
	if err != nil {
		return StageDecision{}, err
	}
	var result StageDecision
	err = service.pool.QueryRow(ctx, `
		SELECT stage_run_id, stage_attempt_id, stage_state, stage_fence,
		       stage_version, replayed
		FROM vela_apply_stage_command($1::jsonb)
	`, payload).Scan(
		&result.StageRunID,
		&result.StageAttemptID,
		&result.State,
		&result.StageFence,
		&result.StageVersion,
		&result.Replayed,
	)
	if err != nil {
		return StageDecision{}, fmt.Errorf("apply Stage command: %w", err)
	}
	return result, nil
}

func (service *Service) Reconcile(
	ctx context.Context,
	limit int,
) ([]ReconcileDecision, error) {
	if service == nil || service.pool == nil {
		return nil, errors.New("AttemptCoordinator is not configured")
	}
	if limit < 1 || limit > 1000 {
		return nil, errors.New("AttemptCoordinator reconcile limit must be between 1 and 1000")
	}
	rows, err := service.pool.Query(ctx, `
		SELECT attempt_id, stage_run_id, stage_state, stage_fence,
		       stage_version, reason
		FROM vela_reconcile_stage_graphs($1)
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("reconcile Stage graphs: %w", err)
	}
	defer rows.Close()
	decisions := make([]ReconcileDecision, 0)
	for rows.Next() {
		var decision ReconcileDecision
		if err := rows.Scan(
			&decision.AttemptID,
			&decision.StageRunID,
			&decision.State,
			&decision.StageFence,
			&decision.StageVersion,
			&decision.Reason,
		); err != nil {
			return nil, fmt.Errorf("scan reconciled Stage graph: %w", err)
		}
		decisions = append(decisions, decision)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read reconciled Stage graphs: %w", err)
	}
	return decisions, nil
}

func encodeFail(command FailStageCommand) ([]byte, error) {
	if command.CommandID == uuid.Nil || command.AttemptID == uuid.Nil ||
		command.StageRunID == uuid.Nil || command.StageAttemptID == uuid.Nil ||
		command.StageLeaseID == uuid.Nil {
		return nil, errors.New("AttemptCoordinator failure identities are required")
	}
	if command.ExpectedAttemptFence <= 0 || command.ExpectedStageFence <= 0 ||
		command.ExpectedStageVersion <= 0 || command.ConsumedResourceUnits <= 0 ||
		command.FailedAt.IsZero() || !command.RetryAt.After(command.FailedAt) ||
		len(command.FailureFingerprint) != 32 || command.FailureClass == "" ||
		len(command.FailureClass) > 100 {
		return nil, errors.New("AttemptCoordinator failure authority is invalid")
	}
	return json.Marshal(map[string]any{
		"schema_version":          1,
		"command_kind":            "FAIL",
		"command_id":              command.CommandID,
		"attempt_id":              command.AttemptID,
		"stage_run_id":            command.StageRunID,
		"stage_attempt_id":        command.StageAttemptID,
		"stage_lease_id":          command.StageLeaseID,
		"expected_attempt_fence":  command.ExpectedAttemptFence,
		"expected_stage_fence":    command.ExpectedStageFence,
		"expected_stage_version":  command.ExpectedStageVersion,
		"failure_class":           command.FailureClass,
		"failure_fingerprint":     hex.EncodeToString(command.FailureFingerprint),
		"consumed_resource_units": command.ConsumedResourceUnits,
		"failed_at":               command.FailedAt.UTC().Format(time.RFC3339Nano),
		"retry_at":                command.RetryAt.UTC().Format(time.RFC3339Nano),
	})
}

func encodeComplete(command CompleteStageCommand) ([]byte, error) {
	if command.CommandID == uuid.Nil || command.AttemptID == uuid.Nil ||
		command.StageRunID == uuid.Nil || command.StageAttemptID == uuid.Nil ||
		command.StageLeaseID == uuid.Nil || command.ProgressReceiptID == uuid.Nil {
		return nil, errors.New("AttemptCoordinator completion identities are required")
	}
	if command.ExpectedAttemptFence <= 0 || command.ExpectedStageFence <= 0 ||
		command.ExpectedStageVersion <= 0 || command.CompletedAt.IsZero() ||
		len(command.OutputDigest) != 32 || command.OutputIdentity == "" ||
		len(command.OutputIdentity) > 1000 {
		return nil, errors.New("AttemptCoordinator completion authority is invalid")
	}
	return json.Marshal(map[string]any{
		"schema_version":         1,
		"command_kind":           "COMPLETE",
		"command_id":             command.CommandID,
		"attempt_id":             command.AttemptID,
		"stage_run_id":           command.StageRunID,
		"stage_attempt_id":       command.StageAttemptID,
		"stage_lease_id":         command.StageLeaseID,
		"expected_attempt_fence": command.ExpectedAttemptFence,
		"expected_stage_fence":   command.ExpectedStageFence,
		"expected_stage_version": command.ExpectedStageVersion,
		"progress_receipt_id":    command.ProgressReceiptID,
		"output_identity":        command.OutputIdentity,
		"output_digest":          hex.EncodeToString(command.OutputDigest),
		"completed_at":           command.CompletedAt.UTC().Format(time.RFC3339Nano),
	})
}

func encodeExactCacheAdvance(command ExactCacheAdvanceCommand) ([]byte, error) {
	if command.CommandID == uuid.Nil || command.AttemptID == uuid.Nil ||
		command.StageRunID == uuid.Nil || command.ProgressReceiptID == uuid.Nil {
		return nil, errors.New("AttemptCoordinator exact cache identities are required")
	}
	if command.ExpectedAttemptFence <= 0 || command.ExpectedStageFence <= 0 ||
		command.ExpectedStageVersion <= 0 || command.AdvancedAt.IsZero() ||
		len(command.OutputDigest) != 32 || command.CacheSourceIdentity == "" ||
		len(command.CacheSourceIdentity) > 1000 {
		return nil, errors.New("AttemptCoordinator exact cache authority is invalid")
	}
	return json.Marshal(map[string]any{
		"schema_version":         1,
		"command_kind":           "EXACT_CACHE_ADVANCE",
		"command_id":             command.CommandID,
		"attempt_id":             command.AttemptID,
		"stage_run_id":           command.StageRunID,
		"expected_attempt_fence": command.ExpectedAttemptFence,
		"expected_stage_fence":   command.ExpectedStageFence,
		"expected_stage_version": command.ExpectedStageVersion,
		"progress_receipt_id":    command.ProgressReceiptID,
		"cache_source_identity":  command.CacheSourceIdentity,
		"output_digest":          hex.EncodeToString(command.OutputDigest),
		"advanced_at":            command.AdvancedAt.UTC().Format(time.RFC3339Nano),
	})
}

func encodeStart(command StartStageCommand) ([]byte, error) {
	if command.CommandID == uuid.Nil || command.AttemptID == uuid.Nil ||
		command.StageRunID == uuid.Nil || command.StageAttemptID == uuid.Nil ||
		command.StageLeaseID == uuid.Nil {
		return nil, errors.New("AttemptCoordinator start identities are required")
	}
	if command.ExpectedAttemptFence <= 0 || command.ExpectedStageFence <= 0 ||
		command.ExpectedStageVersion <= 0 || command.StartedAt.IsZero() {
		return nil, errors.New("AttemptCoordinator start authority is invalid")
	}
	return json.Marshal(map[string]any{
		"schema_version":         1,
		"command_kind":           "START",
		"command_id":             command.CommandID,
		"attempt_id":             command.AttemptID,
		"stage_run_id":           command.StageRunID,
		"stage_attempt_id":       command.StageAttemptID,
		"stage_lease_id":         command.StageLeaseID,
		"expected_attempt_fence": command.ExpectedAttemptFence,
		"expected_stage_fence":   command.ExpectedStageFence,
		"expected_stage_version": command.ExpectedStageVersion,
		"started_at":             command.StartedAt.UTC().Format(time.RFC3339Nano),
	})
}

func encodeAssign(command AssignStageCommand) ([]byte, error) {
	if command.CommandID == uuid.Nil || command.AttemptID == uuid.Nil ||
		command.StageRunID == uuid.Nil || command.StageAttemptID == uuid.Nil ||
		command.StageAllocationID == uuid.Nil || command.StageLeaseID == uuid.Nil ||
		command.StageProfileRevisionID == uuid.Nil || command.CapacityPoolID == uuid.Nil ||
		command.WorkerInstanceID == uuid.Nil || command.ModelResidencyID == uuid.Nil {
		return nil, errors.New("AttemptCoordinator assignment identities are required")
	}
	if command.ExpectedAttemptFence <= 0 || command.ExpectedStageFence <= 0 ||
		command.ExpectedStageVersion <= 0 || command.WorkerInstanceEpoch <= 0 ||
		command.ModelRuntimeEpoch <= 0 {
		return nil, errors.New("AttemptCoordinator assignment fences or epochs are invalid")
	}
	if len(command.DeviceSetDigest) != 32 || len(command.MembershipDigest) != 32 ||
		len(command.TokenDigest) != 32 || len(command.ExecutionNonce) != 32 ||
		len(command.CapacityVector) == 0 || command.SigningKeyID == "" {
		return nil, errors.New("AttemptCoordinator assignment authority is incomplete")
	}
	if command.IssuedAt.IsZero() || !command.ExpiresAt.After(command.IssuedAt) ||
		!command.LocalDeadlineAt.After(command.IssuedAt) ||
		command.LocalDeadlineAt.After(command.ExpiresAt) {
		return nil, errors.New("AttemptCoordinator assignment deadline is invalid")
	}
	return json.Marshal(map[string]any{
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
	})
}

func validateInstantiate(command InstantiateCommand) error {
	if command.CommandID == uuid.Nil || command.JobID == uuid.Nil ||
		command.ExecutionGraphSnapshotID == uuid.Nil ||
		command.ExecutionGraphRevisionID == uuid.Nil ||
		command.ExecutionProfileRevisionID == uuid.Nil || command.AttemptID == uuid.Nil ||
		command.StorageReservationID == uuid.Nil {
		return errors.New("AttemptCoordinator instantiation identities are required")
	}
	if command.ExpectedJobVersion <= 0 || command.ExpectedJobFence < 0 {
		return errors.New("AttemptCoordinator expected Job version or fence is invalid")
	}
	if command.ReservedStorageBytes <= 0 {
		return errors.New("AttemptCoordinator storage reservation must be positive")
	}
	return nil
}
