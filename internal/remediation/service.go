package remediation

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	store "github.com/vivym/vela/internal/store/sqlc"
)

type ActionLevel = store.RemediationActionLevel

const (
	ActionL0ProcessRestart = store.RemediationActionLevelL0PROCESSRESTART
	ActionL1CUDACleanup    = store.RemediationActionLevelL1CUDACLEANUP
	ActionL2GPUReset       = store.RemediationActionLevelL2GPURESET
	ActionL3PCIeFLR        = store.RemediationActionLevelL3PCIEFLR
	ActionL4DriverReload   = store.RemediationActionLevelL4DRIVERRELOAD
	ActionL5NodeReboot     = store.RemediationActionLevelL5NODEREBOOT
	ActionL6BMCPowerCycle  = store.RemediationActionLevelL6BMCPOWERCYCLE
	ActionL7Quarantine     = store.RemediationActionLevelL7QUARANTINE
)

type OperationState = store.RemediationOperationState

const (
	StateRequested        = store.RemediationOperationStateREQUESTED
	StateApprovalRequired = store.RemediationOperationStateAPPROVALREQUIRED
	StateExecuting        = store.RemediationOperationStateEXECUTING
	StateSucceeded        = store.RemediationOperationStateSUCCEEDED
	StateQuarantined      = store.RemediationOperationStateQUARANTINED
)

type FailureCode string

const (
	FailureInvalid     FailureCode = "invalid"
	FailureConflict    FailureCode = "conflict"
	FailureNotFound    FailureCode = "not_found"
	FailureUnavailable FailureCode = "unavailable"
	FailureUncertified FailureCode = "uncertified_action"
	FailureExecution   FailureCode = "execution_failed"
)

type Failure struct {
	Code    FailureCode
	Message string
}

func (e *Failure) Error() string {
	if e == nil {
		return ""
	}
	return string(e.Code) + ": " + e.Message
}

type Request struct {
	OperationID           uuid.UUID
	WorkerID              uuid.UUID
	WorkerEpoch           int64
	NodeIdentity          string
	DeviceIdentity        string
	FailureClass          string
	EvidenceDigest        []byte
	CertificationRevision string
	ActionLevel           ActionLevel
	IdempotencyKey        string
	RequestedBy           string
}

type Result struct {
	OperationID          uuid.UUID
	Replayed             bool
	State                OperationState
	ActionLevel          ActionLevel
	WorkerLifecycleState store.WorkerLifecycleState
	WorkerReachability   store.WorkerReachabilityCondition
	RequiresApproval     bool
	ResultCode           string
}

type ApprovalResult struct {
	OperationID      uuid.UUID
	Replayed         bool
	State            OperationState
	ApprovalCount    int32
	RequiresApproval bool
}

type ClaimResult struct {
	OperationID uuid.UUID
	ClaimID     uuid.UUID
	Replayed    bool
	DeadlineAt  time.Time
}

type Completion struct {
	OperationID   uuid.UUID
	WorkerID      uuid.UUID
	WorkerEpoch   int64
	Success       bool
	ResultCode    string
	ResultDetail  string
	PostcheckHash []byte
	ActorIdentity string
}

type Recovery struct {
	OperationID   uuid.UUID
	ActorIdentity string
}

type Operation struct {
	ID                    uuid.UUID
	WorkerID              uuid.UUID
	WorkerEpoch           int64
	NodeIdentity          string
	DeviceIdentity        string
	FailureClass          string
	EvidenceDigest        []byte
	CertificationRevision string
	ActionLevel           ActionLevel
	IdempotencyKey        string
	RequestedBy           string
	State                 OperationState
	RequestedAt           time.Time
	DeadlineAt            time.Time
	StartedAt             *time.Time
	FinishedAt            *time.Time
	ResultCode            string
	ResultDetail          string
	PostcheckDigest       []byte
	FirstApprover         string
	SecondApprover        string
	ApprovedAt            *time.Time
}

type Service struct {
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) (*Service, error) {
	if pool == nil {
		return nil, errors.New("remediation database pool is required")
	}
	return &Service{pool: pool}, nil
}

func (s *Service) Request(ctx context.Context, request Request) (Result, error) {
	if s == nil || s.pool == nil {
		return Result{}, errors.New("remediation service is not configured")
	}
	if err := validateRequest(request); err != nil {
		return Result{}, &Failure{Code: FailureInvalid, Message: err.Error()}
	}
	var result Result
	err := s.pool.QueryRow(ctx, `
		SELECT operation_id, replayed, state, action_level,
			worker_lifecycle_state, worker_reachability_condition, requires_approval
		FROM vela_request_remediation($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`, request.OperationID, request.WorkerID, request.WorkerEpoch, request.NodeIdentity,
		request.DeviceIdentity, request.FailureClass, request.EvidenceDigest,
		request.CertificationRevision, request.ActionLevel, request.IdempotencyKey,
		request.RequestedBy,
	).Scan(
		&result.OperationID, &result.Replayed, &result.State, &result.ActionLevel,
		&result.WorkerLifecycleState, &result.WorkerReachability, &result.RequiresApproval,
	)
	if err != nil {
		return Result{}, mapDatabaseError("request Remediation", err)
	}
	return result, nil
}

func (s *Service) Approve(ctx context.Context, operationID uuid.UUID, approverIdentity string) (ApprovalResult, error) {
	if s == nil || s.pool == nil {
		return ApprovalResult{}, errors.New("remediation service is not configured")
	}
	if operationID == uuid.Nil || !validText(approverIdentity, 500) {
		return ApprovalResult{}, &Failure{Code: FailureInvalid, Message: "Remediation approval identity is invalid"}
	}
	var result ApprovalResult
	err := s.pool.QueryRow(ctx, `
		SELECT operation_id, replayed, state, approval_count, requires_approval
		FROM vela_approve_remediation($1, $2)
	`, operationID, approverIdentity).Scan(
		&result.OperationID, &result.Replayed, &result.State,
		&result.ApprovalCount, &result.RequiresApproval,
	)
	if err != nil {
		return ApprovalResult{}, mapDatabaseError("approve Remediation", err)
	}
	return result, nil
}

func (s *Service) Start(ctx context.Context, operationID, workerID uuid.UUID, workerEpoch int64, actorIdentity string) (Result, error) {
	if s == nil || s.pool == nil {
		return Result{}, errors.New("remediation service is not configured")
	}
	if operationID == uuid.Nil || workerID == uuid.Nil || workerEpoch <= 0 || !validText(actorIdentity, 500) {
		return Result{}, &Failure{Code: FailureInvalid, Message: "Remediation start identity is invalid"}
	}
	var result Result
	err := s.pool.QueryRow(ctx, `
		SELECT operation_id, replayed, state, action_level,
			worker_lifecycle_state, worker_reachability_condition
		FROM vela_start_remediation($1, $2, $3, $4)
	`, operationID, workerID, workerEpoch, actorIdentity).Scan(
		&result.OperationID, &result.Replayed, &result.State, &result.ActionLevel,
		&result.WorkerLifecycleState, &result.WorkerReachability,
	)
	if err != nil {
		return Result{}, mapDatabaseError("start Remediation", err)
	}
	return result, nil
}

func (s *Service) ClaimExecution(
	ctx context.Context,
	operationID, workerID uuid.UUID,
	workerEpoch int64,
	claimID uuid.UUID,
	actorIdentity string,
) (ClaimResult, error) {
	if s == nil || s.pool == nil {
		return ClaimResult{}, errors.New("remediation service is not configured")
	}
	if operationID == uuid.Nil || workerID == uuid.Nil || workerEpoch <= 0 ||
		claimID == uuid.Nil || !validText(actorIdentity, 500) {
		return ClaimResult{}, &Failure{Code: FailureInvalid, Message: "Remediation execution claim identity is invalid"}
	}
	var result ClaimResult
	err := s.pool.QueryRow(ctx, `
		SELECT operation_id, claim_id, replayed, deadline_at
		FROM vela_claim_remediation_execution($1, $2, $3, $4, $5)
	`, operationID, workerID, workerEpoch, claimID, actorIdentity).Scan(
		&result.OperationID, &result.ClaimID, &result.Replayed, &result.DeadlineAt,
	)
	if err != nil {
		return ClaimResult{}, mapDatabaseError("claim Remediation execution", err)
	}
	return result, nil
}

func (s *Service) Complete(ctx context.Context, completion Completion) (Result, error) {
	if s == nil || s.pool == nil {
		return Result{}, errors.New("remediation service is not configured")
	}
	if err := validateCompletion(completion); err != nil {
		return Result{}, &Failure{Code: FailureInvalid, Message: err.Error()}
	}
	var result Result
	err := s.pool.QueryRow(ctx, `
		SELECT operation_id, replayed, state, action_level,
			worker_lifecycle_state, worker_reachability_condition, result_code
		FROM vela_complete_remediation($1, $2, $3, $4, $5, $6, $7, $8)
	`, completion.OperationID, completion.WorkerID, completion.WorkerEpoch,
		completion.Success, completion.ResultCode, completion.ResultDetail,
		completion.PostcheckHash, completion.ActorIdentity,
	).Scan(
		&result.OperationID, &result.Replayed, &result.State, &result.ActionLevel,
		&result.WorkerLifecycleState, &result.WorkerReachability, &result.ResultCode,
	)
	if err != nil {
		return Result{}, mapDatabaseError("complete Remediation", err)
	}
	return result, nil
}

func (s *Service) Recover(ctx context.Context, recovery Recovery) (Result, error) {
	if s == nil || s.pool == nil {
		return Result{}, errors.New("remediation service is not configured")
	}
	if recovery.OperationID == uuid.Nil || !validText(recovery.ActorIdentity, 500) {
		return Result{}, &Failure{Code: FailureInvalid, Message: "Remediation recovery identity is invalid"}
	}
	var result Result
	err := s.pool.QueryRow(ctx, `
		SELECT operation_id, replayed, state, action_level,
			worker_lifecycle_state, worker_reachability_condition, result_code
		FROM vela_recover_remediation($1, $2)
	`, recovery.OperationID, recovery.ActorIdentity).Scan(
		&result.OperationID, &result.Replayed, &result.State, &result.ActionLevel,
		&result.WorkerLifecycleState, &result.WorkerReachability, &result.ResultCode,
	)
	if err != nil {
		return Result{}, mapDatabaseError("recover Remediation", err)
	}
	return result, nil
}

func (s *Service) Get(ctx context.Context, operationID uuid.UUID) (Operation, error) {
	if s == nil || s.pool == nil {
		return Operation{}, errors.New("remediation service is not configured")
	}
	if operationID == uuid.Nil {
		return Operation{}, &Failure{Code: FailureInvalid, Message: "Remediation operation id is required"}
	}
	var row store.RemediationOperation
	err := s.pool.QueryRow(ctx, `
		SELECT operation_id, worker_id, worker_epoch, node_identity, device_identity,
				failure_class, evidence_digest, certification_revision, action_level,
			idempotency_key, requested_by, state, requested_at, deadline_at, started_at, finished_at,
				result_code, result_detail, postcheck_digest, first_approver, second_approver,
			approved_at
		FROM vela_get_remediation_operation($1)
	`, operationID).Scan(
		&row.ID, &row.WorkerID, &row.WorkerEpoch, &row.NodeIdentity, &row.DeviceIdentity,
		&row.FailureClass, &row.EvidenceDigest, &row.CertificationRevision, &row.ActionLevel,
		&row.IdempotencyKey, &row.RequestedBy, &row.State, &row.RequestedAt, &row.DeadlineAt,
		&row.StartedAt, &row.FinishedAt, &row.ResultCode, &row.ResultDetail, &row.PostcheckDigest,
		&row.FirstApprover, &row.SecondApprover, &row.ApprovedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, &Failure{Code: FailureNotFound, Message: "Remediation operation is not found"}
	}
	if err != nil {
		return Operation{}, mapDatabaseError("get Remediation operation", err)
	}
	return operationFromRow(row), nil
}

func validateRequest(request Request) error {
	if request.OperationID == uuid.Nil || request.WorkerID == uuid.Nil || request.WorkerEpoch <= 0 {
		return errors.New("remediation operation and Worker identity are required")
	}
	if !validText(request.NodeIdentity, 500) || !validText(request.DeviceIdentity, 500) ||
		!validText(request.FailureClass, 200) || !validOptionalText(request.CertificationRevision, 200) ||
		!validText(request.IdempotencyKey, 200) || !validText(request.RequestedBy, 500) {
		return errors.New("remediation request text is invalid")
	}
	if len(request.EvidenceDigest) != sha256.Size {
		return errors.New("remediation evidence digest must be SHA-256")
	}
	if !validAction(request.ActionLevel) {
		return errors.New("remediation action level is invalid")
	}
	if request.ActionLevel != ActionL7Quarantine && request.CertificationRevision == "" {
		return errors.New("automatic Remediation requires a certification revision")
	}
	return nil
}

func validateCompletion(completion Completion) error {
	if completion.OperationID == uuid.Nil || completion.WorkerID == uuid.Nil || completion.WorkerEpoch <= 0 ||
		!validText(completion.ResultCode, 200) || !validText(completion.ResultDetail, 2000) ||
		!validText(completion.ActorIdentity, 500) {
		return errors.New("remediation completion fields are invalid")
	}
	if completion.Success && len(completion.PostcheckHash) != sha256.Size {
		return errors.New("successful Remediation completion requires a post-check digest")
	}
	if len(completion.PostcheckHash) != 0 && len(completion.PostcheckHash) != sha256.Size {
		return errors.New("remediation post-check digest must be SHA-256")
	}
	return nil
}

func validAction(action ActionLevel) bool {
	switch action {
	case ActionL0ProcessRestart, ActionL1CUDACleanup, ActionL2GPUReset, ActionL3PCIeFLR,
		ActionL4DriverReload, ActionL5NodeReboot, ActionL6BMCPowerCycle, ActionL7Quarantine:
		return true
	default:
		return false
	}
}

func validText(value string, max int) bool {
	return value != "" && len(value) <= max && strings.TrimSpace(value) == value && !strings.ContainsRune(value, '\x00')
}

func validOptionalText(value string, max int) bool {
	return len(value) <= max && strings.TrimSpace(value) == value && !strings.ContainsRune(value, '\x00')
}

func operationFromRow(row store.RemediationOperation) Operation {
	return Operation{
		ID: row.ID, WorkerID: row.WorkerID, WorkerEpoch: row.WorkerEpoch,
		NodeIdentity: row.NodeIdentity, DeviceIdentity: row.DeviceIdentity,
		FailureClass: row.FailureClass, EvidenceDigest: append([]byte(nil), row.EvidenceDigest...),
		CertificationRevision: row.CertificationRevision, ActionLevel: row.ActionLevel,
		IdempotencyKey: row.IdempotencyKey, RequestedBy: row.RequestedBy, State: row.State,
		RequestedAt: row.RequestedAt.Time, DeadlineAt: row.DeadlineAt.Time, StartedAt: nullableTime(row.StartedAt),
		FinishedAt: nullableTime(row.FinishedAt), ResultCode: nullableString(row.ResultCode),
		ResultDetail: nullableString(row.ResultDetail), PostcheckDigest: append([]byte(nil), row.PostcheckDigest...),
		FirstApprover: nullableString(row.FirstApprover), SecondApprover: nullableString(row.SecondApprover),
		ApprovedAt: nullableTime(row.ApprovedAt),
	}
}

func nullableTime(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time
	return &result
}

func nullableString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func mapDatabaseError(operation string, err error) error {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return fmt.Errorf("%s: %w", operation, err)
	}
	code := FailureUnavailable
	switch postgresError.Code {
	case "22023":
		code = FailureInvalid
	case "P0001", "23514", "23505":
		code = FailureConflict
	case "P0002", "02000":
		code = FailureNotFound
	}
	return &Failure{Code: code, Message: postgresError.Message}
}
