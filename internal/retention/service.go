package retention

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/identity"
)

type FailureCode string

const (
	FailureUnauthorized FailureCode = "unauthorized"
	FailureForbidden    FailureCode = "forbidden"
	FailureInvalid      FailureCode = "invalid_request"
	FailureNotFound     FailureCode = "not_found"
	FailureConflict     FailureCode = "conflict"
)

type Failure struct {
	Code    FailureCode
	Message string
}

func (f *Failure) Error() string {
	return f.Message
}

type Policy struct {
	ProjectID                       uuid.UUID
	PolicyRevisionID                uuid.UUID
	StableID                        string
	ArtifactRetentionDays           int32
	RequestContentRetentionDays     int32
	IncompleteContentRetentionHours int32
	ScratchRetentionHours           int32
	DebugRetentionHours             int32
	MetadataRetentionDays           int32
	FinancialRetentionDays          int32
	SelectedAt                      time.Time
}

type DeletionState string

const (
	DeletionStatePending    DeletionState = "PENDING"
	DeletionStateInProgress DeletionState = "IN_PROGRESS"
	DeletionStateRetryWait  DeletionState = "RETRY_WAIT"
	DeletionStateCompleted  DeletionState = "COMPLETED"
)

type DeletionRequest struct {
	RequestID            uuid.UUID
	ProjectID            uuid.UUID
	JobID                uuid.UUID
	State                DeletionState
	RequestedAt          time.Time
	DeadlineAt           time.Time
	CompletedAt          *time.Time
	Overdue              bool
	TargetCount          int64
	CompletedTargetCount int64
	RetryingTargetCount  int64
	LastErrorCode        *string
	LastErrorMessage     *string
}

type Service struct {
	requestPool *pgxpool.Pool
}

func NewService(requestPool *pgxpool.Pool) (*Service, error) {
	if requestPool == nil {
		return nil, errors.New("retention request database pool is required")
	}
	return &Service{requestPool: requestPool}, nil
}

func (s *Service) GetProjectPolicy(
	ctx context.Context,
	actor identity.Principal,
	projectID uuid.UUID,
) (Policy, error) {
	if err := authorizePolicyManagement(actor, projectID); err != nil {
		return Policy{}, err
	}
	if s == nil || s.requestPool == nil {
		return Policy{}, errors.New("retention policy service is not configured")
	}
	tx, err := s.requestPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Policy{}, fmt.Errorf("begin Retention Policy read transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishRequestContext(
		ctx, tx, actor, projectID, identity.ScopeRetentionPolicyManage,
	); err != nil {
		return Policy{}, err
	}
	policy, err := scanPolicy(tx.QueryRow(ctx, `
		SELECT project_id, policy_revision_id, stable_id, artifact_retention_days,
			request_content_retention_days, incomplete_content_retention_hours,
			scratch_retention_hours, debug_retention_hours, metadata_retention_days,
			financial_retention_days, selected_at
		FROM vela_get_project_retention_policy($1)
	`, projectID))
	if err != nil {
		return Policy{}, mapDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Policy{}, fmt.Errorf("commit Retention Policy read: %w", err)
	}
	return policy, nil
}

func (s *Service) SetProjectPolicy(
	ctx context.Context,
	actor identity.Principal,
	projectID uuid.UUID,
	artifactRetentionDays int32,
) (Policy, error) {
	if err := authorizePolicyManagement(actor, projectID); err != nil {
		return Policy{}, err
	}
	if artifactRetentionDays != 7 && artifactRetentionDays != 30 && artifactRetentionDays != 90 {
		return Policy{}, &Failure{
			Code: FailureInvalid, Message: "Artifact retention must be 7, 30, or 90 days",
		}
	}
	if s == nil || s.requestPool == nil {
		return Policy{}, errors.New("retention policy service is not configured")
	}
	tx, err := s.requestPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Policy{}, fmt.Errorf("begin Retention Policy selection transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishRequestContext(
		ctx, tx, actor, projectID, identity.ScopeRetentionPolicyManage,
	); err != nil {
		return Policy{}, err
	}
	policy, err := scanPolicy(tx.QueryRow(ctx, `
		SELECT project_id, policy_revision_id, stable_id, artifact_retention_days,
			request_content_retention_days, incomplete_content_retention_hours,
			scratch_retention_hours, debug_retention_hours, metadata_retention_days,
			financial_retention_days, selected_at
		FROM vela_set_project_retention_policy($1, $2, $3)
	`, projectID, artifactRetentionDays, uuid.New()))
	if err != nil {
		return Policy{}, mapDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Policy{}, fmt.Errorf("commit Retention Policy selection: %w", err)
	}
	return policy, nil
}

func (s *Service) AcceptContentDeletion(
	ctx context.Context,
	actor identity.Principal,
	projectID, jobID uuid.UUID,
	idempotencyKey string,
) (DeletionRequest, error) {
	if err := authorizeContentDeletion(actor, projectID); err != nil {
		return DeletionRequest{}, err
	}
	if jobID == uuid.Nil || len(idempotencyKey) < 1 || len(idempotencyKey) > 128 {
		return DeletionRequest{}, &Failure{
			Code: FailureInvalid, Message: "Content Deletion request is invalid",
		}
	}
	if s == nil || s.requestPool == nil {
		return DeletionRequest{}, errors.New("content deletion service is not configured")
	}
	var lastErr error
	for range 3 {
		deletion, err := s.acceptContentDeletionOnce(
			ctx,
			actor,
			projectID,
			jobID,
			idempotencyKey,
		)
		if err == nil || !retryableRetentionTransaction(err) {
			return deletion, err
		}
		lastErr = err
	}
	return DeletionRequest{}, fmt.Errorf(
		"content deletion transaction did not stabilize: %w",
		lastErr,
	)
}

func (s *Service) acceptContentDeletionOnce(
	ctx context.Context,
	actor identity.Principal,
	projectID, jobID uuid.UUID,
	idempotencyKey string,
) (DeletionRequest, error) {
	tx, err := s.requestPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return DeletionRequest{}, fmt.Errorf("begin Content Deletion transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishRequestContext(
		ctx, tx, actor, projectID, identity.ScopeContentDeletionManage,
	); err != nil {
		return DeletionRequest{}, err
	}
	requestHash := sha256.Sum256([]byte(
		"vela.content-deletion.customer.v1\x00" + projectID.String() + "\x00" + jobID.String(),
	))
	deletion, err := scanDeletionRequest(tx.QueryRow(ctx, `
		SELECT request_id, project_id, job_id, request_state::text, requested_at,
			deadline_at, completed_at, overdue, target_count,
			completed_target_count, retrying_target_count,
			last_error_code, last_error_message
		FROM vela_accept_content_deletion_request(
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10, $11, $12
		)
	`,
		uuid.New(), projectID, jobID, idempotencyKey, requestHash[:],
		uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(),
	))
	if err != nil {
		return DeletionRequest{}, mapDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return DeletionRequest{}, fmt.Errorf("commit Content Deletion request: %w", err)
	}
	return deletion, nil
}

func (s *Service) GetContentDeletion(
	ctx context.Context,
	actor identity.Principal,
	projectID, requestID uuid.UUID,
) (DeletionRequest, error) {
	if err := authorizeContentDeletion(actor, projectID); err != nil {
		return DeletionRequest{}, err
	}
	if requestID == uuid.Nil {
		return DeletionRequest{}, &Failure{
			Code: FailureInvalid, Message: "Content Deletion request is invalid",
		}
	}
	if s == nil || s.requestPool == nil {
		return DeletionRequest{}, errors.New("content deletion service is not configured")
	}
	tx, err := s.requestPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return DeletionRequest{}, fmt.Errorf("begin Content Deletion read transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishRequestContext(
		ctx, tx, actor, projectID, identity.ScopeContentDeletionManage,
	); err != nil {
		return DeletionRequest{}, err
	}
	deletion, err := scanDeletionRequest(tx.QueryRow(ctx, `
		SELECT request_id, project_id, job_id, request_state::text, requested_at,
			deadline_at, completed_at, overdue, target_count,
			completed_target_count, retrying_target_count,
			last_error_code, last_error_message
		FROM vela_get_content_deletion_request($1, $2)
	`, projectID, requestID))
	if err != nil {
		return DeletionRequest{}, mapDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return DeletionRequest{}, fmt.Errorf("commit Content Deletion read: %w", err)
	}
	return deletion, nil
}

func authorizePolicyManagement(actor identity.Principal, projectID uuid.UUID) error {
	if actor.CredentialID == uuid.Nil {
		return &Failure{Code: FailureUnauthorized, Message: "valid bearer credential is required"}
	}
	if actor.Kind != identity.PrincipalKindHuman || actor.OrganizationID == uuid.Nil ||
		actor.ProjectID != projectID || !actor.HasScope(identity.ScopeRetentionPolicyManage) {
		return &Failure{
			Code: FailureForbidden, Message: "Human ProjectAdmin authorization is required",
		}
	}
	return nil
}

func authorizeContentDeletion(actor identity.Principal, projectID uuid.UUID) error {
	if actor.CredentialID == uuid.Nil {
		return &Failure{Code: FailureUnauthorized, Message: "valid bearer credential is required"}
	}
	if actor.ProjectID != projectID {
		return &Failure{Code: FailureNotFound, Message: "Project is not visible"}
	}
	if actor.OrganizationID == uuid.Nil || actor.PrincipalID == uuid.Nil ||
		(actor.Kind != identity.PrincipalKindHuman && actor.Kind != identity.PrincipalKindService) ||
		!actor.HasScope(identity.ScopeContentDeletionManage) {
		return &Failure{
			Code: FailureForbidden, Message: "Content Deletion authority is required",
		}
	}
	return nil
}

func establishRequestContext(
	ctx context.Context,
	tx pgx.Tx,
	actor identity.Principal,
	projectID uuid.UUID,
	requiredScope string,
) error {
	var organizationID, returnedProjectID, principalID uuid.UUID
	var transactionTime time.Time
	credentialProof := actor.RequestContextProof()
	defer clear(credentialProof)
	err := tx.QueryRow(ctx, `
		SELECT organization_id, project_id, principal_id, transaction_time
		FROM vela_set_request_context($1, $2, $3)
	`, actor.CredentialID, credentialProof, requiredScope).Scan(
		&organizationID,
		&returnedProjectID,
		&principalID,
		&transactionTime,
	)
	if err != nil {
		return mapDatabaseFailure(err)
	}
	if organizationID != actor.OrganizationID || returnedProjectID != projectID ||
		principalID != actor.PrincipalID {
		return errors.New("retention policy context does not match authenticated Principal")
	}
	return nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanPolicy(row rowScanner) (Policy, error) {
	var policy Policy
	err := row.Scan(
		&policy.ProjectID,
		&policy.PolicyRevisionID,
		&policy.StableID,
		&policy.ArtifactRetentionDays,
		&policy.RequestContentRetentionDays,
		&policy.IncompleteContentRetentionHours,
		&policy.ScratchRetentionHours,
		&policy.DebugRetentionHours,
		&policy.MetadataRetentionDays,
		&policy.FinancialRetentionDays,
		&policy.SelectedAt,
	)
	return policy, err
}

func scanDeletionRequest(row rowScanner) (DeletionRequest, error) {
	var deletion DeletionRequest
	var state string
	err := row.Scan(
		&deletion.RequestID,
		&deletion.ProjectID,
		&deletion.JobID,
		&state,
		&deletion.RequestedAt,
		&deletion.DeadlineAt,
		&deletion.CompletedAt,
		&deletion.Overdue,
		&deletion.TargetCount,
		&deletion.CompletedTargetCount,
		&deletion.RetryingTargetCount,
		&deletion.LastErrorCode,
		&deletion.LastErrorMessage,
	)
	if err != nil {
		return DeletionRequest{}, err
	}
	deletion.State, err = deletionStateFromDatabase(state)
	if err != nil {
		return DeletionRequest{}, err
	}
	return deletion, nil
}

func deletionStateFromDatabase(value string) (DeletionState, error) {
	state := DeletionState(value)
	switch state {
	case DeletionStatePending,
		DeletionStateInProgress,
		DeletionStateRetryWait,
		DeletionStateCompleted:
		return state, nil
	default:
		return "", fmt.Errorf("unknown Content Deletion state %q", value)
	}
}

func mapDatabaseFailure(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return &Failure{Code: FailureNotFound, Message: "Project is not visible"}
	}
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return err
	}
	switch postgresError.Code {
	case "22023", "22P02", "23514":
		return &Failure{Code: FailureInvalid, Message: "Retention request is invalid"}
	case "28000":
		return &Failure{Code: FailureUnauthorized, Message: "credential is no longer active"}
	case "42501":
		return &Failure{Code: FailureForbidden, Message: "Retention authority is required"}
	case "P0002":
		return &Failure{Code: FailureNotFound, Message: "Retention target is not visible"}
	case "23505":
		return &Failure{Code: FailureConflict, Message: "Idempotency-Key conflicts with another target"}
	default:
		return err
	}
}

func retryableRetentionTransaction(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) &&
		(postgresError.Code == "40001" || postgresError.Code == "40P01")
}
