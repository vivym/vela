package debugdump

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/identity"
)

type FailureCode string

const (
	FailureUnauthorized FailureCode = "unauthorized"
	FailureForbidden    FailureCode = "forbidden"
	FailureInvalid      FailureCode = "invalid_request"
	FailureNotFound     FailureCode = "not_found"
	FailureConflict     FailureCode = "conflict"
	FailureUnavailable  FailureCode = "unavailable"
)

type Failure struct {
	Code    FailureCode
	Message string
}

func (failure *Failure) Error() string {
	return failure.Message
}

type Purpose string

const (
	PurposeCustomerSupport       Purpose = "CUSTOMER_SUPPORT"
	PurposeIncidentInvestigation Purpose = "INCIDENT_INVESTIGATION"
)

type Authorization struct {
	ID             uuid.UUID
	OrganizationID uuid.UUID
	ProjectID      uuid.UUID
	JobID          uuid.UUID
	Purpose        Purpose
	AuthorizedAt   time.Time
	ExpiresAt      time.Time
	RevokedAt      *time.Time
	Replayed       bool
}

type State string

const (
	StateUploading State = "UPLOADING"
	StateAvailable State = "AVAILABLE"
	StateDeleted   State = "DELETED"
)

type Dump struct {
	ID              uuid.UUID
	AuthorizationID uuid.UUID
	AttemptID       uuid.UUID
	State           State
	SizeBytes       int64
	SHA256          [sha256.Size]byte
	ContentType     string
	CreatedAt       time.Time
	UploadedAt      *time.Time
	ExpiresAt       time.Time
	DeletedAt       *time.Time
}

type Download struct {
	Dump
	DownloadURL          string
	DownloadURLExpiresAt time.Time
}

type ExactVersionSigner interface {
	PresignExactVersionUntil(
		context.Context, string, string, time.Time,
	) (artifactstore.SignedRead, error)
}

type Service struct {
	pool   *pgxpool.Pool
	signer ExactVersionSigner
}

func NewService(pool *pgxpool.Pool, signer ...ExactVersionSigner) (*Service, error) {
	if pool == nil {
		return nil, errors.New("debug dump request database pool is required")
	}
	if len(signer) > 1 {
		return nil, errors.New("at most one debug dump exact-version signer is allowed")
	}
	service := &Service{pool: pool}
	if len(signer) == 1 {
		if signer[0] == nil {
			return nil, errors.New("debug dump exact-version signer is nil")
		}
		service.signer = signer[0]
	}
	return service, nil
}

func (service *Service) Authorize(
	ctx context.Context,
	actor identity.Principal,
	projectID, jobID uuid.UUID,
	idempotencyKey string,
	purpose Purpose,
) (Authorization, error) {
	if err := authorizeManagement(actor, projectID); err != nil {
		return Authorization{}, err
	}
	if jobID == uuid.Nil || len(idempotencyKey) < 1 || len(idempotencyKey) > 128 ||
		(purpose != PurposeCustomerSupport && purpose != PurposeIncidentInvestigation) {
		return Authorization{}, &Failure{
			Code: FailureInvalid, Message: "debug dump authorization request is invalid",
		}
	}
	if service == nil || service.pool == nil {
		return Authorization{}, errors.New("debug dump service is not configured")
	}
	tx, err := service.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Authorization{}, fmt.Errorf("begin debug dump authorization transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishRequestContext(ctx, tx, actor, projectID); err != nil {
		return Authorization{}, err
	}
	requestHash := sha256.Sum256([]byte(
		"vela.debug-dump.authorization.v1\x00" + projectID.String() + "\x00" +
			jobID.String() + "\x00" + string(purpose),
	))
	result, err := scanAuthorization(tx.QueryRow(ctx, `
		SELECT authorization_id, organization_id, project_id, job_id, purpose,
			authorized_at, expires_at, revoked_at, replayed
		FROM vela_authorize_debug_dump($1, $2, $3, $4, $5, $6)
	`, uuid.New(), projectID, jobID, idempotencyKey, requestHash[:], string(purpose)))
	if err != nil {
		return Authorization{}, mapDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Authorization{}, fmt.Errorf("commit debug dump authorization: %w", err)
	}
	return result, nil
}

func (service *Service) GetAuthorization(
	ctx context.Context,
	actor identity.Principal,
	projectID, jobID, authorizationID uuid.UUID,
) (Authorization, error) {
	if err := authorizeManagement(actor, projectID); err != nil {
		return Authorization{}, err
	}
	if jobID == uuid.Nil || authorizationID == uuid.Nil {
		return Authorization{}, &Failure{
			Code: FailureInvalid, Message: "debug dump authorization request is invalid",
		}
	}
	if service == nil || service.pool == nil {
		return Authorization{}, errors.New("debug dump service is not configured")
	}
	tx, err := service.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Authorization{}, fmt.Errorf("begin debug dump authorization read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishRequestContext(ctx, tx, actor, projectID); err != nil {
		return Authorization{}, err
	}
	result, err := scanAuthorization(tx.QueryRow(ctx, `
		SELECT authorization_id, organization_id, project_id, job_id, purpose,
			authorized_at, expires_at, revoked_at, replayed
		FROM vela_get_debug_dump_authorization($1, $2, $3)
	`, projectID, jobID, authorizationID))
	if err != nil {
		return Authorization{}, mapDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Authorization{}, fmt.Errorf("commit debug dump authorization read: %w", err)
	}
	return result, nil
}

func (service *Service) RevokeAuthorization(
	ctx context.Context,
	actor identity.Principal,
	projectID, jobID, authorizationID uuid.UUID,
	idempotencyKey string,
) (Authorization, error) {
	if err := authorizeManagement(actor, projectID); err != nil {
		return Authorization{}, err
	}
	if jobID == uuid.Nil || authorizationID == uuid.Nil ||
		len(idempotencyKey) < 1 || len(idempotencyKey) > 128 {
		return Authorization{}, &Failure{
			Code: FailureInvalid, Message: "debug dump revocation request is invalid",
		}
	}
	if service == nil || service.pool == nil {
		return Authorization{}, errors.New("debug dump service is not configured")
	}
	tx, err := service.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Authorization{}, fmt.Errorf("begin debug dump revocation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishRequestContext(ctx, tx, actor, projectID); err != nil {
		return Authorization{}, err
	}
	requestHash := sha256.Sum256([]byte(
		"vela.debug-dump.revocation.v1\x00" + projectID.String() + "\x00" +
			jobID.String() + "\x00" + authorizationID.String(),
	))
	result, err := scanAuthorization(tx.QueryRow(ctx, `
		SELECT authorization_id, organization_id, project_id, job_id, purpose,
			authorized_at, expires_at, revoked_at, replayed
		FROM vela_revoke_debug_dump_authorization($1, $2, $3, $4, $5)
	`, projectID, jobID, authorizationID, idempotencyKey, requestHash[:]))
	if err != nil {
		return Authorization{}, mapDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Authorization{}, fmt.Errorf("commit debug dump revocation: %w", err)
	}
	return result, nil
}

func (service *Service) ListDumps(
	ctx context.Context,
	actor identity.Principal,
	projectID, jobID, authorizationID uuid.UUID,
) ([]Dump, error) {
	if err := authorizeManagement(actor, projectID); err != nil {
		return nil, err
	}
	if jobID == uuid.Nil || authorizationID == uuid.Nil {
		return nil, &Failure{
			Code: FailureInvalid, Message: "debug dump list request is invalid",
		}
	}
	if service == nil || service.pool == nil {
		return nil, errors.New("debug dump service is not configured")
	}
	tx, err := service.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin debug dump list: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishRequestContext(ctx, tx, actor, projectID); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT debug_dump_id, authorization_id, attempt_id, dump_state,
			size_bytes, sha256, content_type, created_at, uploaded_at,
			expires_at, deleted_at
		FROM vela_list_debug_dumps($1, $2, $3)
	`, projectID, jobID, authorizationID)
	if err != nil {
		return nil, mapDatabaseFailure(err)
	}
	defer rows.Close()
	dumps := make([]Dump, 0)
	for rows.Next() {
		var dump Dump
		var state string
		var digest []byte
		var uploadedAt, deletedAt pgtype.Timestamptz
		if err := rows.Scan(
			&dump.ID,
			&dump.AuthorizationID,
			&dump.AttemptID,
			&state,
			&dump.SizeBytes,
			&digest,
			&dump.ContentType,
			&dump.CreatedAt,
			&uploadedAt,
			&dump.ExpiresAt,
			&deletedAt,
		); err != nil {
			return nil, mapDatabaseFailure(err)
		}
		if len(digest) != sha256.Size || dump.SizeBytes <= 0 || dump.ContentType == "" {
			return nil, &Failure{
				Code: FailureUnavailable, Message: "debug dump metadata is unavailable",
			}
		}
		copy(dump.SHA256[:], digest)
		dump.State = State(state)
		if uploadedAt.Valid {
			value := uploadedAt.Time
			dump.UploadedAt = &value
		}
		if deletedAt.Valid {
			value := deletedAt.Time
			dump.DeletedAt = &value
		}
		dumps = append(dumps, dump)
	}
	if err := rows.Err(); err != nil {
		return nil, mapDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit debug dump list: %w", err)
	}
	return dumps, nil
}

func (service *Service) ReadDump(
	ctx context.Context,
	actor identity.Principal,
	projectID, jobID, authorizationID, dumpID uuid.UUID,
) (Download, error) {
	if err := authorizeManagement(actor, projectID); err != nil {
		return Download{}, err
	}
	if jobID == uuid.Nil || authorizationID == uuid.Nil || dumpID == uuid.Nil {
		return Download{}, &Failure{
			Code: FailureInvalid, Message: "debug dump read request is invalid",
		}
	}
	if service == nil || service.pool == nil || service.signer == nil {
		return Download{}, &Failure{
			Code: FailureUnavailable, Message: "debug dump read dependency is unavailable",
		}
	}

	tx, err := service.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Download{}, fmt.Errorf("begin debug dump read authorization: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishRequestContext(ctx, tx, actor, projectID); err != nil {
		return Download{}, err
	}
	var authorized bool
	var objectKey, objectVersionID, contentType pgtype.Text
	var sizeBytes pgtype.Int8
	var digest []byte
	var authorizationExpiresAt time.Time
	err = tx.QueryRow(ctx, `
		SELECT authorized, object_key, object_version_id, size_bytes, sha256,
			content_type, authorization_expires_at
		FROM vela_authorize_debug_dump_read($1, $2, $3, $4)
	`, projectID, jobID, authorizationID, dumpID).Scan(
		&authorized,
		&objectKey,
		&objectVersionID,
		&sizeBytes,
		&digest,
		&contentType,
		&authorizationExpiresAt,
	)
	if err != nil {
		return Download{}, mapDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Download{}, fmt.Errorf("commit debug dump read authorization: %w", err)
	}
	if !authorized {
		return Download{}, &Failure{
			Code: FailureForbidden, Message: "debug dump is not available for reading",
		}
	}
	if !objectKey.Valid || !objectVersionID.Valid || !sizeBytes.Valid ||
		!contentType.Valid || len(digest) != sha256.Size || sizeBytes.Int64 <= 0 {
		return Download{}, &Failure{
			Code: FailureUnavailable, Message: "debug dump metadata is unavailable",
		}
	}

	signed, signErr := service.signer.PresignExactVersionUntil(
		ctx, objectKey.String, objectVersionID.String, authorizationExpiresAt,
	)
	if signErr != nil {
		if _, recordErr := service.recordDelivery(
			ctx, actor, projectID, jobID, authorizationID, dumpID,
			objectVersionID.String, false,
		); recordErr != nil {
			return Download{}, &Failure{
				Code:    FailureUnavailable,
				Message: "debug dump delivery evidence is unavailable",
			}
		}
		return Download{}, &Failure{
			Code: FailureUnavailable, Message: "debug dump signing is unavailable",
		}
	}
	if signed.URL == "" || signed.IssuedAt.IsZero() ||
		!signed.ExpiresAt.After(signed.IssuedAt) ||
		signed.ExpiresAt.After(authorizationExpiresAt) ||
		signed.ExpiresAt.Sub(signed.IssuedAt) > artifactstore.MaxSignedGETTTL {
		if _, recordErr := service.recordDelivery(
			ctx, actor, projectID, jobID, authorizationID, dumpID,
			objectVersionID.String, false,
		); recordErr != nil {
			return Download{}, &Failure{
				Code:    FailureUnavailable,
				Message: "debug dump delivery evidence is unavailable",
			}
		}
		return Download{}, &Failure{
			Code: FailureUnavailable, Message: "debug dump signing is unavailable",
		}
	}
	delivered, err := service.recordDelivery(
		ctx, actor, projectID, jobID, authorizationID, dumpID,
		objectVersionID.String, true,
	)
	if err != nil {
		return Download{}, err
	}
	if !delivered {
		return Download{}, &Failure{
			Code:    FailureForbidden,
			Message: "debug dump authorization became inactive before delivery",
		}
	}
	result := Download{
		Dump: Dump{
			ID: dumpID, AuthorizationID: authorizationID, State: StateAvailable,
			SizeBytes: sizeBytes.Int64, ContentType: contentType.String,
			ExpiresAt: authorizationExpiresAt,
		},
		DownloadURL:          signed.URL,
		DownloadURLExpiresAt: signed.ExpiresAt,
	}
	copy(result.SHA256[:], digest)
	return result, nil
}

func (service *Service) recordDelivery(
	ctx context.Context,
	actor identity.Principal,
	projectID, jobID, authorizationID, dumpID uuid.UUID,
	objectVersionID string,
	signingSucceeded bool,
) (bool, error) {
	tx, err := service.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return false, fmt.Errorf("begin debug dump delivery evidence: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishRequestContext(ctx, tx, actor, projectID); err != nil {
		return false, err
	}
	var delivered bool
	if err := tx.QueryRow(ctx, `
		SELECT vela_record_debug_dump_delivery($1, $2, $3, $4, $5, $6)
	`, projectID, jobID, authorizationID, dumpID, objectVersionID, signingSucceeded).Scan(
		&delivered,
	); err != nil {
		return false, mapDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit debug dump delivery evidence: %w", err)
	}
	return delivered, nil
}

func authorizeManagement(actor identity.Principal, projectID uuid.UUID) error {
	if actor.CredentialID == uuid.Nil {
		return &Failure{Code: FailureUnauthorized, Message: "valid bearer credential is required"}
	}
	if actor.ProjectID != projectID {
		return &Failure{Code: FailureNotFound, Message: "Project is not visible"}
	}
	if actor.Kind != identity.PrincipalKindHuman || actor.OrganizationID == uuid.Nil ||
		actor.PrincipalID == uuid.Nil || !actor.HasScope(identity.ScopeDebugDumpsManage) {
		return &Failure{
			Code: FailureForbidden, Message: "Human ProjectAdmin debug dump authority is required",
		}
	}
	return nil
}

func establishRequestContext(
	ctx context.Context,
	tx pgx.Tx,
	actor identity.Principal,
	projectID uuid.UUID,
) error {
	var organizationID, returnedProjectID, principalID uuid.UUID
	var transactionTime time.Time
	credentialProof := actor.RequestContextProof()
	defer clear(credentialProof)
	err := tx.QueryRow(ctx, `
		SELECT organization_id, project_id, principal_id, transaction_time
		FROM vela_set_request_context($1, $2, $3)
	`, actor.CredentialID, credentialProof, identity.ScopeDebugDumpsManage).Scan(
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
		return errors.New("debug dump request context does not match authenticated Principal")
	}
	return nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanAuthorization(row rowScanner) (Authorization, error) {
	var result Authorization
	var purpose string
	var revokedAt pgtype.Timestamptz
	err := row.Scan(
		&result.ID,
		&result.OrganizationID,
		&result.ProjectID,
		&result.JobID,
		&purpose,
		&result.AuthorizedAt,
		&result.ExpiresAt,
		&revokedAt,
		&result.Replayed,
	)
	if err != nil {
		return Authorization{}, err
	}
	result.Purpose = Purpose(purpose)
	if revokedAt.Valid {
		value := revokedAt.Time
		result.RevokedAt = &value
	}
	return result, nil
}

func mapDatabaseFailure(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return &Failure{Code: FailureNotFound, Message: "debug dump target is not visible"}
	}
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return err
	}
	switch postgresError.Code {
	case "22023":
		return &Failure{Code: FailureInvalid, Message: "debug dump request is invalid"}
	case "23505", "55000":
		return &Failure{Code: FailureConflict, Message: postgresError.Message}
	case "P0002":
		return &Failure{Code: FailureNotFound, Message: "debug dump target is not visible"}
	case "28000", "42501":
		return &Failure{Code: FailureForbidden, Message: "debug dump authority is required"}
	default:
		return err
	}
}
