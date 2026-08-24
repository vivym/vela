package breakglass

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/artifactstore"
)

type Scope string

const (
	ScopeRequestContentRead Scope = "REQUEST_CONTENT_READ"
	ScopeArtifactRead       Scope = "ARTIFACT_READ"
)

type ReasonCode string

const (
	ReasonCustomerSupport       ReasonCode = "CUSTOMER_SUPPORT"
	ReasonSecurityInvestigation ReasonCode = "SECURITY_INVESTIGATION"
	ReasonServiceRecovery       ReasonCode = "SERVICE_RECOVERY"
	ReasonLegalResponse         ReasonCode = "LEGAL_RESPONSE"
)

type State string

const (
	StatePending State = "PENDING"
	StateActive  State = "ACTIVE"
	StateRevoked State = "REVOKED"
	StateExpired State = "EXPIRED"
)

type FailureCode string

const (
	FailureInvalid      FailureCode = "invalid_request"
	FailureUnauthorized FailureCode = "unauthorized"
	FailureForbidden    FailureCode = "forbidden"
	FailureNotFound     FailureCode = "not_found"
	FailureConflict     FailureCode = "conflict"
	FailureUnavailable  FailureCode = "service_unavailable"
)

type Failure struct {
	Code    FailureCode
	Message string
}

func (failure *Failure) Error() string {
	return failure.Message
}

func IsFailure(err error, code FailureCode) bool {
	var failure *Failure
	return errors.As(err, &failure) && failure.Code == code
}

type Target struct {
	OrganizationID uuid.UUID
	ProjectID      uuid.UUID
	JobID          uuid.UUID
}

type RequestInput struct {
	Target
	Scopes                   []Scope
	ReasonCode               ReasonCode
	TicketReference          string
	RequestedDurationSeconds int32
}

type Request struct {
	ID uuid.UUID
	Target
	Scopes                   []Scope
	ReasonCode               ReasonCode
	TicketReference          string
	RequestedDurationSeconds int32
	RequesterOperatorID      uuid.UUID
	RequestedAt              time.Time
	ApprovalDeadlineAt       time.Time
	GrantID                  *uuid.UUID
	ApproverOperatorID       *uuid.UUID
	ApprovedAt               *time.Time
	ExpiresAt                *time.Time
	RevokedAt                *time.Time
	RevokedByOperatorID      *uuid.UUID
	State                    State
}

type RequestContent struct {
	Target
	RequestContent json.RawMessage
}

type ArtifactSet struct {
	ID uuid.UUID
	Target
	RetentionExpiresAt time.Time
	CommittedAt        time.Time
	Artifacts          []Artifact
}

type Artifact struct {
	ID                   uuid.UUID
	Kind                 string
	Ordinal              int32
	SizeBytes            int64
	SHA256               [sha256.Size]byte
	ContentType          string
	DownloadURL          string
	DownloadURLExpiresAt time.Time
	objectKey            string
	objectVersionID      string
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

var (
	idempotencyKeyPattern  = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)
	ticketReferencePattern = regexp.MustCompile(
		`^[A-Za-z0-9][A-Za-z0-9._:/-]{2,99}$`,
	)
)

func NewService(pool *pgxpool.Pool, signer ...ExactVersionSigner) (*Service, error) {
	if pool == nil {
		return nil, errors.New("break-glass request database pool is required")
	}
	if len(signer) > 1 {
		return nil, errors.New("at most one Break-glass Artifact signer is allowed")
	}
	service := &Service{pool: pool}
	if len(signer) == 1 {
		if signer[0] == nil {
			return nil, errors.New("Artifact signer for Break-glass Access is nil")
		}
		service.signer = signer[0]
	}
	return service, nil
}

func (service *Service) Request(
	ctx context.Context,
	operator Operator,
	idempotencyKey string,
	input RequestInput,
) (Request, bool, error) {
	normalized, requestHash, err := normalizeRequest(idempotencyKey, input)
	if err != nil {
		return Request{}, false, err
	}
	tx, err := service.begin(ctx, operator)
	if err != nil {
		return Request{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	scopes := make([]string, len(normalized.Scopes))
	for index, scope := range normalized.Scopes {
		scopes[index] = string(scope)
	}
	requestID := uuid.New()
	var replayed bool
	if err := tx.QueryRow(ctx, `
		SELECT request_id, replayed
		FROM vela_create_break_glass_request(
			$1, $2, $3, $4, $5, $6, $7::break_glass_scope[],
			$8::break_glass_reason_code, $9, $10
		)
	`,
		requestID,
		idempotencyKey,
		requestHash[:],
		normalized.OrganizationID,
		normalized.ProjectID,
		normalized.JobID,
		scopes,
		normalized.ReasonCode,
		normalized.TicketReference,
		normalized.RequestedDurationSeconds,
	).Scan(&requestID, &replayed); err != nil {
		return Request{}, false, mapDatabaseFailure(err)
	}
	result, err := getRequestByID(ctx, tx, requestID)
	if err != nil {
		return Request{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Request{}, false, mapDatabaseFailure(err)
	}
	return result, replayed, nil
}

func (service *Service) Approve(
	ctx context.Context,
	operator Operator,
	requestID uuid.UUID,
) (Request, error) {
	if requestID == uuid.Nil {
		return Request{}, invalidFailure("Break-glass request id is required")
	}
	tx, err := service.begin(ctx, operator)
	if err != nil {
		return Request{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var grantID uuid.UUID
	var expiresAt time.Time
	var replayed bool
	if err := tx.QueryRow(ctx, `
		SELECT grant_id, expires_at, replayed
		FROM vela_approve_break_glass_request($1, $2)
	`, requestID, uuid.New()).Scan(&grantID, &expiresAt, &replayed); err != nil {
		return Request{}, mapDatabaseFailure(err)
	}
	result, err := getRequestByID(ctx, tx, requestID)
	if err != nil {
		return Request{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Request{}, mapDatabaseFailure(err)
	}
	return result, nil
}

func (service *Service) Revoke(
	ctx context.Context,
	operator Operator,
	grantID uuid.UUID,
) (Request, error) {
	if grantID == uuid.Nil {
		return Request{}, invalidFailure("Break-glass grant id is required")
	}
	tx, err := service.begin(ctx, operator)
	if err != nil {
		return Request{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var revokedAt time.Time
	var replayed bool
	if err := tx.QueryRow(ctx, `
		SELECT revoked_at, replayed
		FROM vela_revoke_break_glass_grant($1)
	`, grantID).Scan(&revokedAt, &replayed); err != nil {
		return Request{}, mapDatabaseFailure(err)
	}
	result, err := getRequestByGrantID(ctx, tx, grantID)
	if err != nil {
		return Request{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Request{}, mapDatabaseFailure(err)
	}
	return result, nil
}

func (service *Service) GetRequest(
	ctx context.Context,
	operator Operator,
	requestID uuid.UUID,
) (Request, error) {
	if requestID == uuid.Nil {
		return Request{}, invalidFailure("Break-glass request id is required")
	}
	tx, err := service.begin(ctx, operator)
	if err != nil {
		return Request{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := getRequestByID(ctx, tx, requestID)
	if err != nil {
		return Request{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Request{}, mapDatabaseFailure(err)
	}
	return result, nil
}

func (service *Service) ReadRequestContent(
	ctx context.Context,
	operator Operator,
	grantID uuid.UUID,
) (RequestContent, error) {
	if grantID == uuid.Nil {
		return RequestContent{}, invalidFailure("Break-glass grant id is required")
	}
	tx, err := service.begin(ctx, operator)
	if err != nil {
		return RequestContent{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var (
		authorized                  bool
		outcome                     string
		eventID, organizationID     pgtype.UUID
		projectID, jobID, requestID pgtype.UUID
		grantExpiresAt              pgtype.Timestamptz
		requestContent              []byte
	)
	err = tx.QueryRow(ctx, `
		SELECT authorized, outcome_code, event_id, organization_id, project_id,
			job_id, request_id, grant_expires_at, request_content
		FROM vela_authorize_break_glass_request_content($1)
	`, grantID).Scan(
		&authorized,
		&outcome,
		&eventID,
		&organizationID,
		&projectID,
		&jobID,
		&requestID,
		&grantExpiresAt,
		&requestContent,
	)
	if err != nil {
		return RequestContent{}, mapDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return RequestContent{}, mapDatabaseFailure(err)
	}
	if !authorized {
		return RequestContent{}, accessFailure(outcome)
	}
	if !eventID.Valid || !organizationID.Valid || !projectID.Valid || !jobID.Valid ||
		!requestID.Valid || !grantExpiresAt.Valid || len(requestContent) == 0 {
		return RequestContent{}, unavailableFailure("Break-glass database is unavailable")
	}
	return RequestContent{
		Target: Target{
			OrganizationID: uuid.UUID(organizationID.Bytes),
			ProjectID:      uuid.UUID(projectID.Bytes),
			JobID:          uuid.UUID(jobID.Bytes),
		},
		RequestContent: append(json.RawMessage(nil), requestContent...),
	}, nil
}

func (service *Service) GetArtifacts(
	ctx context.Context,
	operator Operator,
	grantID uuid.UUID,
) (ArtifactSet, error) {
	if grantID == uuid.Nil {
		return ArtifactSet{}, invalidFailure("Break-glass grant id is required")
	}
	if service == nil || service.signer == nil {
		return ArtifactSet{}, unavailableFailure("Break-glass Artifact signing is unavailable")
	}
	tx, err := service.begin(ctx, operator)
	if err != nil {
		return ArtifactSet{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT authorized, outcome_code, event_id, organization_id, project_id,
			job_id, request_id, grant_expires_at, artifact_set_id,
			retention_expires_at, committed_at, artifact_id, artifact_kind,
			ordinal, object_key, object_version_id, size_bytes, sha256, content_type
		FROM vela_authorize_break_glass_artifacts($1)
	`, grantID)
	if err != nil {
		return ArtifactSet{}, mapDatabaseFailure(err)
	}
	defer rows.Close()
	var (
		result               ArtifactSet
		authorizationEventID uuid.UUID
		grantExpiresAt       time.Time
		authorized           bool
		outcome              string
		readRow              bool
	)
	for rows.Next() {
		var (
			eventID, organizationID, projectID, jobID, requestID pgtype.UUID
			expiresAt, retentionExpiresAt, committedAt           pgtype.Timestamptz
			artifactSetID, artifactID                            pgtype.UUID
			kind, objectKey, objectVersionID, contentType        pgtype.Text
			ordinal                                              pgtype.Int4
			sizeBytes                                            pgtype.Int8
			digest                                               []byte
		)
		if err := rows.Scan(
			&authorized,
			&outcome,
			&eventID,
			&organizationID,
			&projectID,
			&jobID,
			&requestID,
			&expiresAt,
			&artifactSetID,
			&retentionExpiresAt,
			&committedAt,
			&artifactID,
			&kind,
			&ordinal,
			&objectKey,
			&objectVersionID,
			&sizeBytes,
			&digest,
			&contentType,
		); err != nil {
			return ArtifactSet{}, mapDatabaseFailure(err)
		}
		readRow = true
		if !authorized {
			continue
		}
		if !eventID.Valid || !organizationID.Valid || !projectID.Valid || !jobID.Valid ||
			!requestID.Valid || !expiresAt.Valid || !artifactSetID.Valid ||
			!retentionExpiresAt.Valid || !committedAt.Valid || !artifactID.Valid ||
			!kind.Valid || !ordinal.Valid || !objectKey.Valid || !objectVersionID.Valid ||
			!sizeBytes.Valid || len(digest) != sha256.Size || !contentType.Valid {
			return ArtifactSet{}, unavailableFailure("Break-glass database is unavailable")
		}
		if result.ID == uuid.Nil {
			result = ArtifactSet{
				ID: uuid.UUID(artifactSetID.Bytes),
				Target: Target{
					OrganizationID: uuid.UUID(organizationID.Bytes),
					ProjectID:      uuid.UUID(projectID.Bytes),
					JobID:          uuid.UUID(jobID.Bytes),
				},
				RetentionExpiresAt: retentionExpiresAt.Time,
				CommittedAt:        committedAt.Time,
			}
			authorizationEventID = uuid.UUID(eventID.Bytes)
			grantExpiresAt = expiresAt.Time
		} else if result.ID != uuid.UUID(artifactSetID.Bytes) ||
			authorizationEventID != uuid.UUID(eventID.Bytes) ||
			!grantExpiresAt.Equal(expiresAt.Time) {
			return ArtifactSet{}, unavailableFailure("Break-glass database is unavailable")
		}
		var artifactDigest [sha256.Size]byte
		copy(artifactDigest[:], digest)
		result.Artifacts = append(result.Artifacts, Artifact{
			ID:              uuid.UUID(artifactID.Bytes),
			Kind:            kind.String,
			Ordinal:         ordinal.Int32,
			SizeBytes:       sizeBytes.Int64,
			SHA256:          artifactDigest,
			ContentType:     contentType.String,
			objectKey:       objectKey.String,
			objectVersionID: objectVersionID.String,
		})
	}
	if err := rows.Err(); err != nil {
		return ArtifactSet{}, mapDatabaseFailure(err)
	}
	if !readRow {
		return ArtifactSet{}, unavailableFailure("Break-glass database is unavailable")
	}
	if err := tx.Commit(ctx); err != nil {
		return ArtifactSet{}, mapDatabaseFailure(err)
	}
	if !authorized {
		return ArtifactSet{}, accessFailure(outcome)
	}
	if result.ID == uuid.Nil || len(result.Artifacts) == 0 || authorizationEventID == uuid.Nil ||
		grantExpiresAt.IsZero() {
		return ArtifactSet{}, unavailableFailure("Break-glass database is unavailable")
	}
	for index := range result.Artifacts {
		artifact := &result.Artifacts[index]
		signed, signErr := service.signer.PresignExactVersionUntil(
			ctx, artifact.objectKey, artifact.objectVersionID, grantExpiresAt,
		)
		if signErr != nil {
			if _, recordErr := service.recordArtifactDelivery(
				ctx, operator, authorizationEventID, false,
			); recordErr != nil {
				return ArtifactSet{}, unavailableFailure(
					"Break-glass Artifact signing evidence is unavailable",
				)
			}
			return ArtifactSet{}, unavailableFailure("Break-glass Artifact signing is unavailable")
		}
		if signed.URL == "" || signed.IssuedAt.IsZero() ||
			!signed.ExpiresAt.After(signed.IssuedAt) || signed.ExpiresAt.After(grantExpiresAt) ||
			signed.ExpiresAt.Sub(signed.IssuedAt) > artifactstore.MaxSignedGETTTL {
			_, _ = service.recordArtifactDelivery(ctx, operator, authorizationEventID, false)
			return ArtifactSet{}, unavailableFailure("Break-glass Artifact signing is unavailable")
		}
		artifact.DownloadURL = signed.URL
		artifact.DownloadURLExpiresAt = signed.ExpiresAt
	}
	delivered, err := service.recordArtifactDelivery(
		ctx, operator, authorizationEventID, true,
	)
	if err != nil {
		return ArtifactSet{}, err
	}
	if !delivered {
		return ArtifactSet{}, &Failure{
			Code: FailureForbidden, Message: "Break-glass grant became inactive before delivery",
		}
	}
	return result, nil
}

func (service *Service) recordArtifactDelivery(
	ctx context.Context,
	operator Operator,
	authorizationEventID uuid.UUID,
	signingSucceeded bool,
) (bool, error) {
	tx, err := service.begin(ctx, operator)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var delivered bool
	var outcome string
	var eventID uuid.UUID
	if err := tx.QueryRow(ctx, `
		SELECT delivered, outcome_code, event_id
		FROM vela_record_break_glass_artifact_delivery($1, $2)
	`, authorizationEventID, signingSucceeded).Scan(&delivered, &outcome, &eventID); err != nil {
		return false, mapDatabaseFailure(err)
	}
	if eventID == uuid.Nil || outcome == "" {
		return false, unavailableFailure("Break-glass database is unavailable")
	}
	if err := tx.Commit(ctx); err != nil {
		return false, mapDatabaseFailure(err)
	}
	return delivered, nil
}

func (service *Service) begin(
	ctx context.Context,
	operator Operator,
) (pgx.Tx, error) {
	if service == nil || service.pool == nil {
		return nil, unavailableFailure("Break-glass database is unavailable")
	}
	proof := operator.SessionProof()
	defer clear(proof)
	if operator.ID == uuid.Nil || operator.SessionID == uuid.Nil || len(proof) != sha256.Size {
		return nil, &Failure{
			Code: FailureUnauthorized, Message: "active Platform Operator session is required",
		}
	}
	tx, err := service.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, unavailableFailure("Break-glass database is unavailable")
	}
	var operatorID uuid.UUID
	var transactionTime time.Time
	if err := tx.QueryRow(ctx, `
		SELECT operator_id, transaction_time
		FROM vela_set_break_glass_request_context($1, $2)
	`, operator.SessionID, proof).Scan(&operatorID, &transactionTime); err != nil {
		_ = tx.Rollback(ctx)
		return nil, mapDatabaseFailure(err)
	}
	if operatorID != operator.ID || transactionTime.IsZero() {
		_ = tx.Rollback(ctx)
		return nil, &Failure{
			Code: FailureUnauthorized, Message: "Platform Operator session identity changed",
		}
	}
	return tx, nil
}

const breakGlassRequestProjection = `request_id, organization_id, project_id, job_id, scopes,
	reason_code, ticket_reference, requested_duration_seconds,
	requester_operator_id, requested_at, approval_deadline_at,
	grant_id, approver_operator_id, approved_at, expires_at,
	revoked_at, revoked_by_operator_id, state, transaction_time`

func getRequestByID(ctx context.Context, tx pgx.Tx, requestID uuid.UUID) (Request, error) {
	return scanRequest(tx.QueryRow(ctx, `
		SELECT `+breakGlassRequestProjection+`
		FROM vela_get_break_glass_request($1)
	`, requestID))
}

func getRequestByGrantID(ctx context.Context, tx pgx.Tx, grantID uuid.UUID) (Request, error) {
	return scanRequest(tx.QueryRow(ctx, `
		SELECT `+breakGlassRequestProjection+`
		FROM vela_get_break_glass_grant($1)
	`, grantID))
}

func scanRequest(row pgx.Row) (Request, error) {
	var (
		result                                            Request
		scopes                                            []string
		reason, state                                     string
		grantID, approverID, revokedByID                  pgtype.UUID
		approvedAt, expiresAt, revokedAt, transactionTime pgtype.Timestamptz
	)
	err := row.Scan(
		&result.ID,
		&result.OrganizationID,
		&result.ProjectID,
		&result.JobID,
		&scopes,
		&reason,
		&result.TicketReference,
		&result.RequestedDurationSeconds,
		&result.RequesterOperatorID,
		&result.RequestedAt,
		&result.ApprovalDeadlineAt,
		&grantID,
		&approverID,
		&approvedAt,
		&expiresAt,
		&revokedAt,
		&revokedByID,
		&state,
		&transactionTime,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Request{}, &Failure{Code: FailureNotFound, Message: "Break-glass request is not found"}
	}
	if err != nil {
		return Request{}, mapDatabaseFailure(err)
	}
	if !transactionTime.Valid {
		return Request{}, unavailableFailure("Break-glass database is unavailable")
	}
	result.Scopes = make([]Scope, len(scopes))
	for index, scope := range scopes {
		result.Scopes[index] = Scope(scope)
	}
	result.ReasonCode = ReasonCode(reason)
	result.State = State(state)
	result.GrantID = nullableUUID(grantID)
	result.ApproverOperatorID = nullableUUID(approverID)
	result.ApprovedAt = nullableTime(approvedAt)
	result.ExpiresAt = nullableTime(expiresAt)
	result.RevokedAt = nullableTime(revokedAt)
	result.RevokedByOperatorID = nullableUUID(revokedByID)
	return result, nil
}

func normalizeRequest(
	idempotencyKey string,
	input RequestInput,
) (RequestInput, [sha256.Size]byte, error) {
	if input.OrganizationID == uuid.Nil || input.ProjectID == uuid.Nil || input.JobID == uuid.Nil ||
		len(idempotencyKey) < 1 || len(idempotencyKey) > 200 ||
		!idempotencyKeyPattern.MatchString(idempotencyKey) ||
		!ticketReferencePattern.MatchString(input.TicketReference) ||
		input.RequestedDurationSeconds < 60 || input.RequestedDurationSeconds > 3600 ||
		!validReason(input.ReasonCode) || len(input.Scopes) < 1 || len(input.Scopes) > 2 {
		return RequestInput{}, [sha256.Size]byte{}, invalidFailure("invalid Break-glass request")
	}
	seen := make(map[Scope]struct{}, len(input.Scopes))
	for _, scope := range input.Scopes {
		if scope != ScopeRequestContentRead && scope != ScopeArtifactRead {
			return RequestInput{}, [sha256.Size]byte{}, invalidFailure("invalid Break-glass scope")
		}
		if _, duplicate := seen[scope]; duplicate {
			return RequestInput{}, [sha256.Size]byte{}, invalidFailure("duplicate Break-glass scope")
		}
		seen[scope] = struct{}{}
	}
	input.Scopes = append([]Scope(nil), input.Scopes...)
	sort.Slice(input.Scopes, func(left, right int) bool {
		return input.Scopes[left] < input.Scopes[right]
	})
	canonical, err := json.Marshal(struct {
		OrganizationID           uuid.UUID  `json:"organization_id"`
		ProjectID                uuid.UUID  `json:"project_id"`
		JobID                    uuid.UUID  `json:"job_id"`
		Scopes                   []Scope    `json:"scopes"`
		ReasonCode               ReasonCode `json:"reason_code"`
		TicketReference          string     `json:"ticket_reference"`
		RequestedDurationSeconds int32      `json:"requested_duration_seconds"`
	}{
		input.OrganizationID,
		input.ProjectID,
		input.JobID,
		input.Scopes,
		input.ReasonCode,
		input.TicketReference,
		input.RequestedDurationSeconds,
	})
	if err != nil {
		return RequestInput{}, [sha256.Size]byte{}, fmt.Errorf("canonicalize Break-glass request: %w", err)
	}
	return input, sha256.Sum256(canonical), nil
}

func validReason(reason ReasonCode) bool {
	switch reason {
	case ReasonCustomerSupport, ReasonSecurityInvestigation, ReasonServiceRecovery, ReasonLegalResponse:
		return true
	default:
		return false
	}
}

func invalidFailure(message string) error {
	return &Failure{Code: FailureInvalid, Message: message}
}

func unavailableFailure(message string) error {
	return &Failure{Code: FailureUnavailable, Message: message}
}

func mapDatabaseFailure(err error) error {
	var failure *Failure
	if errors.As(err, &failure) {
		return failure
	}
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return unavailableFailure("Break-glass database is unavailable")
	}
	switch postgresError.Code {
	case "22023":
		return &Failure{Code: FailureInvalid, Message: "invalid Break-glass request"}
	case "28000":
		return &Failure{Code: FailureUnauthorized, Message: "Platform Operator session is no longer active"}
	case "42501":
		return &Failure{Code: FailureForbidden, Message: "Break-glass action is forbidden"}
	case "02000":
		return &Failure{Code: FailureNotFound, Message: "Break-glass target is not found"}
	case "55000":
		if postgresError.ConstraintName == "break_glass_grant_inactive" {
			return &Failure{Code: FailureForbidden, Message: "Break-glass grant is no longer active"}
		}
		return &Failure{Code: FailureConflict, Message: "Break-glass request conflicts with existing state"}
	case "23505":
		return &Failure{Code: FailureConflict, Message: "Break-glass request conflicts with existing state"}
	default:
		return unavailableFailure("Break-glass database is unavailable")
	}
}

func accessFailure(outcome string) error {
	if outcome == "NOT_FOUND" || outcome == "TARGET_NOT_FOUND" {
		return &Failure{Code: FailureNotFound, Message: "Break-glass content target is not found"}
	}
	return &Failure{
		Code: FailureForbidden, Message: "Break-glass content access is not authorized",
	}
}

func nullableUUID(value pgtype.UUID) *uuid.UUID {
	if !value.Valid {
		return nil
	}
	converted := uuid.UUID(value.Bytes)
	return &converted
}

func nullableTime(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	converted := value.Time
	return &converted
}
