package legalhold

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type FailureCode string

const (
	FailureInvalid      FailureCode = "invalid_request"
	FailureUnauthorized FailureCode = "unauthorized"
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

type Service struct {
	pool     *pgxpool.Pool
	identity Identity
}

var reasonCodePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,99}$`)

func NewService(ctx context.Context, pool *pgxpool.Pool) (*Service, error) {
	if pool == nil {
		return nil, errors.New("compliance database pool is required")
	}
	var identity Identity
	if err := pool.QueryRow(ctx, `
		SELECT principal_id, stable_id, tls_uri_identity
		FROM vela_get_compliance_identity()
	`).Scan(&identity.PrincipalID, &identity.StableID, &identity.TLSURIIdentity); err != nil {
		return nil, fmt.Errorf("resolve Compliance Principal: %w", err)
	}
	if identity.PrincipalID == uuid.Nil || !validBoundedText(identity.StableID, 200) ||
		!validURIIdentity(identity.TLSURIIdentity) {
		return nil, errors.New("compliance Principal identity is invalid")
	}
	return &Service{pool: pool, identity: identity}, nil
}

func (s *Service) Identity() Identity {
	if s == nil {
		return Identity{}
	}
	return s.identity
}

func (s *Service) Apply(ctx context.Context, request Request) (Result, error) {
	if s == nil || s.pool == nil || s.identity.PrincipalID == uuid.Nil {
		return Result{}, errors.New("legal hold service is not configured")
	}
	canonical, err := validateRequest(request)
	if err != nil {
		return Result{}, err
	}
	request.RecordClasses = canonical
	classes := make([]string, len(request.RecordClasses))
	for index, class := range request.RecordClasses {
		classes[index] = string(class)
	}
	var scope, organizationID, projectID, jobID, recordClasses any
	if request.Kind == KindHoldPlaced {
		scope = string(request.Scope)
		organizationID = request.OrganizationID
		projectID = nullableUUID(request.ProjectID)
		jobID = nullableUUID(request.JobID)
		recordClasses = classes
	}
	var result Result
	var state, resultScope string
	var resultClasses []string
	if err := s.pool.QueryRow(ctx, `
		SELECT event_id, replayed, hold_id, state::text, scope::text,
			record_classes::text[], recorded_at, released_at
		FROM vela_apply_legal_hold_event(
			$1, $2, $3, $4, $5, $6, $7, $8, $9,
			$10::legal_hold_record_class[], $11, $12, $13
		)
	`,
		uuid.New(), request.IdempotencyKey, request.SourceSequence, request.HoldID,
		string(request.Kind), scope, organizationID, projectID, jobID, recordClasses,
		request.ReasonCode, request.ExternalReference, request.EffectiveAt.UTC(),
	).Scan(
		&result.EventID, &result.Replayed, &result.HoldID, &state, &resultScope,
		&resultClasses, &result.RecordedAt, &result.ReleasedAt,
	); err != nil {
		return Result{}, mapDatabaseFailure(err)
	}
	result.State = State(state)
	result.Scope = Scope(resultScope)
	result.RecordClasses = make([]RecordClass, len(resultClasses))
	for index, class := range resultClasses {
		result.RecordClasses[index] = RecordClass(class)
	}
	return result, nil
}

func validateRequest(request Request) ([]RecordClass, error) {
	if !validBoundedText(request.IdempotencyKey, 500) || request.SourceSequence <= 0 ||
		request.HoldID == uuid.Nil || !reasonCodePattern.MatchString(request.ReasonCode) ||
		!validBoundedText(request.ExternalReference, 500) || request.EffectiveAt.IsZero() ||
		request.EffectiveAt.Nanosecond()%int(time.Microsecond) != 0 {
		return nil, &Failure{Code: FailureInvalid, Message: "Legal Hold event input is invalid"}
	}
	switch request.Kind {
	case KindHoldPlaced:
		classes, ok := canonicalRecordClasses(request.RecordClasses)
		if !ok || request.OrganizationID == uuid.Nil ||
			!validScopeTarget(request.Scope, request.ProjectID, request.JobID) {
			return nil, &Failure{Code: FailureInvalid, Message: "Legal Hold placement input is invalid"}
		}
		return classes, nil
	case KindHoldReleased:
		if request.Scope != "" || request.OrganizationID != uuid.Nil ||
			request.ProjectID != nil || request.JobID != nil || len(request.RecordClasses) != 0 {
			return nil, &Failure{Code: FailureInvalid, Message: "Legal Hold release input is invalid"}
		}
		return nil, nil
	default:
		return nil, &Failure{Code: FailureInvalid, Message: "Legal Hold event kind is invalid"}
	}
}

func validScopeTarget(scope Scope, projectID, jobID *uuid.UUID) bool {
	switch scope {
	case ScopeOrganization:
		return projectID == nil && jobID == nil
	case ScopeProject:
		return projectID != nil && *projectID != uuid.Nil && jobID == nil
	case ScopeJob:
		return projectID != nil && *projectID != uuid.Nil &&
			jobID != nil && *jobID != uuid.Nil
	default:
		return false
	}
}

func nullableUUID(value *uuid.UUID) any {
	if value == nil {
		return nil
	}
	return *value
}

func mapDatabaseFailure(err error) error {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return err
	}
	switch postgresError.Code {
	case "22023":
		return &Failure{Code: FailureInvalid, Message: "Legal Hold event input is invalid"}
	case "28000":
		return &Failure{Code: FailureUnauthorized, Message: "Compliance identity is inactive"}
	case "P0002", "23503":
		return &Failure{Code: FailureNotFound, Message: "Legal Hold target is not found"}
	case "P0001", "23505", "23514", "55000":
		return &Failure{Code: FailureConflict, Message: "Legal Hold conflicts with committed state"}
	default:
		return err
	}
}
