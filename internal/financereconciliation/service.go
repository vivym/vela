package financereconciliation

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Kind string

const (
	KindSettlementPosted           Kind = "SETTLEMENT_POSTED"
	KindCreditAdjustmentPosted     Kind = "CREDIT_ADJUSTMENT_POSTED"
	KindContractCreditLimitChanged Kind = "CONTRACT_CREDIT_LIMIT_CHANGED"
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

type Identity struct {
	PrincipalID    uuid.UUID
	StableID       string
	TLSURIIdentity string
}

type Request struct {
	IdempotencyKey           string
	SourceSequence           int64
	OrganizationID           uuid.UUID
	Kind                     Kind
	Currency                 string
	SettlementMinor          *int64
	CreditAdjustmentMinor    *int64
	ContractCreditLimitMinor *int64
	ExternalReference        string
	EffectiveAt              time.Time
}

type Result struct {
	RecordID                 uuid.UUID
	Replayed                 bool
	OrganizationID           uuid.UUID
	Kind                     Kind
	Currency                 string
	ContractCreditLimitMinor int64
	UnsettledPostedMinor     int64
	AccountVersion           int64
	PostedAt                 time.Time
}

type Service struct {
	pool     *pgxpool.Pool
	identity Identity
}

func NewService(ctx context.Context, pool *pgxpool.Pool) (*Service, error) {
	if pool == nil {
		return nil, errors.New("finance reconciliation database pool is required")
	}
	var identity Identity
	if err := pool.QueryRow(ctx, `
		SELECT principal_id, stable_id, tls_uri_identity
		FROM vela_get_finance_reconciliation_identity()
	`).Scan(&identity.PrincipalID, &identity.StableID, &identity.TLSURIIdentity); err != nil {
		return nil, fmt.Errorf("resolve Finance Reconciliation Principal: %w", err)
	}
	if identity.PrincipalID == uuid.Nil || !validBoundedText(identity.StableID, 200) ||
		!validSPIFFEIdentity(identity.TLSURIIdentity) {
		return nil, errors.New("finance reconciliation Principal identity is invalid")
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
		return Result{}, errors.New("finance reconciliation is not configured")
	}
	if err := validateRequest(request); err != nil {
		return Result{}, err
	}
	var result Result
	var kind string
	err := s.pool.QueryRow(ctx, `
		SELECT record_id, replayed, organization_id, kind::text, currency,
			contract_credit_limit_minor, unsettled_posted_minor,
			account_version, posted_at
		FROM vela_apply_finance_reconciliation(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		)
	`,
		uuid.New(),
		request.IdempotencyKey,
		request.SourceSequence,
		request.OrganizationID,
		string(request.Kind),
		request.Currency,
		request.SettlementMinor,
		request.CreditAdjustmentMinor,
		request.ContractCreditLimitMinor,
		request.ExternalReference,
		request.EffectiveAt.UTC(),
	).Scan(
		&result.RecordID,
		&result.Replayed,
		&result.OrganizationID,
		&kind,
		&result.Currency,
		&result.ContractCreditLimitMinor,
		&result.UnsettledPostedMinor,
		&result.AccountVersion,
		&result.PostedAt,
	)
	if err != nil {
		return Result{}, mapDatabaseFailure(err)
	}
	result.Kind = Kind(kind)
	return result, nil
}

func validateRequest(request Request) error {
	if !validBoundedText(request.IdempotencyKey, 500) ||
		!validBoundedText(request.ExternalReference, 500) ||
		request.SourceSequence <= 0 || request.OrganizationID == uuid.Nil ||
		len(request.Currency) != 3 || request.EffectiveAt.IsZero() ||
		request.EffectiveAt.Nanosecond()%int(time.Microsecond) != 0 {
		return &Failure{Code: FailureInvalid, Message: "Finance Reconciliation input is invalid"}
	}
	for _, character := range request.Currency {
		if character < 'A' || character > 'Z' {
			return &Failure{Code: FailureInvalid, Message: "Finance Reconciliation currency is invalid"}
		}
	}
	validAmount := request.Kind == KindSettlementPosted && request.SettlementMinor != nil &&
		*request.SettlementMinor > 0 && request.CreditAdjustmentMinor == nil &&
		request.ContractCreditLimitMinor == nil
	validAmount = validAmount ||
		(request.Kind == KindCreditAdjustmentPosted && request.SettlementMinor == nil &&
			request.CreditAdjustmentMinor != nil && *request.CreditAdjustmentMinor != 0 &&
			request.ContractCreditLimitMinor == nil)
	validAmount = validAmount ||
		(request.Kind == KindContractCreditLimitChanged && request.SettlementMinor == nil &&
			request.CreditAdjustmentMinor == nil && request.ContractCreditLimitMinor != nil &&
			*request.ContractCreditLimitMinor >= 0)
	if !validAmount {
		return &Failure{Code: FailureInvalid, Message: "Finance Reconciliation kind or amount is invalid"}
	}
	return nil
}

func validBoundedText(value string, maximum int) bool {
	if value == "" || len(value) > maximum || strings.TrimSpace(value) != value {
		return false
	}
	return strings.IndexFunc(value, unicode.IsControl) < 0
}

func validSPIFFEIdentity(value string) bool {
	if !validBoundedText(value, 500) || strings.IndexFunc(value, unicode.IsSpace) >= 0 {
		return false
	}
	identity, err := url.Parse(value)
	return err == nil && identity.Scheme == "spiffe" && identity.Host != "" &&
		identity.User == nil && identity.RawQuery == "" && identity.Fragment == ""
}

func mapDatabaseFailure(err error) error {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return err
	}
	switch postgresError.Code {
	case "22023":
		return &Failure{Code: FailureInvalid, Message: "Finance Reconciliation input is invalid"}
	case "28000":
		return &Failure{Code: FailureUnauthorized, Message: "Finance Reconciliation identity is inactive"}
	case "P0002":
		return &Failure{Code: FailureNotFound, Message: "Customer Organization credit account is not found"}
	case "P0001", "23505", "23514", "55000":
		return &Failure{Code: FailureConflict, Message: "Finance Reconciliation conflicts with committed ledger state"}
	default:
		return err
	}
}
