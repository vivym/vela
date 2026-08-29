package organizationreporting

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/identity"
)

type FailureCode string

const (
	FailureUnauthorized FailureCode = "unauthorized"
	FailureForbidden    FailureCode = "forbidden"
	FailureInvalid      FailureCode = "invalid_request"
	FailureNotFound     FailureCode = "not_found"
)

type Failure struct {
	Code    FailureCode
	Message string
}

func (f *Failure) Error() string {
	return f.Message
}

type CreditSummary struct {
	OrganizationID           uuid.UUID
	Currency                 string
	ContractCreditLimitMinor int64
	ReservedMinor            int64
	UnsettledPostedMinor     int64
	AvailableMinor           int64
	Version                  int64
	UpdatedAt                time.Time
}

type Charge struct {
	ChargeID         uuid.UUID
	ProjectID        uuid.UUID
	JobID            uuid.UUID
	Reason           string
	AmountMinor      int64
	Currency         string
	PostedAt         time.Time
	InvoiceReference *string
	LineReference    *string
	ExportedAt       *time.Time
}

type CreateSettlementContactRequest struct {
	DisplayName string
	Email       string
}

type SettlementContact struct {
	ID                    uuid.UUID
	OrganizationID        uuid.UUID
	DisplayName           string
	Email                 string
	CreatedByPrincipalID  uuid.UUID
	CreatedAt             time.Time
	DisabledAt            *time.Time
	DisabledByPrincipalID *uuid.UUID
}

type UsageAggregate struct {
	TotalJobs               int64
	QueuedJobs              int64
	AssignedJobs            int64
	RunningJobs             int64
	FinalizingJobs          int64
	RetryWaitJobs           int64
	CancelingJobs           int64
	SucceededJobs           int64
	FailedJobs              int64
	CanceledJobs            int64
	QuotedAmountMinor       int64
	PostedChargeAmountMinor int64
}

type ProjectUsage struct {
	ProjectID uuid.UUID
	UsageAggregate
}

type UsageSummary struct {
	OrganizationID uuid.UUID
	From           time.Time
	To             time.Time
	Currency       string
	Total          UsageAggregate
	Projects       []ProjectUsage
}

type AuditEvent struct {
	EventID          uuid.UUID
	Source           string
	Action           string
	Scope            *string
	OutcomeCode      *string
	ProjectID        *uuid.UUID
	ActorPrincipalID uuid.UUID
	ActorSessionID   uuid.UUID
	TargetKind       string
	TargetID         uuid.UUID
	CreatedAt        time.Time
}

type Service struct {
	billingPool       *pgxpool.Pool
	auditPool         *pgxpool.Pool
	extendedAuditPool *pgxpool.Pool
}

func NewService(
	billingPool, auditPool, extendedAuditPool *pgxpool.Pool,
) (*Service, error) {
	if billingPool == nil {
		return nil, errors.New("organization billing request database pool is required")
	}
	if auditPool == nil {
		return nil, errors.New("organization audit request database pool is required")
	}
	if extendedAuditPool == nil {
		return nil, errors.New("extended Organization audit request database pool is required")
	}
	return &Service{
		billingPool:       billingPool,
		auditPool:         auditPool,
		extendedAuditPool: extendedAuditPool,
	}, nil
}

func (s *Service) GetCreditSummary(
	ctx context.Context,
	actor identity.Principal,
	organizationID uuid.UUID,
) (CreditSummary, error) {
	if err := authorizeOrganization(actor, organizationID, identity.ScopeOrganizationBillingRead); err != nil {
		return CreditSummary{}, err
	}
	if s == nil || s.billingPool == nil {
		return CreditSummary{}, errors.New("organization billing reporting is not configured")
	}
	tx, err := s.billingPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return CreditSummary{}, fmt.Errorf("begin Organization credit summary transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishOrganizationContext(
		ctx, tx, actor, identity.ScopeOrganizationBillingRead,
	); err != nil {
		return CreditSummary{}, err
	}
	var summary CreditSummary
	err = tx.QueryRow(ctx, `
		SELECT organization_id, currency, contract_credit_limit_minor,
			reserved_minor, unsettled_posted_minor, available_minor, version, updated_at
		FROM vela_get_organization_credit_summary($1)
	`, organizationID).Scan(
		&summary.OrganizationID,
		&summary.Currency,
		&summary.ContractCreditLimitMinor,
		&summary.ReservedMinor,
		&summary.UnsettledPostedMinor,
		&summary.AvailableMinor,
		&summary.Version,
		&summary.UpdatedAt,
	)
	if err != nil {
		return CreditSummary{}, mapDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return CreditSummary{}, fmt.Errorf("commit Organization credit summary: %w", err)
	}
	return summary, nil
}

func (s *Service) ListCharges(
	ctx context.Context,
	actor identity.Principal,
	organizationID uuid.UUID,
	limit int32,
) ([]Charge, error) {
	if err := authorizeOrganization(actor, organizationID, identity.ScopeOrganizationBillingRead); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 100 {
		return nil, &Failure{
			Code:    FailureInvalid,
			Message: "Charge list limit must be between 1 and 100",
		}
	}
	if s == nil || s.billingPool == nil {
		return nil, errors.New("organization billing reporting is not configured")
	}
	tx, err := s.billingPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin Organization Charge list transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishOrganizationContext(
		ctx, tx, actor, identity.ScopeOrganizationBillingRead,
	); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT charge_id, project_id, job_id, reason, amount_minor, currency,
			posted_at, external_invoice_reference, external_line_reference, exported_at
		FROM vela_list_organization_charges($1, $2)
	`, organizationID, limit)
	if err != nil {
		return nil, mapDatabaseFailure(err)
	}
	defer rows.Close()
	charges := make([]Charge, 0)
	for rows.Next() {
		var charge Charge
		var invoiceReference, lineReference pgtype.Text
		var exportedAt pgtype.Timestamptz
		if err := rows.Scan(
			&charge.ChargeID,
			&charge.ProjectID,
			&charge.JobID,
			&charge.Reason,
			&charge.AmountMinor,
			&charge.Currency,
			&charge.PostedAt,
			&invoiceReference,
			&lineReference,
			&exportedAt,
		); err != nil {
			return nil, fmt.Errorf("read Organization Charge list: %w", err)
		}
		if invoiceReference.Valid {
			charge.InvoiceReference = &invoiceReference.String
		}
		if lineReference.Valid {
			charge.LineReference = &lineReference.String
		}
		if exportedAt.Valid {
			charge.ExportedAt = &exportedAt.Time
		}
		charges = append(charges, charge)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list Organization Charges: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Organization Charge list: %w", err)
	}
	return charges, nil
}

func (s *Service) CreateSettlementContact(
	ctx context.Context,
	actor identity.Principal,
	organizationID uuid.UUID,
	request CreateSettlementContactRequest,
) (SettlementContact, error) {
	if err := authorizeOrganization(
		actor, organizationID, identity.ScopeOrganizationBillingContactsManage,
	); err != nil {
		return SettlementContact{}, err
	}
	displayName, email, err := normalizeSettlementContact(request)
	if err != nil {
		return SettlementContact{}, err
	}
	if s == nil || s.billingPool == nil {
		return SettlementContact{}, errors.New("organization billing reporting is not configured")
	}
	tx, err := s.billingPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return SettlementContact{}, fmt.Errorf("begin settlement contact creation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishOrganizationContext(
		ctx, tx, actor, identity.ScopeOrganizationBillingContactsManage,
	); err != nil {
		return SettlementContact{}, err
	}
	contact, err := scanSettlementContact(tx.QueryRow(ctx, `
		SELECT contact_id, organization_id, display_name, normalized_email,
			created_by_principal_id, created_at, disabled_at, disabled_by_principal_id
		FROM vela_create_settlement_contact($1, $2, $3, $4)
	`, uuid.New(), organizationID, displayName, email))
	if err != nil {
		return SettlementContact{}, mapDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return SettlementContact{}, fmt.Errorf("commit settlement contact creation: %w", err)
	}
	return contact, nil
}

func (s *Service) ListSettlementContacts(
	ctx context.Context,
	actor identity.Principal,
	organizationID uuid.UUID,
	limit int32,
) ([]SettlementContact, error) {
	if err := authorizeOrganization(
		actor, organizationID, identity.ScopeOrganizationBillingContactsRead,
	); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 100 {
		return nil, &Failure{
			Code:    FailureInvalid,
			Message: "settlement contact list limit must be between 1 and 100",
		}
	}
	if s == nil || s.billingPool == nil {
		return nil, errors.New("organization billing reporting is not configured")
	}
	tx, err := s.billingPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin settlement contact list: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishOrganizationContext(
		ctx, tx, actor, identity.ScopeOrganizationBillingContactsRead,
	); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT contact_id, organization_id, display_name, normalized_email,
			created_by_principal_id, created_at, disabled_at, disabled_by_principal_id
		FROM vela_list_settlement_contacts($1, $2)
	`, organizationID, limit)
	if err != nil {
		return nil, mapDatabaseFailure(err)
	}
	defer rows.Close()
	contacts := make([]SettlementContact, 0)
	for rows.Next() {
		contact, err := scanSettlementContact(rows)
		if err != nil {
			return nil, fmt.Errorf("read settlement contact list: %w", err)
		}
		contacts = append(contacts, contact)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list settlement contacts: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit settlement contact list: %w", err)
	}
	return contacts, nil
}

func (s *Service) DisableSettlementContact(
	ctx context.Context,
	actor identity.Principal,
	organizationID, contactID uuid.UUID,
) (SettlementContact, error) {
	if err := authorizeOrganization(
		actor, organizationID, identity.ScopeOrganizationBillingContactsManage,
	); err != nil {
		return SettlementContact{}, err
	}
	if contactID == uuid.Nil {
		return SettlementContact{}, &Failure{
			Code:    FailureInvalid,
			Message: "settlement contact id is required",
		}
	}
	if s == nil || s.billingPool == nil {
		return SettlementContact{}, errors.New("organization billing reporting is not configured")
	}
	tx, err := s.billingPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return SettlementContact{}, fmt.Errorf("begin settlement contact disablement: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishOrganizationContext(
		ctx, tx, actor, identity.ScopeOrganizationBillingContactsManage,
	); err != nil {
		return SettlementContact{}, err
	}
	contact, err := scanSettlementContact(tx.QueryRow(ctx, `
		SELECT contact_id, organization_id, display_name, normalized_email,
			created_by_principal_id, created_at, disabled_at, disabled_by_principal_id
		FROM vela_disable_settlement_contact($1, $2)
	`, organizationID, contactID))
	if err != nil {
		return SettlementContact{}, mapDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return SettlementContact{}, fmt.Errorf("commit settlement contact disablement: %w", err)
	}
	return contact, nil
}

func (s *Service) GetUsage(
	ctx context.Context,
	actor identity.Principal,
	organizationID uuid.UUID,
	from, to time.Time,
) (UsageSummary, error) {
	if err := authorizeOrganization(actor, organizationID, identity.ScopeOrganizationUsageRead); err != nil {
		return UsageSummary{}, err
	}
	if !validUTCUsageInterval(from, to) {
		return UsageSummary{}, &Failure{
			Code:    FailureInvalid,
			Message: "usage interval must be UTC, positive, and no greater than 366 days",
		}
	}
	if s == nil {
		return UsageSummary{}, errors.New("organization usage reporting is not configured")
	}
	pool := s.auditPool
	if actor.HasScope(identity.ScopeOrganizationBillingRead) {
		pool = s.billingPool
	}
	if pool == nil {
		return UsageSummary{}, errors.New("organization usage reporting is not configured")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return UsageSummary{}, fmt.Errorf("begin Organization usage transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishOrganizationContext(
		ctx, tx, actor, identity.ScopeOrganizationUsageRead,
	); err != nil {
		return UsageSummary{}, err
	}
	rows, err := tx.Query(ctx, `
		SELECT project_id, total_jobs, queued_jobs, assigned_jobs, running_jobs,
			finalizing_jobs, retry_wait_jobs, canceling_jobs, succeeded_jobs,
			failed_jobs, canceled_jobs, quoted_amount_minor,
			posted_charge_amount_minor, currency
		FROM vela_get_organization_usage($1, $2, $3)
	`, organizationID, from, to)
	if err != nil {
		return UsageSummary{}, mapDatabaseFailure(err)
	}
	defer rows.Close()
	summary := UsageSummary{
		OrganizationID: organizationID,
		From:           from,
		To:             to,
		Projects:       make([]ProjectUsage, 0),
	}
	readTotal := false
	for rows.Next() {
		var projectID pgtype.UUID
		var usage UsageAggregate
		var currency string
		if err := rows.Scan(
			&projectID,
			&usage.TotalJobs,
			&usage.QueuedJobs,
			&usage.AssignedJobs,
			&usage.RunningJobs,
			&usage.FinalizingJobs,
			&usage.RetryWaitJobs,
			&usage.CancelingJobs,
			&usage.SucceededJobs,
			&usage.FailedJobs,
			&usage.CanceledJobs,
			&usage.QuotedAmountMinor,
			&usage.PostedChargeAmountMinor,
			&currency,
		); err != nil {
			return UsageSummary{}, fmt.Errorf("read Organization usage: %w", err)
		}
		if summary.Currency == "" {
			summary.Currency = currency
		} else if summary.Currency != currency {
			return UsageSummary{}, errors.New("organization usage returned mixed currencies")
		}
		if !projectID.Valid {
			if readTotal {
				return UsageSummary{}, errors.New("organization usage returned duplicate totals")
			}
			summary.Total = usage
			readTotal = true
			continue
		}
		summary.Projects = append(summary.Projects, ProjectUsage{
			ProjectID:      uuid.UUID(projectID.Bytes),
			UsageAggregate: usage,
		})
	}
	if err := rows.Err(); err != nil {
		return UsageSummary{}, fmt.Errorf("get Organization usage: %w", err)
	}
	if !readTotal || summary.Currency == "" {
		return UsageSummary{}, errors.New("organization usage returned no total")
	}
	if err := tx.Commit(ctx); err != nil {
		return UsageSummary{}, fmt.Errorf("commit Organization usage: %w", err)
	}
	return summary, nil
}

func (s *Service) ListAuditEvents(
	ctx context.Context,
	actor identity.Principal,
	organizationID uuid.UUID,
	limit int32,
) ([]AuditEvent, error) {
	if err := authorizeOrganization(actor, organizationID, identity.ScopeOrganizationAuditRead); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 100 {
		return nil, &Failure{
			Code:    FailureInvalid,
			Message: "audit event list limit must be between 1 and 100",
		}
	}
	if s == nil || s.extendedAuditPool == nil {
		return nil, errors.New("organization audit reporting is not configured")
	}
	tx, err := s.extendedAuditPool.BeginTx(
		ctx,
		pgx.TxOptions{IsoLevel: pgx.ReadCommitted},
	)
	if err != nil {
		return nil, fmt.Errorf("begin Organization audit event list: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishOrganizationContext(
		ctx, tx, actor, identity.ScopeOrganizationAuditRead,
	); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT event_id, source, action, project_id, actor_principal_id,
			actor_session_id, target_kind, target_id, created_at, scope, outcome_code
		FROM vela_list_organization_audit_events_v3($1, $2)
	`, organizationID, limit)
	if err != nil {
		return nil, mapDatabaseFailure(err)
	}
	defer rows.Close()
	events := make([]AuditEvent, 0)
	for rows.Next() {
		var event AuditEvent
		var projectID pgtype.UUID
		var actorPrincipalID, actorSessionID pgtype.UUID
		var scope, outcomeCode pgtype.Text
		if err := rows.Scan(
			&event.EventID,
			&event.Source,
			&event.Action,
			&projectID,
			&actorPrincipalID,
			&actorSessionID,
			&event.TargetKind,
			&event.TargetID,
			&event.CreatedAt,
			&scope,
			&outcomeCode,
		); err != nil {
			return nil, fmt.Errorf("read Organization audit event list: %w", err)
		}
		if projectID.Valid {
			value := uuid.UUID(projectID.Bytes)
			event.ProjectID = &value
		}
		if actorPrincipalID.Valid {
			event.ActorPrincipalID = uuid.UUID(actorPrincipalID.Bytes)
		}
		if actorSessionID.Valid {
			event.ActorSessionID = uuid.UUID(actorSessionID.Bytes)
		}
		if scope.Valid {
			value := scope.String
			event.Scope = &value
		}
		if outcomeCode.Valid {
			value := outcomeCode.String
			event.OutcomeCode = &value
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list Organization audit events: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Organization audit event list: %w", err)
	}
	return events, nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanSettlementContact(row rowScanner) (SettlementContact, error) {
	var contact SettlementContact
	var disabledAt pgtype.Timestamptz
	var disabledByPrincipalID pgtype.UUID
	if err := row.Scan(
		&contact.ID,
		&contact.OrganizationID,
		&contact.DisplayName,
		&contact.Email,
		&contact.CreatedByPrincipalID,
		&contact.CreatedAt,
		&disabledAt,
		&disabledByPrincipalID,
	); err != nil {
		return SettlementContact{}, err
	}
	if disabledAt.Valid {
		contact.DisabledAt = &disabledAt.Time
	}
	if disabledByPrincipalID.Valid {
		value := uuid.UUID(disabledByPrincipalID.Bytes)
		contact.DisabledByPrincipalID = &value
	}
	return contact, nil
}

func normalizeSettlementContact(
	request CreateSettlementContactRequest,
) (string, string, error) {
	displayName := strings.TrimSpace(request.DisplayName)
	if displayName == "" || utf8.RuneCountInString(displayName) > 200 {
		return "", "", &Failure{
			Code:    FailureInvalid,
			Message: "settlement contact display name must contain 1 to 200 characters",
		}
	}
	if len(request.Email) < 3 || len(request.Email) > 320 ||
		strings.TrimSpace(request.Email) != request.Email ||
		strings.IndexFunc(request.Email, unicode.IsSpace) >= 0 ||
		strings.Count(request.Email, "@") != 1 {
		return "", "", &Failure{
			Code:    FailureInvalid,
			Message: "settlement contact email is invalid",
		}
	}
	at := strings.IndexByte(request.Email, '@')
	if at <= 0 || at == len(request.Email)-1 {
		return "", "", &Failure{
			Code:    FailureInvalid,
			Message: "settlement contact email is invalid",
		}
	}
	return displayName, strings.ToLower(request.Email), nil
}

func validUTCUsageInterval(from, to time.Time) bool {
	if from.IsZero() || to.IsZero() || !from.Before(to) ||
		to.Sub(from) > 366*24*time.Hour {
		return false
	}
	_, fromOffset := from.Zone()
	_, toOffset := to.Zone()
	return fromOffset == 0 && toOffset == 0
}

func authorizeOrganization(
	actor identity.Principal,
	organizationID uuid.UUID,
	requiredScope string,
) error {
	if actor.CredentialID == uuid.Nil {
		return &Failure{Code: FailureUnauthorized, Message: "valid bearer credential is required"}
	}
	if organizationID == uuid.Nil {
		return &Failure{Code: FailureInvalid, Message: "Customer Organization id is required"}
	}
	if actor.Kind != identity.PrincipalKindHuman || actor.OrganizationID != organizationID ||
		actor.ProjectID != uuid.Nil || !actor.HasScope(requiredScope) {
		return &Failure{
			Code:    FailureForbidden,
			Message: "Human Organization reporting authorization is required",
		}
	}
	return nil
}

func establishOrganizationContext(
	ctx context.Context,
	tx pgx.Tx,
	actor identity.Principal,
	requiredScope string,
) error {
	var organizationID, principalID uuid.UUID
	var transactionTime time.Time
	err := tx.QueryRow(ctx, `
		SELECT organization_id, principal_id, transaction_time
		FROM vela_set_organization_identity_admin_context($1, $2, $3)
	`, actor.CredentialID, actor.RequestContextProof(), requiredScope).Scan(
		&organizationID, &principalID, &transactionTime,
	)
	if err != nil {
		return mapDatabaseFailure(err)
	}
	if organizationID != actor.OrganizationID || principalID != actor.PrincipalID {
		return errors.New("organization reporting context does not match authenticated Principal")
	}
	return nil
}

func mapDatabaseFailure(err error) error {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return err
	}
	switch postgresError.Code {
	case "22023":
		return &Failure{Code: FailureInvalid, Message: postgresError.Message}
	case "28000":
		return &Failure{Code: FailureUnauthorized, Message: "credential is no longer active"}
	case "42501":
		return &Failure{Code: FailureForbidden, Message: postgresError.Message}
	case "P0002":
		return &Failure{Code: FailureNotFound, Message: postgresError.Message}
	default:
		return err
	}
}
