package identity

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ServicePrincipal struct {
	ID             uuid.UUID
	OrganizationID uuid.UUID
	ProjectID      uuid.UUID
	DisplayName    string
	CreatedAt      time.Time
	DisabledAt     *time.Time
}

type CreateServicePrincipalRequest struct {
	DisplayName string
}

type Credential struct {
	ID                 uuid.UUID
	OrganizationID     uuid.UUID
	ProjectID          uuid.UUID
	ServicePrincipalID uuid.UUID
	Scopes             []string
	ExpiresAt          time.Time
	CreatedAt          time.Time
	RevokedAt          *time.Time
}

type IssueCredentialRequest struct {
	Scopes    []string
	ExpiresAt time.Time
}

type IssuedCredential struct {
	Credential       Credential
	BearerCredential string
}

type AdministrationService struct {
	pool   *pgxpool.Pool
	pepper []byte
}

func NewAdministrationService(
	pool *pgxpool.Pool,
	pepper []byte,
) (*AdministrationService, error) {
	if pool == nil {
		return nil, errors.New("identity Administration service database pool is required")
	}
	if len(pepper) < 32 {
		return nil, errors.New("identity Administration credential pepper must contain at least 32 bytes")
	}
	return &AdministrationService{pool: pool, pepper: append([]byte(nil), pepper...)}, nil
}

func (s *AdministrationService) CreateServicePrincipal(
	ctx context.Context,
	actor Principal,
	projectID uuid.UUID,
	request CreateServicePrincipalRequest,
) (ServicePrincipal, error) {
	if err := authorizeIdentityAdministration(actor, projectID, ScopeServicePrincipalsManage); err != nil {
		return ServicePrincipal{}, err
	}
	displayName := strings.TrimSpace(request.DisplayName)
	displayNameLength := utf8.RuneCountInString(displayName)
	if displayNameLength == 0 || displayNameLength > 200 {
		return ServicePrincipal{}, &AdministrationFailure{
			Code:    AdministrationFailureInvalidRequest,
			Message: "Service Principal display name must contain 1 to 200 characters",
		}
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ServicePrincipal{}, fmt.Errorf("begin Service Principal creation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishIdentityAdministrationContext(
		ctx, tx, actor, ScopeServicePrincipalsManage,
	); err != nil {
		return ServicePrincipal{}, err
	}

	var result ServicePrincipal
	var disabledAt pgtype.Timestamptz
	err = tx.QueryRow(ctx, `
		SELECT principal_id, organization_id, project_id, display_name, created_at, disabled_at
		FROM vela_create_service_principal($1, $2, $3)
	`, uuid.New(), projectID, displayName).Scan(
		&result.ID,
		&result.OrganizationID,
		&result.ProjectID,
		&result.DisplayName,
		&result.CreatedAt,
		&disabledAt,
	)
	if err != nil {
		return ServicePrincipal{}, mapAdministrationDatabaseFailure(err)
	}
	if disabledAt.Valid {
		value := disabledAt.Time
		result.DisabledAt = &value
	}
	if err := tx.Commit(ctx); err != nil {
		return ServicePrincipal{}, fmt.Errorf("commit Service Principal creation: %w", err)
	}
	return result, nil
}

func (s *AdministrationService) ListServicePrincipals(
	ctx context.Context,
	actor Principal,
	projectID uuid.UUID,
	limit int32,
) ([]ServicePrincipal, error) {
	if err := authorizeIdentityAdministration(actor, projectID, ScopeServicePrincipalsRead); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 100 {
		return nil, &AdministrationFailure{
			Code:    AdministrationFailureInvalidRequest,
			Message: "Service Principal list limit must be between 1 and 100",
		}
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin Service Principal list transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishIdentityAdministrationContext(
		ctx, tx, actor, ScopeServicePrincipalsRead,
	); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT principal_id, organization_id, project_id, display_name, created_at, disabled_at
		FROM vela_list_service_principals($1, $2)
	`, projectID, limit)
	if err != nil {
		return nil, mapAdministrationDatabaseFailure(err)
	}
	defer rows.Close()
	results := make([]ServicePrincipal, 0)
	for rows.Next() {
		var result ServicePrincipal
		var disabledAt pgtype.Timestamptz
		if err := rows.Scan(
			&result.ID,
			&result.OrganizationID,
			&result.ProjectID,
			&result.DisplayName,
			&result.CreatedAt,
			&disabledAt,
		); err != nil {
			return nil, fmt.Errorf("read Service Principal list: %w", err)
		}
		if disabledAt.Valid {
			value := disabledAt.Time
			result.DisabledAt = &value
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list Service Principals: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Service Principal list: %w", err)
	}
	return results, nil
}

func (s *AdministrationService) IssueCredential(
	ctx context.Context,
	actor Principal,
	projectID, servicePrincipalID uuid.UUID,
	request IssueCredentialRequest,
) (IssuedCredential, error) {
	if err := authorizeIdentityAdministration(actor, projectID, ScopeServicePrincipalsManage); err != nil {
		return IssuedCredential{}, err
	}
	scopes, err := validateServiceCredentialScopes(request.Scopes)
	if err != nil {
		return IssuedCredential{}, err
	}
	now := time.Now().UTC()
	if servicePrincipalID == uuid.Nil || !request.ExpiresAt.After(now) ||
		request.ExpiresAt.After(now.Add(366*24*time.Hour)) {
		return IssuedCredential{}, &AdministrationFailure{
			Code:    AdministrationFailureInvalidRequest,
			Message: "Service Credential expiry must be in the future and within 366 days",
		}
	}

	credentialID := uuid.New()
	secret := make([]byte, credentialBytes)
	if _, err := rand.Read(secret); err != nil {
		return IssuedCredential{}, fmt.Errorf("generate Service Credential secret: %w", err)
	}
	defer clear(secret)
	digester := hmac.New(sha256.New, s.pepper)
	_, _ = digester.Write(secret)
	secretDigest := digester.Sum(nil)
	defer clear(secretDigest)
	bearerCredential := credentialPrefix + credentialID.String() + "." +
		base64.RawURLEncoding.EncodeToString(secret)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return IssuedCredential{}, fmt.Errorf("begin Service Credential issue transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishIdentityAdministrationContext(
		ctx, tx, actor, ScopeServicePrincipalsManage,
	); err != nil {
		return IssuedCredential{}, err
	}
	credential, err := scanCredential(tx.QueryRow(ctx, `
		SELECT credential_id, organization_id, project_id, principal_id,
			scopes, expires_at, created_at, revoked_at
		FROM vela_issue_service_credential($1, $2, $3, $4, $5, $6)
	`,
		credentialID,
		projectID,
		servicePrincipalID,
		secretDigest,
		scopes,
		request.ExpiresAt,
	))
	if err != nil {
		return IssuedCredential{}, mapAdministrationDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return IssuedCredential{}, fmt.Errorf("commit Service Credential issue: %w", err)
	}
	return IssuedCredential{
		Credential: credential, BearerCredential: bearerCredential,
	}, nil
}

func (s *AdministrationService) ListCredentials(
	ctx context.Context,
	actor Principal,
	projectID, servicePrincipalID uuid.UUID,
	limit int32,
) ([]Credential, error) {
	if err := authorizeIdentityAdministration(actor, projectID, ScopeServicePrincipalsRead); err != nil {
		return nil, err
	}
	if servicePrincipalID == uuid.Nil || limit < 1 || limit > 100 {
		return nil, &AdministrationFailure{
			Code:    AdministrationFailureInvalidRequest,
			Message: "Service Credential list request is invalid",
		}
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin Service Credential list transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishIdentityAdministrationContext(
		ctx, tx, actor, ScopeServicePrincipalsRead,
	); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT credential_id, organization_id, project_id, principal_id,
			scopes, expires_at, created_at, revoked_at
		FROM vela_list_service_credentials($1, $2, $3)
	`, projectID, servicePrincipalID, limit)
	if err != nil {
		return nil, mapAdministrationDatabaseFailure(err)
	}
	defer rows.Close()
	results := make([]Credential, 0)
	for rows.Next() {
		credential, err := scanCredential(rows)
		if err != nil {
			return nil, fmt.Errorf("read Service Credential list: %w", err)
		}
		results = append(results, credential)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list Service Credentials: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Service Credential list: %w", err)
	}
	return results, nil
}

func (s *AdministrationService) RevokeCredential(
	ctx context.Context,
	actor Principal,
	projectID, servicePrincipalID, credentialID uuid.UUID,
) (Credential, error) {
	if err := authorizeIdentityAdministration(actor, projectID, ScopeServicePrincipalsManage); err != nil {
		return Credential{}, err
	}
	if servicePrincipalID == uuid.Nil || credentialID == uuid.Nil {
		return Credential{}, &AdministrationFailure{
			Code:    AdministrationFailureInvalidRequest,
			Message: "Service Principal and Credential ids are required",
		}
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Credential{}, fmt.Errorf("begin Service Credential revocation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishIdentityAdministrationContext(
		ctx, tx, actor, ScopeServicePrincipalsManage,
	); err != nil {
		return Credential{}, err
	}
	credential, err := scanCredential(tx.QueryRow(ctx, `
		SELECT credential_id, organization_id, project_id, principal_id,
			scopes, expires_at, created_at, revoked_at
		FROM vela_revoke_service_credential($1, $2, $3)
	`, projectID, servicePrincipalID, credentialID))
	if err != nil {
		return Credential{}, mapAdministrationDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Credential{}, fmt.Errorf("commit Service Credential revocation: %w", err)
	}
	return credential, nil
}

func (s *AdministrationService) DisableServicePrincipal(
	ctx context.Context,
	actor Principal,
	projectID, servicePrincipalID uuid.UUID,
) (ServicePrincipal, error) {
	if err := authorizeIdentityAdministration(actor, projectID, ScopeServicePrincipalsManage); err != nil {
		return ServicePrincipal{}, err
	}
	if servicePrincipalID == uuid.Nil {
		return ServicePrincipal{}, &AdministrationFailure{
			Code:    AdministrationFailureInvalidRequest,
			Message: "Service Principal id is required",
		}
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ServicePrincipal{}, fmt.Errorf("begin Service Principal disable transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishIdentityAdministrationContext(
		ctx, tx, actor, ScopeServicePrincipalsManage,
	); err != nil {
		return ServicePrincipal{}, err
	}
	result, err := scanServicePrincipal(tx.QueryRow(ctx, `
		SELECT principal_id, organization_id, project_id, display_name, created_at, disabled_at
		FROM vela_disable_service_principal($1, $2)
	`, projectID, servicePrincipalID))
	if err != nil {
		return ServicePrincipal{}, mapAdministrationDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ServicePrincipal{}, fmt.Errorf("commit Service Principal disable: %w", err)
	}
	return result, nil
}

type credentialScanner interface {
	Scan(...any) error
}

func scanServicePrincipal(row credentialScanner) (ServicePrincipal, error) {
	var result ServicePrincipal
	var disabledAt pgtype.Timestamptz
	err := row.Scan(
		&result.ID,
		&result.OrganizationID,
		&result.ProjectID,
		&result.DisplayName,
		&result.CreatedAt,
		&disabledAt,
	)
	if disabledAt.Valid {
		value := disabledAt.Time
		result.DisabledAt = &value
	}
	return result, err
}

func scanCredential(row credentialScanner) (Credential, error) {
	var credential Credential
	var revokedAt pgtype.Timestamptz
	err := row.Scan(
		&credential.ID,
		&credential.OrganizationID,
		&credential.ProjectID,
		&credential.ServicePrincipalID,
		&credential.Scopes,
		&credential.ExpiresAt,
		&credential.CreatedAt,
		&revokedAt,
	)
	if revokedAt.Valid {
		value := revokedAt.Time
		credential.RevokedAt = &value
	}
	return credential, err
}

func validateServiceCredentialScopes(scopes []string) ([]string, error) {
	allowed := map[string]struct{}{
		ScopeJobsSubmit: {}, ScopeJobsRead: {}, ScopeJobsCancel: {},
		ScopeArtifactsRead: {}, ScopeWebhooksManage: {}, ScopeWebhooksRead: {},
	}
	if len(scopes) == 0 || len(scopes) > len(allowed) {
		return nil, &AdministrationFailure{
			Code:    AdministrationFailureInvalidRequest,
			Message: "Service Credential requires one or more allowed scopes",
		}
	}
	validated := append([]string(nil), scopes...)
	seen := make(map[string]struct{}, len(validated))
	for _, scope := range validated {
		if _, ok := allowed[scope]; !ok {
			return nil, &AdministrationFailure{
				Code:    AdministrationFailureInvalidRequest,
				Message: "Service Credential contains an unknown scope",
			}
		}
		if _, duplicate := seen[scope]; duplicate {
			return nil, &AdministrationFailure{
				Code:    AdministrationFailureInvalidRequest,
				Message: "Service Credential scopes must be unique",
			}
		}
		seen[scope] = struct{}{}
	}
	sort.Strings(validated)
	return validated, nil
}

type AdministrationFailureCode string

const (
	AdministrationFailureUnauthorized   AdministrationFailureCode = "unauthorized"
	AdministrationFailureForbidden      AdministrationFailureCode = "forbidden"
	AdministrationFailureInvalidRequest AdministrationFailureCode = "invalid_request"
	AdministrationFailureConflict       AdministrationFailureCode = "conflict"
	AdministrationFailureNotFound       AdministrationFailureCode = "not_found"
)

type AdministrationFailure struct {
	Code    AdministrationFailureCode
	Message string
}

func (f *AdministrationFailure) Error() string {
	return f.Message
}

func authorizeIdentityAdministration(actor Principal, projectID uuid.UUID, requiredScope string) error {
	if actor.CredentialID == uuid.Nil {
		return &AdministrationFailure{
			Code: AdministrationFailureUnauthorized, Message: "valid bearer credential is required",
		}
	}
	if actor.Kind != PrincipalKindHuman || actor.ProjectID != projectID || !actor.HasScope(requiredScope) {
		return &AdministrationFailure{
			Code: AdministrationFailureForbidden, Message: "Human ProjectAdmin authorization is required",
		}
	}
	return nil
}

func establishIdentityAdministrationContext(
	ctx context.Context,
	tx pgx.Tx,
	actor Principal,
	requiredScope string,
) error {
	var organizationID, projectID, principalID uuid.UUID
	var transactionTime time.Time
	err := tx.QueryRow(ctx, `
		SELECT organization_id, project_id, principal_id, transaction_time
		FROM vela_set_identity_admin_context($1, $2, $3)
	`, actor.CredentialID, actor.RequestContextProof(), requiredScope).Scan(
		&organizationID, &projectID, &principalID, &transactionTime,
	)
	if err != nil {
		return mapAdministrationDatabaseFailure(err)
	}
	if organizationID != actor.OrganizationID || projectID != actor.ProjectID ||
		principalID != actor.PrincipalID {
		return errors.New("identity administration context does not match authenticated Principal")
	}
	return nil
}

func mapAdministrationDatabaseFailure(err error) error {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return err
	}
	switch postgresError.Code {
	case "28000":
		return &AdministrationFailure{
			Code: AdministrationFailureUnauthorized, Message: "credential is no longer active",
		}
	case "42501":
		return &AdministrationFailure{
			Code: AdministrationFailureForbidden, Message: "Human ProjectAdmin authorization is required",
		}
	case "22023", "22001", "23502", "23503", "23514", "22P02":
		return &AdministrationFailure{
			Code: AdministrationFailureInvalidRequest, Message: "identity administration request is invalid",
		}
	case "23505":
		return &AdministrationFailure{
			Code: AdministrationFailureConflict, Message: "identity administration resource already exists",
		}
	case "P0002":
		return &AdministrationFailure{
			Code: AdministrationFailureNotFound, Message: "Service Principal is not visible",
		}
	default:
		return err
	}
}
