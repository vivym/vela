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

type HumanMember struct {
	ID             uuid.UUID
	OrganizationID uuid.UUID
	OIDCIssuer     string
	OIDCSubject    string
	DisplayName    string
	CreatedAt      time.Time
	DisabledAt     *time.Time
}

type CreateHumanMemberRequest struct {
	OIDCSubject string
	DisplayName string
}

type OrganizationRole string

const (
	OrganizationRoleOrganizationOwner   OrganizationRole = "OrganizationOwner"
	OrganizationRoleBillingAdmin        OrganizationRole = "BillingAdmin"
	OrganizationRoleOrganizationAuditor OrganizationRole = "OrganizationAuditor"
)

type OrganizationRoleAssignment struct {
	OrganizationID        uuid.UUID
	PrincipalID           uuid.UUID
	Role                  OrganizationRole
	Active                bool
	AssignedByPrincipalID uuid.UUID
	AssignedAt            *time.Time
}

type OrganizationMember struct {
	HumanMember
	Roles []OrganizationRole
}

type ProjectRole string

const (
	ProjectRoleProjectAdmin  ProjectRole = "ProjectAdmin"
	ProjectRoleDeveloper     ProjectRole = "Developer"
	ProjectRoleProjectViewer ProjectRole = "ProjectViewer"
)

type ProjectRoleAssignment struct {
	OrganizationID        uuid.UUID
	ProjectID             uuid.UUID
	PrincipalID           uuid.UUID
	Role                  ProjectRole
	Active                bool
	AssignedByPrincipalID uuid.UUID
	AssignedAt            *time.Time
}

type ProjectMember struct {
	ID             uuid.UUID
	OrganizationID uuid.UUID
	ProjectID      uuid.UUID
	DisplayName    string
	DisabledAt     *time.Time
	Roles          []ProjectRole
}

type IssuedCredential struct {
	Credential       Credential
	BearerCredential string
}

type AdministrationService struct {
	pool                *pgxpool.Pool
	humanMembershipPool *pgxpool.Pool
	pepper              []byte
	oidcIssuer          string
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

func NewAdministrationServiceWithOIDCIssuer(
	pool *pgxpool.Pool,
	pepper []byte,
	oidcIssuer string,
) (*AdministrationService, error) {
	service, err := NewAdministrationService(pool, pepper)
	if err != nil {
		return nil, err
	}
	issuer, err := validateOIDCEndpoint("issuer", oidcIssuer, false)
	if err != nil {
		return nil, err
	}
	service.oidcIssuer = issuer
	service.humanMembershipPool = pool
	return service, nil
}

func NewAdministrationServiceWithHumanMembership(
	pool *pgxpool.Pool,
	humanMembershipPool *pgxpool.Pool,
	pepper []byte,
	oidcIssuer string,
) (*AdministrationService, error) {
	if humanMembershipPool == nil {
		return nil, errors.New("human membership Administration database pool is required")
	}
	service, err := NewAdministrationServiceWithOIDCIssuer(pool, pepper, oidcIssuer)
	if err != nil {
		return nil, err
	}
	service.humanMembershipPool = humanMembershipPool
	return service, nil
}

func (s *AdministrationService) CreateHumanMember(
	ctx context.Context,
	actor Principal,
	organizationID uuid.UUID,
	request CreateHumanMemberRequest,
) (HumanMember, error) {
	if err := authorizeOrganizationIdentityAdministration(
		actor, organizationID, ScopeOrganizationMembersManage,
	); err != nil {
		return HumanMember{}, err
	}
	if s == nil || s.oidcIssuer == "" || len(request.OIDCSubject) == 0 ||
		len(request.OIDCSubject) > 500 {
		return HumanMember{}, &AdministrationFailure{
			Code:    AdministrationFailureInvalidRequest,
			Message: "Human member OIDC subject must contain 1 to 500 bytes",
		}
	}
	displayName := "Human member"
	if request.DisplayName != "" {
		displayName = strings.TrimSpace(request.DisplayName)
		if displayName == "" {
			return HumanMember{}, &AdministrationFailure{
				Code:    AdministrationFailureInvalidRequest,
				Message: "Human member display name must contain 1 to 200 characters",
			}
		}
	}
	if utf8.RuneCountInString(displayName) > 200 {
		return HumanMember{}, &AdministrationFailure{
			Code:    AdministrationFailureInvalidRequest,
			Message: "Human member display name must contain 1 to 200 characters",
		}
	}

	tx, err := s.humanMembershipPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return HumanMember{}, fmt.Errorf("begin Human member creation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishOrganizationIdentityAdministrationContext(
		ctx, tx, actor, ScopeOrganizationMembersManage,
	); err != nil {
		return HumanMember{}, err
	}
	result, err := scanHumanMember(tx.QueryRow(ctx, `
		SELECT principal_id, organization_id, oidc_issuer, oidc_subject,
			display_name, created_at, disabled_at
		FROM vela_create_human_member($1, $2, $3, $4, $5)
	`, uuid.New(), organizationID, s.oidcIssuer, request.OIDCSubject, displayName))
	if err != nil {
		return HumanMember{}, mapHumanAdministrationDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return HumanMember{}, fmt.Errorf("commit Human member creation: %w", err)
	}
	return result, nil
}

func (s *AdministrationService) DisableHumanMember(
	ctx context.Context,
	actor Principal,
	organizationID, principalID uuid.UUID,
) (HumanMember, error) {
	if err := authorizeOrganizationIdentityAdministration(
		actor, organizationID, ScopeOrganizationMembersManage,
	); err != nil {
		return HumanMember{}, err
	}
	if principalID == uuid.Nil {
		return HumanMember{}, &AdministrationFailure{
			Code:    AdministrationFailureInvalidRequest,
			Message: "Human member id is required",
		}
	}
	tx, err := s.humanMembershipPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return HumanMember{}, fmt.Errorf("begin Human member disable transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishOrganizationIdentityAdministrationContext(
		ctx, tx, actor, ScopeOrganizationMembersManage,
	); err != nil {
		return HumanMember{}, err
	}
	member, err := scanHumanMember(tx.QueryRow(ctx, `
		SELECT principal_id, organization_id, oidc_issuer, oidc_subject,
			display_name, created_at, disabled_at
		FROM vela_disable_human_member($1, $2)
	`, organizationID, principalID))
	if err != nil {
		return HumanMember{}, mapHumanAdministrationDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return HumanMember{}, fmt.Errorf("commit Human member disable: %w", err)
	}
	return member, nil
}

func (s *AdministrationService) ListHumanMembers(
	ctx context.Context,
	actor Principal,
	organizationID uuid.UUID,
	limit int32,
) ([]OrganizationMember, error) {
	if err := authorizeOrganizationIdentityAdministration(
		actor, organizationID, ScopeOrganizationMembersRead,
	); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 100 {
		return nil, &AdministrationFailure{
			Code:    AdministrationFailureInvalidRequest,
			Message: "Human member list limit must be between 1 and 100",
		}
	}
	tx, err := s.humanMembershipPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin Human member list transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishOrganizationIdentityAdministrationContext(
		ctx, tx, actor, ScopeOrganizationMembersRead,
	); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT principal_id, organization_id, oidc_issuer, oidc_subject,
			display_name, created_at, disabled_at, organization_roles
		FROM vela_list_human_members($1, $2)
	`, organizationID, limit)
	if err != nil {
		return nil, mapHumanAdministrationDatabaseFailure(err)
	}
	defer rows.Close()
	results := make([]OrganizationMember, 0)
	for rows.Next() {
		member, err := scanOrganizationMember(rows)
		if err != nil {
			return nil, fmt.Errorf("read Human member list: %w", err)
		}
		results = append(results, member)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list Human members: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Human member list: %w", err)
	}
	return results, nil
}

func (s *AdministrationService) ListProjectMembers(
	ctx context.Context,
	actor Principal,
	projectID uuid.UUID,
	limit int32,
) ([]ProjectMember, error) {
	if err := authorizeProjectMembershipAdministration(
		actor, projectID, ScopeProjectMembersRead,
	); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 100 {
		return nil, &AdministrationFailure{
			Code:    AdministrationFailureInvalidRequest,
			Message: "Project member list limit must be between 1 and 100",
		}
	}
	tx, err := s.humanMembershipPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin Project member list transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishProjectMembershipAdministrationContext(
		ctx, tx, actor, projectID, ScopeProjectMembersRead,
	); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `
		SELECT principal_id, organization_id, project_id, display_name,
			disabled_at, project_roles
		FROM vela_list_project_members($1, $2)
	`, projectID, limit)
	if err != nil {
		return nil, mapHumanAdministrationDatabaseFailure(err)
	}
	defer rows.Close()
	results := make([]ProjectMember, 0)
	for rows.Next() {
		member, err := scanProjectMember(rows)
		if err != nil {
			return nil, fmt.Errorf("read Project member list: %w", err)
		}
		results = append(results, member)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list Project members: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Project member list: %w", err)
	}
	return results, nil
}

func (s *AdministrationService) AssignOrganizationRole(
	ctx context.Context,
	actor Principal,
	organizationID, principalID uuid.UUID,
	role OrganizationRole,
) (OrganizationRoleAssignment, error) {
	return s.changeOrganizationRole(
		ctx, actor, organizationID, principalID, role, "assign",
	)
}

func (s *AdministrationService) RevokeOrganizationRole(
	ctx context.Context,
	actor Principal,
	organizationID, principalID uuid.UUID,
	role OrganizationRole,
) (OrganizationRoleAssignment, error) {
	return s.changeOrganizationRole(
		ctx, actor, organizationID, principalID, role, "revoke",
	)
}

func (s *AdministrationService) changeOrganizationRole(
	ctx context.Context,
	actor Principal,
	organizationID, principalID uuid.UUID,
	role OrganizationRole,
	operation string,
) (OrganizationRoleAssignment, error) {
	if err := authorizeOrganizationIdentityAdministration(
		actor, organizationID, ScopeOrganizationMembersManage,
	); err != nil {
		return OrganizationRoleAssignment{}, err
	}
	if principalID == uuid.Nil || !validOrganizationRole(role) {
		return OrganizationRoleAssignment{}, &AdministrationFailure{
			Code:    AdministrationFailureInvalidRequest,
			Message: "Human Organization role request is invalid",
		}
	}
	functionName := "vela_assign_organization_role"
	if operation == "revoke" {
		functionName = "vela_revoke_organization_role"
	}
	tx, err := s.humanMembershipPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return OrganizationRoleAssignment{}, fmt.Errorf(
			"begin Human Organization role %s transaction: %w", operation, err,
		)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishOrganizationIdentityAdministrationContext(
		ctx, tx, actor, ScopeOrganizationMembersManage,
	); err != nil {
		return OrganizationRoleAssignment{}, err
	}
	assignment, err := scanOrganizationRoleAssignment(tx.QueryRow(ctx, `
		SELECT organization_id, principal_id, organization_role, active,
			assigned_by_principal_id, assigned_at
		FROM `+functionName+`($1, $2, $3::organization_role)
	`, organizationID, principalID, string(role)))
	if err != nil {
		return OrganizationRoleAssignment{}, mapHumanAdministrationDatabaseFailure(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return OrganizationRoleAssignment{}, fmt.Errorf(
			"commit Human Organization role %s: %w", operation, err,
		)
	}
	return assignment, nil
}

func validOrganizationRole(role OrganizationRole) bool {
	switch role {
	case OrganizationRoleOrganizationOwner,
		OrganizationRoleBillingAdmin,
		OrganizationRoleOrganizationAuditor:
		return true
	default:
		return false
	}
}

func (s *AdministrationService) AssignProjectRole(
	ctx context.Context,
	actor Principal,
	projectID, principalID uuid.UUID,
	role ProjectRole,
) (ProjectRoleAssignment, error) {
	return s.changeProjectRole(ctx, actor, projectID, principalID, role, "assign")
}

func (s *AdministrationService) RevokeProjectRole(
	ctx context.Context,
	actor Principal,
	projectID, principalID uuid.UUID,
	role ProjectRole,
) (ProjectRoleAssignment, error) {
	return s.changeProjectRole(ctx, actor, projectID, principalID, role, "revoke")
}

func (s *AdministrationService) changeProjectRole(
	ctx context.Context,
	actor Principal,
	projectID, principalID uuid.UUID,
	role ProjectRole,
	operation string,
) (ProjectRoleAssignment, error) {
	if err := authorizeProjectMembershipAdministration(
		actor, projectID, ScopeProjectMembersManage,
	); err != nil {
		return ProjectRoleAssignment{}, err
	}
	if principalID == uuid.Nil || !validProjectRole(role) {
		return ProjectRoleAssignment{}, &AdministrationFailure{
			Code:    AdministrationFailureInvalidRequest,
			Message: "Human Project role request is invalid",
		}
	}
	functionName := "vela_assign_project_role"
	if operation == "revoke" {
		functionName = "vela_revoke_project_role"
	}
	tx, err := s.humanMembershipPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ProjectRoleAssignment{}, fmt.Errorf(
			"begin Human Project role %s transaction: %w", operation, err,
		)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := establishProjectMembershipAdministrationContext(
		ctx, tx, actor, projectID, ScopeProjectMembersManage,
	); err != nil {
		return ProjectRoleAssignment{}, err
	}
	assignment, err := scanProjectRoleAssignment(tx.QueryRow(ctx, `
		SELECT organization_id, project_id, principal_id, assigned_project_role,
			active, assigned_by_principal_id, assigned_at
		FROM `+functionName+`($1, $2, $3::project_role)
	`, projectID, principalID, string(role)))
	if err != nil {
		return ProjectRoleAssignment{}, mapHumanAdministrationDatabaseFailure(err)
	}
	if assignment.OrganizationID != actor.OrganizationID || assignment.ProjectID != projectID {
		return ProjectRoleAssignment{}, errors.New(
			"project role assignment does not match authenticated Principal",
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return ProjectRoleAssignment{}, fmt.Errorf(
			"commit Human Project role %s: %w", operation, err,
		)
	}
	return assignment, nil
}

func validProjectRole(role ProjectRole) bool {
	switch role {
	case ProjectRoleProjectAdmin, ProjectRoleDeveloper, ProjectRoleProjectViewer:
		return true
	default:
		return false
	}
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

func scanHumanMember(row credentialScanner) (HumanMember, error) {
	var member HumanMember
	var disabledAt pgtype.Timestamptz
	err := row.Scan(
		&member.ID,
		&member.OrganizationID,
		&member.OIDCIssuer,
		&member.OIDCSubject,
		&member.DisplayName,
		&member.CreatedAt,
		&disabledAt,
	)
	if disabledAt.Valid {
		value := disabledAt.Time
		member.DisabledAt = &value
	}
	return member, err
}

func scanOrganizationMember(row credentialScanner) (OrganizationMember, error) {
	var member OrganizationMember
	var disabledAt pgtype.Timestamptz
	var roles []string
	err := row.Scan(
		&member.ID,
		&member.OrganizationID,
		&member.OIDCIssuer,
		&member.OIDCSubject,
		&member.DisplayName,
		&member.CreatedAt,
		&disabledAt,
		&roles,
	)
	if disabledAt.Valid {
		value := disabledAt.Time
		member.DisabledAt = &value
	}
	member.Roles = make([]OrganizationRole, len(roles))
	for index, role := range roles {
		member.Roles[index] = OrganizationRole(role)
	}
	return member, err
}

func scanProjectMember(row credentialScanner) (ProjectMember, error) {
	var member ProjectMember
	var disabledAt pgtype.Timestamptz
	var roles []string
	err := row.Scan(
		&member.ID,
		&member.OrganizationID,
		&member.ProjectID,
		&member.DisplayName,
		&disabledAt,
		&roles,
	)
	if disabledAt.Valid {
		value := disabledAt.Time
		member.DisabledAt = &value
	}
	member.Roles = make([]ProjectRole, len(roles))
	for index, role := range roles {
		member.Roles[index] = ProjectRole(role)
	}
	return member, err
}

func scanOrganizationRoleAssignment(row credentialScanner) (OrganizationRoleAssignment, error) {
	var assignment OrganizationRoleAssignment
	var assignedBy uuid.NullUUID
	var assignedAt pgtype.Timestamptz
	err := row.Scan(
		&assignment.OrganizationID,
		&assignment.PrincipalID,
		&assignment.Role,
		&assignment.Active,
		&assignedBy,
		&assignedAt,
	)
	if assignedBy.Valid {
		assignment.AssignedByPrincipalID = assignedBy.UUID
	}
	if assignedAt.Valid {
		value := assignedAt.Time
		assignment.AssignedAt = &value
	}
	return assignment, err
}

func scanProjectRoleAssignment(row credentialScanner) (ProjectRoleAssignment, error) {
	var assignment ProjectRoleAssignment
	var assignedBy uuid.NullUUID
	var assignedAt pgtype.Timestamptz
	err := row.Scan(
		&assignment.OrganizationID,
		&assignment.ProjectID,
		&assignment.PrincipalID,
		&assignment.Role,
		&assignment.Active,
		&assignedBy,
		&assignedAt,
	)
	if assignedBy.Valid {
		assignment.AssignedByPrincipalID = assignedBy.UUID
	}
	if assignedAt.Valid {
		value := assignedAt.Time
		assignment.AssignedAt = &value
	}
	return assignment, err
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

func authorizeOrganizationIdentityAdministration(
	actor Principal,
	organizationID uuid.UUID,
	requiredScope string,
) error {
	if actor.CredentialID == uuid.Nil {
		return &AdministrationFailure{
			Code: AdministrationFailureUnauthorized, Message: "valid bearer credential is required",
		}
	}
	if actor.Kind != PrincipalKindHuman || actor.OrganizationID != organizationID ||
		actor.ProjectID != uuid.Nil || !actor.HasScope(requiredScope) {
		return &AdministrationFailure{
			Code: AdministrationFailureForbidden, Message: "Human OrganizationOwner authorization is required",
		}
	}
	return nil
}

func authorizeProjectMembershipAdministration(
	actor Principal,
	projectID uuid.UUID,
	requiredScope string,
) error {
	if actor.CredentialID == uuid.Nil {
		return &AdministrationFailure{
			Code: AdministrationFailureUnauthorized, Message: "valid bearer credential is required",
		}
	}
	if actor.Kind != PrincipalKindHuman || actor.OrganizationID == uuid.Nil ||
		(actor.ProjectID != uuid.Nil && actor.ProjectID != projectID) ||
		!actor.HasScope(requiredScope) {
		return &AdministrationFailure{
			Code:    AdministrationFailureForbidden,
			Message: "Human Project membership administration authorization is required",
		}
	}
	return nil
}

func establishOrganizationIdentityAdministrationContext(
	ctx context.Context,
	tx pgx.Tx,
	actor Principal,
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
		return mapHumanAdministrationDatabaseFailure(err)
	}
	if organizationID != actor.OrganizationID || principalID != actor.PrincipalID {
		return errors.New("organization identity administration context does not match authenticated Principal")
	}
	return nil
}

func establishProjectMembershipAdministrationContext(
	ctx context.Context,
	tx pgx.Tx,
	actor Principal,
	projectID uuid.UUID,
	requiredScope string,
) error {
	var organizationID, returnedProjectID, principalID uuid.UUID
	var transactionTime time.Time
	err := tx.QueryRow(ctx, `
		SELECT organization_id, project_id, principal_id, transaction_time
		FROM vela_set_project_membership_admin_context($1, $2, $3, $4)
	`, actor.CredentialID, actor.RequestContextProof(), projectID, requiredScope).Scan(
		&organizationID, &returnedProjectID, &principalID, &transactionTime,
	)
	if err != nil {
		return mapHumanAdministrationDatabaseFailure(err)
	}
	if organizationID != actor.OrganizationID || returnedProjectID != projectID ||
		principalID != actor.PrincipalID {
		return errors.New("project membership administration context does not match authenticated Principal")
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

func mapHumanAdministrationDatabaseFailure(err error) error {
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
			Code:    AdministrationFailureForbidden,
			Message: "Human membership administration authorization is required",
		}
	case "22023", "22001", "23502", "23503", "23514", "22P02":
		return &AdministrationFailure{
			Code:    AdministrationFailureInvalidRequest,
			Message: "Human membership administration request is invalid",
		}
	case "23505", "55000":
		return &AdministrationFailure{
			Code: AdministrationFailureConflict, Message: "Human identity already exists",
		}
	case "23P01":
		return &AdministrationFailure{
			Code:    AdministrationFailureConflict,
			Message: "the last active OrganizationOwner must remain active",
		}
	case "P0002":
		return &AdministrationFailure{
			Code: AdministrationFailureNotFound, Message: "Human member is not visible",
		}
	default:
		return err
	}
}
