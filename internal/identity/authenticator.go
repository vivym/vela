package identity

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	credentialPrefix = "vla_"
	credentialBytes  = 32
	maximumBearerLen = 32 * 1024
)

var ErrInvalidCredential = errors.New("invalid bearer credential")

const (
	ScopeJobsSubmit                        = "jobs:submit"
	ScopeJobsRead                          = "jobs:read"
	ScopeJobsCancel                        = "jobs:cancel"
	ScopeArtifactsRead                     = "artifacts:read"
	ScopeOrganizationMembersManage         = "organization_members:manage"
	ScopeOrganizationMembersRead           = "organization_members:read"
	ScopeOrganizationBillingRead           = "organization_billing:read"
	ScopeOrganizationBillingContactsManage = "organization_billing_contacts:manage"
	ScopeOrganizationBillingContactsRead   = "organization_billing_contacts:read"
	ScopeOrganizationUsageRead             = "organization_usage:read"
	ScopeOrganizationAuditRead             = "organization_audit:read"
	ScopeProjectMembersManage              = "project_members:manage"
	ScopeProjectMembersRead                = "project_members:read"
	ScopeContentDeletionManage             = "content_deletion:manage"
	ScopeRetentionPolicyManage             = "retention_policy:manage"
	ScopeServicePrincipalsManage           = "service_principals:manage"
	ScopeServicePrincipalsRead             = "service_principals:read"
	ScopeWebhooksManage                    = "webhooks:manage"
	ScopeWebhooksRead                      = "webhooks:read"
)

type Principal struct {
	Kind              PrincipalKind
	CredentialID      uuid.UUID
	OrganizationID    uuid.UUID
	ProjectID         uuid.UUID
	PrincipalID       uuid.UUID
	Scopes            []string
	credentialProof   [sha256.Size]byte
	humanOrganization *humanOrganizationAuthorization
	humanProjects     map[uuid.UUID]humanProjectAuthorization
}

type PrincipalKind string

const (
	PrincipalKindHuman   PrincipalKind = "HUMAN"
	PrincipalKindService PrincipalKind = "SERVICE"
)

type humanProjectAuthorization struct {
	sessionID uuid.UUID
	scopes    []string
	proof     [sha256.Size]byte
}

type humanOrganizationAuthorization struct {
	sessionID uuid.UUID
	scopes    []string
	proof     [sha256.Size]byte
}

func (p Principal) HasScope(required string) bool {
	for _, scope := range p.Scopes {
		if scope == required {
			return true
		}
	}
	return false
}

func (p Principal) ForProject(projectID uuid.UUID) (Principal, bool) {
	if projectID == uuid.Nil {
		return Principal{}, false
	}
	if p.Kind == PrincipalKindService {
		return p, p.ProjectID == projectID
	}
	if p.Kind != PrincipalKindHuman {
		return Principal{}, false
	}
	authorization, ok := p.humanProjects[projectID]
	if !ok || authorization.sessionID == uuid.Nil {
		return Principal{}, false
	}
	contextual := p
	contextual.CredentialID = authorization.sessionID
	contextual.ProjectID = projectID
	contextual.Scopes = append([]string(nil), authorization.scopes...)
	contextual.credentialProof = authorization.proof
	contextual.humanOrganization = nil
	contextual.humanProjects = nil
	return contextual, true
}

func (p Principal) ForOrganization(organizationID uuid.UUID) (Principal, bool) {
	if organizationID == uuid.Nil || p.Kind != PrincipalKindHuman ||
		p.OrganizationID != organizationID || p.humanOrganization == nil ||
		p.humanOrganization.sessionID == uuid.Nil {
		return Principal{}, false
	}
	contextual := p
	contextual.CredentialID = p.humanOrganization.sessionID
	contextual.ProjectID = uuid.Nil
	contextual.Scopes = append([]string(nil), p.humanOrganization.scopes...)
	contextual.credentialProof = p.humanOrganization.proof
	contextual.humanOrganization = nil
	contextual.humanProjects = nil
	return contextual, true
}

func (p Principal) RequestContextProof() []byte {
	proof := make([]byte, len(p.credentialProof))
	copy(proof, p.credentialProof[:])
	return proof
}

type Authenticator struct {
	servicePool         *pgxpool.Pool
	humanPool           *pgxpool.Pool
	humanMembershipPool *pgxpool.Pool
	pepper              []byte
	oidcVerifier        OIDCTokenVerifier
}

func NewAuthenticator(pool *pgxpool.Pool, pepper []byte) *Authenticator {
	return &Authenticator{
		servicePool: pool,
		pepper:      append([]byte(nil), pepper...),
	}
}

func NewAuthenticatorWithOIDC(
	servicePool *pgxpool.Pool,
	humanPool *pgxpool.Pool,
	pepper []byte,
	verifier OIDCTokenVerifier,
) *Authenticator {
	return &Authenticator{
		servicePool:  servicePool,
		humanPool:    humanPool,
		pepper:       append([]byte(nil), pepper...),
		oidcVerifier: verifier,
	}
}

func NewAuthenticatorWithHumanMembershipOIDC(
	servicePool *pgxpool.Pool,
	humanPool *pgxpool.Pool,
	humanMembershipPool *pgxpool.Pool,
	pepper []byte,
	verifier OIDCTokenVerifier,
) *Authenticator {
	return &Authenticator{
		servicePool:         servicePool,
		humanPool:           humanPool,
		humanMembershipPool: humanMembershipPool,
		pepper:              append([]byte(nil), pepper...),
		oidcVerifier:        verifier,
	}
}

func (a *Authenticator) Authenticate(ctx context.Context, token string) (Principal, error) {
	if strings.HasPrefix(token, credentialPrefix) {
		return a.authenticateServiceCredential(ctx, token)
	}
	return a.authenticateHumanOIDC(ctx, token)
}

func (a *Authenticator) authenticateServiceCredential(
	ctx context.Context,
	token string,
) (Principal, error) {
	credentialID, secret, err := parseCredential(token)
	if err != nil {
		return Principal{}, ErrInvalidCredential
	}
	defer clear(secret)
	if a == nil || a.servicePool == nil || len(a.pepper) == 0 {
		return Principal{}, errors.New("credential authenticator is not configured")
	}

	var principal Principal
	var expectedDigest []byte
	var expiresAt pgtype.Timestamptz
	var revokedAt pgtype.Timestamptz
	err = a.servicePool.QueryRow(ctx, `
        SELECT organization_id, project_id, principal_id, secret_digest, scopes, expires_at, revoked_at
        FROM vela_authenticate_service_credential($1)
    `, credentialID).Scan(
		&principal.OrganizationID,
		&principal.ProjectID,
		&principal.PrincipalID,
		&expectedDigest,
		&principal.Scopes,
		&expiresAt,
		&revokedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Principal{}, ErrInvalidCredential
	}
	if err != nil {
		return Principal{}, fmt.Errorf("look up service principal credential: %w", err)
	}
	defer clear(expectedDigest)

	digest := hmac.New(sha256.New, a.pepper)
	_, _ = digest.Write(secret)
	actualDigest := digest.Sum(nil)
	defer clear(actualDigest)
	if !hmac.Equal(actualDigest, expectedDigest) {
		return Principal{}, ErrInvalidCredential
	}
	principal.CredentialID = credentialID
	principal.Kind = PrincipalKindService
	copy(principal.credentialProof[:], expectedDigest)
	return principal, nil
}

func (a *Authenticator) authenticateHumanOIDC(
	ctx context.Context,
	token string,
) (Principal, error) {
	if a == nil || a.humanPool == nil || len(a.pepper) == 0 || a.oidcVerifier == nil ||
		ctx == nil || token == "" || len(token) > maximumBearerLen {
		return Principal{}, ErrInvalidCredential
	}
	verified, err := a.oidcVerifier.Verify(ctx, token)
	if err != nil || verified.Issuer == "" || verified.Subject == "" ||
		len(verified.Subject) > 500 || !verified.ExpiresAt.After(time.Now()) {
		return Principal{}, ErrInvalidCredential
	}
	digest := hmac.New(sha256.New, a.pepper)
	tokenBytes := []byte(token)
	_, _ = digest.Write(tokenBytes)
	clear(tokenBytes)
	proof := digest.Sum(nil)
	defer clear(proof)

	rows, err := a.humanPool.Query(ctx, `
		SELECT organization_id, principal_id, project_id, session_id, scopes
		FROM vela_authenticate_human_oidc($1, $2, $3, $4)
	`, verified.Issuer, verified.Subject, proof, verified.ExpiresAt)
	if err != nil {
		return Principal{}, humanAuthenticationFailure(err)
	}
	defer rows.Close()

	principal := Principal{
		Kind:          PrincipalKindHuman,
		humanProjects: make(map[uuid.UUID]humanProjectAuthorization),
	}
	found := false
	for rows.Next() {
		var organizationID, principalID uuid.UUID
		var projectID, sessionID uuid.NullUUID
		var scopes []string
		if err := rows.Scan(
			&organizationID, &principalID, &projectID, &sessionID, &scopes,
		); err != nil {
			return Principal{}, fmt.Errorf("read Human OIDC authorization: %w", err)
		}
		if !found {
			principal.OrganizationID = organizationID
			principal.PrincipalID = principalID
			found = true
		} else if principal.OrganizationID != organizationID || principal.PrincipalID != principalID {
			return Principal{}, errors.New("human OIDC authorization returned inconsistent identity")
		}
		if projectID.Valid != sessionID.Valid {
			return Principal{}, errors.New("human OIDC authorization returned incomplete Project session")
		}
		if !projectID.Valid {
			continue
		}
		if projectID.UUID == uuid.Nil || sessionID.UUID == uuid.Nil || len(scopes) == 0 {
			return Principal{}, errors.New("human OIDC authorization returned invalid Project session")
		}
		var sessionProof [sha256.Size]byte
		copy(sessionProof[:], proof)
		principal.humanProjects[projectID.UUID] = humanProjectAuthorization{
			sessionID: sessionID.UUID,
			scopes:    append([]string(nil), scopes...),
			proof:     sessionProof,
		}
	}
	if err := rows.Err(); err != nil {
		return Principal{}, humanAuthenticationFailure(err)
	}
	rows.Close()
	if !found || principal.OrganizationID == uuid.Nil || principal.PrincipalID == uuid.Nil {
		return Principal{}, ErrInvalidCredential
	}
	if a.humanMembershipPool == nil {
		return principal, nil
	}

	var organizationID, principalID uuid.UUID
	var organizationSessionID uuid.NullUUID
	var organizationScopes []string
	err = a.humanMembershipPool.QueryRow(ctx, `
		SELECT organization_id, principal_id, session_id, scopes
		FROM vela_authenticate_human_organization_oidc($1, $2, $3, $4)
	`, verified.Issuer, verified.Subject, proof, verified.ExpiresAt).Scan(
		&organizationID,
		&principalID,
		&organizationSessionID,
		&organizationScopes,
	)
	if err != nil {
		return Principal{}, humanAuthenticationFailure(err)
	}
	if organizationID != principal.OrganizationID || principalID != principal.PrincipalID {
		return Principal{}, errors.New("human Organization authorization returned inconsistent identity")
	}
	if organizationSessionID.Valid {
		if organizationSessionID.UUID == uuid.Nil || len(organizationScopes) == 0 {
			return Principal{}, errors.New("human Organization authorization returned invalid session")
		}
		var sessionProof [sha256.Size]byte
		copy(sessionProof[:], proof)
		principal.humanOrganization = &humanOrganizationAuthorization{
			sessionID: organizationSessionID.UUID,
			scopes:    append([]string(nil), organizationScopes...),
			proof:     sessionProof,
		}
	} else if len(organizationScopes) != 0 {
		return Principal{}, errors.New("human Organization authorization returned scopes without a session")
	}
	return principal, nil
}

func humanAuthenticationFailure(err error) error {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.Code == "28000" {
		return ErrInvalidCredential
	}
	return fmt.Errorf("authenticate Human OIDC identity: %w", err)
}

func parseCredential(token string) (uuid.UUID, []byte, error) {
	if !strings.HasPrefix(token, credentialPrefix) {
		return uuid.Nil, nil, ErrInvalidCredential
	}
	identifier, encodedSecret, ok := strings.Cut(strings.TrimPrefix(token, credentialPrefix), ".")
	if !ok || identifier == "" || encodedSecret == "" {
		return uuid.Nil, nil, ErrInvalidCredential
	}
	credentialID, err := uuid.Parse(identifier)
	if err != nil {
		return uuid.Nil, nil, ErrInvalidCredential
	}
	secret, err := base64.RawURLEncoding.DecodeString(encodedSecret)
	if err != nil || len(secret) != credentialBytes {
		return uuid.Nil, nil, ErrInvalidCredential
	}
	return credentialID, secret, nil
}
