package identity

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	credentialPrefix = "vla_"
	credentialBytes  = 32
)

var ErrInvalidCredential = errors.New("invalid service principal credential")

const (
	ScopeJobsSubmit     = "jobs:submit"
	ScopeJobsRead       = "jobs:read"
	ScopeJobsCancel     = "jobs:cancel"
	ScopeArtifactsRead  = "artifacts:read"
	ScopeWebhooksManage = "webhooks:manage"
	ScopeWebhooksRead   = "webhooks:read"
)

type Principal struct {
	CredentialID    uuid.UUID
	OrganizationID  uuid.UUID
	ProjectID       uuid.UUID
	PrincipalID     uuid.UUID
	Scopes          []string
	credentialProof [sha256.Size]byte
}

func (p Principal) HasScope(required string) bool {
	for _, scope := range p.Scopes {
		if scope == required {
			return true
		}
	}
	return false
}

func (p Principal) RequestContextProof() []byte {
	proof := make([]byte, len(p.credentialProof))
	copy(proof, p.credentialProof[:])
	return proof
}

type Authenticator struct {
	pool   *pgxpool.Pool
	pepper []byte
}

func NewAuthenticator(pool *pgxpool.Pool, pepper []byte) *Authenticator {
	return &Authenticator{
		pool:   pool,
		pepper: append([]byte(nil), pepper...),
	}
}

func (a *Authenticator) Authenticate(ctx context.Context, token string) (Principal, error) {
	credentialID, secret, err := parseCredential(token)
	if err != nil {
		return Principal{}, ErrInvalidCredential
	}
	defer clear(secret)
	if a == nil || a.pool == nil || len(a.pepper) == 0 {
		return Principal{}, errors.New("credential authenticator is not configured")
	}

	var principal Principal
	var expectedDigest []byte
	var expiresAt pgtype.Timestamptz
	var revokedAt pgtype.Timestamptz
	err = a.pool.QueryRow(ctx, `
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
	copy(principal.credentialProof[:], expectedDigest)
	return principal, nil
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
