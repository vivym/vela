package breakglass

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/identity"
)

const maximumBearerLength = 32 * 1024

var ErrInvalidOperatorCredential = errors.New("invalid Platform Operator credential")

type Operator struct {
	ID        uuid.UUID
	SessionID uuid.UUID
	ExpiresAt time.Time
	proof     [sha256.Size]byte
}

func NewOperatorForSession(
	operatorID uuid.UUID,
	sessionID uuid.UUID,
	proof []byte,
) Operator {
	operator := Operator{ID: operatorID, SessionID: sessionID}
	copy(operator.proof[:], proof)
	return operator
}

func (operator Operator) SessionProof() []byte {
	proof := make([]byte, len(operator.proof))
	copy(proof, operator.proof[:])
	return proof
}

type Authenticator struct {
	pool     *pgxpool.Pool
	verifier identity.OIDCTokenVerifier
}

func NewAuthenticator(
	pool *pgxpool.Pool,
	verifier identity.OIDCTokenVerifier,
) *Authenticator {
	return &Authenticator{pool: pool, verifier: verifier}
}

func (authenticator *Authenticator) Authenticate(
	ctx context.Context,
	token string,
) (Operator, error) {
	if authenticator == nil || authenticator.pool == nil || authenticator.verifier == nil {
		return Operator{}, errors.New("authenticator for Platform Operator is not configured")
	}
	if ctx == nil || token == "" || len(token) > maximumBearerLength ||
		strings.HasPrefix(token, "vla_") {
		return Operator{}, ErrInvalidOperatorCredential
	}
	verified, err := authenticator.verifier.Verify(ctx, token)
	if err != nil {
		return Operator{}, ErrInvalidOperatorCredential
	}
	proof := sha256.Sum256([]byte(token))
	operator := Operator{proof: proof}
	err = authenticator.pool.QueryRow(ctx, `
		SELECT operator_id, session_id, expires_at
		FROM vela_authenticate_platform_operator_oidc($1, $2, $3, $4)
	`, verified.Issuer, verified.Subject, proof[:], verified.ExpiresAt).Scan(
		&operator.ID,
		&operator.SessionID,
		&operator.ExpiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) || isAuthenticationFailure(err) {
		return Operator{}, ErrInvalidOperatorCredential
	}
	if err != nil {
		return Operator{}, fmt.Errorf("authenticate Platform Operator OIDC identity: %w", err)
	}
	if operator.ID == uuid.Nil || operator.SessionID == uuid.Nil ||
		operator.ExpiresAt.IsZero() || !operator.ExpiresAt.After(time.Now().UTC()) {
		return Operator{}, errors.New("authentication for Platform Operator returned an invalid session")
	}
	return operator, nil
}

func isAuthenticationFailure(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) && postgresError.Code == "28000"
}
