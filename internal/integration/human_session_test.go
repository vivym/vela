//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivym/vela/internal/identity"
)

func TestHumanAuthenticationRejectsUnboundDisabledAndExpiredOIDCIdentity(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	principalID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		principalID,
		"human-auth-negative",
		nil,
		map[string][]string{testProjectID: {"Developer"}},
	)
	serviceAuthPool := newRolePool(
		t, database.DSN, "vela_auth_login", "vela-auth-password",
	)
	humanAuthPool := newRolePool(
		t, database.DSN, "vela_human_auth_login", "vela-human-auth-password",
	)
	authenticate := func(subject string, expiresAt time.Time) error {
		_, err := identity.NewAuthenticatorWithOIDC(
			serviceAuthPool,
			humanAuthPool,
			testCredentialPepper,
			staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
				Issuer: "https://identity.example.com", Subject: subject, ExpiresAt: expiresAt,
			}},
		).Authenticate(context.Background(), "human-auth-negative-token")
		return err
	}
	if err := authenticate("unbound-human", time.Now().UTC().Add(time.Hour)); !errors.Is(err, identity.ErrInvalidCredential) {
		t.Fatalf("unbound Human authentication error = %v, want ErrInvalidCredential", err)
	}
	if err := authenticate("human-auth-negative", time.Now().UTC().Add(-time.Minute)); !errors.Is(err, identity.ErrInvalidCredential) {
		t.Fatalf("expired Human authentication error = %v, want ErrInvalidCredential", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE human_oidc_bindings
		SET disabled_at = clock_timestamp()
		WHERE principal_id = $1
	`, principalID); err != nil {
		t.Fatalf("disable Human OIDC binding: %v", err)
	}
	if err := authenticate("human-auth-negative", time.Now().UTC().Add(time.Hour)); !errors.Is(err, identity.ErrInvalidCredential) {
		t.Fatalf("disabled Human authentication error = %v, want ErrInvalidCredential", err)
	}
}

func TestHumanAuthorizationSessionProofExpiryBindingAndActorAreRevalidated(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	principalID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		principalID,
		"human-session-revalidation",
		nil,
		map[string][]string{testProjectID: {"Developer"}},
	)
	serviceAuthPool := newRolePool(
		t, database.DSN, "vela_auth_login", "vela-auth-password",
	)
	humanAuthPool := newRolePool(
		t, database.DSN, "vela_human_auth_login", "vela-human-auth-password",
	)
	token := "human-session-revalidation-token"
	tokenExpiresAt := time.Now().UTC().Add(time.Hour)
	newAuthenticator := func() *identity.Authenticator {
		return identity.NewAuthenticatorWithOIDC(
			serviceAuthPool,
			humanAuthPool,
			testCredentialPepper,
			staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
				Issuer: "https://identity.example.com", Subject: "human-session-revalidation",
				ExpiresAt: tokenExpiresAt,
			}},
		)
	}
	principal, err := newAuthenticator().Authenticate(context.Background(), token)
	if err != nil {
		t.Fatalf("authenticate Human session fixture: %v", err)
	}
	contextual, ok := principal.ForProject(uuid.MustParse(testProjectID))
	if !ok {
		t.Fatal("Human session fixture lacks Project authorization")
	}
	var proofLength int
	var boundedByServer, boundedByToken, rawTokenAbsent bool
	if err := database.Admin.QueryRow(`
		SELECT
			octet_length(token_proof),
			expires_at <= created_at + interval '5 minutes',
			expires_at <= $2,
			NOT EXISTS (
				SELECT 1
				FROM human_auth_sessions AS candidate
				WHERE candidate.token_proof = convert_to($3, 'UTF8')
			)
		FROM human_auth_sessions
		WHERE id = $1
	`, contextual.CredentialID, tokenExpiresAt, token).Scan(
		&proofLength,
		&boundedByServer,
		&boundedByToken,
		&rawTokenAbsent,
	); err != nil {
		t.Fatalf("read Human session proof contract: %v", err)
	}
	if proofLength != 32 || !boundedByServer || !boundedByToken || !rawTokenAbsent {
		t.Fatalf(
			"Human session proof = length %d server bound %t token bound %t raw absent %t",
			proofLength,
			boundedByServer,
			boundedByToken,
			rawTokenAbsent,
		)
	}
	requestPool := newRolePool(
		t, database.DSN, "vela_request_login", "vela-request-password",
	)
	wrongProof := contextual.RequestContextProof()
	wrongProof[0] ^= 0xff
	_, err = requestPool.Exec(context.Background(), `
		SELECT * FROM vela_set_request_context($1, $2, 'jobs:read')
	`, contextual.CredentialID, wrongProof)
	assertHumanRequestContextRejected(t, err, "wrong Human session proof")

	servicePrincipal, err := identity.NewAuthenticator(
		serviceAuthPool,
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate Service actor-switch fixture: %v", err)
	}
	tx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin actor-switch request transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(context.Background(), `
		SELECT * FROM vela_set_request_context($1, $2, 'jobs:read')
	`, contextual.CredentialID, contextual.RequestContextProof()); err != nil {
		t.Fatalf("establish Human actor-switch context: %v", err)
	}
	_, err = tx.Exec(context.Background(), `
		SELECT * FROM vela_set_request_context($1, $2, 'jobs:read')
	`, servicePrincipal.CredentialID, servicePrincipal.RequestContextProof())
	assertHumanRequestContextRejected(t, err, "Human to Service actor switch")
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatalf("roll back actor-switch request transaction: %v", err)
	}

	shortExpiry := time.Now().UTC().Add(1500 * time.Millisecond)
	shortLived, err := identity.NewAuthenticatorWithOIDC(
		serviceAuthPool,
		humanAuthPool,
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer: "https://identity.example.com", Subject: "human-session-revalidation",
			ExpiresAt: shortExpiry,
		}},
	).Authenticate(context.Background(), "human-session-short-lived-token")
	if err != nil {
		t.Fatalf("authenticate short-lived Human session: %v", err)
	}
	shortContextual, ok := shortLived.ForProject(uuid.MustParse(testProjectID))
	if !ok {
		t.Fatal("short-lived Human session lacks Project authorization")
	}
	if wait := time.Until(shortExpiry) + 100*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	_, err = requestPool.Exec(context.Background(), `
		SELECT * FROM vela_set_request_context($1, $2, 'jobs:read')
	`, shortContextual.CredentialID, shortContextual.RequestContextProof())
	assertHumanRequestContextRejected(t, err, "expired Human session")

	refreshed, err := newAuthenticator().Authenticate(context.Background(), token)
	if err != nil {
		t.Fatalf("refresh Human authorization session: %v", err)
	}
	refreshedContextual, ok := refreshed.ForProject(uuid.MustParse(testProjectID))
	if !ok || refreshedContextual.CredentialID != contextual.CredentialID {
		t.Fatalf("refreshed Human Project authorization = %#v", refreshedContextual)
	}
	if _, err := database.Admin.Exec(`
		UPDATE human_oidc_bindings
		SET disabled_at = clock_timestamp()
		WHERE principal_id = $1
	`, principalID); err != nil {
		t.Fatalf("disable Human binding after authentication: %v", err)
	}
	_, err = requestPool.Exec(context.Background(), `
		SELECT * FROM vela_set_request_context($1, $2, 'jobs:read')
	`, refreshedContextual.CredentialID, refreshedContextual.RequestContextProof())
	assertHumanRequestContextRejected(t, err, "disabled Human binding session")
	if _, err := newAuthenticator().Authenticate(context.Background(), token); !errors.Is(err, identity.ErrInvalidCredential) {
		t.Fatalf("disabled Human binding reauthentication error = %v", err)
	}
}

func TestHumanAuthenticationReusesSessionAndDefersActorSessionAttribution(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	principalID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		principalID,
		"human-session-reuse",
		nil,
		map[string][]string{testProjectID: {"Developer", "ProjectAdmin"}},
	)
	authenticator := identity.NewAuthenticatorWithOIDC(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(t, database.DSN, "vela_human_auth_login", "vela-human-auth-password"),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer: "https://identity.example.com", Subject: "human-session-reuse",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	var contextual identity.Principal
	for attempt := 0; attempt < 5; attempt++ {
		principal, err := authenticator.Authenticate(context.Background(), "human-session-reuse-token")
		if err != nil {
			t.Fatalf("authenticate Human reuse attempt %d: %v", attempt, err)
		}
		selected, ok := principal.ForProject(uuid.MustParse(testProjectID))
		if !ok {
			t.Fatalf("Human reuse attempt %d lacks Project authorization", attempt)
		}
		if attempt > 0 && selected.CredentialID != contextual.CredentialID {
			t.Fatalf(
				"Human reuse attempt %d session = %s, want %s",
				attempt,
				selected.CredentialID,
				contextual.CredentialID,
			)
		}
		contextual = selected
	}

	assertCounts := func(wantSessions, wantAttributions int) {
		t.Helper()
		var sessions, attributions int
		if err := database.Admin.QueryRow(`
			SELECT
				(SELECT count(*) FROM human_auth_sessions WHERE principal_id = $1),
				(SELECT count(*) FROM project_actor_session_attributions
				 WHERE principal_id = $1 AND actor_kind = 'HUMAN')
		`, principalID).Scan(&sessions, &attributions); err != nil {
			t.Fatalf("read bounded Human session evidence: %v", err)
		}
		if sessions != wantSessions || attributions != wantAttributions {
			t.Fatalf(
				"Human session evidence = sessions %d attributions %d, want %d/%d",
				sessions,
				attributions,
				wantSessions,
				wantAttributions,
			)
		}
	}
	assertCounts(1, 0)

	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	if _, err := requestPool.Exec(context.Background(), `
		SELECT * FROM vela_set_request_context($1, $2, 'jobs:read')
	`, contextual.CredentialID, contextual.RequestContextProof()); err != nil {
		t.Fatalf("establish Human read context: %v", err)
	}
	assertCounts(1, 0)
	if _, err := requestPool.Exec(context.Background(), `
		SELECT * FROM vela_set_request_context($1, $2, 'webhooks:manage')
	`, contextual.CredentialID, contextual.RequestContextProof()); err != nil {
		t.Fatalf("establish Human Webhook manage context: %v", err)
	}
	assertCounts(1, 1)
	if _, err := requestPool.Exec(context.Background(), `
		SELECT * FROM vela_set_request_context($1, $2, 'webhooks:manage')
	`, contextual.CredentialID, contextual.RequestContextProof()); err != nil {
		t.Fatalf("re-establish Human Webhook manage context: %v", err)
	}
	assertCounts(1, 1)
}

func assertHumanRequestContextRejected(t *testing.T, err error, operation string) {
	t.Helper()
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "28000" {
		t.Fatalf("%s error = %v, want SQLSTATE 28000", operation, err)
	}
}
