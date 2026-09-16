//go:build integration

package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/identity"
)

func TestPlatformProvisionedNonExpiringCredentialCanAuthenticateListAndRevoke(t *testing.T) {
	fixture := newIdentityAdministrationFixture(t, "permanent-credential-admin")
	database, actor, projectID, service := fixture.database, fixture.actor, fixture.projectID, fixture.service
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	authenticator := identity.NewAuthenticator(authPool, testCredentialPepper)
	target, err := service.CreateServicePrincipal(
		context.Background(),
		actor,
		projectID,
		identity.CreateServicePrincipalRequest{DisplayName: "Inference caller"},
	)
	if err != nil {
		t.Fatalf("create credential target: %v", err)
	}
	expiresAt := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Microsecond)
	issued, err := service.IssueCredential(
		context.Background(),
		actor,
		projectID,
		target.ID,
		identity.IssueCredentialRequest{
			Scopes:    []string{identity.ScopeJobsRead},
			ExpiresAt: expiresAt,
		},
	)
	if err != nil {
		t.Fatalf("issue Service Credential: %v", err)
	}
	if !strings.HasPrefix(issued.BearerCredential, "vla_"+issued.Credential.ID.String()+".") {
		t.Fatalf("issued bearer Credential = %q", issued.BearerCredential)
	}
	if issued.Credential.ServicePrincipalID != target.ID ||
		issued.Credential.ProjectID != projectID ||
		len(issued.Credential.Scopes) != 1 ||
		issued.Credential.Scopes[0] != identity.ScopeJobsRead ||
		!issued.Credential.ExpiresAt.Equal(expiresAt) ||
		issued.Credential.RevokedAt != nil {
		t.Fatalf("issued Credential = %#v", issued.Credential)
	}

	// Permanent credentials are platform-provisioned; the public issue API keeps
	// its bounded expiry policy. Exercise real PostgreSQL infinity decoding.
	if _, err := database.Admin.Exec(`UPDATE credentials SET expires_at='infinity'::timestamptz WHERE id=$1`, issued.Credential.ID); err != nil {
		t.Fatal(err)
	}

	authenticated, err := identity.NewAuthenticator(
		authPool, testCredentialPepper,
	).Authenticate(context.Background(), issued.BearerCredential)
	if err != nil {
		t.Fatalf("authenticate issued Credential: %v", err)
	}
	if authenticated.Kind != identity.PrincipalKindService ||
		authenticated.PrincipalID != target.ID ||
		authenticated.ProjectID != projectID ||
		!authenticated.HasScope(identity.ScopeJobsRead) {
		t.Fatalf("issued Credential Principal = %#v", authenticated)
	}

	listed, err := service.ListCredentials(
		context.Background(), actor, projectID, target.ID, 100,
	)
	if err != nil {
		t.Fatalf("list Service Credentials: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != issued.Credential.ID ||
		listed[0].ServicePrincipalID != target.ID ||
		len(listed[0].Scopes) != 1 || listed[0].Scopes[0] != identity.ScopeJobsRead {
		t.Fatalf("listed Credentials = %#v", listed)
	}

	if listed[0].ExpiresAt != nil {
		t.Fatal("permanent Credential unexpectedly exposes a finite expiry")
	}
	revoked, err := service.RevokeCredential(context.Background(), actor, projectID, target.ID, issued.Credential.ID)
	if err != nil || revoked.ExpiresAt != nil || revoked.RevokedAt == nil {
		t.Fatalf("revoke permanent Credential: %#v, %v", revoked, err)
	}
	if _, err := authenticator.Authenticate(context.Background(), issued.BearerCredential); !errors.Is(err, identity.ErrInvalidCredential) {
		t.Fatalf("revoked permanent Credential authenticated: %v", err)
	}
}
