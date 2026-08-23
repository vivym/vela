//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivym/vela/internal/identity"
)

func TestHumanFixedRoleMatrixKeepsProjectPermissionsExplicit(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		INSERT INTO projects (
			id, organization_id, display_name, queued_limit, running_limit
		) VALUES ($1, $2, 'Human RBAC Project Two', 10, 2)
	`, testProjectTwoID, testOrganizationID); err != nil {
		t.Fatalf("seed Human RBAC second Project: %v", err)
	}

	type roleFixture struct {
		name          string
		subject       string
		organization  []string
		projectRoles  map[string][]string
		expectedScope map[string][]string
	}
	fixtures := []roleFixture{
		{
			name: "OrganizationOwner", subject: "organization-owner",
			organization: []string{"OrganizationOwner"},
		},
		{
			name: "BillingAdmin", subject: "billing-admin",
			organization: []string{"BillingAdmin"},
		},
		{
			name: "OrganizationAuditor", subject: "organization-auditor",
			organization: []string{"OrganizationAuditor"},
		},
		{
			name: "ProjectAdmin", subject: "project-admin-matrix",
			projectRoles: map[string][]string{testProjectID: {"ProjectAdmin"}},
				expectedScope: map[string][]string{
					testProjectID: {
						identity.ScopeContentDeletionManage,
						identity.ScopeProjectMembersManage,
						identity.ScopeProjectMembersRead,
						identity.ScopeRetentionPolicyManage,
						identity.ScopeServicePrincipalsManage,
						identity.ScopeServicePrincipalsRead,
					identity.ScopeWebhooksManage,
					identity.ScopeWebhooksRead,
				},
			},
		},
		{
			name: "Developer", subject: "developer-matrix",
			projectRoles: map[string][]string{testProjectID: {"Developer"}},
			expectedScope: map[string][]string{
				testProjectID: {
					identity.ScopeArtifactsRead,
					identity.ScopeJobsCancel,
					identity.ScopeJobsRead,
					identity.ScopeJobsSubmit,
				},
			},
		},
		{
			name: "ProjectViewer", subject: "project-viewer-matrix",
			projectRoles: map[string][]string{testProjectID: {"ProjectViewer"}},
			expectedScope: map[string][]string{
				testProjectID: {identity.ScopeArtifactsRead, identity.ScopeJobsRead},
			},
		},
		{
			name: "same Project union stays Project local", subject: "project-role-union",
			projectRoles: map[string][]string{
				testProjectID:    {"ProjectAdmin", "ProjectViewer"},
				testProjectTwoID: {"Developer"},
			},
				expectedScope: map[string][]string{
					testProjectID: {
						identity.ScopeArtifactsRead,
						identity.ScopeContentDeletionManage,
						identity.ScopeJobsRead,
						identity.ScopeProjectMembersManage,
						identity.ScopeProjectMembersRead,
						identity.ScopeRetentionPolicyManage,
						identity.ScopeServicePrincipalsManage,
					identity.ScopeServicePrincipalsRead,
					identity.ScopeWebhooksManage,
					identity.ScopeWebhooksRead,
				},
				testProjectTwoID: {
					identity.ScopeArtifactsRead,
					identity.ScopeJobsCancel,
					identity.ScopeJobsRead,
					identity.ScopeJobsSubmit,
				},
			},
		},
	}
	serviceAuthPool := newRolePool(
		t, database.DSN, "vela_auth_login", "vela-auth-password",
	)
	humanAuthPool := newRolePool(
		t, database.DSN, "vela_human_auth_login", "vela-human-auth-password",
	)
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			principalID := uuid.New()
			seedHumanRoleFixture(
				t,
				database.Admin,
				principalID,
				fixture.subject,
				fixture.organization,
				fixture.projectRoles,
			)
			principal, err := identity.NewAuthenticatorWithOIDC(
				serviceAuthPool,
				humanAuthPool,
				testCredentialPepper,
				staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
					Issuer:    "https://identity.example.com",
					Subject:   fixture.subject,
					ExpiresAt: time.Now().UTC().Add(time.Hour),
				}},
			).Authenticate(context.Background(), fixture.subject+"-token")
			if err != nil {
				t.Fatalf("authenticate %s: %v", fixture.name, err)
			}
			if principal.Kind != identity.PrincipalKindHuman || principal.PrincipalID != principalID {
				t.Fatalf("%s Principal = %#v", fixture.name, principal)
			}
			for _, projectID := range []string{testProjectID, testProjectTwoID} {
				expected, shouldAuthorize := fixture.expectedScope[projectID]
				contextual, authorized := principal.ForProject(uuid.MustParse(projectID))
				if authorized != shouldAuthorize {
					t.Fatalf(
						"%s Project %s authorization = %t, want %t",
						fixture.name,
						projectID,
						authorized,
						shouldAuthorize,
					)
				}
				if shouldAuthorize {
					assertExactHumanScopes(t, contextual, expected)
				}
			}
		})
	}
}

func TestHumanRoleBindingsRejectServiceAndIsolationSubstitution(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedOtherOrganization(t, database.Admin)
	principalID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		principalID,
		"constraint-human",
		nil,
		nil,
	)

	_, err := database.Admin.Exec(`
		INSERT INTO human_oidc_bindings (
			organization_id, principal_id, issuer, subject, display_name
		) VALUES ($1, $2, 'https://identity.example.com', 'service-as-human', 'Invalid Human')
	`, testOrganizationID, testPrincipalID)
	assertHumanSQLState(t, err, "23503", "SERVICE Principal Human binding")

	_, err = database.Admin.Exec(`
		INSERT INTO project_role_bindings (
			organization_id, project_id, principal_id, role, assigned_by_principal_id
		) VALUES ($1, $2, $3, 'Developer', $3)
	`, testOrganizationID, testOtherProjectID, principalID)
	assertHumanSQLState(t, err, "23503", "cross-Organization Project substitution")

	_, err = database.Admin.Exec(`
		INSERT INTO project_role_bindings (
			organization_id, project_id, principal_id, role, assigned_by_principal_id
		) VALUES ($1, $2, $3, 'Developer', $4)
	`, testOtherOrganizationID, testOtherProjectID, principalID, testOtherPrincipalID)
	assertHumanSQLState(t, err, "23503", "cross-Organization Principal substitution")

	_, err = database.Admin.Exec(`
		INSERT INTO organization_role_bindings (
			organization_id, principal_id, role, assigned_by_principal_id
		) VALUES ($1, $2, 'OrganizationOwner', $3)
	`, testOtherOrganizationID, principalID, testOtherPrincipalID)
	assertHumanSQLState(t, err, "23503", "cross-Organization role substitution")

	duplicatePrincipalID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO principals (id, organization_id, kind, display_name)
		VALUES ($1, $2, 'HUMAN', 'Duplicate OIDC Human')
	`, duplicatePrincipalID, testOrganizationID); err != nil {
		t.Fatalf("seed duplicate OIDC Human Principal: %v", err)
	}
	_, err = database.Admin.Exec(`
		INSERT INTO human_oidc_bindings (
			organization_id, principal_id, issuer, subject, display_name
		) VALUES ($1, $2, 'https://identity.example.com', 'constraint-human', 'Duplicate OIDC Human')
	`, testOrganizationID, duplicatePrincipalID)
	assertHumanSQLState(t, err, "23505", "ambiguous OIDC subject")

	_, err = database.Admin.Exec(`
		INSERT INTO project_role_bindings (
			organization_id, project_id, principal_id, role, assigned_by_principal_id
		) VALUES ($1, $2, $3, 'CustomAdministrator', $3)
	`, testOrganizationID, testProjectID, principalID)
	assertHumanSQLState(t, err, "22P02", "unknown Human role")
}

func TestHumanOIDCBindingAllowsOptionalDisplayMetadataAndBoundsSubjectBytes(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	t.Run("optional display metadata", func(t *testing.T) {
		principalID := uuid.New()
		if _, err := database.Admin.Exec(`
			INSERT INTO principals (id, organization_id, kind, display_name)
			VALUES ($1, $2, 'HUMAN', 'Optional OIDC Metadata')
		`, principalID, testOrganizationID); err != nil {
			t.Fatalf("seed optional-metadata Human Principal: %v", err)
		}
		if _, err := database.Admin.Exec(`
			INSERT INTO human_oidc_bindings (
				organization_id, principal_id, issuer, subject
			) VALUES ($1, $2, 'https://identity.example.com', 'optional-metadata')
		`, testOrganizationID, principalID); err != nil {
			t.Fatalf("insert Human OIDC binding without display metadata: %v", err)
		}
	})

	t.Run("subject byte ceiling", func(t *testing.T) {
		principalID := uuid.New()
		if _, err := database.Admin.Exec(`
			INSERT INTO principals (id, organization_id, kind, display_name)
			VALUES ($1, $2, 'HUMAN', 'Oversized OIDC Subject')
		`, principalID, testOrganizationID); err != nil {
			t.Fatalf("seed oversized-subject Human Principal: %v", err)
		}
		subject := strings.Repeat("\u754c", 167)
		if len(subject) != 501 {
			t.Fatalf("multibyte OIDC subject length = %d, want 501 bytes", len(subject))
		}
		_, err := database.Admin.Exec(`
			INSERT INTO human_oidc_bindings (
				organization_id, principal_id, issuer, subject, display_name
			) VALUES ($1, $2, 'https://identity.example.com', $3, 'Oversized Subject')
		`, testOrganizationID, principalID, subject)
		assertHumanSQLState(t, err, "23514", "oversized multibyte OIDC subject")
	})
}

func TestHumanIdentityAndRoleEvidenceCannotBeRewritten(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	principalID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		principalID,
		"immutable-human-evidence",
		[]string{"OrganizationOwner"},
		map[string][]string{testProjectID: {"Developer"}},
	)
	authenticator := identity.NewAuthenticatorWithOIDC(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(t, database.DSN, "vela_human_auth_login", "vela-human-auth-password"),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer: "https://identity.example.com", Subject: "immutable-human-evidence",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	principal, err := authenticator.Authenticate(context.Background(), "immutable-human-token")
	if err != nil {
		t.Fatalf("authenticate immutable Human evidence fixture: %v", err)
	}
	contextual, ok := principal.ForProject(uuid.MustParse(testProjectID))
	if !ok {
		t.Fatal("immutable Human evidence fixture lacks Project authorization")
	}

	bindingOnlyPrincipalID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO principals (id, organization_id, kind, display_name)
		VALUES ($1, $2, 'HUMAN', 'Binding-only Human')
	`, bindingOnlyPrincipalID, testOrganizationID); err != nil {
		t.Fatalf("seed binding-only Human Principal: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO human_oidc_bindings (
			organization_id, principal_id, issuer, subject, display_name
		) VALUES ($2, $1, 'https://identity.example.com', 'binding-only-human', 'Binding-only Human')
	`, bindingOnlyPrincipalID, testOrganizationID); err != nil {
		t.Fatalf("seed binding-only Human evidence: %v", err)
	}

	for _, test := range []struct {
		name      string
		statement string
		args      []any
	}{
		{
			name:      "OIDC identity",
			statement: "UPDATE human_oidc_bindings SET subject = 'rewritten' WHERE principal_id = $1",
			args:      []any{principalID},
		},
		{
			name: "Organization role assignment",
			statement: `
				UPDATE organization_role_bindings
				SET role = 'BillingAdmin'
				WHERE organization_id = $1 AND principal_id = $2
			`,
			args: []any{testOrganizationID, principalID},
		},
		{
			name: "Project role assignment",
			statement: `
				UPDATE project_role_bindings
				SET role = 'ProjectViewer'
				WHERE organization_id = $1 AND project_id = $2 AND principal_id = $3
			`,
			args: []any{testOrganizationID, testProjectID, principalID},
		},
		{
			name:      "authorization session",
			statement: "UPDATE human_auth_sessions SET expires_at = expires_at + interval '1 hour' WHERE id = $1",
			args:      []any{contextual.CredentialID},
		},
		{
			name:      "OIDC binding deletion",
			statement: "DELETE FROM human_oidc_bindings WHERE principal_id = $1",
			args:      []any{bindingOnlyPrincipalID},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx, err := database.Admin.Begin()
			if err != nil {
				t.Fatalf("begin Human evidence rewrite: %v", err)
			}
			defer func() { _ = tx.Rollback() }()
			_, err = tx.Exec(test.statement, test.args...)
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
				postgresError.ConstraintName != "human_identity_evidence_immutable" {
				t.Fatalf("rewrite %s error = %v, want immutable-evidence refusal", test.name, err)
			}
		})
	}

	if _, err := database.Admin.Exec(`
		UPDATE human_oidc_bindings
		SET display_name = NULL, disabled_at = clock_timestamp()
		WHERE principal_id = $1
	`, bindingOnlyPrincipalID); err != nil {
		t.Fatalf("update mutable Human OIDC display/disable metadata: %v", err)
	}
}

func seedHumanRoleFixture(
	t *testing.T,
	database *sql.DB,
	principalID uuid.UUID,
	subject string,
	organizationRoles []string,
	projectRoles map[string][]string,
) {
	t.Helper()
	if _, err := database.Exec(`
		INSERT INTO principals (id, organization_id, kind, display_name)
		VALUES ($1, $2, 'HUMAN', $3)
	`, principalID, testOrganizationID, subject); err != nil {
		t.Fatalf("seed Human Principal %s: %v", subject, err)
	}
	if _, err := database.Exec(`
		INSERT INTO human_oidc_bindings (
			organization_id, principal_id, issuer, subject, display_name
		) VALUES ($1, $2, 'https://identity.example.com', $3, $3)
	`, testOrganizationID, principalID, subject); err != nil {
		t.Fatalf("seed Human OIDC binding %s: %v", subject, err)
	}
	for _, role := range organizationRoles {
		if _, err := database.Exec(`
			INSERT INTO organization_role_bindings (
				organization_id, principal_id, role, assigned_by_principal_id
			) VALUES ($1, $2, $3::organization_role, $2)
		`, testOrganizationID, principalID, role); err != nil {
			t.Fatalf("seed Human Organization role %s: %v", role, err)
		}
	}
	for projectID, roles := range projectRoles {
		for _, role := range roles {
			if _, err := database.Exec(`
				INSERT INTO project_role_bindings (
					organization_id, project_id, principal_id, role, assigned_by_principal_id
				) VALUES ($1, $2, $3, $4::project_role, $3)
			`, testOrganizationID, projectID, principalID, role); err != nil {
				t.Fatalf("seed Human Project role %s: %v", role, err)
			}
		}
	}
}

func assertExactHumanScopes(
	t *testing.T,
	principal identity.Principal,
	expected []string,
) {
	t.Helper()
	if len(principal.Scopes) != len(expected) {
		t.Fatalf("Human scopes = %v, want %v", principal.Scopes, expected)
	}
	for _, scope := range expected {
		if !principal.HasScope(scope) {
			t.Fatalf("Human scopes %v lack %s", principal.Scopes, scope)
		}
	}
}

func assertHumanSQLState(t *testing.T, err error, code, operation string) {
	t.Helper()
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != code {
		t.Fatalf("%s error = %v, want SQLSTATE %s", operation, err, code)
	}
}
