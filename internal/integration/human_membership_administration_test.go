//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/artifactaccess"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/webhook"
)

func TestOrganizationOwnerAddsHumanMember(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	ownerID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		ownerID,
		"membership-owner",
		[]string{"OrganizationOwner"},
		nil,
	)
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	humanAuthPool := newRolePool(
		t,
		database.DSN,
		"vela_human_auth_login",
		"vela-human-auth-password",
	)
	ownerAuthenticator := newHumanMembershipAuthenticator(t, database,
		authPool,
		humanAuthPool,
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "membership-owner",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	owner, err := ownerAuthenticator.Authenticate(context.Background(), "membership-owner-token")
	if err != nil {
		t.Fatalf("authenticate OrganizationOwner: %v", err)
	}
	organizationID := uuid.MustParse(testOrganizationID)
	owner, ok := owner.ForOrganization(organizationID)
	if !ok {
		t.Fatal("OrganizationOwner lacks Organization administration authorization")
	}
	service, err := newHumanMembershipAdministrationService(t, database,
		newRolePool(
			t,
			database.DSN,
			"vela_identity_request_login",
			"vela-identity-request-password",
		),
		testCredentialPepper,
		"https://identity.example.com",
	)
	if err != nil {
		t.Fatalf("create Human membership Administration service: %v", err)
	}

	created, err := service.CreateHumanMember(
		context.Background(),
		owner,
		organizationID,
		identity.CreateHumanMemberRequest{
			OIDCSubject: "new-human-member",
			DisplayName: "New Human Member",
		},
	)
	if err != nil {
		t.Fatalf("add Human member: %v", err)
	}
	if created.ID == uuid.Nil || created.OrganizationID != organizationID ||
		created.OIDCIssuer != "https://identity.example.com" ||
		created.OIDCSubject != "new-human-member" ||
		created.DisplayName != "New Human Member" || created.DisabledAt != nil {
		t.Fatalf("created Human member = %#v", created)
	}

	memberAuthenticator := newHumanMembershipAuthenticator(t, database,
		authPool,
		humanAuthPool,
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "new-human-member",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	member, err := memberAuthenticator.Authenticate(context.Background(), "new-human-member-token")
	if err != nil {
		t.Fatalf("authenticate newly added Human member: %v", err)
	}
	if member.Kind != identity.PrincipalKindHuman || member.PrincipalID != created.ID ||
		member.OrganizationID != organizationID {
		t.Fatalf("authenticated Human member = %#v", member)
	}
	if _, ok := member.ForOrganization(organizationID); ok {
		t.Fatal("new Human member received Organization authorization without a role")
	}
	if _, ok := member.ForProject(uuid.MustParse(testProjectID)); ok {
		t.Fatal("new Human member received Project authorization without a role")
	}
}

func TestOrganizationOwnerAssignsAndRevokesOrganizationRole(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	ownerID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		ownerID,
		"role-owner",
		[]string{"OrganizationOwner"},
		nil,
	)
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	humanAuthPool := newRolePool(
		t,
		database.DSN,
		"vela_human_auth_login",
		"vela-human-auth-password",
	)
	ownerAuthenticator := newHumanMembershipAuthenticator(t, database,
		authPool,
		humanAuthPool,
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "role-owner",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	owner, err := ownerAuthenticator.Authenticate(context.Background(), "role-owner-token")
	if err != nil {
		t.Fatalf("authenticate OrganizationOwner: %v", err)
	}
	organizationID := uuid.MustParse(testOrganizationID)
	owner, ok := owner.ForOrganization(organizationID)
	if !ok {
		t.Fatal("OrganizationOwner lacks Organization administration authorization")
	}
	service, err := newHumanMembershipAdministrationService(t, database,
		newRolePool(
			t,
			database.DSN,
			"vela_identity_request_login",
			"vela-identity-request-password",
		),
		testCredentialPepper,
		"https://identity.example.com",
	)
	if err != nil {
		t.Fatalf("create Human membership Administration service: %v", err)
	}
	target, err := service.CreateHumanMember(
		context.Background(),
		owner,
		organizationID,
		identity.CreateHumanMemberRequest{
			OIDCSubject: "organization-role-target",
			DisplayName: "Organization Role Target",
		},
	)
	if err != nil {
		t.Fatalf("add Organization role target: %v", err)
	}

	assigned, err := service.AssignOrganizationRole(
		context.Background(),
		owner,
		organizationID,
		target.ID,
		identity.OrganizationRoleOrganizationOwner,
	)
	if err != nil {
		t.Fatalf("assign OrganizationOwner: %v", err)
	}
	if !assigned.Active || assigned.OrganizationID != organizationID ||
		assigned.PrincipalID != target.ID ||
		assigned.Role != identity.OrganizationRoleOrganizationOwner ||
		assigned.AssignedByPrincipalID != ownerID || assigned.AssignedAt == nil {
		t.Fatalf("assigned Organization role = %#v", assigned)
	}

	targetAuthenticator := newHumanMembershipAuthenticator(t, database,
		authPool,
		humanAuthPool,
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "organization-role-target",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	targetPrincipal, err := targetAuthenticator.Authenticate(
		context.Background(), "organization-role-target-token",
	)
	if err != nil {
		t.Fatalf("authenticate assigned OrganizationOwner: %v", err)
	}
	if _, ok := targetPrincipal.ForOrganization(organizationID); !ok {
		t.Fatal("assigned OrganizationOwner lacks Organization authorization")
	}

	revoked, err := service.RevokeOrganizationRole(
		context.Background(),
		owner,
		organizationID,
		target.ID,
		identity.OrganizationRoleOrganizationOwner,
	)
	if err != nil {
		t.Fatalf("revoke OrganizationOwner: %v", err)
	}
	if revoked.Active || revoked.OrganizationID != organizationID ||
		revoked.PrincipalID != target.ID ||
		revoked.Role != identity.OrganizationRoleOrganizationOwner {
		t.Fatalf("revoked Organization role = %#v", revoked)
	}
	targetPrincipal, err = targetAuthenticator.Authenticate(
		context.Background(), "organization-role-target-token",
	)
	if err != nil {
		t.Fatalf("authenticate role-revoked Human member: %v", err)
	}
	if targetPrincipal.PrincipalID != target.ID {
		t.Fatalf("role revocation changed Human Principal: %#v", targetPrincipal)
	}
	if _, ok := targetPrincipal.ForOrganization(organizationID); ok {
		t.Fatal("revoked OrganizationOwner retained Organization authorization")
	}
}

func TestOrganizationOwnerAndProjectAdminManageProjectRoles(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	ownerID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		ownerID,
		"project-role-owner",
		[]string{"OrganizationOwner"},
		nil,
	)
	projectAdminID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		projectAdminID,
		"project-role-admin",
		nil,
		map[string][]string{testProjectID: {"ProjectAdmin"}},
	)
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	humanAuthPool := newRolePool(
		t,
		database.DSN,
		"vela_human_auth_login",
		"vela-human-auth-password",
	)
	authenticate := func(subject, token string) identity.Principal {
		t.Helper()
		authenticator := newHumanMembershipAuthenticator(t, database,
			authPool,
			humanAuthPool,
			testCredentialPepper,
			staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
				Issuer:    "https://identity.example.com",
				Subject:   subject,
				ExpiresAt: time.Now().UTC().Add(time.Hour),
			}},
		)
		principal, err := authenticator.Authenticate(context.Background(), token)
		if err != nil {
			t.Fatalf("authenticate %s: %v", subject, err)
		}
		return principal
	}
	organizationID := uuid.MustParse(testOrganizationID)
	projectID := uuid.MustParse(testProjectID)
	owner, ok := authenticate("project-role-owner", "project-role-owner-token").
		ForOrganization(organizationID)
	if !ok {
		t.Fatal("OrganizationOwner lacks Organization administration authorization")
	}
	projectAdmin, ok := authenticate("project-role-admin", "project-role-admin-token").
		ForProject(projectID)
	if !ok {
		t.Fatal("ProjectAdmin lacks Project administration authorization")
	}
	service, err := newHumanMembershipAdministrationService(t, database,
		newRolePool(
			t,
			database.DSN,
			"vela_identity_request_login",
			"vela-identity-request-password",
		),
		testCredentialPepper,
		"https://identity.example.com",
	)
	if err != nil {
		t.Fatalf("create Human membership Administration service: %v", err)
	}
	target, err := service.CreateHumanMember(
		context.Background(),
		owner,
		organizationID,
		identity.CreateHumanMemberRequest{
			OIDCSubject: "project-role-target",
			DisplayName: "Project Role Target",
		},
	)
	if err != nil {
		t.Fatalf("add Project role target: %v", err)
	}

	developer, err := service.AssignProjectRole(
		context.Background(),
		owner,
		projectID,
		target.ID,
		identity.ProjectRoleDeveloper,
	)
	if err != nil {
		t.Fatalf("OrganizationOwner assigns Developer: %v", err)
	}
	if !developer.Active || developer.OrganizationID != organizationID ||
		developer.ProjectID != projectID || developer.PrincipalID != target.ID ||
		developer.Role != identity.ProjectRoleDeveloper ||
		developer.AssignedByPrincipalID != ownerID || developer.AssignedAt == nil {
		t.Fatalf("assigned Developer role = %#v", developer)
	}

	viewer, err := service.AssignProjectRole(
		context.Background(),
		projectAdmin,
		projectID,
		target.ID,
		identity.ProjectRoleProjectViewer,
	)
	if err != nil {
		t.Fatalf("ProjectAdmin assigns ProjectViewer: %v", err)
	}
	if !viewer.Active || viewer.Role != identity.ProjectRoleProjectViewer ||
		viewer.AssignedByPrincipalID != projectAdminID {
		t.Fatalf("assigned ProjectViewer role = %#v", viewer)
	}

	targetPrincipal := authenticate("project-role-target", "project-role-target-token")
	targetProject, ok := targetPrincipal.ForProject(projectID)
	if !ok || !targetProject.HasScope(identity.ScopeJobsSubmit) ||
		!targetProject.HasScope(identity.ScopeArtifactsRead) {
		t.Fatalf("Project role union authorization = %#v, selected=%v", targetProject, ok)
	}

	revoked, err := service.RevokeProjectRole(
		context.Background(),
		projectAdmin,
		projectID,
		target.ID,
		identity.ProjectRoleDeveloper,
	)
	if err != nil {
		t.Fatalf("ProjectAdmin revokes Developer: %v", err)
	}
	if revoked.Active || revoked.Role != identity.ProjectRoleDeveloper {
		t.Fatalf("revoked Developer role = %#v", revoked)
	}
	targetPrincipal = authenticate("project-role-target", "project-role-target-token")
	targetProject, ok = targetPrincipal.ForProject(projectID)
	if !ok || targetProject.HasScope(identity.ScopeJobsSubmit) ||
		!targetProject.HasScope(identity.ScopeArtifactsRead) {
		t.Fatalf("post-revocation Project authorization = %#v, selected=%v", targetProject, ok)
	}
}

func TestOrganizationOwnerDisablesMemberPermanentlyAndPreservesLastOwner(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	firstOwnerID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		firstOwnerID,
		"disable-first-owner",
		[]string{"OrganizationOwner"},
		nil,
	)
	secondOwnerID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		secondOwnerID,
		"disable-second-owner",
		[]string{"OrganizationOwner"},
		nil,
	)
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	humanAuthPool := newRolePool(
		t,
		database.DSN,
		"vela_human_auth_login",
		"vela-human-auth-password",
	)
	authenticatorFor := func(subject string) *identity.Authenticator {
		t.Helper()
		return newHumanMembershipAuthenticator(t, database,
			authPool,
			humanAuthPool,
			testCredentialPepper,
			staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
				Issuer:    "https://identity.example.com",
				Subject:   subject,
				ExpiresAt: time.Now().UTC().Add(time.Hour),
			}},
		)
	}
	firstOwner, err := authenticatorFor("disable-first-owner").Authenticate(
		context.Background(), "disable-first-owner-token",
	)
	if err != nil {
		t.Fatalf("authenticate first OrganizationOwner: %v", err)
	}
	organizationID := uuid.MustParse(testOrganizationID)
	firstOwner, ok := firstOwner.ForOrganization(organizationID)
	if !ok {
		t.Fatal("first OrganizationOwner lacks Organization authorization")
	}
	service, err := newHumanMembershipAdministrationService(t, database,
		newRolePool(
			t,
			database.DSN,
			"vela_identity_request_login",
			"vela-identity-request-password",
		),
		testCredentialPepper,
		"https://identity.example.com",
	)
	if err != nil {
		t.Fatalf("create Human membership Administration service: %v", err)
	}

	disabled, err := service.DisableHumanMember(
		context.Background(), firstOwner, organizationID, secondOwnerID,
	)
	if err != nil {
		t.Fatalf("disable second OrganizationOwner: %v", err)
	}
	if disabled.ID != secondOwnerID || disabled.DisabledAt == nil {
		t.Fatalf("disabled Human member = %#v", disabled)
	}
	replayed, err := service.DisableHumanMember(
		context.Background(), firstOwner, organizationID, secondOwnerID,
	)
	if err != nil {
		t.Fatalf("replay Human member disable: %v", err)
	}
	if replayed.DisabledAt == nil || !replayed.DisabledAt.Equal(*disabled.DisabledAt) {
		t.Fatalf("disable replay = %#v, want disabled_at %s", replayed, disabled.DisabledAt)
	}
	if _, err := authenticatorFor("disable-second-owner").Authenticate(
		context.Background(), "disable-second-owner-token",
	); !errors.Is(err, identity.ErrInvalidCredential) {
		t.Fatalf("disabled Human authentication error = %v, want ErrInvalidCredential", err)
	}

	if _, err := service.DisableHumanMember(
		context.Background(), firstOwner, organizationID, firstOwnerID,
	); err == nil {
		t.Fatal("last active OrganizationOwner disable succeeded")
	} else {
		var failure *identity.AdministrationFailure
		if !errors.As(err, &failure) || failure.Code != identity.AdministrationFailureConflict {
			t.Fatalf("last owner disable error = %v, want conflict", err)
		}
	}
	var firstOwnerDisabledAt *time.Time
	if err := database.Admin.QueryRow(`
		SELECT disabled_at
		FROM human_oidc_bindings
		WHERE principal_id = $1
	`, firstOwnerID).Scan(&firstOwnerDisabledAt); err != nil {
		t.Fatalf("inspect last OrganizationOwner: %v", err)
	}
	if firstOwnerDisabledAt != nil {
		t.Fatalf("last OrganizationOwner disabled_at = %s", *firstOwnerDisabledAt)
	}

	if _, err := database.Admin.Exec(`
		UPDATE human_oidc_bindings
		SET disabled_at = NULL
		WHERE principal_id = $1
	`, secondOwnerID); err == nil {
		t.Fatal("direct Human member re-enable succeeded")
	} else {
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != "55000" {
			t.Fatalf("direct Human re-enable error = %v, want SQLSTATE 55000", err)
		}
	}
	var disableEventCount int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM human_identity_events
		WHERE target_principal_id = $1
		  AND action = 'HUMAN_MEMBER_DISABLED'
	`, secondOwnerID).Scan(&disableEventCount); err != nil {
		t.Fatalf("count Human disable events: %v", err)
	}
	if disableEventCount != 1 {
		t.Fatalf("Human disable event count = %d, want 1", disableEventCount)
	}
}

func TestHumanAdministratorsListExactMemberships(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	ownerID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		ownerID,
		"membership-list-owner",
		[]string{"OrganizationOwner"},
		nil,
	)
	projectAdminID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		projectAdminID,
		"membership-list-project-admin",
		nil,
		map[string][]string{testProjectID: {"ProjectAdmin"}},
	)
	memberID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		memberID,
		"membership-list-member",
		[]string{"BillingAdmin", "OrganizationAuditor"},
		map[string][]string{testProjectID: {"Developer", "ProjectViewer"}},
	)
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	humanAuthPool := newRolePool(
		t,
		database.DSN,
		"vela_human_auth_login",
		"vela-human-auth-password",
	)
	authenticate := func(subject string) identity.Principal {
		t.Helper()
		authenticator := newHumanMembershipAuthenticator(t, database,
			authPool,
			humanAuthPool,
			testCredentialPepper,
			staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
				Issuer:    "https://identity.example.com",
				Subject:   subject,
				ExpiresAt: time.Now().UTC().Add(time.Hour),
			}},
		)
		principal, err := authenticator.Authenticate(context.Background(), subject+"-token")
		if err != nil {
			t.Fatalf("authenticate %s: %v", subject, err)
		}
		return principal
	}
	organizationID := uuid.MustParse(testOrganizationID)
	projectID := uuid.MustParse(testProjectID)
	owner, ok := authenticate("membership-list-owner").ForOrganization(organizationID)
	if !ok {
		t.Fatal("OrganizationOwner lacks Organization authorization")
	}
	projectAdmin, ok := authenticate("membership-list-project-admin").ForProject(projectID)
	if !ok {
		t.Fatal("ProjectAdmin lacks Project authorization")
	}
	service, err := newHumanMembershipAdministrationService(t, database,
		newRolePool(
			t,
			database.DSN,
			"vela_identity_request_login",
			"vela-identity-request-password",
		),
		testCredentialPepper,
		"https://identity.example.com",
	)
	if err != nil {
		t.Fatalf("create Human membership Administration service: %v", err)
	}

	organizationMembers, err := service.ListHumanMembers(
		context.Background(), owner, organizationID, 100,
	)
	if err != nil {
		t.Fatalf("list Organization Human members: %v", err)
	}
	var listedOrganizationMember *identity.OrganizationMember
	for index := range organizationMembers {
		if organizationMembers[index].ID == memberID {
			listedOrganizationMember = &organizationMembers[index]
			break
		}
	}
	if listedOrganizationMember == nil ||
		len(listedOrganizationMember.Roles) != 2 ||
		listedOrganizationMember.Roles[0] != identity.OrganizationRoleBillingAdmin ||
		listedOrganizationMember.Roles[1] != identity.OrganizationRoleOrganizationAuditor {
		t.Fatalf("listed Organization member = %#v", listedOrganizationMember)
	}
	if _, err := service.ListHumanMembers(
		context.Background(), projectAdmin, organizationID, 100,
	); err == nil {
		t.Fatal("ProjectAdmin listed Organization membership")
	} else {
		var failure *identity.AdministrationFailure
		if !errors.As(err, &failure) || failure.Code != identity.AdministrationFailureForbidden {
			t.Fatalf("ProjectAdmin Organization list error = %v, want forbidden", err)
		}
	}

	for label, actor := range map[string]identity.Principal{
		"OrganizationOwner": owner,
		"ProjectAdmin":      projectAdmin,
	} {
		projectMembers, err := service.ListProjectMembers(
			context.Background(), actor, projectID, 100,
		)
		if err != nil {
			t.Fatalf("%s lists Project members: %v", label, err)
		}
		var listedProjectMember *identity.ProjectMember
		for index := range projectMembers {
			if projectMembers[index].ID == memberID {
				listedProjectMember = &projectMembers[index]
				break
			}
		}
		if listedProjectMember == nil || listedProjectMember.ProjectID != projectID ||
			len(listedProjectMember.Roles) != 2 ||
			listedProjectMember.Roles[0] != identity.ProjectRoleDeveloper ||
			listedProjectMember.Roles[1] != identity.ProjectRoleProjectViewer {
			t.Fatalf("%s listed Project member = %#v", label, listedProjectMember)
		}
	}
}

func TestOrganizationOwnerManagesHumanMembershipThroughProductionHTTPPath(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	otherOrganizationID := uuid.New()
	otherProjectID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO customer_organizations (id, display_name)
		VALUES ($1, 'Other HTTP membership Organization')
	`, otherOrganizationID); err != nil {
		t.Fatalf("seed other HTTP membership Organization: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO projects (id, organization_id, display_name, queued_limit, running_limit)
		VALUES ($1, $2, 'Other HTTP membership Project', 10, 2)
	`, otherProjectID, otherOrganizationID); err != nil {
		t.Fatalf("seed other HTTP membership Project: %v", err)
	}

	ownerID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		ownerID,
		"membership-http-owner",
		[]string{"OrganizationOwner"},
		nil,
	)
	authenticator := newHumanMembershipAuthenticator(t, database,
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t,
			database.DSN,
			"vela_human_auth_login",
			"vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "membership-http-owner",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	administration, err := newHumanMembershipAdministrationService(t, database,
		newRolePool(
			t,
			database.DSN,
			"vela_identity_request_login",
			"vela-identity-request-password",
		),
		testCredentialPepper,
		"https://identity.example.com",
	)
	if err != nil {
		t.Fatalf("create Human membership Administration service: %v", err)
	}
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          authenticator,
		IdentityAdministration: administration,
		Admission:              &admission.Service{},
		Cancellation:           &cancellation.Service{},
		Artifacts:              &artifactaccess.Service{},
		Webhooks:               &webhook.Service{},
	})
	if err != nil {
		t.Fatalf("create Human membership HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	request, err := http.NewRequest(
		http.MethodPost,
		server.URL+"/v1/organizations/"+testOrganizationID+"/members",
		bytes.NewBufferString(`{"oidc_subject":"membership-http-target","display_name":"HTTP Human Member"}`),
	)
	if err != nil {
		t.Fatalf("create Human member HTTP request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer membership-http-owner-token")
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("add Human member over HTTP: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("add Human member HTTP status = %d, want 201", response.StatusCode)
	}
	var created struct {
		PrincipalID    string `json:"principal_id"`
		OrganizationID string `json:"organization_id"`
		OIDCIssuer     string `json:"oidc_issuer"`
		OIDCSubject    string `json:"oidc_subject"`
		DisplayName    string `json:"display_name"`
		CreatedAt      string `json:"created_at"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode created Human member: %v", err)
	}
	if _, err := uuid.Parse(created.PrincipalID); err != nil ||
		created.OrganizationID != testOrganizationID ||
		created.OIDCIssuer != "https://identity.example.com" ||
		created.OIDCSubject != "membership-http-target" ||
		created.DisplayName != "HTTP Human Member" || created.CreatedAt == "" {
		t.Fatalf("created Human member response = %#v; id error=%v", created, err)
	}

	listRequest, err := http.NewRequest(
		http.MethodGet,
		server.URL+"/v1/organizations/"+testOrganizationID+"/members?limit=100",
		nil,
	)
	if err != nil {
		t.Fatalf("create Human member list request: %v", err)
	}
	listRequest.Header.Set("Authorization", "Bearer membership-http-owner-token")
	listResponse, err := server.Client().Do(listRequest)
	if err != nil {
		t.Fatalf("list Human members over HTTP: %v", err)
	}
	defer listResponse.Body.Close()
	if listResponse.StatusCode != http.StatusOK {
		t.Fatalf("list Human members HTTP status = %d, want 200", listResponse.StatusCode)
	}
	var listBody struct {
		Members []struct {
			PrincipalID       string   `json:"principal_id"`
			OIDCSubject       string   `json:"oidc_subject"`
			OrganizationRoles []string `json:"organization_roles"`
		} `json:"members"`
	}
	if err := json.NewDecoder(listResponse.Body).Decode(&listBody); err != nil {
		t.Fatalf("decode Human member list: %v", err)
	}
	found := false
	for _, member := range listBody.Members {
		if member.PrincipalID == created.PrincipalID {
			found = member.OIDCSubject == "membership-http-target" &&
				len(member.OrganizationRoles) == 0
		}
	}
	if !found {
		t.Fatalf("Human member list = %#v, want created member", listBody)
	}

	roleBasePath := "/v1/organizations/" + testOrganizationID + "/members/" +
		created.PrincipalID + "/roles"
	assignRequest, err := http.NewRequest(
		http.MethodPost,
		server.URL+roleBasePath,
		bytes.NewBufferString(`{"role":"BillingAdmin"}`),
	)
	if err != nil {
		t.Fatalf("create Organization role assignment request: %v", err)
	}
	assignRequest.Header.Set("Authorization", "Bearer membership-http-owner-token")
	assignRequest.Header.Set("Content-Type", "application/json")
	assignResponse, err := server.Client().Do(assignRequest)
	if err != nil {
		t.Fatalf("assign Organization role over HTTP: %v", err)
	}
	if assignResponse.StatusCode != http.StatusOK {
		defer assignResponse.Body.Close()
		t.Fatalf("assign Organization role HTTP status = %d, want 200", assignResponse.StatusCode)
	}
	var assignedRole struct {
		PrincipalID string `json:"principal_id"`
		Role        string `json:"role"`
		Active      bool   `json:"active"`
	}
	if err := json.NewDecoder(assignResponse.Body).Decode(&assignedRole); err != nil {
		assignResponse.Body.Close()
		t.Fatalf("decode Organization role assignment: %v", err)
	}
	assignResponse.Body.Close()
	if assignedRole.PrincipalID != created.PrincipalID ||
		assignedRole.Role != "BillingAdmin" || !assignedRole.Active {
		t.Fatalf("assigned Organization role response = %#v", assignedRole)
	}

	revokeRequest, err := http.NewRequest(
		http.MethodPost,
		server.URL+roleBasePath+"/BillingAdmin/revoke",
		nil,
	)
	if err != nil {
		t.Fatalf("create Organization role revocation request: %v", err)
	}
	revokeRequest.Header.Set("Authorization", "Bearer membership-http-owner-token")
	revokeResponse, err := server.Client().Do(revokeRequest)
	if err != nil {
		t.Fatalf("revoke Organization role over HTTP: %v", err)
	}
	defer revokeResponse.Body.Close()
	if revokeResponse.StatusCode != http.StatusOK {
		t.Fatalf("revoke Organization role HTTP status = %d, want 200", revokeResponse.StatusCode)
	}
	var revokedRole struct {
		PrincipalID string `json:"principal_id"`
		Role        string `json:"role"`
		Active      bool   `json:"active"`
	}
	if err := json.NewDecoder(revokeResponse.Body).Decode(&revokedRole); err != nil {
		t.Fatalf("decode Organization role revocation: %v", err)
	}
	if revokedRole.PrincipalID != created.PrincipalID ||
		revokedRole.Role != "BillingAdmin" || revokedRole.Active {
		t.Fatalf("revoked Organization role response = %#v", revokedRole)
	}

	projectRoleBasePath := "/v1/projects/" + testProjectID + "/members/" +
		created.PrincipalID + "/roles"
	assignProjectRequest, err := http.NewRequest(
		http.MethodPost,
		server.URL+projectRoleBasePath,
		bytes.NewBufferString(`{"role":"Developer"}`),
	)
	if err != nil {
		t.Fatalf("create Project role assignment request: %v", err)
	}
	assignProjectRequest.Header.Set("Authorization", "Bearer membership-http-owner-token")
	assignProjectRequest.Header.Set("Content-Type", "application/json")
	assignProjectResponse, err := server.Client().Do(assignProjectRequest)
	if err != nil {
		t.Fatalf("assign Project role over HTTP: %v", err)
	}
	if assignProjectResponse.StatusCode != http.StatusOK {
		defer assignProjectResponse.Body.Close()
		t.Fatalf("assign Project role HTTP status = %d, want 200", assignProjectResponse.StatusCode)
	}
	var assignedProjectRole struct {
		PrincipalID string `json:"principal_id"`
		ProjectID   string `json:"project_id"`
		Role        string `json:"role"`
		Active      bool   `json:"active"`
	}
	if err := json.NewDecoder(assignProjectResponse.Body).Decode(&assignedProjectRole); err != nil {
		assignProjectResponse.Body.Close()
		t.Fatalf("decode Project role assignment: %v", err)
	}
	assignProjectResponse.Body.Close()
	if assignedProjectRole.PrincipalID != created.PrincipalID ||
		assignedProjectRole.ProjectID != testProjectID ||
		assignedProjectRole.Role != "Developer" || !assignedProjectRole.Active {
		t.Fatalf("assigned Project role response = %#v", assignedProjectRole)
	}

	projectListRequest, err := http.NewRequest(
		http.MethodGet,
		server.URL+"/v1/projects/"+testProjectID+"/members?limit=100",
		nil,
	)
	if err != nil {
		t.Fatalf("create Project member list request: %v", err)
	}
	projectListRequest.Header.Set("Authorization", "Bearer membership-http-owner-token")
	projectListResponse, err := server.Client().Do(projectListRequest)
	if err != nil {
		t.Fatalf("list Project members over HTTP: %v", err)
	}
	if projectListResponse.StatusCode != http.StatusOK {
		defer projectListResponse.Body.Close()
		t.Fatalf("list Project members HTTP status = %d, want 200", projectListResponse.StatusCode)
	}
	var projectListBody struct {
		Members []struct {
			PrincipalID  string   `json:"principal_id"`
			ProjectRoles []string `json:"project_roles"`
		} `json:"members"`
	}
	if err := json.NewDecoder(projectListResponse.Body).Decode(&projectListBody); err != nil {
		projectListResponse.Body.Close()
		t.Fatalf("decode Project member list: %v", err)
	}
	projectListResponse.Body.Close()
	projectMemberFound := false
	for _, member := range projectListBody.Members {
		if member.PrincipalID == created.PrincipalID {
			projectMemberFound = len(member.ProjectRoles) == 1 &&
				member.ProjectRoles[0] == "Developer"
		}
	}
	if !projectMemberFound {
		t.Fatalf("Project member list = %#v, want assigned member", projectListBody)
	}

	crossOrganizationListRequest, err := http.NewRequest(
		http.MethodGet,
		server.URL+"/v1/projects/"+otherProjectID.String()+"/members?limit=100",
		nil,
	)
	if err != nil {
		t.Fatalf("create cross-Organization Project member list request: %v", err)
	}
	crossOrganizationListRequest.Header.Set(
		"Authorization", "Bearer membership-http-owner-token",
	)
	crossOrganizationListResponse, err := server.Client().Do(crossOrganizationListRequest)
	if err != nil {
		t.Fatalf("list cross-Organization Project members over HTTP: %v", err)
	}
	defer crossOrganizationListResponse.Body.Close()
	if crossOrganizationListResponse.StatusCode != http.StatusNotFound {
		t.Fatalf(
			"cross-Organization Project member list HTTP status = %d, want 404",
			crossOrganizationListResponse.StatusCode,
		)
	}
	var crossOrganizationListFailure struct {
		Code string `json:"code"`
	}
	if err := json.NewDecoder(crossOrganizationListResponse.Body).Decode(
		&crossOrganizationListFailure,
	); err != nil {
		t.Fatalf("decode cross-Organization Project member list failure: %v", err)
	}
	if crossOrganizationListFailure.Code != "not_found" {
		t.Fatalf(
			"cross-Organization Project member list failure code = %q, want not_found",
			crossOrganizationListFailure.Code,
		)
	}

	revokeProjectRequest, err := http.NewRequest(
		http.MethodPost,
		server.URL+projectRoleBasePath+"/Developer/revoke",
		nil,
	)
	if err != nil {
		t.Fatalf("create Project role revocation request: %v", err)
	}
	revokeProjectRequest.Header.Set("Authorization", "Bearer membership-http-owner-token")
	revokeProjectResponse, err := server.Client().Do(revokeProjectRequest)
	if err != nil {
		t.Fatalf("revoke Project role over HTTP: %v", err)
	}
	defer revokeProjectResponse.Body.Close()
	if revokeProjectResponse.StatusCode != http.StatusOK {
		t.Fatalf("revoke Project role HTTP status = %d, want 200", revokeProjectResponse.StatusCode)
	}
	var revokedProjectRole struct {
		PrincipalID string `json:"principal_id"`
		Role        string `json:"role"`
		Active      bool   `json:"active"`
	}
	if err := json.NewDecoder(revokeProjectResponse.Body).Decode(&revokedProjectRole); err != nil {
		t.Fatalf("decode Project role revocation: %v", err)
	}
	if revokedProjectRole.PrincipalID != created.PrincipalID ||
		revokedProjectRole.Role != "Developer" || revokedProjectRole.Active {
		t.Fatalf("revoked Project role response = %#v", revokedProjectRole)
	}

	disablePath := "/v1/organizations/" + testOrganizationID + "/members/" +
		created.PrincipalID + "/disable"
	disable := func() string {
		t.Helper()
		disableRequest, err := http.NewRequest(http.MethodPost, server.URL+disablePath, nil)
		if err != nil {
			t.Fatalf("create Human member disable request: %v", err)
		}
		disableRequest.Header.Set("Authorization", "Bearer membership-http-owner-token")
		disableResponse, err := server.Client().Do(disableRequest)
		if err != nil {
			t.Fatalf("disable Human member over HTTP: %v", err)
		}
		defer disableResponse.Body.Close()
		if disableResponse.StatusCode != http.StatusOK {
			t.Fatalf("disable Human member HTTP status = %d, want 200", disableResponse.StatusCode)
		}
		var disabled struct {
			PrincipalID string `json:"principal_id"`
			DisabledAt  string `json:"disabled_at"`
		}
		if err := json.NewDecoder(disableResponse.Body).Decode(&disabled); err != nil {
			t.Fatalf("decode disabled Human member: %v", err)
		}
		if disabled.PrincipalID != created.PrincipalID || disabled.DisabledAt == "" {
			t.Fatalf("disabled Human member response = %#v", disabled)
		}
		return disabled.DisabledAt
	}
	disabledAt := disable()
	if replayedDisabledAt := disable(); replayedDisabledAt != disabledAt {
		t.Fatalf("disable replay disabled_at = %q, want %q", replayedDisabledAt, disabledAt)
	}
}

func TestHumanMembershipAdministrationRequiresCurrentExactHumanRoles(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	organizationID := uuid.MustParse(testOrganizationID)
	projectID := uuid.MustParse(testProjectID)
	secondProjectID := uuid.MustParse(testProjectTwoID)
	otherOrganizationID := uuid.New()
	otherProjectID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO projects (id, organization_id, display_name, queued_limit, running_limit)
		VALUES ($1, $2, 'Second membership Project', 10, 2)
	`, secondProjectID, organizationID); err != nil {
		t.Fatalf("seed second membership Project: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO customer_organizations (id, display_name)
		VALUES ($1, 'Other membership Organization')
	`, otherOrganizationID); err != nil {
		t.Fatalf("seed other membership Organization: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO projects (id, organization_id, display_name, queued_limit, running_limit)
		VALUES ($1, $2, 'Other membership Project', 10, 2)
	`, otherProjectID, otherOrganizationID); err != nil {
		t.Fatalf("seed other membership Project: %v", err)
	}

	ownerID := uuid.New()
	projectAdminID := uuid.New()
	developerID := uuid.New()
	billingAdminID := uuid.New()
	organizationAuditorID := uuid.New()
	projectViewerID := uuid.New()
	seedHumanRoleFixture(
		t, database.Admin, ownerID, "membership-exact-owner",
		[]string{"OrganizationOwner"}, nil,
	)
	seedHumanRoleFixture(
		t, database.Admin, projectAdminID, "membership-exact-project-admin", nil,
		map[string][]string{testProjectID: {"ProjectAdmin"}},
	)
	seedHumanRoleFixture(
		t, database.Admin, developerID, "membership-exact-developer", nil,
		map[string][]string{testProjectID: {"Developer"}},
	)
	seedHumanRoleFixture(
		t, database.Admin, billingAdminID, "membership-exact-billing-admin",
		[]string{"BillingAdmin"}, nil,
	)
	seedHumanRoleFixture(
		t, database.Admin, organizationAuditorID, "membership-exact-organization-auditor",
		[]string{"OrganizationAuditor"}, nil,
	)
	seedHumanRoleFixture(
		t, database.Admin, projectViewerID, "membership-exact-project-viewer", nil,
		map[string][]string{testProjectID: {"ProjectViewer"}},
	)
	otherMemberID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO principals (id, organization_id, kind, display_name)
		VALUES ($1, $2, 'HUMAN', 'Other Organization member')
	`, otherMemberID, otherOrganizationID); err != nil {
		t.Fatalf("seed cross-Organization Principal: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO human_oidc_bindings (
			organization_id, principal_id, issuer, subject, display_name
		) VALUES ($2, $1, 'https://identity.example.com', 'other-membership-member',
			'Other Organization member')
	`, otherMemberID, otherOrganizationID); err != nil {
		t.Fatalf("seed cross-Organization Human member: %v", err)
	}

	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	humanAuthPool := newRolePool(
		t, database.DSN, "vela_human_auth_login", "vela-human-auth-password",
	)
	authenticate := func(subject string) identity.Principal {
		t.Helper()
		principal, err := newHumanMembershipAuthenticator(t, database,
			authPool,
			humanAuthPool,
			testCredentialPepper,
			staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
				Issuer: "https://identity.example.com", Subject: subject,
				ExpiresAt: time.Now().UTC().Add(time.Hour),
			}},
		).Authenticate(context.Background(), subject+"-token")
		if err != nil {
			t.Fatalf("authenticate %s: %v", subject, err)
		}
		return principal
	}
	owner, ok := authenticate("membership-exact-owner").ForOrganization(organizationID)
	if !ok {
		t.Fatal("OrganizationOwner lacks Organization administration authorization")
	}
	projectAdmin, ok := authenticate("membership-exact-project-admin").ForProject(projectID)
	if !ok {
		t.Fatal("ProjectAdmin lacks exact Project authorization")
	}
	developer, ok := authenticate("membership-exact-developer").ForProject(projectID)
	if !ok {
		t.Fatal("Developer lacks ordinary Project authorization")
	}
	billingAdmin := authenticate("membership-exact-billing-admin")
	organizationAuditor := authenticate("membership-exact-organization-auditor")
	projectViewer, ok := authenticate("membership-exact-project-viewer").ForProject(projectID)
	if !ok {
		t.Fatal("ProjectViewer lacks ordinary Project authorization")
	}
	unknownPrincipal := identity.Principal{
		Kind:           identity.PrincipalKind("UNKNOWN"),
		CredentialID:   uuid.New(),
		OrganizationID: organizationID,
		ProjectID:      projectID,
		PrincipalID:    uuid.New(),
		Scopes: []string{
			identity.ScopeProjectMembersManage,
			identity.ScopeProjectMembersRead,
		},
	}
	serviceActor, err := identity.NewAuthenticator(
		authPool, testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate Service Principal: %v", err)
	}
	service, err := newHumanMembershipAdministrationService(t, database,
		newRolePool(
			t,
			database.DSN,
			"vela_identity_request_login",
			"vela-identity-request-password",
		),
		testCredentialPepper,
		"https://identity.example.com",
	)
	if err != nil {
		t.Fatalf("create membership Administration service: %v", err)
	}
	target, err := service.CreateHumanMember(
		context.Background(), owner, organizationID,
		identity.CreateHumanMemberRequest{OIDCSubject: "membership-exact-target"},
	)
	if err != nil {
		t.Fatalf("create exact-authorization target: %v", err)
	}

	if _, err := service.AssignProjectRole(
		context.Background(), projectAdmin, projectID, target.ID, identity.ProjectRoleDeveloper,
	); err != nil {
		t.Fatalf("ProjectAdmin manages exact Project: %v", err)
	}
	if _, err := service.AssignProjectRole(
		context.Background(), owner, secondProjectID, target.ID, identity.ProjectRoleProjectViewer,
	); err != nil {
		t.Fatalf("OrganizationOwner manages another Project in its Organization: %v", err)
	}
	if _, err := service.AssignProjectRole(
		context.Background(), projectAdmin, secondProjectID, target.ID, identity.ProjectRoleDeveloper,
	); !administrationFailureHasCode(err, identity.AdministrationFailureForbidden) {
		t.Fatalf("cross-Project ProjectAdmin error = %v, want forbidden", err)
	}
	if _, err := service.AssignProjectRole(
		context.Background(), owner, otherProjectID, target.ID, identity.ProjectRoleDeveloper,
	); !administrationFailureHasCode(err, identity.AdministrationFailureNotFound) {
		t.Fatalf("cross-Organization Project error = %v, want not_found", err)
	}
	if _, err := service.AssignOrganizationRole(
		context.Background(), owner, organizationID, otherMemberID,
		identity.OrganizationRoleOrganizationAuditor,
	); !administrationFailureHasCode(err, identity.AdministrationFailureNotFound) {
		t.Fatalf("cross-Organization member error = %v, want not_found", err)
	}
	if _, err := service.CreateHumanMember(
		context.Background(), projectAdmin, organizationID,
		identity.CreateHumanMemberRequest{OIDCSubject: "project-admin-organization-escape"},
	); !administrationFailureHasCode(err, identity.AdministrationFailureForbidden) {
		t.Fatalf("ProjectAdmin Organization administration error = %v, want forbidden", err)
	}
	for label, actor := range map[string]identity.Principal{
		"Developer":           developer,
		"BillingAdmin":        billingAdmin,
		"OrganizationAuditor": organizationAuditor,
		"ProjectViewer":       projectViewer,
		"ServicePrincipal":    serviceActor,
		"UnknownPrincipal":    unknownPrincipal,
	} {
		if _, err := service.AssignProjectRole(
			context.Background(), actor, projectID, target.ID, identity.ProjectRoleProjectViewer,
		); err == nil {
			t.Fatalf("%s assigned a Project role", label)
		}
	}

	if _, err := database.Admin.Exec(`
		DELETE FROM project_role_bindings
		WHERE organization_id = $1 AND project_id = $2
		  AND principal_id = $3 AND role = 'ProjectAdmin'
	`, organizationID, projectID, projectAdminID); err != nil {
		t.Fatalf("remove ProjectAdmin role after authentication: %v", err)
	}
	if _, err := service.AssignProjectRole(
		context.Background(), projectAdmin, projectID, target.ID,
		identity.ProjectRoleProjectViewer,
	); !administrationFailureHasCode(err, identity.AdministrationFailureUnauthorized) {
		t.Fatalf("stale ProjectAdmin session error = %v, want unauthorized", err)
	}
	var staleProjectMutationCount int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM project_role_bindings
		WHERE organization_id = $1 AND project_id = $2
		  AND principal_id = $3 AND role = 'ProjectViewer'
	`, organizationID, projectID, target.ID).Scan(&staleProjectMutationCount); err != nil {
		t.Fatalf("count stale ProjectAdmin mutations: %v", err)
	}
	if staleProjectMutationCount != 0 {
		t.Fatalf("stale ProjectAdmin created %d Project roles", staleProjectMutationCount)
	}
	for _, scope := range []string{
		identity.ScopeJobsSubmit,
		identity.ScopeJobsRead,
		identity.ScopeJobsCancel,
		identity.ScopeArtifactsRead,
	} {
		if owner.HasScope(scope) {
			t.Fatalf("OrganizationOwner received Customer Content scope %s", scope)
		}
	}

	if _, err := database.Admin.Exec(`
		DELETE FROM organization_role_bindings
		WHERE organization_id = $1 AND principal_id = $2 AND role = 'OrganizationOwner'
	`, organizationID, ownerID); err != nil {
		t.Fatalf("remove OrganizationOwner role after authentication: %v", err)
	}
	if _, err := service.CreateHumanMember(
		context.Background(), owner, organizationID,
		identity.CreateHumanMemberRequest{OIDCSubject: "stale-owner-escape"},
	); !administrationFailureHasCode(err, identity.AdministrationFailureUnauthorized) {
		t.Fatalf("stale OrganizationOwner session error = %v, want unauthorized", err)
	}
	var staleMutationCount int
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM human_oidc_bindings WHERE subject = 'stale-owner-escape'
	`).Scan(&staleMutationCount); err != nil {
		t.Fatalf("count stale-session mutations: %v", err)
	}
	if staleMutationCount != 0 {
		t.Fatalf("stale OrganizationOwner created %d Human members", staleMutationCount)
	}
}

func TestHumanMembershipMutationsAreValidatedIdempotentAndImmutablyAttributed(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	organizationID := uuid.MustParse(testOrganizationID)
	projectID := uuid.MustParse(testProjectID)
	ownerID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		ownerID,
		"membership-idempotency-owner",
		[]string{"OrganizationOwner"},
		nil,
	)
	ownerAuthenticator := newHumanMembershipAuthenticator(t, database,
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t, database.DSN, "vela_human_auth_login", "vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer: "https://identity.example.com", Subject: "membership-idempotency-owner",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	owner, err := ownerAuthenticator.Authenticate(
		context.Background(), "membership-idempotency-owner-token",
	)
	if err != nil {
		t.Fatalf("authenticate idempotency OrganizationOwner: %v", err)
	}
	owner, ok := owner.ForOrganization(organizationID)
	if !ok {
		t.Fatal("idempotency OrganizationOwner lacks Organization authorization")
	}
	service, err := newHumanMembershipAdministrationService(t, database,
		newRolePool(
			t,
			database.DSN,
			"vela_identity_request_login",
			"vela-identity-request-password",
		),
		testCredentialPepper,
		"https://identity.example.com",
	)
	if err != nil {
		t.Fatalf("create idempotency Administration service: %v", err)
	}

	invalidMembers := []identity.CreateHumanMemberRequest{
		{},
		{OIDCSubject: strings.Repeat("s", 501)},
		{OIDCSubject: "valid-subject", DisplayName: " \t\n "},
		{OIDCSubject: "valid-subject", DisplayName: strings.Repeat("界", 201)},
	}
	for index, request := range invalidMembers {
		if _, err := service.CreateHumanMember(
			context.Background(), owner, organizationID, request,
		); !administrationFailureHasCode(err, identity.AdministrationFailureInvalidRequest) {
			t.Fatalf("invalid Human member request %d error = %v, want invalid_request", index, err)
		}
	}
	longSubjectMember, err := service.CreateHumanMember(
		context.Background(), owner, organizationID,
		identity.CreateHumanMemberRequest{OIDCSubject: strings.Repeat("s", 500)},
	)
	if err != nil || longSubjectMember.DisplayName != "Human member" {
		t.Fatalf("optional display name fallback = %#v error=%v", longSubjectMember, err)
	}
	if _, err := service.AssignOrganizationRole(
		context.Background(), owner, organizationID, uuid.New(), identity.OrganizationRole("Owner"),
	); !administrationFailureHasCode(err, identity.AdministrationFailureInvalidRequest) {
		t.Fatalf("invalid Organization role error = %v, want invalid_request", err)
	}
	if _, err := service.AssignProjectRole(
		context.Background(), owner, projectID, uuid.New(), identity.ProjectRole("Operator"),
	); !administrationFailureHasCode(err, identity.AdministrationFailureInvalidRequest) {
		t.Fatalf("invalid Project role error = %v, want invalid_request", err)
	}
	for _, limit := range []int32{0, 101} {
		if _, err := service.ListHumanMembers(
			context.Background(), owner, organizationID, limit,
		); !administrationFailureHasCode(err, identity.AdministrationFailureInvalidRequest) {
			t.Fatalf("invalid Human list limit %d error = %v", limit, err)
		}
		if _, err := service.ListProjectMembers(
			context.Background(), owner, projectID, limit,
		); !administrationFailureHasCode(err, identity.AdministrationFailureInvalidRequest) {
			t.Fatalf("invalid Project list limit %d error = %v", limit, err)
		}
	}

	created, err := service.CreateHumanMember(
		context.Background(),
		owner,
		organizationID,
		identity.CreateHumanMemberRequest{
			OIDCSubject: "membership-idempotency-target", DisplayName: "Audited Human",
		},
	)
	if err != nil {
		t.Fatalf("create idempotency target: %v", err)
	}
	replayedCreate, err := service.CreateHumanMember(
		context.Background(),
		owner,
		organizationID,
		identity.CreateHumanMemberRequest{
			OIDCSubject: "membership-idempotency-target", DisplayName: "Ignored Replay Name",
		},
	)
	if err != nil || replayedCreate.ID != created.ID ||
		replayedCreate.DisplayName != created.DisplayName {
		t.Fatalf("Human member create replay = %#v error=%v, want %#v", replayedCreate, err, created)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := service.AssignOrganizationRole(
			context.Background(), owner, organizationID, created.ID,
			identity.OrganizationRoleOrganizationAuditor,
		); err != nil {
			t.Fatalf("assign Organization role attempt %d: %v", attempt, err)
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := service.RevokeOrganizationRole(
			context.Background(), owner, organizationID, created.ID,
			identity.OrganizationRoleOrganizationAuditor,
		); err != nil {
			t.Fatalf("revoke Organization role attempt %d: %v", attempt, err)
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := service.AssignProjectRole(
			context.Background(), owner, projectID, created.ID, identity.ProjectRoleDeveloper,
		); err != nil {
			t.Fatalf("assign Project role attempt %d: %v", attempt, err)
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := service.RevokeProjectRole(
			context.Background(), owner, projectID, created.ID, identity.ProjectRoleDeveloper,
		); err != nil {
			t.Fatalf("revoke Project role attempt %d: %v", attempt, err)
		}
	}
	var disabledAt time.Time
	for attempt := 0; attempt < 2; attempt++ {
		disabled, err := service.DisableHumanMember(
			context.Background(), owner, organizationID, created.ID,
		)
		if err != nil || disabled.DisabledAt == nil {
			t.Fatalf("disable Human member attempt %d = %#v error=%v", attempt, disabled, err)
		}
		if attempt == 0 {
			disabledAt = *disabled.DisabledAt
		} else if !disabled.DisabledAt.Equal(disabledAt) {
			t.Fatalf("disable replay changed disabled_at from %s to %s", disabledAt, *disabled.DisabledAt)
		}
	}
	if _, err := service.AssignProjectRole(
		context.Background(), owner, projectID, created.ID, identity.ProjectRoleDeveloper,
	); !administrationFailureHasCode(err, identity.AdministrationFailureNotFound) {
		t.Fatalf("role assignment to disabled Human error = %v, want not_found", err)
	}

	expectedActions := map[string]bool{
		"HUMAN_MEMBER_CREATED":       false,
		"HUMAN_MEMBER_DISABLED":      false,
		"ORGANIZATION_ROLE_ASSIGNED": false,
		"ORGANIZATION_ROLE_REVOKED":  false,
		"PROJECT_ROLE_ASSIGNED":      false,
		"PROJECT_ROLE_REVOKED":       false,
	}
	rows, err := database.Admin.Query(`
		SELECT action::text, actor_principal_id, actor_session_id, details::text, count(*)
		FROM human_identity_events
		WHERE target_principal_id = $1
		GROUP BY action, actor_principal_id, actor_session_id, details
		ORDER BY action
	`, created.ID)
	if err != nil {
		t.Fatalf("read idempotent Human identity events: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var action, details string
		var actorID, sessionID uuid.UUID
		var count int
		if err := rows.Scan(&action, &actorID, &sessionID, &details, &count); err != nil {
			t.Fatalf("scan Human identity event: %v", err)
		}
		if _, expected := expectedActions[action]; !expected || expectedActions[action] {
			t.Fatalf("unexpected or duplicate Human identity action %q", action)
		}
		expectedActions[action] = true
		if count != 1 || actorID != ownerID || sessionID != owner.CredentialID {
			t.Fatalf(
				"Human identity action %s = count %d actor %s session %s",
				action, count, actorID, sessionID,
			)
		}
		if action == "HUMAN_MEMBER_CREATED" {
			if details != `{"display_name": "Audited Human"}` {
				t.Fatalf("Human creation event details = %s", details)
			}
		} else if details != `{}` {
			t.Fatalf("Human identity action %s details = %s, want empty object", action, details)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate Human identity events: %v", err)
	}
	for action, found := range expectedActions {
		if !found {
			t.Fatalf("missing Human identity action %s", action)
		}
	}
	var attributionActor uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT actor_principal_id
		FROM human_administration_actor_attributions
		WHERE organization_id = $1 AND actor_session_id = $2
	`, organizationID, owner.CredentialID).Scan(&attributionActor); err != nil {
		t.Fatalf("read Human administration actor attribution: %v", err)
	}
	if attributionActor != ownerID {
		t.Fatalf("Human administration attribution actor = %s, want %s", attributionActor, ownerID)
	}

	var postgresError *pgconn.PgError
	for label, statement := range map[string]string{
		"event update":       "UPDATE human_identity_events SET details = '{}'::jsonb WHERE target_principal_id = $1",
		"event delete":       "DELETE FROM human_identity_events WHERE target_principal_id = $1",
		"attribution update": "UPDATE human_administration_actor_attributions SET first_attributed_at = clock_timestamp() WHERE actor_session_id = $1",
		"attribution delete": "DELETE FROM human_administration_actor_attributions WHERE actor_session_id = $1",
	} {
		_, err := database.Admin.Exec(statement, map[bool]uuid.UUID{
			true: owner.CredentialID, false: created.ID,
		}[strings.HasPrefix(label, "attribution")])
		if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
			postgresError.ConstraintName != "human_administration_evidence_immutable" {
			t.Fatalf("%s error = %v, want immutable refusal", label, err)
		}
	}
}

func TestConcurrentHumanMemberCreateReplaysCommittedIdentity(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	organizationID := uuid.MustParse(testOrganizationID)
	ownerID := uuid.New()
	seedHumanRoleFixture(
		t, database.Admin, ownerID, "membership-concurrent-create-owner",
		[]string{"OrganizationOwner"}, nil,
	)
	authenticator := newHumanMembershipAuthenticator(t, database,
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t, database.DSN, "vela_human_auth_login", "vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer: "https://identity.example.com", Subject: "membership-concurrent-create-owner",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	owner, err := authenticator.Authenticate(
		context.Background(), "membership-concurrent-create-owner-token",
	)
	if err != nil {
		t.Fatalf("authenticate concurrent-create OrganizationOwner: %v", err)
	}
	owner, ok := owner.ForOrganization(organizationID)
	if !ok {
		t.Fatal("concurrent-create OrganizationOwner lacks Organization authorization")
	}
	service, err := newHumanMembershipAdministrationService(t, database,
		newRolePool(
			t, database.DSN, "vela_identity_request_login", "vela-identity-request-password",
		),
		testCredentialPepper,
		"https://identity.example.com",
	)
	if err != nil {
		t.Fatalf("create concurrent-create Administration service: %v", err)
	}

	type createResult struct {
		member identity.HumanMember
		err    error
	}
	blocker, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin concurrent-create blocker: %v", err)
	}
	t.Cleanup(func() { _ = blocker.Rollback() })
	if _, err := blocker.Exec("LOCK TABLE human_oidc_bindings IN SHARE MODE"); err != nil {
		t.Fatalf("lock Human bindings for concurrent create: %v", err)
	}
	start := make(chan struct{})
	results := make(chan createResult, 2)
	for _, displayName := range []string{"Concurrent Member A", "Concurrent Member B"} {
		displayName := displayName
		go func() {
			<-start
			member, err := service.CreateHumanMember(
				context.Background(), owner, organizationID,
				identity.CreateHumanMemberRequest{
					OIDCSubject: "membership-concurrent-create-target",
					DisplayName: displayName,
				},
			)
			results <- createResult{member: member, err: err}
		}()
	}
	close(start)
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiters int
		if err := database.Admin.QueryRow(`
			SELECT count(*)
			FROM pg_catalog.pg_locks AS lock
			JOIN pg_catalog.pg_stat_activity AS activity ON activity.pid = lock.pid
			WHERE lock.relation = 'human_oidc_bindings'::regclass
			  AND lock.mode = 'RowExclusiveLock'
			  AND NOT lock.granted
			  AND activity.usename = 'vela_human_membership_request_login'
		`).Scan(&waiters); err != nil {
			t.Fatalf("observe concurrent Human create waiters: %v", err)
		}
		if waiters >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("concurrent Human create waiters = %d, want 2", waiters)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := blocker.Commit(); err != nil {
		t.Fatalf("release concurrent-create blocker: %v", err)
	}
	first := <-results
	second := <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("concurrent Human creates errors = %v and %v", first.err, second.err)
	}
	if first.member.ID == uuid.Nil || first.member.ID != second.member.ID ||
		first.member.DisplayName != second.member.DisplayName {
		t.Fatalf("concurrent Human creates = %#v and %#v", first.member, second.member)
	}
	var bindings, events int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM human_oidc_bindings
			 WHERE issuer = 'https://identity.example.com'
			   AND subject = 'membership-concurrent-create-target'),
			(SELECT count(*) FROM human_identity_events
			 WHERE target_principal_id = $1 AND action = 'HUMAN_MEMBER_CREATED')
	`, first.member.ID).Scan(&bindings, &events); err != nil {
		t.Fatalf("read concurrent Human create evidence: %v", err)
	}
	if bindings != 1 || events != 1 {
		t.Fatalf("concurrent Human create evidence = bindings %d events %d", bindings, events)
	}
}

func TestOrganizationSessionAttributionRemainsProjectNeutral(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	organizationID := uuid.MustParse(testOrganizationID)
	firstProjectID := uuid.MustParse(testProjectID)
	secondProjectID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO projects (id, organization_id, display_name, queued_limit, running_limit)
		VALUES ($1, $2, 'Second attribution Project', 10, 2)
	`, secondProjectID, organizationID); err != nil {
		t.Fatalf("seed second attribution Project: %v", err)
	}
	ownerID := uuid.New()
	targetID := uuid.New()
	seedHumanRoleFixture(
		t, database.Admin, ownerID, "membership-attribution-owner",
		[]string{"OrganizationOwner"}, nil,
	)
	seedHumanRoleFixture(
		t, database.Admin, targetID, "membership-attribution-target", nil, nil,
	)
	authenticator := newHumanMembershipAuthenticator(t, database,
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t, database.DSN, "vela_human_auth_login", "vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer: "https://identity.example.com", Subject: "membership-attribution-owner",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	owner, err := authenticator.Authenticate(
		context.Background(), "membership-attribution-owner-token",
	)
	if err != nil {
		t.Fatalf("authenticate attribution OrganizationOwner: %v", err)
	}
	owner, ok := owner.ForOrganization(organizationID)
	if !ok {
		t.Fatal("attribution OrganizationOwner lacks Organization authorization")
	}
	service, err := newHumanMembershipAdministrationService(t, database,
		newRolePool(
			t, database.DSN, "vela_identity_request_login", "vela-identity-request-password",
		),
		testCredentialPepper,
		"https://identity.example.com",
	)
	if err != nil {
		t.Fatalf("create attribution Administration service: %v", err)
	}
	for _, projectID := range []uuid.UUID{firstProjectID, secondProjectID} {
		if _, err := service.AssignProjectRole(
			context.Background(), owner, projectID, targetID, identity.ProjectRoleProjectViewer,
		); err != nil {
			t.Fatalf("OrganizationOwner assigns role in Project %s: %v", projectID, err)
		}
	}

	var contextKind string
	var attributedProject uuid.NullUUID
	if err := database.Admin.QueryRow(`
		SELECT actor_context_kind::text, project_id
		FROM human_administration_actor_attributions
		WHERE organization_id = $1 AND actor_session_id = $2
	`, organizationID, owner.CredentialID).Scan(&contextKind, &attributedProject); err != nil {
		t.Fatalf("read Organization-session attribution: %v", err)
	}
	if contextKind != "ORGANIZATION" || attributedProject.Valid {
		t.Fatalf(
			"Organization-session attribution = kind %s Project %#v, want neutral Project",
			contextKind, attributedProject,
		)
	}
	var events, projects int
	if err := database.Admin.QueryRow(`
		SELECT count(*), count(DISTINCT project_id)
		FROM human_identity_events
		WHERE organization_id = $1 AND actor_session_id = $2
		  AND target_principal_id = $3 AND action = 'PROJECT_ROLE_ASSIGNED'
	`, organizationID, owner.CredentialID, targetID).Scan(&events, &projects); err != nil {
		t.Fatalf("read Organization-session Project events: %v", err)
	}
	if events != 2 || projects != 2 {
		t.Fatalf("Organization-session Project evidence = events %d Projects %d", events, projects)
	}
}

func TestConcurrentLastOrganizationOwnerTransitionsPreserveOneActiveOwner(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	organizationID := uuid.MustParse(testOrganizationID)
	firstOwnerID := uuid.New()
	secondOwnerID := uuid.New()
	seedHumanRoleFixture(
		t, database.Admin, firstOwnerID, "concurrent-first-owner",
		[]string{"OrganizationOwner"}, nil,
	)
	seedHumanRoleFixture(
		t, database.Admin, secondOwnerID, "concurrent-second-owner",
		[]string{"OrganizationOwner"}, nil,
	)
	authenticator := newHumanMembershipAuthenticator(t, database,
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t, database.DSN, "vela_human_auth_login", "vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer: "https://identity.example.com", Subject: "concurrent-first-owner",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	actor, err := authenticator.Authenticate(context.Background(), "concurrent-owner-token")
	if err != nil {
		t.Fatalf("authenticate concurrent OrganizationOwner: %v", err)
	}
	actor, ok := actor.ForOrganization(organizationID)
	if !ok {
		t.Fatal("concurrent OrganizationOwner lacks Organization authorization")
	}
	service, err := newHumanMembershipAdministrationService(t, database,
		newRolePool(
			t,
			database.DSN,
			"vela_identity_request_login",
			"vela-identity-request-password",
		),
		testCredentialPepper,
		"https://identity.example.com",
	)
	if err != nil {
		t.Fatalf("create concurrent membership Administration service: %v", err)
	}

	organizationLock, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Organization serialization lock: %v", err)
	}
	defer func() { _ = organizationLock.Rollback() }()
	if _, err := organizationLock.Exec(`
		SELECT 1 FROM customer_organizations WHERE id = $1 FOR UPDATE
	`, organizationID); err != nil {
		t.Fatalf("lock Organization serialization row: %v", err)
	}
	type transitionResult struct {
		operation string
		err       error
	}
	results := make(chan transitionResult, 2)
	go func() {
		_, err := service.RevokeOrganizationRole(
			context.Background(), actor, organizationID, firstOwnerID,
			identity.OrganizationRoleOrganizationOwner,
		)
		results <- transitionResult{operation: "revoke", err: err}
	}()
	go func() {
		_, err := service.DisableHumanMember(
			context.Background(), actor, organizationID, secondOwnerID,
		)
		results <- transitionResult{operation: "disable", err: err}
	}()
	waitForIdentityAdministrationLockWaiters(t, database.Admin, 2)
	if err := organizationLock.Commit(); err != nil {
		t.Fatalf("release Organization serialization lock: %v", err)
	}

	succeeded := 0
	failed := 0
	for range 2 {
		result := <-results
		if result.err == nil {
			succeeded++
			continue
		}
		failed++
		if !administrationFailureHasCode(result.err, identity.AdministrationFailureConflict) {
			t.Fatalf("concurrent %s error = %v, want conflict", result.operation, result.err)
		}
	}
	if succeeded != 1 || failed != 1 {
		t.Fatalf("concurrent last-owner transitions = %d succeeded, %d failed", succeeded, failed)
	}

	var activeOwners, transitionEvents int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM organization_role_bindings AS role_binding
		JOIN human_oidc_bindings AS binding
		  ON binding.organization_id = role_binding.organization_id
		 AND binding.principal_id = role_binding.principal_id
		 AND binding.disabled_at IS NULL
		WHERE role_binding.organization_id = $1
		  AND role_binding.role = 'OrganizationOwner'
	`, organizationID).Scan(&activeOwners); err != nil {
		t.Fatalf("count active OrganizationOwners after concurrent transitions: %v", err)
	}
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM human_identity_events
		WHERE action IN ('HUMAN_MEMBER_DISABLED', 'ORGANIZATION_ROLE_REVOKED')
		  AND target_principal_id IN ($1, $2)
	`, firstOwnerID, secondOwnerID).Scan(&transitionEvents); err != nil {
		t.Fatalf("count concurrent last-owner events: %v", err)
	}
	if activeOwners != 1 || transitionEvents != 1 {
		t.Fatalf(
			"concurrent last-owner postcondition = active owners %d, transition events %d",
			activeOwners, transitionEvents,
		)
	}
}

func TestHumanMembershipAdministrationMigrationAllowsEmptyDownUp(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	developerID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		developerID,
		"membership-migration-developer",
		nil,
		map[string][]string{testProjectID: {"Developer"}},
	)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 13); err != nil {
		t.Fatalf("contract empty Human membership administration migration: %v", err)
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 13 {
		t.Fatalf("migration version after Human membership Down = %d error=%v", version, err)
	}
	var contracted bool
	if err := database.Admin.QueryRow(`
		SELECT
			to_regclass('public.human_organization_auth_sessions') IS NULL
			AND to_regclass('public.human_identity_events') IS NULL
			AND to_regprocedure(
				'vela_set_organization_identity_admin_context(uuid,bytea,text)'
			) IS NULL
			AND to_regprocedure('vela_create_human_member(uuid,uuid,text,text,text)') IS NULL
			AND NOT ('project_members:manage' = ANY(
				vela_project_role_scopes('ProjectAdmin'::project_role)
			))
	`).Scan(&contracted); err != nil {
		t.Fatalf("inspect contracted Human membership surface: %v", err)
	}
	if !contracted {
		t.Fatal("Human membership migration Down left expanded schema surface")
	}
	if _, err := identity.NewAuthenticator(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential()); err != nil {
		t.Fatalf("authenticate legacy Service request after Human membership Down: %v", err)
	}
	humanAuthPool := newRolePool(
		t, database.DSN, "vela_human_auth_login", "vela-human-auth-password",
	)
	proof := make([]byte, 32)
	rows, err := humanAuthPool.Query(context.Background(), `
		SELECT organization_id, project_id, principal_id, session_id, scopes
		FROM vela_authenticate_human_oidc($1, $2, $3, $4)
	`,
		"https://identity.example.com",
		"membership-migration-developer",
		proof,
		time.Now().UTC().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("authenticate legacy Human request after Human membership Down: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("legacy Human authentication returned no Project authorization after Down")
	}
	var organizationID, projectID, principalID, sessionID uuid.UUID
	var scopes []string
	if err := rows.Scan(&organizationID, &projectID, &principalID, &sessionID, &scopes); err != nil {
		t.Fatalf("scan legacy Human authorization after Down: %v", err)
	}
	if organizationID != uuid.MustParse(testOrganizationID) ||
		projectID != uuid.MustParse(testProjectID) || principalID != developerID ||
		sessionID == uuid.Nil || len(scopes) == 0 {
		t.Fatalf(
			"legacy Human authorization after Down = organization %s Project %s principal %s session %s scopes %v",
			organizationID, projectID, principalID, sessionID, scopes,
		)
	}
	rows.Close()

	if err := goose.Up(database.Admin, migrations); err != nil {
		t.Fatalf("re-expand Human membership administration migration: %v", err)
	}
	version, err = goose.GetDBVersion(database.Admin)
	if err != nil || version != 14 {
		t.Fatalf("migration version after Human membership Down/Up = %d error=%v", version, err)
	}
	if err := database.Admin.QueryRow(`
		SELECT
			to_regclass('public.human_organization_auth_sessions') IS NOT NULL
			AND to_regclass('public.human_identity_events') IS NOT NULL
			AND to_regprocedure('vela_create_human_member(uuid,uuid,text,text,text)') IS NOT NULL
	`).Scan(&contracted); err != nil {
		t.Fatalf("inspect re-expanded Human membership surface: %v", err)
	}
	if !contracted {
		t.Fatal("Human membership migration Up did not restore expanded schema surface")
	}
}

func TestHumanMembershipAdministrationMigrationDownRefusesDurableEvidence(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	organizationID := uuid.MustParse(testOrganizationID)
	ownerID := uuid.New()
	seedHumanRoleFixture(
		t, database.Admin, ownerID, "membership-migration-owner",
		[]string{"OrganizationOwner"}, nil,
	)
	authenticator := newHumanMembershipAuthenticator(t, database,
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t, database.DSN, "vela_human_auth_login", "vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer: "https://identity.example.com", Subject: "membership-migration-owner",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	owner, err := authenticator.Authenticate(context.Background(), "membership-migration-token")
	if err != nil {
		t.Fatalf("authenticate migration OrganizationOwner: %v", err)
	}
	owner, ok := owner.ForOrganization(organizationID)
	if !ok {
		t.Fatal("migration OrganizationOwner lacks Organization authorization")
	}
	service, err := newHumanMembershipAdministrationService(t, database,
		newRolePool(
			t,
			database.DSN,
			"vela_identity_request_login",
			"vela-identity-request-password",
		),
		testCredentialPepper,
		"https://identity.example.com",
	)
	if err != nil {
		t.Fatalf("create migration Administration service: %v", err)
	}
	target, err := service.CreateHumanMember(
		context.Background(), owner, organizationID,
		identity.CreateHumanMemberRequest{OIDCSubject: "durable-membership-evidence"},
	)
	if err != nil {
		t.Fatalf("create durable Human membership evidence: %v", err)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err = goose.DownTo(database.Admin, migrations, 13)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
		postgresError.ConstraintName != "human_membership_administration_contract_requires_empty_evidence" {
		t.Fatalf("Human membership migration Down error = %v, want named SQLSTATE 55000", err)
	}
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil || version != 14 {
		t.Fatalf("migration version after refused Human membership Down = %d error=%v", version, versionErr)
	}
	var events int
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM human_identity_events WHERE target_principal_id = $1
	`, target.ID).Scan(&events); err != nil {
		t.Fatalf("read preserved Human membership evidence: %v", err)
	}
	if events != 1 {
		t.Fatalf("preserved Human membership events = %d, want 1", events)
	}
}

func newHumanMembershipAuthenticator(
	t *testing.T,
	database testDatabase,
	servicePool *pgxpool.Pool,
	humanPool *pgxpool.Pool,
	pepper []byte,
	verifier identity.OIDCTokenVerifier,
) *identity.Authenticator {
	t.Helper()
	return identity.NewAuthenticatorWithHumanMembershipOIDC(
		servicePool,
		humanPool,
		newRolePool(
			t,
			database.DSN,
			"vela_human_membership_auth_login",
			"vela-human-membership-auth-password",
		),
		pepper,
		verifier,
	)
}

func newHumanMembershipAdministrationService(
	t *testing.T,
	database testDatabase,
	servicePool *pgxpool.Pool,
	pepper []byte,
	oidcIssuer string,
) (*identity.AdministrationService, error) {
	t.Helper()
	return identity.NewAdministrationServiceWithHumanMembership(
		servicePool,
		newRolePool(
			t,
			database.DSN,
			"vela_human_membership_request_login",
			"vela-human-membership-request-password",
		),
		pepper,
		oidcIssuer,
	)
}
