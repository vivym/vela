//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
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
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/artifactaccess"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/webhook"
)

func TestProjectAdminCreatesServicePrincipalThroughProductionHTTPPath(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	adminID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		adminID,
		"service-principal-http-admin",
		nil,
		map[string][]string{testProjectID: {"ProjectAdmin"}},
	)
	authenticator := identity.NewAuthenticatorWithOIDC(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(t, database.DSN, "vela_human_auth_login", "vela-human-auth-password"),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "service-principal-http-admin",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	administration, err := identity.NewAdministrationService(
		newRolePool(
			t,
			database.DSN,
			"vela_identity_request_login",
			"vela-identity-request-password",
		),
		testCredentialPepper,
	)
	if err != nil {
		t.Fatalf("create identity Administration service: %v", err)
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
		t.Fatalf("create identity Administration HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	request, err := http.NewRequest(
		http.MethodPost,
		server.URL+"/v1/projects/"+testProjectID+"/service-principals",
		bytes.NewBufferString(`{"display_name":"HTTP render caller"}`),
	)
	if err != nil {
		t.Fatalf("create Service Principal HTTP request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer service-principal-http-token")
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("create Service Principal over HTTP: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create Service Principal HTTP status = %d, want 201", response.StatusCode)
	}
	var created struct {
		ServicePrincipalID string `json:"service_principal_id"`
		ProjectID          string `json:"project_id"`
		DisplayName        string `json:"display_name"`
		CreatedAt          string `json:"created_at"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode created Service Principal: %v", err)
	}
	if _, err := uuid.Parse(created.ServicePrincipalID); err != nil ||
		created.ProjectID != testProjectID || created.DisplayName != "HTTP render caller" ||
		created.CreatedAt == "" {
		t.Fatalf("created Service Principal response = %#v; id error=%v", created, err)
	}

	doRequest := func(method, path string, body []byte) *http.Response {
		t.Helper()
		request, err := http.NewRequest(method, server.URL+path, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("create identity HTTP request: %v", err)
		}
		request.Header.Set("Authorization", "Bearer service-principal-http-token")
		if body != nil {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatalf("execute identity HTTP request: %v", err)
		}
		return response
	}
	basePath := "/v1/projects/" + testProjectID + "/service-principals/" +
		created.ServicePrincipalID
	issueBody, err := json.Marshal(map[string]any{
		"scopes":     []string{identity.ScopeJobsRead},
		"expires_at": time.Now().UTC().Add(24 * time.Hour).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("encode Credential issue request: %v", err)
	}
	issueResponse := doRequest(http.MethodPost, basePath+"/credentials", issueBody)
	if issueResponse.StatusCode != http.StatusCreated {
		defer issueResponse.Body.Close()
		t.Fatalf("issue Credential HTTP status = %d, want 201", issueResponse.StatusCode)
	}
	var issued struct {
		CredentialID     string   `json:"credential_id"`
		BearerCredential string   `json:"bearer_credential"`
		Scopes           []string `json:"scopes"`
	}
	if err := json.NewDecoder(issueResponse.Body).Decode(&issued); err != nil {
		issueResponse.Body.Close()
		t.Fatalf("decode issued Credential: %v", err)
	}
	issueResponse.Body.Close()
	if _, err := uuid.Parse(issued.CredentialID); err != nil ||
		!strings.HasPrefix(issued.BearerCredential, "vla_"+issued.CredentialID+".") ||
		len(issued.Scopes) != 1 || issued.Scopes[0] != identity.ScopeJobsRead {
		t.Fatalf("issued Credential response = %#v; id error=%v", issued, err)
	}

	principalListResponse := doRequest(
		http.MethodGet,
		"/v1/projects/"+testProjectID+"/service-principals?limit=100",
		nil,
	)
	if principalListResponse.StatusCode != http.StatusOK {
		defer principalListResponse.Body.Close()
		t.Fatalf("list Service Principals HTTP status = %d, want 200", principalListResponse.StatusCode)
	}
	var principalListBody map[string]json.RawMessage
	if err := json.NewDecoder(principalListResponse.Body).Decode(&principalListBody); err != nil {
		principalListResponse.Body.Close()
		t.Fatalf("decode Service Principal list: %v", err)
	}
	principalListResponse.Body.Close()
	if _, ok := principalListBody["service_principals"]; !ok {
		t.Fatalf("Service Principal list response = %#v", principalListBody)
	}

	credentialListResponse := doRequest(http.MethodGet, basePath+"/credentials?limit=100", nil)
	if credentialListResponse.StatusCode != http.StatusOK {
		defer credentialListResponse.Body.Close()
		t.Fatalf("list Credentials HTTP status = %d, want 200", credentialListResponse.StatusCode)
	}
	var credentialList struct {
		Credentials []map[string]any `json:"credentials"`
	}
	if err := json.NewDecoder(credentialListResponse.Body).Decode(&credentialList); err != nil {
		credentialListResponse.Body.Close()
		t.Fatalf("decode Credential list: %v", err)
	}
	credentialListResponse.Body.Close()
	if len(credentialList.Credentials) != 1 ||
		credentialList.Credentials[0]["credential_id"] != issued.CredentialID {
		t.Fatalf("Credential list response = %#v", credentialList)
	}
	if _, exposed := credentialList.Credentials[0]["bearer_credential"]; exposed {
		t.Fatalf("Credential list exposes bearer material: %#v", credentialList)
	}
	if _, exposed := credentialList.Credentials[0]["secret_digest"]; exposed {
		t.Fatalf("Credential list exposes digest: %#v", credentialList)
	}

	revokeResponse := doRequest(
		http.MethodPost,
		basePath+"/credentials/"+issued.CredentialID+"/revoke",
		nil,
	)
	if revokeResponse.StatusCode != http.StatusOK {
		defer revokeResponse.Body.Close()
		t.Fatalf("revoke Credential HTTP status = %d, want 200", revokeResponse.StatusCode)
	}
	var revoked struct {
		RevokedAt *time.Time `json:"revoked_at"`
	}
	if err := json.NewDecoder(revokeResponse.Body).Decode(&revoked); err != nil {
		revokeResponse.Body.Close()
		t.Fatalf("decode revoked Credential: %v", err)
	}
	revokeResponse.Body.Close()
	if revoked.RevokedAt == nil {
		t.Fatal("revoke Credential response lacks revoked_at")
	}

	disableResponse := doRequest(http.MethodPost, basePath+"/disable", nil)
	if disableResponse.StatusCode != http.StatusOK {
		defer disableResponse.Body.Close()
		t.Fatalf("disable Service Principal HTTP status = %d, want 200", disableResponse.StatusCode)
	}
	var disabled struct {
		DisabledAt *time.Time `json:"disabled_at"`
	}
	if err := json.NewDecoder(disableResponse.Body).Decode(&disabled); err != nil {
		disableResponse.Body.Close()
		t.Fatalf("decode disabled Service Principal: %v", err)
	}
	disableResponse.Body.Close()
	if disabled.DisabledAt == nil {
		t.Fatal("disable Service Principal response lacks disabled_at")
	}
}

func TestProjectAdminCreatesAndListsServicePrincipal(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	adminID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		adminID,
		"service-principal-admin",
		nil,
		map[string][]string{testProjectID: {"ProjectAdmin"}},
	)
	authenticator := identity.NewAuthenticatorWithOIDC(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(t, database.DSN, "vela_human_auth_login", "vela-human-auth-password"),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "service-principal-admin",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	principal, err := authenticator.Authenticate(context.Background(), "service-principal-admin-token")
	if err != nil {
		t.Fatalf("authenticate ProjectAdmin: %v", err)
	}
	projectID := uuid.MustParse(testProjectID)
	principal, ok := principal.ForProject(projectID)
	if !ok {
		t.Fatal("ProjectAdmin lacks Project authorization")
	}
	identityRequestPool := newRolePool(
		t,
		database.DSN,
		"vela_identity_request_login",
		"vela-identity-request-password",
	)
	service, err := identity.NewAdministrationService(
		identityRequestPool,
		testCredentialPepper,
	)
	if err != nil {
		t.Fatalf("create identity Administration service: %v", err)
	}

	created, err := service.CreateServicePrincipal(
		context.Background(),
		principal,
		projectID,
		identity.CreateServicePrincipalRequest{DisplayName: "Render integration"},
	)
	if err != nil {
		t.Fatalf("create Service Principal: %v", err)
	}
	if created.ID == uuid.Nil || created.ProjectID != projectID ||
		created.DisplayName != "Render integration" || created.DisabledAt != nil {
		t.Fatalf("created Service Principal = %#v", created)
	}

	listed, err := service.ListServicePrincipals(
		context.Background(), principal, projectID, 100,
	)
	if err != nil {
		t.Fatalf("list Service Principals: %v", err)
	}
	if len(listed) != 2 || listed[1] != created {
		t.Fatalf("listed Service Principals = %#v, want seeded Principal then %#v", listed, created)
	}
}

func TestServicePrincipalDisplayNameUsesOpenAPICharacterLimit(t *testing.T) {
	fixture := newIdentityAdministrationFixture(t, "unicode-service-principal-admin")
	displayName := strings.Repeat("服", 200)

	created, err := fixture.service.CreateServicePrincipal(
		context.Background(),
		fixture.actor,
		fixture.projectID,
		identity.CreateServicePrincipalRequest{DisplayName: displayName},
	)
	if err != nil {
		t.Fatalf("create Service Principal with 200-character display name: %v", err)
	}
	if created.DisplayName != displayName {
		t.Fatalf("created Service Principal display name = %q, want %q", created.DisplayName, displayName)
	}
}

func TestServiceAuthenticationPreservesInactiveOrganizationDomainResponse(t *testing.T) {
	server, admin := newAdmissionServer(t)
	if _, err := admin.Exec(
		"UPDATE customer_organizations SET status = 'SUSPENDED' WHERE id = $1",
		testOrganizationID,
	); err != nil {
		t.Fatalf("suspend Customer Organization: %v", err)
	}

	result := submitJob(t, server.URL, "inactive-organization-auth-response", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"preserve authentication response"
	}`))
	wantBody := []byte("{\"code\":\"organization_inactive\",\"message\":\"Customer Organization is not active\"}\n")
	if result.StatusCode != http.StatusForbidden || !bytes.Equal(result.Body, wantBody) {
		t.Fatalf(
			"inactive Customer Organization response = %d %s, want 403 %s",
			result.StatusCode,
			result.Body,
			wantBody,
		)
	}
}

func TestProjectAdminIssuesCredentialThatAuthenticatesAndListsWithoutSecret(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	adminID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		adminID,
		"credential-issuer",
		nil,
		map[string][]string{testProjectID: {"ProjectAdmin"}},
	)
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	authenticator := identity.NewAuthenticatorWithOIDC(
		authPool,
		newRolePool(t, database.DSN, "vela_human_auth_login", "vela-human-auth-password"),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "credential-issuer",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	actor, err := authenticator.Authenticate(context.Background(), "credential-issuer-token")
	if err != nil {
		t.Fatalf("authenticate credential issuer: %v", err)
	}
	projectID := uuid.MustParse(testProjectID)
	actor, ok := actor.ForProject(projectID)
	if !ok {
		t.Fatal("credential issuer lacks Project authorization")
	}
	identityRequestPool := newRolePool(
		t,
		database.DSN,
		"vela_identity_request_login",
		"vela-identity-request-password",
	)
	service, err := identity.NewAdministrationService(
		identityRequestPool,
		testCredentialPepper,
	)
	if err != nil {
		t.Fatalf("create identity Administration service: %v", err)
	}
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
}

func TestCredentialRotationRevocationAndServicePrincipalDisableArePermanent(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	adminID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		adminID,
		"credential-lifecycle-admin",
		nil,
		map[string][]string{testProjectID: {"ProjectAdmin"}},
	)
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	authenticator := identity.NewAuthenticatorWithOIDC(
		authPool,
		newRolePool(t, database.DSN, "vela_human_auth_login", "vela-human-auth-password"),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "credential-lifecycle-admin",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	actor, err := authenticator.Authenticate(context.Background(), "credential-lifecycle-token")
	if err != nil {
		t.Fatalf("authenticate lifecycle admin: %v", err)
	}
	projectID := uuid.MustParse(testProjectID)
	actor, ok := actor.ForProject(projectID)
	if !ok {
		t.Fatal("lifecycle admin lacks Project authorization")
	}
	service, err := identity.NewAdministrationService(
		newRolePool(
			t,
			database.DSN,
			"vela_identity_request_login",
			"vela-identity-request-password",
		),
		testCredentialPepper,
	)
	if err != nil {
		t.Fatalf("create identity Administration service: %v", err)
	}
	target, err := service.CreateServicePrincipal(
		context.Background(),
		actor,
		projectID,
		identity.CreateServicePrincipalRequest{DisplayName: "Rotating caller"},
	)
	if err != nil {
		t.Fatalf("create lifecycle target: %v", err)
	}
	issue := func(scope string) identity.IssuedCredential {
		t.Helper()
		issued, err := service.IssueCredential(
			context.Background(),
			actor,
			projectID,
			target.ID,
			identity.IssueCredentialRequest{
				Scopes: []string{scope}, ExpiresAt: time.Now().UTC().Add(24 * time.Hour),
			},
		)
		if err != nil {
			t.Fatalf("issue rotating Credential: %v", err)
		}
		return issued
	}
	first := issue(identity.ScopeJobsRead)
	second := issue(identity.ScopeJobsSubmit)
	serviceAuthenticator := identity.NewAuthenticator(authPool, testCredentialPepper)
	for _, token := range []string{first.BearerCredential, second.BearerCredential} {
		if _, err := serviceAuthenticator.Authenticate(context.Background(), token); err != nil {
			t.Fatalf("authenticate overlapping Credential: %v", err)
		}
	}

	revoked, err := service.RevokeCredential(
		context.Background(), actor, projectID, target.ID, first.Credential.ID,
	)
	if err != nil {
		t.Fatalf("revoke first Credential: %v", err)
	}
	if revoked.RevokedAt == nil {
		t.Fatalf("revoked Credential = %#v", revoked)
	}
	replayed, err := service.RevokeCredential(
		context.Background(), actor, projectID, target.ID, first.Credential.ID,
	)
	if err != nil {
		t.Fatalf("replay Credential revocation: %v", err)
	}
	if replayed.RevokedAt == nil || !replayed.RevokedAt.Equal(*revoked.RevokedAt) {
		t.Fatalf("replayed revocation = %#v, want %#v", replayed, revoked)
	}
	if _, err := serviceAuthenticator.Authenticate(
		context.Background(), first.BearerCredential,
	); !errors.Is(err, identity.ErrInvalidCredential) {
		t.Fatalf("revoked Credential authentication error = %v", err)
	}
	if _, err := serviceAuthenticator.Authenticate(
		context.Background(), second.BearerCredential,
	); err != nil {
		t.Fatalf("overlapping Credential stopped after peer revocation: %v", err)
	}

	disabled, err := service.DisableServicePrincipal(
		context.Background(), actor, projectID, target.ID,
	)
	if err != nil {
		t.Fatalf("disable Service Principal: %v", err)
	}
	if disabled.DisabledAt == nil {
		t.Fatalf("disabled Service Principal = %#v", disabled)
	}
	disabledReplay, err := service.DisableServicePrincipal(
		context.Background(), actor, projectID, target.ID,
	)
	if err != nil {
		t.Fatalf("replay Service Principal disable: %v", err)
	}
	if disabledReplay.DisabledAt == nil ||
		!disabledReplay.DisabledAt.Equal(*disabled.DisabledAt) {
		t.Fatalf("replayed disable = %#v, want %#v", disabledReplay, disabled)
	}
	if _, err := serviceAuthenticator.Authenticate(
		context.Background(), second.BearerCredential,
	); !errors.Is(err, identity.ErrInvalidCredential) {
		t.Fatalf("disabled Principal Credential authentication error = %v", err)
	}
	if _, err := service.IssueCredential(
		context.Background(),
		actor,
		projectID,
		target.ID,
		identity.IssueCredentialRequest{
			Scopes: []string{identity.ScopeJobsRead}, ExpiresAt: time.Now().UTC().Add(time.Hour),
		},
	); !administrationFailureHasCode(err, identity.AdministrationFailureNotFound) {
		t.Fatalf("issue after disable error = %v, want not_found", err)
	}

	var revokeEvents, disableEvents int
	if err := database.Admin.QueryRow(`
		SELECT
			count(*) FILTER (WHERE action = 'CREDENTIAL_REVOKED'),
			count(*) FILTER (WHERE action = 'SERVICE_PRINCIPAL_DISABLED')
		FROM project_identity_events
		WHERE target_principal_id = $1
	`, target.ID).Scan(&revokeEvents, &disableEvents); err != nil {
		t.Fatalf("read lifecycle audit events: %v", err)
	}
	if revokeEvents != 1 || disableEvents != 1 {
		t.Fatalf("lifecycle audit events = revoke %d disable %d", revokeEvents, disableEvents)
	}
}

func TestServicePrincipalAdministrationRequiresCurrentHumanProjectAdmin(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	projectID := uuid.MustParse(testProjectID)
	adminID := uuid.New()
	developerID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		adminID,
		"current-project-admin",
		nil,
		map[string][]string{testProjectID: {"ProjectAdmin"}},
	)
	seedHumanRoleFixture(
		t,
		database.Admin,
		developerID,
		"identity-developer",
		nil,
		map[string][]string{testProjectID: {"Developer"}},
	)
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	humanAuthPool := newRolePool(
		t, database.DSN, "vela_human_auth_login", "vela-human-auth-password",
	)
	authenticateHuman := func(subject, token string) identity.Principal {
		t.Helper()
		principal, err := identity.NewAuthenticatorWithOIDC(
			authPool,
			humanAuthPool,
			testCredentialPepper,
			staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
				Issuer: "https://identity.example.com", Subject: subject,
				ExpiresAt: time.Now().UTC().Add(time.Hour),
			}},
		).Authenticate(context.Background(), token)
		if err != nil {
			t.Fatalf("authenticate %s: %v", subject, err)
		}
		return principal
	}
	admin, ok := authenticateHuman("current-project-admin", "current-admin-token").ForProject(projectID)
	if !ok {
		t.Fatal("ProjectAdmin lacks Project authorization")
	}
	developer, ok := authenticateHuman("identity-developer", "identity-developer-token").ForProject(projectID)
	if !ok {
		t.Fatal("Developer lacks existing Project authorization")
	}
	identityRequestPool := newRolePool(
		t,
		database.DSN,
		"vela_identity_request_login",
		"vela-identity-request-password",
	)
	service, err := identity.NewAdministrationService(
		identityRequestPool,
		testCredentialPepper,
	)
	if err != nil {
		t.Fatalf("create identity Administration service: %v", err)
	}
	created, err := service.CreateServicePrincipal(
		context.Background(),
		admin,
		projectID,
		identity.CreateServicePrincipalRequest{DisplayName: "Authorization baseline"},
	)
	if err != nil {
		t.Fatalf("create authorization baseline: %v", err)
	}

	if _, err := service.CreateServicePrincipal(
		context.Background(),
		developer,
		projectID,
		identity.CreateServicePrincipalRequest{DisplayName: "Developer escape"},
	); !administrationFailureHasCode(err, identity.AdministrationFailureForbidden) {
		t.Fatalf("Developer administration error = %v, want forbidden", err)
	}
	if _, err := service.CreateServicePrincipal(
		context.Background(),
		admin,
		uuid.New(),
		identity.CreateServicePrincipalRequest{DisplayName: "Cross Project escape"},
	); !administrationFailureHasCode(err, identity.AdministrationFailureForbidden) {
		t.Fatalf("cross-Project administration error = %v, want forbidden", err)
	}

	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'service_principals:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("inject legacy Service management scope: %v", err)
	}
	serviceActor, err := identity.NewAuthenticator(
		authPool, testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate legacy Service Credential: %v", err)
	}
	if _, err := service.CreateServicePrincipal(
		context.Background(),
		serviceActor,
		projectID,
		identity.CreateServicePrincipalRequest{DisplayName: "Service escape"},
	); !administrationFailureHasCode(err, identity.AdministrationFailureForbidden) {
		t.Fatalf("Service administration error = %v, want forbidden", err)
	}
	_, err = identityRequestPool.Exec(context.Background(), `
		SELECT * FROM vela_set_identity_admin_context($1, $2, $3)
	`,
		serviceActor.CredentialID,
		serviceActor.RequestContextProof(),
		identity.ScopeServicePrincipalsManage,
	)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
		t.Fatalf("Service database administration context error = %v, want SQLSTATE 42501", err)
	}

	if _, err := database.Admin.Exec(`
		DELETE FROM project_role_bindings
		WHERE organization_id = $1 AND project_id = $2
		  AND principal_id = $3 AND role = 'ProjectAdmin'
	`, testOrganizationID, testProjectID, adminID); err != nil {
		t.Fatalf("remove ProjectAdmin role after authentication: %v", err)
	}
	if _, err := service.IssueCredential(
		context.Background(),
		admin,
		projectID,
		created.ID,
		identity.IssueCredentialRequest{
			Scopes: []string{identity.ScopeJobsRead}, ExpiresAt: time.Now().UTC().Add(time.Hour),
		},
	); !administrationFailureHasCode(err, identity.AdministrationFailureUnauthorized) {
		t.Fatalf("stale ProjectAdmin session error = %v, want unauthorized", err)
	}

	var principals, events, credentials int
	if err := database.Admin.QueryRow(`
		SELECT
			count(*) FILTER (WHERE principal_id <> $1) FROM service_principals;
	`, testPrincipalID).Scan(&principals); err != nil {
		t.Fatalf("count managed Service Principals: %v", err)
	}
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM project_identity_events
		WHERE target_principal_id = $1
	`, created.ID).Scan(&events); err != nil {
		t.Fatalf("count identity events: %v", err)
	}
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM credentials WHERE principal_id = $1
	`, created.ID).Scan(&credentials); err != nil {
		t.Fatalf("count rejected Credentials: %v", err)
	}
	if principals != 1 || events != 1 || credentials != 0 {
		t.Fatalf(
			"rejected administration effects = principals %d events %d credentials %d",
			principals,
			events,
			credentials,
		)
	}
}

func TestCredentialValidationAndIdentityAuditKeepBearerMaterialOutOfStorage(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	projectID := uuid.MustParse(testProjectID)
	adminID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		adminID,
		"credential-audit-admin",
		nil,
		map[string][]string{testProjectID: {"ProjectAdmin"}},
	)
	authenticator := identity.NewAuthenticatorWithOIDC(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(t, database.DSN, "vela_human_auth_login", "vela-human-auth-password"),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer: "https://identity.example.com", Subject: "credential-audit-admin",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	actor, err := authenticator.Authenticate(context.Background(), "credential-audit-token")
	if err != nil {
		t.Fatalf("authenticate credential audit admin: %v", err)
	}
	actor, ok := actor.ForProject(projectID)
	if !ok {
		t.Fatal("credential audit admin lacks Project authorization")
	}
	identityRequestPool := newRolePool(
		t,
		database.DSN,
		"vela_identity_request_login",
		"vela-identity-request-password",
	)
	service, err := identity.NewAdministrationService(identityRequestPool, testCredentialPepper)
	if err != nil {
		t.Fatalf("create identity Administration service: %v", err)
	}
	target, err := service.CreateServicePrincipal(
		context.Background(),
		actor,
		projectID,
		identity.CreateServicePrincipalRequest{DisplayName: "Audited caller"},
	)
	if err != nil {
		t.Fatalf("create audit target: %v", err)
	}

	for _, test := range []struct {
		name    string
		scopes  []string
		expires time.Time
	}{
		{name: "empty scopes", expires: time.Now().UTC().Add(time.Hour)},
		{name: "duplicate scopes", scopes: []string{identity.ScopeJobsRead, identity.ScopeJobsRead}, expires: time.Now().UTC().Add(time.Hour)},
		{name: "unknown scope", scopes: []string{"jobs:delete"}, expires: time.Now().UTC().Add(time.Hour)},
		{name: "administrative scope", scopes: []string{identity.ScopeServicePrincipalsManage}, expires: time.Now().UTC().Add(time.Hour)},
		{name: "expired", scopes: []string{identity.ScopeJobsRead}, expires: time.Now().UTC().Add(-time.Second)},
		{name: "beyond maximum", scopes: []string{identity.ScopeJobsRead}, expires: time.Now().UTC().Add(367 * 24 * time.Hour)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := service.IssueCredential(
				context.Background(),
				actor,
				projectID,
				target.ID,
				identity.IssueCredentialRequest{Scopes: test.scopes, ExpiresAt: test.expires},
			); !administrationFailureHasCode(err, identity.AdministrationFailureInvalidRequest) {
				t.Fatalf("invalid Credential error = %v, want invalid_request", err)
			}
		})
	}

	tx, err := identityRequestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin database scope validation transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(context.Background(), `
		SELECT * FROM vela_set_identity_admin_context($1, $2, $3)
	`, actor.CredentialID, actor.RequestContextProof(), identity.ScopeServicePrincipalsManage); err != nil {
		t.Fatalf("establish database scope validation context: %v", err)
	}
	_, err = tx.Exec(context.Background(), `
		SELECT * FROM vela_issue_service_credential(
			$1, $2, $3, $4, ARRAY['jobs:delete'], clock_timestamp() + interval '1 hour'
		)
	`, uuid.New(), projectID, target.ID, make([]byte, 32))
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "22023" {
		t.Fatalf("database invalid scope error = %v, want SQLSTATE 22023", err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback database scope validation: %v", err)
	}

	issued, err := service.IssueCredential(
		context.Background(),
		actor,
		projectID,
		target.ID,
		identity.IssueCredentialRequest{
			Scopes:    []string{identity.ScopeJobsSubmit, identity.ScopeJobsRead},
			ExpiresAt: time.Now().UTC().Add(24 * time.Hour),
		},
	)
	if err != nil {
		t.Fatalf("issue audited Credential: %v", err)
	}
	_, encodedSecret, ok := strings.Cut(issued.BearerCredential, ".")
	if !ok {
		t.Fatalf("issued bearer Credential format = %q", issued.BearerCredential)
	}
	rawSecret, err := base64.RawURLEncoding.DecodeString(encodedSecret)
	if err != nil {
		t.Fatalf("decode issued bearer secret: %v", err)
	}
	defer clear(rawSecret)
	var storedDigest []byte
	var storedScopesJSON []byte
	if err := database.Admin.QueryRow(`
		SELECT secret_digest, array_to_json(scopes)
		FROM credentials
		WHERE id = $1
	`, issued.Credential.ID).Scan(&storedDigest, &storedScopesJSON); err != nil {
		t.Fatalf("read stored Credential proof: %v", err)
	}
	var storedScopes []string
	if err := json.Unmarshal(storedScopesJSON, &storedScopes); err != nil {
		t.Fatalf("decode stored Credential scopes: %v", err)
	}
	if len(storedDigest) != 32 || bytes.Equal(storedDigest, rawSecret) ||
		len(storedScopes) != 2 || storedScopes[0] != identity.ScopeJobsRead ||
		storedScopes[1] != identity.ScopeJobsSubmit {
		t.Fatalf("stored Credential evidence = digest bytes %d scopes %v", len(storedDigest), storedScopes)
	}

	var createActor, issueActor, createSession, issueSession uuid.UUID
	var createDetails, issueDetails string
	if err := database.Admin.QueryRow(`
		SELECT actor_principal_id, actor_session_id, details::text
		FROM project_identity_events
		WHERE target_principal_id = $1 AND action = 'SERVICE_PRINCIPAL_CREATED'
	`, target.ID).Scan(
		&createActor,
		&createSession,
		&createDetails,
	); err != nil {
		t.Fatalf("read Service Principal creation audit attribution: %v", err)
	}
	if err := database.Admin.QueryRow(`
		SELECT actor_principal_id, actor_session_id, details::text
		FROM project_identity_events
		WHERE target_principal_id = $1 AND action = 'CREDENTIAL_ISSUED'
	`, target.ID).Scan(&issueActor, &issueSession, &issueDetails); err != nil {
		t.Fatalf("read Credential issue audit attribution: %v", err)
	}
	if createActor != adminID || issueActor != adminID ||
		createSession != actor.CredentialID || issueSession != actor.CredentialID ||
		strings.Contains(createDetails, issued.BearerCredential) ||
		strings.Contains(issueDetails, issued.BearerCredential) ||
		strings.Contains(issueDetails, encodedSecret) ||
		strings.Contains(issueDetails, base64.RawStdEncoding.EncodeToString(storedDigest)) {
		t.Fatalf(
			"identity audit attribution/details = actors %s/%s sessions %s/%s details %q/%q",
			createActor,
			issueActor,
			createSession,
			issueSession,
			createDetails,
			issueDetails,
		)
	}

	for _, statement := range []string{
		"UPDATE project_identity_events SET details = '{}'::jsonb WHERE target_principal_id = $1",
		"DELETE FROM project_identity_events WHERE target_principal_id = $1",
	} {
		_, err := database.Admin.Exec(statement, target.ID)
		if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
			postgresError.ConstraintName != "identity_administration_evidence_immutable" {
			t.Fatalf("identity audit mutation error = %v, want immutable refusal", err)
		}
	}
}

func TestServicePrincipalAdministrationMigrationAllowsLegacyRowsAndEmptyDownUp(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")

	if err := goose.DownTo(database.Admin, migrations, 12); err != nil {
		t.Fatalf("contract unused Service Principal administration migration: %v", err)
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 12 {
		t.Fatalf("migration version after Service Principal administration Down = %d error=%v", version, err)
	}
	var (
		roleExists       bool
		schemaUsageGrant bool
		functionExists   bool
		disabledAtExists bool
	)
	if err := database.Admin.QueryRow(`
		SELECT
			EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vela_identity_request'),
			EXISTS (
				SELECT 1
				FROM pg_catalog.pg_namespace AS namespace
				CROSS JOIN LATERAL pg_catalog.aclexplode(
					coalesce(
						namespace.nspacl,
						pg_catalog.acldefault('n', namespace.nspowner)
					)
				) AS privilege
				JOIN pg_catalog.pg_roles AS grantee ON grantee.oid = privilege.grantee
				WHERE namespace.nspname = 'public'
				  AND grantee.rolname = 'vela_identity_request'
				  AND privilege.privilege_type = 'USAGE'
			),
			to_regprocedure('vela_create_service_principal(uuid,uuid,text)') IS NOT NULL,
			EXISTS (
				SELECT 1
				FROM information_schema.columns
				WHERE table_schema = 'public'
				  AND table_name = 'service_principals'
				  AND column_name = 'disabled_at'
			)
	`).Scan(&roleExists, &schemaUsageGrant, &functionExists, &disabledAtExists); err != nil {
		t.Fatalf("inspect contracted identity administration surface: %v", err)
	}
	if !roleExists || schemaUsageGrant || functionExists || disabledAtExists {
		t.Fatalf(
			"contracted identity surface = role %t direct schema usage %t function %t disabled_at %t",
			roleExists,
			schemaUsageGrant,
			functionExists,
			disabledAtExists,
		)
	}
	if _, err := identity.NewAuthenticator(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential()); err != nil {
		t.Fatalf("authenticate legacy Service Credential after migration Down: %v", err)
	}

	if err := goose.Up(database.Admin, migrations); err != nil {
		t.Fatalf("re-expand unused Service Principal administration migration: %v", err)
	}
	version, err = goose.GetDBVersion(database.Admin)
	if err != nil || version != 14 {
		t.Fatalf("migration version after Service Principal administration Down/Up = %d error=%v", version, err)
	}
	if err := database.Admin.QueryRow(`
		SELECT to_regprocedure('vela_create_service_principal(uuid,uuid,text)') IS NOT NULL
	`).Scan(&functionExists); err != nil {
		t.Fatalf("inspect re-expanded identity administration surface: %v", err)
	}
	if !functionExists {
		t.Fatal("re-expanded identity administration function is absent")
	}
}

func TestServicePrincipalAdministrationMigrationDownRefusesDurableEvidence(t *testing.T) {
	fixture := newIdentityAdministrationFixture(t, "identity-migration-admin")
	target, err := fixture.service.CreateServicePrincipal(
		context.Background(),
		fixture.actor,
		fixture.projectID,
		identity.CreateServicePrincipalRequest{DisplayName: "Durable identity evidence"},
	)
	if err != nil {
		t.Fatalf("create durable identity administration evidence: %v", err)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err = goose.DownTo(fixture.database.Admin, migrations, 12)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
		postgresError.ConstraintName != "identity_administration_contract_requires_empty_evidence" {
		t.Fatalf("identity administration migration Down error = %v, want named SQLSTATE 55000", err)
	}
	version, versionErr := goose.GetDBVersion(fixture.database.Admin)
	if versionErr != nil || version != 13 {
		t.Fatalf("migration version after refused identity Down = %d error=%v", version, versionErr)
	}
	var events int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*) FROM project_identity_events WHERE target_principal_id = $1
	`, target.ID).Scan(&events); err != nil {
		t.Fatalf("read preserved identity administration evidence: %v", err)
	}
	if events != 1 {
		t.Fatalf("preserved identity administration events = %d, want 1", events)
	}
}

func TestServicePrincipalDisableAndCredentialRevocationCannotBeReversedDirectly(t *testing.T) {
	fixture := newIdentityAdministrationFixture(t, "identity-transition-admin")
	target, err := fixture.service.CreateServicePrincipal(
		context.Background(),
		fixture.actor,
		fixture.projectID,
		identity.CreateServicePrincipalRequest{DisplayName: "Permanent identity state"},
	)
	if err != nil {
		t.Fatalf("create identity transition target: %v", err)
	}
	issued, err := fixture.service.IssueCredential(
		context.Background(),
		fixture.actor,
		fixture.projectID,
		target.ID,
		identity.IssueCredentialRequest{
			Scopes:    []string{identity.ScopeJobsRead},
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		},
	)
	if err != nil {
		t.Fatalf("issue identity transition Credential: %v", err)
	}
	revoked, err := fixture.service.RevokeCredential(
		context.Background(), fixture.actor, fixture.projectID, target.ID, issued.Credential.ID,
	)
	if err != nil || revoked.RevokedAt == nil {
		t.Fatalf("revoke identity transition Credential = %#v error=%v", revoked, err)
	}
	disabled, err := fixture.service.DisableServicePrincipal(
		context.Background(), fixture.actor, fixture.projectID, target.ID,
	)
	if err != nil || disabled.DisabledAt == nil {
		t.Fatalf("disable identity transition Service Principal = %#v error=%v", disabled, err)
	}

	internalPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	transitions := []struct {
		name       string
		statement  string
		argument   uuid.UUID
		constraint string
	}{
		{
			name:       "Credential revocation",
			statement:  "UPDATE credentials SET revoked_at = NULL WHERE id = $1",
			argument:   issued.Credential.ID,
			constraint: "service_credential_revocation_is_permanent",
		},
		{
			name:       "Service Principal disable",
			statement:  "UPDATE service_principals SET disabled_at = NULL WHERE principal_id = $1",
			argument:   target.ID,
			constraint: "service_principal_disable_is_permanent",
		},
	}
	executors := []struct {
		name string
		exec func(string, uuid.UUID) error
	}{
		{
			name: "restricted internal role",
			exec: func(statement string, argument uuid.UUID) error {
				_, err := internalPool.Exec(context.Background(), statement, argument)
				return err
			},
		},
		{
			name: "database administrator",
			exec: func(statement string, argument uuid.UUID) error {
				_, err := fixture.database.Admin.Exec(statement, argument)
				return err
			},
		},
	}
	for _, executor := range executors {
		for _, transition := range transitions {
			t.Run(executor.name+"/"+transition.name, func(t *testing.T) {
				err := executor.exec(transition.statement, transition.argument)
				var postgresError *pgconn.PgError
				if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
					postgresError.ConstraintName != transition.constraint {
					t.Fatalf("direct transition reversal error = %v, want named SQLSTATE 55000", err)
				}
			})
		}
	}

	var storedRevokedAt, storedDisabledAt time.Time
	if err := fixture.database.Admin.QueryRow(`
		SELECT credential.revoked_at, service.disabled_at
		FROM credentials AS credential
		JOIN service_principals AS service
		  ON service.organization_id = credential.organization_id
		 AND service.project_id = credential.project_id
		 AND service.principal_id = credential.principal_id
		WHERE credential.id = $1
	`, issued.Credential.ID).Scan(&storedRevokedAt, &storedDisabledAt); err != nil {
		t.Fatalf("read permanent identity transition state: %v", err)
	}
	if !storedRevokedAt.Equal(*revoked.RevokedAt) || !storedDisabledAt.Equal(*disabled.DisabledAt) {
		t.Fatalf(
			"identity transition state changed = revoked %s disabled %s",
			storedRevokedAt,
			storedDisabledAt,
		)
	}
}

func TestServicePrincipalAndCredentialOwnershipCannotBeChangedDirectly(t *testing.T) {
	fixture := newIdentityAdministrationFixture(t, "identity-ownership-admin")
	serviceTarget, err := fixture.service.CreateServicePrincipal(
		context.Background(),
		fixture.actor,
		fixture.projectID,
		identity.CreateServicePrincipalRequest{DisplayName: "Stable Project owner"},
	)
	if err != nil {
		t.Fatalf("create Service Principal ownership target: %v", err)
	}
	credentialTarget, err := fixture.service.CreateServicePrincipal(
		context.Background(),
		fixture.actor,
		fixture.projectID,
		identity.CreateServicePrincipalRequest{DisplayName: "Stable Credential owner"},
	)
	if err != nil {
		t.Fatalf("create Credential ownership target: %v", err)
	}
	issued, err := fixture.service.IssueCredential(
		context.Background(),
		fixture.actor,
		fixture.projectID,
		credentialTarget.ID,
		identity.IssueCredentialRequest{
			Scopes:    []string{identity.ScopeJobsRead},
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		},
	)
	if err != nil {
		t.Fatalf("issue Credential ownership target: %v", err)
	}
	otherProjectID := uuid.New()
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO projects (
			id, organization_id, display_name, queued_limit, running_limit
		) VALUES ($1, $2, 'Identity ownership target', 1, 1)
	`, otherProjectID, testOrganizationID); err != nil {
		t.Fatalf("seed alternate Project for ownership mutation: %v", err)
	}

	for _, mutation := range []struct {
		name       string
		statement  string
		arguments  []any
		constraint string
	}{
		{
			name: "Service Principal Project",
			statement: `
				UPDATE service_principals SET project_id = $2 WHERE principal_id = $1
			`,
			arguments:  []any{serviceTarget.ID, otherProjectID},
			constraint: "service_principal_identity_is_immutable",
		},
		{
			name: "Credential Service Principal",
			statement: `
				UPDATE credentials SET principal_id = $2 WHERE id = $1
			`,
			arguments:  []any{issued.Credential.ID, testPrincipalID},
			constraint: "service_credential_ownership_is_immutable",
		},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			_, err := fixture.database.Admin.Exec(mutation.statement, mutation.arguments...)
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
				postgresError.ConstraintName != mutation.constraint {
				t.Fatalf("direct ownership mutation error = %v, want named SQLSTATE 55000", err)
			}
		})
	}

	var storedProjectID, storedPrincipalID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT project_id FROM service_principals WHERE principal_id = $1),
			(SELECT principal_id FROM credentials WHERE id = $2)
	`, serviceTarget.ID, issued.Credential.ID).Scan(&storedProjectID, &storedPrincipalID); err != nil {
		t.Fatalf("read stable identity ownership: %v", err)
	}
	if storedProjectID != fixture.projectID || storedPrincipalID != credentialTarget.ID {
		t.Fatalf(
			"identity ownership changed = Project %s Credential Principal %s",
			storedProjectID,
			storedPrincipalID,
		)
	}
}

func TestConcurrentCredentialIssueAndServicePrincipalDisableLeaveNoActiveCredential(t *testing.T) {
	fixture := newIdentityAdministrationFixture(t, "identity-concurrency-admin")
	target, err := fixture.service.CreateServicePrincipal(
		context.Background(),
		fixture.actor,
		fixture.projectID,
		identity.CreateServicePrincipalRequest{DisplayName: "Concurrent identity state"},
	)
	if err != nil {
		t.Fatalf("create concurrent identity target: %v", err)
	}

	blocker, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin identity concurrency blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	var lockedID uuid.UUID
	if err := blocker.QueryRow(`
		SELECT principal_id
		FROM service_principals
		WHERE principal_id = $1
		FOR UPDATE
	`, target.ID).Scan(&lockedID); err != nil {
		t.Fatalf("lock concurrent identity target: %v", err)
	}

	type issueResult struct {
		issued identity.IssuedCredential
		err    error
	}
	issueResults := make(chan issueResult, 1)
	disableResults := make(chan error, 1)
	operationContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		issued, issueErr := fixture.service.IssueCredential(
			operationContext,
			fixture.actor,
			fixture.projectID,
			target.ID,
			identity.IssueCredentialRequest{
				Scopes:    []string{identity.ScopeJobsRead},
				ExpiresAt: time.Now().UTC().Add(time.Hour),
			},
		)
		issueResults <- issueResult{issued: issued, err: issueErr}
	}()
	go func() {
		_, disableErr := fixture.service.DisableServicePrincipal(
			operationContext, fixture.actor, fixture.projectID, target.ID,
		)
		disableResults <- disableErr
	}()
	waitForIdentityAdministrationLockWaiters(t, fixture.database.Admin, 2)
	if err := blocker.Commit(); err != nil {
		t.Fatalf("release identity concurrency blocker: %v", err)
	}

	issued := <-issueResults
	if issued.err != nil &&
		!administrationFailureHasCode(issued.err, identity.AdministrationFailureNotFound) {
		t.Fatalf("concurrent Credential issue error = %v", issued.err)
	}
	if disableErr := <-disableResults; disableErr != nil {
		t.Fatalf("concurrent Service Principal disable error = %v", disableErr)
	}

	var (
		disabledAt      *time.Time
		credentialCount int
		activeCount     int
		issueEvents     int
		disableEvents   int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			service.disabled_at,
			count(credential.id),
			count(credential.id) FILTER (WHERE credential.revoked_at IS NULL),
			(SELECT count(*) FROM project_identity_events AS event
			 WHERE event.target_principal_id = service.principal_id
			   AND event.action = 'CREDENTIAL_ISSUED'),
			(SELECT count(*) FROM project_identity_events AS event
			 WHERE event.target_principal_id = service.principal_id
			   AND event.action = 'SERVICE_PRINCIPAL_DISABLED')
		FROM service_principals AS service
		LEFT JOIN credentials AS credential
		  ON credential.organization_id = service.organization_id
		 AND credential.project_id = service.project_id
		 AND credential.principal_id = service.principal_id
		WHERE service.principal_id = $1
		GROUP BY service.principal_id, service.disabled_at
	`, target.ID).Scan(
		&disabledAt,
		&credentialCount,
		&activeCount,
		&issueEvents,
		&disableEvents,
	); err != nil {
		t.Fatalf("read concurrent identity terminal state: %v", err)
	}
	if disabledAt == nil || activeCount != 0 || disableEvents != 1 {
		t.Fatalf(
			"concurrent identity terminal state = disabled %v Credentials %d active %d disable events %d",
			disabledAt,
			credentialCount,
			activeCount,
			disableEvents,
		)
	}
	if issued.err == nil {
		if issued.issued.Credential.ID == uuid.Nil || credentialCount != 1 || issueEvents != 1 {
			t.Fatalf(
				"committed concurrent issue = %#v Credentials %d events %d",
				issued.issued,
				credentialCount,
				issueEvents,
			)
		}
	} else if credentialCount != 0 || issueEvents != 0 {
		t.Fatalf(
			"rejected concurrent issue left Credentials %d events %d",
			credentialCount,
			issueEvents,
		)
	}
}

func waitForIdentityAdministrationLockWaiters(t *testing.T, database *sql.DB, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var waiters int
		if err := database.QueryRow(`
			SELECT count(*)
			FROM pg_stat_activity
			WHERE usename IN (
				'vela_identity_request_login',
				'vela_human_membership_request_login'
			)
			  AND state = 'active'
			  AND wait_event_type = 'Lock'
		`).Scan(&waiters); err != nil {
			t.Fatalf("inspect identity administration lock waiters: %v", err)
		}
		if waiters >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("identity administration lock waiters did not reach %d", want)
}

type identityAdministrationFixture struct {
	database  testDatabase
	actor     identity.Principal
	projectID uuid.UUID
	service   *identity.AdministrationService
}

func newIdentityAdministrationFixture(t *testing.T, subject string) identityAdministrationFixture {
	t.Helper()
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	projectID := uuid.MustParse(testProjectID)
	seedHumanRoleFixture(
		t,
		database.Admin,
		uuid.New(),
		subject,
		nil,
		map[string][]string{testProjectID: {"ProjectAdmin"}},
	)
	authenticator := identity.NewAuthenticatorWithOIDC(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(t, database.DSN, "vela_human_auth_login", "vela-human-auth-password"),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   subject,
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	actor, err := authenticator.Authenticate(context.Background(), subject+"-token")
	if err != nil {
		t.Fatalf("authenticate identity administration fixture: %v", err)
	}
	actor, ok := actor.ForProject(projectID)
	if !ok {
		t.Fatal("identity administration fixture lacks Project authorization")
	}
	service, err := identity.NewAdministrationService(
		newRolePool(
			t,
			database.DSN,
			"vela_identity_request_login",
			"vela-identity-request-password",
		),
		testCredentialPepper,
	)
	if err != nil {
		t.Fatalf("create identity Administration service: %v", err)
	}
	return identityAdministrationFixture{
		database: database, actor: actor, projectID: projectID, service: service,
	}
}

func administrationFailureHasCode(err error, code identity.AdministrationFailureCode) bool {
	var failure *identity.AdministrationFailure
	return errors.As(err, &failure) && failure.Code == code
}
