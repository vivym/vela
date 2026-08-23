//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
	"github.com/vivym/vela/internal/organizationreporting"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/workercontrol"
)

const humanDeveloperPrincipalID = "00000000-0000-0000-0000-000000000081"
const humanProjectAdminPrincipalID = "00000000-0000-0000-0000-000000000082"
const humanProjectViewerPrincipalID = "00000000-0000-0000-0000-000000000083"

func TestHumanOIDCAuthenticationReturnsDeveloperProjectAuthorization(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		INSERT INTO principals (id, organization_id, kind, display_name)
		VALUES ($1, $2, 'HUMAN', 'Human Developer')
	`, humanDeveloperPrincipalID, testOrganizationID); err != nil {
		t.Fatalf("seed Human Developer Principal: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO human_oidc_bindings (
			organization_id, principal_id, issuer, subject, display_name
		) VALUES ($2, $1, 'https://identity.example.com', 'human-developer', 'Human Developer')
	`, humanDeveloperPrincipalID, testOrganizationID); err != nil {
		t.Fatalf("seed Human Developer OIDC binding: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO project_role_bindings (
			organization_id, project_id, principal_id, role, assigned_by_principal_id
		) VALUES ($2, $3, $1, 'Developer', $1)
	`, humanDeveloperPrincipalID, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("seed Human Developer role: %v", err)
	}

	authenticator := identity.NewAuthenticatorWithOIDC(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(t, database.DSN, "vela_human_auth_login", "vela-human-auth-password"),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "human-developer",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	principal, err := authenticator.Authenticate(context.Background(), "human-oidc-access-token")
	if err != nil {
		t.Fatalf("authenticate Human Developer: %v", err)
	}
	contextual, ok := principal.ForProject(uuid.MustParse(testProjectID))
	if !ok {
		t.Fatal("Human Developer has no authorization for its assigned Project")
	}
	if contextual.Kind != identity.PrincipalKindHuman ||
		contextual.OrganizationID != uuid.MustParse(testOrganizationID) ||
		contextual.ProjectID != uuid.MustParse(testProjectID) ||
		contextual.PrincipalID != uuid.MustParse(humanDeveloperPrincipalID) ||
		contextual.CredentialID == uuid.Nil || len(contextual.RequestContextProof()) != 32 {
		t.Fatalf("contextual Human Principal = %#v", contextual)
	}
	for _, scope := range []string{
		identity.ScopeJobsSubmit,
		identity.ScopeJobsRead,
		identity.ScopeJobsCancel,
		identity.ScopeArtifactsRead,
	} {
		if !contextual.HasScope(scope) {
			t.Fatalf("Human Developer lacks %s", scope)
		}
	}
	if contextual.HasScope(identity.ScopeWebhooksManage) {
		t.Fatal("Human Developer unexpectedly has Webhook management permission")
	}
	if _, ok := principal.ForProject(uuid.New()); ok {
		t.Fatal("Human Developer authorized an unrelated Project")
	}

	var sessions int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM human_auth_sessions
		WHERE principal_id = $1
		  AND project_id = $2
		  AND expires_at > clock_timestamp()
	`, humanDeveloperPrincipalID, testProjectID).Scan(&sessions); err != nil {
		t.Fatalf("read Human authorization sessions: %v", err)
	}
	if sessions != 1 {
		t.Fatalf("active Human authorization sessions = %d, want 1", sessions)
	}
}

func TestHumanRequestContextRevalidatesProjectRole(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		INSERT INTO principals (id, organization_id, kind, display_name)
		VALUES ($1, $2, 'HUMAN', 'Human Developer')
	`, humanDeveloperPrincipalID, testOrganizationID); err != nil {
		t.Fatalf("seed Human Developer Principal: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO human_oidc_bindings (
			organization_id, principal_id, issuer, subject, display_name
		) VALUES ($2, $1, 'https://identity.example.com', 'human-developer', 'Human Developer')
	`, humanDeveloperPrincipalID, testOrganizationID); err != nil {
		t.Fatalf("seed Human Developer OIDC binding: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO project_role_bindings (
			organization_id, project_id, principal_id, role, assigned_by_principal_id
		) VALUES ($2, $3, $1, 'Developer', $1)
	`, humanDeveloperPrincipalID, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("seed Human Developer role: %v", err)
	}
	authenticator := identity.NewAuthenticatorWithOIDC(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(t, database.DSN, "vela_human_auth_login", "vela-human-auth-password"),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "human-developer",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	principal, err := authenticator.Authenticate(context.Background(), "human-oidc-access-token")
	if err != nil {
		t.Fatalf("authenticate Human Developer: %v", err)
	}
	contextual, ok := principal.ForProject(uuid.MustParse(testProjectID))
	if !ok {
		t.Fatal("Human Developer has no authorization for its assigned Project")
	}
	requestPool := newRolePool(
		t, database.DSN, "vela_request_login", "vela-request-password",
	)
	tx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin Human request transaction: %v", err)
	}
	var organizationID, projectID, principalID uuid.UUID
	var transactionTime time.Time
	if err := tx.QueryRow(context.Background(), `
		SELECT * FROM vela_set_request_context($1, $2, 'jobs:submit')
	`, contextual.CredentialID, contextual.RequestContextProof()).Scan(
		&organizationID, &projectID, &principalID, &transactionTime,
	); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatalf("establish Human request context: %v", err)
	}
	if organizationID != contextual.OrganizationID || projectID != contextual.ProjectID ||
		principalID != contextual.PrincipalID || transactionTime.IsZero() {
		_ = tx.Rollback(context.Background())
		t.Fatalf(
			"Human request context = organization %s project %s principal %s time %s",
			organizationID, projectID, principalID, transactionTime,
		)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit Human request transaction: %v", err)
	}

	if _, err := database.Admin.Exec(`
		DELETE FROM project_role_bindings
		WHERE organization_id = $1 AND project_id = $2 AND principal_id = $3
	`, testOrganizationID, testProjectID, humanDeveloperPrincipalID); err != nil {
		t.Fatalf("remove Human Developer role: %v", err)
	}
	_, err = admission.NewLegacyService(requestPool).Submit(
		context.Background(),
		contextual,
		uuid.MustParse(testProjectID),
		"removed-human-role-no-mutation",
		admission.Request{
			Model:            "minimax-h3",
			GenerationPreset: "balanced",
			ServiceClass:     "standard",
			OutputSpec:       "video-1080p-5s-24fps",
			GenerationCount:  1,
			Prompt:           "removed Human role must not mutate",
		},
	)
	var failure *admission.Failure
	if !errors.As(err, &failure) || failure.Code != admission.FailureCodeForbidden {
		t.Fatalf("Admission after Human role removal error = %v, want Forbidden", err)
	}
	assertNoAdmissionEffects(t, database.Admin)
	staleTx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin stale Human request transaction: %v", err)
	}
	defer func() { _ = staleTx.Rollback(context.Background()) }()
	_, err = staleTx.Exec(context.Background(), `
		SELECT * FROM vela_set_request_context($1, $2, 'jobs:submit')
	`, contextual.CredentialID, contextual.RequestContextProof())
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "28000" {
		t.Fatalf("stale Human role error = %v, want SQLSTATE 28000", err)
	}
}

func TestHumanDeveloperCanSubmitJobThroughProductionHTTPPath(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		INSERT INTO principals (id, organization_id, kind, display_name)
		VALUES ($1, $2, 'HUMAN', 'Human Developer')
	`, humanDeveloperPrincipalID, testOrganizationID); err != nil {
		t.Fatalf("seed Human Developer Principal: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO human_oidc_bindings (
			organization_id, principal_id, issuer, subject, display_name
		) VALUES ($2, $1, 'https://identity.example.com', 'human-developer', 'Human Developer')
	`, humanDeveloperPrincipalID, testOrganizationID); err != nil {
		t.Fatalf("seed Human Developer OIDC binding: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO project_role_bindings (
			organization_id, project_id, principal_id, role, assigned_by_principal_id
		) VALUES ($2, $3, $1, 'Developer', $1)
	`, humanDeveloperPrincipalID, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("seed Human Developer role: %v", err)
	}
	authenticator := identity.NewAuthenticatorWithOIDC(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(t, database.DSN, "vela_human_auth_login", "vela-human-auth-password"),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "human-developer",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	requestPool := newRolePool(
		t, database.DSN, "vela_request_login", "vela-request-password",
	)
	artifactPool := newRolePool(
		t, database.DSN, "vela_artifact_request_login", "vela-artifact-request-password",
	)
	cancelPool := newRolePool(
		t, database.DSN, "vela_cancel_login", "vela-cancel-password",
	)
	internalPool := newRolePool(
		t, database.DSN, "vela_internal_login", "vela-internal-password",
	)
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          authenticator,
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              &retention.Service{},
		Admission:              admission.NewLegacyService(requestPool),
		Cancellation:           cancellation.NewService(cancelPool, internalPool),
		Artifacts:              testArtifactAccessService(artifactPool),
		Webhooks:               testWebhookService(t, webhookRequestPoolForDatabase(t, database)),
	})
	if err != nil {
		t.Fatalf("create Human HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	requestBody := []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"Human OIDC production path"
	}`)
	request, err := http.NewRequest(
		http.MethodPost,
		server.URL+"/v1/projects/"+testProjectID+"/jobs",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		t.Fatalf("create Human submit request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer human-oidc-access-token")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "human-developer-submit")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("submit Human Job: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("Human submit status = %d, want 202", response.StatusCode)
	}
	var submitted jobResponse
	if err := json.NewDecoder(response.Body).Decode(&submitted); err != nil {
		t.Fatalf("decode Human submit response: %v", err)
	}
	var attributedPrincipal uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT created_by_principal_id
		FROM jobs
		WHERE id = $1
	`, submitted.JobID).Scan(&attributedPrincipal); err != nil {
		t.Fatalf("read Human Job attribution: %v", err)
	}
	if attributedPrincipal != uuid.MustParse(humanDeveloperPrincipalID) {
		t.Fatalf(
			"Human Job principal = %s, want %s",
			attributedPrincipal, humanDeveloperPrincipalID,
		)
	}
	getRequest, err := http.NewRequest(
		http.MethodGet,
		server.URL+"/v1/projects/"+testProjectID+"/jobs/"+submitted.JobID,
		nil,
	)
	if err != nil {
		t.Fatalf("create Human get request: %v", err)
	}
	getRequest.Header.Set("Authorization", "Bearer human-oidc-access-token")
	getResponse, err := http.DefaultClient.Do(getRequest)
	if err != nil {
		t.Fatalf("get Human Job: %v", err)
	}
	_ = getResponse.Body.Close()
	if getResponse.StatusCode != http.StatusOK {
		t.Fatalf("Human get status = %d, want 200", getResponse.StatusCode)
	}
	canceled, err := doCancelJob(
		server.URL,
		testProjectID,
		submitted.JobID,
		"human-oidc-access-token",
	)
	if err != nil {
		t.Fatalf("cancel Human Job: %v", err)
	}
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("Human cancel status = %d, want 200; body=%s", canceled.StatusCode, canceled.Body)
	}
}

func TestHumanProjectAdminCanManageWebhooksThroughProductionHTTPPath(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		INSERT INTO principals (id, organization_id, kind, display_name)
		VALUES ($1, $2, 'HUMAN', 'Human Project Admin')
	`, humanProjectAdminPrincipalID, testOrganizationID); err != nil {
		t.Fatalf("seed Human ProjectAdmin Principal: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO human_oidc_bindings (
			organization_id, principal_id, issuer, subject, display_name
		) VALUES ($2, $1, 'https://identity.example.com', 'human-project-admin', 'Human Project Admin')
	`, humanProjectAdminPrincipalID, testOrganizationID); err != nil {
		t.Fatalf("seed Human ProjectAdmin OIDC binding: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO project_role_bindings (
			organization_id, project_id, principal_id, role, assigned_by_principal_id
		) VALUES ($2, $3, $1, 'ProjectAdmin', $1)
	`, humanProjectAdminPrincipalID, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("seed Human ProjectAdmin role: %v", err)
	}
	authenticator := identity.NewAuthenticatorWithOIDC(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(t, database.DSN, "vela_human_auth_login", "vela-human-auth-password"),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "human-project-admin",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          authenticator,
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              &retention.Service{},
		Admission: admission.NewLegacyService(
			newRolePool(t, database.DSN, "vela_request_login", "vela-request-password"),
		),
		Cancellation: cancellation.NewService(
			newRolePool(t, database.DSN, "vela_cancel_login", "vela-cancel-password"),
			newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password"),
		),
		Artifacts: testArtifactAccessService(
			newRolePool(
				t, database.DSN, "vela_artifact_request_login", "vela-artifact-request-password",
			),
		),
		Webhooks: testWebhookService(t, webhookRequestPoolForDatabase(t, database)),
	})
	if err != nil {
		t.Fatalf("create Human ProjectAdmin HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	createRequest, err := http.NewRequest(
		http.MethodPost,
		server.URL+"/v1/projects/"+testProjectID+"/webhook-subscriptions",
		bytes.NewBufferString(`{
			"endpoint":"https://hooks.example.com/human-project-admin",
			"event_types":["job.succeeded"]
		}`),
	)
	if err != nil {
		t.Fatalf("create Human ProjectAdmin Webhook request: %v", err)
	}
	createRequest.Header.Set("Authorization", "Bearer human-project-admin-oidc-token")
	createRequest.Header.Set("Content-Type", "application/json")
	createResponse, err := http.DefaultClient.Do(createRequest)
	if err != nil {
		t.Fatalf("create Human ProjectAdmin Webhook: %v", err)
	}
	defer createResponse.Body.Close()
	if createResponse.StatusCode != http.StatusCreated {
		t.Fatalf("Human ProjectAdmin Webhook create status = %d, want 201", createResponse.StatusCode)
	}
	var created struct {
		SubscriptionID string `json:"subscription_id"`
	}
	if err := json.NewDecoder(createResponse.Body).Decode(&created); err != nil {
		t.Fatalf("decode Human ProjectAdmin Webhook creation: %v", err)
	}
	if _, err := uuid.Parse(created.SubscriptionID); err != nil {
		t.Fatalf("Human ProjectAdmin Webhook id = %q: %v", created.SubscriptionID, err)
	}

	listRequest, err := http.NewRequest(
		http.MethodGet,
		server.URL+"/v1/projects/"+testProjectID+"/webhook-subscriptions",
		nil,
	)
	if err != nil {
		t.Fatalf("create Human ProjectAdmin Webhook list request: %v", err)
	}
	listRequest.Header.Set("Authorization", "Bearer human-project-admin-oidc-token")
	listResponse, err := http.DefaultClient.Do(listRequest)
	if err != nil {
		t.Fatalf("list Human ProjectAdmin Webhooks: %v", err)
	}
	defer listResponse.Body.Close()
	if listResponse.StatusCode != http.StatusOK {
		t.Fatalf("Human ProjectAdmin Webhook list status = %d, want 200", listResponse.StatusCode)
	}
	var listed struct {
		Subscriptions []struct {
			Endpoint string `json:"endpoint"`
		} `json:"subscriptions"`
	}
	if err := json.NewDecoder(listResponse.Body).Decode(&listed); err != nil {
		t.Fatalf("decode Human ProjectAdmin Webhook list: %v", err)
	}
	if len(listed.Subscriptions) != 1 ||
		listed.Subscriptions[0].Endpoint != "https://hooks.example.com/human-project-admin" {
		t.Fatalf("Human ProjectAdmin Webhook list = %#v", listed.Subscriptions)
	}

	baseURL := server.URL + "/v1/projects/" + testProjectID +
		"/webhook-subscriptions/" + created.SubscriptionID
	rotateStatus, rotateBody := humanWebhookRequest(
		t, http.MethodPost, baseURL+"/rotate-secret", "human-project-admin-oidc-token",
	)
	if rotateStatus != http.StatusOK {
		t.Fatalf("Human ProjectAdmin rotate status = %d, want 200; body=%s", rotateStatus, rotateBody)
	}
	var rotated struct {
		SecretRevision int32 `json:"secret_revision"`
	}
	if err := json.Unmarshal(rotateBody, &rotated); err != nil || rotated.SecretRevision != 2 {
		t.Fatalf("Human ProjectAdmin rotate response = %s; error=%v", rotateBody, err)
	}
	deliveriesStatus, deliveriesBody := humanWebhookRequest(
		t, http.MethodGet, baseURL+"/deliveries", "human-project-admin-oidc-token",
	)
	if deliveriesStatus != http.StatusOK {
		t.Fatalf(
			"Human ProjectAdmin Delivery list status = %d, want 200; body=%s",
			deliveriesStatus,
			deliveriesBody,
		)
	}
	accepted := submitJob(t, server.URL, "human-project-admin-replay-fixture", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"Human ProjectAdmin replay attribution fixture"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf(
			"Service Admission for Human replay status = %d, want 202; body=%s",
			accepted.StatusCode,
			accepted.Body,
		)
	}
	var replayJob jobResponse
	if err := json.Unmarshal(accepted.Body, &replayJob); err != nil {
		t.Fatalf("decode Human replay Job fixture: %v", err)
	}
	eventID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO outbox_events (
			event_id, organization_id, project_id, aggregate_type, aggregate_id,
			aggregate_version, event_type, schema_version, payload, occurred_at, available_at
		) VALUES ($1, $2, $3, 'Job', $4, 5, 'job.succeeded', 1, decode('00', 'hex'),
			clock_timestamp(), clock_timestamp())
	`, eventID, testOrganizationID, testProjectID, replayJob.JobID); err != nil {
		t.Fatalf("insert Human replay terminal event fixture: %v", err)
	}
	var deliveryID uuid.UUID
	if err := database.Admin.QueryRow(`
		UPDATE webhook_deliveries
		SET state = 'DEAD_LETTER',
			dead_lettered_at = clock_timestamp(),
			last_error = 'Human replay fixture'
		WHERE event_id = $1
		RETURNING id
	`, eventID).Scan(&deliveryID); err != nil {
		t.Fatalf("prepare Human replay Delivery: %v", err)
	}
	successfulReplayStatus, successfulReplayBody := humanWebhookRequest(
		t,
		http.MethodPost,
		baseURL+"/deliveries/"+deliveryID.String()+"/replay",
		"human-project-admin-oidc-token",
	)
	if successfulReplayStatus != http.StatusOK {
		t.Fatalf(
			"Human ProjectAdmin replay status = %d, want 200; body=%s",
			successfulReplayStatus,
			successfulReplayBody,
		)
	}
	var replayed struct {
		EventID    string `json:"event_id"`
		State      string `json:"state"`
		Generation int32  `json:"generation"`
	}
	if err := json.Unmarshal(successfulReplayBody, &replayed); err != nil ||
		replayed.EventID != eventID.String() || replayed.State != "PENDING" || replayed.Generation != 2 {
		t.Fatalf("Human ProjectAdmin replay response = %s; error=%v", successfulReplayBody, err)
	}
	var requestedByPrincipal, requestedBySession uuid.UUID
	var actorKind string
	if err := database.Admin.QueryRow(`
		SELECT
			replay.requested_by_principal_id,
			replay.requested_by_credential_id,
			attribution.actor_kind::text
		FROM webhook_delivery_replays AS replay
		JOIN project_actor_session_attributions AS attribution
		  ON attribution.organization_id = replay.organization_id
		 AND attribution.project_id = replay.project_id
		 AND attribution.actor_session_id = replay.requested_by_credential_id
		WHERE replay.delivery_id = $1
	`, deliveryID).Scan(&requestedByPrincipal, &requestedBySession, &actorKind); err != nil {
		t.Fatalf("read Human replay attribution: %v", err)
	}
	if requestedByPrincipal != uuid.MustParse(humanProjectAdminPrincipalID) ||
		requestedBySession == uuid.Nil || actorKind != "HUMAN" {
		t.Fatalf(
			"Human replay attribution = principal %s session %s actor %s",
			requestedByPrincipal,
			requestedBySession,
			actorKind,
		)
	}
	replayStatus, replayBody := humanWebhookRequest(
		t,
		http.MethodPost,
		baseURL+"/deliveries/"+uuid.NewString()+"/replay",
		"human-project-admin-oidc-token",
	)
	if replayStatus != http.StatusNotFound {
		t.Fatalf("Human ProjectAdmin replay status = %d, want 404; body=%s", replayStatus, replayBody)
	}
	disableStatus, disableBody := humanWebhookRequest(
		t, http.MethodPost, baseURL+"/disable", "human-project-admin-oidc-token",
	)
	if disableStatus != http.StatusOK {
		t.Fatalf("Human ProjectAdmin disable status = %d, want 200; body=%s", disableStatus, disableBody)
	}
	var disabled struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(disableBody, &disabled); err != nil || disabled.State != "DISABLED" {
		t.Fatalf("Human ProjectAdmin disable response = %s; error=%v", disableBody, err)
	}
}

func TestHumanMigrationUsesUnifiedWebhookActorSessionAttribution(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	for _, test := range []struct {
		table  string
		column string
	}{
		{table: "webhook_subscriptions", column: "created_by_credential_id"},
		{table: "webhook_subscription_secrets", column: "created_by_credential_id"},
		{table: "webhook_delivery_replays", column: "requested_by_credential_id"},
	} {
		t.Run(test.table, func(t *testing.T) {
			var constraintName, referencedTable string
			if err := database.Admin.QueryRow(`
				SELECT constraint_row.conname, constraint_row.confrelid::regclass::text
				FROM pg_catalog.pg_constraint AS constraint_row
				WHERE constraint_row.contype = 'f'
				  AND constraint_row.conrelid = $1::regclass
				  AND EXISTS (
					  SELECT 1
					  FROM unnest(constraint_row.conkey) AS key(attnum)
					  JOIN pg_catalog.pg_attribute AS attribute
					    ON attribute.attrelid = constraint_row.conrelid
					   AND attribute.attnum = key.attnum
					  WHERE attribute.attname = $2
				  )
			`, test.table, test.column).Scan(&constraintName, &referencedTable); err != nil {
				t.Fatalf("read %s.%s attribution constraint: %v", test.table, test.column, err)
			}
			if referencedTable != "project_actor_session_attributions" {
				t.Fatalf(
					"%s.%s constraint %s references %s, want project_actor_session_attributions",
					test.table,
					test.column,
					constraintName,
					referencedTable,
				)
			}
		})
	}
}

func TestHumanProjectViewerCanReadCommittedArtifactsThroughProductionHTTPPath(t *testing.T) {
	fixture := newStartFixture(t, "human-project-viewer-artifacts", 7)
	started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("start Human ProjectViewer Artifact fixture = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("begin Human ProjectViewer finalization = %#v error=%v", plan, err)
	}
	completionService := visibleCompletionService(t, fixture.database.DSN)
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t,
		completionService,
		fixture.worker,
		fixture.credentials,
		plan,
	)
	completed, err := completionService.CompleteVisibleCompletion(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		workercontrol.VisibleCompletionCandidate{
			CompletionID:       uuid.New(),
			ExpectedJobVersion: plan.JobVersion,
			ArtifactIDs:        artifactIDs,
		},
	)
	if err != nil || completed.Decision != workercontrol.VisibleCompletionCommitted {
		t.Fatalf("complete Human ProjectViewer fixture = %#v error=%v", completed, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO principals (id, organization_id, kind, display_name)
		VALUES ($1, $2, 'HUMAN', 'Human Project Viewer')
	`, humanProjectViewerPrincipalID, testOrganizationID); err != nil {
		t.Fatalf("seed Human ProjectViewer Principal: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO human_oidc_bindings (
			organization_id, principal_id, issuer, subject, display_name
		) VALUES ($2, $1, 'https://identity.example.com', 'human-project-viewer', 'Human Project Viewer')
	`, humanProjectViewerPrincipalID, testOrganizationID); err != nil {
		t.Fatalf("seed Human ProjectViewer OIDC binding: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO project_role_bindings (
			organization_id, project_id, principal_id, role, assigned_by_principal_id
		) VALUES ($2, $3, $1, 'ProjectViewer', $1)
	`, humanProjectViewerPrincipalID, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("seed Human ProjectViewer role: %v", err)
	}
	authenticator := identity.NewAuthenticatorWithOIDC(
		newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_human_auth_login",
			"vela-human-auth-password",
		),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "human-project-viewer",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	signer := &recordingArtifactSigner{}
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          authenticator,
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              &retention.Service{},
		Admission: admission.NewLegacyService(
			newRolePool(t, fixture.database.DSN, "vela_request_login", "vela-request-password"),
		),
		Cancellation: cancellation.NewService(
			newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password"),
			newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password"),
		),
		Artifacts: artifactaccess.NewService(
			newRolePool(
				t,
				fixture.database.DSN,
				"vela_artifact_request_login",
				"vela-artifact-request-password",
			),
			signer,
		),
		Webhooks: testWebhookService(
			t,
			webhookRequestPoolForDatabase(t, fixture.database),
		),
	})
	if err != nil {
		t.Fatalf("create Human ProjectViewer HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	request, err := http.NewRequest(
		http.MethodGet,
		server.URL+"/v1/projects/"+testProjectID+"/jobs/"+
			fixture.assignment.JobID.String()+"/artifacts",
		nil,
	)
	if err != nil {
		t.Fatalf("create Human ProjectViewer Artifact request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer human-project-viewer-oidc-token")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("read ArtifactSet as Human ProjectViewer: %v", err)
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(response.Body)
	if readErr != nil {
		t.Fatalf("read Human ProjectViewer Artifact response: %v", readErr)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("Human ProjectViewer Artifact status = %d, want 200; body=%s", response.StatusCode, body)
	}
	var payload struct {
		JobID     uuid.UUID `json:"job_id"`
		Artifacts []struct {
			DownloadURL string `json:"download_url"`
		} `json:"artifacts"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode Human ProjectViewer ArtifactSet: %v", err)
	}
	if payload.JobID != fixture.assignment.JobID ||
		len(payload.Artifacts) != len(completed.Artifacts) ||
		len(signer.calls) != len(completed.Artifacts) {
		t.Fatalf("Human ProjectViewer ArtifactSet = %#v signer=%#v", payload, signer.calls)
	}
}

func TestHumanMigrationEmptyDownUpRestoresNMinusOneSurface(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 11); err != nil {
		t.Fatalf("contract empty Human OIDC/RBAC migration: %v", err)
	}
	var version int64
	if err := database.Admin.QueryRow(`
		SELECT max(version_id) FROM goose_db_version WHERE is_applied
	`).Scan(&version); err != nil {
		t.Fatalf("read migration version after Human Down: %v", err)
	}
	if version != 11 {
		t.Fatalf("migration version after Human Down = %d, want 11", version)
	}
	var humanTablesAbsent, actorKindAbsent bool
	if err := database.Admin.QueryRow(`
		SELECT
			to_regclass('public.human_oidc_bindings') IS NULL
			AND to_regclass('public.organization_role_bindings') IS NULL
			AND to_regclass('public.project_role_bindings') IS NULL
			AND to_regclass('public.human_auth_sessions') IS NULL
			AND to_regclass('public.project_principal_attributions') IS NULL
			AND to_regclass('public.project_actor_session_attributions') IS NULL,
			NOT EXISTS (
				SELECT 1
				FROM information_schema.columns
				WHERE table_schema = 'vela_private'
				  AND table_name = 'request_contexts'
				  AND column_name = 'actor_kind'
			)
	`).Scan(&humanTablesAbsent, &actorKindAbsent); err != nil {
		t.Fatalf("inspect Human migration Down surface: %v", err)
	}
	if !humanTablesAbsent || !actorKindAbsent {
		t.Fatalf(
			"Human migration Down surface = tables absent %t actor kind absent %t",
			humanTablesAbsent,
			actorKindAbsent,
		)
	}
	for _, test := range []struct {
		table      string
		column     string
		referenced string
	}{
		{table: "jobs", column: "created_by_principal_id", referenced: "service_principals"},
		{
			table: "job_cancellation_decisions", column: "requested_by_principal_id",
			referenced: "service_principals",
		},
		{table: "webhook_subscriptions", column: "created_by_credential_id", referenced: "credentials"},
		{
			table: "webhook_subscription_secrets", column: "created_by_credential_id",
			referenced: "credentials",
		},
		{
			table: "webhook_delivery_replays", column: "requested_by_credential_id",
			referenced: "credentials",
		},
	} {
		var referenced string
		if err := database.Admin.QueryRow(`
			SELECT constraint_row.confrelid::regclass::text
			FROM pg_catalog.pg_constraint AS constraint_row
			WHERE constraint_row.contype = 'f'
			  AND constraint_row.conrelid = $1::regclass
			  AND EXISTS (
				  SELECT 1
				  FROM unnest(constraint_row.conkey) AS key(attnum)
				  JOIN pg_catalog.pg_attribute AS attribute
				    ON attribute.attrelid = constraint_row.conrelid
				   AND attribute.attnum = key.attnum
				  WHERE attribute.attname = $2
			  )
		`, test.table, test.column).Scan(&referenced); err != nil {
			t.Fatalf("read restored %s.%s constraint: %v", test.table, test.column, err)
		}
		if referenced != test.referenced {
			t.Fatalf(
				"restored %s.%s references %s, want %s",
				test.table,
				test.column,
				referenced,
				test.referenced,
			)
		}
	}
	principal, err := identity.NewAuthenticator(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate Service Principal after Human migration Down: %v", err)
	}
	requestPool := newRolePool(
		t, database.DSN, "vela_request_login", "vela-request-password",
	)
	var organizationID, projectID, principalID uuid.UUID
	var transactionTime time.Time
	if err := requestPool.QueryRow(context.Background(), `
		SELECT * FROM vela_set_request_context($1, $2, 'jobs:read')
	`, principal.CredentialID, principal.RequestContextProof()).Scan(
		&organizationID,
		&projectID,
		&principalID,
		&transactionTime,
	); err != nil {
		t.Fatalf("establish N-1 Service request context after Human Down: %v", err)
	}
	if organizationID != principal.OrganizationID || projectID != principal.ProjectID ||
		principalID != principal.PrincipalID || transactionTime.IsZero() {
		t.Fatalf(
			"restored Service request context = organization %s project %s principal %s time %s",
			organizationID,
			projectID,
			principalID,
			transactionTime,
		)
	}
	if err := goose.Up(database.Admin, migrations); err != nil {
		t.Fatalf("re-expand Human OIDC/RBAC migration: %v", err)
	}
}

func TestHumanMigrationDownRefusesDurableIdentityEvidence(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		INSERT INTO principals (id, organization_id, kind, display_name)
		VALUES ($1, $2, 'HUMAN', 'Migration Human')
	`, humanDeveloperPrincipalID, testOrganizationID); err != nil {
		t.Fatalf("seed migration Human Principal: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO human_oidc_bindings (
			organization_id, principal_id, issuer, subject, display_name
		) VALUES ($2, $1, 'https://identity.example.com', 'migration-human', 'Migration Human')
	`, humanDeveloperPrincipalID, testOrganizationID); err != nil {
		t.Fatalf("seed migration Human binding: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO project_role_bindings (
			organization_id, project_id, principal_id, role, assigned_by_principal_id
		) VALUES ($2, $3, $1, 'Developer', $1)
	`, humanDeveloperPrincipalID, testOrganizationID, testProjectID); err != nil {
		t.Fatalf("seed migration Human role: %v", err)
	}
	if _, err := identity.NewAuthenticatorWithOIDC(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(t, database.DSN, "vela_human_auth_login", "vela-human-auth-password"),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer:    "https://identity.example.com",
			Subject:   "migration-human",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	).Authenticate(context.Background(), "migration-human-token"); err != nil {
		t.Fatalf("create migration Human session: %v", err)
	}
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err := goose.DownTo(database.Admin, migrations, 11)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
		postgresError.ConstraintName != "human_oidc_rbac_contract_requires_empty_evidence" {
		t.Fatalf("Human migration Down refusal = %v, want fail-closed SQLSTATE 55000", err)
	}
	var version int64
	var bindings, roles, sessions int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT max(version_id) FROM goose_db_version WHERE is_applied),
			(SELECT count(*) FROM human_oidc_bindings),
			(SELECT count(*) FROM project_role_bindings),
			(SELECT count(*) FROM human_auth_sessions)
	`).Scan(&version, &bindings, &roles, &sessions); err != nil {
		t.Fatalf("read refused Human migration state: %v", err)
	}
	if version != 12 || bindings != 1 || roles != 1 || sessions != 1 {
		t.Fatalf(
			"refused Human migration state = version %d bindings %d roles %d sessions %d",
			version,
			bindings,
			roles,
			sessions,
		)
	}
}

func humanWebhookRequest(
	t *testing.T,
	method string,
	url string,
	token string,
) (int, []byte) {
	t.Helper()
	request, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("create Human Webhook request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("perform Human Webhook request: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatalf("read Human Webhook response: %v", readErr)
	}
	return response.StatusCode, body
}

type staticOIDCTokenVerifier struct {
	identity identity.OIDCIdentity
	err      error
}

func (v staticOIDCTokenVerifier) Verify(context.Context, string) (identity.OIDCIdentity, error) {
	return v.identity, v.err
}
