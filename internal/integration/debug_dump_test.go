//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/artifactaccess"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/debugdump"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/organizationreporting"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/webhook"
)

func TestProjectAdminAuthorizesQueuedJobDebugDumpIdempotently(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	principalID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		principalID,
		"debug-dump-admin",
		nil,
		map[string][]string{testProjectID: {"ProjectAdmin"}},
	)
	authenticator := newHumanMembershipAuthenticator(
		t,
		database,
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
			Subject:   "debug-dump-admin",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	actor, err := authenticator.Authenticate(context.Background(), "debug-dump-admin-token")
	if err != nil {
		t.Fatalf("authenticate debug dump ProjectAdmin: %v", err)
	}
	projectID := uuid.MustParse(testProjectID)
	actor, ok := actor.ForProject(projectID)
	if !ok || !actor.HasScope(identity.ScopeDebugDumpsManage) {
		t.Fatal("ProjectAdmin lacks exact debug dump authority")
	}

	admissionServer := admissionServerForDatabase(t, database)
	accepted := submitJob(t, admissionServer.URL, "debug-dump-authorized-job", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"authorize bounded failure diagnostics"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit debug dump Job status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode debug dump Job: %v", err)
	}
	jobID := uuid.MustParse(job.JobID)
	var graphState string
	var physicalAttempts int
	if err := database.Admin.QueryRow(`
		SELECT attempt.graph_state::text,
			(SELECT count(*) FROM stage_attempts AS physical
			 WHERE physical.attempt_id = attempt.id)
		FROM attempts AS attempt
		WHERE attempt.job_id = $1
	`, jobID).Scan(&graphState, &physicalAttempts); err != nil {
		t.Fatalf("read admitted debug dump Stage graph: %v", err)
	}
	if graphState != "QUEUED" || physicalAttempts != 0 {
		t.Fatalf(
			"admitted debug dump Stage graph state/physical attempts = %s/%d, want QUEUED/0",
			graphState,
			physicalAttempts,
		)
	}
	service, err := debugdump.NewService(newRolePool(
		t,
		database.DSN,
		"vela_debug_dump_request_login",
		"vela-debug-dump-request-password",
	))
	if err != nil {
		t.Fatalf("create debug dump service: %v", err)
	}
	first, err := service.Authorize(
		context.Background(),
		actor,
		projectID,
		jobID,
		"debug-dump-authorization-1",
		debugdump.PurposeCustomerSupport,
	)
	if err != nil {
		t.Fatalf("authorize debug dump: %v", err)
	}
	replayed, err := service.Authorize(
		context.Background(),
		actor,
		projectID,
		jobID,
		"debug-dump-authorization-1",
		debugdump.PurposeCustomerSupport,
	)
	if err != nil {
		t.Fatalf("replay debug dump authorization: %v", err)
	}
	if first.ID == uuid.Nil || first.OrganizationID != uuid.MustParse(testOrganizationID) ||
		first.ProjectID != projectID || first.JobID != jobID ||
		first.Purpose != debugdump.PurposeCustomerSupport || first.Replayed ||
		first.AuthorizedAt.IsZero() ||
		first.ExpiresAt.Sub(first.AuthorizedAt) != 72*time.Hour {
		t.Fatalf("first debug dump authorization = %#v", first)
	}
	if replayed.ID != first.ID || !replayed.Replayed ||
		!replayed.AuthorizedAt.Equal(first.AuthorizedAt) ||
		!replayed.ExpiresAt.Equal(first.ExpiresAt) {
		t.Fatalf("replayed debug dump authorization = %#v, want replay of %#v", replayed, first)
	}

	var authorizationCount, eventCount int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM debug_dump_authorizations WHERE job_id = $1),
			(SELECT count(*) FROM debug_dump_events
			 WHERE job_id = $1 AND action = 'AUTHORIZED')
	`, jobID).Scan(&authorizationCount, &eventCount); err != nil {
		t.Fatalf("read debug dump authorization evidence: %v", err)
	}
	if authorizationCount != 1 || eventCount != 1 {
		t.Fatalf("debug dump authorization rows/events = %d/%d, want 1/1", authorizationCount, eventCount)
	}
}

func TestProjectAdminAuthorizesDebugDumpOverProductionHTTP(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedHumanRoleFixture(
		t,
		database.Admin,
		uuid.New(),
		"debug-dump-http-admin",
		nil,
		map[string][]string{testProjectID: {"ProjectAdmin"}},
	)

	admissionServer := admissionServerForDatabase(t, database)
	accepted := submitJob(t, admissionServer.URL, "debug-dump-http-job", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"authorize diagnostics through the public contract"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit debug dump HTTP Job status = %d; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode debug dump HTTP Job: %v", err)
	}

	authenticator := newHumanMembershipAuthenticator(
		t,
		database,
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		newRolePool(t, database.DSN, "vela_human_auth_login", "vela-human-auth-password"),
		testCredentialPepper,
		staticOIDCTokenVerifier{identity: identity.OIDCIdentity{
			Issuer: "https://identity.example.com", Subject: "debug-dump-http-admin",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	debugService, err := debugdump.NewService(newRolePool(
		t,
		database.DSN,
		"vela_debug_dump_request_login",
		"vela-debug-dump-request-password",
	))
	if err != nil {
		t.Fatalf("create HTTP debug dump service: %v", err)
	}
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          authenticator,
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              &retention.Service{},
		DebugDumps:             debugService,
		Admission:              &admission.Service{},
		Cancellation:           &cancellation.Service{},
		Artifacts:              &artifactaccess.Service{},
		Webhooks:               &webhook.Service{},
	})
	if err != nil {
		t.Fatalf("create debug dump HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	endpoint := server.URL + "/v1/projects/" + testProjectID + "/jobs/" +
		job.JobID + "/debug-dump-authorizations"
	requestAuthorization := func() (int, []byte) {
		t.Helper()
		request, err := http.NewRequest(
			http.MethodPost,
			endpoint,
			bytes.NewBufferString(`{"purpose":"CUSTOMER_SUPPORT"}`),
		)
		if err != nil {
			t.Fatalf("create debug dump authorization HTTP request: %v", err)
		}
		request.Header.Set("Authorization", "Bearer debug-dump-http-admin-token")
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "debug-dump-http-authorization")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatalf("execute debug dump authorization HTTP request: %v", err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatalf("read debug dump authorization HTTP response: %v", err)
		}
		return response.StatusCode, body
	}

	firstStatus, firstBody := requestAuthorization()
	replayStatus, replayBody := requestAuthorization()
	if firstStatus != http.StatusCreated || replayStatus != http.StatusOK ||
		!bytes.Equal(firstBody, replayBody) {
		t.Fatalf(
			"debug dump HTTP create/replay = %d %s / %d %s",
			firstStatus,
			firstBody,
			replayStatus,
			replayBody,
		)
	}

	var created struct {
		AuthorizationID string     `json:"authorization_id"`
		RevokedAt       *time.Time `json:"revoked_at"`
	}
	if err := json.Unmarshal(firstBody, &created); err != nil {
		t.Fatalf("decode created debug dump authorization: %v", err)
	}
	authorizationEndpoint := endpoint + "/" + created.AuthorizationID
	requestLifecycle := func(method, target string) (int, []byte) {
		t.Helper()
		request, err := http.NewRequest(method, target, nil)
		if err != nil {
			t.Fatalf("create debug dump lifecycle HTTP request: %v", err)
		}
		request.Header.Set("Authorization", "Bearer debug-dump-http-admin-token")
		if method == http.MethodPost {
			request.Header.Set("Idempotency-Key", "debug-dump-http-revocation")
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatalf("execute debug dump lifecycle HTTP request: %v", err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatalf("read debug dump lifecycle HTTP response: %v", err)
		}
		return response.StatusCode, body
	}

	getStatus, getBody := requestLifecycle(http.MethodGet, authorizationEndpoint)
	if getStatus != http.StatusOK || !bytes.Equal(firstBody, getBody) {
		t.Fatalf("debug dump authorization GET = %d %s, want 200 %s", getStatus, getBody, firstBody)
	}
	revokeStatus, revokeBody := requestLifecycle(http.MethodPost, authorizationEndpoint+"/revoke")
	revokeReplayStatus, revokeReplayBody := requestLifecycle(http.MethodPost, authorizationEndpoint+"/revoke")
	var revoked struct {
		RevokedAt *time.Time `json:"revoked_at"`
	}
	if err := json.Unmarshal(revokeBody, &revoked); err != nil {
		t.Fatalf("decode revoked debug dump authorization: %v", err)
	}
	if revokeStatus != http.StatusOK || revokeReplayStatus != http.StatusOK ||
		revoked.RevokedAt == nil || !bytes.Equal(revokeBody, revokeReplayBody) {
		t.Fatalf(
			"debug dump revoke/replay = %d %s / %d %s",
			revokeStatus,
			revokeBody,
			revokeReplayStatus,
			revokeReplayBody,
		)
	}
	getRevokedStatus, getRevokedBody := requestLifecycle(http.MethodGet, authorizationEndpoint)
	if getRevokedStatus != http.StatusOK || !bytes.Equal(revokeBody, getRevokedBody) {
		t.Fatalf(
			"revoked debug dump authorization GET = %d %s, want 200 %s",
			getRevokedStatus,
			getRevokedBody,
			revokeBody,
		)
	}
	var revokeEvents int
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM debug_dump_events
		WHERE authorization_id = $1 AND action = 'REVOKED'
	`, uuid.MustParse(created.AuthorizationID)).Scan(&revokeEvents); err != nil {
		t.Fatalf("read debug dump revocation evidence: %v", err)
	}
	if revokeEvents != 1 {
		t.Fatalf("debug dump revocation events = %d, want 1", revokeEvents)
	}
}
