//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/artifactaccess"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/cancellation"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/organizationreporting"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/webhook"
	"github.com/vivym/vela/internal/workercontrol"
)

func TestProjectAdminManagesRetentionPolicyOverProductionHTTP(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedOtherOrganization(t, database.Admin)
	seedHumanRoleFixture(
		t,
		database.Admin,
		uuid.New(),
		"retention-policy-admin",
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
			Subject:   "retention-policy-admin",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	retentionPool := newRolePool(
		t,
		database.DSN,
		"vela_retention_request_login",
		"vela-retention-request-password",
	)
	retentionService, err := retention.NewService(retentionPool)
	if err != nil {
		t.Fatalf("create Retention Policy service: %v", err)
	}
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          authenticator,
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              retentionService,
		Admission:              admission.NewLegacyService(requestPool),
		Cancellation:           &cancellation.Service{},
		Artifacts:              &artifactaccess.Service{},
		Webhooks:               &webhook.Service{},
	})
	if err != nil {
		t.Fatalf("create Retention Policy HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	jobBody := []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"Retention Policy changes affect only later Admissions"
	}`)
	submitAndDecode := func(idempotencyKey string) jobResponse {
		t.Helper()
		result := submitJob(t, server.URL, idempotencyKey, jobBody)
		if result.StatusCode != http.StatusAccepted {
			t.Fatalf("submit %s status = %d, want 202; body=%s", idempotencyKey, result.StatusCode, result.Body)
		}
		var accepted jobResponse
		if err := json.Unmarshal(result.Body, &accepted); err != nil {
			t.Fatalf("decode accepted Job %s: %v", idempotencyKey, err)
		}
		return accepted
	}
	beforeChange := submitAndDecode("retention-policy-before-change")

	type policyResponse struct {
		ProjectID             uuid.UUID `json:"project_id"`
		PolicyRevisionID      uuid.UUID `json:"policy_revision_id"`
		StableID              string    `json:"stable_id"`
		ArtifactRetentionDays int       `json:"artifact_retention_days"`
		SelectedAt            time.Time `json:"selected_at"`
	}
	doRequest := func(method string, body []byte) policyResponse {
		t.Helper()
		request, err := http.NewRequest(
			method,
			server.URL+"/v1/projects/"+testProjectID+"/retention-policy",
			bytes.NewReader(body),
		)
		if err != nil {
			t.Fatalf("create Retention Policy request: %v", err)
		}
		request.Header.Set("Authorization", "Bearer retention-policy-admin-token")
		if body != nil {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatalf("execute Retention Policy request: %v", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			failureBody, _ := io.ReadAll(response.Body)
			t.Fatalf(
				"Retention Policy %s status = %d, want 200; body=%s",
				method,
				response.StatusCode,
				failureBody,
			)
		}
		var policy policyResponse
		if err := json.NewDecoder(response.Body).Decode(&policy); err != nil {
			t.Fatalf("decode Retention Policy response: %v", err)
		}
		return policy
	}

	initial := doRequest(http.MethodGet, nil)
	if initial.ProjectID.String() != testProjectID ||
		initial.PolicyRevisionID == uuid.Nil ||
		initial.StableID != "artifact-30d-v1" ||
		initial.ArtifactRetentionDays != 30 || initial.SelectedAt.IsZero() {
		t.Fatalf("initial Retention Policy = %#v", initial)
	}
	selected := doRequest(http.MethodPut, []byte(`{"artifact_retention_days":7}`))
	replayed := doRequest(http.MethodPut, []byte(`{"artifact_retention_days":7}`))
	current := doRequest(http.MethodGet, nil)
	afterChange := submitAndDecode("retention-policy-after-change")
	if selected.ProjectID != initial.ProjectID ||
		selected.PolicyRevisionID == initial.PolicyRevisionID ||
		selected.StableID != "artifact-7d-v1" ||
		selected.ArtifactRetentionDays != 7 ||
		replayed != selected || current != selected {
		t.Fatalf(
			"selected/replayed/current Retention Policy = %#v / %#v / %#v",
			selected,
			replayed,
			current,
		)
	}

	var eventCount int
	if err := database.Admin.QueryRow(`
		SELECT count(*)
		FROM project_retention_policy_events
		WHERE project_id = $1
	`, testProjectID).Scan(&eventCount); err != nil {
		t.Fatalf("count Project Retention Policy events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("Project Retention Policy event count = %d, want 1", eventCount)
	}
	var beforeDays, afterDays int
	if err := database.Admin.QueryRow(`
		SELECT before_job.retention_artifact_days, after_job.retention_artifact_days
		FROM jobs AS before_job
		JOIN jobs AS after_job ON after_job.id = $2
		WHERE before_job.id = $1
	`, beforeChange.JobID, afterChange.JobID).Scan(&beforeDays, &afterDays); err != nil {
		t.Fatalf("read Job Retention Policy snapshots: %v", err)
	}
	if beforeDays != 30 || afterDays != 7 {
		t.Fatalf("Job Retention Policy snapshots = %d then %d, want 30 then 7", beforeDays, afterDays)
	}
	assertProjectStatus := func(
		method, projectID, authorization string,
		body []byte,
		wantStatus int,
		wantCode string,
	) {
		t.Helper()
		request, err := http.NewRequest(
			method,
			server.URL+"/v1/projects/"+projectID+"/retention-policy",
			bytes.NewReader(body),
		)
		if err != nil {
			t.Fatalf("create Retention Policy failure request: %v", err)
		}
		if authorization != "" {
			request.Header.Set("Authorization", "Bearer "+authorization)
		}
		if body != nil {
			request.Header.Set("Content-Type", "application/json")
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatalf("execute Retention Policy failure request: %v", err)
		}
		defer response.Body.Close()
		var failure struct {
			Code string `json:"code"`
		}
		if err := json.NewDecoder(response.Body).Decode(&failure); err != nil {
			t.Fatalf("decode Retention Policy failure: %v", err)
		}
		if response.StatusCode != wantStatus || failure.Code != wantCode {
			t.Fatalf(
				"Retention Policy failure = status %d code %q, want %d %q",
				response.StatusCode,
				failure.Code,
				wantStatus,
				wantCode,
			)
		}
	}
	assertStatus := func(
		method, authorization string,
		body []byte,
		wantStatus int,
		wantCode string,
	) {
		t.Helper()
		assertProjectStatus(method, testProjectID, authorization, body, wantStatus, wantCode)
	}
	assertStatus(http.MethodGet, "", nil, http.StatusUnauthorized, "unauthorized")
	assertStatus(http.MethodGet, testBearerCredential(), nil, http.StatusForbidden, "forbidden")
	assertProjectStatus(
		http.MethodGet,
		testOtherProjectID,
		"retention-policy-admin-token",
		nil,
		http.StatusNotFound,
		"not_found",
	)
	assertProjectStatus(
		http.MethodPut,
		testOtherProjectID,
		"retention-policy-admin-token",
		[]byte(`{"artifact_retention_days":7}`),
		http.StatusNotFound,
		"not_found",
	)
	assertStatus(
		http.MethodPut,
		"retention-policy-admin-token",
		[]byte(`{"artifact_retention_days":14}`),
		http.StatusBadRequest,
		"invalid_request",
	)
	if _, err := database.Admin.Exec(`
		DELETE FROM project_role_bindings
		WHERE project_id = $1
		  AND principal_id = (
			SELECT principal_id
			FROM human_oidc_bindings
			WHERE subject = 'retention-policy-admin'
		  )
		  AND role = 'ProjectAdmin'
	`, testProjectID); err != nil {
		t.Fatalf("remove ProjectAdmin role: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO project_role_bindings (
			organization_id, project_id, principal_id, role, assigned_by_principal_id
		)
		SELECT organization_id, $1, principal_id, 'Developer', principal_id
		FROM human_oidc_bindings
		WHERE subject = 'retention-policy-admin'
	`, testProjectID); err != nil {
		t.Fatalf("assign Developer role: %v", err)
	}
	assertStatus(
		http.MethodGet,
		"retention-policy-admin-token",
		nil,
		http.StatusForbidden,
		"forbidden",
	)
}

func TestServicePrincipalAcceptsQueuedJobContentDeletionOverProductionHTTP(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'content_deletion:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant Content Deletion scope: %v", err)
	}
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	retentionService, err := retention.NewService(newRolePool(
		t,
		database.DSN,
		"vela_retention_request_login",
		"vela-retention-request-password",
	))
	if err != nil {
		t.Fatalf("create Content Deletion service: %v", err)
	}
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          identity.NewAuthenticator(authPool, testCredentialPepper),
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              retentionService,
		Admission:              admission.NewLegacyService(requestPool),
		Cancellation:           &cancellation.Service{},
		Artifacts:              &artifactaccess.Service{},
		Webhooks:               &webhook.Service{},
	})
	if err != nil {
		t.Fatalf("create Content Deletion HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	jobBody := []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"delete this queued Customer Content"
	}`)
	acceptedResult := submitJob(t, server.URL, "content-deletion-queued", jobBody)
	if acceptedResult.StatusCode != http.StatusAccepted {
		t.Fatalf("submit deletion target status = %d; body=%s", acceptedResult.StatusCode, acceptedResult.Body)
	}
	var accepted jobResponse
	if err := json.Unmarshal(acceptedResult.Body, &accepted); err != nil {
		t.Fatalf("decode deletion target Job: %v", err)
	}

	type deletionResponse struct {
		RequestID   uuid.UUID  `json:"request_id"`
		ProjectID   uuid.UUID  `json:"project_id"`
		JobID       uuid.UUID  `json:"job_id"`
		State       string     `json:"state"`
		RequestedAt time.Time  `json:"requested_at"`
		DeadlineAt  time.Time  `json:"deadline_at"`
		CompletedAt *time.Time `json:"completed_at"`
		Overdue     bool       `json:"overdue"`
	}
	requestDeletion := func(jobID, idempotencyKey string) (int, deletionResponse, []byte) {
		t.Helper()
		request, err := http.NewRequest(
			http.MethodPost,
			server.URL+"/v1/projects/"+testProjectID+"/jobs/"+jobID+
				"/content-deletion-requests",
			nil,
		)
		if err != nil {
			t.Fatalf("create Content Deletion request: %v", err)
		}
		request.Header.Set("Authorization", "Bearer "+testBearerCredential())
		request.Header.Set("Idempotency-Key", idempotencyKey)
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatalf("request Content Deletion: %v", err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatalf("read Content Deletion response: %v", err)
		}
		var result deletionResponse
		if response.StatusCode == http.StatusAccepted {
			if err := json.Unmarshal(body, &result); err != nil {
				t.Fatalf("decode Content Deletion response: %v", err)
			}
		}
		return response.StatusCode, result, body
	}
	artifactRequest, err := http.NewRequest(
		http.MethodGet,
		server.URL+"/v1/projects/"+testProjectID+"/jobs/"+accepted.JobID+"/artifacts",
		nil,
	)
	if err != nil {
		t.Fatalf("create deletion-only Artifact read request: %v", err)
	}
	artifactRequest.Header.Set("Authorization", "Bearer "+testBearerCredential())
	artifactResponse, err := server.Client().Do(artifactRequest)
	if err != nil {
		t.Fatalf("request Artifact read with deletion-only Credential: %v", err)
	}
	artifactResponse.Body.Close()
	if artifactResponse.StatusCode != http.StatusForbidden {
		t.Fatalf(
			"deletion-only Artifact read status = %d, want 403",
			artifactResponse.StatusCode,
		)
	}

	status, deletion, body := requestDeletion(accepted.JobID, "queued-content-deletion-key")
	if status != http.StatusAccepted {
		t.Fatalf("Content Deletion status = %d, want 202; body=%s", status, body)
	}
	if deletion.RequestID == uuid.Nil || deletion.ProjectID.String() != testProjectID ||
		deletion.JobID.String() != accepted.JobID || deletion.State != "PENDING" ||
		deletion.RequestedAt.IsZero() ||
		!deletion.DeadlineAt.Equal(deletion.RequestedAt.Add(24*time.Hour)) ||
		deletion.CompletedAt != nil || deletion.Overdue {
		t.Fatalf("accepted Content Deletion = %#v", deletion)
	}
	replayStatus, replayed, replayBody := requestDeletion(
		accepted.JobID,
		"queued-content-deletion-key",
	)
	if replayStatus != http.StatusAccepted || replayed != deletion {
		t.Fatalf("replayed Content Deletion = %d %#v body=%s", replayStatus, replayed, replayBody)
	}

	var (
		jobState, requestContent, reservationState  string
		deletedAt                                   *time.Time
		chargeCount, cancellationCount, targetCount int
	)
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, job.request_content::text, job.request_content_deleted_at,
			reservation.state::text,
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
			(SELECT count(*) FROM content_deletion_targets WHERE request_id = $2)
		FROM jobs AS job
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		WHERE job.id = $1
	`, accepted.JobID, deletion.RequestID).Scan(
		&jobState,
		&requestContent,
		&deletedAt,
		&reservationState,
		&chargeCount,
		&cancellationCount,
		&targetCount,
	); err != nil {
		t.Fatalf("read accepted Content Deletion effects: %v", err)
	}
	if jobState != "CANCELED" || requestContent != `{"deleted": true}` || deletedAt == nil ||
		reservationState != "RELEASED" || chargeCount != 0 || cancellationCount != 1 ||
		targetCount != 1 {
		t.Fatalf(
			"accepted Content Deletion effects = state %s content %s deleted %v reservation %s charges %d cancellations %d targets %d",
			jobState,
			requestContent,
			deletedAt,
			reservationState,
			chargeCount,
			cancellationCount,
			targetCount,
		)
	}

	getRequest, err := http.NewRequest(
		http.MethodGet,
		server.URL+"/v1/projects/"+testProjectID+"/content-deletion-requests/"+
			deletion.RequestID.String(),
		nil,
	)
	if err != nil {
		t.Fatalf("create Content Deletion status request: %v", err)
	}
	getRequest.Header.Set("Authorization", "Bearer "+testBearerCredential())
	getResponse, err := server.Client().Do(getRequest)
	if err != nil {
		t.Fatalf("get Content Deletion status: %v", err)
	}
	defer getResponse.Body.Close()
	getBody, err := io.ReadAll(getResponse.Body)
	if err != nil {
		t.Fatalf("read Content Deletion status: %v", err)
	}
	if getResponse.StatusCode != http.StatusOK || !bytes.Contains(getBody, []byte(`"target_count":1`)) ||
		bytes.Contains(getBody, []byte("object_key")) || bytes.Contains(getBody, []byte("object_version")) ||
		bytes.Contains(getBody, []byte("delete this queued")) {
		t.Fatalf("Content Deletion status = %d %s", getResponse.StatusCode, getBody)
	}

	secondAcceptedResult := submitJob(
		t,
		server.URL,
		"content-deletion-conflict-target",
		jobBody,
	)
	if secondAcceptedResult.StatusCode != http.StatusAccepted {
		t.Fatalf(
			"submit conflicting deletion target status = %d; body=%s",
			secondAcceptedResult.StatusCode,
			secondAcceptedResult.Body,
		)
	}
	var secondAccepted jobResponse
	if err := json.Unmarshal(secondAcceptedResult.Body, &secondAccepted); err != nil {
		t.Fatalf("decode conflicting deletion target Job: %v", err)
	}
	conflictStatus, _, conflictBody := requestDeletion(
		secondAccepted.JobID,
		"queued-content-deletion-key",
	)
	if conflictStatus != http.StatusConflict ||
		!bytes.Contains(conflictBody, []byte(`"code":"conflict"`)) {
		t.Fatalf(
			"conflicting Content Deletion = %d %s, want 409 conflict",
			conflictStatus,
			conflictBody,
		)
	}

	crossProjectRequest, err := http.NewRequest(
		http.MethodGet,
		server.URL+"/v1/projects/"+uuid.NewString()+
			"/content-deletion-requests/"+deletion.RequestID.String(),
		nil,
	)
	if err != nil {
		t.Fatalf("create cross-Project Content Deletion request: %v", err)
	}
	crossProjectRequest.Header.Set("Authorization", "Bearer "+testBearerCredential())
	crossProjectResponse, err := server.Client().Do(crossProjectRequest)
	if err != nil {
		t.Fatalf("request cross-Project Content Deletion status: %v", err)
	}
	defer crossProjectResponse.Body.Close()
	if crossProjectResponse.StatusCode != http.StatusNotFound {
		t.Fatalf(
			"cross-Project Content Deletion status = %d, want 404",
			crossProjectResponse.StatusCode,
		)
	}
}

func TestContentDeletionReadDoesNotCrossOrganizationFunctionBoundary(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedOtherOrganization(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'content_deletion:manage')
		WHERE id = ANY($1::uuid[])
	`, []uuid.UUID{uuid.MustParse(testCredentialID), uuid.MustParse(testOtherCredentialID)}); err != nil {
		t.Fatalf("grant cross-Organization Content Deletion scopes: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	otherResult, err := doSubmitJob(
		server.URL,
		testOtherProjectID,
		bearerCredential(testOtherCredentialID, testOtherSecret),
		"content-deletion-other-organization-job",
		[]byte(`{
			"model":"minimax-h3",
			"generation_preset":"balanced",
			"service_class":"standard",
			"output_spec":"video-1080p-5s-24fps",
			"generation_count":1,
			"prompt":"Organization B content must remain invisible"
		}`),
	)
	if err != nil || otherResult.StatusCode != http.StatusAccepted {
		t.Fatalf(
			"submit Organization B deletion target = status %d body=%s error=%v",
			otherResult.StatusCode,
			otherResult.Body,
			err,
		)
	}
	var otherJob jobResponse
	if err := json.Unmarshal(otherResult.Body, &otherJob); err != nil {
		t.Fatalf("decode Organization B deletion target: %v", err)
	}
	authenticator := identity.NewAuthenticator(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	)
	organizationAPrincipal, err := authenticator.Authenticate(
		context.Background(), testBearerCredential(),
	)
	if err != nil {
		t.Fatalf("authenticate Organization A Principal: %v", err)
	}
	organizationBPrincipal, err := authenticator.Authenticate(
		context.Background(), bearerCredential(testOtherCredentialID, testOtherSecret),
	)
	if err != nil {
		t.Fatalf("authenticate Organization B Principal: %v", err)
	}
	requestService, err := retention.NewService(newRolePool(
		t,
		database.DSN,
		"vela_retention_request_login",
		"vela-retention-request-password",
	))
	if err != nil {
		t.Fatalf("create cross-Organization Content Deletion service: %v", err)
	}
	organizationBDeletion, err := requestService.AcceptContentDeletion(
		context.Background(),
		organizationBPrincipal,
		uuid.MustParse(testOtherProjectID),
		uuid.MustParse(otherJob.JobID),
		"content-deletion-other-organization-request",
	)
	if err != nil {
		t.Fatalf("accept Organization B Content Deletion: %v", err)
	}

	_, err = requestService.GetContentDeletion(
		context.Background(),
		organizationAPrincipal,
		uuid.MustParse(testProjectID),
		organizationBDeletion.RequestID,
	)
	var failure *retention.Failure
	if !errors.As(err, &failure) || failure.Code != retention.FailureNotFound {
		t.Fatalf("Organization A read of Organization B request error = %v, want not_found", err)
	}
	visibleToOwner, err := requestService.GetContentDeletion(
		context.Background(),
		organizationBPrincipal,
		uuid.MustParse(testOtherProjectID),
		organizationBDeletion.RequestID,
	)
	if err != nil || visibleToOwner.RequestID != organizationBDeletion.RequestID ||
		visibleToOwner.ProjectID.String() != testOtherProjectID || visibleToOwner.TargetCount != 1 {
		t.Fatalf("Organization B Content Deletion after negative read = %#v error=%v", visibleToOwner, err)
	}
}

func TestRunningJobContentDeletionPreservesBillableCancellationOverProductionHTTP(
	t *testing.T,
) {
	fixture := newStartFixtureWithRetention(t, "content-deletion-running", 30, 29)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'content_deletion:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant Content Deletion scope: %v", err)
	}
	authPool := newRolePool(
		t, fixture.database.DSN, "vela_auth_login", "vela-auth-password",
	)
	retentionService, err := retention.NewService(newRolePool(
		t,
		fixture.database.DSN,
		"vela_retention_request_login",
		"vela-retention-request-password",
	))
	if err != nil {
		t.Fatalf("create Content Deletion service: %v", err)
	}
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          identity.NewAuthenticator(authPool, testCredentialPepper),
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              retentionService,
		Admission:              &admission.Service{},
		Cancellation:           &cancellation.Service{},
		Artifacts:              &artifactaccess.Service{},
		Webhooks:               &webhook.Service{},
	})
	if err != nil {
		t.Fatalf("create Content Deletion HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	request, err := http.NewRequest(
		http.MethodPost,
		server.URL+"/v1/projects/"+testProjectID+"/jobs/"+
			fixture.candidate.JobID.String()+"/content-deletion-requests",
		nil,
	)
	if err != nil {
		t.Fatalf("create running Content Deletion request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+testBearerCredential())
	request.Header.Set("Idempotency-Key", "running-content-deletion-key")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("request running Content Deletion: %v", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read running Content Deletion response: %v", err)
	}
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf(
			"running Content Deletion status = %d, want 202; body=%s",
			response.StatusCode,
			responseBody,
		)
	}

	var (
		jobState, requestContent, reservationState, chargeReason string
		requestContentDeletedAt                                  *time.Time
		quotedAmount, chargeAmount                               int64
		chargeCount, cancellationCount, targetCount              int
		billable                                                 bool
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT job.state::text, job.request_content::text,
			job.request_content_deleted_at, reservation.state::text,
			job.pricing_quoted_amount_minor,
			coalesce(charge.amount_minor, 0), coalesce(charge.reason::text, ''),
			coalesce(decision.billable, false),
			count(charge.id) OVER (), count(decision.id) OVER (),
			(SELECT count(*)
			 FROM content_deletion_targets AS target
			 JOIN content_deletion_requests AS deletion ON deletion.id = target.request_id
			 WHERE deletion.job_id = job.id)
		FROM jobs AS job
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		LEFT JOIN charges AS charge ON charge.job_id = job.id
		LEFT JOIN job_cancellation_decisions AS decision ON decision.job_id = job.id
		WHERE job.id = $1
	`, fixture.candidate.JobID).Scan(
		&jobState,
		&requestContent,
		&requestContentDeletedAt,
		&reservationState,
		&quotedAmount,
		&chargeAmount,
		&chargeReason,
		&billable,
		&chargeCount,
		&cancellationCount,
		&targetCount,
	); err != nil {
		t.Fatalf("read running Content Deletion effects: %v", err)
	}
	if jobState != "CANCELING" || requestContent != `{"deleted": true}` ||
		requestContentDeletedAt == nil || reservationState != "CONSUMED" ||
		chargeCount != 1 || chargeAmount != quotedAmount ||
		chargeReason != "CUSTOMER_CANCELLATION" || cancellationCount != 1 ||
		!billable || targetCount != 1 {
		t.Fatalf(
			"running Content Deletion effects = state %s content %s deleted %v reservation %s charge %d/%d %s count %d cancellation %d billable %v targets %d",
			jobState,
			requestContent,
			requestContentDeletedAt,
			reservationState,
			chargeAmount,
			quotedAmount,
			chargeReason,
			chargeCount,
			cancellationCount,
			billable,
			targetCount,
		)
	}
}

func TestVisibleCompletionAndContentDeletionSerializeOneChargeAndPublication(t *testing.T) {
	fixture := newStartFixtureWithRetention(t, "visible-completion-content-deletion-race", 30, 43)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	completionService := visibleCompletionService(t, fixture.database.DSN)
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t,
		completionService,
		fixture.worker,
		fixture.credentials,
		plan,
	)
	deletionService, principal := contentDeletionAuthority(t, fixture.database)

	type completionOutcome struct {
		result workercontrol.VisibleCompletionResult
		err    error
	}
	type deletionOutcome struct {
		result retention.DeletionRequest
		err    error
	}
	completionResults := make(chan completionOutcome, 1)
	deletionResults := make(chan deletionOutcome, 1)
	start := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		<-start
		result, callErr := completionService.CompleteVisibleCompletion(
			ctx,
			fixture.worker,
			fixture.credentials,
			workercontrol.VisibleCompletionCandidate{
				CompletionID:       uuid.New(),
				ExpectedJobVersion: plan.JobVersion,
				ArtifactIDs:        artifactIDs,
			},
		)
		completionResults <- completionOutcome{result: result, err: callErr}
	}()
	go func() {
		<-start
		result, callErr := deletionService.AcceptContentDeletion(
			ctx,
			principal,
			uuid.MustParse(testProjectID),
			fixture.candidate.JobID,
			"visible-completion-content-deletion-race",
		)
		deletionResults <- deletionOutcome{result: result, err: callErr}
	}()
	close(start)

	var completion completionOutcome
	select {
	case completion = <-completionResults:
	case <-ctx.Done():
		t.Fatalf("Visible Completion/Content Deletion race deadlocked: %v", ctx.Err())
	}
	var deletion deletionOutcome
	select {
	case deletion = <-deletionResults:
	case <-ctx.Done():
		t.Fatalf("Visible Completion/Content Deletion race deadlocked: %v", ctx.Err())
	}
	if completion.err != nil || deletion.err != nil {
		t.Fatalf(
			"Visible Completion/Content Deletion race errors = %v / %v",
			completion.err,
			deletion.err,
		)
	}
	completionWon := completion.result.Decision == workercontrol.VisibleCompletionCommitted
	deletionWon := completion.result.Decision == workercontrol.VisibleCompletionCancellationWon
	if completionWon == deletionWon {
		t.Fatalf(
			"Visible Completion/Content Deletion decision = %s",
			completion.result.Decision,
		)
	}
	if deletion.result.RequestID == uuid.Nil || deletion.result.State != "PENDING" {
		t.Fatalf("accepted Content Deletion = %#v", deletion.result)
	}

	var (
		jobState, requestContent, reservationState, chargeReason string
		requestContentDeletedAt                                  *time.Time
		chargeCount, cancellationCount                           int
		artifactSetCount, visibleCompletionCount                 int
		artifactCount, committedArtifactCount                    int
		accessGrantCount, revokedAccessGrantCount, targetCount   int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state::text,
			job.request_content::text,
			job.request_content_deleted_at,
			reservation.state::text,
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			coalesce((SELECT reason::text FROM charges WHERE job_id = job.id), ''),
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
			(SELECT count(*) FROM artifact_sets WHERE job_id = job.id),
			(SELECT count(*) FROM visible_completions WHERE job_id = job.id),
			(SELECT count(*) FROM artifacts WHERE job_id = job.id),
			(SELECT count(*) FROM artifacts WHERE job_id = job.id AND state = 'COMMITTED'),
			(SELECT count(*) FROM artifact_access_grants WHERE job_id = job.id),
			(SELECT count(*) FROM artifact_access_grants
			 WHERE job_id = job.id AND revoked_at IS NOT NULL),
			(SELECT count(*) FROM content_deletion_targets WHERE request_id = $2)
		FROM jobs AS job
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		WHERE job.id = $1
	`, fixture.candidate.JobID, deletion.result.RequestID).Scan(
		&jobState,
		&requestContent,
		&requestContentDeletedAt,
		&reservationState,
		&chargeCount,
		&chargeReason,
		&cancellationCount,
		&artifactSetCount,
		&visibleCompletionCount,
		&artifactCount,
		&committedArtifactCount,
		&accessGrantCount,
		&revokedAccessGrantCount,
		&targetCount,
	); err != nil {
		t.Fatalf("read Visible Completion/Content Deletion winner: %v", err)
	}
	if requestContent != `{"deleted": true}` || requestContentDeletedAt == nil ||
		reservationState != "CONSUMED" || chargeCount != 1 ||
		artifactCount != len(artifactIDs) || targetCount != len(artifactIDs)+1 {
		t.Fatalf(
			"race invariant = state %s content %s deleted %v reservation %s charge %d artifacts %d targets %d",
			jobState,
			requestContent,
			requestContentDeletedAt,
			reservationState,
			chargeCount,
			artifactCount,
			targetCount,
		)
	}
	if completionWon && (jobState != "SUCCEEDED" || chargeReason != "VISIBLE_COMPLETION" ||
		cancellationCount != 0 || artifactSetCount != 1 || visibleCompletionCount != 1 ||
		committedArtifactCount != len(artifactIDs) || accessGrantCount != 1 ||
		revokedAccessGrantCount != 1) {
		t.Fatalf(
			"completion winner = state %s reason %s cancellations %d sets/visible %d/%d committed %d access/revoked %d/%d",
			jobState,
			chargeReason,
			cancellationCount,
			artifactSetCount,
			visibleCompletionCount,
			committedArtifactCount,
			accessGrantCount,
			revokedAccessGrantCount,
		)
	}
	if deletionWon && (jobState != "CANCELING" || chargeReason != "CUSTOMER_CANCELLATION" ||
		cancellationCount != 1 || artifactSetCount != 0 || visibleCompletionCount != 0 ||
		committedArtifactCount != 0 || accessGrantCount != 0 || revokedAccessGrantCount != 0) {
		t.Fatalf(
			"deletion winner = state %s reason %s cancellations %d sets/visible %d/%d committed %d access/revoked %d/%d",
			jobState,
			chargeReason,
			cancellationCount,
			artifactSetCount,
			visibleCompletionCount,
			committedArtifactCount,
			accessGrantCount,
			revokedAccessGrantCount,
		)
	}
	assertRequestContentTombstoneImmutable(t, fixture.database.Admin, fixture.candidate.JobID)
}

func TestCustomerCancellationAndContentDeletionShareOneCancellationAuthority(t *testing.T) {
	fixture := newStartFixtureWithRetention(t, "customer-cancellation-content-deletion-race", 30, 47)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	deletionService, principal := contentDeletionAuthority(t, fixture.database)
	cancelService := cancellation.NewService(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_cancel_login",
			"vela-cancel-password",
		),
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_internal_login",
			"vela-internal-password",
		),
	)

	type cancellationOutcome struct {
		result cancellation.Result
		err    error
	}
	type deletionOutcome struct {
		result retention.DeletionRequest
		err    error
	}
	cancellationResults := make(chan cancellationOutcome, 1)
	deletionResults := make(chan deletionOutcome, 1)
	start := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		<-start
		result, callErr := cancelService.Cancel(
			ctx,
			principal,
			principal.ProjectID,
			fixture.candidate.JobID,
		)
		cancellationResults <- cancellationOutcome{result: result, err: callErr}
	}()
	go func() {
		<-start
		result, callErr := deletionService.AcceptContentDeletion(
			ctx,
			principal,
			principal.ProjectID,
			fixture.candidate.JobID,
			"customer-cancellation-content-deletion-race",
		)
		deletionResults <- deletionOutcome{result: result, err: callErr}
	}()
	close(start)

	var canceled cancellationOutcome
	select {
	case canceled = <-cancellationResults:
	case <-ctx.Done():
		t.Fatalf("Customer Cancellation/Content Deletion race deadlocked: %v", ctx.Err())
	}
	var deletion deletionOutcome
	select {
	case deletion = <-deletionResults:
	case <-ctx.Done():
		t.Fatalf("Customer Cancellation/Content Deletion race deadlocked: %v", ctx.Err())
	}
	if canceled.err != nil || deletion.err != nil {
		t.Fatalf(
			"Customer Cancellation/Content Deletion race errors = %v / %v",
			canceled.err,
			deletion.err,
		)
	}
	if canceled.result.Decision != cancellation.DecisionCanceling ||
		deletion.result.RequestID == uuid.Nil || deletion.result.State != "PENDING" {
		t.Fatalf(
			"Customer Cancellation/Content Deletion decisions = %#v / %#v",
			canceled.result,
			deletion.result,
		)
	}

	var jobState, requestContent, reservationState, chargeReason string
	var requestContentDeletedAt *time.Time
	var chargeCount, cancellationCount, targetCount int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state::text,
			job.request_content::text,
			job.request_content_deleted_at,
			reservation.state::text,
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			coalesce((SELECT reason::text FROM charges WHERE job_id = job.id), ''),
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
			(SELECT count(*) FROM content_deletion_targets WHERE request_id = $2)
		FROM jobs AS job
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		WHERE job.id = $1
	`, fixture.candidate.JobID, deletion.result.RequestID).Scan(
		&jobState,
		&requestContent,
		&requestContentDeletedAt,
		&reservationState,
		&chargeCount,
		&chargeReason,
		&cancellationCount,
		&targetCount,
	); err != nil {
		t.Fatalf("read Customer Cancellation/Content Deletion authority: %v", err)
	}
	if jobState != "CANCELING" || requestContent != `{"deleted": true}` ||
		requestContentDeletedAt == nil || reservationState != "CONSUMED" ||
		chargeCount != 1 || chargeReason != "CUSTOMER_CANCELLATION" ||
		cancellationCount != 1 || targetCount != 1 {
		t.Fatalf(
			"shared cancellation authority = state %s content %s deleted %v reservation %s charge %d/%s cancellation %d targets %d",
			jobState,
			requestContent,
			requestContentDeletedAt,
			reservationState,
			chargeCount,
			chargeReason,
			cancellationCount,
			targetCount,
		)
	}
	assertRequestContentTombstoneImmutable(t, fixture.database.Admin, fixture.candidate.JobID)
}

func TestProjectAdminContentDeletionAuthorityIsRevokedForDeveloperOverProductionHTTP(
	t *testing.T,
) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	adminID := uuid.New()
	seedHumanRoleFixture(
		t,
		database.Admin,
		adminID,
		"content-deletion-project-admin",
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
			Subject:   "content-deletion-project-admin",
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}},
	)
	retentionService, err := retention.NewService(newRolePool(
		t,
		database.DSN,
		"vela_retention_request_login",
		"vela-retention-request-password",
	))
	if err != nil {
		t.Fatalf("create Content Deletion service: %v", err)
	}
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          authenticator,
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              retentionService,
		Admission:              admission.NewLegacyService(requestPool),
		Cancellation:           &cancellation.Service{},
		Artifacts:              &artifactaccess.Service{},
		Webhooks:               &webhook.Service{},
	})
	if err != nil {
		t.Fatalf("create ProjectAdmin Content Deletion handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	jobBody := []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"ProjectAdmin may delete this Customer Content"
	}`)
	submit := func(idempotencyKey string) jobResponse {
		t.Helper()
		result := submitJob(t, server.URL, idempotencyKey, jobBody)
		if result.StatusCode != http.StatusAccepted {
			t.Fatalf(
				"submit %s status = %d, want 202; body=%s",
				idempotencyKey,
				result.StatusCode,
				result.Body,
			)
		}
		var accepted jobResponse
		if err := json.Unmarshal(result.Body, &accepted); err != nil {
			t.Fatalf("decode %s Job: %v", idempotencyKey, err)
		}
		return accepted
	}
	requestDeletion := func(jobID, idempotencyKey string) (int, []byte) {
		t.Helper()
		request, err := http.NewRequest(
			http.MethodPost,
			server.URL+"/v1/projects/"+testProjectID+"/jobs/"+jobID+
				"/content-deletion-requests",
			nil,
		)
		if err != nil {
			t.Fatalf("create ProjectAdmin Content Deletion request: %v", err)
		}
		request.Header.Set("Authorization", "Bearer content-deletion-project-admin-token")
		request.Header.Set("Idempotency-Key", idempotencyKey)
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatalf("request ProjectAdmin Content Deletion: %v", err)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatalf("read ProjectAdmin Content Deletion response: %v", err)
		}
		return response.StatusCode, body
	}

	adminTarget := submit("content-deletion-project-admin-target")
	status, body := requestDeletion(adminTarget.JobID, "project-admin-content-deletion")
	if status != http.StatusAccepted {
		t.Fatalf("ProjectAdmin Content Deletion = %d %s, want 202", status, body)
	}
	if _, err := database.Admin.Exec(`
		DELETE FROM project_role_bindings
		WHERE organization_id = $1 AND project_id = $2 AND principal_id = $3
	`, testOrganizationID, testProjectID, adminID); err != nil {
		t.Fatalf("remove ProjectAdmin role: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO project_role_bindings (
			organization_id, project_id, principal_id, role, assigned_by_principal_id
		) VALUES ($1, $2, $3, 'Developer', $3)
	`, testOrganizationID, testProjectID, adminID); err != nil {
		t.Fatalf("assign Developer role: %v", err)
	}
	developerTarget := submit("content-deletion-developer-target")
	status, body = requestDeletion(developerTarget.JobID, "developer-content-deletion")
	if status != http.StatusForbidden || !bytes.Contains(body, []byte(`"code":"forbidden"`)) {
		t.Fatalf("Developer Content Deletion = %d %s, want 403 forbidden", status, body)
	}
}

func TestContentDeletionReconcilerDeletesExactArtifactVersionsAndMultipartUploads(
	t *testing.T,
) {
	fixture := newStartFixtureWithRetention(t, "content-deletion-visible-completion", 30, 31)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
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
		t.Fatalf("CompleteVisibleCompletion = %#v error=%v", completed, err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'content_deletion:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant Content Deletion scope: %v", err)
	}
	authenticator := identity.NewAuthenticator(
		newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	)
	principal, err := authenticator.Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate Content Deletion Principal: %v", err)
	}
	requestService, err := retention.NewService(newRolePool(
		t,
		fixture.database.DSN,
		"vela_retention_request_login",
		"vela-retention-request-password",
	))
	if err != nil {
		t.Fatalf("create Content Deletion request service: %v", err)
	}
	deletion, err := requestService.AcceptContentDeletion(
		context.Background(),
		principal,
		uuid.MustParse(testProjectID),
		fixture.candidate.JobID,
		"visible-completion-content-deletion",
	)
	if err != nil {
		t.Fatalf("accept completed Job Content Deletion: %v", err)
	}

	type exactIdentity struct {
		key, version string
	}
	wantExact := make(map[exactIdentity]struct{}, len(artifactIDs))
	rows, err := fixture.database.Admin.Query(`
		SELECT object_key, object_version_id
		FROM artifacts
		WHERE job_id = $1
		ORDER BY kind, ordinal
	`, fixture.candidate.JobID)
	if err != nil {
		t.Fatalf("read exact Artifact identities: %v", err)
	}
	for rows.Next() {
		var identity exactIdentity
		if err := rows.Scan(&identity.key, &identity.version); err != nil {
			rows.Close()
			t.Fatalf("scan exact Artifact identity: %v", err)
		}
		wantExact[identity] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatalf("iterate exact Artifact identities: %v", err)
	}
	rows.Close()
	prefix := "artifacts/" + testOrganizationID + "/" + testProjectID + "/" +
		fixture.candidate.JobID.String() + "/"
	store := &recordingRetentionStore{
		multipart: []artifactstore.IncompleteMultipartUpload{{
			ObjectKey:   prefix + "orphan/video.mp4",
			UploadID:    "content-deletion-orphan-upload",
			InitiatedAt: time.Now().UTC().Add(-time.Hour),
		}},
	}
	reconciler, err := retention.NewReconciler(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_retention_login",
			"vela-retention-password",
		),
		store,
		retention.ReconcilerConfig{
			InstanceID: "retention-integration-reconciler",
			BatchSize:  10,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create Content Deletion Reconciler: %v", err)
	}
	result, err := reconciler.ReconcileBatch(context.Background())
	if err != nil {
		t.Fatalf("reconcile Content Deletion: %v", err)
	}
	if result.Claimed != len(artifactIDs)+1 || result.Completed != result.Claimed ||
		result.Failed != 0 {
		t.Fatalf("Content Deletion reconciliation result = %#v", result)
	}
	if len(store.deleted) != len(wantExact) {
		t.Fatalf("exact deletes = %#v, want %#v", store.deleted, wantExact)
	}
	for _, deleted := range store.deleted {
		if _, ok := wantExact[exactIdentity{key: deleted.ObjectKey, version: deleted.VersionID}]; !ok {
			t.Fatalf("unexpected exact deletion = %#v", deleted)
		}
	}
	if len(store.aborted) != 1 || store.aborted[0].ObjectKey != prefix+"orphan/video.mp4" ||
		store.aborted[0].UploadID != "content-deletion-orphan-upload" {
		t.Fatalf("multipart aborts = %#v", store.aborted)
	}

	var (
		requestState                                  string
		requestCompletedAt                            *time.Time
		receiptCount, deletedArtifacts                int
		artifactSetCount, visibleCompletionCount      int
		chargeCount, cancellationDecisionCount        int
		completedTargetCount, totalTargetAttemptCount int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT deletion.state::text, deletion.completed_at,
			(SELECT count(*) FROM content_deletion_receipts WHERE request_id = deletion.id),
			(SELECT count(*) FROM artifacts WHERE job_id = deletion.job_id AND state = 'DELETED'),
			(SELECT count(*) FROM artifact_sets WHERE job_id = deletion.job_id),
			(SELECT count(*) FROM visible_completions WHERE job_id = deletion.job_id),
			(SELECT count(*) FROM charges WHERE job_id = deletion.job_id),
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = deletion.job_id),
			(SELECT count(*) FROM content_deletion_targets
			 WHERE request_id = deletion.id AND state = 'COMPLETED'),
			(SELECT coalesce(sum(attempt_count), 0) FROM content_deletion_targets
			 WHERE request_id = deletion.id)
		FROM content_deletion_requests AS deletion
		WHERE deletion.id = $1
	`, deletion.RequestID).Scan(
		&requestState,
		&requestCompletedAt,
		&receiptCount,
		&deletedArtifacts,
		&artifactSetCount,
		&visibleCompletionCount,
		&chargeCount,
		&cancellationDecisionCount,
		&completedTargetCount,
		&totalTargetAttemptCount,
	); err != nil {
		t.Fatalf("read completed Content Deletion evidence: %v", err)
	}
	if requestState != "COMPLETED" || requestCompletedAt == nil || receiptCount != 1 ||
		deletedArtifacts != len(artifactIDs) || artifactSetCount != 1 ||
		visibleCompletionCount != 1 || chargeCount != 1 ||
		cancellationDecisionCount != 0 || completedTargetCount != len(artifactIDs)+1 ||
		totalTargetAttemptCount != len(artifactIDs)+1 {
		t.Fatalf(
			"completed Content Deletion evidence = state %s at %v receipt %d artifacts %d set %d visible %d charges %d cancellations %d targets %d attempts %d",
			requestState,
			requestCompletedAt,
			receiptCount,
			deletedArtifacts,
			artifactSetCount,
			visibleCompletionCount,
			chargeCount,
			cancellationDecisionCount,
			completedTargetCount,
			totalTargetAttemptCount,
		)
	}
	var receiptTargetCount, receiptTargetMismatchCount int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*)
			 FROM content_deletion_receipt_targets AS snapshot
			 JOIN content_deletion_receipts AS receipt ON receipt.id = snapshot.receipt_id
			 WHERE receipt.request_id = $1),
			(SELECT count(*) FROM (
				(
					SELECT target.id, target.action::text, target.attempt_count,
						target.completed_at, target.storage_outcome
					FROM content_deletion_targets AS target
					WHERE target.request_id = $1
					EXCEPT
					SELECT snapshot.target_id, snapshot.action::text, snapshot.attempt_count,
						snapshot.target_completed_at, snapshot.storage_outcome
					FROM content_deletion_receipt_targets AS snapshot
					WHERE snapshot.request_id = $1
				)
				UNION ALL
				(
					SELECT snapshot.target_id, snapshot.action::text, snapshot.attempt_count,
						snapshot.target_completed_at, snapshot.storage_outcome
					FROM content_deletion_receipt_targets AS snapshot
					WHERE snapshot.request_id = $1
					EXCEPT
					SELECT target.id, target.action::text, target.attempt_count,
						target.completed_at, target.storage_outcome
					FROM content_deletion_targets AS target
					WHERE target.request_id = $1
				)
			) AS mismatch)
	`, deletion.RequestID).Scan(&receiptTargetCount, &receiptTargetMismatchCount); err != nil {
		t.Fatalf("read immutable per-target receipt: %v", err)
	}
	if receiptTargetCount != len(artifactIDs)+1 || receiptTargetMismatchCount != 0 {
		t.Fatalf(
			"per-target receipt = count %d mismatches %d, want %d and 0",
			receiptTargetCount,
			receiptTargetMismatchCount,
			len(artifactIDs)+1,
		)
	}
	for operation, statement := range map[string]string{
		"update": `UPDATE content_deletion_receipt_targets
			SET attempt_count = attempt_count WHERE request_id = $1`,
		"delete": `DELETE FROM content_deletion_receipt_targets WHERE request_id = $1`,
	} {
		_, err := fixture.database.Admin.Exec(statement, deletion.RequestID)
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != "P0001" {
			t.Fatalf("%s per-target receipt error = %v, want immutable refusal", operation, err)
		}
	}
}

func TestContentDeletionReconcilerDeletesRealMinIOVersionsAndMultipartUploads(t *testing.T) {
	ctx := context.Background()
	minio := newMinIOFixture(t, "vela-retention-content-deletion")
	minio.enableVersioning(t)
	if err := minio.store.ValidateBucket(ctx); err != nil {
		t.Fatalf("validate retention Artifact Store: %v", err)
	}
	fixture := newStartFixtureWithRetention(t, "minio-content-deletion", 30, 59)
	if started, err := fixture.service.Start(
		ctx, fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	completionService := visibleCompletionService(t, fixture.database.DSN)
	plan, err := completionService.BeginFinalization(
		ctx, fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}

	artifactIDs := make([]uuid.UUID, 0, len(plan.Artifacts))
	versions := make([]artifactstore.ObjectVersion, 0, len(plan.Artifacts))
	for index, artifact := range plan.Artifacts {
		claimID := uuid.New()
		claim, claimErr := completionService.ClaimArtifactUpload(
			ctx, fixture.worker, fixture.credentials, artifact.UploadID, claimID,
		)
		if claimErr != nil || claim.Decision != workercontrol.ArtifactUploadClaimGranted {
			t.Fatalf("claim real Artifact %d upload = %#v error=%v", index, claim, claimErr)
		}
		session, sessionErr := minio.store.CreateMultipartUpload(
			ctx, artifact.ObjectKey, claim.ExpectedContentType,
		)
		if sessionErr != nil {
			t.Fatalf("create real Artifact %d multipart session: %v", index, sessionErr)
		}
		recorded, recordErr := completionService.RecordArtifactMultipartSession(
			ctx,
			fixture.worker,
			fixture.credentials,
			artifact.UploadID,
			claimID,
			session.UploadID,
		)
		if recordErr != nil || recorded.Decision != workercontrol.ArtifactMultipartSessionRecorded {
			t.Fatalf("record real Artifact %d multipart = %#v error=%v", index, recorded, recordErr)
		}
		payload := []byte(fmt.Sprintf("real MinIO retention Artifact %d", index))
		digest := sha256.Sum256(payload)
		part, uploadErr := minio.store.UploadPart(
			ctx, session, 1, bytes.NewReader(payload), int64(len(payload)), digest,
		)
		if uploadErr != nil {
			t.Fatalf("upload real Artifact %d multipart part: %v", index, uploadErr)
		}
		report := workercontrol.ArtifactUploadReport{
			SizeBytes:   int64(len(payload)),
			SHA256:      digest,
			ContentType: claim.ExpectedContentType,
			CompletedParts: []workercontrol.ArtifactUploadPart{{
				Number:         part.Number,
				ETag:           part.ETag,
				SizeBytes:      part.SizeBytes,
				ChecksumSHA256: part.ChecksumSHA256,
			}},
		}
		intent, intentErr := completionService.RecordArtifactCompletionIntent(
			ctx, fixture.worker, fixture.credentials, artifact.UploadID, report,
		)
		if intentErr != nil || intent.Decision != workercontrol.ArtifactCompletionIntentRecorded {
			t.Fatalf("record real Artifact %d completion intent = %#v error=%v", index, intent, intentErr)
		}
		version, completeErr := minio.store.CompleteMultipartUpload(
			ctx, session, []artifactstore.CompletedPart{part},
		)
		if completeErr != nil || version.VersionID == "" || version.ObjectKey != artifact.ObjectKey {
			t.Fatalf("complete real Artifact %d = %#v error=%v", index, version, completeErr)
		}
		report.ObjectVersionID = version.VersionID
		uploaded, uploadErr := completionService.RecordArtifactUploaded(
			ctx, fixture.worker, fixture.credentials, artifact.UploadID, report,
		)
		if uploadErr != nil || uploaded.Decision != workercontrol.ArtifactUploadRecorded ||
			uploaded.ObjectVersionID != version.VersionID {
			t.Fatalf("record real Artifact %d upload = %#v error=%v", index, uploaded, uploadErr)
		}
		verified, verifyErr := completionService.VerifyArtifact(
			ctx, fixture.worker, fixture.credentials, artifact.UploadID, uuid.New(),
		)
		if verifyErr != nil || verified.Decision != workercontrol.ArtifactVerified {
			t.Fatalf("verify real Artifact %d = %#v error=%v", index, verified, verifyErr)
		}
		reader, readErr := minio.store.ReadExactVersion(ctx, version.ObjectKey, version.VersionID)
		if readErr != nil {
			t.Fatalf("read real Artifact %d before deletion: %v", index, readErr)
		}
		if closeErr := reader.Close(); closeErr != nil {
			t.Fatalf("close real Artifact %d before deletion: %v", index, closeErr)
		}
		artifactIDs = append(artifactIDs, artifact.ArtifactID)
		versions = append(versions, version)
	}
	completed, err := completionService.CompleteVisibleCompletion(
		ctx,
		fixture.worker,
		fixture.credentials,
		workercontrol.VisibleCompletionCandidate{
			CompletionID:       uuid.New(),
			ExpectedJobVersion: plan.JobVersion,
			ArtifactIDs:        artifactIDs,
		},
	)
	if err != nil || completed.Decision != workercontrol.VisibleCompletionCommitted {
		t.Fatalf("real MinIO Visible Completion = %#v error=%v", completed, err)
	}

	objectPrefix := "artifacts/" + testOrganizationID + "/" + testProjectID + "/" +
		fixture.candidate.JobID.String() + "/"
	orphan, err := minio.store.CreateMultipartUpload(
		ctx, objectPrefix+"orphan/debug.bin", "application/octet-stream",
	)
	if err != nil {
		t.Fatalf("create real orphan multipart session: %v", err)
	}
	orphanPayload := []byte("incomplete multipart must be aborted")
	orphanDigest := sha256.Sum256(orphanPayload)
	if _, err := minio.store.UploadPart(
		ctx,
		orphan,
		1,
		bytes.NewReader(orphanPayload),
		int64(len(orphanPayload)),
		orphanDigest,
	); err != nil {
		t.Fatalf("upload real orphan multipart part: %v", err)
	}
	incomplete, err := minio.store.ListIncompleteMultipartUploads(ctx, objectPrefix)
	if err != nil || len(incomplete) != 1 || incomplete[0].UploadID != orphan.UploadID {
		t.Fatalf("real incomplete multipart before deletion = %#v error=%v", incomplete, err)
	}

	requestService, principal := contentDeletionAuthority(t, fixture.database)
	deletion, err := requestService.AcceptContentDeletion(
		ctx,
		principal,
		uuid.MustParse(testProjectID),
		fixture.candidate.JobID,
		"real-minio-content-deletion",
	)
	if err != nil {
		t.Fatalf("accept real MinIO Content Deletion: %v", err)
	}
	retentionPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_retention_login",
		"vela-retention-password",
	)
	crashedClaimID := uuid.New()
	var crashedTargetID uuid.UUID
	if err := retentionPool.QueryRow(ctx, `
		SELECT target_id
		FROM vela_claim_content_deletion_target($1, $2, $3)
	`, "crashed-real-minio-reconciler", crashedClaimID, 1).Scan(&crashedTargetID); err != nil {
		t.Fatalf("claim real MinIO target before simulated crash: %v", err)
	}
	time.Sleep(1100 * time.Millisecond)

	const injectedStorageError = "injected S3-compatible storage failure"
	failingStore := &failFirstRetentionStore{
		delegate: minio.store,
		failure:  errors.New(injectedStorageError),
	}
	failingReconciler, err := retention.NewReconciler(
		retentionPool,
		failingStore,
		retention.ReconcilerConfig{
			InstanceID: "real-minio-retry-reconciler",
			BatchSize:  1,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create failing real MinIO retention Reconciler: %v", err)
	}
	failed, reconcileErr := failingReconciler.ReconcileBatch(ctx)
	if reconcileErr == nil || strings.Contains(reconcileErr.Error(), injectedStorageError) ||
		failed.Claimed != 1 || failed.Completed != 0 || failed.Failed != 1 {
		t.Fatalf("real MinIO injected failure = %#v error=%v", failed, reconcileErr)
	}
	var failedState string
	var failedAttempts int
	if err := fixture.database.Admin.QueryRow(`
		SELECT state::text, attempt_count
		FROM content_deletion_targets
		WHERE id = $1
	`, crashedTargetID).Scan(&failedState, &failedAttempts); err != nil {
		t.Fatalf("read recovered real MinIO target after injected failure: %v", err)
	}
	if failedState != "RETRY_WAIT" || failedAttempts != 2 {
		t.Fatalf(
			"recovered real MinIO target after failure = state %s attempts %d, want RETRY_WAIT/2",
			failedState,
			failedAttempts,
		)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE content_deletion_requests
		SET next_retry_at = clock_timestamp()
		WHERE id = $1
	`, deletion.RequestID); err != nil {
		t.Fatalf("advance real MinIO request retry eligibility: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE content_deletion_targets
		SET next_retry_at = clock_timestamp()
		WHERE id = $1
	`, crashedTargetID); err != nil {
		t.Fatalf("advance real MinIO target retry eligibility: %v", err)
	}
	restartedStore, err := artifactstore.NewS3(minio.config)
	if err != nil {
		t.Fatalf("restart real MinIO Artifact Store after failure: %v", err)
	}
	reconciler, err := retention.NewReconciler(
		retentionPool,
		restartedStore,
		retention.ReconcilerConfig{
			InstanceID: "restarted-real-minio-retention-reconciler",
			BatchSize:  10,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create restarted real MinIO retention Reconciler: %v", err)
	}
	result, err := reconciler.ReconcileBatch(ctx)
	if err != nil || result.Claimed != len(versions)+1 || result.Completed != result.Claimed ||
		result.Failed != 0 {
		t.Fatalf("real MinIO Content Deletion result = %#v error=%v", result, err)
	}
	for index, version := range versions {
		reader, readErr := minio.store.ReadExactVersion(ctx, version.ObjectKey, version.VersionID)
		if readErr == nil {
			_ = reader.Close()
			t.Fatalf("real Artifact %d exact version remained readable after deletion", index)
		}
	}
	incomplete, err = minio.store.ListIncompleteMultipartUploads(ctx, objectPrefix)
	if err != nil || len(incomplete) != 0 {
		t.Fatalf("real incomplete multipart after deletion = %#v error=%v", incomplete, err)
	}
	if _, err := minio.store.UploadPart(
		ctx,
		orphan,
		2,
		bytes.NewReader(orphanPayload),
		int64(len(orphanPayload)),
		orphanDigest,
	); err == nil {
		t.Fatal("aborted real multipart session accepted a later part")
	}

	var (
		requestState                                      string
		deletedArtifacts, receiptTargets, matchingTargets int
		multipartOutcomes                                 int
		totalAttempts, crashedTargetAttempts              int64
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT deletion.state::text,
			(SELECT count(*) FROM artifacts
			 WHERE job_id = deletion.job_id AND state = 'DELETED'),
			(SELECT count(*) FROM content_deletion_receipt_targets
			 WHERE request_id = deletion.id),
			(SELECT count(*)
			 FROM content_deletion_receipt_targets AS snapshot
			 JOIN content_deletion_targets AS target
			   ON target.id = snapshot.target_id
			  AND target.organization_id = snapshot.organization_id
			  AND target.project_id = snapshot.project_id
			  AND target.job_id = snapshot.job_id
			  AND target.request_id = snapshot.request_id
			  AND target.action = snapshot.action
			  AND target.attempt_count = snapshot.attempt_count
			  AND target.completed_at = snapshot.target_completed_at
			  AND target.storage_outcome = snapshot.storage_outcome
			 WHERE snapshot.request_id = deletion.id),
			(SELECT count(*) FROM content_deletion_receipt_targets
			 WHERE request_id = deletion.id
			   AND action = 'MULTIPART_PREFIX'
			   AND storage_outcome = 'MULTIPART_ABORTED'),
			(SELECT total_attempt_count FROM content_deletion_receipts
			 WHERE request_id = deletion.id),
			(SELECT attempt_count FROM content_deletion_targets
			 WHERE id = $2)
		FROM content_deletion_requests AS deletion
		WHERE deletion.id = $1
	`, deletion.RequestID, crashedTargetID).Scan(
		&requestState,
		&deletedArtifacts,
		&receiptTargets,
		&matchingTargets,
		&multipartOutcomes,
		&totalAttempts,
		&crashedTargetAttempts,
	); err != nil {
		t.Fatalf("read real MinIO deletion receipt: %v", err)
	}
	if requestState != "COMPLETED" || deletedArtifacts != len(versions) ||
		receiptTargets != len(versions)+1 || matchingTargets != receiptTargets ||
		multipartOutcomes != 1 || totalAttempts != int64(receiptTargets)+2 ||
		crashedTargetAttempts != 3 {
		t.Fatalf(
			"real MinIO deletion receipt = state %s artifacts %d targets %d matching %d multipart %d attempts %d recoveredTargetAttempts %d",
			requestState,
			deletedArtifacts,
			receiptTargets,
			matchingTargets,
			multipartOutcomes,
			totalAttempts,
			crashedTargetAttempts,
		)
	}
}

func TestContentDeletionReconcilerDiscoversStagingVersionBeforeExactDelete(t *testing.T) {
	fixture := newStartFixtureWithRetention(t, "content-deletion-staging-discovery", 30, 37)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	resolved := make(map[string]artifactstore.ObjectVersion)
	rows, err := fixture.database.Admin.Query(`
		SELECT object_key
		FROM artifacts
		WHERE job_id = $1 AND state = 'STAGING' AND object_version_id IS NULL
		ORDER BY kind, ordinal
	`, fixture.candidate.JobID)
	if err != nil {
		t.Fatalf("read STAGING Artifact keys: %v", err)
	}
	for rows.Next() {
		var objectKey string
		if err := rows.Scan(&objectKey); err != nil {
			rows.Close()
			t.Fatalf("scan STAGING Artifact key: %v", err)
		}
		resolved[objectKey] = artifactstore.ObjectVersion{
			ObjectKey: objectKey,
			VersionID: "discovered-version-" + uuid.NewString(),
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatalf("iterate STAGING Artifact keys: %v", err)
	}
	rows.Close()
	if len(resolved) == 0 {
		t.Fatal("BeginFinalization created no STAGING Artifacts")
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'content_deletion:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant Content Deletion scope: %v", err)
	}
	principal, err := identity.NewAuthenticator(
		newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate Content Deletion Principal: %v", err)
	}
	requestService, err := retention.NewService(newRolePool(
		t,
		fixture.database.DSN,
		"vela_retention_request_login",
		"vela-retention-request-password",
	))
	if err != nil {
		t.Fatalf("create Content Deletion request service: %v", err)
	}
	deletion, err := requestService.AcceptContentDeletion(
		context.Background(),
		principal,
		uuid.MustParse(testProjectID),
		fixture.candidate.JobID,
		"staging-discovery-content-deletion",
	)
	if err != nil {
		t.Fatalf("accept STAGING Content Deletion: %v", err)
	}
	store := &recordingRetentionStore{resolved: resolved}
	reconciler, err := retention.NewReconciler(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_retention_login",
			"vela-retention-password",
		),
		store,
		retention.ReconcilerConfig{
			InstanceID: "retention-discovery-reconciler",
			BatchSize:  len(resolved) + 1,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create discovery Reconciler: %v", err)
	}
	result, err := reconciler.ReconcileBatch(context.Background())
	if err != nil {
		t.Fatalf("reconcile STAGING Content Deletion: %v", err)
	}
	if result.Claimed != len(resolved)+1 || result.Completed != result.Claimed ||
		result.Failed != 0 || len(store.deleted) != len(resolved) {
		t.Fatalf("STAGING reconciliation = %#v deletes=%#v", result, store.deleted)
	}
	for _, exact := range store.deleted {
		want, ok := resolved[exact.ObjectKey]
		if !ok || exact.VersionID != want.VersionID {
			t.Fatalf("STAGING exact deletion = %#v, resolved=%#v", exact, resolved)
		}
	}
	var (
		completedDiscoveries, deletedArtifacts, abortedUploads int
		requestState                                           string
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT deletion.state::text,
			(SELECT count(*) FROM content_deletion_targets
			 WHERE request_id = deletion.id
			   AND action = 'OBJECT_DISCOVERY'
			   AND state = 'COMPLETED'
			   AND discovered_object_version_id IS NOT NULL),
			(SELECT count(*) FROM artifacts
			 WHERE job_id = deletion.job_id AND state = 'DELETED'),
			(SELECT count(*) FROM artifact_uploads
			 WHERE job_id = deletion.job_id AND state = 'ABORTED')
		FROM content_deletion_requests AS deletion
		WHERE deletion.id = $1
	`, deletion.RequestID).Scan(
		&requestState,
		&completedDiscoveries,
		&deletedArtifacts,
		&abortedUploads,
	); err != nil {
		t.Fatalf("read STAGING deletion evidence: %v", err)
	}
	if requestState != "COMPLETED" || completedDiscoveries != len(resolved) ||
		deletedArtifacts != len(resolved) || abortedUploads != len(resolved) {
		t.Fatalf(
			"STAGING deletion evidence = state %s discoveries %d artifacts %d uploads %d, want %d",
			requestState,
			completedDiscoveries,
			deletedArtifacts,
			abortedUploads,
			len(resolved),
		)
	}
}

func TestContentDeletionReconcilerRetriesWithSanitizedOperationalError(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'content_deletion:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant Content Deletion scope: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	acceptedResult := submitJob(t, server.URL, "content-deletion-retry-target", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"raw Customer Content must not enter retry errors"
	}`))
	if acceptedResult.StatusCode != http.StatusAccepted {
		t.Fatalf(
			"submit retry target status = %d; body=%s",
			acceptedResult.StatusCode,
			acceptedResult.Body,
		)
	}
	var accepted jobResponse
	if err := json.Unmarshal(acceptedResult.Body, &accepted); err != nil {
		t.Fatalf("decode retry target Job: %v", err)
	}
	principal, err := identity.NewAuthenticator(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate retry Content Deletion Principal: %v", err)
	}
	requestService, err := retention.NewService(newRolePool(
		t,
		database.DSN,
		"vela_retention_request_login",
		"vela-retention-request-password",
	))
	if err != nil {
		t.Fatalf("create retry Content Deletion request service: %v", err)
	}
	deletion, err := requestService.AcceptContentDeletion(
		context.Background(),
		principal,
		uuid.MustParse(testProjectID),
		uuid.MustParse(accepted.JobID),
		"content-deletion-retry-key",
	)
	if err != nil {
		t.Fatalf("accept retry Content Deletion: %v", err)
	}
	const rawStorageError = "S3 failure for artifacts/private-customer-path secret-version"
	store := &recordingRetentionStore{listErr: errors.New(rawStorageError)}
	reconciler, err := retention.NewReconciler(
		newRolePool(
			t,
			database.DSN,
			"vela_retention_login",
			"vela-retention-password",
		),
		store,
		retention.ReconcilerConfig{
			InstanceID: "retention-retry-reconciler",
			BatchSize:  1,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create retry Reconciler: %v", err)
	}
	failed, reconcileErr := reconciler.ReconcileBatch(context.Background())
	if reconcileErr == nil || strings.Contains(reconcileErr.Error(), rawStorageError) ||
		failed.Claimed != 1 || failed.Completed != 0 || failed.Failed != 1 {
		t.Fatalf("failed reconciliation = %#v error=%v", failed, reconcileErr)
	}
	var (
		requestState, targetState, errorCode, errorMessage string
		attemptCount                                       int
	)
	if err := database.Admin.QueryRow(`
		SELECT deletion.state::text, target.state::text,
			deletion.last_error_code, deletion.last_error_message,
			target.attempt_count
		FROM content_deletion_requests AS deletion
		JOIN content_deletion_targets AS target ON target.request_id = deletion.id
		WHERE deletion.id = $1
	`, deletion.RequestID).Scan(
		&requestState,
		&targetState,
		&errorCode,
		&errorMessage,
		&attemptCount,
	); err != nil {
		t.Fatalf("read retry evidence: %v", err)
	}
	if requestState != "RETRY_WAIT" || targetState != "RETRY_WAIT" ||
		errorCode != "STORAGE_OPERATION_FAILED" ||
		errorMessage != "Artifact storage operation failed" || attemptCount != 1 ||
		strings.Contains(errorMessage, "artifacts/") || strings.Contains(errorMessage, "secret") {
		t.Fatalf(
			"retry evidence = request %s target %s code %s message %q attempts %d",
			requestState,
			targetState,
			errorCode,
			errorMessage,
			attemptCount,
		)
	}
	if _, err := database.Admin.Exec(`
		UPDATE content_deletion_requests
		SET next_retry_at = clock_timestamp()
		WHERE id = $1
	`, deletion.RequestID); err != nil {
		t.Fatalf("advance request retry eligibility: %v", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE content_deletion_targets
		SET next_retry_at = clock_timestamp()
		WHERE request_id = $1
	`, deletion.RequestID); err != nil {
		t.Fatalf("advance target retry eligibility: %v", err)
	}
	store.listErr = nil
	retried, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || retried.Claimed != 1 || retried.Completed != 1 || retried.Failed != 0 {
		t.Fatalf("successful retry = %#v error=%v", retried, err)
	}
	var (
		completedState                  string
		completedAttempts, receiptCount int
		lastErrorCode, lastErrorMessage *string
	)
	if err := database.Admin.QueryRow(`
		SELECT deletion.state::text, deletion.last_error_code, deletion.last_error_message,
			target.attempt_count,
			(SELECT count(*) FROM content_deletion_receipts WHERE request_id = deletion.id)
		FROM content_deletion_requests AS deletion
		JOIN content_deletion_targets AS target ON target.request_id = deletion.id
		WHERE deletion.id = $1
	`, deletion.RequestID).Scan(
		&completedState,
		&lastErrorCode,
		&lastErrorMessage,
		&completedAttempts,
		&receiptCount,
	); err != nil {
		t.Fatalf("read completed retry evidence: %v", err)
	}
	if completedState != "COMPLETED" || lastErrorCode != nil || lastErrorMessage != nil ||
		completedAttempts != 2 || receiptCount != 1 {
		t.Fatalf(
			"completed retry evidence = state %s code %v message %v attempts %d receipts %d",
			completedState,
			lastErrorCode,
			lastErrorMessage,
			completedAttempts,
			receiptCount,
		)
	}
}

func TestAutomaticRequestContentExpiryIsIndependentAndIdempotent(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_cat(scopes, ARRAY['jobs:cancel', 'content_deletion:manage'])
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation and Content Deletion scopes: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	acceptedResult := submitJob(t, server.URL, "automatic-request-content-expiry", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"expire this request content independently"
	}`))
	if acceptedResult.StatusCode != http.StatusAccepted {
		t.Fatalf(
			"submit request-expiry target status = %d; body=%s",
			acceptedResult.StatusCode,
			acceptedResult.Body,
		)
	}
	var accepted jobResponse
	if err := json.Unmarshal(acceptedResult.Body, &accepted); err != nil {
		t.Fatalf("decode request-expiry target Job: %v", err)
	}
	principal, err := identity.NewAuthenticator(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate request-expiry Principal: %v", err)
	}
	cancellationResult, err := cancellation.NewService(
		newRolePool(t, database.DSN, "vela_cancel_login", "vela-cancel-password"),
		newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password"),
	).Cancel(
		context.Background(),
		principal,
		uuid.MustParse(testProjectID),
		uuid.MustParse(accepted.JobID),
	)
	if err != nil || cancellationResult.State != "CANCELED" {
		t.Fatalf("cancel request-expiry target = %#v error=%v", cancellationResult, err)
	}
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin request-expiry time setup: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`SET LOCAL session_replication_role = 'replica'`); err != nil {
		t.Fatalf("disable immutable triggers for time setup: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE jobs
		SET created_at = clock_timestamp() - interval '40 days',
			job_expires_at = clock_timestamp() - interval '31 days',
			request_content_expires_at = clock_timestamp() - interval '1 day'
		WHERE id = $1
	`, accepted.JobID); err != nil {
		t.Fatalf("move request-content deadline into past: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit request-expiry time setup: %v", err)
	}
	reconciler, err := retention.NewReconciler(
		newRolePool(
			t,
			database.DSN,
			"vela_retention_login",
			"vela-retention-password",
		),
		&recordingRetentionStore{},
		retention.ReconcilerConfig{
			InstanceID: "automatic-request-content-reconciler",
			BatchSize:  10,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create automatic request-content Reconciler: %v", err)
	}
	result, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || result.RequestContentExpired != 1 ||
		result.ArtifactRequestsCreated != 0 || result.Claimed != 0 {
		t.Fatalf("automatic request-content expiry = %#v error=%v", result, err)
	}
	var (
		jobState, requestContent, reservationState, deletionState, source string
		deletedAt                                                         *time.Time
		requestCount, receiptCount, targetCount, cancellationCount        int
	)
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, job.request_content::text, job.request_content_deleted_at,
			reservation.state::text, deletion.state::text, deletion.source::text,
			(SELECT count(*) FROM content_deletion_requests WHERE job_id = job.id
			 AND source = 'RETENTION_REQUEST_CONTENT'),
			(SELECT count(*) FROM content_deletion_receipts WHERE request_id = deletion.id),
			(SELECT count(*) FROM content_deletion_targets WHERE request_id = deletion.id),
			(SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id)
		FROM jobs AS job
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN content_deletion_requests AS deletion
		  ON deletion.job_id = job.id AND deletion.source = 'RETENTION_REQUEST_CONTENT'
		WHERE job.id = $1
	`, accepted.JobID).Scan(
		&jobState,
		&requestContent,
		&deletedAt,
		&reservationState,
		&deletionState,
		&source,
		&requestCount,
		&receiptCount,
		&targetCount,
		&cancellationCount,
	); err != nil {
		t.Fatalf("read automatic request-content evidence: %v", err)
	}
	if jobState != "CANCELED" || requestContent != `{"deleted": true}` || deletedAt == nil ||
		reservationState != "RELEASED" || deletionState != "COMPLETED" ||
		source != "RETENTION_REQUEST_CONTENT" || requestCount != 1 || receiptCount != 1 ||
		targetCount != 0 || cancellationCount != 1 {
		t.Fatalf(
			"automatic request-content evidence = job %s content %s deleted %v reservation %s deletion %s source %s requests %d receipts %d targets %d cancellations %d",
			jobState,
			requestContent,
			deletedAt,
			reservationState,
			deletionState,
			source,
			requestCount,
			receiptCount,
			targetCount,
			cancellationCount,
		)
	}
	replayed, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || replayed.RequestContentExpired != 0 ||
		replayed.ArtifactRequestsCreated != 0 || replayed.Claimed != 0 {
		t.Fatalf("replayed automatic request-content expiry = %#v error=%v", replayed, err)
	}
}

func TestAutomaticRequestContentExpiryProcessesOverdueNonterminalJobs(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	submit := func(idempotencyKey string) uuid.UUID {
		t.Helper()
		result := submitJob(t, server.URL, idempotencyKey, []byte(`{
			"model":"minimax-h3",
			"generation_preset":"balanced",
			"service_class":"standard",
			"output_spec":"video-1080p-5s-24fps",
			"generation_count":1,
			"prompt":"expire content for a stuck nonterminal Job"
		}`))
		if result.StatusCode != http.StatusAccepted {
			t.Fatalf("submit %s status = %d; body=%s", idempotencyKey, result.StatusCode, result.Body)
		}
		var accepted jobResponse
		if err := json.Unmarshal(result.Body, &accepted); err != nil {
			t.Fatalf("decode %s Job: %v", idempotencyKey, err)
		}
		return uuid.MustParse(accepted.JobID)
	}
	queuedJobID := submit("automatic-request-content-expiry-queued")
	runningJobID := submit("automatic-request-content-expiry-running")

	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin nonterminal expiry setup: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`SET LOCAL session_replication_role = 'replica'`); err != nil {
		t.Fatalf("disable immutable triggers for nonterminal expiry setup: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE jobs
		SET created_at = clock_timestamp() - interval '40 days',
			job_expires_at = clock_timestamp() - interval '31 days',
			request_content_expires_at = clock_timestamp() - interval '1 day'
		WHERE id = $1
	`, queuedJobID); err != nil {
		t.Fatalf("expire QUEUED Job request content: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE jobs
		SET state = 'RUNNING',
			created_at = clock_timestamp() - interval '40 days',
			billable_started_at = clock_timestamp() - interval '39 days',
			job_expires_at = clock_timestamp() - interval '31 days',
			request_content_expires_at = clock_timestamp() - interval '1 day'
		WHERE id = $1
	`, runningJobID); err != nil {
		t.Fatalf("expire RUNNING Job request content: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit nonterminal expiry setup: %v", err)
	}

	reconciler, err := retention.NewReconciler(
		newRolePool(t, database.DSN, "vela_retention_login", "vela-retention-password"),
		&recordingRetentionStore{},
		retention.ReconcilerConfig{
			InstanceID: "nonterminal-request-content-reconciler",
			BatchSize:  10,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create nonterminal request-content Reconciler: %v", err)
	}
	result, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || result.RequestContentExpired != 2 ||
		result.ArtifactRequestsCreated != 0 || result.Claimed != 0 {
		t.Fatalf("nonterminal request-content expiry = %#v error=%v", result, err)
	}

	rows, err := database.Admin.Query(`
		SELECT job.id, job.state::text, job.request_content::text,
			job.request_content_deleted_at,
			deletion.state::text,
			(SELECT count(*) FROM content_deletion_targets WHERE request_id = deletion.id),
			(SELECT count(*) FROM content_deletion_receipts WHERE request_id = deletion.id)
		FROM jobs AS job
		JOIN content_deletion_requests AS deletion
		  ON deletion.job_id = job.id AND deletion.source = 'RETENTION_REQUEST_CONTENT'
		WHERE job.id = ANY($1::uuid[])
		ORDER BY job.id
	`, []uuid.UUID{queuedJobID, runningJobID})
	if err != nil {
		t.Fatalf("read nonterminal request-content evidence: %v", err)
	}
	defer rows.Close()
	wantStates := map[uuid.UUID]string{queuedJobID: "QUEUED", runningJobID: "RUNNING"}
	seen := 0
	for rows.Next() {
		var (
			jobID                            uuid.UUID
			jobState, content, deletionState string
			deletedAt                        *time.Time
			targetCount, receiptCount        int
		)
		if err := rows.Scan(
			&jobID, &jobState, &content, &deletedAt, &deletionState, &targetCount, &receiptCount,
		); err != nil {
			t.Fatalf("scan nonterminal request-content evidence: %v", err)
		}
		if jobState != wantStates[jobID] || content != `{"deleted": true}` || deletedAt == nil ||
			deletionState != "COMPLETED" || targetCount != 0 || receiptCount != 1 {
			t.Fatalf(
				"nonterminal request-content evidence = job %s state %s content %s deleted %v deletion %s targets %d receipts %d",
				jobID, jobState, content, deletedAt, deletionState, targetCount, receiptCount,
			)
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate nonterminal request-content evidence: %v", err)
	}
	if seen != 2 {
		t.Fatalf("nonterminal request-content evidence rows = %d, want 2", seen)
	}
	replayed, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || replayed.RequestContentExpired != 0 || replayed.Claimed != 0 {
		t.Fatalf("replayed nonterminal request-content expiry = %#v error=%v", replayed, err)
	}
}

func TestTerminalFailedJobEnqueuesAndDeletesIncompleteArtifacts(t *testing.T) {
	fixture := newStartFixture(t, "terminal-failed-incomplete-artifact-cleanup", 7)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	observation := workercontrol.FailureObservation{
		FailureClass: "ARTIFACT_VALIDATION_FAILED",
		FailureFingerprint: fmt.Sprintf(
			"artifact.validation/%s/%s",
			plan.Artifacts[0].ArtifactID,
			plan.Artifacts[0].UploadID,
		),
		ErrorSummary:             "Artifact failed certified output validation",
		BackendStage:             "finalization",
		InferenceBackendRevision: "sglang@terminal-cleanup-test",
		RetryRecommended:         true,
		WorkerReusable:           true,
	}
	failed, err := fixture.service.Fail(
		context.Background(), fixture.worker, fixture.credentials, observation,
	)
	if err != nil || failed.Disposition != workercontrol.RetryDispositionFailed {
		t.Fatalf("Fail terminal finalization = %#v error=%v", failed, err)
	}
	var terminalAt time.Time
	if err := fixture.database.Admin.QueryRow(`
		SELECT occurred_at
		FROM outbox_events
		WHERE aggregate_id = $1
		  AND event_type = 'job.failed'
	`, plan.JobID).Scan(&terminalAt); err != nil {
		t.Fatalf("read immutable terminal event time: %v", err)
	}
	requestService, principal := contentDeletionAuthority(t, fixture.database)
	customerDeletion, err := requestService.AcceptContentDeletion(
		context.Background(),
		principal,
		uuid.MustParse(testProjectID),
		plan.JobID,
		"customer-and-internal-terminal-cleanup",
	)
	if err != nil || customerDeletion.RequestID == uuid.Nil {
		t.Fatalf("accept Customer deletion before internal cleanup = %#v error=%v", customerDeletion, err)
	}
	mutatedUpdatedAt := terminalAt.Add(72 * time.Hour)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE jobs SET updated_at = $2 WHERE id = $1
	`, plan.JobID, mutatedUpdatedAt); err != nil {
		t.Fatalf("move mutable Job updated_at after terminal transition: %v", err)
	}

	store := &recordingRetentionStore{
		resolved: make(map[string]artifactstore.ObjectVersion, len(plan.Artifacts)),
	}
	for _, artifact := range plan.Artifacts {
		store.resolved[artifact.ObjectKey] = artifactstore.ObjectVersion{
			ObjectKey: artifact.ObjectKey,
			VersionID: "lost-report-" + artifact.ArtifactID.String(),
		}
		store.multipart = append(store.multipart, artifactstore.IncompleteMultipartUpload{
			ObjectKey:   artifact.ObjectKey,
			UploadID:    "orphan-" + artifact.UploadID.String(),
			InitiatedAt: plan.FinalizationStartedAt,
		})
	}
	reconciler, err := retention.NewReconciler(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_retention_login",
			"vela-retention-password",
		),
		store,
		retention.ReconcilerConfig{
			InstanceID: "terminal-incomplete-artifact-reconciler",
			BatchSize:  2 * (len(plan.Artifacts) + 1),
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create incomplete Artifact Reconciler: %v", err)
	}
	result, err := reconciler.ReconcileBatch(context.Background())
	wantTargets := 2 * (len(plan.Artifacts) + 1)
	if err != nil || result.RequestContentExpired != 0 ||
		result.ArtifactRequestsCreated != 1 || result.Claimed != wantTargets ||
		result.Completed != result.Claimed || result.Failed != 0 {
		t.Fatalf("terminal incomplete Artifact cleanup = %#v error=%v", result, err)
	}
	if len(store.deleted) != 2*len(plan.Artifacts) || len(store.aborted) != 2*len(plan.Artifacts) {
		t.Fatalf(
			"terminal cleanup storage calls = deleted %#v aborted %#v",
			store.deleted,
			store.aborted,
		)
	}

	var (
		jobState, deletionState, deletionSource, customerState    string
		requestedAt, deadlineAt, jobUpdatedAt, jobTerminalAt      time.Time
		deletedArtifacts, abortedUploads, requests, customerCount int
		receipts, customerReceipts                                int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT job.state::text, deletion.state::text, deletion.source::text,
			customer.state::text,
			deletion.requested_at, deletion.deadline_at,
			job.updated_at,
			terminal.occurred_at,
			(SELECT count(*) FROM artifacts
			 WHERE job_id = job.id AND state = 'DELETED'),
			(SELECT count(*) FROM artifact_uploads
			 WHERE job_id = job.id AND state = 'ABORTED'),
			(SELECT count(*) FROM content_deletion_requests
			 WHERE job_id = job.id AND source = 'RETENTION_INCOMPLETE_ARTIFACT'),
			(SELECT count(*) FROM content_deletion_requests
			 WHERE job_id = job.id AND source = 'CUSTOMER'),
			(SELECT count(*) FROM content_deletion_receipts
			 WHERE request_id = deletion.id),
			(SELECT count(*) FROM content_deletion_receipts
			 WHERE request_id = customer.id)
		FROM jobs AS job
		JOIN outbox_events AS terminal
		  ON terminal.aggregate_id = job.id
		 AND terminal.event_type = 'job.failed'
		JOIN content_deletion_requests AS deletion
		  ON deletion.job_id = job.id
		 AND deletion.source = 'RETENTION_INCOMPLETE_ARTIFACT'
		JOIN content_deletion_requests AS customer
		  ON customer.id = $2
		 AND customer.job_id = job.id
		 AND customer.source = 'CUSTOMER'
		WHERE job.id = $1
	`, plan.JobID, customerDeletion.RequestID).Scan(
		&jobState,
		&deletionState,
		&deletionSource,
		&customerState,
		&requestedAt,
		&deadlineAt,
		&jobUpdatedAt,
		&jobTerminalAt,
		&deletedArtifacts,
		&abortedUploads,
		&requests,
		&customerCount,
		&receipts,
		&customerReceipts,
	); err != nil {
		t.Fatalf("read terminal incomplete Artifact cleanup evidence: %v", err)
	}
	if jobState != "FAILED" || deletionState != "COMPLETED" || customerState != "COMPLETED" ||
		deletionSource != "RETENTION_INCOMPLETE_ARTIFACT" ||
		!requestedAt.Equal(jobTerminalAt) || !deadlineAt.Equal(jobTerminalAt.Add(24*time.Hour)) ||
		!jobUpdatedAt.Equal(mutatedUpdatedAt) ||
		deletedArtifacts != len(plan.Artifacts) || abortedUploads != len(plan.Artifacts) ||
		requests != 1 || customerCount != 1 || receipts != 1 || customerReceipts != 1 {
		t.Fatalf(
			"terminal cleanup evidence = job %s deletion/customer/source %s/%s/%s times %s/%s updated/terminal %s/%s artifacts/uploads %d/%d requests/customer/receipts %d/%d/%d/%d",
			jobState,
			deletionState,
			customerState,
			deletionSource,
			requestedAt,
			deadlineAt,
			jobUpdatedAt,
			jobTerminalAt,
			deletedArtifacts,
			abortedUploads,
			requests,
			customerCount,
			receipts,
			customerReceipts,
		)
	}
	replayed, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || replayed.ArtifactRequestsCreated != 0 || replayed.Claimed != 0 {
		t.Fatalf("replayed terminal incomplete Artifact cleanup = %#v error=%v", replayed, err)
	}
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	downErr := goose.DownTo(fixture.database.Admin, migrations, 23)
	var postgresError *pgconn.PgError
	if !errors.As(downErr, &postgresError) || postgresError.Code != "55000" ||
		postgresError.ConstraintName != "incomplete_artifact_cleanup_requires_empty_evidence" {
		t.Fatalf("incomplete Artifact cleanup migration Down error = %v, want named SQLSTATE 55000", downErr)
	}
	version, versionErr := goose.GetDBVersion(fixture.database.Admin)
	if versionErr != nil || version != 24 {
		t.Fatalf("retention version after refused incomplete cleanup Down = %d error=%v", version, versionErr)
	}
}

func TestCanceledJobConcurrentCleanupCreatesOneRequestAndReceipt(t *testing.T) {
	fixture := newStartFixture(t, "canceled-incomplete-artifact-cleanup", 7)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	completionService := visibleCompletionService(t, fixture.database.DSN)
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t,
		completionService,
		fixture.worker,
		fixture.credentials,
		plan,
	)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant cancellation scope: %v", err)
	}
	canceled := cancelJob(
		t,
		admissionServerForDatabase(t, fixture.database).URL,
		testProjectID,
		plan.JobID.String(),
		testBearerCredential(),
	)
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel FINALIZING Job status = %d; body=%s", canceled.StatusCode, canceled.Body)
	}
	var cancellationResult cancelResponse
	if err := json.Unmarshal(canceled.Body, &cancellationResult); err != nil {
		t.Fatalf("decode FINALIZING cancellation: %v", err)
	}
	if cancellationResult.Decision != "CANCELING" || cancellationResult.Charge == nil {
		t.Fatalf("FINALIZING cancellation = %#v", cancellationResult)
	}
	preterminalStore := &recordingRetentionStore{}
	preterminalReconciler, err := retention.NewReconciler(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_retention_login",
			"vela-retention-password",
		),
		preterminalStore,
		retention.ReconcilerConfig{
			InstanceID: "canceling-incomplete-artifact-reconciler",
			BatchSize:  len(plan.Artifacts) + 1,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create CANCELING incomplete Artifact Reconciler: %v", err)
	}
	preterminal, err := preterminalReconciler.ReconcileBatch(context.Background())
	if err != nil || preterminal.ArtifactRequestsCreated != 0 || preterminal.Claimed != 0 ||
		len(preterminalStore.deleted) != 0 || len(preterminalStore.aborted) != 0 {
		t.Fatalf("CANCELING cleanup = %#v store=%#v error=%v", preterminal, preterminalStore, err)
	}
	var preterminalRequests int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*)
		FROM content_deletion_requests
		WHERE job_id = $1 AND source = 'RETENTION_INCOMPLETE_ARTIFACT'
	`, plan.JobID).Scan(&preterminalRequests); err != nil {
		t.Fatalf("count CANCELING incomplete cleanup requests: %v", err)
	}
	if preterminalRequests != 0 {
		t.Fatalf("CANCELING incomplete cleanup requests = %d, want 0", preterminalRequests)
	}
	coordinator := cancellation.NewService(
		newRolePool(t, fixture.database.DSN, "vela_cancel_login", "vela-cancel-password"),
		newRolePool(t, fixture.database.DSN, "vela_internal_login", "vela-internal-password"),
	)
	stopped, err := coordinator.AcknowledgeCancellationStop(
		context.Background(),
		fixture.worker,
		fixture.credentials,
		uuid.MustParse(cancellationResult.CancellationID),
	)
	if err != nil || stopped.Decision != cancellation.StopAcknowledged || stopped.State != "CANCELED" {
		t.Fatalf("acknowledge FINALIZING cancellation = %#v error=%v", stopped, err)
	}

	stores := []*recordingRetentionStore{{}, {}}
	reconcilers := make([]*retention.Reconciler, 0, len(stores))
	for index, store := range stores {
		reconciler, reconcileErr := retention.NewReconciler(
			newRolePool(
				t,
				fixture.database.DSN,
				"vela_retention_login",
				"vela-retention-password",
			),
			store,
			retention.ReconcilerConfig{
				InstanceID: fmt.Sprintf("canceled-incomplete-artifact-reconciler-%d", index),
				BatchSize:  len(plan.Artifacts) + 1,
				ClaimTTL:   time.Minute,
				RetryDelay: time.Minute,
			},
		)
		if reconcileErr != nil {
			t.Fatalf("create concurrent incomplete Artifact Reconciler %d: %v", index, reconcileErr)
		}
		reconcilers = append(reconcilers, reconciler)
	}
	type concurrentReconcileOutcome struct {
		result retention.ReconcileResult
		err    error
	}
	start := make(chan struct{})
	outcomes := make(chan concurrentReconcileOutcome, len(reconcilers))
	var wait sync.WaitGroup
	for _, reconciler := range reconcilers {
		reconciler := reconciler
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			result, reconcileErr := reconciler.ReconcileBatch(context.Background())
			outcomes <- concurrentReconcileOutcome{result: result, err: reconcileErr}
		}()
	}
	close(start)
	wait.Wait()
	close(outcomes)
	var created, claimed, completed, failed int
	for outcome := range outcomes {
		if outcome.err != nil {
			t.Fatalf("concurrent incomplete Artifact reconciliation: %v", outcome.err)
		}
		created += outcome.result.ArtifactRequestsCreated
		claimed += outcome.result.Claimed
		completed += outcome.result.Completed
		failed += outcome.result.Failed
	}
	wantTargets := len(plan.Artifacts) + 1
	if created != 1 || claimed != wantTargets || completed != wantTargets || failed != 0 {
		t.Fatalf(
			"concurrent canceled cleanup = created/claimed/completed/failed %d/%d/%d/%d, want 1/%d/%d/0",
			created,
			claimed,
			completed,
			failed,
			wantTargets,
			wantTargets,
		)
	}
	wantVersions := make(map[string]string, len(plan.Artifacts))
	for _, artifact := range plan.Artifacts {
		wantVersions[artifact.ObjectKey] = "version-" + artifact.ArtifactID.String()
	}
	deleted := 0
	for _, store := range stores {
		for _, object := range store.deleted {
			if wantVersions[object.ObjectKey] != object.VersionID {
				t.Fatalf("concurrent cleanup deleted unexpected object version %#v", object)
			}
			delete(wantVersions, object.ObjectKey)
			deleted++
		}
	}
	if deleted != len(plan.Artifacts) || len(wantVersions) != 0 {
		t.Fatalf("concurrent cleanup exact deletes = %d missing %#v", deleted, wantVersions)
	}

	var (
		jobState, requestState, requestSource       string
		requestedAt, deadlineAt, terminalAt         time.Time
		requests, targets, receipts, receiptTargets int
		deletedArtifacts, expiredUploads, charges   int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state::text,
			deletion.state::text,
			deletion.source::text,
			deletion.requested_at,
			deletion.deadline_at,
			job.updated_at,
			(SELECT count(*) FROM content_deletion_requests
			 WHERE job_id = job.id AND source = 'RETENTION_INCOMPLETE_ARTIFACT'),
			(SELECT count(*) FROM content_deletion_targets WHERE request_id = deletion.id),
			(SELECT count(*) FROM content_deletion_receipts WHERE request_id = deletion.id),
			(SELECT count(*) FROM content_deletion_receipt_targets
			 WHERE request_id = deletion.id),
			(SELECT count(*) FROM artifacts
			 WHERE job_id = job.id AND state = 'DELETED'),
			(SELECT count(*) FROM artifact_uploads
			 WHERE job_id = job.id AND state = 'EXPIRED'),
			(SELECT count(*) FROM charges WHERE job_id = job.id)
		FROM jobs AS job
		JOIN content_deletion_requests AS deletion
		  ON deletion.job_id = job.id
		 AND deletion.source = 'RETENTION_INCOMPLETE_ARTIFACT'
		WHERE job.id = $1
	`, plan.JobID).Scan(
		&jobState,
		&requestState,
		&requestSource,
		&requestedAt,
		&deadlineAt,
		&terminalAt,
		&requests,
		&targets,
		&receipts,
		&receiptTargets,
		&deletedArtifacts,
		&expiredUploads,
		&charges,
	); err != nil {
		t.Fatalf("read concurrent canceled cleanup evidence: %v", err)
	}
	if jobState != "CANCELED" || requestState != "COMPLETED" ||
		requestSource != "RETENTION_INCOMPLETE_ARTIFACT" ||
		!requestedAt.Equal(terminalAt) || !deadlineAt.Equal(terminalAt.Add(24*time.Hour)) ||
		requests != 1 || targets != wantTargets || receipts != 1 || receiptTargets != wantTargets ||
		deletedArtifacts != len(artifactIDs) || expiredUploads != len(artifactIDs) || charges != 1 {
		t.Fatalf(
			"concurrent canceled cleanup evidence = job/request %s/%s/%s times %s/%s/%s requests/targets/receipts/receipt-targets %d/%d/%d/%d artifacts/uploads/charges %d/%d/%d",
			jobState,
			requestState,
			requestSource,
			requestedAt,
			deadlineAt,
			terminalAt,
			requests,
			targets,
			receipts,
			receiptTargets,
			deletedArtifacts,
			expiredUploads,
			charges,
		)
	}
}

func TestAutomaticArtifactExpiryDoesNotShortenRequestContentRetention(t *testing.T) {
	fixture := newStartFixtureWithRetention(t, "automatic-artifact-expiry-7d", 7, 41)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
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
		t.Fatalf("CompleteVisibleCompletion = %#v error=%v", completed, err)
	}
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin Artifact expiry time setup: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`SET LOCAL session_replication_role = 'replica'`); err != nil {
		t.Fatalf("disable immutable triggers for Artifact time setup: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE artifact_sets
		SET committed_at = clock_timestamp() - interval '8 days',
			retention_expires_at = clock_timestamp() - interval '1 day'
		WHERE job_id = $1
	`, fixture.candidate.JobID); err != nil {
		t.Fatalf("move ArtifactSet retention into past: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE artifacts
		SET verified_at = clock_timestamp() - interval '8 days',
			retention_expires_at = clock_timestamp() - interval '1 day'
		WHERE job_id = $1
	`, fixture.candidate.JobID); err != nil {
		t.Fatalf("move Artifact retention into past: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE artifact_access_grants
		SET eligible_at = clock_timestamp() - interval '8 days',
			retention_expires_at = clock_timestamp() - interval '1 day'
		WHERE job_id = $1
	`, fixture.candidate.JobID); err != nil {
		t.Fatalf("move Artifact grant retention into past: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit Artifact expiry time setup: %v", err)
	}
	type exactIdentity struct {
		key, version string
	}
	wantExact := make(map[exactIdentity]struct{}, len(artifactIDs))
	rows, err := fixture.database.Admin.Query(`
		SELECT object_key, object_version_id
		FROM artifacts
		WHERE job_id = $1
	`, fixture.candidate.JobID)
	if err != nil {
		t.Fatalf("read expiring Artifact identities: %v", err)
	}
	for rows.Next() {
		var identity exactIdentity
		if err := rows.Scan(&identity.key, &identity.version); err != nil {
			rows.Close()
			t.Fatalf("scan expiring Artifact identity: %v", err)
		}
		wantExact[identity] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatalf("iterate expiring Artifact identities: %v", err)
	}
	rows.Close()
	store := &recordingRetentionStore{}
	reconciler, err := retention.NewReconciler(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_retention_login",
			"vela-retention-password",
		),
		store,
		retention.ReconcilerConfig{
			InstanceID: "automatic-artifact-expiry-reconciler",
			BatchSize:  len(artifactIDs) + 1,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create automatic Artifact Reconciler: %v", err)
	}
	result, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || result.RequestContentExpired != 0 ||
		result.ArtifactRequestsCreated != 1 || result.Claimed != len(artifactIDs)+1 ||
		result.Completed != result.Claimed || result.Failed != 0 {
		t.Fatalf("automatic Artifact expiry = %#v error=%v", result, err)
	}
	if len(store.deleted) != len(wantExact) {
		t.Fatalf("automatic exact deletes = %#v, want %#v", store.deleted, wantExact)
	}
	for _, deleted := range store.deleted {
		if _, ok := wantExact[exactIdentity{key: deleted.ObjectKey, version: deleted.VersionID}]; !ok {
			t.Fatalf("unexpected automatic exact deletion = %#v", deleted)
		}
	}
	var (
		requestContent, deletionState, deletionSource string
		requestContentDeletedAt, grantRevokedAt       *time.Time
		deletedArtifacts, requestCount, receiptCount  int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT job.request_content::text, job.request_content_deleted_at,
			deletion.state::text, deletion.source::text,
			access_grant.revoked_at,
			(SELECT count(*) FROM artifacts WHERE job_id = job.id AND state = 'DELETED'),
			(SELECT count(*) FROM content_deletion_requests
			 WHERE job_id = job.id AND source = 'RETENTION_ARTIFACT'),
			(SELECT count(*) FROM content_deletion_receipts WHERE request_id = deletion.id)
		FROM jobs AS job
		JOIN artifact_access_grants AS access_grant ON access_grant.job_id = job.id
		JOIN content_deletion_requests AS deletion
		  ON deletion.job_id = job.id AND deletion.source = 'RETENTION_ARTIFACT'
		WHERE job.id = $1
	`, fixture.candidate.JobID).Scan(
		&requestContent,
		&requestContentDeletedAt,
		&deletionState,
		&deletionSource,
		&grantRevokedAt,
		&deletedArtifacts,
		&requestCount,
		&receiptCount,
	); err != nil {
		t.Fatalf("read automatic Artifact evidence: %v", err)
	}
	if !strings.Contains(requestContent, "verify the immutable Artifact retention snapshot") ||
		requestContentDeletedAt != nil || deletionState != "COMPLETED" ||
		deletionSource != "RETENTION_ARTIFACT" || grantRevokedAt == nil ||
		deletedArtifacts != len(artifactIDs) || requestCount != 1 || receiptCount != 1 {
		t.Fatalf(
			"automatic Artifact evidence = content %s contentDeleted %v deletion %s source %s revoked %v artifacts %d requests %d receipts %d",
			requestContent,
			requestContentDeletedAt,
			deletionState,
			deletionSource,
			grantRevokedAt,
			deletedArtifacts,
			requestCount,
			receiptCount,
		)
	}
	replayed, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || replayed.RequestContentExpired != 0 ||
		replayed.ArtifactRequestsCreated != 0 || replayed.Claimed != 0 {
		t.Fatalf("replayed automatic Artifact expiry = %#v error=%v", replayed, err)
	}
}

func TestNinetyDayArtifactRetentionDoesNotExtendRequestContentRetention(t *testing.T) {
	fixture := newStartFixtureWithRetention(t, "automatic-request-expiry-90d", 90, 43)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
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
		t.Fatalf("CompleteVisibleCompletion = %#v error=%v", completed, err)
	}
	var (
		policyRevisionID                           uuid.UUID
		policyStableID, artifactSetPolicyRevision  string
		artifactRetentionDays, artifactCount       int
		setExpiry, artifactMinExpiry, accessExpiry time.Time
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT job.retention_policy_revision_id,
			policy.stable_id,
			job.retention_artifact_days,
			artifact_set.retention_policy_revision,
			artifact_set.retention_expires_at,
			min(artifact.retention_expires_at),
			count(artifact.id),
			access_grant.retention_expires_at
		FROM jobs AS job
		JOIN retention_policy_revisions AS policy
		  ON policy.id = job.retention_policy_revision_id
		JOIN artifact_sets AS artifact_set
		  ON artifact_set.job_id = job.id
		JOIN artifacts AS artifact
		  ON artifact.job_id = job.id
		JOIN artifact_access_grants AS access_grant
		  ON access_grant.artifact_set_id = artifact_set.id
		WHERE job.id = $1
		  AND artifact_set.id = $2
		GROUP BY job.id, policy.id, artifact_set.id, access_grant.id
	`, fixture.candidate.JobID, completed.ArtifactSetID).Scan(
		&policyRevisionID,
		&policyStableID,
		&artifactRetentionDays,
		&artifactSetPolicyRevision,
		&setExpiry,
		&artifactMinExpiry,
		&artifactCount,
		&accessExpiry,
	); err != nil {
		t.Fatalf("read 90-day Admission and Visible Completion retention snapshot: %v", err)
	}
	wantArtifactExpiry := completed.CompletedAt.Add(90 * 24 * time.Hour)
	if policyRevisionID == uuid.Nil || policyStableID != "artifact-90d-v1" ||
		artifactRetentionDays != 90 || artifactSetPolicyRevision != policyStableID ||
		artifactCount != len(artifactIDs) || !setExpiry.Equal(wantArtifactExpiry) ||
		!artifactMinExpiry.Equal(wantArtifactExpiry) || !accessExpiry.Equal(wantArtifactExpiry) {
		t.Fatalf(
			"90-day snapshot = policy %s/%s days %d setPolicy %s set %s artifact %s access %s count %d, want +90d %s",
			policyRevisionID,
			policyStableID,
			artifactRetentionDays,
			artifactSetPolicyRevision,
			setExpiry,
			artifactMinExpiry,
			accessExpiry,
			artifactCount,
			wantArtifactExpiry,
		)
	}
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin 90-day request-expiry time setup: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`SET LOCAL session_replication_role = 'replica'`); err != nil {
		t.Fatalf("disable immutable triggers for 90-day time setup: %v", err)
	}
	if _, err := tx.Exec(`
		UPDATE jobs
		SET created_at = clock_timestamp() - interval '40 days',
			job_expires_at = clock_timestamp() - interval '31 days',
			request_content_expires_at = clock_timestamp() - interval '1 day'
		WHERE id = $1
	`, fixture.candidate.JobID); err != nil {
		t.Fatalf("move 90-day Job request deadline into past: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit 90-day request-expiry time setup: %v", err)
	}
	store := &recordingRetentionStore{}
	reconciler, err := retention.NewReconciler(
		newRolePool(
			t,
			fixture.database.DSN,
			"vela_retention_login",
			"vela-retention-password",
		),
		store,
		retention.ReconcilerConfig{
			InstanceID: "automatic-request-expiry-90d-reconciler",
			BatchSize:  10,
			ClaimTTL:   time.Minute,
			RetryDelay: time.Minute,
		},
	)
	if err != nil {
		t.Fatalf("create 90-day request-expiry Reconciler: %v", err)
	}
	result, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || result.RequestContentExpired != 1 ||
		result.ArtifactRequestsCreated != 0 || result.Claimed != 0 ||
		len(store.deleted) != 0 {
		t.Fatalf("90-day request-content expiry = %#v deletes=%#v error=%v", result, store.deleted, err)
	}
	var (
		requestContent                         string
		requestContentDeletedAt                *time.Time
		committedArtifacts, artifactRequests   int
		unrevokedGrants, requestExpiryReceipts int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT job.request_content::text, job.request_content_deleted_at,
			(SELECT count(*) FROM artifacts
			 WHERE job_id = job.id AND state = 'COMMITTED'),
			(SELECT count(*) FROM content_deletion_requests
			 WHERE job_id = job.id AND source = 'RETENTION_ARTIFACT'),
			(SELECT count(*) FROM artifact_access_grants
			 WHERE job_id = job.id AND revoked_at IS NULL),
			(SELECT count(*)
			 FROM content_deletion_receipts AS receipt
			 JOIN content_deletion_requests AS deletion ON deletion.id = receipt.request_id
			 WHERE deletion.job_id = job.id
			   AND deletion.source = 'RETENTION_REQUEST_CONTENT')
		FROM jobs AS job
		WHERE job.id = $1
	`, fixture.candidate.JobID).Scan(
		&requestContent,
		&requestContentDeletedAt,
		&committedArtifacts,
		&artifactRequests,
		&unrevokedGrants,
		&requestExpiryReceipts,
	); err != nil {
		t.Fatalf("read 90-day independent retention evidence: %v", err)
	}
	if requestContent != `{"deleted": true}` || requestContentDeletedAt == nil ||
		committedArtifacts != len(artifactIDs) || artifactRequests != 0 ||
		unrevokedGrants != 1 || requestExpiryReceipts != 1 {
		t.Fatalf(
			"90-day retention evidence = content %s deleted %v artifacts %d artifactRequests %d grants %d receipts %d",
			requestContent,
			requestContentDeletedAt,
			committedArtifacts,
			artifactRequests,
			unrevokedGrants,
			requestExpiryReceipts,
		)
	}
}

func TestContentDeletionExpiredClaimIsRecoveredByAnotherReconciler(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'content_deletion:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant Content Deletion scope: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	acceptedResult := submitJob(t, server.URL, "content-deletion-expired-claim", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"recover this deletion after a Reconciler crash"
	}`))
	if acceptedResult.StatusCode != http.StatusAccepted {
		t.Fatalf(
			"submit claim-recovery target status = %d; body=%s",
			acceptedResult.StatusCode,
			acceptedResult.Body,
		)
	}
	var accepted jobResponse
	if err := json.Unmarshal(acceptedResult.Body, &accepted); err != nil {
		t.Fatalf("decode claim-recovery target Job: %v", err)
	}
	principal, err := identity.NewAuthenticator(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate claim-recovery Principal: %v", err)
	}
	requestService, err := retention.NewService(newRolePool(
		t,
		database.DSN,
		"vela_retention_request_login",
		"vela-retention-request-password",
	))
	if err != nil {
		t.Fatalf("create claim-recovery request service: %v", err)
	}
	deletion, err := requestService.AcceptContentDeletion(
		context.Background(),
		principal,
		uuid.MustParse(testProjectID),
		uuid.MustParse(accepted.JobID),
		"content-deletion-expired-claim-key",
	)
	if err != nil {
		t.Fatalf("accept claim-recovery Content Deletion: %v", err)
	}
	store := newBlockingRetentionStore()
	newReconciler := func(instanceID string) *retention.Reconciler {
		t.Helper()
		reconciler, err := retention.NewReconciler(
			newRolePool(
				t,
				database.DSN,
				"vela_retention_login",
				"vela-retention-password",
			),
			store,
			retention.ReconcilerConfig{
				InstanceID: instanceID,
				BatchSize:  1,
				ClaimTTL:   time.Second,
				RetryDelay: time.Minute,
			},
		)
		if err != nil {
			t.Fatalf("create %s: %v", instanceID, err)
		}
		return reconciler
	}
	type reconcileOutcome struct {
		result retention.ReconcileResult
		err    error
	}
	firstDone := make(chan reconcileOutcome, 1)
	go func() {
		result, err := newReconciler("expired-claim-first").ReconcileBatch(
			context.Background(),
		)
		firstDone <- reconcileOutcome{result: result, err: err}
	}()
	select {
	case <-store.firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first Reconciler did not reach storage after claiming")
	}
	time.Sleep(1100 * time.Millisecond)
	secondResult, secondErr := newReconciler("expired-claim-second").ReconcileBatch(
		context.Background(),
	)
	if secondErr != nil || secondResult.Claimed != 1 || secondResult.Completed != 1 ||
		secondResult.Failed != 0 {
		t.Fatalf("claim recovery result = %#v error=%v", secondResult, secondErr)
	}
	close(store.releaseFirst)
	var first reconcileOutcome
	select {
	case first = <-firstDone:
	case <-time.After(5 * time.Second):
		t.Fatal("first Reconciler did not return after release")
	}
	if first.err == nil || !strings.Contains(first.err.Error(), "stale") ||
		first.result.Claimed != 1 || first.result.Completed != 0 || first.result.Failed != 1 {
		t.Fatalf("expired first claim result = %#v error=%v", first.result, first.err)
	}
	var requestState string
	var attemptCount, receiptCount int
	if err := database.Admin.QueryRow(`
		SELECT deletion.state::text, target.attempt_count,
			(SELECT count(*) FROM content_deletion_receipts WHERE request_id = deletion.id)
		FROM content_deletion_requests AS deletion
		JOIN content_deletion_targets AS target ON target.request_id = deletion.id
		WHERE deletion.id = $1
	`, deletion.RequestID).Scan(&requestState, &attemptCount, &receiptCount); err != nil {
		t.Fatalf("read recovered claim evidence: %v", err)
	}
	if requestState != "COMPLETED" || attemptCount != 2 || receiptCount != 1 ||
		store.callCount() != 2 {
		t.Fatalf(
			"recovered claim evidence = state %s attempts %d receipts %d calls %d",
			requestState,
			attemptCount,
			receiptCount,
			store.callCount(),
		)
	}
}

func TestRetentionMigrationAllowsEmptyDownUpAndRestoresNMinusOneSurface(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 15); err != nil {
		t.Fatalf("contract empty retention migration: %v", err)
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 15 {
		t.Fatalf("retention migration version after Down = %d error=%v", version, err)
	}

	var contracted bool
	if err := database.Admin.QueryRow(`
		SELECT
			to_regclass('public.retention_policy_revisions') IS NULL
			AND to_regclass('public.project_retention_policy_events') IS NULL
			AND to_regclass('public.content_deletion_requests') IS NULL
				AND to_regclass('public.content_deletion_targets') IS NULL
				AND to_regclass('public.content_deletion_receipts') IS NULL
				AND to_regclass('public.content_deletion_receipt_targets') IS NULL
			AND to_regprocedure('vela_get_project_retention_policy(uuid)') IS NULL
			AND to_regprocedure(
				'vela_accept_content_deletion_request(uuid,uuid,uuid,text,bytea,uuid,uuid,uuid,uuid,uuid,uuid,uuid)'
			) IS NULL
			AND to_regprocedure(
				'vela_complete_content_deletion_target(uuid,uuid,uuid,text,text)'
			) IS NULL
			AND to_regtype('public.content_deletion_source') IS NULL
			AND NOT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'public'
				  AND table_name IN ('projects', 'jobs')
				  AND column_name IN (
					'retention_policy_revision_id',
					'retention_artifact_days',
					'request_content_deleted_at'
				  )
			)
			AND NOT ('content_deletion:manage' = ANY(
				vela_project_role_scopes('ProjectAdmin'::project_role)
			))
			AND NOT ('retention_policy:manage' = ANY(
				vela_project_role_scopes('ProjectAdmin'::project_role)
			))
			AND NOT vela_service_credential_scopes_valid(
				ARRAY['content_deletion:manage']::text[]
			)
	`).Scan(&contracted); err != nil {
		t.Fatalf("inspect contracted retention surface: %v", err)
	}
	if !contracted {
		t.Fatal("retention migration Down left expanded schema or scope surface")
	}
	if err := veladb.VerifyRole(
		context.Background(),
		newRolePool(t, database.DSN, "vela_request_login", "vela-request-password"),
		veladb.RoleRequest,
	); err != nil {
		t.Fatalf("verify N-1 request role after retention Down: %v", err)
	}

	if err := goose.UpTo(database.Admin, migrations, 16); err != nil {
		t.Fatalf("re-expand retention migration: %v", err)
	}
	version, err = goose.GetDBVersion(database.Admin)
	if err != nil || version != 16 {
		t.Fatalf("retention migration version after Down/Up = %d error=%v", version, err)
	}
	for _, runtime := range []struct {
		name     string
		login    string
		password string
		role     veladb.Role
	}{
		{
			name: "request", login: "vela_retention_request_login",
			password: "vela-retention-request-password", role: veladb.RoleRetentionRequest,
		},
		{
			name: "Reconciler", login: "vela_retention_login",
			password: "vela-retention-password", role: veladb.RoleRetention,
		},
	} {
		if err := veladb.VerifyRole(
			context.Background(),
			newRolePool(t, database.DSN, runtime.login, runtime.password),
			runtime.role,
		); err != nil {
			t.Fatalf("verify re-expanded retention %s role: %v", runtime.name, err)
		}
	}
}

func TestIncompleteArtifactRetentionMigrationRestoresExact23And24Surface(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")

	assertVersion24Surface := func(label string) {
		t.Helper()
		var (
			enumLabels, owner, configuration    string
			functionExists, securityDefiner     bool
			retentionExecute, requestExecute    bool
			legacyFunctionExists, legacyExecute bool
			compatibilityExecute                bool
			updatedAtRead, versionRead          bool
			retentionHoursRead                  bool
		)
		if err := database.Admin.QueryRow(`
			SELECT
				(SELECT string_agg(value.enumlabel, ',' ORDER BY value.enumsortorder)
				 FROM pg_catalog.pg_enum AS value
				 JOIN pg_catalog.pg_type AS enum_type ON enum_type.oid = value.enumtypid
				 WHERE enum_type.typname = 'content_deletion_source'),
				procedure.oid IS NOT NULL,
				COALESCE(owner.rolname, ''),
				COALESCE(procedure.prosecdef, false),
				COALESCE(array_to_string(procedure.proconfig, ','), ''),
				pg_catalog.has_function_privilege(
					'vela_retention',
					'vela_enqueue_incomplete_artifact_deletions(integer)',
					'EXECUTE'
				),
				pg_catalog.has_function_privilege(
					'vela_request',
					'vela_enqueue_incomplete_artifact_deletions(integer)',
					'EXECUTE'
				),
				to_regprocedure(
					'vela_enqueue_expired_content_deletions_v23(integer)'
				) IS NOT NULL,
				pg_catalog.has_function_privilege(
					'vela_retention',
					'vela_enqueue_expired_content_deletions_v23(integer)',
					'EXECUTE'
				),
				pg_catalog.has_function_privilege(
					'vela_retention',
					'vela_enqueue_expired_content_deletions(integer)',
					'EXECUTE'
				),
				pg_catalog.has_column_privilege(
					'vela_retention_owner', 'jobs', 'updated_at', 'SELECT'
				),
				pg_catalog.has_column_privilege(
					'vela_retention_owner', 'jobs', 'version', 'SELECT'
				),
				pg_catalog.has_column_privilege(
					'vela_retention_owner', 'jobs',
					'retention_incomplete_content_hours', 'SELECT'
				)
			FROM (SELECT to_regprocedure(
				'vela_enqueue_incomplete_artifact_deletions(integer)'
			) AS oid) AS resolved
			LEFT JOIN pg_catalog.pg_proc AS procedure ON procedure.oid = resolved.oid
			LEFT JOIN pg_catalog.pg_roles AS owner ON owner.oid = procedure.proowner
		`).Scan(
			&enumLabels,
			&functionExists,
			&owner,
			&securityDefiner,
			&configuration,
			&retentionExecute,
			&requestExecute,
			&legacyFunctionExists,
			&legacyExecute,
			&compatibilityExecute,
			&updatedAtRead,
			&versionRead,
			&retentionHoursRead,
		); err != nil {
			t.Fatalf("inspect %s migration 24 surface: %v", label, err)
		}
		if enumLabels != "CUSTOMER,RETENTION_REQUEST_CONTENT,RETENTION_ARTIFACT,"+
			"RETENTION_INCOMPLETE_ARTIFACT" || !functionExists ||
			owner != "vela_retention_owner" || !securityDefiner ||
			configuration != "search_path=pg_catalog, public" ||
			retentionExecute || requestExecute || !legacyFunctionExists || legacyExecute ||
			!compatibilityExecute || updatedAtRead || !versionRead || !retentionHoursRead {
			t.Fatalf(
				"%s migration 24 surface = enum %q function %t owner %q security %t config %q helper execute %t/%t legacy %t/%t compatibility %t columns %t/%t/%t",
				label,
				enumLabels,
				functionExists,
				owner,
				securityDefiner,
				configuration,
				retentionExecute,
				requestExecute,
				legacyFunctionExists,
				legacyExecute,
				compatibilityExecute,
				updatedAtRead,
				versionRead,
				retentionHoursRead,
			)
		}
	}

	assertVersion24Surface("initial Up")
	if err := goose.DownTo(database.Admin, migrations, 23); err != nil {
		t.Fatalf("contract empty incomplete Artifact retention migration: %v", err)
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 23 {
		t.Fatalf("incomplete Artifact retention version after Down = %d error=%v", version, err)
	}
	var enumLabels string
	var (
		functionAbsent, legacyFunctionAbsent      bool
		compatibilityExists, compatibilityExecute bool
		updatedAtRead, versionRead                bool
		retentionHoursRead                        bool
	)
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT string_agg(value.enumlabel, ',' ORDER BY value.enumsortorder)
			 FROM pg_catalog.pg_enum AS value
			 JOIN pg_catalog.pg_type AS enum_type ON enum_type.oid = value.enumtypid
			 WHERE enum_type.typname = 'content_deletion_source'),
			to_regprocedure('vela_enqueue_incomplete_artifact_deletions(integer)') IS NULL,
			to_regprocedure(
				'vela_enqueue_expired_content_deletions_v23(integer)'
			) IS NULL,
			to_regprocedure(
				'vela_enqueue_expired_content_deletions(integer)'
			) IS NOT NULL,
			pg_catalog.has_function_privilege(
				'vela_retention',
				'vela_enqueue_expired_content_deletions(integer)',
				'EXECUTE'
			),
			pg_catalog.has_column_privilege(
				'vela_retention_owner', 'jobs', 'updated_at', 'SELECT'
			),
			pg_catalog.has_column_privilege(
				'vela_retention_owner', 'jobs', 'version', 'SELECT'
			),
			pg_catalog.has_column_privilege(
				'vela_retention_owner', 'jobs',
				'retention_incomplete_content_hours', 'SELECT'
			)
	`).Scan(
		&enumLabels,
		&functionAbsent,
		&legacyFunctionAbsent,
		&compatibilityExists,
		&compatibilityExecute,
		&updatedAtRead,
		&versionRead,
		&retentionHoursRead,
	); err != nil {
		t.Fatalf("inspect restored migration 23 surface: %v", err)
	}
	if enumLabels != "CUSTOMER,RETENTION_REQUEST_CONTENT,RETENTION_ARTIFACT" ||
		!functionAbsent || !legacyFunctionAbsent || !compatibilityExists ||
		!compatibilityExecute || updatedAtRead || versionRead || retentionHoursRead {
		t.Fatalf(
			"restored migration 23 surface = enum %q helper/legacy absent %t/%t compatibility %t/%t columns %t/%t/%t",
			enumLabels,
			functionAbsent,
			legacyFunctionAbsent,
			compatibilityExists,
			compatibilityExecute,
			updatedAtRead,
			versionRead,
			retentionHoursRead,
		)
	}
	var (
		typeOIDBefore, tableFileBefore, indexOIDBefore int64
		indexFileBefore, constraintOIDBefore           int64
	)
	if err := database.Admin.QueryRow(`
		SELECT
			enum_type.oid::bigint,
			request_table.relfilenode::bigint,
			customer_index.oid::bigint,
			customer_index.relfilenode::bigint,
			source_constraint.oid::bigint
		FROM pg_catalog.pg_type AS enum_type
		JOIN pg_catalog.pg_class AS request_table
		  ON request_table.oid = 'public.content_deletion_requests'::regclass
		JOIN pg_catalog.pg_class AS customer_index
		  ON customer_index.oid =
			 'public.content_deletion_requests_customer_idempotency_idx'::regclass
		JOIN pg_catalog.pg_constraint AS source_constraint
		  ON source_constraint.conrelid = request_table.oid
		 AND source_constraint.conname = 'content_deletion_requests_source_actor_check'
		WHERE enum_type.typname = 'content_deletion_source'
		  AND enum_type.typnamespace = 'public'::regnamespace
	`).Scan(
		&typeOIDBefore,
		&tableFileBefore,
		&indexOIDBefore,
		&indexFileBefore,
		&constraintOIDBefore,
	); err != nil {
		t.Fatalf("snapshot migration 23 additive identities: %v", err)
	}

	if err := goose.UpTo(database.Admin, migrations, 24); err != nil {
		t.Fatalf("re-expand incomplete Artifact retention migration: %v", err)
	}
	version, err = goose.GetDBVersion(database.Admin)
	if err != nil || version != 24 {
		t.Fatalf("incomplete Artifact retention version after Down/Up = %d error=%v", version, err)
	}
	assertVersion24Surface("replayed Up")
	var (
		typeOIDAfter, tableFileAfter, indexOIDAfter int64
		indexFileAfter, constraintOIDAfter          int64
	)
	if err := database.Admin.QueryRow(`
		SELECT
			enum_type.oid::bigint,
			request_table.relfilenode::bigint,
			customer_index.oid::bigint,
			customer_index.relfilenode::bigint,
			source_constraint.oid::bigint
		FROM pg_catalog.pg_type AS enum_type
		JOIN pg_catalog.pg_class AS request_table
		  ON request_table.oid = 'public.content_deletion_requests'::regclass
		JOIN pg_catalog.pg_class AS customer_index
		  ON customer_index.oid =
			 'public.content_deletion_requests_customer_idempotency_idx'::regclass
		JOIN pg_catalog.pg_constraint AS source_constraint
		  ON source_constraint.conrelid = request_table.oid
		 AND source_constraint.conname = 'content_deletion_requests_source_actor_check'
		WHERE enum_type.typname = 'content_deletion_source'
		  AND enum_type.typnamespace = 'public'::regnamespace
	`).Scan(
		&typeOIDAfter,
		&tableFileAfter,
		&indexOIDAfter,
		&indexFileAfter,
		&constraintOIDAfter,
	); err != nil {
		t.Fatalf("inspect migration 24 additive identities: %v", err)
	}
	if typeOIDAfter != typeOIDBefore || tableFileAfter != tableFileBefore ||
		indexOIDAfter != indexOIDBefore || indexFileAfter != indexFileBefore ||
		constraintOIDAfter != constraintOIDBefore {
		t.Fatalf(
			"migration 24 rewrote existing identities: type %d/%d table %d/%d index %d/%d file %d/%d constraint %d/%d",
			typeOIDBefore,
			typeOIDAfter,
			tableFileBefore,
			tableFileAfter,
			indexOIDBefore,
			indexOIDAfter,
			indexFileBefore,
			indexFileAfter,
			constraintOIDBefore,
			constraintOIDAfter,
		)
	}
}

func TestRetentionMigrationDownRefusesDurableEvidence(t *testing.T) {
	for _, test := range []struct {
		name string
		seed func(*testing.T, testDatabase)
	}{
		{
			name: "non-default Project policy",
			seed: func(t *testing.T, database testDatabase) {
				seedAdmissionFixture(t, database.Admin)
				if _, err := database.Admin.Exec(`
					UPDATE projects
					SET retention_policy_revision_id =
						'00000000-0000-0000-0000-000000001607'
					WHERE id = $1
				`, testProjectID); err != nil {
					t.Fatalf("select non-default retention policy evidence: %v", err)
				}
			},
		},
		{
			name: "Content Deletion request target and receipt",
			seed: func(t *testing.T, database testDatabase) {
				seedAdmissionFixture(t, database.Admin)
				server := admissionServerForDatabase(t, database)
				acceptedResult := submitJob(t, server.URL, "retention-migration-evidence", []byte(`{
					"model":"minimax-h3",
					"generation_preset":"balanced",
					"service_class":"standard",
					"output_spec":"video-1080p-5s-24fps",
					"generation_count":1,
					"prompt":"durable migration refusal evidence"
				}`))
				if acceptedResult.StatusCode != http.StatusAccepted {
					t.Fatalf(
						"submit migration evidence Job status = %d; body=%s",
						acceptedResult.StatusCode,
						acceptedResult.Body,
					)
				}
				var accepted jobResponse
				if err := json.Unmarshal(acceptedResult.Body, &accepted); err != nil {
					t.Fatalf("decode migration evidence Job: %v", err)
				}
				requestID := uuid.New()
				if _, err := database.Admin.Exec(`
					INSERT INTO content_deletion_requests (
						id, organization_id, project_id, job_id, source,
						state, requested_at, deadline_at, completed_at
					) VALUES (
						$1, $2, $3, $4, 'RETENTION_REQUEST_CONTENT',
						'COMPLETED', transaction_timestamp(),
						transaction_timestamp() + interval '24 hours', transaction_timestamp()
					)
				`,
					requestID,
					testOrganizationID,
					testProjectID,
					accepted.JobID,
				); err != nil {
					t.Fatalf("create durable Content Deletion request evidence: %v", err)
				}
				if _, err := database.Admin.Exec(`
					INSERT INTO content_deletion_receipts (
						id, organization_id, project_id, job_id, request_id,
						target_count, completed_target_count, total_attempt_count, completed_at
					) VALUES ($1, $3, $4, $5, $2, 0, 0, 0, transaction_timestamp())
				`,
					uuid.New(),
					requestID,
					testOrganizationID,
					testProjectID,
					accepted.JobID,
				); err != nil {
					t.Fatalf("create durable Content Deletion receipt evidence: %v", err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := newPostgres(t)
			applyFoundation(t, database.Admin)
			test.seed(t, database)

			migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
			err := goose.DownTo(database.Admin, migrations, 15)
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
				postgresError.ConstraintName != "retention_contract_requires_default_empty_evidence" {
				t.Fatalf("retention migration Down error = %v, want named SQLSTATE 55000", err)
			}
			version, versionErr := goose.GetDBVersion(database.Admin)
			if versionErr != nil || version != 16 {
				t.Fatalf("retention version after refused Down = %d error=%v", version, versionErr)
			}
			assertTableExists(t, database.Admin, "content_deletion_requests")
		})
	}
}

func TestRetentionMigrationDownSerializesConcurrentEvidenceBeforeContraction(t *testing.T) {
	for _, test := range []struct {
		name       string
		seedWriter func(*testing.T, testDatabase) *sql.Tx
	}{
		{
			name: "non-default Project policy",
			seedWriter: func(t *testing.T, database testDatabase) *sql.Tx {
				seedAdmissionFixture(t, database.Admin)
				tx, err := database.Admin.Begin()
				if err != nil {
					t.Fatalf("begin concurrent Retention Policy writer: %v", err)
				}
				if _, err := tx.Exec(`
					UPDATE projects AS project
					SET retention_policy_revision_id = policy.id,
						retention_artifact_days = policy.artifact_retention_days,
						retention_request_content_days = policy.request_content_retention_days,
						retention_incomplete_content_hours = policy.incomplete_content_retention_hours,
						retention_scratch_hours = policy.scratch_retention_hours,
						retention_debug_hours = policy.debug_retention_hours,
						retention_metadata_days = policy.metadata_retention_days,
						retention_financial_days = policy.financial_retention_days
					FROM retention_policy_revisions AS policy
					WHERE project.id = $1
					  AND policy.stable_id = 'artifact-7d-v1'
				`, testProjectID); err != nil {
					_ = tx.Rollback()
					t.Fatalf("write concurrent Retention Policy evidence: %v", err)
				}
				return tx
			},
		},
		{
			name: "Content Deletion request",
			seedWriter: func(t *testing.T, database testDatabase) *sql.Tx {
				seedAdmissionFixture(t, database.Admin)
				server := admissionServerForDatabase(t, database)
				acceptedResult := submitJob(t, server.URL, "concurrent-retention-down", []byte(`{
					"model":"minimax-h3",
					"generation_preset":"balanced",
					"service_class":"standard",
					"output_spec":"video-1080p-5s-24fps",
					"generation_count":1,
					"prompt":"serialize this Content Deletion evidence with migration Down"
				}`))
				if acceptedResult.StatusCode != http.StatusAccepted {
					t.Fatalf(
						"submit concurrent deletion evidence Job status = %d; body=%s",
						acceptedResult.StatusCode,
						acceptedResult.Body,
					)
				}
				var accepted jobResponse
				if err := json.Unmarshal(acceptedResult.Body, &accepted); err != nil {
					t.Fatalf("decode concurrent deletion evidence Job: %v", err)
				}
				tx, err := database.Admin.Begin()
				if err != nil {
					t.Fatalf("begin concurrent Content Deletion writer: %v", err)
				}
				requestedAt := time.Now().UTC()
				if _, err := tx.Exec(`
					INSERT INTO content_deletion_requests (
						id, organization_id, project_id, job_id, source,
						requested_at, deadline_at
					) VALUES (
						$1, $2, $3, $4, 'RETENTION_REQUEST_CONTENT',
						$5::timestamptz, $5::timestamptz + interval '24 hours'
					)
				`,
					uuid.New(),
					testOrganizationID,
					testProjectID,
					accepted.JobID,
					requestedAt,
				); err != nil {
					_ = tx.Rollback()
					t.Fatalf("write concurrent Content Deletion evidence: %v", err)
				}
				return tx
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := newPostgres(t)
			applyFoundation(t, database.Admin)
			writer := test.seedWriter(t, database)
			defer func() { _ = writer.Rollback() }()

			migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
			downErrors := make(chan error, 1)
			go func() {
				downErrors <- goose.DownTo(database.Admin, migrations, 15)
			}()
			waitForRoleDatabaseLock(t, database.Admin, "postgres")
			if err := writer.Commit(); err != nil {
				t.Fatalf("commit concurrent retention evidence: %v", err)
			}

			select {
			case downErr := <-downErrors:
				var postgresError *pgconn.PgError
				if !errors.As(downErr, &postgresError) || postgresError.Code != "55000" ||
					postgresError.ConstraintName != "retention_contract_requires_default_empty_evidence" {
					t.Fatalf(
						"concurrent retention migration Down error = %v, want named SQLSTATE 55000",
						downErr,
					)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("concurrent retention migration Down did not finish")
			}
			version, err := goose.GetDBVersion(database.Admin)
			if err != nil || version != 16 {
				t.Fatalf("retention version after concurrent refused Down = %d error=%v", version, err)
			}
			assertTableExists(t, database.Admin, "content_deletion_requests")
		})
	}
}

type recordingRetentionStore struct {
	deleted   []artifactstore.ObjectVersion
	multipart []artifactstore.IncompleteMultipartUpload
	aborted   []artifactstore.MultipartUpload
	resolved  map[string]artifactstore.ObjectVersion
	listErr   error
}

type blockingRetentionStore struct {
	firstStarted chan struct{}
	releaseFirst chan struct{}
	mu           sync.Mutex
	calls        int
}

type failFirstRetentionStore struct {
	delegate retention.DeletionStore
	failure  error
	mu       sync.Mutex
	failed   bool
}

func (store *failFirstRetentionStore) DeleteExactVersion(
	ctx context.Context,
	objectKey, versionID string,
) error {
	if err := store.takeFailure(); err != nil {
		return err
	}
	return store.delegate.DeleteExactVersion(ctx, objectKey, versionID)
}

func (store *failFirstRetentionStore) ResolveCurrentVersion(
	ctx context.Context,
	objectKey string,
) (artifactstore.ObjectVersion, bool, error) {
	if err := store.takeFailure(); err != nil {
		return artifactstore.ObjectVersion{}, false, err
	}
	return store.delegate.ResolveCurrentVersion(ctx, objectKey)
}

func (store *failFirstRetentionStore) ListIncompleteMultipartUploads(
	ctx context.Context,
	objectPrefix string,
) ([]artifactstore.IncompleteMultipartUpload, error) {
	if err := store.takeFailure(); err != nil {
		return nil, err
	}
	return store.delegate.ListIncompleteMultipartUploads(ctx, objectPrefix)
}

func (store *failFirstRetentionStore) AbortMultipartUpload(
	ctx context.Context,
	upload artifactstore.MultipartUpload,
) error {
	if err := store.takeFailure(); err != nil {
		return err
	}
	return store.delegate.AbortMultipartUpload(ctx, upload)
}

func (store *failFirstRetentionStore) takeFailure() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.failed {
		return nil
	}
	store.failed = true
	return store.failure
}

func newBlockingRetentionStore() *blockingRetentionStore {
	return &blockingRetentionStore{
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
}

func (store *blockingRetentionStore) DeleteExactVersion(
	context.Context,
	string,
	string,
) error {
	return nil
}

func (store *blockingRetentionStore) ResolveCurrentVersion(
	context.Context,
	string,
) (artifactstore.ObjectVersion, bool, error) {
	return artifactstore.ObjectVersion{}, false, nil
}

func (store *blockingRetentionStore) ListIncompleteMultipartUploads(
	_ context.Context,
	_ string,
) ([]artifactstore.IncompleteMultipartUpload, error) {
	store.mu.Lock()
	store.calls++
	call := store.calls
	store.mu.Unlock()
	if call == 1 {
		close(store.firstStarted)
		<-store.releaseFirst
	}
	return nil, nil
}

func (store *blockingRetentionStore) AbortMultipartUpload(
	context.Context,
	artifactstore.MultipartUpload,
) error {
	return nil
}

func (store *blockingRetentionStore) callCount() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.calls
}

func (store *recordingRetentionStore) DeleteExactVersion(
	_ context.Context,
	objectKey, versionID string,
) error {
	store.deleted = append(store.deleted, artifactstore.ObjectVersion{
		ObjectKey: objectKey,
		VersionID: versionID,
	})
	return nil
}

func (store *recordingRetentionStore) ResolveCurrentVersion(
	_ context.Context,
	objectKey string,
) (artifactstore.ObjectVersion, bool, error) {
	version, ok := store.resolved[objectKey]
	return version, ok, nil
}

func (store *recordingRetentionStore) ListIncompleteMultipartUploads(
	_ context.Context,
	objectPrefix string,
) ([]artifactstore.IncompleteMultipartUpload, error) {
	if store.listErr != nil {
		return nil, store.listErr
	}
	result := make([]artifactstore.IncompleteMultipartUpload, 0, len(store.multipart))
	for _, upload := range store.multipart {
		if bytes.HasPrefix([]byte(upload.ObjectKey), []byte(objectPrefix)) {
			result = append(result, upload)
		}
	}
	return result, nil
}

func (store *recordingRetentionStore) AbortMultipartUpload(
	_ context.Context,
	upload artifactstore.MultipartUpload,
) error {
	store.aborted = append(store.aborted, upload)
	return nil
}

func TestRetentionOrganizationScopedTablesForceRowLevelSecurity(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	for _, relation := range []string{
		"project_retention_policy_events",
		"content_deletion_requests",
		"content_deletion_targets",
		"content_deletion_receipts",
		"content_deletion_receipt_targets",
	} {
		var enabled, forced bool
		if err := database.Admin.QueryRow(`
			SELECT relrowsecurity, relforcerowsecurity
			FROM pg_catalog.pg_class
			WHERE oid = $1::regclass
		`, relation).Scan(&enabled, &forced); err != nil {
			t.Fatalf("inspect %s RLS: %v", relation, err)
		}
		if !enabled || !forced {
			t.Fatalf("%s RLS = enabled %t forced %t, want true/true", relation, enabled, forced)
		}
	}
}

func TestRetentionDatabaseFunctionResultContractsMatchCallers(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)

	for _, contract := range []struct {
		function string
		columns  string
	}{
		{
			function: "vela_set_project_retention_policy(uuid,integer,uuid)",
			columns: "project_id,policy_revision_id,stable_id,artifact_retention_days," +
				"request_content_retention_days,incomplete_content_retention_hours," +
				"scratch_retention_hours,debug_retention_hours,metadata_retention_days," +
				"financial_retention_days,selected_at",
		},
		{
			function: "vela_accept_content_deletion_request(uuid,uuid,uuid,text,bytea," +
				"uuid,uuid,uuid,uuid,uuid,uuid,uuid)",
			columns: "request_id,project_id,job_id,request_state,requested_at,deadline_at," +
				"completed_at,overdue,target_count,completed_target_count," +
				"retrying_target_count,last_error_code,last_error_message",
		},
		{
			function: "vela_claim_content_deletion_target(text,uuid,integer)",
			columns:  "target_id,target_action,object_key,object_version_id",
		},
		{
			function: "vela_complete_content_deletion_target(uuid,uuid,uuid,text,text)",
			columns:  "marked",
		},
	} {
		t.Run(contract.function, func(t *testing.T) {
			var columns string
			if err := database.Admin.QueryRow(`
				SELECT string_agg(argument.name, ',' ORDER BY argument.ordinality)
				FROM pg_catalog.pg_proc AS function
				CROSS JOIN LATERAL unnest(
					function.proargnames,
					function.proargmodes
				) WITH ORDINALITY AS argument(name, mode, ordinality)
				WHERE function.oid = $1::regprocedure
				  AND argument.mode IN ('o', 'b', 't')
			`, contract.function).Scan(&columns); err != nil {
				t.Fatalf("inspect %s result contract: %v", contract.function, err)
			}
			if columns != contract.columns {
				t.Fatalf("%s result columns = %q, want %q", contract.function, columns, contract.columns)
			}
		})
	}
}

func TestRetentionPolicyCatalogAndProjectDefault(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)

	rows, err := database.Admin.Query(`
		SELECT stable_id, artifact_retention_days, request_content_retention_days,
			incomplete_content_retention_hours, scratch_retention_hours,
			debug_retention_hours, metadata_retention_days,
			financial_retention_days, state
		FROM retention_policy_revisions
		ORDER BY artifact_retention_days
	`)
	if err != nil {
		t.Fatalf("read Retention Policy catalog: %v", err)
	}
	defer rows.Close()
	type policy struct {
		stableID                                   string
		artifactDays, requestDays, incompleteHours int
		scratchHours, debugHours, metadataDays     int
		financialDays                              int
		state                                      string
	}
	want := []policy{
		{"artifact-7d-v1", 7, 30, 24, 24, 72, 365, 2557, "ACTIVE"},
		{"artifact-30d-v1", 30, 30, 24, 24, 72, 365, 2557, "ACTIVE"},
		{"artifact-90d-v1", 90, 30, 24, 24, 72, 365, 2557, "ACTIVE"},
	}
	var got []policy
	for rows.Next() {
		var value policy
		if err := rows.Scan(
			&value.stableID, &value.artifactDays, &value.requestDays,
			&value.incompleteHours, &value.scratchHours, &value.debugHours,
			&value.metadataDays, &value.financialDays, &value.state,
		); err != nil {
			t.Fatalf("scan Retention Policy: %v", err)
		}
		got = append(got, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate Retention Policy catalog: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("Retention Policy count = %d, want %d: %#v", len(got), len(want), got)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("Retention Policy[%d] = %#v, want %#v", index, got[index], want[index])
		}
	}

	var projectPolicy string
	if err := database.Admin.QueryRow(`
		SELECT policy.stable_id
		FROM projects AS project
		JOIN retention_policy_revisions AS policy
		  ON policy.id = project.retention_policy_revision_id
		WHERE project.id = $1
	`, testProjectID).Scan(&projectPolicy); err != nil {
		t.Fatalf("read Project Retention Policy: %v", err)
	}
	if projectPolicy != "artifact-30d-v1" {
		t.Fatalf("Project Retention Policy = %q, want artifact-30d-v1", projectPolicy)
	}
}

func TestAdmissionLocksIndependentRequestAndArtifactRetention(t *testing.T) {
	server, admin := newAdmissionServer(t)
	selectProjectRetentionPolicyForFixture(t, admin, 7)
	result := submitJob(t, server.URL, "retention-snapshot-7d", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"content with independent request and Artifact retention"
	}`))
	if result.StatusCode != http.StatusAccepted {
		t.Fatalf("submit status = %d, want 202; body=%s", result.StatusCode, result.Body)
	}
	var accepted jobResponse
	if err := json.Unmarshal(result.Body, &accepted); err != nil {
		t.Fatalf("decode accepted Job: %v", err)
	}

	var (
		policyStableID                            string
		artifactDays, requestDays                 int
		incompleteHours, scratchHours, debugHours int
		metadataDays, financialDays               int
		requestContentExpiresAt, admissionAt      time.Time
	)
	if err := admin.QueryRow(`
		SELECT
			policy.stable_id,
			job.retention_artifact_days,
			job.retention_request_content_days,
			job.retention_incomplete_content_hours,
			job.retention_scratch_hours,
			job.retention_debug_hours,
			job.retention_metadata_days,
			job.retention_financial_days,
			job.request_content_expires_at,
			ready.occurred_at
		FROM jobs AS job
		JOIN retention_policy_revisions AS policy
		  ON policy.id = job.retention_policy_revision_id
		JOIN outbox_events AS ready
		  ON ready.aggregate_id = job.id
		 AND ready.aggregate_version = 1
		 AND ready.event_type = 'job.ready'
		WHERE job.id = $1
	`, accepted.JobID).Scan(
		&policyStableID, &artifactDays, &requestDays, &incompleteHours,
		&scratchHours, &debugHours, &metadataDays, &financialDays,
		&requestContentExpiresAt, &admissionAt,
	); err != nil {
		t.Fatalf("read Job Retention Policy snapshot: %v", err)
	}
	if policyStableID != "artifact-7d-v1" || artifactDays != 7 || requestDays != 30 ||
		incompleteHours != 24 || scratchHours != 24 || debugHours != 72 ||
		metadataDays != 365 || financialDays != 2557 {
		t.Fatalf(
			"Job Retention Policy snapshot = %s %d/%d/%d/%d/%d/%d/%d",
			policyStableID, artifactDays, requestDays, incompleteHours,
			scratchHours, debugHours, metadataDays, financialDays,
		)
	}
	if !requestContentExpiresAt.Equal(admissionAt.Add(30 * 24 * time.Hour)) {
		t.Fatalf(
			"request content expiry = %s, want Admission + 30 days (%s)",
			requestContentExpiresAt,
			admissionAt.Add(30*24*time.Hour),
		)
	}
}

func TestVisibleCompletionUsesJobArtifactRetentionSnapshot(t *testing.T) {
	fixture := newStartFixtureWithRetention(t, "visible-completion-retention-7d", 7, 17)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != workercontrol.StartGranted {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	plan, err := fixture.service.BeginFinalization(
		context.Background(), fixture.worker, fixture.credentials,
	)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted {
		t.Fatalf("BeginFinalization = %#v error=%v", plan, err)
	}
	service := visibleCompletionService(t, fixture.database.DSN)
	artifactIDs := uploadAndVerifyFinalizationPlan(
		t, service, fixture.worker, fixture.credentials, plan,
	)
	completed, err := service.CompleteVisibleCompletion(
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
		t.Fatalf("CompleteVisibleCompletion = %#v error=%v", completed, err)
	}

	var (
		policyRevision                             string
		setExpiry, artifactMinExpiry, accessExpiry time.Time
		artifactCount                              int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			artifact_set.retention_policy_revision,
			artifact_set.retention_expires_at,
			min(artifact.retention_expires_at),
			count(artifact.id),
			access_grant.retention_expires_at
		FROM artifact_sets AS artifact_set
		JOIN artifacts AS artifact ON artifact.job_id = artifact_set.job_id
		JOIN artifact_access_grants AS access_grant
		  ON access_grant.artifact_set_id = artifact_set.id
		WHERE artifact_set.id = $1
		GROUP BY artifact_set.id, access_grant.id
	`, completed.ArtifactSetID).Scan(
		&policyRevision, &setExpiry, &artifactMinExpiry, &artifactCount, &accessExpiry,
	); err != nil {
		t.Fatalf("read committed retention: %v", err)
	}
	wantExpiry := completed.CompletedAt.Add(7 * 24 * time.Hour)
	if policyRevision != "artifact-7d-v1" || artifactCount != len(artifactIDs) ||
		!setExpiry.Equal(wantExpiry) || !artifactMinExpiry.Equal(wantExpiry) ||
		!accessExpiry.Equal(wantExpiry) {
		t.Fatalf(
			"committed retention = policy %s set %s artifact %s access %s count %d, want %s",
			policyRevision, setExpiry, artifactMinExpiry, accessExpiry, artifactCount, wantExpiry,
		)
	}
}

func newStartFixtureWithRetention(
	t *testing.T,
	idempotencyKey string,
	artifactRetentionDays int,
	workerEpoch int64,
) startFixture {
	t.Helper()
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "retention integration fixture")
	seedAdmissionFixture(t, database.Admin)
	selectProjectRetentionPolicyForFixture(t, database.Admin, artifactRetentionDays)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, idempotencyKey, []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"verify the immutable Artifact retention snapshot"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Accepted Job: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/h3-retention-fixture', $2, 'READY', 'HEALTHY'
		)
	`, testWorkerID, workerEpoch); err != nil {
		t.Fatalf("seed READY Worker: %v", err)
	}
	internalPool := newRolePool(
		t, database.DSN, "vela_internal_login", "vela-internal-password",
	)
	service, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create Worker coordinator: %v", err)
	}
	assignment, err := service.Acquire(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: uuid.MustParse(testWorkerID)},
		workerEpoch,
		&workercontrol.AssignmentCandidate{
			JobID:                      uuid.MustParse(job.JobID),
			ExpectedJobVersion:         1,
			ExecutionProfileRevisionID: uuid.MustParse("00000000-0000-0000-0000-000000000014"),
		},
	)
	if err != nil {
		t.Fatalf("create Assignment: %v", err)
	}
	return startFixture{
		assignmentFixture: assignmentFixture{
			database: database,
			service:  service,
			worker:   workercontrol.AuthenticatedWorker{ID: uuid.MustParse(testWorkerID)},
			candidate: workercontrol.AssignmentCandidate{
				JobID:                      uuid.MustParse(job.JobID),
				ExpectedJobVersion:         1,
				ExecutionProfileRevisionID: uuid.MustParse("00000000-0000-0000-0000-000000000014"),
			},
		},
		assignment:  assignment,
		credentials: leaseCredentials(assignment),
	}
}

func selectProjectRetentionPolicyForFixture(
	t *testing.T,
	admin *sql.DB,
	artifactRetentionDays int,
) {
	t.Helper()
	result, err := admin.Exec(`
		UPDATE projects AS project
		SET retention_policy_revision_id = policy.id,
			retention_artifact_days = policy.artifact_retention_days,
			retention_request_content_days = policy.request_content_retention_days,
			retention_incomplete_content_hours = policy.incomplete_content_retention_hours,
			retention_scratch_hours = policy.scratch_retention_hours,
			retention_debug_hours = policy.debug_retention_hours,
			retention_metadata_days = policy.metadata_retention_days,
			retention_financial_days = policy.financial_retention_days
		FROM retention_policy_revisions AS policy
		WHERE project.id = $2
		  AND policy.artifact_retention_days = $1
		  AND policy.state = 'ACTIVE'
	`, artifactRetentionDays, testProjectID)
	if err != nil {
		t.Fatalf("select Project Retention Policy: %v", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("count selected Project Retention Policy rows: %v", err)
	}
	if rowsAffected != 1 {
		t.Fatalf("selected Project Retention Policy rows = %d, want 1", rowsAffected)
	}
}

func contentDeletionAuthority(
	t *testing.T,
	database testDatabase,
) (*retention.Service, identity.Principal) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY[
			'jobs:submit', 'jobs:read', 'jobs:cancel', 'content_deletion:manage'
		]
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant Customer Cancellation and Content Deletion scopes: %v", err)
	}
	principal, err := identity.NewAuthenticator(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate Content Deletion Principal: %v", err)
	}
	service, err := retention.NewService(newRolePool(
		t,
		database.DSN,
		"vela_retention_request_login",
		"vela-retention-request-password",
	))
	if err != nil {
		t.Fatalf("create Content Deletion request service: %v", err)
	}
	return service, principal
}

func assertRequestContentTombstoneImmutable(t *testing.T, admin *sql.DB, jobID uuid.UUID) {
	t.Helper()
	_, err := admin.Exec(`
		UPDATE jobs
		SET request_content = '{"prompt":"restored"}'::jsonb,
			request_content_deleted_at = NULL
		WHERE id = $1
	`, jobID)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "P0001" {
		t.Fatalf("restore request content error = %v, want immutable snapshot refusal", err)
	}
}
