//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
		Admission:              admission.NewService(requestPool),
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
		Admission:              admission.NewService(requestPool),
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
		Admission:              admission.NewService(requestPool),
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

func newAdditionalMinIOStore(
	t *testing.T,
	fixture *minIOFixture,
	bucket string,
) *artifactstore.S3 {
	t.Helper()
	ctx := context.Background()
	if _, err := fixture.admin.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("create off-cluster backup bucket: %v", err)
	}
	if _, err := fixture.admin.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	}); err != nil {
		t.Fatalf("enable off-cluster backup versioning: %v", err)
	}
	config := fixture.config
	config.Bucket = bucket
	store, err := artifactstore.NewS3(config)
	if err != nil {
		t.Fatalf("create off-cluster backup store: %v", err)
	}
	if err := store.ValidateBucket(ctx); err != nil {
		t.Fatalf("validate off-cluster backup store: %v", err)
	}
	return store
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
		result.ContentDeletionRequestsCreated != 0 || result.Claimed != 0 {
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
		replayed.ContentDeletionRequestsCreated != 0 || replayed.Claimed != 0 {
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
		result.ContentDeletionRequestsCreated != 0 || result.Claimed != 0 {
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
	applyFoundationTo(t, database.Admin, 16)
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
	applyFoundationTo(t, database.Admin, 24)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 24 {
		t.Fatalf("initial incomplete Artifact retention version = %d error=%v", version, err)
	}

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
	version, err = goose.GetDBVersion(database.Admin)
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
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	for _, test := range []struct {
		name  string
		setup func(*testing.T) testDatabase
	}{
		{
			name: "non-default Project policy",
			setup: func(t *testing.T) testDatabase {
				database := newPostgres(t)
				applyFoundationTo(t, database.Admin, 16)
				seedAdmissionFixture(t, database.Admin)
				if _, err := database.Admin.Exec(`
					UPDATE projects
					SET retention_policy_revision_id =
						'00000000-0000-0000-0000-000000001607'
					WHERE id = $1
				`, testProjectID); err != nil {
					t.Fatalf("select non-default retention policy evidence: %v", err)
				}
				return database
			},
		},
		{
			name: "Content Deletion request target and receipt",
			setup: func(t *testing.T) testDatabase {
				fixture := newInvoiceExportMigrationFixture(t, "retention-migration-evidence")
				if err := goose.UpTo(fixture.database.Admin, migrations, 16); err != nil {
					t.Fatalf("expand historical retention evidence fixture: %v", err)
				}
				requestID := uuid.New()
				if _, err := fixture.database.Admin.Exec(`
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
					fixture.jobID,
				); err != nil {
					t.Fatalf("create durable Content Deletion request evidence: %v", err)
				}
				if _, err := fixture.database.Admin.Exec(`
					INSERT INTO content_deletion_receipts (
						id, organization_id, project_id, job_id, request_id,
						target_count, completed_target_count, total_attempt_count, completed_at
					) VALUES ($1, $3, $4, $5, $2, 0, 0, 0, transaction_timestamp())
				`,
					uuid.New(),
					requestID,
					testOrganizationID,
					testProjectID,
					fixture.jobID,
				); err != nil {
					t.Fatalf("create durable Content Deletion receipt evidence: %v", err)
				}
				return fixture.database
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := test.setup(t)

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
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	for _, test := range []struct {
		name        string
		setupWriter func(*testing.T) (testDatabase, *sql.Tx)
	}{
		{
			name: "non-default Project policy",
			setupWriter: func(t *testing.T) (testDatabase, *sql.Tx) {
				database := newPostgres(t)
				applyFoundationTo(t, database.Admin, 16)
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
				return database, tx
			},
		},
		{
			name: "Content Deletion request",
			setupWriter: func(t *testing.T) (testDatabase, *sql.Tx) {
				fixture := newInvoiceExportMigrationFixture(t, "concurrent-retention-down")
				if err := goose.UpTo(fixture.database.Admin, migrations, 16); err != nil {
					t.Fatalf("expand historical concurrent retention fixture: %v", err)
				}
				tx, err := fixture.database.Admin.Begin()
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
					fixture.jobID,
					requestedAt,
				); err != nil {
					_ = tx.Rollback()
					t.Fatalf("write concurrent Content Deletion evidence: %v", err)
				}
				return fixture.database, tx
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			database, writer := test.setupWriter(t)
			defer func() { _ = writer.Rollback() }()

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

type recordingBackupRetentionStore struct {
	purged []string
}

func (store *recordingBackupRetentionStore) PurgeObjectVersions(
	_ context.Context,
	objectKey string,
) (artifactstore.ObjectVersionsPurgeResult, error) {
	store.purged = append(store.purged, objectKey)
	return artifactstore.ObjectVersionsPurgeResult{PurgedVersionCount: 1}, nil
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
