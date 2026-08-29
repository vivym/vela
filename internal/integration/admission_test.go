//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/organizationreporting"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/scheduler"
)

const (
	testOrganizationID  = "00000000-0000-0000-0000-000000000001"
	testProjectID       = "00000000-0000-0000-0000-000000000002"
	testPrincipalID     = "00000000-0000-0000-0000-000000000003"
	testCredentialID    = "00000000-0000-0000-0000-000000000004"
	testProjectTwoID    = "00000000-0000-0000-0000-000000000102"
	testPrincipalTwoID  = "00000000-0000-0000-0000-000000000103"
	testCredentialTwoID = "00000000-0000-0000-0000-000000000104"
)

var testCredentialPepper = []byte("vela-integration-credential-pepper")

const testCredentialSecret = "0123456789abcdef0123456789abcdef"
const testCredentialTwoSecret = "abcdef0123456789abcdef0123456789"

const (
	testOtherOrganizationID = "00000000-0000-0000-0000-000000000201"
	testOtherProjectID      = "00000000-0000-0000-0000-000000000202"
	testOtherPrincipalID    = "00000000-0000-0000-0000-000000000203"
	testOtherCredentialID   = "00000000-0000-0000-0000-000000000204"
	testOtherSecret         = "fedcba9876543210fedcba9876543210"
)

func TestServicePrincipalCanSubmitAndGetAcceptedJob(t *testing.T) {
	server, admin := newAdmissionServer(t)

	requestBody := []byte(`{
        "model": "minimax-h3",
        "generation_preset": "balanced",
        "service_class": "standard",
        "output_spec": "video-1080p-5s-24fps",
        "generation_count": 2,
        "prompt": "a lighthouse in a winter storm"
    }`)
	req, err := http.NewRequest(
		http.MethodPost,
		server.URL+"/v1/projects/"+testProjectID+"/jobs",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		t.Fatalf("create submit request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testBearerCredential())
	req.Header.Set("Idempotency-Key", "admission-valid-1")

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		var body json.RawMessage
		_ = json.NewDecoder(response.Body).Decode(&body)
		t.Fatalf("submit status = %d, want 202; body=%s", response.StatusCode, body)
	}

	var submitted jobResponse
	if err := json.NewDecoder(response.Body).Decode(&submitted); err != nil {
		t.Fatalf("decode submit response: %v", err)
	}
	if submitted.ProjectID != testProjectID || submitted.State != "QUEUED" {
		t.Fatalf("submitted Job identity/state = %#v", submitted)
	}
	if submitted.Phase == nil || *submitted.Phase != "QUEUED" ||
		submitted.AttemptsStarted != 0 || submitted.PhaseProgress != nil ||
		submitted.NextRetryAt != nil || submitted.EstimatedFinishAt != nil ||
		submitted.ProgressUpdatedAt != nil {
		t.Fatalf("submitted Job execution view = %#v", submitted)
	}
	if submitted.Pricing.QuotedAmountMinor != 2500 || submitted.Pricing.Currency != "CNY" {
		t.Fatalf("submitted pricing = %#v, want 2500 CNY", submitted.Pricing)
	}

	getRequest, err := http.NewRequest(
		http.MethodGet,
		server.URL+"/v1/projects/"+testProjectID+"/jobs/"+submitted.JobID,
		nil,
	)
	if err != nil {
		t.Fatalf("create get request: %v", err)
	}
	getRequest.Header.Set("Authorization", "Bearer "+testBearerCredential())
	getResponse, err := http.DefaultClient.Do(getRequest)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	defer getResponse.Body.Close()
	if getResponse.StatusCode != http.StatusOK {
		t.Fatalf("get status = %d, want 200", getResponse.StatusCode)
	}
	var fetched jobResponse
	if err := json.NewDecoder(getResponse.Body).Decode(&fetched); err != nil {
		t.Fatalf("decode get response: %v", err)
	}
	if fetched.JobID != submitted.JobID || fetched.Pricing.QuotedAmountMinor != 2500 ||
		fetched.Phase == nil || *fetched.Phase != "QUEUED" || fetched.AttemptsStarted != 0 {
		t.Fatalf("fetched Job = %#v, want submitted Job", fetched)
	}
	var runtimeStates, reservations, events int
	var maxAttempts int
	var maxTotalComputeSeconds int64
	if err := admin.QueryRow(`
        SELECT
            (SELECT count(*) FROM retry_runtime_states WHERE job_id = $1),
            (SELECT count(*) FROM credit_reservations WHERE job_id = $1),
            (SELECT count(*) FROM outbox_events WHERE aggregate_id = $1),
            execution_max_attempts,
            execution_max_total_compute_seconds
        FROM jobs
        WHERE id = $1
    `, submitted.JobID).Scan(
		&runtimeStates,
		&reservations,
		&events,
		&maxAttempts,
		&maxTotalComputeSeconds,
	); err != nil {
		t.Fatalf("read Accepted Job facts: %v", err)
	}
	if runtimeStates != 1 || reservations != 1 || events != 1 || maxAttempts != 3 || maxTotalComputeSeconds != 2400 {
		t.Fatalf(
			"Accepted Job facts = runtime %d, reservations %d, events %d, attempts %d, compute %d",
			runtimeStates,
			reservations,
			events,
			maxAttempts,
			maxTotalComputeSeconds,
		)
	}
}

func TestInvalidSKUResolvesBeforeProjectAdmissionLock(t *testing.T) {
	server, admin := newAdmissionServer(t)
	lockTransaction, err := admin.Begin()
	if err != nil {
		t.Fatalf("begin Project lock transaction: %v", err)
	}
	defer func() { _ = lockTransaction.Rollback() }()
	if _, err := lockTransaction.Exec(
		"SELECT id FROM projects WHERE id = $1 FOR UPDATE",
		testProjectID,
	); err != nil {
		t.Fatalf("lock Project row: %v", err)
	}

	body := []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"not-an-active-output-spec",
		"generation_count":1,
		"prompt":"reject before waiting for the Project lock"
	}`)
	request, err := http.NewRequest(
		http.MethodPost,
		server.URL+"/v1/projects/"+testProjectID+"/jobs",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("create invalid SKU request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+testBearerCredential())
	request.Header.Set("Idempotency-Key", "resolve-before-project-lock")
	response, err := (&http.Client{Timeout: time.Second}).Do(request)
	if err != nil {
		t.Fatalf("invalid SKU waited for the Project lock: %v", err)
	}
	defer response.Body.Close()
	var responseError struct {
		Code string `json:"code"`
	}
	if err := json.NewDecoder(response.Body).Decode(&responseError); err != nil {
		t.Fatalf("decode invalid SKU response: %v", err)
	}
	if response.StatusCode != http.StatusBadRequest || responseError.Code != "invalid_sku" {
		t.Fatalf("invalid SKU response = %d %#v, want 400 invalid_sku", response.StatusCode, responseError)
	}
}

func TestJobExpiryIncludesQueueComputeAndFinalizationBudgets(t *testing.T) {
	server, _ := newAdmissionServer(t)
	result := submitJob(t, server.URL, "derived-job-expiry", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"derive the complete lifecycle ceiling"
	}`))
	if result.StatusCode != http.StatusAccepted {
		t.Fatalf("submit status = %d, want 202; body=%s", result.StatusCode, result.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(result.Body, &job); err != nil {
		t.Fatalf("decode Accepted Job: %v", err)
	}
	const expectedLifetime = 11400 * time.Second
	actualLifetime := job.JobExpiresAt.Sub(job.CreatedAt).Round(time.Second)
	if actualLifetime != expectedLifetime {
		t.Fatalf("Job lifetime = %s, want %s from queue, compute and finalization budgets", actualLifetime, expectedLifetime)
	}
}

func TestIdempotencyKeyReplaysOriginalJobAndRejectsDifferentRequest(t *testing.T) {
	server, admin := newAdmissionServer(t)
	body := []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":2,
        "prompt":"idempotent lighthouse"
    }`)

	first := submitJob(t, server.URL, "same-key", body)
	if first.StatusCode != http.StatusAccepted {
		t.Fatalf("first submit status = %d, want 202; body=%s", first.StatusCode, first.Body)
	}
	var firstJob jobResponse
	if err := json.Unmarshal(first.Body, &firstJob); err != nil {
		t.Fatalf("decode first submit: %v", err)
	}

	replay := submitJob(t, server.URL, "same-key", body)
	if replay.StatusCode != http.StatusAccepted {
		t.Fatalf("replay status = %d, want 202; body=%s", replay.StatusCode, replay.Body)
	}
	var replayedJob jobResponse
	if err := json.Unmarshal(replay.Body, &replayedJob); err != nil {
		t.Fatalf("decode replay: %v", err)
	}
	if replayedJob.JobID != firstJob.JobID {
		t.Fatalf("replay job_id = %s, want %s", replayedJob.JobID, firstJob.JobID)
	}

	differentBody := bytes.Replace(body, []byte("idempotent lighthouse"), []byte("different prompt"), 1)
	conflict := submitJob(t, server.URL, "same-key", differentBody)
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409; body=%s", conflict.StatusCode, conflict.Body)
	}
	var conflictError struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(conflict.Body, &conflictError); err != nil {
		t.Fatalf("decode conflict response: %v", err)
	}
	if conflictError.Code != "idempotency_conflict" {
		t.Fatalf("conflict code = %q, want idempotency_conflict", conflictError.Code)
	}

	var queuedCount int
	var reservedMinor int64
	if err := admin.QueryRow(`
        SELECT p.queued_count, a.reserved_minor
        FROM projects AS p
        JOIN organization_credit_accounts AS a ON a.organization_id = p.organization_id
        WHERE p.id = $1
    `, testProjectID).Scan(&queuedCount, &reservedMinor); err != nil {
		t.Fatalf("read Admission counters: %v", err)
	}
	if queuedCount != 1 || reservedMinor != 2500 {
		t.Fatalf("replay counters = queued %d, reserved %d; want 1 and 2500", queuedCount, reservedMinor)
	}
}

func TestIdempotencyDistinguishesLargeMetadataIntegers(t *testing.T) {
	server, _ := newAdmissionServer(t)
	const key = "large-metadata-number"
	first := submitJob(t, server.URL, key, []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"preserve the exact metadata number",
        "client_metadata":{"sequence":9007199254740992}
    }`))
	if first.StatusCode != http.StatusAccepted {
		t.Fatalf("first submit status = %d, want 202; body=%s", first.StatusCode, first.Body)
	}

	conflict := submitJob(t, server.URL, key, []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"preserve the exact metadata number",
        "client_metadata":{"sequence":9007199254740993}
    }`))
	if conflict.StatusCode != http.StatusConflict {
		t.Fatalf("large-number replay status = %d, want 409; body=%s", conflict.StatusCode, conflict.Body)
	}
}

func TestAdmissionRejectionsHaveNoDurableEffects(t *testing.T) {
	requestBody := []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":2,
        "prompt":"rejection must be side-effect free"
    }`)
	tests := []struct {
		name               string
		prepare            func(*testing.T, *sql.DB)
		wantStatus         int
		wantCode           string
		wantRetryAfter     bool
		retryAfterRecovery bool
	}{
		{
			name: "credit limit",
			prepare: func(t *testing.T, db *sql.DB) {
				t.Helper()
				if _, err := db.Exec(
					"UPDATE organization_credit_accounts SET contract_credit_limit_minor = 2499 WHERE organization_id = $1",
					testOrganizationID,
				); err != nil {
					t.Fatalf("lower credit limit: %v", err)
				}
			},
			wantStatus: http.StatusPaymentRequired,
			wantCode:   "credit_limit_exceeded",
		},
		{
			name: "Project queue bound",
			prepare: func(t *testing.T, db *sql.DB) {
				t.Helper()
				if _, err := db.Exec(
					"UPDATE projects SET queued_limit = 0 WHERE id = $1",
					testProjectID,
				); err != nil {
					t.Fatalf("close Project queue: %v", err)
				}
			},
			wantStatus:     http.StatusTooManyRequests,
			wantCode:       "project_limit_exceeded",
			wantRetryAfter: true,
		},
		{
			name: "Worker pool closed",
			prepare: func(t *testing.T, db *sql.DB) {
				t.Helper()
				if _, err := db.Exec("UPDATE worker_pools SET admission_open = false"); err != nil {
					t.Fatalf("close Worker pool: %v", err)
				}
			},
			wantStatus:         http.StatusServiceUnavailable,
			wantCode:           "capacity_unavailable",
			wantRetryAfter:     true,
			retryAfterRecovery: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, admin := newAdmissionServer(t)
			test.prepare(t, admin)
			result := submitJob(t, server.URL, "retryable-rejection", requestBody)
			if result.StatusCode != test.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", result.StatusCode, test.wantStatus, result.Body)
			}
			var responseError struct {
				Code string `json:"code"`
			}
			if err := json.Unmarshal(result.Body, &responseError); err != nil {
				t.Fatalf("decode rejection: %v", err)
			}
			if responseError.Code != test.wantCode {
				t.Fatalf("code = %q, want %q", responseError.Code, test.wantCode)
			}
			if test.wantRetryAfter && result.Header.Get("Retry-After") == "" {
				t.Fatal("Retry-After header is missing")
			}
			assertNoAdmissionEffects(t, admin)
			if test.retryAfterRecovery {
				if _, err := admin.Exec("UPDATE worker_pools SET admission_open = true"); err != nil {
					t.Fatalf("reopen Worker pool: %v", err)
				}
				recovered := submitJob(t, server.URL, "retryable-rejection", requestBody)
				if recovered.StatusCode != http.StatusAccepted {
					t.Fatalf("recovered status = %d, want 202; body=%s", recovered.StatusCode, recovered.Body)
				}
			}
		})
	}
}

func TestAdmissionPredictionRejectsExcessQueueWaitWithoutDurableEffects(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	predictor := &capacityPredictorStub{wait: 7201 * time.Second}

	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	artifactPool := newRolePool(
		t, database.DSN, "vela_artifact_request_login", "vela-artifact-request-password",
	)
	cancelPool := newRolePool(t, database.DSN, "vela_cancel_login", "vela-cancel-password")
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          identity.NewAuthenticator(authPool, testCredentialPepper),
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              &retention.Service{},
		Admission:              admission.NewService(requestPool, predictor),
		Cancellation:           cancellation.NewService(cancelPool, internalPool),
		Artifacts:              testArtifactAccessService(artifactPool),
		Webhooks:               testWebhookService(t, webhookRequestPoolForDatabase(t, database)),
	})
	if err != nil {
		t.Fatalf("create predicted Admission HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	requestBody := []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"prediction rejection must be side-effect free"
	}`)
	result := submitJob(t, server.URL, "predicted-capacity-rejection", requestBody)
	if result.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("predicted Admission status = %d, want 503; body=%s", result.StatusCode, result.Body)
	}
	var responseError struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(result.Body, &responseError); err != nil {
		t.Fatalf("decode predicted Admission rejection: %v", err)
	}
	if responseError.Code != "capacity_unavailable" || result.Header.Get("Retry-After") == "" {
		t.Fatalf(
			"predicted Admission response code=%q Retry-After=%q",
			responseError.Code,
			result.Header.Get("Retry-After"),
		)
	}
	if predictor.calls != 1 {
		t.Fatalf("capacity predictor calls = %d, want 1", predictor.calls)
	}
	assertNoAdmissionEffects(t, database.Admin)

	predictor.wait = 0
	accepted := submitJob(t, server.URL, "predicted-capacity-rejection", requestBody)
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("recovered predicted Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var acceptedJob jobResponse
	if err := json.Unmarshal(accepted.Body, &acceptedJob); err != nil {
		t.Fatalf("decode recovered predicted Admission: %v", err)
	}
	if predictor.calls != 2 {
		t.Fatalf("capacity predictor calls after recovery = %d, want 2", predictor.calls)
	}

	predictor.wait = 7201 * time.Second
	replayed := submitJob(t, server.URL, "predicted-capacity-rejection", requestBody)
	if replayed.StatusCode != http.StatusAccepted {
		t.Fatalf("idempotent predicted Admission replay status = %d, want 202; body=%s", replayed.StatusCode, replayed.Body)
	}
	var replayedJob jobResponse
	if err := json.Unmarshal(replayed.Body, &replayedJob); err != nil {
		t.Fatalf("decode idempotent predicted Admission replay: %v", err)
	}
	if replayedJob.JobID != acceptedJob.JobID || predictor.calls != 2 {
		t.Fatalf(
			"idempotent predicted Admission replay Job=%s calls=%d, want Job=%s calls=2",
			replayedJob.JobID,
			predictor.calls,
			acceptedJob.JobID,
		)
	}
}

func TestAdmissionProductionConstructorRequiresCapacityPredictor(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")

	_, err := admission.NewService(requestPool, nil).Submit(
		context.Background(),
		identity.Principal{},
		uuid.Nil,
		"missing-capacity-predictor",
		admission.Request{},
	)
	if err == nil || err.Error() != "admission capacity predictor is not configured" {
		t.Fatalf("missing production Capacity Predictor error = %v", err)
	}
	assertNoAdmissionEffects(t, database.Admin)
}

type capacityPredictorStub struct {
	wait      time.Duration
	finishAt  time.Time
	err       error
	jobFinish time.Time
	jobErr    error
	calls     int
	jobCalls  int
}

func (predictor *capacityPredictorStub) PredictCapacity(
	_ context.Context,
	_ admission.CapacityPredictionRequest,
) (admission.CapacityPrediction, error) {
	predictor.calls++
	finishAt := predictor.finishAt
	if finishAt.IsZero() {
		finishAt = time.Now().UTC().Add(predictor.wait + 20*time.Minute)
	}
	return admission.CapacityPrediction{
		QueueWait:         predictor.wait,
		EstimatedFinishAt: finishAt,
	}, predictor.err
}

func (predictor *capacityPredictorStub) PredictJobDynamicETA(
	_ context.Context,
	_ uuid.UUID,
) (time.Time, error) {
	predictor.jobCalls++
	if predictor.jobErr != nil {
		return time.Time{}, predictor.jobErr
	}
	if predictor.jobFinish.IsZero() {
		return time.Time{}, pgx.ErrNoRows
	}
	return predictor.jobFinish, nil
}

func TestOpenAPIValidationReturnsContractErrorJSON(t *testing.T) {
	server, admin := newAdmissionServer(t)
	result := submitJob(t, server.URL, "invalid-contract", []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1
    }`))
	if result.StatusCode != http.StatusBadRequest {
		t.Fatalf("schema validation status = %d, want 400; body=%s", result.StatusCode, result.Body)
	}
	if contentType := result.Header.Get("Content-Type"); !strings.HasPrefix(contentType, "application/json") {
		t.Fatalf("schema validation Content-Type = %q, want application/json", contentType)
	}
	var responseError struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(result.Body, &responseError); err != nil {
		t.Fatalf("decode schema validation error: %v; body=%s", err, result.Body)
	}
	if responseError.Code != "invalid_request" || responseError.Message == "" {
		t.Fatalf("schema validation error = %#v, want invalid_request with message", responseError)
	}
	assertNoAdmissionEffects(t, admin)
}

func TestConcurrentAdmissionsCannotExceedProjectBound(t *testing.T) {
	server, admin := newAdmissionServer(t)
	if _, err := admin.Exec(
		"UPDATE projects SET queued_limit = 2 WHERE id = $1",
		testProjectID,
	); err != nil {
		t.Fatalf("set Project queue bound: %v", err)
	}
	body := []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"concurrent Admission"
    }`)

	const requestCount = 12
	results := concurrentSubmissions(t, server.URL, "concurrent", requestCount, body)

	accepted := 0
	rejected := 0
	for _, result := range results {
		switch result.StatusCode {
		case http.StatusAccepted:
			accepted++
		case http.StatusTooManyRequests:
			rejected++
		default:
			t.Errorf("unexpected concurrent status %d; body=%s", result.StatusCode, result.Body)
		}
	}
	if accepted != 2 || rejected != requestCount-2 {
		t.Fatalf("concurrent results = %d accepted, %d rejected; want 2 and %d", accepted, rejected, requestCount-2)
	}

	var projectQueued, poolQueued, jobs int
	var reserved int64
	if err := admin.QueryRow(`
        SELECT
            (SELECT queued_count FROM projects WHERE id = $1),
            (SELECT queued_count FROM worker_pools WHERE stable_id = 'h3-primary'),
            (SELECT count(*) FROM jobs),
            (SELECT reserved_minor FROM organization_credit_accounts WHERE organization_id = $2)
    `, testProjectID, testOrganizationID).Scan(
		&projectQueued, &poolQueued, &jobs, &reserved,
	); err != nil {
		t.Fatalf("read concurrent Admission effects: %v", err)
	}
	if projectQueued != 2 || poolQueued != 2 || jobs != 2 || reserved != 2500 {
		t.Fatalf(
			"concurrent durable state = project %d, pool %d, jobs %d, reserved %d; want 2, 2, 2, 2500",
			projectQueued, poolQueued, jobs, reserved,
		)
	}
}

func TestConcurrentAdmissionsCannotExceedPoolBound(t *testing.T) {
	server, admin := newAdmissionServer(t)
	if _, err := admin.Exec(
		"UPDATE worker_pools SET queued_limit = 2 WHERE stable_id = 'h3-primary'",
	); err != nil {
		t.Fatalf("set Worker pool queue bound: %v", err)
	}
	body := []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"concurrent pool Admission"
    }`)

	const requestCount = 12
	results := concurrentSubmissions(t, server.URL, "pool-concurrent", requestCount, body)
	accepted := 0
	rejected := 0
	for _, result := range results {
		switch result.StatusCode {
		case http.StatusAccepted:
			accepted++
		case http.StatusServiceUnavailable:
			rejected++
			if result.Header.Get("Retry-After") == "" {
				t.Error("pool rejection is missing Retry-After")
			}
		default:
			t.Errorf("unexpected pool-concurrent status %d; body=%s", result.StatusCode, result.Body)
		}
	}
	if accepted != 2 || rejected != requestCount-2 {
		t.Fatalf("pool results = %d accepted, %d rejected; want 2 and %d", accepted, rejected, requestCount-2)
	}

	var projectQueued, poolQueued, jobs int
	var reserved int64
	if err := admin.QueryRow(`
        SELECT
            (SELECT queued_count FROM projects WHERE id = $1),
            (SELECT queued_count FROM worker_pools WHERE stable_id = 'h3-primary'),
            (SELECT count(*) FROM jobs),
            (SELECT reserved_minor FROM organization_credit_accounts WHERE organization_id = $2)
    `, testProjectID, testOrganizationID).Scan(
		&projectQueued, &poolQueued, &jobs, &reserved,
	); err != nil {
		t.Fatalf("read pool-concurrent Admission effects: %v", err)
	}
	if projectQueued != 2 || poolQueued != 2 || jobs != 2 || reserved != 2500 {
		t.Fatalf(
			"pool-concurrent state = project %d, pool %d, jobs %d, reserved %d; want 2, 2, 2, 2500",
			projectQueued,
			poolQueued,
			jobs,
			reserved,
		)
	}
}

func TestCompatiblePoolLockStabilizesCertificationUntilAdmissionCommits(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")

	requestTx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin request transaction: %v", err)
	}
	defer func() { _ = requestTx.Rollback(context.Background()) }()
	if _, err := requestTx.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testCredentialID,
		credentialDigest([]byte(testCredentialSecret)),
		identity.ScopeJobsSubmit,
	); err != nil {
		t.Fatalf("establish request context: %v", err)
	}
	lockedPool, err := selectCompatiblePool(context.Background(), requestTx)
	if err != nil {
		t.Fatalf("lock compatible Worker pool: %v", err)
	}
	if lockedPool.ID == uuid.Nil {
		t.Fatal("compatible Worker pool lock returned no pool")
	}

	mutationTx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin certification mutation: %v", err)
	}
	defer func() { _ = mutationTx.Rollback() }()
	if _, err := mutationTx.Exec("SET LOCAL lock_timeout = '100ms'"); err != nil {
		t.Fatalf("set certification lock timeout: %v", err)
	}
	_, err = mutationTx.Exec(`
        UPDATE profile_certifications
        SET state = 'INVALID', invalidated_at = clock_timestamp()
        WHERE id = '00000000-0000-0000-0000-000000000015'
    `)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "55P03" {
		t.Fatalf("concurrent certification mutation error = %v, want SQLSTATE 55P03", err)
	}
	if err := mutationTx.Rollback(); err != nil {
		t.Fatalf("roll back blocked certification mutation: %v", err)
	}
	if err := requestTx.Commit(context.Background()); err != nil {
		t.Fatalf("commit request transaction: %v", err)
	}
	if _, err := database.Admin.Exec(`
        UPDATE profile_certifications
        SET state = 'INVALID', invalidated_at = clock_timestamp()
        WHERE id = '00000000-0000-0000-0000-000000000015'
    `); err != nil {
		t.Fatalf("mutate certification after Admission commit: %v", err)
	}
}

func TestCompatiblePoolSelectionContinuesAfterWaitedCandidateFills(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedSecondProjectAndPool(t, database.Admin)
	if _, err := database.Admin.Exec("UPDATE worker_pools SET queued_limit = 1"); err != nil {
		t.Fatalf("set pool limits: %v", err)
	}
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")

	first, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin first pool-selection transaction: %v", err)
	}
	defer func() { _ = first.Rollback(context.Background()) }()
	if _, err := first.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testCredentialID,
		credentialDigest([]byte(testCredentialSecret)),
		identity.ScopeJobsSubmit,
	); err != nil {
		t.Fatalf("establish first pool-selection context: %v", err)
	}
	if _, err := selectCompatiblePool(context.Background(), first); err != nil {
		t.Fatalf("lock first compatible pool: %v", err)
	}

	type poolResult struct {
		pool compatiblePoolRow
		err  error
	}
	backendPID := make(chan int, 1)
	result := make(chan poolResult, 1)
	go func() {
		second, beginErr := requestPool.Begin(context.Background())
		if beginErr != nil {
			result <- poolResult{err: beginErr}
			return
		}
		defer func() { _ = second.Rollback(context.Background()) }()
		if _, contextErr := second.Exec(
			context.Background(),
			"SELECT * FROM vela_set_request_context($1, $2, $3)",
			testCredentialID,
			credentialDigest([]byte(testCredentialSecret)),
			identity.ScopeJobsSubmit,
		); contextErr != nil {
			result <- poolResult{err: contextErr}
			return
		}
		var pid int
		if queryErr := second.QueryRow(context.Background(), "SELECT pg_backend_pid()").Scan(&pid); queryErr != nil {
			result <- poolResult{err: queryErr}
			return
		}
		backendPID <- pid
		selected, queryErr := selectCompatiblePool(context.Background(), second)
		result <- poolResult{pool: selected, err: queryErr}
	}()

	pid := <-backendPID
	waitForDatabaseLock(t, database.Admin, pid)
	if _, err := first.Exec(context.Background(), `
        UPDATE worker_pools
        SET queued_count = queued_limit
        WHERE id = '00000000-0000-0000-0000-000000000005'
    `); err != nil {
		t.Fatalf("fill first compatible pool: %v", err)
	}
	if err := first.Commit(context.Background()); err != nil {
		t.Fatalf("commit first pool-selection transaction: %v", err)
	}

	secondResult := <-result
	if secondResult.err != nil {
		t.Fatalf("select next compatible pool: %v", secondResult.err)
	}
	if secondResult.pool.ID.String() != "00000000-0000-0000-0000-000000000105" {
		t.Fatalf("selected pool = %s, want available secondary pool", secondResult.pool.ID)
	}
}

type compatiblePoolRow struct {
	ID                uuid.UUID
	AdmissionOpen     bool
	QueuedCount       int32
	QueuedLimit       int32
	RetryAfterSeconds int32
}

func selectCompatiblePool(ctx context.Context, tx pgx.Tx) (compatiblePoolRow, error) {
	var pool compatiblePoolRow
	err := tx.QueryRow(ctx, `
        SELECT * FROM vela_lock_compatible_pool($1, $2, $3)
    `,
		uuid.MustParse("00000000-0000-0000-0000-000000000010"),
		uuid.MustParse("00000000-0000-0000-0000-000000000011"),
		uuid.MustParse("00000000-0000-0000-0000-000000000013"),
	).Scan(
		&pool.ID,
		&pool.AdmissionOpen,
		&pool.QueuedCount,
		&pool.QueuedLimit,
		&pool.RetryAfterSeconds,
	)
	return pool, err
}

func waitForDatabaseLock(t *testing.T, db *sql.DB, backendPID int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		if err := db.QueryRow(`
            SELECT COALESCE(wait_event_type = 'Lock', false)
            FROM pg_stat_activity
            WHERE pid = $1
        `, backendPID).Scan(&waiting); err != nil {
			t.Fatalf("inspect waiting database backend: %v", err)
		}
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("second pool-selection transaction did not wait for the first pool lock")
}

func TestConcurrentProjectsCannotExceedOrganizationCredit(t *testing.T) {
	server, admin := newAdmissionServer(t)
	seedSecondProjectAndPool(t, admin)
	if _, err := admin.Exec(
		"UPDATE organization_credit_accounts SET contract_credit_limit_minor = 5000 WHERE organization_id = $1",
		testOrganizationID,
	); err != nil {
		t.Fatalf("set Organization credit limit: %v", err)
	}
	body := []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":2,
        "prompt":"shared Organization credit"
    }`)
	type caller struct {
		projectID string
		token     string
	}
	callers := []caller{
		{projectID: testProjectID, token: testBearerCredential()},
		{projectID: testProjectTwoID, token: bearerCredential(testCredentialTwoID, testCredentialTwoSecret)},
	}

	const requestCount = 8
	results := make(chan httpResult, requestCount)
	errorsChannel := make(chan error, requestCount)
	var waitGroup sync.WaitGroup
	for index := range requestCount {
		selected := callers[index%len(callers)]
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			result, err := doSubmitJob(
				server.URL,
				selected.projectID,
				selected.token,
				fmt.Sprintf("shared-credit-%d", index),
				body,
			)
			if err != nil {
				errorsChannel <- err
				return
			}
			results <- result
		}()
	}
	waitGroup.Wait()
	close(results)
	close(errorsChannel)
	for err := range errorsChannel {
		t.Errorf("concurrent shared-credit submit: %v", err)
	}

	accepted := 0
	creditRejected := 0
	for result := range results {
		switch result.StatusCode {
		case http.StatusAccepted:
			accepted++
		case http.StatusPaymentRequired:
			creditRejected++
		default:
			t.Errorf("unexpected shared-credit status %d; body=%s", result.StatusCode, result.Body)
		}
	}
	if accepted != 2 || creditRejected != requestCount-2 {
		t.Fatalf(
			"shared-credit results = %d accepted, %d rejected; want 2 and %d",
			accepted,
			creditRejected,
			requestCount-2,
		)
	}

	var reserved int64
	var jobs, projectQueued, poolQueued int
	if err := admin.QueryRow(`
        SELECT
            (SELECT reserved_minor FROM organization_credit_accounts WHERE organization_id = $1),
            (SELECT count(*) FROM jobs),
            (SELECT sum(queued_count) FROM projects WHERE organization_id = $1),
            (SELECT sum(queued_count) FROM worker_pools)
    `, testOrganizationID).Scan(&reserved, &jobs, &projectQueued, &poolQueued); err != nil {
		t.Fatalf("read shared-credit durable state: %v", err)
	}
	if reserved != 5000 || jobs != 2 || projectQueued != 2 || poolQueued != 2 {
		t.Fatalf(
			"shared-credit state = reserved %d, jobs %d, Project queued %d, pool queued %d; want 5000, 2, 2, 2",
			reserved,
			jobs,
			projectQueued,
			poolQueued,
		)
	}
}

func TestOrganizationIsolationFailsClosedAcrossHTTPRLSAndForeignKeys(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedOtherOrganization(t, database.Admin)
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	artifactPool := newRolePool(
		t, database.DSN, "vela_artifact_request_login", "vela-artifact-request-password",
	)
	cancelPool := newRolePool(t, database.DSN, "vela_cancel_login", "vela-cancel-password")
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          identity.NewAuthenticator(authPool, testCredentialPepper),
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              &retention.Service{},
		Admission:              admission.NewLegacyService(requestPool),
		Cancellation:           cancellation.NewService(cancelPool, internalPool),
		Artifacts:              testArtifactAccessService(artifactPool),
		Webhooks:               testWebhookService(t, webhookRequestPoolForDatabase(t, database)),
	})
	if err != nil {
		t.Fatalf("create HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	body := []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"Organization B content"
    }`)
	otherResult, err := doSubmitJob(
		server.URL,
		testOtherProjectID,
		bearerCredential(testOtherCredentialID, testOtherSecret),
		"other-organization-job",
		body,
	)
	if err != nil {
		t.Fatalf("submit Organization B Job: %v", err)
	}
	if otherResult.StatusCode != http.StatusAccepted {
		t.Fatalf("Organization B submit status = %d; body=%s", otherResult.StatusCode, otherResult.Body)
	}
	var otherJob jobResponse
	if err := json.Unmarshal(otherResult.Body, &otherJob); err != nil {
		t.Fatalf("decode Organization B Job: %v", err)
	}

	getRequest, err := http.NewRequest(
		http.MethodGet,
		server.URL+"/v1/projects/"+testOtherProjectID+"/jobs/"+otherJob.JobID,
		nil,
	)
	if err != nil {
		t.Fatalf("create cross-Organization GET: %v", err)
	}
	getRequest.Header.Set("Authorization", "Bearer "+testBearerCredential())
	getResponse, err := http.DefaultClient.Do(getRequest)
	if err != nil {
		t.Fatalf("cross-Organization GET: %v", err)
	}
	defer getResponse.Body.Close()
	getBody, err := io.ReadAll(getResponse.Body)
	if err != nil {
		t.Fatalf("read cross-Organization GET response: %v", err)
	}
	wantGetBody := "{\"code\":\"not_found\",\"message\":\"Job is not visible in this Project\"}\n"
	if getResponse.StatusCode != http.StatusNotFound || string(getBody) != wantGetBody {
		t.Fatalf(
			"cross-Organization GET = status %d body %q, want 404/%q",
			getResponse.StatusCode,
			getBody,
			wantGetBody,
		)
	}

	tx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin RLS transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testCredentialID,
		credentialDigest([]byte(testCredentialSecret)),
		identity.ScopeJobsSubmit,
	); err != nil {
		t.Fatalf("establish Organization A RLS context: %v", err)
	}
	if _, err := tx.Exec(context.Background(), `
        SELECT
            set_config('vela.organization_id', $1, true),
            set_config('vela.project_id', $2, true),
            set_config('vela.principal_id', $3, true)
    `, testOtherOrganizationID, testOtherProjectID, testOtherPrincipalID); err != nil {
		t.Fatalf("write forged Organization B GUCs: %v", err)
	}
	var visibleProjects, visibleJobs int
	if err := tx.QueryRow(
		context.Background(),
		`SELECT
            (SELECT count(*) FROM projects WHERE id = $1),
            (SELECT count(*) FROM jobs WHERE id = $2)`,
		testProjectID,
		otherJob.JobID,
	).Scan(&visibleProjects, &visibleJobs); err != nil {
		t.Fatalf("query Organization B Job under Organization A RLS: %v", err)
	}
	if visibleProjects != 1 {
		t.Fatalf("Organization A request context exposed %d own Projects, want 1", visibleProjects)
	}
	if visibleJobs != 0 {
		t.Fatalf("Organization A request role can see %d Organization B Jobs", visibleJobs)
	}
	commandTag, err := tx.Exec(
		context.Background(),
		"UPDATE projects SET queued_count = queued_count WHERE id = $1",
		testOtherProjectID,
	)
	if err != nil {
		t.Fatalf("attempt cross-Organization Project update: %v", err)
	}
	if commandTag.RowsAffected() != 0 {
		t.Fatalf("cross-Organization Project update affected %d rows", commandTag.RowsAffected())
	}

	_, err = database.Admin.Exec(`
        INSERT INTO idempotency_results (
            organization_id, project_id, idempotency_key, request_hash, job_id
        ) VALUES ($1, $2, 'cross-organization-reference', decode(repeat('00', 32), 'hex'), $3)
    `, testOrganizationID, testProjectID, otherJob.JobID)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "23503" {
		t.Fatalf("cross-Organization composite FK error = %v, want SQLSTATE 23503", err)
	}
}

func TestJobAttributionIsBoundToAuthenticatedProjectServicePrincipal(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedSecondProjectAndPool(t, database.Admin)
	server := admissionServerForDatabase(t, database)

	accepted := submitJob(t, server.URL, "job-attribution-source", []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"bind immutable audit attribution"
    }`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("source Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var sourceJob jobResponse
	if err := json.Unmarshal(accepted.Body, &sourceJob); err != nil {
		t.Fatalf("decode source Job: %v", err)
	}

	const cloneJob = `
        INSERT INTO jobs (
            id, organization_id, project_id, created_by_principal_id, state, version,
            model_revision_id, generation_preset_revision_id, service_class_revision_id,
            output_spec_id, worker_pool_id, request_hash, request_content,
            request_content_expires_at, pricing_rate_card_revision_id, pricing_rate_line_id,
            pricing_unit_amount_minor, pricing_quantity, pricing_quoted_amount_minor,
            pricing_currency, execution_max_attempts, execution_max_total_compute_seconds,
            execution_max_finalization_seconds_per_attempt, execution_retry_backoff_policy,
            execution_retryable_failure_classes, execution_circuit_breaker_policy,
            job_expires_at, created_at, updated_at
        )
        SELECT
            $1, organization_id, project_id, $2, state, version,
            model_revision_id, generation_preset_revision_id, service_class_revision_id,
            output_spec_id, worker_pool_id, request_hash, request_content,
            request_content_expires_at, pricing_rate_card_revision_id, pricing_rate_line_id,
            pricing_unit_amount_minor, pricing_quantity, pricing_quoted_amount_minor,
            pricing_currency, execution_max_attempts, execution_max_total_compute_seconds,
            execution_max_finalization_seconds_per_attempt, execution_retry_backoff_policy,
            execution_retryable_failure_classes, execution_circuit_breaker_policy,
            job_expires_at, created_at, updated_at
        FROM jobs
        WHERE id = $3
    `

	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	requestTx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin forged-attribution request transaction: %v", err)
	}
	if _, err := requestTx.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testCredentialID,
		credentialDigest([]byte(testCredentialSecret)),
		identity.ScopeJobsSubmit,
	); err != nil {
		t.Fatalf("establish request context: %v", err)
	}
	_, err = requestTx.Exec(
		context.Background(),
		cloneJob,
		uuid.New(),
		testPrincipalTwoID,
		sourceJob.JobID,
	)
	_ = requestTx.Rollback(context.Background())
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
		t.Fatalf("forged request-role Job attribution error = %v, want SQLSTATE 42501", err)
	}

	_, err = database.Admin.Exec(
		cloneJob,
		uuid.New(),
		testPrincipalTwoID,
		sourceJob.JobID,
	)
	if !errors.As(err, &postgresError) || postgresError.Code != "23503" {
		t.Fatalf("cross-Project Service Principal Job FK error = %v, want SQLSTATE 23503", err)
	}
}

func TestRequestRoleCannotEstablishContextWithCallerControlledGUCs(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedOtherOrganization(t, database.Admin)
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")

	tx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin request transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(context.Background(), `
        SELECT
            set_config('vela.organization_id', $1, true),
            set_config('vela.project_id', $2, true),
            set_config('vela.principal_id', $3, true)
    `, testOtherOrganizationID, testOtherProjectID, testOtherPrincipalID); err != nil {
		t.Fatalf("write caller-controlled GUCs: %v", err)
	}

	var visibleProjects int
	if err := tx.QueryRow(
		context.Background(),
		"SELECT count(*) FROM projects WHERE id = $1",
		testOtherProjectID,
	).Scan(&visibleProjects); err != nil {
		t.Fatalf("query Project after writing caller-controlled GUCs: %v", err)
	}
	if visibleProjects != 0 {
		t.Fatalf("caller-controlled GUCs exposed %d Project rows, want 0", visibleProjects)
	}
}

func TestRequestContextRejectsWrongCredentialProof(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")

	tx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin request transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = tx.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testCredentialID,
		make([]byte, sha256.Size),
		identity.ScopeJobsSubmit,
	)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "28000" {
		t.Fatalf("wrong request context proof error = %v, want SQLSTATE 28000", err)
	}
}

func TestRequestContextCannotChangeIdentityWithinTransaction(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedOtherOrganization(t, database.Admin)
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")

	tx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin request transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testCredentialID,
		credentialDigest([]byte(testCredentialSecret)),
		identity.ScopeJobsSubmit,
	); err != nil {
		t.Fatalf("establish Organization A context: %v", err)
	}
	_, err = tx.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testOtherCredentialID,
		credentialDigest([]byte(testOtherSecret)),
		identity.ScopeJobsSubmit,
	)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "28000" {
		t.Fatalf("request context identity change error = %v, want SQLSTATE 28000", err)
	}
}

func TestRequestContextCannotChangeScopeWithinTransaction(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")

	tx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin request transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	credentialProof := credentialDigest([]byte(testCredentialSecret))
	if _, err := tx.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testCredentialID,
		credentialProof,
		identity.ScopeJobsSubmit,
	); err != nil {
		t.Fatalf("establish jobs:submit context: %v", err)
	}
	_, err = tx.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testCredentialID,
		credentialProof,
		identity.ScopeJobsRead,
	)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "28000" {
		t.Fatalf("request context scope change error = %v, want SQLSTATE 28000", err)
	}
}

func TestRequestContextRejectsInactiveCredentials(t *testing.T) {
	for _, test := range []struct {
		name     string
		mutation string
	}{
		{name: "revoked", mutation: "revoked_at = clock_timestamp(), expires_at = clock_timestamp() + interval '1 day'"},
		{name: "expired", mutation: "revoked_at = NULL, expires_at = clock_timestamp() - interval '1 second'"},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := newPostgres(t)
			applyFoundation(t, database.Admin)
			seedAdmissionFixture(t, database.Admin)
			requestPool := newRolePool(
				t,
				database.DSN,
				"vela_request_login",
				"vela-request-password",
			)
			if _, err := database.Admin.Exec(
				"UPDATE credentials SET "+test.mutation+" WHERE id = $1",
				testCredentialID,
			); err != nil {
				t.Fatalf("make credential %s: %v", test.name, err)
			}
			tx, err := requestPool.Begin(context.Background())
			if err != nil {
				t.Fatalf("begin request transaction: %v", err)
			}
			_, err = tx.Exec(
				context.Background(),
				"SELECT * FROM vela_set_request_context($1, $2, $3)",
				testCredentialID,
				credentialDigest([]byte(testCredentialSecret)),
				identity.ScopeJobsSubmit,
			)
			_ = tx.Rollback(context.Background())
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "28000" {
				t.Fatalf("%s request context error = %v, want SQLSTATE 28000", test.name, err)
			}
		})
	}
}

func TestScopeRemovalAfterAuthenticationFailsClosedAtRequestTransaction(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")

	principal, err := identity.NewAuthenticator(authPool, testCredentialPepper).Authenticate(
		context.Background(),
		testBearerCredential(),
	)
	if err != nil {
		t.Fatalf("authenticate credential before scope removal: %v", err)
	}
	if _, err := database.Admin.Exec(
		"UPDATE credentials SET scopes = ARRAY['jobs:read'] WHERE id = $1",
		testCredentialID,
	); err != nil {
		t.Fatalf("remove jobs:submit scope: %v", err)
	}

	_, err = admission.NewLegacyService(requestPool).Submit(
		context.Background(),
		principal,
		uuid.MustParse(testProjectID),
		"scope-removed-after-authentication",
		admission.Request{
			Model:            "minimax-h3",
			GenerationPreset: "balanced",
			ServiceClass:     "standard",
			OutputSpec:       "video-1080p-5s-24fps",
			GenerationCount:  1,
			Prompt:           "scope removal must fail closed",
		},
	)
	var admissionFailure *admission.Failure
	if !errors.As(err, &admissionFailure) || admissionFailure.Code != admission.FailureCodeForbidden {
		t.Fatalf("scope removal Admission error = %v, want forbidden", err)
	}
	assertNoAdmissionEffects(t, database.Admin)
}

func TestReadScopeRequestContextCannotMutateAdmissionState(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(
		"UPDATE credentials SET scopes = ARRAY['jobs:read'] WHERE id = $1",
		testCredentialID,
	); err != nil {
		t.Fatalf("limit credential to jobs:read: %v", err)
	}
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")

	tx, err := requestPool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin read-scope request transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testCredentialID,
		credentialDigest([]byte(testCredentialSecret)),
		identity.ScopeJobsRead,
	); err != nil {
		t.Fatalf("establish jobs:read request context: %v", err)
	}

	projectUpdate, err := tx.Exec(
		context.Background(),
		"UPDATE projects SET queued_count = queued_count + 1 WHERE id = $1",
		testProjectID,
	)
	if err != nil {
		t.Fatalf("attempt Project mutation with jobs:read context: %v", err)
	}
	if projectUpdate.RowsAffected() != 0 {
		t.Fatalf("jobs:read context updated %d Project rows, want 0", projectUpdate.RowsAffected())
	}

	poolUpdate, err := tx.Exec(
		context.Background(),
		"UPDATE worker_pools SET queued_count = queued_count + 1 WHERE stable_id = 'h3-primary'",
	)
	if err != nil {
		t.Fatalf("attempt Worker pool mutation with jobs:read context: %v", err)
	}
	if poolUpdate.RowsAffected() != 0 {
		t.Fatalf("jobs:read context updated %d Worker pool rows, want 0", poolUpdate.RowsAffected())
	}
}

func TestRequestContextCleansRowsForTerminatedBackends(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedOtherOrganization(t, database.Admin)

	staleConnection := newRoleConnection(
		t,
		database.DSN,
		"vela_request_login",
		"vela-request-password",
	)
	staleTransaction, err := staleConnection.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin stale request transaction: %v", err)
	}
	var staleBackendPID int32
	if err := staleTransaction.QueryRow(context.Background(), "SELECT pg_backend_pid()").Scan(
		&staleBackendPID,
	); err != nil {
		t.Fatalf("read stale request backend PID: %v", err)
	}
	if _, err := staleTransaction.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testCredentialID,
		credentialDigest([]byte(testCredentialSecret)),
		identity.ScopeJobsSubmit,
	); err != nil {
		t.Fatalf("establish stale request context: %v", err)
	}
	if err := staleTransaction.Commit(context.Background()); err != nil {
		t.Fatalf("commit stale request context: %v", err)
	}
	if err := staleConnection.Close(context.Background()); err != nil {
		t.Fatalf("close stale request backend: %v", err)
	}

	cleanupConnection := newRoleConnection(
		t,
		database.DSN,
		"vela_request_login",
		"vela-request-password",
	)
	cleanupTransaction, err := cleanupConnection.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin cleanup request transaction: %v", err)
	}
	var cleanupBackendPID int32
	if err := cleanupTransaction.QueryRow(context.Background(), "SELECT pg_backend_pid()").Scan(
		&cleanupBackendPID,
	); err != nil {
		t.Fatalf("read cleanup request backend PID: %v", err)
	}
	if _, err := cleanupTransaction.Exec(
		context.Background(),
		"SELECT * FROM vela_set_request_context($1, $2, $3)",
		testOtherCredentialID,
		credentialDigest([]byte(testOtherSecret)),
		identity.ScopeJobsSubmit,
	); err != nil {
		t.Fatalf("establish cleanup request context: %v", err)
	}
	if err := cleanupTransaction.Commit(context.Background()); err != nil {
		t.Fatalf("commit cleanup request context: %v", err)
	}

	if staleBackendPID == cleanupBackendPID {
		var credentialID string
		if err := database.Admin.QueryRow(
			"SELECT credential_id::text FROM vela_private.request_contexts WHERE backend_pid = $1",
			cleanupBackendPID,
		).Scan(&credentialID); err != nil {
			t.Fatalf("read reused backend request context: %v", err)
		}
		if credentialID != testOtherCredentialID {
			t.Fatalf("reused backend credential = %s, want %s", credentialID, testOtherCredentialID)
		}
		return
	}

	var staleRows int
	if err := database.Admin.QueryRow(
		"SELECT count(*) FROM vela_private.request_contexts WHERE backend_pid = $1",
		staleBackendPID,
	).Scan(&staleRows); err != nil {
		t.Fatalf("count stale request contexts: %v", err)
	}
	if staleRows != 0 {
		t.Fatalf("terminated backend retained %d request context rows, want 0", staleRows)
	}
}

func TestCredentialLookupScopeAndRevocationFailClosed(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	artifactPool := newRolePool(
		t, database.DSN, "vela_artifact_request_login", "vela-artifact-request-password",
	)
	cancelPool := newRolePool(t, database.DSN, "vela_cancel_login", "vela-cancel-password")
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")

	var credentialCount int
	err := authPool.QueryRow(context.Background(), "SELECT count(*) FROM credentials").Scan(&credentialCount)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "42501" {
		t.Fatalf("auth role direct credential read error = %v, want SQLSTATE 42501", err)
	}

	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          identity.NewAuthenticator(authPool, testCredentialPepper),
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              &retention.Service{},
		Admission:              admission.NewLegacyService(requestPool),
		Cancellation:           cancellation.NewService(cancelPool, internalPool),
		Artifacts:              testArtifactAccessService(artifactPool),
		Webhooks:               testWebhookService(t, webhookRequestPoolForDatabase(t, database)),
	})
	if err != nil {
		t.Fatalf("create HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	body := []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"credential scope"
    }`)

	if _, err := database.Admin.Exec(
		"UPDATE credentials SET scopes = ARRAY['jobs:read'] WHERE id = $1",
		testCredentialID,
	); err != nil {
		t.Fatalf("remove submit scope: %v", err)
	}
	forbidden := submitJob(t, server.URL, "missing-scope", body)
	if forbidden.StatusCode != http.StatusForbidden {
		t.Fatalf("missing scope status = %d, want 403; body=%s", forbidden.StatusCode, forbidden.Body)
	}
	assertNoAdmissionEffects(t, database.Admin)

	if _, err := database.Admin.Exec(
		"UPDATE credentials SET scopes = ARRAY['jobs:submit', 'jobs:read'], revoked_at = clock_timestamp() WHERE id = $1",
		testCredentialID,
	); err != nil {
		t.Fatalf("revoke credential: %v", err)
	}
	revoked := submitJob(t, server.URL, "revoked", body)
	if revoked.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked credential status = %d, want 401; body=%s", revoked.StatusCode, revoked.Body)
	}
	assertNoAdmissionEffects(t, database.Admin)
}

type jobResponse struct {
	JobID             string     `json:"job_id"`
	ProjectID         string     `json:"project_id"`
	State             string     `json:"state"`
	Phase             *string    `json:"phase"`
	PhaseProgress     *float64   `json:"phase_progress"`
	AttemptsStarted   int        `json:"attempts_started"`
	NextRetryAt       *time.Time `json:"next_retry_at"`
	EstimatedFinishAt *time.Time `json:"estimated_finish_at"`
	ProgressUpdatedAt *time.Time `json:"progress_updated_at"`
	JobExpiresAt      time.Time  `json:"job_expires_at"`
	CreatedAt         time.Time  `json:"created_at"`
	Pricing           struct {
		QuotedAmountMinor int64  `json:"quoted_amount_minor"`
		Currency          string `json:"currency"`
	} `json:"pricing"`
}

type httpResult struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

func newAdmissionServer(t *testing.T) (*httptest.Server, *sql.DB) {
	t.Helper()
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	return admissionServerForDatabase(t, database), database.Admin
}

func admissionServerForDatabase(t *testing.T, database testDatabase) *httptest.Server {
	return admissionServerForDatabaseWithPredictor(t, database, nil)
}

func schedulerAdmissionServerForDatabase(t *testing.T, database testDatabase) *httptest.Server {
	t.Helper()
	schedulerPool := newRolePool(
		t, database.DSN, "vela_scheduler_login", "vela-scheduler-password",
	)
	predictor, err := scheduler.NewCapacityPredictor(schedulerPool)
	if err != nil {
		t.Fatalf("create Scheduler-aware Admission predictor: %v", err)
	}
	return admissionServerForDatabaseWithPredictor(t, database, predictor)
}

func admissionServerForDatabaseWithPredictor(
	t *testing.T,
	database testDatabase,
	predictor admission.CapacityPredictor,
) *httptest.Server {
	t.Helper()
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	artifactPool := newRolePool(
		t, database.DSN, "vela_artifact_request_login", "vela-artifact-request-password",
	)
	cancelPool := newRolePool(t, database.DSN, "vela_cancel_login", "vela-cancel-password")
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	admissionService := admission.NewLegacyService(requestPool)
	if predictor != nil {
		admissionService = admission.NewService(requestPool, predictor)
	}
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          identity.NewAuthenticator(authPool, testCredentialPepper),
		IdentityAdministration: &identity.AdministrationService{},
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              &retention.Service{},
		Admission:              admissionService,
		Cancellation:           cancellation.NewService(cancelPool, internalPool),
		Artifacts:              testArtifactAccessService(artifactPool),
		Webhooks:               testWebhookService(t, webhookRequestPoolForDatabase(t, database)),
	})
	if err != nil {
		t.Fatalf("create HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func submitJob(t *testing.T, serverURL, idempotencyKey string, body []byte) httpResult {
	t.Helper()
	result, err := doSubmitJob(
		serverURL,
		testProjectID,
		testBearerCredential(),
		idempotencyKey,
		body,
	)
	if err != nil {
		t.Fatalf("submit Job: %v", err)
	}
	return result
}

func doSubmitJob(serverURL, projectID, bearerCredential, idempotencyKey string, body []byte) (httpResult, error) {
	request, err := http.NewRequest(
		http.MethodPost,
		serverURL+"/v1/projects/"+projectID+"/jobs",
		bytes.NewReader(body),
	)
	if err != nil {
		return httpResult{}, fmt.Errorf("create submit request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+bearerCredential)
	request.Header.Set("Idempotency-Key", idempotencyKey)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return httpResult{}, fmt.Errorf("submit Job: %w", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return httpResult{}, fmt.Errorf("read submit response: %w", err)
	}
	return httpResult{StatusCode: response.StatusCode, Header: response.Header.Clone(), Body: responseBody}, nil
}

func concurrentSubmissions(
	t *testing.T,
	serverURL string,
	keyPrefix string,
	requestCount int,
	body []byte,
) []httpResult {
	t.Helper()
	resultsChannel := make(chan httpResult, requestCount)
	errorsChannel := make(chan error, requestCount)
	var waitGroup sync.WaitGroup
	for index := range requestCount {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			result, err := doSubmitJob(
				serverURL,
				testProjectID,
				testBearerCredential(),
				fmt.Sprintf("%s-%d", keyPrefix, index),
				body,
			)
			if err != nil {
				errorsChannel <- err
				return
			}
			resultsChannel <- result
		}()
	}
	waitGroup.Wait()
	close(resultsChannel)
	close(errorsChannel)
	for err := range errorsChannel {
		t.Errorf("concurrent submit: %v", err)
	}
	results := make([]httpResult, 0, requestCount)
	for result := range resultsChannel {
		results = append(results, result)
	}
	return results
}

func assertNoAdmissionEffects(t *testing.T, db *sql.DB) {
	t.Helper()
	var values [8]int64
	if err := db.QueryRow(`
        SELECT
            (SELECT queued_count FROM projects WHERE id = $1),
            (SELECT queued_count FROM worker_pools WHERE stable_id = 'h3-primary'),
            (SELECT reserved_minor FROM organization_credit_accounts WHERE organization_id = $2),
            (SELECT count(*) FROM jobs),
            (SELECT count(*) FROM retry_runtime_states),
            (SELECT count(*) FROM credit_reservations),
            (SELECT count(*) FROM idempotency_results),
            (SELECT count(*) FROM outbox_events)
    `, testProjectID, testOrganizationID).Scan(
		&values[0], &values[1], &values[2], &values[3],
		&values[4], &values[5], &values[6], &values[7],
	); err != nil {
		t.Fatalf("read rejected Admission effects: %v", err)
	}
	if values != [8]int64{} {
		t.Fatalf("rejected Admission left durable effects: %v", values)
	}
}

func applyFoundation(t *testing.T, db *sql.DB) {
	t.Helper()
	root := repositoryRoot(t)
	bootstrapSQL, err := os.ReadFile(filepath.Join(root, "db", "bootstrap", "roles.sql"))
	if err != nil {
		t.Fatalf("read role bootstrap: %v", err)
	}
	if _, err := db.Exec(string(bootstrapSQL)); err != nil {
		t.Fatalf("apply role bootstrap: %v", err)
	}
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatalf("set goose dialect: %v", err)
	}
	if err := goose.Up(db, filepath.Join(root, "db", "migrations")); err != nil {
		t.Fatalf("migrate foundation: %v", err)
	}

	if _, err := db.Exec(`
	        CREATE ROLE vela_auth_login LOGIN PASSWORD 'vela-auth-password' IN ROLE vela_auth;
			CREATE ROLE vela_human_auth_login LOGIN PASSWORD 'vela-human-auth-password' IN ROLE vela_human_auth;
			CREATE ROLE vela_human_membership_auth_login LOGIN PASSWORD 'vela-human-membership-auth-password' IN ROLE vela_human_membership_auth;
			CREATE ROLE vela_identity_request_login LOGIN PASSWORD 'vela-identity-request-password' IN ROLE vela_identity_request;
			CREATE ROLE vela_human_membership_request_login LOGIN PASSWORD 'vela-human-membership-request-password' IN ROLE vela_human_membership_request;
			CREATE ROLE vela_organization_billing_request_login LOGIN PASSWORD 'vela-organization-billing-request-password' IN ROLE vela_organization_billing_request;
				CREATE ROLE vela_organization_audit_request_login LOGIN PASSWORD 'vela-organization-audit-request-password' IN ROLE vela_organization_audit_request;
				CREATE ROLE vela_retention_request_login LOGIN PASSWORD 'vela-retention-request-password' IN ROLE vela_retention_request;
				CREATE ROLE vela_debug_dump_request_login LOGIN PASSWORD 'vela-debug-dump-request-password' IN ROLE vela_debug_dump_request;
				CREATE ROLE vela_debug_dump_audit_request_login LOGIN PASSWORD 'vela-debug-dump-audit-request-password' IN ROLE vela_debug_dump_audit_request;
					CREATE ROLE vela_retention_login LOGIN PASSWORD 'vela-retention-password' IN ROLE vela_retention;
					CREATE ROLE vela_backup_retention_login LOGIN PASSWORD 'vela-backup-retention-password' IN ROLE vela_backup_retention;
					CREATE ROLE vela_artifact_replication_login LOGIN PASSWORD 'vela-artifact-replication-password' IN ROLE vela_artifact_replication;
			CREATE ROLE vela_platform_operator_auth_login LOGIN PASSWORD 'vela-platform-operator-auth-password' IN ROLE vela_platform_operator_auth;
			CREATE ROLE vela_break_glass_request_login LOGIN PASSWORD 'vela-break-glass-request-password' IN ROLE vela_break_glass_request;
			CREATE ROLE vela_break_glass_audit_request_login LOGIN PASSWORD 'vela-break-glass-audit-request-password' IN ROLE vela_break_glass_audit_request;
			CREATE ROLE vela_request_login LOGIN PASSWORD 'vela-request-password' IN ROLE vela_request;
		CREATE ROLE vela_cancel_login LOGIN PASSWORD 'vela-cancel-password' IN ROLE vela_cancel;
				CREATE ROLE vela_artifact_request_login LOGIN PASSWORD 'vela-artifact-request-password' IN ROLE vela_artifact_request;
				CREATE ROLE vela_scheduler_login LOGIN PASSWORD 'vela-scheduler-password' IN ROLE vela_scheduler;
				CREATE ROLE vela_scheduler_inbox_login LOGIN PASSWORD 'vela-scheduler-inbox-password' IN ROLE vela_scheduler_inbox;
				CREATE ROLE vela_billing_login LOGIN PASSWORD 'vela-billing-password' IN ROLE vela_billing;
			CREATE ROLE vela_finance_reconciliation_login LOGIN PASSWORD 'vela-finance-reconciliation-password' IN ROLE vela_finance_reconciliation;
			CREATE ROLE vela_compliance_login LOGIN PASSWORD 'vela-compliance-password' IN ROLE vela_compliance;
			CREATE ROLE vela_non_content_expiry_login LOGIN PASSWORD 'vela-non-content-expiry-password' IN ROLE vela_non_content_expiry;
			CREATE ROLE vela_catalog_promotion_login LOGIN PASSWORD 'vela-catalog-promotion-password' IN ROLE vela_catalog_promotion;
			CREATE ROLE vela_stage_catalog_activation_login LOGIN PASSWORD 'vela-stage-catalog-activation-password' IN ROLE vela_stage_catalog_activation;
			CREATE ROLE vela_webhook_request_login LOGIN PASSWORD 'vela-webhook-request-password' IN ROLE vela_webhook_request;
			CREATE ROLE vela_webhook_login LOGIN PASSWORD 'vela-webhook-password' IN ROLE vela_webhook;
				CREATE ROLE vela_remediation_login LOGIN PASSWORD 'vela-remediation-password' IN ROLE vela_remediation;
				CREATE ROLE vela_fleet_login LOGIN PASSWORD 'vela-fleet-password' IN ROLE vela_fleet;
				CREATE ROLE vela_internal_login LOGIN PASSWORD 'vela-internal-password' BYPASSRLS IN ROLE vela_internal;
    `); err != nil {
		t.Fatalf("create application login roles: %v", err)
	}
}

func seedAdmissionFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	digest := credentialDigest([]byte(testCredentialSecret))
	if _, err := db.Exec(fmt.Sprintf(`
        INSERT INTO customer_organizations (id, display_name)
        VALUES ('%[1]s', 'Integration Organization');
        INSERT INTO projects (
            id, organization_id, display_name, queued_limit, running_limit
        ) VALUES ('%[2]s', '%[1]s', 'Integration Project', 10, 2);
        INSERT INTO principals (id, organization_id, kind, display_name)
        VALUES ('%[3]s', '%[1]s', 'SERVICE', 'Integration Service Principal');
        INSERT INTO service_principals (principal_id, organization_id, project_id)
        VALUES ('%[3]s', '%[1]s', '%[2]s');
        INSERT INTO credentials (
            id, organization_id, project_id, principal_id, secret_digest, scopes, expires_at,
            created_by_principal_id
        ) VALUES (
            '%[4]s', '%[1]s', '%[2]s', '%[3]s', decode('%[5]s', 'hex'),
            ARRAY['jobs:submit', 'jobs:read'], clock_timestamp() + interval '1 day', '%[3]s'
        );
        INSERT INTO organization_credit_accounts (
            organization_id, currency, contract_credit_limit_minor
        ) VALUES ('%[1]s', 'CNY', 100000);
        INSERT INTO worker_pools (id, stable_id, queued_limit)
        VALUES ('00000000-0000-0000-0000-000000000005', 'h3-primary', 10);
        INSERT INTO model_revisions (id, stable_id, revision, state, content_hash)
        VALUES (
            '00000000-0000-0000-0000-000000000010', 'minimax-h3', 1, 'ACTIVE',
            '0123456789abcdef0123456789abcdef'
        );
        INSERT INTO generation_preset_revisions (
            id, model_revision_id, stable_id, revision, state, certified_p95_compute_seconds
        ) VALUES (
            '00000000-0000-0000-0000-000000000011',
            '00000000-0000-0000-0000-000000000010',
            'balanced', 1, 'ACTIVE', 1200
        );
        INSERT INTO service_class_revisions (
			id, stable_id, revision, state, queue_retry_allowance_seconds, max_attempts,
            max_total_compute_multiplier_milli, max_finalization_seconds_per_attempt,
            retry_backoff_policy, retryable_failure_classes, circuit_breaker_policy
        ) VALUES (
            '00000000-0000-0000-0000-000000000012', 'standard', 1, 'ACTIVE', 7200, 3,
            2000, 600, '{"kind":"exponential","initial_seconds":30,"max_seconds":300}',
            ARRAY['WORKER_LOST', 'TRANSIENT_BACKEND'], '{"policy_revision":"h3-standard-v1"}'
        );
        INSERT INTO output_specs (
            id, stable_id, revision, state, width, height, duration_milliseconds,
            frame_rate_milli, codec
        ) VALUES (
            '00000000-0000-0000-0000-000000000013', 'video-1080p-5s-24fps', 1,
            'ACTIVE', 1920, 1080, 5000, 24000, 'h264'
        );
        INSERT INTO execution_profile_revisions (
            id, model_revision_id, worker_pool_id, stable_id, revision, state
        ) VALUES (
            '00000000-0000-0000-0000-000000000014',
            '00000000-0000-0000-0000-000000000010',
            '00000000-0000-0000-0000-000000000005', 'h3-balanced', 1, 'ACTIVE'
        );
        INSERT INTO profile_certifications (
            id, model_revision_id, generation_preset_revision_id, output_spec_id,
            execution_profile_revision_id, state, evidence_digest, certified_at
        ) VALUES (
            '00000000-0000-0000-0000-000000000015',
            '00000000-0000-0000-0000-000000000010',
            '00000000-0000-0000-0000-000000000011',
            '00000000-0000-0000-0000-000000000013',
            '00000000-0000-0000-0000-000000000014', 'ACTIVE',
            'abcdef0123456789abcdef0123456789', clock_timestamp()
        );
        INSERT INTO rate_card_revisions (id, revision, state, effective_at)
        VALUES (
            '00000000-0000-0000-0000-000000000016', 1, 'ACTIVE',
            clock_timestamp() - interval '1 hour'
        );
        INSERT INTO rate_card_lines (
            id, rate_card_revision_id, model_revision_id, generation_preset_revision_id,
            service_class_revision_id, output_spec_id, unit_amount_minor, currency
        ) VALUES (
            '00000000-0000-0000-0000-000000000017',
            '00000000-0000-0000-0000-000000000016',
            '00000000-0000-0000-0000-000000000010',
            '00000000-0000-0000-0000-000000000011',
            '00000000-0000-0000-0000-000000000012',
            '00000000-0000-0000-0000-000000000013', 1250, 'CNY'
        );
    `, testOrganizationID, testProjectID, testPrincipalID, testCredentialID, hex.EncodeToString(digest))); err != nil {
		t.Fatalf("seed Admission fixture: %v", err)
	}
}

func seedSecondProjectAndPool(t *testing.T, db *sql.DB) {
	t.Helper()
	digest := credentialDigest([]byte(testCredentialTwoSecret))
	if _, err := db.Exec(fmt.Sprintf(`
        INSERT INTO projects (
            id, organization_id, display_name, queued_limit, running_limit
        ) VALUES ('%[1]s', '%[2]s', 'Integration Project Two', 10, 2);
        INSERT INTO principals (id, organization_id, kind, display_name)
        VALUES ('%[3]s', '%[2]s', 'SERVICE', 'Integration Service Principal Two');
        INSERT INTO service_principals (principal_id, organization_id, project_id)
        VALUES ('%[3]s', '%[2]s', '%[1]s');
        INSERT INTO credentials (
            id, organization_id, project_id, principal_id, secret_digest, scopes, expires_at,
            created_by_principal_id
        ) VALUES (
            '%[4]s', '%[2]s', '%[1]s', '%[3]s', decode('%[5]s', 'hex'),
            ARRAY['jobs:submit', 'jobs:read'], clock_timestamp() + interval '1 day', '%[3]s'
        );
        INSERT INTO worker_pools (id, stable_id, queued_limit)
        VALUES ('00000000-0000-0000-0000-000000000105', 'h3-secondary', 10);
        INSERT INTO execution_profile_revisions (
            id, model_revision_id, worker_pool_id, stable_id, revision, state
        ) VALUES (
            '00000000-0000-0000-0000-000000000106',
            '00000000-0000-0000-0000-000000000010',
            '00000000-0000-0000-0000-000000000105', 'h3-balanced-secondary', 1, 'ACTIVE'
        );
        INSERT INTO profile_certifications (
            id, model_revision_id, generation_preset_revision_id, output_spec_id,
            execution_profile_revision_id, state, evidence_digest, certified_at
        ) VALUES (
            '00000000-0000-0000-0000-000000000107',
            '00000000-0000-0000-0000-000000000010',
            '00000000-0000-0000-0000-000000000011',
            '00000000-0000-0000-0000-000000000013',
            '00000000-0000-0000-0000-000000000106', 'ACTIVE',
            '123456789abcdef0123456789abcdef0', clock_timestamp()
        );
    `, testProjectTwoID, testOrganizationID, testPrincipalTwoID, testCredentialTwoID, hex.EncodeToString(digest))); err != nil {
		t.Fatalf("seed second Project and pool: %v", err)
	}
}

func seedOtherOrganization(t *testing.T, db *sql.DB) {
	t.Helper()
	digest := credentialDigest([]byte(testOtherSecret))
	if _, err := db.Exec(fmt.Sprintf(`
        INSERT INTO customer_organizations (id, display_name)
        VALUES ('%[1]s', 'Other Organization');
        INSERT INTO projects (
            id, organization_id, display_name, queued_limit, running_limit
        ) VALUES ('%[2]s', '%[1]s', 'Other Project', 10, 2);
        INSERT INTO principals (id, organization_id, kind, display_name)
        VALUES ('%[3]s', '%[1]s', 'SERVICE', 'Other Service Principal');
        INSERT INTO service_principals (principal_id, organization_id, project_id)
        VALUES ('%[3]s', '%[1]s', '%[2]s');
        INSERT INTO credentials (
            id, organization_id, project_id, principal_id, secret_digest, scopes, expires_at,
            created_by_principal_id
        ) VALUES (
            '%[4]s', '%[1]s', '%[2]s', '%[3]s', decode('%[5]s', 'hex'),
            ARRAY['jobs:submit', 'jobs:read'], clock_timestamp() + interval '1 day', '%[3]s'
        );
        INSERT INTO organization_credit_accounts (
            organization_id, currency, contract_credit_limit_minor
        ) VALUES ('%[1]s', 'CNY', 100000);
    `, testOtherOrganizationID, testOtherProjectID, testOtherPrincipalID, testOtherCredentialID, hex.EncodeToString(digest))); err != nil {
		t.Fatalf("seed other Organization: %v", err)
	}
}

func newRolePool(t *testing.T, adminDSN, username, password string) *pgxpool.Pool {
	t.Helper()
	dsn, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	dsn.User = url.UserPassword(username, password)
	pool, err := pgxpool.New(context.Background(), dsn.String())
	if err != nil {
		t.Fatalf("open PostgreSQL pool for %s: %v", username, err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("ping PostgreSQL as %s: %v", username, err)
	}
	return pool
}

func newRoleConnection(t *testing.T, adminDSN, username, password string) *pgx.Conn {
	t.Helper()
	dsn, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	dsn.User = url.UserPassword(username, password)
	connection, err := pgx.Connect(context.Background(), dsn.String())
	if err != nil {
		t.Fatalf("connect to PostgreSQL as %s: %v", username, err)
	}
	t.Cleanup(func() { _ = connection.Close(context.Background()) })
	return connection
}

func credentialDigest(secret []byte) []byte {
	digest := hmac.New(sha256.New, testCredentialPepper)
	_, _ = digest.Write(secret)
	return digest.Sum(nil)
}

func testBearerCredential() string {
	return bearerCredential(testCredentialID, testCredentialSecret)
}

func bearerCredential(credentialID, secret string) string {
	encodedSecret := base64.RawURLEncoding.EncodeToString([]byte(secret))
	return "vla_" + credentialID + "." + encodedSecret
}
