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
	"net/netip"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/cancellation"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/webhook"
)

func TestWebhookHTTPCreateAndListUseExplicitScopes(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'webhooks:manage', 'webhooks:read']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant webhook API scopes: %v", err)
	}
	server := newWebhookHTTPServer(t, database)
	request, err := http.NewRequest(
		http.MethodPost,
		server.URL+"/v1/projects/"+testProjectID+"/webhook-subscriptions",
		strings.NewReader(`{
			"endpoint":"https://hooks.example.com/http-api",
			"event_types":["job.succeeded","job.failed"]
		}`),
	)
	if err != nil {
		t.Fatalf("create Webhook Subscription request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+testBearerCredential())
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("create Webhook Subscription: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatalf("read create Subscription response: %v", readErr)
	}
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201; body=%s", response.StatusCode, body)
	}
	var created struct {
		SubscriptionID string   `json:"subscription_id"`
		SigningSecret  string   `json:"signing_secret"`
		State          string   `json:"state"`
		EventTypes     []string `json:"event_types"`
	}
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("decode create Subscription response: %v", err)
	}
	if created.SubscriptionID == "" || !strings.HasPrefix(created.SigningSecret, "vwhsec_") ||
		created.State != "ACTIVE" || len(created.EventTypes) != 2 {
		t.Fatalf("create Subscription response = %#v", created)
	}

	listRequest, err := http.NewRequest(
		http.MethodGet,
		server.URL+"/v1/projects/"+testProjectID+"/webhook-subscriptions",
		nil,
	)
	if err != nil {
		t.Fatalf("create list Subscriptions request: %v", err)
	}
	listRequest.Header.Set("Authorization", "Bearer "+testBearerCredential())
	listResponse, err := http.DefaultClient.Do(listRequest)
	if err != nil {
		t.Fatalf("list Webhook Subscriptions: %v", err)
	}
	listBody, readErr := io.ReadAll(listResponse.Body)
	_ = listResponse.Body.Close()
	if readErr != nil {
		t.Fatalf("read list Subscriptions response: %v", readErr)
	}
	if listResponse.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d, want 200; body=%s", listResponse.StatusCode, listBody)
	}
	var listed struct {
		Subscriptions []struct {
			SubscriptionID string `json:"subscription_id"`
			Endpoint       string `json:"endpoint"`
		} `json:"subscriptions"`
	}
	if err := json.Unmarshal(listBody, &listed); err != nil {
		t.Fatalf("decode list Subscriptions response: %v", err)
	}
	if len(listed.Subscriptions) != 1 ||
		listed.Subscriptions[0].SubscriptionID != created.SubscriptionID ||
		listed.Subscriptions[0].Endpoint != "https://hooks.example.com/http-api" ||
		bytes.Contains(listBody, []byte("signing_secret")) {
		t.Fatalf("list Subscriptions response = %s", listBody)
	}

	if _, err := database.Admin.Exec(`
		UPDATE credentials SET scopes = ARRAY['webhooks:read'] WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("remove webhook management scope: %v", err)
	}
	request, err = http.NewRequest(
		http.MethodPost,
		server.URL+"/v1/projects/"+testProjectID+"/webhook-subscriptions",
		strings.NewReader(`{"endpoint":"https://hooks.example.com/forbidden","event_types":["job.failed"]}`),
	)
	if err != nil {
		t.Fatalf("create forbidden Subscription request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+testBearerCredential())
	request.Header.Set("Content-Type", "application/json")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("submit forbidden Subscription request: %v", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("manage without scope status = %d, want 403", response.StatusCode)
	}
}

func TestWebhookHTTPCreateRejectsEndpointResolvingToNonPublicAddress(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'webhooks:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant webhook management scope: %v", err)
	}
	server := newWebhookHTTPServerWithResolver(t, database, staticWebhookAddressResolver{
		addresses: []netip.Addr{netip.MustParseAddr("10.0.0.1")},
	})
	status, body := webhookJSONRequest(
		t,
		http.MethodPost,
		server.URL+"/v1/projects/"+testProjectID+"/webhook-subscriptions",
		`{"endpoint":"https://hooks.example.com/private","event_types":["job.failed"]}`,
	)
	if status != http.StatusBadRequest {
		t.Fatalf("create private-resolution Subscription status = %d, want 400; body=%s", status, body)
	}
}

func TestWebhookHTTPCreateRejectsZonedLinkLocalEndpoint(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'webhooks:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant webhook management scope: %v", err)
	}
	server := newWebhookHTTPServer(t, database)
	status, body := webhookJSONRequest(
		t,
		http.MethodPost,
		server.URL+"/v1/projects/"+testProjectID+"/webhook-subscriptions",
		`{"endpoint":"https://[fe80::1%25en0]/hook","event_types":["job.failed"]}`,
	)
	if status != http.StatusBadRequest {
		t.Fatalf("create zoned link-local Subscription status = %d, want 400; body=%s", status, body)
	}
}

func TestWebhookHTTPManagementCommandsAndDeliveryVisibility(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY[
			'jobs:submit', 'jobs:read', 'jobs:cancel',
			'webhooks:manage', 'webhooks:read'
		]
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant webhook workflow scopes: %v", err)
	}
	server := newWebhookHTTPServer(t, database)
	createStatus, createBody := webhookJSONRequest(
		t,
		http.MethodPost,
		server.URL+"/v1/projects/"+testProjectID+"/webhook-subscriptions",
		`{"endpoint":"https://hooks.example.com/workflow","event_types":["job.canceled"]}`,
	)
	if createStatus != http.StatusCreated {
		t.Fatalf("create status = %d, want 201; body=%s", createStatus, createBody)
	}
	var created struct {
		SubscriptionID string `json:"subscription_id"`
		SigningSecret  string `json:"signing_secret"`
	}
	if err := json.Unmarshal(createBody, &created); err != nil {
		t.Fatalf("decode created Subscription: %v", err)
	}

	rotateStatus, rotateBody := webhookJSONRequest(
		t,
		http.MethodPost,
		server.URL+"/v1/projects/"+testProjectID+"/webhook-subscriptions/"+
			created.SubscriptionID+"/rotate-secret",
		"",
	)
	if rotateStatus != http.StatusOK {
		t.Fatalf("rotate status = %d, want 200; body=%s", rotateStatus, rotateBody)
	}
	var rotated struct {
		SigningSecret            string    `json:"signing_secret"`
		SecretRevision           int32     `json:"secret_revision"`
		PreviousSecretValidUntil time.Time `json:"previous_secret_valid_until"`
	}
	if err := json.Unmarshal(rotateBody, &rotated); err != nil {
		t.Fatalf("decode rotated Subscription: %v", err)
	}
	if rotated.SecretRevision != 2 || rotated.SigningSecret == created.SigningSecret ||
		rotated.PreviousSecretValidUntil.IsZero() {
		t.Fatalf("rotate response = %#v", rotated)
	}

	accepted := submitJob(t, server.URL, "webhook-http-workflow", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"HTTP delivery visibility"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Accepted Job: %v", err)
	}
	canceled := cancelJob(t, server.URL, testProjectID, job.JobID, testBearerCredential())
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200; body=%s", canceled.StatusCode, canceled.Body)
	}

	deliveriesStatus, deliveriesBody := webhookJSONRequest(
		t,
		http.MethodGet,
		server.URL+"/v1/projects/"+testProjectID+"/webhook-subscriptions/"+
			created.SubscriptionID+"/deliveries",
		"",
	)
	if deliveriesStatus != http.StatusOK {
		t.Fatalf("deliveries status = %d, want 200; body=%s", deliveriesStatus, deliveriesBody)
	}
	var deliveries struct {
		Deliveries []struct {
			DeliveryID string `json:"delivery_id"`
			EventID    string `json:"event_id"`
			EventType  string `json:"event_type"`
			JobID      string `json:"job_id"`
			State      string `json:"state"`
		} `json:"deliveries"`
	}
	if err := json.Unmarshal(deliveriesBody, &deliveries); err != nil {
		t.Fatalf("decode Delivery list: %v", err)
	}
	if len(deliveries.Deliveries) != 1 || deliveries.Deliveries[0].DeliveryID == "" ||
		deliveries.Deliveries[0].EventID == "" ||
		deliveries.Deliveries[0].EventType != "job.canceled" ||
		deliveries.Deliveries[0].JobID != job.JobID ||
		deliveries.Deliveries[0].State != "PENDING" ||
		bytes.Contains(deliveriesBody, []byte("encrypted_secret")) ||
		bytes.Contains(deliveriesBody, []byte("payload")) {
		t.Fatalf("Delivery list = %s", deliveriesBody)
	}
	deliveryID := deliveries.Deliveries[0].DeliveryID
	if _, err := database.Admin.Exec(`
		UPDATE webhook_deliveries
		SET state = 'DEAD_LETTER', dead_lettered_at = clock_timestamp(),
			last_error = 'HTTP replay fixture'
		WHERE id = $1
	`, deliveryID); err != nil {
		t.Fatalf("prepare HTTP replay Delivery: %v", err)
	}
	replayStatus, replayBody := webhookJSONRequest(
		t,
		http.MethodPost,
		server.URL+"/v1/projects/"+testProjectID+"/webhook-subscriptions/"+
			created.SubscriptionID+"/deliveries/"+deliveryID+"/replay",
		"",
	)
	if replayStatus != http.StatusOK {
		t.Fatalf("replay status = %d, want 200; body=%s", replayStatus, replayBody)
	}
	var replayed struct {
		State      string `json:"state"`
		Generation int32  `json:"generation"`
		EventID    string `json:"event_id"`
	}
	if err := json.Unmarshal(replayBody, &replayed); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}
	if replayed.State != "PENDING" || replayed.Generation != 2 ||
		replayed.EventID != deliveries.Deliveries[0].EventID {
		t.Fatalf("replay response = %#v", replayed)
	}

	disableStatus, disableBody := webhookJSONRequest(
		t,
		http.MethodPost,
		server.URL+"/v1/projects/"+testProjectID+"/webhook-subscriptions/"+
			created.SubscriptionID+"/disable",
		"",
	)
	if disableStatus != http.StatusOK {
		t.Fatalf("disable status = %d, want 200; body=%s", disableStatus, disableBody)
	}
	var disabled struct {
		State      string     `json:"state"`
		DisabledAt *time.Time `json:"disabled_at"`
	}
	if err := json.Unmarshal(disableBody, &disabled); err != nil {
		t.Fatalf("decode disable response: %v", err)
	}
	if disabled.State != "DISABLED" || disabled.DisabledAt == nil {
		t.Fatalf("disable response = %#v", disabled)
	}
}

func TestWebhookHTTPReplayIdentifiesMissingDelivery(t *testing.T) {
	fixture := newWebhookDispatchFixture(
		t,
		webhook.EventJobFailed,
		"https://hooks.example.com/missing-delivery",
	)
	var subscriptionID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT subscription_id
		FROM webhook_deliveries
		WHERE event_id = $1
	`, fixture.eventID).Scan(&subscriptionID); err != nil {
		t.Fatalf("read Webhook Subscription id: %v", err)
	}
	server := newWebhookHTTPServer(t, fixture.database)
	status, body := webhookJSONRequest(
		t,
		http.MethodPost,
		server.URL+"/v1/projects/"+testProjectID+"/webhook-subscriptions/"+
			subscriptionID.String()+"/deliveries/"+uuid.NewString()+"/replay",
		"",
	)
	if status != http.StatusNotFound ||
		!bytes.Contains(body, []byte(`"message":"Webhook Delivery was not found"`)) {
		t.Fatalf("missing Delivery replay status = %d body=%s", status, body)
	}
}

func TestWebhookTerminalEventCreatesSafeProjectDelivery(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel', 'webhooks:manage']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant webhook and Job scopes: %v", err)
	}
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	webhookRequestPool := newRolePool(
		t, database.DSN, "vela_webhook_request_login", "vela-webhook-request-password",
	)
	principal, err := identity.NewAuthenticator(authPool, testCredentialPepper).Authenticate(
		context.Background(), testBearerCredential(),
	)
	if err != nil {
		t.Fatalf("authenticate webhook Principal: %v", err)
	}
	sealer, err := webhook.NewAESGCMSealer("webhook-key-v1", map[string][]byte{
		"webhook-key-v1": []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatalf("configure webhook secret sealer: %v", err)
	}
	service, err := webhook.NewService(webhookRequestPool, sealer, publicWebhookAddressResolver())
	if err != nil {
		t.Fatalf("configure webhook service: %v", err)
	}
	created, err := service.Create(context.Background(), principal, uuid.MustParse(testProjectID), webhook.CreateRequest{
		Endpoint:   "https://hooks.example.com/terminal",
		EventTypes: []webhook.EventType{webhook.EventJobCanceled},
	})
	if err != nil {
		t.Fatalf("create webhook Subscription: %v", err)
	}

	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "webhook-terminal-event", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"this Customer Content must never enter a webhook"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Accepted Job: %v", err)
	}
	canceled := cancelJob(t, server.URL, testProjectID, job.JobID, testBearerCredential())
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200; body=%s", canceled.StatusCode, canceled.Body)
	}

	var (
		deliveryID, eventID, deliveryJobID string
		eventType, state                   string
		jobVersion                         int64
		payload                            []byte
		retryWindowSeconds                 int64
	)
	if err := database.Admin.QueryRow(`
		SELECT delivery.id, delivery.event_id, delivery.event_type::text,
			delivery.job_id, delivery.job_version, delivery.payload::text,
			delivery.state::text,
			extract(epoch FROM delivery.retry_deadline_at - delivery.created_at)::bigint
		FROM webhook_deliveries AS delivery
		WHERE delivery.subscription_id = $1
	`, created.Subscription.ID).Scan(
		&deliveryID,
		&eventID,
		&eventType,
		&deliveryJobID,
		&jobVersion,
		&payload,
		&state,
		&retryWindowSeconds,
	); err != nil {
		t.Fatalf("read terminal webhook Delivery: %v", err)
	}
	if deliveryID == "" || eventID == "" || deliveryJobID != job.JobID ||
		eventType != "job.canceled" || jobVersion != 2 || state != "PENDING" ||
		retryWindowSeconds != int64((72*time.Hour)/time.Second) {
		t.Fatalf(
			"Delivery id=%s event=%s type=%s job=%s version=%d state=%s window=%ds",
			deliveryID, eventID, eventType, deliveryJobID, jobVersion, state, retryWindowSeconds,
		)
	}
	var body map[string]any
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("decode webhook payload: %v", err)
	}
	occurredAt, occurredAtOK := body["occurred_at"].(string)
	if len(body) != 9 || body["schema_version"] != float64(1) ||
		body["event_id"] != eventID || body["event_type"] != "job.canceled" ||
		body["organization_id"] != testOrganizationID || body["project_id"] != testProjectID ||
		body["job_id"] != job.JobID || body["job_version"] != float64(2) ||
		body["job_state"] != "CANCELED" || !occurredAtOK || occurredAt == "" ||
		bytes.Contains(payload, []byte("Customer Content")) ||
		bytes.Contains(payload, []byte("vwhsec_")) {
		t.Fatalf("webhook payload = %s", payload)
	}
}

func TestWebhookDispatcherDeliversAndCommitsAttemptReceipt(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'webhooks:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant webhook management scope: %v", err)
	}
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	webhookRequestPool := newRolePool(
		t, database.DSN, "vela_webhook_request_login", "vela-webhook-request-password",
	)
	principal, err := identity.NewAuthenticator(authPool, testCredentialPepper).Authenticate(
		context.Background(), testBearerCredential(),
	)
	if err != nil {
		t.Fatalf("authenticate webhook Principal: %v", err)
	}
	sealer, err := webhook.NewAESGCMSealer("webhook-key-v1", map[string][]byte{
		"webhook-key-v1": []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatalf("configure webhook secret sealer: %v", err)
	}
	service, err := webhook.NewService(webhookRequestPool, sealer, publicWebhookAddressResolver())
	if err != nil {
		t.Fatalf("configure webhook service: %v", err)
	}
	created, err := service.Create(context.Background(), principal, uuid.MustParse(testProjectID), webhook.CreateRequest{
		Endpoint:   "https://hooks.example.com/dispatch",
		EventTypes: []webhook.EventType{webhook.EventJobFailed},
	})
	if err != nil {
		t.Fatalf("create webhook Subscription: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "webhook-dispatch-receipt", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"dispatcher receipt fixture"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Accepted Job: %v", err)
	}
	jobID := uuid.MustParse(job.JobID)
	eventID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO outbox_events (
			event_id, organization_id, project_id, aggregate_type, aggregate_id,
			aggregate_version, event_type, schema_version, payload, occurred_at, available_at
		) VALUES (
			$1, $2, $3, 'Job', $4, 9, 'job.failed', 1,
			decode('00', 'hex'), clock_timestamp(), clock_timestamp()
		)
	`, eventID, testOrganizationID, testProjectID, jobID); err != nil {
		t.Fatalf("insert terminal Outbox fixture: %v", err)
	}

	webhookPool := newRolePool(t, database.DSN, "vela_webhook_login", "vela-webhook-password")
	adapter := &recordingDeliveryAdapter{statusCode: http.StatusNoContent}
	dispatcher, err := webhook.NewDispatcher(webhookPool, sealer, adapter, webhook.DispatcherConfig{
		InstanceID:      "webhook-dispatcher-a",
		BatchSize:       10,
		ClaimTTL:        30 * time.Second,
		DeliveryTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("configure webhook Dispatcher: %v", err)
	}
	result, err := dispatcher.DispatchBatch(context.Background())
	if err != nil {
		t.Fatalf("dispatch webhook batch: %v", err)
	}
	if result.Claimed != 1 || result.Delivered != 1 || result.Failed != 0 || len(adapter.requests) != 1 {
		t.Fatalf("dispatch result = %#v adapter calls=%d", result, len(adapter.requests))
	}
	request := adapter.requests[0]
	if request.EventID != eventID || request.SubscriptionID != created.Subscription.ID ||
		request.Endpoint != "https://hooks.example.com/dispatch" ||
		len(request.Secrets) != 1 || string(request.Secrets[0]) != created.SigningSecret ||
		!bytes.Contains(request.Payload, []byte(`"job_state": "FAILED"`)) {
		t.Fatalf("Delivery request = %#v", request)
	}

	var deliveryState, attemptState string
	var attempts, attemptNumber int
	var deliveredAt, completedAt bool
	var httpStatus int
	if err := database.Admin.QueryRow(`
		SELECT delivery.state::text, delivery.attempts,
			delivery.delivered_at IS NOT NULL,
			attempt.state::text, attempt.attempt_number,
			attempt.completed_at IS NOT NULL, attempt.http_status
		FROM webhook_deliveries AS delivery
		JOIN webhook_delivery_attempts AS attempt ON attempt.delivery_id = delivery.id
		WHERE delivery.event_id = $1
	`, eventID).Scan(
		&deliveryState,
		&attempts,
		&deliveredAt,
		&attemptState,
		&attemptNumber,
		&completedAt,
		&httpStatus,
	); err != nil {
		t.Fatalf("read webhook delivery receipt: %v", err)
	}
	if deliveryState != "DELIVERED" || attempts != 1 || !deliveredAt ||
		attemptState != "SUCCEEDED" || attemptNumber != 1 || !completedAt ||
		httpStatus != http.StatusNoContent {
		t.Fatalf(
			"receipt delivery=%s attempts=%d delivered=%t attempt=%s/%d completed=%t status=%d",
			deliveryState, attempts, deliveredAt, attemptState, attemptNumber, completedAt, httpStatus,
		)
	}
}

func TestConcurrentWebhookDispatchersClaimDeliveryOnce(t *testing.T) {
	fixture := newWebhookDispatchFixture(
		t,
		webhook.EventJobFailed,
		"https://hooks.example.com/concurrent-dispatch",
	)
	webhookPoolA := newRolePool(
		t, fixture.database.DSN, "vela_webhook_login", "vela-webhook-password",
	)
	webhookPoolB := newRolePool(
		t, fixture.database.DSN, "vela_webhook_login", "vela-webhook-password",
	)
	adapterA := &recordingDeliveryAdapter{statusCode: http.StatusNoContent}
	adapterB := &recordingDeliveryAdapter{statusCode: http.StatusNoContent}
	dispatcherA, err := webhook.NewDispatcher(
		webhookPoolA,
		fixture.sealer,
		adapterA,
		webhook.DispatcherConfig{
			InstanceID:      "concurrent-dispatcher-a",
			BatchSize:       1,
			ClaimTTL:        30 * time.Second,
			DeliveryTimeout: time.Second,
		},
	)
	if err != nil {
		t.Fatalf("configure concurrent Dispatcher A: %v", err)
	}
	dispatcherB, err := webhook.NewDispatcher(
		webhookPoolB,
		fixture.sealer,
		adapterB,
		webhook.DispatcherConfig{
			InstanceID:      "concurrent-dispatcher-b",
			BatchSize:       1,
			ClaimTTL:        30 * time.Second,
			DeliveryTimeout: time.Second,
		},
	)
	if err != nil {
		t.Fatalf("configure concurrent Dispatcher B: %v", err)
	}

	type dispatchCall struct {
		result webhook.BatchResult
		err    error
	}
	start := make(chan struct{})
	calls := make(chan dispatchCall, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for _, dispatcher := range []*webhook.Dispatcher{dispatcherA, dispatcherB} {
		go func(candidate *webhook.Dispatcher) {
			ready.Done()
			<-start
			result, dispatchErr := candidate.DispatchBatch(context.Background())
			calls <- dispatchCall{result: result, err: dispatchErr}
		}(dispatcher)
	}
	ready.Wait()
	close(start)

	var claimed, delivered, failed int
	for range 2 {
		select {
		case call := <-calls:
			if call.err != nil {
				t.Fatalf("concurrent Dispatcher error: %v", call.err)
			}
			claimed += call.result.Claimed
			delivered += call.result.Delivered
			failed += call.result.Failed
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Webhook Dispatchers did not finish")
		}
	}
	if claimed != 1 || delivered != 1 || failed != 0 ||
		len(adapterA.requests)+len(adapterB.requests) != 1 {
		t.Fatalf(
			"concurrent Dispatch = claimed %d delivered %d failed %d requests %d/%d",
			claimed,
			delivered,
			failed,
			len(adapterA.requests),
			len(adapterB.requests),
		)
	}
	var deliveryState string
	var attempts, attemptCount, succeededAttempts int
	if err := fixture.database.Admin.QueryRow(`
		SELECT delivery.state::text, delivery.attempts,
			count(attempt.id),
			count(attempt.id) FILTER (WHERE attempt.state = 'SUCCEEDED')
		FROM webhook_deliveries AS delivery
		LEFT JOIN webhook_delivery_attempts AS attempt ON attempt.delivery_id = delivery.id
		WHERE delivery.event_id = $1
		GROUP BY delivery.id
	`, fixture.eventID).Scan(
		&deliveryState,
		&attempts,
		&attemptCount,
		&succeededAttempts,
	); err != nil {
		t.Fatalf("read concurrent Webhook Delivery receipts: %v", err)
	}
	if deliveryState != "DELIVERED" || attempts != 1 || attemptCount != 1 ||
		succeededAttempts != 1 {
		t.Fatalf(
			"concurrent Delivery state=%s attempts=%d receipts=%d succeeded=%d",
			deliveryState,
			attempts,
			attemptCount,
			succeededAttempts,
		)
	}
}

func TestWebhookDispatcherClaimsEachDeliveryOnlyWhenReadyToSend(t *testing.T) {
	fixture := newWebhookDispatchFixture(
		t,
		webhook.EventJobFailed,
		"https://hooks.example.com/claim-when-ready",
	)
	secondEventID := uuid.New()
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO outbox_events (
			event_id, organization_id, project_id, aggregate_type, aggregate_id,
			aggregate_version, event_type, schema_version, payload, occurred_at, available_at
		)
		SELECT $1, organization_id, project_id, aggregate_type, aggregate_id,
			aggregate_version + 1, event_type, schema_version, payload,
			clock_timestamp(), clock_timestamp()
		FROM outbox_events
		WHERE event_id = $2
	`, secondEventID, fixture.eventID); err != nil {
		t.Fatalf("create second Webhook Delivery: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'webhooks:read')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant webhook read scope: %v", err)
	}
	principal, err := identity.NewAuthenticator(
		newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate webhook reader: %v", err)
	}
	service, err := webhook.NewService(
		webhookRequestPoolForDatabase(t, fixture.database),
		fixture.sealer,
		publicWebhookAddressResolver(),
	)
	if err != nil {
		t.Fatalf("configure webhook list service: %v", err)
	}
	var subscriptionID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT subscription_id
		FROM webhook_deliveries
		WHERE event_id = $1
	`, fixture.eventID).Scan(&subscriptionID); err != nil {
		t.Fatalf("read Webhook Subscription id: %v", err)
	}

	adapter := newBlockingFirstDeliveryAdapter()
	dispatcher, err := webhook.NewDispatcher(
		newRolePool(t, fixture.database.DSN, "vela_webhook_login", "vela-webhook-password"),
		fixture.sealer,
		adapter,
		webhook.DispatcherConfig{
			InstanceID:      "claim-when-ready-dispatcher",
			BatchSize:       2,
			ClaimTTL:        30 * time.Second,
			DeliveryTimeout: time.Second,
		},
	)
	if err != nil {
		t.Fatalf("configure claim-when-ready Dispatcher: %v", err)
	}
	type dispatchCall struct {
		result webhook.BatchResult
		err    error
	}
	call := make(chan dispatchCall, 1)
	go func() {
		result, dispatchErr := dispatcher.DispatchBatch(context.Background())
		call <- dispatchCall{result: result, err: dispatchErr}
	}()
	select {
	case <-adapter.started:
	case <-time.After(5 * time.Second):
		t.Fatal("first Webhook Delivery did not start")
	}
	deliveries, err := service.ListDeliveries(
		context.Background(),
		principal,
		uuid.MustParse(testProjectID),
		subscriptionID,
		10,
	)
	if err != nil {
		t.Fatalf("list claimed Webhook Deliveries: %v", err)
	}
	states := map[webhook.DeliveryState]int{}
	for _, delivery := range deliveries {
		states[delivery.State]++
	}
	if states[webhook.DeliveryInFlight] != 1 || states[webhook.DeliveryPending] != 1 {
		t.Fatalf("blocked batch Delivery states = %#v, want one IN_FLIGHT and one PENDING", states)
	}
	close(adapter.release)
	select {
	case completed := <-call:
		if completed.err != nil || completed.result.Claimed != 2 || completed.result.Delivered != 2 {
			t.Fatalf("claim-when-ready result = %#v error=%v", completed.result, completed.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("claim-when-ready Dispatcher did not finish")
	}
}

func TestWebhookDispatcherRejectsClaimTTLTooShortForRequestAndReceipt(t *testing.T) {
	fixture := newWebhookDispatchFixture(
		t,
		webhook.EventJobFailed,
		"https://hooks.example.com/short-claim",
	)
	_, err := webhook.NewDispatcher(
		newRolePool(t, fixture.database.DSN, "vela_webhook_login", "vela-webhook-password"),
		fixture.sealer,
		&recordingDeliveryAdapter{statusCode: http.StatusNoContent},
		webhook.DispatcherConfig{
			InstanceID:      "short-claim-dispatcher",
			BatchSize:       1,
			ClaimTTL:        6 * time.Second,
			DeliveryTimeout: time.Second,
		},
	)
	if err == nil || !strings.Contains(err.Error(), "must exceed delivery timeout") {
		t.Fatalf("short webhook claim TTL error = %v", err)
	}
}

func TestWebhookDispatcherEnforcesConfiguredDeliveryTimeout(t *testing.T) {
	fixture := newWebhookDispatchFixture(
		t,
		webhook.EventJobFailed,
		"https://hooks.example.com/delivery-timeout",
	)
	adapter := &deadlineRecordingDeliveryAdapter{}
	dispatcher, err := webhook.NewDispatcher(
		newRolePool(t, fixture.database.DSN, "vela_webhook_login", "vela-webhook-password"),
		fixture.sealer,
		adapter,
		webhook.DispatcherConfig{
			InstanceID:      "delivery-timeout-dispatcher",
			BatchSize:       1,
			ClaimTTL:        7 * time.Second,
			DeliveryTimeout: 250 * time.Millisecond,
		},
	)
	if err != nil {
		t.Fatalf("configure delivery-timeout Dispatcher: %v", err)
	}
	result, err := dispatcher.DispatchBatch(context.Background())
	if err != nil || result.Delivered != 1 {
		t.Fatalf("delivery-timeout dispatch result = %#v error=%v", result, err)
	}
	if !adapter.hasDeadline || adapter.remaining <= 0 || adapter.remaining > 250*time.Millisecond {
		t.Fatalf(
			"Delivery adapter deadline = present %t remaining %s",
			adapter.hasDeadline,
			adapter.remaining,
		)
	}
}

func TestWebhookDisableAndClaimSerializeWithoutDeadlock(t *testing.T) {
	fixture := newWebhookDispatchFixture(
		t,
		webhook.EventJobFailed,
		"https://hooks.example.com/disable-claim-lock-order",
	)
	concurrency := newWebhookConcurrencyFixture(t, fixture)
	const advisoryLockKey int64 = 5800111
	if _, err := fixture.database.Admin.Exec(`
		CREATE FUNCTION vela_test_pause_webhook_disable() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF OLD.state = 'PENDING' AND NEW.state = 'DEAD_LETTER'
			   AND NEW.last_error = 'Webhook Subscription was disabled' THEN
				PERFORM pg_advisory_xact_lock(5800111);
			END IF;
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER vela_test_pause_webhook_disable
		BEFORE UPDATE ON webhook_deliveries
		FOR EACH ROW EXECUTE FUNCTION vela_test_pause_webhook_disable();
	`); err != nil {
		t.Fatalf("install webhook disable pause trigger: %v", err)
	}
	blocker := beginWebhookAdvisoryBlocker(t, fixture.database.Admin, advisoryLockKey)
	defer func() { _ = blocker.Rollback() }()

	type disableCall struct {
		subscription webhook.Subscription
		err          error
	}
	disableResults := make(chan disableCall, 1)
	go func() {
		disabled, disableErr := concurrency.service.Disable(
			context.Background(),
			concurrency.principal,
			uuid.MustParse(testProjectID),
			concurrency.subscriptionID,
		)
		disableResults <- disableCall{subscription: disabled, err: disableErr}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_webhook_request_login")

	claimResults := make(chan webhookClaimCall, 1)
	go func() {
		claimed, claimErr := claimWebhookDeliveryCount(
			context.Background(), concurrency.webhookPool, "disable-claim-lock-order",
		)
		claimResults <- webhookClaimCall{claimed: claimed, err: claimErr}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_webhook_login")
	releaseWebhookAdvisoryBlocker(t, blocker, advisoryLockKey)

	select {
	case result := <-disableResults:
		if result.err != nil || result.subscription.State != webhook.SubscriptionDisabled {
			t.Fatalf("concurrent webhook disable = %#v error=%v", result.subscription, result.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent webhook disable did not finish")
	}
	select {
	case result := <-claimResults:
		if result.err != nil || result.claimed != 0 {
			t.Fatalf("claim racing webhook disable = %d error=%v, want no claim", result.claimed, result.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("claim racing webhook disable did not finish")
	}
	var subscriptionState, deliveryState string
	if err := fixture.database.Admin.QueryRow(`
		SELECT subscription.state::text, delivery.state::text
		FROM webhook_subscriptions AS subscription
		JOIN webhook_deliveries AS delivery ON delivery.subscription_id = subscription.id
		WHERE delivery.id = $1
	`, concurrency.deliveryID).Scan(&subscriptionState, &deliveryState); err != nil {
		t.Fatalf("read disable/claim outcome: %v", err)
	}
	if subscriptionState != "DISABLED" || deliveryState != "DEAD_LETTER" {
		t.Fatalf("disable/claim outcome = Subscription %s Delivery %s", subscriptionState, deliveryState)
	}
}

func TestWebhookReplayAndClaimSerializeWithoutDeadlock(t *testing.T) {
	fixture := newWebhookDispatchFixture(
		t,
		webhook.EventJobFailed,
		"https://hooks.example.com/replay-claim-lock-order",
	)
	concurrency := newWebhookConcurrencyFixture(t, fixture)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE webhook_deliveries
		SET state = 'DEAD_LETTER', dead_lettered_at = clock_timestamp(),
			last_error = 'terminal fixture for concurrent replay'
		WHERE id = $1
	`, concurrency.deliveryID); err != nil {
		t.Fatalf("prepare concurrent replay Delivery: %v", err)
	}
	const advisoryLockKey int64 = 5800112
	if _, err := fixture.database.Admin.Exec(`
		CREATE FUNCTION vela_test_pause_webhook_replay() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock(5800112);
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER vela_test_pause_webhook_replay
		BEFORE INSERT ON webhook_delivery_replays
		FOR EACH ROW EXECUTE FUNCTION vela_test_pause_webhook_replay();
	`); err != nil {
		t.Fatalf("install webhook replay pause trigger: %v", err)
	}
	blocker := beginWebhookAdvisoryBlocker(t, fixture.database.Admin, advisoryLockKey)
	defer func() { _ = blocker.Rollback() }()

	type replayCall struct {
		delivery webhook.Delivery
		err      error
	}
	replayResults := make(chan replayCall, 1)
	go func() {
		replayed, replayErr := concurrency.service.Replay(
			context.Background(),
			concurrency.principal,
			uuid.MustParse(testProjectID),
			concurrency.subscriptionID,
			concurrency.deliveryID,
		)
		replayResults <- replayCall{delivery: replayed, err: replayErr}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_webhook_request_login")

	claimContext, cancelClaim := context.WithTimeout(context.Background(), 3*time.Second)
	claimedBeforeCommit, err := claimWebhookDeliveryCount(
		claimContext, concurrency.webhookPool, "replay-before-commit",
	)
	cancelClaim()
	if err != nil || claimedBeforeCommit != 0 {
		t.Fatalf(
			"claim during uncommitted replay = %d error=%v, want no visible claim",
			claimedBeforeCommit,
			err,
		)
	}
	releaseWebhookAdvisoryBlocker(t, blocker, advisoryLockKey)
	select {
	case result := <-replayResults:
		if result.err != nil || result.delivery.State != webhook.DeliveryPending ||
			result.delivery.Generation != 2 {
			t.Fatalf("concurrent webhook replay = %#v error=%v", result.delivery, result.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent webhook replay did not finish")
	}
	claimedAfterCommit, err := claimWebhookDeliveryCount(
		context.Background(), concurrency.webhookPool, "replay-after-commit",
	)
	if err != nil || claimedAfterCommit != 1 {
		t.Fatalf("claim after committed replay = %d error=%v, want one claim", claimedAfterCommit, err)
	}
}

func TestWebhookRotationAndClaimSerializeWithoutDeadlock(t *testing.T) {
	fixture := newWebhookDispatchFixture(
		t,
		webhook.EventJobFailed,
		"https://hooks.example.com/rotation-claim-lock-order",
	)
	concurrency := newWebhookConcurrencyFixture(t, fixture)
	const advisoryLockKey int64 = 5800113
	if _, err := fixture.database.Admin.Exec(`
		CREATE FUNCTION vela_test_pause_webhook_rotation() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			IF OLD.valid_until IS NULL AND NEW.valid_until IS NOT NULL THEN
				PERFORM pg_advisory_xact_lock(5800113);
			END IF;
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER vela_test_pause_webhook_rotation
		BEFORE UPDATE ON webhook_subscription_secrets
		FOR EACH ROW EXECUTE FUNCTION vela_test_pause_webhook_rotation();
	`); err != nil {
		t.Fatalf("install webhook rotation pause trigger: %v", err)
	}
	blocker := beginWebhookAdvisoryBlocker(t, fixture.database.Admin, advisoryLockKey)
	defer func() { _ = blocker.Rollback() }()

	type rotationCall struct {
		rotation webhook.RotatedSubscription
		err      error
	}
	rotationResults := make(chan rotationCall, 1)
	go func() {
		rotated, rotationErr := concurrency.service.RotateSecret(
			context.Background(),
			concurrency.principal,
			uuid.MustParse(testProjectID),
			concurrency.subscriptionID,
		)
		rotationResults <- rotationCall{rotation: rotated, err: rotationErr}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_webhook_request_login")

	claimResults := make(chan webhookClaimCall, 1)
	go func() {
		claimed, claimErr := claimWebhookDeliveryCount(
			context.Background(), concurrency.webhookPool, "rotation-claim-lock-order",
		)
		claimResults <- webhookClaimCall{claimed: claimed, err: claimErr}
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_webhook_login")
	releaseWebhookAdvisoryBlocker(t, blocker, advisoryLockKey)

	select {
	case result := <-rotationResults:
		if result.err != nil || result.rotation.Subscription.SecretRevision != 2 {
			t.Fatalf("concurrent webhook rotation = %#v error=%v", result.rotation, result.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent webhook rotation did not finish")
	}
	select {
	case result := <-claimResults:
		if result.err != nil || result.claimed != 1 {
			t.Fatalf("claim racing webhook rotation = %d error=%v, want one claim", result.claimed, result.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("claim racing webhook rotation did not finish")
	}
	var currentSecrets, overlappingSecrets, attempts int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			count(*) FILTER (WHERE secret.valid_until IS NULL),
			count(*) FILTER (WHERE secret.valid_until > clock_timestamp()),
			(SELECT count(*) FROM webhook_delivery_attempts WHERE delivery_id = $2)
		FROM webhook_subscription_secrets AS secret
		WHERE secret.subscription_id = $1
	`, concurrency.subscriptionID, concurrency.deliveryID).Scan(
		&currentSecrets, &overlappingSecrets, &attempts,
	); err != nil {
		t.Fatalf("read rotation/claim outcome: %v", err)
	}
	if currentSecrets != 1 || overlappingSecrets != 1 || attempts != 1 {
		t.Fatalf(
			"rotation/claim outcome = current %d overlapping %d attempts %d",
			currentSecrets,
			overlappingSecrets,
			attempts,
		)
	}
}

func TestWebhookDispatcherRetriesSameEventAfterRemoteFailure(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'webhooks:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant webhook management scope: %v", err)
	}
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	webhookRequestPool := newRolePool(
		t, database.DSN, "vela_webhook_request_login", "vela-webhook-request-password",
	)
	principal, err := identity.NewAuthenticator(authPool, testCredentialPepper).Authenticate(
		context.Background(), testBearerCredential(),
	)
	if err != nil {
		t.Fatalf("authenticate webhook Principal: %v", err)
	}
	sealer, err := webhook.NewAESGCMSealer("webhook-key-v1", map[string][]byte{
		"webhook-key-v1": []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatalf("configure webhook secret sealer: %v", err)
	}
	service, err := webhook.NewService(webhookRequestPool, sealer, publicWebhookAddressResolver())
	if err != nil {
		t.Fatalf("configure webhook service: %v", err)
	}
	_, err = service.Create(context.Background(), principal, uuid.MustParse(testProjectID), webhook.CreateRequest{
		Endpoint:   "https://hooks.example.com/retry",
		EventTypes: []webhook.EventType{webhook.EventJobFailed},
	})
	if err != nil {
		t.Fatalf("create webhook Subscription: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "webhook-retry", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"retry the notification, not the Job"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Accepted Job: %v", err)
	}
	eventID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO outbox_events (
			event_id, organization_id, project_id, aggregate_type, aggregate_id,
			aggregate_version, event_type, schema_version, payload, occurred_at, available_at
		) VALUES (
			$1, $2, $3, 'Job', $4, 4, 'job.failed', 1,
			decode('00', 'hex'), clock_timestamp(), clock_timestamp()
		)
	`, eventID, testOrganizationID, testProjectID, job.JobID); err != nil {
		t.Fatalf("insert terminal Outbox fixture: %v", err)
	}

	webhookPool := newRolePool(t, database.DSN, "vela_webhook_login", "vela-webhook-password")
	adapter := &recordingDeliveryAdapter{
		statusCode: http.StatusServiceUnavailable,
		err:        errors.New("remote endpoint unavailable"),
	}
	dispatcher, err := webhook.NewDispatcher(webhookPool, sealer, adapter, webhook.DispatcherConfig{
		InstanceID:      "webhook-dispatcher-retry",
		BatchSize:       10,
		ClaimTTL:        30 * time.Second,
		DeliveryTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("configure webhook Dispatcher: %v", err)
	}
	first, firstErr := dispatcher.DispatchBatch(context.Background())
	if firstErr == nil || first.Claimed != 1 || first.Delivered != 0 || first.Failed != 1 {
		t.Fatalf("first dispatch result = %#v error=%v", first, firstErr)
	}
	var state, attemptState string
	var attempts int
	var availableInFuture bool
	if err := database.Admin.QueryRow(`
		SELECT delivery.state::text, delivery.attempts,
			delivery.available_at > delivery.updated_at,
			attempt.state::text
		FROM webhook_deliveries AS delivery
		JOIN webhook_delivery_attempts AS attempt ON attempt.delivery_id = delivery.id
		WHERE delivery.event_id = $1
	`, eventID).Scan(&state, &attempts, &availableInFuture, &attemptState); err != nil {
		t.Fatalf("read failed webhook attempt: %v", err)
	}
	if state != "PENDING" || attempts != 1 || !availableInFuture || attemptState != "FAILED" {
		t.Fatalf("failed attempt state=%s attempts=%d future=%t receipt=%s", state, attempts, availableInFuture, attemptState)
	}

	if _, err := database.Admin.Exec(`
		UPDATE webhook_deliveries
		SET available_at = clock_timestamp() - interval '1 second'
		WHERE event_id = $1
	`, eventID); err != nil {
		t.Fatalf("advance webhook retry availability: %v", err)
	}
	adapter.statusCode = http.StatusOK
	adapter.err = nil
	second, err := dispatcher.DispatchBatch(context.Background())
	if err != nil {
		t.Fatalf("dispatch webhook retry: %v", err)
	}
	if second.Claimed != 1 || second.Delivered != 1 || second.Failed != 0 ||
		len(adapter.requests) != 2 || adapter.requests[0].EventID != eventID ||
		adapter.requests[1].EventID != eventID {
		t.Fatalf("retry result = %#v requests=%#v", second, adapter.requests)
	}
	var attemptStates []string
	rows, err := database.Admin.Query(`
		SELECT state::text
		FROM webhook_delivery_attempts
		WHERE delivery_id = (SELECT id FROM webhook_deliveries WHERE event_id = $1)
		ORDER BY attempt_number
	`, eventID)
	if err != nil {
		t.Fatalf("query webhook attempts: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			t.Fatalf("scan webhook attempt: %v", err)
		}
		attemptStates = append(attemptStates, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read webhook attempts: %v", err)
	}
	if len(attemptStates) != 2 || attemptStates[0] != "FAILED" || attemptStates[1] != "SUCCEEDED" {
		t.Fatalf("attempt states = %v", attemptStates)
	}
}

func TestWebhookDispatcherPersistsFailureAfterDeliveryCancelsContext(t *testing.T) {
	fixture := newWebhookDispatchFixture(
		t,
		webhook.EventJobFailed,
		"https://hooks.example.com/canceled-context",
	)
	webhookPool := newRolePool(
		t, fixture.database.DSN, "vela_webhook_login", "vela-webhook-password",
	)
	ctx, cancel := context.WithCancel(context.Background())
	adapter := &cancelingDeliveryAdapter{cancel: cancel}
	dispatcher, err := webhook.NewDispatcher(webhookPool, fixture.sealer, adapter, webhook.DispatcherConfig{
		InstanceID:      "context-cancel-dispatcher",
		BatchSize:       1,
		ClaimTTL:        30 * time.Second,
		DeliveryTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("configure context-cancel Dispatcher: %v", err)
	}
	result, dispatchErr := dispatcher.DispatchBatch(ctx)
	if dispatchErr == nil || result.Claimed != 1 || result.Failed != 1 {
		t.Fatalf("canceled dispatch result = %#v error=%v", result, dispatchErr)
	}

	var deliveryState, attemptState string
	var claimTokenCleared, attemptCompleted bool
	if err := fixture.database.Admin.QueryRow(`
		SELECT delivery.state::text, delivery.claim_token IS NULL,
			attempt.state::text, attempt.completed_at IS NOT NULL
		FROM webhook_deliveries AS delivery
		JOIN webhook_delivery_attempts AS attempt ON attempt.delivery_id = delivery.id
		WHERE delivery.event_id = $1
	`, fixture.eventID).Scan(
		&deliveryState,
		&claimTokenCleared,
		&attemptState,
		&attemptCompleted,
	); err != nil {
		t.Fatalf("read canceled-context receipt: %v", err)
	}
	if deliveryState != "PENDING" || !claimTokenCleared ||
		attemptState != "FAILED" || !attemptCompleted {
		t.Fatalf(
			"canceled-context receipt delivery=%s claim-cleared=%t attempt=%s completed=%t",
			deliveryState,
			claimTokenCleared,
			attemptState,
			attemptCompleted,
		)
	}
}

func TestWebhookDispatcherRecoversExpiredClaimAfterCrash(t *testing.T) {
	fixture := newWebhookDispatchFixture(t, webhook.EventJobFailed, "https://hooks.example.com/crash")
	webhookPool := newRolePool(
		t, fixture.database.DSN, "vela_webhook_login", "vela-webhook-password",
	)
	rows, err := webhookPool.Query(context.Background(), `
		SELECT delivery_id
		FROM vela_claim_webhook_deliveries('crashed-dispatcher', 30, 1)
	`)
	if err != nil {
		t.Fatalf("claim webhook before crash: %v", err)
	}
	var claimedDeliveryID uuid.UUID
	if !rows.Next() || rows.Scan(&claimedDeliveryID) != nil || rows.Next() {
		rows.Close()
		t.Fatal("crash fixture did not claim exactly one Webhook Delivery")
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatalf("read crash claim: %v", err)
	}
	rows.Close()
	if _, err := fixture.database.Admin.Exec(`
		UPDATE webhook_deliveries
		SET claim_expires_at = clock_timestamp() - interval '1 second'
		WHERE id = $1
	`, claimedDeliveryID); err != nil {
		t.Fatalf("expire crashed webhook claim: %v", err)
	}

	adapter := &recordingDeliveryAdapter{statusCode: http.StatusNoContent}
	dispatcher, err := webhook.NewDispatcher(webhookPool, fixture.sealer, adapter, webhook.DispatcherConfig{
		InstanceID:      "replacement-dispatcher",
		BatchSize:       1,
		ClaimTTL:        30 * time.Second,
		DeliveryTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("configure replacement Dispatcher: %v", err)
	}
	result, err := dispatcher.DispatchBatch(context.Background())
	if err != nil {
		t.Fatalf("recover expired webhook claim: %v", err)
	}
	if result.Claimed != 1 || result.Delivered != 1 || len(adapter.requests) != 1 ||
		adapter.requests[0].EventID != fixture.eventID {
		t.Fatalf("recovery result = %#v requests=%#v", result, adapter.requests)
	}
	var states []string
	attemptRows, err := fixture.database.Admin.Query(`
		SELECT state::text
		FROM webhook_delivery_attempts
		WHERE delivery_id = $1
		ORDER BY attempt_number
	`, claimedDeliveryID)
	if err != nil {
		t.Fatalf("query crash recovery attempts: %v", err)
	}
	defer attemptRows.Close()
	for attemptRows.Next() {
		var state string
		if err := attemptRows.Scan(&state); err != nil {
			t.Fatalf("scan crash recovery attempt: %v", err)
		}
		states = append(states, state)
	}
	if err := attemptRows.Err(); err != nil {
		t.Fatalf("read crash recovery attempts: %v", err)
	}
	if len(states) != 2 || states[0] != "ABANDONED" || states[1] != "SUCCEEDED" {
		t.Fatalf("crash recovery attempt states = %v", states)
	}
}

func TestWebhookStaleClaimReceiptsCannotMutateReplacementClaim(t *testing.T) {
	fixture := newWebhookDispatchFixture(
		t,
		webhook.EventJobFailed,
		"https://hooks.example.com/stale-claim",
	)
	webhookPool := newRolePool(
		t, fixture.database.DSN, "vela_webhook_login", "vela-webhook-password",
	)
	var deliveryID, staleToken uuid.UUID
	if err := webhookPool.QueryRow(context.Background(), `
		SELECT delivery_id, claim_token
		FROM vela_claim_webhook_deliveries('stale-owner', 30, 1)
	`).Scan(&deliveryID, &staleToken); err != nil {
		t.Fatalf("create stale webhook claim: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE webhook_deliveries
		SET claim_expires_at = clock_timestamp() - interval '1 second'
		WHERE id = $1
	`, deliveryID); err != nil {
		t.Fatalf("expire stale webhook claim: %v", err)
	}
	var replacementDeliveryID, replacementToken uuid.UUID
	if err := webhookPool.QueryRow(context.Background(), `
		SELECT delivery_id, claim_token
		FROM vela_claim_webhook_deliveries('replacement-owner', 30, 1)
	`).Scan(&replacementDeliveryID, &replacementToken); err != nil {
		t.Fatalf("create replacement webhook claim: %v", err)
	}
	if replacementDeliveryID != deliveryID || replacementToken == staleToken {
		t.Fatalf(
			"replacement claim Delivery/token = %s/%s, stale = %s/%s",
			replacementDeliveryID,
			replacementToken,
			deliveryID,
			staleToken,
		)
	}

	for _, test := range []struct {
		name      string
		statement string
		args      []any
	}{
		{
			name:      "success",
			statement: `SELECT vela_mark_webhook_delivered($1, $2, 204)`,
			args:      []any{deliveryID, staleToken},
		},
		{
			name:      "failure",
			statement: `SELECT vela_mark_webhook_failed($1, $2, 503, 'stale failure')`,
			args:      []any{deliveryID, staleToken},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var marked bool
			if err := webhookPool.QueryRow(
				context.Background(), test.statement, test.args...,
			).Scan(&marked); err != nil {
				t.Fatalf("submit stale %s receipt: %v", test.name, err)
			}
			if marked {
				t.Fatalf("stale %s receipt was accepted", test.name)
			}
		})
	}

	var state, claimedBy, replacementAttemptState, staleAttemptState string
	var persistedToken uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT delivery.state::text, delivery.claimed_by, delivery.claim_token,
			replacement.state::text, stale.state::text
		FROM webhook_deliveries AS delivery
		JOIN webhook_delivery_attempts AS replacement
		  ON replacement.delivery_id = delivery.id
		 AND replacement.claim_token = delivery.claim_token
		JOIN webhook_delivery_attempts AS stale
		  ON stale.delivery_id = delivery.id AND stale.claim_token = $2
		WHERE delivery.id = $1
	`, deliveryID, staleToken).Scan(
		&state,
		&claimedBy,
		&persistedToken,
		&replacementAttemptState,
		&staleAttemptState,
	); err != nil {
		t.Fatalf("read replacement webhook claim: %v", err)
	}
	if state != "IN_FLIGHT" || claimedBy != "replacement-owner" ||
		persistedToken != replacementToken || replacementAttemptState != "STARTED" ||
		staleAttemptState != "ABANDONED" {
		t.Fatalf(
			"replacement claim = state %s owner %s token %s attempts %s/%s",
			state,
			claimedBy,
			persistedToken,
			staleAttemptState,
			replacementAttemptState,
		)
	}
}

func TestWebhookDispatcherDeadLettersAfterSeventyTwoHours(t *testing.T) {
	fixture := newWebhookDispatchFixture(t, webhook.EventJobFailed, "https://hooks.example.com/dead-letter")
	retryWindowStartedAt := time.Now().UTC().Add(-73 * time.Hour).Truncate(time.Microsecond)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE webhook_deliveries
		SET retry_window_started_at = $2::timestamptz,
			retry_deadline_at = $2::timestamptz + interval '72 hours',
			available_at = $2::timestamptz + interval '71 hours'
		WHERE event_id = $1
	`, fixture.eventID, retryWindowStartedAt); err != nil {
		t.Fatalf("expire webhook automatic retry window: %v", err)
	}
	webhookPool := newRolePool(
		t, fixture.database.DSN, "vela_webhook_login", "vela-webhook-password",
	)
	adapter := &recordingDeliveryAdapter{statusCode: http.StatusNoContent}
	dispatcher, err := webhook.NewDispatcher(webhookPool, fixture.sealer, adapter, webhook.DispatcherConfig{
		InstanceID:      "dead-letter-reconciler",
		BatchSize:       10,
		ClaimTTL:        30 * time.Second,
		DeliveryTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("configure webhook Dispatcher: %v", err)
	}
	result, err := dispatcher.DispatchBatch(context.Background())
	if err != nil {
		t.Fatalf("reconcile expired webhook Delivery: %v", err)
	}
	if result != (webhook.BatchResult{}) || len(adapter.requests) != 0 {
		t.Fatalf("expired dispatch result = %#v requests=%#v", result, adapter.requests)
	}
	var state string
	var attempts int
	var deadLettered bool
	if err := fixture.database.Admin.QueryRow(`
		SELECT state::text, attempts, dead_lettered_at IS NOT NULL
		FROM webhook_deliveries
		WHERE event_id = $1
	`, fixture.eventID).Scan(&state, &attempts, &deadLettered); err != nil {
		t.Fatalf("read expired webhook Delivery: %v", err)
	}
	if state != "DEAD_LETTER" || attempts != 0 || !deadLettered {
		t.Fatalf("expired Delivery state=%s attempts=%d dead_lettered=%t", state, attempts, deadLettered)
	}
}

func TestWebhookDispatcherSQLFunctionsRejectNullRequiredParameters(t *testing.T) {
	fixture := newWebhookDispatchFixture(
		t,
		webhook.EventJobFailed,
		"https://hooks.example.com/null-parameters",
	)
	webhookPool := newRolePool(
		t, fixture.database.DSN, "vela_webhook_login", "vela-webhook-password",
	)

	for _, test := range []struct {
		name      string
		statement string
		args      []any
	}{
		{
			name:      "claim instance",
			statement: `SELECT * FROM vela_claim_webhook_deliveries(NULL, 30, 1)`,
		},
		{
			name:      "claim TTL",
			statement: `SELECT * FROM vela_claim_webhook_deliveries('null-contract', NULL, 1)`,
		},
		{
			name:      "claim batch",
			statement: `SELECT * FROM vela_claim_webhook_deliveries('null-contract', 30, NULL)`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := webhookPool.Exec(context.Background(), test.statement, test.args...)
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "22023" {
				t.Fatalf("NULL %s error = %v, want SQLSTATE 22023", test.name, err)
			}
		})
	}

	var deliveryID, claimToken uuid.UUID
	if err := webhookPool.QueryRow(context.Background(), `
		SELECT delivery_id, claim_token
		FROM vela_claim_webhook_deliveries('null-contract-live-claim', 30, 1)
	`).Scan(&deliveryID, &claimToken); err != nil {
		t.Fatalf("create live claim for NULL receipt contract: %v", err)
	}
	for _, test := range []struct {
		name      string
		statement string
		args      []any
	}{
		{
			name: "delivered Delivery",
			statement: `SELECT vela_mark_webhook_delivered(
				NULL, $1, 204
			)`,
			args: []any{claimToken},
		},
		{
			name: "delivered token",
			statement: `SELECT vela_mark_webhook_delivered(
				$1, NULL, 204
			)`,
			args: []any{deliveryID},
		},
		{
			name: "delivered status",
			statement: `SELECT vela_mark_webhook_delivered(
				$1, $2, NULL
			)`,
			args: []any{deliveryID, claimToken},
		},
		{
			name: "failed Delivery",
			statement: `SELECT vela_mark_webhook_failed(
				NULL, $1, NULL, 'transport failed'
			)`,
			args: []any{claimToken},
		},
		{
			name: "failed token",
			statement: `SELECT vela_mark_webhook_failed(
				$1, NULL, NULL, 'transport failed'
			)`,
			args: []any{deliveryID},
		},
		{
			name: "failed error",
			statement: `SELECT vela_mark_webhook_failed(
				$1, $2, NULL, NULL
			)`,
			args: []any{deliveryID, claimToken},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := webhookPool.Exec(context.Background(), test.statement, test.args...)
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "22023" {
				t.Fatalf("NULL %s error = %v, want SQLSTATE 22023", test.name, err)
			}
		})
	}

	var state, attemptState string
	var persistedToken uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT delivery.state::text, delivery.claim_token, attempt.state::text
		FROM webhook_deliveries AS delivery
		JOIN webhook_delivery_attempts AS attempt
		  ON attempt.delivery_id = delivery.id AND attempt.claim_token = delivery.claim_token
		WHERE delivery.id = $1
	`, deliveryID).Scan(&state, &persistedToken, &attemptState); err != nil {
		t.Fatalf("read live claim after NULL receipt calls: %v", err)
	}
	if state != "IN_FLIGHT" || persistedToken != claimToken || attemptState != "STARTED" {
		t.Fatalf(
			"live claim after NULL calls = state %s token %s attempt %s",
			state,
			persistedToken,
			attemptState,
		)
	}
}

func TestWebhookManagementSQLFunctionsRejectNullRequiredParameters(t *testing.T) {
	fixture := newWebhookDispatchFixture(
		t,
		webhook.EventJobFailed,
		"https://hooks.example.com/management-null-parameters",
	)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'webhooks:read')
		WHERE id = $1 AND NOT ('webhooks:read' = ANY(scopes))
	`, testCredentialID); err != nil {
		t.Fatalf("grant webhook read scope for NULL contract: %v", err)
	}
	principal, err := identity.NewAuthenticator(
		newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate webhook NULL-contract Principal: %v", err)
	}
	webhookRequestPool := webhookRequestPoolForDatabase(t, fixture.database)
	projectID := uuid.MustParse(testProjectID)
	var subscriptionID, deliveryID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT subscription_id, id
		FROM webhook_deliveries
		WHERE event_id = $1
	`, fixture.eventID).Scan(&subscriptionID, &deliveryID); err != nil {
		t.Fatalf("read webhook NULL-contract identities: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE webhook_deliveries
		SET state = 'DEAD_LETTER', dead_lettered_at = clock_timestamp(),
			last_error = 'terminal fixture for NULL replay contract'
		WHERE id = $1
	`, deliveryID); err != nil {
		t.Fatalf("prepare terminal webhook NULL-contract Delivery: %v", err)
	}

	for _, test := range []struct {
		name      string
		statement string
		args      []any
	}{
		{
			name:      "rotation lock Project",
			statement: `SELECT vela_lock_webhook_secret_rotation(NULL, $1)`,
			args:      []any{subscriptionID},
		},
		{
			name:      "rotation lock Subscription",
			statement: `SELECT vela_lock_webhook_secret_rotation($1, NULL)`,
			args:      []any{projectID},
		},
	} {
		_, err := fixture.database.Admin.Exec(test.statement, test.args...)
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != "22023" {
			t.Fatalf("NULL %s error = %v, want SQLSTATE 22023", test.name, err)
		}
	}

	type nullSQLCase struct {
		name      string
		scope     string
		statement string
		args      []any
	}
	var tests []nullSQLCase
	addCases := func(
		function, scope, statement string,
		parameterNames []string,
		validArgs []any,
	) {
		t.Helper()
		for index, parameterName := range parameterNames {
			args := append([]any(nil), validArgs...)
			args[index] = nil
			tests = append(tests, nullSQLCase{
				name:      function + " " + parameterName,
				scope:     scope,
				statement: statement,
				args:      args,
			})
		}
	}
	nonce := bytes.Repeat([]byte{1}, 12)
	ciphertext := bytes.Repeat([]byte{2}, 16)
	addCases(
		"create",
		identity.ScopeWebhooksManage,
		`SELECT * FROM vela_create_webhook_subscription(
			$1, $2, $3, $4::webhook_event_type[], $5, $6, $7, $8
		)`,
		[]string{
			"Subscription", "Project", "endpoint", "event types",
			"secret", "key id", "nonce", "ciphertext",
		},
		[]any{
			uuid.New(), projectID, "https://hooks.example.com/null-create",
			[]string{"job.failed"}, uuid.New(), "webhook-key-v1", nonce, ciphertext,
		},
	)
	addCases(
		"rotate",
		identity.ScopeWebhooksManage,
		`SELECT * FROM vela_rotate_webhook_secret($1, $2, $3, $4, $5, $6, $7)`,
		[]string{"Project", "Subscription", "secret", "revision", "key id", "nonce", "ciphertext"},
		[]any{projectID, subscriptionID, uuid.New(), int32(2), "webhook-key-v1", nonce, ciphertext},
	)
	addCases(
		"disable",
		identity.ScopeWebhooksManage,
		`SELECT * FROM vela_disable_webhook_subscription($1, $2)`,
		[]string{"Project", "Subscription"},
		[]any{projectID, subscriptionID},
	)
	addCases(
		"list Subscriptions",
		identity.ScopeWebhooksRead,
		`SELECT * FROM vela_list_webhook_subscriptions($1, $2)`,
		[]string{"Project", "limit"},
		[]any{projectID, int32(100)},
	)
	addCases(
		"list Deliveries",
		identity.ScopeWebhooksRead,
		`SELECT * FROM vela_list_webhook_deliveries($1, $2, $3)`,
		[]string{"Project", "Subscription", "limit"},
		[]any{projectID, subscriptionID, int32(100)},
	)
	addCases(
		"replay",
		identity.ScopeWebhooksManage,
		`SELECT * FROM vela_replay_webhook_delivery($1, $2, $3, $4)`,
		[]string{"Project", "Subscription", "Delivery", "receipt"},
		[]any{projectID, subscriptionID, deliveryID, uuid.New()},
	)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tx, err := webhookRequestPool.Begin(context.Background())
			if err != nil {
				t.Fatalf("begin NULL %s transaction: %v", test.name, err)
			}
			defer func() { _ = tx.Rollback(context.Background()) }()
			if _, err := tx.Exec(
				context.Background(),
				"SELECT * FROM vela_set_request_context($1, $2, $3)",
				principal.CredentialID,
				principal.RequestContextProof(),
				test.scope,
			); err != nil {
				t.Fatalf("establish NULL %s request context: %v", test.name, err)
			}
			_, err = tx.Exec(context.Background(), test.statement, test.args...)
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != "22023" {
				t.Fatalf("NULL %s error = %v, want SQLSTATE 22023", test.name, err)
			}
		})
	}

	var subscriptions, secrets, replays int
	var deliveryState string
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM webhook_subscriptions WHERE id = $1),
			(SELECT count(*) FROM webhook_subscription_secrets WHERE subscription_id = $1),
			(SELECT state::text FROM webhook_deliveries WHERE id = $2),
			(SELECT count(*) FROM webhook_delivery_replays WHERE delivery_id = $2)
	`, subscriptionID, deliveryID).Scan(
		&subscriptions, &secrets, &deliveryState, &replays,
	); err != nil {
		t.Fatalf("read webhook evidence after NULL management calls: %v", err)
	}
	if subscriptions != 1 || secrets != 1 || deliveryState != "DEAD_LETTER" || replays != 0 {
		t.Fatalf(
			"webhook evidence after NULL calls = subscriptions %d secrets %d state %s replays %d",
			subscriptions,
			secrets,
			deliveryState,
			replays,
		)
	}
}

func TestWebhookManualReplayPreservesEventAndCreatesReceipt(t *testing.T) {
	fixture := newWebhookDispatchFixture(t, webhook.EventJobFailed, "https://hooks.example.com/manual-replay")
	if _, err := fixture.database.Admin.Exec(`
		UPDATE webhook_deliveries
		SET state = 'DEAD_LETTER', dead_lettered_at = clock_timestamp(),
			last_error = 'forced dead letter for replay test'
		WHERE event_id = $1
	`, fixture.eventID); err != nil {
		t.Fatalf("prepare dead-letter Delivery: %v", err)
	}
	authPool := newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password")
	webhookRequestPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_webhook_request_login",
		"vela-webhook-request-password",
	)
	principal, err := identity.NewAuthenticator(authPool, testCredentialPepper).Authenticate(
		context.Background(), testBearerCredential(),
	)
	if err != nil {
		t.Fatalf("authenticate replay Principal: %v", err)
	}
	service, err := webhook.NewService(
		webhookRequestPool,
		fixture.sealer,
		publicWebhookAddressResolver(),
	)
	if err != nil {
		t.Fatalf("configure webhook service: %v", err)
	}
	var subscriptionID, deliveryID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT subscription_id, id FROM webhook_deliveries WHERE event_id = $1
	`, fixture.eventID).Scan(&subscriptionID, &deliveryID); err != nil {
		t.Fatalf("read dead-letter Delivery identity: %v", err)
	}
	replayed, err := service.Replay(
		context.Background(),
		principal,
		uuid.MustParse(testProjectID),
		subscriptionID,
		deliveryID,
	)
	if err != nil {
		t.Fatalf("replay Webhook Delivery: %v", err)
	}
	if replayed.EventID != fixture.eventID || replayed.State != webhook.DeliveryPending ||
		replayed.Generation != 2 || replayed.RetryDeadlineAt.Before(time.Now().Add(71*time.Hour)) {
		t.Fatalf("replayed Delivery = %#v", replayed)
	}
	_, err = service.Replay(
		context.Background(),
		principal,
		uuid.MustParse(testProjectID),
		subscriptionID,
		deliveryID,
	)
	var failure *webhook.Failure
	if err == nil || !errors.As(err, &failure) || failure.Code != webhook.FailureConflict {
		t.Fatalf("duplicate replay error = %v, want conflict", err)
	}
	var replayCount int
	var requestedPrincipal, requestedCredential uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM webhook_delivery_replays WHERE delivery_id = $1),
			replay.requested_by_principal_id,
			replay.requested_by_credential_id
		FROM webhook_delivery_replays AS replay
		WHERE replay.delivery_id = $1
		LIMIT 1
	`, deliveryID).Scan(&replayCount, &requestedPrincipal, &requestedCredential); err != nil {
		t.Fatalf("read Webhook replay receipt: %v", err)
	}
	if replayCount != 1 || requestedPrincipal.String() != testPrincipalID ||
		requestedCredential.String() != testCredentialID {
		t.Fatalf(
			"replay receipts=%d principal=%s credential=%s",
			replayCount, requestedPrincipal, requestedCredential,
		)
	}

	webhookPool := newRolePool(
		t, fixture.database.DSN, "vela_webhook_login", "vela-webhook-password",
	)
	adapter := &recordingDeliveryAdapter{statusCode: http.StatusNoContent}
	dispatcher, err := webhook.NewDispatcher(webhookPool, fixture.sealer, adapter, webhook.DispatcherConfig{
		InstanceID:      "manual-replay-dispatcher",
		BatchSize:       1,
		ClaimTTL:        30 * time.Second,
		DeliveryTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("configure replay Dispatcher: %v", err)
	}
	result, err := dispatcher.DispatchBatch(context.Background())
	if err != nil {
		t.Fatalf("dispatch manual replay: %v", err)
	}
	if result.Delivered != 1 || len(adapter.requests) != 1 ||
		adapter.requests[0].EventID != fixture.eventID {
		t.Fatalf("manual replay dispatch = %#v requests=%#v", result, adapter.requests)
	}
}

func TestWebhookDisableStopsFanoutAndReplay(t *testing.T) {
	fixture := newWebhookDispatchFixture(t, webhook.EventJobFailed, "https://hooks.example.com/disable")
	authPool := newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password")
	webhookRequestPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_webhook_request_login",
		"vela-webhook-request-password",
	)
	principal, err := identity.NewAuthenticator(authPool, testCredentialPepper).Authenticate(
		context.Background(), testBearerCredential(),
	)
	if err != nil {
		t.Fatalf("authenticate disable Principal: %v", err)
	}
	service, err := webhook.NewService(
		webhookRequestPool,
		fixture.sealer,
		publicWebhookAddressResolver(),
	)
	if err != nil {
		t.Fatalf("configure webhook service: %v", err)
	}
	var subscriptionID, deliveryID, jobID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT subscription_id, id, job_id FROM webhook_deliveries WHERE event_id = $1
	`, fixture.eventID).Scan(&subscriptionID, &deliveryID, &jobID); err != nil {
		t.Fatalf("read Webhook Delivery identity: %v", err)
	}
	disabled, err := service.Disable(
		context.Background(), principal, uuid.MustParse(testProjectID), subscriptionID,
	)
	if err != nil {
		t.Fatalf("disable Webhook Subscription: %v", err)
	}
	if disabled.State != webhook.SubscriptionDisabled || disabled.ID != subscriptionID {
		t.Fatalf("disabled Subscription = %#v", disabled)
	}
	var deliveryState string
	if err := fixture.database.Admin.QueryRow(`
		SELECT state::text FROM webhook_deliveries WHERE id = $1
	`, deliveryID).Scan(&deliveryState); err != nil {
		t.Fatalf("read disabled Subscription Delivery: %v", err)
	}
	if deliveryState != "DEAD_LETTER" {
		t.Fatalf("disabled Subscription Delivery state = %s", deliveryState)
	}

	_, err = service.Replay(
		context.Background(), principal, uuid.MustParse(testProjectID), subscriptionID, deliveryID,
	)
	var failure *webhook.Failure
	if err == nil || !errors.As(err, &failure) || failure.Code != webhook.FailureConflict {
		t.Fatalf("disabled replay error = %v, want conflict", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO outbox_events (
			event_id, organization_id, project_id, aggregate_type, aggregate_id,
			aggregate_version, event_type, schema_version, payload, occurred_at, available_at
		) VALUES ($1, $2, $3, 'Job', $4, 6, 'job.failed', 1, decode('00', 'hex'),
			clock_timestamp(), clock_timestamp())
	`, uuid.New(), testOrganizationID, testProjectID, jobID); err != nil {
		t.Fatalf("insert terminal event after disable: %v", err)
	}
	var deliveryCount int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*) FROM webhook_deliveries WHERE subscription_id = $1
	`, subscriptionID).Scan(&deliveryCount); err != nil {
		t.Fatalf("count disabled Subscription Deliveries: %v", err)
	}
	if deliveryCount != 1 {
		t.Fatalf("disabled Subscription Delivery count = %d, want 1", deliveryCount)
	}
}

func TestWebhookSecretRotationCreatesOneBoundedOverlap(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'webhooks:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant webhook management scope: %v", err)
	}
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	webhookRequestPool := newRolePool(
		t, database.DSN, "vela_webhook_request_login", "vela-webhook-request-password",
	)
	principal, err := identity.NewAuthenticator(authPool, testCredentialPepper).Authenticate(
		context.Background(), testBearerCredential(),
	)
	if err != nil {
		t.Fatalf("authenticate webhook Principal: %v", err)
	}
	sealer, err := webhook.NewAESGCMSealer("webhook-key-v1", map[string][]byte{
		"webhook-key-v1": []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatalf("configure webhook secret sealer: %v", err)
	}
	service, err := webhook.NewService(webhookRequestPool, sealer, publicWebhookAddressResolver())
	if err != nil {
		t.Fatalf("configure webhook service: %v", err)
	}
	created, err := service.Create(context.Background(), principal, uuid.MustParse(testProjectID), webhook.CreateRequest{
		Endpoint:   "https://hooks.example.com/rotation",
		EventTypes: []webhook.EventType{webhook.EventJobSucceeded},
	})
	if err != nil {
		t.Fatalf("create webhook Subscription: %v", err)
	}

	rotated, err := service.RotateSecret(
		context.Background(), principal, uuid.MustParse(testProjectID), created.Subscription.ID,
	)
	if err != nil {
		t.Fatalf("rotate webhook secret: %v", err)
	}
	if rotated.Subscription.SecretRevision != 2 || rotated.SigningSecret == created.SigningSecret ||
		rotated.PreviousSecretValidUntil.Before(time.Now().Add(23*time.Hour)) ||
		rotated.PreviousSecretValidUntil.After(time.Now().Add(25*time.Hour)) {
		t.Fatalf("rotation result = %#v", rotated)
	}
	var currentCount, overlappingCount int
	if err := database.Admin.QueryRow(`
		SELECT
			count(*) FILTER (WHERE valid_until IS NULL),
			count(*) FILTER (WHERE valid_until > clock_timestamp())
		FROM webhook_subscription_secrets
		WHERE subscription_id = $1
	`, created.Subscription.ID).Scan(&currentCount, &overlappingCount); err != nil {
		t.Fatalf("read rotated webhook secrets: %v", err)
	}
	if currentCount != 1 || overlappingCount != 1 {
		t.Fatalf("current secrets = %d overlapping previous = %d", currentCount, overlappingCount)
	}

	_, err = service.RotateSecret(
		context.Background(), principal, uuid.MustParse(testProjectID), created.Subscription.ID,
	)
	var failure *webhook.Failure
	if err == nil || !errors.As(err, &failure) || failure.Code != webhook.FailureConflict {
		t.Fatalf("second rotation error = %v, want conflict", err)
	}
	var secretCount int
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM webhook_subscription_secrets WHERE subscription_id = $1
	`, created.Subscription.ID).Scan(&secretCount); err != nil {
		t.Fatalf("count webhook secrets: %v", err)
	}
	if secretCount != 2 {
		t.Fatalf("secret count after rejected rotation = %d, want 2", secretCount)
	}

	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "webhook-rotation-overlap", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"prove current and previous webhook signing secrets"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode rotation overlap Job: %v", err)
	}
	eventID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO outbox_events (
			event_id, organization_id, project_id, aggregate_type, aggregate_id,
			aggregate_version, event_type, schema_version, payload, occurred_at, available_at
		) VALUES ($1, $2, $3, 'Job', $4, 7, 'job.succeeded', 1, decode('00', 'hex'),
			clock_timestamp(), clock_timestamp())
	`, eventID, testOrganizationID, testProjectID, job.JobID); err != nil {
		t.Fatalf("insert rotation overlap terminal event: %v", err)
	}
	webhookPool := newRolePool(
		t, database.DSN, "vela_webhook_login", "vela-webhook-password",
	)
	adapter := &recordingDeliveryAdapter{statusCode: http.StatusNoContent}
	dispatcher, err := webhook.NewDispatcher(
		webhookPool,
		sealer,
		adapter,
		webhook.DispatcherConfig{
			InstanceID:      "rotation-overlap-dispatcher",
			BatchSize:       1,
			ClaimTTL:        30 * time.Second,
			DeliveryTimeout: time.Second,
		},
	)
	if err != nil {
		t.Fatalf("configure rotation overlap Dispatcher: %v", err)
	}
	result, err := dispatcher.DispatchBatch(context.Background())
	if err != nil || result.Delivered != 1 || len(adapter.requests) != 1 {
		t.Fatalf("dispatch rotation overlap = %#v error=%v requests=%#v", result, err, adapter.requests)
	}
	request := adapter.requests[0]
	if len(request.Secrets) != 2 || string(request.Secrets[0]) != rotated.SigningSecret ||
		string(request.Secrets[1]) != created.SigningSecret {
		t.Fatalf("rotation overlap signing secrets = %#v", request.Secrets)
	}
	var signatureRevisions string
	if err := database.Admin.QueryRow(`
		SELECT attempt.signature_secret_revisions
		FROM webhook_delivery_attempts AS attempt
		JOIN webhook_deliveries AS delivery ON delivery.id = attempt.delivery_id
		WHERE delivery.event_id = $1
	`, eventID).Scan(&signatureRevisions); err != nil {
		t.Fatalf("read rotation overlap attempt receipt: %v", err)
	}
	if signatureRevisions != "{2,1}" {
		t.Fatalf("rotation overlap signature revisions = %s", signatureRevisions)
	}
}

func TestWebhookDeliveryListHidesAttemptErrorEvidence(t *testing.T) {
	fixture := newWebhookDispatchFixture(
		t,
		webhook.EventJobFailed,
		"https://hooks.example.com/safe-delivery-list",
	)
	if _, err := fixture.database.Admin.Exec(`
		UPDATE webhook_deliveries
		SET last_error = 'internal transport evidence must stay private'
		WHERE event_id = $1
	`, fixture.eventID); err != nil {
		t.Fatalf("seed private webhook error evidence: %v", err)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'webhooks:read')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant webhook read scope: %v", err)
	}
	authPool := newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password")
	webhookRequestPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_webhook_request_login",
		"vela-webhook-request-password",
	)
	principal, err := identity.NewAuthenticator(authPool, testCredentialPepper).Authenticate(
		context.Background(), testBearerCredential(),
	)
	if err != nil {
		t.Fatalf("authenticate webhook reader: %v", err)
	}
	service, err := webhook.NewService(
		webhookRequestPool,
		fixture.sealer,
		publicWebhookAddressResolver(),
	)
	if err != nil {
		t.Fatalf("configure webhook list service: %v", err)
	}
	var subscriptionID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT subscription_id FROM webhook_deliveries WHERE event_id = $1
	`, fixture.eventID).Scan(&subscriptionID); err != nil {
		t.Fatalf("read webhook Subscription identity: %v", err)
	}
	deliveries, err := service.ListDeliveries(
		context.Background(),
		principal,
		uuid.MustParse(testProjectID),
		subscriptionID,
		100,
	)
	if err != nil {
		t.Fatalf("list safe webhook Deliveries: %v", err)
	}
	if len(deliveries) != 1 || deliveries[0].LastError != nil {
		t.Fatalf("safe Delivery projection = %#v", deliveries)
	}
}

func TestWebhookDeliveryAttemptAndReplayEvidenceAreImmutable(t *testing.T) {
	fixture := newWebhookDispatchFixture(
		t,
		webhook.EventJobFailed,
		"https://hooks.example.com/immutable-evidence",
	)
	webhookPool := newRolePool(
		t, fixture.database.DSN, "vela_webhook_login", "vela-webhook-password",
	)
	dispatcher, err := webhook.NewDispatcher(
		webhookPool,
		fixture.sealer,
		&recordingDeliveryAdapter{statusCode: http.StatusNoContent},
		webhook.DispatcherConfig{
			InstanceID:      "immutable-evidence-dispatcher",
			BatchSize:       1,
			ClaimTTL:        30 * time.Second,
			DeliveryTimeout: time.Second,
		},
	)
	if err != nil {
		t.Fatalf("configure immutable-evidence Dispatcher: %v", err)
	}
	if result, err := dispatcher.DispatchBatch(context.Background()); err != nil || result.Delivered != 1 {
		t.Fatalf("deliver immutable-evidence fixture = %#v error=%v", result, err)
	}

	authPool := newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password")
	webhookRequestPool := newRolePool(
		t,
		fixture.database.DSN,
		"vela_webhook_request_login",
		"vela-webhook-request-password",
	)
	principal, err := identity.NewAuthenticator(authPool, testCredentialPepper).Authenticate(
		context.Background(), testBearerCredential(),
	)
	if err != nil {
		t.Fatalf("authenticate immutable-evidence Principal: %v", err)
	}
	service, err := webhook.NewService(
		webhookRequestPool,
		fixture.sealer,
		publicWebhookAddressResolver(),
	)
	if err != nil {
		t.Fatalf("configure immutable-evidence service: %v", err)
	}
	var subscriptionID, deliveryID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT subscription_id, id FROM webhook_deliveries WHERE event_id = $1
	`, fixture.eventID).Scan(&subscriptionID, &deliveryID); err != nil {
		t.Fatalf("read immutable webhook evidence identity: %v", err)
	}
	if _, err := service.Replay(
		context.Background(),
		principal,
		uuid.MustParse(testProjectID),
		subscriptionID,
		deliveryID,
	); err != nil {
		t.Fatalf("create immutable replay receipt: %v", err)
	}

	for name, mutation := range map[string]struct {
		statement string
		args      []any
	}{
		"Delivery payload update": {
			statement: `UPDATE webhook_deliveries SET payload = '{"schema_version":1}'::jsonb WHERE id = $1`,
			args:      []any{deliveryID},
		},
		"Delivery delete": {
			statement: `DELETE FROM webhook_deliveries WHERE id = $1`, args: []any{deliveryID},
		},
		"attempt update": {
			statement: `UPDATE webhook_delivery_attempts SET error = 'forged' WHERE delivery_id = $1`,
			args:      []any{deliveryID},
		},
		"attempt delete": {
			statement: `DELETE FROM webhook_delivery_attempts WHERE delivery_id = $1`, args: []any{deliveryID},
		},
		"attempt truncate": {statement: `TRUNCATE webhook_delivery_attempts`},
		"replay update": {
			statement: `UPDATE webhook_delivery_replays SET requested_at = clock_timestamp() WHERE delivery_id = $1`,
			args:      []any{deliveryID},
		},
		"replay delete": {
			statement: `DELETE FROM webhook_delivery_replays WHERE delivery_id = $1`, args: []any{deliveryID},
		},
		"replay truncate": {statement: `TRUNCATE webhook_delivery_replays`},
	} {
		t.Run(name, func(t *testing.T) {
			_, mutationErr := fixture.database.Admin.Exec(mutation.statement, mutation.args...)
			if mutationErr == nil || !strings.Contains(mutationErr.Error(), "immutable") {
				t.Fatalf("immutable webhook evidence accepted %q: %v", mutation.statement, mutationErr)
			}
		})
	}
}

func TestWebhookCreatePersistsOnlyCiphertextAndIsProjectScoped(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'webhooks:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant webhook management scope: %v", err)
	}

	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	webhookRequestPool := newRolePool(
		t, database.DSN, "vela_webhook_request_login", "vela-webhook-request-password",
	)
	principal, err := identity.NewAuthenticator(authPool, testCredentialPepper).Authenticate(
		context.Background(),
		testBearerCredential(),
	)
	if err != nil {
		t.Fatalf("authenticate webhook Principal: %v", err)
	}
	sealer, err := webhook.NewAESGCMSealer("webhook-key-v1", map[string][]byte{
		"webhook-key-v1": []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatalf("configure webhook secret sealer: %v", err)
	}
	service, err := webhook.NewService(webhookRequestPool, sealer, publicWebhookAddressResolver())
	if err != nil {
		t.Fatalf("configure webhook service: %v", err)
	}

	created, err := service.Create(context.Background(), principal, uuid.MustParse(testProjectID), webhook.CreateRequest{
		Endpoint: "https://hooks.example.com/vela",
		EventTypes: []webhook.EventType{
			webhook.EventJobSucceeded,
			webhook.EventJobFailed,
		},
	})
	if err != nil {
		t.Fatalf("create webhook Subscription: %v", err)
	}
	if created.Subscription.ID == uuid.Nil || created.Subscription.ProjectID.String() != testProjectID ||
		created.Subscription.State != webhook.SubscriptionActive ||
		len(created.SigningSecret) < len("vwhsec_")+32 {
		t.Fatalf("created Subscription = %#v", created)
	}

	var keyID string
	var nonce, ciphertext []byte
	if err := database.Admin.QueryRow(`
		SELECT encryption_key_id, encryption_nonce, encrypted_secret
		FROM webhook_subscription_secrets
		WHERE subscription_id = $1
	`, created.Subscription.ID).Scan(&keyID, &nonce, &ciphertext); err != nil {
		t.Fatalf("read encrypted signing secret: %v", err)
	}
	if keyID != "webhook-key-v1" || len(nonce) != 12 ||
		bytes.Contains(ciphertext, []byte(created.SigningSecret)) {
		t.Fatalf("stored secret key=%q nonce=%d ciphertext=%x", keyID, len(nonce), ciphertext)
	}

	_, err = service.Create(
		context.Background(),
		principal,
		uuid.MustParse(testProjectTwoID),
		webhook.CreateRequest{
			Endpoint:   "https://hooks.example.com/other-project",
			EventTypes: []webhook.EventType{webhook.EventJobCanceled},
		},
	)
	var failure *webhook.Failure
	if err == nil || !errors.As(err, &failure) || failure.Code != webhook.FailureForbidden {
		t.Fatalf("cross-Project create error = %v, want forbidden", err)
	}
}

type recordingDeliveryAdapter struct {
	statusCode int
	err        error
	requests   []webhook.DeliveryRequest
}

type blockingFirstDeliveryAdapter struct {
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	calls   int
}

type deadlineRecordingDeliveryAdapter struct {
	hasDeadline bool
	remaining   time.Duration
}

func (a *deadlineRecordingDeliveryAdapter) Deliver(
	ctx context.Context,
	_ webhook.DeliveryRequest,
) (int, error) {
	deadline, ok := ctx.Deadline()
	a.hasDeadline = ok
	if ok {
		a.remaining = time.Until(deadline)
	}
	return http.StatusNoContent, nil
}

func newBlockingFirstDeliveryAdapter() *blockingFirstDeliveryAdapter {
	return &blockingFirstDeliveryAdapter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (a *blockingFirstDeliveryAdapter) Deliver(
	ctx context.Context,
	_ webhook.DeliveryRequest,
) (int, error) {
	a.mu.Lock()
	a.calls++
	call := a.calls
	a.mu.Unlock()
	if call == 1 {
		close(a.started)
		select {
		case <-a.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return http.StatusNoContent, nil
}

func TestWebhookMigrationEmptyDownUpRestoresDefaultSurface(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 10); err != nil {
		t.Fatalf("contract empty webhook migration: %v", err)
	}
	for _, table := range []string{
		"webhook_subscriptions",
		"webhook_subscription_secrets",
		"webhook_deliveries",
		"webhook_delivery_attempts",
		"webhook_delivery_replays",
	} {
		assertTableDoesNotExist(t, database.Admin, table)
	}
	if err := goose.Up(database.Admin, migrations); err != nil {
		t.Fatalf("re-expand empty webhook migration: %v", err)
	}
	for _, table := range []string{
		"webhook_subscriptions",
		"webhook_subscription_secrets",
		"webhook_deliveries",
		"webhook_delivery_attempts",
		"webhook_delivery_replays",
	} {
		assertTableExists(t, database.Admin, table)
	}
	webhookRequestPool := webhookRequestPoolForDatabase(t, database)
	if err := veladb.VerifyRole(
		context.Background(), webhookRequestPool, veladb.RoleWebhookRequest,
	); err != nil {
		t.Fatalf("verify re-expanded webhook request role: %v", err)
	}
	webhookPool := newRolePool(
		t, database.DSN, "vela_webhook_login", "vela-webhook-password",
	)
	if err := veladb.VerifyRole(context.Background(), webhookPool, veladb.RoleWebhook); err != nil {
		t.Fatalf("verify re-expanded webhook Dispatcher role: %v", err)
	}

	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'webhooks:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant webhook scope after migration re-expansion: %v", err)
	}
	principal, err := identity.NewAuthenticator(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate after webhook migration re-expansion: %v", err)
	}
	created, err := testWebhookService(t, webhookRequestPool).Create(
		context.Background(),
		principal,
		uuid.MustParse(testProjectID),
		webhook.CreateRequest{
			Endpoint:   "https://hooks.example.com/down-up",
			EventTypes: []webhook.EventType{webhook.EventJobSucceeded},
		},
	)
	if err != nil || created.Subscription.ID == uuid.Nil || created.SigningSecret == "" {
		t.Fatalf("create Subscription after migration re-expansion = %#v error=%v", created, err)
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 14 {
		t.Fatalf("webhook migration version after Down/Up = %d error=%v", version, err)
	}
}

func TestWebhookMigrationDownRefusesDurableEvidence(t *testing.T) {
	t.Run("Subscription and secret", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		seedAdmissionFixture(t, database.Admin)
		if _, err := database.Admin.Exec(`
			UPDATE credentials
			SET scopes = array_append(scopes, 'webhooks:manage')
			WHERE id = $1
		`, testCredentialID); err != nil {
			t.Fatalf("grant webhook scope for migration Down refusal: %v", err)
		}
		principal, err := identity.NewAuthenticator(
			newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
			testCredentialPepper,
		).Authenticate(context.Background(), testBearerCredential())
		if err != nil {
			t.Fatalf("authenticate migration Down refusal Principal: %v", err)
		}
		if _, err := testWebhookService(
			t, webhookRequestPoolForDatabase(t, database),
		).Create(
			context.Background(),
			principal,
			uuid.MustParse(testProjectID),
			webhook.CreateRequest{
				Endpoint:   "https://hooks.example.com/down-refusal-subscription",
				EventTypes: []webhook.EventType{webhook.EventJobFailed},
			},
		); err != nil {
			t.Fatalf("create durable webhook Subscription: %v", err)
		}
		assertWebhookMigrationDownRefused(t, database)
	})

	t.Run("Delivery attempt and replay", func(t *testing.T) {
		fixture := newWebhookDispatchFixture(
			t,
			webhook.EventJobFailed,
			"https://hooks.example.com/down-refusal-replay",
		)
		webhookPool := newRolePool(
			t, fixture.database.DSN, "vela_webhook_login", "vela-webhook-password",
		)
		dispatcher, err := webhook.NewDispatcher(
			webhookPool,
			fixture.sealer,
			&recordingDeliveryAdapter{statusCode: http.StatusNoContent},
			webhook.DispatcherConfig{
				InstanceID:      "migration-down-refusal-dispatcher",
				BatchSize:       1,
				ClaimTTL:        30 * time.Second,
				DeliveryTimeout: time.Second,
			},
		)
		if err != nil {
			t.Fatalf("configure migration Down refusal Dispatcher: %v", err)
		}
		if result, err := dispatcher.DispatchBatch(context.Background()); err != nil ||
			result.Delivered != 1 {
			t.Fatalf("create durable webhook attempt = %#v error=%v", result, err)
		}
		principal, err := identity.NewAuthenticator(
			newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
			testCredentialPepper,
		).Authenticate(context.Background(), testBearerCredential())
		if err != nil {
			t.Fatalf("authenticate replay receipt Principal: %v", err)
		}
		var subscriptionID, deliveryID uuid.UUID
		if err := fixture.database.Admin.QueryRow(`
			SELECT subscription_id, id
			FROM webhook_deliveries
			WHERE event_id = $1
		`, fixture.eventID).Scan(&subscriptionID, &deliveryID); err != nil {
			t.Fatalf("read migration Down refusal Delivery: %v", err)
		}
		service, err := webhook.NewService(
			webhookRequestPoolForDatabase(t, fixture.database),
			fixture.sealer,
			publicWebhookAddressResolver(),
		)
		if err != nil {
			t.Fatalf("configure migration Down refusal service: %v", err)
		}
		if _, err := service.Replay(
			context.Background(),
			principal,
			uuid.MustParse(testProjectID),
			subscriptionID,
			deliveryID,
		); err != nil {
			t.Fatalf("create durable webhook replay receipt: %v", err)
		}
		assertWebhookMigrationDownRefused(t, fixture.database)
	})
}

func TestWebhookMigrationDownSerializesBeforeEvidenceCheck(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'webhooks:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant concurrent webhook evidence scope: %v", err)
	}
	if _, err := database.Admin.Exec(`
		CREATE FUNCTION vela_test_pause_webhook_subscription_insert() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock(580011);
			RETURN NEW;
		END
		$$;
		CREATE TRIGGER vela_test_pause_webhook_subscription_insert
		BEFORE INSERT ON webhook_subscriptions
		FOR EACH ROW EXECUTE FUNCTION vela_test_pause_webhook_subscription_insert();
	`); err != nil {
		t.Fatalf("install concurrent webhook evidence pause trigger: %v", err)
	}
	principal, err := identity.NewAuthenticator(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate concurrent webhook evidence Principal: %v", err)
	}
	service := testWebhookService(t, webhookRequestPoolForDatabase(t, database))
	const advisoryLockKey int64 = 580011
	blocker, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin concurrent webhook evidence blocker: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	if _, err := blocker.Exec("SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		t.Fatalf("acquire concurrent webhook evidence blocker: %v", err)
	}

	createErrors := make(chan error, 1)
	go func() {
		_, createErr := service.Create(
			context.Background(),
			principal,
			uuid.MustParse(testProjectID),
			webhook.CreateRequest{
				Endpoint:   "https://hooks.example.com/concurrent-down",
				EventTypes: []webhook.EventType{webhook.EventJobFailed},
			},
		)
		createErrors <- createErr
	}()
	waitForRoleDatabaseLock(t, database.Admin, "vela_webhook_request_login")

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	downErrors := make(chan error, 1)
	go func() {
		downErrors <- goose.DownTo(database.Admin, migrations, 10)
	}()
	waitForRoleDatabaseLock(t, database.Admin, "postgres")
	var unlocked bool
	if err := blocker.QueryRow("SELECT pg_advisory_unlock($1)", advisoryLockKey).Scan(&unlocked); err != nil {
		t.Fatalf("release concurrent webhook evidence blocker: %v", err)
	}
	if !unlocked {
		t.Fatal("concurrent webhook evidence blocker was not held")
	}
	if err := blocker.Commit(); err != nil {
		t.Fatalf("commit concurrent webhook evidence blocker: %v", err)
	}
	select {
	case createErr := <-createErrors:
		if createErr != nil {
			t.Fatalf("write concurrent webhook evidence: %v", createErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent webhook evidence writer did not finish")
	}

	select {
	case downErr := <-downErrors:
		var postgresError *pgconn.PgError
		if !errors.As(downErr, &postgresError) || postgresError.Code != "55000" ||
			postgresError.ConstraintName != "project_webhook_contract_has_durable_evidence" {
			t.Fatalf(
				"concurrent webhook migration Down error = %v, want durable-evidence refusal",
				downErr,
			)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent webhook migration Down did not finish")
	}
	version, err := goose.GetDBVersion(database.Admin)
	if err != nil || version != 11 {
		t.Fatalf("migration version after concurrent evidence = %d error=%v", version, err)
	}
	var subscriptions int
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM webhook_subscriptions
	`).Scan(&subscriptions); err != nil {
		t.Fatalf("read preserved concurrent webhook evidence: %v", err)
	}
	if subscriptions != 1 {
		t.Fatalf("preserved concurrent webhook Subscriptions = %d, want 1", subscriptions)
	}
}

func assertWebhookMigrationDownRefused(t *testing.T, database testDatabase) {
	t.Helper()
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err := goose.DownTo(database.Admin, migrations, 10)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) || postgresError.Code != "55000" ||
		!strings.Contains(postgresError.Message, "durable evidence") {
		t.Fatalf("webhook migration Down with durable evidence error = %v", err)
	}
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil || version != 11 {
		t.Fatalf("webhook migration version after refused Down = %d error=%v", version, versionErr)
	}
	assertTableExists(t, database.Admin, "webhook_subscriptions")
}

type webhookConcurrencyFixture struct {
	principal      identity.Principal
	service        *webhook.Service
	webhookPool    *pgxpool.Pool
	subscriptionID uuid.UUID
	deliveryID     uuid.UUID
}

type webhookClaimCall struct {
	claimed int
	err     error
}

func newWebhookConcurrencyFixture(
	t *testing.T,
	fixture webhookDispatchFixture,
) webhookConcurrencyFixture {
	t.Helper()
	principal, err := identity.NewAuthenticator(
		newRolePool(t, fixture.database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate webhook concurrency Principal: %v", err)
	}
	service, err := webhook.NewService(
		webhookRequestPoolForDatabase(t, fixture.database),
		fixture.sealer,
		publicWebhookAddressResolver(),
	)
	if err != nil {
		t.Fatalf("configure webhook concurrency service: %v", err)
	}
	var subscriptionID, deliveryID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`
		SELECT subscription_id, id
		FROM webhook_deliveries
		WHERE event_id = $1
	`, fixture.eventID).Scan(&subscriptionID, &deliveryID); err != nil {
		t.Fatalf("read webhook concurrency identities: %v", err)
	}
	return webhookConcurrencyFixture{
		principal: principal,
		service:   service,
		webhookPool: newRolePool(
			t,
			fixture.database.DSN,
			"vela_webhook_login",
			"vela-webhook-password",
		),
		subscriptionID: subscriptionID,
		deliveryID:     deliveryID,
	}
}

func beginWebhookAdvisoryBlocker(t *testing.T, database *sql.DB, key int64) *sql.Tx {
	t.Helper()
	blocker, err := database.Begin()
	if err != nil {
		t.Fatalf("begin webhook advisory-lock blocker: %v", err)
	}
	if _, err := blocker.Exec("SELECT pg_advisory_lock($1)", key); err != nil {
		_ = blocker.Rollback()
		t.Fatalf("acquire webhook advisory-lock blocker: %v", err)
	}
	return blocker
}

func releaseWebhookAdvisoryBlocker(t *testing.T, blocker *sql.Tx, key int64) {
	t.Helper()
	var unlocked bool
	if err := blocker.QueryRow("SELECT pg_advisory_unlock($1)", key).Scan(&unlocked); err != nil {
		t.Fatalf("release webhook advisory-lock blocker: %v", err)
	}
	if !unlocked {
		t.Fatal("webhook advisory-lock blocker was not held")
	}
	if err := blocker.Commit(); err != nil {
		t.Fatalf("commit webhook advisory-lock blocker: %v", err)
	}
}

func claimWebhookDeliveryCount(
	ctx context.Context,
	pool *pgxpool.Pool,
	instanceID string,
) (int, error) {
	rows, err := pool.Query(ctx, `
		SELECT delivery_id
		FROM vela_claim_webhook_deliveries($1, 30, 1)
	`, instanceID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	claimed := 0
	for rows.Next() {
		var deliveryID uuid.UUID
		if err := rows.Scan(&deliveryID); err != nil {
			return 0, err
		}
		claimed++
	}
	return claimed, rows.Err()
}

type webhookDispatchFixture struct {
	database testDatabase
	sealer   *webhook.AESGCMSealer
	eventID  uuid.UUID
}

func newWebhookDispatchFixture(
	t *testing.T,
	eventType webhook.EventType,
	endpoint string,
) webhookDispatchFixture {
	t.Helper()
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = array_append(scopes, 'webhooks:manage')
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant webhook management scope: %v", err)
	}
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	webhookRequestPool := newRolePool(
		t, database.DSN, "vela_webhook_request_login", "vela-webhook-request-password",
	)
	principal, err := identity.NewAuthenticator(authPool, testCredentialPepper).Authenticate(
		context.Background(), testBearerCredential(),
	)
	if err != nil {
		t.Fatalf("authenticate webhook Principal: %v", err)
	}
	sealer, err := webhook.NewAESGCMSealer("webhook-key-v1", map[string][]byte{
		"webhook-key-v1": []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatalf("configure webhook secret sealer: %v", err)
	}
	service, err := webhook.NewService(webhookRequestPool, sealer, publicWebhookAddressResolver())
	if err != nil {
		t.Fatalf("configure webhook service: %v", err)
	}
	if _, err := service.Create(context.Background(), principal, uuid.MustParse(testProjectID), webhook.CreateRequest{
		Endpoint: endpoint, EventTypes: []webhook.EventType{eventType},
	}); err != nil {
		t.Fatalf("create webhook Subscription: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "webhook-dispatch-fixture-"+uuid.NewString(), []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"Webhook Delivery fixture"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Accepted Job: %v", err)
	}
	eventID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO outbox_events (
			event_id, organization_id, project_id, aggregate_type, aggregate_id,
			aggregate_version, event_type, schema_version, payload, occurred_at, available_at
		) VALUES ($1, $2, $3, 'Job', $4, 5, $5, 1, decode('00', 'hex'),
			clock_timestamp(), clock_timestamp())
	`, eventID, testOrganizationID, testProjectID, job.JobID, string(eventType)); err != nil {
		t.Fatalf("insert terminal Outbox fixture: %v", err)
	}
	return webhookDispatchFixture{database: database, sealer: sealer, eventID: eventID}
}

func newWebhookHTTPServer(t *testing.T, database testDatabase) *httptest.Server {
	t.Helper()
	return newWebhookHTTPServerWithResolver(t, database, publicWebhookAddressResolver())
}

func newWebhookHTTPServerWithResolver(
	t *testing.T,
	database testDatabase,
	resolver webhook.AddressResolver,
) *httptest.Server {
	t.Helper()
	authPool := newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password")
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")
	webhookRequestPool := newRolePool(
		t, database.DSN, "vela_webhook_request_login", "vela-webhook-request-password",
	)
	artifactPool := newRolePool(
		t, database.DSN, "vela_artifact_request_login", "vela-artifact-request-password",
	)
	cancelPool := newRolePool(t, database.DSN, "vela_cancel_login", "vela-cancel-password")
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	handler, err := httpapi.NewHandler(httpapi.Config{
		Authenticator:          identity.NewAuthenticator(authPool, testCredentialPepper),
		IdentityAdministration: &identity.AdministrationService{},
		Admission:              admission.NewLegacyService(requestPool),
		Cancellation:           cancellation.NewService(cancelPool, internalPool),
		Artifacts:              testArtifactAccessService(artifactPool),
		Webhooks:               testWebhookService(t, webhookRequestPool, resolver),
	})
	if err != nil {
		t.Fatalf("create webhook HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func testWebhookService(
	t *testing.T,
	webhookRequestPool *pgxpool.Pool,
	resolvers ...webhook.AddressResolver,
) *webhook.Service {
	t.Helper()
	resolver := publicWebhookAddressResolver()
	if len(resolvers) == 1 {
		resolver = resolvers[0]
	} else if len(resolvers) > 1 {
		t.Fatalf("configure test webhook service: got %d DNS resolvers, want at most one", len(resolvers))
	}
	sealer, err := webhook.NewAESGCMSealer("webhook-key-v1", map[string][]byte{
		"webhook-key-v1": []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatalf("configure test webhook secret sealer: %v", err)
	}
	service, err := webhook.NewService(webhookRequestPool, sealer, resolver)
	if err != nil {
		t.Fatalf("configure test webhook service: %v", err)
	}
	return service
}

type staticWebhookAddressResolver struct {
	addresses []netip.Addr
	err       error
}

func (r staticWebhookAddressResolver) LookupNetIP(
	context.Context,
	string,
	string,
) ([]netip.Addr, error) {
	return append([]netip.Addr(nil), r.addresses...), r.err
}

func publicWebhookAddressResolver() webhook.AddressResolver {
	return staticWebhookAddressResolver{
		addresses: []netip.Addr{netip.MustParseAddr("93.184.216.34")},
	}
}

func webhookRequestPoolForDatabase(t *testing.T, database testDatabase) *pgxpool.Pool {
	t.Helper()
	return newRolePool(
		t,
		database.DSN,
		"vela_webhook_request_login",
		"vela-webhook-request-password",
	)
}

func createTerminalWebhookSubscription(
	t *testing.T,
	database testDatabase,
	eventType string,
) uuid.UUID {
	t.Helper()
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = CASE
			WHEN 'webhooks:manage' = ANY(scopes) THEN scopes
			ELSE array_append(scopes, 'webhooks:manage')
		END
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant webhook scope for terminal %s: %v", eventType, err)
	}
	principal, err := identity.NewAuthenticator(
		newRolePool(t, database.DSN, "vela_auth_login", "vela-auth-password"),
		testCredentialPepper,
	).Authenticate(context.Background(), testBearerCredential())
	if err != nil {
		t.Fatalf("authenticate terminal %s webhook Principal: %v", eventType, err)
	}
	created, err := testWebhookService(
		t, webhookRequestPoolForDatabase(t, database),
	).Create(
		context.Background(),
		principal,
		uuid.MustParse(testProjectID),
		webhook.CreateRequest{
			Endpoint:   "https://hooks.example.com/terminal-" + strings.TrimPrefix(eventType, "job."),
			EventTypes: []webhook.EventType{webhook.EventType(eventType)},
		},
	)
	if err != nil {
		t.Fatalf("create terminal %s webhook Subscription: %v", eventType, err)
	}
	return created.Subscription.ID
}

func assertTerminalWebhookDelivery(
	t *testing.T,
	database testDatabase,
	subscriptionID, jobID uuid.UUID,
	eventType, jobState string,
) {
	t.Helper()
	var count int
	var actualEventType, actualJobState, deliveryState string
	var actualJobID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM webhook_deliveries WHERE subscription_id = $1),
			delivery.event_type,
			delivery.job_id,
			delivery.state::text,
			delivery.payload->>'job_state'
		FROM webhook_deliveries AS delivery
		WHERE delivery.subscription_id = $1
		LIMIT 1
	`, subscriptionID).Scan(
		&count,
		&actualEventType,
		&actualJobID,
		&deliveryState,
		&actualJobState,
	); err != nil {
		t.Fatalf("read terminal %s webhook Delivery: %v", eventType, err)
	}
	if count != 1 || actualEventType != eventType || actualJobID != jobID ||
		deliveryState != "PENDING" || actualJobState != jobState {
		t.Fatalf(
			"terminal %s Delivery = count %d event %s job %s state %s payload state %s",
			eventType,
			count,
			actualEventType,
			actualJobID,
			deliveryState,
			actualJobState,
		)
	}
}

func webhookJSONRequest(t *testing.T, method, url, body string) (int, []byte) {
	t.Helper()
	request, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("create webhook HTTP request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+testBearerCredential())
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("perform webhook HTTP request: %v", err)
	}
	responseBody, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatalf("read webhook HTTP response: %v", readErr)
	}
	return response.StatusCode, responseBody
}

func (a *recordingDeliveryAdapter) Deliver(
	_ context.Context,
	request webhook.DeliveryRequest,
) (int, error) {
	copyOfRequest := request
	copyOfRequest.Payload = append([]byte(nil), request.Payload...)
	copyOfRequest.Secrets = make([][]byte, len(request.Secrets))
	for index, secret := range request.Secrets {
		copyOfRequest.Secrets[index] = append([]byte(nil), secret...)
	}
	a.requests = append(a.requests, copyOfRequest)
	return a.statusCode, a.err
}

type cancelingDeliveryAdapter struct {
	cancel context.CancelFunc
}

func (a *cancelingDeliveryAdapter) Deliver(
	_ context.Context,
	_ webhook.DeliveryRequest,
) (int, error) {
	a.cancel()
	return 0, context.Canceled
}
