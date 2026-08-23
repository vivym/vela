//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/inbox"
	"github.com/vivym/vela/internal/outbox"
)

func TestOutboxPublisherRetriesWithStableEventIDAndRecordsAcknowledgement(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
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
		Webhooks:               testWebhookService(t, webhookRequestPool),
	})
	if err != nil {
		t.Fatalf("create HTTP handler: %v", err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	accepted := submitJob(t, server.URL, "outbox-publish", []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"publish exactly the durable event intent"
    }`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}

	broker := &recordingBroker{loseFirstAcknowledgement: true}
	publisher, err := outbox.NewPublisher(internalPool, broker, outbox.Config{
		InstanceID: "integration-publisher",
		BatchSize:  10,
		ClaimTTL:   30 * time.Second,
		RetryDelay: 0,
	})
	if err != nil {
		t.Fatalf("create Outbox Publisher: %v", err)
	}
	if _, err := publisher.PublishBatch(context.Background()); err == nil {
		t.Fatal("first publish unexpectedly received an acknowledgement")
	}
	if published, err := publisher.PublishBatch(context.Background()); err != nil || published != 1 {
		t.Fatalf("retry publish = %d, %v; want 1, nil", published, err)
	}

	calls := broker.Calls()
	if len(calls) != 2 {
		t.Fatalf("Broker calls = %d, want 2", len(calls))
	}
	if calls[0].MessageID == "" || calls[0].MessageID != calls[1].MessageID {
		t.Fatalf("message ids = %q and %q, want one stable event id", calls[0].MessageID, calls[1].MessageID)
	}
	if calls[0].Subject != "vela.events.job.ready" || calls[1].Subject != calls[0].Subject {
		t.Fatalf("subjects = %q and %q", calls[0].Subject, calls[1].Subject)
	}
	if !bytes.Equal(calls[0].Payload, calls[1].Payload) {
		t.Fatal("Outbox retry changed the event payload")
	}

	var eventID, brokerStream string
	var brokerSequence int64
	var publishAttempts int
	var published, unclaimed bool
	if err := database.Admin.QueryRow(`
        SELECT
            event_id::text,
            broker_stream,
            broker_sequence,
            publish_attempts,
            published_at IS NOT NULL,
            claimed_by IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL
        FROM outbox_events
    `).Scan(
		&eventID,
		&brokerStream,
		&brokerSequence,
		&publishAttempts,
		&published,
		&unclaimed,
	); err != nil {
		t.Fatalf("read Outbox publish receipt: %v", err)
	}
	if eventID != calls[0].MessageID || brokerStream != "VELA_EVENTS" || brokerSequence != 42 {
		t.Fatalf("publish receipt = %s/%s/%d", eventID, brokerStream, brokerSequence)
	}
	if publishAttempts != 2 || !published || !unclaimed {
		t.Fatalf("Outbox state = attempts %d, published %v, unclaimed %v", publishAttempts, published, unclaimed)
	}
}

func TestJetStreamBrokerUsesEventIDForDeduplication(t *testing.T) {
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "nats:2.12-alpine",
			ExposedPorts: []string{"4222/tcp"},
			Cmd:          []string{"-js", "-sd", "/data"},
			WaitingFor: wait.ForLog("Server is ready").
				WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start NATS JetStream: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Errorf("terminate NATS JetStream: %v", err)
		}
	})
	endpoint, err := container.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("NATS endpoint: %v", err)
	}
	connection, err := nats.Connect("nats://" + endpoint)
	if err != nil {
		t.Fatalf("connect NATS: %v", err)
	}
	t.Cleanup(connection.Close)
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("create JetStream client: %v", err)
	}
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:       "VELA_EVENTS",
		Subjects:   []string{"vela.events.>"},
		Storage:    jetstream.FileStorage,
		Duplicates: time.Minute,
	})
	if err != nil {
		t.Fatalf("create VELA_EVENTS stream: %v", err)
	}

	broker, err := outbox.NewJetStreamBroker(connection)
	if err != nil {
		t.Fatalf("create JetStream Broker: %v", err)
	}
	const eventID = "00000000-0000-0000-0000-000000000301"
	first, err := broker.Publish(ctx, "vela.events.job.ready", eventID, []byte("event-payload"))
	if err != nil {
		t.Fatalf("first JetStream publish: %v", err)
	}
	duplicate, err := broker.Publish(ctx, "vela.events.job.ready", eventID, []byte("event-payload"))
	if err != nil {
		t.Fatalf("duplicate JetStream publish: %v", err)
	}
	if first.Stream != "VELA_EVENTS" || duplicate.Stream != first.Stream || duplicate.Sequence != first.Sequence {
		t.Fatalf("JetStream receipts = %#v and %#v", first, duplicate)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("read VELA_EVENTS stream info: %v", err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("JetStream stored %d messages, want 1 deduplicated event", info.State.Msgs)
	}
}

func TestInboxReceiptAppliesAggregateTransitionOnce(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "inbox-transition", []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"apply this aggregate transition once"
    }`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}

	var event inbox.Event
	if err := database.Admin.QueryRow(`
        SELECT event_id, organization_id, project_id, aggregate_type,
            aggregate_id, aggregate_version, event_type
        FROM outbox_events
    `).Scan(
		&event.ID,
		&event.OrganizationID,
		&event.ProjectID,
		&event.AggregateType,
		&event.AggregateID,
		&event.AggregateVersion,
		&event.Type,
	); err != nil {
		t.Fatalf("read source event: %v", err)
	}
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	processor, err := inbox.NewProcessor(internalPool, "scheduler")
	if err != nil {
		t.Fatalf("create Inbox processor: %v", err)
	}
	transition := func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE jobs SET version = version + 1 WHERE id = $1`, event.AggregateID)
		return err
	}
	applied, err := processor.ProcessOnce(context.Background(), event, transition)
	if err != nil || !applied {
		t.Fatalf("first Inbox processing = %v, %v; want true, nil", applied, err)
	}
	applied, err = processor.ProcessOnce(context.Background(), event, func(context.Context, pgx.Tx) error {
		t.Fatal("duplicate event invoked aggregate transition")
		return nil
	})
	if err != nil || applied {
		t.Fatalf("duplicate Inbox processing = %v, %v; want false, nil", applied, err)
	}

	duplicateVersion := event
	duplicateVersion.ID = uuid.New()
	applied, err = processor.ProcessOnce(context.Background(), duplicateVersion, func(context.Context, pgx.Tx) error {
		t.Fatal("duplicate aggregate version invoked aggregate transition")
		return nil
	})
	if err != nil || applied {
		t.Fatalf("duplicate aggregate version = %v, %v; want false, nil", applied, err)
	}

	var version int64
	if err := database.Admin.QueryRow("SELECT version FROM jobs WHERE id = $1", event.AggregateID).Scan(&version); err != nil {
		t.Fatalf("read transitioned Job version: %v", err)
	}
	if version != 2 {
		t.Fatalf("Job version = %d, want exactly one transition to version 2", version)
	}
}

type brokerCall struct {
	Subject   string
	MessageID string
	Payload   []byte
}

type recordingBroker struct {
	mutex                    sync.Mutex
	loseFirstAcknowledgement bool
	calls                    []brokerCall
}

func (b *recordingBroker) Publish(
	_ context.Context,
	subject string,
	messageID string,
	payload []byte,
) (outbox.Receipt, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.calls = append(b.calls, brokerCall{
		Subject: subject, MessageID: messageID, Payload: append([]byte(nil), payload...),
	})
	if b.loseFirstAcknowledgement && len(b.calls) == 1 {
		return outbox.Receipt{}, errors.New("simulated PubAck timeout after Broker acceptance")
	}
	return outbox.Receipt{Stream: "VELA_EVENTS", Sequence: 42}, nil
}

func (b *recordingBroker) Calls() []brokerCall {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return append([]brokerCall(nil), b.calls...)
}
