//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/eventstream"
	"github.com/vivym/vela/internal/httpapi"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/inbox"
	"github.com/vivym/vela/internal/natsauth"
	"github.com/vivym/vela/internal/organizationreporting"
	"github.com/vivym/vela/internal/outbox"
	"github.com/vivym/vela/internal/retention"
)

func TestOutboxPublisherRetriesWithStableEventIDAndRecordsAcknowledgement(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
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
		OrganizationReporting:  &organizationreporting.Service{},
		Retention:              &retention.Service{},
		Admission:              admission.NewService(requestPool),
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

func TestOutboxPublisherRecoversCrashAfterPubAckBeforeDatabaseMarker(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "puback-before-marker-crash", []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"retain the Outbox row after PubAck and before its marker"
    }`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}

	ctx := context.Background()
	fixture := startAuthenticatedNATS(t)
	bootstrap, bootstrapErrors := fixture.connectCredential(t, fixture.bootstrapCredential, true)
	t.Cleanup(bootstrap.Close)
	js, err := jetstream.New(bootstrap)
	if err != nil {
		t.Fatalf("create authenticated bootstrap JetStream client: %v", err)
	}
	streamConfig := eventstream.StreamConfig()
	streamConfig.Replicas = 1
	stream, err := js.CreateStream(ctx, streamConfig)
	if err != nil {
		t.Fatalf("create PubAck crash-window stream: %v", err)
	}
	assertNoUnexpectedNATSError(t, bootstrapErrors)
	outboxConnection, err := natsauth.ConnectOutbox(
		fixture.outboxConfig(fixture.outboxCredential, fixture.outboxUser),
		natsauth.Handlers{},
	)
	if err != nil {
		t.Fatalf("connect Outbox publisher: %v", err)
	}
	t.Cleanup(outboxConnection.Close)
	outboxJetStream, err := jetstream.New(outboxConnection.Conn)
	if err != nil {
		t.Fatalf("create crash-window Outbox JetStream client: %v", err)
	}
	jetStreamBroker := &testJetStreamBroker{client: outboxJetStream}

	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	publishContext, cancelPublish := context.WithCancel(ctx)
	crashBroker := &cancelAfterPubAckBroker{delegate: jetStreamBroker, cancel: cancelPublish}
	crashingPublisher, err := outbox.NewPublisher(internalPool, crashBroker, outbox.Config{
		InstanceID: "publisher-crashing-after-puback",
		BatchSize:  1,
		ClaimTTL:   30 * time.Second,
		RetryDelay: 0,
	})
	if err != nil {
		t.Fatalf("create crashing Outbox Publisher: %v", err)
	}
	if published, err := crashingPublisher.PublishBatch(publishContext); err == nil || published != 0 ||
		!errors.Is(err, context.Canceled) {
		t.Fatalf("publish across PubAck crash = %d, %v", published, err)
	}
	if crashBroker.receipt.Stream != eventstream.StreamName || crashBroker.receipt.Sequence < 1 ||
		crashBroker.messageID == "" {
		t.Fatalf("pre-crash PubAck = %#v message id %q", crashBroker.receipt, crashBroker.messageID)
	}
	var eventID string
	var publishAttempts int
	var unpublished, claimed bool
	if err := database.Admin.QueryRow(`
		SELECT event_id::text, publish_attempts, published_at IS NULL,
		       claim_token IS NOT NULL AND claim_expires_at IS NOT NULL
		FROM outbox_events
	`).Scan(&eventID, &publishAttempts, &unpublished, &claimed); err != nil {
		t.Fatalf("read Outbox state after PubAck crash: %v", err)
	}
	if eventID != crashBroker.messageID || publishAttempts != 1 || !unpublished || !claimed {
		t.Fatalf(
			"Outbox after PubAck crash = event %s attempts %d unpublished/claimed %v/%v",
			eventID,
			publishAttempts,
			unpublished,
			claimed,
		)
	}
	if _, err := database.Admin.Exec(`
		UPDATE outbox_events
		SET claim_expires_at = clock_timestamp() - interval '1 second'
	`); err != nil {
		t.Fatalf("expire crashed Outbox claim: %v", err)
	}
	recoveryPublisher, err := outbox.NewPublisher(internalPool, jetStreamBroker, outbox.Config{
		InstanceID: "publisher-recovering-puback",
		BatchSize:  1,
		ClaimTTL:   30 * time.Second,
		RetryDelay: 0,
	})
	if err != nil {
		t.Fatalf("create recovery Outbox Publisher: %v", err)
	}
	if published, err := recoveryPublisher.PublishBatch(ctx); err != nil || published != 1 {
		t.Fatalf("recover publish after PubAck crash = %d, %v", published, err)
	}
	information, err := stream.Info(ctx)
	if err != nil || information.State.Msgs != 1 {
		t.Fatalf("deduplicated stream after recovery = %#v error %v", information, err)
	}
	var brokerStream string
	var brokerSequence int64
	var published, unclaimed bool
	if err := database.Admin.QueryRow(`
		SELECT broker_stream, broker_sequence, publish_attempts,
		       published_at IS NOT NULL,
		       claimed_by IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL
		FROM outbox_events
	`).Scan(
		&brokerStream,
		&brokerSequence,
		&publishAttempts,
		&published,
		&unclaimed,
	); err != nil {
		t.Fatalf("read recovered PubAck marker: %v", err)
	}
	if brokerStream != eventstream.StreamName ||
		brokerSequence != crashBroker.receipt.Sequence || publishAttempts != 2 ||
		!published || !unclaimed {
		t.Fatalf(
			"recovered PubAck marker = %s/%d attempts %d published/unclaimed %v/%v",
			brokerStream,
			brokerSequence,
			publishAttempts,
			published,
			unclaimed,
		)
	}
}

func TestJetStreamBrokerRejectsSingleReplicaBeforePublish(t *testing.T) {
	ctx := context.Background()
	fixture := startAuthenticatedNATS(t)
	bootstrap, bootstrapErrors := fixture.connectCredential(t, fixture.bootstrapCredential, true)
	t.Cleanup(bootstrap.Close)
	js, err := jetstream.New(bootstrap)
	if err != nil {
		t.Fatalf("create authenticated bootstrap JetStream client: %v", err)
	}
	streamConfig := eventstream.StreamConfig()
	streamConfig.Replicas = 1
	stream, err := js.CreateStream(ctx, streamConfig)
	if err != nil {
		t.Fatalf("create authenticated VELA_EVENTS stream: %v", err)
	}
	assertNoUnexpectedNATSError(t, bootstrapErrors)
	connection, err := natsauth.ConnectOutbox(
		fixture.outboxConfig(fixture.outboxCredential, fixture.outboxUser),
		natsauth.Handlers{},
	)
	if err != nil {
		t.Fatalf("connect authenticated Outbox workload: %v", err)
	}
	t.Cleanup(connection.Close)

	broker, err := outbox.NewJetStreamBroker(connection.Conn)
	if err != nil {
		t.Fatalf("create JetStream Broker: %v", err)
	}
	receipt, err := broker.Publish(
		ctx,
		"vela.events.job.ready",
		"00000000-0000-0000-0000-000000000301",
		[]byte("event-payload"),
	)
	if err == nil || receipt.Stream != "" {
		t.Fatalf("single-replica JetStream publish = %#v error %v, want fail closed", receipt, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("read VELA_EVENTS stream info: %v", err)
	}
	if info.State.Msgs != 0 {
		t.Fatalf("JetStream stored %d messages before rejecting R3 drift", info.State.Msgs)
	}
}

func TestJetStreamBrokerRejectsPubAckFromUnexpectedStream(t *testing.T) {
	ctx := context.Background()
	fixture := startAuthenticatedNATS(t)
	bootstrap, bootstrapErrors := fixture.connectCredential(t, fixture.bootstrapCredential, true)
	t.Cleanup(bootstrap.Close)
	js, err := jetstream.New(bootstrap)
	if err != nil {
		t.Fatalf("create authenticated bootstrap JetStream client: %v", err)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:       "WRONG_EVENTS",
		Subjects:   []string{"vela.events.>"},
		Storage:    jetstream.FileStorage,
		Duplicates: time.Minute,
	}); err != nil {
		t.Fatalf("create wrong JetStream stream: %v", err)
	}
	assertNoUnexpectedNATSError(t, bootstrapErrors)
	connection, err := natsauth.ConnectOutbox(
		fixture.outboxConfig(fixture.outboxCredential, fixture.outboxUser),
		natsauth.Handlers{},
	)
	if err != nil {
		t.Fatalf("connect authenticated Outbox workload: %v", err)
	}
	t.Cleanup(connection.Close)
	broker, err := outbox.NewJetStreamBroker(connection.Conn)
	if err != nil {
		t.Fatalf("create JetStream Broker: %v", err)
	}
	if receipt, err := broker.Publish(
		ctx,
		"vela.events.job.ready",
		"00000000-0000-0000-0000-000000000302",
		[]byte("wrong-stream-event"),
	); err == nil || receipt.Stream != "" {
		t.Fatalf("wrong stream publish = %#v error %v, want fail closed", receipt, err)
	}
}

func TestInboxReceiptAppliesAggregateTransitionOnce(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
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
	var versionBefore int64
	if err := database.Admin.QueryRow(
		"SELECT version FROM jobs WHERE id = $1",
		event.AggregateID,
	).Scan(&versionBefore); err != nil {
		t.Fatalf("read Job version before Inbox transition: %v", err)
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
	if version != versionBefore+1 {
		t.Fatalf(
			"Job version = %d, want exactly one transition from %d to %d",
			version,
			versionBefore,
			versionBefore+1,
		)
	}
}

func TestJetStreamConsumerRedeliveryAfterCommitBeforeAckAppliesOnce(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "consumer-commit-before-ack", []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"redeliver after the local transaction commits"
    }`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var eventID, jobID uuid.UUID
	var payload []byte
	if err := database.Admin.QueryRow(`
		SELECT event_id, aggregate_id, payload
		FROM outbox_events
	`).Scan(&eventID, &jobID, &payload); err != nil {
		t.Fatalf("read Scheduler wakeup event: %v", err)
	}
	var jobVersionBefore int
	if err := database.Admin.QueryRow(
		"SELECT version FROM jobs WHERE id = $1",
		jobID,
	).Scan(&jobVersionBefore); err != nil {
		t.Fatalf("read Job version before consumer crash window: %v", err)
	}

	ctx := context.Background()
	fixture := startAuthenticatedNATS(t)
	bootstrap, bootstrapErrors := fixture.connectCredential(t, fixture.bootstrapCredential, true)
	t.Cleanup(bootstrap.Close)
	js, err := jetstream.New(bootstrap)
	if err != nil {
		t.Fatalf("create authenticated bootstrap JetStream client: %v", err)
	}
	streamConfig := eventstream.StreamConfig()
	streamConfig.Replicas = 1
	stream, err := js.CreateStream(ctx, streamConfig)
	if err != nil {
		t.Fatalf("create consumer crash-window stream: %v", err)
	}
	consumerConfig := eventstream.SchedulerConsumerConfig()
	consumerConfig.Replicas = 1
	consumerConfig.AckWait = 250 * time.Millisecond
	if _, err := stream.CreateConsumer(ctx, consumerConfig); err != nil {
		t.Fatalf("create consumer crash-window durable consumer: %v", err)
	}
	schedulerConnection, schedulerErrors := fixture.connectCredential(
		t,
		fixture.schedulerConsumerCredential,
		true,
	)
	t.Cleanup(schedulerConnection.Close)
	schedulerJetStream, err := jetstream.New(schedulerConnection)
	if err != nil {
		t.Fatalf("create Scheduler JetStream client: %v", err)
	}
	consumer, err := schedulerJetStream.Consumer(
		ctx,
		eventstream.StreamName,
		eventstream.SchedulerConsumerName,
	)
	if err != nil {
		t.Fatalf("bind Scheduler durable consumer: %v", err)
	}
	assertNoUnexpectedNATSError(t, bootstrapErrors)
	assertNoUnexpectedNATSError(t, schedulerErrors)
	outboxConnection, err := natsauth.ConnectOutbox(
		fixture.outboxConfig(fixture.outboxCredential, fixture.outboxUser),
		natsauth.Handlers{},
	)
	if err != nil {
		t.Fatalf("connect Outbox publisher: %v", err)
	}
	t.Cleanup(outboxConnection.Close)
	outboxJetStream, err := jetstream.New(outboxConnection.Conn)
	if err != nil {
		t.Fatalf("create consumer crash-window Outbox JetStream client: %v", err)
	}
	broker := &testJetStreamBroker{client: outboxJetStream}
	if _, err := broker.Publish(
		ctx,
		eventstream.SchedulerFilterSubject,
		eventID.String(),
		payload,
	); err != nil {
		t.Fatalf("publish Scheduler wakeup: %v", err)
	}

	processor, err := inbox.NewProcessor(
		newRolePool(
			t,
			database.DSN,
			"vela_internal_login",
			"vela-internal-password",
		),
		"scheduler",
	)
	if err != nil {
		t.Fatalf("create Inbox processor: %v", err)
	}
	postCommitHookCalls := 0
	messageConsumer, err := inbox.NewJetStreamConsumerWithPostCommitHook(
		processor,
		func(_ context.Context, event inbox.Event, message jetstream.Msg) error {
			postCommitHookCalls++
			metadata, metadataErr := message.Metadata()
			if metadataErr != nil {
				t.Fatalf("read post-commit message metadata: %v", metadataErr)
			}
			if event.ID != eventID || metadata.Stream != eventstream.StreamName ||
				metadata.Consumer != eventstream.SchedulerConsumerName ||
				metadata.NumDelivered != 1 {
				t.Fatalf("post-commit boundary = event %s metadata %#v", event.ID, metadata)
			}
			return errors.New("simulated process loss before ack")
		},
	)
	if err != nil {
		t.Fatalf("create JetStream Inbox consumer: %v", err)
	}
	firstMessage, err := consumer.Next(jetstream.FetchMaxWait(2 * time.Second))
	if err != nil {
		t.Fatalf("fetch first Scheduler wakeup: %v", err)
	}
	transitionCalls := 0
	applied, err := messageConsumer.ProcessMessage(
		ctx,
		firstMessage,
		func(context.Context, pgx.Tx) error {
			transitionCalls++
			return nil
		},
	)
	if !applied || err == nil || !strings.Contains(err.Error(), "simulated process loss before ack") {
		t.Fatalf("commit-before-ack processing = applied %v error %v", applied, err)
	}

	redelivered, err := consumer.Next(jetstream.FetchMaxWait(2 * time.Second))
	if err != nil {
		t.Fatalf("fetch redelivered Scheduler wakeup: %v", err)
	}
	metadata, err := redelivered.Metadata()
	if err != nil || metadata.NumDelivered < 2 {
		t.Fatalf("redelivered Scheduler metadata = %#v error %v", metadata, err)
	}
	applied, err = messageConsumer.ProcessMessage(
		ctx,
		redelivered,
		func(context.Context, pgx.Tx) error {
			t.Fatal("redelivery invoked the aggregate transition twice")
			return nil
		},
	)
	if err != nil || applied {
		t.Fatalf("redelivery processing = applied %v error %v", applied, err)
	}
	if transitionCalls != 1 {
		t.Fatalf("aggregate transition calls = %d, want 1", transitionCalls)
	}
	if postCommitHookCalls != 1 {
		t.Fatalf("post-commit hook calls = %d, want 1", postCommitHookCalls)
	}
	var jobVersion, receipts int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT version FROM jobs WHERE id = $1),
			(SELECT count(*) FROM inbox_receipts
			 WHERE consumer_name = 'scheduler' AND event_id = $2)
	`, jobID, eventID).Scan(&jobVersion, &receipts); err != nil {
		t.Fatalf("read consumer crash-window state: %v", err)
	}
	if jobVersion != jobVersionBefore || receipts != 1 {
		t.Fatalf(
			"consumer crash-window state = Job version %d (started at %d) receipts %d",
			jobVersion,
			jobVersionBefore,
			receipts,
		)
	}
	consumerInfo, err := consumer.Info(ctx)
	if err != nil || consumerInfo.NumAckPending != 0 {
		t.Fatalf("confirmed ack state = %#v error %v", consumerInfo, err)
	}
}

type testJetStreamBroker struct {
	client jetstream.JetStream
}

func (b *testJetStreamBroker) Publish(
	ctx context.Context,
	subject string,
	messageID string,
	payload []byte,
) (outbox.Receipt, error) {
	acknowledgement, err := b.client.Publish(
		ctx,
		subject,
		payload,
		jetstream.WithMsgID(messageID),
		jetstream.WithExpectStream(eventstream.StreamName),
	)
	if err != nil {
		return outbox.Receipt{}, err
	}
	return outbox.Receipt{
		Stream: acknowledgement.Stream, Sequence: int64(acknowledgement.Sequence),
	}, nil
}

type brokerCall struct {
	Subject   string
	MessageID string
	Payload   []byte
}

type cancelAfterPubAckBroker struct {
	delegate  outbox.Broker
	cancel    context.CancelFunc
	receipt   outbox.Receipt
	messageID string
}

func (b *cancelAfterPubAckBroker) Publish(
	ctx context.Context,
	subject string,
	messageID string,
	payload []byte,
) (outbox.Receipt, error) {
	receipt, err := b.delegate.Publish(ctx, subject, messageID, payload)
	if err == nil {
		b.receipt = receipt
		b.messageID = messageID
		b.cancel()
	}
	return receipt, err
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
