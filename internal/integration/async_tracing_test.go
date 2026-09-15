//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/vivym/vela/internal/eventstream"
	"github.com/vivym/vela/internal/inbox"
	"github.com/vivym/vela/internal/outbox"
	"github.com/vivym/vela/internal/tracing"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const asyncTraceParent = "00-abcdef1234567890abcdef1234567890-1234567890abcdef-01"

func TestAsyncTraceSurvivesOutboxRetryAndInboxRedelivery(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()); otel.SetTracerProvider(old) })
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	original := admissionServerForDatabase(t, database)
	server := httptest.NewServer(tracing.HTTPServer(original.Config.Handler))
	t.Cleanup(server.Close)
	body := `{"model":"minimax-h3","generation_preset":"balanced","service_class":"standard","output_spec":"video-1080p-5s-24fps","generation_count":1,"prompt":"async-private-prompt"}`
	submit := func(parent string) {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/projects/"+testProjectID+"/jobs", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+testBearerCredential())
		request.Header.Set("Idempotency-Key", "async-trace-durable")
		request.Header.Set("Traceparent", parent)
		request.Header.Set("Tracestate", "vendor=async-private-state")
		request.Header.Set("Baggage", "key=async-private-baggage")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		b, _ := io.ReadAll(response.Body)
		if response.StatusCode != 202 {
			t.Fatalf("admit: %d %s", response.StatusCode, b)
		}
	}
	submit(asyncTraceParent)
	var parent, jobID, eventID string
	var payload []byte
	if err := database.Admin.QueryRow(`SELECT j.origin_trace_parent,j.id::text,e.event_id::text,e.payload FROM jobs j JOIN outbox_events e ON e.aggregate_id=j.id`).Scan(&parent, &jobID, &eventID, &payload); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(parent, "00-abcdef1234567890abcdef1234567890-") || parent == asyncTraceParent {
		t.Fatal("admission server span was not committed with Job")
	}
	submit("00-99999999999999999999999999999999-7777777777777777-01")
	var after string
	if err := database.Admin.QueryRow(`SELECT origin_trace_parent FROM jobs WHERE id=$1`, jobID).Scan(&after); err != nil || after != parent {
		t.Fatalf("idempotent replay replaced origin: %v", err)
	}
	if _, err := database.Admin.Exec(`UPDATE jobs SET origin_trace_parent=$2 WHERE id=$1`, jobID, asyncTraceParent); err == nil || !strings.Contains(err.Error(), "Job origin trace cannot be replaced") {
		t.Fatalf("origin update was not rejected: %v", err)
	}
	assertOriginTraceConstraint(t, database, jobID)
	connection := startAsyncTraceNATSCluster(t)
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	var stream jetstream.Stream
	// Cluster metadata election may finish after the individual listeners start.
	for attempt := 0; attempt < 40; attempt++ {
		attemptCtx, stop := context.WithTimeout(ctx, 3*time.Second)
		stream, err = js.CreateStream(attemptCtx, eventstream.StreamConfig())
		stop()
		if err == nil {
			break
		}
		if attempt < 3 {
			t.Logf("stream initialization attempt %d: %v", attempt+1, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("stream initialization: %v; last request: %v", ctx.Err(), err)
		case <-time.After(250 * time.Millisecond):
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	config := eventstream.SchedulerConsumerConfig()
	consumer, err := stream.CreateConsumer(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	broker, err := outbox.NewJetStreamBroker(connection)
	if err != nil {
		t.Fatal(err)
	}
	pool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	firstCtx, abort := context.WithCancel(ctx)
	crash := &cancelAfterPubAckBroker{delegate: broker, cancel: abort}
	publisher, err := outbox.NewPublisher(pool, crash, outbox.Config{InstanceID: "async-crash", BatchSize: 1, ClaimTTL: time.Second, RetryDelay: 0})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := publisher.PublishBatch(firstCtx); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("PubAck crash: %d %v", n, err)
	}
	pool.Close()
	if _, err := database.Admin.Exec(`UPDATE outbox_events SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE event_id=$1`, eventID); err != nil {
		t.Fatal(err)
	}
	// A new Publisher and connection pool have only database state to recover from.
	broker, err = outbox.NewJetStreamBroker(connection)
	if err != nil {
		t.Fatal(err)
	}
	pool = newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	publisher, err = outbox.NewPublisher(pool, broker, outbox.Config{InstanceID: "async-recovered", BatchSize: 1, ClaimTTL: time.Second, RetryDelay: 0})
	if err != nil {
		t.Fatal(err)
	}
	if n, err := publisher.PublishBatch(ctx); n != 1 || err != nil {
		t.Fatalf("retry: %d %v", n, err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Msgs != 1 || info.Config.Replicas != 3 {
		t.Fatalf("stream dedup/replicas: %#v %v", info, err)
	}
	message, err := consumer.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if message.Headers().Get("Nats-Msg-Id") != eventID || !bytes.Equal(message.Data(), payload) || message.Headers().Get("Baggage") != "" || message.Headers().Get("Tracestate") != "" {
		t.Fatal("message identity/payload changed or private context propagated")
	}
	wireParent := message.Headers().Get("traceparent")
	if !strings.HasPrefix(wireParent, "00-abcdef1234567890abcdef1234567890-") || wireParent == parent {
		t.Fatal("actual NATS message missing producer span")
	}
	processor, err := inbox.NewProcessor(pool, "async-trace-consumer")
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := inbox.NewJetStreamConsumerWithPostCommitHook(processor, func(context.Context, inbox.Event, jetstream.Msg) error { return errors.New("async-private-ack-error") })
	if err != nil {
		t.Fatal(err)
	}
	transitions := 0
	if applied, err := adapter.ProcessMessage(ctx, message, func(handlerCtx context.Context, _ pgx.Tx) error {
		transitions++
		if got := trace.SpanContextFromContext(handlerCtx).TraceID().String(); got != "abcdef1234567890abcdef1234567890" {
			t.Fatalf("handler trace: %s", got)
		}
		return nil
	}); !applied || err == nil {
		t.Fatalf("commit-before-ack: %v %v", applied, err)
	}
	// Expedite a real NATS redelivery after the intentionally skipped acknowledgement.
	if err := message.Nak(); err != nil {
		t.Fatal(err)
	}
	redelivered, err := consumer.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := redelivered.Metadata()
	if err != nil || metadata.NumDelivered < 2 || redelivered.Headers().Get("traceparent") != wireParent {
		t.Fatalf("real redelivery metadata/context: %#v %v", metadata, err)
	}
	processor, err = inbox.NewProcessor(newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password"), "async-trace-consumer")
	if err != nil {
		t.Fatal(err)
	}
	adapter, err = inbox.NewJetStreamConsumer(processor)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := adapter.ProcessMessage(ctx, redelivered, func(context.Context, pgx.Tx) error { transitions++; return nil }); applied || err != nil {
		t.Fatalf("deduplicated redelivery: %v %v", applied, err)
	}
	if transitions != 1 {
		t.Fatalf("transitions: %d", transitions)
	}
	var attempts, receipts int
	if err := database.Admin.QueryRow(`SELECT (SELECT publish_attempts FROM outbox_events WHERE event_id=$1),(SELECT count(*) FROM inbox_receipts WHERE event_id=$1)`, eventID).Scan(&attempts, &receipts); err != nil || attempts != 2 || receipts != 1 {
		t.Fatalf("durable receipts: %d %d %v", attempts, receipts, err)
	}
	ci, err := consumer.Info(ctx)
	if err != nil || ci.NumAckPending != 0 {
		t.Fatalf("final ack: %#v %v", ci, err)
	}
	var producers, consumers []sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		b, err := json.Marshal(tracetest.SpanStubFromReadOnlySpan(s))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(b), "async-private") || strings.Contains(string(b), testBearerCredential()) || s.SpanContext().TraceState().Len() != 0 {
			t.Fatal("span contains private payload, credentials, raw errors or vendor state")
		}
		if s.SpanKind() == trace.SpanKindProducer {
			producers = append(producers, s)
		}
		if s.SpanKind() == trace.SpanKindConsumer {
			consumers = append(consumers, s)
		}
	}
	if len(producers) != 2 || len(consumers) != 2 {
		t.Fatalf("message spans: %d/%d", len(producers), len(consumers))
	}
	origin := trace.SpanContextFromContext(tracing.WithDurableParent(context.Background(), &parent))
	published := trace.SpanContextFromContext(tracing.WithDurableParent(context.Background(), &wireParent))
	for _, s := range producers {
		if s.Parent().TraceID() != origin.TraceID() || s.Parent().SpanID() != origin.SpanID() {
			t.Fatal("retry lost persisted request parent")
		}
	}
	if producers[0].SpanContext().SpanID() != published.SpanID() || producers[0].SpanContext().SpanID() == producers[1].SpanContext().SpanID() {
		t.Fatal("dedup did not preserve original publish span or retry reused span ID")
	}
	for _, s := range consumers {
		if s.Parent().TraceID() != published.TraceID() || s.Parent().SpanID() != published.SpanID() {
			t.Fatal("redelivery lost producer parent")
		}
	}
	if consumers[0].SpanContext().SpanID() == consumers[1].SpanContext().SpanID() {
		t.Fatal("redelivery reused its processing span")
	}
	if producers[0].Status().Code != codes.Error || producers[1].Status().Code == codes.Error || consumers[0].Status().Code != codes.Error || consumers[1].Status().Code == codes.Error {
		t.Fatal("crash/recovery status incorrect")
	}
	// A request through the uninstrumented handler remains admissible with NULL context.
	accepted := submitJob(t, original.URL, "async-untraced", []byte(body))
	if accepted.StatusCode != 202 {
		t.Fatalf("untraced admission: %d %s", accepted.StatusCode, accepted.Body)
	}
	var absent int
	if err := database.Admin.QueryRow(`SELECT count(*) FROM jobs WHERE origin_trace_parent IS NULL`).Scan(&absent); err != nil || absent != 1 {
		t.Fatalf("legacy context: %d %v", absent, err)
	}
	t.Log("verified transactional trace, idempotent admission, real three-replica publish/dedup, commit-before-ack redelivery, one Inbox effect, and payload-free spans")
}

func assertOriginTraceConstraint(t *testing.T, database testDatabase, jobID string) {
	t.Helper()
	ctx := t.Context()
	connection, err := database.Admin.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	// Copy the actual migrated CHECK constraints, without mutation triggers or
	// business side effects, to exercise malformed SQL inputs independently.
	if _, err := connection.ExecContext(ctx, `CREATE TEMP TABLE origin_trace_probe (LIKE jobs INCLUDING CONSTRAINTS)`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := connection.ExecContext(context.Background(), `DROP TABLE pg_temp.origin_trace_probe`); err != nil {
			t.Error(err)
		}
	}()
	for _, input := range []string{
		asyncTraceParent, strings.TrimSuffix(asyncTraceParent, "01") + "00", "",
		asyncTraceParent + "\n", strings.ToUpper(asyncTraceParent),
		strings.Replace(asyncTraceParent, "abcdef1234567890abcdef1234567890", strings.Repeat("0", 32), 1),
		strings.Replace(asyncTraceParent, "-1234567890abcdef-", "-0000000000000000-", 1),
		strings.TrimSuffix(asyncTraceParent, "01") + "ff",
	} {
		_, err := connection.ExecContext(ctx, `INSERT INTO pg_temp.origin_trace_probe
			SELECT (jsonb_populate_record(NULL::pg_temp.origin_trace_probe,
			    to_jsonb(j) || jsonb_build_object('origin_trace_parent', $2::text))).*
			FROM jobs j WHERE id=$1`, jobID, input)
		if input == asyncTraceParent || input == strings.TrimSuffix(asyncTraceParent, "01")+"00" {
			if err != nil {
				t.Fatal(err)
			}
			continue
		}
		var databaseError *pgconn.PgError
		if !errors.As(err, &databaseError) || databaseError.ConstraintName != "jobs_origin_trace_parent_valid" {
			t.Fatalf("malformed trace not rejected by migrated constraint: %v", err)
		}
	}
}

func startAsyncTraceNATSCluster(t *testing.T) *nats.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	net, err := network.New(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := net.Remove(cleanup); err != nil {
			t.Error(err)
		}
	})
	token := uuid.NewString()
	var endpoint string
	for i := 0; i < 3; i++ {
		alias := fmt.Sprintf("async-nats-%d", i)
		config := fmt.Sprintf(`server_name: %s
port: 4222
authorization { token: %q }
jetstream { store_dir: /data, max_memory_store: 64MB, max_file_store: 80GB }
cluster { name: async-trace, listen: 0.0.0.0:6222, routes: [nats://async-nats-0:6222,nats://async-nats-1:6222,nats://async-nats-2:6222] }
`, alias, token)
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
			Image: "nats:2.12-alpine", ExposedPorts: []string{"4222/tcp"}, Networks: []string{net.Name}, NetworkAliases: map[string][]string{net.Name: {alias}},
			Files: []testcontainers.ContainerFile{{Reader: strings.NewReader(config), ContainerFilePath: "/etc/nats/trace.conf", FileMode: 0600}}, Cmd: []string{"-c", "/etc/nats/trace.conf"}, WaitingFor: wait.ForLog("Server is ready").WithStartupTimeout(30 * time.Second),
		}, Started: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if err := container.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
		if i == 0 {
			endpoint, err = container.PortEndpoint(ctx, "4222/tcp", "nats")
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	connection, err := nats.Connect(endpoint, nats.Token(token), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(connection.Close)
	return connection
}
