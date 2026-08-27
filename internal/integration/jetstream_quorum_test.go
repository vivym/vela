//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/vivym/vela/internal/eventstream"
	"github.com/vivym/vela/internal/inbox"
	"github.com/vivym/vela/internal/outbox"
	"github.com/vivym/vela/internal/scheduler"
)

func TestThreeReplicaJetStreamRequiresQuorumBeforeOutboxMarker(t *testing.T) {
	cluster := startThreeNodeJetStream(t)
	ctx := context.Background()
	connection, err := nats.Connect(strings.Join(cluster.urls, ","), nats.Timeout(5*time.Second))
	if err != nil {
		t.Fatalf("connect three-node JetStream: %v", err)
	}
	t.Cleanup(connection.Close)
	js, err := jetstream.New(connection)
	if err != nil {
		t.Fatalf("create three-node JetStream client: %v", err)
	}
	stream := createReleaseStream(t, js, cluster.containers)
	waitForReleaseStreamCurrent(t, stream, cluster.containers...)
	information, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("read release stream contract: %v", err)
	}
	if err := eventstream.ValidateStreamConfig(information.Config); err != nil {
		t.Fatalf("three-node release stream drift: %v; config=%#v", err, information.Config)
	}
	consumer, err := stream.CreateConsumer(ctx, eventstream.SchedulerConsumerConfig())
	if err != nil {
		t.Fatalf("create three-replica Scheduler consumer: %v", err)
	}
	waitForReleaseConsumerCurrent(t, consumer)
	consumerInformation, err := consumer.Info(ctx)
	if err != nil {
		t.Fatalf("read release Scheduler consumer contract: %v", err)
	}
	if err := eventstream.ValidateSchedulerConsumerConfig(consumerInformation.Config); err != nil {
		t.Fatalf(
			"three-node Scheduler consumer drift: %v; config=%#v",
			err,
			consumerInformation.Config,
		)
	}

	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	schedulerInboxPool := newRolePool(
		t,
		database.DSN,
		"vela_scheduler_inbox_login",
		"vela-scheduler-inbox-password",
	)
	inboxProcessor, err := inbox.NewSchedulerProcessor(schedulerInboxPool)
	if err != nil {
		t.Fatalf("create Scheduler Inbox processor: %v", err)
	}
	messageConsumer, err := inbox.NewJetStreamConsumer(inboxProcessor)
	if err != nil {
		t.Fatalf("create Scheduler JetStream Inbox consumer: %v", err)
	}
	handlerObserved := make(chan error, 1)
	wakeupConsumer, err := scheduler.BindJetStreamWakeupConsumer(
		ctx,
		connection,
		messageConsumer,
		func(handlerContext context.Context, _ pgx.Tx) error {
			information, infoErr := consumer.Info(handlerContext)
			if infoErr != nil {
				return fmt.Errorf("inspect Scheduler consumer before handler completion: %w", infoErr)
			}
			if information.NumAckPending != 1 {
				return fmt.Errorf(
					"Scheduler ack pending during handler = %d, want 1",
					information.NumAckPending,
				)
			}
			handlerObserved <- nil
			return nil
		},
	)
	if err != nil {
		t.Fatalf("bind production Scheduler wakeup consumer: %v", err)
	}
	wakeupContext, cancelWakeup := context.WithCancel(ctx)
	wakeupDone := make(chan error, 1)
	wakeupErrors := make(chan error, 1)
	go func() {
		wakeupDone <- wakeupConsumer.Run(wakeupContext, func(err error) {
			wakeupErrors <- err
		})
	}()
	broker, err := outbox.NewJetStreamBroker(connection)
	if err != nil {
		t.Fatalf("create quorum JetStream Broker: %v", err)
	}
	publisher, err := outbox.NewPublisher(
		internalPool,
		&boundedPublishBroker{delegate: broker, timeout: 8 * time.Second},
		outbox.Config{
			InstanceID: "three-replica-quorum-publisher",
			BatchSize:  1,
			ClaimTTL:   10 * time.Second,
			RetryDelay: 0,
		},
	)
	if err != nil {
		t.Fatalf("create quorum Outbox Publisher: %v", err)
	}

	firstEvent := submitQuorumEvent(t, database, server.URL, "all-replicas")
	if published, err := publisher.PublishBatch(ctx); err != nil || published != 1 {
		t.Fatalf("publish with all replicas = %d, %v", published, err)
	}
	assertPublishedQuorumReceipt(t, database, firstEvent, 1)
	select {
	case err := <-handlerObserved:
		if err != nil {
			t.Fatalf("run Scheduler handler before confirmed ack: %v", err)
		}
	case err := <-wakeupErrors:
		t.Fatalf("consume release Scheduler wakeup: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("release Scheduler consumer did not run the handler before confirmed ack")
	}
	cancelWakeup()
	select {
	case err := <-wakeupDone:
		if err != nil {
			t.Fatalf("stop release Scheduler wakeup consumer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("release Scheduler wakeup consumer did not stop with context")
	}

	stopTimeout := 5 * time.Second
	if err := cluster.containers[0].Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop first JetStream replica: %v", err)
	}
	waitForReleaseStreamQuorumWithOneReplicaOffline(t, stream, cluster.containers...)
	secondEvent := submitQuorumEvent(t, database, server.URL, "one-replica-offline")
	if published, err := publisher.PublishBatch(ctx); err != nil || published != 1 {
		t.Fatalf("publish with one replica offline = %d, %v", published, err)
	}
	assertPublishedQuorumReceipt(t, database, secondEvent, 1)

	if err := cluster.containers[1].Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop second JetStream replica: %v", err)
	}
	thirdEvent := submitQuorumEvent(t, database, server.URL, "no-stream-quorum")
	if published, err := publisher.PublishBatch(ctx); err == nil || published != 0 {
		t.Fatalf("publish without stream quorum = %d, %v; want no receipt", published, err)
	}
	var attempts int
	var unpublished, noReceipt, unclaimed bool
	if err := database.Admin.QueryRow(`
		SELECT publish_attempts, published_at IS NULL,
		       broker_stream IS NULL AND broker_sequence IS NULL,
		       claimed_by IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL
		FROM outbox_events
		WHERE event_id = $1
	`, thirdEvent).Scan(&attempts, &unpublished, &noReceipt, &unclaimed); err != nil {
		t.Fatalf("read no-quorum Outbox row: %v", err)
	}
	if attempts != 1 || !unpublished || !noReceipt || !unclaimed {
		t.Fatalf(
			"no-quorum Outbox = attempts %d unpublished/no-receipt/unclaimed %v/%v/%v",
			attempts,
			unpublished,
			noReceipt,
			unclaimed,
		)
	}

	if err := cluster.containers[0].Start(ctx); err != nil {
		t.Fatalf("restart first JetStream replica: %v", err)
	}
	if err := cluster.containers[1].Start(ctx); err != nil {
		t.Fatalf("restart second JetStream replica: %v", err)
	}
	waitForReleaseStreamCurrent(t, stream, cluster.containers...)
	if published, err := publisher.PublishBatch(ctx); err != nil || published != 1 {
		t.Fatalf("publish after quorum recovery = %d, %v", published, err)
	}
	assertPublishedQuorumReceipt(t, database, thirdEvent, 2)
	information, err = stream.Info(ctx)
	if err != nil || information.State.Msgs != 3 {
		t.Fatalf("stream after quorum recovery = %#v error %v", information, err)
	}
}

type threeNodeJetStream struct {
	containers []testcontainers.Container
	urls       []string
}

func startThreeNodeJetStream(t *testing.T) threeNodeJetStream {
	t.Helper()
	ctx := context.Background()
	dockerNetwork, err := network.New(ctx, network.WithAttachable())
	if err != nil {
		t.Fatalf("create JetStream test network: %v", err)
	}
	t.Cleanup(func() {
		if err := dockerNetwork.Remove(context.Background()); err != nil {
			t.Errorf("remove JetStream test network: %v", err)
		}
	})
	routes := []string{
		"nats://nats-0:6222",
		"nats://nats-1:6222",
		"nats://nats-2:6222",
	}
	cluster := threeNodeJetStream{
		containers: make([]testcontainers.Container, 0, 3),
		urls:       make([]string, 0, 3),
	}
	for index := range 3 {
		name := fmt.Sprintf("nats-%d", index)
		configuration := fmt.Sprintf(`
server_name: %s
port: 4222
http: 8222
jetstream {
  store_dir: "/data/jetstream"
}
cluster {
  name: VELA
  listen: 0.0.0.0:6222
  routes = [%s]
}
`, name, strings.Join(routes, ","))
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:          "nats:2.10.22",
				ExposedPorts:   []string{"4222/tcp"},
				Cmd:            []string{"-c", "/etc/nats/nats.conf"},
				Networks:       []string{dockerNetwork.Name},
				NetworkAliases: map[string][]string{dockerNetwork.Name: {name}},
				Files: []testcontainers.ContainerFile{{
					Reader:            strings.NewReader(configuration),
					ContainerFilePath: "/etc/nats/nats.conf",
					FileMode:          0o600,
				}},
				WaitingFor: wait.ForLog("Server is ready").WithStartupTimeout(60 * time.Second),
			},
			Started: true,
		})
		if err != nil {
			t.Fatalf("start JetStream replica %s: %v", name, err)
		}
		cluster.containers = append(cluster.containers, container)
		t.Cleanup(func() {
			if err := container.Terminate(context.Background()); err != nil {
				t.Errorf("terminate JetStream replica %s: %v", name, err)
			}
		})
		endpoint, err := container.Endpoint(ctx, "")
		if err != nil {
			t.Fatalf("resolve JetStream replica %s: %v", name, err)
		}
		cluster.urls = append(cluster.urls, "nats://"+endpoint)
	}
	return cluster
}

func createReleaseStream(
	t *testing.T,
	js jetstream.JetStream,
	containers []testcontainers.Container,
) jetstream.Stream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var lastErr error
	for ctx.Err() == nil {
		attemptContext, attemptCancel := context.WithTimeout(ctx, 2*time.Second)
		stream, err := js.CreateStream(attemptContext, eventstream.StreamConfig())
		attemptCancel()
		if err == nil {
			return stream
		}
		lastErr = err
		time.Sleep(250 * time.Millisecond)
	}
	for index, container := range containers {
		logs, err := container.Logs(context.Background())
		if err != nil {
			t.Logf("read JetStream replica %d logs: %v", index, err)
			continue
		}
		contents, err := io.ReadAll(logs)
		_ = logs.Close()
		if err != nil {
			t.Logf("read JetStream replica %d log contents: %v", index, err)
			continue
		}
		if len(contents) > 4000 {
			contents = contents[len(contents)-4000:]
		}
		t.Logf("JetStream replica %d logs:\n%s", index, contents)
	}
	t.Fatalf("create three-replica release stream: %v", lastErr)
	return nil
}

func waitForReleaseStreamCurrent(
	t *testing.T,
	stream jetstream.Stream,
	containers ...testcontainers.Container,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var lastInfo *jetstream.StreamInfo
	var lastErr error
	for ctx.Err() == nil {
		attemptContext, cancelAttempt := context.WithTimeout(ctx, 2*time.Second)
		information, err := stream.Info(attemptContext)
		cancelAttempt()
		lastInfo, lastErr = information, err
		if err == nil && information.Cluster != nil && information.Cluster.Leader != "" &&
			len(information.Cluster.Replicas) == 2 &&
			information.Cluster.Replicas[0].Current && information.Cluster.Replicas[1].Current {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	for index, container := range containers {
		logs, err := container.Logs(context.Background())
		if err != nil {
			t.Logf("read JetStream recovery replica %d logs: %v", index, err)
			continue
		}
		contents, err := io.ReadAll(logs)
		_ = logs.Close()
		if err != nil {
			t.Logf("read JetStream recovery replica %d log contents: %v", index, err)
			continue
		}
		if len(contents) > 8000 {
			contents = contents[len(contents)-8000:]
		}
		t.Logf("JetStream recovery replica %d logs:\n%s", index, contents)
	}
	t.Fatalf("three-replica stream did not become current: info=%#v error=%v", lastInfo, lastErr)
}

func waitForReleaseStreamQuorumWithOneReplicaOffline(
	t *testing.T,
	stream jetstream.Stream,
	containers ...testcontainers.Container,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var lastInfo *jetstream.StreamInfo
	var lastErr error
	for ctx.Err() == nil {
		attemptContext, cancelAttempt := context.WithTimeout(ctx, 2*time.Second)
		information, err := stream.Info(attemptContext)
		cancelAttempt()
		lastInfo, lastErr = information, err
		if err == nil && information.Cluster != nil && information.Cluster.Leader != "" &&
			len(information.Cluster.Replicas) == 2 {
			current, offline := 0, 0
			for _, replica := range information.Cluster.Replicas {
				if replica.Current {
					current++
				}
				if replica.Offline {
					offline++
				}
			}
			if current == 1 && offline == 1 {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	for index, container := range containers {
		logs, err := container.Logs(context.Background())
		if err != nil {
			t.Logf("read one-offline JetStream replica %d logs: %v", index, err)
			continue
		}
		contents, err := io.ReadAll(logs)
		_ = logs.Close()
		if err != nil {
			t.Logf("read one-offline JetStream replica %d log contents: %v", index, err)
			continue
		}
		if len(contents) > 8000 {
			contents = contents[len(contents)-8000:]
		}
		t.Logf("one-offline JetStream replica %d logs:\n%s", index, contents)
	}
	t.Fatalf("three-replica stream did not retain quorum with one replica offline: info=%#v error=%v", lastInfo, lastErr)
}

func waitForReleaseConsumerCurrent(t *testing.T, consumer jetstream.Consumer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var lastInfo *jetstream.ConsumerInfo
	var lastErr error
	for ctx.Err() == nil {
		attemptContext, cancelAttempt := context.WithTimeout(ctx, 2*time.Second)
		information, err := consumer.Info(attemptContext)
		cancelAttempt()
		lastInfo, lastErr = information, err
		if err == nil && information.Cluster != nil && information.Cluster.Leader != "" &&
			len(information.Cluster.Replicas) == 2 &&
			information.Cluster.Replicas[0].Current && information.Cluster.Replicas[1].Current {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("three-replica consumer did not become current: info=%#v error=%v", lastInfo, lastErr)
}

func submitQuorumEvent(
	t *testing.T,
	database testDatabase,
	serverURL string,
	idempotencyKey string,
) uuid.UUID {
	t.Helper()
	accepted := submitJob(t, serverURL, idempotencyKey, []byte(`{
        "model":"minimax-h3",
        "generation_preset":"balanced",
        "service_class":"standard",
        "output_spec":"video-1080p-5s-24fps",
        "generation_count":1,
        "prompt":"prove a three-replica quorum PubAck"
    }`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission status = %d, want 202; body=%s", accepted.StatusCode, accepted.Body)
	}
	var eventID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT event_id
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY occurred_at
		LIMIT 1
	`).Scan(&eventID); err != nil {
		t.Fatalf("read admitted quorum Outbox event: %v", err)
	}
	return eventID
}

func assertPublishedQuorumReceipt(
	t *testing.T,
	database testDatabase,
	eventID uuid.UUID,
	wantAttempts int,
) {
	t.Helper()
	var stream string
	var sequence int64
	var attempts int
	if err := database.Admin.QueryRow(`
		SELECT broker_stream, broker_sequence, publish_attempts
		FROM outbox_events
		WHERE event_id = $1 AND published_at IS NOT NULL
	`, eventID).Scan(&stream, &sequence, &attempts); err != nil {
		t.Fatalf("read quorum PubAck receipt for %s: %v", eventID, err)
	}
	if stream != eventstream.StreamName || sequence < 1 || attempts != wantAttempts {
		t.Fatalf("quorum PubAck receipt = %s/%d attempts %d", stream, sequence, attempts)
	}
}

type boundedPublishBroker struct {
	delegate outbox.Broker
	timeout  time.Duration
}

func (b *boundedPublishBroker) Publish(
	ctx context.Context,
	subject string,
	messageID string,
	payload []byte,
) (outbox.Receipt, error) {
	publishContext, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	return b.delegate.Publish(publishContext, subject, messageID, payload)
}
