//go:build integration

package integration_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/vivym/vela/internal/eventstream"
	"github.com/vivym/vela/internal/natsauth"
)

func TestNATSWorkloadIdentityAndSubjectAuthorization(t *testing.T) {
	fixture := startAuthenticatedNATS(t)

	if connection, err := nats.Connect(
		fixture.url,
		nats.Secure(fixture.clientTLSConfig(t)),
		nats.Timeout(2*time.Second),
	); err == nil {
		connection.Close()
		t.Fatal("operator/account JWT server accepted an anonymous connection")
	}

	revokedConnection, err := natsauth.ConnectOutbox(
		fixture.outboxConfig(fixture.revokedOutboxCredential, fixture.revokedOutboxUser),
		natsauth.Handlers{},
	)
	if revokedConnection != nil {
		revokedConnection.Close()
		t.Fatal("account-revoked Outbox credential returned a connection")
	}
	if !errors.Is(err, natsauth.ErrOutboxConnection) {
		t.Fatalf("revoked Outbox credential error = %v, want startup authentication rejection", err)
	}
	for _, serverList := range []string{
		fixture.url + ",tls://127.0.0.1:1",
		"tls://127.0.0.1:1," + fixture.url,
	} {
		mixedEndpointConfig := fixture.outboxConfig(
			fixture.revokedOutboxCredential,
			fixture.revokedOutboxUser,
		)
		mixedEndpointConfig.URL = serverList
		mixedEndpointConnection, mixedEndpointErr := natsauth.ConnectOutbox(
			mixedEndpointConfig,
			natsauth.Handlers{},
		)
		if mixedEndpointConnection != nil {
			mixedEndpointConnection.Close()
			t.Fatal("mixed auth-rejected/offline endpoint pool returned a connection")
		}
		if !errors.Is(mixedEndpointErr, natsauth.ErrOutboxConnection) {
			t.Fatalf(
				"mixed auth-rejected/offline endpoint pool error = %v, want startup rejection",
				mixedEndpointErr,
			)
		}
	}
	mixedReadyConfig := fixture.outboxConfig(fixture.outboxCredential, fixture.outboxUser)
	mixedReadyConfig.URL = "tls://127.0.0.1:1," + fixture.url
	mixedReadyConnection, err := natsauth.ConnectOutbox(mixedReadyConfig, natsauth.Handlers{})
	if err != nil {
		t.Fatalf("connect authenticated endpoint from mixed pool: %v", err)
	}
	if err := mixedReadyConnection.Activate(); err != nil {
		mixedReadyConnection.Close()
		t.Fatalf("activate already-authenticated endpoint pool: %v", err)
	}
	if !mixedReadyConnection.IsConnected() {
		mixedReadyConnection.Close()
		t.Fatal("mixed endpoint pool did not return its authenticated connection")
	}
	configuredServers := mixedReadyConnection.Servers()
	configuredServerList := strings.Join(configuredServers, ",")
	if len(configuredServers) != 2 ||
		!strings.Contains(configuredServerList, fixture.url) ||
		!strings.Contains(configuredServerList, "tls://127.0.0.1:1") {
		mixedReadyConnection.Close()
		t.Fatalf("mixed endpoint connection server pool = %v", configuredServers)
	}
	mixedReadyConnection.Close()
	dnsUnavailableURL := "tls://nats-unavailable.invalid:4222"
	dnsMixedConfig := fixture.outboxConfig(fixture.outboxCredential, fixture.outboxUser)
	dnsMixedConfig.URL = dnsUnavailableURL + "," + fixture.url
	dnsMixedConnection, err := natsauth.ConnectOutbox(dnsMixedConfig, natsauth.Handlers{})
	if err != nil {
		t.Fatalf("connect authenticated endpoint with DNS-unavailable peer: %v", err)
	}
	if !dnsMixedConnection.IsConnected() {
		dnsMixedConnection.Close()
		t.Fatal("DNS-unavailable peer prevented authenticated endpoint connection")
	}
	dnsMixedConnection.Close()

	systemConnection, err := natsauth.ConnectOutbox(
		fixture.outboxConfig(fixture.systemCredential, fixture.outboxUser),
		natsauth.Handlers{},
	)
	if systemConnection != nil {
		systemConnection.Close()
		t.Fatal("Outbox connector accepted a system-account credential")
	}
	if !errors.Is(err, natsauth.ErrInvalidOutboxCredential) {
		t.Fatalf("system-account Outbox credential error = %v, want identity rejection", err)
	}
	systemBoundaryConnection, systemBoundaryErrors := fixture.connectCredential(
		t,
		fixture.systemCredential,
		true,
	)
	defer systemBoundaryConnection.Close()
	expectPublishPermissionViolation(
		t,
		systemBoundaryConnection,
		systemBoundaryErrors,
		"vela.events.job.ready",
	)
	expectSubscribePermissionViolation(
		t,
		systemBoundaryConnection,
		systemBoundaryErrors,
		"vela.events.>",
	)

	bootstrap, bootstrapErrors := fixture.connectCredential(t, fixture.bootstrapCredential, true)
	defer bootstrap.Close()
	js, err := jetstream.New(bootstrap)
	if err != nil {
		t.Fatalf("create bootstrap JetStream client: %v", err)
	}
	stream, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:       "VELA_EVENTS",
		Subjects:   []string{"vela.events.>"},
		Storage:    jetstream.FileStorage,
		Duplicates: time.Minute,
	})
	if err != nil {
		t.Fatalf("create authenticated VELA_EVENTS stream: %v", err)
	}
	assertNoUnexpectedNATSError(t, bootstrapErrors)

	outboxErrors := make(chan error, 16)
	outboxConnection, err := natsauth.ConnectOutbox(
		fixture.outboxConfig(fixture.outboxCredential, fixture.outboxUser),
		natsauth.Handlers{AsyncError: func(err error) { outboxErrors <- err }},
	)
	if err != nil {
		t.Fatalf("connect replacement Outbox workload: %v", err)
	}
	defer outboxConnection.Close()

	scheduler, schedulerErrors := fixture.connectCredential(t, fixture.schedulerCredential, true)
	defer scheduler.Close()
	readySubscription, err := scheduler.SubscribeSync("vela.events.job.ready")
	if err != nil {
		t.Fatalf("Scheduler subscribe to job.ready: %v", err)
	}
	if err := scheduler.FlushTimeout(2 * time.Second); err != nil {
		t.Fatalf("flush Scheduler job.ready subscription: %v", err)
	}

	outboxJetStream, err := jetstream.New(outboxConnection.Conn)
	if err != nil {
		t.Fatalf("create authenticated Outbox JetStream client: %v", err)
	}
	const eventID = "00000000-0000-0000-0000-000000001801"
	receipt, err := outboxJetStream.Publish(
		context.Background(),
		"vela.events.job.ready",
		[]byte(`{"event_id":"`+eventID+`","event_type":"job.ready"}`),
		jetstream.WithMsgID(eventID),
		jetstream.WithExpectStream(eventstream.StreamName),
	)
	if err != nil {
		t.Fatalf("publish with exact Outbox workload credential: %v", err)
	}
	if receipt.Stream != "VELA_EVENTS" || receipt.Sequence != 1 {
		t.Fatalf("authenticated Outbox PubAck = %#v", receipt)
	}
	message, err := readySubscription.NextMsg(2 * time.Second)
	if err != nil || message.Subject != "vela.events.job.ready" {
		t.Fatalf("Scheduler received job.ready = subject %q error %v", messageSubject(message), err)
	}
	information, err := stream.Info(context.Background())
	if err != nil || information.State.Msgs != 1 {
		t.Fatalf("authenticated stream state = %#v error %v", information, err)
	}
	assertNoUnexpectedNATSError(t, outboxErrors)
	assertNoUnexpectedNATSError(t, schedulerErrors)

	rotatingCredential := filepath.Join(t.TempDir(), "outbox.creds")
	replaceCredentialFile(t, fixture.overlapOutboxCredential, rotatingCredential)
	reconnected := make(chan string, 1)
	disconnected := make(chan error, 1)
	rotationErrors := make(chan error, 4)
	rotatingConnection, err := natsauth.ConnectOutbox(
		fixture.outboxConfig(
			rotatingCredential,
			fixture.overlapOutboxUser,
			fixture.outboxUser,
		),
		natsauth.Handlers{
			Disconnect: func(err error) {
				select {
				case disconnected <- err:
				default:
				}
			},
			Reconnect: func(connectedURL string) {
				select {
				case reconnected <- connectedURL:
				default:
				}
			},
			AsyncError: func(err error) {
				select {
				case rotationErrors <- err:
				default:
				}
			},
		},
	)
	if err != nil {
		t.Fatalf("connect overlapping Outbox credential: %v", err)
	}
	defer rotatingConnection.Close()
	replaceCredentialFile(t, fixture.outboxCredential, rotatingCredential)
	replacementProbe, err := natsauth.ConnectOutbox(
		fixture.outboxConfig(
			rotatingCredential,
			fixture.overlapOutboxUser,
			fixture.outboxUser,
		),
		natsauth.Handlers{},
	)
	if err != nil {
		t.Fatalf("connect atomically replaced NKey before prior JWT expiry: %v", err)
	}
	replacementProbe.Close()
	select {
	case connectedURL := <-reconnected:
		if connectedURL != fixture.url {
			t.Fatalf("rotated Outbox reconnected to %q, want %q", connectedURL, fixture.url)
		}
	case <-time.After(15 * time.Second):
		var disconnectErr, asyncErr error
		select {
		case disconnectErr = <-disconnected:
		default:
		}
		select {
		case asyncErr = <-rotationErrors:
		default:
		}
		t.Fatalf(
			"Outbox did not reconnect with atomically replaced NKey credential; status=%s last_error=%v disconnect=%v async=%v",
			rotatingConnection.Status(),
			rotatingConnection.LastError(),
			disconnectErr,
			asyncErr,
		)
	}
	rotatingJetStream, err := jetstream.New(rotatingConnection.Conn)
	if err != nil {
		t.Fatalf("create rotated Outbox JetStream client: %v", err)
	}
	rotatedReceipt, err := rotatingJetStream.Publish(
		context.Background(),
		"vela.events.job.ready",
		[]byte(`{"event_type":"job.ready","rotation":true}`),
		jetstream.WithMsgID("00000000-0000-0000-0000-000000001802"),
		jetstream.WithExpectStream(eventstream.StreamName),
	)
	if err != nil || rotatedReceipt.Stream != "VELA_EVENTS" || rotatedReceipt.Sequence != 2 {
		t.Fatalf("rotated Outbox PubAck = %#v error %v", rotatedReceipt, err)
	}

	expectSubscribePermissionViolation(
		t, scheduler, schedulerErrors, "vela.events.invoice.export_requested",
	)
	expectSubscribePermissionViolation(t, scheduler, schedulerErrors, "$SYS.>")
	expectPublishPermissionViolation(t, scheduler, schedulerErrors, "vela.events.job.ready")
	expectPublishPermissionViolation(t, scheduler, schedulerErrors, "$SYS.REQ.SERVER.PING")

	expectSubscribePermissionViolation(t, outboxConnection.Conn, outboxErrors, "vela.events.>")
	expectPublishPermissionViolation(t, outboxConnection.Conn, outboxErrors, "$SYS.REQ.SERVER.PING")
	expectPublishPermissionViolation(t, outboxConnection.Conn, outboxErrors, "$JS.API.INFO")

	dnsUnavailableConfig := fixture.outboxConfig(fixture.outboxCredential, fixture.outboxUser)
	dnsUnavailableConfig.URL = dnsUnavailableURL
	dnsUnavailableConnection, err := natsauth.ConnectOutbox(
		dnsUnavailableConfig,
		natsauth.Handlers{},
	)
	if err != nil {
		t.Fatalf("start dormant Outbox connector during DNS outage: %v", err)
	}
	if dnsUnavailableConnection.IsConnected() || dnsUnavailableConnection.IsClosed() {
		dnsUnavailableConnection.Close()
		t.Fatalf(
			"DNS-outage connection status = %s, want dormant reconnecting",
			dnsUnavailableConnection.Status(),
		)
	}
	dnsUnavailableConnection.Close()

	proxyAddress, proxyURL := reserveNATSProxyAddress(t)
	degradedConfig := fixture.outboxConfig(fixture.outboxCredential, fixture.outboxUser)
	degradedConfig.URL = proxyURL
	degradedConnection, err := natsauth.ConnectOutbox(
		degradedConfig,
		natsauth.Handlers{},
	)
	if err != nil {
		t.Fatalf("start Outbox connector during NATS transport outage: %v", err)
	}
	defer degradedConnection.Close()
	if degradedConnection.IsConnected() || degradedConnection.IsClosed() {
		t.Fatalf(
			"degraded Outbox connection status = %s, want reconnecting without false acceptance",
			degradedConnection.Status(),
		)
	}
	if strings.Contains(strings.Join(degradedConnection.Servers(), ","), proxyURL) {
		t.Fatal("dormant degraded connection exposed real endpoints before activation")
	}
	degradedRevokedConfig := fixture.outboxConfig(
		fixture.revokedOutboxCredential,
		fixture.revokedOutboxUser,
	)
	degradedRevokedConfig.URL = proxyURL
	degradedRevokedConnection, err := natsauth.ConnectOutbox(
		degradedRevokedConfig,
		natsauth.Handlers{},
	)
	if err != nil {
		t.Fatalf("prepare revoked Outbox credential during transport outage: %v", err)
	}
	defer degradedRevokedConnection.Close()
	degradedConnected := degradedConnection.StatusChanged(nats.CONNECTED)
	revokedConnected := degradedRevokedConnection.StatusChanged(nats.CONNECTED)
	if err := degradedConnection.Activate(); err != nil {
		t.Fatalf("activate degraded Outbox endpoint pool: %v", err)
	}
	if err := degradedRevokedConnection.Activate(); err != nil {
		t.Fatalf("activate degraded revoked endpoint pool: %v", err)
	}
	if !strings.Contains(strings.Join(degradedConnection.Servers(), ","), proxyURL) {
		t.Fatal("activated degraded connection did not install the configured endpoint pool")
	}
	startNATSTCPProxy(t, proxyAddress, fixture.url)
	select {
	case <-degradedConnected:
		if connectedURL := degradedConnection.ConnectedUrl(); connectedURL != proxyURL {
			t.Fatalf("degraded Outbox connected to %q, want %q", connectedURL, proxyURL)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf(
			"degraded Outbox did not connect; status=%s last_error=%v servers=%v",
			degradedConnection.Status(),
			degradedConnection.LastError(),
			degradedConnection.Servers(),
		)
	}
	select {
	case <-revokedConnected:
		t.Fatal("degraded revoked Outbox credential reached connected state")
	case <-time.After(3 * time.Second):
	}
	if degradedRevokedConnection.IsConnected() || degradedRevokedConnection.IsClosed() {
		t.Fatalf(
			"degraded revoked Outbox state = %s, want fail-closed reconnecting",
			degradedRevokedConnection.Status(),
		)
	}
}

func TestSchedulerConsumerWorkloadIdentity(t *testing.T) {
	fixture := startAuthenticatedNATS(t)
	ctx := context.Background()
	bootstrap, bootstrapErrors := fixture.connectCredential(t, fixture.bootstrapCredential, true)
	t.Cleanup(bootstrap.Close)
	bootstrapJetStream, err := jetstream.New(bootstrap)
	if err != nil {
		t.Fatalf("create bootstrap JetStream client: %v", err)
	}
	streamConfig := eventstream.StreamConfig()
	streamConfig.Replicas = 1
	stream, err := bootstrapJetStream.CreateStream(ctx, streamConfig)
	if err != nil {
		t.Fatalf("create Scheduler workload test stream: %v", err)
	}
	consumerConfig := eventstream.SchedulerConsumerConfig()
	consumerConfig.Replicas = 1
	if _, err := stream.CreateConsumer(ctx, consumerConfig); err != nil {
		t.Fatalf("create Scheduler workload test consumer: %v", err)
	}
	assertNoUnexpectedNATSError(t, bootstrapErrors)

	legacyConnection, err := natsauth.ConnectScheduler(
		fixture.schedulerConfig(fixture.schedulerCredential, fixture.schedulerUser),
		natsauth.Handlers{},
	)
	if legacyConnection != nil {
		legacyConnection.Close()
		t.Fatal("Scheduler durable-consumer connector accepted the legacy subscription credential")
	}
	if !errors.Is(err, natsauth.ErrInvalidSchedulerCredential) {
		t.Fatalf("legacy Scheduler credential error = %v, want workload identity rejection", err)
	}

	schedulerErrors := make(chan error, 16)
	schedulerConnection, err := natsauth.ConnectScheduler(
		fixture.schedulerConfig(
			fixture.schedulerConsumerCredential,
			fixture.schedulerConsumerUser,
		),
		natsauth.Handlers{AsyncError: func(err error) { schedulerErrors <- err }},
	)
	if err != nil {
		t.Fatalf("connect Scheduler durable-consumer workload: %v", err)
	}
	t.Cleanup(schedulerConnection.Close)
	schedulerJetStream, err := jetstream.New(schedulerConnection.Conn)
	if err != nil {
		t.Fatalf("create Scheduler JetStream client: %v", err)
	}
	schedulerStream, err := schedulerJetStream.Stream(ctx, eventstream.StreamName)
	if err != nil {
		t.Fatalf("read release-owned Scheduler stream: %v", err)
	}
	schedulerConsumer, err := schedulerStream.Consumer(ctx, eventstream.SchedulerConsumerName)
	if err != nil {
		t.Fatalf("bind release-owned Scheduler durable consumer: %v", err)
	}

	outboxConnection, err := natsauth.ConnectOutbox(
		fixture.outboxConfig(fixture.outboxCredential, fixture.outboxUser),
		natsauth.Handlers{},
	)
	if err != nil {
		t.Fatalf("connect Outbox workload for Scheduler wakeup: %v", err)
	}
	t.Cleanup(outboxConnection.Close)
	outboxJetStream, err := jetstream.New(outboxConnection.Conn)
	if err != nil {
		t.Fatalf("create Scheduler wakeup Outbox JetStream client: %v", err)
	}
	messageID := uuid.NewString()
	if _, err := outboxJetStream.Publish(
		ctx,
		eventstream.SchedulerFilterSubject,
		[]byte("scheduler-wakeup"),
		jetstream.WithMsgID(messageID),
		jetstream.WithExpectStream(eventstream.StreamName),
	); err != nil {
		t.Fatalf("publish Scheduler wakeup with Outbox workload: %v", err)
	}
	delivery, err := schedulerConsumer.Next(jetstream.FetchMaxWait(2 * time.Second))
	if err != nil {
		t.Fatalf("fetch Scheduler wakeup with least-authority credential: %v", err)
	}
	if delivery.Headers().Get(jetstream.MsgIDHeader) != messageID {
		t.Fatalf("fetched Scheduler wakeup id = %q, want %q", delivery.Headers().Get(jetstream.MsgIDHeader), messageID)
	}
	if err := delivery.DoubleAck(ctx); err != nil {
		t.Fatalf("confirm Scheduler wakeup with least-authority credential: %v", err)
	}
	assertNoUnexpectedNATSError(t, schedulerErrors)

	expectSubscribePermissionViolation(
		t,
		schedulerConnection.Conn,
		schedulerErrors,
		eventstream.SchedulerFilterSubject,
	)
	expectPublishPermissionViolation(
		t,
		schedulerConnection.Conn,
		schedulerErrors,
		eventstream.SchedulerFilterSubject,
	)
	expectPublishPermissionViolation(
		t,
		schedulerConnection.Conn,
		schedulerErrors,
		"$JS.API.STREAM.DELETE."+eventstream.StreamName,
	)
}

func reserveNATSProxyAddress(t *testing.T) (string, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve degraded NATS proxy address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release degraded NATS proxy address: %v", err)
	}
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("parse degraded NATS proxy address: %v", err)
	}
	return address, "tls://localhost:" + port
}

func startNATSTCPProxy(t *testing.T, listenAddress, targetURL string) {
	t.Helper()
	parsedTarget, err := url.Parse(targetURL)
	if err != nil || parsedTarget.Host == "" {
		t.Fatalf("parse authenticated NATS proxy target: %v", err)
	}
	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		t.Fatalf("start degraded NATS proxy: %v", err)
	}
	t.Cleanup(func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close degraded NATS proxy: %v", err)
		}
	})
	go func() {
		for {
			client, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			upstream, dialErr := net.DialTimeout("tcp", parsedTarget.Host, 2*time.Second)
			if dialErr != nil {
				_ = client.Close()
				continue
			}
			go proxyNATSConnection(client, upstream)
		}
	}()
}

func proxyNATSConnection(client, upstream net.Conn) {
	defer client.Close()
	defer upstream.Close()
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(upstream, client)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(client, upstream)
		done <- struct{}{}
	}()
	<-done
}

type authenticatedNATSFixture struct {
	url                         string
	rootCAFile                  string
	workloadAccount             string
	workloadAccountSigner       string
	outboxUser                  string
	revokedOutboxUser           string
	outboxCredential            string
	revokedOutboxCredential     string
	overlapOutboxUser           string
	overlapOutboxCredential     string
	schedulerUser               string
	schedulerCredential         string
	schedulerConsumerUser       string
	schedulerConsumerCredential string
	bootstrapCredential         string
	systemCredential            string
	rootCAPEM                   []byte
	container                   testcontainers.Container
}

func startAuthenticatedNATS(t *testing.T) authenticatedNATSFixture {
	t.Helper()
	directory := t.TempDir()
	caCertificate, caKey, caPEM := issueWorkerTransportTestCA(t)
	serverCertificate, serverKey := issueWorkerTransportTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "localhost"},
		[]string{"localhost"},
		nil,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	)
	rootCAFile := filepath.Join(directory, "nats-root-ca.pem")
	if err := os.WriteFile(rootCAFile, caPEM, 0o600); err != nil {
		t.Fatalf("write NATS test root CA: %v", err)
	}

	operator := mustCreateOperatorKey(t)
	defer operator.Wipe()
	operatorSigner := mustCreateOperatorKey(t)
	defer operatorSigner.Wipe()
	operatorPublic := mustPublicKey(t, operator)
	operatorSignerPublic := mustPublicKey(t, operatorSigner)

	workloadAccount := mustCreateAccountKey(t)
	defer workloadAccount.Wipe()
	workloadSigner := mustCreateAccountKey(t)
	defer workloadSigner.Wipe()
	workloadAccountPublic := mustPublicKey(t, workloadAccount)
	workloadSignerPublic := mustPublicKey(t, workloadSigner)

	systemAccount := mustCreateAccountKey(t)
	defer systemAccount.Wipe()
	systemSigner := mustCreateAccountKey(t)
	defer systemSigner.Wipe()
	systemAccountPublic := mustPublicKey(t, systemAccount)
	systemSignerPublic := mustPublicKey(t, systemSigner)

	operatorClaims := jwt.NewOperatorClaims(operatorPublic)
	operatorClaims.Name = "Vela Integration Operator"
	operatorClaims.SigningKeys.Add(operatorSignerPublic)
	operatorClaims.SystemAccount = systemAccountPublic
	operatorJWT, err := operatorClaims.Encode(operator)
	if err != nil {
		t.Fatalf("encode NATS operator JWT: %v", err)
	}

	revokedOutbox := issueNATSUser(
		t, directory, "revoked-outbox.creds", workloadAccountPublic, workloadSigner,
		"vela-outbox-dispatcher", configureOutboxPermissions,
	)
	outboxUser := issueNATSUser(
		t, directory, "outbox.creds", workloadAccountPublic, workloadSigner,
		"vela-outbox-dispatcher", configureOutboxPermissions,
	)
	scheduler := issueNATSUser(
		t, directory, "scheduler.creds", workloadAccountPublic, workloadSigner,
		"vela-scheduler", func(claims *jwt.UserClaims) {
			claims.Pub.Deny.Add(">")
			claims.Sub.Allow.Add("vela.events.job.ready")
		},
	)
	schedulerConsumer := issueNATSUser(
		t, directory, "scheduler-consumer.creds", workloadAccountPublic, workloadSigner,
		"vela-scheduler-consumer", func(claims *jwt.UserClaims) {
			claims.Pub.Allow.Add("$JS.API.STREAM.INFO.VELA_EVENTS")
			claims.Pub.Allow.Add("$JS.API.CONSUMER.INFO.VELA_EVENTS.VELA_SCHEDULER")
			claims.Pub.Allow.Add("$JS.API.CONSUMER.MSG.NEXT.VELA_EVENTS.VELA_SCHEDULER")
			claims.Pub.Allow.Add("$JS.ACK.VELA_EVENTS.VELA_SCHEDULER.>")
			claims.Sub.Allow.Add("_INBOX.>")
		},
	)
	bootstrap := issueNATSUser(
		t, directory, "bootstrap.creds", workloadAccountPublic, workloadSigner,
		"vela-test-account-bootstrap", func(claims *jwt.UserClaims) {
			claims.Pub.Allow.Add("$JS.API.>")
			claims.Sub.Allow.Add("_INBOX.>")
		},
	)
	system := issueNATSUser(
		t, directory, "system.creds", systemAccountPublic, systemSigner,
		"vela-test-system", func(claims *jwt.UserClaims) {
			claims.Pub.Allow.Add("$SYS.>")
			claims.Sub.Allow.Add("$SYS.>")
		},
	)

	workloadClaims := jwt.NewAccountClaims(workloadAccountPublic)
	workloadClaims.Name = "Vela Workloads"
	workloadClaims.SigningKeys.Add(workloadSignerPublic)
	workloadClaims.Limits.JetStreamLimits = jwt.JetStreamLimits{
		MemoryStorage: jwt.NoLimit,
		DiskStorage:   jwt.NoLimit,
		Streams:       jwt.NoLimit,
		Consumer:      jwt.NoLimit,
	}
	workloadClaims.RevokeAt(revokedOutbox.publicKey, time.Now())
	workloadJWT, err := workloadClaims.Encode(operatorSigner)
	if err != nil {
		t.Fatalf("encode NATS workload account JWT: %v", err)
	}

	systemClaims := jwt.NewAccountClaims(systemAccountPublic)
	systemClaims.Name = "Vela System"
	systemClaims.SigningKeys.Add(systemSignerPublic)
	systemJWT, err := systemClaims.Encode(operatorSigner)
	if err != nil {
		t.Fatalf("encode NATS system account JWT: %v", err)
	}
	configuration := fmt.Sprintf(`
port: 4222
operator: %s
resolver: MEMORY
resolver_preload: {
  %s: %s
  %s: %s
}
jetstream {
  store_dir: "/data"
  # Keep the production-sized stream contract independent of CI runner disk size.
  max_file_store: 128GiB
}
tls {
  cert_file: "/etc/nats/server.crt"
  key_file: "/etc/nats/server.key"
  timeout: 2
}
`, operatorJWT, workloadAccountPublic, workloadJWT, systemAccountPublic, systemJWT)
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "nats:2.12-alpine",
			ExposedPorts: []string{"4222/tcp"},
			Cmd:          []string{"-c", "/etc/nats/nats.conf"},
			Files: []testcontainers.ContainerFile{
				{
					Reader: strings.NewReader(configuration), ContainerFilePath: "/etc/nats/nats.conf",
					FileMode: 0o600,
				},
				{
					Reader:            strings.NewReader(string(serverCertificate)),
					ContainerFilePath: "/etc/nats/server.crt", FileMode: 0o600,
				},
				{
					Reader:            strings.NewReader(string(serverKey)),
					ContainerFilePath: "/etc/nats/server.key", FileMode: 0o600,
				},
			},
			WaitingFor: wait.ForLog("Server is ready").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start authenticated NATS JetStream: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Errorf("terminate authenticated NATS JetStream: %v", err)
		}
	})
	endpoint, err := container.Endpoint(ctx, "")
	if err != nil {
		t.Fatalf("authenticated NATS endpoint: %v", err)
	}
	overlapOutbox := issueNATSUser(
		t, directory, "overlap-outbox.creds", workloadAccountPublic, workloadSigner,
		"vela-outbox-dispatcher", func(claims *jwt.UserClaims) {
			configureOutboxPermissions(claims)
			claims.Expires = time.Now().Add(10 * time.Second).Unix()
		},
	)
	return authenticatedNATSFixture{
		url:                         "tls://" + endpoint,
		rootCAFile:                  rootCAFile,
		workloadAccount:             workloadAccountPublic,
		workloadAccountSigner:       workloadSignerPublic,
		outboxUser:                  outboxUser.publicKey,
		revokedOutboxUser:           revokedOutbox.publicKey,
		outboxCredential:            outboxUser.path,
		revokedOutboxCredential:     revokedOutbox.path,
		overlapOutboxUser:           overlapOutbox.publicKey,
		overlapOutboxCredential:     overlapOutbox.path,
		schedulerUser:               scheduler.publicKey,
		schedulerCredential:         scheduler.path,
		schedulerConsumerUser:       schedulerConsumer.publicKey,
		schedulerConsumerCredential: schedulerConsumer.path,
		bootstrapCredential:         bootstrap.path,
		systemCredential:            system.path,
		rootCAPEM:                   caPEM,
		container:                   container,
	}
}

type issuedNATSUser struct {
	publicKey string
	path      string
}

func issueNATSUser(
	t *testing.T,
	directory string,
	filename string,
	accountPublicKey string,
	signer nkeys.KeyPair,
	name string,
	configure func(*jwt.UserClaims),
) issuedNATSUser {
	t.Helper()
	user, err := nkeys.CreateUser()
	if err != nil {
		t.Fatalf("create %s NATS user key: %v", name, err)
	}
	defer user.Wipe()
	userPublicKey := mustPublicKey(t, user)
	claims := jwt.NewUserClaims(userPublicKey)
	claims.Name = name
	claims.IssuerAccount = accountPublicKey
	claims.Expires = time.Now().Add(time.Hour).Unix()
	claims.AllowedConnectionTypes.Add(jwt.ConnectionTypeStandard)
	configure(claims)
	userJWT, err := claims.Encode(signer)
	if err != nil {
		t.Fatalf("encode %s NATS user JWT: %v", name, err)
	}
	seed, err := user.Seed()
	if err != nil {
		t.Fatalf("read %s NATS user seed: %v", name, err)
	}
	defer clear(seed)
	credential, err := jwt.FormatUserConfig(userJWT, seed)
	if err != nil {
		t.Fatalf("format %s NATS credential: %v", name, err)
	}
	path := filepath.Join(directory, filename)
	if err := os.WriteFile(path, credential, 0o600); err != nil {
		t.Fatalf("write %s NATS credential: %v", name, err)
	}
	clear(credential)
	return issuedNATSUser{publicKey: userPublicKey, path: path}
}

func configureOutboxPermissions(claims *jwt.UserClaims) {
	claims.Pub.Allow.Add("vela.events.>")
	claims.Pub.Allow.Add("$JS.API.STREAM.INFO.VELA_EVENTS")
	claims.Sub.Allow.Add("_INBOX.>")
}

func replaceCredentialFile(t *testing.T, source, destination string) {
	t.Helper()
	contents, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("read replacement NATS credential: %v", err)
	}
	defer clear(contents)
	temporary := destination + ".next"
	if err := os.WriteFile(temporary, contents, 0o600); err != nil {
		t.Fatalf("write replacement NATS credential: %v", err)
	}
	if err := os.Rename(temporary, destination); err != nil {
		t.Fatalf("replace NATS credential atomically: %v", err)
	}
}

func mustCreateOperatorKey(t *testing.T) nkeys.KeyPair {
	t.Helper()
	key, err := nkeys.CreateOperator()
	if err != nil {
		t.Fatalf("create NATS operator key: %v", err)
	}
	return key
}

func mustCreateAccountKey(t *testing.T) nkeys.KeyPair {
	t.Helper()
	key, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatalf("create NATS account key: %v", err)
	}
	return key
}

func mustPublicKey(t *testing.T, key nkeys.KeyPair) string {
	t.Helper()
	publicKey, err := key.PublicKey()
	if err != nil {
		t.Fatalf("read NATS public key: %v", err)
	}
	return publicKey
}

func (f authenticatedNATSFixture) outboxConfig(
	credentialFile string,
	expectedUsers ...string,
) natsauth.OutboxConfig {
	return natsauth.OutboxConfig{
		URL:                             f.url,
		CredentialsFile:                 credentialFile,
		RootCAFile:                      f.rootCAFile,
		ExpectedAccountPublicKey:        f.workloadAccount,
		ExpectedAccountSignerPublicKeys: []string{f.workloadAccountSigner},
		ExpectedUserPublicKeys:          expectedUsers,
	}
}

func (f authenticatedNATSFixture) schedulerConfig(
	credentialFile string,
	expectedUsers ...string,
) natsauth.SchedulerConfig {
	return natsauth.SchedulerConfig{
		URL:                             f.url,
		CredentialsFile:                 credentialFile,
		RootCAFile:                      f.rootCAFile,
		ExpectedAccountPublicKey:        f.workloadAccount,
		ExpectedAccountSignerPublicKeys: []string{f.workloadAccountSigner},
		ExpectedUserPublicKeys:          expectedUsers,
	}
}

func (f authenticatedNATSFixture) clientTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(f.rootCAPEM) {
		t.Fatal("append authenticated NATS root CA")
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: rootCAs}
}

func (f authenticatedNATSFixture) connectCredential(
	t *testing.T,
	credentialFile string,
	permissionErrorOnSubscribe bool,
) (*nats.Conn, chan error) {
	t.Helper()
	errorsChannel := make(chan error, 16)
	options := []nats.Option{
		nats.Secure(f.clientTLSConfig(t)),
		nats.UserCredentials(credentialFile),
		nats.Timeout(2 * time.Second),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			errorsChannel <- err
		}),
	}
	if permissionErrorOnSubscribe {
		options = append(options, nats.PermissionErrOnSubscribe(true))
	}
	connection, err := nats.Connect(f.url, options...)
	if err != nil {
		t.Fatalf("connect authenticated NATS fixture credential: %v", err)
	}
	return connection, errorsChannel
}

func expectPublishPermissionViolation(
	t *testing.T,
	connection *nats.Conn,
	errorsChannel chan error,
	subject string,
) {
	t.Helper()
	drainErrors(errorsChannel)
	if err := connection.Publish(subject, []byte("denied")); err != nil {
		t.Fatalf("queue denied publish to %s: %v", subject, err)
	}
	if err := connection.FlushTimeout(2 * time.Second); err != nil {
		t.Fatalf("flush denied publish to %s: %v", subject, err)
	}
	expectPermissionViolation(t, errorsChannel)
}

func expectSubscribePermissionViolation(
	t *testing.T,
	connection *nats.Conn,
	errorsChannel chan error,
	subject string,
) {
	t.Helper()
	drainErrors(errorsChannel)
	subscription, err := connection.SubscribeSync(subject)
	if errors.Is(err, nats.ErrPermissionViolation) {
		return
	}
	if err != nil {
		t.Fatalf("create denied subscription to %s: %v", subject, err)
	}
	defer func() { _ = subscription.Unsubscribe() }()
	if err := connection.FlushTimeout(2 * time.Second); err != nil {
		t.Fatalf("flush denied subscription to %s: %v", subject, err)
	}
	expectPermissionViolation(t, errorsChannel)
}

func expectPermissionViolation(t *testing.T, errorsChannel chan error) {
	t.Helper()
	select {
	case err := <-errorsChannel:
		if !errors.Is(err, nats.ErrPermissionViolation) {
			t.Fatalf("NATS asynchronous error = %v, want permission violation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for NATS permission violation")
	}
}

func assertNoUnexpectedNATSError(t *testing.T, errorsChannel chan error) {
	t.Helper()
	select {
	case err := <-errorsChannel:
		t.Fatalf("unexpected NATS asynchronous error: %v", err)
	default:
	}
}

func drainErrors(errorsChannel chan error) {
	for {
		select {
		case <-errorsChannel:
		default:
			return
		}
	}
}

func messageSubject(message *nats.Msg) string {
	if message == nil {
		return ""
	}
	return message.Subject
}
