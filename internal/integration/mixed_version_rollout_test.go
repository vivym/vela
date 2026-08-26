//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/eventstream"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/inbox"
	"github.com/vivym/vela/internal/scheduler"
	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

func TestExactNMinusOneMixedVersionRolloutDrainRollbackAndRetainedBacklog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	setLeaseRenewalProtocolGate(t, database.Admin, true, "Slice 29 adjacent N-1 rollout")
	enforceFleetProtocol(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("00000000-0000-0000-0000-000000000014")
	oldWorkerID := uuid.MustParse("23000000-0000-0000-0000-000000000440")
	seedSchedulerCapacityShares(t, database.Admin, poolID, schedulerCapacityShare{
		OrganizationID: testOrganizationID,
		ProjectID:      testProjectID,
		Weight:         1,
		RunningLimit:   2,
		ProjectWeight:  1,
	})
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, schedulerWorker{
		ID:       oldWorkerID,
		SPIFFEID: "spiffe://vela.internal/worker/h3-primary-fixture",
		Epoch:    7,
	})
	fleetService, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create current Fleet service: %v", err)
	}
	configureTestCapacityPolicies(t, fleetService, poolID)
	capacity, err := fleetService.ObserveCapacity(
		ctx,
		capacityObservation(oldWorkerID, poolID, 7, 1, 300, true),
	)
	if err != nil || !capacity.WorkerAssignmentAllowed || !capacity.PoolAssignmentAllowed {
		t.Fatalf("record mixed-version Worker capacity = %#v error=%v", capacity, err)
	}

	nMinusOne := buildNMinusOneBinaries(t, debugDumpNMinusOneCommit)
	assertAdjacentNMinusOneControlStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)

	oldJobID := uuid.MustParse(runNMinusOneAdmissionProbeWithKey(
		t,
		nMinusOne.AdmissionProbe,
		database.DSN,
		"slice-29-old-admission",
	))
	var oldEventID uuid.UUID
	var oldPayload []byte
	if err := database.Admin.QueryRow(`
		SELECT event_id, payload
		FROM outbox_events
		WHERE aggregate_id = $1 AND event_type = 'job.ready'
	`, oldJobID).Scan(&oldEventID, &oldPayload); err != nil {
		t.Fatalf("read exact N-1 retained event: %v", err)
	}
	oldPayloadDigest := sha256.Sum256(oldPayload)

	internalPool := newRolePool(
		t, database.DSN, "vela_internal_login", "vela-internal-password",
	)
	coordinator, err := newWorkerControlService(internalPool)
	if err != nil {
		t.Fatalf("create current Assignment coordinator: %v", err)
	}
	schedulerPool := newRolePool(
		t, database.DSN, "vela_scheduler_login", "vela-scheduler-password",
	)
	currentScheduler, err := scheduler.NewService(schedulerPool, coordinator, scheduler.Config{
		SchedulerID:       "slice-29-current-scheduler",
		ClaimTTL:          30 * time.Second,
		CandidateAttempts: 1,
	})
	if err != nil {
		t.Fatalf("create current Scheduler: %v", err)
	}

	cluster := startThreeNodeJetStream(t)
	natsConnection, err := nats.Connect(
		strings.Join(cluster.urls, ","),
		nats.Timeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("connect mixed-version JetStream: %v", err)
	}
	t.Cleanup(natsConnection.Close)
	jetStream, err := jetstream.New(natsConnection)
	if err != nil {
		t.Fatalf("create mixed-version JetStream client: %v", err)
	}
	stream := createReleaseStream(t, jetStream, cluster.containers)
	waitForReleaseStreamCurrent(t, stream, cluster.containers...)
	durableConsumer, err := stream.CreateConsumer(ctx, eventstream.SchedulerConsumerConfig())
	if err != nil {
		t.Fatalf("create mixed-version Scheduler consumer: %v", err)
	}
	waitForReleaseConsumerCurrent(t, durableConsumer)

	published := runNMinusOneJetStreamOutboxProbe(
		t,
		nMinusOne.JetStreamOutboxProbe,
		database.DSN,
		strings.Join(cluster.urls, ","),
	)
	if published.EventID != oldEventID || published.Stream != eventstream.StreamName ||
		published.Sequence < 1 {
		t.Fatalf("exact N-1 retained publish receipt = %#v", published)
	}
	message, err := durableConsumer.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatalf("fetch exact N-1 retained event: %v", err)
	}
	if message.Headers().Get(jetstream.MsgIDHeader) != oldEventID.String() ||
		!bytes.Equal(message.Data(), oldPayload) {
		t.Fatalf("retained event identity or raw payload changed before current consumption")
	}

	inboxProcessor, err := inbox.NewSchedulerProcessor(newRolePool(
		t,
		database.DSN,
		"vela_scheduler_inbox_login",
		"vela-scheduler-inbox-password",
	))
	if err != nil {
		t.Fatalf("create current Scheduler Inbox processor: %v", err)
	}
	messageConsumer, err := inbox.NewJetStreamConsumer(inboxProcessor)
	if err != nil {
		t.Fatalf("create current JetStream Inbox consumer: %v", err)
	}
	var currentDispatches []scheduler.Dispatch
	applied, err := messageConsumer.ProcessMessage(
		ctx,
		message,
		func(handlerContext context.Context, _ pgx.Tx) error {
			var cycleErr error
			currentDispatches, cycleErr = currentScheduler.RunCycle(handlerContext)
			return cycleErr
		},
	)
	if err != nil || !applied || len(currentDispatches) != 1 ||
		currentDispatches[0].Assignment.JobID != oldJobID ||
		currentDispatches[0].Assignment.WorkerID != oldWorkerID {
		t.Fatalf(
			"current consumer/Scheduler retained backlog result = applied %t dispatches %#v error %v",
			applied,
			currentDispatches,
			err,
		)
	}

	workerEndpoint := startMixedVersionWorkerTransportServer(
		t,
		database,
		coordinator,
	)
	started := runNMinusOneWorkerTransportProbe(
		t,
		nMinusOne.WorkerTransportProbe,
		workerEndpoint,
		nMinusOneWorkerProbeRequest{Action: "start", WorkerEpoch: 7},
	)
	assignment := currentDispatches[0].Assignment
	if started.JobID != oldJobID || started.AttemptID != assignment.AttemptID ||
		started.WorkerID != oldWorkerID || started.WorkerEpoch != 7 ||
		started.LeaseFence != assignment.LeaseFence || started.LeaseToken == "" ||
		started.StartDecision != workercontrol.StartGranted ||
		started.HeartbeatDecision != workercontrol.HeartbeatContinue {
		t.Fatalf("exact N-1 Worker start result = %#v", started)
	}

	drainID := uuid.MustParse("23000000-0000-0000-0000-000000000441")
	drainRequest := fleet.DrainRequest{
		OperationID:   drainID,
		WorkerID:      oldWorkerID,
		ExpectedEpoch: 7,
		Reason:        "Slice 29 adjacent Worker rollout",
		Deadline:      time.Now().Add(time.Hour),
		RequestedBy:   "fleet-controller/slice-29",
	}
	draining, err := fleetService.RequestDrain(ctx, drainRequest)
	if err != nil || draining.State != fleet.DrainDraining ||
		draining.WorkerLifecycle != "DRAINING" {
		t.Fatalf("request mixed-version active Worker drain = %#v error=%v", draining, err)
	}
	stillDraining, err := fleetService.ReconcileDrain(
		ctx,
		drainID,
		"fleet-controller/slice-29",
	)
	if err != nil || stillDraining.State != fleet.DrainDraining {
		t.Fatalf("active Lease completed drain early = %#v error=%v", stillDraining, err)
	}

	continued := runNMinusOneWorkerTransportProbe(
		t,
		nMinusOne.WorkerTransportProbe,
		workerEndpoint,
		nMinusOneWorkerProbeRequest{
			Action:      "heartbeat-fail",
			WorkerEpoch: started.WorkerEpoch,
			AttemptID:   started.AttemptID,
			LeaseFence:  started.LeaseFence,
			LeaseToken:  started.LeaseToken,
		},
	)
	if continued.HeartbeatDecision != workercontrol.HeartbeatContinue ||
		continued.FailureDisposition != workercontrol.RetryDispositionFailed {
		t.Fatalf("exact N-1 Worker did not continue through drain = %#v", continued)
	}
	completedDrain, err := fleetService.ReconcileDrain(
		ctx,
		drainID,
		"fleet-controller/slice-29",
	)
	if err != nil || completedDrain.State != fleet.DrainComplete ||
		completedDrain.WorkerLifecycle != "DRAINING" {
		t.Fatalf("complete mixed-version Worker drain = %#v error=%v", completedDrain, err)
	}

	rollbackWorkerID := uuid.MustParse("23000000-0000-0000-0000-000000000442")
	seedSchedulerWorkers(t, database.Admin, poolID, profileID, schedulerWorker{
		ID:       rollbackWorkerID,
		SPIFFEID: "spiffe://vela.internal/worker/slice-29-rollback",
		Epoch:    8,
	})
	rollbackCapacity, err := fleetService.ObserveCapacity(
		ctx,
		capacityObservation(rollbackWorkerID, poolID, 8, 1, 300, true),
	)
	if err != nil || !rollbackCapacity.WorkerAssignmentAllowed ||
		!rollbackCapacity.PoolAssignmentAllowed {
		t.Fatalf("record rollback Worker capacity = %#v error=%v", rollbackCapacity, err)
	}
	currentServer := admissionServerForDatabase(t, database)
	rollbackJobID := submitSchedulerJob(
		t,
		currentServer.URL,
		testProjectID,
		testBearerCredential(),
		"slice-29-current-admission-before-rollback",
	)
	assertAdjacentNMinusOneControlStartupPassed(
		t,
		runSchedulerNMinusOneStartupProbe(t, nMinusOne.Control, database.DSN),
	)
	rollbackAdmissionID := uuid.MustParse(runNMinusOneAdmissionProbeWithKey(
		t,
		nMinusOne.AdmissionProbe,
		database.DSN,
		"slice-29-n-minus-one-admission-after-rollback",
	))
	rollbackDispatch := runNMinusOneSchedulerProbe(
		t,
		nMinusOne.SchedulerProbe,
		database.DSN,
		poolID,
		"slice-29-n-minus-one-rollback",
	)
	if rollbackDispatch.SQLState != "" || !rollbackDispatch.Dispatched ||
		rollbackDispatch.JobID != rollbackJobID ||
		rollbackDispatch.WorkerID != rollbackWorkerID ||
		rollbackDispatch.IntentID == uuid.Nil || rollbackDispatch.AttemptID == uuid.Nil {
		t.Fatalf("exact N-1 Scheduler rollback result = %#v", rollbackDispatch)
	}
	var (
		oldJobState       string
		oldAttemptState   string
		oldLeaseRevoked   bool
		inboxReceipts     int
		rolloutJobs       int
		rolloutAttempts   int
		rolloutLeases     int
		rolloutDispatches int
	)
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT state::text FROM jobs WHERE id = $1),
			(SELECT state::text FROM attempts WHERE id = $2),
			(SELECT revoked_at IS NOT NULL FROM attempt_leases WHERE attempt_id = $2),
			(SELECT count(*) FROM inbox_receipts
			 WHERE consumer_name = 'scheduler' AND event_id = $3),
			(SELECT count(*) FROM jobs WHERE id = ANY($4::uuid[])),
			(SELECT count(*) FROM attempts WHERE job_id = ANY($4::uuid[])),
			(SELECT count(*) FROM attempt_leases
			 WHERE attempt_id IN (SELECT id FROM attempts WHERE job_id = ANY($4::uuid[]))),
			(SELECT count(*) FROM scheduler_dispatch_intents
			 WHERE job_id = ANY($4::uuid[]))
	`,
		oldJobID,
		assignment.AttemptID,
		oldEventID,
		[]uuid.UUID{oldJobID, rollbackJobID, rollbackAdmissionID},
	).Scan(
		&oldJobState,
		&oldAttemptState,
		&oldLeaseRevoked,
		&inboxReceipts,
		&rolloutJobs,
		&rolloutAttempts,
		&rolloutLeases,
		&rolloutDispatches,
	); err != nil {
		t.Fatalf("read mixed-version rollout authority: %v", err)
	}
	if oldJobState != "FAILED" || oldAttemptState != "FAILED" || !oldLeaseRevoked ||
		inboxReceipts != 1 || rolloutJobs != 3 || rolloutAttempts != 2 ||
		rolloutLeases != 2 || rolloutDispatches != 2 {
		t.Fatalf(
			"mixed-version authority = old %s/%s revoked %t inbox %d jobs/attempts/leases/dispatches %d/%d/%d/%d",
			oldJobState,
			oldAttemptState,
			oldLeaseRevoked,
			inboxReceipts,
			rolloutJobs,
			rolloutAttempts,
			rolloutLeases,
			rolloutDispatches,
		)
	}
	t.Logf(
		"mixed-version rollout current=%s n_minus_one=%s event=%s payload_sha256=%x old_job=%s old_attempt=%s old_worker=%s drain=%s rollback_job=%s rollback_intent=%s rollback_attempt=%s rollback_admission=%s authority=jobs:%d,attempts:%d,leases:%d,dispatches:%d,inbox:%d",
		"507384774052efda1f14f5689e6bee487ba3259e",
		debugDumpNMinusOneCommit,
		oldEventID,
		oldPayloadDigest,
		oldJobID,
		assignment.AttemptID,
		oldWorkerID,
		drainID,
		rollbackJobID,
		rollbackDispatch.IntentID,
		rollbackDispatch.AttemptID,
		rollbackAdmissionID,
		rolloutJobs,
		rolloutAttempts,
		rolloutLeases,
		rolloutDispatches,
		inboxReceipts,
	)
}

type mixedVersionWorkerEndpoint struct {
	Address               string
	ServerName            string
	ClientCertificateFile string
	ClientKeyFile         string
	ServerCAFile          string
}

func startMixedVersionWorkerTransportServer(
	t *testing.T,
	database testDatabase,
	coordinator workertransport.WorkerCoordinator,
) mixedVersionWorkerEndpoint {
	t.Helper()
	internalPool := newRolePool(
		t,
		database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	resolver, err := workertransport.NewPostgresIdentityResolver(internalPool)
	if err != nil {
		t.Fatalf("create mixed-version Worker identity resolver: %v", err)
	}
	server, err := workertransport.NewServer(
		resolver,
		coordinator,
		unavailableArtifactUploadStore{},
	)
	if err != nil {
		t.Fatalf("create mixed-version Worker transport server: %v", err)
	}

	caCertificate, caKey, caPEM := issueWorkerTransportTestCA(t)
	serverCertificate, serverKey := issueWorkerTransportTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "worker-control.internal"},
		[]string{"worker-control.internal"},
		nil,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	)
	workerSPIFFEID, err := url.Parse("spiffe://vela.internal/worker/h3-primary-fixture")
	if err != nil {
		t.Fatalf("parse mixed-version Worker SPIFFE ID: %v", err)
	}
	clientCertificate, clientKey := issueWorkerTransportTestCertificate(
		t,
		caCertificate,
		caKey,
		pkix.Name{CommonName: "h3-primary-fixture"},
		nil,
		[]*url.URL{workerSPIFFEID},
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	directory := t.TempDir()
	endpoint := mixedVersionWorkerEndpoint{
		ServerName:            "worker-control.internal",
		ClientCertificateFile: filepath.Join(directory, "worker.crt"),
		ClientKeyFile:         filepath.Join(directory, "worker.key"),
		ServerCAFile:          filepath.Join(directory, "server-ca.crt"),
	}
	serverCertificateFile := filepath.Join(directory, "server.crt")
	serverKeyFile := filepath.Join(directory, "server.key")
	for path, contents := range map[string][]byte{
		endpoint.ClientCertificateFile: clientCertificate,
		endpoint.ClientKeyFile:         clientKey,
		endpoint.ServerCAFile:          caPEM,
		serverCertificateFile:          serverCertificate,
		serverKeyFile:                  serverKey,
	} {
		if err := os.WriteFile(path, contents, 0o600); err != nil {
			t.Fatalf("write mixed-version Worker TLS fixture %s: %v", path, err)
		}
	}
	serverCredentials, err := workertransport.NewServerTLSCredentials(
		serverCertificateFile,
		serverKeyFile,
		endpoint.ServerCAFile,
	)
	if err != nil {
		t.Fatalf("create mixed-version Worker server TLS: %v", err)
	}
	grpcServer := grpc.NewServer(grpc.Creds(serverCredentials))
	velav1.RegisterWorkerControlServiceServer(grpcServer, server)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for mixed-version Worker transport: %v", err)
	}
	endpoint.Address = listener.Addr().String()
	serverDone := make(chan error, 1)
	go func() { serverDone <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		if serveErr := <-serverDone; serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			t.Errorf("serve mixed-version Worker transport: %v", serveErr)
		}
	})
	return endpoint
}

type unavailableArtifactUploadStore struct{}

func (unavailableArtifactUploadStore) CreateMultipartUpload(
	context.Context,
	string,
	string,
) (artifactstore.MultipartUpload, error) {
	return artifactstore.MultipartUpload{}, errors.New("Artifact upload is outside rollout conformance")
}

func (unavailableArtifactUploadStore) ListParts(
	context.Context,
	artifactstore.MultipartUpload,
) ([]artifactstore.CompletedPart, error) {
	return nil, errors.New("Artifact upload is outside rollout conformance")
}

func (unavailableArtifactUploadStore) PresignUploadPart(
	context.Context,
	artifactstore.MultipartUpload,
	int32,
	int64,
	[sha256.Size]byte,
	time.Time,
) (artifactstore.SignedUploadPart, error) {
	return artifactstore.SignedUploadPart{}, errors.New("Artifact upload is outside rollout conformance")
}

func (unavailableArtifactUploadStore) CompleteMultipartUpload(
	context.Context,
	artifactstore.MultipartUpload,
	[]artifactstore.CompletedPart,
) (artifactstore.ObjectVersion, error) {
	return artifactstore.ObjectVersion{}, errors.New("Artifact upload is outside rollout conformance")
}

func (unavailableArtifactUploadStore) HeadCurrentVersion(
	context.Context,
	string,
) (artifactstore.ObjectVersion, error) {
	return artifactstore.ObjectVersion{}, errors.New("Artifact upload is outside rollout conformance")
}

func (unavailableArtifactUploadStore) AbortMultipartUpload(
	context.Context,
	artifactstore.MultipartUpload,
) error {
	return errors.New("Artifact upload is outside rollout conformance")
}
