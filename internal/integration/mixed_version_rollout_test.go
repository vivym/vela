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
	_ = newFinanceReconciliationService(t, database)
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

	nMinusOne := buildNMinusOneBinaries(t, adjacentRolloutNMinusOneCommit)
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
	authorityExpectation := mixedVersionAuthorityExpectation{
		OldJobID:            oldJobID,
		OldIntentID:         currentDispatches[0].IntentID,
		OldAttemptID:        assignment.AttemptID,
		OldEventID:          oldEventID,
		OldWorkerID:         oldWorkerID,
		OldLeaseFence:       assignment.LeaseFence,
		RollbackJobID:       rollbackJobID,
		RollbackAdmissionID: rollbackAdmissionID,
		RollbackIntentID:    rollbackDispatch.IntentID,
		RollbackAttemptID:   rollbackDispatch.AttemptID,
		RollbackWorkerID:    rollbackWorkerID,
		RollbackLeaseFence:  rollbackDispatch.LeaseFence,
	}
	authority := readMixedVersionAuthoritySnapshot(t, database, authorityExpectation)
	assertMixedVersionAuthoritySnapshot(t, authority, authorityExpectation)
	t.Logf(
		"mixed-version rollout current=%s n_minus_one=%s event=%s payload_sha256=%x old_job=%s old_intent=%s old_attempt=%s old_lease=%s old_lease_fence=%d old_worker=%s drain=%s inbox=%s/%s/%s/%s/%s/%s/%d/%s rollback_job=%s rollback_intent=%s rollback_attempt=%s rollback_lease=%s rollback_lease_fence=%d rollback_worker=%s rollback_admission=%s authority=jobs:%d,attempts:%d,leases:%d,dispatches:%d,inbox:1",
		"507384774052efda1f14f5689e6bee487ba3259e",
		adjacentRolloutNMinusOneCommit,
		oldEventID,
		oldPayloadDigest,
		oldJobID,
		authority.Old.IntentID,
		authority.Old.AttemptID,
		authority.Old.LeaseID,
		authority.Old.LeaseFence,
		oldWorkerID,
		drainID,
		authority.Inbox.Consumer,
		authority.Inbox.EventID,
		authority.Inbox.OrganizationID,
		authority.Inbox.ProjectID,
		authority.Inbox.AggregateType,
		authority.Inbox.AggregateID,
		authority.Inbox.AggregateVersion,
		authority.Inbox.EventType,
		rollbackJobID,
		authority.Rollback.IntentID,
		authority.Rollback.AttemptID,
		authority.Rollback.LeaseID,
		authority.Rollback.LeaseFence,
		authority.Rollback.LeaseWorkerID,
		rollbackAdmissionID,
		authority.Counts.Jobs,
		authority.Counts.Attempts,
		authority.Counts.Leases,
		authority.Counts.Dispatches,
	)
}

type mixedVersionAuthorityChain struct {
	JobState      string
	AttemptState  string
	IntentID      uuid.UUID
	AttemptID     uuid.UUID
	LeaseID       uuid.UUID
	LeaseWorkerID uuid.UUID
	LeaseFence    int64
	LeaseRevoked  bool
}

type mixedVersionInboxIdentity struct {
	Consumer         string
	EventID          uuid.UUID
	OrganizationID   uuid.UUID
	ProjectID        uuid.UUID
	AggregateType    string
	AggregateID      uuid.UUID
	AggregateVersion int64
	EventType        string
}

type mixedVersionAuthorityCounts struct {
	Jobs       int
	Attempts   int
	Leases     int
	Dispatches int
}

type mixedVersionAuthoritySnapshot struct {
	Old                    mixedVersionAuthorityChain
	Inbox                  mixedVersionInboxIdentity
	Rollback               mixedVersionAuthorityChain
	RollbackAdmissionState string
	Counts                 mixedVersionAuthorityCounts
}

type mixedVersionAuthorityExpectation struct {
	OldJobID            uuid.UUID
	OldIntentID         uuid.UUID
	OldAttemptID        uuid.UUID
	OldEventID          uuid.UUID
	OldWorkerID         uuid.UUID
	OldLeaseFence       int64
	RollbackJobID       uuid.UUID
	RollbackAdmissionID uuid.UUID
	RollbackIntentID    uuid.UUID
	RollbackAttemptID   uuid.UUID
	RollbackWorkerID    uuid.UUID
	RollbackLeaseFence  int64
}

func readMixedVersionAuthoritySnapshot(
	t *testing.T,
	database testDatabase,
	expected mixedVersionAuthorityExpectation,
) mixedVersionAuthoritySnapshot {
	t.Helper()
	var snapshot mixedVersionAuthoritySnapshot
	err := database.Admin.QueryRow(`
		SELECT
			old_job.state::text,
			old_attempt.state::text,
			old_intent.id,
			old_attempt.id,
			old_lease.id,
			old_lease.worker_id,
			old_lease.fence,
			old_lease.revoked_at IS NOT NULL,
			inbox.consumer_name,
			inbox.event_id,
			inbox.organization_id,
			inbox.project_id,
			inbox.aggregate_type,
			inbox.aggregate_id,
			inbox.aggregate_version,
			inbox.event_type,
			rollback_job.state::text,
			rollback_attempt.state::text,
			rollback_intent.id,
			rollback_attempt.id,
			rollback_lease.id,
			rollback_lease.worker_id,
			rollback_lease.fence,
			rollback_lease.revoked_at IS NOT NULL,
			rollback_admission.state::text,
			(SELECT count(*) FROM jobs WHERE id = ANY($9::uuid[])),
			(SELECT count(*) FROM attempts WHERE job_id = ANY($9::uuid[])),
			(SELECT count(*) FROM attempt_leases
			 WHERE attempt_id IN (SELECT id FROM attempts WHERE job_id = ANY($9::uuid[]))),
			(SELECT count(*) FROM scheduler_dispatch_intents
			 WHERE job_id = ANY($9::uuid[]))
		FROM jobs AS old_job
		JOIN scheduler_dispatch_intents AS old_intent
		  ON old_intent.id = $2 AND old_intent.job_id = old_job.id
		JOIN attempts AS old_attempt
		  ON old_attempt.id = $3
		 AND old_attempt.job_id = old_job.id
		 AND old_attempt.scheduler_dispatch_intent_id = old_intent.id
		JOIN attempt_leases AS old_lease ON old_lease.attempt_id = old_attempt.id
		JOIN inbox_receipts AS inbox
		  ON inbox.consumer_name = 'scheduler' AND inbox.event_id = $4
		JOIN jobs AS rollback_job ON rollback_job.id = $5
		JOIN jobs AS rollback_admission ON rollback_admission.id = $6
		JOIN scheduler_dispatch_intents AS rollback_intent
		  ON rollback_intent.id = $7 AND rollback_intent.job_id = rollback_job.id
		JOIN attempts AS rollback_attempt
		  ON rollback_attempt.id = $8
		 AND rollback_attempt.job_id = rollback_job.id
		 AND rollback_attempt.scheduler_dispatch_intent_id = rollback_intent.id
		JOIN attempt_leases AS rollback_lease ON rollback_lease.attempt_id = rollback_attempt.id
		WHERE old_job.id = $1
	`,
		expected.OldJobID,
		expected.OldIntentID,
		expected.OldAttemptID,
		expected.OldEventID,
		expected.RollbackJobID,
		expected.RollbackAdmissionID,
		expected.RollbackIntentID,
		expected.RollbackAttemptID,
		[]uuid.UUID{expected.OldJobID, expected.RollbackJobID, expected.RollbackAdmissionID},
	).Scan(
		&snapshot.Old.JobState,
		&snapshot.Old.AttemptState,
		&snapshot.Old.IntentID,
		&snapshot.Old.AttemptID,
		&snapshot.Old.LeaseID,
		&snapshot.Old.LeaseWorkerID,
		&snapshot.Old.LeaseFence,
		&snapshot.Old.LeaseRevoked,
		&snapshot.Inbox.Consumer,
		&snapshot.Inbox.EventID,
		&snapshot.Inbox.OrganizationID,
		&snapshot.Inbox.ProjectID,
		&snapshot.Inbox.AggregateType,
		&snapshot.Inbox.AggregateID,
		&snapshot.Inbox.AggregateVersion,
		&snapshot.Inbox.EventType,
		&snapshot.Rollback.JobState,
		&snapshot.Rollback.AttemptState,
		&snapshot.Rollback.IntentID,
		&snapshot.Rollback.AttemptID,
		&snapshot.Rollback.LeaseID,
		&snapshot.Rollback.LeaseWorkerID,
		&snapshot.Rollback.LeaseFence,
		&snapshot.Rollback.LeaseRevoked,
		&snapshot.RollbackAdmissionState,
		&snapshot.Counts.Jobs,
		&snapshot.Counts.Attempts,
		&snapshot.Counts.Leases,
		&snapshot.Counts.Dispatches,
	)
	if err != nil {
		t.Fatalf("read mixed-version rollout authority: %v", err)
	}
	return snapshot
}

func assertMixedVersionAuthoritySnapshot(
	t *testing.T,
	snapshot mixedVersionAuthoritySnapshot,
	expected mixedVersionAuthorityExpectation,
) {
	t.Helper()
	if snapshot.Old.JobState != "FAILED" || snapshot.Old.AttemptState != "FAILED" ||
		!snapshot.Old.LeaseRevoked || snapshot.Old.IntentID != expected.OldIntentID ||
		snapshot.Old.AttemptID != expected.OldAttemptID || snapshot.Old.LeaseID == uuid.Nil ||
		snapshot.Old.LeaseWorkerID != expected.OldWorkerID ||
		snapshot.Old.LeaseFence != expected.OldLeaseFence ||
		snapshot.Inbox.Consumer != "scheduler" || snapshot.Inbox.EventID != expected.OldEventID ||
		snapshot.Inbox.OrganizationID != uuid.MustParse(testOrganizationID) ||
		snapshot.Inbox.ProjectID != uuid.MustParse(testProjectID) ||
		snapshot.Inbox.AggregateType != "Job" || snapshot.Inbox.AggregateID != expected.OldJobID ||
		snapshot.Inbox.AggregateVersion < 1 || snapshot.Inbox.EventType != "job.ready" ||
		snapshot.Rollback.JobState != "ASSIGNED" || snapshot.Rollback.AttemptState != "ASSIGNED" ||
		snapshot.Rollback.LeaseRevoked || snapshot.Rollback.IntentID != expected.RollbackIntentID ||
		snapshot.Rollback.AttemptID != expected.RollbackAttemptID ||
		snapshot.Rollback.LeaseID == uuid.Nil ||
		snapshot.Rollback.LeaseWorkerID != expected.RollbackWorkerID ||
		snapshot.Rollback.LeaseFence != expected.RollbackLeaseFence ||
		snapshot.RollbackAdmissionState != "QUEUED" || snapshot.Counts.Jobs != 3 ||
		snapshot.Counts.Attempts != 2 || snapshot.Counts.Leases != 2 ||
		snapshot.Counts.Dispatches != 2 {
		t.Fatalf("mixed-version authority mismatch: snapshot=%#v expected=%#v", snapshot, expected)
	}
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
