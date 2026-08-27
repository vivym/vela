//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestWorkerMTLSIdentityResolvesExactRegisteredSPIFFEID(t *testing.T) {
	fixture := newAssignmentFixture(t, "worker-transport-identity", 7)
	internal := newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	resolver, err := workertransport.NewPostgresIdentityResolver(internal)
	if err != nil {
		t.Fatalf("NewPostgresIdentityResolver: %v", err)
	}
	worker, err := resolver.ResolveWorker(
		context.Background(),
		"spiffe://vela.internal/worker/h3-primary-fixture",
	)
	if err != nil {
		t.Fatalf("resolve registered Worker SPIFFE ID: %v", err)
	}
	if worker.ID != uuid.MustParse(testWorkerID) ||
		worker.PoolID != uuid.MustParse("00000000-0000-0000-0000-000000000005") ||
		worker.SPIFFEID != "spiffe://vela.internal/worker/h3-primary-fixture" {
		t.Fatalf("resolved Worker = %#v", worker)
	}
	if _, err := resolver.ResolveWorker(
		context.Background(),
		"spiffe://vela.internal/worker/not-registered",
	); err == nil {
		t.Fatal("resolved an unregistered Worker SPIFFE ID")
	}
}

func TestWorkerControlReportsAuthoritativeCapacityOverMTLS(t *testing.T) {
	fixture := newAssignmentFixture(t, "worker-transport-capacity", 7)
	fleetService, err := fleet.NewService(newRolePool(
		t, fixture.database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	configureTestCapacityPolicies(t, fleetService, poolID)
	minio := newMinIOFixture(t, "vela-worker-transport-capacity")
	client := newWorkerTransportTestClient(
		t, fixture.database, fixture.service, minio.store, fleetService,
	)
	observation := workercontrol.CapacityObservation{
		WorkerEpoch: 7, Sequence: 1, ObservedAt: time.Now().UTC(),
		WatermarkState: workercontrol.ScratchWatermarkNormal,
		TotalBytes:     1000, FreeBytes: 700,
		HighWatermarkBytes: 800, LowWatermarkBytes: 400, CriticalFreeBytes: 50,
		ArtifactStoreReachable: false,
	}
	result, err := client.ReportCapacity(context.Background(), observation)
	if err != nil || result.WorkerPoolID != poolID ||
		result.WorkerState != workercontrol.CapacityStorageUnavailable ||
		result.PoolState != workercontrol.CapacityStorageUnavailable ||
		!result.WorkerAssignmentAllowed || !result.PoolAssignmentAllowed {
		t.Fatalf("ReportCapacity = %#v error=%v", result, err)
	}
	enforceFleetProtocol(t, fixture.database.Admin)
	replayed, err := client.ReportCapacity(context.Background(), observation)
	if err != nil || !replayed.Replayed || replayed.WorkerPoolID != poolID ||
		replayed.WorkerState != workercontrol.CapacityStorageUnavailable ||
		replayed.PoolState != workercontrol.CapacityStorageUnavailable ||
		replayed.WorkerAssignmentAllowed || replayed.PoolAssignmentAllowed {
		t.Fatalf("ReportCapacity after Fleet enforcement = %#v error=%v", replayed, err)
	}
	pool, err := fleetService.GetPoolCapacity(context.Background(), poolID)
	if err != nil || pool.PoolState != fleet.CapacityStorageUnavailable ||
		pool.PoolAssignmentAllowed {
		t.Fatalf("authoritative pool capacity = %#v error=%v", pool, err)
	}
}

func TestWorkerControlExecutesCurrentReadinessWorkOverMTLS(t *testing.T) {
	fixture := newAssignmentFixture(t, "worker-transport-readiness", 7)
	enforceFleetProtocol(t, fixture.database.Admin)
	workerID := uuid.MustParse(testWorkerID)
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := fixture.candidate.ExecutionProfileRevisionID
	var nodeIdentity string
	if err := fixture.database.Admin.QueryRow(`
		UPDATE workers
		SET lifecycle_state = 'RECOVERING', reachability_condition = 'OFFLINE'
		WHERE id = $1 AND epoch = 7
		RETURNING node_identity
	`, workerID).Scan(&nodeIdentity); err != nil {
		t.Fatalf("prepare Worker readiness identity: %v", err)
	}
	fleetService, err := fleet.NewService(newRolePool(
		t, fixture.database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("create Fleet service: %v", err)
	}
	configureTestCapacityPolicies(t, fleetService, poolID)
	if _, err := fleetService.ObserveCapacity(
		context.Background(),
		capacityObservation(workerID, poolID, 7, 1, 300, true),
	); err != nil {
		t.Fatalf("record readiness capacity: %v", err)
	}
	cycleID := uuid.MustParse("23020000-0000-0000-0000-000000000022")
	deadline := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	if _, err := fleetService.BeginReadiness(context.Background(), fleet.ReadinessRequest{
		CycleID: cycleID, WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 7,
		NodeIdentity: nodeIdentity, ExecutionProfileRevisionID: profileID,
		InferenceBackendRevision: "sglang-h3-v3", RequestedBy: "fleet-controller/primary",
		Deadline: deadline,
	}); err != nil {
		t.Fatalf("begin Worker readiness: %v", err)
	}
	minio := newMinIOFixture(t, "vela-worker-transport-readiness")
	client := newWorkerTransportTestClient(
		t, fixture.database, fixture.service, minio.store, fleetService,
	)
	work, err := client.GetReadinessWork(context.Background(), 7)
	if err != nil || !work.Available || work.CycleID != cycleID ||
		work.Check != workercontrol.ReadinessIdentity || work.WorkerID != workerID ||
		work.WorkerPoolID != poolID || work.WorkerEpoch != 7 ||
		work.NodeIdentity != nodeIdentity ||
		work.ExecutionProfileRevisionID != profileID ||
		work.InferenceBackendRevision != "sglang-h3-v3" || !work.Deadline.Equal(deadline) {
		t.Fatalf("mTLS Worker readiness work = %#v error=%v", work, err)
	}
	digest := sha256.Sum256([]byte("authenticated identity readiness evidence"))
	result, err := client.ReportReadiness(context.Background(), workercontrol.ReadinessEvidence{
		WorkerEpoch: 7, CycleID: cycleID, Check: workercontrol.ReadinessIdentity,
		Passed: true, EvidenceDigest: digest,
	})
	if err != nil || result.State != workercontrol.ReadinessChecking ||
		result.NextCheck != workercontrol.ReadinessDevice {
		t.Fatalf("mTLS Worker readiness result = %#v error=%v", result, err)
	}
	var observedBy string
	if err := fixture.database.Admin.QueryRow(`
		SELECT observed_by
		FROM worker_readiness_evidence
		WHERE cycle_id = $1 AND check_kind = 'IDENTITY'
	`, cycleID).Scan(&observedBy); err != nil {
		t.Fatalf("read authenticated readiness evidence: %v", err)
	}
	if observedBy != "spiffe://vela.internal/worker/h3-primary-fixture" {
		t.Fatalf("readiness evidence actor = %q", observedBy)
	}
}

func TestWorkerControlProductionTransportExecutesAssignmentLifecycleOverMTLS(t *testing.T) {
	fixture := newAssignmentFixture(t, "worker-transport-execution", 7)
	scheduled, err := fixture.service.Acquire(
		context.Background(), fixture.worker, 7, &fixture.candidate,
	)
	if err != nil {
		t.Fatalf("create centrally scheduled Assignment: %v", err)
	}
	minio := newMinIOFixture(t, "vela-worker-transport-execution")
	client := newWorkerTransportTestClient(t, fixture.database, fixture.service, minio.store)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	assignment, err := client.Acquire(ctx, 7)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if assignment.AttemptID != scheduled.AttemptID || assignment.JobID != fixture.candidate.JobID ||
		assignment.WorkerID != fixture.worker.ID ||
		assignment.WorkerEpoch != 7 || assignment.AttemptID == uuid.Nil ||
		assignment.LeaseFence <= 0 || assignment.LeaseToken == "" {
		t.Fatalf("Assignment = %#v", assignment)
	}
	lease := leaseCredentials(assignment)
	started, err := client.Start(ctx, lease)
	if err != nil || started.Decision != workercontrol.StartGranted ||
		started.AttemptID != assignment.AttemptID || started.JobID != assignment.JobID {
		t.Fatalf("Start = %#v error=%v", started, err)
	}
	heartbeat := validHeartbeatObservation(1)
	heartbeatResult, err := client.Heartbeat(ctx, lease, heartbeat)
	if err != nil || heartbeatResult.Decision != workercontrol.HeartbeatContinue ||
		heartbeatResult.AttemptID != assignment.AttemptID ||
		heartbeatResult.HeartbeatSequence != heartbeat.Sequence ||
		heartbeatResult.ExecutionPhase != workercontrol.ExecutionPhaseGenerating {
		t.Fatalf("Heartbeat = %#v error=%v", heartbeatResult, err)
	}
	failure := validFailureObservation()
	decision, err := client.Fail(ctx, lease, failure)
	if err != nil || decision.Disposition != workercontrol.RetryDispositionRetryWait ||
		decision.AttemptID != assignment.AttemptID || decision.JobID != assignment.JobID ||
		decision.FailureClass != failure.FailureClass {
		t.Fatalf("Fail = %#v error=%v", decision, err)
	}
	state := readFailureState(t, fixture.database.Admin, assignment.AttemptID)
	if state.AttemptState != "FAILED" || state.JobState != "RETRY_WAIT" ||
		!state.LeaseRevokedAt.Valid || state.DecisionCount != 1 || state.OutboxCount != 1 {
		t.Fatalf("authoritative execution transport state = %#v", state)
	}
}

func TestWorkerControlProductionTransportResumesAndCompletesArtifactSetOverMTLS(t *testing.T) {
	fixture := newStartFixture(t, "worker-transport-begin-finalization", 7)
	if started, err := fixture.service.Start(
		context.Background(), fixture.worker, fixture.credentials,
	); err != nil || started.Decision != "START_GRANTED" {
		t.Fatalf("Start = %#v error=%v", started, err)
	}

	internal := newRolePool(
		t,
		fixture.database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	resolver, err := workertransport.NewPostgresIdentityResolver(internal)
	if err != nil {
		t.Fatalf("NewPostgresIdentityResolver: %v", err)
	}
	minio := newMinIOFixture(t, "vela-worker-transport")
	minio.enableVersioning(t)
	uploadStore := minio.store
	if err := uploadStore.ValidateBucket(context.Background()); err != nil {
		t.Fatalf("validate Worker transport Artifact Store: %v", err)
	}
	coordinator := visibleCompletionService(t, fixture.database.DSN)
	workerServer, err := workertransport.NewServer(
		resolver,
		coordinator,
		uploadStore,
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
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
		t.Fatalf("parse Worker SPIFFE ID: %v", err)
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
	serverCertificatePath := filepath.Join(directory, "server.crt")
	serverKeyPath := filepath.Join(directory, "server.key")
	clientCAPath := filepath.Join(directory, "client-ca.crt")
	for path, content := range map[string][]byte{
		serverCertificatePath: serverCertificate,
		serverKeyPath:         serverKey,
		clientCAPath:          caPEM,
	} {
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("write Worker transport TLS fixture %s: %v", path, err)
		}
	}
	serverCredentials, err := workertransport.NewServerTLSCredentials(
		serverCertificatePath,
		serverKeyPath,
		clientCAPath,
	)
	if err != nil {
		t.Fatalf("NewServerTLSCredentials: %v", err)
	}
	grpcServer := grpc.NewServer(grpc.Creds(serverCredentials))
	velav1.RegisterWorkerControlServiceServer(grpcServer, workerServer)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Worker control integration server: %v", err)
	}
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		if serveErr := <-serverDone; serveErr != nil && serveErr != grpc.ErrServerStopped {
			t.Errorf("serve Worker control integration server: %v", serveErr)
		}
	})

	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caPEM) {
		t.Fatal("append Worker transport test CA")
	}
	clientPair, err := tls.X509KeyPair(clientCertificate, clientKey)
	if err != nil {
		t.Fatalf("parse Worker transport client certificate: %v", err)
	}
	clientCredentials := credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		ServerName:   "worker-control.internal",
		RootCAs:      rootCAs,
		Certificates: []tls.Certificate{clientPair},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := workertransport.DialClient(ctx, listener.Addr().String(), clientCredentials)
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	defer client.Close()
	plan, err := client.BeginFinalization(ctx, fixture.credentials)
	if err != nil || plan.Decision != workercontrol.FinalizationGranted ||
		plan.AttemptID != fixture.assignment.AttemptID || plan.JobID != fixture.assignment.JobID ||
		len(plan.Artifacts) != 2 {
		t.Fatalf("Worker control FinalizationPlan = %#v error=%v", plan, err)
	}
	artifactIDs := make([]uuid.UUID, 0, len(plan.Artifacts))
	objectVersions := make(map[uuid.UUID]string, len(plan.Artifacts))
	for index, artifact := range plan.Artifacts {
		contentType := "video/mp4"
		partBytes := []byte("transport-video-artifact")
		if artifact.Kind == workercontrol.ArtifactKindThumbnail {
			contentType = "image/webp"
			partBytes = []byte("transport-thumbnail-artifact")
		} else if artifact.Kind != workercontrol.ArtifactKindVideo {
			t.Fatalf("planned Artifact %d = %#v", index, artifact)
		}
		partDigest := sha256.Sum256(partBytes)
		claimID := uuid.New()
		intent := workertransport.ArtifactUploadPartIntent{
			Number: 1, SizeBytes: int64(len(partBytes)), SHA256: partDigest,
		}
		claim, err := client.ClaimArtifactUpload(
			ctx, fixture.credentials, artifact.UploadID, claimID, intent,
		)
		if err != nil || claim.Decision != workercontrol.ArtifactUploadClaimGranted ||
			claim.MultipartUploadID == "" || claim.UploadPart == nil ||
			claim.UploadPart.Number != 1 ||
			claim.UploadPart.RequiredHeaders["X-Amz-Checksum-Sha256"] == "" ||
			len(claim.CompletedParts) != 0 {
			t.Fatalf("first production ArtifactUploadClaim %d = %#v error=%v", index, claim, err)
		}
		put, err := http.NewRequestWithContext(
			ctx,
			http.MethodPut,
			claim.UploadPart.URL,
			bytes.NewReader(partBytes),
		)
		if err != nil {
			t.Fatalf("create presigned Artifact PUT %d: %v", index, err)
		}
		for name, value := range claim.UploadPart.RequiredHeaders {
			if http.CanonicalHeaderKey(name) != "Content-Length" {
				put.Header.Set(name, value)
			}
		}
		put.ContentLength = int64(len(partBytes))
		putResponse, err := http.DefaultClient.Do(put)
		if err != nil {
			t.Fatalf("execute presigned Artifact PUT %d: %v", index, err)
		}
		_ = putResponse.Body.Close()
		if putResponse.StatusCode != http.StatusOK {
			t.Fatalf("presigned Artifact PUT %d status = %s", index, putResponse.Status)
		}

		resumed, err := client.ClaimArtifactUpload(
			ctx, fixture.credentials, artifact.UploadID, claimID, intent,
		)
		if err != nil || resumed.MultipartUploadID != claim.MultipartUploadID ||
			!resumed.PartAlreadyUploaded || resumed.UploadPart != nil || len(resumed.CompletedParts) != 1 {
			t.Fatalf("resumed production ArtifactUploadClaim %d = %#v error=%v", index, resumed, err)
		}
		report := workercontrol.ArtifactUploadReport{
			SizeBytes: int64(len(partBytes)), SHA256: partDigest, ContentType: contentType,
			CompletedParts: []workercontrol.ArtifactUploadPart{{
				Number: 1, ETag: resumed.CompletedParts[0].ETag,
				SizeBytes:      int64(len(partBytes)),
				ChecksumSHA256: base64.StdEncoding.EncodeToString(partDigest[:]),
			}},
		}
		var objectVersionID string
		for replay := 0; replay < 2; replay++ {
			result, err := client.CompleteArtifactMultipartUpload(
				ctx, fixture.credentials, artifact.UploadID, claimID, report,
			)
			if err != nil || result.Decision != workercontrol.ArtifactUploadRecorded ||
				result.ObjectVersionID == "" ||
				(objectVersionID != "" && result.ObjectVersionID != objectVersionID) {
				t.Fatalf("ArtifactUploadResult %d/%d = %#v error=%v", index, replay, result, err)
			}
			objectVersionID = result.ObjectVersionID
		}
		storedObject, err := uploadStore.HeadCurrentVersion(context.Background(), artifact.ObjectKey)
		if err != nil || storedObject.VersionID != objectVersionID ||
			storedObject.SizeBytes != int64(len(partBytes)) {
			t.Fatalf("transport-completed Artifact object %d = %#v error=%v", index, storedObject, err)
		}
		verificationID := uuid.New()
		verified, err := client.VerifyArtifact(
			ctx, fixture.credentials, artifact.UploadID, verificationID,
		)
		if err != nil || verified.Decision != workercontrol.ArtifactVerified ||
			verified.VerificationID != verificationID || verified.UploadID != artifact.UploadID ||
			verified.ArtifactID != artifact.ArtifactID || verified.ObjectVersionID != objectVersionID {
			t.Fatalf("ArtifactVerificationResult %d = %#v error=%v", index, verified, err)
		}
		artifactIDs = append(artifactIDs, artifact.ArtifactID)
		objectVersions[artifact.UploadID] = objectVersionID
	}
	completionID := uuid.New()
	candidate := workercontrol.VisibleCompletionCandidate{
		CompletionID: completionID, ExpectedJobVersion: plan.JobVersion, ArtifactIDs: artifactIDs,
	}
	var firstCompletion workercontrol.VisibleCompletionResult
	for replay := 0; replay < 2; replay++ {
		completion, err := client.CompleteVisibleCompletion(ctx, fixture.credentials, candidate)
		if err != nil || completion.Decision != workercontrol.VisibleCompletionCommitted ||
			completion.CompletionID != completionID || completion.JobID != fixture.assignment.JobID ||
			completion.AttemptID != fixture.assignment.AttemptID ||
			completion.ArtifactSetID == uuid.Nil || completion.ChargeID == uuid.Nil ||
			completion.JobVersion != plan.JobVersion+1 ||
			completion.ManifestSHA256 == [sha256.Size]byte{} ||
			len(completion.Artifacts) != len(plan.Artifacts) {
			t.Fatalf("VisibleCompletionResult %d = %#v error=%v", replay, completion, err)
		}
		if replay > 0 &&
			(completion.ArtifactSetID != firstCompletion.ArtifactSetID ||
				completion.ChargeID != firstCompletion.ChargeID) {
			t.Fatalf("Visible Completion replay changed business identities: %#v / %#v", firstCompletion, completion)
		}
		firstCompletion = completion
	}

	var (
		jobState, attemptState, reservationState string
		leaseRevoked                             bool
		artifactSets, visibleCompletions         int
		charges, accessGrants, terminalEvents    int
	)
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			job.state::text,
			attempt.state::text,
			lease.revoked_at IS NOT NULL,
			reservation.state::text,
			(SELECT count(*) FROM artifact_sets WHERE job_id = job.id),
			(SELECT count(*) FROM visible_completions WHERE job_id = job.id),
			(SELECT count(*) FROM charges WHERE job_id = job.id),
			(SELECT count(*) FROM artifact_access_grants WHERE job_id = job.id),
			(SELECT count(*) FROM outbox_events
			 WHERE aggregate_id = job.id
			   AND event_type IN ('job.succeeded', 'charge.posted', 'invoice.export_requested'))
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.job_id = job.id
		JOIN attempt_leases AS lease ON lease.attempt_id = attempt.id
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		WHERE job.id = $1
	`, fixture.assignment.JobID).Scan(
		&jobState,
		&attemptState,
		&leaseRevoked,
		&reservationState,
		&artifactSets,
		&visibleCompletions,
		&charges,
		&accessGrants,
		&terminalEvents,
	); err != nil {
		t.Fatalf("read transport-committed Finalization: %v", err)
	}
	if jobState != "SUCCEEDED" || attemptState != "SUCCEEDED" || !leaseRevoked ||
		reservationState != "CONSUMED" || artifactSets != 1 || visibleCompletions != 1 ||
		charges != 1 || accessGrants != 1 || terminalEvents != 3 {
		t.Fatalf(
			"transport Visible Completion = Job %s Attempt %s Lease revoked=%t reservation=%s sets=%d completions=%d charges=%d access=%d events=%d",
			jobState,
			attemptState,
			leaseRevoked,
			reservationState,
			artifactSets,
			visibleCompletions,
			charges,
			accessGrants,
			terminalEvents,
		)
	}
	firstArtifact := plan.Artifacts[0]
	var uploadState, storedObjectVersionID, completedPartChecksum string
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			state::text,
			object_version_id,
			completed_parts -> 0 ->> 'checksum_sha256'
		FROM artifact_uploads
		WHERE id = $1
	`, firstArtifact.UploadID).Scan(
		&uploadState,
		&storedObjectVersionID,
		&completedPartChecksum,
	); err != nil {
		t.Fatalf("read transport-committed Artifact upload: %v", err)
	}
	firstDigest := sha256.Sum256([]byte("transport-video-artifact"))
	if uploadState != "VERIFIED" || storedObjectVersionID != objectVersions[firstArtifact.UploadID] ||
		completedPartChecksum != base64.StdEncoding.EncodeToString(firstDigest[:]) {
		t.Fatalf(
			"transport-committed Artifact upload = state %s version %s checksum %s",
			uploadState,
			storedObjectVersionID,
			completedPartChecksum,
		)
	}
}

func newWorkerTransportTestClient(
	t *testing.T,
	database testDatabase,
	coordinator workertransport.WorkerCoordinator,
	uploadStore workertransport.ArtifactUploadStore,
	fleetServices ...workertransport.WorkerFleetService,
) *workertransport.Client {
	t.Helper()
	return newWorkerTransportTestClientForSPIFFE(
		t,
		database,
		coordinator,
		uploadStore,
		"spiffe://vela.internal/worker/h3-primary-fixture",
		fleetServices...,
	)
}

func newWorkerTransportTestClientForSPIFFE(
	t *testing.T,
	database testDatabase,
	coordinator workertransport.WorkerCoordinator,
	uploadStore workertransport.ArtifactUploadStore,
	workerSPIFFE string,
	fleetServices ...workertransport.WorkerFleetService,
) *workertransport.Client {
	t.Helper()
	internal := newRolePool(
		t,
		database.DSN,
		"vela_internal_login",
		"vela-internal-password",
	)
	resolver, err := workertransport.NewPostgresIdentityResolver(internal)
	if err != nil {
		t.Fatalf("NewPostgresIdentityResolver: %v", err)
	}
	workerServer, err := workertransport.NewServer(
		resolver, coordinator, uploadStore, fleetServices...,
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
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
	workerSPIFFEID, err := url.Parse(workerSPIFFE)
	if err != nil {
		t.Fatalf("parse Worker SPIFFE ID: %v", err)
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
	serverCertificatePath := filepath.Join(directory, "server.crt")
	serverKeyPath := filepath.Join(directory, "server.key")
	clientCAPath := filepath.Join(directory, "client-ca.crt")
	for path, content := range map[string][]byte{
		serverCertificatePath: serverCertificate,
		serverKeyPath:         serverKey,
		clientCAPath:          caPEM,
	} {
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("write Worker transport TLS fixture %s: %v", path, err)
		}
	}
	serverCredentials, err := workertransport.NewServerTLSCredentials(
		serverCertificatePath,
		serverKeyPath,
		clientCAPath,
	)
	if err != nil {
		t.Fatalf("NewServerTLSCredentials: %v", err)
	}
	grpcServer := grpc.NewServer(grpc.Creds(serverCredentials))
	velav1.RegisterWorkerControlServiceServer(grpcServer, workerServer)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for Worker control integration server: %v", err)
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		if serveErr := <-serverDone; serveErr != nil && serveErr != grpc.ErrServerStopped {
			t.Errorf("serve Worker control integration server: %v", serveErr)
		}
	})
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caPEM) {
		t.Fatal("append Worker transport test CA")
	}
	clientPair, err := tls.X509KeyPair(clientCertificate, clientKey)
	if err != nil {
		t.Fatalf("parse Worker transport client certificate: %v", err)
	}
	clientCredentials := credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, ServerName: "worker-control.internal",
		RootCAs: rootCAs, Certificates: []tls.Certificate{clientPair},
	})
	clientContext, cancelClient := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancelClient)
	client, err := workertransport.DialClient(
		clientContext,
		listener.Addr().String(),
		clientCredentials,
	)
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func issueWorkerTransportTestCA(t *testing.T) (*x509.Certificate, *rsa.PrivateKey, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate Worker transport test CA key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Vela Worker Transport Test CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("issue Worker transport test CA: %v", err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse Worker transport test CA: %v", err)
	}
	return certificate, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func issueWorkerTransportTestCertificate(
	t *testing.T,
	caCertificate *x509.Certificate,
	caKey *rsa.PrivateKey,
	subject pkix.Name,
	dnsNames []string,
	identities []*url.URL,
	usage []x509.ExtKeyUsage,
) ([]byte, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate Worker transport test certificate key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatalf("generate Worker transport test certificate serial: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      subject,
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  usage,
		DNSNames:     dnsNames,
		URIs:         identities,
	}
	der, err := x509.CreateCertificate(
		rand.Reader,
		template,
		caCertificate,
		&key.PublicKey,
		caKey,
	)
	if err != nil {
		t.Fatalf("issue Worker transport test certificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}
