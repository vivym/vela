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
	if worker.ID != uuid.MustParse(testWorkerID) {
		t.Fatalf("resolved Worker = %s, want %s", worker.ID, testWorkerID)
	}
	if _, err := resolver.ResolveWorker(
		context.Background(),
		"spiffe://vela.internal/worker/not-registered",
	); err == nil {
		t.Fatal("resolved an unregistered Worker SPIFFE ID")
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
	connection, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(clientCredentials),
	)
	if err != nil {
		t.Fatalf("create Worker control client: %v", err)
	}
	defer connection.Close()
	stream, err := velav1.NewWorkerControlServiceClient(connection).Connect(ctx)
	if err != nil {
		t.Fatalf("connect Worker control stream: %v", err)
	}
	lease := &velav1.WorkerLeaseCredentials{
		AttemptId:   fixture.credentials.AttemptID.String(),
		WorkerEpoch: fixture.credentials.WorkerEpoch,
		Fence:       fixture.credentials.Fence,
		Token:       fixture.credentials.Token,
	}
	requestID := uuid.NewString()
	if err := stream.Send(&velav1.ConnectRequest{
		RequestId: requestID,
		Operation: &velav1.ConnectRequest_BeginFinalization{
			BeginFinalization: &velav1.BeginFinalizationRequest{Lease: lease},
		},
	}); err != nil {
		t.Fatalf("send BeginFinalization: %v", err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive FinalizationPlan: %v", err)
	}
	plan := response.GetFinalizationPlan()
	if response.GetRequestId() != requestID || plan == nil ||
		plan.GetDecision() != "FINALIZATION_GRANTED" ||
		plan.GetAttemptId() != fixture.assignment.AttemptID.String() ||
		plan.GetJobId() != fixture.assignment.JobID.String() ||
		len(plan.GetArtifacts()) != 2 {
		t.Fatalf("Worker control FinalizationPlan = %#v", response)
	}
	exchange := func(request *velav1.ConnectRequest) *velav1.ConnectResponse {
		t.Helper()
		request.RequestId = uuid.NewString()
		if err := stream.Send(request); err != nil {
			t.Fatalf("send Worker control request %s: %v", request.GetRequestId(), err)
		}
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("receive Worker control response %s: %v", request.GetRequestId(), err)
		}
		if response.GetRequestId() != request.GetRequestId() || response.GetOperationError() != nil {
			t.Fatalf("Worker control response for %s = %#v", request.GetRequestId(), response)
		}
		return response
	}
	artifactIDs := make([]string, 0, len(plan.GetArtifacts()))
	objectVersions := make(map[string]string, len(plan.GetArtifacts()))
	for index, artifact := range plan.GetArtifacts() {
		contentType := "video/mp4"
		partBytes := []byte("transport-video-artifact")
		if artifact.GetKind() == "THUMBNAIL" {
			contentType = "image/webp"
			partBytes = []byte("transport-thumbnail-artifact")
		} else if artifact.GetKind() != "VIDEO" {
			t.Fatalf("planned Artifact %d = %#v", index, artifact)
		}
		partDigest := sha256.Sum256(partBytes)
		claimID := uuid.NewString()
		claimRequest := func(claimID string) *velav1.ConnectRequest {
			return &velav1.ConnectRequest{
				Operation: &velav1.ConnectRequest_ClaimArtifactUpload{
					ClaimArtifactUpload: &velav1.ClaimArtifactUploadRequest{
						Lease: lease, UploadId: artifact.GetUploadId(), ClaimId: claimID,
						Part: &velav1.ArtifactUploadPartIntent{
							Number: 1, SizeBytes: int64(len(partBytes)), Sha256: partDigest[:],
						},
					},
				},
			}
		}
		claimResponse := exchange(claimRequest(claimID))
		claim := claimResponse.GetArtifactUploadClaim()
		if claim == nil || claim.GetDecision() != "UPLOAD_CLAIM_GRANTED" ||
			claim.GetMultipartUploadId() == "" || claim.GetUploadPart() == nil ||
			claim.GetUploadPart().GetNumber() != 1 ||
			claim.GetUploadPart().GetRequiredHeaders()["X-Amz-Checksum-Sha256"] == "" ||
			len(claim.GetCompletedParts()) != 0 {
			t.Fatalf("first production ArtifactUploadClaim %d = %#v", index, claimResponse)
		}
		put, err := http.NewRequestWithContext(
			ctx,
			http.MethodPut,
			claim.GetUploadPart().GetUrl(),
			bytes.NewReader(partBytes),
		)
		if err != nil {
			t.Fatalf("create presigned Artifact PUT %d: %v", index, err)
		}
		for name, value := range claim.GetUploadPart().GetRequiredHeaders() {
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

		resumedResponse := exchange(claimRequest(uuid.NewString()))
		resumed := resumedResponse.GetArtifactUploadClaim()
		if resumed == nil || resumed.GetMultipartUploadId() != claim.GetMultipartUploadId() ||
			!resumed.GetPartAlreadyUploaded() || resumed.GetUploadPart() != nil ||
			len(resumed.GetCompletedParts()) != 1 {
			t.Fatalf("resumed production ArtifactUploadClaim %d = %#v", index, resumedResponse)
		}
		completeRequest := func() *velav1.ConnectRequest {
			return &velav1.ConnectRequest{
				Operation: &velav1.ConnectRequest_CompleteArtifactMultipartUpload{
					CompleteArtifactMultipartUpload: &velav1.CompleteArtifactMultipartUploadRequest{
						Lease: lease, UploadId: artifact.GetUploadId(), ClaimId: claimID,
						SizeBytes: int64(len(partBytes)), Sha256: partDigest[:], ContentType: contentType,
						CompletedParts: []*velav1.ArtifactUploadPartReport{{
							Number: 1, Etag: resumed.GetCompletedParts()[0].GetEtag(),
							SizeBytes: int64(len(partBytes)), ChecksumSha256: partDigest[:],
						}},
					},
				},
			}
		}
		var objectVersionID string
		for replay := 0; replay < 2; replay++ {
			completeResponse := exchange(completeRequest())
			result := completeResponse.GetArtifactUploadResult()
			if result == nil || result.GetDecision() != "ARTIFACT_UPLOAD_RECORDED" ||
				result.GetObjectVersionId() == "" ||
				(objectVersionID != "" && result.GetObjectVersionId() != objectVersionID) {
				t.Fatalf("ArtifactUploadResult %d/%d = %#v", index, replay, completeResponse)
			}
			objectVersionID = result.GetObjectVersionId()
		}
		storedObject, err := uploadStore.HeadCurrentVersion(context.Background(), artifact.GetObjectKey())
		if err != nil || storedObject.VersionID != objectVersionID ||
			storedObject.SizeBytes != int64(len(partBytes)) {
			t.Fatalf("transport-completed Artifact object %d = %#v error=%v", index, storedObject, err)
		}
		verificationID := uuid.NewString()
		verificationResponse := exchange(&velav1.ConnectRequest{
			Operation: &velav1.ConnectRequest_VerifyArtifact{
				VerifyArtifact: &velav1.VerifyArtifactRequest{
					Lease: lease, UploadId: artifact.GetUploadId(), VerificationId: verificationID,
				},
			},
		})
		verified := verificationResponse.GetArtifactVerificationResult()
		if verified == nil || verified.GetDecision() != "ARTIFACT_VERIFIED" ||
			verified.GetVerificationId() != verificationID ||
			verified.GetUploadId() != artifact.GetUploadId() ||
			verified.GetArtifactId() != artifact.GetArtifactId() ||
			verified.GetObjectVersionId() != objectVersionID {
			t.Fatalf("ArtifactVerificationResult %d = %#v", index, verificationResponse)
		}
		artifactIDs = append(artifactIDs, artifact.GetArtifactId())
		objectVersions[artifact.GetUploadId()] = objectVersionID
	}
	completionID := uuid.NewString()
	completionRequest := func() *velav1.ConnectRequest {
		return &velav1.ConnectRequest{
			Operation: &velav1.ConnectRequest_CompleteVisibleCompletion{
				CompleteVisibleCompletion: &velav1.CompleteVisibleCompletionRequest{
					Lease: lease,
					Candidate: &velav1.VisibleCompletionCandidate{
						CompletionId: completionID, ExpectedJobVersion: plan.GetJobVersion(),
						ArtifactIds: artifactIDs,
					},
				},
			},
		}
	}
	var firstCompletion *velav1.VisibleCompletionResult
	for replay := 0; replay < 2; replay++ {
		completionResponse := exchange(completionRequest())
		completion := completionResponse.GetVisibleCompletionResult()
		if completion == nil ||
			(replay == 0 && completion.GetDecision() != "VISIBLE_COMPLETION_COMMITTED") ||
			(replay == 1 && completion.GetDecision() != "VISIBLE_COMPLETION_COMMITTED") ||
			completion.GetCompletionId() != completionID ||
			completion.GetJobId() != fixture.assignment.JobID.String() ||
			completion.GetAttemptId() != fixture.assignment.AttemptID.String() ||
			completion.GetArtifactSetId() == "" || completion.GetChargeId() == "" ||
			completion.GetJobVersion() != plan.GetJobVersion()+1 ||
			len(completion.GetManifestSha256()) != sha256.Size ||
			len(completion.GetArtifacts()) != len(plan.GetArtifacts()) {
			t.Fatalf("VisibleCompletionResult %d = %#v", replay, completionResponse)
		}
		if firstCompletion != nil &&
			(completion.GetArtifactSetId() != firstCompletion.GetArtifactSetId() ||
				completion.GetChargeId() != firstCompletion.GetChargeId()) {
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
	firstArtifact := plan.GetArtifacts()[0]
	var uploadState, storedObjectVersionID, completedPartChecksum string
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			state::text,
			object_version_id,
			completed_parts -> 0 ->> 'checksum_sha256'
		FROM artifact_uploads
		WHERE id = $1
	`, firstArtifact.GetUploadId()).Scan(
		&uploadState,
		&storedObjectVersionID,
		&completedPartChecksum,
	); err != nil {
		t.Fatalf("read transport-committed Artifact upload: %v", err)
	}
	firstDigest := sha256.Sum256([]byte("transport-video-artifact"))
	if uploadState != "VERIFIED" || storedObjectVersionID != objectVersions[firstArtifact.GetUploadId()] ||
		completedPartChecksum != base64.StdEncoding.EncodeToString(firstDigest[:]) {
		t.Fatalf(
			"transport-committed Artifact upload = state %s version %s checksum %s",
			uploadState,
			storedObjectVersionID,
			completedPartChecksum,
		)
	}
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
