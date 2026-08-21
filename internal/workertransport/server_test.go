package workertransport

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

func TestConnectDispatchesFinalizationOperationsUnderMTLSWorkerIdentity(t *testing.T) {
	const spiffeID = "spiffe://vela.internal/worker/h3-primary-01"
	workerID := uuid.MustParse("00000000-0000-0000-0000-000000000020")
	attemptID := uuid.New()
	uploadID := uuid.New()
	claimID := uuid.New()
	verificationID := uuid.New()
	completionID := uuid.New()
	artifactID := uuid.New()
	lease := &velav1.WorkerLeaseCredentials{
		AttemptId: attemptID.String(), WorkerEpoch: 7, Fence: 3, Token: "opaque-lease-token",
	}
	partDigest := sha256.Sum256([]byte("upload-part-two"))
	objectDigest := sha256.Sum256([]byte("complete-object"))
	requests := []*velav1.ConnectRequest{
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_BeginFinalization{
				BeginFinalization: &velav1.BeginFinalizationRequest{Lease: lease},
			},
		},
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_ClaimArtifactUpload{
				ClaimArtifactUpload: &velav1.ClaimArtifactUploadRequest{
					Lease: lease, UploadId: uploadID.String(), ClaimId: claimID.String(),
					Part: &velav1.ArtifactUploadPartIntent{
						Number: 2, SizeBytes: 5, Sha256: partDigest[:],
					},
				},
			},
		},
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_RecordArtifactMultipartSession{
				RecordArtifactMultipartSession: &velav1.RecordArtifactMultipartSessionRequest{
					Lease: lease, UploadId: uploadID.String(), ClaimId: claimID.String(),
					MultipartUploadId: "s3-multipart-session",
				},
			},
		},
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_RecordArtifactUploaded{
				RecordArtifactUploaded: &velav1.RecordArtifactUploadedRequest{
					Lease:    lease,
					UploadId: uploadID.String(),
					Report: &velav1.ArtifactUploadReport{
						ObjectVersionId: "version-1",
						SizeBytes:       12,
						Sha256:          make([]byte, sha256.Size),
						ContentType:     "video/mp4",
						CompletedParts: []*velav1.ArtifactUploadPartReport{{
							Number: 1, Etag: "etag-1", SizeBytes: 12,
						}},
					},
				},
			},
		},
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_CompleteArtifactMultipartUpload{
				CompleteArtifactMultipartUpload: &velav1.CompleteArtifactMultipartUploadRequest{
					Lease: lease, UploadId: uploadID.String(), ClaimId: claimID.String(),
					SizeBytes: 15, Sha256: objectDigest[:], ContentType: "video/mp4",
					CompletedParts: []*velav1.ArtifactUploadPartReport{
						{
							Number: 1, Etag: "etag-1", SizeBytes: 10,
							ChecksumSha256: sha256Bytes("completed-part-one"),
						},
						{
							Number: 2, Etag: "etag-2", SizeBytes: 5,
							ChecksumSha256: partDigest[:],
						},
					},
				},
			},
		},
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_VerifyArtifact{
				VerifyArtifact: &velav1.VerifyArtifactRequest{
					Lease: lease, UploadId: uploadID.String(), VerificationId: verificationID.String(),
				},
			},
		},
		{
			RequestId: uuid.NewString(),
			Operation: &velav1.ConnectRequest_CompleteVisibleCompletion{
				CompleteVisibleCompletion: &velav1.CompleteVisibleCompletionRequest{
					Lease: lease,
					Candidate: &velav1.VisibleCompletionCandidate{
						CompletionId: completionID.String(), ExpectedJobVersion: 4,
						ArtifactIds: []string{artifactID.String()},
					},
				},
			},
		},
	}
	resolver := &recordingIdentityResolver{worker: workercontrol.AuthenticatedWorker{ID: workerID}}
	coordinator := &recordingFinalizationCoordinator{
		attemptID: attemptID, uploadID: uploadID, claimID: claimID,
		verificationID: verificationID, completionID: completionID, artifactID: artifactID,
	}
	uploadStore := &recordingArtifactUploadStore{}
	server, err := NewServer(resolver, coordinator, uploadStore)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	stream := &recordingConnectStream{ctx: mtlsPeerContext(t, spiffeID), requests: requests}
	if err := server.Connect(stream); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if resolver.spiffeID != spiffeID {
		t.Fatalf("resolved SPIFFE ID = %q", resolver.spiffeID)
	}
	if len(coordinator.workers) != len(requests)+3 {
		t.Fatalf("coordinator calls = %d, want %d", len(coordinator.workers), len(requests)+3)
	}
	for _, worker := range coordinator.workers {
		if worker.ID != workerID {
			t.Fatalf("coordinator Worker = %s, want mTLS identity %s", worker.ID, workerID)
		}
	}
	if len(stream.responses) != len(requests) {
		t.Fatalf("responses = %d, want %d", len(stream.responses), len(requests))
	}
	for index, response := range stream.responses {
		if response.RequestId != requests[index].RequestId || response.GetOperationError() != nil {
			t.Fatalf("response %d = %#v", index, response)
		}
	}
	if stream.responses[0].GetFinalizationPlan() == nil ||
		stream.responses[1].GetArtifactUploadClaim() == nil ||
		stream.responses[2].GetArtifactMultipartSession() == nil ||
		stream.responses[3].GetArtifactUploadResult() == nil ||
		stream.responses[4].GetArtifactUploadResult() == nil ||
		stream.responses[5].GetArtifactVerificationResult() == nil ||
		stream.responses[6].GetVisibleCompletionResult() == nil {
		t.Fatalf("typed finalization responses = %#v", stream.responses)
	}
	claimResponse := stream.responses[1].GetArtifactUploadClaim()
	if claimResponse.GetMultipartUploadId() != "s3-multipart-session" ||
		len(claimResponse.GetCompletedParts()) != 1 || claimResponse.GetUploadPart() == nil ||
		claimResponse.GetUploadPart().GetNumber() != 2 ||
		claimResponse.GetUploadPart().GetRequiredHeaders()["X-Amz-Checksum-Sha256"] == "" ||
		len(uploadStore.created) != 1 || len(uploadStore.presigned) != 1 ||
		len(uploadStore.completed) != 1 {
		t.Fatalf("production Artifact upload flow = claim %#v store %#v", claimResponse, uploadStore)
	}
}

func TestArtifactUploadClaimAbortsOrphanAndResumesWinningMultipartSession(t *testing.T) {
	uploadID := uuid.New()
	artifactID := uuid.New()
	coordinator := &recordingFinalizationCoordinator{
		multipartDecision: workercontrol.ArtifactMultipartSessionConflict,
		inspection: &workercontrol.ArtifactUploadStatus{
			Decision: workercontrol.ArtifactUploadStatusFound,
			UploadID: uploadID, ArtifactID: artifactID, State: workercontrol.ArtifactUploadStateUploading,
			ObjectKey:           "artifacts/org/project/job/attempt/artifact/video.mp4",
			ExpectedContentType: "video/mp4", MultipartUploadID: "winning-session",
			UploadExpiresAt: time.Now().Add(10 * time.Minute), Version: 4,
		},
	}
	uploadStore := &recordingArtifactUploadStore{createdUploadID: "orphan-session"}
	server := &Server{coordinator: coordinator, uploadStore: uploadStore}
	claim, err := server.prepareArtifactUploadClaim(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: uuid.New()},
		workercontrol.LeaseCredentials{
			AttemptID: uuid.New(), WorkerEpoch: 2, Fence: 3, Token: "lease-token",
		},
		uuid.New(),
		workercontrol.ArtifactUploadClaim{
			Decision: workercontrol.ArtifactUploadClaimGranted,
			ClaimID:  uuid.New(), UploadID: uploadID, ArtifactID: artifactID,
			ObjectKey:           "artifacts/org/project/job/attempt/artifact/video.mp4",
			ExpectedContentType: "video/mp4",
			UploadExpiresAt:     time.Now().Add(10 * time.Minute), Version: 2,
		},
		nil,
	)
	if err != nil {
		t.Fatalf("prepare Artifact upload claim: %v", err)
	}
	if claim.GetDecision() != string(workercontrol.ArtifactUploadClaimGranted) ||
		claim.GetMultipartUploadId() != "winning-session" || claim.GetVersion() != 4 ||
		len(uploadStore.aborted) != 1 || uploadStore.aborted[0].UploadID != "orphan-session" ||
		len(uploadStore.created) != 1 {
		t.Fatalf("resumed claim = %#v store = %#v", claim, uploadStore)
	}
}

func TestArtifactMultipartCompletionRecoversVersionAfterLostResponse(t *testing.T) {
	uploadID := uuid.New()
	artifactID := uuid.New()
	partDigest := sha256.Sum256([]byte("completed-part"))
	parts := []artifactstore.CompletedPart{{
		Number: 1, ETag: "etag-1", SizeBytes: 15,
		ChecksumSHA256: base64.StdEncoding.EncodeToString(partDigest[:]),
	}}
	coordinator := &recordingFinalizationCoordinator{
		uploadID:   uploadID,
		artifactID: artifactID,
		inspection: &workercontrol.ArtifactUploadStatus{
			Decision: workercontrol.ArtifactUploadStatusFound,
			UploadID: uploadID, ArtifactID: artifactID, State: workercontrol.ArtifactUploadStateUploading,
			ObjectKey:           "artifacts/org/project/job/attempt/artifact/video.mp4",
			ExpectedContentType: "video/mp4", MultipartUploadID: "completed-session",
		},
	}
	uploadStore := &recordingArtifactUploadStore{
		listedParts: [][]artifactstore.CompletedPart{parts},
		completeErr: errors.New("connection closed after publication"),
		headVersion: artifactstore.ObjectVersion{
			ObjectKey: "artifacts/org/project/job/attempt/artifact/video.mp4",
			VersionID: "recovered-version", SizeBytes: 15, ContentType: "video/mp4",
			ChecksumSHA256: mustMultipartChecksum(t, parts),
		},
	}
	uploadStore.beforeComplete = func() {
		if len(coordinator.completionIntents) != 1 {
			t.Fatalf("multipart completion ran before its durable intent: %#v", coordinator)
		}
	}
	server := &Server{coordinator: coordinator, uploadStore: uploadStore}
	objectDigest := sha256.Sum256([]byte("completed-object"))
	result, err := server.completeArtifactMultipartUpload(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: uuid.New()},
		workercontrol.LeaseCredentials{
			AttemptID: uuid.New(), WorkerEpoch: 2, Fence: 3, Token: "lease-token",
		},
		uploadID,
		workercontrol.ArtifactUploadReport{
			SizeBytes: 15, SHA256: objectDigest, ContentType: "video/mp4",
			CompletedParts: []workercontrol.ArtifactUploadPart{{
				Number: 1, ETag: "etag-1", SizeBytes: 15,
			}},
		},
		parts,
	)
	if err != nil {
		t.Fatalf("complete Artifact multipart upload: %v", err)
	}
	if result.Decision != workercontrol.ArtifactUploadRecorded ||
		result.ObjectVersionID != "recovered-version" || uploadStore.headCalls != 1 ||
		len(uploadStore.completed) != 1 || len(coordinator.uploadReports) != 1 ||
		coordinator.uploadReports[0].ObjectVersionID != "recovered-version" {
		t.Fatalf("recovered result = %#v store = %#v coordinator = %#v", result, uploadStore, coordinator)
	}
}

func TestArtifactMultipartCompletionReplaysPersistedReceiptWithoutS3Mutation(t *testing.T) {
	uploadID := uuid.New()
	coordinator := &recordingFinalizationCoordinator{
		inspection: &workercontrol.ArtifactUploadStatus{
			Decision: workercontrol.ArtifactUploadStatusFound,
			UploadID: uploadID, ArtifactID: uuid.New(), State: workercontrol.ArtifactUploadStateVerified,
			ObjectKey:           "artifacts/org/project/job/attempt/artifact/video.mp4",
			ExpectedContentType: "video/mp4", ObjectVersionID: "persisted-version",
		},
	}
	uploadStore := &recordingArtifactUploadStore{}
	server := &Server{coordinator: coordinator, uploadStore: uploadStore}
	digest := sha256.Sum256([]byte("object"))
	result, err := server.completeArtifactMultipartUpload(
		context.Background(),
		workercontrol.AuthenticatedWorker{ID: uuid.New()},
		workercontrol.LeaseCredentials{
			AttemptID: uuid.New(), WorkerEpoch: 2, Fence: 3, Token: "lease-token",
		},
		uploadID,
		workercontrol.ArtifactUploadReport{
			SizeBytes: 15, SHA256: digest, ContentType: "video/mp4",
			CompletedParts: []workercontrol.ArtifactUploadPart{{
				Number: 1, ETag: "etag-1", SizeBytes: 15,
			}},
		},
		[]artifactstore.CompletedPart{{
			Number: 1, ETag: "etag-1", SizeBytes: 15, ChecksumSHA256: "unused",
		}},
	)
	if err != nil {
		t.Fatalf("replay Artifact multipart completion: %v", err)
	}
	if result.ObjectVersionID != "persisted-version" || uploadStore.listed != 0 ||
		len(uploadStore.completed) != 0 || uploadStore.headCalls != 0 {
		t.Fatalf("replayed result = %#v store = %#v", result, uploadStore)
	}
}

type recordingIdentityResolver struct {
	spiffeID string
	worker   workercontrol.AuthenticatedWorker
}

func (resolver *recordingIdentityResolver) ResolveWorker(
	_ context.Context,
	spiffeID string,
) (workercontrol.AuthenticatedWorker, error) {
	resolver.spiffeID = spiffeID
	return resolver.worker, nil
}

type recordingFinalizationCoordinator struct {
	attemptID, uploadID, claimID, verificationID, completionID, artifactID uuid.UUID
	workers                                                                []workercontrol.AuthenticatedWorker
	multipartDecision                                                      workercontrol.ArtifactMultipartSessionDecision
	inspection                                                             *workercontrol.ArtifactUploadStatus
	completionIntents                                                      []workercontrol.ArtifactUploadReport
	uploadReports                                                          []workercontrol.ArtifactUploadReport
}

func (coordinator *recordingFinalizationCoordinator) record(
	worker workercontrol.AuthenticatedWorker,
) {
	coordinator.workers = append(coordinator.workers, worker)
}

func (coordinator *recordingFinalizationCoordinator) BeginFinalization(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
) (workercontrol.FinalizationPlan, error) {
	coordinator.record(worker)
	return workercontrol.FinalizationPlan{
		Decision: workercontrol.FinalizationGranted, AttemptID: coordinator.attemptID,
	}, nil
}

func (coordinator *recordingFinalizationCoordinator) ClaimArtifactUpload(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_, _ uuid.UUID,
) (workercontrol.ArtifactUploadClaim, error) {
	coordinator.record(worker)
	return workercontrol.ArtifactUploadClaim{
		Decision: workercontrol.ArtifactUploadClaimGranted, ClaimID: coordinator.claimID,
		UploadID: coordinator.uploadID, ArtifactID: coordinator.artifactID,
		ObjectKey:           "artifacts/org/project/job/attempt/artifact/video.mp4",
		ExpectedContentType: "video/mp4", UploadExpiresAt: time.Now().Add(10 * time.Minute),
	}, nil
}

func (coordinator *recordingFinalizationCoordinator) RecordArtifactMultipartSession(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_, _ uuid.UUID,
	multipartUploadID string,
) (workercontrol.ArtifactMultipartSession, error) {
	coordinator.record(worker)
	decision := coordinator.multipartDecision
	if decision == "" {
		decision = workercontrol.ArtifactMultipartSessionRecorded
	}
	return workercontrol.ArtifactMultipartSession{
		Decision: decision, UploadID: coordinator.uploadID,
		ArtifactID: coordinator.artifactID, MultipartUploadID: multipartUploadID,
	}, nil
}

func (coordinator *recordingFinalizationCoordinator) InspectArtifactUpload(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_ uuid.UUID,
) (workercontrol.ArtifactUploadStatus, error) {
	coordinator.record(worker)
	if coordinator.inspection != nil {
		return *coordinator.inspection, nil
	}
	return workercontrol.ArtifactUploadStatus{
		Decision: workercontrol.ArtifactUploadStatusFound,
		UploadID: coordinator.uploadID, ArtifactID: coordinator.artifactID,
		State:               workercontrol.ArtifactUploadStateUploading,
		ObjectKey:           "artifacts/org/project/job/attempt/artifact/video.mp4",
		ExpectedContentType: "video/mp4", MultipartUploadID: "s3-multipart-session",
		UploadExpiresAt: time.Now().Add(10 * time.Minute),
	}, nil
}

func (coordinator *recordingFinalizationCoordinator) RecordArtifactCompletionIntent(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_ uuid.UUID,
	report workercontrol.ArtifactUploadReport,
) (workercontrol.ArtifactCompletionIntentResult, error) {
	coordinator.record(worker)
	coordinator.completionIntents = append(coordinator.completionIntents, report)
	return workercontrol.ArtifactCompletionIntentResult{
		Decision:   workercontrol.ArtifactCompletionIntentRecorded,
		UploadID:   coordinator.uploadID,
		ArtifactID: coordinator.artifactID,
	}, nil
}

func (coordinator *recordingFinalizationCoordinator) RecordArtifactUploaded(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_ uuid.UUID,
	report workercontrol.ArtifactUploadReport,
) (workercontrol.ArtifactUploadResult, error) {
	coordinator.record(worker)
	coordinator.uploadReports = append(coordinator.uploadReports, report)
	return workercontrol.ArtifactUploadResult{
		Decision: workercontrol.ArtifactUploadRecorded, UploadID: coordinator.uploadID,
		ObjectVersionID: report.ObjectVersionID,
	}, nil
}

func (coordinator *recordingFinalizationCoordinator) VerifyArtifact(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_, _ uuid.UUID,
) (workercontrol.ArtifactVerificationResult, error) {
	coordinator.record(worker)
	return workercontrol.ArtifactVerificationResult{
		Decision: workercontrol.ArtifactVerified, VerificationID: coordinator.verificationID,
	}, nil
}

func (coordinator *recordingFinalizationCoordinator) CompleteVisibleCompletion(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_ workercontrol.VisibleCompletionCandidate,
) (workercontrol.VisibleCompletionResult, error) {
	coordinator.record(worker)
	return workercontrol.VisibleCompletionResult{
		Decision: workercontrol.VisibleCompletionCommitted, CompletionID: coordinator.completionID,
	}, nil
}

type recordingArtifactUploadStore struct {
	createdUploadID string
	created         []artifactstore.MultipartUpload
	listed          int
	listedParts     [][]artifactstore.CompletedPart
	presigned       []artifactstore.MultipartUpload
	completed       []artifactstore.MultipartUpload
	completeErr     error
	headVersion     artifactstore.ObjectVersion
	headCalls       int
	beforeComplete  func()
	aborted         []artifactstore.MultipartUpload
}

func (store *recordingArtifactUploadStore) CreateMultipartUpload(
	_ context.Context,
	objectKey string,
	contentType string,
) (artifactstore.MultipartUpload, error) {
	uploadID := store.createdUploadID
	if uploadID == "" {
		uploadID = "s3-multipart-session"
	}
	upload := artifactstore.MultipartUpload{
		ObjectKey: objectKey, UploadID: uploadID, ContentType: contentType,
	}
	store.created = append(store.created, upload)
	return upload, nil
}

func (store *recordingArtifactUploadStore) ListParts(
	_ context.Context,
	upload artifactstore.MultipartUpload,
) ([]artifactstore.CompletedPart, error) {
	store.listed++
	if len(store.listedParts) != 0 {
		index := store.listed - 1
		if index >= len(store.listedParts) {
			index = len(store.listedParts) - 1
		}
		return append([]artifactstore.CompletedPart(nil), store.listedParts[index]...), nil
	}
	firstDigest := sha256.Sum256([]byte("completed-part-one"))
	parts := []artifactstore.CompletedPart{{
		Number: 1, ETag: "etag-1", SizeBytes: 10,
		ChecksumSHA256: base64.StdEncoding.EncodeToString(firstDigest[:]),
	}}
	if store.listed > 1 {
		secondDigest := sha256.Sum256([]byte("upload-part-two"))
		parts = append(parts, artifactstore.CompletedPart{
			Number: 2, ETag: "etag-2", SizeBytes: 5,
			ChecksumSHA256: base64.StdEncoding.EncodeToString(secondDigest[:]),
		})
	}
	return parts, nil
}

func (store *recordingArtifactUploadStore) PresignUploadPart(
	_ context.Context,
	upload artifactstore.MultipartUpload,
	_ int32,
	sizeBytes int64,
	digest [sha256.Size]byte,
	expiresAt time.Time,
) (artifactstore.SignedUploadPart, error) {
	store.presigned = append(store.presigned, upload)
	checksum := base64.StdEncoding.EncodeToString(digest[:])
	return artifactstore.SignedUploadPart{
		URL: "https://s3.invalid/signed-part", Method: http.MethodPut,
		Headers: http.Header{
			"Content-Length":        []string{strconv.FormatInt(sizeBytes, 10)},
			"X-Amz-Checksum-Sha256": []string{checksum},
		},
		IssuedAt: time.Now(), ExpiresAt: expiresAt,
	}, nil
}

func (store *recordingArtifactUploadStore) CompleteMultipartUpload(
	_ context.Context,
	upload artifactstore.MultipartUpload,
	parts []artifactstore.CompletedPart,
) (artifactstore.ObjectVersion, error) {
	if store.beforeComplete != nil {
		store.beforeComplete()
	}
	store.completed = append(store.completed, upload)
	if store.completeErr != nil {
		return artifactstore.ObjectVersion{}, store.completeErr
	}
	return artifactstore.ObjectVersion{
		ObjectKey: upload.ObjectKey, VersionID: "version-1", SizeBytes: 15, ContentType: "video/mp4",
		ChecksumSHA256: mustMultipartChecksumForStore(parts),
	}, nil
}

func (store *recordingArtifactUploadStore) HeadCurrentVersion(
	_ context.Context,
	objectKey string,
) (artifactstore.ObjectVersion, error) {
	store.headCalls++
	if store.headVersion.VersionID != "" {
		return store.headVersion, nil
	}
	return artifactstore.ObjectVersion{
		ObjectKey: objectKey, VersionID: "version-1", SizeBytes: 15, ContentType: "video/mp4",
	}, nil
}

func (store *recordingArtifactUploadStore) AbortMultipartUpload(
	_ context.Context,
	upload artifactstore.MultipartUpload,
) error {
	store.aborted = append(store.aborted, upload)
	return nil
}

type recordingConnectStream struct {
	grpc.ServerStream
	ctx       context.Context
	requests  []*velav1.ConnectRequest
	responses []*velav1.ConnectResponse
	index     int
}

func (stream *recordingConnectStream) Context() context.Context {
	return stream.ctx
}

func (stream *recordingConnectStream) Recv() (*velav1.ConnectRequest, error) {
	if stream.index == len(stream.requests) {
		return nil, io.EOF
	}
	request := stream.requests[stream.index]
	stream.index++
	return request, nil
}

func (stream *recordingConnectStream) Send(response *velav1.ConnectResponse) error {
	stream.responses = append(stream.responses, response)
	return nil
}

func sha256Bytes(value string) []byte {
	digest := sha256.Sum256([]byte(value))
	return digest[:]
}

func mustMultipartChecksum(t *testing.T, parts []artifactstore.CompletedPart) string {
	t.Helper()
	checksum, err := artifactstore.MultipartCompositeChecksum(parts)
	if err != nil {
		t.Fatalf("multipart checksum: %v", err)
	}
	return checksum
}

func mustMultipartChecksumForStore(parts []artifactstore.CompletedPart) string {
	checksum, _ := artifactstore.MultipartCompositeChecksum(parts)
	return checksum
}

func mtlsPeerContext(t *testing.T, spiffeID string) context.Context {
	t.Helper()
	uri, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatalf("parse SPIFFE ID: %v", err)
	}
	certificate := &x509.Certificate{URIs: []*url.URL{uri}}
	return peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
			HandshakeComplete: true,
			PeerCertificates:  []*x509.Certificate{certificate},
			VerifiedChains:    [][]*x509.Certificate{{certificate}},
		}},
	})
}

var _ velav1.WorkerControlService_ConnectServer = (*recordingConnectStream)(nil)
