package workertransport

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestClientClaimsChecksumBoundDebugDumpUpload(t *testing.T) {
	attemptID := uuid.MustParse("7d000000-0000-0000-0000-000000000001")
	dumpID := uuid.MustParse("7d000000-0000-0000-0000-000000000002")
	authorizationID := uuid.MustParse("7d000000-0000-0000-0000-000000000003")
	claimID := uuid.MustParse("7d000000-0000-0000-0000-000000000004")
	digest := sha256.Sum256([]byte(`{"failure":"bounded"}`))
	expiresAt := time.Date(2026, 8, 27, 8, 10, 0, 0, time.UTC)
	server := &oneResponseClientTestServer{response: func(request *velav1.ConnectRequest) *velav1.ConnectResponse {
		operation, ok := request.GetOperation().(*velav1.ConnectRequest_ClaimDebugDumpUpload)
		if !ok || operation.ClaimDebugDumpUpload.GetDebugDumpId() != dumpID.String() ||
			operation.ClaimDebugDumpUpload.GetAuthorizationId() != authorizationID.String() ||
			operation.ClaimDebugDumpUpload.GetClaimId() != claimID.String() ||
			operation.ClaimDebugDumpUpload.GetPart().GetNumber() != 1 ||
			operation.ClaimDebugDumpUpload.GetPart().GetSizeBytes() != 21 ||
			string(operation.ClaimDebugDumpUpload.GetPart().GetSha256()) != string(digest[:]) {
			return nil
		}
		return &velav1.ConnectResponse{
			RequestId: request.GetRequestId(),
			Result: &velav1.ConnectResponse_DebugDumpUploadClaim{
				DebugDumpUploadClaim: &velav1.DebugDumpUploadClaim{
					Decision: string(workercontrol.DebugDumpUploadClaimGranted),
					ClaimId:  claimID.String(), DebugDumpId: dumpID.String(),
					AuthorizationId:   authorizationID.String(),
					ObjectKey:         "debug-dumps/org/project/job/authorization/attempt/dump",
					ExpectedSizeBytes: 21, ExpectedSha256: digest[:],
					ExpectedContentType: debugDumpUploadContentType,
					ClaimExpiresAt:      timestamppb.New(expiresAt),
					UploadExpiresAt:     timestamppb.New(expiresAt), Version: 2,
					MultipartUploadId: "multipart-debug-1",
					UploadPart: &velav1.SignedDebugDumpUploadPart{
						Number: 1, SizeBytes: 21, Sha256: digest[:],
						Url: "https://objects.internal/debug-dump?partNumber=1",
						RequiredHeaders: map[string]string{
							"Content-Length":        "21",
							"X-Amz-Checksum-Sha256": base64.StdEncoding.EncodeToString(digest[:]),
						},
						ExpiresAt: timestamppb.New(expiresAt),
					},
				},
			},
		}
	}}
	client := newClientTestConnection(t, server)
	credentials := workercontrol.LeaseCredentials{
		AttemptID: attemptID, WorkerEpoch: 7, Fence: 3, Token: "lease-token",
	}
	intent := workercontrol.DebugDumpUploadIntent{
		DebugDumpID: dumpID, AuthorizationID: authorizationID,
		SizeBytes: 21, SHA256: digest, ContentType: debugDumpUploadContentType,
	}

	claim, err := client.ClaimDebugDumpUpload(
		context.Background(), credentials, intent, claimID,
		DebugDumpUploadPartIntent{Number: 1, SizeBytes: 21, SHA256: digest},
	)
	if err != nil {
		t.Fatalf("ClaimDebugDumpUpload: %v", err)
	}
	if claim.Decision != workercontrol.DebugDumpUploadClaimGranted || claim.ClaimID != claimID ||
		claim.DebugDumpID != dumpID || claim.AuthorizationID != authorizationID ||
		claim.MultipartUploadID != "multipart-debug-1" || claim.UploadPart == nil ||
		claim.UploadPart.Number != 1 || claim.UploadPart.SHA256 != digest ||
		claim.UploadPart.RequiredHeaders["Content-Length"] != "21" {
		t.Fatalf("DebugDumpUploadClaim = %#v", claim)
	}
}

func TestClientRejectsDebugDumpClaimForDifferentAuthorization(t *testing.T) {
	attemptID := uuid.MustParse("7e000000-0000-0000-0000-000000000001")
	dumpID := uuid.MustParse("7e000000-0000-0000-0000-000000000002")
	authorizationID := uuid.MustParse("7e000000-0000-0000-0000-000000000003")
	claimID := uuid.MustParse("7e000000-0000-0000-0000-000000000004")
	digest := sha256.Sum256([]byte("debug dump"))
	expiresAt := time.Date(2026, 8, 27, 9, 0, 0, 0, time.UTC)
	server := &oneResponseClientTestServer{response: func(request *velav1.ConnectRequest) *velav1.ConnectResponse {
		return &velav1.ConnectResponse{
			RequestId: request.GetRequestId(),
			Result: &velav1.ConnectResponse_DebugDumpUploadClaim{
				DebugDumpUploadClaim: &velav1.DebugDumpUploadClaim{
					Decision: string(workercontrol.DebugDumpUploadClaimGranted),
					ClaimId:  claimID.String(), DebugDumpId: dumpID.String(),
					AuthorizationId:   uuid.NewString(),
					ObjectKey:         "debug-dumps/org/project/job/authorization/attempt/dump",
					ExpectedSizeBytes: 10, ExpectedSha256: digest[:],
					ExpectedContentType: debugDumpUploadContentType,
					ClaimExpiresAt:      timestamppb.New(expiresAt),
					UploadExpiresAt:     timestamppb.New(expiresAt), Version: 2,
					MultipartUploadId: "multipart-debug-1",
					UploadPart: &velav1.SignedDebugDumpUploadPart{
						Number: 1, SizeBytes: 10, Sha256: digest[:],
						Url:             "https://objects.internal/debug-dump?partNumber=1",
						RequiredHeaders: map[string]string{"Content-Length": "10"},
						ExpiresAt:       timestamppb.New(expiresAt),
					},
				},
			},
		}
	}}
	client := newClientTestConnection(t, server)
	credentials := workercontrol.LeaseCredentials{
		AttemptID: attemptID, WorkerEpoch: 7, Fence: 3, Token: "lease-token",
	}
	intent := workercontrol.DebugDumpUploadIntent{
		DebugDumpID: dumpID, AuthorizationID: authorizationID,
		SizeBytes: 10, SHA256: digest, ContentType: debugDumpUploadContentType,
	}

	if _, err := client.ClaimDebugDumpUpload(
		context.Background(), credentials, intent, claimID,
		DebugDumpUploadPartIntent{Number: 1, SizeBytes: 10, SHA256: digest},
	); err == nil {
		t.Fatal("ClaimDebugDumpUpload accepted a different authorization identity")
	}
}

func TestConnectCompletesAuthorizedDebugDumpOutsideArtifactFlow(t *testing.T) {
	const spiffeID = "spiffe://vela.internal/worker/h3-debug-dump-01"
	workerID := uuid.MustParse("7f000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("7f000000-0000-0000-0000-000000000002")
	dumpID := uuid.MustParse("7f000000-0000-0000-0000-000000000003")
	authorizationID := uuid.MustParse("7f000000-0000-0000-0000-000000000004")
	claimID := uuid.MustParse("7f000000-0000-0000-0000-000000000005")
	payload := []byte(`{"failure":"bounded"}`)
	digest := sha256.Sum256(payload)
	checksum := base64.StdEncoding.EncodeToString(digest[:])
	now := time.Date(2026, 8, 27, 8, 0, 0, 0, time.UTC)
	part := artifactstore.CompletedPart{
		Number: 1, ETag: "etag-debug-1", SizeBytes: int64(len(payload)), ChecksumSHA256: checksum,
	}
	coordinator := &recordingDebugDumpCoordinator{
		recordingFinalizationCoordinator: &recordingFinalizationCoordinator{},
		claim: workercontrol.DebugDumpUploadClaim{
			Decision: workercontrol.DebugDumpUploadClaimGranted, ClaimID: claimID,
			DebugDumpID: dumpID, AuthorizationID: authorizationID,
			ObjectKey:         "debug-dumps/org/project/job/authorization/attempt/dump",
			ExpectedSizeBytes: int64(len(payload)), ExpectedSHA256: digest,
			ExpectedContentType: debugDumpUploadContentType,
			ClaimExpiresAt:      now.Add(30 * time.Second), UploadExpiresAt: now.Add(time.Minute), Version: 1,
		},
		status: workercontrol.DebugDumpUploadStatus{
			Decision:    workercontrol.DebugDumpUploadRecorded,
			DebugDumpID: dumpID, AuthorizationID: authorizationID,
			State:             workercontrol.DebugDumpUploadStateUploading,
			ObjectKey:         "debug-dumps/org/project/job/authorization/attempt/dump",
			ExpectedSizeBytes: int64(len(payload)), ExpectedSHA256: digest,
			ExpectedContentType: debugDumpUploadContentType,
			UploadExpiresAt:     now.Add(time.Minute), Version: 2,
		},
	}
	uploadStore := &recordingArtifactUploadStore{
		createdUploadID: "multipart-debug-1",
		listedParts:     [][]artifactstore.CompletedPart{nil, {part}},
		completeVersion: artifactstore.ObjectVersion{
			ObjectKey: "debug-dumps/org/project/job/authorization/attempt/dump",
			VersionID: "debug-version-1", SizeBytes: int64(len(payload)),
			ContentType: debugDumpUploadContentType,
		},
	}
	server, err := NewServer(
		&recordingIdentityResolver{worker: workercontrol.AuthenticatedWorker{ID: workerID}},
		coordinator,
		uploadStore,
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	lease := &velav1.WorkerLeaseCredentials{
		AttemptId: attemptID.String(), WorkerEpoch: 7, Fence: 3, Token: "lease-token",
	}
	stream := &recordingConnectStream{
		ctx: mtlsPeerContext(t, spiffeID),
		requests: []*velav1.ConnectRequest{
			{
				RequestId: uuid.NewString(),
				Operation: &velav1.ConnectRequest_ClaimDebugDumpUpload{
					ClaimDebugDumpUpload: &velav1.ClaimDebugDumpUploadRequest{
						Lease: lease, DebugDumpId: dumpID.String(), AuthorizationId: authorizationID.String(),
						SizeBytes: int64(len(payload)), Sha256: digest[:], ContentType: debugDumpUploadContentType,
						ClaimId: claimID.String(),
						Part: &velav1.DebugDumpUploadPartIntent{
							Number: 1, SizeBytes: int64(len(payload)), Sha256: digest[:],
						},
					},
				},
			},
			{
				RequestId: uuid.NewString(),
				Operation: &velav1.ConnectRequest_CompleteDebugDumpMultipartUpload{
					CompleteDebugDumpMultipartUpload: &velav1.CompleteDebugDumpMultipartUploadRequest{
						Lease: lease, DebugDumpId: dumpID.String(), ClaimId: claimID.String(),
						AuthorizationId: authorizationID.String(), SizeBytes: int64(len(payload)),
						Sha256: digest[:], ContentType: debugDumpUploadContentType,
						CompletedParts: []*velav1.DebugDumpUploadPartReport{{
							Number: 1, Etag: part.ETag, SizeBytes: part.SizeBytes,
							ChecksumSha256: digest[:],
						}},
					},
				},
			},
		},
	}

	if err := server.Connect(stream); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if len(stream.responses) != 2 || stream.responses[0].GetDebugDumpUploadClaim() == nil ||
		stream.responses[0].GetDebugDumpUploadClaim().GetUploadPart() == nil ||
		stream.responses[1].GetDebugDumpUploadResult().GetDecision() !=
			string(workercontrol.DebugDumpUploadRecorded) ||
		stream.responses[1].GetDebugDumpUploadResult().GetObjectVersionId() != "debug-version-1" {
		t.Fatalf("debug dump responses = %#v", stream.responses)
	}
	if len(coordinator.completionIntents) != 1 || len(coordinator.uploadReports) != 1 ||
		coordinator.uploadReports[0].ObjectVersionID != "debug-version-1" ||
		len(uploadStore.created) != 1 || len(uploadStore.presigned) != 1 || len(uploadStore.completed) != 1 {
		t.Fatalf("debug dump coordinator = %#v store = %#v", coordinator, uploadStore)
	}
}

type recordingDebugDumpCoordinator struct {
	*recordingFinalizationCoordinator
	claim             workercontrol.DebugDumpUploadClaim
	status            workercontrol.DebugDumpUploadStatus
	completionIntents []workercontrol.DebugDumpUploadReport
	uploadReports     []workercontrol.DebugDumpUploadReport
}

func (coordinator *recordingDebugDumpCoordinator) ClaimDebugDumpUpload(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_ workercontrol.DebugDumpUploadIntent,
	_ uuid.UUID,
) (workercontrol.DebugDumpUploadClaim, error) {
	coordinator.record(worker)
	return coordinator.claim, nil
}

func (coordinator *recordingDebugDumpCoordinator) RecordDebugDumpMultipartSession(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	debugDumpID, _ uuid.UUID,
	multipartUploadID string,
) (workercontrol.DebugDumpMultipartSession, error) {
	coordinator.record(worker)
	coordinator.status.MultipartUploadID = multipartUploadID
	return workercontrol.DebugDumpMultipartSession{
		Decision:    workercontrol.DebugDumpMultipartSessionRecorded,
		DebugDumpID: debugDumpID, AuthorizationID: coordinator.claim.AuthorizationID,
		MultipartUploadID: multipartUploadID, Version: 2,
	}, nil
}

func (coordinator *recordingDebugDumpCoordinator) InspectDebugDumpUpload(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	_ uuid.UUID,
) (workercontrol.DebugDumpUploadStatus, error) {
	coordinator.record(worker)
	return coordinator.status, nil
}

func (coordinator *recordingDebugDumpCoordinator) RecordDebugDumpCompletionIntent(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	debugDumpID uuid.UUID,
	report workercontrol.DebugDumpUploadReport,
) (workercontrol.DebugDumpCompletionIntentResult, error) {
	coordinator.record(worker)
	coordinator.completionIntents = append(coordinator.completionIntents, report)
	return workercontrol.DebugDumpCompletionIntentResult{
		Decision:    workercontrol.DebugDumpCompletionIntentRecorded,
		DebugDumpID: debugDumpID, AuthorizationID: coordinator.claim.AuthorizationID, Version: 3,
	}, nil
}

func (coordinator *recordingDebugDumpCoordinator) RecordDebugDumpUploaded(
	_ context.Context,
	worker workercontrol.AuthenticatedWorker,
	_ workercontrol.LeaseCredentials,
	debugDumpID uuid.UUID,
	report workercontrol.DebugDumpUploadReport,
) (workercontrol.DebugDumpUploadResult, error) {
	coordinator.record(worker)
	coordinator.uploadReports = append(coordinator.uploadReports, report)
	return workercontrol.DebugDumpUploadResult{
		Decision:    workercontrol.DebugDumpUploadRecorded,
		DebugDumpID: debugDumpID, AuthorizationID: coordinator.claim.AuthorizationID,
		ObjectVersionID: report.ObjectVersionID, Version: 4,
	}, nil
}
