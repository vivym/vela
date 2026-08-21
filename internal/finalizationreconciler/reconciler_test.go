package finalizationreconciler

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/workercontrol"
)

func TestReconcilerVerifiesDurablePlanAndCompletesWithStableIdentities(t *testing.T) {
	takeover := testTakeover()
	coordinator := &recordingCoordinator{takeover: takeover}
	reconciler, err := New(coordinator, &recordingUploadStore{}, workercontrol.AuthenticatedReconciler{
		ID: "spiffe://vela.internal/reconciler/artifact-finalization",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	first, err := reconciler.ReconcileNext(context.Background())
	if err != nil {
		t.Fatalf("first ReconcileNext: %v", err)
	}
	second, err := reconciler.ReconcileNext(context.Background())
	if err != nil {
		t.Fatalf("second ReconcileNext: %v", err)
	}
	if first.Takeover != workercontrol.FinalizationTakeoverGranted ||
		first.Verified != len(takeover.Plan.Artifacts) ||
		first.Completion.Decision != workercontrol.VisibleCompletionCommitted ||
		!reflect.DeepEqual(second, first) {
		t.Fatalf("reconciliation results = %#v / %#v", first, second)
	}
	if len(coordinator.verificationCalls) != 4 || len(coordinator.completionCalls) != 2 {
		t.Fatalf(
			"coordinator calls = %d verifications / %d completions",
			len(coordinator.verificationCalls),
			len(coordinator.completionCalls),
		)
	}
	for index := range takeover.Plan.Artifacts {
		firstCall := coordinator.verificationCalls[index]
		secondCall := coordinator.verificationCalls[index+len(takeover.Plan.Artifacts)]
		if firstCall.verificationID == uuid.Nil || firstCall != secondCall ||
			firstCall.uploadID != takeover.Plan.Artifacts[index].UploadID {
			t.Fatalf("verification calls for Artifact %d = %#v / %#v", index, firstCall, secondCall)
		}
	}
	firstCandidate := coordinator.completionCalls[0]
	secondCandidate := coordinator.completionCalls[1]
	if firstCandidate.CompletionID == uuid.Nil || !reflect.DeepEqual(firstCandidate, secondCandidate) ||
		firstCandidate.ExpectedJobVersion != takeover.Plan.JobVersion ||
		!reflect.DeepEqual(firstCandidate.ArtifactIDs, []uuid.UUID{
			takeover.Plan.Artifacts[0].ArtifactID,
			takeover.Plan.Artifacts[1].ArtifactID,
		}) {
		t.Fatalf("Visible Completion candidates = %#v / %#v", firstCandidate, secondCandidate)
	}
}

func TestReconcilerDoesNotCompleteUntilEveryArtifactIsVerified(t *testing.T) {
	takeover := testTakeover()
	coordinator := &recordingCoordinator{
		takeover: takeover,
		verificationDecisions: map[uuid.UUID]workercontrol.ArtifactVerificationDecision{
			takeover.Plan.Artifacts[1].UploadID: workercontrol.ArtifactVerificationBusy,
		},
	}
	reconciler, err := New(coordinator, &recordingUploadStore{}, workercontrol.AuthenticatedReconciler{
		ID: "spiffe://vela.internal/reconciler/artifact-finalization",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := reconciler.ReconcileNext(context.Background())
	if err != nil {
		t.Fatalf("ReconcileNext: %v", err)
	}
	if result.Takeover != workercontrol.FinalizationTakeoverGranted || result.Verified != 1 ||
		result.Completion.Decision != "" {
		t.Fatalf("pending reconciliation result = %#v", result)
	}
	if len(coordinator.completionCalls) != 0 {
		t.Fatalf("pending Artifact set triggered completion: %#v", coordinator.completionCalls)
	}
}

func TestReconcilerRecoversCompletedMultipartAfterResponseLoss(t *testing.T) {
	takeover := testTakeover()
	takeover.Plan.Artifacts = takeover.Plan.Artifacts[:1]
	partDigest := sha256.Sum256([]byte("completed multipart part"))
	objectDigest := sha256.Sum256([]byte("completed Artifact object"))
	partChecksum := base64.StdEncoding.EncodeToString(partDigest[:])
	parts := []workercontrol.ArtifactUploadPart{{
		Number: 1, ETag: "completed-etag", SizeBytes: 24, ChecksumSHA256: partChecksum,
	}}
	coordinator := &recordingCoordinator{
		takeover: takeover,
		uploadStatuses: map[uuid.UUID]workercontrol.ArtifactUploadStatus{
			takeover.Plan.Artifacts[0].UploadID: {
				Decision:                 workercontrol.ArtifactUploadStatusFound,
				UploadID:                 takeover.Plan.Artifacts[0].UploadID,
				ArtifactID:               takeover.Plan.Artifacts[0].ArtifactID,
				State:                    workercontrol.ArtifactUploadStateUploading,
				ObjectKey:                takeover.Plan.Artifacts[0].ObjectKey,
				ExpectedContentType:      "video/mp4",
				MultipartUploadID:        "completed-session",
				CompletedParts:           parts,
				SizeBytes:                24,
				SHA256:                   objectDigest,
				ContentType:              "video/mp4",
				CompletionIntentRecorded: true,
			},
		},
	}
	composite := sha256.Sum256(partDigest[:])
	store := &recordingUploadStore{
		listErr: errors.New("multipart session no longer exists"),
		head: artifactstore.ObjectVersion{
			ObjectKey:      takeover.Plan.Artifacts[0].ObjectKey,
			VersionID:      "recovered-version",
			SizeBytes:      24,
			ContentType:    "video/mp4",
			ChecksumSHA256: base64.StdEncoding.EncodeToString(composite[:]) + "-1",
		},
	}
	reconciler, err := New(coordinator, store, workercontrol.AuthenticatedReconciler{
		ID: "spiffe://vela.internal/reconciler/artifact-finalization",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := reconciler.ReconcileNext(context.Background())
	if err != nil {
		t.Fatalf("ReconcileNext: %v", err)
	}
	if result.Verified != 1 || result.Completion.Decision != workercontrol.VisibleCompletionCommitted {
		t.Fatalf("recovered reconciliation = %#v", result)
	}
	if store.listCalls != 1 || store.completeCalls != 0 || store.headCalls != 1 {
		t.Fatalf("Artifact Store recovery calls = %#v", store)
	}
	if len(coordinator.uploadReports) != 1 {
		t.Fatalf("recovered upload reports = %#v", coordinator.uploadReports)
	}
	report := coordinator.uploadReports[0]
	if report.ObjectVersionID != "recovered-version" || report.SizeBytes != 24 ||
		report.SHA256 != objectDigest || report.ContentType != "video/mp4" ||
		!reflect.DeepEqual(report.CompletedParts, parts) {
		t.Fatalf("recovered upload report = %#v", report)
	}
}

func TestReconcilerRecordsMultipartPartsMismatchAsUnrecoverable(t *testing.T) {
	takeover := testTakeover()
	takeover.Plan.Artifacts = takeover.Plan.Artifacts[:1]
	partDigest := sha256.Sum256([]byte("persisted multipart part"))
	differentDigest := sha256.Sum256([]byte("different multipart part"))
	parts := []workercontrol.ArtifactUploadPart{{
		Number: 1, ETag: "persisted-etag", SizeBytes: 24,
		ChecksumSHA256: base64.StdEncoding.EncodeToString(partDigest[:]),
	}}
	coordinator := &recordingCoordinator{
		takeover: takeover,
		uploadStatuses: map[uuid.UUID]workercontrol.ArtifactUploadStatus{
			takeover.Plan.Artifacts[0].UploadID: {
				Decision:                 workercontrol.ArtifactUploadStatusFound,
				UploadID:                 takeover.Plan.Artifacts[0].UploadID,
				ArtifactID:               takeover.Plan.Artifacts[0].ArtifactID,
				State:                    workercontrol.ArtifactUploadStateUploading,
				ObjectKey:                takeover.Plan.Artifacts[0].ObjectKey,
				ExpectedContentType:      "video/mp4",
				MultipartUploadID:        "mismatched-session",
				CompletedParts:           parts,
				SizeBytes:                24,
				SHA256:                   sha256.Sum256([]byte("completed Artifact object")),
				ContentType:              "video/mp4",
				CompletionIntentRecorded: true,
			},
		},
	}
	store := &recordingUploadStore{list: []artifactstore.CompletedPart{{
		Number: 1, ETag: "different-etag", SizeBytes: 24,
		ChecksumSHA256: base64.StdEncoding.EncodeToString(differentDigest[:]),
	}}}
	reconciler, err := New(coordinator, store, workercontrol.AuthenticatedReconciler{
		ID: "spiffe://vela.internal/reconciler/artifact-finalization",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := reconciler.ReconcileNext(context.Background())
	if err != nil {
		t.Fatalf("ReconcileNext: %v", err)
	}
	if result.Failure == nil || result.Failure.Disposition != workercontrol.RetryDispositionFailed ||
		result.Failure.FailureClass != "ARTIFACT_UNRECOVERABLE" {
		t.Fatalf("unrecoverable reconciliation = %#v", result)
	}
	if len(coordinator.failureCalls) != 1 ||
		coordinator.failureCalls[0].ArtifactID != takeover.Plan.Artifacts[0].ArtifactID ||
		coordinator.failureCalls[0].UploadID != takeover.Plan.Artifacts[0].UploadID ||
		coordinator.failureCalls[0].Code != workercontrol.ArtifactFinalizationFailureMultipartPartsMismatch {
		t.Fatalf("durable Artifact finalization failures = %#v", coordinator.failureCalls)
	}
	if len(coordinator.verificationCalls) != 0 || len(coordinator.completionCalls) != 0 {
		t.Fatalf(
			"unrecoverable Artifact continued to verification/publication: %#v / %#v",
			coordinator.verificationCalls,
			coordinator.completionCalls,
		)
	}
}

func TestReconcilerRecordsCompletedObjectMismatchAsUnrecoverable(t *testing.T) {
	takeover := testTakeover()
	takeover.Plan.Artifacts = takeover.Plan.Artifacts[:1]
	partDigest := sha256.Sum256([]byte("persisted multipart part"))
	parts := []workercontrol.ArtifactUploadPart{{
		Number: 1, ETag: "persisted-etag", SizeBytes: 24,
		ChecksumSHA256: base64.StdEncoding.EncodeToString(partDigest[:]),
	}}
	coordinator := &recordingCoordinator{
		takeover: takeover,
		uploadStatuses: map[uuid.UUID]workercontrol.ArtifactUploadStatus{
			takeover.Plan.Artifacts[0].UploadID: {
				Decision:                 workercontrol.ArtifactUploadStatusFound,
				UploadID:                 takeover.Plan.Artifacts[0].UploadID,
				ArtifactID:               takeover.Plan.Artifacts[0].ArtifactID,
				State:                    workercontrol.ArtifactUploadStateUploading,
				ObjectKey:                takeover.Plan.Artifacts[0].ObjectKey,
				ExpectedContentType:      "video/mp4",
				MultipartUploadID:        "completed-session",
				CompletedParts:           parts,
				SizeBytes:                24,
				SHA256:                   sha256.Sum256([]byte("completed Artifact object")),
				ContentType:              "video/mp4",
				CompletionIntentRecorded: true,
			},
		},
	}
	completedParts := []artifactstore.CompletedPart{{
		Number: 1, ETag: "persisted-etag", SizeBytes: 24,
		ChecksumSHA256: base64.StdEncoding.EncodeToString(partDigest[:]),
	}}
	composite, err := artifactstore.MultipartCompositeChecksum(completedParts)
	if err != nil {
		t.Fatalf("MultipartCompositeChecksum: %v", err)
	}
	store := &recordingUploadStore{
		list: completedParts,
		complete: artifactstore.ObjectVersion{
			ObjectKey:      takeover.Plan.Artifacts[0].ObjectKey,
			VersionID:      "mismatched-version",
			SizeBytes:      25,
			ContentType:    "video/mp4",
			ChecksumSHA256: composite,
		},
	}
	reconciler, err := New(coordinator, store, workercontrol.AuthenticatedReconciler{
		ID: "spiffe://vela.internal/reconciler/artifact-finalization",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := reconciler.ReconcileNext(context.Background())
	if err != nil {
		t.Fatalf("ReconcileNext: %v", err)
	}
	if result.Failure == nil || result.Failure.Disposition != workercontrol.RetryDispositionFailed ||
		result.Failure.FailureClass != "ARTIFACT_UNRECOVERABLE" {
		t.Fatalf("unrecoverable reconciliation = %#v", result)
	}
	if store.listCalls != 1 || store.completeCalls != 1 || store.headCalls != 0 {
		t.Fatalf("Artifact Store recovery calls = %#v", store)
	}
	if len(coordinator.failureCalls) != 1 ||
		coordinator.failureCalls[0].ArtifactID != takeover.Plan.Artifacts[0].ArtifactID ||
		coordinator.failureCalls[0].UploadID != takeover.Plan.Artifacts[0].UploadID ||
		coordinator.failureCalls[0].Code != workercontrol.ArtifactFinalizationFailureCompletedObjectMismatch {
		t.Fatalf("durable Artifact finalization failures = %#v", coordinator.failureCalls)
	}
	if len(coordinator.uploadReports) != 0 || len(coordinator.verificationCalls) != 0 ||
		len(coordinator.completionCalls) != 0 {
		t.Fatalf(
			"mismatched completed object continued to recording/verification/publication: %#v / %#v / %#v",
			coordinator.uploadReports,
			coordinator.verificationCalls,
			coordinator.completionCalls,
		)
	}
}

type verificationCall struct {
	uploadID       uuid.UUID
	verificationID uuid.UUID
}

type recordingCoordinator struct {
	takeover              workercontrol.FinalizationTakeover
	verificationDecisions map[uuid.UUID]workercontrol.ArtifactVerificationDecision
	uploadStatuses        map[uuid.UUID]workercontrol.ArtifactUploadStatus
	uploadReports         []workercontrol.ArtifactUploadReport
	verificationCalls     []verificationCall
	completionCalls       []workercontrol.VisibleCompletionCandidate
	failureCalls          []workercontrol.ArtifactFinalizationFailure
}

func (coordinator *recordingCoordinator) ReconcileNextFinalization(
	context.Context,
	workercontrol.AuthenticatedReconciler,
) (workercontrol.FinalizationTakeover, error) {
	return coordinator.takeover, nil
}

func (coordinator *recordingCoordinator) InspectArtifactUploadAsReconciler(
	_ context.Context,
	_ workercontrol.AuthenticatedReconciler,
	_ workercontrol.ReconcilerFinalizationCredentials,
	uploadID uuid.UUID,
) (workercontrol.ArtifactUploadStatus, error) {
	status, ok := coordinator.uploadStatuses[uploadID]
	if !ok {
		for _, artifact := range coordinator.takeover.Plan.Artifacts {
			if artifact.UploadID == uploadID {
				return workercontrol.ArtifactUploadStatus{
					Decision:        workercontrol.ArtifactUploadStatusFound,
					UploadID:        uploadID,
					ArtifactID:      artifact.ArtifactID,
					State:           workercontrol.ArtifactUploadStateUploaded,
					ObjectKey:       artifact.ObjectKey,
					ObjectVersionID: "persisted-version",
				}, nil
			}
		}
	}
	return status, nil
}

func (coordinator *recordingCoordinator) RecordArtifactUploadedAsReconciler(
	_ context.Context,
	_ workercontrol.AuthenticatedReconciler,
	_ workercontrol.ReconcilerFinalizationCredentials,
	uploadID uuid.UUID,
	report workercontrol.ArtifactUploadReport,
) (workercontrol.ArtifactUploadResult, error) {
	coordinator.uploadReports = append(coordinator.uploadReports, report)
	status := coordinator.uploadStatuses[uploadID]
	status.State = workercontrol.ArtifactUploadStateUploaded
	status.ObjectVersionID = report.ObjectVersionID
	coordinator.uploadStatuses[uploadID] = status
	return workercontrol.ArtifactUploadResult{
		Decision:        workercontrol.ArtifactUploadRecorded,
		UploadID:        uploadID,
		ArtifactID:      status.ArtifactID,
		ObjectVersionID: report.ObjectVersionID,
	}, nil
}

func (coordinator *recordingCoordinator) VerifyArtifactAsReconciler(
	_ context.Context,
	_ workercontrol.AuthenticatedReconciler,
	_ workercontrol.ReconcilerFinalizationCredentials,
	uploadID uuid.UUID,
	verificationID uuid.UUID,
) (workercontrol.ArtifactVerificationResult, error) {
	coordinator.verificationCalls = append(
		coordinator.verificationCalls,
		verificationCall{uploadID: uploadID, verificationID: verificationID},
	)
	decision := coordinator.verificationDecisions[uploadID]
	if decision == "" {
		decision = workercontrol.ArtifactVerified
	}
	result := workercontrol.ArtifactVerificationResult{
		Decision:       decision,
		VerificationID: verificationID,
		UploadID:       uploadID,
	}
	for _, artifact := range coordinator.takeover.Plan.Artifacts {
		if artifact.UploadID == uploadID {
			result.ArtifactID = artifact.ArtifactID
		}
	}
	return result, nil
}

func (coordinator *recordingCoordinator) CompleteVisibleCompletionAsReconciler(
	_ context.Context,
	_ workercontrol.AuthenticatedReconciler,
	_ workercontrol.ReconcilerFinalizationCredentials,
	candidate workercontrol.VisibleCompletionCandidate,
) (workercontrol.VisibleCompletionResult, error) {
	coordinator.completionCalls = append(coordinator.completionCalls, candidate)
	return workercontrol.VisibleCompletionResult{
		Decision:     workercontrol.VisibleCompletionCommitted,
		CompletionID: candidate.CompletionID,
		JobID:        coordinator.takeover.Plan.JobID,
		AttemptID:    coordinator.takeover.Plan.AttemptID,
		JobVersion:   candidate.ExpectedJobVersion + 1,
	}, nil
}

func (coordinator *recordingCoordinator) RecordUnrecoverableArtifactFinalizationAsReconciler(
	_ context.Context,
	_ workercontrol.AuthenticatedReconciler,
	_ workercontrol.ReconcilerFinalizationCredentials,
	failure workercontrol.ArtifactFinalizationFailure,
) (workercontrol.RetryDecision, error) {
	coordinator.failureCalls = append(coordinator.failureCalls, failure)
	return workercontrol.RetryDecision{
		Disposition:  workercontrol.RetryDispositionFailed,
		FailureClass: "ARTIFACT_UNRECOVERABLE",
		AttemptID:    coordinator.takeover.Plan.AttemptID,
		JobID:        coordinator.takeover.Plan.JobID,
		AttemptState: workercontrol.FailedAttempt,
	}, nil
}

type recordingUploadStore struct {
	list          []artifactstore.CompletedPart
	listErr       error
	complete      artifactstore.ObjectVersion
	completeErr   error
	head          artifactstore.ObjectVersion
	headErr       error
	listCalls     int
	completeCalls int
	headCalls     int
}

func (store *recordingUploadStore) ListParts(
	context.Context,
	artifactstore.MultipartUpload,
) ([]artifactstore.CompletedPart, error) {
	store.listCalls++
	return store.list, store.listErr
}

func (store *recordingUploadStore) CompleteMultipartUpload(
	_ context.Context,
	_ artifactstore.MultipartUpload,
	_ []artifactstore.CompletedPart,
) (artifactstore.ObjectVersion, error) {
	store.completeCalls++
	return store.complete, store.completeErr
}

func (store *recordingUploadStore) HeadCurrentVersion(
	context.Context,
	string,
) (artifactstore.ObjectVersion, error) {
	store.headCalls++
	return store.head, store.headErr
}

func testTakeover() workercontrol.FinalizationTakeover {
	attemptID := uuid.New()
	startedAt := time.Now()
	deadlineAt := startedAt.Add(10 * time.Minute)
	return workercontrol.FinalizationTakeover{
		Decision: workercontrol.FinalizationTakeoverGranted,
		LeaseID:  uuid.New(),
		Credentials: workercontrol.ReconcilerFinalizationCredentials{
			AttemptID:   attemptID,
			WorkerID:    uuid.New(),
			WorkerEpoch: 7,
			Fence:       9,
			Token:       "reconciler-finalization-token",
		},
		LeaseExpiresAt: startedAt.Add(time.Minute),
		Plan: workercontrol.FinalizationPlan{
			Decision:               workercontrol.FinalizationGranted,
			AttemptID:              attemptID,
			JobID:                  uuid.New(),
			JobVersion:             4,
			FinalizationStartedAt:  startedAt,
			FinalizationDeadlineAt: deadlineAt,
			Artifacts: []workercontrol.PlannedArtifact{
				{
					ArtifactID: uuid.New(),
					UploadID:   uuid.New(),
					Kind:       workercontrol.ArtifactKindVideo,
					ObjectKey:  "artifacts/org/project/job/attempt/video/video.mp4",
					ExpiresAt:  deadlineAt,
				},
				{
					ArtifactID: uuid.New(),
					UploadID:   uuid.New(),
					Kind:       workercontrol.ArtifactKindThumbnail,
					ObjectKey:  "artifacts/org/project/job/attempt/thumbnail/thumbnail.webp",
					ExpiresAt:  deadlineAt,
				},
			},
		},
	}
}
