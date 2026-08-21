package finalizationreconciler

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/workercontrol"
)

var reconciliationIdentityNamespace = uuid.MustParse("f004bed8-8b87-4ba6-b3ff-066b1c693b46")

type Coordinator interface {
	ReconcileNextFinalization(
		context.Context,
		workercontrol.AuthenticatedReconciler,
	) (workercontrol.FinalizationTakeover, error)
	InspectArtifactUploadAsReconciler(
		context.Context,
		workercontrol.AuthenticatedReconciler,
		workercontrol.ReconcilerFinalizationCredentials,
		uuid.UUID,
	) (workercontrol.ArtifactUploadStatus, error)
	RecordArtifactUploadedAsReconciler(
		context.Context,
		workercontrol.AuthenticatedReconciler,
		workercontrol.ReconcilerFinalizationCredentials,
		uuid.UUID,
		workercontrol.ArtifactUploadReport,
	) (workercontrol.ArtifactUploadResult, error)
	VerifyArtifactAsReconciler(
		context.Context,
		workercontrol.AuthenticatedReconciler,
		workercontrol.ReconcilerFinalizationCredentials,
		uuid.UUID,
		uuid.UUID,
	) (workercontrol.ArtifactVerificationResult, error)
	CompleteVisibleCompletionAsReconciler(
		context.Context,
		workercontrol.AuthenticatedReconciler,
		workercontrol.ReconcilerFinalizationCredentials,
		workercontrol.VisibleCompletionCandidate,
	) (workercontrol.VisibleCompletionResult, error)
	RecordUnrecoverableArtifactFinalizationAsReconciler(
		context.Context,
		workercontrol.AuthenticatedReconciler,
		workercontrol.ReconcilerFinalizationCredentials,
		workercontrol.ArtifactFinalizationFailure,
	) (workercontrol.RetryDecision, error)
}

type ArtifactCompletionStore interface {
	ListParts(context.Context, artifactstore.MultipartUpload) ([]artifactstore.CompletedPart, error)
	CompleteMultipartUpload(
		context.Context,
		artifactstore.MultipartUpload,
		[]artifactstore.CompletedPart,
	) (artifactstore.ObjectVersion, error)
	HeadCurrentVersion(context.Context, string) (artifactstore.ObjectVersion, error)
}

type Result struct {
	Takeover   workercontrol.FinalizationTakeoverDecision
	AttemptID  uuid.UUID
	JobID      uuid.UUID
	Verified   int
	Completion workercontrol.VisibleCompletionResult
	Failure    *workercontrol.RetryDecision
}

type Reconciler struct {
	coordinator Coordinator
	store       ArtifactCompletionStore
	identity    workercontrol.AuthenticatedReconciler
}

func New(
	coordinator Coordinator,
	store ArtifactCompletionStore,
	identity workercontrol.AuthenticatedReconciler,
) (*Reconciler, error) {
	if coordinator == nil || store == nil || !validIdentity(identity.ID) {
		return nil, errors.New("invalid Artifact Reconciler configuration")
	}
	return &Reconciler{coordinator: coordinator, store: store, identity: identity}, nil
}

func (reconciler *Reconciler) ReconcileNext(ctx context.Context) (Result, error) {
	if reconciler == nil || reconciler.coordinator == nil || reconciler.store == nil {
		return Result{}, errors.New("artifact Reconciler is not configured")
	}
	if ctx == nil {
		return Result{}, errors.New("artifact Reconciler context is required")
	}
	takeover, err := reconciler.coordinator.ReconcileNextFinalization(ctx, reconciler.identity)
	if err != nil {
		return Result{}, fmt.Errorf("claim Artifact finalization: %w", err)
	}
	result := Result{Takeover: takeover.Decision}
	if takeover.Decision == workercontrol.FinalizationTakeoverNoWork {
		return result, nil
	}
	if err := validateTakeover(takeover); err != nil {
		return Result{}, err
	}
	result.AttemptID = takeover.Plan.AttemptID
	result.JobID = takeover.Plan.JobID

	artifactIDs := make([]uuid.UUID, 0, len(takeover.Plan.Artifacts))
	for _, artifact := range takeover.Plan.Artifacts {
		recovered, failure, recoverErr := reconciler.recoverArtifactUpload(ctx, takeover, artifact)
		if recoverErr != nil {
			return result, fmt.Errorf("recover durable Artifact %s: %w", artifact.ArtifactID, recoverErr)
		}
		if failure != nil {
			return reconciler.recordUnrecoverable(ctx, takeover, result, *failure)
		}
		if !recovered {
			continue
		}
		verification, verifyErr := reconciler.coordinator.VerifyArtifactAsReconciler(
			ctx,
			reconciler.identity,
			takeover.Credentials,
			artifact.UploadID,
			stableIdentity("verification", artifact.UploadID.String()),
		)
		if verifyErr != nil {
			return result, fmt.Errorf("verify durable Artifact %s: %w", artifact.ArtifactID, verifyErr)
		}
		if verification.Decision == workercontrol.ArtifactValidationFailed {
			return reconciler.recordUnrecoverable(
				ctx,
				takeover,
				result,
				workercontrol.ArtifactFinalizationFailure{
					ArtifactID:   artifact.ArtifactID,
					UploadID:     artifact.UploadID,
					Code:         workercontrol.ArtifactFinalizationFailureValidation,
					ErrorSummary: "Artifact validation rejected the persisted object version",
				},
			)
		}
		if verification.Decision != workercontrol.ArtifactVerified {
			continue
		}
		if verification.UploadID != artifact.UploadID || verification.ArtifactID != artifact.ArtifactID ||
			verification.VerificationID == uuid.Nil {
			return reconciler.recordUnrecoverable(
				ctx,
				takeover,
				result,
				workercontrol.ArtifactFinalizationFailure{
					ArtifactID:   artifact.ArtifactID,
					UploadID:     artifact.UploadID,
					Code:         workercontrol.ArtifactFinalizationFailureRecoveryReceiptMismatch,
					ErrorSummary: "Artifact verification receipt does not match the finalization plan",
				},
			)
		}
		result.Verified++
		artifactIDs = append(artifactIDs, artifact.ArtifactID)
	}
	if result.Verified != len(takeover.Plan.Artifacts) {
		return result, nil
	}

	completion, err := reconciler.coordinator.CompleteVisibleCompletionAsReconciler(
		ctx,
		reconciler.identity,
		takeover.Credentials,
		workercontrol.VisibleCompletionCandidate{
			CompletionID: stableIdentity(
				"visible-completion",
				fmt.Sprintf(
					"%s/%d/%d",
					takeover.Plan.AttemptID,
					takeover.Credentials.Fence,
					takeover.Plan.JobVersion,
				),
			),
			ExpectedJobVersion: takeover.Plan.JobVersion,
			ArtifactIDs:        artifactIDs,
		},
	)
	if err != nil {
		return result, fmt.Errorf("publish reconciled Visible Completion: %w", err)
	}
	result.Completion = completion
	return result, nil
}

func (reconciler *Reconciler) recoverArtifactUpload(
	ctx context.Context,
	takeover workercontrol.FinalizationTakeover,
	artifact workercontrol.PlannedArtifact,
) (bool, *workercontrol.ArtifactFinalizationFailure, error) {
	status, err := reconciler.coordinator.InspectArtifactUploadAsReconciler(
		ctx,
		reconciler.identity,
		takeover.Credentials,
		artifact.UploadID,
	)
	if err != nil {
		return false, nil, err
	}
	if status.Decision != workercontrol.ArtifactUploadStatusFound {
		return false, nil, nil
	}
	if status.UploadID != artifact.UploadID || status.ArtifactID != artifact.ArtifactID ||
		status.ObjectKey != artifact.ObjectKey {
		return false, unrecoverableFailure(
			artifact,
			workercontrol.ArtifactFinalizationFailureRecoveryReceiptMismatch,
			"ArtifactUpload status does not match the finalization plan",
		), nil
	}
	switch status.State {
	case workercontrol.ArtifactUploadStateUploaded, workercontrol.ArtifactUploadStateVerified:
		if status.ObjectVersionID == "" {
			return false, unrecoverableFailure(
				artifact,
				workercontrol.ArtifactFinalizationFailureRecoveryReceiptMismatch,
				"ArtifactUpload receipt has no object version",
			), nil
		}
		return true, nil, nil
	case workercontrol.ArtifactUploadStateUploading:
		if !status.CompletionIntentRecorded || status.MultipartUploadID == "" {
			return false, nil, nil
		}
		if status.SizeBytes <= 0 || status.SHA256 == [sha256.Size]byte{} ||
			status.ContentType == "" || status.ContentType != status.ExpectedContentType {
			return false, unrecoverableFailure(
				artifact,
				workercontrol.ArtifactFinalizationFailureInvalidCompletionIntent,
				"persisted Artifact completion intent is invalid",
			), nil
		}
	default:
		return false, nil, nil
	}

	parts, err := completionParts(status.CompletedParts)
	if err != nil {
		return false, unrecoverableFailure(
			artifact,
			workercontrol.ArtifactFinalizationFailureInvalidCompletionIntent,
			err.Error(),
		), nil
	}
	upload := artifactstore.MultipartUpload{
		ObjectKey:   status.ObjectKey,
		UploadID:    status.MultipartUploadID,
		ContentType: status.ExpectedContentType,
	}
	listed, listErr := reconciler.store.ListParts(ctx, upload)
	var object artifactstore.ObjectVersion
	if listErr == nil {
		if !artifactstore.EqualCompletedParts(listed, parts) {
			return false, unrecoverableFailure(
				artifact,
				workercontrol.ArtifactFinalizationFailureMultipartPartsMismatch,
				"stored completion intent does not match multipart parts",
			), nil
		}
		object, err = reconciler.store.CompleteMultipartUpload(ctx, upload, parts)
	}
	if listErr != nil || err != nil {
		object, err = reconciler.store.HeadCurrentVersion(ctx, status.ObjectKey)
		if err != nil {
			return false, nil, errors.Join(listErr, err)
		}
	}
	expectedChecksum, err := artifactstore.MultipartCompositeChecksum(parts)
	if err != nil {
		return false, unrecoverableFailure(
			artifact,
			workercontrol.ArtifactFinalizationFailureInvalidCompletionIntent,
			err.Error(),
		), nil
	}
	if object.ObjectKey != status.ObjectKey || object.VersionID == "" ||
		object.SizeBytes != status.SizeBytes || strings.TrimSpace(object.ContentType) != status.ContentType ||
		object.ChecksumSHA256 != expectedChecksum {
		return false, unrecoverableFailure(
			artifact,
			workercontrol.ArtifactFinalizationFailureCompletedObjectMismatch,
			"completed Artifact object does not match its persisted intent",
		), nil
	}
	recorded, err := reconciler.coordinator.RecordArtifactUploadedAsReconciler(
		ctx,
		reconciler.identity,
		takeover.Credentials,
		artifact.UploadID,
		workercontrol.ArtifactUploadReport{
			ObjectVersionID: object.VersionID,
			SizeBytes:       status.SizeBytes,
			SHA256:          status.SHA256,
			ContentType:     status.ContentType,
			CompletedParts:  status.CompletedParts,
		},
	)
	if err != nil {
		return false, nil, err
	}
	if recorded.Decision != workercontrol.ArtifactUploadRecorded ||
		recorded.UploadID != artifact.UploadID || recorded.ArtifactID != artifact.ArtifactID ||
		recorded.ObjectVersionID != object.VersionID {
		return false, unrecoverableFailure(
			artifact,
			workercontrol.ArtifactFinalizationFailureRecoveryReceiptMismatch,
			"recovered ArtifactUpload receipt does not match its intent",
		), nil
	}
	return true, nil, nil
}

func (reconciler *Reconciler) recordUnrecoverable(
	ctx context.Context,
	takeover workercontrol.FinalizationTakeover,
	result Result,
	failure workercontrol.ArtifactFinalizationFailure,
) (Result, error) {
	decision, err := reconciler.coordinator.RecordUnrecoverableArtifactFinalizationAsReconciler(
		ctx,
		reconciler.identity,
		takeover.Credentials,
		failure,
	)
	if err != nil {
		return result, fmt.Errorf("record unrecoverable Artifact finalization: %w", err)
	}
	if decision.Disposition != workercontrol.RetryDispositionFailed &&
		decision.Disposition != workercontrol.RetryDispositionRejectedStaleLease {
		return result, errors.New("artifact Reconciler received an invalid unrecoverable outcome")
	}
	result.Failure = &decision
	return result, nil
}

func unrecoverableFailure(
	artifact workercontrol.PlannedArtifact,
	code workercontrol.ArtifactFinalizationFailureCode,
	summary string,
) *workercontrol.ArtifactFinalizationFailure {
	return &workercontrol.ArtifactFinalizationFailure{
		ArtifactID:   artifact.ArtifactID,
		UploadID:     artifact.UploadID,
		Code:         code,
		ErrorSummary: summary,
	}
}

func completionParts(parts []workercontrol.ArtifactUploadPart) ([]artifactstore.CompletedPart, error) {
	if len(parts) == 0 || len(parts) > 10_000 {
		return nil, errors.New("persisted completion intent has invalid parts")
	}
	result := make([]artifactstore.CompletedPart, len(parts))
	for index, part := range parts {
		if part.Number != int32(index+1) || part.ETag == "" || part.SizeBytes <= 0 ||
			part.ChecksumSHA256 == "" {
			return nil, errors.New("persisted completion intent has invalid parts")
		}
		result[index] = artifactstore.CompletedPart{
			Number:         part.Number,
			ETag:           part.ETag,
			SizeBytes:      part.SizeBytes,
			ChecksumSHA256: part.ChecksumSHA256,
		}
	}
	if _, err := artifactstore.MultipartCompositeChecksum(result); err != nil {
		return nil, err
	}
	return result, nil
}

func validateTakeover(takeover workercontrol.FinalizationTakeover) error {
	if takeover.Decision != workercontrol.FinalizationTakeoverGranted ||
		takeover.LeaseID == uuid.Nil || takeover.Credentials.AttemptID == uuid.Nil ||
		takeover.Credentials.WorkerID == uuid.Nil || takeover.Credentials.WorkerEpoch <= 0 ||
		takeover.Credentials.Fence <= 0 || takeover.Credentials.Token == "" ||
		takeover.Plan.Decision != workercontrol.FinalizationGranted ||
		takeover.Plan.AttemptID != takeover.Credentials.AttemptID ||
		takeover.Plan.JobID == uuid.Nil || takeover.Plan.JobVersion <= 0 ||
		len(takeover.Plan.Artifacts) == 0 || takeover.Plan.FinalizationDeadlineAt.IsZero() ||
		!takeover.Plan.FinalizationDeadlineAt.After(takeover.Plan.FinalizationStartedAt) {
		return errors.New("artifact Reconciler received an invalid finalization takeover")
	}
	artifactIDs := make(map[uuid.UUID]struct{}, len(takeover.Plan.Artifacts))
	uploadIDs := make(map[uuid.UUID]struct{}, len(takeover.Plan.Artifacts))
	for _, artifact := range takeover.Plan.Artifacts {
		if artifact.ArtifactID == uuid.Nil || artifact.UploadID == uuid.Nil ||
			artifact.ObjectKey == "" || artifact.ExpiresAt.IsZero() {
			return errors.New("artifact Reconciler received an incomplete Artifact plan")
		}
		if _, exists := artifactIDs[artifact.ArtifactID]; exists {
			return errors.New("artifact Reconciler received duplicate Artifact identity")
		}
		if _, exists := uploadIDs[artifact.UploadID]; exists {
			return errors.New("artifact Reconciler received duplicate upload identity")
		}
		artifactIDs[artifact.ArtifactID] = struct{}{}
		uploadIDs[artifact.UploadID] = struct{}{}
	}
	return nil
}

func stableIdentity(kind string, identity string) uuid.UUID {
	return uuid.NewSHA1(
		reconciliationIdentityNamespace,
		[]byte("vela/"+kind+"/"+identity),
	)
}

func validIdentity(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 500 {
		return false
	}
	for _, character := range value {
		if !unicode.IsPrint(character) || unicode.IsSpace(character) {
			return false
		}
	}
	return true
}
