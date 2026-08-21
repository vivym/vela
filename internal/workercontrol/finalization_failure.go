package workercontrol

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	store "github.com/vivym/vela/internal/store/sqlc"
)

type ArtifactFinalizationFailureCode string

const (
	ArtifactFinalizationFailureInvalidCompletionIntent ArtifactFinalizationFailureCode = "INVALID_COMPLETION_INTENT"
	ArtifactFinalizationFailureMultipartPartsMismatch  ArtifactFinalizationFailureCode = "MULTIPART_PARTS_MISMATCH"
	ArtifactFinalizationFailureCompletedObjectMismatch ArtifactFinalizationFailureCode = "COMPLETED_OBJECT_MISMATCH"
	ArtifactFinalizationFailureValidation              ArtifactFinalizationFailureCode = "VALIDATION_FAILED"
	ArtifactFinalizationFailureRecoveryReceiptMismatch ArtifactFinalizationFailureCode = "RECOVERY_RECEIPT_MISMATCH"
)

type ArtifactFinalizationFailure struct {
	ArtifactID   uuid.UUID
	UploadID     uuid.UUID
	Code         ArtifactFinalizationFailureCode
	ErrorSummary string
}

func (s *Service) RecordUnrecoverableArtifactFinalizationAsReconciler(
	ctx context.Context,
	reconciler AuthenticatedReconciler,
	credentials ReconcilerFinalizationCredentials,
	failure ArtifactFinalizationFailure,
) (RetryDecision, error) {
	if s == nil || s.pool == nil {
		return RetryDecision{}, errors.New("worker coordinator is not configured")
	}
	if !validPrintableFailureText(reconciler.ID, 500) || credentials.AttemptID == uuid.Nil ||
		credentials.WorkerID == uuid.Nil || credentials.WorkerEpoch <= 0 || credentials.Fence <= 0 ||
		credentials.Token == "" || failure.ArtifactID == uuid.Nil || failure.UploadID == uuid.Nil ||
		!validArtifactFinalizationFailureCode(failure.Code) {
		return rejectedStaleFailure(), nil
	}
	observation := artifactFinalizationFailureObservation(failure)
	normalized, requestHash, err := normalizeFailureObservation(observation)
	if err != nil {
		return RetryDecision{}, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return RetryDecision{}, fmt.Errorf("begin unrecoverable Artifact finalization transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	worker, err := queries.LockWorkerAuthority(ctx, credentials.WorkerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedStaleFailure(), nil
	}
	if err != nil {
		return RetryDecision{}, fmt.Errorf("lock Worker for unrecoverable Artifact finalization: %w", err)
	}
	if err := queries.LockExecutionLeaseWrites(ctx); err != nil {
		return RetryDecision{}, fmt.Errorf("lock Lease writes for unrecoverable Artifact finalization: %w", err)
	}
	authority, err := queries.LockFailureAuthority(ctx, credentials.AttemptID)
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedStaleFailure(), nil
	}
	if err != nil {
		return RetryDecision{}, fmt.Errorf("lock unrecoverable Artifact finalization authority: %w", err)
	}
	if authority.WorkerID != credentials.WorkerID || authority.WorkerEpoch != credentials.WorkerEpoch ||
		authority.AttemptFence != credentials.Fence || authority.LeasePhase != store.LeasePhaseFINALIZATION ||
		authority.LeaseOwnerKind != store.LeaseOwnerKindRECONCILER || authority.LeaseOwnerID != reconciler.ID {
		return rejectedStaleFailure(), nil
	}
	presentedDigest := sha256.Sum256([]byte(credentials.Token))
	if !hmac.Equal(presentedDigest[:], authority.TokenDigest) {
		return rejectedStaleFailure(), nil
	}
	stored, decisionErr := queries.GetExecutionFailureDecision(
		ctx,
		uuid.NullUUID{UUID: credentials.AttemptID, Valid: true},
	)
	if decisionErr == nil {
		if !hmac.Equal(requestHash[:], stored.RequestHash) ||
			stored.Source != store.ExecutionFailureSourceARTIFACTRECOVERYUNRECOVERABLE ||
			!stored.ArtifactID.Valid || stored.ArtifactID.UUID != failure.ArtifactID ||
			!stored.ArtifactUploadID.Valid || stored.ArtifactUploadID.UUID != failure.UploadID ||
			stored.FinalizationFailureCode == nil || *stored.FinalizationFailureCode != string(failure.Code) {
			return rejectedStaleFailure(), nil
		}
		decision, convertErr := retryDecisionFromStored(stored)
		if convertErr != nil {
			return RetryDecision{}, convertErr
		}
		if err := tx.Commit(ctx); err != nil {
			return RetryDecision{}, fmt.Errorf("commit unrecoverable Artifact finalization replay: %w", err)
		}
		return decision, nil
	}
	if !errors.Is(decisionErr, pgx.ErrNoRows) {
		return RetryDecision{}, fmt.Errorf("read durable Artifact finalization failure: %w", decisionErr)
	}
	now, err := postgresTime(ctx, queries)
	if err != nil {
		return RetryDecision{}, err
	}
	if authority.LeaseRevokedAt.Valid || authority.CurrentFence != credentials.Fence ||
		!authority.LeaseExpiresAt.Valid || !authority.LeaseExpiresAt.Time.After(now) ||
		!authority.FinalizationDeadlineAt.Valid || !authority.FinalizationDeadlineAt.Time.After(now) ||
		!authority.JobExpiresAt.Valid || !authority.JobExpiresAt.Time.After(now) ||
		authority.AttemptState != store.AttemptStateFINALIZING || authority.JobState != store.JobStateFINALIZING {
		return rejectedStaleFailure(), nil
	}
	upload, err := queries.GetArtifactUploadStatus(ctx, store.GetArtifactUploadStatusParams{
		UploadID:     failure.UploadID,
		AttemptID:    authority.AttemptID,
		AttemptFence: authority.AttemptFence,
	})
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && upload.ArtifactID != failure.ArtifactID) {
		return rejectedStaleFailure(), nil
	}
	if err != nil {
		return RetryDecision{}, fmt.Errorf("lock failed ArtifactUpload: %w", err)
	}
	code := string(failure.Code)
	decision, err := s.applyAttemptFailure(
		ctx,
		queries,
		worker,
		authority,
		normalized,
		requestHash,
		now,
		attemptFailureTransition{
			Source:                  store.ExecutionFailureSourceARTIFACTRECOVERYUNRECOVERABLE,
			AttemptState:            store.AttemptStateFAILED,
			AllowRetry:              false,
			WorkerTransition:        workerFailureDrain,
			ArtifactID:              uuid.NullUUID{UUID: failure.ArtifactID, Valid: true},
			ArtifactUploadID:        uuid.NullUUID{UUID: failure.UploadID, Valid: true},
			FinalizationFailureCode: &code,
		},
	)
	if err != nil {
		return RetryDecision{}, err
	}
	commitTime, err := postgresTime(ctx, queries)
	if err != nil {
		return RetryDecision{}, err
	}
	if !authority.LeaseExpiresAt.Time.After(commitTime) ||
		!authority.FinalizationDeadlineAt.Time.After(commitTime) ||
		!authority.JobExpiresAt.Time.After(commitTime) {
		return rejectedStaleFailure(), nil
	}
	if err := tx.Commit(ctx); err != nil {
		return RetryDecision{}, fmt.Errorf("commit unrecoverable Artifact finalization: %w", err)
	}
	return decision, nil
}

func artifactFinalizationFailureObservation(failure ArtifactFinalizationFailure) FailureObservation {
	code := strings.ToLower(strings.ReplaceAll(string(failure.Code), "_", "-"))
	return FailureObservation{
		FailureClass: "ARTIFACT_UNRECOVERABLE",
		FailureFingerprint: fmt.Sprintf(
			"artifact.recovery.%s/%s/%s",
			code,
			failure.ArtifactID,
			failure.UploadID,
		),
		ErrorSummary:             failure.ErrorSummary,
		BackendStage:             "finalization",
		GPUUUIDs:                 []string{},
		InferenceBackendRevision: "vela-artifact-reconciler/v1",
		RetryRecommended:         false,
		WorkerReusable:           false,
	}
}

func validArtifactFinalizationFailureCode(code ArtifactFinalizationFailureCode) bool {
	switch code {
	case ArtifactFinalizationFailureInvalidCompletionIntent,
		ArtifactFinalizationFailureMultipartPartsMismatch,
		ArtifactFinalizationFailureCompletedObjectMismatch,
		ArtifactFinalizationFailureValidation,
		ArtifactFinalizationFailureRecoveryReceiptMismatch:
		return true
	default:
		return false
	}
}
