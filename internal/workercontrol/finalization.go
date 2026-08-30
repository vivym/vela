package workercontrol

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	store "github.com/vivym/vela/internal/store/sqlc"
)

type FinalizationDecision string

const (
	FinalizationGranted            FinalizationDecision = "FINALIZATION_GRANTED"
	FinalizationRejectedStaleLease FinalizationDecision = "REJECTED_STALE_LEASE"
)

type ArtifactKind string

const (
	ArtifactKindVideo     ArtifactKind = "VIDEO"
	ArtifactKindThumbnail ArtifactKind = "THUMBNAIL"
)

type PlannedArtifact struct {
	ArtifactID uuid.UUID
	UploadID   uuid.UUID
	Kind       ArtifactKind
	Ordinal    int32
	ObjectKey  string
	ExpiresAt  time.Time
}

type FinalizationPlan struct {
	Decision               FinalizationDecision
	AttemptID              uuid.UUID
	JobID                  uuid.UUID
	JobVersion             int64
	FinalizationStartedAt  time.Time
	FinalizationDeadlineAt time.Time
	Artifacts              []PlannedArtifact
}

type ArtifactUploadClaimDecision string

const (
	ArtifactUploadClaimGranted            ArtifactUploadClaimDecision = "UPLOAD_CLAIM_GRANTED"
	ArtifactUploadClaimBusy               ArtifactUploadClaimDecision = "UPLOAD_BUSY"
	ArtifactUploadClaimRejectedStaleLease ArtifactUploadClaimDecision = "REJECTED_STALE_LEASE"
)

type ArtifactUploadClaim struct {
	Decision            ArtifactUploadClaimDecision
	ClaimID             uuid.UUID
	UploadID            uuid.UUID
	ArtifactID          uuid.UUID
	ObjectKey           string
	ExpectedContentType string
	MultipartUploadID   string
	ClaimExpiresAt      time.Time
	UploadExpiresAt     time.Time
	Version             int64
}

type ArtifactMultipartSessionDecision string

const (
	ArtifactMultipartSessionRecorded ArtifactMultipartSessionDecision = "MULTIPART_SESSION_RECORDED"
	ArtifactMultipartSessionConflict ArtifactMultipartSessionDecision = "MULTIPART_SESSION_CONFLICT"
	ArtifactMultipartSessionRejected ArtifactMultipartSessionDecision = "REJECTED_STALE_LEASE"
)

type ArtifactMultipartSession struct {
	Decision          ArtifactMultipartSessionDecision
	UploadID          uuid.UUID
	ArtifactID        uuid.UUID
	MultipartUploadID string
	Version           int64
}

type ArtifactUploadPart struct {
	Number         int32  `json:"number"`
	ETag           string `json:"etag"`
	SizeBytes      int64  `json:"size_bytes"`
	ChecksumSHA256 string `json:"checksum_sha256,omitempty"`
}

type ArtifactUploadReport struct {
	ObjectVersionID string
	SizeBytes       int64
	SHA256          [sha256.Size]byte
	ContentType     string
	CompletedParts  []ArtifactUploadPart
}

type ArtifactUploadDecision string

const (
	ArtifactUploadRecorded ArtifactUploadDecision = "ARTIFACT_UPLOAD_RECORDED"
	ArtifactUploadConflict ArtifactUploadDecision = "ARTIFACT_UPLOAD_CONFLICT"
	ArtifactUploadRejected ArtifactUploadDecision = "REJECTED_STALE_LEASE"
)

type ArtifactUploadResult struct {
	Decision        ArtifactUploadDecision
	UploadID        uuid.UUID
	ArtifactID      uuid.UUID
	ObjectVersionID string
	Version         int64
}

type ArtifactCompletionIntentDecision string

const (
	ArtifactCompletionIntentRecorded ArtifactCompletionIntentDecision = "ARTIFACT_COMPLETION_INTENT_RECORDED"
	ArtifactCompletionIntentConflict ArtifactCompletionIntentDecision = "ARTIFACT_COMPLETION_INTENT_CONFLICT"
	ArtifactCompletionIntentRejected ArtifactCompletionIntentDecision = "REJECTED_STALE_LEASE"
)

type ArtifactCompletionIntentResult struct {
	Decision   ArtifactCompletionIntentDecision
	UploadID   uuid.UUID
	ArtifactID uuid.UUID
	Version    int64
}

type ArtifactUploadStatusDecision string

const (
	ArtifactUploadStatusFound              ArtifactUploadStatusDecision = "ARTIFACT_UPLOAD_STATUS_FOUND"
	ArtifactUploadStatusRejectedStaleLease ArtifactUploadStatusDecision = "REJECTED_STALE_LEASE"
)

type ArtifactUploadState string

const (
	ArtifactUploadStateInitiated ArtifactUploadState = "INITIATED"
	ArtifactUploadStateUploading ArtifactUploadState = "UPLOADING"
	ArtifactUploadStateUploaded  ArtifactUploadState = "UPLOADED"
	ArtifactUploadStateVerified  ArtifactUploadState = "VERIFIED"
	ArtifactUploadStateAborted   ArtifactUploadState = "ABORTED"
	ArtifactUploadStateExpired   ArtifactUploadState = "EXPIRED"
)

type ArtifactUploadStatus struct {
	Decision                 ArtifactUploadStatusDecision
	UploadID                 uuid.UUID
	ArtifactID               uuid.UUID
	State                    ArtifactUploadState
	ObjectKey                string
	ExpectedContentType      string
	MultipartUploadID        string
	CompletedParts           []ArtifactUploadPart
	ObjectVersionID          string
	SizeBytes                int64
	SHA256                   [sha256.Size]byte
	ContentType              string
	CompletionIntentRecorded bool
	UploadExpiresAt          time.Time
	Version                  int64
}

type ArtifactInspectionRequest struct {
	ArtifactID             uuid.UUID
	UploadID               uuid.UUID
	Kind                   ArtifactKind
	Ordinal                int32
	ObjectKey              string
	ObjectVersionID        string
	ExpectedSizeBytes      int64
	ExpectedSHA256         [sha256.Size]byte
	ExpectedContentType    string
	ExpectedWidth          int32
	ExpectedHeight         int32
	ExpectedDurationMillis int32
	ExpectedFrameRateMilli int32
	ExpectedFrameCount     int32
	ExpectedCodec          string
	ExpectedContainer      string
}

type ArtifactInspection struct {
	ObjectVersionID   string
	SizeBytes         int64
	SHA256            [sha256.Size]byte
	ContentType       string
	Width             int32
	Height            int32
	DurationMillis    int32
	FrameRateMilli    int32
	FrameCount        int32
	Codec             string
	Container         string
	ValidatorRevision string
}

type ArtifactInspector interface {
	Inspect(context.Context, ArtifactInspectionRequest) (ArtifactInspection, error)
}

type ArtifactVerificationDecision string

const (
	ArtifactVerified             ArtifactVerificationDecision = "ARTIFACT_VERIFIED"
	ArtifactVerificationBusy     ArtifactVerificationDecision = "VERIFICATION_BUSY"
	ArtifactValidationFailed     ArtifactVerificationDecision = "VALIDATION_FAILED"
	ArtifactVerificationRejected ArtifactVerificationDecision = "REJECTED_STALE_LEASE"
)

type ArtifactVerificationResult struct {
	Decision        ArtifactVerificationDecision
	VerificationID  uuid.UUID
	UploadID        uuid.UUID
	ArtifactID      uuid.UUID
	ObjectVersionID string
	Version         int64
	VerifiedAt      time.Time
}

func (s *Service) BeginFinalization(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
) (FinalizationPlan, error) {
	if s == nil || s.pool == nil {
		return FinalizationPlan{}, errors.New("worker coordinator is not configured")
	}
	if worker.ID == uuid.Nil || credentials.AttemptID == uuid.Nil ||
		credentials.WorkerEpoch <= 0 || credentials.Fence <= 0 || credentials.Token == "" {
		return rejectedFinalization(), nil
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return FinalizationPlan{}, fmt.Errorf("begin Finalization transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)

	workerRow, err := queries.LockWorkerAuthority(ctx, worker.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedFinalization(), nil
	}
	if err != nil {
		return FinalizationPlan{}, fmt.Errorf("lock Worker for Finalization: %w", err)
	}
	if err := queries.LockExecutionLeaseWrites(ctx); err != nil {
		return FinalizationPlan{}, fmt.Errorf("lock execution Lease writes for Finalization: %w", err)
	}
	authority, err := queries.LockFinalizationAuthority(ctx, store.LockFinalizationAuthorityParams{
		AttemptID:   credentials.AttemptID,
		WorkerID:    worker.ID,
		WorkerEpoch: credentials.WorkerEpoch,
		Fence:       credentials.Fence,
		OwnerKind:   store.LeaseOwnerKindWORKER,
		OwnerID:     workerRow.SpiffeID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedFinalization(), nil
	}
	if err != nil {
		return FinalizationPlan{}, fmt.Errorf("lock Finalization authority: %w", err)
	}
	if workerRow.Epoch != credentials.WorkerEpoch ||
		authority.CurrentFence != credentials.Fence ||
		authority.AttemptFence != credentials.Fence ||
		authority.LeaseRevokedAt.Valid ||
		authority.CreditReservationState != store.CreditReservationStateRESERVED {
		return rejectedFinalization(), nil
	}
	presentedDigest := sha256.Sum256([]byte(credentials.Token))
	if !hmac.Equal(presentedDigest[:], authority.TokenDigest) {
		return rejectedFinalization(), nil
	}
	now, err := postgresTime(ctx, queries)
	if err != nil {
		return FinalizationPlan{}, err
	}
	if !authority.LeaseExpiresAt.Valid || !authority.LeaseExpiresAt.Time.After(now) ||
		!authority.JobExpiresAt.Valid || !authority.JobExpiresAt.Time.After(now) {
		return rejectedFinalization(), nil
	}

	if authority.AttemptState == store.AttemptStateFINALIZING &&
		authority.JobState == store.JobStateFINALIZING &&
		authority.LeasePhase == store.LeasePhaseFINALIZATION {
		plan, replayErr := finalizationPlan(ctx, queries, authority)
		if replayErr != nil {
			return FinalizationPlan{}, replayErr
		}
		if !plan.FinalizationDeadlineAt.After(now) {
			return rejectedFinalization(), nil
		}
		if err := tx.Commit(ctx); err != nil {
			return FinalizationPlan{}, fmt.Errorf("commit Finalization replay: %w", err)
		}
		return plan, nil
	}
	if authority.AttemptState != store.AttemptStateRUNNING ||
		authority.JobState != store.JobStateRUNNING ||
		authority.LeasePhase != store.LeasePhaseEXECUTION ||
		authority.FinalizationStartedAt.Valid || authority.FinalizationDeadlineAt.Valid {
		return rejectedFinalization(), nil
	}

	deadline := now.Add(time.Duration(authority.ExecutionMaxFinalizationSecondsPerAttempt) * time.Second)
	if authority.JobExpiresAt.Time.Before(deadline) {
		deadline = authority.JobExpiresAt.Time
	}
	startedAt := pgtype.Timestamptz{Time: now, Valid: true}
	deadlineAt := pgtype.Timestamptz{Time: deadline, Valid: true}
	workerEpoch := credentials.WorkerEpoch
	if rows, updateErr := queries.MarkAttemptFinalizing(ctx, store.MarkAttemptFinalizingParams{
		FinalizationStartedAt:  startedAt,
		FinalizationDeadlineAt: deadlineAt,
		AttemptID:              authority.AttemptID,
		WorkerID:               uuid.NullUUID{UUID: worker.ID, Valid: true},
		WorkerEpoch:            &workerEpoch,
		Fence:                  credentials.Fence,
	}); updateErr != nil || rows != 1 {
		return FinalizationPlan{}, changedRowsError("transition Attempt to FINALIZING", rows, updateErr)
	}
	jobVersion, err := queries.MarkJobFinalizing(ctx, store.MarkJobFinalizingParams{
		FinalizationStartedAt: startedAt,
		JobID:                 authority.JobID,
		ExpectedVersion:       authority.JobVersion,
		Fence:                 credentials.Fence,
	})
	if err != nil {
		return FinalizationPlan{}, fmt.Errorf("transition Job to FINALIZING: %w", err)
	}
	if rows, updateErr := queries.MarkLeaseFinalizing(ctx, store.MarkLeaseFinalizingParams{
		FinalizationDeadlineAt: deadlineAt,
		FinalizationStartedAt:  startedAt,
		LeaseID:                authority.LeaseID,
		AttemptID:              authority.AttemptID,
		WorkerID:               worker.ID,
		WorkerEpoch:            credentials.WorkerEpoch,
		Fence:                  credentials.Fence,
	}); updateErr != nil || rows != 1 {
		return FinalizationPlan{}, changedRowsError("transition Lease to FINALIZATION", rows, updateErr)
	}

	for ordinal := int32(0); ordinal < authority.GenerationCount; ordinal++ {
		for _, required := range requiredArtifacts() {
			artifactID := uuid.New()
			uploadID := uuid.New()
			objectKey := finalizationObjectKey(
				authority.OrganizationID,
				authority.ProjectID,
				authority.JobID,
				authority.AttemptID,
				artifactID,
				required.kind,
				authority.Container,
			)
			if err := queries.InsertPlannedArtifact(ctx, store.InsertPlannedArtifactParams{
				ArtifactID:          artifactID,
				OrganizationID:      authority.OrganizationID,
				ProjectID:           authority.ProjectID,
				JobID:               authority.JobID,
				AttemptID:           authority.AttemptID,
				AttemptFence:        authority.AttemptFence,
				Kind:                required.storeKind,
				Ordinal:             ordinal,
				ObjectKey:           objectKey,
				ExpectedContentType: required.contentType,
				ExpiresAt:           deadlineAt,
			}); err != nil {
				return FinalizationPlan{}, fmt.Errorf("insert planned Artifact: %w", err)
			}
			if err := queries.InsertPlannedArtifactUpload(ctx, store.InsertPlannedArtifactUploadParams{
				UploadID:       uploadID,
				OrganizationID: authority.OrganizationID,
				ProjectID:      authority.ProjectID,
				JobID:          authority.JobID,
				AttemptID:      authority.AttemptID,
				AttemptFence:   authority.AttemptFence,
				ArtifactID:     artifactID,
				ExpiresAt:      deadlineAt,
			}); err != nil {
				return FinalizationPlan{}, fmt.Errorf("insert planned ArtifactUpload: %w", err)
			}
		}
	}

	authority.JobVersion = jobVersion
	authority.AttemptState = store.AttemptStateFINALIZING
	authority.JobState = store.JobStateFINALIZING
	authority.LeasePhase = store.LeasePhaseFINALIZATION
	authority.FinalizationStartedAt = startedAt
	authority.FinalizationDeadlineAt = deadlineAt
	plan, err := finalizationPlan(ctx, queries, authority)
	if err != nil {
		return FinalizationPlan{}, err
	}
	commitTime, err := postgresTime(ctx, queries)
	if err != nil {
		return FinalizationPlan{}, err
	}
	if !authority.LeaseExpiresAt.Time.After(commitTime) || !deadline.After(commitTime) {
		return rejectedFinalization(), nil
	}
	if err := tx.Commit(ctx); err != nil {
		return FinalizationPlan{}, fmt.Errorf("commit Finalization: %w", err)
	}
	return plan, nil
}

type requiredArtifact struct {
	kind        ArtifactKind
	storeKind   store.ArtifactKind
	contentType string
}

const artifactClaimTTL = 30 * time.Second

func artifactClaimExpiry(now time.Time, authority store.LockFinalizationAuthorityRow) time.Time {
	expiresAt := now.Add(artifactClaimTTL)
	for _, ceiling := range []time.Time{
		authority.LeaseExpiresAt.Time,
		authority.FinalizationDeadlineAt.Time,
		authority.JobExpiresAt.Time,
	} {
		if ceiling.Before(expiresAt) {
			expiresAt = ceiling
		}
	}
	return expiresAt
}

func requiredArtifacts() []requiredArtifact {
	return []requiredArtifact{
		{kind: ArtifactKindVideo, storeKind: store.ArtifactKindVIDEO, contentType: "video/mp4"},
		{kind: ArtifactKindThumbnail, storeKind: store.ArtifactKindTHUMBNAIL, contentType: "image/webp"},
	}
}

func finalizationObjectKey(
	organizationID, projectID, jobID, attemptID, artifactID uuid.UUID,
	kind ArtifactKind,
	container string,
) string {
	filename := "thumbnail.webp"
	if kind == ArtifactKindVideo {
		filename = "video." + container
	}
	return fmt.Sprintf(
		"artifacts/%s/%s/%s/%s/%s/%s",
		organizationID,
		projectID,
		jobID,
		attemptID,
		artifactID,
		filename,
	)
}

func finalizationPlan(
	ctx context.Context,
	queries *store.Queries,
	authority store.LockFinalizationAuthorityRow,
) (FinalizationPlan, error) {
	if !authority.FinalizationStartedAt.Valid || !authority.FinalizationDeadlineAt.Valid {
		return FinalizationPlan{}, errors.New("finalization authority has no fixed deadline")
	}
	rows, err := queries.ListFinalizationArtifacts(ctx, store.ListFinalizationArtifactsParams{
		AttemptID:    authority.AttemptID,
		AttemptFence: authority.AttemptFence,
	})
	if err != nil {
		return FinalizationPlan{}, fmt.Errorf("list planned Artifacts: %w", err)
	}
	artifacts := make([]PlannedArtifact, 0, len(rows))
	for _, row := range rows {
		artifacts = append(artifacts, PlannedArtifact{
			ArtifactID: row.ArtifactID,
			UploadID:   row.UploadID,
			Kind:       ArtifactKind(row.Kind),
			Ordinal:    row.Ordinal,
			ObjectKey:  row.ObjectKey,
			ExpiresAt:  row.ExpiresAt.Time,
		})
	}
	return FinalizationPlan{
		Decision:               FinalizationGranted,
		AttemptID:              authority.AttemptID,
		JobID:                  authority.JobID,
		JobVersion:             authority.JobVersion,
		FinalizationStartedAt:  authority.FinalizationStartedAt.Time,
		FinalizationDeadlineAt: authority.FinalizationDeadlineAt.Time,
		Artifacts:              artifacts,
	}, nil
}

func rejectedFinalization() FinalizationPlan {
	return FinalizationPlan{Decision: FinalizationRejectedStaleLease}
}

type finalizationActor struct {
	workerID            uuid.UUID
	ownerKind           store.LeaseOwnerKind
	ownerID             string
	requireCurrentEpoch bool
}

type lockedFinalizationAuthority struct {
	ownerID   string
	authority store.LockFinalizationAuthorityRow
	now       time.Time
}

func lockFinalizationAuthority(
	ctx context.Context,
	queries *store.Queries,
	actor finalizationActor,
	credentials LeaseCredentials,
	operation string,
) (lockedFinalizationAuthority, bool, error) {
	workerRow, err := queries.LockWorkerAuthority(ctx, actor.workerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return lockedFinalizationAuthority{}, false, nil
	}
	if err != nil {
		return lockedFinalizationAuthority{}, false, fmt.Errorf("lock Worker for %s: %w", operation, err)
	}
	if err := queries.LockExecutionLeaseWrites(ctx); err != nil {
		return lockedFinalizationAuthority{}, false,
			fmt.Errorf("lock execution Lease writes for %s: %w", operation, err)
	}
	ownerID := actor.ownerID
	if actor.ownerKind == store.LeaseOwnerKindWORKER {
		ownerID = workerRow.SpiffeID
	}
	authority, err := queries.LockFinalizationAuthority(ctx, store.LockFinalizationAuthorityParams{
		AttemptID:   credentials.AttemptID,
		WorkerID:    actor.workerID,
		WorkerEpoch: credentials.WorkerEpoch,
		Fence:       credentials.Fence,
		OwnerKind:   actor.ownerKind,
		OwnerID:     ownerID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return lockedFinalizationAuthority{}, false, nil
	}
	if err != nil {
		return lockedFinalizationAuthority{}, false,
			fmt.Errorf("lock Finalization authority for %s: %w", operation, err)
	}
	presentedDigest := sha256.Sum256([]byte(credentials.Token))
	if (actor.requireCurrentEpoch && workerRow.Epoch != credentials.WorkerEpoch) ||
		authority.CurrentFence != credentials.Fence ||
		authority.AttemptFence != credentials.Fence ||
		authority.AttemptState != store.AttemptStateFINALIZING ||
		authority.JobState != store.JobStateFINALIZING ||
		authority.LeasePhase != store.LeasePhaseFINALIZATION ||
		authority.LeaseRevokedAt.Valid ||
		authority.CreditReservationState != store.CreditReservationStateRESERVED ||
		!hmac.Equal(presentedDigest[:], authority.TokenDigest) {
		return lockedFinalizationAuthority{}, false, nil
	}
	now, err := postgresTime(ctx, queries)
	if err != nil {
		return lockedFinalizationAuthority{}, false, err
	}
	if !authority.LeaseExpiresAt.Valid || !authority.LeaseExpiresAt.Time.After(now) ||
		!authority.FinalizationDeadlineAt.Valid ||
		!authority.FinalizationDeadlineAt.Time.After(now) ||
		!authority.JobExpiresAt.Valid || !authority.JobExpiresAt.Time.After(now) {
		return lockedFinalizationAuthority{}, false, nil
	}
	return lockedFinalizationAuthority{
		ownerID:   ownerID,
		authority: authority,
		now:       now,
	}, true, nil
}

func workerFinalizationActor(worker AuthenticatedWorker) finalizationActor {
	return finalizationActor{
		workerID: worker.ID, ownerKind: store.LeaseOwnerKindWORKER, requireCurrentEpoch: true,
	}
}

func (s *Service) ClaimArtifactUpload(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
	uploadID uuid.UUID,
	claimID uuid.UUID,
) (ArtifactUploadClaim, error) {
	if s == nil || s.pool == nil {
		return ArtifactUploadClaim{}, errors.New("worker coordinator is not configured")
	}
	if worker.ID == uuid.Nil || credentials.AttemptID == uuid.Nil || uploadID == uuid.Nil ||
		claimID == uuid.Nil || credentials.WorkerEpoch <= 0 || credentials.Fence <= 0 ||
		credentials.Token == "" {
		return rejectedArtifactUploadClaim(), nil
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ArtifactUploadClaim{}, fmt.Errorf("begin ArtifactUpload claim transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)

	locked, authorized, err := lockFinalizationAuthority(
		ctx, queries, workerFinalizationActor(worker), credentials, "ArtifactUpload claim",
	)
	if err != nil {
		return ArtifactUploadClaim{}, err
	}
	if !authorized {
		return rejectedArtifactUploadClaim(), nil
	}
	claimExpiresAt := artifactClaimExpiry(locked.now, locked.authority)
	claimOwnerID := locked.ownerID
	row, err := queries.ClaimArtifactUpload(ctx, store.ClaimArtifactUploadParams{
		ClaimID:        uuid.NullUUID{UUID: claimID, Valid: true},
		ClaimOwnerID:   &claimOwnerID,
		ClaimExpiresAt: pgtype.Timestamptz{Time: claimExpiresAt, Valid: true},
		ClaimedAt:      pgtype.Timestamptz{Time: locked.now, Valid: true},
		UploadID:       uploadID,
		AttemptID:      locked.authority.AttemptID,
		AttemptFence:   locked.authority.AttemptFence,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return ArtifactUploadClaim{Decision: ArtifactUploadClaimBusy}, nil
	}
	if err != nil {
		return ArtifactUploadClaim{}, fmt.Errorf("claim ArtifactUpload: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ArtifactUploadClaim{}, fmt.Errorf("commit ArtifactUpload claim: %w", err)
	}
	return ArtifactUploadClaim{
		Decision:            ArtifactUploadClaimGranted,
		ClaimID:             claimID,
		UploadID:            row.UploadID,
		ArtifactID:          row.ArtifactID,
		ObjectKey:           row.ObjectKey,
		ExpectedContentType: row.ExpectedContentType,
		MultipartUploadID:   stringValue(row.MultipartUploadID),
		ClaimExpiresAt:      row.ClaimExpiresAt.Time,
		UploadExpiresAt:     row.ExpiresAt.Time,
		Version:             row.Version,
	}, nil
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func rejectedArtifactUploadClaim() ArtifactUploadClaim {
	return ArtifactUploadClaim{Decision: ArtifactUploadClaimRejectedStaleLease}
}

func (s *Service) InspectArtifactUpload(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
	uploadID uuid.UUID,
) (ArtifactUploadStatus, error) {
	return s.inspectArtifactUpload(ctx, workerFinalizationActor(worker), credentials, uploadID)
}

func (s *Service) InspectArtifactUploadAsReconciler(
	ctx context.Context,
	reconciler AuthenticatedReconciler,
	credentials ReconcilerFinalizationCredentials,
	uploadID uuid.UUID,
) (ArtifactUploadStatus, error) {
	if !validPrintableFailureText(reconciler.ID, 500) {
		return rejectedArtifactUploadStatus(), nil
	}
	return s.inspectArtifactUpload(
		ctx,
		finalizationActor{
			workerID:  credentials.WorkerID,
			ownerKind: store.LeaseOwnerKindRECONCILER,
			ownerID:   reconciler.ID,
		},
		LeaseCredentials{
			AttemptID:   credentials.AttemptID,
			WorkerEpoch: credentials.WorkerEpoch,
			Fence:       credentials.Fence,
			Token:       credentials.Token,
		},
		uploadID,
	)
}

func (s *Service) inspectArtifactUpload(
	ctx context.Context,
	actor finalizationActor,
	credentials LeaseCredentials,
	uploadID uuid.UUID,
) (ArtifactUploadStatus, error) {
	if s == nil || s.pool == nil {
		return ArtifactUploadStatus{}, errors.New("worker coordinator is not configured")
	}
	if actor.workerID == uuid.Nil || credentials.AttemptID == uuid.Nil || uploadID == uuid.Nil ||
		credentials.WorkerEpoch <= 0 || credentials.Fence <= 0 || credentials.Token == "" {
		return rejectedArtifactUploadStatus(), nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ArtifactUploadStatus{}, fmt.Errorf("begin ArtifactUpload status transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	locked, authorized, err := lockFinalizationAuthority(
		ctx, queries, actor, credentials, "ArtifactUpload status",
	)
	if err != nil {
		return ArtifactUploadStatus{}, err
	}
	if !authorized {
		return rejectedArtifactUploadStatus(), nil
	}
	row, err := queries.GetArtifactUploadStatus(ctx, store.GetArtifactUploadStatusParams{
		UploadID:     uploadID,
		AttemptID:    locked.authority.AttemptID,
		AttemptFence: locked.authority.AttemptFence,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedArtifactUploadStatus(), nil
	}
	if err != nil {
		return ArtifactUploadStatus{}, fmt.Errorf("read ArtifactUpload status: %w", err)
	}
	var completedParts []ArtifactUploadPart
	if err := json.Unmarshal(row.CompletedParts, &completedParts); err != nil || len(completedParts) > 10_000 {
		return ArtifactUploadStatus{}, errors.New("stored ArtifactUpload parts are invalid")
	}
	var digest [sha256.Size]byte
	if len(row.Sha256) != 0 {
		if len(row.Sha256) != sha256.Size {
			return ArtifactUploadStatus{}, errors.New("stored ArtifactUpload checksum is invalid")
		}
		copy(digest[:], row.Sha256)
	}
	if len(row.CompletionRequestHash) != 0 && len(row.CompletionRequestHash) != sha256.Size {
		return ArtifactUploadStatus{}, errors.New("stored ArtifactUpload completion intent is invalid")
	}
	if err := tx.Commit(ctx); err != nil {
		return ArtifactUploadStatus{}, fmt.Errorf("commit ArtifactUpload status: %w", err)
	}
	return ArtifactUploadStatus{
		Decision:                 ArtifactUploadStatusFound,
		UploadID:                 row.UploadID,
		ArtifactID:               row.ArtifactID,
		State:                    ArtifactUploadState(row.State),
		ObjectKey:                row.ObjectKey,
		ExpectedContentType:      row.ExpectedContentType,
		MultipartUploadID:        stringValue(row.MultipartUploadID),
		CompletedParts:           completedParts,
		ObjectVersionID:          stringValue(row.ObjectVersionID),
		SizeBytes:                int64Value(row.SizeBytes),
		SHA256:                   digest,
		ContentType:              stringValue(row.ContentType),
		CompletionIntentRecorded: len(row.CompletionRequestHash) == sha256.Size,
		UploadExpiresAt:          row.ExpiresAt.Time,
		Version:                  row.Version,
	}, nil
}

func rejectedArtifactUploadStatus() ArtifactUploadStatus {
	return ArtifactUploadStatus{Decision: ArtifactUploadStatusRejectedStaleLease}
}

func int64Value(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func (s *Service) RecordArtifactMultipartSession(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
	uploadID uuid.UUID,
	claimID uuid.UUID,
	multipartUploadID string,
) (ArtifactMultipartSession, error) {
	if s == nil || s.pool == nil {
		return ArtifactMultipartSession{}, errors.New("worker coordinator is not configured")
	}
	if worker.ID == uuid.Nil || credentials.AttemptID == uuid.Nil || uploadID == uuid.Nil ||
		claimID == uuid.Nil || credentials.WorkerEpoch <= 0 || credentials.Fence <= 0 ||
		credentials.Token == "" || len(multipartUploadID) > 1000 ||
		strings.TrimSpace(multipartUploadID) == "" || strings.ContainsRune(multipartUploadID, '\x00') {
		return rejectedArtifactMultipartSession(), nil
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ArtifactMultipartSession{}, fmt.Errorf("begin multipart session transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)

	locked, authorized, err := lockFinalizationAuthority(
		ctx, queries, workerFinalizationActor(worker), credentials, "multipart session",
	)
	if err != nil {
		return ArtifactMultipartSession{}, err
	}
	if !authorized {
		return rejectedArtifactMultipartSession(), nil
	}
	claimOwnerID := locked.ownerID
	row, err := queries.RecordArtifactMultipartSession(
		ctx,
		store.RecordArtifactMultipartSessionParams{
			MultipartUploadID: &multipartUploadID,
			RecordedAt:        pgtype.Timestamptz{Time: locked.now, Valid: true},
			UploadID:          uploadID,
			AttemptID:         locked.authority.AttemptID,
			AttemptFence:      locked.authority.AttemptFence,
			ClaimID:           uuid.NullUUID{UUID: claimID, Valid: true},
			ClaimOwnerID:      &claimOwnerID,
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ArtifactMultipartSession{Decision: ArtifactMultipartSessionConflict}, nil
	}
	if err != nil {
		return ArtifactMultipartSession{}, fmt.Errorf("record multipart session: %w", err)
	}
	if row.MultipartUploadID == nil {
		return ArtifactMultipartSession{}, errors.New("recorded multipart session is null")
	}
	if err := tx.Commit(ctx); err != nil {
		return ArtifactMultipartSession{}, fmt.Errorf("commit multipart session: %w", err)
	}
	return ArtifactMultipartSession{
		Decision:          ArtifactMultipartSessionRecorded,
		UploadID:          row.UploadID,
		ArtifactID:        row.ArtifactID,
		MultipartUploadID: *row.MultipartUploadID,
		Version:           row.Version,
	}, nil
}

func rejectedArtifactMultipartSession() ArtifactMultipartSession {
	return ArtifactMultipartSession{Decision: ArtifactMultipartSessionRejected}
}

func (s *Service) RecordArtifactCompletionIntent(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
	uploadID uuid.UUID,
	report ArtifactUploadReport,
) (ArtifactCompletionIntentResult, error) {
	if s == nil || s.pool == nil {
		return ArtifactCompletionIntentResult{}, errors.New("worker coordinator is not configured")
	}
	normalized, completedParts, requestHash, normalizeErr := normalizeArtifactCompletionIntent(report)
	if normalizeErr != nil || normalized.ObjectVersionID != "" || worker.ID == uuid.Nil ||
		credentials.AttemptID == uuid.Nil || uploadID == uuid.Nil || credentials.WorkerEpoch <= 0 ||
		credentials.Fence <= 0 || credentials.Token == "" {
		return rejectedArtifactCompletionIntent(), nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ArtifactCompletionIntentResult{}, fmt.Errorf("begin Artifact completion intent transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	locked, authorized, err := lockFinalizationAuthority(
		ctx, queries, workerFinalizationActor(worker), credentials, "Artifact completion intent",
	)
	if err != nil {
		return ArtifactCompletionIntentResult{}, err
	}
	if !authorized {
		return rejectedArtifactCompletionIntent(), nil
	}
	contentType := normalized.ContentType
	row, err := queries.RecordArtifactCompletionIntent(
		ctx,
		store.RecordArtifactCompletionIntentParams{
			CompletedParts:        completedParts,
			SizeBytes:             &normalized.SizeBytes,
			Sha256:                normalized.SHA256[:],
			ContentType:           &contentType,
			CompletionRequestHash: requestHash[:],
			RecordedAt:            pgtype.Timestamptz{Time: locked.now, Valid: true},
			UploadID:              uploadID,
			AttemptID:             locked.authority.AttemptID,
			AttemptFence:          locked.authority.AttemptFence,
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ArtifactCompletionIntentResult{Decision: ArtifactCompletionIntentConflict}, nil
	}
	if err != nil {
		return ArtifactCompletionIntentResult{}, fmt.Errorf("record Artifact completion intent: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return ArtifactCompletionIntentResult{}, fmt.Errorf("commit Artifact completion intent: %w", err)
	}
	return ArtifactCompletionIntentResult{
		Decision:   ArtifactCompletionIntentRecorded,
		UploadID:   row.UploadID,
		ArtifactID: row.ArtifactID,
		Version:    row.Version,
	}, nil
}

func rejectedArtifactCompletionIntent() ArtifactCompletionIntentResult {
	return ArtifactCompletionIntentResult{Decision: ArtifactCompletionIntentRejected}
}

func (s *Service) RecordArtifactUploaded(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
	uploadID uuid.UUID,
	report ArtifactUploadReport,
) (ArtifactUploadResult, error) {
	return s.recordArtifactUploaded(
		ctx, workerFinalizationActor(worker), credentials, uploadID, report,
	)
}

func (s *Service) RecordArtifactUploadedAsReconciler(
	ctx context.Context,
	reconciler AuthenticatedReconciler,
	credentials ReconcilerFinalizationCredentials,
	uploadID uuid.UUID,
	report ArtifactUploadReport,
) (ArtifactUploadResult, error) {
	if !validPrintableFailureText(reconciler.ID, 500) {
		return rejectedArtifactUpload(), nil
	}
	return s.recordArtifactUploaded(
		ctx,
		finalizationActor{
			workerID:  credentials.WorkerID,
			ownerKind: store.LeaseOwnerKindRECONCILER,
			ownerID:   reconciler.ID,
		},
		LeaseCredentials{
			AttemptID:   credentials.AttemptID,
			WorkerEpoch: credentials.WorkerEpoch,
			Fence:       credentials.Fence,
			Token:       credentials.Token,
		},
		uploadID,
		report,
	)
}

func (s *Service) recordArtifactUploaded(
	ctx context.Context,
	actor finalizationActor,
	credentials LeaseCredentials,
	uploadID uuid.UUID,
	report ArtifactUploadReport,
) (ArtifactUploadResult, error) {
	if s == nil || s.pool == nil {
		return ArtifactUploadResult{}, errors.New("worker coordinator is not configured")
	}
	normalized, completedParts, requestHash, normalizeErr := normalizeArtifactUploadReport(report)
	if normalizeErr != nil || actor.workerID == uuid.Nil || credentials.AttemptID == uuid.Nil ||
		uploadID == uuid.Nil || credentials.WorkerEpoch <= 0 || credentials.Fence <= 0 ||
		credentials.Token == "" {
		return rejectedArtifactUpload(), nil
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ArtifactUploadResult{}, fmt.Errorf("begin uploaded Artifact transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)

	locked, authorized, err := lockFinalizationAuthority(
		ctx, queries, actor, credentials, "uploaded Artifact",
	)
	if err != nil {
		return ArtifactUploadResult{}, err
	}
	if !authorized {
		return rejectedArtifactUpload(), nil
	}
	objectVersionID := normalized.ObjectVersionID
	contentType := normalized.ContentType
	row, err := queries.RecordArtifactUploaded(ctx, store.RecordArtifactUploadedParams{
		CompletedParts:        completedParts,
		ObjectVersionID:       &objectVersionID,
		SizeBytes:             &normalized.SizeBytes,
		Sha256:                normalized.SHA256[:],
		ContentType:           &contentType,
		CompletionRequestHash: requestHash[:],
		UploadedAt:            pgtype.Timestamptz{Time: locked.now, Valid: true},
		UploadID:              uploadID,
		AttemptID:             locked.authority.AttemptID,
		AttemptFence:          locked.authority.AttemptFence,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return ArtifactUploadResult{Decision: ArtifactUploadConflict}, nil
	}
	if err != nil {
		return ArtifactUploadResult{}, fmt.Errorf("record uploaded Artifact: %w", err)
	}
	if row.ObjectVersionID == nil {
		return ArtifactUploadResult{}, errors.New("recorded uploaded Artifact version is null")
	}
	if err := tx.Commit(ctx); err != nil {
		return ArtifactUploadResult{}, fmt.Errorf("commit uploaded Artifact: %w", err)
	}
	return ArtifactUploadResult{
		Decision:        ArtifactUploadRecorded,
		UploadID:        row.UploadID,
		ArtifactID:      row.ArtifactID,
		ObjectVersionID: *row.ObjectVersionID,
		Version:         row.Version,
	}, nil
}

type normalizedArtifactUploadReport struct {
	ObjectVersionID string               `json:"-"`
	SizeBytes       int64                `json:"size_bytes"`
	SHA256          [sha256.Size]byte    `json:"-"`
	SHA256Hex       string               `json:"sha256"`
	ContentType     string               `json:"content_type"`
	CompletedParts  []ArtifactUploadPart `json:"completed_parts"`
}

func normalizeArtifactUploadReport(
	report ArtifactUploadReport,
) (normalizedArtifactUploadReport, []byte, [sha256.Size]byte, error) {
	normalized, completedParts, requestHash, err := normalizeArtifactCompletionIntent(report)
	if err != nil || normalized.ObjectVersionID == "" || len(normalized.ObjectVersionID) > 1000 ||
		strings.ContainsRune(normalized.ObjectVersionID, '\x00') {
		return normalizedArtifactUploadReport{}, nil, [sha256.Size]byte{}, errors.New("invalid ArtifactUploadReport")
	}
	return normalized, completedParts, requestHash, nil
}

func normalizeArtifactCompletionIntent(
	report ArtifactUploadReport,
) (normalizedArtifactUploadReport, []byte, [sha256.Size]byte, error) {
	normalized := normalizedArtifactUploadReport{
		ObjectVersionID: strings.TrimSpace(report.ObjectVersionID),
		SizeBytes:       report.SizeBytes,
		SHA256:          report.SHA256,
		SHA256Hex:       hex.EncodeToString(report.SHA256[:]),
		ContentType:     strings.TrimSpace(report.ContentType),
		CompletedParts:  append([]ArtifactUploadPart(nil), report.CompletedParts...),
	}
	if normalized.SizeBytes <= 0 || normalized.SHA256 == [sha256.Size]byte{} || len(normalized.ContentType) == 0 ||
		len(normalized.ContentType) > 200 || strings.ContainsRune(normalized.ContentType, '\x00') ||
		len(normalized.CompletedParts) == 0 || len(normalized.CompletedParts) > 10_000 {
		return normalizedArtifactUploadReport{}, nil, [sha256.Size]byte{}, errors.New("invalid ArtifactUploadReport")
	}
	var totalSize int64
	for index, part := range normalized.CompletedParts {
		if part.Number != int32(index+1) || part.SizeBytes <= 0 ||
			len(part.ETag) == 0 || len(part.ETag) > 1000 || strings.ContainsRune(part.ETag, '\x00') ||
			totalSize > normalized.SizeBytes-part.SizeBytes {
			return normalizedArtifactUploadReport{}, nil, [sha256.Size]byte{}, errors.New("invalid ArtifactUpload parts")
		}
		if part.ChecksumSHA256 != "" {
			checksum, err := base64.StdEncoding.DecodeString(part.ChecksumSHA256)
			if err != nil || len(checksum) != sha256.Size {
				return normalizedArtifactUploadReport{}, nil, [sha256.Size]byte{}, errors.New("invalid ArtifactUpload part checksum")
			}
			var checksumDigest [sha256.Size]byte
			copy(checksumDigest[:], checksum)
			if checksumDigest == [sha256.Size]byte{} {
				return normalizedArtifactUploadReport{}, nil, [sha256.Size]byte{}, errors.New("invalid ArtifactUpload part checksum")
			}
		}
		totalSize += part.SizeBytes
	}
	if totalSize != normalized.SizeBytes {
		return normalizedArtifactUploadReport{}, nil, [sha256.Size]byte{}, errors.New("ArtifactUpload part sizes do not match object size")
	}
	completedParts, err := json.Marshal(normalized.CompletedParts)
	if err != nil {
		return normalizedArtifactUploadReport{}, nil, [sha256.Size]byte{}, fmt.Errorf("marshal ArtifactUpload parts: %w", err)
	}
	canonical, err := json.Marshal(normalized)
	if err != nil {
		return normalizedArtifactUploadReport{}, nil, [sha256.Size]byte{}, fmt.Errorf("marshal ArtifactUpload report: %w", err)
	}
	return normalized, completedParts, sha256.Sum256(canonical), nil
}

func rejectedArtifactUpload() ArtifactUploadResult {
	return ArtifactUploadResult{Decision: ArtifactUploadRejected}
}

type claimedArtifactVerification struct {
	request ArtifactInspectionRequest
}

func (s *Service) VerifyArtifact(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
	uploadID uuid.UUID,
	verificationID uuid.UUID,
) (ArtifactVerificationResult, error) {
	return s.verifyArtifact(
		ctx,
		finalizationActor{
			workerID:            worker.ID,
			ownerKind:           store.LeaseOwnerKindWORKER,
			requireCurrentEpoch: true,
		},
		credentials,
		uploadID,
		verificationID,
	)
}

func (s *Service) VerifyArtifactAsReconciler(
	ctx context.Context,
	reconciler AuthenticatedReconciler,
	credentials ReconcilerFinalizationCredentials,
	uploadID uuid.UUID,
	verificationID uuid.UUID,
) (ArtifactVerificationResult, error) {
	if !validPrintableFailureText(reconciler.ID, 500) {
		return rejectedArtifactVerification(), nil
	}
	return s.verifyArtifact(
		ctx,
		finalizationActor{
			workerID:  credentials.WorkerID,
			ownerKind: store.LeaseOwnerKindRECONCILER,
			ownerID:   reconciler.ID,
		},
		LeaseCredentials{
			AttemptID:   credentials.AttemptID,
			WorkerEpoch: credentials.WorkerEpoch,
			Fence:       credentials.Fence,
			Token:       credentials.Token,
		},
		uploadID,
		verificationID,
	)
}

func (s *Service) verifyArtifact(
	ctx context.Context,
	actor finalizationActor,
	credentials LeaseCredentials,
	uploadID uuid.UUID,
	verificationID uuid.UUID,
) (ArtifactVerificationResult, error) {
	if s == nil || s.pool == nil {
		return ArtifactVerificationResult{}, errors.New("worker coordinator is not configured")
	}
	if s.artifactInspector == nil {
		return ArtifactVerificationResult{}, errors.New("artifact inspector is not configured")
	}
	if actor.workerID == uuid.Nil || credentials.AttemptID == uuid.Nil || uploadID == uuid.Nil ||
		verificationID == uuid.Nil || credentials.WorkerEpoch <= 0 || credentials.Fence <= 0 ||
		credentials.Token == "" {
		return rejectedArtifactVerification(), nil
	}

	claim, terminal, err := s.claimArtifactVerification(
		ctx,
		actor,
		credentials,
		uploadID,
		verificationID,
	)
	if err != nil || terminal.Decision != "" {
		return terminal, err
	}
	inspection, err := s.artifactInspector.Inspect(ctx, claim.request)
	if err != nil {
		if releaseErr := s.releaseArtifactVerificationClaim(
			ctx,
			actor,
			credentials,
			claim.request.UploadID,
			verificationID,
		); releaseErr != nil {
			return ArtifactVerificationResult{}, errors.Join(
				fmt.Errorf("inspect exact Artifact object version: %w", err),
				releaseErr,
			)
		}
		return ArtifactVerificationResult{}, fmt.Errorf("inspect exact Artifact object version: %w", err)
	}
	receipt, requestHash, valid := validateArtifactInspection(claim.request, inspection)
	if !valid {
		if err := s.releaseArtifactVerificationClaim(
			ctx,
			actor,
			credentials,
			claim.request.UploadID,
			verificationID,
		); err != nil {
			return ArtifactVerificationResult{}, err
		}
		return ArtifactVerificationResult{
			Decision:        ArtifactValidationFailed,
			VerificationID:  verificationID,
			UploadID:        claim.request.UploadID,
			ArtifactID:      claim.request.ArtifactID,
			ObjectVersionID: claim.request.ObjectVersionID,
		}, nil
	}
	return s.recordArtifactVerification(
		ctx,
		actor,
		credentials,
		verificationID,
		claim.request,
		receipt,
		requestHash,
	)
}

func (s *Service) claimArtifactVerification(
	ctx context.Context,
	actor finalizationActor,
	credentials LeaseCredentials,
	uploadID uuid.UUID,
	verificationID uuid.UUID,
) (claimedArtifactVerification, ArtifactVerificationResult, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return claimedArtifactVerification{}, ArtifactVerificationResult{},
			fmt.Errorf("begin Artifact verification claim transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	locked, authorized, err := lockFinalizationAuthority(
		ctx,
		queries,
		actor,
		credentials,
		"Artifact verification",
	)
	if err != nil {
		return claimedArtifactVerification{}, ArtifactVerificationResult{}, err
	}
	if !authorized {
		return claimedArtifactVerification{}, rejectedArtifactVerification(), nil
	}
	ownerID, authority, now := locked.ownerID, locked.authority, locked.now

	recorded, err := queries.GetRecordedArtifactVerification(
		ctx,
		store.GetRecordedArtifactVerificationParams{
			UploadID:     uploadID,
			AttemptID:    authority.AttemptID,
			AttemptFence: authority.AttemptFence,
		},
	)
	if err == nil {
		if !recorded.VerificationID.Valid || recorded.ObjectVersionID == nil || !recorded.VerifiedAt.Valid {
			return claimedArtifactVerification{}, ArtifactVerificationResult{},
				errors.New("recorded Artifact verification is incomplete")
		}
		if err := tx.Commit(ctx); err != nil {
			return claimedArtifactVerification{}, ArtifactVerificationResult{},
				fmt.Errorf("commit Artifact verification replay: %w", err)
		}
		return claimedArtifactVerification{}, ArtifactVerificationResult{
			Decision:        ArtifactVerified,
			VerificationID:  recorded.VerificationID.UUID,
			UploadID:        recorded.UploadID,
			ArtifactID:      recorded.ArtifactID,
			ObjectVersionID: *recorded.ObjectVersionID,
			Version:         recorded.Version,
			VerifiedAt:      recorded.VerifiedAt.Time,
		}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return claimedArtifactVerification{}, ArtifactVerificationResult{},
			fmt.Errorf("read recorded Artifact verification: %w", err)
	}

	claimExpiresAt := artifactClaimExpiry(now, authority)
	owner := ownerID
	ownerKind := actor.ownerKind
	row, err := queries.ClaimArtifactVerification(ctx, store.ClaimArtifactVerificationParams{
		VerificationID: uuid.NullUUID{UUID: verificationID, Valid: true},
		ClaimOwnerKind: &ownerKind,
		ClaimOwnerID:   &owner,
		ClaimExpiresAt: pgtype.Timestamptz{Time: claimExpiresAt, Valid: true},
		ClaimedAt:      pgtype.Timestamptz{Time: now, Valid: true},
		UploadID:       uploadID,
		AttemptID:      authority.AttemptID,
		AttemptFence:   authority.AttemptFence,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return claimedArtifactVerification{}, ArtifactVerificationResult{
			Decision: ArtifactVerificationBusy,
		}, nil
	}
	if err != nil {
		return claimedArtifactVerification{}, ArtifactVerificationResult{},
			fmt.Errorf("claim Artifact verification: %w", err)
	}
	request, err := artifactInspectionRequest(row)
	if err != nil {
		return claimedArtifactVerification{}, ArtifactVerificationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return claimedArtifactVerification{}, ArtifactVerificationResult{},
			fmt.Errorf("commit Artifact verification claim: %w", err)
	}
	return claimedArtifactVerification{request: request}, ArtifactVerificationResult{}, nil
}

func artifactInspectionRequest(
	row store.ClaimArtifactVerificationRow,
) (ArtifactInspectionRequest, error) {
	if row.ObjectVersionID == nil || row.SizeBytes == nil || row.ContentType == nil ||
		len(row.Sha256) != sha256.Size || !row.VerificationID.Valid || !row.ClaimExpiresAt.Valid {
		return ArtifactInspectionRequest{}, errors.New("claimed Artifact verification identity is incomplete")
	}
	var digest [sha256.Size]byte
	copy(digest[:], row.Sha256)
	request := ArtifactInspectionRequest{
		ArtifactID:          row.ArtifactID,
		UploadID:            row.UploadID,
		Kind:                ArtifactKind(row.Kind),
		Ordinal:             row.Ordinal,
		ObjectKey:           row.ObjectKey,
		ObjectVersionID:     *row.ObjectVersionID,
		ExpectedSizeBytes:   *row.SizeBytes,
		ExpectedSHA256:      digest,
		ExpectedContentType: *row.ContentType,
	}
	return applyArtifactInspectionExpectations(request, artifactInspectionExpectations{
		width: row.ExpectedWidth, height: row.ExpectedHeight,
		durationMilliseconds: row.ExpectedDurationMilliseconds,
		frameRateMilli:       row.ExpectedFrameRateMilli,
		codec:                row.ExpectedCodec, container: row.ExpectedContainer,
	})
}

type artifactInspectionExpectations struct {
	width                int32
	height               int32
	durationMilliseconds int32
	frameRateMilli       int32
	codec                string
	container            string
}

func applyArtifactInspectionExpectations(
	request ArtifactInspectionRequest,
	expected artifactInspectionExpectations,
) (ArtifactInspectionRequest, error) {
	switch request.Kind {
	case ArtifactKindVideo:
		frameProduct := int64(expected.durationMilliseconds) * int64(expected.frameRateMilli)
		if frameProduct%1_000_000 != 0 || frameProduct/1_000_000 > math.MaxInt32 {
			return ArtifactInspectionRequest{}, errors.New("OutputSpec frame count is not integral")
		}
		request.ExpectedWidth = expected.width
		request.ExpectedHeight = expected.height
		request.ExpectedDurationMillis = expected.durationMilliseconds
		request.ExpectedFrameRateMilli = expected.frameRateMilli
		request.ExpectedFrameCount = int32(frameProduct / 1_000_000)
		request.ExpectedCodec = expected.codec
		request.ExpectedContainer = expected.container
	case ArtifactKindThumbnail:
		request.ExpectedWidth = 320
		request.ExpectedHeight = 180
		request.ExpectedFrameCount = 1
		request.ExpectedCodec = "webp"
		request.ExpectedContainer = "webp"
	default:
		return ArtifactInspectionRequest{}, errors.New("unsupported Artifact kind")
	}
	return request, nil
}

func (s *Service) releaseArtifactVerificationClaim(
	ctx context.Context,
	actor finalizationActor,
	credentials LeaseCredentials,
	uploadID uuid.UUID,
	verificationID uuid.UUID,
) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin Artifact verification claim release transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	locked, authorized, err := lockFinalizationAuthority(
		ctx,
		queries,
		actor,
		credentials,
		"Artifact verification claim release",
	)
	if err != nil {
		return err
	}
	if !authorized {
		return nil
	}
	ownerID, authority, now := locked.ownerID, locked.authority, locked.now
	owner := ownerID
	ownerKind := actor.ownerKind
	rows, err := queries.ReleaseArtifactVerificationClaim(
		ctx,
		store.ReleaseArtifactVerificationClaimParams{
			ReleasedAt:     pgtype.Timestamptz{Time: now, Valid: true},
			UploadID:       uploadID,
			AttemptID:      authority.AttemptID,
			AttemptFence:   authority.AttemptFence,
			VerificationID: uuid.NullUUID{UUID: verificationID, Valid: true},
			ClaimOwnerKind: &ownerKind,
			ClaimOwnerID:   &owner,
		},
	)
	if err != nil {
		return fmt.Errorf("release Artifact verification claim: %w", err)
	}
	if rows == 0 {
		return nil
	}
	if rows != 1 {
		return fmt.Errorf("release Artifact verification claim changed %d rows", rows)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit Artifact verification claim release: %w", err)
	}
	return nil
}

type artifactValidationReceipt struct {
	ArtifactID        string `json:"artifact_id"`
	ObjectKey         string `json:"object_key"`
	ObjectVersionID   string `json:"object_version_id"`
	SizeBytes         int64  `json:"size_bytes"`
	SHA256            string `json:"sha256"`
	ContentType       string `json:"content_type"`
	Width             int32  `json:"width"`
	Height            int32  `json:"height"`
	DurationMillis    int32  `json:"duration_milliseconds"`
	FrameRateMilli    int32  `json:"frame_rate_milli"`
	FrameCount        int32  `json:"frame_count"`
	Codec             string `json:"codec"`
	Container         string `json:"container"`
	ValidatorRevision string `json:"validator_revision"`
}

func validateArtifactInspection(
	request ArtifactInspectionRequest,
	inspection ArtifactInspection,
) ([]byte, [sha256.Size]byte, bool) {
	if inspection.ObjectVersionID != request.ObjectVersionID ||
		inspection.SizeBytes != request.ExpectedSizeBytes ||
		inspection.SHA256 != request.ExpectedSHA256 ||
		inspection.ContentType != request.ExpectedContentType ||
		inspection.Width != request.ExpectedWidth ||
		inspection.Height != request.ExpectedHeight ||
		inspection.DurationMillis != request.ExpectedDurationMillis ||
		inspection.FrameRateMilli != request.ExpectedFrameRateMilli ||
		inspection.FrameCount != request.ExpectedFrameCount ||
		inspection.Codec != request.ExpectedCodec ||
		inspection.Container != request.ExpectedContainer ||
		len(inspection.ValidatorRevision) == 0 || len(inspection.ValidatorRevision) > 200 ||
		strings.ContainsRune(inspection.ValidatorRevision, '\x00') {
		return nil, [sha256.Size]byte{}, false
	}
	receipt, err := json.Marshal(artifactValidationReceipt{
		ArtifactID:        request.ArtifactID.String(),
		ObjectKey:         request.ObjectKey,
		ObjectVersionID:   inspection.ObjectVersionID,
		SizeBytes:         inspection.SizeBytes,
		SHA256:            hex.EncodeToString(inspection.SHA256[:]),
		ContentType:       inspection.ContentType,
		Width:             inspection.Width,
		Height:            inspection.Height,
		DurationMillis:    inspection.DurationMillis,
		FrameRateMilli:    inspection.FrameRateMilli,
		FrameCount:        inspection.FrameCount,
		Codec:             inspection.Codec,
		Container:         inspection.Container,
		ValidatorRevision: inspection.ValidatorRevision,
	})
	if err != nil {
		return nil, [sha256.Size]byte{}, false
	}
	return receipt, sha256.Sum256(receipt), true
}

func (s *Service) recordArtifactVerification(
	ctx context.Context,
	actor finalizationActor,
	credentials LeaseCredentials,
	verificationID uuid.UUID,
	request ArtifactInspectionRequest,
	receipt []byte,
	requestHash [sha256.Size]byte,
) (ArtifactVerificationResult, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ArtifactVerificationResult{}, fmt.Errorf("begin Artifact verification record transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	locked, authorized, err := lockFinalizationAuthority(
		ctx,
		queries,
		actor,
		credentials,
		"Artifact verification record",
	)
	if err != nil {
		return ArtifactVerificationResult{}, err
	}
	if !authorized {
		return rejectedArtifactVerification(), nil
	}
	ownerID, authority, now := locked.ownerID, locked.authority, locked.now
	objectVersionID := request.ObjectVersionID
	contentType := request.ExpectedContentType
	owner := ownerID
	ownerKind := actor.ownerKind
	row, err := queries.RecordArtifactVerified(ctx, store.RecordArtifactVerifiedParams{
		VerificationRequestHash: requestHash[:],
		ValidationReceipt:       receipt,
		VerificationID:          uuid.NullUUID{UUID: verificationID, Valid: true},
		VerifiedAt:              pgtype.Timestamptz{Time: now, Valid: true},
		UploadID:                request.UploadID,
		AttemptID:               authority.AttemptID,
		AttemptFence:            authority.AttemptFence,
		ClaimOwnerKind:          &ownerKind,
		ClaimOwnerID:            &owner,
		ObjectVersionID:         &objectVersionID,
		SizeBytes:               &request.ExpectedSizeBytes,
		Sha256:                  request.ExpectedSHA256[:],
		ContentType:             &contentType,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedArtifactVerification(), nil
	}
	if err != nil {
		return ArtifactVerificationResult{}, fmt.Errorf("record verified Artifact: %w", err)
	}
	if !row.VerificationID.Valid || row.ObjectVersionID == nil || !row.VerifiedAt.Valid {
		return ArtifactVerificationResult{}, errors.New("verified Artifact result is incomplete")
	}
	if err := tx.Commit(ctx); err != nil {
		return ArtifactVerificationResult{}, fmt.Errorf("commit verified Artifact: %w", err)
	}
	return ArtifactVerificationResult{
		Decision:        ArtifactVerified,
		VerificationID:  row.VerificationID.UUID,
		UploadID:        row.UploadID,
		ArtifactID:      row.ArtifactID,
		ObjectVersionID: *row.ObjectVersionID,
		Version:         row.Version,
		VerifiedAt:      row.VerifiedAt.Time,
	}, nil
}

func rejectedArtifactVerification() ArtifactVerificationResult {
	return ArtifactVerificationResult{Decision: ArtifactVerificationRejected}
}
