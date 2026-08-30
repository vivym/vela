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
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/vivym/vela/internal/debugdumpcontract"
	store "github.com/vivym/vela/internal/store/sqlc"
)

const debugDumpClaimTTL = 30 * time.Second

type DebugDumpUploadIntent struct {
	DebugDumpID     uuid.UUID
	AuthorizationID uuid.UUID
	SizeBytes       int64
	SHA256          [sha256.Size]byte
	ContentType     string
}

type DebugDumpUploadClaimDecision string

const (
	DebugDumpUploadClaimGranted  DebugDumpUploadClaimDecision = "DEBUG_DUMP_UPLOAD_CLAIM_GRANTED"
	DebugDumpUploadClaimConflict DebugDumpUploadClaimDecision = "DEBUG_DUMP_UPLOAD_CONFLICT"
	DebugDumpUploadClaimRejected DebugDumpUploadClaimDecision = "REJECTED_STALE_LEASE"
)

type DebugDumpUploadPart struct {
	Number         int32  `json:"number"`
	ETag           string `json:"etag"`
	SizeBytes      int64  `json:"size_bytes"`
	ChecksumSHA256 string `json:"checksum_sha256"`
}

type DebugDumpUploadClaim struct {
	Decision            DebugDumpUploadClaimDecision
	ClaimID             uuid.UUID
	DebugDumpID         uuid.UUID
	AuthorizationID     uuid.UUID
	ObjectKey           string
	ExpectedSizeBytes   int64
	ExpectedSHA256      [sha256.Size]byte
	ExpectedContentType string
	MultipartUploadID   string
	CompletedParts      []DebugDumpUploadPart
	ClaimExpiresAt      time.Time
	UploadExpiresAt     time.Time
	Version             int64
}

type DebugDumpMultipartSessionDecision string

const (
	DebugDumpMultipartSessionRecorded DebugDumpMultipartSessionDecision = "DEBUG_DUMP_MULTIPART_SESSION_RECORDED"
	DebugDumpMultipartSessionConflict DebugDumpMultipartSessionDecision = "DEBUG_DUMP_MULTIPART_SESSION_CONFLICT"
	DebugDumpMultipartSessionRejected DebugDumpMultipartSessionDecision = "REJECTED_STALE_LEASE"
)

type DebugDumpMultipartSession struct {
	Decision          DebugDumpMultipartSessionDecision
	DebugDumpID       uuid.UUID
	AuthorizationID   uuid.UUID
	MultipartUploadID string
	Version           int64
}

type DebugDumpUploadReport struct {
	ObjectVersionID string
	SizeBytes       int64
	SHA256          [sha256.Size]byte
	ContentType     string
	CompletedParts  []DebugDumpUploadPart
}

type DebugDumpCompletionIntentDecision string

const (
	DebugDumpCompletionIntentRecorded DebugDumpCompletionIntentDecision = "DEBUG_DUMP_COMPLETION_INTENT_RECORDED"
	DebugDumpCompletionIntentConflict DebugDumpCompletionIntentDecision = "DEBUG_DUMP_UPLOAD_CONFLICT"
	DebugDumpCompletionIntentRejected DebugDumpCompletionIntentDecision = "REJECTED_STALE_LEASE"
)

type DebugDumpCompletionIntentResult struct {
	Decision        DebugDumpCompletionIntentDecision
	DebugDumpID     uuid.UUID
	AuthorizationID uuid.UUID
	Version         int64
}

type DebugDumpUploadDecision string

const (
	DebugDumpUploadRecorded DebugDumpUploadDecision = "DEBUG_DUMP_UPLOAD_RECORDED"
	DebugDumpUploadConflict DebugDumpUploadDecision = "DEBUG_DUMP_UPLOAD_CONFLICT"
	DebugDumpUploadRejected DebugDumpUploadDecision = "REJECTED_STALE_LEASE"
)

type DebugDumpUploadResult struct {
	Decision        DebugDumpUploadDecision
	DebugDumpID     uuid.UUID
	AuthorizationID uuid.UUID
	ObjectVersionID string
	Version         int64
}

type DebugDumpUploadState string

const (
	DebugDumpUploadStateUploading DebugDumpUploadState = "UPLOADING"
	DebugDumpUploadStateAvailable DebugDumpUploadState = "AVAILABLE"
	DebugDumpUploadStateDeleted   DebugDumpUploadState = "DELETED"
)

type DebugDumpUploadStatus struct {
	Decision                 DebugDumpUploadDecision
	DebugDumpID              uuid.UUID
	AuthorizationID          uuid.UUID
	State                    DebugDumpUploadState
	ObjectKey                string
	ExpectedSizeBytes        int64
	ExpectedSHA256           [sha256.Size]byte
	ExpectedContentType      string
	MultipartUploadID        string
	CompletedParts           []DebugDumpUploadPart
	ObjectVersionID          string
	SizeBytes                int64
	SHA256                   [sha256.Size]byte
	ContentType              string
	CompletionIntentRecorded bool
	UploadExpiresAt          time.Time
	Version                  int64
}

type lockedDebugDumpAuthority struct {
	row store.LockDebugDumpUploadAuthorityRow
	now time.Time
}

func (s *Service) ClaimDebugDumpUpload(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
	intent DebugDumpUploadIntent,
	claimID uuid.UUID,
) (DebugDumpUploadClaim, error) {
	if s == nil || s.pool == nil {
		return DebugDumpUploadClaim{}, errors.New("worker coordinator is not configured")
	}
	if !validDebugDumpIntent(intent) || claimID == uuid.Nil {
		return rejectedDebugDumpUploadClaim(), nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return DebugDumpUploadClaim{}, fmt.Errorf("begin debug dump upload claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	locked, authorized, err := lockDebugDumpAuthority(ctx, queries, worker, credentials)
	if err != nil {
		return DebugDumpUploadClaim{}, err
	}
	if !authorized || locked.row.DebugDumpAuthorizationID.UUID != intent.AuthorizationID {
		return rejectedDebugDumpUploadClaim(), nil
	}
	objectKey := fmt.Sprintf(
		"debug-dumps/%s/%s/%s/%s/%s/%s",
		locked.row.OrganizationID, locked.row.ProjectID, locked.row.JobID,
		intent.AuthorizationID, locked.row.AttemptID, intent.DebugDumpID,
	)
	expiresAt := locked.row.DebugDumpAuthorizationExpiresAt.Time
	if _, err := queries.InsertDebugDumpUpload(ctx, store.InsertDebugDumpUploadParams{
		DebugDumpID: intent.DebugDumpID, OrganizationID: locked.row.OrganizationID,
		ProjectID: locked.row.ProjectID, JobID: locked.row.JobID,
		AuthorizationID: intent.AuthorizationID, AttemptID: locked.row.AttemptID,
		AttemptFence: locked.row.AttemptFence, WorkerID: worker.ID,
		WorkerEpoch: credentials.WorkerEpoch, ObjectKey: objectKey,
		ExpectedSizeBytes: intent.SizeBytes, ExpectedSha256: intent.SHA256[:],
		ExpectedContentType: intent.ContentType,
		ExpiresAt:           pgtype.Timestamptz{Time: expiresAt, Valid: true},
		CreatedAt:           pgtype.Timestamptz{Time: locked.now, Valid: true},
	}); err != nil {
		return DebugDumpUploadClaim{}, fmt.Errorf("insert debug dump upload: %w", err)
	}
	claimExpiresAt := locked.now.Add(debugDumpClaimTTL)
	if claimExpiresAt.After(locked.row.LeaseExpiresAt.Time) {
		claimExpiresAt = locked.row.LeaseExpiresAt.Time
	}
	if claimExpiresAt.After(expiresAt) {
		claimExpiresAt = expiresAt
	}
	row, err := queries.ClaimDebugDumpUpload(ctx, store.ClaimDebugDumpUploadParams{
		ClaimID:        uuid.NullUUID{UUID: claimID, Valid: true},
		ClaimExpiresAt: pgtype.Timestamptz{Time: claimExpiresAt, Valid: true},
		ClaimedAt:      pgtype.Timestamptz{Time: locked.now, Valid: true},
		DebugDumpID:    intent.DebugDumpID, OrganizationID: locked.row.OrganizationID,
		ProjectID: locked.row.ProjectID, JobID: locked.row.JobID,
		AuthorizationID: intent.AuthorizationID, AttemptID: locked.row.AttemptID,
		AttemptFence: locked.row.AttemptFence, WorkerID: worker.ID,
		WorkerEpoch: credentials.WorkerEpoch, ObjectKey: objectKey,
		ExpectedSizeBytes: intent.SizeBytes, ExpectedSha256: intent.SHA256[:],
		ExpectedContentType: intent.ContentType,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return DebugDumpUploadClaim{Decision: DebugDumpUploadClaimConflict}, nil
	}
	if err != nil {
		return DebugDumpUploadClaim{}, fmt.Errorf("claim debug dump upload: %w", err)
	}
	if err := queries.InsertDebugDumpClaimedEvent(ctx, store.InsertDebugDumpClaimedEventParams{
		CreatedAt:   pgtype.Timestamptz{Time: locked.now, Valid: true},
		DebugDumpID: intent.DebugDumpID,
	}); err != nil {
		return DebugDumpUploadClaim{}, fmt.Errorf("record debug dump upload claim: %w", err)
	}
	parts, err := decodeDebugDumpParts(row.CompletedParts)
	if err != nil {
		return DebugDumpUploadClaim{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return DebugDumpUploadClaim{}, fmt.Errorf("commit debug dump upload claim: %w", err)
	}
	return DebugDumpUploadClaim{
		Decision: DebugDumpUploadClaimGranted, ClaimID: claimID,
		DebugDumpID: row.DebugDumpID, AuthorizationID: row.AuthorizationID,
		ObjectKey: row.ObjectKey, ExpectedSizeBytes: row.ExpectedSizeBytes,
		ExpectedSHA256: intent.SHA256, ExpectedContentType: row.ExpectedContentType,
		MultipartUploadID: stringValue(row.MultipartUploadID), CompletedParts: parts,
		ClaimExpiresAt:  claimExpiresAt,
		UploadExpiresAt: minTime(locked.row.LeaseExpiresAt.Time, expiresAt),
		Version:         row.Version,
	}, nil
}

func (s *Service) RecordDebugDumpMultipartSession(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
	debugDumpID, claimID uuid.UUID,
	multipartUploadID string,
) (DebugDumpMultipartSession, error) {
	if s == nil || s.pool == nil {
		return DebugDumpMultipartSession{}, errors.New("worker coordinator is not configured")
	}
	if debugDumpID == uuid.Nil || claimID == uuid.Nil ||
		!validMultipartUploadID(multipartUploadID) {
		return rejectedDebugDumpMultipartSession(), nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return DebugDumpMultipartSession{}, fmt.Errorf("begin debug dump multipart session: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	locked, authorized, err := lockDebugDumpAuthority(ctx, queries, worker, credentials)
	if err != nil {
		return DebugDumpMultipartSession{}, err
	}
	if !authorized {
		return rejectedDebugDumpMultipartSession(), nil
	}
	multipartID := multipartUploadID
	row, err := queries.RecordDebugDumpMultipartSession(
		ctx,
		store.RecordDebugDumpMultipartSessionParams{
			MultipartUploadID: &multipartID,
			RecordedAt:        pgtype.Timestamptz{Time: locked.now, Valid: true},
			DebugDumpID:       debugDumpID, AttemptID: locked.row.AttemptID,
			AttemptFence: locked.row.AttemptFence,
			ClaimID:      uuid.NullUUID{UUID: claimID, Valid: true},
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return DebugDumpMultipartSession{Decision: DebugDumpMultipartSessionConflict}, nil
	}
	if err != nil {
		return DebugDumpMultipartSession{}, fmt.Errorf("record debug dump multipart session: %w", err)
	}
	if row.MultipartUploadID == nil {
		return DebugDumpMultipartSession{}, errors.New("recorded debug dump multipart session is null")
	}
	if err := tx.Commit(ctx); err != nil {
		return DebugDumpMultipartSession{}, fmt.Errorf("commit debug dump multipart session: %w", err)
	}
	return DebugDumpMultipartSession{
		Decision: DebugDumpMultipartSessionRecorded, DebugDumpID: row.DebugDumpID,
		AuthorizationID: row.AuthorizationID, MultipartUploadID: *row.MultipartUploadID,
		Version: row.Version,
	}, nil
}

func (s *Service) InspectDebugDumpUpload(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
	debugDumpID uuid.UUID,
) (DebugDumpUploadStatus, error) {
	if s == nil || s.pool == nil {
		return DebugDumpUploadStatus{}, errors.New("worker coordinator is not configured")
	}
	if debugDumpID == uuid.Nil {
		return rejectedDebugDumpUploadStatus(), nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return DebugDumpUploadStatus{}, fmt.Errorf("begin debug dump upload inspection: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	locked, authorized, err := lockDebugDumpAuthority(ctx, queries, worker, credentials)
	if err != nil {
		return DebugDumpUploadStatus{}, err
	}
	if !authorized {
		return rejectedDebugDumpUploadStatus(), nil
	}
	row, err := queries.GetDebugDumpUploadStatus(ctx, store.GetDebugDumpUploadStatusParams{
		DebugDumpID: debugDumpID, AttemptID: locked.row.AttemptID,
		AttemptFence: locked.row.AttemptFence,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedDebugDumpUploadStatus(), nil
	}
	if err != nil {
		return DebugDumpUploadStatus{}, fmt.Errorf("inspect debug dump upload: %w", err)
	}
	parts, err := decodeDebugDumpParts(row.CompletedParts)
	if err != nil {
		return DebugDumpUploadStatus{}, err
	}
	status, err := debugDumpStatusFromRow(row, parts, minTime(
		locked.row.LeaseExpiresAt.Time, locked.row.DebugDumpAuthorizationExpiresAt.Time,
	))
	if err != nil {
		return DebugDumpUploadStatus{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return DebugDumpUploadStatus{}, fmt.Errorf("commit debug dump upload inspection: %w", err)
	}
	return status, nil
}

func (s *Service) RecordDebugDumpCompletionIntent(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
	debugDumpID uuid.UUID,
	report DebugDumpUploadReport,
) (DebugDumpCompletionIntentResult, error) {
	normalized, completedParts, requestHash, err := normalizeDebugDumpReport(report, false)
	if err != nil || debugDumpID == uuid.Nil {
		return DebugDumpCompletionIntentResult{Decision: DebugDumpCompletionIntentRejected}, nil
	}
	if s == nil || s.pool == nil {
		return DebugDumpCompletionIntentResult{}, errors.New("worker coordinator is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return DebugDumpCompletionIntentResult{}, fmt.Errorf("begin debug dump completion intent: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	locked, authorized, err := lockDebugDumpAuthority(ctx, queries, worker, credentials)
	if err != nil {
		return DebugDumpCompletionIntentResult{}, err
	}
	if !authorized {
		return DebugDumpCompletionIntentResult{Decision: DebugDumpCompletionIntentRejected}, nil
	}
	row, err := queries.RecordDebugDumpCompletionIntent(
		ctx,
		store.RecordDebugDumpCompletionIntentParams{
			CompletedParts: completedParts, CompletionRequestHash: requestHash[:],
			RecordedAt:  pgtype.Timestamptz{Time: locked.now, Valid: true},
			DebugDumpID: debugDumpID, AttemptID: locked.row.AttemptID,
			AttemptFence: locked.row.AttemptFence, SizeBytes: normalized.SizeBytes,
			Sha256: normalized.SHA256[:], ContentType: normalized.ContentType,
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return DebugDumpCompletionIntentResult{Decision: DebugDumpCompletionIntentConflict}, nil
	}
	if err != nil {
		return DebugDumpCompletionIntentResult{}, fmt.Errorf("record debug dump completion intent: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return DebugDumpCompletionIntentResult{}, fmt.Errorf("commit debug dump completion intent: %w", err)
	}
	return DebugDumpCompletionIntentResult{
		Decision: DebugDumpCompletionIntentRecorded, DebugDumpID: row.DebugDumpID,
		AuthorizationID: row.AuthorizationID, Version: row.Version,
	}, nil
}

func (s *Service) RecordDebugDumpUploaded(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
	debugDumpID uuid.UUID,
	report DebugDumpUploadReport,
) (DebugDumpUploadResult, error) {
	normalized, completedParts, requestHash, err := normalizeDebugDumpReport(report, true)
	if err != nil || debugDumpID == uuid.Nil {
		return rejectedDebugDumpUpload(), nil
	}
	if s == nil || s.pool == nil {
		return DebugDumpUploadResult{}, errors.New("worker coordinator is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return DebugDumpUploadResult{}, fmt.Errorf("begin debug dump upload receipt: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	locked, authorized, err := lockDebugDumpAuthority(ctx, queries, worker, credentials)
	if err != nil {
		return DebugDumpUploadResult{}, err
	}
	if !authorized {
		return rejectedDebugDumpUpload(), nil
	}
	row, err := queries.RecordDebugDumpUploaded(ctx, store.RecordDebugDumpUploadedParams{
		ObjectVersionID: normalized.ObjectVersionID, SizeBytes: normalized.SizeBytes,
		Sha256: normalized.SHA256[:], ContentType: normalized.ContentType,
		CompletedParts:        completedParts,
		CompletionRequestHash: requestHash[:],
		UploadedAt:            pgtype.Timestamptz{Time: locked.now, Valid: true},
		DebugDumpID:           debugDumpID, AttemptID: locked.row.AttemptID,
		AttemptFence: locked.row.AttemptFence,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return DebugDumpUploadResult{Decision: DebugDumpUploadConflict}, nil
	}
	if err != nil {
		return DebugDumpUploadResult{}, fmt.Errorf("record debug dump upload receipt: %w", err)
	}
	if row.ObjectVersionID == nil {
		return DebugDumpUploadResult{}, errors.New("debug dump upload receipt has no object version")
	}
	if err := tx.Commit(ctx); err != nil {
		return DebugDumpUploadResult{}, fmt.Errorf("commit debug dump upload receipt: %w", err)
	}
	return DebugDumpUploadResult{
		Decision: DebugDumpUploadRecorded, DebugDumpID: row.DebugDumpID,
		AuthorizationID: row.AuthorizationID, ObjectVersionID: *row.ObjectVersionID,
		Version: row.Version,
	}, nil
}

func lockDebugDumpAuthority(
	ctx context.Context,
	queries *store.Queries,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
) (lockedDebugDumpAuthority, bool, error) {
	if worker.ID == uuid.Nil || credentials.AttemptID == uuid.Nil ||
		credentials.WorkerEpoch <= 0 || credentials.Fence <= 0 || credentials.Token == "" {
		return lockedDebugDumpAuthority{}, false, nil
	}
	workerRow, err := queries.LockWorkerAuthority(ctx, worker.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return lockedDebugDumpAuthority{}, false, nil
	}
	if err != nil {
		return lockedDebugDumpAuthority{}, false, fmt.Errorf("lock Worker for debug dump upload: %w", err)
	}
	if err := queries.LockExecutionLeaseWrites(ctx); err != nil {
		return lockedDebugDumpAuthority{}, false, fmt.Errorf("lock execution Lease writes for debug dump upload: %w", err)
	}
	row, err := queries.LockDebugDumpUploadAuthority(ctx, credentials.AttemptID)
	if errors.Is(err, pgx.ErrNoRows) {
		return lockedDebugDumpAuthority{}, false, nil
	}
	if err != nil {
		return lockedDebugDumpAuthority{}, false, fmt.Errorf("lock debug dump upload authority: %w", err)
	}
	now, err := postgresTime(ctx, queries)
	if err != nil {
		return lockedDebugDumpAuthority{}, false, err
	}
	activeAuthorization := false
	if row.DebugDumpAuthorizationID.Valid {
		activeAuthorization, err = queries.ConfirmDebugDumpUploadAuthorization(
			ctx,
			store.ConfirmDebugDumpUploadAuthorizationParams{
				AuthorizationID: row.DebugDumpAuthorizationID.UUID,
				ConfirmedAt:     pgtype.Timestamptz{Time: now, Valid: true},
			},
		)
		if err != nil {
			return lockedDebugDumpAuthority{}, false,
				fmt.Errorf("confirm debug dump upload authorization: %w", err)
		}
	}
	presentedDigest := sha256.Sum256([]byte(credentials.Token))
	active := workerRow.Epoch == credentials.WorkerEpoch && row.WorkerID.Valid &&
		row.WorkerID.UUID == worker.ID && row.WorkerEpoch != nil &&
		*row.WorkerEpoch == credentials.WorkerEpoch && row.AttemptFence == credentials.Fence &&
		row.CurrentFence == credentials.Fence && row.LeaseOwnerKind == store.LeaseOwnerKindWORKER &&
		row.LeaseOwnerID == workerRow.SpiffeID && row.LeasePhase == store.LeasePhaseEXECUTION &&
		(row.AttemptState == store.AttemptStateASSIGNED || row.AttemptState == store.AttemptStateRUNNING) &&
		(row.JobState == store.JobStateASSIGNED || row.JobState == store.JobStateRUNNING) &&
		!row.LeaseRevokedAt.Valid && row.LeaseExpiresAt.Valid && row.LeaseExpiresAt.Time.After(now) &&
		row.JobExpiresAt.Valid && row.JobExpiresAt.Time.After(now) &&
		row.DebugDumpAuthorizationID.Valid && row.DebugDumpAuthorizationExpiresAt.Valid &&
		row.DebugDumpAuthorizationExpiresAt.Time.After(now) && activeAuthorization &&
		hmac.Equal(presentedDigest[:], row.TokenDigest)
	return lockedDebugDumpAuthority{row: row, now: now}, active, nil
}

type normalizedDebugDumpReport struct {
	ObjectVersionID string                `json:"-"`
	SizeBytes       int64                 `json:"size_bytes"`
	SHA256          [sha256.Size]byte     `json:"-"`
	SHA256Hex       string                `json:"sha256"`
	ContentType     string                `json:"content_type"`
	CompletedParts  []DebugDumpUploadPart `json:"completed_parts"`
}

func normalizeDebugDumpReport(
	report DebugDumpUploadReport,
	requireVersion bool,
) (normalizedDebugDumpReport, []byte, [sha256.Size]byte, error) {
	normalized := normalizedDebugDumpReport{
		ObjectVersionID: strings.TrimSpace(report.ObjectVersionID),
		SizeBytes:       report.SizeBytes, SHA256: report.SHA256,
		SHA256Hex:      hex.EncodeToString(report.SHA256[:]),
		ContentType:    strings.TrimSpace(report.ContentType),
		CompletedParts: append([]DebugDumpUploadPart(nil), report.CompletedParts...),
	}
	if normalized.SizeBytes <= 0 || normalized.SizeBytes > debugdumpcontract.MaxBytes ||
		normalized.SHA256 == [sha256.Size]byte{} ||
		normalized.ContentType != debugdumpcontract.ContentType ||
		len(normalized.CompletedParts) != 1 || (requireVersion &&
		(normalized.ObjectVersionID == "" || len(normalized.ObjectVersionID) > 1000 ||
			strings.ContainsRune(normalized.ObjectVersionID, '\x00'))) ||
		(!requireVersion && normalized.ObjectVersionID != "") {
		return normalizedDebugDumpReport{}, nil, [sha256.Size]byte{}, errors.New("invalid debug dump upload report")
	}
	part := normalized.CompletedParts[0]
	checksum, err := base64.StdEncoding.DecodeString(part.ChecksumSHA256)
	if part.Number != 1 || part.SizeBytes != normalized.SizeBytes || part.ETag == "" ||
		len(part.ETag) > 1000 || strings.ContainsRune(part.ETag, '\x00') || err != nil ||
		!hmac.Equal(checksum, normalized.SHA256[:]) {
		return normalizedDebugDumpReport{}, nil, [sha256.Size]byte{}, errors.New("invalid debug dump upload part")
	}
	completedParts, err := json.Marshal(normalized.CompletedParts)
	if err != nil {
		return normalizedDebugDumpReport{}, nil, [sha256.Size]byte{}, fmt.Errorf("marshal debug dump parts: %w", err)
	}
	canonical, err := json.Marshal(normalized)
	if err != nil {
		return normalizedDebugDumpReport{}, nil, [sha256.Size]byte{}, fmt.Errorf("marshal debug dump report: %w", err)
	}
	return normalized, completedParts, sha256.Sum256(append(
		[]byte("vela.debug-dump.upload.v1\x00"), canonical...,
	)), nil
}

func validDebugDumpIntent(intent DebugDumpUploadIntent) bool {
	return intent.DebugDumpID != uuid.Nil && intent.AuthorizationID != uuid.Nil &&
		intent.SizeBytes > 0 && intent.SizeBytes <= debugdumpcontract.MaxBytes &&
		intent.SHA256 != [sha256.Size]byte{} && intent.ContentType == debugdumpcontract.ContentType
}

func validMultipartUploadID(value string) bool {
	return len(value) >= 1 && len(value) <= 1000 && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, '\x00')
}

func decodeDebugDumpParts(raw []byte) ([]DebugDumpUploadPart, error) {
	var parts []DebugDumpUploadPart
	if err := json.Unmarshal(raw, &parts); err != nil || len(parts) > 1 {
		return nil, errors.New("stored debug dump upload parts are invalid")
	}
	return parts, nil
}

func debugDumpStatusFromRow(
	row store.GetDebugDumpUploadStatusRow,
	parts []DebugDumpUploadPart,
	uploadExpiresAt time.Time,
) (DebugDumpUploadStatus, error) {
	if len(row.ExpectedSha256) != sha256.Size ||
		(len(row.Sha256) != 0 && len(row.Sha256) != sha256.Size) ||
		(len(row.CompletionRequestHash) != 0 && len(row.CompletionRequestHash) != sha256.Size) {
		return DebugDumpUploadStatus{}, errors.New("stored debug dump receipt is invalid")
	}
	var expected, actual [sha256.Size]byte
	copy(expected[:], row.ExpectedSha256)
	copy(actual[:], row.Sha256)
	return DebugDumpUploadStatus{
		Decision: DebugDumpUploadRecorded, DebugDumpID: row.DebugDumpID,
		AuthorizationID: row.AuthorizationID, State: DebugDumpUploadState(row.State),
		ObjectKey: row.ObjectKey, ExpectedSizeBytes: row.ExpectedSizeBytes,
		ExpectedSHA256: expected, ExpectedContentType: row.ExpectedContentType,
		MultipartUploadID: stringValue(row.MultipartUploadID), CompletedParts: parts,
		ObjectVersionID: stringValue(row.ObjectVersionID), SizeBytes: int64Value(row.SizeBytes),
		SHA256: actual, ContentType: stringValue(row.ContentType),
		CompletionIntentRecorded: len(row.CompletionRequestHash) == sha256.Size,
		UploadExpiresAt:          uploadExpiresAt, Version: row.Version,
	}, nil
}

func rejectedDebugDumpUploadClaim() DebugDumpUploadClaim {
	return DebugDumpUploadClaim{Decision: DebugDumpUploadClaimRejected}
}

func rejectedDebugDumpMultipartSession() DebugDumpMultipartSession {
	return DebugDumpMultipartSession{Decision: DebugDumpMultipartSessionRejected}
}

func rejectedDebugDumpUploadStatus() DebugDumpUploadStatus {
	return DebugDumpUploadStatus{Decision: DebugDumpUploadRejected}
}

func rejectedDebugDumpUpload() DebugDumpUploadResult {
	return DebugDumpUploadResult{Decision: DebugDumpUploadRejected}
}

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}
