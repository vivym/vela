package workercontrol

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/vivym/vela/internal/execution"
	store "github.com/vivym/vela/internal/store/sqlc"
)

const maxHeartbeatJSONObjectBytes = 16 * 1024

type HeartbeatDecision string

const (
	HeartbeatContinue HeartbeatDecision = "CONTINUE"
	HeartbeatStop     HeartbeatDecision = "STOP"
)

const (
	StopNotHeartbeatable  StopReason = "NOT_HEARTBEATABLE"
	StopStaleHeartbeat    StopReason = "STALE_HEARTBEAT"
	StopInvalidProgress   StopReason = "INVALID_PROGRESS"
	StopProtocolMigration StopReason = "PROTOCOL_MIGRATION_REQUIRED"
)

type ExecutionPhase = execution.Phase

const (
	ExecutionPhasePreparing  = execution.PhasePreparing
	ExecutionPhaseGenerating = execution.PhaseGenerating
)

type HeartbeatObservation struct {
	Sequence                  int64
	BackendStage              string
	BackendStageProgress      *float64
	EstimatedRemainingSeconds *int64
	GPUHealthSummary          json.RawMessage
	LocalArtifactState        json.RawMessage
	ScratchFreeBytes          int64
	ArtifactStoreReachable    bool
}

type HeartbeatResult struct {
	Decision          HeartbeatDecision
	StopReason        StopReason
	AttemptID         uuid.UUID
	JobID             uuid.UUID
	WorkerID          uuid.UUID
	WorkerEpoch       int64
	LeaseFence        int64
	HeartbeatSequence int64
	ExecutionPhase    ExecutionPhase
	ProgressUpdatedAt time.Time
	LeaseExpiresAt    time.Time
	LeaseValidFor     time.Duration
}

type normalizedHeartbeatObservation struct {
	Sequence                  int64           `json:"sequence"`
	BackendStage              string          `json:"backend_stage"`
	BackendStageProgress      *float64        `json:"backend_stage_progress"`
	EstimatedRemainingSeconds *int64          `json:"estimated_remaining_seconds"`
	GPUHealthSummary          json.RawMessage `json:"gpu_health_summary"`
	LocalArtifactState        json.RawMessage `json:"local_artifact_state"`
	ScratchFreeBytes          int64           `json:"scratch_free_bytes"`
	ArtifactStoreReachable    bool            `json:"artifact_store_reachable"`
}

func (s *Service) Heartbeat(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
	observation HeartbeatObservation,
) (HeartbeatResult, error) {
	if s == nil || s.pool == nil {
		return HeartbeatResult{}, errors.New("worker coordinator is not configured")
	}
	if worker.ID == uuid.Nil || credentials.AttemptID == uuid.Nil ||
		credentials.WorkerEpoch <= 0 || credentials.Fence <= 0 || credentials.Token == "" {
		return heartbeatStopped(StopInvalidAuthority), nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return HeartbeatResult{}, fmt.Errorf("begin Heartbeat transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)

	workerRow, err := queries.LockWorkerAuthority(ctx, worker.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return heartbeatStopped(StopInvalidAuthority), nil
	}
	if err != nil {
		return HeartbeatResult{}, fmt.Errorf("lock Worker for Heartbeat: %w", err)
	}
	if workerRow.Epoch != credentials.WorkerEpoch {
		return heartbeatStopped(StopInvalidAuthority), nil
	}
	if err := queries.LockExecutionLeaseWrites(ctx); err != nil {
		return HeartbeatResult{}, fmt.Errorf("lock execution Lease writes: %w", err)
	}

	authority, err := queries.LockHeartbeatAuthority(ctx, store.LockHeartbeatAuthorityParams{
		AttemptID:   credentials.AttemptID,
		WorkerID:    worker.ID,
		WorkerEpoch: credentials.WorkerEpoch,
		Fence:       credentials.Fence,
		OwnerID:     workerRow.SpiffeID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return heartbeatStopped(StopInvalidAuthority), nil
	}
	if err != nil {
		return HeartbeatResult{}, fmt.Errorf("lock Heartbeat authority: %w", err)
	}
	if authority.LeaseRevokedAt.Valid || authority.CurrentFence != credentials.Fence {
		return heartbeatStopped(StopInvalidAuthority), nil
	}
	presentedDigest := sha256.Sum256([]byte(credentials.Token))
	if !hmac.Equal(presentedDigest[:], authority.TokenDigest) {
		return heartbeatStopped(StopInvalidAuthority), nil
	}

	now, err := postgresTime(ctx, queries)
	if err != nil {
		return HeartbeatResult{}, err
	}
	if !authority.LeaseExpiresAt.Valid || !authority.LeaseExpiresAt.Time.After(now) {
		return heartbeatStopped(StopLeaseExpired), nil
	}
	if !authority.JobExpiresAt.Valid || !authority.JobExpiresAt.Time.After(now) {
		return heartbeatStopped(StopJobExpired), nil
	}
	phase, heartbeatable := heartbeatPhase(
		authority.AttemptState,
		authority.JobState,
		authority.JobExecutionPhase,
	)
	if !heartbeatable || authority.CreditReservationState != store.CreditReservationStateRESERVED {
		return heartbeatStopped(StopNotHeartbeatable), nil
	}
	renewalEnabled, err := queries.LockExecutionLeaseRenewalProtocol(ctx)
	if err != nil {
		return HeartbeatResult{}, fmt.Errorf("lock execution Lease renewal protocol: %w", err)
	}
	if !renewalEnabled {
		return heartbeatStopped(StopProtocolMigration), nil
	}
	normalized, requestHash, err := normalizeHeartbeatObservation(observation)
	if err != nil {
		return heartbeatStopped(StopInvalidProgress), nil
	}

	previous, progressErr := queries.GetAttemptProgressForHeartbeat(ctx, authority.AttemptID)
	progressExists := progressErr == nil
	if progressErr != nil && !errors.Is(progressErr, pgx.ErrNoRows) {
		return HeartbeatResult{}, fmt.Errorf("lock Attempt progress: %w", progressErr)
	}
	if progressExists && normalized.Sequence <= previous.HeartbeatSequence {
		if normalized.Sequence != previous.HeartbeatSequence ||
			!hmac.Equal(requestHash[:], previous.RequestHash) {
			return heartbeatStopped(StopStaleHeartbeat), nil
		}
		return s.replayHeartbeat(ctx, tx, queries, authority, previous, worker.ID, credentials)
	}

	progressUpdatedAt := now
	if progressExists && !progressUpdatedAt.After(previous.ProgressUpdatedAt.Time) {
		progressUpdatedAt = previous.ProgressUpdatedAt.Time.Add(time.Microsecond)
	}
	renewedExpiry := now.Add(s.leaseTTL)
	if authority.LeaseExpiresAt.Time.After(renewedExpiry) {
		renewedExpiry = authority.LeaseExpiresAt.Time
	}
	if authority.JobExpiresAt.Time.Before(renewedExpiry) {
		renewedExpiry = authority.JobExpiresAt.Time
	}
	if !renewedExpiry.After(progressUpdatedAt) {
		return heartbeatStopped(StopJobExpired), nil
	}

	estimatedFinishAt := pgtype.Timestamptz{}
	if normalized.EstimatedRemainingSeconds != nil {
		estimatedFinishAt = pgtype.Timestamptz{
			Time:  progressUpdatedAt.Add(time.Duration(*normalized.EstimatedRemainingSeconds) * time.Second),
			Valid: true,
		}
	}
	progressTime := pgtype.Timestamptz{Time: progressUpdatedAt, Valid: true}
	renewedExpiryValue := pgtype.Timestamptz{Time: renewedExpiry, Valid: true}
	if rows, updateErr := queries.RenewExecutionLease(ctx, store.RenewExecutionLeaseParams{
		ExpiresAt:         renewedExpiryValue,
		UpdatedAt:         progressTime,
		LeaseID:           authority.LeaseID,
		AttemptID:         authority.AttemptID,
		WorkerID:          worker.ID,
		WorkerEpoch:       credentials.WorkerEpoch,
		Fence:             credentials.Fence,
		PreviousExpiresAt: authority.LeaseExpiresAt,
	}); updateErr != nil || rows != 1 {
		return HeartbeatResult{}, changedRowsError("renew EXECUTION Lease", rows, updateErr)
	}
	if rows, updateErr := queries.UpsertAttemptProgress(ctx, store.UpsertAttemptProgressParams{
		AttemptID:                 authority.AttemptID,
		OrganizationID:            authority.OrganizationID,
		ProjectID:                 authority.ProjectID,
		JobID:                     authority.JobID,
		WorkerID:                  worker.ID,
		WorkerEpoch:               credentials.WorkerEpoch,
		Fence:                     credentials.Fence,
		HeartbeatSequence:         normalized.Sequence,
		RequestHash:               requestHash[:],
		BackendStage:              normalized.BackendStage,
		ExecutionPhase:            phase,
		PhaseProgress:             normalized.BackendStageProgress,
		EstimatedRemainingSeconds: normalized.EstimatedRemainingSeconds,
		EstimatedFinishAt:         estimatedFinishAt,
		GpuHealthSummary:          normalized.GPUHealthSummary,
		LocalArtifactState:        normalized.LocalArtifactState,
		ScratchFreeBytes:          normalized.ScratchFreeBytes,
		ArtifactStoreReachable:    normalized.ArtifactStoreReachable,
		ProgressUpdatedAt:         progressTime,
		ProgressValidUntil:        renewedExpiryValue,
	}); updateErr != nil || rows != 1 {
		return HeartbeatResult{}, changedRowsError("upsert Attempt progress", rows, updateErr)
	}
	if rows, updateErr := queries.MarkWorkerHeartbeat(ctx, store.MarkWorkerHeartbeatParams{
		HeartbeatAt: progressTime,
		WorkerID:    worker.ID,
		WorkerEpoch: credentials.WorkerEpoch,
	}); updateErr != nil || rows != 1 {
		return HeartbeatResult{}, changedRowsError("record Worker heartbeat", rows, updateErr)
	}

	commitTime, err := postgresTime(ctx, queries)
	if err != nil {
		return HeartbeatResult{}, err
	}
	if !authority.JobExpiresAt.Time.After(commitTime) {
		return heartbeatStopped(StopJobExpired), nil
	}
	if !renewedExpiry.After(commitTime) {
		return heartbeatStopped(StopLeaseExpired), nil
	}
	result := continuedHeartbeat(
		authority,
		worker.ID,
		credentials,
		normalized.Sequence,
		phase,
		progressUpdatedAt,
		renewedExpiry,
		renewedExpiry.Sub(commitTime),
	)
	if err := tx.Commit(ctx); err != nil {
		return HeartbeatResult{}, fmt.Errorf("commit Heartbeat: %w", err)
	}
	return result, nil
}

func (s *Service) replayHeartbeat(
	ctx context.Context,
	tx pgx.Tx,
	queries *store.Queries,
	authority store.LockHeartbeatAuthorityRow,
	progress store.GetAttemptProgressForHeartbeatRow,
	workerID uuid.UUID,
	credentials LeaseCredentials,
) (HeartbeatResult, error) {
	if !progress.ProgressUpdatedAt.Valid {
		return HeartbeatResult{}, errors.New("stored Heartbeat progress time is null")
	}
	commitTime, err := postgresTime(ctx, queries)
	if err != nil {
		return HeartbeatResult{}, err
	}
	if !authority.JobExpiresAt.Time.After(commitTime) {
		return heartbeatStopped(StopJobExpired), nil
	}
	if !authority.LeaseExpiresAt.Time.After(commitTime) {
		return heartbeatStopped(StopLeaseExpired), nil
	}
	result := continuedHeartbeat(
		authority,
		workerID,
		credentials,
		progress.HeartbeatSequence,
		progress.ExecutionPhase,
		progress.ProgressUpdatedAt.Time,
		authority.LeaseExpiresAt.Time,
		authority.LeaseExpiresAt.Time.Sub(commitTime),
	)
	if err := tx.Commit(ctx); err != nil {
		return HeartbeatResult{}, fmt.Errorf("commit Heartbeat replay: %w", err)
	}
	return result, nil
}

func heartbeatPhase(
	attemptState store.AttemptState,
	jobState store.JobState,
	jobExecutionPhase *execution.Phase,
) (ExecutionPhase, bool) {
	if string(attemptState) != string(jobState) ||
		(attemptState != store.AttemptStateASSIGNED && attemptState != store.AttemptStateRUNNING) ||
		jobExecutionPhase == nil {
		return "", false
	}
	return *jobExecutionPhase, true
}

func normalizeHeartbeatObservation(
	observation HeartbeatObservation,
) (normalizedHeartbeatObservation, [sha256.Size]byte, error) {
	if observation.Sequence <= 0 || observation.ScratchFreeBytes < 0 ||
		!validBackendStage(observation.BackendStage) {
		return normalizedHeartbeatObservation{}, [sha256.Size]byte{}, errors.New("invalid Heartbeat observation")
	}
	if observation.BackendStageProgress != nil &&
		(math.IsNaN(*observation.BackendStageProgress) ||
			math.IsInf(*observation.BackendStageProgress, 0) ||
			*observation.BackendStageProgress < 0 || *observation.BackendStageProgress >= 1) {
		return normalizedHeartbeatObservation{}, [sha256.Size]byte{}, errors.New("invalid backend stage progress")
	}
	if observation.EstimatedRemainingSeconds != nil &&
		(*observation.EstimatedRemainingSeconds < 0 ||
			*observation.EstimatedRemainingSeconds > math.MaxInt64/int64(time.Second)) {
		return normalizedHeartbeatObservation{}, [sha256.Size]byte{}, errors.New("invalid estimated remaining seconds")
	}
	gpuHealth, err := canonicalJSONObject(observation.GPUHealthSummary)
	if err != nil {
		return normalizedHeartbeatObservation{}, [sha256.Size]byte{}, err
	}
	localArtifactState, err := canonicalJSONObject(observation.LocalArtifactState)
	if err != nil {
		return normalizedHeartbeatObservation{}, [sha256.Size]byte{}, err
	}
	normalized := normalizedHeartbeatObservation{
		Sequence:                  observation.Sequence,
		BackendStage:              observation.BackendStage,
		BackendStageProgress:      cloneFloat64(observation.BackendStageProgress),
		EstimatedRemainingSeconds: cloneInt64(observation.EstimatedRemainingSeconds),
		GPUHealthSummary:          gpuHealth,
		LocalArtifactState:        localArtifactState,
		ScratchFreeBytes:          observation.ScratchFreeBytes,
		ArtifactStoreReachable:    observation.ArtifactStoreReachable,
	}
	canonical, err := json.Marshal(normalized)
	if err != nil {
		return normalizedHeartbeatObservation{}, [sha256.Size]byte{}, fmt.Errorf("canonicalize Heartbeat observation: %w", err)
	}
	return normalized, sha256.Sum256(canonical), nil
}

func validBackendStage(stage string) bool {
	if !utf8.ValidString(stage) || strings.TrimSpace(stage) == "" {
		return false
	}
	count := 0
	for _, character := range stage {
		if !unicode.IsPrint(character) {
			return false
		}
		count++
	}
	return count <= 100
}

func canonicalJSONObject(value json.RawMessage) (json.RawMessage, error) {
	if len(value) == 0 || len(value) > maxHeartbeatJSONObjectBytes {
		return nil, errors.New("Heartbeat JSON object is absent or too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil || object == nil {
		return nil, errors.New("Heartbeat JSON value must be an object")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("Heartbeat JSON value must contain one object")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("canonicalize Heartbeat JSON object: %w", err)
	}
	if len(canonical) > maxHeartbeatJSONObjectBytes {
		return nil, errors.New("canonical Heartbeat JSON object is too large")
	}
	return canonical, nil
}

func cloneFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func continuedHeartbeat(
	authority store.LockHeartbeatAuthorityRow,
	workerID uuid.UUID,
	credentials LeaseCredentials,
	sequence int64,
	phase ExecutionPhase,
	progressUpdatedAt time.Time,
	leaseExpiresAt time.Time,
	leaseValidFor time.Duration,
) HeartbeatResult {
	return HeartbeatResult{
		Decision:          HeartbeatContinue,
		AttemptID:         authority.AttemptID,
		JobID:             authority.JobID,
		WorkerID:          workerID,
		WorkerEpoch:       credentials.WorkerEpoch,
		LeaseFence:        credentials.Fence,
		HeartbeatSequence: sequence,
		ExecutionPhase:    phase,
		ProgressUpdatedAt: progressUpdatedAt,
		LeaseExpiresAt:    leaseExpiresAt,
		LeaseValidFor:     leaseValidFor,
	}
}

func heartbeatStopped(reason StopReason) HeartbeatResult {
	return HeartbeatResult{Decision: HeartbeatStop, StopReason: reason}
}
