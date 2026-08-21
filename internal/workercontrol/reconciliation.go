package workercontrol

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	store "github.com/vivym/vela/internal/store/sqlc"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FailureSource string

const (
	FailureSourceWorkerReported        FailureSource = "WORKER_REPORTED"
	FailureSourceExecutionLeaseExpired FailureSource = "EXECUTION_LEASE_EXPIRED"
	FailureSourceJobExpired            FailureSource = "JOB_EXPIRED"
)

type ReconciliationResult struct {
	Processed bool
	Source    FailureSource
	Decision  RetryDecision
}

func (s *Service) ReconcileNextExecutionFailure(ctx context.Context) (ReconciliationResult, error) {
	if s == nil || s.pool == nil {
		return ReconciliationResult{}, errors.New("worker coordinator is not configured")
	}
	queries := store.New(s.pool)
	expiredJob, err := queries.FindExpiredJobFailureCandidate(ctx)
	if err == nil {
		if expiredJob.AttemptID != uuid.Nil {
			return s.reconcileAttemptFailure(
				ctx,
				expiredJob.JobID,
				expiredJob.AttemptID,
				expiredJob.WorkerID,
				FailureSourceJobExpired,
			)
		}
		return s.reconcileJobExpiryWithoutAttempt(ctx, expiredJob.JobID)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ReconciliationResult{}, fmt.Errorf("find expired Job failure candidate: %w", err)
	}

	expiredLease, err := queries.FindExpiredExecutionLeaseCandidate(
		ctx,
		int64(s.workerLostGrace/time.Second),
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReconciliationResult{}, nil
	}
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf("find expired EXECUTION Lease candidate: %w", err)
	}
	return s.reconcileAttemptFailure(
		ctx,
		expiredLease.JobID,
		expiredLease.AttemptID,
		expiredLease.WorkerID,
		FailureSourceExecutionLeaseExpired,
	)
}

func (s *Service) reconcileAttemptFailure(
	ctx context.Context,
	jobID uuid.UUID,
	attemptID uuid.UUID,
	workerID uuid.UUID,
	source FailureSource,
) (ReconciliationResult, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf("begin failure reconciliation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)

	workerRow, err := queries.LockWorkerAuthority(ctx, workerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReconciliationResult{}, nil
	}
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf("lock Worker for failure reconciliation: %w", err)
	}
	if err := queries.LockExecutionLeaseWrites(ctx); err != nil {
		return ReconciliationResult{}, fmt.Errorf("lock execution Lease writes: %w", err)
	}
	authority, err := queries.LockFailureAuthority(ctx, attemptID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReconciliationResult{}, nil
	}
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf("lock reconciled failure authority: %w", err)
	}
	if authority.JobID != jobID || authority.WorkerID != workerID ||
		authority.LeasePhase != store.LeasePhaseEXECUTION ||
		authority.LeaseOwnerKind != store.LeaseOwnerKindWORKER ||
		authority.LeaseRevokedAt.Valid || authority.CurrentFence != authority.AttemptFence ||
		(authority.AttemptState != store.AttemptStateASSIGNED && authority.AttemptState != store.AttemptStateRUNNING) ||
		(authority.JobState != store.JobStateASSIGNED && authority.JobState != store.JobStateRUNNING) ||
		!authority.LeaseExpiresAt.Valid || !authority.JobExpiresAt.Valid {
		return ReconciliationResult{}, nil
	}

	decidedAt, err := postgresTime(ctx, queries)
	if err != nil {
		return ReconciliationResult{}, err
	}
	observation := systemFailureObservation(source)
	normalized, requestHash, err := normalizeFailureObservation(observation)
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf("normalize internal failure observation: %w", err)
	}
	transition := attemptFailureTransition{
		Source:           store.ExecutionFailureSource(source),
		AttemptState:     store.AttemptStateFAILED,
		AllowRetry:       false,
		WorkerTransition: workerFailureDrain,
	}
	switch source {
	case FailureSourceJobExpired:
		if authority.JobExpiresAt.Time.After(decidedAt) {
			return ReconciliationResult{}, nil
		}
	case FailureSourceExecutionLeaseExpired:
		if !authority.JobExpiresAt.Time.After(decidedAt) ||
			authority.LeaseExpiresAt.Time.After(decidedAt.Add(-s.workerLostGrace)) {
			return ReconciliationResult{}, nil
		}
		transition.AttemptState = store.AttemptStateLOST
		transition.AllowRetry = true
		transition.WorkerTransition = workerFailureLost
	default:
		return ReconciliationResult{}, fmt.Errorf("unsupported reconciliation source %q", source)
	}

	decision, err := s.applyAttemptFailure(
		ctx,
		queries,
		workerRow,
		authority,
		normalized,
		requestHash,
		decidedAt,
		transition,
	)
	if err != nil {
		return ReconciliationResult{}, err
	}
	commitTime, err := postgresTime(ctx, queries)
	if err != nil {
		return ReconciliationResult{}, err
	}
	if source == FailureSourceExecutionLeaseExpired && !authority.JobExpiresAt.Time.After(commitTime) {
		return ReconciliationResult{}, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return ReconciliationResult{}, fmt.Errorf("commit failure reconciliation: %w", err)
	}
	return ReconciliationResult{Processed: true, Source: source, Decision: decision}, nil
}

func (s *Service) reconcileJobExpiryWithoutAttempt(
	ctx context.Context,
	jobID uuid.UUID,
) (ReconciliationResult, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf("begin Job Expiry reconciliation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	if err := queries.LockExecutionLeaseWrites(ctx); err != nil {
		return ReconciliationResult{}, fmt.Errorf("lock execution Lease writes: %w", err)
	}
	authority, err := queries.LockJobExpiryWithoutAttempt(ctx, jobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReconciliationResult{}, nil
	}
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf("lock Job Expiry authority without Attempt: %w", err)
	}
	decidedAt, err := postgresTime(ctx, queries)
	if err != nil {
		return ReconciliationResult{}, err
	}
	if !authority.JobExpiresAt.Valid || authority.JobExpiresAt.Time.After(decidedAt) {
		return ReconciliationResult{}, nil
	}
	project, err := queries.LockFailureProjectCounters(ctx, store.LockFailureProjectCountersParams{
		OrganizationID: authority.OrganizationID,
		ProjectID:      authority.ProjectID,
	})
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf("lock Project Job Expiry counters: %w", err)
	}
	fromRetry := authority.JobState == store.JobStateRETRYWAIT
	if (fromRetry && project.RetryWaitCount <= 0) || (!fromRetry && project.QueuedCount <= 0) {
		return ReconciliationResult{}, errors.New("project waiting counter is inconsistent with expired Job")
	}
	poolCounters, err := queries.LockFailurePoolCounters(ctx, authority.WorkerPoolID)
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf("lock Worker pool Job Expiry counters: %w", err)
	}
	if (fromRetry && poolCounters.RetryWaitCount <= 0) || (!fromRetry && poolCounters.QueuedCount <= 0) {
		return ReconciliationResult{}, errors.New("worker pool waiting counter is inconsistent with expired Job")
	}
	credit, err := queries.LockFailureCreditReservation(ctx, jobID)
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf("lock CreditReservation for Job Expiry: %w", err)
	}
	if credit.OrganizationID != authority.OrganizationID || credit.ProjectID != authority.ProjectID ||
		credit.State != store.CreditReservationStateRESERVED {
		return ReconciliationResult{}, errors.New("credit reservation is inconsistent with expired Job")
	}
	organizationCredit, err := queries.LockFailureOrganizationCredit(ctx, authority.OrganizationID)
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf("lock Organization credit for Job Expiry: %w", err)
	}
	if organizationCredit.Currency != credit.Currency || organizationCredit.ReservedMinor < credit.AmountMinor {
		return ReconciliationResult{}, errors.New("organization credit is inconsistent with expired Job")
	}
	if authority.CurrentFence == math.MaxInt64 {
		return ReconciliationResult{}, errors.New("job fence overflows bigint")
	}
	jobFence := authority.CurrentFence + 1
	decidedAtValue := pgtype.Timestamptz{Time: decidedAt, Valid: true}
	observation := systemFailureObservation(FailureSourceJobExpired)
	normalized, requestHash, err := normalizeFailureObservation(observation)
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf("normalize internal Job Expiry observation: %w", err)
	}
	fingerprints, err := appendFailureFingerprint(
		authority.FailureFingerprints,
		failureFingerprintRecord{
			Fingerprint:  normalized.FailureFingerprint,
			FailureClass: normalized.FailureClass,
			AttemptID:    uuid.Nil,
			ObservedAt:   decidedAt,
		},
	)
	if err != nil {
		return ReconciliationResult{}, err
	}
	if rows, updateErr := queries.UpdateRetryRuntimeForJobExpiry(ctx, store.UpdateRetryRuntimeForJobExpiryParams{
		DecidedAt:       decidedAtValue,
		JobID:           jobID,
		ExpectedVersion: authority.RetryRuntimeVersion,
	}); updateErr != nil || rows != 1 {
		return ReconciliationResult{}, changedRowsError("update RetryRuntimeState for Job Expiry", rows, updateErr)
	}
	if rows, updateErr := queries.UpdateExecutionRetryEvidenceForJobExpiry(
		ctx,
		store.UpdateExecutionRetryEvidenceForJobExpiryParams{
			FailureFingerprints: fingerprints,
			DecidedAt:           decidedAtValue,
			JobID:               jobID,
		},
	); updateErr != nil || rows != 1 {
		return ReconciliationResult{}, changedRowsError("update protected retry evidence for Job Expiry", rows, updateErr)
	}
	if rows, updateErr := queries.DecrementProjectWaitingForFailure(ctx, store.DecrementProjectWaitingForFailureParams{
		FromRetry:      fromRetry,
		OrganizationID: authority.OrganizationID,
		ProjectID:      authority.ProjectID,
	}); updateErr != nil || rows != 1 {
		return ReconciliationResult{}, changedRowsError("decrement Project waiting counter for Job Expiry", rows, updateErr)
	}
	if rows, updateErr := queries.DecrementPoolWaitingForFailure(ctx, store.DecrementPoolWaitingForFailureParams{
		FromRetry:    fromRetry,
		WorkerPoolID: authority.WorkerPoolID,
	}); updateErr != nil || rows != 1 {
		return ReconciliationResult{}, changedRowsError("decrement Worker pool waiting counter for Job Expiry", rows, updateErr)
	}
	jobVersion, err := queries.MarkJobFailedWithoutAttempt(ctx, store.MarkJobFailedWithoutAttemptParams{
		JobFence:        jobFence,
		DecidedAt:       decidedAtValue,
		JobID:           jobID,
		ExpectedVersion: authority.JobVersion,
		PreviousFence:   authority.CurrentFence,
		ExpectedState:   authority.JobState,
	})
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf("transition expired Job without Attempt to FAILED: %w", err)
	}
	if err := releaseFailureCredit(ctx, queries, authority.OrganizationID, jobID, credit, decidedAtValue); err != nil {
		return ReconciliationResult{}, err
	}
	if err := insertJobExpiryDecisionWithoutAttempt(
		ctx,
		queries,
		authority,
		normalized,
		requestHash,
		jobID,
		jobFence,
		jobVersion,
		decidedAt,
	); err != nil {
		return ReconciliationResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ReconciliationResult{}, fmt.Errorf("commit Job Expiry without Attempt: %w", err)
	}
	decision := RetryDecision{
		Disposition:         RetryDispositionFailed,
		FailureClass:        normalized.FailureClass,
		JobID:               jobID,
		TotalComputeSeconds: authority.ComputeSecondsConsumed,
		JobFence:            jobFence,
		JobVersion:          jobVersion,
		DecidedAt:           decidedAt,
	}
	return ReconciliationResult{
		Processed: true,
		Source:    FailureSourceJobExpired,
		Decision:  decision,
	}, nil
}

func systemFailureObservation(source FailureSource) FailureObservation {
	switch source {
	case FailureSourceExecutionLeaseExpired:
		return FailureObservation{
			FailureClass:             "WORKER_LOST",
			FailureFingerprint:       "execution.lease.expired",
			ErrorSummary:             "EXECUTION Lease expired beyond the Worker Lost grace period",
			BackendStage:             "coordinator",
			GPUUUIDs:                 []string{},
			InferenceBackendRevision: "vela-control/v1",
			RetryRecommended:         true,
			WorkerReusable:           false,
		}
	default:
		return FailureObservation{
			FailureClass:             "JOB_EXPIRED",
			FailureFingerprint:       "job.expired",
			ErrorSummary:             "Job Expiry reached before Visible Completion",
			BackendStage:             "coordinator",
			GPUUUIDs:                 []string{},
			InferenceBackendRevision: "vela-control/v1",
			RetryRecommended:         false,
			WorkerReusable:           false,
		}
	}
}

func insertJobExpiryDecisionWithoutAttempt(
	ctx context.Context,
	queries *store.Queries,
	authority store.LockJobExpiryWithoutAttemptRow,
	normalized normalizedFailureObservation,
	requestHash [sha256Size]byte,
	jobID uuid.UUID,
	jobFence int64,
	jobVersion int64,
	decidedAt time.Time,
) error {
	decidedAtValue := pgtype.Timestamptz{Time: decidedAt, Valid: true}
	if err := queries.InsertExecutionFailureDecision(ctx, store.InsertExecutionFailureDecisionParams{
		ID:                       uuid.New(),
		OrganizationID:           authority.OrganizationID,
		ProjectID:                authority.ProjectID,
		JobID:                    jobID,
		AttemptID:                uuid.NullUUID{},
		WorkerID:                 uuid.NullUUID{},
		Source:                   store.ExecutionFailureSourceJOBEXPIRED,
		Disposition:              store.RetryDispositionFAILED,
		FailureClass:             normalized.FailureClass,
		FailureFingerprint:       normalized.FailureFingerprint,
		RequestHash:              requestHash[:],
		ErrorSummary:             normalized.ErrorSummary,
		BackendStage:             normalized.BackendStage,
		GpuUuids:                 mustMarshalJSON(normalized.GPUUUIDs),
		InferenceBackendRevision: normalized.InferenceBackendRevision,
		RetryRecommended:         false,
		WorkerReusable:           false,
		AttemptComputeSeconds:    0,
		TotalComputeSeconds:      authority.ComputeSecondsConsumed,
		NextRetryAt:              pgtype.Timestamptz{},
		JobFence:                 jobFence,
		JobVersion:               jobVersion,
		DecidedAt:                decidedAtValue,
	}); err != nil {
		return fmt.Errorf("insert Job Expiry decision without Attempt: %w", err)
	}
	return insertJobFailedWithoutAttemptEvent(
		ctx,
		queries,
		authority,
		jobID,
		jobFence,
		jobVersion,
		authority.ComputeSecondsConsumed,
		decidedAt,
	)
}

func insertJobFailedWithoutAttemptEvent(
	ctx context.Context,
	queries *store.Queries,
	authority store.LockJobExpiryWithoutAttemptRow,
	jobID uuid.UUID,
	jobFence int64,
	jobVersion int64,
	totalComputeSeconds int64,
	decidedAt time.Time,
) error {
	eventID := uuid.New()
	event := &velav1.EventEnvelope{
		EventId:          eventID.String(),
		AggregateType:    "Job",
		AggregateId:      jobID.String(),
		AggregateVersion: uint64(jobVersion),
		EventType:        "job.failed",
		SchemaVersion:    1,
		OccurredAt:       timestamppb.New(decidedAt),
		Payload: &velav1.EventEnvelope_JobFailed{JobFailed: &velav1.JobFailed{
			OrganizationId:      authority.OrganizationID.String(),
			ProjectId:           authority.ProjectID.String(),
			JobId:               jobID.String(),
			JobFence:            uint64(jobFence),
			FailureClass:        "JOB_EXPIRED",
			TotalComputeSeconds: uint64(totalComputeSeconds),
			DecidedAt:           timestamppb.New(decidedAt),
		}},
	}
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal Job Expiry job.failed event: %w", err)
	}
	if err := queries.InsertJobFailedOutboxEvent(ctx, store.InsertJobFailedOutboxEventParams{
		EventID:        eventID,
		OrganizationID: authority.OrganizationID,
		ProjectID:      authority.ProjectID,
		JobID:          jobID,
		JobVersion:     jobVersion,
		Payload:        payload,
		OccurredAt:     pgtype.Timestamptz{Time: decidedAt, Valid: true},
	}); err != nil {
		return fmt.Errorf("insert Job Expiry job.failed Outbox event: %w", err)
	}
	return nil
}
