package workercontrol

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	store "github.com/vivym/vela/internal/store/sqlc"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FailureCode string

const (
	FailureNoAssignment         FailureCode = "no_assignment"
	FailureWorkerNotFound       FailureCode = "worker_not_found"
	FailureStaleWorkerEpoch     FailureCode = "stale_worker_epoch"
	FailureLeaseExpired         FailureCode = "lease_expired"
	FailureWorkerUnavailable    FailureCode = "worker_unavailable"
	FailureCandidateUnavailable FailureCode = "candidate_unavailable"
)

type Failure struct {
	Code    FailureCode
	Message string
}

func (e *Failure) Error() string {
	return string(e.Code) + ": " + e.Message
}

type AuthenticatedWorker struct {
	ID uuid.UUID
}

type AssignmentCandidate struct {
	JobID                      uuid.UUID
	ExpectedJobVersion         int64
	ExecutionProfileRevisionID uuid.UUID
}

type Assignment struct {
	AttemptID                  uuid.UUID
	JobID                      uuid.UUID
	WorkerID                   uuid.UUID
	WorkerEpoch                int64
	ExecutionProfileRevisionID uuid.UUID
	AttemptNumber              int32
	LeaseToken                 string
	LeaseFence                 int64
	LeaseExpiresAt             time.Time
	LeaseValidFor              time.Duration
}

type Config struct {
	LeaseTTL          time.Duration
	WorkerLostGrace   time.Duration
	ActiveLeaseKeyID  string
	LeaseKeys         map[string][]byte
	ArtifactInspector ArtifactInspector
}

type Service struct {
	pool              *pgxpool.Pool
	leaseTTL          time.Duration
	workerLostGrace   time.Duration
	activeLeaseKeyID  string
	leaseKeys         map[string][]byte
	artifactInspector ArtifactInspector
}

func NewService(ctx context.Context, pool *pgxpool.Pool, config Config) (*Service, error) {
	if ctx == nil {
		return nil, errors.New("worker coordinator startup context is required")
	}
	if pool == nil {
		return nil, errors.New("worker coordinator database pool is required")
	}
	if config.LeaseTTL < time.Second || config.LeaseTTL%time.Second != 0 {
		return nil, errors.New("execution Lease TTL must be a positive whole number of seconds")
	}
	workerLostGrace := config.WorkerLostGrace
	if workerLostGrace == 0 {
		workerLostGrace = 30 * time.Second
	}
	if workerLostGrace < time.Second || workerLostGrace%time.Second != 0 {
		return nil, errors.New("worker lost grace must be a positive whole number of seconds")
	}
	if config.ActiveLeaseKeyID == "" {
		return nil, errors.New("active Lease signing-key id is required")
	}
	keys := make(map[string][]byte, len(config.LeaseKeys))
	for keyID, key := range config.LeaseKeys {
		if keyID == "" || len(key) < 32 {
			return nil, errors.New("every Lease signing key must have an id and at least 32 bytes")
		}
		keys[keyID] = append([]byte(nil), key...)
	}
	if _, ok := keys[config.ActiveLeaseKeyID]; !ok {
		return nil, errors.New("active Lease signing key is absent from the keyring")
	}
	activeKeyIDs, err := store.New(pool).ListActiveLeaseSigningKeyIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("validate active Lease signing-key coverage: %w", err)
	}
	for _, keyID := range activeKeyIDs {
		if _, ok := keys[keyID]; !ok {
			return nil, fmt.Errorf("active Lease signing key %q is absent from the keyring", keyID)
		}
	}
	return &Service{
		pool:              pool,
		leaseTTL:          config.LeaseTTL,
		workerLostGrace:   workerLostGrace,
		activeLeaseKeyID:  config.ActiveLeaseKeyID,
		leaseKeys:         keys,
		artifactInspector: config.ArtifactInspector,
	}, nil
}

func (s *Service) Acquire(
	ctx context.Context,
	worker AuthenticatedWorker,
	workerEpoch int64,
	candidate *AssignmentCandidate,
) (Assignment, error) {
	if s == nil || s.pool == nil {
		return Assignment{}, errors.New("worker coordinator is not configured")
	}
	if worker.ID == uuid.Nil || workerEpoch <= 0 {
		return Assignment{}, failure(FailureWorkerNotFound, "authenticated Worker identity is invalid")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Assignment{}, fmt.Errorf("begin Assignment transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)

	workerRow, err := queries.LockWorkerAuthority(ctx, worker.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Assignment{}, failure(FailureWorkerNotFound, "authenticated Worker is not registered")
	}
	if err != nil {
		return Assignment{}, fmt.Errorf("lock Worker for Assignment: %w", err)
	}
	if workerRow.Epoch != workerEpoch {
		return Assignment{}, failure(FailureStaleWorkerEpoch, "Worker epoch is not current")
	}
	existing, err := queries.GetActiveWorkerAssignment(ctx, store.GetActiveWorkerAssignmentParams{
		WorkerID:    worker.ID,
		WorkerEpoch: workerEpoch,
		OwnerID:     workerRow.SpiffeID,
	})
	if err == nil {
		now, clockErr := postgresTime(ctx, queries)
		if clockErr != nil {
			return Assignment{}, clockErr
		}
		if !existing.ExpiresAt.Valid || !existing.ExpiresAt.Time.After(now) {
			return Assignment{}, failure(FailureLeaseExpired, "active Assignment Lease has expired")
		}
		assignment, replayErr := s.assignmentFromExisting(existing)
		if replayErr != nil {
			return Assignment{}, replayErr
		}
		commitTime, clockErr := postgresTime(ctx, queries)
		if clockErr != nil {
			return Assignment{}, clockErr
		}
		if !existing.ExpiresAt.Time.After(commitTime) {
			return Assignment{}, failure(FailureLeaseExpired, "active Assignment Lease expired before replay commit")
		}
		assignment.LeaseValidFor = existing.ExpiresAt.Time.Sub(commitTime)
		if err := tx.Commit(ctx); err != nil {
			return Assignment{}, fmt.Errorf("commit Assignment replay: %w", err)
		}
		return assignment, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Assignment{}, fmt.Errorf("read active Worker Assignment: %w", err)
	}
	if candidate == nil {
		return Assignment{}, failure(FailureNoAssignment, "Worker has no active Assignment")
	}
	if candidate.JobID == uuid.Nil ||
		candidate.ExpectedJobVersion <= 0 ||
		candidate.ExecutionProfileRevisionID == uuid.Nil {
		return Assignment{}, failure(FailureCandidateUnavailable, "Assignment candidate is invalid")
	}
	if workerRow.LifecycleState != store.WorkerLifecycleStateREADY ||
		workerRow.ReachabilityCondition != store.WorkerReachabilityConditionHEALTHY {
		return Assignment{}, failure(FailureWorkerUnavailable, "Worker is not READY and HEALTHY")
	}

	job, err := queries.LockJobForAssignment(ctx, candidate.JobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Assignment{}, failure(FailureCandidateUnavailable, "candidate Job is unavailable")
	}
	if err != nil {
		return Assignment{}, fmt.Errorf("lock candidate Job: %w", err)
	}
	if job.Version != candidate.ExpectedJobVersion ||
		(job.State != store.JobStateQUEUED && job.State != store.JobStateRETRYWAIT) ||
		job.ServiceClassRevisionState != store.CatalogStateACTIVE ||
		!job.JobExpiresAt.Valid ||
		job.CreditReservationState != store.CreditReservationStateRESERVED ||
		job.WorkerPoolID != workerRow.WorkerPoolID {
		return Assignment{}, failure(FailureCandidateUnavailable, "candidate Job no longer satisfies Assignment preconditions")
	}
	if job.AttemptsStarted >= job.ExecutionMaxAttempts ||
		job.ComputeSecondsConsumed >= job.ExecutionMaxTotalComputeSeconds {
		return Assignment{}, failure(FailureCandidateUnavailable, "candidate Job has exhausted Retry Budget")
	}
	now, err := postgresTime(ctx, queries)
	if err != nil {
		return Assignment{}, err
	}
	excluded, err := workerExcludedForJob(job.ExcludedWorkers, worker.ID, now)
	if err != nil {
		return Assignment{}, err
	}
	if excluded {
		return Assignment{}, failure(FailureCandidateUnavailable, "Worker is excluded by the Job RetryRuntimeState")
	}

	project, err := queries.LockProjectForAssignment(ctx, store.LockProjectForAssignmentParams{
		OrganizationID: job.OrganizationID,
		ProjectID:      job.ProjectID,
	})
	if err != nil {
		return Assignment{}, fmt.Errorf("lock Project Assignment counters: %w", err)
	}
	if project.RunningCount >= project.RunningLimit ||
		(job.State == store.JobStateQUEUED && project.QueuedCount-project.RetryWaitCount <= 0) ||
		(job.State == store.JobStateRETRYWAIT && project.RetryWaitCount <= 0) {
		return Assignment{}, failure(FailureCandidateUnavailable, "Project Assignment capacity is unavailable")
	}
	if _, err := queries.ValidateProfileForAssignment(ctx, store.ValidateProfileForAssignmentParams{
		ExecutionProfileRevisionID: candidate.ExecutionProfileRevisionID,
		ModelRevisionID:            job.ModelRevisionID,
		WorkerPoolID:               workerRow.WorkerPoolID,
		GenerationPresetRevisionID: job.GenerationPresetRevisionID,
		OutputSpecID:               job.OutputSpecID,
	}); errors.Is(err, pgx.ErrNoRows) {
		return Assignment{}, failure(FailureCandidateUnavailable, "Execution Profile is not actively certified for the Job")
	} else if err != nil {
		return Assignment{}, fmt.Errorf("validate Assignment profile: %w", err)
	}
	if !job.JobExpiresAt.Time.After(now) ||
		(job.State == store.JobStateRETRYWAIT && (!job.NextRetryAt.Valid || job.NextRetryAt.Time.After(now))) {
		return Assignment{}, failure(FailureCandidateUnavailable, "candidate Job no longer satisfies Assignment time preconditions")
	}
	assignedAt := pgtype.Timestamptz{Time: now, Valid: true}

	attemptID := uuid.New()
	leaseID := uuid.New()
	attemptNumber := job.AttemptsStarted + 1
	fence := job.CurrentFence + 1
	expiresAt := now.Add(s.leaseTTL)
	if job.JobExpiresAt.Time.Before(expiresAt) {
		expiresAt = job.JobExpiresAt.Time
	}
	expiresAtValue := pgtype.Timestamptz{Time: expiresAt, Valid: true}
	leaseToken, tokenDigest, err := s.issueLeaseToken(leaseTokenClaims{
		AttemptID: attemptID,
		WorkerID:  worker.ID,
		Epoch:     workerEpoch,
		Fence:     fence,
		IssuedAt:  now,
		ExpiresAt: expiresAt,
	}, s.activeLeaseKeyID)
	if err != nil {
		return Assignment{}, err
	}

	if err := queries.InsertAttempt(ctx, store.InsertAttemptParams{
		ID:                         attemptID,
		OrganizationID:             job.OrganizationID,
		ProjectID:                  job.ProjectID,
		JobID:                      job.ID,
		AttemptNumber:              attemptNumber,
		ExecutionProfileRevisionID: candidate.ExecutionProfileRevisionID,
		WorkerPoolID:               workerRow.WorkerPoolID,
		WorkerID:                   worker.ID,
		WorkerEpoch:                workerEpoch,
		Fence:                      fence,
		AssignedAt:                 assignedAt,
	}); err != nil {
		return Assignment{}, fmt.Errorf("insert Attempt: %w", err)
	}
	if err := queries.InsertExecutionLease(ctx, store.InsertExecutionLeaseParams{
		ID:             leaseID,
		OrganizationID: job.OrganizationID,
		ProjectID:      job.ProjectID,
		AttemptID:      attemptID,
		WorkerID:       worker.ID,
		WorkerEpoch:    workerEpoch,
		OwnerID:        workerRow.SpiffeID,
		Fence:          fence,
		TokenDigest:    tokenDigest,
		SigningKeyID:   s.activeLeaseKeyID,
		IssuedAt:       assignedAt,
		ExpiresAt:      expiresAtValue,
	}); err != nil {
		return Assignment{}, fmt.Errorf("insert EXECUTION Lease: %w", err)
	}
	jobVersion, err := queries.MarkJobAssigned(ctx, store.MarkJobAssignedParams{
		Fence:           fence,
		AssignedAt:      assignedAt,
		JobID:           job.ID,
		ExpectedVersion: candidate.ExpectedJobVersion,
	})
	if err != nil {
		return Assignment{}, fmt.Errorf("transition Job to ASSIGNED: %w", err)
	}
	if rows, err := queries.MarkWorkerBusy(ctx, store.MarkWorkerBusyParams{
		AssignedAt:  assignedAt,
		WorkerID:    worker.ID,
		WorkerEpoch: workerEpoch,
	}); err != nil || rows != 1 {
		return Assignment{}, changedRowsError("transition Worker to BUSY", rows, err)
	}
	if rows, err := queries.MoveProjectCountersToRunning(ctx, store.MoveProjectCountersToRunningParams{
		OrganizationID: job.OrganizationID,
		ProjectID:      job.ProjectID,
	}); err != nil || rows != 1 {
		return Assignment{}, changedRowsError("move Project counters to running", rows, err)
	}
	if rows, err := queries.DecrementPoolQueuedForAssignment(
		ctx,
		workerRow.WorkerPoolID,
	); err != nil || rows != 1 {
		return Assignment{}, changedRowsError("decrement Worker pool queued counter", rows, err)
	}
	if rows, err := queries.IncrementAttemptsStarted(ctx, store.IncrementAttemptsStartedParams{
		AssignedAt:              assignedAt,
		JobID:                   job.ID,
		PreviousAttemptsStarted: job.AttemptsStarted,
		ExpectedVersion:         job.RetryRuntimeVersion,
	}); err != nil || rows != 1 {
		return Assignment{}, changedRowsError("increment attempts started", rows, err)
	}

	eventID := uuid.New()
	event := &velav1.EventEnvelope{
		EventId:          eventID.String(),
		AggregateType:    "Job",
		AggregateId:      job.ID.String(),
		AggregateVersion: uint64(jobVersion),
		EventType:        "job.assigned",
		SchemaVersion:    1,
		OccurredAt:       timestamppb.New(now),
		Payload: &velav1.EventEnvelope_JobAssigned{JobAssigned: &velav1.JobAssigned{
			OrganizationId:             job.OrganizationID.String(),
			ProjectId:                  job.ProjectID.String(),
			JobId:                      job.ID.String(),
			AttemptId:                  attemptID.String(),
			AttemptNumber:              uint32(attemptNumber),
			WorkerId:                   worker.ID.String(),
			WorkerEpoch:                uint64(workerEpoch),
			ExecutionProfileRevisionId: candidate.ExecutionProfileRevisionID.String(),
			LeaseFence:                 uint64(fence),
			LeaseExpiresAt:             timestamppb.New(expiresAt),
		}},
	}
	payload, err := proto.Marshal(event)
	if err != nil {
		return Assignment{}, fmt.Errorf("marshal job.assigned event: %w", err)
	}
	if err := queries.InsertAssignmentOutboxEvent(ctx, store.InsertAssignmentOutboxEventParams{
		EventID:        eventID,
		OrganizationID: job.OrganizationID,
		ProjectID:      job.ProjectID,
		JobID:          job.ID,
		JobVersion:     jobVersion,
		Payload:        payload,
		OccurredAt:     assignedAt,
	}); err != nil {
		return Assignment{}, fmt.Errorf("insert job.assigned Outbox event: %w", err)
	}
	commitTime, err := postgresTime(ctx, queries)
	if err != nil {
		return Assignment{}, err
	}
	if !job.JobExpiresAt.Time.After(commitTime) || !expiresAt.After(commitTime) {
		return Assignment{}, failure(
			FailureCandidateUnavailable,
			"candidate Job or EXECUTION Lease expired before Assignment commit",
		)
	}
	leaseValidFor := expiresAt.Sub(commitTime)
	if err := tx.Commit(ctx); err != nil {
		return Assignment{}, fmt.Errorf("commit Assignment: %w", err)
	}
	return Assignment{
		AttemptID:                  attemptID,
		JobID:                      job.ID,
		WorkerID:                   worker.ID,
		WorkerEpoch:                workerEpoch,
		ExecutionProfileRevisionID: candidate.ExecutionProfileRevisionID,
		AttemptNumber:              attemptNumber,
		LeaseToken:                 leaseToken,
		LeaseFence:                 fence,
		LeaseExpiresAt:             expiresAt,
		LeaseValidFor:              leaseValidFor,
	}, nil
}

func (s *Service) assignmentFromExisting(row store.GetActiveWorkerAssignmentRow) (Assignment, error) {
	if !row.IssuedAt.Valid || !row.TokenClaimExpiresAt.Valid || !row.ExpiresAt.Valid {
		return Assignment{}, errors.New("stored Lease timestamps are null")
	}
	token, digest, err := s.issueLeaseToken(leaseTokenClaims{
		AttemptID: row.AttemptID,
		WorkerID:  row.WorkerID,
		Epoch:     row.WorkerEpoch,
		Fence:     row.Fence,
		IssuedAt:  row.IssuedAt.Time,
		ExpiresAt: row.TokenClaimExpiresAt.Time,
	}, row.SigningKeyID)
	if err != nil {
		return Assignment{}, err
	}
	if !hmac.Equal(digest, row.TokenDigest) {
		return Assignment{}, errors.New("stored Lease digest does not match reconstructed token")
	}
	return Assignment{
		AttemptID:                  row.AttemptID,
		JobID:                      row.JobID,
		WorkerID:                   row.WorkerID,
		WorkerEpoch:                row.WorkerEpoch,
		ExecutionProfileRevisionID: row.ExecutionProfileRevisionID,
		AttemptNumber:              row.AttemptNumber,
		LeaseToken:                 token,
		LeaseFence:                 row.Fence,
		LeaseExpiresAt:             row.ExpiresAt.Time,
	}, nil
}

func postgresTime(ctx context.Context, queries *store.Queries) (time.Time, error) {
	wallClock, err := queries.GetPostgresTime(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("read PostgreSQL time: %w", err)
	}
	if !wallClock.Valid {
		return time.Time{}, errors.New("PostgreSQL time is null")
	}
	return wallClock.Time, nil
}

type leaseTokenClaims struct {
	AttemptID uuid.UUID
	WorkerID  uuid.UUID
	Epoch     int64
	Fence     int64
	IssuedAt  time.Time
	ExpiresAt time.Time
}

func (s *Service) issueLeaseToken(claims leaseTokenClaims, keyID string) (string, []byte, error) {
	key, ok := s.leaseKeys[keyID]
	if !ok {
		return "", nil, fmt.Errorf("lease signing key %q is unavailable", keyID)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = fmt.Fprintf(
		mac,
		"vela-execution-lease-v1\n%s\n%s\n%d\n%d\n%d\n%d",
		claims.AttemptID,
		claims.WorkerID,
		claims.Epoch,
		claims.Fence,
		claims.IssuedAt.UnixNano(),
		claims.ExpiresAt.UnixNano(),
	)
	token := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	digest := sha256.Sum256([]byte(token))
	return token, digest[:], nil
}

func changedRowsError(operation string, rows int64, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return fmt.Errorf("%s changed %d rows, want 1", operation, rows)
}

func failure(code FailureCode, message string) error {
	return &Failure{Code: code, Message: message}
}
