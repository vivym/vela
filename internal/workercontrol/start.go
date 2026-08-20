package workercontrol

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	store "github.com/vivym/vela/internal/store/sqlc"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type StartDecision string

const (
	StartGranted StartDecision = "START_GRANTED"
	Stop         StartDecision = "STOP"
)

type StopReason string

const (
	StopInvalidAuthority StopReason = "INVALID_AUTHORITY"
	StopLeaseExpired     StopReason = "LEASE_EXPIRED"
	StopJobExpired       StopReason = "JOB_EXPIRED"
	StopNotStartable     StopReason = "NOT_STARTABLE"
)

type LeaseCredentials struct {
	AttemptID   uuid.UUID
	WorkerEpoch int64
	Fence       int64
	Token       string
}

type StartResult struct {
	Decision    StartDecision
	StopReason  StopReason
	AttemptID   uuid.UUID
	JobID       uuid.UUID
	WorkerID    uuid.UUID
	WorkerEpoch int64
	LeaseFence  int64
	StartedAt   time.Time
}

func (s *Service) Start(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
) (StartResult, error) {
	if s == nil || s.pool == nil {
		return StartResult{}, errors.New("worker coordinator is not configured")
	}
	if worker.ID == uuid.Nil || credentials.AttemptID == uuid.Nil ||
		credentials.WorkerEpoch <= 0 || credentials.Fence <= 0 || credentials.Token == "" {
		return stopped(StopInvalidAuthority), nil
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return StartResult{}, fmt.Errorf("begin Start transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)

	workerRow, err := queries.LockWorkerForAssignment(ctx, worker.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return stopped(StopInvalidAuthority), nil
	}
	if err != nil {
		return StartResult{}, fmt.Errorf("lock Worker for Start: %w", err)
	}
	if workerRow.Epoch != credentials.WorkerEpoch {
		return stopped(StopInvalidAuthority), nil
	}

	authority, err := queries.LockStartAuthority(ctx, store.LockStartAuthorityParams{
		AttemptID:   credentials.AttemptID,
		WorkerID:    worker.ID,
		WorkerEpoch: credentials.WorkerEpoch,
		Fence:       credentials.Fence,
		OwnerID:     workerRow.SpiffeID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return stopped(StopInvalidAuthority), nil
	}
	if err != nil {
		return StartResult{}, fmt.Errorf("lock Start authority: %w", err)
	}
	if authority.LeaseRevokedAt.Valid || authority.CurrentFence != credentials.Fence {
		return stopped(StopInvalidAuthority), nil
	}
	presentedDigest := sha256.Sum256([]byte(credentials.Token))
	if !hmac.Equal(presentedDigest[:], authority.TokenDigest) {
		return stopped(StopInvalidAuthority), nil
	}

	now, err := assignmentWallClock(ctx, queries)
	if err != nil {
		return StartResult{}, err
	}
	if !authority.LeaseExpiresAt.Valid || !authority.LeaseExpiresAt.Time.After(now) {
		return stopped(StopLeaseExpired), nil
	}
	if !authority.JobExpiresAt.Valid || !authority.JobExpiresAt.Time.After(now) {
		return stopped(StopJobExpired), nil
	}
	if authority.CreditReservationState != store.CreditReservationStateRESERVED {
		return stopped(StopNotStartable), nil
	}

	if authority.AttemptState == store.AttemptStateRUNNING && authority.JobState == store.JobStateRUNNING {
		if !authority.AttemptStartedAt.Valid || !authority.BillableStartedAt.Valid {
			return stopped(StopNotStartable), nil
		}
		result := grantedStart(authority, worker.ID, credentials, authority.AttemptStartedAt.Time)
		commitTime, clockErr := assignmentWallClock(ctx, queries)
		if clockErr != nil {
			return StartResult{}, clockErr
		}
		if !authority.LeaseExpiresAt.Time.After(commitTime) {
			return stopped(StopLeaseExpired), nil
		}
		if !authority.JobExpiresAt.Time.After(commitTime) {
			return stopped(StopJobExpired), nil
		}
		if err := tx.Commit(ctx); err != nil {
			return StartResult{}, fmt.Errorf("commit Start replay: %w", err)
		}
		return result, nil
	}
	if authority.AttemptState != store.AttemptStateASSIGNED ||
		authority.JobState != store.JobStateASSIGNED ||
		authority.AttemptStartedAt.Valid {
		return stopped(StopNotStartable), nil
	}

	startedAt := pgtype.Timestamptz{Time: now, Valid: true}
	if rows, err := queries.MarkAttemptRunning(ctx, store.MarkAttemptRunningParams{
		StartedAt:   startedAt,
		AttemptID:   authority.AttemptID,
		WorkerID:    worker.ID,
		WorkerEpoch: credentials.WorkerEpoch,
		Fence:       credentials.Fence,
	}); err != nil || rows != 1 {
		return StartResult{}, changedRowsError("transition Attempt to RUNNING", rows, err)
	}
	job, err := queries.MarkJobRunning(ctx, store.MarkJobRunningParams{
		StartedAt:       startedAt,
		JobID:           authority.JobID,
		ExpectedVersion: authority.JobVersion,
		Fence:           credentials.Fence,
	})
	if err != nil {
		return StartResult{}, fmt.Errorf("transition Job to RUNNING: %w", err)
	}
	if !job.BillableStartedAt.Valid {
		return StartResult{}, errors.New("committed Billable Start is null")
	}

	eventID := uuid.New()
	event := &velav1.EventEnvelope{
		EventId:          eventID.String(),
		AggregateType:    "Job",
		AggregateId:      authority.JobID.String(),
		AggregateVersion: uint64(job.Version),
		EventType:        "job.started",
		SchemaVersion:    1,
		OccurredAt:       timestamppb.New(now),
		Payload: &velav1.EventEnvelope_JobStarted{JobStarted: &velav1.JobStarted{
			OrganizationId: authority.OrganizationID.String(),
			ProjectId:      authority.ProjectID.String(),
			JobId:          authority.JobID.String(),
			AttemptId:      authority.AttemptID.String(),
			AttemptNumber:  uint32(authority.AttemptNumber),
			WorkerId:       worker.ID.String(),
			WorkerEpoch:    uint64(credentials.WorkerEpoch),
			LeaseFence:     uint64(credentials.Fence),
			StartedAt:      timestamppb.New(now),
		}},
	}
	payload, err := proto.Marshal(event)
	if err != nil {
		return StartResult{}, fmt.Errorf("marshal job.started event: %w", err)
	}
	if err := queries.InsertStartOutboxEvent(ctx, store.InsertStartOutboxEventParams{
		EventID:        eventID,
		OrganizationID: authority.OrganizationID,
		ProjectID:      authority.ProjectID,
		JobID:          authority.JobID,
		JobVersion:     job.Version,
		Payload:        payload,
		OccurredAt:     startedAt,
	}); err != nil {
		return StartResult{}, fmt.Errorf("insert job.started Outbox event: %w", err)
	}

	commitTime, err := assignmentWallClock(ctx, queries)
	if err != nil {
		return StartResult{}, err
	}
	if !authority.LeaseExpiresAt.Time.After(commitTime) {
		return stopped(StopLeaseExpired), nil
	}
	if !authority.JobExpiresAt.Time.After(commitTime) {
		return stopped(StopJobExpired), nil
	}
	if err := tx.Commit(ctx); err != nil {
		return StartResult{}, fmt.Errorf("commit Start: %w", err)
	}
	return grantedStart(authority, worker.ID, credentials, now), nil
}

func stopped(reason StopReason) StartResult {
	return StartResult{Decision: Stop, StopReason: reason}
}

func grantedStart(
	authority store.LockStartAuthorityRow,
	workerID uuid.UUID,
	credentials LeaseCredentials,
	startedAt time.Time,
) StartResult {
	return StartResult{
		Decision:    StartGranted,
		AttemptID:   authority.AttemptID,
		JobID:       authority.JobID,
		WorkerID:    workerID,
		WorkerEpoch: credentials.WorkerEpoch,
		LeaseFence:  credentials.Fence,
		StartedAt:   startedAt,
	}
}
