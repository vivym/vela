package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	store "github.com/vivym/vela/internal/store/sqlc"
	"github.com/vivym/vela/internal/workercontrol"
)

type AssignmentCoordinator interface {
	Acquire(
		context.Context,
		workercontrol.AuthenticatedWorker,
		int64,
		*workercontrol.AssignmentCandidate,
	) (workercontrol.Assignment, error)
}

type Config struct {
	SchedulerID       string
	ClaimTTL          time.Duration
	CandidateAttempts int
}

type Dispatch struct {
	IntentID   uuid.UUID
	Lane       store.SchedulerLane
	Assignment workercontrol.Assignment
}

type Service struct {
	pool              *pgxpool.Pool
	coordinator       AssignmentCoordinator
	schedulerID       string
	claimTTLSeconds   int32
	candidateAttempts int
}

func NewService(
	pool *pgxpool.Pool,
	coordinator AssignmentCoordinator,
	config Config,
) (*Service, error) {
	if pool == nil {
		return nil, errors.New("scheduler PostgreSQL pool is required")
	}
	if coordinator == nil {
		return nil, errors.New("assignment coordinator is required")
	}
	if len(config.SchedulerID) < 1 || len(config.SchedulerID) > 200 {
		return nil, errors.New("scheduler identity must contain between 1 and 200 bytes")
	}
	if config.ClaimTTL <= 0 || config.ClaimTTL > 5*time.Minute {
		return nil, errors.New("scheduler claim TTL must be in (0, 5m]")
	}
	claimTTLSeconds := int64(math.Ceil(config.ClaimTTL.Seconds()))
	if claimTTLSeconds < 1 || claimTTLSeconds > 300 {
		return nil, errors.New("scheduler claim TTL must resolve to 1..300 seconds")
	}
	if config.CandidateAttempts < 1 || config.CandidateAttempts > 20 {
		return nil, errors.New("scheduler candidate attempts must be in 1..20")
	}
	return &Service{
		pool:              pool,
		coordinator:       coordinator,
		schedulerID:       config.SchedulerID,
		claimTTLSeconds:   int32(claimTTLSeconds),
		candidateAttempts: config.CandidateAttempts,
	}, nil
}

func (service *Service) RunOnce(
	ctx context.Context,
	workerPoolID uuid.UUID,
) (Dispatch, bool, error) {
	if service == nil || service.pool == nil || service.coordinator == nil {
		return Dispatch{}, false, errors.New("scheduler is not configured")
	}
	if workerPoolID == uuid.Nil {
		return Dispatch{}, false, errors.New("worker pool identity is required")
	}
	queries := store.New(service.pool)
	for attempt := 0; attempt < service.candidateAttempts; attempt++ {
		claim, err := service.claimNext(ctx, workerPoolID)
		if errors.Is(err, pgx.ErrNoRows) {
			return Dispatch{}, false, nil
		}
		if err != nil {
			return Dispatch{}, false, fmt.Errorf("claim Scheduler dispatch: %w", err)
		}

		assignment, acquireErr := service.coordinator.Acquire(
			ctx,
			workercontrol.AuthenticatedWorker{ID: claim.WorkerID},
			claim.WorkerEpoch,
			&workercontrol.AssignmentCandidate{
				JobID:                      claim.JobID,
				ExpectedJobVersion:         claim.ExpectedJobVersion,
				ExecutionProfileRevisionID: claim.ExecutionProfileRevisionID,
				SchedulerClaim: &workercontrol.SchedulerClaim{
					IntentID:     claim.IntentID,
					WorkerPoolID: workerPoolID,
				},
			},
		)
		if acquireErr == nil &&
			(assignment.SchedulerDispatchIntentID != claim.IntentID ||
				assignment.JobID != claim.JobID ||
				assignment.WorkerID != claim.WorkerID ||
				assignment.WorkerEpoch != claim.WorkerEpoch ||
				assignment.ExecutionProfileRevisionID != claim.ExecutionProfileRevisionID) {
			acquireErr = &workercontrol.Failure{
				Code:    workercontrol.FailureCandidateUnavailable,
				Message: "Assignment receipt does not match the Scheduler dispatch claim",
			}
		}
		if acquireErr == nil {
			return Dispatch{
				IntentID:   claim.IntentID,
				Lane:       claim.Lane,
				Assignment: assignment,
			}, true, nil
		}

		if _, abandonErr := queries.AbandonSchedulerDispatch(
			ctx,
			store.AbandonSchedulerDispatchParams{
				IntentID:    claim.IntentID,
				SchedulerID: service.schedulerID,
				Reason:      boundedAbandonReason(acquireErr),
			},
		); abandonErr != nil {
			return Dispatch{}, false, fmt.Errorf(
				"acquire failed (%v) and Scheduler claim could not be abandoned: %w",
				acquireErr,
				abandonErr,
			)
		}
		var failure *workercontrol.Failure
		if !errors.As(acquireErr, &failure) ||
			(failure.Code != workercontrol.FailureCandidateUnavailable &&
				failure.Code != workercontrol.FailureWorkerUnavailable) {
			return Dispatch{}, false, fmt.Errorf("acquire scheduled Assignment: %w", acquireErr)
		}
	}
	return Dispatch{}, false, errors.New("scheduler exhausted candidate attempts")
}

func (service *Service) RunCycle(ctx context.Context) ([]Dispatch, error) {
	if service == nil || service.pool == nil || service.coordinator == nil {
		return nil, errors.New("scheduler is not configured")
	}
	workerPoolIDs, err := store.New(service.pool).ListSchedulableWorkerPools(ctx)
	if err != nil {
		return nil, fmt.Errorf("list schedulable Worker pools: %w", err)
	}

	dispatches := make([]Dispatch, 0)
	var poolErrors []error
	for _, workerPoolID := range workerPoolIDs {
		for {
			dispatch, ok, runErr := service.RunOnce(ctx, workerPoolID)
			if runErr != nil {
				if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
					return dispatches, runErr
				}
				poolErrors = append(poolErrors, fmt.Errorf(
					"run Scheduler for Worker pool %s: %w",
					workerPoolID,
					runErr,
				))
				break
			}
			if !ok {
				break
			}
			dispatches = append(dispatches, dispatch)
		}
	}
	return dispatches, errors.Join(poolErrors...)
}

func (service *Service) claimNext(
	ctx context.Context,
	workerPoolID uuid.UUID,
) (store.ClaimSchedulerDispatchRow, error) {
	return store.New(service.pool).ClaimSchedulerDispatch(
		ctx,
		store.ClaimSchedulerDispatchParams{
			WorkerPoolID:    workerPoolID,
			SchedulerID:     service.schedulerID,
			ClaimTtlSeconds: service.claimTTLSeconds,
		},
	)
}

func (service *Service) ReconcileExpired(ctx context.Context) (int64, error) {
	if service == nil || service.pool == nil {
		return 0, errors.New("scheduler is not configured")
	}
	count, err := store.New(service.pool).ReconcileExpiredSchedulerDispatches(ctx)
	if err != nil {
		return 0, fmt.Errorf("reconcile expired Scheduler dispatches: %w", err)
	}
	return count, nil
}

func boundedAbandonReason(err error) string {
	reason := "acquire_failed: " + err.Error()
	if len(reason) > 500 {
		return reason[:500]
	}
	return reason
}
