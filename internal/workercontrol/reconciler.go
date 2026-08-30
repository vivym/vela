package workercontrol

import (
	"context"
	"crypto/hmac"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	store "github.com/vivym/vela/internal/store/sqlc"
)

type AuthenticatedReconciler struct {
	ID string
}

type FinalizationTakeoverDecision string

const (
	FinalizationTakeoverGranted FinalizationTakeoverDecision = "FINALIZATION_TAKEOVER_GRANTED"
	FinalizationTakeoverNoWork  FinalizationTakeoverDecision = "NO_FINALIZATION_WORK"
)

type ReconcilerFinalizationCredentials struct {
	AttemptID   uuid.UUID
	WorkerID    uuid.UUID
	WorkerEpoch int64
	Fence       int64
	Token       string
}

type FinalizationTakeover struct {
	Decision       FinalizationTakeoverDecision
	LeaseID        uuid.UUID
	Credentials    ReconcilerFinalizationCredentials
	LeaseExpiresAt time.Time
	LeaseValidFor  time.Duration
	Plan           FinalizationPlan
}

type finalizationTakeoverCandidate struct {
	attemptID   uuid.UUID
	workerID    uuid.UUID
	workerEpoch int64
	fence       int64
	ownerKind   store.LeaseOwnerKind
	ownerID     string
	replay      bool
}

func (s *Service) ReconcileNextFinalization(
	ctx context.Context,
	reconciler AuthenticatedReconciler,
) (FinalizationTakeover, error) {
	if s == nil || s.pool == nil {
		return FinalizationTakeover{}, errors.New("worker coordinator is not configured")
	}
	if !validPrintableFailureText(reconciler.ID, 500) {
		return FinalizationTakeover{}, errors.New("authenticated Reconciler identity is invalid")
	}
	queries := store.New(s.pool)
	existing, err := queries.FindActiveReconcilerFinalizationCandidate(ctx, reconciler.ID)
	if err == nil {
		if !existing.WorkerID.Valid || existing.WorkerEpoch == nil {
			return FinalizationTakeover{}, errors.New("active Reconciler finalization is missing Worker binding")
		}
		return s.claimFinalizationForReconciler(ctx, reconciler, finalizationTakeoverCandidate{
			attemptID:   existing.AttemptID,
			workerID:    existing.WorkerID.UUID,
			workerEpoch: *existing.WorkerEpoch,
			fence:       existing.Fence,
			ownerKind:   store.LeaseOwnerKindRECONCILER,
			ownerID:     reconciler.ID,
			replay:      true,
		})
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return FinalizationTakeover{}, fmt.Errorf("find active Reconciler finalization: %w", err)
	}
	candidate, err := queries.FindRecoverableExpiredFinalizationCandidate(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return FinalizationTakeover{Decision: FinalizationTakeoverNoWork}, nil
	}
	if err != nil {
		return FinalizationTakeover{}, fmt.Errorf("find recoverable Worker finalization: %w", err)
	}
	if !candidate.WorkerID.Valid || candidate.WorkerEpoch == nil {
		return FinalizationTakeover{}, errors.New("recoverable Worker finalization is missing Worker binding")
	}
	return s.claimFinalizationForReconciler(ctx, reconciler, finalizationTakeoverCandidate{
		attemptID:   candidate.AttemptID,
		workerID:    candidate.WorkerID.UUID,
		workerEpoch: *candidate.WorkerEpoch,
		fence:       candidate.Fence,
		ownerKind:   candidate.OwnerKind,
		ownerID:     candidate.OwnerID,
	})
}

func (s *Service) claimFinalizationForReconciler(
	ctx context.Context,
	reconciler AuthenticatedReconciler,
	candidate finalizationTakeoverCandidate,
) (FinalizationTakeover, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return FinalizationTakeover{}, fmt.Errorf("begin Reconciler finalization transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)

	worker, err := queries.LockWorkerAuthority(ctx, candidate.workerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return FinalizationTakeover{Decision: FinalizationTakeoverNoWork}, nil
	}
	if err != nil {
		return FinalizationTakeover{}, fmt.Errorf("lock Worker for Reconciler finalization: %w", err)
	}
	if err := queries.LockExecutionLeaseWrites(ctx); err != nil {
		return FinalizationTakeover{}, fmt.Errorf("lock Lease writes for Reconciler finalization: %w", err)
	}
	authority, err := queries.LockFinalizationAuthority(ctx, store.LockFinalizationAuthorityParams{
		AttemptID:   candidate.attemptID,
		WorkerID:    candidate.workerID,
		WorkerEpoch: candidate.workerEpoch,
		Fence:       candidate.fence,
		OwnerKind:   candidate.ownerKind,
		OwnerID:     candidate.ownerID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return FinalizationTakeover{Decision: FinalizationTakeoverNoWork}, nil
	}
	if err != nil {
		return FinalizationTakeover{}, fmt.Errorf("lock Reconciler finalization authority: %w", err)
	}
	return s.finishFinalizationTakeover(ctx, tx, queries, worker, reconciler, authority, candidate.replay)
}

func (s *Service) finishFinalizationTakeover(
	ctx context.Context,
	tx pgx.Tx,
	queries *store.Queries,
	worker store.LockWorkerAuthorityRow,
	reconciler AuthenticatedReconciler,
	authority store.LockFinalizationAuthorityRow,
	replay bool,
) (FinalizationTakeover, error) {
	if !authority.WorkerID.Valid || authority.WorkerEpoch == nil ||
		authority.AttemptState != store.AttemptStateFINALIZING ||
		authority.JobState != store.JobStateFINALIZING ||
		authority.LeasePhase != store.LeasePhaseFINALIZATION ||
		authority.CurrentFence != authority.AttemptFence ||
		authority.LeaseRevokedAt.Valid ||
		authority.CreditReservationState != store.CreditReservationStateRESERVED ||
		!authority.FinalizationStartedAt.Valid || !authority.FinalizationDeadlineAt.Valid ||
		!authority.JobExpiresAt.Valid || !authority.LeaseIssuedAt.Valid ||
		!authority.LeaseExpiresAt.Valid {
		return FinalizationTakeover{Decision: FinalizationTakeoverNoWork}, nil
	}
	workerID := authority.WorkerID.UUID
	workerEpoch := *authority.WorkerEpoch
	now, err := postgresTime(ctx, queries)
	if err != nil {
		return FinalizationTakeover{}, err
	}
	if !authority.FinalizationDeadlineAt.Time.After(now) ||
		!authority.JobExpiresAt.Time.After(now) {
		return FinalizationTakeover{Decision: FinalizationTakeoverNoWork}, nil
	}
	if replay {
		if authority.LeaseOwnerKind != store.LeaseOwnerKindRECONCILER ||
			authority.LeaseOwnerID != reconciler.ID ||
			!authority.LeaseExpiresAt.Time.After(now) {
			return FinalizationTakeover{Decision: FinalizationTakeoverNoWork}, nil
		}
		return s.commitReconcilerFinalizationLease(
			ctx,
			tx,
			queries,
			authority,
			reconciler.ID,
			authority.LeaseID,
			authority.SigningKeyID,
			authority.LeaseIssuedAt.Time,
			authority.LeaseExpiresAt.Time,
			now,
			authority.TokenDigest,
		)
	}
	if (authority.LeaseOwnerKind != store.LeaseOwnerKindWORKER &&
		authority.LeaseOwnerKind != store.LeaseOwnerKindRECONCILER) ||
		authority.LeaseExpiresAt.Time.After(now) {
		return FinalizationTakeover{Decision: FinalizationTakeoverNoWork}, nil
	}
	takenOverAt := pgtype.Timestamptz{Time: now, Valid: true}
	if rows, revokeErr := queries.RevokeExpiredFinalizationLeaseForTakeover(
		ctx,
		store.RevokeExpiredFinalizationLeaseForTakeoverParams{
			TakenOverAt:       takenOverAt,
			LeaseID:           authority.LeaseID,
			AttemptID:         authority.AttemptID,
			WorkerID:          workerID,
			WorkerEpoch:       workerEpoch,
			Fence:             authority.AttemptFence,
			PreviousOwnerKind: authority.LeaseOwnerKind,
			PreviousOwnerID:   authority.LeaseOwnerID,
		},
	); revokeErr != nil || rows != 1 {
		return FinalizationTakeover{}, changedRowsError(
			"revoke expired Finalization Lease for takeover",
			rows,
			revokeErr,
		)
	}
	if authority.LeaseOwnerKind == store.LeaseOwnerKindWORKER && worker.Epoch == workerEpoch {
		if rows, updateErr := queries.MarkWorkerDrainingAfterFailure(
			ctx,
			store.MarkWorkerDrainingAfterFailureParams{
				DecidedAt:   takenOverAt,
				WorkerID:    workerID,
				WorkerEpoch: workerEpoch,
			},
		); updateErr != nil || rows != 1 {
			return FinalizationTakeover{}, changedRowsError(
				"drain Worker after Reconciler takeover",
				rows,
				updateErr,
			)
		}
	}
	expiresAt := now.Add(s.leaseTTL)
	if authority.FinalizationDeadlineAt.Time.Before(expiresAt) {
		expiresAt = authority.FinalizationDeadlineAt.Time
	}
	if authority.JobExpiresAt.Time.Before(expiresAt) {
		expiresAt = authority.JobExpiresAt.Time
	}
	leaseID := uuid.New()
	_, digest, err := s.issueLeaseToken(leaseTokenClaims{
		AttemptID: authority.AttemptID,
		WorkerID:  workerID,
		Epoch:     workerEpoch,
		Fence:     authority.AttemptFence,
		IssuedAt:  now,
		ExpiresAt: expiresAt,
	}, s.activeLeaseKeyID)
	if err != nil {
		return FinalizationTakeover{}, err
	}
	if err := queries.InsertReconcilerFinalizationLease(
		ctx,
		store.InsertReconcilerFinalizationLeaseParams{
			LeaseID:        leaseID,
			OrganizationID: authority.OrganizationID,
			ProjectID:      authority.ProjectID,
			AttemptID:      authority.AttemptID,
			WorkerID:       workerID,
			WorkerEpoch:    workerEpoch,
			OwnerID:        reconciler.ID,
			Fence:          authority.AttemptFence,
			TokenDigest:    digest,
			SigningKeyID:   s.activeLeaseKeyID,
			IssuedAt:       takenOverAt,
			ExpiresAt:      pgtype.Timestamptz{Time: expiresAt, Valid: true},
		},
	); err != nil {
		return FinalizationTakeover{}, fmt.Errorf("insert Reconciler Finalization Lease: %w", err)
	}
	return s.commitReconcilerFinalizationLease(
		ctx,
		tx,
		queries,
		authority,
		reconciler.ID,
		leaseID,
		s.activeLeaseKeyID,
		now,
		expiresAt,
		now,
		digest,
	)
}

func (s *Service) commitReconcilerFinalizationLease(
	ctx context.Context,
	tx pgx.Tx,
	queries *store.Queries,
	authority store.LockFinalizationAuthorityRow,
	reconcilerID string,
	leaseID uuid.UUID,
	keyID string,
	issuedAt time.Time,
	expiresAt time.Time,
	observedAt time.Time,
	storedDigest []byte,
) (FinalizationTakeover, error) {
	if !authority.WorkerID.Valid || authority.WorkerEpoch == nil {
		return FinalizationTakeover{}, errors.New("reconciler finalization authority is missing Worker binding")
	}
	workerID := authority.WorkerID.UUID
	workerEpoch := *authority.WorkerEpoch
	token, digest, err := s.issueLeaseToken(leaseTokenClaims{
		AttemptID: authority.AttemptID,
		WorkerID:  workerID,
		Epoch:     workerEpoch,
		Fence:     authority.AttemptFence,
		IssuedAt:  issuedAt,
		ExpiresAt: expiresAt,
	}, keyID)
	if err != nil {
		return FinalizationTakeover{}, err
	}
	if !hmac.Equal(digest, storedDigest) {
		return FinalizationTakeover{}, errors.New("stored Reconciler Lease digest does not match reconstructed token")
	}
	plan, err := finalizationPlan(ctx, queries, authority)
	if err != nil {
		return FinalizationTakeover{}, err
	}
	commitTime, err := postgresTime(ctx, queries)
	if err != nil {
		return FinalizationTakeover{}, err
	}
	if !expiresAt.After(commitTime) ||
		!authority.FinalizationDeadlineAt.Time.After(commitTime) ||
		!authority.JobExpiresAt.Time.After(commitTime) {
		return FinalizationTakeover{Decision: FinalizationTakeoverNoWork}, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return FinalizationTakeover{}, fmt.Errorf("commit Reconciler Finalization Lease: %w", err)
	}
	return FinalizationTakeover{
		Decision: FinalizationTakeoverGranted,
		LeaseID:  leaseID,
		Credentials: ReconcilerFinalizationCredentials{
			AttemptID:   authority.AttemptID,
			WorkerID:    workerID,
			WorkerEpoch: workerEpoch,
			Fence:       authority.AttemptFence,
			Token:       token,
		},
		LeaseExpiresAt: expiresAt,
		LeaseValidFor:  expiresAt.Sub(observedAt),
		Plan:           plan,
	}, nil
}
