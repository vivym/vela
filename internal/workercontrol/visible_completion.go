package workercontrol

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	store "github.com/vivym/vela/internal/store/sqlc"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type VisibleCompletionDecision string

const (
	VisibleCompletionCommitted          VisibleCompletionDecision = "VISIBLE_COMPLETION_COMMITTED"
	VisibleCompletionAlreadySucceeded   VisibleCompletionDecision = "ALREADY_SUCCEEDED"
	VisibleCompletionCancellationWon    VisibleCompletionDecision = "CANCELLATION_WON"
	VisibleCompletionAlreadyFailed      VisibleCompletionDecision = "ALREADY_FAILED"
	VisibleCompletionCandidateConflict  VisibleCompletionDecision = "CANDIDATE_CONFLICT"
	VisibleCompletionIncompleteArtifact VisibleCompletionDecision = "INCOMPLETE_ARTIFACTS"
	VisibleCompletionRejectedStaleLease VisibleCompletionDecision = "REJECTED_STALE_LEASE"
)

type VisibleCompletionCandidate struct {
	CompletionID       uuid.UUID
	ExpectedJobVersion int64
	ArtifactIDs        []uuid.UUID
}

type CommittedArtifact struct {
	ArtifactID      uuid.UUID
	Kind            ArtifactKind
	Ordinal         int32
	ObjectKey       string
	ObjectVersionID string
	SizeBytes       int64
	SHA256          [sha256.Size]byte
	ContentType     string
}

type VisibleCompletionResult struct {
	Decision       VisibleCompletionDecision
	CompletionID   uuid.UUID
	JobID          uuid.UUID
	AttemptID      uuid.UUID
	ArtifactSetID  uuid.UUID
	ChargeID       uuid.UUID
	JobVersion     int64
	ManifestSHA256 [sha256.Size]byte
	Artifacts      []CommittedArtifact
	CompletedAt    time.Time
}

type normalizedVisibleCompletionCandidate struct {
	completionID       uuid.UUID
	expectedJobVersion int64
	artifactIDs        []uuid.UUID
	hash               [sha256.Size]byte
}

type visibleCompletionCandidatePayload struct {
	ExpectedJobVersion int64    `json:"expected_job_version"`
	ArtifactIDs        []string `json:"artifact_ids"`
}

type visibleCompletionManifestItem struct {
	ArtifactID        string          `json:"artifact_id"`
	Kind              ArtifactKind    `json:"kind"`
	Ordinal           int32           `json:"ordinal"`
	ObjectKey         string          `json:"object_key"`
	ObjectVersionID   string          `json:"object_version_id"`
	SizeBytes         int64           `json:"size_bytes"`
	SHA256            string          `json:"sha256"`
	ContentType       string          `json:"content_type"`
	ValidationReceipt json.RawMessage `json:"validation_receipt"`
	VerifiedAt        string          `json:"verified_at"`
}

func (s *Service) CompleteVisibleCompletion(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
	candidate VisibleCompletionCandidate,
) (VisibleCompletionResult, error) {
	return s.completeVisibleCompletion(
		ctx,
		visibleCompletionActor{
			workerID:            worker.ID,
			ownerKind:           store.LeaseOwnerKindWORKER,
			requireCurrentEpoch: true,
			releaseWorker:       true,
		},
		credentials,
		candidate,
	)
}

func (s *Service) CompleteVisibleCompletionAsReconciler(
	ctx context.Context,
	reconciler AuthenticatedReconciler,
	credentials ReconcilerFinalizationCredentials,
	candidate VisibleCompletionCandidate,
) (VisibleCompletionResult, error) {
	if !validPrintableFailureText(reconciler.ID, 500) {
		return rejectedVisibleCompletion(), nil
	}
	return s.completeVisibleCompletion(
		ctx,
		visibleCompletionActor{
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
		candidate,
	)
}

type visibleCompletionActor struct {
	workerID            uuid.UUID
	ownerKind           store.LeaseOwnerKind
	ownerID             string
	requireCurrentEpoch bool
	releaseWorker       bool
}

type visibleCompletionAuthority struct {
	AttemptID                                        uuid.UUID
	JobID                                            uuid.UUID
	AttemptState                                     store.AttemptState
	AttemptFence                                     int64
	AttemptEndedAt                                   pgtype.Timestamptz
	FinalizationDeadlineAt                           pgtype.Timestamptz
	OrganizationID                                   uuid.UUID
	ProjectID                                        uuid.UUID
	WorkerPoolID                                     uuid.UUID
	JobState                                         store.JobState
	JobVersion                                       int64
	CurrentFence                                     int64
	JobExpiresAt                                     pgtype.Timestamptz
	RequestContentDeletedAt                          pgtype.Timestamptz
	RetentionArtifactDays                            int32
	RetentionPolicyRevision                          string
	GenerationCount                                  int32
	CreditReservationID                              uuid.UUID
	CreditReservationState                           store.CreditReservationState
	AmountMinor                                      int64
	Currency                                         string
	CompletionID                                     uuid.NullUUID
	CompletionAuthorityLeaseID                       uuid.NullUUID
	CompletionAuthorityStageGraphFinalizationClaimID uuid.NullUUID
	CandidateSha256                                  []byte
	ArtifactSetID                                    uuid.NullUUID
	ChargeID                                         uuid.NullUUID
	CompletionJobVersion                             *int64
	CompletedAt                                      pgtype.Timestamptz
	ManifestSha256                                   []byte
	RetentionExpiresAt                               pgtype.Timestamptz
}

type visibleCompletionLeaseCommit struct {
	leaseID       uuid.UUID
	workerID      uuid.UUID
	workerEpoch   int64
	fence         int64
	ownerKind     store.LeaseOwnerKind
	releaseWorker bool
}

type visibleCompletionStageGraphCommit struct {
	claimID uuid.UUID
	ownerID string
	fence   int64
}

type visibleCompletionCommitActor struct {
	lease            *visibleCompletionLeaseCommit
	stageGraph       *visibleCompletionStageGraphCommit
	prepareArtifacts func(context.Context, *store.Queries, time.Time) error
}

func leaseVisibleCompletionAuthority(
	row store.LockVisibleCompletionAuthorityRow,
) visibleCompletionAuthority {
	return visibleCompletionAuthority{
		AttemptID: row.AttemptID, JobID: row.JobID, AttemptState: row.AttemptState,
		AttemptFence: row.AttemptFence, AttemptEndedAt: row.AttemptEndedAt,
		FinalizationDeadlineAt: row.FinalizationDeadlineAt,
		OrganizationID:         row.OrganizationID, ProjectID: row.ProjectID,
		WorkerPoolID: row.WorkerPoolID, JobState: row.JobState,
		JobVersion: row.JobVersion, CurrentFence: row.CurrentFence,
		JobExpiresAt: row.JobExpiresAt, RequestContentDeletedAt: row.RequestContentDeletedAt,
		RetentionArtifactDays:   row.RetentionArtifactDays,
		RetentionPolicyRevision: row.RetentionPolicyRevision,
		GenerationCount:         row.GenerationCount, CreditReservationID: row.CreditReservationID,
		CreditReservationState: row.CreditReservationState,
		AmountMinor:            row.AmountMinor, Currency: row.Currency,
		CompletionID:               row.CompletionID,
		CompletionAuthorityLeaseID: row.CompletionAuthorityLeaseID,
		CandidateSha256:            row.CandidateSha256, ArtifactSetID: row.ArtifactSetID,
		ChargeID: row.ChargeID, CompletionJobVersion: row.CompletionJobVersion,
		CompletedAt: row.CompletedAt, ManifestSha256: row.ManifestSha256,
		RetentionExpiresAt: row.RetentionExpiresAt,
	}
}

func stageGraphVisibleCompletionAuthority(
	row store.LockStageGraphFinalizationCompletionAuthorityRow,
) visibleCompletionAuthority {
	return visibleCompletionAuthority{
		AttemptID: row.AttemptID, JobID: row.JobID, AttemptState: row.AttemptState,
		AttemptFence: row.AttemptFence, AttemptEndedAt: row.AttemptEndedAt,
		FinalizationDeadlineAt: row.FinalizationDeadlineAt,
		OrganizationID:         row.OrganizationID, ProjectID: row.ProjectID,
		WorkerPoolID: row.WorkerPoolID, JobState: row.JobState,
		JobVersion: row.JobVersion, CurrentFence: row.CurrentFence,
		JobExpiresAt: row.JobExpiresAt, RequestContentDeletedAt: row.RequestContentDeletedAt,
		RetentionArtifactDays:   row.RetentionArtifactDays,
		RetentionPolicyRevision: row.RetentionPolicyRevision,
		GenerationCount:         row.GenerationCount, CreditReservationID: row.CreditReservationID,
		CreditReservationState: row.CreditReservationState,
		AmountMinor:            row.AmountMinor, Currency: row.Currency,
		CompletionID:               row.CompletionID,
		CompletionAuthorityLeaseID: row.CompletionAuthorityLeaseID,
		CompletionAuthorityStageGraphFinalizationClaimID: row.CompletionAuthorityStageGraphFinalizationClaimID,
		CandidateSha256: row.CandidateSha256, ArtifactSetID: row.ArtifactSetID,
		ChargeID: row.ChargeID, CompletionJobVersion: row.CompletionJobVersion,
		CompletedAt: row.CompletedAt, ManifestSha256: row.ManifestSha256,
		RetentionExpiresAt: row.RetentionExpiresAt,
	}
}

func (s *Service) completeVisibleCompletion(
	ctx context.Context,
	actor visibleCompletionActor,
	credentials LeaseCredentials,
	candidate VisibleCompletionCandidate,
) (VisibleCompletionResult, error) {
	if s == nil || s.pool == nil {
		return VisibleCompletionResult{}, errors.New("worker coordinator is not configured")
	}
	normalized, valid := normalizeVisibleCompletionCandidate(candidate)
	if actor.workerID == uuid.Nil || credentials.AttemptID == uuid.Nil ||
		credentials.WorkerEpoch <= 0 || credentials.Fence <= 0 || credentials.Token == "" {
		return rejectedVisibleCompletion(), nil
	}
	if !valid {
		return VisibleCompletionResult{Decision: VisibleCompletionCandidateConflict}, nil
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("begin Visible Completion transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)

	workerRow, err := queries.LockWorkerAuthority(ctx, actor.workerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedVisibleCompletion(), nil
	}
	if err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("lock Worker for Visible Completion: %w", err)
	}
	if err := queries.LockExecutionLeaseWrites(ctx); err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("lock Lease writes for Visible Completion: %w", err)
	}
	ownerID := actor.ownerID
	if actor.ownerKind == store.LeaseOwnerKindWORKER {
		ownerID = workerRow.SpiffeID
	}
	leaseAuthority, err := queries.LockVisibleCompletionAuthority(
		ctx,
		store.LockVisibleCompletionAuthorityParams{
			AttemptID:   credentials.AttemptID,
			WorkerID:    actor.workerID,
			WorkerEpoch: credentials.WorkerEpoch,
			Fence:       credentials.Fence,
			OwnerKind:   actor.ownerKind,
			OwnerID:     ownerID,
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedVisibleCompletion(), nil
	}
	if err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("lock Visible Completion authority: %w", err)
	}
	presentedDigest := sha256.Sum256([]byte(credentials.Token))
	if (actor.requireCurrentEpoch && workerRow.Epoch != credentials.WorkerEpoch) ||
		leaseAuthority.AttemptFence != credentials.Fence ||
		!hmac.Equal(presentedDigest[:], leaseAuthority.TokenDigest) {
		return rejectedVisibleCompletion(), nil
	}
	authority := leaseVisibleCompletionAuthority(leaseAuthority)

	if authority.CompletionID.Valid {
		result, replayErr := committedVisibleCompletionResult(ctx, queries, authority)
		if replayErr != nil {
			return VisibleCompletionResult{}, replayErr
		}
		if !authority.CompletionAuthorityLeaseID.Valid ||
			authority.CompletionAuthorityStageGraphFinalizationClaimID.Valid {
			return VisibleCompletionResult{}, errors.New("visible Completion has no winning Lease identity")
		}
		switch {
		case authority.CompletionAuthorityLeaseID.UUID != leaseAuthority.LeaseID:
			result.Decision = VisibleCompletionAlreadySucceeded
		case authority.CompletionID.UUID == normalized.completionID &&
			hmac.Equal(authority.CandidateSha256, normalized.hash[:]):
			result.Decision = VisibleCompletionCommitted
		case authority.CompletionID.UUID == normalized.completionID:
			result.Decision = VisibleCompletionCandidateConflict
		default:
			result.Decision = VisibleCompletionAlreadySucceeded
		}
		if err := tx.Commit(ctx); err != nil {
			return VisibleCompletionResult{}, fmt.Errorf("commit Visible Completion replay: %w", err)
		}
		return result, nil
	}
	if authority.JobState == store.JobStateCANCELING ||
		authority.JobState == store.JobStateCANCELED ||
		authority.RequestContentDeletedAt.Valid {
		return VisibleCompletionResult{Decision: VisibleCompletionCancellationWon}, nil
	}
	if authority.JobState == store.JobStateFAILED {
		return VisibleCompletionResult{Decision: VisibleCompletionAlreadyFailed}, nil
	}
	if authority.CurrentFence != credentials.Fence ||
		authority.AttemptState != store.AttemptStateFINALIZING ||
		authority.JobState != store.JobStateFINALIZING ||
		leaseAuthority.LeaseRevokedAt.Valid ||
		authority.CreditReservationState != store.CreditReservationStateRESERVED ||
		authority.JobVersion != normalized.expectedJobVersion {
		return rejectedVisibleCompletion(), nil
	}

	now, err := postgresTime(ctx, queries)
	if err != nil {
		return VisibleCompletionResult{}, err
	}
	if !leaseAuthority.LeaseExpiresAt.Valid || !leaseAuthority.LeaseExpiresAt.Time.After(now) ||
		!authority.FinalizationDeadlineAt.Valid ||
		!authority.FinalizationDeadlineAt.Time.After(now) ||
		!authority.JobExpiresAt.Valid || !authority.JobExpiresAt.Time.After(now) {
		return rejectedVisibleCompletion(), nil
	}

	return commitVisibleCompletion(
		ctx, tx, queries, authority, normalized, now,
		visibleCompletionCommitActor{lease: &visibleCompletionLeaseCommit{
			leaseID: leaseAuthority.LeaseID, workerID: actor.workerID,
			workerEpoch: credentials.WorkerEpoch, fence: credentials.Fence,
			ownerKind: actor.ownerKind, releaseWorker: actor.releaseWorker,
		}},
	)
}

func commitVisibleCompletion(
	ctx context.Context,
	tx pgx.Tx,
	queries *store.Queries,
	authority visibleCompletionAuthority,
	normalized normalizedVisibleCompletionCandidate,
	now time.Time,
	actor visibleCompletionCommitActor,
) (VisibleCompletionResult, error) {
	if (actor.lease == nil) == (actor.stageGraph == nil) {
		return VisibleCompletionResult{}, errors.New("Visible Completion authority is ambiguous")
	}
	runningCount, err := queries.LockVisibleCompletionProject(
		ctx,
		store.LockVisibleCompletionProjectParams{
			OrganizationID: authority.OrganizationID,
			ProjectID:      authority.ProjectID,
		},
	)
	if err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("lock Project counters for Visible Completion: %w", err)
	}
	if runningCount <= 0 {
		return VisibleCompletionResult{}, errors.New("visible Completion Project running counter is empty")
	}
	if _, err := queries.LockVisibleCompletionPool(ctx, authority.WorkerPoolID); err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("lock pool for Visible Completion: %w", err)
	}
	reservation, err := queries.LockVisibleCompletionCreditReservation(
		ctx,
		store.LockVisibleCompletionCreditReservationParams{
			CreditReservationID: authority.CreditReservationID,
			JobID:               authority.JobID,
		},
	)
	if err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("lock CreditReservation for Visible Completion: %w", err)
	}
	credit, err := queries.LockVisibleCompletionOrganizationCredit(ctx, authority.OrganizationID)
	if err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("lock Organization credit for Visible Completion: %w", err)
	}
	if reservation.State != store.CreditReservationStateRESERVED ||
		reservation.AmountMinor != authority.AmountMinor ||
		reservation.Currency != authority.Currency || credit.Currency != reservation.Currency ||
		credit.ReservedMinor < reservation.AmountMinor {
		return VisibleCompletionResult{}, errors.New("visible Completion credit authority is inconsistent")
	}
	if actor.prepareArtifacts != nil {
		if err := actor.prepareArtifacts(ctx, queries, now); err != nil {
			return VisibleCompletionResult{}, err
		}
	}

	rows, err := queries.ListCompletionArtifactsForUpdate(
		ctx,
		store.ListCompletionArtifactsForUpdateParams{
			AttemptID:    authority.AttemptID,
			AttemptFence: authority.AttemptFence,
		},
	)
	if err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("lock candidate Artifacts: %w", err)
	}
	artifacts, _, manifestHash, complete := completionManifest(
		authority,
		normalized.artifactIDs,
		rows,
	)
	if !complete {
		return VisibleCompletionResult{Decision: VisibleCompletionIncompleteArtifact}, nil
	}

	artifactSetID := uuid.New()
	chargeID := uuid.New()
	accessGrantID := uuid.New()
	completedAt := pgtype.Timestamptz{Time: now, Valid: true}
	if authority.RetentionArtifactDays != 7 && authority.RetentionArtifactDays != 30 &&
		authority.RetentionArtifactDays != 90 || authority.RetentionPolicyRevision == "" {
		return VisibleCompletionResult{}, errors.New("job Retention Policy snapshot is invalid")
	}
	retentionExpiresAt := pgtype.Timestamptz{
		Time:  now.Add(time.Duration(authority.RetentionArtifactDays) * 24 * time.Hour),
		Valid: true,
	}
	nextJobVersion := authority.JobVersion + 1
	if err := queries.InsertArtifactSet(ctx, store.InsertArtifactSetParams{
		ID: artifactSetID, OrganizationID: authority.OrganizationID,
		ProjectID: authority.ProjectID, JobID: authority.JobID,
		AttemptID: authority.AttemptID, AttemptFence: authority.AttemptFence,
		ManifestSha256: manifestHash[:], RetentionPolicyRevision: authority.RetentionPolicyRevision,
		RetentionExpiresAt: retentionExpiresAt, CommittedAt: completedAt,
	}); err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("insert ArtifactSet: %w", err)
	}
	for _, row := range rows {
		if err := insertArtifactSetItem(ctx, queries, authority, artifactSetID, row); err != nil {
			return VisibleCompletionResult{}, err
		}
		if changed, updateErr := queries.MarkArtifactCommitted(ctx, store.MarkArtifactCommittedParams{
			RetentionExpiresAt: retentionExpiresAt, CommittedAt: completedAt,
			ArtifactID: row.ID, AttemptID: authority.AttemptID,
			AttemptFence: authority.AttemptFence,
		}); updateErr != nil || changed != 1 {
			return VisibleCompletionResult{}, changedRowsError("commit Artifact", changed, updateErr)
		}
	}
	if err := queries.InsertVisibleCompletionCharge(ctx, store.InsertVisibleCompletionChargeParams{
		ID: chargeID, OrganizationID: authority.OrganizationID,
		ProjectID: authority.ProjectID, JobID: authority.JobID,
		CreditReservationID: reservation.ID,
		ArtifactSetID:       uuid.NullUUID{UUID: artifactSetID, Valid: true},
		AmountMinor:         reservation.AmountMinor, Currency: reservation.Currency,
		PostedAt: completedAt,
	}); err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("insert Visible Completion Charge: %w", err)
	}
	if err := queries.InsertArtifactAccessGrant(ctx, store.InsertArtifactAccessGrantParams{
		ID: accessGrantID, OrganizationID: authority.OrganizationID,
		ProjectID: authority.ProjectID, JobID: authority.JobID,
		ArtifactSetID: artifactSetID, EligibleAt: completedAt,
		RetentionExpiresAt: retentionExpiresAt,
	}); err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("insert Artifact access eligibility: %w", err)
	}
	if actor.lease != nil {
		if err := queries.InsertVisibleCompletion(ctx, store.InsertVisibleCompletionParams{
			ID: normalized.completionID, OrganizationID: authority.OrganizationID,
			ProjectID: authority.ProjectID, JobID: authority.JobID,
			AttemptID: authority.AttemptID, AttemptFence: authority.AttemptFence,
			AuthorityLeaseID: uuid.NullUUID{UUID: actor.lease.leaseID, Valid: true},
			ArtifactSetID:    artifactSetID, ChargeID: chargeID,
			CandidateSha256: normalized.hash[:], JobVersion: nextJobVersion,
			CompletedAt: completedAt,
		}); err != nil {
			return VisibleCompletionResult{}, fmt.Errorf("insert lease Visible Completion: %w", err)
		}
	} else {
		if err := queries.InsertStageGraphVisibleCompletion(
			ctx,
			store.InsertStageGraphVisibleCompletionParams{
				ID: normalized.completionID, OrganizationID: authority.OrganizationID,
				ProjectID: authority.ProjectID, JobID: authority.JobID,
				AttemptID: authority.AttemptID, AttemptFence: authority.AttemptFence,
				AuthorityStageGraphFinalizationClaimID: uuid.NullUUID{
					UUID: actor.stageGraph.claimID, Valid: true,
				},
				ArtifactSetID: artifactSetID, ChargeID: chargeID,
				CandidateSha256: normalized.hash[:], JobVersion: nextJobVersion,
				CompletedAt: completedAt,
			},
		); err != nil {
			return VisibleCompletionResult{}, fmt.Errorf(
				"insert Stage graph Visible Completion: %w", err,
			)
		}
	}
	if actor.lease != nil {
		if changed, updateErr := queries.MarkCompletionAttemptSucceeded(
			ctx,
			store.MarkCompletionAttemptSucceededParams{
				CompletedAt: completedAt, AttemptID: authority.AttemptID,
				WorkerID:    uuid.NullUUID{UUID: actor.lease.workerID, Valid: true},
				WorkerEpoch: &actor.lease.workerEpoch, Fence: actor.lease.fence,
			},
		); updateErr != nil || changed != 1 {
			return VisibleCompletionResult{}, changedRowsError(
				"terminalize successful Attempt", changed, updateErr,
			)
		}
		if changed, updateErr := queries.RevokeCompletionLease(ctx, store.RevokeCompletionLeaseParams{
			CompletedAt: completedAt, LeaseID: actor.lease.leaseID,
			AttemptID: authority.AttemptID, WorkerID: actor.lease.workerID,
			WorkerEpoch: actor.lease.workerEpoch, Fence: actor.lease.fence,
			OwnerKind: actor.lease.ownerKind,
		}); updateErr != nil || changed != 1 {
			return VisibleCompletionResult{}, changedRowsError(
				"revoke successful Finalization Lease", changed, updateErr,
			)
		}
		if actor.lease.releaseWorker {
			if changed, updateErr := queries.ReleaseWorkerAfterVisibleCompletion(
				ctx,
				store.ReleaseWorkerAfterVisibleCompletionParams{
					CompletedAt: completedAt, WorkerID: actor.lease.workerID,
					WorkerEpoch: actor.lease.workerEpoch,
				},
			); updateErr != nil || changed != 1 {
				return VisibleCompletionResult{}, changedRowsError(
					"release Worker after Visible Completion", changed, updateErr,
				)
			}
		}
	} else {
		if changed, updateErr := queries.CompleteStageGraphAttemptForVisibleCompletion(
			ctx,
			store.CompleteStageGraphAttemptForVisibleCompletionParams{
				CompletedAt: completedAt, AttemptID: authority.AttemptID,
				Fence: actor.stageGraph.fence,
			},
		); updateErr != nil || !changed {
			if updateErr != nil {
				return VisibleCompletionResult{}, fmt.Errorf(
					"terminalize successful Stage graph Attempt: %w", updateErr,
				)
			}
			return VisibleCompletionResult{}, errors.New(
				"terminalize successful Stage graph Attempt changed no rows",
			)
		}
		if changed, updateErr := queries.CompleteStageGraphFinalizationClaim(
			ctx,
			store.CompleteStageGraphFinalizationClaimParams{
				CompletedAt: completedAt, ClaimID: actor.stageGraph.claimID,
				AttemptID: authority.AttemptID, AttemptFence: authority.AttemptFence,
				OwnerID: actor.stageGraph.ownerID,
			},
		); updateErr != nil || changed != 1 {
			return VisibleCompletionResult{}, changedRowsError(
				"complete Stage graph Finalization claim", changed, updateErr,
			)
		}
	}
	if changed, updateErr := queries.DecrementProjectRunningForVisibleCompletion(
		ctx,
		store.DecrementProjectRunningForVisibleCompletionParams{
			OrganizationID: authority.OrganizationID, ProjectID: authority.ProjectID,
		},
	); updateErr != nil || changed != 1 {
		return VisibleCompletionResult{}, changedRowsError(
			"decrement Project running counter for Visible Completion", changed, updateErr,
		)
	}
	if changed, updateErr := queries.ConsumeVisibleCompletionCreditReservation(
		ctx,
		store.ConsumeVisibleCompletionCreditReservationParams{
			CompletedAt: completedAt, CreditReservationID: reservation.ID,
			JobID: authority.JobID,
		},
	); updateErr != nil || changed != 1 {
		return VisibleCompletionResult{}, changedRowsError(
			"consume Visible Completion CreditReservation", changed, updateErr,
		)
	}
	if changed, updateErr := queries.PostVisibleCompletionOrganizationCredit(
		ctx,
		store.PostVisibleCompletionOrganizationCreditParams{
			AmountMinor: reservation.AmountMinor, CompletedAt: completedAt,
			OrganizationID: authority.OrganizationID, Currency: reservation.Currency,
		},
	); updateErr != nil || changed != 1 {
		return VisibleCompletionResult{}, changedRowsError(
			"post Visible Completion Organization credit", changed, updateErr,
		)
	}
	if err := insertVisibleCompletionEvents(
		ctx, queries, authority, artifactSetID, chargeID, manifestHash,
		artifacts, nextJobVersion, now,
	); err != nil {
		return VisibleCompletionResult{}, err
	}
	jobVersion, err := queries.MarkJobSucceeded(ctx, store.MarkJobSucceededParams{
		ArtifactSetID: uuid.NullUUID{UUID: artifactSetID, Valid: true},
		CompletedAt:   completedAt, JobID: authority.JobID,
		ExpectedVersion: authority.JobVersion, Fence: authority.AttemptFence,
	})
	if err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("mark Job SUCCEEDED: %w", err)
	}
	if jobVersion != nextJobVersion {
		return VisibleCompletionResult{}, fmt.Errorf(
			"committed Job version %d, want %d", jobVersion, nextJobVersion,
		)
	}
	if err := tx.Commit(ctx); err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("commit Visible Completion: %w", err)
	}
	return VisibleCompletionResult{
		Decision: VisibleCompletionCommitted, CompletionID: normalized.completionID,
		JobID: authority.JobID, AttemptID: authority.AttemptID,
		ArtifactSetID: artifactSetID, ChargeID: chargeID, JobVersion: jobVersion,
		ManifestSHA256: manifestHash, Artifacts: artifacts, CompletedAt: now,
	}, nil
}

func normalizeVisibleCompletionCandidate(
	candidate VisibleCompletionCandidate,
) (normalizedVisibleCompletionCandidate, bool) {
	if candidate.CompletionID == uuid.Nil || candidate.ExpectedJobVersion <= 0 ||
		len(candidate.ArtifactIDs) == 0 || len(candidate.ArtifactIDs) > 10_000 {
		return normalizedVisibleCompletionCandidate{}, false
	}
	ids := append([]uuid.UUID(nil), candidate.ArtifactIDs...)
	sort.Slice(ids, func(left, right int) bool {
		return ids[left].String() < ids[right].String()
	})
	encodedIDs := make([]string, 0, len(ids))
	for index, id := range ids {
		if id == uuid.Nil || (index > 0 && id == ids[index-1]) {
			return normalizedVisibleCompletionCandidate{}, false
		}
		encodedIDs = append(encodedIDs, id.String())
	}
	payload, err := json.Marshal(visibleCompletionCandidatePayload{
		ExpectedJobVersion: candidate.ExpectedJobVersion,
		ArtifactIDs:        encodedIDs,
	})
	if err != nil {
		return normalizedVisibleCompletionCandidate{}, false
	}
	return normalizedVisibleCompletionCandidate{
		completionID:       candidate.CompletionID,
		expectedJobVersion: candidate.ExpectedJobVersion,
		artifactIDs:        ids,
		hash:               sha256.Sum256(payload),
	}, true
}

func completionManifest(
	authority visibleCompletionAuthority,
	candidateArtifactIDs []uuid.UUID,
	rows []store.ListCompletionArtifactsForUpdateRow,
) ([]CommittedArtifact, []byte, [sha256.Size]byte, bool) {
	expectedCount := int(authority.GenerationCount) * 2
	if expectedCount <= 0 || len(rows) != expectedCount || len(candidateArtifactIDs) != expectedCount {
		return nil, nil, [sha256.Size]byte{}, false
	}
	rowIDs := make([]uuid.UUID, 0, len(rows))
	artifacts := make([]CommittedArtifact, 0, len(rows))
	manifestItems := make([]visibleCompletionManifestItem, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if row.State != store.ArtifactStateVERIFIED || row.ObjectVersionID == nil ||
			row.SizeBytes == nil || row.ContentType == nil || len(row.Sha256) != sha256.Size ||
			len(row.ValidationReceipt) == 0 || !row.VerifiedAt.Valid ||
			row.OrganizationID != authority.OrganizationID || row.ProjectID != authority.ProjectID ||
			row.JobID != authority.JobID || row.AttemptID != authority.AttemptID ||
			row.AttemptFence != authority.AttemptFence {
			return nil, nil, [sha256.Size]byte{}, false
		}
		identity := fmt.Sprintf("%s/%d", row.Kind, row.Ordinal)
		if _, duplicate := seen[identity]; duplicate {
			return nil, nil, [sha256.Size]byte{}, false
		}
		seen[identity] = struct{}{}
		var digest [sha256.Size]byte
		copy(digest[:], row.Sha256)
		artifacts = append(artifacts, CommittedArtifact{
			ArtifactID:      row.ID,
			Kind:            ArtifactKind(row.Kind),
			Ordinal:         row.Ordinal,
			ObjectKey:       row.ObjectKey,
			ObjectVersionID: *row.ObjectVersionID,
			SizeBytes:       *row.SizeBytes,
			SHA256:          digest,
			ContentType:     *row.ContentType,
		})
		manifestItems = append(manifestItems, visibleCompletionManifestItem{
			ArtifactID:        row.ID.String(),
			Kind:              ArtifactKind(row.Kind),
			Ordinal:           row.Ordinal,
			ObjectKey:         row.ObjectKey,
			ObjectVersionID:   *row.ObjectVersionID,
			SizeBytes:         *row.SizeBytes,
			SHA256:            hex.EncodeToString(row.Sha256),
			ContentType:       *row.ContentType,
			ValidationReceipt: append(json.RawMessage(nil), row.ValidationReceipt...),
			VerifiedAt:        row.VerifiedAt.Time.UTC().Format(time.RFC3339Nano),
		})
		rowIDs = append(rowIDs, row.ID)
	}
	sort.Slice(rowIDs, func(left, right int) bool {
		return rowIDs[left].String() < rowIDs[right].String()
	})
	for index := range rowIDs {
		if rowIDs[index] != candidateArtifactIDs[index] {
			return nil, nil, [sha256.Size]byte{}, false
		}
	}
	for ordinal := int32(0); ordinal < authority.GenerationCount; ordinal++ {
		if _, ok := seen[fmt.Sprintf("VIDEO/%d", ordinal)]; !ok {
			return nil, nil, [sha256.Size]byte{}, false
		}
		if _, ok := seen[fmt.Sprintf("THUMBNAIL/%d", ordinal)]; !ok {
			return nil, nil, [sha256.Size]byte{}, false
		}
	}
	manifest, err := json.Marshal(manifestItems)
	if err != nil {
		return nil, nil, [sha256.Size]byte{}, false
	}
	return artifacts, manifest, sha256.Sum256(manifest), true
}

func insertArtifactSetItem(
	ctx context.Context,
	queries *store.Queries,
	authority visibleCompletionAuthority,
	artifactSetID uuid.UUID,
	row store.ListCompletionArtifactsForUpdateRow,
) error {
	if row.ObjectVersionID == nil || row.SizeBytes == nil || row.ContentType == nil {
		return errors.New("verified Artifact snapshot is incomplete")
	}
	if err := queries.InsertArtifactSetItem(ctx, store.InsertArtifactSetItemParams{
		OrganizationID:    authority.OrganizationID,
		ProjectID:         authority.ProjectID,
		JobID:             authority.JobID,
		AttemptID:         authority.AttemptID,
		AttemptFence:      authority.AttemptFence,
		ArtifactSetID:     artifactSetID,
		ArtifactID:        row.ID,
		Kind:              row.Kind,
		Ordinal:           row.Ordinal,
		ObjectKey:         row.ObjectKey,
		ObjectVersionID:   *row.ObjectVersionID,
		SizeBytes:         *row.SizeBytes,
		Sha256:            row.Sha256,
		ContentType:       *row.ContentType,
		ValidationReceipt: row.ValidationReceipt,
		VerifiedAt:        row.VerifiedAt,
	}); err != nil {
		return fmt.Errorf("insert ArtifactSet item: %w", err)
	}
	return nil
}

func insertVisibleCompletionEvents(
	ctx context.Context,
	queries *store.Queries,
	authority visibleCompletionAuthority,
	artifactSetID uuid.UUID,
	chargeID uuid.UUID,
	manifestHash [sha256.Size]byte,
	artifacts []CommittedArtifact,
	jobVersion int64,
	completedAt time.Time,
) error {
	artifactSnapshots := make([]*velav1.ArtifactSnapshot, 0, len(artifacts))
	for _, artifact := range artifacts {
		artifactSnapshots = append(artifactSnapshots, &velav1.ArtifactSnapshot{
			ArtifactId:      artifact.ArtifactID.String(),
			Kind:            string(artifact.Kind),
			Ordinal:         uint32(artifact.Ordinal),
			ObjectKey:       artifact.ObjectKey,
			ObjectVersionId: artifact.ObjectVersionID,
			SizeBytes:       uint64(artifact.SizeBytes),
			Sha256:          append([]byte(nil), artifact.SHA256[:]...),
			ContentType:     artifact.ContentType,
		})
	}
	events := []struct {
		eventType string
		payload   func(*velav1.EventEnvelope)
	}{
		{
			eventType: "job.succeeded",
			payload: func(event *velav1.EventEnvelope) {
				event.Payload = &velav1.EventEnvelope_JobSucceeded{JobSucceeded: &velav1.JobSucceeded{
					OrganizationId: authority.OrganizationID.String(),
					ProjectId:      authority.ProjectID.String(),
					JobId:          authority.JobID.String(),
					AttemptId:      authority.AttemptID.String(),
					AttemptFence:   uint64(authority.AttemptFence),
					ArtifactSetId:  artifactSetID.String(),
					ManifestSha256: append([]byte(nil), manifestHash[:]...),
					ChargeId:       chargeID.String(),
					Artifacts:      artifactSnapshots,
					CompletedAt:    timestamppb.New(completedAt),
				}}
			},
		},
		{
			eventType: "charge.posted",
			payload: func(event *velav1.EventEnvelope) {
				event.Payload = &velav1.EventEnvelope_ChargePosted{ChargePosted: &velav1.ChargePosted{
					OrganizationId: authority.OrganizationID.String(),
					ProjectId:      authority.ProjectID.String(),
					JobId:          authority.JobID.String(),
					ChargeId:       chargeID.String(),
					AmountMinor:    uint64(authority.AmountMinor),
					Currency:       authority.Currency,
					Reason:         "VISIBLE_COMPLETION",
					PostedAt:       timestamppb.New(completedAt),
				}}
			},
		},
		{
			eventType: "invoice.export_requested",
			payload: func(event *velav1.EventEnvelope) {
				event.Payload = &velav1.EventEnvelope_InvoiceExportRequested{
					InvoiceExportRequested: &velav1.InvoiceExportRequested{
						OrganizationId: authority.OrganizationID.String(),
						ProjectId:      authority.ProjectID.String(),
						JobId:          authority.JobID.String(),
						ChargeId:       chargeID.String(),
						RequestedAt:    timestamppb.New(completedAt),
					},
				}
			},
		},
	}
	for _, definition := range events {
		eventID := uuid.New()
		event := &velav1.EventEnvelope{
			EventId:          eventID.String(),
			AggregateType:    "Job",
			AggregateId:      authority.JobID.String(),
			AggregateVersion: uint64(jobVersion),
			EventType:        definition.eventType,
			SchemaVersion:    1,
			OccurredAt:       timestamppb.New(completedAt),
		}
		definition.payload(event)
		payload, err := proto.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal %s event: %w", definition.eventType, err)
		}
		if err := queries.InsertVisibleCompletionOutboxEvent(
			ctx,
			store.InsertVisibleCompletionOutboxEventParams{
				EventID:        eventID,
				OrganizationID: authority.OrganizationID,
				ProjectID:      authority.ProjectID,
				JobID:          authority.JobID,
				JobVersion:     jobVersion,
				EventType:      definition.eventType,
				Payload:        payload,
				OccurredAt:     pgtype.Timestamptz{Time: completedAt, Valid: true},
			},
		); err != nil {
			return fmt.Errorf("insert %s Outbox event: %w", definition.eventType, err)
		}
	}
	return nil
}

func committedVisibleCompletionResult(
	ctx context.Context,
	queries *store.Queries,
	authority visibleCompletionAuthority,
) (VisibleCompletionResult, error) {
	if !authority.CompletionID.Valid || !authority.ArtifactSetID.Valid ||
		!authority.ChargeID.Valid || authority.CompletionJobVersion == nil ||
		!authority.CompletedAt.Valid || len(authority.ManifestSha256) != sha256.Size ||
		authority.JobState != store.JobStateSUCCEEDED ||
		authority.AttemptState != store.AttemptStateSUCCEEDED || !authority.AttemptEndedAt.Valid {
		return VisibleCompletionResult{}, errors.New("committed Visible Completion is incomplete")
	}
	rows, err := queries.ListCommittedArtifactSetItems(ctx, authority.ArtifactSetID.UUID)
	if err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("list committed ArtifactSet items: %w", err)
	}
	artifacts := make([]CommittedArtifact, 0, len(rows))
	for _, row := range rows {
		if len(row.Sha256) != sha256.Size {
			return VisibleCompletionResult{}, errors.New("committed ArtifactSet digest is invalid")
		}
		var digest [sha256.Size]byte
		copy(digest[:], row.Sha256)
		artifacts = append(artifacts, CommittedArtifact{
			ArtifactID:      row.ArtifactID,
			Kind:            ArtifactKind(row.Kind),
			Ordinal:         row.Ordinal,
			ObjectKey:       row.ObjectKey,
			ObjectVersionID: row.ObjectVersionID,
			SizeBytes:       row.SizeBytes,
			SHA256:          digest,
			ContentType:     row.ContentType,
		})
	}
	var manifestHash [sha256.Size]byte
	copy(manifestHash[:], authority.ManifestSha256)
	return VisibleCompletionResult{
		CompletionID:   authority.CompletionID.UUID,
		JobID:          authority.JobID,
		AttemptID:      authority.AttemptID,
		ArtifactSetID:  authority.ArtifactSetID.UUID,
		ChargeID:       authority.ChargeID.UUID,
		JobVersion:     *authority.CompletionJobVersion,
		ManifestSHA256: manifestHash,
		Artifacts:      artifacts,
		CompletedAt:    authority.CompletedAt.Time,
	}, nil
}

func rejectedVisibleCompletion() VisibleCompletionResult {
	return VisibleCompletionResult{Decision: VisibleCompletionRejectedStaleLease}
}
