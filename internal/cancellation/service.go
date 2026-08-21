package cancellation

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/identity"
	store "github.com/vivym/vela/internal/store/sqlc"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Decision string

const (
	DecisionCanceled         Decision = "CANCELED"
	DecisionCanceling        Decision = "CANCELING"
	DecisionAlreadySucceeded Decision = "ALREADY_SUCCEEDED"
	DecisionAlreadyFailed    Decision = "ALREADY_FAILED"
)

type Charge struct {
	ID          uuid.UUID
	AmountMinor int64
	Currency    string
	Reason      string
	PostedAt    time.Time
}

type Result struct {
	CancellationID uuid.UUID
	JobID          uuid.UUID
	Decision       Decision
	State          string
	JobVersion     int64
	Billable       bool
	Charge         *Charge
	DecidedAt      time.Time
}

type FailureCode string

const (
	FailureForbidden    FailureCode = "forbidden"
	FailureUnauthorized FailureCode = "unauthorized"
	FailureNotFound     FailureCode = "not_found"
	FailureUnavailable  FailureCode = "cancellation_unavailable"
)

type Failure struct {
	Code    FailureCode
	Message string
}

func (e *Failure) Error() string {
	return string(e.Code) + ": " + e.Message
}

type Service struct {
	pool         *pgxpool.Pool
	internalPool *pgxpool.Pool
}

func NewService(pool, internalPool *pgxpool.Pool) *Service {
	return &Service{pool: pool, internalPool: internalPool}
}

type StopDecision string

const (
	StopAcknowledged           StopDecision = "ACKNOWLEDGED"
	StopReconciled             StopDecision = "RECONCILED"
	StopNoWork                 StopDecision = "NO_WORK"
	StopRejectedStaleAuthority StopDecision = "REJECTED_STALE_AUTHORITY"
)

type StopResult struct {
	Decision       StopDecision
	ReceiptID      uuid.UUID
	CancellationID uuid.UUID
	JobID          uuid.UUID
	State          string
	JobVersion     int64
	Source         string
	StoppedAt      time.Time
}

func (s *Service) Cancel(
	ctx context.Context,
	principal identity.Principal,
	projectID, jobID uuid.UUID,
) (Result, error) {
	if s == nil || s.pool == nil {
		return Result{}, errors.New("cancellation service is not configured")
	}
	if projectID == uuid.Nil || jobID == uuid.Nil ||
		principal.CredentialID == uuid.Nil || principal.ProjectID != projectID {
		return Result{}, &Failure{Code: FailureForbidden, Message: "Job is outside the authenticated Project"}
	}
	var lastErr error
	for range 3 {
		result, err := s.cancelOnce(ctx, principal, projectID, jobID)
		if err == nil || !retryableCancellationTransaction(err) {
			return result, err
		}
		lastErr = err
	}
	return Result{}, fmt.Errorf("cancellation transaction did not stabilize: %w", lastErr)
}

func (s *Service) cancelOnce(
	ctx context.Context,
	principal identity.Principal,
	projectID, jobID uuid.UUID,
) (Result, error) {
	if s == nil || s.pool == nil {
		return Result{}, errors.New("cancellation service is not configured")
	}
	if projectID == uuid.Nil || jobID == uuid.Nil ||
		principal.CredentialID == uuid.Nil || principal.ProjectID != projectID {
		return Result{}, &Failure{Code: FailureForbidden, Message: "Job is outside the authenticated Project"}
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Result{}, fmt.Errorf("begin cancellation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	credentialProof := principal.RequestContextProof()
	defer clear(credentialProof)
	requestContext, err := queries.SetCancellationRequestContext(ctx, store.SetCancellationRequestContextParams{
		CredentialID:    principal.CredentialID,
		CredentialProof: credentialProof,
	})
	if err != nil {
		return Result{}, cancellationFailure(err)
	}
	if requestContext.OrganizationID != principal.OrganizationID ||
		requestContext.ProjectID != projectID ||
		requestContext.PrincipalID != principal.PrincipalID {
		return Result{}, &Failure{Code: FailureForbidden, Message: "credential request context changed"}
	}

	row, err := queries.CancelJob(ctx, store.CancelJobParams{
		JobID:                  jobID,
		CancellationID:         uuid.New(),
		ChargeID:               uuid.New(),
		CancelRequestedEventID: uuid.New(),
		CancelingEventID:       uuid.New(),
		CanceledEventID:        uuid.New(),
		ChargePostedEventID:    uuid.New(),
		InvoiceExportEventID:   uuid.New(),
	})
	if err != nil {
		return Result{}, cancellationFailure(err)
	}
	if !row.DecidedAt.Valid {
		return Result{}, errors.New("cancellation decision has no decision time")
	}
	if row.Billable && (row.ChargeID == uuid.Nil || row.ChargeAmountMinor < 0 ||
		row.ChargeCurrency == "" || row.ChargeReason == "" || !row.ChargePostedAt.Valid) {
		return Result{}, errors.New("billable cancellation decision has no immutable Charge")
	}
	if row.AttemptID != uuid.Nil && (row.AuthorityLeaseID == uuid.Nil ||
		row.AuthorityLeasePhase == "" || row.AuthorityLeaseOwnerKind == "" ||
		row.AuthorityLeaseOwnerID == "" || !row.AuthorityLeaseExpiresAt.Valid) {
		return Result{}, errors.New("active cancellation decision has incomplete Lease authority")
	}
	result := resultFromRow(row)
	if err := tx.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("commit cancellation: %w", err)
	}
	return result, nil
}

func (s *Service) AcknowledgeCancellationStop(
	ctx context.Context,
	worker workercontrol.AuthenticatedWorker,
	credentials workercontrol.LeaseCredentials,
	cancellationID uuid.UUID,
) (StopResult, error) {
	if s == nil || s.internalPool == nil {
		return StopResult{}, errors.New("cancellation stop coordinator is not configured")
	}
	if worker.ID == uuid.Nil || credentials.AttemptID == uuid.Nil ||
		credentials.WorkerEpoch <= 0 || credentials.Fence <= 0 ||
		credentials.Token == "" || cancellationID == uuid.Nil {
		return rejectedStaleStop(), nil
	}

	tx, err := s.internalPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return StopResult{}, fmt.Errorf("begin cancellation stop acknowledgement: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	authorityLeaseID, err := queries.GetCancellationStopLeaseID(
		ctx,
		store.GetCancellationStopLeaseIDParams{
			CancellationID: cancellationID,
			AttemptID:      credentials.AttemptID,
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedStaleStop(), nil
	}
	if err != nil {
		return StopResult{}, fmt.Errorf("discover cancellation stop Lease: %w", err)
	}

	workerRow, err := queries.LockWorkerAuthority(ctx, worker.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedStaleStop(), nil
	}
	if err != nil {
		return StopResult{}, fmt.Errorf("lock Worker for cancellation stop: %w", err)
	}
	if err := queries.LockExecutionLeaseWrites(ctx); err != nil {
		return StopResult{}, fmt.Errorf("lock execution Lease writes for cancellation stop: %w", err)
	}
	lease, err := queries.LockCancellationLease(ctx, store.LockCancellationLeaseParams{
		AuthorityLeaseID: authorityLeaseID,
		AttemptID:        credentials.AttemptID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedStaleStop(), nil
	}
	if err != nil {
		return StopResult{}, fmt.Errorf("lock cancellation Lease: %w", err)
	}
	attempt, err := queries.LockCancellationAttempt(ctx, credentials.AttemptID)
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedStaleStop(), nil
	}
	if err != nil {
		return StopResult{}, fmt.Errorf("lock canceled Attempt: %w", err)
	}
	authority, err := queries.LockCancellationStopAuthority(ctx, cancellationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedStaleStop(), nil
	}
	if err != nil {
		return StopResult{}, fmt.Errorf("lock cancellation stop authority: %w", err)
	}

	presentedDigest := sha256.Sum256([]byte(credentials.Token))
	if workerRow.ID != worker.ID || workerRow.Epoch != credentials.WorkerEpoch ||
		!cancellationLeaseMatchesDecision(lease, authority) ||
		lease.AttemptID != credentials.AttemptID || lease.WorkerID != worker.ID ||
		lease.WorkerEpoch != credentials.WorkerEpoch || lease.Fence != credentials.Fence ||
		lease.OwnerKind != store.LeaseOwnerKindWORKER ||
		lease.OwnerID != workerRow.SpiffeID || !lease.RevokedAt.Valid ||
		!hmac.Equal(presentedDigest[:], lease.TokenDigest) ||
		attempt.ID != credentials.AttemptID || attempt.JobID != authority.JobID ||
		attempt.WorkerID != worker.ID || attempt.WorkerEpoch != credentials.WorkerEpoch ||
		attempt.Fence != credentials.Fence || attempt.State != store.AttemptStateCANCELED ||
		!attempt.EndedAt.Valid || authority.AttemptID != credentials.AttemptID ||
		authority.WorkerID != worker.ID || authority.WorkerEpoch != credentials.WorkerEpoch ||
		authority.AttemptFence != credentials.Fence ||
		authority.CancellationFence <= authority.AttemptFence ||
		(authority.JobState != store.JobStateCANCELING && authority.JobState != store.JobStateCANCELED) {
		return rejectedStaleStop(), nil
	}

	if authority.ReceiptID != uuid.Nil {
		result, resultErr := stopResultFromAuthority(authority)
		if resultErr != nil {
			return StopResult{}, resultErr
		}
		if err := tx.Commit(ctx); err != nil {
			return StopResult{}, fmt.Errorf("commit cancellation stop replay: %w", err)
		}
		return result, nil
	}

	stoppedAt, err := queries.GetPostgresTime(ctx)
	if err != nil || !stoppedAt.Valid {
		if err == nil {
			err = errors.New("PostgreSQL stop time is absent")
		}
		return StopResult{}, fmt.Errorf("read cancellation stop time: %w", err)
	}
	return finalizeCancellationStop(
		ctx,
		tx,
		queries,
		authority,
		stoppedAt,
		stopFinalizationPolicy{
			Source:        store.CancellationStopSourceACKNOWLEDGED,
			Operation:     "acknowledgement",
			ReleaseWorker: true,
		},
	)
}

func (s *Service) ReconcileNextCancellationStop(ctx context.Context) (StopResult, error) {
	if s == nil || s.internalPool == nil {
		return StopResult{}, errors.New("cancellation stop coordinator is not configured")
	}
	tx, err := s.internalPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return StopResult{}, fmt.Errorf("begin cancellation stop reconciliation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)

	candidate, err := queries.FindNextCancellationStopCandidate(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return StopResult{Decision: StopNoWork}, nil
	}
	if err != nil {
		return StopResult{}, fmt.Errorf("find expired cancellation stop: %w", err)
	}
	workerRow, err := queries.LockWorkerAuthority(ctx, candidate.WorkerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return StopResult{Decision: StopNoWork}, nil
	}
	if err != nil {
		return StopResult{}, fmt.Errorf("lock Worker for cancellation reconciliation: %w", err)
	}
	if err := queries.LockExecutionLeaseWrites(ctx); err != nil {
		return StopResult{}, fmt.Errorf("lock execution Lease writes for cancellation reconciliation: %w", err)
	}
	lease, err := queries.LockCancellationLease(ctx, store.LockCancellationLeaseParams{
		AuthorityLeaseID: candidate.AuthorityLeaseID,
		AttemptID:        candidate.AttemptID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return StopResult{Decision: StopNoWork}, nil
	}
	if err != nil {
		return StopResult{}, fmt.Errorf("lock reconciled cancellation Lease: %w", err)
	}
	attempt, err := queries.LockCancellationAttempt(ctx, candidate.AttemptID)
	if errors.Is(err, pgx.ErrNoRows) {
		return StopResult{Decision: StopNoWork}, nil
	}
	if err != nil {
		return StopResult{}, fmt.Errorf("lock reconciled canceled Attempt: %w", err)
	}
	authority, err := queries.LockCancellationStopAuthority(ctx, candidate.CancellationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return StopResult{Decision: StopNoWork}, nil
	}
	if err != nil {
		return StopResult{}, fmt.Errorf("lock reconciled cancellation authority: %w", err)
	}
	postgresNow, err := queries.GetPostgresTime(ctx)
	if err != nil || !postgresNow.Valid {
		if err == nil {
			err = errors.New("PostgreSQL reconciliation time is absent")
		}
		return StopResult{}, fmt.Errorf("read cancellation reconciliation time: %w", err)
	}
	if authority.ReceiptID != uuid.Nil || workerRow.ID != candidate.WorkerID ||
		candidate.AuthorityLeaseID != authority.AuthorityLeaseID ||
		!cancellationLeaseMatchesDecision(lease, authority) ||
		lease.AttemptID != candidate.AttemptID || lease.WorkerID != candidate.WorkerID ||
		lease.WorkerEpoch != authority.WorkerEpoch || lease.Fence != authority.AttemptFence ||
		!lease.RevokedAt.Valid || !authority.AuthorityLeaseExpiresAt.Valid ||
		authority.AuthorityLeaseExpiresAt.Time.After(postgresNow.Time) ||
		attempt.ID != candidate.AttemptID || attempt.JobID != authority.JobID ||
		attempt.WorkerID != authority.WorkerID || attempt.WorkerEpoch != authority.WorkerEpoch ||
		attempt.Fence != authority.AttemptFence || attempt.State != store.AttemptStateCANCELED ||
		!attempt.EndedAt.Valid || authority.CancellationID != candidate.CancellationID ||
		authority.AttemptID != candidate.AttemptID || authority.WorkerID != candidate.WorkerID ||
		authority.CancellationFence <= authority.AttemptFence ||
		(authority.JobState != store.JobStateCANCELING && authority.JobState != store.JobStateCANCELED) {
		return StopResult{Decision: StopNoWork}, nil
	}

	return finalizeCancellationStop(
		ctx,
		tx,
		queries,
		authority,
		postgresNow,
		stopFinalizationPolicy{
			Source:    store.CancellationStopSourceLEASEEXPIREDRECONCILIATION,
			Operation: "reconciliation",
		},
	)
}

func cancellationLeaseMatchesDecision(
	lease store.LockCancellationLeaseRow,
	authority store.LockCancellationStopAuthorityRow,
) bool {
	if authority.AuthorityLeaseID == uuid.Nil || !authority.AuthorityLeaseExpiresAt.Valid ||
		lease.ID != authority.AuthorityLeaseID ||
		string(lease.Phase) != authority.AuthorityLeasePhase ||
		string(lease.OwnerKind) != authority.AuthorityLeaseOwnerKind ||
		lease.OwnerID != authority.AuthorityLeaseOwnerID {
		return false
	}
	if lease.Phase == store.LeasePhaseEXECUTION {
		return lease.OwnerKind == store.LeaseOwnerKindWORKER
	}
	return lease.Phase == store.LeasePhaseFINALIZATION &&
		(lease.OwnerKind == store.LeaseOwnerKindWORKER ||
			lease.OwnerKind == store.LeaseOwnerKindRECONCILER)
}

type stopFinalizationPolicy struct {
	Source        store.CancellationStopSource
	Operation     string
	ReleaseWorker bool
}

func finalizeCancellationStop(
	ctx context.Context,
	tx pgx.Tx,
	queries *store.Queries,
	authority store.LockCancellationStopAuthorityRow,
	stoppedAt pgtype.Timestamptz,
	policy stopFinalizationPolicy,
) (StopResult, error) {
	decision, err := stopDecisionForSource(policy.Source)
	if err != nil || policy.Operation == "" || !stoppedAt.Valid {
		if err == nil {
			err = errors.New("cancellation stop finalization policy is incomplete")
		}
		return StopResult{}, err
	}

	terminalJobVersion := authority.JobVersion
	terminalized := authority.JobState == store.JobStateCANCELING
	if terminalized {
		terminalJobVersion, err = queries.CompleteCancelingJob(ctx, store.CompleteCancelingJobParams{
			StoppedAt:         stoppedAt,
			JobID:             authority.JobID,
			ExpectedVersion:   authority.JobVersion,
			CancellationFence: authority.CancellationFence,
		})
		if err != nil {
			return StopResult{}, fmt.Errorf(
				"terminalize cancellation by %s: %w",
				policy.Operation,
				err,
			)
		}
	}

	receiptID := uuid.New()
	if err := queries.InsertCancellationStopReceipt(ctx, store.InsertCancellationStopReceiptParams{
		ID:                 receiptID,
		OrganizationID:     authority.OrganizationID,
		ProjectID:          authority.ProjectID,
		JobID:              authority.JobID,
		CancellationID:     authority.CancellationID,
		AttemptID:          authority.AttemptID,
		WorkerID:           authority.WorkerID,
		WorkerEpoch:        authority.WorkerEpoch,
		AttemptFence:       authority.AttemptFence,
		CancellationFence:  authority.CancellationFence,
		Source:             policy.Source,
		TerminalJobVersion: terminalJobVersion,
		StoppedAt:          stoppedAt,
	}); err != nil {
		return StopResult{}, fmt.Errorf(
			"insert cancellation stop receipt for %s: %w",
			policy.Operation,
			err,
		)
	}
	if policy.ReleaseWorker {
		if _, err := queries.ReleaseWorkerAfterCancellationStop(
			ctx,
			store.ReleaseWorkerAfterCancellationStopParams{
				StoppedAt:   stoppedAt,
				WorkerID:    authority.WorkerID,
				WorkerEpoch: authority.WorkerEpoch,
			},
		); err != nil {
			return StopResult{}, fmt.Errorf("release Worker after cancellation stop: %w", err)
		}
	}
	if terminalized {
		if err := insertStoppedEvent(ctx, queries, authority, terminalJobVersion, stoppedAt.Time); err != nil {
			return StopResult{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return StopResult{}, fmt.Errorf("commit cancellation stop %s: %w", policy.Operation, err)
	}
	return StopResult{
		Decision:       decision,
		ReceiptID:      receiptID,
		CancellationID: authority.CancellationID,
		JobID:          authority.JobID,
		State:          string(store.JobStateCANCELED),
		JobVersion:     terminalJobVersion,
		Source:         string(policy.Source),
		StoppedAt:      stoppedAt.Time,
	}, nil
}

func insertStoppedEvent(
	ctx context.Context,
	queries *store.Queries,
	authority store.LockCancellationStopAuthorityRow,
	jobVersion int64,
	stoppedAt time.Time,
) error {
	eventID := uuid.New()
	event := &velav1.EventEnvelope{
		EventId:          eventID.String(),
		AggregateType:    "Job",
		AggregateId:      authority.JobID.String(),
		AggregateVersion: uint64(jobVersion),
		EventType:        "job.canceled",
		SchemaVersion:    1,
		OccurredAt:       timestamppb.New(stoppedAt),
		Payload: &velav1.EventEnvelope_JobCanceled{JobCanceled: &velav1.JobCanceled{
			OrganizationId: authority.OrganizationID.String(),
			ProjectId:      authority.ProjectID.String(),
			JobId:          authority.JobID.String(),
			CancellationId: authority.CancellationID.String(),
			JobFence:       uint64(authority.CancellationFence),
			Billable:       authority.Billable,
			ChargeId:       authority.ChargeID.String(),
			DecidedAt:      timestamppb.New(authority.DecidedAt.Time),
		}},
	}
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal terminal job.canceled event: %w", err)
	}
	if err := queries.InsertCancellationStoppedOutboxEvent(
		ctx,
		store.InsertCancellationStoppedOutboxEventParams{
			EventID:        eventID,
			OrganizationID: authority.OrganizationID,
			ProjectID:      authority.ProjectID,
			JobID:          authority.JobID,
			JobVersion:     jobVersion,
			Payload:        payload,
			OccurredAt:     pgtype.Timestamptz{Time: stoppedAt, Valid: true},
		},
	); err != nil {
		return fmt.Errorf("insert terminal job.canceled Outbox event: %w", err)
	}
	return nil
}

func stopResultFromAuthority(authority store.LockCancellationStopAuthorityRow) (StopResult, error) {
	if authority.ReceiptID == uuid.Nil || authority.ReceiptSource == "" ||
		authority.ReceiptJobVersion <= 0 || !authority.ReceiptStoppedAt.Valid {
		return StopResult{}, errors.New("stored cancellation stop receipt is incomplete")
	}
	decision, err := stopDecisionForSource(store.CancellationStopSource(authority.ReceiptSource))
	if err != nil {
		return StopResult{}, err
	}
	return StopResult{
		Decision:       decision,
		ReceiptID:      authority.ReceiptID,
		CancellationID: authority.CancellationID,
		JobID:          authority.JobID,
		State:          string(store.JobStateCANCELED),
		JobVersion:     authority.ReceiptJobVersion,
		Source:         authority.ReceiptSource,
		StoppedAt:      authority.ReceiptStoppedAt.Time,
	}, nil
}

func stopDecisionForSource(source store.CancellationStopSource) (StopDecision, error) {
	switch source {
	case store.CancellationStopSourceACKNOWLEDGED:
		return StopAcknowledged, nil
	case store.CancellationStopSourceLEASEEXPIREDRECONCILIATION:
		return StopReconciled, nil
	default:
		return "", errors.New("stored cancellation stop receipt has an unknown source")
	}
}

func rejectedStaleStop() StopResult {
	return StopResult{Decision: StopRejectedStaleAuthority}
}

func resultFromRow(row store.CancelJobRow) Result {
	result := Result{
		CancellationID: row.CancellationID,
		JobID:          row.JobID,
		Decision:       Decision(row.Decision),
		State:          string(row.JobState),
		JobVersion:     row.JobVersion,
		Billable:       row.Billable,
		DecidedAt:      row.DecidedAt.Time,
	}
	if row.ChargeID != uuid.Nil {
		result.Charge = &Charge{
			ID:          row.ChargeID,
			AmountMinor: row.ChargeAmountMinor,
			Currency:    row.ChargeCurrency,
			Reason:      row.ChargeReason,
			PostedAt:    row.ChargePostedAt.Time,
		}
	}
	return result
}

func cancellationFailure(err error) error {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return err
	}
	switch postgresError.Code {
	case "28000":
		return &Failure{Code: FailureUnauthorized, Message: "credential is no longer active"}
	case "42501":
		return &Failure{Code: FailureForbidden, Message: "credential is not authorized to cancel this Job"}
	case "P0002":
		return &Failure{Code: FailureNotFound, Message: "Job is not visible in this Project"}
	case "55000":
		return &Failure{Code: FailureUnavailable, Message: "Job cannot be canceled in its current state"}
	default:
		return err
	}
}

func retryableCancellationTransaction(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) &&
		(postgresError.Code == "40001" || postgresError.Code == "40P01")
}
