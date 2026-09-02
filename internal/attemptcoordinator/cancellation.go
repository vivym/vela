package attemptcoordinator

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/identity"
	store "github.com/vivym/vela/internal/store/sqlc"
)

const cancelStageGraphSQL = `
SELECT
    coalesce(result.cancellation_id, '00000000-0000-0000-0000-000000000000'::uuid),
    result.job_id,
    result.decision::text,
    result.job_state::text,
    result.job_version,
    result.billable,
    coalesce(result.charge_id, '00000000-0000-0000-0000-000000000000'::uuid),
    coalesce(result.charge_amount_minor, 0),
    coalesce(result.charge_currency, ''),
    coalesce(result.charge_reason::text, ''),
    coalesce(result.charge_posted_at, 'epoch'::timestamptz),
    result.decided_at
FROM vela_cancel_stage_graph(jsonb_build_object(
    'schema_version', 1,
    'job_id', $1::uuid,
    'cancellation_id', $2::uuid,
    'charge_id', $3::uuid,
    'cancel_requested_event_id', $4::uuid,
    'canceling_event_id', $5::uuid,
    'canceled_event_id', $6::uuid,
    'charge_posted_event_id', $7::uuid,
    'invoice_export_event_id', $8::uuid
)) AS result
`

type stageGraphCancellationRow struct {
	cancellationID   uuid.UUID
	jobID            uuid.UUID
	decision         string
	jobState         string
	jobVersion       int64
	billable         bool
	chargeID         uuid.UUID
	chargeAmount     int64
	chargeCurrency   string
	chargeReason     string
	chargePostedAt   pgtype.Timestamptz
	decisionRecorded pgtype.Timestamptz
}

func (service *Service) Cancel(
	ctx context.Context,
	principal identity.Principal,
	projectID, jobID uuid.UUID,
) (cancellation.Result, bool, error) {
	if service == nil || service.cancellationPool == nil {
		return cancellation.Result{}, true, errors.New("AttemptCoordinator cancellation is not configured")
	}
	if projectID == uuid.Nil || jobID == uuid.Nil ||
		principal.CredentialID == uuid.Nil || principal.ProjectID != projectID {
		return cancellation.Result{}, true, &cancellation.Failure{
			Code:    cancellation.FailureForbidden,
			Message: "Job is outside the authenticated Project",
		}
	}
	var lastErr error
	for range 3 {
		result, handled, err := service.cancelOnce(ctx, principal, projectID, jobID)
		if err == nil || !retryableStageGraphCancellation(err) {
			return result, handled, err
		}
		lastErr = err
	}
	return cancellation.Result{}, true, fmt.Errorf(
		"stage graph cancellation transaction did not stabilize: %w",
		lastErr,
	)
}

func (service *Service) cancelOnce(
	ctx context.Context,
	principal identity.Principal,
	projectID, jobID uuid.UUID,
) (cancellation.Result, bool, error) {
	tx, err := service.cancellationPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return cancellation.Result{}, true, fmt.Errorf("begin Stage graph cancellation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	credentialProof := principal.RequestContextProof()
	defer clear(credentialProof)
	requestContext, err := store.New(tx).SetCancellationRequestContext(
		ctx,
		store.SetCancellationRequestContextParams{
			CredentialID:    principal.CredentialID,
			CredentialProof: credentialProof,
		},
	)
	if err != nil {
		return cancellation.Result{}, true, stageGraphCancellationFailure(err)
	}
	if requestContext.OrganizationID != principal.OrganizationID ||
		requestContext.ProjectID != projectID ||
		requestContext.PrincipalID != principal.PrincipalID {
		return cancellation.Result{}, true, &cancellation.Failure{
			Code:    cancellation.FailureForbidden,
			Message: "credential request context changed",
		}
	}

	identities := [7]uuid.UUID{
		uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(),
	}
	var row stageGraphCancellationRow
	err = tx.QueryRow(
		ctx,
		cancelStageGraphSQL,
		jobID,
		identities[0],
		identities[1],
		identities[2],
		identities[3],
		identities[4],
		identities[5],
		identities[6],
	).Scan(
		&row.cancellationID,
		&row.jobID,
		&row.decision,
		&row.jobState,
		&row.jobVersion,
		&row.billable,
		&row.chargeID,
		&row.chargeAmount,
		&row.chargeCurrency,
		&row.chargeReason,
		&row.chargePostedAt,
		&row.decisionRecorded,
	)
	if err != nil {
		if stageGraphCancellationNotApplicable(err) {
			return cancellation.Result{}, false, nil
		}
		return cancellation.Result{}, true, stageGraphCancellationFailure(err)
	}
	if !row.decisionRecorded.Valid {
		return cancellation.Result{}, true, errors.New("stage graph cancellation has no decision time")
	}
	if row.billable && (row.chargeID == uuid.Nil || row.chargeAmount < 0 ||
		row.chargeCurrency == "" || row.chargeReason == "" || !row.chargePostedAt.Valid) {
		return cancellation.Result{}, true, errors.New("stage graph cancellation has no immutable Charge")
	}
	if err := tx.Commit(ctx); err != nil {
		return cancellation.Result{}, true, fmt.Errorf("commit Stage graph cancellation: %w", err)
	}
	result := cancellation.Result{
		CancellationID: row.cancellationID,
		JobID:          row.jobID,
		Decision:       cancellation.Decision(row.decision),
		State:          row.jobState,
		JobVersion:     row.jobVersion,
		Billable:       row.billable,
		DecidedAt:      row.decisionRecorded.Time,
	}
	if row.chargeID != uuid.Nil {
		result.Charge = &cancellation.Charge{
			ID:          row.chargeID,
			AmountMinor: row.chargeAmount,
			Currency:    row.chargeCurrency,
			Reason:      row.chargeReason,
			PostedAt:    row.chargePostedAt.Time,
		}
	}
	return result, true, nil
}

func stageGraphCancellationNotApplicable(err error) bool {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return false
	}
	return postgresError.Code == "42883" ||
		(postgresError.Code == "P0003" &&
			postgresError.ConstraintName == "stage_graph_cancellation_not_applicable")
}

func stageGraphCancellationFailure(err error) error {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return err
	}
	switch postgresError.Code {
	case "28000":
		return &cancellation.Failure{
			Code:    cancellation.FailureUnauthorized,
			Message: "credential is no longer active",
		}
	case "42501":
		return &cancellation.Failure{
			Code:    cancellation.FailureForbidden,
			Message: "credential is not authorized to cancel this Job",
		}
	case "P0002":
		return &cancellation.Failure{
			Code:    cancellation.FailureNotFound,
			Message: "Job is not visible in this Project",
		}
	case "55000":
		return &cancellation.Failure{
			Code:    cancellation.FailureUnavailable,
			Message: "Job cannot be canceled in its current state",
		}
	default:
		return err
	}
}

func retryableStageGraphCancellation(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) &&
		(postgresError.Code == "40001" || postgresError.Code == "40P01")
}
