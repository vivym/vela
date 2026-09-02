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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/identity"
	store "github.com/vivym/vela/internal/store/sqlc"
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

type Artifact struct {
	ID              uuid.UUID
	Kind            string
	Ordinal         int32
	ObjectKey       string
	ObjectVersionID string
	SizeBytes       int64
	SHA256          [sha256.Size]byte
	ContentType     string
}

type ArtifactSet struct {
	ID                 uuid.UUID
	ManifestSHA256     [sha256.Size]byte
	ChargeID           uuid.UUID
	RetentionExpiresAt time.Time
	CompletedAt        time.Time
	Artifacts          []Artifact
}

type Result struct {
	CancellationID uuid.UUID
	JobID          uuid.UUID
	Decision       Decision
	State          string
	JobVersion     int64
	Billable       bool
	Charge         *Charge
	ArtifactSet    *ArtifactSet
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
	if result.Decision == DecisionAlreadySucceeded {
		if s.internalPool == nil {
			return Result{}, errors.New("cancellation ArtifactSet reader is not configured")
		}
		artifacts, listErr := store.New(s.internalPool).ListSucceededArtifactSetForCancellation(
			ctx,
			store.ListSucceededArtifactSetForCancellationParams{
				JobID:          row.JobID,
				OrganizationID: principal.OrganizationID,
				ProjectID:      projectID,
			},
		)
		if listErr != nil {
			return Result{}, fmt.Errorf("read winning ArtifactSet after cancellation loss: %w", listErr)
		}
		artifactSet, buildErr := cancellationArtifactSet(artifacts)
		if buildErr != nil {
			return Result{}, buildErr
		}
		result.ArtifactSet = &artifactSet
	}
	return result, nil
}

func cancellationArtifactSet(
	rows []store.ListSucceededArtifactSetForCancellationRow,
) (ArtifactSet, error) {
	if len(rows) == 0 {
		return ArtifactSet{}, errors.New("winning ArtifactSet has no items")
	}
	first := rows[0]
	if first.ArtifactSetID == uuid.Nil || first.ChargeID == uuid.Nil ||
		len(first.ManifestSha256) != sha256.Size || !first.RetentionExpiresAt.Valid ||
		!first.CompletedAt.Valid {
		return ArtifactSet{}, errors.New("winning ArtifactSet identity is incomplete")
	}
	var manifest [sha256.Size]byte
	copy(manifest[:], first.ManifestSha256)
	result := ArtifactSet{
		ID:                 first.ArtifactSetID,
		ManifestSHA256:     manifest,
		ChargeID:           first.ChargeID,
		RetentionExpiresAt: first.RetentionExpiresAt.Time,
		CompletedAt:        first.CompletedAt.Time,
		Artifacts:          make([]Artifact, 0, len(rows)),
	}
	for _, row := range rows {
		if row.ArtifactSetID != result.ID || row.ChargeID != result.ChargeID ||
			!hmac.Equal(row.ManifestSha256, result.ManifestSHA256[:]) ||
			!row.RetentionExpiresAt.Valid ||
			!row.RetentionExpiresAt.Time.Equal(result.RetentionExpiresAt) ||
			!row.CompletedAt.Valid || !row.CompletedAt.Time.Equal(result.CompletedAt) ||
			row.ArtifactID == uuid.Nil || row.ObjectKey == "" || row.ObjectVersionID == "" ||
			row.SizeBytes <= 0 || len(row.Sha256) != sha256.Size || row.ContentType == "" {
			return ArtifactSet{}, errors.New("winning ArtifactSet item is inconsistent")
		}
		var digest [sha256.Size]byte
		copy(digest[:], row.Sha256)
		result.Artifacts = append(result.Artifacts, Artifact{
			ID:              row.ArtifactID,
			Kind:            row.Kind,
			Ordinal:         row.Ordinal,
			ObjectKey:       row.ObjectKey,
			ObjectVersionID: row.ObjectVersionID,
			SizeBytes:       row.SizeBytes,
			SHA256:          digest,
			ContentType:     row.ContentType,
		})
	}
	return result, nil
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
