package artifactaccess

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/identity"
	store "github.com/vivym/vela/internal/store/sqlc"
)

type FailureCode string

const (
	FailureUnauthorized FailureCode = "unauthorized"
	FailureForbidden    FailureCode = "forbidden"
	FailureNotFound     FailureCode = "not_found"
)

type Failure struct {
	Code    FailureCode
	Message string
}

func (failure *Failure) Error() string {
	return string(failure.Code) + ": " + failure.Message
}

type ExactVersionSigner interface {
	PresignExactVersion(context.Context, string, string) (artifactstore.SignedRead, error)
}

type Service struct {
	pool   *pgxpool.Pool
	signer ExactVersionSigner
}

type ArtifactSet struct {
	ID                 uuid.UUID
	JobID              uuid.UUID
	RetentionExpiresAt time.Time
	CommittedAt        time.Time
	Artifacts          []Artifact
}

type Artifact struct {
	ID                   uuid.UUID
	Kind                 string
	Ordinal              int32
	ObjectKey            string
	ObjectVersionID      string
	SizeBytes            int64
	SHA256               [sha256.Size]byte
	ContentType          string
	DownloadURL          string
	DownloadURLExpiresAt time.Time
}

func NewService(pool *pgxpool.Pool, signer ExactVersionSigner) *Service {
	return &Service{pool: pool, signer: signer}
}

func (service *Service) Get(
	ctx context.Context,
	principal identity.Principal,
	projectID uuid.UUID,
	jobID uuid.UUID,
) (ArtifactSet, error) {
	if service == nil || service.pool == nil || service.signer == nil {
		return ArtifactSet{}, errors.New("Artifact access service is not configured")
	}
	if projectID == uuid.Nil || jobID == uuid.Nil || principal.CredentialID == uuid.Nil ||
		principal.ProjectID != projectID {
		return ArtifactSet{}, &Failure{
			Code: FailureNotFound, Message: "ArtifactSet is not visible in this Project",
		}
	}
	tx, err := service.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ArtifactSet{}, fmt.Errorf("begin Artifact access transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	proof := principal.RequestContextProof()
	defer clear(proof)
	requestContext, err := queries.SetArtifactRequestContext(
		ctx,
		store.SetArtifactRequestContextParams{
			CredentialID:    principal.CredentialID,
			CredentialProof: proof,
		},
	)
	if err != nil {
		return ArtifactSet{}, accessFailure(err)
	}
	if requestContext.OrganizationID != principal.OrganizationID ||
		requestContext.ProjectID != projectID ||
		requestContext.PrincipalID != principal.PrincipalID {
		return ArtifactSet{}, &Failure{
			Code: FailureForbidden, Message: "credential request context changed",
		}
	}
	if !requestContext.TransactionTime.Valid {
		return ArtifactSet{}, errors.New("Artifact access transaction time is unavailable")
	}
	rows, err := queries.ListReadableArtifactSet(ctx, store.ListReadableArtifactSetParams{
		OrganizationID: principal.OrganizationID,
		ProjectID:      projectID,
		JobID:          jobID,
		ReadAt:         requestContext.TransactionTime,
	})
	if err != nil {
		return ArtifactSet{}, fmt.Errorf("read committed ArtifactSet: %w", err)
	}
	if len(rows) == 0 {
		return ArtifactSet{}, &Failure{
			Code: FailureNotFound, Message: "ArtifactSet is not visible in this Project",
		}
	}
	result, err := artifactSetFromRows(rows, requestContext.TransactionTime.Time)
	if err != nil {
		return ArtifactSet{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return ArtifactSet{}, fmt.Errorf("commit Artifact access authorization: %w", err)
	}
	for index := range result.Artifacts {
		artifact := &result.Artifacts[index]
		signed, err := service.signer.PresignExactVersion(
			ctx,
			artifact.ObjectKey,
			artifact.ObjectVersionID,
		)
		if err != nil {
			return ArtifactSet{}, fmt.Errorf("sign exact Artifact version: %w", err)
		}
		if signed.URL == "" || signed.IssuedAt.IsZero() || !signed.ExpiresAt.After(signed.IssuedAt) ||
			signed.ExpiresAt.Sub(signed.IssuedAt) > artifactstore.MaxSignedGETTTL {
			return ArtifactSet{}, errors.New("signed Artifact URL violates the expiry contract")
		}
		artifact.DownloadURL = signed.URL
		artifact.DownloadURLExpiresAt = signed.ExpiresAt
	}
	return result, nil
}

func artifactSetFromRows(
	rows []store.ListReadableArtifactSetRow,
	readAt time.Time,
) (ArtifactSet, error) {
	first := rows[0]
	if first.ArtifactSetID == uuid.Nil || first.JobID == uuid.Nil ||
		!first.RetentionExpiresAt.Valid || !first.RetentionExpiresAt.Time.After(readAt) ||
		!first.CommittedAt.Valid {
		return ArtifactSet{}, errors.New("committed ArtifactSet identity is incomplete")
	}
	result := ArtifactSet{
		ID:                 first.ArtifactSetID,
		JobID:              first.JobID,
		RetentionExpiresAt: first.RetentionExpiresAt.Time,
		CommittedAt:        first.CommittedAt.Time,
		Artifacts:          make([]Artifact, 0, len(rows)),
	}
	seen := make(map[uuid.UUID]struct{}, len(rows))
	for _, row := range rows {
		if row.ArtifactSetID != result.ID || row.JobID != result.JobID ||
			!row.RetentionExpiresAt.Valid || !row.RetentionExpiresAt.Time.Equal(result.RetentionExpiresAt) ||
			!row.CommittedAt.Valid || !row.CommittedAt.Time.Equal(result.CommittedAt) ||
			row.ArtifactID == uuid.Nil || row.ObjectKey == "" || row.ObjectVersionID == "" ||
			row.SizeBytes <= 0 || len(row.Sha256) != sha256.Size || row.ContentType == "" {
			return ArtifactSet{}, errors.New("committed Artifact snapshot is incomplete")
		}
		if _, duplicate := seen[row.ArtifactID]; duplicate {
			return ArtifactSet{}, errors.New("committed ArtifactSet contains a duplicate Artifact")
		}
		seen[row.ArtifactID] = struct{}{}
		var digest [sha256.Size]byte
		copy(digest[:], row.Sha256)
		result.Artifacts = append(result.Artifacts, Artifact{
			ID:              row.ArtifactID,
			Kind:            string(row.Kind),
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

func accessFailure(err error) error {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return err
	}
	switch postgresError.Code {
	case "28000":
		return &Failure{Code: FailureUnauthorized, Message: "credential is no longer active"}
	case "42501":
		return &Failure{Code: FailureForbidden, Message: "credential cannot read Artifacts"}
	default:
		return err
	}
}
