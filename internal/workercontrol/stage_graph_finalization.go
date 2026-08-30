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
	store "github.com/vivym/vela/internal/store/sqlc"
)

type AuthenticatedFinalizer struct {
	ID string
}

type StageGraphFinalizationDecision string

const (
	StageGraphFinalizationGranted StageGraphFinalizationDecision = "STAGE_GRAPH_FINALIZATION_GRANTED"
	StageGraphFinalizationNoWork  StageGraphFinalizationDecision = "NO_STAGE_GRAPH_FINALIZATION_WORK"
)

type StageGraphFinalizationCredentials struct {
	ClaimID   uuid.UUID
	AttemptID uuid.UUID
	Fence     int64
	Token     string
}

type StageGraphFinalizationSource struct {
	StageRunID               uuid.UUID
	StageArtifactID          uuid.UUID
	StageInterfaceRevisionID uuid.UUID
	ObjectKey                string
	ObjectVersion            string
	ContentType              string
	SHA256                   [sha256.Size]byte
	SizeBytes                int64
	ExpiresAt                time.Time
}

type StageGraphFinalizationClaim struct {
	Decision               StageGraphFinalizationDecision
	ClaimID                uuid.UUID
	JobID                  uuid.UUID
	JobVersion             int64
	Credentials            StageGraphFinalizationCredentials
	FinalizationStartedAt  time.Time
	FinalizationDeadlineAt time.Time
	ClaimExpiresAt         time.Time
	Source                 StageGraphFinalizationSource
}

type stageGraphFinalizationTokenClaims struct {
	ClaimID         uuid.UUID
	AttemptID       uuid.UUID
	Fence           int64
	OwnerID         string
	StageArtifactID uuid.UUID
	ObjectVersion   string
	IssuedAt        time.Time
	ExpiresAt       time.Time
}

func (s *Service) ClaimNextStageGraphFinalization(
	ctx context.Context,
	finalizer AuthenticatedFinalizer,
) (StageGraphFinalizationClaim, error) {
	if s == nil || s.pool == nil {
		return StageGraphFinalizationClaim{}, errors.New("worker coordinator is not configured")
	}
	if !validPrintableFailureText(finalizer.ID, 500) {
		return StageGraphFinalizationClaim{}, errors.New("authenticated Finalizer identity is invalid")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return StageGraphFinalizationClaim{}, fmt.Errorf("begin Stage graph finalization claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	now, err := postgresTime(ctx, queries)
	if err != nil {
		return StageGraphFinalizationClaim{}, err
	}
	observedAt := pgtype.Timestamptz{Time: now, Valid: true}
	if err := queries.ExpireStageGraphFinalizationClaims(ctx, observedAt); err != nil {
		return StageGraphFinalizationClaim{}, fmt.Errorf("expire Stage graph finalization claims: %w", err)
	}

	active, err := queries.FindActiveStageGraphFinalizationClaim(
		ctx,
		store.FindActiveStageGraphFinalizationClaimParams{
			OwnerID: finalizer.ID, ObservedAt: observedAt,
		},
	)
	if err == nil {
		claim, buildErr := s.replayStageGraphFinalizationClaim(active)
		if buildErr != nil {
			return StageGraphFinalizationClaim{}, buildErr
		}
		if err := tx.Commit(ctx); err != nil {
			return StageGraphFinalizationClaim{}, fmt.Errorf("commit Stage graph finalization replay: %w", err)
		}
		return claim, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return StageGraphFinalizationClaim{}, fmt.Errorf("find active Stage graph finalization claim: %w", err)
	}

	candidate, err := queries.LockNextStageGraphFinalizationCandidate(ctx, observedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return StageGraphFinalizationClaim{}, fmt.Errorf("commit empty Stage graph finalization scan: %w", err)
		}
		return StageGraphFinalizationClaim{Decision: StageGraphFinalizationNoWork}, nil
	}
	if err != nil {
		return StageGraphFinalizationClaim{}, fmt.Errorf("lock Stage graph finalization candidate: %w", err)
	}
	if !candidate.FinalizationStartedAt.Valid || !candidate.FinalizationDeadlineAt.Valid ||
		!candidate.JobExpiresAt.Valid || !candidate.ArtifactExpiresAt.Valid ||
		len(candidate.Sha256) != sha256.Size {
		return StageGraphFinalizationClaim{}, errors.New("Stage graph finalization candidate is incomplete")
	}
	expiresAt := now.Add(s.leaseTTL)
	for _, ceiling := range []time.Time{
		candidate.FinalizationDeadlineAt.Time,
		candidate.JobExpiresAt.Time,
		candidate.ArtifactExpiresAt.Time,
	} {
		if ceiling.Before(expiresAt) {
			expiresAt = ceiling
		}
	}
	if !expiresAt.After(now) {
		return StageGraphFinalizationClaim{Decision: StageGraphFinalizationNoWork}, nil
	}
	claimID := uuid.New()
	tokenClaims := stageGraphFinalizationTokenClaims{
		ClaimID: claimID, AttemptID: candidate.AttemptID, Fence: candidate.AttemptFence,
		OwnerID: finalizer.ID, StageArtifactID: candidate.FinalStageArtifactID,
		ObjectVersion: candidate.ObjectVersion, IssuedAt: now, ExpiresAt: expiresAt,
	}
	token, tokenDigest, err := s.issueStageGraphFinalizationToken(
		tokenClaims, s.activeLeaseKeyID,
	)
	if err != nil {
		return StageGraphFinalizationClaim{}, err
	}
	if err := queries.InsertStageGraphFinalizationClaim(
		ctx,
		store.InsertStageGraphFinalizationClaimParams{
			ClaimID: claimID, OrganizationID: candidate.OrganizationID,
			ProjectID: candidate.ProjectID, JobID: candidate.JobID,
			AttemptID: candidate.AttemptID, AttemptFence: candidate.AttemptFence,
			FinalStageRunID:      candidate.FinalStageRunID,
			FinalStageArtifactID: candidate.FinalStageArtifactID,
			ExactObjectVersion:   candidate.ObjectVersion, OwnerID: finalizer.ID,
			TokenDigest: tokenDigest, SigningKeyID: s.activeLeaseKeyID,
			IssuedAt:  observedAt,
			ExpiresAt: pgtype.Timestamptz{Time: expiresAt, Valid: true},
		},
	); err != nil {
		return StageGraphFinalizationClaim{}, fmt.Errorf("insert Stage graph finalization claim: %w", err)
	}
	claim := stageGraphFinalizationClaimFromCandidate(candidate, claimID, token, expiresAt)
	if err := tx.Commit(ctx); err != nil {
		return StageGraphFinalizationClaim{}, fmt.Errorf("commit Stage graph finalization claim: %w", err)
	}
	return claim, nil
}

func (s *Service) replayStageGraphFinalizationClaim(
	row store.FindActiveStageGraphFinalizationClaimRow,
) (StageGraphFinalizationClaim, error) {
	if !row.IssuedAt.Valid || !row.ExpiresAt.Valid || !row.FinalizationStartedAt.Valid ||
		!row.FinalizationDeadlineAt.Valid || !row.ArtifactExpiresAt.Valid ||
		len(row.Sha256) != sha256.Size {
		return StageGraphFinalizationClaim{}, errors.New("persisted Stage graph finalization claim is incomplete")
	}
	token, digest, err := s.issueStageGraphFinalizationToken(
		stageGraphFinalizationTokenClaims{
			ClaimID: row.ClaimID, AttemptID: row.AttemptID, Fence: row.AttemptFence,
			OwnerID: row.OwnerID, StageArtifactID: row.FinalStageArtifactID,
			ObjectVersion: row.ExactObjectVersion, IssuedAt: row.IssuedAt.Time,
			ExpiresAt: row.ExpiresAt.Time,
		},
		row.SigningKeyID,
	)
	if err != nil {
		return StageGraphFinalizationClaim{}, err
	}
	if !hmac.Equal(digest, row.TokenDigest) {
		return StageGraphFinalizationClaim{}, errors.New("persisted Stage graph finalization token digest is invalid")
	}
	var digestValue [sha256.Size]byte
	copy(digestValue[:], row.Sha256)
	return StageGraphFinalizationClaim{
		Decision: StageGraphFinalizationGranted, ClaimID: row.ClaimID,
		JobID: row.JobID, JobVersion: row.JobVersion,
		Credentials: StageGraphFinalizationCredentials{
			ClaimID: row.ClaimID, AttemptID: row.AttemptID,
			Fence: row.AttemptFence, Token: token,
		},
		FinalizationStartedAt:  row.FinalizationStartedAt.Time,
		FinalizationDeadlineAt: row.FinalizationDeadlineAt.Time,
		ClaimExpiresAt:         row.ExpiresAt.Time,
		Source: StageGraphFinalizationSource{
			StageRunID: row.FinalStageRunID, StageArtifactID: row.FinalStageArtifactID,
			StageInterfaceRevisionID: row.StageInterfaceRevisionID,
			ObjectKey:                row.ObjectKey, ObjectVersion: row.ExactObjectVersion,
			ContentType: row.ContentType, SHA256: digestValue, SizeBytes: row.SizeBytes,
			ExpiresAt: row.ArtifactExpiresAt.Time,
		},
	}, nil
}

func stageGraphFinalizationClaimFromCandidate(
	row store.LockNextStageGraphFinalizationCandidateRow,
	claimID uuid.UUID,
	token string,
	expiresAt time.Time,
) StageGraphFinalizationClaim {
	var digest [sha256.Size]byte
	copy(digest[:], row.Sha256)
	return StageGraphFinalizationClaim{
		Decision: StageGraphFinalizationGranted, ClaimID: claimID,
		JobID: row.JobID, JobVersion: row.JobVersion,
		Credentials: StageGraphFinalizationCredentials{
			ClaimID: claimID, AttemptID: row.AttemptID, Fence: row.AttemptFence, Token: token,
		},
		FinalizationStartedAt:  row.FinalizationStartedAt.Time,
		FinalizationDeadlineAt: row.FinalizationDeadlineAt.Time,
		ClaimExpiresAt:         expiresAt,
		Source: StageGraphFinalizationSource{
			StageRunID: row.FinalStageRunID, StageArtifactID: row.FinalStageArtifactID,
			StageInterfaceRevisionID: row.StageInterfaceRevisionID,
			ObjectKey:                row.ObjectKey, ObjectVersion: row.ObjectVersion,
			ContentType: row.ContentType, SHA256: digest, SizeBytes: row.SizeBytes,
			ExpiresAt: row.ArtifactExpiresAt.Time,
		},
	}
}

func (s *Service) issueStageGraphFinalizationToken(
	claims stageGraphFinalizationTokenClaims,
	keyID string,
) (string, []byte, error) {
	key, ok := s.leaseKeys[keyID]
	if !ok {
		return "", nil, fmt.Errorf("lease signing key %q is unavailable", keyID)
	}
	mac := hmac.New(sha256.New, key)
	_, _ = fmt.Fprintf(
		mac,
		"vela-stage-graph-finalization-v1\n%s\n%s\n%d\n%s\n%s\n%s\n%d\n%d",
		claims.ClaimID,
		claims.AttemptID,
		claims.Fence,
		claims.OwnerID,
		claims.StageArtifactID,
		claims.ObjectVersion,
		claims.IssuedAt.UnixNano(),
		claims.ExpiresAt.UnixNano(),
	)
	token := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	digest := sha256.Sum256([]byte(token))
	return token, digest[:], nil
}
