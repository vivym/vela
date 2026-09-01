package stagefinalization

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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
	OutputKey                string
	ArtifactKind             ArtifactKind
	Ordinal                  int32
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
	Sources                []StageGraphFinalizationSource
}

type stageGraphFinalizationTokenClaims struct {
	ClaimID         uuid.UUID
	AttemptID       uuid.UUID
	Fence           int64
	OwnerID         string
	StageArtifactID uuid.UUID
	ObjectVersion   string
	OutputSetDigest [sha256.Size]byte
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
	if !validPrintableText(finalizer.ID, 500) {
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
		claim, buildErr := s.replayStageGraphFinalizationClaim(ctx, queries, active)
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
		!candidate.JobExpiresAt.Valid {
		return StageGraphFinalizationClaim{}, errors.New("stage graph finalization candidate is incomplete")
	}
	sources, outputSetDigest, err := loadStageGraphFinalizationCandidateSources(
		ctx, queries, candidate.AttemptID,
	)
	if err != nil {
		return StageGraphFinalizationClaim{}, err
	}
	primary := sources[0]
	expiresAt := now.Add(s.leaseTTL)
	for _, ceiling := range []time.Time{
		candidate.FinalizationDeadlineAt.Time,
		candidate.JobExpiresAt.Time,
	} {
		if ceiling.Before(expiresAt) {
			expiresAt = ceiling
		}
	}
	for _, source := range sources {
		if source.ExpiresAt.Before(expiresAt) {
			expiresAt = source.ExpiresAt
		}
	}
	if !expiresAt.After(now) {
		return StageGraphFinalizationClaim{Decision: StageGraphFinalizationNoWork}, nil
	}
	claimID := uuid.New()
	tokenClaims := stageGraphFinalizationTokenClaims{
		ClaimID: claimID, AttemptID: candidate.AttemptID, Fence: candidate.AttemptFence,
		OwnerID: finalizer.ID, StageArtifactID: primary.StageArtifactID,
		ObjectVersion: primary.ObjectVersion, OutputSetDigest: outputSetDigest,
		IssuedAt: now, ExpiresAt: expiresAt,
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
			FinalStageRunID:      primary.StageRunID,
			FinalStageArtifactID: primary.StageArtifactID,
			ExactObjectVersion:   primary.ObjectVersion, OwnerID: finalizer.ID,
			TokenDigest: tokenDigest, SigningKeyID: s.activeLeaseKeyID,
			OutputSetDigest: outputSetDigest[:],
			IssuedAt:        observedAt,
			ExpiresAt:       pgtype.Timestamptz{Time: expiresAt, Valid: true},
		},
	); err != nil {
		return StageGraphFinalizationClaim{}, fmt.Errorf("insert Stage graph finalization claim: %w", err)
	}
	for _, source := range sources {
		if err := queries.InsertStageGraphFinalizationClaimOutput(
			ctx,
			store.InsertStageGraphFinalizationClaimOutputParams{
				ClaimID: claimID, OrganizationID: candidate.OrganizationID,
				ProjectID: candidate.ProjectID, AttemptID: candidate.AttemptID,
				OutputKey: source.OutputKey, ArtifactKind: store.ArtifactKind(source.ArtifactKind),
				Ordinal: source.Ordinal, StageRunID: source.StageRunID,
				StageArtifactID:          source.StageArtifactID,
				StageInterfaceRevisionID: source.StageInterfaceRevisionID,
				ExactObjectVersion:       source.ObjectVersion,
			},
		); err != nil {
			return StageGraphFinalizationClaim{}, fmt.Errorf(
				"insert Stage graph finalization claim output %s: %w", source.OutputKey, err,
			)
		}
	}
	claim := stageGraphFinalizationClaimFromCandidate(
		candidate, claimID, token, expiresAt, sources,
	)
	if err := tx.Commit(ctx); err != nil {
		return StageGraphFinalizationClaim{}, fmt.Errorf("commit Stage graph finalization claim: %w", err)
	}
	return claim, nil
}

func (s *Service) replayStageGraphFinalizationClaim(
	ctx context.Context,
	queries *store.Queries,
	row store.FindActiveStageGraphFinalizationClaimRow,
) (StageGraphFinalizationClaim, error) {
	if !row.IssuedAt.Valid || !row.ExpiresAt.Valid || !row.FinalizationStartedAt.Valid ||
		!row.FinalizationDeadlineAt.Valid || len(row.OutputSetDigest) != sha256.Size {
		return StageGraphFinalizationClaim{}, errors.New("persisted Stage graph finalization claim is incomplete")
	}
	sources, outputSetDigest, err := loadStageGraphFinalizationClaimSources(ctx, queries, row.ClaimID)
	if err != nil {
		return StageGraphFinalizationClaim{}, err
	}
	if !hmac.Equal(outputSetDigest[:], row.OutputSetDigest) {
		return StageGraphFinalizationClaim{}, errors.New("persisted Stage graph finalization output-set digest is invalid")
	}
	primary := sources[0]
	token, digest, err := s.issueStageGraphFinalizationToken(
		stageGraphFinalizationTokenClaims{
			ClaimID: row.ClaimID, AttemptID: row.AttemptID, Fence: row.AttemptFence,
			OwnerID: row.OwnerID, StageArtifactID: primary.StageArtifactID,
			ObjectVersion: primary.ObjectVersion, OutputSetDigest: outputSetDigest,
			IssuedAt: row.IssuedAt.Time, ExpiresAt: row.ExpiresAt.Time,
		},
		row.SigningKeyID,
	)
	if err != nil {
		return StageGraphFinalizationClaim{}, err
	}
	if !hmac.Equal(digest, row.TokenDigest) {
		return StageGraphFinalizationClaim{}, errors.New("persisted Stage graph finalization token digest is invalid")
	}
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
		Source:                 primary, Sources: slices.Clone(sources),
	}, nil
}

func stageGraphFinalizationClaimFromCandidate(
	row store.LockNextStageGraphFinalizationCandidateRow,
	claimID uuid.UUID,
	token string,
	expiresAt time.Time,
	sources []StageGraphFinalizationSource,
) StageGraphFinalizationClaim {
	primary := sources[0]
	return StageGraphFinalizationClaim{
		Decision: StageGraphFinalizationGranted, ClaimID: claimID,
		JobID: row.JobID, JobVersion: row.JobVersion,
		Credentials: StageGraphFinalizationCredentials{
			ClaimID: claimID, AttemptID: row.AttemptID, Fence: row.AttemptFence, Token: token,
		},
		FinalizationStartedAt:  row.FinalizationStartedAt.Time,
		FinalizationDeadlineAt: row.FinalizationDeadlineAt.Time,
		ClaimExpiresAt:         expiresAt,
		Source:                 primary, Sources: slices.Clone(sources),
	}
}

type stageGraphFinalizationOutputDigestItem struct {
	OutputKey                string `json:"output_key"`
	ArtifactKind             string `json:"artifact_kind"`
	Ordinal                  int32  `json:"ordinal"`
	StageRunID               string `json:"stage_run_id"`
	StageArtifactID          string `json:"stage_artifact_id"`
	StageInterfaceRevisionID string `json:"stage_interface_revision_id"`
	ObjectKey                string `json:"object_key"`
	ObjectVersion            string `json:"object_version"`
	ContentType              string `json:"content_type"`
	SHA256                   string `json:"sha256"`
	SizeBytes                int64  `json:"size_bytes"`
}

func loadStageGraphFinalizationCandidateSources(
	ctx context.Context,
	queries *store.Queries,
	attemptID uuid.UUID,
) ([]StageGraphFinalizationSource, [sha256.Size]byte, error) {
	rows, err := queries.ListStageGraphFinalizationOutputs(ctx, attemptID)
	if err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("list Stage graph finalization outputs: %w", err)
	}
	sources := make([]StageGraphFinalizationSource, 0, len(rows))
	for _, row := range rows {
		kind, ordinal, valid := stageGraphOutputArtifactIdentity(row.OutputKey)
		if !valid || !row.ExpiresAt.Valid || len(row.Sha256) != sha256.Size {
			return nil, [sha256.Size]byte{}, errors.New("stage graph finalization output is incomplete")
		}
		var digest [sha256.Size]byte
		copy(digest[:], row.Sha256)
		sources = append(sources, StageGraphFinalizationSource{
			OutputKey: row.OutputKey, ArtifactKind: kind, Ordinal: ordinal,
			StageRunID: row.StageRunID, StageArtifactID: row.StageArtifactID,
			StageInterfaceRevisionID: row.StageInterfaceRevisionID,
			ObjectKey:                row.ObjectKey, ObjectVersion: row.ObjectVersion,
			ContentType: row.ContentType, SHA256: digest, SizeBytes: row.SizeBytes,
			ExpiresAt: row.ExpiresAt.Time,
		})
	}
	return validatedStageGraphFinalizationSources(sources)
}

func loadStageGraphFinalizationClaimSources(
	ctx context.Context,
	queries *store.Queries,
	claimID uuid.UUID,
) ([]StageGraphFinalizationSource, [sha256.Size]byte, error) {
	rows, err := queries.ListStageGraphFinalizationClaimOutputs(ctx, claimID)
	if err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("list persisted Stage graph finalization outputs: %w", err)
	}
	sources := make([]StageGraphFinalizationSource, 0, len(rows))
	for _, row := range rows {
		if !row.ExpiresAt.Valid || len(row.Sha256) != sha256.Size {
			return nil, [sha256.Size]byte{}, errors.New("persisted Stage graph finalization output is incomplete")
		}
		var digest [sha256.Size]byte
		copy(digest[:], row.Sha256)
		sources = append(sources, StageGraphFinalizationSource{
			OutputKey: row.OutputKey, ArtifactKind: ArtifactKind(row.ArtifactKind),
			Ordinal: row.Ordinal, StageRunID: row.StageRunID,
			StageArtifactID:          row.StageArtifactID,
			StageInterfaceRevisionID: row.StageInterfaceRevisionID,
			ObjectKey:                row.ObjectKey, ObjectVersion: row.ExactObjectVersion,
			ContentType: row.ContentType, SHA256: digest, SizeBytes: row.SizeBytes,
			ExpiresAt: row.ExpiresAt.Time,
		})
	}
	return validatedStageGraphFinalizationSources(sources)
}

func validatedStageGraphFinalizationSources(
	sources []StageGraphFinalizationSource,
) ([]StageGraphFinalizationSource, [sha256.Size]byte, error) {
	if len(sources) == 0 || len(sources) > 10_000 {
		return nil, [sha256.Size]byte{}, errors.New("stage graph finalization output set is empty or unbounded")
	}
	digestItems := make([]stageGraphFinalizationOutputDigestItem, 0, len(sources))
	seenKeys := make(map[string]struct{}, len(sources))
	seenArtifacts := make(map[uuid.UUID]struct{}, len(sources))
	for index, source := range sources {
		if source.OutputKey == "" || source.StageRunID == uuid.Nil ||
			source.StageArtifactID == uuid.Nil || source.StageInterfaceRevisionID == uuid.Nil ||
			source.ObjectKey == "" || source.ObjectVersion == "" || source.ContentType == "" ||
			source.SizeBytes <= 0 || source.SHA256 == ([sha256.Size]byte{}) ||
			(index > 0 && sources[index-1].OutputKey >= source.OutputKey) {
			return nil, [sha256.Size]byte{}, errors.New("stage graph finalization output set is not canonical")
		}
		if _, exists := seenKeys[source.OutputKey]; exists {
			return nil, [sha256.Size]byte{}, errors.New("stage graph finalization output key is duplicated")
		}
		if _, exists := seenArtifacts[source.StageArtifactID]; exists {
			return nil, [sha256.Size]byte{}, errors.New("stage graph finalization Artifact is duplicated")
		}
		seenKeys[source.OutputKey] = struct{}{}
		seenArtifacts[source.StageArtifactID] = struct{}{}
		digestItems = append(digestItems, stageGraphFinalizationOutputDigestItem{
			OutputKey: source.OutputKey, ArtifactKind: string(source.ArtifactKind),
			Ordinal: source.Ordinal, StageRunID: source.StageRunID.String(),
			StageArtifactID:          source.StageArtifactID.String(),
			StageInterfaceRevisionID: source.StageInterfaceRevisionID.String(),
			ObjectKey:                source.ObjectKey, ObjectVersion: source.ObjectVersion,
			ContentType: source.ContentType, SHA256: hex.EncodeToString(source.SHA256[:]),
			SizeBytes: source.SizeBytes,
		})
	}
	payload, err := json.Marshal(struct {
		SchemaVersion int                                      `json:"schema_version"`
		Outputs       []stageGraphFinalizationOutputDigestItem `json:"outputs"`
	}{SchemaVersion: 1, Outputs: digestItems})
	if err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("encode Stage graph finalization output set: %w", err)
	}
	return slices.Clone(sources), sha256.Sum256(payload), nil
}

func stageGraphOutputArtifactIdentity(outputKey string) (ArtifactKind, int32, bool) {
	switch outputKey {
	case "video":
		return ArtifactKindVideo, 0, true
	case "thumbnail":
		return ArtifactKindThumbnail, 0, true
	default:
		return "", 0, false
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
		"vela-stage-graph-finalization-v1\n%s\n%s\n%d\n%s\n%s\n%s\n%x\n%d\n%d",
		claims.ClaimID,
		claims.AttemptID,
		claims.Fence,
		claims.OwnerID,
		claims.StageArtifactID,
		claims.ObjectVersion,
		claims.OutputSetDigest,
		claims.IssuedAt.UnixNano(),
		claims.ExpiresAt.UnixNano(),
	)
	token := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	digest := sha256.Sum256([]byte(token))
	return token, digest[:], nil
}
