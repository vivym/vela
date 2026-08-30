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
)

type StageGraphVisibleCompletionCandidate struct {
	CompletionID       uuid.UUID
	ExpectedJobVersion int64
}

type inspectedStageGraphArtifact struct {
	source                  StageGraphFinalizationSource
	artifactID              uuid.UUID
	verificationID          uuid.UUID
	verificationRequestHash [sha256.Size]byte
	validationReceipt       []byte
}

func (s *Service) CompleteStageGraphVisibleCompletion(
	ctx context.Context,
	finalizer AuthenticatedFinalizer,
	credentials StageGraphFinalizationCredentials,
	candidate StageGraphVisibleCompletionCandidate,
) (VisibleCompletionResult, error) {
	if s == nil || s.pool == nil {
		return VisibleCompletionResult{}, errors.New("worker coordinator is not configured")
	}
	if ctx == nil || !validPrintableFailureText(finalizer.ID, 500) ||
		credentials.ClaimID == uuid.Nil || credentials.AttemptID == uuid.Nil ||
		credentials.Fence <= 0 || credentials.Token == "" ||
		candidate.CompletionID == uuid.Nil || candidate.ExpectedJobVersion <= 0 {
		return rejectedVisibleCompletion(), nil
	}

	authorityRow, sources, normalized, terminal, err := s.preflightStageGraphVisibleCompletion(
		ctx, finalizer, credentials, candidate,
	)
	if err != nil || terminal.Decision != "" {
		return terminal, err
	}
	if s.artifactInspector == nil {
		return VisibleCompletionResult{}, errors.New("artifact inspector is not configured")
	}
	inspected, err := s.inspectStageGraphVisibleCompletionArtifacts(ctx, authorityRow, sources, normalized)
	if err != nil {
		return VisibleCompletionResult{}, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("begin Stage graph Visible Completion commit: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	locked, err := queries.LockStageGraphFinalizationCompletionAuthority(
		ctx,
		store.LockStageGraphFinalizationCompletionAuthorityParams{
			ClaimID: credentials.ClaimID, AttemptID: credentials.AttemptID,
			AttemptFence: credentials.Fence,
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedVisibleCompletion(), nil
	}
	if err != nil {
		return VisibleCompletionResult{}, fmt.Errorf("lock Stage graph Visible Completion authority: %w", err)
	}
	if !validStageGraphVisibleCompletionIdentity(locked, finalizer, credentials) {
		return rejectedVisibleCompletion(), nil
	}
	authority := stageGraphVisibleCompletionAuthority(locked)
	if replay, ok, replayErr := stageGraphVisibleCompletionReplay(
		ctx, queries, locked, authority, normalized,
	); ok || replayErr != nil {
		if replayErr != nil {
			return VisibleCompletionResult{}, replayErr
		}
		if err := tx.Commit(ctx); err != nil {
			return VisibleCompletionResult{}, fmt.Errorf("commit Stage graph Visible Completion replay: %w", err)
		}
		return replay, nil
	}
	now, err := postgresTime(ctx, queries)
	if err != nil {
		return VisibleCompletionResult{}, err
	}
	if !activeStageGraphVisibleCompletionAuthority(locked, credentials, normalized, now) {
		return rejectedVisibleCompletion(), nil
	}
	lockedSources, outputSetDigest, err := loadStageGraphFinalizationClaimSources(
		ctx, queries, credentials.ClaimID,
	)
	if err != nil {
		return VisibleCompletionResult{}, err
	}
	if !hmac.Equal(outputSetDigest[:], locked.OutputSetDigest) ||
		!sameStageGraphFinalizationSources(sources, lockedSources) {
		return rejectedVisibleCompletion(), nil
	}

	return commitVisibleCompletion(
		ctx, tx, queries, authority, normalized, now,
		visibleCompletionCommitActor{
			stageGraph: &visibleCompletionStageGraphCommit{
				claimID: locked.ClaimID, ownerID: locked.OwnerID, fence: locked.AttemptFence,
			},
			prepareArtifacts: func(
				prepareCtx context.Context,
				prepareQueries *store.Queries,
				verifiedAt time.Time,
			) error {
				return insertVerifiedStageGraphArtifacts(
					prepareCtx, prepareQueries, authority, inspected, verifiedAt,
				)
			},
		},
	)
}

func (s *Service) preflightStageGraphVisibleCompletion(
	ctx context.Context,
	finalizer AuthenticatedFinalizer,
	credentials StageGraphFinalizationCredentials,
	candidate StageGraphVisibleCompletionCandidate,
) (
	store.LockStageGraphFinalizationCompletionAuthorityRow,
	[]StageGraphFinalizationSource,
	normalizedVisibleCompletionCandidate,
	VisibleCompletionResult,
	error,
) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return store.LockStageGraphFinalizationCompletionAuthorityRow{}, nil,
			normalizedVisibleCompletionCandidate{}, VisibleCompletionResult{},
			fmt.Errorf("begin Stage graph Visible Completion preflight: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	authorityRow, err := queries.LockStageGraphFinalizationCompletionAuthority(
		ctx,
		store.LockStageGraphFinalizationCompletionAuthorityParams{
			ClaimID: credentials.ClaimID, AttemptID: credentials.AttemptID,
			AttemptFence: credentials.Fence,
		},
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return authorityRow, nil, normalizedVisibleCompletionCandidate{}, rejectedVisibleCompletion(), nil
	}
	if err != nil {
		return authorityRow, nil, normalizedVisibleCompletionCandidate{}, VisibleCompletionResult{},
			fmt.Errorf("lock Stage graph Visible Completion authority: %w", err)
	}
	if !validStageGraphVisibleCompletionIdentity(authorityRow, finalizer, credentials) {
		return authorityRow, nil, normalizedVisibleCompletionCandidate{}, rejectedVisibleCompletion(), nil
	}
	sources, outputSetDigest, err := loadStageGraphFinalizationClaimSources(
		ctx, queries, credentials.ClaimID,
	)
	if err != nil {
		return authorityRow, nil, normalizedVisibleCompletionCandidate{}, VisibleCompletionResult{}, err
	}
	if !hmac.Equal(outputSetDigest[:], authorityRow.OutputSetDigest) {
		return authorityRow, nil, normalizedVisibleCompletionCandidate{}, rejectedVisibleCompletion(), nil
	}
	normalized, valid := normalizeStageGraphVisibleCompletionCandidate(candidate, sources)
	if !valid {
		return authorityRow, nil, normalized, VisibleCompletionResult{
			Decision: VisibleCompletionCandidateConflict,
		}, nil
	}
	authority := stageGraphVisibleCompletionAuthority(authorityRow)
	if replay, ok, replayErr := stageGraphVisibleCompletionReplay(
		ctx, queries, authorityRow, authority, normalized,
	); ok || replayErr != nil {
		if replayErr != nil {
			return authorityRow, nil, normalized, VisibleCompletionResult{}, replayErr
		}
		if err := tx.Commit(ctx); err != nil {
			return authorityRow, nil, normalized, VisibleCompletionResult{},
				fmt.Errorf("commit Stage graph Visible Completion replay: %w", err)
		}
		return authorityRow, sources, normalized, replay, nil
	}
	if authority.JobState == store.JobStateCANCELING ||
		authority.JobState == store.JobStateCANCELED ||
		authority.RequestContentDeletedAt.Valid {
		return authorityRow, nil, normalized, VisibleCompletionResult{
			Decision: VisibleCompletionCancellationWon,
		}, nil
	}
	if authority.JobState == store.JobStateFAILED {
		return authorityRow, nil, normalized, VisibleCompletionResult{
			Decision: VisibleCompletionAlreadyFailed,
		}, nil
	}
	now, err := postgresTime(ctx, queries)
	if err != nil {
		return authorityRow, nil, normalized, VisibleCompletionResult{}, err
	}
	if !activeStageGraphVisibleCompletionAuthority(authorityRow, credentials, normalized, now) {
		return authorityRow, nil, normalized, rejectedVisibleCompletion(), nil
	}
	if !completeStageGraphArtifactKinds(sources, authorityRow.GenerationCount) {
		return authorityRow, nil, normalized, VisibleCompletionResult{
			Decision: VisibleCompletionIncompleteArtifact,
		}, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return authorityRow, nil, normalized, VisibleCompletionResult{},
			fmt.Errorf("commit Stage graph Visible Completion preflight: %w", err)
	}
	return authorityRow, sources, normalized, VisibleCompletionResult{}, nil
}

func validStageGraphVisibleCompletionIdentity(
	authority store.LockStageGraphFinalizationCompletionAuthorityRow,
	finalizer AuthenticatedFinalizer,
	credentials StageGraphFinalizationCredentials,
) bool {
	presentedDigest := sha256.Sum256([]byte(credentials.Token))
	return authority.ClaimID == credentials.ClaimID &&
		authority.AttemptID == credentials.AttemptID &&
		authority.AttemptFence == credentials.Fence &&
		authority.OwnerID == finalizer.ID &&
		hmac.Equal(presentedDigest[:], authority.TokenDigest) &&
		len(authority.OutputSetDigest) == sha256.Size &&
		authority.CurrentAttemptFence == credentials.Fence &&
		authority.CurrentFence == credentials.Fence
}

func activeStageGraphVisibleCompletionAuthority(
	authority store.LockStageGraphFinalizationCompletionAuthorityRow,
	credentials StageGraphFinalizationCredentials,
	normalized normalizedVisibleCompletionCandidate,
	now time.Time,
) bool {
	return authority.ClaimState == store.StageGraphFinalizationClaimStateACTIVE &&
		authority.ClaimExpiresAt.Valid && authority.ClaimExpiresAt.Time.After(now) &&
		authority.AttemptState == store.AttemptStateFINALIZING &&
		authority.GraphState != nil && *authority.GraphState == store.GraphAttemptStateFINALIZING &&
		!authority.WorkerID.Valid && authority.CurrentAttemptFence == credentials.Fence &&
		authority.JobState == store.JobStateFINALIZING &&
		authority.CurrentFence == credentials.Fence &&
		authority.JobVersion == normalized.expectedJobVersion &&
		authority.CreditReservationState == store.CreditReservationStateRESERVED &&
		!authority.RequestContentDeletedAt.Valid &&
		authority.FinalizationDeadlineAt.Valid && authority.FinalizationDeadlineAt.Time.After(now) &&
		authority.JobExpiresAt.Valid && authority.JobExpiresAt.Time.After(now)
}

func normalizeStageGraphVisibleCompletionCandidate(
	candidate StageGraphVisibleCompletionCandidate,
	sources []StageGraphFinalizationSource,
) (normalizedVisibleCompletionCandidate, bool) {
	artifactIDs := make([]uuid.UUID, 0, len(sources))
	for _, source := range sources {
		artifactIDs = append(artifactIDs, stageGraphVisibleArtifactID(candidate.CompletionID, source))
	}
	return normalizeVisibleCompletionCandidate(VisibleCompletionCandidate{
		CompletionID: candidate.CompletionID, ExpectedJobVersion: candidate.ExpectedJobVersion,
		ArtifactIDs: artifactIDs,
	})
}

func stageGraphVisibleArtifactID(
	completionID uuid.UUID,
	source StageGraphFinalizationSource,
) uuid.UUID {
	return uuid.NewSHA1(
		completionID,
		[]byte(fmt.Sprintf(
			"vela-stage-graph-artifact-v1\n%s\n%s\n%d\n%s\n%s",
			source.OutputKey, source.ArtifactKind, source.Ordinal,
			source.StageArtifactID, source.ObjectVersion,
		)),
	)
}

func stageGraphVisibleCompletionReplay(
	ctx context.Context,
	queries *store.Queries,
	row store.LockStageGraphFinalizationCompletionAuthorityRow,
	authority visibleCompletionAuthority,
	normalized normalizedVisibleCompletionCandidate,
) (VisibleCompletionResult, bool, error) {
	if !authority.CompletionID.Valid {
		return VisibleCompletionResult{}, false, nil
	}
	result, err := committedVisibleCompletionResult(ctx, queries, authority)
	if err != nil {
		return VisibleCompletionResult{}, true, err
	}
	if authority.CompletionAuthorityLeaseID.Valid ||
		!authority.CompletionAuthorityStageGraphFinalizationClaimID.Valid {
		return VisibleCompletionResult{}, true,
			errors.New("visible Completion has no winning Stage graph claim identity")
	}
	switch {
	case authority.CompletionAuthorityStageGraphFinalizationClaimID.UUID != row.ClaimID:
		result.Decision = VisibleCompletionAlreadySucceeded
	case authority.CompletionID.UUID == normalized.completionID &&
		hmac.Equal(authority.CandidateSha256, normalized.hash[:]):
		result.Decision = VisibleCompletionCommitted
	case authority.CompletionID.UUID == normalized.completionID:
		result.Decision = VisibleCompletionCandidateConflict
	default:
		result.Decision = VisibleCompletionAlreadySucceeded
	}
	return result, true, nil
}

func (s *Service) inspectStageGraphVisibleCompletionArtifacts(
	ctx context.Context,
	authority store.LockStageGraphFinalizationCompletionAuthorityRow,
	sources []StageGraphFinalizationSource,
	normalized normalizedVisibleCompletionCandidate,
) ([]inspectedStageGraphArtifact, error) {
	artifactIDs := make(map[uuid.UUID]struct{}, len(normalized.artifactIDs))
	for _, artifactID := range normalized.artifactIDs {
		artifactIDs[artifactID] = struct{}{}
	}
	inspected := make([]inspectedStageGraphArtifact, 0, len(sources))
	for _, source := range sources {
		artifactID := stageGraphVisibleArtifactID(normalized.completionID, source)
		if _, ok := artifactIDs[artifactID]; !ok {
			return nil, errors.New("Stage graph Artifact candidate identity is inconsistent")
		}
		request, err := applyArtifactInspectionExpectations(ArtifactInspectionRequest{
			ArtifactID: artifactID, Kind: source.ArtifactKind, Ordinal: source.Ordinal,
			ObjectKey: source.ObjectKey, ObjectVersionID: source.ObjectVersion,
			ExpectedSizeBytes: source.SizeBytes, ExpectedSHA256: source.SHA256,
			ExpectedContentType: source.ContentType,
		}, artifactInspectionExpectations{
			width: authority.ExpectedWidth, height: authority.ExpectedHeight,
			durationMilliseconds: authority.ExpectedDurationMilliseconds,
			frameRateMilli:       authority.ExpectedFrameRateMilli,
			codec:                authority.ExpectedCodec, container: authority.ExpectedContainer,
		})
		if err != nil {
			return nil, err
		}
		inspection, err := s.artifactInspector.Inspect(ctx, request)
		if err != nil {
			return nil, fmt.Errorf(
				"inspect exact Stage graph Artifact object version %s: %w", source.OutputKey, err,
			)
		}
		receipt, requestHash, valid := validateArtifactInspection(request, inspection)
		if !valid {
			return nil, fmt.Errorf("validate exact Stage graph Artifact %s: inspection mismatch", source.OutputKey)
		}
		inspected = append(inspected, inspectedStageGraphArtifact{
			source: source, artifactID: artifactID,
			verificationID:          uuid.NewSHA1(artifactID, []byte("vela-stage-graph-verification-v1")),
			verificationRequestHash: requestHash, validationReceipt: receipt,
		})
	}
	return inspected, nil
}

func insertVerifiedStageGraphArtifacts(
	ctx context.Context,
	queries *store.Queries,
	authority visibleCompletionAuthority,
	inspected []inspectedStageGraphArtifact,
	verifiedAt time.Time,
) error {
	observedAt := pgtype.Timestamptz{Time: verifiedAt, Valid: true}
	for _, artifact := range inspected {
		if !artifact.source.ExpiresAt.After(verifiedAt) {
			return errors.New("Stage graph Artifact expired before Visible Completion")
		}
		objectVersionID := artifact.source.ObjectVersion
		sizeBytes := artifact.source.SizeBytes
		if err := queries.InsertVerifiedStageGraphArtifact(
			ctx,
			store.InsertVerifiedStageGraphArtifactParams{
				ID: artifact.artifactID, OrganizationID: authority.OrganizationID,
				ProjectID: authority.ProjectID, JobID: authority.JobID,
				AttemptID: authority.AttemptID, AttemptFence: authority.AttemptFence,
				Kind: store.ArtifactKind(artifact.source.ArtifactKind), Ordinal: artifact.source.Ordinal,
				ObjectKey: artifact.source.ObjectKey, ContentType: artifact.source.ContentType,
				ObjectVersionID: &objectVersionID, SizeBytes: &sizeBytes,
				Sha256: artifact.source.SHA256[:], VerifiedAt: observedAt,
				VerificationID:          uuid.NullUUID{UUID: artifact.verificationID, Valid: true},
				VerificationRequestHash: artifact.verificationRequestHash[:],
				ValidationReceipt:       artifact.validationReceipt,
				ExpiresAt:               pgtype.Timestamptz{Time: artifact.source.ExpiresAt, Valid: true},
				SourceStageArtifactID: uuid.NullUUID{
					UUID: artifact.source.StageArtifactID, Valid: true,
				},
			},
		); err != nil {
			return fmt.Errorf("bind Stage graph output %s as verified Artifact: %w", artifact.source.OutputKey, err)
		}
	}
	return nil
}

func sameStageGraphFinalizationSources(
	left []StageGraphFinalizationSource,
	right []StageGraphFinalizationSource,
) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].OutputKey != right[index].OutputKey ||
			left[index].ArtifactKind != right[index].ArtifactKind ||
			left[index].Ordinal != right[index].Ordinal ||
			left[index].StageRunID != right[index].StageRunID ||
			left[index].StageArtifactID != right[index].StageArtifactID ||
			left[index].StageInterfaceRevisionID != right[index].StageInterfaceRevisionID ||
			left[index].ObjectKey != right[index].ObjectKey ||
			left[index].ObjectVersion != right[index].ObjectVersion ||
			left[index].ContentType != right[index].ContentType ||
			left[index].SHA256 != right[index].SHA256 ||
			left[index].SizeBytes != right[index].SizeBytes ||
			!left[index].ExpiresAt.Equal(right[index].ExpiresAt) {
			return false
		}
	}
	return true
}

func completeStageGraphArtifactKinds(
	sources []StageGraphFinalizationSource,
	generationCount int32,
) bool {
	if generationCount <= 0 || len(sources) != int(generationCount)*2 {
		return false
	}
	seen := make(map[string]struct{}, len(sources))
	for _, source := range sources {
		if source.Ordinal < 0 || source.Ordinal >= generationCount ||
			(source.ArtifactKind != ArtifactKindVideo &&
				source.ArtifactKind != ArtifactKindThumbnail) {
			return false
		}
		identity := fmt.Sprintf("%s/%d", source.ArtifactKind, source.Ordinal)
		if _, duplicate := seen[identity]; duplicate {
			return false
		}
		seen[identity] = struct{}{}
	}
	for ordinal := int32(0); ordinal < generationCount; ordinal++ {
		for _, kind := range []ArtifactKind{ArtifactKindVideo, ArtifactKindThumbnail} {
			if _, ok := seen[fmt.Sprintf("%s/%d", kind, ordinal)]; !ok {
				return false
			}
		}
	}
	return true
}
