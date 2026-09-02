package stagecache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/vivym/vela/internal/h3request"
)

type H3ExactCandidate struct {
	Action                      string          `json:"action"`
	OrganizationID              uuid.UUID       `json:"organization_id"`
	ProjectID                   uuid.UUID       `json:"project_id"`
	AttemptID                   uuid.UUID       `json:"attempt_id"`
	AttemptFence                int64           `json:"attempt_fence"`
	StageRunID                  uuid.UUID       `json:"stage_run_id"`
	StageFence                  int64           `json:"stage_fence"`
	StageVersion                int64           `json:"stage_version"`
	StageKey                    string          `json:"stage_key"`
	StageKind                   string          `json:"stage_kind"`
	CachePolicyRevisionID       uuid.UUID       `json:"cache_policy_revision_id"`
	CacheTTLSeconds             int64           `json:"cache_ttl_seconds"`
	StageProfileRevisionID      uuid.UUID       `json:"stage_profile_revision_id"`
	ResultEquivalenceRevisionID uuid.UUID       `json:"result_equivalence_revision_id"`
	StageProfileContentDigest   string          `json:"stage_profile_content_digest"`
	RequestContent              json.RawMessage `json:"request_content"`
	RequestHash                 string          `json:"request_hash"`
	DependencyDigests           []string        `json:"dependency_digests"`
	ArtifactID                  uuid.UUID       `json:"artifact_id,omitempty"`
	ArtifactCommittedAt         time.Time       `json:"artifact_committed_at,omitempty"`
	ArtifactExpiresAt           time.Time       `json:"artifact_expires_at,omitempty"`
}

type H3ExactCacheBackend interface {
	ReadH3ExactCandidates(context.Context, string, int) ([]H3ExactCandidate, error)
	FindH3ExactEntry(context.Context, H3ExactCandidate, Digest, time.Time) (uuid.UUID, bool, error)
	Admit(context.Context, AdmitCommand) (AdmitDecision, error)
	Hit(context.Context, HitCommand) (HitDecision, error)
}

type H3ExactReconcilerConfig struct {
	ProjectScopeKeys                map[uuid.UUID][]byte
	InputCanonicalizationRevisionID uuid.UUID
	SeedAndRNGRevision              string
	BatchSize                       int
	ExpectedSavedComputeMinor       int64
	CarryCostMinor                  int64
	Now                             func() time.Time
}

type H3ExactReconcileResult struct {
	AdmissionCandidates int
	Admitted            int
	HitCandidates       int
	Hits                int
	Skipped             int
}

type H3ExactReconciler struct {
	backend H3ExactCacheBackend
	config  H3ExactReconcilerConfig
}

func NewH3ExactReconciler(
	backend H3ExactCacheBackend,
	config H3ExactReconcilerConfig,
) (*H3ExactReconciler, error) {
	if backend == nil || config.InputCanonicalizationRevisionID == uuid.Nil ||
		strings.TrimSpace(config.SeedAndRNGRevision) == "" ||
		len(config.SeedAndRNGRevision) > 100 || config.BatchSize < 1 || config.BatchSize > 1000 ||
		config.ExpectedSavedComputeMinor < 0 || config.CarryCostMinor < 0 ||
		len(config.ProjectScopeKeys) == 0 {
		return nil, errors.New("H3 exact cache reconciler configuration is incomplete")
	}
	keys := make(map[uuid.UUID][]byte, len(config.ProjectScopeKeys))
	for projectID, key := range config.ProjectScopeKeys {
		if projectID == uuid.Nil || len(key) < sha256.Size {
			return nil, errors.New("H3 exact cache project scope key is invalid")
		}
		keys[projectID] = append([]byte(nil), key...)
	}
	config.ProjectScopeKeys = keys
	if config.Now == nil {
		config.Now = time.Now
	}
	return &H3ExactReconciler{backend: backend, config: config}, nil
}

func (reconciler *H3ExactReconciler) Reconcile(
	ctx context.Context,
) (H3ExactReconcileResult, error) {
	if reconciler == nil || reconciler.backend == nil || ctx == nil {
		return H3ExactReconcileResult{}, errors.New("H3 exact cache reconciler is not configured")
	}
	result := H3ExactReconcileResult{}
	var failures []error
	admissions, err := reconciler.backend.ReadH3ExactCandidates(ctx, "ADMIT", reconciler.config.BatchSize)
	if err != nil {
		return result, fmt.Errorf("read H3 exact cache admission candidates: %w", err)
	}
	result.AdmissionCandidates = len(admissions)
	for _, candidate := range admissions {
		key, enabled, keyErr := reconciler.key(candidate, "ADMIT")
		if keyErr != nil {
			failures = append(failures, keyErr)
			continue
		}
		if !enabled {
			result.Skipped++
			continue
		}
		now := reconciler.config.Now().UTC()
		expiresAt := now.Add(time.Duration(candidate.CacheTTLSeconds) * time.Second)
		if !candidate.ArtifactExpiresAt.IsZero() && !candidate.ArtifactExpiresAt.After(expiresAt) {
			// PostgreSQL rounds timestamptz to microseconds; preserve the strict
			// artifact.expires_at > cache.expires_at authority check after encoding.
			expiresAt = candidate.ArtifactExpiresAt.Add(-time.Microsecond)
		}
		if !expiresAt.After(now) {
			result.Skipped++
			continue
		}
		_, err := reconciler.backend.Admit(ctx, AdmitCommand{
			CommandID:                   deterministicCacheUUID(candidate.StageRunID, "admit-command/"+candidate.ArtifactID.String()),
			EntryID:                     deterministicCacheUUID(candidate.StageRunID, "entry/"+candidate.ArtifactID.String()),
			ArtifactID:                  candidate.ArtifactID,
			CachePolicyRevisionID:       candidate.CachePolicyRevisionID,
			StageProfileRevisionID:      candidate.StageProfileRevisionID,
			ResultEquivalenceRevisionID: candidate.ResultEquivalenceRevisionID,
			Scope:                       ScopeProject, StageKey: candidate.StageKey, CacheKeyDigest: key,
			ExpectedSavedComputeMinor: reconciler.config.ExpectedSavedComputeMinor,
			CarryCostMinor:            reconciler.config.CarryCostMinor,
			AdmittedAt:                now, ExpiresAt: expiresAt,
		})
		if err != nil {
			failures = append(failures, fmt.Errorf("admit H3 %s exact cache entry: %w", candidate.StageKey, err))
			continue
		}
		result.Admitted++
	}

	hits, err := reconciler.backend.ReadH3ExactCandidates(ctx, "HIT", reconciler.config.BatchSize)
	if err != nil {
		failures = append(failures, fmt.Errorf("read H3 exact cache hit candidates: %w", err))
		return result, errors.Join(failures...)
	}
	result.HitCandidates = len(hits)
	hitStageRuns := make(map[uuid.UUID]struct{})
	for _, candidate := range hits {
		if _, alreadyHit := hitStageRuns[candidate.StageRunID]; alreadyHit {
			continue
		}
		key, enabled, keyErr := reconciler.key(candidate, "HIT")
		if keyErr != nil {
			failures = append(failures, keyErr)
			continue
		}
		if !enabled {
			result.Skipped++
			continue
		}
		now := reconciler.config.Now().UTC()
		entryID, found, findErr := reconciler.backend.FindH3ExactEntry(ctx, candidate, key, now)
		if findErr != nil {
			failures = append(failures, fmt.Errorf("find H3 %s exact cache entry: %w", candidate.StageKey, findErr))
			continue
		}
		if !found {
			continue
		}
		_, err := reconciler.backend.Hit(ctx, HitCommand{
			CommandID: deterministicCacheUUID(candidate.StageRunID, "hit-command/"+hex.EncodeToString(key[:])),
			EntryID:   entryID,
			PinID:     deterministicCacheUUID(candidate.StageRunID, "hit-pin/"+entryID.String()),
			AttemptID: candidate.AttemptID, StageRunID: candidate.StageRunID,
			StageProfileRevisionID: candidate.StageProfileRevisionID,
			ExpectedOrganizationID: candidate.OrganizationID,
			ExpectedProjectID:      candidate.ProjectID,
			ExpectedAttemptFence:   candidate.AttemptFence,
			ExpectedStageFence:     candidate.StageFence,
			ExpectedStageVersion:   candidate.StageVersion,
			ProgressReceiptID:      deterministicCacheUUID(candidate.StageRunID, "hit-progress/"+entryID.String()),
			CacheKeyDigest:         key, HitAt: now,
		})
		if err != nil {
			failures = append(failures, fmt.Errorf("apply H3 %s exact cache hit: %w", candidate.StageKey, err))
			continue
		}
		result.Hits++
		hitStageRuns[candidate.StageRunID] = struct{}{}
	}
	return result, errors.Join(failures...)
}

func (reconciler *H3ExactReconciler) key(
	candidate H3ExactCandidate,
	expectedAction string,
) (Digest, bool, error) {
	scopeKey, enabled := reconciler.config.ProjectScopeKeys[candidate.ProjectID]
	if !enabled {
		return Digest{}, false, nil
	}
	if candidate.Action != expectedAction ||
		candidate.OrganizationID == uuid.Nil || candidate.AttemptID == uuid.Nil ||
		candidate.StageRunID == uuid.Nil || candidate.CachePolicyRevisionID == uuid.Nil ||
		candidate.StageProfileRevisionID == uuid.Nil ||
		candidate.ResultEquivalenceRevisionID == uuid.Nil ||
		candidate.AttemptFence <= 0 || candidate.StageFence <= 0 || candidate.StageVersion <= 0 ||
		(candidate.StageKind != "ENCODER" && candidate.StageKind != "DIT") ||
		strings.ToLower(candidate.StageKind) != candidate.StageKey ||
		candidate.CacheTTLSeconds <= 0 {
		return Digest{}, false, errors.New("H3 exact cache candidate authority is invalid")
	}
	if _, err := decodeH3ExactDigest(candidate.RequestHash); err != nil {
		return Digest{}, false, errors.New("H3 exact cache request digest is invalid")
	}
	if expectedAction == "ADMIT" &&
		(candidate.ArtifactID == uuid.Nil || candidate.ArtifactCommittedAt.IsZero() ||
			!candidate.ArtifactExpiresAt.After(candidate.ArtifactCommittedAt)) {
		return Digest{}, false, errors.New("H3 exact cache Artifact authority is invalid")
	}
	var request struct {
		H3 h3request.FrozenRequest `json:"h3"`
	}
	if err := json.Unmarshal(candidate.RequestContent, &request); err != nil ||
		request.H3.Parameters.SchemaRevision != h3request.StageParametersRevision ||
		request.H3.Parameters.CanonicalRequest.Schema != h3request.CanonicalRequestSchema {
		return Digest{}, false, errors.New("H3 exact cache candidate request snapshot is invalid")
	}
	parameters, err := json.Marshal(request.H3.Parameters)
	if err != nil {
		return Digest{}, false, fmt.Errorf("encode H3 exact cache parameters: %w", err)
	}
	outputShape, err := json.Marshal(request.H3.Parameters.CanonicalRequest.Target)
	if err != nil {
		return Digest{}, false, fmt.Errorf("encode H3 exact cache output shape: %w", err)
	}
	rootDigests := make([]Digest, 0, len(request.H3.RootInputs))
	if candidate.StageKind == "ENCODER" {
		for _, root := range request.H3.RootInputs {
			digest, digestErr := decodeH3ExactDigest(root.SHA256)
			if digestErr != nil {
				return Digest{}, false, errors.New("H3 exact cache root input digest is invalid")
			}
			rootDigests = append(rootDigests, digest)
		}
	}
	dependencyDigests := make([]Digest, 0, len(candidate.DependencyDigests))
	for _, encoded := range candidate.DependencyDigests {
		digest, digestErr := decodeH3ExactDigest(encoded)
		if digestErr != nil {
			return Digest{}, false, errors.New("H3 exact cache dependency digest is invalid")
		}
		dependencyDigests = append(dependencyDigests, digest)
	}
	if len(rootDigests)+len(dependencyDigests) == 0 {
		rootDigests = append(rootDigests, sha256.Sum256(parameters))
	}
	profileDigest, err := decodeH3ExactDigest(candidate.StageProfileContentDigest)
	if err != nil {
		return Digest{}, false, errors.New("H3 exact cache StageProfile digest is invalid")
	}
	seedDigest := sha256.Sum256([]byte(fmt.Sprintf(
		"vela/h3-stage-cache-seed/v1\x00%s\x00%d",
		reconciler.config.SeedAndRNGRevision,
		request.H3.Parameters.CanonicalRequest.Seed,
	)))
	key, err := ComputeKeyV1(scopeKey, KeyInput{
		Scope: ScopeProject, OrganizationID: candidate.OrganizationID, ProjectID: candidate.ProjectID,
		StageKind:                        candidate.StageKey,
		StageResultEquivalenceRevisionID: candidate.ResultEquivalenceRevisionID,
		EquivalenceMode:                  EquivalenceBitwise,
		InputCanonicalizationRevisionID:  reconciler.config.InputCanonicalizationRevisionID,
		RootInputDigests:                 rootDigests, InputStageArtifactDigests: dependencyDigests,
		NormalizedStageParameters: parameters,
		SeedAndRNGRevision:        reconciler.config.SeedAndRNGRevision, SeedDigest: seedDigest,
		OutputShape: outputShape, AdapterAndLoRADigests: []Digest{profileDigest},
	})
	if err != nil {
		return Digest{}, false, fmt.Errorf("compute H3 exact cache key: %w", err)
	}
	return key, true, nil
}

func (repository *PostgresRepository) ReadH3ExactCandidates(
	ctx context.Context,
	action string,
	limit int,
) ([]H3ExactCandidate, error) {
	if err := repository.validate(); err != nil {
		return nil, err
	}
	if ctx == nil || (action != "ADMIT" && action != "HIT") || limit < 1 || limit > 1000 {
		return nil, errors.New("H3 exact cache candidate query is incomplete")
	}
	rows, err := repository.pool.Query(ctx, `
		SELECT candidate
		FROM vela_read_h3_exact_cache_candidates($1, $2) AS candidate
	`, action, limit)
	if err != nil {
		return nil, fmt.Errorf("query H3 exact cache candidates: %w", err)
	}
	defer rows.Close()
	candidates := make([]H3ExactCandidate, 0)
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return nil, fmt.Errorf("scan H3 exact cache candidate: %w", err)
		}
		var candidate H3ExactCandidate
		if err := json.Unmarshal(encoded, &candidate); err != nil {
			return nil, fmt.Errorf("decode H3 exact cache candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read H3 exact cache candidates: %w", err)
	}
	return candidates, nil
}

func (repository *PostgresRepository) FindH3ExactEntry(
	ctx context.Context,
	candidate H3ExactCandidate,
	key Digest,
	observedAt time.Time,
) (uuid.UUID, bool, error) {
	if err := repository.validate(); err != nil {
		return uuid.Nil, false, err
	}
	if ctx == nil || candidate.OrganizationID == uuid.Nil || candidate.ProjectID == uuid.Nil ||
		candidate.CachePolicyRevisionID == uuid.Nil || candidate.StageProfileRevisionID == uuid.Nil ||
		candidate.ResultEquivalenceRevisionID == uuid.Nil || candidate.StageKey == "" ||
		key == (Digest{}) || observedAt.IsZero() {
		return uuid.Nil, false, errors.New("H3 exact cache entry lookup is incomplete")
	}
	var entryID uuid.UUID
	err := repository.pool.QueryRow(ctx, `
		SELECT entry_id
		FROM vela_find_h3_exact_cache_entry($1, $2, $3, $4, $5, $6, $7, $8)
	`, candidate.OrganizationID, candidate.ProjectID, candidate.CachePolicyRevisionID,
		candidate.StageProfileRevisionID, candidate.ResultEquivalenceRevisionID,
		candidate.StageKey, key[:], observedAt.UTC()).Scan(&entryID)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("lookup H3 exact cache entry: %w", err)
	}
	return entryID, true, nil
}

func decodeH3ExactDigest(value string) (Digest, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != value {
		return Digest{}, errors.New("digest is not canonical SHA-256")
	}
	var digest Digest
	copy(digest[:], decoded)
	return digest, nil
}

func deterministicCacheUUID(namespace uuid.UUID, value string) uuid.UUID {
	return uuid.NewSHA1(namespace, []byte("vela/h3-exact-cache/v1/"+value))
}

var _ H3ExactCacheBackend = (*PostgresRepository)(nil)
