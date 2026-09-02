package stagecache

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/strictjson"
)

type Digest = [sha256.Size]byte

type Scope string

const (
	ScopeProject      Scope = "PROJECT"
	ScopeOrganization Scope = "ORGANIZATION"
)

type EquivalenceMode string

const (
	EquivalenceBitwise       EquivalenceMode = "BITWISE"
	EquivalenceCanonicalByte EquivalenceMode = "CANONICAL_BYTES"
	EquivalenceTolerance     EquivalenceMode = "TOLERANCE"
	EquivalenceQuality       EquivalenceMode = "QUALITY"
)

type KeyInput struct {
	Scope                            Scope
	OrganizationID                   uuid.UUID
	ProjectID                        uuid.UUID
	StageKind                        string
	StageResultEquivalenceRevisionID uuid.UUID
	EquivalenceMode                  EquivalenceMode
	InputCanonicalizationRevisionID  uuid.UUID
	RootInputDigests                 []Digest
	InputStageArtifactDigests        []Digest
	NormalizedStageParameters        json.RawMessage
	SeedAndRNGRevision               string
	SeedDigest                       Digest
	OutputShape                      json.RawMessage
	AdapterAndLoRADigests            []Digest
}

type canonicalKeyV1 struct {
	SchemaVersion                    int             `json:"schema_version"`
	Scope                            Scope           `json:"scope"`
	OrganizationID                   string          `json:"organization_id"`
	ProjectID                        string          `json:"project_id,omitempty"`
	StageKind                        string          `json:"stage_kind"`
	StageResultEquivalenceRevisionID string          `json:"stage_result_equivalence_revision_id"`
	EquivalenceMode                  EquivalenceMode `json:"equivalence_mode"`
	InputCanonicalizationRevisionID  string          `json:"input_canonicalization_revision_id"`
	RootInputDigests                 []string        `json:"root_input_digests"`
	InputStageArtifactDigests        []string        `json:"input_stage_artifact_digests"`
	NormalizedStageParameters        json.RawMessage `json:"normalized_stage_parameters"`
	SeedAndRNGRevision               string          `json:"seed_and_rng_revision"`
	SeedDigest                       string          `json:"seed_digest"`
	OutputShape                      json.RawMessage `json:"output_shape"`
	AdapterAndLoRADigests            []string        `json:"adapter_and_lora_digests"`
}

var stageKindPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,99}$`)

func ComputeKeyV1(scopeKey []byte, input KeyInput) (Digest, error) {
	if len(scopeKey) < sha256.Size {
		return Digest{}, errors.New("StageCacheKeyV1 scope key must contain at least 32 bytes")
	}
	canonical, err := canonicalizeKeyInput(input)
	if err != nil {
		return Digest{}, err
	}
	payload, err := json.Marshal(canonical)
	if err != nil {
		return Digest{}, fmt.Errorf("encode StageCacheKeyV1: %w", err)
	}
	mac := hmac.New(sha256.New, scopeKey)
	_, _ = mac.Write([]byte("vela-stage-cache-key-v1\n"))
	_, _ = mac.Write(payload)
	var digest Digest
	copy(digest[:], mac.Sum(nil))
	return digest, nil
}

func canonicalizeKeyInput(input KeyInput) (canonicalKeyV1, error) {
	if input.OrganizationID == uuid.Nil || !stageKindPattern.MatchString(input.StageKind) ||
		input.StageResultEquivalenceRevisionID == uuid.Nil ||
		input.InputCanonicalizationRevisionID == uuid.Nil ||
		(input.EquivalenceMode != EquivalenceBitwise &&
			input.EquivalenceMode != EquivalenceCanonicalByte) ||
		len(input.RootInputDigests)+len(input.InputStageArtifactDigests) == 0 ||
		len(input.RootInputDigests) > 64 || len(input.InputStageArtifactDigests) > 64 ||
		len(input.AdapterAndLoRADigests) > 64 ||
		input.SeedDigest == (Digest{}) || input.SeedAndRNGRevision == "" ||
		len(input.SeedAndRNGRevision) > 100 {
		return canonicalKeyV1{}, errors.New("StageCacheKeyV1 exact identity is incomplete")
	}
	if (input.Scope == ScopeProject && input.ProjectID == uuid.Nil) ||
		(input.Scope == ScopeOrganization && input.ProjectID != uuid.Nil) ||
		(input.Scope != ScopeProject && input.Scope != ScopeOrganization) {
		return canonicalKeyV1{}, errors.New("StageCacheKeyV1 scope identity is invalid")
	}
	for _, digest := range append(
		append(slices.Clone(input.RootInputDigests), input.InputStageArtifactDigests...),
		input.AdapterAndLoRADigests...,
	) {
		if digest == (Digest{}) {
			return canonicalKeyV1{}, errors.New("StageCacheKeyV1 contains an empty digest")
		}
	}
	parameters, err := canonicalJSON(input.NormalizedStageParameters, 64<<10)
	if err != nil {
		return canonicalKeyV1{}, fmt.Errorf("canonicalize StageCacheKeyV1 parameters: %w", err)
	}
	outputShape, err := canonicalJSON(input.OutputShape, 16<<10)
	if err != nil {
		return canonicalKeyV1{}, fmt.Errorf("canonicalize StageCacheKeyV1 output shape: %w", err)
	}
	projectID := ""
	if input.ProjectID != uuid.Nil {
		projectID = input.ProjectID.String()
	}
	return canonicalKeyV1{
		SchemaVersion: 1, Scope: input.Scope, OrganizationID: input.OrganizationID.String(),
		ProjectID: projectID, StageKind: input.StageKind,
		StageResultEquivalenceRevisionID: input.StageResultEquivalenceRevisionID.String(),
		EquivalenceMode:                  input.EquivalenceMode,
		InputCanonicalizationRevisionID:  input.InputCanonicalizationRevisionID.String(),
		RootInputDigests:                 digestStrings(input.RootInputDigests),
		InputStageArtifactDigests:        digestStrings(input.InputStageArtifactDigests),
		NormalizedStageParameters:        parameters,
		SeedAndRNGRevision:               input.SeedAndRNGRevision,
		SeedDigest:                       hex.EncodeToString(input.SeedDigest[:]),
		OutputShape:                      outputShape,
		AdapterAndLoRADigests:            digestStrings(input.AdapterAndLoRADigests),
	}, nil
}

func canonicalJSON(value json.RawMessage, maxBytes int) (json.RawMessage, error) {
	if len(value) == 0 || len(value) > maxBytes {
		return nil, errors.New("canonical JSON is empty or exceeds its bound")
	}
	if err := strictjson.RejectDuplicateKeys(value); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return nil, err
	}
	return canonical, nil
}

func digestStrings(digests []Digest) []string {
	values := make([]string, len(digests))
	for index, digest := range digests {
		values[index] = hex.EncodeToString(digest[:])
	}
	return values
}

type EntryState string

const (
	EntryLive            EntryState = "LIVE"
	EntryInvalid         EntryState = "INVALID"
	EntryRetired         EntryState = "RETIRED"
	EntryDeletionBlocked EntryState = "DELETION_BLOCKED"
)

type ShadowEntry struct {
	ID                        uuid.UUID
	State                     EntryState
	SizeBytes                 int64
	ExpectedSavedComputeMinor int64
	CarryCostMinor            int64
	LastAccessedAt            time.Time
	ExpiresAt                 time.Time
	Pinned                    bool
}

type Quota struct {
	MaxEntries int
	MaxBytes   int64
}

type EvictionReason string

const (
	EvictionExpired  EvictionReason = "EXPIRED"
	EvictionInvalid  EvictionReason = "INVALID"
	EvictionRetired  EvictionReason = "RETIRED"
	EvictionDeletion EvictionReason = "DELETION_REQUESTED"
	EvictionQuota    EvictionReason = "QUOTA"
)

type ShadowEviction struct {
	EntryID uuid.UUID
	Reason  EvictionReason
}

type evictionCandidate struct {
	entry  ShadowEntry
	reason EvictionReason
}

func PlanShadowEviction(
	now time.Time,
	entries []ShadowEntry,
	quota Quota,
) ([]ShadowEviction, error) {
	if now.IsZero() || quota.MaxEntries <= 0 || quota.MaxBytes <= 0 {
		return nil, errors.New("shadow eviction quota is invalid")
	}
	seen := make(map[uuid.UUID]struct{}, len(entries))
	activeCount := 0
	activeBytes := int64(0)
	mandatory := make([]evictionCandidate, 0)
	valueCandidates := make([]ShadowEntry, 0)
	for _, entry := range entries {
		if entry.ID == uuid.Nil || entry.SizeBytes <= 0 || entry.LastAccessedAt.IsZero() ||
			entry.ExpiresAt.IsZero() || entry.ExpectedSavedComputeMinor < 0 ||
			entry.CarryCostMinor < 0 {
			return nil, errors.New("shadow eviction entry is invalid")
		}
		if _, duplicate := seen[entry.ID]; duplicate {
			return nil, errors.New("shadow eviction entry identity is duplicated")
		}
		seen[entry.ID] = struct{}{}
		activeCount++
		if entry.SizeBytes > math.MaxInt64-activeBytes {
			return nil, errors.New("shadow eviction total size exceeds int64")
		}
		activeBytes += entry.SizeBytes
		if entry.Pinned {
			continue
		}
		reason := EvictionReason("")
		switch {
		case !entry.ExpiresAt.After(now):
			reason = EvictionExpired
		case entry.State == EntryInvalid:
			reason = EvictionInvalid
		case entry.State == EntryRetired:
			reason = EvictionRetired
		case entry.State == EntryDeletionBlocked:
			reason = EvictionDeletion
		case entry.State == EntryLive:
			valueCandidates = append(valueCandidates, entry)
		default:
			return nil, errors.New("shadow eviction entry state is unsupported")
		}
		if reason != "" {
			mandatory = append(mandatory, evictionCandidate{entry: entry, reason: reason})
		}
	}
	sort.Slice(mandatory, func(left, right int) bool {
		if mandatory[left].reason != mandatory[right].reason {
			return evictionReasonPriority(mandatory[left].reason) <
				evictionReasonPriority(mandatory[right].reason)
		}
		if !mandatory[left].entry.LastAccessedAt.Equal(mandatory[right].entry.LastAccessedAt) {
			return mandatory[left].entry.LastAccessedAt.Before(mandatory[right].entry.LastAccessedAt)
		}
		return mandatory[left].entry.ID.String() < mandatory[right].entry.ID.String()
	})
	sort.Slice(valueCandidates, func(left, right int) bool {
		leftValue := valueCandidates[left].ExpectedSavedComputeMinor - valueCandidates[left].CarryCostMinor
		rightValue := valueCandidates[right].ExpectedSavedComputeMinor - valueCandidates[right].CarryCostMinor
		leftScaled := new(big.Int).Mul(big.NewInt(leftValue), big.NewInt(valueCandidates[right].SizeBytes))
		rightScaled := new(big.Int).Mul(big.NewInt(rightValue), big.NewInt(valueCandidates[left].SizeBytes))
		if comparison := leftScaled.Cmp(rightScaled); comparison != 0 {
			return comparison < 0
		}
		if !valueCandidates[left].LastAccessedAt.Equal(valueCandidates[right].LastAccessedAt) {
			return valueCandidates[left].LastAccessedAt.Before(valueCandidates[right].LastAccessedAt)
		}
		return valueCandidates[left].ID.String() < valueCandidates[right].ID.String()
	})
	result := make([]ShadowEviction, 0)
	selected := make(map[uuid.UUID]struct{})
	selectEntry := func(entry ShadowEntry, reason EvictionReason) {
		if _, exists := selected[entry.ID]; exists {
			return
		}
		selected[entry.ID] = struct{}{}
		activeCount--
		activeBytes -= entry.SizeBytes
		result = append(result, ShadowEviction{EntryID: entry.ID, Reason: reason})
	}
	for _, candidate := range mandatory {
		selectEntry(candidate.entry, candidate.reason)
	}
	for _, entry := range valueCandidates {
		if activeCount <= quota.MaxEntries && activeBytes <= quota.MaxBytes {
			break
		}
		selectEntry(entry, EvictionQuota)
	}
	return result, nil
}

func evictionReasonPriority(reason EvictionReason) int {
	switch reason {
	case EvictionExpired:
		return 0
	case EvictionInvalid:
		return 1
	case EvictionRetired:
		return 2
	case EvictionDeletion:
		return 3
	default:
		return 4
	}
}
