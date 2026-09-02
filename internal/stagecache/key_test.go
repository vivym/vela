package stagecache_test

import (
	"crypto/sha256"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stagecache"
)

func TestStageCacheKeyV1EveryExactFieldAffectsScopedHMAC(t *testing.T) {
	base := exactKeyInput()
	secret := []byte("project-scoped-cache-key-0123456789abcdef")
	want, err := stagecache.ComputeKeyV1(secret, base)
	if err != nil {
		t.Fatalf("ComputeKeyV1: %v", err)
	}
	mutations := map[string]func(*stagecache.KeyInput){
		"scope": func(input *stagecache.KeyInput) {
			input.Scope = stagecache.ScopeOrganization
			input.ProjectID = uuid.Nil
		},
		"organization": func(input *stagecache.KeyInput) { input.OrganizationID = uuid.New() },
		"project":      func(input *stagecache.KeyInput) { input.ProjectID = uuid.New() },
		"stage kind":   func(input *stagecache.KeyInput) { input.StageKind = "dit-v2" },
		"result equivalence": func(input *stagecache.KeyInput) {
			input.StageResultEquivalenceRevisionID = uuid.New()
		},
		"equivalence mode": func(input *stagecache.KeyInput) {
			input.EquivalenceMode = stagecache.EquivalenceCanonicalByte
		},
		"canonicalization": func(input *stagecache.KeyInput) {
			input.InputCanonicalizationRevisionID = uuid.New()
		},
		"root input": func(input *stagecache.KeyInput) {
			input.RootInputDigests[0] = sha256.Sum256([]byte("another root"))
		},
		"stage input": func(input *stagecache.KeyInput) {
			input.InputStageArtifactDigests[0] = sha256.Sum256([]byte("another artifact"))
		},
		"parameters": func(input *stagecache.KeyInput) {
			input.NormalizedStageParameters = json.RawMessage(`{"guidance":8}`)
		},
		"seed and RNG revision": func(input *stagecache.KeyInput) {
			input.SeedAndRNGRevision = "philox-v2"
		},
		"seed": func(input *stagecache.KeyInput) {
			input.SeedDigest = sha256.Sum256([]byte("another seed"))
		},
		"output shape": func(input *stagecache.KeyInput) {
			input.OutputShape = json.RawMessage(`{"frames":121,"height":1080,"width":1920}`)
		},
		"adapter and LoRA": func(input *stagecache.KeyInput) {
			input.AdapterAndLoRADigests[0] = sha256.Sum256([]byte("another adapter"))
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := cloneKeyInput(base)
			mutate(&changed)
			got, err := stagecache.ComputeKeyV1(secret, changed)
			if err != nil {
				t.Fatalf("ComputeKeyV1 changed input: %v", err)
			}
			if got == want {
				t.Fatal("changed exact field produced the same StageCacheKeyV1")
			}
		})
	}

	reordered := cloneKeyInput(base)
	reordered.NormalizedStageParameters = json.RawMessage(`{"steps":40,"guidance":7}`)
	reordered.OutputShape = json.RawMessage(`{"width":1920,"frames":120,"height":1080}`)
	got, err := stagecache.ComputeKeyV1(secret, reordered)
	if err != nil || got != want {
		t.Fatalf("canonical JSON key = %x error=%v, want %x", got, err, want)
	}
	otherSecret, err := stagecache.ComputeKeyV1(
		[]byte("another-project-cache-key-0123456789abcdef"), base,
	)
	if err != nil || otherSecret == want {
		t.Fatalf("different scope secret key = %x error=%v", otherSecret, err)
	}
}

func TestH3EncoderAndDiTExactCacheKeysAreRetryStableAndProjectIsolated(t *testing.T) {
	projectSecret := []byte("h3-project-cache-key-0123456789abcdef")
	encoder := exactKeyInput()
	encoder.StageKind = "encoder"
	encoder.RootInputDigests = []stagecache.Digest{
		sha256.Sum256([]byte("verified root material")),
	}
	encoder.InputStageArtifactDigests = nil
	encoder.StageResultEquivalenceRevisionID = uuid.MustParse(
		"49000000-0000-0000-0000-000000000023",
	)
	encoder.NormalizedStageParameters = json.RawMessage(
		`{"conditions":[{"media_type":"image","uri":"s3://project/input.png"}],"prompt":"cache me"}`,
	)
	encoder.OutputShape = json.RawMessage(`{"conditioning_revision":"h3-encoder-v1"}`)

	encoderKey, err := stagecache.ComputeKeyV1(projectSecret, encoder)
	if err != nil {
		t.Fatalf("compute H3 Encoder cache key: %v", err)
	}
	retryKey, err := stagecache.ComputeKeyV1(projectSecret, cloneKeyInput(encoder))
	if err != nil || retryKey != encoderKey {
		t.Fatalf("H3 Encoder retry key = %x error=%v, want %x", retryKey, err, encoderKey)
	}

	changedRoot := cloneKeyInput(encoder)
	changedRoot.RootInputDigests[0] = sha256.Sum256([]byte("different root material"))
	changedRootKey, err := stagecache.ComputeKeyV1(projectSecret, changedRoot)
	if err != nil || changedRootKey == encoderKey {
		t.Fatalf("H3 Encoder root-digest isolation key = %x error=%v", changedRootKey, err)
	}

	otherProject := cloneKeyInput(encoder)
	otherProject.ProjectID = uuid.MustParse("49800000-0000-0000-0000-000000000099")
	otherProjectKey, err := stagecache.ComputeKeyV1(
		[]byte("other-h3-project-key-0123456789abcdef"),
		otherProject,
	)
	if err != nil || otherProjectKey == encoderKey {
		t.Fatalf("cross-Project H3 Encoder key = %x error=%v", otherProjectKey, err)
	}

	dit := cloneKeyInput(encoder)
	dit.StageKind = "dit"
	dit.StageResultEquivalenceRevisionID = uuid.MustParse(
		"49000000-0000-0000-0000-000000000024",
	)
	dit.RootInputDigests = nil
	dit.InputStageArtifactDigests = []stagecache.Digest{
		sha256.Sum256([]byte("encoder artifact")),
	}
	dit.OutputShape = json.RawMessage(`{"audio_latents":true,"video_latents":true}`)
	ditKey, err := stagecache.ComputeKeyV1(projectSecret, dit)
	if err != nil || ditKey == encoderKey {
		t.Fatalf("H3 DiT cache key = %x error=%v, Encoder key %x", ditKey, err, encoderKey)
	}
	changedInput := cloneKeyInput(dit)
	changedInput.InputStageArtifactDigests[0] = sha256.Sum256([]byte("different encoder artifact"))
	changedInputKey, err := stagecache.ComputeKeyV1(projectSecret, changedInput)
	if err != nil || changedInputKey == ditKey {
		t.Fatalf("H3 DiT artifact-digest isolation key = %x error=%v", changedInputKey, err)
	}
}

func TestPlanShadowEvictionComparesValueDensityWithoutOverflow(t *testing.T) {
	now := time.Date(2026, time.August, 31, 1, 0, 0, 0, time.UTC)
	lowDensity := stagecache.ShadowEntry{
		ID: uuid.MustParse("49800000-0000-0000-0000-000000000021"), State: stagecache.EntryLive,
		SizeBytes: 1_000_000_000_000, ExpectedSavedComputeMinor: 1_000_000_000_000,
		LastAccessedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	highDensity := stagecache.ShadowEntry{
		ID: uuid.MustParse("49800000-0000-0000-0000-000000000022"), State: stagecache.EntryLive,
		SizeBytes: 1, ExpectedSavedComputeMinor: 999_999_999_999,
		LastAccessedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	got, err := stagecache.PlanShadowEviction(
		now,
		[]stagecache.ShadowEntry{highDensity, lowDensity},
		stagecache.Quota{MaxEntries: 1, MaxBytes: math.MaxInt64},
	)
	if err != nil {
		t.Fatalf("PlanShadowEviction: %v", err)
	}
	if len(got) != 1 || got[0].EntryID != lowDensity.ID {
		t.Fatalf("shadow eviction = %#v, want low-density entry %s", got, lowDensity.ID)
	}
}

func TestStageCacheKeyV1RejectsApproximateEquivalenceAndInvalidScope(t *testing.T) {
	secret := []byte("project-scoped-cache-key-0123456789abcdef")
	for _, mode := range []stagecache.EquivalenceMode{
		stagecache.EquivalenceTolerance, stagecache.EquivalenceQuality,
	} {
		input := exactKeyInput()
		input.EquivalenceMode = mode
		if _, err := stagecache.ComputeKeyV1(secret, input); err == nil {
			t.Fatalf("ComputeKeyV1 accepted approximate equivalence mode %q", mode)
		}
	}
	for _, mutate := range []func(*stagecache.KeyInput){
		func(input *stagecache.KeyInput) { input.ProjectID = uuid.Nil },
		func(input *stagecache.KeyInput) {
			input.Scope = stagecache.ScopeOrganization
		},
		func(input *stagecache.KeyInput) {
			input.NormalizedStageParameters = json.RawMessage(`{"guidance":7,"guidance":8}`)
		},
	} {
		input := exactKeyInput()
		mutate(&input)
		if _, err := stagecache.ComputeKeyV1(secret, input); err == nil {
			t.Fatal("ComputeKeyV1 accepted an invalid scoped or ambiguous input")
		}
	}
}

func TestPlanShadowEvictionIsDeterministicValueAwareAndPinSafe(t *testing.T) {
	now := time.Date(2026, time.August, 31, 1, 0, 0, 0, time.UTC)
	entries := []stagecache.ShadowEntry{
		{ID: uuid.MustParse("49800000-0000-0000-0000-000000000001"), State: stagecache.EntryLive,
			SizeBytes: 60, ExpectedSavedComputeMinor: 100, CarryCostMinor: 10,
			LastAccessedAt: now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour)},
		{ID: uuid.MustParse("49800000-0000-0000-0000-000000000002"), State: stagecache.EntryLive,
			SizeBytes: 40, ExpectedSavedComputeMinor: 5, CarryCostMinor: 10,
			LastAccessedAt: now.Add(-2 * time.Hour), ExpiresAt: now.Add(time.Hour)},
		{ID: uuid.MustParse("49800000-0000-0000-0000-000000000003"), State: stagecache.EntryLive,
			SizeBytes: 80, ExpectedSavedComputeMinor: 0, CarryCostMinor: 20,
			LastAccessedAt: now.Add(-3 * time.Hour), ExpiresAt: now.Add(-time.Minute)},
		{ID: uuid.MustParse("49800000-0000-0000-0000-000000000004"), State: stagecache.EntryDeletionBlocked,
			SizeBytes: 100, ExpectedSavedComputeMinor: 0, CarryCostMinor: 100,
			LastAccessedAt: now.Add(-4 * time.Hour), ExpiresAt: now.Add(time.Hour), Pinned: true},
	}
	want := []uuid.UUID{entries[2].ID, entries[1].ID}
	for iteration := 0; iteration < 20; iteration++ {
		got, err := stagecache.PlanShadowEviction(now, entries, stagecache.Quota{
			MaxEntries: 2, MaxBytes: 160,
		})
		if err != nil {
			t.Fatalf("PlanShadowEviction: %v", err)
		}
		if len(got) != len(want) {
			t.Fatalf("shadow eviction count = %d, want %d: %#v", len(got), len(want), got)
		}
		for index := range want {
			if got[index].EntryID != want[index] {
				t.Fatalf("shadow eviction[%d] = %s, want %s", index, got[index].EntryID, want[index])
			}
		}
	}
}

func exactKeyInput() stagecache.KeyInput {
	return stagecache.KeyInput{
		Scope: stagecache.ScopeProject, OrganizationID: uuid.MustParse("49800000-0000-0000-0000-000000000010"),
		ProjectID: uuid.MustParse("49800000-0000-0000-0000-000000000011"), StageKind: "dit",
		StageResultEquivalenceRevisionID: uuid.MustParse("49800000-0000-0000-0000-000000000012"),
		EquivalenceMode:                  stagecache.EquivalenceBitwise,
		InputCanonicalizationRevisionID:  uuid.MustParse("49800000-0000-0000-0000-000000000013"),
		RootInputDigests:                 []stagecache.Digest{sha256.Sum256([]byte("root input"))},
		InputStageArtifactDigests:        []stagecache.Digest{sha256.Sum256([]byte("conditioning"))},
		NormalizedStageParameters:        json.RawMessage(`{"guidance":7,"steps":40}`),
		SeedAndRNGRevision:               "philox-v1", SeedDigest: sha256.Sum256([]byte("seed 42")),
		OutputShape:           json.RawMessage(`{"frames":120,"height":1080,"width":1920}`),
		AdapterAndLoRADigests: []stagecache.Digest{sha256.Sum256([]byte("base adapter"))},
	}
}

func cloneKeyInput(input stagecache.KeyInput) stagecache.KeyInput {
	input.RootInputDigests = append([]stagecache.Digest(nil), input.RootInputDigests...)
	input.InputStageArtifactDigests = append(
		[]stagecache.Digest(nil), input.InputStageArtifactDigests...,
	)
	input.NormalizedStageParameters = append(json.RawMessage(nil), input.NormalizedStageParameters...)
	input.OutputShape = append(json.RawMessage(nil), input.OutputShape...)
	input.AdapterAndLoRADigests = append(
		[]stagecache.Digest(nil), input.AdapterAndLoRADigests...,
	)
	return input
}
