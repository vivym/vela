package stagecache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/h3request"
)

func TestH3ExactReconcilerAdmitsAndHitsThroughBackend(t *testing.T) {
	now := time.Date(2026, time.September, 2, 8, 0, 0, 0, time.UTC)
	projectID := uuid.MustParse("4a000000-0000-0000-0000-000000000002")
	encoder := validH3ExactCandidate(t, "ADMIT", "ENCODER", projectID)
	encoder.ArtifactExpiresAt = now.Add(30 * time.Minute)
	dit := validH3ExactCandidate(t, "HIT", "DIT", projectID)
	dit.DependencyDigests = []string{digestString("encoder artifact")}
	alternativeProfile := dit
	alternativeProfile.StageProfileRevisionID = uuid.New()
	alternativeProfile.StageProfileContentDigest = digestString("alternative StageProfile")
	entryID := uuid.MustParse("4a000000-0000-0000-0000-000000000099")
	backend := &fakeH3ExactCacheBackend{
		candidates: map[string][]H3ExactCandidate{
			"ADMIT": {encoder},
			"HIT":   {dit, alternativeProfile},
		},
		entryID: entryID,
		found:   true,
	}
	reconciler, err := NewH3ExactReconciler(backend, H3ExactReconcilerConfig{
		ProjectScopeKeys: map[uuid.UUID][]byte{
			projectID: []byte("project-specific-H3-cache-key-material"),
		},
		InputCanonicalizationRevisionID: uuid.MustParse("4a000000-0000-0000-0000-000000000003"),
		SeedAndRNGRevision:              "sglang-minimax-h3-philox-v1",
		BatchSize:                       20,
		ExpectedSavedComputeMinor:       50_000,
		CarryCostMinor:                  300,
		Now:                             func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewH3ExactReconciler: %v", err)
	}

	result, err := reconciler.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result != (H3ExactReconcileResult{
		AdmissionCandidates: 1, Admitted: 1, HitCandidates: 2, Hits: 1,
	}) {
		t.Fatalf("reconcile result = %#v", result)
	}
	if len(backend.admissions) != 1 || len(backend.hits) != 1 || len(backend.lookups) != 1 {
		t.Fatalf(
			"backend calls admissions=%d lookups=%d hits=%d",
			len(backend.admissions), len(backend.lookups), len(backend.hits),
		)
	}
	admission := backend.admissions[0]
	if admission.ArtifactID != encoder.ArtifactID || admission.StageKey != "encoder" ||
		admission.Scope != ScopeProject || admission.ExpectedSavedComputeMinor != 50_000 ||
		admission.CarryCostMinor != 300 || !admission.AdmittedAt.Equal(now) ||
		!admission.ExpiresAt.Equal(encoder.ArtifactExpiresAt.Add(-time.Microsecond)) ||
		admission.CacheKeyDigest == (Digest{}) {
		t.Fatalf("admission command = %#v", admission)
	}
	lookup := backend.lookups[0]
	if lookup.candidate.StageRunID != dit.StageRunID || lookup.key == (Digest{}) ||
		lookup.key == admission.CacheKeyDigest || !lookup.observedAt.Equal(now) {
		t.Fatalf("lookup = %#v", lookup)
	}
	hit := backend.hits[0]
	if hit.EntryID != entryID || hit.AttemptID != dit.AttemptID ||
		hit.StageRunID != dit.StageRunID || hit.StageProfileRevisionID != dit.StageProfileRevisionID ||
		hit.ExpectedAttemptFence != dit.AttemptFence || hit.ExpectedStageFence != dit.StageFence ||
		hit.ExpectedStageVersion != dit.StageVersion || hit.CacheKeyDigest != lookup.key ||
		!hit.HitAt.Equal(now) {
		t.Fatalf("hit command = %#v", hit)
	}
}

func TestH3ExactReconcilerSkipsProjectsWithoutScopeKeys(t *testing.T) {
	enabledProject := uuid.MustParse("4a000000-0000-0000-0000-000000000012")
	disabledProject := uuid.MustParse("4a000000-0000-0000-0000-000000000013")
	backend := &fakeH3ExactCacheBackend{
		candidates: map[string][]H3ExactCandidate{
			"ADMIT": {validH3ExactCandidate(t, "ADMIT", "ENCODER", disabledProject)},
			"HIT":   {validH3ExactCandidate(t, "HIT", "DIT", disabledProject)},
		},
	}
	reconciler, err := NewH3ExactReconciler(backend, H3ExactReconcilerConfig{
		ProjectScopeKeys: map[uuid.UUID][]byte{
			enabledProject: []byte("enabled-project-H3-cache-key-material"),
		},
		InputCanonicalizationRevisionID: uuid.MustParse("4a000000-0000-0000-0000-000000000014"),
		SeedAndRNGRevision:              "sglang-minimax-h3-philox-v1",
		BatchSize:                       10,
	})
	if err != nil {
		t.Fatalf("NewH3ExactReconciler: %v", err)
	}

	result, err := reconciler.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Skipped != 2 || len(backend.admissions) != 0 ||
		len(backend.lookups) != 0 || len(backend.hits) != 0 {
		t.Fatalf("fail-closed result=%#v backend=%#v", result, backend)
	}
}

func TestH3ExactReconcilerRejectsCandidateAuthorityBeforeMutation(t *testing.T) {
	projectID := uuid.MustParse("4a000000-0000-0000-0000-000000000022")
	candidate := validH3ExactCandidate(t, "ADMIT", "DIT", projectID)
	candidate.RequestHash = "not-a-digest"
	backend := &fakeH3ExactCacheBackend{
		candidates: map[string][]H3ExactCandidate{"ADMIT": {candidate}},
	}
	reconciler, err := NewH3ExactReconciler(backend, H3ExactReconcilerConfig{
		ProjectScopeKeys: map[uuid.UUID][]byte{
			projectID: []byte("authority-test-H3-cache-key-material"),
		},
		InputCanonicalizationRevisionID: uuid.MustParse("4a000000-0000-0000-0000-000000000023"),
		SeedAndRNGRevision:              "sglang-minimax-h3-philox-v1",
		BatchSize:                       10,
	})
	if err != nil {
		t.Fatalf("NewH3ExactReconciler: %v", err)
	}

	result, err := reconciler.Reconcile(context.Background())
	if err == nil || result.Admitted != 0 || len(backend.admissions) != 0 {
		t.Fatalf("invalid candidate result=%#v error=%v backend=%#v", result, err, backend)
	}
}

type fakeH3ExactCacheBackend struct {
	candidates map[string][]H3ExactCandidate
	entryID    uuid.UUID
	found      bool
	readErr    map[string]error
	admitErr   error
	findErr    error
	hitErr     error
	admissions []AdmitCommand
	lookups    []h3ExactLookup
	hits       []HitCommand
}

type h3ExactLookup struct {
	candidate  H3ExactCandidate
	key        Digest
	observedAt time.Time
}

func (backend *fakeH3ExactCacheBackend) ReadH3ExactCandidates(
	_ context.Context,
	action string,
	_ int,
) ([]H3ExactCandidate, error) {
	if err := backend.readErr[action]; err != nil {
		return nil, err
	}
	return append([]H3ExactCandidate(nil), backend.candidates[action]...), nil
}

func (backend *fakeH3ExactCacheBackend) FindH3ExactEntry(
	_ context.Context,
	candidate H3ExactCandidate,
	key Digest,
	observedAt time.Time,
) (uuid.UUID, bool, error) {
	backend.lookups = append(backend.lookups, h3ExactLookup{candidate, key, observedAt})
	return backend.entryID, backend.found, backend.findErr
}

func (backend *fakeH3ExactCacheBackend) AdmitH3Exact(
	_ context.Context,
	command AdmitCommand,
) (AdmitDecision, error) {
	backend.admissions = append(backend.admissions, command)
	return AdmitDecision{}, backend.admitErr
}

func (backend *fakeH3ExactCacheBackend) Hit(
	_ context.Context,
	command HitCommand,
) (HitDecision, error) {
	backend.hits = append(backend.hits, command)
	return HitDecision{}, backend.hitErr
}

func validH3ExactCandidate(
	t *testing.T,
	action string,
	stageKind string,
	projectID uuid.UUID,
) H3ExactCandidate {
	t.Helper()
	seed := int64(17)
	frozen, err := h3request.Freeze("cache this request", "balanced", "cache-test", h3request.Request{
		Task: "ref2va",
		Seed: &seed,
		Conditions: []h3request.Condition{{
			Role: "reference", Type: "image", URI: "vela://uploads/reference",
			DownloadURL: "https://objects.example.test/reference?signature=secret",
			SHA256:      digestString("root input"), SizeBytes: 4096,
		}},
	})
	if err != nil {
		t.Fatalf("freeze H3 cache request: %v", err)
	}
	requestContent, err := json.Marshal(struct {
		H3 h3request.FrozenRequest `json:"h3"`
	}{H3: frozen})
	if err != nil {
		t.Fatalf("encode H3 cache request: %v", err)
	}
	requestHash := sha256.Sum256(requestContent)
	now := time.Date(2026, time.September, 2, 7, 0, 0, 0, time.UTC)
	candidate := H3ExactCandidate{
		Action:                      action,
		OrganizationID:              uuid.MustParse("4a000000-0000-0000-0000-000000000001"),
		ProjectID:                   projectID,
		AttemptID:                   uuid.New(),
		AttemptFence:                3,
		StageRunID:                  uuid.New(),
		StageFence:                  5,
		StageVersion:                7,
		StageKey:                    map[string]string{"ENCODER": "encoder", "DIT": "dit"}[stageKind],
		StageKind:                   stageKind,
		CachePolicyRevisionID:       uuid.MustParse("4a000000-0000-0000-0000-000000000004"),
		CacheTTLSeconds:             3600,
		StageProfileRevisionID:      uuid.New(),
		ResultEquivalenceRevisionID: uuid.New(),
		StageProfileContentDigest:   digestString("stage profile"),
		RequestContent:              requestContent,
		RequestHash:                 hex.EncodeToString(requestHash[:]),
		ArtifactID:                  uuid.New(),
		ArtifactCommittedAt:         now,
		ArtifactExpiresAt:           now.Add(2 * time.Hour),
	}
	if stageKind == "DIT" {
		candidate.DependencyDigests = []string{digestString("encoder artifact")}
	}
	return candidate
}

func digestString(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

var _ H3ExactCacheBackend = (*fakeH3ExactCacheBackend)(nil)
