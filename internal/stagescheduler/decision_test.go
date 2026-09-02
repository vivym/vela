package stagescheduler

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestDecideReproducesWinnerAndEvidenceAcrossCandidateOrder(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 4, 0, 0, 0, time.UTC)
	first := schedulingCandidate(
		"51000000-0000-0000-0000-000000000001",
		"51000000-0000-0000-0000-000000000101",
		"51000000-0000-0000-0000-000000000201",
		now.Add(-time.Minute),
	)
	second := schedulingCandidate(
		"51000000-0000-0000-0000-000000000002",
		"51000000-0000-0000-0000-000000000102",
		"51000000-0000-0000-0000-000000000202",
		now.Add(-time.Minute),
	)
	first.Score.LocalityCreditMillis = 50
	second.Score.LocalityCreditMillis = 10

	forward := schedulingSnapshot(now, []Candidate{first, second})
	reverse := schedulingSnapshot(now, []Candidate{second, first})

	forwardEvidence, ok, err := Decide(forward)
	if err != nil || !ok {
		t.Fatalf("Decide(forward) ok=%t error=%v", ok, err)
	}
	reverseEvidence, ok, err := Decide(reverse)
	if err != nil || !ok {
		t.Fatalf("Decide(reverse) ok=%t error=%v", ok, err)
	}
	if forwardEvidence.SelectedStageRunID != first.StageRunID {
		t.Fatalf("selected StageRun = %s, want %s", forwardEvidence.SelectedStageRunID, first.StageRunID)
	}
	if !reflect.DeepEqual(forwardEvidence, reverseEvidence) {
		t.Fatalf("candidate order changed evidence:\nforward=%#v\nreverse=%#v", forwardEvidence, reverseEvidence)
	}
}

func TestDecideFailsClosedWhenSnapshotEvidenceIsStale(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 4, 0, 0, 0, time.UTC)
	snapshot := schedulingSnapshot(now, []Candidate{schedulingCandidate(
		"51000000-0000-0000-0000-000000000003",
		"51000000-0000-0000-0000-000000000103",
		"51000000-0000-0000-0000-000000000203",
		now.Add(-time.Minute),
	)})
	snapshot.ValidUntil = now

	_, ok, err := Decide(snapshot)
	if ok || !errors.Is(err, ErrStaleSnapshot) {
		t.Fatalf("Decide(stale) ok=%t error=%v, want fail-closed stale error", ok, err)
	}
}

func TestDecideAppliesHierarchicalFairnessBeforeLocalityScore(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 4, 0, 0, 0, time.UTC)
	fair := schedulingCandidate(
		"51000000-0000-0000-0000-000000000004",
		"51000000-0000-0000-0000-000000000104",
		"51000000-0000-0000-0000-000000000204",
		now.Add(-time.Minute),
	)
	local := schedulingCandidate(
		"51000000-0000-0000-0000-000000000005",
		"51000000-0000-0000-0000-000000000105",
		"51000000-0000-0000-0000-000000000205",
		now.Add(-time.Minute),
	)
	fair.OrganizationDeficitMillis = 100_000
	local.OrganizationDeficitMillis = 90_000
	fair.Score.TransferPenaltyMillis = 10_000
	local.Score.LocalityCreditMillis = 100_000

	evidence, ok, err := Decide(schedulingSnapshot(now, []Candidate{local, fair}))
	if err != nil || !ok {
		t.Fatalf("Decide(fairness) ok=%t error=%v", ok, err)
	}
	if evidence.SelectedStageRunID != fair.StageRunID {
		t.Fatalf("selected StageRun = %s, want fairness winner %s", evidence.SelectedStageRunID, fair.StageRunID)
	}
}

func TestDecideNeverLetsFilteredLocalityCandidateBecomeEligible(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 4, 0, 0, 0, time.UTC)
	eligible := schedulingCandidate(
		"51000000-0000-0000-0000-000000000006",
		"51000000-0000-0000-0000-000000000106",
		"51000000-0000-0000-0000-000000000206",
		now.Add(-time.Minute),
	)
	filtered := schedulingCandidate(
		"51000000-0000-0000-0000-000000000007",
		"51000000-0000-0000-0000-000000000107",
		"51000000-0000-0000-0000-000000000207",
		now.Add(-time.Minute),
	)
	filtered.FilterReasons = []FilterReason{FilterModelResidencyStale}
	filtered.Score.LocalityCreditMillis = 1_000_000

	evidence, ok, err := Decide(schedulingSnapshot(now, []Candidate{filtered, eligible}))
	if err != nil || !ok {
		t.Fatalf("Decide(filtered) ok=%t error=%v", ok, err)
	}
	if evidence.SelectedStageRunID != eligible.StageRunID {
		t.Fatalf("selected StageRun = %s, want eligible %s", evidence.SelectedStageRunID, eligible.StageRunID)
	}
	if evidence.FilterReasonCounts[FilterModelResidencyStale] != 1 {
		t.Fatalf("filter reason counts = %#v", evidence.FilterReasonCounts)
	}
}

func TestDecideAppliesLanePriorityBeforeFairnessAndScore(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 4, 0, 0, 0, time.UTC)
	retry := schedulingCandidate(
		"51000000-0000-0000-0000-000000000008",
		"51000000-0000-0000-0000-000000000108",
		"51000000-0000-0000-0000-000000000208",
		now.Add(-time.Minute),
	)
	normal := schedulingCandidate(
		"51000000-0000-0000-0000-000000000009",
		"51000000-0000-0000-0000-000000000109",
		"51000000-0000-0000-0000-000000000209",
		now.Add(-time.Minute),
	)
	retry.Lane = LaneRetry
	retry.OrganizationDeficitMillis = 1
	retry.Score.TransferPenaltyMillis = 1_000_000
	normal.OrganizationDeficitMillis = 1_000_000
	normal.Score.LocalityCreditMillis = 1_000_000

	evidence, ok, err := Decide(schedulingSnapshot(now, []Candidate{normal, retry}))
	if err != nil || !ok {
		t.Fatalf("Decide(lane priority) ok=%t error=%v", ok, err)
	}
	if evidence.SelectedStageRunID != retry.StageRunID {
		t.Fatalf("selected StageRun = %s, want retry lane winner %s", evidence.SelectedStageRunID, retry.StageRunID)
	}
}

func TestDecideAppliesServiceClassThenProjectFairness(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 4, 0, 0, 0, time.UTC)
	serviceWinner := schedulingCandidate(
		"51000000-0000-0000-0000-000000000012",
		"51000000-0000-0000-0000-000000000110",
		"51000000-0000-0000-0000-000000000210",
		now.Add(-time.Minute),
	)
	projectWinner := schedulingCandidate(
		"51000000-0000-0000-0000-000000000013",
		"51000000-0000-0000-0000-000000000110",
		"51000000-0000-0000-0000-000000000211",
		now.Add(-time.Minute),
	)
	lowerService := schedulingCandidate(
		"51000000-0000-0000-0000-000000000014",
		"51000000-0000-0000-0000-000000000110",
		"51000000-0000-0000-0000-000000000212",
		now.Add(-time.Minute),
	)
	serviceWinner.ServiceClassDeficitMillis = 200
	projectWinner.ServiceClassDeficitMillis = 200
	lowerService.ServiceClassRevisionID = uuid.MustParse("51000000-0000-0000-0000-000000000023")
	lowerService.ServiceClassDeficitMillis = 100
	lowerService.ProjectDeficitMillis = 1_000_000
	serviceWinner.ProjectDeficitMillis = 100
	projectWinner.ProjectDeficitMillis = 200

	evidence, ok, err := Decide(schedulingSnapshot(now, []Candidate{lowerService, serviceWinner, projectWinner}))
	if err != nil || !ok {
		t.Fatalf("Decide(hierarchical fairness) ok=%t error=%v", ok, err)
	}
	if evidence.SelectedStageRunID != projectWinner.StageRunID {
		t.Fatalf("selected StageRun = %s, want project fairness winner %s", evidence.SelectedStageRunID, projectWinner.StageRunID)
	}
}

func TestDecideUsesDeterministicTieBreak(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 4, 0, 0, 0, time.UTC)
	older := schedulingCandidate(
		"51000000-0000-0000-0000-000000000015",
		"51000000-0000-0000-0000-000000000110",
		"51000000-0000-0000-0000-000000000210",
		now.Add(-2*time.Minute),
	)
	newer := schedulingCandidate(
		"51000000-0000-0000-0000-000000000016",
		"51000000-0000-0000-0000-000000000110",
		"51000000-0000-0000-0000-000000000210",
		now.Add(-time.Minute),
	)

	evidence, ok, err := Decide(schedulingSnapshot(now, []Candidate{newer, older}))
	if err != nil || !ok {
		t.Fatalf("Decide(tie break) ok=%t error=%v", ok, err)
	}
	if evidence.SelectedStageRunID != older.StageRunID {
		t.Fatalf("selected StageRun = %s, want oldest %s", evidence.SelectedStageRunID, older.StageRunID)
	}
}

func TestDecideReturnsNoWorkWhenEveryCandidateIsFiltered(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 4, 0, 0, 0, time.UTC)
	filtered := schedulingCandidate(
		"51000000-0000-0000-0000-000000000017",
		"51000000-0000-0000-0000-000000000117",
		"51000000-0000-0000-0000-000000000217",
		now.Add(-time.Minute),
	)
	filtered.FilterReasons = []FilterReason{FilterCapacityExhausted}

	_, ok, err := Decide(schedulingSnapshot(now, []Candidate{filtered}))
	if err != nil || ok {
		t.Fatalf("Decide(no work) ok=%t error=%v, want clean no-work result", ok, err)
	}
}

func TestDecideRejectsUnsupportedRevisionAndFilterReason(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 30, 4, 0, 0, 0, time.UTC)
	candidate := schedulingCandidate(
		"51000000-0000-0000-0000-000000000018",
		"51000000-0000-0000-0000-000000000118",
		"51000000-0000-0000-0000-000000000218",
		now.Add(-time.Minute),
	)

	unsupportedRevision := schedulingSnapshot(now, []Candidate{candidate})
	unsupportedRevision.AlgorithmRevision = "stage-filter-fairness-score-pick-v2"
	if _, ok, err := Decide(unsupportedRevision); err == nil || ok {
		t.Fatalf("Decide(unsupported revision) ok=%t error=%v, want rejection", ok, err)
	}

	candidate.FilterReasons = []FilterReason{"UNBOUNDED_REASON"}
	if _, ok, err := Decide(schedulingSnapshot(now, []Candidate{candidate})); err == nil || ok {
		t.Fatalf("Decide(unsupported filter reason) ok=%t error=%v, want rejection", ok, err)
	}

	candidate.FilterReasons = nil
	candidate.ResourceMillis = maxResourceMillis + 1
	if _, ok, err := Decide(schedulingSnapshot(now, []Candidate{candidate})); err == nil || ok {
		t.Fatalf("Decide(unbounded resource) ok=%t error=%v, want rejection", ok, err)
	}
}

func schedulingSnapshot(now time.Time, candidates []Candidate) Snapshot {
	return Snapshot{
		AlgorithmRevision:   AlgorithmRevisionV1,
		EvaluatedAt:         now,
		ValidUntil:          now.Add(time.Minute),
		CapacityPoolID:      uuid.MustParse("51000000-0000-0000-0000-000000000010"),
		CapacityPoolVersion: 3,
		WorkerInstanceID:    uuid.MustParse("51000000-0000-0000-0000-000000000011"),
		WorkerInstanceEpoch: 7,
		ObservationSequence: 9,
		Candidates:          candidates,
	}
}

func schedulingCandidate(
	stageRunID string,
	organizationID string,
	projectID string,
	enqueuedAt time.Time,
) Candidate {
	return Candidate{
		StageRunID:                uuid.MustParse(stageRunID),
		AttemptID:                 uuid.MustParse("51000000-0000-0000-0000-000000000020"),
		StageProfileRevisionID:    uuid.MustParse("51000000-0000-0000-0000-000000000021"),
		OrganizationID:            uuid.MustParse(organizationID),
		ServiceClassRevisionID:    uuid.MustParse("51000000-0000-0000-0000-000000000022"),
		ProjectID:                 uuid.MustParse(projectID),
		Lane:                      LaneNormal,
		EnqueuedAt:                enqueuedAt,
		AttemptFence:              1,
		StageFence:                1,
		StageVersion:              1,
		ResourceMillis:            5_000,
		OrganizationDeficitMillis: 100_000,
		ServiceClassDeficitMillis: 100_000,
		ProjectDeficitMillis:      100_000,
		Score: ScoreTerms{
			PredictedFinishMillis: 5_000,
		},
	}
}
