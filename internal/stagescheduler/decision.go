package stagescheduler

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
)

const (
	AlgorithmRevisionV1 = "stage-filter-fairness-score-pick-v1"
	maxResourceMillis   = int64(604_800_000)
)

var ErrStaleSnapshot = errors.New("StageScheduler snapshot evidence is stale")

type Lane string

const (
	LaneRetry     Lane = "RETRY"
	LaneProtected Lane = "PROTECTED"
	LaneNormal    Lane = "NORMAL"
)

type FilterReason string

const (
	FilterStageNotReady            FilterReason = "STAGE_NOT_READY"
	FilterDependencyBlocked        FilterReason = "DEPENDENCY_BLOCKED"
	FilterAttemptFenceStale        FilterReason = "ATTEMPT_FENCE_STALE"
	FilterStageFenceStale          FilterReason = "STAGE_FENCE_STALE"
	FilterProfileIncompatible      FilterReason = "PROFILE_INCOMPATIBLE"
	FilterSecurityMismatch         FilterReason = "SECURITY_MISMATCH"
	FilterRegionMismatch           FilterReason = "REGION_MISMATCH"
	FilterConnectorUnavailable     FilterReason = "CONNECTOR_UNAVAILABLE"
	FilterPinUnavailable           FilterReason = "PIN_UNAVAILABLE"
	FilterBufferUnavailable        FilterReason = "BUFFER_UNAVAILABLE"
	FilterWorkerUnavailable        FilterReason = "WORKER_UNAVAILABLE"
	FilterCapacityStale            FilterReason = "CAPACITY_STALE"
	FilterCapacityExhausted        FilterReason = "CAPACITY_EXHAUSTED"
	FilterProjectCapacityExhausted FilterReason = "PROJECT_CAPACITY_EXHAUSTED"
	FilterModelResidencyStale      FilterReason = "MODEL_RESIDENCY_STALE"
	FilterDeviceEvidenceStale      FilterReason = "DEVICE_EVIDENCE_STALE"
	FilterMemberEvidenceStale      FilterReason = "MEMBER_EVIDENCE_STALE"
)

type ScoreTerms struct {
	LocalityCreditMillis     int64 `json:"locality_credit_millis"`
	TransferPenaltyMillis    int64 `json:"transfer_penalty_millis"`
	LoadPenaltyMillis        int64 `json:"load_penalty_millis"`
	PredictedFinishMillis    int64 `json:"predicted_finish_millis"`
	CriticalPathCreditMillis int64 `json:"critical_path_credit_millis"`
	AgeCreditMillis          int64 `json:"age_credit_millis"`
}

func (terms ScoreTerms) TotalMillis() int64 {
	return terms.TransferPenaltyMillis + terms.LoadPenaltyMillis +
		terms.PredictedFinishMillis - terms.LocalityCreditMillis -
		terms.CriticalPathCreditMillis - terms.AgeCreditMillis
}

type Candidate struct {
	StageRunID                uuid.UUID      `json:"stage_run_id"`
	AttemptID                 uuid.UUID      `json:"attempt_id"`
	StageProfileRevisionID    uuid.UUID      `json:"stage_profile_revision_id"`
	OrganizationID            uuid.UUID      `json:"organization_id"`
	ServiceClassRevisionID    uuid.UUID      `json:"service_class_revision_id"`
	ProjectID                 uuid.UUID      `json:"project_id"`
	Lane                      Lane           `json:"lane"`
	EnqueuedAt                time.Time      `json:"enqueued_at"`
	AttemptFence              int64          `json:"attempt_fence"`
	StageFence                int64          `json:"stage_fence"`
	StageVersion              int64          `json:"stage_version"`
	ResourceMillis            int64          `json:"resource_millis"`
	OrganizationDeficitMillis int64          `json:"organization_deficit_millis"`
	ServiceClassDeficitMillis int64          `json:"service_class_deficit_millis"`
	ProjectDeficitMillis      int64          `json:"project_deficit_millis"`
	Score                     ScoreTerms     `json:"score"`
	FilterReasons             []FilterReason `json:"filter_reasons"`
}

type Snapshot struct {
	AlgorithmRevision   string      `json:"algorithm_revision"`
	EvaluatedAt         time.Time   `json:"evaluated_at"`
	ValidUntil          time.Time   `json:"valid_until"`
	CapacityPoolID      uuid.UUID   `json:"capacity_pool_id"`
	CapacityPoolVersion int64       `json:"capacity_pool_version"`
	WorkerInstanceID    uuid.UUID   `json:"worker_instance_id"`
	WorkerInstanceEpoch int64       `json:"worker_instance_epoch"`
	ObservationSequence int64       `json:"observation_sequence"`
	Candidates          []Candidate `json:"candidates"`
}

type TieBreakEvidence struct {
	EnqueuedAt             time.Time `json:"enqueued_at"`
	StageRunID             uuid.UUID `json:"stage_run_id"`
	StageProfileRevisionID uuid.UUID `json:"stage_profile_revision_id"`
}

type DecisionEvidence struct {
	AlgorithmRevision              string               `json:"algorithm_revision"`
	InputDigest                    [32]byte             `json:"input_digest"`
	EvidenceDigest                 [32]byte             `json:"evidence_digest"`
	CapacityPoolID                 uuid.UUID            `json:"capacity_pool_id"`
	WorkerInstanceID               uuid.UUID            `json:"worker_instance_id"`
	SelectedStageRunID             uuid.UUID            `json:"selected_stage_run_id"`
	SelectedAttemptID              uuid.UUID            `json:"selected_attempt_id"`
	SelectedStageProfileRevisionID uuid.UUID            `json:"selected_stage_profile_revision_id"`
	OrganizationID                 uuid.UUID            `json:"organization_id"`
	ServiceClassRevisionID         uuid.UUID            `json:"service_class_revision_id"`
	ProjectID                      uuid.UUID            `json:"project_id"`
	AttemptFence                   int64                `json:"attempt_fence"`
	StageFence                     int64                `json:"stage_fence"`
	StageVersion                   int64                `json:"stage_version"`
	Lane                           Lane                 `json:"lane"`
	ResourceMillis                 int64                `json:"resource_millis"`
	OrganizationDeficitMillis      int64                `json:"organization_deficit_millis"`
	ServiceClassDeficitMillis      int64                `json:"service_class_deficit_millis"`
	ProjectDeficitMillis           int64                `json:"project_deficit_millis"`
	Score                          ScoreTerms           `json:"score"`
	ScoreTotalMillis               int64                `json:"score_total_millis"`
	FilterReasonCounts             map[FilterReason]int `json:"filter_reason_counts"`
	TieBreak                       TieBreakEvidence     `json:"tie_break"`
	inputPayload                   []byte
	evidencePayload                []byte
}

func Decide(snapshot Snapshot) (DecisionEvidence, bool, error) {
	canonical, err := canonicalSnapshot(snapshot)
	if err != nil {
		return DecisionEvidence{}, false, err
	}
	if !canonical.ValidUntil.After(canonical.EvaluatedAt) {
		return DecisionEvidence{}, false, ErrStaleSnapshot
	}
	encodedSnapshot, err := json.Marshal(canonical)
	if err != nil {
		return DecisionEvidence{}, false, fmt.Errorf("encode StageScheduler snapshot: %w", err)
	}
	inputDigest := sha256.Sum256(encodedSnapshot)

	reasonCounts := make(map[FilterReason]int)
	eligible := make([]Candidate, 0, len(canonical.Candidates))
	for _, candidate := range canonical.Candidates {
		if len(candidate.FilterReasons) == 0 {
			eligible = append(eligible, candidate)
			continue
		}
		for _, reason := range candidate.FilterReasons {
			reasonCounts[reason]++
		}
	}
	if len(eligible) == 0 {
		return DecisionEvidence{}, false, nil
	}

	selectedLane := eligible[0].Lane
	for _, candidate := range eligible[1:] {
		if lanePriority(candidate.Lane) < lanePriority(selectedLane) {
			selectedLane = candidate.Lane
		}
	}
	eligible = slices.DeleteFunc(eligible, func(candidate Candidate) bool {
		return candidate.Lane != selectedLane
	})
	eligible = keepMaximum(eligible, func(candidate Candidate) int64 {
		return candidate.OrganizationDeficitMillis
	})
	selectedOrganization := minimumUUID(eligible, func(candidate Candidate) uuid.UUID {
		return candidate.OrganizationID
	})
	eligible = slices.DeleteFunc(eligible, func(candidate Candidate) bool {
		return candidate.OrganizationID != selectedOrganization
	})
	eligible = keepMaximum(eligible, func(candidate Candidate) int64 {
		return candidate.ServiceClassDeficitMillis
	})
	selectedServiceClass := minimumUUID(eligible, func(candidate Candidate) uuid.UUID {
		return candidate.ServiceClassRevisionID
	})
	eligible = slices.DeleteFunc(eligible, func(candidate Candidate) bool {
		return candidate.ServiceClassRevisionID != selectedServiceClass
	})
	eligible = keepMaximum(eligible, func(candidate Candidate) int64 {
		return candidate.ProjectDeficitMillis
	})
	selectedProject := minimumUUID(eligible, func(candidate Candidate) uuid.UUID {
		return candidate.ProjectID
	})
	eligible = slices.DeleteFunc(eligible, func(candidate Candidate) bool {
		return candidate.ProjectID != selectedProject
	})

	slices.SortFunc(eligible, compareScoreAndTieBreak)
	winner := eligible[0]
	evidence := DecisionEvidence{
		AlgorithmRevision:              canonical.AlgorithmRevision,
		InputDigest:                    inputDigest,
		CapacityPoolID:                 canonical.CapacityPoolID,
		WorkerInstanceID:               canonical.WorkerInstanceID,
		SelectedStageRunID:             winner.StageRunID,
		SelectedAttemptID:              winner.AttemptID,
		SelectedStageProfileRevisionID: winner.StageProfileRevisionID,
		OrganizationID:                 winner.OrganizationID,
		ServiceClassRevisionID:         winner.ServiceClassRevisionID,
		ProjectID:                      winner.ProjectID,
		AttemptFence:                   winner.AttemptFence,
		StageFence:                     winner.StageFence,
		StageVersion:                   winner.StageVersion,
		Lane:                           winner.Lane,
		ResourceMillis:                 winner.ResourceMillis,
		OrganizationDeficitMillis:      winner.OrganizationDeficitMillis,
		ServiceClassDeficitMillis:      winner.ServiceClassDeficitMillis,
		ProjectDeficitMillis:           winner.ProjectDeficitMillis,
		Score:                          winner.Score,
		ScoreTotalMillis:               winner.Score.TotalMillis(),
		FilterReasonCounts:             reasonCounts,
		TieBreak: TieBreakEvidence{
			EnqueuedAt:             winner.EnqueuedAt,
			StageRunID:             winner.StageRunID,
			StageProfileRevisionID: winner.StageProfileRevisionID,
		},
		inputPayload: append([]byte(nil), encodedSnapshot...),
	}
	encodedEvidence, err := json.Marshal(decisionEvidenceDigestPayload(evidence))
	if err != nil {
		return DecisionEvidence{}, false, fmt.Errorf("encode StageScheduler evidence: %w", err)
	}
	evidence.evidencePayload = append([]byte(nil), encodedEvidence...)
	evidence.EvidenceDigest = sha256.Sum256(encodedEvidence)
	return evidence, true, nil
}

func decisionEvidenceDigestPayload(evidence DecisionEvidence) map[string]any {
	return map[string]any{
		"algorithm_revision":                 evidence.AlgorithmRevision,
		"input_digest":                       hex.EncodeToString(evidence.InputDigest[:]),
		"capacity_pool_id":                   evidence.CapacityPoolID,
		"worker_instance_id":                 evidence.WorkerInstanceID,
		"selected_stage_run_id":              evidence.SelectedStageRunID,
		"selected_attempt_id":                evidence.SelectedAttemptID,
		"selected_stage_profile_revision_id": evidence.SelectedStageProfileRevisionID,
		"organization_id":                    evidence.OrganizationID,
		"service_class_revision_id":          evidence.ServiceClassRevisionID,
		"project_id":                         evidence.ProjectID,
		"attempt_fence":                      evidence.AttemptFence,
		"stage_fence":                        evidence.StageFence,
		"stage_version":                      evidence.StageVersion,
		"lane":                               evidence.Lane,
		"resource_millis":                    evidence.ResourceMillis,
		"organization_deficit_millis":        evidence.OrganizationDeficitMillis,
		"service_class_deficit_millis":       evidence.ServiceClassDeficitMillis,
		"project_deficit_millis":             evidence.ProjectDeficitMillis,
		"score":                              evidence.Score,
		"score_total_millis":                 evidence.ScoreTotalMillis,
		"filter_reason_counts":               evidence.FilterReasonCounts,
		"tie_break":                          evidence.TieBreak,
	}
}

func canonicalSnapshot(snapshot Snapshot) (Snapshot, error) {
	if snapshot.AlgorithmRevision != AlgorithmRevisionV1 {
		return Snapshot{}, errors.New("StageScheduler algorithm revision is unsupported")
	}
	if snapshot.EvaluatedAt.IsZero() || snapshot.ValidUntil.IsZero() ||
		snapshot.CapacityPoolID == uuid.Nil || snapshot.WorkerInstanceID == uuid.Nil ||
		snapshot.CapacityPoolVersion <= 0 || snapshot.WorkerInstanceEpoch <= 0 ||
		snapshot.ObservationSequence <= 0 {
		return Snapshot{}, errors.New("StageScheduler snapshot authority is incomplete")
	}
	canonical := snapshot
	canonical.EvaluatedAt = snapshot.EvaluatedAt.UTC()
	canonical.ValidUntil = snapshot.ValidUntil.UTC()
	canonical.Candidates = slices.Clone(snapshot.Candidates)
	for index := range canonical.Candidates {
		candidate := &canonical.Candidates[index]
		if err := validateCandidate(*candidate); err != nil {
			return Snapshot{}, err
		}
		candidate.EnqueuedAt = candidate.EnqueuedAt.UTC()
		candidate.FilterReasons = slices.Clone(candidate.FilterReasons)
		slices.Sort(candidate.FilterReasons)
		candidate.FilterReasons = slices.Compact(candidate.FilterReasons)
	}
	slices.SortFunc(canonical.Candidates, func(left, right Candidate) int {
		if compared := bytes.Compare(left.StageRunID[:], right.StageRunID[:]); compared != 0 {
			return compared
		}
		return bytes.Compare(left.StageProfileRevisionID[:], right.StageProfileRevisionID[:])
	})
	return canonical, nil
}

func validateCandidate(candidate Candidate) error {
	if candidate.StageRunID == uuid.Nil || candidate.AttemptID == uuid.Nil ||
		candidate.StageProfileRevisionID == uuid.Nil || candidate.OrganizationID == uuid.Nil ||
		candidate.ServiceClassRevisionID == uuid.Nil || candidate.ProjectID == uuid.Nil ||
		candidate.EnqueuedAt.IsZero() || candidate.AttemptFence <= 0 ||
		candidate.StageFence <= 0 || candidate.StageVersion <= 0 ||
		candidate.ResourceMillis <= 0 || candidate.ResourceMillis > maxResourceMillis {
		return errors.New("StageScheduler candidate authority is incomplete")
	}
	if lanePriority(candidate.Lane) < 0 {
		return errors.New("StageScheduler candidate lane is unsupported")
	}
	for _, reason := range candidate.FilterReasons {
		if !validFilterReason(reason) {
			return errors.New("StageScheduler filter reason is unsupported")
		}
	}
	return nil
}

func validFilterReason(reason FilterReason) bool {
	switch reason {
	case FilterStageNotReady, FilterDependencyBlocked, FilterAttemptFenceStale,
		FilterStageFenceStale, FilterProfileIncompatible, FilterSecurityMismatch,
		FilterRegionMismatch, FilterConnectorUnavailable, FilterPinUnavailable,
		FilterBufferUnavailable, FilterWorkerUnavailable, FilterCapacityStale,
		FilterCapacityExhausted, FilterProjectCapacityExhausted, FilterModelResidencyStale,
		FilterDeviceEvidenceStale, FilterMemberEvidenceStale:
		return true
	default:
		return false
	}
}

func lanePriority(lane Lane) int {
	switch lane {
	case LaneRetry:
		return 0
	case LaneProtected:
		return 1
	case LaneNormal:
		return 2
	default:
		return -1
	}
}

func keepMaximum(candidates []Candidate, value func(Candidate) int64) []Candidate {
	maximum := value(candidates[0])
	for _, candidate := range candidates[1:] {
		maximum = max(maximum, value(candidate))
	}
	return slices.DeleteFunc(candidates, func(candidate Candidate) bool {
		return value(candidate) != maximum
	})
}

func minimumUUID(candidates []Candidate, value func(Candidate) uuid.UUID) uuid.UUID {
	minimum := value(candidates[0])
	for _, candidate := range candidates[1:] {
		current := value(candidate)
		if bytes.Compare(current[:], minimum[:]) < 0 {
			minimum = current
		}
	}
	return minimum
}

func compareScoreAndTieBreak(left, right Candidate) int {
	if left.Score.TotalMillis() < right.Score.TotalMillis() {
		return -1
	}
	if left.Score.TotalMillis() > right.Score.TotalMillis() {
		return 1
	}
	if compared := left.EnqueuedAt.Compare(right.EnqueuedAt); compared != 0 {
		return compared
	}
	if compared := bytes.Compare(left.StageRunID[:], right.StageRunID[:]); compared != 0 {
		return compared
	}
	return bytes.Compare(left.StageProfileRevisionID[:], right.StageProfileRevisionID[:])
}
