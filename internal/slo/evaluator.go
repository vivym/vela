package slo

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	AlgorithmRevision        = "queued-visible-nearest-rank-wilson-v1"
	ConfidenceMethod         = "wilson-one-sided-95-v1"
	CancellationPolicy       = "exclude-customer-cancellation-v1"
	OneSidedConfidencePPM    = 950_000
	APIAvailabilityTargetPPM = 999_000
	maximumObservations      = 1_000_000
	oneSided95PercentZ       = 1.6448536269514722
)

var ErrInvalidInput = errors.New("invalid SLO evaluation input")

type Outcome string

const (
	OutcomeSucceeded        Outcome = "SUCCEEDED"
	OutcomeFailed           Outcome = "FAILED"
	OutcomeCustomerCanceled Outcome = "CUSTOMER_CANCELED"
	OutcomeOpen             Outcome = "OPEN"
)

type Result string

const (
	ResultPass             Result = "PASS"
	ResultFail             Result = "FAIL"
	ResultInsufficientData Result = "INSUFFICIENT_DATA"
)

// Contract is one immutable saleable SKU target. Every dimension is exact;
// callers must evaluate separate contracts instead of merging cohorts.
type Contract struct {
	RevisionID                 string `json:"revision_id"`
	ModelRevisionID            string `json:"model_revision_id"`
	GenerationPreset           string `json:"generation_preset"`
	GenerationPresetRevisionID string `json:"generation_preset_revision_id"`
	ServiceClassRevisionID     string `json:"service_class_revision_id"`
	OutputSpecID               string `json:"output_spec_id"`
	GenerationCount            int    `json:"generation_count"`
	P95TargetMilliseconds      int64  `json:"p95_target_milliseconds"`
	SuccessTargetPPM           int    `json:"success_target_ppm"`
	MinimumSample              int    `json:"minimum_sample"`
	ConfidenceMethod           string `json:"confidence_method"`
	OneSidedConfidencePPM      int    `json:"one_sided_confidence_ppm"`
	CancellationPolicy         string `json:"cancellation_policy"`
}

type Window struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type Observation struct {
	JobID              string     `json:"job_id"`
	ContractRevisionID string     `json:"contract_revision_id"`
	AcceptedAt         time.Time  `json:"accepted_at"`
	ExpiresAt          time.Time  `json:"expires_at"`
	Outcome            Outcome    `json:"outcome"`
	TerminalAt         *time.Time `json:"terminal_at,omitempty"`
	VisibleCompletedAt *time.Time `json:"visible_completed_at,omitempty"`
}

type Report struct {
	AlgorithmRevision     string `json:"algorithm_revision"`
	ContractRevisionID    string `json:"contract_revision_id"`
	Window                Window `json:"window"`
	SourceSetDigest       string `json:"source_set_digest"`
	ObservationCount      int    `json:"observation_count"`
	EligibleCount         int    `json:"eligible_count"`
	SucceededCount        int    `json:"succeeded_count"`
	FailedCount           int    `json:"failed_count"`
	CustomerCanceledCount int    `json:"customer_canceled_count"`
	OpenCount             int    `json:"open_count"`
	P95Milliseconds       int64  `json:"p95_milliseconds"`
	SuccessObservedPPM    int    `json:"success_observed_ppm"`
	SuccessLowerBoundPPM  int    `json:"success_lower_bound_ppm"`
	Result                Result `json:"result"`
}

// Evaluate measures accepted Jobs from QUEUED (AcceptedAt) to Visible
// Completion. Queueing, retries, recovery and finalization therefore remain in
// the latency. Only explicit customer cancellation is excluded from the
// success-rate denominator, and it remains visible as a separate count.
func Evaluate(contract Contract, window Window, now time.Time, observations []Observation) (Report, error) {
	report := Report{
		AlgorithmRevision:  AlgorithmRevision,
		ContractRevisionID: contract.RevisionID,
		Window:             window,
		Result:             ResultInsufficientData,
	}
	if err := validateContract(contract); err != nil {
		return Report{}, err
	}
	if err := validateWindow(window); err != nil {
		return Report{}, err
	}
	if now.Before(window.End) {
		return Report{}, fmt.Errorf("%w: evaluation time precedes the closed window", ErrInvalidInput)
	}
	if len(observations) > maximumObservations {
		return Report{}, fmt.Errorf("%w: observation count exceeds %d", ErrInvalidInput, maximumObservations)
	}

	canonical := append([]Observation(nil), observations...)
	sort.Slice(canonical, func(i, j int) bool { return canonical[i].JobID < canonical[j].JobID })
	latencies := make([]int64, 0, len(canonical))
	seen := make(map[string]struct{}, len(canonical))
	for _, observation := range canonical {
		if err := validateObservation(contract, window, observation); err != nil {
			return Report{}, err
		}
		if _, duplicate := seen[observation.JobID]; duplicate {
			return Report{}, fmt.Errorf("%w: duplicate job_id %q", ErrInvalidInput, observation.JobID)
		}
		seen[observation.JobID] = struct{}{}
		switch measuredOutcome(observation, now) {
		case OutcomeSucceeded:
			report.SucceededCount++
			latencies = append(latencies, observation.VisibleCompletedAt.Sub(observation.AcceptedAt).Milliseconds())
		case OutcomeFailed:
			report.FailedCount++
		case OutcomeCustomerCanceled:
			report.CustomerCanceledCount++
		case OutcomeOpen:
			report.OpenCount++
		}
	}
	report.ObservationCount = len(canonical)
	report.EligibleCount = report.SucceededCount + report.FailedCount
	report.SourceSetDigest = sourceSetDigest(canonical)
	if report.EligibleCount > 0 {
		report.SuccessObservedPPM = ratePPM(report.SucceededCount, report.EligibleCount)
		report.SuccessLowerBoundPPM = wilsonLowerBoundPPM(report.SucceededCount, report.EligibleCount)
	}
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		report.P95Milliseconds = nearestRank(latencies, 95, 100)
	}
	if report.OpenCount > 0 || report.EligibleCount < contract.MinimumSample || report.SucceededCount == 0 {
		return report, nil
	}
	report.Result = ResultFail
	if report.P95Milliseconds <= contract.P95TargetMilliseconds &&
		report.SuccessLowerBoundPPM >= contract.SuccessTargetPPM {
		report.Result = ResultPass
	}
	return report, nil
}

type AvailabilityReport struct {
	EligibleCount int    `json:"eligible_count"`
	GoodCount     int    `json:"good_count"`
	ObservedPPM   int    `json:"observed_ppm"`
	LowerBoundPPM int    `json:"lower_bound_ppm"`
	TargetPPM     int    `json:"target_ppm"`
	MinimumSample int    `json:"minimum_sample"`
	Result        Result `json:"result"`
}

func EvaluateAvailability(eligible, good, minimumSample int) (AvailabilityReport, error) {
	report := AvailabilityReport{
		EligibleCount: eligible,
		GoodCount:     good,
		TargetPPM:     APIAvailabilityTargetPPM,
		MinimumSample: minimumSample,
		Result:        ResultInsufficientData,
	}
	if eligible < 0 || good < 0 || good > eligible || minimumSample < 1 {
		return AvailabilityReport{}, fmt.Errorf("%w: invalid API availability counts", ErrInvalidInput)
	}
	if eligible > 0 {
		report.ObservedPPM = ratePPM(good, eligible)
		report.LowerBoundPPM = wilsonLowerBoundPPM(good, eligible)
	}
	if eligible < minimumSample {
		return report, nil
	}
	report.Result = ResultFail
	if report.LowerBoundPPM >= report.TargetPPM {
		report.Result = ResultPass
	}
	return report, nil
}

func validateContract(contract Contract) error {
	if !bounded(contract.RevisionID, 200) || !bounded(contract.ModelRevisionID, 200) ||
		!bounded(contract.GenerationPresetRevisionID, 200) ||
		!bounded(contract.ServiceClassRevisionID, 200) || !bounded(contract.OutputSpecID, 200) ||
		(contract.GenerationPreset != "quality" && contract.GenerationPreset != "balanced" &&
			contract.GenerationPreset != "fast") ||
		contract.GenerationCount < 1 || contract.GenerationCount > 16 ||
		contract.P95TargetMilliseconds < 1 ||
		contract.SuccessTargetPPM < 1 || contract.SuccessTargetPPM > 1_000_000 ||
		contract.MinimumSample < 1 || contract.MinimumSample > maximumObservations ||
		contract.ConfidenceMethod != ConfidenceMethod ||
		contract.OneSidedConfidencePPM != OneSidedConfidencePPM ||
		contract.CancellationPolicy != CancellationPolicy {
		return fmt.Errorf("%w: SLO contract is invalid", ErrInvalidInput)
	}
	return nil
}

func validateWindow(window Window) error {
	if window.Start.IsZero() || window.End.IsZero() || !window.Start.Before(window.End) ||
		window.Start.Location() != time.UTC || window.End.Location() != time.UTC ||
		window.Start.Day() != 1 || window.Start.Hour() != 0 || window.Start.Minute() != 0 ||
		window.Start.Second() != 0 || window.Start.Nanosecond() != 0 ||
		!window.End.Equal(window.Start.AddDate(0, 1, 0)) {
		return fmt.Errorf("%w: window must be one UTC calendar month [start,end)", ErrInvalidInput)
	}
	return nil
}

func validateObservation(contract Contract, window Window, observation Observation) error {
	if !bounded(observation.JobID, 200) || observation.ContractRevisionID != contract.RevisionID ||
		observation.AcceptedAt.Before(window.Start) || !observation.AcceptedAt.Before(window.End) ||
		!observation.ExpiresAt.After(observation.AcceptedAt) {
		return fmt.Errorf("%w: observation %q has an invalid identity, contract or cohort clock", ErrInvalidInput, observation.JobID)
	}
	switch observation.Outcome {
	case OutcomeSucceeded:
		if observation.TerminalAt == nil || observation.VisibleCompletedAt == nil ||
			!observation.TerminalAt.Equal(*observation.VisibleCompletedAt) ||
			observation.VisibleCompletedAt.Before(observation.AcceptedAt) {
			return fmt.Errorf("%w: successful observation %q lacks canonical Visible Completion", ErrInvalidInput, observation.JobID)
		}
	case OutcomeFailed, OutcomeCustomerCanceled:
		if observation.TerminalAt == nil || observation.VisibleCompletedAt != nil ||
			observation.TerminalAt.Before(observation.AcceptedAt) {
			return fmt.Errorf("%w: terminal observation %q has invalid terminal evidence", ErrInvalidInput, observation.JobID)
		}
	case OutcomeOpen:
		if observation.TerminalAt != nil || observation.VisibleCompletedAt != nil {
			return fmt.Errorf("%w: open observation %q includes terminal evidence", ErrInvalidInput, observation.JobID)
		}
	default:
		return fmt.Errorf("%w: observation %q has unknown outcome", ErrInvalidInput, observation.JobID)
	}
	return nil
}

func measuredOutcome(observation Observation, evaluatedAt time.Time) Outcome {
	if observation.Outcome == OutcomeOpen && !observation.ExpiresAt.After(evaluatedAt) {
		return OutcomeFailed
	}
	if (observation.Outcome == OutcomeSucceeded || observation.Outcome == OutcomeCustomerCanceled) &&
		observation.TerminalAt.After(observation.ExpiresAt) {
		return OutcomeFailed
	}
	return observation.Outcome
}

func nearestRank(sorted []int64, numerator, denominator int) int64 {
	rank := (numerator*len(sorted) + denominator - 1) / denominator
	return sorted[rank-1]
}

func ratePPM(successes, total int) int {
	return int((int64(successes) * 1_000_000) / int64(total))
}

func wilsonLowerBoundPPM(successes, total int) int {
	if total == 0 {
		return 0
	}
	n := float64(total)
	p := float64(successes) / n
	zSquared := oneSided95PercentZ * oneSided95PercentZ
	center := p + zSquared/(2*n)
	margin := oneSided95PercentZ * math.Sqrt((p*(1-p)+zSquared/(4*n))/n)
	lower := (center - margin) / (1 + zSquared/n)
	if lower < 0 {
		lower = 0
	}
	return int(math.Floor(lower * 1_000_000))
}

func sourceSetDigest(observations []Observation) string {
	digest := sha256.New()
	for _, observation := range observations {
		writeDigestField(digest, observation.JobID)
		writeDigestField(digest, observation.ContractRevisionID)
		writeDigestField(digest, strconv.FormatInt(observation.AcceptedAt.UnixMicro(), 10))
		writeDigestField(digest, strconv.FormatInt(observation.ExpiresAt.UnixMicro(), 10))
		writeDigestField(digest, string(observation.Outcome))
		writeDigestTime(digest, observation.TerminalAt)
		writeDigestTime(digest, observation.VisibleCompletedAt)
	}
	return "sha256:" + hex.EncodeToString(digest.Sum(nil))
}

func writeDigestField(writer io.Writer, value string) {
	_, _ = io.WriteString(writer, strconv.Itoa(len(value)))
	_, _ = io.WriteString(writer, ":")
	_, _ = io.WriteString(writer, value)
}

func writeDigestTime(writer io.Writer, value *time.Time) {
	if value == nil {
		_, _ = io.WriteString(writer, "-1:")
		return
	}
	writeDigestField(writer, strconv.FormatInt(value.UnixMicro(), 10))
}

func bounded(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value
}
