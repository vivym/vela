package slo

import (
	"errors"
	"testing"
	"time"
)

func TestEvaluateIncludesQueueRetryAndFinalizationInNearestRankP95(t *testing.T) {
	contract := testContract()
	contract.MinimumSample = 20
	contract.SuccessTargetPPM = 800_000
	window := testWindow()
	observations := make([]Observation, 0, 22)
	for index := 0; index < 20; index++ {
		latency := time.Duration(index+1) * time.Second
		observations = append(observations, succeededObservation(index, window.Start, latency))
	}
	observations = append(observations,
		terminalObservation("failed", window.Start.Add(21*time.Minute), OutcomeFailed),
		terminalObservation("canceled", window.Start.Add(22*time.Minute), OutcomeCustomerCanceled),
	)
	report, err := Evaluate(contract, window, window.End, observations)
	if err != nil {
		t.Fatalf("evaluate SLO: %v", err)
	}
	if report.P95Milliseconds != 19_000 || report.EligibleCount != 21 ||
		report.SucceededCount != 20 || report.FailedCount != 1 ||
		report.CustomerCanceledCount != 1 || report.Result != ResultPass {
		t.Fatalf("report = %#v", report)
	}
}

func TestEvaluateFailsClosedForOpenLowSampleAndMixedContract(t *testing.T) {
	contract := testContract()
	window := testWindow()
	open := Observation{
		JobID: "open", ContractRevisionID: contract.RevisionID,
		AcceptedAt: window.Start.Add(time.Hour), ExpiresAt: window.End.Add(time.Hour),
		Outcome: OutcomeOpen,
	}
	report, err := Evaluate(contract, window, window.End, []Observation{open})
	if err != nil || report.Result != ResultInsufficientData || report.OpenCount != 1 {
		t.Fatalf("open report = %#v error=%v", report, err)
	}

	mixed := succeededObservation(1, window.Start, time.Second)
	mixed.ContractRevisionID = "other-contract"
	if _, err := Evaluate(contract, window, window.End, []Observation{mixed}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("mixed contract error = %v", err)
	}
}

func TestEvaluateRejectsAttemptLikeCompletionAndDuplicateJobs(t *testing.T) {
	contract := testContract()
	window := testWindow()
	bad := succeededObservation(1, window.Start, time.Second)
	different := bad.VisibleCompletedAt.Add(time.Second)
	bad.TerminalAt = &different
	if _, err := Evaluate(contract, window, window.End, []Observation{bad}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("mismatched terminal error = %v", err)
	}
	valid := succeededObservation(2, window.Start, time.Second)
	if _, err := Evaluate(contract, window, window.End, []Observation{valid, valid}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("duplicate error = %v", err)
	}
}

func TestEvaluateTreatsExpiredAndLateTerminalJobsAsFailures(t *testing.T) {
	contract := testContract()
	window := testWindow()
	accepted := window.Start.Add(time.Hour)
	expires := accepted.Add(time.Minute)
	lateSuccessExpires := expires.Add(time.Minute)
	lateSuccess := lateSuccessExpires.Add(time.Second)
	lateCancelExpires := accepted.Add(3 * time.Minute)
	lateCancellation := lateCancelExpires.Add(time.Second)
	observations := []Observation{
		succeededObservation(1, window.Start, time.Second),
		{
			JobID: "expired-open", ContractRevisionID: contract.RevisionID,
			AcceptedAt: accepted, ExpiresAt: expires, Outcome: OutcomeOpen,
		},
		{
			JobID: "late-success", ContractRevisionID: contract.RevisionID,
			AcceptedAt: accepted.Add(time.Minute), ExpiresAt: lateSuccessExpires,
			Outcome: OutcomeSucceeded, TerminalAt: &lateSuccess, VisibleCompletedAt: &lateSuccess,
		},
		{
			JobID: "late-cancellation", ContractRevisionID: contract.RevisionID,
			AcceptedAt: accepted.Add(2 * time.Minute), ExpiresAt: lateCancelExpires,
			Outcome: OutcomeCustomerCanceled, TerminalAt: &lateCancellation,
		},
	}
	report, err := Evaluate(contract, window, window.End, observations)
	if err != nil {
		t.Fatalf("evaluate expired observations: %v", err)
	}
	if report.SucceededCount != 1 || report.FailedCount != 3 ||
		report.CustomerCanceledCount != 0 || report.OpenCount != 0 ||
		report.EligibleCount != 4 {
		t.Fatalf("expired report = %#v", report)
	}
}

func TestEvaluateRejectsGenerationCountOutsideAdmissionContract(t *testing.T) {
	contract := testContract()
	contract.GenerationCount = 17
	if _, err := Evaluate(contract, testWindow(), testWindow().End, nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("generation_count=17 error = %v", err)
	}
}

func TestAvailabilityUsesFixedMonthlyTargetAndWilsonBound(t *testing.T) {
	pass, err := EvaluateAvailability(1_000_000, 1_000_000, 1_000)
	if err != nil || pass.Result != ResultPass || pass.TargetPPM != APIAvailabilityTargetPPM {
		t.Fatalf("availability pass = %#v error=%v", pass, err)
	}
	fail, err := EvaluateAvailability(1_000, 999, 1_000)
	if err != nil || fail.Result != ResultFail || fail.ObservedPPM != 999_000 {
		t.Fatalf("availability fail = %#v error=%v", fail, err)
	}
	insufficient, err := EvaluateAvailability(99, 99, 100)
	if err != nil || insufficient.Result != ResultInsufficientData {
		t.Fatalf("availability insufficient = %#v error=%v", insufficient, err)
	}
}

func testContract() Contract {
	return Contract{
		RevisionID: "slo-fast-v1", ModelRevisionID: "model-v1",
		GenerationPreset:           "fast",
		GenerationPresetRevisionID: "fast-v1", ServiceClassRevisionID: "standard-v1",
		OutputSpecID: "1080p-v1", GenerationCount: 1,
		P95TargetMilliseconds: 30_000, SuccessTargetPPM: 900_000, MinimumSample: 1,
		ConfidenceMethod: ConfidenceMethod, OneSidedConfidencePPM: OneSidedConfidencePPM,
		CancellationPolicy: CancellationPolicy,
	}
}

func testWindow() Window {
	return Window{
		Start: time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC),
		End:   time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC),
	}
}

func succeededObservation(index int, start time.Time, latency time.Duration) Observation {
	accepted := start.Add(time.Duration(index+1) * time.Minute)
	completed := accepted.Add(latency)
	return Observation{
		JobID: string(rune('a' + index)), ContractRevisionID: "slo-fast-v1",
		AcceptedAt: accepted, ExpiresAt: accepted.Add(time.Hour), Outcome: OutcomeSucceeded,
		TerminalAt: &completed, VisibleCompletedAt: &completed,
	}
}

func terminalObservation(id string, accepted time.Time, outcome Outcome) Observation {
	terminal := accepted.Add(time.Minute)
	return Observation{
		JobID: id, ContractRevisionID: "slo-fast-v1", AcceptedAt: accepted,
		ExpiresAt: accepted.Add(time.Hour), Outcome: outcome, TerminalAt: &terminal,
	}
}
