package nodeagent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
)

func TestWorkerInstanceReportingRunsImmediatelyPeriodicallyAndIsolatesFailures(t *testing.T) {
	healthyID := uuid.MustParse("49430000-0000-0000-0000-000000000001")
	failingID := uuid.MustParse("49430000-0000-0000-0000-000000000002")
	reporter := &recordingWorkerInstanceLoopReporter{failures: map[uuid.UUID]error{
		failingID: errors.New("fleet unavailable for one WorkerInstance"),
	}}
	results := make(chan WorkerInstanceReportResult, 64)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	started := time.Now()
	go func() {
		done <- RunWorkerInstanceEvidenceReporting(
			ctx,
			reporter,
			[]WorkerInstanceEvidenceTemplate{
				{Evidence: fleet.WorkerInstanceEvidence{WorkerInstanceID: healthyID}},
				{Evidence: fleet.WorkerInstanceEvidence{WorkerInstanceID: failingID}},
			},
			WorkerInstanceReportingConfig{
				Interval: 15 * time.Millisecond, CallTimeout: 20 * time.Millisecond,
				InitialBackoff: 5 * time.Millisecond, MaxBackoff: 10 * time.Millisecond,
				ObserveResult: func(result WorkerInstanceReportResult) { results <- result },
			},
		)
	}()

	counts := map[uuid.UUID]int{}
	for counts[healthyID] < 2 || counts[failingID] < 3 {
		select {
		case result := <-results:
			counts[result.WorkerInstanceID]++
			if counts[result.WorkerInstanceID] == 1 && time.Since(started) > 100*time.Millisecond {
				t.Fatalf("first report for %s was not immediate", result.WorkerInstanceID)
			}
			if result.WorkerInstanceID == healthyID && result.Err != nil {
				t.Fatalf("healthy WorkerInstance report error=%v", result.Err)
			}
			if result.WorkerInstanceID == failingID && result.Err == nil {
				t.Fatal("failing WorkerInstance unexpectedly succeeded")
			}
		case <-time.After(time.Second):
			t.Fatalf("report counts=%#v did not reach periodic/backoff thresholds", counts)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("reporting loop cancellation error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reporting loop did not stop after cancellation")
	}
}

func TestWorkerInstanceReportingAppliesPerCallTimeout(t *testing.T) {
	workerID := uuid.MustParse("49430000-0000-0000-0000-000000000003")
	reporter := &recordingWorkerInstanceLoopReporter{block: true}
	results := make(chan WorkerInstanceReportResult, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunWorkerInstanceEvidenceReporting(
			ctx,
			reporter,
			[]WorkerInstanceEvidenceTemplate{{
				Evidence: fleet.WorkerInstanceEvidence{WorkerInstanceID: workerID},
			}},
			WorkerInstanceReportingConfig{
				Interval: time.Second, CallTimeout: 10 * time.Millisecond,
				InitialBackoff: 5 * time.Millisecond, MaxBackoff: 10 * time.Millisecond,
				ObserveResult: func(result WorkerInstanceReportResult) { results <- result },
			},
		)
	}()
	select {
	case result := <-results:
		if !errors.Is(result.Err, context.DeadlineExceeded) {
			t.Fatalf("timed report error=%v", result.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("report call did not honor its timeout")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("timed reporting loop cancellation error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed reporting loop did not stop after cancellation")
	}
}

func TestWorkerInstanceReportingAdoptsDurableControlSessionEpoch(t *testing.T) {
	workerID := uuid.MustParse("49430000-0000-0000-0000-000000000005")
	reporter := &recordingWorkerInstanceLoopReporter{
		decisionEpochs: map[uuid.UUID]int64{workerID: 7},
	}
	results := make(chan WorkerInstanceReportResult, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunWorkerInstanceEvidenceReporting(
			ctx,
			reporter,
			[]WorkerInstanceEvidenceTemplate{{Evidence: fleet.WorkerInstanceEvidence{
				WorkerInstanceID: workerID, InstanceEpoch: 3, ControlSessionEpoch: 1,
			}}},
			WorkerInstanceReportingConfig{
				Interval: 5 * time.Millisecond, CallTimeout: time.Second,
				InitialBackoff: 5 * time.Millisecond, MaxBackoff: 10 * time.Millisecond,
				ObserveResult: func(result WorkerInstanceReportResult) { results <- result },
			},
		)
	}()
	for range 2 {
		select {
		case result := <-results:
			if result.Err != nil {
				t.Fatalf("WorkerInstance report error=%v", result.Err)
			}
		case <-time.After(time.Second):
			t.Fatal("WorkerInstance reporting did not complete two cycles")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("reporting loop cancellation error=%v", err)
	}
	reporter.mu.Lock()
	epochs := append([]int64(nil), reporter.epochs[workerID]...)
	reporter.mu.Unlock()
	if len(epochs) < 2 || epochs[0] != 1 || epochs[1] != 7 {
		t.Fatalf("reported control epochs = %v, want prefix [1 7]", epochs)
	}
}

func TestWorkerInstanceReportingRejectsDuplicateTemplatesAndBoundsBackoff(t *testing.T) {
	workerID := uuid.MustParse("49430000-0000-0000-0000-000000000004")
	template := WorkerInstanceEvidenceTemplate{
		Evidence: fleet.WorkerInstanceEvidence{WorkerInstanceID: workerID},
	}
	err := RunWorkerInstanceEvidenceReporting(
		context.Background(),
		&recordingWorkerInstanceLoopReporter{},
		[]WorkerInstanceEvidenceTemplate{template, template},
		WorkerInstanceReportingConfig{
			Interval: time.Second, CallTimeout: time.Second,
			InitialBackoff: time.Second, MaxBackoff: 4 * time.Second,
			ObserveResult: func(WorkerInstanceReportResult) {},
		},
	)
	if err == nil {
		t.Fatal("duplicate WorkerInstance templates were accepted")
	}
	if nextWorkerInstanceReportBackoff(time.Second, 4*time.Second) != 2*time.Second ||
		nextWorkerInstanceReportBackoff(2*time.Second, 4*time.Second) != 4*time.Second ||
		nextWorkerInstanceReportBackoff(4*time.Second, 4*time.Second) != 4*time.Second {
		t.Fatal("WorkerInstance report backoff is not exponentially bounded")
	}
}

type recordingWorkerInstanceLoopReporter struct {
	mu             sync.Mutex
	failures       map[uuid.UUID]error
	block          bool
	calls          map[uuid.UUID]int
	decisionEpochs map[uuid.UUID]int64
	epochs         map[uuid.UUID][]int64
}

func (reporter *recordingWorkerInstanceLoopReporter) Report(
	ctx context.Context,
	template WorkerInstanceEvidenceTemplate,
) (fleet.WorkerInstanceDecision, error) {
	reporter.mu.Lock()
	if reporter.calls == nil {
		reporter.calls = make(map[uuid.UUID]int)
	}
	reporter.calls[template.Evidence.WorkerInstanceID]++
	if reporter.epochs == nil {
		reporter.epochs = make(map[uuid.UUID][]int64)
	}
	reporter.epochs[template.Evidence.WorkerInstanceID] = append(
		reporter.epochs[template.Evidence.WorkerInstanceID],
		template.Evidence.ControlSessionEpoch,
	)
	decisionEpoch := reporter.decisionEpochs[template.Evidence.WorkerInstanceID]
	reporter.mu.Unlock()
	if reporter.block {
		<-ctx.Done()
		return fleet.WorkerInstanceDecision{}, ctx.Err()
	}
	if err := reporter.failures[template.Evidence.WorkerInstanceID]; err != nil {
		return fleet.WorkerInstanceDecision{}, err
	}
	return fleet.WorkerInstanceDecision{
		WorkerInstanceID:    template.Evidence.WorkerInstanceID,
		InstanceEpoch:       template.Evidence.InstanceEpoch,
		ControlSessionEpoch: decisionEpoch,
		Readiness:           fleet.WorkerInstanceReady,
	}, nil
}
