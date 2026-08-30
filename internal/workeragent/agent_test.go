package workeragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/runnertransport"
	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workerrecovery"
	"github.com/vivym/vela/internal/workertransport"
)

func TestRunOnceReportsLocalIdentityReadinessBeforeAcquire(t *testing.T) {
	workerID := uuid.MustParse("20000000-0000-0000-0000-000000000001")
	poolID := uuid.MustParse("20000000-0000-0000-0000-000000000002")
	cycleID := uuid.MustParse("20000000-0000-0000-0000-000000000003")
	profileID := uuid.MustParse("20000000-0000-0000-0000-000000000004")
	deadline := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
	work := workercontrol.ReadinessWork{
		Available: true, CycleID: cycleID, Check: workercontrol.ReadinessIdentity,
		WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 7,
		NodeIdentity: "h3-node-01", ExecutionProfileRevisionID: profileID,
		InferenceBackendRevision: "sglang@backend-1", Deadline: deadline,
	}
	events := []string{}
	control := &recordingControlPlane{
		events:         &events,
		readinessWorks: []workercontrol.ReadinessWork{work},
		readinessReportResults: []workercontrol.ReadinessResult{{
			CycleID: cycleID, State: workercontrol.ReadinessChecking,
			NextCheck:       workercontrol.ReadinessDevice,
			WorkerLifecycle: "WARMING", WorkerReachability: "SUSPECT",
		}},
	}
	recovery := newTestRecoveryManager(t, workerID, 7, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 7, NodeIdentity: "h3-node-01",
		Recovery: recovery, Control: control, Runner: &recordingRunner{events: &events},
		HeartbeatInterval: time.Second, OutputRoot: t.TempDir(),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeReadinessProgress ||
		result.Readiness.State != workercontrol.ReadinessChecking ||
		result.Readiness.NextCheck != workercontrol.ReadinessDevice {
		t.Fatalf("RunOnce result = %#v", result)
	}
	if control.acquireCalls != 0 || len(control.readinessReports) != 1 {
		t.Fatalf("Acquire calls=%d readiness reports=%#v", control.acquireCalls, control.readinessReports)
	}
	wantEvidence := []byte(
		`{"check":"IDENTITY","cycle_id":"` + cycleID.String() +
			`","deadline":"` + deadline.Format(time.RFC3339Nano) +
			`","execution_profile_revision_id":"` + profileID.String() +
			`","inference_backend_revision":"sglang@backend-1","node_identity":"h3-node-01","passed":true,"schema_version":1,"worker_epoch":7,"worker_id":"` + workerID.String() +
			`","worker_pool_id":"` + poolID.String() + `"}`,
	)
	wantDigest := sha256.Sum256(wantEvidence)
	if report := control.readinessReports[0]; report.WorkerEpoch != 7 ||
		report.CycleID != cycleID || report.Check != workercontrol.ReadinessIdentity ||
		!report.Passed || report.EvidenceDigest != wantDigest {
		t.Fatalf("identity readiness report = %#v", report)
	}
	if !reflect.DeepEqual(events, []string{"control.readiness.get", "control.readiness.report"}) {
		t.Fatalf("readiness events = %#v", events)
	}
}

func TestRunOnceBootstrapsReadinessBeforeAssignmentEligibility(t *testing.T) {
	workerID := uuid.MustParse("20010000-0000-0000-0000-000000000001")
	cycleID := uuid.MustParse("20010000-0000-0000-0000-000000000002")
	work := testReadinessWork(workerID, workercontrol.ReadinessIdentity)
	work.CycleID = cycleID
	control := &recordingControlPlane{
		capacityResult: workercontrol.CapacityResult{
			WorkerState:             workercontrol.CapacityAdmittable,
			PoolState:               workercontrol.CapacityAdmittable,
			WorkerAssignmentAllowed: true,
			PoolReadinessAllowed:    true,
		},
		readinessWorks: []workercontrol.ReadinessWork{work},
		readinessReportResults: []workercontrol.ReadinessResult{{
			CycleID: cycleID, State: workercontrol.ReadinessChecking,
			NextCheck:       workercontrol.ReadinessDevice,
			WorkerLifecycle: "WARMING", WorkerReachability: "SUSPECT",
		}},
	}
	recovery := newTestRecoveryManager(t, workerID, 7, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	agent := newTestReadinessAgent(t, workerID, recovery, control, &recordingRunner{})

	result, err := agent.RunOnce(context.Background())
	if err != nil || result.Outcome != OutcomeReadinessProgress ||
		result.Readiness.NextCheck != workercontrol.ReadinessDevice {
		t.Fatalf("bootstrap readiness result=%#v error=%v", result, err)
	}
	if len(control.readinessReports) != 1 || control.acquireCalls != 0 {
		t.Fatalf(
			"bootstrap readiness reports=%#v Acquire calls=%d",
			control.readinessReports,
			control.acquireCalls,
		)
	}
}

func TestRunOnceReportsRejectedRunnerReadinessAsTerminalEvidence(t *testing.T) {
	workerID := uuid.MustParse("20020000-0000-0000-0000-000000000001")
	cycleID := uuid.MustParse("20020000-0000-0000-0000-000000000002")
	work := testReadinessWork(workerID, workercontrol.ReadinessModelWarmup)
	work.CycleID = cycleID
	control := &recordingControlPlane{
		readinessWorks: []workercontrol.ReadinessWork{work},
		readinessReportResults: []workercontrol.ReadinessResult{{
			CycleID: cycleID, State: workercontrol.ReadinessFailed,
			WorkerLifecycle: "DRAINING", WorkerReachability: "SUSPECT",
		}},
	}
	runner := &recordingRunner{readinessResults: []runnertransport.ReadinessResult{{
		Decision: runnertransport.CommandRejected,
		Detail:   "model warm-up backend exited nonzero",
	}}}
	recovery := newTestRecoveryManager(t, workerID, 7, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	agent := newTestReadinessAgent(t, workerID, recovery, control, runner)

	result, err := agent.RunOnce(context.Background())
	if err != nil || result.Outcome != OutcomeReadinessProgress ||
		result.Readiness.State != workercontrol.ReadinessFailed {
		t.Fatalf("rejected Runner readiness result=%#v error=%v", result, err)
	}
	if len(control.readinessReports) != 1 || control.readinessReports[0].Passed ||
		control.acquireCalls != 0 {
		t.Fatalf(
			"rejected Runner readiness reports=%#v Acquire calls=%d",
			control.readinessReports,
			control.acquireCalls,
		)
	}
	if len(runner.readinessChecks) != 1 ||
		runner.readinessChecks[0] != runnertransport.ReadinessModelWarmup {
		t.Fatalf("rejected Runner readiness checks=%#v", runner.readinessChecks)
	}
}

func TestRunOnceExecutesBackendReadinessThroughRunnerBeforeAcquire(t *testing.T) {
	checks := []struct {
		workerCheck  workercontrol.ReadinessCheck
		runnerCheck  runnertransport.ReadinessCheck
		state        workercontrol.ReadinessState
		next         workercontrol.ReadinessCheck
		lifecycle    string
		reachability string
	}{
		{workercontrol.ReadinessDevice, runnertransport.ReadinessDevice,
			workercontrol.ReadinessChecking, workercontrol.ReadinessInferenceBackend, "WARMING", "SUSPECT"},
		{workercontrol.ReadinessInferenceBackend, runnertransport.ReadinessInferenceBackend,
			workercontrol.ReadinessChecking, workercontrol.ReadinessModelWarmup, "WARMING", "SUSPECT"},
		{workercontrol.ReadinessModelWarmup, runnertransport.ReadinessModelWarmup,
			workercontrol.ReadinessChecking, workercontrol.ReadinessCanary, "WARMING", "SUSPECT"},
		{workercontrol.ReadinessCanary, runnertransport.ReadinessCanary,
			workercontrol.ReadinessReady, "", "READY", "HEALTHY"},
	}
	for _, test := range checks {
		t.Run(string(test.workerCheck), func(t *testing.T) {
			workerID := uuid.MustParse("20100000-0000-0000-0000-000000000001")
			cycleID := uuid.New()
			profileID := uuid.New()
			deadline := time.Now().UTC().Add(time.Minute)
			work := workercontrol.ReadinessWork{
				Available: true, CycleID: cycleID, Check: test.workerCheck,
				WorkerID: workerID, WorkerPoolID: uuid.New(), WorkerEpoch: 7,
				NodeIdentity: "h3-node-01", ExecutionProfileRevisionID: profileID,
				InferenceBackendRevision: "sglang@backend-1", Deadline: deadline,
			}
			evidence := []byte(`{"runner":"verified"}`)
			events := []string{}
			control := &recordingControlPlane{
				events: &events, readinessWorks: []workercontrol.ReadinessWork{work},
				readinessReportResults: []workercontrol.ReadinessResult{{
					CycleID: cycleID, State: test.state, NextCheck: test.next,
					WorkerLifecycle: test.lifecycle, WorkerReachability: test.reachability,
				}},
			}
			runner := &recordingRunner{
				events: &events,
				readinessResults: []runnertransport.ReadinessResult{{
					Decision: runnertransport.CommandAccepted, Passed: true, Evidence: evidence,
				}},
			}
			recovery := newTestRecoveryManager(t, workerID, 7, workerrecovery.Space{
				TotalBytes: 1 << 20, FreeBytes: 1 << 20,
			})
			agent, err := New(Config{
				WorkerID: workerID, WorkerEpoch: 7, NodeIdentity: "h3-node-01",
				Recovery: recovery, Control: control, Runner: runner,
				HeartbeatInterval: time.Second, OutputRoot: t.TempDir(),
				InferenceBackendRevision: "sglang@backend-1",
				Finalization:             &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			result, err := agent.RunOnce(context.Background())
			if err != nil || result.Outcome != OutcomeReadinessProgress || result.Readiness.State != test.state {
				t.Fatalf("RunOnce result=%#v error=%v", result, err)
			}
			if len(runner.readinessIdentities) != 1 || runner.readinessChecks[0] != test.runnerCheck {
				t.Fatalf("Runner readiness identities=%#v checks=%#v", runner.readinessIdentities, runner.readinessChecks)
			}
			identity := runner.readinessIdentities[0]
			if identity.CycleID != cycleID || identity.WorkerID != workerID || identity.WorkerEpoch != 7 ||
				identity.NodeIdentity != "h3-node-01" || identity.ExecutionProfileRevisionID != profileID ||
				identity.InferenceBackendRevision != "sglang@backend-1" || !identity.Deadline.Equal(deadline) {
				t.Fatalf("Runner readiness identity = %#v", identity)
			}
			if len(control.readinessReports) != 1 ||
				control.readinessReports[0].EvidenceDigest != sha256.Sum256(evidence) ||
				control.acquireCalls != 0 {
				t.Fatalf("readiness reports=%#v Acquire calls=%d", control.readinessReports, control.acquireCalls)
			}
			wantEvents := []string{"control.readiness.get", "runner.readiness", "control.readiness.report"}
			if !reflect.DeepEqual(events, wantEvents) {
				t.Fatalf("readiness events = %#v", events)
			}
		})
	}
}

func TestRunOnceReplaysDurableReadinessDigestAfterReportResponseLoss(t *testing.T) {
	workerID := uuid.MustParse("20200000-0000-0000-0000-000000000001")
	cycleID := uuid.MustParse("20200000-0000-0000-0000-000000000002")
	root := t.TempDir()
	newRecovery := func() *workerrecovery.Manager {
		manager, err := workerrecovery.New(workerrecovery.Config{
			Root: root, WorkerID: workerID, WorkerEpoch: 7,
			AttemptQuotaBytes: 1 << 20, MaxEntryBytes: 1 << 18, MaxEntries: 16,
			HighWatermarkBytes: 50, LowWatermarkBytes: 20, CriticalFreeBytes: 10,
			TerminalRetention: time.Minute,
			SpaceProbe: func(string) (workerrecovery.Space, error) {
				return workerrecovery.Space{TotalBytes: 1 << 20, FreeBytes: 1 << 20}, nil
			},
		})
		if err != nil {
			t.Fatalf("create recovery Manager: %v", err)
		}
		return manager
	}
	work := workercontrol.ReadinessWork{
		Available: true, CycleID: cycleID, Check: workercontrol.ReadinessDevice,
		WorkerID: workerID, WorkerPoolID: uuid.New(), WorkerEpoch: 7,
		NodeIdentity: "h3-node-01", ExecutionProfileRevisionID: uuid.New(),
		InferenceBackendRevision: "sglang@backend-1", Deadline: time.Now().UTC().Add(time.Minute),
	}
	evidence := []byte(`{"runner":"stable-device-evidence"}`)
	firstControl := &recordingControlPlane{
		readinessWorks:        []workercontrol.ReadinessWork{work},
		readinessReportErrors: []error{errors.New("readiness response lost")},
	}
	firstRunner := &recordingRunner{readinessResults: []runnertransport.ReadinessResult{{
		Decision: runnertransport.CommandAccepted, Passed: true, Evidence: evidence,
	}}}
	firstRecovery := newRecovery()
	firstAgent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 7, NodeIdentity: "h3-node-01",
		Recovery: firstRecovery, Control: firstControl, Runner: firstRunner,
		HeartbeatInterval: time.Second, OutputRoot: t.TempDir(),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New first Agent: %v", err)
	}
	if _, err := firstAgent.RunOnce(context.Background()); err == nil {
		t.Fatal("first RunOnce did not surface lost readiness response")
	}
	pending, exists, err := firstRecovery.PendingReadinessReport(context.Background())
	if err != nil || !exists || !bytes.Equal(pending.Evidence, evidence) {
		t.Fatalf("durable raw readiness evidence = %q exists=%t error=%v", pending.Evidence, exists, err)
	}

	secondControl := &recordingControlPlane{
		readinessWorks: []workercontrol.ReadinessWork{work},
		readinessReportResults: []workercontrol.ReadinessResult{{
			CycleID: cycleID, Replayed: true, State: workercontrol.ReadinessChecking,
			NextCheck:       workercontrol.ReadinessInferenceBackend,
			WorkerLifecycle: "WARMING", WorkerReachability: "SUSPECT",
		}},
	}
	secondRunner := &recordingRunner{}
	secondRecovery := newRecovery()
	secondAgent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 7, NodeIdentity: "h3-node-01",
		Recovery: secondRecovery, Control: secondControl, Runner: secondRunner,
		HeartbeatInterval: time.Second, OutputRoot: t.TempDir(),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New second Agent: %v", err)
	}
	result, err := secondAgent.RunOnce(context.Background())
	if err != nil || result.Readiness.NextCheck != workercontrol.ReadinessInferenceBackend {
		t.Fatalf("second RunOnce result=%#v error=%v", result, err)
	}

	wantDigest := sha256.Sum256(evidence)
	if firstRunner.readinessChecks == nil || len(secondRunner.readinessChecks) != 0 ||
		len(firstControl.readinessReports) != 1 || len(secondControl.readinessReports) != 1 ||
		firstControl.readinessReports[0].EvidenceDigest != wantDigest ||
		secondControl.readinessReports[0].EvidenceDigest != wantDigest {
		t.Fatalf(
			"Runner calls first=%#v second=%#v reports first=%#v second=%#v",
			firstRunner.readinessChecks, secondRunner.readinessChecks,
			firstControl.readinessReports, secondControl.readinessReports,
		)
	}
	if _, exists, err := secondRecovery.PendingReadinessReport(context.Background()); err != nil || exists {
		t.Fatalf("pending readiness after replay exists=%t error=%v", exists, err)
	}
}

func TestRunOnceReportsFailedRunnerReadinessWithoutAcquire(t *testing.T) {
	workerID := uuid.MustParse("20300000-0000-0000-0000-000000000001")
	work := testReadinessWork(workerID, workercontrol.ReadinessDevice)
	evidence := []byte(`{"device":"failed"}`)
	control := &recordingControlPlane{
		readinessWorks: []workercontrol.ReadinessWork{work},
		readinessReportResults: []workercontrol.ReadinessResult{{
			CycleID: work.CycleID, State: workercontrol.ReadinessFailed,
			WorkerLifecycle: "DRAINING", WorkerReachability: "SUSPECT",
		}},
	}
	runner := &recordingRunner{readinessResults: []runnertransport.ReadinessResult{{
		Decision: runnertransport.CommandAccepted, Passed: false, Evidence: evidence,
	}}}
	recovery := newTestRecoveryManager(t, workerID, 7, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	agent := newTestReadinessAgent(t, workerID, recovery, control, runner)

	result, err := agent.RunOnce(context.Background())
	if err != nil || result.Outcome != OutcomeReadinessProgress ||
		result.Readiness.State != workercontrol.ReadinessFailed {
		t.Fatalf("failed readiness result=%#v error=%v", result, err)
	}
	if len(control.readinessReports) != 1 || control.readinessReports[0].Passed ||
		control.readinessReports[0].EvidenceDigest != sha256.Sum256(evidence) ||
		control.acquireCalls != 0 || !reflect.DeepEqual(
		runner.readinessChecks, []runnertransport.ReadinessCheck{runnertransport.ReadinessDevice},
	) {
		t.Fatalf(
			"failed readiness reports=%#v Acquire=%d Runner=%#v",
			control.readinessReports, control.acquireCalls, runner.readinessChecks,
		)
	}
	if _, exists, pendingErr := recovery.PendingReadinessReport(context.Background()); pendingErr != nil || exists {
		t.Fatalf("failed readiness pending ledger exists=%t error=%v", exists, pendingErr)
	}
}

func TestRunOnceRejectsMismatchedReadinessAuthorityBeforeRunnerOrAcquire(t *testing.T) {
	workerID := uuid.MustParse("20400000-0000-0000-0000-000000000001")
	tests := []struct {
		name   string
		mutate func(*workercontrol.ReadinessWork)
	}{
		{"worker-id", func(work *workercontrol.ReadinessWork) { work.WorkerID = uuid.New() }},
		{"worker-epoch", func(work *workercontrol.ReadinessWork) { work.WorkerEpoch++ }},
		{"worker-pool", func(work *workercontrol.ReadinessWork) { work.WorkerPoolID = uuid.Nil }},
		{"node-identity", func(work *workercontrol.ReadinessWork) { work.NodeIdentity = "h3-node-02" }},
		{"profile", func(work *workercontrol.ReadinessWork) { work.ExecutionProfileRevisionID = uuid.Nil }},
		{"backend", func(work *workercontrol.ReadinessWork) { work.InferenceBackendRevision = "other-backend" }},
		{"deadline", func(work *workercontrol.ReadinessWork) { work.Deadline = time.Time{} }},
		{"check", func(work *workercontrol.ReadinessWork) { work.Check = "UNRECOGNIZED" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			work := testReadinessWork(workerID, workercontrol.ReadinessDevice)
			test.mutate(&work)
			control := &recordingControlPlane{readinessWorks: []workercontrol.ReadinessWork{work}}
			runner := &recordingRunner{}
			recovery := newTestRecoveryManager(t, workerID, 7, workerrecovery.Space{
				TotalBytes: 1 << 20, FreeBytes: 1 << 20,
			})
			agent := newTestReadinessAgent(t, workerID, recovery, control, runner)

			if _, err := agent.RunOnce(context.Background()); err == nil ||
				!strings.Contains(err.Error(), "readiness work") {
				t.Fatalf("mismatched readiness authority error = %v", err)
			}
			if len(runner.readinessChecks) != 0 || len(control.readinessReports) != 0 ||
				control.acquireCalls != 0 {
				t.Fatalf(
					"mismatched authority Runner=%#v reports=%#v Acquire=%d",
					runner.readinessChecks, control.readinessReports, control.acquireCalls,
				)
			}
		})
	}
}

func TestRunOnceRetainsPendingReadinessAfterInvalidControlResult(t *testing.T) {
	workerID := uuid.MustParse("20500000-0000-0000-0000-000000000001")
	work := testReadinessWork(workerID, workercontrol.ReadinessDevice)
	evidence := []byte(`{"device":"verified"}`)
	control := &recordingControlPlane{
		readinessWorks: []workercontrol.ReadinessWork{work, work},
		readinessReportResults: []workercontrol.ReadinessResult{
			{
				CycleID: work.CycleID, State: workercontrol.ReadinessChecking,
				NextCheck:       workercontrol.ReadinessCanary,
				WorkerLifecycle: "WARMING", WorkerReachability: "SUSPECT",
			},
			{
				CycleID: work.CycleID, Replayed: true, State: workercontrol.ReadinessChecking,
				NextCheck:       workercontrol.ReadinessInferenceBackend,
				WorkerLifecycle: "WARMING", WorkerReachability: "SUSPECT",
			},
		},
	}
	runner := &recordingRunner{readinessResults: []runnertransport.ReadinessResult{{
		Decision: runnertransport.CommandAccepted, Passed: true, Evidence: evidence,
	}}}
	recovery := newTestRecoveryManager(t, workerID, 7, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	agent := newTestReadinessAgent(t, workerID, recovery, control, runner)

	if _, err := agent.RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "checking result is invalid") {
		t.Fatalf("invalid readiness result error = %v", err)
	}
	pending, exists, err := recovery.PendingReadinessReport(context.Background())
	if err != nil || !exists || pending.CycleID != work.CycleID ||
		pending.Check != string(work.Check) || pending.EvidenceDigest != sha256.Sum256(evidence) {
		t.Fatalf("pending readiness after invalid result=%#v exists=%t error=%v", pending, exists, err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil || result.Readiness.NextCheck != workercontrol.ReadinessInferenceBackend {
		t.Fatalf("replay after invalid result=%#v error=%v", result, err)
	}
	if len(runner.readinessChecks) != 1 || len(control.readinessReports) != 2 ||
		control.readinessReports[0].EvidenceDigest != control.readinessReports[1].EvidenceDigest ||
		control.acquireCalls != 0 {
		t.Fatalf(
			"invalid-result replay Runner=%#v reports=%#v Acquire=%d",
			runner.readinessChecks, control.readinessReports, control.acquireCalls,
		)
	}
	if _, exists, err := recovery.PendingReadinessReport(context.Background()); err != nil || exists {
		t.Fatalf("pending readiness after valid replay exists=%t error=%v", exists, err)
	}
}

func TestRunOnceClearsLostResponseLedgerWhenControllerAdvancesCheck(t *testing.T) {
	workerID := uuid.MustParse("20600000-0000-0000-0000-000000000001")
	deviceWork := testReadinessWork(workerID, workercontrol.ReadinessDevice)
	backendWork := deviceWork
	backendWork.Check = workercontrol.ReadinessInferenceBackend
	control := &recordingControlPlane{
		readinessWorks:        []workercontrol.ReadinessWork{deviceWork, backendWork},
		readinessReportErrors: []error{errors.New("readiness response lost")},
		readinessReportResults: []workercontrol.ReadinessResult{{
			CycleID: backendWork.CycleID, State: workercontrol.ReadinessChecking,
			NextCheck:       workercontrol.ReadinessModelWarmup,
			WorkerLifecycle: "WARMING", WorkerReachability: "SUSPECT",
		}},
	}
	runner := &recordingRunner{readinessResults: []runnertransport.ReadinessResult{
		{Decision: runnertransport.CommandAccepted, Passed: true, Evidence: []byte(`{"device":"verified"}`)},
		{Decision: runnertransport.CommandAccepted, Passed: true, Evidence: []byte(`{"backend":"verified"}`)},
	}}
	recovery := newTestRecoveryManager(t, workerID, 7, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	agent := newTestReadinessAgent(t, workerID, recovery, control, runner)

	if _, err := agent.RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "readiness response lost") {
		t.Fatalf("lost readiness response error = %v", err)
	}
	result, err := agent.RunOnce(context.Background())
	if err != nil || result.Readiness.NextCheck != workercontrol.ReadinessModelWarmup {
		t.Fatalf("advanced readiness result=%#v error=%v", result, err)
	}
	if !reflect.DeepEqual(runner.readinessChecks, []runnertransport.ReadinessCheck{
		runnertransport.ReadinessDevice, runnertransport.ReadinessInferenceBackend,
	}) || len(control.readinessReports) != 2 ||
		control.readinessReports[0].Check != workercontrol.ReadinessDevice ||
		control.readinessReports[1].Check != workercontrol.ReadinessInferenceBackend ||
		control.acquireCalls != 0 {
		t.Fatalf(
			"advanced readiness Runner=%#v reports=%#v Acquire=%d",
			runner.readinessChecks, control.readinessReports, control.acquireCalls,
		)
	}
	if _, exists, err := recovery.PendingReadinessReport(context.Background()); err != nil || exists {
		t.Fatalf("pending readiness after advancement exists=%t error=%v", exists, err)
	}
}

func testReadinessWork(
	workerID uuid.UUID,
	check workercontrol.ReadinessCheck,
) workercontrol.ReadinessWork {
	return workercontrol.ReadinessWork{
		Available: true, CycleID: uuid.New(), Check: check,
		WorkerID: workerID, WorkerPoolID: uuid.New(), WorkerEpoch: 7,
		NodeIdentity: "h3-node-01", ExecutionProfileRevisionID: uuid.New(),
		InferenceBackendRevision: "sglang@backend-1",
		Deadline:                 time.Now().UTC().Add(time.Minute),
	}
}

func newTestReadinessAgent(
	t *testing.T,
	workerID uuid.UUID,
	recovery *workerrecovery.Manager,
	control *recordingControlPlane,
	runner *recordingRunner,
) *Agent {
	t.Helper()
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 7, NodeIdentity: "h3-node-01",
		Recovery: recovery, Control: control, Runner: runner,
		HeartbeatInterval: time.Second, OutputRoot: t.TempDir(),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New readiness Agent: %v", err)
	}
	return agent
}

func TestRunOnceDoesNotAcquireWhenLocalRecoveryStorageIsPressured(t *testing.T) {
	workerID := uuid.MustParse("21000000-0000-0000-0000-000000000001")
	recovery := newTestRecoveryManager(t, workerID, 7, workerrecovery.Space{
		TotalBytes: 2 << 20, FreeBytes: (2 << 20) - 60,
	})
	control := &recordingControlPlane{}
	runner := &recordingRunner{}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 7, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeBackpressured || result.Watermark != workerrecovery.WatermarkPressured {
		t.Fatalf("RunOnce result = %#v", result)
	}
	if control.acquireCalls != 0 || runner.calls != 0 {
		t.Fatalf("backpressured calls = control acquire %d runner %d", control.acquireCalls, runner.calls)
	}
}

func TestRunOnceReportsCapacityBeforeAcquireAndHonorsAuthoritativeClosure(t *testing.T) {
	workerID := uuid.MustParse("21100000-0000-0000-0000-000000000001")
	recovery := newTestRecoveryManager(t, workerID, 7, workerrecovery.Space{
		TotalBytes: 2 << 20, FreeBytes: 2 << 20,
	})
	control := &recordingControlPlane{capacityResult: workercontrol.CapacityResult{
		WorkerState: workercontrol.CapacityStorageUnavailable,
		PoolState:   workercontrol.CapacityStorageUnavailable,
	}}
	runner := &recordingRunner{}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 7, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		ArtifactStoreReachable: func(context.Context) bool { return false },
		OutputRoot:             t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	beforeObservation := time.Now().UTC()
	result, err := agent.RunOnce(context.Background())
	afterObservation := time.Now().UTC()
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeBackpressured {
		t.Fatalf("RunOnce result = %#v", result)
	}
	want := workercontrol.CapacityObservation{
		WorkerEpoch: 7, Sequence: 1, TotalBytes: 2 << 20, FreeBytes: 2 << 20,
		WatermarkState:     workercontrol.ScratchWatermarkNormal,
		HighWatermarkBytes: 50, LowWatermarkBytes: 20, CriticalFreeBytes: 10,
		ArtifactStoreReachable: false,
	}
	if len(control.capacityObservations) != 1 {
		t.Fatalf("capacity observations = %#v, want %#v", control.capacityObservations, want)
	}
	observed := control.capacityObservations[0]
	watermarkState := reflect.ValueOf(observed).FieldByName("WatermarkState")
	if !watermarkState.IsValid() || watermarkState.String() != "NORMAL" {
		t.Fatalf("capacity watermark state = %v", watermarkState)
	}
	if observed.ObservedAt.Before(beforeObservation) || observed.ObservedAt.After(afterObservation) {
		t.Fatalf("capacity observed_at = %s, want within [%s, %s]",
			observed.ObservedAt, beforeObservation, afterObservation)
	}
	observed.ObservedAt = time.Time{}
	if !reflect.DeepEqual(observed, want) {
		t.Fatalf("capacity observation = %#v, want %#v", observed, want)
	}
	if control.acquireCalls != 0 || runner.calls != 0 {
		t.Fatalf("closed capacity calls = control acquire %d runner %d", control.acquireCalls, runner.calls)
	}
}

func TestRunOnceRefreshesCapacityWhileAcceptedJobRemainsBusy(t *testing.T) {
	workerID := uuid.MustParse("21200000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("21200000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("21200000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 7, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 7, time.Minute)
	control := &periodicCapacityControl{
		recordingControlPlane: &recordingControlPlane{
			assignment: &assignment, startResult: grantedTestStart(assignment),
		},
		reports: make(chan workercontrol.CapacityObservation, 8),
	}
	statusStarted := make(chan struct{}, 1)
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statusHook: func(ctx context.Context) error {
			select {
			case statusStarted <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 7, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		CapacityReportInterval: 10 * time.Millisecond,
		ArtifactStoreReachable: func(context.Context) bool { return true },
		OutputRoot:             t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, runErr := agent.RunOnce(ctx)
		done <- runErr
	}()
	select {
	case <-statusStarted:
	case <-time.After(time.Second):
		t.Fatal("Accepted Job did not reach BUSY runner Status")
	}
	observations := make([]workercontrol.CapacityObservation, 0, 3)
	for len(observations) < 3 {
		select {
		case observation := <-control.reports:
			observations = append(observations, observation)
		case <-time.After(time.Second):
			t.Fatalf("BUSY Worker capacity observations = %#v", observations)
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled BUSY RunOnce error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("BUSY RunOnce did not stop after cancellation")
	}
	for index, observation := range observations {
		if observation.Sequence != int64(index+1) || observation.ObservedAt.IsZero() ||
			(index > 0 && observation.ObservedAt.Before(observations[index-1].ObservedAt)) {
			t.Fatalf("periodic BUSY capacity observations = %#v", observations)
		}
	}
}

func TestRunOnceTreatsNoAssignmentAsIdleWithoutCallingRunner(t *testing.T) {
	workerID := uuid.MustParse("22000000-0000-0000-0000-000000000002")
	recovery := newTestRecoveryManager(t, workerID, 7, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	control := &recordingControlPlane{}
	runner := &recordingRunner{}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 7, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeIdle || result.Watermark != workerrecovery.WatermarkNormal {
		t.Fatalf("RunOnce result = %#v", result)
	}
	if control.acquireCalls != 1 || runner.calls != 0 {
		t.Fatalf("idle calls = control acquire %d runner %d", control.acquireCalls, runner.calls)
	}
}

func TestNewRejectsOutputRootEqualToRecoveryQuarantineRoot(t *testing.T) {
	workerID := uuid.MustParse("22500000-0000-0000-0000-000000000002")
	recovery := newTestRecoveryManager(t, workerID, 7, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	quarantineRoot, err := recovery.QuarantineRoot()
	if err != nil {
		t.Fatalf("QuarantineRoot: %v", err)
	}

	_, err = New(Config{
		WorkerID: workerID, WorkerEpoch: 7, Recovery: recovery,
		Control: &recordingControlPlane{}, Runner: &recordingRunner{},
		HeartbeatInterval: time.Second, OutputRoot: quarantineRoot,
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             &recordingFinalizationControl{},
		PartUploader:             &recordingPartUploader{},
	})
	if err == nil || err.Error() != "approved output root and output quarantine root must be distinct" {
		t.Fatalf("New error = %v", err)
	}
}

func TestRunOnceBindsRecoveryAndStartsRunnerOnlyAfterBillableStart(t *testing.T) {
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000003")
	attemptID := uuid.MustParse("24000000-0000-0000-0000-000000000004")
	jobID := uuid.MustParse("25000000-0000-0000-0000-000000000005")
	recovery := newTestRecoveryManager(t, workerID, 7, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	events := []string{}
	assignment := workercontrol.Assignment{
		AttemptID: attemptID, JobID: jobID, WorkerID: workerID, WorkerEpoch: 7,
		ModelRevisionID:            uuid.MustParse("26000000-0000-0000-0000-000000000006"),
		GenerationPresetRevisionID: uuid.MustParse("27000000-0000-0000-0000-000000000007"),
		ExecutionProfileRevisionID: uuid.MustParse("28000000-0000-0000-0000-000000000008"),
		OutputSpecID:               uuid.MustParse("29000000-0000-0000-0000-000000000009"),
		RequestContent:             `{"prompt":"execute privately"}`,
		AttemptNumber:              1, LeaseToken: "lease-token", LeaseFence: 3,
		LeaseValidFor: 30 * time.Second,
	}
	control := &recordingControlPlane{
		assignment: &assignment, events: &events,
		startResult: workercontrol.StartResult{
			Decision: workercontrol.StartGranted, AttemptID: attemptID, JobID: jobID,
			WorkerID: workerID, WorkerEpoch: 7, LeaseFence: 3,
		},
	}
	outputRoot := t.TempDir()
	attemptOutputRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptOutputRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	content := []byte("0123456789")
	digest := sha256.Sum256(content)
	outputPath := filepath.Join(attemptOutputRoot, "video.mp4")
	if err := os.WriteFile(outputPath, content, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	runner := &recordingRunner{
		events: &events,
		prepareResult: runnertransport.PrepareResult{
			Decision: runnertransport.CommandAccepted,
		},
		startResult: runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth:          json.RawMessage(`{"healthy":true}`),
			LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted,
			Outputs: []runnertransport.Output{{
				Kind: "VIDEO", Path: outputPath, SizeBytes: 10,
				SHA256: digest, ContentType: "video/mp4",
			}},
		},
	}
	finalization := successfulTestFinalization(assignment, runner.collectResult.Outputs, &events)
	uploader := &recordingPartUploader{events: &events}
	monotonic := time.Duration(5 * time.Second)
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 7, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		MonotonicNow: func() time.Duration { return monotonic },
		OutputRoot:   outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             finalization, PartUploader: uploader,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeVisibleCompletion || result.AttemptID != attemptID ||
		result.VisibleCompletion.ArtifactSetID == uuid.Nil {
		t.Fatalf("RunOnce result = %#v", result)
	}
	wantEvents := []string{
		"control.acquire", "runner.prepare", "control.start", "runner.start", "runner.status",
		"runner.collect_outputs", "finalization.begin", "control.heartbeat",
		"finalization.claim", "artifact.put",
		"finalization.complete_upload", "finalization.verify", "finalization.visible_completion",
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("execution events = %#v, want %#v", events, wantEvents)
	}
	if runner.identity.AttemptID != attemptID || runner.identity.WorkerID != workerID ||
		runner.identity.WorkerEpoch != 7 || runner.identity.LeaseFence != 3 ||
		runner.spec.ExecutionProfileRevisionID != assignment.ExecutionProfileRevisionID ||
		string(runner.spec.RequestContent) != assignment.RequestContent || !runner.sameAuthorityRecovery {
		t.Fatalf("runner authority/spec = %#v / %#v recovery=%t", runner.identity, runner.spec, runner.sameAuthorityRecovery)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 7, Fence: 3,
	}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
		t.Fatalf("Visible Completion Local Recovery State reopened: %v", err)
	}
}

func TestRunOnceRejectsRunnerOutputOutsideApprovedRoot(t *testing.T) {
	workerID := uuid.MustParse("2a000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("2a000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("2a000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 7, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 7, time.Minute)
	control := &recordingControlPlane{
		assignment:  &assignment,
		startResult: grantedTestStart(assignment),
	}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted,
			Outputs: []runnertransport.Output{{
				Kind: "VIDEO", Path: filepath.Join(t.TempDir(), "escaped.mp4"), SizeBytes: 10,
				SHA256: [32]byte{1}, ContentType: "video/mp4",
			}},
		},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 7, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := agent.RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "approved output root") {
		t.Fatalf("RunOnce error = %v, want approved output root rejection", err)
	}
}

func TestOpenVerifiedOutputRejectsCrossRootHardlink(t *testing.T) {
	attemptID := uuid.MustParse("2a100000-0000-0000-0000-000000000001")
	outputRoot := t.TempDir()
	outputRoot, err := filepath.EvalSymlinks(outputRoot)
	if err != nil {
		t.Fatalf("resolve output root: %v", err)
	}
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := []byte("cross-root-hardlink")
	digest := sha256.Sum256(payload)
	externalPath := filepath.Join(t.TempDir(), "external.mp4")
	if err := os.WriteFile(externalPath, payload, 0o600); err != nil {
		t.Fatalf("write external output inode: %v", err)
	}
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.Link(externalPath, outputPath); err != nil {
		t.Fatalf("link external output inode: %v", err)
	}
	agent := &Agent{outputRoot: outputRoot, outputOwnerUID: uint32(os.Geteuid())}
	file, _, err := agent.openVerifiedOutput(context.Background(), runnertransport.Output{
		Kind: "VIDEO", Path: outputPath, SizeBytes: int64(len(payload)),
		SHA256: digest, ContentType: "video/mp4",
	}, attemptID)
	if file != nil {
		_ = file.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "link") {
		t.Fatalf("openVerifiedOutput error = %v, want hardlink rejection", err)
	}
}

func TestCleanupCommittedOutputsPreservesBothInodesOnDirectorySwap(t *testing.T) {
	outputRoot := t.TempDir()
	quarantineRoot := t.TempDir()
	var err error
	outputRoot, err = filepath.EvalSymlinks(outputRoot)
	if err != nil {
		t.Fatalf("resolve output root: %v", err)
	}
	quarantineRoot, err = filepath.EvalSymlinks(quarantineRoot)
	if err != nil {
		t.Fatalf("resolve quarantine root: %v", err)
	}
	attemptID := uuid.MustParse("29000000-0000-0000-0000-000000000001")
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create original Attempt output directory: %v", err)
	}
	original := []byte("original-runner-output")
	replacement := []byte("replacement-runner-file")
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, original, 0o600); err != nil {
		t.Fatalf("write original output: %v", err)
	}
	digest := sha256.Sum256(original)
	movedRoot := filepath.Join(outputRoot, "moved-original")
	agent := &Agent{
		outputRoot: outputRoot, outputQuarantineRoot: quarantineRoot,
		outputOwnerUID: uint32(os.Geteuid()),
		beforeOutputQuarantine: func() {
			if err := os.Rename(attemptRoot, movedRoot); err != nil {
				t.Fatalf("move original Attempt directory: %v", err)
			}
			if err := os.Mkdir(attemptRoot, 0o700); err != nil {
				t.Fatalf("create replacement Attempt directory: %v", err)
			}
			if err := os.WriteFile(outputPath, replacement, 0o600); err != nil {
				t.Fatalf("write replacement output: %v", err)
			}
		},
	}

	err = agent.cleanupCommittedOutputs(context.Background(), attemptID, []runnertransport.Output{{
		Kind: "VIDEO", Ordinal: 0, Path: outputPath,
		SizeBytes: int64(len(original)), SHA256: digest, ContentType: "video/mp4",
	}})
	if err == nil {
		t.Fatal("cleanup accepted an Attempt directory swap")
	}
	if content, readErr := os.ReadFile(filepath.Join(movedRoot, "video.mp4")); readErr != nil || !bytes.Equal(content, original) {
		t.Fatalf("original output changed: content=%q error=%v cleanup=%v", content, readErr, err)
	}
	entries, readErr := os.ReadDir(quarantineRoot)
	if readErr != nil || len(entries) != 1 {
		t.Fatalf("quarantine entries = %#v error=%v", entries, readErr)
	}
	quarantinedReplacement := filepath.Join(quarantineRoot, entries[0].Name(), "video.mp4")
	if content, readErr := os.ReadFile(quarantinedReplacement); readErr != nil || !bytes.Equal(content, replacement) {
		t.Fatalf("replacement output changed: content=%q error=%v", content, readErr)
	}
}

func TestRunOnceReportsPrepareRejectionBeforeTerminalRecoveryCleanup(t *testing.T) {
	workerID := uuid.MustParse("2b000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("2b000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("2b000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 8, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 8, time.Minute)
	events := []string{}
	control := &recordingControlPlane{
		assignment: &assignment, events: &events,
		failResult: workercontrol.RetryDecision{
			Disposition: workercontrol.RetryDispositionFailed, FailureClass: "RUNNER_PREPARE_REJECTED",
			AttemptID: attemptID, JobID: jobID, AttemptState: workercontrol.FailedAttempt,
			JobFence: 4, JobVersion: 5, DecidedAt: time.Date(2026, 8, 25, 7, 0, 0, 0, time.UTC),
		},
	}
	runner := &recordingRunner{
		events: &events,
		prepareResult: runnertransport.PrepareResult{
			Decision: runnertransport.CommandRejected, Detail: "execution profile is unavailable",
		},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 8, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeFailed || result.AttemptID != attemptID || result.JobID != jobID {
		t.Fatalf("RunOnce result = %#v", result)
	}
	if control.failureObservation.FailureClass != "RUNNER_PREPARE_REJECTED" ||
		control.failureObservation.FailureFingerprint != "runner/prepare/rejected" ||
		control.failureObservation.ErrorSummary != "execution profile is unavailable" ||
		control.failureObservation.BackendStage != "prepare" ||
		control.failureObservation.InferenceBackendRevision != "sglang@backend-1" ||
		control.failureObservation.RetryRecommended || !control.failureObservation.WorkerReusable {
		t.Fatalf("Prepare FailureObservation = %#v", control.failureObservation)
	}
	if !reflect.DeepEqual(events, []string{"control.acquire", "runner.prepare", "control.fail"}) {
		t.Fatalf("Prepare rejection events = %#v", events)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 8, Fence: assignment.LeaseFence,
	}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
		t.Fatalf("Prepare-rejected Local Recovery State reopened: %v", err)
	}
}

func TestRunOnceReportsStartRejectionAfterBillableStart(t *testing.T) {
	workerID := uuid.MustParse("2c000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("2c000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("2c000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 9, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 9, time.Minute)
	events := []string{}
	control := &recordingControlPlane{
		assignment: &assignment, events: &events, startResult: grantedTestStart(assignment),
		failResult: workercontrol.RetryDecision{
			Disposition:  workercontrol.RetryDispositionRetryWait,
			FailureClass: "RUNNER_START_REJECTED", AttemptID: attemptID, JobID: jobID,
			AttemptState: workercontrol.FailedAttempt, JobFence: 5, JobVersion: 6,
			DecidedAt: time.Date(2026, 8, 25, 7, 1, 0, 0, time.UTC),
		},
	}
	runner := &recordingRunner{
		events:        &events,
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult: runnertransport.CommandResult{
			Decision: runnertransport.CommandRejected, Detail: "backend refused execution",
		},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 9, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeRetryScheduled || result.AttemptID != attemptID {
		t.Fatalf("RunOnce result = %#v", result)
	}
	if control.failureObservation.FailureClass != "RUNNER_START_REJECTED" ||
		control.failureObservation.FailureFingerprint != "runner/start/rejected" ||
		control.failureObservation.ErrorSummary != "backend refused execution" ||
		control.failureObservation.BackendStage != "start" ||
		control.failureObservation.InferenceBackendRevision != "sglang@backend-1" {
		t.Fatalf("Start FailureObservation = %#v", control.failureObservation)
	}
	wantEvents := []string{"control.acquire", "runner.prepare", "control.start", "runner.start", "control.fail"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("Start rejection events = %#v, want %#v", events, wantEvents)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 9, Fence: assignment.LeaseFence,
	}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
		t.Fatalf("Start-rejected Local Recovery State reopened: %v", err)
	}
}

func TestRunOnceReportsUnexpectedRunnerCancellation(t *testing.T) {
	workerID := uuid.MustParse("2d000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("2d000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("2d000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 10, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 10, time.Minute)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
		failResult: workercontrol.RetryDecision{
			Disposition: workercontrol.RetryDispositionRetryWait, FailureClass: "RUNNER_CANCELED",
			AttemptID: attemptID, JobID: jobID, AttemptState: workercontrol.FailedAttempt,
			JobFence: 6, JobVersion: 7, DecidedAt: time.Date(2026, 8, 25, 7, 2, 0, 0, time.UTC),
		},
	}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionCanceled, Sequence: 2, BackendStage: "dit",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"dit":"canceled"}`),
		}},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 10, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeRetryScheduled || result.AttemptID != attemptID {
		t.Fatalf("RunOnce result = %#v", result)
	}
	if control.failureObservation.FailureClass != "RUNNER_CANCELED" ||
		control.failureObservation.FailureFingerprint != "runner/canceled/unexpected" ||
		control.failureObservation.BackendStage != "dit" ||
		!control.failureObservation.RetryRecommended || !control.failureObservation.WorkerReusable {
		t.Fatalf("unexpected cancellation FailureObservation = %#v", control.failureObservation)
	}
}

func TestRunOnceStopsAndCleansUpWhenFailRejectsStaleLease(t *testing.T) {
	workerID := uuid.MustParse("2e000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("2e000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("2e000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 11, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 11, time.Minute)
	events := []string{}
	control := &recordingControlPlane{
		assignment: &assignment, events: &events, startResult: grantedTestStart(assignment),
		failResult: workercontrol.RetryDecision{
			Disposition: workercontrol.RetryDispositionRejectedStaleLease,
		},
	}
	runner := &recordingRunner{
		events:        &events,
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionFailed, Sequence: 3, BackendStage: "dit",
			GPUHealth: json.RawMessage(`{"healthy":false}`), LocalArtifactState: json.RawMessage(`{"dit":"failed"}`),
			Failure: &runnertransport.Failure{
				FailureClass: "TRANSIENT_BACKEND", FailureFingerprint: "dit/timeout",
				ErrorSummary: "backend timed out", BackendStage: "dit",
				InferenceBackendRevision: "sglang@backend-1", RetryRecommended: true, WorkerReusable: true,
			},
		}},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 11, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeControlPlaneStopped || result.StopReason != workercontrol.StopInvalidAuthority {
		t.Fatalf("RunOnce result = %#v", result)
	}
	wantEvents := []string{
		"control.acquire", "runner.prepare", "control.start", "runner.start",
		"runner.status", "control.fail", "runner.cancel", "runner.status",
	}
	if !reflect.DeepEqual(events, wantEvents) || runner.cancelReason != runnertransport.CancelControlPlaneStop {
		t.Fatalf("stale Fail events = %#v cancel=%s", events, runner.cancelReason)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 11, Fence: assignment.LeaseFence,
	}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
		t.Fatalf("stale-fenced Local Recovery State reopened: %v", err)
	}
}

func TestRunOnceUsesLiveCleanupContextWhenCanceledDuringRunnerRPC(t *testing.T) {
	workerID := uuid.MustParse("2f000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("2f000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("2f000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 12, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 12, time.Minute)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
	}
	ctx, cancel := context.WithCancel(context.Background())
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statusHook: func(context.Context) error {
			cancel()
			return context.Canceled
		},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 12, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = agent.RunOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnce error = %v, want context canceled", err)
	}
	if runner.cancelReason != runnertransport.CancelAgentShutdown || runner.cancelContextErr != nil {
		t.Fatalf("shutdown cancellation reason=%s context error=%v", runner.cancelReason, runner.cancelContextErr)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 12, Fence: assignment.LeaseFence,
	}); err != nil {
		t.Fatalf("canceled RPC removed resumable Local Recovery State: %v", err)
	}
}

func TestRunOncePublishesVisibleCompletionBeforeTerminalCleanup(t *testing.T) {
	workerID := uuid.MustParse("30000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("30000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("30000000-0000-0000-0000-000000000003")
	videoArtifactID := uuid.MustParse("30000000-0000-0000-0000-000000000004")
	videoUploadID := uuid.MustParse("30000000-0000-0000-0000-000000000005")
	thumbnailArtifactID := uuid.MustParse("30000000-0000-0000-0000-000000000006")
	thumbnailUploadID := uuid.MustParse("30000000-0000-0000-0000-000000000007")
	artifactSetID := uuid.MustParse("30000000-0000-0000-0000-000000000008")
	chargeID := uuid.MustParse("30000000-0000-0000-0000-000000000009")
	recovery := newTestRecoveryManager(t, workerID, 13, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 13, time.Minute)
	events := []string{}
	control := &recordingControlPlane{
		assignment: &assignment, events: &events, startResult: grantedTestStart(assignment),
	}
	outputRoot := t.TempDir()
	attemptOutputRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptOutputRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	video := []byte("worker-agent-video")
	thumbnail := []byte("worker-agent-thumbnail")
	videoPath := filepath.Join(attemptOutputRoot, "video.mp4")
	thumbnailPath := filepath.Join(attemptOutputRoot, "thumbnail.webp")
	for path, content := range map[string][]byte{videoPath: video, thumbnailPath: thumbnail} {
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatalf("write runner output: %v", err)
		}
	}
	videoDigest := sha256.Sum256(video)
	thumbnailDigest := sha256.Sum256(thumbnail)
	runner := &recordingRunner{
		events:        &events,
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted,
			Outputs: []runnertransport.Output{
				{Kind: "VIDEO", Ordinal: 0, Path: videoPath, SizeBytes: int64(len(video)), SHA256: videoDigest, ContentType: "video/mp4"},
				{Kind: "THUMBNAIL", Ordinal: 0, Path: thumbnailPath, SizeBytes: int64(len(thumbnail)), SHA256: thumbnailDigest, ContentType: "image/webp"},
			},
		},
	}
	finalization := &recordingFinalizationControl{
		events: &events,
		plan: workercontrol.FinalizationPlan{
			Decision: workercontrol.FinalizationGranted, AttemptID: attemptID, JobID: jobID,
			JobVersion: 9, FinalizationStartedAt: time.Date(2026, 8, 25, 9, 0, 0, 0, time.UTC),
			FinalizationDeadlineAt: time.Date(2026, 8, 25, 9, 10, 0, 0, time.UTC),
			Artifacts: []workercontrol.PlannedArtifact{
				{ArtifactID: videoArtifactID, UploadID: videoUploadID, Kind: workercontrol.ArtifactKindVideo, Ordinal: 0, ObjectKey: "artifacts/org/project/job/attempt/video/video.mp4", ExpiresAt: time.Date(2026, 9, 25, 9, 0, 0, 0, time.UTC)},
				{ArtifactID: thumbnailArtifactID, UploadID: thumbnailUploadID, Kind: workercontrol.ArtifactKindThumbnail, Ordinal: 0, ObjectKey: "artifacts/org/project/job/attempt/thumbnail/thumbnail.webp", ExpiresAt: time.Date(2026, 9, 25, 9, 0, 0, 0, time.UTC)},
			},
		},
		contentTypes: map[uuid.UUID]string{videoUploadID: "video/mp4", thumbnailUploadID: "image/webp"},
		completion: workercontrol.VisibleCompletionResult{
			Decision: workercontrol.VisibleCompletionCommitted, JobID: jobID, AttemptID: attemptID,
			ArtifactSetID: artifactSetID, ChargeID: chargeID, JobVersion: 10,
			ManifestSHA256: sha256.Sum256([]byte("manifest")),
			CompletedAt:    time.Date(2026, 8, 25, 9, 5, 0, 0, time.UTC),
			Artifacts: []workercontrol.CommittedArtifact{
				{ArtifactID: videoArtifactID, Kind: workercontrol.ArtifactKindVideo, Ordinal: 0, ObjectKey: "artifacts/org/project/job/attempt/video/video.mp4", ObjectVersionID: "video-version", SizeBytes: int64(len(video)), SHA256: videoDigest, ContentType: "video/mp4"},
				{ArtifactID: thumbnailArtifactID, Kind: workercontrol.ArtifactKindThumbnail, Ordinal: 0, ObjectKey: "artifacts/org/project/job/attempt/thumbnail/thumbnail.webp", ObjectVersionID: "thumbnail-version", SizeBytes: int64(len(thumbnail)), SHA256: thumbnailDigest, ContentType: "image/webp"},
			},
		},
	}
	uploader := &recordingPartUploader{events: &events}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 13, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             finalization, PartUploader: uploader,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeVisibleCompletion || result.VisibleCompletion.ArtifactSetID != artifactSetID ||
		result.VisibleCompletion.ChargeID != chargeID {
		t.Fatalf("RunOnce result = %#v", result)
	}
	if len(uploader.payloads) != 2 || string(uploader.payloads[0]) != string(video) ||
		string(uploader.payloads[1]) != string(thumbnail) {
		t.Fatalf("uploaded payloads = %#v", uploader.payloads)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 13, Fence: assignment.LeaseFence,
	}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
		t.Fatalf("completed Local Recovery State reopened: %v", err)
	}
	for _, path := range []string{videoPath, thumbnailPath} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("terminal output %s remains: %v", path, err)
		}
	}
}

func TestRunOnceResumesCleanupAfterVisibleCompletion(t *testing.T) {
	workerID := uuid.MustParse("2f100000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("2f100000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("2f100000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 12, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 12, time.Minute)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
	}
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := []byte("cleanup-replay")
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	blocker := filepath.Join(attemptRoot, "untracked-blocker")
	if err := os.WriteFile(blocker, []byte("block cleanup"), 0o600); err != nil {
		t.Fatalf("write cleanup blocker: %v", err)
	}
	outputs := []runnertransport.Output{{
		Kind: "VIDEO", Ordinal: 0, Path: outputPath, SizeBytes: int64(len(payload)),
		SHA256: digest, ContentType: "video/mp4",
	}}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted, Outputs: outputs,
		},
	}
	finalization := successfulTestFinalization(assignment, outputs, nil)
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 12, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             finalization, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	result, err := agent.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unexpected entry") ||
		result.Outcome != OutcomeVisibleCompletion {
		t.Fatalf("first RunOnce = %#v error=%v, want committed cleanup failure", result, err)
	}
	if _, statErr := os.Lstat(outputPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("committed output was not removed before injected cleanup failure: %v", statErr)
	}
	quarantinedAttemptRoot := testAttemptQuarantinePath(t, recovery, attemptID)
	quarantineRoot, err := recovery.QuarantineRoot()
	if err != nil {
		t.Fatalf("QuarantineRoot: %v", err)
	}
	duplicateQuarantine := filepath.Join(quarantineRoot, attemptID.String()+"-duplicate")
	if err := os.Mkdir(duplicateQuarantine, 0o700); err != nil {
		t.Fatalf("create duplicate Attempt quarantine: %v", err)
	}
	beginCalls := len(finalization.completionIDs)
	result, err = agent.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "multiple Attempt output quarantines") ||
		result.Outcome != OutcomeVisibleCompletion {
		t.Fatalf("duplicate-quarantine RunOnce = %#v error=%v", result, err)
	}
	if len(finalization.completionIDs) != beginCalls {
		t.Fatalf("duplicate-quarantine replay repeated Visible Completion: %d -> %d", beginCalls, len(finalization.completionIDs))
	}
	if err := os.Remove(duplicateQuarantine); err != nil {
		t.Fatalf("remove duplicate Attempt quarantine: %v", err)
	}

	quarantinedBlocker := filepath.Join(quarantinedAttemptRoot, filepath.Base(blocker))
	if err := os.Remove(quarantinedBlocker); err != nil {
		t.Fatalf("remove cleanup blocker: %v", err)
	}
	marker := filepath.Join(quarantinedAttemptRoot, ".cleanup-complete")
	markerTarget := filepath.Join(t.TempDir(), "outside-marker-target")
	if err := os.WriteFile(markerTarget, []byte("must remain"), 0o600); err != nil {
		t.Fatalf("write external cleanup-marker target: %v", err)
	}
	if err := os.Symlink(markerTarget, marker); err != nil {
		t.Fatalf("create invalid cleanup marker: %v", err)
	}
	result, err = agent.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "cleanup marker is invalid") ||
		result.Outcome != OutcomeVisibleCompletion {
		t.Fatalf("invalid-marker RunOnce = %#v error=%v", result, err)
	}
	if len(finalization.completionIDs) != beginCalls {
		t.Fatalf("invalid-marker replay repeated Visible Completion: %d -> %d", beginCalls, len(finalization.completionIDs))
	}
	if target, err := os.ReadFile(markerTarget); err != nil || string(target) != "must remain" {
		t.Fatalf("external cleanup-marker target changed: payload=%q error=%v", target, err)
	}
	if err := os.Remove(marker); err != nil {
		t.Fatalf("remove invalid cleanup marker: %v", err)
	}
	if err := os.WriteFile(filepath.Join(quarantinedAttemptRoot, ".cleanup-complete"), nil, 0o600); err != nil {
		t.Fatalf("record simulated cleanup marker: %v", err)
	}
	replacementPayload := []byte("replacement Attempt output")
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create replacement Attempt output root: %v", err)
	}
	replacementPath := filepath.Join(attemptRoot, "replacement.mp4")
	if err := os.WriteFile(replacementPath, replacementPayload, 0o600); err != nil {
		t.Fatalf("write replacement Attempt output: %v", err)
	}
	result, err = agent.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "replaced during quarantine") ||
		result.Outcome != OutcomeVisibleCompletion {
		t.Fatalf("replacement-path RunOnce = %#v error=%v", result, err)
	}
	if len(finalization.completionIDs) != beginCalls {
		t.Fatalf("replacement-path replay repeated Visible Completion: %d -> %d", beginCalls, len(finalization.completionIDs))
	}
	if replacement, err := os.ReadFile(replacementPath); err != nil || !bytes.Equal(replacement, replacementPayload) {
		t.Fatalf("replacement Attempt output changed: payload=%q error=%v", replacement, err)
	}
	if _, err := os.Lstat(quarantinedAttemptRoot); err != nil {
		t.Fatalf("Attempt output quarantine was removed while source replacement existed: %v", err)
	}
	if err := os.Remove(replacementPath); err != nil {
		t.Fatalf("remove replacement Attempt output: %v", err)
	}
	if err := os.Remove(attemptRoot); err != nil {
		t.Fatalf("remove replacement Attempt output root: %v", err)
	}

	result, err = agent.RunOnce(context.Background())
	if err != nil || result.Outcome != OutcomeVisibleCompletion {
		t.Fatalf("resumed RunOnce = %#v error=%v", result, err)
	}
	if len(finalization.completionIDs) != beginCalls {
		t.Fatalf("cleanup replay repeated Visible Completion: %d -> %d", beginCalls, len(finalization.completionIDs))
	}
	if _, statErr := os.Lstat(attemptRoot); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("Attempt output root remains after cleanup replay: %v", statErr)
	}
	if _, statErr := os.Lstat(quarantinedAttemptRoot); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("Attempt output quarantine remains after cleanup replay: %v", statErr)
	}
}

func TestRunOnceRemovesExactOutputsWhenFinalizationAuthorityIsStale(t *testing.T) {
	workerID := uuid.MustParse("30010000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("30010000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("30010000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 13, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 13, time.Minute)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
	}
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := []byte("stale-finalization-output")
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted,
			Outputs: []runnertransport.Output{{
				Kind: "VIDEO", Path: outputPath, SizeBytes: int64(len(payload)),
				SHA256: digest, ContentType: "video/mp4",
			}},
		},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 13, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{plan: workercontrol.FinalizationPlan{
			Decision: workercontrol.FinalizationRejectedStaleLease,
		}},
		PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeControlPlaneStopped || result.StopReason != workercontrol.StopInvalidAuthority ||
		runner.cancelReason != runnertransport.CancelControlPlaneStop {
		t.Fatalf("stale Finalization result = %#v cancel=%s", result, runner.cancelReason)
	}
	if _, err := os.Lstat(outputPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale Finalization output remains: %v", err)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 13, Fence: assignment.LeaseFence,
	}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
		t.Fatalf("stale Finalization Local Recovery State reopened: %v", err)
	}
}

func TestRunOnceResumesFinalizationAfterLostBeginResponse(t *testing.T) {
	workerID := uuid.MustParse("30100000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("30100000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("30100000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 14, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 14, time.Minute)
	events := []string{}
	control := &recordingControlPlane{
		assignment: &assignment, events: &events, startResult: grantedTestStart(assignment),
	}
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := []byte("resume-finalization")
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	outputs := []runnertransport.Output{{
		Kind: "VIDEO", Ordinal: 0, Path: outputPath, SizeBytes: int64(len(payload)),
		SHA256: digest, ContentType: "video/mp4",
	}}
	runner := &recordingRunner{
		events:        &events,
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{Decision: runnertransport.CommandAccepted, Outputs: outputs},
	}
	finalization := successfulTestFinalization(assignment, outputs, &events)
	finalization.beginErrors = []error{errors.New("lost BeginFinalization response"), nil}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 14, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             finalization, PartUploader: &recordingPartUploader{events: &events},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := agent.RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "lost BeginFinalization response") {
		t.Fatalf("first RunOnce error = %v", err)
	}
	runnerCalls := runner.calls
	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("resumed RunOnce: %v", err)
	}
	if result.Outcome != OutcomeVisibleCompletion || control.acquireCalls != 1 || runner.calls != runnerCalls {
		t.Fatalf("resumed result=%#v acquire=%d runner calls=%d/%d", result, control.acquireCalls, runner.calls, runnerCalls)
	}
}

func TestRunOnceCleansOutputsWhenResumedFinalizationAuthorityIsStale(t *testing.T) {
	workerID := uuid.MustParse("30105000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("30105000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("30105000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 14, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 14, time.Minute)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
	}
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := []byte("stale-resumed-finalization")
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	outputs := []runnertransport.Output{{
		Kind: "VIDEO", Ordinal: 0, Path: outputPath, SizeBytes: int64(len(payload)),
		SHA256: digest, ContentType: "video/mp4",
	}}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelErrors:  []error{errors.New("lost finalization Cancel response")},
		cancelResults: []runnertransport.CommandResult{{Decision: runnertransport.CommandAccepted}},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted, Outputs: outputs,
		},
	}
	finalization := successfulTestFinalization(assignment, outputs, nil)
	grantedPlan := finalization.plan
	finalization.beginErrors = []error{errors.New("lost BeginFinalization response")}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 14, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             finalization, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := agent.RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "lost BeginFinalization response") {
		t.Fatalf("first RunOnce error = %v", err)
	}
	runnerCalls := runner.calls
	finalization.plan = workercontrol.FinalizationPlan{
		Decision: workercontrol.FinalizationRejectedStaleLease,
	}
	result, err := agent.RunOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "lost finalization Cancel response") {
		t.Fatalf("first terminal RunOnce = %#v error=%v", result, err)
	}
	if _, err := os.Lstat(outputPath); err != nil {
		t.Fatalf("failed Cancel removed finalization output: %v", err)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 14, Fence: assignment.LeaseFence,
	}); err != nil {
		t.Fatalf("failed Cancel removed finalization recovery state: %v", err)
	}
	beginCalls := finalization.beginCalls
	finalization.plan = grantedPlan
	result, err = agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("replayed terminal RunOnce: %v", err)
	}
	if result.Outcome != OutcomeControlPlaneStopped || result.StopReason != workercontrol.StopInvalidAuthority ||
		control.acquireCalls != 1 || runner.calls != runnerCalls+2 ||
		runner.cancelReason != runnertransport.CancelControlPlaneStop ||
		finalization.beginCalls != beginCalls {
		t.Fatalf("replayed result=%#v acquire=%d runner calls=%d/%d cancel=%s Begin=%d/%d",
			result, control.acquireCalls, runner.calls, runnerCalls, runner.cancelReason,
			finalization.beginCalls, beginCalls)
	}
	if _, err := os.Lstat(outputPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale resumed Finalization output remains: %v", err)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 14, Fence: assignment.LeaseFence,
	}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
		t.Fatalf("stale resumed Finalization Local Recovery State reopened: %v", err)
	}
}

func TestRunOnceKeepsRecoveryStateWhenStaleFinalizationOutputIsUnsafe(t *testing.T) {
	workerID := uuid.MustParse("30106000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("30106000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("30106000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 14, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 14, time.Minute)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
	}
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := []byte("unsafe-stale-finalization-output")
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o640); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted,
			Outputs: []runnertransport.Output{{
				Kind: "VIDEO", Path: outputPath, SizeBytes: int64(len(payload)),
				SHA256: digest, ContentType: "video/mp4",
			}},
		},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 14, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{plan: workercontrol.FinalizationPlan{
			Decision: workercontrol.FinalizationRejectedStaleLease,
		}},
		PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := agent.RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "owner, mode, type, or size is invalid") {
		t.Fatalf("RunOnce error = %v", err)
	}
	if _, err := os.Lstat(outputPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unsafe output remained visible to the Runner: %v", err)
	}
	quarantinedOutput := filepath.Join(testAttemptQuarantinePath(t, recovery, attemptID), filepath.Base(outputPath))
	preserved, err := os.ReadFile(quarantinedOutput)
	if err != nil || !bytes.Equal(preserved, payload) {
		t.Fatalf("unsafe output inode was not preserved in quarantine: payload=%q error=%v", preserved, err)
	}
	info, err := os.Lstat(quarantinedOutput)
	if err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("unsafe output metadata changed in quarantine: info=%v error=%v", info, err)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 14, Fence: assignment.LeaseFence,
	}); err != nil {
		t.Fatalf("Local Recovery State was not retained: %v", err)
	}
}

func testAttemptQuarantinePath(
	t *testing.T,
	recovery *workerrecovery.Manager,
	attemptID uuid.UUID,
) string {
	t.Helper()
	root, err := recovery.QuarantineRoot()
	if err != nil {
		t.Fatalf("QuarantineRoot: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read output quarantine root: %v", err)
	}
	prefix := attemptID.String() + "-"
	found := ""
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), prefix) {
			continue
		}
		if found != "" {
			t.Fatalf("multiple test Attempt quarantines: %q and %q", found, entry.Name())
		}
		found = filepath.Join(root, entry.Name())
	}
	if found == "" {
		t.Fatalf("Attempt %s has no output quarantine", attemptID)
	}
	return found
}

func TestRunOnceRenewsFinalizationLeaseDuringSlowArtifactUpload(t *testing.T) {
	workerID := uuid.MustParse("30110000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("30110000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("30110000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 14, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 14, time.Minute)
	heartbeats := make(chan int64, 8)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
		finalizationHeartbeats: heartbeats,
	}
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := []byte("slow-finalization-upload")
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	outputs := []runnertransport.Output{{
		Kind: "VIDEO", Ordinal: 0, Path: outputPath, SizeBytes: int64(len(payload)),
		SHA256: digest, ContentType: "video/mp4",
	}}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted, Outputs: outputs,
		},
	}
	uploader := &blockingPartUploader{started: make(chan struct{}), release: make(chan struct{})}
	released := false
	defer func() {
		if !released {
			close(uploader.release)
		}
	}()
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 14, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: 20 * time.Millisecond,
		ArtifactStoreReachable: func(context.Context) bool { return true },
		OutputRoot:             outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             successfulTestFinalization(assignment, outputs, nil),
		PartUploader:             uploader,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	type runResult struct {
		result Result
		err    error
	}
	done := make(chan runResult, 1)
	go func() {
		result, runErr := agent.RunOnce(context.Background())
		done <- runResult{result: result, err: runErr}
	}()
	select {
	case <-uploader.started:
	case <-time.After(time.Second):
		t.Fatal("Artifact upload did not start")
	}
	for count := 0; count < 2; count++ {
		select {
		case sequence := <-heartbeats:
			if sequence <= 1 {
				t.Fatalf("Finalization Heartbeat sequence = %d", sequence)
			}
		case <-time.After(300 * time.Millisecond):
			t.Fatal("Finalization Lease was not renewed during the blocked Artifact upload")
		}
	}
	close(uploader.release)
	released = true
	select {
	case completed := <-done:
		if completed.err != nil || completed.result.Outcome != OutcomeVisibleCompletion {
			t.Fatalf("RunOnce = %#v error=%v", completed.result, completed.err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunOnce did not finish after Artifact upload resumed")
	}
}

func TestRunOnceFinalizationHeartbeatPreservesCandidateWhileCompletionBlocked(t *testing.T) {
	workerID := uuid.MustParse("30115000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("30115000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("30115000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 14, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 14, time.Minute)
	heartbeats := make(chan int64, 16)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
		finalizationHeartbeats: heartbeats,
	}
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := []byte("candidate-survives-finalization-heartbeat")
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	outputs := []runnertransport.Output{{
		Kind: "VIDEO", Ordinal: 0, Path: outputPath, SizeBytes: int64(len(payload)),
		SHA256: digest, ContentType: "video/mp4",
	}}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted, Outputs: outputs,
		},
	}
	completionStarted := make(chan workercontrol.VisibleCompletionCandidate, 1)
	releaseCompletion := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseCompletion)
		}
	}()
	finalization := successfulTestFinalization(assignment, outputs, nil)
	finalization.completionHook = func(
		ctx context.Context,
		candidate workercontrol.VisibleCompletionCandidate,
	) error {
		completionStarted <- candidate
		select {
		case <-releaseCompletion:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 14, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: 20 * time.Millisecond,
		ArtifactStoreReachable: func(context.Context) bool { return true },
		OutputRoot:             outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             finalization, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	type runResult struct {
		result Result
		err    error
	}
	done := make(chan runResult, 1)
	go func() {
		result, runErr := agent.RunOnce(context.Background())
		done <- runResult{result: result, err: runErr}
	}()
	var candidate workercontrol.VisibleCompletionCandidate
	select {
	case candidate = <-completionStarted:
	case <-time.After(time.Second):
		t.Fatal("Visible Completion did not block")
	}
	handle, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 14, Fence: assignment.LeaseFence,
	})
	if err != nil {
		t.Fatalf("open active Finalization State: %v", err)
	}
	before, err := readFinalizationState(context.Background(), handle)
	if err != nil || before.CompletionCandidate == nil {
		t.Fatalf("candidate before next Heartbeat = %#v error=%v", before.CompletionCandidate, err)
	}
	deadline := time.After(time.Second)
	for {
		select {
		case sequence := <-heartbeats:
			if sequence > before.HeartbeatSequence {
				goto heartbeatObserved
			}
		case <-deadline:
			t.Fatal("Finalization Heartbeat did not run while Visible Completion was blocked")
		}
	}

heartbeatObserved:
	after, err := readFinalizationState(context.Background(), handle)
	if err != nil || after.CompletionCandidate == nil ||
		after.CompletionCandidate.CompletionID != candidate.CompletionID ||
		!reflect.DeepEqual(after.CompletionCandidate.ArtifactIDs, candidate.ArtifactIDs) {
		t.Fatalf("candidate after next Heartbeat = %#v error=%v", after.CompletionCandidate, err)
	}
	close(releaseCompletion)
	released = true
	select {
	case completed := <-done:
		if completed.err != nil || completed.result.Outcome != OutcomeVisibleCompletion {
			t.Fatalf("RunOnce = %#v error=%v", completed.result, completed.err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunOnce did not finish after Visible Completion resumed")
	}
}

func TestRunOnceReplaysPersistedFinalizationHeartbeatAfterLostResponse(t *testing.T) {
	workerID := uuid.MustParse("30120000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("30120000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("30120000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 14, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 14, time.Minute)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
		heartbeatErrors: []error{errors.New("lost Finalization Heartbeat response")},
	}
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := []byte("resume-after-lost-finalization-heartbeat")
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	outputs := []runnertransport.Output{{
		Kind: "VIDEO", Ordinal: 0, Path: outputPath, SizeBytes: int64(len(payload)),
		SHA256: digest, ContentType: "video/mp4",
	}}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted, Outputs: outputs,
		},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 14, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             successfulTestFinalization(assignment, outputs, nil),
		PartUploader:             &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := agent.RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "lost Finalization Heartbeat response") {
		t.Fatalf("first RunOnce error = %v", err)
	}
	if len(control.heartbeatObservations) != 1 || control.heartbeatObservations[0].Sequence != 2 {
		t.Fatalf("first Finalization Heartbeats = %#v", control.heartbeatObservations)
	}
	runnerCalls := runner.calls
	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("resumed RunOnce: %v", err)
	}
	if result.Outcome != OutcomeVisibleCompletion || control.acquireCalls != 1 || runner.calls != runnerCalls {
		t.Fatalf("resumed result=%#v acquire=%d runner calls=%d/%d",
			result, control.acquireCalls, runner.calls, runnerCalls)
	}
	if len(control.heartbeatObservations) != 3 ||
		control.heartbeatObservations[1].Sequence != 2 ||
		control.heartbeatObservations[2].Sequence != 3 {
		t.Fatalf("resumed Finalization Heartbeats = %#v", control.heartbeatObservations)
	}
}

func TestRunOnceReportsArtifactValidationFailureAuthoritatively(t *testing.T) {
	workerID := uuid.MustParse("30130000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("30130000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("30130000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 14, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 14, time.Minute)
	decidedAt := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
		failHook: func(context.Context, workercontrol.FailureObservation) error {
			cancel()
			return nil
		},
		failResult: workercontrol.RetryDecision{
			Disposition: workercontrol.RetryDispositionFailed, FailureClass: "ARTIFACT_VALIDATION_FAILED",
			AttemptID: attemptID, JobID: jobID, AttemptState: workercontrol.FailedAttempt,
			JobFence: assignment.LeaseFence + 1, JobVersion: 10, DecidedAt: decidedAt,
		},
	}
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := []byte("invalid-certified-output")
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	outputs := []runnertransport.Output{{
		Kind: "VIDEO", Ordinal: 0, Path: outputPath, SizeBytes: int64(len(payload)),
		SHA256: digest, ContentType: "video/mp4",
	}}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted, Outputs: outputs,
		},
	}
	finalization := successfulTestFinalization(assignment, outputs, nil)
	finalization.verificationDecisions = []workercontrol.ArtifactVerificationDecision{
		workercontrol.ArtifactValidationFailed,
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 14, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             finalization, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeFailed || result.AttemptID != attemptID || result.JobID != jobID {
		t.Fatalf("validation failure result = %#v", result)
	}
	if control.failureObservation.FailureClass != "ARTIFACT_VALIDATION_FAILED" ||
		control.failureObservation.BackendStage != "finalization" ||
		!control.failureObservation.RetryRecommended || !control.failureObservation.WorkerReusable {
		t.Fatalf("validation FailureObservation = %#v", control.failureObservation)
	}
	if runner.cancelContextErr != nil {
		t.Fatalf("terminal cleanup inherited canceled Agent context: %v", runner.cancelContextErr)
	}
	if _, err := os.Lstat(outputPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("validation-failed output remains: %v", err)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 14, Fence: assignment.LeaseFence,
	}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
		t.Fatalf("validation-failed Local Recovery State reopened: %v", err)
	}
}

func TestRunOnceResumesRecoverableFinalizationBusyDecision(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		wantError string
		configure func(*recordingFinalizationControl)
	}{
		{
			name:      "upload claim",
			wantError: string(workercontrol.ArtifactUploadClaimBusy),
			configure: func(control *recordingFinalizationControl) {
				control.claimDecisions = []workercontrol.ArtifactUploadClaimDecision{
					workercontrol.ArtifactUploadClaimBusy,
					workercontrol.ArtifactUploadClaimGranted,
				}
			},
		},
		{
			name:      "verification",
			wantError: string(workercontrol.ArtifactVerificationBusy),
			configure: func(control *recordingFinalizationControl) {
				control.verificationDecisions = []workercontrol.ArtifactVerificationDecision{
					workercontrol.ArtifactVerificationBusy,
					workercontrol.ArtifactVerified,
				}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			workerID := uuid.New()
			attemptID := uuid.New()
			jobID := uuid.New()
			recovery := newTestRecoveryManager(t, workerID, 14, workerrecovery.Space{
				TotalBytes: 1 << 30, FreeBytes: 1 << 30,
			})
			assignment := validTestAssignment(workerID, attemptID, jobID, 14, time.Minute)
			control := &recordingControlPlane{
				assignment: &assignment, startResult: grantedTestStart(assignment),
			}
			outputRoot := t.TempDir()
			attemptRoot := filepath.Join(outputRoot, attemptID.String())
			if err := os.Mkdir(attemptRoot, 0o700); err != nil {
				t.Fatalf("create Attempt output root: %v", err)
			}
			payload := []byte("recoverable-finalization-busy")
			digest := sha256.Sum256(payload)
			outputPath := filepath.Join(attemptRoot, "video.mp4")
			if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
				t.Fatalf("write runner output: %v", err)
			}
			outputs := []runnertransport.Output{{
				Kind: "VIDEO", Ordinal: 0, Path: outputPath, SizeBytes: int64(len(payload)),
				SHA256: digest, ContentType: "video/mp4",
			}}
			runner := &recordingRunner{
				prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
				startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
				statuses: []runnertransport.Status{{
					State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
					GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
				}},
				collectResult: runnertransport.CollectOutputsResult{
					Decision: runnertransport.CommandAccepted, Outputs: outputs,
				},
			}
			finalization := successfulTestFinalization(assignment, outputs, nil)
			testCase.configure(finalization)
			agent, err := New(Config{
				WorkerID: workerID, WorkerEpoch: 14, Recovery: recovery,
				Control: control, Runner: runner, HeartbeatInterval: time.Second,
				OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
				InferenceBackendRevision: "sglang@backend-1",
				Finalization:             finalization, PartUploader: &recordingPartUploader{},
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			if _, err := agent.RunOnce(context.Background()); err == nil ||
				!strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("first RunOnce error = %v", err)
			}
			if _, err := os.Lstat(outputPath); err != nil {
				t.Fatalf("recoverable busy removed output: %v", err)
			}
			runnerCalls := runner.calls
			result, err := agent.RunOnce(context.Background())
			if err != nil {
				t.Fatalf("resumed RunOnce: %v", err)
			}
			if result.Outcome != OutcomeVisibleCompletion || control.acquireCalls != 1 ||
				runner.calls != runnerCalls {
				t.Fatalf("resumed result=%#v acquire=%d runner calls=%d/%d",
					result, control.acquireCalls, runner.calls, runnerCalls)
			}
		})
	}
}

func TestRunOnceStopsAndCleansOutputsOnVisibleCompletionCandidateConflict(t *testing.T) {
	workerID := uuid.MustParse("30140000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("30140000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("30140000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 14, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 14, time.Minute)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
	}
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := []byte("conflicting-visible-completion-candidate")
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	outputs := []runnertransport.Output{{
		Kind: "VIDEO", Ordinal: 0, Path: outputPath, SizeBytes: int64(len(payload)),
		SHA256: digest, ContentType: "video/mp4",
	}}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted, Outputs: outputs,
		},
	}
	finalization := successfulTestFinalization(assignment, outputs, nil)
	finalization.completion.Decision = workercontrol.VisibleCompletionCandidateConflict
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 14, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             finalization, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeControlPlaneStopped || result.StopReason != workercontrol.StopInvalidAuthority ||
		runner.cancelReason != runnertransport.CancelControlPlaneStop {
		t.Fatalf("candidate conflict result = %#v cancel=%s", result, runner.cancelReason)
	}
	if _, err := os.Lstat(outputPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate-conflict output remains: %v", err)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 14, Fence: assignment.LeaseFence,
	}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
		t.Fatalf("candidate-conflict Local Recovery State reopened: %v", err)
	}
}

func TestRunOnceReusesFinalizationIdentitiesAfterLostStageResponse(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		wantError string
		configure func(*recordingFinalizationControl, error)
	}{
		{
			name: "upload claim", wantError: "lost upload claim response",
			configure: func(control *recordingFinalizationControl, lost error) {
				control.claimErrors = []error{lost}
			},
		},
		{
			name: "multipart completion", wantError: "lost multipart completion response",
			configure: func(control *recordingFinalizationControl, lost error) {
				control.completeUploadErrors = []error{lost}
			},
		},
		{
			name: "verification", wantError: "lost verification response",
			configure: func(control *recordingFinalizationControl, lost error) {
				control.verificationErrors = []error{lost}
			},
		},
		{
			name: "Visible Completion", wantError: "lost Visible Completion response",
			configure: func(control *recordingFinalizationControl, lost error) {
				control.completionErrors = []error{lost}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			workerID := uuid.New()
			attemptID := uuid.New()
			jobID := uuid.New()
			recovery := newTestRecoveryManager(t, workerID, 14, workerrecovery.Space{
				TotalBytes: 1 << 30, FreeBytes: 1 << 30,
			})
			assignment := validTestAssignment(workerID, attemptID, jobID, 14, time.Minute)
			control := &recordingControlPlane{
				assignment: &assignment, startResult: grantedTestStart(assignment),
			}
			outputRoot := t.TempDir()
			attemptRoot := filepath.Join(outputRoot, attemptID.String())
			if err := os.Mkdir(attemptRoot, 0o700); err != nil {
				t.Fatalf("create Attempt output root: %v", err)
			}
			payload := []byte("stable-finalization-identities")
			digest := sha256.Sum256(payload)
			outputPath := filepath.Join(attemptRoot, "video.mp4")
			if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
				t.Fatalf("write runner output: %v", err)
			}
			outputs := []runnertransport.Output{{
				Kind: "VIDEO", Ordinal: 0, Path: outputPath, SizeBytes: int64(len(payload)),
				SHA256: digest, ContentType: "video/mp4",
			}}
			runner := &recordingRunner{
				prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
				startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
				statuses: []runnertransport.Status{{
					State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
					GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
				}},
				collectResult: runnertransport.CollectOutputsResult{
					Decision: runnertransport.CommandAccepted, Outputs: outputs,
				},
			}
			finalization := successfulTestFinalization(assignment, outputs, nil)
			testCase.configure(finalization, errors.New(testCase.wantError))
			agent, err := New(Config{
				WorkerID: workerID, WorkerEpoch: 14, Recovery: recovery,
				Control: control, Runner: runner, HeartbeatInterval: time.Second,
				OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
				InferenceBackendRevision: "sglang@backend-1",
				Finalization:             finalization, PartUploader: &recordingPartUploader{},
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			if _, err := agent.RunOnce(context.Background()); err == nil ||
				!strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("first RunOnce error = %v", err)
			}
			runnerCalls := runner.calls
			result, err := agent.RunOnce(context.Background())
			if err != nil {
				t.Fatalf("resumed RunOnce: %v", err)
			}
			if result.Outcome != OutcomeVisibleCompletion || control.acquireCalls != 1 ||
				runner.calls != runnerCalls {
				t.Fatalf("resumed result=%#v acquire=%d runner calls=%d/%d",
					result, control.acquireCalls, runner.calls, runnerCalls)
			}
			assertStableUUIDs(t, "claim", finalization.claimIDs)
			if len(finalization.verificationIDs) > 1 {
				assertStableUUIDs(t, "verification", finalization.verificationIDs)
			}
			if len(finalization.completionIDs) > 1 {
				assertStableUUIDs(t, "Visible Completion", finalization.completionIDs)
			}
		})
	}
}

func TestRunOnceReplaysVisibleCompletionAfterCommittedResponseIsLost(t *testing.T) {
	workerID := uuid.MustParse("30145000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("30145000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("30145000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 14, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 14, time.Minute)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
	}
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := []byte("committed-visible-completion-response-lost")
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	outputs := []runnertransport.Output{{
		Kind: "VIDEO", Ordinal: 0, Path: outputPath, SizeBytes: int64(len(payload)),
		SHA256: digest, ContentType: "video/mp4",
	}}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted, Outputs: outputs,
		},
	}
	finalization := successfulTestFinalization(assignment, outputs, nil)
	finalization.completionErrors = []error{errors.New("response lost after Visible Completion commit")}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 14, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             finalization, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := agent.RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "response lost after Visible Completion commit") {
		t.Fatalf("first RunOnce error = %v", err)
	}
	if len(finalization.completionIDs) != 1 {
		t.Fatalf("first Visible Completion calls = %#v", finalization.completionIDs)
	}
	completionID := finalization.completionIDs[0]
	runnerCalls := runner.calls
	finalization.plan = workercontrol.FinalizationPlan{
		Decision: workercontrol.FinalizationRejectedStaleLease,
	}
	finalization.completion.Decision = workercontrol.VisibleCompletionAlreadySucceeded
	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("resumed RunOnce: %v", err)
	}
	if result.Outcome != OutcomeVisibleCompletion ||
		result.VisibleCompletion.Decision != workercontrol.VisibleCompletionAlreadySucceeded ||
		result.VisibleCompletion.CompletionID != completionID {
		t.Fatalf("resumed result = %#v, completion ID = %s", result, completionID)
	}
	if control.acquireCalls != 1 || runner.calls != runnerCalls {
		t.Fatalf("receipt replay repeated execution: Acquire=%d Runner=%d/%d", control.acquireCalls, runner.calls, runnerCalls)
	}
	assertStableUUIDs(t, "Visible Completion", finalization.completionIDs)
}

func TestRunOnceCleansOutputsAtEveryStaleFinalizationEdge(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		configure func(*recordingFinalizationControl)
	}{
		{
			name: "upload claim",
			configure: func(control *recordingFinalizationControl) {
				control.claimDecisions = []workercontrol.ArtifactUploadClaimDecision{
					workercontrol.ArtifactUploadClaimRejectedStaleLease,
				}
			},
		},
		{
			name: "multipart completion",
			configure: func(control *recordingFinalizationControl) {
				control.completeUploadDecisions = []workercontrol.ArtifactUploadDecision{
					workercontrol.ArtifactUploadRejected,
				}
			},
		},
		{
			name: "verification",
			configure: func(control *recordingFinalizationControl) {
				control.verificationDecisions = []workercontrol.ArtifactVerificationDecision{
					workercontrol.ArtifactVerificationRejected,
				}
			},
		},
		{
			name: "Visible Completion",
			configure: func(control *recordingFinalizationControl) {
				control.completion.Decision = workercontrol.VisibleCompletionRejectedStaleLease
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			workerID := uuid.New()
			attemptID := uuid.New()
			jobID := uuid.New()
			recovery := newTestRecoveryManager(t, workerID, 14, workerrecovery.Space{
				TotalBytes: 1 << 30, FreeBytes: 1 << 30,
			})
			assignment := validTestAssignment(workerID, attemptID, jobID, 14, time.Minute)
			control := &recordingControlPlane{
				assignment: &assignment, startResult: grantedTestStart(assignment),
			}
			outputRoot := t.TempDir()
			attemptRoot := filepath.Join(outputRoot, attemptID.String())
			if err := os.Mkdir(attemptRoot, 0o700); err != nil {
				t.Fatalf("create Attempt output root: %v", err)
			}
			payload := []byte("stale-finalization-stage")
			digest := sha256.Sum256(payload)
			outputPath := filepath.Join(attemptRoot, "video.mp4")
			if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
				t.Fatalf("write runner output: %v", err)
			}
			outputs := []runnertransport.Output{{
				Kind: "VIDEO", Ordinal: 0, Path: outputPath, SizeBytes: int64(len(payload)),
				SHA256: digest, ContentType: "video/mp4",
			}}
			runner := &recordingRunner{
				prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
				startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
				cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
				statuses: []runnertransport.Status{{
					State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
					GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
				}},
				collectResult: runnertransport.CollectOutputsResult{
					Decision: runnertransport.CommandAccepted, Outputs: outputs,
				},
			}
			finalization := successfulTestFinalization(assignment, outputs, nil)
			testCase.configure(finalization)
			agent, err := New(Config{
				WorkerID: workerID, WorkerEpoch: 14, Recovery: recovery,
				Control: control, Runner: runner, HeartbeatInterval: time.Second,
				OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
				InferenceBackendRevision: "sglang@backend-1",
				Finalization:             finalization, PartUploader: &recordingPartUploader{},
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			result, err := agent.RunOnce(context.Background())
			if err != nil {
				t.Fatalf("RunOnce: %v", err)
			}
			if result.Outcome != OutcomeControlPlaneStopped ||
				result.StopReason != workercontrol.StopInvalidAuthority ||
				runner.cancelReason != runnertransport.CancelControlPlaneStop {
				t.Fatalf("stale stage result = %#v cancel=%s", result, runner.cancelReason)
			}
			if _, err := os.Lstat(outputPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("stale stage output remains: %v", err)
			}
			if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
				AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 14, Fence: assignment.LeaseFence,
			}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
				t.Fatalf("stale stage Local Recovery State reopened: %v", err)
			}
		})
	}
}

func assertStableUUIDs(t *testing.T, name string, values []uuid.UUID) {
	t.Helper()
	if len(values) < 2 || values[0] == uuid.Nil {
		t.Fatalf("%s identities = %#v", name, values)
	}
	for _, value := range values[1:] {
		if value != values[0] {
			t.Fatalf("%s identities changed: %#v", name, values)
		}
	}
}

func TestRunOnceFailsClosedForMultipleActiveFinalizations(t *testing.T) {
	workerID := uuid.MustParse("30200000-0000-0000-0000-000000000001")
	recovery := newTestRecoveryManager(t, workerID, 14, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	for index, attemptID := range []uuid.UUID{
		uuid.MustParse("30200000-0000-0000-0000-000000000002"),
		uuid.MustParse("30200000-0000-0000-0000-000000000003"),
	} {
		handle, err := recovery.Open(context.Background(), workerrecovery.Identity{
			AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 14, Fence: int64(index + 1),
		})
		if err != nil {
			t.Fatalf("Open recovery handle: %v", err)
		}
		if err := writeFinalizationState(context.Background(), handle, localFinalizationState{
			AttemptID: attemptID, JobID: uuid.New(), WorkerID: workerID,
			WorkerEpoch: 14, LeaseFence: int64(index + 1), LeaseToken: "lease-token",
			CompletionID: uuid.New(), HeartbeatSequence: 1,
			GPUHealthSummary: json.RawMessage(`{"healthy":true}`),
			Outputs:          []runnertransport.Output{{Kind: "VIDEO"}},
		}); err != nil {
			t.Fatalf("write finalization state: %v", err)
		}
	}
	events := []string{}
	control := &recordingControlPlane{events: &events}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 14, Recovery: recovery,
		Control: control, Runner: &recordingRunner{events: &events}, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             &recordingFinalizationControl{events: &events},
		PartUploader:             &recordingPartUploader{events: &events},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := agent.RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "multiple active Local Finalization States") {
		t.Fatalf("RunOnce error = %v", err)
	}
	if control.acquireCalls != 0 || len(events) != 0 {
		t.Fatalf("multiple recovery states invoked external operations: acquire=%d events=%#v", control.acquireCalls, events)
	}
}

func TestRunOnceRejectsDuplicatePendingControlAuthority(t *testing.T) {
	workerID := uuid.New()
	attemptID := uuid.New()
	jobID := uuid.New()
	recovery := newTestRecoveryManager(t, workerID, 15, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	handle, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 15, Fence: 3,
	})
	if err != nil {
		t.Fatalf("Open recovery handle: %v", err)
	}
	encoded, err := json.Marshal(pendingControlOperation{
		SchemaVersion: 1,
		Kind:          "START",
		Assignment:    validTestAssignment(workerID, attemptID, jobID, 15, time.Minute),
	})
	if err != nil {
		t.Fatalf("encode pending control operation: %v", err)
	}
	corrupted := strings.Replace(
		string(encoded),
		`"LeaseToken":"lease-token"`,
		`"LeaseToken":"lease-token","LeaseToken":"lease-token"`,
		1,
	)
	if corrupted == string(encoded) {
		t.Fatal("test fixture did not duplicate Lease token authority")
	}
	if _, err := handle.Write(
		context.Background(), workerrecovery.StageUpload, pendingControlOperationName,
		strings.NewReader(corrupted),
	); err != nil {
		t.Fatalf("write corrupted pending control operation: %v", err)
	}
	events := []string{}
	control := &recordingControlPlane{events: &events}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 15, Recovery: recovery,
		Control: control, Runner: &recordingRunner{events: &events}, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             &recordingFinalizationControl{events: &events},
		PartUploader:             &recordingPartUploader{events: &events},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := agent.RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "duplicate JSON key") {
		t.Fatalf("RunOnce error = %v", err)
	}
	if control.acquireCalls != 0 || control.startCalls != 0 || len(events) != 0 {
		t.Fatalf(
			"duplicate authority invoked external operations: acquire=%d start=%d events=%#v",
			control.acquireCalls, control.startCalls, events,
		)
	}
}

func TestRunOnceRejectsCaseFoldedPendingControlAuthority(t *testing.T) {
	workerID := uuid.New()
	attemptID := uuid.New()
	jobID := uuid.New()
	recovery := newTestRecoveryManager(t, workerID, 15, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	handle, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 15, Fence: 3,
	})
	if err != nil {
		t.Fatalf("Open recovery handle: %v", err)
	}
	encoded, err := json.Marshal(pendingControlOperation{
		SchemaVersion: 1,
		Kind:          "START",
		Assignment:    validTestAssignment(workerID, attemptID, jobID, 15, time.Minute),
	})
	if err != nil {
		t.Fatalf("encode pending control operation: %v", err)
	}
	corrupted := strings.Replace(
		string(encoded), `"LeaseToken":"lease-token"`, `"leasetoken":"lease-token"`, 1,
	)
	if corrupted == string(encoded) {
		t.Fatal("test fixture did not case-fold Lease token authority")
	}
	if _, err := handle.Write(
		context.Background(), workerrecovery.StageUpload, pendingControlOperationName,
		strings.NewReader(corrupted),
	); err != nil {
		t.Fatalf("write corrupted pending control operation: %v", err)
	}
	events := []string{}
	control := &recordingControlPlane{events: &events}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 15, Recovery: recovery,
		Control: control, Runner: &recordingRunner{events: &events}, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             &recordingFinalizationControl{events: &events},
		PartUploader:             &recordingPartUploader{events: &events},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := agent.RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "non-canonical JSON key") {
		t.Fatalf("RunOnce error = %v", err)
	}
	if control.acquireCalls != 0 || control.startCalls != 0 || len(events) != 0 {
		t.Fatalf(
			"case-folded authority invoked external operations: acquire=%d start=%d events=%#v",
			control.acquireCalls, control.startCalls, events,
		)
	}
}

func TestRunOnceRejectsTrailingPendingControlOperation(t *testing.T) {
	workerID := uuid.New()
	attemptID := uuid.New()
	jobID := uuid.New()
	recovery := newTestRecoveryManager(t, workerID, 15, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	handle, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 15, Fence: 3,
	})
	if err != nil {
		t.Fatalf("Open recovery handle: %v", err)
	}
	encoded, err := json.Marshal(pendingControlOperation{
		SchemaVersion: 1,
		Kind:          "START",
		Assignment:    validTestAssignment(workerID, attemptID, jobID, 15, time.Minute),
	})
	if err != nil {
		t.Fatalf("encode pending control operation: %v", err)
	}
	if _, err := handle.Write(
		context.Background(), workerrecovery.StageUpload, pendingControlOperationName,
		bytes.NewReader(append(encoded, []byte(` {"kind":"START"}`)...)),
	); err != nil {
		t.Fatalf("write trailing pending control operation: %v", err)
	}
	events := []string{}
	control := &recordingControlPlane{events: &events}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 15, Recovery: recovery,
		Control: control, Runner: &recordingRunner{events: &events}, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             &recordingFinalizationControl{events: &events},
		PartUploader:             &recordingPartUploader{events: &events},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := agent.RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "trailing data") {
		t.Fatalf("RunOnce error = %v", err)
	}
	if control.acquireCalls != 0 || control.startCalls != 0 || len(events) != 0 {
		t.Fatalf(
			"trailing operation invoked external operations: acquire=%d start=%d events=%#v",
			control.acquireCalls, control.startCalls, events,
		)
	}
}

func TestRunOnceFailsClosedForUntrustedFinalizationState(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		write func(*testing.T, *workerrecovery.Handle, uuid.UUID, uuid.UUID)
	}{
		{
			name: "malformed",
			write: func(t *testing.T, handle *workerrecovery.Handle, _, _ uuid.UUID) {
				t.Helper()
				if _, err := handle.Write(
					context.Background(), workerrecovery.StageUpload, finalizationStateName,
					strings.NewReader("{"),
				); err != nil {
					t.Fatalf("write malformed finalization state: %v", err)
				}
			},
		},
		{
			name: "identity mismatch",
			write: func(t *testing.T, handle *workerrecovery.Handle, workerID, _ uuid.UUID) {
				t.Helper()
				if err := writeFinalizationState(context.Background(), handle, localFinalizationState{
					AttemptID: uuid.New(), JobID: uuid.New(), WorkerID: workerID,
					WorkerEpoch: 15, LeaseFence: 7, LeaseToken: "lease-token",
					CompletionID: uuid.New(), HeartbeatSequence: 1,
					GPUHealthSummary: json.RawMessage(`{"healthy":true}`),
					Outputs:          []runnertransport.Output{{Kind: "VIDEO"}},
				}); err != nil {
					t.Fatalf("write mismatched finalization state: %v", err)
				}
			},
		},
		{
			name: "unknown field",
			write: func(t *testing.T, handle *workerrecovery.Handle, workerID, attemptID uuid.UUID) {
				t.Helper()
				encoded, err := json.Marshal(localFinalizationState{
					AttemptID: attemptID, JobID: uuid.New(), WorkerID: workerID,
					WorkerEpoch: 15, LeaseFence: 7, LeaseToken: "lease-token",
					CompletionID: uuid.New(), HeartbeatSequence: 1,
					GPUHealthSummary: json.RawMessage(`{"healthy":true}`),
					Outputs:          []runnertransport.Output{{Kind: "VIDEO"}},
				})
				if err != nil {
					t.Fatalf("encode finalization state: %v", err)
				}
				encoded = append(encoded[:len(encoded)-1], []byte(`,"unexpected":true}`)...)
				if _, err := handle.Write(
					context.Background(), workerrecovery.StageUpload, finalizationStateName,
					bytes.NewReader(encoded),
				); err != nil {
					t.Fatalf("write unknown-field finalization state: %v", err)
				}
			},
		},
		{
			name: "nested duplicate key",
			write: func(t *testing.T, handle *workerrecovery.Handle, workerID, attemptID uuid.UUID) {
				t.Helper()
				if err := writeFinalizationState(context.Background(), handle, localFinalizationState{
					AttemptID: attemptID, JobID: uuid.New(), WorkerID: workerID,
					WorkerEpoch: 15, LeaseFence: 7, LeaseToken: "lease-token",
					CompletionID: uuid.New(), HeartbeatSequence: 1,
					GPUHealthSummary: json.RawMessage(`{"healthy":true,"healthy":false}`),
					Outputs:          []runnertransport.Output{{Kind: "VIDEO"}},
				}); err != nil {
					t.Fatalf("write duplicate-key finalization state: %v", err)
				}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			workerID := uuid.New()
			attemptID := uuid.New()
			recovery := newTestRecoveryManager(t, workerID, 15, workerrecovery.Space{
				TotalBytes: 1 << 30, FreeBytes: 1 << 30,
			})
			handle, err := recovery.Open(context.Background(), workerrecovery.Identity{
				AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 15, Fence: 7,
			})
			if err != nil {
				t.Fatalf("Open recovery handle: %v", err)
			}
			testCase.write(t, handle, workerID, attemptID)
			events := []string{}
			control := &recordingControlPlane{events: &events}
			agent, err := New(Config{
				WorkerID: workerID, WorkerEpoch: 15, Recovery: recovery,
				Control: control, Runner: &recordingRunner{events: &events}, HeartbeatInterval: time.Second,
				OutputRoot: t.TempDir(), OutputOwnerUID: uint32(os.Geteuid()),
				InferenceBackendRevision: "sglang@backend-1",
				Finalization:             &recordingFinalizationControl{events: &events},
				PartUploader:             &recordingPartUploader{events: &events},
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			if _, err := agent.RunOnce(context.Background()); err == nil {
				t.Fatal("RunOnce accepted untrusted Local Finalization State")
			}
			if control.acquireCalls != 0 || len(events) != 0 {
				t.Fatalf("untrusted state invoked external operations: acquire=%d events=%#v", control.acquireCalls, events)
			}
		})
	}
}

func TestRunOnceFailsClosedForUntrustedExecutionHeartbeatState(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		write func(*testing.T, *workerrecovery.Handle, uuid.UUID, uuid.UUID)
	}{
		{
			name: "malformed",
			write: func(t *testing.T, handle *workerrecovery.Handle, _, _ uuid.UUID) {
				t.Helper()
				if _, err := handle.Write(
					context.Background(), workerrecovery.StageUpload, executionHeartbeatStateName,
					strings.NewReader("{"),
				); err != nil {
					t.Fatalf("write malformed execution Heartbeat State: %v", err)
				}
			},
		},
		{
			name: "identity mismatch",
			write: func(t *testing.T, handle *workerrecovery.Handle, _, attemptID uuid.UUID) {
				t.Helper()
				encoded, err := json.Marshal(executionHeartbeatState{
					SchemaVersion: 1,
					Authority: localAttemptAuthority{
						AttemptID: attemptID, JobID: uuid.New(), WorkerID: uuid.New(),
						WorkerEpoch: 15, LeaseFence: 7,
					},
					Sequence: 3,
				})
				if err != nil {
					t.Fatalf("encode mismatched execution Heartbeat State: %v", err)
				}
				if _, err := handle.Write(
					context.Background(), workerrecovery.StageUpload, executionHeartbeatStateName,
					bytes.NewReader(encoded),
				); err != nil {
					t.Fatalf("write mismatched execution Heartbeat State: %v", err)
				}
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			workerID := uuid.New()
			attemptID := uuid.New()
			recovery := newTestRecoveryManager(t, workerID, 15, workerrecovery.Space{
				TotalBytes: 1 << 30, FreeBytes: 1 << 30,
			})
			handle, err := recovery.Open(context.Background(), workerrecovery.Identity{
				AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 15, Fence: 7,
			})
			if err != nil {
				t.Fatalf("Open recovery handle: %v", err)
			}
			testCase.write(t, handle, workerID, attemptID)
			events := []string{}
			control := &recordingControlPlane{events: &events}
			agent, err := New(Config{
				WorkerID: workerID, WorkerEpoch: 15, Recovery: recovery,
				Control: control, Runner: &recordingRunner{events: &events}, HeartbeatInterval: time.Second,
				OutputRoot: t.TempDir(), OutputOwnerUID: uint32(os.Geteuid()),
				InferenceBackendRevision: "sglang@backend-1",
				Finalization:             &recordingFinalizationControl{events: &events},
				PartUploader:             &recordingPartUploader{events: &events},
			})
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			if _, err := agent.RunOnce(context.Background()); err == nil {
				t.Fatal("RunOnce accepted untrusted execution Heartbeat State")
			}
			if control.acquireCalls != 0 || len(events) != 0 {
				t.Fatalf(
					"untrusted state invoked external operations: acquire=%d events=%#v",
					control.acquireCalls, events,
				)
			}
		})
	}
}

func TestRunOnceFailsClosedWhenExecutionHeartbeatSequenceIsExhausted(t *testing.T) {
	workerID := uuid.New()
	attemptID := uuid.New()
	recovery := newTestRecoveryManager(t, workerID, 15, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	handle, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 15, Fence: 7,
	})
	if err != nil {
		t.Fatalf("Open recovery handle: %v", err)
	}
	encoded, err := json.Marshal(executionHeartbeatState{
		SchemaVersion: 1,
		Authority: localAttemptAuthority{
			AttemptID: attemptID, JobID: uuid.New(), WorkerID: workerID,
			WorkerEpoch: 15, LeaseFence: 7,
		},
		Sequence: math.MaxInt64,
	})
	if err != nil {
		t.Fatalf("encode exhausted execution Heartbeat State: %v", err)
	}
	if _, err := handle.Write(
		context.Background(), workerrecovery.StageUpload, executionHeartbeatStateName,
		bytes.NewReader(encoded),
	); err != nil {
		t.Fatalf("write exhausted execution Heartbeat State: %v", err)
	}
	events := []string{}
	control := &recordingControlPlane{events: &events}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 15, Recovery: recovery,
		Control: control, Runner: &recordingRunner{events: &events}, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             &recordingFinalizationControl{events: &events},
		PartUploader:             &recordingPartUploader{events: &events},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := agent.RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "sequence is exhausted") {
		t.Fatalf("RunOnce error = %v", err)
	}
	if control.acquireCalls != 0 || len(events) != 0 {
		t.Fatalf(
			"exhausted sequence invoked external operations: acquire=%d events=%#v",
			control.acquireCalls, events,
		)
	}
}

func TestRunOnceReplaysPendingHeartbeatAtMaximumSequenceBeforeExhaustion(t *testing.T) {
	workerID := uuid.New()
	attemptID := uuid.New()
	jobID := uuid.New()
	recovery := newTestRecoveryManager(t, workerID, 15, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 15, time.Minute)
	handle, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 15,
		Fence: assignment.LeaseFence,
	})
	if err != nil {
		t.Fatalf("Open recovery handle: %v", err)
	}
	control := &recordingControlPlane{heartbeatResults: []workercontrol.HeartbeatResult{{
		Decision: workercontrol.HeartbeatContinue, AttemptID: attemptID, JobID: jobID,
		WorkerID: workerID, WorkerEpoch: 15, LeaseFence: assignment.LeaseFence,
		HeartbeatSequence: math.MaxInt64, ExecutionPhase: workercontrol.ExecutionPhaseGenerating,
		LeaseValidFor: time.Minute,
	}}}
	runner := &recordingRunner{statuses: []runnertransport.Status{{
		State: runnertransport.ExecutionRunning, Sequence: math.MaxInt64,
		BackendStage: "dit", GPUHealth: json.RawMessage(`{"healthy":true}`),
		LocalArtifactState: json.RawMessage(`{"output_count":0}`),
	}}}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 15, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		MonotonicNow:             func() time.Duration { return 10 * time.Second },
		Wait:                     func(context.Context, time.Duration) error { return nil },
		OutputRoot:               t.TempDir(),
		OutputOwnerUID:           uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             &recordingFinalizationControl{},
		PartUploader:             &recordingPartUploader{},
		ArtifactStoreReachable:   func(context.Context) bool { return true },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := persistExecutionHeartbeatSequence(
		context.Background(), handle, assignment, math.MaxInt64,
	); err != nil {
		t.Fatalf("persist maximum execution Heartbeat sequence: %v", err)
	}
	if err := agent.persistPendingHeartbeat(
		context.Background(), handle, assignment,
		workercontrol.HeartbeatObservation{
			Sequence: math.MaxInt64, BackendStage: "dit",
			GPUHealthSummary:       json.RawMessage(`{"healthy":true}`),
			LocalArtifactState:     json.RawMessage(`{"output_count":0}`),
			ArtifactStoreReachable: true,
		},
		workercontrol.ExecutionPhaseGenerating,
		nil,
	); err != nil {
		t.Fatalf("persist maximum pending Heartbeat: %v", err)
	}

	if _, err := agent.RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "sequence is exhausted") {
		t.Fatalf("RunOnce error = %v", err)
	}
	if len(control.heartbeatObservations) != 1 ||
		control.heartbeatObservations[0].Sequence != math.MaxInt64 {
		t.Fatalf("replayed Heartbeats = %#v", control.heartbeatObservations)
	}
	if control.acquireCalls != 0 || control.startCalls != 0 {
		t.Fatalf(
			"maximum pending replay acquired or started: acquire=%d start=%d",
			control.acquireCalls, control.startCalls,
		)
	}
}

func TestRunOnceRejectsExecutionHeartbeatStateThatConflictsWithPendingReplay(t *testing.T) {
	workerID := uuid.New()
	attemptID := uuid.New()
	jobID := uuid.New()
	recovery := newTestRecoveryManager(t, workerID, 15, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 15, time.Minute)
	handle, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 15,
		Fence: assignment.LeaseFence,
	})
	if err != nil {
		t.Fatalf("Open recovery handle: %v", err)
	}
	events := []string{}
	control := &recordingControlPlane{events: &events}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 15, Recovery: recovery,
		Control: control, Runner: &recordingRunner{events: &events}, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             &recordingFinalizationControl{events: &events},
		PartUploader:             &recordingPartUploader{events: &events},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := persistExecutionHeartbeatSequence(context.Background(), handle, assignment, 8); err != nil {
		t.Fatalf("persist execution Heartbeat sequence: %v", err)
	}
	if err := agent.persistPendingHeartbeat(
		context.Background(), handle, assignment,
		workercontrol.HeartbeatObservation{
			Sequence: 7, BackendStage: "dit",
			GPUHealthSummary:       json.RawMessage(`{"healthy":true}`),
			LocalArtifactState:     json.RawMessage(`{"output_count":0}`),
			ArtifactStoreReachable: true,
		},
		workercontrol.ExecutionPhaseGenerating,
		nil,
	); err != nil {
		t.Fatalf("persist conflicting pending Heartbeat: %v", err)
	}

	if _, err := agent.RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "does not match execution Heartbeat State") {
		t.Fatalf("RunOnce error = %v", err)
	}
	if len(control.heartbeatObservations) != 0 || control.acquireCalls != 0 || len(events) != 0 {
		t.Fatalf(
			"conflicting recovery records invoked external operations: heartbeats=%#v acquire=%d events=%#v",
			control.heartbeatObservations, control.acquireCalls, events,
		)
	}
}

func TestRunOnceRenewsOnlyFromRequestStartMonotonicPlusLeaseValidFor(t *testing.T) {
	workerID := uuid.MustParse("31000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("32000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("33000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 9, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 9, 5*time.Second)
	events := []string{}
	control := &recordingControlPlane{
		assignment: &assignment, events: &events,
		startResult: grantedTestStart(assignment),
		heartbeatResults: []workercontrol.HeartbeatResult{{
			Decision:  workercontrol.HeartbeatContinue,
			AttemptID: attemptID, JobID: jobID, WorkerID: workerID,
			WorkerEpoch: 9, LeaseFence: assignment.LeaseFence,
			HeartbeatSequence: 1, ExecutionPhase: workercontrol.ExecutionPhaseGenerating,
			LeaseValidFor: 20 * time.Second,
		}},
	}
	outputRoot := t.TempDir()
	attemptOutputRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptOutputRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	content := []byte("0123456789")
	digest := sha256.Sum256(content)
	outputPath := filepath.Join(attemptOutputRoot, "video.mp4")
	if err := os.WriteFile(outputPath, content, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	runner := &recordingRunner{
		events:        &events,
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{
			{
				State: runnertransport.ExecutionRunning, Sequence: 1, BackendStage: "dit",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"dit":"running"}`),
			},
			{
				State: runnertransport.ExecutionSucceeded, Sequence: 2, BackendStage: "complete",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
			},
		},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted,
			Outputs: []runnertransport.Output{{
				Kind: "VIDEO", Path: outputPath, SizeBytes: 10,
				SHA256: digest, ContentType: "video/mp4",
			}},
		},
	}
	finalization := successfulTestFinalization(assignment, runner.collectResult.Outputs, &events)
	uploader := &recordingPartUploader{events: &events}
	monotonic := 10 * time.Second
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 9, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		MonotonicNow: func() time.Duration { return monotonic },
		Wait: func(context.Context, time.Duration) error {
			monotonic = 16 * time.Second
			return nil
		},
		ArtifactStoreReachable:   func(context.Context) bool { return true },
		OutputRoot:               outputRoot,
		OutputOwnerUID:           uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             finalization,
		PartUploader:             uploader,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil || result.Outcome != OutcomeVisibleCompletion {
		t.Fatalf("RunOnce result = %#v error=%v", result, err)
	}
	if len(control.heartbeatObservations) != 2 {
		t.Fatalf("Heartbeat observations = %#v", control.heartbeatObservations)
	}
	observation := control.heartbeatObservations[0]
	if observation.Sequence != 1 || observation.BackendStage != "dit" ||
		observation.ScratchFreeBytes != 1<<30 || !observation.ArtifactStoreReachable {
		t.Fatalf("Heartbeat observation = %#v", observation)
	}
	finalizing := control.heartbeatObservations[1]
	if finalizing.Sequence != 3 || finalizing.BackendStage != "artifact-finalization" ||
		finalizing.ScratchFreeBytes != 1<<30 || !finalizing.ArtifactStoreReachable {
		t.Fatalf("Finalization Heartbeat observation = %#v", finalizing)
	}
	wantEvents := []string{
		"control.acquire", "runner.prepare", "control.start", "runner.start",
		"runner.status", "control.heartbeat", "runner.status", "runner.collect_outputs",
		"finalization.begin", "control.heartbeat", "finalization.claim", "artifact.put",
		"finalization.complete_upload", "finalization.verify", "finalization.visible_completion",
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("execution events = %#v, want %#v", events, wantEvents)
	}
}

func TestRunOnceAdvancesHeartbeatWhenRunnerSequenceDoesNotChange(t *testing.T) {
	workerID := uuid.MustParse("31010000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("31010000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("31010000-0000-0000-0000-000000000003")
	spaceReads := int64(0)
	recovery, err := workerrecovery.New(workerrecovery.Config{
		Root: t.TempDir(), WorkerID: workerID, WorkerEpoch: 9,
		AttemptQuotaBytes: 1 << 20, MaxEntryBytes: 1 << 18, MaxEntries: 16,
		HighWatermarkBytes: 50, LowWatermarkBytes: 20, CriticalFreeBytes: 10,
		TerminalRetention: time.Minute,
		SpaceProbe: func(string) (workerrecovery.Space, error) {
			spaceReads++
			return workerrecovery.Space{
				TotalBytes: 1 << 30,
				FreeBytes:  (1 << 30) - spaceReads,
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("create recovery Manager: %v", err)
	}
	assignment := validTestAssignment(workerID, attemptID, jobID, 9, time.Minute)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
		heartbeatResults: []workercontrol.HeartbeatResult{
			{
				Decision: workercontrol.HeartbeatContinue, AttemptID: attemptID, JobID: jobID,
				WorkerID: workerID, WorkerEpoch: 9, LeaseFence: assignment.LeaseFence,
				HeartbeatSequence: 1, ExecutionPhase: workercontrol.ExecutionPhaseGenerating,
				LeaseValidFor: time.Minute,
			},
			{
				Decision: workercontrol.HeartbeatContinue, AttemptID: attemptID, JobID: jobID,
				WorkerID: workerID, WorkerEpoch: 9, LeaseFence: assignment.LeaseFence,
				HeartbeatSequence: 2, ExecutionPhase: workercontrol.ExecutionPhaseGenerating,
				LeaseValidFor: time.Minute,
			},
		},
	}
	outputRoot := t.TempDir()
	attemptOutputRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptOutputRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	content := []byte("same-runner-sequence")
	digest := sha256.Sum256(content)
	outputPath := filepath.Join(attemptOutputRoot, "video.mp4")
	if err := os.WriteFile(outputPath, content, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{
			{
				State: runnertransport.ExecutionRunning, Sequence: 1, BackendStage: "mock/prepare",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"output_count":0}`),
			},
			{
				State: runnertransport.ExecutionRunning, Sequence: 1, BackendStage: "mock/prepare",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"output_count":0}`),
			},
			{
				State: runnertransport.ExecutionSucceeded, Sequence: 2, BackendStage: "complete",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
			},
		},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted,
			Outputs: []runnertransport.Output{{
				Kind: "VIDEO", Path: outputPath, SizeBytes: int64(len(content)),
				SHA256: digest, ContentType: "video/mp4",
			}},
		},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 9, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		MonotonicNow:             func() time.Duration { return 10 * time.Second },
		Wait:                     func(context.Context, time.Duration) error { return nil },
		ArtifactStoreReachable:   func(context.Context) bool { return true },
		OutputRoot:               outputRoot,
		OutputOwnerUID:           uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization: successfulTestFinalization(
			assignment, runner.collectResult.Outputs, nil,
		),
		PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil || result.Outcome != OutcomeVisibleCompletion {
		t.Fatalf("RunOnce result = %#v error=%v", result, err)
	}
	if len(control.heartbeatObservations) != 3 {
		t.Fatalf("Heartbeat observations = %#v", control.heartbeatObservations)
	}
	first := control.heartbeatObservations[0]
	second := control.heartbeatObservations[1]
	if first.Sequence != 1 || second.Sequence != 2 ||
		first.BackendStage != second.BackendStage ||
		first.ScratchFreeBytes <= second.ScratchFreeBytes {
		t.Fatalf("same-stage Heartbeats = %#v, %#v", first, second)
	}
}

func TestRunOnceAdvancesHeartbeatAfterReplayingLostResponse(t *testing.T) {
	workerID := uuid.MustParse("31020000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("31020000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("31020000-0000-0000-0000-000000000003")
	spaceReads := int64(0)
	recovery, err := workerrecovery.New(workerrecovery.Config{
		Root: t.TempDir(), WorkerID: workerID, WorkerEpoch: 9,
		AttemptQuotaBytes: 1 << 20, MaxEntryBytes: 1 << 18, MaxEntries: 16,
		HighWatermarkBytes: 50, LowWatermarkBytes: 20, CriticalFreeBytes: 10,
		TerminalRetention: time.Minute,
		SpaceProbe: func(string) (workerrecovery.Space, error) {
			spaceReads++
			return workerrecovery.Space{
				TotalBytes: 1 << 30,
				FreeBytes:  (1 << 30) - spaceReads,
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("create recovery Manager: %v", err)
	}
	assignment := validTestAssignment(workerID, attemptID, jobID, 9, time.Minute)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
		heartbeatErrors: []error{errors.New("response lost after Heartbeat commit")},
		heartbeatResults: []workercontrol.HeartbeatResult{
			{
				Decision: workercontrol.HeartbeatContinue, AttemptID: attemptID, JobID: jobID,
				WorkerID: workerID, WorkerEpoch: 9, LeaseFence: assignment.LeaseFence,
				HeartbeatSequence: 7, ExecutionPhase: workercontrol.ExecutionPhaseGenerating,
				LeaseValidFor: time.Minute,
			},
			{
				Decision: workercontrol.HeartbeatContinue, AttemptID: attemptID, JobID: jobID,
				WorkerID: workerID, WorkerEpoch: 9, LeaseFence: assignment.LeaseFence,
				HeartbeatSequence: 8, ExecutionPhase: workercontrol.ExecutionPhaseGenerating,
				LeaseValidFor: time.Minute,
			},
		},
	}
	outputRoot := t.TempDir()
	attemptOutputRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptOutputRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	content := []byte("replayed-heartbeat")
	digest := sha256.Sum256(content)
	outputPath := filepath.Join(attemptOutputRoot, "video.mp4")
	if err := os.WriteFile(outputPath, content, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{
			{
				State: runnertransport.ExecutionRunning, Sequence: 7, BackendStage: "mock/prepare",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"output_count":0}`),
			},
			{
				State: runnertransport.ExecutionRunning, Sequence: 7, BackendStage: "mock/prepare",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"output_count":0}`),
			},
			{
				State: runnertransport.ExecutionSucceeded, Sequence: 8, BackendStage: "complete",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
			},
		},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted,
			Outputs: []runnertransport.Output{{
				Kind: "VIDEO", Path: outputPath, SizeBytes: int64(len(content)),
				SHA256: digest, ContentType: "video/mp4",
			}},
		},
	}
	newAgent := func() *Agent {
		agent, newErr := New(Config{
			WorkerID: workerID, WorkerEpoch: 9, Recovery: recovery,
			Control: control, Runner: runner, HeartbeatInterval: time.Second,
			MonotonicNow:             func() time.Duration { return 10 * time.Second },
			Wait:                     func(context.Context, time.Duration) error { return nil },
			ArtifactStoreReachable:   func(context.Context) bool { return true },
			OutputRoot:               outputRoot,
			OutputOwnerUID:           uint32(os.Geteuid()),
			InferenceBackendRevision: "sglang@backend-1",
			Finalization: successfulTestFinalization(
				assignment, runner.collectResult.Outputs, nil,
			),
			PartUploader: &recordingPartUploader{},
		})
		if newErr != nil {
			t.Fatalf("New: %v", newErr)
		}
		return agent
	}

	if _, err := newAgent().RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "response lost after Heartbeat commit") {
		t.Fatalf("first RunOnce error = %v", err)
	}
	result, err := newAgent().RunOnce(context.Background())
	if err != nil || result.Outcome != OutcomeVisibleCompletion {
		t.Fatalf("resumed RunOnce result = %#v error=%v", result, err)
	}
	if control.acquireCalls != 1 || control.startCalls != 1 {
		t.Fatalf(
			"resumed running Attempt acquired or started again: Acquire=%d Start=%d",
			control.acquireCalls, control.startCalls,
		)
	}
	gotSequences := make([]int64, len(control.heartbeatObservations))
	for index, observation := range control.heartbeatObservations {
		gotSequences[index] = observation.Sequence
	}
	if !reflect.DeepEqual(gotSequences, []int64{7, 7, 8, 9}) {
		t.Fatalf("Heartbeat sequences = %#v", gotSequences)
	}
}

func TestRunOnceAdvancesHeartbeatAfterConfirmedResponseAndProcessLoss(t *testing.T) {
	workerID := uuid.MustParse("31030000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("31030000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("31030000-0000-0000-0000-000000000003")
	spaceReads := int64(0)
	recovery, err := workerrecovery.New(workerrecovery.Config{
		Root: t.TempDir(), WorkerID: workerID, WorkerEpoch: 9,
		AttemptQuotaBytes: 1 << 20, MaxEntryBytes: 1 << 18, MaxEntries: 16,
		HighWatermarkBytes: 50, LowWatermarkBytes: 20, CriticalFreeBytes: 10,
		TerminalRetention: time.Minute,
		SpaceProbe: func(string) (workerrecovery.Space, error) {
			spaceReads++
			return workerrecovery.Space{
				TotalBytes: 1 << 30,
				FreeBytes:  (1 << 30) - spaceReads,
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("create recovery Manager: %v", err)
	}
	assignment := validTestAssignment(workerID, attemptID, jobID, 9, time.Minute)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
		heartbeatResults: []workercontrol.HeartbeatResult{
			{
				Decision: workercontrol.HeartbeatContinue, AttemptID: attemptID, JobID: jobID,
				WorkerID: workerID, WorkerEpoch: 9, LeaseFence: assignment.LeaseFence,
				HeartbeatSequence: 7, ExecutionPhase: workercontrol.ExecutionPhaseGenerating,
				LeaseValidFor: time.Minute,
			},
			{
				Decision: workercontrol.HeartbeatContinue, AttemptID: attemptID, JobID: jobID,
				WorkerID: workerID, WorkerEpoch: 9, LeaseFence: assignment.LeaseFence,
				HeartbeatSequence: 8, ExecutionPhase: workercontrol.ExecutionPhaseGenerating,
				LeaseValidFor: time.Minute,
			},
		},
	}
	outputRoot := t.TempDir()
	attemptOutputRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptOutputRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	content := []byte("confirmed-heartbeat")
	digest := sha256.Sum256(content)
	outputPath := filepath.Join(attemptOutputRoot, "video.mp4")
	if err := os.WriteFile(outputPath, content, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{
			{
				State: runnertransport.ExecutionRunning, Sequence: 7, BackendStage: "mock/prepare",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"output_count":0}`),
			},
			{
				State: runnertransport.ExecutionRunning, Sequence: 7, BackendStage: "mock/prepare",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"output_count":0}`),
			},
			{
				State: runnertransport.ExecutionSucceeded, Sequence: 8, BackendStage: "complete",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
			},
		},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted,
			Outputs: []runnertransport.Output{{
				Kind: "VIDEO", Path: outputPath, SizeBytes: int64(len(content)),
				SHA256: digest, ContentType: "video/mp4",
			}},
		},
	}
	processLost := errors.New("simulated Worker Agent process loss")
	newAgent := func(wait func(context.Context, time.Duration) error) *Agent {
		agent, newErr := New(Config{
			WorkerID: workerID, WorkerEpoch: 9, Recovery: recovery,
			Control: control, Runner: runner, HeartbeatInterval: time.Second,
			MonotonicNow:             func() time.Duration { return 10 * time.Second },
			Wait:                     wait,
			ArtifactStoreReachable:   func(context.Context) bool { return true },
			OutputRoot:               outputRoot,
			OutputOwnerUID:           uint32(os.Geteuid()),
			InferenceBackendRevision: "sglang@backend-1",
			Finalization: successfulTestFinalization(
				assignment, runner.collectResult.Outputs, nil,
			),
			PartUploader: &recordingPartUploader{},
		})
		if newErr != nil {
			t.Fatalf("New: %v", newErr)
		}
		return agent
	}

	if _, err := newAgent(func(context.Context, time.Duration) error {
		return processLost
	}).RunOnce(context.Background()); !errors.Is(err, processLost) {
		t.Fatalf("first RunOnce error = %v", err)
	}
	result, err := newAgent(func(context.Context, time.Duration) error {
		return nil
	}).RunOnce(context.Background())
	if err != nil || result.Outcome != OutcomeVisibleCompletion {
		t.Fatalf("resumed RunOnce result = %#v error=%v", result, err)
	}
	if control.acquireCalls != 2 || control.startCalls != 2 {
		t.Fatalf(
			"same-authority recovery calls: Acquire=%d Start=%d",
			control.acquireCalls, control.startCalls,
		)
	}
	gotSequences := make([]int64, len(control.heartbeatObservations))
	for index, observation := range control.heartbeatObservations {
		gotSequences[index] = observation.Sequence
	}
	if !reflect.DeepEqual(gotSequences, []int64{7, 8, 9}) {
		t.Fatalf("Heartbeat sequences = %#v", gotSequences)
	}
}

func TestRunOnceDoesNotReuseHeartbeatSequenceReservedBeforePendingWrite(t *testing.T) {
	workerID := uuid.MustParse("31040000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("31040000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("31040000-0000-0000-0000-000000000003")
	recoveryRoot := t.TempDir()
	newRecovery := func(maxEntries int) *workerrecovery.Manager {
		recovery, err := workerrecovery.New(workerrecovery.Config{
			Root: recoveryRoot, WorkerID: workerID, WorkerEpoch: 9,
			AttemptQuotaBytes: 1 << 20, MaxEntryBytes: 1 << 18, MaxEntries: maxEntries,
			HighWatermarkBytes: 50, LowWatermarkBytes: 20, CriticalFreeBytes: 10,
			TerminalRetention: time.Minute,
			SpaceProbe: func(string) (workerrecovery.Space, error) {
				return workerrecovery.Space{TotalBytes: 1 << 30, FreeBytes: 1 << 30}, nil
			},
		})
		if err != nil {
			t.Fatalf("create recovery Manager: %v", err)
		}
		return recovery
	}
	assignment := validTestAssignment(workerID, attemptID, jobID, 9, time.Minute)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
		heartbeatResults: []workercontrol.HeartbeatResult{{
			Decision: workercontrol.HeartbeatContinue, AttemptID: attemptID, JobID: jobID,
			WorkerID: workerID, WorkerEpoch: 9, LeaseFence: assignment.LeaseFence,
			HeartbeatSequence: 8, ExecutionPhase: workercontrol.ExecutionPhaseGenerating,
			LeaseValidFor: time.Minute,
		}},
	}
	outputRoot := t.TempDir()
	attemptOutputRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptOutputRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	content := []byte("reserved-before-pending")
	digest := sha256.Sum256(content)
	outputPath := filepath.Join(attemptOutputRoot, "video.mp4")
	if err := os.WriteFile(outputPath, content, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{
			{
				State: runnertransport.ExecutionRunning, Sequence: 7, BackendStage: "mock/prepare",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"output_count":0}`),
			},
			{
				State: runnertransport.ExecutionRunning, Sequence: 7, BackendStage: "mock/prepare",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"output_count":0}`),
			},
			{
				State: runnertransport.ExecutionSucceeded, Sequence: 8, BackendStage: "complete",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
			},
		},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted,
			Outputs: []runnertransport.Output{{
				Kind: "VIDEO", Path: outputPath, SizeBytes: int64(len(content)),
				SHA256: digest, ContentType: "video/mp4",
			}},
		},
	}
	newAgent := func(recovery *workerrecovery.Manager) *Agent {
		agent, err := New(Config{
			WorkerID: workerID, WorkerEpoch: 9, Recovery: recovery,
			Control: control, Runner: runner, HeartbeatInterval: time.Second,
			MonotonicNow:             func() time.Duration { return 10 * time.Second },
			Wait:                     func(context.Context, time.Duration) error { return nil },
			ArtifactStoreReachable:   func(context.Context) bool { return true },
			OutputRoot:               outputRoot,
			OutputOwnerUID:           uint32(os.Geteuid()),
			InferenceBackendRevision: "sglang@backend-1",
			Finalization: successfulTestFinalization(
				assignment, runner.collectResult.Outputs, nil,
			),
			PartUploader: &recordingPartUploader{},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return agent
	}

	if _, err := newAgent(newRecovery(1)).RunOnce(context.Background()); !errors.Is(err, workerrecovery.ErrQuotaExceeded) {
		t.Fatalf("first RunOnce error = %v", err)
	}
	if len(control.heartbeatObservations) != 0 {
		t.Fatalf("Heartbeat sent before pending write = %#v", control.heartbeatObservations)
	}
	result, err := newAgent(newRecovery(16)).RunOnce(context.Background())
	if err != nil || result.Outcome != OutcomeVisibleCompletion {
		t.Fatalf("resumed RunOnce result = %#v error=%v", result, err)
	}
	gotSequences := make([]int64, len(control.heartbeatObservations))
	for index, observation := range control.heartbeatObservations {
		gotSequences[index] = observation.Sequence
	}
	if !reflect.DeepEqual(gotSequences, []int64{8, 9}) {
		t.Fatalf("Heartbeat sequences = %#v", gotSequences)
	}
}

func TestRunOnceFailsClosedWhenMonotonicLeaseDeadlineElapsesBeforeStart(t *testing.T) {
	workerID := uuid.MustParse("41000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("42000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("43000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 11, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 11, 5*time.Second)
	events := []string{}
	control := &recordingControlPlane{
		assignment: &assignment, events: &events, startResult: grantedTestStart(assignment),
	}
	runner := &recordingRunner{
		events:        &events,
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
	}
	clockReads := 0
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 11, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		MonotonicNow: func() time.Duration {
			clockReads++
			if clockReads < 3 {
				return 10 * time.Second
			}
			return 16 * time.Second
		},
		OutputRoot: t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeLeaseDeadlineElapsed || result.AttemptID != attemptID {
		t.Fatalf("RunOnce result = %#v", result)
	}
	wantEvents := []string{"control.acquire", "runner.prepare", "runner.cancel", "runner.status"}
	if !reflect.DeepEqual(events, wantEvents) || control.startCalls != 0 ||
		runner.cancelReason != runnertransport.CancelLeaseDeadline {
		t.Fatalf("deadline calls = %#v control start=%d cancel reason=%s", events, control.startCalls, runner.cancelReason)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 11, Fence: assignment.LeaseFence,
	}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
		t.Fatalf("deadline-expired Local Recovery State reopened: %v", err)
	}
}

func TestRunOnceBoundsBlockedRunnerStatusByLeaseDeadline(t *testing.T) {
	workerID := uuid.MustParse("44000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("44000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("44000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 11, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 11, 30*time.Millisecond)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
	}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statusHook: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 11, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	started := time.Now()
	result, err := agent.RunOnce(ctx)
	if err != nil || result.Outcome != OutcomeLeaseDeadlineElapsed {
		t.Fatalf("RunOnce = %#v error=%v", result, err)
	}
	if elapsed := time.Since(started); elapsed >= 200*time.Millisecond {
		t.Fatalf("blocked Status stopped after %s, want Lease-bound stop", elapsed)
	}
	if runner.cancelReason != runnertransport.CancelLeaseDeadline {
		t.Fatalf("cancel reason = %s, want Lease deadline", runner.cancelReason)
	}
}

func TestRunOnceBoundsBlockedArtifactStoreProbeByLeaseDeadline(t *testing.T) {
	workerID := uuid.MustParse("44010000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("44010000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("44010000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 11, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 11, 100*time.Millisecond)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
		heartbeatHook: func(ctx context.Context, _ workercontrol.HeartbeatObservation) error {
			return ctx.Err()
		},
	}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionRunning, Sequence: 1, BackendStage: "dit",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"dit":"running"}`),
		}},
	}
	probeCalls := 0
	probeDeadline := make(chan time.Duration, 1)
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 11, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		ArtifactStoreReachable: func(ctx context.Context) bool {
			probeCalls++
			if probeCalls == 1 {
				return true
			}
			deadline, ok := ctx.Deadline()
			if !ok {
				probeDeadline <- -1
			} else {
				probeDeadline <- time.Until(deadline)
			}
			<-ctx.Done()
			return false
		},
		OutputRoot: t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	result, err := agent.RunOnce(ctx)
	if err != nil || result.Outcome != OutcomeLeaseDeadlineElapsed {
		t.Fatalf("RunOnce = %#v error=%v", result, err)
	}
	if ctx.Err() != nil {
		t.Fatalf("blocked Artifact-store probe exhausted outer watchdog: %v", ctx.Err())
	}
	select {
	case remaining := <-probeDeadline:
		if remaining <= 0 || remaining > assignment.LeaseValidFor {
			t.Fatalf("Artifact-store probe deadline = %s, want Lease budget", remaining)
		}
	default:
		t.Fatal("Artifact-store probe did not observe a Lease deadline")
	}
	if runner.cancelReason != runnertransport.CancelLeaseDeadline {
		t.Fatalf("cancel reason = %s, want Lease deadline", runner.cancelReason)
	}
	if probeCalls != 2 {
		t.Fatalf("Artifact-store probe calls = %d, want pre-Acquire and running probes", probeCalls)
	}
}

func TestRunOnceBoundsBlockedRunnerFailureReportByLeaseDeadline(t *testing.T) {
	workerID := uuid.MustParse("44100000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("44100000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("44100000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 11, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 11, 30*time.Millisecond)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
		failHook: func(ctx context.Context, _ workercontrol.FailureObservation) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionFailed, Sequence: 1, BackendStage: "dit",
			Failure: &runnertransport.Failure{
				FailureClass: "TRANSIENT_BACKEND", FailureFingerprint: "dit/timeout",
				ErrorSummary: "backend timed out", BackendStage: "dit",
				InferenceBackendRevision: "sglang@backend-1", RetryRecommended: true,
				WorkerReusable: true,
			},
		}},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 11, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	started := time.Now()
	result, err := agent.RunOnce(ctx)
	if err != nil || result.Outcome != OutcomeLeaseDeadlineElapsed {
		t.Fatalf("RunOnce = %#v error=%v", result, err)
	}
	if elapsed := time.Since(started); elapsed >= 200*time.Millisecond {
		t.Fatalf("blocked Fail stopped after %s, want Lease-bound stop", elapsed)
	}
	if runner.cancelReason != runnertransport.CancelLeaseDeadline {
		t.Fatalf("cancel reason = %s, want Lease deadline", runner.cancelReason)
	}
}

func TestRunOnceReplaysFinalizationLeaseDeadlineAfterCancelResponseIsLost(t *testing.T) {
	workerID := uuid.MustParse("45000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("45000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("45000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 11, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 11, 500*time.Millisecond)
	events := []string{}
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment), events: &events,
		heartbeatHook: func(ctx context.Context, observation workercontrol.HeartbeatObservation) error {
			if observation.BackendStage == "artifact-finalization" {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		},
	}
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := []byte("blocked-finalization-heartbeat")
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	outputs := []runnertransport.Output{{
		Kind: "VIDEO", Path: outputPath, SizeBytes: int64(len(payload)),
		SHA256: digest, ContentType: "video/mp4",
	}}
	runner := &recordingRunner{
		events:        &events,
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelErrors:  []error{errors.New("lost finalization Lease-deadline Cancel response")},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted, Outputs: outputs,
		},
	}
	finalization := successfulTestFinalization(assignment, outputs, &events)
	newAgent := func() *Agent {
		agent, err := New(Config{
			WorkerID: workerID, WorkerEpoch: 11, Recovery: recovery,
			Control: control, Runner: runner, HeartbeatInterval: time.Second,
			OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
			InferenceBackendRevision: "sglang@backend-1",
			Finalization:             finalization, PartUploader: &recordingPartUploader{},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return agent
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if result, err := newAgent().RunOnce(ctx); err == nil ||
		!strings.Contains(err.Error(), "lost finalization Lease-deadline Cancel response") {
		t.Fatalf("first RunOnce = %#v error=%v events=%#v", result, err, events)
	}
	if runner.cancelReason != runnertransport.CancelLeaseDeadline || len(finalization.claimIDs) != 0 ||
		len(control.heartbeatObservations) != 1 {
		t.Fatalf(
			"cancel=%s Artifact claims=%d Heartbeats=%d",
			runner.cancelReason,
			len(finalization.claimIDs),
			len(control.heartbeatObservations),
		)
	}
	if _, statErr := os.Lstat(outputPath); statErr != nil {
		t.Fatalf("failed Cancel removed Finalization output: %v", statErr)
	}
	if _, openErr := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 11, Fence: assignment.LeaseFence,
	}); openErr != nil {
		t.Fatalf("failed Cancel removed recoverable Local Recovery State: %v", openErr)
	}
	beginCalls := finalization.beginCalls
	control.assignment = nil
	result, err := newAgent().RunOnce(context.Background())
	if err != nil || result.Outcome != OutcomeLeaseDeadlineElapsed {
		t.Fatalf("replayed RunOnce = %#v error=%v events=%#v", result, err, events)
	}
	if control.acquireCalls != 1 || finalization.beginCalls != beginCalls ||
		len(control.heartbeatObservations) != 1 {
		t.Fatalf(
			"replay acquired=%d Begin=%d/%d Heartbeats=%d",
			control.acquireCalls,
			finalization.beginCalls,
			beginCalls,
			len(control.heartbeatObservations),
		)
	}
	if _, statErr := os.Lstat(outputPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("Lease-expired Finalization output remains: %v", statErr)
	}
	if _, openErr := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 11, Fence: assignment.LeaseFence,
	}); !errors.Is(openErr, workerrecovery.ErrStateTerminal) {
		t.Fatalf("replayed termination did not terminalize Local Recovery State: %v", openErr)
	}
}

func TestRunOnceStopsOutputHashWhenOperationContextIsCanceled(t *testing.T) {
	workerID := uuid.MustParse("30170000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("30170000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("30170000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 16, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 16, time.Minute)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
	}
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := bytes.Repeat([]byte("context-aware-output-hash"), 32*1024)
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	outputs := []runnertransport.Output{{
		Kind: "VIDEO", Ordinal: 0, Path: outputPath, SizeBytes: int64(len(payload)),
		SHA256: digest, ContentType: "video/mp4",
	}}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted, Outputs: outputs,
		},
	}
	finalization := successfulTestFinalization(assignment, outputs, nil)
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 16, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             finalization, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	canceled := false
	agent.afterOutputHashChunk = func(context.Context, int64) {
		if !canceled {
			canceled = true
			cancel()
		}
	}

	if _, err := agent.RunOnce(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnce error = %v, want context canceled during output hash", err)
	}
	if len(finalization.completionIDs) != 0 {
		t.Fatalf("canceled hash published Visible Completion: %#v", finalization.completionIDs)
	}
	if content, err := os.ReadFile(outputPath); err != nil || !bytes.Equal(content, payload) {
		t.Fatalf("canceled hash removed or changed output: bytes=%d error=%v", len(content), err)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 16, Fence: assignment.LeaseFence,
	}); err != nil {
		t.Fatalf("canceled hash removed Local Recovery State: %v", err)
	}
}

func TestRunOnceScalesTerminalCleanupOutputHashBudgetAndResumesIt(t *testing.T) {
	workerID := uuid.MustParse("30171000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("30171000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("30171000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 16, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 16, time.Minute)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
	}
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := bytes.Repeat([]byte("bounded-terminal-cleanup-hash"), 32*1024)
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	outputs := []runnertransport.Output{{
		Kind: "VIDEO", Ordinal: 0, Path: outputPath, SizeBytes: int64(len(payload)),
		SHA256: digest, ContentType: "video/mp4",
	}}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted, Outputs: outputs,
		},
	}
	finalization := successfulTestFinalization(assignment, outputs, nil)
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 16, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		OutputCleanupMinBytesPerSecond: int64(len(payload)),
		InferenceBackendRevision:       "sglang@backend-1",
		Finalization:                   finalization, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	hashOperation := 0
	agent.cleanupTimeout = 20 * time.Millisecond
	agent.beforeOutputHash = func(context.Context) { hashOperation++ }
	agent.afterOutputHashChunk = func(ctx context.Context, _ int64) {
		if hashOperation == 2 {
			<-ctx.Done()
		}
	}

	started := time.Now()
	result, err := agent.RunOnce(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || result.Outcome != OutcomeVisibleCompletion {
		t.Fatalf("first RunOnce = %#v error=%v, want committed bounded cleanup", result, err)
	}
	if elapsed := time.Since(started); elapsed < time.Second {
		t.Fatalf("terminal cleanup stopped after %s, want size-derived hash budget", elapsed)
	}
	if len(finalization.completionIDs) != 1 {
		t.Fatalf("Visible Completion calls = %#v", finalization.completionIDs)
	}
	quarantinedOutput := filepath.Join(testAttemptQuarantinePath(t, recovery, attemptID), "video.mp4")
	if content, err := os.ReadFile(quarantinedOutput); err != nil || !bytes.Equal(content, payload) {
		t.Fatalf("timed-out cleanup removed or changed quarantined output: bytes=%d error=%v", len(content), err)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 16, Fence: assignment.LeaseFence,
	}); err != nil {
		t.Fatalf("timed-out cleanup removed Local Recovery State: %v", err)
	}

	agent.beforeOutputHash = nil
	agent.afterOutputHashChunk = nil
	agent.cleanupTimeout = time.Second
	result, err = agent.RunOnce(context.Background())
	if err != nil || result.Outcome != OutcomeVisibleCompletion {
		t.Fatalf("resumed cleanup RunOnce = %#v error=%v", result, err)
	}
	if len(finalization.completionIDs) != 1 {
		t.Fatalf("cleanup replay repeated Visible Completion: %#v", finalization.completionIDs)
	}
}

func TestRunOnceBoundsValidationFailureReportByRenewedLeaseDeadline(t *testing.T) {
	workerID := uuid.MustParse("45100000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("45100000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("45100000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 11, workerrecovery.Space{
		TotalBytes: 1 << 30, FreeBytes: 1 << 30,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 11, 2*time.Second)
	type failDeadlineObservation struct {
		started   time.Time
		remaining time.Duration
	}
	failDeadline := make(chan failDeadlineObservation, 1)
	control := &recordingControlPlane{
		assignment: &assignment, startResult: grantedTestStart(assignment),
		heartbeatResults: []workercontrol.HeartbeatResult{{
			Decision:  workercontrol.HeartbeatContinue,
			AttemptID: attemptID, JobID: jobID, WorkerID: workerID,
			WorkerEpoch: 11, LeaseFence: assignment.LeaseFence,
			HeartbeatSequence: 2, ExecutionPhase: workercontrol.ExecutionPhaseFinalizing,
			LeaseValidFor: 500 * time.Millisecond,
		}},
		failHook: func(ctx context.Context, _ workercontrol.FailureObservation) error {
			deadline, ok := ctx.Deadline()
			if !ok {
				return errors.New("validation failure report has no Lease deadline")
			}
			failDeadline <- failDeadlineObservation{
				started: time.Now(), remaining: time.Until(deadline),
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := []byte("validation-failure-lease-bound")
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	outputs := []runnertransport.Output{{
		Kind: "VIDEO", Path: outputPath, SizeBytes: int64(len(payload)),
		SHA256: digest, ContentType: "video/mp4",
	}}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionSucceeded, Sequence: 1, BackendStage: "complete",
			GPUHealth: json.RawMessage(`{"healthy":true}`), LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
		}},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted, Outputs: outputs,
		},
	}
	finalization := successfulTestFinalization(assignment, outputs, nil)
	finalization.verificationDecisions = []workercontrol.ArtifactVerificationDecision{
		workercontrol.ArtifactValidationFailed,
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 11, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		OutputRoot: outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
		InferenceBackendRevision: "sglang@backend-1",
		Finalization:             finalization, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := agent.RunOnce(ctx)
	if err != nil || result.Outcome != OutcomeLeaseDeadlineElapsed {
		t.Fatalf("RunOnce = %#v error=%v", result, err)
	}
	var observation failDeadlineObservation
	select {
	case observation = <-failDeadline:
	default:
		t.Fatal("validation failure report did not observe a Lease deadline")
	}
	if observation.remaining <= 0 || observation.remaining > 500*time.Millisecond {
		t.Fatalf("validation Fail context remaining = %s, want renewed Lease budget", observation.remaining)
	}
	if elapsed := time.Since(observation.started); elapsed >= 1500*time.Millisecond {
		t.Fatalf("blocked validation Fail stopped after %s, want renewed Lease-bound stop", elapsed)
	}
	if runner.cancelReason != runnertransport.CancelLeaseDeadline {
		t.Fatalf("cancel reason = %s, want Lease deadline", runner.cancelReason)
	}
	if _, statErr := os.Lstat(outputPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("Lease-expired validation output remains: %v", statErr)
	}
}

func TestRunOnceReportsRunnerFailureBeforeTerminalRecoveryCleanup(t *testing.T) {
	workerID := uuid.MustParse("51000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("52000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("53000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 13, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 13, time.Minute)
	events := []string{}
	control := &recordingControlPlane{
		assignment: &assignment, events: &events,
		startResult: grantedTestStart(assignment),
		failResult: workercontrol.RetryDecision{
			Disposition:  workercontrol.RetryDispositionRetryWait,
			FailureClass: "TRANSIENT_BACKEND", AttemptID: attemptID, JobID: jobID,
			AttemptState: workercontrol.FailedAttempt, JobFence: 4, JobVersion: 5,
			DecidedAt: time.Date(2026, 8, 25, 7, 3, 0, 0, time.UTC),
		},
	}
	runner := &recordingRunner{
		events:        &events,
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionFailed, Sequence: 3, BackendStage: "dit",
			GPUHealth:          json.RawMessage(`{"healthy":true}`),
			LocalArtifactState: json.RawMessage(`{"dit":"failed"}`),
			Failure: &runnertransport.Failure{
				FailureClass: "TRANSIENT_BACKEND", FailureFingerprint: "dit/timeout",
				ErrorSummary: "backend timed out", BackendStage: "dit",
				GPUUUIDs: []string{"GPU-1"}, InferenceBackendRevision: "sglang@abc",
				RetryRecommended: true, WorkerReusable: true,
			},
		}},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 13, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		MonotonicNow: func() time.Duration { return 10 * time.Second },
		OutputRoot:   t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeRetryScheduled || result.AttemptID != attemptID {
		t.Fatalf("RunOnce result = %#v", result)
	}
	if control.failureObservation.FailureClass != "TRANSIENT_BACKEND" ||
		control.failureObservation.FailureFingerprint != "dit/timeout" ||
		!control.failureObservation.RetryRecommended || !control.failureObservation.WorkerReusable {
		t.Fatalf("reported FailureObservation = %#v", control.failureObservation)
	}
	wantEvents := []string{
		"control.acquire", "runner.prepare", "control.start", "runner.start",
		"runner.status", "control.fail",
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("failure events = %#v, want %#v", events, wantEvents)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 13, Fence: assignment.LeaseFence,
	}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
		t.Fatalf("failed Attempt Local Recovery State reopened: %v", err)
	}
}

func TestRunOnceReplaysCommittedFailAfterTheResponseIsLost(t *testing.T) {
	workerID := uuid.MustParse("54000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("55000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("56000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 13, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 13, time.Minute)
	control := &recordingControlPlane{
		assignment:  &assignment,
		startResult: grantedTestStart(assignment),
		failErrors:  []error{errors.New("response lost after Fail commit")},
		failResult: workercontrol.RetryDecision{
			Disposition:  workercontrol.RetryDispositionRetryWait,
			FailureClass: "TRANSIENT_BACKEND", AttemptID: attemptID, JobID: jobID,
			AttemptState: workercontrol.FailedAttempt, JobFence: 4, JobVersion: 5,
			DecidedAt: time.Date(2026, 8, 25, 7, 4, 0, 0, time.UTC),
		},
	}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionFailed, Sequence: 3, BackendStage: "dit",
			GPUHealth:          json.RawMessage(`{"healthy":true}`),
			LocalArtifactState: json.RawMessage(`{"dit":"failed"}`),
			Failure: &runnertransport.Failure{
				FailureClass: "TRANSIENT_BACKEND", FailureFingerprint: "dit/timeout",
				ErrorSummary: "backend timed out", BackendStage: "dit",
				GPUUUIDs: []string{"GPU-1"}, InferenceBackendRevision: "sglang@abc",
				RetryRecommended: true, WorkerReusable: true,
			},
		}},
	}
	newAgent := func() *Agent {
		agent, err := New(Config{
			WorkerID: workerID, WorkerEpoch: 13, Recovery: recovery,
			Control: control, Runner: runner, HeartbeatInterval: time.Second,
			MonotonicNow: func() time.Duration { return 10 * time.Second },
			OutputRoot:   t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
			Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return agent
	}

	if _, err := newAgent().RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "response lost after Fail commit") {
		t.Fatalf("first RunOnce error = %v", err)
	}
	control.assignment = nil
	result, err := newAgent().RunOnce(context.Background())
	if err != nil {
		t.Fatalf("replayed RunOnce: %v", err)
	}
	if result.Outcome != OutcomeRetryScheduled || result.AttemptID != attemptID ||
		control.acquireCalls != 1 {
		t.Fatalf("replayed result = %#v Acquire calls=%d", result, control.acquireCalls)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 13, Fence: assignment.LeaseFence,
	}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
		t.Fatalf("replayed failed Attempt remains active: %v", err)
	}
}

func TestRunOnceReplaysHeartbeatWhenAStopResponseIsLost(t *testing.T) {
	workerID := uuid.MustParse("57000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("58000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("59000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 14, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 14, time.Minute)
	control := &recordingControlPlane{
		assignment:      &assignment,
		startResult:     grantedTestStart(assignment),
		heartbeatErrors: []error{errors.New("response lost after Heartbeat commit")},
		heartbeatResults: []workercontrol.HeartbeatResult{{
			Decision: workercontrol.HeartbeatStop, StopReason: workercontrol.StopInvalidAuthority,
		}},
	}
	progress := 0.5
	remaining := int64(30)
	outputRoot := t.TempDir()
	attemptRoot := filepath.Join(outputRoot, attemptID.String())
	if err := os.Mkdir(attemptRoot, 0o700); err != nil {
		t.Fatalf("create Attempt output root: %v", err)
	}
	payload := []byte("completion-lost-to-replayed-stop")
	digest := sha256.Sum256(payload)
	outputPath := filepath.Join(attemptRoot, "video.mp4")
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		t.Fatalf("write runner output: %v", err)
	}
	outputs := []runnertransport.Output{{
		Kind: "VIDEO", Path: outputPath, SizeBytes: int64(len(payload)),
		SHA256: digest, ContentType: "video/mp4",
	}}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{
			{
				State: runnertransport.ExecutionRunning, Sequence: 7, BackendStage: "dit",
				BackendStageProgress: &progress, EstimatedRemainingSeconds: &remaining,
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"dit":"running"}`),
			},
			{
				State: runnertransport.ExecutionSucceeded, Sequence: 8, BackendStage: "complete",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"outputs":"ready"}`),
			},
		},
		collectResult: runnertransport.CollectOutputsResult{
			Decision: runnertransport.CommandAccepted, Outputs: outputs,
		},
	}
	newAgent := func() *Agent {
		agent, err := New(Config{
			WorkerID: workerID, WorkerEpoch: 14, Recovery: recovery,
			Control: control, Runner: runner, HeartbeatInterval: time.Second,
			MonotonicNow:           func() time.Duration { return 10 * time.Second },
			ArtifactStoreReachable: func(context.Context) bool { return true },
			OutputRoot:             outputRoot, OutputOwnerUID: uint32(os.Geteuid()),
			InferenceBackendRevision: "sglang@backend-1",
			Finalization:             &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return agent
	}

	if _, err := newAgent().RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "response lost after Heartbeat commit") {
		t.Fatalf("first RunOnce error = %v", err)
	}
	control.assignment = nil
	result, err := newAgent().RunOnce(context.Background())
	if err != nil {
		t.Fatalf("replayed RunOnce: %v", err)
	}
	if result.Outcome != OutcomeControlPlaneStopped ||
		result.StopReason != workercontrol.StopInvalidAuthority || control.acquireCalls != 1 {
		t.Fatalf("replayed result = %#v Acquire calls=%d", result, control.acquireCalls)
	}
	if runner.cancelReason != runnertransport.CancelControlPlaneStop ||
		len(control.heartbeatObservations) != 2 ||
		control.heartbeatObservations[1].Sequence != 7 ||
		control.heartbeatObservations[1].BackendStageProgress == nil ||
		*control.heartbeatObservations[1].BackendStageProgress != 0.5 {
		t.Fatalf("replayed Heartbeat = %#v cancel=%s", control.heartbeatObservations, runner.cancelReason)
	}
	if _, statErr := os.Lstat(outputPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("successful output remains after replayed Heartbeat STOP: %v", statErr)
	}
	if _, openErr := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 14, Fence: assignment.LeaseFence,
	}); !errors.Is(openErr, workerrecovery.ErrStateTerminal) {
		t.Fatalf("replayed Heartbeat STOP did not terminalize Local Recovery State: %v", openErr)
	}
}

func TestRunOnceContinuesSameRunningAttemptWhenHeartbeatContinueResponseIsLost(t *testing.T) {
	workerID := uuid.MustParse("59100000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("59100000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("59100000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 14, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 14, time.Minute)
	events := []string{}
	control := &recordingControlPlane{
		assignment:      &assignment,
		events:          &events,
		startResult:     grantedTestStart(assignment),
		heartbeatErrors: []error{errors.New("response lost after Heartbeat CONTINUE commit")},
		heartbeatResults: []workercontrol.HeartbeatResult{
			{
				Decision:  workercontrol.HeartbeatContinue,
				AttemptID: attemptID, JobID: jobID, WorkerID: workerID,
				WorkerEpoch: 14, LeaseFence: assignment.LeaseFence,
				HeartbeatSequence: 7, ExecutionPhase: workercontrol.ExecutionPhaseGenerating,
				LeaseValidFor: time.Minute,
			},
			{
				Decision: workercontrol.HeartbeatStop, StopReason: workercontrol.StopInvalidAuthority,
			},
		},
	}
	runner := &recordingRunner{
		events:        &events,
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{
			{
				State: runnertransport.ExecutionRunning, Sequence: 7, BackendStage: "dit",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"dit":"running"}`),
			},
			{
				State: runnertransport.ExecutionRunning, Sequence: 8, BackendStage: "dit",
				GPUHealth:          json.RawMessage(`{"healthy":true}`),
				LocalArtifactState: json.RawMessage(`{"dit":"running"}`),
			},
		},
	}
	outputRoot := t.TempDir()
	newAgent := func() *Agent {
		agent, err := New(Config{
			WorkerID: workerID, WorkerEpoch: 14, Recovery: recovery,
			Control: control, Runner: runner, HeartbeatInterval: time.Second,
			MonotonicNow:           func() time.Duration { return 10 * time.Second },
			Wait:                   func(context.Context, time.Duration) error { return nil },
			ArtifactStoreReachable: func(context.Context) bool { return true },
			OutputRoot:             outputRoot, InferenceBackendRevision: "sglang@backend-1",
			Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return agent
	}

	if _, err := newAgent().RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "response lost after Heartbeat CONTINUE commit") {
		t.Fatalf("first RunOnce error = %v", err)
	}
	control.assignment = nil
	result, err := newAgent().RunOnce(context.Background())
	if err != nil {
		t.Fatalf("resumed RunOnce: %v", err)
	}
	if result.Outcome != OutcomeControlPlaneStopped || result.AttemptID != attemptID ||
		result.StopReason != workercontrol.StopInvalidAuthority {
		t.Fatalf("resumed result = %#v", result)
	}
	if control.acquireCalls != 1 || control.startCalls != 1 {
		t.Fatalf("resumed running Attempt acquired or started again: Acquire=%d Start=%d", control.acquireCalls, control.startCalls)
	}
	wantEvents := []string{
		"control.acquire", "runner.prepare", "control.start", "runner.start",
		"runner.status", "control.heartbeat", "control.heartbeat",
		"runner.status", "control.heartbeat", "runner.cancel", "runner.status",
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("continued events = %#v, want %#v", events, wantEvents)
	}
}

func TestRunOnceReplaysStartWhenAStopResponseIsLost(t *testing.T) {
	workerID := uuid.MustParse("5a000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("5b000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("5c000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 15, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 15, time.Minute)
	control := &recordingControlPlane{
		assignment:  &assignment,
		startErrors: []error{errors.New("response lost after Start decision")},
		startResult: workercontrol.StartResult{
			Decision: workercontrol.Stop, StopReason: workercontrol.StopInvalidAuthority,
		},
	}
	runner := &recordingRunner{
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
	}
	outputRoot := t.TempDir()
	newAgent := func() *Agent {
		agent, err := New(Config{
			WorkerID: workerID, WorkerEpoch: 15, Recovery: recovery,
			Control: control, Runner: runner, HeartbeatInterval: time.Second,
			MonotonicNow: func() time.Duration { return 10 * time.Second },
			OutputRoot:   outputRoot, InferenceBackendRevision: "sglang@backend-1",
			Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return agent
	}

	if _, err := newAgent().RunOnce(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "response lost after Start decision") {
		t.Fatalf("first RunOnce error = %v", err)
	}
	control.assignment = nil
	result, err := newAgent().RunOnce(context.Background())
	if err != nil {
		t.Fatalf("replayed RunOnce: %v", err)
	}
	if result.Outcome != OutcomeControlPlaneStopped ||
		result.StopReason != workercontrol.StopInvalidAuthority || control.acquireCalls != 1 ||
		control.startCalls != 2 {
		t.Fatalf(
			"replayed result = %#v Acquire calls=%d Start calls=%d",
			result, control.acquireCalls, control.startCalls,
		)
	}
	if runner.cancelReason != runnertransport.CancelControlPlaneStop || runner.calls != 3 {
		t.Fatalf("Runner calls=%d cancel=%s", runner.calls, runner.cancelReason)
	}
}

func TestRunOnceCancelsAndCleansUpWhenHeartbeatFencesTheAttempt(t *testing.T) {
	workerID := uuid.MustParse("61000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("62000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("63000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 15, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 15, time.Minute)
	events := []string{}
	control := &recordingControlPlane{
		assignment: &assignment, events: &events,
		startResult: grantedTestStart(assignment),
		heartbeatResults: []workercontrol.HeartbeatResult{{
			Decision: workercontrol.HeartbeatStop, StopReason: workercontrol.StopInvalidAuthority,
		}},
	}
	runner := &recordingRunner{
		events:        &events,
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionRunning, Sequence: 1, BackendStage: "dit",
			GPUHealth:          json.RawMessage(`{"healthy":true}`),
			LocalArtifactState: json.RawMessage(`{"dit":"running"}`),
		}},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 15, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		MonotonicNow: func() time.Duration { return 10 * time.Second },
		OutputRoot:   t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	result, err := agent.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if result.Outcome != OutcomeControlPlaneStopped || result.StopReason != workercontrol.StopInvalidAuthority {
		t.Fatalf("RunOnce result = %#v", result)
	}
	wantEvents := []string{
		"control.acquire", "runner.prepare", "control.start", "runner.start",
		"runner.status", "control.heartbeat", "runner.cancel", "runner.status",
	}
	if !reflect.DeepEqual(events, wantEvents) || runner.cancelReason != runnertransport.CancelControlPlaneStop {
		t.Fatalf("fenced events = %#v cancel reason=%s", events, runner.cancelReason)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 15, Fence: assignment.LeaseFence,
	}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
		t.Fatalf("fenced Attempt Local Recovery State reopened: %v", err)
	}
}

func TestRunOnceRetainsAndReplaysTerminalCancellationUntilRunnerAccepts(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		leaseDeadline bool
		cancelErrors  []error
		cancelResults []runnertransport.CommandResult
		wantError     string
		wantOutcome   Outcome
		wantStop      workercontrol.StopReason
	}{
		{
			name: "Lease deadline Cancel error", leaseDeadline: true,
			cancelErrors:  []error{errors.New("runner Cancel response lost")},
			cancelResults: []runnertransport.CommandResult{{Decision: runnertransport.CommandAccepted}},
			wantError:     "runner Cancel response lost", wantOutcome: OutcomeLeaseDeadlineElapsed,
		},
		{
			name: "control stop Cancel rejection",
			cancelResults: []runnertransport.CommandResult{
				{Decision: runnertransport.CommandRejected, Detail: "runner still stopping"},
				{Decision: runnertransport.CommandAccepted},
			},
			wantError:   "runner rejected control-plane cancellation",
			wantOutcome: OutcomeControlPlaneStopped, wantStop: workercontrol.StopInvalidAuthority,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			workerID := uuid.New()
			attemptID := uuid.New()
			jobID := uuid.New()
			recovery := newTestRecoveryManager(t, workerID, 18, workerrecovery.Space{
				TotalBytes: 1 << 20, FreeBytes: 1 << 20,
			})
			assignment := validTestAssignment(workerID, attemptID, jobID, 18, time.Second)
			control := &recordingControlPlane{
				assignment: &assignment, startResult: grantedTestStart(assignment),
			}
			runner := &recordingRunner{
				prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
				startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
				cancelErrors:  append([]error(nil), testCase.cancelErrors...),
				cancelResults: append([]runnertransport.CommandResult(nil), testCase.cancelResults...),
			}
			clockCalls := 0
			monotonicNow := func() time.Duration {
				if testCase.leaseDeadline {
					clockCalls++
					if clockCalls == 1 {
						return 10 * time.Second
					}
					return 11 * time.Second
				}
				return 10 * time.Second
			}
			if !testCase.leaseDeadline {
				control.startResult = workercontrol.StartResult{
					Decision: workercontrol.Stop, StopReason: workercontrol.StopInvalidAuthority,
				}
			}
			outputRoot := t.TempDir()
			newAgent := func() *Agent {
				agent, err := New(Config{
					WorkerID: workerID, WorkerEpoch: 18, Recovery: recovery,
					Control: control, Runner: runner, HeartbeatInterval: time.Second,
					MonotonicNow: monotonicNow,
					OutputRoot:   outputRoot, InferenceBackendRevision: "sglang@backend-1",
					Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
				})
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				return agent
			}

			if _, err := newAgent().RunOnce(context.Background()); err == nil ||
				!strings.Contains(err.Error(), testCase.wantError) {
				t.Fatalf("first RunOnce error = %v, want %q", err, testCase.wantError)
			}
			if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
				AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 18, Fence: assignment.LeaseFence,
			}); err != nil {
				t.Fatalf("failed cancellation removed recoverable Local Recovery State: %v", err)
			}
			control.assignment = nil
			result, err := newAgent().RunOnce(context.Background())
			if err != nil {
				t.Fatalf("replayed RunOnce: %v", err)
			}
			if result.Outcome != testCase.wantOutcome || result.AttemptID != attemptID ||
				result.StopReason != testCase.wantStop {
				t.Fatalf("replayed result = %#v", result)
			}
			if control.acquireCalls != 1 {
				t.Fatalf("cancellation replay acquired new work: %d", control.acquireCalls)
			}
			if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
				AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 18, Fence: assignment.LeaseFence,
			}); !errors.Is(err, workerrecovery.ErrStateTerminal) {
				t.Fatalf("accepted cancellation did not terminalize Local Recovery State: %v", err)
			}
		})
	}
}

func TestRunOnceCancelsRunnerButRetainsRecoveryStateOnAgentShutdown(t *testing.T) {
	workerID := uuid.MustParse("81000000-0000-0000-0000-000000000001")
	attemptID := uuid.MustParse("82000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("83000000-0000-0000-0000-000000000003")
	recovery := newTestRecoveryManager(t, workerID, 17, workerrecovery.Space{
		TotalBytes: 1 << 20, FreeBytes: 1 << 20,
	})
	assignment := validTestAssignment(workerID, attemptID, jobID, 17, time.Minute)
	events := []string{}
	control := &recordingControlPlane{
		assignment: &assignment, events: &events,
		startResult: grantedTestStart(assignment),
		heartbeatResults: []workercontrol.HeartbeatResult{{
			Decision:  workercontrol.HeartbeatContinue,
			AttemptID: attemptID, JobID: jobID, WorkerID: workerID,
			WorkerEpoch: 17, LeaseFence: assignment.LeaseFence,
			HeartbeatSequence: 1, ExecutionPhase: workercontrol.ExecutionPhaseGenerating,
			LeaseValidFor: time.Minute,
		}},
	}
	runner := &recordingRunner{
		events:        &events,
		prepareResult: runnertransport.PrepareResult{Decision: runnertransport.CommandAccepted},
		startResult:   runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		cancelResult:  runnertransport.CommandResult{Decision: runnertransport.CommandAccepted},
		statuses: []runnertransport.Status{{
			State: runnertransport.ExecutionRunning, Sequence: 1, BackendStage: "dit",
			GPUHealth:          json.RawMessage(`{"healthy":true}`),
			LocalArtifactState: json.RawMessage(`{"dit":"running"}`),
		}},
	}
	agent, err := New(Config{
		WorkerID: workerID, WorkerEpoch: 17, Recovery: recovery,
		Control: control, Runner: runner, HeartbeatInterval: time.Second,
		MonotonicNow: func() time.Duration { return 10 * time.Second },
		Wait:         func(context.Context, time.Duration) error { return context.Canceled },
		OutputRoot:   t.TempDir(), InferenceBackendRevision: "sglang@backend-1",
		Finalization: &recordingFinalizationControl{}, PartUploader: &recordingPartUploader{},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = agent.RunOnce(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunOnce error = %v, want context canceled", err)
	}
	if runner.cancelReason != runnertransport.CancelAgentShutdown {
		t.Fatalf("shutdown cancel reason = %s", runner.cancelReason)
	}
	if _, err := recovery.Open(context.Background(), workerrecovery.Identity{
		AttemptID: attemptID, WorkerID: workerID, WorkerEpoch: 17, Fence: assignment.LeaseFence,
	}); err != nil {
		t.Fatalf("shutdown removed resumable Local Recovery State: %v", err)
	}
}

func newTestRecoveryManager(
	t *testing.T,
	workerID uuid.UUID,
	epoch int64,
	space workerrecovery.Space,
) *workerrecovery.Manager {
	t.Helper()
	manager, err := workerrecovery.New(workerrecovery.Config{
		Root: t.TempDir(), WorkerID: workerID, WorkerEpoch: epoch,
		AttemptQuotaBytes: 1 << 20, MaxEntryBytes: 1 << 18, MaxEntries: 16,
		HighWatermarkBytes: 50, LowWatermarkBytes: 20, CriticalFreeBytes: 10,
		TerminalRetention: time.Minute,
		SpaceProbe:        func(string) (workerrecovery.Space, error) { return space, nil },
	})
	if err != nil {
		t.Fatalf("create recovery Manager: %v", err)
	}
	return manager
}

type recordingControlPlane struct {
	acquireCalls           int
	capacityResult         workercontrol.CapacityResult
	capacityObservations   []workercontrol.CapacityObservation
	startCalls             int
	assignment             *workercontrol.Assignment
	acquireErr             error
	startResult            workercontrol.StartResult
	startErrors            []error
	events                 *[]string
	heartbeatResults       []workercontrol.HeartbeatResult
	heartbeatErrors        []error
	heartbeatObservations  []workercontrol.HeartbeatObservation
	finalizationHeartbeats chan<- int64
	heartbeatHook          func(context.Context, workercontrol.HeartbeatObservation) error
	failHook               func(context.Context, workercontrol.FailureObservation) error
	failErrors             []error
	failResult             workercontrol.RetryDecision
	failureObservation     workercontrol.FailureObservation
	readinessWorks         []workercontrol.ReadinessWork
	readinessWorkErrors    []error
	readinessReports       []workercontrol.ReadinessEvidence
	readinessReportResults []workercontrol.ReadinessResult
	readinessReportErrors  []error
}

type periodicCapacityControl struct {
	*recordingControlPlane
	reports chan workercontrol.CapacityObservation
}

func (control *periodicCapacityControl) ReportCapacity(
	_ context.Context,
	observation workercontrol.CapacityObservation,
) (workercontrol.CapacityResult, error) {
	control.reports <- observation
	return workercontrol.CapacityResult{
		WorkerState:             workercontrol.CapacityAdmittable,
		PoolState:               workercontrol.CapacityAdmittable,
		WorkerAssignmentAllowed: true,
		PoolReadinessAllowed:    true,
		PoolAssignmentAllowed:   true,
	}, nil
}

func (control *recordingControlPlane) ReportCapacity(
	_ context.Context,
	observation workercontrol.CapacityObservation,
) (workercontrol.CapacityResult, error) {
	control.capacityObservations = append(control.capacityObservations, observation)
	result := control.capacityResult
	if result.WorkerState == "" && result.PoolState == "" {
		result.WorkerState = workercontrol.CapacityAdmittable
		result.PoolState = workercontrol.CapacityAdmittable
		result.WorkerAssignmentAllowed = true
		result.PoolReadinessAllowed = true
		result.PoolAssignmentAllowed = true
	}
	return result, nil
}

func (control *recordingControlPlane) GetReadinessWork(
	context.Context,
	int64,
) (workercontrol.ReadinessWork, error) {
	if len(control.readinessWorkErrors) != 0 {
		err := control.readinessWorkErrors[0]
		control.readinessWorkErrors = control.readinessWorkErrors[1:]
		if err != nil {
			control.record("control.readiness.get")
			return workercontrol.ReadinessWork{}, err
		}
	}
	if len(control.readinessWorks) == 0 {
		return workercontrol.ReadinessWork{}, nil
	}
	work := control.readinessWorks[0]
	control.readinessWorks = control.readinessWorks[1:]
	control.record("control.readiness.get")
	return work, nil
}

func (control *recordingControlPlane) ReportReadiness(
	_ context.Context,
	evidence workercontrol.ReadinessEvidence,
) (workercontrol.ReadinessResult, error) {
	control.record("control.readiness.report")
	control.readinessReports = append(control.readinessReports, evidence)
	if len(control.readinessReportErrors) != 0 {
		err := control.readinessReportErrors[0]
		control.readinessReportErrors = control.readinessReportErrors[1:]
		if err != nil {
			return workercontrol.ReadinessResult{}, err
		}
	}
	if len(control.readinessReportResults) == 0 {
		return workercontrol.ReadinessResult{}, nil
	}
	result := control.readinessReportResults[0]
	control.readinessReportResults = control.readinessReportResults[1:]
	return result, nil
}

type recordingFinalizationControl struct {
	events                  *[]string
	plan                    workercontrol.FinalizationPlan
	contentTypes            map[uuid.UUID]string
	completion              workercontrol.VisibleCompletionResult
	beginErrors             []error
	claimDecisions          []workercontrol.ArtifactUploadClaimDecision
	verificationDecisions   []workercontrol.ArtifactVerificationDecision
	claimErrors             []error
	completeUploadErrors    []error
	completeUploadDecisions []workercontrol.ArtifactUploadDecision
	verificationErrors      []error
	completionErrors        []error
	completionHook          func(context.Context, workercontrol.VisibleCompletionCandidate) error
	claimIDs                []uuid.UUID
	verificationIDs         []uuid.UUID
	completionIDs           []uuid.UUID
	beginCalls              int
}

func successfulTestFinalization(
	assignment workercontrol.Assignment,
	outputs []runnertransport.Output,
	events *[]string,
) *recordingFinalizationControl {
	startedAt := time.Date(2026, 8, 25, 10, 0, 0, 0, time.UTC)
	deadlineAt := startedAt.Add(10 * time.Minute)
	plan := workercontrol.FinalizationPlan{
		Decision: workercontrol.FinalizationGranted, AttemptID: assignment.AttemptID,
		JobID: assignment.JobID, JobVersion: 9,
		FinalizationStartedAt: startedAt, FinalizationDeadlineAt: deadlineAt,
	}
	contentTypes := make(map[uuid.UUID]string, len(outputs))
	committed := make([]workercontrol.CommittedArtifact, len(outputs))
	for index, output := range outputs {
		artifactID := uuid.New()
		uploadID := uuid.New()
		kind := workercontrol.ArtifactKind(output.Kind)
		objectKey := "artifacts/org/project/job/attempt/" + artifactID.String() + "/output"
		plan.Artifacts = append(plan.Artifacts, workercontrol.PlannedArtifact{
			ArtifactID: artifactID, UploadID: uploadID, Kind: kind, Ordinal: output.Ordinal,
			ObjectKey: objectKey, ExpiresAt: deadlineAt,
		})
		contentTypes[uploadID] = output.ContentType
		committed[index] = workercontrol.CommittedArtifact{
			ArtifactID: artifactID, Kind: kind, Ordinal: output.Ordinal, ObjectKey: objectKey,
			ObjectVersionID: strings.ToLower(output.Kind) + "-version",
			SizeBytes:       output.SizeBytes, SHA256: output.SHA256, ContentType: output.ContentType,
		}
	}
	return &recordingFinalizationControl{
		events: events, plan: plan, contentTypes: contentTypes,
		completion: workercontrol.VisibleCompletionResult{
			Decision: workercontrol.VisibleCompletionCommitted,
			JobID:    assignment.JobID, AttemptID: assignment.AttemptID,
			ArtifactSetID: uuid.New(), ChargeID: uuid.New(), JobVersion: plan.JobVersion + 1,
			ManifestSHA256: sha256.Sum256([]byte("test-manifest")),
			Artifacts:      committed, CompletedAt: startedAt.Add(time.Minute),
		},
	}
}

func (control *recordingFinalizationControl) BeginFinalization(
	context.Context,
	workercontrol.LeaseCredentials,
) (workercontrol.FinalizationPlan, error) {
	control.beginCalls++
	control.record("finalization.begin")
	if len(control.beginErrors) != 0 {
		err := control.beginErrors[0]
		control.beginErrors = control.beginErrors[1:]
		if err != nil {
			return workercontrol.FinalizationPlan{}, err
		}
	}
	return control.plan, nil
}

func (control *recordingFinalizationControl) ClaimArtifactUpload(
	_ context.Context,
	_ workercontrol.LeaseCredentials,
	uploadID uuid.UUID,
	claimID uuid.UUID,
	part workertransport.ArtifactUploadPartIntent,
) (workertransport.ArtifactUploadClaim, error) {
	control.record("finalization.claim")
	control.claimIDs = append(control.claimIDs, claimID)
	if len(control.claimErrors) != 0 {
		err := control.claimErrors[0]
		control.claimErrors = control.claimErrors[1:]
		if err != nil {
			return workertransport.ArtifactUploadClaim{}, err
		}
	}
	decision := workercontrol.ArtifactUploadClaimGranted
	if len(control.claimDecisions) != 0 {
		decision = control.claimDecisions[0]
		control.claimDecisions = control.claimDecisions[1:]
	}
	if decision != workercontrol.ArtifactUploadClaimGranted {
		return workertransport.ArtifactUploadClaim{
			ArtifactUploadClaim: workercontrol.ArtifactUploadClaim{Decision: decision},
		}, nil
	}
	for _, planned := range control.plan.Artifacts {
		if planned.UploadID != uploadID {
			continue
		}
		return workertransport.ArtifactUploadClaim{
			ArtifactUploadClaim: workercontrol.ArtifactUploadClaim{
				Decision: workercontrol.ArtifactUploadClaimGranted,
				ClaimID:  claimID, UploadID: uploadID, ArtifactID: planned.ArtifactID,
				ObjectKey: planned.ObjectKey, ExpectedContentType: control.contentTypes[uploadID],
				MultipartUploadID: "multipart-" + uploadID.String(),
				ClaimExpiresAt:    control.plan.FinalizationDeadlineAt,
				UploadExpiresAt:   control.plan.FinalizationDeadlineAt, Version: 1,
			},
			UploadPart: &workertransport.SignedArtifactUploadPart{
				Number: part.Number, SizeBytes: part.SizeBytes, SHA256: part.SHA256,
				URL: "https://objects.internal/upload", ExpiresAt: control.plan.FinalizationDeadlineAt,
				RequiredHeaders: map[string]string{
					"Content-Length":        strconv.FormatInt(part.SizeBytes, 10),
					"X-Amz-Checksum-Sha256": base64.StdEncoding.EncodeToString(part.SHA256[:]),
				},
			},
		}, nil
	}
	return workertransport.ArtifactUploadClaim{}, errors.New("unknown test upload")
}

func (control *recordingFinalizationControl) CompleteArtifactMultipartUpload(
	_ context.Context,
	_ workercontrol.LeaseCredentials,
	uploadID uuid.UUID,
	claimID uuid.UUID,
	_ workercontrol.ArtifactUploadReport,
) (workercontrol.ArtifactUploadResult, error) {
	control.record("finalization.complete_upload")
	control.claimIDs = append(control.claimIDs, claimID)
	if len(control.completeUploadErrors) != 0 {
		err := control.completeUploadErrors[0]
		control.completeUploadErrors = control.completeUploadErrors[1:]
		if err != nil {
			return workercontrol.ArtifactUploadResult{}, err
		}
	}
	decision := workercontrol.ArtifactUploadRecorded
	if len(control.completeUploadDecisions) != 0 {
		decision = control.completeUploadDecisions[0]
		control.completeUploadDecisions = control.completeUploadDecisions[1:]
	}
	if decision != workercontrol.ArtifactUploadRecorded {
		return workercontrol.ArtifactUploadResult{Decision: decision}, nil
	}
	for _, planned := range control.plan.Artifacts {
		if planned.UploadID == uploadID {
			return workercontrol.ArtifactUploadResult{
				Decision: workercontrol.ArtifactUploadRecorded, UploadID: uploadID,
				ArtifactID: planned.ArtifactID, ObjectVersionID: strings.ToLower(string(planned.Kind)) + "-version", Version: 2,
			}, nil
		}
	}
	return workercontrol.ArtifactUploadResult{}, errors.New("unknown test upload")
}

func (control *recordingFinalizationControl) VerifyArtifact(
	_ context.Context,
	_ workercontrol.LeaseCredentials,
	uploadID uuid.UUID,
	verificationID uuid.UUID,
) (workercontrol.ArtifactVerificationResult, error) {
	control.record("finalization.verify")
	control.verificationIDs = append(control.verificationIDs, verificationID)
	if len(control.verificationErrors) != 0 {
		err := control.verificationErrors[0]
		control.verificationErrors = control.verificationErrors[1:]
		if err != nil {
			return workercontrol.ArtifactVerificationResult{}, err
		}
	}
	decision := workercontrol.ArtifactVerified
	if len(control.verificationDecisions) != 0 {
		decision = control.verificationDecisions[0]
		control.verificationDecisions = control.verificationDecisions[1:]
	}
	for _, planned := range control.plan.Artifacts {
		if planned.UploadID == uploadID {
			return workercontrol.ArtifactVerificationResult{
				Decision: decision, VerificationID: verificationID,
				UploadID: uploadID, ArtifactID: planned.ArtifactID,
				ObjectVersionID: strings.ToLower(string(planned.Kind)) + "-version", Version: 3,
				VerifiedAt: control.plan.FinalizationStartedAt.Add(time.Minute),
			}, nil
		}
	}
	return workercontrol.ArtifactVerificationResult{}, errors.New("unknown test upload")
}

func (control *recordingFinalizationControl) CompleteVisibleCompletion(
	ctx context.Context,
	_ workercontrol.LeaseCredentials,
	candidate workercontrol.VisibleCompletionCandidate,
) (workercontrol.VisibleCompletionResult, error) {
	control.record("finalization.visible_completion")
	control.completionIDs = append(control.completionIDs, candidate.CompletionID)
	if control.completionHook != nil {
		if err := control.completionHook(ctx, candidate); err != nil {
			return workercontrol.VisibleCompletionResult{}, err
		}
	}
	if len(control.completionErrors) != 0 {
		err := control.completionErrors[0]
		control.completionErrors = control.completionErrors[1:]
		if err != nil {
			return workercontrol.VisibleCompletionResult{}, err
		}
	}
	result := control.completion
	result.CompletionID = candidate.CompletionID
	return result, nil
}

func (control *recordingFinalizationControl) record(event string) {
	if control.events != nil {
		*control.events = append(*control.events, event)
	}
}

type recordingPartUploader struct {
	events   *[]string
	payloads [][]byte
}

func (uploader *recordingPartUploader) Upload(
	_ context.Context,
	part workertransport.SignedArtifactUploadPart,
	payload []byte,
) (workercontrol.ArtifactUploadPart, error) {
	if uploader.events != nil {
		*uploader.events = append(*uploader.events, "artifact.put")
	}
	uploader.payloads = append(uploader.payloads, append([]byte(nil), payload...))
	return workercontrol.ArtifactUploadPart{
		Number: part.Number, ETag: "etag", SizeBytes: int64(len(payload)),
		ChecksumSHA256: base64.StdEncoding.EncodeToString(part.SHA256[:]),
	}, nil
}

type blockingPartUploader struct {
	started chan struct{}
	release chan struct{}
}

func (uploader *blockingPartUploader) Upload(
	ctx context.Context,
	part workertransport.SignedArtifactUploadPart,
	payload []byte,
) (workercontrol.ArtifactUploadPart, error) {
	select {
	case <-uploader.started:
	default:
		close(uploader.started)
	}
	select {
	case <-uploader.release:
	case <-ctx.Done():
		return workercontrol.ArtifactUploadPart{}, ctx.Err()
	}
	return workercontrol.ArtifactUploadPart{
		Number: part.Number, ETag: "etag", SizeBytes: int64(len(payload)),
		ChecksumSHA256: base64.StdEncoding.EncodeToString(part.SHA256[:]),
	}, nil
}

func (control *recordingControlPlane) Acquire(
	context.Context,
	int64,
) (workercontrol.Assignment, error) {
	control.acquireCalls++
	control.record("control.acquire")
	if control.acquireErr != nil {
		return workercontrol.Assignment{}, control.acquireErr
	}
	if control.assignment != nil {
		return *control.assignment, nil
	}
	return workercontrol.Assignment{}, &workercontrol.Failure{
		Code: workercontrol.FailureNoAssignment, Message: "none",
	}
}

func (control *recordingControlPlane) Start(
	context.Context,
	workercontrol.LeaseCredentials,
) (workercontrol.StartResult, error) {
	control.startCalls++
	control.record("control.start")
	if len(control.startErrors) != 0 {
		err := control.startErrors[0]
		control.startErrors = control.startErrors[1:]
		if err != nil {
			return workercontrol.StartResult{}, err
		}
	}
	return control.startResult, nil
}

func (control *recordingControlPlane) Heartbeat(
	ctx context.Context,
	_ workercontrol.LeaseCredentials,
	observation workercontrol.HeartbeatObservation,
) (workercontrol.HeartbeatResult, error) {
	control.record("control.heartbeat")
	control.heartbeatObservations = append(control.heartbeatObservations, observation)
	if control.heartbeatHook != nil {
		if err := control.heartbeatHook(ctx, observation); err != nil {
			return workercontrol.HeartbeatResult{}, err
		}
	}
	if observation.BackendStage == "artifact-finalization" && control.finalizationHeartbeats != nil {
		control.finalizationHeartbeats <- observation.Sequence
	}
	if len(control.heartbeatErrors) != 0 {
		err := control.heartbeatErrors[0]
		control.heartbeatErrors = control.heartbeatErrors[1:]
		if err != nil {
			return workercontrol.HeartbeatResult{}, err
		}
	}
	if len(control.heartbeatResults) == 0 {
		if control.assignment != nil && observation.BackendStage == "artifact-finalization" {
			now := time.Now().UTC()
			return workercontrol.HeartbeatResult{
				Decision:  workercontrol.HeartbeatContinue,
				AttemptID: control.assignment.AttemptID, JobID: control.assignment.JobID,
				WorkerID: control.assignment.WorkerID, WorkerEpoch: control.assignment.WorkerEpoch,
				LeaseFence: control.assignment.LeaseFence, HeartbeatSequence: observation.Sequence,
				ExecutionPhase:    workercontrol.ExecutionPhaseFinalizing,
				ProgressUpdatedAt: now, LeaseExpiresAt: now.Add(time.Minute), LeaseValidFor: time.Minute,
			}, nil
		}
		return workercontrol.HeartbeatResult{}, nil
	}
	result := control.heartbeatResults[0]
	control.heartbeatResults = control.heartbeatResults[1:]
	return result, nil
}

func (control *recordingControlPlane) Fail(
	ctx context.Context,
	_ workercontrol.LeaseCredentials,
	observation workercontrol.FailureObservation,
) (workercontrol.RetryDecision, error) {
	control.record("control.fail")
	control.failureObservation = observation
	if control.failHook != nil {
		if err := control.failHook(ctx, observation); err != nil {
			return workercontrol.RetryDecision{}, err
		}
	}
	if len(control.failErrors) != 0 {
		err := control.failErrors[0]
		control.failErrors = control.failErrors[1:]
		if err != nil {
			return workercontrol.RetryDecision{}, err
		}
	}
	return control.failResult, nil
}

type recordingRunner struct {
	calls                 int
	events                *[]string
	prepareResult         runnertransport.PrepareResult
	startResult           runnertransport.CommandResult
	cancelResult          runnertransport.CommandResult
	cancelResults         []runnertransport.CommandResult
	cancelErrors          []error
	statuses              []runnertransport.Status
	collectResult         runnertransport.CollectOutputsResult
	identity              runnertransport.AttemptIdentity
	spec                  runnertransport.ExecutionSpec
	sameAuthorityRecovery bool
	cancelReason          runnertransport.CancelReason
	statusHook            func(context.Context) error
	cancelContextErr      error
	canceled              bool
	readinessResults      []runnertransport.ReadinessResult
	readinessErrors       []error
	readinessIdentities   []runnertransport.ReadinessIdentity
	readinessChecks       []runnertransport.ReadinessCheck
}

func (runner *recordingRunner) ProbeReadiness(
	_ context.Context,
	identity runnertransport.ReadinessIdentity,
	check runnertransport.ReadinessCheck,
) (runnertransport.ReadinessResult, error) {
	runner.record("runner.readiness")
	runner.readinessIdentities = append(runner.readinessIdentities, identity)
	runner.readinessChecks = append(runner.readinessChecks, check)
	if len(runner.readinessErrors) != 0 {
		err := runner.readinessErrors[0]
		runner.readinessErrors = runner.readinessErrors[1:]
		if err != nil {
			return runnertransport.ReadinessResult{}, err
		}
	}
	if len(runner.readinessResults) == 0 {
		return runnertransport.ReadinessResult{}, nil
	}
	result := runner.readinessResults[0]
	runner.readinessResults = runner.readinessResults[1:]
	return result, nil
}

func (runner *recordingRunner) Prepare(
	_ context.Context,
	identity runnertransport.AttemptIdentity,
	spec runnertransport.ExecutionSpec,
	sameAuthorityRecovery bool,
) (runnertransport.PrepareResult, error) {
	runner.calls++
	runner.record("runner.prepare")
	runner.identity = identity
	runner.spec = spec
	runner.sameAuthorityRecovery = sameAuthorityRecovery
	return runner.prepareResult, nil
}

func (runner *recordingRunner) Start(
	_ context.Context,
	identity runnertransport.AttemptIdentity,
) (runnertransport.CommandResult, error) {
	runner.calls++
	runner.record("runner.start")
	runner.identity = identity
	return runner.startResult, nil
}

func (runner *recordingRunner) Cancel(
	ctx context.Context,
	_ runnertransport.AttemptIdentity,
	reason runnertransport.CancelReason,
) (runnertransport.CommandResult, error) {
	runner.calls++
	runner.record("runner.cancel")
	runner.cancelReason = reason
	runner.cancelContextErr = ctx.Err()
	if len(runner.cancelErrors) != 0 {
		err := runner.cancelErrors[0]
		runner.cancelErrors = runner.cancelErrors[1:]
		if err != nil {
			return runnertransport.CommandResult{}, err
		}
	}
	if len(runner.cancelResults) != 0 {
		result := runner.cancelResults[0]
		runner.cancelResults = runner.cancelResults[1:]
		if result.Decision == runnertransport.CommandAccepted {
			runner.canceled = true
		}
		return result, nil
	}
	if runner.cancelResult.Decision == runnertransport.CommandAccepted {
		runner.canceled = true
	}
	return runner.cancelResult, nil
}

func (runner *recordingRunner) Status(
	ctx context.Context,
	_ runnertransport.AttemptIdentity,
) (runnertransport.Status, error) {
	runner.calls++
	runner.record("runner.status")
	if runner.canceled {
		if len(runner.statuses) != 0 {
			status := runner.statuses[0]
			runner.statuses = runner.statuses[1:]
			if status.State == runnertransport.ExecutionSucceeded ||
				status.State == runnertransport.ExecutionFailed ||
				status.State == runnertransport.ExecutionCanceled {
				return status, nil
			}
		}
		return runnertransport.Status{State: runnertransport.ExecutionCanceled}, nil
	}
	if runner.statusHook != nil {
		if err := runner.statusHook(ctx); err != nil {
			return runnertransport.Status{}, err
		}
	}
	if len(runner.statuses) == 0 {
		return runnertransport.Status{}, nil
	}
	status := runner.statuses[0]
	runner.statuses = runner.statuses[1:]
	return status, nil
}

func (runner *recordingRunner) CollectOutputs(
	context.Context,
	runnertransport.AttemptIdentity,
) (runnertransport.CollectOutputsResult, error) {
	runner.calls++
	runner.record("runner.collect_outputs")
	return runner.collectResult, nil
}

func (control *recordingControlPlane) record(event string) {
	if control.events != nil {
		*control.events = append(*control.events, event)
	}
}

func (runner *recordingRunner) record(event string) {
	if runner.events != nil {
		*runner.events = append(*runner.events, event)
	}
}

func validTestAssignment(
	workerID, attemptID, jobID uuid.UUID,
	epoch int64,
	validFor time.Duration,
) workercontrol.Assignment {
	return workercontrol.Assignment{
		AttemptID: attemptID, JobID: jobID, WorkerID: workerID, WorkerEpoch: epoch,
		ModelRevisionID:            uuid.MustParse("34000000-0000-0000-0000-000000000004"),
		GenerationPresetRevisionID: uuid.MustParse("35000000-0000-0000-0000-000000000005"),
		ExecutionProfileRevisionID: uuid.MustParse("36000000-0000-0000-0000-000000000006"),
		OutputSpecID:               uuid.MustParse("37000000-0000-0000-0000-000000000007"),
		RequestContent:             `{"prompt":"execute privately"}`,
		AttemptNumber:              1, LeaseToken: "lease-token", LeaseFence: 3,
		LeaseValidFor: validFor,
	}
}

func grantedTestStart(assignment workercontrol.Assignment) workercontrol.StartResult {
	return workercontrol.StartResult{
		Decision: workercontrol.StartGranted, AttemptID: assignment.AttemptID,
		JobID: assignment.JobID, WorkerID: assignment.WorkerID,
		WorkerEpoch: assignment.WorkerEpoch, LeaseFence: assignment.LeaseFence,
	}
}
