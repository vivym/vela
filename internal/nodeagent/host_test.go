package nodeagent

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/remediation"
)

type fakeCommandRunner struct {
	output  []byte
	outputs map[string][]byte
	err     error
	paths   []string
}

func (runner *fakeCommandRunner) Run(_ context.Context, _ remediation.Plan, path string, _ []string) ([]byte, error) {
	runner.paths = append(runner.paths, path)
	if output, ok := runner.outputs[path]; ok {
		return append([]byte(nil), output...), runner.err
	}
	return append([]byte(nil), runner.output...), runner.err
}

func TestCertifiedExecutorRequiresCapabilityFenceRateAndPostcheck(t *testing.T) {
	plan := remediation.Plan{
		OperationID: uuid.New(), ExecutionClaimID: uuid.New(), WorkerID: uuid.New(), WorkerEpoch: 1,
		NodeIdentity: "node-1", DeviceIdentity: "gpu-0", FailureClass: "process_failure", ActionLevel: remediation.ActionL0ProcessRestart,
		CertificationRevision: "matrix-v1", FailureEvidenceDigest: digestForTest("failure"),
		DeadlineAt: time.Now().Add(time.Minute).UTC(),
	}
	runner := &fakeCommandRunner{output: []byte("command-output"), outputs: map[string][]byte{
		"/usr/local/bin/health": postcheckEvidenceForTest(t, plan),
	}}
	allowlisted, err := remediation.NewAllowlistedExecutor(runner, map[remediation.ActionLevel]struct {
		Path string
		Args []string
	}{remediation.ActionL0ProcessRestart: {Path: "/usr/local/bin/restart", Args: []string{"worker"}}})
	if err != nil {
		t.Fatalf("NewAllowlistedExecutor: %v", err)
	}
	policy, err := NewStaticCapabilityPolicy(map[string]DeviceCapability{
		"gpu-0": {CertificationRevision: "matrix-v1", Actions: map[remediation.ActionLevel]bool{remediation.ActionL0ProcessRestart: true}},
	})
	if err != nil {
		t.Fatalf("NewStaticCapabilityPolicy: %v", err)
	}
	fenceCalls := 0
	fence := CallbackFence(func(context.Context, remediation.Plan) error { fenceCalls++; return nil })
	postcheck, err := NewCommandPostcheck(runner, "/usr/local/bin/health", nil)
	if err != nil {
		t.Fatalf("NewCommandPostcheck: %v", err)
	}
	limiter, err := NewRateLimiter(RateLimit{MinimumInterval: time.Second, Window: time.Minute, MaxExecutions: 1})
	if err != nil {
		t.Fatalf("NewRateLimiter: %v", err)
	}
	executor, err := NewCertifiedExecutor(allowlisted, policy, fence, postcheck, limiter)
	if err != nil {
		t.Fatalf("NewCertifiedExecutor: %v", err)
	}
	result, err := executor.Execute(context.Background(), plan)
	if err != nil || !result.PostcheckVerified || result.ResultCode != "POSTCHECK_OK" || fenceCalls != 1 || len(runner.paths) != 2 {
		t.Fatalf("certified execution = %#v err=%v fence=%d paths=%v", result, err, fenceCalls, runner.paths)
	}
	if _, err := executor.Execute(context.Background(), plan); err == nil || !strings.Contains(err.Error(), "rate limit") {
		t.Fatalf("second execution error = %v, want rate limit", err)
	}
}

func TestCertifiedExecutorFailsClosedOnPolicyFenceAndPostcheck(t *testing.T) {
	runner := &fakeCommandRunner{output: []byte("ok")}
	allowlisted, err := remediation.NewAllowlistedExecutor(runner, map[remediation.ActionLevel]struct {
		Path string
		Args []string
	}{remediation.ActionL0ProcessRestart: {Path: "/usr/local/bin/restart"}})
	if err != nil {
		t.Fatalf("NewAllowlistedExecutor: %v", err)
	}
	policy, err := NewStaticCapabilityPolicy(map[string]DeviceCapability{
		"gpu-0": {CertificationRevision: "matrix-v1", Actions: map[remediation.ActionLevel]bool{remediation.ActionL0ProcessRestart: true}},
	})
	if err != nil {
		t.Fatalf("NewStaticCapabilityPolicy: %v", err)
	}
	limiter, _ := NewRateLimiter(RateLimit{MinimumInterval: time.Second, Window: time.Minute, MaxExecutions: 1})
	postcheck, _ := NewCommandPostcheck(runner, "/usr/local/bin/health", nil)
	blocked, _ := NewCertifiedExecutor(allowlisted, policy, CallbackFence(func(context.Context, remediation.Plan) error { return errors.New("active lease") }), postcheck, limiter)
	plan := remediation.Plan{
		OperationID: uuid.New(), ExecutionClaimID: uuid.New(), WorkerID: uuid.New(), WorkerEpoch: 1,
		NodeIdentity: "node-1", DeviceIdentity: "gpu-0", FailureClass: "process_failure", ActionLevel: remediation.ActionL0ProcessRestart,
		CertificationRevision: "matrix-v1", FailureEvidenceDigest: digestForTest("failure"),
		DeadlineAt: time.Now().Add(time.Minute).UTC(),
	}
	if _, err := blocked.Execute(context.Background(), plan); err == nil || !strings.Contains(err.Error(), "fence") {
		t.Fatalf("fence rejection = %v", err)
	}
	failedPolicy, _ := NewCertifiedExecutor(allowlisted, policy, CallbackFence(func(context.Context, remediation.Plan) error { return nil }), postcheck, limiter)
	plan.DeviceIdentity = "gpu-1"
	if _, err := failedPolicy.Execute(context.Background(), plan); err == nil || !strings.Contains(err.Error(), "not certified") {
		t.Fatalf("policy rejection = %v", err)
	}
	postcheckRunner := &fakeCommandRunner{err: errors.New("health unavailable")}
	failedPostcheck, _ := NewCommandPostcheck(postcheckRunner, "/usr/local/bin/health", nil)
	postcheckExecutor, _ := NewCertifiedExecutor(allowlisted, policy, CallbackFence(func(context.Context, remediation.Plan) error { return nil }), failedPostcheck, limiter)
	plan.DeviceIdentity = "gpu-0"
	result, err := postcheckExecutor.Execute(context.Background(), plan)
	if err != nil || result.PostcheckVerified || result.ResultCode != "POSTCHECK_FAILED" {
		t.Fatalf("post-check failure = %#v err=%v", result, err)
	}
}

func TestExecCommandRunnerRejectsUnsafePathAndNUL(t *testing.T) {
	runner := ExecCommandRunner{}
	if _, err := runner.Run(context.Background(), remediation.Plan{}, "relative", nil); err == nil {
		t.Fatal("relative command path was accepted")
	}
	if _, err := runner.Run(context.Background(), remediation.Plan{}, "/bin/echo", []string{"bad\x00arg"}); err == nil {
		t.Fatal("NUL argument was accepted")
	}
}

func TestExecCommandRunnerPassesImmutablePlanAsBoundedArguments(t *testing.T) {
	path := writeHostHelper(t, "#!/bin/sh\nprintf '%s\\n' \"$@\"\n")
	evidence := digestForTest("failure")
	plan := remediation.Plan{
		OperationID:      uuid.MustParse("10000000-0000-0000-0000-000000000001"),
		ExecutionClaimID: uuid.MustParse("20000000-0000-0000-0000-000000000002"),
		WorkerID:         uuid.MustParse("30000000-0000-0000-0000-000000000003"),
		WorkerEpoch:      7, NodeIdentity: "node-7", DeviceIdentity: "GPU-7", FailureClass: "gpu_fault",
		ActionLevel: remediation.ActionL2GPUReset, CertificationRevision: "matrix-v7",
		FailureEvidenceDigest: evidence, DeadlineAt: time.Unix(20_000, 123).UTC(),
	}
	output, err := (ExecCommandRunner{}).Run(context.Background(), plan, path, []string{"fixed"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, expected := range []string{
		"fixed", "--vela-operation-id=10000000-0000-0000-0000-000000000001",
		"--vela-execution-claim-id=20000000-0000-0000-0000-000000000002",
		"--vela-worker-id=30000000-0000-0000-0000-000000000003", "--vela-worker-epoch=7",
		"--vela-node-identity=node-7", "--vela-device-identity=GPU-7", "--vela-failure-class=gpu_fault", "--vela-action-level=L2_GPU_RESET",
		"--vela-certification-revision=matrix-v7", "--vela-failure-evidence-sha256=" + hex.EncodeToString(evidence),
		"--vela-deadline-at=1970-01-01T05:33:20.000000123Z",
	} {
		if !strings.Contains(string(output), expected) {
			t.Fatalf("host helper output %q lacks %q", output, expected)
		}
	}
}

func TestExecCommandRunnerPassesHeldExecutableFileDescriptor(t *testing.T) {
	path := writeHostHelper(t, "#!/bin/sh\ntest -r /dev/fd/3 || exit 42\nprintf held-executable-fd\n")
	plan := remediation.Plan{
		OperationID: uuid.New(), ExecutionClaimID: uuid.New(), WorkerID: uuid.New(), WorkerEpoch: 1,
		NodeIdentity: "node-1", DeviceIdentity: "gpu-0", FailureClass: "process_failure",
		ActionLevel: remediation.ActionL0ProcessRestart, CertificationRevision: "matrix-v1",
		FailureEvidenceDigest: digestForTest("failure"), DeadlineAt: time.Now().Add(time.Minute).UTC(),
	}
	output, err := (ExecCommandRunner{}).Run(context.Background(), plan, path, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if string(output) != "held-executable-fd" {
		t.Fatalf("host helper output = %q, want held executable fd proof", output)
	}
}

func TestExecCommandRunnerRejectsUnsafeDarwinTemporaryParent(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin uses a private staging copy instead of direct fd execution")
	}
	path := writeHostHelper(t, "#!/bin/sh\nprintf should-not-run\n")
	unsafeTemporaryParent := t.TempDir()
	if err := os.Chmod(unsafeTemporaryParent, 0o777); err != nil {
		t.Fatalf("relax temporary parent permissions: %v", err)
	}
	t.Setenv("TMPDIR", unsafeTemporaryParent)
	plan := remediation.Plan{
		OperationID: uuid.New(), ExecutionClaimID: uuid.New(), WorkerID: uuid.New(), WorkerEpoch: 1,
		NodeIdentity: "node-1", DeviceIdentity: "gpu-0", FailureClass: "process_failure",
		ActionLevel: remediation.ActionL0ProcessRestart, CertificationRevision: "matrix-v1",
		FailureEvidenceDigest: digestForTest("failure"), DeadlineAt: time.Now().Add(time.Minute).UTC(),
	}
	if _, err := (ExecCommandRunner{}).Run(context.Background(), plan, path, nil); err == nil || !strings.Contains(err.Error(), "staging parent") {
		t.Fatalf("Run error = %v, want untrusted staging parent rejection", err)
	}
}

func writeHostHelper(t *testing.T, content string) string {
	t.Helper()
	directory := t.TempDir()
	path := filepath.Join(directory, "helper")
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatalf("write host helper: %v", err)
	}
	return path
}

func TestFenceAndPostcheckRejectMismatchedStructuredEvidence(t *testing.T) {
	plan := remediation.Plan{
		OperationID: uuid.New(), ExecutionClaimID: uuid.New(), WorkerID: uuid.New(), WorkerEpoch: 2,
		NodeIdentity: "node-2", DeviceIdentity: "gpu-2", FailureClass: "cuda_fault", ActionLevel: remediation.ActionL1CUDACleanup,
		CertificationRevision: "matrix-v2", FailureEvidenceDigest: digestForTest("failure"),
		DeadlineAt: time.Now().Add(time.Minute).UTC(),
	}
	wrongPlan := plan
	wrongPlan.DeviceIdentity = "gpu-other"
	fenceRunner := &fakeCommandRunner{output: fenceEvidenceForTest(t, wrongPlan)}
	fence, err := NewCommandFence(fenceRunner, "/usr/local/bin/fence", nil)
	if err != nil {
		t.Fatalf("NewCommandFence: %v", err)
	}
	if err := fence.Check(context.Background(), plan); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("mismatched fence evidence error = %v", err)
	}
	postcheckRunner := &fakeCommandRunner{output: postcheckEvidenceForTest(t, wrongPlan)}
	postcheck, err := NewCommandPostcheck(postcheckRunner, "/usr/local/bin/health", nil)
	if err != nil {
		t.Fatalf("NewCommandPostcheck: %v", err)
	}
	if _, err := postcheck.Verify(context.Background(), plan); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("mismatched post-check evidence error = %v", err)
	}
}

func TestFileRateLimiterSurvivesAgentRestart(t *testing.T) {
	directory := t.TempDir()
	configuration := RateLimit{MinimumInterval: time.Minute, Window: time.Hour, MaxExecutions: 2}
	limiter, err := NewFileRateLimiter(directory, configuration)
	if err != nil {
		t.Fatalf("NewFileRateLimiter: %v", err)
	}
	now := time.Unix(30_000, 0).UTC()
	if err := limiter.Allow(now); err != nil {
		t.Fatalf("first Allow: %v", err)
	}
	restarted, err := NewFileRateLimiter(directory, configuration)
	if err != nil {
		t.Fatalf("reopen FileRateLimiter: %v", err)
	}
	if err := restarted.Allow(now.Add(30 * time.Second)); err == nil || !strings.Contains(err.Error(), "minimum interval") {
		t.Fatalf("post-restart Allow error = %v, want minimum interval", err)
	}
	if err := restarted.Allow(now.Add(time.Minute)); err != nil {
		t.Fatalf("Allow after minimum interval: %v", err)
	}
	if err := restarted.Allow(now.Add(2 * time.Minute)); err == nil || !strings.Contains(err.Error(), "rate limit") {
		t.Fatalf("window limit error = %v, want rate limit", err)
	}
}

func fenceEvidenceForTest(t *testing.T, plan remediation.Plan) []byte {
	t.Helper()
	return marshalHostEvidenceForTest(t, plan, map[string]any{
		"new_assignments_stopped":  true,
		"target_processes_stopped": true,
	})
}

func postcheckEvidenceForTest(t *testing.T, plan remediation.Plan) []byte {
	t.Helper()
	return marshalHostEvidenceForTest(t, plan, map[string]any{
		"device_healthy":            true,
		"inference_backend_healthy": true,
		"detail":                    "device and inference backend health verified",
	})
}

func marshalHostEvidenceForTest(t *testing.T, plan remediation.Plan, fields map[string]any) []byte {
	t.Helper()
	fields["operation_id"] = plan.OperationID.String()
	fields["execution_claim_id"] = plan.ExecutionClaimID.String()
	fields["worker_id"] = plan.WorkerID.String()
	fields["worker_epoch"] = plan.WorkerEpoch
	fields["node_identity"] = plan.NodeIdentity
	fields["device_identity"] = plan.DeviceIdentity
	fields["failure_class"] = plan.FailureClass
	fields["action_level"] = string(plan.ActionLevel)
	fields["certification_revision"] = plan.CertificationRevision
	fields["failure_evidence_sha256"] = hex.EncodeToString(plan.FailureEvidenceDigest)
	encoded, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal host evidence: %v", err)
	}
	return encoded
}
