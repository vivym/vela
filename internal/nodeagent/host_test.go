package nodeagent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/remediation"
)

type fakeCommandRunner struct {
	output []byte
	err    error
	paths  []string
}

func (runner *fakeCommandRunner) Run(_ context.Context, _ remediation.Plan, path string, _ []string) ([]byte, error) {
	runner.paths = append(runner.paths, path)
	return append([]byte(nil), runner.output...), runner.err
}

func TestCertifiedExecutorRequiresCapabilityFenceRateAndPostcheck(t *testing.T) {
	runner := &fakeCommandRunner{output: []byte("command-output")}
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
	plan := remediation.Plan{OperationID: uuid.New(), WorkerID: uuid.New(), WorkerEpoch: 1, NodeIdentity: "node-1", DeviceIdentity: "gpu-0", ActionLevel: remediation.ActionL0ProcessRestart, CertificationRevision: "matrix-v1"}
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
	plan := remediation.Plan{OperationID: uuid.New(), WorkerID: uuid.New(), WorkerEpoch: 1, NodeIdentity: "node-1", DeviceIdentity: "gpu-0", ActionLevel: remediation.ActionL0ProcessRestart, CertificationRevision: "matrix-v1"}
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
