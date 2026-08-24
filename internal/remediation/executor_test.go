package remediation

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

type recordingCommandRunner struct {
	plan Plan
	path string
	args []string
	err  error
}

func (r *recordingCommandRunner) Run(_ context.Context, plan Plan, path string, args []string) ([]byte, error) {
	r.plan = plan
	r.path = path
	r.args = append([]string(nil), args...)
	if r.err != nil {
		return nil, r.err
	}
	return []byte("post-check-ok"), nil
}

func TestAllowlistedExecutorRunsOnlyRegisteredAction(t *testing.T) {
	runner := &recordingCommandRunner{}
	executor, err := NewAllowlistedExecutor(runner, map[ActionLevel]struct {
		Path string
		Args []string
	}{
		ActionL0ProcessRestart: {Path: "/usr/local/bin/vela-process-restart", Args: []string{"--scope", "worker"}},
	})
	if err != nil {
		t.Fatalf("create allowlisted Remediation executor: %v", err)
	}
	operationID := uuid.New()
	workerID := uuid.New()
	result, err := executor.Execute(context.Background(), Plan{
		OperationID: operationID, WorkerID: workerID,
		ActionLevel:           ActionL0ProcessRestart,
		NodeIdentity:          "node-1",
		DeviceIdentity:        "gpu-0",
		WorkerEpoch:           3,
		CertificationRevision: "matrix-v1",
	})
	if err != nil {
		t.Fatalf("execute allowlisted Remediation: %v", err)
	}
	if runner.plan.OperationID != operationID || runner.plan.WorkerID != workerID ||
		runner.plan.DeviceIdentity != "gpu-0" || runner.path != "/usr/local/bin/vela-process-restart" || len(runner.args) != 2 ||
		result.Detail == "" {
		t.Fatalf("allowlisted execution = runner %#v result %#v", runner, result)
	}

	_, err = executor.Execute(context.Background(), Plan{
		ActionLevel:           ActionL5NodeReboot,
		NodeIdentity:          "node-1",
		DeviceIdentity:        "gpu-0",
		WorkerEpoch:           3,
		CertificationRevision: "matrix-v1",
	})
	var failure *Failure
	if !errors.As(err, &failure) || failure.Code != FailureUncertified {
		t.Fatalf("uncertified Remediation action error = %v", err)
	}
}

func TestAllowlistedExecutorRejectsPrivilegedAndUnsafeDefinitions(t *testing.T) {
	runner := &recordingCommandRunner{}
	for name, commands := range map[string]map[ActionLevel]struct {
		Path string
		Args []string
	}{
		"BMC power cycle":   {ActionL6BMCPowerCycle: {Path: "/usr/local/bin/bmc"}},
		"quarantine action": {ActionL7Quarantine: {Path: "/usr/local/bin/quarantine"}},
		"relative path":     {ActionL0ProcessRestart: {Path: "restart"}},
		"NUL argument":      {ActionL0ProcessRestart: {Path: "/usr/local/bin/restart", Args: []string{"bad\x00arg"}}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewAllowlistedExecutor(runner, commands); err == nil {
				t.Fatal("unsafe Remediation command definition was accepted")
			}
		})
	}
}

func TestRemediationRequestValidation(t *testing.T) {
	service := &Service{}
	_, err := service.Request(context.Background(), Request{})
	if err == nil || err.Error() != "remediation service is not configured" {
		t.Fatalf("unconfigured Remediation service error = %v", err)
	}

}
