//go:build integration && (darwin || linux)

package nodeagent

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const runtimeImageStateResetPhase = "VELA_TEST_IMAGE_STATE_RESET_PHASE"
const runtimeImageStateResetRoot = "/vela-image-state-reset"

func runRuntimeImageStateResetSandbox(t *testing.T, image, binary, maintenanceBinary string) {
	t.Helper()
	persistent := t.TempDir()
	if err := os.Mkdir(filepath.Join(persistent, "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	create := func(phase string) string {
		container := strings.TrimSpace(string(containerdDocker(t, "create", "--pull", "never", "--network", "none", "--privileged", "--cgroupns", "private",
			"--pids-limit", "256", "--memory", "1g", "--cpus", "2", "--env", "VELA_TEST_CONTAINERD_SANDBOX=1",
			"--env", runtimeImageStateResetPhase+"="+phase, "--env", "VELA_TEST_NODE_AGENT_BINARY=/vela-node-agent",
			"--mount", "type=bind,src="+persistent+",dst="+runtimeImageStateResetRoot,
			"--mount", "type=bind,src="+maintenanceBinary+",dst=/vela-node-agent,readonly",
			"--mount", "type=bind,src="+binary+",dst=/nodeagent.test,readonly", "--entrypoint", "/nodeagent.test", image,
			"-test.run=^TestRuntimeImageStateReset$", "-test.v", "-test.timeout=60s")))
		if !runtimeContainerIDPattern.MatchString(container) {
			t.Fatalf("invalid state reset container ID: %q", container)
		}
		return container
	}
	seed := create("seed")
	seedRemoved := false
	t.Cleanup(func() {
		if !seedRemoved {
			containerdDocker(t, "rm", "--force", "--volumes", seed)
		}
	})
	containerdDocker(t, "start", seed)
	deadline := time.NewTimer(25 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		data, err := os.ReadFile(filepath.Join(persistent, "seed-ready.json"))
		if err == nil && json.Valid(data) {
			break
		}
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("state reset seed did not acknowledge retained resources: %s", containerdDocker(t, "logs", seed))
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		}
	}
	if strings.TrimSpace(string(containerdDocker(t, "inspect", "--format", "{{.State.Running}}", seed))) != "true" {
		t.Fatalf("state reset seed exited before forced loss: %s", containerdDocker(t, "logs", seed))
	}
	containerdDocker(t, "kill", "--signal", "KILL", seed)
	if strings.TrimSpace(string(containerdDocker(t, "wait", seed))) != "137" ||
		strings.TrimSpace(string(containerdDocker(t, "inspect", "--format", "{{.State.OOMKilled}}", seed))) != "false" {
		t.Fatal("state reset seed was not deliberately terminated by SIGKILL")
	}
	seedOutput := string(containerdDocker(t, "logs", seed))
	if strings.Contains(seedOutput, "--- FAIL:") || strings.Contains(seedOutput, "--- SKIP:") || strings.Contains(seedOutput, "WARNING: DATA RACE") {
		t.Fatalf("state reset seed failed before termination: %s", seedOutput)
	}
	containerdDocker(t, "rm", "--volumes", seed)
	seedRemoved = true
	t.Logf("removed SIGKILL seed sandbox %s; retained only fixture receipt and containerd root", seed)

	recovery := create("recover")
	t.Cleanup(func() { containerdDocker(t, "rm", "--force", "--volumes", recovery) })
	if recovery == seed {
		t.Fatal("state reset reused its original sandbox")
	}
	output := string(containerdDocker(t, "start", "--attach", recovery))
	t.Log(output)
	if !strings.Contains(output, "--- PASS: TestRuntimeImageStateReset (") || strings.Contains(output, "--- SKIP:") ||
		strings.Contains(output, "WARNING: DATA RACE") ||
		strings.TrimSpace(string(containerdDocker(t, "inspect", "--format", "{{.State.ExitCode}}", recovery))) != "0" {
		t.Fatal("fresh sandbox did not prove image recovery after volatile state loss")
	}
}
