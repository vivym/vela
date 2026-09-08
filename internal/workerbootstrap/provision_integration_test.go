//go:build integration

package workerbootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Build the CPU fixture from this checkout only. No base image, registry, GPU,
// containerd installation, or host workload directory is needed.
func TestProtectedProvisioningSandbox(t *testing.T) {
	if os.Getenv("VELA_TEST_PROVISION_SANDBOX") != "1" {
		t.Skip("set VELA_TEST_PROVISION_SANDBOX=1 to run isolated Linux provisioning checks")
	}
	docker := func(arguments ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		command := exec.CommandContext(ctx, "docker", arguments...)
		var stderr bytes.Buffer
		command.Stderr = &stderr
		output, err := command.Output()
		if err != nil {
			t.Fatalf("Docker %s: %s %s %v", arguments[0], output, stderr.Bytes(), err)
		}
		return output
	}
	var server struct {
		Version string
		Arch    string
		Os      string
	}
	if err := json.Unmarshal(docker("version", "--format", "{{json .Server}}"), &server); err != nil {
		t.Fatal(err)
	}
	if server.Os != "linux" || server.Arch != "arm64" && server.Arch != "amd64" {
		t.Fatalf("unsupported CPU server: %+v", server)
	}
	directory := t.TempDir()
	if err := os.MkdirAll(filepath.Join(directory, "rootfs", "tmp"), 0o1777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(directory, "rootfs", "tmp"), os.ModeSticky|0o777); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(directory, "rootfs", "workerbootstrap.test")
	buildArgs := []string{"test", "-c", "-o", binary}
	cgo := "0"
	race := os.Getenv("VELA_TEST_PROVISION_RACE")
	if race != "" && race != "1" {
		t.Fatal("VELA_TEST_PROVISION_RACE must be empty or 1")
	}
	if race == "1" {
		if runtime.GOOS != "linux" || runtime.GOARCH != server.Arch {
			t.Fatal("static race provisioning checks require a matching native Linux toolchain")
		}
		cgo = "1"
		buildArgs = append(buildArgs, "-race", "-ldflags", "-linkmode external -extldflags=-static")
	}
	build := exec.CommandContext(t.Context(), "go", append(buildArgs, ".")...)
	build.Env = append(os.Environ(), "CGO_ENABLED="+cgo, "GOOS=linux", "GOARCH="+server.Arch)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Linux provisioning fixture: %s %v", output, err)
	}
	if err := os.WriteFile(filepath.Join(directory, "Dockerfile"), []byte("FROM scratch\nCOPY rootfs /\nENTRYPOINT [\"/workerbootstrap.test\"]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	image := strings.TrimSpace(string(docker("build", "--quiet", "--network", "none", directory)))
	if !strings.HasPrefix(image, "sha256:") || len(image) != 71 {
		t.Fatalf("build returned no immutable CPU image: %q", image)
	}
	t.Logf("CPU image=%s Docker=%s platform=%s/%s race=%t", image, server.Version, server.Os, server.Arch, race == "1")
	names := []string{"TestProvisionProtectsOriginalEvidenceThroughOwnershipTransfer", "TestProvisionNeverAdoptsOrRetriesExistingState",
		"TestProvisionConcurrentFirstUse", "TestProvisionProcessExitKeepsFirstUseConsumed", "TestProvisionRejectsUnsafeRootsBeforeClaim", "TestProvisionDetectsChangedTransfer",
		"TestInspectProvisionedJournalsRetainsLocksAndOriginalEvidence", "TestInspectProvisionedJournalsRejectsInterruptedHandover",
		"TestInspectProvisionedJournalsRejectsChangedStateAndHistory", "TestInspectProvisionedJournalsRechecksAfterLookup",
		"TestInspectProvisionedJournalsRejectsLiveOwnersWithoutLeakingLocks"}
	container := strings.TrimSpace(string(docker("create", "--pull", "never", "--network", "none", "--privileged", "--pids-limit", "128", "--memory", "512m", "--cpus", "2",
		image, "-test.run=^("+strings.Join(names, "|")+")$", "-test.v", "-test.timeout=90s")))
	if len(container) != 64 || strings.ContainsAny(container, " /\n\r") {
		t.Fatalf("invalid test container: %q", container)
	}
	t.Cleanup(func() { docker("rm", "--force", "--volumes", container) })
	output := string(docker("start", "--attach", container))
	t.Log(output)
	for _, name := range names {
		if !strings.Contains(output, "--- PASS: "+name+" ") {
			t.Fatalf("mandatory test did not pass: %s", name)
		}
	}
	if strings.Contains(output, "--- SKIP:") || strings.TrimSpace(string(docker("inspect", "--format", "{{.State.ExitCode}}", container))) != "0" {
		t.Fatal("CPU provisioning checks did not complete without skips")
	}
}
