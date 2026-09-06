//go:build integration && (darwin || linux)

package nodeagent

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRuntimeContainerdSandbox(t *testing.T) {
	image := os.Getenv("VELA_TEST_CONTAINERD_IMAGE")
	if image == "" {
		t.Skip("set VELA_TEST_CONTAINERD_IMAGE to the existing digest-pinned CPU fixture image")
	}
	if !strings.HasPrefix(image, "sha256:") || !runtimeContainerIDPattern.MatchString(strings.TrimPrefix(image, "sha256:")) {
		t.Fatal("containerd experiment requires an exact local sha256 image ID")
	}
	var info struct {
		ID           string `json:"Id"`
		OS           string `json:"Os"`
		Architecture string
	}
	if err := json.Unmarshal(containerdDocker(t, "image", "inspect", "--format", "{{json .}}", image), &info); err != nil {
		t.Fatal(err)
	}
	if info.ID != image || info.OS != "linux" || (info.Architecture != "arm64" && info.Architecture != "amd64") {
		t.Fatalf("unsupported CPU fixture image: %+v", info)
	}
	t.Logf("image=%s platform=%s/%s Docker=%s", info.ID, info.OS, info.Architecture,
		strings.TrimSpace(string(containerdDocker(t, "version", "--format", "{{.Server.Version}}"))))
	binary := filepath.Join(t.TempDir(), "nodeagent.test")
	build := exec.CommandContext(t.Context(), "go", "test", "-tags=integration", "-c", "-o", binary, ".")
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+info.Architecture)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build Linux CPU fixture: %s %v", output, err)
	}
	testNames := []string{
		"TestRuntimeContainerdProcessEvidence", "TestRuntimeCallerAuthenticatedMessage", "TestRuntimeCallerRejectsInvalidMessages",
		"TestRuntimeCallerDeadline", "TestRuntimeCallerProcessParser", "TestRuntimeCallerContainerCRI",
		"TestRuntimeContainerCallerCorrelation", "TestRuntimeContainerCallerRejectsNonInit",
		"TestRuntimeLaunchPlanAuthenticatesCompleteConfiguration", "TestRuntimeLaunchPlanRejectsUnboundHistory",
		"TestRuntimeLaunchPlanPreservesMemberAndAUXTopology", "TestRuntimePlannedCallerCorrelatesTrustedPod",
		"TestRuntimeExecutableObservation", "TestRuntimeExecutablePathAndExec", "TestRuntimeExecutableFileBounds",
		"TestRuntimeImageLayerExecutableIdentity",
		"TestRuntimeImageTargetBounds", "TestRuntimeImageContentBounds", "TestRuntimeImageFileResolution",
	}
	container := strings.TrimSpace(string(containerdDocker(t, "create", "--pull", "never", "--network", "none", "--privileged", "--cgroupns", "private",
		"--pids-limit", "256", "--memory", "1g", "--cpus", "2", "--env", "VELA_TEST_CONTAINERD_SANDBOX=1",
		"--mount", "type=bind,src="+binary+",dst=/nodeagent.test,readonly", "--entrypoint", "/nodeagent.test", image,
		"-test.run=^("+strings.Join(testNames, "|")+")$", "-test.v", "-test.timeout=120s")))
	if !runtimeContainerIDPattern.MatchString(container) {
		t.Fatalf("Docker returned an invalid fixture container ID: %q", container)
	}
	t.Cleanup(func() { containerdDocker(t, "rm", "--force", "--volumes", container) })
	output := containerdDocker(t, "start", "--attach", container)
	t.Log(string(output))
	for _, name := range testNames {
		if !strings.Contains(string(output), "--- PASS: "+name+" ") {
			t.Fatalf("selected CPU test did not pass: %s", name)
		}
	}
	if strings.Contains(string(output), "--- SKIP:") ||
		strings.TrimSpace(string(containerdDocker(t, "inspect", "--format", "{{.State.ExitCode}}", container))) != "0" {
		t.Fatal("real containerd experiment did not complete without skips")
	}
}

func containerdDocker(t *testing.T, arguments ...string) []byte {
	t.Helper()
	// Cleanup must work after the enclosing test context is canceled.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("Docker %s: %s %v", arguments[0], output, err)
	}
	return output
}
