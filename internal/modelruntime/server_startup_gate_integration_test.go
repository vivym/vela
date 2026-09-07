//go:build integration && (darwin || linux)

package modelruntime_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRuntimeBackendStartupSandbox(t *testing.T) {
	image := os.Getenv("VELA_TEST_CONTAINERD_IMAGE")
	if image == "" {
		t.Skip("set VELA_TEST_CONTAINERD_IMAGE to the existing digest-pinned CPU fixture image")
	}
	digest, err := hex.DecodeString(strings.TrimPrefix(image, "sha256:"))
	if err != nil || len(digest) != 32 || image != "sha256:"+hex.EncodeToString(digest) {
		t.Fatal("startup experiment requires an exact local sha256 image ID")
	}
	var info struct {
		ID           string `json:"Id"`
		OS           string `json:"Os"`
		Architecture string
	}
	if err := json.Unmarshal(startupDocker(t, "image", "inspect", "--format", "{{json .}}", image), &info); err != nil {
		t.Fatal(err)
	}
	if info.ID != image || info.OS != "linux" || (info.Architecture != "arm64" && info.Architecture != "amd64") {
		t.Fatalf("unsupported CPU fixture image: %+v", info)
	}
	binary := filepath.Join(t.TempDir(), "modelruntime.test")
	build := exec.CommandContext(t.Context(), "go", "test", "-c", "-o", binary, ".")
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS=linux", "GOARCH="+info.Architecture)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build startup CPU fixture: %s %v", output, err)
	}
	output := startupDocker(t, "run", "--rm", "--pull", "never", "--network", "none", "--cap-drop", "ALL",
		"--cap-add", "SYS_ADMIN", "--cap-add", "SETUID", "--cap-add", "SETGID", "--cap-add", "CHOWN", "--cap-add", "DAC_OVERRIDE", "--cap-add", "KILL", "--cap-add", "SYS_PTRACE",
		"--security-opt", "seccomp=unconfined", "--pids-limit", "128", "--memory", "1g", "--cpus", "2",
		"--mount", "type=bind,src="+binary+",dst=/modelruntime.test,readonly", "--entrypoint", "/modelruntime.test", image,
		"-test.run=^TestRuntimeServerNodeStartupExchange$", "-test.v", "-test.timeout=120s")
	t.Log(string(output))
	if !strings.Contains(string(output), "--- PASS: TestRuntimeServerNodeStartupExchange ") || strings.Contains(string(output), "--- SKIP:") {
		t.Fatal("mandatory startup exchange did not pass without skips")
	}
}

func startupDocker(t *testing.T, arguments ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("Docker %s: %s %v", arguments[0], output, err)
	}
	return output
}
