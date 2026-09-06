//go:build integration && (darwin || linux)

package modelruntime_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
)

type lifecycleContainerState struct {
	Status    string
	Running   bool
	ExitCode  int
	OOMKilled bool
}

func TestRuntimePIDNamespaceContainsBackendWriters(t *testing.T) {
	image := os.Getenv("VELA_TEST_RUNTIME_CONTAINER_IMAGE")
	if image == "" {
		t.Skip("set VELA_TEST_RUNTIME_CONTAINER_IMAGE to an existing digest-pinned Linux CPU image")
	}
	if !strings.HasPrefix(image, "sha256:") && !strings.Contains(image, "@sha256:") {
		t.Fatal("the lifecycle experiment requires a digest-pinned image")
	}
	var metadata struct {
		ID           string `json:"Id"`
		OS           string `json:"Os"`
		Architecture string
	}
	if err := json.Unmarshal(lifecycleDocker(t, "image", "inspect", "--format", "{{json .}}", image), &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata.OS != "linux" || metadata.Architecture != "arm64" && metadata.Architecture != "amd64" {
		t.Fatalf("unsupported experiment image: %+v", metadata)
	}
	t.Logf("image=%s platform=%s/%s daemon=%s", metadata.ID, metadata.OS, metadata.Architecture,
		strings.TrimSpace(string(lifecycleDocker(t, "version", "--format", "{{.Server.Version}}"))))
	binary := filepath.Join(t.TempDir(), "runtime.test")
	build := exec.CommandContext(t.Context(), "go", "test", "-c", "-o", binary, ".")
	build.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+metadata.Architecture, "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CPU process helper: %s %v", output, err)
	}
	t.Run("fresh-journal-positive-control", func(t *testing.T) {
		volume := strings.TrimSpace(string(lifecycleDocker(t, "volume", "create")))
		t.Cleanup(func() { lifecycleDocker(t, "volume", "rm", volume) })
		initializer := createLifecycleContainer(t, image, binary, volume, "init-volume", "idle")
		lifecycleDocker(t, "start", "--attach", initializer)
		waitLifecycleContainerExit(t, initializer, 0)
		container := createLifecycleContainer(t, image, binary, volume, "fresh-replacement", "idle")
		lifecycleDocker(t, "start", "--attach", container)
		waitLifecycleContainerExit(t, container, 0)
		assertLifecycleReplacement(t, readLifecycleSnapshot(t, container), nil, "fresh")
	})
	for _, phase := range []string{"initializing", "idle", "admitted", "closed-idle"} {
		for _, mode := range []string{"owner", "wrapper"} {
			t.Run(phase+"/"+mode, func(t *testing.T) {
				volume := strings.TrimSpace(string(lifecycleDocker(t, "volume", "create")))
				t.Cleanup(func() { lifecycleDocker(t, "volume", "rm", volume) })
				initializer := createLifecycleContainer(t, image, binary, volume, "init-volume", phase)
				lifecycleDocker(t, "start", "--attach", initializer)
				if state := readLifecycleContainerState(t, initializer); state.ExitCode != 0 || state.Running {
					t.Fatalf("private evidence volume initialization failed: %+v", state)
				}
				container := createLifecycleContainer(t, image, binary, volume, mode, phase)
				lifecycleDocker(t, "start", container)
				before := waitLifecycleSnapshot(t, container, func(files map[string][]byte) bool {
					return files["owner-ready"] != nil && len(files["writer-data"]) > 0
				})
				owner := decodeLifecycleIdentity(t, before["owner.json"])
				driver := decodeLifecycleIdentity(t, before["driver.json"])
				writer := decodeLifecycleIdentity(t, before["writer.json"])
				if owner.Namespace != driver.Namespace || owner.Namespace != writer.Namespace || owner.BootID != writer.BootID ||
					driver.ParentPID != owner.PID || writer.ParentPID != driver.PID || driver.GroupID != driver.PID ||
					writer.GroupID != writer.PID || writer.GroupID == driver.GroupID || writer.GroupID == owner.GroupID {
					t.Fatalf("experiment did not create an escaped writer: owner=%+v driver=%+v writer=%+v", owner, driver, writer)
				}
				if mode == "owner" && owner.PID != 1 || mode == "wrapper" && (owner.PID == 1 || owner.ParentPID != 1) {
					t.Fatalf("incorrect Runtime PID namespace ownership: mode=%s owner=%+v", mode, owner)
				}
				if mode == "wrapper" {
					wrapper := decodeLifecycleIdentity(t, before["wrapper.json"])
					if wrapper.PID != 1 || wrapper.Namespace != owner.Namespace {
						t.Fatalf("wrapper is not the namespace init: %+v", wrapper)
					}
				}
				assertLifecycleJournal(t, before, phase)
				before = waitLifecycleSnapshot(t, container, func(files map[string][]byte) bool {
					return len(files["writer-data"]) > len(before["writer-data"])
				})
				if phase == "closed-idle" {
					if before["owner-server-closed"] == nil {
						t.Fatal("owner did not complete graceful Close before the observation")
					}
					replacement := createLifecycleContainer(t, image, binary, volume, "replacement", phase)
					lifecycleDocker(t, "start", "--attach", replacement)
					waitLifecycleContainerExit(t, replacement, 0)
					replaced := readLifecycleSnapshot(t, container)
					assertLifecycleReplacement(t, replaced, before, phase)
					if !bytes.Equal(replaced["journal/"+durableStateFileName], before["journal/"+durableStateFileName]) {
						t.Fatal("separate-namespace replacement changed execution history")
					}
					identity := decodeLifecycleIdentity(t, replaced["replacement-owner.json"])
					if identity.Namespace == owner.Namespace || identity.PID != 1 {
						t.Fatalf("replacement was not in a separate PID 1-owned namespace: %+v", identity)
					}
					lifecycleDocker(t, "exec", "--env", lifecycleProcessMode+"=inspect-writer",
						container, "/runtime.test", "-test.run=^TestRuntimeLifecycleProcessHelper$")
					before = waitLifecycleSnapshot(t, container, func(files map[string][]byte) bool {
						return len(files["writer-data"]) > len(replaced["writer-data"])
					})
					t.Log("Close returned while the old owner and writer remained alive; durable incarnation blocked drivers in the new PID namespace")
				}
				// PID 1 may kill the gate-writing exec process before Docker collects
				// its status. The exact owner/container exit below is authoritative.
				gateOutput, gateErr := runLifecycleDocker("exec", "--env", lifecycleProcessMode+"=exit-owner",
					container, "/runtime.test", "-test.run=^TestRuntimeLifecycleProcessHelper$")
				if mode == "owner" {
					waitLifecycleContainerExit(t, container, 72)
					after := readLifecycleSnapshot(t, container)
					assertLifecycleJournal(t, after, phase)
					assertLifecycleStableWrites(t, container, after)
					if after["writer-stopped"] != nil {
						t.Fatal("writer used its cooperative stop path instead of namespace termination")
					}
					t.Logf("Runtime PID 1 exited 72; escaped writer stopped: owner=%+v writer=%+v bytes=%d", owner, writer, len(after["writer-data"]))
					return
				}
				if gateErr != nil {
					t.Fatalf("request owner exit: %s %v", gateOutput, gateErr)
				}
				after := waitLifecycleSnapshot(t, container, func(files map[string][]byte) bool {
					return files["owner-exited.json"] != nil
				})
				assertLifecycleJournal(t, after, phase)
				state := readLifecycleContainerState(t, container)
				if !state.Running || state.OOMKilled {
					t.Fatalf("positive-control wrapper failed: %+v", state)
				}
				current := decodeLifecycleIdentity(t, lifecycleDocker(t, "exec", "--env", lifecycleProcessMode+"=inspect-writer",
					container, "/runtime.test", "-test.run=^TestRuntimeLifecycleProcessHelper$"))
				if current.PID != writer.PID || current.StartTicks != writer.StartTicks || current.Namespace != writer.Namespace {
					t.Fatalf("positive control observed a different writer: before=%+v current=%+v", writer, current)
				}
				grown := waitLifecycleSnapshot(t, container, func(files map[string][]byte) bool {
					return len(files["writer-data"]) > len(after["writer-data"])
				})
				lifecycleDocker(t, "exec", "--env", lifecycleProcessMode+"=replacement",
					container, "/runtime.test", "-test.run=^TestRuntimeLifecycleProcessHelper$")
				replaced := readLifecycleSnapshot(t, container)
				assertLifecycleReplacement(t, replaced, before, phase)
				if !bytes.Equal(replaced["journal/"+durableStateFileName], before["journal/"+durableStateFileName]) {
					t.Fatal("replacement startup or shutdown changed execution history")
				}
				waitLifecycleSnapshot(t, container, func(files map[string][]byte) bool {
					return len(files["writer-data"]) > len(replaced["writer-data"])
				})
				lifecycleDocker(t, "exec", "--env", lifecycleProcessMode+"=inspect-writer",
					container, "/runtime.test", "-test.run=^TestRuntimeLifecycleProcessHelper$")
				lifecycleDocker(t, "exec", "--env", lifecycleProcessMode+"=stop-writer",
					container, "/runtime.test", "-test.run=^TestRuntimeLifecycleProcessHelper$")
				stopped := waitLifecycleSnapshot(t, container, func(files map[string][]byte) bool { return files["writer-stopped"] != nil })
				assertLifecycleStableWrites(t, container, stopped)
				_, _ = runLifecycleDocker("exec", "--env", lifecycleProcessMode+"=stop-wrapper",
					container, "/runtime.test", "-test.run=^TestRuntimeLifecycleProcessHelper$")
				waitLifecycleContainerExit(t, container, 0)
				if readLifecycleSnapshot(t, container)["stop-wrapper"] == nil {
					t.Fatal("wrapper exited without the requested stop gate")
				}
				t.Logf("Runtime child exited 72; exact escaped writer survived: owner=%+v writer=%+v bytes=%d->%d",
					owner, current, len(after["writer-data"]), len(grown["writer-data"]))
			})
		}
	}
}

func createLifecycleContainer(t *testing.T, image, binary, volume, mode, phase string) string {
	t.Helper()
	args := []string{"create", "--pull=never", "--network=none", "--read-only", "--cap-drop=ALL",
		"--security-opt=no-new-privileges", "--pids-limit=128", "--memory=256m",
		"--mount", "type=bind,source=" + binary + ",target=/runtime.test,readonly",
		"--mount", "type=volume,source=" + volume + ",target=" + lifecycleProcessRoot,
		"--tmpfs", "/tmp:rw,exec,nosuid,nodev,size=64m,mode=1777",
		"--env", lifecycleProcessMode + "=" + mode, "--env", "VELA_TEST_LIFECYCLE_PHASE=" + phase,
		"--entrypoint", "/runtime.test"}
	if mode == "init-volume" {
		args = append(args, "--user=0:0", "--cap-add=CHOWN")
	} else {
		args = append(args, "--user=65534:65534")
	}
	args = append(args, image, "-test.run=^TestRuntimeLifecycleProcessHelper$")
	container := strings.TrimSpace(string(lifecycleDocker(t, args...)))
	t.Cleanup(func() { lifecycleDocker(t, "rm", "--force", container) })
	return container
}

func runLifecycleDocker(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, "docker", args...).CombinedOutput()
}

func lifecycleDocker(t *testing.T, args ...string) []byte {
	t.Helper()
	output, err := runLifecycleDocker(args...)
	if err != nil {
		t.Fatalf("docker %v: %s %v", args, output, err)
	}
	return output
}

func readLifecycleContainerState(t *testing.T, container string) lifecycleContainerState {
	t.Helper()
	var state lifecycleContainerState
	if err := json.Unmarshal(lifecycleDocker(t, "inspect", "--format", "{{json .State}}", container), &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func waitLifecycleContainerExit(t *testing.T, container string, wantExit int) {
	t.Helper()
	if exited := strings.TrimSpace(string(lifecycleDocker(t, "wait", container))); exited != fmt.Sprint(wantExit) {
		t.Fatalf("container exit=%s want=%d logs=%s", exited, wantExit, lifecycleDocker(t, "logs", container))
	}
	if state := readLifecycleContainerState(t, container); state.Running || state.Status != "exited" || state.ExitCode != wantExit || state.OOMKilled {
		t.Fatalf("unexpected post-exit container state: %+v", state)
	}
}

func readLifecycleSnapshot(t *testing.T, container string) map[string][]byte {
	t.Helper()
	archive := tar.NewReader(bytes.NewReader(lifecycleDocker(t, "cp", container+":"+lifecycleProcessRoot+"/.", "-")))
	files := make(map[string][]byte)
	for {
		header, err := archive.Next()
		if err == io.EOF {
			return files
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		if header.Size > 12<<20 {
			t.Fatalf("unexpectedly large experiment file: %s", header.Name)
		}
		data, err := io.ReadAll(archive)
		if err != nil {
			t.Fatal(err)
		}
		files[strings.TrimPrefix(header.Name, "./")] = data
	}
}

func waitLifecycleSnapshot(t *testing.T, container string, ready func(map[string][]byte) bool) map[string][]byte {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		files := readLifecycleSnapshot(t, container)
		if ready(files) {
			return files
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("container evidence did not reach the expected state: %s", lifecycleDocker(t, "logs", container))
	return nil
}

func decodeLifecycleIdentity(t *testing.T, document []byte) lifecycleProcessIdentity {
	t.Helper()
	var identity lifecycleProcessIdentity
	if err := json.Unmarshal(document, &identity); err != nil {
		t.Fatal(err)
	}
	if identity.PID <= 0 || identity.GroupID <= 0 || identity.StartTicks == "" || identity.Namespace == "" || identity.BootID == "" {
		t.Fatalf("incomplete process identity: %+v", identity)
	}
	return identity
}

func assertLifecycleJournal(t *testing.T, files map[string][]byte, phase string) {
	t.Helper()
	var state struct {
		SchemaVersion int `json:"schema_version"`
		Executions    []struct {
			Drain json.RawMessage `json:"drain"`
		} `json:"executions"`
	}
	if err := json.Unmarshal(files["journal/"+durableStateFileName], &state); err != nil {
		t.Fatal(err)
	}
	want := 0
	if phase == "admitted" {
		want = 1
	}
	if state.SchemaVersion != 6 || len(state.Executions) != want || want == 1 && string(state.Executions[0].Drain) != "null" {
		t.Fatalf("incorrect execution history for %s: %+v", phase, state)
	}
	if phase == "initializing" && files["server-ready"] != nil || phase != "initializing" && files["server-ready"] == nil {
		t.Fatalf("incorrect initialization boundary for %s", phase)
	}
}

func assertLifecycleStableWrites(t *testing.T, container string, stopped map[string][]byte) {
	t.Helper()
	if len(stopped["writer-data"]) == 0 {
		t.Fatal("stopped writer lost its previously observed output")
	}
	for range 3 {
		time.Sleep(50 * time.Millisecond)
		if current := readLifecycleSnapshot(t, container); !bytes.Equal(current["writer-data"], stopped["writer-data"]) {
			t.Fatal("writer continued after the expected stop boundary")
		}
	}
}

func assertLifecycleReplacement(t *testing.T, files, before map[string][]byte, phase string) {
	t.Helper()
	var journal modelruntime.ExecutionJournalStatus
	if err := json.Unmarshal(files["replacement-journal.json"], &journal); err != nil {
		t.Fatal(err)
	}
	wantPending, wantDrivers := 0, 0
	if phase == "admitted" {
		wantPending = 1
	}
	wantLifecycle := modelruntime.BackendLifecycleUnresolved
	if phase == "fresh" {
		wantDrivers, wantLifecycle = 2, modelruntime.BackendLifecycleUnstarted
	}
	if journal.PendingExecutions != wantPending || journal.RetainedExecutions != wantPending || files["replacement-closed"] == nil ||
		journal.BackendLifecycle.State != wantLifecycle {
		t.Fatalf("replacement did not recover the original journal: %+v", journal)
	}
	drivers := 0
	for _, component := range []string{"ENCODER", "VAE_DECODER"} {
		if document := files["replacement-"+component+".json"]; document != nil {
			decodeLifecycleIdentity(t, document)
			drivers++
		}
	}
	if drivers != wantDrivers {
		t.Fatalf("replacement startup did not match durable backend ownership: phase=%s drivers=%d want=%d", phase, drivers, wantDrivers)
	}
	epochs := 0
	for name, data := range files {
		if !strings.HasPrefix(name, "epochs/") || !strings.HasSuffix(name, ".epoch") {
			continue
		}
		current, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		if err != nil || current < 10 {
			t.Fatalf("invalid persisted replacement epoch: %s %v", data, err)
		}
		if previous := before[name]; previous != nil {
			prior, err := strconv.ParseInt(strings.TrimSpace(string(previous)), 10, 64)
			if err != nil || current <= prior {
				t.Fatalf("replacement reset or reused an epoch: prior=%s current=%s error=%v", previous, data, err)
			}
		}
		epochs++
	}
	if epochs != 2 {
		t.Fatalf("replacement did not retain both Runtime epochs: %d", epochs)
	}
	t.Logf("reopened original journal: phase=%s pending_executions=%d replacement_drivers=%d", phase, journal.PendingExecutions, drivers)
}
