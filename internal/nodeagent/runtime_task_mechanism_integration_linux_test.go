//go:build integration && linux

package nodeagent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
)

func verifyRuntimeTaskMechanism(t *testing.T, fixture *containerdProcessFixture, observer *RuntimeContainerObserver, target RuntimeContainerTarget, caller *RuntimeCaller, explicit bool) {
	t.Helper()
	state := filepath.Join(fixture.root, "state")
	bundle := filepath.Join(state, "io.containerd.runtime.v2.task", "k8s.io", target.ContainerID)
	launch, err := observer.ObserveTaskLaunch(t.Context(), state, target, caller)
	if err != nil {
		t.Fatal(err)
	}
	policy := runtimeTaskPolicyFixture()
	if err := launch.CheckRuntimeMechanism(policy); (err == nil) != explicit {
		t.Fatalf("explicit=%v did not select the expected mechanism policy result: %v", explicit, err)
	}
	sandboxBundle := filepath.Join(state, "io.containerd.runtime.v2.task", "k8s.io", target.SandboxID)
	if launch.ShimBundleID != target.SandboxID {
		t.Fatal("shim source does not identify the actual sandbox")
	}
	files := []struct {
		name, path  string
		observation RuntimeTaskFileObservation
	}{
		{"options", filepath.Join(bundle, "options.json"), launch.OptionsFile},
		{"runtime", filepath.Join(bundle, "runtime"), launch.RuntimeFile},
		{"shim-binary", filepath.Join(sandboxBundle, "shim-binary-path"), launch.ShimFile},
		{"sandbox-id", filepath.Join(bundle, "sandbox"), launch.SandboxFile},
		{"task-bootstrap", filepath.Join(bundle, "bootstrap.json"), launch.TaskBootstrapFile},
		{"shim-bootstrap", filepath.Join(sandboxBundle, "bootstrap.json"), launch.ShimBootstrapFile},
	}
	for _, file := range files {
		name, observed := file.name, file.observation
		original, err := os.ReadFile(file.path)
		if err != nil || observed.Bytes != int64(len(original)) || observed.Digest != sha256.Sum256(original) {
			t.Fatalf("%s is not the actual task-created file: %v", name, err)
		}
		for _, fault := range []string{"missing", "writable", "wrong-owner", "symlink", "hardlink", "fifo", "oversize"} {
			t.Run("mechanism-"+name+"-"+fault, func(t *testing.T) {
				path := file.path
				defer func() {
					_ = os.Remove(path)
					if err := os.WriteFile(path, original, 0o600); err != nil {
						t.Error(err)
					}
				}()
				var err error
				switch fault {
				case "missing":
					err = os.Remove(path)
				case "writable":
					err = os.Chmod(path, 0o666)
				case "wrong-owner":
					err = os.Chown(path, 65532, 65532)
				case "oversize":
					err = os.WriteFile(path, make([]byte, (64<<10)+1), 0o600)
				default:
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
					switch fault {
					case "symlink":
						err = os.Symlink(filepath.Join(bundle, "init.pid"), path)
					case "hardlink":
						err = os.Link(filepath.Join(bundle, "init.pid"), path)
					case "fifo":
						err = unix.Mkfifo(path, 0o600)
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				if result, err := observer.ObserveTaskLaunch(t.Context(), state, target, caller); err == nil || result != nil {
					t.Fatal("untrusted runtime mechanism file produced a launch observation")
				}
			})
		}
	}
	for _, mismatch := range []string{"sandbox", "bootstrap.json"} {
		t.Run("mechanism-mismatched-"+mismatch, func(t *testing.T) {
			path := filepath.Join(bundle, mismatch)
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := os.WriteFile(path, original, 0o600); err != nil {
					t.Error(err)
				}
			}()
			data := []byte(strings.Repeat("0", 64))
			if mismatch == "bootstrap.json" {
				data = []byte(`{"version":3,"address":"unix:///another/shim","protocol":"ttrpc"}`)
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if result, err := observer.ObserveTaskLaunch(t.Context(), state, target, caller); !errors.Is(err, ErrRuntimeTaskMechanism) || result != nil {
				t.Fatal("task borrowed a different sandbox/shim")
			}
		})
	}
	t.Run("mechanism-runtime-disagrees-with-options", func(t *testing.T) {
		path := filepath.Join(bundle, "runtime")
		original, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Error(err)
			}
		}()
		if err := os.WriteFile(path, []byte("/different/runc"), 0o600); err != nil {
			t.Fatal(err)
		}
		if result, err := observer.ObserveTaskLaunch(t.Context(), state, target, caller); !errors.Is(err, ErrRuntimeTaskMechanism) || result != nil {
			t.Fatal("runtime record contradicted options but produced an observation")
		}
	})
	if explicit {
		t.Run("mechanism-hook-rejected", func(t *testing.T) {
			path := filepath.Join(bundle, "config.json")
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := os.WriteFile(path, original, 0o644); err != nil {
					t.Error(err)
				}
			}()
			configuration, err := launch.Configuration()
			if err != nil {
				t.Fatal(err)
			}
			configuration.Hooks = &specs.Hooks{CreateRuntime: []specs.Hook{{Path: "/unapproved-hook"}}}
			encoded, err := json.Marshal(configuration)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, encoded, 0o644); err != nil {
				t.Fatal(err)
			}
			changed, err := observer.ObserveTaskLaunch(t.Context(), state, target, caller)
			if err != nil {
				t.Fatal(err)
			}
			if err := changed.CheckRuntimeMechanism(policy); !errors.Is(err, ErrRuntimeTaskMechanism) {
				t.Fatal("hook-bearing bundle passed the content policy")
			}
			if bytes.Equal(encoded, original) {
				t.Fatal("hook counterexample did not change the bundle")
			}
		})
	}
	t.Logf("actual mechanism options=%x runtime=%q shim=%q explicit_policy_match=%v; 42 file negatives, mismatched sandbox/bootstrap and contradictory runtime record rejected",
		launch.OptionsFile.Digest, launch.RuntimeBinaryPath, launch.ShimBinaryPath, explicit)
}
