package workerbootstrap

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageworkeragent"
	"golang.org/x/sys/unix"
)

type provisionHelperInput struct {
	Config   Config
	Expected Result
	NodeRoot string
	Boundary string
}

func provisionFixture(t *testing.T) (Config, string, *fakeAuthority) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("requires Linux root and isolated CPU mount namespace")
	}
	config, registry := bootstrapFixture(t)
	directory := t.TempDir()
	mustDo(t, os.Chmod(directory, 0o700))
	config.ScratchDirectory = filepath.Join(directory, "scratch")
	return config, directory, registry
}

func TestProvisionProtectsOriginalEvidenceThroughOwnershipTransfer(t *testing.T) {
	config, directory, registry := provisionFixture(t)
	result, err := provision(t.Context(), config, directory, registry, func(phase string) error {
		if phase == "journals-prepared" || phase == "origin-durable" || phase == "after-handover:." {
			command := provisionHelper(t, "denied", provisionHelperInput{Config: config, NodeRoot: directory}, true)
			if output, err := command.CombinedOutput(); err != nil {
				return fmt.Errorf("non-root access at %s: %s: %w", phase, output, err)
			}
		}
		return nil
	})
	mustDo(t, err)
	if registry.claimCalls != 1 || registry.receiptCalls != 1 || result.RequestID != registry.claim.RequestID {
		t.Fatal("protected provisioning changed first-use authority")
	}
	originWire, err := os.ReadFile(filepath.Join(directory, provisionOriginName))
	mustDo(t, err)
	var origin provisionOrigin
	mustDo(t, json.Unmarshal(originWire, &origin))
	if sha256.Sum256(originWire) != result.OriginDigest || origin.Intent.ID != result.ProvisionID ||
		origin.Result.RequestID != result.RequestID || len(origin.Files) != 15 {
		t.Fatal("independent origin does not bind the full actual prepared tree")
	}
	for _, name := range []string{provisionIntentName, provisionOriginName, provisionDoneName} {
		info, err := os.Stat(filepath.Join(directory, name))
		mustDo(t, err)
		if !privateFile(info) {
			t.Fatalf("Node evidence ownership changed: %s", name)
		}
	}
	// A workload sees only the scratch bind mount. Root's private parent and its
	// records are excluded even though all scratch files now belong to UID 10001.
	exposed, err := os.MkdirTemp("/tmp", "vela-provision-exposed-")
	mustDo(t, err)
	t.Cleanup(func() { _ = os.Remove(exposed) })
	if err := unix.Mount(config.ScratchDirectory, exposed, "", unix.MS_BIND, ""); err != nil {
		t.Fatalf("mandatory CPU scratch bind mount: %v", err)
	}
	t.Cleanup(func() { mustDo(t, unix.Unmount(exposed, 0)) })
	config.ScratchDirectory = exposed
	command := provisionHelper(t, "recover", provisionHelperInput{Config: config, Expected: origin.Result, NodeRoot: directory}, true)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("transferred journals could not recover through workload mount: %s %v", output, err)
	}
	after, err := os.ReadFile(filepath.Join(directory, provisionOriginName))
	mustDo(t, err)
	if !bytes.Equal(after, originWire) {
		t.Fatal("workload changed independent Node evidence")
	}
}

func TestProvisionNeverAdoptsOrRetriesExistingState(t *testing.T) {
	for _, phase := range []string{"intent-durable", "roots-created", "journals-prepared", "origin-durable",
		"after-handover:runtime-admission/execution-admission.lock", "after-handover:.", "handover-durable"} {
		t.Run(phase, func(t *testing.T) {
			config, directory, registry := provisionFixture(t)
			interrupted := errors.New("injected provisioning interruption")
			result, err := provision(t.Context(), config, directory, registry, func(current string) error {
				if current == phase {
					return interrupted
				}
				return nil
			})
			if !errors.Is(err, interrupted) || result != (ProvisionedJournals{}) {
				t.Fatalf("interruption returned success: %+v %v", result, err)
			}
			before := snapshotFiles(t, directory)
			claimCalls, receiptCalls := registry.claimCalls, registry.receiptCalls
			result, err = Provision(t.Context(), config, directory, registry)
			if !errors.Is(err, ErrIncomplete) || result != (ProvisionedJournals{}) ||
				registry.claimCalls != claimCalls || registry.receiptCalls != receiptCalls || !reflect.DeepEqual(before, snapshotFiles(t, directory)) {
				t.Fatalf("retry adopted or altered retained state: %+v %v", result, err)
			}
		})
	}
}

func TestProvisionConcurrentFirstUse(t *testing.T) {
	config, directory, registry := provisionFixture(t)
	var wait sync.WaitGroup
	successes := make(chan ProvisionedJournals, 8)
	for range 8 {
		wait.Go(func() {
			if result, err := Provision(t.Context(), config, directory, registry); err == nil {
				successes <- result
			}
		})
	}
	wait.Wait()
	close(successes)
	if len(successes) != 1 || registry.claimCalls != 1 || registry.receiptCalls != 1 {
		t.Fatalf("concurrent provision: successes=%d claims=%d receipts=%d", len(successes), registry.claimCalls, registry.receiptCalls)
	}
}

func TestProvisionProcessExitKeepsFirstUseConsumed(t *testing.T) {
	for _, phase := range []string{"intent-durable", "journals-prepared", "origin-durable",
		"after-handover:runtime-admission/execution-admission.lock", "handover-durable"} {
		t.Run(phase, func(t *testing.T) {
			config, directory, registry := provisionFixture(t)
			command := provisionHelper(t, "crash", provisionHelperInput{Config: config, NodeRoot: directory, Boundary: phase}, false)
			output, err := command.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 83 {
				t.Fatalf("helper did not exit at retained boundary: %s %v", output, err)
			}
			before := snapshotFiles(t, directory)
			if result, err := Provision(t.Context(), config, directory, registry); !errors.Is(err, ErrIncomplete) || result != (ProvisionedJournals{}) ||
				registry.claimCalls != 0 || registry.receiptCalls != 0 || !reflect.DeepEqual(before, snapshotFiles(t, directory)) {
				t.Fatalf("process restart consumed first use again: %+v %v", result, err)
			}
		})
	}
}

func TestProvisionRejectsUnsafeRootsBeforeClaim(t *testing.T) {
	for _, fault := range []string{"mode", "owner", "symlink", "existing-scratch", "wrong-scratch", "relative", "canceled"} {
		t.Run(fault, func(t *testing.T) {
			config, directory, registry := provisionFixture(t)
			ctx := t.Context()
			switch fault {
			case "mode":
				mustDo(t, os.Chmod(directory, 0o755))
			case "owner":
				mustDo(t, os.Chown(directory, provisionOwner, provisionOwner))
			case "symlink":
				original := directory
				directory += "-link"
				mustDo(t, os.Symlink(original, directory))
				t.Cleanup(func() { _ = os.Remove(directory) })
				config.ScratchDirectory = filepath.Join(directory, "scratch")
			case "existing-scratch":
				mustDo(t, os.Mkdir(config.ScratchDirectory, 0o700))
			case "wrong-scratch":
				config.ScratchDirectory = t.TempDir()
			case "relative":
				directory = "relative"
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if result, err := Provision(ctx, config, directory, registry); err == nil || result != (ProvisionedJournals{}) || registry.claimCalls != 0 {
				t.Fatalf("unsafe root acquired first use: %+v %v", result, err)
			}
		})
	}
}

func TestProvisionDetectsChangedTransfer(t *testing.T) {
	for _, fault := range []string{"extra-file", "symlink", "replace", "change-bytes"} {
		t.Run(fault, func(t *testing.T) {
			config, directory, registry := provisionFixture(t)
			injected := false
			result, err := provision(t.Context(), config, directory, registry, func(phase string) error {
				path := filepath.Join(config.ScratchDirectory, "runtime-admission", "execution-admission.lock")
				switch {
				case phase == "journals-prepared" && fault == "extra-file":
					injected = true
					return os.WriteFile(filepath.Join(config.ScratchDirectory, "unexpected"), []byte("preserve"), 0o600)
				case phase == "journals-prepared" && fault == "symlink":
					injected = true
					return os.Symlink("/nonexistent", filepath.Join(config.ScratchDirectory, "outputs", "untrusted"))
				case phase == "origin-durable" && fault == "replace":
					injected = true
					if err := os.Rename(path, path+".original"); err != nil {
						return err
					}
					return os.WriteFile(path, []byte("replacement"), 0o600)
				case phase == "after-handover:." && fault == "change-bytes":
					injected = true
					return os.WriteFile(path, []byte("changed"), 0o600)
				}
				return nil
			})
			if !injected || err == nil || result != (ProvisionedJournals{}) {
				t.Fatal("changed storage acquired completed handover")
			}
			if _, err := os.Stat(filepath.Join(directory, provisionDoneName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("changed storage published completion")
			}
		})
	}
}

func provisionHelper(t *testing.T, mode string, input provisionHelperInput, nonroot bool) *exec.Cmd {
	t.Helper()
	input.Config.Validator = nil
	wire, err := json.Marshal(input)
	mustDo(t, err)
	binary, err := os.Executable()
	mustDo(t, err)
	command := exec.CommandContext(t.Context(), binary, "-test.run=^TestProvisionProcessHelper$", "-test.v")
	command.Env = append(os.Environ(), "VELA_PROVISION_TEST_HELPER="+mode)
	command.Stdin = bytes.NewReader(wire)
	if nonroot {
		command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: provisionOwner, Gid: provisionOwner}}
	}
	return command
}

func TestProvisionProcessHelper(t *testing.T) {
	mode := os.Getenv("VELA_PROVISION_TEST_HELPER")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	var input provisionHelperInput
	mustDo(t, json.NewDecoder(os.Stdin).Decode(&input))
	if mode == "crash" {
		input.Config.Validator = testValidator(t)
		_, err := provision(t.Context(), input.Config, input.NodeRoot, &fakeAuthority{}, func(phase string) error {
			if phase == input.Boundary {
				os.Exit(83)
			}
			return nil
		})
		t.Fatalf("crash boundary was not reached: %v", err)
	}
	if os.Geteuid() != provisionOwner {
		t.Fatal("helper must run as the actual non-root journal owner")
	}
	for _, path := range []string{input.NodeRoot, filepath.Join(input.NodeRoot, provisionOriginName), filepath.Join(input.NodeRoot, "scratch")} {
		if file, err := os.Open(path); !errors.Is(err, os.ErrPermission) {
			if file != nil {
				_ = file.Close()
			}
			t.Fatalf("workload reached Node-private path: %s %v", path, err)
		}
	}
	if mode == "denied" {
		if result, err := Provision(t.Context(), input.Config, input.NodeRoot, &fakeAuthority{}); err == nil || result != (ProvisionedJournals{}) {
			t.Fatal("non-root helper could invoke protected provisioning")
		}
		return
	}
	if mode != "recover" {
		t.Fatal("unknown provisioning helper")
	}
	input.Config.Validator = testValidator(t)
	p, err := bind(input.Config)
	mustDo(t, err)
	worker, err := stageworkeragent.PrepareAssignmentJournal(t.Context(), p.worker)
	mustDo(t, err)
	runtime, err := modelruntime.PrepareExecutionJournal(t.Context(), p.launch, input.Config.Validator,
		modelruntime.ExecutionFloorStateConfig{Directory: filepath.Join(input.Config.ScratchDirectory, "runtime-admission")})
	mustDo(t, err)
	if worker != input.Expected.Worker || runtime != input.Expected.Runtime {
		t.Fatal("ownership transfer or bind mount changed actual journal identity/state")
	}
	// The workload can corrupt its local origin, but cannot replace Node's copy.
	mustDo(t, os.WriteFile(filepath.Join(input.Config.ScratchDirectory, "bootstrap", originName), []byte("owner changed local origin"), 0o600))
	for _, path := range []string{filepath.Join(input.NodeRoot, provisionOriginName), filepath.Join(input.NodeRoot, "forged.json")} {
		if err := os.WriteFile(path, []byte(strings.Repeat("x", 64)), 0o600); !errors.Is(err, os.ErrPermission) {
			t.Fatalf("workload wrote protected evidence: %s %v", path, err)
		}
		if err := os.Remove(path); !errors.Is(err, os.ErrPermission) {
			t.Fatalf("workload removed protected evidence: %s %v", path, err)
		}
	}
}
