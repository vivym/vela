package stageworkeragent_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func terminalRetirer(t *testing.T, gate *stageworkeragent.FileAssignmentAdmission, f *floorCollectorFixture) *stageworkeragent.TerminalScratchRetirement {
	t.Helper()
	retirer, err := stageworkeragent.NewTerminalScratchRetirement(gate, f.agent(t), stageworkeragent.AttemptOwnedFilesystemScratchV1)
	if err != nil {
		t.Fatal(err)
	}
	return retirer
}

func retirementScratch(t *testing.T, f *floorCollectorFixture) []string {
	t.Helper()
	paths := []string{filepath.Join(f.admissionFixture.config.InputRoot, "stage-runs", f.disposition.StageRunId, "inputs", "payload")}
	for _, allocation := range f.disposition.Allocations {
		paths = append(paths, filepath.Join(f.admissionFixture.config.OutputRoot, allocation.StageAttemptId, "output"))
	}
	for _, name := range append(paths, filepath.Join(f.admissionFixture.config.InputRoot, "unrelated", "keep"), filepath.Join(f.admissionFixture.config.OutputRoot, "unrelated", "keep")) {
		if err := os.MkdirAll(filepath.Dir(name), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(name, []byte("owned-scratch"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return paths
}

func assertRetirementScratch(t *testing.T, paths []string, present bool) {
	t.Helper()
	for _, name := range paths {
		_, err := os.Stat(name)
		if present && err != nil || !present && !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("scratch %s present=%t: %v", name, present, err)
		}
	}
}

func TestTerminalScratchRetirementCombinesInputFloorAndMixedRuntimeProofs(t *testing.T) {
	f := newAssignmentFloorFixture(t)
	gate := f.open(t)
	handle := beginAdmission(t, gate, f.assignment, f.acquireID)
	if err := handle.CompleteInputs(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := handle.EnterRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	handle.Release()
	group := startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
	a := f.assignment.Authority
	if response, err := group.clients[0].PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: a, ExecutionSpec: f.assignment.ExecutionSpec}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("prepare: %v %v", response, err)
	}
	if response, err := group.clients[0].StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: a}); err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("start: %v %v", response, err)
	}
	group.activeBackends[0].MarkOutputReady([]byte(`{"kind":"LATENT","path":"/local/dit.bin"}`))
	if response, err := group.clients[0].SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: a}); err != nil || response.GetReceipt() == nil {
		t.Fatalf("seal: %v %v", response, err)
	}
	paths := retirementScratch(t, f)
	retirer := terminalRetirer(t, gate, f)
	result, err := retirer.Retire(t.Context(), f.disposition, map[string]*velav1.StageAuthority{a.StageAllocationId: a}, drainCollectorTargets(f))
	if err != nil || result.Phase != stageworkeragent.TerminalRetirementRetired {
		t.Fatalf("retire: %+v %v", result, err)
	}
	assertRetirementScratch(t, paths, false)
	assertRetirementScratch(t, []string{filepath.Join(f.admissionFixture.config.InputRoot, "unrelated", "keep"), filepath.Join(f.admissionFixture.config.OutputRoot, "unrelated", "keep")}, true)
	state := admissionSnapshot(t, gate)
	if state.Floor != 7 || state.Watermark != 1 || state.Latest.Phase != stageworkeragent.AssignmentClosed || len(state.Retirements) != 1 {
		t.Fatalf("retirement lost admission restrictions: %+v", state)
	}
	if next, err := gate.Begin(t.Context(), f.next(t, 8), uuid.New()); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
		if next != nil {
			next.Release()
		}
		t.Fatalf("retired StageRun reopened above its cutoff: %v", err)
	}
	next := f.next(t, 8)
	next.Authority.StageRunId = uuid.NewString()
	f.sign(t, next)
	completeAdmissionInputs(t, beginAdmission(t, gate, next, uuid.New()))
	for _, backend := range group.activeBackends {
		if backend.closed.Load() {
			t.Fatal("retirement unloaded resident models")
		}
	}
	if _, err := retirer.Resume(t.Context(), result.StageRunID); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalScratchRetirementRetainsUnknownInputsAndRuntimeHistory(t *testing.T) {
	for _, fault := range []string{"unknown-input", "active-input", "runtime-intent", "lost-floor-reply", "invalid-signature"} {
		t.Run(fault, func(t *testing.T) {
			f := newAssignmentFloorFixture(t)
			gate := f.open(t)
			group := startFloorCollectorRuntimes(t, f, t.TempDir(), true, fault == "lost-floor-reply")
			var handle *stageworkeragent.AssignmentAdmission
			if fault == "unknown-input" || fault == "active-input" {
				handle = beginAdmission(t, gate, f.assignment, f.acquireID)
				if fault == "unknown-input" {
					handle.Release()
				}
			}
			if fault == "runtime-intent" {
				if _, err := group.clients[0].PrepareStage(t.Context(), &velav1.ModelRuntimeServicePrepareStageRequest{Authority: f.assignment.Authority, ExecutionSpec: f.assignment.ExecutionSpec}); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "invalid-signature" {
				f.disposition.Signature[0] ^= 1
			}
			paths := retirementScratch(t, f)
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			result, err := terminalRetirer(t, gate, f).Retire(ctx, f.disposition, nil, drainCollectorTargets(f))
			if err == nil || result.Phase == stageworkeragent.TerminalRetirementRetired {
				t.Fatalf("unproven retirement: %+v %v", result, err)
			}
			assertRetirementScratch(t, paths, true)
			if handle != nil {
				handle.Release()
			}
			state := admissionSnapshot(t, gate)
			if fault == "invalid-signature" {
				if len(state.Retirements) != 0 || state.Floor != 0 {
					t.Fatal("invalid signature changed admission")
				}
			} else if len(state.Retirements) != 1 || state.Retirements[0].Phase != stageworkeragent.TerminalRetirementIntent || state.Floor != 7 {
				t.Fatalf("partial collection lost its intent: %+v", state)
			}
			if fault == "lost-floor-reply" {
				if result, err := terminalRetirer(t, gate, f).Retire(t.Context(), f.disposition, nil, drainCollectorTargets(f)); err != nil || result.Phase != stageworkeragent.TerminalRetirementRetired {
					t.Fatalf("lost floor reply recovery: %+v %v", result, err)
				}
				assertRetirementScratch(t, paths, false)
			}
		})
	}
}

func TestTerminalScratchRetirementResumesPartialDeletionAfterEpochAndProfileChange(t *testing.T) {
	f := newAssignmentFloorFixture(t)
	gate := f.open(t)
	group := startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
	paths := retirementScratch(t, f)
	restore := stageworkeragent.SetTerminalRetirementDirectoryHookForTest(gate, func(index int) error {
		if index == 0 {
			return errors.New("injected failure after deleting inputs")
		}
		return nil
	})
	retirer := terminalRetirer(t, gate, f)
	result, err := retirer.Retire(t.Context(), f.disposition, nil, drainCollectorTargets(f))
	restore()
	if err == nil || result.Phase != stageworkeragent.TerminalRetirementReady {
		t.Fatalf("partial deletion: %+v %v", result, err)
	}
	assertRetirementScratch(t, paths[:1], false)
	assertRetirementScratch(t, paths[1:], true)
	group.close()
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	f.clock.Add(int64(2 * time.Minute))
	f.admissionFixture.config.Bindings = f.admissionFixture.config.Bindings[2:]
	for index := range f.admissionFixture.config.Bindings {
		f.admissionFixture.config.Bindings[index].Runtime.ModelRuntimeEpoch++
	}
	gate = f.open(t)
	// Runtime sockets are closed. READY recovery must not contact any backend.
	result, err = terminalRetirer(t, gate, f).Resume(t.Context(), uuid.MustParse(f.disposition.StageRunId))
	if err != nil || result.Phase != stageworkeragent.TerminalRetirementRetired {
		t.Fatalf("resume: %+v %v", result, err)
	}
	assertRetirementScratch(t, paths, false)
}

func TestTerminalScratchRetirementProofDurabilityFailurePrecedesDeletion(t *testing.T) {
	for _, fault := range []string{"sync", "lost-reply"} {
		t.Run(fault, func(t *testing.T) {
			f := newAssignmentFloorFixture(t)
			gate := f.open(t)
			startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
			paths := retirementScratch(t, f)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			writes := 0
			restore := stageworkeragent.SetAssignmentAdmissionSyncHookForTest(gate, func(sync func() error) error {
				writes++
				if writes == 2 && fault == "sync" {
					return errors.New("injected READY directory sync failure")
				}
				err := sync()
				if writes == 2 && fault == "lost-reply" {
					cancel()
				}
				return err
			})
			if result, err := terminalRetirer(t, gate, f).Retire(ctx, f.disposition, nil, drainCollectorTargets(f)); err == nil || result.Phase == stageworkeragent.TerminalRetirementRetired {
				t.Fatalf("uncertain proof: %+v %v", result, err)
			}
			restore()
			assertRetirementScratch(t, paths, true)
			if writes != 2 {
				t.Fatalf("did not reach READY checkpoint: %d writes", writes)
			}
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			gate = f.open(t)
			if result, err := terminalRetirer(t, gate, f).Resume(t.Context(), uuid.MustParse(f.disposition.StageRunId)); err != nil || result.Phase != stageworkeragent.TerminalRetirementRetired {
				t.Fatalf("published proof recovery: %+v %v", result, err)
			}
			assertRetirementScratch(t, paths, false)
		})
	}
}

func TestTerminalScratchRetirementSchema3UpgradePreservesEvidence(t *testing.T) {
	f := newAssignmentFloorFixture(t)
	gate := f.open(t)
	completeAdmissionInputs(t, beginAdmission(t, gate, f.assignment, f.acquireID))
	if _, err := gate.InstallExecutionFloor(t.Context(), f.disposition); err != nil {
		t.Fatal(err)
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(f.admissionFixture.config.Directory, admissionTestState)
	original, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	legacy := bytes.Replace(original, []byte(`"schema_version":4`), []byte(`"schema_version":3`), 1)
	if err := os.WriteFile(name, legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	if reopened, err := stageworkeragent.NewFileAssignmentAdmission(f.admissionFixture.config); err == nil {
		_ = reopened.Close()
		t.Fatal("legacy journal migrated implicitly")
	}
	f.admissionFixture.config.UpgradeV3 = true
	gate = f.open(t)
	if after, err := os.ReadFile(name); err != nil || !bytes.Equal(original, after) {
		t.Fatal("upgrade changed existing evidence")
	}
	if state := admissionSnapshot(t, gate); len(state.Retirements) != 0 || state.Latest.InputDrain == nil || state.Floor != 7 {
		t.Fatalf("upgrade invented retirement or lost history: %+v", state)
	}
}

func TestTerminalScratchRetirementSyncsMissingNamespaceBeforeRetired(t *testing.T) {
	for _, missing := range []string{"input", "input-parent", "output"} {
		t.Run(missing, func(t *testing.T) {
			f := newAssignmentFloorFixture(t)
			gate := f.open(t)
			group := startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
			paths := retirementScratch(t, f)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			writes := 0
			restore := stageworkeragent.SetAssignmentAdmissionSyncHookForTest(gate, func(sync func() error) error {
				writes++
				err := sync()
				if writes == 2 {
					cancel()
				}
				return err
			})
			if _, err := terminalRetirer(t, gate, f).Retire(ctx, f.disposition, nil, drainCollectorTargets(f)); !errors.Is(err, context.Canceled) {
				t.Fatalf("READY checkpoint: %v", err)
			}
			restore()
			assertRetirementScratch(t, paths, true)
			parent := filepath.Join(f.admissionFixture.config.InputRoot, "stage-runs")
			target := filepath.Join(parent, f.disposition.StageRunId)
			switch missing {
			case "input-parent":
				target, parent = parent, f.admissionFixture.config.InputRoot
			case "output":
				target, parent = filepath.Dir(paths[1]), f.admissionFixture.config.OutputRoot
			}
			// Model the visible filesystem after unlink but before its parent sync.
			// A process restart does not make that deletion power-loss durable.
			if err := os.RemoveAll(target); err != nil {
				t.Fatal(err)
			}
			parent, err := filepath.EvalSymlinks(parent)
			if err != nil {
				t.Fatal(err)
			}
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			group.close()
			gate = f.open(t)
			retirer := terminalRetirer(t, gate, f)
			stageRunID := uuid.MustParse(f.disposition.StageRunId)
			syncErr := errors.New("injected missing-parent sync failure")
			syncs := 0
			restore = stageworkeragent.SetTerminalRetirementAbsentParentSyncHookForTest(gate, func(name string, _ func() error) error {
				syncs++
				if name != parent {
					t.Errorf("sync parent = %s, want %s", name, parent)
				}
				return syncErr
			})
			result, err := retirer.Resume(t.Context(), stageRunID)
			restore()
			if !errors.Is(err, syncErr) || result.Phase != stageworkeragent.TerminalRetirementReady || syncs != 1 {
				t.Fatalf("absence bypassed directory durability: %+v %v, syncs=%d", result, err, syncs)
			}
			if state := admissionSnapshot(t, gate); state.Retirements[0].Phase != stageworkeragent.TerminalRetirementReady {
				t.Fatal("failed namespace sync persisted RETIRED")
			}
			if missing != "output" {
				assertRetirementScratch(t, paths[1:], true)
			}
			synced := make(map[string]bool)
			restore = stageworkeragent.SetTerminalRetirementAbsentParentSyncHookForTest(gate, func(name string, sync func() error) error {
				synced[name] = true
				return sync()
			})
			result, err = retirer.Resume(t.Context(), stageRunID)
			restore()
			if err != nil || result.Phase != stageworkeragent.TerminalRetirementRetired || !synced[parent] {
				t.Fatalf("durable absence recovery: %+v %v, synced=%v", result, err, synced)
			}
			assertRetirementScratch(t, paths, false)
			assertRetirementScratch(t, []string{filepath.Join(f.admissionFixture.config.InputRoot, "unrelated", "keep"), filepath.Join(f.admissionFixture.config.OutputRoot, "unrelated", "keep")}, true)
		})
	}
}

func TestTerminalScratchRetirementRejectsDirectoryReplacementAndNewNamespaces(t *testing.T) {
	for _, fault := range []string{"input", "output", "symlink", "appeared", "nested-link", "writable"} {
		t.Run(fault, func(t *testing.T) {
			f := newAssignmentFloorFixture(t)
			gate := f.open(t)
			startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
			paths := retirementScratch(t, f)
			directory := filepath.Dir(paths[1])
			if fault == "input" {
				directory = filepath.Join(f.admissionFixture.config.InputRoot, "stage-runs", f.disposition.StageRunId)
			}
			if fault == "appeared" {
				if err := os.RemoveAll(directory); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			writes := 0
			restore := stageworkeragent.SetAssignmentAdmissionSyncHookForTest(gate, func(sync func() error) error {
				writes++
				err := sync()
				if writes == 2 {
					cancel()
				}
				return err
			})
			retirer := terminalRetirer(t, gate, f)
			if _, err := retirer.Retire(ctx, f.disposition, nil, drainCollectorTargets(f)); !errors.Is(err, context.Canceled) {
				t.Fatalf("READY checkpoint: %v", err)
			}
			restore()
			switch fault {
			case "input", "output", "symlink":
				if err := os.Rename(directory, directory+".original"); err != nil {
					t.Fatal(err)
				}
				if fault == "symlink" {
					if err := os.Symlink(directory+".original", directory); err != nil {
						t.Fatal(err)
					}
				} else if err := os.Mkdir(directory, 0o700); err != nil {
					t.Fatal(err)
				}
			case "appeared":
				if err := os.Mkdir(directory, 0o700); err != nil {
					t.Fatal(err)
				}
			case "nested-link":
				if err := os.Symlink(filepath.Join(f.admissionFixture.config.OutputRoot, "unrelated"), filepath.Join(directory, "link")); err != nil {
					t.Fatal(err)
				}
			case "writable":
				if err := os.Chmod(directory, 0o777); err != nil {
					t.Fatal(err)
				}
			}
			if result, err := retirer.Resume(t.Context(), uuid.MustParse(f.disposition.StageRunId)); err == nil || result.Phase != stageworkeragent.TerminalRetirementReady {
				t.Fatalf("unsafe directory reused: %+v %v", result, err)
			}
			assertRetirementScratch(t, paths[2:], true)
			if fault != "input" {
				assertRetirementScratch(t, paths[:1], true)
			}
			assertRetirementScratch(t, []string{filepath.Join(f.admissionFixture.config.OutputRoot, "unrelated", "keep")}, true)
		})
	}
}

func TestTerminalScratchRetirementRejectsLegacyProofAndConflictingUpgradeFlags(t *testing.T) {
	for _, fault := range []string{"legacy-proof", "initialize", "both-upgrades"} {
		t.Run(fault, func(t *testing.T) {
			f := newAssignmentFloorFixture(t)
			gate := f.open(t)
			startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
			if _, err := terminalRetirer(t, gate, f).Retire(t.Context(), f.disposition, nil, drainCollectorTargets(f)); err != nil {
				t.Fatal(err)
			}
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(f.admissionFixture.config.Directory, admissionTestState)
			document, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			document = bytes.Replace(document, []byte(`"schema_version":4`), []byte(`"schema_version":3`), 1)
			if err := os.WriteFile(name, document, 0o600); err != nil {
				t.Fatal(err)
			}
			config := f.admissionFixture.config
			config.UpgradeV3 = true
			config.Initialize, config.UpgradeV2 = fault == "initialize", fault == "both-upgrades"
			if unexpected, err := stageworkeragent.NewFileAssignmentAdmission(config); err == nil {
				_ = unexpected.Close()
				t.Fatal("invalid migration accepted")
			}
			if after, err := os.ReadFile(name); err != nil || !bytes.Equal(after, document) {
				t.Fatal("failed migration changed source history")
			}
		})
	}
}

func TestTerminalScratchRetirementBoundPreservesExistingProof(t *testing.T) {
	f := newAssignmentFloorFixture(t)
	f.admissionFixture.config.MaxRecords = 1
	gate := f.open(t)
	startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
	retirer := terminalRetirer(t, gate, f)
	if _, err := retirer.Retire(t.Context(), f.disposition, nil, drainCollectorTargets(f)); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(f.admissionFixture.config.Directory, admissionTestState)
	before, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	f.disposition.StageRunId = uuid.NewString()
	f.signDisposition(t)
	if _, err := retirer.Retire(t.Context(), f.disposition, nil, drainCollectorTargets(f)); !errors.Is(err, stageworkeragent.ErrAdmissionCapacity) {
		t.Fatalf("full retirement history: %v", err)
	}
	if after, err := os.ReadFile(name); err != nil || !bytes.Equal(before, after) {
		t.Fatal("overflow evicted existing proof")
	}
}

func TestTerminalScratchRetirementAbruptProcessExitResumesExactIntent(t *testing.T) {
	base := t.TempDir()
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestTerminalScratchRetirementProcessHelper$", "-test.count=1")
	command.Env = append(os.Environ(), "VELA_TERMINAL_RETIREMENT_HELPER="+base)
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 83 {
		t.Fatalf("retirement process did not exit at deletion boundary: %v %s", err, output)
	}
	f := newAssignmentFloorFixture(t)
	f.admissionFixture.config.Directory = filepath.Join(base, "state")
	f.admissionFixture.config.InputRoot = filepath.Join(base, "inputs")
	f.admissionFixture.config.OutputRoot = filepath.Join(base, "outputs")
	f.admissionFixture.config.Initialize = false
	gate := f.open(t)
	state := admissionSnapshot(t, gate)
	if len(state.Retirements) != 1 || state.Retirements[0].Phase != stageworkeragent.TerminalRetirementReady {
		t.Fatalf("abrupt exit lost READY evidence: %+v", state)
	}
	result, err := terminalRetirer(t, gate, f).Resume(t.Context(), state.Retirements[0].StageRunID)
	if err != nil || result.Phase != stageworkeragent.TerminalRetirementRetired {
		t.Fatalf("process recovery: %+v %v", result, err)
	}
	outputs, err := filepath.Glob(filepath.Join(base, "outputs", "*", "output"))
	if err != nil || len(outputs) != 0 {
		t.Fatalf("owned outputs remained after recovery: %v %v", outputs, err)
	}
	assertRetirementScratch(t, []string{filepath.Join(base, "inputs", "unrelated", "keep"), filepath.Join(base, "outputs", "unrelated", "keep")}, true)
}

func TestTerminalScratchRetirementProcessHelper(t *testing.T) {
	base := os.Getenv("VELA_TERMINAL_RETIREMENT_HELPER")
	if base == "" {
		return
	}
	f := newAssignmentFloorFixture(t)
	f.admissionFixture.config.Directory = filepath.Join(base, "state")
	f.admissionFixture.config.InputRoot = filepath.Join(base, "inputs")
	f.admissionFixture.config.OutputRoot = filepath.Join(base, "outputs")
	for _, directory := range []string{f.admissionFixture.config.Directory, f.admissionFixture.config.InputRoot, f.admissionFixture.config.OutputRoot} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	gate := f.open(t)
	startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
	retirementScratch(t, f)
	stageworkeragent.SetTerminalRetirementDirectoryHookForTest(gate, func(int) error { os.Exit(83); return nil })
	if _, err := terminalRetirer(t, gate, f).Retire(t.Context(), f.disposition, nil, drainCollectorTargets(f)); err != nil {
		t.Fatal(err)
	}
	t.Fatal("retirement did not reach process-exit boundary")
}

func TestTerminalScratchRetirementRevalidatesPersistedProof(t *testing.T) {
	for _, fault := range []string{"missing-floor", "swapped-floor", "missing-proof", "swapped-proof", "mixed-proof", "signature", "member", "decision", "contract", "input-drain", "directory-root", "directory-path", "directory-identity", "phase", "cutoff"} {
		t.Run(fault, func(t *testing.T) {
			f := newAssignmentFloorFixture(t)
			gate := f.open(t)
			completeAdmissionInputs(t, beginAdmission(t, gate, f.assignment, f.acquireID))
			startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
			paths := retirementScratch(t, f)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			writes := 0
			restore := stageworkeragent.SetAssignmentAdmissionSyncHookForTest(gate, func(sync func() error) error {
				writes++
				err := sync()
				if writes == 2 {
					cancel()
				}
				return err
			})
			if _, err := terminalRetirer(t, gate, f).Retire(ctx, f.disposition, nil, drainCollectorTargets(f)); !errors.Is(err, context.Canceled) {
				t.Fatalf("prepare READY: %v", err)
			}
			restore()
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(f.admissionFixture.config.Directory, admissionTestState)
			document, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			document, err = stageworkeragent.CorruptTerminalRetirementForTest(document, fault)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(name, document, 0o600); err != nil {
				t.Fatal(err)
			}
			if recovered, err := stageworkeragent.NewFileAssignmentAdmission(f.admissionFixture.config); err == nil {
				_ = recovered.Close()
				t.Fatal("invalid retirement proof was accepted")
			}
			assertRetirementScratch(t, paths, true)
		})
	}
}

func TestTerminalScratchRetirementConcurrentReplay(t *testing.T) {
	f := newAssignmentFloorFixture(t)
	gate := f.open(t)
	startFloorCollectorRuntimes(t, f, t.TempDir(), true, false)
	paths := retirementScratch(t, f)
	retirer := terminalRetirer(t, gate, f)
	results := make(chan error, 2)
	targets := drainCollectorTargets(f)
	for range 2 {
		go func() {
			result, err := retirer.Retire(t.Context(), f.disposition, nil, targets)
			if err == nil && result.Phase != stageworkeragent.TerminalRetirementRetired {
				err = errors.New("retirement remained incomplete")
			}
			results <- err
		}()
	}
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if len(admissionSnapshot(t, gate).Retirements) != 1 {
		t.Fatal("replay created duplicate retirement records")
	}
	assertRetirementScratch(t, paths, false)
}
