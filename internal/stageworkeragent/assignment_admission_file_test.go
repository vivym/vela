package stageworkeragent_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageworkeragent"
)

func TestAssignmentAdmissionRejectsMissingOrCorruptHistory(t *testing.T) {
	for _, mutation := range []string{"missing-state", "missing-lock", "truncated", "empty", "duplicate-field", "unknown-field", "trailing-data", "noncanonical", "missing-watermark", "phase", "signature", "worker", "epoch", "max-records"} {
		t.Run(mutation, func(t *testing.T) {
			fixture := newAdmissionFixture(t)
			gate := fixture.open(t)
			beginAdmission(t, gate, fixture.assignment, fixture.acquireID).Release()
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			statePath := filepath.Join(fixture.config.Directory, admissionTestState)
			document, err := os.ReadFile(statePath)
			if err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "missing-state":
				err = os.Remove(statePath)
			case "missing-lock":
				err = os.Remove(filepath.Join(fixture.config.Directory, "assignment-admission.lock"))
			case "truncated":
				document = document[:len(document)/2]
			case "empty":
				document = nil
			case "duplicate-field":
				document = append([]byte(`{"schema_version":1,`), document[1:]...)
			case "unknown-field":
				document = append([]byte(`{"unexpected":true,`), document[1:]...)
			case "trailing-data":
				document = append(document, []byte(`{}`)...)
			case "noncanonical":
				document = append(document, '\n')
			case "missing-watermark":
				document = bytes.Replace(document, []byte(`"watermark":1,`), nil, 1)
			case "phase":
				document = bytes.Replace(document, []byte(`"phase":"INPUTS_PENDING"`), []byte(`"phase":"DRAINED"`), 1)
			case "signature":
				document = bytes.Replace(document, []byte(`"original_authority":"`), []byte(`"original_authority":"AAAA`), 1)
			case "worker":
				fixture.config.WorkerInstanceID[0] ^= 1
				for index := range fixture.config.Bindings {
					fixture.config.Bindings[index].Runtime.WorkerInstanceID = fixture.config.WorkerInstanceID.String()
				}
			case "epoch":
				fixture.config.WorkerInstanceEpoch++
				for index := range fixture.config.Bindings {
					fixture.config.Bindings[index].Runtime.WorkerInstanceEpoch++
				}
			case "max-records":
				fixture.config.MaxRecords++
			}
			if err != nil {
				t.Fatal(err)
			}
			if mutation != "missing-state" && mutation != "missing-lock" {
				if err := os.WriteFile(statePath, document, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if reopened, err := stageworkeragent.NewFileAssignmentAdmission(fixture.config); err == nil {
				_ = reopened.Close()
				t.Fatal("ambiguous history was reinitialized or accepted")
			}
		})
	}
}

func TestAssignmentAdmissionRecoveryRejectsCompleteStateLoss(t *testing.T) {
	fixture := newAdmissionFixture(t)
	gate := fixture.open(t)
	beginAdmission(t, gate, fixture.assignment, fixture.acquireID).Release()
	if err := gate.CloseExecution(t.Context(), fixture.assignment.Authority); err != nil {
		t.Fatal(err)
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{fixture.config.Directory, fixture.config.InputRoot, fixture.config.OutputRoot} {
		if err := os.Rename(path, path+".retained"); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if recovered, err := stageworkeragent.NewFileAssignmentAdmission(fixture.config); err == nil {
		defer func() { _ = recovered.Close() }()
		handle, beginErr := recovered.Begin(t.Context(), fixture.assignment, fixture.acquireID)
		if handle != nil {
			handle.Release()
		}
		t.Fatalf("complete state loss initialized a new journal; old execution admission error=%v", beginErr)
	}
}

func TestAssignmentAdmissionRequiresExplicitFirstBootstrap(t *testing.T) {
	fixture := newAdmissionFixture(t)
	config := fixture.config
	config.Initialize = false
	if gate, err := stageworkeragent.NewFileAssignmentAdmission(config); err == nil {
		_ = gate.Close()
		t.Fatal("empty directories were treated as bootstrap authority")
	}
	if entries, err := os.ReadDir(config.Directory); err != nil || len(entries) != 0 {
		t.Fatalf("recovery created state: %v %v", entries, err)
	}
	gate := fixture.open(t)
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = true
	if gate, err := stageworkeragent.NewFileAssignmentAdmission(config); err == nil {
		_ = gate.Close()
		t.Fatal("bootstrap reused an initialized journal")
	}
}

func TestAssignmentAdmissionRejectsUntrustedFilesystem(t *testing.T) {
	for _, mutation := range []string{"reused-inputs", "reused-outputs", "overlap", "root-symlink", "root-writable", "state-symlink", "state-fifo", "state-hardlink", "state-readable", "lock-fifo", "marker-fifo", "marker-mismatch"} {
		t.Run(mutation, func(t *testing.T) {
			fixture := newAdmissionFixture(t)
			gate := fixture.open(t)
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			var err error
			switch mutation {
			case "reused-inputs", "reused-outputs":
				fixture.config.Directory = t.TempDir()
				if mutation == "reused-inputs" {
					fixture.config.OutputRoot = t.TempDir()
				} else {
					fixture.config.InputRoot = t.TempDir()
				}
			case "overlap":
				fixture.config.OutputRoot = fixture.config.InputRoot
			case "root-symlink":
				link := filepath.Join(t.TempDir(), "input-link")
				err = os.Symlink(fixture.config.InputRoot, link)
				fixture.config.InputRoot = link
			case "root-writable":
				err = os.Chmod(fixture.config.OutputRoot, 0o777)
			case "state-symlink", "state-fifo", "state-hardlink", "state-readable":
				path := filepath.Join(fixture.config.Directory, admissionTestState)
				switch mutation {
				case "state-readable":
					err = os.Chmod(path, 0o644)
				case "state-hardlink":
					err = os.Link(path, filepath.Join(fixture.config.Directory, "state-alias"))
				default:
					if err = os.Rename(path, path+".original"); err != nil {
						t.Fatal(err)
					}
					if mutation == "state-fifo" {
						err = syscall.Mkfifo(path, 0o600)
					} else {
						err = os.Symlink(path+".original", path)
					}
				}
			case "lock-fifo", "marker-fifo", "marker-mismatch":
				path := filepath.Join(fixture.config.InputRoot, ".vela-assignment-admission")
				if mutation == "lock-fifo" {
					path = filepath.Join(fixture.config.Directory, "assignment-admission.lock")
				}
				if err = os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if mutation == "marker-mismatch" {
					err = os.WriteFile(path, []byte("82000000-0000-0000-0000-000000000001"), 0o600)
				} else {
					err = syscall.Mkfifo(path, 0o600)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasSuffix(mutation, "fifo") {
				// A subprocess timeout detects accidental blocking on a FIFO open.
				runAdmissionProcess(t, filepath.Dir(fixture.config.Directory), "reject")
				return
			}
			if reopened, err := stageworkeragent.NewFileAssignmentAdmission(fixture.config); err == nil {
				_ = reopened.Close()
				t.Fatal("untrusted or reused filesystem was accepted")
			}
		})
	}
}

func TestAssignmentAdmissionLiveBindingFailureRemainsClosed(t *testing.T) {
	for _, mutation := range []string{"state-root", "input-root", "output-root", "lock", "state", "state-mtime", "marker"} {
		t.Run(mutation, func(t *testing.T) {
			fixture := newAdmissionFixture(t)
			gate := fixture.open(t)
			beginAdmission(t, gate, fixture.assignment, fixture.acquireID).Release()
			path := fixture.config.Directory
			isDirectory := true
			switch mutation {
			case "input-root":
				path = fixture.config.InputRoot
			case "output-root":
				path = fixture.config.OutputRoot
			case "lock":
				path, isDirectory = filepath.Join(path, "assignment-admission.lock"), false
			case "state", "state-mtime":
				path, isDirectory = filepath.Join(path, admissionTestState), false
			case "marker":
				path, isDirectory = filepath.Join(fixture.config.InputRoot, ".vela-assignment-admission"), false
			}
			var restore func()
			if mutation == "state-mtime" {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Chtimes(path, info.ModTime(), info.ModTime().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				restore = func() {
					if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				if err := os.Rename(path, path+".original"); err != nil {
					t.Fatal(err)
				}
				if isDirectory {
					if err := os.Mkdir(path, 0o700); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
				restore = func() {
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
					if err := os.Rename(path+".original", path); err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, err := gate.Snapshot(t.Context()); err == nil {
				t.Fatal("live binding replacement was accepted")
			}
			restore()
			if _, err := gate.Snapshot(t.Context()); err == nil {
				t.Fatal("binding failure was not sticky")
			}
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			gate = fixture.open(t)
			if snapshot := admissionSnapshot(t, gate); snapshot.Watermark != 1 {
				t.Fatal("recovery lost watermark")
			}
		})
	}
}

func TestAssignmentAdmissionPersistenceFailureRequiresReopen(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory write permissions")
	}
	fixture := newAdmissionFixture(t)
	gate := fixture.open(t)
	if err := os.Chmod(fixture.config.Directory, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(fixture.config.Directory, 0o700) })
	if handle, err := gate.Begin(t.Context(), fixture.assignment, fixture.acquireID); err == nil {
		handle.Release()
		t.Fatal("admitted input without a durable checkpoint")
	}
	if err := os.Chmod(fixture.config.Directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if handle, err := gate.Begin(t.Context(), fixture.assignment, fixture.acquireID); err == nil {
		handle.Release()
		t.Fatal("persistence failure did not require recovery")
	}
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = fixture.open(t)
	if snapshot := admissionSnapshot(t, gate); snapshot.Watermark != 0 {
		t.Fatal("failed write consumed watermark")
	}
	beginAdmission(t, gate, fixture.assignment, fixture.acquireID).Release()
}

func TestAssignmentAdmissionProcessLockAndAbruptExit(t *testing.T) {
	fixture := newAdmissionFixture(t)
	gate := fixture.open(t)
	base := filepath.Dir(fixture.config.Directory)
	runAdmissionProcess(t, base, "reject")
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	runAdmissionProcess(t, base, "enter-and-exit")
	gate = fixture.open(t)
	if snapshot := admissionSnapshot(t, gate); snapshot.Watermark != 1 || snapshot.Latest.Phase != stageworkeragent.AssignmentRuntimeEntered {
		t.Fatalf("process exit lost Runtime intent: %#v", snapshot)
	}
	if handle, err := gate.Begin(t.Context(), fixture.assignment, fixture.acquireID); !errors.Is(err, stageworkeragent.ErrAdmissionRecoveryRequired) {
		if handle != nil {
			handle.Release()
		}
		t.Fatalf("abrupt process exit allowed Runtime reentry: %v", err)
	}
}

func runAdmissionProcess(t *testing.T, base, mode string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAssignmentAdmissionProcessHelper$")
	command.Env = append(os.Environ(), "VELA_ADMISSION_TEST_ROOT="+base, "VELA_ADMISSION_TEST_MODE="+mode)
	output, err := command.CombinedOutput()
	if err != nil || ctx.Err() != nil {
		t.Fatalf("admission helper %s: %v, context=%v, output=%s", mode, err, ctx.Err(), output)
	}
}

func TestAssignmentAdmissionProcessHelper(t *testing.T) {
	base := os.Getenv("VELA_ADMISSION_TEST_ROOT")
	if base == "" {
		return
	}
	fixture := admissionFixtureAt(t, base)
	gate, err := stageworkeragent.NewFileAssignmentAdmission(fixture.config)
	if os.Getenv("VELA_ADMISSION_TEST_MODE") == "reject" {
		if err == nil {
			_ = gate.Close()
			t.Fatal("process admitted locked or untrusted storage")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	handle, err := gate.Begin(t.Context(), fixture.assignment, fixture.acquireID)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.EnterRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	// Skip Release, Close and test cleanups to model a crash after the durable intent.
	os.Exit(0)
}
