package stageworkeragent_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
)

func completeAdmissionInputs(t *testing.T, handle *stageworkeragent.AssignmentAdmission) {
	t.Helper()
	if err := handle.CompleteInputs(t.Context()); err != nil {
		t.Fatal(err)
	}
	handle.Release()
}

func TestAssignmentInputDrainRequiredBeforeRuntimeEntry(t *testing.T) {
	f := newAdmissionFixture(t)
	gate := f.open(t)
	handle := beginAdmission(t, gate, f.assignment, f.acquireID)
	if err := handle.EnterRuntime(t.Context()); !errors.Is(err, stageworkeragent.ErrInputWritersUnproven) {
		t.Fatalf("Runtime entered before input completion: %v", err)
	}
	if err := handle.CompleteInputs(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := handle.EnterRuntime(t.Context()); err != nil {
		t.Fatalf("completed input could not enter Runtime: %v", err)
	}
}

func TestAssignmentInputDrainChecksAllRetainedInputsThroughCutoff(t *testing.T) {
	f := newAssignmentFloorFixture(t)
	gate := f.open(t)
	completeAdmissionInputs(t, beginAdmission(t, gate, f.assignment, f.acquireID))
	first := admissionSnapshot(t, gate).Latest.InputDrain
	completeAdmissionInputs(t, beginAdmission(t, gate, f.next(t, 2), uuid.New()))
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	// Model preserved legacy history whose older input invocation has no proof.
	path := filepath.Join(f.admissionFixture.config.Directory, admissionTestState)
	document, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	document = bytes.Replace(document, append([]byte(`,"input_drain":`), encoded...), nil, 1)
	if err := os.WriteFile(path, document, 0o600); err != nil {
		t.Fatal(err)
	}
	gate = f.open(t)
	if handle, err := gate.Begin(t.Context(), f.next(t, 3), uuid.New()); !errors.Is(err, stageworkeragent.ErrInputWritersUnproven) {
		if handle != nil {
			handle.Release()
		}
		t.Fatalf("new allocation bypassed historical input writers: %v", err)
	}
	installed, err := gate.InstallExecutionFloor(t.Context(), f.disposition)
	if err != nil {
		t.Fatal(err)
	}
	if err := installed.WaitInputWriters(t.Context()); !errors.Is(err, stageworkeragent.ErrInputWritersUnproven) {
		t.Fatalf("latest completed input hid older unproven writers: %v", err)
	}
}

func TestAssignmentInputDrainProcessExitPreservesOnlyRecordedCompletion(t *testing.T) {
	for _, mode := range []string{"inputs-and-exit", "complete-inputs-and-exit"} {
		t.Run(mode, func(t *testing.T) {
			f := newAssignmentFloorFixture(t)
			gate := f.open(t)
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			runAdmissionProcess(t, filepath.Dir(f.admissionFixture.config.Directory), mode)
			gate = f.open(t)
			installed, err := gate.InstallExecutionFloor(t.Context(), f.disposition)
			if err != nil {
				t.Fatal(err)
			}
			err = installed.WaitInputWriters(t.Context())
			if mode == "complete-inputs-and-exit" {
				if err != nil {
					t.Fatalf("durable completion lost after process exit: %v", err)
				}
			} else if !errors.Is(err, stageworkeragent.ErrInputWritersUnproven) {
				t.Fatalf("process exit supplied missing completion: %v", err)
			}
		})
	}
}

func TestAssignmentInputDrainSchemaUpgradeRequiresExplicitValidatedSource(t *testing.T) {
	for _, fault := range []string{"valid", "schema-1", "signature", "proof-in-v2", "initialize"} {
		t.Run(fault, func(t *testing.T) {
			f := newAssignmentFloorFixture(t)
			gate := f.open(t)
			handle := beginAdmission(t, gate, f.assignment, f.acquireID)
			if fault == "proof-in-v2" {
				if err := handle.CompleteInputs(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			handle.Release()
			if _, err := gate.InstallExecutionFloor(t.Context(), f.disposition); err != nil {
				t.Fatal(err)
			}
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.admissionFixture.config.Directory, admissionTestState)
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			legacy := bytes.Replace(original, []byte(`"schema_version":4`), []byte(`"schema_version":2`), 1)
			if fault == "schema-1" {
				legacy = bytes.Replace(legacy, []byte(`"schema_version":2`), []byte(`"schema_version":1`), 1)
			}
			if err := os.WriteFile(path, legacy, 0o600); err != nil {
				t.Fatal(err)
			}
			config := f.admissionFixture.config
			if unexpected, err := stageworkeragent.NewFileAssignmentAdmission(config); err == nil {
				_ = unexpected.Close()
				t.Fatal("schema-2 state upgraded without explicit opt-in")
			}
			config.UpgradeV2 = true
			config.Initialize = fault == "initialize"
			if fault == "signature" {
				// Keep canonical bytes unchanged but remove their authenticating key.
				config.Validator, err = stageauthority.NewValidator(map[string][]byte{"barrier-key": bytes.Repeat([]byte{0x71}, 32)}, func() time.Time { return time.Unix(0, f.clock.Load()) })
				if err != nil {
					t.Fatal(err)
				}
			}
			upgraded, err := stageworkeragent.NewFileAssignmentAdmission(config)
			if fault != "valid" {
				if err == nil {
					_ = upgraded.Close()
					t.Fatal("invalid upgrade was accepted")
				}
				if after, err := os.ReadFile(path); err != nil || !bytes.Equal(after, legacy) {
					t.Fatal("rejected upgrade changed original state")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if after, err := os.ReadFile(path); err != nil || !bytes.Equal(after, original) {
				t.Fatal("upgrade changed evidence beyond schema version")
			}
			installed, err := upgraded.InstallExecutionFloor(t.Context(), f.disposition)
			if err != nil {
				t.Fatal(err)
			}
			if err := installed.WaitInputWriters(t.Context()); !errors.Is(err, stageworkeragent.ErrInputWritersUnproven) {
				t.Fatalf("upgrade invented input drain proof: %v", err)
			}
			if err := upgraded.Close(); err != nil {
				t.Fatal(err)
			}
			replayed, err := stageworkeragent.NewFileAssignmentAdmission(config)
			if err != nil {
				t.Fatalf("upgrade retry failed: %v", err)
			}
			if err := replayed.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAssignmentInputDrainRecoveryDoesNotInferCompletion(t *testing.T) {
	f := newAssignmentFloorFixture(t)
	gate := f.open(t)
	// Release removes an in-process handle. There is no durable assertion that
	// the resolver and every writable handle exited before the Worker stopped.
	beginAdmission(t, gate, f.assignment, f.acquireID).Release()
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	gate = f.open(t)
	installed, err := gate.InstallExecutionFloor(t.Context(), f.disposition)
	if err != nil {
		t.Fatal(err)
	}
	if err := installed.WaitInputWriters(t.Context()); !errors.Is(err, stageworkeragent.ErrInputWritersUnproven) {
		t.Fatalf("recovery inferred input writer completion from a missing process-local handle: %v", err)
	}
}

func TestAssignmentInputDrainPersistsAfterWriterClosesAndSurvivesRecovery(t *testing.T) {
	f := newAssignmentFloorFixture(t)
	gate := f.open(t)
	handle := beginAdmission(t, gate, f.assignment, f.acquireID)
	file, err := os.Create(filepath.Join(f.admissionFixture.config.InputRoot, "pending-input"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	installed, err := gate.InstallExecutionFloor(t.Context(), f.disposition)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := installed.WaitInputWriters(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("live writer was declared drained: %v", err)
	}
	if _, err := file.WriteString("still-owned"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := handle.CompleteInputs(t.Context()); err != nil {
		t.Fatal(err)
	}
	before := admissionSnapshot(t, gate).Latest.InputDrain
	if before == nil || before.Contract != stageworkeragent.AssignmentInputDrainContract || before.ObservedAt.IsZero() {
		t.Fatal("completion was not persisted")
	}
	if err := handle.CompleteInputs(t.Context()); err != nil {
		t.Fatal(err)
	}
	if after := admissionSnapshot(t, gate).Latest.InputDrain; *after != *before {
		t.Fatal("completion retry replaced its checkpoint")
	}
	// Input work is complete even if the handle is still owned by Runtime entry.
	if err := installed.WaitInputWriters(t.Context()); err != nil {
		t.Fatal(err)
	}
	handle.Release()
	if err := gate.Close(); err != nil {
		t.Fatal(err)
	}
	if err := installed.WaitInputWriters(t.Context()); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
		t.Fatalf("closed gate retained usable input evidence: %v", err)
	}
	gate = f.open(t)
	installed, err = gate.InstallExecutionFloor(t.Context(), f.disposition)
	if err != nil {
		t.Fatal(err)
	}
	if err := installed.WaitInputWriters(t.Context()); err != nil {
		t.Fatal(err)
	}
	if after := admissionSnapshot(t, gate).Latest.InputDrain; after == nil || *after != *before {
		t.Fatal("recovery lost the input checkpoint")
	}
	before.Contract = "mutated"
	if admissionSnapshot(t, gate).Latest.InputDrain.Contract != stageworkeragent.AssignmentInputDrainContract {
		t.Fatal("snapshot mutated the retained input checkpoint")
	}
}

func TestAssignmentInputDrainRetryMustCompleteEachInvocation(t *testing.T) {
	f := newAdmissionFixture(t)
	gate := f.open(t)
	completeAdmissionInputs(t, beginAdmission(t, gate, f.assignment, f.acquireID))
	handle := beginAdmission(t, gate, f.assignment, f.acquireID)
	if admissionSnapshot(t, gate).Latest.InputDrain != nil {
		t.Fatal("new input invocation inherited previous completion")
	}
	handle.Release()
	if err := handle.CompleteInputs(t.Context()); !errors.Is(err, stageworkeragent.ErrAdmissionClosed) {
		t.Fatalf("released handle created historical completion: %v", err)
	}
	if next, err := gate.Begin(t.Context(), f.assignment, f.acquireID); !errors.Is(err, stageworkeragent.ErrInputWritersUnproven) {
		if next != nil {
			next.Release()
		}
		t.Fatalf("unknown earlier input invocation allowed retry: %v", err)
	}
	if next, err := gate.Begin(t.Context(), f.next(t, 2), uuid.New()); !errors.Is(err, stageworkeragent.ErrInputWritersUnproven) {
		if next != nil {
			next.Release()
		}
		t.Fatalf("unknown input invocation allowed a later allocation: %v", err)
	}
}

func TestAssignmentInputDrainSyncFailureAndLostAcknowledgementRecover(t *testing.T) {
	for _, fault := range []string{"sync", "acknowledgement"} {
		t.Run(fault, func(t *testing.T) {
			f := newAssignmentFloorFixture(t)
			gate := f.open(t)
			handle := beginAdmission(t, gate, f.assignment, f.acquireID)
			installed, err := gate.InstallExecutionFloor(t.Context(), f.disposition)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			restore := stageworkeragent.SetAssignmentAdmissionSyncHookForTest(gate, func(sync func() error) error {
				if fault == "sync" {
					return errors.New("injected directory sync failure")
				}
				err := sync()
				cancel()
				return err
			})
			err = handle.CompleteInputs(ctx)
			restore()
			if err == nil {
				t.Fatal("uncertain completion returned success")
			}
			handle.Release()
			if fault == "sync" && installed.WaitInputWriters(t.Context()) == nil {
				t.Fatal("uncertain durability supplied input proof")
			}
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			gate = f.open(t)
			installed, err = gate.InstallExecutionFloor(t.Context(), f.disposition)
			if err != nil {
				t.Fatal(err)
			}
			if err := installed.WaitInputWriters(t.Context()); err != nil {
				t.Fatalf("published completion did not recover: %v", err)
			}
		})
	}
}

func TestAssignmentInputDrainRejectsMalformedRecoveredCheckpoint(t *testing.T) {
	for _, field := range []string{"contract", "time"} {
		t.Run(field, func(t *testing.T) {
			f := newAdmissionFixture(t)
			gate := f.open(t)
			completeAdmissionInputs(t, beginAdmission(t, gate, f.assignment, f.acquireID))
			proof := admissionSnapshot(t, gate).Latest.InputDrain
			if err := gate.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(f.config.Directory, admissionTestState)
			document, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if field == "contract" {
				document = bytes.Replace(document, []byte(proof.Contract), []byte("STOPPED"), 1)
			} else {
				observed, err := json.Marshal(proof.ObservedAt)
				if err != nil {
					t.Fatal(err)
				}
				zero, err := json.Marshal(time.Time{})
				if err != nil {
					t.Fatal(err)
				}
				document = bytes.Replace(document, observed, zero, 1)
			}
			if err := os.WriteFile(path, document, 0o600); err != nil {
				t.Fatal(err)
			}
			if reopened, err := stageworkeragent.NewFileAssignmentAdmission(f.config); !errors.Is(err, stageworkeragent.ErrInputWritersUnproven) {
				if reopened != nil {
					_ = reopened.Close()
				}
				t.Fatalf("malformed input checkpoint recovered: %v", err)
			}
		})
	}
}
