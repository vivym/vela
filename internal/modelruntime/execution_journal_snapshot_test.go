package modelruntime_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func snapshotIdentity(status modelruntime.ExecutionJournalStatus) modelruntime.ExecutionJournalIdentity {
	return modelruntime.ExecutionJournalIdentity{JournalID: status.JournalID, Scope: status.Scope, Storage: status.Storage}
}

func snapshotFiles(t *testing.T, directory string) ([]byte, []byte) {
	t.Helper()
	document, err := os.ReadFile(filepath.Join(directory, durableStateFileName))
	if err != nil {
		t.Fatal(err)
	}
	lock, err := os.ReadFile(filepath.Join(directory, durableStateLockName))
	if err != nil {
		t.Fatal(err)
	}
	return document, lock
}

func TestExecutionJournalSnapshotBeforeActualFactory(t *testing.T) {
	config := journalRuntimeServerConfig(t)
	original, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
	if err != nil {
		t.Fatal(err)
	}
	config.ExecutionFloor.State.Initialize = false
	config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, original, nil)
	var observed modelruntime.ExecutionJournalSnapshot
	gateCalls := 0
	config.BackendStartupGate = func(ctx context.Context, request modelruntime.BackendStartupRequest) error {
		gateCalls++
		document, lock := snapshotFiles(t, config.ExecutionFloor.State.Directory)
		snapshot, err := modelruntime.VerifyExecutionJournalSnapshot(document, lock, config.Manifest, config.Validator, snapshotIdentity(original))
		if err != nil || snapshot.MatchStartup(request) != nil || snapshot.Digest() != sha256.Sum256(document) {
			t.Fatalf("actual persisted startup snapshot: %+v %v", snapshot.Status(), err)
		}
		if _, err := modelruntime.PrepareExecutionJournal(ctx, config.Manifest, config.Validator, *config.ExecutionFloor.State); err == nil {
			t.Fatal("snapshot stole or released the live journal lock")
		}
		for _, fault := range []string{"nonce", "scope", "journal", "launch"} {
			changed := request
			switch fault {
			case "nonce":
				changed.IncarnationID = uuid.New()
			case "scope":
				changed.JournalScope[0] ^= 1
			case "journal":
				changed.JournalID = uuid.New()
			case "launch":
				changed.LaunchDigest[0] ^= 1
			}
			if snapshot.MatchStartup(changed) == nil {
				t.Fatalf("startup matched changed %s", fault)
			}
		}
		if (modelruntime.ExecutionJournalSnapshot{}).MatchStartup(request) == nil {
			t.Fatal("zero snapshot matched startup")
		}
		otherLaunch := config.Manifest
		otherLaunch.Runtimes = append([]modelruntime.LaunchRuntime(nil), config.Manifest.Runtimes...)
		otherLaunch.Runtimes[0].Command = []string{"/different-approved-backend"}
		other, err := modelruntime.VerifyExecutionJournalSnapshot(document, lock, otherLaunch, config.Validator, snapshotIdentity(original))
		if err != nil || other.MatchStartup(request) == nil {
			t.Fatalf("member scope incorrectly substituted for exact startup launch: %v", err)
		}
		observed = snapshot
		// Returned status and input buffers cannot mutate the verified snapshot.
		status := snapshot.Status()
		status.BackendLifecycle.IncarnationID = uuid.New()
		clear(document)
		clear(lock)
		if snapshot.MatchStartup(request) != nil {
			t.Fatal("snapshot retained mutable input aliases")
		}
		return nil // Explicit test permit; snapshot matching is not a Node issuer.
	}
	server, err := modelruntime.StartRuntimeServer(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
	if err != nil || gateCalls != 1 || recovered != observed.Status() {
		t.Fatalf("snapshot and live recovery disagree: %+v %+v %v", recovered, observed.Status(), err)
	}
}

func TestExecutionJournalSnapshotRejectsUnboundAndMalformedDocuments(t *testing.T) {
	config := journalRuntimeServerConfig(t)
	original, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
	if err != nil {
		t.Fatal(err)
	}
	document, lock := snapshotFiles(t, config.ExecutionFloor.State.Directory)
	for _, fault := range []string{"empty", "oversized", "truncated", "duplicate", "unknown", "noncanonical", "trailing", "legacy",
		"identity", "scope", "storage", "lock", "empty-expectation", "nil-verifier", "manifest-scope", "lifecycle", "nonce", "watermark", "missing-history"} {
		t.Run(fault, func(t *testing.T) {
			wire, lockWire := bytes.Clone(document), bytes.Clone(lock)
			expected := snapshotIdentity(original)
			manifest := config.Manifest
			validator := config.Validator
			state := readDurableExecutionState(t, config.ExecutionFloor.State.Directory)
			switch fault {
			case "empty":
				wire = nil
			case "oversized":
				wire = bytes.Repeat([]byte{' '}, (12<<20)+1)
			case "truncated":
				wire = wire[:len(wire)/2]
			case "duplicate":
				wire = bytes.Replace(wire, []byte(`"schema_version":8`), []byte(`"schema_version":8,"schema_version":8`), 1)
			case "unknown":
				wire = bytes.Replace(wire, []byte(`"schema_version":8`), []byte(`"schema_version":8,"unexpected":true`), 1)
			case "noncanonical":
				wire = append(wire, ' ')
			case "trailing":
				wire = append(wire, document...)
			case "legacy":
				state.SchemaVersion = 5
				wire = encodeDurableExecutionState(t, state)
			case "identity":
				expected.JournalID = uuid.New()
			case "scope":
				expected.Scope[0] ^= 1
			case "storage":
				expected.Storage.Lock.Inode++
			case "lock":
				lockWire = []byte(uuid.NewString())
			case "empty-expectation":
				expected = modelruntime.ExecutionJournalIdentity{}
			case "nil-verifier":
				validator = nil
			case "manifest-scope":
				manifest.WorkerInstanceEpoch++
			case "lifecycle":
				state.BackendLifecycle = nil
				wire = encodeDurableExecutionState(t, state)
			case "nonce":
				state.BackendLifecycle.State = modelruntime.BackendLifecycleUnresolved
				wire = encodeDurableExecutionState(t, state)
			case "watermark":
				state.Highest = -1
				wire = encodeDurableExecutionState(t, state)
			case "missing-history":
				state.Highest = 1
				wire = encodeDurableExecutionState(t, state)
			}
			got, err := modelruntime.VerifyExecutionJournalSnapshot(wire, lockWire, manifest, validator, expected)
			if err == nil || got != (modelruntime.ExecutionJournalSnapshot{}) {
				t.Fatalf("accepted %s snapshot: %+v %v", fault, got.Status(), err)
			}
		})
	}
	after, afterLock := snapshotFiles(t, config.ExecutionFloor.State.Directory)
	if !bytes.Equal(after, document) || !bytes.Equal(afterLock, lock) {
		t.Fatal("snapshot validation changed files")
	}
}

func TestExecutionJournalSnapshotVerifiesCompleteSignedHistory(t *testing.T) {
	for _, kind := range []string{"pending", "drained", "renewal", "non-admission", "terminal-non-admission"} {
		t.Run(kind, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			if kind == "drained" {
				readyDrainOutput(t, f, f.backend.FakeRuntime, f.authorities[0])
				response, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
				if err != nil || response.GetReceipt() == nil {
					t.Fatalf("drained history fixture: %v %v", response, err)
				}
			} else {
				prepareFloorRuntime(t, f.supervisor, f.authorities[0])
			}
			switch kind {
			case "renewal":
				f.clock.Advance(time.Second)
				renewal := renewWatchdogAuthority(t, f.signer, f.authorities[0], f.clock.Now())
				response, err := f.supervisor.Status(t.Context(), &velav1.ModelRuntimeServiceStatusRequest{Authority: renewal})
				if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
					t.Fatalf("renewal fixture: %v %v", response, err)
				}
			case "non-admission":
				if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
					t.Fatal(err)
				}
				if _, err := f.supervisor.CheckpointNonAdmission(t.Context(), f.authorities[1]); err != nil {
					t.Fatal(err)
				}
			case "terminal-non-admission":
				disposition := unsignedTerminalAllocation(t, f)
				if _, err := f.supervisor.InstallExecutionFloor(t.Context(), disposition); err != nil {
					t.Fatal(err)
				}
				if _, err := f.supervisor.CheckpointTerminalNonAdmission(t.Context(), disposition, disposition.Allocations[1].StageAllocationId); err != nil {
					t.Fatal(err)
				}
			}
			f.supervisor.Close()
			config := recoveredRuntimeServerConfig(t, f, directory)
			original, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
			if err != nil {
				t.Fatal(err)
			}
			document, lock := snapshotFiles(t, directory)
			// Historical expiry does not invalidate durable restrictions.
			f.clock.Advance(2 * time.Minute)
			snapshot, err := modelruntime.VerifyExecutionJournalSnapshot(document, lock, config.Manifest, config.Validator, snapshotIdentity(original))
			if err != nil || snapshot.Status() != original || snapshot.Status().Highest != 10 || snapshot.Status().RetainedExecutions != 1 {
				t.Fatalf("signed history snapshot differs from recovery: %+v %v", snapshot.Status(), err)
			}
			if (snapshot.Status().PendingExecutions == 0) != (kind == "drained") {
				t.Fatal("snapshot lost pending versus drained history")
			}
			envelopeCount, terminalCount := snapshot.NonAdmissions()
			if (envelopeCount == 1) != (kind == "non-admission") || (terminalCount == 1) != (kind == "terminal-non-admission") {
				t.Fatalf("snapshot omitted non-admission history: %d %d", envelopeCount, terminalCount)
			}
			state := readDurableExecutionState(t, directory)
			faults := []string{"signature", "missing-retained", "nested-signature"}
			if kind == "drained" || kind == "non-admission" || kind == "terminal-non-admission" {
				faults = append(faults, "checkpoint-contract")
			}
			for _, fault := range faults {
				changed := state
				switch fault {
				case "signature":
					changed.Authority = bytes.Clone(state.Authority)
					changed.Authority[len(changed.Authority)-1] ^= 1
				case "missing-retained":
					changed.Executions = json.RawMessage("[]")
				case "nested-signature":
					var records []retainedExecutionDocument
					if err := json.Unmarshal(changed.Executions, &records); err != nil {
						t.Fatal(err)
					}
					records[0].Candidates.Accepted[len(records[0].Candidates.Accepted)-1] ^= 1
					changed.Executions, err = json.Marshal(records)
					if err != nil {
						t.Fatal(err)
					}
				case "checkpoint-contract":
					switch kind {
					case "drained":
						var records []retainedExecutionDocument
						if err := json.Unmarshal(changed.Executions, &records); err != nil {
							t.Fatal(err)
						}
						records[0].Drain.Result.Contract = "unproven"
						changed.Executions, err = json.Marshal(records)
					case "non-admission":
						var records []nonAdmissionDocument
						if err := json.Unmarshal(changed.NonAdmissions, &records); err != nil {
							t.Fatal(err)
						}
						records[0].Contract = "unproven"
						changed.NonAdmissions, err = json.Marshal(records)
					case "terminal-non-admission":
						var records []terminalNonAdmissionDocument
						if err := json.Unmarshal(changed.TerminalNonAdmissions, &records); err != nil {
							t.Fatal(err)
						}
						records[0].Contract = "unproven"
						changed.TerminalNonAdmissions, err = json.Marshal(records)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				wire := encodeDurableExecutionState(t, changed)
				if got, err := modelruntime.VerifyExecutionJournalSnapshot(wire, lock, config.Manifest, config.Validator, snapshotIdentity(original)); err == nil || got != (modelruntime.ExecutionJournalSnapshot{}) {
					t.Fatalf("accepted corrupt %s history: %s %v", kind, fault, err)
				}
			}
			// A different signing key must reject actual retained evidence.
			untrusted, err := stageauthority.NewValidator(map[string][]byte{"authority-v1": bytes.Repeat([]byte{0xaa}, 32)}, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := modelruntime.VerifyExecutionJournalSnapshot(document, lock, config.Manifest, untrusted, snapshotIdentity(original)); err == nil {
				t.Fatal("snapshot ignored its trusted signature verifier")
			}
			launch, err := modelruntime.EncodeLaunchManifest(config.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			state.BackendLifecycle = &modelruntime.BackendLifecycleStatus{State: modelruntime.BackendLifecycleUnresolved,
				IncarnationID: uuid.New(), LaunchDigest: sha256.Sum256(launch), RecordedAt: time.Now().UTC()}
			running, err := modelruntime.VerifyExecutionJournalSnapshot(encodeDurableExecutionState(t, state), lock, config.Manifest, config.Validator, snapshotIdentity(original))
			if err != nil {
				t.Fatalf("valid running history was rejected: %v", err)
			}
			request := modelruntime.BackendStartupRequest{SchemaVersion: 1, NodeIdentity: "cpu-node", RegistryBindingDigest: sha256.Sum256([]byte("fixture")),
				JournalID: original.JournalID, JournalScope: original.Scope, IncarnationID: state.BackendLifecycle.IncarnationID, LaunchDigest: state.BackendLifecycle.LaunchDigest}
			if request.Validate() != nil || !errors.Is(running.MatchStartup(request), modelruntime.ErrBackendStartupDenied) {
				t.Fatal("retained restriction snapshot permitted a matching first-start request")
			}
		})
	}
}
