package modelruntime_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func readSealedHistory(t *testing.T, directory string) []retainedExecutionDocument {
	t.Helper()
	state := readDurableExecutionState(t, directory)
	var records []retainedExecutionDocument
	if err := json.Unmarshal(state.Executions, &records); err != nil {
		t.Fatal(err)
	}
	return records
}

func TestDurableSealedReceiptReplaysThroughWorkerAfterEpochAndAuthorityExpiry(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	authority := f.authorities[0]
	readyDrainOutput(t, f, f.backend.FakeRuntime, authority)
	response, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: authority})
	if err != nil || response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || response.GetReceipt() == nil {
		t.Fatalf("original seal: %v %v", response, err)
	}
	saved := proto.Clone(response.GetReceipt()).(*velav1.LocalMaterializationReceipt)
	records := readSealedHistory(t, directory)
	if len(records) != 1 || records[0].Seal == nil || records[0].Drain == nil || !bytes.Equal(records[0].Seal.Authority, records[0].Drain.Authority) {
		t.Fatal("successful seal was acknowledged without durable receipt and exact drain")
	}
	if _, err := f.supervisor.InstallExecutionFloor(t.Context(), f.disposition(t)); err != nil {
		t.Fatal(err)
	}
	if err := f.supervisor.Shutdown(); err != nil {
		t.Fatal(err)
	}
	recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now().Add(2*time.Minute))
	if _, err := recovered.validator.Validate(authority, recovered.bindings[0]); err == nil {
		t.Fatal("old execution authority unexpectedly remains valid")
	}
	client, _ := serveRuntimeServer(t, recovered.supervisor)
	// The actual Worker adapter consumes the existing RPC's original identity,
	// including the old epoch. It gets receipt data, never fresh execution rights.
	agent, err := stageworkeragent.New(stageworkeragent.Config{Members: []stageworkeragent.RuntimeMember{{ID: authority.GetMembers()[0].GetWorkerMemberId(), Client: client}}})
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(directory, durableStateFileName))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		receipt, err := agent.SealOutput(t.Context(), authority)
		if err != nil || !proto.Equal(receipt, saved) {
			t.Fatalf("Worker did not recover original receipt: %v %v", receipt, err)
		}
		receipt.OutputManifestJson[0] ^= 1 // No mutable replay aliases.
	}
	after, err := os.ReadFile(filepath.Join(directory, durableStateFileName))
	if err != nil || !bytes.Equal(before, after) || recovered.backend.calls.Load() != 0 {
		t.Fatal("receipt recovery changed journal or entered replacement backend")
	}
	start, err := client.StartStage(t.Context(), &velav1.ModelRuntimeServiceStartStageRequest{Authority: authority})
	if err != nil || start.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE {
		t.Fatalf("receipt replay restored execution: %v %v", start, err)
	}
	for _, mode := range []string{"signature", "unseen-renewal", "different-allocation", "canceled"} {
		changed := proto.Clone(authority).(*velav1.StageAuthority)
		ctx := t.Context()
		switch mode {
		case "signature":
			changed.Signature[0] ^= 1
		case "unseen-renewal":
			changed = renewWatchdogAuthority(t, f.signer, authority, f.clock.Now().Add(time.Second))
		case "different-allocation":
			changed = f.authority(t, 0, 12)
		case "canceled":
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(ctx)
			cancel()
		}
		invalid, _ := recovered.supervisor.SealOutput(ctx, &velav1.ModelRuntimeServiceSealOutputRequest{Authority: changed})
		if invalid.GetReceipt() != nil {
			t.Fatal("receipt replay accepted", mode)
		}
	}
}

func TestDurableSealedReceiptPrecedesDrainAndWithholdsIncompleteOutcome(t *testing.T) {
	for _, failure := range []string{"drain-pending", "receipt-sync", "drain-sync"} {
		t.Run(failure, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			backend := &executionDrainBackend{FakeRuntime: modelruntime.NewFakeDiTRuntime()}
			f := newExecutionDrainFixture(t, directory, backend)
			readyDrainOutput(t, f, backend.FakeRuntime, f.authorities[0])
			if failure == "drain-pending" {
				backend.failure = "error"
			}
			writes := 0
			restore := modelruntime.SetExecutionStateSyncHookForTest(f.supervisor, func(syncDirectory func() error) error {
				writes++
				if failure == "receipt-sync" && writes == 1 || failure == "drain-sync" && writes == 2 {
					return errors.New("injected sealed-output fsync uncertainty")
				}
				return syncDirectory()
			})
			response, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
			restore()
			if err != nil || response.GetReceipt() != nil || response.GetDecision() == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("incomplete seal outcome acknowledged: %v %v", response, err)
			}
			records := readSealedHistory(t, directory)
			if len(records) != 1 || records[0].Seal == nil || (records[0].Drain != nil) != (failure == "drain-sync") {
				t.Fatalf("wrong persistence boundary: %+v", records)
			}
			if failure == "receipt-sync" && backend.drainCalls.Load() != 0 {
				t.Fatal("drain entered before receipt durability")
			}
			if failure == "drain-pending" {
				backend.failure = ""
				retried, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
				if err != nil || retried.GetReceipt() == nil {
					t.Fatalf("same live execution failed to finish drain: %v %v", retried, err)
				}
			}
			f.supervisor.Close()
			recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now().Add(2*time.Minute))
			replay, err := recovered.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
			if err != nil || (replay.GetReceipt() != nil) != (failure != "receipt-sync") {
				t.Fatalf("recovery fabricated or lost completed seal: %v %v", replay, err)
			}
			if recovered.backend.calls.Load() != 0 {
				t.Fatal("recovering receipt entered replacement backend")
			}
		})
	}
}

func TestDurableSealedReceiptBindsConfirmedRenewal(t *testing.T) {
	directory := privateExecutionStateDirectory(t)
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	original := f.authorities[0]
	readyDrainOutput(t, f, f.backend.FakeRuntime, original)
	f.clock.Advance(time.Second)
	renewal := renewWatchdogAuthority(t, f.signer, original, f.clock.Now())
	response, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: renewal})
	if err != nil || response.GetReceipt() == nil {
		t.Fatalf("renewed seal failed: %v %v", response, err)
	}
	f.supervisor.Close()
	recovered := durableExecutionFixture(t, directory, false, "", 10, f.clock.Now().Add(2*time.Minute))
	replayed, err := recovered.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: renewal})
	if err != nil || !proto.Equal(replayed.GetReceipt(), response.GetReceipt()) {
		t.Fatalf("confirmed renewal receipt lost: %v %v", replayed, err)
	}
	old, err := recovered.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: original})
	if err != nil || old.GetReceipt() != nil {
		t.Fatalf("old envelope substituted for sealed renewal: %v %v", old, err)
	}
}

func TestDurableSealedReceiptRejectsCorruptionOnRecoveryAndSnapshot(t *testing.T) {
	for _, fault := range []string{"manifest", "digest", "id", "size", "time", "unknown", "authority", "confirmed", "drain", "legacy-schema"} {
		t.Run(fault, func(t *testing.T) {
			directory := privateExecutionStateDirectory(t)
			f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
			readyDrainOutput(t, f, f.backend.FakeRuntime, f.authorities[0])
			sealed, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
			if err != nil || sealed.GetReceipt() == nil {
				t.Fatal("fixture seal failed", err)
			}
			f.supervisor.Close()
			config := recoveredRuntimeServerConfig(t, f, directory)
			identity, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
			if err != nil {
				t.Fatal(err)
			}
			state := readDurableExecutionState(t, directory)
			records := readSealedHistory(t, directory)
			receipt := proto.Clone(sealed.GetReceipt()).(*velav1.LocalMaterializationReceipt)
			switch fault {
			case "manifest":
				receipt.OutputManifestJson[0] ^= 1
			case "digest":
				receipt.ManifestSha256[0] ^= 1
			case "id":
				receipt.ReceiptId = "wrong-receipt"
			case "size":
				receipt.TotalSizeBytes = -1
			case "time":
				receipt.SealedAt = nil
			case "unknown":
				receipt.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "authority":
				records[0].Seal.Authority[0] ^= 1
			case "confirmed":
				records[0].Candidates.Confirmed = nil
			case "drain":
				records[0].Drain.Result.AuthorityDigest = sha256.Sum256([]byte("wrong"))
			case "legacy-schema":
				state.SchemaVersion = 6
			}
			records[0].Seal.Receipt, err = proto.MarshalOptions{Deterministic: true}.Marshal(receipt)
			if err != nil {
				t.Fatal(err)
			}
			state.Executions, err = json.Marshal(records)
			if err != nil {
				t.Fatal(err)
			}
			wire := encodeDurableExecutionState(t, state)
			if err := os.WriteFile(filepath.Join(directory, durableStateFileName), wire, 0o600); err != nil {
				t.Fatal(err)
			}
			lock, err := os.ReadFile(filepath.Join(directory, durableStateLockName))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := modelruntime.VerifyExecutionJournalSnapshot(wire, lock, config.Manifest, config.Validator, snapshotIdentity(identity)); err == nil {
				t.Fatal("snapshot accepted corrupt sealed history")
			}
			config.ExecutionFloor.State.UpgradeV6 = fault == "legacy-schema"
			if _, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State); err == nil {
				t.Fatal("recovery accepted corrupt sealed history")
			}
		})
	}
}

func TestExecutionJournalUpgradeV6PreservesBackendLifecycle(t *testing.T) {
	config := journalRuntimeServerConfig(t)
	server, err := modelruntime.StartRuntimeServer(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	config.ExecutionFloor.State.Initialize = false
	original, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
	if err != nil {
		t.Fatal(err)
	}
	state := readDurableExecutionState(t, config.ExecutionFloor.State.Directory)
	state.SchemaVersion = 6
	wire := encodeDurableExecutionState(t, state)
	path := filepath.Join(config.ExecutionFloor.State.Directory, durableStateFileName)
	if err := os.WriteFile(path, wire, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State); err == nil {
		t.Fatal("normal recovery implicitly upgraded schema 6")
	}
	config.ExecutionFloor.State.UpgradeV6 = true
	upgraded, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
	if err != nil || upgraded != original || upgraded.BackendLifecycle.State != modelruntime.BackendLifecycleUnresolved {
		t.Fatalf("schema-6 upgrade lost original lifecycle: %+v %v", upgraded, err)
	}
}

func TestDurableSealedReceiptSurvivesActualProcessCrash(t *testing.T) {
	for _, phase := range []string{"receipt-renamed", "receipt-synced", "drain-renamed", "drain-synced", "response-created"} {
		t.Run(phase, func(t *testing.T) {
			base := t.TempDir()
			directory := filepath.Join(base, "journal")
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestDurableSealedReceiptCrashHelper$", "-test.v", "-test.timeout=15s")
			command.Env = append(os.Environ(), "VELA_SEALED_CRASH_ROOT="+base, "VELA_SEALED_CRASH_PHASE="+phase)
			output, err := command.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 93 || bytes.Contains(output, []byte("WARNING: DATA RACE")) {
				t.Fatalf("actual seal crash: %v %s", err, output)
			}
			wire, err := os.ReadFile(filepath.Join(base, "authority.pb"))
			if err != nil {
				t.Fatal(err)
			}
			authority := &velav1.StageAuthority{}
			if err := proto.Unmarshal(wire, authority); err != nil {
				t.Fatal(err)
			}
			records := readSealedHistory(t, directory)
			wantReceipt := phase == "drain-renamed" || phase == "drain-synced" || phase == "response-created"
			if len(records) != 1 || records[0].Seal == nil || (records[0].Drain != nil) != wantReceipt {
				t.Fatal("crash occurred at the wrong durable boundary")
			}
			saved := &velav1.LocalMaterializationReceipt{}
			if err := proto.Unmarshal(records[0].Seal.Receipt, saved); err != nil {
				t.Fatal(err)
			}
			originalID := readDurableExecutionState(t, directory).ID
			recovered := durableExecutionFixture(t, directory, false, "", 10, authority.GetIssuedAt().AsTime().Add(2*time.Minute))
			for range 2 {
				response, err := recovered.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: authority})
				if err != nil || (response.GetReceipt() != nil) != wantReceipt || wantReceipt && !proto.Equal(response.GetReceipt(), saved) {
					t.Fatalf("crashed receipt recovery: %v %v", response, err)
				}
			}
			if recovered.backend.calls.Load() != 0 || readDurableExecutionState(t, directory).ID != originalID {
				t.Fatal("process recovery reentered backend or reset journal")
			}
		})
	}
}

func TestDurableSealedReceiptCrashHelper(t *testing.T) {
	base := os.Getenv("VELA_SEALED_CRASH_ROOT")
	if base == "" {
		return
	}
	phase := os.Getenv("VELA_SEALED_CRASH_PHASE")
	directory := filepath.Join(base, "journal")
	f := durableExecutionFixture(t, directory, true, "", 9, time.Time{})
	wire, err := proto.Marshal(f.authorities[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "authority.pb"), wire, 0o600); err != nil {
		t.Fatal(err)
	}
	readyDrainOutput(t, f, f.backend.FakeRuntime, f.authorities[0])
	writes := 0
	modelruntime.SetExecutionStateSyncHookForTest(f.supervisor, func(syncDirectory func() error) error {
		writes++
		if phase == "receipt-renamed" && writes == 1 || phase == "drain-renamed" && writes == 2 {
			os.Exit(93)
		}
		if err := syncDirectory(); err != nil {
			return err
		}
		if phase == "receipt-synced" && writes == 1 || phase == "drain-synced" && writes == 2 {
			os.Exit(93)
		}
		return nil
	})
	response, err := f.supervisor.SealOutput(t.Context(), &velav1.ModelRuntimeServiceSealOutputRequest{Authority: f.authorities[0]})
	if err != nil || response.GetReceipt() == nil || phase != "response-created" {
		t.Fatalf("unexpected seal crash boundary: %v %v", response, err)
	}
	os.Exit(93)
}
