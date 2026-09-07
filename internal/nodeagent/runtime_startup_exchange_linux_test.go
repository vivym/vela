package nodeagent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"golang.org/x/sys/unix"
)

func TestRuntimeStartupLedgerBeforeActualFactory(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root Node and independent non-root Runtime PID 1")
	}
	for _, mode := range []string{"mock-permit", "deny", "lost-response", "record-error"} {
		t.Run(mode, func(t *testing.T) {
			fixture := runtimeLaunchFixture(t)
			root, err := os.MkdirTemp("/run", "vela-startup-ledger-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(root) })
			if err := os.Chmod(root, 0o755); err != nil {
				t.Fatal(err)
			}
			runtimeRoot := filepath.Join(root, "runtime")
			if err := os.Mkdir(runtimeRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chown(runtimeRoot, 10001, 10001); err != nil {
				t.Fatal(err)
			}
			manifest, err := modelruntime.EncodeLaunchManifest(fixture.launch)
			if err != nil {
				t.Fatal(err)
			}
			manifestPath := filepath.Join(runtimeRoot, "launch.json")
			if err := os.WriteFile(manifestPath, manifest, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chown(manifestPath, 10001, 10001); err != nil {
				t.Fatal(err)
			}
			socket := filepath.Join(root, "node.sock")
			listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socket, Net: "unixpacket"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			if err := errors.Join(os.Chown(socket, 0, 10001), os.Chmod(socket, 0o660)); err != nil {
				t.Fatal(err)
			}
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(t.Context(), binary, "-test.run=^TestRuntimeStartupLedgerRuntimeHelper$", "-test.v", "-test.timeout=25s")
			command.Env = []string{"VELA_STARTUP_LEDGER_HELPER=" + mode, "VELA_STARTUP_LEDGER_ROOT=" + runtimeRoot, "VELA_STARTUP_LEDGER_SOCKET=" + socket}
			command.SysProcAttr = &syscall.SysProcAttr{Cloneflags: unix.CLONE_NEWPID, Credential: &syscall.Credential{Uid: 10001, Gid: 10001}}
			input, err := command.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			reader, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
			command.ExtraFiles = []*os.File{writer}
			var output bytes.Buffer
			command.Stdout, command.Stderr = &output, &output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			_ = writer.Close()
			done := make(chan struct{})
			var waitErr error
			go func() { waitErr = command.Wait(); close(done) }()
			t.Cleanup(func() { _ = input.Close(); _ = command.Process.Kill(); <-done })
			if err := reader.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			var journal modelruntime.ExecutionJournalStatus
			if err := json.NewDecoder(reader).Decode(&journal); err != nil {
				t.Fatal(err)
			}
			fixture.binding.Pair.RuntimeJournalId, fixture.binding.Pair.RuntimeScope = journal.JournalID.String(), journal.Scope[:]
			fixture.binding.SigningKeyId, fixture.binding.Signature = "", nil
			fixture.binding, err = fixture.signer.Sign(fixture.binding)
			if err != nil {
				t.Fatal(err)
			}
			binding, err := journalbinding.Encode(fixture.binding)
			if err != nil {
				t.Fatal(err)
			}
			bindingPath := filepath.Join(runtimeRoot, "binding.json")
			if err := os.WriteFile(bindingPath, binding, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chown(bindingPath, 10001, 10001); err != nil {
				t.Fatal(err)
			}
			plan, err := VerifyRuntimeLaunchPlan("cpu-node", fixture.verifier, fixture.binding, fixture.wire)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := input.Write([]byte{'b'}); err != nil {
				t.Fatal(err)
			}
			if err := listener.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			connection, err := listener.AcceptUnix()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = connection.Close() })
			caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: plan.uid, GID: plan.gid})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = caller.Close() })
			observer, pods := startupPlanObserverFixture(t, plan, caller)
			ledger, directory := startupTestLedger(t)
			if mode == "record-error" {
				ledger.boundary = func(phase string) error {
					if phase == "after-append" {
						return errors.New("injected append uncertainty")
					}
					return nil
				}
			}
			record, recordErr := ledger.Record(t.Context(), plan, pods, observer, caller)
			if mode == "record-error" {
				if recordErr == nil {
					t.Fatal("uncertain Node record succeeded")
				}
			} else {
				if recordErr != nil || record.Request.JournalID != journal.JournalID || record.Request.JournalScope != journal.Scope {
					t.Fatalf("Node record: %+v %v", record, recordErr)
				}
				if _, err := ledger.RecordExit(t.Context(), journal.JournalID); !errors.Is(err, ErrRuntimeNamespaceOwnerLive) {
					t.Fatalf("live Runtime owner marked exited: %v", err)
				}
			}
			if _, err := os.Lstat(filepath.Join(runtimeRoot, "factory")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("backend started before Node record and reply")
			}
			lock, err := os.OpenFile(filepath.Join(runtimeRoot, "state", "execution-admission.lock"), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			lockErr := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
			_ = lock.Close()
			if !errors.Is(lockErr, unix.EWOULDBLOCK) {
				t.Fatalf("Runtime journal not locked during registration: %v", lockErr)
			}
			if mode == "lost-response" || mode == "record-error" {
				_ = caller.Close()
				_ = connection.Close()
			} else {
				// Mock authorization is deliberately outside the production ledger.
				decision, err := json.Marshal(modelruntime.BackendStartupDecision{SchemaVersion: 1, RequestDigest: sha256.Sum256(caller.Payload()), Permit: mode == "mock-permit"})
				if err != nil {
					t.Fatal(err)
				}
				if err := caller.Reply(t.Context(), decision); err != nil {
					t.Fatal(err)
				}
			}
			_ = input.Close()
			select {
			case <-done:
				if waitErr != nil || !strings.Contains(output.String(), "--- PASS: TestRuntimeStartupLedgerRuntimeHelper ") {
					t.Fatalf("actual Runtime: %s %v", output.String(), waitErr)
				}
				t.Log(output.String())
			case <-time.After(10 * time.Second):
				t.Fatal("actual Runtime did not finish")
			}
			if mode != "record-error" {
				if exit, err := ledger.RecordExit(t.Context(), journal.JournalID); err != nil || exit.OperationID != record.OperationID {
					t.Fatalf("exact owner exit not persisted: %v", err)
				}
			}
			if err := ledger.Close(); err != nil {
				t.Fatal(err)
			}
			recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = recovered.Close() })
			if _, err := recovered.Inspect(t.Context(), journal.JournalID); err != nil {
				t.Fatal("Node restart lost original startup")
			}
			_, exitErr := recovered.RecordExit(t.Context(), journal.JournalID)
			if mode == "record-error" && !errors.Is(exitErr, ErrRuntimeNamespaceOwnerLost) || mode != "record-error" && exitErr != nil {
				t.Fatalf("Node restart changed exit evidence: %v", exitErr)
			}
		})
	}
}

func TestRuntimeStartupLedgerRuntimeHelper(t *testing.T) {
	mode := os.Getenv("VELA_STARTUP_LEDGER_HELPER")
	if mode == "" {
		t.Skip("Runtime startup ledger subprocess helper")
	}
	if os.Getpid() != 1 || os.Geteuid() != 10001 {
		t.Fatal("Runtime fixture is not independent non-root PID 1")
	}
	root := os.Getenv("VELA_STARTUP_LEDGER_ROOT")
	manifest, err := modelruntime.LoadLaunchManifest(filepath.Join(root, "launch.json"))
	if err != nil {
		t.Fatal(err)
	}
	validator, err := stageauthority.NewValidator(map[string][]byte{"authority": make([]byte, 32)}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	state := modelruntime.ExecutionFloorStateConfig{Directory: filepath.Join(root, "state"), Initialize: true}
	if err := os.Mkdir(state.Directory, 0o700); err != nil {
		t.Fatal(err)
	}
	journal, err := modelruntime.PrepareExecutionJournal(t.Context(), manifest, validator, state)
	if err != nil {
		t.Fatal(err)
	}
	prepared := os.NewFile(3, "prepared-runtime-journal")
	if err := json.NewEncoder(prepared).Encode(journal); err != nil {
		t.Fatal(err)
	}
	_ = prepared.Close()
	var ready [1]byte
	if _, err := io.ReadFull(os.Stdin, ready[:]); err != nil || ready[0] != 'b' {
		t.Fatal("Registry fixture binding not ready")
	}
	verifier, err := journalbinding.NewVerifier(map[string][]byte{"registry-launch-test": ed25519.NewKeyFromSeed(bytes.Repeat([]byte{71}, 32)).Public().(ed25519.PublicKey)})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := journalbinding.LoadFile(filepath.Join(root, "binding.json"), verifier)
	if err != nil {
		t.Fatal(err)
	}
	state.Initialize = false
	epochs, err := modelruntime.NewFileEpochStore(filepath.Join(root, "epochs"))
	if err != nil {
		t.Fatal(err)
	}
	gate, err := modelruntime.NewNodeBackendStartupGate(os.Getenv("VELA_STARTUP_LEDGER_SOCKET"))
	if err != nil {
		t.Fatal(err)
	}
	factories, gates := 0, 0
	config := modelruntime.RuntimeServerConfig{Manifest: manifest, EpochStore: epochs, Validator: validator, SocketPath: filepath.Join(root, "runtime.sock"), CancelTimeout: time.Second,
		RegistryBinding: binding, RegistryVerifier: verifier, ExecutionFloor: &modelruntime.ExecutionFloorConfig{State: &state},
		BackendStartupGate: func(ctx context.Context, request modelruntime.BackendStartupRequest) error {
			gates++
			return gate(ctx, request)
		},
		BackendFactory: func(context.Context, modelruntime.LaunchRuntime, stageauthority.RuntimeBinding, modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
			factories++
			if err := os.WriteFile(filepath.Join(root, "factory"), []byte("called"), 0o600); err != nil {
				return nil, err
			}
			return modelruntime.NewFakeDiTRuntime(), nil
		}}
	server, err := modelruntime.StartRuntimeServer(t.Context(), config)
	want := 0
	if mode == "mock-permit" {
		want = 1
		if err != nil || server == nil {
			t.Fatalf("mock permitted startup: %v", err)
		}
		if err := server.Close(); err != nil {
			t.Fatal(err)
		}
	} else if server != nil || !errors.Is(err, modelruntime.ErrBackendStartupDenied) {
		t.Fatalf("startup unexpectedly allowed: %v", err)
	}
	if factories != want || gates != 1 {
		t.Fatalf("factory/gate calls: %d/%d", factories, gates)
	}
	retained, err := modelruntime.PrepareExecutionJournal(t.Context(), manifest, validator, state)
	if err != nil || retained.BackendLifecycle.State != modelruntime.BackendLifecycleUnresolved {
		t.Fatalf("startup nonce not retained: %v", err)
	}
	reopened, err := modelruntime.StartRuntimeServer(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if factories != want || gates != 1 {
		t.Fatal("recovery reissued startup")
	}
	t.Logf("mode=%s factories=%d gates=%d; recovery-only reopen", mode, factories, gates)
	_, _ = io.Copy(io.Discard, os.Stdin)
}
