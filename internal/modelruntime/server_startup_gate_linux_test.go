package modelruntime_test

import (
	"bytes"
	"context"
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

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/stageauthority"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

func TestRuntimeServerNodeStartupExchange(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires a root Node and non-root namespace PID-1 Runtime CPU fixture")
	}
	for _, mode := range []string{"permit", "deny", "wrong-digest", "malformed", "duplicate", "unknown-field", "noncanonical", "lost-response", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			root, err := os.MkdirTemp("/run", "vela-startup-")
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
			if err := os.Chown(runtimeRoot, 65532, 65532); err != nil {
				t.Fatal(err)
			}
			socket := filepath.Join(root, "node.sock")
			listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socket, Net: "unixpacket"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			if err := errors.Join(os.Chown(socket, 0, 65532), os.Chmod(socket, 0o660)); err != nil {
				t.Fatal(err)
			}
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(t.Context(), binary, "-test.run=^TestRuntimeServerNodeStartupHelper$", "-test.v", "-test.timeout=20s")
			command.Env = []string{"VELA_STARTUP_HELPER=" + mode, "VELA_STARTUP_ROOT=" + runtimeRoot, "VELA_STARTUP_SOCKET=" + socket}
			command.SysProcAttr = &syscall.SysProcAttr{Cloneflags: unix.CLONE_NEWPID, Credential: &syscall.Credential{Uid: 65532, Gid: 65532}}
			input, err := command.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			command.Stdout, command.Stderr = &output, &output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			var waitErr error
			go func() { waitErr = command.Wait(); close(done) }()
			t.Cleanup(func() { _ = input.Close(); _ = command.Process.Kill(); <-done })
			if err := listener.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			connection, err := listener.AcceptUnix()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = connection.Close() })
			caller, err := nodeagent.ReceiveRuntimeCaller(t.Context(), connection, nodeagent.RuntimeCallerCredentials{UID: 65532, GID: 65532})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = caller.Close() })
			payload := caller.Payload()
			request, err := modelruntime.ParseBackendStartupRequest(payload)
			if err != nil {
				t.Fatal(err)
			}
			state := readDurableExecutionState(t, filepath.Join(runtimeRoot, "state"))
			var scope [sha256.Size]byte
			if err := json.Unmarshal(state.Scope, &scope); err != nil {
				t.Fatal(err)
			}
			manifest, err := os.ReadFile(filepath.Join(runtimeRoot, "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			binding, err := os.ReadFile(filepath.Join(runtimeRoot, "binding.pb"))
			if err != nil {
				t.Fatal(err)
			}
			if state.BackendLifecycle == nil || state.BackendLifecycle.State != modelruntime.BackendLifecycleUnresolved ||
				request.JournalID.String() != state.ID || request.JournalScope != scope || request.NodeIdentity != "cpu-node" ||
				request.IncarnationID != state.BackendLifecycle.IncarnationID || request.LaunchDigest != state.BackendLifecycle.LaunchDigest ||
				request.LaunchDigest != sha256.Sum256(manifest) || request.RegistryBindingDigest != sha256.Sum256(binding) {
				t.Fatalf("request did not bind the persisted startup: %+v", request)
			}
			if _, err := os.Lstat(filepath.Join(runtimeRoot, "factories")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("backend factory ran before Node reply: %v", err)
			}
			// This is an independent process's lock attempt against the fixture's
			// known path, not production authentication of a Runtime-declared path.
			lock, err := os.OpenFile(filepath.Join(runtimeRoot, "state", durableStateLockName), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			lockErr := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
			_ = lock.Close()
			if !errors.Is(lockErr, unix.EWOULDBLOCK) {
				t.Fatalf("journal lock not held across Node exchange: %v", lockErr)
			}
			decision := modelruntime.BackendStartupDecision{SchemaVersion: 1, RequestDigest: sha256.Sum256(payload), Permit: mode != "deny"}
			if mode == "wrong-digest" {
				decision.RequestDigest[0] ^= 1
			}
			document, err := json.Marshal(decision)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "malformed":
				document = []byte("{")
			case "duplicate":
				document = bytes.Replace(document, []byte(`"permit":true`), []byte(`"permit":false,"permit":true`), 1)
			case "unknown-field":
				document = bytes.Replace(document, []byte(`"permit":true`), []byte(`"permit":true,"extra":1`), 1)
			case "noncanonical":
				document = append(document, ' ')
			}
			switch mode {
			case "lost-response":
				_ = caller.Close()
				_ = connection.Close()
			case "cancel":
				if _, err := input.Write([]byte{'c'}); err != nil {
					t.Fatal(err)
				}
			default:
				// This root process issues only an explicit CPU mock decision.
				if err := caller.Reply(t.Context(), document); err != nil {
					t.Fatal(err)
				}
			}
			_ = input.Close()
			select {
			case <-done:
				if waitErr != nil || !strings.Contains(output.String(), "--- PASS: TestRuntimeServerNodeStartupHelper ") {
					t.Fatalf("Runtime startup helper: %s %v", output.String(), waitErr)
				}
				t.Log(output.String())
			case <-time.After(10 * time.Second):
				t.Fatal("Runtime startup helper did not finish")
			}
		})
	}
}

func TestRuntimeServerNodeStartupHelper(t *testing.T) {
	mode := os.Getenv("VELA_STARTUP_HELPER")
	if mode == "" {
		t.Skip("Runtime startup subprocess helper")
	}
	if os.Getpid() != 1 || os.Geteuid() != 65532 || os.Getegid() != 65532 {
		t.Fatal("Runtime helper is not the independent non-root namespace owner")
	}
	root := os.Getenv("VELA_STARTUP_ROOT")
	config := journalRuntimeServerConfig(t)
	config.SocketPath = filepath.Join(root, "runtime.sock")
	config.ExecutionFloor.State.Directory = filepath.Join(root, "state")
	if err := os.Mkdir(config.ExecutionFloor.State.Directory, 0o700); err != nil {
		t.Fatal(err)
	}
	journal, err := modelruntime.PrepareExecutionJournal(t.Context(), config.Manifest, config.Validator, *config.ExecutionFloor.State)
	if err != nil {
		t.Fatal(err)
	}
	config.ExecutionFloor.State.Initialize = false
	config.RegistryBinding, config.RegistryVerifier = runtimeRegistryBinding(t, config, journal, nil)
	manifest, err := modelruntime.EncodeLaunchManifest(config.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := proto.MarshalOptions{Deterministic: true}.Marshal(config.RegistryBinding)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(os.WriteFile(filepath.Join(root, "manifest.json"), manifest, 0o600), os.WriteFile(filepath.Join(root, "binding.pb"), binding, 0o600)); err != nil {
		t.Fatal(err)
	}
	gate, err := modelruntime.NewNodeBackendStartupGate(os.Getenv("VELA_STARTUP_SOCKET"))
	if err != nil {
		t.Fatal(err)
	}
	gates, factories := 0, 0
	config.BackendStartupGate = func(ctx context.Context, request modelruntime.BackendStartupRequest) error {
		gates++
		return gate(ctx, request)
	}
	factory := config.BackendFactory
	config.BackendFactory = func(ctx context.Context, runtime modelruntime.LaunchRuntime, binding stageauthority.RuntimeBinding, backend modelruntime.ProcessBackendConfig) (modelruntime.Backend, error) {
		factories++
		if err := os.WriteFile(filepath.Join(root, "factories"), []byte{byte(factories)}, 0o600); err != nil {
			return nil, err
		}
		return factory(ctx, runtime, binding, backend)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	inputDone := make(chan struct{})
	go func() {
		var command [1]byte
		if count, _ := os.Stdin.Read(command[:]); count == 1 && command[0] == 'c' {
			cancel()
		}
		_, _ = io.Copy(io.Discard, os.Stdin)
		close(inputDone)
	}()
	server, err := modelruntime.StartRuntimeServer(ctx, config)
	wantFactories := 0
	if mode == "permit" {
		wantFactories = len(config.Manifest.Runtimes)
		if err != nil || server == nil {
			t.Fatalf("mock permit did not start AUX: %v", err)
		}
		if err := server.Close(); err != nil {
			t.Fatal(err)
		}
	} else if server != nil || !errors.Is(err, modelruntime.ErrBackendStartupDenied) || mode == "cancel" && !errors.Is(err, context.Canceled) {
		t.Fatalf("%s failed to reject before startup: %v", mode, err)
	}
	if gates != 1 || factories != wantFactories {
		t.Fatalf("startup calls: gates=%d factories=%d want=%d", gates, factories, wantFactories)
	}
	before := readDurableExecutionState(t, config.ExecutionFloor.State.Directory).BackendLifecycle
	if before == nil || before.State != modelruntime.BackendLifecycleUnresolved {
		t.Fatal("startup outcome lost its unresolved intent")
	}
	// Retain the actual gate to prove recovery never asks for another permit.
	recovered, err := modelruntime.StartRuntimeServer(t.Context(), config)
	if err != nil {
		t.Fatalf("recovery-only reopen: %v", err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	after := readDurableExecutionState(t, config.ExecutionFloor.State.Directory).BackendLifecycle
	if gates != 1 || factories != wantFactories || after == nil || *before != *after {
		t.Fatal("recovery dispatched a gate/factory or replaced the startup intent")
	}
	t.Logf("mode=%s gates=%d factories=%d; unresolved nonce retained across recovery", mode, gates, factories)
	// Keep the authenticated PID alive until the parent finishes its response.
	<-inputDone
}
