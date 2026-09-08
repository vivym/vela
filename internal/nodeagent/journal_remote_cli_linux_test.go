package nodeagent

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
)

func TestJournalServerActualRemoteCLI(t *testing.T) {
	if _, err := os.Stat("/vela-model-runtime"); err != nil {
		t.Skip("requires actual CLI in the native remote startup sandbox")
	}
	for _, mode := range []string{"permit", "deny", "changed-config", "workload-owned", "writable", "symlink", "wrong-scope", "wrong-incarnation", "workload-parent", "hardlink", "fifo", "oversize", "duplicate", "unknown", "socket-alias", "bad-key", "missing-file", "empty"} {
		t.Run(mode, func(t *testing.T) {
			var directory, eventPath string
			f := newJournalEndpointConfiguredFixture(t, 1, func(manifest *modelruntime.LaunchManifest, root string) {
				directory = filepath.Join(root, "workload")
				if err := os.Mkdir(directory, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.Chown(directory, 65532, 65532); err != nil {
					t.Fatal(err)
				}
				eventPath = filepath.Join(directory, "events.log")
				for _, path := range []string{"inputs", "outputs"} {
					p := filepath.Join(directory, path)
					if err := os.Mkdir(p, 0o700); err != nil {
						t.Fatal(err)
					}
					if err := os.Chown(p, 65532, 65532); err != nil {
						t.Fatal(err)
					}
				}
				manifest.Runtimes[0].Command = []string{"/runtime-command.test", "-test.run=^TestModelRuntimeCommandDriverHelper$"}
				manifest.Runtimes[0].Environment = []string{"GO_WANT_VELA_MODEL_RUNTIME_DRIVER=1", "TEST_VELA_MODEL_RUNTIME_DRIVER_EVENTS=" + eventPath}
				// The actual driver is a race-instrumented test binary, whose
				// default exit delay alone is one second. Match the CLI fixture's
				// five-second driver budgets instead of the fake backend defaults.
				manifest.Runtimes[0].InitializationTimeout, manifest.Runtimes[0].ShutdownTimeout = "5s", "5s"
				manifest.Runtimes[0].ScratchRoot, manifest.Runtimes[0].InputRoot, manifest.Runtimes[0].OutputRoot = directory, filepath.Join(directory, "inputs"), filepath.Join(directory, "outputs")
			}, true)
			server, done := startJournalTestServer(t, f, 30*time.Second)
			root := filepath.Dir(f.listener.Addr().String())
			startupPath := filepath.Join(root, "startup.sock")
			listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: startupPath, Net: "unixpacket"})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = listener.Close() }()
			if err := errors.Join(os.Chown(startupPath, 0, 65532), os.Chmod(startupPath, 0o660)); err != nil {
				t.Fatal(err)
			}
			keys, err := stageauthority.DeriveVerifierKeyring(map[string][]byte{"journal-test": bytes.Repeat([]byte{73}, 32)})
			if err != nil {
				t.Fatal(err)
			}
			seed := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{17}, 32))
			bootstrap := modelruntime.RemoteRuntimeBootstrap{SchemaVersion: 1, Manifest: f.manifest, AuthorityKeys: keys, RegistryKeys: map[string][]byte{"registry": seed.Public().(ed25519.PublicKey)}, RegistryBinding: journalRemoteServerBinding(t, f), Identity: f.identity, Startup: f.startup,
				JournalSocket: f.listener.Addr().String(), StartupSocket: startupPath, RuntimeSocket: filepath.Join(directory, "runtime.sock"), JournalTimeout: 5 * time.Second, CancelTimeout: time.Second, ShutdownTimeout: 5 * time.Second}
			wire, err := modelruntime.EncodeRemoteRuntimeBootstrap(bootstrap)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "wrong-scope" {
				bootstrap.Identity.Scope[0] ^= 1
				wire, err = json.Marshal(bootstrap)
			}
			if mode == "wrong-incarnation" {
				bootstrap.Startup.IncarnationID[0] ^= 1
				wire, err = json.Marshal(bootstrap)
			}
			if mode == "socket-alias" {
				bootstrap.StartupSocket = bootstrap.JournalSocket
				wire, err = json.Marshal(bootstrap)
			}
			if mode == "bad-key" {
				bootstrap.RegistryKeys["registry"][0] ^= 1
				wire, err = json.Marshal(bootstrap)
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "changed-config" {
				wire = append(wire, ' ')
			}
			if mode == "duplicate" {
				wire = append([]byte(`{"schema_version":1,`), wire[1:]...)
			}
			if mode == "unknown" {
				wire = append([]byte(`{"unknown":true,`), wire[1:]...)
			}
			if mode == "oversize" {
				wire = bytes.Repeat([]byte{' '}, modelruntime.MaximumRemoteBootstrapBytes+1)
			}
			if mode == "empty" {
				wire = nil
			}
			path := filepath.Join(root, "bootstrap.json")
			if err := os.WriteFile(path, wire, 0o444); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "workload-owned":
				if err := os.Chown(path, 65532, 65532); err != nil {
					t.Fatal(err)
				}
			case "writable":
				if err := os.Chmod(path, 0o644); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(path, path+".link"); err != nil {
					t.Fatal(err)
				}
				path += ".link"
			case "workload-parent":
				other := filepath.Join(directory, "bootstrap.json")
				if err := os.Rename(path, other); err != nil {
					t.Fatal(err)
				}
				path = other
			case "hardlink":
				if err := os.Link(path, path+".link"); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := syscall.Mkfifo(path, 0o444); err != nil {
					t.Fatal(err)
				}
			case "missing-file":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.runtime.input.Encode(journalEndpointControl{ExecRemoteCLI: path}); err != nil {
				t.Fatal(err)
			}
			if mode == "permit" || mode == "deny" {
				connection := journalEndpointAccept(t, listener)
				caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
				if err != nil {
					t.Fatal(err)
				}
				request, err := modelruntime.ParseBackendStartupRequest(caller.Payload())
				observed, inspectErr := caller.Inspect(t.Context())
				if err != nil || inspectErr != nil || observed.HostPID != int32(f.runtime.process.Pid) || request.JournalID != f.identity.JournalID || request.IncarnationID != f.startup.IncarnationID || request.RegistryBindingDigest != sha256.Sum256(bootstrap.RegistryBinding) || request.SchemaVersion != 2 || request.BootstrapDigest != sha256.Sum256(wire) || request.BootstrapPath != path {
					t.Fatalf("actual CLI requested wrong owner/startup: %v %v", err, inspectErr)
				}
				if _, err := os.Stat(eventPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("actual backend started before Node decision")
				}
				decision, err := json.Marshal(modelruntime.BackendStartupDecision{SchemaVersion: 1, RequestDigest: sha256.Sum256(caller.Payload()), Permit: mode == "permit"})
				if err != nil {
					t.Fatal(err)
				}
				if err := caller.Reply(t.Context(), decision); err != nil {
					t.Fatal(err)
				}
				_ = caller.Close()
				_ = connection.Close()
			}
			if mode == "permit" {
				deadline := time.Now().Add(10 * time.Second)
				for {
					data, _ := os.ReadFile(eventPath)
					_, socketErr := os.Stat(bootstrap.RuntimeSocket)
					if strings.Contains(string(data), "initialize\n") && socketErr == nil {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("actual process backend never initialized")
					}
					time.Sleep(10 * time.Millisecond)
				}
				reply := journalServerRequest(t, f.worker, journalEndpointControl{WorkerSocket: bootstrap.RuntimeSocket, WorkerAction: "discover-only", Manifest: &f.manifest}).Discovery
				if len(reply.GetIdentities()) != 1 || reply.Identities[0].GetModelRuntimeEpoch() != 2 {
					t.Fatalf("actual remote CLI discovery: %v", reply)
				}
				if err := f.runtime.process.Signal(syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-f.runtime.done:
			case <-time.After(10 * time.Second):
				t.Fatal("actual CLI did not exit")
			}
			state := f.runtime.command.ProcessState
			if (mode == "permit") != state.Success() {
				t.Fatalf("actual CLI exit mode=%s: %v", mode, state)
			}
			data, readErr := os.ReadFile(eventPath)
			if mode == "permit" {
				if readErr != nil || string(data) != "initialize\nshutdown\n" {
					t.Fatalf("actual backend lifecycle: %q %v", data, readErr)
				}
			} else if !errors.Is(readErr, os.ErrNotExist) {
				t.Fatal("rejected startup initialized backend")
			}
			if _, err := os.Stat(bootstrap.RuntimeSocket); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("CLI left runtime socket")
			}
			entries, err := os.ReadDir(directory)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				allowed := entry.Name() == "inputs" || entry.Name() == "outputs" || entry.Name() == "events.log" || (mode == "workload-parent" && entry.Name() == "bootstrap.json")
				if !allowed {
					t.Fatalf("unexpected workload state: %s", entry.Name())
				}
			}
			status, err := f.owner.Status(t.Context())
			if err != nil || status.Highest != 0 || status.BackendLifecycle != f.startup {
				t.Fatalf("CLI rewrote Node startup history: %v", err)
			}
			if err := server.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			assertJournalServerJoined(t, server, done)
			t.Logf("actual CLI and process backend: mode=%s exit=%s Node journal original incarnation retained; no local epoch/journal allocation", mode, state.String())
		})
	}
}
