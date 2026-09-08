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

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
	"golang.org/x/sys/unix"
)

func publicationFixture(t *testing.T, actualCLI bool) RuntimeBootstrapPublicationConfig {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("requires root Node in native Linux sandbox")
	}
	root, err := os.MkdirTemp("/run", "vela-publication-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	launch := runtimeLaunchFixture(t)
	if actualCLI {
		backend := &launch.bundle.WorkerInstances[0].ModelRuntimes[0]
		backend.Command = []string{"/runtime-command.test", "-test.run=^TestModelRuntimeCommandDriverHelper$"}
		backend.Environment = []string{"GO_WANT_VELA_MODEL_RUNTIME_DRIVER=1", "TEST_VELA_MODEL_RUNTIME_DRIVER_EVENTS=/var/lib/vela/stage-worker/scratch/events.log"}
		backend.InitializationTimeout, backend.ShutdownTimeout = "5s", "5s"
		launch.bind(t, 0, 0)
	}
	for _, name := range []string{"state", "publication"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	validator, err := stageauthority.NewValidator(map[string][]byte{"authority": make([]byte, 32)}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := modelruntime.RemoteStartupBindings(launch.launch)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := modelruntime.OpenExecutionJournalOwner(modelruntime.ExecutionJournalOwnerConfig{Manifest: launch.launch, Validator: validator, Routes: routes,
		State: modelruntime.ExecutionFloorStateConfig{Directory: filepath.Join(root, "state"), Initialize: true}, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if _, err := owner.RecordBackendStartupIntent(t.Context()); err != nil {
		t.Fatal(err)
	}
	status, err := owner.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	launch.binding.Pair.RuntimeJournalId, launch.binding.Pair.RuntimeScope = status.JournalID.String(), status.Scope[:]
	launch.binding.Signature, launch.binding.SigningKeyId = nil, ""
	launch.binding, err = launch.signer.Sign(launch.binding)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := VerifyRuntimeLaunchPlan("cpu-node", launch.verifier, launch.binding, launch.wire)
	if err != nil {
		t.Fatal(err)
	}
	return RuntimeBootstrapPublicationConfig{Directory: filepath.Join(root, "publication"), Plan: plan, Journal: owner,
		RegistryKeys:  map[string][]byte{"registry-launch-test": ed25519.NewKeyFromSeed(bytes.Repeat([]byte{71}, 32)).Public().(ed25519.PublicKey)},
		JournalSocket: filepath.Join(root, "node.sock"), StartupSocket: filepath.Join(root, "startup.sock"), RuntimeSocket: filepath.Join(root, "runtime.sock"),
		JournalTimeout: 5 * time.Second, CancelTimeout: time.Second, ShutdownTimeout: 5 * time.Second}
}

func TestRuntimeBootstrapPublicationAccessHelper(t *testing.T) {
	directory := os.Getenv("VELA_PUBLICATION_ACCESS")
	if directory == "" {
		t.Skip("non-root publication helper")
	}
	if os.Geteuid() != 10001 || os.Getegid() != 10001 {
		t.Fatal("wrong workload credentials")
	}
	wire, err := modelruntime.ReadRemoteRuntimeBootstrapFile(t.Context(), filepath.Join(directory, runtimeBootstrapFilename))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := modelruntime.RemoteRuntimeServerConfig(wire); err != nil {
		t.Fatal(err)
	}
	if _, err := os.ReadFile(filepath.Join(directory, runtimeBootstrapRecordName)); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("private record accessible: %v", err)
	}
	for _, name := range []string{runtimeBootstrapFilename, runtimeBootstrapRecordName, "forged"} {
		file, err := os.OpenFile(filepath.Join(directory, name), os.O_RDWR|os.O_CREATE, 0o600)
		if file != nil {
			_ = file.Close()
		}
		if !errors.Is(err, os.ErrPermission) {
			t.Fatalf("workload can write %s: %v", name, err)
		}
	}
	if err := os.Remove(filepath.Join(directory, runtimeBootstrapFilename)); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("workload can unlink bootstrap: %v", err)
	}
}

func TestRuntimeBootstrapPublication(t *testing.T) {
	config := publicationFixture(t, false)
	publication, err := PublishRuntimeBootstrap(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := publication.Bootstrap()
	if err != nil {
		t.Fatal(err)
	}
	status, err := config.Journal.Status(t.Context())
	if err != nil || bootstrap.Identity.JournalID != status.JournalID || bootstrap.Identity.Storage != status.Storage || bootstrap.Startup != status.BackendLifecycle || config.Plan.MatchManifest(bootstrap.Manifest) != nil {
		t.Fatalf("publication is not derived from held journal and verified plan: %v", err)
	}
	bootstrap.RegistryBinding[0] ^= 1
	bootstrap.AuthorityKeys["authority"][0] ^= 1
	copyBootstrap, err := publication.Bootstrap()
	if err != nil || bytes.Equal(copyBootstrap.RegistryBinding, bootstrap.RegistryBinding) {
		t.Fatal("caller mutated publication snapshot")
	}
	reopened, err := InspectRuntimeBootstrapPublication(t.Context(), config.Directory)
	if err != nil || reopened.Record() != publication.Record() {
		t.Fatalf("history mismatch: %v", err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), binary, "-test.run=^TestRuntimeBootstrapPublicationAccessHelper$")
	command.Env = []string{"VELA_PUBLICATION_ACCESS=" + config.Directory}
	command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 10001, Gid: 10001}}
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("non-root read/write boundary: %v %s", err, output)
	}
	if retry, err := PublishRuntimeBootstrap(t.Context(), config); err == nil || retry != nil {
		t.Fatal("publication overwritten")
	}
	if err := config.Journal.Close(); err != nil {
		t.Fatal(err)
	}
	if history, err := InspectRuntimeBootstrapPublication(t.Context(), config.Directory); err != nil || history.Record() != publication.Record() {
		t.Fatalf("closed owner destroyed history: %v", err)
	}
	t.Log("verified plan and held journal -> immutable snapshot; actual UID/GID 10001 can read only bootstrap; reopening returns history after owner closure")
}

func TestRuntimeBootstrapPublicationPreflight(t *testing.T) {
	for _, mode := range []string{"nil-plan", "nil-journal", "closed-journal", "wrong-plan", "wrong-registry", "socket-alias", "writable-directory", "workload-directory", "nonempty", "sticky-parent", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			config := publicationFixture(t, false)
			ctx := t.Context()
			switch mode {
			case "nil-plan":
				config.Plan = nil
			case "nil-journal":
				config.Journal = nil
			case "closed-journal":
				if err := config.Journal.Close(); err != nil {
					t.Fatal(err)
				}
			case "wrong-plan":
				config.Plan = publicationFixture(t, false).Plan
			case "wrong-registry":
				config.RegistryKeys["registry-launch-test"][0] ^= 1
			case "socket-alias":
				config.StartupSocket = config.JournalSocket
			case "writable-directory":
				if err := os.Chmod(config.Directory, 0o770); err != nil {
					t.Fatal(err)
				}
			case "workload-directory":
				if err := os.Chown(config.Directory, 10001, 10001); err != nil {
					t.Fatal(err)
				}
			case "nonempty":
				if err := os.WriteFile(filepath.Join(config.Directory, "unexpected"), []byte("x"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "sticky-parent":
				config.Directory = t.TempDir()
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if result, err := PublishRuntimeBootstrap(ctx, config); err == nil || result != nil {
				t.Fatal("invalid publication accepted")
			}
			if _, err := os.Lstat(filepath.Join(config.Directory, runtimeBootstrapRecordName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("preflight wrote intent: %v", err)
			}
		})
	}
}

func TestRuntimeBootstrapPublicationBoundaries(t *testing.T) {
	for _, phase := range []string{"intent-created", "bootstrap-synced", "intent-synced", "renamed", "exposed", "verified"} {
		t.Run(phase, func(t *testing.T) {
			config := publicationFixture(t, false)
			fault := errors.New("lost publication response")
			result, err := publishRuntimeBootstrap(t.Context(), config, func(point string) error {
				if point == phase {
					if observed, err := InspectRuntimeBootstrapPublication(t.Context(), config.Directory); err == nil || observed != nil {
						t.Fatal("active publisher inspection succeeded")
					}
					return fault
				}
				return nil
			})
			if !errors.Is(err, fault) || result != nil {
				t.Fatalf("boundary did not fail: %v", err)
			}
			if retry, err := PublishRuntimeBootstrap(t.Context(), config); err == nil || retry != nil {
				t.Fatal("partial publication was retried")
			}
			history, err := InspectRuntimeBootstrapPublication(t.Context(), config.Directory)
			complete := phase == "exposed" || phase == "verified"
			if complete != (err == nil && history != nil) {
				t.Fatalf("wrong recovery at %s: %v", phase, err)
			}
		})
	}
}

func TestRuntimeBootstrapPublicationConcurrent(t *testing.T) {
	config := publicationFixture(t, false)
	start := make(chan struct{})
	results := make(chan error, 8)
	for range 8 {
		go func() { <-start; _, err := PublishRuntimeBootstrap(t.Context(), config); results <- err }()
	}
	close(start)
	successes := 0
	for range 8 {
		if <-results == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("got %d successful publishers", successes)
	}
	if _, err := InspectRuntimeBootstrapPublication(t.Context(), config.Directory); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeBootstrapPublicationProcessCrash(t *testing.T) {
	const modeKey = "VELA_PUBLICATION_CRASH"
	if phase := os.Getenv(modeKey); phase != "" {
		config := publicationFixture(t, false)
		config.Directory = os.Getenv("VELA_PUBLICATION_CRASH_DIRECTORY")
		control := os.NewFile(3, "publication-crash-boundary")
		defer func() { _ = control.Close() }()
		_, err := publishRuntimeBootstrap(t.Context(), config, func(point string) error {
			if point == phase {
				if _, err := control.WriteString(phase + "\n"); err != nil {
					return err
				}
				select {}
			}
			return nil
		})
		t.Fatalf("Node did not stop at requested publication boundary: %v", err)
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root Node")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"intent-created", "bootstrap-synced", "intent-synced", "renamed", "exposed", "verified"} {
		t.Run(phase, func(t *testing.T) {
			root, err := os.MkdirTemp("/run", "vela-publication-crash-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(root) })
			directory := filepath.Join(root, "publication")
			if err := os.Mkdir(directory, 0o700); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, binary, "-test.run=^TestRuntimeBootstrapPublicationProcessCrash$", "-test.timeout=12s")
			command.Env = []string{modeKey + "=" + phase, "VELA_PUBLICATION_CRASH_DIRECTORY=" + directory}
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
			waited := false
			t.Cleanup(func() {
				if !waited {
					_ = command.Process.Kill()
					_ = command.Wait()
				}
				if t.Failed() {
					t.Log(output.String())
				}
			})
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			if err := reader.SetReadDeadline(time.Now().Add(8 * time.Second)); err != nil {
				t.Fatal(err)
			}
			point := make([]byte, len(phase)+1)
			if _, err := io.ReadFull(reader, point); err != nil || string(point) != phase+"\n" {
				t.Fatalf("wrong boundary: %q %v", point, err)
			}
			if err := command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err = command.Wait()
			waited = true
			var exit *exec.ExitError
			if !errors.As(err, &exit) || ctx.Err() != nil {
				t.Fatalf("Node not killed at boundary: %v", err)
			}
			status, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("unexpected exit: %v", status)
			}
			history, err := InspectRuntimeBootstrapPublication(t.Context(), directory)
			complete := phase == "exposed" || phase == "verified"
			if complete != (err == nil && history != nil) {
				t.Fatalf("wrong SIGKILL recovery history at %s: %v", phase, err)
			}
			// Even a new valid owner/plan cannot consume the old directory again.
			retry := publicationFixture(t, false)
			retry.Directory = directory
			if result, err := PublishRuntimeBootstrap(t.Context(), retry); err == nil || result != nil {
				t.Fatal("SIGKILL history overwritten by new startup")
			}
			t.Logf("actual Node SIGKILL at %s: history_complete=%t, no publication retry or grant restoration", phase, complete)
		})
	}
}

func TestRuntimeBootstrapPublicationChanged(t *testing.T) {
	for _, mode := range []string{"content", "file-copy", "file-link", "file-symlink", "file-fifo", "file-writable", "file-owner", "record-copy", "record-missing", "record-writable", "record-content", "directory-copy", "directory-writable", "extra"} {
		t.Run(mode, func(t *testing.T) {
			config := publicationFixture(t, false)
			if _, err := PublishRuntimeBootstrap(t.Context(), config); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(config.Directory, runtimeBootstrapFilename)
			if strings.HasPrefix(mode, "record-") {
				path = filepath.Join(config.Directory, runtimeBootstrapRecordName)
			}
			must := func(err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "content", "record-content":
				must(os.WriteFile(path, []byte("{}"), 0o600))
			case "file-copy", "record-copy":
				wire, err := os.ReadFile(path)
				must(err)
				must(os.Rename(path, path+".old"))
				must(os.WriteFile(path, wire, 0o600))
				if mode == "file-copy" {
					must(os.Chown(path, 0, 10001))
					must(os.Chmod(path, 0o440))
				}
				must(os.Remove(path + ".old"))
			case "file-link":
				must(os.Link(path, filepath.Join(filepath.Dir(config.Directory), "linked")))
			case "file-symlink":
				must(os.Rename(path, path+".old"))
				must(os.Symlink(path+".old", path))
			case "file-fifo":
				must(os.Remove(path))
				must(unix.Mkfifo(path, 0o440))
			case "file-writable":
				must(os.Chmod(path, 0o640))
			case "file-owner":
				must(os.Chown(path, 10001, 10001))
			case "record-missing":
				must(os.Remove(path))
			case "record-writable":
				must(os.Chmod(path, 0o660))
			case "directory-copy":
				must(os.Rename(config.Directory, config.Directory+".old"))
				must(os.Mkdir(config.Directory, 0o750))
				must(os.Chown(config.Directory, 0, 10001))
				for _, name := range []string{runtimeBootstrapFilename, runtimeBootstrapRecordName} {
					must(os.Rename(filepath.Join(config.Directory+".old", name), filepath.Join(config.Directory, name)))
				}
			case "directory-writable":
				must(os.Chmod(config.Directory, 0o770))
			case "extra":
				must(os.WriteFile(filepath.Join(config.Directory, "extra"), []byte("x"), 0o600))
			}
			if result, err := InspectRuntimeBootstrapPublication(t.Context(), config.Directory); err == nil || result != nil {
				t.Fatal("changed publication accepted")
			}
		})
	}
}

func TestRuntimeBootstrapPublicationCustodyLoss(t *testing.T) {
	for _, phase := range []string{"intent-synced", "renamed", "exposed"} {
		t.Run(phase, func(t *testing.T) {
			config := publicationFixture(t, false)
			result, err := publishRuntimeBootstrap(t.Context(), config, func(point string) error {
				if point == phase {
					return config.Journal.Close()
				}
				return nil
			})
			if err == nil || result != nil {
				t.Fatal("publication returned success after custody loss")
			}
			_, historyErr := InspectRuntimeBootstrapPublication(t.Context(), config.Directory)
			if (phase == "exposed") != (historyErr == nil) {
				t.Fatalf("custody loss misclassified history: %v", historyErr)
			}
		})
	}
	for _, mode := range []string{"pending-corrupt", "directory-replaced", "record-replaced"} {
		t.Run(mode, func(t *testing.T) {
			config := publicationFixture(t, false)
			result, err := publishRuntimeBootstrap(t.Context(), config, func(point string) error {
				if point != "intent-synced" {
					return nil
				}
				switch mode {
				case "pending-corrupt":
					return os.WriteFile(filepath.Join(config.Directory, runtimeBootstrapPending), []byte("{}"), 0o440)
				case "directory-replaced":
					if err := os.Rename(config.Directory, config.Directory+".old"); err != nil {
						return err
					}
					return os.Mkdir(config.Directory, 0o700)
				default:
					path := filepath.Join(config.Directory, runtimeBootstrapRecordName)
					wire, err := os.ReadFile(path)
					if err != nil {
						return err
					}
					if err := os.Rename(path, path+".old"); err != nil {
						return err
					}
					if err := os.WriteFile(path, wire, 0o600); err != nil {
						return err
					}
					return os.Remove(path + ".old")
				}
			})
			if err == nil || result != nil {
				t.Fatal("changed pending publication returned success")
			}
			info, err := os.Stat(config.Directory)
			if err != nil || info.Mode().Perm() != 0o700 {
				t.Fatalf("invalid publication exposed: %v", err)
			}
		})
	}
}

func TestRuntimeBootstrapPublicationActualCLI(t *testing.T) {
	if _, err := os.Stat("/vela-model-runtime"); err != nil {
		t.Skip("requires actual native CLI sandbox")
	}
	config := publicationFixture(t, true)
	var manifest modelruntime.LaunchManifest
	if err := json.Unmarshal(config.Plan.manifest, &manifest); err != nil {
		t.Fatal(err)
	}
	scratch := manifest.Runtimes[0].ScratchRoot
	for _, path := range []string{scratch, manifest.Runtimes[0].InputRoot, manifest.Runtimes[0].OutputRoot} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(os.Chown(path, 10001, 10001), os.Chmod(path, 0o700)); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = os.RemoveAll(scratch) })
	config.RuntimeSocket = filepath.Join(scratch, "runtime.sock")
	publication, err := PublishRuntimeBootstrap(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := publication.Bootstrap()
	if err != nil {
		t.Fatal(err)
	}
	listen := func(path string) *net.UnixListener {
		t.Helper()
		listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		if err := errors.Join(os.Chown(path, 0, 10001), os.Chmod(path, 0o660)); err != nil {
			t.Fatal(err)
		}
		return listener
	}
	listener := listen(config.JournalSocket)
	credentials := RuntimeCallerCredentials{UID: 10001, GID: 10001}
	state := filepath.Join(filepath.Dir(config.Directory), "state")
	runtime := journalEndpointStartCredentials(t, listener, state, credentials)
	worker := journalEndpointStartCredentials(t, listener, state, credentials)
	endpoint, err := NewJournalEndpoint(t.Context(), config.Journal, runtime.owner, worker.owner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	server, err := NewJournalServer(endpoint, JournalServerConfig{Credentials: []RuntimeCallerCredentials{credentials}, MaxConcurrent: 2, ExchangeTimeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(t.Context(), listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	startupListener := listen(config.StartupSocket)
	if err := runtime.input.Encode(journalEndpointControl{ExecRemoteCLI: filepath.Join(config.Directory, runtimeBootstrapFilename)}); err != nil {
		t.Fatal(err)
	}
	connection := journalEndpointAccept(t, startupListener)
	defer func() { _ = connection.Close() }()
	caller, err := ReceiveRuntimeCaller(t.Context(), connection, credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = caller.Close() }()
	request, err := modelruntime.ParseBackendStartupRequest(caller.Payload())
	if err != nil {
		t.Fatal(err)
	}
	observed, err := caller.Inspect(t.Context())
	if err != nil || observed.HostPID != int32(runtime.process.Pid) || request.RegistryBindingDigest != publication.Record().BindingDigest {
		t.Fatalf("CLI caller not bound to publication: %v", err)
	}
	if _, err := config.Journal.InspectStartup(t.Context(), manifest, request); err != nil {
		t.Fatal(err)
	}
	events := filepath.Join(scratch, "events.log")
	if _, err := os.Stat(events); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("backend initialized before Node fixture decision")
	}
	// Explicit test Permit only: publishing configuration is never a grant.
	decision, err := json.Marshal(modelruntime.BackendStartupDecision{SchemaVersion: 1, RequestDigest: sha256.Sum256(caller.Payload()), Permit: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := caller.Reply(t.Context(), decision); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		wire, _ := os.ReadFile(events)
		_, socketErr := os.Stat(config.RuntimeSocket)
		if string(wire) == "initialize\n" && socketErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("published config CLI did not initialize")
		}
		time.Sleep(10 * time.Millisecond)
	}
	reply := journalServerRequest(t, worker, journalEndpointControl{WorkerSocket: config.RuntimeSocket, WorkerAction: "discover-only", Manifest: &manifest}).Discovery
	if len(reply.GetIdentities()) != 1 || reply.Identities[0].GetModelRuntimeEpoch() != 2 {
		t.Fatalf("actual Worker discovery: %v", reply)
	}
	if err := runtime.process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runtime.done:
	case <-time.After(10 * time.Second):
		t.Fatal("actual CLI did not exit")
	}
	if !runtime.command.ProcessState.Success() {
		t.Fatal(runtime.command.ProcessState)
	}
	wire, err := os.ReadFile(events)
	if err != nil || string(wire) != "initialize\nshutdown\n" {
		t.Fatalf("actual backend lifecycle: %q %v", wire, err)
	}
	status, err := config.Journal.Status(t.Context())
	if err != nil || status.BackendLifecycle != bootstrap.Startup || status.Highest != 0 {
		t.Fatalf("CLI changed publication incarnation: %v", err)
	}
	entries, err := os.ReadDir(scratch)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() != "inputs" && entry.Name() != "outputs" && entry.Name() != "events.log" {
			t.Fatalf("unexpected local Runtime state: %s", entry.Name())
		}
	}
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertJournalServerJoined(t, server, done)
	t.Log("Node verified plan + held journal publication -> actual UID/GID 10001 CLI -> explicit fixture Permit -> real CPU backend initialize/shutdown, actual Worker epoch=2, no local epoch/journal")
}
