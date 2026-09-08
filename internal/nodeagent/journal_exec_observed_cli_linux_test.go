package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"golang.org/x/sys/unix"
)

// This is a compatibility/failure experiment for a creation-time C observer.
// Its launch relationship, Pod/CRI and Permit are fixtures, not Node authority.
func TestJournalServerExecObservedRemoteCLI(t *testing.T) {
	for _, binary := range []string{"/exec-observer", "/vela-model-runtime", "/runtime-command.test"} {
		if _, err := os.Stat(binary); err != nil {
			t.Skip("requires native exec observer and actual CLI sandbox")
		}
	}
	for _, scenario := range []string{"permit", "deny", "observer-lost-before-permit", "observer-lost-after-permit"} {
		t.Run(scenario, func(t *testing.T) { verifyExecObservedRemoteCLI(t, scenario) })
	}
}

func verifyExecObservedRemoteCLI(t *testing.T, scenario string) {
	t.Helper()
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
	journalListener, startupListener := listen(config.JournalSocket), listen(config.StartupSocket)
	credentials := RuntimeCallerCredentials{UID: 10001, GID: 10001}
	worker := journalEndpointStartCredentials(t, journalListener, filepath.Join(filepath.Dir(config.Directory), "state"), credentials)
	bootstrapPath := filepath.Join(config.Directory, runtimeBootstrapFilename)
	command := exec.CommandContext(t.Context(), "/exec-observer", "--new-pid", "10001", "10001", "/vela-model-runtime", "serve-remote", "--bootstrap-file", bootstrapPath)
	command.Env = []string{"HOME=/", "PATH=" + runtimeRemoteCLIPath}
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	finished := make(chan struct{})
	var waitErr error
	go func() { waitErr = command.Wait(); close(finished) }()
	t.Cleanup(func() {
		_ = command.Process.Kill()
		<-finished
		if t.Failed() {
			t.Logf("exec observer output: %s", output.String())
		}
	})
	first := journalEndpointAccept(t, journalListener)
	defer func() { _ = first.Close() }()
	firstCaller, err := ReceiveRuntimeCallerWithRequestLimit(t.Context(), first, credentials, modelruntime.MaximumJournalCommandBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = firstCaller.Close() }()
	// Also clean up the negative-control image where observer death deliberately
	// does not terminate its target. This runs after the assertions, before Close.
	defer func() { _ = unix.PidfdSendSignal(int(firstCaller.pidfd.Fd()), syscall.SIGKILL, nil, 0) }()
	observed, err := firstCaller.Inspect(t.Context())
	if err != nil || observed.NamespacePID != 1 || observed.NamespaceDepth < 2 || observed.HostPID == int32(command.Process.Pid) {
		t.Fatalf("actual CLI must be the original non-root namespace init: %+v %v", observed, err)
	}
	cri, _, observer := runtimeCallerObserverFixture(t, observed)
	defer func() { _ = observer.Close() }()
	runtimeOwner, err := observer.RetainNamespaceOwner(t.Context(), cri.target, firstCaller)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runtimeOwner.Close() }()
	endpoint, err := NewReadOnlyJournalEndpoint(t.Context(), config.Journal, runtimeOwner, worker.owner)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = endpoint.Close() }()
	reply, err := endpoint.Handle(t.Context(), firstCaller)
	if err != nil {
		t.Fatal(err)
	}
	if err := firstCaller.Reply(t.Context(), reply); err != nil {
		t.Fatal(err)
	}
	server, err := NewJournalServer(endpoint, JournalServerConfig{Credentials: []RuntimeCallerCredentials{credentials}, MaxConcurrent: 2, ExchangeTimeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(t.Context(), journalListener) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	connection := journalEndpointAccept(t, startupListener)
	defer func() { _ = connection.Close() }()
	caller, err := ReceiveRuntimeCaller(t.Context(), connection, credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = caller.Close() }()
	request, err := modelruntime.ParseBackendStartupRequest(caller.Payload())
	if err != nil || request.SchemaVersion != 2 || request.BootstrapDigest != publication.Record().BootstrapDigest || request.BootstrapPath != bootstrapPath {
		t.Fatalf("traced CLI changed bootstrap consumption: %+v %v", request, err)
	}
	if _, err := config.Journal.InspectStartup(t.Context(), manifest, request); err != nil {
		t.Fatal(err)
	}
	events := filepath.Join(scratch, "events.log")
	if _, err := os.Stat(events); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("backend initialized before explicit fixture decision")
	}
	permit := scenario != "deny"
	decision, err := json.Marshal(modelruntime.BackendStartupDecision{SchemaVersion: 1, RequestDigest: sha256.Sum256(caller.Payload()), Permit: permit})
	if err != nil {
		t.Fatal(err)
	}
	awaitOwnerExit := func() {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			if _, err := runtimeOwner.ObserveExit(t.Context()); err == nil {
				return
			} else if !errors.Is(err, ErrRuntimeNamespaceOwnerLive) || time.Now().After(deadline) {
				t.Fatalf("original namespace owner did not exit: %v", err)
			}
			time.Sleep(time.Millisecond)
		}
	}
	if scenario == "observer-lost-before-permit" {
		if err := command.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		awaitOwnerExit()
		if err := caller.Reply(t.Context(), decision); err == nil {
			t.Fatal("dead original caller accepted startup decision")
		}
	} else {
		if err := caller.Reply(t.Context(), decision); err != nil {
			t.Fatal(err)
		}
	}
	if scenario == "permit" || scenario == "observer-lost-after-permit" {
		deadline := time.Now().Add(10 * time.Second)
		for {
			data, _ := os.ReadFile(events)
			_, socketErr := os.Stat(config.RuntimeSocket)
			if string(data) == "initialize\n" && socketErr == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("observed actual CLI did not initialize real process backend")
			}
			time.Sleep(10 * time.Millisecond)
		}
		backendFD := execObservedBackendPIDFD(t, int(observed.HostPID))
		defer func() { _ = unix.Close(backendFD) }()
		if scenario == "permit" {
			discovery := journalServerRequest(t, worker, journalEndpointControl{WorkerSocket: config.RuntimeSocket, WorkerAction: "discover-only", Manifest: &manifest}).Discovery
			if len(discovery.GetIdentities()) != 1 || discovery.Identities[0].GetModelRuntimeEpoch() != 2 {
				t.Fatalf("observed CLI discovery: %v", discovery)
			}
			if err := unix.PidfdSendSignal(int(caller.pidfd.Fd()), syscall.SIGTERM, nil, 0); err != nil {
				t.Fatal(err)
			}
		} else if err := command.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		awaitOwnerExit()
		watch := []unix.PollFd{{Fd: int32(backendFD), Events: unix.POLLIN}}
		if count, err := unix.Poll(watch, 5000); err != nil || count != 1 || watch[0].Revents&unix.POLLIN == 0 {
			t.Fatalf("real backend survived observer/runtime exit: %d %v", count, err)
		}
	}
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("observer did not finish")
	}
	if (scenario == "permit") != (waitErr == nil) {
		t.Fatalf("observer exit scenario=%s: %v", scenario, waitErr)
	}
	data, readErr := os.ReadFile(events)
	switch scenario {
	case "permit":
		if readErr != nil || string(data) != "initialize\nshutdown\n" {
			t.Fatalf("observed actual backend lifecycle: %q %v", data, readErr)
		}
	case "observer-lost-after-permit":
		if readErr != nil || string(data) != "initialize\n" {
			t.Fatalf("crash must not fabricate graceful shutdown: %q %v", data, readErr)
		}
	default:
		if !errors.Is(readErr, os.ErrNotExist) {
			t.Fatal("rejected/lost observer startup initialized backend")
		}
	}
	status, err := config.Journal.Status(t.Context())
	if err != nil || status.Highest != 0 || status.BackendLifecycle.IncarnationID != request.IncarnationID {
		t.Fatalf("observer experiment changed Node journal authority: %+v %v", status, err)
	}
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertJournalServerJoined(t, server, done)
	t.Logf("creation-time observer retained actual CLI namespace init; scenario=%s; original process/backend exits are kernel observations, Permit and CRI remain fixtures", scenario)
}

func execObservedBackendPIDFD(t *testing.T, parent int) int {
	t.Helper()
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		root := filepath.Join("/proc", entry.Name())
		status, err := os.ReadFile(filepath.Join(root, "status"))
		if err != nil || !strings.Contains(string(status), "\nPPid:\t"+strconv.Itoa(parent)+"\n") {
			continue
		}
		path, err := os.Readlink(filepath.Join(root, "exe"))
		if err != nil || path != "/runtime-command.test" {
			continue
		}
		fd, err := unix.PidfdOpen(pid, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := checkRuntimePIDFD(fd, int32(pid)); err != nil {
			_ = unix.Close(fd)
			t.Fatal(err)
		}
		return fd
	}
	t.Fatal("no actual process backend found under original CLI")
	return -1
}
