//go:build integration && linux

package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	containersapi "github.com/containerd/containerd/api/services/containers/v1"
	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	tasktypes "github.com/containerd/containerd/api/types/task"
	"github.com/google/uuid"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const containerdCallerMode = "VELA_CONTAINERD_CALLER_MODE"

type containerdProcessFixture struct {
	ctx        context.Context
	containers containersapi.ContainersClient
	tasks      tasksapi.TasksClient
	root       string
	dataRoot   string
	binary     string
	socket     string
	connection *grpc.ClientConn
	daemon     *exec.Cmd
	daemonLog  *os.File
}

type containerdCaller struct {
	connection *net.UnixConn
	pid        int32
	proof      *RuntimeCaller
	startTicks uint64
	namespace  string
	nestedPIDs []string
}

// Run only inside the disposable, network-disabled Linux container described
// in the evidence report. This starts a private daemon, never a host socket.
func TestRuntimeContainerdProcessEvidence(t *testing.T) {
	if os.Getenv("VELA_TEST_CONTAINERD_SANDBOX") != "1" {
		t.Skip("requires the explicitly enabled disposable containerd CPU sandbox")
	}
	if os.Geteuid() != 0 {
		t.Fatal("the disposable daemon fixture requires root")
	}
	fixture := startProcessContainerd(t)
	for _, mode := range []string{"owner", "wrapper"} {
		t.Run(mode, func(t *testing.T) {
			container, listener := fixture.create(t, mode, "")
			process, caller := fixture.start(t, container.ID, listener)
			namespaceInit := caller.nestedPIDs[len(caller.nestedPIDs)-1] == "1"
			if (caller.pid == int32(process.Pid)) != (mode == "owner") || namespaceInit != (mode == "owner") {
				t.Fatalf("kernel caller/task ownership mismatch: mode=%s task=%d peer=%d NSpid=%v", mode, process.Pid, caller.pid, caller.nestedPIDs)
			}
			t.Logf("mode=%s task_pid=%d peer_pid=%d peer_start_ticks=%d NSpid=%v namespace=%s",
				mode, process.Pid, caller.pid, caller.startTicks, caller.nestedPIDs, caller.namespace)
			if mode == "wrapper" {
				fixture.stop(t, container.ID, caller)
				return
			}

			// Metadata is not the immutable OCI bundle used to create this task.
			changed := proto.Clone(container).(*containersapi.Container)
			var configuration specs.Spec
			if err := json.Unmarshal(changed.Spec.Value, &configuration); err != nil {
				t.Fatal(err)
			}
			configuration.Process.Args = []string{"/metadata-only-command"}
			configuration.Linux.Namespaces = []specs.LinuxNamespace{{Type: specs.MountNamespace}}
			changed.Spec = encodeContainerdSpec(t, configuration)
			updated, err := fixture.containers.Update(fixture.ctx, &containersapi.UpdateContainerRequest{
				Container: changed, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec"}},
			})
			if err != nil || !proto.Equal(updated.GetContainer().GetSpec(), changed.Spec) {
				t.Fatalf("update live container metadata: %v", err)
			}
			for range 2 {
				current, err := fixture.containers.Get(fixture.ctx, &containersapi.GetContainerRequest{ID: container.ID})
				if err != nil || !proto.Equal(current.GetContainer().GetSpec(), changed.Spec) {
					t.Fatalf("metadata change was not readable: %v", err)
				}
				live := fixture.task(t, container.ID)
				if live.Pid != process.Pid || live.Status != tasktypes.Status_RUNNING {
					t.Fatalf("metadata update replaced the actual task: %+v", live)
				}
				assertContainerdCallerAlive(t, caller)
			}
			t.Log("two matching Containers.Get replies returned changed PID/argv metadata while the original namespace-init caller remained live")

			// Joining an existing namespace makes a task init different from the
			// namespace init, even when the socket peer equals Tasks.Get.Pid.
			shared, sharedListener := fixture.create(t, "owner", fmt.Sprintf("/proc/%d/ns/pid", process.Pid))
			sharedProcess, sharedCaller := fixture.start(t, shared.ID, sharedListener)
			if sharedCaller.pid != int32(sharedProcess.Pid) || sharedCaller.namespace != caller.namespace ||
				sharedCaller.nestedPIDs[len(sharedCaller.nestedPIDs)-1] == "1" {
				t.Fatalf("shared-namespace counterexample failed: task=%d peer=%d NSpid=%v namespace=%s", sharedProcess.Pid,
					sharedCaller.pid, sharedCaller.nestedPIDs, sharedCaller.namespace)
			}
			t.Logf("shared PID namespace: task_pid=peer_pid=%d but NSpid=%v; task ownership alone does not prove namespace ownership",
				sharedProcess.Pid, sharedCaller.nestedPIDs)
			fixture.stop(t, shared.ID, sharedCaller)

			if _, err := fixture.containers.Update(fixture.ctx, &containersapi.UpdateContainerRequest{
				Container: container, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"spec"}},
			}); err != nil {
				t.Fatal(err)
			}
			fixture.stop(t, container.ID, caller)
			if _, err := fixture.tasks.Delete(fixture.ctx, &tasksapi.DeleteTaskRequest{ContainerID: container.ID}); err != nil {
				t.Fatal(err)
			}
			replacement, nextCaller := fixture.start(t, container.ID, listener)
			current, err := fixture.containers.Get(fixture.ctx, &containersapi.GetContainerRequest{ID: container.ID})
			if err != nil || !proto.Equal(current.GetContainer().GetCreatedAt(), container.CreatedAt) ||
				!proto.Equal(current.GetContainer().GetSpec(), container.Spec) ||
				(replacement.Pid == process.Pid && nextCaller.startTicks == caller.startTicks) {
				t.Fatalf("same-container task recreation was not demonstrated: old_pid=%d new_pid=%d err=%v", process.Pid, replacement.Pid, err)
			}
			assertContainerdCallerExited(t, caller)
			assertContainerdCallerAlive(t, nextCaller)
			t.Logf("same container id=%s and CreatedAt retained after task recreation: old_pid=%d new_pid=%d old_ticks=%d new_ticks=%d",
				container.ID, process.Pid, replacement.Pid, caller.startTicks, nextCaller.startTicks)
			fixture.stop(t, container.ID, nextCaller)
		})
	}
}

func startProcessContainerd(t *testing.T) *containerdProcessFixture {
	return startConfiguredProcessContainerd(t, "version = 3\ndisabled_plugins = [\"io.containerd.cri.v1.runtime\", \"io.containerd.cri.v1.images\"]\n")
}

func startConfiguredProcessContainerd(t *testing.T, configuration string) *containerdProcessFixture {
	return startPersistentProcessContainerd(t, configuration, "")
}

func startPersistentProcessContainerd(t *testing.T, configuration, dataRoot string) *containerdProcessFixture {
	t.Helper()
	version, err := exec.CommandContext(t.Context(), "containerd", "--version").CombinedOutput()
	if err != nil || strings.TrimSpace(string(version)) != "containerd github.com/containerd/containerd/v2 v2.3.1 64b425cf570b3b8dd1d4cc46da7c1fce65c6651a" {
		t.Fatalf("experiment requires the pinned containerd build: %s %v", version, err)
	}
	t.Log(strings.TrimSpace(string(version)))
	version, err = exec.CommandContext(t.Context(), "runc", "--version").CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(strings.TrimSpace(string(version)))
	root, err := os.MkdirTemp("/run", "vela-ctrd-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	})
	config := filepath.Join(root, "containerd.toml")
	if err := os.WriteFile(config, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	log, err := os.Create(filepath.Join(root, "daemon.log"))
	if err != nil {
		t.Fatal(err)
	}
	// Testing cancels t.Context before Cleanup callbacks. Keep the daemon
	// available for resource cleanup, then stop it in the registered callback.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 90*time.Second)
	t.Cleanup(cancel)
	socket := filepath.Join(root, "containerd.sock")
	if dataRoot == "" {
		dataRoot = filepath.Join(root, "data")
	}
	fixture := &containerdProcessFixture{ctx: ctx, root: root, dataRoot: dataRoot, socket: socket, daemonLog: log}
	t.Cleanup(func() {
		fixture.stopDaemon(t, unix.SIGTERM, false)
		_ = log.Close()
		if t.Failed() {
			data, _ := os.ReadFile(log.Name())
			t.Logf("private daemon log: %s", data)
		}
	})
	connection, err := grpc.NewClient("unix://"+socket, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	ctx = metadata.AppendToOutgoingContext(ctx, "containerd-namespace", "vela-cpu-"+uuid.NewString())
	fixture.ctx, fixture.connection = ctx, connection
	fixture.containers, fixture.tasks = containersapi.NewContainersClient(connection), tasksapi.NewTasksClient(connection)
	fixture.binary, err = os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	fixture.startDaemon(t)
	return fixture
}

func (fixture *containerdProcessFixture) startDaemon(t *testing.T) {
	t.Helper()
	if fixture.daemon != nil {
		t.Fatal("private containerd is already running")
	}
	daemon := exec.CommandContext(fixture.ctx, "containerd", "--config", filepath.Join(fixture.root, "containerd.toml"),
		"--root", fixture.dataRoot, "--state", filepath.Join(fixture.root, "state"), "--address", fixture.socket)
	daemon.Stdout, daemon.Stderr = fixture.daemonLog, fixture.daemonLog
	if err := daemon.Start(); err != nil {
		t.Fatal(err)
	}
	fixture.daemon = daemon
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := fixture.containers.List(fixture.ctx, &containersapi.ListContainersRequest{}); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("private containerd did not become ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (fixture *containerdProcessFixture) stopDaemon(t *testing.T, signal unix.Signal, verify bool) {
	t.Helper()
	if fixture.daemon == nil {
		return
	}
	daemon := fixture.daemon
	signalErr := daemon.Process.Signal(signal)
	done := make(chan error, 1)
	go func() { done <- daemon.Wait() }()
	var exitErr error
	timedOut := false
	select {
	case exitErr = <-done:
	case <-time.After(5 * time.Second):
		timedOut = true
		_ = daemon.Process.Kill()
		<-done
	}
	fixture.daemon = nil
	if verify {
		if signalErr != nil || timedOut {
			t.Fatalf("private containerd stop failed: signal=%v timeout=%v", signalErr, timedOut)
		}
		status, ok := daemon.ProcessState.Sys().(syscall.WaitStatus)
		if signal == unix.SIGKILL {
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("private containerd did not die by SIGKILL: %v", exitErr)
			}
		} else if exitErr != nil {
			t.Fatalf("private containerd did not stop gracefully: %v", exitErr)
		}
	}
}

func (fixture *containerdProcessFixture) create(t *testing.T, mode, pidNamespace string) (*containersapi.Container, *net.UnixListener) {
	t.Helper()
	id := strings.ReplaceAll(uuid.NewString()+uuid.NewString(), "-", "")
	root := filepath.Join(fixture.root, id[:12])
	for _, path := range []string{root, filepath.Join(root, "rootfs"), filepath.Join(root, "socket")} {
		if err := os.Mkdir(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: filepath.Join(root, "socket", "caller.sock"), Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	if err := os.Chmod(listener.Addr().String(), 0o666); err != nil {
		t.Fatal(err)
	}
	configuration := specs.Spec{
		Version: specs.Version,
		Root:    &specs.Root{Path: filepath.Join(root, "rootfs"), Readonly: true},
		Process: &specs.Process{Args: []string{"/probe", "-test.run=^TestContainerdRuntimeCallerHelper$", "-test.timeout=60s"},
			Env: []string{containerdCallerMode + "=" + mode}, Cwd: "/", User: specs.User{UID: 65532, GID: 65532},
			NoNewPrivileges: true, Capabilities: &specs.LinuxCapabilities{}},
		Mounts: []specs.Mount{
			{Destination: "/proc", Type: "proc", Source: "proc", Options: []string{"nosuid", "noexec", "nodev"}},
			{Destination: "/probe", Type: "bind", Source: fixture.binary, Options: []string{"bind", "ro", "nosuid", "nodev"}},
			{Destination: "/proof", Type: "bind", Source: filepath.Join(root, "socket"), Options: []string{"bind", "ro", "nosuid", "nodev", "noexec"}},
		},
		Linux: &specs.Linux{CgroupsPath: "/vela-cpu-" + id, Namespaces: []specs.LinuxNamespace{
			{Type: specs.PIDNamespace, Path: pidNamespace}, {Type: specs.MountNamespace},
			{Type: specs.NetworkNamespace}, {Type: specs.IPCNamespace}, {Type: specs.UTSNamespace},
		}},
	}
	created, err := fixture.containers.Create(fixture.ctx, &containersapi.CreateContainerRequest{Container: &containersapi.Container{
		ID: id, Runtime: &containersapi.Container_Runtime{Name: "io.containerd.runc.v2"}, Spec: encodeContainerdSpec(t, configuration),
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = fixture.tasks.Kill(fixture.ctx, &tasksapi.KillRequest{ContainerID: id, Signal: uint32(unix.SIGKILL), All: true})
		_, _ = fixture.tasks.Delete(fixture.ctx, &tasksapi.DeleteTaskRequest{ContainerID: id})
		_, _ = fixture.containers.Delete(fixture.ctx, &containersapi.DeleteContainerRequest{ID: id})
	})
	return created.Container, listener
}

func encodeContainerdSpec(t *testing.T, configuration specs.Spec) *anypb.Any {
	t.Helper()
	data, err := json.Marshal(configuration)
	if err != nil {
		t.Fatal(err)
	}
	return &anypb.Any{TypeUrl: "types.containerd.io/opencontainers/runtime-spec/1/Spec", Value: data}
}

func (fixture *containerdProcessFixture) task(t *testing.T, id string) *tasktypes.Process {
	t.Helper()
	response, err := fixture.tasks.Get(fixture.ctx, &tasksapi.GetRequest{ContainerID: id})
	if err != nil || response.GetProcess().GetID() != id {
		t.Fatalf("read actual task: %v %v", response, err)
	}
	return response.Process
}

func (fixture *containerdProcessFixture) start(t *testing.T, id string, listener *net.UnixListener) (*tasktypes.Process, *containerdCaller) {
	t.Helper()
	if _, err := fixture.tasks.Create(fixture.ctx, &tasksapi.CreateTaskRequest{ContainerID: id}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.tasks.Start(fixture.ctx, &tasksapi.StartRequest{ContainerID: id}); err != nil {
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
	if err := connection.SetDeadline(time.Now().Add(40 * time.Second)); err != nil {
		t.Fatal(err)
	}
	proof, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = proof.Close() })
	observed, err := proof.Inspect(t.Context())
	if err != nil || observed.NamespaceDepth != 2 || string(proof.Payload()) != "containerd-cpu-caller" {
		t.Fatalf("authenticate nested CPU caller: %+v %v", observed, err)
	}
	caller := &containerdCaller{connection: connection, proof: proof, pid: observed.HostPID, startTicks: observed.StartTicks,
		namespace: observed.PIDNamespace, nestedPIDs: []string{fmt.Sprint(observed.HostPID), fmt.Sprint(observed.NamespacePID)}}
	assertContainerdCallerAlive(t, caller)
	return fixture.task(t, id), caller
}

func (fixture *containerdProcessFixture) stop(t *testing.T, id string, caller *containerdCaller) {
	t.Helper()
	if _, err := caller.connection.Write([]byte("exit")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, caller.connection); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for fixture.task(t, id).Status != tasktypes.Status_STOPPED {
		if time.Now().After(deadline) {
			t.Fatal("caller/task did not exit")
		}
		time.Sleep(10 * time.Millisecond)
	}
	assertContainerdCallerExited(t, caller)
}

func assertContainerdCallerAlive(t *testing.T, caller *containerdCaller) {
	t.Helper()
	if _, err := caller.proof.Inspect(t.Context()); err != nil {
		t.Fatalf("original kernel process handle is no longer live: %v", err)
	}
}

func assertContainerdCallerExited(t *testing.T, caller *containerdCaller) {
	t.Helper()
	fds := []unix.PollFd{{Fd: int32(caller.proof.pidfd.Fd()), Events: unix.POLLIN}}
	if count, err := unix.Poll(fds, 1000); err != nil || count != 1 || fds[0].Revents&unix.POLLIN == 0 {
		t.Fatalf("original kernel process handle did not report exit: %d %v %+v", count, err, fds)
	}
	if result, err := caller.proof.Inspect(t.Context()); err == nil || result != (RuntimeCallerObservation{}) {
		t.Fatal("exited owner still produced a live process observation")
	}
}

func TestContainerdRuntimeCallerHelper(t *testing.T) {
	mode := os.Getenv(containerdCallerMode)
	if mode == "" {
		t.Skip("containerd subprocess helper")
	}
	if mode == "wrapper" {
		command := exec.CommandContext(t.Context(), "/probe", "-test.run=^TestContainerdRuntimeCallerHelper$", "-test.timeout=60s")
		command.Env = []string{containerdCallerMode + "=owner"}
		if err := command.Run(); err != nil {
			t.Fatal(err)
		}
		return
	}
	if mode != "owner" {
		t.Fatalf("unknown fixture mode: %s", mode)
	}
	connection, err := net.DialTimeout("unixpacket", "/proof/caller.sock", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	if err := connection.SetDeadline(time.Now().Add(40 * time.Second)); err != nil {
		t.Fatal(err)
	}
	challenge := make([]byte, len(runtimeCallerProtocol)+32)
	if count, err := connection.Read(challenge); err != nil || count != len(challenge) || !bytes.HasPrefix(challenge, []byte(runtimeCallerProtocol)) {
		t.Fatalf("read Node caller challenge: %d %v", count, err)
	}
	if _, err := connection.Write(append(challenge, runtimeCallerPayload(t, "containerd-cpu-caller")...)); err != nil {
		t.Fatal(err)
	}
	var command [4]byte
	if _, err := io.ReadFull(connection, command[:]); err != nil || !bytes.Equal(command[:], []byte("exit")) {
		t.Fatalf("caller did not receive its exit command: %q %v", command, err)
	}
}
