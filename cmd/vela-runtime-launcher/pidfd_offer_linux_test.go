//go:build linux

package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	tasktypes "github.com/containerd/containerd/api/types/task"
	"github.com/vivym/vela/internal/runtimechannel"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type offeredTask struct {
	tasksapi.TasksClient
	testing *testing.T
	id      string
	pid     uint32
}

func (task offeredTask) Get(ctx context.Context, request *tasksapi.GetRequest, _ ...grpc.CallOption) (*tasksapi.GetResponse, error) {
	task.testing.Helper()
	header, _ := metadata.FromOutgoingContext(ctx)
	if request.ContainerID != task.id || len(header.Get("containerd-namespace")) != 1 || header.Get("containerd-namespace")[0] != "k8s.io" {
		task.testing.Error("offer query is not bound to the expected CRI task and namespace")
	}
	return &tasksapi.GetResponse{Process: &tasktypes.Process{Pid: task.pid, Status: tasktypes.Status_RUNNING}}, nil
}

func TestProductionPIDFDOfferHelper(t *testing.T) {
	mode := os.Getenv("VELA_TEST_PIDFD_OFFER_MODE")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	socket := os.Getenv("VELA_TEST_PIDFD_OFFER_SOCKET")
	if mode == "self" {
		if err := runPIDFDOffer([]string{"--socket", socket, "--", "/bin/sleep", "60"}); err != nil {
			t.Fatal(err)
		}
		return
	}
	connection, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: socket, Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	var descriptor *os.File
	var child *exec.Cmd
	if mode == "different-process" {
		fd := -1
		child = exec.Command("/bin/sleep", "60")
		child.SysProcAttr = &syscall.SysProcAttr{PidFD: &fd, Pdeathsig: syscall.SIGKILL}
		if err := child.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
		descriptor = os.NewFile(uintptr(fd), "different-process-pidfd")
	} else {
		descriptor, err = os.Open("/dev/null")
		if err != nil {
			t.Fatal(err)
		}
	}
	defer descriptor.Close()
	if _, _, err := connection.WriteMsgUnix([]byte(pidfdOfferFrame), unix.UnixRights(int(descriptor.Fd())), nil); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin) // parent retains helper until validation finishes
}

func TestPIDFDOfferReleaseWaitFiltersSignalsAndTimesOut(t *testing.T) {
	release := make(chan os.Signal, 2)
	release <- syscall.SIGUSR1
	if err := waitForPIDFDOfferRelease(release, 5*time.Millisecond); err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("unrelated signal released pidfd gate: %v", err)
	}
	release <- syscall.SIGCONT
	if err := waitForPIDFDOfferRelease(release, time.Second); err != nil {
		t.Fatalf("SIGCONT did not release pidfd gate: %v", err)
	}
}

func TestProductionPIDFDOfferBindsKernelSenderToTask(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root to launch a distinct non-root process")
	}
	root, err := os.MkdirTemp("/tmp", "vela-offer-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "offer.test")
	data, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binary, data, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"self", "wrong-task", "wrong-uid", "regular-file", "different-process"} {
		t.Run(scenario, func(t *testing.T) {
			directory, err := os.MkdirTemp(root, "offer-")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(directory, 0o755); err != nil {
				t.Fatal(err)
			}
			uid := uint32(65532)
			if scenario == "wrong-uid" {
				uid--
			}
			// The host service uses UMask=0077. It must still publish the
			// explicitly approved group traversal for the non-root sender.
			previousMask := unix.Umask(0o077)
			offer, err := newPIDFDOffer(directory, "worker", uid, 65532)
			unix.Umask(previousMask)
			if err != nil {
				t.Fatal(err)
			}
			defer offer.Close()
			mode := scenario
			if scenario == "wrong-task" || scenario == "wrong-uid" {
				mode = "self"
			}
			command := exec.Command(binary, "-test.run=^TestProductionPIDFDOfferHelper$")
			command.Env = append(os.Environ(), "VELA_TEST_PIDFD_OFFER_MODE="+mode, "VELA_TEST_PIDFD_OFFER_SOCKET="+offer.hostPath)
			if scenario == "self" {
				// Exercise the release race: the parent sends SIGCONT as soon as
				// custody receives the offered pidfd, before the wrapper necessarily
				// reaches its wait.
				command.Env = append(command.Env, "VELA_RUNTIME_LAUNCHER_PIDFD_GATE=1")
			}
			original := -1
			command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65532, Gid: 65532}, PidFD: &original}
			command.Stderr = os.Stderr
			stdin, err := command.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = stdin.Close(); _ = command.Process.Kill(); _ = command.Wait(); _ = unix.Close(original) }()
			expectedPID := uint32(command.Process.Pid)
			if scenario == "wrong-task" {
				expectedPID++
			}
			file, err := offer.accept(t.Context(), offeredTask{testing: t, id: "current-task", pid: expectedPID}, "current-task")
			if scenario != "self" {
				if err == nil || file != nil {
					t.Fatalf("invalid offer accepted: file=%v error=%v", file, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if err := unix.PidfdSendSignal(original, unix.SIGCONT, nil, 0); err != nil {
				t.Fatalf("release pidfd gate: %v", err)
			}
			if err := runtimechannel.SameLiveProcess(original, int(file.Fd())); err != nil {
				t.Fatalf("offered pidfd differs from original child handle: %v", err)
			}
			deadline := time.Now().Add(time.Second)
			for {
				args, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(command.Process.Pid), "cmdline"))
				if err == nil && strings.HasPrefix(string(args), "/bin/sleep\x0060\x00") {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("wrapper did not exec the target: %q %v", args, err)
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestProductionPIDFDOfferCanceledAccept(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root-owned socket")
	}
	offer, err := newPIDFDOffer(t.TempDir(), "runtime", 65532, 65532)
	if err != nil {
		t.Fatal(err)
	}
	defer offer.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	started := time.Now()
	file, err := offer.accept(ctx, offeredTask{testing: t}, "task")
	if file != nil || !errors.Is(err, context.Canceled) || time.Since(started) > time.Second {
		t.Fatalf("accept cancellation: file=%v error=%v", file, err)
	}
}
