//go:build integration && linux

package nodeagent

import (
	"context"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const runtimeImagePrivateMount = "VELA_TEST_IMAGE_PRIVATE_MOUNT"

func testRuntimeImageMountReaderDeadPeer(t *testing.T) {
	fixture := startProcessContainerd(t)
	connection, err := net.Dial("unix", fixture.socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	reader := &runtimeImageMountReader{}
	t.Cleanup(func() { _ = reader.close() })
	if err := reader.authenticate(connection); err != nil {
		t.Fatal(err)
	}
	fixture.stopDaemon(t, syscall.SIGKILL, true)
	if mounted, err := reader.mounted("/absent"); err == nil || mounted {
		t.Fatalf("dead daemon produced mount absence evidence: %v %v", mounted, err)
	}
	if err := reader.close(); err != nil {
		t.Fatal(err)
	}
	stdin, err := os.Stdin.Stat()
	if err != nil {
		t.Fatal(err)
	}
	before := runtimeImagePIDFDCount(t)
	for range 8 {
		candidate := &runtimeImageMountReader{}
		authErr := candidate.authenticate(connection)
		if err := candidate.close(); err != nil || authErr == nil {
			t.Fatalf("dead socket creator was authenticated: %v %v", authErr, err)
		}
	}
	current, err := os.Stdin.Stat()
	if err != nil || !os.SameFile(stdin, current) || runtimeImagePIDFDCount(t) != before {
		t.Fatalf("failed daemon authentication closed unrelated descriptors or leaked pidfds: %v", err)
	}
}

func testRuntimeImagePrivateRecovery(t *testing.T, fixture *containerdProcessFixture, observer *RuntimeImageObserver, point runtimeImageCrashPoint, released bool) {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), binary, "-test.run=^TestRuntimeImagePrivateRecoveryHelper$", "-test.v", "-test.timeout=45s")
	command.Env = append(os.Environ(), runtimeImagePrivateMount+"="+point.MountPoint,
		"VELA_TEST_IMAGE_SOCKET="+fixture.socket, "VELA_TEST_IMAGE_NAMESPACE="+observer.namespace)
	if released {
		command.Env = append(command.Env, "VELA_TEST_IMAGE_PRIVATE_RELEASED=1")
	}
	command.SysProcAttr = &syscall.SysProcAttr{Cloneflags: unix.CLONE_NEWNS}
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "--- PASS: TestRuntimeImagePrivateRecoveryHelper ") || strings.Contains(string(output), "--- SKIP:") {
		t.Fatalf("private mount recovery did not preserve daemon allocation state: %v\n%s", err, output)
	}
	t.Log(string(output))
}

func TestRuntimeImagePrivateRecoveryHelper(t *testing.T) {
	path := os.Getenv(runtimeImagePrivateMount)
	if path == "" {
		t.Skip("requires the private image mount subprocess")
	}
	if os.Getenv("VELA_TEST_CONTAINERD_SANDBOX") != "1" || !validRuntimeImagePath(path) || path == "/" {
		t.Fatal("invalid private image mount fixture")
	}
	if os.Getenv("VELA_TEST_IMAGE_PRIVATE_READER") != "1" {
		if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
			t.Fatal(err)
		}
		if mounted, err := runtimeImagePathMounted(path); err != nil || !mounted {
			t.Fatalf("private clone did not start with the retained mount: %v %v", mounted, err)
		}
		if err := unix.Unmount(path, unix.MNT_DETACH); err != nil {
			t.Fatal(err)
		}
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		command := exec.CommandContext(t.Context(), "/usr/bin/setpriv", "--bounding-set=-all", "--inh-caps=-all", "--ambient-caps=-all", "--no-new-privs",
			binary, "-test.run=^TestRuntimeImagePrivateRecoveryHelper$", "-test.v", "-test.timeout=40s")
		command.Env = append(os.Environ(), "VELA_TEST_IMAGE_PRIVATE_READER=1")
		output, err := command.CombinedOutput()
		if err != nil || !strings.Contains(string(output), "--- PASS: TestRuntimeImagePrivateRecoveryHelper ") || strings.Contains(string(output), "--- SKIP:") {
			t.Fatalf("capability-free private recovery failed: %v\n%s", err, output)
		}
		t.Log(string(output))
		return
	}
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"CapInh:\t0000000000000000", "CapPrm:\t0000000000000000", "CapEff:\t0000000000000000",
		"CapBnd:\t0000000000000000", "CapAmb:\t0000000000000000", "NoNewPrivs:\t1"} {
		if !strings.Contains(string(status), line+"\n") {
			t.Fatalf("private reader did not retain capability restriction: %s", line)
		}
	}
	if mounted, err := runtimeImagePathMounted(path); err != nil || mounted {
		t.Fatalf("private mount view did not hide the retained mount: %v %v", mounted, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	observer, err := DialRuntimeImageObserver(ctx, RuntimeImageObserverConfig{
		RuntimeContainerObserverConfig: RuntimeContainerObserverConfig{SocketPath: os.Getenv("VELA_TEST_IMAGE_SOCKET"), NodeIdentity: "cpu-image-crash-node"},
		Namespace:                      os.Getenv("VELA_TEST_IMAGE_NAMESPACE"), Snapshotter: "native",
	})
	if err != nil {
		t.Fatalf("private reader failed to authenticate containerd: %v", err)
	}
	defer func() { _ = observer.Close() }()
	count, err := observer.RecoverExpired(ctx)
	t.Logf("private mount recovery returned: %d %v", count, err)
	if os.Getenv("VELA_TEST_IMAGE_PRIVATE_RELEASED") == "1" {
		if err != nil || count != 1 {
			t.Fatalf("private reader did not complete released daemon mount: %d %v", count, err)
		}
	} else if err == nil || count != 0 || !strings.Contains(err.Error(), "kernel mount is still busy") || ctx.Err() != nil {
		t.Fatalf("private mount view incorrectly completed daemon recovery: %d %v", count, err)
	}
}
