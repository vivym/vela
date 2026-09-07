package nodeagent

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRuntimeContainerObserverPinsSocketLifetime(t *testing.T) {
	t.Run("unlink-and-replace", func(t *testing.T) {
		fixture := newContainerCRIServer()
		path := serveContainerCRI(t, fixture)
		original, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		observer := dialContainerCRI(t, path)
		assertRuntimeContainerSocketPins(t, original, 1)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		for range 8 {
			replacement, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o600); err != nil {
				_ = replacement.Close()
				t.Fatal(err)
			}
			current, err := os.Lstat(path)
			if err != nil || os.SameFile(original, current) {
				_ = replacement.Close()
				t.Fatalf("pinned socket inode reused: %v", err)
			}
			if result, err := observer.Inspect(t.Context(), fixture.target); err == nil || result != (RuntimeContainerObservation{}) {
				_ = replacement.Close()
				t.Fatalf("replacement produced evidence: %+v %v", result, err)
			}
			assertRuntimeContainerSocketPins(t, original, 1)
			if err := replacement.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if err := observer.Close(); err != nil {
			t.Fatal(err)
		}
		if err := observer.Close(); err != nil {
			t.Fatal(err)
		}
		assertRuntimeContainerSocketPins(t, original, 0)
	})
	t.Run("failed-boot-validation", func(t *testing.T) {
		path := serveContainerCRI(t, newContainerCRIServer())
		original, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		for range 8 {
			observer, err := dialRuntimeContainerObserver(t.Context(), RuntimeContainerObserverConfig{SocketPath: path, NodeIdentity: "node-1"},
				uint32(os.Geteuid()), func() (string, error) { return "invalid", nil })
			if err == nil || observer != nil {
				t.Fatal("invalid boot produced an observer")
			}
			assertRuntimeContainerSocketPins(t, original, 0)
		}
	})
	t.Run("failed-connection", func(t *testing.T) {
		path := serveContainerCRI(t, newContainerCRIServer()) + ".offline"
		listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		listener.SetUnlinkOnClose(false)
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatal(err)
		}
		original, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		observer, err := dialRuntimeContainerObserver(ctx, RuntimeContainerObserverConfig{SocketPath: path, NodeIdentity: "node-1"},
			uint32(os.Geteuid()), func() (string, error) { return uuid.NewString(), nil })
		if err == nil || observer != nil {
			t.Fatal("offline socket produced an observer")
		}
		assertRuntimeContainerSocketPins(t, original, 0)
	})
}

func assertRuntimeContainerSocketPins(t *testing.T, original os.FileInfo, expected int) {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		info, err := os.Stat(filepath.Join("/proc/self/fd", entry.Name()))
		if err == nil && os.SameFile(original, info) {
			count++
		}
	}
	if count != expected {
		t.Fatalf("socket inode descriptor count=%d, want=%d", count, expected)
	}
}

func TestRuntimeContainerObserverUsesHostBootAndRootPeer(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("the public Node observer requires a root-owned CRI fixture")
	}
	fixture := newContainerCRIServer()
	path := serveContainerCRI(t, fixture)
	observer, err := DialRuntimeContainerObserver(t.Context(), RuntimeContainerObserverConfig{SocketPath: path, NodeIdentity: "node-1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observer.Close() })
	result, err := observer.Inspect(t.Context(), fixture.target)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || result.BootID.String() != strings.TrimSpace(string(boot)) {
		t.Fatalf("public observer did not use the actual host boot identity: %+v %v", result, err)
	}
}

func TestRuntimeContainerObserverRejectsSocketOwnerDifferentFromKernelPeer(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("changing the fixture socket owner requires root")
	}
	path := serveContainerCRI(t, newContainerCRIServer())
	if err := os.Chmod(path, 0o660); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(path, 65534, 0); err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skip("changing the fixture socket owner requires CAP_CHOWN")
		}
		t.Fatal(err)
	}
	connection, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := runtimeContainerPeerUID(connection)
	if closeErr := connection.Close(); err != nil || closeErr != nil || peer != 0 {
		t.Fatalf("fixture did not retain the original root kernel peer: uid=%d err=%v close=%v", peer, err, closeErr)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	observer, err := dialRuntimeContainerObserver(ctx, RuntimeContainerObserverConfig{SocketPath: path, NodeIdentity: "node-1"}, 65534,
		func() (string, error) { return uuid.NewString(), nil })
	if observer != nil || err == nil {
		t.Fatal("socket file ownership impersonated the kernel-reported CRI peer")
	}
}

func TestRuntimeContainerObserverCommandAgainstCRI(t *testing.T) {
	binary := os.Getenv("VELA_TEST_NODE_AGENT_BINARY")
	if os.Geteuid() != 0 || binary == "" {
		t.Skip("requires root and an explicitly compiled Node Agent binary")
	}
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "observed", true: "garbage-collected"}[missing], func(t *testing.T) {
			fixture := newContainerCRIServer()
			if missing {
				fixture.hook = func(method string, _ int) error {
					if method == "ContainerStatus" {
						return status.Error(codes.NotFound, "garbage collected")
					}
					return nil
				}
			}
			path := serveContainerCRI(t, fixture)
			target := fixture.target
			command := exec.CommandContext(t.Context(), binary, "inspect-runtime-container", "--cri-socket", path,
				"--node-identity", "node-1", "--container-id", target.ContainerID, "--sandbox-id", target.SandboxID,
				"--pod-uid", target.PodUID.String(), "--pod-namespace", target.PodNamespace, "--pod-name", target.PodName,
				"--container-name", target.ContainerName, "--container-attempt", "3")
			output, err := command.Output()
			if missing {
				if err == nil || len(output) != 0 {
					t.Fatalf("missing container produced command evidence: %v %s", err, output)
				}
				return
			}
			var result RuntimeContainerObservation
			if err != nil || json.Unmarshal(output, &result) != nil || result.Target != target || result.BootID == uuid.Nil ||
				result.RuntimeName != "vela-cri-mock" || result.SandboxPIDNamespace != "CONTAINER" {
				t.Fatalf("compiled Node command did not preserve CRI observation: %v %s", err, output)
			}
		})
	}
}
