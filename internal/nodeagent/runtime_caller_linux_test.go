package nodeagent

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

const runtimeCallerTestMode = "VELA_RUNTIME_CALLER_TEST_MODE"

func TestRuntimeCallerAuthenticatedMessage(t *testing.T) {
	connection, process, wait := runtimeCallerConnection(t, "normal", "unixpacket")
	caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = caller.Close() })
	observed, err := caller.Inspect(t.Context())
	if err != nil || observed.HostPID != int32(process.Pid) || observed.NamespacePID != int32(process.Pid) ||
		observed.NamespaceDepth != 1 || observed.UID != 65532 || observed.GID != 65532 ||
		observed.StartTicks == 0 || observed.BootID == uuid.Nil || observed.ObservedAt.IsZero() || string(caller.Payload()) != "test-startup-request" {
		t.Fatalf("wrong process/payload observation: %+v %v", observed, err)
	}
	flags, err := unix.FcntlInt(caller.pidfd.Fd(), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("retained pidfd may escape exec: %d %v", flags, err)
	}
	copy := caller.Payload()
	copy[0] = 'X'
	if string(caller.Payload()) != "test-startup-request" {
		t.Fatal("caller payload aliases retained request bytes")
	}
	if _, err := connection.Write([]byte("exit")); err != nil {
		t.Fatal(err)
	}
	wait()
	if result, err := caller.Inspect(t.Context()); err == nil || result != (RuntimeCallerObservation{}) {
		t.Fatal("exited process retained a live observation")
	}
	var parallel sync.WaitGroup
	for range 8 {
		parallel.Go(func() {
			_ = caller.Close()
			_, _ = caller.Inspect(t.Context())
			_ = caller.Payload()
		})
	}
	parallel.Wait()
	if caller.Payload() != nil || caller.Close() != nil {
		t.Fatal("closed caller retained payload or failed idempotent Close")
	}
}

func TestRuntimeCallerRejectsInvalidMessages(t *testing.T) {
	for _, mode := range []string{"delegated", "wrong-challenge", "empty", "oversized", "rights", "truncated-rights", "wrong-uid", "wrong-gid", "stream"} {
		t.Run(mode, func(t *testing.T) {
			network := "unixpacket"
			if mode == "stream" {
				network = "unix"
			}
			connection, _, wait := runtimeCallerConnection(t, mode, network)
			before := runtimeCallerDescriptorCount(t)
			expected := RuntimeCallerCredentials{UID: 65532, GID: 65532}
			if mode == "wrong-uid" {
				expected.UID--
			}
			if mode == "wrong-gid" {
				expected.GID--
			}
			caller, err := ReceiveRuntimeCaller(t.Context(), connection, expected)
			if err == nil || caller != nil {
				if caller != nil {
					_ = caller.Close()
				}
				t.Fatal("untrusted message produced a pinned caller")
			}
			if mode == "stream" {
				if !strings.Contains(err.Error(), "requires a Unix seqpacket socket") {
					t.Fatalf("stream fixture failed for an unrelated reason: %v", err)
				}
			} else if !errors.Is(err, ErrRuntimeCallerIdentity) {
				t.Fatalf("fixture did not reach its identity rejection: %v", err)
			}
			if after := runtimeCallerDescriptorCount(t); before != after {
				t.Fatalf("rejected ancillary data leaked descriptors: before=%d after=%d", before, after)
			}
			if mode == "delegated" {
				// Let the opener reap its child before fixture cleanup kills it.
				if err := connection.Close(); err != nil {
					t.Fatal(err)
				}
				wait()
			}
			t.Logf("rejected %s without descriptor growth", mode)
		})
	}
}

func TestRuntimeCallerDeadline(t *testing.T) {
	for _, cancelBefore := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending-read", true: "before-read"}[cancelBefore], func(t *testing.T) {
			connection, _, _ := runtimeCallerConnection(t, "idle", "unixpacket")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if cancelBefore {
				cancel()
			} else {
				timer := time.AfterFunc(50*time.Millisecond, cancel)
				defer timer.Stop()
			}
			before := runtimeCallerDescriptorCount(t)
			started := time.Now()
			caller, err := ReceiveRuntimeCaller(ctx, connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
			if caller != nil || !errors.Is(err, context.Canceled) || time.Since(started) > time.Second {
				t.Fatalf("canceled read did not fail promptly: %v", err)
			}
			if after := runtimeCallerDescriptorCount(t); before != after {
				t.Fatalf("canceled read leaked descriptors: before=%d after=%d", before, after)
			}
		})
	}
}

func TestRuntimeCallerProcessParser(t *testing.T) {
	status := "Name:\tprobe\nUid:\t65532\t65532\t65532\t65532\nGid:\t65532\t65532\t65532\t65532\nNSpid:\t123\t1\n"
	stat := "123 (probe (with) spaces) S " + strings.Repeat("0 ", 18) + "456 0\n"
	peer := unix.Ucred{Pid: 123, Uid: 65532, Gid: 65532}
	if value, err := parseRuntimeProcess(status, stat, peer); err != nil || value.HostPID != 123 || value.StartTicks != 456 || value.NamespacePID != 1 || value.NamespaceDepth != 2 {
		t.Fatalf("valid kernel process fields failed: %+v %v", value, err)
	}
	for name, changed := range map[string]string{
		"changed-uid": strings.ReplaceAll(status, "Uid:\t65532\t65532\t65532\t65532", "Uid:\t65532\t0\t65532\t65532"),
		"changed-gid": strings.ReplaceAll(status, "Gid:\t65532\t65532\t65532\t65532", "Gid:\t65532\t65532\t0\t65532"),
		"wrong-pid":   strings.ReplaceAll(status, "123\t1", "124\t1"),
		"duplicate":   status + "NSpid:\t123\t1\n",
		"missing":     strings.ReplaceAll(status, "NSpid:\t123\t1\n", ""),
		"zero-pid":    strings.ReplaceAll(status, "123\t1", "123\t0"),
	} {
		t.Run(name, func(t *testing.T) {
			if value, err := parseRuntimeProcess(changed, stat, peer); err == nil || value != (RuntimeCallerObservation{}) {
				t.Fatal("invalid process status produced an observation")
			}
		})
	}
	for _, changed := range []string{"", "123 bad-format", strings.Replace(stat, "123 (", "124 (", 1),
		strings.Replace(stat, ") S ", ") Z ", 1), strings.Replace(stat, "456", "0", 1), strings.Replace(stat, "456", "bad", 1)} {
		if value, err := parseRuntimeProcess(status, changed, peer); err == nil || value != (RuntimeCallerObservation{}) {
			t.Fatal("invalid process stat produced an observation")
		}
	}
}

func runtimeCallerDescriptorCount(t *testing.T) int {
	t.Helper()
	files, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(files)
}

func runtimeCallerConnection(t *testing.T, mode, network string, privatePID ...bool) (*net.UnixConn, *os.Process, func()) {
	t.Helper()
	return runtimeCallerConfiguredConnection(t, mode, network, []byte("test-startup-request"),
		RuntimeCallerCredentials{UID: 65532, GID: 65532}, len(privatePID) != 0 && privatePID[0])
}

func runtimeCallerConfiguredConnection(t *testing.T, mode, network string, payload []byte, credentials RuntimeCallerCredentials, privatePID bool) (*net.UnixConn, *os.Process, func()) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("the Runtime caller fixture must launch a different non-root UID")
	}
	root, err := os.MkdirTemp("/tmp", "vela-runtime-caller-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(root, "caller.sock")
	listener, err := net.ListenUnix(network, &net.UnixAddr{Name: socket, Net: network})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	if err := os.Chmod(socket, 0o666); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), binary, "-test.run=^TestRuntimeCallerProcessHelper$", "-test.timeout=15s")
	command.Env = []string{runtimeCallerTestMode + "=" + mode, "VELA_RUNTIME_CALLER_TEST_SOCKET=" + socket, "VELA_RUNTIME_CALLER_TEST_NETWORK=" + network,
		"VELA_RUNTIME_CALLER_TEST_PAYLOAD=" + base64.StdEncoding.EncodeToString(payload)}
	command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: credentials.UID, Gid: credentials.GID}}
	if privatePID {
		command.SysProcAttr.Cloneflags = unix.CLONE_NEWPID
	}
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = command.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		_ = command.Process.Kill()
		<-done
	})
	if err := listener.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	connection, err := listener.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection, command.Process, func() {
		select {
		case <-done:
			if waitErr != nil {
				t.Fatalf("Runtime caller helper: %s %v", output.String(), waitErr)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Runtime caller helper did not exit")
		}
	}
}

func TestRuntimeCallerProcessHelper(t *testing.T) {
	mode := os.Getenv(runtimeCallerTestMode)
	if mode == "" {
		t.Skip("Runtime caller subprocess helper")
	}
	var connection *net.UnixConn
	if mode == "inherited" {
		file := os.NewFile(3, "inherited-connection")
		local, err := net.FileConn(file)
		_ = file.Close()
		if err != nil {
			t.Fatal(err)
		}
		var ok bool
		connection, ok = local.(*net.UnixConn)
		if !ok {
			_ = local.Close()
			t.Fatal("inherited connection is not a Unix socket")
		}
	} else {
		var err error
		connection, err = net.DialUnix(os.Getenv("VELA_RUNTIME_CALLER_TEST_NETWORK"), nil,
			&net.UnixAddr{Name: os.Getenv("VELA_RUNTIME_CALLER_TEST_SOCKET"), Net: os.Getenv("VELA_RUNTIME_CALLER_TEST_NETWORK")})
		if err != nil {
			t.Fatal(err)
		}
	}
	defer func() { _ = connection.Close() }()
	if mode == "delegated" {
		file, err := connection.File()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = file.Close() }()
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		command := exec.CommandContext(t.Context(), binary, "-test.run=^TestRuntimeCallerProcessHelper$", "-test.timeout=15s")
		command.ExtraFiles = []*os.File{file}
		command.Env = []string{runtimeCallerTestMode + "=inherited"}
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("inherited sender: %s %v", output, err)
		}
		return
	}
	challenge := make([]byte, len(runtimeCallerProtocol)+32)
	if count, err := connection.Read(challenge); err != nil || count != len(challenge) {
		return
	}
	packet := append(challenge, runtimeCallerPayload(t, "test-startup-request")...)
	var ancillary []byte
	switch mode {
	case "idle":
		_, _ = io.Copy(io.Discard, connection)
		return
	case "wrong-challenge":
		packet[0]++
	case "empty":
		packet = challenge
	case "oversized":
		packet = append(challenge, make([]byte, runtimeCallerMaximum+1)...)
	case "rights", "truncated-rights":
		file, err := os.Open("/proc/self/stat")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = file.Close() }()
		count := 1
		if mode == "truncated-rights" {
			count = 250
		}
		fds := make([]int, count)
		for i := range fds {
			fds[i] = int(file.Fd())
		}
		ancillary = unix.UnixRights(fds...)
	}
	if _, _, err := connection.WriteMsgUnix(packet, ancillary, nil); err != nil {
		t.Fatal(err)
	}
	var response [4]byte
	_, _ = io.ReadFull(connection, response[:])
}

func runtimeCallerPayload(t *testing.T, fallback string) []byte {
	t.Helper()
	if encoded := os.Getenv("VELA_RUNTIME_CALLER_TEST_PAYLOAD"); encoded != "" {
		payload, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	return []byte(fallback)
}
