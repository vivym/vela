package nodeagent

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
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

	"github.com/vivym/vela/internal/runtimechannel"
	"golang.org/x/sys/unix"
)

const runtimeChannelHelperMode = "VELA_RUNTIME_CHANNEL_HELPER"

func TestRuntimeChannelRoundTrip(t *testing.T) {
	for _, privatePID := range []bool{false, true} {
		t.Run(fmt.Sprintf("private-pid=%t", privatePID), func(t *testing.T) {
			connection, _, finish := runtimeChannelFixture(t, "normal", privatePID)
			caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = caller.Close() })
			if string(caller.Payload()) != "channel-request" {
				t.Fatal("authenticated request changed")
			}
			before := runtimeCallerDescriptorCount(t)
			var replies sync.WaitGroup
			results := make(chan error, 8)
			for range 8 {
				replies.Go(func() { results <- caller.Reply(t.Context(), []byte("channel-response")) })
			}
			replies.Wait()
			close(results)
			success := 0
			for err := range results {
				if err == nil {
					success++
				} else if !errors.Is(err, ErrRuntimeCallerIdentity) {
					t.Fatal(err)
				}
			}
			if success != 1 || runtimeCallerDescriptorCount(t) != before {
				t.Fatalf("one-shot reply: successes=%d", success)
			}
			finish()
			if err := caller.Close(); err != nil {
				t.Fatal(err)
			}
			if err := caller.Reply(t.Context(), []byte("late")); !errors.Is(err, ErrRuntimeCallerIdentity) {
				t.Fatalf("closed caller accepted a response: %v", err)
			}
		})
	}
}

func TestRuntimeChannelLargeRequestBounds(t *testing.T) {
	for _, mode := range []string{"large-request", "large-default-receiver", "large-response"} {
		t.Run(mode, func(t *testing.T) {
			connection, _, finish := runtimeChannelFixture(t, mode, true)
			maximum := runtimechannel.MaximumRequestPayload
			if mode == "large-default-receiver" {
				maximum = runtimechannel.MaximumPayload
			}
			caller, err := ReceiveRuntimeCallerWithRequestLimit(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532}, maximum)
			if mode == "large-default-receiver" {
				if err == nil || caller != nil {
					t.Fatal("default receiver accepted oversized request")
				}
				_ = connection.Close()
			} else {
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = caller.Close() }()
				if !bytes.Equal(caller.Payload(), bytes.Repeat([]byte{'x'}, runtimechannel.MaximumRequestPayload)) {
					t.Fatal("large request truncated or changed")
				}
				if mode == "large-response" {
					if err := caller.Reply(t.Context(), make([]byte, runtimechannel.MaximumPayload+1)); err == nil {
						t.Fatal("larger request widened reply limit")
					}
				}
				if err := caller.Reply(t.Context(), []byte("channel-response")); err != nil {
					t.Fatal(err)
				}
			}
			finish()
		})
	}
}

func TestRuntimeChannelRejectsUntrustedExchange(t *testing.T) {
	for _, mode := range []string{"challenge-prefix", "challenge-short", "challenge-long", "challenge-rights", "challenge-delegated",
		"response-replay", "response-reflected", "response-empty", "response-long", "response-rights", "response-truncated-rights",
		"response-delegated", "socket-replaced", "cancel-challenge", "cancel-response", "cancel-before",
		"socket-owner", "socket-mode", "socket-link", "directory-owner", "directory-mode", "directory-link",
		"peer-uid", "peer-gid"} {
		t.Run(mode, func(t *testing.T) {
			// The actual Node is outside this Runtime's namespace. A delegated root
			// sender also reports PID 0; only the pinned pidfs identity distinguishes it.
			connection, socket, finish := runtimeChannelFixture(t, mode, true)
			if connection == nil {
				finish()
				return
			}
			if mode == "cancel-challenge" {
				finish()
				return
			}
			challenge := append([]byte(runtimechannel.Protocol), bytes.Repeat([]byte{42}, 32)...)
			if strings.HasPrefix(mode, "challenge-") {
				packet := bytes.Clone(challenge)
				switch mode {
				case "challenge-prefix":
					packet[0]++
				case "challenge-short":
					packet = packet[:len(packet)-1]
				case "challenge-long":
					packet = append(packet, 0)
				}
				runtimeChannelSend(t, connection, packet, mode)
				finish()
				return
			}
			caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = caller.Close() })
			if mode == "cancel-response" {
				finish()
				return
			}
			if mode == "socket-replaced" {
				if err := os.Rename(socket, socket+".original"); err != nil {
					t.Fatal(err)
				}
				replacement, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socket, Net: "unixpacket"})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = replacement.Close() }()
				if err := errors.Join(os.Chown(socket, 0, 65532), os.Chmod(socket, 0o660)); err != nil {
					t.Fatal(err)
				}
			}
			packet := append([]byte(runtimechannel.ResponseProtocol), caller.challenge...)
			switch mode {
			case "response-replay":
				packet[len(packet)-1]++
			case "response-reflected":
				packet = bytes.Clone(caller.challenge)
			}
			if mode != "response-empty" {
				packet = append(packet, []byte("channel-response")...)
			}
			if mode == "response-long" {
				packet = append(packet, make([]byte, runtimechannel.MaximumPayload)...)
			}
			runtimeChannelSend(t, connection, packet, mode)
			finish()
		})
	}
}

func TestRuntimeChannelReplyRejectsLostLifetime(t *testing.T) {
	for _, mode := range []string{"expired", "exited", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			connection, process, wait := runtimeCallerConnection(t, "normal", "unixpacket")
			caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = caller.Close() })
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch mode {
			case "expired":
				caller.replyDeadline = time.Now().Add(-time.Second)
			case "exited":
				if _, err := connection.Write([]byte("exit")); err != nil {
					t.Fatal(err)
				}
				wait()
			case "canceled":
				cancel()
			}
			if err := caller.Reply(ctx, []byte("must-not-send")); err == nil {
				t.Fatalf("%s caller accepted a response, pid=%d", mode, process.Pid)
			}
		})
	}
}

func TestRuntimeChannelPIDNamespaceIdentity(t *testing.T) {
	connection, _, finish := runtimeChannelFixture(t, "identity-probe", true)
	packet := make([]byte, 16)
	if count, err := connection.Read(packet); err != nil || string(packet[:count]) != "ready" {
		t.Fatalf("namespace identity probe not ready: %v", err)
	}
	runtimeChannelSend(t, connection, []byte("original"), "normal")
	runtimeChannelSend(t, connection, []byte("delegated"), "probe-delegated")
	finish()
}

func runtimeChannelSend(t *testing.T, connection *net.UnixConn, packet []byte, mode string) {
	if strings.HasSuffix(mode, "delegated") {
		file, err := connection.File()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = file.Close() }()
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Cleanup(cancel)
		command := exec.CommandContext(ctx, binary, "-test.run=^TestRuntimeChannelProcessHelper$")
		command.ExtraFiles = []*os.File{file}
		command.Env = []string{runtimeChannelHelperMode + "=delegate", "VELA_CHANNEL_PACKET=" + base64.StdEncoding.EncodeToString(packet)}
		// Keep the delegate alive until the client decides, so liveness alone
		// cannot accidentally reject a mismatched but still-live root sender.
		input, err := command.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		var output bytes.Buffer
		command.Stdout, command.Stderr = &output, &output
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = input.Close()
			if err := command.Wait(); err != nil {
				t.Fatalf("delegate failed: %s %v", output.String(), err)
			}
		})
		return
	}
	var ancillary []byte
	if strings.HasSuffix(mode, "rights") {
		count := 1
		if mode == "response-truncated-rights" {
			count = 250
		}
		fds := make([]int, count)
		for i := range fds {
			fds[i] = 1
		}
		ancillary = unix.UnixRights(fds...)
	}
	if _, _, err := connection.WriteMsgUnix(packet, ancillary, nil); err != nil {
		t.Fatal(err)
	}
}

func runtimeChannelFixture(t *testing.T, mode string, privatePID bool) (*net.UnixConn, string, func()) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("requires a root Node and non-root Runtime CPU fixture")
	}
	root, err := os.MkdirTemp("/run", "vela-channel-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, "endpoint")
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(directory, "node.sock")
	var listener *net.UnixListener
	if strings.HasPrefix(mode, "peer-") {
		runtimeChannelUntrustedListener(t, socket, mode)
	} else {
		listener, err = net.ListenUnix("unixpacket", &net.UnixAddr{Name: socket, Net: "unixpacket"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
	}
	if err := errors.Join(os.Chown(socket, 0, 65532), os.Chmod(socket, 0o660)); err != nil {
		t.Fatal(err)
	}
	var fixtureErr error
	switch mode {
	case "socket-owner":
		fixtureErr = os.Chown(socket, 65532, 65532)
	case "socket-mode":
		fixtureErr = os.Chmod(socket, 0o666)
	case "socket-link":
		fixtureErr = os.Symlink(socket, filepath.Join(directory, "alias.sock"))
		socket = filepath.Join(directory, "alias.sock")
	case "directory-owner":
		fixtureErr = os.Chown(directory, 65532, 65532)
	case "directory-mode":
		fixtureErr = os.Chmod(directory, 0o777)
	case "directory-link":
		fixtureErr = os.Symlink(directory, filepath.Join(root, "alias"))
		socket = filepath.Join(root, "alias", "node.sock")
	}
	if fixtureErr != nil {
		t.Fatal(fixtureErr)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), binary, "-test.run=^TestRuntimeChannelProcessHelper$", "-test.v", "-test.timeout=15s")
	command.Env = []string{runtimeChannelHelperMode + "=client", "VELA_CHANNEL_MODE=" + mode, "VELA_CHANNEL_SOCKET=" + socket,
		fmt.Sprintf("VELA_CHANNEL_PRIVATE_PID=%t", privatePID)}
	command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 65532, Gid: 65532}}
	if privatePID {
		command.SysProcAttr.Cloneflags = unix.CLONE_NEWPID
	}
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
	finish := func() {
		_ = input.Close()
		select {
		case <-done:
			if waitErr != nil || !strings.Contains(output.String(), "--- PASS: TestRuntimeChannelProcessHelper ") {
				t.Fatalf("Runtime client: %s %v", output.String(), waitErr)
			}
			t.Log(output.String())
		case <-time.After(7 * time.Second):
			t.Fatal("Runtime client did not finish")
		}
	}
	if listener == nil || strings.HasPrefix(mode, "directory-") || strings.HasPrefix(mode, "socket-") && mode != "socket-replaced" || mode == "cancel-before" {
		return nil, socket, finish
	}
	if err := listener.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	connection, err := listener.AcceptUnix()
	if err != nil {
		finish()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection, socket, finish
}

func TestRuntimeChannelProcessHelper(t *testing.T) {
	switch os.Getenv(runtimeChannelHelperMode) {
	case "client":
		privatePID := os.Getenv("VELA_CHANNEL_PRIVATE_PID") == "true"
		if privatePID && os.Getpid() != 1 {
			t.Fatal("Runtime fixture is not namespace PID 1")
		}
		mode := os.Getenv("VELA_CHANNEL_MODE")
		if mode == "identity-probe" {
			runtimeChannelNamespaceProbe(t)
			_, _ = io.Copy(io.Discard, os.Stdin)
			return
		}
		ctx := t.Context()
		if strings.HasPrefix(mode, "cancel-") {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, 250*time.Millisecond)
			defer cancel()
			if mode == "cancel-before" {
				cancel()
			}
		}
		before := runtimeCallerDescriptorCount(t)
		payload, maximum := []byte("channel-request"), runtimechannel.MaximumPayload
		if strings.HasPrefix(mode, "large-") {
			payload, maximum = bytes.Repeat([]byte{'x'}, runtimechannel.MaximumRequestPayload), runtimechannel.MaximumRequestPayload
			if _, err := runtimechannel.Exchange(ctx, os.Getenv("VELA_CHANNEL_SOCKET"), payload); !errors.Is(err, runtimechannel.ErrIdentity) {
				t.Fatalf("default client accepted large request: %v", err)
			}
		}
		response, err := runtimechannel.ExchangeWithRequestLimit(ctx, os.Getenv("VELA_CHANNEL_SOCKET"), payload, maximum)
		if mode == "normal" || mode == "large-request" || mode == "large-response" {
			if err != nil || string(response) != "channel-response" {
				t.Fatalf("authenticated round trip failed: %q %v", response, err)
			}
		} else {
			if err == nil || response != nil {
				t.Fatalf("untrusted %s exchange accepted: %q %v", mode, response, err)
			}
			if strings.HasPrefix(mode, "challenge-") || strings.HasPrefix(mode, "response-") || strings.HasPrefix(mode, "peer-") {
				var networkError net.Error
				if !errors.Is(err, runtimechannel.ErrIdentity) || errors.As(err, &networkError) && networkError.Timeout() {
					t.Fatalf("%s missed its identity rejection: %v", mode, err)
				}
			}
			if strings.HasPrefix(mode, "cancel-") && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost its cause: %v", err)
			}
			t.Logf("rejected %s: %v", mode, err)
		}
		if after := runtimeCallerDescriptorCount(t); before != after {
			t.Fatalf("exchange leaked descriptors: before=%d after=%d", before, after)
		}
		t.Logf("Runtime pid=%d, private-pid=%t", os.Getpid(), privatePID)
	case "untrusted-listener":
		listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: os.Getenv("VELA_CHANNEL_SOCKET"), Net: "unixpacket"})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = listener.Close() }()
		ready := os.NewFile(3, "listener-ready")
		if _, err := ready.Write([]byte("ready")); err != nil {
			t.Fatal(err)
		}
		_ = ready.Close()
		connection, err := listener.AcceptUnix()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = connection.Close() }()
	case "delegate":
		file := os.NewFile(3, "delegated-Node-socket")
		connection, err := net.FileConn(file)
		_ = file.Close()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = connection.Close() }()
		packet, err := base64.StdEncoding.DecodeString(os.Getenv("VELA_CHANNEL_PACKET"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := connection.Write(packet); err != nil {
			t.Fatal(err)
		}
	default:
		t.Skip("Runtime channel subprocess helper")
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
}

func runtimeChannelUntrustedListener(t *testing.T, socket, mode string) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// The fixture allows the non-root process to create its socket, then restores
	// trusted path ownership before the client starts. Only kernel peer identity differs.
	if err := os.Chmod(filepath.Dir(socket), 0o777); err != nil {
		t.Fatal(err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close(); _ = writer.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, binary, "-test.run=^TestRuntimeChannelProcessHelper$")
	command.Env = []string{runtimeChannelHelperMode + "=untrusted-listener", "VELA_CHANNEL_SOCKET=" + socket}
	credentials := &syscall.Credential{Uid: 65532, Gid: 65532}
	if mode == "peer-gid" {
		credentials.Uid = 0
	}
	command.SysProcAttr = &syscall.SysProcAttr{Credential: credentials}
	command.ExtraFiles = []*os.File{writer}
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	t.Cleanup(func() {
		_ = input.Close()
		if err := command.Wait(); err != nil {
			t.Errorf("untrusted listener failed: %s %v", output.String(), err)
		}
	})
	if err := reader.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	ready, err := io.ReadAll(reader)
	if err != nil || string(ready) != "ready" {
		t.Fatalf("untrusted listener not ready: %q %v", ready, err)
	}
	if err := os.Chmod(filepath.Dir(socket), 0o755); err != nil {
		t.Fatal(err)
	}
}

func runtimeChannelNamespaceProbe(t *testing.T) {
	connection, err := net.DialUnix("unixpacket", nil, &net.UnixAddr{Name: os.Getenv("VELA_CHANNEL_SOCKET"), Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	if err := connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	raw, err := connection.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var peer *unix.Ucred
	peerFD := -1
	var setupErr error
	err = raw.Control(func(fd uintptr) {
		peer, setupErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if setupErr != nil {
			return
		}
		acquired, err := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PEERPIDFD)
		setupErr = err
		if err != nil {
			return
		}
		peerFD = acquired
		setupErr = errors.Join(unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSCRED, 1),
			unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSPIDFD, 1))
	})
	if peerFD >= 0 {
		defer func() { _ = unix.Close(peerFD) }()
	}
	if err != nil || setupErr != nil || peer == nil || *peer != (unix.Ucred{Pid: 0, Uid: 0, Gid: 0}) {
		t.Fatalf("Node not an invisible root peer: %+v %v %v", peer, err, setupErr)
	}
	if _, err := connection.Write([]byte("ready")); err != nil {
		t.Fatal(err)
	}
	identityComparable := runtimechannel.SameLiveProcess(peerFD, peerFD) == nil
	for _, expected := range []string{"original", "delegated"} {
		packet, sender, senderFD, err := runtimechannel.ReadPacket(connection, 16)
		if err != nil {
			t.Fatal(err)
		}
		identityErr := runtimechannel.SameLiveProcess(peerFD, senderFD)
		_ = unix.Close(senderFD)
		wantSame := expected == "original" && identityComparable
		if sender != *peer || string(packet) != expected || (identityErr == nil) != wantSame {
			t.Fatalf("PID 0 identity comparison failed: %q sender=%+v err=%v", packet, sender, identityErr)
		}
		if !identityComparable && !errors.Is(identityErr, runtimechannel.ErrPIDFDIdentityUnavailable) {
			t.Fatalf("legacy invisible pidfd returned an unrelated error: %v", identityErr)
		}
		t.Logf("%s sender: peer_pid=0 message_pid=0 uid=0 same_live_identity=%t comparable=%t", expected, identityErr == nil, identityComparable)
	}
}
