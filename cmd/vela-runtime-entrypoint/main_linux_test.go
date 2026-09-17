//go:build linux

package main

import (
	"bytes"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/vivym/vela/internal/runtimechannel"
	"github.com/vivym/vela/internal/runtimelaunch"
	"golang.org/x/sys/unix"
)

func TestEntrypointTransfersOriginalHandleBeforeApprovedExec(t *testing.T) {
	if os.Getenv("VELA_ENTRYPOINT_TEST_CHILD") == "1" {
		err := runWithExec([]string{"runtime"}, func(path string, argv, env []string) error {
			if path != runtimelaunch.Runtime || !reflect.DeepEqual(argv, runtimelaunch.RuntimeArguments()) || !reflect.DeepEqual(env, os.Environ()) {
				return errors.New("unapproved exec vectors")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root to run the real non-root original-pidfd process")
	}
	if err := os.Mkdir(runtimelaunch.OfferRoot, 0o755); err != nil {
		t.Fatal("exclusive test mount point unavailable", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(runtimelaunch.OfferRoot) })
	root := filepath.Join(runtimelaunch.OfferRoot, "runtime-offer")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "pidfd.sock")
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(listener.Close)
	if err := os.Chown(path, 0, 10001); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o660); err != nil {
		t.Fatal(err)
	}
	raw, err := listener.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var setup error
	if err := raw.Control(func(fd uintptr) {
		setup = errors.Join(unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSCRED, 1), unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSPIDFD, 1))
	}); err != nil || setup != nil {
		t.Fatal(err, setup)
	}
	if err := listener.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), binary, "-test.run=^TestEntrypointTransfersOriginalHandleBeforeApprovedExec$")
	command.Env = append(os.Environ(), "VELA_ENTRYPOINT_TEST_CHILD=1")
	pidfd := -1
	command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: 10001, Gid: 10001}, PidFD: &pidfd}
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func(fd int) { _ = unix.Close(fd) }(pidfd)
	t.Cleanup(func() { _ = command.Process.Kill() })
	connection, err := listener.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(connection.Close)
	packet, sender, senderFD, offeredFD, err := runtimechannel.ReadProcessOffer(connection, len("vela-runtime-pidfd-v1"))
	if err != nil {
		t.Fatal(err)
	}
	defer func(fd int) { _ = unix.Close(fd) }(senderFD)
	defer func(fd int) { _ = unix.Close(fd) }(offeredFD)
	if string(packet) != "vela-runtime-pidfd-v1" || sender.Uid != 10001 || sender.Gid != 10001 {
		t.Fatal("wrong process handoff")
	}
	if err := errors.Join(runtimechannel.SameLiveProcess(pidfd, senderFD), runtimechannel.SameLiveProcess(senderFD, offeredFD)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		t.Fatalf("exec happened before release: %v %s", err, output.String())
	case <-time.After(30 * time.Millisecond):
	}
	if err := unix.PidfdSendSignal(offeredFD, unix.SIGCONT, nil, 0); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("entrypoint: %v %s", err, output.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("release was lost")
	}
}
