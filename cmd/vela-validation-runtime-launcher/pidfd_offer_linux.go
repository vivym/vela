//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	"github.com/vivym/vela/internal/runtimechannel"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/metadata"
)

const pidfdOfferFrame = "vela-validation-pidfd-v1"

// The wrapper's self-opened pidfd is sampled process identity, not independent
// launch authorization. The receiver binds it to both kernel sender handles and
// this invocation's CRI task before retaining it. No host numeric PID is opened.
type pidfdOfferListener struct {
	listener      *net.UnixListener
	directory     string
	hostPath      string
	containerPath string
	identity      os.FileInfo
	uid, gid      uint32
}

func newPIDFDOfferListener(root, name string, uid, gid uint32) (*pidfdOfferListener, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || (name != "runtime" && name != "worker") || uid == 0 || gid == 0 {
		return nil, errors.New("pidfd offer listener configuration is invalid")
	}
	directory := filepath.Join(root, name+"-offer")
	if err := os.Mkdir(directory, 0o750); err != nil {
		return nil, err
	}
	if err := os.Chown(directory, 0, int(gid)); err != nil {
		return nil, err
	}
	hostPath := filepath.Join(directory, "pidfd.sock")
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: hostPath, Net: "unixpacket"})
	if err != nil {
		return nil, fmt.Errorf("listen for %s pidfd: %w", name, err)
	}
	failed := true
	defer func() {
		if failed {
			_ = listener.Close()
		}
	}()
	raw, err := listener.SyscallConn()
	if err != nil {
		return nil, err
	}
	var setupErr error
	if err := raw.Control(func(fd uintptr) {
		setupErr = errors.Join(unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSCRED, 1),
			unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSPIDFD, 1))
	}); err != nil || setupErr != nil {
		return nil, errors.Join(err, setupErr)
	}
	if err := os.Chown(hostPath, 0, int(gid)); err != nil {
		return nil, err
	}
	if err := os.Chmod(hostPath, 0o660); err != nil {
		return nil, err
	}
	identity, err := os.Lstat(hostPath)
	if err != nil {
		return nil, err
	}
	listener.SetUnlinkOnClose(false)
	failed = false
	return &pidfdOfferListener{listener: listener, directory: directory, hostPath: hostPath,
		containerPath: "/vela-validation/offer/pidfd.sock", identity: identity, uid: uid, gid: gid}, nil
}

func (offer *pidfdOfferListener) accept(ctx context.Context, tasks tasksapi.TasksClient, id string) (*os.File, error) {
	if offer == nil || offer.listener == nil || ctx == nil || tasks == nil || id == "" {
		return nil, errors.New("pidfd offer listener or task is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	if err := offer.listener.SetDeadline(deadline); err != nil {
		return nil, err
	}
	stopAccept := context.AfterFunc(ctx, func() { _ = offer.listener.SetDeadline(time.Now()) })
	defer stopAccept()
	connection, err := offer.listener.AcceptUnix()
	if err != nil {
		return nil, errors.Join(err, context.Cause(ctx))
	}
	defer connection.Close()
	if err := connection.SetDeadline(deadline); err != nil {
		return nil, err
	}
	stopRead := context.AfterFunc(ctx, func() { _ = connection.SetDeadline(time.Now()) })
	defer stopRead()
	return offer.receive(ctx, connection, tasks, id)
}

func (offer *pidfdOfferListener) receive(ctx context.Context, connection *net.UnixConn, tasks tasksapi.TasksClient, id string) (*os.File, error) {
	raw, err := connection.SyscallConn()
	if err != nil {
		return nil, err
	}
	peerFD := -1
	var peer *unix.Ucred
	var peerErr error
	err = raw.Control(func(fd uintptr) {
		peer, peerErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if peerErr == nil {
			peerFD, peerErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PEERPIDFD)
		}
	})
	if peerFD >= 0 {
		defer unix.Close(peerFD)
	}
	if err != nil || peerErr != nil || peer == nil || peer.Uid != offer.uid || peer.Gid != offer.gid || peer.Pid <= 0 {
		return nil, errors.Join(runtimechannel.ErrIdentity, err, peerErr)
	}
	packet, sender, senderFD, offeredFD, err := runtimechannel.ReadProcessOffer(connection, len(pidfdOfferFrame))
	if err != nil {
		return nil, errors.Join(err, context.Cause(ctx))
	}
	defer unix.Close(senderFD)
	file := os.NewFile(uintptr(offeredFD), "validation-self-offered-pidfd")
	failed := true
	defer func() {
		if failed {
			_ = file.Close()
		}
	}()
	if !bytes.Equal(packet, []byte(pidfdOfferFrame)) || sender != *peer {
		return nil, runtimechannel.ErrIdentity
	}
	if err := errors.Join(runtimechannel.SameLiveProcess(peerFD, senderFD), runtimechannel.SameLiveProcess(senderFD, offeredFD)); err != nil {
		return nil, err
	}
	header, _ := metadata.FromOutgoingContext(ctx)
	header = header.Copy()
	header.Set("containerd-namespace", "k8s.io")
	status, err := tasks.Get(metadata.NewOutgoingContext(ctx, header), &tasksapi.GetRequest{ContainerID: id})
	if err != nil {
		return nil, fmt.Errorf("bind pidfd offer to CRI task: %w", err)
	}
	// A numeric PID is used only to correlate authenticated kernel metadata to
	// the task API while the independently retained process is still live.
	if status.GetProcess().GetPid() != uint32(sender.Pid) || status.GetProcess().GetStatus().String() != "RUNNING" {
		return nil, errors.New("pidfd offer does not match this invocation's running CRI task")
	}
	if err := errors.Join(context.Cause(ctx), runtimechannel.SameLiveProcess(peerFD, offeredFD)); err != nil {
		return nil, err
	}
	failed = false
	return file, nil
}

func (offer *pidfdOfferListener) Close() error {
	if offer == nil || offer.listener == nil {
		return nil
	}
	err := offer.listener.Close()
	if errors.Is(err, net.ErrClosed) {
		err = nil
	}
	named, inspectErr := os.Lstat(offer.hostPath)
	if errors.Is(inspectErr, os.ErrNotExist) {
		return err
	}
	if inspectErr != nil || offer.identity == nil || !os.SameFile(named, offer.identity) {
		return errors.Join(err, inspectErr, errors.New("pidfd offer socket was replaced"))
	}
	return errors.Join(err, os.Remove(offer.hostPath))
}

func runPIDFDOffer(args []string) error {
	if len(args) < 4 || args[0] != "--socket" || args[2] != "--" {
		return errors.New("pidfd offer requires --socket PATH -- COMMAND [ARGS]")
	}
	if !filepath.IsAbs(args[1]) || filepath.Clean(args[1]) != args[1] || !filepath.IsAbs(args[3]) || filepath.Clean(args[3]) != args[3] {
		return errors.New("pidfd offer socket or command path is invalid")
	}
	fd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		return fmt.Errorf("open self pidfd before exec: %w", err)
	}
	defer unix.Close(fd)
	connection, err := net.DialTimeout("unixpacket", args[1], 15*time.Second)
	if err != nil {
		return fmt.Errorf("connect pidfd offer socket: %w", err)
	}
	defer connection.Close()
	if err := connection.SetWriteDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return err
	}
	n, _, err := connection.(*net.UnixConn).WriteMsgUnix([]byte(pidfdOfferFrame), unix.UnixRights(fd), nil)
	if err != nil || n != len(pidfdOfferFrame) {
		return errors.Join(errors.New("send self pidfd before exec"), err)
	}
	if err := connection.Close(); err != nil {
		return err
	}
	environment := make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "VELA_VALIDATION_PIDFD_OFFER=") {
			environment = append(environment, entry)
		}
	}
	return unix.Exec(args[3], args[3:], environment)
}
