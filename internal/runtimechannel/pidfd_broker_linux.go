//go:build linux

package runtimechannel

import (
	"context"
	"crypto/rand"
	"errors"
	"net"
	"time"

	"golang.org/x/sys/unix"
)

const (
	pidFDBrokerProtocol = "vela-pidfd-broker-v1\x00"
	pidFDBrokerFrame    = len(pidFDBrokerProtocol) + 32
	pidFDBrokerReply    = pidFDBrokerFrame + 1
)

var (
	// ErrPIDFDBrokerUnavailable is returned when an invisible legacy pidfd
	// needs a host-side comparison but no trusted broker was configured.
	ErrPIDFDBrokerUnavailable = errors.New("trusted host-side pidfd identity broker is unavailable")
	ErrPIDFDBrokerProtocol    = errors.New("pidfd identity broker protocol is invalid")
)

// ComparePIDFDsWithBroker asks a root-owned broker to compare two already
// retained pidfds in the broker's PID namespace. The descriptors are sent as
// SCM_RIGHTS; no numeric PID is serialized or reopened. The broker must be
// published as a root-owned 0660 Unix seqpacket socket whose group is the
// caller's configured Runtime GID.
func ComparePIDFDsWithBroker(ctx context.Context, socketPath string, first, second int) error {
	if ctx == nil || socketPath == "" || first < 0 || second < 0 {
		return ErrPIDFDBrokerUnavailable
	}
	if err := ValidatePIDFD(first); err != nil {
		return err
	}
	if err := ValidatePIDFD(second); err != nil {
		return err
	}
	if err := errors.Join(PollLivePIDFD(first), PollLivePIDFD(second)); err != nil {
		return err
	}
	socket, err := openNodeSocket(socketPath)
	if err != nil {
		return errors.Join(ErrPIDFDBrokerUnavailable, err)
	}
	defer func() { _ = socket.close() }()
	connection, err := (&net.Dialer{Timeout: HandshakeTimeout}).DialContext(ctx, "unixpacket", socket.address())
	if err != nil {
		return errors.Join(ErrPIDFDBrokerUnavailable, err)
	}
	unixConnection, ok := connection.(*net.UnixConn)
	if !ok {
		_ = connection.Close()
		return ErrPIDFDBrokerProtocol
	}
	defer func() { _ = unixConnection.Close() }()
	if err := checkPIDFDBrokerPeer(unixConnection); err != nil {
		return err
	}
	deadline := time.Now().Add(HandshakeTimeout)
	if limit, ok := ctx.Deadline(); ok && limit.Before(deadline) {
		deadline = limit
	}
	if err := unixConnection.SetDeadline(deadline); err != nil {
		return err
	}
	frame := make([]byte, pidFDBrokerFrame)
	copy(frame, pidFDBrokerProtocol)
	if _, err := rand.Read(frame[len(pidFDBrokerProtocol):]); err != nil {
		return err
	}
	if err := sendPIDFDBrokerFrame(unixConnection, frame, []int{first, second}); err != nil {
		return err
	}
	reply, err := readPIDFDBrokerFrame(unixConnection, pidFDBrokerReply)
	if err != nil {
		return err
	}
	if len(reply) != pidFDBrokerReply || string(reply[:len(pidFDBrokerProtocol)]) != pidFDBrokerProtocol ||
		!bytesEqual(reply[len(pidFDBrokerProtocol):pidFDBrokerFrame], frame[len(pidFDBrokerProtocol):]) {
		return ErrPIDFDBrokerProtocol
	}
	if reply[len(reply)-1] != 1 {
		return ErrIdentity
	}
	return nil
}

// ServePIDFDBroker serves one-shot comparisons for a root-owned Node process.
// The accepted peer is restricted to the configured non-root Runtime GID.
// Each request owns exactly two pidfds and each connection is closed after one
// response, preventing descriptor or request state from being reused.
func ServePIDFDBroker(ctx context.Context, listener *net.UnixListener, runtimeGID uint32) error {
	if ctx == nil || listener == nil || listener.Addr().Network() != "unixpacket" || runtimeGID == 0 {
		return ErrPIDFDBrokerProtocol
	}
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()
	for {
		connection, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			return err
		}
		if err := servePIDFDBrokerConnection(ctx, connection, runtimeGID); err != nil {
			_ = connection.Close()
		}
	}
}

func servePIDFDBrokerConnection(ctx context.Context, connection *net.UnixConn, runtimeGID uint32) error {
	defer connection.Close()
	peer, err := pidFDBrokerPeer(connection)
	if err != nil || peer.Uid == 0 || peer.Gid != runtimeGID {
		return errors.Join(ErrIdentity, err)
	}
	deadline := time.Now().Add(HandshakeTimeout)
	if limit, ok := ctx.Deadline(); ok && limit.Before(deadline) {
		deadline = limit
	}
	if err := connection.SetDeadline(deadline); err != nil {
		return err
	}
	frame, rights, err := readPIDFDBrokerRequest(connection)
	if err != nil {
		return err
	}
	defer func() {
		for _, fd := range rights {
			_ = unix.Close(fd)
		}
	}()
	matched := false
	if err := SameLiveProcess(rights[0], rights[1]); err == nil {
		matched = true
	}
	reply := make([]byte, pidFDBrokerReply)
	copy(reply, pidFDBrokerProtocol)
	copy(reply[len(pidFDBrokerProtocol):pidFDBrokerFrame], frame[len(pidFDBrokerProtocol):])
	if matched {
		reply[len(reply)-1] = 1
	}
	return writePIDFDBrokerFrame(connection, reply)
}

func checkPIDFDBrokerPeer(connection *net.UnixConn) error {
	peer, err := pidFDBrokerPeer(connection)
	if err != nil || peer.Uid != 0 || peer.Gid != 0 {
		return errors.Join(ErrIdentity, err)
	}
	return nil
}

func pidFDBrokerPeer(connection *net.UnixConn) (*unix.Ucred, error) {
	raw, err := connection.SyscallConn()
	if err != nil {
		return nil, err
	}
	var peer *unix.Ucred
	var peerErr error
	if err := raw.Control(func(fd uintptr) { peer, peerErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil {
		return nil, err
	}
	return peer, peerErr
}

func sendPIDFDBrokerFrame(connection *net.UnixConn, frame []byte, rights []int) error {
	if connection == nil || len(frame) == 0 || len(rights) != 2 {
		return ErrPIDFDBrokerProtocol
	}
	raw, err := connection.SyscallConn()
	if err != nil {
		return err
	}
	var sendErr error
	var count int
	if err := raw.Write(func(fd uintptr) bool {
		for {
			count, sendErr = unix.SendmsgN(int(fd), frame, unix.UnixRights(rights...), nil, 0)
			if errors.Is(sendErr, unix.EINTR) {
				continue
			}
			return !errors.Is(sendErr, unix.EAGAIN) && !errors.Is(sendErr, unix.EWOULDBLOCK)
		}
	}); err != nil {
		return err
	}
	if sendErr != nil || count != len(frame) {
		return errors.Join(ErrPIDFDBrokerProtocol, sendErr)
	}
	return nil
}

func writePIDFDBrokerFrame(connection *net.UnixConn, frame []byte) error {
	count, err := connection.Write(frame)
	if err != nil || count != len(frame) {
		return errors.Join(ErrPIDFDBrokerProtocol, err)
	}
	return nil
}

func readPIDFDBrokerFrame(connection *net.UnixConn, maximum int) ([]byte, error) {
	frame := make([]byte, maximum)
	count, err := connection.Read(frame)
	if err != nil {
		return nil, err
	}
	if count != maximum {
		return nil, ErrPIDFDBrokerProtocol
	}
	return frame[:count], nil
}

func readPIDFDBrokerRequest(connection *net.UnixConn) ([]byte, []int, error) {
	raw, err := connection.SyscallConn()
	if err != nil {
		return nil, nil, err
	}
	frame := make([]byte, pidFDBrokerFrame)
	oob := make([]byte, unix.CmsgSpace(2*4)+unix.CmsgSpace(4))
	var count, oobCount, flags int
	var receiveErr error
	if err := raw.Read(func(fd uintptr) bool {
		for {
			count, oobCount, flags, _, receiveErr = unix.Recvmsg(int(fd), frame, oob, unix.MSG_CMSG_CLOEXEC)
			if errors.Is(receiveErr, unix.EINTR) {
				continue
			}
			// A connection can be accepted before its first packet arrives.
			// Let Go's poller wait for readability instead of closing the peer.
			return !errors.Is(receiveErr, unix.EAGAIN) && !errors.Is(receiveErr, unix.EWOULDBLOCK)
		}
	}); err != nil {
		return nil, nil, err
	}
	if receiveErr != nil || count != len(frame) || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 ||
		string(frame[:len(pidFDBrokerProtocol)]) != pidFDBrokerProtocol {
		return nil, nil, errors.Join(ErrPIDFDBrokerProtocol, receiveErr)
	}
	messages, err := unix.ParseSocketControlMessage(oob[:oobCount])
	if err != nil {
		return nil, nil, errors.Join(ErrPIDFDBrokerProtocol, err)
	}
	var rights []int
	for _, message := range messages {
		if message.Header.Level != unix.SOL_SOCKET || message.Header.Type != unix.SCM_RIGHTS || len(rights) != 0 {
			closePIDFDBrokerRights(rights)
			return nil, nil, ErrPIDFDBrokerProtocol
		}
		rights, err = unix.ParseUnixRights(&message)
		if err != nil {
			closePIDFDBrokerRights(rights)
			return nil, nil, errors.Join(ErrPIDFDBrokerProtocol, err)
		}
	}
	if len(rights) != 2 {
		closePIDFDBrokerRights(rights)
		return nil, nil, ErrPIDFDBrokerProtocol
	}
	for _, fd := range rights {
		if err := ValidatePIDFD(fd); err != nil {
			closePIDFDBrokerRights(rights)
			return nil, nil, err
		}
	}
	return frame, rights, nil
}

func closePIDFDBrokerRights(rights []int) {
	for _, fd := range rights {
		_ = unix.Close(fd)
	}
}

func bytesEqual(first, second []byte) bool {
	if len(first) != len(second) {
		return false
	}
	for index := range first {
		if first[index] != second[index] {
			return false
		}
	}
	return true
}
