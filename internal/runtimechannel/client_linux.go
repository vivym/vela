package runtimechannel

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// Exchange authenticates a root Node peer at a root-owned 0660 socket whose
// group matches this non-root Runtime. All directories must be root-owned and
// non-writable by group/others, with no symlinks. Linux pidfs is mandatory.
// The reply is authenticated data, not a startup or retirement grant.
func Exchange(ctx context.Context, socketPath string, payload []byte) (result []byte, resultErr error) {
	return ExchangeWithRequestLimit(ctx, socketPath, payload, MaximumPayload)
}

// ExchangeWithRequestLimit opts into a bounded larger request, retaining the
// default reply limit and all original peer/challenge/socket checks.
func ExchangeWithRequestLimit(ctx context.Context, socketPath string, payload []byte, maximum int) (result []byte, resultErr error) {
	defer func() {
		if resultErr != nil {
			result = nil
		}
	}()
	if ctx == nil || maximum <= 0 || maximum > MaximumRequestPayload || len(payload) == 0 || len(payload) > maximum || os.Geteuid() == 0 || os.Getegid() == 0 {
		return nil, ErrIdentity
	}
	ctx, cancel := context.WithTimeout(ctx, ExchangeTimeout)
	defer cancel()
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	socket, err := openNodeSocket(socketPath)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, socket.close()) }()
	dialer := &net.Dialer{Timeout: HandshakeTimeout, Control: func(_, _ string, raw syscall.RawConn) error {
		var setupErr error
		err := raw.Control(func(fd uintptr) {
			setupErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSCRED, 1)
			if setupErr == nil {
				setupErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSPIDFD, 1)
			}
		})
		return errors.Join(err, setupErr)
	}}
	connected, err := dialer.DialContext(ctx, "unixpacket", socket.address())
	if err != nil {
		return nil, err
	}
	connection := connected.(*net.UnixConn)
	defer func() { resultErr = errors.Join(resultErr, connection.Close()) }()
	deadline, _ := ctx.Deadline()
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = connection.SetDeadline(time.Now())
		close(canceled)
	})
	defer func() {
		if !stop() {
			<-canceled
		}
		cause := context.Cause(ctx)
		// The socket timer can fire before the context timer at the same deadline.
		if cause == nil && !time.Now().Before(deadline) {
			cause = context.DeadlineExceeded
		}
		resultErr = errors.Join(resultErr, cause)
	}()
	if err := connection.SetDeadline(minTime(deadline, time.Now().Add(HandshakeTimeout))); err != nil {
		return nil, err
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	raw, err := connection.SyscallConn()
	if err != nil {
		return nil, err
	}
	var peer *unix.Ucred
	peerFD := -1
	var setupErr error
	err = raw.Control(func(fd uintptr) {
		peer, setupErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if setupErr != nil || peer.Uid != 0 || peer.Gid != 0 || peer.Pid < 0 {
			setupErr = errors.Join(ErrIdentity, setupErr)
			return
		}
		acquired, err := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PEERPIDFD)
		setupErr = err
		if err == nil {
			peerFD = acquired
		}
	})
	if peerFD >= 0 {
		defer func() { _ = unix.Close(peerFD) }()
	}
	if err := errors.Join(err, setupErr, socket.check()); err != nil {
		return nil, err
	}
	receive := func(maximum int) ([]byte, error) {
		packet, sender, senderFD, err := ReadPacket(connection, maximum)
		if err != nil {
			return nil, err
		}
		defer func() { _ = unix.Close(senderFD) }()
		if sender != *peer {
			return nil, ErrIdentity
		}
		if err := SameLiveProcess(peerFD, senderFD); err != nil {
			return nil, err
		}
		return packet, nil
	}
	challenge, err := receive(ChallengeSize)
	if err != nil || len(challenge) != ChallengeSize || !bytes.HasPrefix(challenge, []byte(Protocol)) {
		return nil, errors.Join(ErrIdentity, err)
	}
	if err := errors.Join(context.Cause(ctx), socket.check(), SameLiveProcess(peerFD, peerFD)); err != nil {
		return nil, err
	}
	packet := append(bytes.Clone(challenge), payload...)
	if count, err := connection.Write(packet); err != nil || count != len(packet) {
		return nil, errors.Join(errors.New("send Runtime request"), err)
	}
	if err := connection.SetDeadline(deadline); err != nil {
		return nil, err
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	responsePrefix := append([]byte(ResponseProtocol), challenge...)
	response, err := receive(len(responsePrefix) + MaximumPayload)
	if err != nil || len(response) <= len(responsePrefix) || !bytes.HasPrefix(response, responsePrefix) {
		return nil, errors.Join(ErrIdentity, err)
	}
	if err := errors.Join(context.Cause(ctx), socket.check(), SameLiveProcess(peerFD, peerFD)); err != nil {
		return nil, err
	}
	return bytes.Clone(response[len(responsePrefix):]), nil
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
