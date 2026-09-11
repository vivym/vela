// Package runtimechannel authenticates local Node/Runtime messages. It grants
// no authority to load a backend, retire an incarnation or release a device.
package runtimechannel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"

	"golang.org/x/sys/unix"
)

const (
	Protocol         = "vela-runtime-caller-v1\x00"
	ResponseProtocol = "vela-runtime-response-v1\x00"
	ChallengeSize    = len(Protocol) + 32
	MaximumPayload   = 32 << 10
	// Larger requests require explicit opt-in at both endpoints. Replies and
	// ordinary startup exchanges retain the original 32 KiB limit.
	MaximumRequestPayload = 192 << 10
	HandshakeTimeout      = 5 * time.Second
	ExchangeTimeout       = 45 * time.Second
)

var ErrIdentity = errors.New("runtime channel kernel identity or frame is untrusted")

// ParseAncillary owns all received descriptors. Only one validated kernel
// message pidfd escapes; rejected SCM_RIGHTS and duplicate pidfds are closed.
// PID zero is legitimate when the sender is outside the receiver's namespace.
func ParseAncillary(data []byte) (unix.Ucred, int, error) {
	credential, pidfd, _, err := parseAncillary(data, 0)
	return credential, pidfd, err
}

func parseAncillary(data []byte, expectedRights int) (unix.Ucred, int, []int, error) {
	messages, err := unix.ParseSocketControlMessage(data)
	if err != nil {
		return unix.Ucred{}, -1, nil, err
	}
	var credential unix.Ucred
	var descriptors, rights []int
	rightsMessages := 0
	credentialCount, pidfdCount, pidfd := 0, 0, -1
	for _, message := range messages {
		switch {
		case message.Header.Level == unix.SOL_SOCKET && message.Header.Type == unix.SCM_CREDENTIALS:
			value, parseErr := unix.ParseUnixCredentials(&message)
			err = errors.Join(err, parseErr)
			if parseErr == nil {
				credential = *value
				credentialCount++
			}
		case message.Header.Level == unix.SOL_SOCKET && message.Header.Type == unix.SCM_PIDFD && len(message.Data) == 4:
			pidfd = int(int32(binary.NativeEndian.Uint32(message.Data)))
			descriptors = append(descriptors, pidfd)
			pidfdCount++
		case message.Header.Level == unix.SOL_SOCKET && message.Header.Type == unix.SCM_RIGHTS:
			values, parseErr := unix.ParseUnixRights(&message)
			descriptors = append(descriptors, values...)
			rights = append(rights, values...)
			rightsMessages++
			err = errors.Join(err, parseErr)
		default:
			err = errors.Join(err, ErrIdentity)
		}
	}
	if credentialCount != 1 || pidfdCount != 1 || credential.Pid < 0 || pidfd < 0 || len(rights) != expectedRights || rightsMessages != expectedRights {
		err = errors.Join(err, ErrIdentity)
	}
	if err != nil {
		for _, fd := range descriptors {
			_ = unix.Close(fd)
		}
		return unix.Ucred{}, -1, nil, err
	}
	return credential, pidfd, rights, nil
}

// ReadPacket closes ancillary descriptors even on truncation or read failure.
// On success the caller owns the returned message pidfd.
func ReadPacket(connection *net.UnixConn, maximum int) ([]byte, unix.Ucred, int, error) {
	packet, peer, pidfd, _, err := readPacket(connection, maximum, 0)
	return packet, peer, pidfd, err
}

// ReadProcessOffer additionally receives exactly one SCM_RIGHTS pidfd. The
// sender and offered process handles are distinct and owned by the recipient.
// This does not authenticate their relationship or the sender's authority.
func ReadProcessOffer(connection *net.UnixConn, maximum int) ([]byte, unix.Ucred, int, int, error) {
	packet, peer, sender, rights, err := readPacket(connection, maximum, 1)
	if err != nil {
		return nil, unix.Ucred{}, -1, -1, err
	}
	target := rights[0]
	if err := SameLiveProcess(target, target); err != nil {
		_ = unix.Close(sender)
		_ = unix.Close(target)
		return nil, unix.Ucred{}, -1, -1, err
	}
	return packet, peer, sender, target, nil
}

func readPacket(connection *net.UnixConn, maximum, expectedRights int) ([]byte, unix.Ucred, int, []int, error) {
	if connection == nil || maximum <= 0 || maximum > len(ResponseProtocol)+ChallengeSize+MaximumPayload {
		return nil, unix.Ucred{}, -1, nil, ErrIdentity
	}
	raw, err := connection.SyscallConn()
	if err != nil {
		return nil, unix.Ucred{}, -1, nil, err
	}
	packet, ancillary := make([]byte, maximum), make([]byte, 1024)
	var count, ancillaryCount, flags int
	var receiveErr error
	err = raw.Read(func(fd uintptr) bool {
		count, ancillaryCount, flags, _, receiveErr = unix.Recvmsg(int(fd), packet, ancillary, unix.MSG_CMSG_CLOEXEC)
		return !errors.Is(receiveErr, unix.EAGAIN) && !errors.Is(receiveErr, unix.EWOULDBLOCK)
	})
	peer, pidfd, rights, parseErr := parseAncillary(ancillary[:ancillaryCount], expectedRights)
	err = errors.Join(err, receiveErr, parseErr)
	if count <= 0 || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		err = errors.Join(err, ErrIdentity)
	}
	if err != nil {
		if pidfd >= 0 {
			_ = unix.Close(pidfd)
		}
		for _, fd := range rights {
			_ = unix.Close(fd)
		}
		return nil, unix.Ucred{}, -1, nil, err
	}
	return packet[:count], peer, pidfd, rights, nil
}

// SameLiveProcess compares retained pidfs identities, including invisible
// ancestor processes whose namespace-relative credentials both report PID 0.
// Older anonymous-inode pidfds cannot establish this equality and fail closed.
func SameLiveProcess(original, message int) error {
	var identity [2]unix.Stat_t
	for i, fd := range []int{original, message} {
		if fd < 0 {
			return ErrIdentity
		}
		var filesystem unix.Statfs_t
		if err := unix.Fstatfs(fd, &filesystem); err != nil {
			return errors.Join(ErrIdentity, err)
		}
		if filesystem.Type != unix.PID_FS_MAGIC {
			return fmt.Errorf("%w: pidfd filesystem type %#x; requires pidfs (%#x)", ErrIdentity, filesystem.Type, unix.PID_FS_MAGIC)
		}
		if err := unix.Fstat(fd, &identity[i]); err != nil || identity[i].Ino == 0 {
			return errors.Join(ErrIdentity, err)
		}
		if err := PollLivePIDFD(fd); err != nil {
			return err
		}
		if flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil || flags&unix.FD_CLOEXEC == 0 {
			return errors.Join(ErrIdentity, err)
		}
	}
	if identity[0].Dev != identity[1].Dev || identity[0].Ino != identity[1].Ino {
		return ErrIdentity
	}
	return nil
}
