// Package runtimechannel authenticates local Node/Runtime messages. It grants
// no authority to load a backend, retire an incarnation or release a device.
package runtimechannel

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
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

// ErrPIDFDIdentityUnavailable means the descriptor is a real pidfd but the
// current procfs view cannot expose a comparable process identity. Linux
// 6.8's anonymous-inode pidfds report Pid/NSpid as zero for invisible
// processes in a nested PID namespace. Callers must use a trusted host-side
// identity check in that case; numeric-PID equality is never a fallback.
var ErrPIDFDIdentityUnavailable = errors.New("pidfd process identity is not visible in this pid namespace")

// PIDFDIdentityClass describes the identity evidence available from the
// current procfs view.
type PIDFDIdentityClass uint8

const (
	PIDFDIdentityPIDFS PIDFDIdentityClass = iota + 1
	PIDFDIdentityLegacyVisible
	PIDFDIdentityLegacyInvisible
)

// ClassifyPIDFD reports the identity evidence available from the current
// procfs view without requiring the process to remain live. Callers that need
// liveness must additionally call PollLivePIDFD. A successful
// PIDFDIdentityLegacyInvisible result is deliberately not sufficient for
// SameLiveProcess; it only says that the descriptor itself is a valid pidfd.
func ClassifyPIDFD(fd int) (PIDFDIdentityClass, error) {
	if fd < 0 {
		return 0, ErrIdentity
	}
	var filesystem unix.Statfs_t
	if err := unix.Fstatfs(fd, &filesystem); err != nil {
		return 0, errors.Join(ErrIdentity, err)
	}
	if filesystem.Type == unix.PID_FS_MAGIC {
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil || stat.Ino == 0 {
			return 0, errors.Join(ErrIdentity, err)
		}
		return PIDFDIdentityPIDFS, nil
	}
	legacy, err := readAnonymousPIDFDIdentity(fd)
	if err != nil {
		return 0, fmt.Errorf("%w: pidfd filesystem type %#x; legacy fdinfo identity unavailable: %v", ErrIdentity, filesystem.Type, err)
	}
	if legacy.visible {
		return PIDFDIdentityLegacyVisible, nil
	}
	return PIDFDIdentityLegacyInvisible, nil
}

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

// SameLiveProcess compares retained pidfd identities. Kernels before pidfs
// expose pidfds as anonymous inodes; those handles are accepted only when
// their kernel fdinfo Pid/NSpid identity is visible and matches. If the
// current procfs view reports zero for an invisible process, the descriptor is
// still structurally valid but this function returns
// ErrPIDFDIdentityUnavailable. No PID is used to reacquire a process or to
// construct a handle.
func SameLiveProcess(original, message int) error {
	type identity struct {
		device, inode uint64
		pid, nspid    string
		pidfs         bool
		visible       bool
	}
	var identities [2]identity
	for i, fd := range []int{original, message} {
		if fd < 0 {
			return ErrIdentity
		}
		var filesystem unix.Statfs_t
		if err := unix.Fstatfs(fd, &filesystem); err != nil {
			return errors.Join(ErrIdentity, err)
		}
		if filesystem.Type == unix.PID_FS_MAGIC {
			var stat unix.Stat_t
			if err := unix.Fstat(fd, &stat); err != nil || stat.Ino == 0 {
				return errors.Join(ErrIdentity, err)
			}
			identities[i] = identity{device: stat.Dev, inode: stat.Ino, pidfs: true, visible: true}
		} else {
			legacy, err := readAnonymousPIDFDIdentity(fd)
			if err != nil {
				return fmt.Errorf("%w: pidfd filesystem type %#x; legacy fdinfo identity unavailable: %v", ErrIdentity, filesystem.Type, err)
			}
			identities[i] = identity{pid: legacy.pid, nspid: legacy.nspid, visible: legacy.visible}
		}
		if err := ValidatePIDFD(fd); err != nil {
			return err
		}
		if err := PollLivePIDFD(fd); err != nil {
			return err
		}
		if flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil || flags&unix.FD_CLOEXEC == 0 {
			return errors.Join(ErrIdentity, err)
		}
	}
	if !identities[0].visible || !identities[1].visible {
		return errors.Join(ErrIdentity, ErrPIDFDIdentityUnavailable)
	}
	if identities[0].pidfs != identities[1].pidfs {
		return ErrIdentity
	}
	if identities[0].pidfs && (identities[0].device != identities[1].device || identities[0].inode != identities[1].inode) {
		return ErrIdentity
	}
	if !identities[0].pidfs && (identities[0].pid != identities[1].pid || identities[0].nspid != identities[1].nspid) {
		return ErrIdentity
	}
	return nil
}

// ValidatePIDFD verifies that fd is a kernel pidfd and has close-on-exec set.
// It deliberately does not poll for liveness, so callers that need to observe
// an exit can validate the retained handle first and poll it separately. A
// legacy pidfd whose fdinfo reports zero is structurally valid; comparing it
// with another process must handle ErrPIDFDIdentityUnavailable from
// SameLiveProcess and use a trusted host-side identity check.
func ValidatePIDFD(fd int) error {
	if fd < 0 {
		return ErrIdentity
	}
	var filesystem unix.Statfs_t
	if err := unix.Fstatfs(fd, &filesystem); err != nil {
		return errors.Join(ErrIdentity, err)
	}
	if filesystem.Type == unix.PID_FS_MAGIC {
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil || stat.Ino == 0 {
			return errors.Join(ErrIdentity, err)
		}
	} else if _, err := readAnonymousPIDFDIdentity(fd); err != nil {
		return fmt.Errorf("%w: pidfd filesystem type %#x; legacy fdinfo identity unavailable: %v", ErrIdentity, filesystem.Type, err)
	}
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		return errors.Join(ErrIdentity, err)
	}
	return nil
}

type anonymousPIDFDIdentity struct {
	pid, nspid string
	visible    bool
}

func readAnonymousPIDFDIdentity(fd int) (anonymousPIDFDIdentity, error) {
	link, err := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd))
	if err != nil || link != "anon_inode:[pidfd]" {
		return anonymousPIDFDIdentity{}, errors.New("descriptor is not an anonymous-inode pidfd")
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/self/fdinfo/%d", fd))
	if err != nil {
		return anonymousPIDFDIdentity{}, err
	}
	var result anonymousPIDFDIdentity
	nspidVisible := true
	for line := range strings.SplitSeq(string(data), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		switch key {
		case "Pid":
			if !validAnonymousPIDFDNumber(value) {
				return anonymousPIDFDIdentity{}, errors.New("fdinfo Pid is invalid")
			}
			result.pid = value
			if value == "0" {
				nspidVisible = false
			}
		case "NSpid":
			fields := strings.Fields(value)
			if len(fields) == 0 {
				return anonymousPIDFDIdentity{}, errors.New("fdinfo NSpid is empty")
			}
			for _, field := range fields {
				if !validAnonymousPIDFDNumber(field) {
					return anonymousPIDFDIdentity{}, errors.New("fdinfo NSpid is invalid")
				}
				if field == "0" {
					nspidVisible = false
				}
			}
			result.nspid = strings.Join(fields, ",")
		}
	}
	if result.pid == "" || result.nspid == "" {
		return anonymousPIDFDIdentity{}, errors.New("fdinfo lacks Pid/NSpid")
	}
	result.visible = nspidVisible
	return result, nil
}

func validAnonymousPIDFDNumber(value string) bool {
	if value == "-1" {
		return true
	}
	_, err := strconv.ParseUint(value, 10, 32)
	return err == nil
}
