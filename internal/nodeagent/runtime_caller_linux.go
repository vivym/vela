package nodeagent

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/runtimechannel"
	"golang.org/x/sys/unix"
)

const (
	runtimeCallerProtocol = runtimechannel.Protocol
	runtimeCallerMaximum  = runtimechannel.MaximumPayload
	runtimeCallerTimeout  = runtimechannel.HandshakeTimeout
)

var ErrRuntimeCallerIdentity = errors.New("runtime caller kernel identity is untrusted or no longer live")

// RuntimeCallerCredentials must be resolved from trusted Node configuration,
// never from the request. This channel supports non-root Runtime processes.
type RuntimeCallerCredentials struct{ UID, GID uint32 }

// RuntimeCallerObservation describes one live process. It does not authenticate
// its container, executable, launch, journal, device or Registry authority.
type RuntimeCallerObservation struct {
	BootID         uuid.UUID `json:"boot_id"`
	HostPID        int32     `json:"host_pid"`
	UID            uint32    `json:"uid"`
	GID            uint32    `json:"gid"`
	StartTicks     uint64    `json:"start_ticks"`
	NamespacePID   int32     `json:"namespace_pid"`
	PIDNamespace   string    `json:"pid_namespace"`
	NamespaceDepth int       `json:"namespace_depth"`
	ObservedAt     time.Time `json:"observed_at"`
}

// RuntimeCaller retains the kernel's original process handle. It cannot be
// constructed from a caller-supplied PID or reconstructed from its observation.
type RuntimeCaller struct {
	mu            sync.Mutex
	pidfd         *os.File
	process       *os.Root
	peer          unix.Ucred
	boot          uuid.UUID
	payload       []byte
	connection    *net.UnixConn
	challenge     []byte
	replyDeadline time.Time
	replied       bool
}

// ReceiveRuntimeCaller authenticates one challenge-bound Unix seqpacket from
// the connection opener, using both connection and per-message kernel pidfds.
// Linux SO_PEERPIDFD and SO_PASSPIDFD support is mandatory; there is no PID-only
// fallback. The caller owns connection and must not use it concurrently here.
// Its deadlines are changed for this exchange and cleared before returning.
func ReceiveRuntimeCaller(ctx context.Context, connection *net.UnixConn, expected RuntimeCallerCredentials) (result *RuntimeCaller, resultErr error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if connection == nil || expected.UID == 0 || expected.GID == 0 || expected.UID == ^uint32(0) || expected.GID == ^uint32(0) {
		return nil, ErrRuntimeCallerIdentity
	}
	deadline := time.Now().Add(runtimeCallerTimeout)
	if limit, ok := ctx.Deadline(); ok && limit.Before(deadline) {
		deadline = limit
	}
	if err := connection.SetDeadline(deadline); err != nil {
		return nil, err
	}
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = connection.SetDeadline(time.Now())
		close(canceled)
	})
	defer func() {
		if !stop() {
			<-canceled
		}
		resultErr = errors.Join(resultErr, connection.SetDeadline(time.Time{}))
		if resultErr != nil && result != nil {
			resultErr = errors.Join(resultErr, result.Close())
			result = nil
		}
	}()
	raw, err := connection.SyscallConn()
	if err != nil {
		return nil, err
	}
	var peer *unix.Ucred
	peerFD := -1
	var setupErr error
	controlErr := raw.Control(func(fd uintptr) {
		kind, err := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TYPE)
		if err != nil || kind != unix.SOCK_SEQPACKET {
			setupErr = errors.Join(errors.New("runtime caller requires a Unix seqpacket socket"), err)
			return
		}
		peer, setupErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if setupErr != nil || peer.Pid <= 0 || peer.Uid != expected.UID || peer.Gid != expected.GID {
			setupErr = errors.Join(ErrRuntimeCallerIdentity, setupErr)
			return
		}
		acquired, err := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PEERPIDFD)
		setupErr = err
		if setupErr == nil {
			peerFD = acquired
			flags, err := unix.FcntlInt(uintptr(peerFD), unix.F_GETFD, 0)
			if err != nil || flags&unix.FD_CLOEXEC == 0 {
				setupErr = errors.Join(ErrRuntimeCallerIdentity, err)
			}
		}
		if setupErr == nil {
			setupErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSCRED, 1)
		}
		if setupErr == nil {
			setupErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSPIDFD, 1)
		}
	})
	if peerFD >= 0 {
		defer func() {
			if peerFD >= 0 {
				_ = unix.Close(peerFD)
			}
		}()
	}
	if err := errors.Join(controlErr, setupErr); err != nil {
		return nil, fmt.Errorf("pin Runtime connection opener: %w", err)
	}
	if err := checkRuntimePIDFD(peerFD, peer.Pid); err != nil {
		return nil, err
	}
	challenge := make([]byte, len(runtimeCallerProtocol)+32)
	copy(challenge, runtimeCallerProtocol)
	if _, err := rand.Read(challenge[len(runtimeCallerProtocol):]); err != nil {
		return nil, err
	}
	if count, err := connection.Write(challenge); err != nil || count != len(challenge) {
		return nil, errors.Join(errors.New("send Runtime caller challenge"), err)
	}
	packet, ancillary := make([]byte, len(challenge)+runtimeCallerMaximum), make([]byte, 1024)
	var count, ancillaryCount, flags int
	var receiveErr error
	err = raw.Read(func(fd uintptr) bool {
		count, ancillaryCount, flags, _, receiveErr = unix.Recvmsg(int(fd), packet, ancillary, unix.MSG_CMSG_CLOEXEC)
		return !errors.Is(receiveErr, unix.EAGAIN) && !errors.Is(receiveErr, unix.EWOULDBLOCK)
	})
	messagePeer, messageFD, ancillaryErr := runtimechannel.ParseAncillary(ancillary[:ancillaryCount])
	if messageFD >= 0 {
		defer func() { _ = unix.Close(messageFD) }()
	}
	if err := errors.Join(err, receiveErr, context.Cause(ctx)); err != nil {
		return nil, err
	}
	if ancillaryErr != nil {
		return nil, errors.Join(ErrRuntimeCallerIdentity, ancillaryErr)
	}
	if flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 || count <= len(challenge) ||
		!bytes.Equal(packet[:min(count, len(challenge))], challenge) || messagePeer != *peer {
		return nil, ErrRuntimeCallerIdentity
	}
	if err := errors.Join(checkRuntimePIDFD(peerFD, peer.Pid), checkRuntimePIDFD(messageFD, peer.Pid)); err != nil {
		return nil, err
	}
	boot, err := readBootID("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return nil, err
	}
	process, err := os.OpenRoot(fmt.Sprintf("/proc/%d", peer.Pid))
	if err != nil {
		return nil, err
	}
	caller := &RuntimeCaller{pidfd: os.NewFile(uintptr(peerFD), "runtime-caller-pidfd"), process: process,
		peer: *peer, boot: uuid.MustParse(boot), payload: slices.Clone(packet[len(challenge):count]),
		connection: connection, challenge: challenge, replyDeadline: time.Now().Add(runtimechannel.ExchangeTimeout)}
	peerFD = -1
	if _, err := caller.Inspect(ctx); err != nil {
		_ = caller.Close()
		return nil, err
	}
	return caller, nil
}

// Payload is authenticated as sent by the pinned process, not as authorized
// Registry or launch content. The returned copy still requires domain validation.
func (caller *RuntimeCaller) Payload() []byte {
	if caller == nil {
		return nil
	}
	caller.mu.Lock()
	defer caller.mu.Unlock()
	if caller.pidfd == nil {
		return nil
	}
	return slices.Clone(caller.payload)
}

// Reply attempts at most one challenge-bound response on the accepted socket.
// The owner must retain that socket and avoid concurrent I/O until this returns.
// Successful transmission does not prove receipt or grant startup authority.
func (caller *RuntimeCaller) Reply(ctx context.Context, payload []byte) (resultErr error) {
	if err := contextError(ctx); err != nil {
		return err
	}
	if caller == nil || len(payload) == 0 || len(payload) > runtimeCallerMaximum {
		return ErrRuntimeCallerIdentity
	}
	caller.mu.Lock()
	defer caller.mu.Unlock()
	if caller.pidfd == nil || caller.connection == nil || caller.replied {
		return ErrRuntimeCallerIdentity
	}
	caller.replied = true
	if _, err := caller.inspectLocked(ctx); err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(ctx, caller.replyDeadline)
	defer cancel()
	deadline, _ := ctx.Deadline()
	if err := caller.connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = caller.connection.SetWriteDeadline(time.Now())
		close(canceled)
	})
	defer func() {
		if !stop() {
			<-canceled
		}
		cause := context.Cause(ctx)
		if cause == nil && !time.Now().Before(deadline) {
			cause = context.DeadlineExceeded
		}
		resultErr = errors.Join(resultErr, cause, caller.connection.SetWriteDeadline(time.Time{}))
	}()
	if err := context.Cause(ctx); err != nil {
		return err
	}
	packet := append([]byte(runtimechannel.ResponseProtocol), caller.challenge...)
	packet = append(packet, payload...)
	if count, err := caller.connection.Write(packet); err != nil || count != len(packet) {
		return errors.Join(errors.New("send Runtime caller response"), err)
	}
	return errors.Join(context.Cause(ctx), checkRuntimePIDFD(int(caller.pidfd.Fd()), caller.peer.Pid))
}

func (caller *RuntimeCaller) Close() error {
	if caller == nil {
		return nil
	}
	caller.mu.Lock()
	defer caller.mu.Unlock()
	if caller.pidfd == nil {
		return nil
	}
	err := errors.Join(caller.pidfd.Close(), caller.process.Close())
	caller.pidfd, caller.process, caller.payload = nil, nil, nil
	caller.connection, caller.challenge = nil, nil
	return err
}

func (caller *RuntimeCaller) Inspect(ctx context.Context) (RuntimeCallerObservation, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeCallerObservation{}, err
	}
	if caller == nil {
		return RuntimeCallerObservation{}, ErrRuntimeCallerIdentity
	}
	caller.mu.Lock()
	defer caller.mu.Unlock()
	return caller.inspectLocked(ctx)
}

func (caller *RuntimeCaller) inspectLocked(ctx context.Context) (RuntimeCallerObservation, error) {
	if caller.pidfd == nil || caller.process == nil || checkRuntimePIDFD(int(caller.pidfd.Fd()), caller.peer.Pid) != nil {
		return RuntimeCallerObservation{}, ErrRuntimeCallerIdentity
	}
	status, err := readRuntimeProc(caller.process, "status")
	if err != nil {
		return RuntimeCallerObservation{}, err
	}
	stat, err := readRuntimeProc(caller.process, "stat")
	if err != nil {
		return RuntimeCallerObservation{}, err
	}
	observation, err := parseRuntimeProcess(status, stat, caller.peer)
	if err != nil {
		return RuntimeCallerObservation{}, err
	}
	observation.PIDNamespace, err = caller.process.Readlink("ns/pid")
	if err != nil || !strings.HasPrefix(observation.PIDNamespace, "pid:[") || !strings.HasSuffix(observation.PIDNamespace, "]") {
		return RuntimeCallerObservation{}, ErrRuntimeCallerIdentity
	}
	boot, err := readBootID("/proc/sys/kernel/random/boot_id")
	if err != nil || boot != caller.boot.String() {
		return RuntimeCallerObservation{}, ErrRuntimeCallerIdentity
	}
	if err := errors.Join(context.Cause(ctx), checkRuntimePIDFD(int(caller.pidfd.Fd()), caller.peer.Pid)); err != nil {
		return RuntimeCallerObservation{}, err
	}
	observation.BootID, observation.ObservedAt = caller.boot, time.Now().UTC()
	return observation, nil
}

func readRuntimeProc(root *os.Root, name string) (string, error) {
	file, err := root.Open(name)
	if err != nil {
		return "", err
	}
	data, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if err := errors.Join(err, file.Close()); err != nil || len(data) > 64<<10 {
		return "", errors.Join(ErrRuntimeCallerIdentity, err)
	}
	return string(data), nil
}

func checkRuntimePIDFD(fd int, expectedPID int32) error {
	if fd < 0 || expectedPID <= 0 {
		return ErrRuntimeCallerIdentity
	}
	if err := runtimechannel.PollLivePIDFD(fd); err != nil {
		return errors.Join(ErrRuntimeCallerIdentity, err)
	}
	info, err := readBoundedSystemText(fmt.Sprintf("/proc/self/fdinfo/%d", fd), 4096)
	if err != nil {
		return err
	}
	for line := range strings.SplitSeq(info, "\n") {
		if strings.HasPrefix(line, "Pid:") && strings.TrimSpace(strings.TrimPrefix(line, "Pid:")) == strconv.Itoa(int(expectedPID)) {
			return nil
		}
	}
	return ErrRuntimeCallerIdentity
}

func parseRuntimeProcess(status, stat string, peer unix.Ucred) (RuntimeCallerObservation, error) {
	fields := make(map[string][]string)
	for line := range strings.SplitSeq(status, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok || key != "Uid" && key != "Gid" && key != "NSpid" {
			continue
		}
		if _, exists := fields[key]; exists {
			return RuntimeCallerObservation{}, ErrRuntimeCallerIdentity
		}
		fields[key] = strings.Fields(value)
	}
	for key, expected := range map[string]uint32{"Uid": peer.Uid, "Gid": peer.Gid} {
		if len(fields[key]) != 4 {
			return RuntimeCallerObservation{}, ErrRuntimeCallerIdentity
		}
		for _, value := range fields[key] {
			if value != strconv.FormatUint(uint64(expected), 10) {
				return RuntimeCallerObservation{}, ErrRuntimeCallerIdentity
			}
		}
	}
	nested := fields["NSpid"]
	if len(nested) == 0 || len(nested) > 32 || nested[0] != strconv.Itoa(int(peer.Pid)) {
		return RuntimeCallerObservation{}, ErrRuntimeCallerIdentity
	}
	var namespacePID int64
	for _, value := range nested {
		var err error
		namespacePID, err = strconv.ParseInt(value, 10, 32)
		if err != nil || namespacePID <= 0 || strconv.FormatInt(namespacePID, 10) != value {
			return RuntimeCallerObservation{}, ErrRuntimeCallerIdentity
		}
	}
	start, end := strings.IndexByte(stat, '('), strings.LastIndexByte(stat, ')')
	if start < 2 || end <= start || strings.TrimSpace(stat[:start]) != strconv.Itoa(int(peer.Pid)) {
		return RuntimeCallerObservation{}, ErrRuntimeCallerIdentity
	}
	values := strings.Fields(stat[end+1:])
	if len(values) < 20 || values[0] == "Z" || values[0] == "X" || values[0] == "x" {
		return RuntimeCallerObservation{}, ErrRuntimeCallerIdentity
	}
	ticks, err := strconv.ParseUint(values[19], 10, 64)
	if err != nil || ticks == 0 {
		return RuntimeCallerObservation{}, ErrRuntimeCallerIdentity
	}
	return RuntimeCallerObservation{HostPID: peer.Pid, UID: peer.Uid, GID: peer.Gid, StartTicks: ticks,
		NamespacePID: int32(namespacePID), NamespaceDepth: len(nested)}, nil
}
