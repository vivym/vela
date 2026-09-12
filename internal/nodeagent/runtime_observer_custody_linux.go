package nodeagent

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vivym/vela/internal/runtimechannel"
	"golang.org/x/sys/unix"
)

const runtimeObserverProtocol = "vela-exec-custody-v1"
const runtimeObserverFrameSize = len(runtimeObserverProtocol) + 1 + 32

var ErrRuntimeObserverCustody = errors.New("runtime observer custody or live exchange is unavailable")

// RuntimeObserverCustody owns a private creation channel and independent kernel
// handles. A descriptor offer proves neither approved code nor a launch grant.
// The Node must independently trust the creator and its execution policy.
// It must not reconstruct this object from recorded observations or a PID.
type RuntimeObserverCustody struct {
	exchangeGate  chan struct{}
	revokedSignal chan struct{}
	mu            sync.Mutex
	connection    *net.UnixConn
	observer      *os.File
	target        *os.File
	challenge     []byte
	started       bool
	revoked       bool
}

// ReceiveRuntimeObserverCustody takes ownership of an unnamed Node-created Unix
// socketpair endpoint. originalObserver is borrowed and duplicated: it must be
// the independently retained pidfd of the trusted creator started by this Node,
// never a PID/handle supplied by a workload or the incoming offer. The creator
// must close its endpoint in the target before any target executable runs.
//
// The original child remains in its initial ptrace stop until Start succeeds.
// Once the private channel and observer handle are validated, receive errors
// revoke that creator. Close also requests termination; it is not a detach or
// process-exit proof. This API does not establish production CRI/plan approval.
func ReceiveRuntimeObserverCustody(ctx context.Context, connection *net.UnixConn, originalObserver *os.File) (*RuntimeObserverCustody, error) {
	return ReceiveRuntimeObserverCustodyFromCreator(ctx, connection, originalObserver, nil)
}

// ReceiveRuntimeObserverCustodyFromCreator is the launcher composition-root
// variant. The observer is a direct child of the root-owned launcher helper,
// which is itself a direct child of Node. creatorPIDFD is the launcher's
// original kernel handle; its identity is compared with the observer's parent
// metadata without reopening a numeric PID.
func ReceiveRuntimeObserverCustodyFromCreator(ctx context.Context, connection *net.UnixConn, originalObserver, creatorPIDFD *os.File) (*RuntimeObserverCustody, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if connection == nil || originalObserver == nil {
		return nil, ErrRuntimeObserverCustody
	}
	if err := runtimechannel.SameLiveProcess(int(originalObserver.Fd()), int(originalObserver.Fd())); err != nil {
		return nil, errors.Join(ErrRuntimeObserverCustody, err)
	}
	if err := inspectRuntimeObserverOrigin(int(originalObserver.Fd()), creatorPIDFD); err != nil {
		return nil, err
	}
	fd, err := unix.FcntlInt(originalObserver.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	custody := &RuntimeObserverCustody{connection: connection, observer: os.NewFile(uintptr(fd), "runtime-observer-pidfd"), exchangeGate: make(chan struct{}, 1), revokedSignal: make(chan struct{})}
	success := false
	defer func() {
		if !success {
			_ = custody.Close()
		}
	}()
	if err := configureRuntimeObserverChannel(connection); err != nil {
		return nil, err
	}
	err = custody.exchange(ctx, func() error {
		challenge, err := newRuntimeObserverFrame('C')
		if err != nil {
			return err
		}
		if err := writeRuntimeObserverFrame(connection, challenge); err != nil {
			return err
		}
		packet, peer, sender, target, err := runtimechannel.ReadProcessOffer(connection, runtimeObserverFrameSize)
		if err != nil {
			return err
		}
		defer func() { _ = unix.Close(sender) }()
		accepted := false
		defer func() {
			if !accepted {
				_ = unix.Close(target)
			}
		}()
		if err := custody.checkSender(peer, sender); err != nil {
			return err
		}
		if !sameRuntimeObserverFrame(packet, challenge, 'O') {
			return ErrRuntimeObserverCustody
		}
		if err := inspectObserverChild(fd, target, peer.Pid); err != nil {
			return err
		}
		custody.target = os.NewFile(uintptr(target), "runtime-observer-original-target")
		custody.challenge = challenge
		accepted = true
		return nil
	})
	if err != nil {
		return nil, errors.Join(ErrRuntimeObserverCustody, err)
	}
	success = true
	return custody, nil
}

// Start attempts once to release the already offered child for its initial
// executable. This only acknowledges custody: it is not backend authorization.
func (custody *RuntimeObserverCustody) Start(ctx context.Context) error {
	if custody == nil {
		return ErrRuntimeObserverCustody
	}
	if err := custody.beginExchange(ctx); err != nil {
		return err
	}
	defer func() { <-custody.exchangeGate }()
	custody.mu.Lock()
	if custody.started || custody.revoked || custody.target == nil {
		custody.mu.Unlock()
		return ErrRuntimeObserverCustody
	}
	custody.started = true
	custody.mu.Unlock()
	err := custody.exchange(ctx, func() error {
		packet := bytes.Clone(custody.challenge)
		packet[len(runtimeObserverProtocol)] = 'A'
		if err := writeRuntimeObserverFrame(custody.connection, packet); err != nil {
			return err
		}
		return custody.readResponse(packet, 'R')
	})
	custody.challenge = nil
	if err != nil {
		return errors.Join(ErrRuntimeObserverCustody, err, custody.Revoke())
	}
	return nil
}

// Check exchanges a fresh challenge with the creator's actual event loop.
// Failure revokes the retained creator and target; a live pidfd alone is not a
// liveness response. This is an explicit check, not an autonomous watchdog.
func (custody *RuntimeObserverCustody) Check(ctx context.Context) error {
	if custody == nil {
		return ErrRuntimeObserverCustody
	}
	if err := custody.beginExchange(ctx); err != nil {
		return err
	}
	defer func() { <-custody.exchangeGate }()
	custody.mu.Lock()
	if !custody.started || custody.revoked || custody.target == nil {
		custody.mu.Unlock()
		return ErrRuntimeObserverCustody
	}
	custody.mu.Unlock()
	err := custody.exchange(ctx, func() error {
		if err := runtimechannel.PollLivePIDFD(int(custody.target.Fd())); err != nil {
			return err
		}
		packet, err := newRuntimeObserverFrame('P')
		if err != nil {
			return err
		}
		if err := writeRuntimeObserverFrame(custody.connection, packet); err != nil {
			return err
		}
		return custody.readResponse(packet, 'L')
	})
	if err != nil {
		return errors.Join(ErrRuntimeObserverCustody, err, custody.Revoke())
	}
	return nil
}

// MatchCaller checks the live creator and exact offered process handle against
// an independently authenticated Runtime caller. It grants no write authority.
func (custody *RuntimeObserverCustody) MatchCaller(ctx context.Context, caller *RuntimeCaller) error {
	if caller == nil {
		return ErrRuntimeObserverCustody
	}
	if err := custody.Check(ctx); err != nil {
		return err
	}
	custody.mu.Lock()
	defer custody.mu.Unlock()
	caller.mu.Lock()
	defer caller.mu.Unlock()
	if custody.revoked || custody.target == nil || caller.pidfd == nil {
		return ErrRuntimeObserverCustody
	}
	return errors.Join(context.Cause(ctx), runtimechannel.SameLiveProcess(int(custody.target.Fd()), int(caller.pidfd.Fd())), runtimechannel.PollLivePIDFD(int(custody.observer.Fd())))
}

// TargetExited observes only the retained original pidfd. A closed or lost
// handle returns an error; a termination request alone never returns true.
func (custody *RuntimeObserverCustody) TargetExited(ctx context.Context) (bool, error) {
	if err := contextError(ctx); err != nil {
		return false, err
	}
	if custody == nil {
		return false, ErrRuntimeObserverCustody
	}
	custody.mu.Lock()
	defer custody.mu.Unlock()
	if custody.target == nil {
		return false, ErrRuntimeObserverCustody
	}
	watch := []unix.PollFd{{Fd: int32(custody.target.Fd()), Events: unix.POLLIN}}
	for {
		count, err := unix.Poll(watch, 0)
		if errors.Is(err, unix.EINTR) && ctx.Err() == nil {
			continue
		}
		if err != nil || watch[0].Revents&(unix.POLLNVAL|unix.POLLERR) != 0 || ctx.Err() != nil {
			return false, errors.Join(ErrRuntimeObserverCustody, err, context.Cause(ctx))
		}
		return count == 1 && watch[0].Revents&unix.POLLIN != 0, nil
	}
}

// Revoke is irreversible and uses only the original pidfds. The handles remain
// available for TargetExited until Close. Revoke does not prove termination.
func (custody *RuntimeObserverCustody) Revoke() error {
	if custody == nil {
		return nil
	}
	custody.mu.Lock()
	defer custody.mu.Unlock()
	return custody.revokeLocked()
}

func (custody *RuntimeObserverCustody) revokeLocked() error {
	if custody.revoked {
		return nil
	}
	custody.revoked = true
	if custody.revokedSignal != nil {
		close(custody.revokedSignal)
	}
	var result error
	if custody.connection != nil {
		// Interrupt an in-flight receive without waiting for its exchange gate.
		result = custody.connection.SetDeadline(time.Now())
	}
	for _, process := range []*os.File{custody.target, custody.observer} {
		if process != nil {
			if err := unix.PidfdSendSignal(int(process.Fd()), unix.SIGKILL, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
				result = errors.Join(result, err)
			}
		}
	}
	return result
}

func (custody *RuntimeObserverCustody) Close() error {
	if custody == nil || custody.exchangeGate == nil {
		return nil
	}
	err := custody.Revoke()
	custody.exchangeGate <- struct{}{}
	defer func() { <-custody.exchangeGate }()
	custody.mu.Lock()
	defer custody.mu.Unlock()
	for _, file := range []*os.File{custody.target, custody.observer} {
		if file != nil {
			err = errors.Join(err, file.Close())
		}
	}
	if custody.connection != nil {
		err = errors.Join(err, custody.connection.Close())
	}
	custody.target, custody.observer, custody.connection, custody.challenge = nil, nil, nil, nil
	return err
}

func (custody *RuntimeObserverCustody) beginExchange(ctx context.Context) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if custody.exchangeGate == nil {
		return ErrRuntimeObserverCustody
	}
	select {
	case <-custody.revokedSignal:
		return ErrRuntimeObserverCustody
	default:
	}
	select {
	case custody.exchangeGate <- struct{}{}:
		return nil
	case <-custody.revokedSignal:
		return ErrRuntimeObserverCustody
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (custody *RuntimeObserverCustody) exchange(ctx context.Context, action func() error) (resultErr error) {
	if err := contextError(ctx); err != nil {
		return err
	}
	deadline := time.Now().Add(runtimeCallerTimeout)
	if limit, ok := ctx.Deadline(); ok && limit.Before(deadline) {
		deadline = limit
	}
	if err := custody.connection.SetDeadline(deadline); err != nil {
		return err
	}
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = custody.connection.SetDeadline(time.Now())
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
		resultErr = errors.Join(resultErr, cause, custody.connection.SetDeadline(time.Time{}))
	}()
	return action()
}

func (custody *RuntimeObserverCustody) checkSender(peer unix.Ucred, fd int) error {
	custody.mu.Lock()
	defer custody.mu.Unlock()
	if custody.revoked || custody.observer == nil || peer.Pid <= 0 || peer.Uid != 0 || peer.Gid != 0 {
		return ErrRuntimeObserverCustody
	}
	return runtimechannel.SameLiveProcess(int(custody.observer.Fd()), fd)
}

func (custody *RuntimeObserverCustody) readResponse(challenge []byte, operation byte) error {
	packet, peer, sender, err := runtimechannel.ReadPacket(custody.connection, runtimeObserverFrameSize)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(sender) }()
	if !sameRuntimeObserverFrame(packet, challenge, operation) {
		return ErrRuntimeObserverCustody
	}
	return custody.checkSender(peer, sender)
}

func newRuntimeObserverFrame(operation byte) ([]byte, error) {
	packet := make([]byte, runtimeObserverFrameSize)
	copy(packet, runtimeObserverProtocol)
	packet[len(runtimeObserverProtocol)] = operation
	_, err := rand.Read(packet[len(runtimeObserverProtocol)+1:])
	return packet, err
}

func sameRuntimeObserverFrame(packet, challenge []byte, operation byte) bool {
	return len(packet) == runtimeObserverFrameSize && len(challenge) == runtimeObserverFrameSize &&
		bytes.Equal(packet[:len(runtimeObserverProtocol)], []byte(runtimeObserverProtocol)) &&
		packet[len(runtimeObserverProtocol)] == operation && bytes.Equal(packet[len(runtimeObserverProtocol)+1:], challenge[len(runtimeObserverProtocol)+1:])
}

func writeRuntimeObserverFrame(connection *net.UnixConn, packet []byte) error {
	if count, err := connection.Write(packet); err != nil || count != len(packet) {
		return errors.Join(ErrRuntimeObserverCustody, err)
	}
	return nil
}

func configureRuntimeObserverChannel(connection *net.UnixConn) error {
	raw, err := connection.SyscallConn()
	if err != nil {
		return err
	}
	var configured error
	err = raw.Control(func(fd uintptr) {
		kind, kindErr := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TYPE)
		local, localErr := unix.Getsockname(int(fd))
		peer, peerErr := unix.Getpeername(int(fd))
		localUnix, localOK := local.(*unix.SockaddrUnix)
		peerUnix, peerOK := peer.(*unix.SockaddrUnix)
		if kind != unix.SOCK_SEQPACKET || !localOK || !peerOK || localUnix.Name != "" && localUnix.Name != "@" || peerUnix.Name != "" && peerUnix.Name != "@" {
			configured = fmt.Errorf("private observer channel type=%d local=%#v peer=%#v: %w", kind, local, peer, ErrRuntimeObserverCustody)
			return
		}
		configured = errors.Join(kindErr, localErr, peerErr, unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSCRED, 1), unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PASSPIDFD, 1))
	})
	return errors.Join(err, configured)
}

func inspectObserverChild(observer, target int, observerPID int32) error {
	if err := checkRuntimePIDFD(observer, observerPID); err != nil {
		return err
	}
	info, err := readBoundedSystemText(fmt.Sprintf("/proc/self/fdinfo/%d", target), 4096)
	if err != nil {
		return err
	}
	pid := 0
	for line := range strings.SplitSeq(info, "\n") {
		if strings.HasPrefix(line, "Pid:") {
			pid, err = strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "Pid:")))
		}
	}
	if err != nil || pid <= 0 || pid > 1<<31-1 {
		return ErrRuntimeObserverCustody
	}
	status, err := readBoundedSystemText(fmt.Sprintf("/proc/%d/status", pid), 64<<10)
	if err != nil {
		return err
	}
	fields := make(map[string][]string)
	for line := range strings.SplitSeq(status, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok {
			fields[key] = strings.Fields(value)
		}
	}
	for key, expected := range map[string]string{"PPid": strconv.Itoa(int(observerPID)), "TracerPid": strconv.Itoa(int(observerPID)), "Tgid": strconv.Itoa(pid), "Threads": "1"} {
		if len(fields[key]) != 1 || fields[key][0] != expected {
			return ErrRuntimeObserverCustody
		}
	}
	if len(fields["State"]) == 0 || fields["State"][0] != "t" {
		return ErrRuntimeObserverCustody
	}
	return errors.Join(checkRuntimePIDFD(observer, observerPID), checkRuntimePIDFD(target, int32(pid)))
}

func inspectRuntimeObserverOrigin(fd int, creatorPIDFD *os.File) error {
	info, err := readBoundedSystemText(fmt.Sprintf("/proc/self/fdinfo/%d", fd), 4096)
	if err != nil {
		return err
	}
	pid := 0
	for line := range strings.SplitSeq(info, "\n") {
		if strings.HasPrefix(line, "Pid:") {
			pid, err = strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "Pid:")))
		}
	}
	if err != nil || pid <= 0 || pid > 1<<31-1 {
		return ErrRuntimeObserverCustody
	}
	status, err := readBoundedSystemText(fmt.Sprintf("/proc/%d/status", pid), 64<<10)
	if err != nil {
		return err
	}
	fields := make(map[string][]string)
	for line := range strings.SplitSeq(status, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if ok {
			fields[key] = strings.Fields(value)
		}
	}
	if len(fields["PPid"]) != 1 {
		return ErrRuntimeObserverCustody
	}
	if creatorPIDFD == nil {
		if fields["PPid"][0] != strconv.Itoa(os.Getpid()) {
			return ErrRuntimeObserverCustody
		}
	} else {
		if err := runtimechannel.ValidatePIDFD(int(creatorPIDFD.Fd())); err != nil {
			return errors.Join(ErrRuntimeObserverCustody, err)
		}
		creatorInfo, err := readBoundedSystemText(fmt.Sprintf("/proc/self/fdinfo/%d", creatorPIDFD.Fd()), 4096)
		if err != nil {
			return errors.Join(ErrRuntimeObserverCustody, err)
		}
		creatorPID := ""
		for line := range strings.SplitSeq(creatorInfo, "\n") {
			if strings.HasPrefix(line, "Pid:") {
				creatorPID = strings.TrimSpace(strings.TrimPrefix(line, "Pid:"))
			}
		}
		if creatorPID == "" || fields["PPid"][0] != creatorPID {
			return ErrRuntimeObserverCustody
		}
	}
	for _, key := range []string{"Uid", "Gid"} {
		if len(fields[key]) != 4 {
			return ErrRuntimeObserverCustody
		}
		for _, value := range fields[key] {
			if value != "0" {
				return ErrRuntimeObserverCustody
			}
		}
	}
	return checkRuntimePIDFD(fd, int32(pid))
}
