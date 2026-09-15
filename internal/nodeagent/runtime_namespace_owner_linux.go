package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/runtimechannel"
	"golang.org/x/sys/unix"
)

var (
	ErrRuntimeNamespaceOwnerLive = errors.New("retained runtime namespace owner has not exited")
	ErrRuntimeNamespaceOwnerLost = errors.New("runtime namespace owner handle is unavailable or invalid")
)

// RuntimeNamespaceExitObservation records a kernel-observed exit of the exact
// retained namespace-init process. ObservedAt is not the actual exit timestamp.
// It is not a durable incarnation, startup grant, Stage drain or device release.
type RuntimeNamespaceExitObservation struct {
	Owner      RuntimeContainerCallerObservation `json:"owner"`
	RetainedAt time.Time                         `json:"retained_at"`
	ObservedAt time.Time                         `json:"observed_at"`
}

// RuntimeNamespaceOwner owns an independent copy of the original kernel pidfd.
// Neither a serialized observation nor a PID can reconstruct this handle.
// A Node restart losing the handle cannot infer exit from absent metadata.
type RuntimeNamespaceOwner struct {
	mu         sync.Mutex
	pidfd      *os.File
	owner      RuntimeContainerCallerObservation
	retainedAt time.Time
	exit       *RuntimeNamespaceExitObservation
}

// RetainNamespaceOwner first verifies the live CRI/native-task/PID-1 relation.
// target must come from trusted Node inventory. The resulting handle survives
// closure of caller, its request socket and observer. It neither starts nor
// stops a process, and does not attest effective launch configuration.
func (observer *RuntimeContainerObserver) RetainNamespaceOwner(ctx context.Context, target RuntimeContainerTarget, caller *RuntimeCaller) (*RuntimeNamespaceOwner, error) {
	observed, err := observer.ObserveCaller(ctx, target, caller)
	if err != nil {
		return nil, err
	}
	caller.mu.Lock()
	defer caller.mu.Unlock()
	current, err := caller.inspectLocked(ctx)
	if err != nil {
		return nil, err
	}
	previous := observed.Process
	previous.ObservedAt = current.ObservedAt
	if previous != current || current.NamespacePID != 1 || current.NamespaceDepth < 2 {
		return nil, ErrRuntimeNamespaceOwnerLost
	}
	fd, err := unix.FcntlInt(caller.pidfd.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	owner := &RuntimeNamespaceOwner{pidfd: os.NewFile(uintptr(fd), "runtime-namespace-owner-pidfd"), owner: observed, retainedAt: time.Now().UTC()}
	success := false
	defer func() {
		if !success {
			_ = owner.Close()
		}
	}()
	if err := errors.Join(owner.checkLocked(), checkRuntimePIDFD(fd, current.HostPID), context.Cause(ctx)); err != nil {
		return nil, err
	}
	success = true
	return owner, nil
}

// RetainNamespaceOwnerFromPIDFD enrolls a distinct Worker process whose
// original pidfd was supplied by the trusted launcher. The pidfd is never
// reacquired from the numeric PID; procfs is opened only as a read-only view
// for the already retained kernel handle.
func (observer *RuntimeContainerObserver) RetainNamespaceOwnerFromPIDFD(ctx context.Context, target RuntimeContainerTarget, pidfd *os.File, credentials RuntimeCallerCredentials) (*RuntimeNamespaceOwner, error) {
	if observer == nil || pidfd == nil || credentials.UID == 0 || credentials.GID == 0 {
		return nil, ErrRuntimeNamespaceOwnerLost
	}
	if err := errors.Join(runtimechannel.ValidatePIDFD(int(pidfd.Fd())), runtimechannel.PollLivePIDFD(int(pidfd.Fd()))); err != nil {
		return nil, errors.Join(ErrRuntimeNamespaceOwnerLost, err)
	}
	info, err := readBoundedSystemText(fmt.Sprintf("/proc/self/fdinfo/%d", pidfd.Fd()), 4096)
	if err != nil {
		return nil, errors.Join(ErrRuntimeNamespaceOwnerLost, err)
	}
	pid := ""
	for line := range strings.SplitSeq(info, "\n") {
		if strings.HasPrefix(line, "Pid:") {
			pid = strings.TrimSpace(strings.TrimPrefix(line, "Pid:"))
		}
	}
	parsed, err := strconv.ParseInt(pid, 10, 32)
	if err != nil || parsed <= 0 {
		return nil, ErrRuntimeNamespaceOwnerLost
	}
	root, err := os.OpenRoot(fmt.Sprintf("/proc/%d", parsed))
	if err != nil {
		return nil, errors.Join(ErrRuntimeNamespaceOwnerLost, err)
	}
	boot, err := readBootID("/proc/sys/kernel/random/boot_id")
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	caller := &RuntimeCaller{pidfd: pidfd, process: root, peer: unix.Ucred{Pid: int32(parsed), Uid: credentials.UID, Gid: credentials.GID}, boot: uuid.MustParse(boot)}
	if _, err := caller.Inspect(ctx); err != nil {
		_ = root.Close()
		return nil, errors.Join(ErrRuntimeNamespaceOwnerLost, err)
	}
	owner, err := observer.RetainNamespaceOwner(ctx, target, caller)
	_ = root.Close()
	return owner, err
}

// ObserveExit performs no CRI, task or numeric-PID lookup. Only readiness of the
// retained pidfd can establish process exit. The first successful observation
// is stable across repeat calls. A live process, canceled context or lost handle
// yields no observation; Close is never interpreted as process retirement.
func (owner *RuntimeNamespaceOwner) ObserveExit(ctx context.Context) (RuntimeNamespaceExitObservation, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeNamespaceExitObservation{}, err
	}
	if owner == nil {
		return RuntimeNamespaceExitObservation{}, ErrRuntimeNamespaceOwnerLost
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err := errors.Join(owner.checkLocked(), context.Cause(ctx)); err != nil {
		return RuntimeNamespaceExitObservation{}, err
	}
	if owner.exit != nil {
		return *owner.exit, nil
	}
	watch := []unix.PollFd{{Fd: int32(owner.pidfd.Fd()), Events: unix.POLLIN}}
	count, err := unix.Poll(watch, 0)
	if err != nil || count < 0 || count > 1 || watch[0].Revents&(unix.POLLERR|unix.POLLNVAL) != 0 {
		return RuntimeNamespaceExitObservation{}, errors.Join(ErrRuntimeNamespaceOwnerLost, err)
	}
	if count == 0 || watch[0].Revents&unix.POLLIN == 0 {
		return RuntimeNamespaceExitObservation{}, ErrRuntimeNamespaceOwnerLive
	}
	if err := context.Cause(ctx); err != nil {
		return RuntimeNamespaceExitObservation{}, err
	}
	now := time.Now().UTC()
	if now.Before(owner.retainedAt) {
		return RuntimeNamespaceExitObservation{}, ErrRuntimeNamespaceOwnerLost
	}
	owner.exit = &RuntimeNamespaceExitObservation{Owner: owner.owner, RetainedAt: owner.retainedAt, ObservedAt: now}
	return *owner.exit, nil
}

func (owner *RuntimeNamespaceOwner) checkLocked() error {
	if owner.pidfd == nil {
		return ErrRuntimeNamespaceOwnerLost
	}
	fd := int(owner.pidfd.Fd())
	if err := runtimechannel.ValidatePIDFD(fd); err != nil {
		return errors.Join(ErrRuntimeNamespaceOwnerLost, err)
	}
	if flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil || flags&unix.FD_CLOEXEC == 0 {
		return errors.Join(ErrRuntimeNamespaceOwnerLost, err)
	}
	boot, err := readBootID("/proc/sys/kernel/random/boot_id")
	if err != nil || boot != owner.owner.Process.BootID.String() {
		return errors.Join(ErrRuntimeNamespaceOwnerLost, err)
	}
	return nil
}

func (owner *RuntimeNamespaceOwner) Close() error {
	if owner == nil {
		return nil
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.pidfd == nil {
		return nil
	}
	err := owner.pidfd.Close()
	owner.pidfd, owner.exit = nil, nil
	return err
}

// Credentials returns the UID/GID observed from the retained original owner.
// It is configuration evidence for a protected local listener; it does not
// grant journal or startup authority and cannot reconstruct the pidfd.
func (owner *RuntimeNamespaceOwner) Credentials() (RuntimeCallerCredentials, error) {
	if owner == nil {
		return RuntimeCallerCredentials{}, ErrRuntimeNamespaceOwnerLost
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err := owner.checkLocked(); err != nil {
		return RuntimeCallerCredentials{}, err
	}
	return RuntimeCallerCredentials{UID: owner.owner.Process.UID, GID: owner.owner.Process.GID}, nil
}
