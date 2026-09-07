package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

var ErrRuntimeFileLock = errors.New("runtime descriptor has no consistently observed private exclusive flock")

// RuntimeFileLockObservation describes a current descriptor of the retained
// caller. The descriptor number is only a selector. Neither inode equality nor
// the kernel's numeric lock PID proves original journal provenance, a unique
// descriptor owner, uninterrupted locking, or startup authorization.
type RuntimeFileLockObservation struct {
	Process         RuntimeCallerObservation `json:"process"`
	Descriptor      int                      `json:"descriptor"`
	FileDevice      uint64                   `json:"file_device"`
	FileInode       uint64                   `json:"file_inode"`
	SizeBytes       int64                    `json:"size_bytes"`
	MountID         uint64                   `json:"mount_id"`
	LockPID         int32                    `json:"lock_pid"`
	ObservedFrom    time.Time                `json:"observed_from"`
	ObservedThrough time.Time                `json:"observed_through"`
}

// InspectFileLock reads kernel fdinfo through the original pinned procfs root.
// It requires one whole-file exclusive flock on a private, single-link regular
// file owned by this non-root caller. O_PATH pins only the inode: unlike dup or
// pidfd_getfd it does not retain, acquire, unlock or extend the source flock.
// Repeated reads reject visible changes; unlock/relock and fd reuse ABA remain
// unproven. No caller-supplied path or file contents are opened as authority.
func (caller *RuntimeCaller) InspectFileLock(ctx context.Context, descriptor int) (RuntimeFileLockObservation, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeFileLockObservation{}, err
	}
	if caller == nil || descriptor < 0 || descriptor >= 1<<20 {
		return RuntimeFileLockObservation{}, ErrRuntimeFileLock
	}
	ctx, cancel := context.WithTimeout(ctx, runtimeCallerTimeout)
	defer cancel()
	from := time.Now().UTC()
	caller.mu.Lock()
	defer caller.mu.Unlock()
	first, err := caller.inspectLocked(ctx)
	if err != nil {
		return RuntimeFileLockObservation{}, err
	}
	process, err := caller.process.Open(".")
	if err != nil {
		return RuntimeFileLockObservation{}, err
	}
	defer func() { _ = process.Close() }()
	file, err := openRuntimeFileLock(process, descriptor)
	if err != nil {
		return RuntimeFileLockObservation{}, err
	}
	defer func() { _ = file.Close() }()
	identity, err := inspectRuntimeFileLock(file, caller.peer)
	if err != nil {
		return RuntimeFileLockObservation{}, err
	}
	read := func() (runtimeFileLockInfo, error) {
		wire, err := readRuntimeProc(caller.process, "fdinfo/"+strconv.Itoa(descriptor))
		if err != nil {
			return runtimeFileLockInfo{}, err
		}
		return parseRuntimeFileLockInfo(wire, identity, caller.peer.Pid)
	}
	info, err := read()
	if err != nil {
		return RuntimeFileLockObservation{}, err
	}
	current, err := openRuntimeFileLock(process, descriptor)
	if err != nil {
		return RuntimeFileLockObservation{}, err
	}
	defer func() { _ = current.Close() }()
	currentIdentity, err := inspectRuntimeFileLock(current, caller.peer)
	if err != nil || identity != currentIdentity {
		return RuntimeFileLockObservation{}, errors.Join(ErrRuntimeFileLock, err)
	}
	lastInfo, err := read()
	if err != nil || info != lastInfo {
		return RuntimeFileLockObservation{}, errors.Join(ErrRuntimeFileLock, err)
	}
	lastIdentity, err := inspectRuntimeFileLock(file, caller.peer)
	if err != nil || identity != lastIdentity {
		return RuntimeFileLockObservation{}, errors.Join(ErrRuntimeFileLock, err)
	}
	last, err := caller.inspectLocked(ctx)
	if err != nil {
		return RuntimeFileLockObservation{}, err
	}
	first.ObservedAt = last.ObservedAt
	through := time.Now().UTC()
	if first != last || through.Before(from) || through.Sub(from) > runtimeCallerTimeout {
		return RuntimeFileLockObservation{}, ErrRuntimeFileLock
	}
	if err := context.Cause(ctx); err != nil {
		return RuntimeFileLockObservation{}, err
	}
	return RuntimeFileLockObservation{Process: last, Descriptor: descriptor, FileDevice: identity.device,
		FileInode: identity.inode, SizeBytes: identity.size, MountID: info.mount, LockPID: info.pid,
		ObservedFrom: from, ObservedThrough: through}, nil
}

func openRuntimeFileLock(process *os.File, descriptor int) (*os.File, error) {
	var filesystem unix.Statfs_t
	if err := unix.Fstatfs(int(process.Fd()), &filesystem); err != nil || filesystem.Type != unix.PROC_SUPER_MAGIC {
		return nil, errors.Join(ErrRuntimeFileLock, err)
	}
	// Intentionally follow only this fixed procfs magic link. O_PATH does not
	// invoke the untrusted target's open method, even if it is a device or FIFO.
	fd, err := unix.Openat(int(process.Fd()), "fd/"+strconv.Itoa(descriptor), unix.O_PATH|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.Join(ErrRuntimeFileLock, err)
	}
	return os.NewFile(uintptr(fd), "runtime-file-lock-inode"), nil
}

type runtimeFileLockIdentity struct {
	device, inode     uint64
	size              int64
	modified, changed unix.Timespec
}

func inspectRuntimeFileLock(file *os.File, peer unix.Ucred) (runtimeFileLockIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return runtimeFileLockIdentity{}, err
	}
	if stat.Mode != unix.S_IFREG|0o600 || stat.Nlink != 1 || stat.Uid != peer.Uid || stat.Gid != peer.Gid || stat.Size < 0 {
		return runtimeFileLockIdentity{}, ErrRuntimeFileLock
	}
	return runtimeFileLockIdentity{device: stat.Dev, inode: stat.Ino, size: stat.Size, modified: stat.Mtim, changed: stat.Ctim}, nil
}

type runtimeFileLockInfo struct {
	position, flags, mount uint64
	pid                    int32
}

func parseRuntimeFileLockInfo(document string, identity runtimeFileLockIdentity, expectedPID int32) (runtimeFileLockInfo, error) {
	if len(document) == 0 || len(document) > 4096 || !strings.HasSuffix(document, "\n") || expectedPID <= 0 {
		return runtimeFileLockInfo{}, ErrRuntimeFileLock
	}
	fields := make(map[string]string, 5)
	for line := range strings.SplitSeq(strings.TrimSuffix(document, "\n"), "\n") {
		key, value, ok := strings.Cut(line, ":\t")
		if !ok || fields[key] != "" || value == "" || key != "pos" && key != "flags" && key != "mnt_id" && key != "ino" && key != "lock" {
			return runtimeFileLockInfo{}, ErrRuntimeFileLock
		}
		fields[key] = value
	}
	if len(fields) != 5 {
		return runtimeFileLockInfo{}, ErrRuntimeFileLock
	}
	position, positionErr := strconv.ParseUint(fields["pos"], 10, 63)
	flags, flagsErr := strconv.ParseUint(fields["flags"], 8, 32)
	mount, mountErr := strconv.ParseUint(fields["mnt_id"], 10, 64)
	if err := errors.Join(positionErr, flagsErr, mountErr); err != nil || mount == 0 ||
		flags&unix.O_ACCMODE != unix.O_RDWR || flags&unix.O_CLOEXEC == 0 || flags&unix.O_PATH != 0 ||
		fields["ino"] != strconv.FormatUint(identity.inode, 10) {
		return runtimeFileLockInfo{}, errors.Join(ErrRuntimeFileLock, err)
	}
	lock := strings.Fields(fields["lock"])
	device := fmt.Sprintf("%02x:%02x:%d", unix.Major(identity.device), unix.Minor(identity.device), identity.inode)
	if len(lock) != 8 || lock[0] != "1:" || lock[1] != "FLOCK" || lock[2] != "ADVISORY" || lock[3] != "WRITE" ||
		lock[4] != strconv.Itoa(int(expectedPID)) || lock[5] != device || lock[6] != "0" || lock[7] != "EOF" {
		return runtimeFileLockInfo{}, ErrRuntimeFileLock
	}
	return runtimeFileLockInfo{position: position, flags: flags, mount: mount, pid: expectedPID}, nil
}
