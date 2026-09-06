package nodeagent

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

const maximumRuntimeExecutableBytes = int64(256 << 20)

var ErrRuntimeExecutable = errors.New("runtime executable file could not be observed consistently")

// RuntimeExecutableObservation hashes the executable file referenced by the
// live process's kernel mm. It is not loaded-memory attestation, image approval,
// configuration authentication or proof that this executable sent Payload().
// The same process/pidfd can survive exec, including unobserved ABA changes.
type RuntimeExecutableObservation struct {
	Process         RuntimeCallerObservation `json:"process"`
	Digest          [sha256.Size]byte        `json:"digest"`
	SizeBytes       int64                    `json:"size_bytes"`
	FileDevice      uint64                   `json:"file_device"`
	FileInode       uint64                   `json:"file_inode"`
	FileUID         uint32                   `json:"file_uid"`
	FileGID         uint32                   `json:"file_gid"`
	FileMode        uint32                   `json:"file_mode"`
	FileLinks       uint64                   `json:"file_links"`
	ObservedFrom    time.Time                `json:"observed_from"`
	ObservedThrough time.Time                `json:"observed_through"`
}

func (caller *RuntimeCaller) InspectExecutable(ctx context.Context) (RuntimeExecutableObservation, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeExecutableObservation{}, err
	}
	if caller == nil {
		return RuntimeExecutableObservation{}, ErrRuntimeCallerIdentity
	}
	ctx, cancel := context.WithTimeout(ctx, runtimeCallerTimeout)
	defer cancel()
	from := time.Now().UTC()
	caller.mu.Lock()
	defer caller.mu.Unlock()
	first, err := caller.inspectLocked(ctx)
	if err != nil {
		return RuntimeExecutableObservation{}, err
	}
	directory, err := caller.process.Open(".")
	if err != nil {
		return RuntimeExecutableObservation{}, err
	}
	defer func() { _ = directory.Close() }()
	file, err := openRuntimeExecutable(directory)
	if err != nil {
		return RuntimeExecutableObservation{}, err
	}
	defer func() { _ = file.Close() }()
	identity, err := runtimeExecutableIdentity(file)
	if err != nil {
		return RuntimeExecutableObservation{}, err
	}
	digest := sha256.New()
	count, err := io.Copy(digest, io.LimitReader(&runtimeExecutableReader{ctx: ctx, file: file}, identity.size+1))
	if err != nil || count != identity.size {
		return RuntimeExecutableObservation{}, errors.Join(ErrRuntimeExecutable, err)
	}
	lastIdentity, err := runtimeExecutableIdentity(file)
	if err != nil || identity != lastIdentity {
		return RuntimeExecutableObservation{}, errors.Join(ErrRuntimeExecutable, err)
	}
	current, err := openRuntimeExecutable(directory)
	if err != nil {
		return RuntimeExecutableObservation{}, err
	}
	defer func() { _ = current.Close() }()
	currentIdentity, err := runtimeExecutableIdentity(current)
	if err != nil || identity != currentIdentity {
		return RuntimeExecutableObservation{}, errors.Join(ErrRuntimeExecutable, err)
	}
	last, err := caller.inspectLocked(ctx)
	if err != nil {
		return RuntimeExecutableObservation{}, err
	}
	first.ObservedAt = last.ObservedAt
	through := time.Now().UTC()
	if first != last || through.Before(from) || through.Sub(from) > runtimeCallerTimeout {
		return RuntimeExecutableObservation{}, ErrRuntimeExecutable
	}
	if err := ctx.Err(); err != nil {
		return RuntimeExecutableObservation{}, err
	}
	result := RuntimeExecutableObservation{Process: last, SizeBytes: count, FileDevice: identity.device,
		FileInode: identity.inode, FileUID: identity.uid, FileGID: identity.gid, FileMode: identity.mode,
		FileLinks: identity.links, ObservedFrom: from, ObservedThrough: through}
	copy(result.Digest[:], digest.Sum(nil))
	return result, nil
}

func openRuntimeExecutable(directory *os.File) (*os.File, error) {
	var filesystem unix.Statfs_t
	if err := unix.Fstatfs(int(directory.Fd()), &filesystem); err != nil || filesystem.Type != unix.PROC_SUPER_MAGIC {
		return nil, errors.Join(ErrRuntimeExecutable, err)
	}
	// This one fixed procfs magic link intentionally leaves os.Root. Anchor it
	// to the retained process directory; never reopen a numeric /proc/PID path
	// or resolve the executable's replaceable pathname in a container mount.
	fd, err := unix.Openat(int(directory.Fd()), "exe", unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, errors.Join(ErrRuntimeExecutable, err)
	}
	return os.NewFile(uintptr(fd), "runtime-executable"), nil
}

type runtimeExecutableFileIdentity struct {
	device, inode, links uint64
	uid, gid, mode       uint32
	size                 int64
	modified, changed    unix.Timespec
}

func runtimeExecutableIdentity(file *os.File) (runtimeExecutableFileIdentity, error) {
	var info unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &info); err != nil {
		return runtimeExecutableFileIdentity{}, err
	}
	if info.Mode&unix.S_IFMT != unix.S_IFREG || info.Size <= 0 || info.Size > maximumRuntimeExecutableBytes {
		return runtimeExecutableFileIdentity{}, ErrRuntimeExecutable
	}
	return runtimeExecutableFileIdentity{device: info.Dev, inode: info.Ino, links: uint64(info.Nlink),
		uid: info.Uid, gid: info.Gid, mode: info.Mode, size: info.Size, modified: info.Mtim, changed: info.Ctim}, nil
}

type runtimeExecutableReader struct {
	ctx  context.Context
	file *os.File
}

func (reader *runtimeExecutableReader) Read(output []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.file.Read(output)
}
