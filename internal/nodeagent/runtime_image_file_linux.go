package nodeagent

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"slices"
	"strconv"

	"github.com/containerd/containerd/api/types"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

func validRuntimeNativeView(mounts []*types.Mount) bool {
	if len(mounts) != 1 || mounts[0] == nil {
		return false
	}
	mount := mounts[0]
	return mount.Type == "bind" && mount.Target == "" && validRuntimeImagePath(mount.Source) &&
		len(mount.Options) == 2 && slices.Contains(mount.Options, "ro") && slices.Contains(mount.Options, "rbind")
}

func openRuntimeImageActivation(key string, mounts []*types.Mount, info *types.ActivationInfo) (*os.File, error) {
	if !validRuntimeNativeView(mounts) || info == nil || info.Name != key || len(info.Active) != 1 || len(info.System) != 1 ||
		info.Active[0] == nil || info.System[0] == nil || !proto.Equal(info.Active[0].Mount, mounts[0]) ||
		info.Active[0].MountedAt == nil || info.Active[0].MountedAt.CheckValid() != nil || len(info.Active[0].Data) != 0 ||
		info.System[0].Type != "bind" || info.System[0].Target != "" || !slices.Equal(info.System[0].Options, []string{"rbind"}) ||
		info.System[0].Source != info.Active[0].MountPoint || !validRuntimeImagePath(info.Active[0].MountPoint) {
		return nil, errors.New("image activation is not a complete materialized native view")
	}
	fd, err := unix.Openat2(unix.AT_FDCWD, info.Active[0].MountPoint, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		return nil, err
	}
	root := os.NewFile(uintptr(fd), "runtime-image-view")
	if err := requireReadOnlyRuntimeImage(root); err != nil {
		_ = root.Close()
		return nil, err
	}
	return root, nil
}

func runtimeImagePersistedActivation(info *types.ActivationInfo) *types.ActivationInfo {
	// containerd 2.3.1 putActiveMount persists Type, MountPoint and MountedAt,
	// but explicitly omits Source, Target and Options from the active mount.
	// The initial Activate response was validated against the snapshot view;
	// System may be reconstructed only by Activate on other daemon versions.
	stored := proto.CloneOf(info)
	for _, active := range stored.Active {
		active.Mount.Source, active.Mount.Target, active.Mount.Options = "", "", nil
	}
	return stored
}

func sameRuntimeImageActivation(initial, current *types.ActivationInfo) bool {
	if initial == nil || current == nil {
		return false
	}
	expected := runtimeImagePersistedActivation(initial)
	if len(current.System) == 0 {
		expected.System = nil
	}
	return proto.Equal(expected, current)
}

func requireReadOnlyRuntimeImage(file *os.File) error {
	var filesystem unix.Statfs_t
	if err := unix.Fstatfs(int(file.Fd()), &filesystem); err != nil || filesystem.Flags&unix.ST_RDONLY == 0 {
		return errors.Join(errors.New("image observation requires a kernel read-only view"), err)
	}
	return nil
}

func openRuntimeImageFile(root *os.File, path string) (*os.File, error) {
	// Resolve absolute and relative image symlinks inside the retained root.
	// A nested mount or proc magic link cannot redirect a read into the host.
	fd, err := unix.Openat2(int(root.Fd()), path, &unix.OpenHow{Flags: unix.O_PATH | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_IN_ROOT | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_XDEV})
	if err != nil {
		return nil, err
	}
	pinned := os.NewFile(uintptr(fd), "runtime-image-executable-path")
	defer func() { _ = pinned.Close() }()
	identity, err := runtimeExecutableIdentity(pinned)
	if err != nil || identity.mode&0o111 == 0 {
		return nil, errors.Join(ErrRuntimeImage, err)
	}
	// O_PATH checks type without opening an image device or FIFO. This fixed
	// host procfs directory reopens only our retained regular-file descriptor.
	descriptors, err := os.Open("/proc/self/fd")
	if err != nil {
		return nil, err
	}
	defer func() { _ = descriptors.Close() }()
	var filesystem unix.Statfs_t
	if err := unix.Fstatfs(int(descriptors.Fd()), &filesystem); err != nil || filesystem.Type != unix.PROC_SUPER_MAGIC {
		return nil, errors.Join(ErrRuntimeImage, err)
	}
	opened, err := unix.Openat(int(descriptors.Fd()), strconv.Itoa(fd), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK|unix.O_NOCTTY, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(opened), "runtime-image-executable")
	current, err := runtimeExecutableIdentity(file)
	if err != nil || current != identity {
		_ = file.Close()
		return nil, errors.Join(ErrRuntimeImage, err)
	}
	return file, nil
}

func measureRuntimeImageExecutable(ctx context.Context, root *os.File, path string) ([sha256.Size]byte, runtimeExecutableFileIdentity, error) {
	var hash [sha256.Size]byte
	var identity runtimeExecutableFileIdentity
	if err := errors.Join(ctx.Err(), requireReadOnlyRuntimeImage(root)); err != nil {
		return hash, identity, err
	}
	file, err := openRuntimeImageFile(root, path)
	if err != nil {
		return hash, identity, err
	}
	defer func() { _ = file.Close() }()
	identity, err = runtimeExecutableIdentity(file)
	if err != nil || identity.mode&0o111 == 0 {
		return hash, identity, errors.Join(ErrRuntimeImage, err)
	}
	hasher := sha256.New()
	count, err := io.Copy(hasher, io.LimitReader(&runtimeExecutableReader{ctx: ctx, file: file}, identity.size+1))
	if err != nil || count != identity.size {
		return hash, identity, errors.Join(ErrRuntimeImage, err)
	}
	last, err := runtimeExecutableIdentity(file)
	if err != nil || last != identity {
		return hash, identity, errors.Join(ErrRuntimeImage, err)
	}
	current, err := openRuntimeImageFile(root, path)
	if err != nil {
		return hash, identity, err
	}
	defer func() { _ = current.Close() }()
	currentIdentity, err := runtimeExecutableIdentity(current)
	if err != nil || currentIdentity != identity {
		return hash, identity, errors.Join(ErrRuntimeImage, err)
	}
	if err := errors.Join(ctx.Err(), requireReadOnlyRuntimeImage(root), requireReadOnlyRuntimeImage(file)); err != nil {
		return hash, identity, err
	}
	copy(hash[:], hasher.Sum(nil))
	return hash, identity, nil
}
