package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
	"golang.org/x/sys/unix"
)

var ErrRuntimeTaskLaunch = errors.New("runtime task launch bundle is missing, untrusted or inconsistent")

// RuntimeTaskLaunch is the supported containerd task's original bundle content.
// It does not approve that content, loaded memory, effective mounts or a grant.
// Configuration returns a copy; the retained bytes cannot be changed by callers.
type RuntimeTaskLaunch struct {
	Caller          RuntimeContainerCallerObservation
	ConfigDigest    [sha256.Size]byte
	ConfigBytes     int64
	BundleDevice    uint64
	BundleInode     uint64
	ConfigDevice    uint64
	ConfigInode     uint64
	ObservedFrom    time.Time
	ObservedThrough time.Time
	encoded         []byte
}

func (launch *RuntimeTaskLaunch) Configuration() (specs.Spec, error) {
	if launch == nil || len(launch.encoded) == 0 {
		return specs.Spec{}, ErrRuntimeTaskLaunch
	}
	var configuration specs.Spec
	err := json.Unmarshal(launch.encoded, &configuration)
	return configuration, err
}

// ObserveTaskLaunch reads config.json from containerd's task bundle, not mutable
// Containers.Get.spec/CRI verbose metadata. stateDirectory is trusted Node
// configuration naming THIS daemon's state root in the Node's mount view; it
// must never come from a workload. This adapter supports rootful runc-v2 tasks
// without remapped bundle ownership. Root administrators remain trusted.
func (observer *RuntimeContainerObserver) ObserveTaskLaunch(ctx context.Context, stateDirectory string, target RuntimeContainerTarget, caller *RuntimeCaller) (*RuntimeTaskLaunch, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if os.Geteuid() != 0 || !filepath.IsAbs(stateDirectory) || filepath.Clean(stateDirectory) != stateDirectory || strings.ContainsRune(stateDirectory, '\x00') {
		return nil, ErrRuntimeTaskLaunch
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	from := time.Now().UTC()
	first, err := observer.ObserveCaller(ctx, target, caller)
	if err != nil {
		return nil, err
	}
	bundlePath := filepath.Join(stateDirectory, "io.containerd.runtime.v2.task", "k8s.io", target.ContainerID)
	bundle, err := securefile.OpenTrustedDirectory(bundlePath)
	if err != nil {
		return nil, errors.Join(ErrRuntimeTaskLaunch, err)
	}
	defer func() { _ = bundle.Close() }()
	var directory unix.Stat_t
	if err := unix.Fstat(int(bundle.Fd()), &directory); err != nil || directory.Uid != 0 || directory.Gid != 0 || directory.Mode != unix.S_IFDIR|0o700 {
		return nil, errors.Join(ErrRuntimeTaskLaunch, err)
	}
	config, configIdentity, err := readRuntimeTaskBundleFile(ctx, bundle, "config.json", 1<<20)
	if err != nil {
		return nil, err
	}
	pidBytes, pidIdentity, err := readRuntimeTaskBundleFile(ctx, bundle, "init.pid", 16)
	if err != nil {
		return nil, err
	}
	pid, err := strconv.ParseUint(strings.TrimSpace(string(pidBytes)), 10, 32)
	if err != nil || pid == 0 || pid != uint64(first.Process.HostPID) {
		return nil, errors.Join(ErrRuntimeTaskLaunch, err)
	}
	var configuration specs.Spec
	decoder := json.NewDecoder(bytes.NewReader(config))
	decoder.DisallowUnknownFields()
	if strictjson.RejectDuplicateKeys(config) != nil || decoder.Decode(&configuration) != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) ||
		configuration.Version != specs.Version || configuration.Process == nil || configuration.Root == nil || configuration.Linux == nil {
		return nil, ErrRuntimeTaskLaunch
	}
	last, err := observer.ObserveCaller(ctx, target, caller)
	if err != nil {
		return nil, err
	}
	comparableFirst, comparableLast := first, last
	for _, observed := range []*RuntimeContainerCallerObservation{&comparableFirst, &comparableLast} {
		observed.ObservedFrom, observed.ObservedThrough = time.Time{}, time.Time{}
		observed.Container.ObservedFrom, observed.Container.ObservedThrough = time.Time{}, time.Time{}
		observed.Process.ObservedAt = time.Time{}
	}
	if comparableFirst != comparableLast {
		return nil, ErrRuntimeTaskLaunch
	}
	// Retain the directory inode through both reads and re-open its trusted
	// pathname to reject visible deletion/replacement or changed ancestors.
	current, err := securefile.OpenTrustedDirectory(bundlePath)
	if err != nil {
		return nil, errors.Join(ErrRuntimeTaskLaunch, err)
	}
	defer func() { _ = current.Close() }()
	var currentDirectory unix.Stat_t
	if err := unix.Fstat(int(current.Fd()), &currentDirectory); err != nil || directory.Dev != currentDirectory.Dev || directory.Ino != currentDirectory.Ino ||
		directory.Mode != currentDirectory.Mode || directory.Uid != currentDirectory.Uid || directory.Gid != currentDirectory.Gid {
		return nil, errors.Join(ErrRuntimeTaskLaunch, err)
	}
	lastConfig, lastIdentity, err := readRuntimeTaskBundleFile(ctx, bundle, "config.json", 1<<20)
	if err != nil || configIdentity != lastIdentity || !bytes.Equal(config, lastConfig) {
		return nil, errors.Join(ErrRuntimeTaskLaunch, err)
	}
	lastPID, lastPIDIdentity, err := readRuntimeTaskBundleFile(ctx, bundle, "init.pid", 16)
	if err != nil || pidIdentity != lastPIDIdentity || !bytes.Equal(pidBytes, lastPID) {
		return nil, errors.Join(ErrRuntimeTaskLaunch, err)
	}
	if err := errors.Join(observer.check(), ctx.Err()); err != nil {
		return nil, err
	}
	finalProcess, err := caller.Inspect(ctx)
	expectedProcess := last.Process
	expectedProcess.ObservedAt = finalProcess.ObservedAt
	if err != nil || expectedProcess != finalProcess {
		return nil, errors.Join(ErrRuntimeTaskLaunch, err)
	}
	return &RuntimeTaskLaunch{Caller: last, ConfigDigest: sha256.Sum256(config), ConfigBytes: int64(len(config)),
		BundleDevice: directory.Dev, BundleInode: directory.Ino, ConfigDevice: configIdentity.device, ConfigInode: configIdentity.inode,
		ObservedFrom: from, ObservedThrough: time.Now().UTC(), encoded: config}, nil
}

type runtimeTaskFileIdentity struct {
	device, inode uint64
	size          int64
	mode          uint32
	modified      unix.Timespec
	changed       unix.Timespec
}

func readRuntimeTaskBundleFile(ctx context.Context, bundle *os.File, name string, limit int64) ([]byte, runtimeTaskFileIdentity, error) {
	if err := ctx.Err(); err != nil {
		return nil, runtimeTaskFileIdentity{}, err
	}
	// Inspect an O_PATH inode before opening data. A wrong file type must not
	// open a device or wait for a FIFO as a side effect of validation.
	fd, err := unix.Openat(int(bundle.Fd()), name, unix.O_PATH|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, runtimeTaskFileIdentity{}, errors.Join(ErrRuntimeTaskLaunch, err)
	}
	file := os.NewFile(uintptr(fd), "containerd-task-launch")
	defer func() { _ = file.Close() }()
	identity := func() (runtimeTaskFileIdentity, error) {
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != 0 || stat.Gid != 0 ||
			stat.Mode&0o7022 != 0 || stat.Nlink != 1 || stat.Size <= 0 || stat.Size > limit {
			return runtimeTaskFileIdentity{}, errors.Join(ErrRuntimeTaskLaunch, err)
		}
		return runtimeTaskFileIdentity{stat.Dev, stat.Ino, stat.Size, stat.Mode, stat.Mtim, stat.Ctim}, nil
	}
	first, err := identity()
	if err != nil {
		return nil, runtimeTaskFileIdentity{}, err
	}
	descriptors, err := os.Open("/proc/self/fd")
	if err != nil {
		return nil, runtimeTaskFileIdentity{}, err
	}
	defer func() { _ = descriptors.Close() }()
	var filesystem unix.Statfs_t
	if err := unix.Fstatfs(int(descriptors.Fd()), &filesystem); err != nil || filesystem.Type != unix.PROC_SUPER_MAGIC {
		return nil, runtimeTaskFileIdentity{}, errors.Join(ErrRuntimeTaskLaunch, err)
	}
	readFD, err := unix.Openat(int(descriptors.Fd()), strconv.Itoa(fd), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK|unix.O_NOCTTY, 0)
	if err != nil {
		return nil, runtimeTaskFileIdentity{}, err
	}
	reader := os.NewFile(uintptr(readFD), "containerd-task-launch-data")
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	last, lastErr := identity()
	if err != nil || lastErr != nil || first != last || int64(len(data)) != first.size || ctx.Err() != nil {
		return nil, runtimeTaskFileIdentity{}, errors.Join(ErrRuntimeTaskLaunch, err, lastErr, ctx.Err())
	}
	return data, first, nil
}
