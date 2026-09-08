package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/vivym/vela/internal/modelruntime"
	"golang.org/x/sys/unix"
)

var ErrRuntimeStartupPublication = errors.New("runtime caller does not see the original published bootstrap through a protected mount")

// Paths are trusted Node deployment inputs. Directory is the Node publication
// view; BootstrapPath is the expected path inside the original caller's root.
type RuntimeStartupPublicationConfig struct {
	Directory     string
	BootstrapPath string
}

// RuntimeStartupBootstrapObservation describes a sampled effective file/mount,
// not the configuration previously consumed by the process or a startup permit.
// It retains publication identity in the Node/Fleet owner observation digest.
type RuntimeStartupBootstrapObservation struct {
	Publication    RuntimeBootstrapPublicationRecord `json:"publication"`
	BootstrapPath  string                            `json:"bootstrap_path"`
	MountNamespace string                            `json:"mount_namespace"`
	MountID        uint64                            `json:"mount_id"`
}

// ReservePublishedImageRemote checks the Node-published artifact through the
// original caller's filesystem view and binds it to the same held journal,
// startup request and approved image used by the single Fleet reservation.
// No historical publication/receipt grants startup. argv/env consumption,
// execution continuity and once-only permission remain separate requirements.
func (ledger *RuntimeStartupLedger) ReservePublishedImageRemote(ctx context.Context, config RuntimeStartupReservationConfig, image RuntimeStartupImageConfig, publication RuntimeStartupPublicationConfig) (RuntimeStartupReservationRecord, error) {
	if image.Images == nil || !validRuntimeBootstrapPath(publication.BootstrapPath) || publication.Directory == "" {
		return RuntimeStartupReservationRecord{}, ErrRuntimeStartupPublication
	}
	config.publication = &publication
	return ledger.reserveRemote(ctx, config, &runtimeStartupImageCheck{config: image})
}

func validRuntimeBootstrapPath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && path != "/" && len(path) <= 4096 && !strings.ContainsRune(path, '\x00')
}

func inspectStartupPublication(ctx context.Context, config RuntimeStartupReservationConfig) (*RuntimeStartupBootstrapObservation, error) {
	if config.publication == nil {
		return nil, nil
	}
	publication, err := InspectRuntimeBootstrapPublication(ctx, config.publication.Directory)
	if err != nil {
		return nil, err
	}
	bootstrap, err := publication.Bootstrap()
	if err != nil {
		return nil, err
	}
	request, err := matchStartupBootstrapConsumption(ctx, config, publication, bootstrap)
	if err != nil {
		return nil, err
	}
	observation, err := config.Caller.inspectPublishedBootstrap(ctx, publication, config.publication.BootstrapPath)
	if err != nil {
		return nil, err
	}
	// Potentially blocking process-root I/O must not hide loss/replacement of
	// Node's publication or live journal. Repeat both after the remote read.
	current, err := InspectRuntimeBootstrapPublication(ctx, config.publication.Directory)
	if err != nil || current.Record() != publication.Record() {
		return nil, errors.Join(ErrRuntimeStartupPublication, err)
	}
	if err := config.Journal.RequireRootCustody(ctx); err != nil {
		return nil, err
	}
	if _, err := config.Journal.InspectStartup(ctx, bootstrap.Manifest, request); err != nil {
		return nil, err
	}
	return &observation, nil
}

// This comparison binds a consumption declaration, not the effective mount or
// caller's executable. The enclosing reservation must check those separately.
func matchStartupBootstrapConsumption(ctx context.Context, config RuntimeStartupReservationConfig, publication *RuntimeBootstrapPublication, bootstrap modelruntime.RemoteRuntimeBootstrap) (modelruntime.BackendStartupRequest, error) {
	// Rebuild from the actual plan/owner, including its actual public verifier
	// keys. Reusing a same-shaped publication for another journal is rejected.
	wire, err := buildRuntimeBootstrap(ctx, RuntimeBootstrapPublicationConfig{Plan: config.Plan, Journal: config.Journal, RegistryKeys: bootstrap.RegistryKeys,
		JournalSocket: bootstrap.JournalSocket, StartupSocket: bootstrap.StartupSocket, RuntimeSocket: bootstrap.RuntimeSocket,
		JournalTimeout: bootstrap.JournalTimeout, CancelTimeout: bootstrap.CancelTimeout, ShutdownTimeout: bootstrap.ShutdownTimeout})
	if err != nil || !bytes.Equal(wire, publication.encoded) {
		return modelruntime.BackendStartupRequest{}, errors.Join(ErrRuntimeStartupPublication, err)
	}
	request, _, err := parseRuntimeStartupPlan(config.Plan, config.Caller.Payload())
	if err != nil || request.IncarnationID != bootstrap.Startup.IncarnationID || request.SchemaVersion != 2 || request.BootstrapDigest != publication.Record().BootstrapDigest || request.BootstrapPath != config.publication.BootstrapPath {
		return modelruntime.BackendStartupRequest{}, errors.Join(ErrRuntimeStartupPublication, err)
	}
	return request, nil
}

func (caller *RuntimeCaller) inspectPublishedBootstrap(ctx context.Context, publication *RuntimeBootstrapPublication, path string) (RuntimeStartupBootstrapObservation, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeStartupBootstrapObservation{}, err
	}
	if caller == nil || publication == nil || !validRuntimeBootstrapPath(path) {
		return RuntimeStartupBootstrapObservation{}, ErrRuntimeStartupPublication
	}
	caller.mu.Lock()
	defer caller.mu.Unlock()
	first, err := caller.inspectLocked(ctx)
	if err != nil {
		return RuntimeStartupBootstrapObservation{}, err
	}
	namespace, err := caller.process.Readlink("ns/mnt")
	if err != nil {
		return RuntimeStartupBootstrapObservation{}, err
	}
	file, err := openCallerBootstrap(caller.process, path)
	if err != nil {
		return RuntimeStartupBootstrapObservation{}, err
	}
	defer func() { _ = file.Close() }()
	mount, err := runtimeBootstrapReadOnlyMount(file)
	if err != nil {
		return RuntimeStartupBootstrapObservation{}, err
	}
	record := publication.Record()
	content, info, err := readRuntimePublicationFile(ctx, file, record.RuntimeGID, 0o440, modelruntime.MaximumRemoteBootstrapBytes)
	if err != nil || record.BootstrapFile != (runtimeStartupFileIdentity{Device: info.Dev, Inode: info.Ino}) || record.BootstrapDigest != sha256.Sum256(content) || record.BootstrapBytes != int64(len(content)) {
		return RuntimeStartupBootstrapObservation{}, errors.Join(ErrRuntimeStartupPublication, err)
	}
	// Reopen from the original proc root, not a numeric PID or Node pathname.
	// This observes current path resolution rather than trusting the old FD.
	current, err := openCallerBootstrap(caller.process, path)
	if err != nil {
		return RuntimeStartupBootstrapObservation{}, err
	}
	defer func() { _ = current.Close() }()
	currentMount, err := runtimeBootstrapReadOnlyMount(current)
	if err != nil || currentMount != mount {
		return RuntimeStartupBootstrapObservation{}, errors.Join(ErrRuntimeStartupPublication, err)
	}
	var last unix.Stat_t
	if err := unix.Fstat(int(current.Fd()), &last); err != nil {
		return RuntimeStartupBootstrapObservation{}, err
	}
	last.Atim = info.Atim
	if last != info {
		return RuntimeStartupBootstrapObservation{}, ErrRuntimeStartupPublication
	}
	lastNamespace, err := caller.process.Readlink("ns/mnt")
	if err != nil || lastNamespace != namespace {
		return RuntimeStartupBootstrapObservation{}, errors.Join(ErrRuntimeStartupPublication, err)
	}
	lastProcess, err := caller.inspectLocked(ctx)
	first.ObservedAt = lastProcess.ObservedAt
	if err != nil || first != lastProcess {
		return RuntimeStartupBootstrapObservation{}, errors.Join(ErrRuntimeStartupPublication, err)
	}
	return RuntimeStartupBootstrapObservation{Publication: record, BootstrapPath: path, MountNamespace: namespace, MountID: mount}, nil
}

func openCallerBootstrap(process *os.Root, path string) (*os.File, error) {
	// The fixed procfs root link deliberately enters the caller's mount view.
	// All further components reject symlinks; O_PATH probes the final inode
	// before the existing pinned-inode reader can open its regular-file data.
	anchor, err := process.Open(".")
	if err != nil {
		return nil, err
	}
	defer func() { _ = anchor.Close() }()
	var procfs unix.Statfs_t
	if err := unix.Fstatfs(int(anchor.Fd()), &procfs); err != nil || procfs.Type != unix.PROC_SUPER_MAGIC {
		return nil, errors.Join(ErrRuntimeStartupPublication, err)
	}
	fd, err := unix.Openat(int(anchor.Fd()), "root", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	directory := os.NewFile(uintptr(fd), "caller-root")
	defer func() { _ = directory.Close() }()
	components := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, component := range components {
		var info unix.Stat_t
		if err := unix.Fstat(int(directory.Fd()), &info); err != nil || info.Uid != 0 || info.Mode&0o7022 != 0 {
			return nil, errors.Join(ErrRuntimeStartupPublication, err)
		}
		if i == len(components)-1 {
			return openRuntimePublicationFile(directory, component)
		}
		next, err := unix.Openat(int(directory.Fd()), component, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, err
		}
		_ = directory.Close()
		directory = os.NewFile(uintptr(next), "caller-bootstrap-directory")
	}
	return nil, ErrRuntimeStartupPublication
}

func runtimeBootstrapReadOnlyMount(file *os.File) (uint64, error) {
	var filesystem unix.Statfs_t
	if err := unix.Fstatfs(int(file.Fd()), &filesystem); err != nil || filesystem.Flags&unix.ST_RDONLY == 0 {
		return 0, errors.Join(ErrRuntimeStartupPublication, err)
	}
	var info unix.Statx_t
	if err := unix.Statx(int(file.Fd()), "", unix.AT_EMPTY_PATH|unix.AT_STATX_SYNC_AS_STAT, unix.STATX_MNT_ID, &info); err != nil || info.Mask&unix.STATX_MNT_ID == 0 || info.Mnt_id == 0 {
		return 0, errors.Join(ErrRuntimeStartupPublication, err)
	}
	return info.Mnt_id, nil
}
