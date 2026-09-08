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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

const (
	runtimeBootstrapFilename   = "bootstrap.json"
	runtimeBootstrapPending    = "bootstrap.pending"
	runtimeBootstrapRecordName = "publication.json"
)

var ErrRuntimeBootstrapPublication = errors.New("runtime bootstrap publication is missing, changed, busy or incomplete")

// Settings are trusted Node deployment inputs, not Runtime request fields.
// The manifest, signed binding, identity and incarnation cannot be supplied here.
type RuntimeBootstrapPublicationConfig struct {
	Directory                                      string
	Plan                                           *RuntimeLaunchPlan
	Journal                                        *modelruntime.ExecutionJournalOwner
	RegistryKeys                                   map[string][]byte
	JournalSocket, StartupSocket, RuntimeSocket    string
	JournalTimeout, CancelTimeout, ShutdownTimeout time.Duration
}

type RuntimeBootstrapPublicationRecord struct {
	SchemaVersion   int                        `json:"schema_version"`
	ID              uuid.UUID                  `json:"publication_id"`
	NodeIdentity    string                     `json:"node_identity"`
	Directory       runtimeStartupFileIdentity `json:"directory"`
	RecordFile      runtimeStartupFileIdentity `json:"record_file"`
	BootstrapFile   runtimeStartupFileIdentity `json:"bootstrap_file"`
	BootstrapBytes  int64                      `json:"bootstrap_bytes"`
	BootstrapDigest [sha256.Size]byte          `json:"bootstrap_digest"`
	BindingDigest   [sha256.Size]byte          `json:"binding_digest"`
	RuntimeGID      uint32                     `json:"runtime_gid"`
	RecordedAt      time.Time                  `json:"recorded_at"`
}

// RuntimeBootstrapPublication is a historical file observation, never a permit
// or original process handle. Reopening does not authorize another incarnation.
type RuntimeBootstrapPublication struct {
	record  RuntimeBootstrapPublicationRecord
	encoded []byte
}

func (publication *RuntimeBootstrapPublication) Record() RuntimeBootstrapPublicationRecord {
	if publication == nil {
		return RuntimeBootstrapPublicationRecord{}
	}
	return publication.record
}
func (publication *RuntimeBootstrapPublication) Bootstrap() (modelruntime.RemoteRuntimeBootstrap, error) {
	if publication == nil {
		return modelruntime.RemoteRuntimeBootstrap{}, ErrRuntimeBootstrapPublication
	}
	return modelruntime.ParseRemoteRuntimeBootstrap(publication.encoded)
}

// PublishRuntimeBootstrap only uses an empty root:root 0700 directory. A private
// intent and fixed pending inode precede publication. There is no replace, retry,
// repair or journal-initialization path. Errors retain any partial artifacts.
func PublishRuntimeBootstrap(ctx context.Context, config RuntimeBootstrapPublicationConfig) (*RuntimeBootstrapPublication, error) {
	return publishRuntimeBootstrap(ctx, config, nil)
}

func publishRuntimeBootstrap(ctx context.Context, config RuntimeBootstrapPublicationConfig, boundary func(string) error) (result *RuntimeBootstrapPublication, resultErr error) {
	checkpoint := func(name string) error {
		if err := contextError(ctx); err != nil {
			return err
		}
		if boundary != nil {
			return boundary(name)
		}
		return nil
	}
	if err := checkpoint("preflight"); err != nil {
		return nil, err
	}
	wire, err := buildRuntimeBootstrap(ctx, config)
	if err != nil {
		return nil, err
	}
	directory, err := openRuntimePublicationDirectory(config.Directory)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, directory.Close())
		if resultErr != nil {
			result = nil
		}
	}()
	info, err := directory.Stat()
	if err != nil || !runtimeStartupPrivate(info, true) {
		return nil, errors.Join(ErrRuntimeBootstrapPublication, err)
	}
	if err := runtimePublicationNames(directory, nil); err != nil {
		return nil, err
	}
	create := func(name string) (*os.File, error) {
		fd, err := unix.Openat(int(directory.Fd()), name, unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
		if err != nil {
			return nil, err
		}
		return os.NewFile(uintptr(fd), name), nil
	}
	marker, err := create(runtimeBootstrapRecordName)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, marker.Close())
		if resultErr != nil {
			result = nil
		}
	}()
	if err := unix.Flock(int(marker.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, err
	}
	if err := checkpoint("intent-created"); err != nil {
		return nil, err
	}
	file, err := create(runtimeBootstrapPending)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, file.Close())
		if resultErr != nil {
			result = nil
		}
	}()
	if _, err := file.Write(wire); err != nil {
		return nil, err
	}
	if err := file.Chown(0, int(config.Plan.gid)); err != nil {
		return nil, err
	}
	if err := file.Chmod(0o440); err != nil {
		return nil, err
	}
	if err := file.Sync(); err != nil {
		return nil, err
	}
	if err := checkpoint("bootstrap-synced"); err != nil {
		return nil, err
	}
	markerInfo, err := marker.Stat()
	if err != nil {
		return nil, err
	}
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	binding, err := proto.MarshalOptions{Deterministic: true}.Marshal(config.Plan.binding)
	if err != nil {
		return nil, err
	}
	record := RuntimeBootstrapPublicationRecord{SchemaVersion: 1, ID: uuid.New(), NodeIdentity: config.Plan.binding.Claim.NodeIdentity,
		Directory: runtimeStartupIdentity(info), RecordFile: runtimeStartupIdentity(markerInfo), BootstrapFile: runtimeStartupIdentity(fileInfo),
		BootstrapBytes: int64(len(wire)), BootstrapDigest: sha256.Sum256(wire), BindingDigest: sha256.Sum256(binding), RuntimeGID: config.Plan.gid, RecordedAt: time.Now().UTC()}
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if _, err := marker.Write(encoded); err != nil {
		return nil, err
	}
	if err := errors.Join(marker.Sync(), directory.Sync()); err != nil {
		return nil, err
	}
	if err := checkpoint("intent-synced"); err != nil {
		return nil, err
	}
	current, err := buildRuntimeBootstrap(ctx, config)
	if err != nil || !bytes.Equal(current, wire) {
		return nil, errors.Join(ErrRuntimeBootstrapPublication, err)
	}
	if err := unix.Renameat2(int(directory.Fd()), runtimeBootstrapPending, int(directory.Fd()), runtimeBootstrapFilename, unix.RENAME_NOREPLACE); err != nil {
		return nil, err
	}
	if err := directory.Sync(); err != nil {
		return nil, err
	}
	if err := checkpoint("renamed"); err != nil {
		return nil, err
	}
	// Validate the durable files while the Runtime still cannot traverse the
	// directory. A corrupt pending inode must never be exposed as configuration.
	if _, err := inspectRuntimeBootstrapFiles(ctx, directory, marker, false); err != nil {
		return nil, err
	}
	if err := runtimePublicationDirectoryBound(config.Directory, directory); err != nil {
		return nil, err
	}
	// Filesystem validation can block. Recheck custody after it, before making
	// the directory traversable by the Runtime. Publication is still no grant.
	current, err = buildRuntimeBootstrap(ctx, config)
	if err != nil || !bytes.Equal(current, wire) {
		return nil, errors.Join(ErrRuntimeBootstrapPublication, err)
	}
	if err := directory.Chown(0, int(config.Plan.gid)); err != nil {
		return nil, err
	}
	if err := directory.Chmod(0o750); err != nil {
		return nil, err
	}
	if err := directory.Sync(); err != nil {
		return nil, err
	}
	if err := checkpoint("exposed"); err != nil {
		return nil, err
	}
	result, err = inspectRuntimeBootstrapFiles(ctx, directory, marker, true)
	if err != nil {
		return nil, err
	}
	current, err = buildRuntimeBootstrap(ctx, config)
	if err != nil || !bytes.Equal(current, wire) {
		return nil, errors.Join(ErrRuntimeBootstrapPublication, err)
	}
	if err := checkpoint("verified"); err != nil {
		return nil, err
	}
	if err := runtimePublicationDirectoryBound(config.Directory, directory); err != nil {
		return nil, err
	}
	return result, nil
}

func buildRuntimeBootstrap(ctx context.Context, config RuntimeBootstrapPublicationConfig) ([]byte, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if os.Geteuid() != 0 || os.Getegid() != 0 || config.Plan == nil || config.Plan.binding == nil {
		return nil, ErrRuntimeBootstrapPublication
	}
	if err := config.Journal.RequireRootCustody(ctx); err != nil {
		return nil, err
	}
	status, err := config.Journal.Status(ctx)
	if err != nil {
		return nil, err
	}
	keys, err := config.Journal.AuthorityVerifierKeys(ctx)
	if err != nil {
		return nil, err
	}
	var manifest modelruntime.LaunchManifest
	if err := json.Unmarshal(config.Plan.manifest, &manifest); err != nil {
		return nil, err
	}
	binding, err := proto.MarshalOptions{Deterministic: true}.Marshal(config.Plan.binding)
	if err != nil {
		return nil, err
	}
	bootstrap := modelruntime.RemoteRuntimeBootstrap{SchemaVersion: 1, Manifest: manifest, AuthorityKeys: keys, RegistryKeys: config.RegistryKeys, RegistryBinding: binding,
		Identity: modelruntime.ExecutionJournalIdentity{JournalID: status.JournalID, Scope: status.Scope, Storage: status.Storage}, Startup: status.BackendLifecycle,
		JournalSocket: config.JournalSocket, StartupSocket: config.StartupSocket, RuntimeSocket: config.RuntimeSocket, JournalTimeout: config.JournalTimeout, CancelTimeout: config.CancelTimeout, ShutdownTimeout: config.ShutdownTimeout}
	wire, err := modelruntime.EncodeRemoteRuntimeBootstrap(bootstrap)
	if err != nil {
		return nil, err
	}
	request := modelruntime.BackendStartupRequest{SchemaVersion: 1, NodeIdentity: config.Plan.binding.Claim.NodeIdentity, RegistryBindingDigest: sha256.Sum256(binding),
		JournalID: status.JournalID, JournalScope: status.Scope, IncarnationID: status.BackendLifecycle.IncarnationID, LaunchDigest: status.BackendLifecycle.LaunchDigest}
	if _, err := config.Journal.InspectStartup(ctx, manifest, request); err != nil {
		return nil, err
	}
	return wire, config.Journal.RequireRootCustody(ctx)
}

// InspectRuntimeBootstrapPublication reopens only complete publication history.
// It does not finish partial publication or consult/restore a live owner/grant.
func InspectRuntimeBootstrapPublication(ctx context.Context, path string) (result *RuntimeBootstrapPublication, resultErr error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	directory, err := openRuntimePublicationDirectory(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, directory.Close())
		if resultErr != nil {
			result = nil
		}
	}()
	marker, err := openRuntimePublicationFile(directory, runtimeBootstrapRecordName)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, marker.Close())
		if resultErr != nil {
			result = nil
		}
	}()
	if err := unix.Flock(int(marker.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, err
	}
	return inspectRuntimeBootstrapFiles(ctx, directory, marker, true)
}

func openRuntimePublicationDirectory(path string) (*os.File, error) {
	if os.Geteuid() != 0 || os.Getegid() != 0 || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || len(path) > 4096 {
		return nil, ErrRuntimeBootstrapPublication
	}
	// Match the CLI reader: even root-owned sticky writable ancestors are
	// rejected. Each next component is opened relative to the retained parent.
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	for _, component := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		var info unix.Stat_t
		if err := unix.Fstat(fd, &info); err != nil || info.Uid != 0 || info.Mode&0o022 != 0 {
			_ = unix.Close(fd)
			return nil, errors.Join(ErrRuntimeBootstrapPublication, err)
		}
		next, err := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(fd)
		if err != nil {
			return nil, err
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), path), nil
}

func runtimePublicationDirectoryBound(path string, directory *os.File) error {
	current, err := openRuntimePublicationDirectory(path)
	if err != nil {
		return err
	}
	defer func() { _ = current.Close() }()
	want, wantErr := directory.Stat()
	got, gotErr := current.Stat()
	if wantErr != nil || gotErr != nil || !os.SameFile(want, got) {
		return errors.Join(ErrRuntimeBootstrapPublication, wantErr, gotErr)
	}
	return nil
}

func openRuntimePublicationFile(directory *os.File, name string) (*os.File, error) {
	fd, err := unix.Openat(int(directory.Fd()), name, unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unix.Close(fd) }()
	var info unix.Stat_t
	if err := unix.Fstat(fd, &info); err != nil || info.Mode&unix.S_IFMT != unix.S_IFREG {
		return nil, errors.Join(ErrRuntimeBootstrapPublication, err)
	}
	descriptors, err := os.Open("/proc/self/fd")
	if err != nil {
		return nil, err
	}
	defer func() { _ = descriptors.Close() }()
	var filesystem unix.Statfs_t
	if err := unix.Fstatfs(int(descriptors.Fd()), &filesystem); err != nil || filesystem.Type != unix.PROC_SUPER_MAGIC {
		return nil, errors.Join(ErrRuntimeBootstrapPublication, err)
	}
	opened, err := unix.Openat(int(descriptors.Fd()), strconv.Itoa(fd), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK|unix.O_NOCTTY, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(opened), name), nil
}

func runtimePublicationNames(directory *os.File, expected []string) error {
	fd, err := unix.Openat(int(directory.Fd()), ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	copyDir := os.NewFile(uintptr(fd), "publication-names")
	defer func() { _ = copyDir.Close() }()
	names, err := copyDir.Readdirnames(4)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	slices.Sort(names)
	expected = slices.Clone(expected)
	slices.Sort(expected)
	if !slices.Equal(names, expected) {
		return ErrRuntimeBootstrapPublication
	}
	return nil
}

func readRuntimePublicationFile(ctx context.Context, file *os.File, gid, mode uint32, maximum int64) ([]byte, unix.Stat_t, error) {
	var first unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &first); err != nil {
		return nil, first, err
	}
	if first.Uid != 0 || first.Gid != gid || first.Mode != unix.S_IFREG|mode || first.Nlink != 1 || first.Size <= 0 || first.Size > maximum {
		return nil, first, ErrRuntimeBootstrapPublication
	}
	if err := contextError(ctx); err != nil {
		return nil, first, err
	}
	wire, err := io.ReadAll(io.NewSectionReader(file, 0, first.Size+1))
	if err != nil {
		return nil, first, err
	}
	var last unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &last); err != nil {
		return nil, first, err
	}
	last.Atim = first.Atim
	if first != last || int64(len(wire)) != first.Size {
		return nil, first, ErrRuntimeBootstrapPublication
	}
	return wire, first, contextError(ctx)
}

func inspectRuntimeBootstrapFiles(ctx context.Context, directory, marker *os.File, exposed bool) (*RuntimeBootstrapPublication, error) {
	if err := runtimePublicationNames(directory, []string{runtimeBootstrapRecordName, runtimeBootstrapFilename}); err != nil {
		return nil, err
	}
	wire, markerInfo, err := readRuntimePublicationFile(ctx, marker, 0, 0o600, 64<<10)
	if err != nil {
		return nil, err
	}
	var record RuntimeBootstrapPublicationRecord
	if err := decodeRuntimeStartupLine(wire, &record); err != nil {
		return nil, err
	}
	var root unix.Stat_t
	if err := unix.Fstat(int(directory.Fd()), &root); err != nil {
		return nil, err
	}
	gid, mode := uint32(0), uint32(0o700)
	if exposed {
		gid, mode = record.RuntimeGID, 0o750
	}
	var namedMarker unix.Stat_t
	if err := unix.Fstatat(int(directory.Fd()), runtimeBootstrapRecordName, &namedMarker, unix.AT_SYMLINK_NOFOLLOW); err != nil || namedMarker.Dev != markerInfo.Dev || namedMarker.Ino != markerInfo.Ino {
		return nil, errors.Join(ErrRuntimeBootstrapPublication, err)
	}
	if record.SchemaVersion != 1 || record.ID == uuid.Nil || record.RuntimeGID == 0 || record.RuntimeGID == ^uint32(0) || record.RecordedAt.IsZero() || record.RecordedAt.Location() != time.UTC ||
		root.Uid != 0 || root.Gid != gid || root.Mode != unix.S_IFDIR|mode || record.Directory != (runtimeStartupFileIdentity{Device: root.Dev, Inode: root.Ino}) || record.RecordFile != (runtimeStartupFileIdentity{Device: markerInfo.Dev, Inode: markerInfo.Ino}) {
		return nil, ErrRuntimeBootstrapPublication
	}
	file, err := openRuntimePublicationFile(directory, runtimeBootstrapFilename)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	content, fileInfo, err := readRuntimePublicationFile(ctx, file, record.RuntimeGID, 0o440, modelruntime.MaximumRemoteBootstrapBytes)
	if err != nil {
		return nil, err
	}
	if record.BootstrapFile != (runtimeStartupFileIdentity{Device: fileInfo.Dev, Inode: fileInfo.Ino}) || record.BootstrapBytes != int64(len(content)) || record.BootstrapDigest != sha256.Sum256(content) {
		return nil, ErrRuntimeBootstrapPublication
	}
	bootstrap, err := modelruntime.ParseRemoteRuntimeBootstrap(content)
	if err != nil {
		return nil, err
	}
	if record.BindingDigest != sha256.Sum256(bootstrap.RegistryBinding) {
		return nil, ErrRuntimeBootstrapPublication
	}
	var binding velav1.WorkerBootstrapBinding
	if err := proto.Unmarshal(bootstrap.RegistryBinding, &binding); err != nil {
		return nil, err
	}
	if binding.GetClaim().GetNodeIdentity() != record.NodeIdentity {
		return nil, ErrRuntimeBootstrapPublication
	}
	return &RuntimeBootstrapPublication{record: record, encoded: content}, nil
}
