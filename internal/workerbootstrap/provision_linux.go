package workerbootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/vivym/vela/internal/runtimelaunch"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/securefile"
)

const (
	provisionIntentName = "provision-intent.json"
	provisionOriginName = "provision-origin.json"
	provisionDoneName   = "provision-handover.json"
	provisionOwner      = 10001
)

type provisionIntent struct {
	SchemaVersion int               `json:"schema_version"`
	ID            uuid.UUID         `json:"provision_id"`
	Root          fileIdentity      `json:"root"`
	File          fileIdentity      `json:"file"`
	Node          string            `json:"node"`
	Actor         string            `json:"actor"`
	BundleDigest  [sha256.Size]byte `json:"bundle_digest"`
	LaunchDigest  [sha256.Size]byte `json:"launch_digest"`
	OwnerUID      int               `json:"owner_uid"`
	OwnerGID      int               `json:"owner_gid"`
	MaxRecords    int               `json:"max_records"`
	NodeCustody   bool              `json:"node_custody,omitempty"`
}

type provisionFile struct {
	Path      string            `json:"path"`
	Identity  fileIdentity      `json:"identity"`
	Directory bool              `json:"directory"`
	Mode      uint32            `json:"mode"`
	Size      int64             `json:"size"`
	Digest    [sha256.Size]byte `json:"digest"`
}

type provisionOrigin struct {
	Intent provisionIntent `json:"intent"`
	Result Result          `json:"preparation"`
	Files  []provisionFile `json:"files"`
}

type provisionStorage struct {
	path string
	root *os.Root
	info os.FileInfo
}

func provision(ctx context.Context, config Config, directory string, authority Authority, boundary func(string) error) (result ProvisionedJournals, err error) {
	if ctx == nil || authority == nil || os.Geteuid() != 0 || !filepath.IsAbs(directory) ||
		filepath.Clean(directory) != directory || config.ScratchDirectory != filepath.Join(directory, "scratch") {
		return result, errors.New("protected provisioning requires Linux root and scratch directly under its private Node directory")
	}
	p, err := bind(config)
	if err != nil {
		return result, err
	}
	checkpoint := func(name string) error {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if boundary != nil {
			return boundary(name)
		}
		return nil
	}
	if err := checkpoint("preflight"); err != nil {
		return result, err
	}
	root, err := securefile.OpenTrustedRoot(directory)
	if err != nil {
		return result, err
	}
	defer func() {
		err = errors.Join(err, root.Close())
		if err != nil {
			result = ProvisionedJournals{}
		}
	}()
	info, err := root.Stat(".")
	if err != nil || !privateDirectory(info) {
		return result, errors.New("node provisioning directory must be root-owned mode 0700")
	}
	storage := provisionStorage{path: directory, root: root, info: info}
	if err := errors.Join(storage.check(), provisionNames(root, ".", nil)); err != nil {
		return result, fmt.Errorf("%w: Node provisioning requires an unused directory: %w", ErrIncomplete, err)
	}
	intentFile, err := root.OpenFile(provisionIntentName, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, intentFile.Close()) }()
	if err := syscall.Flock(int(intentFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return result, err
	}
	intentInfo, err := intentFile.Stat()
	if err != nil || !privateFile(intentInfo) {
		return result, errors.New("node provisioning intent is not private")
	}
	intent := provisionIntent{SchemaVersion: 1, ID: uuid.New(), Root: identity(info), File: identity(intentInfo),
		Node: config.NodeIdentity, Actor: config.ActorIdentity, BundleDigest: p.digest, LaunchDigest: p.launchID,
		OwnerUID: provisionOwner, OwnerGID: provisionOwner, MaxRecords: config.MaxRecords, NodeCustody: config.Bundle.RuntimeLaunchProtocol == runtimelaunch.Protocol}
	intentWire, err := json.Marshal(intent)
	if err != nil {
		return result, err
	}
	if _, err := intentFile.Write(intentWire); err != nil {
		return result, err
	}
	if err := errors.Join(intentFile.Sync(), syncRoot(root), storage.check(), checkpoint("intent-durable")); err != nil {
		return result, err
	}
	if err := root.Mkdir("scratch", 0o700); err != nil {
		return result, err
	}
	for _, name := range rootNames[1:] {
		if err := root.Mkdir("scratch/"+name, 0o700); err != nil {
			return result, err
		}
	}
	if err := errors.Join(syncRoot(root), storage.check(), checkpoint("roots-created")); err != nil {
		return result, err
	}
	prepared, err := Prepare(ctx, config, authority)
	if err != nil {
		return result, err
	}
	if err := errors.Join(storage.check(), checkpoint("journals-prepared")); err != nil {
		return result, err
	}
	// Open only the exact newly prepared tree. The protected ancestor excludes
	// workload UIDs throughout observation and ownership transfer; no recursive
	// chmod/chown or owner-supplied status document is used as initialization proof.
	files, held, err := storage.capture(p, prepared)
	if err != nil {
		return result, err
	}
	defer func() {
		for _, file := range held {
			err = errors.Join(err, file.Close())
		}
	}()
	originWire, err := json.Marshal(provisionOrigin{Intent: intent, Result: prepared, Files: files})
	if err != nil {
		return result, err
	}
	if err := storage.write(provisionOriginName, originWire); err != nil {
		return result, err
	}
	if err := checkpoint("origin-durable"); err != nil {
		return result, err
	}
	for i := len(held) - 1; i >= 0; i-- {
		if err := errors.Join(storage.check(), checkpoint("before-handover:"+files[i].Path)); err != nil {
			return result, err
		}
		info, err := held[i].Stat()
		current, pathErr := root.Lstat("scratch/" + files[i].Path)
		if err != nil || pathErr != nil || !os.SameFile(info, current) || identity(info) != files[i].Identity {
			return result, errors.New("prepared storage changed before ownership transfer")
		}
		if err := errors.Join(held[i].Chown(provisionFileOwner(intent.NodeCustody, files[i].Path), provisionFileOwner(intent.NodeCustody, files[i].Path)), held[i].Sync()); err != nil {
			return result, err
		}
		if err := checkpoint("after-handover:" + files[i].Path); err != nil {
			return result, err
		}
	}
	if err := storage.check(); err != nil {
		return result, err
	}
	for i, file := range held {
		info, err := file.Stat()
		current, pathErr := root.Lstat("scratch/" + files[i].Path)
		if err != nil || pathErr != nil || !os.SameFile(info, current) || identity(info) != files[i].Identity || uint32(info.Mode().Perm()) != files[i].Mode {
			return result, errors.New("transferred journal storage identity or mode changed")
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != uint32(provisionFileOwner(intent.NodeCustody, files[i].Path)) || stat.Gid != uint32(provisionFileOwner(intent.NodeCustody, files[i].Path)) {
			return result, errors.New("journal ownership transfer is incomplete")
		}
		if !files[i].Directory {
			wire, readErr := io.ReadAll(io.NewSectionReader(file, 0, files[i].Size+1))
			if readErr != nil || stat.Nlink != 1 || info.Size() != files[i].Size || sha256.Sum256(wire) != files[i].Digest {
				return result, errors.New("journal bytes or links changed during ownership transfer")
			}
		}
	}
	result = ProvisionedJournals{SchemaVersion: 1, ProvisionID: intent.ID, RequestID: prepared.RequestID, OriginDigest: sha256.Sum256(originWire)}
	doneWire, err := json.Marshal(result)
	if err != nil {
		return ProvisionedJournals{}, err
	}
	if err := storage.write(provisionDoneName, doneWire); err != nil {
		return ProvisionedJournals{}, err
	}
	return result, checkpoint("handover-durable")
}

func (storage provisionStorage) check() error {
	current, err := os.Lstat(storage.path)
	opened, openErr := storage.root.Stat(".")
	if err != nil || openErr != nil || !privateDirectory(current) || !privateDirectory(opened) ||
		!os.SameFile(current, storage.info) || !os.SameFile(opened, storage.info) {
		return errors.New("protected Node provisioning root changed")
	}
	return nil
}

func (storage provisionStorage) write(name string, wire []byte) error {
	if err := storage.check(); err != nil {
		return err
	}
	file, err := storage.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(wire)
	return errors.Join(writeErr, file.Sync(), file.Close(), syncRoot(storage.root), storage.check())
}

func (storage provisionStorage) capture(p preparation, prepared Result) (files []provisionFile, held []*os.File, err error) {
	local, err := openOperation(p, false)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		err = errors.Join(err, local.validate(), local.close())
		if err != nil {
			for _, file := range held {
				_ = file.Close()
			}
			files, held = nil, nil
		}
	}()
	origin, err := local.readOrigin()
	if err != nil || origin.Pair.RequestID != prepared.RequestID || origin.Worker != prepared.Worker.Storage || origin.Runtime != prepared.Runtime.Storage {
		return nil, nil, errors.Join(err, errors.New("private preparation differs from original storage"))
	}
	children := provisionChildren()
	for i, directory := range rootNames {
		path := "scratch/" + directory
		if err := provisionNames(storage.root, path, children[i]); err != nil {
			return files, held, err
		}
		paths := []string{directory}
		if i > 0 {
			for _, name := range children[i] {
				paths = append(paths, directory+"/"+name)
			}
		}
		for j, path := range paths {
			file, err := storage.root.OpenFile("scratch/"+path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
			if err != nil {
				return files, held, err
			}
			held = append(held, file)
			info, err := file.Stat()
			current, currentErr := storage.root.Lstat("scratch/" + path)
			isDirectory := j == 0
			if err != nil || currentErr != nil || !os.SameFile(info, current) || isDirectory && !privateDirectory(info) || !isDirectory && !privateFile(info) {
				return files, held, errors.New("private journal preparation contains unexpected storage")
			}
			record := provisionFile{Path: path, Identity: identity(info), Directory: isDirectory, Mode: uint32(info.Mode().Perm())}
			if !isDirectory {
				wire, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
				if err != nil || len(wire) > 1<<20 || int64(len(wire)) != info.Size() {
					return files, held, errors.New("initial journal file exceeds its observation bound")
				}
				record.Size, record.Digest = int64(len(wire)), sha256.Sum256(wire)
			}
			if err := file.Sync(); err != nil {
				return files, held, err
			}
			files = append(files, record)
		}
	}
	return files, held, nil
}

func provisionChildren() [][]string {
	return [][]string{rootNames[1:], {operationName, originName, pairName},
		{"assignment-admission.json", "assignment-admission.lock"}, {"execution-admission.json", "execution-admission.lock"},
		{".vela-assignment-admission"}, {".vela-assignment-admission"}}
}

func provisionNames(root *os.Root, path string, expected []string) error {
	file, err := root.OpenFile(path, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	names, readErr := file.Readdirnames(16)
	closeErr := file.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) || closeErr != nil {
		return errors.Join(readErr, closeErr)
	}
	if len(names) != len(expected) {
		return fmt.Errorf("unexpected files in provisioning directory %s", path)
	}
	for _, name := range names {
		if !slices.Contains(expected, name) {
			return errors.New("unrecognized provisioning file")
		}
	}
	return nil
}

// Kubernetes mounts only the Worker journal and content children. Node retains
// the parent, bootstrap evidence and Runtime journal outside workload mounts.
func provisionFileOwner(nodeCustody bool, path string) int {
	if !nodeCustody {
		return provisionOwner
	}
	for _, child := range []string{"worker-admission", "inputs", "outputs"} {
		if path == child || strings.HasPrefix(path, child+"/") {
			return provisionOwner
		}
	}
	return 0
}
