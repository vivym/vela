package workerbootstrap

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/securefile"
)

type provisionObservation struct {
	file *os.File
	info os.FileInfo
	wire []byte
}

func inspectProvisionedJournals(ctx context.Context, config Config, directory string, reader HistoryReader) (result ProvisionedJournals, err error) {
	if ctx == nil || reader == nil || os.Geteuid() != 0 || !filepath.IsAbs(directory) ||
		filepath.Clean(directory) != directory || config.ScratchDirectory != filepath.Join(directory, "scratch") {
		return result, errors.New("protected inspection requires Linux root and its original private Node directory")
	}
	if err := context.Cause(ctx); err != nil {
		return result, err
	}
	p, err := bind(config)
	if err != nil {
		return result, err
	}
	root, err := securefile.OpenTrustedRoot(directory)
	if err != nil {
		return result, err
	}
	held := make(map[string]provisionObservation)
	defer func() {
		for _, observation := range held {
			err = errors.Join(err, observation.file.Close())
		}
		err = errors.Join(err, root.Close(), context.Cause(ctx))
		if err != nil {
			result = ProvisionedJournals{}
		}
	}()
	info, err := root.Stat(".")
	if err != nil || !privateDirectory(info) {
		return result, errors.New("protected inspection root is not private")
	}
	storage := provisionStorage{path: directory, root: root, info: info}
	if err := errors.Join(storage.check(), provisionNames(root, ".", []string{provisionIntentName, provisionOriginName, provisionDoneName, "scratch"})); err != nil {
		return result, fmt.Errorf("%w: incomplete protected handover: %w", ErrIncomplete, err)
	}
	var intent provisionIntent
	var origin provisionOrigin
	var done ProvisionedJournals
	for _, record := range []struct {
		name  string
		value any
	}{{provisionIntentName, &intent}, {provisionOriginName, &origin}, {provisionDoneName, &done}} {
		file, err := root.OpenFile(record.name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return result, err
		}
		held[record.name] = provisionObservation{file: file}
		if record.name == provisionIntentName {
			if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
				return result, fmt.Errorf("protected provisioning is still owned: %w", err)
			}
		}
		wire, err := readDocument(file, record.value)
		if err != nil {
			return result, err
		}
		info, err := file.Stat()
		if err != nil {
			return result, err
		}
		held[record.name] = provisionObservation{file: file, info: info, wire: wire}
	}
	if intent.SchemaVersion != 1 || intent.ID == uuid.Nil || intent.Root != identity(info) ||
		intent.File != identity(held[provisionIntentName].info) || intent.Node != config.NodeIdentity || intent.Actor != config.ActorIdentity ||
		intent.BundleDigest != p.digest || intent.LaunchDigest != p.launchID || intent.MaxRecords != config.MaxRecords ||
		intent.OwnerUID != provisionOwner || intent.OwnerGID != provisionOwner || origin.Intent != intent ||
		done.SchemaVersion != 1 || done.ProvisionID != intent.ID || done.RequestID == uuid.Nil || done.RequestID != origin.Result.RequestID ||
		done.OriginDigest != sha256.Sum256(held[provisionOriginName].wire) {
		return result, errors.New("protected handover records differ from original scope or storage")
	}
	// The protected producer recorded successful journal preparation. Exact bytes
	// below deliberately reject even valid later journal evolution. This is an
	// initial-handover inspector, not a substitute for current journal recovery.
	children := provisionChildren()
	expected := make(map[string]bool)
	for i, directory := range rootNames {
		expected[directory] = true
		if i > 0 {
			for _, name := range children[i] {
				expected[directory+"/"+name] = false
			}
		}
	}
	if len(origin.Files) != len(expected) {
		return result, errors.New("protected origin inventory is incomplete")
	}
	identities := make(map[fileIdentity]bool)
	inventory := make(map[string]provisionFile)
	for _, record := range origin.Files {
		directory, exists := expected[record.Path]
		if !exists || directory != record.Directory || record.Identity.Inode == 0 || identities[record.Identity] ||
			record.Size < 0 || record.Size > 1<<20 || directory && (record.Mode != 0o700 || record.Size != 0 || record.Digest != ([sha256.Size]byte{})) ||
			!directory && record.Mode != 0o600 {
			return result, errors.New("protected origin contains invalid or duplicate inventory")
		}
		delete(expected, record.Path)
		identities[record.Identity] = true
		inventory[record.Path] = record
		name := "scratch/" + record.Path
		file, err := root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			return result, err
		}
		held[name] = provisionObservation{file: file}
		if record.Path == "worker-admission/assignment-admission.lock" || record.Path == "runtime-admission/execution-admission.lock" ||
			record.Path == "bootstrap/"+operationName {
			if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
				return result, fmt.Errorf("provisioned storage is still owned: %w", err)
			}
		}
	}
	validate := func() error {
		if err := errors.Join(storage.check(), context.Cause(ctx), provisionNames(root, ".", []string{provisionIntentName, provisionOriginName, provisionDoneName, "scratch"})); err != nil {
			return err
		}
		for name, observation := range held {
			if observation.info == nil {
				continue // Scratch inventory is checked against the independent origin below.
			}
			current, err := root.Lstat(name)
			opened, openErr := observation.file.Stat()
			wire, readErr := io.ReadAll(io.NewSectionReader(observation.file, 0, maxStateBytes+1))
			if err != nil || openErr != nil || readErr != nil || !privateFile(current) || !privateFile(opened) ||
				!os.SameFile(current, observation.info) || !os.SameFile(opened, current) || !bytes.Equal(wire, observation.wire) {
				return errors.New("protected handover record changed during inspection")
			}
		}
		for i, directory := range rootNames {
			if err := provisionNames(root, "scratch/"+directory, children[i]); err != nil {
				return err
			}
		}
		for _, record := range origin.Files {
			name := "scratch/" + record.Path
			file := held[name].file
			info, err := file.Stat()
			current, pathErr := root.Lstat(name)
			if err != nil || pathErr != nil || !os.SameFile(info, current) || identity(info) != record.Identity ||
				info.IsDir() != record.Directory || uint32(info.Mode().Perm()) != record.Mode || info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
				return errors.New("provisioned storage identity, type or mode changed")
			}
			stat := info.Sys().(*syscall.Stat_t)
			if stat.Uid != provisionOwner || stat.Gid != provisionOwner || !record.Directory && (!info.Mode().IsRegular() || stat.Nlink != 1) {
				return errors.New("provisioned storage ownership or links changed")
			}
			if !record.Directory {
				wire, err := io.ReadAll(io.NewSectionReader(file, 0, record.Size+1))
				if err != nil || info.Size() != record.Size || sha256.Sum256(wire) != record.Digest {
					return errors.New("provisioned storage has changed since initial handover")
				}
			}
		}
		return nil
	}
	if err := validate(); err != nil {
		return result, err
	}
	p.request.RequestID = done.RequestID
	history, err := reader.LookupWorkerBootstrap(ctx, done.RequestID)
	if err != nil {
		return result, err
	}
	pair, err := p.recordedPair(history)
	if err != nil {
		return result, err
	}
	worker, runtime := origin.Result.Worker, origin.Result.Runtime
	storageIdentity := func(directory, lock string) journalbinding.StorageIdentity {
		return journalbinding.StorageIdentity{Root: journalbinding.FileIdentity(inventory[directory].Identity),
			Lock: journalbinding.FileIdentity(inventory[directory+"/"+lock].Identity)}
	}
	if worker.JournalID != pair.WorkerID || runtime.JournalID != pair.RuntimeID || worker.Scope != pair.WorkerScope || runtime.Scope != pair.RuntimeScope ||
		!origin.Result.RecordedAt.Equal(history.RecordedAt) || worker.Storage != storageIdentity("worker-admission", "assignment-admission.lock") ||
		runtime.Storage != storageIdentity("runtime-admission", "execution-admission.lock") {
		return result, errors.New("protected origin differs from committed Registry pair or original journal storage")
	}
	if err := validate(); err != nil {
		return result, err
	}
	return done, nil
}
