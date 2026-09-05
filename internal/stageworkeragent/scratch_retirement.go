package stageworkeragent

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageartifact"
)

type ScratchRetirer interface {
	RetireCommitted(context.Context, stageartifact.LocalOutputManifestV1, stageartifact.Artifact) error
	RetireSourceLost(context.Context, stageartifact.LocalOutputManifestV1, MaterializationSourceLossEvidence) error
}

var ErrScratchRetirementUnproven = errors.New("stage scratch retirement requires durable admission exclusion and writer-drain evidence")

// RetainScratchRetirer preserves scratch and the caller's recovery record while
// the terminal retirement protocol is unavailable. Publication is not drain.
type RetainScratchRetirer struct{}

func (RetainScratchRetirer) RetireCommitted(context.Context, stageartifact.LocalOutputManifestV1, stageartifact.Artifact) error {
	return ErrScratchRetirementUnproven
}

func (RetainScratchRetirer) RetireSourceLost(context.Context, stageartifact.LocalOutputManifestV1, MaterializationSourceLossEvidence) error {
	return ErrScratchRetirementUnproven
}

const AttemptOwnedFilesystemScratchV1 = "attempt-owned-filesystem-scratch/v1"

func validateAttemptOwnedScratchManifest(manifest stageartifact.LocalOutputManifestV1) error {
	if _, err := manifest.LineageDigest(); err != nil {
		return err
	}
	if !strings.HasPrefix(manifest.LocalLocator, manifest.Lineage.StageAttemptID.String()+"/") {
		return errors.New("scratch output locator must be inside its StageAttempt UUID directory")
	}
	return nil
}

// FilesystemScratchRetirer is a destructive filesystem primitive. It does not
// establish admission exclusion or writer drain and must not be wired directly
// into a live Worker without an independent durable retirement protocol.
type FilesystemScratchRetirer struct {
	mu              sync.Mutex
	inputs, outputs *os.Root
}

func NewFilesystemScratchRetirer(inputRoot, outputRoot string) (*FilesystemScratchRetirer, error) {
	resolved := make([]string, 2)
	for index, root := range []string{inputRoot, outputRoot} {
		info, err := os.Lstat(root)
		if err != nil || !filepath.IsAbs(root) || filepath.Clean(root) != root ||
			!info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("scratch retirement requires distinct trusted input/output directories")
		}
		resolved[index], err = filepath.EvalSymlinks(root)
		if err != nil {
			return nil, err
		}
	}
	for index := range resolved {
		relative, err := filepath.Rel(resolved[index], resolved[1-index])
		if err != nil || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))) {
			return nil, errors.New("scratch retirement roots must not overlap")
		}
	}
	inputs, err := securefile.OpenTrustedRoot(inputRoot)
	if err != nil {
		return nil, err
	}
	outputs, err := securefile.OpenTrustedRoot(outputRoot)
	if err != nil {
		_ = inputs.Close()
		return nil, err
	}
	return &FilesystemScratchRetirer{inputs: inputs, outputs: outputs}, nil
}

func (retirer *FilesystemScratchRetirer) Close() error {
	if retirer == nil {
		return nil
	}
	retirer.mu.Lock()
	defer retirer.mu.Unlock()
	if retirer.inputs == nil {
		return nil
	}
	err := errors.Join(retirer.inputs.Close(), retirer.outputs.Close())
	retirer.inputs, retirer.outputs = nil, nil
	return err
}

// RetireCommitted consumes a confirmed materialization result. Callers must keep
// their recovery record until this operation succeeds; publication alone is not
// confirmation that the source is dispensable.
func (retirer *FilesystemScratchRetirer) RetireCommitted(ctx context.Context, manifest stageartifact.LocalOutputManifestV1, committed stageartifact.Artifact) error {
	if retirer == nil || ctx == nil {
		return errors.New("scratch retirement is not configured")
	}
	if err := validateAttemptOwnedScratchManifest(manifest); err != nil {
		return err
	}
	if committed.ID == uuid.Nil || committed.ObjectKey == "" || committed.ObjectVersion == "" ||
		committed.CommittedAt.IsZero() || committed.SHA256 != manifest.PayloadSHA256 || committed.SizeBytes != manifest.SizeBytes {
		return errors.New("scratch retirement requires matching committed Artifact evidence")
	}
	retirer.mu.Lock()
	defer retirer.mu.Unlock()
	if retirer.inputs == nil || retirer.outputs == nil {
		return errors.New("scratch retirement is closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	inputDirectory := path.Join("stage-runs", manifest.Lineage.StageRunID.String())
	inputs, err := bindScratchDirectory(retirer.inputs, inputDirectory)
	if err != nil {
		return err
	}
	defer inputs.close()
	outputs, err := bindScratchDirectory(retirer.outputs, path.Dir(manifest.LocalLocator))
	if err != nil {
		return err
	}
	defer outputs.close()
	return retireBoundScratch(ctx, inputs, outputs, path.Base(manifest.LocalLocator), manifest)
}

// RetireSourceLost requires a confirmed SOURCE_LOST report. The old output is
// dispensable, but the same StageRun may retry and still owns its input files.
func (retirer *FilesystemScratchRetirer) RetireSourceLost(ctx context.Context, manifest stageartifact.LocalOutputManifestV1, reported MaterializationSourceLossEvidence) error {
	if retirer == nil || ctx == nil {
		return errors.New("scratch retirement is not configured")
	}
	if err := validateAttemptOwnedScratchManifest(manifest); err != nil {
		return err
	}
	if err := validateSourceLossEvidence(reported); err != nil {
		return err
	}
	retirer.mu.Lock()
	defer retirer.mu.Unlock()
	if retirer.outputs == nil {
		return errors.New("scratch retirement is closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	outputs, err := bindScratchDirectory(retirer.outputs, path.Dir(manifest.LocalLocator))
	if err != nil {
		return err
	}
	defer outputs.close()
	if outputs.target != nil {
		leaf := path.Base(manifest.LocalLocator)
		info, err := outputs.target.Lstat(leaf)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err == nil {
			if !info.Mode().IsRegular() {
				return errors.New("lost output retirement target type is invalid")
			}
			if err := outputs.target.Remove(leaf); err != nil {
				return err
			}
		}
	}
	return outputs.pruneEmpty(0)
}

type scratchDirectoryComponent struct {
	parent, root *os.Root
	name         string
	info         fs.FileInfo
}

type scratchDirectoryBinding struct {
	components []scratchDirectoryComponent
	target     *os.Root
}

// Bind every component before using descendants. A total-root handle alone can
// follow a concurrently replaced directory symlink into another Stage namespace.
func bindScratchDirectory(root *os.Root, relative string) (*scratchDirectoryBinding, error) {
	if !fs.ValidPath(relative) || relative == "." {
		return nil, errors.New("scratch directory path is invalid")
	}
	binding := &scratchDirectoryBinding{}
	for _, name := range strings.Split(relative, "/") {
		info, err := root.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			return binding, nil
		}
		if err != nil {
			binding.close()
			return nil, err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			binding.close()
			return nil, errors.New("scratch retirement rejects symlink or invalid parent")
		}
		child, err := root.OpenRoot(name)
		if err != nil {
			binding.close()
			return nil, err
		}
		opened, err := child.Stat(".")
		if err != nil || !os.SameFile(info, opened) {
			_ = child.Close()
			binding.close()
			return nil, errors.Join(err, errors.New("scratch directory changed while binding"))
		}
		binding.components = append(binding.components, scratchDirectoryComponent{parent: root, root: child, name: name, info: info})
		root = child
	}
	binding.target = root
	return binding, nil
}

func (binding *scratchDirectoryBinding) close() {
	for index := len(binding.components) - 1; index >= 0; index-- {
		_ = binding.components[index].root.Close()
	}
}

func retireBoundScratch(ctx context.Context, inputs, outputs *scratchDirectoryBinding, outputLeaf string, manifest stageartifact.LocalOutputManifestV1) error {
	if inputs.target != nil {
		if err := fs.WalkDir(inputs.target.FS(), ".", func(_ string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.Type()&os.ModeSymlink != 0 {
				return errors.New("scratch retirement rejects symlink in input namespace")
			}
			return nil
		}); err != nil {
			return err
		}
	}
	outputExists := false
	var outputInfo fs.FileInfo
	if outputs.target != nil {
		info, err := outputs.target.Lstat(outputLeaf)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		outputExists = err == nil
		if outputExists && !info.Mode().IsRegular() {
			return errors.New("scratch retirement target type is invalid")
		}
		outputInfo = info
	}
	if outputExists {
		file, err := outputs.target.OpenFile(outputLeaf, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return err
		}
		opened, err := file.Stat()
		if err != nil || !os.SameFile(outputInfo, opened) {
			_ = file.Close()
			return errors.Join(err, errors.New("scratch output changed while opening"))
		}
		digest := sha256.New()
		written, readErr := io.Copy(digest, io.LimitReader(file, manifest.SizeBytes+1))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil {
			return errors.Join(readErr, closeErr)
		}
		if written != manifest.SizeBytes || !equalDigest(digest.Sum(nil), manifest.PayloadSHA256) {
			return errors.New("scratch output no longer matches committed Artifact bytes")
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if outputExists {
		current, err := outputs.target.Lstat(outputLeaf)
		if err != nil || !os.SameFile(outputInfo, current) {
			return errors.Join(err, errors.New("scratch output changed before retirement"))
		}
		if err := outputs.target.Remove(outputLeaf); err != nil {
			return err
		}
	}
	if inputs.target != nil {
		directory, err := inputs.target.Open(".")
		if err != nil {
			return err
		}
		entries, readErr := directory.ReadDir(-1)
		if err := errors.Join(readErr, directory.Close()); err != nil {
			return err
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := inputs.target.RemoveAll(entry.Name()); err != nil {
				return err
			}
		}
	}
	return errors.Join(outputs.pruneEmpty(0), inputs.pruneEmpty(1))
}

func (binding *scratchDirectoryBinding) pruneEmpty(first int) error {
	for index := len(binding.components) - 1; index >= first; index-- {
		component := binding.components[index]
		if err := syncScratchDirectory(component.root, "."); err != nil {
			return err
		}
		current, err := component.parent.Lstat(component.name)
		if errors.Is(err, os.ErrNotExist) || (err == nil && !os.SameFile(component.info, current)) {
			return nil
		}
		if err != nil {
			return err
		}
		// Remove only an empty directory, never recursively through the parent.
		if err := component.parent.Remove(component.name); errors.Is(err, syscall.ENOTEMPTY) {
			return nil
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err := syncScratchDirectory(component.parent, "."); err != nil {
			return err
		}
	}
	return nil
}

func syncScratchDirectory(root *os.Root, relative string) error {
	directory, err := root.Open(relative)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open scratch directory for sync: %w", err)
	}
	return errors.Join(directory.Sync(), directory.Close())
}
