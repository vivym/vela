package stageworkeragent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageartifact"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

type FilesystemInputTransferTarget struct {
	mu sync.Mutex

	root         *os.Root
	expected     stageartifact.TransferDescriptor
	finalPath    string
	pendingPath  string
	pendingFile  *os.File
	writerClosed bool
	committed    bool
	closed       bool
}

func NewFilesystemInputTransferTarget(
	rootPath string,
	stageRunID uuid.UUID,
	input *velav1.StageInputArtifact,
) (*FilesystemInputTransferTarget, error) {
	cleaned := filepath.Clean(rootPath)
	if !filepath.IsAbs(cleaned) || cleaned != rootPath || input == nil ||
		stageRunID == uuid.Nil {
		return nil, errors.New("stage input transfer target configuration is invalid")
	}
	artifactID, err := uuid.Parse(input.GetStageArtifactId())
	if err != nil || artifactID == uuid.Nil || input.GetObjectVersion() == "" ||
		len(input.GetSha256()) != sha256.Size || input.GetSizeBytes() <= 0 {
		return nil, errors.New("stage input transfer target exact Artifact is invalid")
	}
	var digest [sha256.Size]byte
	copy(digest[:], input.GetSha256())
	finalPath, err := StageInputRelativePath(stageRunID, artifactID, digest)
	if err != nil {
		return nil, err
	}
	root, err := securefile.OpenTrustedRoot(cleaned)
	if err != nil {
		return nil, fmt.Errorf("open Stage input root: %w", err)
	}
	return &FilesystemInputTransferTarget{
		root: root,
		expected: stageartifact.TransferDescriptor{
			ArtifactID: artifactID, ObjectVersion: input.GetObjectVersion(),
			SHA256: digest, SizeBytes: input.GetSizeBytes(),
		},
		finalPath: finalPath,
	}, nil
}

func StageInputRelativePath(
	stageRunID uuid.UUID,
	artifactID uuid.UUID,
	digest [sha256.Size]byte,
) (string, error) {
	if stageRunID == uuid.Nil || artifactID == uuid.Nil || digest == ([sha256.Size]byte{}) {
		return "", errors.New("stage input local path identity is invalid")
	}
	return path.Join(
		"stage-runs", stageRunID.String(), "inputs", artifactID.String(),
		hex.EncodeToString(digest[:])+".bin",
	), nil
}

func (target *FilesystemInputTransferTarget) Begin(
	ctx context.Context,
	descriptor stageartifact.TransferDescriptor,
) (io.WriteCloser, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, errors.New("stage input transfer target context is invalid")
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.root == nil || target.closed || target.pendingFile != nil || target.committed ||
		descriptor.TicketID == uuid.Nil || descriptor.ArtifactID != target.expected.ArtifactID ||
		descriptor.ObjectVersion != target.expected.ObjectVersion ||
		descriptor.SHA256 != target.expected.SHA256 ||
		descriptor.SizeBytes != target.expected.SizeBytes {
		return nil, errors.New("stage input transfer descriptor does not match exact target")
	}
	directory := path.Dir(target.finalPath)
	if err := target.root.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create Stage input directory: %w", err)
	}
	pendingPath := target.finalPath + ".partial." + uuid.NewString()
	file, err := target.root.OpenFile(
		pendingPath,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("create Stage input temporary file: %w", err)
	}
	target.expected.TicketID = descriptor.TicketID
	target.pendingPath = pendingPath
	target.pendingFile = file
	target.writerClosed = false
	return &inputTargetWriter{target: target, file: file}, nil
}

func (target *FilesystemInputTransferTarget) Commit(
	ctx context.Context,
	receipt stageartifact.PullReceipt,
) error {
	if ctx == nil || ctx.Err() != nil {
		return errors.New("stage input transfer commit context is invalid")
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.root == nil || target.closed || target.pendingPath == "" ||
		target.pendingFile == nil || !target.writerClosed || target.committed ||
		receipt.TicketID != target.expected.TicketID ||
		receipt.ArtifactID != target.expected.ArtifactID || receipt.SHA256 != target.expected.SHA256 ||
		receipt.SizeBytes != target.expected.SizeBytes || receipt.CompletedAt.IsZero() {
		return errors.New("stage input transfer receipt does not match exact target")
	}
	if err := target.verifyPending(); err != nil {
		return err
	}
	if err := target.root.Rename(target.pendingPath, target.finalPath); err != nil {
		return fmt.Errorf("commit Stage input file: %w", err)
	}
	directory, err := target.root.Open(path.Dir(target.finalPath))
	if err != nil {
		return fmt.Errorf("open committed Stage input directory: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil || closeErr != nil {
		return fmt.Errorf("sync committed Stage input directory: %w", errors.Join(syncErr, closeErr))
	}
	target.pendingPath = ""
	target.pendingFile = nil
	target.committed = true
	return nil
}

func (target *FilesystemInputTransferTarget) VerifyCommitted(ctx context.Context) error {
	if target == nil || ctx == nil || ctx.Err() != nil {
		return errors.New("stage input committed-file verification is invalid")
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.root == nil || target.closed || target.pendingFile != nil {
		return errors.New("stage input transfer target is unavailable")
	}
	return target.verifyFile(target.finalPath)
}

func (target *FilesystemInputTransferTarget) verifyPending() error {
	return target.verifyFile(target.pendingPath)
}

func (target *FilesystemInputTransferTarget) verifyFile(relativePath string) error {
	file, err := target.root.Open(relativePath)
	if err != nil {
		return fmt.Errorf("open Stage input file: %w", err)
	}
	digest := sha256.New()
	written, readErr := io.Copy(digest, io.LimitReader(file, target.expected.SizeBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return fmt.Errorf("verify Stage input file: %w", errors.Join(readErr, closeErr))
	}
	if written != target.expected.SizeBytes ||
		!equalDigest(digest.Sum(nil), target.expected.SHA256) {
		return errors.New("stage input file failed exact integrity verification")
	}
	return nil
}

func equalDigest(value []byte, expected [sha256.Size]byte) bool {
	if len(value) != sha256.Size {
		return false
	}
	var actual [sha256.Size]byte
	copy(actual[:], value)
	return actual == expected
}

func (target *FilesystemInputTransferTarget) Abort(context.Context) error {
	if target == nil {
		return nil
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	return target.abortLocked()
}

func (target *FilesystemInputTransferTarget) Close() error {
	if target == nil {
		return nil
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.closed {
		return nil
	}
	abortErr := target.abortLocked()
	target.closed = true
	closeErr := target.root.Close()
	target.root = nil
	return errors.Join(abortErr, closeErr)
}

func (target *FilesystemInputTransferTarget) abortLocked() error {
	if target.pendingFile != nil && !target.writerClosed {
		_ = target.pendingFile.Close()
	}
	var removeErr error
	if target.root != nil && target.pendingPath != "" {
		removeErr = target.root.Remove(target.pendingPath)
		if errors.Is(removeErr, os.ErrNotExist) {
			removeErr = nil
		}
	}
	target.pendingPath = ""
	target.pendingFile = nil
	target.writerClosed = false
	return removeErr
}

type inputTargetWriter struct {
	target *FilesystemInputTransferTarget
	file   *os.File
	once   sync.Once
	err    error
}

func (writer *inputTargetWriter) Write(value []byte) (int, error) {
	return writer.file.Write(value)
}

func (writer *inputTargetWriter) Close() error {
	writer.once.Do(func() {
		syncErr := writer.file.Sync()
		closeErr := writer.file.Close()
		writer.err = errors.Join(syncErr, closeErr)
		writer.target.mu.Lock()
		writer.target.writerClosed = writer.err == nil && writer.target.pendingFile == writer.file
		writer.target.mu.Unlock()
	})
	return writer.err
}

var _ stageartifact.TransferTarget = (*FilesystemInputTransferTarget)(nil)
