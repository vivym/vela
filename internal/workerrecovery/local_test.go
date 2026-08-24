package workerrecovery

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestLocalRecoveryStateBindsWorkerEpochAndFence(t *testing.T) {
	root := t.TempDir()
	workerID := uuid.New()
	manager := newTestManager(t, root, workerID, 9)
	identity := Identity{AttemptID: uuid.New(), WorkerID: workerID, WorkerEpoch: 9, Fence: 11}
	handle, err := manager.Open(context.Background(), identity)
	if err != nil {
		t.Fatalf("Open local recovery state: %v", err)
	}
	entry, err := handle.Write(context.Background(), StageDiT, "latent.bin", strings.NewReader("latent"))
	if err != nil {
		t.Fatalf("Write local recovery state: %v", err)
	}
	if entry.Stage != StageDiT || entry.Name != "latent.bin" || entry.SizeBytes != 6 {
		t.Fatalf("entry = %#v", entry)
	}
	content, err := handle.Read(context.Background(), StageDiT, "latent.bin")
	if err != nil || string(content) != "latent" {
		t.Fatalf("Read local recovery state = %q, error=%v", content, err)
	}
	entries, err := handle.List(context.Background())
	if err != nil || len(entries) != 1 || entries[0].Name != "latent.bin" {
		t.Fatalf("List local recovery state = %#v, error=%v", entries, err)
	}
	rootInfo, err := os.Stat(root)
	if err != nil {
		t.Fatalf("stat recovery root: %v", err)
	}
	if rootInfo.Mode().Perm() != 0700 {
		t.Fatalf("recovery root mode = %o, want 700", rootInfo.Mode().Perm())
	}
	metadataInfo, err := os.Stat(filepath.Join(root, "attempts", identity.AttemptID.String(), ".identity.json"))
	if err != nil {
		t.Fatalf("stat recovery metadata: %v", err)
	}
	if metadataInfo.Mode().Perm() != 0600 {
		t.Fatalf("recovery metadata mode = %o, want 600", metadataInfo.Mode().Perm())
	}
	if _, err := manager.Open(context.Background(), Identity{
		AttemptID: identity.AttemptID, WorkerID: workerID, WorkerEpoch: 9, Fence: 12,
	}); !errors.Is(err, ErrStateIdentityMismatch) {
		t.Fatalf("new fence Open error = %v, want identity mismatch", err)
	}
	if _, err := manager.Open(context.Background(), Identity{
		AttemptID: identity.AttemptID, WorkerID: uuid.New(), WorkerEpoch: 9, Fence: 11,
	}); !errors.Is(err, ErrInvalidIdentity) {
		t.Fatalf("wrong Worker Open error = %v, want invalid identity", err)
	}
	restarted := newTestManager(t, root, workerID, 9)
	if _, err := restarted.Open(context.Background(), identity); err != nil {
		t.Fatalf("same-epoch process restart Open: %v", err)
	}
	space := Space{TotalBytes: 1 << 30, FreeBytes: 1 << 29}
	if _, err := New(Config{
		Root: root, WorkerID: workerID, WorkerEpoch: 10,
		AttemptQuotaBytes: 64, MaxEntryBytes: 16, MaxEntries: 4,
		HighWatermarkBytes: 800, LowWatermarkBytes: 400, CriticalFreeBytes: 32,
		TerminalRetention: time.Hour,
		SpaceProbe:        func(string) (Space, error) { return space, nil },
	}); !errors.Is(err, ErrStateIdentityMismatch) {
		t.Fatalf("new Worker epoch manager error = %v, want identity mismatch", err)
	}
}

func TestLocalRecoveryStateRejectsUnsafeNamesAndSymlinks(t *testing.T) {
	root := t.TempDir()
	workerID := uuid.New()
	manager := newTestManager(t, root, workerID, 1)
	identity := Identity{AttemptID: uuid.New(), WorkerID: workerID, WorkerEpoch: 1, Fence: 1}
	handle, err := manager.Open(context.Background(), identity)
	if err != nil {
		t.Fatalf("Open local recovery state: %v", err)
	}
	for _, name := range []string{"../escape", "nested/file", ".", "", "bad name"} {
		if _, err := handle.Write(context.Background(), StageEncoder, name, strings.NewReader("x")); !errors.Is(err, ErrUnsafeName) {
			t.Errorf("Write name %q error = %v, want unsafe-name error", name, err)
		}
	}
	outside := filepath.Join(root, "outside")
	if err := os.WriteFile(outside, []byte("outside"), 0600); err != nil {
		t.Fatalf("write outside fixture: %v", err)
	}
	symlinkPath := filepath.Join(root, "attempts", identity.AttemptID.String(), stateFileName(StageEncoder, "linked"))
	if err := os.Symlink(outside, symlinkPath); err != nil {
		t.Skipf("symlink creation is unavailable: %v", err)
	}
	if _, err := handle.Write(context.Background(), StageEncoder, "linked", strings.NewReader("replace")); !errors.Is(err, ErrInvalidStateDirectory) {
		t.Fatalf("Write through symlink error = %v, want invalid-state error", err)
	}
	if content, err := os.ReadFile(outside); err != nil || string(content) != "outside" {
		t.Fatalf("outside file changed through symlink: content=%q error=%v", content, err)
	}
}

func TestLocalRecoveryStateEnforcesAttemptQuotaAndWatermarks(t *testing.T) {
	root := t.TempDir()
	workerID := uuid.New()
	space := Space{TotalBytes: 1000, FreeBytes: 900}
	manager := newTestManagerWithSpaceAndQuota(t, root, workerID, 1, &space, 4, 4)
	identity := Identity{AttemptID: uuid.New(), WorkerID: workerID, WorkerEpoch: 1, Fence: 1}
	handle, err := manager.Open(context.Background(), identity)
	if err != nil {
		t.Fatalf("Open local recovery state: %v", err)
	}
	if _, err := handle.Write(context.Background(), StageEncoder, "one", strings.NewReader("1234")); err != nil {
		t.Fatalf("first quota write: %v", err)
	}
	if _, err := handle.Write(context.Background(), StageDiT, "two", strings.NewReader("5")); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("second quota write error = %v, want quota exceeded", err)
	}
	state, err := manager.Watermark(context.Background())
	if err != nil || state.State != WatermarkNormal || !state.AssignmentAllowed || !state.ResumeEligible {
		t.Fatalf("normal watermark = %#v, error=%v", state, err)
	}
	space.FreeBytes = 100
	state, err = manager.Watermark(context.Background())
	if err != nil || state.State != WatermarkPressured || state.AssignmentAllowed {
		t.Fatalf("pressured watermark = %#v, error=%v", state, err)
	}
	space.FreeBytes = 10
	if _, err := handle.Write(context.Background(), StageVAE, "critical", strings.NewReader("x")); !errors.Is(err, ErrCriticalWatermark) {
		t.Fatalf("critical write error = %v, want critical watermark", err)
	}
	state, err = manager.Watermark(context.Background())
	if err != nil || state.State != WatermarkCritical || state.AssignmentAllowed {
		t.Fatalf("critical watermark = %#v, error=%v", state, err)
	}
}

func TestLocalRecoveryStateTerminalCleanupAndReconcile(t *testing.T) {
	root := t.TempDir()
	workerID := uuid.New()
	manager := newTestManager(t, root, workerID, 1)
	identity := Identity{AttemptID: uuid.New(), WorkerID: workerID, WorkerEpoch: 1, Fence: 1}
	handle, err := manager.Open(context.Background(), identity)
	if err != nil {
		t.Fatalf("Open local recovery state: %v", err)
	}
	if _, err := handle.Write(context.Background(), StageUpload, "partial", strings.NewReader("upload")); err != nil {
		t.Fatalf("write terminal fixture: %v", err)
	}
	if err := handle.MarkTerminal(context.Background()); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "attempts", identity.AttemptID.String())); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("terminal Attempt directory stat error = %v, want absent", err)
	}
	if _, err := manager.Open(context.Background(), identity); !errors.Is(err, ErrStateTerminal) {
		t.Fatalf("reopen terminal Attempt error = %v, want terminal error", err)
	}
	if _, err := handle.Read(context.Background(), StageUpload, "partial"); !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("read after terminal cleanup error = %v, want not-found", err)
	}

	orphanIdentity := Identity{AttemptID: uuid.New(), WorkerID: workerID, WorkerEpoch: 1, Fence: 2}
	orphanHandle, err := manager.Open(context.Background(), orphanIdentity)
	if err != nil {
		t.Fatalf("Open orphan fixture: %v", err)
	}
	if _, err := orphanHandle.Write(context.Background(), StageEncoder, "state", strings.NewReader("orphan")); err != nil {
		t.Fatalf("write orphan fixture: %v", err)
	}
	old := time.Now().UTC().Add(-2 * time.Hour)
	manager.mu.Lock()
	metadata, err := manager.requireMetadata(manager.attemptPath(orphanIdentity.AttemptID), orphanIdentity)
	if err != nil {
		manager.mu.Unlock()
		t.Fatalf("read orphan metadata: %v", err)
	}
	metadata.TerminalAt = &old
	metadata.UpdatedAt = old
	if err := manager.writeAttemptMetadata(manager.attemptPath(orphanIdentity.AttemptID), metadata); err != nil {
		manager.mu.Unlock()
		t.Fatalf("mark orphan metadata terminal: %v", err)
	}
	manager.mu.Unlock()
	result, err := manager.Reconcile(context.Background())
	if err != nil || result.Removed != 1 {
		t.Fatalf("Reconcile result = %#v, error=%v", result, err)
	}
}

func newTestManager(t *testing.T, root string, workerID uuid.UUID, epoch int64) *Manager {
	t.Helper()
	space := Space{TotalBytes: 1 << 30, FreeBytes: 1 << 29}
	return newTestManagerWithSpace(t, root, workerID, epoch, &space)
}

func newTestManagerWithSpace(t *testing.T, root string, workerID uuid.UUID, epoch int64, space *Space) *Manager {
	t.Helper()
	return newTestManagerWithSpaceAndQuota(t, root, workerID, epoch, space, 64, 16)
}

func newTestManagerWithSpaceAndQuota(
	t *testing.T,
	root string,
	workerID uuid.UUID,
	epoch int64,
	space *Space,
	quotaBytes int64,
	maxEntryBytes int64,
) *Manager {
	t.Helper()
	manager, err := New(Config{
		Root: root, WorkerID: workerID, WorkerEpoch: epoch,
		AttemptQuotaBytes: quotaBytes, MaxEntryBytes: maxEntryBytes, MaxEntries: 4,
		HighWatermarkBytes: 800, LowWatermarkBytes: 400, CriticalFreeBytes: 32,
		TerminalRetention: time.Hour,
		SpaceProbe:        func(string) (Space, error) { return *space, nil },
	})
	if err != nil {
		t.Fatalf("New local recovery manager: %v", err)
	}
	return manager
}
