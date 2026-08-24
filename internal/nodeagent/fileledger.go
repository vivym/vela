package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/securefile"
)

const maxLedgerRecordBytes = 64 * 1024

var errExecutionInProgress = errors.New("node Agent execution is already in progress")

// FileLedger is the host-local durable receipt authority. It stores only
// operation metadata and result digests, never command output or Customer
// Content. The control plane remains the authoritative business ledger.
type FileLedger struct {
	directory      string
	mu             sync.Mutex
	executionLocks map[uuid.UUID]*os.File
	syncDirectory  func() error
}

func NewFileLedger(directory string) (*FileLedger, error) {
	if directory == "" || !filepath.IsAbs(directory) {
		return nil, errors.New("node Agent receipt directory must be absolute")
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return nil, fmt.Errorf("create node Agent receipt directory: %w", err)
	}
	if err := securefile.ValidateDirectory(directory); err != nil {
		return nil, errors.New("node Agent receipt directory does not satisfy the security contract")
	}
	ledger := &FileLedger{directory: directory, executionLocks: make(map[uuid.UUID]*os.File)}
	ledger.syncDirectory = func() error { return syncDirectory(directory) }
	return ledger, nil
}

type fileReceipt struct {
	RequestHash   string `json:"request_hash"`
	ActorIdentity string `json:"actor_identity"`
	Result        Result `json:"result"`
}

type fileExecutionIntent struct {
	OperationID   uuid.UUID `json:"operation_id"`
	RequestHash   string    `json:"request_hash"`
	ActorIdentity string    `json:"actor_identity"`
	StartedAt     time.Time `json:"started_at"`
}

func (ledger *FileLedger) Begin(ctx context.Context, intent ExecutionIntent) (ExecutionIntentResult, error) {
	if ledger == nil || ledger.directory == "" {
		return ExecutionIntentResult{}, errors.New("node Agent file ledger is not configured")
	}
	if intent.OperationID == uuid.Nil || intent.RequestHash == [sha256.Size]byte{} ||
		!validText(intent.ActorIdentity, maxIdentityText) || intent.StartedAt.IsZero() {
		return ExecutionIntentResult{}, errors.New("node Agent execution intent is invalid")
	}
	if err := contextError(ctx); err != nil {
		return ExecutionIntentResult{}, err
	}
	if err := ledger.acquireExecutionLock(intent.OperationID); err != nil {
		return ExecutionIntentResult{}, err
	}
	keepLock := false
	defer func() {
		if !keepLock {
			_ = ledger.releaseExecutionLock(intent.OperationID)
		}
	}()
	path := ledger.intentPath(intent.OperationID)
	if prior, found, err := ledger.loadIntent(ctx, intent.OperationID); err != nil {
		return ExecutionIntentResult{}, err
	} else if found {
		if prior.RequestHash != intent.RequestHash || prior.ActorIdentity != intent.ActorIdentity {
			return ExecutionIntentResult{}, errors.New("node Agent execution intent conflicts with an existing operation")
		}
		keepLock = true
		return ExecutionIntentResult{StartedAt: prior.StartedAt, release: func() error {
			return ledger.releaseExecutionLock(intent.OperationID)
		}}, nil
	}
	encoded, err := json.Marshal(fileExecutionIntent{
		OperationID: intent.OperationID, RequestHash: hex.EncodeToString(intent.RequestHash[:]),
		ActorIdentity: intent.ActorIdentity, StartedAt: intent.StartedAt,
	})
	if err != nil {
		return ExecutionIntentResult{}, fmt.Errorf("encode node Agent execution intent: %w", err)
	}
	if err := ledger.publish(path, ".intent-*.tmp", encoded); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return ExecutionIntentResult{}, fmt.Errorf("publish node Agent execution intent: %w", err)
		}
		if prior, found, loadErr := ledger.loadIntent(ctx, intent.OperationID); loadErr == nil && found &&
			prior.RequestHash == intent.RequestHash && prior.ActorIdentity == intent.ActorIdentity {
			keepLock = true
			return ExecutionIntentResult{StartedAt: prior.StartedAt, release: func() error {
				return ledger.releaseExecutionLock(intent.OperationID)
			}}, nil
		}
		return ExecutionIntentResult{}, fmt.Errorf("publish node Agent execution intent: %w", err)
	}
	keepLock = true
	return ExecutionIntentResult{Acquired: true, StartedAt: intent.StartedAt, release: func() error {
		return ledger.releaseExecutionLock(intent.OperationID)
	}}, nil
}

func (ledger *FileLedger) loadIntent(ctx context.Context, operationID uuid.UUID) (ExecutionIntent, bool, error) {
	if err := contextError(ctx); err != nil {
		return ExecutionIntent{}, false, err
	}
	content, err := readLedgerRecord(ledger.intentPath(operationID))
	if errors.Is(err, os.ErrNotExist) {
		return ExecutionIntent{}, false, nil
	}
	if err != nil {
		return ExecutionIntent{}, false, fmt.Errorf("read node Agent execution intent: %w", err)
	}
	if err := ledger.confirmDirectoryDurability(); err != nil {
		return ExecutionIntent{}, false, err
	}
	var stored fileExecutionIntent
	if err := json.Unmarshal(content, &stored); err != nil {
		return ExecutionIntent{}, false, fmt.Errorf("decode node Agent execution intent: %w", err)
	}
	requestHash, err := hex.DecodeString(stored.RequestHash)
	if err != nil || len(requestHash) != sha256.Size || stored.OperationID != operationID ||
		!validText(stored.ActorIdentity, maxIdentityText) || stored.StartedAt.IsZero() {
		return ExecutionIntent{}, false, errors.New("node Agent execution intent is invalid")
	}
	var result ExecutionIntent
	result.OperationID = stored.OperationID
	copy(result.RequestHash[:], requestHash)
	result.ActorIdentity = stored.ActorIdentity
	result.StartedAt = stored.StartedAt
	return result, true, nil
}

func (ledger *FileLedger) Load(ctx context.Context, operationID uuid.UUID) (Receipt, bool, error) {
	if ledger == nil || ledger.directory == "" {
		return Receipt{}, false, errors.New("node Agent file ledger is not configured")
	}
	if err := contextError(ctx); err != nil {
		return Receipt{}, false, err
	}
	path := ledger.path(operationID)
	content, err := readLedgerRecord(path)
	if errors.Is(err, os.ErrNotExist) {
		return Receipt{}, false, nil
	}
	if err != nil {
		return Receipt{}, false, fmt.Errorf("read node Agent receipt: %w", err)
	}
	if err := ledger.confirmDirectoryDurability(); err != nil {
		return Receipt{}, false, err
	}
	var stored fileReceipt
	if err := json.Unmarshal(content, &stored); err != nil {
		return Receipt{}, false, fmt.Errorf("decode node Agent receipt: %w", err)
	}
	requestHash, err := hex.DecodeString(stored.RequestHash)
	if err != nil || len(requestHash) != len(Receipt{}.RequestHash) {
		return Receipt{}, false, errors.New("node Agent receipt request hash is invalid")
	}
	var receipt Receipt
	copy(receipt.RequestHash[:], requestHash)
	receipt.ActorIdentity = stored.ActorIdentity
	receipt.Result = stored.Result
	if !validReceipt(receipt) {
		return Receipt{}, false, errors.New("node Agent receipt is invalid")
	}
	return receipt, true, nil
}

func (ledger *FileLedger) confirmDirectoryDurability() error {
	if ledger.syncDirectory == nil {
		return errors.New("node Agent ledger directory sync is not configured")
	}
	if err := ledger.syncDirectory(); err != nil {
		return fmt.Errorf("confirm node Agent ledger directory durability: %w", err)
	}
	return nil
}

func (ledger *FileLedger) Save(ctx context.Context, receipt Receipt) error {
	if ledger == nil || ledger.directory == "" {
		return errors.New("node Agent file ledger is not configured")
	}
	defer func() { _ = ledger.releaseExecutionLock(receipt.Result.OperationID) }()
	if !validReceipt(receipt) {
		return errors.New("node Agent receipt is invalid")
	}
	if err := contextError(ctx); err != nil {
		return err
	}
	path := ledger.path(receipt.Result.OperationID)
	if prior, found, err := ledger.Load(ctx, receipt.Result.OperationID); err != nil {
		return err
	} else if found {
		if prior.RequestHash == receipt.RequestHash && prior.ActorIdentity == receipt.ActorIdentity &&
			equalResult(prior.Result, receipt.Result) {
			return nil
		}
		return errors.New("node Agent receipt conflicts with an existing operation")
	}
	encoded, err := json.Marshal(fileReceipt{
		RequestHash:   hex.EncodeToString(receipt.RequestHash[:]),
		ActorIdentity: receipt.ActorIdentity,
		Result:        receipt.Result,
	})
	if err != nil {
		return fmt.Errorf("encode node Agent receipt: %w", err)
	}
	if err := ledger.publish(path, ".receipt-*.tmp", encoded); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("publish node Agent receipt: %w", err)
		}
		if prior, found, loadErr := ledger.Load(ctx, receipt.Result.OperationID); loadErr == nil && found &&
			prior.RequestHash == receipt.RequestHash && prior.ActorIdentity == receipt.ActorIdentity && equalResult(prior.Result, receipt.Result) {
			return nil
		}
		return fmt.Errorf("publish node Agent receipt: %w", err)
	}
	return nil
}

func readLedgerRecord(path string) ([]byte, error) {
	return securefile.Read(path, maxLedgerRecordBytes, true)
}

func validReceipt(receipt Receipt) bool {
	result := receipt.Result
	return receipt.RequestHash != [sha256.Size]byte{} && validText(receipt.ActorIdentity, maxIdentityText) &&
		result.OperationID != uuid.Nil && validText(result.ResultCode, 200) &&
		validText(result.ResultDetail, maxDetailText) && !result.StartedAt.IsZero() &&
		!result.FinishedAt.IsZero() && !result.FinishedAt.Before(result.StartedAt) &&
		(!result.Success && (len(result.PostcheckHash) == 0 || len(result.PostcheckHash) == sha256.Size) ||
			result.Success && len(result.PostcheckHash) == sha256.Size)
}

func (ledger *FileLedger) publish(path, pattern string, encoded []byte) error {
	temporary, err := os.CreateTemp(ledger.directory, pattern)
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err := temporary.Write(encoded); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	// Link publishes the fully synced inode without replacing a record another
	// process may have committed concurrently.
	if err := os.Link(temporaryPath, path); err != nil {
		return err
	}
	if ledger.syncDirectory == nil {
		return errors.New("node Agent ledger directory sync is not configured")
	}
	return ledger.syncDirectory()
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	if err := directory.Sync(); err != nil {
		return err
	}
	return nil
}

func equalResult(left, right Result) bool {
	if left.OperationID != right.OperationID || left.Success != right.Success ||
		left.ResultCode != right.ResultCode || left.ResultDetail != right.ResultDetail ||
		left.StartedAt != right.StartedAt || left.FinishedAt != right.FinishedAt {
		return false
	}
	if len(left.PostcheckHash) != len(right.PostcheckHash) {
		return false
	}
	for index := range left.PostcheckHash {
		if left.PostcheckHash[index] != right.PostcheckHash[index] {
			return false
		}
	}
	return true
}

func (ledger *FileLedger) path(operationID uuid.UUID) string {
	return filepath.Join(ledger.directory, operationID.String()+".json")
}

func (ledger *FileLedger) intentPath(operationID uuid.UUID) string {
	return filepath.Join(ledger.directory, operationID.String()+".intent.json")
}

func (ledger *FileLedger) lockPath(operationID uuid.UUID) string {
	return filepath.Join(ledger.directory, operationID.String()+".execution.lock")
}

func (ledger *FileLedger) acquireExecutionLock(operationID uuid.UUID) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if _, held := ledger.executionLocks[operationID]; held {
		return errExecutionInProgress
	}
	lock, err := securefile.OpenPrivateState(ledger.lockPath(operationID))
	if err != nil {
		return fmt.Errorf("open node Agent execution lock: %w", err)
	}
	closeOnError := func() { _ = lock.Close() }
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		closeOnError()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return errExecutionInProgress
		}
		return fmt.Errorf("lock node Agent execution intent: %w", err)
	}
	ledger.executionLocks[operationID] = lock
	return nil
}

func (ledger *FileLedger) releaseExecutionLock(operationID uuid.UUID) error {
	ledger.mu.Lock()
	lock := ledger.executionLocks[operationID]
	delete(ledger.executionLocks, operationID)
	ledger.mu.Unlock()
	if lock == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	closeErr := lock.Close()
	return errors.Join(unlockErr, closeErr)
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

var _ Ledger = (*FileLedger)(nil)
var _ HostLedger = (*FileLedger)(nil)
