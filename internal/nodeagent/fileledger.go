package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

const maxLedgerRecordBytes = 64 * 1024

// FileLedger is the host-local durable receipt authority. It stores only
// operation metadata and result digests, never command output or Customer
// Content. The control plane remains the authoritative business ledger.
type FileLedger struct {
	directory string
}

func NewFileLedger(directory string) (*FileLedger, error) {
	if directory == "" || !filepath.IsAbs(directory) {
		return nil, errors.New("node Agent receipt directory must be absolute")
	}
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return nil, fmt.Errorf("create node Agent receipt directory: %w", err)
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
		return nil, errors.New("node Agent receipt directory does not satisfy the security contract")
	}
	return &FileLedger{directory: directory}, nil
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
	path := ledger.intentPath(intent.OperationID)
	if prior, found, err := ledger.loadIntent(ctx, intent.OperationID); err != nil {
		return ExecutionIntentResult{}, err
	} else if found {
		if prior.RequestHash != intent.RequestHash || prior.ActorIdentity != intent.ActorIdentity {
			return ExecutionIntentResult{}, errors.New("node Agent execution intent conflicts with an existing operation")
		}
		return ExecutionIntentResult{StartedAt: prior.StartedAt}, nil
	}
	encoded, err := json.Marshal(fileExecutionIntent{
		OperationID: intent.OperationID, RequestHash: hex.EncodeToString(intent.RequestHash[:]),
		ActorIdentity: intent.ActorIdentity, StartedAt: intent.StartedAt,
	})
	if err != nil {
		return ExecutionIntentResult{}, fmt.Errorf("encode node Agent execution intent: %w", err)
	}
	if err := ledger.publish(path, ".intent-*.tmp", encoded); err != nil {
		if prior, found, loadErr := ledger.loadIntent(ctx, intent.OperationID); loadErr == nil && found &&
			prior.RequestHash == intent.RequestHash && prior.ActorIdentity == intent.ActorIdentity {
			return ExecutionIntentResult{StartedAt: prior.StartedAt}, nil
		}
		return ExecutionIntentResult{}, fmt.Errorf("publish node Agent execution intent: %w", err)
	}
	return ExecutionIntentResult{Acquired: true, StartedAt: intent.StartedAt}, nil
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

func (ledger *FileLedger) Save(ctx context.Context, receipt Receipt) error {
	if ledger == nil || ledger.directory == "" {
		return errors.New("node Agent file ledger is not configured")
	}
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
		if prior, found, loadErr := ledger.Load(ctx, receipt.Result.OperationID); loadErr == nil && found &&
			prior.RequestHash == receipt.RequestHash && prior.ActorIdentity == receipt.ActorIdentity && equalResult(prior.Result, receipt.Result) {
			return nil
		}
		return fmt.Errorf("publish node Agent receipt: %w", err)
	}
	return nil
}

func readLedgerRecord(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 ||
		info.Size() <= 0 || info.Size() > maxLedgerRecordBytes {
		return nil, errors.New("node Agent ledger record does not satisfy the security contract")
	}
	content, err := io.ReadAll(io.LimitReader(file, maxLedgerRecordBytes+1))
	if err != nil || len(content) == 0 || len(content) > maxLedgerRecordBytes {
		return nil, errors.New("node Agent ledger record exceeds its bound")
	}
	return content, nil
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
	directory, err := os.Open(ledger.directory)
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
