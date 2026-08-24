package nodeagent

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

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
	info, err := os.Stat(directory)
	if err != nil || !info.IsDir() {
		return nil, errors.New("node Agent receipt directory is not a directory")
	}
	return &FileLedger{directory: directory}, nil
}

type fileReceipt struct {
	RequestHash   string `json:"request_hash"`
	ActorIdentity string `json:"actor_identity"`
	Result        Result `json:"result"`
}

func (ledger *FileLedger) Load(ctx context.Context, operationID uuid.UUID) (Receipt, bool, error) {
	if ledger == nil || ledger.directory == "" {
		return Receipt{}, false, errors.New("node Agent file ledger is not configured")
	}
	if err := contextError(ctx); err != nil {
		return Receipt{}, false, err
	}
	path := ledger.path(operationID)
	content, err := os.ReadFile(path)
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
	return receipt, true, nil
}

func (ledger *FileLedger) Save(ctx context.Context, receipt Receipt) error {
	if ledger == nil || ledger.directory == "" {
		return errors.New("node Agent file ledger is not configured")
	}
	if receipt.Result.OperationID == uuid.Nil {
		return errors.New("node Agent receipt operation id is required")
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
	temporary, err := os.CreateTemp(ledger.directory, ".receipt-*.tmp")
	if err != nil {
		return fmt.Errorf("create node Agent receipt temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("restrict node Agent receipt permissions: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		return fmt.Errorf("write node Agent receipt: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync node Agent receipt: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close node Agent receipt: %w", err)
	}
	// Link publishes the fully synced inode without replacing a receipt another
	// process may have committed concurrently.
	if err := os.Link(temporaryPath, path); err != nil {
		if prior, found, loadErr := ledger.Load(ctx, receipt.Result.OperationID); loadErr == nil && found &&
			prior.RequestHash == receipt.RequestHash && prior.ActorIdentity == receipt.ActorIdentity && equalResult(prior.Result, receipt.Result) {
			return nil
		}
		return fmt.Errorf("publish node Agent receipt: %w", err)
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
