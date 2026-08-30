package stageworkeragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const maxMaterializationJournalRecordBytes = 4 << 20

type FileMaterializationJournal struct {
	mu    sync.Mutex
	root  string
	limit int
}

type fileMaterializationRecordV1 struct {
	SchemaVersion            int    `json:"schema_version"`
	ID                       string `json:"id"`
	StageAuthority           []byte `json:"stage_authority"`
	LocalReceipt             []byte `json:"local_receipt"`
	MaterializationAuthority []byte `json:"materialization_authority,omitempty"`
	ObjectVersion            string `json:"object_version,omitempty"`
	SourceLossFingerprint    []byte `json:"source_loss_fingerprint,omitempty"`
	SourceLossResourceUnits  int64  `json:"source_loss_resource_units,omitempty"`
	SourceLostAt             string `json:"source_lost_at,omitempty"`
	SourceRetryAt            string `json:"source_retry_at,omitempty"`
}

func NewFileMaterializationJournal(
	root string,
	limit int,
) (*FileMaterializationJournal, error) {
	root = strings.TrimSpace(root)
	if root == "" || limit <= 0 || limit > 10000 {
		return nil, errors.New("file materialization journal configuration is invalid")
	}
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve materialization journal root: %w", err)
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, fmt.Errorf("create materialization journal root: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, fmt.Errorf("resolve materialization journal symlinks: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return nil, errors.New("materialization journal root is not a directory")
	}
	journal := &FileMaterializationJournal{root: resolved, limit: limit}
	records, err := journal.load()
	if err != nil {
		return nil, err
	}
	if len(records) > limit {
		return nil, ErrMaterializationJournalFull
	}
	return journal, nil
}

func (journal *FileMaterializationJournal) Put(
	ctx context.Context,
	record PendingMaterialization,
) error {
	if journal == nil || journal.root == "" || ctx == nil {
		return errors.New("file materialization journal is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validatePendingMaterialization(record); err != nil {
		return err
	}
	document, err := encodeFileMaterializationRecord(record)
	if err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	path := journal.recordPath(record.ID)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		records, loadErr := journal.loadLocked()
		if loadErr != nil {
			return loadErr
		}
		if len(records) >= journal.limit {
			return ErrMaterializationJournalFull
		}
	} else if err != nil {
		return fmt.Errorf("inspect materialization journal record: %w", err)
	}
	return journal.writeAtomic(path, document)
}

func (journal *FileMaterializationJournal) List(
	ctx context.Context,
) ([]PendingMaterialization, error) {
	if journal == nil || journal.root == "" || ctx == nil {
		return nil, errors.New("file materialization journal is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	return journal.loadLocked()
}

func (journal *FileMaterializationJournal) Delete(ctx context.Context, id string) error {
	if journal == nil || journal.root == "" || ctx == nil || id == "" {
		return errors.New("file materialization journal delete is invalid")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	err := os.Remove(journal.recordPath(id))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("delete materialization journal record: %w", err)
	}
	return syncDirectory(journal.root)
}

func (journal *FileMaterializationJournal) load() ([]PendingMaterialization, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	return journal.loadLocked()
}

func (journal *FileMaterializationJournal) loadLocked() ([]PendingMaterialization, error) {
	entries, err := os.ReadDir(journal.root)
	if err != nil {
		return nil, fmt.Errorf("read materialization journal: %w", err)
	}
	records := make([]PendingMaterialization, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(journal.root, entry.Name())
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 ||
			info.Size() > maxMaterializationJournalRecordBytes {
			return nil, errors.New("materialization journal record is invalid")
		}
		document, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read materialization journal record: %w", err)
		}
		record, err := decodeFileMaterializationRecord(document)
		if err != nil {
			return nil, fmt.Errorf("decode materialization journal record %s: %w", entry.Name(), err)
		}
		if journal.recordPath(record.ID) != path {
			return nil, errors.New("materialization journal filename does not match record identity")
		}
		records = append(records, record)
	}
	slices.SortFunc(records, func(left, right PendingMaterialization) int {
		leftTime := left.LocalReceipt.GetSealedAt().AsTime()
		rightTime := right.LocalReceipt.GetSealedAt().AsTime()
		if comparison := leftTime.Compare(rightTime); comparison != 0 {
			return comparison
		}
		return strings.Compare(left.ID, right.ID)
	})
	return records, nil
}

func (journal *FileMaterializationJournal) recordPath(id string) string {
	digest := sha256.Sum256([]byte(id))
	return filepath.Join(journal.root, hex.EncodeToString(digest[:])+".json")
}

func (journal *FileMaterializationJournal) writeAtomic(path string, document []byte) error {
	temporary, err := os.CreateTemp(journal.root, ".materialization-*.tmp")
	if err != nil {
		return fmt.Errorf("create materialization journal temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err := temporary.Write(document); err != nil {
		return fmt.Errorf("write materialization journal record: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync materialization journal record: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close materialization journal record: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("commit materialization journal record: %w", err)
	}
	if err := syncDirectory(journal.root); err != nil {
		return err
	}
	committed = true
	return nil
}

func encodeFileMaterializationRecord(record PendingMaterialization) ([]byte, error) {
	marshal := proto.MarshalOptions{Deterministic: true}
	stage, err := marshal.Marshal(record.StageAuthority)
	if err != nil {
		return nil, err
	}
	receipt, err := marshal.Marshal(record.LocalReceipt)
	if err != nil {
		return nil, err
	}
	var authority []byte
	if record.MaterializationAuthority != nil {
		authority, err = marshal.Marshal(record.MaterializationAuthority)
		if err != nil {
			return nil, err
		}
	}
	diskRecord := fileMaterializationRecordV1{
		SchemaVersion: 1, ID: record.ID, StageAuthority: stage, LocalReceipt: receipt,
		MaterializationAuthority: authority, ObjectVersion: record.ObjectVersion,
	}
	if record.SourceLoss != nil {
		diskRecord.SourceLossFingerprint = append(
			[]byte(nil), record.SourceLoss.FailureFingerprint[:]...,
		)
		diskRecord.SourceLossResourceUnits = record.SourceLoss.ConsumedResourceUnits
		diskRecord.SourceLostAt = record.SourceLoss.LostAt.UTC().Format(time.RFC3339Nano)
		diskRecord.SourceRetryAt = record.SourceLoss.RetryAt.UTC().Format(time.RFC3339Nano)
	}
	document, err := json.Marshal(diskRecord)
	if err != nil {
		return nil, err
	}
	if len(document) > maxMaterializationJournalRecordBytes {
		return nil, errors.New("materialization journal record exceeds size bound")
	}
	return document, nil
}

func decodeFileMaterializationRecord(document []byte) (PendingMaterialization, error) {
	if len(document) == 0 || len(document) > maxMaterializationJournalRecordBytes {
		return PendingMaterialization{}, errors.New("materialization journal document is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	var decoded fileMaterializationRecordV1
	if err := decoder.Decode(&decoded); err != nil {
		return PendingMaterialization{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return PendingMaterialization{}, errors.New("materialization journal contains trailing data")
	}
	if decoded.SchemaVersion != 1 || decoded.ID == "" || len(decoded.StageAuthority) == 0 ||
		len(decoded.LocalReceipt) == 0 {
		return PendingMaterialization{}, errors.New("materialization journal fields are incomplete")
	}
	record := PendingMaterialization{
		ID: decoded.ID, StageAuthority: &velav1.StageAuthority{},
		LocalReceipt: &velav1.LocalMaterializationReceipt{}, ObjectVersion: decoded.ObjectVersion,
	}
	if err := proto.Unmarshal(decoded.StageAuthority, record.StageAuthority); err != nil {
		return PendingMaterialization{}, err
	}
	if err := proto.Unmarshal(decoded.LocalReceipt, record.LocalReceipt); err != nil {
		return PendingMaterialization{}, err
	}
	if len(decoded.MaterializationAuthority) != 0 {
		record.MaterializationAuthority = &velav1.MaterializationAuthority{}
		if err := proto.Unmarshal(decoded.MaterializationAuthority, record.MaterializationAuthority); err != nil {
			return PendingMaterialization{}, err
		}
	}
	if len(decoded.SourceLossFingerprint) != 0 || decoded.SourceLossResourceUnits != 0 ||
		decoded.SourceLostAt != "" || decoded.SourceRetryAt != "" {
		lostAt, err := time.Parse(time.RFC3339Nano, decoded.SourceLostAt)
		if err != nil {
			return PendingMaterialization{}, err
		}
		retryAt, err := time.Parse(time.RFC3339Nano, decoded.SourceRetryAt)
		if err != nil {
			return PendingMaterialization{}, err
		}
		evidence := MaterializationSourceLossEvidence{
			ConsumedResourceUnits: decoded.SourceLossResourceUnits,
			LostAt:                lostAt.UTC(), RetryAt: retryAt.UTC(),
		}
		copy(evidence.FailureFingerprint[:], decoded.SourceLossFingerprint)
		record.SourceLoss = &evidence
	}
	if err := validatePendingMaterialization(record); err != nil {
		return PendingMaterialization{}, err
	}
	return record, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open materialization journal directory: %w", err)
	}
	defer func() { _ = directory.Close() }()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync materialization journal directory: %w", err)
	}
	return nil
}
