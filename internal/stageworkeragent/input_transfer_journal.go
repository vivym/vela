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
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/strictjson"
)

const maxInputTransferJournalRecordBytes = 16 << 10

type InputTransferJournalRecord struct {
	TokenDigest [sha256.Size]byte
	Command     stageartifact.ConsumeTransferCommand
	Consumed    bool
}

type InputTransferJournal interface {
	Load(context.Context, [sha256.Size]byte) (InputTransferJournalRecord, bool, error)
	PutPending(context.Context, InputTransferJournalRecord) error
	MarkConsumed(context.Context, InputTransferJournalRecord) error
}

type MemoryInputTransferJournal struct {
	mu      sync.Mutex
	records map[[sha256.Size]byte]InputTransferJournalRecord
}

func NewMemoryInputTransferJournal() *MemoryInputTransferJournal {
	return &MemoryInputTransferJournal{
		records: make(map[[sha256.Size]byte]InputTransferJournalRecord),
	}
}

func (journal *MemoryInputTransferJournal) Load(
	ctx context.Context,
	tokenDigest [sha256.Size]byte,
) (InputTransferJournalRecord, bool, error) {
	if journal == nil || journal.records == nil || ctx == nil || tokenDigest == ([sha256.Size]byte{}) {
		return InputTransferJournalRecord{}, false, errors.New("input transfer journal lookup is invalid")
	}
	if err := ctx.Err(); err != nil {
		return InputTransferJournalRecord{}, false, err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	record, ok := journal.records[tokenDigest]
	return record, ok, nil
}

func (journal *MemoryInputTransferJournal) PutPending(
	ctx context.Context,
	record InputTransferJournalRecord,
) error {
	return journal.put(ctx, record, false)
}

func (journal *MemoryInputTransferJournal) MarkConsumed(
	ctx context.Context,
	record InputTransferJournalRecord,
) error {
	return journal.put(ctx, record, true)
}

func (journal *MemoryInputTransferJournal) put(
	ctx context.Context,
	record InputTransferJournalRecord,
	consumed bool,
) error {
	if journal == nil || journal.records == nil || ctx == nil || !validInputTransferJournalRecord(record) {
		return errors.New("input transfer journal record is invalid")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	existing, ok := journal.records[record.TokenDigest]
	if ok && !sameInputTransferCommand(existing.Command, record.Command) {
		return errors.New("input transfer journal replay does not match")
	}
	if consumed && !ok {
		return errors.New("input transfer journal has no pending consume intent")
	}
	record.Consumed = consumed || existing.Consumed
	journal.records[record.TokenDigest] = record
	return nil
}

type FileInputTransferJournal struct {
	mu   sync.Mutex
	root *os.Root
}

type fileInputTransferRecordV1 struct {
	SchemaVersion int                               `json:"schema_version"`
	TokenDigest   string                            `json:"token_digest"`
	CommandID     string                            `json:"command_id"`
	TicketID      string                            `json:"ticket_id"`
	Destination   stageartifact.TransferDestination `json:"destination"`
	OutcomeDigest string                            `json:"outcome_digest"`
	ConsumedAt    string                            `json:"consumed_at"`
	Consumed      bool                              `json:"consumed"`
}

func NewFileInputTransferJournal(rootPath string) (*FileInputTransferJournal, error) {
	cleaned := filepath.Clean(rootPath)
	if !filepath.IsAbs(cleaned) || cleaned != rootPath {
		return nil, errors.New("file input transfer journal root is invalid")
	}
	if err := os.MkdirAll(cleaned, 0o700); err != nil {
		return nil, fmt.Errorf("create input transfer journal: %w", err)
	}
	root, err := securefile.OpenTrustedRoot(cleaned)
	if err != nil {
		return nil, fmt.Errorf("resolve trusted input transfer journal root: %w", err)
	}
	return &FileInputTransferJournal{root: root}, nil
}

func (journal *FileInputTransferJournal) Close() error {
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.root == nil {
		return nil
	}
	err := journal.root.Close()
	journal.root = nil
	return err
}

func (journal *FileInputTransferJournal) Load(
	ctx context.Context,
	tokenDigest [sha256.Size]byte,
) (InputTransferJournalRecord, bool, error) {
	if journal == nil || ctx == nil || tokenDigest == ([sha256.Size]byte{}) {
		return InputTransferJournalRecord{}, false, errors.New("file input transfer journal lookup is invalid")
	}
	if err := ctx.Err(); err != nil {
		return InputTransferJournalRecord{}, false, err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.root == nil {
		return InputTransferJournalRecord{}, false, errors.New("file input transfer journal is closed")
	}
	document, err := journal.root.ReadFile(journal.path(tokenDigest))
	if errors.Is(err, os.ErrNotExist) {
		return InputTransferJournalRecord{}, false, nil
	}
	if err != nil {
		return InputTransferJournalRecord{}, false, fmt.Errorf("read input transfer journal: %w", err)
	}
	record, err := decodeInputTransferJournalRecord(document)
	if err != nil || record.TokenDigest != tokenDigest {
		return InputTransferJournalRecord{}, false, errors.New("input transfer journal record is invalid")
	}
	return record, true, nil
}

func (journal *FileInputTransferJournal) PutPending(
	ctx context.Context,
	record InputTransferJournalRecord,
) error {
	return journal.put(ctx, record, false)
}

func (journal *FileInputTransferJournal) MarkConsumed(
	ctx context.Context,
	record InputTransferJournalRecord,
) error {
	return journal.put(ctx, record, true)
}

func (journal *FileInputTransferJournal) put(
	ctx context.Context,
	record InputTransferJournalRecord,
	consumed bool,
) error {
	if journal == nil || ctx == nil || !validInputTransferJournalRecord(record) {
		return errors.New("file input transfer journal record is invalid")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.root == nil {
		return errors.New("file input transfer journal is closed")
	}
	path := journal.path(record.TokenDigest)
	document, err := journal.root.ReadFile(path)
	if err == nil {
		existing, decodeErr := decodeInputTransferJournalRecord(document)
		if decodeErr != nil || !sameInputTransferCommand(existing.Command, record.Command) {
			return errors.New("input transfer journal replay does not match")
		}
		if consumed || existing.Consumed {
			record.Consumed = true
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read input transfer journal: %w", err)
	} else if consumed {
		return errors.New("input transfer journal has no pending consume intent")
	}
	record.Consumed = record.Consumed || consumed
	encoded, err := encodeInputTransferJournalRecord(record)
	if err != nil {
		return err
	}
	return writeInputTransferJournalAtomic(journal.root, path, encoded)
}

func (journal *FileInputTransferJournal) path(tokenDigest [sha256.Size]byte) string {
	return hex.EncodeToString(tokenDigest[:]) + ".json"
}

func encodeInputTransferJournalRecord(record InputTransferJournalRecord) ([]byte, error) {
	if !validInputTransferJournalRecord(record) {
		return nil, errors.New("input transfer journal record is invalid")
	}
	document, err := json.Marshal(fileInputTransferRecordV1{
		SchemaVersion: 1, TokenDigest: hex.EncodeToString(record.TokenDigest[:]),
		CommandID: record.Command.CommandID.String(), TicketID: record.Command.TicketID.String(),
		Destination:   record.Command.Destination,
		OutcomeDigest: hex.EncodeToString(record.Command.OutcomeDigest[:]),
		ConsumedAt:    record.Command.ConsumedAt.UTC().Format(time.RFC3339Nano),
		Consumed:      record.Consumed,
	})
	if err != nil || len(document) > maxInputTransferJournalRecordBytes {
		return nil, errors.New("encode input transfer journal record")
	}
	return document, nil
}

func decodeInputTransferJournalRecord(document []byte) (InputTransferJournalRecord, error) {
	if len(document) == 0 || len(document) > maxInputTransferJournalRecordBytes ||
		strictjson.RejectDuplicateKeys(document) != nil {
		return InputTransferJournalRecord{}, errors.New("input transfer journal document is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	var decoded fileInputTransferRecordV1
	if err := decoder.Decode(&decoded); err != nil {
		return InputTransferJournalRecord{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || decoded.SchemaVersion != 1 {
		return InputTransferJournalRecord{}, errors.New("input transfer journal document is invalid")
	}
	commandID, commandErr := uuid.Parse(decoded.CommandID)
	ticketID, ticketErr := uuid.Parse(decoded.TicketID)
	tokenDigest, tokenErr := decodeInputTransferDigest(decoded.TokenDigest)
	outcomeDigest, outcomeErr := decodeInputTransferDigest(decoded.OutcomeDigest)
	consumedAt, timeErr := time.Parse(time.RFC3339Nano, decoded.ConsumedAt)
	if commandErr != nil || ticketErr != nil || tokenErr != nil || outcomeErr != nil || timeErr != nil {
		return InputTransferJournalRecord{}, errors.New("input transfer journal fields are invalid")
	}
	record := InputTransferJournalRecord{
		TokenDigest: tokenDigest,
		Command: stageartifact.ConsumeTransferCommand{
			CommandID: commandID, TicketID: ticketID, TokenDigest: tokenDigest,
			Destination:   decoded.Destination,
			OutcomeDigest: outcomeDigest, ConsumedAt: consumedAt.UTC(),
		},
		Consumed: decoded.Consumed,
	}
	if !validInputTransferJournalRecord(record) {
		return InputTransferJournalRecord{}, errors.New("input transfer journal record is invalid")
	}
	return record, nil
}

func decodeInputTransferDigest(value string) ([sha256.Size]byte, error) {
	var result [sha256.Size]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size || value != hex.EncodeToString(decoded) {
		return result, errors.New("input transfer journal digest is invalid")
	}
	copy(result[:], decoded)
	if result == ([sha256.Size]byte{}) {
		return result, errors.New("input transfer journal digest is empty")
	}
	return result, nil
}

func writeInputTransferJournalAtomic(root *os.Root, path string, document []byte) error {
	temporaryPath := ".input-transfer-" + uuid.NewString() + ".tmp"
	temporary, err := root.OpenFile(temporaryPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create input transfer journal record: %w", err)
	}
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = root.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err := temporary.Write(document); err != nil {
		return fmt.Errorf("write input transfer journal record: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync input transfer journal record: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close input transfer journal record: %w", err)
	}
	if err := root.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("commit input transfer journal record: %w", err)
	}
	directory, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("open input transfer journal root: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil || closeErr != nil {
		return fmt.Errorf("sync input transfer journal root: %w", errors.Join(syncErr, closeErr))
	}
	committed = true
	return nil
}

func validInputTransferJournalRecord(record InputTransferJournalRecord) bool {
	return record.TokenDigest != ([sha256.Size]byte{}) && record.Command.CommandID != uuid.Nil &&
		record.Command.TicketID != uuid.Nil && record.Command.TokenDigest == record.TokenDigest &&
		record.Command.Destination.WorkerInstanceID != uuid.Nil &&
		record.Command.Destination.WorkerInstanceEpoch > 0 &&
		record.Command.Destination.ModelResidencyID != uuid.Nil &&
		record.Command.Destination.ModelRuntimeEpoch > 0 &&
		record.Command.Destination.ConnectorRevisionID != uuid.Nil &&
		record.Command.OutcomeDigest != ([sha256.Size]byte{}) && !record.Command.ConsumedAt.IsZero()
}

func sameInputTransferCommand(
	left stageartifact.ConsumeTransferCommand,
	right stageartifact.ConsumeTransferCommand,
) bool {
	return left.CommandID == right.CommandID && left.TicketID == right.TicketID &&
		left.TokenDigest == right.TokenDigest && left.Destination == right.Destination &&
		left.OutcomeDigest == right.OutcomeDigest &&
		left.ConsumedAt.Equal(right.ConsumedAt)
}

var _ InputTransferJournal = (*MemoryInputTransferJournal)(nil)
var _ InputTransferJournal = (*FileInputTransferJournal)(nil)
