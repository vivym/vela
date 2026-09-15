//go:build linux

package stageworkeragent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/runtimechannel"
	"github.com/vivym/vela/internal/workerjournalwire"
)

var (
	ErrRemoteWorkerJournalRejected  = errors.New("node rejected Worker journal operation")
	ErrRemoteWorkerJournalUncertain = errors.New("worker journal mutation outcome is uncertain")
	ErrRemoteWorkerJournalClosed    = errors.New("worker journal client is closed")
)

type RemoteWorkerJournalConfig struct {
	Socket            string
	PIDFDBrokerSocket string
	Identity          workerjournalwire.Identity
}

type RemoteInputTransferJournal struct {
	mu     sync.Mutex
	closed bool
	config RemoteWorkerJournalConfig
}

type RemoteMaterializationJournal struct {
	mu     sync.Mutex
	closed bool
	config RemoteWorkerJournalConfig
}

func NewRemoteInputTransferJournal(config RemoteWorkerJournalConfig) (*RemoteInputTransferJournal, error) {
	if err := validateRemoteWorkerJournalConfig(config); err != nil {
		return nil, err
	}
	return &RemoteInputTransferJournal{config: config}, nil
}

func NewRemoteMaterializationJournal(config RemoteWorkerJournalConfig) (*RemoteMaterializationJournal, error) {
	if err := validateRemoteWorkerJournalConfig(config); err != nil {
		return nil, err
	}
	return &RemoteMaterializationJournal{config: config}, nil
}

func (journal *RemoteInputTransferJournal) Load(ctx context.Context, digest [32]byte) (InputTransferJournalRecord, bool, error) {
	if err := journal.check(ctx); err != nil {
		return InputTransferJournalRecord{}, false, err
	}
	response, err := journal.exchange(ctx, &workerjournalwire.InputRequest{Operation: "load", TokenDigest: hexDigest(digest)})
	if err != nil || !response.Found {
		return InputTransferJournalRecord{}, response.Found, err
	}
	record, err := DecodeInputTransferJournalRecord(response.InputRecord)
	if err != nil || record.TokenDigest != digest {
		return InputTransferJournalRecord{}, false, err
	}
	return record, true, nil
}

func (journal *RemoteInputTransferJournal) PutPending(ctx context.Context, record InputTransferJournalRecord) error {
	return journal.put(ctx, "put_pending", record)
}

func (journal *RemoteInputTransferJournal) MarkConsumed(ctx context.Context, record InputTransferJournalRecord) error {
	return journal.put(ctx, "mark_consumed", record)
}

func (journal *RemoteInputTransferJournal) put(ctx context.Context, operation string, record InputTransferJournalRecord) error {
	if err := journal.check(ctx); err != nil {
		return err
	}
	wire, err := EncodeInputTransferJournalRecord(record)
	if err != nil {
		return err
	}
	_, err = journal.exchange(ctx, &workerjournalwire.InputRequest{Operation: operation, TokenDigest: hexDigest(record.TokenDigest), Record: wire})
	return err
}

func (journal *RemoteInputTransferJournal) Close() error {
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	journal.closed = true
	journal.mu.Unlock()
	return nil
}

func (journal *RemoteInputTransferJournal) check(ctx context.Context) error {
	if journal == nil || ctx == nil {
		return ErrRemoteWorkerJournalClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	journal.mu.Lock()
	closed := journal.closed
	journal.mu.Unlock()
	if closed {
		return ErrRemoteWorkerJournalClosed
	}
	return nil
}

func (journal *RemoteInputTransferJournal) exchange(ctx context.Context, input *workerjournalwire.InputRequest) (workerjournalwire.Response, error) {
	wire, digest, err := workerjournalwire.EncodeRequest(journal.config.Identity, input, nil)
	if err != nil {
		return workerjournalwire.Response{}, err
	}
	reply, err := runtimechannel.ExchangeWithRequestLimitAndPIDFDBroker(ctx, journal.config.Socket, journal.config.PIDFDBrokerSocket, wire, workerjournalwire.MaximumRequestBytes)
	if err != nil {
		return workerjournalwire.Response{}, ErrRemoteWorkerJournalUncertain
	}
	response, err := workerjournalwire.ParseResponse(reply, digest)
	if err != nil {
		return workerjournalwire.Response{}, err
	}
	return response, remoteJournalResponseError(response)
}

func (journal *RemoteMaterializationJournal) EnsureCapacity(ctx context.Context) error {
	_, err := journal.exchange(ctx, &workerjournalwire.MaterializeRequest{Operation: "ensure_capacity"})
	return err
}

func (journal *RemoteMaterializationJournal) Put(ctx context.Context, record PendingMaterialization) error {
	wire, err := EncodePendingMaterialization(record)
	if err != nil {
		return err
	}
	return journal.sendChunks(ctx, "put", record.ID, wire)
}

func (journal *RemoteMaterializationJournal) List(ctx context.Context) ([]PendingMaterialization, error) {
	records := make([]PendingMaterialization, 0)
	offset := 0
	expectedDigest := ""
	for {
		response, err := journal.exchange(ctx, &workerjournalwire.MaterializeRequest{Operation: "list", Offset: offset, PageSize: 1, ChunkOffset: 0})
		if err != nil {
			return nil, err
		}
		if expectedDigest == "" {
			expectedDigest = response.MaterializationDigest
		} else if response.MaterializationDigest != expectedDigest {
			return nil, ErrRemoteWorkerJournalUncertain
		}
		if len(response.MaterializationRecordChunk) != 0 {
			if response.MaterializationRecordOffset != 0 || response.MaterializationRecordTotal <= 0 || response.MaterializationRecordDigest == "" {
				return nil, ErrRemoteWorkerJournalUncertain
			}
			total := response.MaterializationRecordTotal
			digestHex := response.MaterializationRecordDigest
			var assembled []byte
			for {
				if response.MaterializationRecordOffset != len(assembled) || response.MaterializationRecordTotal != total || response.MaterializationRecordDigest != digestHex {
					return nil, ErrRemoteWorkerJournalUncertain
				}
				assembled = append(assembled, response.MaterializationRecordChunk...)
				if !response.MaterializationRecordMore {
					break
				}
				next, chunkErr := journal.exchange(ctx, &workerjournalwire.MaterializeRequest{Operation: "list", Offset: offset, PageSize: 1, ChunkOffset: len(assembled)})
				if chunkErr != nil || next.MaterializationDigest != expectedDigest || next.MaterializationRecordTotal != total || next.MaterializationRecordDigest != digestHex || next.MaterializationRecordOffset != len(assembled) {
					return nil, ErrRemoteWorkerJournalUncertain
				}
				response = next
			}
			digest, decodeErr := decodeHexDigest(response.MaterializationRecordDigest)
			if decodeErr != nil || sha256.Sum256(assembled) != digest {
				return nil, ErrRemoteWorkerJournalUncertain
			}
			record, decodeErr := DecodePendingMaterialization(assembled)
			if decodeErr != nil {
				return nil, decodeErr
			}
			records = append(records, record)
			offset++
			if !response.MaterializationMore {
				return records, nil
			}
			continue
		}
		for _, wire := range response.MaterializationList {
			record, decodeErr := DecodePendingMaterialization(wire)
			if decodeErr != nil {
				return nil, decodeErr
			}
			records = append(records, record)
		}
		if !response.MaterializationMore {
			return records, nil
		}
		if response.MaterializationNext <= offset || response.MaterializationNext != offset+len(response.MaterializationList) {
			return nil, ErrRemoteWorkerJournalUncertain
		}
		offset = response.MaterializationNext
	}
}

func (journal *RemoteMaterializationJournal) Delete(ctx context.Context, id string) error {
	if err := journal.check(ctx); err != nil {
		return err
	}
	records, err := journal.List(ctx)
	if err != nil {
		return err
	}
	var selected *PendingMaterialization
	for index := range records {
		if records[index].ID == id {
			selected = &records[index]
			break
		}
	}
	if selected == nil {
		return nil
	}
	wire, err := EncodePendingMaterialization(*selected)
	if err != nil {
		return err
	}
	return journal.sendChunks(ctx, "delete", id, wire)
}

func (journal *RemoteMaterializationJournal) sendChunks(ctx context.Context, operation, id string, record []byte) error {
	if err := journal.check(ctx); err != nil {
		return err
	}
	if len(record) == 0 || len(record) > 4<<20 {
		return ErrRemoteWorkerJournalRejected
	}
	digest := sha256.Sum256(record)
	requestID := uuid.New()
	for offset := 0; offset < len(record); {
		end := offset + 12<<10
		if end > len(record) {
			end = len(record)
		}
		_, err := journal.exchangeWithID(ctx, requestID, &workerjournalwire.MaterializeRequest{Operation: operation, ID: id, Record: record[offset:end], ChunkOffset: offset, ChunkTotal: len(record), ChunkDigest: hexDigest(digest), ChunkFinal: end == len(record)})
		if err != nil {
			return err
		}
		offset = end
	}
	return nil
}

func (journal *RemoteMaterializationJournal) Close() error {
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	journal.closed = true
	journal.mu.Unlock()
	return nil
}

func (journal *RemoteMaterializationJournal) check(ctx context.Context) error {
	if journal == nil || ctx == nil {
		return ErrRemoteWorkerJournalClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	journal.mu.Lock()
	closed := journal.closed
	journal.mu.Unlock()
	if closed {
		return ErrRemoteWorkerJournalClosed
	}
	return nil
}

func (journal *RemoteMaterializationJournal) exchange(ctx context.Context, materialize *workerjournalwire.MaterializeRequest) (workerjournalwire.Response, error) {
	return journal.exchangeWithID(ctx, uuid.New(), materialize)
}

func (journal *RemoteMaterializationJournal) exchangeWithID(ctx context.Context, requestID uuid.UUID, materialize *workerjournalwire.MaterializeRequest) (workerjournalwire.Response, error) {
	if err := journal.check(ctx); err != nil {
		return workerjournalwire.Response{}, err
	}
	wire, digest, err := workerjournalwire.EncodeRequestWithID(journal.config.Identity, requestID, nil, materialize)
	if err != nil {
		return workerjournalwire.Response{}, err
	}
	reply, err := runtimechannel.ExchangeWithRequestLimitAndPIDFDBroker(ctx, journal.config.Socket, journal.config.PIDFDBrokerSocket, wire, workerjournalwire.MaximumRequestBytes)
	if err != nil {
		return workerjournalwire.Response{}, ErrRemoteWorkerJournalUncertain
	}
	response, err := workerjournalwire.ParseResponse(reply, digest)
	if err != nil {
		return workerjournalwire.Response{}, err
	}
	return response, remoteJournalResponseError(response)
}

func decodeHexDigest(value string) ([32]byte, error) {
	var digest [32]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(digest) {
		return digest, ErrRemoteWorkerJournalUncertain
	}
	copy(digest[:], decoded)
	return digest, nil
}

func validateRemoteWorkerJournalConfig(config RemoteWorkerJournalConfig) error {
	if config.Socket == "" || config.Identity.JournalID == (workerjournalwire.Identity{}).JournalID || config.Identity.Scope == ([32]byte{}) {
		return ErrRemoteWorkerJournalRejected
	}
	return nil
}

func remoteJournalResponseError(response workerjournalwire.Response) error {
	switch response.Error {
	case "":
		return nil
	case "REJECTED":
		return ErrRemoteWorkerJournalRejected
	case "UNCERTAIN":
		return ErrRemoteWorkerJournalUncertain
	default:
		return workerjournalwire.ErrProtocol
	}
}

func hexDigest(digest [32]byte) string {
	const hex = "0123456789abcdef"
	result := make([]byte, 64)
	for i, value := range digest {
		result[i*2] = hex[value>>4]
		result[i*2+1] = hex[value&0xf]
	}
	return string(result)
}

var _ InputTransferJournal = (*RemoteInputTransferJournal)(nil)
var _ MaterializationJournal = (*RemoteMaterializationJournal)(nil)
