package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/vivym/vela/internal/runtimechannel"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/workerjournalwire"
)

var (
	ErrWorkerJournalIdentity = errors.New("worker journal caller identity is not authorized")
	ErrWorkerJournalClosed   = errors.New("worker journal endpoint is closed")
)

// WorkerJournalEndpoint is the Node-owned authority for the two journals used
// by a Stage Worker. The Worker receives no journal directory and cannot
// select storage; every operation is checked against the retained original
// Worker pidfd and the immutable journal identity.
type WorkerJournalEndpoint struct {
	mu              sync.Mutex
	input           *stageworkeragent.FileInputTransferJournal
	materialization *stageworkeragent.FileMaterializationJournal
	worker          *os.File
	identity        workerjournalwire.Identity
	pidfdBroker     string
	writable        bool
	uploads         map[string]*materializationUpload
}

const workerJournalChunkBytes = 12 << 10
const workerJournalMaximumUploads = 8

type materializationUpload struct {
	operation string
	id        string
	total     int
	digest    [sha256.Size]byte
	next      int
	data      []byte
	deadline  time.Time
}

type WorkerJournalEndpointConfig struct {
	Input             *stageworkeragent.FileInputTransferJournal
	Materialization   *stageworkeragent.FileMaterializationJournal
	WorkerOwner       *RuntimeNamespaceOwner
	Identity          workerjournalwire.Identity
	PIDFDBrokerSocket string
}

func NewWorkerJournalEndpoint(ctx context.Context, config WorkerJournalEndpointConfig) (*WorkerJournalEndpoint, error) {
	if ctx == nil || os.Geteuid() != 0 || config.Input == nil || config.Materialization == nil || config.WorkerOwner == nil ||
		config.Identity.JournalID == (workerjournalwire.Identity{}).JournalID || config.Identity.Scope == ([sha256.Size]byte{}) {
		return nil, ErrWorkerJournalIdentity
	}
	worker, err := retainJournalProcess(ctx, config.WorkerOwner)
	if err != nil {
		return nil, err
	}
	return &WorkerJournalEndpoint{input: config.Input, materialization: config.Materialization, worker: worker,
		identity: config.Identity, pidfdBroker: config.PIDFDBrokerSocket, uploads: make(map[string]*materializationUpload)}, nil
}

func (endpoint *WorkerJournalEndpoint) Handle(ctx context.Context, caller *RuntimeCaller) ([]byte, error) {
	if endpoint == nil || caller == nil || ctx == nil {
		return nil, ErrWorkerJournalIdentity
	}
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if endpoint.worker == nil {
		return nil, ErrWorkerJournalClosed
	}
	if _, err := caller.Inspect(ctx); err != nil {
		return nil, err
	}
	if err := endpoint.sameWorker(ctx, caller); err != nil {
		return nil, err
	}
	payload := caller.Payload()
	request, identity, err := workerjournalwire.ParseRequest(payload)
	if err != nil || identity != endpoint.identity {
		return nil, ErrWorkerJournalIdentity
	}
	requestDigest := sha256.Sum256(payload)
	response := workerjournalwire.Response{}
	if !endpoint.writable && workerJournalRequestMutates(request) {
		return workerjournalwire.EncodeResponse(requestDigest, workerjournalwire.Response{Error: "REJECTED"})
	}
	switch {
	case request.Input != nil:
		response, err = endpoint.handleInput(ctx, request.Input)
	case request.Materialize != nil:
		response, err = endpoint.handleMaterialization(ctx, request.RequestID, request.Materialize)
	default:
		err = workerjournalwire.ErrProtocol
	}
	if err != nil {
		response = workerjournalwire.Response{Error: classifyWorkerJournalError(ctx, err)}
	}
	return workerjournalwire.EncodeResponse(requestDigest, response)
}

// Activate opens mutation authority only after the independent Runtime startup
// grant has been consumed. Reads remain available during the pre-grant
// startup snapshot phase.
func (endpoint *WorkerJournalEndpoint) Activate(ctx context.Context) error {
	if endpoint == nil || ctx == nil {
		return ErrWorkerJournalIdentity
	}
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if endpoint.worker == nil {
		return ErrWorkerJournalClosed
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := runtimechannel.PollLivePIDFD(int(endpoint.worker.Fd())); err != nil {
		return err
	}
	endpoint.writable = true
	return nil
}

func (endpoint *WorkerJournalEndpoint) handleInput(ctx context.Context, request *workerjournalwire.InputRequest) (workerjournalwire.Response, error) {
	digest, err := decodeWorkerDigest(request.TokenDigest)
	if err != nil {
		return workerjournalwire.Response{}, err
	}
	switch request.Operation {
	case "load":
		record, found, err := endpoint.input.Load(ctx, digest)
		if err != nil || !found {
			return workerjournalwire.Response{Found: found}, err
		}
		encoded, err := stageworkeragent.EncodeInputTransferJournalRecord(record)
		return workerjournalwire.Response{Found: true, InputRecord: encoded}, err
	case "put_pending", "mark_consumed":
		record, err := stageworkeragent.DecodeInputTransferJournalRecord(request.Record)
		if err != nil || record.TokenDigest != digest {
			return workerjournalwire.Response{}, err
		}
		if request.Operation == "put_pending" {
			err = endpoint.input.PutPending(ctx, record)
		} else {
			err = endpoint.input.MarkConsumed(ctx, record)
		}
		return workerjournalwire.Response{}, err
	default:
		return workerjournalwire.Response{}, workerjournalwire.ErrProtocol
	}
}

func (endpoint *WorkerJournalEndpoint) handleMaterialization(ctx context.Context, requestID string, request *workerjournalwire.MaterializeRequest) (workerjournalwire.Response, error) {
	switch request.Operation {
	case "ensure_capacity":
		return workerjournalwire.Response{}, endpoint.materialization.EnsureCapacity(ctx)
	case "put":
		recordWire, final, err := endpoint.acceptMaterializationChunk(requestID, request)
		if err != nil || !final {
			return workerjournalwire.Response{}, err
		}
		record, err := stageworkeragent.DecodePendingMaterialization(recordWire)
		if err != nil {
			return workerjournalwire.Response{}, err
		}
		if current, foundErr := endpoint.currentMaterialization(ctx, record.ID); foundErr != nil {
			return workerjournalwire.Response{}, foundErr
		} else if current != nil {
			if err := stageworkeragent.ValidateMaterializationTransition(*current, record); err != nil {
				return workerjournalwire.Response{}, err
			}
		}
		return workerjournalwire.Response{}, endpoint.materialization.Put(ctx, record)
	case "list":
		records, err := endpoint.materialization.List(ctx)
		if err != nil {
			return workerjournalwire.Response{}, err
		}
		encoded := make([][]byte, 0, len(records))
		digestHash := sha256.New()
		for _, record := range records {
			wire, encodeErr := stageworkeragent.EncodePendingMaterialization(record)
			if encodeErr != nil {
				return workerjournalwire.Response{}, encodeErr
			}
			var length [8]byte
			binary.BigEndian.PutUint64(length[:], uint64(len(wire)))
			_, _ = digestHash.Write(length[:])
			_, _ = digestHash.Write(wire)
			encoded = append(encoded, wire)
		}
		pageSize := request.PageSize
		if pageSize == 0 {
			pageSize = 1
		}
		if request.Offset > len(encoded) {
			return workerjournalwire.Response{}, errors.New("worker materialization list offset is invalid")
		}
		end := request.Offset + pageSize
		if end > len(encoded) {
			end = len(encoded)
		}
		page := encoded[request.Offset:end]
		listDigest := hex.EncodeToString(digestHash.Sum(nil))
		if len(page) == 1 && len(page[0]) > workerJournalChunkBytes {
			chunkOffset := request.ChunkOffset
			if chunkOffset > len(page[0]) {
				return workerjournalwire.Response{}, errors.New("worker materialization record chunk offset is invalid")
			}
			chunkEnd := chunkOffset + workerJournalChunkBytes
			if chunkEnd > len(page[0]) {
				chunkEnd = len(page[0])
			}
			recordDigest := sha256.Sum256(page[0])
			return workerjournalwire.Response{MaterializationNext: request.Offset, MaterializationDigest: listDigest, MaterializationMore: end < len(encoded), MaterializationRecordChunk: page[0][chunkOffset:chunkEnd], MaterializationRecordOffset: chunkOffset, MaterializationRecordTotal: len(page[0]), MaterializationRecordDigest: hex.EncodeToString(recordDigest[:]), MaterializationRecordMore: chunkEnd < len(page[0])}, nil
		}
		return workerjournalwire.Response{MaterializationList: page, MaterializationNext: end, MaterializationDigest: listDigest, MaterializationMore: end < len(encoded)}, nil
	case "delete":
		recordWire, final, err := endpoint.acceptMaterializationChunk(requestID, request)
		if err != nil || !final {
			return workerjournalwire.Response{}, err
		}
		record, err := stageworkeragent.DecodePendingMaterialization(recordWire)
		if err != nil || record.ID != request.ID || record.ConfirmedDisposition == "" {
			return workerjournalwire.Response{}, errors.New("worker materialization deletion proof is invalid")
		}
		current, currentErr := endpoint.currentMaterialization(ctx, request.ID)
		if currentErr != nil || current == nil {
			return workerjournalwire.Response{}, errors.New("worker materialization deletion proof is stale")
		}
		currentWire, encodeErr := stageworkeragent.EncodePendingMaterialization(*current)
		if encodeErr != nil || !bytes.Equal(currentWire, recordWire) {
			return workerjournalwire.Response{}, errors.New("worker materialization deletion proof is stale")
		}
		return workerjournalwire.Response{}, endpoint.materialization.Delete(ctx, request.ID)
	default:
		return workerjournalwire.Response{}, workerjournalwire.ErrProtocol
	}
}

func (endpoint *WorkerJournalEndpoint) acceptMaterializationChunk(requestID string, request *workerjournalwire.MaterializeRequest) ([]byte, bool, error) {
	if requestID == "" || request == nil || len(request.Record) == 0 {
		return nil, false, workerjournalwire.ErrProtocol
	}
	digest, err := decodeWorkerDigest(request.ChunkDigest)
	if err != nil {
		return nil, false, err
	}
	now := time.Now()
	for id, pending := range endpoint.uploads {
		if pending == nil || now.After(pending.deadline) {
			delete(endpoint.uploads, id)
		}
	}
	upload := endpoint.uploads[requestID]
	if upload == nil {
		if request.ChunkOffset != 0 {
			return nil, false, errors.New("worker materialization chunk sequence is invalid")
		}
		if len(endpoint.uploads) >= workerJournalMaximumUploads {
			return nil, false, errors.New("worker materialization chunk uploads are full")
		}
		upload = &materializationUpload{operation: request.Operation, id: request.ID, total: request.ChunkTotal, digest: digest, deadline: now.Add(2 * time.Minute)}
		endpoint.uploads[requestID] = upload
	}
	if time.Now().After(upload.deadline) || upload.operation != request.Operation || upload.id != request.ID || upload.total != request.ChunkTotal || upload.digest != digest || upload.next != request.ChunkOffset {
		delete(endpoint.uploads, requestID)
		return nil, false, errors.New("worker materialization chunk sequence is invalid")
	}
	if upload.next+len(request.Record) > upload.total || upload.total > 4<<20 {
		delete(endpoint.uploads, requestID)
		return nil, false, errors.New("worker materialization chunk size is invalid")
	}
	upload.data = append(upload.data, request.Record...)
	upload.next += len(request.Record)
	if !request.ChunkFinal {
		return nil, false, nil
	}
	if upload.next != upload.total || sha256.Sum256(upload.data) != upload.digest {
		delete(endpoint.uploads, requestID)
		return nil, false, errors.New("worker materialization chunk digest is invalid")
	}
	data := append([]byte(nil), upload.data...)
	delete(endpoint.uploads, requestID)
	return data, true, nil
}

func (endpoint *WorkerJournalEndpoint) currentMaterialization(ctx context.Context, id string) (*stageworkeragent.PendingMaterialization, error) {
	records, err := endpoint.materialization.List(ctx)
	if err != nil {
		return nil, err
	}
	for index := range records {
		if records[index].ID == id {
			return &records[index], nil
		}
	}
	return nil, nil
}

func (endpoint *WorkerJournalEndpoint) sameWorker(ctx context.Context, caller *RuntimeCaller) error {
	if endpoint == nil {
		return ErrWorkerJournalClosed
	}
	caller.mu.Lock()
	defer caller.mu.Unlock()
	if caller.pidfd == nil || endpoint.worker == nil {
		return ErrWorkerJournalClosed
	}
	return endpoint.sameWorkerLocked(ctx, caller.pidfd)
}

func (endpoint *WorkerJournalEndpoint) sameWorkerLocked(ctx context.Context, callerPIDFD *os.File) error {
	if callerPIDFD == nil || endpoint.worker == nil {
		return ErrWorkerJournalClosed
	}
	err := runtimechannel.SameLiveProcess(int(endpoint.worker.Fd()), int(callerPIDFD.Fd()))
	if errors.Is(err, runtimechannel.ErrPIDFDIdentityUnavailable) && endpoint.pidfdBroker != "" {
		return runtimechannel.ComparePIDFDsWithBroker(ctx, endpoint.pidfdBroker, int(endpoint.worker.Fd()), int(callerPIDFD.Fd()))
	}
	return err
}

func workerJournalRequestMutates(request workerjournalwire.Request) bool {
	return request.Input != nil && request.Input.Operation != "load" || request.Materialize != nil && request.Materialize.Operation != "list"
}

func (endpoint *WorkerJournalEndpoint) Close() error {
	if endpoint == nil {
		return nil
	}
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if endpoint.worker == nil {
		return nil
	}
	err := endpoint.worker.Close()
	endpoint.worker = nil
	endpoint.writable = false
	endpoint.uploads = nil
	return err
}

func classifyWorkerJournalError(ctx context.Context, err error) string {
	if err == nil {
		return ""
	}
	if context.Cause(ctx) != nil {
		return "UNCERTAIN"
	}
	return "REJECTED"
}

func decodeWorkerDigest(value string) ([sha256.Size]byte, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return [sha256.Size]byte{}, errors.New("invalid Worker journal digest")
	}
	var digest [sha256.Size]byte
	copy(digest[:], decoded)
	if digest == ([sha256.Size]byte{}) {
		return [sha256.Size]byte{}, errors.New("invalid Worker journal digest")
	}
	return digest, nil
}
