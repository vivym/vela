// Package workerjournal defines the small, typed wire protocol used by a
// Stage Worker to access journals owned by the Node.  The protocol carries
// domain operations only; it never accepts a path, a replacement snapshot or
// a writer identity from the caller.
package workerjournalwire

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/strictjson"
)

const (
	SchemaVersion        = 1
	MaximumRequestBytes  = 192 << 10
	MaximumResponseBytes = 32 << 10
)

var ErrProtocol = errors.New("invalid typed Worker journal protocol message")

type Identity struct {
	JournalID uuid.UUID
	Scope     [sha256.Size]byte
}

type Request struct {
	SchemaVersion int                 `json:"schema_version"`
	RequestID     string              `json:"request_id"`
	JournalID     string              `json:"journal_id"`
	Scope         string              `json:"scope"`
	Input         *InputRequest       `json:"input,omitempty"`
	Materialize   *MaterializeRequest `json:"materialize,omitempty"`
}

type InputRequest struct {
	Operation   string `json:"operation"`
	TokenDigest string `json:"token_digest"`
	Record      []byte `json:"record,omitempty"`
}

type MaterializeRequest struct {
	Operation   string `json:"operation"`
	ID          string `json:"id,omitempty"`
	Record      []byte `json:"record,omitempty"`
	Offset      int    `json:"offset,omitempty"`
	PageSize    int    `json:"page_size,omitempty"`
	ChunkOffset int    `json:"chunk_offset,omitempty"`
	ChunkTotal  int    `json:"chunk_total,omitempty"`
	ChunkDigest string `json:"chunk_digest,omitempty"`
	ChunkFinal  bool   `json:"chunk_final,omitempty"`
}

type Response struct {
	SchemaVersion               int      `json:"schema_version"`
	RequestDigest               string   `json:"request_digest"`
	Found                       bool     `json:"found,omitempty"`
	InputRecord                 []byte   `json:"input_record,omitempty"`
	MaterializationList         [][]byte `json:"materialization_list,omitempty"`
	MaterializationNext         int      `json:"materialization_next,omitempty"`
	MaterializationDigest       string   `json:"materialization_digest,omitempty"`
	MaterializationMore         bool     `json:"materialization_more,omitempty"`
	MaterializationRecordChunk  []byte   `json:"materialization_record_chunk,omitempty"`
	MaterializationRecordOffset int      `json:"materialization_record_offset,omitempty"`
	MaterializationRecordTotal  int      `json:"materialization_record_total,omitempty"`
	MaterializationRecordDigest string   `json:"materialization_record_digest,omitempty"`
	MaterializationRecordMore   bool     `json:"materialization_record_more,omitempty"`
	Error                       string   `json:"error,omitempty"`
}

func EncodeRequest(identity Identity, input *InputRequest, materialize *MaterializeRequest) ([]byte, [sha256.Size]byte, error) {
	return EncodeRequestWithID(identity, uuid.New(), input, materialize)
}

func EncodeRequestWithID(identity Identity, requestID uuid.UUID, input *InputRequest, materialize *MaterializeRequest) ([]byte, [sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if requestID == uuid.Nil {
		return nil, zero, ErrProtocol
	}
	request := Request{SchemaVersion: SchemaVersion, RequestID: requestID.String(), JournalID: identity.JournalID.String(), Scope: hex.EncodeToString(identity.Scope[:]), Input: input, Materialize: materialize}
	if err := validateRequest(request); err != nil {
		return nil, zero, err
	}
	wire, err := json.Marshal(request)
	if err != nil || len(wire) > MaximumRequestBytes {
		return nil, zero, ErrProtocol
	}
	return wire, sha256.Sum256(wire), nil
}

func ParseRequest(wire []byte) (Request, Identity, error) {
	var request Request
	if len(wire) == 0 || len(wire) > MaximumRequestBytes || strictjson.RejectDuplicateKeys(wire) != nil {
		return Request{}, Identity{}, ErrProtocol
	}
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) || validateRequest(request) != nil {
		return Request{}, Identity{}, ErrProtocol
	}
	canonical, err := json.Marshal(request)
	if err != nil || !bytes.Equal(canonical, wire) {
		return Request{}, Identity{}, ErrProtocol
	}
	identity, err := parseIdentity(request.JournalID, request.Scope)
	if err != nil {
		return Request{}, Identity{}, err
	}
	return request, identity, nil
}

func EncodeResponse(requestDigest [sha256.Size]byte, response Response) ([]byte, error) {
	response.SchemaVersion = SchemaVersion
	response.RequestDigest = hex.EncodeToString(requestDigest[:])
	if response.Error != "" && (response.Found || len(response.InputRecord) != 0 || len(response.MaterializationList) != 0 || response.MaterializationNext != 0 || response.MaterializationDigest != "" || response.MaterializationMore || len(response.MaterializationRecordChunk) != 0) {
		return nil, ErrProtocol
	}
	wire, err := json.Marshal(response)
	if err != nil || len(wire) > MaximumResponseBytes {
		return nil, ErrProtocol
	}
	return wire, nil
}

func ParseResponse(wire []byte, requestDigest [sha256.Size]byte) (Response, error) {
	var response Response
	if len(wire) == 0 || len(wire) > MaximumResponseBytes || strictjson.RejectDuplicateKeys(wire) != nil {
		return Response{}, ErrProtocol
	}
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&response) != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) || response.SchemaVersion != SchemaVersion {
		return Response{}, ErrProtocol
	}
	digest, err := hex.DecodeString(response.RequestDigest)
	if err != nil || len(digest) != sha256.Size || !bytes.Equal(digest, requestDigest[:]) {
		return Response{}, ErrProtocol
	}
	if response.MaterializationDigest != "" {
		if _, err := decodeDigest(response.MaterializationDigest); err != nil {
			return Response{}, err
		}
	}
	if response.MaterializationRecordDigest != "" {
		if _, err := decodeDigest(response.MaterializationRecordDigest); err != nil {
			return Response{}, err
		}
	}
	canonical, err := json.Marshal(response)
	if err != nil || !bytes.Equal(canonical, wire) {
		return Response{}, ErrProtocol
	}
	return response, nil
}

func parseIdentity(journalID, scope string) (Identity, error) {
	var identity Identity
	id, err := uuid.Parse(journalID)
	if err != nil || id == uuid.Nil {
		return identity, ErrProtocol
	}
	decoded, err := hex.DecodeString(scope)
	if err != nil || len(decoded) != sha256.Size {
		return identity, ErrProtocol
	}
	copy(identity.Scope[:], decoded)
	if identity.Scope == ([sha256.Size]byte{}) {
		return Identity{}, ErrProtocol
	}
	identity.JournalID = id
	return identity, nil
}

func validateRequest(request Request) error {
	if request.SchemaVersion != SchemaVersion || request.RequestID == "" {
		return ErrProtocol
	}
	if _, err := uuid.Parse(request.RequestID); err != nil {
		return ErrProtocol
	}
	if _, err := parseIdentity(request.JournalID, request.Scope); err != nil {
		return err
	}
	if (request.Input == nil) == (request.Materialize == nil) {
		return ErrProtocol
	}
	if request.Input != nil {
		if request.Input.Operation != "load" && request.Input.Operation != "put_pending" && request.Input.Operation != "mark_consumed" {
			return ErrProtocol
		}
		if _, err := decodeDigest(request.Input.TokenDigest); err != nil {
			return err
		}
		if request.Input.Operation == "load" && len(request.Input.Record) != 0 || request.Input.Operation != "load" && len(request.Input.Record) == 0 {
			return ErrProtocol
		}
	}
	if request.Materialize != nil {
		switch request.Materialize.Operation {
		case "ensure_capacity", "list":
			if request.Materialize.ID != "" || len(request.Materialize.Record) != 0 || request.Materialize.Offset < 0 || request.Materialize.PageSize < 0 || request.Materialize.PageSize > 128 || request.Materialize.ChunkOffset < 0 || request.Materialize.ChunkTotal != 0 || request.Materialize.ChunkDigest != "" || request.Materialize.ChunkFinal {
				return ErrProtocol
			}
			if request.Materialize.Operation == "ensure_capacity" && request.Materialize.ChunkOffset != 0 {
				return ErrProtocol
			}
		case "put":
			if request.Materialize.ID == "" || len(request.Materialize.Record) == 0 || request.Materialize.ChunkOffset < 0 || request.Materialize.ChunkTotal <= 0 || request.Materialize.ChunkTotal > 4<<20 || request.Materialize.ChunkOffset+len(request.Materialize.Record) > request.Materialize.ChunkTotal || request.Materialize.ChunkDigest == "" || (!request.Materialize.ChunkFinal && request.Materialize.ChunkOffset+len(request.Materialize.Record) >= request.Materialize.ChunkTotal) || (request.Materialize.ChunkFinal && request.Materialize.ChunkOffset+len(request.Materialize.Record) != request.Materialize.ChunkTotal) {
				return ErrProtocol
			}
			if _, err := decodeDigest(request.Materialize.ChunkDigest); err != nil {
				return err
			}
		case "delete":
			if request.Materialize.ID == "" || len(request.Materialize.Record) == 0 || request.Materialize.ChunkOffset < 0 || request.Materialize.ChunkTotal <= 0 || request.Materialize.ChunkTotal > 4<<20 || request.Materialize.ChunkOffset+len(request.Materialize.Record) > request.Materialize.ChunkTotal || request.Materialize.ChunkDigest == "" || (!request.Materialize.ChunkFinal && request.Materialize.ChunkOffset+len(request.Materialize.Record) >= request.Materialize.ChunkTotal) || (request.Materialize.ChunkFinal && request.Materialize.ChunkOffset+len(request.Materialize.Record) != request.Materialize.ChunkTotal) {
				return ErrProtocol
			}
			if _, err := decodeDigest(request.Materialize.ChunkDigest); err != nil {
				return err
			}
		default:
			return ErrProtocol
		}
	}
	return nil
}

func decodeDigest(value string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return digest, ErrProtocol
	}
	copy(digest[:], decoded)
	if digest == ([sha256.Size]byte{}) {
		return [sha256.Size]byte{}, ErrProtocol
	}
	return digest, nil
}
