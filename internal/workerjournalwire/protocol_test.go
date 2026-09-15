package workerjournalwire

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/google/uuid"
)

func TestMaterializationChunkRoundTrip(t *testing.T) {
	identity := Identity{JournalID: uuid.New(), Scope: sha256.Sum256([]byte("worker-scope"))}
	requestID := uuid.New()
	record := bytes.Repeat([]byte("x"), 12<<10)
	digest := sha256.Sum256(record)
	wire, _, err := EncodeRequestWithID(identity, requestID, nil, &MaterializeRequest{Operation: "put", ID: "record", Record: record, ChunkOffset: 0, ChunkTotal: len(record), ChunkDigest: hex.EncodeToString(digest[:]), ChunkFinal: true})
	if err != nil {
		t.Fatal(err)
	}
	request, actual, err := ParseRequest(wire)
	if err != nil || actual != identity || request.RequestID != requestID.String() || request.Materialize == nil || !bytes.Equal(request.Materialize.Record, record) {
		t.Fatalf("chunk request round trip: %#v %#v %v", request, actual, err)
	}
}

func TestRequestRoundTripAndDigestBinding(t *testing.T) {
	identity := Identity{JournalID: uuid.New()}
	identity.Scope = sha256.Sum256([]byte("worker-scope"))
	wire, digest, err := EncodeRequest(identity, &InputRequest{Operation: "load", TokenDigest: "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"}, nil)
	if err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	got, actual, err := ParseRequest(wire)
	if err != nil || actual != identity || got.Input == nil || got.Input.Operation != "load" {
		t.Fatalf("ParseRequest = %#v %#v %v", got, actual, err)
	}
	if _, err := EncodeResponse(digest, Response{}); err != nil {
		t.Fatalf("EncodeResponse: %v", err)
	}
	responseWire, err := EncodeResponse(digest, Response{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseResponse(responseWire, sha256.Sum256([]byte("wrong"))); err == nil {
		t.Fatal("response accepted with wrong request digest")
	}
}

func TestRequestRejectsAmbiguousOrUnboundedOperations(t *testing.T) {
	identity := Identity{JournalID: uuid.New(), Scope: sha256.Sum256([]byte("worker-scope"))}
	if _, _, err := EncodeRequest(identity, &InputRequest{Operation: "load", TokenDigest: "00"}, nil); err == nil {
		t.Fatal("short input digest accepted")
	}
	if _, _, err := EncodeRequest(identity, &InputRequest{Operation: "put_pending", TokenDigest: "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"}, nil); err == nil {
		t.Fatal("mutation without record accepted")
	}
	if _, _, err := EncodeRequest(identity, nil, &MaterializeRequest{Operation: "delete", ID: "record"}); err == nil {
		t.Fatal("deletion without proof accepted")
	}
}
