//go:build linux

package nodeagent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/workerjournalwire"
)

func TestWorkerJournalMaterializationChunkAssembly(t *testing.T) {
	endpoint := &WorkerJournalEndpoint{uploads: make(map[string]*materializationUpload)}
	payload := bytes.Repeat([]byte("materialization-record"), 3000)
	digest := sha256.Sum256(payload)
	requestID := uuid.NewString()
	for offset := 0; offset < len(payload); {
		end := offset + workerJournalChunkBytes
		if end > len(payload) {
			end = len(payload)
		}
		request := &workerjournalwire.MaterializeRequest{Operation: "put", ID: "record-1", Record: payload[offset:end], ChunkOffset: offset, ChunkTotal: len(payload), ChunkDigest: hex.EncodeToString(digest[:]), ChunkFinal: end == len(payload)}
		assembled, final, err := endpoint.acceptMaterializationChunk(requestID, request)
		if err != nil {
			t.Fatalf("chunk %d: %v", offset, err)
		}
		if end != len(payload) {
			if final || assembled != nil {
				t.Fatalf("non-final chunk returned final=%v bytes=%d", final, len(assembled))
			}
		} else if !final || !bytes.Equal(assembled, payload) {
			t.Fatalf("final assembly mismatch: final=%v bytes=%d", final, len(assembled))
		}
		offset = end
	}
	if len(endpoint.uploads) != 0 {
		t.Fatal("completed upload was retained")
	}
	if _, _, err := endpoint.acceptMaterializationChunk(requestID, &workerjournalwire.MaterializeRequest{Operation: "put", ID: "record-1", Record: []byte("late"), ChunkOffset: 0, ChunkTotal: 4, ChunkDigest: hex.EncodeToString(digest[:]), ChunkFinal: true}); err == nil {
		t.Fatal("reused request ID accepted after completion")
	}
}

func TestWorkerJournalMaterializationChunkRejectsDigestAndSequence(t *testing.T) {
	endpoint := &WorkerJournalEndpoint{uploads: make(map[string]*materializationUpload)}
	good := sha256.Sum256([]byte("abcdef"))
	request := func(offset int, data string, final bool) *workerjournalwire.MaterializeRequest {
		return &workerjournalwire.MaterializeRequest{Operation: "delete", ID: "record-1", Record: []byte(data), ChunkOffset: offset, ChunkTotal: 6, ChunkDigest: hex.EncodeToString(good[:]), ChunkFinal: final}
	}
	if _, _, err := endpoint.acceptMaterializationChunk("sequence", request(1, "a", false)); err == nil {
		t.Fatal("non-zero first offset accepted")
	}
	wrong := sha256.Sum256([]byte("wrong"))
	if _, _, err := endpoint.acceptMaterializationChunk("digest", &workerjournalwire.MaterializeRequest{Operation: "put", ID: "record-1", Record: []byte("abcdef"), ChunkOffset: 0, ChunkTotal: 6, ChunkDigest: hex.EncodeToString(wrong[:]), ChunkFinal: true}); err == nil {
		t.Fatal("wrong digest accepted")
	}
	if _, _, err := endpoint.acceptMaterializationChunk("order", request(0, "abc", false)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := endpoint.acceptMaterializationChunk("order", request(4, "ef", true)); err == nil {
		t.Fatal("offset gap accepted")
	}
}
