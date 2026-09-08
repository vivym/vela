package modelruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
)

const JournalPageBytes = 16 << 10

var ErrJournalChanged = errors.New("execution journal changed during paged read")

type JournalPage struct {
	JournalID    uuid.UUID         `json:"journal_id"`
	JournalScope [sha256.Size]byte `json:"journal_scope"`
	StateDigest  [sha256.Size]byte `json:"state_digest"`
	TotalBytes   int               `json:"total_bytes"`
	Offset       int               `json:"offset"`
	Document     []byte            `json:"document"`
	LockDocument []byte            `json:"lock_document"`
}

// JournalDocument comes from a separately authenticated owner. It is not
// accepted as a mutation and does not by itself establish launch permission.
type JournalDocument struct {
	Document     []byte
	LockDocument []byte
}

func (owner *ExecutionJournalOwner) Read(ctx context.Context, request JournalReadCommand) (JournalPage, error) {
	if owner == nil || ctx == nil || request.Offset < 0 || request.Offset%JournalPageBytes != 0 ||
		(request.StateDigest == ([sha256.Size]byte{}) && request.Offset != 0) {
		return JournalPage{}, ErrJournalCommand
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err := owner.check(ctx); err != nil {
		return JournalPage{}, err
	}
	if request.StateDigest != ([sha256.Size]byte{}) && request.StateDigest != owner.store.stateDigest {
		return JournalPage{}, ErrJournalChanged
	}
	document, err := json.Marshal(owner.store.state)
	if err != nil || sha256.Sum256(document) != owner.store.stateDigest || request.Offset >= len(document) {
		return JournalPage{}, ErrJournalCommand
	}
	lock, err := owner.store.root.ReadFile(executionStateLockName)
	if err != nil {
		return JournalPage{}, owner.mutationError(err)
	}
	if err := context.Cause(ctx); err != nil {
		return JournalPage{}, err
	}
	return JournalPage{JournalID: owner.store.state.ID, JournalScope: owner.store.state.Scope, StateDigest: owner.store.stateDigest,
		TotalBytes: len(document), Offset: request.Offset, Document: bytes.Clone(document[request.Offset:min(len(document), request.Offset+JournalPageBytes)]),
		LockDocument: lock}, nil
}

// ReadJournalDocument assembles one digest-bound document. It does not restart
// automatically after changes or return a partial/mixed snapshot on failure.
func ReadJournalDocument(ctx context.Context, expected ExecutionJournalIdentity, read func(context.Context, JournalReadCommand) (JournalPage, error)) (JournalDocument, error) {
	if ctx == nil || expected.JournalID == uuid.Nil || expected.Scope == ([sha256.Size]byte{}) || read == nil {
		return JournalDocument{}, ErrJournalCommand
	}
	var result JournalDocument
	var digest [sha256.Size]byte
	total := 0
	for offset := 0; ; offset += JournalPageBytes {
		if err := context.Cause(ctx); err != nil {
			return JournalDocument{}, err
		}
		page, err := read(ctx, JournalReadCommand{StateDigest: digest, Offset: offset})
		if err != nil {
			return JournalDocument{}, err
		}
		if page.JournalID != expected.JournalID || page.JournalScope != expected.Scope || page.Offset != offset ||
			page.TotalBytes <= 0 || page.TotalBytes > maxExecutionStateBytes || page.TotalBytes <= offset ||
			len(page.Document) != min(JournalPageBytes, page.TotalBytes-offset) || page.StateDigest == ([sha256.Size]byte{}) ||
			string(page.LockDocument) != expected.JournalID.String() {
			return JournalDocument{}, ErrJournalCommand
		}
		if offset == 0 {
			digest, total = page.StateDigest, page.TotalBytes
			result.Document = make([]byte, 0, total)
			result.LockDocument = bytes.Clone(page.LockDocument)
		} else if page.StateDigest != digest || page.TotalBytes != total {
			return JournalDocument{}, ErrJournalChanged
		}
		result.Document = append(result.Document, page.Document...)
		if len(result.Document) == total {
			if sha256.Sum256(result.Document) != digest {
				return JournalDocument{}, ErrJournalCommand
			}
			if err := context.Cause(ctx); err != nil {
				return JournalDocument{}, err
			}
			return result, nil
		}
	}
}
