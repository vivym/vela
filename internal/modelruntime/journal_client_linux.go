package modelruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/runtimechannel"
	"github.com/vivym/vela/internal/strictjson"
)

type JournalEndpointResponse struct {
	SchemaVersion int                     `json:"schema_version"`
	RequestDigest [sha256.Size]byte       `json:"request_digest"`
	Receipt       *JournalMutationReceipt `json:"receipt,omitempty"`
	Error         string                  `json:"error,omitempty"`
}

// ExchangeJournalCommand performs one exchange. It never retries or interprets
// retained metadata as permission to run a backend. Identity must come from
// independently approved journal binding, not from the returned receipt.
func ExchangeJournalCommand(ctx context.Context, socket string, identity ExecutionJournalIdentity, command JournalCommand) (JournalMutationReceipt, error) {
	if identity.JournalID == uuid.Nil || identity.Scope == ([sha256.Size]byte{}) {
		return JournalMutationReceipt{}, ErrJournalCommand
	}
	wire, err := EncodeJournalCommand(command)
	if err != nil {
		return JournalMutationReceipt{}, err
	}
	response, err := runtimechannel.ExchangeWithRequestLimit(ctx, socket, wire, MaximumJournalCommandBytes)
	if err != nil {
		return JournalMutationReceipt{}, err
	}
	return parseJournalResponse(response, sha256.Sum256(wire), identity)
}

func parseJournalResponse(wire []byte, request [sha256.Size]byte, identity ExecutionJournalIdentity) (JournalMutationReceipt, error) {
	var response JournalEndpointResponse
	if strictjson.RejectDuplicateKeys(wire) != nil {
		return JournalMutationReceipt{}, ErrJournalCommand
	}
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&response) != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) {
		return JournalMutationReceipt{}, ErrJournalCommand
	}
	canonical, err := json.Marshal(response)
	if err != nil || !bytes.Equal(canonical, wire) || response.SchemaVersion != 1 || response.RequestDigest != request {
		return JournalMutationReceipt{}, ErrJournalCommand
	}
	if response.Receipt == nil {
		if response.Error == "UNCERTAIN" {
			return JournalMutationReceipt{}, ErrExecutionStateRecovery
		}
		return JournalMutationReceipt{}, ErrJournalCommand
	}
	receipt := response.Receipt
	if response.Error != "" || receipt.SchemaVersion != 1 || receipt.RequestDigest != request ||
		receipt.JournalID != identity.JournalID || receipt.JournalScope != identity.Scope ||
		receipt.Highest < 0 || receipt.Floor < 0 || receipt.StateDigest == ([sha256.Size]byte{}) {
		return JournalMutationReceipt{}, ErrJournalCommand
	}
	return *receipt, nil
}
