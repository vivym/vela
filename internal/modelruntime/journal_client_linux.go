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
	Page          *JournalPage            `json:"page,omitempty"`
	Error         string                  `json:"error,omitempty"`
}

// ExchangeJournalCommand performs one exchange. It never retries or interprets
// retained metadata as permission to run a backend. Identity must come from
// independently approved journal binding, not from the returned receipt.
func ExchangeJournalCommand(ctx context.Context, socket string, identity ExecutionJournalIdentity, command JournalCommand) (JournalMutationReceipt, error) {
	return exchangeJournalCommandWithPIDFDBroker(ctx, socket, "", identity, command)
}

func exchangeJournalCommandWithPIDFDBroker(ctx context.Context, socket, brokerSocket string, identity ExecutionJournalIdentity, command JournalCommand) (JournalMutationReceipt, error) {
	if identity.JournalID == uuid.Nil || identity.Scope == ([sha256.Size]byte{}) {
		return JournalMutationReceipt{}, ErrJournalCommand
	}
	wire, err := EncodeJournalCommand(command)
	if err != nil {
		return JournalMutationReceipt{}, err
	}
	response, err := runtimechannel.ExchangeWithRequestLimitAndPIDFDBroker(ctx, socket, brokerSocket, wire, MaximumJournalCommandBytes)
	if err != nil {
		return JournalMutationReceipt{}, err
	}
	return parseJournalResponse(response, sha256.Sum256(wire), identity)
}

func parseJournalResponse(wire []byte, request [sha256.Size]byte, identity ExecutionJournalIdentity) (JournalMutationReceipt, error) {
	response, err := decodeJournalEndpointResponse(wire, request)
	if err != nil {
		return JournalMutationReceipt{}, err
	}
	if response.Page != nil {
		return JournalMutationReceipt{}, ErrJournalCommand
	}
	if response.Receipt == nil {
		if response.Error == "UNCERTAIN" {
			return JournalMutationReceipt{}, ErrExecutionStateRecovery
		}
		if response.Error == "REJECTED" {
			return JournalMutationReceipt{}, ErrJournalRejected
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

// UnixRuntimeJournalTransport uses the authenticated Node channel for every
// mutation and read. It retains no local files and performs no mutation retries.
type UnixRuntimeJournalTransport struct {
	Socket            string
	PIDFDBrokerSocket string
	Identity          ExecutionJournalIdentity
}

func (transport UnixRuntimeJournalTransport) Apply(ctx context.Context, command JournalCommand) (JournalMutationReceipt, error) {
	return exchangeJournalCommandWithPIDFDBroker(ctx, transport.Socket, transport.PIDFDBrokerSocket, transport.Identity, command)
}
func (transport UnixRuntimeJournalTransport) Read(ctx context.Context) (JournalDocument, error) {
	return ReadJournalDocument(ctx, transport.Identity, func(ctx context.Context, request JournalReadCommand) (JournalPage, error) {
		return exchangeJournalReadWithPIDFDBroker(ctx, transport.Socket, transport.PIDFDBrokerSocket, request)
	})
}

func decodeJournalEndpointResponse(wire []byte, request [sha256.Size]byte) (JournalEndpointResponse, error) {
	var response JournalEndpointResponse
	if strictjson.RejectDuplicateKeys(wire) != nil {
		return response, ErrJournalCommand
	}
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&response) != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) {
		return JournalEndpointResponse{}, ErrJournalCommand
	}
	canonical, err := json.Marshal(response)
	if err != nil || !bytes.Equal(canonical, wire) || response.SchemaVersion != 1 || response.RequestDigest != request {
		return JournalEndpointResponse{}, ErrJournalCommand
	}
	return response, nil
}

func ExchangeJournalRead(ctx context.Context, socket string, request JournalReadCommand) (JournalPage, error) {
	return exchangeJournalReadWithPIDFDBroker(ctx, socket, "", request)
}

func exchangeJournalReadWithPIDFDBroker(ctx context.Context, socket, brokerSocket string, request JournalReadCommand) (JournalPage, error) {
	wire, err := EncodeJournalCommand(JournalCommand{SchemaVersion: 1, Read: &request})
	if err != nil {
		return JournalPage{}, err
	}
	reply, err := runtimechannel.ExchangeWithRequestLimitAndPIDFDBroker(ctx, socket, brokerSocket, wire, runtimechannel.MaximumPayload)
	if err != nil {
		// No page escaped the authenticated channel. Retrying a later pure
		// read is safe even if this failed exchange could not authenticate;
		// the next exchange must satisfy every channel and snapshot check.
		return JournalPage{}, errors.Join(ErrJournalReadUnavailable, err)
	}
	return parseJournalReadResponse(reply, sha256.Sum256(wire))
}

func parseJournalReadResponse(reply []byte, request [sha256.Size]byte) (JournalPage, error) {
	response, err := decodeJournalEndpointResponse(reply, request)
	if err != nil {
		return JournalPage{}, err
	}
	if response.Receipt != nil {
		return JournalPage{}, ErrJournalCommand
	}
	if response.Page == nil {
		switch response.Error {
		case "CHANGED":
			return JournalPage{}, ErrJournalChanged
		case "UNCERTAIN":
			return JournalPage{}, ErrExecutionStateRecovery
		default:
			return JournalPage{}, ErrJournalCommand
		}
	}
	if response.Error != "" {
		return JournalPage{}, ErrJournalCommand
	}
	return *response.Page, nil
}
