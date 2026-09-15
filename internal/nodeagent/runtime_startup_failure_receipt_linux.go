//go:build linux

package nodeagent

import (
	"crypto/sha256"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
)

type RuntimeStartupFailurePhase string

const (
	RuntimeStartupFailureResourceLoad  RuntimeStartupFailurePhase = "resource-load"
	RuntimeStartupFailureLauncher      RuntimeStartupFailurePhase = "launcher"
	RuntimeStartupFailureCompose       RuntimeStartupFailurePhase = "compose"
	RuntimeStartupFailureCaller        RuntimeStartupFailurePhase = "caller"
	RuntimeStartupFailureReservation   RuntimeStartupFailurePhase = "reservation"
	RuntimeStartupFailureAuthorization RuntimeStartupFailurePhase = "authorization"
	RuntimeStartupFailureGrant         RuntimeStartupFailurePhase = "grant"
	RuntimeStartupFailureBackend       RuntimeStartupFailurePhase = "backend"
	RuntimeStartupFailureShutdown      RuntimeStartupFailurePhase = "shutdown"
	RuntimeStartupFailureNodeRestart   RuntimeStartupFailurePhase = "node-restart"
	RuntimeStartupFailureBrokerRestart RuntimeStartupFailurePhase = "broker-restart"
	RuntimeStartupFailureCleanup       RuntimeStartupFailurePhase = "cleanup"
)

type RuntimeStartupFailureReceiptInput struct {
	RunID, OperationID, JournalID uuid.UUID
	Phase                         RuntimeStartupFailurePhase
	Cause                         error
	RequestDigest                 [sha256.Size]byte
	ReservationDigest             [sha256.Size]byte
	AuthorizationDigest           [sha256.Size]byte
	GrantAttemptDigest            [sha256.Size]byte
	ReservationCreated            bool
	GrantCreated                  bool
	PermitIssued                  bool
	CleanupVerified               bool
}

// RuntimeStartupFailureReceipt records a startup attempt that stopped before a
// successful composition receipt could be produced. It deliberately permits a
// nil OperationID and JournalID when failure happened before durable
// reservation. A missing object is represented by false, never by a fabricated
// digest or UUID.
type RuntimeStartupFailureReceipt struct {
	SchemaVersion       int               `json:"schema_version"`
	RunID               uuid.UUID         `json:"run_id"`
	OperationID         uuid.UUID         `json:"operation_id,omitempty"`
	JournalID           uuid.UUID         `json:"journal_id,omitempty"`
	Phase               string            `json:"phase"`
	ErrorDigest         [sha256.Size]byte `json:"error_digest"`
	RequestDigest       [sha256.Size]byte `json:"request_digest,omitempty"`
	ReservationDigest   [sha256.Size]byte `json:"reservation_digest,omitempty"`
	AuthorizationDigest [sha256.Size]byte `json:"authorization_digest,omitempty"`
	GrantAttemptDigest  [sha256.Size]byte `json:"grant_attempt_digest,omitempty"`
	ReservationCreated  bool              `json:"reservation_created"`
	GrantCreated        bool              `json:"grant_created"`
	PermitIssued        bool              `json:"permit_issued"`
	CleanupVerified     bool              `json:"cleanup_verified"`
	ReceiptDigest       [sha256.Size]byte `json:"receipt_digest"`
}

// NewRuntimeStartupFailureReceipt binds the error text to the receipt without
// persisting the text itself. Callers should pass the exact stage error and
// independently observed resource booleans.
func NewRuntimeStartupFailureReceipt(input RuntimeStartupFailureReceiptInput) RuntimeStartupFailureReceipt {
	receipt := RuntimeStartupFailureReceipt{
		SchemaVersion:       1,
		RunID:               input.RunID,
		OperationID:         input.OperationID,
		JournalID:           input.JournalID,
		Phase:               string(input.Phase),
		RequestDigest:       input.RequestDigest,
		ReservationDigest:   input.ReservationDigest,
		AuthorizationDigest: input.AuthorizationDigest,
		GrantAttemptDigest:  input.GrantAttemptDigest,
		ReservationCreated:  input.ReservationCreated,
		GrantCreated:        input.GrantCreated,
		PermitIssued:        input.PermitIssued,
		CleanupVerified:     input.CleanupVerified,
	}
	if input.Cause != nil {
		receipt.ErrorDigest = sha256.Sum256([]byte(input.Cause.Error()))
	}
	receipt.ReceiptDigest = failureReceiptDigest(receipt)
	return receipt
}

func failureReceiptDigest(receipt RuntimeStartupFailureReceipt) [sha256.Size]byte {
	receipt.ReceiptDigest = [sha256.Size]byte{}
	wire, _ := marshalFailureReceipt(receipt)
	return sha256.Sum256(wire)
}

// marshalFailureReceipt is kept in this file so digest encoding cannot be
// changed accidentally by callers. JSON is deterministic for this fixed
// struct and UUID/array fields.
func marshalFailureReceipt(receipt RuntimeStartupFailureReceipt) ([]byte, error) {
	return json.Marshal(receipt)
}

// Verify checks structural invariants and the monotonic authority ordering.
// It does not claim that cleanup was actually performed; that fact must come
// from the driver which sets CleanupVerified after inspecting the resources.
func (receipt RuntimeStartupFailureReceipt) Verify() error {
	if receipt.SchemaVersion != 1 || receipt.RunID == uuid.Nil || receipt.Phase == "" || receipt.ErrorDigest == ([sha256.Size]byte{}) || receipt.ReceiptDigest != failureReceiptDigest(receipt) {
		return errors.New("runtime startup failure receipt is invalid")
	}
	switch RuntimeStartupFailurePhase(receipt.Phase) {
	case RuntimeStartupFailureResourceLoad, RuntimeStartupFailureLauncher, RuntimeStartupFailureCompose, RuntimeStartupFailureCaller, RuntimeStartupFailureReservation, RuntimeStartupFailureAuthorization, RuntimeStartupFailureGrant, RuntimeStartupFailureBackend, RuntimeStartupFailureShutdown, RuntimeStartupFailureNodeRestart, RuntimeStartupFailureBrokerRestart, RuntimeStartupFailureCleanup:
	default:
		return errors.New("runtime startup failure receipt has unknown phase")
	}
	if !receipt.ReservationCreated && (receipt.OperationID != uuid.Nil || receipt.JournalID != uuid.Nil || receipt.GrantCreated || receipt.PermitIssued) {
		return errors.New("runtime startup failure receipt contains authority before reservation")
	}
	if receipt.ReservationCreated && (receipt.OperationID == uuid.Nil || receipt.JournalID == uuid.Nil) {
		return errors.New("runtime startup failure receipt reservation identity is incomplete")
	}
	if receipt.ReservationCreated && receipt.RequestDigest == ([sha256.Size]byte{}) {
		return errors.New("runtime startup failure receipt reservation digest is missing")
	}
	if receipt.ReservationCreated && receipt.ReservationDigest == ([sha256.Size]byte{}) {
		return errors.New("runtime startup failure receipt reservation binding is missing")
	}
	if !receipt.ReservationCreated && (receipt.RequestDigest != ([sha256.Size]byte{}) || receipt.ReservationDigest != ([sha256.Size]byte{}) || receipt.AuthorizationDigest != ([sha256.Size]byte{}) || receipt.GrantAttemptDigest != ([sha256.Size]byte{})) {
		return errors.New("runtime startup failure receipt contains digest before reservation")
	}
	if !receipt.GrantCreated && receipt.PermitIssued {
		return errors.New("runtime startup failure receipt contains Permit without grant")
	}
	if receipt.GrantCreated && receipt.GrantAttemptDigest == ([sha256.Size]byte{}) {
		return errors.New("runtime startup failure receipt grant digest is missing")
	}
	if receipt.GrantCreated && receipt.AuthorizationDigest == ([sha256.Size]byte{}) {
		return errors.New("runtime startup failure receipt authorization binding is missing")
	}
	if receipt.Phase == string(RuntimeStartupFailureAuthorization) || receipt.Phase == string(RuntimeStartupFailureGrant) || receipt.Phase == string(RuntimeStartupFailureBackend) || receipt.Phase == string(RuntimeStartupFailureShutdown) || receipt.Phase == string(RuntimeStartupFailureCleanup) || receipt.Phase == string(RuntimeStartupFailureNodeRestart) || receipt.Phase == string(RuntimeStartupFailureBrokerRestart) {
		if !receipt.ReservationCreated {
			return errors.New("runtime startup failure receipt phase has no reservation")
		}
	}
	if receipt.Phase == string(RuntimeStartupFailureResourceLoad) || receipt.Phase == string(RuntimeStartupFailureLauncher) || receipt.Phase == string(RuntimeStartupFailureCompose) || receipt.Phase == string(RuntimeStartupFailureCaller) || receipt.Phase == string(RuntimeStartupFailureReservation) || receipt.Phase == string(RuntimeStartupFailureAuthorization) || receipt.Phase == string(RuntimeStartupFailureGrant) {
		if receipt.GrantCreated || receipt.PermitIssued {
			return errors.New("runtime startup failure receipt phase contains a later authority")
		}
	}
	if receipt.Phase == string(RuntimeStartupFailureResourceLoad) || receipt.Phase == string(RuntimeStartupFailureLauncher) || receipt.Phase == string(RuntimeStartupFailureCompose) || receipt.Phase == string(RuntimeStartupFailureCaller) || receipt.Phase == string(RuntimeStartupFailureReservation) {
		if receipt.AuthorizationDigest != ([sha256.Size]byte{}) || receipt.GrantAttemptDigest != ([sha256.Size]byte{}) {
			return errors.New("runtime startup failure receipt phase contains later authority bindings")
		}
	}
	if receipt.Phase == string(RuntimeStartupFailureResourceLoad) || receipt.Phase == string(RuntimeStartupFailureLauncher) || receipt.Phase == string(RuntimeStartupFailureCompose) || receipt.Phase == string(RuntimeStartupFailureCaller) {
		if receipt.ReservationDigest != ([sha256.Size]byte{}) {
			return errors.New("runtime startup failure receipt phase contains reservation binding")
		}
	}
	return nil
}
