//go:build linux

package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
)

// RuntimeStartupCompositionReceipt is a typed, validation-stage projection of
// one startup operation. Every digest is derived from the exact bytes held by
// the Node ledger or startup request. It records a Permit decision; it does
// not by itself promote a release or assert production readiness.
type RuntimeStartupCompositionReceipt struct {
	SchemaVersion       int               `json:"schema_version"`
	OperationID         uuid.UUID         `json:"operation_id"`
	JournalID           uuid.UUID         `json:"journal_id"`
	RequestDigest       [sha256.Size]byte `json:"request_digest"`
	ReservationDigest   [sha256.Size]byte `json:"reservation_digest"`
	AuthorizationDigest [sha256.Size]byte `json:"authorization_digest"`
	GrantAttemptDigest  [sha256.Size]byte `json:"grant_attempt_digest"`
	Permit              bool              `json:"permit"`
	Outcome             string            `json:"outcome"`
	ReceiptDigest       [sha256.Size]byte `json:"receipt_digest"`
}

// CompositionReceipt snapshots the durable grant fence and the coordinator's
// one-shot decision. Calling it before the backend request is handled returns a
// receipt with Permit=false and Outcome="pending"; no synthetic Permit is
// inferred from reservation or grant existence.
func (orchestration *RuntimeStartupOrchestration) CompositionReceipt(ctx context.Context) (RuntimeStartupCompositionReceipt, error) {
	if orchestration == nil || orchestration.coordinator == nil || ctx == nil {
		return RuntimeStartupCompositionReceipt{}, ErrRuntimeStartupAuthority
	}
	coordinator := orchestration.coordinator
	if coordinator.ledger == nil || coordinator.grant == nil {
		return RuntimeStartupCompositionReceipt{}, ErrRuntimeStartupAuthority
	}
	coordinator.mu.Lock()
	handled := coordinator.handled
	closed := coordinator.closed
	permit := coordinator.observation != nil && !closed
	coordinator.mu.Unlock()
	// Grant attempts are indexed by journal ID, while the grant retains only
	// operation identity. Resolve the index from the immutable ledger map and
	// then use the public inspection method so all state checks remain intact.
	coordinator.ledger.mu.Lock()
	var journalID uuid.UUID
	for candidateJournalID, candidate := range coordinator.ledger.grantAttempts {
		if candidate.OperationID == coordinator.grant.operationID {
			journalID = candidateJournalID
			break
		}
	}
	coordinator.ledger.mu.Unlock()
	requestWire, err := modelruntime.EncodeBackendStartupRequest(coordinator.expected)
	if err != nil {
		return RuntimeStartupCompositionReceipt{}, err
	}
	requestDigest := sha256.Sum256(requestWire)
	if journalID == uuid.Nil {
		// Before the backend's one-shot request arrives the durable grant attempt
		// does not exist yet. Return an explicitly pending receipt; callers must
		// never interpret it as a Permit.
		receipt := RuntimeStartupCompositionReceipt{SchemaVersion: 1, OperationID: coordinator.grant.operationID, JournalID: coordinator.expected.JournalID, RequestDigest: requestDigest, Outcome: "pending"}
		receipt.ReceiptDigest = receiptDigest(receipt)
		return receipt, nil
	}
	attempt, err := coordinator.ledger.InspectJournalGrantAttempt(ctx, journalID)
	if err != nil {
		return RuntimeStartupCompositionReceipt{}, err
	}
	attemptWire, err := json.Marshal(attempt)
	if err != nil {
		return RuntimeStartupCompositionReceipt{}, err
	}
	receipt := RuntimeStartupCompositionReceipt{SchemaVersion: 1, OperationID: attempt.OperationID, JournalID: attempt.JournalID, RequestDigest: requestDigest, ReservationDigest: attempt.ReservationDigest, AuthorizationDigest: attempt.AuthorizationDigest, GrantAttemptDigest: sha256.Sum256(attemptWire), Permit: permit}
	switch {
	case permit:
		receipt.Outcome = "permitted"
	case closed:
		receipt.Outcome = "revoked"
	case handled:
		receipt.Outcome = "denied"
	default:
		receipt.Outcome = "pending"
	}
	receipt.ReceiptDigest = receiptDigest(receipt)
	return receipt, nil
}

func receiptDigest(receipt RuntimeStartupCompositionReceipt) [sha256.Size]byte {
	receipt.ReceiptDigest = [sha256.Size]byte{}
	wire, _ := json.Marshal(receipt)
	return sha256.Sum256(wire)
}

// Verify checks the receipt's self digest and rejects an empty identity or an
// impossible outcome. It is intended for receipt consumers and evidence
// writers, not as an authorization path.
func (receipt RuntimeStartupCompositionReceipt) Verify() error {
	if receipt.SchemaVersion != 1 || receipt.OperationID == uuid.Nil || receipt.JournalID == uuid.Nil || receipt.RequestDigest == ([sha256.Size]byte{}) || receipt.ReceiptDigest != receiptDigest(receipt) {
		return errors.New("runtime startup composition receipt is invalid")
	}
	if receipt.Outcome == "pending" {
		if receipt.Permit || receipt.GrantAttemptDigest != ([sha256.Size]byte{}) || receipt.ReservationDigest != ([sha256.Size]byte{}) || receipt.AuthorizationDigest != ([sha256.Size]byte{}) {
			return errors.New("pending runtime startup composition receipt contains authority")
		}
		return nil
	}
	if receipt.GrantAttemptDigest == ([sha256.Size]byte{}) {
		return errors.New("runtime startup composition receipt has no grant attempt")
	}
	if receipt.ReservationDigest == ([sha256.Size]byte{}) || receipt.AuthorizationDigest == ([sha256.Size]byte{}) {
		return errors.New("runtime startup composition receipt has incomplete authority digests")
	}
	switch receipt.Outcome {
	case "permitted":
		if !receipt.Permit {
			return errors.New("runtime startup composition receipt outcome is inconsistent")
		}
	case "revoked", "denied":
		if receipt.Permit {
			return errors.New("runtime startup composition receipt outcome is inconsistent")
		}
	default:
		return errors.New("runtime startup composition receipt has unknown outcome")
	}
	if receipt.Permit && receipt.Outcome != "permitted" || !receipt.Permit && receipt.Outcome == "permitted" {
		return errors.New("runtime startup composition receipt outcome is inconsistent")
	}
	return nil
}

// VerifyBinding checks that a receipt belongs to the exact operation and
// authority material selected by its consumer. Verify only checks the receipt
// self-digest and structural invariants; this method adds the external binding
// that prevents a valid receipt from one startup being replayed for another.
func (receipt RuntimeStartupCompositionReceipt) VerifyBinding(operationID, journalID uuid.UUID, requestDigest, reservationDigest, authorizationDigest [sha256.Size]byte) error {
	if err := receipt.Verify(); err != nil {
		return err
	}
	if operationID == uuid.Nil || journalID == uuid.Nil || requestDigest == ([sha256.Size]byte{}) {
		return errors.New("runtime startup composition binding is incomplete")
	}
	if receipt.OperationID != operationID || receipt.JournalID != journalID || receipt.RequestDigest != requestDigest {
		return errors.New("runtime startup composition receipt identity does not match operation")
	}
	if receipt.Outcome == "pending" {
		return nil
	}
	if reservationDigest == ([sha256.Size]byte{}) || authorizationDigest == ([sha256.Size]byte{}) || receipt.ReservationDigest != reservationDigest || receipt.AuthorizationDigest != authorizationDigest {
		return errors.New("runtime startup composition receipt authority does not match operation")
	}
	return nil
}
