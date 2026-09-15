//go:build linux

package nodeagent

import (
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestRuntimeStartupFailureReceiptAllowsPreReservationFailure(t *testing.T) {
	receipt := NewRuntimeStartupFailureReceipt(RuntimeStartupFailureReceiptInput{RunID: uuid.New(), Phase: RuntimeStartupFailureLauncher, Cause: errors.New("helper exited before handoff"), CleanupVerified: true})
	if err := receipt.Verify(); err != nil {
		t.Fatalf("pre-reservation failure receipt rejected: %v", err)
	}
}

func TestRuntimeStartupFailureReceiptRequiresMonotonicAuthority(t *testing.T) {
	runID, operationID, journalID := uuid.New(), uuid.New(), uuid.New()
	valid := NewRuntimeStartupFailureReceipt(RuntimeStartupFailureReceiptInput{RunID: runID, OperationID: operationID, JournalID: journalID, Phase: RuntimeStartupFailureBackend, Cause: errors.New("Permit response lost"), RequestDigest: sha256.Sum256([]byte("request")), ReservationDigest: sha256.Sum256([]byte("reservation")), AuthorizationDigest: sha256.Sum256([]byte("authorization")), GrantAttemptDigest: sha256.Sum256([]byte("grant")), ReservationCreated: true, GrantCreated: true, CleanupVerified: true})
	if err := valid.Verify(); err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.PermitIssued = true
	invalid.ReceiptDigest = failureReceiptDigest(invalid)
	if err := invalid.Verify(); err != nil {
		t.Fatalf("Permit-bearing failed receipt should remain structurally valid: %v", err)
	}
	invalid = valid
	invalid.GrantCreated = false
	invalid.PermitIssued = true
	invalid.ReceiptDigest = failureReceiptDigest(invalid)
	if err := invalid.Verify(); err == nil {
		t.Fatal("Permit without grant was accepted")
	}
	invalid = valid
	invalid.Phase = "arbitrary"
	invalid.ReceiptDigest = failureReceiptDigest(invalid)
	if err := invalid.Verify(); err == nil {
		t.Fatal("unknown failure phase was accepted")
	}
	invalid = valid
	invalid.ReservationDigest = [sha256.Size]byte{}
	invalid.ReceiptDigest = failureReceiptDigest(invalid)
	if err := invalid.Verify(); err == nil {
		t.Fatal("grant receipt without reservation binding was accepted")
	}
	invalid = valid
	invalid.AuthorizationDigest = [sha256.Size]byte{}
	invalid.ReceiptDigest = failureReceiptDigest(invalid)
	if err := invalid.Verify(); err == nil {
		t.Fatal("grant receipt without authorization binding was accepted")
	}
	invalid = valid
	invalid.Phase = string(RuntimeStartupFailureCompose)
	invalid.ReceiptDigest = failureReceiptDigest(invalid)
	if err := invalid.Verify(); err == nil {
		t.Fatal("compose receipt with a later grant was accepted")
	}
}

func TestRuntimeStartupFailureReceiptRejectsFabricatedAuthorityBeforeReservation(t *testing.T) {
	receipt := NewRuntimeStartupFailureReceipt(RuntimeStartupFailureReceiptInput{RunID: uuid.New(), OperationID: uuid.New(), Phase: RuntimeStartupFailureCaller, Cause: errors.New("caller rejected"), CleanupVerified: true})
	if err := receipt.Verify(); err == nil {
		t.Fatal("pre-reservation receipt with fabricated operation was accepted")
	}
}
