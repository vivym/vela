//go:build linux

package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/nodeagent"
)

func TestWriteRuntimeStartupCompositionReceiptIsExclusiveAndVerifiable(t *testing.T) {
	directory := t.TempDir()
	receipt := nodeagent.RuntimeStartupCompositionReceipt{SchemaVersion: 1, OperationID: uuid.New(), JournalID: uuid.New(), RequestDigest: sha256.Sum256([]byte("request")), ReservationDigest: sha256.Sum256([]byte("reservation")), AuthorizationDigest: sha256.Sum256([]byte("authorization")), GrantAttemptDigest: sha256.Sum256([]byte("grant")), Permit: true, Outcome: "permitted"}
	unsigned := receipt
	unsigned.ReceiptDigest = [sha256.Size]byte{}
	wire, err := json.Marshal(unsigned)
	if err != nil {
		t.Fatal(err)
	}
	receipt.ReceiptDigest = sha256.Sum256(wire)
	if err := writeRuntimeStartupCompositionReceipt(directory, receipt); err != nil {
		t.Fatal(err)
	}
	if err := writeRuntimeStartupCompositionReceipt(directory, receipt); err == nil {
		t.Fatal("composition receipt path was overwritten")
	}
}

func TestWriteRuntimeStartupFailureReceiptIsExclusiveAndVerifiable(t *testing.T) {
	directory := t.TempDir()
	runID := uuid.New()
	receipt := nodeagent.NewRuntimeStartupFailureReceipt(nodeagent.RuntimeStartupFailureReceiptInput{RunID: runID, Phase: nodeagent.RuntimeStartupFailureResourceLoad, Cause: errors.New("missing validation input"), CleanupVerified: true})
	if err := writeRuntimeStartupFailureReceipt(directory, runID, receipt); err != nil {
		t.Fatal(err)
	}
	wire, err := os.ReadFile(filepath.Join(directory, "runtime-startup-failure-"+runID.String()+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var decoded nodeagent.RuntimeStartupFailureReceipt
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Verify(); err != nil {
		t.Fatalf("written failure receipt is invalid: %v", err)
	}
	if err := writeRuntimeStartupFailureReceipt(directory, runID, receipt); err == nil {
		t.Fatal("failure receipt path was overwritten")
	}
}

func TestRuntimeStartupFailureInputPreservesPermitBeforeShutdown(t *testing.T) {
	receipt := nodeagent.RuntimeStartupCompositionReceipt{
		SchemaVersion: 1, OperationID: uuid.New(), JournalID: uuid.New(),
		RequestDigest:     sha256.Sum256([]byte("request")),
		ReservationDigest: sha256.Sum256([]byte("reservation")), AuthorizationDigest: sha256.Sum256([]byte("authorization")),
		GrantAttemptDigest: sha256.Sum256([]byte("grant")), Permit: true, Outcome: "permitted",
	}
	input := runtimeStartupFailureInputFromComposition(nodeagent.RuntimeStartupFailureBackend, errors.New("wait failed"), nil, receipt, false)
	if !input.PermitIssued || !input.ReservationCreated || input.GrantAttemptDigest == ([sha256.Size]byte{}) {
		t.Fatalf("pre-shutdown Permit state was lost: %+v", input)
	}
	replayed := nodeagent.NewRuntimeStartupFailureReceipt(nodeagent.RuntimeStartupFailureReceiptInput{RunID: uuid.New(), OperationID: input.OperationID, JournalID: input.JournalID, Phase: input.Phase, Cause: errors.New("wait failed"), RequestDigest: input.RequestDigest, ReservationDigest: input.ReservationDigest, AuthorizationDigest: input.AuthorizationDigest, GrantAttemptDigest: input.GrantAttemptDigest, ReservationCreated: input.ReservationCreated, GrantCreated: input.GrantCreated, PermitIssued: input.PermitIssued, CleanupVerified: input.CleanupVerified})
	if err := replayed.Verify(); err != nil {
		t.Fatalf("Permit-bearing failure receipt is invalid: %v", err)
	}
}
