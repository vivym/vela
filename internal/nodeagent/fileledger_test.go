package nodeagent

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestFileLedgerPersistsExactReceiptAndRejectsConflict(t *testing.T) {
	ledger, err := NewFileLedger(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileLedger: %v", err)
	}
	operationID := uuid.New()
	postcheckHash := sha256.Sum256([]byte("health"))
	receipt := Receipt{
		RequestHash: sha256.Sum256([]byte("request")), ActorIdentity: "controller/control-1",
		Result: Result{OperationID: operationID, Success: true, ResultCode: "POSTCHECK_OK", ResultDetail: "verified", PostcheckHash: postcheckHash[:], StartedAt: time.Unix(1, 0).UTC(), FinishedAt: time.Unix(2, 0).UTC()},
	}
	if err := ledger.Save(context.Background(), receipt); err != nil {
		t.Fatalf("Save: %v", err)
	}
	reopened, err := NewFileLedger(ledger.directory)
	if err != nil {
		t.Fatalf("reopen ledger: %v", err)
	}
	loaded, found, err := reopened.Load(context.Background(), operationID)
	if err != nil || !found || loaded.ActorIdentity != receipt.ActorIdentity || !equalResult(loaded.Result, receipt.Result) {
		t.Fatalf("Load = %#v found=%v err=%v", loaded, found, err)
	}
	if err := reopened.Save(context.Background(), receipt); err != nil {
		t.Fatalf("idempotent Save: %v", err)
	}
	conflicting := receipt
	conflicting.Result.ResultDetail = "different"
	if err := reopened.Save(context.Background(), conflicting); err == nil {
		t.Fatal("conflicting receipt was accepted")
	}
}
