package nodeagent

import (
	"context"
	"crypto/sha256"
	"os"
	"sync"
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

func TestFileLedgerPublishesOnlyOneConcurrentReceipt(t *testing.T) {
	ledger, err := NewFileLedger(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileLedger: %v", err)
	}
	operationID := uuid.New()
	base := Receipt{
		RequestHash: sha256.Sum256([]byte("request")), ActorIdentity: "controller/control-1",
		Result: Result{OperationID: operationID, ResultCode: "FAILED", ResultDetail: "first", StartedAt: time.Unix(1, 0).UTC(), FinishedAt: time.Unix(2, 0).UTC()},
	}
	conflicting := base
	conflicting.Result.ResultDetail = "second"
	start := make(chan struct{})
	errorsByWriter := make(chan error, 2)
	var writers sync.WaitGroup
	for _, receipt := range []Receipt{base, conflicting} {
		writers.Add(1)
		go func(value Receipt) {
			defer writers.Done()
			<-start
			errorsByWriter <- ledger.Save(context.Background(), value)
		}(receipt)
	}
	close(start)
	writers.Wait()
	close(errorsByWriter)
	succeeded := 0
	for err := range errorsByWriter {
		if err == nil {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Fatalf("concurrent receipt successes = %d, want 1", succeeded)
	}
	loaded, found, err := ledger.Load(context.Background(), operationID)
	if err != nil || !found || loaded.Result.ResultDetail != "first" && loaded.Result.ResultDetail != "second" {
		t.Fatalf("concurrent receipt winner = %#v found=%v error=%v", loaded, found, err)
	}
}

func TestFileLedgerRejectsWritableReceiptDirectory(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o777); err != nil {
		t.Fatalf("relax receipt directory permissions: %v", err)
	}
	if _, err := NewFileLedger(directory); err == nil {
		t.Fatal("group/world-writable receipt directory was accepted")
	}
}
