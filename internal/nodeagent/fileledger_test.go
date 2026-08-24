package nodeagent

import (
	"context"
	"crypto/sha256"
	"errors"
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

func TestFileLedgerExecutionIntentIsLockedAcrossAgentProcesses(t *testing.T) {
	directory := t.TempDir()
	first, err := NewFileLedger(directory)
	if err != nil {
		t.Fatalf("NewFileLedger first: %v", err)
	}
	second, err := NewFileLedger(directory)
	if err != nil {
		t.Fatalf("NewFileLedger second: %v", err)
	}
	operationID := uuid.New()
	intent := ExecutionIntent{
		OperationID: operationID, RequestHash: sha256.Sum256([]byte("request")),
		ActorIdentity: "controller/control-1", StartedAt: time.Unix(10, 0).UTC(),
	}
	acquired, err := first.Begin(context.Background(), intent)
	if err != nil || !acquired.Acquired {
		t.Fatalf("first Begin = %#v error=%v", acquired, err)
	}
	if _, err := second.Begin(context.Background(), intent); !errors.Is(err, errExecutionInProgress) {
		t.Fatalf("concurrent Begin error = %v, want execution in progress", err)
	}
	receipt := Receipt{
		RequestHash: intent.RequestHash, ActorIdentity: intent.ActorIdentity,
		Result: Result{OperationID: operationID, ResultCode: "FAILED", ResultDetail: "known failure", StartedAt: intent.StartedAt, FinishedAt: intent.StartedAt.Add(time.Second)},
	}
	if err := first.Save(context.Background(), receipt); err != nil {
		t.Fatalf("Save terminal receipt: %v", err)
	}
	loaded, found, err := second.Load(context.Background(), operationID)
	if err != nil || !found || !equalResult(loaded.Result, receipt.Result) {
		t.Fatalf("Load after lock release = %#v found=%v error=%v", loaded, found, err)
	}
}

func TestFileLedgerInterruptedIntentRequiresReleasedProcessLock(t *testing.T) {
	directory := t.TempDir()
	first, err := NewFileLedger(directory)
	if err != nil {
		t.Fatalf("NewFileLedger first: %v", err)
	}
	operationID := uuid.New()
	intent := ExecutionIntent{
		OperationID: operationID, RequestHash: sha256.Sum256([]byte("request")),
		ActorIdentity: "controller/control-1", StartedAt: time.Unix(10, 0).UTC(),
	}
	if acquired, err := first.Begin(context.Background(), intent); err != nil || !acquired.Acquired {
		t.Fatalf("first Begin = %#v error=%v", acquired, err)
	}
	if err := first.releaseExecutionLock(operationID); err != nil {
		t.Fatalf("simulate process exit: %v", err)
	}
	restarted, err := NewFileLedger(directory)
	if err != nil {
		t.Fatalf("NewFileLedger restarted: %v", err)
	}
	interrupted, err := restarted.Begin(context.Background(), intent)
	if err != nil || interrupted.Acquired || interrupted.StartedAt != intent.StartedAt {
		t.Fatalf("restarted Begin = %#v error=%v", interrupted, err)
	}
	receipt := Receipt{
		RequestHash: intent.RequestHash, ActorIdentity: intent.ActorIdentity,
		Result: Result{OperationID: operationID, ResultCode: "EXECUTION_OUTCOME_UNKNOWN", ResultDetail: "interrupted", StartedAt: intent.StartedAt, FinishedAt: intent.StartedAt.Add(time.Second)},
	}
	if err := restarted.Save(context.Background(), receipt); err != nil {
		t.Fatalf("Save interrupted receipt: %v", err)
	}
}

func TestFileLedgerDoesNotHideDirectorySyncFailure(t *testing.T) {
	directory := t.TempDir()
	ledger, err := NewFileLedger(directory)
	if err != nil {
		t.Fatalf("NewFileLedger: %v", err)
	}
	syncErr := errors.New("injected directory sync failure")
	ledger.syncDirectory = func() error { return syncErr }
	operationID := uuid.New()
	receipt := Receipt{
		RequestHash: sha256.Sum256([]byte("request")), ActorIdentity: "controller/control-1",
		Result: Result{OperationID: operationID, ResultCode: "FAILED", ResultDetail: "known failure", StartedAt: time.Unix(1, 0).UTC(), FinishedAt: time.Unix(2, 0).UTC()},
	}
	if err := ledger.Save(context.Background(), receipt); !errors.Is(err, syncErr) {
		t.Fatalf("Save error = %v, want directory sync failure", err)
	}
	if _, found, err := ledger.Load(context.Background(), operationID); !errors.Is(err, syncErr) || found {
		t.Fatalf("Load before durability confirmation found=%v error=%v, want directory sync failure", found, err)
	}

	reopened, err := NewFileLedger(directory)
	if err != nil {
		t.Fatalf("reopen ledger: %v", err)
	}
	reopened.syncDirectory = func() error { return syncErr }
	if _, found, err := reopened.Load(context.Background(), operationID); !errors.Is(err, syncErr) || found {
		t.Fatalf("reopened Load before durability confirmation found=%v error=%v, want directory sync failure", found, err)
	}
	reopened.syncDirectory = func() error { return syncDirectory(directory) }
	loaded, found, err := reopened.Load(context.Background(), operationID)
	if err != nil || !found || loaded.ActorIdentity != receipt.ActorIdentity || !equalResult(loaded.Result, receipt.Result) {
		t.Fatalf("Load after durability confirmation = %#v found=%v error=%v", loaded, found, err)
	}
}

func TestFileLedgerDoesNotReplayIntentBeforeDirectorySyncSucceeds(t *testing.T) {
	directory := t.TempDir()
	ledger, err := NewFileLedger(directory)
	if err != nil {
		t.Fatalf("NewFileLedger: %v", err)
	}
	syncErr := errors.New("injected directory sync failure")
	ledger.syncDirectory = func() error { return syncErr }
	intent := ExecutionIntent{
		OperationID: uuid.New(), RequestHash: sha256.Sum256([]byte("request")),
		ActorIdentity: "controller/control-1", StartedAt: time.Unix(10, 0).UTC(),
	}
	if _, err := ledger.Begin(context.Background(), intent); !errors.Is(err, syncErr) {
		t.Fatalf("initial Begin error = %v, want directory sync failure", err)
	}
	if replayed, err := ledger.Begin(context.Background(), intent); !errors.Is(err, syncErr) || replayed.Acquired {
		t.Fatalf("Begin before durability confirmation = %#v error=%v, want directory sync failure", replayed, err)
	}

	reopened, err := NewFileLedger(directory)
	if err != nil {
		t.Fatalf("reopen ledger: %v", err)
	}
	reopened.syncDirectory = func() error { return syncErr }
	if replayed, err := reopened.Begin(context.Background(), intent); !errors.Is(err, syncErr) || replayed.Acquired {
		t.Fatalf("reopened Begin before durability confirmation = %#v error=%v, want directory sync failure", replayed, err)
	}
	reopened.syncDirectory = func() error { return syncDirectory(directory) }
	replayed, err := reopened.Begin(context.Background(), intent)
	if err != nil || replayed.Acquired || replayed.StartedAt != intent.StartedAt {
		t.Fatalf("Begin after durability confirmation = %#v error=%v", replayed, err)
	}
	if err := replayed.releaseExecution(); err != nil {
		t.Fatalf("release confirmed intent lock: %v", err)
	}
}
