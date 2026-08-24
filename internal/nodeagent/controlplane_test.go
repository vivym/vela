package nodeagent

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/remediation"
)

func TestControlPlaneAuthorizerEnforcesAuthoritativeOperation(t *testing.T) {
	now := time.Unix(8000, 0).UTC()
	evidence := digestForTest("failure")
	operation := remediation.Operation{
		ID: uuid.New(), WorkerID: uuid.New(), WorkerEpoch: 4,
		NodeIdentity: "node-1", DeviceIdentity: "gpu-0",
		EvidenceDigest: evidence, CertificationRevision: "matrix-v2",
		ActionLevel: remediation.ActionL2GPUReset, State: remediation.StateExecuting,
		DeadlineAt: now.Add(time.Minute),
	}
	reader := &recordingOperationReader{operation: operation}
	claimer := &recordingExecutionClaimer{}
	authorizer, err := NewControlPlaneAuthorizer(reader, claimer, "controller/node-1")
	if err != nil {
		t.Fatalf("NewControlPlaneAuthorizer: %v", err)
	}
	authorizer.clock = func() time.Time { return now }
	request := requestFromOperation(operation)
	if err := authorizer.Authorize(context.Background(), request); err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if len(claimer.claims) != 1 || claimer.claims[0].ClaimID != request.ExecutionClaimID {
		t.Fatalf("execution claims = %#v", claimer.claims)
	}

	request.DeviceIdentity = "gpu-1"
	if err := authorizer.Authorize(context.Background(), request); err == nil {
		t.Fatal("device mismatch was accepted")
	}
	request = requestFromOperation(operation)
	request.DeadlineAt = operation.DeadlineAt.Add(time.Second)
	if err := authorizer.Authorize(context.Background(), request); err == nil {
		t.Fatal("deadline extension was accepted")
	}
	reader.operation.State = remediation.StateRequested
	if err := authorizer.Authorize(context.Background(), requestFromOperation(reader.operation)); err == nil {
		t.Fatal("non-executing operation was accepted")
	}
}

func TestControlPlaneLedgerCompletesAndReplaysAuthoritativeOperation(t *testing.T) {
	now := time.Unix(9000, 0).UTC()
	operation := remediation.Operation{
		ID: uuid.New(), WorkerID: uuid.New(), WorkerEpoch: 8,
		NodeIdentity: "node-2", DeviceIdentity: "gpu-1",
		EvidenceDigest: digestForTest("failure"), CertificationRevision: "matrix-v3",
		ActionLevel: remediation.ActionL0ProcessRestart, State: remediation.StateExecuting,
		RequestedAt: now.Add(-time.Second), StartedAt: timePtr(now.Add(-500 * time.Millisecond)),
		DeadlineAt: now.Add(time.Minute),
	}
	store := &recordingOperationStore{operation: operation, finishedAt: now}
	ledger, err := NewControlPlaneLedger(store, store, "controller/node-2")
	if err != nil {
		t.Fatalf("NewControlPlaneLedger: %v", err)
	}
	result := Result{
		OperationID: operation.ID, Success: true, ResultCode: "POSTCHECK_OK",
		ResultDetail: "verified", PostcheckHash: digestForTest("postcheck"),
		StartedAt: *operation.StartedAt, FinishedAt: now,
	}
	if err := ledger.Save(context.Background(), Receipt{RequestHash: hashRequest(requestFromOperation(operation)), Result: result}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if len(store.completions) != 1 || !store.completions[0].Success ||
		store.completions[0].ActorIdentity != "controller/node-2" {
		t.Fatalf("completion = %#v", store.completions)
	}
	receipt, found, err := ledger.Load(context.Background(), operation.ID)
	if err != nil || !found {
		t.Fatalf("Load terminal receipt: found=%v err=%v", found, err)
	}
	if !receipt.Result.Success || receipt.Result.ResultCode != "POSTCHECK_OK" {
		t.Fatalf("replayed receipt = %#v", receipt)
	}
	if _, found, err := ledger.Load(context.Background(), uuid.New()); err != nil || found {
		t.Fatalf("missing operation load = found=%v err=%v", found, err)
	}
}

func TestControlPlaneLedgerRejectsWrongRequestHash(t *testing.T) {
	operation := remediation.Operation{
		ID: uuid.New(), WorkerID: uuid.New(), WorkerEpoch: 2,
		NodeIdentity: "node-3", DeviceIdentity: "gpu-0",
		EvidenceDigest: digestForTest("failure"), CertificationRevision: "matrix-v1",
		ActionLevel: remediation.ActionL1CUDACleanup, State: remediation.StateExecuting,
		RequestedAt: time.Unix(10000, 0).UTC(), DeadlineAt: time.Unix(10060, 0).UTC(),
	}
	store := &recordingOperationStore{operation: operation}
	ledger, err := NewControlPlaneLedger(store, store, "controller/node-3")
	if err != nil {
		t.Fatalf("NewControlPlaneLedger: %v", err)
	}
	err = ledger.Save(context.Background(), Receipt{
		RequestHash: [32]byte{1},
		Result:      Result{OperationID: operation.ID, ResultCode: "FAILED", ResultDetail: "failed"},
	})
	if err == nil || len(store.completions) != 0 {
		t.Fatalf("wrong hash save = %v completions=%d", err, len(store.completions))
	}
}

type recordingOperationReader struct {
	operation remediation.Operation
	err       error
}

type recordedExecutionClaim struct {
	OperationID uuid.UUID
	WorkerID    uuid.UUID
	WorkerEpoch int64
	ClaimID     uuid.UUID
	Actor       string
}

type recordingExecutionClaimer struct {
	claims []recordedExecutionClaim
}

func (claimer *recordingExecutionClaimer) ClaimExecution(
	_ context.Context,
	operationID, workerID uuid.UUID,
	workerEpoch int64,
	claimID uuid.UUID,
	actor string,
) (remediation.ClaimResult, error) {
	claimer.claims = append(claimer.claims, recordedExecutionClaim{
		OperationID: operationID, WorkerID: workerID, WorkerEpoch: workerEpoch,
		ClaimID: claimID, Actor: actor,
	})
	return remediation.ClaimResult{OperationID: operationID, ClaimID: claimID}, nil
}

func (reader *recordingOperationReader) Get(_ context.Context, operationID uuid.UUID) (remediation.Operation, error) {
	if reader.err != nil {
		return remediation.Operation{}, reader.err
	}
	if reader.operation.ID != operationID {
		return remediation.Operation{}, &remediation.Failure{Code: remediation.FailureNotFound, Message: "not found"}
	}
	return reader.operation, nil
}

type recordingOperationStore struct {
	operation   remediation.Operation
	completions []remediation.Completion
	finishedAt  time.Time
}

func (store *recordingOperationStore) Get(_ context.Context, operationID uuid.UUID) (remediation.Operation, error) {
	if store.operation.ID != operationID {
		return remediation.Operation{}, &remediation.Failure{Code: remediation.FailureNotFound, Message: "not found"}
	}
	return store.operation, nil
}

func (store *recordingOperationStore) Complete(_ context.Context, completion remediation.Completion) (remediation.Result, error) {
	if completion.OperationID != store.operation.ID {
		return remediation.Result{}, errors.New("operation mismatch")
	}
	store.completions = append(store.completions, completion)
	store.operation.State = remediation.StateSucceeded
	store.operation.ResultCode = completion.ResultCode
	store.operation.ResultDetail = completion.ResultDetail
	store.operation.PostcheckDigest = append([]byte(nil), completion.PostcheckHash...)
	store.operation.FinishedAt = &store.finishedAt
	return remediation.Result{OperationID: completion.OperationID, State: remediation.StateSucceeded}, nil
}

func digestForTest(value string) []byte {
	digest := sha256.Sum256([]byte(value))
	return digest[:]
}

func timePtr(value time.Time) *time.Time { return &value }
