package nodeagent

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/remediation"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExecutionDispatcherClaimsExecutesAndCompletes(t *testing.T) {
	now := time.Now().UTC()
	operation := remediation.Operation{
		ID: uuid.New(), WorkerID: uuid.New(), WorkerEpoch: 3,
		NodeIdentity: "node-1", DeviceIdentity: "gpu-0",
		EvidenceDigest: digestForTest("failure"), CertificationRevision: "matrix-v1",
		ActionLevel: remediation.ActionL0ProcessRestart, State: remediation.StateExecuting,
		RequestedAt: now.Add(-time.Second), DeadlineAt: now.Add(time.Minute),
	}
	source := &dispatchSource{operation: operation, finishedAt: now}
	client, err := NewClient(&recordingNodeAgentClient{response: &velav1.ExecuteRemediationResponse{
		OperationId: operation.ID.String(), Success: true, ResultCode: "POSTCHECK_OK", ResultDetail: "verified",
		PostcheckSha256: digestForTest("postcheck"), StartedAt: timestamppb.New(now), FinishedAt: timestamppb.New(now),
	}}, "controller/control-1")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	dispatcher, err := NewExecutionDispatcher(source, fakeAgentResolver{client: client}, "controller/control-1", 10)
	if err != nil {
		t.Fatalf("NewExecutionDispatcher: %v", err)
	}
	dispatcher.clock = func() time.Time { return now }
	result, err := dispatcher.RunOnce(context.Background())
	if err != nil || result.Listed != 1 || result.Dispatched != 1 || len(source.claims) != 1 || len(source.completions) != 1 {
		t.Fatalf("dispatch result = %#v error=%v claims=%#v completions=%#v", result, err, source.claims, source.completions)
	}
}

type fakeAgentResolver struct{ client *Client }

func (resolver fakeAgentResolver) Resolve(context.Context, string) (*Client, error) {
	return resolver.client, nil
}

type dispatchSource struct {
	operation   remediation.Operation
	claims      []recordedExecutionClaim
	completions []remediation.Completion
	finishedAt  time.Time
}

func (source *dispatchSource) ListExecuting(context.Context, int) ([]remediation.Operation, error) {
	if source.operation.State != remediation.StateExecuting {
		return nil, nil
	}
	return []remediation.Operation{source.operation}, nil
}

func (source *dispatchSource) Get(_ context.Context, operationID uuid.UUID) (remediation.Operation, error) {
	if operationID != source.operation.ID {
		return remediation.Operation{}, &remediation.Failure{Code: remediation.FailureNotFound, Message: "not found"}
	}
	return source.operation, nil
}

func (source *dispatchSource) ClaimExecution(_ context.Context, operationID, workerID uuid.UUID, epoch int64, claimID uuid.UUID, actor string) (remediation.ClaimResult, error) {
	source.claims = append(source.claims, recordedExecutionClaim{OperationID: operationID, WorkerID: workerID, WorkerEpoch: epoch, ClaimID: claimID, Actor: actor})
	return remediation.ClaimResult{OperationID: operationID, ClaimID: claimID}, nil
}

func (source *dispatchSource) Complete(_ context.Context, completion remediation.Completion) (remediation.Result, error) {
	source.completions = append(source.completions, completion)
	source.operation.State = remediation.StateSucceeded
	source.operation.FinishedAt = &source.finishedAt
	source.operation.ResultCode = completion.ResultCode
	source.operation.ResultDetail = completion.ResultDetail
	return remediation.Result{OperationID: completion.OperationID, State: remediation.StateSucceeded}, nil
}

func (source *dispatchSource) Recover(_ context.Context, recovery remediation.Recovery) (remediation.Result, error) {
	source.operation.State = remediation.StateQuarantined
	return remediation.Result{OperationID: recovery.OperationID, State: remediation.StateQuarantined}, nil
}

var _ AgentResolver = fakeAgentResolver{}
var _ ExecutionSource = (*dispatchSource)(nil)
