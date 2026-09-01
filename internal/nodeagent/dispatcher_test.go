package nodeagent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/remediation"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestExecutionDispatcherClaimsExecutesAndCompletes(t *testing.T) {
	now := time.Now().UTC()
	operation := remediation.Operation{
		ID: uuid.New(), WorkerInstanceID: uuid.New(), WorkerInstanceEpoch: 3,
		NodeIdentity: "node-1", DeviceIdentity: "gpu-0", FailureClass: "process_failure",
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

func TestExecutionDispatcherReusesClaimAfterLostRPCResponse(t *testing.T) {
	now := time.Now().UTC()
	operation := remediation.Operation{
		ID: uuid.New(), WorkerInstanceID: uuid.New(), WorkerInstanceEpoch: 4,
		NodeIdentity: "node-2", DeviceIdentity: "gpu-1", FailureClass: "gpu_fault",
		EvidenceDigest: digestForTest("failure"), CertificationRevision: "matrix-v2",
		ActionLevel: remediation.ActionL2GPUReset, State: remediation.StateExecuting,
		RequestedAt: now.Add(-time.Second), DeadlineAt: now.Add(time.Minute),
	}
	source := &dispatchSource{operation: operation, finishedAt: now, enforceExactClaimReplay: true}
	transport := &lostResponseNodeAgentClient{response: &velav1.ExecuteRemediationResponse{
		OperationId: operation.ID.String(), Success: true, ResultCode: "POSTCHECK_OK", ResultDetail: "verified",
		PostcheckSha256: digestForTest("postcheck"), StartedAt: timestamppb.New(now), FinishedAt: timestamppb.New(now),
	}}
	client, err := NewClient(transport, "controller/control-1")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	dispatcher, err := NewExecutionDispatcher(source, fakeAgentResolver{client: client}, "controller/control-1", 10)
	if err != nil {
		t.Fatalf("NewExecutionDispatcher: %v", err)
	}
	dispatcher.clock = func() time.Time { return now }
	first, err := dispatcher.RunOnce(context.Background())
	if err != nil || first.Deferred != 1 || first.Dispatched != 0 {
		t.Fatalf("first dispatch = %#v error=%v", first, err)
	}
	second, err := dispatcher.RunOnce(context.Background())
	if err != nil || second.Dispatched != 1 || len(source.completions) != 1 || len(source.claims) != 2 {
		t.Fatalf("replayed dispatch = %#v error=%v claims=%#v completions=%#v", second, err, source.claims, source.completions)
	}
	if source.claims[0].ClaimID != source.claims[1].ClaimID {
		t.Fatalf("dispatcher changed claim across replay: %s then %s", source.claims[0].ClaimID, source.claims[1].ClaimID)
	}
}

func TestStaticAgentResolverRequiresEndpointAgentAndSPIFFEIdentity(t *testing.T) {
	identity := NodeAgentIdentity{NodeIdentity: "node-1", AgentID: uuid.New(), AgentEpoch: 3}
	endpoint := AgentEndpoint{
		Address: "127.0.0.1:9443", ServerName: "node-agent.internal",
		AgentID: identity.AgentID, AgentEpoch: identity.AgentEpoch,
		SPIFFEIdentity: NodeAgentSPIFFEIdentity(identity),
	}
	tlsConfig := ClientTLSConfig{CertificatePath: "/client.crt", PrivateKeyPath: "/client.key", RootCAPath: "/ca.crt"}
	if _, err := NewStaticAgentResolver(map[string]AgentEndpoint{identity.NodeIdentity: endpoint}, tlsConfig, "controller/control-1"); err != nil {
		t.Fatalf("NewStaticAgentResolver: %v", err)
	}
	endpoint.AgentID = uuid.New()
	if _, err := NewStaticAgentResolver(map[string]AgentEndpoint{identity.NodeIdentity: endpoint}, tlsConfig, "controller/control-1"); err == nil {
		t.Fatal("endpoint with mismatched Agent and SPIFFE identity was accepted")
	}
	endpoint.AgentID = identity.AgentID
	endpoint.AgentEpoch = identity.AgentEpoch - 1
	endpoint.SPIFFEIdentity = NodeAgentSPIFFEIdentity(identity)
	if _, err := NewStaticAgentResolver(map[string]AgentEndpoint{identity.NodeIdentity: endpoint}, tlsConfig, "controller/control-1"); err != nil {
		t.Fatalf("static resolver rejected structurally valid Agent epoch: %v", err)
	}
}

type lostResponseNodeAgentClient struct {
	velav1.NodeAgentServiceClient
	response *velav1.ExecuteRemediationResponse
	calls    int
}

func (client *lostResponseNodeAgentClient) ExecuteRemediation(context.Context, *velav1.ExecuteRemediationRequest, ...grpc.CallOption) (*velav1.ExecuteRemediationResponse, error) {
	client.calls++
	if client.calls == 1 {
		return nil, errors.New("response lost after host receipt persisted")
	}
	return client.response, nil
}

type fakeAgentResolver struct{ client *Client }

func (resolver fakeAgentResolver) Resolve(context.Context, string) (*Client, error) {
	return resolver.client, nil
}

type dispatchSource struct {
	operation               remediation.Operation
	claims                  []recordedExecutionClaim
	completions             []remediation.Completion
	finishedAt              time.Time
	enforceExactClaimReplay bool
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
	claim := recordedExecutionClaim{OperationID: operationID, WorkerInstanceID: workerID, WorkerInstanceEpoch: epoch, ClaimID: claimID, Actor: actor}
	if source.enforceExactClaimReplay && len(source.claims) > 0 {
		prior := source.claims[0]
		source.claims = append(source.claims, claim)
		if prior != claim {
			return remediation.ClaimResult{}, errors.New("conflicting execution claim")
		}
		return remediation.ClaimResult{OperationID: operationID, ClaimID: claimID, Replayed: true}, nil
	}
	source.claims = append(source.claims, claim)
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
