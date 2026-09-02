package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/remediation"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const controllerSPIFFE = "spiffe://vela.internal/controller/control-1"

func TestNodeAgentServerBindsControllerIdentityAndLocalNodeTarget(t *testing.T) {
	workerID := uuid.New()
	now := time.Unix(1000, 0).UTC()
	resolver := mustControllerResolver(t)
	executor := &recordingExecutor{}
	server, err := NewServer(NodeAgentIdentity{NodeIdentity: "node-1", AgentID: uuid.New(), AgentEpoch: 9}, resolver, executor, &memoryLedger{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	server.clock = func() time.Time { return now }
	evidence := sha256.Sum256([]byte("failure"))
	request := validAgentRequest(workerID, now)
	request.FailureEvidenceDigest = evidence[:]
	response, err := server.ExecuteRemediation(controllerPeerContext(t, controllerSPIFFE), request)
	if err != nil {
		t.Fatalf("ExecuteRemediation: %v", err)
	}
	if !response.GetSuccess() || response.GetResultCode() != "POSTCHECK_OK" || len(response.GetPostcheckSha256()) != sha256.Size {
		t.Fatalf("Node Agent response = %#v", response)
	}
	if executor.plan.OperationID != uuid.MustParse(request.GetOperationId()) || executor.plan.ExecutionClaimID != uuid.MustParse(request.GetExecutionClaimId()) || executor.plan.WorkerInstanceID != workerID || executor.plan.WorkerInstanceEpoch != 1 || executor.plan.DeviceID != testDeviceID0 || executor.plan.DeviceEpoch != 1 || executor.plan.FailureClass != request.GetFailureClass() || executor.plan.DeadlineAt != request.GetDeadlineAt().AsTime() || !bytes.Equal(executor.plan.FailureEvidenceDigest, evidence[:]) {
		t.Fatalf("executor plan = %#v", executor.plan)
	}
}

func TestNodeAgentServerRejectsUnauthenticatedOrUnsafeRequests(t *testing.T) {
	workerID := uuid.New()
	now := time.Unix(2000, 0).UTC()
	server, err := NewServer(NodeAgentIdentity{NodeIdentity: "node-1", AgentID: uuid.New(), AgentEpoch: 1}, mustControllerResolver(t), &recordingExecutor{}, &memoryLedger{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	server.clock = func() time.Time { return now }
	base := validAgentRequest(workerID, now)
	if _, err := server.ExecuteRemediation(context.Background(), base); status.Code(err) != 16 {
		t.Fatalf("missing TLS error = %v, want unauthenticated", err)
	}
	base.ActionLevel = string(remediation.ActionL6BMCPowerCycle)
	if _, err := server.ExecuteRemediation(controllerPeerContext(t, controllerSPIFFE), base); status.Code(err) != 7 {
		t.Fatalf("L6 error = %v, want permission denied", err)
	}
	base = validAgentRequest(workerID, now)
	base.NodeIdentity = "node-2"
	if _, err := server.ExecuteRemediation(controllerPeerContext(t, controllerSPIFFE), base); status.Code(err) != 7 {
		t.Fatalf("local identity mismatch error = %v, want permission denied", err)
	}
	base = validAgentRequest(workerID, now)
	base.DeadlineAt = timestamppb.New(now.Add(-time.Second))
	if _, err := server.ExecuteRemediation(controllerPeerContext(t, controllerSPIFFE), base); status.Code(err) != 3 {
		t.Fatalf("expired deadline error = %v, want invalid argument", err)
	}
	if _, err := server.ExecuteRemediation(controllerPeerContext(t, "spiffe://vela.internal/controller/unknown"), validAgentRequest(workerID, now)); status.Code(err) != 16 {
		t.Fatalf("unregistered controller error = %v, want unauthenticated", err)
	}
}

func TestNodeAgentServerReplaysAndRejectsConflictingReceipts(t *testing.T) {
	workerID := uuid.New()
	now := time.Unix(3000, 0).UTC()
	executor := &recordingExecutor{}
	server, err := NewServer(NodeAgentIdentity{NodeIdentity: "node-1", AgentID: uuid.New(), AgentEpoch: 1}, mustControllerResolver(t), executor, &memoryLedger{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	server.clock = func() time.Time { return now }
	request := validAgentRequest(workerID, now)
	first, err := server.ExecuteRemediation(controllerPeerContext(t, controllerSPIFFE), request)
	if err != nil {
		t.Fatalf("first ExecuteRemediation: %v", err)
	}
	second, err := server.ExecuteRemediation(controllerPeerContext(t, controllerSPIFFE), request)
	if err != nil {
		t.Fatalf("replay ExecuteRemediation: %v", err)
	}
	if executor.calls != 1 || first.GetResultCode() != second.GetResultCode() || !bytes.Equal(first.GetPostcheckSha256(), second.GetPostcheckSha256()) {
		t.Fatalf("replay changed execution: calls=%d first=%#v second=%#v", executor.calls, first, second)
	}
	conflicting := validAgentRequest(workerID, now)
	conflicting.OperationId = request.GetOperationId()
	conflicting.DeviceIdentity = testGPUUUID1
	if _, err := server.ExecuteRemediation(controllerPeerContext(t, controllerSPIFFE), conflicting); status.Code(err) != 6 {
		t.Fatalf("conflicting replay error = %v, want AlreadyExists", err)
	}
	conflicting = proto.Clone(request).(*velav1.ExecuteRemediationRequest)
	conflicting.FailureClass = "different_failure"
	if _, err := server.ExecuteRemediation(controllerPeerContext(t, controllerSPIFFE), conflicting); status.Code(err) != 6 {
		t.Fatalf("conflicting failure class replay error = %v, want AlreadyExists", err)
	}
	conflicting = validAgentRequest(workerID, now)
	conflicting.OperationId = request.GetOperationId()
	if _, err := server.ExecuteRemediation(controllerPeerContext(t, controllerSPIFFE), conflicting); status.Code(err) != 6 {
		t.Fatalf("conflicting claim replay error = %v, want AlreadyExists", err)
	}
}

func TestNodeAgentServerFailsClosedOnExecutorAndPostcheck(t *testing.T) {
	workerID := uuid.New()
	now := time.Unix(4000, 0).UTC()
	server, err := NewServer(NodeAgentIdentity{NodeIdentity: "node-1", AgentID: uuid.New(), AgentEpoch: 1}, mustControllerResolver(t), &recordingExecutor{err: errors.New("host command failed")}, &memoryLedger{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	server.clock = func() time.Time { return now }
	response, err := server.ExecuteRemediation(controllerPeerContext(t, controllerSPIFFE), validAgentRequest(workerID, now))
	if err != nil || response.GetSuccess() || response.GetResultCode() != "EXECUTION_FAILED" {
		t.Fatalf("executor failure = %#v error=%v", response, err)
	}
	invalid, err := NewServer(NodeAgentIdentity{NodeIdentity: "node-1", AgentID: uuid.New(), AgentEpoch: 1}, mustControllerResolver(t), &recordingExecutor{result: &remediation.ExecutionResult{ResultCode: "POSTCHECK_OK"}}, &memoryLedger{})
	if err != nil {
		t.Fatalf("NewServer invalid postcheck: %v", err)
	}
	invalid.clock = func() time.Time { return now }
	response, err = invalid.ExecuteRemediation(controllerPeerContext(t, controllerSPIFFE), validAgentRequest(workerID, now))
	if err != nil || response.GetSuccess() || response.GetResultCode() != "INVALID_POSTCHECK" {
		t.Fatalf("invalid postcheck = %#v error=%v", response, err)
	}
}

func TestNodeAgentServerDoesNotRepeatActionAfterInterruptedIntent(t *testing.T) {
	workerID := uuid.New()
	now := time.Unix(4500, 0).UTC()
	executor := &recordingExecutor{}
	ledger := &memoryLedger{}
	server, err := NewServer(
		NodeAgentIdentity{NodeIdentity: "node-1", AgentID: uuid.New(), AgentEpoch: 1},
		mustControllerResolver(t), executor, ledger,
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	server.clock = func() time.Time { return now }
	request := validAgentRequest(workerID, now)
	parsed, _, err := parseRequest(request, now)
	if err != nil {
		t.Fatalf("parse interrupted request: %v", err)
	}
	intent, err := ledger.Begin(context.Background(), ExecutionIntent{
		OperationID: parsed.OperationID, RequestHash: hashRequest(parsed),
		ActorIdentity: "controller/control-1", StartedAt: now.Add(-time.Second),
	})
	if err != nil || !intent.Acquired {
		t.Fatalf("seed interrupted intent = %#v error=%v", intent, err)
	}
	response, err := server.ExecuteRemediation(controllerPeerContext(t, controllerSPIFFE), request)
	if err != nil || response.GetSuccess() || response.GetResultCode() != "EXECUTION_OUTCOME_UNKNOWN" {
		t.Fatalf("interrupted execution response = %#v error=%v", response, err)
	}
	if executor.calls != 0 {
		t.Fatalf("interrupted execution repeated privileged action %d times", executor.calls)
	}
}

type recordingExecutor struct {
	plan   remediation.Plan
	err    error
	result *remediation.ExecutionResult
	calls  int
}

func (executor *recordingExecutor) Execute(_ context.Context, plan remediation.Plan) (remediation.ExecutionResult, error) {
	executor.plan = plan
	executor.calls++
	if executor.err != nil {
		return remediation.ExecutionResult{}, executor.err
	}
	if executor.result != nil {
		return *executor.result, nil
	}
	return remediation.ExecutionResult{PostcheckDigest: sha256.Sum256([]byte("post-check")), PostcheckVerified: true, Detail: "node agent remediation completed", ResultCode: "POSTCHECK_OK"}, nil
}

type memoryLedger struct {
	mu       sync.Mutex
	receipts map[uuid.UUID]Receipt
	intents  map[uuid.UUID]ExecutionIntent
}

func (ledger *memoryLedger) Load(_ context.Context, operationID uuid.UUID) (Receipt, bool, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	result, found := ledger.receipts[operationID]
	return result, found, nil
}

func (ledger *memoryLedger) Save(_ context.Context, receipt Receipt) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.receipts == nil {
		ledger.receipts = make(map[uuid.UUID]Receipt)
	}
	if prior, found := ledger.receipts[receipt.Result.OperationID]; found {
		if prior.RequestHash == receipt.RequestHash && prior.ActorIdentity == receipt.ActorIdentity && equalResult(prior.Result, receipt.Result) {
			return nil
		}
		return errors.New("receipt already exists")
	}
	ledger.receipts[receipt.Result.OperationID] = receipt
	return nil
}

func (ledger *memoryLedger) Begin(_ context.Context, intent ExecutionIntent) (ExecutionIntentResult, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.intents == nil {
		ledger.intents = make(map[uuid.UUID]ExecutionIntent)
	}
	if prior, found := ledger.intents[intent.OperationID]; found {
		if prior.RequestHash != intent.RequestHash || prior.ActorIdentity != intent.ActorIdentity {
			return ExecutionIntentResult{}, errors.New("execution intent already exists")
		}
		return ExecutionIntentResult{StartedAt: prior.StartedAt}, nil
	}
	ledger.intents[intent.OperationID] = intent
	return ExecutionIntentResult{Acquired: true, StartedAt: intent.StartedAt}, nil
}

func validAgentRequest(workerID uuid.UUID, now time.Time) *velav1.ExecuteRemediationRequest {
	evidence := sha256.Sum256([]byte("failure"))
	return &velav1.ExecuteRemediationRequest{OperationId: uuid.NewString(), WorkerInstanceId: workerID.String(), WorkerInstanceEpoch: 1, NodeIdentity: "node-1", DeviceIdentity: testGPUUUID0, FailureClass: "process_failure", ActionLevel: string(remediation.ActionL0ProcessRestart), CertificationRevision: "matrix-v1", FailureEvidenceDigest: evidence[:], DeadlineAt: timestamppb.New(now.Add(time.Minute)), ExecutionClaimId: uuid.NewString(), DeviceId: testDeviceID0.String(), DeviceEpoch: 1}
}

func mustControllerResolver(t *testing.T) *StaticControllerIdentityResolver {
	t.Helper()
	resolver, err := NewStaticControllerIdentityResolver(map[string]string{controllerSPIFFE: "controller/control-1"})
	if err != nil {
		t.Fatalf("NewStaticControllerIdentityResolver: %v", err)
	}
	return resolver
}

func controllerPeerContext(t *testing.T, spiffeID string) context.Context {
	t.Helper()
	identity, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatalf("parse test SPIFFE ID: %v", err)
	}
	certificate := &x509.Certificate{Raw: []byte(spiffeID), URIs: []*url.URL{identity}}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{HandshakeComplete: true, PeerCertificates: []*x509.Certificate{certificate}, VerifiedChains: [][]*x509.Certificate{{certificate}}}}})
}

var _ velav1.NodeAgentServiceServer = (*Server)(nil)
