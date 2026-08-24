package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"

	"crypto/tls"
	"crypto/x509"
	"github.com/google/uuid"
	"github.com/vivym/vela/internal/remediation"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNodeAgentServerBindsVerifiedSPIFFEIdentityAndDeadline(t *testing.T) {
	workerID := uuid.New()
	operationID := uuid.New()
	nodeIdentity := "node-1"
	now := time.Unix(1000, 0).UTC()
	resolver := &recordingIdentityResolver{identity: NodeAgentIdentity{
		NodeIdentity: nodeIdentity, WorkerID: workerID,
	}}
	executor := &recordingExecutor{}
	server, err := NewServer(resolver, &allowAuthorizer{}, executor, &memoryLedger{})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	server.clock = func() time.Time { return now }
	evidence := sha256.Sum256([]byte("failure"))
	request := &velav1.ExecuteRemediationRequest{
		OperationId: operationID.String(), WorkerId: workerID.String(), WorkerEpoch: 7,
		NodeIdentity: nodeIdentity, DeviceIdentity: "gpu-0",
		ActionLevel: string(remediation.ActionL0ProcessRestart), CertificationRevision: "matrix-v1",
		FailureEvidenceDigest: evidence[:], DeadlineAt: timestamppb.New(now.Add(time.Minute)),
		ExecutionClaimId: uuid.NewString(),
	}
	response, err := server.ExecuteRemediation(nodeAgentPeerContext(t, "spiffe://vela.internal/node/node-1"), request)
	if err != nil {
		t.Fatalf("ExecuteRemediation: %v", err)
	}
	if !response.GetSuccess() || response.GetOperationId() != operationID.String() ||
		response.GetResultCode() != "POSTCHECK_OK" || len(response.GetPostcheckSha256()) != sha256.Size {
		t.Fatalf("Node Agent response = %#v", response)
	}
	if executor.plan.OperationID != operationID || executor.plan.WorkerID != workerID ||
		executor.plan.WorkerEpoch != 7 || executor.plan.NodeIdentity != nodeIdentity ||
		!bytes.Equal(executor.plan.FailureEvidenceDigest, evidence[:]) {
		t.Fatalf("executor plan = %#v", executor.plan)
	}
	if resolver.spiffeID != "spiffe://vela.internal/node/node-1" {
		t.Fatalf("resolver SPIFFE ID = %q", resolver.spiffeID)
	}
}

func TestNodeAgentServerRejectsIdentityPrivilegeAndDeadlineFailures(t *testing.T) {
	workerID := uuid.New()
	now := time.Unix(2000, 0).UTC()
	server, err := NewServer(
		&recordingIdentityResolver{identity: NodeAgentIdentity{NodeIdentity: "node-1", WorkerID: workerID}},
		&allowAuthorizer{},
		&recordingExecutor{},
		&memoryLedger{},
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	server.clock = func() time.Time { return now }
	base := validAgentRequest(workerID, now)
	if _, err := server.ExecuteRemediation(context.Background(), base); status.Code(err) != 16 {
		t.Fatalf("missing TLS error = %v, want unauthenticated", err)
	}
	base.ActionLevel = string(remediation.ActionL6BMCPowerCycle)
	if _, err := server.ExecuteRemediation(nodeAgentPeerContext(t, "spiffe://vela.internal/node/node-1"), base); status.Code(err) != 7 {
		t.Fatalf("L6 error = %v, want permission denied", err)
	}
	base = validAgentRequest(workerID, now)
	base.NodeIdentity = "node-2"
	if _, err := server.ExecuteRemediation(nodeAgentPeerContext(t, "spiffe://vela.internal/node/node-1"), base); status.Code(err) != 7 {
		t.Fatalf("identity mismatch error = %v, want permission denied", err)
	}
	base = validAgentRequest(workerID, now)
	base.DeadlineAt = timestamppb.New(now.Add(-time.Second))
	if _, err := server.ExecuteRemediation(nodeAgentPeerContext(t, "spiffe://vela.internal/node/node-1"), base); status.Code(err) != 3 {
		t.Fatalf("expired deadline error = %v, want invalid argument", err)
	}
}

func TestNodeAgentServerReturnsStructuredExecutorFailure(t *testing.T) {
	workerID := uuid.New()
	now := time.Unix(3000, 0).UTC()
	executor := &recordingExecutor{err: errors.New("host command failed")}
	server, err := NewServer(
		&recordingIdentityResolver{identity: NodeAgentIdentity{NodeIdentity: "node-1", WorkerID: workerID}},
		&allowAuthorizer{},
		executor,
		&memoryLedger{},
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	server.clock = func() time.Time { return now }
	response, err := server.ExecuteRemediation(
		nodeAgentPeerContext(t, "spiffe://vela.internal/node/node-1"), validAgentRequest(workerID, now),
	)
	if err != nil || response.GetSuccess() || response.GetResultCode() != "EXECUTION_FAILED" ||
		response.GetResultDetail() != "host command failed" {
		t.Fatalf("structured executor failure = %#v error=%v", response, err)
	}
}

func TestNodeAgentServerReplaysReceiptWithoutReexecuting(t *testing.T) {
	workerID := uuid.New()
	now := time.Unix(4000, 0).UTC()
	executor := &recordingExecutor{}
	server, err := NewServer(
		&recordingIdentityResolver{identity: NodeAgentIdentity{NodeIdentity: "node-1", WorkerID: workerID}},
		&allowAuthorizer{},
		executor,
		&memoryLedger{},
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	server.clock = func() time.Time { return now }
	request := validAgentRequest(workerID, now)
	first, err := server.ExecuteRemediation(nodeAgentPeerContext(t, "spiffe://vela.internal/node/node-1"), request)
	if err != nil {
		t.Fatalf("first ExecuteRemediation: %v", err)
	}
	second, err := server.ExecuteRemediation(nodeAgentPeerContext(t, "spiffe://vela.internal/node/node-1"), request)
	if err != nil {
		t.Fatalf("replay ExecuteRemediation: %v", err)
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
	if first.GetResultCode() != second.GetResultCode() ||
		!bytes.Equal(first.GetPostcheckSha256(), second.GetPostcheckSha256()) {
		t.Fatalf("replay response changed: first=%#v second=%#v", first, second)
	}
}

func TestNodeAgentServerRejectsConflictingOperationReplay(t *testing.T) {
	workerID := uuid.New()
	now := time.Unix(5000, 0).UTC()
	executor := &recordingExecutor{}
	server, err := NewServer(
		&recordingIdentityResolver{identity: NodeAgentIdentity{NodeIdentity: "node-1", WorkerID: workerID}},
		&allowAuthorizer{},
		executor,
		&memoryLedger{},
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	server.clock = func() time.Time { return now }
	request := validAgentRequest(workerID, now)
	if _, err := server.ExecuteRemediation(nodeAgentPeerContext(t, "spiffe://vela.internal/node/node-1"), request); err != nil {
		t.Fatalf("first ExecuteRemediation: %v", err)
	}
	conflicting := validAgentRequest(workerID, now)
	conflicting.OperationId = request.GetOperationId()
	conflicting.DeviceIdentity = "gpu-1"
	if _, err := server.ExecuteRemediation(nodeAgentPeerContext(t, "spiffe://vela.internal/node/node-1"), conflicting); status.Code(err) != 6 {
		t.Fatalf("conflicting replay error = %v, want AlreadyExists", err)
	}
	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
}

func TestNodeAgentServerRecordsDeadlineAfterExecutorReturns(t *testing.T) {
	workerID := uuid.New()
	now := time.Now().UTC()
	executor := &recordingExecutor{delay: 20 * time.Millisecond}
	server, err := NewServer(
		&recordingIdentityResolver{identity: NodeAgentIdentity{NodeIdentity: "node-1", WorkerID: workerID}},
		&allowAuthorizer{},
		executor,
		&memoryLedger{},
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	server.clock = time.Now
	request := validAgentRequest(workerID, now)
	request.DeadlineAt = timestamppb.New(now.Add(2 * time.Millisecond))
	response, err := server.ExecuteRemediation(nodeAgentPeerContext(t, "spiffe://vela.internal/node/node-1"), request)
	if err != nil || response.GetSuccess() || response.GetResultCode() != "DEADLINE_EXCEEDED" {
		t.Fatalf("deadline response = %#v error=%v", response, err)
	}
}

func TestNodeAgentServerRejectsInvalidPostcheck(t *testing.T) {
	workerID := uuid.New()
	now := time.Unix(6000, 0).UTC()
	executor := &recordingExecutor{result: &remediation.ExecutionResult{ResultCode: "POSTCHECK_OK"}}
	server, err := NewServer(
		&recordingIdentityResolver{identity: NodeAgentIdentity{NodeIdentity: "node-1", WorkerID: workerID}},
		&allowAuthorizer{},
		executor,
		&memoryLedger{},
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	server.clock = func() time.Time { return now }
	response, err := server.ExecuteRemediation(nodeAgentPeerContext(t, "spiffe://vela.internal/node/node-1"), validAgentRequest(workerID, now))
	if err != nil || response.GetSuccess() || response.GetResultCode() != "INVALID_POSTCHECK" {
		t.Fatalf("invalid postcheck response = %#v error=%v", response, err)
	}
}

func TestNodeAgentServerRequiresControlPlaneAuthorization(t *testing.T) {
	workerID := uuid.New()
	now := time.Unix(7000, 0).UTC()
	executor := &recordingExecutor{}
	server, err := NewServer(
		&recordingIdentityResolver{identity: NodeAgentIdentity{NodeIdentity: "node-1", WorkerID: workerID}},
		&allowAuthorizer{err: errors.New("operation is not executing")},
		executor,
		&memoryLedger{},
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	server.clock = func() time.Time { return now }
	if _, err := server.ExecuteRemediation(nodeAgentPeerContext(t, "spiffe://vela.internal/node/node-1"), validAgentRequest(workerID, now)); status.Code(err) != 9 {
		t.Fatalf("authorization error = %v, want FailedPrecondition", err)
	}
	if executor.calls != 0 {
		t.Fatalf("executor calls = %d, want 0", executor.calls)
	}
}

type recordingIdentityResolver struct {
	identity NodeAgentIdentity
	spiffeID string
}

func (resolver *recordingIdentityResolver) ResolveNodeAgent(_ context.Context, spiffeID string) (NodeAgentIdentity, error) {
	resolver.spiffeID = spiffeID
	return resolver.identity, nil
}

type recordingExecutor struct {
	plan   remediation.Plan
	err    error
	result *remediation.ExecutionResult
	delay  time.Duration
	calls  int
}

func (executor *recordingExecutor) Execute(ctx context.Context, plan remediation.Plan) (remediation.ExecutionResult, error) {
	executor.plan = plan
	executor.calls++
	if executor.delay > 0 {
		timer := time.NewTimer(executor.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
		}
	}
	if executor.err != nil {
		return remediation.ExecutionResult{}, executor.err
	}
	if executor.result != nil {
		return *executor.result, nil
	}
	return remediation.ExecutionResult{
		PostcheckDigest:   sha256.Sum256([]byte("post-check")),
		PostcheckVerified: true,
		Detail:            "node Agent remediation completed", ResultCode: "POSTCHECK_OK",
	}, nil
}

type allowAuthorizer struct {
	err error
}

func (authorizer *allowAuthorizer) Authorize(context.Context, Request) error {
	return authorizer.err
}

type memoryLedger struct {
	mu       sync.Mutex
	receipts map[uuid.UUID]Receipt
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
	if _, found := ledger.receipts[receipt.Result.OperationID]; found {
		return errors.New("receipt already exists")
	}
	ledger.receipts[receipt.Result.OperationID] = receipt
	return nil
}

func validAgentRequest(workerID uuid.UUID, now time.Time) *velav1.ExecuteRemediationRequest {
	evidence := sha256.Sum256([]byte("failure"))
	return &velav1.ExecuteRemediationRequest{
		OperationId: uuid.NewString(), WorkerId: workerID.String(), WorkerEpoch: 1,
		NodeIdentity: "node-1", DeviceIdentity: "gpu-0",
		ActionLevel: string(remediation.ActionL0ProcessRestart), CertificationRevision: "matrix-v1",
		FailureEvidenceDigest: evidence[:], DeadlineAt: timestamppb.New(now.Add(time.Minute)),
		ExecutionClaimId: uuid.NewString(),
	}
}

func nodeAgentPeerContext(t *testing.T, spiffeID string) context.Context {
	t.Helper()
	identity, err := url.Parse(spiffeID)
	if err != nil {
		t.Fatalf("parse test SPIFFE ID: %v", err)
	}
	certificate := &x509.Certificate{Raw: []byte(spiffeID), URIs: []*url.URL{identity}}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{
		State: tls.ConnectionState{
			HandshakeComplete: true, PeerCertificates: []*x509.Certificate{certificate},
			VerifiedChains: [][]*x509.Certificate{{certificate}},
		},
	}})
}

var _ velav1.NodeAgentServiceServer = (*Server)(nil)
