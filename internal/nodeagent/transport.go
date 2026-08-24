package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/remediation"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxIdentityText     = 500
	maxDetailText       = 2000
	maxDeadline         = 24 * time.Hour
	receiptWriteTimeout = 5 * time.Second
)

type NodeAgentIdentity struct {
	NodeIdentity string
	WorkerID     uuid.UUID
}

type IdentityResolver interface {
	ResolveNodeAgent(context.Context, string) (NodeAgentIdentity, error)
}

type Executor interface {
	Execute(context.Context, remediation.Plan) (remediation.ExecutionResult, error)
}

// Authorizer binds the transport request to the control-plane operation state.
// Implementations must check the current Worker epoch, Lease fencing, action
// certification, and any device/capability policy before host execution.
type Authorizer interface {
	Authorize(context.Context, Request) error
}

type Receipt struct {
	RequestHash [sha256.Size]byte
	Result      Result
}

type Ledger interface {
	Load(context.Context, uuid.UUID) (Receipt, bool, error)
	Save(context.Context, Receipt) error
}

type Request struct {
	OperationID           uuid.UUID
	WorkerID              uuid.UUID
	WorkerEpoch           int64
	NodeIdentity          string
	DeviceIdentity        string
	ActionLevel           remediation.ActionLevel
	CertificationRevision string
	FailureEvidenceDigest []byte
	DeadlineAt            time.Time
}

type Result struct {
	OperationID   uuid.UUID
	Success       bool
	ResultCode    string
	ResultDetail  string
	PostcheckHash []byte
	StartedAt     time.Time
	FinishedAt    time.Time
}

type Server struct {
	velav1.UnimplementedNodeAgentServiceServer
	resolver   IdentityResolver
	authorizer Authorizer
	executor   Executor
	ledger     Ledger
	clock      func() time.Time
	mu         sync.Mutex
}

func NewServer(resolver IdentityResolver, authorizer Authorizer, executor Executor, ledger Ledger) (*Server, error) {
	if resolver == nil {
		return nil, errors.New("node Agent identity resolver is required")
	}
	if executor == nil {
		return nil, errors.New("node Agent executor is required")
	}
	if authorizer == nil {
		return nil, errors.New("node Agent control-plane authorizer is required")
	}
	if ledger == nil {
		return nil, errors.New("node Agent receipt ledger is required")
	}
	return &Server{resolver: resolver, authorizer: authorizer, executor: executor, ledger: ledger, clock: time.Now}, nil
}

func (server *Server) ExecuteRemediation(
	ctx context.Context,
	request *velav1.ExecuteRemediationRequest,
) (*velav1.ExecuteRemediationResponse, error) {
	if server == nil || server.resolver == nil || server.authorizer == nil || server.executor == nil || server.ledger == nil {
		return nil, status.Error(codes.FailedPrecondition, "node Agent server is not configured")
	}
	if ctx == nil {
		return nil, status.Error(codes.InvalidArgument, "node Agent context is required")
	}
	identity, err := authenticatedNodeAgent(ctx, server.resolver)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "verified node Agent mTLS identity is required")
	}
	parsed, deadline, err := parseRequest(request, server.clock().UTC())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if identity.NodeIdentity != parsed.NodeIdentity || identity.WorkerID != parsed.WorkerID {
		return nil, status.Error(codes.PermissionDenied, "node Agent identity does not match remediation target")
	}
	if parsed.ActionLevel == remediation.ActionL6BMCPowerCycle ||
		parsed.ActionLevel == remediation.ActionL7Quarantine {
		return nil, status.Error(codes.PermissionDenied, "node Agent cannot directly execute privileged remediation")
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	requestHash := hashRequest(parsed)
	prior, found, err := server.ledger.Load(ctx, parsed.OperationID)
	if err != nil {
		return nil, status.Error(codes.Internal, "node Agent receipt lookup failed")
	}
	if found {
		if prior.RequestHash != requestHash {
			return nil, status.Error(codes.AlreadyExists, "node Agent operation id was reused with different input")
		}
		return responseFromResult(prior.Result), nil
	}
	if err := server.authorizer.Authorize(ctx, parsed); err != nil {
		if status.Code(err) != codes.Unknown {
			return nil, err
		}
		return nil, status.Error(codes.FailedPrecondition, "node Agent remediation operation is not authorized")
	}
	startedAt := server.clock().UTC()
	executionContext, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	executionResult, executionErr := server.executor.Execute(executionContext, remediation.Plan{
		OperationID:           parsed.OperationID,
		WorkerID:              parsed.WorkerID,
		ActionLevel:           parsed.ActionLevel,
		NodeIdentity:          parsed.NodeIdentity,
		DeviceIdentity:        parsed.DeviceIdentity,
		WorkerEpoch:           parsed.WorkerEpoch,
		CertificationRevision: parsed.CertificationRevision,
		FailureEvidenceDigest: append([]byte(nil), parsed.FailureEvidenceDigest...),
	})
	finishedAt := server.clock().UTC()
	if finishedAt.After(deadline) || errors.Is(executionErr, context.DeadlineExceeded) {
		result := Result{
			OperationID: parsed.OperationID, Success: false,
			ResultCode: "DEADLINE_EXCEEDED", ResultDetail: "node Agent remediation exceeded its deadline",
			StartedAt: startedAt, FinishedAt: finishedAt,
		}
		if saveErr := saveReceipt(server.ledger, ctx, Receipt{RequestHash: requestHash, Result: result}); saveErr != nil {
			return nil, status.Error(codes.Internal, "node Agent receipt write failed")
		}
		return responseFromResult(result), nil
	}
	if executionErr != nil {
		result := Result{
			OperationID: parsed.OperationID, Success: false,
			ResultCode: "EXECUTION_FAILED", ResultDetail: boundedDetail(executionErr.Error()),
			StartedAt: startedAt, FinishedAt: finishedAt,
		}
		if saveErr := saveReceipt(server.ledger, ctx, Receipt{RequestHash: requestHash, Result: result}); saveErr != nil {
			return nil, status.Error(codes.Internal, "node Agent receipt write failed")
		}
		return responseFromResult(result), nil
	}
	if !executionResult.PostcheckVerified || executionResult.ResultCode == "" || executionResult.PostcheckDigest == [sha256.Size]byte{} {
		result := Result{
			OperationID: parsed.OperationID, Success: false,
			ResultCode: "INVALID_POSTCHECK", ResultDetail: "executor returned no certified post-check",
			StartedAt: startedAt, FinishedAt: finishedAt,
		}
		if saveErr := saveReceipt(server.ledger, ctx, Receipt{RequestHash: requestHash, Result: result}); saveErr != nil {
			return nil, status.Error(codes.Internal, "node Agent receipt write failed")
		}
		return responseFromResult(result), nil
	}
	result := Result{
		OperationID: parsed.OperationID, Success: true,
		ResultCode: executionResult.ResultCode, ResultDetail: boundedDetail(executionResult.Detail),
		PostcheckHash: append([]byte(nil), executionResult.PostcheckDigest[:]...),
		StartedAt:     startedAt, FinishedAt: finishedAt,
	}
	if saveErr := saveReceipt(server.ledger, ctx, Receipt{RequestHash: requestHash, Result: result}); saveErr != nil {
		return nil, status.Error(codes.Internal, "node Agent receipt write failed")
	}
	return responseFromResult(result), nil
}

type Client struct {
	client   velav1.NodeAgentServiceClient
	identity NodeAgentIdentity
}

func NewClient(client velav1.NodeAgentServiceClient, identity NodeAgentIdentity) (*Client, error) {
	if client == nil {
		return nil, errors.New("node Agent gRPC client is required")
	}
	if !validIdentity(identity) {
		return nil, errors.New("node Agent identity is invalid")
	}
	return &Client{client: client, identity: identity}, nil
}

func (client *Client) Execute(ctx context.Context, request Request) (Result, error) {
	if client == nil || client.client == nil {
		return Result{}, errors.New("node Agent client is not configured")
	}
	if ctx == nil {
		return Result{}, errors.New("node Agent execution context is required")
	}
	if err := validateClientRequest(request); err != nil {
		return Result{}, err
	}
	if request.NodeIdentity != client.identity.NodeIdentity || request.WorkerID != client.identity.WorkerID {
		return Result{}, errors.New("node Agent client identity does not match request")
	}
	response, err := client.client.ExecuteRemediation(ctx, &velav1.ExecuteRemediationRequest{
		OperationId: request.OperationID.String(), WorkerId: request.WorkerID.String(),
		WorkerEpoch: request.WorkerEpoch, NodeIdentity: request.NodeIdentity,
		DeviceIdentity: request.DeviceIdentity, ActionLevel: string(request.ActionLevel),
		CertificationRevision: request.CertificationRevision,
		FailureEvidenceDigest: append([]byte(nil), request.FailureEvidenceDigest...),
		DeadlineAt:            timestamppb.New(request.DeadlineAt),
	})
	if err != nil {
		return Result{}, fmt.Errorf("execute remediation through node Agent: %w", err)
	}
	return parseResponse(response, request.OperationID)
}

func parseRequest(
	request *velav1.ExecuteRemediationRequest,
	now time.Time,
) (Request, time.Time, error) {
	if request == nil {
		return Request{}, time.Time{}, errors.New("node Agent remediation request is required")
	}
	operationID, err := uuid.Parse(request.GetOperationId())
	if err != nil || operationID == uuid.Nil {
		return Request{}, time.Time{}, errors.New("node Agent operation id is invalid")
	}
	workerID, err := uuid.Parse(request.GetWorkerId())
	if err != nil || workerID == uuid.Nil {
		return Request{}, time.Time{}, errors.New("node Agent Worker id is invalid")
	}
	deadline, err := parseDeadline(request.GetDeadlineAt(), now)
	if err != nil {
		return Request{}, time.Time{}, err
	}
	parsed := Request{
		OperationID: operationID, WorkerID: workerID, WorkerEpoch: request.GetWorkerEpoch(),
		NodeIdentity: request.GetNodeIdentity(), DeviceIdentity: request.GetDeviceIdentity(),
		ActionLevel:           remediation.ActionLevel(request.GetActionLevel()),
		CertificationRevision: request.GetCertificationRevision(),
		FailureEvidenceDigest: append([]byte(nil), request.GetFailureEvidenceDigest()...),
		DeadlineAt:            deadline,
	}
	if err := validateRequest(parsed); err != nil {
		return Request{}, time.Time{}, err
	}
	return parsed, deadline, nil
}

func parseResponse(response *velav1.ExecuteRemediationResponse, operationID uuid.UUID) (Result, error) {
	if response == nil {
		return Result{}, errors.New("node Agent returned no remediation response")
	}
	returnedID, err := uuid.Parse(response.GetOperationId())
	if err != nil || returnedID != operationID || !validText(response.GetResultCode(), 200) ||
		!validText(response.GetResultDetail(), maxDetailText) {
		return Result{}, errors.New("node Agent remediation response is invalid")
	}
	startedAt, err := validTimestamp(response.GetStartedAt())
	if err != nil {
		return Result{}, err
	}
	finishedAt, err := validTimestamp(response.GetFinishedAt())
	if err != nil || finishedAt.Before(startedAt) {
		return Result{}, errors.New("node Agent remediation response timestamps are invalid")
	}
	postcheck := append([]byte(nil), response.GetPostcheckSha256()...)
	if response.GetSuccess() && len(postcheck) != sha256.Size {
		return Result{}, errors.New("successful node Agent remediation lacks post-check digest")
	}
	if len(postcheck) != 0 && len(postcheck) != sha256.Size {
		return Result{}, errors.New("node Agent post-check digest is invalid")
	}
	return Result{
		OperationID: operationID, Success: response.GetSuccess(), ResultCode: response.GetResultCode(),
		ResultDetail: response.GetResultDetail(), PostcheckHash: postcheck,
		StartedAt: startedAt, FinishedAt: finishedAt,
	}, nil
}

func validateRequest(request Request) error {
	if request.OperationID == uuid.Nil || request.WorkerID == uuid.Nil || request.WorkerEpoch <= 0 ||
		!validText(request.NodeIdentity, maxIdentityText) || !validText(request.DeviceIdentity, maxIdentityText) ||
		!remediation.IsActionLevel(request.ActionLevel) ||
		(request.ActionLevel != remediation.ActionL7Quarantine && !validText(request.CertificationRevision, 200)) ||
		len(request.FailureEvidenceDigest) != sha256.Size ||
		request.DeadlineAt.IsZero() {
		return errors.New("node Agent remediation request is invalid")
	}
	return nil
}

func validateClientRequest(request Request) error {
	if err := validateRequest(request); err != nil {
		return err
	}
	if request.ActionLevel == remediation.ActionL6BMCPowerCycle ||
		request.ActionLevel == remediation.ActionL7Quarantine {
		return errors.New("node Agent client cannot directly execute privileged remediation")
	}
	return nil
}

func hashRequest(request Request) [sha256.Size]byte {
	canonical := struct {
		OperationID           string `json:"operation_id"`
		WorkerID              string `json:"worker_id"`
		WorkerEpoch           int64  `json:"worker_epoch"`
		NodeIdentity          string `json:"node_identity"`
		DeviceIdentity        string `json:"device_identity"`
		ActionLevel           string `json:"action_level"`
		CertificationRevision string `json:"certification_revision"`
		FailureEvidenceDigest []byte `json:"failure_evidence_digest"`
		DeadlineUnixNano      int64  `json:"deadline_unix_nano"`
	}{
		OperationID:           request.OperationID.String(),
		WorkerID:              request.WorkerID.String(),
		WorkerEpoch:           request.WorkerEpoch,
		NodeIdentity:          request.NodeIdentity,
		DeviceIdentity:        request.DeviceIdentity,
		ActionLevel:           string(request.ActionLevel),
		CertificationRevision: request.CertificationRevision,
		FailureEvidenceDigest: append([]byte(nil), request.FailureEvidenceDigest...),
		DeadlineUnixNano:      request.DeadlineAt.UTC().UnixNano(),
	}
	payload, err := json.Marshal(canonical)
	if err != nil {
		panic(fmt.Sprintf("marshal node Agent request hash: %v", err))
	}
	return sha256.Sum256(payload)
}

func responseFromResult(result Result) *velav1.ExecuteRemediationResponse {
	return &velav1.ExecuteRemediationResponse{
		OperationId:     result.OperationID.String(),
		Success:         result.Success,
		ResultCode:      result.ResultCode,
		ResultDetail:    result.ResultDetail,
		PostcheckSha256: append([]byte(nil), result.PostcheckHash...),
		StartedAt:       timestamppb.New(result.StartedAt),
		FinishedAt:      timestamppb.New(result.FinishedAt),
	}
}

func parseDeadline(timestamp *timestamppb.Timestamp, now time.Time) (time.Time, error) {
	if timestamp == nil || !timestamp.IsValid() {
		return time.Time{}, errors.New("node Agent remediation deadline is invalid")
	}
	deadline := timestamp.AsTime().UTC()
	if !deadline.After(now) || deadline.After(now.Add(maxDeadline)) {
		return time.Time{}, errors.New("node Agent remediation deadline is outside the allowed window")
	}
	return deadline, nil
}

func authenticatedNodeAgent(ctx context.Context, resolver IdentityResolver) (NodeAgentIdentity, error) {
	connectionPeer, ok := peer.FromContext(ctx)
	if !ok || connectionPeer.AuthInfo == nil {
		return NodeAgentIdentity{}, errors.New("mTLS node Agent peer is absent")
	}
	var tlsInfo credentials.TLSInfo
	switch typed := connectionPeer.AuthInfo.(type) {
	case credentials.TLSInfo:
		tlsInfo = typed
	case *credentials.TLSInfo:
		if typed == nil {
			return NodeAgentIdentity{}, errors.New("mTLS node Agent peer is absent")
		}
		tlsInfo = *typed
	default:
		return NodeAgentIdentity{}, errors.New("node Agent peer authentication is not TLS")
	}
	state := tlsInfo.State
	if !state.HandshakeComplete || len(state.PeerCertificates) == 0 || len(state.VerifiedChains) == 0 {
		return NodeAgentIdentity{}, errors.New("node Agent certificate chain is not verified")
	}
	leaf := state.PeerCertificates[0]
	verifiedLeaf := false
	for _, chain := range state.VerifiedChains {
		if len(chain) != 0 && bytes.Equal(chain[0].Raw, leaf.Raw) {
			verifiedLeaf = true
			break
		}
	}
	if !verifiedLeaf || len(leaf.URIs) != 1 || !validSPIFFEID(leaf.URIs[0]) {
		return NodeAgentIdentity{}, errors.New("node Agent certificate has no unique SPIFFE ID")
	}
	identity, err := resolver.ResolveNodeAgent(ctx, leaf.URIs[0].String())
	if err != nil || !validIdentity(identity) {
		return NodeAgentIdentity{}, errors.New("node Agent SPIFFE ID is not registered")
	}
	return identity, nil
}

func validIdentity(identity NodeAgentIdentity) bool {
	return identity.WorkerID != uuid.Nil && validText(identity.NodeIdentity, maxIdentityText)
}

func validSPIFFEID(identity *url.URL) bool {
	return identity != nil && identity.Scheme == "spiffe" && identity.Host != "" &&
		identity.Path != "" && identity.User == nil && identity.RawQuery == "" && identity.Fragment == ""
}

func validTimestamp(value *timestamppb.Timestamp) (time.Time, error) {
	if value == nil || !value.IsValid() {
		return time.Time{}, errors.New("node Agent timestamp is invalid")
	}
	return value.AsTime().UTC(), nil
}

func validText(value string, max int) bool {
	return value != "" && len(value) <= max && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, '\x00')
}

func boundedDetail(value string) string {
	if !validText(value, maxDetailText) {
		if value == "" {
			return "node Agent executor failed"
		}
		value = strings.TrimSpace(strings.ReplaceAll(value, "\x00", ""))
		if value == "" {
			return "node Agent executor failed"
		}
	}
	if len(value) > maxDetailText {
		return value[:maxDetailText]
	}
	return value
}

func saveReceipt(ledger Ledger, ctx context.Context, receipt Receipt) error {
	persistContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), receiptWriteTimeout)
	defer cancel()
	return ledger.Save(persistContext, receipt)
}

var _ velav1.NodeAgentServiceServer = (*Server)(nil)
