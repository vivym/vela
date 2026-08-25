package workertransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/workercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Client struct {
	connection *grpc.ClientConn
	service    velav1.WorkerControlServiceClient
}

func DialClient(
	ctx context.Context,
	address string,
	transportCredentials credentials.TransportCredentials,
) (*Client, error) {
	if ctx == nil || transportCredentials == nil {
		return nil, errors.New("worker control client context and transport credentials are required")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return nil, errors.New("worker control address must contain a host and port")
	}
	connection, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(4<<20),
			grpc.MaxCallSendMsgSize(1<<20),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("configure Worker control client: %w", err)
	}
	connection.Connect()
	for state := connection.GetState(); state != connectivity.Ready; state = connection.GetState() {
		if state == connectivity.Shutdown {
			_ = connection.Close()
			return nil, errors.New("worker control connection shut down during startup")
		}
		if !connection.WaitForStateChange(ctx, state) {
			_ = connection.Close()
			return nil, fmt.Errorf("connect to Worker control: %w", ctx.Err())
		}
	}
	return &Client{
		connection: connection,
		service:    velav1.NewWorkerControlServiceClient(connection),
	}, nil
}

func (client *Client) Close() error {
	if client == nil || client.connection == nil {
		return nil
	}
	return client.connection.Close()
}

func (client *Client) Acquire(ctx context.Context, workerEpoch int64) (workercontrol.Assignment, error) {
	if workerEpoch <= 0 {
		return workercontrol.Assignment{}, errors.New("positive Worker epoch is required")
	}
	response, err := client.exchange(ctx, &velav1.ConnectRequest{
		Operation: &velav1.ConnectRequest_Acquire{
			Acquire: &velav1.AcquireRequest{WorkerEpoch: workerEpoch},
		},
	})
	if err != nil {
		return workercontrol.Assignment{}, err
	}
	return parseAssignment(response.GetAssignment())
}

func (client *Client) Start(
	ctx context.Context,
	credentials workercontrol.LeaseCredentials,
) (workercontrol.StartResult, error) {
	lease, err := protoLeaseCredentials(credentials)
	if err != nil {
		return workercontrol.StartResult{}, err
	}
	response, err := client.exchange(ctx, &velav1.ConnectRequest{
		Operation: &velav1.ConnectRequest_Start{
			Start: &velav1.StartWorkerRequest{Lease: lease},
		},
	})
	if err != nil {
		return workercontrol.StartResult{}, err
	}
	return parseStartResult(response.GetStartResult())
}

func (client *Client) Heartbeat(
	ctx context.Context,
	credentials workercontrol.LeaseCredentials,
	observation workercontrol.HeartbeatObservation,
) (workercontrol.HeartbeatResult, error) {
	lease, err := protoLeaseCredentials(credentials)
	if err != nil {
		return workercontrol.HeartbeatResult{}, err
	}
	request := &velav1.HeartbeatRequest{
		Lease: lease, Sequence: observation.Sequence, BackendStage: observation.BackendStage,
		GpuHealthJson:          append([]byte(nil), observation.GPUHealthSummary...),
		LocalArtifactStateJson: append([]byte(nil), observation.LocalArtifactState...),
		ScratchFreeBytes:       observation.ScratchFreeBytes,
		ArtifactStoreReachable: observation.ArtifactStoreReachable,
	}
	if observation.BackendStageProgress != nil {
		value := *observation.BackendStageProgress
		request.BackendStageProgress = &value
	}
	if observation.EstimatedRemainingSeconds != nil {
		value := *observation.EstimatedRemainingSeconds
		request.EstimatedRemainingSeconds = &value
	}
	response, err := client.exchange(ctx, &velav1.ConnectRequest{
		Operation: &velav1.ConnectRequest_Heartbeat{Heartbeat: request},
	})
	if err != nil {
		return workercontrol.HeartbeatResult{}, err
	}
	return parseHeartbeatResult(response.GetHeartbeatResult())
}

func (client *Client) Fail(
	ctx context.Context,
	credentials workercontrol.LeaseCredentials,
	observation workercontrol.FailureObservation,
) (workercontrol.RetryDecision, error) {
	lease, err := protoLeaseCredentials(credentials)
	if err != nil {
		return workercontrol.RetryDecision{}, err
	}
	response, err := client.exchange(ctx, &velav1.ConnectRequest{
		Operation: &velav1.ConnectRequest_Fail{Fail: &velav1.FailRequest{
			Lease: lease,
			Observation: &velav1.WorkerFailureObservation{
				FailureClass:       observation.FailureClass,
				FailureFingerprint: observation.FailureFingerprint,
				ErrorSummary:       observation.ErrorSummary, BackendStage: observation.BackendStage,
				GpuUuids:                 append([]string(nil), observation.GPUUUIDs...),
				InferenceBackendRevision: observation.InferenceBackendRevision,
				RetryRecommended:         observation.RetryRecommended,
				WorkerReusable:           observation.WorkerReusable,
			},
		}},
	})
	if err != nil {
		return workercontrol.RetryDecision{}, err
	}
	return parseRetryDecision(response.GetRetryDecision())
}

func (client *Client) exchange(
	ctx context.Context,
	request *velav1.ConnectRequest,
) (*velav1.ConnectResponse, error) {
	if client == nil || client.service == nil || request == nil || ctx == nil {
		return nil, errors.New("worker control client is not configured")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	requestID := uuid.NewString()
	request.RequestId = requestID
	stream, err := client.service.Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("open Worker control operation stream: %w", err)
	}
	if err := stream.Send(request); err != nil {
		return nil, fmt.Errorf("send Worker control request: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("close Worker control request stream: %w", err)
	}
	response, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("receive Worker control response: %w", err)
	}
	if response == nil || response.GetRequestId() != requestID {
		return nil, errors.New("worker control response request identity does not match")
	}
	if operationError := response.GetOperationError(); operationError != nil {
		if operationError.GetCode() == "" || operationError.GetMessage() == "" {
			return nil, errors.New("worker control operation error is malformed")
		}
		return nil, &workercontrol.Failure{
			Code:    workercontrol.FailureCode(operationError.GetCode()),
			Message: operationError.GetMessage(),
		}
	}
	return response, nil
}

func protoLeaseCredentials(
	credentials workercontrol.LeaseCredentials,
) (*velav1.WorkerLeaseCredentials, error) {
	if credentials.AttemptID == uuid.Nil || credentials.WorkerEpoch <= 0 ||
		credentials.Fence <= 0 || credentials.Token == "" {
		return nil, errors.New("worker Lease credentials are invalid")
	}
	return &velav1.WorkerLeaseCredentials{
		AttemptId: credentials.AttemptID.String(), WorkerEpoch: credentials.WorkerEpoch,
		Fence: credentials.Fence, Token: credentials.Token,
	}, nil
}

func parseAssignment(message *velav1.WorkerAssignment) (workercontrol.Assignment, error) {
	if message == nil {
		return workercontrol.Assignment{}, errors.New("worker control response omitted Assignment")
	}
	attemptID, err := requiredUUID(message.GetAttemptId())
	if err != nil {
		return workercontrol.Assignment{}, err
	}
	jobID, err := requiredUUID(message.GetJobId())
	if err != nil {
		return workercontrol.Assignment{}, err
	}
	workerID, err := requiredUUID(message.GetWorkerId())
	if err != nil {
		return workercontrol.Assignment{}, err
	}
	modelID, err := requiredUUID(message.GetModelRevisionId())
	if err != nil {
		return workercontrol.Assignment{}, err
	}
	presetID, err := requiredUUID(message.GetGenerationPresetRevisionId())
	if err != nil {
		return workercontrol.Assignment{}, err
	}
	profileID, err := requiredUUID(message.GetExecutionProfileRevisionId())
	if err != nil {
		return workercontrol.Assignment{}, err
	}
	outputSpecID, err := requiredUUID(message.GetOutputSpecId())
	if err != nil {
		return workercontrol.Assignment{}, err
	}
	expiresAt, err := requiredTimestamp(message.GetLeaseExpiresAt())
	if err != nil {
		return workercontrol.Assignment{}, err
	}
	leaseValidFor := message.GetLeaseValidFor()
	if leaseValidFor == nil || leaseValidFor.CheckValid() != nil {
		return workercontrol.Assignment{}, errors.New("worker control Assignment Lease duration is invalid")
	}
	validFor := leaseValidFor.AsDuration()
	if validFor <= 0 ||
		message.GetWorkerEpoch() <= 0 || message.GetAttemptNumber() <= 0 ||
		message.GetLeaseFence() <= 0 || message.GetLeaseToken() == "" {
		return workercontrol.Assignment{}, errors.New("worker control Assignment authority is invalid")
	}
	requestContent, err := canonicalRequestContent(message.GetRequestContentJson())
	if err != nil {
		return workercontrol.Assignment{}, err
	}
	return workercontrol.Assignment{
		AttemptID: attemptID, JobID: jobID, WorkerID: workerID,
		WorkerEpoch: message.GetWorkerEpoch(), ModelRevisionID: modelID,
		GenerationPresetRevisionID: presetID, ExecutionProfileRevisionID: profileID,
		OutputSpecID: outputSpecID, RequestContent: requestContent,
		AttemptNumber: message.GetAttemptNumber(), LeaseToken: message.GetLeaseToken(),
		LeaseFence: message.GetLeaseFence(), LeaseExpiresAt: expiresAt, LeaseValidFor: validFor,
	}, nil
}

func parseStartResult(message *velav1.StartWorkerResult) (workercontrol.StartResult, error) {
	if message == nil {
		return workercontrol.StartResult{}, errors.New("worker control response omitted Start result")
	}
	decision := workercontrol.StartDecision(message.GetDecision())
	if decision == workercontrol.Stop {
		stopReason, err := parseStartStopReason(message.GetStopReason())
		if err != nil {
			return workercontrol.StartResult{}, err
		}
		return workercontrol.StartResult{
			Decision: decision, StopReason: stopReason,
		}, nil
	}
	if decision != workercontrol.StartGranted {
		return workercontrol.StartResult{}, errors.New("worker control Start decision is invalid")
	}
	identity, err := parseResultIdentity(
		message.GetAttemptId(), message.GetJobId(), message.GetWorkerId(),
		message.GetWorkerEpoch(), message.GetLeaseFence(),
	)
	if err != nil {
		return workercontrol.StartResult{}, err
	}
	startedAt, err := requiredTimestamp(message.GetStartedAt())
	if err != nil {
		return workercontrol.StartResult{}, err
	}
	return workercontrol.StartResult{
		Decision: decision, AttemptID: identity.attemptID, JobID: identity.jobID,
		WorkerID: identity.workerID, WorkerEpoch: identity.workerEpoch,
		LeaseFence: identity.fence, StartedAt: startedAt,
	}, nil
}

func parseStartStopReason(value string) (workercontrol.StopReason, error) {
	reason := workercontrol.StopReason(value)
	switch reason {
	case workercontrol.StopInvalidAuthority,
		workercontrol.StopLeaseExpired,
		workercontrol.StopJobExpired,
		workercontrol.StopNotStartable:
		return reason, nil
	default:
		return "", errors.New("worker control Start STOP reason is invalid")
	}
}

func parseHeartbeatResult(message *velav1.HeartbeatResult) (workercontrol.HeartbeatResult, error) {
	if message == nil {
		return workercontrol.HeartbeatResult{}, errors.New("worker control response omitted Heartbeat result")
	}
	decision := workercontrol.HeartbeatDecision(message.GetDecision())
	if decision == workercontrol.HeartbeatStop {
		stopReason, err := parseHeartbeatStopReason(message.GetStopReason())
		if err != nil {
			return workercontrol.HeartbeatResult{}, err
		}
		return workercontrol.HeartbeatResult{
			Decision: decision, StopReason: stopReason,
		}, nil
	}
	if decision != workercontrol.HeartbeatContinue {
		return workercontrol.HeartbeatResult{}, errors.New("worker control Heartbeat decision is invalid")
	}
	identity, err := parseResultIdentity(
		message.GetAttemptId(), message.GetJobId(), message.GetWorkerId(),
		message.GetWorkerEpoch(), message.GetLeaseFence(),
	)
	if err != nil {
		return workercontrol.HeartbeatResult{}, err
	}
	progressAt, err := requiredTimestamp(message.GetProgressUpdatedAt())
	if err != nil {
		return workercontrol.HeartbeatResult{}, err
	}
	expiresAt, err := requiredTimestamp(message.GetLeaseExpiresAt())
	if err != nil {
		return workercontrol.HeartbeatResult{}, err
	}
	leaseValidFor := message.GetLeaseValidFor()
	if leaseValidFor == nil || leaseValidFor.CheckValid() != nil {
		return workercontrol.HeartbeatResult{}, errors.New("worker control Heartbeat Lease duration is invalid")
	}
	validFor := leaseValidFor.AsDuration()
	if validFor <= 0 || message.GetHeartbeatSequence() <= 0 || message.GetExecutionPhase() == "" {
		return workercontrol.HeartbeatResult{}, errors.New("worker control Heartbeat continuation is invalid")
	}
	return workercontrol.HeartbeatResult{
		Decision: decision, AttemptID: identity.attemptID, JobID: identity.jobID,
		WorkerID: identity.workerID, WorkerEpoch: identity.workerEpoch, LeaseFence: identity.fence,
		HeartbeatSequence: message.GetHeartbeatSequence(),
		ExecutionPhase:    workercontrol.ExecutionPhase(message.GetExecutionPhase()),
		ProgressUpdatedAt: progressAt, LeaseExpiresAt: expiresAt, LeaseValidFor: validFor,
	}, nil
}

func parseHeartbeatStopReason(value string) (workercontrol.StopReason, error) {
	reason := workercontrol.StopReason(value)
	switch reason {
	case workercontrol.StopInvalidAuthority,
		workercontrol.StopLeaseExpired,
		workercontrol.StopJobExpired,
		workercontrol.StopNotHeartbeatable,
		workercontrol.StopStaleHeartbeat,
		workercontrol.StopInvalidProgress,
		workercontrol.StopProtocolMigration:
		return reason, nil
	default:
		return "", errors.New("worker control Heartbeat STOP reason is invalid")
	}
}

func parseRetryDecision(message *velav1.RetryDecision) (workercontrol.RetryDecision, error) {
	if message == nil {
		return workercontrol.RetryDecision{}, errors.New("worker control response omitted Retry decision")
	}
	disposition := workercontrol.RetryDisposition(message.GetDisposition())
	if disposition == workercontrol.RetryDispositionRejectedStaleLease {
		return workercontrol.RetryDecision{Disposition: disposition}, nil
	}
	if disposition != workercontrol.RetryDispositionRetryWait && disposition != workercontrol.RetryDispositionFailed {
		return workercontrol.RetryDecision{}, errors.New("worker control Retry disposition is invalid")
	}
	attemptID, err := requiredUUID(message.GetAttemptId())
	if err != nil {
		return workercontrol.RetryDecision{}, err
	}
	jobID, err := requiredUUID(message.GetJobId())
	if err != nil {
		return workercontrol.RetryDecision{}, err
	}
	decidedAt, err := requiredTimestamp(message.GetDecidedAt())
	if err != nil {
		return workercontrol.RetryDecision{}, err
	}
	var nextRetryAt *time.Time
	if message.GetNextRetryAt() != nil {
		value, err := requiredTimestamp(message.GetNextRetryAt())
		if err != nil {
			return workercontrol.RetryDecision{}, err
		}
		nextRetryAt = &value
	}
	if message.GetFailureClass() == "" || message.GetAttemptState() == "" ||
		message.GetJobFence() <= 0 || message.GetJobVersion() <= 0 {
		return workercontrol.RetryDecision{}, errors.New("worker control Retry decision authority is invalid")
	}
	if workercontrol.AttemptTerminalState(message.GetAttemptState()) != workercontrol.FailedAttempt ||
		message.GetAttemptComputeSeconds() < 0 || message.GetTotalComputeSeconds() < message.GetAttemptComputeSeconds() ||
		message.GetAttemptFinalizationSeconds() < 0 ||
		message.GetTotalFinalizationSeconds() < message.GetAttemptFinalizationSeconds() {
		return workercontrol.RetryDecision{}, errors.New("worker control Retry decision accounting is invalid")
	}
	if disposition == workercontrol.RetryDispositionRetryWait {
		if nextRetryAt == nil || !nextRetryAt.After(decidedAt) {
			return workercontrol.RetryDecision{}, errors.New("worker control RETRY_WAIT decision omitted a future retry time")
		}
	} else if nextRetryAt != nil {
		return workercontrol.RetryDecision{}, errors.New("worker control FAILED decision included a retry time")
	}
	return workercontrol.RetryDecision{
		Disposition: disposition, FailureClass: message.GetFailureClass(),
		AttemptID: attemptID, JobID: jobID,
		AttemptState:               workercontrol.AttemptTerminalState(message.GetAttemptState()),
		AttemptComputeSeconds:      message.GetAttemptComputeSeconds(),
		TotalComputeSeconds:        message.GetTotalComputeSeconds(),
		AttemptFinalizationSeconds: message.GetAttemptFinalizationSeconds(),
		TotalFinalizationSeconds:   message.GetTotalFinalizationSeconds(),
		NextRetryAt:                nextRetryAt, JobFence: message.GetJobFence(), JobVersion: message.GetJobVersion(),
		DecidedAt: decidedAt,
	}, nil
}

type resultIdentity struct {
	attemptID, jobID, workerID uuid.UUID
	workerEpoch, fence         int64
}

func parseResultIdentity(
	attemptRaw, jobRaw, workerRaw string,
	workerEpoch, fence int64,
) (resultIdentity, error) {
	attemptID, err := requiredUUID(attemptRaw)
	if err != nil {
		return resultIdentity{}, err
	}
	jobID, err := requiredUUID(jobRaw)
	if err != nil {
		return resultIdentity{}, err
	}
	workerID, err := requiredUUID(workerRaw)
	if err != nil {
		return resultIdentity{}, err
	}
	if workerEpoch <= 0 || fence <= 0 {
		return resultIdentity{}, errors.New("worker control result epoch or fence is invalid")
	}
	return resultIdentity{
		attemptID: attemptID, jobID: jobID, workerID: workerID,
		workerEpoch: workerEpoch, fence: fence,
	}, nil
}

func requiredUUID(raw string) (uuid.UUID, error) {
	parsed, err := uuid.Parse(raw)
	if err != nil || parsed == uuid.Nil {
		return uuid.Nil, errors.New("worker control result contains an invalid UUID")
	}
	return parsed, nil
}

func requiredTimestamp(value *timestamppb.Timestamp) (time.Time, error) {
	if value == nil || value.CheckValid() != nil {
		return time.Time{}, errors.New("worker control result contains an invalid timestamp")
	}
	return value.AsTime(), nil
}

func canonicalRequestContent(raw []byte) (string, error) {
	if len(raw) == 0 || len(raw) > 64*1024 {
		return "", errors.New("worker control Assignment request content is absent or too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil || object == nil {
		return "", errors.New("worker control Assignment request content is not one JSON object")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return "", errors.New("worker control Assignment request content is not one JSON object")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return "", fmt.Errorf("canonicalize Worker Assignment request content: %w", err)
	}
	return string(canonical), nil
}
