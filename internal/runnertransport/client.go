package runnertransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/executionprogress"
	"github.com/vivym/vela/internal/securefile"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	maxUnixSocketPathBytes = 100
	maxRequestContentBytes = 64 * 1024
	maxStatusJSONBytes     = 16 * 1024
	maxRunnerDetailRunes   = 1000
	maxRunnerOutputPath    = 4096
)

var (
	failureClassPattern       = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,99}$`)
	failureFingerprintPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$`)
	outputKindPattern         = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,31}$`)
	contentTypePattern        = regexp.MustCompile(`^[a-z0-9][a-z0-9.+-]*/[a-z0-9][a-z0-9.+-]*$`)
)

type Config struct {
	SocketPath  string
	ExpectedUID uint32
}

type AttemptIdentity struct {
	AttemptID   uuid.UUID
	JobID       uuid.UUID
	WorkerID    uuid.UUID
	WorkerEpoch int64
	LeaseFence  int64
}

type ExecutionSpec struct {
	ModelRevisionID            uuid.UUID
	GenerationPresetRevisionID uuid.UUID
	ExecutionProfileRevisionID uuid.UUID
	OutputSpecID               uuid.UUID
	RequestContent             json.RawMessage
}

type CommandDecision string

const (
	CommandAccepted CommandDecision = "ACCEPTED"
	CommandRejected CommandDecision = "REJECTED"
)

type PrepareResult struct {
	Decision          CommandDecision
	ResumedLocalState bool
	Detail            string
}

type CommandResult struct {
	Decision CommandDecision
	Detail   string
}

type CancelReason string

const (
	CancelControlPlaneStop CancelReason = "CONTROL_PLANE_STOP"
	CancelLeaseDeadline    CancelReason = "LEASE_DEADLINE"
	CancelAgentShutdown    CancelReason = "AGENT_SHUTDOWN"
)

type ExecutionState string

const (
	ExecutionPreparing ExecutionState = "PREPARING"
	ExecutionReady     ExecutionState = "READY"
	ExecutionRunning   ExecutionState = "RUNNING"
	ExecutionSucceeded ExecutionState = "SUCCEEDED"
	ExecutionFailed    ExecutionState = "FAILED"
	ExecutionCanceled  ExecutionState = "CANCELED"
)

type Failure struct {
	FailureClass             string
	FailureFingerprint       string
	ErrorSummary             string
	BackendStage             string
	GPUUUIDs                 []string
	InferenceBackendRevision string
	RetryRecommended         bool
	WorkerReusable           bool
}

type Status struct {
	State                     ExecutionState
	Sequence                  int64
	BackendStage              string
	BackendStageProgress      *float64
	EstimatedRemainingSeconds *int64
	GPUHealth                 json.RawMessage
	LocalArtifactState        json.RawMessage
	Failure                   *Failure
}

type Output struct {
	Kind        string
	Ordinal     int32
	Path        string
	SizeBytes   int64
	SHA256      [32]byte
	ContentType string
}

type CollectOutputsResult struct {
	Decision CommandDecision
	Outputs  []Output
	Detail   string
}

type Client struct {
	connection *grpc.ClientConn
	runner     velav1.RunnerServiceClient
}

func Dial(ctx context.Context, config Config) (*Client, error) {
	if ctx == nil {
		return nil, errors.New("runner dial context is required")
	}
	socketPath, err := validateSocketPath(config.SocketPath, config.ExpectedUID)
	if err != nil {
		return nil, err
	}
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		before, err := validateSocket(socketPath, config.ExpectedUID)
		if err != nil {
			return nil, err
		}
		connection, err := (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		if err != nil {
			return nil, err
		}
		after, err := validateSocket(socketPath, config.ExpectedUID)
		if err != nil || !os.SameFile(before, after) {
			_ = connection.Close()
			return nil, errors.New("runner socket changed while it was connected")
		}
		return connection, nil
	}
	connection, err := grpc.NewClient(
		"passthrough:///vela-runner",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(1<<20),
			grpc.MaxCallSendMsgSize(1<<20),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("configure runner gRPC client: %w", err)
	}
	connection.Connect()
	for state := connection.GetState(); state != connectivity.Ready; state = connection.GetState() {
		if state == connectivity.Shutdown {
			_ = connection.Close()
			return nil, errors.New("runner gRPC connection shut down during startup")
		}
		if !connection.WaitForStateChange(ctx, state) {
			_ = connection.Close()
			return nil, fmt.Errorf("connect to runner: %w", ctx.Err())
		}
	}
	return &Client{connection: connection, runner: velav1.NewRunnerServiceClient(connection)}, nil
}

func (client *Client) Close() error {
	if client == nil || client.connection == nil {
		return nil
	}
	return client.connection.Close()
}

func (client *Client) Prepare(
	ctx context.Context,
	identity AttemptIdentity,
	spec ExecutionSpec,
	sameAuthorityLocalRecovery bool,
) (PrepareResult, error) {
	if client == nil || client.runner == nil {
		return PrepareResult{}, errors.New("runner client is not configured")
	}
	if err := validateIdentity(identity); err != nil {
		return PrepareResult{}, err
	}
	if err := validateExecutionSpec(spec); err != nil {
		return PrepareResult{}, err
	}
	response, err := client.runner.Prepare(ctx, &velav1.PrepareRequest{
		Identity: protoIdentity(identity),
		ExecutionSpec: &velav1.RunnerExecutionSpec{
			ModelRevisionId:            spec.ModelRevisionID.String(),
			GenerationPresetRevisionId: spec.GenerationPresetRevisionID.String(),
			ExecutionProfileRevisionId: spec.ExecutionProfileRevisionID.String(),
			OutputSpecId:               spec.OutputSpecID.String(),
			RequestContentJson:         append([]byte(nil), spec.RequestContent...),
		},
		SameAuthorityLocalRecovery: sameAuthorityLocalRecovery,
	})
	if err != nil {
		return PrepareResult{}, err
	}
	if response == nil || !sameProtoIdentity(response.GetIdentity(), identity) {
		return PrepareResult{}, errors.New("runner Prepare response identity does not match its request")
	}
	decision, err := commandDecision(response.GetDecision())
	if err != nil {
		return PrepareResult{}, err
	}
	if !validDetail(response.GetDetail()) {
		return PrepareResult{}, errors.New("runner Prepare response detail is invalid")
	}
	return PrepareResult{
		Decision:          decision,
		ResumedLocalState: response.GetResumedLocalState(),
		Detail:            response.GetDetail(),
	}, nil
}

func (client *Client) Start(ctx context.Context, identity AttemptIdentity) (CommandResult, error) {
	if client == nil || client.runner == nil {
		return CommandResult{}, errors.New("runner client is not configured")
	}
	if err := validateIdentity(identity); err != nil {
		return CommandResult{}, err
	}
	response, err := client.runner.Start(ctx, &velav1.StartRequest{Identity: protoIdentity(identity)})
	if err != nil {
		return CommandResult{}, err
	}
	if response == nil || !sameProtoIdentity(response.GetIdentity(), identity) {
		return CommandResult{}, errors.New("runner Start response identity does not match its request")
	}
	decision, err := commandDecision(response.GetDecision())
	if err != nil {
		return CommandResult{}, err
	}
	if !validDetail(response.GetDetail()) {
		return CommandResult{}, errors.New("runner Start response detail is invalid")
	}
	return CommandResult{Decision: decision, Detail: response.GetDetail()}, nil
}

func (client *Client) Cancel(
	ctx context.Context,
	identity AttemptIdentity,
	reason CancelReason,
) (CommandResult, error) {
	if client == nil || client.runner == nil {
		return CommandResult{}, errors.New("runner client is not configured")
	}
	if err := validateIdentity(identity); err != nil {
		return CommandResult{}, err
	}
	protoReason, err := cancelReason(reason)
	if err != nil {
		return CommandResult{}, err
	}
	response, err := client.runner.Cancel(ctx, &velav1.CancelRequest{
		Identity: protoIdentity(identity),
		Reason:   protoReason,
	})
	if err != nil {
		return CommandResult{}, err
	}
	if response == nil || !sameProtoIdentity(response.GetIdentity(), identity) {
		return CommandResult{}, errors.New("runner Cancel response identity does not match its request")
	}
	decision, err := commandDecision(response.GetDecision())
	if err != nil {
		return CommandResult{}, err
	}
	if !validDetail(response.GetDetail()) {
		return CommandResult{}, errors.New("runner Cancel response detail is invalid")
	}
	return CommandResult{Decision: decision, Detail: response.GetDetail()}, nil
}

func (client *Client) Status(ctx context.Context, identity AttemptIdentity) (Status, error) {
	if client == nil || client.runner == nil {
		return Status{}, errors.New("runner client is not configured")
	}
	if err := validateIdentity(identity); err != nil {
		return Status{}, err
	}
	response, err := client.runner.Status(ctx, &velav1.StatusRequest{Identity: protoIdentity(identity)})
	if err != nil {
		return Status{}, err
	}
	if response == nil || !sameProtoIdentity(response.GetIdentity(), identity) {
		return Status{}, errors.New("runner Status response identity does not match its request")
	}
	state, err := executionState(response.GetState())
	if err != nil {
		return Status{}, err
	}
	if response.GetSequence() <= 0 || !validPrintableText(response.GetBackendStage(), 100, false) {
		return Status{}, errors.New("runner Status sequence or backend stage is invalid")
	}
	var progress *float64
	if response.BackendStageProgress != nil {
		value := response.GetBackendStageProgress()
		if !executionprogress.ValidStageProgress(value) {
			return Status{}, errors.New("runner Status progress is invalid")
		}
		progress = &value
	}
	var remaining *int64
	if response.EstimatedRemainingSeconds != nil {
		value := response.GetEstimatedRemainingSeconds()
		if !executionprogress.ValidEstimatedRemainingSeconds(value) {
			return Status{}, errors.New("runner Status estimated remaining time is invalid")
		}
		remaining = &value
	}
	gpuHealth, err := canonicalJSONObject(response.GetGpuHealthJson(), maxStatusJSONBytes)
	if err != nil {
		return Status{}, fmt.Errorf("validate runner GPU health: %w", err)
	}
	localState, err := canonicalJSONObject(response.GetLocalArtifactStateJson(), maxStatusJSONBytes)
	if err != nil {
		return Status{}, fmt.Errorf("validate runner local Artifact state: %w", err)
	}
	failure, err := parseFailure(state, response.GetFailure())
	if err != nil {
		return Status{}, err
	}
	return Status{
		State:                     state,
		Sequence:                  response.GetSequence(),
		BackendStage:              response.GetBackendStage(),
		BackendStageProgress:      progress,
		EstimatedRemainingSeconds: remaining,
		GPUHealth:                 gpuHealth,
		LocalArtifactState:        localState,
		Failure:                   failure,
	}, nil
}

func (client *Client) CollectOutputs(
	ctx context.Context,
	identity AttemptIdentity,
) (CollectOutputsResult, error) {
	if client == nil || client.runner == nil {
		return CollectOutputsResult{}, errors.New("runner client is not configured")
	}
	if err := validateIdentity(identity); err != nil {
		return CollectOutputsResult{}, err
	}
	response, err := client.runner.CollectOutputs(
		ctx,
		&velav1.CollectOutputsRequest{Identity: protoIdentity(identity)},
	)
	if err != nil {
		return CollectOutputsResult{}, err
	}
	if response == nil || !sameProtoIdentity(response.GetIdentity(), identity) {
		return CollectOutputsResult{}, errors.New(
			"runner CollectOutputs response identity does not match its request",
		)
	}
	decision, err := commandDecision(response.GetDecision())
	if err != nil {
		return CollectOutputsResult{}, err
	}
	if !validDetail(response.GetDetail()) {
		return CollectOutputsResult{}, errors.New("runner CollectOutputs response detail is invalid")
	}
	if decision == CommandRejected {
		if len(response.GetOutputs()) != 0 {
			return CollectOutputsResult{}, errors.New("rejected runner output collection returned outputs")
		}
		return CollectOutputsResult{Decision: decision, Detail: response.GetDetail()}, nil
	}
	if len(response.GetOutputs()) == 0 || len(response.GetOutputs()) > 32 {
		return CollectOutputsResult{}, errors.New("accepted runner output collection has an invalid count")
	}
	outputs := make([]Output, 0, len(response.GetOutputs()))
	identities := make(map[string]struct{}, len(response.GetOutputs()))
	paths := make(map[string]struct{}, len(response.GetOutputs()))
	for _, candidate := range response.GetOutputs() {
		output, err := parseOutput(candidate)
		if err != nil {
			return CollectOutputsResult{}, err
		}
		identityKey := fmt.Sprintf("%s/%d", output.Kind, output.Ordinal)
		if _, exists := identities[identityKey]; exists {
			return CollectOutputsResult{}, errors.New("runner outputs contain a duplicate kind and ordinal")
		}
		if _, exists := paths[output.Path]; exists {
			return CollectOutputsResult{}, errors.New("runner outputs contain a duplicate path")
		}
		identities[identityKey] = struct{}{}
		paths[output.Path] = struct{}{}
		outputs = append(outputs, output)
	}
	return CollectOutputsResult{
		Decision: decision,
		Outputs:  outputs,
		Detail:   response.GetDetail(),
	}, nil
}

func validateSocketPath(path string, expectedUID uint32) (string, error) {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned != path || strings.ContainsRune(path, '\x00') ||
		len([]byte(path)) > maxUnixSocketPathBytes {
		return "", errors.New("runner socket path or owner is invalid")
	}
	parent, err := securefile.ResolveTrustedDirectory(filepath.Dir(cleaned))
	if err != nil {
		return "", fmt.Errorf("validate runner socket directory: %w", err)
	}
	resolved := filepath.Join(parent, filepath.Base(cleaned))
	if _, err := validateSocket(resolved, expectedUID); err != nil {
		return "", err
	}
	return resolved, nil
}

func validateSocket(path string, expectedUID uint32) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect runner socket: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 || stat.Uid != expectedUID {
		return nil, errors.New("runner socket owner, type, or permissions are invalid")
	}
	return info, nil
}

func validateIdentity(identity AttemptIdentity) error {
	if identity.AttemptID == uuid.Nil || identity.JobID == uuid.Nil || identity.WorkerID == uuid.Nil ||
		identity.WorkerEpoch <= 0 || identity.LeaseFence <= 0 {
		return errors.New("runner Attempt authority is invalid")
	}
	return nil
}

func validateExecutionSpec(spec ExecutionSpec) error {
	if spec.ModelRevisionID == uuid.Nil || spec.GenerationPresetRevisionID == uuid.Nil ||
		spec.ExecutionProfileRevisionID == uuid.Nil || spec.OutputSpecID == uuid.Nil ||
		len(spec.RequestContent) == 0 || len(spec.RequestContent) > maxRequestContentBytes {
		return errors.New("runner execution specification is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(spec.RequestContent))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil || object == nil {
		return errors.New("runner request content must contain one JSON object")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("runner request content must contain one JSON object")
	}
	return nil
}

func canonicalJSONObject(raw []byte, maxBytes int) ([]byte, error) {
	if len(raw) == 0 || len(raw) > maxBytes {
		return nil, errors.New("JSON object is absent or too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil || object == nil {
		return nil, errors.New("value must contain one JSON object")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("value must contain one JSON object")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return nil, fmt.Errorf("canonicalize JSON object: %w", err)
	}
	return canonical, nil
}

func protoIdentity(identity AttemptIdentity) *velav1.RunnerAttemptIdentity {
	return &velav1.RunnerAttemptIdentity{
		AttemptId:   identity.AttemptID.String(),
		JobId:       identity.JobID.String(),
		WorkerId:    identity.WorkerID.String(),
		WorkerEpoch: identity.WorkerEpoch,
		LeaseFence:  identity.LeaseFence,
	}
}

func sameProtoIdentity(got *velav1.RunnerAttemptIdentity, want AttemptIdentity) bool {
	return got != nil && got.GetAttemptId() == want.AttemptID.String() &&
		got.GetJobId() == want.JobID.String() && got.GetWorkerId() == want.WorkerID.String() &&
		got.GetWorkerEpoch() == want.WorkerEpoch && got.GetLeaseFence() == want.LeaseFence
}

func commandDecision(decision velav1.RunnerCommandDecision) (CommandDecision, error) {
	switch decision {
	case velav1.RunnerCommandDecision_RUNNER_COMMAND_DECISION_ACCEPTED:
		return CommandAccepted, nil
	case velav1.RunnerCommandDecision_RUNNER_COMMAND_DECISION_REJECTED:
		return CommandRejected, nil
	default:
		return "", errors.New("runner response command decision is invalid")
	}
}

func cancelReason(reason CancelReason) (velav1.RunnerCancelReason, error) {
	switch reason {
	case CancelControlPlaneStop:
		return velav1.RunnerCancelReason_RUNNER_CANCEL_REASON_CONTROL_PLANE_STOP, nil
	case CancelLeaseDeadline:
		return velav1.RunnerCancelReason_RUNNER_CANCEL_REASON_LEASE_DEADLINE, nil
	case CancelAgentShutdown:
		return velav1.RunnerCancelReason_RUNNER_CANCEL_REASON_AGENT_SHUTDOWN, nil
	default:
		return velav1.RunnerCancelReason_RUNNER_CANCEL_REASON_UNSPECIFIED,
			errors.New("runner cancellation reason is invalid")
	}
}

func executionState(state velav1.RunnerExecutionState) (ExecutionState, error) {
	switch state {
	case velav1.RunnerExecutionState_RUNNER_EXECUTION_STATE_PREPARING:
		return ExecutionPreparing, nil
	case velav1.RunnerExecutionState_RUNNER_EXECUTION_STATE_READY:
		return ExecutionReady, nil
	case velav1.RunnerExecutionState_RUNNER_EXECUTION_STATE_RUNNING:
		return ExecutionRunning, nil
	case velav1.RunnerExecutionState_RUNNER_EXECUTION_STATE_SUCCEEDED:
		return ExecutionSucceeded, nil
	case velav1.RunnerExecutionState_RUNNER_EXECUTION_STATE_FAILED:
		return ExecutionFailed, nil
	case velav1.RunnerExecutionState_RUNNER_EXECUTION_STATE_CANCELED:
		return ExecutionCanceled, nil
	default:
		return "", errors.New("runner execution state is invalid")
	}
}

func parseFailure(state ExecutionState, protoFailure *velav1.RunnerFailure) (*Failure, error) {
	if state != ExecutionFailed {
		if protoFailure != nil {
			return nil, errors.New("non-failed runner Status contains failure evidence")
		}
		return nil, nil
	}
	if protoFailure == nil || !failureClassPattern.MatchString(protoFailure.GetFailureClass()) ||
		!failureFingerprintPattern.MatchString(protoFailure.GetFailureFingerprint()) ||
		!validPrintableText(protoFailure.GetErrorSummary(), 2000, false) ||
		!validPrintableText(protoFailure.GetBackendStage(), 100, false) ||
		!validPrintableText(protoFailure.GetInferenceBackendRevision(), 200, false) ||
		len(protoFailure.GetGpuUuids()) > 8 {
		return nil, errors.New("failed runner Status has invalid failure evidence")
	}
	gpuUUIDs := append([]string(nil), protoFailure.GetGpuUuids()...)
	for _, gpuUUID := range gpuUUIDs {
		if !validPrintableText(gpuUUID, 100, false) {
			return nil, errors.New("failed runner Status has an invalid GPU UUID")
		}
	}
	sort.Strings(gpuUUIDs)
	for index := 1; index < len(gpuUUIDs); index++ {
		if gpuUUIDs[index] == gpuUUIDs[index-1] {
			return nil, errors.New("failed runner Status has duplicate GPU UUIDs")
		}
	}
	return &Failure{
		FailureClass:             protoFailure.GetFailureClass(),
		FailureFingerprint:       protoFailure.GetFailureFingerprint(),
		ErrorSummary:             protoFailure.GetErrorSummary(),
		BackendStage:             protoFailure.GetBackendStage(),
		GPUUUIDs:                 gpuUUIDs,
		InferenceBackendRevision: protoFailure.GetInferenceBackendRevision(),
		RetryRecommended:         protoFailure.GetRetryRecommended(),
		WorkerReusable:           protoFailure.GetWorkerReusable(),
	}, nil
}

func parseOutput(candidate *velav1.RunnerOutput) (Output, error) {
	if candidate == nil || !outputKindPattern.MatchString(candidate.GetKind()) ||
		candidate.GetOrdinal() < 0 || candidate.GetSizeBytes() <= 0 ||
		len(candidate.GetSha256()) != 32 || !contentTypePattern.MatchString(candidate.GetContentType()) {
		return Output{}, errors.New("runner output receipt is invalid")
	}
	path := candidate.GetPath()
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned != path || cleaned == string(filepath.Separator) ||
		strings.ContainsRune(path, '\x00') || len([]byte(path)) > maxRunnerOutputPath {
		return Output{}, errors.New("runner output path is invalid")
	}
	var digest [32]byte
	copy(digest[:], candidate.GetSha256())
	return Output{
		Kind:        candidate.GetKind(),
		Ordinal:     candidate.GetOrdinal(),
		Path:        path,
		SizeBytes:   candidate.GetSizeBytes(),
		SHA256:      digest,
		ContentType: candidate.GetContentType(),
	}, nil
}

func validDetail(value string) bool {
	return validPrintableText(value, maxRunnerDetailRunes, true)
}

func validPrintableText(value string, maxRunes int, allowEmpty bool) bool {
	if !utf8.ValidString(value) {
		return false
	}
	count := 0
	for _, character := range value {
		if !unicode.IsPrint(character) {
			return false
		}
		count++
		if count > maxRunes {
			return false
		}
	}
	return allowEmpty || count > 0
}
