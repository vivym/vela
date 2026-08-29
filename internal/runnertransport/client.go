package runnertransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/debugdumpcontract"
	"github.com/vivym/vela/internal/executionprogress"
	"github.com/vivym/vela/internal/securefile"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxUnixSocketPathBytes = 100
	maxRequestContentBytes = 64 * 1024
	maxStatusJSONBytes     = 16 * 1024
	maxRunnerDetailRunes   = 1000
	maxRunnerOutputPath    = 4096
	maxReadinessEvidence   = 16 * 1024
	maxReadinessDuration   = 2 * time.Hour
	debugDumpContentType   = debugdumpcontract.ContentType
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

type ReadinessIdentity struct {
	CycleID                    uuid.UUID
	WorkerID                   uuid.UUID
	WorkerEpoch                int64
	NodeIdentity               string
	ExecutionProfileRevisionID uuid.UUID
	InferenceBackendRevision   string
	Deadline                   time.Time
}

type ReadinessCheck string

const (
	ReadinessDevice           ReadinessCheck = "DEVICE"
	ReadinessInferenceBackend ReadinessCheck = "INFERENCE_BACKEND"
	ReadinessModelWarmup      ReadinessCheck = "MODEL_WARMUP"
	ReadinessCanary           ReadinessCheck = "CANARY"
)

type ReadinessResult struct {
	Decision CommandDecision
	Passed   bool
	Evidence json.RawMessage
	Detail   string
}

type ExecutionSpec struct {
	ModelRevisionID            uuid.UUID
	GenerationPresetRevisionID uuid.UUID
	ExecutionProfileRevisionID uuid.UUID
	OutputSpecID               uuid.UUID
	RequestContent             json.RawMessage
	DebugDumpAuthorization     *DebugDumpAuthorizationSnapshot
}

type DebugDumpAuthorizationSnapshot struct {
	AuthorizationID uuid.UUID
	ExpiresAt       time.Time
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
	DebugDump                 *DebugDump
}

type DebugDump struct {
	Content     json.RawMessage
	SizeBytes   int64
	SHA256      [sha256.Size]byte
	ContentType string
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

func (client *Client) ProbeReadiness(
	ctx context.Context,
	identity ReadinessIdentity,
	check ReadinessCheck,
) (ReadinessResult, error) {
	if client == nil || client.runner == nil {
		return ReadinessResult{}, errors.New("runner client is not configured")
	}
	if err := validateReadinessIdentity(identity); err != nil {
		return ReadinessResult{}, err
	}
	protoCheck, err := protoReadinessCheck(check)
	if err != nil {
		return ReadinessResult{}, err
	}
	response, err := client.runner.ProbeReadiness(ctx, &velav1.ProbeReadinessRequest{
		Identity: protoReadinessIdentity(identity),
		Check:    protoCheck,
	})
	if err != nil {
		return ReadinessResult{}, err
	}
	if response == nil || !sameProtoReadinessIdentity(response.GetIdentity(), identity) ||
		response.GetCheck() != protoCheck {
		return ReadinessResult{}, errors.New("runner readiness response authority does not match its request")
	}
	decision, err := commandDecision(response.GetDecision())
	if err != nil {
		return ReadinessResult{}, err
	}
	if !validDetail(response.GetDetail()) {
		return ReadinessResult{}, errors.New("runner readiness response detail is invalid")
	}
	if decision == CommandRejected {
		if response.GetPassed() || len(response.GetEvidenceJson()) != 0 {
			return ReadinessResult{}, errors.New("rejected runner readiness response contains evidence")
		}
		return ReadinessResult{Decision: decision, Detail: response.GetDetail()}, nil
	}
	evidence, err := parseReadinessEvidence(
		response.GetEvidenceJson(), identity, check, response.GetPassed(),
	)
	if err != nil {
		return ReadinessResult{
			Decision: CommandRejected,
			Detail:   "runner readiness evidence is invalid",
		}, nil
	}
	return ReadinessResult{
		Decision: decision, Passed: response.GetPassed(), Evidence: evidence,
		Detail: response.GetDetail(),
	}, nil
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
	protoSpec := &velav1.RunnerExecutionSpec{
		ModelRevisionId:            spec.ModelRevisionID.String(),
		GenerationPresetRevisionId: spec.GenerationPresetRevisionID.String(),
		ExecutionProfileRevisionId: spec.ExecutionProfileRevisionID.String(),
		OutputSpecId:               spec.OutputSpecID.String(),
		RequestContentJson:         append([]byte(nil), spec.RequestContent...),
	}
	if spec.DebugDumpAuthorization != nil {
		protoSpec.DebugDumpAuthorization = &velav1.RunnerDebugDumpAuthorizationSnapshot{
			AuthorizationId: spec.DebugDumpAuthorization.AuthorizationID.String(),
			ExpiresAt:       timestamppb.New(spec.DebugDumpAuthorization.ExpiresAt),
		}
	}
	response, err := client.runner.Prepare(ctx, &velav1.PrepareRequest{
		Identity:                   protoIdentity(identity),
		ExecutionSpec:              protoSpec,
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
	// Optional diagnostic evidence never invalidates the authoritative execution status.
	debugDump, _ := parseDebugDump(state, response.GetDebugDump(), identity, failure)
	return Status{
		State:                     state,
		Sequence:                  response.GetSequence(),
		BackendStage:              response.GetBackendStage(),
		BackendStageProgress:      progress,
		EstimatedRemainingSeconds: remaining,
		GPUHealth:                 gpuHealth,
		LocalArtifactState:        localState,
		Failure:                   failure,
		DebugDump:                 debugDump,
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

func validateReadinessIdentity(identity ReadinessIdentity) error {
	remaining := time.Until(identity.Deadline)
	if identity.CycleID == uuid.Nil || identity.WorkerID == uuid.Nil || identity.WorkerEpoch <= 0 ||
		identity.ExecutionProfileRevisionID == uuid.Nil ||
		!validPrintableText(identity.NodeIdentity, 500, false) ||
		strings.TrimSpace(identity.NodeIdentity) != identity.NodeIdentity ||
		!validPrintableText(identity.InferenceBackendRevision, 200, false) ||
		strings.TrimSpace(identity.InferenceBackendRevision) != identity.InferenceBackendRevision ||
		remaining <= 0 || remaining > maxReadinessDuration {
		return errors.New("runner readiness authority is invalid")
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
	if authorization := spec.DebugDumpAuthorization; authorization != nil {
		timestamp := timestamppb.New(authorization.ExpiresAt)
		if authorization.AuthorizationID == uuid.Nil || authorization.ExpiresAt.IsZero() ||
			timestamp.CheckValid() != nil {
			return errors.New("runner debug dump authorization is invalid")
		}
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

type readinessEvidenceCommon struct {
	Check                      string `json:"check"`
	CycleID                    string `json:"cycle_id"`
	Deadline                   string `json:"deadline"`
	ExecutionProfileRevisionID string `json:"execution_profile_revision_id"`
	InferenceBackendRevision   string `json:"inference_backend_revision"`
	NodeIdentity               string `json:"node_identity"`
	Passed                     *bool  `json:"passed"`
	SchemaVersion              *int   `json:"schema_version"`
	WorkerEpoch                *int64 `json:"worker_epoch"`
	WorkerID                   string `json:"worker_id"`
}

type deviceReadinessEvidence struct {
	readinessEvidenceCommon
	DITGPUUUIDs       []string `json:"dit_gpu_uuids"`
	EncoderVAEGPUUUID string   `json:"encoder_vae_gpu_uuid"`
}

type inferenceBackendReadinessEvidence struct {
	readinessEvidenceCommon
	Loaded *bool `json:"loaded"`
}

type modelWarmupReadinessEvidence struct {
	readinessEvidenceCommon
	Warmed *bool `json:"warmed"`
}

type canaryReadinessEvidence struct {
	readinessEvidenceCommon
	OutputSHA256 string `json:"output_sha256"`
}

func parseReadinessEvidence(
	raw []byte,
	identity ReadinessIdentity,
	check ReadinessCheck,
	passed bool,
) (json.RawMessage, error) {
	canonical, err := canonicalJSONObject(raw, maxReadinessEvidence)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, errors.New("runner readiness evidence is not canonical JSON")
	}
	switch check {
	case ReadinessDevice:
		var evidence deviceReadinessEvidence
		if err := decodeReadinessEvidence(canonical, &evidence); err != nil ||
			!validCommonReadinessEvidence(evidence.readinessEvidenceCommon, identity, check, passed) ||
			!validGPUUUID(evidence.EncoderVAEGPUUUID) || len(evidence.DITGPUUUIDs) != 7 {
			return nil, errors.New("runner DEVICE readiness evidence is invalid")
		}
		seen := map[string]struct{}{evidence.EncoderVAEGPUUUID: {}}
		for _, gpuUUID := range evidence.DITGPUUUIDs {
			if !validGPUUUID(gpuUUID) {
				return nil, errors.New("runner DEVICE readiness evidence is invalid")
			}
			if _, exists := seen[gpuUUID]; exists {
				return nil, errors.New("runner DEVICE readiness evidence contains duplicate GPUs")
			}
			seen[gpuUUID] = struct{}{}
		}
	case ReadinessInferenceBackend:
		var evidence inferenceBackendReadinessEvidence
		if err := decodeReadinessEvidence(canonical, &evidence); err != nil || evidence.Loaded == nil ||
			(passed && !*evidence.Loaded) ||
			!validCommonReadinessEvidence(evidence.readinessEvidenceCommon, identity, check, passed) {
			return nil, errors.New("runner INFERENCE_BACKEND readiness evidence is invalid")
		}
	case ReadinessModelWarmup:
		var evidence modelWarmupReadinessEvidence
		if err := decodeReadinessEvidence(canonical, &evidence); err != nil || evidence.Warmed == nil ||
			(passed && !*evidence.Warmed) ||
			!validCommonReadinessEvidence(evidence.readinessEvidenceCommon, identity, check, passed) {
			return nil, errors.New("runner MODEL_WARMUP readiness evidence is invalid")
		}
	case ReadinessCanary:
		var evidence canaryReadinessEvidence
		if err := decodeReadinessEvidence(canonical, &evidence); err != nil ||
			!validCommonReadinessEvidence(evidence.readinessEvidenceCommon, identity, check, passed) ||
			len(evidence.OutputSHA256) != 64 {
			return nil, errors.New("runner CANARY readiness evidence is invalid")
		}
		for _, character := range evidence.OutputSHA256 {
			if !strings.ContainsRune("0123456789abcdef", character) {
				return nil, errors.New("runner CANARY readiness evidence digest is invalid")
			}
		}
	default:
		return nil, errors.New("runner readiness evidence check is unsupported")
	}
	return append(json.RawMessage(nil), canonical...), nil
}

func decodeReadinessEvidence(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(destination)
}

func validCommonReadinessEvidence(
	evidence readinessEvidenceCommon,
	identity ReadinessIdentity,
	check ReadinessCheck,
	passed bool,
) bool {
	return evidence.SchemaVersion != nil && *evidence.SchemaVersion == 1 &&
		evidence.Passed != nil && *evidence.Passed == passed &&
		evidence.WorkerEpoch != nil && *evidence.WorkerEpoch == identity.WorkerEpoch &&
		evidence.Check == string(check) && evidence.CycleID == identity.CycleID.String() &&
		evidence.WorkerID == identity.WorkerID.String() && evidence.NodeIdentity == identity.NodeIdentity &&
		evidence.ExecutionProfileRevisionID == identity.ExecutionProfileRevisionID.String() &&
		evidence.InferenceBackendRevision == identity.InferenceBackendRevision &&
		evidence.Deadline == identity.Deadline.UTC().Format(time.RFC3339Nano)
}

func validGPUUUID(value string) bool {
	const prefix = "GPU-"
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	parsed, err := uuid.Parse(strings.TrimPrefix(value, prefix))
	return err == nil && prefix+parsed.String() == value
}

func protoReadinessIdentity(identity ReadinessIdentity) *velav1.RunnerReadinessIdentity {
	return &velav1.RunnerReadinessIdentity{
		CycleId: identity.CycleID.String(), WorkerId: identity.WorkerID.String(),
		WorkerEpoch: identity.WorkerEpoch, NodeIdentity: identity.NodeIdentity,
		ExecutionProfileRevisionId: identity.ExecutionProfileRevisionID.String(),
		InferenceBackendRevision:   identity.InferenceBackendRevision,
		Deadline:                   timestamppb.New(identity.Deadline),
	}
}

func sameProtoReadinessIdentity(got *velav1.RunnerReadinessIdentity, want ReadinessIdentity) bool {
	return got != nil && got.GetDeadline() != nil && got.GetDeadline().CheckValid() == nil &&
		got.GetCycleId() == want.CycleID.String() && got.GetWorkerId() == want.WorkerID.String() &&
		got.GetWorkerEpoch() == want.WorkerEpoch && got.GetNodeIdentity() == want.NodeIdentity &&
		got.GetExecutionProfileRevisionId() == want.ExecutionProfileRevisionID.String() &&
		got.GetInferenceBackendRevision() == want.InferenceBackendRevision &&
		got.GetDeadline().AsTime().Equal(want.Deadline)
}

func protoReadinessCheck(check ReadinessCheck) (velav1.RunnerReadinessCheck, error) {
	switch check {
	case ReadinessDevice:
		return velav1.RunnerReadinessCheck_RUNNER_READINESS_CHECK_DEVICE, nil
	case ReadinessInferenceBackend:
		return velav1.RunnerReadinessCheck_RUNNER_READINESS_CHECK_INFERENCE_BACKEND, nil
	case ReadinessModelWarmup:
		return velav1.RunnerReadinessCheck_RUNNER_READINESS_CHECK_MODEL_WARMUP, nil
	case ReadinessCanary:
		return velav1.RunnerReadinessCheck_RUNNER_READINESS_CHECK_CANARY, nil
	default:
		return velav1.RunnerReadinessCheck_RUNNER_READINESS_CHECK_UNSPECIFIED,
			errors.New("runner readiness check is invalid")
	}
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

func parseDebugDump(
	state ExecutionState,
	candidate *velav1.RunnerDebugDump,
	identity AttemptIdentity,
	failure *Failure,
) (*DebugDump, error) {
	if state != ExecutionFailed {
		if candidate != nil {
			return nil, errors.New("non-failed runner Status contains a debug dump")
		}
		return nil, nil
	}
	if candidate == nil {
		return nil, nil
	}
	content := candidate.GetContent()
	if len(content) == 0 || len(content) > debugdumpcontract.MaxBytes ||
		candidate.GetSizeBytes() != int64(len(content)) ||
		len(candidate.GetSha256()) != sha256.Size ||
		candidate.GetContentType() != debugdumpcontract.ContentType {
		return nil, errors.New("failed runner Status has an invalid debug dump receipt")
	}
	envelope, err := debugdumpcontract.Parse(content)
	if err != nil || failure == nil || envelope.AttemptID != identity.AttemptID.String() ||
		envelope.JobID != identity.JobID.String() || envelope.WorkerID != identity.WorkerID.String() ||
		envelope.WorkerEpoch != identity.WorkerEpoch || envelope.LeaseFence != identity.LeaseFence ||
		envelope.BackendStage != failure.BackendStage ||
		envelope.FailureClass != failure.FailureClass ||
		envelope.FailureFingerprint != failure.FailureFingerprint ||
		envelope.InferenceBackendRevision != failure.InferenceBackendRevision ||
		envelope.RetryRecommended != failure.RetryRecommended ||
		envelope.WorkerReusable != failure.WorkerReusable ||
		!slices.Equal(envelope.GPUUUIDs, failure.GPUUUIDs) {
		return nil, errors.New("failed runner Status debug dump envelope is invalid")
	}
	digest := sha256.Sum256(content)
	if !bytes.Equal(digest[:], candidate.GetSha256()) {
		return nil, errors.New("failed runner Status debug dump checksum does not match")
	}
	return &DebugDump{
		Content:     append(json.RawMessage(nil), content...),
		SizeBytes:   candidate.GetSizeBytes(),
		SHA256:      digest,
		ContentType: candidate.GetContentType(),
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
