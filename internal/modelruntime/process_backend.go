package modelruntime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/strictjson"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const (
	driverProtocolVersion  = 1
	maxDriverMessageBytes  = 1 << 20
	defaultShutdownTimeout = 20 * time.Second
)

var (
	driverGPUUUIDPattern = regexp.MustCompile(
		`^GPU-[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`,
	)
	driverPCIBDFPattern = regexp.MustCompile(`^[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}[.][0-7]$`)
)

type DriverDevice struct {
	DeviceID    string `json:"device_id"`
	DeviceEpoch int64  `json:"device_epoch"`
	GPUUUID     string `json:"gpu_uuid"`
	PCIBDF      string `json:"pci_bdf"`
}

type ProcessBackendConfig struct {
	Component              string
	ModelComponentRevision string
	Command                []string
	Environment            []string
	LocalDevices           []DriverDevice
	ScratchRoot            string
	OutputRoot             string
	InitializationTimeout  time.Duration
	ShutdownTimeout        time.Duration
	Stderr                 io.Writer
}

type ProcessBackend struct {
	command         *exec.Cmd
	stdin           io.WriteCloser
	writer          *bufio.Writer
	responses       chan driverReadResult
	done            chan struct{}
	shutdownTimeout time.Duration

	rpcMu     sync.Mutex
	nextID    uint64
	closeOnce sync.Once
	closeErr  error
	waitMu    sync.Mutex
	waitErr   error
}

type driverRequestV1 struct {
	SchemaVersion int                        `json:"schema_version"`
	RequestID     uint64                     `json:"request_id"`
	Operation     string                     `json:"operation"`
	Initialize    *driverInitializeRequestV1 `json:"initialize,omitempty"`
	Probe         *driverProbeRequestV1      `json:"probe,omitempty"`
	Prepare       *driverPrepareRequestV1    `json:"prepare,omitempty"`
	Stage         *driverStageRequestV1      `json:"stage,omitempty"`
	Cancel        *driverCancelRequestV1     `json:"cancel,omitempty"`
}

type driverInitializeRequestV1 struct {
	WorkerInstanceID       string          `json:"worker_instance_id"`
	WorkerInstanceEpoch    int64           `json:"worker_instance_epoch"`
	WorkerMemberID         string          `json:"worker_member_id"`
	WorkerMemberEpoch      int64           `json:"worker_member_epoch"`
	DeviceSetDigest        string          `json:"device_set_digest"`
	Devices                []driverEpochV1 `json:"devices"`
	MembershipDigest       string          `json:"membership_digest"`
	Members                []driverEpochV1 `json:"members"`
	ModelResidencyID       string          `json:"model_residency_id"`
	RuntimeIdentity        string          `json:"runtime_identity"`
	ModelRuntimeEpoch      int64           `json:"model_runtime_epoch"`
	StageProfileRevisionID string          `json:"stage_profile_revision_id"`
	Component              string          `json:"component"`
	ModelComponentRevision string          `json:"model_component_revision"`
	LocalDevices           []DriverDevice  `json:"local_devices"`
	ScratchRoot            string          `json:"scratch_root"`
	OutputRoot             string          `json:"output_root"`
}

type driverEpochV1 struct {
	ID    string `json:"id"`
	Epoch int64  `json:"epoch"`
}

type driverProbeRequestV1 struct {
	Check string `json:"check"`
}

type driverStageIdentityV1 struct {
	AuthorityDigest string `json:"authority_digest"`
	JobID           string `json:"job_id"`
	AttemptID       string `json:"attempt_id"`
	StageRunID      string `json:"stage_run_id"`
	StageAttemptID  string `json:"stage_attempt_id"`
	StageLeaseID    string `json:"stage_lease_id"`
}

type driverPrepareRequestV1 struct {
	Identity      driverStageIdentityV1 `json:"identity"`
	ExecutionSpec []byte                `json:"execution_spec"`
}

type driverStageRequestV1 struct {
	Identity driverStageIdentityV1 `json:"identity"`
}

type driverCancelRequestV1 struct {
	Identity driverStageIdentityV1 `json:"identity"`
	Reason   string                `json:"reason"`
}

type driverResponseV1 struct {
	SchemaVersion int                  `json:"schema_version"`
	RequestID     uint64               `json:"request_id"`
	Acknowledged  bool                 `json:"acknowledged,omitempty"`
	Initialized   bool                 `json:"initialized,omitempty"`
	Probe         *driverProbeResultV1 `json:"probe,omitempty"`
	Status        *driverStatusV1      `json:"status,omitempty"`
	Output        *driverOutputV1      `json:"output,omitempty"`
	Error         string               `json:"error,omitempty"`
}

type driverProbeResultV1 struct {
	Ready    bool   `json:"ready"`
	Evidence []byte `json:"evidence"`
	Detail   string `json:"detail,omitempty"`
}

type driverStatusV1 struct {
	State              string                   `json:"state"`
	Sequence           int64                    `json:"sequence"`
	BackendStage       string                   `json:"backend_stage"`
	Progress           *float64                 `json:"progress,omitempty"`
	BoundedStatusJSON  []byte                   `json:"bounded_status_json"`
	LocalReceiptID     string                   `json:"local_receipt_id,omitempty"`
	LocalReceiptDigest []byte                   `json:"local_receipt_digest,omitempty"`
	Detail             string                   `json:"detail,omitempty"`
	Failure            *driverFailureEvidenceV1 `json:"failure,omitempty"`
}

type driverFailureEvidenceV1 struct {
	FailureClass          string `json:"failure_class"`
	FailureFingerprint    []byte `json:"failure_fingerprint"`
	Detail                string `json:"detail"`
	WorkerReusable        bool   `json:"worker_reusable"`
	ConsumedResourceUnits int64  `json:"consumed_resource_units"`
	FailedAt              string `json:"failed_at"`
	RetryAt               string `json:"retry_at"`
}

type driverOutputV1 struct {
	OutputManifestJSON []byte `json:"output_manifest_json"`
	TotalSizeBytes     int64  `json:"total_size_bytes"`
}

type driverReadResult struct {
	response driverResponseV1
	err      error
}

func NewProcessBackend(
	ctx context.Context,
	binding stageauthority.RuntimeBinding,
	config ProcessBackendConfig,
) (*ProcessBackend, error) {
	if ctx == nil {
		return nil, errors.New("ModelRuntime driver context is required")
	}
	if err := validateProcessBackendConfig(binding, config); err != nil {
		return nil, err
	}
	command := exec.Command(config.Command[0], config.Command[1:]...)
	command.Dir = config.ScratchRoot
	command.Env = append(os.Environ(), config.Environment...)
	command.Env = append(command.Env, "VELA_MODEL_DRIVER_PROTOCOL=stdio-json-v1")
	if config.Stderr == nil {
		command.Stderr = os.Stderr
	} else {
		command.Stderr = config.Stderr
	}
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("open ModelRuntime driver stdin: %w", err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("open ModelRuntime driver stdout: %w", err)
	}
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("start ModelRuntime driver: %w", err)
	}
	backend := &ProcessBackend{
		command: command, stdin: stdin, writer: bufio.NewWriterSize(stdin, 64<<10),
		responses: make(chan driverReadResult, 1), done: make(chan struct{}),
		shutdownTimeout: config.ShutdownTimeout,
	}
	if backend.shutdownTimeout == 0 {
		backend.shutdownTimeout = defaultShutdownTimeout
	}
	go backend.readResponses(stdout)
	go backend.wait()
	initializeContext, cancel := context.WithTimeout(ctx, config.InitializationTimeout)
	defer cancel()
	response, err := backend.call(initializeContext, driverRequestV1{
		Operation:  "initialize",
		Initialize: initializeDriverRequest(binding, config),
	})
	if err != nil || !response.Initialized || !response.Acknowledged {
		_ = backend.abort()
		if err != nil {
			return nil, fmt.Errorf("initialize resident ModelRuntime driver: %w", err)
		}
		return nil, errors.New("resident ModelRuntime driver did not complete load and warmup")
	}
	return backend, nil
}

func (backend *ProcessBackend) Probe(
	ctx context.Context,
	check velav1.ModelRuntimeReadinessCheck,
) (ProbeResult, error) {
	response, err := backend.call(ctx, driverRequestV1{
		Operation: "probe", Probe: &driverProbeRequestV1{Check: check.String()},
	})
	if err != nil {
		return ProbeResult{}, err
	}
	if response.Probe == nil {
		return ProbeResult{}, errors.New("ModelRuntime driver returned no readiness result")
	}
	return ProbeResult{
		Ready: response.Probe.Ready, Evidence: append([]byte(nil), response.Probe.Evidence...),
		Detail: response.Probe.Detail,
	}, nil
}

func (backend *ProcessBackend) Prepare(
	ctx context.Context,
	authority stageauthority.Verified,
	spec *velav1.StageExecutionSpec,
) error {
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(spec)
	if err != nil {
		return fmt.Errorf("encode ModelRuntime driver execution spec: %w", err)
	}
	response, err := backend.call(ctx, driverRequestV1{
		Operation: "prepare",
		Prepare: &driverPrepareRequestV1{
			Identity: driverStageIdentity(authority), ExecutionSpec: encoded,
		},
	})
	return requireDriverAcknowledgement(response, err, "prepare")
}

func (backend *ProcessBackend) Start(
	ctx context.Context,
	authority stageauthority.Verified,
) error {
	response, err := backend.call(ctx, driverRequestV1{
		Operation: "start", Stage: &driverStageRequestV1{Identity: driverStageIdentity(authority)},
	})
	return requireDriverAcknowledgement(response, err, "start")
}

func (backend *ProcessBackend) Cancel(
	ctx context.Context,
	authority stageauthority.Verified,
	reason velav1.ModelRuntimeCancelReason,
) error {
	response, err := backend.call(ctx, driverRequestV1{
		Operation: "cancel",
		Cancel: &driverCancelRequestV1{
			Identity: driverStageIdentity(authority), Reason: reason.String(),
		},
	})
	return requireDriverAcknowledgement(response, err, "cancel")
}

func (backend *ProcessBackend) Status(
	ctx context.Context,
	authority stageauthority.Verified,
) (BackendStatus, error) {
	response, err := backend.call(ctx, driverRequestV1{
		Operation: "status", Stage: &driverStageRequestV1{Identity: driverStageIdentity(authority)},
	})
	if err != nil {
		return BackendStatus{}, err
	}
	if response.Status == nil {
		return BackendStatus{}, errors.New("ModelRuntime driver returned no execution status")
	}
	return decodeDriverStatus(response.Status)
}

func (backend *ProcessBackend) Seal(
	ctx context.Context,
	authority stageauthority.Verified,
) (SealedOutput, error) {
	response, err := backend.call(ctx, driverRequestV1{
		Operation: "seal", Stage: &driverStageRequestV1{Identity: driverStageIdentity(authority)},
	})
	if err != nil {
		return SealedOutput{}, err
	}
	if response.Output == nil {
		return SealedOutput{}, errors.New("ModelRuntime driver returned no sealed output")
	}
	return SealedOutput{
		OutputManifestJSON: append([]byte(nil), response.Output.OutputManifestJSON...),
		TotalSizeBytes:     response.Output.TotalSizeBytes,
	}, nil
}

func (backend *ProcessBackend) Close() error {
	if backend == nil {
		return nil
	}
	backend.closeOnce.Do(func() {
		response, err := backend.callWithTimeout(driverRequestV1{Operation: "shutdown"})
		if err == nil {
			err = requireDriverAcknowledgement(response, nil, "shutdown")
		}
		_ = backend.stdin.Close()
		if err != nil {
			_ = backend.abort()
			backend.closeErr = err
			return
		}
		select {
		case <-backend.done:
			backend.closeErr = backend.processWaitError()
		case <-time.After(backend.shutdownTimeout):
			backend.closeErr = errors.New("ModelRuntime driver did not exit after shutdown")
			_ = backend.abort()
		}
	})
	return backend.closeErr
}

func (backend *ProcessBackend) callWithTimeout(request driverRequestV1) (driverResponseV1, error) {
	ctx, cancel := context.WithTimeout(context.Background(), backend.shutdownTimeout)
	defer cancel()
	return backend.call(ctx, request)
}

func (backend *ProcessBackend) call(
	ctx context.Context,
	request driverRequestV1,
) (driverResponseV1, error) {
	if backend == nil || ctx == nil {
		return driverResponseV1{}, errors.New("ModelRuntime driver call is not configured")
	}
	if err := ctx.Err(); err != nil {
		return driverResponseV1{}, err
	}
	backend.rpcMu.Lock()
	defer backend.rpcMu.Unlock()
	select {
	case <-backend.done:
		return driverResponseV1{}, fmt.Errorf("ModelRuntime driver exited: %w", backend.processWaitError())
	default:
	}
	backend.nextID++
	request.SchemaVersion = driverProtocolVersion
	request.RequestID = backend.nextID
	encoded, err := json.Marshal(request)
	if err != nil || len(encoded) == 0 || len(encoded) > maxDriverMessageBytes {
		return driverResponseV1{}, errors.New("encode bounded ModelRuntime driver request")
	}
	if _, err := backend.writer.Write(encoded); err != nil {
		return driverResponseV1{}, fmt.Errorf("write ModelRuntime driver request: %w", err)
	}
	if err := backend.writer.WriteByte('\n'); err != nil {
		return driverResponseV1{}, fmt.Errorf("terminate ModelRuntime driver request: %w", err)
	}
	if err := backend.writer.Flush(); err != nil {
		return driverResponseV1{}, fmt.Errorf("flush ModelRuntime driver request: %w", err)
	}
	select {
	case <-ctx.Done():
		_ = backend.abort()
		return driverResponseV1{}, ctx.Err()
	case <-backend.done:
		return driverResponseV1{}, fmt.Errorf("ModelRuntime driver exited: %w", backend.processWaitError())
	case result := <-backend.responses:
		if result.err != nil {
			_ = backend.abort()
			return driverResponseV1{}, result.err
		}
		if result.response.RequestID != request.RequestID ||
			result.response.SchemaVersion != driverProtocolVersion {
			_ = backend.abort()
			return driverResponseV1{}, errors.New("ModelRuntime driver response identity is invalid")
		}
		if result.response.Error != "" {
			return driverResponseV1{}, errors.New(result.response.Error)
		}
		return result.response, nil
	}
}

func (backend *ProcessBackend) readResponses(stdout io.ReadCloser) {
	defer func() { _ = stdout.Close() }()
	reader := bufio.NewReaderSize(stdout, 64<<10)
	for {
		line, err := readBoundedDriverLine(reader)
		if err != nil {
			backend.deliver(driverReadResult{err: err})
			return
		}
		var response driverResponseV1
		if err := strictjson.RejectDuplicateKeys(line); err != nil {
			backend.deliver(driverReadResult{err: fmt.Errorf("decode ModelRuntime driver response: %w", err)})
			return
		}
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&response); err != nil {
			backend.deliver(driverReadResult{err: fmt.Errorf("decode ModelRuntime driver response: %w", err)})
			return
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			backend.deliver(driverReadResult{err: errors.New("ModelRuntime driver response contains trailing data")})
			return
		}
		backend.deliver(driverReadResult{response: response})
	}
}

func (backend *ProcessBackend) deliver(result driverReadResult) {
	select {
	case backend.responses <- result:
	case <-backend.done:
	}
}

func (backend *ProcessBackend) wait() {
	err := backend.command.Wait()
	backend.waitMu.Lock()
	backend.waitErr = err
	backend.waitMu.Unlock()
	close(backend.done)
}

func (backend *ProcessBackend) processWaitError() error {
	backend.waitMu.Lock()
	defer backend.waitMu.Unlock()
	return backend.waitErr
}

func (backend *ProcessBackend) abort() error {
	if backend == nil || backend.command == nil || backend.command.Process == nil {
		return nil
	}
	err := backend.command.Process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}

func initializeDriverRequest(
	binding stageauthority.RuntimeBinding,
	config ProcessBackendConfig,
) *driverInitializeRequestV1 {
	request := &driverInitializeRequestV1{
		WorkerInstanceID: binding.WorkerInstanceID, WorkerInstanceEpoch: binding.WorkerInstanceEpoch,
		WorkerMemberID: binding.WorkerMemberID, WorkerMemberEpoch: binding.WorkerMemberEpoch,
		DeviceSetDigest:  hex.EncodeToString(binding.DeviceSetDigest),
		MembershipDigest: hex.EncodeToString(binding.MembershipDigest),
		ModelResidencyID: binding.ModelResidencyID, RuntimeIdentity: binding.ModelRuntimeIdentity,
		ModelRuntimeEpoch: binding.ModelRuntimeEpoch, StageProfileRevisionID: binding.StageProfileRevisionID,
		Component: config.Component, ModelComponentRevision: config.ModelComponentRevision,
		LocalDevices: append([]DriverDevice(nil), config.LocalDevices...),
		ScratchRoot:  config.ScratchRoot, OutputRoot: config.OutputRoot,
	}
	for _, device := range binding.Devices {
		request.Devices = append(request.Devices, driverEpochV1{ID: device.ID, Epoch: device.Epoch})
	}
	for _, member := range binding.Members {
		request.Members = append(request.Members, driverEpochV1{ID: member.ID, Epoch: member.Epoch})
	}
	return request
}

func driverStageIdentity(authority stageauthority.Verified) driverStageIdentityV1 {
	return driverStageIdentityV1{
		AuthorityDigest: hex.EncodeToString(authority.Digest[:]),
		JobID:           authority.Authority.GetJobId(), AttemptID: authority.Authority.GetAttemptId(),
		StageRunID: authority.Authority.GetStageRunId(), StageAttemptID: authority.Authority.GetStageAttemptId(),
		StageLeaseID: authority.Authority.GetStageLeaseId(),
	}
}

func requireDriverAcknowledgement(
	response driverResponseV1,
	err error,
	operation string,
) error {
	if err != nil {
		return err
	}
	if !response.Acknowledged {
		return fmt.Errorf("ModelRuntime driver did not acknowledge %s", operation)
	}
	return nil
}

func decodeDriverStatus(status *driverStatusV1) (BackendStatus, error) {
	state, ok := driverRuntimeStates[status.State]
	if !ok {
		return BackendStatus{}, errors.New("ModelRuntime driver returned an unknown execution state")
	}
	decoded := BackendStatus{
		State: state, Sequence: status.Sequence, BackendStage: status.BackendStage,
		Progress: status.Progress, BoundedStatusJSON: append([]byte(nil), status.BoundedStatusJSON...),
		LocalReceiptID:     status.LocalReceiptID,
		LocalReceiptDigest: append([]byte(nil), status.LocalReceiptDigest...), Detail: status.Detail,
	}
	if status.Failure != nil {
		failedAt, err := time.Parse(time.RFC3339Nano, status.Failure.FailedAt)
		if err != nil {
			return BackendStatus{}, errors.New("ModelRuntime driver failure time is invalid")
		}
		retryAt, err := time.Parse(time.RFC3339Nano, status.Failure.RetryAt)
		if err != nil {
			return BackendStatus{}, errors.New("ModelRuntime driver retry time is invalid")
		}
		decoded.FailureEvidence = &FailureEvidence{
			FailureClass:       status.Failure.FailureClass,
			FailureFingerprint: append([]byte(nil), status.Failure.FailureFingerprint...),
			Detail:             status.Failure.Detail, WorkerReusable: status.Failure.WorkerReusable,
			ConsumedResourceUnits: status.Failure.ConsumedResourceUnits,
			FailedAt:              failedAt, RetryAt: retryAt,
		}
	}
	return decoded, nil
}

var driverRuntimeStates = map[string]velav1.ModelRuntimeExecutionState{
	"PREPARING":     velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARING,
	"PREPARED":      velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED,
	"RUNNING":       velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING,
	"CANCELING":     velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING,
	"STOPPED":       velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED,
	"OUTPUT_READY":  velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_READY,
	"OUTPUT_SEALED": velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED,
	"FAILED":        velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED,
}

func readBoundedDriverLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		fragment, more, err := reader.ReadLine()
		if err != nil {
			return nil, fmt.Errorf("read ModelRuntime driver response: %w", err)
		}
		if len(line)+len(fragment) > maxDriverMessageBytes {
			return nil, errors.New("ModelRuntime driver response exceeds bound")
		}
		line = append(line, fragment...)
		if !more {
			break
		}
	}
	if len(line) == 0 {
		return nil, errors.New("ModelRuntime driver returned an empty response")
	}
	return line, nil
}

func validateProcessBackendConfig(
	binding stageauthority.RuntimeBinding,
	config ProcessBackendConfig,
) error {
	if err := validateBindingTemplate(binding); err != nil || binding.ModelRuntimeEpoch <= 0 {
		return errors.New("ModelRuntime driver binding is invalid")
	}
	if !validDriverText(config.Component, 100) ||
		!validDriverText(config.ModelComponentRevision, 300) || len(config.Command) == 0 ||
		len(config.Command) > 128 || !filepath.IsAbs(config.Command[0]) ||
		filepath.Clean(config.Command[0]) != config.Command[0] {
		return errors.New("ModelRuntime driver process configuration is invalid")
	}
	for _, argument := range config.Command {
		if !validDriverText(argument, 4096) {
			return errors.New("ModelRuntime driver process configuration is invalid")
		}
	}
	if config.InitializationTimeout <= 0 || config.InitializationTimeout > 24*time.Hour {
		return errors.New("ModelRuntime driver initialization timeout is invalid")
	}
	if config.ShutdownTimeout != 0 &&
		(config.ShutdownTimeout <= 0 || config.ShutdownTimeout > 10*time.Minute) {
		return errors.New("ModelRuntime driver shutdown timeout is invalid")
	}
	if len(config.LocalDevices) == 0 || len(config.LocalDevices) > 64 {
		return errors.New("ModelRuntime driver local DeviceSet is invalid")
	}
	seenDevices := make(map[string]struct{}, len(config.LocalDevices))
	seenGPUs := make(map[string]struct{}, len(config.LocalDevices))
	seenBDFs := make(map[string]struct{}, len(config.LocalDevices))
	for _, device := range config.LocalDevices {
		if uuid.Validate(device.DeviceID) != nil || device.DeviceEpoch <= 0 ||
			!driverGPUUUIDPattern.MatchString(device.GPUUUID) ||
			!driverPCIBDFPattern.MatchString(device.PCIBDF) {
			return errors.New("ModelRuntime driver local Device identity is invalid")
		}
		if _, duplicate := seenDevices[device.DeviceID]; duplicate {
			return errors.New("ModelRuntime driver local Device identity is duplicated")
		}
		if _, duplicate := seenGPUs[device.GPUUUID]; duplicate {
			return errors.New("ModelRuntime driver local GPU identity is duplicated")
		}
		if _, duplicate := seenBDFs[device.PCIBDF]; duplicate {
			return errors.New("ModelRuntime driver local PCI identity is duplicated")
		}
		seenDevices[device.DeviceID] = struct{}{}
		seenGPUs[device.GPUUUID] = struct{}{}
		seenBDFs[device.PCIBDF] = struct{}{}
	}
	if !validPrivateDriverRoot(config.ScratchRoot) || !validPrivateDriverRoot(config.OutputRoot) {
		return errors.New("ModelRuntime driver roots are invalid")
	}
	relative, err := filepath.Rel(config.ScratchRoot, config.OutputRoot)
	if err != nil || relative == "." || relative == ".." ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("ModelRuntime driver output root must be below its scratch root")
	}
	seenEnvironment := make(map[string]struct{}, len(config.Environment))
	for _, entry := range config.Environment {
		name, _, found := strings.Cut(entry, "=")
		if !found || name == "" || strings.ContainsAny(name, "\x00=") ||
			strings.ContainsRune(entry, '\x00') {
			return errors.New("ModelRuntime driver environment is invalid")
		}
		if _, duplicate := seenEnvironment[name]; duplicate || name == "VELA_MODEL_DRIVER_PROTOCOL" {
			return errors.New("ModelRuntime driver environment is duplicated or reserved")
		}
		seenEnvironment[name] = struct{}{}
	}
	return nil
}

func validPrivateDriverRoot(path string) bool {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, '\x00') {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && info.IsDir() && info.Mode().Perm()&0o022 == 0
}

func validDriverText(value string, maximum int) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= maximum &&
		utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}

var _ Backend = (*ProcessBackend)(nil)
