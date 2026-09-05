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
	"github.com/vivym/vela/internal/driverchannel"
	"github.com/vivym/vela/internal/driverdrain"
	"github.com/vivym/vela/internal/driverinspection"
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
	DeviceID      string `json:"device_id"`
	DeviceEpoch   int64  `json:"device_epoch"`
	ResourceClass string `json:"resource_class,omitempty"`
	GPUUUID       string `json:"gpu_uuid,omitempty"`
	PCIBDF        string `json:"pci_bdf,omitempty"`
}

type ProcessBackendConfig struct {
	Component              string
	ModelComponentRevision string
	Command                []string
	Environment            []string
	LocalDevices           []DriverDevice
	ScratchRoot            string
	InputRoot              string
	OutputRoot             string
	InitializationTimeout  time.Duration
	ShutdownTimeout        time.Duration
	// Stderr must return from Write. A blocked caller-owned writer cannot be canceled.
	Stderr io.Writer
}

type ProcessBackend struct {
	command            *exec.Cmd
	stdin              io.WriteCloser
	stdout             io.ReadCloser
	stderr             io.ReadCloser
	writer             *bufio.Writer
	responses          chan driverReadResult
	done               chan struct{}
	readStop           chan struct{}
	shutdownTimeout    time.Duration
	inspection         *driverinspection.Client
	inspectionProtocol string
	drain              *driverdrain.Client
	drainProtocol      string

	rpcGate   chan struct{}
	nextID    uint64
	closeOnce sync.Once
	closeErr  error
	killOnce  sync.Once
	killErr   error
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
	InputRoot              string          `json:"input_root"`
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
	ExecutionSequence      int64  `json:"execution_sequence,omitempty"`
	AuthorityDigest        string `json:"authority_digest"`
	JobID                  string `json:"job_id"`
	AttemptID              string `json:"attempt_id"`
	StageRunID             string `json:"stage_run_id"`
	StageAttemptID         string `json:"stage_attempt_id"`
	StageLeaseID           string `json:"stage_lease_id"`
	AttemptFence           int64  `json:"attempt_fence"`
	StageFence             int64  `json:"stage_fence"`
	StageVersion           int64  `json:"stage_version"`
	StageProfileRevisionID string `json:"stage_profile_revision_id"`
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
	SchemaVersion      int                  `json:"schema_version"`
	RequestID          uint64               `json:"request_id"`
	Acknowledged       bool                 `json:"acknowledged,omitempty"`
	Initialized        bool                 `json:"initialized,omitempty"`
	InspectionProtocol string               `json:"inspection_protocol,omitempty"`
	DrainProtocol      string               `json:"drain_protocol,omitempty"`
	Probe              *driverProbeResultV1 `json:"probe,omitempty"`
	Status             *driverStatusV1      `json:"status,omitempty"`
	Output             *driverOutputV1      `json:"output,omitempty"`
	Error              string               `json:"error,omitempty"`
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
	if err := configureDriverProcess(command); err != nil {
		return nil, err
	}
	inspectionConn, inspectionChild, err := driverinspection.Pair()
	if err != nil {
		return nil, fmt.Errorf("open ModelRuntime inspection channel: %w", err)
	}
	defer func() { _ = inspectionChild.Close() }()
	channelsOwned := false
	defer func() {
		if !channelsOwned {
			_ = inspectionConn.Close()
		}
	}()
	drainConn, drainChild, err := driverchannel.Pair()
	if err != nil {
		return nil, fmt.Errorf("open ModelRuntime drain channel: %w", err)
	}
	defer func() { _ = drainChild.Close() }()
	defer func() {
		if !channelsOwned {
			_ = drainConn.Close()
		}
	}()
	command.ExtraFiles = []*os.File{inspectionChild, drainChild}
	command.Dir = config.ScratchRoot
	command.Env = append(os.Environ(), config.Environment...)
	command.Env = append(command.Env, "VELA_MODEL_DRIVER_PROTOCOL=stdio-json-v1", driverinspection.Environment+"=3", driverdrain.Environment+"=4")
	stderrWriter := config.Stderr
	if stderrWriter == nil {
		stderrWriter = os.Stderr
	}
	stdinRead, stdin, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("open ModelRuntime driver stdin: %w", err)
	}
	defer func() { _ = stdinRead.Close() }()
	command.Stdin = stdinRead
	// Own the output pipes: exec.Cmd.Wait must only wait for the direct process,
	// independent of inherited descriptors and caller-provided stderr writers.
	stdout, stdoutWrite, err := os.Pipe()
	if err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("open ModelRuntime driver stdout: %w", err)
	}
	command.Stdout = stdoutWrite
	var stderr, stderrWrite *os.File
	if file, ok := stderrWriter.(*os.File); ok {
		command.Stderr = file
	} else {
		stderr, stderrWrite, err = os.Pipe()
		if err != nil {
			_ = stdin.Close()
			_ = stdout.Close()
			_ = stdoutWrite.Close()
			return nil, fmt.Errorf("open ModelRuntime driver stderr: %w", err)
		}
		command.Stderr = stderrWrite
	}
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stdoutWrite.Close()
		if stderr != nil {
			_ = stderr.Close()
			_ = stderrWrite.Close()
		}
		return nil, fmt.Errorf("start ModelRuntime driver: %w", err)
	}
	_ = stdinRead.Close()
	_ = stdoutWrite.Close()
	if stderrWrite != nil {
		_ = stderrWrite.Close()
	}
	backend := &ProcessBackend{
		command: command, stdin: stdin, writer: bufio.NewWriterSize(stdin, 64<<10),
		stdout:    stdout,
		responses: make(chan driverReadResult, 1), done: make(chan struct{}),
		readStop:        make(chan struct{}),
		rpcGate:         make(chan struct{}, 1),
		shutdownTimeout: config.ShutdownTimeout,
		inspection:      driverinspection.NewClient(inspectionConn),
		drain:           driverdrain.NewClient(drainConn),
	}
	channelsOwned = true
	if backend.shutdownTimeout == 0 {
		backend.shutdownTimeout = defaultShutdownTimeout
	}
	readerDone := make(chan struct{})
	stderrDone := make(chan error, 1)
	if stderr != nil {
		backend.stderr = stderr
		go func() {
			defer func() { _ = stderr.Close() }()
			_, err := io.Copy(stderrWriter, stderr)
			stderrDone <- err
		}()
	} else {
		stderrDone <- nil
	}
	go backend.readResponses(stdout, readerDone)
	go backend.wait(readerDone, stderrDone)
	initializeContext, cancel := context.WithTimeout(ctx, config.InitializationTimeout)
	defer cancel()
	response, err := backend.call(initializeContext, driverRequestV1{
		Operation:  "initialize",
		Initialize: initializeDriverRequest(binding, config),
	})
	if err != nil || !response.Initialized || !response.Acknowledged {
		cleanupErr := backend.abort()
		if err != nil {
			return nil, fmt.Errorf("initialize resident ModelRuntime driver: %w", errors.Join(err, cleanupErr))
		}
		return nil, errors.Join(errors.New("resident ModelRuntime driver did not complete load and warmup"), cleanupErr)
	}
	backend.inspectionProtocol = response.InspectionProtocol
	backend.drainProtocol = response.DrainProtocol
	return backend, nil
}

func (backend *ProcessBackend) DrainExecution(ctx context.Context, authority stageauthority.Verified) (BackendDrain, error) {
	if backend == nil || backend.drainProtocol != driverdrain.Protocol || authority.Authority == nil {
		return BackendDrain{}, ErrExecutionDrainUnproven
	}
	identity := driverdrain.Identity{AuthorityDigest: hex.EncodeToString(authority.Digest[:]),
		ExecutionSequence: authority.Authority.GetExecutionSequence()}
	if err := backend.drain.Drain(ctx, identity); err != nil {
		return BackendDrain{}, errors.Join(ErrExecutionDrainUnproven, err)
	}
	return BackendDrain{Contract: driverdrain.Contract, AuthorityDigest: authority.Digest,
		ExecutionSequence: identity.ExecutionSequence}, nil
}

func (backend *ProcessBackend) InspectExecution(ctx context.Context, authority stageauthority.Verified) (ExecutionInspection, error) {
	if backend == nil || backend.inspectionProtocol != driverinspection.Protocol {
		return ExecutionInspection{}, errors.New("ModelRuntime driver does not support read-only execution inspection")
	}
	observation, err := backend.inspection.Inspect(ctx, hex.EncodeToString(authority.Digest[:]))
	if err != nil {
		return ExecutionInspection{}, err
	}
	inspection := ExecutionInspection{Known: observation.Known, Sequence: observation.Sequence}
	if observation.Known {
		inspection.State = velav1.ModelRuntimeExecutionState(velav1.ModelRuntimeExecutionState_value["MODEL_RUNTIME_EXECUTION_STATE_"+observation.State])
	}
	if err := validateExecutionInspection(inspection); err != nil {
		return ExecutionInspection{}, err
	}
	return inspection, nil
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
			Identity: backend.stageIdentity(authority), ExecutionSpec: encoded,
		},
	})
	return requireDriverAcknowledgement(response, err, "prepare")
}

func (backend *ProcessBackend) Start(
	ctx context.Context,
	authority stageauthority.Verified,
) error {
	response, err := backend.call(ctx, driverRequestV1{
		Operation: "start", Stage: &driverStageRequestV1{Identity: backend.stageIdentity(authority)},
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
			Identity: backend.stageIdentity(authority), Reason: reason.String(),
		},
	})
	return requireDriverAcknowledgement(response, err, "cancel")
}

func (backend *ProcessBackend) Status(
	ctx context.Context,
	authority stageauthority.Verified,
) (BackendStatus, error) {
	response, err := backend.call(ctx, driverRequestV1{
		Operation: "status", Stage: &driverStageRequestV1{Identity: backend.stageIdentity(authority)},
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
		Operation: "seal", Stage: &driverStageRequestV1{Identity: backend.stageIdentity(authority)},
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
			backend.closeErr = errors.Join(err, backend.abort())
			return
		}
		select {
		case <-backend.done:
			backend.closeErr = backend.processWaitError()
		case <-time.After(backend.shutdownTimeout):
			backend.closeErr = errors.Join(errors.New("ModelRuntime driver did not exit after shutdown"), backend.abort())
		}
	})
	return backend.closeErr
}

// Done reports direct-driver teardown completion, including bounded output drain.
// Err reports incomplete drain; Done does not certify all descendants are gone.
func (backend *ProcessBackend) Done() <-chan struct{} {
	if backend == nil {
		return nil
	}
	return backend.done
}

func (backend *ProcessBackend) Err() error {
	if backend == nil {
		return errors.New("ModelRuntime driver is not configured")
	}
	select {
	case <-backend.done:
		if err := backend.processWaitError(); err != nil {
			return err
		}
		return errors.New("ModelRuntime driver exited")
	default:
		return nil
	}
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
	select {
	case backend.rpcGate <- struct{}{}:
		defer func() { <-backend.rpcGate }()
	case <-ctx.Done():
		return driverResponseV1{}, ctx.Err()
	case <-backend.done:
		return driverResponseV1{}, backend.Err()
	}
	if err := ctx.Err(); err != nil {
		return driverResponseV1{}, err
	}
	select {
	case <-backend.done:
		return driverResponseV1{}, backend.Err()
	default:
	}
	backend.nextID++
	request.SchemaVersion = driverProtocolVersion
	request.RequestID = backend.nextID
	encoded, err := json.Marshal(request)
	if err != nil || len(encoded) == 0 || len(encoded) > maxDriverMessageBytes {
		return driverResponseV1{}, errors.New("encode bounded ModelRuntime driver request")
	}
	// Cancellation must also interrupt writes when a driver stops reading stdin.
	cancellationDone := make(chan struct{})
	stopCancellation := context.AfterFunc(ctx, func() {
		defer close(cancellationDone)
		_ = backend.terminate()
	})
	defer func() {
		if !stopCancellation() {
			<-cancellationDone
		}
	}()
	if _, err := backend.writer.Write(encoded); err != nil {
		if ctx.Err() != nil {
			return driverResponseV1{}, ctx.Err()
		}
		return driverResponseV1{}, fmt.Errorf("write ModelRuntime driver request: %w", err)
	}
	if err := backend.writer.WriteByte('\n'); err != nil {
		if ctx.Err() != nil {
			return driverResponseV1{}, ctx.Err()
		}
		return driverResponseV1{}, fmt.Errorf("terminate ModelRuntime driver request: %w", err)
	}
	if err := backend.writer.Flush(); err != nil {
		if ctx.Err() != nil {
			return driverResponseV1{}, ctx.Err()
		}
		return driverResponseV1{}, fmt.Errorf("flush ModelRuntime driver request: %w", err)
	}
	var result driverReadResult
	select {
	case <-ctx.Done():
		_ = backend.abort()
		return driverResponseV1{}, ctx.Err()
	case <-backend.done:
		// The driver may exit immediately after its final acknowledgement.
		select {
		case result = <-backend.responses:
		default:
			return driverResponseV1{}, backend.Err()
		}
	case result = <-backend.responses:
	}
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

func (backend *ProcessBackend) readResponses(stdout io.ReadCloser, readerDone chan<- struct{}) {
	defer close(readerDone)
	defer func() { _ = stdout.Close() }()
	reader := bufio.NewReaderSize(stdout, 64<<10)
	for {
		line, err := readBoundedDriverLine(reader)
		if err != nil {
			backend.deliverTerminal(driverReadResult{err: err})
			return
		}
		var response driverResponseV1
		if err := strictjson.RejectDuplicateKeys(line); err != nil {
			backend.deliverTerminal(driverReadResult{err: fmt.Errorf("decode ModelRuntime driver response: %w", err)})
			return
		}
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&response); err != nil {
			backend.deliverTerminal(driverReadResult{err: fmt.Errorf("decode ModelRuntime driver response: %w", err)})
			return
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			backend.deliverTerminal(driverReadResult{err: errors.New("ModelRuntime driver response contains trailing data")})
			return
		}
		if !backend.deliver(driverReadResult{response: response}) {
			return
		}
	}
}

func (backend *ProcessBackend) deliver(result driverReadResult) bool {
	// Keep a final acknowledgement even if exit was already observed.
	select {
	case backend.responses <- result:
		return true
	default:
	}
	select {
	case backend.responses <- result:
		return true
	case <-backend.readStop:
		return false
	}
}

func (backend *ProcessBackend) deliverTerminal(result driverReadResult) {
	select {
	case backend.responses <- result:
	default:
	}
}

func (backend *ProcessBackend) wait(readerDone <-chan struct{}, stderrDone <-chan error) {
	// Observe exit without reaping so the PID/PGID cannot be reused before the
	// one group signal. No other path may call command.Wait.
	observeErr := waitDriverProcessExit(backend.command.Process)
	if observeErr != nil {
		observeErr = fmt.Errorf("observe ModelRuntime driver exit: %w", observeErr)
	}
	killErr := backend.terminate()
	waitErr := backend.command.Wait()
	close(backend.readStop)
	err := errors.Join(observeErr, killErr, waitErr, backend.drainOutputs(readerDone, stderrDone))
	backend.waitMu.Lock()
	backend.waitErr = err
	backend.waitMu.Unlock()
	close(backend.done)
}

func (backend *ProcessBackend) drainOutputs(readerDone <-chan struct{}, stderrDone <-chan error) error {
	defer func() {
		_ = backend.stdout.Close()
		if backend.stderr != nil {
			_ = backend.stderr.Close()
		}
	}()
	timer := time.NewTimer(backend.shutdownTimeout)
	defer timer.Stop()
	select {
	case <-readerDone:
	case <-timer.C:
		return errors.New("ModelRuntime driver stdout drain timed out; inherited descriptors may remain open")
	}
	select {
	case err := <-stderrDone:
		if err != nil {
			return fmt.Errorf("drain ModelRuntime driver stderr: %w", err)
		}
		return nil
	case <-timer.C:
		// Closing the pipe releases reads, but cannot interrupt a blocked Write
		// inside the caller's stderr sink. Report that incomplete drain.
		return errors.New("ModelRuntime driver stderr drain timed out; stderr writer may remain blocked")
	}
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
	err := backend.terminate()
	select {
	case <-backend.done:
		return errors.Join(err, backend.processWaitError())
	case <-time.After(2 * backend.shutdownTimeout):
		return errors.Join(err, errors.New("ModelRuntime driver teardown did not finish"))
	}
}

func (backend *ProcessBackend) terminate() error {
	backend.killOnce.Do(func() {
		_ = backend.inspection.Close()
		_ = backend.drain.Close()
		_ = backend.stdin.Close()
		backend.killErr = killDriverProcessGroup(backend.command.Process)
		if backend.killErr != nil {
			backend.killErr = fmt.Errorf("terminate ModelRuntime driver group: %w", backend.killErr)
		}
	})
	return backend.killErr
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
		ScratchRoot:  config.ScratchRoot, InputRoot: config.InputRoot,
		OutputRoot: config.OutputRoot,
	}
	for _, device := range binding.Devices {
		request.Devices = append(request.Devices, driverEpochV1{ID: device.ID, Epoch: device.Epoch})
	}
	for _, member := range binding.Members {
		request.Members = append(request.Members, driverEpochV1{ID: member.ID, Epoch: member.Epoch})
	}
	return request
}

func (backend *ProcessBackend) stageIdentity(authority stageauthority.Verified) driverStageIdentityV1 {
	identity := driverStageIdentity(authority)
	// Legacy strict drivers keep their original wire shape.
	if backend.drainProtocol == driverdrain.Protocol {
		identity.ExecutionSequence = authority.Authority.GetExecutionSequence()
	}
	return identity
}

func driverStageIdentity(authority stageauthority.Verified) driverStageIdentityV1 {
	return driverStageIdentityV1{
		AuthorityDigest: hex.EncodeToString(authority.Digest[:]),
		JobID:           authority.Authority.GetJobId(), AttemptID: authority.Authority.GetAttemptId(),
		StageRunID: authority.Authority.GetStageRunId(), StageAttemptID: authority.Authority.GetStageAttemptId(),
		StageLeaseID: authority.Authority.GetStageLeaseId(),
		AttemptFence: authority.Authority.GetAttemptFence(), StageFence: authority.Authority.GetStageFence(),
		StageVersion:           authority.Authority.GetStageVersion(),
		StageProfileRevisionID: authority.Authority.GetStageProfileRevisionId(),
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
		if !validDriverDevice(device) {
			return errors.New("ModelRuntime driver local Device identity is invalid")
		}
		if _, duplicate := seenDevices[device.DeviceID]; duplicate {
			return errors.New("ModelRuntime driver local Device identity is duplicated")
		}
		if device.GPUUUID != "" {
			if _, duplicate := seenGPUs[device.GPUUUID]; duplicate {
				return errors.New("ModelRuntime driver local GPU identity is duplicated")
			}
			if _, duplicate := seenBDFs[device.PCIBDF]; duplicate {
				return errors.New("ModelRuntime driver local PCI identity is duplicated")
			}
			seenGPUs[device.GPUUUID] = struct{}{}
			seenBDFs[device.PCIBDF] = struct{}{}
		}
		seenDevices[device.DeviceID] = struct{}{}
	}
	if !validPrivateDriverRoot(config.ScratchRoot) || !validPrivateDriverRoot(config.InputRoot) ||
		!validPrivateDriverRoot(config.OutputRoot) {
		return errors.New("ModelRuntime driver roots are invalid")
	}
	if config.InputRoot == config.OutputRoot {
		return errors.New("ModelRuntime driver input and output roots must be distinct")
	}
	for name, root := range map[string]string{"input": config.InputRoot, "output": config.OutputRoot} {
		relative, err := filepath.Rel(config.ScratchRoot, root)
		if err != nil || relative == "." || relative == ".." ||
			strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("ModelRuntime driver %s root must be below its scratch root", name)
		}
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

func validDriverDevice(device DriverDevice) bool {
	if uuid.Validate(device.DeviceID) != nil || device.DeviceEpoch <= 0 {
		return false
	}
	switch device.ResourceClass {
	case "", "GPU":
		return driverGPUUUIDPattern.MatchString(device.GPUUUID) &&
			driverPCIBDFPattern.MatchString(device.PCIBDF)
	case "CPU":
		return device.GPUUUID == "" && device.PCIBDF == ""
	default:
		return false
	}
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
var _ BackendLifecycle = (*ProcessBackend)(nil)
