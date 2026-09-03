package h3stagemock

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/h3mockbackend"
	"github.com/vivym/vela/internal/strictjson"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const (
	protocolVersion       = 1
	maximumMessageBytes   = 1 << 20
	maximumExecutionBytes = 64 << 10
	maximumInputs         = 64
)

var (
	gpuUUIDPattern = regexp.MustCompile(
		`^GPU-[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`,
	)
	pciBDFPattern = regexp.MustCompile(`^[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}[.][0-7]$`)
)

type operation string

const (
	operationInitialize operation = "initialize"
	operationProbe      operation = "probe"
	operationPrepare    operation = "prepare"
	operationStart      operation = "start"
	operationStatus     operation = "status"
	operationCancel     operation = "cancel"
	operationSeal       operation = "seal"
	operationShutdown   operation = "shutdown"
)

type executionState string

const (
	statePrepared     executionState = "PREPARED"
	stateRunning      executionState = "RUNNING"
	stateStopped      executionState = "STOPPED"
	stateOutputReady  executionState = "OUTPUT_READY"
	stateOutputSealed executionState = "OUTPUT_SEALED"
	stateFailed       executionState = "FAILED"
)

type Mode string

const (
	ModeSuccess Mode = "success"
	ModeFailure Mode = "failure"
	ModeHang    Mode = "hang"
)

type Config struct {
	Component string
	Mode      Mode
	Stdin     io.Reader
	Stdout    io.Writer
	Now       func() time.Time
}

type requestV1 struct {
	SchemaVersion int               `json:"schema_version"`
	RequestID     uint64            `json:"request_id"`
	Operation     operation         `json:"operation"`
	Initialize    *initializeV1     `json:"initialize,omitempty"`
	Probe         *probeRequestV1   `json:"probe,omitempty"`
	Prepare       *prepareRequestV1 `json:"prepare,omitempty"`
	Stage         *stageRequestV1   `json:"stage,omitempty"`
	Cancel        *cancelRequestV1  `json:"cancel,omitempty"`
}

type initializeV1 struct {
	WorkerInstanceID       string          `json:"worker_instance_id"`
	WorkerInstanceEpoch    int64           `json:"worker_instance_epoch"`
	WorkerMemberID         string          `json:"worker_member_id"`
	WorkerMemberEpoch      int64           `json:"worker_member_epoch"`
	DeviceSetDigest        string          `json:"device_set_digest"`
	Devices                []epochV1       `json:"devices"`
	MembershipDigest       string          `json:"membership_digest"`
	Members                []epochV1       `json:"members"`
	ModelResidencyID       string          `json:"model_residency_id"`
	RuntimeIdentity        string          `json:"runtime_identity"`
	ModelRuntimeEpoch      int64           `json:"model_runtime_epoch"`
	StageProfileRevisionID string          `json:"stage_profile_revision_id"`
	Component              string          `json:"component"`
	ModelComponentRevision string          `json:"model_component_revision"`
	LocalDevices           []localDeviceV1 `json:"local_devices"`
	ScratchRoot            string          `json:"scratch_root"`
	InputRoot              string          `json:"input_root"`
	OutputRoot             string          `json:"output_root"`
}

type epochV1 struct {
	ID    string `json:"id"`
	Epoch int64  `json:"epoch"`
}

type localDeviceV1 struct {
	DeviceID      string `json:"device_id"`
	DeviceEpoch   int64  `json:"device_epoch"`
	ResourceClass string `json:"resource_class,omitempty"`
	GPUUUID       string `json:"gpu_uuid,omitempty"`
	PCIBDF        string `json:"pci_bdf,omitempty"`
}

type probeRequestV1 struct {
	Check string `json:"check"`
}

type stageIdentityV1 struct {
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

type prepareRequestV1 struct {
	Identity      stageIdentityV1 `json:"identity"`
	ExecutionSpec []byte          `json:"execution_spec"`
}

type stageRequestV1 struct {
	Identity stageIdentityV1 `json:"identity"`
}

type cancelRequestV1 struct {
	Identity stageIdentityV1 `json:"identity"`
	Reason   string          `json:"reason"`
}

type responseV1 struct {
	SchemaVersion int            `json:"schema_version"`
	RequestID     uint64         `json:"request_id"`
	Acknowledged  bool           `json:"acknowledged,omitempty"`
	Initialized   bool           `json:"initialized,omitempty"`
	Probe         *probeResultV1 `json:"probe,omitempty"`
	Status        *statusV1      `json:"status,omitempty"`
	Output        *outputV1      `json:"output,omitempty"`
	Error         string         `json:"error,omitempty"`
}

type probeResultV1 struct {
	Ready    bool   `json:"ready"`
	Evidence []byte `json:"evidence"`
	Detail   string `json:"detail,omitempty"`
}

type statusV1 struct {
	State             executionState `json:"state"`
	Sequence          int64          `json:"sequence"`
	BackendStage      string         `json:"backend_stage"`
	Progress          *float64       `json:"progress,omitempty"`
	BoundedStatusJSON []byte         `json:"bounded_status_json"`
	Detail            string         `json:"detail,omitempty"`
	Failure           *failureV1     `json:"failure,omitempty"`
}

type failureV1 struct {
	FailureClass          string `json:"failure_class"`
	FailureFingerprint    []byte `json:"failure_fingerprint"`
	Detail                string `json:"detail"`
	WorkerReusable        bool   `json:"worker_reusable"`
	ConsumedResourceUnits int64  `json:"consumed_resource_units"`
	FailedAt              string `json:"failed_at"`
	RetryAt               string `json:"retry_at"`
}

type outputV1 struct {
	OutputManifestJSON []byte `json:"output_manifest_json"`
	TotalSizeBytes     int64  `json:"total_size_bytes"`
}

type session struct {
	component      string
	mode           Mode
	now            func() time.Time
	initialization *initializeV1
	active         *execution
}

type execution struct {
	identity         stageIdentityV1
	specification    []byte
	state            executionState
	sequence         int64
	manifest         []byte
	totalSize        int64
	failure          *failureV1
	outputPath       string
	parametersDigest string
	inputDigests     []string
	rootInputDigests []string
}

type mockPayloadV1 struct {
	Component        string   `json:"component"`
	InputSHA256      []string `json:"input_sha256"`
	ParametersSHA256 string   `json:"parameters_sha256"`
	RootInputSHA256  []string `json:"root_input_sha256"`
	SchemaVersion    int      `json:"schema_version"`
}

type localManifestV1 struct {
	SchemaVersion int            `json:"schema_version"`
	OutputPort    string         `json:"output_port"`
	LocalLocator  string         `json:"local_locator"`
	ContentType   string         `json:"content_type"`
	PayloadSHA256 string         `json:"payload_sha256"`
	SizeBytes     int64          `json:"size_bytes"`
	Lineage       localLineageV1 `json:"lineage"`
}

type localLineageV1 struct {
	AttemptID              string `json:"attempt_id"`
	StageRunID             string `json:"stage_run_id"`
	StageAttemptID         string `json:"stage_attempt_id"`
	StageLeaseID           string `json:"stage_lease_id"`
	AttemptFence           int64  `json:"attempt_fence"`
	StageFence             int64  `json:"stage_fence"`
	StageProfileRevisionID string `json:"stage_profile_revision_id"`
}

func Run(ctx context.Context, config Config) error {
	if ctx == nil || config.Stdin == nil || config.Stdout == nil {
		return errors.New("H3 Stage mock runtime is not configured")
	}
	if !validComponent(config.Component) || !validMode(config.Mode) {
		return errors.New("H3 Stage mock component or mode is invalid")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	runtime := &session{
		component: config.Component, mode: config.Mode, now: config.Now,
	}
	reader := bufio.NewReaderSize(config.Stdin, maximumMessageBytes+2)
	encoder := json.NewEncoder(config.Stdout)
	encoder.SetEscapeHTML(false)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := readRequestLine(reader)
		if err != nil {
			return err
		}
		request, err := decodeRequest(line)
		if err != nil {
			return err
		}
		response, shutdown := runtime.handle(request)
		if err := encoder.Encode(response); err != nil {
			return fmt.Errorf("write H3 Stage mock response: %w", err)
		}
		if shutdown {
			return nil
		}
	}
}

func readRequestLine(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) || len(line) > maximumMessageBytes+1 {
		return nil, errors.New("H3 Stage mock request exceeds message bound")
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("H3 Stage mock request stream ended before shutdown")
		}
		return nil, fmt.Errorf("read H3 Stage mock request: %w", err)
	}
	return line[:len(line)-1], nil
}

func decodeRequest(line []byte) (requestV1, error) {
	if len(line) == 0 || !json.Valid(line) {
		return requestV1{}, errors.New("H3 Stage mock request is not one JSON document")
	}
	if err := strictjson.RejectDuplicateKeys(line); err != nil {
		return requestV1{}, fmt.Errorf("decode H3 Stage mock request: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.DisallowUnknownFields()
	var request requestV1
	if err := decoder.Decode(&request); err != nil {
		return requestV1{}, fmt.Errorf("decode H3 Stage mock request: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return requestV1{}, err
	}
	if err := validateRequestShape(request); err != nil {
		return requestV1{}, err
	}
	return request, nil
}

func validateRequestShape(request requestV1) error {
	if request.SchemaVersion != protocolVersion || request.RequestID == 0 {
		return errors.New("H3 Stage mock request identity is invalid")
	}
	payloads := 0
	for _, present := range []bool{
		request.Initialize != nil, request.Probe != nil, request.Prepare != nil,
		request.Stage != nil, request.Cancel != nil,
	} {
		if present {
			payloads++
		}
	}
	wantPayload := request.Operation != operationShutdown
	if payloads != boolInt(wantPayload) {
		return errors.New("H3 Stage mock request fields do not match operation")
	}
	valid := (request.Operation == operationInitialize && request.Initialize != nil) ||
		(request.Operation == operationProbe && request.Probe != nil) ||
		(request.Operation == operationPrepare && request.Prepare != nil) ||
		((request.Operation == operationStart || request.Operation == operationStatus || request.Operation == operationSeal) && request.Stage != nil) ||
		(request.Operation == operationCancel && request.Cancel != nil) || request.Operation == operationShutdown
	if !valid {
		return errors.New("H3 Stage mock request operation is invalid")
	}
	return nil
}

func (runtime *session) handle(request requestV1) (responseV1, bool) {
	response := responseV1{SchemaVersion: protocolVersion, RequestID: request.RequestID}
	var err error
	shutdown := false
	switch request.Operation {
	case operationInitialize:
		err = runtime.initialize(request.Initialize)
		if err == nil {
			response.Acknowledged = true
			response.Initialized = true
		}
	case operationProbe:
		response.Probe, err = runtime.probe(request.Probe)
	case operationPrepare:
		err = runtime.prepare(request.Prepare)
		response.Acknowledged = err == nil
	case operationStart:
		err = runtime.start(request.Stage.Identity)
		response.Acknowledged = err == nil
	case operationStatus:
		response.Status, err = runtime.status(request.Stage.Identity)
	case operationCancel:
		err = runtime.cancel(request.Cancel)
		response.Acknowledged = err == nil
	case operationSeal:
		response.Output, err = runtime.seal(request.Stage.Identity)
	case operationShutdown:
		err = runtime.shutdown()
		response.Acknowledged = err == nil
		shutdown = true
	}
	if err != nil {
		response.Error = boundedError(err)
	}
	return response, shutdown
}

func (runtime *session) initialize(initialization *initializeV1) error {
	if runtime.initialization != nil {
		return errors.New("H3 Stage mock runtime is already initialized")
	}
	if initialization == nil || initialization.Component != runtime.component ||
		!canonicalUUID(initialization.WorkerInstanceID) || initialization.WorkerInstanceEpoch <= 0 ||
		!canonicalUUID(initialization.WorkerMemberID) || initialization.WorkerMemberEpoch <= 0 ||
		!canonicalUUID(initialization.ModelResidencyID) || initialization.ModelRuntimeEpoch <= 0 ||
		!canonicalUUID(initialization.StageProfileRevisionID) ||
		strings.TrimSpace(initialization.RuntimeIdentity) == "" ||
		strings.TrimSpace(initialization.ModelComponentRevision) == "" ||
		!canonicalDigest(initialization.DeviceSetDigest) ||
		!canonicalDigest(initialization.MembershipDigest) ||
		len(initialization.Devices) != 1 || len(initialization.Members) != 1 ||
		len(initialization.LocalDevices) != 1 {
		return errors.New("H3 Stage mock initialization is invalid")
	}
	for _, binding := range append(append([]epochV1(nil), initialization.Devices...), initialization.Members...) {
		if !canonicalUUID(binding.ID) || binding.Epoch <= 0 {
			return errors.New("H3 Stage mock initialization epoch binding is invalid")
		}
	}
	device := initialization.LocalDevices[0]
	if !canonicalUUID(device.DeviceID) || device.DeviceEpoch <= 0 ||
		(device.ResourceClass != "" && device.ResourceClass != "GPU") ||
		!gpuUUIDPattern.MatchString(device.GPUUUID) || !pciBDFPattern.MatchString(device.PCIBDF) ||
		initialization.Devices[0].ID != device.DeviceID ||
		initialization.Devices[0].Epoch != device.DeviceEpoch {
		return errors.New("H3 Stage mock local device is invalid")
	}
	if initialization.Members[0].ID != initialization.WorkerMemberID ||
		initialization.Members[0].Epoch != initialization.WorkerMemberEpoch {
		return errors.New("H3 Stage mock local member is invalid")
	}
	for _, root := range []string{initialization.ScratchRoot, initialization.InputRoot, initialization.OutputRoot} {
		if err := validatePrivateRoot(root); err != nil {
			return err
		}
	}
	if initialization.ScratchRoot == initialization.InputRoot ||
		initialization.ScratchRoot == initialization.OutputRoot ||
		initialization.InputRoot == initialization.OutputRoot ||
		!withinRoot(initialization.ScratchRoot, initialization.InputRoot) ||
		!withinRoot(initialization.ScratchRoot, initialization.OutputRoot) {
		return errors.New("H3 Stage mock runtime roots are invalid")
	}
	copy := *initialization
	runtime.initialization = &copy
	return nil
}

func (runtime *session) probe(request *probeRequestV1) (*probeResultV1, error) {
	if runtime.initialization == nil || request == nil || !validReadinessCheck(request.Check) {
		return nil, errors.New("H3 Stage mock readiness request is invalid")
	}
	return &probeResultV1{
		Ready: true, Evidence: []byte("vela-h3-stage-mock/v1:" + runtime.component + ":ready"),
		Detail: "mock resident runtime ready",
	}, nil
}

func (runtime *session) prepare(request *prepareRequestV1) error {
	if runtime.initialization == nil || request == nil {
		return errors.New("H3 Stage mock runtime is not initialized")
	}
	if err := validateStageIdentity(request.Identity, runtime.initialization.StageProfileRevisionID); err != nil {
		return err
	}
	if runtime.active != nil {
		if sameStageIdentity(runtime.active.identity, request.Identity) {
			if bytes.Equal(runtime.active.specification, request.ExecutionSpec) {
				return nil
			}
			return errors.New("H3 Stage mock authority cannot change execution specification")
		}
		if runtime.active.identity.StageAttemptID == request.Identity.StageAttemptID {
			return errors.New("H3 Stage mock StageAttempt cannot change authority")
		}
		if runtime.active.state != stateStopped && runtime.active.state != stateOutputSealed &&
			(runtime.active.state != stateFailed || runtime.active.failure == nil || !runtime.active.failure.WorkerReusable) {
			return errors.New("H3 Stage mock runtime already has an active stage")
		}
	}
	prepared, err := runtime.prepareExecution(request.Identity, request.ExecutionSpec)
	if err != nil {
		return err
	}
	runtime.active = prepared
	return nil
}

func (runtime *session) prepareExecution(identity stageIdentityV1, encoded []byte) (*execution, error) {
	if len(encoded) == 0 || len(encoded) > maximumExecutionBytes {
		return nil, errors.New("H3 Stage mock execution specification exceeds bounds")
	}
	var specification velav1.StageExecutionSpec
	if err := proto.Unmarshal(encoded, &specification); err != nil || len(specification.ProtoReflect().GetUnknown()) != 0 ||
		len(specification.GetInputs()) > maximumInputs || len(specification.GetRootInputs()) > maximumInputs {
		return nil, errors.New("H3 Stage mock execution specification is invalid")
	}
	if err := validateJSONObject(specification.GetParametersJson(), "parameters"); err != nil {
		return nil, err
	}
	expected, err := decodeJSONObject(specification.GetExpectedOutputManifestJson(), "expected output manifest")
	if err != nil {
		return nil, err
	}
	port, _, _ := componentOutput(runtime.component)
	if _, ok := expected[port]; !ok {
		return nil, errors.New("H3 Stage mock expected output manifest does not contain component port")
	}
	if (runtime.component == "ENCODER" && len(specification.GetInputs()) != 0) ||
		(runtime.component != "ENCODER" && (len(specification.GetInputs()) != 1 || len(specification.GetRootInputs()) != 0)) {
		return nil, errors.New("H3 Stage mock execution inputs do not match component")
	}
	inputDigests, err := validateStageInputs(runtime.initialization.InputRoot, identity.StageRunID, specification.GetInputs())
	if err != nil {
		return nil, err
	}
	rootDigests, err := validateRootInputs(runtime.initialization.InputRoot, identity.StageRunID, specification.GetRootInputs())
	if err != nil {
		return nil, err
	}
	parametersDigest := sha256.Sum256(specification.GetParametersJson())
	return &execution{
		identity: identity, specification: append([]byte(nil), encoded...), state: statePrepared, sequence: 1,
		parametersDigest: hex.EncodeToString(parametersDigest[:]), inputDigests: inputDigests,
		rootInputDigests: rootDigests,
	}, nil
}

func (runtime *session) start(identity stageIdentityV1) error {
	active, err := runtime.requireActive(identity)
	if err != nil {
		return err
	}
	if active.state == stateRunning || active.state == stateOutputReady || active.state == stateOutputSealed || active.state == stateFailed {
		return nil
	}
	if active.state != statePrepared {
		return errors.New("H3 Stage mock runtime stage is not prepared")
	}
	active.state = stateRunning
	active.sequence++
	switch runtime.mode {
	case ModeHang:
		return nil
	case ModeFailure:
		runtime.fail(active, "MOCK_INJECTED_FAILURE", "mock stage execution failed as configured")
		return nil
	default:
		if err := runtime.publish(active); err != nil {
			runtime.fail(active, "MOCK_OUTPUT_PUBLICATION_FAILED", "mock output publication failed")
			return err
		}
		active.state = stateOutputReady
		active.sequence++
		return nil
	}
}

func (runtime *session) status(identity stageIdentityV1) (*statusV1, error) {
	active, err := runtime.requireActive(identity)
	if err != nil {
		return nil, err
	}
	progress := 0.0
	detail := "mock component prepared"
	if active.state == stateRunning {
		detail = "mock component executing"
	}
	if active.state == stateOutputReady || active.state == stateOutputSealed {
		progress = 1
		detail = "mock component output ready"
	}
	if active.state == stateFailed {
		detail = "mock component execution failed"
	}
	if active.state == stateStopped {
		detail = "mock component execution stopped"
	}
	bounded, _ := json.Marshal(struct {
		Component string `json:"component"`
		Mode      Mode   `json:"mode"`
	}{Component: runtime.component, Mode: runtime.mode})
	return &statusV1{
		State: active.state, Sequence: active.sequence,
		BackendStage: "mock/" + strings.ToLower(runtime.component), Progress: &progress,
		BoundedStatusJSON: bounded, Detail: detail, Failure: active.failure,
	}, nil
}

func (runtime *session) cancel(request *cancelRequestV1) error {
	if request == nil || !validCancelReason(request.Reason) {
		return errors.New("H3 Stage mock cancellation request is invalid")
	}
	active, err := runtime.requireActive(request.Identity)
	if err != nil {
		return err
	}
	if active.state == stateOutputSealed {
		return errors.New("sealed H3 Stage mock output cannot be canceled")
	}
	if active.state == stateStopped {
		return nil
	}
	if err := runtime.discardUnsealedOutput(active); err != nil {
		return err
	}
	active.state = stateStopped
	active.sequence++
	return nil
}

func (runtime *session) seal(identity stageIdentityV1) (*outputV1, error) {
	active, err := runtime.requireActive(identity)
	if err != nil {
		return nil, err
	}
	if (active.state != stateOutputReady && active.state != stateOutputSealed) || len(active.manifest) == 0 {
		return nil, errors.New("H3 Stage mock output is not ready to seal")
	}
	if active.state == stateOutputReady {
		active.state = stateOutputSealed
		active.sequence++
	}
	return &outputV1{OutputManifestJSON: append([]byte(nil), active.manifest...), TotalSizeBytes: active.totalSize}, nil
}

func (runtime *session) shutdown() error {
	if runtime.active == nil || runtime.active.state == stateOutputSealed {
		return nil
	}
	if err := runtime.discardUnsealedOutput(runtime.active); err != nil {
		return err
	}
	if runtime.active.state == stateRunning || runtime.active.state == stateOutputReady {
		runtime.active.state = stateStopped
		runtime.active.sequence++
	}
	return nil
}

func (runtime *session) publish(active *execution) error {
	port, contentType, _ := componentOutput(runtime.component)
	descriptor, err := json.Marshal(mockPayloadV1{
		Component: runtime.component, InputSHA256: active.inputDigests,
		ParametersSHA256: active.parametersDigest, RootInputSHA256: active.rootInputDigests,
		SchemaVersion: 1,
	})
	if err != nil {
		return fmt.Errorf("encode H3 Stage mock output: %w", err)
	}
	descriptor = append(descriptor, '\n')
	payload := descriptor
	if runtime.component == "VAE_DECODER" {
		fixture, err := h3mockbackend.ReadVideoFixture()
		if err != nil {
			return err
		}
		payload = append(fixture, []byte("\nVELA-H3-STAGE-MOCK-V1:")...)
		digest := sha256.Sum256(descriptor)
		payload = append(payload, hex.EncodeToString(digest[:])...)
		payload = append(payload, '\n')
	}
	digest := sha256.Sum256(payload)
	finalDirectory := filepath.Join(runtime.initialization.OutputRoot, active.identity.StageAttemptID)
	finalPath := filepath.Join(finalDirectory, port+".bin")
	manifest, err := json.Marshal(localManifestV1{
		SchemaVersion: 1, OutputPort: port,
		LocalLocator: filepath.ToSlash(filepath.Join(active.identity.StageAttemptID, port+".bin")),
		ContentType:  contentType, PayloadSHA256: hex.EncodeToString(digest[:]), SizeBytes: int64(len(payload)),
		Lineage: localLineageV1{
			AttemptID: active.identity.AttemptID, StageRunID: active.identity.StageRunID,
			StageAttemptID: active.identity.StageAttemptID, StageLeaseID: active.identity.StageLeaseID,
			AttemptFence: active.identity.AttemptFence, StageFence: active.identity.StageFence,
			StageProfileRevisionID: active.identity.StageProfileRevisionID,
		},
	})
	if err != nil {
		return fmt.Errorf("encode H3 Stage mock output manifest: %w", err)
	}
	stagingDirectory := filepath.Join(runtime.initialization.OutputRoot, ".staging", active.identity.StageAttemptID)
	if err := os.MkdirAll(stagingDirectory, 0o700); err != nil {
		return fmt.Errorf("create H3 Stage mock staging directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(stagingDirectory) }()
	stagingPath := filepath.Join(stagingDirectory, port+".partial")
	file, err := os.OpenFile(stagingPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create H3 Stage mock output: %w", err)
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return fmt.Errorf("write H3 Stage mock output: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync H3 Stage mock output: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close H3 Stage mock output: %w", err)
	}
	if err := os.Mkdir(finalDirectory, 0o700); err != nil {
		return fmt.Errorf("create H3 Stage mock final directory: %w", err)
	}
	if err := os.Rename(stagingPath, finalPath); err != nil {
		_ = os.Remove(finalDirectory)
		return fmt.Errorf("publish H3 Stage mock output: %w", err)
	}
	active.manifest = manifest
	active.totalSize = int64(len(payload))
	active.outputPath = finalPath
	return nil
}

func (runtime *session) fail(active *execution, failureClass, detail string) {
	now := runtime.now().UTC()
	fingerprint := sha256.Sum256([]byte(failureClass + "\x00" + runtime.component))
	active.failure = &failureV1{
		FailureClass: failureClass, FailureFingerprint: fingerprint[:], Detail: detail,
		WorkerReusable: true, ConsumedResourceUnits: 1,
		FailedAt: now.Format(time.RFC3339Nano), RetryAt: now.Add(5 * time.Second).Format(time.RFC3339Nano),
	}
	active.state = stateFailed
	active.sequence++
}

func (runtime *session) discardUnsealedOutput(active *execution) error {
	if active == nil || active.state == stateOutputSealed || active.outputPath == "" {
		return nil
	}
	expectedDirectory := filepath.Join(runtime.initialization.OutputRoot, active.identity.StageAttemptID)
	expectedPath := filepath.Join(expectedDirectory, componentFileName(runtime.component))
	if active.outputPath != expectedPath {
		return errors.New("H3 Stage mock unsealed output path is invalid")
	}
	if err := os.Remove(expectedPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove unsealed H3 Stage mock output: %w", err)
	}
	if err := os.Remove(expectedDirectory); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove unsealed H3 Stage mock output directory: %w", err)
	}
	active.manifest = nil
	active.totalSize = 0
	active.outputPath = ""
	return nil
}

func componentFileName(component string) string {
	port, _, _ := componentOutput(component)
	return port + ".bin"
}

func validateStageInputs(root, stageRunID string, inputs []*velav1.StageInputArtifact) ([]string, error) {
	digests := make([]string, 0, len(inputs))
	seen := make(map[string]struct{}, len(inputs))
	for _, input := range inputs {
		if input == nil || !canonicalUUID(input.GetStageArtifactId()) ||
			!canonicalUUID(input.GetStageInterfaceRevisionId()) || strings.TrimSpace(input.GetObjectVersion()) == "" ||
			len(input.GetSha256()) != sha256.Size || input.GetSizeBytes() <= 0 {
			return nil, errors.New("H3 Stage mock input Artifact is invalid")
		}
		key := input.GetStageArtifactId() + "\x00" + input.GetObjectVersion()
		if _, duplicate := seen[key]; duplicate {
			return nil, errors.New("H3 Stage mock input Artifact is duplicated")
		}
		seen[key] = struct{}{}
		digest := hex.EncodeToString(input.GetSha256())
		path := filepath.Join(root, "stage-runs", stageRunID, "inputs", input.GetStageArtifactId(), digest+".bin")
		if err := verifyInput(path, input.GetSha256(), input.GetSizeBytes()); err != nil {
			return nil, err
		}
		digests = append(digests, digest)
	}
	return digests, nil
}

func validateRootInputs(root, stageRunID string, inputs []*velav1.StageRootInputMaterial) ([]string, error) {
	digests := make([]string, 0, len(inputs))
	for index, input := range inputs {
		if input == nil || input.GetConditionIndex() != int32(index) || strings.TrimSpace(input.GetUri()) == "" ||
			len(input.GetUri()) > 4096 || len(input.GetSha256()) != sha256.Size || input.GetSizeBytes() <= 0 {
			return nil, errors.New("H3 Stage mock root input is invalid")
		}
		digest := hex.EncodeToString(input.GetSha256())
		path := filepath.Join(root, "stage-runs", stageRunID, "root-inputs", fmt.Sprint(index), digest+".bin")
		if err := verifyInput(path, input.GetSha256(), input.GetSizeBytes()); err != nil {
			return nil, err
		}
		digests = append(digests, digest)
	}
	return digests, nil
}

func verifyInput(path string, expected []byte, size int64) error {
	pathInformation, err := os.Lstat(path)
	if err != nil || !pathInformation.Mode().IsRegular() || pathInformation.Size() != size {
		return errors.New("H3 Stage mock input is not an exact regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return errors.New("H3 Stage mock input is unavailable")
	}
	defer func() { _ = file.Close() }()
	openedInformation, err := file.Stat()
	if err != nil || !openedInformation.Mode().IsRegular() || openedInformation.Size() != size ||
		!os.SameFile(pathInformation, openedInformation) {
		return errors.New("H3 Stage mock input changed while opening exact regular file")
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil || !bytes.Equal(digest.Sum(nil), expected) {
		return errors.New("H3 Stage mock input digest is mismatched")
	}
	return nil
}

func (runtime *session) requireActive(identity stageIdentityV1) (*execution, error) {
	if runtime.initialization == nil {
		return nil, errors.New("H3 Stage mock runtime is not initialized")
	}
	if err := validateStageIdentity(identity, runtime.initialization.StageProfileRevisionID); err != nil {
		return nil, err
	}
	if runtime.active != nil && sameStageIdentity(runtime.active.identity, identity) {
		return runtime.active, nil
	}
	return nil, errors.New("H3 Stage mock identity does not match active execution")
}

func validateStageIdentity(identity stageIdentityV1, profile string) error {
	if !canonicalDigest(identity.AuthorityDigest) || !canonicalUUID(identity.JobID) ||
		!canonicalUUID(identity.AttemptID) || !canonicalUUID(identity.StageRunID) ||
		!canonicalUUID(identity.StageAttemptID) || !canonicalUUID(identity.StageLeaseID) ||
		identity.AttemptFence <= 0 || identity.StageFence <= 0 || identity.StageVersion <= 0 ||
		identity.StageProfileRevisionID != profile {
		return errors.New("H3 Stage mock lineage identity is invalid")
	}
	return nil
}

func validatePrivateRoot(root string) error {
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return errors.New("H3 Stage mock runtime root is invalid")
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != root {
		return errors.New("H3 Stage mock runtime root is not canonical")
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
		return errors.New("H3 Stage mock runtime root is not a private directory")
	}
	return nil
}

func withinRoot(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func validateJSONObject(encoded []byte, name string) error {
	_, err := decodeJSONObject(encoded, name)
	return err
}

func decodeJSONObject(encoded []byte, name string) (map[string]json.RawMessage, error) {
	if len(encoded) == 0 {
		encoded = []byte("{}")
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return nil, fmt.Errorf("H3 Stage mock %s is invalid: %w", name, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var object map[string]json.RawMessage
	if err := decoder.Decode(&object); err != nil || object == nil {
		return nil, fmt.Errorf("H3 Stage mock %s is not a JSON object", name)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, err
	}
	return object, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("H3 Stage mock JSON contains trailing data")
	}
	return nil
}

func componentOutput(component string) (string, string, bool) {
	switch component {
	case "ENCODER":
		return "conditioning", "application/x-minimax-h3-encoder", true
	case "DIT":
		return "latent", "application/x-minimax-h3-latent", true
	case "VAE_DECODER":
		return "video", "video/mp4", true
	default:
		return "", "", false
	}
}

func validComponent(component string) bool {
	_, _, ok := componentOutput(component)
	return ok
}

func validMode(mode Mode) bool {
	return mode == ModeSuccess || mode == ModeFailure || mode == ModeHang
}

func validReadinessCheck(check string) bool {
	switch check {
	case "MODEL_RUNTIME_READINESS_CHECK_DEVICE",
		"MODEL_RUNTIME_READINESS_CHECK_BACKEND",
		"MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP",
		"MODEL_RUNTIME_READINESS_CHECK_CANARY":
		return true
	default:
		return false
	}
}

func validCancelReason(reason string) bool {
	switch reason {
	case "MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP",
		"MODEL_RUNTIME_CANCEL_REASON_MONOTONIC_DEADLINE",
		"MODEL_RUNTIME_CANCEL_REASON_AGENT_SHUTDOWN",
		"MODEL_RUNTIME_CANCEL_REASON_MEMBER_BARRIER_FAILED":
		return true
	default:
		return false
	}
}

func canonicalUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}

func canonicalDigest(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && !bytes.Equal(decoded, make([]byte, sha256.Size))
}

func sameStageIdentity(left, right stageIdentityV1) bool {
	return left == right
}

func boundedError(err error) string {
	message := err.Error()
	if len(message) > 400 {
		return message[:400]
	}
	return message
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
