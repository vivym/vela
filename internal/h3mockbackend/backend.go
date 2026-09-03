package h3mockbackend

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/vivym/vela/internal/strictjson"
)

const maxJSONBytes = 1 << 20

var (
	uuidPattern               = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	gpuPattern                = regexp.MustCompile(`^GPU-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	failureClassPattern       = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,99}$`)
	failureFingerprintPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$`)
	backendStagePattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,99}$`)
)

var ErrInjectedFailure = errors.New("mock backend injected failure")

// ReadVideoFixture returns the bounded media payload used by non-production
// mock backends that must exercise the final video validation path.
func ReadVideoFixture() ([]byte, error) {
	return mediaFixtures.ReadFile("testdata/video-1080p-5s-24fps.mp4")
}

//go:embed testdata/video-1080p-5s-24fps.mp4 testdata/thumbnail-320x180.webp
var mediaFixtures embed.FS

type readinessRequest struct {
	SchemaVersion              int    `json:"schema_version"`
	CycleID                    string `json:"cycle_id"`
	WorkerID                   string `json:"worker_id"`
	WorkerEpoch                int64  `json:"worker_epoch"`
	NodeIdentity               string `json:"node_identity"`
	ExecutionProfileRevisionID string `json:"execution_profile_revision_id"`
	InferenceBackendRevision   string `json:"inference_backend_revision"`
	Deadline                   string `json:"deadline"`
	Check                      string `json:"check"`
}

type deviceReadinessResult struct {
	SchemaVersion     int      `json:"schema_version"`
	Check             string   `json:"check"`
	Passed            bool     `json:"passed"`
	EncoderVAEGPUUUID string   `json:"encoder_vae_gpu_uuid"`
	DiTGPUUUIDs       []string `json:"dit_gpu_uuids"`
}

type backendReadinessResult struct {
	SchemaVersion            int    `json:"schema_version"`
	Check                    string `json:"check"`
	Passed                   bool   `json:"passed"`
	InferenceBackendRevision string `json:"inference_backend_revision"`
	Loaded                   bool   `json:"loaded"`
}

type warmupReadinessResult struct {
	SchemaVersion              int    `json:"schema_version"`
	Check                      string `json:"check"`
	Passed                     bool   `json:"passed"`
	ExecutionProfileRevisionID string `json:"execution_profile_revision_id"`
	Warmed                     bool   `json:"warmed"`
}

type canaryReadinessResult struct {
	SchemaVersion int    `json:"schema_version"`
	Check         string `json:"check"`
	Passed        bool   `json:"passed"`
	OutputSHA256  string `json:"output_sha256"`
}

type executionRequest struct {
	SchemaVersion int             `json:"schema_version"`
	Identity      attemptIdentity `json:"identity"`
	ExecutionSpec executionSpec   `json:"execution_spec"`
}

type attemptIdentity struct {
	AttemptID   string `json:"attempt_id"`
	JobID       string `json:"job_id"`
	WorkerID    string `json:"worker_id"`
	WorkerEpoch int64  `json:"worker_epoch"`
	LeaseFence  int64  `json:"lease_fence"`
}

type executionSpec struct {
	ModelRevisionID            string          `json:"model_revision_id"`
	GenerationPresetRevisionID string          `json:"generation_preset_revision_id"`
	ExecutionProfileRevisionID string          `json:"execution_profile_revision_id"`
	OutputSpecID               string          `json:"output_spec_id"`
	RequestContentBase64       string          `json:"request_content_base64"`
	DebugDumpAuthorization     json.RawMessage `json:"debug_dump_authorization,omitempty"`
}

type debugDumpAuthorization struct {
	AuthorizationID string            `json:"authorization_id"`
	ExpiresAt       protobufTimestamp `json:"expires_at"`
}

type protobufTimestamp struct {
	Seconds int64 `json:"seconds"`
	Nanos   int32 `json:"nanos"`
}

type backendStatus struct {
	SchemaVersion             int     `json:"schema_version"`
	BackendStage              string  `json:"backend_stage"`
	Sequence                  int64   `json:"sequence"`
	BackendStageProgress      float64 `json:"backend_stage_progress"`
	EstimatedRemainingSeconds int64   `json:"estimated_remaining_seconds"`
}

type outputManifest struct {
	SchemaVersion int                   `json:"schema_version"`
	Outputs       []outputManifestEntry `json:"outputs"`
}

type outputManifestEntry struct {
	Kind        string `json:"kind"`
	Ordinal     int    `json:"ordinal"`
	Path        string `json:"path"`
	ContentType string `json:"content_type"`
}

type failureReceipt struct {
	SchemaVersion      int      `json:"schema_version"`
	FailureClass       string   `json:"failure_class"`
	FailureFingerprint string   `json:"failure_fingerprint"`
	ErrorSummary       string   `json:"error_summary"`
	BackendStage       string   `json:"backend_stage"`
	GPUUUIDs           []string `json:"gpu_uuids"`
	RetryRecommended   bool     `json:"retry_recommended"`
	WorkerReusable     bool     `json:"worker_reusable"`
}

// Run executes the versioned file protocol used by the Vela H3 Runner.
func Run(ctx context.Context, arguments []string) error {
	if ctx == nil {
		return errors.New("mock backend context is required")
	}
	flags := flag.NewFlagSet("vela-h3-mock-backend", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	readinessCheck := flags.String("vela-readiness-check", "", "")
	readinessRequestPath := flags.String("vela-readiness-request", "", "")
	readinessResultPath := flags.String("vela-readiness-result", "", "")
	requestPath := flags.String("vela-request", "", "")
	outputDir := flags.String("vela-output-dir", "", "")
	statusPath := flags.String("vela-status", "", "")
	manifestPath := flags.String("vela-output-manifest", "", "")
	failurePath := flags.String("vela-failure", "", "")
	resume := flags.String("vela-resume", "", "")
	stageDelayValue := flags.String("mock-stage-delay", "250ms", "")
	outputSpecID := flags.String("mock-output-spec-id", "", "")
	mode := flags.String("mock-mode", "success", "")
	failureClass := flags.String("mock-failure-class", "TRANSIENT_BACKEND", "")
	failureFingerprint := flags.String("mock-failure-fingerprint", "mock/transient/backend", "")
	failureStage := flags.String("mock-failure-stage", "mock/encode", "")
	failureGPUIndex := flags.Int("mock-failure-gpu-index", -1, "")
	retryRecommendedValue := flags.String("mock-retry-recommended", "true", "")
	workerReusableValue := flags.String("mock-worker-reusable", "true", "")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 {
		return errors.New("mock backend arguments are invalid")
	}
	readinessMode := *readinessCheck != "" || *readinessRequestPath != "" || *readinessResultPath != ""
	executionMode := *requestPath != "" || *outputDir != "" || *statusPath != "" ||
		*manifestPath != "" || *failurePath != "" || *resume != ""
	if readinessMode == executionMode {
		return errors.New("exactly one complete mock backend mode is required")
	}
	if readinessMode {
		if *readinessCheck == "" || *readinessRequestPath == "" || *readinessResultPath == "" {
			return errors.New("complete readiness arguments are required")
		}
		return runReadiness(ctx, *readinessCheck, *readinessRequestPath, *readinessResultPath)
	}
	if *requestPath == "" || *outputDir == "" || *statusPath == "" || *manifestPath == "" ||
		*failurePath == "" || (*resume != "true" && *resume != "false") ||
		!uuidPattern.MatchString(*outputSpecID) {
		return errors.New("complete execution arguments are required")
	}
	stageDelay, err := time.ParseDuration(*stageDelayValue)
	if err != nil || stageDelay < 0 || stageDelay > time.Minute {
		return errors.New("mock stage delay must be in [0s, 1m]")
	}
	retryRecommended, err := strictBool(*retryRecommendedValue)
	if err != nil {
		return err
	}
	workerReusable, err := strictBool(*workerReusableValue)
	if err != nil {
		return err
	}
	if *mode != "success" && *mode != "failure" && *mode != "hang" {
		return errors.New("mock mode must be success, failure, or hang")
	}
	if *failureGPUIndex < -1 || *failureGPUIndex > 7 ||
		!failureClassPattern.MatchString(*failureClass) ||
		!failureFingerprintPattern.MatchString(*failureFingerprint) ||
		!backendStagePattern.MatchString(*failureStage) {
		return errors.New("mock failure configuration is invalid")
	}
	return runExecution(ctx, executionArguments{
		requestPath:  *requestPath,
		outputDir:    *outputDir,
		statusPath:   *statusPath,
		manifestPath: *manifestPath,
		failurePath:  *failurePath,
		resume:       *resume == "true",
		stageDelay:   stageDelay,
		outputSpecID: *outputSpecID,
		mode:         *mode,
		failure: failureReceipt{
			SchemaVersion:      1,
			FailureClass:       *failureClass,
			FailureFingerprint: *failureFingerprint,
			ErrorSummary:       "mock backend injected a configured failure",
			BackendStage:       *failureStage,
			GPUUUIDs:           []string{},
			RetryRecommended:   retryRecommended,
			WorkerReusable:     workerReusable,
		},
		failureGPUIndex: *failureGPUIndex,
	})
}

type executionArguments struct {
	requestPath     string
	outputDir       string
	statusPath      string
	manifestPath    string
	failurePath     string
	resume          bool
	stageDelay      time.Duration
	outputSpecID    string
	mode            string
	failure         failureReceipt
	failureGPUIndex int
}

func runExecution(ctx context.Context, arguments executionArguments) error {
	request := executionRequest{}
	if err := readStrictJSON(arguments.requestPath, &request); err != nil {
		return fmt.Errorf("read execution request: %w", err)
	}
	if err := validateExecutionRequest(request); err != nil {
		return err
	}
	if request.ExecutionSpec.OutputSpecID != arguments.outputSpecID {
		return errors.New("mock execution request does not match configured OutputSpec")
	}
	if err := validateResultPaths(
		arguments.requestPath,
		arguments.statusPath,
		arguments.manifestPath,
		arguments.failurePath,
	); err != nil {
		return err
	}
	if err := validateOutputDirectory(arguments.outputDir); err != nil {
		return err
	}
	gpus, err := runtimeGPUUUIDs()
	if err != nil {
		return err
	}
	if arguments.mode == "hang" {
		if err := writeJSONAtomic(arguments.statusPath, backendStatus{
			SchemaVersion:             1,
			BackendStage:              "mock/hang",
			Sequence:                  1,
			BackendStageProgress:      0.25,
			EstimatedRemainingSeconds: 3600,
		}); err != nil {
			return fmt.Errorf("write hanging mock backend status: %w", err)
		}
		<-ctx.Done()
		return ctx.Err()
	}
	stages := []backendStatus{
		{SchemaVersion: 1, BackendStage: "mock/prepare", Sequence: 1, BackendStageProgress: 0.10, EstimatedRemainingSeconds: 1},
		{SchemaVersion: 1, BackendStage: "mock/encode", Sequence: 2, BackendStageProgress: 0.50, EstimatedRemainingSeconds: 1},
		{SchemaVersion: 1, BackendStage: "mock/package", Sequence: 3, BackendStageProgress: 0.85, EstimatedRemainingSeconds: 0},
		{SchemaVersion: 1, BackendStage: "mock/finalize", Sequence: 4, BackendStageProgress: 0.95, EstimatedRemainingSeconds: 0},
	}
	for _, status := range stages[:3] {
		if err := writeJSONAtomic(arguments.statusPath, status); err != nil {
			return fmt.Errorf("write mock backend status: %w", err)
		}
		if err := waitContext(ctx, arguments.stageDelay); err != nil {
			return err
		}
		if arguments.mode == "failure" && status.Sequence == 2 {
			failure := arguments.failure
			if arguments.failureGPUIndex >= 0 {
				failure.GPUUUIDs = []string{gpus[arguments.failureGPUIndex]}
			}
			if err := writeJSONAtomic(arguments.failurePath, failure); err != nil {
				return fmt.Errorf("write mock failure receipt: %w", err)
			}
			return ErrInjectedFailure
		}
	}
	videoPath := filepath.Join(arguments.outputDir, "video.mp4")
	thumbnailPath := filepath.Join(arguments.outputDir, "thumbnail.webp")
	if err := writeEmbeddedFile(
		videoPath, "testdata/video-1080p-5s-24fps.mp4", arguments.resume,
	); err != nil {
		return fmt.Errorf("write mock video: %w", err)
	}
	if err := writeEmbeddedFile(
		thumbnailPath, "testdata/thumbnail-320x180.webp", arguments.resume,
	); err != nil {
		return fmt.Errorf("write mock thumbnail: %w", err)
	}
	manifest := outputManifest{
		SchemaVersion: 1,
		Outputs: []outputManifestEntry{
			{Kind: "VIDEO", Ordinal: 0, Path: videoPath, ContentType: "video/mp4"},
			{Kind: "THUMBNAIL", Ordinal: 0, Path: thumbnailPath, ContentType: "image/webp"},
		},
	}
	if err := writeJSONAtomic(arguments.manifestPath, manifest); err != nil {
		return fmt.Errorf("write mock output manifest: %w", err)
	}
	if err := writeJSONAtomic(arguments.statusPath, stages[3]); err != nil {
		return fmt.Errorf("write final mock backend status: %w", err)
	}
	return nil
}

func strictBool(value string) (bool, error) {
	switch value {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, errors.New("mock boolean configuration must be true or false")
	}
}

func runtimeGPUUUIDs() ([]string, error) {
	gpus := strings.Split(os.Getenv("CUDA_VISIBLE_DEVICES"), ",")
	if len(gpus) != 8 || !allUniqueGPUs(gpus) {
		return nil, errors.New("CUDA_VISIBLE_DEVICES must contain eight unique canonical GPU UUIDs")
	}
	return gpus, nil
}

func validateExecutionRequest(request executionRequest) error {
	identity := request.Identity
	spec := request.ExecutionSpec
	if request.SchemaVersion != 1 || !uuidPattern.MatchString(identity.AttemptID) ||
		!uuidPattern.MatchString(identity.JobID) || !uuidPattern.MatchString(identity.WorkerID) ||
		identity.WorkerEpoch <= 0 || identity.LeaseFence <= 0 ||
		!uuidPattern.MatchString(spec.ModelRevisionID) ||
		!uuidPattern.MatchString(spec.GenerationPresetRevisionID) ||
		!uuidPattern.MatchString(spec.ExecutionProfileRevisionID) ||
		!uuidPattern.MatchString(spec.OutputSpecID) {
		return errors.New("mock execution request identity is invalid")
	}
	content, err := base64.StdEncoding.Strict().DecodeString(spec.RequestContentBase64)
	if err != nil || base64.StdEncoding.EncodeToString(content) != spec.RequestContentBase64 ||
		len(content) == 0 || len(content) > 64<<10 {
		return errors.New("mock request content encoding is invalid")
	}
	if err := strictjson.RejectDuplicateKeys(content); err != nil {
		return errors.New("mock request content must be one unambiguous JSON object")
	}
	if err := validateDebugDumpAuthorization(spec.DebugDumpAuthorization); err != nil {
		return err
	}
	var requestContent map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	if err := decoder.Decode(&requestContent); err != nil || requestContent == nil {
		return errors.New("mock request content must be one JSON object")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("mock request content must be one JSON object")
	}
	return nil
}

func validateDebugDumpAuthorization(encoded json.RawMessage) error {
	if len(encoded) == 0 {
		return nil
	}
	authorization := debugDumpAuthorization{}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&authorization); err != nil {
		return fmt.Errorf("mock debug dump authorization is invalid: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("mock debug dump authorization must contain exactly one document")
	}
	if !uuidPattern.MatchString(authorization.AuthorizationID) ||
		!validProtobufTimestamp(authorization.ExpiresAt) {
		return errors.New("mock debug dump authorization is invalid")
	}
	return nil
}

func validProtobufTimestamp(value protobufTimestamp) bool {
	const (
		minimumSeconds = -62_135_596_800
		maximumSeconds = 253_402_300_799
	)
	return value.Seconds >= minimumSeconds && value.Seconds <= maximumSeconds &&
		value.Nanos >= 0 && value.Nanos < 1_000_000_000 &&
		(value.Seconds != 0 || value.Nanos != 0)
}

func validateOutputDirectory(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("mock output directory must be canonical and absolute")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	ownerUID, err := currentProcessUID()
	if err != nil {
		return err
	}
	return validateOutputDirectoryInfo(info, ownerUID)
}

func validateOutputDirectoryInfo(info os.FileInfo, ownerUID uint32) error {
	actualOwnerUID, err := filesystemOwnerUID(info)
	if err != nil || actualOwnerUID != ownerUID || !info.IsDir() ||
		info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return errors.New("mock output directory must be a private current-owner directory")
	}
	return nil
}

func validateResultPaths(requestPath string, resultPaths ...string) error {
	requestDirectory := filepath.Dir(requestPath)
	if err := validateOutputDirectory(requestDirectory); err != nil {
		return fmt.Errorf("mock result directory must be a private current-owner directory: %w", err)
	}
	seen := map[string]struct{}{requestPath: {}}
	for _, path := range resultPaths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path ||
			filepath.Dir(path) != requestDirectory {
			return errors.New("mock result files must be direct children of the Runner-owned request directory")
		}
		if _, exists := seen[path]; exists {
			return errors.New("mock result file paths must be unique")
		}
		seen[path] = struct{}{}
	}
	return nil
}

func waitContext(ctx context.Context, duration time.Duration) error {
	if duration == 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func writeEmbeddedFile(path, fixture string, replace bool) error {
	content, err := mediaFixtures.ReadFile(fixture)
	if err != nil {
		return err
	}
	if replace {
		return replaceFileAtomic(path, content)
	}
	destination, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := destination.Write(content); err != nil {
		_ = destination.Close()
		return err
	}
	if err := destination.Chmod(0o600); err != nil {
		_ = destination.Close()
		return err
	}
	if err := destination.Sync(); err != nil {
		_ = destination.Close()
		return err
	}
	return destination.Close()
}

func replaceFileAtomic(path string, content []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".vela-h3-mock-output-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(content); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func runReadiness(ctx context.Context, check, requestPath, resultPath string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	request := readinessRequest{}
	if err := readStrictJSON(requestPath, &request); err != nil {
		return fmt.Errorf("read readiness request: %w", err)
	}
	if err := validateResultPaths(requestPath, resultPath); err != nil {
		return err
	}
	revision := os.Getenv("VELA_RUNNER_BACKEND_REVISION")
	if request.SchemaVersion != 1 || !uuidPattern.MatchString(request.CycleID) ||
		!uuidPattern.MatchString(request.WorkerID) || request.WorkerEpoch <= 0 ||
		request.NodeIdentity == "" || !uuidPattern.MatchString(request.ExecutionProfileRevisionID) ||
		revision == "" || request.InferenceBackendRevision != revision ||
		request.Deadline == "" || request.Check != check {
		return errors.New("readiness request identity is invalid")
	}
	gpus, err := runtimeGPUUUIDs()
	if err != nil {
		return err
	}
	switch check {
	case "DEVICE":
		return writeJSONAtomic(resultPath, deviceReadinessResult{
			SchemaVersion:     1,
			Check:             "DEVICE",
			Passed:            true,
			EncoderVAEGPUUUID: gpus[0],
			DiTGPUUUIDs:       append([]string(nil), gpus[1:]...),
		})
	case "INFERENCE_BACKEND":
		return writeJSONAtomic(resultPath, backendReadinessResult{
			SchemaVersion:            1,
			Check:                    "INFERENCE_BACKEND",
			Passed:                   true,
			InferenceBackendRevision: revision,
			Loaded:                   true,
		})
	case "MODEL_WARMUP":
		return writeJSONAtomic(resultPath, warmupReadinessResult{
			SchemaVersion:              1,
			Check:                      "MODEL_WARMUP",
			Passed:                     true,
			ExecutionProfileRevisionID: request.ExecutionProfileRevisionID,
			Warmed:                     true,
		})
	case "CANARY":
		return writeJSONAtomic(resultPath, canaryReadinessResult{
			SchemaVersion: 1,
			Check:         "CANARY",
			Passed:        true,
			OutputSHA256:  "159936091c96631fa42e0802a4f47a7236770faacfda2d961e51de7d7a85f2ef",
		})
	default:
		return errors.New("readiness check is unsupported")
	}
}

func allUniqueGPUs(gpus []string) bool {
	seen := make(map[string]struct{}, len(gpus))
	for _, gpu := range gpus {
		if !gpuPattern.MatchString(gpu) {
			return false
		}
		if _, exists := seen[gpu]; exists {
			return false
		}
		seen[gpu] = struct{}{}
	}
	return true
}

func readStrictJSON(path string, destination any) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("JSON path must be canonical and absolute")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	ownerUID, err := currentProcessUID()
	if err != nil {
		return err
	}
	if err := validateJSONInputInfo(info, ownerUID); err != nil {
		return err
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := strictjson.RejectDuplicateKeys(content); err != nil {
		return err
	}
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("JSON input must contain exactly one document")
	}
	return nil
}

func validateJSONInputInfo(info os.FileInfo, ownerUID uint32) error {
	actualOwnerUID, err := filesystemOwnerUID(info)
	if err != nil || actualOwnerUID != ownerUID || !info.Mode().IsRegular() ||
		info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 ||
		info.Size() <= 0 || info.Size() > maxJSONBytes {
		return errors.New("JSON input must be a bounded private current-owner regular file")
	}
	return nil
}

func writeJSONAtomic(path string, value any) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("JSON path must be canonical and absolute")
	}
	content, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(content) == 0 || len(content) > maxJSONBytes {
		return errors.New("JSON output is outside its byte limit")
	}
	return replaceFileAtomic(path, content)
}
