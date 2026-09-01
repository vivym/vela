package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/remediation"
	"github.com/vivym/vela/internal/securefile"
)

const maxCommandOutputBytes = 64 * 1024

var (
	gpuUUIDPattern = regexp.MustCompile(`^GPU-[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`)
	pciBDFPattern  = regexp.MustCompile(`^[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]$`)
)

type hostEvidenceIdentity struct {
	OperationID           string `json:"operation_id"`
	ExecutionClaimID      string `json:"execution_claim_id"`
	WorkerInstanceID              string `json:"worker_instance_id"`
	WorkerInstanceEpoch           int64  `json:"worker_instance_epoch"`
	NodeIdentity          string `json:"node_identity"`
	DeviceIdentity        string `json:"device_identity"`
	GPUUUID               string `json:"gpu_uuid"`
	PCIBDF                string `json:"pci_bdf"`
	FailureClass          string `json:"failure_class"`
	ActionLevel           string `json:"action_level"`
	CertificationRevision string `json:"certification_revision"`
	FailureEvidenceSHA256 string `json:"failure_evidence_sha256"`
}

type fenceEvidence struct {
	hostEvidenceIdentity
	NewAssignmentsStopped  bool `json:"new_assignments_stopped"`
	TargetProcessesStopped bool `json:"target_processes_stopped"`
}

type postcheckEvidence struct {
	hostEvidenceIdentity
	DeviceHealthy           bool   `json:"device_healthy"`
	InferenceBackendHealthy bool   `json:"inference_backend_healthy"`
	Detail                  string `json:"detail"`
}

// ExecCommandRunner executes only the already allowlisted binary and argument
// vector. It never invokes a shell and bounds the captured output.
type ExecCommandRunner struct{}

func (ExecCommandRunner) Run(ctx context.Context, plan remediation.Plan, path string, args []string) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("host command context is required")
	}
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned != path {
		return nil, errors.New("host command path must be an absolute clean path")
	}
	for _, arg := range args {
		if strings.ContainsRune(arg, '\x00') {
			return nil, errors.New("host command arguments cannot contain NUL")
		}
	}
	boundArgs, err := planArguments(plan)
	if err != nil {
		return nil, err
	}
	commandArgs := append(append([]string(nil), args...), boundArgs...)
	return runHeldExecutable(ctx, cleaned, commandArgs, maxCommandOutputBytes)
}

func runHeldExecutable(ctx context.Context, path string, args []string, outputLimit int) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("held executable context is required")
	}
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned != path || outputLimit <= 0 {
		return nil, errors.New("held executable path or output bound is invalid")
	}
	for _, arg := range args {
		if strings.ContainsRune(arg, '\x00') {
			return nil, errors.New("held executable arguments cannot contain NUL")
		}
	}
	executable, err := securefile.OpenExecutable(cleaned)
	if err != nil {
		return nil, fmt.Errorf("held executable is not trusted: %w", err)
	}
	defer func() { _ = executable.Close() }()
	executablePath, cleanup, err := prepareExecutablePath(executable, runtime.GOOS)
	if err != nil {
		return nil, err
	}
	command := exec.CommandContext(ctx, executablePath, args...)
	command.Args[0] = cleaned
	command.ExtraFiles = []*os.File{executable}
	output := &boundedBuffer{limit: outputLimit}
	command.Stdout = output
	command.Stderr = output
	runErr := command.Run()
	cleanupErr := cleanup()
	if runErr != nil {
		if output.err != nil {
			return nil, errors.Join(output.err, cleanupErr)
		}
		return nil, errors.Join(runErr, cleanupErr)
	}
	if cleanupErr != nil {
		return nil, cleanupErr
	}
	return output.Bytes(), nil
}

func prepareExecutablePath(executable *os.File, goos string) (string, func() error, error) {
	switch goos {
	case "linux":
		return "/proc/self/fd/3", func() error { return nil }, nil
	case "darwin":
		return stageDarwinExecutable(executable)
	default:
		return "", nil, fmt.Errorf("held executable execution is unsupported on %s", goos)
	}
}

func stageDarwinExecutable(executable *os.File) (string, func() error, error) {
	if executable == nil {
		return "", nil, errors.New("held executable is required")
	}
	info, err := executable.Stat()
	if err != nil {
		return "", nil, fmt.Errorf("stat held executable: %w", err)
	}
	temporaryParent, err := securefile.ResolveTrustedDirectory(filepath.Clean(os.TempDir()))
	if err != nil {
		return "", nil, fmt.Errorf("validate held executable staging parent: %w", err)
	}
	directory, err := os.MkdirTemp(temporaryParent, ".vela-held-executable-*")
	if err != nil {
		return "", nil, fmt.Errorf("create held executable staging directory: %w", err)
	}
	path := filepath.Join(directory, "helper")
	cleanup := func() error {
		return errors.Join(os.Remove(path), os.Remove(directory))
	}
	fail := func(err error) (string, func() error, error) {
		_ = cleanup()
		return "", nil, err
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return fail(fmt.Errorf("restrict held executable staging directory: %w", err))
	}
	staged, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0o500)
	if err != nil {
		return fail(fmt.Errorf("create held executable staging file: %w", err))
	}
	closeStaged := func() { _ = staged.Close() }
	section := io.NewSectionReader(executable, 0, info.Size())
	written, copyErr := io.Copy(staged, section)
	if copyErr == nil && written != info.Size() {
		copyErr = io.ErrUnexpectedEOF
	}
	if copyErr != nil {
		closeStaged()
		return fail(fmt.Errorf("copy held executable to staging file: wrote %d of %d bytes: %w", written, info.Size(), copyErr))
	}
	if err := staged.Sync(); err != nil {
		closeStaged()
		return fail(fmt.Errorf("sync held executable staging file: %w", err))
	}
	if err := staged.Close(); err != nil {
		return fail(fmt.Errorf("close held executable staging file: %w", err))
	}
	stagingDirectory, err := os.Open(directory)
	if err != nil {
		return fail(fmt.Errorf("open held executable staging directory: %w", err))
	}
	directorySyncErr := stagingDirectory.Sync()
	directoryCloseErr := stagingDirectory.Close()
	if err := errors.Join(directorySyncErr, directoryCloseErr); err != nil {
		return fail(fmt.Errorf("sync held executable staging directory: %w", err))
	}
	return path, cleanup, nil
}

func planArguments(plan remediation.Plan) ([]string, error) {
	if plan.OperationID == uuid.Nil || plan.ExecutionClaimID == uuid.Nil || plan.WorkerInstanceID == uuid.Nil ||
		plan.WorkerInstanceEpoch <= 0 || !validText(plan.NodeIdentity, maxIdentityText) ||
		!validText(plan.DeviceIdentity, maxIdentityText) || !validGPUUUID(plan.GPUUUID) ||
		!validPCIBDF(plan.PCIBDF) || !validText(plan.FailureClass, 200) ||
		!remediation.IsActionLevel(plan.ActionLevel) ||
		!validText(plan.CertificationRevision, maxDetailText) ||
		len(plan.FailureEvidenceDigest) != sha256.Size || plan.DeadlineAt.IsZero() {
		return nil, errors.New("host command execution plan is invalid")
	}
	return []string{
		"--vela-operation-id=" + plan.OperationID.String(),
		"--vela-execution-claim-id=" + plan.ExecutionClaimID.String(),
		"--vela-worker-instance-id=" + plan.WorkerInstanceID.String(),
		fmt.Sprintf("--vela-worker-instance-epoch=%d", plan.WorkerInstanceEpoch),
		"--vela-node-identity=" + plan.NodeIdentity,
		"--vela-device-identity=" + plan.DeviceIdentity,
		"--vela-gpu-uuid=" + plan.GPUUUID,
		"--vela-pci-bdf=" + plan.PCIBDF,
		"--vela-failure-class=" + plan.FailureClass,
		"--vela-action-level=" + string(plan.ActionLevel),
		"--vela-certification-revision=" + plan.CertificationRevision,
		"--vela-failure-evidence-sha256=" + hex.EncodeToString(plan.FailureEvidenceDigest),
		"--vela-deadline-at=" + plan.DeadlineAt.UTC().Format(time.RFC3339Nano),
	}, nil
}

type boundedBuffer struct {
	bytes.Buffer
	limit int
	err   error
}

func (buffer *boundedBuffer) Write(value []byte) (int, error) {
	if buffer.err != nil {
		return 0, buffer.err
	}
	if len(value) > buffer.limit-buffer.Len() {
		buffer.err = errors.New("host command output exceeds configured bound")
		return 0, buffer.err
	}
	return buffer.Buffer.Write(value)
}

var _ io.Writer = (*boundedBuffer)(nil)

type PostcheckResult struct {
	Digest [sha256.Size]byte
	Detail string
}

type Postcheck interface {
	Verify(context.Context, remediation.Plan) (PostcheckResult, error)
}

type CommandPostcheck struct {
	runner remediation.CommandRunner
	path   string
	args   []string
}

type CommandFence struct {
	runner remediation.CommandRunner
	path   string
	args   []string
}

func NewCommandFence(runner remediation.CommandRunner, path string, args []string) (*CommandFence, error) {
	if runner == nil {
		return nil, errors.New("node Agent host fence runner is required")
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("node Agent host fence path must be an absolute clean path")
	}
	for _, arg := range args {
		if strings.ContainsRune(arg, '\x00') {
			return nil, errors.New("node Agent host fence arguments cannot contain NUL")
		}
	}
	return &CommandFence{runner: runner, path: path, args: append([]string(nil), args...)}, nil
}

func (fence *CommandFence) Check(ctx context.Context, plan remediation.Plan) error {
	if fence == nil || fence.runner == nil {
		return errors.New("node Agent host fence is not configured")
	}
	output, err := fence.runner.Run(ctx, plan, fence.path, append([]string(nil), fence.args...))
	if err != nil {
		return fmt.Errorf("run host fence precondition: %w", err)
	}
	var evidence fenceEvidence
	if err := decodeHostEvidence(output, &evidence); err != nil {
		return fmt.Errorf("decode host fence evidence: %w", err)
	}
	if !evidence.matches(plan) {
		return errors.New("host fence evidence identity does not match execution plan")
	}
	if !evidence.NewAssignmentsStopped || !evidence.TargetProcessesStopped {
		return errors.New("host fence evidence does not prove the target is stopped")
	}
	return nil
}

func NewCommandPostcheck(runner remediation.CommandRunner, path string, args []string) (*CommandPostcheck, error) {
	if runner == nil {
		return nil, errors.New("node Agent post-check runner is required")
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("node Agent post-check path must be an absolute clean path")
	}
	for _, arg := range args {
		if strings.ContainsRune(arg, '\x00') {
			return nil, errors.New("node Agent post-check arguments cannot contain NUL")
		}
	}
	return &CommandPostcheck{runner: runner, path: path, args: append([]string(nil), args...)}, nil
}

func (postcheck *CommandPostcheck) Verify(ctx context.Context, plan remediation.Plan) (PostcheckResult, error) {
	if postcheck == nil || postcheck.runner == nil {
		return PostcheckResult{}, errors.New("node Agent post-check is not configured")
	}
	output, err := postcheck.runner.Run(ctx, plan, postcheck.path, append([]string(nil), postcheck.args...))
	if err != nil {
		return PostcheckResult{}, fmt.Errorf("run health post-check: %w", err)
	}
	if len(output) == 0 {
		return PostcheckResult{}, errors.New("health post-check returned no evidence")
	}
	var evidence postcheckEvidence
	if err := decodeHostEvidence(output, &evidence); err != nil {
		return PostcheckResult{}, fmt.Errorf("decode health post-check evidence: %w", err)
	}
	if !evidence.matches(plan) {
		return PostcheckResult{}, errors.New("health post-check evidence identity does not match execution plan")
	}
	if !evidence.DeviceHealthy || !evidence.InferenceBackendHealthy {
		return PostcheckResult{}, errors.New("health post-check did not verify device and inference backend health")
	}
	if !validText(evidence.Detail, maxDetailText) {
		return PostcheckResult{}, errors.New("health post-check evidence detail is invalid")
	}
	return PostcheckResult{
		Digest: sha256.Sum256(output),
		Detail: evidence.Detail,
	}, nil
}

func decodeHostEvidence(output []byte, target any) error {
	if len(output) == 0 || len(output) > maxCommandOutputBytes {
		return errors.New("host helper evidence is empty or exceeds its bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(output))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("host helper evidence must contain exactly one JSON document")
	}
	return nil
}

func (identity hostEvidenceIdentity) matches(plan remediation.Plan) bool {
	return identity.OperationID == plan.OperationID.String() &&
		identity.ExecutionClaimID == plan.ExecutionClaimID.String() &&
		identity.WorkerInstanceID == plan.WorkerInstanceID.String() && identity.WorkerInstanceEpoch == plan.WorkerInstanceEpoch &&
		identity.NodeIdentity == plan.NodeIdentity && identity.DeviceIdentity == plan.DeviceIdentity &&
		identity.GPUUUID == plan.GPUUUID && identity.PCIBDF == plan.PCIBDF &&
		identity.FailureClass == plan.FailureClass &&
		identity.ActionLevel == string(plan.ActionLevel) &&
		identity.CertificationRevision == plan.CertificationRevision &&
		identity.FailureEvidenceSHA256 == hex.EncodeToString(plan.FailureEvidenceDigest)
}

type CapabilityPolicy interface {
	Authorize(remediation.Plan) (DeviceBinding, error)
}

type DeviceCapability struct {
	GPUUUID               string
	PCIBDF                string
	CertificationRevision string
	FailureClasses        map[string]bool
	Actions               map[remediation.ActionLevel]bool
}

type DeviceBinding struct {
	GPUUUID string
	PCIBDF  string
}

type StaticCapabilityPolicy struct {
	devices map[string]DeviceCapability
}

func NewStaticCapabilityPolicy(devices map[string]DeviceCapability) (*StaticCapabilityPolicy, error) {
	if len(devices) == 0 {
		return nil, errors.New("at least one certified device capability is required")
	}
	copyOfDevices := make(map[string]DeviceCapability, len(devices))
	seenPCIBDFs := make(map[string]struct{}, len(devices))
	for device, capability := range devices {
		if !validGPUUUID(device) || capability.GPUUUID != device || !validPCIBDF(capability.PCIBDF) ||
			!validText(capability.CertificationRevision, maxDetailText) ||
			len(capability.FailureClasses) == 0 || len(capability.Actions) == 0 {
			return nil, errors.New("certified device capability is invalid")
		}
		if _, exists := seenPCIBDFs[capability.PCIBDF]; exists {
			return nil, errors.New("certified device capabilities reuse a PCI BDF")
		}
		seenPCIBDFs[capability.PCIBDF] = struct{}{}
		failureClasses := make(map[string]bool, len(capability.FailureClasses))
		for failureClass, allowed := range capability.FailureClasses {
			if !validText(failureClass, 200) || !allowed {
				return nil, errors.New("certified device capability contains an invalid failure class")
			}
			failureClasses[failureClass] = true
		}
		actions := make(map[remediation.ActionLevel]bool, len(capability.Actions))
		for action, allowed := range capability.Actions {
			if !remediation.IsActionLevel(action) || action == remediation.ActionL6BMCPowerCycle || action == remediation.ActionL7Quarantine || !allowed {
				return nil, errors.New("certified device capability contains an invalid action")
			}
			actions[action] = true
		}
		copyOfDevices[device] = DeviceCapability{
			GPUUUID: device, PCIBDF: capability.PCIBDF,
			CertificationRevision: capability.CertificationRevision,
			FailureClasses:        failureClasses, Actions: actions,
		}
	}
	return &StaticCapabilityPolicy{devices: copyOfDevices}, nil
}

func (policy *StaticCapabilityPolicy) Authorize(plan remediation.Plan) (DeviceBinding, error) {
	if policy == nil {
		return DeviceBinding{}, errors.New("node Agent capability policy is not configured")
	}
	capability, ok := policy.devices[plan.DeviceIdentity]
	if !ok || capability.CertificationRevision != plan.CertificationRevision ||
		!capability.FailureClasses[plan.FailureClass] || !capability.Actions[plan.ActionLevel] {
		return DeviceBinding{}, fmt.Errorf(
			"device %q is not certified for failure class %q, action %s, and revision %q",
			plan.DeviceIdentity, plan.FailureClass, plan.ActionLevel, plan.CertificationRevision,
		)
	}
	return DeviceBinding{GPUUUID: capability.GPUUUID, PCIBDF: capability.PCIBDF}, nil
}

func validGPUUUID(value string) bool {
	return gpuUUIDPattern.MatchString(value)
}

func validPCIBDF(value string) bool {
	return pciBDFPattern.MatchString(value)
}

type HostFence interface {
	Check(context.Context, remediation.Plan) error
}

type CallbackFence func(context.Context, remediation.Plan) error

func (fence CallbackFence) Check(ctx context.Context, plan remediation.Plan) error {
	if fence == nil {
		return errors.New("node Agent host fence is not configured")
	}
	return fence(ctx, plan)
}

type RateLimit struct {
	MinimumInterval time.Duration
	Window          time.Duration
	MaxExecutions   int
}

type RateLimiter struct {
	config  RateLimit
	mu      sync.Mutex
	history []time.Time
}

type ExecutionRateLimiter interface {
	Allow(time.Time) error
}

func NewRateLimiter(config RateLimit) (*RateLimiter, error) {
	if !validRateLimit(config) {
		return nil, errors.New("node Agent rate limit is invalid")
	}
	return &RateLimiter{config: config}, nil
}

func validRateLimit(config RateLimit) bool {
	return config.MinimumInterval > 0 && config.Window > 0 && config.MaxExecutions > 0 &&
		config.MinimumInterval <= config.Window
}

func allowedHistory(config RateLimit, history []time.Time, now time.Time) ([]time.Time, error) {
	cutoff := now.Add(-config.Window)
	kept := make([]time.Time, 0, len(history)+1)
	for _, timestamp := range history {
		if timestamp.After(cutoff) {
			kept = append(kept, timestamp)
		}
	}
	if len(kept) >= config.MaxExecutions {
		return nil, errors.New("node Agent remediation rate limit exceeded")
	}
	if len(kept) > 0 && now.Sub(kept[len(kept)-1]) < config.MinimumInterval {
		return nil, errors.New("node Agent remediation minimum interval has not elapsed")
	}
	return append(kept, now), nil
}

func (limiter *RateLimiter) Allow(now time.Time) error {
	if limiter == nil {
		return errors.New("node Agent rate limiter is not configured")
	}
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	history, err := allowedHistory(limiter.config, limiter.history, now)
	if err != nil {
		return err
	}
	limiter.history = history
	return nil
}

type CertifiedExecutor struct {
	allowlisted *remediation.AllowlistedExecutor
	policy      CapabilityPolicy
	fence       HostFence
	postcheck   Postcheck
	limiter     ExecutionRateLimiter
	clock       func() time.Time
}

func NewCertifiedExecutor(
	allowlisted *remediation.AllowlistedExecutor,
	policy CapabilityPolicy,
	fence HostFence,
	postcheck Postcheck,
	limiter ExecutionRateLimiter,
) (*CertifiedExecutor, error) {
	if allowlisted == nil || policy == nil || fence == nil || postcheck == nil || limiter == nil {
		return nil, errors.New("node Agent certified executor dependencies are required")
	}
	return &CertifiedExecutor{allowlisted: allowlisted, policy: policy, fence: fence, postcheck: postcheck, limiter: limiter, clock: time.Now}, nil
}

func (executor *CertifiedExecutor) Execute(ctx context.Context, plan remediation.Plan) (remediation.ExecutionResult, error) {
	if executor == nil || executor.allowlisted == nil || executor.policy == nil || executor.fence == nil || executor.postcheck == nil || executor.limiter == nil {
		return remediation.ExecutionResult{}, errors.New("node Agent certified executor is not configured")
	}
	binding, err := executor.policy.Authorize(plan)
	if err != nil {
		return remediation.ExecutionResult{}, err
	}
	boundPlan := plan
	boundPlan.GPUUUID = binding.GPUUUID
	boundPlan.PCIBDF = binding.PCIBDF
	if err := executor.fence.Check(ctx, boundPlan); err != nil {
		return remediation.ExecutionResult{}, fmt.Errorf("host fence rejected remediation: %w", err)
	}
	if err := executor.limiter.Allow(executor.clock().UTC()); err != nil {
		return remediation.ExecutionResult{}, err
	}
	if _, err := executor.allowlisted.Execute(ctx, boundPlan); err != nil {
		return remediation.ExecutionResult{}, err
	}
	postcheckResult, err := executor.postcheck.Verify(ctx, boundPlan)
	if err != nil {
		return remediation.ExecutionResult{ResultCode: "POSTCHECK_FAILED", Detail: err.Error()}, nil
	}
	return remediation.ExecutionResult{
		PostcheckDigest: postcheckResult.Digest, PostcheckVerified: true,
		Detail: postcheckResult.Detail, ResultCode: "POSTCHECK_OK",
	}, nil
}

var _ remediation.CommandRunner = ExecCommandRunner{}
var _ Postcheck = (*CommandPostcheck)(nil)
var _ HostFence = (*CommandFence)(nil)
var _ CapabilityPolicy = (*StaticCapabilityPolicy)(nil)
var _ HostFence = CallbackFence(nil)
var _ ExecutionRateLimiter = (*RateLimiter)(nil)
var _ Executor = (*CertifiedExecutor)(nil)
