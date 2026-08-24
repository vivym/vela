package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vivym/vela/internal/remediation"
)

const maxCommandOutputBytes = 64 * 1024

// ExecCommandRunner executes only the already allowlisted binary and argument
// vector. It never invokes a shell and bounds the captured output.
type ExecCommandRunner struct{}

func (ExecCommandRunner) Run(ctx context.Context, _ remediation.Plan, path string, args []string) ([]byte, error) {
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
	command := exec.CommandContext(ctx, path, args...)
	output := &boundedBuffer{limit: maxCommandOutputBytes}
	command.Stdout = output
	command.Stderr = output
	if err := command.Run(); err != nil {
		if output.err != nil {
			return nil, output.err
		}
		return nil, err
	}
	return output.Bytes(), nil
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
	if _, err := fence.runner.Run(ctx, plan, fence.path, append([]string(nil), fence.args...)); err != nil {
		return fmt.Errorf("run host fence precondition: %w", err)
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
	return PostcheckResult{
		Digest: sha256.Sum256(output),
		Detail: "device and inference backend health post-check verified",
	}, nil
}

type CapabilityPolicy interface {
	Authorize(remediation.Plan) error
}

type DeviceCapability struct {
	CertificationRevision string
	Actions               map[remediation.ActionLevel]bool
}

type StaticCapabilityPolicy struct {
	devices map[string]DeviceCapability
}

func NewStaticCapabilityPolicy(devices map[string]DeviceCapability) (*StaticCapabilityPolicy, error) {
	if len(devices) == 0 {
		return nil, errors.New("at least one certified device capability is required")
	}
	copyOfDevices := make(map[string]DeviceCapability, len(devices))
	for device, capability := range devices {
		if !validText(device, maxIdentityText) || !validText(capability.CertificationRevision, maxDetailText) || len(capability.Actions) == 0 {
			return nil, errors.New("certified device capability is invalid")
		}
		actions := make(map[remediation.ActionLevel]bool, len(capability.Actions))
		for action, allowed := range capability.Actions {
			if !remediation.IsActionLevel(action) || action == remediation.ActionL6BMCPowerCycle || action == remediation.ActionL7Quarantine || !allowed {
				return nil, errors.New("certified device capability contains an invalid action")
			}
			actions[action] = true
		}
		copyOfDevices[device] = DeviceCapability{CertificationRevision: capability.CertificationRevision, Actions: actions}
	}
	return &StaticCapabilityPolicy{devices: copyOfDevices}, nil
}

func (policy *StaticCapabilityPolicy) Authorize(plan remediation.Plan) error {
	if policy == nil {
		return errors.New("node Agent capability policy is not configured")
	}
	capability, ok := policy.devices[plan.DeviceIdentity]
	if !ok || capability.CertificationRevision != plan.CertificationRevision || !capability.Actions[plan.ActionLevel] {
		return fmt.Errorf("device %q is not certified for action %s and revision %q", plan.DeviceIdentity, plan.ActionLevel, plan.CertificationRevision)
	}
	return nil
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

func NewRateLimiter(config RateLimit) (*RateLimiter, error) {
	if config.MinimumInterval <= 0 || config.Window <= 0 || config.MaxExecutions <= 0 || config.MinimumInterval > config.Window {
		return nil, errors.New("node Agent rate limit is invalid")
	}
	return &RateLimiter{config: config}, nil
}

func (limiter *RateLimiter) Allow(now time.Time) error {
	if limiter == nil {
		return errors.New("node Agent rate limiter is not configured")
	}
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	cutoff := now.Add(-limiter.config.Window)
	kept := limiter.history[:0]
	for _, timestamp := range limiter.history {
		if timestamp.After(cutoff) {
			kept = append(kept, timestamp)
		}
	}
	limiter.history = kept
	if len(limiter.history) >= limiter.config.MaxExecutions {
		return errors.New("node Agent remediation rate limit exceeded")
	}
	if len(limiter.history) > 0 && now.Sub(limiter.history[len(limiter.history)-1]) < limiter.config.MinimumInterval {
		return errors.New("node Agent remediation minimum interval has not elapsed")
	}
	limiter.history = append(limiter.history, now)
	return nil
}

type CertifiedExecutor struct {
	allowlisted *remediation.AllowlistedExecutor
	policy      CapabilityPolicy
	fence       HostFence
	postcheck   Postcheck
	limiter     *RateLimiter
	clock       func() time.Time
}

func NewCertifiedExecutor(
	allowlisted *remediation.AllowlistedExecutor,
	policy CapabilityPolicy,
	fence HostFence,
	postcheck Postcheck,
	limiter *RateLimiter,
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
	if err := executor.policy.Authorize(plan); err != nil {
		return remediation.ExecutionResult{}, err
	}
	if err := executor.fence.Check(ctx, plan); err != nil {
		return remediation.ExecutionResult{}, fmt.Errorf("host fence rejected remediation: %w", err)
	}
	if err := executor.limiter.Allow(executor.clock().UTC()); err != nil {
		return remediation.ExecutionResult{}, err
	}
	if _, err := executor.allowlisted.Execute(ctx, plan); err != nil {
		return remediation.ExecutionResult{}, err
	}
	postcheckResult, err := executor.postcheck.Verify(ctx, plan)
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
var _ Executor = (*CertifiedExecutor)(nil)
