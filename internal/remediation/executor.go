package remediation

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

var ErrUncertifiedAction = errors.New("remediation action is not certified")

type Plan struct {
	OperationID           uuid.UUID
	ExecutionClaimID      uuid.UUID
	WorkerID              uuid.UUID
	ActionLevel           ActionLevel
	NodeIdentity          string
	DeviceIdentity        string
	FailureClass          string
	WorkerEpoch           int64
	DeadlineAt            time.Time
	CertificationRevision string
	FailureEvidenceDigest []byte
}

func IsActionLevel(action ActionLevel) bool {
	return validAction(action)
}

type ExecutionResult struct {
	PostcheckDigest   [sha256.Size]byte
	PostcheckVerified bool
	Detail            string
	ResultCode        string
}

type CommandRunner interface {
	Run(context.Context, Plan, string, []string) ([]byte, error)
}

type command struct {
	path string
	args []string
}

type AllowlistedExecutor struct {
	runner   CommandRunner
	commands map[ActionLevel]command
}

func NewAllowlistedExecutor(runner CommandRunner, commands map[ActionLevel]struct {
	Path string
	Args []string
}) (*AllowlistedExecutor, error) {
	if runner == nil {
		return nil, errors.New("remediation command runner is required")
	}
	allowlist := make(map[ActionLevel]command, len(commands))
	for action, specification := range commands {
		if action == ActionL6BMCPowerCycle || action == ActionL7Quarantine || !validAction(action) {
			return nil, fmt.Errorf("%w: %s", ErrUncertifiedAction, action)
		}
		if !filepath.IsAbs(specification.Path) || strings.TrimSpace(specification.Path) != specification.Path {
			return nil, errors.New("remediation command path must be an absolute unpadded path")
		}
		args := append([]string(nil), specification.Args...)
		for _, arg := range args {
			if strings.ContainsRune(arg, '\x00') {
				return nil, errors.New("remediation command arguments cannot contain NUL")
			}
		}
		allowlist[action] = command{path: specification.Path, args: args}
	}
	return &AllowlistedExecutor{runner: runner, commands: allowlist}, nil
}

func (e *AllowlistedExecutor) Execute(ctx context.Context, plan Plan) (ExecutionResult, error) {
	if e == nil || e.runner == nil {
		return ExecutionResult{}, errors.New("remediation executor is not configured")
	}
	if plan.WorkerEpoch <= 0 || !validText(plan.NodeIdentity, 500) ||
		!validText(plan.DeviceIdentity, 500) || !validText(plan.FailureClass, 200) ||
		!validText(plan.CertificationRevision, 200) {
		return ExecutionResult{}, &Failure{Code: FailureInvalid, Message: "Remediation execution identity is invalid"}
	}
	selected, ok := e.commands[plan.ActionLevel]
	if !ok {
		return ExecutionResult{}, &Failure{Code: FailureUncertified, Message: ErrUncertifiedAction.Error()}
	}
	output, err := e.runner.Run(ctx, plan, selected.path, append([]string(nil), selected.args...))
	if err != nil {
		return ExecutionResult{}, &Failure{Code: FailureExecution, Message: err.Error()}
	}
	digest := sha256.Sum256(output)
	return ExecutionResult{
		PostcheckDigest:   digest,
		Detail:            "allowlisted remediation command completed; health post-check is not certified",
		ResultCode:        "EXECUTION_OUTPUT_ONLY",
		PostcheckVerified: false,
	}, nil
}
