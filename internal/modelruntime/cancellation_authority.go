package modelruntime

import (
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// The caller holds operationMu. A fresh compatible successor can authorize a
// stop, but cannot replace the installed envelope or extend its watchdog.
func (service *Service) cancellationTarget(request stageauthority.Verified, allowSuccessor bool) (stageauthority.Verified, velav1.ModelRuntimeExecutionState, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active == nil {
		return stageauthority.Verified{}, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED, errActiveAuthorityMismatch
	}
	active := service.active
	if active.verified.Digest != request.Digest && (!allowSuccessor || active.deadlineExpired || terminalState(active.state) ||
		stageauthority.ValidateRenewal(active.verified.Authority, request.Authority) != nil) {
		return stageauthority.Verified{}, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED, errActiveAuthorityMismatch
	}
	target := active.verified
	target.Authority = proto.Clone(target.Authority).(*velav1.StageAuthority)
	return target, active.state, nil
}
