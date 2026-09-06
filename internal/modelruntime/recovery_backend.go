package modelruntime

import (
	"context"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

// recoveryOnlyBackend has no process, device or writer. It keeps authenticated
// journal routes available without claiming that an old execution has stopped.
// It never activates in place; subsequent startup must revalidate the journal.
type recoveryOnlyBackend struct{ reason error }

func (backend recoveryOnlyBackend) rejection() error {
	if backend.reason != nil {
		return backend.reason
	}
	return ErrBackendIncarnationUnproven
}

func (backend recoveryOnlyBackend) Probe(context.Context, velav1.ModelRuntimeReadinessCheck) (ProbeResult, error) {
	return ProbeResult{}, backend.rejection()
}

func (backend recoveryOnlyBackend) Prepare(context.Context, stageauthority.Verified, *velav1.StageExecutionSpec) error {
	return backend.rejection()
}

func (backend recoveryOnlyBackend) Start(context.Context, stageauthority.Verified) error {
	return backend.rejection()
}

func (backend recoveryOnlyBackend) Cancel(context.Context, stageauthority.Verified, velav1.ModelRuntimeCancelReason) error {
	return backend.rejection()
}

func (backend recoveryOnlyBackend) Status(context.Context, stageauthority.Verified) (BackendStatus, error) {
	return BackendStatus{}, backend.rejection()
}

func (backend recoveryOnlyBackend) Seal(context.Context, stageauthority.Verified) (SealedOutput, error) {
	return SealedOutput{}, backend.rejection()
}
