package nodeagent

import (
	"context"
	"errors"
	"os"

	"github.com/vivym/vela/internal/runtimechannel"
)

// ValidateRuntimeWorkerOwnerPIDFD checks that a launcher supplied the exact
// live process represented by the authenticated caller. The check compares
// kernel pidfd identity and liveness; it never opens /proc/<pid> or accepts a
// numeric PID. The caller remains the owner of its pidfd after this check.
func ValidateRuntimeWorkerOwnerPIDFD(ctx context.Context, caller *RuntimeCaller, workerPIDFD *os.File) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if caller == nil || workerPIDFD == nil {
		return ErrRuntimeNamespaceOwnerLost
	}
	if err := runtimechannel.ValidatePIDFD(int(workerPIDFD.Fd())); err != nil {
		return errors.Join(ErrRuntimeNamespaceOwnerLost, err)
	}
	if err := runtimechannel.PollLivePIDFD(int(workerPIDFD.Fd())); err != nil {
		return errors.Join(ErrRuntimeNamespaceOwnerLost, err)
	}
	caller.mu.Lock()
	defer caller.mu.Unlock()
	if caller.pidfd == nil {
		return ErrRuntimeNamespaceOwnerLost
	}
	if err := runtimechannel.SameLiveProcess(int(caller.pidfd.Fd()), int(workerPIDFD.Fd())); err == nil {
		return errors.Join(ErrRuntimeNamespaceOwnerLost, errors.New("runtime and Worker owner must be different original processes"))
	}
	return context.Cause(ctx)
}
