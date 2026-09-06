package modelruntime

import (
	"context"
	"os"
)

func StartRuntimeServerWithStateSyncHookForTest(ctx context.Context, config RuntimeServerConfig, hook func(func() error) error) (*RuntimeServer, error) {
	return startRuntimeServer(ctx, config, func(store *executionStateFile) {
		original := store.syncDirectory
		store.syncDirectory = func(root *os.Root) error {
			return hook(func() error { return original(root) })
		}
	})
}

// SetExecutionStateSyncHookForTest injects the boundary after Rename and before
// directory fsync, without exporting filesystem fault controls in the runtime.
func SetExecutionStateSyncHookForTest(supervisor *Supervisor, hook func(func() error) error) func() {
	admission := supervisor.admission
	admission.mu.Lock()
	previous := admission.store.syncDirectory
	admission.store.syncDirectory = func(root *os.Root) error {
		return hook(func() error { return previous(root) })
	}
	admission.mu.Unlock()
	return func() {
		admission.mu.Lock()
		defer admission.mu.Unlock()
		admission.store.syncDirectory = previous
	}
}
