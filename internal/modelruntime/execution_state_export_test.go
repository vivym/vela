package modelruntime

import (
	"context"
	"os"
)

// ExecutionDeadlineExpiredForTest observes watchdog delivery without changing
// its timer or production lock ordering. Tests use it only to join injection.
func ExecutionDeadlineExpiredForTest(service *Service) bool {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.active != nil && service.active.deadlineExpired
}

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
	previous := admission.store.(*executionStateFile).syncDirectory
	admission.store.(*executionStateFile).syncDirectory = func(root *os.Root) error {
		return hook(func() error { return previous(root) })
	}
	admission.mu.Unlock()
	return func() {
		admission.mu.Lock()
		defer admission.mu.Unlock()
		admission.store.(*executionStateFile).syncDirectory = previous
	}
}

func SetJournalOwnerSyncHookForTest(owner *ExecutionJournalOwner, hook func(func() error) error) func() {
	owner.mu.Lock()
	previous := owner.store.syncDirectory
	owner.store.syncDirectory = func(root *os.Root) error { return hook(func() error { return previous(root) }) }
	owner.mu.Unlock()
	return func() {
		owner.mu.Lock()
		defer owner.mu.Unlock()
		owner.store.syncDirectory = previous
	}
}
