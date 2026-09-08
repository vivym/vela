package modelruntime

import (
	"context"
	"errors"
	"os"
	"syscall"
)

// RequireRootCustody checks present file ownership and live binding. It cannot
// prove first-use provenance, authorized adoption or original process ownership.
func (owner *ExecutionJournalOwner) RequireRootCustody(ctx context.Context) error {
	if owner == nil || ctx == nil || os.Geteuid() != 0 {
		return errors.New("journal endpoint requires a root owner")
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err := owner.check(ctx); err != nil {
		return err
	}
	root, err := os.Lstat(owner.store.path)
	if err != nil {
		return err
	}
	for _, name := range []string{"", executionStateName, executionStateLockName} {
		info := root
		if name != "" {
			info, err = owner.store.root.Lstat(name)
			if err != nil {
				return err
			}
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != 0 || stat.Gid != 0 {
			return errors.New("journal endpoint storage is not root-private")
		}
	}
	return nil
}
