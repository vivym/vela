package main

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Disable ptrace-class access by same-UID backend processes before reading
// launch configuration or invoking any factory. This mm-wide property covers
// every Go thread. It also disables core dumps. Privileged host/Node inspection
// still requires CAP_SYS_PTRACE in the target's user namespace.
//
// This is not launch attestation or an exec/descendant containment policy:
// exec and credential changes can reset dumpability. The resident Runtime
// must not perform those transitions after installing this protection.
func protectRuntimeProcess() error {
	if err := unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		return fmt.Errorf("disable Runtime process memory inspection: %w", err)
	}
	value, err := unix.PrctlRetInt(unix.PR_GET_DUMPABLE, 0, 0, 0, 0)
	if err != nil {
		return fmt.Errorf("verify Runtime process memory protection: %w", err)
	}
	if value != 0 {
		return fmt.Errorf("runtime process remains dumpable: %d", value)
	}
	return nil
}
