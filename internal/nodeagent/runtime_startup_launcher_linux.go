package nodeagent

import (
	"context"
	"net"
	"os"
)

// RuntimeStartupLaunch is the authority-bearing handoff returned by a
// RuntimeStartupLauncher. All process handles must originate from the current
// launch invocation. The Node owns the descriptors after successful assembly.
type RuntimeStartupLaunch struct {
	WorkerOwnerPIDFD *os.File
	ObserverPIDFD    *os.File
	// LauncherPIDFD is the original pidfd of the root-owned helper process.
	// The observer is normally a child of that helper, so Node must bind the
	// observer's parent to this handle before accepting custody.
	LauncherPIDFD *os.File
	ObserverConn  *net.UnixConn
	Target        RuntimeContainerTarget
	WorkerTarget  RuntimeContainerTarget
	Policy        RuntimeStartupAuthorizationPolicy
	// Close releases launcher-owned state and requests termination of any
	// process that has not yet been transferred to Node custody. It is called
	// exactly once on every failed assembly and after Node shutdown.
	Close func() error
}

// RuntimeStartupLauncher creates the Runtime/Worker and observer before the
// Node performs reservation or grants authority. Implementations must create
// the observer socketpair before exec, retain original pidfds, and return the
// exact CRI target for this invocation. They must never reconstruct handles
// from numeric PIDs, historical receipts or prior reservations.
type RuntimeStartupLauncher interface {
	Launch(context.Context, *RuntimeLaunchPlan, string) (RuntimeStartupLaunch, error)
}
