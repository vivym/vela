package stageworkermembertransport

import (
	"context"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type WorkerJournalCommandGuard interface {
	WorkerJournalBindingObserver
	RetainJournalBinding(context.Context) (*velav1.WorkerBootstrapBinding, func() error, error)
}

// Renewal-capable calls retain journal ownership without holding its mutex over
// the RPC. Recovery calls use their own Runtime authority and remain available.
func forwardWithWorkerJournal[T any](ctx context.Context, server *Server, identity *velav1.ModelRuntimeIdentity, forward func() (T, error)) (result T, err error) {
	if ctx == nil {
		return result, status.Error(codes.InvalidArgument, "Stage Worker command requires a context")
	}
	if err := ctx.Err(); err != nil {
		return result, status.FromContextError(err).Err()
	}
	if server.workerJournal == nil {
		return forward()
	}
	guard, ok := server.workerJournal.(WorkerJournalCommandGuard)
	if !ok {
		return result, status.Error(codes.FailedPrecondition, "Stage Worker journal has no command lifetime guard")
	}
	binding, release, retainErr := guard.RetainJournalBinding(ctx)
	if release != nil {
		defer func() {
			if releaseErr := release(); releaseErr != nil && err == nil {
				var empty T
				result, err = empty, status.Error(codes.FailedPrecondition, "Stage Worker journal ownership was lost during command forwarding")
			}
		}()
	}
	if err := ctx.Err(); err != nil {
		return result, status.FromContextError(err).Err()
	}
	if retainErr != nil || release == nil || !bindingMatchesDiscovery(discoveryRequestFor(identity), binding) {
		return result, status.Error(codes.FailedPrecondition, "Stage Worker journal ownership is unavailable for command forwarding")
	}
	result, err = forward()
	if err == nil && ctx.Err() != nil {
		var empty T
		return empty, status.FromContextError(ctx.Err()).Err()
	}
	return result, err
}
