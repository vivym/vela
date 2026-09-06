package nodeagent

import (
	"context"
	"errors"
	"time"

	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	tasktypes "github.com/containerd/containerd/api/types/task"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

var ErrRuntimeContainerCaller = errors.New("runtime caller does not match the supported live container task and namespace owner")

// RuntimeContainerCallerObservation correlates one pinned live sender with a
// CRI container and its native task. It does not attest immutable configuration,
// grant Registry startup authority, or prove later physical retirement.
type RuntimeContainerCallerObservation struct {
	SchemaVersion   int                         `json:"schema_version"`
	Container       RuntimeContainerObservation `json:"container"`
	Process         RuntimeCallerObservation    `json:"process"`
	ObservedFrom    time.Time                   `json:"observed_from"`
	ObservedThrough time.Time                   `json:"observed_through"`
}

// ObserveCaller uses CRI and native Tasks.Get on the same authenticated local
// connection. The Kubernetes containerd namespace is fixed, never supplied by
// the Runtime. Only the currently tested containerd version is supported here.
func (observer *RuntimeContainerObserver) ObserveCaller(ctx context.Context, target RuntimeContainerTarget, caller *RuntimeCaller) (RuntimeContainerCallerObservation, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeContainerCallerObservation{}, err
	}
	if observer == nil || observer.tasks == nil || observer.clock == nil || caller == nil {
		return RuntimeContainerCallerObservation{}, ErrRuntimeContainerCaller
	}
	if err := target.Validate(); err != nil {
		return RuntimeContainerCallerObservation{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	from := observer.clock().UTC()
	firstProcess, err := caller.Inspect(ctx)
	if err != nil || firstProcess.NamespacePID != 1 || firstProcess.NamespaceDepth < 2 {
		return RuntimeContainerCallerObservation{}, errors.Join(ErrRuntimeContainerCaller, err)
	}
	first, err := observer.Inspect(ctx, target)
	if err != nil || !supportedRuntimeCallerContainer(first, firstProcess) {
		return RuntimeContainerCallerObservation{}, errors.Join(ErrRuntimeContainerCaller, err)
	}
	firstTask, err := observer.callerTask(ctx, target, firstProcess)
	if err != nil {
		return RuntimeContainerCallerObservation{}, err
	}
	last, err := observer.Inspect(ctx, target)
	if err != nil {
		return RuntimeContainerCallerObservation{}, err
	}
	lastTask, err := observer.callerTask(ctx, target, firstProcess)
	if err != nil {
		return RuntimeContainerCallerObservation{}, err
	}
	lastProcess, err := caller.Inspect(ctx)
	if err != nil {
		return RuntimeContainerCallerObservation{}, err
	}
	first.ObservedFrom, first.ObservedThrough = time.Time{}, time.Time{}
	comparable := last
	comparable.ObservedFrom, comparable.ObservedThrough = time.Time{}, time.Time{}
	firstProcess.ObservedAt = lastProcess.ObservedAt
	if first != comparable || firstProcess != lastProcess || !proto.Equal(firstTask, lastTask) ||
		!supportedRuntimeCallerContainer(last, lastProcess) {
		return RuntimeContainerCallerObservation{}, ErrRuntimeContainerCaller
	}
	through := observer.clock().UTC()
	if from.IsZero() || through.Before(from) || through.Sub(from) > 10*time.Second {
		return RuntimeContainerCallerObservation{}, ErrRuntimeContainerCaller
	}
	if err := errors.Join(observer.check(), context.Cause(ctx)); err != nil {
		return RuntimeContainerCallerObservation{}, err
	}
	return RuntimeContainerCallerObservation{SchemaVersion: 1, Container: last, Process: lastProcess,
		ObservedFrom: from, ObservedThrough: through}, nil
}

func supportedRuntimeCallerContainer(container RuntimeContainerObservation, process RuntimeCallerObservation) bool {
	return container.RuntimeName == "containerd" && container.RuntimeVersion == "v2.3.1" &&
		container.ContainerState == "CONTAINER_RUNNING" && container.SandboxState == "SANDBOX_READY" &&
		container.BootID == process.BootID
}

func (observer *RuntimeContainerObserver) callerTask(ctx context.Context, target RuntimeContainerTarget, process RuntimeCallerObservation) (*tasktypes.Process, error) {
	// Replace an inherited namespace header instead of appending another value.
	headers, _ := metadata.FromOutgoingContext(ctx)
	headers = headers.Copy()
	headers.Set("containerd-namespace", "k8s.io")
	response, err := observer.tasks.Get(metadata.NewOutgoingContext(ctx, headers), &tasksapi.GetRequest{ContainerID: target.ContainerID})
	task := response.GetProcess()
	if err != nil || task == nil || task.ID != target.ContainerID || task.Pid != uint32(process.HostPID) ||
		task.Status != tasktypes.Status_RUNNING || task.ExitStatus != 0 ||
		(task.ExitedAt != nil && (task.ExitedAt.CheckValid() != nil || !task.ExitedAt.AsTime().IsZero())) {
		return nil, errors.Join(ErrRuntimeContainerCaller, err)
	}
	return proto.CloneOf(task), nil
}
