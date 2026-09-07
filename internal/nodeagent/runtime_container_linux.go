package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"time"

	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// The observer cannot call CRI start, stop, remove, exec or image operations.
type runtimeContainerReader interface {
	Version(context.Context, *runtimev1.VersionRequest, ...grpc.CallOption) (*runtimev1.VersionResponse, error)
	ListContainers(context.Context, *runtimev1.ListContainersRequest, ...grpc.CallOption) (*runtimev1.ListContainersResponse, error)
	ContainerStatus(context.Context, *runtimev1.ContainerStatusRequest, ...grpc.CallOption) (*runtimev1.ContainerStatusResponse, error)
	PodSandboxStatus(context.Context, *runtimev1.PodSandboxStatusRequest, ...grpc.CallOption) (*runtimev1.PodSandboxStatusResponse, error)
}

type runtimeContainerTaskReader interface {
	Get(context.Context, *tasksapi.GetRequest, ...grpc.CallOption) (*tasksapi.GetResponse, error)
}

type RuntimeContainerObserver struct {
	connection   *grpc.ClientConn
	reader       runtimeContainerReader
	tasks        runtimeContainerTaskReader
	nodeIdentity string
	bootID       func() (string, error)
	check        func() error
	close        func() error
	clock        func() time.Time
}

func (observer *RuntimeContainerObserver) Close() error {
	if observer == nil || observer.close == nil {
		return nil
	}
	return observer.close()
}

func (observer *RuntimeContainerObserver) Inspect(ctx context.Context, target RuntimeContainerTarget) (RuntimeContainerObservation, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeContainerObservation{}, err
	}
	if observer == nil || observer.reader == nil || observer.bootID == nil || observer.check == nil || observer.clock == nil ||
		!validText(observer.nodeIdentity, maxIdentityText) {
		return RuntimeContainerObservation{}, errors.New("runtime container observer is not configured")
	}
	if err := target.Validate(); err != nil {
		return RuntimeContainerObservation{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	from := observer.clock().UTC()
	boot, err := observer.readBoot()
	if err != nil {
		return RuntimeContainerObservation{}, err
	}
	if err := observer.check(); err != nil {
		return RuntimeContainerObservation{}, err
	}
	version, err := observer.reader.Version(ctx, &runtimev1.VersionRequest{Version: "0.1.0"})
	if err != nil {
		return RuntimeContainerObservation{}, fmt.Errorf("read CRI version: %w", err)
	}
	if version == nil || version.RuntimeApiVersion != "v1" || !validText(version.RuntimeName, 100) || !validText(version.RuntimeVersion, 100) {
		return RuntimeContainerObservation{}, errors.New("CRI runtime version is missing or unsupported")
	}
	first, sandbox, err := observer.readContainer(ctx, target)
	if err != nil {
		return RuntimeContainerObservation{}, err
	}
	// CRI offers no atomic snapshot across these calls. Reject visible changes
	// and report the collection interval; never imply a continuing lifetime lock.
	last, lastSandbox, err := observer.readContainer(ctx, target)
	if err != nil {
		return RuntimeContainerObservation{}, err
	}
	if !proto.Equal(first, last) || !proto.Equal(sandbox, lastSandbox) {
		return RuntimeContainerObservation{}, errors.New("CRI container or sandbox changed during observation")
	}
	finalBoot, err := observer.readBoot()
	if err != nil {
		return RuntimeContainerObservation{}, err
	}
	if boot != finalBoot {
		return RuntimeContainerObservation{}, errors.New("node boot identity changed during container observation")
	}
	if err := errors.Join(observer.check(), context.Cause(ctx)); err != nil {
		return RuntimeContainerObservation{}, err
	}
	through := observer.clock().UTC()
	if from.IsZero() || through.Before(from) || through.Sub(from) > 10*time.Second ||
		first.CreatedAt > through.UnixNano() || first.StartedAt > through.UnixNano() || first.FinishedAt > through.UnixNano() {
		return RuntimeContainerObservation{}, errors.New("CRI container observation time is inconsistent")
	}
	return RuntimeContainerObservation{
		SchemaVersion: 1, Target: target, NodeIdentity: observer.nodeIdentity, BootID: boot,
		RuntimeName: version.RuntimeName, RuntimeVersion: version.RuntimeVersion, RuntimeAPIVersion: version.RuntimeApiVersion,
		ContainerState: first.State.String(), CreatedAt: criTimestamp(first.CreatedAt), StartedAt: criTimestamp(first.StartedAt),
		FinishedAt: criTimestamp(first.FinishedAt), ExitCode: first.ExitCode, ImageRef: first.ImageRef,
		SandboxCreatedAt: criTimestamp(sandbox.CreatedAt), SandboxAttempt: sandbox.Metadata.Attempt,
		SandboxState: sandbox.State.String(), SandboxPIDNamespace: sandbox.Linux.Namespaces.Options.Pid.String(),
		SandboxPIDTargetID: sandbox.Linux.Namespaces.Options.TargetId,
		ObservedFrom:       from, ObservedThrough: through,
	}, nil
}

func (observer *RuntimeContainerObserver) readBoot() (uuid.UUID, error) {
	value, err := observer.bootID()
	if err != nil {
		return uuid.Nil, err
	}
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil || id.String() != value {
		return uuid.Nil, errors.New("node boot identity is invalid")
	}
	return id, nil
}

func (observer *RuntimeContainerObserver) readContainer(ctx context.Context, target RuntimeContainerTarget) (*runtimev1.ContainerStatus, *runtimev1.PodSandboxStatus, error) {
	listed, err := observer.reader.ListContainers(ctx, &runtimev1.ListContainersRequest{Filter: &runtimev1.ContainerFilter{
		Id: target.ContainerID, PodSandboxId: target.SandboxID,
	}})
	if err != nil {
		return nil, nil, fmt.Errorf("locate exact CRI container: %w", err)
	}
	if listed == nil || len(listed.Containers) != 1 || listed.Containers[0] == nil {
		return nil, nil, errors.New("exact CRI container is absent or ambiguous")
	}
	entry := listed.Containers[0]
	if entry.Id != target.ContainerID || entry.PodSandboxId != target.SandboxID ||
		entry.Metadata == nil || entry.Metadata.Name != target.ContainerName || entry.Metadata.Attempt != target.ContainerAttempt {
		return nil, nil, errors.New("CRI container listing does not match the exact target")
	}
	response, err := observer.reader.ContainerStatus(ctx, &runtimev1.ContainerStatusRequest{ContainerId: target.ContainerID})
	if err != nil {
		return nil, nil, fmt.Errorf("read exact CRI container: %w", err)
	}
	status := response.GetStatus()
	if status == nil || status.Id != target.ContainerID || status.Metadata == nil || !proto.Equal(status.Metadata, entry.Metadata) ||
		status.CreatedAt != entry.CreatedAt || status.State != entry.State || status.ImageRef != entry.ImageRef ||
		!validText(status.ImageRef, 1024) || !validCRIContainerTimes(status) {
		return nil, nil, errors.New("CRI container status is incomplete or inconsistent")
	}
	pod, err := observer.reader.PodSandboxStatus(ctx, &runtimev1.PodSandboxStatusRequest{PodSandboxId: target.SandboxID})
	if err != nil {
		return nil, nil, fmt.Errorf("read exact CRI sandbox: %w", err)
	}
	sandbox := pod.GetStatus()
	if sandbox == nil || sandbox.Id != target.SandboxID || sandbox.Metadata == nil ||
		sandbox.Metadata.Uid != target.PodUID.String() || sandbox.Metadata.Namespace != target.PodNamespace || sandbox.Metadata.Name != target.PodName ||
		sandbox.CreatedAt <= 0 || sandbox.CreatedAt > status.CreatedAt ||
		(sandbox.State != runtimev1.PodSandboxState_SANDBOX_READY && sandbox.State != runtimev1.PodSandboxState_SANDBOX_NOTREADY) ||
		sandbox.Linux == nil || sandbox.Linux.Namespaces == nil || sandbox.Linux.Namespaces.Options == nil ||
		sandbox.Linux.Namespaces.Options.Pid < runtimev1.NamespaceMode_POD || sandbox.Linux.Namespaces.Options.Pid > runtimev1.NamespaceMode_TARGET {
		return nil, nil, errors.New("CRI sandbox identity or namespace status is incomplete")
	}
	options := sandbox.Linux.Namespaces.Options
	if options.Pid == runtimev1.NamespaceMode_TARGET && !runtimeContainerIDPattern.MatchString(options.TargetId) ||
		options.Pid != runtimev1.NamespaceMode_TARGET && options.TargetId != "" {
		return nil, nil, errors.New("CRI sandbox namespace target is inconsistent")
	}
	return proto.CloneOf(status), proto.CloneOf(sandbox), nil
}

func validCRIContainerTimes(status *runtimev1.ContainerStatus) bool {
	if status.CreatedAt <= 0 || status.StartedAt < 0 || status.FinishedAt < 0 {
		return false
	}
	switch status.State {
	case runtimev1.ContainerState_CONTAINER_CREATED:
		return status.StartedAt == 0 && status.FinishedAt == 0 && status.ExitCode == 0
	case runtimev1.ContainerState_CONTAINER_RUNNING:
		return status.StartedAt >= status.CreatedAt && status.FinishedAt == 0 && status.ExitCode == 0
	case runtimev1.ContainerState_CONTAINER_EXITED:
		return status.FinishedAt >= status.CreatedAt && (status.StartedAt == 0 || status.StartedAt >= status.CreatedAt && status.StartedAt <= status.FinishedAt)
	default:
		return false
	}
}

func criTimestamp(value int64) time.Time {
	if value == 0 {
		return time.Time{}
	}
	return time.Unix(0, value).UTC()
}
