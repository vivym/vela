package nodeagent

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

var ErrRuntimePlannedImage = errors.New("runtime executable or task arguments do not match the Registry-bound image entrypoint")

type RuntimePlannedImageCallerConfig struct {
	Plan           *RuntimeLaunchPlan
	Pods           RuntimeLaunchPodReader
	Caller         *RuntimeCaller
	Images         *RuntimeImageObserver
	StateDirectory string
	RuntimePolicy  RuntimeTaskRuntimePolicy
	remoteCLI      bool
	publication    *RuntimeStartupPublicationConfig
}

// RuntimePlannedImageCallerObservation binds a sampled live executable and
// task-created argv to the approved image's default entrypoint. Registry/Pod,
// actual CRI/task, image content and original process are independently read.
// It does not approve env/config files, effective mounts, loaded memory,
// descendants, current Fleet activation or a startup grant.
type RuntimePlannedImageCallerObservation struct {
	Planned   RuntimePlannedCallerObservation
	Image     RuntimeImageExecutableObservation
	Task      *RuntimeTaskLaunch
	RemoteCLI *RuntimeRemoteCLIObservation
}

func (observer *RuntimeContainerObserver) ObservePlannedImageCaller(ctx context.Context, config RuntimePlannedImageCallerConfig) (*RuntimePlannedImageCallerObservation, error) {
	var declaration []byte
	if config.Plan != nil {
		declaration = config.Plan.manifest
	}
	return observer.observePlannedImageCaller(ctx, config, declaration)
}

func (observer *RuntimeContainerObserver) observePlannedImageCaller(ctx context.Context, config RuntimePlannedImageCallerConfig, declaration []byte) (*RuntimePlannedImageCallerObservation, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	plan := config.Plan
	if plan == nil || plan.binding == nil || config.Images == nil || config.Images.namespace != "k8s.io" {
		return nil, ErrRuntimePlannedImage
	}
	var manifest string
	for _, container := range plan.pod.Spec.Containers {
		if container.Name != "model-runtime" {
			continue
		}
		// Current Fleet assembly uses image defaults. Explicit Pod command/args
		// need their own composition rule before this adapter can support them.
		if manifest != "" || len(container.Command) != 0 || len(container.Args) != 0 {
			return nil, ErrRuntimePlannedImage
		}
		_, digest, ok := strings.Cut(container.Image, "@")
		if !ok || !validRuntimeImageDigest(digest) {
			return nil, ErrRuntimePlannedImage
		}
		manifest = digest
	}
	if manifest == "" {
		return nil, ErrRuntimePlannedImage
	}
	ctx, cancel := context.WithTimeout(ctx, runtimeImageTimeout)
	defer cancel()
	if err := config.Images.sameDaemon(observer); err != nil {
		return nil, err
	}
	first, err := observer.observePlannedCaller(ctx, plan, config.Pods, config.Caller, declaration)
	if err != nil {
		return nil, err
	}
	task, err := observer.ObserveTaskLaunch(ctx, config.StateDirectory, first.Caller.Container.Target, config.Caller)
	if err != nil {
		return nil, err
	}
	if err := task.CheckRuntimeMechanism(config.RuntimePolicy); err != nil {
		return nil, err
	}
	image, err := config.Images.InspectLaunch(ctx, manifest)
	if err != nil {
		return nil, err
	}
	imageConfiguration, err := image.Configuration()
	if err != nil {
		return nil, err
	}
	arguments, err := runtimeImageDefaultArguments(imageConfiguration.Config)
	if err != nil {
		return nil, err
	}
	configuration, err := task.Configuration()
	if err != nil {
		return nil, err
	}
	if !slices.Equal(configuration.Process.Args, arguments) || first.Caller.Container.ImageRef != image.executable.Target.ConfigDigest ||
		first.Executable.Digest != image.executable.Digest || first.Executable.SizeBytes != image.executable.SizeBytes ||
		first.Executable.FileUID != 0 || first.Executable.FileGID != 0 || first.Executable.FileLinks != 1 ||
		first.Executable.FileMode&0o7022 != 0 || first.Executable.FileMode&unix.S_IFMT != unix.S_IFREG {
		return nil, ErrRuntimePlannedImage
	}
	var cli *RuntimeRemoteCLIObservation
	if config.remoteCLI {
		if config.publication == nil || plan.pod.Spec.Hostname != "" || plan.pod.Spec.Subdomain != "" {
			return nil, ErrRuntimeRemoteCLI
		}
		if err := checkRemoteCLIConfiguration(imageConfiguration.Config, configuration.Process, config.publication.BootstrapPath, plan.pod.Name); err != nil {
			return nil, err
		}
		observed, err := config.Caller.inspectRemoteCLIVectors(ctx, configuration.Process)
		if err != nil {
			return nil, err
		}
		cli = &observed
	}
	last, err := observer.observePlannedCaller(ctx, plan, config.Pods, config.Caller, declaration)
	if err != nil {
		return nil, err
	}
	if !samePlannedImageCaller(first, last) {
		return nil, ErrRuntimePlannedImage
	}
	taskProcess := task.Caller.Process
	taskProcess.ObservedAt = last.Caller.Process.ObservedAt
	if taskProcess != last.Caller.Process || image.executable.BootID != last.Caller.Container.BootID {
		return nil, ErrRuntimePlannedImage
	}
	if err := errors.Join(config.Images.sameDaemon(observer), observer.check(), config.Images.local.check(), ctx.Err()); err != nil {
		return nil, err
	}
	return &RuntimePlannedImageCallerObservation{Planned: last, Image: image.executable, Task: task, RemoteCLI: cli}, nil
}

func samePlannedImageCaller(first, last RuntimePlannedCallerObservation) bool {
	for _, value := range []*RuntimePlannedCallerObservation{&first, &last} {
		value.ObservedFrom, value.ObservedThrough = time.Time{}, time.Time{}
		value.Caller.ObservedFrom, value.Caller.ObservedThrough = time.Time{}, time.Time{}
		value.Caller.Container.ObservedFrom, value.Caller.Container.ObservedThrough = time.Time{}, time.Time{}
		value.Caller.Process.ObservedAt = time.Time{}
		value.Executable.ObservedFrom, value.Executable.ObservedThrough = time.Time{}, time.Time{}
		value.Executable.Process.ObservedAt = time.Time{}
	}
	return reflect.DeepEqual(first, last)
}
