package nodeagent

import (
	"context"
	"reflect"
	"time"
)

type RuntimeStartupImageConfig struct {
	Images         *RuntimeImageObserver
	StateDirectory string
	RuntimePolicy  RuntimeTaskRuntimePolicy
}

// ReserveImageRemote checks the approved image's default entrypoint and task
// mechanism before recording intent and on both sides of the single Fleet call.
// All observations come from this invocation's original caller/daemon, never
// caller-supplied records or recovered history. A failure after intent consumes
// this attempt even if the Fleet reply was lost. No method here issues a grant;
// effective env/config/mount approval and execution continuity remain separate.
func (ledger *RuntimeStartupLedger) ReserveImageRemote(ctx context.Context, config RuntimeStartupReservationConfig, image RuntimeStartupImageConfig) (RuntimeStartupReservationRecord, error) {
	if image.Images == nil {
		return RuntimeStartupReservationRecord{}, ErrRuntimePlannedImage
	}
	return ledger.reserveRemote(ctx, config, &runtimeStartupImageCheck{config: image})
}

type runtimeStartupImageCheck struct {
	config    RuntimeStartupImageConfig
	first     *RuntimePlannedImageCallerObservation
	remoteCLI bool
}

func (check *runtimeStartupImageCheck) inspect(ctx context.Context, config RuntimeStartupReservationConfig) error {
	if check == nil {
		return nil // ReserveRemote retains its observation-only reservation contract.
	}
	current, err := config.Observer.ObserveStartupImageCaller(ctx, RuntimePlannedImageCallerConfig{
		Plan: config.Plan, Pods: config.Pods, Caller: config.Caller, Images: check.config.Images,
		StateDirectory: check.config.StateDirectory, RuntimePolicy: check.config.RuntimePolicy,
		remoteCLI: check.remoteCLI, publication: config.publication,
	})
	if err != nil {
		return err
	}
	if check.first == nil {
		check.first = current
		return nil
	}
	if !samePlannedImageCaller(check.first.Planned, current.Planned) {
		return ErrRuntimePlannedImage
	}
	firstImage, lastImage := check.first.Image, current.Image
	firstImage.ObservedFrom, firstImage.ObservedThrough = lastImage.ObservedFrom, lastImage.ObservedThrough
	firstTask, lastTask := *check.first.Task, *current.Task
	firstTask.ObservedFrom, firstTask.ObservedThrough = time.Time{}, time.Time{}
	lastTask.ObservedFrom, lastTask.ObservedThrough = time.Time{}, time.Time{}
	// Each task observation already matches its planned caller's original
	// process. Compare all retained task bytes/file identities without timestamps.
	firstTask.Caller, lastTask.Caller = RuntimeContainerCallerObservation{}, RuntimeContainerCallerObservation{}
	if firstImage != lastImage || !reflect.DeepEqual(firstTask, lastTask) || !reflect.DeepEqual(check.first.RemoteCLI, current.RemoteCLI) {
		return ErrRuntimePlannedImage
	}
	if (check.first.Entrypoint == nil) != (current.Entrypoint == nil) {
		return ErrRuntimePlannedImage
	}
	if current.Entrypoint != nil {
		first, last := *check.first.Entrypoint, *current.Entrypoint
		first.ObservedFrom, first.ObservedThrough = last.ObservedFrom, last.ObservedThrough
		if first != last {
			return ErrRuntimePlannedImage
		}
	}
	return nil
}

func (check *runtimeStartupImageCheck) check(ctx context.Context, config RuntimeStartupReservationConfig, record RuntimeStartupRecord) error {
	if check == nil {
		return nil
	}
	if err := check.inspect(ctx, config); err != nil {
		return err
	}
	observed := check.first.Planned
	if !reflect.DeepEqual(check.first.RemoteCLI, record.Remote.RemoteCLI) {
		return ErrRuntimeRemoteCLI
	}
	originalOwner, originalExecutable := record.Owner, record.Remote.Executable
	originalOwner.ObservedFrom, originalOwner.ObservedThrough = observed.Caller.ObservedFrom, observed.Caller.ObservedThrough
	originalOwner.Container.ObservedFrom, originalOwner.Container.ObservedThrough = observed.Caller.Container.ObservedFrom, observed.Caller.Container.ObservedThrough
	originalOwner.Process.ObservedAt = observed.Caller.Process.ObservedAt
	originalExecutable.ObservedFrom, originalExecutable.ObservedThrough = observed.Executable.ObservedFrom, observed.Executable.ObservedThrough
	originalExecutable.Process.ObservedAt = observed.Executable.Process.ObservedAt
	if originalOwner != observed.Caller || originalExecutable != observed.Executable || record.PodResourceVersion != observed.PodResourceVersion {
		return ErrRuntimePlannedImage
	}
	return nil
}
