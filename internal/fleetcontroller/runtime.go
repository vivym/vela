package fleetcontroller

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
)

type RuntimeConfig struct {
	ResidencyPlanRollouts    []ResidencyPlanRollout
	WorkerInstanceController *ResidencyPlanRolloutController
	PollInterval             time.Duration
	ReportError              func(error)
}

type RuntimeResult struct {
	ResidencyPlansConverged   int
	WorkerInstancePodsCreated int
}

type Runtime struct {
	residencyRollouts []ResidencyPlanRollout
	workerInstances   *ResidencyPlanRolloutController
	pollInterval      time.Duration
	reportError       func(error)
	admissionReady    atomic.Bool
	converged         atomic.Bool
}

func NewRuntime(config RuntimeConfig) (*Runtime, error) {
	if config.PollInterval <= 0 || len(config.ResidencyPlanRollouts) == 0 ||
		config.WorkerInstanceController == nil {
		return nil, errors.New("fleet runtime configuration is invalid")
	}
	rolloutPlans := make(map[uuid.UUID]struct{}, len(config.ResidencyPlanRollouts))
	for _, rollout := range config.ResidencyPlanRollouts {
		if err := ValidateResidencyPlanRollout(rollout); err != nil {
			return nil, err
		}
		if _, exists := rolloutPlans[rollout.ApprovedPlan.ID]; exists {
			return nil, errors.New("fleet runtime contains a duplicate ResidencyPlan rollout")
		}
		rolloutPlans[rollout.ApprovedPlan.ID] = struct{}{}
	}
	runtime := &Runtime{
		residencyRollouts: cloneResidencyPlanRollouts(config.ResidencyPlanRollouts),
		workerInstances:   config.WorkerInstanceController,
		pollInterval:      config.PollInterval,
		reportError:       config.ReportError,
	}
	runtime.admissionReady.Store(true)
	return runtime, nil
}

func cloneResidencyPlanRollouts(rollouts []ResidencyPlanRollout) []ResidencyPlanRollout {
	cloned := make([]ResidencyPlanRollout, len(rollouts))
	for index, rollout := range rollouts {
		cloned[index] = rollout
		cloned[index].ApprovedPlan.CapacityPools = append(
			[]fleet.PlannedCapacityPool(nil), rollout.ApprovedPlan.CapacityPools...,
		)
		cloned[index].ApprovedPlan.WorkerBundles = append(
			[]fleet.PlannedWorkerBundle(nil), rollout.ApprovedPlan.WorkerBundles...,
		)
		cloned[index].ApprovedPlan.WorkerInstances = append(
			[]fleet.PlannedWorkerInstance(nil), rollout.ApprovedPlan.WorkerInstances...,
		)
		cloned[index].WorkerBundles = make([]WorkerBundleActuation, len(rollout.WorkerBundles))
		for bundleIndex, bundle := range rollout.WorkerBundles {
			cloned[index].WorkerBundles[bundleIndex] = cloneWorkerBundleActuation(bundle)
		}
	}
	return cloned
}

func cloneWorkerBundleActuation(bundle WorkerBundleActuation) WorkerBundleActuation {
	cloned := bundle
	cloned.WorkerInstances = make([]WorkerInstanceActuation, len(bundle.WorkerInstances))
	for index, worker := range bundle.WorkerInstances {
		cloned.WorkerInstances[index] = worker
		cloned.WorkerInstances[index].ModelRuntimes = make([]ModelRuntimeProcess, len(worker.ModelRuntimes))
		for runtimeIndex, modelRuntime := range worker.ModelRuntimes {
			cloned.WorkerInstances[index].ModelRuntimes[runtimeIndex] = modelRuntime
			cloned.WorkerInstances[index].ModelRuntimes[runtimeIndex].Command = append(
				[]string(nil), modelRuntime.Command...,
			)
			cloned.WorkerInstances[index].ModelRuntimes[runtimeIndex].Environment = append(
				[]string(nil), modelRuntime.Environment...,
			)
		}
		cloned.WorkerInstances[index].Members = make([]WorkerMemberActuation, len(worker.Members))
		for memberIndex, member := range worker.Members {
			cloned.WorkerInstances[index].Members[memberIndex] = member
			cloned.WorkerInstances[index].Members[memberIndex].DeviceConstraints = append(
				[]DeviceConstraint(nil), member.DeviceConstraints...,
			)
		}
	}
	return cloned
}

func (runtime *Runtime) RunOnce(ctx context.Context) (RuntimeResult, error) {
	if runtime == nil || runtime.workerInstances == nil || len(runtime.residencyRollouts) == 0 {
		if runtime != nil {
			runtime.admissionReady.Store(false)
			runtime.converged.Store(false)
		}
		return RuntimeResult{}, errors.New("fleet runtime is not configured")
	}
	result := RuntimeResult{}
	var failures []error
	allConverged := true
	for _, rollout := range runtime.residencyRollouts {
		actuation, err := runtime.workerInstances.Reconcile(ctx, rollout)
		if err != nil {
			allConverged = false
			failures = append(failures, fmt.Errorf(
				"reconcile ResidencyPlan %s: %w", rollout.ApprovedPlan.ID, err,
			))
			continue
		}
		result.WorkerInstancePodsCreated += actuation.CreatedPods
		if actuation.Converged {
			result.ResidencyPlansConverged++
		} else {
			allConverged = false
		}
	}
	cycleErr := errors.Join(failures...)
	runtime.converged.Store(cycleErr == nil && allConverged)
	return result, cycleErr
}

func (runtime *Runtime) Ready() bool {
	return runtime != nil && runtime.admissionReady.Load()
}

func (runtime *Runtime) Converged() bool {
	return runtime != nil && runtime.converged.Load()
}

func (runtime *Runtime) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("fleet runtime context is required")
	}
	if _, err := runtime.RunOnce(ctx); err != nil && runtime.reportError != nil {
		runtime.reportError(err)
	}
	ticker := time.NewTicker(runtime.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := runtime.RunOnce(ctx); err != nil && runtime.reportError != nil {
				runtime.reportError(err)
			}
		}
	}
}
