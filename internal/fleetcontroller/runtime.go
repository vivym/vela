package fleetcontroller

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type PendingWorkerPodLister interface {
	ListPendingWorkerPods(context.Context) ([]WorkerPod, error)
}

type RuntimeConfig struct {
	DesiredRevisions []DesiredRevision
	RetirementPlans  []RetirementPlan
	PollInterval     time.Duration
	ReportError      func(error)
}

type RuntimeResult struct {
	DesiredRevisionsConverged int
	WorkerPodsBound           int
	RetirementPlansPending    int
	RetirementPlansCompleted  int
	WorkerPodsRetired         int
}

type Runtime struct {
	pods             PendingWorkerPodLister
	reconciler       *Reconciler
	desiredRevisions []DesiredRevision
	retirementPlans  []RetirementPlan
	pollInterval     time.Duration
	reportError      func(error)
	admissionReady   atomic.Bool
	converged        atomic.Bool
}

func NewRuntime(
	pods PendingWorkerPodLister,
	reconciler *Reconciler,
	config RuntimeConfig,
) (*Runtime, error) {
	if pods == nil || reconciler == nil || config.PollInterval <= 0 {
		return nil, errors.New("fleet runtime configuration is invalid")
	}
	if err := ValidateRuntimeConfiguration(config.DesiredRevisions, config.RetirementPlans); err != nil {
		return nil, err
	}
	desiredRevisions := make([]DesiredRevision, 0, len(config.DesiredRevisions))
	for _, desired := range config.DesiredRevisions {
		cloned := desired
		cloned.NodeSelector = cloneMap(desired.NodeSelector)
		cloned.Placements = clonePlacements(desired.Placements)
		desiredRevisions = append(desiredRevisions, cloned)
	}
	retirementPlans := make([]RetirementPlan, 0, len(config.RetirementPlans))
	for _, plan := range config.RetirementPlans {
		cloned := plan
		cloned.Placements = cloneRetirementPlacements(plan.Placements)
		retirementPlans = append(retirementPlans, cloned)
	}
	runtime := &Runtime{
		pods:             pods,
		reconciler:       reconciler,
		desiredRevisions: desiredRevisions,
		retirementPlans:  retirementPlans,
		pollInterval:     config.PollInterval,
		reportError:      config.ReportError,
	}
	runtime.admissionReady.Store(true)
	return runtime, nil
}

func ValidateRuntimeConfiguration(
	desiredRevisions []DesiredRevision,
	retirementPlans []RetirementPlan,
) error {
	if len(desiredRevisions) == 0 {
		return errors.New("fleet runtime requires at least one desired revision")
	}
	desiredPoolIDs := make(map[string]struct{}, len(desiredRevisions))
	desiredPoolNames := make(map[string]struct{}, len(desiredRevisions))
	desiredDaemonSetNames := make(map[string]struct{})
	desiredNodeIdentities := make(map[string]struct{})
	desiredConfigMapNames := make(map[string]struct{})
	desiredTLSSecretNames := make(map[string]struct{})
	for _, desired := range desiredRevisions {
		if err := ValidateDesiredRevision(desired); err != nil {
			return err
		}
		if !claimRuntimeIdentity(desiredPoolIDs, desired.WorkerPoolID.String()) {
			return errors.New("fleet runtime contains a duplicate desired WorkerPool id")
		}
		if !claimRuntimeIdentity(desiredPoolNames, runtimeResourceName(desired.Namespace, desired.Name)) {
			return errors.New("fleet runtime contains a duplicate desired WorkerPool name")
		}
		for _, placement := range desired.Placements {
			if !claimRuntimeIdentity(desiredNodeIdentities, placement.NodeIdentity) {
				return errors.New("fleet runtime contains a duplicate desired Node identity")
			}
			if !claimRuntimeIdentity(
				desiredDaemonSetNames,
				runtimeResourceName(desired.Namespace, placement.DaemonSetName),
			) {
				return errors.New("fleet runtime contains a duplicate desired DaemonSet name")
			}
			for _, name := range []string{
				placement.WorkerRuntimeConfigMap,
				placement.RunnerProfilesConfigMap,
				placement.RunnerGPURolesConfigMap,
			} {
				if !claimRuntimeIdentity(
					desiredConfigMapNames,
					runtimeResourceName(desired.Namespace, name),
				) {
					return errors.New("fleet runtime contains a duplicate desired ConfigMap name")
				}
			}
			if !claimRuntimeIdentity(
				desiredTLSSecretNames,
				runtimeResourceName(desired.Namespace, placement.WorkerControlTLSSecret),
			) {
				return errors.New("fleet runtime contains a duplicate desired Worker TLS Secret name")
			}
		}
	}

	planRevisions := make(map[string]struct{}, len(retirementPlans))
	retiredPoolIDs := make(map[string]struct{}, len(retirementPlans))
	retiredPoolNames := make(map[string]struct{}, len(retirementPlans))
	retiredDaemonSetNames := make(map[string]struct{})
	retiredResourceUIDs := make(map[string]struct{})
	retiredWorkerIDs := make(map[string]struct{})
	retiredPodNames := make(map[string]struct{})
	retiredDrainOperations := make(map[string]struct{})
	for _, plan := range retirementPlans {
		if err := ValidateRetirementPlan(plan); err != nil {
			return err
		}
		poolID := plan.WorkerPoolID.String()
		poolName := runtimeResourceName(plan.Namespace, plan.WorkerPoolName)
		if _, overlapsDesired := desiredPoolIDs[poolID]; overlapsDesired {
			return errors.New("fleet runtime overlaps desired and retiring WorkerPool id")
		}
		if _, overlapsDesired := desiredPoolNames[poolName]; overlapsDesired {
			return errors.New("fleet runtime overlaps desired and retiring WorkerPool name")
		}
		if !claimRuntimeIdentity(planRevisions, plan.Revision) {
			return errors.New("fleet runtime contains a duplicate retirement plan revision")
		}
		if !claimRuntimeIdentity(retiredPoolIDs, poolID) {
			return errors.New("fleet runtime retires a WorkerPool id more than once")
		}
		if !claimRuntimeIdentity(retiredPoolNames, poolName) {
			return errors.New("fleet runtime retires a WorkerPool name more than once")
		}
		if !claimRuntimeIdentity(retiredResourceUIDs, plan.WorkerPoolKubernetesUID) {
			return errors.New("fleet runtime reuses a retirement Kubernetes resource UID")
		}
		for _, placement := range plan.Placements {
			daemonSetName := runtimeResourceName(plan.Namespace, placement.DaemonSetName)
			if _, overlapsDesired := desiredDaemonSetNames[daemonSetName]; overlapsDesired {
				return errors.New("fleet runtime overlaps desired and retiring DaemonSet name")
			}
			if !claimRuntimeIdentity(retiredDaemonSetNames, daemonSetName) {
				return errors.New("fleet runtime retires a DaemonSet name more than once")
			}
			if !claimRuntimeIdentity(retiredResourceUIDs, placement.DaemonSetKubernetesUID) {
				return errors.New("fleet runtime reuses a retirement Kubernetes resource UID")
			}
			for _, worker := range placement.Workers {
				if !claimRuntimeIdentity(retiredDrainOperations, worker.OperationID.String()) {
					return errors.New("fleet runtime reuses a retirement DrainOperation id")
				}
				if !claimRuntimeIdentity(retiredWorkerIDs, worker.WorkerID.String()) {
					return errors.New("fleet runtime retires a Worker id more than once")
				}
				if !claimRuntimeIdentity(
					retiredPodNames,
					runtimeResourceName(plan.Namespace, worker.PodName),
				) {
					return errors.New("fleet runtime retires a Worker Pod name more than once")
				}
				if !claimRuntimeIdentity(retiredResourceUIDs, worker.PodKubernetesUID) {
					return errors.New("fleet runtime reuses a retirement Kubernetes resource UID")
				}
			}
		}
	}
	return nil
}

func cloneRetirementPlacements(placements []RetirementPlacement) []RetirementPlacement {
	cloned := make([]RetirementPlacement, len(placements))
	for index, placement := range placements {
		cloned[index] = placement
		cloned[index].Workers = append([]WorkerRetirement(nil), placement.Workers...)
	}
	return cloned
}

func claimRuntimeIdentity(seen map[string]struct{}, identity string) bool {
	if _, exists := seen[identity]; exists {
		return false
	}
	seen[identity] = struct{}{}
	return true
}

func runtimeResourceName(namespace, name string) string {
	return namespace + "\x00" + name
}

func (runtime *Runtime) RunOnce(ctx context.Context) (RuntimeResult, error) {
	if runtime == nil || runtime.pods == nil || runtime.reconciler == nil {
		if runtime != nil {
			runtime.admissionReady.Store(false)
			runtime.converged.Store(false)
		}
		return RuntimeResult{}, errors.New("fleet runtime is not configured")
	}
	result := RuntimeResult{}
	var failures []error
	reconciledPools := make(map[uuid.UUID]struct{}, len(runtime.desiredRevisions))
	for _, desired := range runtime.desiredRevisions {
		if _, err := runtime.reconciler.Reconcile(ctx, desired); err != nil {
			failures = append(failures, fmt.Errorf(
				"reconcile Fleet desired revision %s/%s: %w",
				desired.Namespace,
				desired.Name,
				err,
			))
			continue
		}
		reconciledPools[desired.WorkerPoolID] = struct{}{}
		result.DesiredRevisionsConverged++
	}
	pods, err := runtime.pods.ListPendingWorkerPods(ctx)
	if err != nil {
		failures = append(failures, fmt.Errorf("list pending Worker Pods: %w", err))
	} else {
		for _, pod := range pods {
			desired, err := runtime.desiredRevisionForPod(pod)
			if err != nil {
				failures = append(failures, fmt.Errorf(
					"match pending Worker Pod %s/%s: %w",
					pod.Metadata.Namespace,
					pod.Metadata.Name,
					err,
				))
				continue
			}
			if _, reconciled := reconciledPools[desired.WorkerPoolID]; !reconciled {
				failures = append(failures, fmt.Errorf(
					"bind pending Worker Pod %s/%s: desired revision did not reconcile in this cycle",
					pod.Metadata.Namespace,
					pod.Metadata.Name,
				))
				continue
			}
			if _, err := runtime.reconciler.BindWorkerPodIdentity(ctx, pod, desired); err != nil {
				failures = append(failures, fmt.Errorf(
					"bind pending Worker Pod %s/%s: %w",
					pod.Metadata.Namespace,
					pod.Metadata.Name,
					err,
				))
				continue
			}
			result.WorkerPodsBound++
		}
	}
	for _, plan := range runtime.retirementPlans {
		retirement, err := runtime.reconciler.ReconcileRetirement(ctx, plan)
		if err != nil {
			failures = append(failures, fmt.Errorf(
				"reconcile Fleet retirement plan %s: %w",
				plan.Revision,
				err,
			))
			continue
		}
		if retirement.Pending {
			result.RetirementPlansPending++
		}
		if retirement.Completed {
			result.RetirementPlansCompleted++
		}
		result.WorkerPodsRetired += retirement.WorkerPodsRetired
	}
	cycleErr := errors.Join(failures...)
	runtime.converged.Store(cycleErr == nil && result.RetirementPlansPending == 0)
	return result, cycleErr
}

func (runtime *Runtime) desiredRevisionForPod(pod WorkerPod) (DesiredRevision, error) {
	workerPoolID, _, err := pendingWorkerPodIdentity(pod)
	if err != nil {
		return DesiredRevision{}, err
	}
	for _, desired := range runtime.desiredRevisions {
		if desired.WorkerPoolID == workerPoolID && desired.Namespace == pod.Metadata.Namespace &&
			desired.Revision == pod.Metadata.Labels[fleetRevisionLabel] {
			return desired, nil
		}
	}
	return DesiredRevision{}, ErrWorkerIdentityConflict
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
	ticker := time.NewTicker(runtime.pollInterval)
	defer ticker.Stop()
	for {
		_, err := runtime.RunOnce(ctx)
		if err != nil && runtime.reportError != nil {
			runtime.reportError(err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
