package fleetcontroller

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
)

const maximumRetirementWorkers = 4096

var ErrDrainExpired = errors.New("routine Worker drain expired without mutation authority")

type WorkerRetirement struct {
	OperationID      uuid.UUID
	WorkerID         uuid.UUID
	WorkerEpoch      int64
	PodName          string
	PodKubernetesUID string
}

type RetirementPlan struct {
	Revision                string
	WorkerPoolID            uuid.UUID
	Namespace               string
	WorkerPoolName          string
	WorkerPoolKubernetesUID string
	DaemonSetName           string
	DaemonSetKubernetesUID  string
	Reason                  string
	Deadline                time.Time
	Workers                 []WorkerRetirement
}

type RetirementResult struct {
	Pending           bool
	Completed         bool
	WorkerPodsRetired int
}

func ValidateRetirementPlan(plan RetirementPlan) error {
	if !validSHA256(plan.Revision) || plan.WorkerPoolID == uuid.Nil ||
		!validResourceName(plan.Namespace) || !validResourceName(plan.WorkerPoolName) ||
		!validBoundedText(plan.WorkerPoolKubernetesUID, 200) ||
		!validResourceName(plan.DaemonSetName) ||
		!validBoundedText(plan.DaemonSetKubernetesUID, 200) ||
		!validBoundedText(plan.Reason, 1000) || plan.Deadline.IsZero() ||
		len(plan.Workers) == 0 || len(plan.Workers) > maximumRetirementWorkers {
		return errors.New("fleet retirement plan is invalid")
	}
	operationIDs := make(map[uuid.UUID]struct{}, len(plan.Workers))
	workerIDs := make(map[uuid.UUID]struct{}, len(plan.Workers))
	podNames := make(map[string]struct{}, len(plan.Workers))
	podUIDs := make(map[string]struct{}, len(plan.Workers))
	for _, worker := range plan.Workers {
		if worker.OperationID == uuid.Nil || worker.WorkerID == uuid.Nil ||
			worker.WorkerEpoch <= 0 || !validResourceName(worker.PodName) ||
			!validBoundedText(worker.PodKubernetesUID, 200) {
			return errors.New("fleet retirement Worker identity is invalid")
		}
		if _, exists := operationIDs[worker.OperationID]; exists {
			return errors.New("fleet retirement plan contains a duplicate DrainOperation id")
		}
		if _, exists := workerIDs[worker.WorkerID]; exists {
			return errors.New("fleet retirement plan contains a duplicate Worker id")
		}
		if _, exists := podNames[worker.PodName]; exists {
			return errors.New("fleet retirement plan contains a duplicate Worker Pod name")
		}
		if _, exists := podUIDs[worker.PodKubernetesUID]; exists {
			return errors.New("fleet retirement plan contains a duplicate Worker Pod UID")
		}
		operationIDs[worker.OperationID] = struct{}{}
		workerIDs[worker.WorkerID] = struct{}{}
		podNames[worker.PodName] = struct{}{}
		podUIDs[worker.PodKubernetesUID] = struct{}{}
	}
	return nil
}

func (reconciler *Reconciler) ReconcileRetirement(
	ctx context.Context,
	plan RetirementPlan,
) (RetirementResult, error) {
	if reconciler == nil || reconciler.resources == nil || reconciler.drains == nil {
		return RetirementResult{}, errors.New("fleet reconciler is not configured")
	}
	if err := ValidateRetirementPlan(plan); err != nil {
		return RetirementResult{}, err
	}
	drainIDs := make([]uuid.UUID, 0, len(plan.Workers))
	allComplete := true
	for _, worker := range plan.Workers {
		request := fleet.DrainRequest{
			OperationID:   worker.OperationID,
			WorkerID:      worker.WorkerID,
			ExpectedEpoch: worker.WorkerEpoch,
			Reason:        plan.Reason,
			Deadline:      plan.Deadline.UTC(),
		}
		drain, err := reconciler.drains.RequestDrain(ctx, request)
		if err != nil {
			return RetirementResult{}, fmt.Errorf("request planned Worker drain %s: %w", worker.OperationID, err)
		}
		if err := validateRetirementDrain(drain, request); err != nil {
			return RetirementResult{}, err
		}
		if drain.State == fleet.DrainDraining {
			drain, err = reconciler.drains.ReconcileDrain(ctx, worker.OperationID)
			if err != nil {
				return RetirementResult{}, fmt.Errorf("reconcile planned Worker drain %s: %w", worker.OperationID, err)
			}
			if err := validateRetirementDrain(drain, request); err != nil {
				return RetirementResult{}, err
			}
		}
		switch drain.State {
		case fleet.DrainComplete:
			drainIDs = append(drainIDs, worker.OperationID)
		case fleet.DrainDraining:
			allComplete = false
		case fleet.DrainExpired:
			return RetirementResult{}, fmt.Errorf("planned Worker drain %s: %w", worker.OperationID, ErrDrainExpired)
		default:
			return RetirementResult{}, ErrDrainIncomplete
		}
	}
	if !allComplete {
		return RetirementResult{Pending: true}, nil
	}

	daemonSet := ProtectedResource{
		Kind: ResourceDaemonSet, KubernetesUID: plan.DaemonSetKubernetesUID,
		Namespace: plan.Namespace, Name: plan.DaemonSetName, WorkerPoolID: plan.WorkerPoolID,
	}
	daemonSetRetired, err := reconciler.retireProtectedResource(ctx, daemonSet, drainIDs)
	if err != nil {
		return RetirementResult{}, fmt.Errorf("retire protected DaemonSet: %w", err)
	}
	if !daemonSetRetired {
		return RetirementResult{Pending: true}, nil
	}
	result := RetirementResult{}
	for index, worker := range plan.Workers {
		pod := ProtectedResource{
			Kind: ResourcePod, KubernetesUID: worker.PodKubernetesUID,
			Namespace: plan.Namespace, Name: worker.PodName, WorkerPoolID: plan.WorkerPoolID,
			WorkerID: worker.WorkerID, WorkerEpoch: worker.WorkerEpoch,
		}
		retired, err := reconciler.retireProtectedResource(ctx, pod, drainIDs[index:index+1])
		if err != nil {
			return RetirementResult{}, fmt.Errorf("retire protected Worker Pod %s: %w", worker.PodName, err)
		}
		if retired {
			result.WorkerPodsRetired++
		} else {
			result.Pending = true
			return result, nil
		}
	}
	workerPool := ProtectedResource{
		Kind: ResourceWorkerPool, KubernetesUID: plan.WorkerPoolKubernetesUID,
		Namespace: plan.Namespace, Name: plan.WorkerPoolName, WorkerPoolID: plan.WorkerPoolID,
	}
	workerPoolRetired, err := reconciler.retireProtectedResource(ctx, workerPool, drainIDs)
	if err != nil {
		return RetirementResult{}, fmt.Errorf("retire protected WorkerPool: %w", err)
	}
	if !workerPoolRetired {
		result.Pending = true
		return result, nil
	}
	result.Completed = true
	return result, nil
}

func validateRetirementDrain(result fleet.DrainResult, request fleet.DrainRequest) error {
	if result.OperationID != request.OperationID || result.WorkerID != request.WorkerID ||
		result.WorkerEpoch != request.ExpectedEpoch || !result.Deadline.Equal(request.Deadline) {
		return ErrDrainIncomplete
	}
	return nil
}

func (reconciler *Reconciler) retireProtectedResource(
	ctx context.Context,
	resource ProtectedResource,
	drainIDs []uuid.UUID,
) (bool, error) {
	request, err := retirementAuthorizationRequest(resource, drainIDs)
	if err != nil {
		return false, err
	}
	completed, err := reconciler.drains.HasRetirementCompletion(ctx, request)
	if err != nil {
		return false, fmt.Errorf("check persisted retirement completion: %w", err)
	}
	if completed {
		return true, nil
	}
	if err := reconciler.resources.AttachDrainOperations(ctx, resource, drainIDs); err != nil {
		if errors.Is(err, ErrResourceNotFound) || errors.Is(err, ErrProtectedResourceDrift) {
			return reconciler.recordRetirementCompletionIfAbsent(ctx, resource, request)
		}
		return false, fmt.Errorf("attach completed DrainOperation set: %w", err)
	}
	if err := reconciler.resources.Delete(ctx, resource); err != nil {
		if errors.Is(err, ErrResourceNotFound) {
			return reconciler.recordRetirementCompletionIfAbsent(ctx, resource, request)
		}
		return false, fmt.Errorf("delete protected resource: %w", err)
	}
	if err := reconciler.resources.RemoveFinalizer(ctx, resource); err != nil {
		if !errors.Is(err, ErrResourceNotFound) && !errors.Is(err, ErrProtectedResourceDrift) {
			return false, fmt.Errorf("remove protected resource finalizer: %w", err)
		}
	}
	return reconciler.recordRetirementCompletionIfAbsent(ctx, resource, request)
}

func (reconciler *Reconciler) recordRetirementCompletionIfAbsent(
	ctx context.Context,
	resource ProtectedResource,
	request fleet.RetirementAuthorizationRequest,
) (bool, error) {
	absent, err := reconciler.resources.IsAbsent(ctx, resource)
	if err != nil {
		return false, fmt.Errorf("observe exact protected resource absence: %w", err)
	}
	if !absent {
		return false, nil
	}
	authorized, err := reconciler.drains.HasRetirementAuthorization(ctx, request)
	if err != nil {
		return false, fmt.Errorf("check persisted retirement authorization: %w", err)
	}
	if !authorized {
		return false, ErrRetirementUnproven
	}
	completion, err := reconciler.drains.RecordRetirementCompletion(ctx, request)
	if err != nil {
		return false, fmt.Errorf("persist retirement completion: %w", err)
	}
	if completion.CompletedAt.IsZero() {
		return false, errors.New("persisted retirement completion result is invalid")
	}
	return true, nil
}

func retirementAuthorizationRequest(
	resource ProtectedResource,
	drainIDs []uuid.UUID,
) (fleet.RetirementAuthorizationRequest, error) {
	resourceKind, err := retirementResourceKind(resource.Kind)
	if err != nil {
		return fleet.RetirementAuthorizationRequest{}, err
	}
	return fleet.RetirementAuthorizationRequest{
		ResourceKind: resourceKind, KubernetesUID: resource.KubernetesUID,
		Namespace: resource.Namespace, Name: resource.Name,
		WorkerPoolID: resource.WorkerPoolID, WorkerID: resource.WorkerID,
		WorkerEpoch:       resource.WorkerEpoch,
		DrainOperationIDs: append([]uuid.UUID(nil), drainIDs...),
	}, nil
}

func retirementResourceKind(kind ResourceKind) (fleet.ProtectedResourceKind, error) {
	switch kind {
	case ResourcePod:
		return fleet.ProtectedPod, nil
	case ResourceDaemonSet:
		return fleet.ProtectedDaemonSet, nil
	case ResourceWorkerPool:
		return fleet.ProtectedWorkerPool, nil
	default:
		return "", errors.New("protected Fleet retirement resource kind is invalid")
	}
}
