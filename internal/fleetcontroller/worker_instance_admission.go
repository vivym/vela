package fleetcontroller

import (
	"context"
	"errors"

	corev1 "k8s.io/api/core/v1"
)

type protectedPodCreateValidator interface {
	ValidateProtectedPodCreate(context.Context, corev1.Pod) error
}

type WorkerInstancePodAdmissionValidator struct {
	legacy  protectedPodCreateValidator
	desired map[string]corev1.Pod
}

func NewWorkerInstancePodAdmissionValidator(
	legacy protectedPodCreateValidator,
	rollouts []ResidencyPlanRollout,
) (*WorkerInstancePodAdmissionValidator, error) {
	if legacy == nil || len(rollouts) == 0 {
		return nil, errors.New("legacy Pod validator and ResidencyPlan rollouts are required")
	}
	desired := make(map[string]corev1.Pod)
	for _, rollout := range rollouts {
		if err := ValidateResidencyPlanRollout(rollout); err != nil {
			return nil, err
		}
		for _, bundle := range rollout.WorkerBundles {
			pods, err := materializeWorkerInstancePods(bundle)
			if err != nil {
				return nil, err
			}
			for _, pod := range pods {
				key := runtimeResourceName(pod.Namespace, pod.Name)
				if _, exists := desired[key]; exists {
					return nil, errors.New("ResidencyPlan rollouts reuse a WorkerInstance Pod identity")
				}
				desired[key] = pod
			}
		}
	}
	return &WorkerInstancePodAdmissionValidator{legacy: legacy, desired: desired}, nil
}

func (validator *WorkerInstancePodAdmissionValidator) ValidateProtectedPodCreate(
	ctx context.Context,
	pod corev1.Pod,
) error {
	if validator == nil || validator.legacy == nil {
		return ErrProtectedResourceDrift
	}
	if pod.Labels[workerInstanceIDLabel] == "" {
		return validator.legacy.ValidateProtectedPodCreate(ctx, pod)
	}
	if ctx == nil || pod.UID != "" || len(pod.OwnerReferences) != 0 ||
		pod.Labels[workerIDLabel] != "" || pod.Labels[workerEpochLabel] != "" {
		return ErrProtectedResourceDrift
	}
	desired, exists := validator.desired[runtimeResourceName(pod.Namespace, pod.Name)]
	if !exists || !workerInstancePodMatches(pod, desired) {
		return ErrProtectedResourceDrift
	}
	return nil
}
