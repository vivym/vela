package fleetcontroller

import (
	"context"
	"errors"

	corev1 "k8s.io/api/core/v1"
)

type WorkerInstancePodAdmissionValidator struct {
	desired map[string]corev1.Pod
}

func NewWorkerInstancePodAdmissionValidator(
	rollouts []ResidencyPlanRollout,
) (*WorkerInstancePodAdmissionValidator, error) {
	if len(rollouts) == 0 {
		return nil, errors.New("ResidencyPlan rollouts are required")
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
	return &WorkerInstancePodAdmissionValidator{desired: desired}, nil
}

func (validator *WorkerInstancePodAdmissionValidator) ValidateProtectedPodCreate(
	ctx context.Context,
	pod corev1.Pod,
) error {
	if validator == nil {
		return ErrProtectedResourceDrift
	}
	if ctx == nil || pod.UID != "" || len(pod.OwnerReferences) != 0 ||
		pod.Labels[workerInstanceIDLabel] == "" ||
		pod.Labels[workerIDLabel] != "" || pod.Labels[workerEpochLabel] != "" {
		return ErrProtectedResourceDrift
	}
	desired, exists := validator.desired[runtimeResourceName(pod.Namespace, pod.Name)]
	if !exists || !workerInstancePodMatches(pod, desired) {
		return ErrProtectedResourceDrift
	}
	return nil
}
