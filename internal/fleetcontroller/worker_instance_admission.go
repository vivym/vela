package fleetcontroller

import (
	"context"
	"errors"

	corev1 "k8s.io/api/core/v1"
)

type StageWorkerMemberPKISecretReader interface {
	GetStageWorkerMemberPKISecret(context.Context, ResourceKey) (corev1.Secret, error)
}

type WorkerInstancePodAdmissionValidator struct {
	desiredPods          map[string]corev1.Pod
	desiredServices      map[string]corev1.Service
	desiredSecretBundles map[string]WorkerBundleActuation
	memberPKISecrets     StageWorkerMemberPKISecretReader
}

func NewWorkerInstancePodAdmissionValidator(
	rollouts []ResidencyPlanRollout,
) (*WorkerInstancePodAdmissionValidator, error) {
	return newWorkerInstanceAdmissionValidator(rollouts, nil)
}

func NewWorkerInstanceAdmissionValidator(
	rollouts []ResidencyPlanRollout,
	memberPKISecrets StageWorkerMemberPKISecretReader,
) (*WorkerInstancePodAdmissionValidator, error) {
	if memberPKISecrets == nil {
		return nil, errors.New("stage worker member PKI Secret reader is required")
	}
	return newWorkerInstanceAdmissionValidator(rollouts, memberPKISecrets)
}

func newWorkerInstanceAdmissionValidator(
	rollouts []ResidencyPlanRollout,
	memberPKISecrets StageWorkerMemberPKISecretReader,
) (*WorkerInstancePodAdmissionValidator, error) {
	if len(rollouts) == 0 {
		return nil, errors.New("ResidencyPlan rollouts are required")
	}
	validator := &WorkerInstancePodAdmissionValidator{
		desiredPods:          make(map[string]corev1.Pod),
		desiredServices:      make(map[string]corev1.Service),
		desiredSecretBundles: make(map[string]WorkerBundleActuation),
		memberPKISecrets:     memberPKISecrets,
	}
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
				if _, exists := validator.desiredPods[key]; exists {
					return nil, errors.New("ResidencyPlan rollouts reuse a WorkerInstance Pod identity")
				}
				validator.desiredPods[key] = pod
			}
			for _, service := range materializeWorkerInstanceMemberServices(bundle) {
				key := runtimeResourceName(service.Namespace, service.Name)
				if _, exists := validator.desiredServices[key]; exists {
					return nil, errors.New("ResidencyPlan rollouts reuse a WorkerInstance member Service identity")
				}
				validator.desiredServices[key] = service
			}
			for _, worker := range bundle.WorkerInstances {
				if len(worker.Members) < 2 {
					continue
				}
				for _, member := range worker.Members {
					key := runtimeResourceName(
						bundle.Namespace, workerInstanceMemberSecretName(worker.ID, member.Key),
					)
					if _, exists := validator.desiredSecretBundles[key]; exists {
						return nil, errors.New("ResidencyPlan rollouts reuse a WorkerInstance member Secret identity")
					}
					validator.desiredSecretBundles[key] = bundle
				}
			}
		}
	}
	return validator, nil
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
	desired, exists := validator.desiredPods[runtimeResourceName(pod.Namespace, pod.Name)]
	if !exists || !workerInstancePodMatches(pod, desired) {
		return ErrProtectedResourceDrift
	}
	return nil
}

func (validator *WorkerInstancePodAdmissionValidator) ValidateProtectedServiceCreate(
	ctx context.Context,
	service corev1.Service,
) error {
	if validator == nil || ctx == nil || service.UID != "" || len(service.OwnerReferences) != 0 {
		return ErrProtectedResourceDrift
	}
	desired, exists := validator.desiredServices[runtimeResourceName(service.Namespace, service.Name)]
	if !exists || !workerInstanceMemberServiceMatches(service, desired) {
		return ErrProtectedResourceDrift
	}
	return nil
}

func (validator *WorkerInstancePodAdmissionValidator) ValidateProtectedSecretCreate(
	ctx context.Context,
	secret corev1.Secret,
) error {
	if validator == nil || validator.memberPKISecrets == nil || ctx == nil || secret.UID != "" ||
		len(secret.OwnerReferences) != 0 || len(secret.StringData) != 0 {
		return ErrProtectedResourceDrift
	}
	bundle, exists := validator.desiredSecretBundles[runtimeResourceName(secret.Namespace, secret.Name)]
	if !exists {
		return ErrProtectedResourceDrift
	}
	source, err := validator.memberPKISecrets.GetStageWorkerMemberPKISecret(ctx, ResourceKey{
		Namespace: bundle.Namespace, Name: bundle.StageWorkerMemberPKISecret,
	})
	if err != nil {
		return ErrProtectedResourceDrift
	}
	desired, err := materializeWorkerInstanceMemberSecrets(bundle, source)
	if err != nil {
		return ErrProtectedResourceDrift
	}
	for _, candidate := range desired {
		if candidate.Name == secret.Name && workerInstanceMemberSecretMatches(secret, candidate) {
			return nil
		}
	}
	return ErrProtectedResourceDrift
}
