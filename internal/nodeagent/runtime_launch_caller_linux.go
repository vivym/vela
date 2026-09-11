package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontroller"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	corev1 "k8s.io/api/core/v1"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// RuntimeLaunchPodReader must use the Node's authenticated Kubernetes client.
// A Pod supplied by a Runtime is not trusted inventory.
type RuntimeLaunchPodReader interface {
	GetWorkerInstancePod(context.Context, fleetcontroller.ResourceKey) (corev1.Pod, error)
}

// CallerCredentials exposes the UID/GID authenticated by the verified Pod
// plan. Node uses this value as the expected peer identity before reservation.
func (plan *RuntimeLaunchPlan) CallerCredentials() (RuntimeCallerCredentials, error) {
	if plan == nil || plan.uid == 0 || plan.gid == 0 || plan.uid == ^uint32(0) || plan.gid == ^uint32(0) {
		return RuntimeCallerCredentials{}, ErrRuntimeLaunchPlan
	}
	return RuntimeCallerCredentials{UID: plan.uid, GID: plan.gid}, nil
}

// RuntimePlannedCallerObservation links a historical approved configuration to
// API-observed Pod content and an authenticated live caller's declared manifest.
// Schema 2 includes the current executable file observation, without approving
// its bytes or associating them with the sender's pre-exec declaration.
// It does not attest effective OCI configuration or grant backend startup.
type RuntimePlannedCallerObservation struct {
	SchemaVersion        int                               `json:"schema_version"`
	RegistryBinding      *velav1.WorkerBootstrapBinding    `json:"registry_binding"`
	LaunchManifestDigest [sha256.Size]byte                 `json:"launch_manifest_digest"`
	PodResourceVersion   string                            `json:"pod_resource_version"`
	Caller               RuntimeContainerCallerObservation `json:"caller"`
	Executable           RuntimeExecutableObservation      `json:"executable"`
	ObservedFrom         time.Time                         `json:"observed_from"`
	ObservedThrough      time.Time                         `json:"observed_through"`
}

// ObservePlannedCaller resolves container identity from the expected Pod and
// CRI. The authenticated payload must be the exact canonical launch manifest;
// it is a declaration, not proof of the file or configuration actually loaded.
func (observer *RuntimeContainerObserver) ObservePlannedCaller(ctx context.Context, plan *RuntimeLaunchPlan, pods RuntimeLaunchPodReader, caller *RuntimeCaller) (RuntimePlannedCallerObservation, error) {
	var declaration []byte
	if plan != nil {
		declaration = plan.manifest
	}
	return observer.observePlannedCaller(ctx, plan, pods, caller, declaration)
}

// A startup request carries the matching manifest digest rather than its bytes.
// The caller must verify that domain request before selecting this declaration.
func (observer *RuntimeContainerObserver) observePlannedCaller(ctx context.Context, plan *RuntimeLaunchPlan, pods RuntimeLaunchPodReader, caller *RuntimeCaller, declaration []byte) (RuntimePlannedCallerObservation, error) {
	if err := contextError(ctx); err != nil {
		return RuntimePlannedCallerObservation{}, err
	}
	if observer == nil || observer.reader == nil || observer.clock == nil || observer.check == nil || pods == nil ||
		plan == nil || plan.binding == nil || caller == nil || observer.nodeIdentity != plan.binding.Claim.NodeIdentity {
		return RuntimePlannedCallerObservation{}, ErrRuntimeLaunchPlan
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	from := observer.clock().UTC()
	if len(declaration) == 0 || !bytes.Equal(caller.Payload(), declaration) {
		return RuntimePlannedCallerObservation{}, ErrRuntimeLaunchPlan
	}
	key := fleetcontroller.ResourceKey{Namespace: plan.pod.Namespace, Name: plan.pod.Name}
	first, err := pods.GetWorkerInstancePod(ctx, key)
	if err != nil {
		return RuntimePlannedCallerObservation{}, err
	}
	first = *first.DeepCopy()
	target, err := plan.podTarget(first)
	if err != nil {
		return RuntimePlannedCallerObservation{}, err
	}
	if err := observer.check(); err != nil {
		return RuntimePlannedCallerObservation{}, err
	}
	listed, err := observer.reader.ListContainers(ctx, &runtimev1.ListContainersRequest{Filter: &runtimev1.ContainerFilter{Id: target.ContainerID}})
	if err != nil || len(listed.GetContainers()) != 1 || listed.Containers[0].GetId() != target.ContainerID {
		return RuntimePlannedCallerObservation{}, errors.Join(ErrRuntimeLaunchPlan, err)
	}
	target.SandboxID = listed.Containers[0].GetPodSandboxId()
	observation, err := observer.ObserveCaller(ctx, target, caller)
	if err != nil || observation.Process.UID != plan.uid || observation.Process.GID != plan.gid {
		return RuntimePlannedCallerObservation{}, errors.Join(ErrRuntimeLaunchPlan, err)
	}
	last, err := pods.GetWorkerInstancePod(ctx, key)
	if err != nil || !reflect.DeepEqual(first, last) {
		return RuntimePlannedCallerObservation{}, errors.Join(ErrRuntimeLaunchPlan, err)
	}
	executable, err := caller.InspectExecutable(ctx)
	if err != nil {
		return RuntimePlannedCallerObservation{}, err
	}
	process := executable.Process
	previous := observation.Process
	previous.ObservedAt = process.ObservedAt
	if previous != process {
		return RuntimePlannedCallerObservation{}, ErrRuntimeLaunchPlan
	}
	through := observer.clock().UTC()
	if from.IsZero() || through.Before(from) || through.Sub(from) > 10*time.Second {
		return RuntimePlannedCallerObservation{}, ErrRuntimeLaunchPlan
	}
	if err := errors.Join(observer.check(), context.Cause(ctx)); err != nil {
		return RuntimePlannedCallerObservation{}, err
	}
	return RuntimePlannedCallerObservation{SchemaVersion: 2, RegistryBinding: plan.RegistryBinding(),
		LaunchManifestDigest: sha256.Sum256(plan.manifest), PodResourceVersion: first.ResourceVersion, Caller: observation,
		Executable: executable, ObservedFrom: from, ObservedThrough: through}, nil
}

func (plan *RuntimeLaunchPlan) podTarget(pod corev1.Pod) (RuntimeContainerTarget, error) {
	uid, err := uuid.Parse(string(pod.UID))
	if err != nil || uid == uuid.Nil || uid.String() != string(pod.UID) || pod.DeletionTimestamp != nil ||
		!validText(pod.ResourceVersion, 253) || pod.Spec.NodeName != plan.binding.Claim.NodeIdentity ||
		!fleetcontroller.WorkerInstancePodMatches(pod, plan.pod) {
		return RuntimeContainerTarget{}, ErrRuntimeLaunchPlan
	}
	var status *corev1.ContainerStatus
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == "model-runtime" {
			if status != nil {
				return RuntimeContainerTarget{}, ErrRuntimeLaunchPlan
			}
			status = &pod.Status.ContainerStatuses[i]
		}
	}
	if status == nil || status.RestartCount < 0 || status.State.Running == nil || status.State.Terminated != nil || status.State.Waiting != nil {
		return RuntimeContainerTarget{}, ErrRuntimeLaunchPlan
	}
	id, ok := strings.CutPrefix(status.ContainerID, "containerd://")
	if !ok || !runtimeContainerIDPattern.MatchString(id) {
		return RuntimeContainerTarget{}, ErrRuntimeLaunchPlan
	}
	return RuntimeContainerTarget{ContainerID: id, PodUID: uid, PodNamespace: pod.Namespace, PodName: pod.Name,
		ContainerName: status.Name, ContainerAttempt: uint32(status.RestartCount)}, nil
}
