//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	tasksapi "github.com/containerd/containerd/api/services/tasks/v1"
	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontract"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/runtimelaunch"
	"github.com/vivym/vela/internal/securefile"
	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func launchKubernetesWorkload(ctx context.Context, startupSocket string, desired *corev1.Pod) (_ *productionWorkload, retErr error) {
	member, err := uuid.Parse(desired.Labels[fleetcontract.WorkerMemberIDLabel])
	if err != nil || member == uuid.Nil || member.String() != desired.Labels[fleetcontract.WorkerMemberIDLabel] {
		return nil, errors.New("kubernetes startup requires a canonical signed member identity")
	}
	root := runtimelaunch.MemberRoot(member.String())
	if startupSocket != root+"/startup.sock" || desired.UID != "" || desired.Spec.RestartPolicy != corev1.RestartPolicyNever ||
		len(desired.Spec.SchedulingGates) != 1 || desired.Spec.SchedulingGates[0].Name != runtimelaunch.Gate {
		return nil, errors.New("kubernetes startup requires a gated signed template and exact member socket")
	}
	runtimeContainer, workerContainer, err := requiredContainers(desired)
	if err != nil {
		return nil, err
	}
	uid, gid, err := productionCredentials(*desired, *runtimeContainer, *workerContainer)
	if err != nil {
		return nil, err
	}
	if _, err := securefile.ResolveTrustedDirectory(root); err != nil {
		return nil, err
	}
	kube, err := loadKubernetesClient()
	if err != nil {
		return nil, err
	}
	pods := kube.CoreV1().Pods(desired.Namespace)
	live, err := pods.Get(ctx, desired.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read Fleet-created gated Pod: %w", err)
	}
	if live.UID == "" || live.ResourceVersion == "" || live.DeletionTimestamp != nil || live.Spec.NodeName != "" ||
		len(live.Spec.SchedulingGates) != 1 || live.Spec.SchedulingGates[0].Name != runtimelaunch.Gate ||
		len(live.Status.ContainerStatuses) != 0 || !fleetcontroller.WorkerInstancePodMatches(*live, *desired) {
		return nil, errors.New("refuse to adopt an ungated, started or drifted Pod")
	}
	connection, err := dialCRI(ctx, os.Getenv("VELA_RUNTIME_LAUNCHER_CRI_SOCKET"))
	if err != nil {
		return nil, err
	}
	w := &productionWorkload{connection: connection, runtime: runtimev1.NewRuntimeServiceClient(connection),
		tasks: tasksapi.NewTasksClient(connection), kube: kube, uid: uid, gid: gid, startupPath: startupSocket}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, w.Close())
		}
	}()
	w.observerEnd, w.observerChild, err = newProductionObserverSocketpair()
	if err != nil {
		return nil, err
	}
	launchRoot := filepath.Join(root, "launch")
	if err := os.Mkdir(launchRoot, 0o750); err != nil {
		return nil, err
	}
	if err := os.Chown(launchRoot, 0, int(gid)); err != nil {
		return nil, err
	}
	if err := os.Chmod(launchRoot, 0o750); err != nil {
		return nil, err
	}
	w.volumeRoot = launchRoot
	for _, role := range []string{"runtime", "worker"} {
		offer, err := newPIDFDOffer(launchRoot, role, uid, gid)
		if err != nil {
			return nil, err
		}
		w.offers = append(w.offers, offer)
	}
	// Retain the exact UID before PATCH, including an uncertain response. The
	// launcher never deletes a protected Pod or bypasses Fleet's retirement.
	w.kubernetesPod = live.DeepCopy()
	patch, _ := json.Marshal([]map[string]any{
		{"op": "test", "path": "/metadata/uid", "value": live.UID},
		{"op": "test", "path": "/metadata/resourceVersion", "value": live.ResourceVersion},
		{"op": "test", "path": "/spec/schedulingGates", "value": live.Spec.SchedulingGates},
		{"op": "remove", "path": "/spec/schedulingGates"},
	})
	if _, err := pods.Patch(ctx, live.Name, types.JSONPatchType, patch, metav1.PatchOptions{}); err != nil {
		return nil, fmt.Errorf("release exact Pod startup gate: %w", err)
	}
	for {
		current, err := pods.Get(ctx, live.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if current.UID != live.UID || current.DeletionTimestamp != nil || !fleetcontroller.WorkerInstancePodMatches(*current, *desired) {
			return nil, errors.New("kubernetes startup Pod changed after gate release")
		}
		if current.Status.Phase == corev1.PodFailed || current.Status.Phase == corev1.PodSucceeded {
			return nil, errors.New("kubernetes startup Pod terminated before handoff")
		}
		ready, err := w.kubernetesTargets(ctx, current)
		if err != nil {
			return nil, err
		}
		if ready {
			if err := w.validateKubernetesClaims(ctx, current); err != nil {
				return nil, err
			}
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	w.runtimeFD, err = w.offers[0].accept(ctx, w.tasks, w.target.ContainerID)
	if err != nil {
		return nil, err
	}
	w.workerFD, err = w.offers[1].accept(ctx, w.tasks, w.worker.ContainerID)
	if err != nil {
		return nil, err
	}
	w.observer, w.observerFD, w.observerEnd, err = startProductionObserver(uid, gid, w.runtimeFD, w.observerEnd, w.observerChild)
	w.observerChild = nil
	if err != nil {
		return nil, err
	}
	// Worker starts only after both original handles have been retained. Runtime
	// remains stopped until Node acknowledges the observer custody protocol.
	if err := unix.PidfdSendSignal(int(w.workerFD.Fd()), unix.SIGCONT, nil, 0); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *productionWorkload) kubernetesTargets(ctx context.Context, pod *corev1.Pod) (bool, error) {
	if pod.Spec.NodeName == "" {
		return false, nil
	}
	if pod.Spec.NodeName != pod.Spec.NodeSelector[corev1.LabelHostname] {
		return false, errors.New("pod scheduled outside the signed node")
	}
	var targets []nodeagent.RuntimeContainerTarget
	for _, name := range []string{"model-runtime", "stage-worker-agent"} {
		var found *corev1.ContainerStatus
		for i := range pod.Status.ContainerStatuses {
			if pod.Status.ContainerStatuses[i].Name == name {
				if found != nil {
					return false, errors.New("duplicate container status")
				}
				found = &pod.Status.ContainerStatuses[i]
			}
		}
		if found == nil || found.State.Running == nil {
			return false, nil
		}
		if found.RestartCount != 0 {
			return false, errors.New("refuse restarted Kubernetes process in a once-only startup")
		}
		id, ok := strings.CutPrefix(found.ContainerID, "containerd://")
		if !ok {
			return false, errors.New("kubernetes container is not a containerd task")
		}
		listed, err := w.runtime.ListContainers(ctx, &runtimev1.ListContainersRequest{Filter: &runtimev1.ContainerFilter{Id: id}})
		if err != nil || len(listed.GetContainers()) != 1 {
			return false, errors.Join(errors.New("kubernetes container CRI lookup mismatch"), err)
		}
		item := listed.Containers[0]
		if item.Id != id || item.GetMetadata().GetName() != name || item.GetMetadata().GetAttempt() != 0 || item.State != runtimev1.ContainerState_CONTAINER_RUNNING || item.Labels["io.kubernetes.pod.uid"] != string(pod.UID) {
			return false, errors.New("kubernetes and CRI container identities differ")
		}
		target := nodeagent.RuntimeContainerTarget{ContainerID: id, SandboxID: item.PodSandboxId,
			PodUID: uuidMust(string(pod.UID)), PodNamespace: pod.Namespace, PodName: pod.Name, ContainerName: name}
		if err := target.Validate(); err != nil {
			return false, err
		}
		targets = append(targets, target)
	}
	if targets[0].SandboxID != targets[1].SandboxID {
		return false, errors.New("runtime and Worker belong to different sandboxes")
	}
	w.target, w.worker = targets[0], targets[1]
	return true, nil
}

func (w *productionWorkload) stopKubernetesWorkload() error {
	var result error
	for _, fd := range []*os.File{w.runtimeFD, w.workerFD} {
		if fd == nil {
			// A wrapper not yet handed off has a bounded self-exit timeout. Do
			// not claim its cleanup or recover a handle from a numeric PID.
			result = errors.Join(result, errors.New("kubernetes handoff incomplete; wrapper exit requires reconciliation"))
			continue
		}
		if err := unix.PidfdSendSignal(int(fd.Fd()), unix.SIGKILL, nil, 0); err != nil && !errors.Is(err, unix.ESRCH) {
			result = errors.Join(result, err)
		}
	}
	// RestartPolicyNever prevents a replacement from inheriting this grant.
	// kubelet owns stopped container GC; Fleet still authorizes Pod retirement.
	return result
}
