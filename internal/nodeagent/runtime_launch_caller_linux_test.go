package nodeagent

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontroller"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type runtimeLaunchPodFixture struct {
	mu    sync.Mutex
	pod   corev1.Pod
	key   fleetcontroller.ResourceKey
	calls int
	hook  func(int) error
	share bool
}

func (fixture *runtimeLaunchPodFixture) GetWorkerInstancePod(_ context.Context, key fleetcontroller.ResourceKey) (corev1.Pod, error) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.calls++
	if key != fixture.key {
		return corev1.Pod{}, errors.New("query did not select the Registry-bound Pod")
	}
	if fixture.hook != nil {
		if err := fixture.hook(fixture.calls); err != nil {
			return corev1.Pod{}, err
		}
	}
	if fixture.share {
		return fixture.pod, nil
	}
	return *fixture.pod.DeepCopy(), nil
}

func TestRuntimePlannedCallerCorrelatesTrustedPod(t *testing.T) {
	for _, scenario := range []string{"matching", "wrong-payload", "wrong-uid", "wrong-node", "nil-plan", "empty-plan", "no-pod-reader",
		"pod-unavailable", "wrong-pod-uid", "wrong-pod-name", "wrong-pod-node", "changed-command", "changed-volume", "terminating-pod",
		"missing-status", "duplicate-status", "waiting", "wrong-id", "wrong-restart", "pod-recreated", "pod-updated", "pod-last-unavailable",
		"aliased-pod", "caller-closed", "canceled", "cancel-final"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := runtimeLaunchFixture(t)
			plan, err := VerifyRuntimeLaunchPlan("cpu-node", fixture.verifier, fixture.binding, fixture.wire)
			if err != nil {
				t.Fatal(err)
			}
			payload, credentials := plan.manifest, RuntimeCallerCredentials{UID: plan.uid, GID: plan.gid}
			if scenario == "wrong-payload" {
				payload = append([]byte(nil), payload...)
				payload[0] = '!'
			}
			if scenario == "wrong-uid" {
				credentials.UID++
			}
			connection, _, _ := runtimeCallerConfiguredConnection(t, "normal", "unixpacket", payload, credentials, true)
			caller, err := ReceiveRuntimeCaller(t.Context(), connection, credentials)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = caller.Close() })
			process, err := caller.Inspect(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			cri, _, observer := runtimeCallerObserverFixture(t, process)
			pod := plan.ExpectedPod()
			pod.UID, pod.ResourceVersion, pod.Spec.NodeName = types.UID(cri.target.PodUID.String()), "1", "cpu-node"
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "model-runtime", ContainerID: "containerd://" + cri.target.ContainerID,
				RestartCount: int32(cri.target.ContainerAttempt), State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
			cri.mu.Lock()
			cri.resolveTarget = true
			cri.target.PodName, cri.target.PodNamespace = pod.Name, pod.Namespace
			cri.sandbox.Status.Metadata.Name, cri.sandbox.Status.Metadata.Namespace = pod.Name, pod.Namespace
			cri.mu.Unlock()
			pods := &runtimeLaunchPodFixture{pod: *pod, key: fleetcontroller.ResourceKey{Namespace: pod.Namespace, Name: pod.Name}}
			var reader RuntimeLaunchPodReader = pods
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch scenario {
			case "wrong-node":
				observer.nodeIdentity = "other-node"
			case "nil-plan":
				plan = nil
			case "empty-plan":
				plan = &RuntimeLaunchPlan{}
			case "no-pod-reader":
				reader = nil
			case "pod-unavailable":
				pods.hook = func(int) error { return errors.New("unavailable") }
			case "wrong-pod-uid":
				pods.pod.UID = types.UID(uuid.NewString())
			case "wrong-pod-name":
				pods.pod.Name = "other-pod"
			case "wrong-pod-node":
				pods.pod.Spec.NodeName = "other-node"
			case "changed-command":
				pods.pod.Spec.Containers[1].Command = []string{"/wrapper"}
			case "changed-volume":
				pods.pod.Spec.Containers[1].VolumeMounts[0].ReadOnly = true
			case "terminating-pod":
				now := metav1.Now()
				pods.pod.DeletionTimestamp = &now
			case "missing-status":
				pods.pod.Status.ContainerStatuses = nil
			case "duplicate-status":
				pods.pod.Status.ContainerStatuses = append(pods.pod.Status.ContainerStatuses, pods.pod.Status.ContainerStatuses[0])
			case "waiting":
				pods.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{}}
			case "wrong-id":
				pods.pod.Status.ContainerStatuses[0].ContainerID = "containerd://short"
			case "wrong-restart":
				pods.pod.Status.ContainerStatuses[0].RestartCount++
			case "pod-recreated", "pod-updated", "pod-last-unavailable", "aliased-pod", "caller-closed", "cancel-final":
				pods.share = scenario == "aliased-pod"
				pods.hook = func(count int) error {
					if count != 2 {
						return nil
					}
					switch scenario {
					case "pod-recreated":
						pods.pod.UID = types.UID(uuid.NewString())
					case "pod-updated":
						pods.pod.ResourceVersion = "2"
					case "pod-last-unavailable":
						return errors.New("lost API connection")
					case "aliased-pod":
						pods.pod.Spec.Containers[1].VolumeMounts[0].ReadOnly = true
					case "caller-closed":
						return caller.Close()
					case "cancel-final":
						cancel()
					}
					return nil
				}
			case "canceled":
				cancel()
			}
			observation, err := observer.ObservePlannedCaller(ctx, plan, reader, caller)
			if scenario == "matching" {
				if err != nil || observation.SchemaVersion != 2 || observation.Caller.Container.Target.PodUID != uuid.MustParse(string(pod.UID)) ||
					observation.Caller.Process.UID != 10001 || observation.RegistryBinding.GetPair().GetRuntimeJournalId() != fixture.binding.Pair.RuntimeJournalId ||
					observation.LaunchManifestDigest != sha256.Sum256(plan.manifest) || observation.PodResourceVersion != "1" ||
					observation.ObservedFrom.IsZero() || observation.ObservedThrough.Before(observation.ObservedFrom) ||
					observation.ObservedThrough.Sub(observation.ObservedFrom) > 10*time.Second || pods.calls != 2 {
					t.Fatalf("planned caller failed correlation: %+v %v", observation, err)
				}
				executable, err := caller.InspectExecutable(t.Context())
				if err != nil || observation.Executable.Digest != executable.Digest || observation.Executable.SizeBytes != executable.SizeBytes ||
					observation.Executable.FileInode != executable.FileInode || observation.Executable.Process.HostPID != process.HostPID {
					t.Fatalf("planned observation omitted the actual executable: %+v %v", observation.Executable, err)
				}
				observation.RegistryBinding.Signature[0] ^= 1
				if _, err := fixture.verifier.Verify(plan.RegistryBinding()); err != nil {
					t.Fatal("observation mutates the retained plan")
				}
			} else if err == nil || observation != (RuntimePlannedCallerObservation{}) {
				t.Fatalf("invalid plan/Pod/caller yielded an observation: %+v %v", observation, err)
			}
		})
	}
}
