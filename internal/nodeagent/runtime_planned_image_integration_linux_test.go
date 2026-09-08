//go:build integration && linux

package nodeagent

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/fleetcontroller"
	"google.golang.org/grpc/metadata"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func verifyRuntimePlannedImageCaller(t *testing.T, fixture *containerdProcessFixture, observer *RuntimeContainerObserver, plan *RuntimeLaunchPlan, pods *runtimeLaunchPodFixture, caller *RuntimeCaller) {
	t.Helper()
	images, err := DialRuntimeImageObserver(t.Context(), RuntimeImageObserverConfig{RuntimeContainerObserverConfig: RuntimeContainerObserverConfig{SocketPath: fixture.socket, NodeIdentity: "cpu-node"}, Namespace: "k8s.io", Snapshotter: "native"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = images.Close() }()
	config := RuntimePlannedImageCallerConfig{Plan: plan, Pods: pods, Caller: caller, Images: images, StateDirectory: filepath.Join(fixture.root, "state"), RuntimePolicy: runtimeTaskPolicyFixture()}
	result, err := observer.ObservePlannedImageCaller(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if result.Image.Digest != result.Planned.Executable.Digest || result.Image.Target.ConfigDigest != result.Planned.Caller.Container.ImageRef || result.Image.Target.ExecutablePath != "/probe" {
		t.Fatal("image-derived entrypoint did not bind to the actual caller")
	}
	criFixture := *fixture
	criFixture.ctx = metadata.NewOutgoingContext(t.Context(), metadata.Pairs("containerd-namespace", "k8s.io"))
	assertRuntimeImageResourcesReleased(t, &criFixture)
	t.Logf("Registry-bound image manifest=%s config=%s entrypoint=/probe sha256=%x matched actual caller and task argv", result.Image.Target.ManifestDigest, result.Image.Target.ConfigDigest, result.Image.Digest)
	t.Run("planned-image-other-daemon", func(t *testing.T) {
		other := startProcessContainerd(t)
		foreign, err := DialRuntimeImageObserver(t.Context(), RuntimeImageObserverConfig{RuntimeContainerObserverConfig: RuntimeContainerObserverConfig{SocketPath: other.socket, NodeIdentity: "cpu-node"}, Namespace: "k8s.io", Snapshotter: "native"})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = foreign.Close() }()
		changed := config
		changed.Images = foreign
		if observed, err := observer.ObservePlannedImageCaller(t.Context(), changed); err == nil || observed != nil {
			t.Fatal("another root daemon supplied the image observation")
		}
		assertRuntimeImageResourcesReleased(t, &criFixture)
	})
	t.Run("planned-image-wrong-argv", func(t *testing.T) {
		path := filepath.Join(config.StateDirectory, "io.containerd.runtime.v2.task", "k8s.io", result.Planned.Caller.Container.Target.ContainerID, "config.json")
		original, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := os.WriteFile(path, original, 0o644); err != nil {
				t.Error(err)
			}
		}()
		configuration, err := result.Task.Configuration()
		if err != nil {
			t.Fatal(err)
		}
		configuration.Process.Args = append(configuration.Process.Args, "unapproved-argument")
		wire, err := json.Marshal(configuration)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, wire, 0o644); err != nil {
			t.Fatal(err)
		}
		if observed, err := observer.ObservePlannedImageCaller(t.Context(), config); !errors.Is(err, ErrRuntimePlannedImage) || observed != nil {
			t.Fatalf("unapproved task argv accepted: %v", err)
		}
		assertRuntimeImageResourcesReleased(t, &criFixture)
	})
	t.Run("planned-image-substituted-executable", func(t *testing.T) {
		client := runtimev1.NewRuntimeServiceClient(fixture.connection)
		target, listener := fixture.createCRICaller(t, client, "substituted-executable", plan)
		if _, err := client.StartContainer(t.Context(), &runtimev1.StartContainerRequest{ContainerId: target.ContainerID}); err != nil {
			t.Fatal(err)
		}
		if err := listener.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
		connection, err := listener.AcceptUnix()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = connection.Close() }()
		other, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: plan.uid, GID: plan.gid})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = other.Close() }()
		pod := plan.ExpectedPod()
		pod.UID, pod.ResourceVersion, pod.Spec.NodeName = types.UID(target.PodUID.String()), "1", "cpu-node"
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: target.ContainerName, RestartCount: int32(target.ContainerAttempt), ContainerID: "containerd://" + target.ContainerID, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
		otherPods := &runtimeLaunchPodFixture{pod: *pod, key: fleetcontroller.ResourceKey{Namespace: pod.Namespace, Name: pod.Name}}
		declared, err := observer.ObservePlannedCaller(t.Context(), plan, otherPods, other)
		if err != nil {
			t.Fatal(err)
		}
		if declared.Caller.Container.ImageRef != result.Image.Target.ConfigDigest || declared.Executable.Digest == result.Image.Digest {
			t.Fatal("substitution did not retain the image identity while replacing executable bytes")
		}
		changed := config
		changed.Pods, changed.Caller = otherPods, other
		if observed, err := observer.ObservePlannedImageCaller(t.Context(), changed); !errors.Is(err, ErrRuntimePlannedImage) || observed != nil {
			t.Fatalf("bind-mounted executable substituted for approved image entrypoint: %v", err)
		}
		assertRuntimeImageResourcesReleased(t, &criFixture)
		t.Log("same declared CRI image and signed manifest, different live executable bytes: rejected")
	})
}
