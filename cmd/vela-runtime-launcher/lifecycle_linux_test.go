//go:build linux

package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/nodeagent"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type failedStartCRI struct {
	runtimev1.RuntimeServiceClient
	config  *runtimev1.ContainerConfig
	removed []string
	stopErr error
}

type initLifecycleCRI struct {
	runtimev1.RuntimeServiceClient
	config    *runtimev1.ContainerConfig
	status    runtimev1.ContainerState
	exitCode  int32
	startErr  error
	statusErr error
	removed   []string
}

type imagePreparationCRI struct {
	runtimev1.ImageServiceClient
	status *runtimev1.Image
	pulls  []string
}

func (cri *imagePreparationCRI) ImageStatus(context.Context, *runtimev1.ImageStatusRequest, ...grpc.CallOption) (*runtimev1.ImageStatusResponse, error) {
	if cri.status == nil {
		return &runtimev1.ImageStatusResponse{}, nil
	}
	return &runtimev1.ImageStatusResponse{Image: cri.status}, nil
}

func (cri *imagePreparationCRI) PullImage(_ context.Context, request *runtimev1.PullImageRequest, _ ...grpc.CallOption) (*runtimev1.PullImageResponse, error) {
	cri.pulls = append(cri.pulls, request.GetImage().GetImage())
	return &runtimev1.PullImageResponse{ImageRef: request.GetImage().GetImage()}, nil
}

func (cri *initLifecycleCRI) CreateContainer(_ context.Context, request *runtimev1.CreateContainerRequest, _ ...grpc.CallOption) (*runtimev1.CreateContainerResponse, error) {
	cri.config = request.Config
	return &runtimev1.CreateContainerResponse{ContainerId: strings.Repeat("i", 64)}, nil
}

func (cri *initLifecycleCRI) StartContainer(context.Context, *runtimev1.StartContainerRequest, ...grpc.CallOption) (*runtimev1.StartContainerResponse, error) {
	if cri.startErr != nil {
		return nil, cri.startErr
	}
	return &runtimev1.StartContainerResponse{}, nil
}

func (cri *initLifecycleCRI) ContainerStatus(_ context.Context, _ *runtimev1.ContainerStatusRequest, _ ...grpc.CallOption) (*runtimev1.ContainerStatusResponse, error) {
	if cri.statusErr != nil {
		return nil, cri.statusErr
	}
	return &runtimev1.ContainerStatusResponse{Status: &runtimev1.ContainerStatus{State: cri.status, ExitCode: cri.exitCode}}, nil
}

func (cri *initLifecycleCRI) StopContainer(context.Context, *runtimev1.StopContainerRequest, ...grpc.CallOption) (*runtimev1.StopContainerResponse, error) {
	return &runtimev1.StopContainerResponse{}, nil
}

func (cri *initLifecycleCRI) RemoveContainer(_ context.Context, request *runtimev1.RemoveContainerRequest, _ ...grpc.CallOption) (*runtimev1.RemoveContainerResponse, error) {
	cri.removed = append(cri.removed, request.ContainerId)
	return &runtimev1.RemoveContainerResponse{}, nil
}

func (cri *initLifecycleCRI) StopPodSandbox(context.Context, *runtimev1.StopPodSandboxRequest, ...grpc.CallOption) (*runtimev1.StopPodSandboxResponse, error) {
	return &runtimev1.StopPodSandboxResponse{}, nil
}

func (cri *initLifecycleCRI) RemovePodSandbox(context.Context, *runtimev1.RemovePodSandboxRequest, ...grpc.CallOption) (*runtimev1.RemovePodSandboxResponse, error) {
	return &runtimev1.RemovePodSandboxResponse{}, nil
}

func (cri *failedStartCRI) CreateContainer(_ context.Context, request *runtimev1.CreateContainerRequest, _ ...grpc.CallOption) (*runtimev1.CreateContainerResponse, error) {
	cri.config = request.Config
	return &runtimev1.CreateContainerResponse{ContainerId: strings.Repeat("c", 64)}, nil
}

func (cri *failedStartCRI) StartContainer(context.Context, *runtimev1.StartContainerRequest, ...grpc.CallOption) (*runtimev1.StartContainerResponse, error) {
	return nil, errors.New("injected start failure")
}

func (cri *failedStartCRI) StopContainer(context.Context, *runtimev1.StopContainerRequest, ...grpc.CallOption) (*runtimev1.StopContainerResponse, error) {
	return &runtimev1.StopContainerResponse{}, cri.stopErr
}

func (cri *failedStartCRI) RemoveContainer(_ context.Context, request *runtimev1.RemoveContainerRequest, _ ...grpc.CallOption) (*runtimev1.RemoveContainerResponse, error) {
	cri.removed = append(cri.removed, request.ContainerId)
	return &runtimev1.RemoveContainerResponse{}, nil
}

func (cri *failedStartCRI) StopPodSandbox(context.Context, *runtimev1.StopPodSandboxRequest, ...grpc.CallOption) (*runtimev1.StopPodSandboxResponse, error) {
	return &runtimev1.StopPodSandboxResponse{}, nil
}

func (cri *failedStartCRI) RemovePodSandbox(context.Context, *runtimev1.RemovePodSandboxRequest, ...grpc.CallOption) (*runtimev1.RemovePodSandboxResponse, error) {
	return &runtimev1.RemovePodSandboxResponse{}, nil
}

func TestProductionStartFailureCleansCreatedContainers(t *testing.T) {
	// Worker path exercises the CRI create/start cleanup without requiring a
	// trusted Runtime bootstrap directory owned by the test user.
	for _, runtime := range []bool{false} {
		name := "stage-worker-agent"
		if runtime {
			name = "model-runtime"
		}
		t.Run(name, func(t *testing.T) {
			cri := &failedStartCRI{}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "startup", Namespace: "default"}}
			startupPath := productionTestStartupPath(t)
			workload := &productionWorkload{runtime: cri, target: nodeagent.RuntimeContainerTarget{SandboxID: strings.Repeat("b", 64), PodUID: uuid.New()}, startupPath: startupPath}
			offer := &pidfdOffer{directory: t.TempDir(), containerPath: "/vela-runtime/offer/pidfd.sock"}
			args := []string{"60"}
			err := workload.createAndStart(t.Context(), &runtimev1.PodSandboxConfig{}, &runtimev1.NamespaceOption{}, pod, &corev1.Container{Name: name, Image: "registry/runtime@sha256:" + strings.Repeat("a", 64), Command: []string{"/bin/sleep"}, Args: args}, 65532, 65532, offer, runtime)
			if err == nil || !strings.Contains(err.Error(), "injected start failure") {
				t.Fatalf("start result: %v", err)
			}
			if err := workload.Close(); err != nil {
				t.Fatal(err)
			}
			if len(cri.removed) != 1 || cri.removed[0] != strings.Repeat("c", 64) {
				t.Fatalf("created container leaked after failed start: %v", cri.removed)
			}
		})
	}
}

func TestProductionInitContainerSuccessAndConfig(t *testing.T) {
	cri := &initLifecycleCRI{status: runtimev1.ContainerState_CONTAINER_EXITED}
	workload := &productionWorkload{runtime: cri, volumeRoot: t.TempDir(), startupPath: "/run/vela/startup.sock", uid: 10001, gid: 10001,
		target: nodeagent.RuntimeContainerTarget{SandboxID: strings.Repeat("b", 64), PodUID: uuid.New()}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "startup", Namespace: "default"}}
	container := &corev1.Container{Name: "model-runtime-private-materialization", Image: "registry/init@sha256:" + strings.Repeat("a", 64), Command: []string{"/bin/true"}}
	if err := workload.createAndWaitInit(t.Context(), &runtimev1.PodSandboxConfig{}, &runtimev1.NamespaceOption{}, pod, container, 10001, 10001); err != nil {
		t.Fatal(err)
	}
	if cri.config == nil || cri.config.Metadata.GetName() != container.Name || len(cri.config.Command) != 1 || cri.config.Command[0] != "/bin/true" {
		t.Fatalf("unexpected init config: %+v", cri.config)
	}
	if len(workload.initIDs) != 1 {
		t.Fatalf("init IDs = %v", workload.initIDs)
	}
}

func TestProductionInitContainerFailureModes(t *testing.T) {
	cases := []struct {
		name string
		cri  *initLifecycleCRI
		want string
	}{
		{name: "nonzero exit", cri: &initLifecycleCRI{status: runtimev1.ContainerState_CONTAINER_EXITED, exitCode: 7}, want: "exited with code 7"},
		{name: "start failure", cri: &initLifecycleCRI{status: runtimev1.ContainerState_CONTAINER_RUNNING, startErr: errors.New("injected init start failure")}, want: "injected init start failure"},
		{name: "status failure", cri: &initLifecycleCRI{status: runtimev1.ContainerState_CONTAINER_RUNNING, statusErr: errors.New("injected init status failure")}, want: "injected init status failure"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			workload := &productionWorkload{runtime: tc.cri, volumeRoot: t.TempDir(), startupPath: "/run/vela/startup.sock", uid: 10001, gid: 10001,
				target: nodeagent.RuntimeContainerTarget{SandboxID: strings.Repeat("b", 64), PodUID: uuid.New()}}
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "startup", Namespace: "default"}}
			container := &corev1.Container{Name: "model-runtime-private-materialization", Image: "registry/init@sha256:" + strings.Repeat("a", 64), Command: []string{"/bin/true"}}
			err := workload.createAndWaitInit(t.Context(), &runtimev1.PodSandboxConfig{}, &runtimev1.NamespaceOption{}, pod, container, 10001, 10001)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			if closeErr := workload.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			if len(tc.cri.removed) != 1 || tc.cri.removed[0] != strings.Repeat("i", 64) {
				t.Fatalf("init container was not cleaned up: %v", tc.cri.removed)
			}
		})
	}
}

func TestProductionInitContainerContextTimeout(t *testing.T) {
	cri := &initLifecycleCRI{status: runtimev1.ContainerState_CONTAINER_RUNNING}
	workload := &productionWorkload{runtime: cri, volumeRoot: t.TempDir(), startupPath: "/run/vela/startup.sock", uid: 10001, gid: 10001,
		target: nodeagent.RuntimeContainerTarget{SandboxID: strings.Repeat("b", 64), PodUID: uuid.New()}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "startup", Namespace: "default"}}
	container := &corev1.Container{Name: "model-runtime-private-materialization", Image: "registry/init@sha256:" + strings.Repeat("a", 64), Command: []string{"/bin/true"}}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	err := workload.createAndWaitInit(ctx, &runtimev1.PodSandboxConfig{}, &runtimev1.NamespaceOption{}, pod, container, 10001, 10001)
	if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("timeout error = %v", err)
	}
}

func TestProductionInitContainerCleanupRemovesAllInitIDs(t *testing.T) {
	cri := &initLifecycleCRI{}
	workload := &productionWorkload{runtime: cri, initIDs: []string{"first", "second"}}
	if err := workload.Close(); err != nil {
		t.Fatal(err)
	}
	if len(cri.removed) != 2 || cri.removed[0] != "second" || cri.removed[1] != "first" {
		t.Fatalf("cleanup order = %v", cri.removed)
	}
}

func TestProductionPrepareImagesPinsAndVerifiesDigest(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	workload := &productionWorkload{image: &imagePreparationCRI{status: &runtimev1.Image{Id: digest}}}
	if err := workload.prepareImages(t.Context(), "registry.example/init@"+digest); err != nil {
		t.Fatal(err)
	}
	if err := workload.prepareImages(t.Context(), "registry.example/init:latest"); err == nil {
		t.Fatal("unpinned image was accepted")
	}
	workload.image = &imagePreparationCRI{}
	if err := workload.prepareImages(t.Context(), "registry.example/init@"+digest); err == nil {
		t.Fatal("image without a resolved digest was accepted")
	}
	workload.image = &imagePreparationCRI{status: &runtimev1.Image{Id: "sha256:" + strings.Repeat("b", 64)}}
	if err := workload.prepareImages(t.Context(), "registry.example/init@"+digest); err == nil {
		t.Fatal("digest-mismatched image was accepted")
	}
}

func TestProductionContainerDispatchesOfferMode(t *testing.T) {
	cri := &failedStartCRI{}
	startupPath := productionTestStartupPath(t)
	workload := &productionWorkload{runtime: cri, target: nodeagent.RuntimeContainerTarget{SandboxID: strings.Repeat("b", 64), PodUID: uuid.New()}, startupPath: startupPath}
	offer := &pidfdOffer{directory: t.TempDir(), containerPath: "/vela-runtime/offer/pidfd.sock"}
	_ = workload.createAndStart(t.Context(), &runtimev1.PodSandboxConfig{}, &runtimev1.NamespaceOption{}, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "startup", Namespace: "default"}}, &corev1.Container{Name: "stage-worker-agent", Command: []string{"/bin/sleep"}, Args: []string{"60"}}, 65532, 65532, offer, false)
	if cri.config == nil || len(cri.config.Args) < 1 || cri.config.Args[0] != "--vela-runtime-pidfd-offer" {
		t.Fatalf("container invokes launcher control path instead of pidfd wrapper: %+v", cri.config)
	}
}

func TestProductionStartupTimeoutConfiguration(t *testing.T) {
	for _, value := range []string{"-1s", "0s", "16m", "invalid"} {
		t.Setenv("VELA_RUNTIME_LAUNCHER_STARTUP_TIMEOUT", value)
		if _, err := runtimeLauncherStartupTimeout(); err == nil {
			t.Fatalf("invalid timeout %q accepted", value)
		}
	}
	t.Setenv("VELA_RUNTIME_LAUNCHER_STARTUP_TIMEOUT", "5s")
	if timeout, err := runtimeLauncherStartupTimeout(); err != nil || timeout != 5*time.Second {
		t.Fatalf("timeout=%v err=%v", timeout, err)
	}
	t.Setenv("VELA_RUNTIME_LAUNCHER_STARTUP_TIMEOUT", "")
	if timeout, err := runtimeLauncherStartupTimeout(); err != nil || timeout != defaultStartupTimeout {
		t.Fatalf("timeout=%v err=%v", timeout, err)
	}
}

func TestProductionCleanupSkipsReplacedOfferVolume(t *testing.T) {
	root := t.TempDir()
	hostPath := filepath.Join(root, "offer.sock")
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: hostPath, Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := os.Lstat(hostPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(hostPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(hostPath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	workload := &productionWorkload{volumeRoot: root, offers: []*pidfdOffer{{hostPath: hostPath, identity: identity, listener: listener}}}
	err = workload.Close()
	if !errors.Is(err, errPidfdOfferSocketReplaced) {
		t.Fatalf("replacement error = %v", err)
	}
	if _, statErr := os.Stat(root); statErr != nil {
		t.Fatalf("volume root removed after replacement: %v", statErr)
	}
}

func productionTestStartupPath(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "runtime-bootstrap"), 0o750); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(root, "startup.sock")
}

func TestProductionCleanupReportsStopFailure(t *testing.T) {
	stopErr := errors.New("injected stop failure")
	cri := &failedStartCRI{stopErr: stopErr}
	workload := &productionWorkload{runtime: cri, target: nodeagent.RuntimeContainerTarget{ContainerID: strings.Repeat("c", 64), SandboxID: strings.Repeat("b", 64), PodUID: uuid.New()}}
	if err := workload.Close(); !errors.Is(err, stopErr) {
		t.Fatalf("cleanup error = %v, want stop failure", err)
	}
}
