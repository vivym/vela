//go:build integration && linux

package nodeagent

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontroller"
	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const (
	callerCRISandboxImage = "docker.io/vela/caller-sandbox:cpu-fixture"
	callerCRIRuntimeImage = "docker.io/vela/caller-runtime:cpu-fixture"
)

func TestRuntimeCallerContainerCRI(t *testing.T) {
	if os.Getenv("VELA_TEST_CONTAINERD_SANDBOX") != "1" {
		t.Skip("requires the explicitly enabled disposable containerd CPU sandbox")
	}
	prepareCRICgroupDelegation(t)
	fixture := startConfiguredProcessContainerd(t, `version = 3
[plugins."io.containerd.cri.v1.images"]
  snapshotter = "native"
  [plugins."io.containerd.cri.v1.images".pinned_images]
    sandbox = "docker.io/vela/caller-sandbox:cpu-fixture"
[plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runc]
  runtime_type = "io.containerd.runc.v2"
`)
	fixture.importCRIImages(t)
	client := runtimev1.NewRuntimeServiceClient(fixture.connection)
	observer, err := DialRuntimeContainerObserver(t.Context(), RuntimeContainerObserverConfig{SocketPath: fixture.socket, NodeIdentity: "cpu-node"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observer.Close() })
	for _, mode := range []string{"owner", "wrapper", "shared-pid", "planned-owner"} {
		t.Run(mode, func(t *testing.T) {
			var plan *RuntimeLaunchPlan
			credentials := RuntimeCallerCredentials{UID: 65532, GID: 65532}
			expectedPayload := []byte("containerd-cpu-caller")
			if mode == "planned-owner" {
				approved := runtimeLaunchFixture(t)
				var err error
				plan, err = VerifyRuntimeLaunchPlan("cpu-node", approved.verifier, approved.binding, approved.wire)
				if err != nil {
					t.Fatal(err)
				}
				credentials, expectedPayload = RuntimeCallerCredentials{UID: plan.uid, GID: plan.gid}, plan.manifest
			}
			target, listener := fixture.createCRICaller(t, client, mode, plan)
			created, err := observer.Inspect(t.Context(), target)
			if err != nil || created.ContainerState != "CONTAINER_CREATED" {
				t.Fatalf("observe actual created CRI container: %+v %v", created, err)
			}
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
			t.Cleanup(func() { _ = connection.Close() })
			caller, err := ReceiveRuntimeCaller(t.Context(), connection, credentials)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = caller.Close() })
			observation, err := observer.ObserveCaller(t.Context(), target, caller)
			if mode != "owner" && mode != "planned-owner" {
				if !errors.Is(err, ErrRuntimeContainerCaller) || observation != (RuntimeContainerCallerObservation{}) {
					t.Fatalf("unsupported lifetime owner was accepted: %+v %v", observation, err)
				}
				t.Logf("actual CRI %s rejected as namespace owner", mode)
				return
			}
			if err != nil || observation.SchemaVersion != 1 || observation.Container.Target != target ||
				observation.Process.NamespacePID != 1 || observation.Process.NamespaceDepth != 2 ||
				observation.Process.BootID != observation.Container.BootID || !bytes.Equal(caller.Payload(), expectedPayload) {
				t.Fatalf("correlate actual CRI/native task/caller: %+v %v", observation, err)
			}
			t.Logf("actual CRI container=%s sandbox=%s Pod=%s host_pid=%d namespace_pid=%d image_ref=%s",
				target.ContainerID, target.SandboxID, target.PodUID, observation.Process.HostPID,
				observation.Process.NamespacePID, observation.Container.ImageRef)
			verifyRuntimeTaskLaunchBundle(t, fixture, observer, target, caller)
			var pods *runtimeLaunchPodFixture
			if plan != nil {
				// Pod API and Registry are fixtures; CRI, native task and caller are real.
				pod := plan.ExpectedPod()
				pod.UID, pod.ResourceVersion, pod.Spec.NodeName = types.UID(target.PodUID.String()), "1", "cpu-node"
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: target.ContainerName, RestartCount: int32(target.ContainerAttempt),
					ContainerID: "containerd://" + target.ContainerID, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
				pods = &runtimeLaunchPodFixture{pod: *pod, key: fleetcontroller.ResourceKey{Namespace: pod.Namespace, Name: pod.Name}}
				planned, err := observer.ObservePlannedCaller(t.Context(), plan, pods, caller)
				digest, size := runtimeExecutableDigest(t, fixture.binary)
				if err != nil || planned.SchemaVersion != 2 || planned.Caller.Container.Target != target || planned.Caller.Process.UID != 10001 ||
					planned.Executable.Digest != digest || planned.Executable.SizeBytes != size || planned.Executable.Process.HostPID != observation.Process.HostPID {
					t.Fatalf("correlate planned caller through actual CRI: %+v %v", planned, err)
				}
				t.Log("Registry/Pod fixtures correlated with actual non-root CRI namespace owner; effective configuration is not attested")
			}
			wrongPod := target
			wrongPod.PodUID = uuid.New()
			if result, err := observer.ObserveCaller(t.Context(), wrongPod, caller); err == nil || result != (RuntimeContainerCallerObservation{}) {
				t.Fatal("caller overrode the actual CRI Pod identity")
			}
			owner, err := observer.RetainNamespaceOwner(t.Context(), target, caller)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.Close() })
			assertNamespaceOwnerLive(t, owner)
			if err := connection.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := connection.Write([]byte("exit")); err != nil {
				t.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, connection); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				status, err := client.ContainerStatus(t.Context(), &runtimev1.ContainerStatusRequest{ContainerId: target.ContainerID})
				if err != nil {
					t.Fatal(err)
				}
				if status.GetStatus().GetState() == runtimev1.ContainerState_CONTAINER_EXITED {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("actual CRI container did not report exit")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if result, err := observer.ObserveCaller(t.Context(), target, caller); err == nil || result != (RuntimeContainerCallerObservation{}) {
				t.Fatal("exited original caller remained correlated with a live container")
			}
			if launch, err := observer.ObserveTaskLaunch(t.Context(), filepath.Join(fixture.root, "state"), target, caller); err == nil || launch != nil {
				t.Fatal("exited caller retained a live task launch observation")
			}
			if plan != nil {
				if result, err := observer.ObservePlannedCaller(t.Context(), plan, pods, caller); err == nil || result != (RuntimePlannedCallerObservation{}) {
					t.Fatal("stale Pod running status revived an exited original caller")
				}
			}
			if _, err := client.StartContainer(t.Context(), &runtimev1.StartContainerRequest{ContainerId: target.ContainerID}); err == nil {
				t.Fatal("CRI restarted an exited container under its existing ID")
			}
			if _, err := client.RemoveContainer(t.Context(), &runtimev1.RemoveContainerRequest{ContainerId: target.ContainerID}); err != nil {
				t.Fatal(err)
			}
			if err := caller.Close(); err != nil {
				t.Fatal(err)
			}
			exit := waitNamespaceOwnerExit(t, owner)
			if exit.Owner.Process.HostPID != observation.Process.HostPID || exit.Owner.Container.Target != target {
				t.Fatalf("real CRI removal changed retained process identity: %+v", exit)
			}
			t.Log("retained namespace-owner pidfd observed original exit after actual CRI container removal and caller handle closure")
			if result, err := observer.Inspect(t.Context(), target); err == nil || result != (RuntimeContainerObservation{}) {
				t.Fatal("removed CRI metadata yielded a container observation")
			}
		})
	}
	t.Run("original-daemon-lifetime", func(t *testing.T) {
		verifyRuntimeDaemonLifetime(t, fixture, observer)
	})
}

func verifyRuntimeDaemonLifetime(t *testing.T, fixture *containerdProcessFixture, observer *RuntimeContainerObserver) {
	t.Helper()
	connect := func(path string) net.Conn {
		connection, err := net.Dial("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = connection.Close() })
		return connection
	}
	sameConnection := connect(fixture.socket)
	if err := observer.daemon.authenticate(sameConnection); err != nil {
		t.Fatalf("same daemon connection could not reuse its original pidfd: %v", err)
	}
	if err := sameConnection.Close(); err != nil {
		t.Fatal(err)
	}
	another := startProcessContainerd(t)
	otherConnection := connect(another.socket)
	if err := observer.daemon.authenticate(otherConnection); err == nil {
		t.Fatal("another root containerd replaced the pinned daemon")
	}
	if err := observer.check(); err != nil {
		t.Fatalf("rejected foreign daemon destroyed the original live observer: %v", err)
	}
	fixture.stopDaemon(t, unix.SIGKILL, true)
	if err := observer.daemon.check(); err == nil {
		t.Fatal("original daemon exit left the retained pidfd live")
	}
	if directory, err := observer.daemon.openDirectory("/"); err == nil || directory != nil {
		t.Fatal("lost original daemon still supplied a filesystem view")
	}
	if err := observer.daemon.authenticate(otherConnection); err == nil {
		t.Fatal("another daemon revived a lost original peer")
	}
	if err := observer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := observer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := observer.daemon.authenticate(otherConnection); err == nil {
		t.Fatal("closed observer acquired a new daemon")
	}
	t.Log("same-daemon reconnect accepted; foreign root peer, exited peer and closed observer cannot replace the original process")
}

func prepareCRICgroupDelegation(t *testing.T) {
	t.Helper()
	// Evacuate only this disposable container's init, as a real init system
	// would before delegating domain controllers to nested Pod cgroups.
	if os.Getpid() != 1 || os.Geteuid() != 0 {
		t.Fatal("CRI cgroup setup requires PID 1 of the disposable root sandbox")
	}
	current, err := os.ReadFile("/proc/self/cgroup")
	if err != nil || strings.TrimSpace(string(current)) != "0::/" {
		t.Fatal("CRI cgroup setup requires the private cgroup namespace root")
	}
	root := "/sys/fs/cgroup"
	processes, err := os.ReadFile(filepath.Join(root, "cgroup.procs"))
	if err != nil || strings.TrimSpace(string(processes)) != strconv.Itoa(os.Getpid()) {
		t.Fatalf("private cgroup root contains unexpected processes: %s %v", processes, err)
	}
	observer := filepath.Join(root, "vela-cpu-observer")
	if err := os.Mkdir(observer, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(observer, "cgroup.procs"), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		t.Fatal(err)
	}
	processes, err = os.ReadFile(filepath.Join(root, "cgroup.procs"))
	if err != nil || len(strings.TrimSpace(string(processes))) != 0 {
		t.Fatal("private cgroup root was not evacuated")
	}
	if err := os.WriteFile(filepath.Join(root, "cgroup.subtree_control"), []byte("+cpu +memory +pids"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (fixture *containerdProcessFixture) importCRIImages(t *testing.T) {
	t.Helper()
	binary, err := os.ReadFile(fixture.binary)
	if err != nil {
		t.Fatal(err)
	}
	var layerBytes bytes.Buffer
	writer := tar.NewWriter(&layerBytes)
	if err := writer.WriteHeader(&tar.Header{Name: "probe", Mode: 0o755, Size: int64(len(binary))}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write(binary); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(layerBytes.Bytes())), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	base, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		t.Fatal(err)
	}
	images := make(map[name.Tag]v1.Image)
	for reference, testName := range map[string]string{callerCRISandboxImage: "TestRuntimeCRISandboxHelper", callerCRIRuntimeImage: "TestContainerdRuntimeCallerHelper"} {
		tag, err := name.NewTag(reference)
		if err != nil {
			t.Fatal(err)
		}
		config, err := base.ConfigFile()
		if err != nil {
			t.Fatal(err)
		}
		config.Architecture, config.OS = runtime.GOARCH, "linux"
		config.Config = v1.Config{Entrypoint: []string{"/probe", "-test.run=^" + testName + "$", "-test.timeout=80s"}, User: "65532:65532"}
		image, err := mutate.ConfigFile(base, config)
		if err != nil {
			t.Fatal(err)
		}
		images[tag] = image
	}
	archive := filepath.Join(fixture.root, "cpu-images.tar")
	if err := tarball.MultiWriteToFile(archive, images); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(fixture.ctx, "ctr", "--address", fixture.socket, "--namespace", "k8s.io",
		"images", "import", "--local", "--snapshotter", "native", archive)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("import local CPU-only CRI images: %s %v", output, err)
	}
	client := runtimev1.NewImageServiceClient(fixture.connection)
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := client.ImageStatus(t.Context(), &runtimev1.ImageStatusRequest{Image: &runtimev1.ImageSpec{Image: callerCRISandboxImage}})
		if err == nil && response.GetImage().GetId() != "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("CRI did not discover its imported CPU sandbox image: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (fixture *containerdProcessFixture) createCRICaller(t *testing.T, client runtimev1.RuntimeServiceClient, mode string, plan *RuntimeLaunchPlan) (RuntimeContainerTarget, *net.UnixListener) {
	t.Helper()
	uid := uuid.New()
	root := filepath.Join(fixture.root, uid.String())
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: filepath.Join(root, "caller.sock"), Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	if err := os.Chmod(listener.Addr().String(), 0o666); err != nil {
		t.Fatal(err)
	}
	pidMode := runtimev1.NamespaceMode_CONTAINER
	if mode == "shared-pid" {
		pidMode = runtimev1.NamespaceMode_POD
	}
	namespaces := &runtimev1.NamespaceOption{Network: runtimev1.NamespaceMode_NODE, Pid: pidMode, Ipc: runtimev1.NamespaceMode_POD}
	config := &runtimev1.PodSandboxConfig{
		Metadata:     &runtimev1.PodSandboxMetadata{Name: "runtime-" + mode, Namespace: "vela-cpu", Uid: uid.String(), Attempt: 1},
		LogDirectory: root,
		Linux: &runtimev1.LinuxPodSandboxConfig{CgroupParent: "/vela-cri-" + uid.String(), SecurityContext: &runtimev1.LinuxSandboxSecurityContext{
			NamespaceOptions: namespaces, RunAsUser: &runtimev1.Int64Value{Value: 65532}, RunAsGroup: &runtimev1.Int64Value{Value: 65532}}},
	}
	if plan != nil {
		config.Metadata.Name, config.Metadata.Namespace = plan.pod.Name, plan.pod.Namespace
	}
	sandbox, err := client.RunPodSandbox(t.Context(), &runtimev1.RunPodSandboxRequest{Config: config, RuntimeHandler: "runc"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.StopPodSandbox(ctx, &runtimev1.StopPodSandboxRequest{PodSandboxId: sandbox.PodSandboxId})
		_, _ = client.RemovePodSandbox(ctx, &runtimev1.RemovePodSandboxRequest{PodSandboxId: sandbox.PodSandboxId})
	})
	callerMode := mode
	if mode == "shared-pid" || mode == "planned-owner" {
		callerMode = "owner"
	}
	environment := []*runtimev1.KeyValue{{Key: containerdCallerMode, Value: callerMode}}
	runtimeUID, runtimeGID := int64(65532), int64(65532)
	if plan != nil {
		runtimeUID, runtimeGID = int64(plan.uid), int64(plan.gid)
		environment = append(environment, &runtimev1.KeyValue{Key: "VELA_RUNTIME_CALLER_TEST_PAYLOAD", Value: base64.StdEncoding.EncodeToString(plan.manifest)})
	}
	container, err := client.CreateContainer(t.Context(), &runtimev1.CreateContainerRequest{PodSandboxId: sandbox.PodSandboxId, SandboxConfig: config,
		Config: &runtimev1.ContainerConfig{Metadata: &runtimev1.ContainerMetadata{Name: "model-runtime", Attempt: 3},
			Image: &runtimev1.ImageSpec{Image: callerCRIRuntimeImage}, LogPath: "runtime.log",
			Envs:   environment,
			Mounts: []*runtimev1.Mount{{ContainerPath: "/proof", HostPath: root, Readonly: true}},
			Linux: &runtimev1.LinuxContainerConfig{SecurityContext: &runtimev1.LinuxContainerSecurityContext{
				NamespaceOptions: namespaces, RunAsUser: &runtimev1.Int64Value{Value: runtimeUID}, RunAsGroup: &runtimev1.Int64Value{Value: runtimeGID},
				ReadonlyRootfs: true, NoNewPrivs: true, Capabilities: &runtimev1.Capability{DropCapabilities: []string{"ALL"}}}},
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			data, _ := os.ReadFile(filepath.Join(root, "runtime.log"))
			t.Logf("CRI caller log: %s", data)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.StopContainer(ctx, &runtimev1.StopContainerRequest{ContainerId: container.ContainerId})
		_, _ = client.RemoveContainer(ctx, &runtimev1.RemoveContainerRequest{ContainerId: container.ContainerId})
	})
	target := RuntimeContainerTarget{ContainerID: container.ContainerId, SandboxID: sandbox.PodSandboxId, PodUID: uid,
		PodNamespace: config.Metadata.Namespace, PodName: config.Metadata.Name, ContainerName: "model-runtime", ContainerAttempt: 3}
	if err := target.Validate(); err != nil {
		t.Fatal(err)
	}
	return target, listener
}

func TestRuntimeCRISandboxHelper(t *testing.T) {
	if !strings.HasPrefix(os.Args[0], "/probe") {
		t.Skip("sandbox image entrypoint helper")
	}
	select {
	case <-time.After(75 * time.Second):
	case <-t.Context().Done():
	}
}
