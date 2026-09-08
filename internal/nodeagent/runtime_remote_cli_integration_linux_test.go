//go:build integration && linux

package nodeagent

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/uuid"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/modelruntime"
	"google.golang.org/grpc/metadata"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const remoteCLITestImage = "docker.io/vela/remote-runtime:cpu-fixture"

func (fixture *containerdProcessFixture) importRemoteCLIImage(t *testing.T) string {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	for _, path := range []string{"/vela-model-runtime", "/runtime-command.test"} {
		wire, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.WriteHeader(&tar.Header{Name: filepath.Base(path), Mode: 0o755, Size: int64(len(wire))}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(wire); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(buffer.Bytes())), nil })
	if err != nil {
		t.Fatal(err)
	}
	base, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := base.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	configuration.Architecture, configuration.OS = runtime.GOARCH, "linux"
	configuration.Config = v1.Config{
		Entrypoint: []string{"/vela-model-runtime", "serve-remote", "--bootstrap-file", "/runtime-config/bootstrap.json"},
		User:       "10001:10001", WorkingDir: "/", Env: []string{"PATH=" + runtimeRemoteCLIPath, "HOME=/"}}
	image, err := mutate.ConfigFile(base, configuration)
	if err != nil {
		t.Fatal(err)
	}
	tag, err := name.NewTag(remoteCLITestImage)
	if err != nil {
		t.Fatal(err)
	}
	oci, err := layout.Write(filepath.Join(fixture.root, "remote-cli-oci"), empty.Index)
	if err != nil {
		t.Fatal(err)
	}
	if err := oci.AppendImage(image, layout.WithAnnotations(map[string]string{"io.containerd.image.name": tag.String()})); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(fixture.root, "remote-cli.tar")
	if output, err := exec.CommandContext(t.Context(), "tar", "--create", "--file", archive, "--directory", string(oci), ".").CombinedOutput(); err != nil {
		t.Fatalf("archive actual CLI image: %s %v", output, err)
	}
	command := exec.CommandContext(fixture.ctx, "ctr", "--address", fixture.socket, "--namespace", "k8s.io", "images", "import", "--local", "--snapshotter", "native", archive)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("import actual CLI: %s %v", output, err)
	}
	digest, err := image.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return "docker.io/vela/remote-runtime@" + digest.String()
}

func verifyRemoteCLIReservation(t *testing.T, fixture *containerdProcessFixture, observer *RuntimeContainerObserver) {
	t.Helper()
	image := fixture.importRemoteCLIImage(t)
	for _, scenario := range []string{"valid", "extra-env", "wrong-path-env", "wrong-hostname", "duplicate-argument", "hidden-env", "hidden-argument", "before-fleet-env-change", "after-fleet-env-change", "fleet-loss"} {
		t.Run(scenario, func(t *testing.T) {
			config := publicationFixture(t, true, image)
			var manifest modelruntime.LaunchManifest
			if err := json.Unmarshal(config.Plan.manifest, &manifest); err != nil {
				t.Fatal(err)
			}
			scratch := manifest.Runtimes[0].ScratchRoot
			for _, path := range []string{scratch, manifest.Runtimes[0].InputRoot, manifest.Runtimes[0].OutputRoot} {
				if err := os.MkdirAll(path, 0o755); err != nil {
					t.Fatal(err)
				}
				if err := errors.Join(os.Chown(path, 10001, 10001), os.Chmod(path, 0o700)); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() { _ = os.RemoveAll(scratch) })
			config.RuntimeSocket = filepath.Join(scratch, "runtime.sock")
			publication, err := PublishRuntimeBootstrap(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			listen := func(path string) *net.UnixListener {
				t.Helper()
				listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = listener.Close() })
				if err := errors.Join(os.Chown(path, 0, 10001), os.Chmod(path, 0o660)); err != nil {
					t.Fatal(err)
				}
				return listener
			}
			journalListener, startupListener := listen(config.JournalSocket), listen(config.StartupSocket)
			credentials := RuntimeCallerCredentials{UID: 10001, GID: 10001}
			worker := journalEndpointStartCredentials(t, journalListener, filepath.Join(filepath.Dir(config.Directory), "state"), credentials)
			client := runtimev1.NewRuntimeServiceClient(fixture.connection)
			target := createRemoteCLIContainer(t, client, fixture, config, scratch, scenario)
			if _, err := client.StartContainer(t.Context(), &runtimev1.StartContainerRequest{ContainerId: target.ContainerID}); err != nil {
				t.Fatal(err)
			}
			// Enroll the original actual CLI on its first real journal request.
			first := journalEndpointAccept(t, journalListener)
			defer func() { _ = first.Close() }()
			firstCaller, err := ReceiveRuntimeCallerWithRequestLimit(t.Context(), first, credentials, modelruntime.MaximumJournalCommandBytes)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = firstCaller.Close() }()
			runtimeOwner, err := observer.RetainNamespaceOwner(t.Context(), target, firstCaller)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = runtimeOwner.Close() }()
			endpoint, err := NewJournalEndpoint(t.Context(), config.Journal, runtimeOwner, worker.owner)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = endpoint.Close() }()
			reply, err := endpoint.Handle(t.Context(), firstCaller)
			if err != nil {
				t.Fatal(err)
			}
			if err := firstCaller.Reply(t.Context(), reply); err != nil {
				t.Fatal(err)
			}
			server, err := NewJournalServer(endpoint, JournalServerConfig{Credentials: []RuntimeCallerCredentials{credentials}, MaxConcurrent: 2, ExchangeTimeout: 30 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- server.Serve(t.Context(), journalListener) }()
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = server.Shutdown(ctx)
			}()
			connection := journalEndpointAccept(t, startupListener)
			defer func() { _ = connection.Close() }()
			caller, err := ReceiveRuntimeCaller(t.Context(), connection, credentials)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = caller.Close() }()
			request, err := modelruntime.ParseBackendStartupRequest(caller.Payload())
			if err != nil || request.SchemaVersion != 2 || request.BootstrapDigest != publication.Record().BootstrapDigest || request.BootstrapPath != "/runtime-config/bootstrap.json" {
				t.Fatalf("actual CLI consumption: %v", err)
			}
			pod := config.Plan.ExpectedPod()
			pod.UID, pod.ResourceVersion, pod.Spec.NodeName = types.UID(target.PodUID.String()), "1", "cpu-node"
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: target.ContainerName, RestartCount: int32(target.ContainerAttempt), ContainerID: "containerd://" + target.ContainerID, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
			pods := &runtimeLaunchPodFixture{pod: *pod, key: fleetcontroller.ResourceKey{Namespace: pod.Namespace, Name: pod.Name}}
			images, err := DialRuntimeImageObserver(t.Context(), RuntimeImageObserverConfig{RuntimeContainerObserverConfig: RuntimeContainerObserverConfig{SocketPath: fixture.socket, NodeIdentity: "cpu-node"}, Namespace: "k8s.io", Snapshotter: "native"})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = images.Close() }()
			ledger, directory := startupTestLedger(t)
			registry := &startupReservationRegistryFixture{node: "cpu-node", actor: config.Plan.binding.Claim.ActorIdentity}
			changeTask := func(change func(*specs.Process)) {
				t.Helper()
				path := filepath.Join(fixture.root, "state", "io.containerd.runtime.v2.task", "k8s.io", target.ContainerID, "config.json")
				wire, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var task specs.Spec
				if err := json.Unmarshal(wire, &task); err != nil {
					t.Fatal(err)
				}
				change(task.Process)
				wire, err = json.Marshal(task)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, wire, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			changeEnv := func() { changeTask(func(process *specs.Process) { process.Env = append(process.Env, "UNAPPROVED=1") }) }
			if scenario == "hidden-env" {
				changeTask(func(process *specs.Process) {
					process.Env = slices.DeleteFunc(process.Env, func(value string) bool { return value == "UNAPPROVED=1" })
				})
			}
			if scenario == "hidden-argument" {
				changeTask(func(process *specs.Process) { process.Args = process.Args[:4] })
			}
			if scenario == "before-fleet-env-change" {
				ledger.boundary = func(phase string) error {
					if phase == "after-sync" {
						changeEnv()
					}
					return nil
				}
			}
			registry.reserve = func(_ context.Context, request fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
				if scenario == "after-fleet-env-change" {
					changeEnv()
				}
				if scenario == "fleet-loss" {
					return fleet.RuntimeStartupReservation{}, errors.New("lost committed fixture reply")
				}
				return fleet.RuntimeStartupReservation{RuntimeStartupRequest: request, ReservedAt: time.Now().UTC(), Fresh: true}, nil
			}
			reserve := func() (RuntimeStartupReservationRecord, error) {
				return ledger.ReservePublishedRemoteCLI(t.Context(), RuntimeStartupReservationConfig{Plan: config.Plan, Pods: pods, Observer: observer, Caller: caller, Journal: config.Journal, Registry: registry},
					RuntimeStartupImageConfig{Images: images, StateDirectory: filepath.Join(fixture.root, "state"), RuntimePolicy: runtimeTaskPolicyFixture()},
					RuntimeStartupPublicationConfig{Directory: config.Directory, BootstrapPath: request.BootstrapPath})
			}
			result, reserveErr := reserve()
			wantCalls, wantIntents := 0, 0
			if scenario == "valid" || scenario == "after-fleet-env-change" || scenario == "fleet-loss" {
				wantCalls, wantIntents = 1, 1
			}
			if scenario == "before-fleet-env-change" {
				wantIntents = 1
			}
			if registry.calls != wantCalls || len(ledger.starts) != wantIntents || (reserveErr == nil) != (scenario == "valid") {
				// Only this fixture's generated vectors are logged; production
				// observation persists digests and never emits environment values.
				wire, _ := os.ReadFile(filepath.Join(fixture.root, "state", "io.containerd.runtime.v2.task", "k8s.io", target.ContainerID, "config.json"))
				var task specs.Spec
				if json.Unmarshal(wire, &task) == nil && task.Process != nil {
					t.Logf("fixture task args=%q env=%q cwd=%q", task.Process.Args, task.Process.Env, task.Process.Cwd)
				}
				for _, name := range []string{"cmdline", "environ"} {
					data, err := caller.process.ReadFile(name)
					t.Logf("fixture proc %s=%q err=%v", name, data, err)
				}
				t.Fatalf("CLI reservation scenario=%s calls=%d intents=%d result=%+v err=%v", scenario, registry.calls, len(ledger.starts), result, reserveErr)
			}
			if scenario == "valid" {
				record, err := ledger.Inspect(t.Context(), request.JournalID)
				if err != nil || record.Remote.RemoteCLI == nil || record.Remote.RemoteCLI.ArgumentsDigest == ([sha256.Size]byte{}) {
					t.Fatalf("CLI vectors missing from durable intent: %v", err)
				}
				expected := *record.Remote.RemoteCLI
				record.Remote.RemoteCLI.ArgumentsDigest[0] ^= 1
				again, err := ledger.Inspect(t.Context(), request.JournalID)
				if err != nil || *again.Remote.RemoteCLI != expected {
					t.Fatalf("returned CLI history aliases ledger: %v", err)
				}
			}
			if wantIntents != 0 {
				if _, err := reserve(); err == nil || registry.calls != wantCalls {
					t.Fatal("live CLI retried consumed reservation")
				}
			}
			if _, err := os.Stat(filepath.Join(scratch, "events.log")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("backend initialized before explicit test decision")
			}
			// Explicit test decision only: a Fleet reservation is not a grant.
			permit := scenario == "valid"
			decision, err := json.Marshal(modelruntime.BackendStartupDecision{SchemaVersion: 1, RequestDigest: sha256.Sum256(caller.Payload()), Permit: permit})
			if err != nil {
				t.Fatal(err)
			}
			if err := caller.Reply(t.Context(), decision); err != nil {
				t.Fatal(err)
			}
			if permit {
				awaitRemoteCLIBackend(t, scratch, "initialize\n")
				if _, err := client.StopContainer(t.Context(), &runtimev1.StopContainerRequest{ContainerId: target.ContainerID, Timeout: 5}); err != nil {
					t.Fatal(err)
				}
				awaitRemoteCLIBackend(t, scratch, "initialize\nshutdown\n")
			} else {
				awaitRemoteCLIExit(t, client, target)
				if _, err := os.Stat(filepath.Join(scratch, "events.log")); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("denied CLI initialized backend")
				}
				if _, err := os.Stat(config.RuntimeSocket); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("denied CLI published Runtime socket")
				}
			}
			if wantIntents != 0 {
				if err := ledger.Close(); err != nil {
					t.Fatal(err)
				}
				recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = recovered.Close() }()
				ledger = recovered
				if _, err := reserve(); err == nil || registry.calls != wantCalls {
					t.Fatal("restart retried original CLI reservation")
				}
				if scenario == "valid" {
					history, err := recovered.InspectReservation(t.Context(), request.JournalID)
					if err != nil || history != result {
						t.Fatalf("lost recovered CLI receipt: %v", err)
					}
				}
			}
			if err := server.Shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			assertJournalServerJoined(t, server, done)
			criFixture := *fixture
			criFixture.ctx = metadata.NewOutgoingContext(t.Context(), metadata.Pairs("containerd-namespace", "k8s.io"))
			assertRuntimeImageResourcesReleased(t, &criFixture)
			t.Logf("actual CLI + CRI image/task + published readonly mount + Node journal + same-call Fleet fixture: %s calls=%d intents=%d", scenario, registry.calls, wantIntents)
		})
	}
}

func createRemoteCLIContainer(t *testing.T, client runtimev1.RuntimeServiceClient, fixture *containerdProcessFixture, config RuntimeBootstrapPublicationConfig, scratch, scenario string) RuntimeContainerTarget {
	t.Helper()
	uid := uuid.New()
	logs := filepath.Join(fixture.root, "remote-cli-"+uid.String())
	if err := os.Mkdir(logs, 0o755); err != nil {
		t.Fatal(err)
	}
	namespaces := &runtimev1.NamespaceOption{Network: runtimev1.NamespaceMode_NODE, Pid: runtimev1.NamespaceMode_CONTAINER, Ipc: runtimev1.NamespaceMode_POD}
	pod := &runtimev1.PodSandboxConfig{Metadata: &runtimev1.PodSandboxMetadata{Name: config.Plan.pod.Name, Namespace: config.Plan.pod.Namespace, Uid: uid.String(), Attempt: 1}, LogDirectory: logs,
		Linux: &runtimev1.LinuxPodSandboxConfig{CgroupParent: "/vela-remote-cli-" + uid.String(), SecurityContext: &runtimev1.LinuxSandboxSecurityContext{NamespaceOptions: namespaces, RunAsUser: &runtimev1.Int64Value{Value: 65532}, RunAsGroup: &runtimev1.Int64Value{Value: 65532}}}}
	sandbox, err := client.RunPodSandbox(t.Context(), &runtimev1.RunPodSandboxRequest{Config: pod, RuntimeHandler: "explicit"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.StopPodSandbox(ctx, &runtimev1.StopPodSandboxRequest{PodSandboxId: sandbox.PodSandboxId})
		_, _ = client.RemovePodSandbox(ctx, &runtimev1.RemovePodSandboxRequest{PodSandboxId: sandbox.PodSandboxId})
	})
	container := &runtimev1.ContainerConfig{Metadata: &runtimev1.ContainerMetadata{Name: "model-runtime", Attempt: 3}, Image: &runtimev1.ImageSpec{Image: remoteCLITestImage}, LogPath: "runtime.log",
		Envs:   []*runtimev1.KeyValue{{Key: "HOSTNAME", Value: config.Plan.pod.Name}},
		Mounts: []*runtimev1.Mount{{ContainerPath: filepath.Dir(config.Directory), HostPath: filepath.Dir(config.Directory), Readonly: true}, {ContainerPath: "/runtime-config", HostPath: config.Directory, Readonly: true}, {ContainerPath: scratch, HostPath: scratch}},
		Linux:  &runtimev1.LinuxContainerConfig{SecurityContext: &runtimev1.LinuxContainerSecurityContext{NamespaceOptions: namespaces, RunAsUser: &runtimev1.Int64Value{Value: 10001}, RunAsGroup: &runtimev1.Int64Value{Value: 10001}, ReadonlyRootfs: true, NoNewPrivs: true, Capabilities: &runtimev1.Capability{DropCapabilities: []string{"ALL"}}}}}
	switch scenario {
	case "extra-env", "hidden-env":
		container.Envs = append(container.Envs, &runtimev1.KeyValue{Key: "UNAPPROVED", Value: "1"})
	case "wrong-path-env":
		container.Envs = append(container.Envs, &runtimev1.KeyValue{Key: "PATH", Value: "/untrusted"})
	case "wrong-hostname":
		container.Envs = []*runtimev1.KeyValue{{Key: "HOSTNAME", Value: "unapproved"}}
	case "duplicate-argument", "hidden-argument":
		container.Args = []string{"--bootstrap-file", "/runtime-config/bootstrap.json"}
	}
	created, err := client.CreateContainer(t.Context(), &runtimev1.CreateContainerRequest{PodSandboxId: sandbox.PodSandboxId, SandboxConfig: pod, Config: container})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			wire, _ := os.ReadFile(filepath.Join(logs, "runtime.log"))
			t.Logf("actual CLI log: %s", wire)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.StopContainer(ctx, &runtimev1.StopContainerRequest{ContainerId: created.ContainerId})
		_, _ = client.RemoveContainer(ctx, &runtimev1.RemoveContainerRequest{ContainerId: created.ContainerId})
	})
	return RuntimeContainerTarget{ContainerID: created.ContainerId, SandboxID: sandbox.PodSandboxId, PodUID: uid, PodNamespace: pod.Metadata.Namespace, PodName: pod.Metadata.Name, ContainerName: "model-runtime", ContainerAttempt: 3}
}

func awaitRemoteCLIBackend(t *testing.T, scratch, expected string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		wire, _ := os.ReadFile(filepath.Join(scratch, "events.log"))
		if string(wire) == expected {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("actual backend events: got=%q want=%q", wire, expected)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func awaitRemoteCLIExit(t *testing.T, client runtimev1.RuntimeServiceClient, target RuntimeContainerTarget) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		response, err := client.ContainerStatus(t.Context(), &runtimev1.ContainerStatusRequest{ContainerId: target.ContainerID})
		if err != nil {
			t.Fatal(err)
		}
		if response.GetStatus().GetState() == runtimev1.ContainerState_CONTAINER_EXITED {
			if response.GetStatus().GetExitCode() == 0 {
				t.Fatal("denied CLI exited successfully")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("denied CLI did not exit")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
