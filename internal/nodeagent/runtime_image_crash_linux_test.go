//go:build integration && linux

package nodeagent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	leasesapi "github.com/containerd/containerd/api/services/leases/v1"
	mountsapi "github.com/containerd/containerd/api/services/mounts/v1"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/uuid"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const runtimeImageCrashEnvironment = "VELA_TEST_IMAGE_CRASH_STAGE"

type runtimeImageCrashPoint struct {
	Key        string    `json:"key"`
	ExpiresAt  time.Time `json:"expires_at"`
	MountPoint string    `json:"mount_point"`
}

func TestRuntimeImageCrashRecovery(t *testing.T) {
	if os.Getenv("VELA_TEST_CONTAINERD_SANDBOX") != "1" {
		t.Skip("requires the explicitly enabled disposable containerd CPU sandbox")
	}
	fixture := startProcessContainerd(t)
	directory := filepath.Join(fixture.root, "crash-image")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	image, err := mutate.AppendLayers(empty.Image, runtimeExecutableLayer(t, directory, "payload", []executableLayerEntry{{name: "probe", trailer: "image-crash-probe"}}))
	if err != nil {
		t.Fatal(err)
	}
	config, err := image.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	config.OS, config.Architecture = "linux", runtime.GOARCH
	image, err = mutate.ConfigFile(image, config)
	if err != nil {
		t.Fatal(err)
	}
	fixture.mountExecutableImage(t, directory, "crash-recovery", image)
	manifest, err := image.Digest()
	if err != nil {
		t.Fatal(err)
	}
	configHash, err := image.ConfigName()
	if err != nil {
		t.Fatal(err)
	}
	var diffIDs []digest.Digest
	for _, id := range config.RootFS.DiffIDs {
		diffIDs = append(diffIDs, digest.Digest(id.String()))
	}
	chain := identity.ChainID(diffIDs).String()
	md, _ := metadata.FromOutgoingContext(fixture.ctx)
	observer, err := DialRuntimeImageObserver(t.Context(), RuntimeImageObserverConfig{
		RuntimeContainerObserverConfig: RuntimeContainerObserverConfig{SocketPath: fixture.socket, NodeIdentity: "cpu-image-crash-node"},
		Namespace:                      md.Get("containerd-namespace")[0], Snapshotter: "native",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observer.Close() })
	target := RuntimeImageTarget{ManifestDigest: manifest.String(), ConfigDigest: configHash.String(), ExecutablePath: "/probe"}
	controlKey := "vela-live-image-control-" + uuid.NewString()
	controlCtx := metadata.AppendToOutgoingContext(fixture.ctx, "containerd-lease", controlKey)
	if _, err := observer.leases.Create(controlCtx, &leasesapi.CreateRequest{ID: controlKey}); err != nil {
		t.Fatal(err)
	}
	control := runtimeImageResources{observer: observer, key: controlKey, lease: true}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(fixture.ctx), 5*time.Second)
		defer cancel()
		if err := control.close(ctx); err != nil {
			t.Error(err)
		}
	})
	view, err := observer.snapshots.View(controlCtx, &snapshotsapi.ViewSnapshotRequest{Snapshotter: "native", Key: controlKey, Parent: chain})
	if err != nil {
		t.Fatal(err)
	}
	control.view = true
	activation, err := observer.mounts.Activate(controlCtx, &mountsapi.ActivateRequest{Name: controlKey, Mounts: view.Mounts, Temporary: true})
	if err != nil {
		t.Fatal(err)
	}
	control.activation = true
	controlMount := activation.GetInfo().Active[0].MountPoint
	controlFile := filepath.Join(controlMount, "probe")
	expected, expectedSize := runtimeExecutableDigest(t, controlFile)
	for _, scenario := range []string{"lease", "view", "activation", "activation-gc"} {
		t.Run(scenario, func(t *testing.T) {
			stage := strings.TrimSuffix(scenario, "-gc")
			point := crashRuntimeImageObserver(t, fixture, observer.namespace, target, stage)
			assertRuntimeImageCrashResources(t, fixture, point, stage, true)
			if count, err := observer.RecoverExpired(t.Context()); err != nil || count != 0 {
				t.Fatalf("recovery selected an unexpired crashed observation: %d %v", count, err)
			}
			assertRuntimeImageCrashResources(t, fixture, point, stage, true)
			forceRuntimeImageGC(t, fixture)
			if time.Until(point.ExpiresAt) < time.Second {
				t.Fatal("crash fixture did not leave a pre-expiry verification window")
			}
			assertRuntimeImageCrashResources(t, fixture, point, stage, true)
			// The daemon is idle after a completed forced GC. Its periodic
			// scheduler skips work without mutations/deletions or a new trigger.
			wait := time.NewTimer(time.Until(point.ExpiresAt) + 800*time.Millisecond)
			defer wait.Stop()
			select {
			case <-wait.C:
			case <-t.Context().Done():
				t.Fatal(t.Context().Err())
			}
			assertRuntimeImageCrashResources(t, fixture, point, stage, true)
			if scenario == "activation-gc" {
				forceRuntimeImageGC(t, fixture)
			} else {
				restarted, err := DialRuntimeImageObserver(t.Context(), RuntimeImageObserverConfig{
					RuntimeContainerObserverConfig: RuntimeContainerObserverConfig{SocketPath: fixture.socket, NodeIdentity: "cpu-image-crash-node"},
					Namespace:                      observer.namespace, Snapshotter: "native",
				})
				if err != nil {
					t.Fatal(err)
				}
				count, recoveryErr := restarted.RecoverExpired(t.Context())
				closeErr := restarted.Close()
				if count != 1 || recoveryErr != nil || closeErr != nil {
					t.Fatalf("new observer did not recover one expired lease: %d %v %v", count, recoveryErr, closeErr)
				}
				count, err = observer.RecoverExpired(t.Context())
				if err != nil || count != 0 {
					t.Fatalf("recovery was not idempotent: %d %v", count, err)
				}
			}
			assertRuntimeImageCrashResources(t, fixture, point, stage, false)
			if point.MountPoint != "" {
				if _, err := os.Stat(point.MountPoint); !os.IsNotExist(err) {
					t.Fatalf("GC retained materialized mount path: %v", err)
				}
				assertRuntimeImageMountAbsent(t, point.MountPoint)
			}
			if _, err := observer.snapshots.Stat(fixture.ctx, &snapshotsapi.StatSnapshotRequest{Snapshotter: "native", Key: controlKey}); err != nil {
				t.Fatal(err)
			}
			if _, err := observer.mounts.Info(fixture.ctx, &mountsapi.InfoRequest{Name: controlKey}); err != nil {
				t.Fatal(err)
			}
			actual, size := runtimeExecutableDigest(t, controlFile)
			if actual != expected || size != expectedSize {
				t.Fatal("GC changed the live control view")
			}
			observed, err := observer.InspectExecutable(t.Context(), target)
			if err != nil || observed.Digest != expected || observed.SizeBytes != expectedSize {
				t.Fatalf("crash recovery lost the source image: %+v %v", observed, err)
			}
			assertRuntimeImageResourcesReleased(t, fixture)
			t.Logf("SIGKILL %s: protected before expiry, retained while idle after expiry, then fully reclaimed; live view and source image preserved", scenario)
		})
	}
}

func crashRuntimeImageObserver(t *testing.T, fixture *containerdProcessFixture, namespace string, target RuntimeImageTarget, stage string) runtimeImageCrashPoint {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
	log, err := os.Create(filepath.Join(fixture.root, "crash-child-"+stage+".log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	command := exec.CommandContext(t.Context(), fixture.binary, "-test.run=^TestRuntimeImageCrashHelper$", "-test.timeout=20s")
	command.Env = append(os.Environ(), runtimeImageCrashEnvironment+"="+stage, "VELA_TEST_IMAGE_SOCKET="+fixture.socket,
		"VELA_TEST_IMAGE_NAMESPACE="+namespace, "VELA_TEST_IMAGE_MANIFEST="+target.ManifestDigest, "VELA_TEST_IMAGE_CONFIG="+target.ConfigDigest)
	command.ExtraFiles = []*os.File{writer}
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
		if t.Failed() {
			data, _ := os.ReadFile(log.Name())
			t.Logf("crashed image helper: %s", data)
		}
	})
	_ = writer.Close()
	if err := reader.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var point runtimeImageCrashPoint
	if err := json.NewDecoder(io.LimitReader(reader, 4096)).Decode(&point); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(point.Key, "vela-image-observation-") {
		t.Fatal("child did not report an observation lease")
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = command.Wait()
	waited = true
	var exited *exec.ExitError
	if !errors.As(err, &exited) {
		t.Fatalf("image observer did not exit by SIGKILL: %v", err)
	}
	waitStatus, ok := exited.Sys().(syscall.WaitStatus)
	if !ok || !waitStatus.Signaled() || waitStatus.Signal() != syscall.SIGKILL {
		t.Fatalf("unexpected helper exit: %v", err)
	}
	return point
}

func forceRuntimeImageGC(t *testing.T, fixture *containerdProcessFixture) {
	t.Helper()
	leases := leasesapi.NewLeasesClient(fixture.connection)
	key := "vela-cpu-gc-trigger-" + uuid.NewString()
	if _, err := leases.Create(fixture.ctx, &leasesapi.CreateRequest{ID: key}); err != nil {
		t.Fatal(err)
	}
	if _, err := leases.Delete(fixture.ctx, &leasesapi.DeleteRequest{ID: key, Sync: true}); err != nil {
		t.Fatal(err)
	}
}

func assertRuntimeImageCrashResources(t *testing.T, fixture *containerdProcessFixture, point runtimeImageCrashPoint, stage string, present bool) {
	t.Helper()
	leases, err := leasesapi.NewLeasesClient(fixture.connection).List(fixture.ctx, &leasesapi.ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, lease := range leases.Leases {
		if lease.ID == point.Key {
			found = true
		}
	}
	if found != present {
		t.Fatalf("crashed lease %s presence=%v expected=%v expires=%s now=%s leases=%v", point.Key, found, present, point.ExpiresAt, time.Now().UTC(), leases.Leases)
	}
	_, err = snapshotsapi.NewSnapshotsClient(fixture.connection).Stat(fixture.ctx, &snapshotsapi.StatSnapshotRequest{Snapshotter: "native", Key: point.Key})
	if present && stage != "lease" {
		if err != nil {
			t.Fatal(err)
		}
	} else if status.Code(err) != codes.NotFound {
		t.Fatalf("unexpected crash view after GC: %v", err)
	}
	_, err = mountsapi.NewMountsClient(fixture.connection).Info(fixture.ctx, &mountsapi.InfoRequest{Name: point.Key})
	if present && stage == "activation" {
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.Open(point.MountPoint)
		if err != nil {
			t.Fatal(err)
		}
		readOnlyErr, closeErr := requireReadOnlyRuntimeImage(file), file.Close()
		if err := errors.Join(readOnlyErr, closeErr); err != nil {
			t.Fatal(err)
		}
	} else if status.Code(err) != codes.NotFound {
		t.Fatalf("unexpected crash activation after GC: %v", err)
	}
}

func assertRuntimeImageMountAbsent(t *testing.T, mountPoint string) {
	t.Helper()
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 4 && fields[4] == mountPoint {
			t.Fatal("GC removed metadata but left the kernel mount")
		}
	}
}

func TestRuntimeImageCrashHelper(t *testing.T) {
	stage := os.Getenv(runtimeImageCrashEnvironment)
	if stage == "" {
		t.Skip("subprocess-only image observer crash helper")
	}
	if os.Getenv("VELA_TEST_CONTAINERD_SANDBOX") != "1" || (stage != "lease" && stage != "view" && stage != "activation") {
		t.Fatal("invalid private crash helper configuration")
	}
	observer, err := DialRuntimeImageObserver(t.Context(), RuntimeImageObserverConfig{
		RuntimeContainerObserverConfig: RuntimeContainerObserverConfig{SocketPath: os.Getenv("VELA_TEST_IMAGE_SOCKET"), NodeIdentity: "cpu-image-crash-node"},
		Namespace:                      os.Getenv("VELA_TEST_IMAGE_NAMESPACE"), Snapshotter: "native",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = observer.Close() }()
	point := &runtimeImageCrashPoint{}
	pause := func() {
		file := os.NewFile(3, "crash-ready")
		if err := json.NewEncoder(file).Encode(point); err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
		// Never return through Go cleanup. The parent kills this exact child
		// after reading the durable-allocation acknowledgement.
		select {}
	}
	observer.leases = &crashRuntimeImageLeases{LeasesClient: observer.leases, point: point, pause: pause, stage: stage}
	observer.snapshots = &crashRuntimeImageSnapshots{SnapshotsClient: observer.snapshots, pause: pause, stage: stage}
	observer.mounts = &crashRuntimeImageMounts{MountsClient: observer.mounts, point: point, pause: pause, stage: stage}
	_, err = observer.InspectExecutable(t.Context(), RuntimeImageTarget{ManifestDigest: os.Getenv("VELA_TEST_IMAGE_MANIFEST"),
		ConfigDigest: os.Getenv("VELA_TEST_IMAGE_CONFIG"), ExecutablePath: "/probe"})
	t.Fatalf("image inspection returned instead of reaching crash boundary: %v", err)
}

type crashRuntimeImageLeases struct {
	leasesapi.LeasesClient
	point *runtimeImageCrashPoint
	pause func()
	stage string
}

func (client *crashRuntimeImageLeases) Create(ctx context.Context, request *leasesapi.CreateRequest, options ...grpc.CallOption) (*leasesapi.CreateResponse, error) {
	expires, err := time.Parse(time.RFC3339, request.Labels["containerd.io/gc.expire"])
	if err != nil || time.Until(expires) < 59*time.Minute || time.Until(expires) > time.Hour {
		return nil, errors.New("production reader omitted its one-hour lease expiry")
	}
	client.point.Key, client.point.ExpiresAt = request.ID, time.Now().Add(6*time.Second).Truncate(time.Second)
	request.Labels["containerd.io/gc.expire"] = client.point.ExpiresAt.Format(time.RFC3339)
	response, err := client.LeasesClient.Create(ctx, request, options...)
	if err == nil && client.stage == "lease" {
		client.pause()
	}
	return response, err
}

type crashRuntimeImageSnapshots struct {
	snapshotsapi.SnapshotsClient
	pause func()
	stage string
}

func (client *crashRuntimeImageSnapshots) View(ctx context.Context, request *snapshotsapi.ViewSnapshotRequest, options ...grpc.CallOption) (*snapshotsapi.ViewSnapshotResponse, error) {
	response, err := client.SnapshotsClient.View(ctx, request, options...)
	if err == nil && client.stage == "view" {
		client.pause()
	}
	return response, err
}

type crashRuntimeImageMounts struct {
	mountsapi.MountsClient
	point *runtimeImageCrashPoint
	pause func()
	stage string
}

func (client *crashRuntimeImageMounts) Activate(ctx context.Context, request *mountsapi.ActivateRequest, options ...grpc.CallOption) (*mountsapi.ActivateResponse, error) {
	response, err := client.MountsClient.Activate(ctx, request, options...)
	if err == nil && client.stage == "activation" {
		client.point.MountPoint = response.Info.Active[0].MountPoint
		client.pause()
	}
	return response, err
}
