//go:build integration && linux

package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type runtimeImageStateResetReceipt struct {
	Namespace      string                 `json:"namespace"`
	StateRoot      string                 `json:"state_root"`
	MountNamespace string                 `json:"mount_namespace"`
	Point          runtimeImageCrashPoint `json:"point"`
	Target         RuntimeImageTarget     `json:"target"`
	ControlKey     string                 `json:"control_key"`
	ControlMount   string                 `json:"control_mount"`
	Digest         [sha256.Size]byte      `json:"digest"`
	Size           int64                  `json:"size"`
}

// The outer harness kills the complete seed sandbox without Go cleanup, then
// attaches only its persistent root to a fresh mount namespace and state tree.
func TestRuntimeImageStateReset(t *testing.T) {
	phase := os.Getenv(runtimeImageStateResetPhase)
	if phase == "" {
		t.Skip("requires the two-sandbox volatile state reset harness")
	}
	if os.Getenv("VELA_TEST_CONTAINERD_SANDBOX") != "1" || (phase != "seed" && phase != "recover") {
		t.Fatal("invalid private state reset phase")
	}
	if phase == "seed" {
		seedRuntimeImageStateReset(t)
		return
	}
	recoverRuntimeImageStateReset(t)
}

func stateResetContainerd(t *testing.T) *containerdProcessFixture {
	t.Helper()
	return startPersistentProcessContainerd(t,
		"version = 3\ndisabled_plugins = [\"io.containerd.cri.v1.runtime\", \"io.containerd.cri.v1.images\"]\n",
		filepath.Join(runtimeImageStateResetRoot, "data"))
}

func stateResetObserver(t *testing.T, fixture *containerdProcessFixture, namespace string) *RuntimeImageObserver {
	t.Helper()
	observer, err := DialRuntimeImageObserver(t.Context(), RuntimeImageObserverConfig{
		RuntimeContainerObserverConfig: RuntimeContainerObserverConfig{SocketPath: fixture.socket, NodeIdentity: "cpu-image-crash-node"},
		Namespace:                      namespace, Snapshotter: "native",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observer.Close() })
	return observer
}

func seedRuntimeImageStateReset(t *testing.T) {
	t.Helper()
	fixture := stateResetContainerd(t)
	directory := filepath.Join(fixture.root, "reset-image")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	image, err := mutate.AppendLayers(empty.Image, runtimeExecutableLayer(t, directory, "payload",
		[]executableLayerEntry{{name: "probe", trailer: "state-reset-retained-image"}}))
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
	fixture.mountExecutableImage(t, directory, "state-reset", image)
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
	md, _ := metadata.FromOutgoingContext(fixture.ctx)
	namespace := md.Get("containerd-namespace")[0]
	observer := stateResetObserver(t, fixture, namespace)
	controlKey := "vela-live-image-control-" + uuid.NewString()
	controlCtx := metadata.AppendToOutgoingContext(fixture.ctx, "containerd-lease", controlKey)
	if _, err := observer.leases.Create(controlCtx, &leasesapi.CreateRequest{ID: controlKey}); err != nil {
		t.Fatal(err)
	}
	view, err := observer.snapshots.View(controlCtx, &snapshotsapi.ViewSnapshotRequest{
		Snapshotter: "native", Key: controlKey, Parent: identity.ChainID(diffIDs).String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	activation, err := observer.mounts.Activate(controlCtx, &mountsapi.ActivateRequest{Name: controlKey, Mounts: view.Mounts, Temporary: true})
	if err != nil || len(activation.GetInfo().GetActive()) != 1 {
		t.Fatalf("seed control activation failed: %v", err)
	}
	controlMount := activation.Info.Active[0].MountPoint
	expected, size := runtimeExecutableDigest(t, filepath.Join(controlMount, "probe"))
	target := RuntimeImageTarget{ManifestDigest: manifest.String(), ConfigDigest: configHash.String(), ExecutablePath: "/probe"}
	point := crashRuntimeImageObserver(t, fixture, namespace, target, "activation", 15*time.Second)
	assertRuntimeImageCrashResources(t, fixture, point, "activation", true)
	if info, err := os.Stat(filepath.Join(fixture.root, "state", "io.containerd.mount-manager.v1.bolt", "mounts.db")); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("seed has no volatile mount database: %v", err)
	}
	if count, err := observer.RecoverExpired(t.Context()); err != nil || count != 0 {
		t.Fatalf("seed recovery changed an unexpired observation: %d %v", count, err)
	}
	mountNamespace, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		t.Fatal(err)
	}
	receipt := runtimeImageStateResetReceipt{Namespace: namespace, StateRoot: filepath.Join(fixture.root, "state"),
		MountNamespace: mountNamespace, Point: point, Target: target, ControlKey: controlKey, ControlMount: controlMount, Digest: expected, Size: size}
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(runtimeImageStateResetRoot, "seed-ready.json")
	if err := os.WriteFile(path+".tmp", data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
	// Deliberately remain live. The outer test requires exit 137 and removes
	// this entire sandbox before constructing the recovery sandbox.
	select {}
}

func recoverRuntimeImageStateReset(t *testing.T) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(runtimeImageStateResetRoot, "seed-ready.json"))
	if err != nil {
		t.Fatal(err)
	}
	var receipt runtimeImageStateResetReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	mountNamespace, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		t.Fatal(err)
	}
	// Namespace inode numbers can be reused once the old sandbox is removed.
	// The outer harness proves destruction/recreation; these paths prove loss.
	if _, err := os.Stat(receipt.StateRoot); !os.IsNotExist(err) {
		t.Fatalf("seed volatile state survived sandbox destruction: %v", err)
	}
	assertRuntimeImageMountAbsent(t, receipt.Point.MountPoint)
	assertRuntimeImageMountAbsent(t, receipt.ControlMount)
	fixture := stateResetContainerd(t)
	fixture.ctx = metadata.NewOutgoingContext(fixture.ctx, metadata.Pairs("containerd-namespace", receipt.Namespace))
	observer := stateResetObserver(t, fixture, receipt.Namespace)
	// The lease and view are durable; the activation belonged to lost state.
	assertRuntimeImageCrashResources(t, fixture, receipt.Point, "view", true)
	if _, err := observer.mounts.Info(fixture.ctx, &mountsapi.InfoRequest{Name: receipt.ControlKey}); status.Code(err) != codes.NotFound {
		t.Fatalf("control activation survived volatile state loss: %v", err)
	}
	if count, err := observer.RecoverExpired(t.Context()); err != nil || count != 0 {
		t.Fatalf("fresh recovery collected unexpired durable resources: %d %v", count, err)
	}
	controlCtx := metadata.AppendToOutgoingContext(fixture.ctx, "containerd-lease", receipt.ControlKey)
	view, err := observer.snapshots.Mounts(controlCtx, &snapshotsapi.MountsRequest{Snapshotter: "native", Key: receipt.ControlKey})
	if err != nil {
		t.Fatal(err)
	}
	activation, err := observer.mounts.Activate(controlCtx, &mountsapi.ActivateRequest{Name: receipt.ControlKey, Mounts: view.Mounts, Temporary: true})
	if err != nil {
		t.Fatal(err)
	}
	control := runtimeImageResources{observer: observer, key: receipt.ControlKey, lease: true, view: true, activation: true}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(fixture.ctx), 5*time.Second)
		defer cancel()
		if err := control.close(ctx); err != nil {
			t.Error(err)
		}
	})
	root, err := openRuntimeImageActivation(receipt.ControlKey, view.Mounts, activation.GetInfo())
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}
	controlPath := filepath.Join(activation.Info.Active[0].MountPoint, "probe")
	forceRuntimeImageGC(t, fixture)
	if time.Until(receipt.Point.ExpiresAt) < time.Second {
		t.Fatal("state reset left no pre-expiry verification window")
	}
	assertRuntimeImageCrashResources(t, fixture, receipt.Point, "view", true)
	wait := time.NewTimer(time.Until(receipt.Point.ExpiresAt) + 800*time.Millisecond)
	defer wait.Stop()
	select {
	case <-wait.C:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	assertRuntimeImageCrashResources(t, fixture, receipt.Point, "view", true)
	runRuntimeImageMaintenanceProcess(t, fixture, receipt.Namespace)
	assertRuntimeImageCrashResources(t, fixture, receipt.Point, "activation", false)
	if count, err := observer.RecoverExpired(t.Context()); err != nil || count != 0 {
		t.Fatalf("state reset recovery was not idempotent: %d %v", count, err)
	}
	actual, size := runtimeExecutableDigest(t, controlPath)
	if actual != receipt.Digest || size != receipt.Size {
		t.Fatal("recovery changed the independently retained control image")
	}
	controlView, err := observer.snapshots.Stat(fixture.ctx, &snapshotsapi.StatSnapshotRequest{Snapshotter: "native", Key: receipt.ControlKey})
	if err != nil || controlView.GetInfo().GetKind() != snapshotsapi.Kind_VIEW {
		t.Fatalf("recovery removed the control view metadata: %v", err)
	}
	controlActivation, err := observer.mounts.Info(fixture.ctx, &mountsapi.InfoRequest{Name: receipt.ControlKey})
	if err != nil || !proto.Equal(runtimeImagePersistedActivation(activation.GetInfo()), controlActivation.GetInfo()) {
		t.Fatalf("recovery changed the new control activation: %v", err)
	}
	leases, err := observer.leases.List(fixture.ctx, &leasesapi.ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	controlLeases := 0
	for _, lease := range leases.Leases {
		if lease.ID == receipt.ControlKey && len(lease.Labels) == 0 {
			controlLeases++
		}
	}
	if controlLeases != 1 {
		t.Fatal("recovery removed or changed the non-expiring control lease")
	}
	observed, err := observer.InspectExecutable(t.Context(), receipt.Target)
	if err != nil || observed.Digest != receipt.Digest || observed.SizeBytes != receipt.Size {
		t.Fatalf("state reset lost the source image: %+v %v", observed, err)
	}
	assertRuntimeImageResourcesReleased(t, fixture)
	t.Logf("seed namespace=%s recovery namespace=%s (numbers may be reused); lost state and activations, retained unexpired lease/view, reclaimed one expired observation, preserved control and source image",
		receipt.MountNamespace, mountNamespace)
}
