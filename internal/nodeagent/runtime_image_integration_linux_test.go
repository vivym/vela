//go:build integration && linux

package nodeagent

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	contentapi "github.com/containerd/containerd/api/services/content/v1"
	leasesapi "github.com/containerd/containerd/api/services/leases/v1"
	mountsapi "github.com/containerd/containerd/api/services/mounts/v1"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func assertRuntimeImageResourcesReleased(t *testing.T, fixture *containerdProcessFixture) {
	t.Helper()
	leases, err := leasesapi.NewLeasesClient(fixture.connection).List(fixture.ctx, &leasesapi.ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for _, lease := range leases.Leases {
		if strings.HasPrefix(lease.ID, "vela-image-observation-") {
			t.Errorf("image observer leaked lease %s", lease.ID)
		}
	}
	snapshots, err := snapshotsapi.NewSnapshotsClient(fixture.connection).List(fixture.ctx, &snapshotsapi.ListSnapshotsRequest{Snapshotter: "native"})
	if err != nil {
		t.Fatal(err)
	}
	for {
		response, err := snapshots.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, info := range response.Info {
			if strings.HasPrefix(info.Name, "vela-image-observation-") {
				t.Errorf("image observer leaked snapshot %s", info.Name)
			}
		}
	}
	mounts, err := mountsapi.NewMountsClient(fixture.connection).List(fixture.ctx, &mountsapi.ListRequest{})
	if err != nil {
		t.Fatal(err)
	}
	for {
		response, err := mounts.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(response.Info.Name, "vela-image-observation-") {
			t.Errorf("image observer leaked activation %s", response.Info.Name)
		}
	}
}

func testRuntimeImageLaunchFailures(t *testing.T, fixture *containerdProcessFixture, observer *RuntimeImageObserver, target RuntimeImageTarget) {
	t.Helper()
	for _, failure := range []string{"corrupt-content", "lost-activation", "canceled-cleanup", "failed-cleanup"} {
		t.Run("derived-entrypoint-"+failure, func(t *testing.T) {
			candidate := *observer
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			mounts := &faultRuntimeImageMounts{MountsClient: observer.mounts}
			switch failure {
			case "corrupt-content":
				candidate.content = &faultRuntimeImageContent{ContentClient: observer.content}
			case "lost-activation":
				mounts.failActivation = true
			case "canceled-cleanup":
				mounts.cancelCleanup = cancel
			case "failed-cleanup":
				mounts.failCleanup = true
			}
			candidate.mounts = mounts
			launch, err := candidate.InspectLaunch(ctx, target.ManifestDigest)
			if err == nil || launch != nil {
				t.Fatal("failed image observation returned a derived entrypoint")
			}
			if failure == "canceled-cleanup" && !errors.Is(err, context.Canceled) {
				t.Fatal("derived entrypoint lost cancellation")
			}
			if failure == "failed-cleanup" {
				if mounts.key == "" || !strings.Contains(err.Error(), "lease retained") {
					t.Fatal("failed cleanup omitted retained authority")
				}
				cleanup := runtimeImageResources{observer: observer, key: mounts.key, lease: true, view: true, activation: true}
				if err := cleanup.close(fixture.ctx); err != nil {
					t.Fatal(err)
				}
			}
			assertRuntimeImageResourcesReleased(t, fixture)
		})
	}
}

func testRuntimeImageObserverFailures(t *testing.T, fixture *containerdProcessFixture, observer *RuntimeImageObserver, target RuntimeImageTarget) {
	t.Helper()
	for _, failure := range []string{"missing-file", "wrong-config", "corrupt-json", "lost-lease-response", "lost-view-response",
		"lost-activation-response", "lost-journal-response", "journal-update-failure", "canceled-activation", "canceled-cleanup", "changed-activation", "cleanup-failure"} {
		t.Run(failure, func(t *testing.T) {
			candidate := *observer
			requested := target
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			mounts := &faultRuntimeImageMounts{MountsClient: observer.mounts}
			switch failure {
			case "missing-file":
				requested.ExecutablePath = "/missing"
			case "wrong-config":
				requested.ConfigDigest = target.ManifestDigest
			case "corrupt-json":
				candidate.content = &faultRuntimeImageContent{ContentClient: observer.content}
			case "lost-lease-response":
				candidate.leases = &faultRuntimeImageLeases{LeasesClient: observer.leases}
			case "lost-view-response":
				candidate.snapshots = &faultRuntimeImageSnapshots{SnapshotsClient: observer.snapshots}
			case "lost-journal-response", "journal-update-failure":
				candidate.snapshots = &faultRuntimeImageJournal{SnapshotsClient: observer.snapshots, failBefore: failure == "journal-update-failure"}
			case "lost-activation-response":
				mounts.failActivation = true
			case "canceled-activation":
				mounts.cancel = cancel
			case "canceled-cleanup":
				mounts.cancelCleanup = cancel
			case "changed-activation":
				mounts.changeInfo = true
			case "cleanup-failure":
				mounts.failCleanup = true
			}
			candidate.mounts = mounts
			result, err := candidate.InspectExecutable(ctx, requested)
			if err == nil || result != (RuntimeImageExecutableObservation{}) {
				t.Fatalf("failed image inspection returned usable evidence: %+v %v", result, err)
			}
			if strings.HasPrefix(failure, "canceled-") && !errors.Is(err, context.Canceled) {
				t.Fatalf("image observer lost cancellation: %v", err)
			}
			if failure == "cleanup-failure" || failure == "changed-activation" || failure == "journal-update-failure" {
				if !strings.Contains(err.Error(), "lease retained") || mounts.key == "" {
					t.Fatalf("cleanup failure omitted the retained lease identity: %v", err)
				}
				resources, err := observer.leases.ListResources(fixture.ctx, &leasesapi.ListResourcesRequest{ID: mounts.key})
				if err != nil || len(resources.Resources) < 4 {
					t.Fatalf("failed cleanup discarded protected image resources: %+v %v", resources, err)
				}
				cleanup := runtimeImageResources{observer: observer, key: mounts.key, lease: true, view: true, activation: true}
				if err := cleanup.close(fixture.ctx); err != nil {
					t.Fatal(err)
				}
			}
			assertRuntimeImageResourcesReleased(t, fixture)
		})
	}
}

type faultRuntimeImageContent struct{ contentapi.ContentClient }

func (client *faultRuntimeImageContent) Read(ctx context.Context, request *contentapi.ReadContentRequest, options ...grpc.CallOption) (contentapi.Content_ReadClient, error) {
	stream, err := client.ContentClient.Read(ctx, request, options...)
	if err != nil {
		return nil, err
	}
	return &faultRuntimeImageStream{Content_ReadClient: stream}, nil
}

type faultRuntimeImageStream struct{ contentapi.Content_ReadClient }

func (stream *faultRuntimeImageStream) Recv() (*contentapi.ReadContentResponse, error) {
	response, err := stream.Content_ReadClient.Recv()
	if err == nil && len(response.Data) != 0 {
		response.Data[0] ^= 1
	}
	return response, err
}

type faultRuntimeImageLeases struct{ leasesapi.LeasesClient }

func (client *faultRuntimeImageLeases) Create(ctx context.Context, request *leasesapi.CreateRequest, options ...grpc.CallOption) (*leasesapi.CreateResponse, error) {
	if _, err := client.LeasesClient.Create(ctx, request, options...); err != nil {
		return nil, err
	}
	return nil, errors.New("fixture lost committed lease response")
}

type faultRuntimeImageSnapshots struct{ snapshotsapi.SnapshotsClient }

type faultRuntimeImageJournal struct {
	snapshotsapi.SnapshotsClient
	failBefore bool
}

func (client *faultRuntimeImageJournal) Update(ctx context.Context, request *snapshotsapi.UpdateSnapshotRequest, options ...grpc.CallOption) (*snapshotsapi.UpdateSnapshotResponse, error) {
	if request.Info.Labels[runtimeImagePhaseLabel] != "MOUNTED" {
		return client.SnapshotsClient.Update(ctx, request, options...)
	}
	if client.failBefore {
		return nil, errors.New("fixture blocked cleanup journal commit")
	}
	if _, err := client.SnapshotsClient.Update(ctx, request, options...); err != nil {
		return nil, err
	}
	return nil, errors.New("fixture lost committed journal response")
}

func (client *faultRuntimeImageSnapshots) View(ctx context.Context, request *snapshotsapi.ViewSnapshotRequest, options ...grpc.CallOption) (*snapshotsapi.ViewSnapshotResponse, error) {
	if _, err := client.SnapshotsClient.View(ctx, request, options...); err != nil {
		return nil, err
	}
	return nil, errors.New("fixture lost created view response")
}

type faultRuntimeImageMounts struct {
	mountsapi.MountsClient
	failActivation, changeInfo, failCleanup bool
	cancel                                  context.CancelFunc
	cancelCleanup                           context.CancelFunc
	key                                     string
}

func (client *faultRuntimeImageMounts) Activate(ctx context.Context, request *mountsapi.ActivateRequest, options ...grpc.CallOption) (*mountsapi.ActivateResponse, error) {
	response, err := client.MountsClient.Activate(ctx, request, options...)
	if err != nil {
		return nil, err
	}
	client.key = request.Name
	if client.cancel != nil {
		client.cancel()
	}
	if client.failActivation {
		return nil, errors.New("fixture lost completed activation response")
	}
	return response, nil
}

func (client *faultRuntimeImageMounts) Info(ctx context.Context, request *mountsapi.InfoRequest, options ...grpc.CallOption) (*mountsapi.InfoResponse, error) {
	response, err := client.MountsClient.Info(ctx, request, options...)
	if err == nil && client.changeInfo {
		response.Info.System[0].Source += "-changed"
	}
	return response, err
}

func (client *faultRuntimeImageMounts) Deactivate(ctx context.Context, request *mountsapi.DeactivateRequest, options ...grpc.CallOption) (*emptypb.Empty, error) {
	if client.failCleanup {
		return nil, errors.New("fixture blocked cleanup")
	}
	response, err := client.MountsClient.Deactivate(ctx, request, options...)
	if client.cancelCleanup != nil {
		client.cancelCleanup()
	}
	return response, err
}
