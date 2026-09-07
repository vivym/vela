package nodeagent

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	leasesapi "github.com/containerd/containerd/api/services/leases/v1"
	mountsapi "github.com/containerd/containerd/api/services/mounts/v1"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/api/types"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type runtimeImageRecoveryFixture struct {
	t            *testing.T
	response     *leasesapi.ListResponse
	calls        []string
	hook         func(string, string) error
	filters      []string
	missing      bool
	mountMissing bool
	viewHook     func(*snapshotsapi.Info)
	gcTriggers   int
}

func newRuntimeImageRecoveryFixture(t *testing.T) (*RuntimeImageObserver, *runtimeImageRecoveryFixture) {
	t.Helper()
	fixture := &runtimeImageRecoveryFixture{t: t, response: &leasesapi.ListResponse{}}
	observer := &RuntimeImageObserver{namespace: "vela-recovery-fixture", snapshotter: "native", local: &RuntimeContainerObserver{
		nodeIdentity: "recovery-node", check: func() error { return nil }, bootID: func() (string, error) { return "cc6392b0-d367-4070-8f2f-fdcfb7754c23", nil },
	}}
	observer.leases = &runtimeImageRecoveryLeases{fixture: fixture}
	observer.snapshots = &runtimeImageRecoverySnapshots{fixture: fixture}
	observer.mounts = &runtimeImageRecoveryMounts{fixture: fixture}
	return observer, fixture
}

func recoveryFixtureLease(observer *RuntimeImageObserver, expiry time.Time) *leasesapi.Lease {
	return &leasesapi.Lease{ID: "vela-image-observation-" + uuid.NewString(), CreatedAt: timestamppb.New(expiry.Add(-time.Hour + time.Second)),
		Labels: observer.leaseLabels(expiry)}
}

func TestRuntimeImageRecoveryOwnership(t *testing.T) {
	for _, fault := range []string{"foreign-node", "unknown-kind", "legacy-kind", "other-snapshotter", "legacy", "extra-label", "malformed-id", "duplicate", "nil-lease",
		"nil-response", "nil-created", "invalid-created", "invalid-expiry", "oversized-ttl", "expiry-before-created"} {
		t.Run(fault, func(t *testing.T) {
			observer, fixture := newRuntimeImageRecoveryFixture(t)
			expired := recoveryFixtureLease(observer, time.Now().Add(-time.Minute))
			other := proto.CloneOf(expired)
			other.ID = "vela-image-observation-" + uuid.NewString()
			fixture.response.Leases = []*leasesapi.Lease{expired, other}
			switch fault {
			case "foreign-node":
				other.Labels[runtimeImageLeaseNodeLabel] = strings.Repeat("0", 64)
			case "unknown-kind":
				other.Labels[runtimeImageLeaseKindLabel] = "v3"
			case "legacy-kind":
				other.Labels[runtimeImageLeaseKindLabel] = "v1"
			case "other-snapshotter":
				other.Labels[runtimeImageLeaseSnapshotterLabel] = "overlayfs"
			case "legacy":
				other.Labels = map[string]string{runtimeImageLeaseExpiryLabel: other.Labels[runtimeImageLeaseExpiryLabel]}
			case "extra-label":
				other.Labels["fixture-extra"] = "unrecognized"
			case "malformed-id":
				other.ID = "vela-image-observation-not-a-uuid"
			case "duplicate":
				other.ID = expired.ID
			case "nil-lease":
				fixture.response.Leases[1] = nil
			case "nil-response":
				fixture.response = nil
			case "nil-created":
				other.CreatedAt = nil
			case "invalid-created":
				other.CreatedAt.Seconds = -10000000000000
			case "invalid-expiry":
				other.Labels[runtimeImageLeaseExpiryLabel] = "not-a-time"
			case "oversized-ttl":
				other.CreatedAt = timestamppb.New(time.Now().Add(-3 * time.Hour))
			case "expiry-before-created":
				other.CreatedAt = timestamppb.New(time.Now())
			}
			count, err := observer.RecoverExpired(t.Context())
			if count != 0 || err == nil || len(fixture.calls) != 0 {
				t.Fatalf("unrecognized ownership allowed partial deletion: %d %v %v", count, err, fixture.calls)
			}
		})
	}
}

func TestRuntimeImageRecoveryBatch(t *testing.T) {
	observer, fixture := newRuntimeImageRecoveryFixture(t)
	for range maximumRuntimeImageRecoveryBatch + 1 {
		fixture.response.Leases = append(fixture.response.Leases, recoveryFixtureLease(observer, time.Now().Add(-time.Minute)))
	}
	live := recoveryFixtureLease(observer, time.Now().Add(30*time.Minute))
	fixture.response.Leases = append(fixture.response.Leases, live)
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("containerd-namespace", "injected-namespace", "containerd-lease", "injected-lease"))
	count, err := observer.RecoverExpired(ctx)
	if err != nil || count != maximumRuntimeImageRecoveryBatch || len(fixture.calls) != 3*count {
		t.Fatalf("recovery was not bounded: %d %v %v", count, err, fixture.calls)
	}
	if len(fixture.filters) != 1 || !strings.Contains(fixture.filters[0], runtimeImageLeaseKindLabel) || !strings.Contains(fixture.filters[0], runtimeImageLeaseNodeLabel) ||
		!strings.Contains(fixture.filters[0], observer.leaseLabels(time.Now())[runtimeImageLeaseNodeLabel]) {
		t.Fatalf("unscoped recovery discovery: %v", fixture.filters)
	}
	for index := 0; index < len(fixture.calls); index += 3 {
		id := strings.TrimPrefix(fixture.calls[index], "mount:")
		if id == live.ID || !slices.Equal(fixture.calls[index:index+3], []string{"mount:" + id, "view:" + id, "lease:" + id}) {
			t.Fatalf("recovery deleted a live lease or violated cleanup order: %v", fixture.calls[index:index+3])
		}
	}
}

func TestRuntimeImageRecoveryFailures(t *testing.T) {
	for _, failure := range []string{"mount", "view", "lease", "missing", "canceled", "closed"} {
		t.Run(failure, func(t *testing.T) {
			observer, fixture := newRuntimeImageRecoveryFixture(t)
			fixture.missing = failure == "missing"
			first := recoveryFixtureLease(observer, time.Now().Add(-time.Minute))
			second := recoveryFixtureLease(observer, time.Now().Add(-time.Minute))
			fixture.response.Leases = []*leasesapi.Lease{first, second}
			ids := []string{first.ID, second.ID}
			slices.Sort(ids)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fixture.hook = func(operation, id string) error {
				if failure == "missing" {
					return status.Error(codes.NotFound, "already collected")
				}
				if id == ids[1] && operation == failure {
					return errors.New("fixture blocked cleanup")
				}
				if id == ids[0] && operation == "lease" {
					if failure == "canceled" {
						cancel()
					}
					if failure == "closed" {
						observer.local.check = func() error { return errors.New("fixture closed observer") }
					}
				}
				return nil
			}
			count, err := observer.RecoverExpired(ctx)
			if failure == "missing" {
				if err != nil || count != 2 {
					t.Fatalf("missing resources are not idempotent: %d %v", count, err)
				}
				return
			}
			if count != 1 || err == nil {
				t.Fatalf("failed cleanup lost partial progress: %d %v", count, err)
			}
			if failure == "mount" && (slices.Contains(fixture.calls, "view:"+ids[1]) || slices.Contains(fixture.calls, "lease:"+ids[1])) {
				t.Fatal("mount failure did not preserve the view and lease")
			}
			if failure == "view" && slices.Contains(fixture.calls, "lease:"+ids[1]) {
				t.Fatal("view failure did not preserve the lease")
			}
		})
	}
}

func (fixture *runtimeImageRecoveryFixture) call(ctx context.Context, operation, id string) error {
	fixture.t.Helper()
	md, _ := metadata.FromOutgoingContext(ctx)
	if !slices.Equal(md.Get("containerd-namespace"), []string{"vela-recovery-fixture"}) || len(md.Get("containerd-lease")) != 0 {
		fixture.t.Fatalf("recovery accepted caller-supplied metadata: %v", md)
	}
	fixture.calls = append(fixture.calls, operation+":"+id)
	if fixture.hook != nil {
		return fixture.hook(operation, id)
	}
	return nil
}

type runtimeImageRecoveryLeases struct {
	leasesapi.LeasesClient
	fixture *runtimeImageRecoveryFixture
}

func (client *runtimeImageRecoveryLeases) List(_ context.Context, request *leasesapi.ListRequest, _ ...grpc.CallOption) (*leasesapi.ListResponse, error) {
	client.fixture.filters = request.Filters
	return client.fixture.response, nil
}

func (client *runtimeImageRecoveryLeases) Delete(ctx context.Context, request *leasesapi.DeleteRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	if !request.Sync {
		client.fixture.t.Fatal("recovery did not require synchronous GC")
	}
	if strings.HasPrefix(request.ID, "vela-image-cleanup-trigger-") {
		return &emptypb.Empty{}, nil
	}
	return &emptypb.Empty{}, client.fixture.call(ctx, "lease", request.ID)
}

func (client *runtimeImageRecoveryLeases) Create(_ context.Context, request *leasesapi.CreateRequest, _ ...grpc.CallOption) (*leasesapi.CreateResponse, error) {
	if !strings.HasPrefix(request.ID, "vela-image-cleanup-trigger-") || len(request.Labels) != 1 || request.Labels["containerd.io/gc.expire"] == "" {
		client.fixture.t.Fatal("recovery did not create a bounded empty GC trigger")
	}
	client.fixture.gcTriggers++
	return &leasesapi.CreateResponse{Lease: &leasesapi.Lease{ID: request.ID}}, nil
}

type runtimeImageRecoverySnapshots struct {
	snapshotsapi.SnapshotsClient
	fixture *runtimeImageRecoveryFixture
}

func (client *runtimeImageRecoverySnapshots) Stat(_ context.Context, request *snapshotsapi.StatSnapshotRequest, _ ...grpc.CallOption) (*snapshotsapi.StatSnapshotResponse, error) {
	if client.fixture.missing {
		return nil, status.Error(codes.NotFound, "already collected")
	}
	info := &snapshotsapi.Info{Name: request.Key, Kind: snapshotsapi.Kind_VIEW, Labels: map[string]string{
		runtimeImagePhaseLabel: "MOUNTED", runtimeImageBootLabel: "cc6392b0-d367-4070-8f2f-fdcfb7754c23",
		runtimeImageMountPathLabel: "/run/vela-recovery-fixture/" + request.Key,
	}}
	if client.fixture.viewHook != nil {
		client.fixture.viewHook(info)
	}
	return &snapshotsapi.StatSnapshotResponse{Info: info}, nil
}

func (client *runtimeImageRecoverySnapshots) Remove(ctx context.Context, request *snapshotsapi.RemoveSnapshotRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	if request.Snapshotter != "native" {
		client.fixture.t.Fatal("recovery selected an unqualified snapshotter")
	}
	return &emptypb.Empty{}, client.fixture.call(ctx, "view", request.Key)
}

type runtimeImageRecoveryMounts struct {
	mountsapi.MountsClient
	fixture *runtimeImageRecoveryFixture
}

func (client *runtimeImageRecoveryMounts) Deactivate(ctx context.Context, request *mountsapi.DeactivateRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	err := client.fixture.call(ctx, "mount", request.Name)
	if err == nil && client.fixture.mountMissing {
		err = status.Error(codes.NotFound, "lost activation metadata")
	}
	return &emptypb.Empty{}, err
}

func (client *runtimeImageRecoveryMounts) Info(_ context.Context, request *mountsapi.InfoRequest, _ ...grpc.CallOption) (*mountsapi.InfoResponse, error) {
	if client.fixture.missing || client.fixture.mountMissing {
		return nil, status.Error(codes.NotFound, "already collected")
	}
	path := "/run/vela-recovery-fixture/" + request.Name
	return &mountsapi.InfoResponse{Info: &types.ActivationInfo{Name: request.Name,
		Active: []*types.ActiveMount{{Mount: &types.Mount{Type: "bind"}, MountPoint: path, MountedAt: timestamppb.New(time.Unix(1, 0))}},
		System: []*types.Mount{{Type: "bind", Source: path, Options: []string{"rbind"}}},
	}}, nil
}

func TestRuntimeImageRecoveryMissingActivation(t *testing.T) {
	for _, scenario := range []string{"unallocated", "pending", "rebooted-pending", "recorded"} {
		t.Run(scenario, func(t *testing.T) {
			observer, fixture := newRuntimeImageRecoveryFixture(t)
			fixture.response.Leases = []*leasesapi.Lease{recoveryFixtureLease(observer, time.Now().Add(-time.Minute))}
			fixture.mountMissing = true
			fixture.viewHook = func(info *snapshotsapi.Info) {
				if scenario == "recorded" {
					return
				}
				delete(info.Labels, runtimeImageMountPathLabel)
				info.Labels[runtimeImagePhaseLabel] = "ACTIVATING"
				if scenario == "unallocated" {
					info.Labels[runtimeImagePhaseLabel] = "VIEW"
				}
				if scenario == "rebooted-pending" {
					info.Labels[runtimeImageBootLabel] = uuid.NewString()
				}
			}
			count, err := observer.RecoverExpired(t.Context())
			if scenario == "pending" {
				if err == nil || count != 0 || len(fixture.calls) != 0 {
					t.Fatalf("ambiguous activation was not retained: %d %v %v", count, err, fixture.calls)
				}
			} else if err != nil || count != 1 {
				t.Fatalf("proven missing activation was not recovered: %d %v", count, err)
			}
			if (fixture.gcTriggers == 1) != (scenario == "recorded") {
				t.Fatalf("unexpected orphan GC trigger count: %d", fixture.gcTriggers)
			}
		})
	}
}

func TestRuntimeImageRecoveryRejectsInvalidJournal(t *testing.T) {
	for _, fault := range []string{"missing", "unknown-phase", "bad-boot", "bad-path", "extra-label", "wrong-kind", "wrong-key"} {
		t.Run(fault, func(t *testing.T) {
			observer, fixture := newRuntimeImageRecoveryFixture(t)
			fixture.response.Leases = []*leasesapi.Lease{recoveryFixtureLease(observer, time.Now().Add(-time.Minute))}
			fixture.viewHook = func(info *snapshotsapi.Info) {
				switch fault {
				case "missing":
					info.Labels = nil
				case "unknown-phase":
					info.Labels[runtimeImagePhaseLabel] = "unknown"
				case "bad-boot":
					info.Labels[runtimeImageBootLabel] = "not-a-boot-id"
				case "bad-path":
					info.Labels[runtimeImageMountPathLabel] = "relative/path"
				case "extra-label":
					info.Labels["unexpected"] = "unknown"
				case "wrong-kind":
					info.Kind = snapshotsapi.Kind_COMMITTED
				case "wrong-key":
					info.Name += "-other"
				}
			}
			count, err := observer.RecoverExpired(t.Context())
			if err == nil || count != 0 || len(fixture.calls) != 0 || fixture.gcTriggers != 0 {
				t.Fatalf("invalid journal allowed cleanup: %d %v %v", count, err, fixture.calls)
			}
		})
	}
}
