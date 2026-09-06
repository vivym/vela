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
	t        *testing.T
	response *leasesapi.ListResponse
	calls    []string
	hook     func(string, string) error
	filters  []string
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
	for _, fault := range []string{"foreign-node", "unknown-kind", "other-snapshotter", "legacy", "extra-label", "malformed-id", "duplicate", "nil-lease",
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
				other.Labels[runtimeImageLeaseKindLabel] = "v2"
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
	return &emptypb.Empty{}, client.fixture.call(ctx, "lease", request.ID)
}

type runtimeImageRecoverySnapshots struct {
	snapshotsapi.SnapshotsClient
	fixture *runtimeImageRecoveryFixture
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
	return &emptypb.Empty{}, client.fixture.call(ctx, "mount", request.Name)
}
