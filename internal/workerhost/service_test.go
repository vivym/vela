package workerhost

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestScratchQuotaServiceReturnsExactConfiguredObservation(t *testing.T) {
	workerID := uuid.MustParse("83000000-0000-0000-0000-000000000001")
	observedAt := time.Unix(50_000, 123).UTC()
	server, err := NewServer(ServerConfig{
		WorkerID:   workerID,
		RootPath:   "/var/lib/vela/worker/scratch",
		DevicePath: "/dev/nvme1n1",
		ProjectID:  7001,
		Clock:      func() time.Time { return observedAt },
	}, func(context.Context) (QuotaObservation, error) {
		return QuotaObservation{
			RootDevice: 2049,
			RootInode:  42,
			TotalBytes: 2 << 40,
			FreeBytes:  1 << 40,
		}, nil
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	response, err := server.GetScratchQuota(context.Background(), &velav1.GetScratchQuotaRequest{
		WorkerId:   workerID.String(),
		RootPath:   "/var/lib/vela/worker/scratch",
		DevicePath: "/dev/nvme1n1",
		ProjectId:  7001,
		RootDevice: 2049,
		RootInode:  42,
	})
	if err != nil {
		t.Fatalf("GetScratchQuota: %v", err)
	}
	if response.GetWorkerId() != workerID.String() ||
		response.GetRootPath() != "/var/lib/vela/worker/scratch" ||
		response.GetDevicePath() != "/dev/nvme1n1" || response.GetProjectId() != 7001 ||
		response.GetRootDevice() != 2049 || response.GetRootInode() != 42 ||
		response.GetTotalBytes() != 2<<40 || response.GetFreeBytes() != 1<<40 ||
		response.GetObservedAt().AsTime() != observedAt {
		t.Fatalf("quota response = %#v", response)
	}
}

func TestScratchQuotaServiceRejectsMismatchedWorkerView(t *testing.T) {
	workerID := uuid.MustParse("83000000-0000-0000-0000-000000000001")
	server, err := NewServer(ServerConfig{
		WorkerID:   workerID,
		RootPath:   "/var/lib/vela/worker/scratch",
		DevicePath: "/dev/nvme1n1",
		ProjectID:  7001,
	}, func(context.Context) (QuotaObservation, error) {
		return QuotaObservation{RootDevice: 2049, RootInode: 42, TotalBytes: 2 << 40, FreeBytes: 1 << 40}, nil
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	request := &velav1.GetScratchQuotaRequest{
		WorkerId: workerID.String(), RootPath: "/var/lib/vela/worker/scratch",
		DevicePath: "/dev/nvme1n1", ProjectId: 7001, RootDevice: 2049, RootInode: 41,
	}
	if _, err := server.GetScratchQuota(context.Background(), request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("mismatched Worker root identity error = %v", err)
	}
}

func TestScratchQuotaServiceRejectsUnsafeHostObservation(t *testing.T) {
	workerID := uuid.MustParse("83000000-0000-0000-0000-000000000001")
	for name, observation := range map[string]QuotaObservation{
		"changed root":    {RootDevice: 2049, RootInode: 43, TotalBytes: 2 << 40, FreeBytes: 1 << 40},
		"unbounded quota": {RootDevice: 2049, RootInode: 42, TotalBytes: 0, FreeBytes: 0},
		"impossible free": {RootDevice: 2049, RootInode: 42, TotalBytes: 1 << 40, FreeBytes: 2 << 40},
	} {
		t.Run(name, func(t *testing.T) {
			server, err := NewServer(ServerConfig{
				WorkerID:   workerID,
				RootPath:   "/var/lib/vela/worker/scratch",
				DevicePath: "/dev/nvme1n1",
				ProjectID:  7001,
			}, func(context.Context) (QuotaObservation, error) { return observation, nil })
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}
			_, err = server.GetScratchQuota(context.Background(), &velav1.GetScratchQuotaRequest{
				WorkerId: workerID.String(), RootPath: "/var/lib/vela/worker/scratch",
				DevicePath: "/dev/nvme1n1", ProjectId: 7001, RootDevice: 2049, RootInode: 42,
			})
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("unsafe observation error = %v", err)
			}
		})
	}
}

func TestScratchQuotaServiceDoesNotMaskProbeFailure(t *testing.T) {
	workerID := uuid.MustParse("83000000-0000-0000-0000-000000000001")
	server, err := NewServer(ServerConfig{
		WorkerID: workerID, RootPath: "/scratch", DevicePath: "/dev/nvme1n1", ProjectID: 7001,
	}, func(context.Context) (QuotaObservation, error) {
		return QuotaObservation{}, errors.New("quotactl permission denied")
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	_, err = server.GetScratchQuota(context.Background(), &velav1.GetScratchQuotaRequest{
		WorkerId: workerID.String(), RootPath: "/scratch", DevicePath: "/dev/nvme1n1",
		ProjectId: 7001, RootDevice: 1, RootInode: 2,
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("probe failure error = %v", err)
	}
}
