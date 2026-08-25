package workerhost

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type QuotaObservation struct {
	RootDevice uint64
	RootInode  uint64
	TotalBytes int64
	FreeBytes  int64
}

type QuotaProbe func(context.Context) (QuotaObservation, error)

type ServerConfig struct {
	WorkerID   uuid.UUID
	RootPath   string
	DevicePath string
	ProjectID  uint32
	Clock      func() time.Time
}

type Server struct {
	velav1.UnimplementedWorkerHostServiceServer
	workerID   uuid.UUID
	rootPath   string
	devicePath string
	projectID  uint32
	probe      QuotaProbe
	clock      func() time.Time
}

func NewServer(config ServerConfig, probe QuotaProbe) (*Server, error) {
	rootPath := filepath.Clean(config.RootPath)
	devicePath := filepath.Clean(config.DevicePath)
	if config.WorkerID == uuid.Nil || !validAbsolutePath(rootPath) ||
		!validAbsolutePath(devicePath) || config.ProjectID == 0 || probe == nil {
		return nil, errors.New("worker host quota service configuration is incomplete")
	}
	clock := config.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Server{
		workerID: config.WorkerID, rootPath: rootPath, devicePath: devicePath,
		projectID: config.ProjectID, probe: probe, clock: clock,
	}, nil
}

func (server *Server) GetScratchQuota(
	ctx context.Context,
	request *velav1.GetScratchQuotaRequest,
) (*velav1.GetScratchQuotaResponse, error) {
	if server == nil || server.probe == nil || ctx == nil || request == nil {
		return nil, status.Error(codes.InvalidArgument, "scratch quota request is incomplete")
	}
	workerID, err := uuid.Parse(request.GetWorkerId())
	if err != nil || workerID != server.workerID || request.GetRootPath() != server.rootPath ||
		request.GetDevicePath() != server.devicePath || request.GetProjectId() != server.projectID ||
		request.GetRootDevice() == 0 || request.GetRootInode() == 0 {
		return nil, status.Error(codes.PermissionDenied, "scratch quota request does not match the configured Worker view")
	}
	observation, err := server.probe(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "host scratch quota observation failed")
	}
	if observation.RootDevice != request.GetRootDevice() ||
		observation.RootInode != request.GetRootInode() || observation.TotalBytes <= 0 ||
		observation.FreeBytes < 0 || observation.FreeBytes > observation.TotalBytes {
		return nil, status.Error(codes.FailedPrecondition, "host scratch quota observation is not safe for this Worker root")
	}
	observedAt := timestamppb.New(server.clock().UTC())
	if err := observedAt.CheckValid(); err != nil {
		return nil, status.Error(codes.Internal, "host scratch quota clock is invalid")
	}
	return &velav1.GetScratchQuotaResponse{
		WorkerId: server.workerID.String(), RootPath: server.rootPath,
		DevicePath: server.devicePath, ProjectId: server.projectID,
		RootDevice: observation.RootDevice, RootInode: observation.RootInode,
		TotalBytes: observation.TotalBytes, FreeBytes: observation.FreeBytes,
		ObservedAt: observedAt,
	}, nil
}

func validAbsolutePath(path string) bool {
	return filepath.IsAbs(path) && path != string(filepath.Separator) &&
		!strings.ContainsRune(path, '\x00')
}
