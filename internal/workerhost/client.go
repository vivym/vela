package workerhost

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const maxUnixSocketPathBytes = 100

type ClientConfig struct {
	SocketPath        string
	ExpectedSocketUID uint32
	ExpectedSocketGID uint32
	WorkerID          uuid.UUID
	RootPath          string
	DevicePath        string
	ProjectID         uint32
	Timeout           time.Duration
}

type Client struct {
	connection *grpc.ClientConn
	service    velav1.WorkerHostServiceClient
	workerID   uuid.UUID
	rootPath   string
	devicePath string
	projectID  uint32
	timeout    time.Duration
}

func DialClient(ctx context.Context, config ClientConfig) (*Client, error) {
	if ctx == nil {
		return nil, errors.New("worker-host dial context is required")
	}
	socketPath := filepath.Clean(config.SocketPath)
	rootPath := filepath.Clean(config.RootPath)
	devicePath := filepath.Clean(config.DevicePath)
	if !validAbsolutePath(socketPath) || len(socketPath) > maxUnixSocketPathBytes ||
		!validAbsolutePath(rootPath) || !validAbsolutePath(devicePath) ||
		config.WorkerID == uuid.Nil || config.ProjectID == 0 ||
		config.Timeout < 100*time.Millisecond || config.Timeout > 30*time.Second {
		return nil, errors.New("worker-host quota client configuration is incomplete")
	}
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		before, err := validateSocket(socketPath, config.ExpectedSocketUID, config.ExpectedSocketGID)
		if err != nil {
			return nil, err
		}
		connection, err := (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		if err != nil {
			return nil, err
		}
		after, err := validateSocket(socketPath, config.ExpectedSocketUID, config.ExpectedSocketGID)
		if err != nil || !os.SameFile(before, after) {
			_ = connection.Close()
			return nil, errors.New("worker host socket changed while it was connected")
		}
		return connection, nil
	}
	connection, err := grpc.NewClient(
		"passthrough:///vela-worker-host",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<10), grpc.MaxCallSendMsgSize(64<<10)),
	)
	if err != nil {
		return nil, fmt.Errorf("configure Worker host gRPC client: %w", err)
	}
	connection.Connect()
	dialCtx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()
	for state := connection.GetState(); state != connectivity.Ready; state = connection.GetState() {
		if state == connectivity.Shutdown {
			_ = connection.Close()
			return nil, errors.New("worker-host gRPC connection shut down during startup")
		}
		if !connection.WaitForStateChange(dialCtx, state) {
			_ = connection.Close()
			return nil, fmt.Errorf("connect to Worker host: %w", dialCtx.Err())
		}
	}
	return &Client{
		connection: connection, service: velav1.NewWorkerHostServiceClient(connection),
		workerID: config.WorkerID, rootPath: rootPath, devicePath: devicePath,
		projectID: config.ProjectID, timeout: config.Timeout,
	}, nil
}

func (client *Client) Observe() (QuotaObservation, error) {
	if client == nil || client.service == nil {
		return QuotaObservation{}, errors.New("worker host quota client is not configured")
	}
	rootDevice, rootInode, err := rootIdentity(client.rootPath)
	if err != nil {
		return QuotaObservation{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), client.timeout)
	defer cancel()
	response, err := client.service.GetScratchQuota(ctx, &velav1.GetScratchQuotaRequest{
		WorkerId: client.workerID.String(), RootPath: client.rootPath,
		DevicePath: client.devicePath, ProjectId: client.projectID,
		RootDevice: rootDevice, RootInode: rootInode,
	})
	if err != nil {
		return QuotaObservation{}, err
	}
	if response == nil || response.GetWorkerId() != client.workerID.String() ||
		response.GetRootPath() != client.rootPath || response.GetDevicePath() != client.devicePath ||
		response.GetProjectId() != client.projectID || response.GetRootDevice() != rootDevice ||
		response.GetRootInode() != rootInode || response.GetTotalBytes() <= 0 ||
		response.GetFreeBytes() < 0 || response.GetFreeBytes() > response.GetTotalBytes() ||
		response.GetObservedAt() == nil || response.GetObservedAt().CheckValid() != nil {
		return QuotaObservation{}, status.Error(codes.FailedPrecondition, "worker host returned an unsafe scratch quota observation")
	}
	return QuotaObservation{
		RootDevice: response.GetRootDevice(), RootInode: response.GetRootInode(),
		TotalBytes: response.GetTotalBytes(), FreeBytes: response.GetFreeBytes(),
	}, nil
}

func (client *Client) Close() error {
	if client == nil || client.connection == nil {
		return nil
	}
	return client.connection.Close()
}
