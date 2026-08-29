package workerhost

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestUnixQuotaClientReadsBoundQuotaFromExpectedPeer(t *testing.T) {
	root := filepath.Join(t.TempDir(), "scratch")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("create scratch root: %v", err)
	}
	if err := os.Chmod(root, 0o710); err != nil {
		t.Fatalf("set shared scratch root mode: %v", err)
	}
	device, inode, err := rootIdentity(root)
	if err != nil {
		t.Fatalf("rootIdentity: %v", err)
	}
	workerID := uuid.MustParse("83000000-0000-0000-0000-000000000001")
	server, err := NewServer(ServerConfig{
		WorkerID: workerID, RootPath: root, DevicePath: "/dev/nvme1n1", ProjectID: 7001,
	}, func(context.Context) (QuotaObservation, error) {
		return QuotaObservation{
			RootDevice: device, RootInode: inode,
			TotalBytes: 2 << 40, FreeBytes: 1 << 40,
		}, nil
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	socketPath := filepath.Join(shortSocketDirectory(t), "quota.sock")
	listener, err := ListenUnix(UnixListenerConfig{
		SocketPath: socketPath, SocketOwnerUID: uint32(os.Geteuid()),
		SocketOwnerGID:  uint32(os.Getegid()),
		ExpectedPeerUID: uint32(os.Geteuid()),
	})
	if err != nil {
		t.Fatalf("ListenUnix: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	info, err := os.Lstat(socketPath)
	if err != nil || info.Mode().Perm() != 0o660 {
		t.Fatalf("quota socket mode = %v error=%v", info.Mode().Perm(), err)
	}
	grpcServer := grpc.NewServer()
	velav1.RegisterWorkerHostServiceServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	client, err := DialClient(context.Background(), ClientConfig{
		SocketPath: socketPath, WorkerID: workerID, RootPath: root,
		DevicePath: "/dev/nvme1n1", ProjectID: 7001, Timeout: time.Second,
		ExpectedSocketUID: uint32(os.Geteuid()), ExpectedSocketGID: uint32(os.Getegid()),
	})
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	observation, err := client.Observe()
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if observation.RootDevice != device || observation.RootInode != inode ||
		observation.TotalBytes != 2<<40 || observation.FreeBytes != 1<<40 {
		t.Fatalf("quota observation = %#v", observation)
	}
}

func TestUnixListenerRejectsUnexpectedPeerUID(t *testing.T) {
	socketPath := filepath.Join(shortSocketDirectory(t), "quota.sock")
	listener, err := ListenUnix(UnixListenerConfig{
		SocketPath: socketPath, SocketOwnerUID: uint32(os.Geteuid()),
		SocketOwnerGID:  uint32(os.Getegid()),
		ExpectedPeerUID: uint32(os.Geteuid() + 1),
	})
	if err != nil {
		t.Fatalf("ListenUnix: %v", err)
	}
	acceptDone := make(chan error, 1)
	go func() {
		_, acceptErr := listener.Accept()
		acceptDone <- acceptErr
	}()
	connection, err := net.DialTimeout("unix", socketPath, time.Second)
	if err != nil {
		t.Fatalf("dial quota socket: %v", err)
	}
	defer func() { _ = connection.Close() }()
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buffer := make([]byte, 1)
	if _, err := connection.Read(buffer); err == nil {
		t.Fatal("unexpected peer UID connection remained open")
	}
	if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("close listener: %v", err)
	}
	select {
	case err := <-acceptDone:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Accept after close = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("UID-filtered listener did not stop")
	}
}

func TestUnixListenerRejectsReplacingLiveEndpoint(t *testing.T) {
	socketPath := filepath.Join(shortSocketDirectory(t), "quota.sock")
	config := UnixListenerConfig{
		SocketPath: socketPath, SocketOwnerUID: uint32(os.Geteuid()),
		SocketOwnerGID: uint32(os.Getegid()), ExpectedPeerUID: uint32(os.Geteuid()),
	}
	listener, err := ListenUnix(config)
	if err != nil {
		t.Fatalf("first ListenUnix: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	original, err := os.Lstat(socketPath)
	if err != nil {
		t.Fatalf("inspect first socket: %v", err)
	}

	if replacement, err := ListenUnix(config); err == nil {
		_ = replacement.Close()
		t.Fatal("second ListenUnix replaced a live endpoint")
	}
	current, err := os.Lstat(socketPath)
	if err != nil || !os.SameFile(original, current) {
		t.Fatalf("live socket changed after replacement attempt: info=%v error=%v", current, err)
	}
	connection, err := net.DialTimeout("unix", socketPath, time.Second)
	if err != nil {
		t.Fatalf("dial original socket after replacement attempt: %v", err)
	}
	_ = connection.Close()
	lockInfo, err := os.Lstat(socketPath + ".lock")
	if err != nil || !lockInfo.Mode().IsRegular() || lockInfo.Mode().Perm() != 0o600 {
		t.Fatalf("socket lifecycle lock = %v error=%v", lockInfo, err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close original listener: %v", err)
	}
	restarted, err := ListenUnix(config)
	if err != nil {
		t.Fatalf("restart listener after serialized close: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
}

func TestUnixListenerReplacesOnlyStaleEndpoint(t *testing.T) {
	socketPath := filepath.Join(shortSocketDirectory(t), "quota.sock")
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		t.Fatalf("bind stale socket fixture: %v", err)
	}
	stale.SetUnlinkOnClose(false)
	if err := os.Chown(socketPath, os.Geteuid(), os.Getegid()); err != nil {
		_ = stale.Close()
		t.Fatalf("set stale socket ownership: %v", err)
	}
	if err := os.Chmod(socketPath, 0o660); err != nil {
		_ = stale.Close()
		t.Fatalf("set stale socket mode: %v", err)
	}
	if err := stale.Close(); err != nil {
		t.Fatalf("close stale socket fixture: %v", err)
	}

	listener, err := ListenUnix(UnixListenerConfig{
		SocketPath: socketPath, SocketOwnerUID: uint32(os.Geteuid()),
		SocketOwnerGID: uint32(os.Getegid()), ExpectedPeerUID: uint32(os.Geteuid()),
	})
	if err != nil {
		t.Fatalf("replace stale endpoint: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	current, err := os.Lstat(socketPath)
	if err != nil || current.Mode()&os.ModeSocket == 0 {
		t.Fatalf("stale socket was not replaced: info=%v error=%v", current, err)
	}
	connection, err := net.DialTimeout("unix", socketPath, time.Second)
	if err != nil {
		t.Fatalf("dial replacement socket: %v", err)
	}
	_ = connection.Close()
}

func TestUnixListenerCloseDoesNotUnlinkReplacementPath(t *testing.T) {
	directory := shortSocketDirectory(t)
	socketPath := filepath.Join(directory, "quota.sock")
	listener, err := ListenUnix(UnixListenerConfig{
		SocketPath: socketPath, SocketOwnerUID: uint32(os.Geteuid()),
		SocketOwnerGID: uint32(os.Getegid()), ExpectedPeerUID: uint32(os.Geteuid()),
	})
	if err != nil {
		t.Fatalf("ListenUnix: %v", err)
	}
	oldPath := filepath.Join(directory, "old-quota.sock")
	if err := os.Rename(socketPath, oldPath); err != nil {
		_ = listener.Close()
		t.Fatalf("move original socket path: %v", err)
	}
	replacement, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		_ = listener.Close()
		t.Fatalf("bind replacement socket: %v", err)
	}
	t.Cleanup(func() { _ = replacement.Close() })
	replacementInfo, err := os.Lstat(socketPath)
	if err != nil {
		t.Fatalf("inspect replacement socket: %v", err)
	}

	if err := listener.Close(); err != nil {
		t.Fatalf("close original listener: %v", err)
	}
	current, err := os.Lstat(socketPath)
	if err != nil || !os.SameFile(replacementInfo, current) {
		t.Fatalf("replacement socket removed by original close: info=%v error=%v", current, err)
	}
}

type forgedQuotaServer struct {
	velav1.UnimplementedWorkerHostServiceServer
	response *velav1.GetScratchQuotaResponse
}

func (server forgedQuotaServer) GetScratchQuota(
	context.Context,
	*velav1.GetScratchQuotaRequest,
) (*velav1.GetScratchQuotaResponse, error) {
	return server.response, nil
}

func TestUnixQuotaClientRejectsForgedHostResponse(t *testing.T) {
	root := filepath.Join(t.TempDir(), "scratch")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatalf("create scratch root: %v", err)
	}
	device, inode, err := rootIdentity(root)
	if err != nil {
		t.Fatalf("rootIdentity: %v", err)
	}
	workerID := uuid.MustParse("83000000-0000-0000-0000-000000000001")
	socketPath := filepath.Join(shortSocketDirectory(t), "quota.sock")
	listener, err := ListenUnix(UnixListenerConfig{
		SocketPath: socketPath, SocketOwnerUID: uint32(os.Geteuid()),
		SocketOwnerGID:  uint32(os.Getegid()),
		ExpectedPeerUID: uint32(os.Geteuid()),
	})
	if err != nil {
		t.Fatalf("ListenUnix: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	grpcServer := grpc.NewServer()
	velav1.RegisterWorkerHostServiceServer(grpcServer, forgedQuotaServer{response: &velav1.GetScratchQuotaResponse{
		WorkerId: workerID.String(), RootPath: root, DevicePath: "/dev/nvme1n1",
		ProjectId: 7001, RootDevice: device, RootInode: inode + 1,
		TotalBytes: 2 << 40, FreeBytes: 1 << 40, ObservedAt: timestamppb.Now(),
	}})
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)
	client, err := DialClient(context.Background(), ClientConfig{
		SocketPath: socketPath, WorkerID: workerID, RootPath: root,
		DevicePath: "/dev/nvme1n1", ProjectID: 7001, Timeout: time.Second,
		ExpectedSocketUID: uint32(os.Geteuid()), ExpectedSocketGID: uint32(os.Getegid()),
	})
	if err != nil {
		t.Fatalf("DialClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if _, err := client.Observe(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("forged quota response error = %v", err)
	}
}

func shortSocketDirectory(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "vela-wh-")
	if err != nil {
		t.Fatalf("create short socket directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	return directory
}
