package modelruntimetransport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/vivym/vela/internal/securefile"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	maxUnixSocketPathBytes = 100
	socketPollInterval     = 50 * time.Millisecond
)

var errSocketPermissionsPending = errors.New("ModelRuntime socket permissions are not ready")

type Config struct {
	SocketPath  string
	ExpectedUID uint32
}

type Client struct {
	velav1.ModelRuntimeServiceClient
	connection *grpc.ClientConn
}

func Dial(ctx context.Context, config Config) (*Client, error) {
	if ctx == nil {
		return nil, errors.New("ModelRuntime dial context is required")
	}
	socketPath, err := resolveSocketPath(config.SocketPath)
	if err != nil {
		return nil, err
	}
	if err := waitForSocket(ctx, socketPath, config.ExpectedUID); err != nil {
		return nil, err
	}
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		before, err := validateSocket(socketPath, config.ExpectedUID)
		if err != nil {
			return nil, err
		}
		connection, err := (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		if err != nil {
			return nil, err
		}
		after, err := validateSocket(socketPath, config.ExpectedUID)
		if err != nil || !os.SameFile(before, after) {
			_ = connection.Close()
			return nil, errors.New("ModelRuntime socket changed while it was connected")
		}
		return connection, nil
	}
	connection, err := grpc.NewClient(
		"passthrough:///vela-model-runtime",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(1<<20),
			grpc.MaxCallSendMsgSize(1<<20),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("configure ModelRuntime gRPC client: %w", err)
	}
	connection.Connect()
	for state := connection.GetState(); state != connectivity.Ready; state = connection.GetState() {
		if state == connectivity.Shutdown {
			_ = connection.Close()
			return nil, errors.New("ModelRuntime gRPC connection shut down during startup")
		}
		if !connection.WaitForStateChange(ctx, state) {
			_ = connection.Close()
			return nil, fmt.Errorf("connect to ModelRuntime: %w", ctx.Err())
		}
	}
	return &Client{
		ModelRuntimeServiceClient: velav1.NewModelRuntimeServiceClient(connection),
		connection:                connection,
	}, nil
}

func (client *Client) Close() error {
	if client == nil || client.connection == nil {
		return nil
	}
	return client.connection.Close()
}

func resolveSocketPath(path string) (string, error) {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned != path || strings.ContainsRune(path, '\x00') ||
		len([]byte(path)) > maxUnixSocketPathBytes {
		return "", errors.New("ModelRuntime socket path or owner is invalid")
	}
	parent, err := securefile.ResolveTrustedDirectory(filepath.Dir(cleaned))
	if err != nil {
		return "", fmt.Errorf("validate ModelRuntime socket directory: %w", err)
	}
	return filepath.Join(parent, filepath.Base(cleaned)), nil
}

func waitForSocket(ctx context.Context, path string, expectedUID uint32) error {
	ticker := time.NewTicker(socketPollInterval)
	defer ticker.Stop()
	for {
		if _, err := validateSocket(path, expectedUID); err == nil {
			return nil
		} else if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, errSocketPermissionsPending) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for ModelRuntime socket: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func validateSocket(path string, expectedUID uint32) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("inspect ModelRuntime socket: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || info.Mode()&os.ModeSocket == 0 || stat.Uid != expectedUID {
		return nil, errors.New("ModelRuntime socket owner, type, or permissions are invalid")
	}
	if info.Mode().Perm() != 0o600 {
		return nil, errSocketPermissionsPending
	}
	return info, nil
}
