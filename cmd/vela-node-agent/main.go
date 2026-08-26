package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/remediation"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/workerhost"
	"github.com/vivym/vela/internal/workerrecovery"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

const (
	defaultAddress      = ":9443"
	defaultRateInterval = time.Minute
	defaultRateWindow   = time.Hour
	defaultRateMax      = 3
	maxConfigFileBytes  = 1 << 20
)

type config struct {
	address             string
	nodeIdentity        string
	workerID            uuid.UUID
	workerEpoch         int64
	serverCertificate   string
	serverPrivateKey    string
	controllerCA        string
	receiptDirectory    string
	controllersFile     string
	commandsFile        string
	capabilitiesFile    string
	postcheckPath       string
	postcheckArgs       []string
	fencePath           string
	fenceArgs           []string
	rateMinimumInterval time.Duration
	rateWindow          time.Duration
	rateMax             int
	workerQuotaSocket   string
	workerUID           uint32
	workerGID           uint32
	workerScratchRoot   string
	workerXFSDevice     string
	workerXFSProjectID  uint32
}

type commandConfig struct {
	Path string   `json:"path"`
	Args []string `json:"args"`
}

type capabilityConfig struct {
	CertificationRevision string   `json:"certification_revision"`
	PCIBDF                string   `json:"pci_bdf"`
	FailureClasses        []string `json:"failure_classes"`
	Actions               []string `json:"actions"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "vela-node-agent stopped: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configuration, err := loadConfig()
	if err != nil {
		return err
	}
	if os.Geteuid() != 0 {
		return errors.New("vela-node-agent must run as root for certified remediation and XFS project quota observation")
	}
	controllers, err := loadControllerIdentities(configuration.controllersFile)
	if err != nil {
		return err
	}
	resolver, err := nodeagent.NewStaticControllerIdentityResolver(controllers)
	if err != nil {
		return fmt.Errorf("configure controller identities: %w", err)
	}
	commands, err := loadCommands(configuration.commandsFile)
	if err != nil {
		return err
	}
	allowlisted, err := remediation.NewAllowlistedExecutor(nodeagent.ExecCommandRunner{}, commands)
	if err != nil {
		return fmt.Errorf("configure remediation command allowlist: %w", err)
	}
	capabilities, err := loadCapabilities(configuration.capabilitiesFile)
	if err != nil {
		return err
	}
	policy, err := nodeagent.NewStaticCapabilityPolicy(capabilities)
	if err != nil {
		return fmt.Errorf("configure device capability policy: %w", err)
	}
	fence, err := nodeagent.NewCommandFence(nodeagent.ExecCommandRunner{}, configuration.fencePath, configuration.fenceArgs)
	if err != nil {
		return fmt.Errorf("configure host fence: %w", err)
	}
	postcheck, err := nodeagent.NewCommandPostcheck(nodeagent.ExecCommandRunner{}, configuration.postcheckPath, configuration.postcheckArgs)
	if err != nil {
		return fmt.Errorf("configure health post-check: %w", err)
	}
	limiter, err := nodeagent.NewFileRateLimiter(configuration.receiptDirectory, nodeagent.RateLimit{
		MinimumInterval: configuration.rateMinimumInterval,
		Window:          configuration.rateWindow,
		MaxExecutions:   configuration.rateMax,
	})
	if err != nil {
		return fmt.Errorf("configure remediation rate limit: %w", err)
	}
	certified, err := nodeagent.NewCertifiedExecutor(allowlisted, policy, fence, postcheck, limiter)
	if err != nil {
		return fmt.Errorf("configure certified remediation executor: %w", err)
	}
	ledger, err := nodeagent.NewFileLedger(configuration.receiptDirectory)
	if err != nil {
		return err
	}
	server, err := nodeagent.NewServer(
		nodeagent.NodeAgentIdentity{
			NodeIdentity: configuration.nodeIdentity,
			WorkerID:     configuration.workerID,
			WorkerEpoch:  configuration.workerEpoch,
		},
		resolver, certified, ledger,
	)
	if err != nil {
		return fmt.Errorf("configure Node Agent service: %w", err)
	}
	quotaProbe := func(ctx context.Context) (workerhost.QuotaObservation, error) {
		if err := ctx.Err(); err != nil {
			return workerhost.QuotaObservation{}, err
		}
		observation, err := probeWorkerXFSProjectQuota(
			configuration.workerScratchRoot,
			workerrecovery.XFSProjectQuotaConfig{
				DevicePath: configuration.workerXFSDevice,
				ProjectID:  configuration.workerXFSProjectID,
			},
		)
		if err != nil {
			return workerhost.QuotaObservation{}, err
		}
		return workerhost.QuotaObservation{
			RootDevice: observation.RootDevice, RootInode: observation.RootInode,
			TotalBytes: observation.TotalBytes, FreeBytes: observation.FreeBytes,
		}, nil
	}
	if _, err := quotaProbe(context.Background()); err != nil {
		return fmt.Errorf("preflight Worker scratch XFS project quota: %w", err)
	}
	quotaService, err := workerhost.NewServer(workerhost.ServerConfig{
		WorkerID: configuration.workerID, RootPath: configuration.workerScratchRoot,
		DevicePath: configuration.workerXFSDevice, ProjectID: configuration.workerXFSProjectID,
	}, quotaProbe)
	if err != nil {
		return fmt.Errorf("configure Worker host quota service: %w", err)
	}
	credentials, err := nodeagent.NewServerTLSCredentials(
		configuration.serverCertificate, configuration.serverPrivateKey, configuration.controllerCA,
		nodeagent.NodeAgentIdentity{
			NodeIdentity: configuration.nodeIdentity,
			WorkerID:     configuration.workerID,
			WorkerEpoch:  configuration.workerEpoch,
		},
	)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", configuration.address)
	if err != nil {
		return fmt.Errorf("listen for Node Agent gRPC: %w", err)
	}
	defer func() { _ = listener.Close() }()
	quotaListener, err := workerhost.ListenUnix(workerhost.UnixListenerConfig{
		SocketPath: configuration.workerQuotaSocket, SocketOwnerUID: 0,
		SocketOwnerGID: configuration.workerGID, ExpectedPeerUID: configuration.workerUID,
	})
	if err != nil {
		return fmt.Errorf("listen for Worker host quota gRPC: %w", err)
	}
	defer func() { _ = quotaListener.Close() }()
	grpcServer := grpc.NewServer(
		grpc.Creds(credentials), grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(4<<20),
	)
	velav1.RegisterNodeAgentServiceServer(grpcServer, server)
	quotaGRPCServer := grpc.NewServer(grpc.MaxRecvMsgSize(64<<10), grpc.MaxSendMsgSize(64<<10))
	velav1.RegisterWorkerHostServiceServer(quotaGRPCServer, quotaService)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return serveGRPCUntilCanceled(ctx, []grpcEndpoint{
		{name: "Node Agent", server: grpcServer, listener: listener},
		{name: "Worker host quota", server: quotaGRPCServer, listener: quotaListener},
	})
}

type grpcEndpoint struct {
	name     string
	server   *grpc.Server
	listener net.Listener
}

var probeWorkerXFSProjectQuota = workerrecovery.ProbeXFSProjectQuota

func serveGRPCUntilCanceled(ctx context.Context, endpoints []grpcEndpoint) error {
	if ctx == nil || len(endpoints) == 0 {
		return errors.New("node-agent gRPC endpoint configuration is incomplete")
	}
	type serveResult struct {
		name string
		err  error
	}
	for _, endpoint := range endpoints {
		if endpoint.server == nil || endpoint.listener == nil || endpoint.name == "" {
			return errors.New("node-agent gRPC endpoint configuration is incomplete")
		}
	}
	defer func() {
		for _, endpoint := range endpoints {
			_ = endpoint.listener.Close()
		}
	}()
	serveErr := make(chan serveResult, len(endpoints))
	for _, endpoint := range endpoints {
		go func(endpoint grpcEndpoint) {
			serveErr <- serveResult{name: endpoint.name, err: endpoint.server.Serve(endpoint.listener)}
		}(endpoint)
	}
	select {
	case <-ctx.Done():
		stopped := make(chan struct{})
		go func() {
			for _, endpoint := range endpoints {
				endpoint.server.GracefulStop()
			}
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(20 * time.Second):
			for _, endpoint := range endpoints {
				endpoint.server.Stop()
			}
			return errors.New("node-agent gRPC servers did not stop before shutdown deadline")
		}
		return nil
	case result := <-serveErr:
		for _, endpoint := range endpoints {
			endpoint.server.Stop()
		}
		return fmt.Errorf("serve %s gRPC: %w", result.name, result.err)
	}
}

func loadConfig() (config, error) {
	workerID, err := uuid.Parse(os.Getenv("VELA_NODE_AGENT_WORKER_ID"))
	if err != nil || workerID == uuid.Nil {
		return config{}, errors.New("VELA_NODE_AGENT_WORKER_ID must be a UUID")
	}
	configuration := config{
		address:             envOrDefault("VELA_NODE_AGENT_ADDRESS", defaultAddress),
		nodeIdentity:        os.Getenv("VELA_NODE_AGENT_NODE_IDENTITY"),
		workerID:            workerID,
		serverCertificate:   os.Getenv("VELA_NODE_AGENT_TLS_CERT_FILE"),
		serverPrivateKey:    os.Getenv("VELA_NODE_AGENT_TLS_KEY_FILE"),
		controllerCA:        os.Getenv("VELA_NODE_AGENT_CONTROLLER_CA_FILE"),
		receiptDirectory:    os.Getenv("VELA_NODE_AGENT_RECEIPT_DIRECTORY"),
		controllersFile:     os.Getenv("VELA_NODE_AGENT_CONTROLLERS_FILE"),
		commandsFile:        os.Getenv("VELA_NODE_AGENT_COMMANDS_FILE"),
		capabilitiesFile:    os.Getenv("VELA_NODE_AGENT_CAPABILITIES_FILE"),
		postcheckPath:       os.Getenv("VELA_NODE_AGENT_POSTCHECK_PATH"),
		fencePath:           os.Getenv("VELA_NODE_AGENT_FENCE_PATH"),
		workerQuotaSocket:   os.Getenv("VELA_NODE_AGENT_WORKER_QUOTA_SOCKET"),
		workerScratchRoot:   os.Getenv("VELA_NODE_AGENT_WORKER_SCRATCH_ROOT"),
		workerXFSDevice:     os.Getenv("VELA_NODE_AGENT_WORKER_XFS_DEVICE"),
		rateMinimumInterval: defaultRateInterval,
		rateWindow:          defaultRateWindow,
		rateMax:             defaultRateMax,
	}
	configuration.workerEpoch, err = positiveInt64Env("VELA_NODE_AGENT_WORKER_EPOCH")
	if err != nil {
		return config{}, err
	}
	for name, value := range map[string]string{
		"VELA_NODE_AGENT_NODE_IDENTITY":       configuration.nodeIdentity,
		"VELA_NODE_AGENT_TLS_CERT_FILE":       configuration.serverCertificate,
		"VELA_NODE_AGENT_TLS_KEY_FILE":        configuration.serverPrivateKey,
		"VELA_NODE_AGENT_CONTROLLER_CA_FILE":  configuration.controllerCA,
		"VELA_NODE_AGENT_RECEIPT_DIRECTORY":   configuration.receiptDirectory,
		"VELA_NODE_AGENT_CONTROLLERS_FILE":    configuration.controllersFile,
		"VELA_NODE_AGENT_COMMANDS_FILE":       configuration.commandsFile,
		"VELA_NODE_AGENT_CAPABILITIES_FILE":   configuration.capabilitiesFile,
		"VELA_NODE_AGENT_POSTCHECK_PATH":      configuration.postcheckPath,
		"VELA_NODE_AGENT_FENCE_PATH":          configuration.fencePath,
		"VELA_NODE_AGENT_WORKER_QUOTA_SOCKET": configuration.workerQuotaSocket,
		"VELA_NODE_AGENT_WORKER_SCRATCH_ROOT": configuration.workerScratchRoot,
		"VELA_NODE_AGENT_WORKER_XFS_DEVICE":   configuration.workerXFSDevice,
	} {
		if value == "" {
			return config{}, fmt.Errorf("%s is required", name)
		}
	}
	for name, value := range map[string]string{
		"VELA_NODE_AGENT_TLS_CERT_FILE":       configuration.serverCertificate,
		"VELA_NODE_AGENT_TLS_KEY_FILE":        configuration.serverPrivateKey,
		"VELA_NODE_AGENT_CONTROLLER_CA_FILE":  configuration.controllerCA,
		"VELA_NODE_AGENT_RECEIPT_DIRECTORY":   configuration.receiptDirectory,
		"VELA_NODE_AGENT_CONTROLLERS_FILE":    configuration.controllersFile,
		"VELA_NODE_AGENT_COMMANDS_FILE":       configuration.commandsFile,
		"VELA_NODE_AGENT_CAPABILITIES_FILE":   configuration.capabilitiesFile,
		"VELA_NODE_AGENT_WORKER_QUOTA_SOCKET": configuration.workerQuotaSocket,
		"VELA_NODE_AGENT_WORKER_SCRATCH_ROOT": configuration.workerScratchRoot,
		"VELA_NODE_AGENT_WORKER_XFS_DEVICE":   configuration.workerXFSDevice,
	} {
		if !filepath.IsAbs(filepath.Clean(value)) {
			return config{}, fmt.Errorf("%s must be an absolute path", name)
		}
	}
	port, err := portFromAddress(configuration.address)
	if err != nil {
		return config{}, err
	}
	_ = port
	configuration.workerUID, err = positiveUint32Env("VELA_NODE_AGENT_WORKER_UID")
	if err != nil {
		return config{}, err
	}
	configuration.workerGID, err = positiveUint32Env("VELA_NODE_AGENT_WORKER_GID")
	if err != nil {
		return config{}, err
	}
	configuration.workerXFSProjectID, err = positiveUint32Env("VELA_NODE_AGENT_WORKER_XFS_PROJECT_ID")
	if err != nil {
		return config{}, err
	}
	configuration.postcheckArgs, err = parseArgsEnv("VELA_NODE_AGENT_POSTCHECK_ARGS_JSON")
	if err != nil {
		return config{}, err
	}
	configuration.fenceArgs, err = parseArgsEnv("VELA_NODE_AGENT_FENCE_ARGS_JSON")
	if err != nil {
		return config{}, err
	}
	if value := os.Getenv("VELA_NODE_AGENT_RATE_MIN_INTERVAL"); value != "" {
		configuration.rateMinimumInterval, err = time.ParseDuration(value)
		if err != nil {
			return config{}, errors.New("VELA_NODE_AGENT_RATE_MIN_INTERVAL is invalid")
		}
	}
	if value := os.Getenv("VELA_NODE_AGENT_RATE_WINDOW"); value != "" {
		configuration.rateWindow, err = time.ParseDuration(value)
		if err != nil {
			return config{}, errors.New("VELA_NODE_AGENT_RATE_WINDOW is invalid")
		}
	}
	if value := os.Getenv("VELA_NODE_AGENT_RATE_MAX"); value != "" {
		configuration.rateMax, err = strconv.Atoi(value)
		if err != nil {
			return config{}, errors.New("VELA_NODE_AGENT_RATE_MAX is invalid")
		}
	}
	if configuration.rateMinimumInterval <= 0 || configuration.rateWindow <= 0 || configuration.rateMax <= 0 {
		return config{}, errors.New("node Agent rate configuration must be positive")
	}
	return configuration, nil
}

func positiveUint32Env(name string) (uint32, error) {
	value, err := strconv.ParseUint(os.Getenv(name), 10, 32)
	if err != nil || value == 0 {
		return 0, fmt.Errorf("%s must be a positive uint32", name)
	}
	return uint32(value), nil
}

func positiveInt64Env(name string) (int64, error) {
	value, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive int64", name)
	}
	return value, nil
}

func loadControllerIdentities(path string) (map[string]string, error) {
	var identities map[string]string
	if err := readJSONFile(path, &identities); err != nil {
		return nil, fmt.Errorf("load controller identities: %w", err)
	}
	return identities, nil
}

func loadCommands(path string) (map[remediation.ActionLevel]struct {
	Path string
	Args []string
}, error) {
	var configured map[string]commandConfig
	if err := readJSONFile(path, &configured); err != nil {
		return nil, fmt.Errorf("load remediation commands: %w", err)
	}
	commands := make(map[remediation.ActionLevel]struct {
		Path string
		Args []string
	}, len(configured))
	for actionText, command := range configured {
		action := remediation.ActionLevel(actionText)
		if !remediation.IsActionLevel(action) {
			return nil, fmt.Errorf("remediation command action %q is invalid", actionText)
		}
		commands[action] = struct {
			Path string
			Args []string
		}{Path: command.Path, Args: append([]string(nil), command.Args...)}
	}
	return commands, nil
}

func loadCapabilities(path string) (map[string]nodeagent.DeviceCapability, error) {
	var configured map[string]capabilityConfig
	if err := readJSONFile(path, &configured); err != nil {
		return nil, fmt.Errorf("load device capabilities: %w", err)
	}
	capabilities := make(map[string]nodeagent.DeviceCapability, len(configured))
	for device, capability := range configured {
		actions := make(map[remediation.ActionLevel]bool, len(capability.Actions))
		for _, actionText := range capability.Actions {
			actions[remediation.ActionLevel(actionText)] = true
		}
		failureClasses := make(map[string]bool, len(capability.FailureClasses))
		for _, failureClass := range capability.FailureClasses {
			failureClasses[failureClass] = true
		}
		capabilities[device] = nodeagent.DeviceCapability{
			GPUUUID: device, PCIBDF: capability.PCIBDF,
			CertificationRevision: capability.CertificationRevision,
			FailureClasses:        failureClasses, Actions: actions,
		}
	}
	return capabilities, nil
}

func parseArgsEnv(name string) ([]string, error) {
	value := os.Getenv(name)
	if value == "" {
		return nil, nil
	}
	var args []string
	if err := json.Unmarshal([]byte(value), &args); err != nil {
		return nil, fmt.Errorf("%s must be a JSON string array", name)
	}
	for _, arg := range args {
		if strings.ContainsRune(arg, '\x00') || strings.TrimSpace(arg) != arg {
			return nil, fmt.Errorf("%s contains an invalid argument", name)
		}
	}
	return args, nil
}

func readJSONFile(path string, target any) error {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) {
		return errors.New("JSON file path must be absolute")
	}
	content, err := securefile.Read(cleaned, maxConfigFileBytes, false)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("JSON file must contain exactly one document")
	}
	return nil
}

func portFromAddress(address string) (int, error) {
	_, portText, err := net.SplitHostPort(address)
	if err != nil {
		return 0, errors.New("VELA_NODE_AGENT_ADDRESS must contain a host and port")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return 0, errors.New("VELA_NODE_AGENT_ADDRESS port is invalid")
	}
	return port, nil
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
