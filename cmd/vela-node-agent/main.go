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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleettransport"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/remediation"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
	"github.com/vivym/vela/internal/workerhost"
	"github.com/vivym/vela/internal/workerrecovery"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

const (
	defaultAddress                      = ":9443"
	defaultRateInterval                 = time.Minute
	defaultRateWindow                   = time.Hour
	defaultRateMax                      = 3
	defaultWorkerInstanceReportInterval = 30 * time.Second
	defaultWorkerInstanceCallTimeout    = 10 * time.Second
	defaultWorkerInstanceBackoffInitial = time.Second
	defaultWorkerInstanceBackoffMax     = 30 * time.Second
	defaultWorkerInstanceEvidenceTTL    = 2 * time.Minute
	defaultFleetDialTimeout             = 15 * time.Second
	maxConfigFileBytes                  = 1 << 20
	maxWorkerInstances                  = 1024
)

var (
	workerInstanceGPUUUIDPattern = regexp.MustCompile(
		`^GPU-[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`,
	)
	workerInstancePCIBDFPattern = regexp.MustCompile(`^[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}[.][0-7]$`)
	workerInstanceKeyPattern    = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
	workerInstanceDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	workerInstanceImagePattern  = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

type config struct {
	address                      string
	nodeIdentity                 string
	workerID                     uuid.UUID
	workerEpoch                  int64
	serverCertificate            string
	serverPrivateKey             string
	controllerCA                 string
	receiptDirectory             string
	controllersFile              string
	commandsFile                 string
	capabilitiesFile             string
	postcheckPath                string
	postcheckArgs                []string
	fencePath                    string
	fenceArgs                    []string
	rateMinimumInterval          time.Duration
	rateWindow                   time.Duration
	rateMax                      int
	workerQuotaSocket            string
	workerUID                    uint32
	workerGID                    uint32
	workerScratchRoot            string
	workerXFSDevice              string
	workerXFSProjectID           uint32
	fleetAddress                 string
	fleetServerName              string
	fleetCA                      string
	fleetClientCertificate       string
	fleetClientPrivateKey        string
	workerInstancesFile          string
	workerInstanceStateDirectory string
	nvidiaSMIPath                string
	pciBusDevicesRoot            string
	sysDevicesRoot               string
	nvidiaDriverVersionPath      string
	bootIDPath                   string
	workerInstanceReportInterval time.Duration
	workerInstanceCallTimeout    time.Duration
	workerInstanceBackoffInitial time.Duration
	workerInstanceBackoffMax     time.Duration
	workerInstanceEvidenceTTL    time.Duration
	fleetDialTimeout             time.Duration
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

type workerInstanceTemplateConfig struct {
	SchemaVersion       int                            `json:"schema_version"`
	WorkerInstanceID    string                         `json:"worker_instance_id"`
	InstanceEpoch       int64                          `json:"instance_epoch"`
	ControlSessionEpoch int64                          `json:"control_session_epoch"`
	DeviceSet           workerDeviceSetTemplateConfig  `json:"device_set"`
	Members             []workerMemberTemplateConfig   `json:"members"`
	Residencies         []modelResidencyTemplateConfig `json:"residencies"`
	Capacity            workerCapacityTemplateConfig   `json:"capacity"`
}

type workerDeviceSetTemplateConfig struct {
	ID      string                       `json:"id"`
	Devices []workerDeviceTemplateConfig `json:"devices"`
}

type workerDeviceTemplateConfig struct {
	ID            string `json:"id"`
	ComputeNodeID string `json:"compute_node_id"`
	NodeIdentity  string `json:"node_identity"`
	Region        string `json:"region"`
	NetworkDomain string `json:"network_domain"`
	FaultDomain   string `json:"fault_domain"`
	Kind          string `json:"kind"`
	GPUUUID       string `json:"gpu_uuid"`
	PCIBDF        string `json:"pci_bdf"`
	Ordinal       int    `json:"ordinal"`
}

type workerMemberTemplateConfig struct {
	ID             string   `json:"id"`
	MemberKey      string   `json:"member_key"`
	ComputeNodeID  string   `json:"compute_node_id"`
	WorkerBundleID *string  `json:"worker_bundle_id,omitempty"`
	MemberEpoch    int64    `json:"member_epoch"`
	DeviceIDs      []string `json:"device_ids"`
	Readiness      string   `json:"readiness"`
}

type modelResidencyTemplateConfig struct {
	ID                     string `json:"id"`
	ModelComponentRevision string `json:"model_component_revision"`
	RuntimeIdentity        string `json:"runtime_identity"`
	RuntimeImageDigest     string `json:"runtime_image_digest"`
	ModelRuntimeEpoch      int64  `json:"model_runtime_epoch"`
	State                  string `json:"state"`
	WarmupEvidenceDigest   string `json:"warmup_evidence_digest"`
	CanaryEvidenceDigest   string `json:"canary_evidence_digest"`
}

type workerCapacityTemplateConfig struct {
	Vector map[string]int64 `json:"vector"`
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
	localIdentity := nodeagent.NodeAgentIdentity{
		NodeIdentity: configuration.nodeIdentity,
		WorkerID:     configuration.workerID,
		WorkerEpoch:  configuration.workerEpoch,
	}
	templates, err := loadWorkerInstanceTemplates(configuration.workerInstancesFile, localIdentity)
	if err != nil {
		return err
	}
	epochStore, err := nodeagent.NewFileWorkerInstanceEpochStore(
		nodeagent.FileWorkerInstanceEpochStoreConfig{
			Directory:    configuration.workerInstanceStateDirectory,
			NodeIdentity: configuration.nodeIdentity, BootIDPath: configuration.bootIDPath,
		},
	)
	if err != nil {
		return fmt.Errorf("configure WorkerInstance epoch state: %w", err)
	}
	defer func() { _ = epochStore.Close() }()
	gpuProbe, err := nodeagent.NewNVIDIAGPUProbe(
		nodeagent.NVIDIAGPUProbeConfig{
			NodeIdentity: configuration.nodeIdentity, NVIDIASMIPath: configuration.nvidiaSMIPath,
			PCIBusDevicesRoot: configuration.pciBusDevicesRoot,
			SysDevicesRoot:    configuration.sysDevicesRoot, DriverVersionPath: configuration.nvidiaDriverVersionPath,
		},
		nodeagent.ExecNVIDIAInventoryRunner{},
		epochStore,
	)
	if err != nil {
		return fmt.Errorf("configure NVIDIA WorkerInstance evidence probe: %w", err)
	}
	fleetCredentials, err := fleettransport.NewClientTLSCredentials(
		configuration.fleetClientCertificate,
		configuration.fleetClientPrivateKey,
		configuration.fleetCA,
		configuration.fleetServerName,
	)
	if err != nil {
		return fmt.Errorf("configure Fleet observation transport: %w", err)
	}
	dialContext, cancelDial := context.WithTimeout(context.Background(), configuration.fleetDialTimeout)
	fleetClient, err := fleettransport.DialClient(
		dialContext,
		configuration.fleetAddress,
		fleetCredentials,
	)
	cancelDial()
	if err != nil {
		return fmt.Errorf("connect WorkerInstance reporter to Fleet: %w", err)
	}
	defer func() { _ = fleetClient.Close() }()
	evidenceReporter, err := nodeagent.NewWorkerInstanceEvidenceReporter(
		gpuProbe,
		fleetClient,
		epochStore,
		configuration.workerInstanceEvidenceTTL,
		time.Now,
	)
	if err != nil {
		return fmt.Errorf("configure WorkerInstance evidence reporter: %w", err)
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
		localIdentity,
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
		localIdentity,
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
	return serveGRPCUntilCanceled(
		ctx,
		[]grpcEndpoint{
			{name: "Node Agent", server: grpcServer, listener: listener},
			{name: "Worker host quota", server: quotaGRPCServer, listener: quotaListener},
		},
		nodeAgentProcess{
			name: "WorkerInstance evidence reporting",
			run: func(ctx context.Context) error {
				return nodeagent.RunWorkerInstanceEvidenceReporting(
					ctx,
					evidenceReporter,
					templates,
					nodeagent.WorkerInstanceReportingConfig{
						Interval:       configuration.workerInstanceReportInterval,
						CallTimeout:    configuration.workerInstanceCallTimeout,
						InitialBackoff: configuration.workerInstanceBackoffInitial,
						MaxBackoff:     configuration.workerInstanceBackoffMax,
						ObserveResult: func(result nodeagent.WorkerInstanceReportResult) {
							if result.Err != nil {
								fmt.Fprintf(
									os.Stderr,
									"WorkerInstance %s evidence report failed: %v\n",
									result.WorkerInstanceID,
									result.Err,
								)
							}
						},
					},
				)
			},
		},
	)
}

type grpcEndpoint struct {
	name     string
	server   *grpc.Server
	listener net.Listener
}

type nodeAgentProcess struct {
	name string
	run  func(context.Context) error
}

var probeWorkerXFSProjectQuota = workerrecovery.ProbeXFSProjectQuota

func serveGRPCUntilCanceled(
	ctx context.Context,
	endpoints []grpcEndpoint,
	processes ...nodeAgentProcess,
) error {
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
	for _, process := range processes {
		if process.name == "" || process.run == nil {
			return errors.New("node-agent background process configuration is incomplete")
		}
	}
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() {
		for _, endpoint := range endpoints {
			_ = endpoint.listener.Close()
		}
	}()
	serveErr := make(chan serveResult, len(endpoints)+len(processes))
	var running sync.WaitGroup
	running.Add(len(endpoints) + len(processes))
	for _, endpoint := range endpoints {
		go func(endpoint grpcEndpoint) {
			defer running.Done()
			serveErr <- serveResult{name: endpoint.name, err: endpoint.server.Serve(endpoint.listener)}
		}(endpoint)
	}
	for _, process := range processes {
		go func(process nodeAgentProcess) {
			defer running.Done()
			serveErr <- serveResult{name: process.name, err: process.run(runContext)}
		}(process)
	}
	finished := make(chan struct{})
	go func() {
		running.Wait()
		close(finished)
	}()
	select {
	case <-ctx.Done():
		cancel()
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
		select {
		case <-finished:
			return nil
		case <-time.After(20 * time.Second):
			for _, endpoint := range endpoints {
				endpoint.server.Stop()
			}
			return errors.New("node-agent processes did not stop before shutdown deadline")
		}
	case result := <-serveErr:
		cancel()
		for _, endpoint := range endpoints {
			endpoint.server.Stop()
		}
		select {
		case <-finished:
		case <-time.After(20 * time.Second):
			return fmt.Errorf("%s stopped and node-agent processes did not terminate", result.name)
		}
		if ctx.Err() != nil {
			return nil
		}
		if result.err == nil {
			return fmt.Errorf("%s stopped unexpectedly", result.name)
		}
		return fmt.Errorf("run node-agent process %s: %w", result.name, result.err)
	}
}

func loadConfig() (config, error) {
	workerID, err := uuid.Parse(os.Getenv("VELA_NODE_AGENT_WORKER_ID"))
	if err != nil || workerID == uuid.Nil {
		return config{}, errors.New("VELA_NODE_AGENT_WORKER_ID must be a UUID")
	}
	configuration := config{
		address:           envOrDefault("VELA_NODE_AGENT_ADDRESS", defaultAddress),
		nodeIdentity:      os.Getenv("VELA_NODE_AGENT_NODE_IDENTITY"),
		workerID:          workerID,
		serverCertificate: os.Getenv("VELA_NODE_AGENT_TLS_CERT_FILE"),
		serverPrivateKey:  os.Getenv("VELA_NODE_AGENT_TLS_KEY_FILE"),
		controllerCA:      os.Getenv("VELA_NODE_AGENT_CONTROLLER_CA_FILE"),
		receiptDirectory:  os.Getenv("VELA_NODE_AGENT_RECEIPT_DIRECTORY"),
		controllersFile:   os.Getenv("VELA_NODE_AGENT_CONTROLLERS_FILE"),
		commandsFile:      os.Getenv("VELA_NODE_AGENT_COMMANDS_FILE"),
		capabilitiesFile:  os.Getenv("VELA_NODE_AGENT_CAPABILITIES_FILE"),
		postcheckPath:     os.Getenv("VELA_NODE_AGENT_POSTCHECK_PATH"),
		fencePath:         os.Getenv("VELA_NODE_AGENT_FENCE_PATH"),
		workerQuotaSocket: os.Getenv("VELA_NODE_AGENT_WORKER_QUOTA_SOCKET"),
		workerScratchRoot: os.Getenv("VELA_NODE_AGENT_WORKER_SCRATCH_ROOT"),
		workerXFSDevice:   os.Getenv("VELA_NODE_AGENT_WORKER_XFS_DEVICE"),
		fleetAddress:      os.Getenv("VELA_NODE_AGENT_FLEET_ADDRESS"),
		fleetServerName:   os.Getenv("VELA_NODE_AGENT_FLEET_SERVER_NAME"),
		fleetCA:           os.Getenv("VELA_NODE_AGENT_FLEET_CA_FILE"),
		fleetClientCertificate: os.Getenv(
			"VELA_NODE_AGENT_FLEET_CLIENT_CERT_FILE",
		),
		fleetClientPrivateKey:        os.Getenv("VELA_NODE_AGENT_FLEET_CLIENT_KEY_FILE"),
		workerInstancesFile:          os.Getenv("VELA_NODE_AGENT_WORKER_INSTANCES_FILE"),
		workerInstanceStateDirectory: os.Getenv("VELA_NODE_AGENT_WORKER_INSTANCE_STATE_DIRECTORY"),
		nvidiaSMIPath:                os.Getenv("VELA_NODE_AGENT_NVIDIA_SMI_PATH"),
		pciBusDevicesRoot:            os.Getenv("VELA_NODE_AGENT_PCI_BUS_DEVICES_ROOT"),
		sysDevicesRoot:               os.Getenv("VELA_NODE_AGENT_SYS_DEVICES_ROOT"),
		nvidiaDriverVersionPath:      os.Getenv("VELA_NODE_AGENT_NVIDIA_DRIVER_VERSION_PATH"),
		bootIDPath:                   os.Getenv("VELA_NODE_AGENT_BOOT_ID_PATH"),
		rateMinimumInterval:          defaultRateInterval,
		rateWindow:                   defaultRateWindow,
		rateMax:                      defaultRateMax,
		workerInstanceReportInterval: defaultWorkerInstanceReportInterval,
		workerInstanceCallTimeout:    defaultWorkerInstanceCallTimeout,
		workerInstanceBackoffInitial: defaultWorkerInstanceBackoffInitial,
		workerInstanceBackoffMax:     defaultWorkerInstanceBackoffMax,
		workerInstanceEvidenceTTL:    defaultWorkerInstanceEvidenceTTL,
		fleetDialTimeout:             defaultFleetDialTimeout,
	}
	configuration.workerEpoch, err = positiveInt64Env("VELA_NODE_AGENT_WORKER_EPOCH")
	if err != nil {
		return config{}, err
	}
	for name, value := range map[string]string{
		"VELA_NODE_AGENT_NODE_IDENTITY":                   configuration.nodeIdentity,
		"VELA_NODE_AGENT_TLS_CERT_FILE":                   configuration.serverCertificate,
		"VELA_NODE_AGENT_TLS_KEY_FILE":                    configuration.serverPrivateKey,
		"VELA_NODE_AGENT_CONTROLLER_CA_FILE":              configuration.controllerCA,
		"VELA_NODE_AGENT_RECEIPT_DIRECTORY":               configuration.receiptDirectory,
		"VELA_NODE_AGENT_CONTROLLERS_FILE":                configuration.controllersFile,
		"VELA_NODE_AGENT_COMMANDS_FILE":                   configuration.commandsFile,
		"VELA_NODE_AGENT_CAPABILITIES_FILE":               configuration.capabilitiesFile,
		"VELA_NODE_AGENT_POSTCHECK_PATH":                  configuration.postcheckPath,
		"VELA_NODE_AGENT_FENCE_PATH":                      configuration.fencePath,
		"VELA_NODE_AGENT_WORKER_QUOTA_SOCKET":             configuration.workerQuotaSocket,
		"VELA_NODE_AGENT_WORKER_SCRATCH_ROOT":             configuration.workerScratchRoot,
		"VELA_NODE_AGENT_WORKER_XFS_DEVICE":               configuration.workerXFSDevice,
		"VELA_NODE_AGENT_FLEET_ADDRESS":                   configuration.fleetAddress,
		"VELA_NODE_AGENT_FLEET_SERVER_NAME":               configuration.fleetServerName,
		"VELA_NODE_AGENT_FLEET_CA_FILE":                   configuration.fleetCA,
		"VELA_NODE_AGENT_FLEET_CLIENT_CERT_FILE":          configuration.fleetClientCertificate,
		"VELA_NODE_AGENT_FLEET_CLIENT_KEY_FILE":           configuration.fleetClientPrivateKey,
		"VELA_NODE_AGENT_WORKER_INSTANCES_FILE":           configuration.workerInstancesFile,
		"VELA_NODE_AGENT_WORKER_INSTANCE_STATE_DIRECTORY": configuration.workerInstanceStateDirectory,
		"VELA_NODE_AGENT_NVIDIA_SMI_PATH":                 configuration.nvidiaSMIPath,
		"VELA_NODE_AGENT_PCI_BUS_DEVICES_ROOT":            configuration.pciBusDevicesRoot,
		"VELA_NODE_AGENT_SYS_DEVICES_ROOT":                configuration.sysDevicesRoot,
		"VELA_NODE_AGENT_NVIDIA_DRIVER_VERSION_PATH":      configuration.nvidiaDriverVersionPath,
		"VELA_NODE_AGENT_BOOT_ID_PATH":                    configuration.bootIDPath,
	} {
		if value == "" {
			return config{}, fmt.Errorf("%s is required", name)
		}
	}
	for name, value := range map[string]string{
		"VELA_NODE_AGENT_TLS_CERT_FILE":                   configuration.serverCertificate,
		"VELA_NODE_AGENT_TLS_KEY_FILE":                    configuration.serverPrivateKey,
		"VELA_NODE_AGENT_CONTROLLER_CA_FILE":              configuration.controllerCA,
		"VELA_NODE_AGENT_RECEIPT_DIRECTORY":               configuration.receiptDirectory,
		"VELA_NODE_AGENT_CONTROLLERS_FILE":                configuration.controllersFile,
		"VELA_NODE_AGENT_COMMANDS_FILE":                   configuration.commandsFile,
		"VELA_NODE_AGENT_CAPABILITIES_FILE":               configuration.capabilitiesFile,
		"VELA_NODE_AGENT_WORKER_QUOTA_SOCKET":             configuration.workerQuotaSocket,
		"VELA_NODE_AGENT_WORKER_SCRATCH_ROOT":             configuration.workerScratchRoot,
		"VELA_NODE_AGENT_WORKER_XFS_DEVICE":               configuration.workerXFSDevice,
		"VELA_NODE_AGENT_FLEET_CA_FILE":                   configuration.fleetCA,
		"VELA_NODE_AGENT_FLEET_CLIENT_CERT_FILE":          configuration.fleetClientCertificate,
		"VELA_NODE_AGENT_FLEET_CLIENT_KEY_FILE":           configuration.fleetClientPrivateKey,
		"VELA_NODE_AGENT_WORKER_INSTANCES_FILE":           configuration.workerInstancesFile,
		"VELA_NODE_AGENT_WORKER_INSTANCE_STATE_DIRECTORY": configuration.workerInstanceStateDirectory,
		"VELA_NODE_AGENT_NVIDIA_SMI_PATH":                 configuration.nvidiaSMIPath,
		"VELA_NODE_AGENT_PCI_BUS_DEVICES_ROOT":            configuration.pciBusDevicesRoot,
		"VELA_NODE_AGENT_SYS_DEVICES_ROOT":                configuration.sysDevicesRoot,
		"VELA_NODE_AGENT_NVIDIA_DRIVER_VERSION_PATH":      configuration.nvidiaDriverVersionPath,
		"VELA_NODE_AGENT_BOOT_ID_PATH":                    configuration.bootIDPath,
	} {
		if cleaned := filepath.Clean(value); !filepath.IsAbs(cleaned) || cleaned != value {
			return config{}, fmt.Errorf("%s must be an absolute clean path", name)
		}
	}
	port, err := portFromAddress(configuration.address)
	if err != nil {
		return config{}, err
	}
	_ = port
	if _, err := portFromEndpointAddress(configuration.fleetAddress); err != nil {
		return config{}, err
	}
	if !validWorkerInstanceText(configuration.fleetServerName, 253) {
		return config{}, errors.New("VELA_NODE_AGENT_FLEET_SERVER_NAME is invalid")
	}
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
	for name, target := range map[string]*time.Duration{
		"VELA_NODE_AGENT_WORKER_INSTANCE_REPORT_INTERVAL": &configuration.workerInstanceReportInterval,
		"VELA_NODE_AGENT_WORKER_INSTANCE_CALL_TIMEOUT":    &configuration.workerInstanceCallTimeout,
		"VELA_NODE_AGENT_WORKER_INSTANCE_BACKOFF_INITIAL": &configuration.workerInstanceBackoffInitial,
		"VELA_NODE_AGENT_WORKER_INSTANCE_BACKOFF_MAX":     &configuration.workerInstanceBackoffMax,
		"VELA_NODE_AGENT_WORKER_INSTANCE_EVIDENCE_TTL":    &configuration.workerInstanceEvidenceTTL,
		"VELA_NODE_AGENT_FLEET_DIAL_TIMEOUT":              &configuration.fleetDialTimeout,
	} {
		if value := os.Getenv(name); value != "" {
			parsed, err := time.ParseDuration(value)
			if err != nil || parsed <= 0 || parsed%time.Millisecond != 0 {
				return config{}, fmt.Errorf("%s is invalid", name)
			}
			*target = parsed
		}
	}
	if configuration.workerInstanceReportInterval < 10*time.Second ||
		configuration.workerInstanceReportInterval > 10*time.Minute ||
		configuration.workerInstanceCallTimeout < time.Second ||
		configuration.workerInstanceCallTimeout > time.Minute ||
		configuration.workerInstanceBackoffInitial < 100*time.Millisecond ||
		configuration.workerInstanceBackoffMax < configuration.workerInstanceBackoffInitial ||
		configuration.workerInstanceBackoffMax > 10*time.Minute ||
		configuration.workerInstanceEvidenceTTL < 10*time.Second ||
		configuration.workerInstanceEvidenceTTL > 10*time.Minute ||
		configuration.workerInstanceEvidenceTTL <= configuration.workerInstanceReportInterval+
			configuration.workerInstanceCallTimeout ||
		configuration.fleetDialTimeout < time.Second || configuration.fleetDialTimeout > 2*time.Minute {
		return config{}, errors.New("WorkerInstance outbound reporting durations are invalid")
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

func loadWorkerInstanceTemplates(
	path string,
	identity nodeagent.NodeAgentIdentity,
) ([]nodeagent.WorkerInstanceEvidenceTemplate, error) {
	observedBy := nodeagent.NodeAgentSPIFFEIdentity(identity)
	if observedBy == "" {
		return nil, errors.New("Node Agent WorkerInstance observation identity is invalid")
	}
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned != path {
		return nil, errors.New("WorkerInstance template file path must be absolute and clean")
	}
	content, err := securefile.Read(cleaned, maxConfigFileBytes, false)
	if err != nil {
		return nil, fmt.Errorf("read WorkerInstance templates: %w", err)
	}
	if err := strictjson.RejectDuplicateKeys(content); err != nil {
		return nil, fmt.Errorf("decode WorkerInstance templates: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var configured []workerInstanceTemplateConfig
	if err := decoder.Decode(&configured); err != nil {
		return nil, errors.New("WorkerInstance template file is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) ||
		len(configured) == 0 || len(configured) > maxWorkerInstances {
		return nil, errors.New("WorkerInstance template file is invalid")
	}
	templates := make([]nodeagent.WorkerInstanceEvidenceTemplate, 0, len(configured))
	seenWorkers := make(map[uuid.UUID]struct{}, len(configured))
	seenDeviceSets := make(map[uuid.UUID]struct{}, len(configured))
	seenDevices := make(map[uuid.UUID]struct{}, len(configured))
	seenGPUUUIDs := make(map[string]struct{}, len(configured))
	seenPCIBDFs := make(map[string]struct{}, len(configured))
	seenResidencies := make(map[uuid.UUID]struct{}, len(configured))
	for _, item := range configured {
		template, err := workerInstanceTemplate(item, identity.NodeIdentity, observedBy)
		if err != nil {
			return nil, err
		}
		workerInstanceID := template.Evidence.WorkerInstanceID
		if _, duplicate := seenWorkers[workerInstanceID]; duplicate {
			return nil, errors.New("WorkerInstance template identity is duplicated")
		}
		seenWorkers[workerInstanceID] = struct{}{}
		deviceSetID := template.Evidence.DeviceSet.ID
		if _, duplicate := seenDeviceSets[deviceSetID]; duplicate {
			return nil, errors.New("WorkerInstance DeviceSet identity is duplicated")
		}
		seenDeviceSets[deviceSetID] = struct{}{}
		for _, device := range template.Evidence.DeviceSet.Devices {
			if _, duplicate := seenDevices[device.ID]; duplicate {
				return nil, errors.New("WorkerInstance Device identity is duplicated")
			}
			if _, duplicate := seenGPUUUIDs[device.GPUUUID]; duplicate {
				return nil, errors.New("WorkerInstance GPU UUID is duplicated")
			}
			if _, duplicate := seenPCIBDFs[device.PCIBDF]; duplicate {
				return nil, errors.New("WorkerInstance PCI BDF is duplicated")
			}
			seenDevices[device.ID] = struct{}{}
			seenGPUUUIDs[device.GPUUUID] = struct{}{}
			seenPCIBDFs[device.PCIBDF] = struct{}{}
		}
		for _, residency := range template.Evidence.Residencies {
			if _, duplicate := seenResidencies[residency.ID]; duplicate {
				return nil, errors.New("WorkerInstance ModelResidency identity is duplicated")
			}
			seenResidencies[residency.ID] = struct{}{}
		}
		templates = append(templates, template)
	}
	return templates, nil
}

func workerInstanceTemplate(
	configured workerInstanceTemplateConfig,
	nodeIdentity string,
	observedBy string,
) (nodeagent.WorkerInstanceEvidenceTemplate, error) {
	workerInstanceID, workerErr := canonicalWorkerInstanceUUID(configured.WorkerInstanceID)
	deviceSetID, deviceSetErr := canonicalWorkerInstanceUUID(configured.DeviceSet.ID)
	if workerErr != nil || deviceSetErr != nil || configured.SchemaVersion != 1 ||
		configured.InstanceEpoch <= 0 || configured.ControlSessionEpoch <= 0 ||
		len(configured.DeviceSet.Devices) == 0 || len(configured.DeviceSet.Devices) > maxWorkerInstances ||
		len(configured.Members) == 0 || len(configured.Members) > maxWorkerInstances ||
		len(configured.Residencies) == 0 || len(configured.Residencies) > maxWorkerInstances ||
		len(configured.Capacity.Vector) == 0 {
		return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance template is invalid")
	}

	devices := make([]fleet.WorkerDeviceEvidence, 0, len(configured.DeviceSet.Devices))
	deviceIDs := make(map[uuid.UUID]struct{}, len(configured.DeviceSet.Devices))
	gpuUUIDs := make(map[string]struct{}, len(configured.DeviceSet.Devices))
	pciBDFs := make(map[string]struct{}, len(configured.DeviceSet.Devices))
	ordinals := make(map[int]struct{}, len(configured.DeviceSet.Devices))
	computeNodeID := uuid.Nil
	for _, item := range configured.DeviceSet.Devices {
		deviceID, deviceErr := canonicalWorkerInstanceUUID(item.ID)
		nodeID, nodeErr := canonicalWorkerInstanceUUID(item.ComputeNodeID)
		if deviceErr != nil || nodeErr != nil || item.NodeIdentity != nodeIdentity ||
			!validWorkerInstanceText(item.Region, 200) ||
			!validWorkerInstanceText(item.NetworkDomain, 200) ||
			!validWorkerInstanceText(item.FaultDomain, 200) || item.Kind != "GPU" ||
			!workerInstanceGPUUUIDPattern.MatchString(item.GPUUUID) ||
			!workerInstancePCIBDFPattern.MatchString(item.PCIBDF) || item.Ordinal < 0 ||
			computeNodeID != uuid.Nil && nodeID != computeNodeID {
			return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance Device template is invalid")
		}
		if _, duplicate := deviceIDs[deviceID]; duplicate {
			return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance Device identity is duplicated")
		}
		if _, duplicate := gpuUUIDs[item.GPUUUID]; duplicate {
			return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance GPU UUID is duplicated")
		}
		if _, duplicate := pciBDFs[item.PCIBDF]; duplicate {
			return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance PCI BDF is duplicated")
		}
		if _, duplicate := ordinals[item.Ordinal]; duplicate {
			return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance Device ordinal is duplicated")
		}
		computeNodeID = nodeID
		deviceIDs[deviceID] = struct{}{}
		gpuUUIDs[item.GPUUUID] = struct{}{}
		pciBDFs[item.PCIBDF] = struct{}{}
		ordinals[item.Ordinal] = struct{}{}
		devices = append(devices, fleet.WorkerDeviceEvidence{
			ID: deviceID, ComputeNodeID: nodeID, NodeIdentity: item.NodeIdentity,
			Region: item.Region, NetworkDomain: item.NetworkDomain, FaultDomain: item.FaultDomain,
			Kind: item.Kind, GPUUUID: item.GPUUUID, PCIBDF: item.PCIBDF, Ordinal: item.Ordinal,
		})
	}
	for ordinal := range len(devices) {
		if _, exists := ordinals[ordinal]; !exists {
			return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance Device ordinals are not contiguous")
		}
	}

	members := make([]fleet.WorkerMemberEvidence, 0, len(configured.Members))
	memberIDs := make(map[uuid.UUID]struct{}, len(configured.Members))
	memberKeys := make(map[string]struct{}, len(configured.Members))
	coveredDevices := make(map[uuid.UUID]struct{}, len(deviceIDs))
	for _, item := range configured.Members {
		memberID, memberErr := canonicalWorkerInstanceUUID(item.ID)
		nodeID, nodeErr := canonicalWorkerInstanceUUID(item.ComputeNodeID)
		if memberErr != nil || nodeErr != nil || nodeID != computeNodeID || item.MemberEpoch <= 0 ||
			!workerInstanceKeyPattern.MatchString(item.MemberKey) || len(item.MemberKey) > 100 ||
			len(item.DeviceIDs) == 0 || item.Readiness != "READY" {
			return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance member template is invalid")
		}
		if _, duplicate := memberIDs[memberID]; duplicate {
			return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance member identity is duplicated")
		}
		if _, duplicate := memberKeys[item.MemberKey]; duplicate {
			return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance member key is duplicated")
		}
		var workerBundleID *uuid.UUID
		if item.WorkerBundleID != nil {
			parsed, err := canonicalWorkerInstanceUUID(*item.WorkerBundleID)
			if err != nil {
				return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance member bundle identity is invalid")
			}
			workerBundleID = &parsed
		}
		memberDeviceIDs := make([]uuid.UUID, 0, len(item.DeviceIDs))
		for _, encodedDeviceID := range item.DeviceIDs {
			deviceID, err := canonicalWorkerInstanceUUID(encodedDeviceID)
			if err != nil {
				return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance member Device identity is invalid")
			}
			if _, exists := deviceIDs[deviceID]; !exists {
				return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance member references an unknown Device")
			}
			if _, duplicate := coveredDevices[deviceID]; duplicate {
				return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance Device ownership is duplicated")
			}
			coveredDevices[deviceID] = struct{}{}
			memberDeviceIDs = append(memberDeviceIDs, deviceID)
		}
		memberIDs[memberID] = struct{}{}
		memberKeys[item.MemberKey] = struct{}{}
		members = append(members, fleet.WorkerMemberEvidence{
			ID: memberID, MemberKey: item.MemberKey, ComputeNodeID: nodeID,
			WorkerBundleID: workerBundleID, MemberEpoch: item.MemberEpoch,
			DeviceIDs: memberDeviceIDs, Readiness: item.Readiness,
		})
	}
	if len(coveredDevices) != len(deviceIDs) {
		return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance member Device coverage is incomplete")
	}

	residencies := make([]fleet.ModelResidencyEvidence, 0, len(configured.Residencies))
	residencyIDs := make(map[uuid.UUID]struct{}, len(configured.Residencies))
	residentComponents := make(map[string]struct{}, len(configured.Residencies))
	for _, item := range configured.Residencies {
		residencyID, residencyErr := canonicalWorkerInstanceUUID(item.ID)
		if residencyErr != nil || !validWorkerInstanceText(item.ModelComponentRevision, 300) ||
			!validWorkerInstanceText(item.RuntimeIdentity, 500) ||
			!workerInstanceImagePattern.MatchString(item.RuntimeImageDigest) ||
			item.ModelRuntimeEpoch <= 0 || item.State != "READY" ||
			!workerInstanceDigestPattern.MatchString(item.WarmupEvidenceDigest) ||
			!workerInstanceDigestPattern.MatchString(item.CanaryEvidenceDigest) {
			return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance ModelResidency template is invalid")
		}
		if _, duplicate := residencyIDs[residencyID]; duplicate {
			return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance ModelResidency identity is duplicated")
		}
		if _, duplicate := residentComponents[item.ModelComponentRevision]; duplicate {
			return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance resident model component is duplicated")
		}
		residencyIDs[residencyID] = struct{}{}
		residentComponents[item.ModelComponentRevision] = struct{}{}
		residencies = append(residencies, fleet.ModelResidencyEvidence{
			ID: residencyID, ModelComponentRevision: item.ModelComponentRevision,
			RuntimeIdentity: item.RuntimeIdentity, RuntimeImageDigest: item.RuntimeImageDigest,
			ModelRuntimeEpoch: item.ModelRuntimeEpoch, State: item.State,
			WarmupEvidenceDigest: item.WarmupEvidenceDigest,
			CanaryEvidenceDigest: item.CanaryEvidenceDigest,
		})
	}

	capacity := make(map[string]int64, len(configured.Capacity.Vector))
	for resource, quantity := range configured.Capacity.Vector {
		if !validWorkerInstanceText(resource, 100) || quantity < 0 {
			return nodeagent.WorkerInstanceEvidenceTemplate{}, errors.New("WorkerInstance capacity template is invalid")
		}
		capacity[resource] = quantity
	}
	return nodeagent.WorkerInstanceEvidenceTemplate{
		Evidence: fleet.WorkerInstanceEvidence{
			SchemaVersion: configured.SchemaVersion, WorkerInstanceID: workerInstanceID,
			InstanceEpoch: configured.InstanceEpoch, ControlSessionEpoch: configured.ControlSessionEpoch,
			DeviceSet: fleet.WorkerDeviceSetEvidence{ID: deviceSetID, Devices: devices},
			Members:   members, Residencies: residencies,
			Capacity: fleet.WorkerCapacityEvidence{Vector: capacity},
		},
		ObservedBy: observedBy,
	}, nil
}

func canonicalWorkerInstanceUUID(value string) (uuid.UUID, error) {
	parsed, err := uuid.Parse(value)
	if err != nil || parsed == uuid.Nil || parsed.String() != value {
		return uuid.Nil, errors.New("UUID is not canonical")
	}
	return parsed, nil
}

func validWorkerInstanceText(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, '\x00')
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

func portFromEndpointAddress(address string) (int, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(host) != host {
		return 0, errors.New("VELA_NODE_AGENT_FLEET_ADDRESS must contain a host and port")
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return 0, errors.New("VELA_NODE_AGENT_FLEET_ADDRESS port is invalid")
	}
	return port, nil
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
