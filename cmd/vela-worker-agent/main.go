package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/runnertransport"
	"github.com/vivym/vela/internal/workeragent"
	"github.com/vivym/vela/internal/workerhost"
	"github.com/vivym/vela/internal/workerrecovery"
	"github.com/vivym/vela/internal/workertransport"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
)

const (
	scratchProbeXFSProjectQuota   = "xfs-project-quota"
	scratchProbeStatFSDevelopment = "statfs-dev"
	defaultHeartbeatInterval      = 20 * time.Second
	defaultCapacityReportInterval = 30 * time.Second
	defaultPollInterval           = time.Second
	defaultBackoffMinimum         = time.Second
	defaultBackoffMaximum         = 30 * time.Second
	defaultArtifactUploadTimeout  = 5 * time.Minute
	defaultArtifactProbeTimeout   = 2 * time.Second
	defaultWorkerHostQuotaTimeout = 5 * time.Second
)

type config struct {
	workerID                       uuid.UUID
	workerEpoch                    int64
	nodeIdentity                   string
	controlAddress                 string
	controlServerName              string
	tlsCertificateFile             string
	tlsPrivateKeyFile              string
	controlCAFile                  string
	runnerSocket                   string
	runnerExpectedUID              uint32
	scratchRoot                    string
	recoveryRoot                   string
	outputRoot                     string
	outputOwnerUID                 uint32
	outputCleanupMinBytesPerSecond int64
	inferenceBackendRevision       string
	scratchProbe                   string
	xfsDevice                      string
	xfsProjectID                   uint32
	workerHostQuotaSocket          string
	workerHostExpectedSocketUID    uint32
	workerHostExpectedSocketGID    uint32
	workerHostQuotaTimeout         time.Duration
	attemptQuotaBytes              int64
	maxEntryBytes                  int64
	maxEntries                     int
	highWatermarkBytes             int64
	lowWatermarkBytes              int64
	criticalFreeBytes              int64
	terminalRetention              time.Duration
	heartbeatInterval              time.Duration
	capacityReportInterval         time.Duration
	pollInterval                   time.Duration
	backoffMinimum                 time.Duration
	backoffMaximum                 time.Duration
	artifactUploadTimeout          time.Duration
	artifactStoreHealthURL         string
	artifactStoreCAFile            string
	artifactStoreProbeTimeout      time.Duration
	allowDevelopmentHTTPUploads    bool
}

type agentRunOnce func(context.Context) (workeragent.Result, error)
type waitFunc func(context.Context, time.Duration) error
type retryReporter func(error, time.Duration)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "vela-worker-agent stopped: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	configuration, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runWithContext(ctx, configuration)
}

func runWithContext(ctx context.Context, configuration config) error {
	if ctx == nil {
		return errors.New("worker agent context is required")
	}
	transportCredentials, err := workertransport.NewClientTLSCredentials(
		configuration.tlsCertificateFile,
		configuration.tlsPrivateKeyFile,
		configuration.controlCAFile,
		configuration.controlServerName,
	)
	if err != nil {
		return fmt.Errorf("configure Worker control mTLS: %w", err)
	}
	control, err := workertransport.DialClient(ctx, configuration.controlAddress, transportCredentials)
	if err != nil {
		return err
	}
	defer func() { _ = control.Close() }()
	runner, err := runnertransport.Dial(ctx, runnertransport.Config{
		SocketPath: configuration.runnerSocket, ExpectedUID: configuration.runnerExpectedUID,
	})
	if err != nil {
		return err
	}
	defer func() { _ = runner.Close() }()

	var spaceProbe func(string) (workerrecovery.Space, error)
	if configuration.scratchProbe == scratchProbeXFSProjectQuota {
		hostClient, err := workerhost.DialClient(ctx, workerhost.ClientConfig{
			SocketPath:        configuration.workerHostQuotaSocket,
			ExpectedSocketUID: configuration.workerHostExpectedSocketUID,
			ExpectedSocketGID: configuration.workerHostExpectedSocketGID,
			WorkerID:          configuration.workerID, RootPath: configuration.scratchRoot,
			DevicePath: configuration.xfsDevice, ProjectID: configuration.xfsProjectID,
			Timeout: configuration.workerHostQuotaTimeout,
		})
		if err != nil {
			return fmt.Errorf("configure Worker host quota probe: %w", err)
		}
		defer func() { _ = hostClient.Close() }()
		spaceProbe = func(root string) (workerrecovery.Space, error) {
			if filepath.Clean(root) != filepath.Clean(configuration.recoveryRoot) {
				return workerrecovery.Space{}, errors.New("worker host quota probe received an unexpected recovery root")
			}
			observation, err := hostClient.Observe()
			if err != nil {
				return workerrecovery.Space{}, err
			}
			return workerrecovery.Space{
				TotalBytes: observation.TotalBytes,
				FreeBytes:  observation.FreeBytes,
			}, nil
		}
	}
	recovery, err := workerrecovery.New(workerrecovery.Config{
		Root: configuration.recoveryRoot, WorkerID: configuration.workerID,
		WorkerEpoch: configuration.workerEpoch, AttemptQuotaBytes: configuration.attemptQuotaBytes,
		MaxEntryBytes: configuration.maxEntryBytes, MaxEntries: configuration.maxEntries,
		HighWatermarkBytes: configuration.highWatermarkBytes,
		LowWatermarkBytes:  configuration.lowWatermarkBytes,
		CriticalFreeBytes:  configuration.criticalFreeBytes,
		TerminalRetention:  configuration.terminalRetention,
		SpaceProbe:         spaceProbe,
	})
	if err != nil {
		return fmt.Errorf("configure Worker Local Recovery State: %w", err)
	}
	uploader, err := workeragent.NewHTTPArtifactPartUploader(workeragent.HTTPArtifactPartUploaderConfig{
		AllowHTTP: configuration.allowDevelopmentHTTPUploads,
		Timeout:   configuration.artifactUploadTimeout,
	})
	if err != nil {
		return err
	}
	artifactStoreProbe, err := workeragent.NewHTTPArtifactStoreProbe(
		workeragent.HTTPArtifactStoreProbeConfig{
			URL:       configuration.artifactStoreHealthURL,
			CAFile:    configuration.artifactStoreCAFile,
			Timeout:   configuration.artifactStoreProbeTimeout,
			AllowHTTP: configuration.allowDevelopmentHTTPUploads,
		},
	)
	if err != nil {
		return fmt.Errorf("configure Artifact-store reachability probe: %w", err)
	}
	agent, err := workeragent.New(workeragent.Config{
		WorkerID: configuration.workerID, WorkerEpoch: configuration.workerEpoch,
		NodeIdentity: configuration.nodeIdentity,
		Recovery:     recovery, Control: control, Runner: runner,
		HeartbeatInterval:      configuration.heartbeatInterval,
		CapacityReportInterval: configuration.capacityReportInterval,
		ReportCapacityError: func(cause error) {
			slog.Warn("periodic Worker capacity report failed", "error", cause)
		},
		OutputRoot:                     configuration.outputRoot,
		OutputOwnerUID:                 configuration.outputOwnerUID,
		OutputCleanupMinBytesPerSecond: configuration.outputCleanupMinBytesPerSecond,
		OutputCleanupMaxBytes:          configuration.attemptQuotaBytes,
		OutputCleanupMaxDuration:       configuration.terminalRetention,
		InferenceBackendRevision:       configuration.inferenceBackendRevision,
		ArtifactStoreReachable:         artifactStoreProbe.Reachable,
		Finalization:                   control,
		PartUploader:                   uploader,
		DebugDumps:                     control,
		DebugDumpUploader:              uploader,
		ReportDebugDumpError: func(cause error) {
			slog.Warn("authorized debug dump upload failed", "error", cause)
		},
	})
	if err != nil {
		return fmt.Errorf("configure Worker Agent: %w", err)
	}
	return runAgentLoop(
		ctx,
		agent.RunOnce,
		waitContext,
		configuration.backoffMinimum,
		configuration.backoffMaximum,
		configuration.pollInterval,
		func(cause error, retryAfter time.Duration) {
			slog.Error("worker agent iteration failed", "error", cause, "retry_after", retryAfter)
		},
	)
}

func runAgentLoop(
	ctx context.Context,
	runOnce agentRunOnce,
	wait waitFunc,
	backoffMinimum time.Duration,
	backoffMaximum time.Duration,
	pollInterval time.Duration,
	report retryReporter,
) error {
	if ctx == nil || runOnce == nil || wait == nil || backoffMinimum <= 0 ||
		backoffMaximum < backoffMinimum || pollInterval <= 0 {
		return errors.New("worker agent loop configuration is invalid")
	}
	backoff := backoffMinimum
	for {
		if ctx.Err() != nil {
			return nil
		}
		_, err := runOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		delay := pollInterval
		if err != nil {
			delay = backoff
			if report != nil {
				report(err, delay)
			}
			if backoff > backoffMaximum/2 {
				backoff = backoffMaximum
			} else {
				backoff *= 2
			}
			if backoff > backoffMaximum {
				backoff = backoffMaximum
			}
		} else {
			backoff = backoffMinimum
		}
		if err := wait(ctx, delay); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func loadConfig() (config, error) {
	workerID, err := uuid.Parse(os.Getenv("VELA_WORKER_ID"))
	if err != nil || workerID == uuid.Nil {
		return config{}, errors.New("VELA_WORKER_ID must be a UUID")
	}
	workerEpoch, err := positiveInt64Env("VELA_WORKER_EPOCH")
	if err != nil {
		return config{}, err
	}
	runnerExpectedUID, err := uint32Env("VELA_WORKER_RUNNER_EXPECTED_UID", false)
	if err != nil {
		return config{}, err
	}
	outputOwnerUID, err := uint32Env("VELA_WORKER_OUTPUT_OWNER_UID", false)
	if err != nil {
		return config{}, err
	}
	outputCleanupMinBytesPerSecond, err := positiveInt64Env(
		"VELA_WORKER_OUTPUT_CLEANUP_MIN_BYTES_PER_SECOND",
	)
	if err != nil {
		return config{}, err
	}
	configuration := config{
		workerID: workerID, workerEpoch: workerEpoch,
		nodeIdentity:                   os.Getenv("VELA_WORKER_NODE_IDENTITY"),
		controlAddress:                 os.Getenv("VELA_WORKER_CONTROL_ADDRESS"),
		controlServerName:              os.Getenv("VELA_WORKER_CONTROL_SERVER_NAME"),
		tlsCertificateFile:             os.Getenv("VELA_WORKER_TLS_CERT_FILE"),
		tlsPrivateKeyFile:              os.Getenv("VELA_WORKER_TLS_KEY_FILE"),
		controlCAFile:                  os.Getenv("VELA_WORKER_CONTROL_CA_FILE"),
		runnerSocket:                   os.Getenv("VELA_WORKER_RUNNER_SOCKET"),
		runnerExpectedUID:              runnerExpectedUID,
		scratchRoot:                    os.Getenv("VELA_WORKER_SCRATCH_ROOT"),
		recoveryRoot:                   os.Getenv("VELA_WORKER_RECOVERY_ROOT"),
		outputRoot:                     os.Getenv("VELA_WORKER_OUTPUT_ROOT"),
		outputOwnerUID:                 outputOwnerUID,
		outputCleanupMinBytesPerSecond: outputCleanupMinBytesPerSecond,
		inferenceBackendRevision:       os.Getenv("VELA_WORKER_INFERENCE_BACKEND_REVISION"),
		artifactStoreHealthURL:         os.Getenv("VELA_WORKER_ARTIFACT_STORE_HEALTH_URL"),
		artifactStoreCAFile:            os.Getenv("VELA_WORKER_ARTIFACT_STORE_CA_FILE"),
		scratchProbe:                   os.Getenv("VELA_WORKER_SCRATCH_PROBE"),
		xfsDevice:                      os.Getenv("VELA_WORKER_XFS_DEVICE"),
		workerHostQuotaSocket:          os.Getenv("VELA_WORKER_HOST_QUOTA_SOCKET"),
		heartbeatInterval:              defaultHeartbeatInterval,
		capacityReportInterval:         defaultCapacityReportInterval,
		pollInterval:                   defaultPollInterval,
		backoffMinimum:                 defaultBackoffMinimum,
		backoffMaximum:                 defaultBackoffMaximum,
		artifactUploadTimeout:          defaultArtifactUploadTimeout,
		artifactStoreProbeTimeout:      defaultArtifactProbeTimeout,
		workerHostQuotaTimeout:         defaultWorkerHostQuotaTimeout,
	}
	for name, value := range map[string]string{
		"VELA_WORKER_NODE_IDENTITY":              configuration.nodeIdentity,
		"VELA_WORKER_CONTROL_ADDRESS":            configuration.controlAddress,
		"VELA_WORKER_CONTROL_SERVER_NAME":        configuration.controlServerName,
		"VELA_WORKER_TLS_CERT_FILE":              configuration.tlsCertificateFile,
		"VELA_WORKER_TLS_KEY_FILE":               configuration.tlsPrivateKeyFile,
		"VELA_WORKER_CONTROL_CA_FILE":            configuration.controlCAFile,
		"VELA_WORKER_RUNNER_SOCKET":              configuration.runnerSocket,
		"VELA_WORKER_SCRATCH_ROOT":               configuration.scratchRoot,
		"VELA_WORKER_RECOVERY_ROOT":              configuration.recoveryRoot,
		"VELA_WORKER_OUTPUT_ROOT":                configuration.outputRoot,
		"VELA_WORKER_INFERENCE_BACKEND_REVISION": configuration.inferenceBackendRevision,
		"VELA_WORKER_ARTIFACT_STORE_HEALTH_URL":  configuration.artifactStoreHealthURL,
		"VELA_WORKER_SCRATCH_PROBE":              configuration.scratchProbe,
	} {
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value {
			return config{}, fmt.Errorf("%s is required and must not contain surrounding whitespace", name)
		}
	}
	if problems := k8svalidation.IsDNS1123Subdomain(configuration.nodeIdentity); len(problems) != 0 {
		return config{}, fmt.Errorf(
			"VELA_WORKER_NODE_IDENTITY must be a Kubernetes node name: %s",
			strings.Join(problems, "; "),
		)
	}
	for name, path := range map[string]string{
		"VELA_WORKER_TLS_CERT_FILE":   configuration.tlsCertificateFile,
		"VELA_WORKER_TLS_KEY_FILE":    configuration.tlsPrivateKeyFile,
		"VELA_WORKER_CONTROL_CA_FILE": configuration.controlCAFile,
		"VELA_WORKER_RUNNER_SOCKET":   configuration.runnerSocket,
		"VELA_WORKER_SCRATCH_ROOT":    configuration.scratchRoot,
		"VELA_WORKER_RECOVERY_ROOT":   configuration.recoveryRoot,
		"VELA_WORKER_OUTPUT_ROOT":     configuration.outputRoot,
	} {
		if !filepath.IsAbs(filepath.Clean(path)) {
			return config{}, fmt.Errorf("%s must be an absolute path", name)
		}
	}
	if configuration.artifactStoreCAFile != "" &&
		!filepath.IsAbs(filepath.Clean(configuration.artifactStoreCAFile)) {
		return config{}, errors.New("VELA_WORKER_ARTIFACT_STORE_CA_FILE must be an absolute path")
	}
	if pathsOverlap(configuration.recoveryRoot, configuration.outputRoot) {
		return config{}, errors.New("worker recovery and output roots must not overlap")
	}
	if !pathWithin(configuration.scratchRoot, configuration.recoveryRoot) ||
		!pathWithin(configuration.scratchRoot, configuration.outputRoot) {
		return config{}, errors.New("worker recovery and output roots must be under VELA_WORKER_SCRATCH_ROOT")
	}
	if configuration.scratchProbe != scratchProbeXFSProjectQuota &&
		configuration.scratchProbe != scratchProbeStatFSDevelopment {
		return config{}, errors.New("VELA_WORKER_SCRATCH_PROBE must be xfs-project-quota or statfs-dev")
	}
	if configuration.scratchProbe == scratchProbeXFSProjectQuota {
		if strings.TrimSpace(configuration.workerHostQuotaSocket) == "" ||
			strings.TrimSpace(configuration.workerHostQuotaSocket) != configuration.workerHostQuotaSocket {
			return config{}, errors.New("VELA_WORKER_HOST_QUOTA_SOCKET is required and must not contain surrounding whitespace")
		}
		if !filepath.IsAbs(filepath.Clean(configuration.workerHostQuotaSocket)) {
			return config{}, errors.New("VELA_WORKER_HOST_QUOTA_SOCKET must be an absolute path")
		}
		if !filepath.IsAbs(filepath.Clean(configuration.xfsDevice)) {
			return config{}, errors.New("VELA_WORKER_XFS_DEVICE must be an absolute block-device path")
		}
		configuration.workerHostExpectedSocketUID, err = uint32Env("VELA_WORKER_HOST_QUOTA_SOCKET_UID", false)
		if err != nil {
			return config{}, err
		}
		configuration.workerHostExpectedSocketGID, err = uint32Env("VELA_WORKER_HOST_QUOTA_SOCKET_GID", true)
		if err != nil {
			return config{}, err
		}
		configuration.xfsProjectID, err = uint32Env("VELA_WORKER_XFS_PROJECT_ID", true)
		if err != nil {
			return config{}, err
		}
	}
	configuration.attemptQuotaBytes, err = positiveInt64Env("VELA_WORKER_ATTEMPT_QUOTA_BYTES")
	if err != nil {
		return config{}, err
	}
	configuration.maxEntryBytes, err = positiveInt64Env("VELA_WORKER_MAX_ENTRY_BYTES")
	if err != nil {
		return config{}, err
	}
	configuration.maxEntries, err = positiveIntEnv("VELA_WORKER_MAX_ENTRIES")
	if err != nil {
		return config{}, err
	}
	configuration.highWatermarkBytes, err = positiveInt64Env("VELA_WORKER_HIGH_WATERMARK_BYTES")
	if err != nil {
		return config{}, err
	}
	configuration.lowWatermarkBytes, err = nonnegativeInt64Env("VELA_WORKER_LOW_WATERMARK_BYTES")
	if err != nil {
		return config{}, err
	}
	configuration.criticalFreeBytes, err = nonnegativeInt64Env("VELA_WORKER_CRITICAL_FREE_BYTES")
	if err != nil {
		return config{}, err
	}
	configuration.terminalRetention, err = durationEnv("VELA_WORKER_TERMINAL_RETENTION", time.Second, 24*time.Hour)
	if err != nil {
		return config{}, err
	}
	if configuration.maxEntryBytes > configuration.attemptQuotaBytes ||
		configuration.highWatermarkBytes <= configuration.lowWatermarkBytes {
		return config{}, errors.New("worker recovery quota and watermark configuration is inconsistent")
	}
	if err := workeragent.ValidateOutputCleanupBudget(
		configuration.attemptQuotaBytes,
		configuration.outputCleanupMinBytesPerSecond,
		configuration.terminalRetention,
	); err != nil {
		return config{}, fmt.Errorf("worker output cleanup exceeds terminal retention: %w", err)
	}
	for name, target := range map[string]*time.Duration{
		"VELA_WORKER_HEARTBEAT_INTERVAL":           &configuration.heartbeatInterval,
		"VELA_WORKER_CAPACITY_REPORT_INTERVAL":     &configuration.capacityReportInterval,
		"VELA_WORKER_POLL_INTERVAL":                &configuration.pollInterval,
		"VELA_WORKER_BACKOFF_MINIMUM":              &configuration.backoffMinimum,
		"VELA_WORKER_BACKOFF_MAXIMUM":              &configuration.backoffMaximum,
		"VELA_WORKER_ARTIFACT_UPLOAD_TIMEOUT":      &configuration.artifactUploadTimeout,
		"VELA_WORKER_ARTIFACT_STORE_PROBE_TIMEOUT": &configuration.artifactStoreProbeTimeout,
		"VELA_WORKER_HOST_QUOTA_TIMEOUT":           &configuration.workerHostQuotaTimeout,
	} {
		if value := os.Getenv(name); value != "" {
			parsed, parseErr := time.ParseDuration(value)
			if parseErr != nil || parsed <= 0 {
				return config{}, fmt.Errorf("%s must be a positive duration", name)
			}
			*target = parsed
		}
	}
	if configuration.backoffMaximum < configuration.backoffMinimum ||
		configuration.artifactUploadTimeout < time.Second ||
		configuration.artifactUploadTimeout > 10*time.Minute ||
		configuration.workerHostQuotaTimeout < 100*time.Millisecond ||
		configuration.workerHostQuotaTimeout > 30*time.Second {
		return config{}, errors.New("worker agent backoff or upload timeout is invalid")
	}
	if value := os.Getenv("VELA_WORKER_ALLOW_DEVELOPMENT_HTTP_UPLOADS"); value != "" {
		configuration.allowDevelopmentHTTPUploads, err = strconv.ParseBool(value)
		if err != nil || (configuration.allowDevelopmentHTTPUploads &&
			configuration.scratchProbe != scratchProbeStatFSDevelopment) {
			return config{}, errors.New("development HTTP uploads require statfs-dev and a boolean setting")
		}
	}
	healthURL, err := url.Parse(configuration.artifactStoreHealthURL)
	if err != nil || healthURL.Host == "" || healthURL.User != nil || healthURL.Fragment != "" ||
		healthURL.RawQuery != "" {
		return config{}, errors.New("VELA_WORKER_ARTIFACT_STORE_HEALTH_URL is invalid")
	}
	if healthURL.Scheme == "https" {
		if strings.TrimSpace(configuration.artifactStoreCAFile) == "" ||
			strings.TrimSpace(configuration.artifactStoreCAFile) != configuration.artifactStoreCAFile {
			return config{}, errors.New("VELA_WORKER_ARTIFACT_STORE_CA_FILE is required for HTTPS")
		}
	} else if healthURL.Scheme != "http" || !configuration.allowDevelopmentHTTPUploads {
		return config{}, errors.New("VELA_WORKER_ARTIFACT_STORE_HEALTH_URL must use HTTPS")
	} else if configuration.artifactStoreCAFile != "" {
		return config{}, errors.New("development HTTP Artifact-store health probe must not configure a CA file")
	}
	return configuration, nil
}

func positiveInt64Env(name string) (int64, error) {
	value, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func nonnegativeInt64Env(name string) (int64, error) {
	value, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s must be a nonnegative integer", name)
	}
	return value, nil
}

func positiveIntEnv(name string) (int, error) {
	value, err := positiveInt64Env(name)
	if err != nil || value > int64(math.MaxInt) {
		return 0, fmt.Errorf("%s must be a bounded positive integer", name)
	}
	return int(value), nil
}

func uint32Env(name string, positive bool) (uint32, error) {
	value, err := strconv.ParseUint(os.Getenv(name), 10, 32)
	if err != nil || (positive && value == 0) {
		qualifier := "a uint32"
		if positive {
			qualifier = "a positive uint32"
		}
		return 0, fmt.Errorf("%s must be %s", name, qualifier)
	}
	return uint32(value), nil
}

func durationEnv(name string, minimum, maximum time.Duration) (time.Duration, error) {
	value, err := time.ParseDuration(os.Getenv(name))
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("%s must be in [%s, %s]", name, minimum, maximum)
	}
	return value, nil
}

func pathsOverlap(left, right string) bool {
	left = filepath.Clean(left)
	right = filepath.Clean(right)
	if left == right {
		return true
	}
	leftRelative, leftErr := filepath.Rel(left, right)
	rightRelative, rightErr := filepath.Rel(right, left)
	return leftErr == nil && leftRelative != ".." &&
		!strings.HasPrefix(leftRelative, ".."+string(filepath.Separator)) ||
		rightErr == nil && rightRelative != ".." &&
			!strings.HasPrefix(rightRelative, ".."+string(filepath.Separator))
}

func pathWithin(parent, child string) bool {
	parent = filepath.Clean(parent)
	child = filepath.Clean(child)
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != "." && relative != ".." &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
