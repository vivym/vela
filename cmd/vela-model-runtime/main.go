package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/vivym/vela/internal/authoritypolicy"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
)

type commandConfig struct {
	launchManifestFile    string
	authorityVerifierFile string
	epochDirectory        string
	socketPath            string
	cancelTimeout         time.Duration
	shutdownTimeout       time.Duration
}

type modelRuntimeServer interface {
	Wait() error
	Close() error
}

type modelRuntimeServerStarter func(
	context.Context,
	modelruntime.RuntimeServerConfig,
) (modelRuntimeServer, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "vela-model-runtime stopped: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	return runUsing(ctx, func(
		ctx context.Context,
		config modelruntime.RuntimeServerConfig,
	) (modelRuntimeServer, error) {
		return modelruntime.StartRuntimeServer(ctx, config)
	})
}

func runUsing(ctx context.Context, start modelRuntimeServerStarter) error {
	if ctx == nil {
		return errors.New("ModelRuntime context is required")
	}
	if start == nil {
		return errors.New("ModelRuntime server starter is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	configuration, err := loadCommandConfig()
	if err != nil {
		return err
	}
	manifest, err := modelruntime.LoadLaunchManifest(configuration.launchManifestFile)
	if err != nil {
		return err
	}
	keyring, err := stageauthority.ReadVerifierKeyringFile(configuration.authorityVerifierFile)
	if err != nil {
		return err
	}
	defer stageauthority.ClearKeyring(keyring)
	validator, err := stageauthority.NewVerifier(keyring, time.Now)
	if err != nil {
		return fmt.Errorf("configure ModelRuntime StageAuthority validator: %w", err)
	}
	epochStore, err := modelruntime.NewFileEpochStore(configuration.epochDirectory)
	if err != nil {
		return err
	}
	server, err := start(ctx, modelruntime.RuntimeServerConfig{
		Manifest: manifest, EpochStore: epochStore, Validator: validator,
		SocketPath: configuration.socketPath, CancelTimeout: configuration.cancelTimeout,
		ShutdownTimeout: configuration.shutdownTimeout,
		MaxClockSkew:    authoritypolicy.ProductionMaxClockSkew,
	})
	if err != nil {
		return err
	}
	wait := make(chan error, 1)
	go func() { wait <- server.Wait() }()
	select {
	case <-wait:
		closeErr := server.Close()
		if closeErr == nil {
			return errors.New("ModelRuntime gRPC server stopped unexpectedly")
		}
		return closeErr
	case <-ctx.Done():
		return server.Close()
	}
}

func loadCommandConfig() (commandConfig, error) {
	var configuration commandConfig
	for name, target := range map[string]*string{
		"VELA_MODEL_RUNTIME_LAUNCH_MANIFEST_FILE":            &configuration.launchManifestFile,
		"VELA_MODEL_RUNTIME_AUTHORITY_VERIFIER_KEYRING_FILE": &configuration.authorityVerifierFile,
		"VELA_MODEL_RUNTIME_EPOCH_DIRECTORY":                 &configuration.epochDirectory,
		"VELA_MODEL_RUNTIME_SOCKET":                          &configuration.socketPath,
	} {
		value, err := requiredCommandAbsolutePath(name)
		if err != nil {
			return commandConfig{}, err
		}
		*target = value
	}
	var err error
	configuration.cancelTimeout, err = requiredCommandDuration(
		"VELA_MODEL_RUNTIME_CANCEL_TIMEOUT", time.Millisecond, time.Minute,
	)
	if err != nil {
		return commandConfig{}, err
	}
	configuration.shutdownTimeout, err = requiredCommandDuration(
		"VELA_MODEL_RUNTIME_SHUTDOWN_TIMEOUT", time.Millisecond, 10*time.Minute,
	)
	if err != nil {
		return commandConfig{}, err
	}
	return configuration, nil
}

func requiredCommandAbsolutePath(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" || filepath.Clean(value) != value || !filepath.IsAbs(value) ||
		strings.ContainsRune(value, '\x00') {
		return "", fmt.Errorf("%s must be a canonical absolute path", name)
	}
	return value, nil
}

func requiredCommandDuration(name string, minimum, maximum time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("%s must be between %s and %s", name, minimum, maximum)
	}
	return parsed, nil
}
