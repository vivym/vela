package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
	"k8s.io/apimachinery/pkg/util/validation"
)

const runtimeImageMaintenanceInterval = time.Minute

type runtimeImageMaintenanceConfig struct {
	SchemaVersion int    `json:"schema_version"`
	SocketPath    string `json:"containerd_socket"`
	NodeIdentity  string `json:"node_identity"`
	Namespace     string `json:"namespace"`
	Snapshotter   string `json:"snapshotter"`
}

type runtimeImageRecovery interface {
	RecoverExpired(context.Context) (int, error)
	Close() error
}

type runtimeImageMaintenanceDial func(context.Context, runtimeImageMaintenanceConfig) (runtimeImageRecovery, error)

func runRuntimeImageMaintenance(ctx context.Context, arguments []string, stdout, stderr io.Writer, dial runtimeImageMaintenanceDial) error {
	if ctx == nil || stdout == nil || stderr == nil || dial == nil {
		return errors.New("image maintenance requires context, output writers and a dialer")
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	flags := flag.NewFlagSet("vela-node-agent runtime-image-maintenance", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config-file", "", "absolute private image maintenance configuration file")
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*configPath) || filepath.Clean(*configPath) != *configPath {
		return errors.New("image maintenance requires only an absolute config-file")
	}
	parent, err := securefile.ResolveTrustedDirectory(filepath.Dir(*configPath))
	if err != nil {
		return err
	}
	wire, err := securefile.Read(filepath.Join(parent, filepath.Base(*configPath)), 4096, true)
	if err != nil {
		return err
	}
	config, err := parseRuntimeImageMaintenanceConfig(wire)
	if err != nil {
		return err
	}
	return maintainRuntimeImages(ctx, config, stdout, dial)
}

func parseRuntimeImageMaintenanceConfig(wire []byte) (runtimeImageMaintenanceConfig, error) {
	var config runtimeImageMaintenanceConfig
	if len(wire) == 0 || len(wire) > 4096 || !utf8.Valid(wire) {
		return config, errors.New("image maintenance configuration is empty, oversized or not UTF-8")
	}
	if err := strictjson.RejectDuplicateKeys(wire); err != nil {
		return config, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(wire, &fields); err != nil {
		return config, err
	}
	// Exact keys reject encoding/json's case-insensitive field aliases.
	keys := []string{"schema_version", "containerd_socket", "node_identity", "namespace", "snapshotter"}
	if len(fields) != len(keys) {
		return config, errors.New("image maintenance configuration requires the exact field set")
	}
	for _, key := range keys {
		if value, ok := fields[key]; !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return config, errors.New("image maintenance configuration has a missing or null field")
		}
	}
	if err := json.Unmarshal(wire, &config); err != nil {
		return config, err
	}
	if config.SchemaVersion != 1 || !filepath.IsAbs(config.SocketPath) || filepath.Clean(config.SocketPath) != config.SocketPath ||
		len(config.SocketPath) > 100 || strings.ContainsRune(config.SocketPath, 0) ||
		config.NodeIdentity == "" || len(config.NodeIdentity) > 500 || strings.TrimSpace(config.NodeIdentity) != config.NodeIdentity ||
		strings.ContainsAny(config.NodeIdentity, "\x00\r\n") || len(validation.IsDNS1123Subdomain(config.Namespace)) != 0 || config.Snapshotter != "native" {
		return runtimeImageMaintenanceConfig{}, errors.New("image maintenance requires schema 1, a local socket, Node identity, namespace and qualified native snapshotter")
	}
	return config, nil
}

func maintainRuntimeImages(ctx context.Context, config runtimeImageMaintenanceConfig, stdout io.Writer, dial runtimeImageMaintenanceDial) error {
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		// Re-authenticate each pass so a replaced daemon socket never inherits
		// the preceding connection's identity. systemd retries failed passes.
		dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		observer, err := dial(dialCtx, config)
		cancel()
		if err != nil {
			return fmt.Errorf("dial image maintenance observer: %w", err)
		}
		recoveryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		count, recoveryErr := observer.RecoverExpired(recoveryCtx)
		recoveryErr = errors.Join(recoveryErr, context.Cause(recoveryCtx))
		cancel()
		if err := errors.Join(recoveryErr, observer.Close(), context.Cause(ctx)); err != nil {
			return fmt.Errorf("image maintenance stopped after %d completed recoveries: %w", count, err)
		}
		if err := json.NewEncoder(stdout).Encode(struct {
			SchemaVersion int    `json:"schema_version"`
			Event         string `json:"event"`
			Recovered     int    `json:"recovered"`
		}{1, "runtime_image_maintenance_completed", count}); err != nil {
			return err
		}
		timer := time.NewTimer(runtimeImageMaintenanceInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			if errors.Is(context.Cause(ctx), context.Canceled) {
				return nil
			}
			return context.Cause(ctx)
		case <-timer.C:
		}
	}
}
