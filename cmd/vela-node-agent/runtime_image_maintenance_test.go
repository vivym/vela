package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

const maintenanceConfigJSON = `{"schema_version":1,"containerd_socket":"/run/containerd/containerd.sock","node_identity":"node-1","namespace":"k8s.io","snapshotter":"native"}`

type commandImageRecovery struct {
	recover  func(context.Context) (int, error)
	closed   bool
	closeErr error
}

func (recovery *commandImageRecovery) RecoverExpired(ctx context.Context) (int, error) {
	return recovery.recover(ctx)
}

func (recovery *commandImageRecovery) Close() error {
	recovery.closed = true
	return recovery.closeErr
}

func TestRuntimeImageMaintenanceRepeatsWithFreshConnections(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var stdout bytes.Buffer
		var observers []*commandImageRecovery
		var starts []time.Time
		var records sync.Mutex
		config, err := parseRuntimeImageMaintenanceConfig([]byte(maintenanceConfigJSON))
		if err != nil {
			t.Fatal(err)
		}
		dial := func(ctx context.Context, actual runtimeImageMaintenanceConfig) (runtimeImageRecovery, error) {
			records.Lock()
			defer records.Unlock()
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) != 10*time.Second || actual != config {
				t.Fatalf("unbounded or changed dial configuration: %+v %v", actual, deadline)
			}
			if len(observers) != 0 && !observers[len(observers)-1].closed {
				t.Fatal("previous connection leaked between passes")
			}
			starts = append(starts, time.Now())
			observer := &commandImageRecovery{recover: func(ctx context.Context) (int, error) {
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) != 30*time.Second {
					t.Fatal("recovery lacks its independent timeout")
				}
				return 32, nil
			}}
			observers = append(observers, observer)
			return observer, nil
		}
		done := make(chan error, 1)
		go func() { done <- maintainRuntimeImages(ctx, config, &stdout, dial) }()
		synctest.Wait()
		records.Lock()
		firstComplete := len(observers) == 1 && observers[0].closed
		records.Unlock()
		if !firstComplete {
			t.Fatal("first pass did not run immediately and close")
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		records.Lock()
		secondComplete := len(observers) == 2 && starts[1].Sub(starts[0]) == time.Minute
		records.Unlock()
		if !secondComplete {
			t.Fatal("periodic recovery did not respect its interval")
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if !observers[1].closed || strings.Count(stdout.String(), `"recovered":32`) != 2 {
			t.Fatalf("missing completed pass evidence: %s", stdout.Bytes())
		}
	})
}

func TestRuntimeImageMaintenanceStopsOnIncompletePass(t *testing.T) {
	for _, fault := range []string{"dial", "recovery", "close", "output", "canceled-during-recovery", "deadline"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				injected := errors.New("injected maintenance failure")
				observer := &commandImageRecovery{recover: func(ctx context.Context) (int, error) {
					switch fault {
					case "recovery":
						return 2, injected
					case "canceled-during-recovery":
						cancel()
					case "deadline":
						<-ctx.Done()
					}
					return 2, nil
				}}
				if fault == "close" {
					observer.closeErr = injected
				}
				var stdout bytes.Buffer
				var writer io.Writer = &stdout
				if fault == "output" {
					writer = failingContainerOutput{injected}
				}
				err := maintainRuntimeImages(ctx, runtimeImageMaintenanceConfig{}, writer,
					func(context.Context, runtimeImageMaintenanceConfig) (runtimeImageRecovery, error) {
						if fault == "dial" {
							return nil, injected
						}
						return observer, nil
					})
				if err == nil || stdout.Len() != 0 || observer.closed != (fault != "dial") {
					t.Fatalf("incomplete pass emitted success or leaked connection: %v %s closed=%v", err, stdout.Bytes(), observer.closed)
				}
				if fault == "recovery" && (!errors.Is(err, injected) || !strings.Contains(err.Error(), "2 completed recoveries")) {
					t.Fatalf("partial progress lost: %v", err)
				}
				if fault == "deadline" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("recovery swallowed deadline: %v", err)
				}
			})
		})
	}
}

func TestRuntimeImageMaintenanceRejectsUntrustedConfigBeforeDial(t *testing.T) {
	for _, fault := range []string{"unknown", "duplicate", "alias", "null", "missing", "version", "namespace", "snapshotter", "socket", "node", "oversized", "trailing", "mode", "symlink", "relative", "positional", "missing-flag", "unknown-flag"} {
		t.Run(fault, func(t *testing.T) {
			wire := maintenanceConfigJSON
			var fields map[string]any
			if err := json.Unmarshal([]byte(wire), &fields); err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "unknown":
				fields["authorize_start"] = true
			case "alias":
				fields["Node_Identity"] = "node-1"
				delete(fields, "node_identity")
			case "null":
				fields["schema_version"] = nil
			case "missing":
				delete(fields, "snapshotter")
			case "version":
				fields["schema_version"] = 2
			case "namespace":
				fields["namespace"] = "../k8s.io"
			case "snapshotter":
				fields["snapshotter"] = "overlayfs"
			case "socket":
				fields["containerd_socket"] = "tcp://localhost:1234"
			case "node":
				fields["node_identity"] = ""
			case "oversized":
				fields["node_identity"] = strings.Repeat("a", 4096)
			}
			encoded, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			wire = string(encoded)
			if fault == "duplicate" {
				wire = strings.Replace(wire, `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1)
			}
			if fault == "trailing" {
				wire += " {}"
			}
			path := filepath.Join(t.TempDir(), "maintenance.json")
			if err := os.WriteFile(path, []byte(wire), 0o600); err != nil {
				t.Fatal(err)
			}
			if fault == "mode" {
				if err := os.Chmod(path, 0o640); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "symlink" {
				if err := os.Symlink(path, path+".link"); err != nil {
					t.Fatal(err)
				}
				path += ".link"
			}
			if fault == "relative" {
				path = "maintenance.json"
			}
			args := []string{"--config-file", path}
			if fault == "positional" {
				args = append(args, "unexpected")
			}
			if fault == "unknown-flag" {
				args = append(args, "--authorize-start")
			}
			if fault == "missing-flag" {
				args = nil
			}
			called := false
			var stdout, stderr bytes.Buffer
			err = runRuntimeImageMaintenance(t.Context(), args, &stdout, &stderr,
				func(context.Context, runtimeImageMaintenanceConfig) (runtimeImageRecovery, error) {
					called = true
					return nil, errors.New("unexpected dial")
				})
			if err == nil || called || stdout.Len() != 0 {
				t.Fatalf("invalid configuration reached containerd: %v %s", err, stdout.Bytes())
			}
		})
	}
	var stdout, stderr bytes.Buffer
	if err := runCommand(t.Context(), []string{"runtime-image-maintenance", "--help"}, &stdout, &stderr); err != nil ||
		stdout.Len() != 0 || !strings.Contains(stderr.String(), "config-file") {
		t.Fatalf("maintenance command is not reachable: %v %s", err, stderr.Bytes())
	}
}
