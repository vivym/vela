package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/nodeagent"
)

type commandContainerInspector struct {
	target   nodeagent.RuntimeContainerTarget
	result   nodeagent.RuntimeContainerObservation
	inspect  error
	closeErr error
	closed   bool
	cancel   context.CancelFunc
}

func (inspector *commandContainerInspector) Inspect(ctx context.Context, target nodeagent.RuntimeContainerTarget) (nodeagent.RuntimeContainerObservation, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nodeagent.RuntimeContainerObservation{}, errors.New("missing inspection deadline")
	}
	inspector.target = target
	if inspector.cancel != nil {
		inspector.cancel()
	}
	return inspector.result, inspector.inspect
}

func (inspector *commandContainerInspector) Close() error {
	inspector.closed = true
	return inspector.closeErr
}

func runtimeContainerCommandArguments() []string {
	return []string{"--cri-socket", "/run/containerd/containerd.sock", "--node-identity", "node-1",
		"--container-id", strings.Repeat("a", 64), "--sandbox-id", strings.Repeat("b", 64),
		"--pod-uid", "12345678-1234-4234-8234-123456789abc", "--pod-namespace", "vela-system",
		"--pod-name", "vela-member", "--container-name", "model-runtime", "--container-attempt", "3"}
}

func TestRuntimeContainerCommandPrintsOnlyCompletedObservation(t *testing.T) {
	for _, fault := range []string{"none", "dial", "inspect", "close", "canceled", "output"} {
		t.Run(fault, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			injected := errors.New("injected inspection failure")
			inspector := &commandContainerInspector{result: nodeagent.RuntimeContainerObservation{SchemaVersion: 1,
				ContainerState: "CONTAINER_EXITED", SandboxPIDNamespace: "POD", BootID: uuid.New()}}
			switch fault {
			case "inspect":
				inspector.inspect = injected
			case "close":
				inspector.closeErr = injected
			case "canceled":
				inspector.cancel = cancel
			}
			var stdout, stderr bytes.Buffer
			var writer io.Writer = &stdout
			if fault == "output" {
				writer = failingContainerOutput{injected}
			}
			err := inspectRuntimeContainer(ctx, runtimeContainerCommandArguments(), writer, &stderr,
				func(ctx context.Context, config nodeagent.RuntimeContainerObserverConfig) (runtimeContainerInspector, error) {
					deadline, ok := ctx.Deadline()
					if !ok || time.Until(deadline) > 10*time.Second || config.SocketPath != "/run/containerd/containerd.sock" || config.NodeIdentity != "node-1" {
						t.Fatalf("invalid bounded CRI dial: %+v deadline=%v", config, deadline)
					}
					if fault == "dial" {
						return nil, injected
					}
					return inspector, nil
				})
			if fault == "none" {
				var result nodeagent.RuntimeContainerObservation
				if err != nil || json.Unmarshal(stdout.Bytes(), &result) != nil || result != inspector.result ||
					inspector.target.ContainerAttempt != 3 || inspector.target.ContainerID != strings.Repeat("a", 64) ||
					inspector.target.PodUID.String() != "12345678-1234-4234-8234-123456789abc" {
					t.Fatalf("command changed exact target or observation: %+v %v %s", inspector.target, err, stdout.Bytes())
				}
			} else if err == nil || stdout.Len() != 0 {
				t.Fatalf("failed observation printed success: %v %s", err, stdout.Bytes())
			}
			if inspector.closed != (fault != "dial") {
				t.Fatal("command did not close its observation connection")
			}
		})
	}
}

type failingContainerOutput struct{ err error }

func (output failingContainerOutput) Write([]byte) (int, error) { return 0, output.err }

func TestRuntimeContainerCommandRejectsAmbiguousTargetsBeforeDial(t *testing.T) {
	for _, fault := range []string{"short-id", "same-id", "pod-uuid", "namespace", "attempt-overflow", "timeout", "positional", "missing-node", "unknown-flag"} {
		t.Run(fault, func(t *testing.T) {
			arguments := runtimeContainerCommandArguments()
			switch fault {
			case "short-id":
				arguments = append(arguments, "--container-id", "abc123")
			case "same-id":
				arguments = append(arguments, "--container-id", strings.Repeat("b", 64))
			case "pod-uuid":
				arguments = append(arguments, "--pod-uid", "urn:uuid:12345678-1234-4234-8234-123456789abc")
			case "namespace":
				arguments = append(arguments, "--pod-namespace", "../vela")
			case "attempt-overflow":
				arguments = append(arguments, "--container-attempt", "4294967296")
			case "timeout":
				arguments = append(arguments, "--timeout", "1m")
			case "positional":
				arguments = append(arguments, "unexpected")
			case "missing-node":
				arguments = append(arguments, "--node-identity", "")
			case "unknown-flag":
				arguments = append(arguments, "--retire")
			}
			called := false
			var stdout, stderr bytes.Buffer
			err := inspectRuntimeContainer(t.Context(), arguments, &stdout, &stderr,
				func(context.Context, nodeagent.RuntimeContainerObserverConfig) (runtimeContainerInspector, error) {
					called = true
					return nil, errors.New("unexpected CRI dial")
				})
			if err == nil || called || stdout.Len() != 0 {
				t.Fatalf("invalid target reached CRI: %v %s", err, stdout.Bytes())
			}
		})
	}
	var stdout, stderr bytes.Buffer
	if err := runCommand(t.Context(), []string{"inspect-runtime-container", "--help"}, &stdout, &stderr); err != nil ||
		stdout.Len() != 0 || !strings.Contains(stderr.String(), "cri-socket") {
		t.Fatalf("inspection command is not reachable: %v %s", err, stderr.Bytes())
	}
}
