package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/nodeagent"
)

type runtimeContainerInspector interface {
	Inspect(context.Context, nodeagent.RuntimeContainerTarget) (nodeagent.RuntimeContainerObservation, error)
	Close() error
}

func runRuntimeContainerInspection(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	return inspectRuntimeContainer(ctx, arguments, stdout, stderr,
		func(ctx context.Context, config nodeagent.RuntimeContainerObserverConfig) (runtimeContainerInspector, error) {
			return nodeagent.DialRuntimeContainerObserver(ctx, config)
		})
}

func inspectRuntimeContainer(ctx context.Context, arguments []string, stdout, stderr io.Writer,
	dial func(context.Context, nodeagent.RuntimeContainerObserverConfig) (runtimeContainerInspector, error),
) error {
	if ctx == nil || stdout == nil || stderr == nil || dial == nil {
		return errors.New("runtime container inspection requires context and output writers")
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	flags := flag.NewFlagSet("vela-node-agent inspect-runtime-container", flag.ContinueOnError)
	flags.SetOutput(stderr)
	socket := flags.String("cri-socket", "", "absolute trusted local CRI socket")
	node := flags.String("node-identity", "", "configured Node identity")
	var target nodeagent.RuntimeContainerTarget
	flags.StringVar(&target.ContainerID, "container-id", "", "exact full container ID")
	flags.StringVar(&target.SandboxID, "sandbox-id", "", "exact full Pod sandbox ID")
	podUID := flags.String("pod-uid", "", "exact Kubernetes Pod UUID")
	flags.StringVar(&target.PodNamespace, "pod-namespace", "", "expected Pod namespace")
	flags.StringVar(&target.PodName, "pod-name", "", "expected Pod name")
	flags.StringVar(&target.ContainerName, "container-name", "", "expected container name")
	attempt := flags.Uint64("container-attempt", 0, "expected container attempt number")
	timeout := flags.Duration("timeout", 10*time.Second, "total inspection timeout (at most 30s)")
	if err := flags.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 || *socket == "" || *node == "" || *attempt > math.MaxUint32 || *timeout <= 0 || *timeout > 30*time.Second {
		return errors.New("runtime container inspection requires CRI socket, Node identity, bounded attempt and timeout")
	}
	var err error
	target.PodUID, err = uuid.Parse(*podUID)
	if err != nil || target.PodUID.String() != *podUID {
		return errors.New("runtime container inspection requires canonical Pod UUID")
	}
	target.ContainerAttempt = uint32(*attempt)
	if err := target.Validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	observer, err := dial(ctx, nodeagent.RuntimeContainerObserverConfig{SocketPath: *socket, NodeIdentity: *node})
	if err != nil {
		return err
	}
	result, inspectErr := observer.Inspect(ctx, target)
	if err := errors.Join(inspectErr, observer.Close(), context.Cause(ctx)); err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(result)
}
