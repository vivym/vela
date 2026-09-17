//go:build linux

// runtime-image-preflight exercises the production image observer without
// reserving a Worker, creating a workload, or granting model execution.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/runtimelaunch"
)

func main() {
	node := flag.String("node", "", "verified local node identity")
	socket := flag.String("cri-socket", "", "trusted local containerd socket")
	manifest := flag.String("manifest-digest", "", "approved single-platform manifest digest")
	flag.Parse()
	if flag.NArg() != 0 || *node == "" || *socket == "" || *manifest == "" {
		flag.Usage()
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	observer, err := nodeagent.DialRuntimeImageObserver(ctx, nodeagent.RuntimeImageObserverConfig{RuntimeContainerObserverConfig: nodeagent.RuntimeContainerObserverConfig{SocketPath: *socket, NodeIdentity: *node}, Namespace: "k8s.io", Snapshotter: "native"})
	if err != nil {
		fail(err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(observer.Close)
	launch, err := observer.InspectLaunch(ctx, *manifest)
	if err != nil {
		fail(err)
	}
	configuration, err := launch.Configuration()
	if err != nil {
		fail(err)
	}
	if !runtimelaunch.RuntimeEntrypoint(configuration.Config.Entrypoint) || len(configuration.Config.Cmd) != 0 ||
		configuration.Config.User != "10001:10001" || configuration.Config.WorkingDir != "/" ||
		!slices.Equal(configuration.Config.Env, []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "HOME=/"}) {
		fail(fmt.Errorf("image configuration violates the Kubernetes Runtime CLI contract"))
	}
	if err := json.NewEncoder(os.Stdout).Encode(launch.Executable()); err != nil {
		fail(err)
	}
}
func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
