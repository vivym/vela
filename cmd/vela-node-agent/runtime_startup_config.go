package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleettransport"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/securefile"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"
)

const maxRuntimeKubeconfigBytes = 1 << 20

// loadRuntimeStartupPlan loads only immutable, signed startup inputs. It does
// not reserve a remote operation, inspect a Pod, create an observer, or grant
// startup permission. Those authority-bearing steps remain the responsibility
// of the runtime startup composition root.
func loadRuntimeStartupPlan(configuration config) (*nodeagent.RuntimeLaunchPlan, error) {
	if !configuration.runtimeStartupEnabled {
		return nil, errors.New("runtime startup is disabled")
	}
	paths := map[string]string{
		"runtime launch manifest":         configuration.runtimeLaunchManifestFile,
		"runtime bundle manifest":         configuration.runtimeBundleManifestFile,
		"runtime binding":                 configuration.runtimeBindingFile,
		"runtime binding verifier":        configuration.runtimeBindingVerifierFile,
		"runtime StageAuthority verifier": configuration.runtimeStageVerifierFile,
		"runtime journal state":           configuration.runtimeJournalStateDir,
		"runtime CRI socket":              configuration.runtimeCRISocket,
		"runtime Kubernetes config":       configuration.runtimeKubeconfig,
		"runtime startup socket":          configuration.runtimeStartupSocket,
	}
	for description, path := range paths {
		cleaned := filepath.Clean(path)
		if path == "" || !filepath.IsAbs(cleaned) || cleaned != path {
			return nil, fmt.Errorf("%s path is missing or not absolute and clean", description)
		}
	}
	launchManifest, err := modelruntime.LoadLaunchManifest(configuration.runtimeLaunchManifestFile)
	if err != nil {
		return nil, fmt.Errorf("load runtime launch manifest: %w", err)
	}
	bundleManifest, err := securefile.Read(configuration.runtimeBundleManifestFile, fleet.MaximumWorkerBootstrapManifestBytes, true)
	if err != nil {
		return nil, fmt.Errorf("load runtime bundle manifest: %w", err)
	}
	bindingVerifier, err := journalbinding.ReadVerifierFile(configuration.runtimeBindingVerifierFile)
	if err != nil {
		return nil, fmt.Errorf("load runtime binding verifier: %w", err)
	}
	binding, err := journalbinding.LoadFile(configuration.runtimeBindingFile, bindingVerifier)
	if err != nil {
		return nil, fmt.Errorf("load runtime binding: %w", err)
	}
	plan, err := nodeagent.VerifyRuntimeLaunchPlan(configuration.nodeIdentity, bindingVerifier, binding, bundleManifest)
	if err != nil {
		return nil, fmt.Errorf("verify runtime launch plan: %w", err)
	}
	if err := plan.MatchManifest(launchManifest); err != nil {
		return nil, fmt.Errorf("runtime launch manifest does not match verified plan: %w", err)
	}
	return plan, nil
}

// loadRuntimeKubernetesCore parses an explicitly provisioned kubeconfig after
// validating its inode, ownership and permissions. It never falls back to
// in-cluster credentials or the ambient KUBECONFIG environment.
func loadRuntimeKubernetesCore(path string) (coreclient.CoreV1Interface, error) {
	cleaned := filepath.Clean(path)
	if path == "" || !filepath.IsAbs(cleaned) || cleaned != path {
		return nil, errors.New("runtime Kubernetes config path is missing or not absolute and clean")
	}
	wire, err := securefile.Read(path, maxRuntimeKubeconfigBytes, true)
	if err != nil {
		return nil, fmt.Errorf("read runtime Kubernetes config: %w", err)
	}
	config, err := clientcmd.Load(bytes.Clone(wire))
	if err != nil {
		return nil, fmt.Errorf("decode runtime Kubernetes config: %w", err)
	}
	restConfig, err := clientcmd.NewDefaultClientConfig(*config, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("configure runtime Kubernetes client: %w", err)
	}
	core, err := coreclient.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create runtime Kubernetes client: %w", err)
	}
	return core, nil
}

// loadRuntimeStartupRegistry creates a separately scoped Fleet bootstrap
// client. The WorkerInstance evidence client is intentionally not reused as a
// startup reservation authority. The returned close function owns the new
// connection and must be called on every subsequent assembly failure.
func loadRuntimeStartupRegistry(ctx context.Context, configuration config) (*fleettransport.BootstrapClient, func() error, error) {
	if !configuration.runtimeStartupEnabled {
		return nil, nil, errors.New("runtime startup is disabled")
	}
	if ctx == nil {
		return nil, nil, errors.New("runtime startup registry context is required")
	}
	credentials, identity, err := fleettransport.NewWorkerBootstrapTLSCredentials(
		configuration.fleetClientCertificate,
		configuration.fleetClientPrivateKey,
		configuration.fleetCA,
		configuration.fleetServerName,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("configure runtime startup Fleet credentials: %w", err)
	}
	expectedIdentity := nodeagent.NodeAgentSPIFFEIdentity(nodeagent.NodeAgentIdentity{
		NodeIdentity: configuration.nodeIdentity,
		AgentID:      configuration.agentID,
		AgentEpoch:   configuration.agentEpoch,
	})
	if identity != expectedIdentity {
		return nil, nil, errors.New("runtime startup Fleet certificate identity does not match Node identity")
	}
	client, err := fleettransport.DialClient(ctx, configuration.fleetAddress, credentials)
	if err != nil {
		return nil, nil, fmt.Errorf("connect runtime startup Fleet registry: %w", err)
	}
	registry, err := client.WorkerBootstrap(identity)
	if err != nil {
		_ = client.Close()
		return nil, nil, fmt.Errorf("scope runtime startup Fleet registry: %w", err)
	}
	return registry, client.Close, nil
}
