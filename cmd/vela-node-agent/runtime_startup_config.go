package main

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageauthority"
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
	// Parse the StageAuthority keyring now so an enabled process cannot start
	// with a malformed or empty authority source that would fail later.
	stageKeys, err := stageauthority.ReadVerifierKeyringFile(configuration.runtimeStageVerifierFile)
	if err != nil {
		return nil, fmt.Errorf("load runtime StageAuthority verifier: %w", err)
	}
	defer stageauthority.ClearKeyring(stageKeys)
	if _, err := stageauthority.NewVerifier(stageKeys, nil); err != nil {
		return nil, fmt.Errorf("configure runtime StageAuthority verifier: %w", err)
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
