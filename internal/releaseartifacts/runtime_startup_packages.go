package releaseartifacts

import (
	"context"
	"debug/elf"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/vivym/vela/internal/releasebundle"
)

type runtimeStartupPackageManifest struct {
	SchemaVersion    int                          `json:"schema_version"`
	Revision         string                       `json:"revision"`
	Packages         []releasebundle.PackageInput `json:"packages"`
	PIDFDBrokerUnit  releasebundle.ArtifactInput  `json:"pidfd_broker_unit"`
	PolicyIssuerUnit releasebundle.ArtifactInput  `json:"runtime_policy_issuer_unit"`
	PIDFDBrokerEnv   releasebundle.ArtifactInput  `json:"pidfd_broker_env"`
	PolicyIssuerEnv  releasebundle.ArtifactInput  `json:"runtime_policy_issuer_env"`
	Provisioning     releasebundle.ArtifactInput  `json:"provisioning"`
}

type runtimeStartupPackageSpec struct {
	input      releasebundle.PackageInput
	packageDir string
	entrypoint string
}

// BuildRuntimeStartupPackages builds the three root-owned Linux helpers that
// form the production Runtime/Worker startup boundary. The output is a
// separate, exact artifact graph so a release plan can bind it explicitly via
// BuildPlan.RuntimeStartup.
func BuildRuntimeStartupPackages(ctx context.Context, sourceRoot, revision, outputDirectory string) error {
	if ctx == nil {
		return fmt.Errorf("runtime startup package build context is required")
	}
	sourceRoot, err := canonicalExistingDirectory(sourceRoot)
	if err != nil {
		return fmt.Errorf("resolve source root: %w", err)
	}
	outputDirectory, parent, err := canonicalNewOutputDirectory(outputDirectory)
	if err != nil {
		return fmt.Errorf("resolve output directory: %w", err)
	}
	if !releasebundle.ValidRevision(revision) {
		return fmt.Errorf("release revision is invalid")
	}
	candidate, err := os.MkdirTemp(parent, ".vela-runtime-startup-packages-")
	if err != nil {
		return fmt.Errorf("create runtime startup package candidate: %w", err)
	}
	defer func() { _ = os.RemoveAll(candidate) }()

	specifications := runtimeStartupPackageSpecifications()
	packages := make([]releasebundle.PackageInput, 0, len(specifications))
	for _, specification := range specifications {
		artifactPath := filepath.Join(candidate, specification.input.ArtifactRef)
		if err := buildLinuxBinary(ctx, sourceRoot, artifactPath, specification.packageDir); err != nil {
			return fmt.Errorf("build %s: %w", specification.input.Name, err)
		}
		digest, size, err := digestFile(artifactPath)
		if err != nil {
			return fmt.Errorf("digest %s: %w", specification.input.Name, err)
		}
		contract := releasebundle.PackageContract{
			SchemaVersion: 1, Name: "vela-" + specification.input.Name,
			OS: "linux", Architecture: "amd64", Revision: revision,
			Entrypoint:     specification.entrypoint,
			ArtifactDigest: "sha256:" + digest, ArtifactSizeBytes: size,
		}
		if err := writeJSONFile(filepath.Join(candidate, specification.input.ContractRef), contract); err != nil {
			return fmt.Errorf("write %s contract: %w", specification.input.Name, err)
		}
		packages = append(packages, specification.input)
	}
	units := []struct {
		name   string
		source string
	}{
		{"pidfd-broker.service", filepath.Join(sourceRoot, "deploy/node-agent/vela-pidfd-broker.service")},
		{"runtime-policy-issuer.service", filepath.Join(sourceRoot, "deploy/node-agent/vela-runtime-policy-issuer.service")},
	}
	for _, unit := range units {
		content, err := readRegularMetadata(unit.source)
		if err != nil {
			return fmt.Errorf("read runtime startup unit %s: %w", unit.name, err)
		}
		if err := writeExactFile(filepath.Join(candidate, unit.name), content); err != nil {
			return fmt.Errorf("write runtime startup unit %s: %w", unit.name, err)
		}
	}
	envExamples := []struct {
		name   string
		source string
	}{
		{"pidfd-broker.env.example", filepath.Join(sourceRoot, "deploy/node-agent/pidfd-broker.env.example")},
		{"runtime-policy-issuer.env.example", filepath.Join(sourceRoot, "deploy/node-agent/runtime-policy-issuer.env.example")},
	}
	for _, example := range envExamples {
		content, err := readRegularMetadata(example.source)
		if err != nil {
			return fmt.Errorf("read runtime startup env example %s: %w", example.name, err)
		}
		if err := writeExactFile(filepath.Join(candidate, example.name), content); err != nil {
			return fmt.Errorf("write runtime startup env example %s: %w", example.name, err)
		}
	}
	manifest := runtimeStartupPackageManifest{
		SchemaVersion: 1, Revision: revision, Packages: packages,
		PIDFDBrokerUnit:  releasebundle.ArtifactInput{Name: "pidfd-broker-systemd-unit", Ref: "pidfd-broker.service"},
		PolicyIssuerUnit: releasebundle.ArtifactInput{Name: "runtime-policy-issuer-systemd-unit", Ref: "runtime-policy-issuer.service"},
		PIDFDBrokerEnv:   releasebundle.ArtifactInput{Name: "pidfd-broker-env-example", Ref: "pidfd-broker.env.example"},
		PolicyIssuerEnv:  releasebundle.ArtifactInput{Name: "runtime-policy-issuer-env-example", Ref: "runtime-policy-issuer.env.example"},
		Provisioning:     releasebundle.ArtifactInput{Name: "runtime-startup-provisioning", Ref: "runtime-startup-provisioning.json"},
	}
	provisioning := releasebundle.RuntimeStartupProvisioningContract{SchemaVersion: 1, SocketParent: "/run/vela-node-agent", BootstrapDirectory: "/run/vela-node-agent/runtime-bootstrap", RequiredFiles: []string{"/etc/vela/pidfd-broker.env", "/etc/vela/runtime-policy-issuer.env", "/etc/vela/runtime-policy-issuer/reply-private.key", "/etc/vela/runtime-policy-issuer/fleet-authorization.pub"}, RequiredEnvKeys: []string{"VELA_NODE_AGENT_RUNTIME_STARTUP_SOCKET", "VELA_NODE_AGENT_RUNTIME_POLICY_ISSUER_SOCKET", "VELA_NODE_AGENT_RUNTIME_POLICY_AUTHORIZATION_DIRECTORY", "VELA_NODE_AGENT_RUNTIME_POLICY_AUTHORIZATION_PUBLIC_KEY_FILE", "VELA_NODE_AGENT_WORKER_JOURNAL_PIDFD_BROKER_SOCKET"}}
	if err := writeJSONFile(filepath.Join(candidate, "runtime-startup-provisioning.json"), provisioning); err != nil {
		return fmt.Errorf("write runtime startup provisioning contract: %w", err)
	}
	if err := writeJSONFile(filepath.Join(candidate, "runtime-startup-packages.json"), manifest); err != nil {
		return fmt.Errorf("write runtime startup package manifest: %w", err)
	}
	if err := verifyRuntimeStartupPackageCandidate(candidate, revision); err != nil {
		return fmt.Errorf("verify runtime startup package candidate: %w", err)
	}
	if err := syncDirectory(candidate); err != nil {
		return fmt.Errorf("sync runtime startup package candidate: %w", err)
	}
	if err := renameNoReplace(candidate, outputDirectory); err != nil {
		return fmt.Errorf("publish runtime startup packages: %w", err)
	}
	if err := syncDirectory(parent); err != nil {
		return fmt.Errorf("sync runtime startup package parent: %w", err)
	}
	return nil
}

// VerifyRuntimeStartupPackages validates an already-published runtime startup
// package directory against the requested release revision. It is the same
// fail-closed verifier used before publishing a newly built candidate, exposed
// so an installer can verify the exact artifact graph before touching system
// paths.
func VerifyRuntimeStartupPackages(directory, revision string) error {
	if directory == "" {
		return errors.New("runtime startup package directory is required")
	}
	if !releasebundle.ValidRevision(revision) {
		return errors.New("release revision is invalid")
	}
	canonical, err := canonicalExistingDirectory(directory)
	if err != nil {
		return fmt.Errorf("resolve runtime startup package directory: %w", err)
	}
	return verifyRuntimeStartupPackageCandidate(canonical, revision)
}

func runtimeStartupPackageSpecifications() [3]runtimeStartupPackageSpec {
	return [3]runtimeStartupPackageSpec{
		{input: releasebundle.PackageInput{Name: "pidfd-broker", ContractRef: "pidfd-broker-contract.json", ArtifactRef: "vela-pidfd-broker"}, packageDir: "./cmd/vela-pidfd-broker", entrypoint: "/usr/local/bin/vela-pidfd-broker"},
		{input: releasebundle.PackageInput{Name: "runtime-launcher", ContractRef: "runtime-launcher-contract.json", ArtifactRef: "vela-runtime-launcher"}, packageDir: "./cmd/vela-runtime-launcher", entrypoint: "/usr/local/bin/vela-runtime-launcher"},
		{input: releasebundle.PackageInput{Name: "runtime-policy-issuer", ContractRef: "runtime-policy-issuer-contract.json", ArtifactRef: "vela-runtime-policy-issuer"}, packageDir: "./cmd/vela-runtime-policy-issuer", entrypoint: "/usr/local/bin/vela-runtime-policy-issuer"},
	}
}

func buildLinuxBinary(ctx context.Context, sourceRoot, output, packageDir string) error {
	command := exec.CommandContext(ctx, "go", "build", "-mod=readonly", "-trimpath", "-buildvcs=false", "-ldflags=-buildid= -s -w", "-o", output, packageDir)
	command.Dir = sourceRoot
	command.Env = buildEnvironment(map[string]string{"CGO_ENABLED": "0", "GOARCH": "amd64", "GOOS": "linux"})
	encoded, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("linux/amd64 build: %w: %s", err, strings.TrimSpace(string(encoded)))
	}
	if err := os.Chmod(output, 0o755); err != nil {
		return fmt.Errorf("set runtime startup package mode: %w", err)
	}
	return syncFile(output)
}

func verifyRuntimeStartupPackageCandidate(directory, revision string) error {
	specifications := runtimeStartupPackageSpecifications()
	expectedInventory := []string{"runtime-startup-packages.json", "runtime-startup-provisioning.json", "pidfd-broker.service", "runtime-policy-issuer.service", "pidfd-broker.env.example", "runtime-policy-issuer.env.example"}
	for _, specification := range specifications {
		expectedInventory = append(expectedInventory, specification.input.ContractRef, specification.input.ArtifactRef)
	}
	slices.Sort(expectedInventory)
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	actualInventory := make([]string, 0, len(entries))
	for _, entry := range entries {
		actualInventory = append(actualInventory, entry.Name())
	}
	if !slices.Equal(actualInventory, expectedInventory) {
		return fmt.Errorf("runtime startup package inventory is not exact: got %v want %v", actualInventory, expectedInventory)
	}
	manifestBytes, err := readRegularMetadata(filepath.Join(directory, "runtime-startup-packages.json"))
	if err != nil {
		return fmt.Errorf("read runtime startup package manifest: %w", err)
	}
	var manifest runtimeStartupPackageManifest
	if err := releasebundle.DecodeStrictJSONForValidation(manifestBytes, &manifest); err != nil {
		return fmt.Errorf("decode runtime startup package manifest: %w", err)
	}
	if manifest.SchemaVersion != 1 || manifest.Revision != revision {
		return errors.New("runtime startup package manifest does not bind the requested revision")
	}
	expectedPackages := make([]releasebundle.PackageInput, 0, len(specifications))
	for _, specification := range specifications {
		expectedPackages = append(expectedPackages, specification.input)
	}
	if !slices.Equal(manifest.Packages, expectedPackages) {
		return errors.New("runtime startup package manifest does not bind the exact package set")
	}
	if manifest.PIDFDBrokerUnit != (releasebundle.ArtifactInput{Name: "pidfd-broker-systemd-unit", Ref: "pidfd-broker.service"}) ||
		manifest.PolicyIssuerUnit != (releasebundle.ArtifactInput{Name: "runtime-policy-issuer-systemd-unit", Ref: "runtime-policy-issuer.service"}) ||
		manifest.PIDFDBrokerEnv != (releasebundle.ArtifactInput{Name: "pidfd-broker-env-example", Ref: "pidfd-broker.env.example"}) ||
		manifest.PolicyIssuerEnv != (releasebundle.ArtifactInput{Name: "runtime-policy-issuer-env-example", Ref: "runtime-policy-issuer.env.example"}) ||
		manifest.Provisioning != (releasebundle.ArtifactInput{Name: "runtime-startup-provisioning", Ref: "runtime-startup-provisioning.json"}) {
		return errors.New("runtime startup package manifest does not bind the exact supporting artifacts")
	}
	for _, specification := range specifications {
		artifact := filepath.Join(directory, specification.input.ArtifactRef)
		if err := requireRuntimeArtifact(artifact); err != nil {
			return fmt.Errorf("validate %s artifact: %w", specification.input.Name, err)
		}
		contract, err := readRegularMetadata(filepath.Join(directory, specification.input.ContractRef))
		if err != nil {
			return err
		}
		artifactDigest, size, err := digestFile(artifact)
		if err != nil {
			return err
		}
		validated, err := releasebundle.ValidatePackageContract(specification.input.Name, contract, releasebundle.Artifact{Digest: "sha256:" + artifactDigest, SizeBytes: size})
		if err != nil {
			return err
		}
		if validated.Revision != revision || validated.Entrypoint != specification.entrypoint {
			return fmt.Errorf("%s contract does not bind the requested revision and entrypoint", specification.input.Name)
		}
	}
	for _, unit := range []struct{ component, ref, entrypoint string }{
		{"pidfd-broker", "pidfd-broker.service", "/usr/local/bin/vela-pidfd-broker"},
		{"runtime-policy-issuer", "runtime-policy-issuer.service", "/usr/local/bin/vela-runtime-policy-issuer"},
	} {
		content, err := readRegularMetadata(filepath.Join(directory, unit.ref))
		if err != nil {
			return err
		}
		if err := releasebundle.ValidateRuntimeStartupSystemdUnit(unit.component, content, unit.entrypoint); err != nil {
			return err
		}
	}
	for _, env := range []struct{ component, ref string }{{"pidfd-broker", "pidfd-broker.env.example"}, {"runtime-policy-issuer", "runtime-policy-issuer.env.example"}} {
		content, err := readRegularMetadata(filepath.Join(directory, env.ref))
		if err != nil {
			return err
		}
		if err := releasebundle.ValidateRuntimeStartupEnvExample(string(content), env.component); err != nil {
			return err
		}
	}
	provisioning, err := readRegularMetadata(filepath.Join(directory, "runtime-startup-provisioning.json"))
	if err != nil {
		return err
	}
	var provisioningContract releasebundle.RuntimeStartupProvisioningContract
	if err := releasebundle.DecodeStrictJSONForValidation(provisioning, &provisioningContract); err != nil {
		return fmt.Errorf("runtime startup provisioning contract is invalid: %w", err)
	}
	if err := releasebundle.ValidateRuntimeStartupProvisioningContract(provisioningContract); err != nil {
		return err
	}
	if !releasebundle.ValidRevision(revision) {
		return fmt.Errorf("runtime startup package revision is invalid")
	}
	return nil
}

func requireRuntimeArtifact(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o755 || info.Size() == 0 {
		return errors.New("artifact must be a non-empty regular executable with mode 0755")
	}
	binary, err := elf.Open(path)
	if err != nil {
		return fmt.Errorf("open ELF: %w", err)
	}
	defer func() { _ = binary.Close() }()
	if binary.Class != elf.ELFCLASS64 || binary.Machine != elf.EM_X86_64 {
		return errors.New("artifact must be a linux/amd64 ELF64 binary")
	}
	return nil
}
