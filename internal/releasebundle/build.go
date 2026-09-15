package releasebundle

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"

	"github.com/vivym/vela/internal/fleetcontroller"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/util/validation"
)

var runtimeStartupBrokerSystemdV1 = map[string]map[string]string{
	"Unit": {
		"Description": "Vela host pidfd identity broker",
		"Wants":       "network-online.target",
		"After":       "network-online.target",
		"Before":      "vela-node-agent.service",
	},
	"Service": {
		"Type":                    "simple",
		"User":                    "root",
		"Group":                   "root",
		"ExecStart":               "",
		"EnvironmentFile":         "/etc/vela/pidfd-broker.env",
		"Restart":                 "on-failure",
		"RestartSec":              "2s",
		"UMask":                   "0077",
		"RuntimeDirectory":        "vela",
		"RuntimeDirectoryMode":    "0755",
		"ProtectHome":             "true",
		"PrivateTmp":              "true",
		"RestrictAddressFamilies": "AF_UNIX",
		"LockPersonality":         "true",
		"MemoryDenyWriteExecute":  "true",
		"NoNewPrivileges":         "false",
		"LimitNOFILE":             "256",
	},
	"Install": {"WantedBy": "multi-user.target"},
}

var runtimeStartupIssuerSystemdV1 = map[string]map[string]string{
	"Unit": {
		"Description": "Vela runtime startup policy issuer",
		"Wants":       "network-online.target",
		"After":       "network-online.target",
		"Before":      "vela-node-agent.service",
	},
	"Service": {
		"Type":                    "simple",
		"User":                    "root",
		"Group":                   "root",
		"ExecStart":               "",
		"EnvironmentFile":         "/etc/vela/runtime-policy-issuer.env",
		"Restart":                 "on-failure",
		"RestartSec":              "2s",
		"UMask":                   "0077",
		"RuntimeDirectory":        "vela-runtime-policy",
		"RuntimeDirectoryMode":    "0755",
		"StateDirectory":          "vela/runtime-policy-issuer",
		"StateDirectoryMode":      "0700",
		"ExecStartPre":            "/usr/bin/install -d -o root -g root -m 0700 /var/lib/vela/runtime-policy-issuer/authorizations /var/lib/vela/runtime-policy-issuer/replies",
		"ProtectHome":             "true",
		"PrivateTmp":              "true",
		"RestrictAddressFamilies": "AF_UNIX",
		"LockPersonality":         "true",
		"MemoryDenyWriteExecute":  "true",
		"NoNewPrivileges":         "false",
		"LimitNOFILE":             "256",
	},
	"Install": {"WantedBy": "multi-user.target"},
}

var (
	fixedRenderNames = []string{
		"control-storage", "fleet-controller", "observability", "stage-worker", "vela-control",
	}
	fixedPackageNames                  = []string{"node-agent"}
	fixedRuntimeStartupPackageNames    = []string{"pidfd-broker", "runtime-launcher", "runtime-policy-issuer"}
	runtimeStartupPIDFDBrokerUnitName  = "pidfd-broker-systemd-unit"
	runtimeStartupPolicyIssuerUnitName = "runtime-policy-issuer-systemd-unit"
	runtimeStartupPIDFDBrokerEnvName   = "pidfd-broker-env-example"
	runtimeStartupPolicyIssuerEnvName  = "runtime-policy-issuer-env-example"
	nodeAgentSystemdV1                 = map[string]map[string]string{
		"Unit": {
			"Description": "Vela host remediation Node Agent",
			"Wants":       "network-online.target",
			"After":       "network-online.target",
		},
		"Service": {
			"Type":                    "simple",
			"ExecStart":               "",
			"EnvironmentFile":         "/etc/vela/node-agent.env",
			"Restart":                 "on-failure",
			"RestartSec":              "5s",
			"UMask":                   "0077",
			"RuntimeDirectory":        "vela-node-agent",
			"RuntimeDirectoryMode":    "0755",
			"StateDirectory":          "vela-node-agent",
			"StateDirectoryMode":      "0750",
			"ProtectHome":             "true",
			"PrivateTmp":              "true",
			"RestrictAddressFamilies": "AF_UNIX AF_INET AF_INET6",
			"LockPersonality":         "true",
			"MemoryDenyWriteExecute":  "true",
			"NoNewPrivileges":         "false",
			"LimitNOFILE":             "4096",
		},
		"Install": {
			"WantedBy": "multi-user.target",
		},
	}
	runtimeImageMaintenanceSystemdV1 = map[string]map[string]string{
		"Unit": {
			"Description":           "Vela runtime image observation maintenance",
			"StartLimitIntervalSec": "0",
		},
		"Service": {
			"Type":                    "simple",
			"User":                    "root",
			"Group":                   "root",
			"ExecStart":               "",
			"Restart":                 "on-failure",
			"RestartSec":              "5s",
			"TimeoutStopSec":          "40s",
			"UMask":                   "0077",
			"CapabilityBoundingSet":   "",
			"NoNewPrivileges":         "true",
			"ProtectSystem":           "strict",
			"ProtectHome":             "true",
			"PrivateTmp":              "true",
			"PrivateDevices":          "true",
			"RestrictAddressFamilies": "AF_UNIX",
			"LockPersonality":         "true",
			"MemoryDenyWriteExecute":  "true",
			"LimitNOFILE":             "1024",
		},
		"Install": {"WantedBy": "multi-user.target"},
	}
)

type resourceKey struct {
	Kind      string
	Namespace string
	Name      string
}

type renderInventory struct {
	renderContract     string
	declared           map[resourceKey]struct{}
	referred           map[resourceKey]struct{}
	expectedRevision   map[resourceKey]string
	secretKeys         map[resourceKey]map[string]struct{}
	secretConsumers    map[resourceKey]map[string]struct{}
	images             map[string]struct{}
	supportImages      map[string]struct{}
	modelRuntimeImages map[string]struct{}
	h3RuntimeImages    map[string]struct{}
	residencyRollouts  []fleetcontroller.ResidencyPlanRollout
}

func newRenderInventory() renderInventory {
	return renderInventory{
		declared:           make(map[resourceKey]struct{}),
		referred:           make(map[resourceKey]struct{}),
		expectedRevision:   make(map[resourceKey]string),
		secretKeys:         make(map[resourceKey]map[string]struct{}),
		secretConsumers:    make(map[resourceKey]map[string]struct{}),
		images:             make(map[string]struct{}),
		supportImages:      make(map[string]struct{}),
		modelRuntimeImages: make(map[string]struct{}),
		h3RuntimeImages:    make(map[string]struct{}),
	}
}

func build(root *rootedFS, plan BuildPlan, sourceRevision string) (Bundle, error) {
	if !sourceRevisionPattern.MatchString(sourceRevision) {
		return Bundle{}, invalid("source revision must be a full Git object ID")
	}
	if plan.RenderContract != "" && plan.RenderContract != KubernetesRenderContractV2 {
		return Bundle{}, invalid("unsupported render contract")
	}
	if plan.SchemaVersion != SchemaVersion || len(plan.FinalRenders) != len(fixedRenderNames) ||
		len(plan.Packages) != len(fixedPackageNames) || len(plan.OCIManifests) == 0 ||
		len(plan.OCIManifests) > maxArtifactCount {
		return Bundle{}, invalid("build plan graph cardinality is invalid")
	}
	artifacts, err := preflightArtifactGraph(root, plan)
	if err != nil {
		return Bundle{}, invalidf("preflight artifact graph: %v", err)
	}
	yamlBudget := &yamlGraphBudget{}
	slices.SortFunc(plan.FinalRenders, func(left, right ArtifactInput) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(plan.Packages, func(left, right PackageInput) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(plan.ExternalResources, compareExternalResources)
	slices.SortFunc(plan.OCIManifests, func(left, right OCIManifestInput) int {
		return strings.Compare(left.Image, right.Image)
	})

	inventory := newRenderInventory()
	configuration := ConfigurationManifest{
		SchemaVersion: SchemaVersion, MediaType: ConfigurationMediaType, SourceRevision: sourceRevision,
		RenderContract:    plan.RenderContract,
		FinalRenders:      make([]NamedArtifact, 0, len(plan.FinalRenders)),
		Packages:          make([]Package, 0, len(plan.Packages)),
		ExternalResources: append([]ExternalResource(nil), plan.ExternalResources...),
	}
	for index, input := range plan.FinalRenders {
		if input.Name != fixedRenderNames[index] {
			return Bundle{}, invalid("final render names must be the exact fixed set")
		}
		artifact, content, err := artifacts.artifactFor(input.Ref, "application/yaml", maxYAMLArtifactBytes)
		if err != nil {
			return Bundle{}, invalidf("read final render %s: %v", input.Name, err)
		}
		if err := validateFinalRenderWithContract(plan.RenderContract, input.Name, content, &inventory, yamlBudget); err != nil {
			return Bundle{}, err
		}
		configuration.FinalRenders = append(configuration.FinalRenders, NamedArtifact{Name: input.Name, Artifact: artifact})
	}

	if plan.NodeAgentUnit.Name != "node-agent-systemd-unit" {
		return Bundle{}, invalid("node agent unit name must be node-agent-systemd-unit")
	}
	unitArtifact, unitContent, err := artifacts.artifactFor(plan.NodeAgentUnit.Ref, "text/plain", maxMetadataBytes)
	if err != nil {
		return Bundle{}, invalidf("read node Agent systemd unit: %v", err)
	}
	configuration.NodeAgentUnit = NamedArtifact{Name: plan.NodeAgentUnit.Name, Artifact: unitArtifact}
	if plan.RuntimeImageMaintenanceUnit.Name != "runtime-image-maintenance-systemd-unit" {
		return Bundle{}, invalid("runtime image maintenance unit name must be runtime-image-maintenance-systemd-unit")
	}
	maintenanceArtifact, maintenanceContent, err := artifacts.artifactFor(plan.RuntimeImageMaintenanceUnit.Ref, "text/plain", maxMetadataBytes)
	if err != nil {
		return Bundle{}, invalidf("read runtime image maintenance systemd unit: %v", err)
	}
	configuration.RuntimeImageMaintenanceUnit = NamedArtifact{Name: plan.RuntimeImageMaintenanceUnit.Name, Artifact: maintenanceArtifact}

	var nodeAgentEntrypoint string
	for index, input := range plan.Packages {
		if input.Name != fixedPackageNames[index] {
			return Bundle{}, invalid("package names must be the exact fixed set")
		}
		contractArtifact, contractContent, err := artifacts.artifactFor(input.ContractRef, "application/json", maxMetadataBytes)
		if err != nil {
			return Bundle{}, invalidf("read %s package contract: %v", input.Name, err)
		}
		packageArtifact, err := artifacts.digestArtifact(input.ArtifactRef, "application/octet-stream", maxPackageBytes)
		if err != nil {
			return Bundle{}, invalidf("read %s package artifact: %v", input.Name, err)
		}
		contract, err := ValidatePackageContract(input.Name, contractContent, packageArtifact)
		if err != nil {
			return Bundle{}, err
		}
		if input.Name == "node-agent" {
			nodeAgentEntrypoint = contract.Entrypoint
		}
		configuration.Packages = append(configuration.Packages, Package{
			Name: input.Name, Contract: contractArtifact, Artifact: packageArtifact,
		})
	}
	if err := validateSystemdUnit(unitContent, nodeAgentEntrypoint, nodeAgentSystemdV1); err != nil {
		return Bundle{}, err
	}
	if err := validateSystemdUnit(maintenanceContent, nodeAgentEntrypoint+" runtime-image-maintenance --config-file /etc/vela/runtime-image-maintenance.json", runtimeImageMaintenanceSystemdV1); err != nil {
		return Bundle{}, invalidf("runtime image maintenance unit: %v", err)
	}
	if plan.RuntimeStartup != nil {
		runtimeStartup, err := buildRuntimeStartupArtifacts(artifacts, *plan.RuntimeStartup)
		if err != nil {
			return Bundle{}, err
		}
		configuration.RuntimeStartup = runtimeStartup
	}

	if len(inventory.residencyRollouts) == 0 {
		return Bundle{}, invalid("Fleet final render must contain target ResidencyPlan rollout authority")
	}
	if err := validateExternalResources(plan.ExternalResources, inventory); err != nil {
		return Bundle{}, err
	}
	if err := validateImageRoleSeparation(inventory); err != nil {
		return Bundle{}, err
	}

	ociImages := make([]OCIImage, 0, len(plan.OCIManifests))
	seenImages := make(map[string]struct{}, len(plan.OCIManifests))
	for _, input := range plan.OCIManifests {
		if _, duplicate := seenImages[input.Image]; duplicate {
			return Bundle{}, invalidf("OCI image %q is duplicated", input.Image)
		}
		seenImages[input.Image] = struct{}{}
		artifact, content, err := artifacts.artifactFor(input.Ref, "", maxMetadataBytes)
		if err != nil {
			return Bundle{}, invalidf("read OCI manifest for %s: %v", input.Image, err)
		}
		configArtifact, configContent, err := artifacts.artifactFor(input.ConfigRef, OCIImageConfigMediaType, maxMetadataBytes)
		if err != nil {
			return Bundle{}, invalidf("read OCI config for %s: %v", input.Image, err)
		}
		mediaType, platform, err := validateOCIManifest(input, artifact, content, configArtifact, configContent)
		if err != nil {
			return Bundle{}, err
		}
		if _, modelRuntime := inventory.modelRuntimeImages[input.Image]; modelRuntime {
			_, h3Runtime := inventory.h3RuntimeImages[input.Image]
			if err := validateModelRuntimeOCIConfig(input.Image, configContent, h3Runtime); err != nil {
				return Bundle{}, err
			}
		}
		artifact.MediaType = mediaType
		ociImages = append(ociImages, OCIImage{Image: input.Image, Descriptor: artifact, Config: configArtifact, Platform: platform})
	}
	if !reflect.DeepEqual(inventory.images, seenImages) {
		return Bundle{}, invalidf("OCI manifest set does not exactly match rendered image set: rendered=%v supplied=%v", sortedKeys(inventory.images), sortedKeys(seenImages))
	}
	configurationRevision, configurationSize, err := canonicalDigest(configuration)
	if err != nil {
		return Bundle{}, invalidf("digest configuration manifest: %v", err)
	}
	release := ReleaseDescriptor{
		SchemaVersion: 2, MediaType: ReleaseDescriptorMediaType, ArtifactType: ReleaseArtifactType,
		Config:    Descriptor{MediaType: ConfigurationMediaType, Digest: configurationRevision, Size: configurationSize},
		Manifests: make([]Descriptor, 0, len(ociImages)),
	}
	for _, image := range ociImages {
		platform := image.Platform
		release.Manifests = append(release.Manifests, Descriptor{
			MediaType: image.Descriptor.MediaType, Digest: image.Descriptor.Digest,
			Size: image.Descriptor.SizeBytes, Platform: &platform,
			Annotations: map[string]string{"org.opencontainers.image.ref.name": image.Image},
		})
	}
	releaseDigest, _, err := canonicalDigest(release)
	if err != nil {
		return Bundle{}, invalidf("digest release descriptor: %v", err)
	}
	return Bundle{
		SchemaVersion: SchemaVersion, ReleaseDigest: releaseDigest,
		ConfigurationRevision: configurationRevision, ConfigurationManifest: configuration,
		ReleaseDescriptor: release, OCIImages: ociImages,
	}, nil
}

func buildRuntimeStartupArtifacts(artifacts *artifactReader, plan RuntimeStartupPlan) (*RuntimeStartupManifest, error) {
	if artifacts == nil || len(plan.Packages) != len(fixedRuntimeStartupPackageNames) {
		return nil, invalid("runtime startup package set is incomplete")
	}
	packages := make([]Package, 0, len(plan.Packages))
	entrypoints := make(map[string]string, len(plan.Packages))
	for index, input := range plan.Packages {
		if input.Name != fixedRuntimeStartupPackageNames[index] {
			return nil, invalid("runtime startup package names must be the exact fixed set")
		}
		contractArtifact, contractContent, err := artifacts.artifactFor(input.ContractRef, "application/json", maxMetadataBytes)
		if err != nil {
			return nil, invalidf("read %s runtime startup package contract: %v", input.Name, err)
		}
		packageArtifact, err := artifacts.digestArtifact(input.ArtifactRef, "application/octet-stream", maxPackageBytes)
		if err != nil {
			return nil, invalidf("read %s runtime startup package artifact: %v", input.Name, err)
		}
		contract, err := ValidatePackageContract(input.Name, contractContent, packageArtifact)
		if err != nil {
			return nil, err
		}
		packages = append(packages, Package{Name: input.Name, Contract: contractArtifact, Artifact: packageArtifact})
		entrypoints[input.Name] = contract.Entrypoint
	}
	if plan.PIDFDBrokerUnit.Name != runtimeStartupPIDFDBrokerUnitName || plan.RuntimePolicyIssuerUnit.Name != runtimeStartupPolicyIssuerUnitName ||
		plan.PIDFDBrokerEnv.Name != runtimeStartupPIDFDBrokerEnvName || plan.RuntimePolicyIssuerEnv.Name != runtimeStartupPolicyIssuerEnvName {
		return nil, invalid("runtime startup artifact names are invalid")
	}
	brokerArtifact, brokerContent, err := artifacts.artifactFor(plan.PIDFDBrokerUnit.Ref, "text/plain", maxMetadataBytes)
	if err != nil {
		return nil, invalidf("read pidfd broker systemd unit: %v", err)
	}
	issuerArtifact, issuerContent, err := artifacts.artifactFor(plan.RuntimePolicyIssuerUnit.Ref, "text/plain", maxMetadataBytes)
	if err != nil {
		return nil, invalidf("read runtime policy issuer systemd unit: %v", err)
	}
	if err := ValidateRuntimeStartupSystemdUnit("pidfd-broker", brokerContent, entrypoints["pidfd-broker"]); err != nil {
		return nil, invalidf("pidfd broker systemd unit: %v", err)
	}
	if err := ValidateRuntimeStartupSystemdUnit("runtime-policy-issuer", issuerContent, entrypoints["runtime-policy-issuer"]); err != nil {
		return nil, invalidf("runtime policy issuer systemd unit: %v", err)
	}
	brokerEnvArtifact, brokerEnvContent, err := artifacts.artifactFor(plan.PIDFDBrokerEnv.Ref, "text/plain", maxMetadataBytes)
	if err != nil {
		return nil, invalidf("read pidfd broker env example: %v", err)
	}
	if err := ValidateRuntimeStartupEnvExample(string(brokerEnvContent), "pidfd-broker"); err != nil {
		return nil, err
	}
	issuerEnvArtifact, issuerEnvContent, err := artifacts.artifactFor(plan.RuntimePolicyIssuerEnv.Ref, "text/plain", maxMetadataBytes)
	if err != nil {
		return nil, invalidf("read runtime policy issuer env example: %v", err)
	}
	if err := ValidateRuntimeStartupEnvExample(string(issuerEnvContent), "runtime-policy-issuer"); err != nil {
		return nil, err
	}
	var provisioning *NamedArtifact
	if plan.Provisioning.Ref != "" || plan.Provisioning.Name != "" {
		if plan.Provisioning.Name != "runtime-startup-provisioning" || plan.Provisioning.Ref == "" {
			return nil, invalid("runtime startup provisioning artifact is invalid")
		}
		artifact, content, err := artifacts.artifactFor(plan.Provisioning.Ref, "application/json", maxMetadataBytes)
		if err != nil {
			return nil, invalidf("read runtime startup provisioning contract: %v", err)
		}
		var contract RuntimeStartupProvisioningContract
		if err := decodeStrictJSON(content, &contract); err != nil {
			return nil, invalid("runtime startup provisioning contract is invalid")
		}
		if err := ValidateRuntimeStartupProvisioningContract(contract); err != nil {
			return nil, invalid("runtime startup provisioning contract is invalid")
		}
		provisioning = &NamedArtifact{Name: plan.Provisioning.Name, Artifact: artifact}
	}
	return &RuntimeStartupManifest{
		Packages:                packages,
		PIDFDBrokerUnit:         NamedArtifact{Name: plan.PIDFDBrokerUnit.Name, Artifact: brokerArtifact},
		RuntimePolicyIssuerUnit: NamedArtifact{Name: plan.RuntimePolicyIssuerUnit.Name, Artifact: issuerArtifact},
		PIDFDBrokerEnv:          NamedArtifact{Name: plan.PIDFDBrokerEnv.Name, Artifact: brokerEnvArtifact},
		RuntimePolicyIssuerEnv:  NamedArtifact{Name: plan.RuntimePolicyIssuerEnv.Name, Artifact: issuerEnvArtifact},
		Provisioning:            provisioning,
	}, nil
}

func validAbsoluteContractPath(path string) bool {
	return path != "" && filepath.IsAbs(path) && filepath.Clean(path) == path && !strings.ContainsRune(path, '\x00')
}

// ValidateRuntimeStartupProvisioningContract validates the filesystem and
// environment inputs required before the Node startup composition can run.
func ValidateRuntimeStartupProvisioningContract(contract RuntimeStartupProvisioningContract) error {
	if contract.SchemaVersion != 1 || !validAbsoluteContractPath(contract.SocketParent) ||
		!validAbsoluteContractPath(contract.BootstrapDirectory) || len(contract.RequiredFiles) == 0 || len(contract.RequiredEnvKeys) == 0 {
		return errors.New("runtime startup provisioning contract is invalid")
	}
	for index, path := range contract.RequiredFiles {
		if !validAbsoluteContractPath(path) || slices.Contains(contract.RequiredFiles[:index], path) {
			return errors.New("runtime startup provisioning required files are invalid")
		}
	}
	for index, key := range contract.RequiredEnvKeys {
		if !strings.HasPrefix(key, "VELA_") || strings.ContainsAny(key, "= \t\n\r") || slices.Contains(contract.RequiredEnvKeys[:index], key) {
			return errors.New("runtime startup provisioning required env keys are invalid")
		}
	}
	return nil
}

func validateImageRoleSeparation(inventory renderInventory) error {
	runtimeDigests := make(map[string]string, len(inventory.modelRuntimeImages))
	for image := range inventory.modelRuntimeImages {
		if _, reused := inventory.supportImages[image]; reused {
			return invalidf("ModelRuntime image %q is also used by a non-ModelRuntime container", image)
		}
		runtimeDigests[imageManifestDigest(image)] = image
	}
	for image := range inventory.supportImages {
		if runtimeImage, reused := runtimeDigests[imageManifestDigest(image)]; reused {
			return invalidf(
				"ModelRuntime image %q and non-ModelRuntime image %q resolve to the same OCI manifest",
				runtimeImage,
				image,
			)
		}
	}
	return nil
}

func imageManifestDigest(image string) string {
	return image[strings.LastIndex(image, "@")+1:]
}

func verify(root *rootedFS, bundle Bundle) error {
	if bundle.SchemaVersion != SchemaVersion || !validDigest(bundle.ReleaseDigest) ||
		!validDigest(bundle.ConfigurationRevision) ||
		bundle.ConfigurationManifest.SchemaVersion != SchemaVersion ||
		bundle.ConfigurationManifest.MediaType != ConfigurationMediaType ||
		!sourceRevisionPattern.MatchString(bundle.ConfigurationManifest.SourceRevision) {
		return invalid("bundle header is invalid")
	}
	plan := BuildPlan{
		SchemaVersion:     SchemaVersion,
		RenderContract:    bundle.ConfigurationManifest.RenderContract,
		ExternalResources: append([]ExternalResource(nil), bundle.ConfigurationManifest.ExternalResources...),
	}
	for _, render := range bundle.ConfigurationManifest.FinalRenders {
		plan.FinalRenders = append(plan.FinalRenders, ArtifactInput{Name: render.Name, Ref: render.Artifact.Ref})
	}
	plan.NodeAgentUnit = ArtifactInput{
		Name: bundle.ConfigurationManifest.NodeAgentUnit.Name,
		Ref:  bundle.ConfigurationManifest.NodeAgentUnit.Artifact.Ref,
	}
	plan.RuntimeImageMaintenanceUnit = ArtifactInput{
		Name: bundle.ConfigurationManifest.RuntimeImageMaintenanceUnit.Name,
		Ref:  bundle.ConfigurationManifest.RuntimeImageMaintenanceUnit.Artifact.Ref,
	}
	if startup := bundle.ConfigurationManifest.RuntimeStartup; startup != nil {
		plan.RuntimeStartup = &RuntimeStartupPlan{
			PIDFDBrokerUnit:         ArtifactInput{Name: startup.PIDFDBrokerUnit.Name, Ref: startup.PIDFDBrokerUnit.Artifact.Ref},
			RuntimePolicyIssuerUnit: ArtifactInput{Name: startup.RuntimePolicyIssuerUnit.Name, Ref: startup.RuntimePolicyIssuerUnit.Artifact.Ref},
			PIDFDBrokerEnv:          ArtifactInput{Name: startup.PIDFDBrokerEnv.Name, Ref: startup.PIDFDBrokerEnv.Artifact.Ref},
			RuntimePolicyIssuerEnv:  ArtifactInput{Name: startup.RuntimePolicyIssuerEnv.Name, Ref: startup.RuntimePolicyIssuerEnv.Artifact.Ref},
		}
		if startup.Provisioning != nil {
			plan.RuntimeStartup.Provisioning = ArtifactInput{Name: startup.Provisioning.Name, Ref: startup.Provisioning.Artifact.Ref}
		}
		for _, item := range startup.Packages {
			plan.RuntimeStartup.Packages = append(plan.RuntimeStartup.Packages, PackageInput{
				Name: item.Name, ContractRef: item.Contract.Ref, ArtifactRef: item.Artifact.Ref,
			})
		}
	}
	for _, item := range bundle.ConfigurationManifest.Packages {
		plan.Packages = append(plan.Packages, PackageInput{
			Name: item.Name, ContractRef: item.Contract.Ref, ArtifactRef: item.Artifact.Ref,
		})
	}
	for _, image := range bundle.OCIImages {
		plan.OCIManifests = append(plan.OCIManifests, OCIManifestInput{
			Image: image.Image, Ref: image.Descriptor.Ref, ConfigRef: image.Config.Ref,
		})
	}
	rebuilt, err := build(root, plan, bundle.ConfigurationManifest.SourceRevision)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(rebuilt, bundle) {
		return invalid("bundle is not the canonical artifact graph for its referenced bytes")
	}
	return nil
}

func invalid(message string) error {
	return fmt.Errorf("%w: %s", ErrInvalidBundle, message)
}

func invalidf(format string, arguments ...any) error {
	return invalid(fmt.Sprintf(format, arguments...))
}

func compareExternalResources(left, right ExternalResource) int {
	if comparison := strings.Compare(left.Kind, right.Kind); comparison != 0 {
		return comparison
	}
	if comparison := strings.Compare(left.Namespace, right.Namespace); comparison != 0 {
		return comparison
	}
	return strings.Compare(left.Name, right.Name)
}

func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func validDigest(value string) bool {
	return len(value) == len("sha256:")+64 && strings.HasPrefix(value, "sha256:") &&
		value != "sha256:"+strings.Repeat("0", 64) && isLowerHex(strings.TrimPrefix(value, "sha256:"))
}

// ValidRevision reports whether value is safe for a release identity or contract field.
func ValidRevision(value string) bool {
	lower := strings.ToLower(value)
	return value != "" && len(value) <= 300 && strings.TrimSpace(value) == value &&
		!strings.ContainsAny(value, "\x00\r\n") && !strings.Contains(lower, "placeholder") &&
		!strings.Contains(lower, "replace-with") && !strings.Contains(lower, "changeme") &&
		!strings.Contains(lower, "todo") && !strings.Contains(lower, ".invalid") &&
		value != strings.Repeat("0", 64) && value != "sha256:"+strings.Repeat("0", 64)
}

func validResourceName(value string) bool {
	return value != "" && len(validation.IsDNS1123Subdomain(value)) == 0 && ValidRevision(value)
}

func isLowerHex(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

// ValidatePackageContract strictly validates one host package contract against its artifact.
func ValidatePackageContract(name string, encoded []byte, artifact Artifact) (PackageContract, error) {
	var contract PackageContract
	if err := decodeStrictJSON(encoded, &contract); err != nil {
		return PackageContract{}, invalidf("decode %s package contract: %v", name, err)
	}
	wantName := "vela-" + name
	if contract.SchemaVersion != 1 || contract.Name != wantName || contract.OS != "linux" ||
		contract.Architecture != "amd64" || !ValidRevision(contract.Revision) ||
		!filepath.IsAbs(contract.Entrypoint) || !ValidRevision(contract.Entrypoint) ||
		contract.ArtifactDigest != artifact.Digest || contract.ArtifactSizeBytes != artifact.SizeBytes {
		return PackageContract{}, invalidf("%s package contract does not bind the linux/amd64 artifact", name)
	}
	return contract, nil
}

func validateSystemdUnit(encoded []byte, entrypoint string, contract map[string]map[string]string) error {
	if entrypoint == "" || containsTemplateValue(string(encoded)) {
		return invalid("node agent systemd unit is not bound to the package entrypoint and hardening contract")
	}
	seenSections := make(map[string]struct{}, len(contract))
	seenDirectives := make(map[string]struct{})
	section := ""
	for lineNumber, raw := range strings.Split(string(encoded), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasSuffix(line, `\`) {
			return invalidf("node agent systemd unit line %d uses a continuation", lineNumber+1)
		}
		if strings.HasPrefix(line, "[") || strings.HasSuffix(line, "]") {
			if len(line) < 3 || !strings.HasPrefix(line, "[") || !strings.HasSuffix(line, "]") {
				return invalidf("node agent systemd unit line %d has a malformed section", lineNumber+1)
			}
			section = strings.TrimSuffix(strings.TrimPrefix(line, "["), "]")
			if _, allowed := contract[section]; !allowed {
				return invalidf("node agent systemd unit contains unknown section %q", section)
			}
			if _, duplicate := seenSections[section]; duplicate {
				return invalidf("node agent systemd unit repeats section %q", section)
			}
			seenSections[section] = struct{}{}
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found || section == "" || key == "" || strings.TrimSpace(key) != key {
			return invalidf("node agent systemd unit line %d is malformed", lineNumber+1)
		}
		expected, allowed := contract[section][key]
		if !allowed {
			return invalidf("node agent systemd unit contains unknown directive %s.%s", section, key)
		}
		identity := section + "." + key
		if _, duplicate := seenDirectives[identity]; duplicate {
			return invalidf("node agent systemd unit repeats directive %s", identity)
		}
		seenDirectives[identity] = struct{}{}
		if key == "ExecStart" {
			expected = entrypoint
		}
		if value != expected {
			return invalidf("node agent systemd unit directive %s does not match version 1", identity)
		}
	}
	for expectedSection, directives := range contract {
		if _, present := seenSections[expectedSection]; !present {
			return invalidf("node agent systemd unit is missing section %q", expectedSection)
		}
		for key := range directives {
			if _, present := seenDirectives[expectedSection+"."+key]; !present {
				return invalidf("node agent systemd unit is missing directive %s.%s", expectedSection, key)
			}
		}
	}
	return nil
}

// ValidateRuntimeStartupSystemdUnit applies the canonical unit contract used
// by both release assembly and the standalone runtime-startup package builder.
func ValidateRuntimeStartupSystemdUnit(component string, encoded []byte, entrypoint string) error {
	var contract map[string]map[string]string
	switch component {
	case "pidfd-broker":
		contract = runtimeStartupBrokerSystemdV1
	case "runtime-policy-issuer":
		contract = runtimeStartupIssuerSystemdV1
	default:
		return invalidf("unknown runtime startup systemd component %q", component)
	}
	return validateSystemdUnit(encoded, entrypoint, contract)
}

func containsTemplateValue(value string) bool {
	lower := strings.ToLower(value)
	return strings.Contains(lower, "placeholder") || strings.Contains(lower, "replace-with") ||
		strings.Contains(lower, "changeme") || strings.Contains(lower, "todo") ||
		strings.Contains(lower, ".invalid") || strings.Contains(value, "sha256:"+strings.Repeat("0", 64)) ||
		value == strings.Repeat("0", 64)
}

// ValidateRuntimeStartupEnvExample validates one canonical runtime startup
// environment example before it is published or embedded in a release.
func ValidateRuntimeStartupEnvExample(content, component string) error {
	var required []string
	switch component {
	case "pidfd-broker":
		required = []string{"VELA_PIDFD_BROKER_SOCKET", "VELA_PIDFD_BROKER_RUNTIME_GID"}
	case "runtime-policy-issuer":
		required = []string{
			"VELA_RUNTIME_POLICY_PRIVATE_KEY_FILE", "VELA_RUNTIME_POLICY_AUTHORIZATION_PUBLIC_KEY_FILE",
			"VELA_RUNTIME_POLICY_AUTHORIZATION_DIRECTORY", "VELA_RUNTIME_POLICY_REPLY_CACHE_DIRECTORY",
			"VELA_RUNTIME_POLICY_ISSUER_SOCKET", "VELA_RUNTIME_POLICY_RUNTIME_GID",
		}
	default:
		return invalidf("unknown runtime startup env component %q", component)
	}
	values := make(map[string]string, len(required))
	for lineNumber, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok || !slices.Contains(required, key) || strings.TrimSpace(value) == "" {
			return invalidf("runtime startup %s env example line %d is invalid", component, lineNumber+1)
		}
		if _, duplicate := values[key]; duplicate {
			return invalidf("runtime startup %s env example repeats %s", component, key)
		}
		values[key] = value
	}
	for _, key := range required {
		if _, present := values[key]; !present {
			return invalidf("runtime startup %s env example is missing %s", component, key)
		}
	}
	return nil
}

type yamlGraphBudget struct {
	documents int
	nodes     int
}

func (budget *yamlGraphBudget) consume(documents, nodes int) error {
	if documents > maxYAMLGraphDocuments-budget.documents {
		return fmt.Errorf("YAML graph exceeds %d documents", maxYAMLGraphDocuments)
	}
	if nodes > maxYAMLGraphNodes-budget.nodes {
		return fmt.Errorf("YAML graph exceeds %d nodes", maxYAMLGraphNodes)
	}
	budget.documents += documents
	budget.nodes += nodes
	return nil
}

func decodeYAMLDocuments(encoded []byte, budget *yamlGraphBudget) ([]map[string]any, error) {
	nodes, err := decodeBoundedYAMLNodes(encoded, budget)
	if err != nil {
		return nil, err
	}
	documents := make([]map[string]any, 0, len(nodes))
	for _, node := range nodes {
		if len(node.Content) == 0 {
			continue
		}
		var document map[string]any
		if err := node.Decode(&document); err != nil {
			return nil, err
		}
		if len(document) != 0 {
			documents = append(documents, document)
		}
	}
	return documents, nil
}

func decodeSingleYAMLNode(encoded []byte, budget *yamlGraphBudget) (*yaml.Node, error) {
	documents, err := decodeBoundedYAMLNodes(encoded, budget)
	if err != nil {
		return nil, err
	}
	if len(documents) != 1 || len(documents[0].Content) == 0 {
		return nil, errors.New("YAML input must contain exactly one non-empty document")
	}
	return documents[0], nil
}

func decodeBoundedYAMLNodes(encoded []byte, budget *yamlGraphBudget) ([]*yaml.Node, error) {
	if len(encoded) == 0 || len(encoded) > maxYAMLArtifactBytes {
		return nil, fmt.Errorf("YAML input must be in 1..%d bytes", maxYAMLArtifactBytes)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	documents := make([]*yaml.Node, 0)
	nodeCount := 0
	aliasCount := 0
	for {
		var document yaml.Node
		err := decoder.Decode(&document)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(documents) == maxYAMLDocuments {
			return nil, fmt.Errorf("YAML input exceeds %d documents", maxYAMLDocuments)
		}
		priorNodeCount := nodeCount
		if err := validateYAMLNodeBounds(&document, 1, &nodeCount, &aliasCount); err != nil {
			return nil, err
		}
		if err := budget.consume(1, nodeCount-priorNodeCount); err != nil {
			return nil, err
		}
		documents = append(documents, &document)
	}
	return documents, nil
}

func validateYAMLNodeBounds(node *yaml.Node, depth int, nodeCount, aliasCount *int) error {
	(*nodeCount)++
	if *nodeCount > maxYAMLNodes {
		return fmt.Errorf("YAML input exceeds %d nodes", maxYAMLNodes)
	}
	if depth > maxYAMLDepth {
		return fmt.Errorf("YAML input exceeds depth %d", maxYAMLDepth)
	}
	if node.Kind == yaml.AliasNode {
		(*aliasCount)++
		if *aliasCount > maxYAMLAliases {
			return fmt.Errorf("YAML input exceeds %d aliases", maxYAMLAliases)
		}
	}
	for _, child := range node.Content {
		if err := validateYAMLNodeBounds(child, depth+1, nodeCount, aliasCount); err != nil {
			return err
		}
	}
	return nil
}
