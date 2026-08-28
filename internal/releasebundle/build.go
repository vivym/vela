package releasebundle

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
	"k8s.io/apimachinery/pkg/util/validation"
)

var (
	fixedRenderNames = []string{
		"control-storage", "fleet-controller", "observability", "vela-control", "worker-agent",
	}
	fixedPackageNames = []string{"h3-runner", "node-agent"}
	imagePattern      = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]*(?:/[a-z0-9][a-z0-9._-]*)+@sha256:[0-9a-f]{64}$`)
	gpuPattern        = regexp.MustCompile(`^GPU-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

type resourceKey struct {
	Kind      string
	Namespace string
	Name      string
}

type objectKey struct {
	APIVersion string
	Kind       string
	Namespace  string
	Name       string
}

type renderInventory struct {
	declared         map[resourceKey]struct{}
	referred         map[resourceKey]struct{}
	expectedRevision map[resourceKey]string
	secretKeys       map[resourceKey]map[string]struct{}
	secretConsumers  map[resourceKey]map[string]struct{}
	objects          map[objectKey]string
	images           map[string]struct{}
}

func newRenderInventory() renderInventory {
	return renderInventory{
		declared:         make(map[resourceKey]struct{}),
		referred:         make(map[resourceKey]struct{}),
		expectedRevision: make(map[resourceKey]string),
		secretKeys:       make(map[resourceKey]map[string]struct{}),
		secretConsumers:  make(map[resourceKey]map[string]struct{}),
		objects:          make(map[objectKey]string),
		images:           make(map[string]struct{}),
	}
}

func build(root *os.Root, plan BuildPlan) (Bundle, error) {
	if plan.SchemaVersion != SchemaVersion || len(plan.FinalRenders) != len(fixedRenderNames) ||
		len(plan.Packages) != len(fixedPackageNames) || len(plan.WorkerMaterializations) == 0 ||
		len(plan.WorkerMaterializations) > maxWorkerNodeCount || len(plan.OCIManifests) == 0 ||
		len(plan.OCIManifests) > maxArtifactCount {
		return Bundle{}, invalid("build plan graph cardinality is invalid")
	}
	slices.SortFunc(plan.FinalRenders, func(left, right ArtifactInput) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(plan.Packages, func(left, right PackageInput) int {
		return strings.Compare(left.Name, right.Name)
	})
	slices.SortFunc(plan.WorkerMaterializations, func(left, right WorkerMaterializationInput) int {
		return strings.Compare(left.NodeIdentity, right.NodeIdentity)
	})
	slices.SortFunc(plan.ExternalResources, compareExternalResources)
	slices.SortFunc(plan.OCIManifests, func(left, right OCIManifestInput) int {
		return strings.Compare(left.Image, right.Image)
	})

	inventory := newRenderInventory()
	configuration := ConfigurationManifest{
		SchemaVersion: SchemaVersion, MediaType: ConfigurationMediaType,
		FinalRenders:           make([]NamedArtifact, 0, len(plan.FinalRenders)),
		Packages:               make([]Package, 0, len(plan.Packages)),
		WorkerMaterializations: make([]WorkerMaterialization, 0, len(plan.WorkerMaterializations)),
		ExternalResources:      append([]ExternalResource(nil), plan.ExternalResources...),
	}
	for index, input := range plan.FinalRenders {
		if input.Name != fixedRenderNames[index] {
			return Bundle{}, invalid("final render names must be the exact fixed set")
		}
		artifact, content, err := artifactFor(root, input.Ref, "application/yaml", maxMetadataBytes)
		if err != nil {
			return Bundle{}, invalidf("read final render %s: %v", input.Name, err)
		}
		if err := validateFinalRender(input.Name, content, &inventory); err != nil {
			return Bundle{}, err
		}
		configuration.FinalRenders = append(configuration.FinalRenders, NamedArtifact{Name: input.Name, Artifact: artifact})
	}

	if plan.NodeAgentUnit.Name != "node-agent-systemd-unit" {
		return Bundle{}, invalid("node Agent unit name must be node-agent-systemd-unit")
	}
	unitArtifact, unitContent, err := artifactFor(root, plan.NodeAgentUnit.Ref, "text/plain", maxMetadataBytes)
	if err != nil {
		return Bundle{}, invalidf("read node Agent systemd unit: %v", err)
	}
	configuration.NodeAgentUnit = NamedArtifact{Name: plan.NodeAgentUnit.Name, Artifact: unitArtifact}

	var nodeAgentEntrypoint string
	for index, input := range plan.Packages {
		if input.Name != fixedPackageNames[index] {
			return Bundle{}, invalid("package names must be the exact fixed set")
		}
		contractArtifact, contractContent, err := artifactFor(root, input.ContractRef, "application/json", maxMetadataBytes)
		if err != nil {
			return Bundle{}, invalidf("read %s package contract: %v", input.Name, err)
		}
		packageArtifact, _, err := artifactFor(root, input.ArtifactRef, "application/octet-stream", maxPackageBytes)
		if err != nil {
			return Bundle{}, invalidf("read %s package artifact: %v", input.Name, err)
		}
		contract, err := validatePackageContract(input.Name, contractContent, packageArtifact)
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
	if err := validateSystemdUnit(unitContent, nodeAgentEntrypoint); err != nil {
		return Bundle{}, err
	}

	materialKeys := make(map[string]struct{})
	for _, input := range plan.WorkerMaterializations {
		materialization, err := buildWorkerMaterialization(root, input, &inventory, materialKeys)
		if err != nil {
			return Bundle{}, err
		}
		configuration.WorkerMaterializations = append(configuration.WorkerMaterializations, materialization)
	}
	if err := validateExternalResources(plan.ExternalResources, inventory); err != nil {
		return Bundle{}, err
	}

	ociImages := make([]OCIImage, 0, len(plan.OCIManifests))
	seenImages := make(map[string]struct{}, len(plan.OCIManifests))
	for _, input := range plan.OCIManifests {
		if _, duplicate := seenImages[input.Image]; duplicate {
			return Bundle{}, invalidf("OCI image %q is duplicated", input.Image)
		}
		seenImages[input.Image] = struct{}{}
		artifact, content, err := artifactFor(root, input.Ref, "", maxMetadataBytes)
		if err != nil {
			return Bundle{}, invalidf("read OCI manifest for %s: %v", input.Image, err)
		}
		mediaType, err := validateOCIManifest(input, artifact, content)
		if err != nil {
			return Bundle{}, err
		}
		artifact.MediaType = mediaType
		ociImages = append(ociImages, OCIImage{Image: input.Image, Descriptor: artifact, Platform: input.Platform})
	}
	if !reflect.DeepEqual(inventory.images, seenImages) {
		return Bundle{}, invalidf("OCI manifest set does not exactly match rendered image set: rendered=%v supplied=%v", sortedKeys(inventory.images), sortedKeys(seenImages))
	}
	if err := validateUniqueArtifactReferences(configuration, ociImages); err != nil {
		return Bundle{}, err
	}

	configurationRevision, configurationSize, err := canonicalDigest(configuration)
	if err != nil {
		return Bundle{}, invalidf("digest configuration manifest: %v", err)
	}
	release := ReleaseDescriptor{
		SchemaVersion: 2, MediaType: OCIIndexMediaType, ArtifactType: ReleaseArtifactType,
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

func validateUniqueArtifactReferences(configuration ConfigurationManifest, images []OCIImage) error {
	seen := make(map[string]string)
	var totalBytes int64
	claim := func(role string, artifact Artifact) error {
		if prior, duplicate := seen[artifact.Ref]; duplicate {
			return invalidf("artifact reference %q is shared by %s and %s", artifact.Ref, prior, role)
		}
		seen[artifact.Ref] = role
		if artifact.SizeBytes <= 0 || totalBytes > maxArtifactBytes-artifact.SizeBytes {
			return invalidf("artifact graph exceeds %d bytes", maxArtifactBytes)
		}
		totalBytes += artifact.SizeBytes
		if len(seen) > maxArtifactCount {
			return invalidf("artifact graph exceeds %d entries", maxArtifactCount)
		}
		return nil
	}
	for _, render := range configuration.FinalRenders {
		if err := claim("render/"+render.Name, render.Artifact); err != nil {
			return err
		}
	}
	if err := claim("node-agent-unit", configuration.NodeAgentUnit.Artifact); err != nil {
		return err
	}
	for _, item := range configuration.Packages {
		if err := claim("package-contract/"+item.Name, item.Contract); err != nil {
			return err
		}
		if err := claim("package/"+item.Name, item.Artifact); err != nil {
			return err
		}
	}
	for _, item := range configuration.WorkerMaterializations {
		for role, artifact := range map[string]Artifact{
			"worker-runtime/" + item.NodeIdentity:   item.WorkerRuntime,
			"runner-profiles/" + item.NodeIdentity:  item.RunnerProfiles,
			"runner-gpu-roles/" + item.NodeIdentity: item.RunnerGPURoles,
		} {
			if err := claim(role, artifact); err != nil {
				return err
			}
		}
	}
	for _, image := range images {
		if err := claim("oci-manifest/"+image.Image, image.Descriptor); err != nil {
			return err
		}
	}
	return nil
}

func verify(root *os.Root, bundle Bundle) error {
	if bundle.SchemaVersion != SchemaVersion || !validDigest(bundle.ReleaseDigest) ||
		!validDigest(bundle.ConfigurationRevision) {
		return invalid("bundle header is invalid")
	}
	plan := BuildPlan{
		SchemaVersion:     SchemaVersion,
		ExternalResources: append([]ExternalResource(nil), bundle.ConfigurationManifest.ExternalResources...),
	}
	for _, render := range bundle.ConfigurationManifest.FinalRenders {
		plan.FinalRenders = append(plan.FinalRenders, ArtifactInput{Name: render.Name, Ref: render.Artifact.Ref})
	}
	plan.NodeAgentUnit = ArtifactInput{
		Name: bundle.ConfigurationManifest.NodeAgentUnit.Name,
		Ref:  bundle.ConfigurationManifest.NodeAgentUnit.Artifact.Ref,
	}
	for _, item := range bundle.ConfigurationManifest.Packages {
		plan.Packages = append(plan.Packages, PackageInput{
			Name: item.Name, ContractRef: item.Contract.Ref, ArtifactRef: item.Artifact.Ref,
		})
	}
	for _, item := range bundle.ConfigurationManifest.WorkerMaterializations {
		plan.WorkerMaterializations = append(plan.WorkerMaterializations, WorkerMaterializationInput{
			NodeIdentity: item.NodeIdentity, Namespace: item.Namespace, WorkerID: item.WorkerID,
			WorkerEpoch: item.WorkerEpoch, WorkerPoolID: item.WorkerPoolID, FleetRevision: item.FleetRevision,
			NodeAgentIdentity:      item.NodeAgentIdentity,
			WorkerRuntimeConfigMap: item.WorkerRuntimeConfigMap, WorkerRuntimeRef: item.WorkerRuntime.Ref,
			RunnerProfilesConfigMap: item.RunnerProfilesConfigMap, RunnerProfilesRef: item.RunnerProfiles.Ref,
			RunnerGPURolesConfigMap: item.RunnerGPURolesConfigMap, RunnerGPURolesRef: item.RunnerGPURoles.Ref,
			WorkerControlTLSSecret:         item.WorkerControlTLSSecret,
			WorkerControlTLSSecretRevision: item.WorkerControlTLSSecretRevision,
			ExecutionProfileRevisionID:     item.ExecutionProfileRevisionID,
			InferenceBackendRevision:       item.InferenceBackendRevision, ModelRevisionID: item.ModelRevisionID,
		})
	}
	for _, image := range bundle.OCIImages {
		plan.OCIManifests = append(plan.OCIManifests, OCIManifestInput{
			Image: image.Image, Ref: image.Descriptor.Ref, Platform: image.Platform,
		})
	}
	rebuilt, err := build(root, plan)
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

func validRevision(value string) bool {
	lower := strings.ToLower(value)
	return value != "" && len(value) <= 300 && strings.TrimSpace(value) == value &&
		!strings.ContainsAny(value, "\x00\r\n") && !strings.Contains(lower, "placeholder") &&
		!strings.Contains(lower, "replace-with") && !strings.Contains(lower, "changeme") &&
		!strings.Contains(lower, "todo") && !strings.Contains(lower, ".invalid") &&
		value != strings.Repeat("0", 64) && value != "sha256:"+strings.Repeat("0", 64)
}

func validResourceName(value string) bool {
	return value != "" && len(validation.IsDNS1123Subdomain(value)) == 0 && validRevision(value)
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

func canonicalUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}

func expectedNodeAgentIdentity(nodeIdentity, workerID string) string {
	return "spiffe://vela.internal/node-agent/" +
		base64.RawURLEncoding.EncodeToString([]byte(nodeIdentity)) + "/" + workerID
}

func validSPIFFE(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "spiffe" && parsed.Host == "vela.internal" &&
		parsed.RawQuery == "" && parsed.Fragment == "" && validRevision(value)
}

func canonicalGPUUUID(value string) bool {
	if !gpuPattern.MatchString(value) {
		return false
	}
	parsed, err := uuid.Parse(strings.TrimPrefix(value, "GPU-"))
	return err == nil && parsed != uuid.Nil && "GPU-"+parsed.String() == value
}

func validatePackageContract(name string, encoded []byte, artifact Artifact) (PackageContract, error) {
	var contract PackageContract
	if err := decodeStrictJSON(encoded, &contract); err != nil {
		return PackageContract{}, invalidf("decode %s package contract: %v", name, err)
	}
	wantName := "vela-" + name
	if contract.SchemaVersion != 1 || contract.Name != wantName || contract.OS != "linux" ||
		contract.Architecture != "amd64" || !validRevision(contract.Revision) ||
		!filepath.IsAbs(contract.Entrypoint) || !validRevision(contract.Entrypoint) ||
		contract.ArtifactDigest != artifact.Digest || contract.ArtifactSizeBytes != artifact.SizeBytes {
		return PackageContract{}, invalidf("%s package contract does not bind the linux/amd64 artifact", name)
	}
	return contract, nil
}

func validateSystemdUnit(encoded []byte, entrypoint string) error {
	content := string(encoded)
	if entrypoint == "" || strings.Count(content, "ExecStart="+entrypoint) != 1 ||
		!strings.Contains(content, "[Service]") || !strings.Contains(content, "[Install]") ||
		!strings.Contains(content, "UMask=0077") || !strings.Contains(content, "WantedBy=multi-user.target") ||
		containsTemplateValue(content) {
		return invalid("node Agent systemd unit is not bound to the package entrypoint and hardening contract")
	}
	return nil
}

func containsTemplateValue(value string) bool {
	lower := strings.ToLower(value)
	return strings.Contains(lower, "placeholder") || strings.Contains(lower, "replace-with") ||
		strings.Contains(lower, "changeme") || strings.Contains(lower, "todo") ||
		strings.Contains(lower, ".invalid") || strings.Contains(value, "sha256:"+strings.Repeat("0", 64)) ||
		value == strings.Repeat("0", 64)
}

func decodeYAMLDocuments(encoded []byte) ([]map[string]any, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	var documents []map[string]any
	for {
		var document map[string]any
		err := decoder.Decode(&document)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(document) != 0 {
			documents = append(documents, document)
		}
	}
	return documents, nil
}
