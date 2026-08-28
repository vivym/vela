package releasebundle

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

func validateFinalRender(name string, encoded []byte, inventory *renderInventory) error {
	documents, err := decodeYAMLDocuments(encoded)
	if err != nil || len(documents) == 0 {
		return invalidf("final render %s must contain valid Kubernetes YAML documents: %v", name, err)
	}
	expectedNamespace := "vela-system"
	if name == "observability" {
		expectedNamespace = "vela-observability"
	}
	namespacedObjects := 0
	for _, document := range documents {
		apiVersion, _ := document["apiVersion"].(string)
		kind, _ := document["kind"].(string)
		metadata, _ := document["metadata"].(map[string]any)
		resourceName, _ := metadata["name"].(string)
		namespace, _ := metadata["namespace"].(string)
		if apiVersion == "" || kind == "" || !validResourceName(resourceName) {
			return invalidf("final render %s contains an invalid Kubernetes object identity", name)
		}
		if kind == "Secret" {
			return invalidf("final render %s embeds a Secret object", name)
		}
		if namespace != "" {
			namespacedObjects++
			if namespace != expectedNamespace {
				return invalidf("final render %s contains object %s/%s in namespace %s, want %s", name, kind, resourceName, namespace, expectedNamespace)
			}
		}
		if kind == "ConfigMap" {
			key := resourceKey{Kind: kind, Namespace: namespace, Name: resourceName}
			if _, duplicate := inventory.declared[key]; duplicate {
				return invalidf("ConfigMap %s/%s is declared more than once", namespace, resourceName)
			}
			inventory.declared[key] = struct{}{}
		}
		if err := scanRenderedValue(document, namespace, inventory, ""); err != nil {
			return invalidf("final render %s: %v", name, err)
		}
	}
	if namespacedObjects == 0 {
		return invalidf("final render %s contains no object in namespace %s", name, expectedNamespace)
	}
	return nil
}

func scanRenderedValue(value any, namespace string, inventory *renderInventory, parentKey string) error {
	switch typed := value.(type) {
	case map[string]any:
		if kind, _ := typed["kind"].(string); kind == "Secret" {
			return errors.New("render embeds a Secret object")
		}
		for key, child := range typed {
			if stringValue, ok := child.(string); ok {
				if containsTemplateValue(stringValue) {
					return fmt.Errorf("field %s contains a template or invalid production value", key)
				}
				if isImageField(key, parentKey) {
					if !validImage(stringValue) {
						return fmt.Errorf("field %s contains an unpinned or invalid OCI image %q", key, stringValue)
					}
					inventory.images[stringValue] = struct{}{}
				}
				if kind := directReferenceKind(key); kind != "" {
					inventory.referred[resourceKey{Kind: kind, Namespace: namespace, Name: stringValue}] = struct{}{}
				}
				if parentKey == "imagePullSecrets" && key == "name" {
					if !validResourceName(stringValue) {
						return errors.New("imagePullSecrets contains an invalid Secret name")
					}
					inventory.referred[resourceKey{Kind: "Secret", Namespace: namespace, Name: stringValue}] = struct{}{}
				}
			}
			if childMap, ok := child.(map[string]any); ok {
				if kind := selectorReferenceKind(key); kind != "" {
					name, _ := childMap["name"].(string)
					if name == "" && key == "secret" {
						name, _ = childMap["secretName"].(string)
					}
					if !validResourceName(name) {
						return fmt.Errorf("%s reference has an invalid name", key)
					}
					inventory.referred[resourceKey{Kind: kind, Namespace: namespace, Name: name}] = struct{}{}
				}
			}
			if parentKey == "data" {
				if stringValue, ok := child.(string); ok && (strings.HasSuffix(key, ".json") || strings.HasSuffix(key, ".yaml") || strings.HasSuffix(key, ".yml")) {
					if err := scanEmbeddedConfiguration(key, []byte(stringValue), namespace, inventory); err != nil {
						return err
					}
				}
			}
			if err := scanRenderedValue(child, namespace, inventory, key); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := scanRenderedValue(child, namespace, inventory, parentKey); err != nil {
				return err
			}
		}
	case string:
		if containsTemplateValue(typed) {
			return fmt.Errorf("render contains a template or invalid production value")
		}
	}
	return nil
}

func scanEmbeddedConfiguration(name string, encoded []byte, namespace string, inventory *renderInventory) error {
	var value any
	if strings.HasSuffix(name, ".json") {
		if err := decodeStrictJSON(encoded, &value); err != nil {
			return fmt.Errorf("embedded ConfigMap JSON %s is invalid: %w", name, err)
		}
	} else {
		decoder := yaml.NewDecoder(bytes.NewReader(encoded))
		if err := decoder.Decode(&value); err != nil {
			return fmt.Errorf("embedded ConfigMap YAML %s is invalid: %w", name, err)
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return fmt.Errorf("embedded ConfigMap YAML %s must contain one document", name)
		}
	}
	return scanRenderedValue(value, namespace, inventory, "")
}

func isImageField(key, parent string) bool {
	switch key {
	case "imageName", "operatorImage", "operator_image", "initImage", "workerAgentImage", "runnerImage":
		return true
	case "image":
		return parent == "containers" || parent == "initContainers" || parent == "ephemeralContainers"
	default:
		return false
	}
}

func validImage(value string) bool {
	if strings.ToLower(value) != value || !imagePattern.MatchString(value) || containsTemplateValue(value) {
		return false
	}
	separator := strings.LastIndex(value, "@sha256:")
	return separator > 0 && validDigest(value[separator+1:])
}

func directReferenceKind(key string) string {
	switch key {
	case "secretName", "workerControlTLSSecret", "artifactStoreTLSSecret":
		return "Secret"
	case "workerRuntimeConfigMap", "runnerProfilesConfigMap", "runnerGPURolesConfigMap":
		return "ConfigMap"
	default:
		return ""
	}
}

func selectorReferenceKind(key string) string {
	if strings.HasSuffix(key, "SecretRef") || strings.HasSuffix(key, "Secret") {
		return "Secret"
	}
	if strings.HasSuffix(key, "ConfigMapRef") || strings.HasSuffix(key, "ConfigMap") {
		return "ConfigMap"
	}
	switch key {
	case "secret", "secretRef", "secretKeyRef", "accessKeyId", "secretAccessKey", "sessionToken":
		return "Secret"
	case "configMap", "configMapRef", "configMapKeyRef":
		return "ConfigMap"
	default:
		return ""
	}
}

type configMapDocument struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name        string            `yaml:"name"`
		Namespace   string            `yaml:"namespace"`
		Labels      map[string]string `yaml:"labels,omitempty"`
		Annotations map[string]string `yaml:"annotations,omitempty"`
	} `yaml:"metadata"`
	Immutable *bool             `yaml:"immutable"`
	Data      map[string]string `yaml:"data"`
}

func decodeConfigMap(encoded []byte) (configMapDocument, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	var document configMapDocument
	if err := decoder.Decode(&document); err != nil {
		return configMapDocument{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return configMapDocument{}, errors.New("ConfigMap artifact must contain exactly one YAML document")
	}
	if document.APIVersion != "v1" || document.Kind != "ConfigMap" ||
		document.Immutable == nil || !*document.Immutable || len(document.Data) == 0 {
		return configMapDocument{}, errors.New("ConfigMap identity or immutability is invalid")
	}
	return document, nil
}

func buildWorkerMaterialization(
	root *os.Root,
	input WorkerMaterializationInput,
	inventory *renderInventory,
	unique map[string]struct{},
) (WorkerMaterialization, error) {
	if !validRevision(input.NodeIdentity) || !validResourceName(input.Namespace) ||
		!canonicalUUID(input.WorkerID) || input.WorkerEpoch <= 0 || !canonicalUUID(input.WorkerPoolID) ||
		!isLowerHex(input.FleetRevision) || input.FleetRevision == strings.Repeat("0", 64) ||
		input.NodeAgentIdentity != expectedNodeAgentIdentity(input.NodeIdentity, input.WorkerID) ||
		!validSPIFFE(input.NodeAgentIdentity) || !validResourceName(input.WorkerRuntimeConfigMap) ||
		!validResourceName(input.RunnerProfilesConfigMap) || !validResourceName(input.RunnerGPURolesConfigMap) ||
		!validResourceName(input.WorkerControlTLSSecret) || !validDigest(input.WorkerControlTLSSecretRevision) ||
		!canonicalUUID(input.ExecutionProfileRevisionID) || !canonicalUUID(input.ModelRevisionID) ||
		!validRevision(input.InferenceBackendRevision) {
		return WorkerMaterialization{}, invalidf("Worker materialization for node %q has invalid identity or revision fields", input.NodeIdentity)
	}
	for _, value := range []string{
		"node:" + input.NodeIdentity, "worker:" + input.WorkerID,
		"agent:" + input.NodeAgentIdentity, "runtime-name:" + input.WorkerRuntimeConfigMap,
		"profile-name:" + input.RunnerProfilesConfigMap, "gpu-name:" + input.RunnerGPURolesConfigMap,
		"tls-name:" + input.WorkerControlTLSSecret, "tls-revision:" + input.WorkerControlTLSSecretRevision,
	} {
		if _, duplicate := unique[value]; duplicate {
			return WorkerMaterialization{}, invalidf("Worker materialization identity or material %q is shared", value)
		}
		unique[value] = struct{}{}
	}

	runtimeArtifact, runtimeContent, err := artifactFor(root, input.WorkerRuntimeRef, "application/yaml", maxMetadataBytes)
	if err != nil {
		return WorkerMaterialization{}, invalidf("read Worker runtime ConfigMap for %s: %v", input.NodeIdentity, err)
	}
	profilesArtifact, profilesContent, err := artifactFor(root, input.RunnerProfilesRef, "application/yaml", maxMetadataBytes)
	if err != nil {
		return WorkerMaterialization{}, invalidf("read runner profiles ConfigMap for %s: %v", input.NodeIdentity, err)
	}
	gpuArtifact, gpuContent, err := artifactFor(root, input.RunnerGPURolesRef, "application/yaml", maxMetadataBytes)
	if err != nil {
		return WorkerMaterialization{}, invalidf("read runner GPU roles ConfigMap for %s: %v", input.NodeIdentity, err)
	}
	for _, digest := range []string{runtimeArtifact.Digest, profilesArtifact.Digest, gpuArtifact.Digest} {
		key := "material-digest:" + digest
		if _, duplicate := unique[key]; duplicate {
			return WorkerMaterialization{}, invalidf("Worker materialization artifact %s is shared", digest)
		}
		unique[key] = struct{}{}
	}

	runtimeConfig, err := validateRuntimeConfigMap(runtimeContent, input)
	if err != nil {
		return WorkerMaterialization{}, invalidf("Worker runtime ConfigMap for %s: %v", input.NodeIdentity, err)
	}
	profilesConfig, err := validateProfilesConfigMap(profilesContent, input)
	if err != nil {
		return WorkerMaterialization{}, invalidf("runner profiles ConfigMap for %s: %v", input.NodeIdentity, err)
	}
	gpuConfig, gpuIDs, err := validateGPURolesConfigMap(gpuContent, input)
	if err != nil {
		return WorkerMaterialization{}, invalidf("runner GPU roles ConfigMap for %s: %v", input.NodeIdentity, err)
	}
	for _, gpuID := range gpuIDs {
		key := "gpu:" + gpuID
		if _, duplicate := unique[key]; duplicate {
			return WorkerMaterialization{}, invalidf("GPU identity %s is shared across Worker materializations", gpuID)
		}
		unique[key] = struct{}{}
	}
	for _, config := range []configMapDocument{runtimeConfig, profilesConfig, gpuConfig} {
		key := resourceKey{Kind: "ConfigMap", Namespace: input.Namespace, Name: config.Metadata.Name}
		if _, duplicate := inventory.declared[key]; duplicate {
			return WorkerMaterialization{}, invalidf("materialized ConfigMap %s/%s is duplicated", input.Namespace, config.Metadata.Name)
		}
		inventory.declared[key] = struct{}{}
		inventory.referred[key] = struct{}{}
	}
	workerSecret := resourceKey{Kind: "Secret", Namespace: input.Namespace, Name: input.WorkerControlTLSSecret}
	inventory.referred[workerSecret] = struct{}{}
	inventory.expectedRevision[workerSecret] = input.WorkerControlTLSSecretRevision

	return WorkerMaterialization{
		NodeIdentity: input.NodeIdentity, Namespace: input.Namespace, WorkerID: input.WorkerID,
		WorkerEpoch: input.WorkerEpoch, WorkerPoolID: input.WorkerPoolID, FleetRevision: input.FleetRevision,
		NodeAgentIdentity:      input.NodeAgentIdentity,
		WorkerRuntimeConfigMap: input.WorkerRuntimeConfigMap, WorkerRuntime: runtimeArtifact,
		RunnerProfilesConfigMap: input.RunnerProfilesConfigMap, RunnerProfiles: profilesArtifact,
		RunnerGPURolesConfigMap: input.RunnerGPURolesConfigMap, RunnerGPURoles: gpuArtifact,
		WorkerControlTLSSecret:         input.WorkerControlTLSSecret,
		WorkerControlTLSSecretRevision: input.WorkerControlTLSSecretRevision,
		ExecutionProfileRevisionID:     input.ExecutionProfileRevisionID,
		InferenceBackendRevision:       input.InferenceBackendRevision, ModelRevisionID: input.ModelRevisionID,
	}, nil
}

func validateConfigMapIdentity(document configMapDocument, namespace, name string) error {
	if document.Metadata.Namespace != namespace || document.Metadata.Name != name ||
		!validResourceName(document.Metadata.Namespace) || !validResourceName(document.Metadata.Name) {
		return errors.New("ConfigMap name or namespace does not match the materialization")
	}
	for key, value := range document.Data {
		if containsTemplateValue(key) || containsTemplateValue(value) {
			return errors.New("ConfigMap contains a template or invalid production value")
		}
	}
	return nil
}

func validateRuntimeConfigMap(encoded []byte, input WorkerMaterializationInput) (configMapDocument, error) {
	document, err := decodeConfigMap(encoded)
	if err != nil {
		return configMapDocument{}, err
	}
	if err := validateConfigMapIdentity(document, input.Namespace, input.WorkerRuntimeConfigMap); err != nil {
		return configMapDocument{}, err
	}
	required := []string{
		"artifact-store-health-url", "attempt-quota-bytes", "control-address", "control-server-name",
		"critical-free-bytes", "high-watermark-bytes", "low-watermark-bytes", "max-entries",
		"max-entry-bytes", "output-cleanup-min-bytes-per-second", "xfs-device", "xfs-project-id",
	}
	keys := make([]string, 0, len(document.Data))
	for key := range document.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if !reflect.DeepEqual(keys, required) {
		return configMapDocument{}, fmt.Errorf("runtime keys are not the exact contract: %v", keys)
	}
	for _, key := range []string{
		"attempt-quota-bytes", "critical-free-bytes", "high-watermark-bytes", "low-watermark-bytes",
		"max-entries", "max-entry-bytes", "output-cleanup-min-bytes-per-second", "xfs-project-id",
	} {
		value, parseErr := strconv.ParseInt(document.Data[key], 10, 64)
		if parseErr != nil || value <= 0 {
			return configMapDocument{}, fmt.Errorf("runtime key %s must be a positive decimal integer", key)
		}
	}
	return document, nil
}

type profileAllowlist struct {
	SchemaVersion   int    `json:"schema_version"`
	BackendRevision string `json:"backend_revision"`
	Profiles        []struct {
		ModelRevisionID            string `json:"model_revision_id"`
		GenerationPresetRevisionID string `json:"generation_preset_revision_id"`
		ExecutionProfileRevisionID string `json:"execution_profile_revision_id"`
		OutputSpecID               string `json:"output_spec_id"`
	} `json:"profiles"`
}

func validateProfilesConfigMap(encoded []byte, input WorkerMaterializationInput) (configMapDocument, error) {
	document, err := decodeConfigMap(encoded)
	if err != nil {
		return configMapDocument{}, err
	}
	if err := validateConfigMapIdentity(document, input.Namespace, input.RunnerProfilesConfigMap); err != nil {
		return configMapDocument{}, err
	}
	if len(document.Data) != 1 || document.Data["profiles.json"] == "" {
		return configMapDocument{}, errors.New("profiles ConfigMap must contain only profiles.json")
	}
	var allowlist profileAllowlist
	if err := decodeStrictJSON([]byte(document.Data["profiles.json"]), &allowlist); err != nil {
		return configMapDocument{}, err
	}
	if allowlist.SchemaVersion != 1 || allowlist.BackendRevision != input.InferenceBackendRevision ||
		len(allowlist.Profiles) == 0 || len(allowlist.Profiles) > 1024 {
		return configMapDocument{}, errors.New("profile allowlist header or count is invalid")
	}
	seen := make(map[string]struct{}, len(allowlist.Profiles))
	for _, profile := range allowlist.Profiles {
		if profile.ModelRevisionID != input.ModelRevisionID ||
			profile.ExecutionProfileRevisionID != input.ExecutionProfileRevisionID ||
			!canonicalUUID(profile.GenerationPresetRevisionID) || !canonicalUUID(profile.OutputSpecID) {
			return configMapDocument{}, errors.New("profile allowlist entry does not match materialized revisions")
		}
		encodedProfile, _ := json.Marshal(profile)
		key := string(encodedProfile)
		if _, duplicate := seen[key]; duplicate {
			return configMapDocument{}, errors.New("profile allowlist contains a duplicate entry")
		}
		seen[key] = struct{}{}
	}
	return document, nil
}

type gpuRoles struct {
	SchemaVersion int      `json:"schema_version"`
	EncoderVAE    string   `json:"encoder_vae"`
	DiT           []string `json:"dit"`
}

func validateGPURolesConfigMap(encoded []byte, input WorkerMaterializationInput) (configMapDocument, []string, error) {
	document, err := decodeConfigMap(encoded)
	if err != nil {
		return configMapDocument{}, nil, err
	}
	if err := validateConfigMapIdentity(document, input.Namespace, input.RunnerGPURolesConfigMap); err != nil {
		return configMapDocument{}, nil, err
	}
	if len(document.Data) != 1 || document.Data["gpu-roles.json"] == "" {
		return configMapDocument{}, nil, errors.New("GPU roles ConfigMap must contain only gpu-roles.json")
	}
	var roles gpuRoles
	if err := decodeStrictJSON([]byte(document.Data["gpu-roles.json"]), &roles); err != nil {
		return configMapDocument{}, nil, err
	}
	if roles.SchemaVersion != 1 || len(roles.DiT) != 7 || !canonicalGPUUUID(roles.EncoderVAE) {
		return configMapDocument{}, nil, errors.New("GPU role map must contain one Encoder/VAE and seven DiT roles")
	}
	seen := map[string]struct{}{roles.EncoderVAE: {}}
	for _, gpu := range roles.DiT {
		if !canonicalGPUUUID(gpu) {
			return configMapDocument{}, nil, errors.New("GPU role map contains a non-canonical GPU UUID")
		}
		if _, duplicate := seen[gpu]; duplicate {
			return configMapDocument{}, nil, errors.New("GPU role map contains a duplicate GPU UUID")
		}
		seen[gpu] = struct{}{}
	}
	return document, append([]string{roles.EncoderVAE}, roles.DiT...), nil
}

func validateExternalResources(resources []ExternalResource, inventory renderInventory) error {
	external := make(map[resourceKey]struct{}, len(resources))
	externalRevision := make(map[resourceKey]string, len(resources))
	for _, resource := range resources {
		if (resource.Kind != "ConfigMap" && resource.Kind != "Secret") ||
			!validResourceName(resource.Namespace) || !validResourceName(resource.Name) ||
			!validDigest(resource.Revision) {
			return invalid("external resource declaration is invalid")
		}
		key := resourceKey{Kind: resource.Kind, Namespace: resource.Namespace, Name: resource.Name}
		if _, duplicate := external[key]; duplicate {
			return invalidf("external resource %v is duplicated", key)
		}
		external[key] = struct{}{}
		externalRevision[key] = resource.Revision
	}
	for resource, expected := range inventory.expectedRevision {
		if externalRevision[resource] != expected {
			return invalidf("external resource %s %s/%s revision does not match materialization", resource.Kind, resource.Namespace, resource.Name)
		}
	}
	for reference := range inventory.referred {
		if _, rendered := inventory.declared[reference]; rendered {
			continue
		}
		if _, declared := external[reference]; !declared {
			return invalidf("referenced %s %s/%s is undeclared", reference.Kind, reference.Namespace, reference.Name)
		}
	}
	for declaration := range external {
		if _, used := inventory.referred[declaration]; !used {
			return invalidf("external resource %s %s/%s is not referenced", declaration.Kind, declaration.Namespace, declaration.Name)
		}
	}
	return nil
}

type ociManifest struct {
	SchemaVersion int                 `json:"schemaVersion"`
	MediaType     string              `json:"mediaType"`
	ArtifactType  string              `json:"artifactType,omitempty"`
	Config        ociBlobDescriptor   `json:"config"`
	Layers        []ociBlobDescriptor `json:"layers"`
	Subject       *ociBlobDescriptor  `json:"subject,omitempty"`
	Annotations   map[string]string   `json:"annotations,omitempty"`
}

type ociBlobDescriptor struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	URLs        []string          `json:"urls,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	Data        []byte            `json:"data,omitempty"`
}

func validateOCIManifest(input OCIManifestInput, artifact Artifact, encoded []byte) (string, error) {
	if !validImage(input.Image) || input.Platform.OS != "linux" || input.Platform.Architecture != "amd64" {
		return "", invalidf("OCI image %q or platform is invalid", input.Image)
	}
	digest := input.Image[strings.LastIndex(input.Image, "@")+1:]
	if artifact.Digest != digest {
		return "", invalidf("OCI manifest digest for %s does not match its image reference", input.Image)
	}
	var manifest ociManifest
	if err := decodeStrictJSON(encoded, &manifest); err != nil {
		return "", invalidf("decode OCI manifest for %s: %v", input.Image, err)
	}
	if manifest.SchemaVersion != 2 ||
		(manifest.MediaType != OCIManifestMediaType && manifest.MediaType != DockerManifestMediaType) ||
		!validOCIBlobDescriptor(manifest.Config) || len(manifest.Layers) == 0 || len(manifest.Layers) > 4096 {
		return "", invalidf("OCI manifest for %s has an invalid header, config, or layer count", input.Image)
	}
	for _, layer := range manifest.Layers {
		if !validOCIBlobDescriptor(layer) {
			return "", invalidf("OCI manifest for %s has an invalid layer descriptor", input.Image)
		}
	}
	if manifest.Subject != nil && !validOCIBlobDescriptor(*manifest.Subject) {
		return "", invalidf("OCI manifest for %s has an invalid subject descriptor", input.Image)
	}
	return manifest.MediaType, nil
}

func validOCIBlobDescriptor(descriptor ociBlobDescriptor) bool {
	if descriptor.MediaType == "" || !validRevision(descriptor.MediaType) ||
		!validDigest(descriptor.Digest) || descriptor.Size <= 0 || len(descriptor.URLs) > 32 || len(descriptor.Data) > maxMetadataBytes {
		return false
	}
	for _, address := range descriptor.URLs {
		if containsTemplateValue(address) {
			return false
		}
	}
	return true
}
