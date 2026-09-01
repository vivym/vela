package releasebundle

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"

	ociv1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/imageref"
	"k8s.io/apimachinery/pkg/util/validation"
)

type renderedResourceContract struct {
	APIVersion    string
	Kind          string
	Namespace     string
	Name          string
	NamePrefix    string
	ClusterScoped bool
}

type renderedImageRole uint8

const (
	renderedImageRoleNone renderedImageRole = iota
	renderedImageRoleSupport
	renderedImageRoleModelRuntime
)

type renderedFieldContract struct {
	ImageRole     renderedImageRole
	ReferenceKind string
	RequiredKeys  []string
}

var renderedFieldContracts = map[string]renderedFieldContract{
	"imageName":                       {ImageRole: renderedImageRoleSupport},
	"operatorImage":                   {ImageRole: renderedImageRoleSupport},
	"operator_image":                  {ImageRole: renderedImageRoleSupport},
	"initImage":                       {ImageRole: renderedImageRoleSupport},
	"init_image":                      {ImageRole: renderedImageRoleSupport},
	"stage_worker_agent_image":        {ImageRole: renderedImageRoleSupport},
	"runtime_image":                   {ImageRole: renderedImageRoleModelRuntime},
	"secretName":                      {ReferenceKind: "Secret"},
	"stage_worker_control_tls_secret": {ReferenceKind: "Secret", RequiredKeys: []string{"ca.crt", "tls.crt", "tls.key"}},
	"stage_worker_authority_secret":   {ReferenceKind: "Secret", RequiredKeys: []string{"keyring.json"}},
	"artifact_store_credentials_secret": {
		ReferenceKind: "Secret", RequiredKeys: []string{"access-key-id", "secret-access-key"},
	},
	"artifact_store_ca_secret": {ReferenceKind: "Secret", RequiredKeys: []string{"ca.crt"}},
	"stage_worker_config_map":  {ReferenceKind: "ConfigMap"},
}

var finalRenderInventory = map[string][]renderedResourceContract{
	"control-storage": {
		{APIVersion: "v1", Kind: "Namespace", Name: "vela-system", ClusterScoped: true},
		{APIVersion: "v1", Kind: "ConfigMap", Namespace: "vela-system", Name: "nats-config"},
		{APIVersion: "v1", Kind: "ConfigMap", Namespace: "vela-system", NamePrefix: "vela-barman-cloud-plugin-contract-"},
		{APIVersion: "v1", Kind: "ConfigMap", Namespace: "vela-system", NamePrefix: "vela-jetstream-contract-"},
		{APIVersion: "v1", Kind: "ConfigMap", Namespace: "vela-system", Name: "vela-recovery-contract"},
		{APIVersion: "v1", Kind: "Service", Namespace: "vela-system", Name: "nats"},
		{APIVersion: "apps/v1", Kind: "StatefulSet", Namespace: "vela-system", Name: "nats"},
		{APIVersion: "policy/v1", Kind: "PodDisruptionBudget", Namespace: "vela-system", Name: "nats"},
		{APIVersion: "policy/v1", Kind: "PodDisruptionBudget", Namespace: "vela-system", Name: "vela-postgres"},
		{APIVersion: "barmancloud.cnpg.io/v1", Kind: "ObjectStore", Namespace: "vela-system", Name: "vela-postgres-backup"},
		{APIVersion: "postgresql.cnpg.io/v1", Kind: "Cluster", Namespace: "vela-system", Name: "vela-postgres"},
		{APIVersion: "postgresql.cnpg.io/v1", Kind: "ScheduledBackup", Namespace: "vela-system", Name: "vela-postgres-daily"},
	},
	"fleet-controller": {
		{APIVersion: "v1", Kind: "Namespace", Name: "vela-system", ClusterScoped: true},
		{APIVersion: "apiextensions.k8s.io/v1", Kind: "CustomResourceDefinition", Name: "workerpools.fleet.vela.ai", ClusterScoped: true},
		{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "vela-system", Name: "vela-fleet-controller"},
		{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role", Namespace: "vela-system", Name: "vela-fleet-controller"},
		{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole", Name: "vela-fleet-controller-node-reader", ClusterScoped: true},
		{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding", Namespace: "vela-system", Name: "vela-fleet-controller"},
		{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding", Name: "vela-fleet-controller-node-reader", ClusterScoped: true},
		{APIVersion: "v1", Kind: "ConfigMap", Namespace: "vela-system", NamePrefix: "vela-fleet-residency-plan-rollouts-"},
		{APIVersion: "v1", Kind: "Service", Namespace: "vela-system", Name: "vela-fleet-admission"},
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "vela-system", Name: "vela-fleet-controller"},
		{APIVersion: "policy/v1", Kind: "PodDisruptionBudget", Namespace: "vela-system", Name: "vela-fleet-controller"},
		{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "vela-fleet-protection", ClusterScoped: true},
	},
	"observability": {
		{APIVersion: "v1", Kind: "ConfigMap", Namespace: "vela-observability", NamePrefix: "vela-slo-alert-rules-"},
		{APIVersion: "v1", Kind: "ConfigMap", Namespace: "vela-observability", NamePrefix: "vela-slo-contract-"},
		{APIVersion: "v1", Kind: "ConfigMap", Namespace: "vela-observability", NamePrefix: "vela-slo-dashboard-"},
		{APIVersion: "monitoring.coreos.com/v1", Kind: "PodMonitor", Namespace: "vela-observability", Name: "vela-control"},
	},
	"stage-worker": {
		{APIVersion: "v1", Kind: "Namespace", Name: "vela-system", ClusterScoped: true},
		{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "vela-system", Name: "vela-worker"},
		{APIVersion: "v1", Kind: "ConfigMap", Namespace: "vela-system", NamePrefix: "vela-stage-worker-runtime-"},
	},
	"vela-control": {
		{APIVersion: "v1", Kind: "Namespace", Name: "vela-system", ClusterScoped: true},
		{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "vela-system", Name: "vela-control"},
		{APIVersion: "v1", Kind: "ConfigMap", Namespace: "vela-system", NamePrefix: "vela-control-node-agents-"},
		{APIVersion: "v1", Kind: "ConfigMap", Namespace: "vela-system", NamePrefix: "vela-control-runtime-"},
		{APIVersion: "v1", Kind: "Service", Namespace: "vela-system", Name: "vela-api"},
		{APIVersion: "v1", Kind: "Service", Namespace: "vela-system", Name: "vela-compliance"},
		{APIVersion: "v1", Kind: "Service", Namespace: "vela-system", Name: "vela-control"},
		{APIVersion: "v1", Kind: "Service", Namespace: "vela-system", Name: "vela-finance-reconciliation"},
		{APIVersion: "v1", Kind: "Service", Namespace: "vela-system", Name: "vela-worker-control"},
		{APIVersion: "scheduling.k8s.io/v1", Kind: "PriorityClass", Name: "vela-control-critical", ClusterScoped: true},
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "vela-system", Name: "vela-control"},
		{APIVersion: "policy/v1", Kind: "PodDisruptionBudget", Namespace: "vela-system", Name: "vela-control"},
		{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "vela-system", Name: "vela-control-allow-api"},
		{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "vela-system", Name: "vela-control-allow-compliance"},
		{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "vela-system", Name: "vela-control-allow-finance"},
		{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "vela-system", Name: "vela-control-allow-fleet"},
		{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "vela-system", Name: "vela-control-allow-observability"},
		{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "vela-system", Name: "vela-control-allow-worker"},
		{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy", Namespace: "vela-system", Name: "vela-control-default-deny-ingress"},
	},
}

func validateFinalRender(name string, encoded []byte, inventory *renderInventory, budget *yamlGraphBudget) error {
	documents, err := decodeYAMLDocuments(encoded, budget)
	if err != nil || len(documents) == 0 {
		return invalidf("final render %s must contain valid Kubernetes YAML documents: %v", name, err)
	}
	expected := finalRenderInventory[name]
	if len(documents) != len(expected) {
		return invalidf("final render %s contains %d resources, want exact inventory of %d", name, len(documents), len(expected))
	}
	found := make([]bool, len(expected))
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
		matched := -1
		for index, contract := range expected {
			if contract.matches(apiVersion, kind, namespace, resourceName) {
				if found[index] {
					return invalidf("final render %s duplicates resource %s/%s/%s/%s", name, apiVersion, kind, namespace, resourceName)
				}
				matched = index
				break
			}
		}
		if matched < 0 {
			return invalidf("final render %s contains unexpected resource %s/%s/%s/%s", name, apiVersion, kind, namespace, resourceName)
		}
		found[matched] = true
		if kind == "ConfigMap" {
			key := resourceKey{Kind: kind, Namespace: namespace, Name: resourceName}
			if _, duplicate := inventory.declared[key]; duplicate {
				return invalidf("ConfigMap %s/%s is declared more than once", namespace, resourceName)
			}
			inventory.declared[key] = struct{}{}
		}
		consumer := consumerIdentity(kind, namespace, resourceName)
		if err := scanRenderedValue(document, namespace, consumer, inventory, "", budget); err != nil {
			return invalidf("final render %s: %v", name, err)
		}
		if kind == "ConfigMap" && namespace == "vela-system" &&
			strings.HasPrefix(resourceName, "vela-fleet-residency-plan-rollouts-") {
			rollouts, err := decodeFleetResidencyPlanConfigMap(document, namespace)
			if err != nil {
				return invalidf("Fleet ResidencyPlan ConfigMap %s/%s: %v", namespace, resourceName, err)
			}
			inventory.residencyRollouts = append(inventory.residencyRollouts, rollouts...)
			recordH3RuntimeImages(inventory, rollouts)
		}
	}
	for index, present := range found {
		if !present {
			contract := expected[index]
			return invalidf("final render %s is missing resource %s/%s/%s/%s", name, contract.APIVersion, contract.Kind, contract.Namespace, contract.displayName())
		}
	}
	return nil
}

func recordH3RuntimeImages(
	inventory *renderInventory,
	rollouts []fleetcontroller.ResidencyPlanRollout,
) {
	for _, rollout := range rollouts {
		for _, bundle := range rollout.WorkerBundles {
			for _, worker := range bundle.WorkerInstances {
				for _, runtime := range worker.ModelRuntimes {
					switch runtime.Component {
					case "ENCODER", "DIT", "VAE_DECODER":
						inventory.h3RuntimeImages[bundle.RuntimeImage] = struct{}{}
					}
				}
			}
		}
	}
}

func decodeFleetResidencyPlanConfigMap(
	document map[string]any,
	namespace string,
) ([]fleetcontroller.ResidencyPlanRollout, error) {
	immutable, _ := document["immutable"].(bool)
	data, ok := document["data"].(map[string]any)
	if !immutable || !ok || len(data) != 1 {
		return nil, errors.New("immutable data must contain only rollouts.json")
	}
	encoded, ok := data["rollouts.json"].(string)
	if !ok || encoded == "" {
		return nil, errors.New("data must contain rollouts.json")
	}
	return fleetcontroller.DecodeResidencyPlanRollouts([]byte(encoded), namespace)
}

// The inventory validates release ownership and identity only. Kubernetes API
// schema and admission validation remain external deployment gates.
func (contract renderedResourceContract) matches(apiVersion, kind, namespace, name string) bool {
	if apiVersion != contract.APIVersion || kind != contract.Kind || (namespace == "") != contract.ClusterScoped {
		return false
	}
	if !contract.ClusterScoped && namespace != contract.Namespace {
		return false
	}
	if contract.Name != "" {
		return name == contract.Name
	}
	if contract.Kind != "ConfigMap" || containsTemplateValue(name) {
		return false
	}
	return strings.HasPrefix(name, contract.NamePrefix) && len(name) > len(contract.NamePrefix)
}

func (contract renderedResourceContract) displayName() string {
	if contract.Name != "" {
		return contract.Name
	}
	return contract.NamePrefix + "*"
}

func scanRenderedValue(
	value any,
	namespace,
	consumer string,
	inventory *renderInventory,
	parentKey string,
	budget *yamlGraphBudget,
) error {
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
				if role, imageField := imageRoleForField(key, parentKey); imageField {
					if !validImage(stringValue) {
						return fmt.Errorf("field %s contains an unpinned or invalid OCI image %q", key, stringValue)
					}
					inventory.images[stringValue] = struct{}{}
					if role == renderedImageRoleModelRuntime {
						inventory.modelRuntimeImages[stringValue] = struct{}{}
					} else {
						inventory.supportImages[stringValue] = struct{}{}
					}
				}
				if kind := directReferenceKind(key); kind != "" {
					resource := resourceKey{Kind: kind, Namespace: namespace, Name: stringValue}
					recordReference(inventory, resource, consumer, directSecretKeys(key))
				}
				if parentKey == "imagePullSecrets" && key == "name" {
					if !validResourceName(stringValue) {
						return errors.New("imagePullSecrets contains an invalid Secret name")
					}
					recordReference(inventory, resourceKey{Kind: "Secret", Namespace: namespace, Name: stringValue}, consumer, []string{".dockerconfigjson"})
				}
			}
			if childMap, ok := child.(map[string]any); ok {
				if kind := selectorReferenceKind(key); kind != "" {
					if err := validateSelectorOptional(childMap); err != nil {
						return fmt.Errorf("%s reference: %w", key, err)
					}
					name, _ := childMap["name"].(string)
					if name == "" && key == "secret" {
						name, _ = childMap["secretName"].(string)
					}
					if !validResourceName(name) {
						return fmt.Errorf("%s reference has an invalid name", key)
					}
					keys, err := selectorSecretKeys(kind, childMap)
					if err != nil {
						return fmt.Errorf("%s reference: %w", key, err)
					}
					resource := resourceKey{Kind: kind, Namespace: namespace, Name: name}
					recordReference(inventory, resource, consumer, keys)
				}
			}
			if parentKey == "data" {
				if stringValue, ok := child.(string); ok && (strings.HasSuffix(key, ".json") || strings.HasSuffix(key, ".yaml") || strings.HasSuffix(key, ".yml")) {
					if err := scanEmbeddedConfiguration(key, []byte(stringValue), namespace, consumer, inventory, budget); err != nil {
						return err
					}
				}
			}
			if err := scanRenderedValue(child, namespace, consumer, inventory, key, budget); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := scanRenderedValue(child, namespace, consumer, inventory, parentKey, budget); err != nil {
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

func validateSelectorOptional(selector map[string]any) error {
	value, present := selector["optional"]
	if !present {
		return nil
	}
	optional, ok := value.(bool)
	if !ok || optional {
		return errors.New("optional must be absent or false")
	}
	return nil
}

func scanEmbeddedConfiguration(
	name string,
	encoded []byte,
	namespace,
	consumer string,
	inventory *renderInventory,
	budget *yamlGraphBudget,
) error {
	var value any
	if strings.HasSuffix(name, ".json") {
		if err := decodeStrictJSON(encoded, &value); err != nil {
			return fmt.Errorf("embedded ConfigMap JSON %s is invalid: %w", name, err)
		}
	} else {
		node, err := decodeSingleYAMLNode(encoded, budget)
		if err != nil {
			return fmt.Errorf("embedded ConfigMap YAML %s is invalid: %w", name, err)
		}
		if err := node.Decode(&value); err != nil {
			return fmt.Errorf("embedded ConfigMap YAML %s is invalid: %w", name, err)
		}
	}
	return scanRenderedValue(value, namespace, consumer, inventory, "", budget)
}

func consumerIdentity(kind, namespace, name string) string {
	return kind + "/" + namespace + "/" + name
}

func recordReference(inventory *renderInventory, resource resourceKey, consumer string, keys []string) {
	inventory.referred[resource] = struct{}{}
	if resource.Kind != "Secret" {
		return
	}
	if inventory.secretConsumers[resource] == nil {
		inventory.secretConsumers[resource] = make(map[string]struct{})
	}
	inventory.secretConsumers[resource][consumer] = struct{}{}
	if inventory.secretKeys[resource] == nil {
		inventory.secretKeys[resource] = make(map[string]struct{})
	}
	for _, key := range keys {
		inventory.secretKeys[resource][key] = struct{}{}
	}
}

func directSecretKeys(key string) []string {
	return renderedFieldContracts[key].RequiredKeys
}

func selectorSecretKeys(kind string, selector map[string]any) ([]string, error) {
	if kind != "Secret" {
		return nil, nil
	}
	keys := make([]string, 0)
	if value, present := selector["key"]; present {
		key, ok := value.(string)
		if !ok || len(validation.IsConfigMapKey(key)) != 0 || containsTemplateValue(key) {
			return nil, errors.New("key must name an exact Secret key")
		}
		keys = append(keys, key)
	}
	if value, present := selector["items"]; present {
		items, ok := value.([]any)
		if !ok {
			return nil, errors.New("items must enumerate exact Secret keys")
		}
		for _, item := range items {
			itemMap, ok := item.(map[string]any)
			key, keyOK := itemMap["key"].(string)
			if !ok || !keyOK || len(validation.IsConfigMapKey(key)) != 0 || containsTemplateValue(key) {
				return nil, errors.New("items must enumerate exact Secret keys")
			}
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil, errors.New("must enumerate at least one exact Secret key")
	}
	return keys, nil
}

func imageRoleForField(key, parent string) (renderedImageRole, bool) {
	if contract, exists := renderedFieldContracts[key]; exists && contract.ImageRole != renderedImageRoleNone {
		return contract.ImageRole, true
	}
	if key == "image" && (parent == "containers" || parent == "initContainers" || parent == "ephemeralContainers") {
		return renderedImageRoleSupport, true
	}
	return renderedImageRoleNone, false
}

func validImage(value string) bool {
	return !containsTemplateValue(value) && imageref.ValidPinned(value)
}

func directReferenceKind(key string) string {
	return renderedFieldContracts[key].ReferenceKind
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

func validateExternalResources(resources []ExternalResource, inventory renderInventory) error {
	external := make(map[resourceKey]struct{}, len(resources))
	externalRevision := make(map[resourceKey]string, len(resources))
	for _, resource := range resources {
		if (resource.Kind != "ConfigMap" && resource.Kind != "Secret") ||
			!validResourceName(resource.Namespace) || !validResourceName(resource.Name) ||
			!validDigest(resource.Revision) {
			return invalid("external resource declaration is invalid")
		}
		if resource.Kind == "ConfigMap" {
			if len(resource.RequiredKeys) != 0 || len(resource.Consumers) != 0 {
				return invalidf("external ConfigMap %s/%s must not carry Secret contract fields", resource.Namespace, resource.Name)
			}
		} else if !validSecretContract(resource.RequiredKeys, resource.Consumers) {
			return invalidf("external Secret %s/%s required_keys or consumers are invalid", resource.Namespace, resource.Name)
		}
		key := resourceKey{Kind: resource.Kind, Namespace: resource.Namespace, Name: resource.Name}
		if _, duplicate := external[key]; duplicate {
			return invalidf("external resource %v is duplicated", key)
		}
		external[key] = struct{}{}
		externalRevision[key] = resource.Revision
		if resource.Kind == "Secret" {
			observedConsumers := sortedSet(inventory.secretConsumers[key])
			if !reflect.DeepEqual(observedConsumers, resource.Consumers) {
				return invalidf("external Secret %s/%s consumers do not exactly match rendered consumers", resource.Namespace, resource.Name)
			}
			observedKeys := sortedSet(inventory.secretKeys[key])
			if !reflect.DeepEqual(observedKeys, resource.RequiredKeys) {
				return invalidf("external Secret %s/%s required_keys do not exactly match keyed references", resource.Namespace, resource.Name)
			}
		}
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

func validSecretContract(keys, consumers []string) bool {
	if len(keys) == 0 || len(consumers) == 0 || !slices.IsSorted(keys) || !slices.IsSorted(consumers) {
		return false
	}
	for index, key := range keys {
		if len(validation.IsConfigMapKey(key)) != 0 || containsTemplateValue(key) ||
			(index > 0 && key == keys[index-1]) {
			return false
		}
	}
	for index, consumer := range consumers {
		if !ValidRevision(consumer) || (index > 0 && consumer == consumers[index-1]) {
			return false
		}
	}
	return true
}

func sortedSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
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

// ValidateOCIManifestInput applies the same manifest/config contract used by
// Build to an in-memory release artifact pair.
func ValidateOCIManifestInput(
	input OCIManifestInput,
	manifestEncoded, configEncoded []byte,
) error {
	if len(manifestEncoded) == 0 || len(manifestEncoded) > maxMetadataBytes ||
		len(configEncoded) == 0 || len(configEncoded) > maxMetadataBytes {
		return invalid("OCI manifest and config must be bounded non-empty metadata")
	}
	manifestDigest := sha256.Sum256(manifestEncoded)
	configDigest := sha256.Sum256(configEncoded)
	manifestArtifact := artifactDescriptor(
		input.Ref, "", manifestDigest[:], int64(len(manifestEncoded)),
	)
	configArtifact := artifactDescriptor(
		input.ConfigRef, OCIImageConfigMediaType, configDigest[:], int64(len(configEncoded)),
	)
	_, _, err := validateOCIManifest(
		input,
		manifestArtifact,
		manifestEncoded,
		configArtifact,
		configEncoded,
	)
	return err
}

func validateOCIManifest(
	input OCIManifestInput,
	artifact Artifact,
	encoded []byte,
	configArtifact Artifact,
	configEncoded []byte,
) (string, Platform, error) {
	if !validImage(input.Image) {
		return "", Platform{}, invalidf("OCI image %q is invalid", input.Image)
	}
	digest := input.Image[strings.LastIndex(input.Image, "@")+1:]
	if artifact.Digest != digest {
		return "", Platform{}, invalidf("OCI manifest digest for %s does not match its image reference", input.Image)
	}
	var manifest ociManifest
	if err := decodeStrictJSON(encoded, &manifest); err != nil {
		return "", Platform{}, invalidf("decode OCI manifest for %s: %v", input.Image, err)
	}
	if manifest.SchemaVersion != 2 || manifest.MediaType != OCIManifestMediaType ||
		!validOCIBlobDescriptor(manifest.Config) || manifest.Config.MediaType != OCIImageConfigMediaType ||
		manifest.Config.Digest != configArtifact.Digest || manifest.Config.Size != configArtifact.SizeBytes ||
		len(manifest.Layers) == 0 || len(manifest.Layers) > 4096 {
		return "", Platform{}, invalidf("OCI manifest for %s has an invalid header, config, or layer count", input.Image)
	}
	for _, layer := range manifest.Layers {
		if !validOCIBlobDescriptor(layer) {
			return "", Platform{}, invalidf("OCI manifest for %s has an invalid layer descriptor", input.Image)
		}
	}
	if manifest.Subject != nil && !validOCIBlobDescriptor(*manifest.Subject) {
		return "", Platform{}, invalidf("OCI manifest for %s has an invalid subject descriptor", input.Image)
	}
	var config ociv1.Image
	if err := decodeStrictJSON(configEncoded, &config); err != nil {
		return "", Platform{}, invalidf("decode OCI config for %s: %v", input.Image, err)
	}
	if config.OS != "linux" || config.Architecture != "amd64" {
		return "", Platform{}, invalidf("OCI config for %s must bind linux/amd64", input.Image)
	}
	return manifest.MediaType, Platform{OS: config.OS, Architecture: config.Architecture}, nil
}

func validateModelRuntimeOCIConfig(image string, encoded []byte, requireH3Composition bool) error {
	var config ociv1.Image
	if err := decodeStrictJSON(encoded, &config); err != nil {
		return invalidf("decode ModelRuntime OCI config for %s: %v", image, err)
	}
	if !reflect.DeepEqual(config.Config.Entrypoint, []string{"/usr/local/bin/vela-model-runtime"}) ||
		len(config.Config.Cmd) != 0 {
		return invalidf(
			"ModelRuntime OCI config for %s must bind the exact vela-model-runtime entrypoint",
			image,
		)
	}
	if !requireH3Composition {
		return nil
	}
	if !imageref.ValidPinned(config.Config.Labels["vela.ai.h3-runtime-base"]) {
		return invalidf(
			"ModelRuntime OCI config for %s must bind a digest-pinned H3 runtime base",
			image,
		)
	}
	for _, label := range []string{
		"vela.ai.h3-encoder.sha256",
		"vela.ai.h3-dit.sha256",
		"vela.ai.h3-vae-decoder.sha256",
	} {
		value := config.Config.Labels[label]
		if len(value) != 64 || value == strings.Repeat("0", 64) || !isLowerHex(value) {
			return invalidf(
				"ModelRuntime OCI config for %s must bind a valid %s label",
				image,
				label,
			)
		}
	}
	return nil
}

func validOCIBlobDescriptor(descriptor ociBlobDescriptor) bool {
	if descriptor.MediaType == "" || !ValidRevision(descriptor.MediaType) ||
		!validDigest(descriptor.Digest) || descriptor.Size <= 0 || len(descriptor.URLs) != 0 || len(descriptor.Data) != 0 {
		return false
	}
	return true
}
