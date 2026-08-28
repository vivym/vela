package releasebundle

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"

	ociv1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/imageref"
	"gopkg.in/yaml.v3"
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
		{APIVersion: "v1", Kind: "ConfigMap", Namespace: "vela-system", NamePrefix: "vela-fleet-desired-"},
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
	"worker-agent": {
		{APIVersion: "v1", Kind: "Namespace", Name: "vela-system", ClusterScoped: true},
		{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "vela-system", Name: "vela-worker"},
		{APIVersion: "policy/v1", Kind: "PodDisruptionBudget", Namespace: "vela-system", Name: "vela-h3-worker"},
		{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "vela-system", Name: "vela-h3-worker"},
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
		if kind == "ConfigMap" && namespace == "vela-system" && strings.HasPrefix(resourceName, "vela-fleet-desired-") {
			desired, err := decodeFleetDesiredConfigMap(document, namespace)
			if err != nil {
				return invalidf("Fleet desired ConfigMap %s/%s: %v", namespace, resourceName, err)
			}
			inventory.fleetDesired = append(inventory.fleetDesired, desired...)
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

func decodeFleetDesiredConfigMap(
	document map[string]any,
	namespace string,
) ([]fleetcontroller.DesiredRevision, error) {
	data, ok := document["data"].(map[string]any)
	if !ok || len(data) == 0 {
		return nil, errors.New("data must contain desired.yaml")
	}
	desiredYAML, ok := data["desired.yaml"].(string)
	if !ok || desiredYAML == "" {
		return nil, errors.New("data must contain desired.yaml")
	}
	revisions, _, err := fleetcontroller.DecodeDesiredConfiguration([]byte(desiredYAML), namespace)
	if err != nil {
		return nil, err
	}
	return revisions, nil
}

func validateFleetDesiredMaterializations(
	desiredRevisions []fleetcontroller.DesiredRevision,
	materializations []WorkerMaterializationInput,
) error {
	type desiredPlacement struct {
		revision  fleetcontroller.DesiredRevision
		placement fleetcontroller.WorkerPlacement
	}
	placements := make(map[string]desiredPlacement, len(materializations))
	placementCount := 0
	for _, desired := range desiredRevisions {
		for _, placement := range desired.Placements {
			key := desired.Namespace + "/" + desired.WorkerPoolID.String() + "/" + placement.NodeIdentity
			if _, duplicate := placements[key]; duplicate {
				return invalid("Fleet desired configuration contains a duplicate Worker placement")
			}
			placements[key] = desiredPlacement{revision: desired, placement: placement}
			placementCount++
		}
	}
	if len(desiredRevisions) == 0 || placementCount != len(materializations) {
		return invalid("Fleet desired and Worker materialization coverage is not exact")
	}
	matched := make(map[string]struct{}, len(materializations))
	for _, materialization := range materializations {
		key := materialization.Namespace + "/" + materialization.WorkerPoolID + "/" + materialization.NodeIdentity
		if _, duplicate := matched[key]; duplicate {
			return invalid("Worker materializations contain a duplicate Worker placement")
		}
		matched[key] = struct{}{}
		expected, present := placements[key]
		desired := expected.revision
		placement := expected.placement
		if !present || desired.Namespace != materialization.Namespace ||
			desired.WorkerPoolID.String() != materialization.WorkerPoolID ||
			desired.Revision != materialization.FleetRevision ||
			placement.NodeIdentity != materialization.NodeIdentity ||
			placement.WorkerRuntimeConfigMap != materialization.WorkerRuntimeConfigMap ||
			placement.RunnerProfilesConfigMap != materialization.RunnerProfilesConfigMap ||
			placement.RunnerGPURolesConfigMap != materialization.RunnerGPURolesConfigMap ||
			placement.WorkerControlTLSSecret != materialization.WorkerControlTLSSecret ||
			desired.ExecutionProfileRevisionID.String() != materialization.ExecutionProfileRevisionID ||
			desired.InferenceBackendRevision != materialization.InferenceBackendRevision {
			return invalid("Fleet desired revision does not match Worker materialization")
		}
	}
	return nil
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
	return contract.Kind == "ConfigMap" && strings.HasPrefix(name, contract.NamePrefix) &&
		len(name) > len(contract.NamePrefix) && !containsTemplateValue(name)
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
				if isImageField(key, parentKey) {
					if !validImage(stringValue) {
						return fmt.Errorf("field %s contains an unpinned or invalid OCI image %q", key, stringValue)
					}
					inventory.images[stringValue] = struct{}{}
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

func workerConsumerIdentity(input WorkerMaterializationInput) string {
	return "WorkerMaterialization/" + input.Namespace + "/" + input.NodeIdentity + "/" + input.WorkerID
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
	switch key {
	case "workerControlTLSSecret":
		return []string{"ca.crt", "tls.crt", "tls.key"}
	case "artifactStoreTLSSecret":
		return []string{"ca.crt"}
	default:
		return nil
	}
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
	return !containsTemplateValue(value) && imageref.ValidPinned(value)
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

func decodeConfigMap(encoded []byte, budget *yamlGraphBudget) (configMapDocument, error) {
	if _, err := decodeSingleYAMLNode(encoded, budget); err != nil {
		return configMapDocument{}, err
	}
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
	artifacts *artifactReader,
	input WorkerMaterializationInput,
	inventory *renderInventory,
	unique map[string]struct{},
	budget *yamlGraphBudget,
) (WorkerMaterialization, error) {
	if !validResourceName(input.NodeIdentity) || !validResourceName(input.Namespace) ||
		!canonicalUUID(input.WorkerID) || input.WorkerEpoch <= 0 || !canonicalUUID(input.WorkerPoolID) ||
		!isLowerHex(input.FleetRevision) || input.FleetRevision == strings.Repeat("0", 64) ||
		input.NodeAgentIdentity != expectedNodeAgentIdentity(input.NodeIdentity, input.WorkerID) ||
		!validSPIFFE(input.NodeAgentIdentity) || !validResourceName(input.WorkerRuntimeConfigMap) ||
		!validResourceName(input.RunnerProfilesConfigMap) || !validResourceName(input.RunnerGPURolesConfigMap) ||
		!validResourceName(input.WorkerControlTLSSecret) || !validDigest(input.WorkerControlTLSSecretRevision) ||
		!canonicalUUID(input.ExecutionProfileRevisionID) || !canonicalUUID(input.ModelRevisionID) ||
		!ValidRevision(input.InferenceBackendRevision) || len(input.InferenceBackendRevision) > 200 {
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

	runtimeArtifact, runtimeContent, err := artifacts.artifactFor(input.WorkerRuntimeRef, "application/yaml", maxYAMLArtifactBytes)
	if err != nil {
		return WorkerMaterialization{}, invalidf("read Worker runtime ConfigMap for %s: %v", input.NodeIdentity, err)
	}
	profilesArtifact, profilesContent, err := artifacts.artifactFor(input.RunnerProfilesRef, "application/yaml", maxYAMLArtifactBytes)
	if err != nil {
		return WorkerMaterialization{}, invalidf("read runner profiles ConfigMap for %s: %v", input.NodeIdentity, err)
	}
	gpuArtifact, gpuContent, err := artifacts.artifactFor(input.RunnerGPURolesRef, "application/yaml", maxYAMLArtifactBytes)
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

	runtimeConfig, err := validateRuntimeConfigMap(runtimeContent, input, budget)
	if err != nil {
		return WorkerMaterialization{}, invalidf("Worker runtime ConfigMap for %s: %v", input.NodeIdentity, err)
	}
	profilesConfig, err := validateProfilesConfigMap(profilesContent, input, budget)
	if err != nil {
		return WorkerMaterialization{}, invalidf("runner profiles ConfigMap for %s: %v", input.NodeIdentity, err)
	}
	gpuConfig, gpuIDs, err := validateGPURolesConfigMap(gpuContent, input, budget)
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
	recordReference(
		inventory,
		workerSecret,
		workerConsumerIdentity(input),
		[]string{"ca.crt", "tls.crt", "tls.key"},
	)
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

func validateRuntimeConfigMap(
	encoded []byte,
	input WorkerMaterializationInput,
	budget *yamlGraphBudget,
) (configMapDocument, error) {
	document, err := decodeConfigMap(encoded, budget)
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
	SchemaVersion   int                     `json:"schema_version"`
	BackendRevision string                  `json:"backend_revision"`
	Profiles        []profileAllowlistEntry `json:"profiles"`
}

type profileAllowlistEntry struct {
	ModelRevisionID            string `json:"model_revision_id"`
	GenerationPresetRevisionID string `json:"generation_preset_revision_id"`
	ExecutionProfileRevisionID string `json:"execution_profile_revision_id"`
	OutputSpecID               string `json:"output_spec_id"`
}

func validateProfilesConfigMap(
	encoded []byte,
	input WorkerMaterializationInput,
	budget *yamlGraphBudget,
) (configMapDocument, error) {
	document, err := decodeConfigMap(encoded, budget)
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
	seen := make(map[profileAllowlistEntry]struct{}, len(allowlist.Profiles))
	for _, profile := range allowlist.Profiles {
		if profile.ModelRevisionID != input.ModelRevisionID ||
			profile.ExecutionProfileRevisionID != input.ExecutionProfileRevisionID ||
			!canonicalUUID(profile.GenerationPresetRevisionID) || !canonicalUUID(profile.OutputSpecID) {
			return configMapDocument{}, errors.New("profile allowlist entry does not match materialized revisions")
		}
		if _, duplicate := seen[profile]; duplicate {
			return configMapDocument{}, errors.New("profile allowlist contains a duplicate entry")
		}
		seen[profile] = struct{}{}
	}
	return document, nil
}

type gpuRoles struct {
	SchemaVersion int      `json:"schema_version"`
	EncoderVAE    string   `json:"encoder_vae"`
	DiT           []string `json:"dit"`
}

func validateGPURolesConfigMap(
	encoded []byte,
	input WorkerMaterializationInput,
	budget *yamlGraphBudget,
) (configMapDocument, []string, error) {
	document, err := decodeConfigMap(encoded, budget)
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

func validOCIBlobDescriptor(descriptor ociBlobDescriptor) bool {
	if descriptor.MediaType == "" || !ValidRevision(descriptor.MediaType) ||
		!validDigest(descriptor.Digest) || descriptor.Size <= 0 || len(descriptor.URLs) != 0 || len(descriptor.Data) != 0 {
		return false
	}
	return true
}
