package deploymentcontract

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/fleetcontroller"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	k8syaml "sigs.k8s.io/yaml"
)

type fleetMetadata struct {
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`
}

type fleetCRD struct {
	APIVersion string        `yaml:"apiVersion"`
	Kind       string        `yaml:"kind"`
	Metadata   fleetMetadata `yaml:"metadata"`
	Spec       struct {
		Group string `yaml:"group"`
		Names struct {
			Kind     string `yaml:"kind"`
			Plural   string `yaml:"plural"`
			Singular string `yaml:"singular"`
		} `yaml:"names"`
		Scope    string `yaml:"scope"`
		Versions []struct {
			Name         string `yaml:"name"`
			Served       bool   `yaml:"served"`
			Storage      bool   `yaml:"storage"`
			Subresources struct {
				Status map[string]any `yaml:"status"`
			} `yaml:"subresources"`
			Schema struct {
				OpenAPIV3Schema fleetSchema `yaml:"openAPIV3Schema"`
			} `yaml:"schema"`
		} `yaml:"versions"`
	} `yaml:"spec"`
}

type fleetSchema struct {
	Type                   string                 `yaml:"type"`
	Required               []string               `yaml:"required"`
	Pattern                string                 `yaml:"pattern"`
	Enum                   []string               `yaml:"enum"`
	MinLength              *int64                 `yaml:"minLength"`
	MaxLength              *int64                 `yaml:"maxLength"`
	MinItems               *int64                 `yaml:"minItems"`
	MaxItems               *int64                 `yaml:"maxItems"`
	MinProperties          *int64                 `yaml:"minProperties"`
	MaxProperties          *int64                 `yaml:"maxProperties"`
	Minimum                *int64                 `yaml:"minimum"`
	Maximum                *int64                 `yaml:"maximum"`
	Properties             map[string]fleetSchema `yaml:"properties"`
	Items                  *fleetSchema           `yaml:"items"`
	AdditionalProperties   *fleetSchema           `yaml:"additionalProperties"`
	XKubernetesValidations []struct {
		Rule    string `yaml:"rule"`
		Message string `yaml:"message"`
	} `yaml:"x-kubernetes-validations"`
}

type fleetRBACDocument struct {
	APIVersion string        `yaml:"apiVersion"`
	Kind       string        `yaml:"kind"`
	Metadata   fleetMetadata `yaml:"metadata"`
	Rules      []struct {
		APIGroups []string `yaml:"apiGroups"`
		Resources []string `yaml:"resources"`
		Verbs     []string `yaml:"verbs"`
	} `yaml:"rules"`
	RoleRef struct {
		APIGroup string `yaml:"apiGroup"`
		Kind     string `yaml:"kind"`
		Name     string `yaml:"name"`
	} `yaml:"roleRef"`
	Subjects []struct {
		Kind      string `yaml:"kind"`
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"subjects"`
}

type fleetWebhookConfiguration struct {
	APIVersion string        `yaml:"apiVersion"`
	Kind       string        `yaml:"kind"`
	Metadata   fleetMetadata `yaml:"metadata"`
	Webhooks   []struct {
		Name                    string   `yaml:"name"`
		AdmissionReviewVersions []string `yaml:"admissionReviewVersions"`
		FailurePolicy           string   `yaml:"failurePolicy"`
		MatchPolicy             string   `yaml:"matchPolicy"`
		SideEffects             string   `yaml:"sideEffects"`
		TimeoutSeconds          int      `yaml:"timeoutSeconds"`
		ClientConfig            struct {
			Service struct {
				Name      string `yaml:"name"`
				Namespace string `yaml:"namespace"`
				Path      string `yaml:"path"`
				Port      int    `yaml:"port"`
			} `yaml:"service"`
			CABundle string `yaml:"caBundle"`
		} `yaml:"clientConfig"`
		ObjectSelector struct {
			MatchLabels map[string]string `yaml:"matchLabels"`
		} `yaml:"objectSelector"`
		Rules []struct {
			Operations  []string `yaml:"operations"`
			APIGroups   []string `yaml:"apiGroups"`
			APIVersions []string `yaml:"apiVersions"`
			Resources   []string `yaml:"resources"`
			Scope       string   `yaml:"scope"`
		} `yaml:"rules"`
	} `yaml:"webhooks"`
}

func TestFleetWorkerPoolCRDRequiresImmutableH3Revision(t *testing.T) {
	var crd fleetCRD
	loadFleetManifest(t, "workerpool-crd.yaml", &crd)
	if crd.APIVersion != "apiextensions.k8s.io/v1" || crd.Kind != "CustomResourceDefinition" ||
		crd.Metadata.Name != "workerpools.fleet.vela.ai" || crd.Spec.Group != "fleet.vela.ai" ||
		crd.Spec.Scope != "Namespaced" || crd.Spec.Names.Kind != "WorkerPool" ||
		crd.Spec.Names.Plural != "workerpools" || crd.Spec.Names.Singular != "workerpool" {
		t.Fatalf("WorkerPool CRD identity = %#v", crd)
	}
	if len(crd.Spec.Versions) != 1 {
		t.Fatalf("WorkerPool CRD versions = %#v", crd.Spec.Versions)
	}
	version := crd.Spec.Versions[0]
	if version.Name != "v1alpha1" || !version.Served || !version.Storage ||
		version.Subresources.Status == nil {
		t.Fatalf("WorkerPool CRD version = %#v", version)
	}
	root := version.Schema.OpenAPIV3Schema
	if root.Type != "object" || !sameStrings(root.Required, []string{"apiVersion", "kind", "metadata", "spec"}) {
		t.Fatalf("WorkerPool root schema = %#v", root)
	}
	spec := root.Properties["spec"]
	if !sameStrings(spec.Required, []string{
		"capacityPolicy", "nodeSelector", "placements", "revision", "workerProfile",
	}) ||
		len(spec.XKubernetesValidations) != 1 || spec.XKubernetesValidations[0].Rule != "self == oldSelf" {
		t.Fatalf("WorkerPool immutable spec schema = %#v", spec)
	}
	if _, obsolete := spec.Properties["daemonSetName"]; obsolete {
		t.Fatal("WorkerPool CRD still exposes the obsolete pool-wide daemonSetName")
	}
	if spec.Properties["revision"].Pattern != "^[0-9a-f]{64}$" ||
		!reflect.DeepEqual(spec.Properties["workerProfile"].Enum, []string{"h3"}) {
		t.Fatalf("WorkerPool revision/profile schema = %#v", spec.Properties)
	}
	selector := spec.Properties["nodeSelector"]
	if schemaInt64(selector.MinProperties) != 2 || schemaInt64(selector.MaxProperties) != 64 ||
		selector.AdditionalProperties == nil ||
		schemaInt64(selector.AdditionalProperties.MaxLength) != 63 ||
		!hasFleetValidation(selector, "!('kubernetes.io/hostname' in self)") {
		t.Fatalf("WorkerPool node selector schema = %#v", selector)
	}
	placements := spec.Properties["placements"]
	if placements.Type != "array" || schemaInt64(placements.MinItems) != 1 ||
		schemaInt64(placements.MaxItems) != 1024 || placements.Items == nil ||
		!sameStrings(placements.Items.Required, []string{
			"daemonSetName", "nodeIdentity", "runnerGPURolesConfigMap",
			"runnerProfilesConfigMap", "workerControlTLSSecret", "workerRuntimeConfigMap",
		}) {
		t.Fatalf("WorkerPool placements schema = %#v", placements)
	}
	for _, field := range []string{"nodeIdentity", "daemonSetName"} {
		property := placements.Items.Properties[field]
		if schemaInt64(property.MinLength) != 1 || schemaInt64(property.MaxLength) != 63 || property.Pattern == "" {
			t.Fatalf("WorkerPool placement label field %q schema = %#v", field, property)
		}
	}
	for _, field := range []string{
		"workerRuntimeConfigMap", "runnerProfilesConfigMap", "runnerGPURolesConfigMap",
		"workerControlTLSSecret",
	} {
		property := placements.Items.Properties[field]
		if schemaInt64(property.MinLength) != 1 || schemaInt64(property.MaxLength) != 253 || property.Pattern == "" {
			t.Fatalf("WorkerPool placement material field %q schema = %#v", field, property)
		}
	}
	if _, obsolete := root.Properties["status"].Properties["daemonSetName"]; obsolete {
		t.Fatal("WorkerPool status still exposes the obsolete daemonSetName")
	}
	capacity := spec.Properties["capacityPolicy"]
	if capacity.Type != "object" || !sameStrings(capacity.Required, []string{
		"observationMaxAgeSeconds", "poolHighWatermarkBytes", "poolLowWatermarkBytes",
		"workerCriticalFreeBytes", "workerHighWatermarkBytes", "workerLowWatermarkBytes",
	}) || schemaMinimum(capacity.Properties["workerHighWatermarkBytes"]) != 1 ||
		schemaMinimum(capacity.Properties["workerLowWatermarkBytes"]) != 0 ||
		schemaMinimum(capacity.Properties["workerCriticalFreeBytes"]) != 0 ||
		schemaMinimum(capacity.Properties["poolHighWatermarkBytes"]) != 1 ||
		schemaMinimum(capacity.Properties["poolLowWatermarkBytes"]) != 0 ||
		schemaMinimum(capacity.Properties["observationMaxAgeSeconds"]) != 10 ||
		schemaMaximum(capacity.Properties["observationMaxAgeSeconds"]) != 600 {
		t.Fatalf("WorkerPool capacity policy schema = %#v", capacity)
	}
}

func TestFleetControllerRBACIsNamespaceBoundAndNodeReadOnly(t *testing.T) {
	documents := loadFleetRBACDocuments(t)
	if strings.Contains(string(readFleetManifest(t, "rbac.yaml")), "argocd") {
		t.Fatal("Fleet RBAC grants an Argo identity")
	}
	roles := map[string]fleetRBACDocument{}
	for _, document := range documents {
		roles[document.Kind] = document
		for _, rule := range document.Rules {
			for _, value := range append(append([]string{}, rule.APIGroups...), append(rule.Resources, rule.Verbs...)...) {
				if value == "*" {
					t.Fatalf("%s contains wildcard RBAC: %#v", document.Kind, rule)
				}
			}
		}
		if strings.HasSuffix(document.Kind, "Binding") {
			if len(document.Subjects) != 1 || document.Subjects[0].Kind != "ServiceAccount" ||
				document.Subjects[0].Name != "vela-fleet-controller" ||
				document.Subjects[0].Namespace != "vela-system" {
				t.Fatalf("%s subjects = %#v", document.Kind, document.Subjects)
			}
		}
	}
	role := roles["Role"]
	if role.Metadata.Namespace != "vela-system" {
		t.Fatalf("Fleet namespaced Role = %#v", role)
	}
	wantRoleRules := map[string][]string{
		"fleet.vela.ai|workerpools":              {"get", "list", "watch", "create", "update", "patch", "delete"},
		"fleet.vela.ai|workerpools/status":       {"get", "update", "patch"},
		"apps|daemonsets":                        {"get", "list", "watch", "create", "update", "patch", "delete"},
		"|pods":                                  {"get", "list", "watch", "create", "update", "patch", "delete"},
		"resource.k8s.io|resourceclaimtemplates": {"get", "list", "watch", "create"},
	}
	if len(role.Rules) != len(wantRoleRules) {
		t.Fatalf("Fleet Role rules = %#v", role.Rules)
	}
	for _, rule := range role.Rules {
		if len(rule.APIGroups) != 1 || len(rule.Resources) != 1 {
			t.Fatalf("Fleet Role rule is not exact: %#v", rule)
		}
		key := rule.APIGroups[0] + "|" + rule.Resources[0]
		verbs, ok := wantRoleRules[key]
		if !ok || !sameStrings(rule.Verbs, verbs) {
			t.Fatalf("Fleet Role rule %q = %#v", key, rule)
		}
		delete(wantRoleRules, key)
	}
	if len(wantRoleRules) != 0 {
		t.Fatalf("Fleet Role omitted rules: %#v", wantRoleRules)
	}
	clusterRole := roles["ClusterRole"]
	if len(clusterRole.Rules) != 1 ||
		!sameStrings(clusterRole.Rules[0].APIGroups, []string{""}) ||
		!sameStrings(clusterRole.Rules[0].Resources, []string{"nodes"}) ||
		!sameStrings(clusterRole.Rules[0].Verbs, []string{"get", "list", "watch"}) {
		t.Fatalf("Fleet ClusterRole is not Node read-only: %#v", clusterRole)
	}
	for _, kind := range []string{"RoleBinding", "ClusterRoleBinding"} {
		if _, ok := roles[kind]; !ok {
			t.Fatalf("Fleet RBAC is missing %s", kind)
		}
	}
}

func TestFleetAdmissionWebhookFailsClosedForEveryProtectedResource(t *testing.T) {
	var configuration fleetWebhookConfiguration
	loadFleetManifest(t, "validating-webhook.yaml", &configuration)
	if configuration.APIVersion != "admissionregistration.k8s.io/v1" ||
		configuration.Kind != "ValidatingWebhookConfiguration" || len(configuration.Webhooks) != 1 {
		t.Fatalf("Fleet webhook configuration = %#v", configuration)
	}
	webhook := configuration.Webhooks[0]
	if webhook.Name != "fleet-protection.vela.ai" || webhook.FailurePolicy != "Fail" ||
		webhook.MatchPolicy != "Exact" || webhook.SideEffects != "None" ||
		webhook.TimeoutSeconds <= 0 || webhook.TimeoutSeconds > 5 ||
		!sameStrings(webhook.AdmissionReviewVersions, []string{"v1"}) ||
		webhook.ClientConfig.Service.Name != "vela-fleet-admission" ||
		webhook.ClientConfig.Service.Namespace != "vela-system" ||
		webhook.ClientConfig.Service.Path != "/validate" ||
		webhook.ClientConfig.Service.Port != 443 || webhook.ClientConfig.CABundle == "" ||
		webhook.ObjectSelector.MatchLabels["vela.ai/fleet-protected"] != "true" {
		t.Fatalf("Fleet webhook contract = %#v", webhook)
	}
	wantRules := map[string][]string{
		"/v1":                    {"pods"},
		"apps/v1":                {"daemonsets"},
		"fleet.vela.ai/v1alpha1": {"workerpools"},
	}
	if len(webhook.Rules) != len(wantRules) {
		t.Fatalf("Fleet webhook rules = %#v", webhook.Rules)
	}
	for _, rule := range webhook.Rules {
		if !sameStrings(rule.Operations, []string{"CREATE", "DELETE", "UPDATE"}) ||
			rule.Scope != "Namespaced" || len(rule.APIGroups) != 1 || len(rule.APIVersions) != 1 {
			t.Fatalf("Fleet webhook rule = %#v", rule)
		}
		key := rule.APIGroups[0] + "/" + rule.APIVersions[0]
		if !sameStrings(rule.Resources, wantRules[key]) {
			t.Fatalf("Fleet webhook rule %q resources = %#v", key, rule.Resources)
		}
		delete(wantRules, key)
	}
	if len(wantRules) != 0 {
		t.Fatalf("Fleet webhook omitted rules: %#v", wantRules)
	}
}

func TestFleetControllerDeploymentRunsReplicatedHardenedRuntime(t *testing.T) {
	var deployment appsv1.Deployment
	if err := k8syaml.Unmarshal(readFleetManifest(t, "deployment.yaml"), &deployment); err != nil {
		t.Fatalf("parse Fleet Deployment: %v", err)
	}
	if deployment.APIVersion != "apps/v1" || deployment.Kind != "Deployment" ||
		deployment.Name != "vela-fleet-controller" || deployment.Namespace != "vela-system" ||
		deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 2 ||
		deployment.Spec.Strategy.Type != appsv1.RollingUpdateDeploymentStrategyType ||
		deployment.Spec.Strategy.RollingUpdate == nil ||
		deployment.Spec.Strategy.RollingUpdate.MaxUnavailable == nil ||
		deployment.Spec.Strategy.RollingUpdate.MaxUnavailable.IntValue() != 0 {
		t.Fatalf("Fleet Deployment identity/availability = %#v", deployment)
	}
	pod := deployment.Spec.Template.Spec
	if pod.ServiceAccountName != "vela-fleet-controller" ||
		pod.AutomountServiceAccountToken == nil || !*pod.AutomountServiceAccountToken ||
		pod.SecurityContext == nil || pod.SecurityContext.RunAsNonRoot == nil ||
		!*pod.SecurityContext.RunAsNonRoot || len(pod.Containers) != 1 {
		t.Fatalf("Fleet Deployment Pod contract = %#v", pod)
	}
	controller := pod.Containers[0]
	if controller.Name != "fleet-controller" ||
		!strings.Contains(controller.Image, "@sha256:") ||
		controller.SecurityContext == nil ||
		controller.SecurityContext.AllowPrivilegeEscalation == nil ||
		*controller.SecurityContext.AllowPrivilegeEscalation ||
		controller.SecurityContext.ReadOnlyRootFilesystem == nil ||
		!*controller.SecurityContext.ReadOnlyRootFilesystem ||
		controller.ReadinessProbe == nil || controller.ReadinessProbe.HTTPGet == nil ||
		controller.ReadinessProbe.HTTPGet.Scheme != corev1.URISchemeHTTPS ||
		controller.ReadinessProbe.HTTPGet.Path != "/readyz" ||
		controller.LivenessProbe == nil || controller.LivenessProbe.HTTPGet == nil ||
		controller.LivenessProbe.HTTPGet.Scheme != corev1.URISchemeHTTPS ||
		controller.LivenessProbe.HTTPGet.Path != "/healthz" {
		t.Fatalf("Fleet controller container contract = %#v", controller)
	}
	namespace := requireFleetEnvironment(t, controller, "VELA_FLEET_NAMESPACE")
	if namespace.ValueFrom == nil || namespace.ValueFrom.FieldRef == nil ||
		namespace.ValueFrom.FieldRef.FieldPath != "metadata.namespace" ||
		requireFleetEnvironment(t, controller, "VELA_FLEET_RESIDENCY_PLAN_ROLLOUTS_FILE").Value !=
			"/etc/vela-fleet/residency-plan-rollouts/rollouts.json" ||
		requireFleetEnvironment(t, controller, "VELA_FLEET_ADMISSION_ADDRESS").Value != ":9443" ||
		requireFleetEnvironment(t, controller, "VELA_FLEET_ADMISSION_CLIENT_CA_FILE").Value !=
			"/etc/vela-fleet/admission-client-ca/ca.crt" ||
		requireFleetEnvironment(t, controller, "VELA_FLEET_ADMISSION_CLIENT_SPIFFE_ID").Value !=
			"spiffe://vela.internal/kube-apiserver/admission" {
		t.Fatalf("Fleet controller environment = %#v", controller.Env)
	}
	for _, name := range []string{
		"residency-plan-rollouts", "control-tls", "admission-tls", "admission-client-ca",
	} {
		if !hasFleetVolume(pod.Volumes, name) || !hasFleetVolumeMount(controller.VolumeMounts, name) {
			t.Fatalf("Fleet controller is missing volume %q", name)
		}
	}

	var disruptionBudget policyv1.PodDisruptionBudget
	if err := k8syaml.Unmarshal(
		readFleetManifest(t, "disruption-budget.yaml"),
		&disruptionBudget,
	); err != nil {
		t.Fatalf("parse Fleet PDB: %v", err)
	}
	if disruptionBudget.Spec.MinAvailable == nil ||
		disruptionBudget.Spec.MinAvailable.IntValue() != 1 ||
		disruptionBudget.Spec.Selector == nil ||
		disruptionBudget.Spec.Selector.MatchLabels["app.kubernetes.io/name"] !=
			"vela-fleet-controller" {
		t.Fatalf("Fleet disruption budget = %#v", disruptionBudget)
	}
}

func TestFleetDefaultRenderUsesTargetOnlySingleGPUResidencyPlan(t *testing.T) {
	var config corev1.ConfigMap
	if err := k8syaml.Unmarshal(
		readFleetManifest(t, "residency-plan-rollouts.yaml"),
		&config,
	); err != nil {
		t.Fatalf("parse Fleet ResidencyPlan ConfigMap: %v", err)
	}
	payload := config.Data["rollouts.json"]
	var input struct {
		SchemaVersion int                                    `json:"schema_version"`
		Rollouts      []fleetcontroller.ResidencyPlanRollout `json:"rollouts"`
	}
	if err := json.Unmarshal([]byte(payload), &input); err != nil {
		t.Fatalf("decode Fleet ResidencyPlan placeholder JSON: %v", err)
	}
	if config.Immutable == nil || !*config.Immutable || input.SchemaVersion != 1 ||
		len(input.Rollouts) != 1 || len(input.Rollouts[0].WorkerBundles) != 1 ||
		len(input.Rollouts[0].WorkerBundles[0].WorkerInstances) != 1 {
		t.Fatalf("Fleet ResidencyPlan placeholder = %#v input=%#v", config, input)
	}
	bundle := input.Rollouts[0].WorkerBundles[0]
	digest, err := fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
	if err != nil || digest != bundle.RevisionDigest {
		t.Fatalf("Fleet ResidencyPlan placeholder digest=%q want=%q error=%v", digest, bundle.RevisionDigest, err)
	}
	worker := bundle.WorkerInstances[0]
	if worker.Role != "dit" || worker.CapacitySlots != 1 || len(worker.ModelRuntimes) != 1 ||
		worker.ModelRuntimes[0].Component != "DIT" || len(worker.Members) != 1 ||
		worker.Members[0].DeviceCount != 1 || len(worker.Members[0].DeviceConstraints) != 1 {
		t.Fatalf("Fleet single-GPU placeholder WorkerInstance = %#v", worker)
	}
	kustomization := string(readFleetManifest(t, "kustomization.yaml"))
	if !strings.Contains(kustomization, "residency-plan-rollouts.yaml") ||
		strings.Contains(kustomization, "desired-revisions.yaml") {
		t.Fatalf("Fleet default Kustomization is not target-only:\n%s", kustomization)
	}
}

func TestFleetLegacyDesiredInputRemainsExplicitRollbackOnly(t *testing.T) {
	var desiredConfig corev1.ConfigMap
	if err := k8syaml.Unmarshal(
		readFleetManifest(t, "desired-revisions.yaml"),
		&desiredConfig,
	); err != nil {
		t.Fatalf("parse Fleet desired ConfigMap: %v", err)
	}
	payload := desiredConfig.Data["desired.yaml"]
	if desiredConfig.Immutable == nil || !*desiredConfig.Immutable || payload == "" {
		t.Fatalf("Fleet desired ConfigMap = %#v", desiredConfig)
	}
	for _, required := range []string{
		"kind: FleetDesiredRevisions",
		"revision: 0000000000000000000000000000000000000000000000000000000000000000",
		"initImage: docker.io/library/busybox@sha256:7a3ebe5bfd1a4a19797d20b0c0bb39d44393e9a03fd852c0865b0f540d868df0",
		"workerRuntimeConfigMap: vela-worker-runtime-placeholder",
		"placements:",
		"nodeIdentity: replace-with-registered-node-identity",
		"daemonSetName: h3-worker-pool-primary-node-placeholder",
		"runnerProfilesConfigMap: vela-runner-profiles-placeholder",
		"runnerGPURolesConfigMap: vela-runner-gpu-roles-placeholder",
		"workerControlTLSSecret: vela-worker-control-mtls-placeholder",
		"artifactStoreTLSSecret: vela-artifact-store-ca-placeholder",
		"workerHighWatermarkBytes: 800000000000",
		"workerLowWatermarkBytes: 700000000000",
		"workerCriticalFreeBytes: 100000000000",
		"poolHighWatermarkBytes: 5600000000000",
		"poolLowWatermarkBytes: 4900000000000",
		"observationMaxAge: 2m",
		"retirements: []",
	} {
		if !strings.Contains(payload, required) {
			t.Fatalf("Fleet desired input omitted %q", required)
		}
	}
	if strings.Contains(payload, "workerProfile: h3\n        daemonSetName:") {
		t.Fatal("Fleet desired input still uses the obsolete pool-wide daemonSetName")
	}
}

func schemaMinimum(schema fleetSchema) int64 {
	if schema.Minimum == nil {
		return -1
	}
	return *schema.Minimum
}

func schemaMaximum(schema fleetSchema) int64 {
	if schema.Maximum == nil {
		return -1
	}
	return *schema.Maximum
}

func schemaInt64(value *int64) int64 {
	if value == nil {
		return -1
	}
	return *value
}

func hasFleetValidation(schema fleetSchema, rule string) bool {
	for _, validation := range schema.XKubernetesValidations {
		if validation.Rule == rule {
			return true
		}
	}
	return false
}

func requireFleetEnvironment(
	t *testing.T,
	container corev1.Container,
	name string,
) corev1.EnvVar {
	t.Helper()
	for _, environment := range container.Env {
		if environment.Name == name {
			return environment
		}
	}
	t.Fatalf("Fleet controller environment %q is missing", name)
	return corev1.EnvVar{}
}

func hasFleetVolume(volumes []corev1.Volume, name string) bool {
	for _, volume := range volumes {
		if volume.Name == name {
			return true
		}
	}
	return false
}

func hasFleetVolumeMount(mounts []corev1.VolumeMount, name string) bool {
	for _, mount := range mounts {
		if mount.Name == name && mount.ReadOnly {
			return true
		}
	}
	return false
}

func loadFleetManifest(t *testing.T, name string, destination any) {
	t.Helper()
	if err := yaml.Unmarshal(readFleetManifest(t, name), destination); err != nil {
		t.Fatalf("parse Fleet manifest %q: %v", name, err)
	}
}

func loadFleetRBACDocuments(t *testing.T) []fleetRBACDocument {
	t.Helper()
	decoder := yaml.NewDecoder(strings.NewReader(string(readFleetManifest(t, "rbac.yaml"))))
	var documents []fleetRBACDocument
	for {
		var document fleetRBACDocument
		err := decoder.Decode(&document)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("parse Fleet RBAC: %v", err)
		}
		if document.Kind != "" {
			documents = append(documents, document)
		}
	}
	return documents
}

func readFleetManifest(t *testing.T, name string) []byte {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve Fleet deployment contract test path")
	}
	path := filepath.Join(filepath.Dir(currentFile), "..", "..", "deploy", "fleet-controller", name)
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Fleet manifest %q: %v", name, err)
	}
	return payload
}

func sameStrings(left, right []string) bool {
	left = append([]string(nil), left...)
	right = append([]string(nil), right...)
	sort.Strings(left)
	sort.Strings(right)
	return reflect.DeepEqual(left, right)
}
