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

	"github.com/google/uuid"
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
		"|pods":                                  {"get", "list", "watch", "create", "update", "patch", "delete"},
		"|services":                              {"get", "create"},
		"|secrets":                               {"get", "create"},
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
		"/v1": {"pods", "secrets", "services"},
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

func TestFleetDefaultRenderUsesCompleteH3ResidencyPlan(t *testing.T) {
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
		len(input.Rollouts) != 1 || len(input.Rollouts[0].WorkerBundles) != 2 ||
		len(input.Rollouts[0].ApprovedPlan.CapacityPools) != 6 ||
		len(input.Rollouts[0].ApprovedPlan.WorkerInstances) != 11 {
		t.Fatalf("Fleet ResidencyPlan placeholder = %#v input=%#v", config, input)
	}
	roleCounts := map[string]int{}
	componentCounts := map[string]int{}
	poolByComponent := map[string]uuid.UUID{}
	workerCount := 0
	var aux fleetcontroller.WorkerInstanceActuation
	for _, bundle := range input.Rollouts[0].WorkerBundles {
		digest, err := fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
		if err != nil || digest != bundle.RevisionDigest {
			t.Fatalf(
				"Fleet ResidencyPlan bundle %s digest=%q want=%q error=%v",
				bundle.WorkerBundleID, digest, bundle.RevisionDigest, err,
			)
		}
		workerCount += len(bundle.WorkerInstances)
		for _, worker := range bundle.WorkerInstances {
			roleCounts[worker.Role]++
			if worker.Role == "aux" {
				aux = worker
			}
			if worker.CapacitySlots != 1 || len(worker.Members) != 1 ||
				worker.Members[0].DeviceCount != 1 || len(worker.Members[0].DeviceConstraints) != 1 {
				t.Fatalf("Fleet H3 WorkerInstance is not one slot/device/member: %#v", worker)
			}
			for _, runtime := range worker.ModelRuntimes {
				componentCounts[runtime.Component]++
				if runtime.CapacityPoolID == uuid.Nil || runtime.StageProfileRevisionID == uuid.Nil ||
					runtime.ModelResidencyID == uuid.Nil || len(runtime.Command) == 0 ||
					!strings.HasPrefix(runtime.Command[0], "/opt/vela/bin/h3-") {
					t.Fatalf("Fleet H3 ModelRuntime route is incomplete: %#v", runtime)
				}
				if runtime.Component == "CPU_MEDIA" {
					if worker.Members[0].ResourceClass != "CPU" || len(runtime.Command) != 3 ||
						runtime.Command[2] != worker.Role {
						t.Fatalf("Fleet H3 CPU media runtime is incomplete: worker=%#v runtime=%#v", worker, runtime)
					}
					continue
				}
				if worker.Members[0].ResourceClass != "GPU" || len(runtime.Command) != 1 ||
					!containsFleetEnvironmentPrefix(runtime.Environment, "FAST_H3_WARMUP_SPEC_PATH=/opt/fast-h3/warmup/") ||
					!containsFleetEnvironmentPrefix(runtime.Environment, "CUDA_VISIBLE_DEVICES=GPU-") {
					t.Fatalf("Fleet H3 GPU ModelRuntime route is incomplete: %#v", runtime)
				}
				if existing, seen := poolByComponent[runtime.Component]; seen &&
					existing != runtime.CapacityPoolID {
					t.Fatalf("component %s spans CapacityPools %s/%s", runtime.Component, existing, runtime.CapacityPoolID)
				}
				poolByComponent[runtime.Component] = runtime.CapacityPoolID
			}
		}
	}
	if workerCount != 11 {
		t.Fatalf("Fleet H3 WorkerInstance count=%d, want 11", workerCount)
	}
	if roleCounts["aux"] != 1 || roleCounts["dit"] != 7 ||
		roleCounts["cpu-encode"] != 1 || roleCounts["cpu-mux"] != 1 ||
		roleCounts["cpu-thumbnail"] != 1 ||
		componentCounts["ENCODER"] != 1 || componentCounts["DIT"] != 7 ||
		componentCounts["VAE_DECODER"] != 1 || componentCounts["CPU_MEDIA"] != 3 ||
		poolByComponent["ENCODER"] == poolByComponent["VAE_DECODER"] {
		t.Fatalf("Fleet H3 topology roles=%v components=%v pools=%v", roleCounts, componentCounts, poolByComponent)
	}
	if aux.Role != "aux" || aux.SharedSlotException != "H3_AUX_ENCODER_VAE" ||
		len(aux.ModelRuntimes) != 2 {
		t.Fatalf("Fleet H3 AUX WorkerInstance = %#v", aux)
	}
	if err := fleetcontroller.ValidateResidencyPlanRollout(input.Rollouts[0]); err != nil {
		t.Fatalf("validate complete H3 ResidencyPlan: %v", err)
	}
	kustomization := string(readFleetManifest(t, "kustomization.yaml"))
	if !strings.Contains(kustomization, "residency-plan-rollouts.yaml") ||
		strings.Contains(kustomization, "desired-revisions.yaml") {
		t.Fatalf("Fleet default Kustomization is not target-only:\n%s", kustomization)
	}
}

func containsFleetEnvironmentPrefix(environment []string, prefix string) bool {
	for _, value := range environment {
		if strings.HasPrefix(value, prefix) {
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
