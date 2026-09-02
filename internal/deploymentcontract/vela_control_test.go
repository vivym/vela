package deploymentcontract

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"
	k8syamlutil "k8s.io/apimachinery/pkg/util/yaml"
)

var pinnedVelaControlImage = regexp.MustCompile(`^[^[:space:]]+@sha256:[0-9a-f]{64}$`)

type velaControlSecretContract struct {
	APIVersion      string `json:"apiVersion"`
	Kind            string `json:"kind"`
	ReleaseRevision string `json:"releaseRevision"`
	RotationPolicy  struct {
		Mode                   string `json:"mode"`
		InPlaceUpdateSupported bool   `json:"inPlaceUpdateSupported"`
		ImmutableRequired      bool   `json:"immutableRequired"`
	} `json:"rotationPolicy"`
	EnvironmentSecrets []velaControlSecretEntry `json:"environmentSecrets"`
	FileSecrets        []velaControlSecretEntry `json:"fileSecrets"`
}

type velaControlSecretEntry struct {
	Name              string            `json:"name"`
	RequiredKeys      []string          `json:"requiredKeys"`
	ProjectedPath     string            `json:"projectedPath,omitempty"`
	MaterializedPaths map[string]string `json:"materializedPaths,omitempty"`
}

type velaControlStorageContract struct {
	APIVersion          string `json:"apiVersion"`
	Kind                string `json:"kind"`
	StorageClassName    string `json:"storageClassName"`
	VolumeBindingMode   string `json:"volumeBindingMode"`
	ReclaimPolicy       string `json:"reclaimPolicy"`
	CapacityPerClaim    string `json:"capacityPerClaim"`
	MinimumActiveClaims int    `json:"minimumActiveClaims"`
	ReclaimHeadroom     int    `json:"terminationAndReclaimHeadroomClaims"`
	MinimumClaims       int    `json:"minimumAggregateClaims"`
	AggregateCapacity   string `json:"minimumAggregateCapacity"`
	DedicatedPool       bool   `json:"dedicatedCapacityPoolRequired"`
	IOPSLimit           bool   `json:"iopsLimitRequired"`
	ThroughputLimit     bool   `json:"throughputLimitRequired"`
	LiveReceiptRequired bool   `json:"liveReceiptRequired"`
}

func TestVelaControlDeploymentRunsReplicatedHardenedRuntime(t *testing.T) {
	var deployment appsv1.Deployment
	loadVelaControlManifest(t, "deployment.yaml", &deployment)

	if deployment.APIVersion != "apps/v1" || deployment.Kind != "Deployment" ||
		deployment.Name != "vela-control" || deployment.Namespace != "vela-system" ||
		deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 2 ||
		deployment.Spec.Strategy.Type != appsv1.RollingUpdateDeploymentStrategyType ||
		deployment.Spec.Strategy.RollingUpdate == nil ||
		deployment.Spec.Strategy.RollingUpdate.MaxUnavailable == nil ||
		deployment.Spec.Strategy.RollingUpdate.MaxUnavailable.IntValue() != 0 ||
		deployment.Spec.Strategy.RollingUpdate.MaxSurge == nil ||
		deployment.Spec.Strategy.RollingUpdate.MaxSurge.IntValue() != 1 {
		t.Fatalf("vela-control Deployment identity/availability = %#v", deployment)
	}
	pod := deployment.Spec.Template.Spec
	if pod.ServiceAccountName != "vela-control" ||
		pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken ||
		pod.TerminationGracePeriodSeconds == nil || *pod.TerminationGracePeriodSeconds < 60 ||
		pod.PriorityClassName != "vela-control-critical" ||
		pod.NodeSelector["vela.ai/node-role"] != "control-storage" ||
		pod.SecurityContext == nil || pod.SecurityContext.RunAsNonRoot == nil ||
		!*pod.SecurityContext.RunAsNonRoot || pod.SecurityContext.RunAsUser == nil ||
		*pod.SecurityContext.RunAsUser != 10001 || pod.SecurityContext.RunAsGroup == nil ||
		*pod.SecurityContext.RunAsGroup != 10001 || pod.SecurityContext.FSGroup == nil ||
		*pod.SecurityContext.FSGroup != 10001 || pod.SecurityContext.SeccompProfile == nil ||
		pod.SecurityContext.SeccompProfile.Type != corev1.SeccompProfileTypeRuntimeDefault {
		t.Fatalf("vela-control Pod identity/security = %#v", pod)
	}
	requiredAntiAffinity := pod.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if len(requiredAntiAffinity) != 1 ||
		requiredAntiAffinity[0].TopologyKey != "kubernetes.io/hostname" ||
		requiredAntiAffinity[0].LabelSelector == nil ||
		requiredAntiAffinity[0].LabelSelector.MatchLabels["app.kubernetes.io/name"] != "vela-control" {
		t.Fatalf("vela-control required anti-affinity = %#v", requiredAntiAffinity)
	}
	if len(pod.InitContainers) != 1 || len(pod.Containers) != 1 {
		t.Fatalf("vela-control containers = init:%#v app:%#v", pod.InitContainers, pod.Containers)
	}
	materializer := pod.InitContainers[0]
	if materializer.Name != "secret-materializer" || !pinnedVelaControlImage.MatchString(materializer.Image) {
		t.Fatalf("vela-control materializer image = %q", materializer.Image)
	}
	requireVelaControlSecurityContext(t, materializer, true, []corev1.Capability{"CHOWN"})
	control := pod.Containers[0]
	if control.Name != "vela-control" || !pinnedVelaControlImage.MatchString(control.Image) {
		t.Fatalf("vela-control image = %q", control.Image)
	}
	requireVelaControlSecurityContext(t, control, false, nil)
	requireVelaControlResources(t, control)
	requireVelaControlPort(t, control, "http", 8080)
	requireVelaControlPort(t, control, "management", 8081)
	requireVelaControlPort(t, control, "worker-grpc", 8443)
	requireVelaControlPort(t, control, "fleet-grpc", 8444)
	requireVelaControlPort(t, control, "finance-https", 8445)
	requireVelaControlPort(t, control, "compliance-https", 8446)
	requireVelaControlPort(t, control, "stage-wkr-grpc", 8447)
	for name, probe := range map[string]*corev1.Probe{
		"startup": control.StartupProbe, "readiness": control.ReadinessProbe,
		"liveness": control.LivenessProbe,
	} {
		if probe == nil || probe.HTTPGet == nil || probe.HTTPGet.Port.StrVal != "management" {
			t.Fatalf("vela-control %s probe = %#v", name, probe)
		}
		wantPath := "/readyz"
		if name == "liveness" {
			wantPath = "/healthz"
		}
		if probe.HTTPGet.Path != wantPath || probe.HTTPGet.Scheme != corev1.URISchemeHTTP {
			t.Fatalf("vela-control %s probe = %#v", name, probe)
		}
	}

	var disruptionBudget policyv1.PodDisruptionBudget
	loadVelaControlManifest(t, "disruption-budget.yaml", &disruptionBudget)
	if disruptionBudget.Spec.MinAvailable == nil ||
		disruptionBudget.Spec.MinAvailable.IntValue() != 1 ||
		disruptionBudget.Spec.Selector == nil ||
		disruptionBudget.Spec.Selector.MatchLabels["app.kubernetes.io/name"] != "vela-control" {
		t.Fatalf("vela-control disruption budget = %#v", disruptionBudget)
	}

	var priorityClass schedulingv1.PriorityClass
	loadVelaControlManifest(t, "priority-class.yaml", &priorityClass)
	if priorityClass.Name != "vela-control-critical" || priorityClass.Value <= 0 ||
		priorityClass.GlobalDefault || priorityClass.PreemptionPolicy == nil ||
		*priorityClass.PreemptionPolicy != corev1.PreemptNever {
		t.Fatalf("vela-control PriorityClass = %#v", priorityClass)
	}
}

func TestVelaControlMaterializesSecretsAndUsesUniqueClaimantIdentities(t *testing.T) {
	var deployment appsv1.Deployment
	loadVelaControlManifest(t, "deployment.yaml", &deployment)
	pod := deployment.Spec.Template.Spec
	control := pod.Containers[0]
	if deployment.Spec.Template.Labels["vela.ai/release-revision"] != "r0-placeholder" {
		t.Fatalf("vela-control Pod template release revision = %#v", deployment.Spec.Template.Labels)
	}
	if len(control.EnvFrom) != 3 || control.EnvFrom[0].ConfigMapRef == nil ||
		control.EnvFrom[0].ConfigMapRef.Name != "vela-control-runtime-r0-placeholder" ||
		control.EnvFrom[1].SecretRef == nil ||
		control.EnvFrom[1].SecretRef.Name != "vela-control-database-urls-r0-placeholder" ||
		control.EnvFrom[2].SecretRef == nil ||
		control.EnvFrom[2].SecretRef.Name != "vela-control-credential-pepper-r0-placeholder" {
		t.Fatalf("vela-control environment sources = %#v", control.EnvFrom)
	}
	podUID := requireVelaControlEnvironment(t, control, "VELA_POD_UID")
	if podUID.ValueFrom == nil || podUID.ValueFrom.FieldRef == nil ||
		podUID.ValueFrom.FieldRef.FieldPath != "metadata.uid" {
		t.Fatalf("vela-control Pod UID environment = %#v", podUID)
	}
	podName := requireVelaControlEnvironment(t, control, "VELA_POD_NAME")
	if podName.ValueFrom == nil || podName.ValueFrom.FieldRef == nil ||
		podName.ValueFrom.FieldRef.FieldPath != "metadata.name" {
		t.Fatalf("vela-control Pod name environment = %#v", podName)
	}
	wantPodBound := map[string]string{
		"VELA_SCHEDULER_ID":                     "scheduler/$(VELA_POD_UID)",
		"VELA_ATTEMPT_COORDINATOR_ID":           "attempt-coordinator/$(VELA_POD_UID)",
		"VELA_STAGE_SCHEDULER_ID":               "stage-scheduler/$(VELA_POD_UID)",
		"VELA_ARTIFACT_RECONCILER_ID":           "artifact-reconciler/$(VELA_POD_UID)",
		"VELA_RETENTION_RECONCILER_ID":          "retention-reconciler/$(VELA_POD_UID)",
		"VELA_NON_CONTENT_EXPIRY_RECONCILER_ID": "non-content-expiry/$(VELA_POD_UID)",
		"VELA_ARTIFACT_REPLICATION_ID":          "artifact-replication/$(VELA_POD_UID)",
		"VELA_WEBHOOK_DISPATCHER_ID":            "webhook-dispatcher/$(VELA_POD_UID)",
		"VELA_INVOICE_EXPORTER_ID":              "invoice-exporter/$(VELA_POD_UID)",
		"VELA_FINANCE_RECONCILIATION_ADDR":      "$(VELA_POD_NAME):8445",
		"VELA_COMPLIANCE_ADDR":                  "$(VELA_POD_NAME):8446",
	}
	for name, want := range wantPodBound {
		if got := requireVelaControlEnvironment(t, control, name).Value; got != want {
			t.Fatalf("vela-control environment %s = %q, want %q", name, got, want)
		}
	}
	if got := requireVelaControlEnvironment(t, control, "VELA_REMEDIATION_ACTOR_IDENTITY").Value; got != "controller/vela-control" {
		t.Fatalf("vela-control remediation actor identity = %q", got)
	}

	materializer := pod.InitContainers[0]
	command := strings.Join(materializer.Command, "\n")
	for _, required := range []string{
		"find /materialized -type d -exec chmod 0700 {} +",
		"find /materialized -type f -exec chmod 0600 {} +",
		"chown -R 10001:10001 /materialized",
		"mkdir -p /artifact-validation/sandboxes /artifact-validation/spool",
		"find /artifact-validation -type d -exec chmod 0700 {} +",
		"chown -R 10001:10001 /artifact-validation",
	} {
		if !strings.Contains(command, required) {
			t.Fatalf("vela-control materializer omitted %q: %s", required, command)
		}
	}
	if strings.Contains(command, "chmod -R 0600") {
		t.Fatal("vela-control materializer makes credential directories non-traversable")
	}
	if strings.Contains(command, "cp -R") || strings.Contains(command, "cp -r") {
		t.Fatal("vela-control materializer recursively copies projected Secret symlinks")
	}
	assertVelaControlDeclaredFileCopies(t, command)
	for _, name := range []string{
		"node-agent-config", "control-transport-tls", "privileged-http-tls", "nats-client",
		"artifact-credentials", "keyrings", "invoice-export", "remediation-client-tls",
		"stage-worker-identity", "h3-exact-cache-keyring",
	} {
		if !hasVelaControlVolumeMount(materializer.VolumeMounts, name) {
			t.Fatalf("vela-control materializer is missing source mount %q", name)
		}
		if hasVelaControlVolumeMount(control.VolumeMounts, name) {
			t.Fatalf("vela-control application directly mounts projected source %q", name)
		}
	}
	if !hasVelaControlVolumeMount(materializer.VolumeMounts, "artifact-validation") {
		t.Fatal("vela-control init does not provision the Artifact validation workspace")
	}
	materialized := requireVelaControlVolume(t, pod.Volumes, "materialized-secrets")
	if materialized.EmptyDir == nil || materialized.EmptyDir.Medium != corev1.StorageMediumMemory ||
		materialized.EmptyDir.SizeLimit == nil || materialized.EmptyDir.SizeLimit.IsZero() {
		t.Fatalf("vela-control materialized Secret volume = %#v", materialized)
	}
	if !hasVelaControlVolumeMount(control.VolumeMounts, "materialized-secrets") {
		t.Fatal("vela-control application does not mount materialized credentials")
	}
	workspace := requireVelaControlVolume(t, pod.Volumes, "artifact-validation")
	workspaceCapacity := resource.Quantity{}
	if workspace.Ephemeral != nil && workspace.Ephemeral.VolumeClaimTemplate != nil {
		workspaceCapacity = workspace.Ephemeral.VolumeClaimTemplate.Spec.Resources.Requests[corev1.ResourceStorage]
	}
	if workspace.Ephemeral == nil || workspace.Ephemeral.VolumeClaimTemplate == nil ||
		workspace.Ephemeral.VolumeClaimTemplate.Spec.StorageClassName == nil ||
		*workspace.Ephemeral.VolumeClaimTemplate.Spec.StorageClassName !=
			"vela-control-artifact-scratch-placeholder" ||
		!reflect.DeepEqual(
			workspace.Ephemeral.VolumeClaimTemplate.Spec.AccessModes,
			[]corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		) || workspaceCapacity.Cmp(resource.MustParse("110Gi")) < 0 {
		t.Fatalf("vela-control Artifact validation workspace = %#v", workspace)
	}

	var runtimeConfig corev1.ConfigMap
	loadVelaControlManifest(t, "runtime-config.yaml", &runtimeConfig)
	if runtimeConfig.Name != "vela-control-runtime-r0-placeholder" ||
		runtimeConfig.Immutable == nil || !*runtimeConfig.Immutable {
		t.Fatalf("vela-control runtime ConfigMap = %#v", runtimeConfig)
	}
	for name, value := range runtimeConfig.Data {
		if problems := validation.IsEnvVarName(name); len(problems) != 0 {
			t.Fatalf("vela-control envFrom key %q is invalid: %v", name, problems)
		}
		if strings.HasSuffix(name, "_DATABASE_URL") || name == "VELA_CREDENTIAL_PEPPER_BASE64" {
			t.Fatalf("vela-control runtime ConfigMap contains Secret key %q", name)
		}
		if strings.Contains(name, "_FILE") && !strings.HasPrefix(value, "/etc/vela-control/materialized/") &&
			name != "VELA_ARTIFACT_VALIDATOR_HELPER_PATH" && name != "VELA_ARTIFACT_FFPROBE_PATH" {
			t.Fatalf("vela-control file environment %s bypasses materialization: %q", name, value)
		}
	}
	var nodeAgents corev1.ConfigMap
	loadVelaControlManifest(t, "node-agent-config.yaml", &nodeAgents)
	if nodeAgents.Name != "vela-control-node-agents-r0-placeholder" ||
		nodeAgents.Immutable == nil || !*nodeAgents.Immutable || len(nodeAgents.Data) != 1 ||
		nodeAgents.Data["node-agents.json"] == "" {
		t.Fatalf("vela-control Node Agent ConfigMap = %#v", nodeAgents)
	}
	nodeAgentVolume := requireVelaControlVolume(t, pod.Volumes, "node-agent-config")
	if nodeAgentVolume.ConfigMap == nil ||
		nodeAgentVolume.ConfigMap.Name != "vela-control-node-agents-r0-placeholder" ||
		!strings.Contains(
			command,
			"cp /projected/node-agents/node-agents.json /materialized/remediation/node-agents.json",
		) {
		t.Fatalf("vela-control Node Agent materialization = volume %#v command %q", nodeAgentVolume, command)
	}
}

func TestVelaControlStorageClassReleaseContractIsExplicit(t *testing.T) {
	content := readVelaControlManifest(t, "storage-contract.json")
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var contract velaControlStorageContract
	if err := decoder.Decode(&contract); err != nil {
		t.Fatalf("decode vela-control storage contract: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("vela-control storage contract has trailing content: %v", err)
	}
	if contract.APIVersion != "vela.ai/v1alpha1" || contract.Kind != "VelaControlStorageContract" ||
		contract.StorageClassName != "vela-control-artifact-scratch-placeholder" ||
		contract.VolumeBindingMode != "WaitForFirstConsumer" || contract.ReclaimPolicy != "Delete" ||
		contract.CapacityPerClaim != "110Gi" || contract.MinimumActiveClaims != 3 ||
		contract.ReclaimHeadroom != 1 || contract.MinimumClaims != 4 ||
		contract.MinimumClaims != contract.MinimumActiveClaims+contract.ReclaimHeadroom ||
		contract.AggregateCapacity != "440Gi" || !contract.DedicatedPool || !contract.IOPSLimit ||
		!contract.ThroughputLimit || !contract.LiveReceiptRequired {
		t.Fatalf("vela-control storage contract = %#v", contract)
	}
}

func TestVelaControlPublishesSixSinglePurposeServices(t *testing.T) {
	services := loadVelaControlServices(t)
	want := map[string]struct {
		port        int32
		target      string
		appProtocol string
	}{
		"vela-api":                    {port: 80, target: "http", appProtocol: "http"},
		"vela-worker-control":         {port: 8443, target: "worker-grpc", appProtocol: "grpc"},
		"vela-control":                {port: 8444, target: "fleet-grpc", appProtocol: "grpc"},
		"vela-stage-worker-control":   {port: 8447, target: "stage-wkr-grpc", appProtocol: "grpc"},
		"vela-finance-reconciliation": {port: 8445, target: "finance-https", appProtocol: "https"},
		"vela-compliance":             {port: 8446, target: "compliance-https", appProtocol: "https"},
	}
	if len(services) != len(want) {
		t.Fatalf("vela-control Services = %#v", services)
	}
	for name, expected := range want {
		service, ok := services[name]
		if !ok {
			t.Fatalf("vela-control Service %q is missing", name)
		}
		if service.Namespace != "vela-system" || service.Spec.Type != corev1.ServiceTypeClusterIP ||
			!reflect.DeepEqual(service.Spec.Selector, map[string]string{"app.kubernetes.io/name": "vela-control"}) ||
			len(service.Spec.Ports) != 1 {
			t.Fatalf("vela-control Service %q boundary = %#v", name, service)
		}
		port := service.Spec.Ports[0]
		if port.Port != expected.port || port.Protocol != corev1.ProtocolTCP ||
			port.TargetPort.StrVal != expected.target || port.AppProtocol == nil ||
			*port.AppProtocol != expected.appProtocol {
			t.Fatalf("vela-control Service %q port = %#v", name, port)
		}
	}
}

func TestVelaControlIngressIsDefaultDeniedAndIdentitySeparated(t *testing.T) {
	policies := loadVelaControlNetworkPolicies(t)
	if len(policies) != 9 {
		t.Fatalf("vela-control NetworkPolicies = %#v", policies)
	}
	defaultDeny, ok := policies["vela-control-default-deny-ingress"]
	if !ok || !reflect.DeepEqual(defaultDeny.Spec.PolicyTypes, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}) ||
		len(defaultDeny.Spec.Ingress) != 0 {
		t.Fatalf("vela-control default deny ingress = %#v", defaultDeny)
	}
	nodeAgent, ok := policies["vela-control-allow-node-agent-placeholder"]
	if !ok || nodeAgent.Annotations["vela.ai/release-placeholder"] != "replace-entire-resource" ||
		!reflect.DeepEqual(nodeAgent.Spec.PodSelector.MatchLabels, map[string]string{"app.kubernetes.io/name": "vela-control"}) ||
		!reflect.DeepEqual(nodeAgent.Spec.PolicyTypes, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}) ||
		len(nodeAgent.Spec.Ingress) != 1 || len(nodeAgent.Spec.Ingress[0].From) != 1 ||
		len(nodeAgent.Spec.Ingress[0].Ports) != 1 ||
		nodeAgent.Spec.Ingress[0].From[0].IPBlock == nil ||
		nodeAgent.Spec.Ingress[0].From[0].IPBlock.CIDR != "192.0.2.0/32" ||
		len(nodeAgent.Spec.Ingress[0].From[0].IPBlock.Except) != 0 ||
		nodeAgent.Spec.Ingress[0].From[0].NamespaceSelector != nil ||
		nodeAgent.Spec.Ingress[0].From[0].PodSelector != nil {
		t.Fatalf("vela-control Node Agent placeholder ingress = %#v", nodeAgent)
	}
	nodeAgentPort := nodeAgent.Spec.Ingress[0].Ports[0]
	if nodeAgentPort.Protocol == nil || *nodeAgentPort.Protocol != corev1.ProtocolTCP ||
		nodeAgentPort.Port == nil || nodeAgentPort.Port.IntValue() != 8444 {
		t.Fatalf("vela-control Node Agent placeholder port = %#v", nodeAgentPort)
	}
	want := map[string]struct {
		port            int
		namespaceLabels map[string]string
		podLabels       map[string]string
	}{
		"vela-control-allow-api": {
			port:            8080,
			namespaceLabels: map[string]string{"vela.ai/network-role": "api-ingress"},
			podLabels:       map[string]string{"vela.ai/client-role": "api-gateway"},
		},
		"vela-control-allow-worker": {
			port:            8443,
			namespaceLabels: map[string]string{"kubernetes.io/metadata.name": "vela-system"},
			podLabels:       map[string]string{"app.kubernetes.io/name": "vela-h3-worker"},
		},
		"vela-control-allow-fleet": {
			port:            8444,
			namespaceLabels: map[string]string{"kubernetes.io/metadata.name": "vela-system"},
			podLabels:       map[string]string{"app.kubernetes.io/name": "vela-fleet-controller"},
		},
		"vela-control-allow-stage-worker": {
			port:            8447,
			namespaceLabels: map[string]string{"kubernetes.io/metadata.name": "vela-system"},
			podLabels:       map[string]string{"app.kubernetes.io/name": "vela-stage-worker"},
		},
		"vela-control-allow-finance": {
			port:            8445,
			namespaceLabels: map[string]string{"vela.ai/network-role": "finance"},
			podLabels:       map[string]string{"vela.ai/client-role": "finance-reconciliation"},
		},
		"vela-control-allow-compliance": {
			port:            8446,
			namespaceLabels: map[string]string{"vela.ai/network-role": "compliance"},
			podLabels:       map[string]string{"vela.ai/client-role": "legal-hold"},
		},
		"vela-control-allow-observability": {
			port:            8081,
			namespaceLabels: map[string]string{"vela.ai/network-role": "observability"},
			podLabels:       map[string]string{"vela.ai/client-role": "otel-collector"},
		},
	}
	for name, expected := range want {
		policy, ok := policies[name]
		if !ok || policy.Namespace != "vela-system" ||
			!reflect.DeepEqual(policy.Spec.PodSelector.MatchLabels, map[string]string{"app.kubernetes.io/name": "vela-control"}) ||
			!reflect.DeepEqual(policy.Spec.PolicyTypes, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}) ||
			len(policy.Spec.Ingress) != 1 || len(policy.Spec.Ingress[0].Ports) != 1 ||
			len(policy.Spec.Ingress[0].From) != 1 {
			t.Fatalf("vela-control ingress policy %q = %#v", name, policy)
		}
		port := policy.Spec.Ingress[0].Ports[0]
		if port.Protocol == nil || *port.Protocol != corev1.ProtocolTCP || port.Port == nil ||
			port.Port.IntValue() != expected.port {
			t.Fatalf("vela-control ingress policy %q port = %#v", name, port)
		}
		peer := policy.Spec.Ingress[0].From[0]
		if peer.IPBlock != nil || peer.NamespaceSelector == nil ||
			!reflect.DeepEqual(peer.NamespaceSelector.MatchLabels, expected.namespaceLabels) {
			t.Fatalf("vela-control ingress policy %q namespace peer = %#v", name, peer)
		}
		if expected.podLabels == nil {
			if peer.PodSelector != nil {
				t.Fatalf("vela-control ingress policy %q Pod peer = %#v", name, peer.PodSelector)
			}
		} else if peer.PodSelector == nil || !reflect.DeepEqual(peer.PodSelector.MatchLabels, expected.podLabels) {
			t.Fatalf("vela-control ingress policy %q Pod peer = %#v", name, peer.PodSelector)
		}
	}
}

func TestVelaControlExternalSecretContractIsExactAndValueFree(t *testing.T) {
	content := readVelaControlManifest(t, "secret-contract.json")
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var contract velaControlSecretContract
	if err := decoder.Decode(&contract); err != nil {
		t.Fatalf("decode vela-control Secret contract: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("vela-control Secret contract has trailing content: %v", err)
	}
	if contract.APIVersion != "vela.ai/v1alpha1" || contract.Kind != "VelaControlSecretContract" ||
		contract.ReleaseRevision != "r0-placeholder" ||
		contract.RotationPolicy.Mode != "new-secret-name-and-rolling-update" ||
		contract.RotationPolicy.InPlaceUpdateSupported || !contract.RotationPolicy.ImmutableRequired {
		t.Fatalf("vela-control Secret contract identity = %#v", contract)
	}
	wantEnvironment := map[string][]string{
		"vela-control-database-urls-r0-placeholder": {
			"VELA_ARTIFACT_REPLICATION_DATABASE_URL", "VELA_ARTIFACT_REQUEST_DATABASE_URL",
			"VELA_ATTEMPT_COORDINATOR_DATABASE_URL",
			"VELA_AUTH_DATABASE_URL", "VELA_BACKUP_RETENTION_DATABASE_URL", "VELA_BILLING_DATABASE_URL",
			"VELA_BREAK_GLASS_AUDIT_DATABASE_URL", "VELA_BREAK_GLASS_REQUEST_DATABASE_URL",
			"VELA_CANCEL_DATABASE_URL", "VELA_COMPLIANCE_DATABASE_URL",
			"VELA_DEBUG_DUMP_AUDIT_REQUEST_DATABASE_URL", "VELA_DEBUG_DUMP_REQUEST_DATABASE_URL",
			"VELA_FINANCE_RECONCILIATION_DATABASE_URL", "VELA_FLEET_DATABASE_URL",
			"VELA_HUMAN_AUTH_DATABASE_URL", "VELA_HUMAN_MEMBERSHIP_AUTH_DATABASE_URL",
			"VELA_HUMAN_MEMBERSHIP_REQUEST_DATABASE_URL", "VELA_IDENTITY_REQUEST_DATABASE_URL",
			"VELA_INTERNAL_DATABASE_URL", "VELA_NON_CONTENT_EXPIRY_DATABASE_URL",
			"VELA_ORGANIZATION_AUDIT_REQUEST_DATABASE_URL", "VELA_ORGANIZATION_BILLING_REQUEST_DATABASE_URL",
			"VELA_PLATFORM_OPERATOR_AUTH_DATABASE_URL", "VELA_REMEDIATION_DATABASE_URL",
			"VELA_REQUEST_DATABASE_URL", "VELA_RETENTION_DATABASE_URL", "VELA_RETENTION_REQUEST_DATABASE_URL",
			"VELA_STAGE_SCHEDULER_DATABASE_URL", "VELA_STAGE_ARTIFACT_DATABASE_URL",
			"VELA_STAGE_WORKER_CONTROL_DATABASE_URL",
			"VELA_WEBHOOK_DATABASE_URL", "VELA_WEBHOOK_REQUEST_DATABASE_URL",
		},
		"vela-control-credential-pepper-r0-placeholder": {"VELA_CREDENTIAL_PEPPER_BASE64"},
	}
	wantFiles := map[string][]string{
		"vela-control-transport-tls-r0-placeholder": {
			"fleet-client-ca.crt", "fleet-tls.crt", "fleet-tls.key",
			"stage-worker-client-ca.crt", "stage-worker-tls.crt", "stage-worker-tls.key",
			"worker-client-ca.crt", "worker-tls.crt", "worker-tls.key",
		},
		"vela-control-stage-worker-identity-r0-placeholder": {"identity-key"},
		"vela-control-privileged-http-tls-r0-placeholder": {
			"compliance-client-ca.crt", "compliance-tls.crt", "compliance-tls.key",
			"finance-client-ca.crt", "finance-tls.crt", "finance-tls.key",
		},
		"vela-control-nats-client-r0-placeholder": {"ca.crt", "outbox.creds", "scheduler.creds", "tls.crt", "tls.key"},
		"vela-control-artifact-credentials-r0-placeholder": {
			"backup-retention-access-key-id", "backup-retention-secret-access-key",
			"primary-access-key-id", "primary-secret-access-key",
			"replication-backup-access-key-id", "replication-backup-secret-access-key",
			"replication-source-access-key-id", "replication-source-secret-access-key",
		},
		"vela-control-keyrings-r0-placeholder":               {"lease.json", "webhook.json"},
		"vela-control-h3-exact-cache-keyring-r0-placeholder": {"projects.json"},
		"vela-control-invoice-export-r0-placeholder":         {"bearer-token"},
		"vela-control-remediation-client-tls-r0-placeholder": {"ca.crt", "tls.crt", "tls.key"},
	}
	assertVelaControlSecretEntries(t, "environment", contract.EnvironmentSecrets, wantEnvironment)
	assertVelaControlSecretEntries(t, "file", contract.FileSecrets, wantFiles)

	var deployment appsv1.Deployment
	loadVelaControlManifest(t, "deployment.yaml", &deployment)
	pod := deployment.Spec.Template.Spec
	var environmentSecretNames []string
	for _, source := range pod.Containers[0].EnvFrom {
		if source.SecretRef != nil {
			environmentSecretNames = append(environmentSecretNames, source.SecretRef.Name)
		}
	}
	if !sameStrings(environmentSecretNames, mapKeys(wantEnvironment)) {
		t.Fatalf("vela-control environment Secret refs = %#v", environmentSecretNames)
	}
	var fileSecretNames []string
	for _, volume := range pod.Volumes {
		if volume.Secret != nil {
			fileSecretNames = append(fileSecretNames, volume.Secret.SecretName)
		}
	}
	if !sameStrings(fileSecretNames, mapKeys(wantFiles)) {
		t.Fatalf("vela-control file Secret refs = %#v", fileSecretNames)
	}
	for _, entry := range contract.FileSecrets {
		var volumeName string
		for _, volume := range pod.Volumes {
			if volume.Secret != nil && volume.Secret.SecretName == entry.Name {
				volumeName = volume.Name
				break
			}
		}
		mount := requireVelaControlVolumeMount(t, pod.InitContainers[0].VolumeMounts, volumeName)
		if mount.MountPath != entry.ProjectedPath || !mount.ReadOnly {
			t.Fatalf("vela-control Secret %q projected mount = %#v", entry.Name, mount)
		}
	}
}

func requireVelaControlSecurityContext(
	t *testing.T,
	container corev1.Container,
	wantRoot bool,
	wantAdded []corev1.Capability,
) {
	t.Helper()
	security := container.SecurityContext
	if security == nil || security.AllowPrivilegeEscalation == nil ||
		*security.AllowPrivilegeEscalation || security.ReadOnlyRootFilesystem == nil ||
		!*security.ReadOnlyRootFilesystem || security.RunAsNonRoot == nil ||
		*security.RunAsNonRoot == wantRoot || security.Capabilities == nil ||
		!reflect.DeepEqual(security.Capabilities.Drop, []corev1.Capability{"ALL"}) ||
		!reflect.DeepEqual(security.Capabilities.Add, wantAdded) {
		t.Fatalf("container %q security = %#v", container.Name, security)
	}
	if security.RunAsUser == nil || *security.RunAsUser != map[bool]int64{true: 0, false: 10001}[wantRoot] {
		t.Fatalf("container %q runAsUser = %#v", container.Name, security.RunAsUser)
	}
}

func assertVelaControlSecretEntries(
	t *testing.T,
	kind string,
	entries []velaControlSecretEntry,
	want map[string][]string,
) {
	t.Helper()
	if len(entries) != len(want) {
		t.Fatalf("vela-control %s Secret entries = %#v", kind, entries)
	}
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		keys, ok := want[entry.Name]
		_, duplicate := seen[entry.Name]
		if !ok || duplicate || !sameStrings(entry.RequiredKeys, keys) {
			t.Fatalf("vela-control %s Secret %q keys = %#v", kind, entry.Name, entry.RequiredKeys)
		}
		seen[entry.Name] = struct{}{}
	}
	if len(seen) != len(want) {
		t.Fatalf("vela-control %s Secret contract omitted entries", kind)
	}
}

func assertVelaControlDeclaredFileCopies(t *testing.T, command string) {
	t.Helper()
	content := readVelaControlManifest(t, "secret-contract.json")
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var contract velaControlSecretContract
	if err := decoder.Decode(&contract); err != nil {
		t.Fatalf("decode vela-control Secret materialization contract: %v", err)
	}
	for _, entry := range contract.FileSecrets {
		if !strings.HasPrefix(entry.ProjectedPath, "/projected/") ||
			filepath.Clean(entry.ProjectedPath) != entry.ProjectedPath ||
			len(entry.MaterializedPaths) != len(entry.RequiredKeys) {
			t.Fatalf("vela-control Secret %q materialization paths = %#v", entry.Name, entry)
		}
		for _, key := range entry.RequiredKeys {
			destination, ok := entry.MaterializedPaths[key]
			if !ok || !strings.HasPrefix(destination, "/materialized/") ||
				filepath.Clean(destination) != destination {
				t.Fatalf("vela-control Secret %q key %q has no materialized path", entry.Name, key)
			}
			want := "cp " + entry.ProjectedPath + "/" + key + " " + destination
			if !strings.Contains(command, want) {
				t.Fatalf("vela-control materializer omitted exact declared copy %q", want)
			}
		}
	}
}

func mapKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

func requireVelaControlResources(t *testing.T, container corev1.Container) {
	t.Helper()
	for _, resource := range []corev1.ResourceName{
		corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage,
	} {
		request := container.Resources.Requests[resource]
		limit := container.Resources.Limits[resource]
		if request.IsZero() || limit.IsZero() {
			t.Fatalf("container %q resource %q is unbounded: %#v", container.Name, resource, container.Resources)
		}
	}
}

func requireVelaControlPort(t *testing.T, container corev1.Container, name string, port int32) {
	t.Helper()
	for _, candidate := range container.Ports {
		if candidate.Name == name && candidate.ContainerPort == port && candidate.Protocol == corev1.ProtocolTCP {
			return
		}
	}
	t.Fatalf("container %q port %s:%d is missing: %#v", container.Name, name, port, container.Ports)
}

func requireVelaControlEnvironment(t *testing.T, container corev1.Container, name string) corev1.EnvVar {
	t.Helper()
	for _, environment := range container.Env {
		if environment.Name == name {
			return environment
		}
	}
	t.Fatalf("container %q environment %q is missing", container.Name, name)
	return corev1.EnvVar{}
}

func hasVelaControlVolumeMount(mounts []corev1.VolumeMount, name string) bool {
	for _, mount := range mounts {
		if mount.Name == name {
			return true
		}
	}
	return false
}

func requireVelaControlVolumeMount(
	t *testing.T,
	mounts []corev1.VolumeMount,
	name string,
) corev1.VolumeMount {
	t.Helper()
	for _, mount := range mounts {
		if mount.Name == name {
			return mount
		}
	}
	t.Fatalf("vela-control volume mount %q is missing", name)
	return corev1.VolumeMount{}
}

func requireVelaControlVolume(t *testing.T, volumes []corev1.Volume, name string) corev1.Volume {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name {
			return volume
		}
	}
	t.Fatalf("vela-control volume %q is missing", name)
	return corev1.Volume{}
}

func loadVelaControlManifest(t *testing.T, name string, destination any) {
	t.Helper()
	identity, ok := map[string]struct {
		apiVersion string
		kind       string
		name       string
	}{
		"deployment.yaml":        {"apps/v1", "Deployment", "vela-control"},
		"disruption-budget.yaml": {"policy/v1", "PodDisruptionBudget", "vela-control"},
		"runtime-config.yaml":    {"v1", "ConfigMap", "vela-control-runtime-r0-placeholder"},
		"node-agent-config.yaml": {"v1", "ConfigMap", "vela-control-node-agents-r0-placeholder"},
		"priority-class.yaml":    {"scheduling.k8s.io/v1", "PriorityClass", "vela-control-critical"},
	}[name]
	if !ok {
		t.Fatalf("vela-control rendered resource identity is unknown for %q", name)
	}
	for _, rendered := range renderVelaControlResources(t) {
		if rendered.GetAPIVersion() != identity.apiVersion || rendered.GetKind() != identity.kind ||
			rendered.GetName() != identity.name {
			continue
		}
		content, err := json.Marshal(rendered.Object)
		if err != nil {
			t.Fatalf("marshal rendered vela-control resource %s/%s: %v", identity.kind, identity.name, err)
		}
		if err := json.Unmarshal(content, destination); err != nil {
			t.Fatalf("parse rendered vela-control resource %s/%s: %v", identity.kind, identity.name, err)
		}
		return
	}
	t.Fatalf("rendered vela-control resource %s/%s is missing", identity.kind, identity.name)
}

func readVelaControlManifest(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join(velaControlManifestDirectory(t), name)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read vela-control manifest %q: %v", name, err)
	}
	return content
}

func loadVelaControlServices(t *testing.T) map[string]corev1.Service {
	t.Helper()
	return loadVelaControlRenderedResources(t, "v1", "Service", func(service corev1.Service) string {
		return service.Name
	})
}

func loadVelaControlNetworkPolicies(t *testing.T) map[string]networkingv1.NetworkPolicy {
	t.Helper()
	return loadVelaControlRenderedResources(
		t,
		"networking.k8s.io/v1",
		"NetworkPolicy",
		func(policy networkingv1.NetworkPolicy) string { return policy.Name },
	)
}

func loadVelaControlRenderedResources[T any](
	t *testing.T,
	apiVersion string,
	kind string,
	name func(T) string,
) map[string]T {
	t.Helper()
	result := make(map[string]T)
	for _, rendered := range renderVelaControlResources(t) {
		if rendered.GetAPIVersion() != apiVersion || rendered.GetKind() != kind {
			continue
		}
		content, err := json.Marshal(rendered.Object)
		if err != nil {
			t.Fatalf("marshal rendered vela-control %s: %v", kind, err)
		}
		var typed T
		if err := json.Unmarshal(content, &typed); err != nil {
			t.Fatalf("parse rendered vela-control %s: %v", kind, err)
		}
		resourceName := name(typed)
		if resourceName == "" {
			t.Fatalf("rendered vela-control %s has no name", kind)
		}
		if _, exists := result[resourceName]; exists {
			t.Fatalf("duplicate rendered vela-control %s %q", kind, resourceName)
		}
		result[resourceName] = typed
	}
	return result
}

func renderVelaControlResources(t *testing.T) []unstructured.Unstructured {
	t.Helper()
	command := exec.Command("kubectl", "kustomize", velaControlManifestDirectory(t))
	content, err := command.Output()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			t.Fatalf("render vela-control Kustomize base: %v: %s", err, exit.Stderr)
		}
		t.Fatalf("render vela-control Kustomize base: %v", err)
	}
	decoder := k8syamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(content), 4096)
	var resources []unstructured.Unstructured
	for {
		var resource unstructured.Unstructured
		err := decoder.Decode(&resource)
		if err == io.EOF {
			return resources
		}
		if err != nil {
			t.Fatalf("parse rendered vela-control resources: %v", err)
		}
		if resource.GetKind() == "" {
			continue
		}
		resources = append(resources, resource)
	}
}

func velaControlManifestDirectory(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate vela-control deployment contract test")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "deploy", "vela-control")
}
