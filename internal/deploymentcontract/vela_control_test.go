package deploymentcontract

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
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
	k8syamlutil "k8s.io/apimachinery/pkg/util/yaml"
	k8syaml "sigs.k8s.io/yaml"
)

var pinnedVelaControlImage = regexp.MustCompile(`^[^[:space:]]+@sha256:[0-9a-f]{64}$`)

type velaControlSecretContract struct {
	APIVersion         string `json:"apiVersion"`
	Kind               string `json:"kind"`
	EnvironmentSecrets []struct {
		Name         string   `json:"name"`
		RequiredKeys []string `json:"requiredKeys"`
	} `json:"environmentSecrets"`
	FileSecrets []struct {
		Name         string   `json:"name"`
		RequiredKeys []string `json:"requiredKeys"`
	} `json:"fileSecrets"`
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
	requireVelaControlPort(t, control, "worker-grpc", 8443)
	requireVelaControlPort(t, control, "fleet-grpc", 8444)
	requireVelaControlPort(t, control, "finance-https", 8445)
	requireVelaControlPort(t, control, "compliance-https", 8446)
	for name, probe := range map[string]*corev1.Probe{
		"startup": control.StartupProbe, "readiness": control.ReadinessProbe,
		"liveness": control.LivenessProbe,
	} {
		if probe == nil || probe.HTTPGet == nil || probe.HTTPGet.Port.StrVal != "http" {
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
}

func TestVelaControlMaterializesSecretsAndUsesUniqueClaimantIdentities(t *testing.T) {
	var deployment appsv1.Deployment
	loadVelaControlManifest(t, "deployment.yaml", &deployment)
	pod := deployment.Spec.Template.Spec
	control := pod.Containers[0]
	if len(control.EnvFrom) != 3 || control.EnvFrom[0].ConfigMapRef == nil ||
		control.EnvFrom[0].ConfigMapRef.Name != "vela-control-runtime-placeholder" ||
		control.EnvFrom[1].SecretRef == nil ||
		control.EnvFrom[1].SecretRef.Name != "vela-control-database-urls" ||
		control.EnvFrom[2].SecretRef == nil ||
		control.EnvFrom[2].SecretRef.Name != "vela-control-credential-pepper" {
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
		"VELA_ARTIFACT_RECONCILER_ID":           "artifact-reconciler/$(VELA_POD_UID)",
		"VELA_RETENTION_RECONCILER_ID":          "retention-reconciler/$(VELA_POD_UID)",
		"VELA_NON_CONTENT_EXPIRY_RECONCILER_ID": "non-content-expiry/$(VELA_POD_UID)",
		"VELA_ARTIFACT_REPLICATION_ID":          "artifact-replication/$(VELA_POD_UID)",
		"VELA_WEBHOOK_DISPATCHER_ID":            "webhook-dispatcher/$(VELA_POD_UID)",
		"VELA_INVOICE_EXPORTER_ID":              "invoice-exporter/$(VELA_POD_UID)",
		"VELA_REMEDIATION_ACTOR_IDENTITY":       "controller/$(VELA_POD_UID)",
		"VELA_FINANCE_RECONCILIATION_ADDR":      "$(VELA_POD_NAME):8445",
		"VELA_COMPLIANCE_ADDR":                  "$(VELA_POD_NAME):8446",
	}
	for name, want := range wantPodBound {
		if got := requireVelaControlEnvironment(t, control, name).Value; got != want {
			t.Fatalf("vela-control environment %s = %q, want %q", name, got, want)
		}
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
	for _, name := range []string{
		"runtime-config", "control-transport-tls", "privileged-http-tls", "nats-client",
		"artifact-credentials", "keyrings", "invoice-export", "remediation-client-tls",
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

	var runtimeConfig corev1.ConfigMap
	loadVelaControlManifest(t, "runtime-config.yaml", &runtimeConfig)
	if runtimeConfig.Immutable == nil || !*runtimeConfig.Immutable {
		t.Fatalf("vela-control runtime ConfigMap = %#v", runtimeConfig)
	}
	for name, value := range runtimeConfig.Data {
		if strings.HasSuffix(name, "_DATABASE_URL") || name == "VELA_CREDENTIAL_PEPPER_BASE64" {
			t.Fatalf("vela-control runtime ConfigMap contains Secret key %q", name)
		}
		if strings.Contains(name, "_FILE") && !strings.HasPrefix(value, "/etc/vela-control/materialized/") &&
			name != "VELA_ARTIFACT_VALIDATOR_HELPER_PATH" && name != "VELA_ARTIFACT_FFPROBE_PATH" {
			t.Fatalf("vela-control file environment %s bypasses materialization: %q", name, value)
		}
	}
}

func TestVelaControlPublishesFiveSinglePurposeServices(t *testing.T) {
	services := loadVelaControlServices(t)
	want := map[string]struct {
		port        int32
		target      string
		appProtocol string
	}{
		"vela-api":                    {port: 80, target: "http", appProtocol: "http"},
		"vela-worker-control":         {port: 8443, target: "worker-grpc", appProtocol: "grpc"},
		"vela-control":                {port: 8444, target: "fleet-grpc", appProtocol: "grpc"},
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
	if len(policies) != 6 {
		t.Fatalf("vela-control NetworkPolicies = %#v", policies)
	}
	defaultDeny, ok := policies["vela-control-default-deny-ingress"]
	if !ok || !reflect.DeepEqual(defaultDeny.Spec.PolicyTypes, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}) ||
		len(defaultDeny.Spec.Ingress) != 0 {
		t.Fatalf("vela-control default deny ingress = %#v", defaultDeny)
	}
	want := map[string]struct {
		port            int
		namespaceLabels map[string]string
		podLabels       map[string]string
	}{
		"vela-control-allow-api": {
			port: 8080, namespaceLabels: map[string]string{"vela.ai/network-role": "api-ingress"},
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
	if contract.APIVersion != "vela.ai/v1alpha1" || contract.Kind != "VelaControlSecretContract" {
		t.Fatalf("vela-control Secret contract identity = %#v", contract)
	}
	wantEnvironment := map[string][]string{
		"vela-control-database-urls": {
			"VELA_ARTIFACT_REPLICATION_DATABASE_URL", "VELA_ARTIFACT_REQUEST_DATABASE_URL",
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
			"VELA_SCHEDULER_DATABASE_URL", "VELA_SCHEDULER_INBOX_DATABASE_URL",
			"VELA_WEBHOOK_DATABASE_URL", "VELA_WEBHOOK_REQUEST_DATABASE_URL",
		},
		"vela-control-credential-pepper": {"VELA_CREDENTIAL_PEPPER_BASE64"},
	}
	wantFiles := map[string][]string{
		"vela-control-transport-tls": {
			"fleet-client-ca.crt", "fleet-tls.crt", "fleet-tls.key",
			"worker-client-ca.crt", "worker-tls.crt", "worker-tls.key",
		},
		"vela-control-privileged-http-tls": {
			"compliance-client-ca.crt", "compliance-tls.crt", "compliance-tls.key",
			"finance-client-ca.crt", "finance-tls.crt", "finance-tls.key",
		},
		"vela-control-nats-client": {"ca.crt", "outbox.creds", "scheduler.creds", "tls.crt", "tls.key"},
		"vela-control-artifact-credentials": {
			"backup-retention-access-key-id", "backup-retention-secret-access-key",
			"primary-access-key-id", "primary-secret-access-key",
			"replication-backup-access-key-id", "replication-backup-secret-access-key",
			"replication-source-access-key-id", "replication-source-secret-access-key",
		},
		"vela-control-keyrings":               {"lease.json", "webhook.json"},
		"vela-control-invoice-export":         {"bearer-token"},
		"vela-control-remediation-client-tls": {"ca.crt", "tls.crt", "tls.key"},
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
	entries []struct {
		Name         string   `json:"name"`
		RequiredKeys []string `json:"requiredKeys"`
	},
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
	content := readVelaControlManifest(t, name)
	if err := k8syaml.Unmarshal(content, destination); err != nil {
		t.Fatalf("parse vela-control manifest %q: %v", name, err)
	}
}

func readVelaControlManifest(t *testing.T, name string) []byte {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate vela-control deployment contract test")
	}
	path := filepath.Join(filepath.Dir(file), "..", "..", "deploy", "vela-control", name)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read vela-control manifest %q: %v", name, err)
	}
	return content
}

func loadVelaControlServices(t *testing.T) map[string]corev1.Service {
	t.Helper()
	decoder := k8syamlutil.NewYAMLOrJSONDecoder(
		bytes.NewReader(readVelaControlManifest(t, "services.yaml")),
		4096,
	)
	services := make(map[string]corev1.Service)
	for {
		var service corev1.Service
		err := decoder.Decode(&service)
		if err == io.EOF {
			return services
		}
		if err != nil {
			t.Fatalf("parse vela-control Services: %v", err)
		}
		if service.Kind == "" {
			continue
		}
		if service.APIVersion != "v1" || service.Kind != "Service" || service.Name == "" {
			t.Fatalf("invalid vela-control Service document = %#v", service)
		}
		if _, exists := services[service.Name]; exists {
			t.Fatalf("duplicate vela-control Service %q", service.Name)
		}
		services[service.Name] = service
	}
}

func loadVelaControlNetworkPolicies(t *testing.T) map[string]networkingv1.NetworkPolicy {
	t.Helper()
	decoder := k8syamlutil.NewYAMLOrJSONDecoder(
		bytes.NewReader(readVelaControlManifest(t, "network-policies.yaml")),
		4096,
	)
	policies := make(map[string]networkingv1.NetworkPolicy)
	for {
		var policy networkingv1.NetworkPolicy
		err := decoder.Decode(&policy)
		if err == io.EOF {
			return policies
		}
		if err != nil {
			t.Fatalf("parse vela-control NetworkPolicies: %v", err)
		}
		if policy.Kind == "" {
			continue
		}
		if policy.APIVersion != "networking.k8s.io/v1" || policy.Kind != "NetworkPolicy" || policy.Name == "" {
			t.Fatalf("invalid vela-control NetworkPolicy document = %#v", policy)
		}
		if _, exists := policies[policy.Name]; exists {
			t.Fatalf("duplicate vela-control NetworkPolicy %q", policy.Name)
		}
		policies[policy.Name] = policy
	}
}
