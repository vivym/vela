package deploymentcontract

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/h3launchevidence"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestBarmanVendoredInstallPreservesSupplyChainRBACAndAvailability(t *testing.T) {
	var contract struct {
		Plugin struct {
			ManifestSHA256 string `json:"manifest_sha256"`
			OperatorImage  string `json:"operator_image"`
			SidecarImage   string `json:"sidecar_image"`
		} `json:"barman_cloud_plugin"`
	}
	content, err := os.ReadFile(controlStoragePath(t, "barman-cloud-plugin-contract.json"))
	if err != nil || json.Unmarshal(content, &contract) != nil {
		t.Fatal("read Barman identity contract")
	}
	content, err = os.ReadFile(controlStoragePath(t, "barman-cloud-plugin-install/manifest.yaml"))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(content)) != contract.Plugin.ManifestSHA256 {
		t.Fatal("vendored Barman manifest does not match its pinned upstream hash")
	}
	var role rbacv1.ClusterRole
	var deployment appsv1.Deployment
	var sidecar corev1.Secret
	for _, object := range renderKustomizeResources(t, controlStoragePath(t, "barman-cloud-plugin-install")) {
		content, err := json.Marshal(object.Object)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case object.GetKind() == "ClusterRole" && object.GetName() == "plugin-barman-cloud":
			err = json.Unmarshal(content, &role)
		case object.GetKind() == "Deployment" && object.GetLabels()["app"] == "barman-cloud":
			err = json.Unmarshal(content, &deployment)
		case object.GetKind() == "Secret":
			var secret corev1.Secret
			err = json.Unmarshal(content, &secret)
			if secret.Data["SIDECAR_IMAGE"] != nil {
				sidecar = secret
			}
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	secretRules := 0
	for _, rule := range role.Rules {
		for _, resource := range rule.Resources {
			if resource == "secrets" || resource == "*" {
				secretRules++
				if !reflect.DeepEqual(rule.APIGroups, []string{""}) ||
					!reflect.DeepEqual(rule.Resources, []string{"secrets"}) ||
					!sameStrings(rule.ResourceNames, []string{"vela-backup-s3"}) ||
					!sameStrings(rule.Verbs, []string{"get", "list", "watch"}) {
					t.Fatal("rendered Barman role broadens backup-secret authority")
				}
			}
		}
	}
	if secretRules != 1 {
		t.Fatal("rendered Barman backup-secret rule missing or duplicated")
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 2 ||
		deployment.Spec.Template.Spec.NodeSelector["vela.ai/control-plane-tier"] != "cpu" ||
		deployment.Spec.Template.Spec.Affinity == nil ||
		deployment.Spec.Template.Spec.Affinity.PodAntiAffinity == nil ||
		len(deployment.Spec.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatal("Barman must have two distinct CPU-host replicas")
	}
	rolling := deployment.Spec.Strategy.RollingUpdate
	if rolling == nil || rolling.MaxSurge == nil || rolling.MaxUnavailable == nil ||
		rolling.MaxSurge.IntValue() != 0 || rolling.MaxUnavailable.IntValue() != 1 {
		t.Fatal("Barman rollout must fit the two eligible hosts")
	}
	if len(deployment.Spec.Template.Spec.Containers) != 1 {
		t.Fatal("unexpected Barman operator containers")
	}
	for name, pair := range map[string][2]string{
		"operator": {deployment.Spec.Template.Spec.Containers[0].Image, contract.Plugin.OperatorImage},
		"sidecar":  {string(sidecar.Data["SIDECAR_IMAGE"]), contract.Plugin.SidecarImage},
	} {
		actual, want := strings.Split(pair[0], "@"), strings.Split(pair[1], "@")
		if len(actual) != 2 || len(want) != 2 || actual[1] != want[1] ||
			strings.Split(actual[0], ":")[0] != strings.Split(want[0], ":")[0] {
			t.Fatalf("rendered Barman %s image does not bind the contract digest", name)
		}
	}
}

func TestMinIOSixMemberQuorumAndListenerContract(t *testing.T) {
	for _, profile := range []struct {
		name, directory, capacity string
	}{
		{"final", "object-store/ha-six", "16Gi"},
		{"staging", "environments/marslab/minio-ha-staging", "8Gi"},
	} {
		t.Run(profile.name, func(t *testing.T) {
			testMinIOSixMemberContract(t, profile.directory, profile.capacity)
		})
	}
}

func testMinIOSixMemberContract(t *testing.T, relativeDirectory, capacity string) {
	t.Helper()
	directory := filepath.Join(velaControlManifestDirectory(t), "..", relativeDirectory)
	var stateful appsv1.StatefulSet
	for _, object := range renderKustomizeResources(t, directory) {
		if object.GetKind() == "StatefulSet" && object.GetName() == "minio-ha" {
			content, _ := json.Marshal(object.Object)
			if err := json.Unmarshal(content, &stateful); err != nil {
				t.Fatal(err)
			}
		}
	}
	if stateful.Spec.Replicas == nil || *stateful.Spec.Replicas != 6 {
		t.Fatal("three-host MinIO topology requires six members")
	}
	if len(stateful.Spec.VolumeClaimTemplates) != 1 {
		t.Fatal("expected one retained data claim per member")
	}
	actualCapacity := stateful.Spec.VolumeClaimTemplates[0].Spec.Resources.Requests[corev1.ResourceStorage]
	if actualCapacity.Cmp(resource.MustParse(capacity)) != 0 {
		t.Fatal("MinIO staging and final claim capacity must remain explicit")
	}
	retention := stateful.Spec.PersistentVolumeClaimRetentionPolicy
	if retention == nil || retention.WhenDeleted != appsv1.RetainPersistentVolumeClaimRetentionPolicyType ||
		retention.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
		t.Fatal("MinIO data claims must survive StatefulSet reconciliation")
	}
	container := stateful.Spec.Template.Spec.Containers[0]
	for _, address := range []string{container.Args[2], container.Args[4]} {
		if _, _, err := net.SplitHostPort(address); err != nil {
			t.Fatalf("MinIO listener lost its port separator: %v", err)
		}
	}
	env := map[string]string{}
	for _, variable := range container.Env {
		env[variable.Name] = variable.Value
	}
	for _, class := range []string{"MINIO_STORAGE_CLASS_STANDARD", "MINIO_STORAGE_CLASS_RRS"} {
		parity, err := strconv.Atoi(strings.TrimPrefix(env[class], "EC:"))
		if err != nil || parity != 3 {
			t.Fatalf("%s must use explicit EC:3", class)
		}
		members := int(*stateful.Spec.Replicas)
		writeQuorum := members - parity
		if writeQuorum == parity {
			writeQuorum++
		}
		if members-2 < writeQuorum {
			t.Fatal("losing two members on one host loses write quorum")
		}
	}
	if container.ReadinessProbe == nil || container.ReadinessProbe.HTTPGet.Path != "/minio/health/cluster" {
		t.Fatal("member readiness must check cluster write availability")
	}
	if stateful.Spec.Template.Spec.NodeSelector["vela.ai/node-role"] != "control-storage" ||
		len(stateful.Spec.Template.Spec.TopologySpreadConstraints) != 1 ||
		stateful.Spec.Template.Spec.TopologySpreadConstraints[0].MaxSkew != 1 {
		t.Fatal("MinIO members must remain spread on the accepted storage hosts")
	}
}

func TestMarslabOverlayResolvesConfigurationAndPreservesValidationBoundary(t *testing.T) {
	directory := filepath.Join(velaControlManifestDirectory(t), "..", "environments", "marslab", "vela-control")
	var deployment appsv1.Deployment
	configMaps := map[string]corev1.ConfigMap{}
	for _, object := range renderKustomizeResources(t, directory) {
		if object.GetKind() == "Secret" {
			t.Fatal("environment overlay embeds Secret values")
		}
		content, err := json.Marshal(object.Object)
		if err != nil {
			t.Fatal(err)
		}
		if object.GetKind() == "Deployment" {
			err = json.Unmarshal(content, &deployment)
		}
		if object.GetKind() == "ConfigMap" {
			var configMap corev1.ConfigMap
			err = json.Unmarshal(content, &configMap)
			configMaps[configMap.Name] = configMap
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if deployment.Annotations["vela.ai/deployment-profile"] != "infrastructure-validation" {
		t.Fatal("current capacity profile must not masquerade as a production release")
	}
	for _, configMap := range configMaps {
		revision, err := h3launchevidence.ConfigMapContentRevision(configMap)
		if err != nil || configMap.Immutable == nil || !*configMap.Immutable ||
			configMap.Annotations["vela.ai/release-revision"] != revision {
			t.Fatalf("rendered ConfigMap %s does not match its immutable content revision", configMap.Name)
		}
	}
	var contract velaControlSecretContract
	content, err := os.ReadFile(filepath.Join(directory, "secret-contract.json"))
	if err != nil || json.Unmarshal(content, &contract) != nil {
		t.Fatal("read environment Secret references")
	}
	secrets := map[string]bool{}
	for _, entry := range append(contract.EnvironmentSecrets, contract.FileSecrets...) {
		if strings.Contains(entry.Name, "placeholder") {
			t.Fatal("site Secret reference is unresolved")
		}
		secrets[entry.Name] = true
	}
	pod := deployment.Spec.Template.Spec
	if pod.SecurityContext.FSGroup != nil {
		t.Fatal("fsGroup breaks the Artifact sandbox on remount")
	}
	for _, container := range append(pod.InitContainers, pod.Containers...) {
		if !pinnedVelaControlImage.MatchString(container.Image) || strings.Contains(container.Image, strings.Repeat("0", 64)) {
			t.Fatal("environment image is not pinned to a real digest")
		}
		for _, value := range container.Env {
			if value.ValueFrom != nil && value.ValueFrom.SecretKeyRef != nil && !secrets[value.ValueFrom.SecretKeyRef.Name] {
				t.Fatal("undeclared keyed environment Secret")
			}
		}
		for _, ref := range container.EnvFrom {
			if ref.SecretRef != nil && !secrets[ref.SecretRef.Name] {
				t.Fatal("undeclared environment Secret")
			}
			if ref.ConfigMapRef != nil {
				cm, found := configMaps[ref.ConfigMapRef.Name]
				if !found || cm.Immutable == nil || !*cm.Immutable {
					t.Fatal("environment ConfigMap does not resolve to an immutable object")
				}
				for key, value := range cm.Data {
					if strings.Contains(value, "$(VELA_POD_") {
						t.Fatalf("unexpanded envFrom identity: %s", key)
					}
				}
				for _, key := range []string{"VELA_HTTP_ADDRESS", "VELA_MANAGEMENT_ADDRESS", "VELA_FLEET_GRPC_ADDRESS", "VELA_STAGE_WORKER_CONTROL_ADDRESS"} {
					if _, _, err := net.SplitHostPort(cm.Data[key]); err != nil {
						t.Fatalf("invalid listener address in %s: %v", key, err)
					}
				}
			}
		}
	}
	positions := map[string]int{}
	for i, variable := range pod.Containers[0].Env {
		positions[variable.Name] = i
	}
	if uid, ok := positions["VELA_POD_UID"]; !ok || uid >= positions["VELA_STAGE_FINALIZER_ID"] {
		t.Fatal("Pod UID must be defined before dependent Finalizer identity expansion")
	}
	for _, volume := range pod.Volumes {
		if volume.Secret != nil && !secrets[volume.Secret.SecretName] {
			t.Fatal("undeclared projected Secret")
		}
		if volume.ConfigMap != nil {
			if _, found := configMaps[volume.ConfigMap.Name]; !found {
				t.Fatal("projected ConfigMap does not resolve")
			}
		}
	}
	workspace := requireVelaControlVolume(t, pod.Volumes, "artifact-validation")
	spec := workspace.Ephemeral.VolumeClaimTemplate.Spec
	capacity := spec.Resources.Requests[corev1.ResourceStorage]
	if capacity.Cmp(resource.MustParse("20Gi")) != 0 ||
		*spec.StorageClassName != "longhorn-wffc" {
		t.Fatal("validation capacity changed without updating its status")
	}
	var status struct {
		ProductionStorageContractSatisfied bool   `json:"productionStorageContractSatisfied"`
		CapacityPerClaim                   string `json:"capacityPerClaim"`
	}
	content, err = os.ReadFile(filepath.Join(directory, "capacity-status.json"))
	if err != nil || json.Unmarshal(content, &status) != nil || status.ProductionStorageContractSatisfied || status.CapacityPerClaim != "20Gi" {
		t.Fatal("environment capacity status must retain the unfinished production contract")
	}
}

func TestMarslabFFprobeVersionMatchesBuiltRuntime(t *testing.T) {
	dockerfile, err := os.ReadFile(filepath.Join(deploymentRepositoryRoot(t), "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	version := regexp.MustCompile(`https://ffmpeg\.org/releases/ffmpeg-([0-9]+\.[0-9]+\.[0-9]+)\.tar\.xz`).FindSubmatch(dockerfile)
	if len(version) != 2 {
		t.Fatal("cannot resolve the pinned ffprobe build version")
	}
	directory := filepath.Join(velaControlManifestDirectory(t), "..", "environments", "marslab", "vela-control")
	found := false
	for _, object := range renderKustomizeResources(t, directory) {
		if object.GetKind() != "ConfigMap" {
			continue
		}
		data, ok := object.Object["data"].(map[string]any)
		if !ok {
			continue
		}
		configured, ok := data["VELA_ARTIFACT_FFPROBE_VERSION"]
		if !ok {
			continue
		}
		found = true
		if configured != string(version[1]) {
			t.Fatalf("configured ffprobe version %q must match program_version.version %q, without a product prefix", configured, version[1])
		}
	}
	if !found {
		t.Fatal("rendered Marslab runtime has no ffprobe version")
	}
}
