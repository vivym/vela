package deploymentcontract

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type cnpgClusterContract struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name   string            `yaml:"name"`
		Labels map[string]string `yaml:"labels"`
	} `yaml:"metadata"`
	Spec struct {
		Instances             int                          `yaml:"instances"`
		ImageName             string                       `yaml:"imageName"`
		PrimaryUpdateStrategy string                       `yaml:"primaryUpdateStrategy"`
		Storage               cnpgStorageContract          `yaml:"storage"`
		WALStorage            cnpgStorageContract          `yaml:"walStorage"`
		Resources             map[string]map[string]string `yaml:"resources"`
		Affinity              struct {
			EnablePodAntiAffinity bool   `yaml:"enablePodAntiAffinity"`
			TopologyKey           string `yaml:"topologyKey"`
		} `yaml:"affinity"`
		PostgreSQL struct {
			Parameters  map[string]string `yaml:"parameters"`
			Synchronous struct {
				DataDurability string `yaml:"dataDurability"`
				Method         string `yaml:"method"`
				Number         int    `yaml:"number"`
			} `yaml:"synchronous"`
		} `yaml:"postgresql"`
		Plugins []cnpgPluginContract `yaml:"plugins"`
	} `yaml:"spec"`
}

type cnpgStorageContract struct {
	Size         string `yaml:"size"`
	StorageClass string `yaml:"storageClass"`
}

type cnpgSecretKeySelector struct {
	Name string `yaml:"name"`
	Key  string `yaml:"key"`
}

type cnpgPluginContract struct {
	Name          string            `yaml:"name"`
	IsWALArchiver bool              `yaml:"isWALArchiver"`
	Parameters    map[string]string `yaml:"parameters"`
}

type barmanObjectStoreContract struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name   string            `yaml:"name"`
		Labels map[string]string `yaml:"labels"`
	} `yaml:"metadata"`
	Spec struct {
		RetentionPolicy string `yaml:"retentionPolicy"`
		Configuration   struct {
			DestinationPath string `yaml:"destinationPath"`
			EndpointURL     string `yaml:"endpointURL"`
			S3Credentials   struct {
				AccessKeyID     cnpgSecretKeySelector `yaml:"accessKeyId"`
				SecretAccessKey cnpgSecretKeySelector `yaml:"secretAccessKey"`
			} `yaml:"s3Credentials"`
			WAL struct {
				Compression string `yaml:"compression"`
				MaxParallel int    `yaml:"maxParallel"`
			} `yaml:"wal"`
		} `yaml:"configuration"`
		InstanceSidecarConfiguration struct {
			RetentionPolicyIntervalSeconds int                          `yaml:"retentionPolicyIntervalSeconds"`
			Resources                      map[string]map[string]string `yaml:"resources"`
		} `yaml:"instanceSidecarConfiguration"`
	} `yaml:"spec"`
}

type cnpgScheduledBackupContract struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name   string            `yaml:"name"`
		Labels map[string]string `yaml:"labels"`
	} `yaml:"metadata"`
	Spec struct {
		Schedule             string `yaml:"schedule"`
		Immediate            bool   `yaml:"immediate"`
		BackupOwnerReference string `yaml:"backupOwnerReference"`
		Method               string `yaml:"method"`
		Cluster              struct {
			Name string `yaml:"name"`
		} `yaml:"cluster"`
		PluginConfiguration struct {
			Name string `yaml:"name"`
		} `yaml:"pluginConfiguration"`
	} `yaml:"spec"`
}

type barmanInstallKustomizationContract struct {
	APIVersion string   `yaml:"apiVersion"`
	Kind       string   `yaml:"kind"`
	Resources  []string `yaml:"resources"`
	Patches    []struct {
		Target struct {
			Group   string `yaml:"group"`
			Version string `yaml:"version"`
			Kind    string `yaml:"kind"`
			Name    string `yaml:"name"`
		} `yaml:"target"`
		Patch string `yaml:"patch"`
	} `yaml:"patches"`
}

type kustomizePatchOperation struct {
	Op    string `yaml:"op"`
	Path  string `yaml:"path"`
	Value any    `yaml:"value"`
}

func TestControlStoragePostgreSQLReplicationBackupAndRecoveryContract(
	t *testing.T,
) {
	var cluster cnpgClusterContract
	loadControlStorageYAML(t, "postgres-cluster.yaml", &cluster)
	if cluster.APIVersion != "postgresql.cnpg.io/v1" || cluster.Kind != "Cluster" ||
		cluster.Metadata.Name != "vela-postgres" || cluster.Spec.Instances != 3 ||
		cluster.Spec.ImageName != "ghcr.io/cloudnative-pg/postgresql:16.4@sha256:"+
			"99be063781d171d3971089b49c992706bdab9ccbd2b57cdf126c7542773aedfe" ||
		cluster.Spec.PrimaryUpdateStrategy != "unsupervised" {
		t.Fatalf("CloudNativePG identity/replication contract = %#v", cluster)
	}
	if !cluster.Spec.Affinity.EnablePodAntiAffinity ||
		cluster.Spec.Affinity.TopologyKey != "kubernetes.io/hostname" ||
		cluster.Spec.PostgreSQL.Parameters["vela.require_synchronous_quorum"] != "on" ||
		cluster.Spec.PostgreSQL.Synchronous.DataDurability != "required" ||
		cluster.Spec.PostgreSQL.Synchronous.Method != "any" ||
		cluster.Spec.PostgreSQL.Synchronous.Number != 1 {
		t.Fatalf("CloudNativePG synchronous placement contract = %#v", cluster.Spec)
	}
	if cluster.Spec.Storage.Size == "" || cluster.Spec.Storage.StorageClass != "local-path" ||
		cluster.Spec.WALStorage.Size == "" || cluster.Spec.WALStorage.StorageClass != "local-path" {
		t.Fatalf(
			"CloudNativePG data/WAL storage contract = %#v / %#v",
			cluster.Spec.Storage,
			cluster.Spec.WALStorage,
		)
	}
	if len(cluster.Spec.Plugins) != 1 || cluster.Spec.Plugins[0].Name !=
		"barman-cloud.cloudnative-pg.io" || !cluster.Spec.Plugins[0].IsWALArchiver ||
		cluster.Spec.Plugins[0].Parameters["barmanObjectName"] != "vela-postgres-backup" {
		t.Fatalf("CloudNativePG plugin contract = %#v", cluster.Spec.Plugins)
	}

	var objectStore barmanObjectStoreContract
	loadControlStorageYAML(t, "postgres-object-store.yaml", &objectStore)
	if objectStore.APIVersion != "barmancloud.cnpg.io/v1" ||
		objectStore.Kind != "ObjectStore" || objectStore.Metadata.Name != "vela-postgres-backup" ||
		objectStore.Spec.RetentionPolicy != "30d" ||
		objectStore.Spec.Configuration.DestinationPath == "" ||
		objectStore.Spec.Configuration.EndpointURL == "" ||
		objectStore.Spec.Configuration.S3Credentials.AccessKeyID != (cnpgSecretKeySelector{
			Name: "vela-backup-s3", Key: "ACCESS_KEY_ID",
		}) || objectStore.Spec.Configuration.S3Credentials.SecretAccessKey != (cnpgSecretKeySelector{
		Name: "vela-backup-s3", Key: "SECRET_ACCESS_KEY",
	}) || objectStore.Spec.Configuration.WAL.Compression != "gzip" ||
		objectStore.Spec.Configuration.WAL.MaxParallel != 2 ||
		objectStore.Spec.InstanceSidecarConfiguration.RetentionPolicyIntervalSeconds != 1800 {
		t.Fatalf("Barman ObjectStore contract = %#v", objectStore)
	}
	resources := objectStore.Spec.InstanceSidecarConfiguration.Resources
	if len(resources) != 2 || len(resources["requests"]) != 2 || len(resources["limits"]) != 2 ||
		resources["requests"]["cpu"] != "100m" || resources["requests"]["memory"] != "128Mi" ||
		resources["limits"]["cpu"] != "1" || resources["limits"]["memory"] != "512Mi" {
		t.Fatalf("Barman ObjectStore sidecar resources = %#v", resources)
	}

	var scheduled cnpgScheduledBackupContract
	loadControlStorageYAML(t, "postgres-scheduled-backup.yaml", &scheduled)
	if scheduled.APIVersion != "postgresql.cnpg.io/v1" || scheduled.Kind != "ScheduledBackup" ||
		scheduled.Metadata.Name != "vela-postgres-daily" || scheduled.Spec.Schedule != "0 0 2 * * *" ||
		!scheduled.Spec.Immediate || scheduled.Spec.BackupOwnerReference != "self" ||
		scheduled.Spec.Method != "plugin" || scheduled.Spec.Cluster.Name != "vela-postgres" ||
		scheduled.Spec.PluginConfiguration.Name != "barman-cloud.cloudnative-pg.io" {
		t.Fatalf("CloudNativePG ScheduledBackup contract = %#v", scheduled)
	}

	assertBarmanPluginIdentityContract(t, cluster.Spec.ImageName)

	assertPostgreSQLDisruptionBudget(t)

	var recovery struct {
		APIVersion string `yaml:"apiVersion"`
		Kind       string `yaml:"kind"`
		Metadata   struct {
			Name   string            `yaml:"name"`
			Labels map[string]string `yaml:"labels"`
		} `yaml:"metadata"`
		Data map[string]string `yaml:"data"`
	}
	loadControlStorageYAML(t, "recovery-contract.yaml", &recovery)
	if recovery.APIVersion != "v1" || recovery.Kind != "ConfigMap" ||
		recovery.Metadata.Name != "vela-recovery-contract" {
		t.Fatalf("recovery contract identity = %#v", recovery)
	}
	wantRecovery := map[string]string{
		"single_node_metadata_rpo":                "0",
		"single_node_control_plane_rto":           "5m",
		"automatic_single_node_failover_required": "true",
		"no_quorum_admission_fail_closed":         "true",
		"no_quorum_assignment_fail_closed":        "true",
		"restore_order": "postgresql,artifact-store,retention-replay,jetstream," +
			"outbox-replay,reconcilers",
		"artifact_backup_scope":                          "committed-artifacts-only",
		"artifact_backup_all_versions_purge_required":    "true",
		"retention_replay_after_restore_required":        "true",
		"restore_not_before_deletion_authority_required": "true",
		"artifact_replication_race_receipt_required":     "true",
		"external_deletion_journal_available":            "false",
	}
	for key, want := range wantRecovery {
		if got := recovery.Data[key]; got != want {
			t.Errorf("recovery contract %s = %q, want %q", key, got, want)
		}
	}
}

func assertBarmanPluginIdentityContract(t *testing.T, postgresImage string) {
	t.Helper()
	contents, err := os.ReadFile(controlStoragePath(t, "barman-cloud-plugin-contract.json"))
	if err != nil {
		t.Fatalf("read Barman plugin identity contract: %v", err)
	}
	var contract struct {
		SchemaVersion int `json:"schema_version"`
		CloudNativePG struct {
			Version        string `json:"version"`
			ManifestURL    string `json:"manifest_url"`
			ManifestSHA256 string `json:"manifest_sha256"`
			OperatorImage  string `json:"operator_image"`
			PostgresImage  string `json:"postgres_image"`
		} `json:"cloudnative_pg"`
		CertManager struct {
			Version        string            `json:"version"`
			ManifestURL    string            `json:"manifest_url"`
			ManifestSHA256 string            `json:"manifest_sha256"`
			Images         map[string]string `json:"images"`
		} `json:"cert_manager"`
		BarmanCloudPlugin struct {
			Version              string `json:"version"`
			Name                 string `json:"name"`
			ManifestURL          string `json:"manifest_url"`
			ManifestSHA256       string `json:"manifest_sha256"`
			InstallKustomization string `json:"install_kustomization"`
			OperatorImage        string `json:"operator_image"`
			SidecarImage         string `json:"sidecar_image"`
		} `json:"barman_cloud_plugin"`
		LocalConformance struct {
			MinIOImage string `json:"minio_image"`
		} `json:"local_conformance"`
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&contract); err != nil {
		t.Fatalf("decode Barman plugin identity contract: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("Barman plugin identity contract has trailing JSON: %v", err)
	}
	if contract.SchemaVersion != 1 || contract.CloudNativePG.Version != "v1.30.0" ||
		contract.CertManager.Version != "v1.21.1" ||
		contract.BarmanCloudPlugin.Version != "v0.14.0" ||
		contract.BarmanCloudPlugin.Name != "barman-cloud.cloudnative-pg.io" ||
		contract.BarmanCloudPlugin.InstallKustomization !=
			"barman-cloud-plugin-install/kustomization.yaml" {
		t.Fatalf("Barman plugin versions = %#v", contract)
	}
	exactValues := map[string][2]string{
		"CloudNativePG manifest URL": {
			contract.CloudNativePG.ManifestURL,
			"https://github.com/cloudnative-pg/cloudnative-pg/releases/download/v1.30.0/cnpg-1.30.0.yaml",
		},
		"CloudNativePG manifest SHA-256": {
			contract.CloudNativePG.ManifestSHA256,
			"f8bede43fe4ee0d478c2355b204a36876b2ae4faac60f2a9452280b293da3b88",
		},
		"CloudNativePG operator image": {
			contract.CloudNativePG.OperatorImage,
			"ghcr.io/cloudnative-pg/cloudnative-pg:1.30.0@sha256:a2701eb97cdd2a34b1fdb2cb51987f544b706e40bec72ae7146cd8580efefebb",
		},
		"PostgreSQL image": {
			contract.CloudNativePG.PostgresImage,
			"ghcr.io/cloudnative-pg/postgresql:16.4@sha256:99be063781d171d3971089b49c992706bdab9ccbd2b57cdf126c7542773aedfe",
		},
		"cert-manager manifest URL": {
			contract.CertManager.ManifestURL,
			"https://github.com/cert-manager/cert-manager/releases/download/v1.21.1/cert-manager.yaml",
		},
		"cert-manager manifest SHA-256": {
			contract.CertManager.ManifestSHA256,
			"5f6a499b8c1857d57f560f536e0dcc830914b45c420899fe7ad0692c8624e408",
		},
		"cert-manager cainjector image": {
			contract.CertManager.Images["cainjector"],
			"quay.io/jetstack/cert-manager-cainjector:v1.21.1@sha256:ccf6b919ec0500745a47a910118f834f9636d0aac1ff221245cd2557ed8c7c98",
		},
		"cert-manager controller image": {
			contract.CertManager.Images["controller"],
			"quay.io/jetstack/cert-manager-controller:v1.21.1@sha256:416a2d76870d996460e62bd7f521bf14fa017be9e3e904aab92163a331fcb61a",
		},
		"cert-manager webhook image": {
			contract.CertManager.Images["webhook"],
			"quay.io/jetstack/cert-manager-webhook:v1.21.1@sha256:d8b3961b51c8c7320633f8208dc46bf88aa13804d0f7cbe48a096b2c523cee42",
		},
		"Barman manifest URL": {
			contract.BarmanCloudPlugin.ManifestURL,
			"https://github.com/cloudnative-pg/plugin-barman-cloud/releases/download/v0.14.0/manifest.yaml",
		},
		"Barman manifest SHA-256": {
			contract.BarmanCloudPlugin.ManifestSHA256,
			"8d4f1719cc54891ddffd7633279ec93b5d2cc547df8684c3b84f3b156a615e7c",
		},
		"Barman operator image": {
			contract.BarmanCloudPlugin.OperatorImage,
			"ghcr.io/cloudnative-pg/plugin-barman-cloud:v0.14.0@sha256:823a8893690980ba5830bbbb11196a35f695b0488db7d846abc33baebf32417c",
		},
		"Barman sidecar image": {
			contract.BarmanCloudPlugin.SidecarImage,
			"ghcr.io/cloudnative-pg/plugin-barman-cloud-sidecar:v0.14.0@sha256:9880817c285c7afa4d195da2145064d21907405489ed6ec39abe59b1feb558a4",
		},
		"MinIO image": {
			contract.LocalConformance.MinIOImage,
			"minio/minio:RELEASE.2025-04-22T22-12-26Z@sha256:a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e",
		},
	}
	for name, values := range exactValues {
		if values[0] != values[1] {
			t.Errorf("%s = %q, want %q", name, values[0], values[1])
		}
	}
	identities := []string{
		contract.CloudNativePG.OperatorImage,
		contract.CloudNativePG.PostgresImage,
		contract.BarmanCloudPlugin.OperatorImage,
		contract.BarmanCloudPlugin.SidecarImage,
		contract.LocalConformance.MinIOImage,
	}
	for name, image := range contract.CertManager.Images {
		if name == "" || image == "" {
			t.Fatalf("empty cert-manager image identity = %q:%q", name, image)
		}
		identities = append(identities, image)
	}
	if len(contract.CertManager.Images) != 3 {
		t.Fatalf("cert-manager images = %#v", contract.CertManager.Images)
	}
	for _, identity := range identities {
		parts := strings.Split(identity, "@sha256:")
		if len(parts) != 2 || parts[0] == "" || !validSHA256(parts[1]) {
			t.Errorf("mutable or empty image digest identity %q", identity)
		}
	}
	if contract.CloudNativePG.PostgresImage != postgresImage {
		t.Errorf("PostgreSQL image contract %q != Cluster %q", contract.CloudNativePG.PostgresImage, postgresImage)
	}
	for name, value := range map[string]string{
		"CloudNativePG": contract.CloudNativePG.ManifestURL,
		"cert-manager":  contract.CertManager.ManifestURL,
		"Barman":        contract.BarmanCloudPlugin.ManifestURL,
	} {
		if !strings.HasPrefix(value, "https://") {
			t.Errorf("%s manifest URL is not HTTPS: %q", name, value)
		}
	}
	for name, value := range map[string]string{
		"CloudNativePG": contract.CloudNativePG.ManifestSHA256,
		"cert-manager":  contract.CertManager.ManifestSHA256,
		"Barman":        contract.BarmanCloudPlugin.ManifestSHA256,
	} {
		if !validSHA256(value) {
			t.Errorf("%s manifest SHA-256 is invalid: %q", name, value)
		}
	}
	assertBarmanPluginRBACHardeningContract(
		t,
		contract.BarmanCloudPlugin.InstallKustomization,
	)
}

func assertBarmanPluginRBACHardeningContract(t *testing.T, relativePath string) {
	t.Helper()
	contents, err := os.ReadFile(controlStoragePath(t, relativePath))
	if err != nil {
		t.Fatalf("read Barman install kustomization: %v", err)
	}
	var kustomization barmanInstallKustomizationContract
	if err := yaml.Unmarshal(contents, &kustomization); err != nil {
		t.Fatalf("decode Barman install kustomization: %v", err)
	}
	if kustomization.APIVersion != "kustomize.config.k8s.io/v1beta1" ||
		kustomization.Kind != "Kustomization" || len(kustomization.Resources) != 1 ||
		kustomization.Resources[0] != "manifest.yaml" || len(kustomization.Patches) != 1 {
		t.Fatalf("Barman install kustomization = %#v", kustomization)
	}
	patch := kustomization.Patches[0]
	if patch.Target.Group != "rbac.authorization.k8s.io" || patch.Target.Version != "v1" ||
		patch.Target.Kind != "ClusterRole" || patch.Target.Name != "plugin-barman-cloud" {
		t.Fatalf("Barman RBAC patch target = %#v", patch.Target)
	}
	var operations []kustomizePatchOperation
	if err := yaml.Unmarshal([]byte(patch.Patch), &operations); err != nil {
		t.Fatalf("decode Barman RBAC patch: %v", err)
	}
	if len(operations) != 4 ||
		operations[0].Op != "test" || operations[0].Path != "/rules/0/apiGroups/0" ||
		operations[0].Value != "" ||
		operations[1].Op != "test" || operations[1].Path != "/rules/0/resources/0" ||
		operations[1].Value != "secrets" ||
		operations[2].Op != "replace" || operations[2].Path != "/rules/0/verbs" ||
		!reflect.DeepEqual(operations[2].Value, []any{"get", "list", "watch"}) ||
		operations[3].Op != "add" || operations[3].Path != "/rules/0/resourceNames" ||
		!reflect.DeepEqual(operations[3].Value, []any{"vela-backup-s3"}) {
		t.Fatalf("Barman RBAC hardening operations = %#v", operations)
	}
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func assertPostgreSQLDisruptionBudget(t *testing.T) {
	t.Helper()
	contents, err := os.ReadFile(controlStoragePath(t, "disruption-budgets.yaml"))
	if err != nil {
		t.Fatalf("read Control/Storage disruption budgets: %v", err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	for {
		var document struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
			Spec struct {
				MinAvailable int `yaml:"minAvailable"`
				Selector     struct {
					MatchLabels map[string]string `yaml:"matchLabels"`
				} `yaml:"selector"`
			} `yaml:"spec"`
		}
		if err := decoder.Decode(&document); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decode Control/Storage disruption budgets: %v", err)
		}
		if document.Kind == "PodDisruptionBudget" && document.Metadata.Name == "vela-postgres" {
			if document.Spec.MinAvailable != 2 ||
				document.Spec.Selector.MatchLabels["cnpg.io/cluster"] != "vela-postgres" {
				t.Fatalf("PostgreSQL disruption budget = %#v", document.Spec)
			}
			return
		}
	}
	t.Fatal("PostgreSQL disruption budget is missing")
}

func loadControlStorageYAML(t *testing.T, name string, target any) {
	t.Helper()
	contents, err := os.ReadFile(controlStoragePath(t, name))
	if err != nil {
		t.Fatalf("read Control/Storage %s: %v", name, err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	if err := decoder.Decode(target); err != nil {
		t.Fatalf("decode Control/Storage %s: %v", name, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		t.Fatalf("Control/Storage %s trailing YAML = %v", name, err)
	}
}
