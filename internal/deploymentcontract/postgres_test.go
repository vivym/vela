package deploymentcontract

import (
	"bytes"
	"io"
	"os"
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
		Backup struct {
			RetentionPolicy   string `yaml:"retentionPolicy"`
			BarmanObjectStore struct {
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
			} `yaml:"barmanObjectStore"`
		} `yaml:"backup"`
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

func TestControlStoragePostgreSQLContractRequiresAutomaticFailoverAndNoQuorumSafety(
	t *testing.T,
) {
	var cluster cnpgClusterContract
	loadControlStorageYAML(t, "postgres-cluster.yaml", &cluster)
	if cluster.APIVersion != "postgresql.cnpg.io/v1" || cluster.Kind != "Cluster" ||
		cluster.Metadata.Name != "vela-postgres" || cluster.Spec.Instances != 3 ||
		cluster.Spec.ImageName != "ghcr.io/cloudnative-pg/postgresql:16.4" ||
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
	backup := cluster.Spec.Backup
	if backup.RetentionPolicy != "30d" || backup.BarmanObjectStore.DestinationPath == "" ||
		backup.BarmanObjectStore.EndpointURL == "" ||
		backup.BarmanObjectStore.S3Credentials.AccessKeyID != (cnpgSecretKeySelector{
			Name: "vela-backup-s3", Key: "ACCESS_KEY_ID",
		}) || backup.BarmanObjectStore.S3Credentials.SecretAccessKey != (cnpgSecretKeySelector{
		Name: "vela-backup-s3", Key: "SECRET_ACCESS_KEY",
	}) || backup.BarmanObjectStore.WAL.Compression != "gzip" ||
		backup.BarmanObjectStore.WAL.MaxParallel != 2 {
		t.Fatalf("CloudNativePG backup contract = %#v", backup)
	}

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
