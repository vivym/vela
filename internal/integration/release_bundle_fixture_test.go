//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/releasebundle"
)

type catalogReleaseResource struct {
	apiVersion string
	kind       string
	namespace  string
	name       string
}

var catalogReleaseInventory = map[string][]catalogReleaseResource{
	"control-storage": {
		{apiVersion: "v1", kind: "Namespace", name: "vela-system"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-system", name: "nats-config"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-system", name: "vela-barman-cloud-plugin-contract-r1"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-system", name: "vela-jetstream-contract-r1"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-system", name: "vela-recovery-contract"},
		{apiVersion: "v1", kind: "Service", namespace: "vela-system", name: "nats"},
		{apiVersion: "apps/v1", kind: "StatefulSet", namespace: "vela-system", name: "nats"},
		{apiVersion: "policy/v1", kind: "PodDisruptionBudget", namespace: "vela-system", name: "nats"},
		{apiVersion: "policy/v1", kind: "PodDisruptionBudget", namespace: "vela-system", name: "vela-postgres"},
		{apiVersion: "barmancloud.cnpg.io/v1", kind: "ObjectStore", namespace: "vela-system", name: "vela-postgres-backup"},
		{apiVersion: "postgresql.cnpg.io/v1", kind: "Cluster", namespace: "vela-system", name: "vela-postgres"},
		{apiVersion: "postgresql.cnpg.io/v1", kind: "ScheduledBackup", namespace: "vela-system", name: "vela-postgres-daily"},
	},
	"fleet-controller": {
		{apiVersion: "v1", kind: "Namespace", name: "vela-system"},
		{apiVersion: "v1", kind: "ServiceAccount", namespace: "vela-system", name: "vela-fleet-controller"},
		{apiVersion: "rbac.authorization.k8s.io/v1", kind: "Role", namespace: "vela-system", name: "vela-fleet-controller"},
		{apiVersion: "rbac.authorization.k8s.io/v1", kind: "ClusterRole", name: "vela-fleet-controller-node-reader"},
		{apiVersion: "rbac.authorization.k8s.io/v1", kind: "RoleBinding", namespace: "vela-system", name: "vela-fleet-controller"},
		{apiVersion: "rbac.authorization.k8s.io/v1", kind: "ClusterRoleBinding", name: "vela-fleet-controller-node-reader"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-system", name: "vela-fleet-residency-plan-rollouts-r1"},
		{apiVersion: "v1", kind: "Service", namespace: "vela-system", name: "vela-fleet-admission"},
		{apiVersion: "apps/v1", kind: "Deployment", namespace: "vela-system", name: "vela-fleet-controller"},
		{apiVersion: "policy/v1", kind: "PodDisruptionBudget", namespace: "vela-system", name: "vela-fleet-controller"},
		{apiVersion: "admissionregistration.k8s.io/v1", kind: "ValidatingWebhookConfiguration", name: "vela-fleet-protection"},
	},
	"observability": {
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-observability", name: "vela-slo-alert-rules-r1"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-observability", name: "vela-slo-contract-r1"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-observability", name: "vela-slo-dashboard-r1"},
		{apiVersion: "monitoring.coreos.com/v1", kind: "PodMonitor", namespace: "vela-observability", name: "vela-control"},
	},
	"stage-worker": {
		{apiVersion: "v1", kind: "Namespace", name: "vela-system"},
		{apiVersion: "v1", kind: "ServiceAccount", namespace: "vela-system", name: "vela-worker"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-system", name: "vela-stage-worker-runtime-r1"},
	},
	"vela-control": {
		{apiVersion: "v1", kind: "Namespace", name: "vela-system"},
		{apiVersion: "v1", kind: "ServiceAccount", namespace: "vela-system", name: "vela-control"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-system", name: "vela-control-node-agents-r1"},
		{apiVersion: "v1", kind: "ConfigMap", namespace: "vela-system", name: "vela-control-runtime-r1"},
		{apiVersion: "v1", kind: "Service", namespace: "vela-system", name: "vela-api"},
		{apiVersion: "v1", kind: "Service", namespace: "vela-system", name: "vela-compliance"},
		{apiVersion: "v1", kind: "Service", namespace: "vela-system", name: "vela-control"},
		{apiVersion: "v1", kind: "Service", namespace: "vela-system", name: "vela-finance-reconciliation"},
		{apiVersion: "v1", kind: "Service", namespace: "vela-system", name: "vela-stage-worker-control"},
		{apiVersion: "scheduling.k8s.io/v1", kind: "PriorityClass", name: "vela-control-critical"},
		{apiVersion: "apps/v1", kind: "Deployment", namespace: "vela-system", name: "vela-control"},
		{apiVersion: "policy/v1", kind: "PodDisruptionBudget", namespace: "vela-system", name: "vela-control"},
		{apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", namespace: "vela-system", name: "vela-control-allow-api"},
		{apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", namespace: "vela-system", name: "vela-control-allow-compliance"},
		{apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", namespace: "vela-system", name: "vela-control-allow-finance"},
		{apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", namespace: "vela-system", name: "vela-control-allow-fleet"},
		{apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", namespace: "vela-system", name: "vela-control-allow-observability"},
		{apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", namespace: "vela-system", name: "vela-control-allow-stage-worker"},
		{apiVersion: "networking.k8s.io/v1", kind: "NetworkPolicy", namespace: "vela-system", name: "vela-control-default-deny-ingress"},
	},
}

func writeCatalogReleaseBundleFixture(t *testing.T, directory, variant string) releasebundle.Bundle {
	t.Helper()
	controlManifest := writeCatalogOCIManifest(t, directory, "control")
	stageManifest := writeCatalogOCIManifest(t, directory, "stage-worker")
	runtimeManifest := writeCatalogOCIManifest(t, directory, "h3-stage-runtime")
	images := []string{
		"ghcr.io/vivym/vela-control@" + controlManifest.digest,
		"ghcr.io/vivym/vela-stage-worker-agent@" + stageManifest.digest,
		"ghcr.io/vivym/vela-h3-stage-runtime@" + runtimeManifest.digest,
	}
	for _, name := range []string{
		"control-storage", "fleet-controller", "observability", "stage-worker", "vela-control",
	} {
		writeCatalogReleaseFile(
			t,
			filepath.Join(directory, "render-"+name+".yaml"),
			[]byte(catalogReleaseRender(name, images[0])),
		)
	}
	rollout := catalogResidencyPlanRollout(t, images)
	rollouts, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"rollouts":       []fleetcontroller.ResidencyPlanRollout{rollout},
	})
	if err != nil {
		t.Fatalf("encode Catalog ResidencyPlan rollout: %v", err)
	}
	fleetRenderPath := filepath.Join(directory, "render-fleet-controller.yaml")
	fleetRender, err := os.ReadFile(fleetRenderPath)
	if err != nil {
		t.Fatalf("read Catalog Fleet render: %v", err)
	}
	identity := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: vela-fleet-residency-plan-rollouts-r1\n  namespace: vela-system\n"
	replacement := identity + "immutable: true\ndata:\n  rollouts.json: |-\n    " +
		strings.ReplaceAll(string(rollouts), "\n", "\n    ") + "\n"
	if !strings.Contains(string(fleetRender), identity) {
		t.Fatal("Catalog Fleet render lacks ResidencyPlan ConfigMap")
	}
	writeCatalogReleaseFile(
		t, fleetRenderPath, []byte(strings.Replace(string(fleetRender), identity, replacement, 1)),
	)

	writeCatalogReleaseFile(t, filepath.Join(directory, "node-agent.service"), []byte(`[Unit]
Description=Vela host remediation Node Agent
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/vela-node-agent
EnvironmentFile=/etc/vela/node-agent.env
Restart=on-failure
RestartSec=5s
UMask=0077
RuntimeDirectory=vela-node-agent
RuntimeDirectoryMode=0755
StateDirectory=vela-node-agent
ProtectHome=true
PrivateTmp=true
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
LockPersonality=true
MemoryDenyWriteExecute=true
NoNewPrivileges=false
LimitNOFILE=4096

[Install]
WantedBy=multi-user.target
`))
	packageArtifact := []byte("production package for node-agent" + variant)
	writeCatalogReleaseFile(t, filepath.Join(directory, "node-agent.tar"), packageArtifact)
	writeJSONFixture(t, filepath.Join(directory, "node-agent-contract.json"), releasebundle.PackageContract{
		SchemaVersion: 1, Name: "vela-node-agent", OS: "linux", Architecture: "amd64",
		Revision: "release-r1", Entrypoint: "/usr/local/bin/vela-node-agent",
		ArtifactDigest: catalogReleaseContentDigest(packageArtifact), ArtifactSizeBytes: int64(len(packageArtifact)),
	})

	consumer := "ConfigMap/vela-system/vela-fleet-residency-plan-rollouts-r1"
	plan := releasebundle.BuildPlan{
		SchemaVersion: releasebundle.SchemaVersion,
		NodeAgentUnit: releasebundle.ArtifactInput{Name: "node-agent-systemd-unit", Ref: "node-agent.service"},
		Packages: []releasebundle.PackageInput{{
			Name: "node-agent", ContractRef: "node-agent-contract.json", ArtifactRef: "node-agent.tar",
		}},
		ExternalResources: []releasebundle.ExternalResource{
			{
				Kind: "Secret", Namespace: "vela-observability", Name: "shared-secret-r1",
				Revision: catalogReleaseDigest("observability shared"), RequiredKeys: []string{"token"},
				Consumers: []string{"PodMonitor/vela-observability/vela-control"},
			},
			{
				Kind: "Secret", Namespace: "vela-system", Name: "shared-secret-r1",
				Revision: catalogReleaseDigest("shared"), RequiredKeys: []string{"token"},
				Consumers: []string{
					"Deployment/vela-system/vela-control",
					"Deployment/vela-system/vela-fleet-controller",
					"StatefulSet/vela-system/nats",
				},
			},
			{
				Kind: "Secret", Namespace: "vela-system", Name: "stage-worker-control-r1",
				Revision: catalogReleaseDigest("stage control"), RequiredKeys: []string{
					"49330000-0000-0000-0000-000000000006.tls.crt",
					"49330000-0000-0000-0000-000000000006.tls.key",
					"ca.crt",
				},
				Consumers: []string{consumer},
			},
			{
				Kind: "Secret", Namespace: "vela-system", Name: "stage-worker-authority-r1",
				Revision: catalogReleaseDigest("stage authority"), RequiredKeys: []string{"keyring.json"},
				Consumers: []string{consumer},
			},
			{
				Kind: "Secret", Namespace: "vela-system", Name: "stage-worker-artifact-credentials-r1",
				Revision:     catalogReleaseDigest("stage artifact credentials"),
				RequiredKeys: []string{"access-key-id", "secret-access-key"}, Consumers: []string{consumer},
			},
			{
				Kind: "Secret", Namespace: "vela-system", Name: "stage-worker-artifact-ca-r1",
				Revision: catalogReleaseDigest("stage artifact ca"), RequiredKeys: []string{"ca.crt"},
				Consumers: []string{consumer},
			},
		},
		OCIManifests: []releasebundle.OCIManifestInput{
			{Image: images[0], Ref: controlManifest.ref, ConfigRef: controlManifest.configRef},
			{Image: images[1], Ref: stageManifest.ref, ConfigRef: stageManifest.configRef},
			{Image: images[2], Ref: runtimeManifest.ref, ConfigRef: runtimeManifest.configRef},
		},
	}
	for _, name := range []string{
		"control-storage", "fleet-controller", "observability", "stage-worker", "vela-control",
	} {
		plan.FinalRenders = append(plan.FinalRenders, releasebundle.ArtifactInput{
			Name: name, Ref: "render-" + name + ".yaml",
		})
	}
	planPath := filepath.Join(directory, "release-bundle-plan.json")
	writeJSONFixture(t, planPath, plan)
	bundle, encoded, err := releasebundle.BuildFromSource(newCatalogReleaseSource(t), planPath)
	if err != nil {
		t.Fatalf("build Catalog release bundle fixture: %v", err)
	}
	writeCatalogReleaseFile(t, filepath.Join(directory, "release-bundle.json"), encoded)
	return bundle
}

func newCatalogReleaseSource(t *testing.T) string {
	t.Helper()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve Catalog release source checkout: %v", err)
	}
	writeCatalogReleaseFile(t, filepath.Join(directory, "source.txt"), []byte("catalog release source"))
	runLegacyH3FixtureGit(t, directory, "init", "--quiet")
	runLegacyH3FixtureGit(t, directory, "add", "source.txt")
	runLegacyH3FixtureGit(
		t, directory,
		"-c", "user.name=Vela Test", "-c", "user.email=vela-test@example.invalid",
		"commit", "--quiet", "-m", "test source",
	)
	return directory
}

func catalogResidencyPlanRollout(
	t *testing.T,
	images []string,
) fleetcontroller.ResidencyPlanRollout {
	t.Helper()
	planID := uuid.MustParse("49330000-0000-0000-0000-000000000001")
	bundleID := uuid.MustParse("49330000-0000-0000-0000-000000000002")
	poolID := uuid.MustParse("49330000-0000-0000-0000-000000000003")
	workerID := uuid.MustParse("49330000-0000-0000-0000-000000000004")
	profileID := uuid.MustParse("49330000-0000-0000-0000-000000000005")
	actuation := fleetcontroller.WorkerBundleActuation{
		SchemaVersion: 1, PlanRevisionID: planID, WorkerBundleID: bundleID,
		Namespace: "vela-system", InitImage: images[0], StageWorkerAgentImage: images[1], RuntimeImage: images[2],
		StageWorkerConfigMap:           "vela-stage-worker-runtime-r1",
		ModelRuntimeVerifierConfigMap:  "model-runtime-verifier-r1",
		StageWorkerControlTLSSecret:    "stage-worker-control-r1",
		StageWorkerAuthoritySecret:     "stage-worker-authority-r1",
		ArtifactStoreCredentialsSecret: "stage-worker-artifact-credentials-r1",
		ArtifactStoreCASecret:          "stage-worker-artifact-ca-r1",
		WorkerInstances: []fleetcontroller.WorkerInstanceActuation{{
			ID: workerID, InstanceEpoch: 1, WorkerProfileRevisionID: profileID,
			CapacityPoolID: poolID, Role: "dit", CapacitySlots: 1,
			DeviceSetDigest: strings.Repeat("1", 64), MembershipDigest: strings.Repeat("2", 64),
			ModelRuntimes: []fleetcontroller.ModelRuntimeProcess{{
				ModelResidencyID:       uuid.MustParse("49330000-0000-0000-0000-000000000009"),
				CapacityPoolID:         poolID,
				StageProfileRevisionID: uuid.MustParse("49330000-0000-0000-0000-000000000008"),
				ModelRuntimeEpochFloor: 1,
				Component:              "DIT", ModelComponentRevision: "h3-dit-r1",
				RuntimeIdentity: "h3-dit-runtime-r1", Command: []string{"/opt/vela/bin/h3-dit"},
				InitializationTimeout: "2h", ShutdownTimeout: "2m",
			}},
			Members: []fleetcontroller.WorkerMemberActuation{{
				ID: uuid.MustParse("49330000-0000-0000-0000-000000000006"), MemberEpoch: 1,
				Key: "member-0", NodeIdentity: "h3-node-01", ResourceClass: "GPU", DeviceCount: 1,
				IdentityDigest: "0f2afefa5711edb6538b2e58335c7a3bbc40502f4bffa7f0d4eaa175961ae85c",
				DeviceConstraints: []fleetcontroller.DeviceConstraint{{
					DeviceID: uuid.MustParse("49330000-0000-0000-0000-000000000007"), DeviceEpoch: 1,
					GPUUUID: "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0",
				}},
			}},
		}},
	}
	var err error
	actuation.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(actuation)
	if err != nil {
		t.Fatalf("compute Catalog WorkerBundle digest: %v", err)
	}
	return fleetcontroller.ResidencyPlanRollout{
		ApprovedPlan: fleet.ApprovedResidencyPlan{
			SchemaVersion: 1, ID: planID, StableID: "h3-stage-target-r1", Revision: 1,
			ContentDigest: actuation.RevisionDigest, ApprovalEvidenceDigest: strings.Repeat("e", 64),
			ApprovedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), ApprovedBy: "fleet/operator-1",
			CapacityPools: []fleet.PlannedCapacityPool{{
				ID: poolID, StableID: "h3-dit",
				StageProfileRevisionID: uuid.MustParse("49330000-0000-0000-0000-000000000008"),
				ResourceClass:          "GPU", SecurityClass: "INTERNAL", Region: "cn-shanghai", MaxReadyQueueDepth: 128,
			}},
			WorkerBundles: []fleet.PlannedWorkerBundle{{
				ID: bundleID, StableID: "h3-node-01", DesiredGeneration: 1, LayoutDigest: actuation.RevisionDigest,
			}},
			WorkerInstances: []fleet.PlannedWorkerInstance{{
				ID: workerID, WorkerProfileRevisionID: profileID, CapacityPoolID: poolID,
				WorkerBundleID: bundleID, DesiredMemberCount: 1, DesiredDeviceCount: 1,
				ModelRuntimeRoutes: []fleet.PlannedModelRuntimeRoute{{
					ModelResidencyID:       uuid.MustParse("49330000-0000-0000-0000-000000000009"),
					CapacityPoolID:         poolID,
					StageProfileRevisionID: uuid.MustParse("49330000-0000-0000-0000-000000000008"),
				}},
			}},
		},
		WorkerBundles: []fleetcontroller.WorkerBundleActuation{actuation},
	}
}

func relocateCatalogReleaseBundleFixture(
	t *testing.T,
	directory,
	bundleRef,
	destinationDirectory string,
) string {
	t.Helper()
	bundle, err := releasebundle.LoadWithin(directory, bundleRef)
	if err != nil {
		t.Fatalf("load Catalog release bundle before relocation: %v", err)
	}
	references := make(map[string]struct{})
	for _, render := range bundle.ConfigurationManifest.FinalRenders {
		references[render.Artifact.Ref] = struct{}{}
	}
	references[bundle.ConfigurationManifest.NodeAgentUnit.Artifact.Ref] = struct{}{}
	for _, item := range bundle.ConfigurationManifest.Packages {
		references[item.Contract.Ref] = struct{}{}
		references[item.Artifact.Ref] = struct{}{}
	}
	for _, image := range bundle.OCIImages {
		references[image.Descriptor.Ref] = struct{}{}
		references[image.Config.Ref] = struct{}{}
	}
	for reference := range references {
		source := filepath.Join(directory, filepath.FromSlash(reference))
		destination := filepath.Join(directory, destinationDirectory, filepath.FromSlash(reference))
		if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
			t.Fatalf("create nested Catalog release artifact directory: %v", err)
		}
		if err := os.Rename(source, destination); err != nil {
			t.Fatalf("relocate Catalog release artifact %s: %v", reference, err)
		}
	}
	nestedBundleRef := filepath.ToSlash(filepath.Join(destinationDirectory, filepath.Base(bundleRef)))
	if err := os.Rename(
		filepath.Join(directory, filepath.FromSlash(bundleRef)),
		filepath.Join(directory, filepath.FromSlash(nestedBundleRef)),
	); err != nil {
		t.Fatalf("relocate Catalog release bundle: %v", err)
	}
	return nestedBundleRef
}

type catalogOCIManifest struct {
	ref       string
	configRef string
	digest    string
}

func writeCatalogOCIManifest(t *testing.T, directory, name string) catalogOCIManifest {
	t.Helper()
	entrypoint := "/usr/local/bin/" + name
	if name == "h3-stage-runtime" {
		entrypoint = "/usr/local/bin/vela-model-runtime"
	}
	imageConfig := map[string]any{"Entrypoint": []string{entrypoint}}
	if name == "h3-stage-runtime" {
		imageConfig["Labels"] = map[string]string{
			"vela.ai.h3-runtime-base":       "ghcr.io/minimax/h3-runtime-base@" + catalogReleaseDigest("runtime base"),
			"vela.ai.h3-encoder.sha256":     strings.TrimPrefix(catalogReleaseDigest("encoder"), "sha256:"),
			"vela.ai.h3-dit.sha256":         strings.TrimPrefix(catalogReleaseDigest("dit"), "sha256:"),
			"vela.ai.h3-vae-decoder.sha256": strings.TrimPrefix(catalogReleaseDigest("vae decoder"), "sha256:"),
		}
	}
	config, err := json.Marshal(map[string]any{
		"architecture": "amd64",
		"os":           "linux",
		"config":       imageConfig,
		"rootfs":       map[string]any{"type": "layers", "diff_ids": []string{}},
	})
	if err != nil {
		t.Fatalf("encode Catalog OCI config: %v", err)
	}
	configRef := "oci-" + name + "-config.json"
	writeCatalogReleaseFile(t, filepath.Join(directory, configRef), config)
	manifest, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"mediaType":     releasebundle.OCIManifestMediaType,
		"config": map[string]any{
			"mediaType": releasebundle.OCIImageConfigMediaType,
			"digest":    catalogReleaseContentDigest(config),
			"size":      len(config),
		},
		"layers": []any{map[string]any{
			"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
			"digest":    catalogReleaseDigest(name + " layer"),
			"size":      256,
		}},
	})
	if err != nil {
		t.Fatalf("encode Catalog OCI manifest: %v", err)
	}
	ref := "oci-" + name + ".json"
	writeCatalogReleaseFile(t, filepath.Join(directory, ref), manifest)
	return catalogOCIManifest{ref: ref, configRef: configRef, digest: catalogReleaseContentDigest(manifest)}
}

func catalogReleaseRender(name, image string) string {
	var rendered strings.Builder
	for _, resource := range catalogReleaseInventory[name] {
		isWorkload := resource.kind == "StatefulSet" || resource.kind == "Deployment" ||
			(name == "observability" && resource.kind == "PodMonitor")
		if !isWorkload {
			rendered.WriteString(catalogReleaseObject(resource))
			continue
		}
		rendered.WriteString(catalogReleaseWorkload(resource, image))
	}
	return rendered.String()
}

func catalogReleaseObject(resource catalogReleaseResource) string {
	encoded := "apiVersion: " + resource.apiVersion + "\nkind: " + resource.kind +
		"\nmetadata:\n  name: " + resource.name + "\n"
	if resource.namespace != "" {
		encoded += "  namespace: " + resource.namespace + "\n"
	}
	return encoded + "---\n"
}

func catalogReleaseWorkload(resource catalogReleaseResource, image string) string {
	return "apiVersion: " + resource.apiVersion + "\nkind: " + resource.kind +
		"\nmetadata:\n  name: " + resource.name + "\n  namespace: " + resource.namespace + `
spec:
  template:
    spec:
      containers:
        - name: service
          image: ` + image + `
      volumes:
        - name: credentials
          secret:
            secretName: shared-secret-r1
            items:
              - key: token
                path: token
---
`
}

func catalogReleaseConfigMap(namespace, name, key, value string) string {
	return "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: " + name + "\n  namespace: " + namespace +
		"\nimmutable: true\ndata:\n  " + key + ": |-\n    " + strings.ReplaceAll(value, "\n", "\n    ") + "\n"
}

func writeCatalogReleaseFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write Catalog release fixture: %v", err)
	}
}

func catalogReleaseDigest(value string) string {
	return catalogReleaseContentDigest([]byte(value))
}

func catalogReleaseContentDigest(content []byte) string {
	digest := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(digest[:])
}
