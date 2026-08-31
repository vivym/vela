package releasebundle

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
)

func TestBuildAndLoadCanonicalReleaseBundle(t *testing.T) {
	fixture := newBundleFixture(t)
	bundle, first, err := Build(fixture.planPath)
	if err != nil {
		t.Fatalf("build release bundle: %v", err)
	}
	if bundle.SchemaVersion != 1 || !validDigest(bundle.ReleaseDigest) ||
		!validDigest(bundle.ConfigurationRevision) ||
		bundle.ReleaseDescriptor.MediaType != ReleaseDescriptorMediaType ||
		bundle.ReleaseDescriptor.Config.Digest != bundle.ConfigurationRevision ||
		len(bundle.ConfigurationManifest.FinalRenders) != 6 ||
		len(bundle.ConfigurationManifest.Packages) != 2 ||
		len(bundle.ConfigurationManifest.WorkerMaterializations) != 1 || len(bundle.OCIImages) != 2 {
		t.Fatalf("built bundle = %#v", bundle)
	}
	for _, image := range bundle.OCIImages {
		if image.Config.MediaType != OCIImageConfigMediaType || image.Platform != (Platform{OS: "linux", Architecture: "amd64"}) {
			t.Fatalf("OCI image config binding = %#v", image)
		}
	}
	configurationDigest, _, err := canonicalDigest(bundle.ConfigurationManifest)
	if err != nil || configurationDigest != bundle.ConfigurationRevision {
		t.Fatalf("configuration digest = %q error=%v", configurationDigest, err)
	}
	releaseDigest, _, err := canonicalDigest(bundle.ReleaseDescriptor)
	if err != nil || releaseDigest != bundle.ReleaseDigest {
		t.Fatalf("release digest = %q error=%v", releaseDigest, err)
	}
	_, second, err := Build(fixture.planPath)
	if err != nil || string(first) != string(second) {
		t.Fatalf("deterministic build differs: error=%v\nfirst=%s\nsecond=%s", err, first, second)
	}
	bundlePath := filepath.Join(fixture.directory, "release-bundle.json")
	writeTestFile(t, bundlePath, first)
	loaded, err := Load(bundlePath)
	if err != nil || loaded.ReleaseDigest != bundle.ReleaseDigest ||
		loaded.ConfigurationRevision != bundle.ConfigurationRevision {
		t.Fatalf("load bundle = %#v error=%v", loaded, err)
	}
}

func TestBuildBindsTargetResidencyPlanStageWorkerAndSecrets(t *testing.T) {
	fixture := newBundleFixture(t)
	stageManifest := testOCIManifest(t, fixture.directory, "stage-worker")
	runtimeManifest := testOCIManifest(t, fixture.directory, "h3-stage-runtime")
	stageImage := "ghcr.io/vivym/vela-stage-worker-agent@" + stageManifest.digest
	runtimeImage := "ghcr.io/vivym/vela-h3-stage-runtime@" + runtimeManifest.digest
	fixture.plan.OCIManifests = append(fixture.plan.OCIManifests,
		OCIManifestInput{
			Image: stageImage, Ref: stageManifest.ref, ConfigRef: stageManifest.configRef,
		},
		OCIManifestInput{
			Image: runtimeImage, Ref: runtimeManifest.ref, ConfigRef: runtimeManifest.configRef,
		},
	)

	planID := uuid.MustParse("49330000-0000-0000-0000-000000000001")
	bundleID := uuid.MustParse("49330000-0000-0000-0000-000000000002")
	poolID := uuid.MustParse("49330000-0000-0000-0000-000000000003")
	workerID := uuid.MustParse("49330000-0000-0000-0000-000000000004")
	profileID := uuid.MustParse("49330000-0000-0000-0000-000000000005")
	actuation := fleetcontroller.WorkerBundleActuation{
		SchemaVersion: 1, PlanRevisionID: planID, WorkerBundleID: bundleID,
		Namespace: "vela-system", InitImage: fixture.images[0],
		StageWorkerAgentImage:          stageImage,
		RuntimeImage:                   runtimeImage,
		StageWorkerConfigMap:           "vela-stage-worker-runtime-r1",
		StageWorkerControlTLSSecret:    "stage-worker-control-r1",
		StageWorkerAuthoritySecret:     "stage-worker-authority-r1",
		ArtifactStoreCredentialsSecret: "stage-worker-artifact-credentials-r1",
		ArtifactStoreCASecret:          "stage-worker-artifact-ca-r1",
		WorkerInstances: []fleetcontroller.WorkerInstanceActuation{{
			ID: workerID, InstanceEpoch: 1, WorkerProfileRevisionID: profileID,
			CapacityPoolID: poolID, Role: "dit", CapacitySlots: 1,
			ModelRuntimes: []fleetcontroller.ModelRuntimeProcess{{
				Component: "DIT", ModelComponentRevision: "h3-dit-r1",
				RuntimeIdentity: "h3-dit-runtime-r1", Command: []string{"/opt/vela/bin/h3-dit"},
			}},
			Members: []fleetcontroller.WorkerMemberActuation{{
				ID: uuid.MustParse("49330000-0000-0000-0000-000000000006"), MemberEpoch: 1,
				Key: "member-0", NodeIdentity: "h3-node-01", ResourceClass: "GPU", DeviceCount: 1,
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
		t.Fatalf("digest target WorkerBundle: %v", err)
	}
	rollout := fleetcontroller.ResidencyPlanRollout{
		ApprovedPlan: fleet.ApprovedResidencyPlan{
			SchemaVersion: 1, ID: planID, StableID: "h3-stage-target-r1", Revision: 1,
			ContentDigest: actuation.RevisionDigest, ApprovalEvidenceDigest: strings.Repeat("e", 64),
			ApprovedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), ApprovedBy: "fleet/operator-1",
			CapacityPools: []fleet.PlannedCapacityPool{{
				ID: poolID, StableID: "h3-dit", StageProfileRevisionID: uuid.MustParse("49330000-0000-0000-0000-000000000008"),
				ResourceClass: "GPU", SecurityClass: "INTERNAL", Region: "cn-shanghai", MaxReadyQueueDepth: 128,
			}},
			WorkerBundles: []fleet.PlannedWorkerBundle{{
				ID: bundleID, StableID: "h3-node-01", DesiredGeneration: 1, LayoutDigest: actuation.RevisionDigest,
			}},
			WorkerInstances: []fleet.PlannedWorkerInstance{{
				ID: workerID, WorkerProfileRevisionID: profileID, CapacityPoolID: poolID,
				WorkerBundleID: bundleID, DesiredMemberCount: 1, DesiredDeviceCount: 1,
			}},
		},
		WorkerBundles: []fleetcontroller.WorkerBundleActuation{actuation},
	}
	encoded, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"rollouts":       []fleetcontroller.ResidencyPlanRollout{rollout},
	})
	if err != nil {
		t.Fatalf("encode target ResidencyPlan: %v", err)
	}
	writeFleetResidencyPlanRender(t, fixture, encoded)
	fixture.plan.WorkerMaterializations = nil
	findExternalResource(t, fixture, "vela-system", "artifact-store-ca-r1").Consumers =
		[]string{"DaemonSet/vela-system/vela-h3-worker"}
	findExternalResource(t, fixture, "vela-system", "worker-control-tls-node-1").Consumers =
		[]string{"DaemonSet/vela-system/vela-h3-worker"}
	consumer := "ConfigMap/vela-system/vela-fleet-residency-plan-rollouts-r1"
	fixture.plan.ExternalResources = append(fixture.plan.ExternalResources,
		ExternalResource{
			Kind: "ConfigMap", Namespace: "vela-system", Name: "worker-runtime-node-1",
			Revision: testDigest("legacy worker runtime"),
		},
		ExternalResource{
			Kind: "ConfigMap", Namespace: "vela-system", Name: "runner-profiles-node-1",
			Revision: testDigest("legacy runner profiles"),
		},
		ExternalResource{
			Kind: "ConfigMap", Namespace: "vela-system", Name: "runner-gpu-roles-node-1",
			Revision: testDigest("legacy runner gpu roles"),
		},
		ExternalResource{
			Kind: "Secret", Namespace: "vela-system", Name: "stage-worker-control-r1",
			Revision: testDigest("stage control"), RequiredKeys: []string{"ca.crt", "tls.crt", "tls.key"},
			Consumers: []string{consumer},
		},
		ExternalResource{
			Kind: "Secret", Namespace: "vela-system", Name: "stage-worker-authority-r1",
			Revision: testDigest("stage authority"), RequiredKeys: []string{"keyring.json"},
			Consumers: []string{consumer},
		},
		ExternalResource{
			Kind: "Secret", Namespace: "vela-system", Name: "stage-worker-artifact-credentials-r1",
			Revision:     testDigest("stage artifact credentials"),
			RequiredKeys: []string{"access-key-id", "secret-access-key"}, Consumers: []string{consumer},
		},
		ExternalResource{
			Kind: "Secret", Namespace: "vela-system", Name: "stage-worker-artifact-ca-r1",
			Revision: testDigest("stage artifact ca"), RequiredKeys: []string{"ca.crt"},
			Consumers: []string{consumer},
		},
	)
	fixture.writePlan(t)

	bundle, _, err := Build(fixture.planPath)
	if err != nil {
		t.Fatalf("build target ResidencyPlan release bundle: %v", err)
	}
	if len(bundle.ConfigurationManifest.FinalRenders) != 6 ||
		len(bundle.ConfigurationManifest.WorkerMaterializations) != 0 || len(bundle.OCIImages) != 4 {
		t.Fatalf("target ResidencyPlan release bundle = %#v", bundle.ConfigurationManifest)
	}

	rewriteTarget := func(runtimeImage string) {
		t.Helper()
		actuation.RuntimeImage = runtimeImage
		actuation.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(actuation)
		if err != nil {
			t.Fatalf("digest aliased target WorkerBundle: %v", err)
		}
		rollout.WorkerBundles[0] = actuation
		rollout.ApprovedPlan.ContentDigest = actuation.RevisionDigest
		rollout.ApprovedPlan.WorkerBundles[0].LayoutDigest = actuation.RevisionDigest
		encoded, err = json.Marshal(map[string]any{
			"schema_version": 1,
			"rollouts":       []fleetcontroller.ResidencyPlanRollout{rollout},
		})
		if err != nil {
			t.Fatalf("encode aliased target ResidencyPlan: %v", err)
		}
		writeFleetResidencyPlanRender(t, fixture, encoded)
	}

	rewriteTarget(stageImage)
	fixture.plan.OCIManifests = slices.DeleteFunc(
		fixture.plan.OCIManifests,
		func(input OCIManifestInput) bool { return input.Image == runtimeImage },
	)
	fixture.writePlan(t)
	if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
		t.Fatalf("Build with exact ModelRuntime image alias error=%v, want ErrInvalidBundle", err)
	}

	digestAlias := "ghcr.io/vivym/vela-h3-stage-runtime@" + stageManifest.digest
	rewriteTarget(digestAlias)
	fixture.plan.OCIManifests = append(fixture.plan.OCIManifests, OCIManifestInput{
		Image: digestAlias, Ref: stageManifest.ref, ConfigRef: stageManifest.configRef,
	})
	fixture.writePlan(t)
	if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
		t.Fatalf("Build with cross-repository ModelRuntime manifest alias error=%v, want ErrInvalidBundle", err)
	}
}

func TestValidateModelRuntimeOCIConfigRequiresAbsoluteEntrypoint(t *testing.T) {
	image := "ghcr.io/vivym/vela-h3-stage-runtime@" + testDigest("runtime")
	for _, test := range []struct {
		name       string
		entrypoint []string
		wantError  bool
	}{
		{name: "absolute", entrypoint: []string{"/opt/vela/bin/h3-stage-runtime"}},
		{name: "absolute with arguments", entrypoint: []string{"/opt/vela/bin/h3-stage-runtime", "--serve"}},
		{name: "missing", wantError: true},
		{name: "relative", entrypoint: []string{"bin/h3-stage-runtime"}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := json.Marshal(map[string]any{
				"architecture": "amd64", "os": "linux",
				"config": map[string]any{"Entrypoint": test.entrypoint},
				"rootfs": map[string]any{"type": "layers", "diff_ids": []string{}},
			})
			if err != nil {
				t.Fatal(err)
			}
			err = validateModelRuntimeOCIConfig(image, encoded)
			if (err != nil) != test.wantError {
				t.Fatalf("validateModelRuntimeOCIConfig error = %v, wantError=%t", err, test.wantError)
			}
		})
	}
}

func TestBuildIsDeterministicForTwoPlacementsInOneWorkerPool(t *testing.T) {
	fixture := newBundleFixture(t)
	addSecondWorker(t, fixture)
	fixture.writePlan(t)
	firstBundle, first, err := Build(fixture.planPath)
	if err != nil {
		t.Fatalf("build two-Worker bundle: %v", err)
	}
	if len(firstBundle.ConfigurationManifest.WorkerMaterializations) != 2 {
		t.Fatalf("Worker materializations = %#v", firstBundle.ConfigurationManifest.WorkerMaterializations)
	}
	if firstBundle.ConfigurationManifest.WorkerMaterializations[0].WorkerPoolID !=
		firstBundle.ConfigurationManifest.WorkerMaterializations[1].WorkerPoolID {
		t.Fatalf("materializations do not share one WorkerPool: %#v", firstBundle.ConfigurationManifest.WorkerMaterializations)
	}
	slices.Reverse(fixture.plan.WorkerMaterializations)
	slices.Reverse(fixture.plan.ExternalResources)
	fixture.writePlan(t)
	secondBundle, second, err := Build(fixture.planPath)
	if err != nil || string(first) != string(second) || firstBundle.ReleaseDigest != secondBundle.ReleaseDigest {
		t.Fatalf("two-Worker deterministic build mismatch: error=%v", err)
	}
}

func TestBuildRejectsWorkerMaterializationAliasing(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *bundleFixture, *WorkerMaterializationInput, WorkerMaterializationInput)
	}{
		{name: "node", mutate: func(_ *testing.T, _ *bundleFixture, second *WorkerMaterializationInput, first WorkerMaterializationInput) {
			second.NodeIdentity = first.NodeIdentity
			second.NodeAgentIdentity = expectedNodeAgentIdentity(second.NodeIdentity, second.WorkerID)
		}},
		{name: "worker", mutate: func(_ *testing.T, _ *bundleFixture, second *WorkerMaterializationInput, first WorkerMaterializationInput) {
			second.WorkerID = first.WorkerID
			second.NodeAgentIdentity = expectedNodeAgentIdentity(second.NodeIdentity, second.WorkerID)
		}},
		{name: "agent", mutate: func(_ *testing.T, _ *bundleFixture, second *WorkerMaterializationInput, first WorkerMaterializationInput) {
			second.NodeAgentIdentity = first.NodeAgentIdentity
		}},
		{name: "config", mutate: func(_ *testing.T, _ *bundleFixture, second *WorkerMaterializationInput, first WorkerMaterializationInput) {
			second.WorkerRuntimeConfigMap = first.WorkerRuntimeConfigMap
		}},
		{name: "artifact", mutate: func(_ *testing.T, _ *bundleFixture, second *WorkerMaterializationInput, first WorkerMaterializationInput) {
			second.WorkerRuntimeRef = first.WorkerRuntimeRef
		}},
		{name: "TLS name", mutate: func(_ *testing.T, _ *bundleFixture, second *WorkerMaterializationInput, first WorkerMaterializationInput) {
			second.WorkerControlTLSSecret = first.WorkerControlTLSSecret
		}},
		{name: "TLS revision", mutate: func(_ *testing.T, _ *bundleFixture, second *WorkerMaterializationInput, first WorkerMaterializationInput) {
			second.WorkerControlTLSSecretRevision = first.WorkerControlTLSSecretRevision
		}},
		{name: "GPU", mutate: func(t *testing.T, fixture *bundleFixture, second *WorkerMaterializationInput, _ WorkerMaterializationInput) {
			writeWorkerMaterializationWithGPUOffset(t, fixture.directory, *second, 0)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundleFixture(t)
			addSecondWorker(t, fixture)
			first := fixture.plan.WorkerMaterializations[0]
			second := &fixture.plan.WorkerMaterializations[1]
			test.mutate(t, fixture, second, first)
			fixture.writePlan(t)
			if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
			}
		})
	}
}

func TestBuildRejectsInvalidFleetDesiredConfiguration(t *testing.T) {
	fixture := newBundleFixture(t)
	path := filepath.Join(fixture.directory, "render-fleet-controller.yaml")
	content := strings.Replace(
		string(readTestFile(t, path)),
		"    kind: FleetDesiredRevisions\n",
		"    kind: FleetDesiredRevisions\n    unknown: value\n",
		1,
	)
	writeTestFile(t, path, []byte(content))
	fixture.writePlan(t)
	if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
		!strings.Contains(err.Error(), "Fleet desired ConfigMap") {
		t.Fatalf("Build error = %v, want strict Fleet desired rejection", err)
	}
}

func TestBuildBoundsFleetDesiredBeforeStrictDecode(t *testing.T) {
	fixture := newBundleFixture(t)
	path := filepath.Join(fixture.directory, "render-fleet-controller.yaml")
	content := strings.Replace(
		string(readTestFile(t, path)),
		"    kind: FleetDesiredRevisions\n",
		"    kind: FleetDesiredRevisions\n    padding: ["+strings.Repeat("a,", maxYAMLNodes)+"a]\n",
		1,
	)
	writeTestFile(t, path, []byte(content))
	fixture.writePlan(t)
	if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
		!strings.Contains(err.Error(), "YAML input exceeds 100000 nodes") {
		t.Fatalf("Build error = %v, want Fleet desired YAML bound before strict decode", err)
	}
}

func TestBuildRejectsFleetDesiredMaterializationMismatch(t *testing.T) {
	tests := []struct {
		name        string
		oldValue    func(WorkerMaterializationInput) string
		replacement string
	}{
		{
			name: "NodeIdentity", oldValue: func(worker WorkerMaterializationInput) string { return worker.NodeIdentity },
			replacement: "h3-node-09",
		},
		{
			name: "WorkerPoolID", oldValue: func(worker WorkerMaterializationInput) string { return worker.WorkerPoolID },
			replacement: "20000000-0000-0000-0000-000000000009",
		},
		{
			name: "Fleet revision", oldValue: func(worker WorkerMaterializationInput) string { return worker.FleetRevision },
			replacement: strings.Repeat("c", 64),
		},
		{
			name: "runtime ConfigMap", oldValue: func(worker WorkerMaterializationInput) string { return worker.WorkerRuntimeConfigMap },
			replacement: "worker-runtime-other",
		},
		{
			name: "profiles ConfigMap", oldValue: func(worker WorkerMaterializationInput) string { return worker.RunnerProfilesConfigMap },
			replacement: "runner-profiles-other",
		},
		{
			name: "GPU roles ConfigMap", oldValue: func(worker WorkerMaterializationInput) string { return worker.RunnerGPURolesConfigMap },
			replacement: "runner-gpu-roles-other",
		},
		{
			name: "Worker TLS Secret", oldValue: func(worker WorkerMaterializationInput) string { return worker.WorkerControlTLSSecret },
			replacement: "worker-control-tls-other",
		},
		{
			name: "ExecutionProfileRevision", oldValue: func(worker WorkerMaterializationInput) string { return worker.ExecutionProfileRevisionID },
			replacement: "30000000-0000-0000-0000-000000000009",
		},
		{
			name: "InferenceBackendRevision", oldValue: func(worker WorkerMaterializationInput) string { return worker.InferenceBackendRevision },
			replacement: "h3-backend-r9",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundleFixture(t)
			worker := fixture.plan.WorkerMaterializations[0]
			path := filepath.Join(fixture.directory, "render-fleet-controller.yaml")
			content := strings.Replace(
				string(readTestFile(t, path)),
				test.oldValue(worker),
				test.replacement,
				1,
			)
			writeTestFile(t, path, []byte(content))
			fixture.writePlan(t)
			if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
				!strings.Contains(err.Error(), "Fleet desired revision does not match Worker materialization") {
				t.Fatalf("Build error = %v, want Fleet desired mismatch rejection", err)
			}
		})
	}
}

func TestBuildRejectsNonExactFleetDesiredCoverage(t *testing.T) {
	t.Run("materialization without desired placement", func(t *testing.T) {
		fixture := newBundleFixture(t)
		addSecondWorker(t, fixture)
		writeFleetDesiredRender(t, fixture.directory, fixture.plan.WorkerMaterializations[:1], fixture.images)
		fixture.writePlan(t)
		if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
			!strings.Contains(err.Error(), "Fleet desired and Worker materialization coverage is not exact") {
			t.Fatalf("Build error = %v, want missing desired coverage rejection", err)
		}
	})

	t.Run("desired placement without materialization", func(t *testing.T) {
		fixture := newBundleFixture(t)
		addSecondWorker(t, fixture)
		fixture.plan.WorkerMaterializations = fixture.plan.WorkerMaterializations[:1]
		fixture.writePlan(t)
		if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
			!strings.Contains(err.Error(), "Fleet desired and Worker materialization coverage is not exact") {
			t.Fatalf("Build error = %v, want extra desired coverage rejection", err)
		}
	})
}

func TestBuildRejectsInvalidNodeIdentity(t *testing.T) {
	for _, identity := range []string{
		"H3-node-01",
		"h3 node 01",
		"rack/h3-node-01",
		strings.Repeat("a", 254),
	} {
		t.Run(identity, func(t *testing.T) {
			fixture := newBundleFixture(t)
			worker := &fixture.plan.WorkerMaterializations[0]
			worker.NodeIdentity = identity
			worker.NodeAgentIdentity = expectedNodeAgentIdentity(worker.NodeIdentity, worker.WorkerID)
			writeWorkerMaterialization(t, fixture.directory, *worker)
			fixture.writePlan(t)
			if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("Build error = %v, want invalid NodeIdentity rejection", err)
			}
		})
	}
}

func TestBuildRejectsInvalidSecretContracts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*bundleFixture, *ExternalResource)
	}{
		{name: "key omission", mutate: func(_ *bundleFixture, secret *ExternalResource) {
			secret.RequiredKeys = secret.RequiredKeys[1:]
		}},
		{name: "key extra", mutate: func(_ *bundleFixture, secret *ExternalResource) {
			secret.RequiredKeys = append(secret.RequiredKeys, "z-extra.crt")
		}},
		{name: "key duplicate", mutate: func(_ *bundleFixture, secret *ExternalResource) {
			secret.RequiredKeys = []string{"ca.crt", "ca.crt", "tls.crt", "tls.key"}
		}},
		{name: "key wildcard", mutate: func(_ *bundleFixture, secret *ExternalResource) {
			secret.RequiredKeys = []string{"*", "ca.crt", "tls.crt", "tls.key"}
		}},
		{name: "consumer omission", mutate: func(_ *bundleFixture, secret *ExternalResource) {
			secret.Consumers = secret.Consumers[1:]
		}},
		{name: "consumer extra", mutate: func(_ *bundleFixture, secret *ExternalResource) {
			secret.Consumers = append(secret.Consumers, "WorkerMaterialization/vela-system/h3-node-99/10000000-0000-0000-0000-000000000099")
		}},
		{name: "consumer duplicate", mutate: func(_ *bundleFixture, secret *ExternalResource) {
			secret.Consumers = []string{secret.Consumers[0], secret.Consumers[0], secret.Consumers[1]}
		}},
		{name: "consumer mismatch", mutate: func(_ *bundleFixture, secret *ExternalResource) {
			secret.Consumers[0] = "ConfigMap/vela-system/wrong-consumer"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundleFixture(t)
			secret := findExternalResource(t, fixture, "vela-system", "worker-control-tls-node-1")
			test.mutate(fixture, secret)
			fixture.writePlan(t)
			if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
			}
		})
	}
	t.Run("ConfigMap carries Secret fields", func(t *testing.T) {
		fixture := newBundleFixture(t)
		fixture.plan.ExternalResources = append(fixture.plan.ExternalResources, ExternalResource{
			Kind: "ConfigMap", Namespace: "vela-system", Name: "external-config-r1",
			Revision: testDigest("external config"), RequiredKeys: []string{"token"},
			Consumers: []string{"Deployment/vela-system/vela-control"},
		})
		fixture.writePlan(t)
		if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
			t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
		}
	})
}

func TestBuildRejectsUnkeyedSecretSelectors(t *testing.T) {
	tests := []struct {
		name       string
		secretName string
		selector   string
		keys       []string
	}{
		{
			name:       "whole Secret volume",
			secretName: "whole-volume-secret-r1",
			selector:   "secretProbe:\n  secret:\n    secretName: whole-volume-secret-r1\n",
			keys:       []string{"token"},
		},
		{
			name:       "whole Secret envFrom",
			secretName: "whole-env-secret-r1",
			selector:   "envProbe:\n  secretRef:\n    name: whole-env-secret-r1\n",
			keys:       []string{"token"},
		},
		{
			name:       "arbitrary declared keys",
			secretName: "arbitrary-secret-r1",
			selector:   "envProbe:\n  secretRef:\n    name: arbitrary-secret-r1\n",
			keys:       []string{"alpha", "zeta"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundleFixture(t)
			consumer := "StatefulSet/vela-system/nats"
			fixture.plan.ExternalResources = append(fixture.plan.ExternalResources, ExternalResource{
				Kind: "Secret", Namespace: "vela-system", Name: test.secretName,
				Revision: testDigest(test.secretName), RequiredKeys: test.keys, Consumers: []string{consumer},
			})
			appendToRenderedResource(t, fixture, "control-storage", "apps/v1", "StatefulSet", "nats", test.selector)
			fixture.writePlan(t)
			if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
			}
		})
	}
}

func TestBuildRequiresDockerConfigJSONForImagePullSecrets(t *testing.T) {
	build := func(t *testing.T, keys []string) error {
		t.Helper()
		fixture := newBundleFixture(t)
		fixture.plan.ExternalResources = append(fixture.plan.ExternalResources, ExternalResource{
			Kind: "Secret", Namespace: "vela-system", Name: "registry-auth-r1",
			Revision: testDigest("registry auth"), RequiredKeys: keys,
			Consumers: []string{"StatefulSet/vela-system/nats"},
		})
		selector := "imagePullSecrets:\n  - name: registry-auth-r1\n"
		appendToRenderedResource(t, fixture, "control-storage", "apps/v1", "StatefulSet", "nats", selector)
		fixture.writePlan(t)
		_, _, err := Build(fixture.planPath)
		return err
	}
	if err := build(t, []string{".dockerconfigjson"}); err != nil {
		t.Fatalf("Build with exact image pull key: %v", err)
	}
	if err := build(t, []string{"token"}); !errors.Is(err, ErrInvalidBundle) {
		t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
	}
}

func TestBuildRequiresExactKeysForKeyedSecretSelectors(t *testing.T) {
	build := func(t *testing.T, keys []string) error {
		t.Helper()
		fixture := newBundleFixture(t)
		fixture.plan.ExternalResources = append(fixture.plan.ExternalResources, ExternalResource{
			Kind: "Secret", Namespace: "vela-system", Name: "keyed-secret-r1",
			Revision: testDigest("keyed secret"), RequiredKeys: keys,
			Consumers: []string{"StatefulSet/vela-system/nats"},
		})
		selector := "envProbe:\n  secretKeyRef:\n    name: keyed-secret-r1\n    key: token\n" +
			"projectedProbe:\n  secret:\n    name: keyed-secret-r1\n    items:\n      - key: certificate\n        path: certificate\n"
		appendToRenderedResource(t, fixture, "control-storage", "apps/v1", "StatefulSet", "nats", selector)
		fixture.writePlan(t)
		_, _, err := Build(fixture.planPath)
		return err
	}
	if err := build(t, []string{"certificate", "token"}); err != nil {
		t.Fatalf("Build with exact keyed selectors: %v", err)
	}
	for _, test := range []struct {
		name string
		keys []string
	}{
		{name: "omission", keys: []string{"token"}},
		{name: "extra", keys: []string{"certificate", "token", "z-extra"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := build(t, test.keys); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
			}
		})
	}
}

func TestBuildRejectsInvalidFinalRenderIdentity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *bundleFixture)
	}{
		{name: "missing essential", mutate: func(t *testing.T, fixture *bundleFixture) {
			path := filepath.Join(fixture.directory, fixture.plan.FinalRenders[0].Ref)
			content := readTestFile(t, path)
			writeTestFile(t, path, []byte(strings.Replace(string(content), "kind: StatefulSet\nmetadata:\n  name: nats", "kind: StatefulSet\nmetadata:\n  name: nats-missing", 1)))
		}},
		{name: "swapped renders", mutate: func(_ *testing.T, fixture *bundleFixture) {
			fixture.plan.FinalRenders[0].Ref, fixture.plan.FinalRenders[1].Ref =
				fixture.plan.FinalRenders[1].Ref, fixture.plan.FinalRenders[0].Ref
		}},
		{name: "duplicate resource", mutate: func(t *testing.T, fixture *bundleFixture) {
			path := filepath.Join(fixture.directory, fixture.plan.FinalRenders[0].Ref)
			content := append(readTestFile(t, path), []byte(testObject("v1", "Service", "vela-system", "nats"))...)
			writeTestFile(t, path, content)
		}},
		{name: "extra resource", mutate: func(t *testing.T, fixture *bundleFixture) {
			path := filepath.Join(fixture.directory, fixture.plan.FinalRenders[0].Ref)
			content := append(readTestFile(t, path), []byte(testObject("v1", "Service", "vela-system", "unexpected"))...)
			writeTestFile(t, path, content)
		}},
		{name: "wrong apiVersion", mutate: func(t *testing.T, fixture *bundleFixture) {
			path := filepath.Join(fixture.directory, fixture.plan.FinalRenders[0].Ref)
			content := readTestFile(t, path)
			writeTestFile(t, path, []byte(strings.Replace(string(content),
				"apiVersion: apps/v1\nkind: StatefulSet", "apiVersion: example.com/v1\nkind: StatefulSet", 1)))
		}},
		{name: "unowned dynamic prefix", mutate: func(t *testing.T, fixture *bundleFixture) {
			path := filepath.Join(fixture.directory, fixture.plan.FinalRenders[1].Ref)
			content := readTestFile(t, path)
			writeTestFile(t, path, []byte(strings.Replace(string(content),
				"vela-fleet-desired-r1", "vela-fleet-candidate-r1", 1)))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundleFixture(t)
			test.mutate(t, fixture)
			fixture.writePlan(t)
			if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
			}
		})
	}
}

func TestBuildPlanStrictJSON(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{name: "duplicate key", mutate: func(encoded []byte) []byte {
			return []byte(strings.Replace(string(encoded), `"schema_version": 1,`, `"schema_version": 1, "schema_version": 1,`, 1))
		}},
		{name: "unknown field", mutate: func(encoded []byte) []byte {
			return []byte(strings.Replace(string(encoded), `"schema_version": 1,`, `"schema_version": 1, "unknown": true,`, 1))
		}},
		{name: "trailing data", mutate: func(encoded []byte) []byte {
			return append(encoded, []byte(` {}`)...)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundleFixture(t)
			writeTestFile(t, fixture.planPath, test.mutate(readTestFile(t, fixture.planPath)))
			if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
			}
		})
	}
}

func TestBuildPreflightsArtifactGraphLimits(t *testing.T) {
	t.Run("reference count", func(t *testing.T) {
		fixture := newBundleFixture(t)
		fixture.plan.WorkerMaterializations = make([]WorkerMaterializationInput, maxWorkerNodeCount)
		for index := range fixture.plan.WorkerMaterializations {
			fixture.plan.WorkerMaterializations[index] = WorkerMaterializationInput{
				NodeIdentity:      fmt.Sprintf("count-worker-%04d", index),
				WorkerRuntimeRef:  fmt.Sprintf("count-worker-%04d-runtime.yaml", index),
				RunnerProfilesRef: fmt.Sprintf("count-worker-%04d-profiles.yaml", index),
				RunnerGPURolesRef: fmt.Sprintf("count-worker-%04d-gpus.yaml", index),
			}
		}
		fixture.plan.OCIManifests = make([]OCIManifestInput, 513)
		for index := range fixture.plan.OCIManifests {
			fixture.plan.OCIManifests[index] = OCIManifestInput{
				Image:     fmt.Sprintf("registry.example.com/release/image-%04d@%s", index, testDigest(fmt.Sprintf("image-%04d", index))),
				Ref:       fmt.Sprintf("count-image-%04d.json", index),
				ConfigRef: fmt.Sprintf("count-image-%04d-config.json", index),
			}
		}
		fixture.writePlan(t)
		if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
			!strings.Contains(err.Error(), "exceeds 4096 entries") {
			t.Fatalf("Build error = %v, want artifact count rejection", err)
		}
	})

	t.Run("aggregate stat bytes", func(t *testing.T) {
		fixture := newBundleFixture(t)
		for _, item := range fixture.plan.Packages {
			writeSparseTestFile(t, filepath.Join(fixture.directory, item.ArtifactRef), maxPackageBytes)
		}
		for index := 0; index < 700; index++ {
			worker := fixture.plan.WorkerMaterializations[0]
			worker.NodeIdentity = fmt.Sprintf("size-worker-%02d", index)
			worker.WorkerRuntimeRef = fmt.Sprintf("size-worker-%02d-runtime.yaml", index)
			worker.RunnerProfilesRef = fmt.Sprintf("size-worker-%02d-profiles.yaml", index)
			worker.RunnerGPURolesRef = fmt.Sprintf("size-worker-%02d-gpus.yaml", index)
			fixture.plan.WorkerMaterializations = append(fixture.plan.WorkerMaterializations, worker)
			for _, reference := range []string{
				worker.WorkerRuntimeRef,
				worker.RunnerProfilesRef,
				worker.RunnerGPURolesRef,
			} {
				writeSparseTestFile(t, filepath.Join(fixture.directory, reference), maxYAMLArtifactBytes)
			}
		}
		fixture.writePlan(t)
		if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
			!strings.Contains(err.Error(), "exceeds 1073741824 bytes") {
			t.Fatalf("Build error = %v, want aggregate byte rejection", err)
		}
	})
}

func TestArtifactReaderEnforcesSharedActualReadBudget(t *testing.T) {
	directory := t.TempDir()
	writeTestFile(t, filepath.Join(directory, "metadata.yaml"), []byte("abc"))
	writeTestFile(t, filepath.Join(directory, "package.tar"), []byte("defg"))
	root, err := openRootedFS(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	newReader := func(remaining int64) *artifactReader {
		return &artifactReader{
			root: root,
			normalized: map[string]string{
				"metadata.yaml": "metadata.yaml",
				"package.tar":   "package.tar",
			},
			remaining: remaining,
		}
	}

	t.Run("buffered read", func(t *testing.T) {
		reader := newReader(2)
		if _, _, err := reader.artifactFor("metadata.yaml", "application/yaml", maxMetadataBytes); err == nil ||
			!strings.Contains(err.Error(), "actual bytes") {
			t.Fatalf("buffered read error = %v, want shared budget rejection", err)
		}
		if reader.remaining != 2 {
			t.Fatalf("remaining budget after rejected buffered read = %d, want 2", reader.remaining)
		}
	})

	t.Run("streamed read after buffered read", func(t *testing.T) {
		reader := newReader(6)
		if _, _, err := reader.artifactFor("metadata.yaml", "application/yaml", maxMetadataBytes); err != nil {
			t.Fatalf("consume buffered artifact: %v", err)
		}
		if reader.remaining != 3 {
			t.Fatalf("remaining budget after buffered read = %d, want 3", reader.remaining)
		}
		if _, err := reader.digestArtifact("package.tar", "application/octet-stream", maxPackageBytes); err == nil ||
			!strings.Contains(err.Error(), "actual bytes") {
			t.Fatalf("streamed read error = %v, want shared budget rejection", err)
		}
		if reader.remaining != 3 {
			t.Fatalf("remaining budget after rejected streamed read = %d, want 3", reader.remaining)
		}

		reader = newReader(4)
		artifact, err := reader.digestArtifact("package.tar", "application/octet-stream", maxPackageBytes)
		if err != nil || artifact.Digest != testContentDigest([]byte("defg")) || artifact.SizeBytes != 4 ||
			reader.remaining != 0 {
			t.Fatalf("exact streamed budget artifact=%#v remaining=%d error=%v", artifact, reader.remaining, err)
		}
	})
}

func TestBuildRejectsUnboundedYAMLStructures(t *testing.T) {
	tests := []struct {
		name    string
		content func() []byte
	}{
		{
			name: "documents",
			content: func() []byte {
				return []byte(strings.Repeat("apiVersion: v1\nkind: ConfigMap\n---\n", maxYAMLDocuments+1))
			},
		},
		{
			name: "nodes",
			content: func() []byte {
				return []byte("[" + strings.Repeat("a,", maxYAMLNodes) + "a]\n")
			},
		},
		{
			name: "depth",
			content: func() []byte {
				var encoded strings.Builder
				for depth := 0; depth < maxYAMLDepth+1; depth++ {
					_, _ = fmt.Fprintf(&encoded, "%slevel-%02d:\n", strings.Repeat("  ", depth), depth)
				}
				encoded.WriteString(strings.Repeat("  ", maxYAMLDepth+1) + "value: bounded\n")
				return []byte(encoded.String())
			},
		},
		{
			name: "alias",
			content: func() []byte {
				return []byte("base: &base\n  value: bounded\ncopy: *base\n")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundleFixture(t)
			writeTestFile(
				t,
				filepath.Join(fixture.directory, fixture.plan.FinalRenders[0].Ref),
				test.content(),
			)
			fixture.writePlan(t)
			if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
				!strings.Contains(err.Error(), "YAML input exceeds") {
				t.Fatalf("Build error = %v, want bounded YAML rejection", err)
			}
		})
	}
}

func TestBuildRejectsOversizedYAMLArtifact(t *testing.T) {
	fixture := newBundleFixture(t)
	writeTestFile(
		t,
		filepath.Join(fixture.directory, fixture.plan.FinalRenders[0].Ref),
		bytes.Repeat([]byte("x"), maxYAMLArtifactBytes+1),
	)
	fixture.writePlan(t)
	if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
		!strings.Contains(err.Error(), "1..262144 bytes") {
		t.Fatalf("Build error = %v, want YAML artifact size rejection", err)
	}
}

func TestBuildRejectsAggregateEmbeddedYAMLDocuments(t *testing.T) {
	fixture := newBundleFixture(t)
	path := filepath.Join(fixture.directory, fixture.plan.FinalRenders[0].Ref)
	content := string(readTestFile(t, path))
	identity := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: nats-config\n  namespace: vela-system\n"
	var data strings.Builder
	data.WriteString("data:\n")
	for index := 0; index <= maxYAMLGraphDocuments; index++ {
		_, _ = fmt.Fprintf(&data, "  item-%04d.yaml: x\n", index)
	}
	writeTestFile(t, path, []byte(strings.Replace(content, identity, identity+data.String(), 1)))
	fixture.writePlan(t)
	if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
		!strings.Contains(err.Error(), "YAML graph exceeds 4096 documents") {
		t.Fatalf("Build error = %v, want aggregate YAML document rejection", err)
	}
}

func TestBuildRejectsAggregateYAMLNodes(t *testing.T) {
	fixture := newBundleFixture(t)
	padding := "padding: [" + strings.Repeat("a,", 81_000) + "a]\n---\n"
	for _, render := range fixture.plan.FinalRenders {
		path := filepath.Join(fixture.directory, render.Ref)
		content := string(readTestFile(t, path))
		writeTestFile(t, path, []byte(strings.Replace(content, "---\n", padding, 1)))
	}
	fixture.writePlan(t)
	if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
		!strings.Contains(err.Error(), "YAML graph exceeds 400000 nodes") {
		t.Fatalf("Build error = %v, want aggregate YAML node rejection", err)
	}
}

func TestBuildRejectsDuplicateProfileTypedKey(t *testing.T) {
	fixture := newBundleFixture(t)
	worker := fixture.plan.WorkerMaterializations[0]
	profile := map[string]any{
		"model_revision_id":             worker.ModelRevisionID,
		"generation_preset_revision_id": "50000000-0000-0000-0000-000000000001",
		"execution_profile_revision_id": worker.ExecutionProfileRevisionID,
		"output_spec_id":                "60000000-0000-0000-0000-000000000001",
	}
	profiles, err := json.Marshal(map[string]any{
		"schema_version":   1,
		"backend_revision": worker.InferenceBackendRevision,
		"profiles":         []any{profile, profile},
	})
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(
		t,
		filepath.Join(fixture.directory, worker.RunnerProfilesRef),
		[]byte(configMapWithJSON(worker.Namespace, worker.RunnerProfilesConfigMap, "profiles.json", string(profiles))),
	)
	fixture.writePlan(t)
	if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
		!strings.Contains(err.Error(), "duplicate entry") {
		t.Fatalf("Build error = %v, want duplicate profile rejection", err)
	}
}

func TestBuildRejectsInvalidArtifactGraph(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *bundleFixture)
	}{
		{
			name: "missing fixed render",
			mutate: func(t *testing.T, fixture *bundleFixture) {
				fixture.plan.FinalRenders = fixture.plan.FinalRenders[1:]
			},
		},
		{
			name: "template value",
			mutate: func(t *testing.T, fixture *bundleFixture) {
				path := filepath.Join(fixture.directory, fixture.plan.FinalRenders[0].Ref)
				content := readTestFile(t, path)
				writeTestFile(t, path, []byte(strings.Replace(string(content), "shared-secret-r1", "shared-secret-placeholder", 1)))
			},
		},
		{
			name: "unpinned image",
			mutate: func(t *testing.T, fixture *bundleFixture) {
				path := filepath.Join(fixture.directory, fixture.plan.FinalRenders[0].Ref)
				content := readTestFile(t, path)
				writeTestFile(t, path, []byte(strings.Replace(string(content), fixture.images[0], "ghcr.io/vivym/vela-control:latest", 1)))
			},
		},
		{
			name: "embedded Secret",
			mutate: func(t *testing.T, fixture *bundleFixture) {
				path := filepath.Join(fixture.directory, fixture.plan.FinalRenders[0].Ref)
				writeTestFile(t, path, []byte("apiVersion: v1\nkind: Secret\nmetadata:\n  name: embedded-secret\n  namespace: vela-system\ndata:\n  token: c2VjcmV0\n"))
			},
		},
		{
			name: "Secret hidden in ConfigMap data",
			mutate: func(t *testing.T, fixture *bundleFixture) {
				path := filepath.Join(fixture.directory, fixture.plan.FinalRenders[0].Ref)
				content := readTestFile(t, path)
				identity := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: nats-config\n  namespace: vela-system\n"
				writeTestFile(t, path, []byte(strings.Replace(string(content),
					identity, identity+"data:\n  secret.yaml: |\n    apiVersion: v1\n    kind: Secret\n    metadata:\n      name: hidden\n      namespace: vela-system\n    data:\n      token: c2VjcmV0\n", 1)))
			},
		},
		{
			name: "undeclared resource",
			mutate: func(t *testing.T, fixture *bundleFixture) {
				fixture.plan.ExternalResources = fixture.plan.ExternalResources[1:]
			},
		},
		{
			name: "missing OCI descriptor",
			mutate: func(t *testing.T, fixture *bundleFixture) {
				fixture.plan.OCIManifests = fixture.plan.OCIManifests[1:]
			},
		},
		{
			name: "package digest mismatch",
			mutate: func(t *testing.T, fixture *bundleFixture) {
				path := filepath.Join(fixture.directory, fixture.plan.Packages[0].ArtifactRef)
				writeTestFile(t, path, []byte("tampered package"))
			},
		},
		{
			name: "invalid GPU role count",
			mutate: func(t *testing.T, fixture *bundleFixture) {
				path := filepath.Join(fixture.directory, fixture.plan.WorkerMaterializations[0].RunnerGPURolesRef)
				content := readTestFile(t, path)
				writeTestFile(t, path, []byte(strings.Replace(string(content),
					`,"GPU-00000000-0000-0000-0000-000000000008"]`, `]`, 1)))
			},
		},
		{
			name: "zero Fleet revision",
			mutate: func(t *testing.T, fixture *bundleFixture) {
				fixture.plan.WorkerMaterializations[0].FleetRevision = strings.Repeat("0", 64)
			},
		},
		{
			name: "TLS revision mismatch",
			mutate: func(t *testing.T, fixture *bundleFixture) {
				for index := range fixture.plan.ExternalResources {
					if fixture.plan.ExternalResources[index].Name == "worker-control-tls-node-1" {
						fixture.plan.ExternalResources[index].Revision = testDigest("wrong tls material")
					}
				}
			},
		},
		{
			name: "shared artifact reference",
			mutate: func(t *testing.T, fixture *bundleFixture) {
				fixture.plan.Packages[1].ContractRef = fixture.plan.Packages[0].ContractRef
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundleFixture(t)
			test.mutate(t, fixture)
			fixture.writePlan(t)
			if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
			}
		})
	}
}

func TestBuildRejectsInvalidOCIConfigBinding(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *bundleFixture)
	}{
		{name: "missing config", mutate: func(_ *testing.T, fixture *bundleFixture) {
			fixture.plan.OCIManifests[0].ConfigRef = "missing-config.json"
		}},
		{name: "Docker schema 2 manifest", mutate: func(t *testing.T, fixture *bundleFixture) {
			input := &fixture.plan.OCIManifests[0]
			rewriteOCIManifest(t, fixture, input, func(manifest map[string]any) {
				manifest["mediaType"] = "application/vnd.docker.distribution.manifest.v2+json"
			})
		}},
		{name: "ARM config", mutate: func(t *testing.T, fixture *bundleFixture) {
			input := &fixture.plan.OCIManifests[0]
			encoded := []byte(strings.Replace(string(readTestFile(t, filepath.Join(fixture.directory, input.ConfigRef))), `"amd64"`, `"arm64"`, 1))
			rewriteOCIInput(t, fixture, input, encoded, nil)
		}},
		{name: "trailing config", mutate: func(t *testing.T, fixture *bundleFixture) {
			input := &fixture.plan.OCIManifests[0]
			encoded := append(readTestFile(t, filepath.Join(fixture.directory, input.ConfigRef)), []byte(` {}`)...)
			rewriteOCIInput(t, fixture, input, encoded, nil)
		}},
		{name: "unknown config field", mutate: func(t *testing.T, fixture *bundleFixture) {
			input := &fixture.plan.OCIManifests[0]
			encoded := []byte(strings.Replace(string(readTestFile(t, filepath.Join(fixture.directory, input.ConfigRef))),
				`{"architecture"`, `{"unknown":true,"architecture"`, 1))
			rewriteOCIInput(t, fixture, input, encoded, nil)
		}},
		{name: "wrong config digest", mutate: func(t *testing.T, fixture *bundleFixture) {
			input := &fixture.plan.OCIManifests[0]
			rewriteOCIInput(t, fixture, input, readTestFile(t, filepath.Join(fixture.directory, input.ConfigRef)), func(config map[string]any) {
				config["digest"] = testDigest("wrong config")
			})
		}},
		{name: "wrong config size", mutate: func(t *testing.T, fixture *bundleFixture) {
			input := &fixture.plan.OCIManifests[0]
			rewriteOCIInput(t, fixture, input, readTestFile(t, filepath.Join(fixture.directory, input.ConfigRef)), func(config map[string]any) {
				config["size"] = float64(1)
			})
		}},
		{name: "wrong config media type", mutate: func(t *testing.T, fixture *bundleFixture) {
			input := &fixture.plan.OCIManifests[0]
			rewriteOCIInput(t, fixture, input, readTestFile(t, filepath.Join(fixture.directory, input.ConfigRef)), func(config map[string]any) {
				config["mediaType"] = "application/octet-stream"
			})
		}},
		{name: "descriptor URLs", mutate: func(t *testing.T, fixture *bundleFixture) {
			input := &fixture.plan.OCIManifests[0]
			rewriteOCIManifest(t, fixture, input, func(manifest map[string]any) {
				manifest["config"].(map[string]any)["urls"] = []any{"https://registry.example.com/config"}
			})
		}},
		{name: "embedded descriptor data", mutate: func(t *testing.T, fixture *bundleFixture) {
			input := &fixture.plan.OCIManifests[0]
			rewriteOCIManifest(t, fixture, input, func(manifest map[string]any) {
				manifest["config"].(map[string]any)["data"] = "eA=="
			})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundleFixture(t)
			test.mutate(t, fixture)
			fixture.writePlan(t)
			if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
			}
		})
	}
}

func TestBuildRejectsOptionalResourceReferences(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(string, string) string
	}{
		{
			name: "secretKeyRef",
			mutate: func(content, optional string) string {
				needle := "          image: "
				index := strings.Index(content, needle)
				lineEnd := index + strings.Index(content[index:], "\n") + 1
				return content[:lineEnd] + "          env:\n" +
					"            - name: TOKEN\n" +
					"              valueFrom:\n" +
					"                secretKeyRef:\n" +
					"                  name: shared-secret-r1\n" +
					"                  key: token\n" +
					"                  optional: " + optional + "\n" + content[lineEnd:]
			},
		},
		{
			name: "Secret volume",
			mutate: func(content, optional string) string {
				return strings.Replace(content, "            secretName: shared-secret-r1\n",
					"            secretName: shared-secret-r1\n            optional: "+optional+"\n", 1)
			},
		},
		{
			name: "envFrom secretRef",
			mutate: func(content, optional string) string {
				needle := "          image: "
				index := strings.Index(content, needle)
				lineEnd := index + strings.Index(content[index:], "\n") + 1
				return content[:lineEnd] + "          envFrom:\n" +
					"            - secretRef:\n" +
					"                name: shared-secret-r1\n" +
					"                optional: " + optional + "\n" + content[lineEnd:]
			},
		},
		{
			name: "ConfigMap key selector",
			mutate: func(content, optional string) string {
				needle := "          image: "
				index := strings.Index(content, needle)
				lineEnd := index + strings.Index(content[index:], "\n") + 1
				return content[:lineEnd] + "          env:\n" +
					"            - name: CONFIG\n" +
					"              valueFrom:\n" +
					"                configMapKeyRef:\n" +
					"                  name: nats-config\n" +
					"                  key: config\n" +
					"                  optional: " + optional + "\n" + content[lineEnd:]
			},
		},
	}
	for _, test := range tests {
		for _, optional := range []string{"true", `"false"`} {
			t.Run(test.name+"/"+optional, func(t *testing.T) {
				fixture := newBundleFixture(t)
				path := filepath.Join(fixture.directory, fixture.plan.FinalRenders[0].Ref)
				content := test.mutate(string(readTestFile(t, path)), optional)
				writeTestFile(t, path, []byte(content))
				fixture.writePlan(t)
				if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
					!strings.Contains(err.Error(), "optional must be absent or false") {
					t.Fatalf("Build error = %v, want optional reference rejection", err)
				}
			})
		}
	}
}

func TestBuildAllowsFalseOptionalResourceReference(t *testing.T) {
	fixture := newBundleFixture(t)
	path := filepath.Join(fixture.directory, fixture.plan.FinalRenders[0].Ref)
	content := strings.Replace(
		string(readTestFile(t, path)),
		"            secretName: shared-secret-r1\n",
		"            secretName: shared-secret-r1\n            optional: false\n",
		1,
	)
	writeTestFile(t, path, []byte(content))
	fixture.writePlan(t)
	if _, _, err := Build(fixture.planPath); err != nil {
		t.Fatalf("Build with optional false: %v", err)
	}
}

func TestBuildRejectsEscapesAndSymlinks(t *testing.T) {
	for _, reference := range []string{"../outside.service", "./node-agent.service", "sub/../node-agent.service", `sub\node-agent.service`} {
		t.Run("path "+reference, func(t *testing.T) {
			fixture := newBundleFixture(t)
			fixture.plan.NodeAgentUnit.Ref = reference
			fixture.writePlan(t)
			if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
			}
		})
	}
	t.Run("symlink", func(t *testing.T) {
		fixture := newBundleFixture(t)
		target := filepath.Join(fixture.directory, fixture.plan.NodeAgentUnit.Ref)
		link := filepath.Join(fixture.directory, "node-agent-link.service")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		fixture.plan.NodeAgentUnit.Ref = filepath.Base(link)
		fixture.writePlan(t)
		if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
			t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
		}
	})
}

func TestBuildRejectsNonCanonicalSystemdUnit(t *testing.T) {
	tests := []struct {
		name    string
		oldText string
		newText string
	}{
		{name: "commented expected command", oldText: "ExecStart=/usr/local/bin/vela-node-agent", newText: "# ExecStart=/usr/local/bin/vela-node-agent\nExecStart=/bin/false"},
		{name: "command suffix", oldText: "ExecStart=/usr/local/bin/vela-node-agent", newText: "ExecStart=/usr/local/bin/vela-node-agent-other"},
		{name: "wrong command", oldText: "ExecStart=/usr/local/bin/vela-node-agent", newText: "ExecStart=/bin/false"},
		{name: "pre command", oldText: "ExecStart=/usr/local/bin/vela-node-agent", newText: "ExecStartPre=/bin/true\nExecStart=/usr/local/bin/vela-node-agent"},
		{name: "post command", oldText: "ExecStart=/usr/local/bin/vela-node-agent", newText: "ExecStart=/usr/local/bin/vela-node-agent\nExecStartPost=/bin/true"},
		{name: "duplicate", oldText: "ExecStart=/usr/local/bin/vela-node-agent", newText: "ExecStart=/usr/local/bin/vela-node-agent\nExecStart=/usr/local/bin/vela-node-agent"},
		{name: "wrong section", oldText: "[Service]\nType=simple", newText: "UMask=0077\n[Service]\nType=simple"},
		{name: "weakened hardening", oldText: "ProtectHome=true", newText: "ProtectHome=false"},
		{name: "continuation", oldText: "After=network-online.target", newText: "After=network-online.target\\"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundleFixture(t)
			path := filepath.Join(fixture.directory, fixture.plan.NodeAgentUnit.Ref)
			content := strings.Replace(string(readTestFile(t, path)), test.oldText, test.newText, 1)
			if test.name == "wrong section" {
				content = strings.Replace(content, "RestartSec=5s\nUMask=0077\n", "RestartSec=5s\n", 1)
			}
			writeTestFile(t, path, []byte(content))
			if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
			}
		})
	}
}

func TestLoadRejectsTamperAndNonCanonicalJSON(t *testing.T) {
	t.Run("referenced artifact tamper", func(t *testing.T) {
		fixture := newBundleFixture(t)
		_, encoded, err := Build(fixture.planPath)
		if err != nil {
			t.Fatal(err)
		}
		bundlePath := filepath.Join(fixture.directory, "release-bundle.json")
		writeTestFile(t, bundlePath, encoded)
		writeTestFile(t, filepath.Join(fixture.directory, fixture.plan.Packages[1].ArtifactRef), []byte("tampered"))
		if _, err := Load(bundlePath); !errors.Is(err, ErrInvalidBundle) {
			t.Fatalf("Load error = %v, want ErrInvalidBundle", err)
		}
	})
	t.Run("duplicate key", func(t *testing.T) {
		directory := t.TempDir()
		path := filepath.Join(directory, "bundle.json")
		writeTestFile(t, path, []byte(`{"schema_version":1,"schema_version":1}`))
		if _, err := Load(path); !errors.Is(err, ErrInvalidBundle) {
			t.Fatalf("Load error = %v, want ErrInvalidBundle", err)
		}
	})
	t.Run("unknown field", func(t *testing.T) {
		fixture := newBundleFixture(t)
		_, encoded, err := Build(fixture.planPath)
		if err != nil {
			t.Fatal(err)
		}
		encoded = []byte(strings.Replace(string(encoded), `"schema_version": 1,`, `"schema_version": 1, "unknown": true,`, 1))
		path := filepath.Join(fixture.directory, "bundle.json")
		writeTestFile(t, path, encoded)
		if _, err := Load(path); !errors.Is(err, ErrInvalidBundle) {
			t.Fatalf("Load error = %v, want ErrInvalidBundle", err)
		}
	})
	t.Run("derived digest tamper", func(t *testing.T) {
		fixture := newBundleFixture(t)
		bundle, _, err := Build(fixture.planPath)
		if err != nil {
			t.Fatal(err)
		}
		bundle.ConfigurationRevision = testDigest("tampered configuration")
		encoded, err := json.Marshal(bundle)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(fixture.directory, "bundle.json")
		writeTestFile(t, path, encoded)
		if _, err := Load(path); !errors.Is(err, ErrInvalidBundle) {
			t.Fatalf("Load error = %v, want ErrInvalidBundle", err)
		}
	})
	t.Run("trailing data", func(t *testing.T) {
		fixture := newBundleFixture(t)
		_, encoded, err := Build(fixture.planPath)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(fixture.directory, "bundle.json")
		writeTestFile(t, path, append(encoded, []byte(` {}`)...))
		if _, err := Load(path); !errors.Is(err, ErrInvalidBundle) {
			t.Fatalf("Load error = %v, want ErrInvalidBundle", err)
		}
	})
}

func TestLoadWithinResolvesArtifactsFromBundleDirectory(t *testing.T) {
	fixture := newBundleFixture(t)
	_, encoded, err := Build(fixture.planPath)
	if err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(fixture.directory, "nested")
	if err := os.MkdirAll(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, artifact := range collectArtifactReferences(fixture.plan) {
		destination := filepath.Join(nested, filepath.FromSlash(artifact.reference))
		if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(filepath.Join(fixture.directory, filepath.FromSlash(artifact.reference)), destination); err != nil {
			t.Fatal(err)
		}
	}
	writeTestFile(t, filepath.Join(nested, "release-bundle.json"), encoded)
	loaded, err := LoadWithin(fixture.directory, "nested/release-bundle.json")
	if err != nil || !validDigest(loaded.ReleaseDigest) {
		t.Fatalf("LoadWithin nested bundle = %#v error=%v", loaded, err)
	}
}

func TestLoadWithinRejectsSymlinkedBundleDirectory(t *testing.T) {
	fixture := newBundleFixture(t)
	_, encoded, err := Build(fixture.planPath)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(fixture.directory, "release-bundle.json"), encoded)
	root := t.TempDir()
	if err := os.Symlink(fixture.directory, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadWithin(root, "linked/release-bundle.json"); !errors.Is(err, ErrInvalidBundle) ||
		!strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("LoadWithin symlink error = %v, want no-symlink rejection", err)
	}
}

type bundleFixture struct {
	directory string
	planPath  string
	plan      BuildPlan
	images    []string
}

func findExternalResource(t *testing.T, fixture *bundleFixture, namespace, name string) *ExternalResource {
	t.Helper()
	for index := range fixture.plan.ExternalResources {
		resource := &fixture.plan.ExternalResources[index]
		if resource.Namespace == namespace && resource.Name == name {
			return resource
		}
	}
	t.Fatalf("external resource %s/%s not found", namespace, name)
	return nil
}

func appendToRenderedResource(
	t *testing.T,
	fixture *bundleFixture,
	renderName,
	apiVersion,
	kind,
	resourceName,
	fragment string,
) {
	t.Helper()
	for _, render := range fixture.plan.FinalRenders {
		if render.Name == renderName {
			path := filepath.Join(fixture.directory, render.Ref)
			content := string(readTestFile(t, path))
			identity := "apiVersion: " + apiVersion + "\nkind: " + kind + "\nmetadata:\n  name: " + resourceName + "\n"
			start := strings.Index(content, identity)
			if start < 0 {
				t.Fatalf("resource %s/%s/%s not found", apiVersion, kind, resourceName)
			}
			endOffset := strings.Index(content[start:], "---\n")
			if endOffset < 0 {
				t.Fatalf("resource %s/%s/%s has no document end", apiVersion, kind, resourceName)
			}
			end := start + endOffset
			writeTestFile(t, path, []byte(content[:end]+fragment+content[end:]))
			return
		}
	}
	t.Fatalf("final render %s not found", renderName)
}

func newBundleFixture(t *testing.T) *bundleFixture {
	t.Helper()
	directory := t.TempDir()
	manifestOne := testOCIManifest(t, directory, "control")
	manifestTwo := testOCIManifest(t, directory, "worker")
	images := []string{
		"ghcr.io/vivym/vela-control@" + manifestOne.digest,
		"ghcr.io/vivym/vela-worker-agent@" + manifestTwo.digest,
	}
	for index, name := range fixedRenderNames {
		render := testFinalRender(name, images[index%len(images)], images)
		writeTestFile(t, filepath.Join(directory, "render-"+name+".yaml"), []byte(render))
	}

	writeTestFile(t, filepath.Join(directory, "node-agent.service"), []byte(`[Unit]
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
	packageInputs := make([]PackageInput, 0, 2)
	for _, name := range fixedPackageNames {
		artifactName := name + ".tar"
		artifactContent := []byte("production package for " + name)
		writeTestFile(t, filepath.Join(directory, artifactName), artifactContent)
		entrypoint := "/opt/vela/bin/vela-h3-runner"
		if name == "node-agent" {
			entrypoint = "/usr/local/bin/vela-node-agent"
		}
		contract := PackageContract{
			SchemaVersion: 1, Name: "vela-" + name, OS: "linux", Architecture: "amd64",
			Revision: "release-r1", Entrypoint: entrypoint,
			ArtifactDigest: testContentDigest(artifactContent), ArtifactSizeBytes: int64(len(artifactContent)),
		}
		writeTestJSON(t, filepath.Join(directory, name+"-contract.json"), contract)
		packageInputs = append(packageInputs, PackageInput{
			Name: name, ContractRef: name + "-contract.json", ArtifactRef: artifactName,
		})
	}

	worker := WorkerMaterializationInput{
		NodeIdentity: "h3-node-01", Namespace: "vela-system",
		WorkerID: "10000000-0000-0000-0000-000000000001", WorkerEpoch: 7,
		WorkerPoolID: "20000000-0000-0000-0000-000000000001", FleetRevision: strings.Repeat("a", 64),
		WorkerRuntimeConfigMap: "worker-runtime-node-1", WorkerRuntimeRef: "worker-runtime.yaml",
		RunnerProfilesConfigMap: "runner-profiles-node-1", RunnerProfilesRef: "runner-profiles.yaml",
		RunnerGPURolesConfigMap: "runner-gpu-roles-node-1", RunnerGPURolesRef: "runner-gpu-roles.yaml",
		WorkerControlTLSSecret: "worker-control-tls-node-1", WorkerControlTLSSecretRevision: testDigest("worker tls"),
		ExecutionProfileRevisionID: "30000000-0000-0000-0000-000000000001",
		InferenceBackendRevision:   "h3-backend-r1", ModelRevisionID: "40000000-0000-0000-0000-000000000001",
	}
	worker.NodeAgentIdentity = expectedNodeAgentIdentity(worker.NodeIdentity, worker.WorkerID)
	writeWorkerMaterialization(t, directory, worker)
	writeFleetDesiredRender(t, directory, []WorkerMaterializationInput{worker}, images)

	plan := BuildPlan{
		SchemaVersion:          1,
		NodeAgentUnit:          ArtifactInput{Name: "node-agent-systemd-unit", Ref: "node-agent.service"},
		Packages:               packageInputs,
		WorkerMaterializations: []WorkerMaterializationInput{worker},
		ExternalResources: []ExternalResource{
			{
				Kind: "Secret", Namespace: "vela-system", Name: "artifact-store-ca-r1", Revision: testDigest("artifact ca"),
				RequiredKeys: []string{"ca.crt"}, Consumers: []string{
					"ConfigMap/vela-system/vela-fleet-desired-r1",
					"DaemonSet/vela-system/vela-h3-worker",
				},
			},
			{
				Kind: "Secret", Namespace: "vela-observability", Name: "shared-secret-r1", Revision: testDigest("observability shared"),
				RequiredKeys: []string{"token"}, Consumers: []string{"PodMonitor/vela-observability/vela-control"},
			},
			{
				Kind: "Secret", Namespace: "vela-system", Name: "shared-secret-r1", Revision: testDigest("shared"),
				RequiredKeys: []string{"token"}, Consumers: []string{
					"DaemonSet/vela-system/vela-h3-worker",
					"Deployment/vela-system/vela-control",
					"Deployment/vela-system/vela-fleet-controller",
					"StatefulSet/vela-system/nats",
				},
			},
			{
				Kind: "Secret", Namespace: "vela-system", Name: "worker-control-tls-node-1", Revision: worker.WorkerControlTLSSecretRevision,
				RequiredKeys: []string{"ca.crt", "tls.crt", "tls.key"}, Consumers: []string{
					"ConfigMap/vela-system/vela-fleet-desired-r1",
					"DaemonSet/vela-system/vela-h3-worker",
					workerConsumerIdentity(worker),
				},
			},
		},
		OCIManifests: []OCIManifestInput{
			{Image: images[0], Ref: manifestOne.ref, ConfigRef: manifestOne.configRef},
			{Image: images[1], Ref: manifestTwo.ref, ConfigRef: manifestTwo.configRef},
		},
	}
	for _, name := range fixedRenderNames {
		plan.FinalRenders = append(plan.FinalRenders, ArtifactInput{Name: name, Ref: "render-" + name + ".yaml"})
	}
	fixture := &bundleFixture{directory: directory, planPath: filepath.Join(directory, "bundle-plan.json"), plan: plan, images: images}
	fixture.writePlan(t)
	return fixture
}

func addSecondWorker(t *testing.T, fixture *bundleFixture) {
	t.Helper()
	second := WorkerMaterializationInput{
		NodeIdentity: "h3-node-02", Namespace: "vela-system",
		WorkerID: "10000000-0000-0000-0000-000000000002", WorkerEpoch: 8,
		WorkerPoolID: "20000000-0000-0000-0000-000000000001", FleetRevision: strings.Repeat("a", 64),
		WorkerRuntimeConfigMap: "worker-runtime-node-2", WorkerRuntimeRef: "worker-runtime-node-2.yaml",
		RunnerProfilesConfigMap: "runner-profiles-node-2", RunnerProfilesRef: "runner-profiles-node-2.yaml",
		RunnerGPURolesConfigMap: "runner-gpu-roles-node-2", RunnerGPURolesRef: "runner-gpu-roles-node-2.yaml",
		WorkerControlTLSSecret: "worker-control-tls-node-2", WorkerControlTLSSecretRevision: testDigest("worker tls node 2"),
		ExecutionProfileRevisionID: "30000000-0000-0000-0000-000000000001",
		InferenceBackendRevision:   "h3-backend-r1", ModelRevisionID: "40000000-0000-0000-0000-000000000001",
	}
	second.NodeAgentIdentity = expectedNodeAgentIdentity(second.NodeIdentity, second.WorkerID)
	writeWorkerMaterializationWithGPUOffset(t, fixture.directory, second, 8)
	fixture.plan.WorkerMaterializations = append(fixture.plan.WorkerMaterializations, second)
	writeFleetDesiredRender(t, fixture.directory, fixture.plan.WorkerMaterializations, fixture.images)
	fixture.plan.ExternalResources = append(fixture.plan.ExternalResources, ExternalResource{
		Kind: "Secret", Namespace: second.Namespace, Name: second.WorkerControlTLSSecret,
		Revision:     second.WorkerControlTLSSecretRevision,
		RequiredKeys: []string{"ca.crt", "tls.crt", "tls.key"},
		Consumers: []string{
			"ConfigMap/vela-system/vela-fleet-desired-r1",
			workerConsumerIdentity(second),
		},
	})
}

func writeFleetDesiredRender(
	t *testing.T,
	directory string,
	workers []WorkerMaterializationInput,
	images []string,
) {
	t.Helper()
	render := testFinalRender("fleet-controller", images[1], images)
	identity := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: vela-fleet-desired-r1\n  namespace: vela-system\n"
	desired := fleetDesiredConfiguration(workers, images)
	data := "data:\n  desired.yaml: |\n    " + strings.ReplaceAll(strings.TrimSuffix(desired, "\n"), "\n", "\n    ") + "\n"
	writeTestFile(
		t,
		filepath.Join(directory, "render-fleet-controller.yaml"),
		[]byte(strings.Replace(render, identity, identity+data, 1)),
	)
}

func writeFleetResidencyPlanRender(t *testing.T, fixture *bundleFixture, encoded []byte) {
	t.Helper()
	render := testFinalRender("fleet-controller", fixture.images[1], fixture.images)
	legacyIdentity := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: vela-fleet-desired-r1\n  namespace: vela-system\n"
	target := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n" +
		"  name: vela-fleet-residency-plan-rollouts-r1\n  namespace: vela-system\n" +
		"immutable: true\ndata:\n  rollouts.json: |-\n    " +
		strings.ReplaceAll(string(encoded), "\n", "\n    ") + "\n"
	if !strings.Contains(render, legacyIdentity) {
		t.Fatal("Fleet release fixture lacks legacy ConfigMap replacement point")
	}
	writeTestFile(
		t,
		filepath.Join(fixture.directory, "render-fleet-controller.yaml"),
		[]byte(strings.Replace(render, legacyIdentity, target, 1)),
	)
}

func fleetDesiredConfiguration(workers []WorkerMaterializationInput, images []string) string {
	var desired strings.Builder
	desired.WriteString("apiVersion: fleet.vela.ai/v1alpha1\nkind: FleetDesiredRevisions\nrevisions:\n")
	type desiredPool struct {
		first      WorkerMaterializationInput
		placements []WorkerMaterializationInput
	}
	pools := make([]desiredPool, 0, len(workers))
	poolIndexes := make(map[string]int, len(workers))
	for _, worker := range workers {
		key := worker.Namespace + "/" + worker.WorkerPoolID
		index, present := poolIndexes[key]
		if !present {
			index = len(pools)
			poolIndexes[key] = index
			pools = append(pools, desiredPool{first: worker})
		}
		pools[index].placements = append(pools[index].placements, worker)
	}
	for poolIndex, pool := range pools {
		worker := pool.first
		_, _ = fmt.Fprintf(&desired, `  - workerPoolID: %s
    name: h3-worker-pool-%02d
    revision: %s
    workerProfile: h3
    nodeSelector:
      vela.ai/worker-profile: h3
      vela.ai/worker-pool: launch-%02d
    initImage: %s
    workerAgentImage: %s
    runnerImage: %s
    artifactStoreTLSSecret: artifact-store-ca-r1
    executionProfileRevisionID: %s
    inferenceBackendRevision: %s
    readinessTimeout: 30m
    capacityPolicy:
      workerHighWatermarkBytes: 800
      workerLowWatermarkBytes: 400
      workerCriticalFreeBytes: 100
      poolHighWatermarkBytes: 5600
      poolLowWatermarkBytes: 2800
      observationMaxAge: 2m
    placements:
`, worker.WorkerPoolID, poolIndex+1, worker.FleetRevision, poolIndex+1,
			images[0], images[1], images[1], worker.ExecutionProfileRevisionID,
			worker.InferenceBackendRevision)
		for placementIndex, placement := range pool.placements {
			_, _ = fmt.Fprintf(&desired, `      - nodeIdentity: %s
        daemonSetName: h3-worker-pool-%02d-node-%02d
        workerRuntimeConfigMap: %s
        runnerProfilesConfigMap: %s
        runnerGPURolesConfigMap: %s
        workerControlTLSSecret: %s
`, placement.NodeIdentity, poolIndex+1, placementIndex+1,
				placement.WorkerRuntimeConfigMap, placement.RunnerProfilesConfigMap,
				placement.RunnerGPURolesConfigMap, placement.WorkerControlTLSSecret)
		}
	}
	desired.WriteString("retirements: []\n")
	return desired.String()
}

func testFinalRender(name, image string, images []string) string {
	contracts, ok := finalRenderInventory[name]
	if !ok {
		panic("unknown final render " + name)
	}
	var rendered strings.Builder
	for _, contract := range contracts {
		resourceName := contract.Name
		if resourceName == "" {
			resourceName = contract.NamePrefix + "r1"
		}
		isWorkload := contract.Kind == "StatefulSet" || contract.Kind == "Deployment" ||
			contract.Kind == "DaemonSet" || (name == "observability" && contract.Kind == "PodMonitor")
		if !isWorkload {
			rendered.WriteString(testObject(contract.APIVersion, contract.Kind, contract.Namespace, resourceName))
			continue
		}
		document := testWorkload(contract.APIVersion, contract.Kind, contract.Namespace, resourceName, image)
		if name == "worker-agent" && contract.Kind == "DaemonSet" {
			document = strings.Replace(document, "spec:\n", "spec:\n"+
				"  workerRuntimeConfigMap: worker-runtime-node-1\n"+
				"  runnerProfilesConfigMap: runner-profiles-node-1\n"+
				"  runnerGPURolesConfigMap: runner-gpu-roles-node-1\n"+
				"  workerControlTLSSecret: worker-control-tls-node-1\n"+
				"  artifactStoreTLSSecret: artifact-store-ca-r1\n"+
				"  initImage: "+images[0]+"\n"+
				"  workerAgentImage: "+images[1]+"\n"+
				"  runnerImage: "+images[1]+"\n", 1)
		}
		rendered.WriteString(document)
	}
	return rendered.String()
}

func testObject(apiVersion, kind, namespace, name string) string {
	encoded := "apiVersion: " + apiVersion + "\nkind: " + kind + "\nmetadata:\n  name: " + name + "\n"
	if namespace != "" {
		encoded += "  namespace: " + namespace + "\n"
	}
	return encoded + "---\n"
}

func testWorkload(apiVersion, kind, namespace, name, image string) string {
	return "apiVersion: " + apiVersion + "\nkind: " + kind + "\nmetadata:\n  name: " + name +
		"\n  namespace: " + namespace + `
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

type manifestFixture struct {
	ref       string
	configRef string
	digest    string
}

func testOCIManifest(t *testing.T, directory, name string) manifestFixture {
	t.Helper()
	config := map[string]any{
		"architecture": "amd64",
		"os":           "linux",
		"config":       map[string]any{"Entrypoint": []string{"/usr/local/bin/" + name}},
		"rootfs":       map[string]any{"type": "layers", "diff_ids": []string{}},
	}
	configEncoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configRef := "oci-" + name + "-config.json"
	writeTestFile(t, filepath.Join(directory, configRef), configEncoded)
	manifest := map[string]any{
		"schemaVersion": 2,
		"mediaType":     OCIManifestMediaType,
		"config": map[string]any{
			"mediaType": OCIImageConfigMediaType,
			"digest":    testContentDigest(configEncoded),
			"size":      len(configEncoded),
		},
		"layers": []any{map[string]any{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": testDigest(name + " layer"), "size": 256}},
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	ref := "oci-" + name + ".json"
	writeTestFile(t, filepath.Join(directory, ref), encoded)
	return manifestFixture{ref: ref, configRef: configRef, digest: testContentDigest(encoded)}
}

func rewriteOCIInput(
	t *testing.T,
	fixture *bundleFixture,
	input *OCIManifestInput,
	configEncoded []byte,
	mutateDescriptor func(map[string]any),
) {
	t.Helper()
	writeTestFile(t, filepath.Join(fixture.directory, input.ConfigRef), configEncoded)
	rewriteOCIManifest(t, fixture, input, func(manifest map[string]any) {
		config := manifest["config"].(map[string]any)
		config["digest"] = testContentDigest(configEncoded)
		config["size"] = float64(len(configEncoded))
		if mutateDescriptor != nil {
			mutateDescriptor(config)
		}
	})
}

func rewriteOCIManifest(
	t *testing.T,
	fixture *bundleFixture,
	input *OCIManifestInput,
	mutate func(map[string]any),
) {
	t.Helper()
	var manifest map[string]any
	if err := json.Unmarshal(readTestFile(t, filepath.Join(fixture.directory, input.Ref)), &manifest); err != nil {
		t.Fatal(err)
	}
	mutate(manifest)
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, filepath.Join(fixture.directory, input.Ref), encoded)
	oldImage := input.Image
	input.Image = oldImage[:strings.LastIndex(oldImage, "@")+1] + testContentDigest(encoded)
	for _, render := range fixture.plan.FinalRenders {
		path := filepath.Join(fixture.directory, render.Ref)
		content := strings.ReplaceAll(string(readTestFile(t, path)), oldImage, input.Image)
		writeTestFile(t, path, []byte(content))
	}
}

func writeWorkerMaterialization(t *testing.T, directory string, worker WorkerMaterializationInput) {
	t.Helper()
	writeWorkerMaterializationWithGPUOffset(t, directory, worker, 0)
}

func writeWorkerMaterializationWithGPUOffset(
	t *testing.T,
	directory string,
	worker WorkerMaterializationInput,
	gpuOffset int,
) {
	t.Helper()
	runtime := `apiVersion: v1
kind: ConfigMap
metadata:
  name: ` + worker.WorkerRuntimeConfigMap + `
  namespace: ` + worker.Namespace + `
immutable: true
data:
  artifact-store-health-url: https://artifacts.example.com/healthz
  attempt-quota-bytes: "1000000"
  control-address: control.vela-system.svc:9443
  control-server-name: control.vela-system.svc
  critical-free-bytes: "100"
  high-watermark-bytes: "800"
  low-watermark-bytes: "700"
  max-entries: "1000"
  max-entry-bytes: "1000000"
  output-cleanup-min-bytes-per-second: "100000"
  xfs-device: /dev/nvme0n1
  xfs-project-id: "1001"
`
	writeTestFile(t, filepath.Join(directory, worker.WorkerRuntimeRef), []byte(runtime))
	profiles := map[string]any{
		"schema_version": 1, "backend_revision": worker.InferenceBackendRevision,
		"profiles": []any{map[string]any{
			"model_revision_id":             worker.ModelRevisionID,
			"generation_preset_revision_id": "50000000-0000-0000-0000-000000000001",
			"execution_profile_revision_id": worker.ExecutionProfileRevisionID,
			"output_spec_id":                "60000000-0000-0000-0000-000000000001",
		}},
	}
	profilesJSON, _ := json.Marshal(profiles)
	profilesConfig := configMapWithJSON(worker.Namespace, worker.RunnerProfilesConfigMap, "profiles.json", string(profilesJSON))
	writeTestFile(t, filepath.Join(directory, worker.RunnerProfilesRef), []byte(profilesConfig))
	gpuID := func(index int) string {
		return fmt.Sprintf("GPU-00000000-0000-0000-0000-%012d", gpuOffset+index)
	}
	roles := map[string]any{
		"schema_version": 1,
		"encoder_vae":    gpuID(1),
		"dit":            []string{gpuID(2), gpuID(3), gpuID(4), gpuID(5), gpuID(6), gpuID(7), gpuID(8)},
	}
	rolesJSON, _ := json.Marshal(roles)
	rolesConfig := configMapWithJSON(worker.Namespace, worker.RunnerGPURolesConfigMap, "gpu-roles.json", string(rolesJSON))
	writeTestFile(t, filepath.Join(directory, worker.RunnerGPURolesRef), []byte(rolesConfig))
}

func configMapWithJSON(namespace, name, key, value string) string {
	return "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: " + name + "\n  namespace: " + namespace +
		"\nimmutable: true\ndata:\n  " + key + ": |-\n    " + strings.ReplaceAll(value, "\n", "\n    ") + "\n"
}

func (fixture *bundleFixture) writePlan(t *testing.T) {
	t.Helper()
	writeTestJSON(t, fixture.planPath, fixture.plan)
}

func writeTestJSON(t *testing.T, path string, value any) {
	t.Helper()
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, path, append(encoded, '\n'))
}

func writeTestFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeSparseTestFile(t *testing.T, path string, size int64) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(size); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func readTestFile(t *testing.T, path string) []byte {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return content
}

func testDigest(value string) string {
	return testContentDigest([]byte(value))
}

func testContentDigest(content []byte) string {
	digest := sha256.Sum256(content)
	return "sha256:" + hex.EncodeToString(digest[:])
}
