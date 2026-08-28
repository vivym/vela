package releasebundle

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildAndLoadCanonicalReleaseBundle(t *testing.T) {
	fixture := newBundleFixture(t)
	bundle, first, err := Build(fixture.planPath)
	if err != nil {
		t.Fatalf("build release bundle: %v", err)
	}
	if bundle.SchemaVersion != 1 || !validDigest(bundle.ReleaseDigest) ||
		!validDigest(bundle.ConfigurationRevision) ||
		bundle.ReleaseDescriptor.Config.Digest != bundle.ConfigurationRevision ||
		len(bundle.ConfigurationManifest.FinalRenders) != 5 ||
		len(bundle.ConfigurationManifest.Packages) != 2 ||
		len(bundle.ConfigurationManifest.WorkerMaterializations) != 1 || len(bundle.OCIImages) != 2 {
		t.Fatalf("built bundle = %#v", bundle)
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
				writeTestFile(t, path, []byte(strings.Replace(string(content),
					"  release: r1", "  secret.yaml: |\n    apiVersion: v1\n    kind: Secret\n    metadata:\n      name: hidden\n      namespace: vela-system\n    data:\n      token: c2VjcmV0", 1)))
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

func TestBuildRejectsEscapesAndSymlinks(t *testing.T) {
	t.Run("path escape", func(t *testing.T) {
		fixture := newBundleFixture(t)
		fixture.plan.NodeAgentUnit.Ref = "../outside.service"
		fixture.writePlan(t)
		if _, _, err := Build(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
			t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
		}
	})
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
}

type bundleFixture struct {
	directory string
	planPath  string
	plan      BuildPlan
	images    []string
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
		namespace := "vela-system"
		if name == "observability" {
			namespace = "vela-observability"
		}
		extra := ""
		if name == "worker-agent" {
			extra = `
data:
  desired.yaml: |
    workerRuntimeConfigMap: worker-runtime-node-1
    runnerProfilesConfigMap: runner-profiles-node-1
    runnerGPURolesConfigMap: runner-gpu-roles-node-1
    workerControlTLSSecret: worker-control-tls-node-1
    artifactStoreTLSSecret: artifact-store-ca-r1
    initImage: ` + images[0] + `
    workerAgentImage: ` + images[1] + `
    runnerImage: ` + images[1] + `
`
		} else {
			extra = "\ndata:\n  release: r1\n"
		}
		render := `apiVersion: v1
kind: ConfigMap
metadata:
  name: ` + name + `-config-r1
  namespace: ` + namespace + `
immutable: true` + extra + `---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ` + name + `-r1
  namespace: ` + namespace + `
spec:
  selector:
    matchLabels:
      app: ` + name + `
  template:
    metadata:
      labels:
        app: ` + name + `
    spec:
      containers:
        - name: service
          image: ` + images[index%len(images)] + `
      volumes:
        - name: credentials
          secret:
            secretName: shared-secret-r1
`
		writeTestFile(t, filepath.Join(directory, "render-"+name+".yaml"), []byte(render))
	}

	writeTestFile(t, filepath.Join(directory, "node-agent.service"), []byte(`[Unit]
Description=Vela Node Agent
[Service]
ExecStart=/usr/local/bin/vela-node-agent
UMask=0077
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

	plan := BuildPlan{
		SchemaVersion:          1,
		NodeAgentUnit:          ArtifactInput{Name: "node-agent-systemd-unit", Ref: "node-agent.service"},
		Packages:               packageInputs,
		WorkerMaterializations: []WorkerMaterializationInput{worker},
		ExternalResources: []ExternalResource{
			{Kind: "Secret", Namespace: "vela-system", Name: "artifact-store-ca-r1", Revision: testDigest("artifact ca")},
			{Kind: "Secret", Namespace: "vela-observability", Name: "shared-secret-r1", Revision: testDigest("observability shared")},
			{Kind: "Secret", Namespace: "vela-system", Name: "shared-secret-r1", Revision: testDigest("shared")},
			{Kind: "Secret", Namespace: "vela-system", Name: "worker-control-tls-node-1", Revision: worker.WorkerControlTLSSecretRevision},
		},
		OCIManifests: []OCIManifestInput{
			{Image: images[0], Ref: manifestOne.ref, Platform: Platform{OS: "linux", Architecture: "amd64"}},
			{Image: images[1], Ref: manifestTwo.ref, Platform: Platform{OS: "linux", Architecture: "amd64"}},
		},
	}
	for _, name := range fixedRenderNames {
		plan.FinalRenders = append(plan.FinalRenders, ArtifactInput{Name: name, Ref: "render-" + name + ".yaml"})
	}
	fixture := &bundleFixture{directory: directory, planPath: filepath.Join(directory, "bundle-plan.json"), plan: plan, images: images}
	fixture.writePlan(t)
	return fixture
}

type manifestFixture struct {
	ref    string
	digest string
}

func testOCIManifest(t *testing.T, directory, name string) manifestFixture {
	t.Helper()
	manifest := map[string]any{
		"schemaVersion": 2,
		"mediaType":     OCIManifestMediaType,
		"config":        map[string]any{"mediaType": "application/vnd.oci.image.config.v1+json", "digest": testDigest(name + " config"), "size": 128},
		"layers":        []any{map[string]any{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": testDigest(name + " layer"), "size": 256}},
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	ref := "oci-" + name + ".json"
	writeTestFile(t, filepath.Join(directory, ref), encoded)
	return manifestFixture{ref: ref, digest: testContentDigest(encoded)}
}

func writeWorkerMaterialization(t *testing.T, directory string, worker WorkerMaterializationInput) {
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
	roles := map[string]any{
		"schema_version": 1,
		"encoder_vae":    "GPU-00000000-0000-0000-0000-000000000001",
		"dit": []string{
			"GPU-00000000-0000-0000-0000-000000000002",
			"GPU-00000000-0000-0000-0000-000000000003",
			"GPU-00000000-0000-0000-0000-000000000004",
			"GPU-00000000-0000-0000-0000-000000000005",
			"GPU-00000000-0000-0000-0000-000000000006",
			"GPU-00000000-0000-0000-0000-000000000007",
			"GPU-00000000-0000-0000-0000-000000000008",
		},
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
