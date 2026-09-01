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
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
)

func TestBuildAndLoadCanonicalReleaseBundle(t *testing.T) {
	fixture := newBundleFixture(t)
	sourceRoot := newCleanGitSource(t)
	sourceRevision := gitHead(t, sourceRoot)
	bundle, first, err := BuildFromSource(sourceRoot, fixture.planPath)
	if err != nil {
		t.Fatalf("build release bundle: %v", err)
	}
	if bundle.SchemaVersion != 2 || bundle.ConfigurationManifest.SchemaVersion != 2 ||
		bundle.ConfigurationManifest.SourceRevision != sourceRevision ||
		!validDigest(bundle.ReleaseDigest) ||
		!validDigest(bundle.ConfigurationRevision) ||
		bundle.ReleaseDescriptor.MediaType != ReleaseDescriptorMediaType ||
		bundle.ReleaseDescriptor.Config.Digest != bundle.ConfigurationRevision ||
		len(bundle.ConfigurationManifest.FinalRenders) != 5 ||
		len(bundle.ConfigurationManifest.Packages) != 1 || len(bundle.OCIImages) != 3 {
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
	_, second, err := BuildFromSource(sourceRoot, fixture.planPath)
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

func TestBuildBindsSourceRevisionIntoCanonicalIdentities(t *testing.T) {
	fixture := newBundleFixture(t)
	firstSource := newCleanGitSource(t)
	secondSource := newCleanGitSource(t)
	writeTestFile(t, filepath.Join(secondSource, "second.txt"), []byte("second source"))
	gitRun(t, secondSource, "add", "second.txt")
	gitRun(t, secondSource, "commit", "--quiet", "-m", "second source")

	first, _, err := BuildFromSource(firstSource, fixture.planPath)
	if err != nil {
		t.Fatalf("build first source release: %v", err)
	}
	second, _, err := BuildFromSource(secondSource, fixture.planPath)
	if err != nil {
		t.Fatalf("build second source release: %v", err)
	}
	if first.ConfigurationManifest.SourceRevision != gitHead(t, firstSource) ||
		second.ConfigurationManifest.SourceRevision != gitHead(t, secondSource) {
		t.Fatalf("source revisions = %q and %q", first.ConfigurationManifest.SourceRevision, second.ConfigurationManifest.SourceRevision)
	}
	if first.ConfigurationRevision == second.ConfigurationRevision || first.ReleaseDigest == second.ReleaseDigest {
		t.Fatalf("source revision is absent from canonical identities: first=%#v second=%#v", first, second)
	}
}

func TestBuildBindsTargetResidencyPlanStageWorkerAndSecrets(t *testing.T) {
	fixture := newBundleFixture(t)
	bundle, bundleBytes, err := buildTestBundle(fixture.planPath)
	if err != nil {
		t.Fatalf("build target ResidencyPlan release bundle: %v", err)
	}
	if len(bundle.ConfigurationManifest.FinalRenders) != 5 ||
		len(bundle.ConfigurationManifest.Packages) != 1 || len(bundle.OCIImages) != 3 {
		t.Fatalf("target ResidencyPlan release bundle = %#v", bundle.ConfigurationManifest)
	}
	for _, legacy := range []string{"worker_materializations", `"worker-agent"`, `"h3-runner"`} {
		if bytes.Contains(bundleBytes, []byte(legacy)) {
			t.Fatalf("schema v2 bundle retains legacy release token %q", legacy)
		}
	}
	bundlePath := filepath.Join(fixture.directory, "release-bundle.json")
	writeTestFile(t, bundlePath, bundleBytes)
	loadedBundle, rollouts, err := LoadResidencyPlanRollouts(bundlePath)
	if err != nil || loadedBundle.ReleaseDigest != bundle.ReleaseDigest || len(rollouts) != 1 ||
		rollouts[0].ApprovedPlan.ID != uuid.MustParse("49330000-0000-0000-0000-000000000001") {
		t.Fatalf("load canonical ResidencyPlan bundle=%#v rollouts=%#v error=%v", loadedBundle, rollouts, err)
	}
}

func TestValidateModelRuntimeOCIConfigRequiresExactEntrypoint(t *testing.T) {
	image := "ghcr.io/vivym/vela-h3-stage-runtime@" + testDigest("runtime")
	validLabels := map[string]string{
		"vela.ai.h3-runtime-base":       "ghcr.io/minimax/h3-runtime-base@" + testDigest("runtime base"),
		"vela.ai.h3-encoder.sha256":     strings.TrimPrefix(testDigest("encoder"), "sha256:"),
		"vela.ai.h3-dit.sha256":         strings.TrimPrefix(testDigest("dit"), "sha256:"),
		"vela.ai.h3-vae-decoder.sha256": strings.TrimPrefix(testDigest("vae decoder"), "sha256:"),
	}
	for _, test := range []struct {
		name       string
		entrypoint []string
		mutate     func(map[string]string)
		wantError  bool
	}{
		{name: "exact", entrypoint: []string{"/usr/local/bin/vela-model-runtime"}},
		{name: "other absolute", entrypoint: []string{"/opt/vela/bin/h3-stage-runtime"}, wantError: true},
		{name: "arguments", entrypoint: []string{"/usr/local/bin/vela-model-runtime", "--serve"}, wantError: true},
		{name: "missing", wantError: true},
		{name: "relative", entrypoint: []string{"bin/h3-stage-runtime"}, wantError: true},
		{
			name: "missing DiT digest", entrypoint: []string{"/usr/local/bin/vela-model-runtime"},
			mutate:    func(labels map[string]string) { delete(labels, "vela.ai.h3-dit.sha256") },
			wantError: true,
		},
		{
			name: "tagged runtime base", entrypoint: []string{"/usr/local/bin/vela-model-runtime"},
			mutate: func(labels map[string]string) {
				labels["vela.ai.h3-runtime-base"] = "ghcr.io/minimax/h3-runtime-base:latest"
			},
			wantError: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			labels := make(map[string]string, len(validLabels))
			for name, value := range validLabels {
				labels[name] = value
			}
			if test.mutate != nil {
				test.mutate(labels)
			}
			encoded, err := json.Marshal(map[string]any{
				"architecture": "amd64", "os": "linux",
				"config": map[string]any{"Entrypoint": test.entrypoint, "Labels": labels},
				"rootfs": map[string]any{"type": "layers", "diff_ids": []string{}},
			})
			if err != nil {
				t.Fatal(err)
			}
			err = validateModelRuntimeOCIConfig(image, encoded, true)
			if (err != nil) != test.wantError {
				t.Fatalf("validateModelRuntimeOCIConfig error = %v, wantError=%t", err, test.wantError)
			}
		})
	}
}

func TestValidateModelRuntimeOCIConfigDoesNotRequireH3LabelsForOtherComponents(t *testing.T) {
	image := "ghcr.io/vivym/vela-llm-stage-runtime@" + testDigest("llm runtime")
	encoded, err := json.Marshal(map[string]any{
		"architecture": "amd64", "os": "linux",
		"config": map[string]any{
			"Entrypoint": []string{"/usr/local/bin/vela-model-runtime"},
		},
		"rootfs": map[string]any{"type": "layers", "diff_ids": []string{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := validateModelRuntimeOCIConfig(image, encoded, false); err != nil {
		t.Fatalf("validate non-H3 ModelRuntime OCI config: %v", err)
	}
}

func TestRecordH3RuntimeImagesUsesRolloutComponentContract(t *testing.T) {
	h3Image := "ghcr.io/vivym/vela-h3-stage-runtime@" + testDigest("h3")
	llmImage := "ghcr.io/vivym/vela-llm-stage-runtime@" + testDigest("llm")
	inventory := newRenderInventory()
	recordH3RuntimeImages(&inventory, []fleetcontroller.ResidencyPlanRollout{{
		WorkerBundles: []fleetcontroller.WorkerBundleActuation{
			{
				RuntimeImage: h3Image,
				WorkerInstances: []fleetcontroller.WorkerInstanceActuation{{
					ModelRuntimes: []fleetcontroller.ModelRuntimeProcess{{Component: "DIT"}},
				}},
			},
			{
				RuntimeImage: llmImage,
				WorkerInstances: []fleetcontroller.WorkerInstanceActuation{{
					ModelRuntimes: []fleetcontroller.ModelRuntimeProcess{{Component: "LLM"}},
				}},
			},
		},
	}})
	if _, exists := inventory.h3RuntimeImages[h3Image]; !exists {
		t.Fatal("H3 runtime image was not classified from the DiT component")
	}
	if _, exists := inventory.h3RuntimeImages[llmImage]; exists {
		t.Fatal("LLM runtime image was incorrectly classified as H3")
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
			secret.Consumers = append(secret.Consumers, "ConfigMap/vela-system/unexpected-consumer")
		}},
		{name: "consumer duplicate", mutate: func(_ *bundleFixture, secret *ExternalResource) {
			secret.Consumers = []string{secret.Consumers[0], secret.Consumers[0]}
		}},
		{name: "consumer mismatch", mutate: func(_ *bundleFixture, secret *ExternalResource) {
			secret.Consumers[0] = "ConfigMap/vela-system/wrong-consumer"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundleFixture(t)
			secret := findExternalResource(t, fixture, "vela-system", "stage-worker-control-r1")
			test.mutate(fixture, secret)
			fixture.writePlan(t)
			if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
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
		if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
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
			if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
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
		_, _, err := buildTestBundle(fixture.planPath)
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
		_, _, err := buildTestBundle(fixture.planPath)
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
				"vela-fleet-residency-plan-rollouts-r1", "vela-fleet-candidate-r1", 1)))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundleFixture(t)
			test.mutate(t, fixture)
			fixture.writePlan(t)
			if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
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
			return []byte(strings.Replace(string(encoded), `"schema_version": 2,`, `"schema_version": 2, "schema_version": 2,`, 1))
		}},
		{name: "unknown field", mutate: func(encoded []byte) []byte {
			return []byte(strings.Replace(string(encoded), `"schema_version": 2,`, `"schema_version": 2, "unknown": true,`, 1))
		}},
		{name: "trailing data", mutate: func(encoded []byte) []byte {
			return append(encoded, []byte(` {}`)...)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundleFixture(t)
			writeTestFile(t, fixture.planPath, test.mutate(readTestFile(t, fixture.planPath)))
			if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
			}
		})
	}
}

func TestBuildPreflightsArtifactGraphLimits(t *testing.T) {
	t.Run("reference count", func(t *testing.T) {
		fixture := newBundleFixture(t)
		fixture.plan.OCIManifests = make([]OCIManifestInput, 2045)
		for index := range fixture.plan.OCIManifests {
			fixture.plan.OCIManifests[index] = OCIManifestInput{
				Image:     fmt.Sprintf("registry.example.com/release/image-%04d@%s", index, testDigest(fmt.Sprintf("image-%04d", index))),
				Ref:       fmt.Sprintf("count-image-%04d.json", index),
				ConfigRef: fmt.Sprintf("count-image-%04d-config.json", index),
			}
		}
		fixture.writePlan(t)
		if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
			!strings.Contains(err.Error(), "exceeds 4096 entries") {
			t.Fatalf("Build error = %v, want artifact count rejection", err)
		}
	})

	t.Run("aggregate stat bytes", func(t *testing.T) {
		fixture := newBundleFixture(t)
		for _, item := range fixture.plan.Packages {
			writeSparseTestFile(t, filepath.Join(fixture.directory, item.ArtifactRef), maxPackageBytes)
		}
		fixture.plan.OCIManifests = nil
		for index := 0; index < 40; index++ {
			manifestRef := fmt.Sprintf("size-image-%02d.json", index)
			configRef := fmt.Sprintf("size-image-%02d-config.json", index)
			fixture.plan.OCIManifests = append(fixture.plan.OCIManifests, OCIManifestInput{
				Image: fmt.Sprintf("registry.example.com/release/image-%02d@%s", index, testDigest(fmt.Sprintf("size-image-%02d", index))),
				Ref:   manifestRef, ConfigRef: configRef,
			})
			writeSparseTestFile(t, filepath.Join(fixture.directory, manifestRef), maxMetadataBytes)
			writeSparseTestFile(t, filepath.Join(fixture.directory, configRef), maxMetadataBytes)
		}
		fixture.writePlan(t)
		if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
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
			if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
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
	if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
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
	if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
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
	if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
		!strings.Contains(err.Error(), "YAML graph exceeds 400000 nodes") {
		t.Fatalf("Build error = %v, want aggregate YAML node rejection", err)
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
			name: "shared artifact reference",
			mutate: func(t *testing.T, fixture *bundleFixture) {
				fixture.plan.Packages[0].ArtifactRef = fixture.plan.Packages[0].ContractRef
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundleFixture(t)
			test.mutate(t, fixture)
			fixture.writePlan(t)
			if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
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
			if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
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
				if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) ||
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
	if _, _, err := buildTestBundle(fixture.planPath); err != nil {
		t.Fatalf("Build with optional false: %v", err)
	}
}

func TestBuildRejectsEscapesAndSymlinks(t *testing.T) {
	for _, reference := range []string{"../outside.service", "./node-agent.service", "sub/../node-agent.service", `sub\node-agent.service`} {
		t.Run("path "+reference, func(t *testing.T) {
			fixture := newBundleFixture(t)
			fixture.plan.NodeAgentUnit.Ref = reference
			fixture.writePlan(t)
			if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
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
		if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
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
			if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("Build error = %v, want ErrInvalidBundle", err)
			}
		})
	}
}

func TestLoadRejectsTamperAndNonCanonicalJSON(t *testing.T) {
	t.Run("referenced artifact tamper", func(t *testing.T) {
		fixture := newBundleFixture(t)
		_, encoded, err := buildTestBundle(fixture.planPath)
		if err != nil {
			t.Fatal(err)
		}
		bundlePath := filepath.Join(fixture.directory, "release-bundle.json")
		writeTestFile(t, bundlePath, encoded)
		writeTestFile(t, filepath.Join(fixture.directory, fixture.plan.Packages[0].ArtifactRef), []byte("tampered"))
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
		_, encoded, err := buildTestBundle(fixture.planPath)
		if err != nil {
			t.Fatal(err)
		}
		encoded = []byte(strings.Replace(string(encoded), `"schema_version": 2,`, `"schema_version": 2, "unknown": true,`, 1))
		path := filepath.Join(fixture.directory, "bundle.json")
		writeTestFile(t, path, encoded)
		if _, err := Load(path); !errors.Is(err, ErrInvalidBundle) {
			t.Fatalf("Load error = %v, want ErrInvalidBundle", err)
		}
	})
	t.Run("derived digest tamper", func(t *testing.T) {
		fixture := newBundleFixture(t)
		bundle, _, err := buildTestBundle(fixture.planPath)
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
		_, encoded, err := buildTestBundle(fixture.planPath)
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
	_, encoded, err := buildTestBundle(fixture.planPath)
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
	_, encoded, err := buildTestBundle(fixture.planPath)
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

func (fixture *bundleFixture) writePlan(t *testing.T) {
	t.Helper()
	writeTestJSON(t, fixture.planPath, fixture.plan)
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
	controlManifest := testOCIManifest(t, directory, "control")
	stageManifest := testOCIManifest(t, directory, "stage-worker")
	runtimeManifest := testOCIManifest(t, directory, "h3-stage-runtime")
	images := []string{
		"ghcr.io/vivym/vela-control@" + controlManifest.digest,
		"ghcr.io/vivym/vela-stage-worker-agent@" + stageManifest.digest,
		"ghcr.io/vivym/vela-h3-stage-runtime@" + runtimeManifest.digest,
	}
	for _, name := range fixedRenderNames {
		writeTestFile(
			t,
			filepath.Join(directory, "render-"+name+".yaml"),
			[]byte(testFinalRender(name, images[0], images)),
		)
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
	artifactContent := []byte("production package for node-agent")
	writeTestFile(t, filepath.Join(directory, "node-agent.tar"), artifactContent)
	writeTestJSON(t, filepath.Join(directory, "node-agent-contract.json"), PackageContract{
		SchemaVersion: 1, Name: "vela-node-agent", OS: "linux", Architecture: "amd64",
		Revision: "release-r1", Entrypoint: "/usr/local/bin/vela-node-agent",
		ArtifactDigest: testContentDigest(artifactContent), ArtifactSizeBytes: int64(len(artifactContent)),
	})

	consumer := "ConfigMap/vela-system/vela-fleet-residency-plan-rollouts-r1"
	plan := BuildPlan{
		SchemaVersion: SchemaVersion,
		NodeAgentUnit: ArtifactInput{Name: "node-agent-systemd-unit", Ref: "node-agent.service"},
		Packages: []PackageInput{{
			Name: "node-agent", ContractRef: "node-agent-contract.json", ArtifactRef: "node-agent.tar",
		}},
		ExternalResources: []ExternalResource{
			{
				Kind: "Secret", Namespace: "vela-observability", Name: "shared-secret-r1",
				Revision: testDigest("observability shared"), RequiredKeys: []string{"token"},
				Consumers: []string{"PodMonitor/vela-observability/vela-control"},
			},
			{
				Kind: "Secret", Namespace: "vela-system", Name: "shared-secret-r1",
				Revision: testDigest("shared"), RequiredKeys: []string{"token"},
				Consumers: []string{
					"Deployment/vela-system/vela-control",
					"Deployment/vela-system/vela-fleet-controller",
					"StatefulSet/vela-system/nats",
				},
			},
			{
				Kind: "Secret", Namespace: "vela-system", Name: "stage-worker-control-r1",
				Revision: testDigest("stage control"), RequiredKeys: []string{"ca.crt", "tls.crt", "tls.key"},
				Consumers: []string{consumer},
			},
			{
				Kind: "Secret", Namespace: "vela-system", Name: "stage-worker-authority-r1",
				Revision: testDigest("stage authority"), RequiredKeys: []string{"keyring.json"},
				Consumers: []string{consumer},
			},
			{
				Kind: "Secret", Namespace: "vela-system", Name: "stage-worker-artifact-credentials-r1",
				Revision:     testDigest("stage artifact credentials"),
				RequiredKeys: []string{"access-key-id", "secret-access-key"}, Consumers: []string{consumer},
			},
			{
				Kind: "Secret", Namespace: "vela-system", Name: "stage-worker-artifact-ca-r1",
				Revision: testDigest("stage artifact ca"), RequiredKeys: []string{"ca.crt"},
				Consumers: []string{consumer},
			},
		},
		OCIManifests: []OCIManifestInput{
			{Image: images[0], Ref: controlManifest.ref, ConfigRef: controlManifest.configRef},
			{Image: images[1], Ref: stageManifest.ref, ConfigRef: stageManifest.configRef},
			{Image: images[2], Ref: runtimeManifest.ref, ConfigRef: runtimeManifest.configRef},
		},
	}
	for _, name := range fixedRenderNames {
		plan.FinalRenders = append(plan.FinalRenders, ArtifactInput{Name: name, Ref: "render-" + name + ".yaml"})
	}
	fixture := &bundleFixture{
		directory: directory, planPath: filepath.Join(directory, "bundle-plan.json"), plan: plan, images: images,
	}
	rollout := testResidencyPlanRollout(t, images)
	encoded, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"rollouts":       []fleetcontroller.ResidencyPlanRollout{rollout},
	})
	if err != nil {
		t.Fatal(err)
	}
	writeFleetResidencyPlanRender(t, fixture, encoded)
	fixture.writePlan(t)
	return fixture
}

func testResidencyPlanRollout(t *testing.T, images []string) fleetcontroller.ResidencyPlanRollout {
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
				StageProfileRevisionID: uuid.MustParse("49330000-0000-0000-0000-000000000008"),
				ModelRuntimeEpochFloor: 1,
				Component:              "DIT", ModelComponentRevision: "h3-dit-r1",
				RuntimeIdentity: "h3-dit-runtime-r1", Command: []string{"/opt/vela/bin/h3-dit"},
				InitializationTimeout: "2h", ShutdownTimeout: "2m",
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
		t.Fatal(err)
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
			}},
		},
		WorkerBundles: []fleetcontroller.WorkerBundleActuation{actuation},
	}
}

func writeFleetResidencyPlanRender(t *testing.T, fixture *bundleFixture, encoded []byte) {
	t.Helper()
	render := testFinalRender("fleet-controller", fixture.images[0], fixture.images)
	identity := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: vela-fleet-residency-plan-rollouts-r1\n  namespace: vela-system\n"
	target := identity +
		"immutable: true\ndata:\n  rollouts.json: |-\n    " +
		strings.ReplaceAll(string(encoded), "\n", "\n    ") + "\n"
	if !strings.Contains(render, identity) {
		t.Fatal("Fleet release fixture lacks ResidencyPlan ConfigMap replacement point")
	}
	writeTestFile(
		t,
		filepath.Join(fixture.directory, "render-fleet-controller.yaml"),
		[]byte(strings.Replace(render, identity, target, 1)),
	)
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
			(name == "observability" && contract.Kind == "PodMonitor")
		if !isWorkload {
			rendered.WriteString(testObject(contract.APIVersion, contract.Kind, contract.Namespace, resourceName))
			continue
		}
		rendered.WriteString(testWorkload(contract.APIVersion, contract.Kind, contract.Namespace, resourceName, image))
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
	entrypoint := "/usr/local/bin/" + name
	if name == "h3-stage-runtime" {
		entrypoint = "/usr/local/bin/vela-model-runtime"
	}
	imageConfig := map[string]any{"Entrypoint": []string{entrypoint}}
	if name == "h3-stage-runtime" {
		imageConfig["Labels"] = map[string]string{
			"vela.ai.h3-runtime-base":       "ghcr.io/minimax/h3-runtime-base@" + testDigest("runtime base"),
			"vela.ai.h3-encoder.sha256":     strings.TrimPrefix(testDigest("encoder"), "sha256:"),
			"vela.ai.h3-dit.sha256":         strings.TrimPrefix(testDigest("dit"), "sha256:"),
			"vela.ai.h3-vae-decoder.sha256": strings.TrimPrefix(testDigest("vae decoder"), "sha256:"),
		}
	}
	config := map[string]any{
		"architecture": "amd64",
		"os":           "linux",
		"config":       imageConfig,
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
