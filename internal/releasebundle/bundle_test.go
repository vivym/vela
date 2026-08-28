package releasebundle

import (
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

func TestBuildIsDeterministicForTwoWorkers(t *testing.T) {
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
			selector:   "  volumes:\n    - name: credentials\n      secret:\n        secretName: whole-volume-secret-r1\n",
			keys:       []string{"token"},
		},
		{
			name:       "whole Secret envFrom",
			secretName: "whole-env-secret-r1",
			selector:   "      envFrom:\n        - secretRef:\n            name: whole-env-secret-r1\n",
			keys:       []string{"token"},
		},
		{
			name:       "arbitrary declared keys",
			secretName: "arbitrary-secret-r1",
			selector:   "      envFrom:\n        - secretRef:\n            name: arbitrary-secret-r1\n",
			keys:       []string{"alpha", "zeta"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newBundleFixture(t)
			consumer := "Pod/vela-system/" + test.secretName
			fixture.plan.ExternalResources = append(fixture.plan.ExternalResources, ExternalResource{
				Kind: "Secret", Namespace: "vela-system", Name: test.secretName,
				Revision: testDigest(test.secretName), RequiredKeys: test.keys, Consumers: []string{consumer},
			})
			appendFinalRender(t, fixture, "control-storage", testSecretPod(test.secretName, fixture.images[0], test.selector))
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
			Consumers: []string{"Pod/vela-system/registry-auth-r1"},
		})
		selector := "  imagePullSecrets:\n    - name: registry-auth-r1\n"
		appendFinalRender(t, fixture, "control-storage", testSecretPod("registry-auth-r1", fixture.images[0], selector))
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
			Consumers: []string{"Pod/vela-system/keyed-secret-r1"},
		})
		selector := "      env:\n        - name: TOKEN\n          valueFrom:\n            secretKeyRef:\n              name: keyed-secret-r1\n              key: token\n" +
			"  volumes:\n    - name: projected-credentials\n      projected:\n        sources:\n          - secret:\n              name: keyed-secret-r1\n              items:\n                - key: certificate\n                  path: certificate\n"
		appendFinalRender(t, fixture, "control-storage", testSecretPod("keyed-secret-r1", fixture.images[0], selector))
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
				path := filepath.Join(fixture.directory, fixture.plan.FinalRenders[len(fixture.plan.FinalRenders)-1].Ref)
				content := readTestFile(t, path)
				writeTestFile(t, path, []byte(strings.Replace(string(content),
					"  desired.yaml: |", "  secret.yaml: |\n    apiVersion: v1\n    kind: Secret\n    metadata:\n      name: hidden\n      namespace: vela-system\n    data:\n      token: c2VjcmV0\n  desired.yaml: |", 1)))
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

func appendFinalRender(t *testing.T, fixture *bundleFixture, name, document string) {
	t.Helper()
	for _, render := range fixture.plan.FinalRenders {
		if render.Name == name {
			path := filepath.Join(fixture.directory, render.Ref)
			writeTestFile(t, path, append(readTestFile(t, path), []byte(document)...))
			return
		}
	}
	t.Fatalf("final render %s not found", name)
}

func testSecretPod(name, image, selector string) string {
	return "apiVersion: v1\nkind: Pod\nmetadata:\n  name: " + name +
		"\n  namespace: vela-system\nspec:\n  containers:\n    - name: probe\n      image: " + image + "\n" +
		selector + "---\n"
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
			{
				Kind: "Secret", Namespace: "vela-system", Name: "artifact-store-ca-r1", Revision: testDigest("artifact ca"),
				RequiredKeys: []string{"ca.crt"}, Consumers: []string{"ConfigMap/vela-system/worker-agent-config-r1"},
			},
			{
				Kind: "Secret", Namespace: "vela-observability", Name: "shared-secret-r1", Revision: testDigest("observability shared"),
				RequiredKeys: []string{"token"}, Consumers: []string{"Deployment/vela-observability/vela-observability-exporter"},
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
					"ConfigMap/vela-system/worker-agent-config-r1",
					workerConsumerIdentity(worker),
				},
			},
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

func addSecondWorker(t *testing.T, fixture *bundleFixture) {
	t.Helper()
	second := WorkerMaterializationInput{
		NodeIdentity: "h3-node-02", Namespace: "vela-system",
		WorkerID: "10000000-0000-0000-0000-000000000002", WorkerEpoch: 8,
		WorkerPoolID: "20000000-0000-0000-0000-000000000001", FleetRevision: strings.Repeat("b", 64),
		WorkerRuntimeConfigMap: "worker-runtime-node-2", WorkerRuntimeRef: "worker-runtime-node-2.yaml",
		RunnerProfilesConfigMap: "runner-profiles-node-2", RunnerProfilesRef: "runner-profiles-node-2.yaml",
		RunnerGPURolesConfigMap: "runner-gpu-roles-node-2", RunnerGPURolesRef: "runner-gpu-roles-node-2.yaml",
		WorkerControlTLSSecret: "worker-control-tls-node-2", WorkerControlTLSSecretRevision: testDigest("worker tls node 2"),
		ExecutionProfileRevisionID: "30000000-0000-0000-0000-000000000002",
		InferenceBackendRevision:   "h3-backend-r2", ModelRevisionID: "40000000-0000-0000-0000-000000000002",
	}
	second.NodeAgentIdentity = expectedNodeAgentIdentity(second.NodeIdentity, second.WorkerID)
	writeWorkerMaterializationWithGPUOffset(t, fixture.directory, second, 8)
	fixture.plan.WorkerMaterializations = append(fixture.plan.WorkerMaterializations, second)
	fixture.plan.ExternalResources = append(fixture.plan.ExternalResources, ExternalResource{
		Kind: "Secret", Namespace: second.Namespace, Name: second.WorkerControlTLSSecret,
		Revision:     second.WorkerControlTLSSecretRevision,
		RequiredKeys: []string{"ca.crt", "tls.crt", "tls.key"},
		Consumers:    []string{workerConsumerIdentity(second)},
	})
}

func testFinalRender(name, image string, images []string) string {
	switch name {
	case "control-storage":
		return testObject("v1", "Service", "vela-system", "nats") +
			testWorkload("apps/v1", "StatefulSet", "vela-system", "nats", image) +
			testObject("barmancloud.cnpg.io/v1", "ObjectStore", "vela-system", "vela-postgres-backup") +
			testObject("postgresql.cnpg.io/v1", "Cluster", "vela-system", "vela-postgres") +
			testObject("postgresql.cnpg.io/v1", "ScheduledBackup", "vela-system", "vela-postgres-daily")
	case "fleet-controller":
		return testWorkload("apps/v1", "Deployment", "vela-system", "vela-fleet-controller", image) +
			testObject("v1", "Service", "vela-system", "vela-fleet-admission") +
			testObject("apiextensions.k8s.io/v1", "CustomResourceDefinition", "", "workerpools.fleet.vela.ai") +
			testObject("admissionregistration.k8s.io/v1", "ValidatingWebhookConfiguration", "", "vela-fleet-protection")
	case "observability":
		return testObject("monitoring.coreos.com/v1", "PodMonitor", "vela-observability", "vela-control") +
			testWorkload("apps/v1", "Deployment", "vela-observability", "vela-observability-exporter", image)
	case "vela-control":
		return testWorkload("apps/v1", "Deployment", "vela-system", "vela-control", image) +
			testObject("v1", "Service", "vela-system", "vela-api") +
			testObject("v1", "Service", "vela-system", "vela-control") +
			testObject("v1", "Service", "vela-system", "vela-worker-control") +
			testObject("v1", "Service", "vela-system", "vela-finance-reconciliation") +
			testObject("v1", "Service", "vela-system", "vela-compliance") +
			testObject("networking.k8s.io/v1", "NetworkPolicy", "vela-system", "vela-control-default-deny-ingress")
	case "worker-agent":
		return testWorkload("apps/v1", "DaemonSet", "vela-system", "vela-h3-worker", image) + `apiVersion: v1
kind: ConfigMap
metadata:
  name: worker-agent-config-r1
  namespace: vela-system
immutable: true
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
---
`
	default:
		panic("unknown final render " + name)
	}
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
