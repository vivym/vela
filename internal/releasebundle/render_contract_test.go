package releasebundle

import (
	"bytes"
	"errors"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func modernBundleFixture(t *testing.T) *bundleFixture {
	t.Helper()
	fixture := newBundleFixture(t)
	fixture.plan.RenderContract = KubernetesRenderContractV2
	modifyFixtureRender(t, fixture, "control-storage", func(documents []map[string]any) []map[string]any {
		var result []map[string]any
		for _, document := range documents {
			meta := document["metadata"].(map[string]any)
			if document["kind"] == "ConfigMap" && meta["name"] == "nats-config" {
				continue
			}
			if document["kind"] == "StatefulSet" && meta["name"] == "nats" {
				pod := document["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
				container := pod["containers"].([]any)[0].(map[string]any)
				container["name"] = "nats"
				container["args"] = []any{"--config", "/etc/nats-config/nats.conf"}
				container["volumeMounts"] = []any{map[string]any{"name": "config", "mountPath": "/etc/nats-config", "readOnly": true}}
				pod["volumes"] = append(pod["volumes"].([]any), map[string]any{"name": "config", "secret": map[string]any{
					"secretName": "nats-auth-r1", "items": []any{map[string]any{"key": "nats.conf", "path": "nats.conf"}},
				}})
			}
			result = append(result, document)
		}
		return result
	})
	modifyFixtureRender(t, fixture, "observability", func(documents []map[string]any) []map[string]any {
		for _, document := range documents {
			document["metadata"].(map[string]any)["namespace"] = "monitoring"
		}
		rules, err := decodeYAMLDocuments(readTestFile(t, "../../deploy/observability/prometheus-rule.yaml"), &yamlGraphBudget{})
		if err != nil || len(rules) != 1 {
			t.Fatalf("real application rules: %v", err)
		}
		return append(documents, rules[0])
	})
	modifyFixtureRender(t, fixture, "vela-control", func(documents []map[string]any) []map[string]any {
		return append(documents, map[string]any{
			"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
			"metadata": map[string]any{"name": "vela-control-allow-node-agent", "namespace": "vela-system"},
			"spec": map[string]any{
				"podSelector": map[string]any{"matchLabels": map[string]any{"app.kubernetes.io/name": "vela-control"}},
				"policyTypes": []any{"Ingress"},
				"ingress": []any{map[string]any{
					"from":  []any{map[string]any{"ipBlock": map[string]any{"cidr": "10.1.201.66/32"}}},
					"ports": []any{map[string]any{"protocol": "TCP", "port": 8444}},
				}},
			},
		})
	})
	secret := findExternalResource(t, fixture, "vela-observability", "shared-secret-r1")
	secret.Namespace = "monitoring"
	secret.Consumers = []string{"PodMonitor/monitoring/vela-control"}
	fixture.plan.ExternalResources = append(fixture.plan.ExternalResources, ExternalResource{
		Kind: "Secret", Namespace: "vela-system", Name: "nats-auth-r1", Revision: testDigest("nats auth"),
		RequiredKeys: []string{"nats.conf"}, Consumers: []string{"StatefulSet/vela-system/nats"},
	})
	fixture.writePlan(t)
	return fixture
}

func modifyFixtureRender(t *testing.T, fixture *bundleFixture, name string, mutate func([]map[string]any) []map[string]any) {
	t.Helper()
	path := filepath.Join(fixture.directory, "render-"+name+".yaml")
	documents, err := decodeYAMLDocuments(readTestFile(t, path), &yamlGraphBudget{})
	if err != nil {
		t.Fatal(err)
	}
	var result bytes.Buffer
	encoder := yaml.NewEncoder(&result)
	for _, document := range mutate(documents) {
		if err := encoder.Encode(document); err != nil {
			t.Fatal(err)
		}
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, path, result.Bytes())
}

func TestRenderContractV2BuildLoadAndDowngradeRejection(t *testing.T) {
	fixture := modernBundleFixture(t)
	bundle, encoded, err := buildTestBundle(fixture.planPath)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.ConfigurationManifest.RenderContract != KubernetesRenderContractV2 || !bytes.Contains(encoded, []byte(`"render_contract": "kubernetes-v2"`)) {
		t.Fatal("resource contract is not bound into the configuration")
	}
	path := filepath.Join(fixture.directory, "release-bundle.json")
	writeTestFile(t, path, encoded)
	loaded, rollouts, err := LoadResidencyPlanRollouts(path)
	if err != nil || loaded.ReleaseDigest != bundle.ReleaseDigest || len(rollouts) == 0 {
		t.Fatalf("modern bundle authority roundtrip: %v", err)
	}
	bundle.ConfigurationManifest.RenderContract = ""
	writeTestJSON(t, path, bundle)
	if _, err := Load(path); !errors.Is(err, ErrInvalidBundle) {
		t.Fatalf("downgraded contract accepted: %v", err)
	}
	legacy := newBundleFixture(t)
	_, encoded, err = buildTestBundle(legacy.planPath)
	if err != nil || bytes.Contains(encoded, []byte("render_contract")) {
		t.Fatalf("legacy schema-3 encoding changed: %v", err)
	}
	legacy.plan.RenderContract = KubernetesRenderContractV2
	legacy.writePlan(t)
	if _, _, err := buildTestBundle(legacy.planPath); !errors.Is(err, ErrInvalidBundle) {
		t.Fatalf("legacy inventory accepted as v2: %v", err)
	}
}

func TestRenderContractV2RejectsMissingOrExpandedAuthority(t *testing.T) {
	for _, scenario := range []string{"unknown-contract", "missing-rules", "duplicate-monitor", "wrong-namespace", "extra-configmap", "embedded-secret", "missing-nats-secret", "wrong-nats-key", "wrong-nats-consumer", "missing-nats-mount", "optional-nats-config", "node-source-wildcard", "node-source-documentation", "node-source-mapped-documentation", "node-source-other-port"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := modernBundleFixture(t)
			switch scenario {
			case "unknown-contract":
				fixture.plan.RenderContract = "kubernetes-v999"
			case "missing-nats-secret":
				fixture.plan.ExternalResources = fixture.plan.ExternalResources[:len(fixture.plan.ExternalResources)-1]
			case "wrong-nats-key":
				findExternalResource(t, fixture, "vela-system", "nats-auth-r1").RequiredKeys = []string{"token"}
			case "wrong-nats-consumer":
				findExternalResource(t, fixture, "vela-system", "nats-auth-r1").Consumers = []string{"Deployment/vela-system/vela-control"}
			case "node-source-wildcard", "node-source-documentation", "node-source-mapped-documentation", "node-source-other-port":
				modifyFixtureRender(t, fixture, "vela-control", func(documents []map[string]any) []map[string]any {
					rule := documents[len(documents)-1]["spec"].(map[string]any)["ingress"].([]any)[0].(map[string]any)
					if scenario == "node-source-other-port" {
						rule["ports"].([]any)[0].(map[string]any)["port"] = 8081
					} else {
						cidr := "0.0.0.0/0"
						if scenario == "node-source-documentation" {
							cidr = "192.0.2.0/32"
						}
						if scenario == "node-source-mapped-documentation" {
							cidr = "::ffff:192.0.2.1/128"
						}
						rule["from"].([]any)[0].(map[string]any)["ipBlock"].(map[string]any)["cidr"] = cidr
					}
					return documents
				})
			case "missing-nats-mount", "optional-nats-config":
				modifyFixtureRender(t, fixture, "control-storage", func(documents []map[string]any) []map[string]any {
					for _, document := range documents {
						if document["kind"] != "StatefulSet" {
							continue
						}
						pod := document["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
						if scenario == "missing-nats-mount" {
							delete(pod["containers"].([]any)[0].(map[string]any), "volumeMounts")
						} else {
							pod["volumes"].([]any)[1].(map[string]any)["secret"].(map[string]any)["optional"] = true
						}
					}
					return documents
				})
			default:
				modifyFixtureRender(t, fixture, "observability", func(documents []map[string]any) []map[string]any {
					switch scenario {
					case "missing-rules":
						return documents[:len(documents)-1]
					case "duplicate-monitor":
						return append(documents, documents[len(documents)-2])
					case "wrong-namespace":
						documents[0]["metadata"].(map[string]any)["namespace"] = "vela-observability"
					case "extra-configmap", "embedded-secret":
						kind := "ConfigMap"
						if scenario == "embedded-secret" {
							kind = "Secret"
						}
						return append(documents, map[string]any{"apiVersion": "v1", "kind": kind, "metadata": map[string]any{"name": "extra", "namespace": "monitoring"}})
					}
					return documents
				})
			}
			fixture.writePlan(t)
			if _, _, err := buildTestBundle(fixture.planPath); !errors.Is(err, ErrInvalidBundle) {
				t.Fatalf("invalid authority accepted: %v", err)
			}
		})
	}
}

func TestCurrentRepositoryRendersMatchVersionedContract(t *testing.T) {
	for _, name := range []string{"observability", "control-storage", "vela-control"} {
		t.Run(name, func(t *testing.T) {
			var content []byte
			var err error
			if name == "observability" {
				path := filepath.Join(t.TempDir(), "observability.yaml")
				command := exec.Command("python3", "../../hack/render-release-observability.py", "--output", path)
				if output, runErr := command.CombinedOutput(); runErr != nil {
					t.Fatalf("application observability render: %v: %s", runErr, output)
				}
				content = readTestFile(t, path)
			} else {
				directory := "../../deploy/control-storage"
				if name == "vela-control" {
					directory = "../../deploy/environments/marslab/vela-control"
				}
				content, err = exec.Command("kubectl", "kustomize", directory).Output()
				if err != nil {
					t.Fatal(err)
				}
			}
			inventory := newRenderInventory()
			if err := validateFinalRenderWithContract(KubernetesRenderContractV2, name, content, &inventory, &yamlGraphBudget{}); err != nil {
				t.Fatalf("real %s render violates release contract: %v", name, err)
			}
			if name == "observability" {
				full, err := exec.Command("kubectl", "kustomize", "../../deploy/observability").Output()
				if err != nil {
					t.Fatal(err)
				}
				platform, err := decodeYAMLDocuments(full, &yamlGraphBudget{})
				if err != nil {
					t.Fatal(err)
				}
				application, err := decodeYAMLDocuments(content, &yamlGraphBudget{})
				if err != nil {
					t.Fatal(err)
				}
				for _, resource := range application {
					matches := 0
					for _, existing := range platform {
						if reflect.DeepEqual(resource, existing) {
							matches++
						}
					}
					if matches != 1 {
						t.Fatalf("application monitoring resource differs from platform render: %v", resource["metadata"])
					}
				}
			}
			if name == "control-storage" {
				// One NATS image and all eight dependency-contract images must
				// require OCI descriptor/config inputs, including the sidecar.
				if len(inventory.images) != 9 {
					t.Fatalf("dependency image closure omitted an image: %v", sortedKeys(inventory.images))
				}
				key := resourceKey{Kind: "Secret", Namespace: "vela-system", Name: "vela-nats-auth"}
				if strings.Join(sortedSet(inventory.secretKeys[key]), ",") != "nats.conf" ||
					strings.Join(sortedSet(inventory.secretConsumers[key]), ",") != "StatefulSet/vela-system/nats" {
					t.Fatal("actual NATS Secret source was not bound to its key and server")
				}
			}
			if name == "vela-control" {
				if len(inventory.secretKeys) != 12 {
					t.Fatalf("expected 12 Control Secret references, got %d", len(inventory.secretKeys))
				}
				for secret, keys := range inventory.secretKeys {
					if len(keys) == 0 {
						t.Fatalf("Control Secret has no explicit key contract: %s", secret.Name)
					}
				}
			}
		})
	}
}
