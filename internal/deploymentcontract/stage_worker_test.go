package deploymentcontract

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"testing"

	"gopkg.in/yaml.v3"
)

type stageWorkerConfigMap struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"metadata"`
	Immutable bool              `yaml:"immutable"`
	Data      map[string]string `yaml:"data"`
}

type stageWorkerNamespace struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name   string            `yaml:"name"`
		Labels map[string]string `yaml:"labels"`
	} `yaml:"metadata"`
}

type stageWorkerSecretContract struct {
	APIVersion      string `json:"apiVersion"`
	Kind            string `json:"kind"`
	ReleaseRevision string `json:"releaseRevision"`
	FileSecrets     []struct {
		Name                        string            `json:"name"`
		RequiredKeys                []string          `json:"requiredKeys"`
		RequiredPerWorkerMemberKeys []string          `json:"requiredPerWorkerMemberKeys"`
		ProjectedPath               string            `json:"projectedPath"`
		MaterializedPaths           map[string]string `json:"materializedPaths"`
	} `json:"fileSecrets"`
}

func TestStageWorkerRuntimeConfigDefinesProductionCompositionInputs(t *testing.T) {
	var config stageWorkerConfigMap
	loadStageWorkerYAML(t, "runtime-config.yaml", &config)
	wantKeys := []string{
		"artifact-s3-bucket",
		"artifact-s3-endpoint",
		"artifact-s3-path-style",
		"artifact-s3-region",
		"artifact-s3-signed-get-ttl",
		"authority-active-key-id",
		"capacity-ttl",
		"connector-revision-id",
		"control-address",
		"control-server-name",
		"heartbeat-interval",
		"materialization-journal-limit",
		"member-dial-timeout",
		"member-shutdown-timeout",
		"model-runtime-cancel-timeout",
		"model-runtime-shutdown-timeout",
		"model-runtime-startup-timeout",
		"retry-maximum",
		"retry-minimum",
		"source-loss-consumed-resource-units",
		"source-loss-retry",
	}
	gotKeys := make([]string, 0, len(config.Data))
	for key, value := range config.Data {
		if value == "" {
			t.Fatalf("Stage Worker runtime ConfigMap key %q is empty", key)
		}
		gotKeys = append(gotKeys, key)
	}
	sort.Strings(gotKeys)
	if config.APIVersion != "v1" || config.Kind != "ConfigMap" ||
		config.Metadata.Name != "vela-stage-worker-runtime-placeholder" ||
		config.Metadata.Namespace != "vela-system" || !config.Immutable ||
		!reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("Stage Worker runtime ConfigMap = %#v keys=%#v", config, gotKeys)
	}
}

func TestModelRuntimeVerifierConfigContainsOnlyImmutablePublicAuthority(t *testing.T) {
	var config stageWorkerConfigMap
	loadStageWorkerYAML(t, "model-runtime-verifier.yaml", &config)
	var keyring map[string]string
	if err := json.Unmarshal([]byte(config.Data["verifier-keyring.json"]), &keyring); err != nil {
		t.Fatalf("decode ModelRuntime verifier keyring: %v", err)
	}
	if config.APIVersion != "v1" || config.Kind != "ConfigMap" ||
		config.Metadata.Name != "vela-model-runtime-verifier-placeholder" ||
		config.Metadata.Namespace != "vela-system" || !config.Immutable || len(config.Data) != 1 ||
		len(keyring) != 1 || keyring["stage-authority-r0-placeholder"] == "" {
		t.Fatalf("ModelRuntime verifier ConfigMap = %#v keyring=%#v", config, keyring)
	}
}

func TestStageWorkerBaseDoesNotMutateSharedNamespacePodSecurityPolicy(t *testing.T) {
	var namespace stageWorkerNamespace
	loadStageWorkerYAML(t, "namespace.yaml", &namespace)
	if namespace.APIVersion != "v1" || namespace.Kind != "Namespace" ||
		namespace.Metadata.Name != "vela-system" ||
		!reflect.DeepEqual(namespace.Metadata.Labels, map[string]string{
			"app.kubernetes.io/part-of": "vela",
		}) {
		t.Fatalf("Stage Worker shared Namespace = %#v", namespace)
	}
}

func TestStageWorkerSecretContractSeparatesAndMaterializesPrivateInputs(t *testing.T) {
	payload := readStageWorkerFile(t, "secret-contract.json")
	var contract stageWorkerSecretContract
	if err := json.Unmarshal(payload, &contract); err != nil {
		t.Fatalf("parse Stage Worker Secret contract: %v", err)
	}
	want := map[string]map[string]string{
		"vela-stage-worker-control-mtls-r0-placeholder": {
			"tls.crt": "/etc/vela-stage-worker/private/control/tls.crt",
			"tls.key": "/etc/vela-stage-worker/private/control/tls.key",
			"ca.crt":  "/etc/vela-stage-worker/private/control/ca.crt",
		},
		"vela-stage-worker-authority-r0-placeholder": {
			"keyring.json": "/etc/vela-stage-worker/private/authority/keyring.json",
		},
		"vela-stage-worker-artifact-credentials-r0-placeholder": {
			"access-key-id":     "/etc/vela-stage-worker/private/artifact/access-key-id",
			"secret-access-key": "/etc/vela-stage-worker/private/artifact/secret-access-key",
		},
		"vela-stage-worker-artifact-ca-r0-placeholder": {
			"ca.crt": "/etc/vela-stage-worker/private/artifact/ca.crt",
		},
	}
	if contract.APIVersion != "vela.ai/v1alpha1" ||
		contract.Kind != "VelaStageWorkerSecretContract" ||
		contract.ReleaseRevision != "r0-placeholder" || len(contract.FileSecrets) != len(want) {
		t.Fatalf("Stage Worker Secret contract identity = %#v", contract)
	}
	for _, secret := range contract.FileSecrets {
		expected, ok := want[secret.Name]
		if !ok || !reflect.DeepEqual(secret.MaterializedPaths, expected) || secret.ProjectedPath == "" {
			t.Fatalf("Stage Worker Secret contract entry = %#v", secret)
		}
		keys := append([]string(nil), secret.RequiredKeys...)
		sort.Strings(keys)
		expectedKeys := make([]string, 0, len(expected))
		for key := range expected {
			expectedKeys = append(expectedKeys, key)
		}
		sort.Strings(expectedKeys)
		if secret.Name == "vela-stage-worker-control-mtls-r0-placeholder" {
			expectedKeys = []string{"ca.crt"}
			if !reflect.DeepEqual(secret.RequiredPerWorkerMemberKeys, []string{
				"<worker-member-uuid>.tls.crt", "<worker-member-uuid>.tls.key",
			}) {
				t.Fatalf("Stage Worker control member key templates=%#v", secret.RequiredPerWorkerMemberKeys)
			}
		} else if len(secret.RequiredPerWorkerMemberKeys) != 0 {
			t.Fatalf("Stage Worker Secret %q has unexpected member key templates", secret.Name)
		}
		if !reflect.DeepEqual(keys, expectedKeys) {
			t.Fatalf("Stage Worker Secret %q keys=%#v want=%#v", secret.Name, keys, expectedKeys)
		}
		delete(want, secret.Name)
	}
	if len(want) != 0 {
		t.Fatalf("Stage Worker Secret contract omitted %#v", want)
	}
}

func TestRenderedStageWorkerBaseContainsNoStaticWorkload(t *testing.T) {
	directory := stageWorkerDirectory(t)
	command := exec.Command("kubectl", "kustomize", directory)
	encoded, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("render Stage Worker base: %v\n%s", err, encoded)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	var kinds []string
	for {
		var document struct {
			Kind string `yaml:"kind"`
		}
		if err := decoder.Decode(&document); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("decode rendered Stage Worker base: %v", err)
		}
		if document.Kind != "" {
			kinds = append(kinds, document.Kind)
		}
	}
	if !reflect.DeepEqual(kinds, []string{"Namespace", "ServiceAccount", "ConfigMap", "ConfigMap"}) {
		t.Fatalf("rendered Stage Worker kinds = %v", kinds)
	}
}

func loadStageWorkerYAML(t *testing.T, name string, destination any) {
	t.Helper()
	payload := readStageWorkerFile(t, name)
	if err := yaml.Unmarshal(payload, destination); err != nil {
		t.Fatalf("parse Stage Worker deployment file %q: %v", name, err)
	}
}

func readStageWorkerFile(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join(stageWorkerDirectory(t), name)
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Stage Worker deployment file %q: %v", name, err)
	}
	return payload
}

func stageWorkerDirectory(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve Stage Worker deployment contract test path")
	}
	return filepath.Join(filepath.Dir(currentFile), "..", "..", "deploy", "stage-worker")
}
