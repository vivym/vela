package deploymentcontract

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/vivym/vela/internal/eventstream"
	"gopkg.in/yaml.v3"
)

type jetStreamReleaseContract struct {
	Revision  int                        `json:"revision"`
	Stream    jetstream.StreamConfig     `json:"stream"`
	Consumers []jetstream.ConsumerConfig `json:"consumers"`
}

func TestRenderedJetStreamContractMatchesTypedReleaseAuthority(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve deployment contract test path")
	}
	path := filepath.Join(
		filepath.Dir(currentFile),
		"..", "..", "deploy", "control-storage", "jetstream-contract.json",
	)
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open rendered JetStream contract: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var contract jetStreamReleaseContract
	if err := decoder.Decode(&contract); err != nil {
		t.Fatalf("decode rendered JetStream contract: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		t.Fatalf("rendered JetStream contract trailing JSON = %v", err)
	}
	if contract.Revision != 1 {
		t.Fatalf("rendered JetStream contract revision = %d", contract.Revision)
	}
	if err := eventstream.ValidateStreamConfig(contract.Stream); err != nil {
		t.Fatalf("rendered JetStream stream drift: %v", err)
	}
	if len(contract.Consumers) != 1 {
		t.Fatalf("rendered JetStream consumers = %#v", contract.Consumers)
	}
	if err := eventstream.ValidateSchedulerConsumerConfig(contract.Consumers[0]); err != nil {
		t.Fatalf("rendered Scheduler consumer drift: %v", err)
	}
}

func TestControlStorageKustomizationPublishesJetStreamReleaseContract(t *testing.T) {
	path := controlStoragePath(t, "kustomization.yaml")
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Control/Storage kustomization: %v", err)
	}
	var kustomization struct {
		ConfigMapGenerator []struct {
			Name  string   `yaml:"name"`
			Files []string `yaml:"files"`
		} `yaml:"configMapGenerator"`
	}
	if err := yaml.Unmarshal(contents, &kustomization); err != nil {
		t.Fatalf("decode Control/Storage kustomization: %v", err)
	}
	if len(kustomization.ConfigMapGenerator) != 1 ||
		kustomization.ConfigMapGenerator[0].Name != "vela-jetstream-contract" ||
		len(kustomization.ConfigMapGenerator[0].Files) != 1 ||
		kustomization.ConfigMapGenerator[0].Files[0] != "jetstream-contract.json" {
		t.Fatalf("JetStream ConfigMap generator = %#v", kustomization.ConfigMapGenerator)
	}
}

func controlStoragePath(t *testing.T, name string) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve deployment contract test path")
	}
	return filepath.Join(filepath.Dir(currentFile), "..", "..", "deploy", "control-storage", name)
}
