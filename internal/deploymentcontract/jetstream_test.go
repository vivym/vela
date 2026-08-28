package deploymentcontract

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
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

func TestControlStorageKustomizationPublishesReleaseContracts(t *testing.T) {
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
	want := map[string]string{
		"vela-jetstream-contract":           "jetstream-contract.json",
		"vela-barman-cloud-plugin-contract": "barman-cloud-plugin-contract.json",
	}
	for _, generator := range kustomization.ConfigMapGenerator {
		file, required := want[generator.Name]
		if !required {
			continue
		}
		if len(generator.Files) != 1 || generator.Files[0] != file {
			t.Fatalf("ConfigMap generator %s = %#v", generator.Name, generator.Files)
		}
		delete(want, generator.Name)
	}
	if len(want) != 0 {
		t.Fatalf("missing Control/Storage ConfigMap generators = %#v", want)
	}
}

func TestRenderedJetStreamUsesPinnedNATSImage(t *testing.T) {
	command := exec.Command("kubectl", "kustomize", filepath.Dir(controlStoragePath(t, "kustomization.yaml")))
	contents, err := command.Output()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			t.Fatalf("render Control/Storage Kustomize base: %v: %s", err, exit.Stderr)
		}
		t.Fatalf("render Control/Storage Kustomize base: %v", err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	for {
		var document struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name      string `yaml:"name"`
				Namespace string `yaml:"namespace"`
			} `yaml:"metadata"`
			Spec struct {
				Template struct {
					Spec struct {
						Containers []struct {
							Name  string `yaml:"name"`
							Image string `yaml:"image"`
						} `yaml:"containers"`
					} `yaml:"spec"`
				} `yaml:"template"`
			} `yaml:"spec"`
		}
		if err := decoder.Decode(&document); err != nil {
			if errors.Is(err, io.EOF) {
				t.Fatal("rendered Control/Storage resources do not contain StatefulSet/vela-system/nats")
			}
			t.Fatalf("decode rendered Control/Storage resources: %v", err)
		}
		if document.Kind != "StatefulSet" || document.Metadata.Namespace != "vela-system" ||
			document.Metadata.Name != "nats" {
			continue
		}
		containers := document.Spec.Template.Spec.Containers
		if len(containers) != 1 || containers[0].Name != "nats" ||
			containers[0].Image != "docker.io/library/nats@sha256:26b0ee1a95285aedae137aefb953701d9da1dfffcf7818eb3aeb536c4373892f" {
			t.Fatalf("rendered NATS container image = %#v", containers)
		}
		return
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
