package deploymentcontract

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os/exec"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

const pinnedBusyBoxLinuxAMD64Image = "docker.io/library/busybox@sha256:7a3ebe5bfd1a4a19797d20b0c0bb39d44393e9a03fd852c0865b0f540d868df0"

type renderedMaterializerDocument struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"metadata"`
	Data map[string]string `yaml:"data"`
	Spec struct {
		Template struct {
			Spec struct {
				InitContainers []struct {
					Name  string `yaml:"name"`
					Image string `yaml:"image"`
				} `yaml:"initContainers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

func TestRenderedRootMaterializersUsePinnedBusyBoxImage(t *testing.T) {
	t.Run("vela-control", func(t *testing.T) {
		document := requireRenderedMaterializerDocument(t, "vela-control", "Deployment", "vela-control")
		if image := requireRenderedInitImage(t, document, "secret-materializer"); image != pinnedBusyBoxLinuxAMD64Image {
			t.Fatalf("rendered vela-control materializer image = %q, want %q", image, pinnedBusyBoxLinuxAMD64Image)
		}
	})

	t.Run("worker-agent", func(t *testing.T) {
		document := requireRenderedMaterializerDocument(t, "worker-agent", "DaemonSet", "vela-h3-worker")
		if image := requireRenderedInitImage(t, document, "runner-socket-permissions"); image != pinnedBusyBoxLinuxAMD64Image {
			t.Fatalf("rendered Worker materializer image = %q, want %q", image, pinnedBusyBoxLinuxAMD64Image)
		}
	})

	t.Run("fleet-controller", func(t *testing.T) {
		document := requireRenderedMaterializerDocument(
			t, "fleet-controller", "ConfigMap", "vela-fleet-residency-plan-rollouts-placeholder",
		)
		var desired struct {
			Rollouts []struct {
				WorkerBundles []struct {
					InitImage string `json:"init_image"`
				} `json:"worker_bundles"`
			} `json:"rollouts"`
		}
		payload := document.Data["rollouts.json"]
		if payload == "" {
			t.Fatal("rendered Fleet ResidencyPlan ConfigMap omitted rollouts.json")
		}
		if err := json.Unmarshal([]byte(payload), &desired); err != nil {
			t.Fatalf("parse rendered Fleet ResidencyPlan input: %v", err)
		}
		if len(desired.Rollouts) != 1 || len(desired.Rollouts[0].WorkerBundles) != 1 ||
			desired.Rollouts[0].WorkerBundles[0].InitImage != pinnedBusyBoxLinuxAMD64Image {
			t.Fatalf("rendered Fleet ResidencyPlan materializer images = %#v, want %q", desired.Rollouts, pinnedBusyBoxLinuxAMD64Image)
		}
	})
}

func requireRenderedMaterializerDocument(
	t *testing.T,
	base string,
	kind string,
	name string,
) renderedMaterializerDocument {
	t.Helper()
	manifestDirectory := filepath.Join(filepath.Dir(velaControlManifestDirectory(t)), base)
	command := exec.Command("kubectl", "kustomize", manifestDirectory)
	content, err := command.Output()
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			t.Fatalf("render %s Kustomize base: %v: %s", base, err, exit.Stderr)
		}
		t.Fatalf("render %s Kustomize base: %v", base, err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(content))
	for {
		var document renderedMaterializerDocument
		if err := decoder.Decode(&document); err != nil {
			if errors.Is(err, io.EOF) {
				t.Fatalf("rendered %s resources omitted %s/vela-system/%s", base, kind, name)
			}
			t.Fatalf("decode rendered %s resources: %v", base, err)
		}
		if document.Kind == kind && document.Metadata.Namespace == "vela-system" && document.Metadata.Name == name {
			return document
		}
	}
}

func requireRenderedInitImage(t *testing.T, document renderedMaterializerDocument, name string) string {
	t.Helper()
	for _, container := range document.Spec.Template.Spec.InitContainers {
		if container.Name == name {
			return container.Image
		}
	}
	t.Fatalf("rendered %s/vela-system/%s omitted init container %q", document.Kind, document.Metadata.Name, name)
	return ""
}
