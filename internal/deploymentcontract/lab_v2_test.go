package deploymentcontract

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/labv2contract"
	"gopkg.in/yaml.v3"
)

const labV2Namespace = "vela-lab-v2"

type labV2Document struct {
	APIVersion                   string `yaml:"apiVersion"`
	Kind                         string `yaml:"kind"`
	AutomountServiceAccountToken *bool  `yaml:"automountServiceAccountToken"`
	Metadata                     struct {
		Name      string            `yaml:"name"`
		Namespace string            `yaml:"namespace"`
		Labels    map[string]string `yaml:"labels"`
	} `yaml:"metadata"`
	Data map[string]string `yaml:"data"`
	Spec struct {
		Template struct {
			Metadata struct {
				Labels map[string]string `yaml:"labels"`
			} `yaml:"metadata"`
			Spec labV2PodSpec `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

type labV2PodSpec struct {
	AutomountServiceAccountToken *bool            `yaml:"automountServiceAccountToken"`
	Containers                   []labV2Container `yaml:"containers"`
	InitContainers               []labV2Container `yaml:"initContainers"`
	Volumes                      []struct {
		Name   string `yaml:"name"`
		Secret *struct {
			SecretName string `yaml:"secretName"`
		} `yaml:"secret"`
		EmptyDir *struct {
			Medium    string `yaml:"medium"`
			SizeLimit string `yaml:"sizeLimit"`
		} `yaml:"emptyDir"`
		Projected *struct {
			Sources []struct {
				ServiceAccountToken *struct {
					Path              string `yaml:"path"`
					ExpirationSeconds int64  `yaml:"expirationSeconds"`
				} `yaml:"serviceAccountToken"`
				ConfigMap *struct {
					Name  string `yaml:"name"`
					Items []struct {
						Key  string `yaml:"key"`
						Path string `yaml:"path"`
					} `yaml:"items"`
				} `yaml:"configMap"`
			} `yaml:"sources"`
		} `yaml:"projected"`
		HostPath *struct {
			Path string `yaml:"path"`
		} `yaml:"hostPath"`
	} `yaml:"volumes"`
	NodeSelector map[string]string `yaml:"nodeSelector"`
}

type labV2Container struct {
	Name         string   `yaml:"name"`
	Image        string   `yaml:"image"`
	Command      []string `yaml:"command"`
	Args         []string `yaml:"args"`
	VolumeMounts []struct {
		Name      string `yaml:"name"`
		MountPath string `yaml:"mountPath"`
		ReadOnly  bool   `yaml:"readOnly"`
	} `yaml:"volumeMounts"`
	Env []struct {
		Name  string `yaml:"name"`
		Value string `yaml:"value"`
	} `yaml:"env"`
	Resources struct {
		Requests map[string]any `yaml:"requests"`
		Limits   map[string]any `yaml:"limits"`
	} `yaml:"resources"`
	SecurityContext struct {
		ReadOnlyRootFilesystem bool `yaml:"readOnlyRootFilesystem"`
	} `yaml:"securityContext"`
	ReadinessProbe struct {
		HTTPGet struct {
			Scheme string `yaml:"scheme"`
		} `yaml:"httpGet"`
	} `yaml:"readinessProbe"`
}

func TestLabV2RenderIsIsolatedStageOnlyAndDigestPinned(t *testing.T) {
	directory := labV2Directory(t)
	temporary := t.TempDir()
	imagesPath := filepath.Join(temporary, "images.env")
	keys := []string{
		"POSTGRES_IMAGE", "NATS_IMAGE", "MINIO_IMAGE", "CONTROL_IMAGE",
		"FLEET_CONTROLLER_IMAGE", "STAGE_WORKER_AGENT_IMAGE", "RUNTIME_IMAGE", "BOOTSTRAP_IMAGE",
	}
	var images strings.Builder
	imageValues := make(map[string]string, len(keys))
	for index, key := range keys {
		digestCharacter := "abcdef0123456789"[index]
		value := "10.1.200.17:5443/vela-lab-v2/" +
			strings.ToLower(strings.ReplaceAll(key, "_", "-")) + "@sha256:" +
			strings.Repeat(string(digestCharacter), 64)
		imageValues[key] = value
		images.WriteString(key + "=" + value + "\n")
	}
	if err := os.WriteFile(imagesPath, []byte(images.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(temporary, "rendered")
	command := exec.Command(filepath.Join(directory, "render-manifests.sh"), imagesPath, output)
	if encoded, err := command.CombinedOutput(); err != nil {
		t.Fatalf("render lab-v2 manifests: %v\n%s", err, encoded)
	}

	documents := loadLabV2Documents(t, output)
	if len(documents) == 0 {
		t.Fatal("lab-v2 render is empty")
	}
	deployments := make(map[string]labV2Document)
	for _, document := range documents {
		if document.Kind == "Namespace" {
			if document.Metadata.Name != labV2Namespace {
				t.Fatalf("Namespace = %q, want %q", document.Metadata.Name, labV2Namespace)
			}
		} else if document.Metadata.Namespace != "" && document.Metadata.Namespace != labV2Namespace {
			t.Fatalf("%s/%s namespace = %q", document.Kind, document.Metadata.Name, document.Metadata.Namespace)
		}
		if document.Metadata.Labels["vela.ai/deployment-identity"] != labV2Namespace {
			t.Fatalf("%s/%s deployment identity = %q", document.Kind, document.Metadata.Name,
				document.Metadata.Labels["vela.ai/deployment-identity"])
		}
		if len(document.Spec.Template.Spec.Containers)+len(document.Spec.Template.Spec.InitContainers) > 0 &&
			document.Spec.Template.Metadata.Labels["vela.ai/deployment-identity"] != labV2Namespace {
			t.Fatalf("%s/%s Pod deployment identity is missing", document.Kind, document.Metadata.Name)
		}
		if document.Kind == "Deployment" {
			deployments[document.Metadata.Name] = document
		}
		for _, container := range append(document.Spec.Template.Spec.InitContainers, document.Spec.Template.Spec.Containers...) {
			if container.Image != "" && !strings.HasPrefix(container.Image, "10.1.200.17:5443/") ||
				(container.Image != "" && !strings.Contains(container.Image, "@sha256:")) {
				t.Fatalf("%s/%s container %s image = %q", document.Kind, document.Metadata.Name, container.Name, container.Image)
			}
			if _, present := container.Resources.Requests["nvidia.com/gpu"]; present {
				t.Fatalf("%s/%s container %s requests a GPU", document.Kind, document.Metadata.Name, container.Name)
			}
			if _, present := container.Resources.Limits["nvidia.com/gpu"]; present {
				t.Fatalf("%s/%s container %s limits a GPU", document.Kind, document.Metadata.Name, container.Name)
			}
		}
		for _, volume := range document.Spec.Template.Spec.Volumes {
			if volume.HostPath != nil && !strings.HasPrefix(volume.HostPath.Path, "/var/lib/vela-lab-v2/") {
				t.Fatalf("%s/%s hostPath = %q", document.Kind, document.Metadata.Name, volume.HostPath.Path)
			}
		}
	}

	minio := deployments["vela-lab-minio"]
	if len(minio.Spec.Template.Spec.Containers) != 1 ||
		minio.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet.Scheme != "HTTPS" ||
		!containsValue(minio.Spec.Template.Spec.Containers[0].Args, "/etc/minio/certs") {
		t.Fatalf("MinIO TLS deployment = %#v", minio.Spec.Template.Spec.Containers)
	}
	fleet := deployments["vela-lab-fleet-controller"]
	if len(fleet.Spec.Template.Spec.Containers) != 1 || fleet.Spec.Template.Spec.Containers[0].Name != "fleet-controller" {
		t.Fatalf("Fleet deployment = %#v", fleet.Spec.Template.Spec.Containers)
	}
	fleetAccount, ok := findLabV2Document(documents, "ServiceAccount", "vela-lab-fleet-controller")
	if !ok || fleetAccount.AutomountServiceAccountToken == nil || *fleetAccount.AutomountServiceAccountToken {
		t.Fatalf("Fleet ServiceAccount automount = %v", fleetAccount.AutomountServiceAccountToken)
	}
	if fleet.Spec.Template.Spec.AutomountServiceAccountToken == nil ||
		*fleet.Spec.Template.Spec.AutomountServiceAccountToken {
		t.Fatalf("Fleet Pod automount = %v", fleet.Spec.Template.Spec.AutomountServiceAccountToken)
	}
	materializer, ok := findContainer(fleet.Spec.Template.Spec.InitContainers, "materialize-secrets")
	if !ok || materializer.Image != imageValues["BOOTSTRAP_IMAGE"] ||
		!equalStrings(materializer.Command, []string{"/bin/sh", "-ec"}) ||
		!strings.Contains(strings.Join(materializer.Args, "\n"), `cp -L -- "$source" "$temporary"`) ||
		!strings.Contains(strings.Join(materializer.Args, "\n"), `chmod 0400 "$temporary"`) ||
		!strings.Contains(strings.Join(materializer.Args, "\n"), `mv -f -- "$temporary" "/materialized/$name"`) ||
		!materializer.SecurityContext.ReadOnlyRootFilesystem ||
		!hasVolumeMount(materializer, "projected-files", "/projected", true) ||
		!hasVolumeMount(materializer, "materialized-files", "/materialized", false) ||
		hasVolumeMountNamed(materializer, "fleet-service-account-token") {
		t.Fatalf("Fleet Secret materializer = %#v", materializer)
	}
	if !hasVolumeMount(fleet.Spec.Template.Spec.Containers[0], "materialized-files", "/etc/vela-fleet/private", true) ||
		!hasVolumeMount(fleet.Spec.Template.Spec.Containers[0], "fleet-service-account-token", "/var/run/secrets/kubernetes.io/serviceaccount", true) ||
		!hasSecretVolume(fleet.Spec.Template.Spec, "projected-files", "vela-lab-fleet-files") ||
		!hasMemoryEmptyDirVolume(fleet.Spec.Template.Spec, "materialized-files", "1Mi") ||
		!hasServiceAccountTokenVolume(fleet.Spec.Template.Spec, "fleet-service-account-token") {
		t.Fatalf("Fleet materialized TLS volumes = %#v", fleet.Spec.Template.Spec)
	}
	rollouts, ok := findLabV2Document(documents, "ConfigMap", "vela-lab-fleet-rollouts")
	if !ok || strings.TrimSpace(rollouts.Data["rollouts.json"]) != `{"schema_version":1,"rollouts":[]}` {
		t.Fatalf("Fleet empty desired state = %#v", rollouts.Data)
	}
	controlRuntime, ok := findLabV2Document(documents, "ConfigMap", "vela-lab-control-runtime")
	if !ok || controlRuntime.Data["VELA_LEASE_ACTIVE_KEY_ID"] != labv2contract.StageAuthorityKeyID {
		t.Fatalf("control Lease active key ID = %q, want %q",
			controlRuntime.Data["VELA_LEASE_ACTIVE_KEY_ID"], labv2contract.StageAuthorityKeyID)
	}
	for index := 1; index <= 2; index++ {
		name := "vela-lab-stage-worker-" + string(rune('0'+index))
		worker := deployments[name]
		assertLabV2RuntimePrivateMaterializer(t, name, worker)
		if got := containerNames(worker.Spec.Template.Spec.Containers); !equalStrings(got, []string{"model-runtime", "stage-worker-agent"}) {
			t.Fatalf("%s containers = %v", name, got)
		}
		if !hasEnvironment(worker.Spec.Template.Spec.Containers, "VELA_MODEL_RUNTIME_SOCKET", "/run/vela-model-runtime/private/runtime.sock") ||
			!hasEnvironment(worker.Spec.Template.Spec.Containers, "VELA_ARTIFACT_S3_ENDPOINT", "https://vela-lab-minio.vela-lab-v2.svc:9000") ||
			!hasEnvironment(worker.Spec.Template.Spec.Containers, "VELA_STAGE_WORKER_AUTHORITY_ACTIVE_KEY_ID", labv2contract.StageAuthorityKeyID) {
			t.Fatalf("%s does not bind the shared runtime socket, TLS Artifact Store, and StageAuthority key", name)
		}
		modelRuntime, ok := findContainer(worker.Spec.Template.Spec.Containers, "model-runtime")
		if !ok || modelRuntime.Image != imageValues["RUNTIME_IMAGE"] || len(modelRuntime.Command) != 0 {
			t.Fatalf("%s ModelRuntime container = %#v", name, modelRuntime)
		}
	}
	thumbnail := deployments["vela-lab-stage-worker-thumbnail"]
	assertLabV2RuntimePrivateMaterializer(t, "vela-lab-stage-worker-thumbnail", thumbnail)
	if got := containerNames(thumbnail.Spec.Template.Spec.Containers); !equalStrings(got, []string{"model-runtime", "stage-worker-agent"}) {
		t.Fatalf("thumbnail containers = %v", got)
	}
	thumbnailRuntime, ok := findContainer(thumbnail.Spec.Template.Spec.Containers, "model-runtime")
	if !ok || thumbnailRuntime.Image != imageValues["BOOTSTRAP_IMAGE"] ||
		!equalStrings(thumbnailRuntime.Command, []string{"/usr/local/bin/vela-model-runtime"}) ||
		thumbnail.Spec.Template.Spec.NodeSelector["kubernetes.io/hostname"] != "vela-lab-control-1" ||
		!hasEnvironment(thumbnail.Spec.Template.Spec.Containers, "VELA_MODEL_RUNTIME_SOCKET", "/run/vela-model-runtime/private/runtime.sock") ||
		!hasEnvironment(thumbnail.Spec.Template.Spec.Containers, "VELA_ARTIFACT_S3_ENDPOINT", "https://vela-lab-minio.vela-lab-v2.svc:9000") ||
		!hasEnvironment(thumbnail.Spec.Template.Spec.Containers, "VELA_STAGE_WORKER_AUTHORITY_ACTIVE_KEY_ID", labv2contract.StageAuthorityKeyID) {
		t.Fatalf("thumbnail CPU Worker deployment = %#v", thumbnail)
	}
}

func assertLabV2RuntimePrivateMaterializer(t *testing.T, name string, deployment labV2Document) {
	t.Helper()
	materializer, ok := findContainer(deployment.Spec.Template.Spec.InitContainers, "materialize-private-files")
	if !ok {
		t.Fatalf("%s private-file materializer is absent", name)
	}
	script := strings.Join(materializer.Args, "\n")
	if !strings.Contains(script, "test -d /runtime-private") {
		t.Fatalf("%s materializer does not verify the mounted runtime-private directory", name)
	}
	if strings.Contains(script, "install -d -m 0700 /runtime-private") {
		t.Fatalf("%s materializer attempts to chmod the root-owned runtime-private mount", name)
	}
	if !strings.Contains(script, "install -d -m 0700 /runtime-socket/private") ||
		!hasVolumeMount(materializer, "model-runtime-socket", "/runtime-socket", false) {
		t.Fatalf("%s materializer does not create the private ModelRuntime socket directory", name)
	}
}

func TestLabV2RenderRequiresDistinctRuntimeImageDigests(t *testing.T) {
	directory := labV2Directory(t)
	temporary := t.TempDir()
	imagesPath := filepath.Join(temporary, "images.env")
	keys := []string{
		"POSTGRES_IMAGE", "NATS_IMAGE", "MINIO_IMAGE", "CONTROL_IMAGE",
		"FLEET_CONTROLLER_IMAGE", "STAGE_WORKER_AGENT_IMAGE", "RUNTIME_IMAGE", "BOOTSTRAP_IMAGE",
	}
	sharedRuntimeDigest := strings.Repeat("a", 64)
	var images strings.Builder
	for index, key := range keys {
		digest := strings.Repeat(string("bcdef012"[index]), 64)
		if key == "RUNTIME_IMAGE" || key == "BOOTSTRAP_IMAGE" {
			digest = sharedRuntimeDigest
		}
		images.WriteString(key + "=10.1.200.17:5443/vela-lab-v2/" +
			strings.ToLower(strings.ReplaceAll(key, "_", "-")) + "@sha256:" + digest + "\n")
	}
	if err := os.WriteFile(imagesPath, []byte(images.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(
		filepath.Join(directory, "render-manifests.sh"), imagesPath,
		filepath.Join(temporary, "rendered"),
	)
	encoded, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(encoded), "must have distinct digests") {
		t.Fatalf("same runtime digest error=%v output=%s", err, encoded)
	}
}

func TestLabV2DeployRejectsManifestImageDriftFromImagesEnv(t *testing.T) {
	directory := labV2Directory(t)
	temporary := t.TempDir()
	imagesPath := filepath.Join(temporary, "images.env")
	keys := []string{
		"POSTGRES_IMAGE", "NATS_IMAGE", "MINIO_IMAGE", "CONTROL_IMAGE",
		"FLEET_CONTROLLER_IMAGE", "STAGE_WORKER_AGENT_IMAGE", "RUNTIME_IMAGE", "BOOTSTRAP_IMAGE",
	}
	var images strings.Builder
	var runtimeImage string
	for index, key := range keys {
		value := "10.1.200.17:5443/vela-lab-v2/" +
			strings.ToLower(strings.ReplaceAll(key, "_", "-")) + "@sha256:" +
			strings.Repeat(string("abcdef01"[index]), 64)
		if key == "RUNTIME_IMAGE" {
			runtimeImage = value
		}
		images.WriteString(key + "=" + value + "\n")
	}
	if err := os.WriteFile(imagesPath, []byte(images.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(temporary, "rendered")
	command := exec.Command(filepath.Join(directory, "render-manifests.sh"), imagesPath, output)
	if encoded, err := command.CombinedOutput(); err != nil {
		t.Fatalf("render lab-v2 manifests: %v\n%s", err, encoded)
	}
	workersPath := filepath.Join(output, "40-workers.yaml")
	workers, err := os.ReadFile(workersPath)
	if err != nil {
		t.Fatal(err)
	}
	tamperedImage := "10.1.200.17:5443/vela-lab-v2/tampered-runtime@sha256:" + strings.Repeat("9", 64)
	tampered := strings.Replace(string(workers), runtimeImage, tamperedImage, 1)
	if tampered == string(workers) {
		t.Fatal("runtime image was absent from rendered Worker manifest")
	}
	if err := os.WriteFile(workersPath, []byte(tampered), 0o640); err != nil {
		t.Fatal(err)
	}

	command = exec.Command(filepath.Join(directory, "deploy.sh"), output, "namespace", "--apply")
	encoded, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(encoded), "rendered manifest 40-workers.yaml does not match") {
		t.Fatalf("manifest drift error=%v output=%s", err, encoded)
	}
}

func TestLabV2ScriptsCannotTargetLegacyLab(t *testing.T) {
	directory := labV2Directory(t)
	for _, name := range []string{"deploy.sh", "install-assets.sh", "prepare-host.sh", "rollback.sh", "smoke.sh"} {
		content, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			t.Fatal(err)
		}
		text := string(content)
		if !strings.Contains(text, "vela-lab-v2") {
			t.Fatalf("%s does not name the isolated lab-v2 identity", name)
		}
		if strings.Contains(text, "namespace=vela-lab\n") || strings.Contains(text, "/var/lib/vela-lab/") ||
			strings.Contains(text, "/var/lib/vela-lab ") ||
			strings.Contains(text, "VELA_LAB_NAMESPACE:-vela-lab}") {
			t.Fatalf("%s retains a legacy vela-lab target", name)
		}
	}
}

func TestLabV2OperationalScriptsFailClosedAroundOwnershipAndRollback(t *testing.T) {
	directory := labV2Directory(t)
	read := func(name string) string {
		t.Helper()
		content, err := os.ReadFile(filepath.Join(directory, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(content)
	}

	for _, name := range []string{"deploy.sh", "install-assets.sh"} {
		text := read(name)
		getNamespace := strings.Index(text, "get namespace \"$namespace\"")
		createNamespace := strings.Index(text, "create -f")
		if getNamespace < 0 || createNamespace < 0 || getNamespace > createNamespace {
			t.Fatalf("%s does not verify an existing namespace before creation", name)
		}
		if strings.Contains(text, "apply -f \"$manifests/00-namespace.yaml\"") {
			t.Fatalf("%s mutates namespace ownership with apply", name)
		}
	}

	assets := read("install-assets.sh")
	for _, required := range []string{
		"--dry-run=client -o json | bind_asset_identity |",
		"--from-literal=runtime-image=\"$runtime_image\"",
		"--from-literal=thumbnail-runtime-image=\"$thumbnail_runtime_image\"",
	} {
		if !strings.Contains(assets, required) {
			t.Fatalf("install-assets.sh is missing atomic asset binding %q", required)
		}
	}
	if strings.Contains(assets, "$kubectl_bin label secret") ||
		strings.Contains(assets, "$kubectl_bin annotate secret") {
		t.Fatal("install-assets.sh labels a Secret after applying it")
	}

	deploy := read("deploy.sh")
	for _, required := range []string{
		".data.runtime-image", ".data.thumbnail-runtime-image",
		"RUNTIME_IMAGE does not match the installed asset identity",
		"BOOTSTRAP_IMAGE does not match the installed thumbnail runtime identity",
	} {
		if !strings.Contains(deploy, required) {
			t.Fatalf("deploy.sh is missing image identity binding %q", required)
		}
	}

	rollback := read("rollback.sh")
	foreground := strings.Index(rollback, "--cascade=foreground")
	dependents := strings.Index(rollback, "get 'pod,replicaset'")
	policies := strings.Index(rollback, "delete networkpolicy")
	if foreground < 0 || dependents <= foreground || policies <= dependents {
		t.Fatal("rollback.sh does not foreground-delete workloads before dependent and policy cleanup")
	}
}

func loadLabV2Documents(t *testing.T, directory string) []labV2Document {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	var result []labV2Document
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}
		payload, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		decoder := yaml.NewDecoder(bytes.NewReader(payload))
		for {
			var document labV2Document
			if err := decoder.Decode(&document); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("decode %s: %v", entry.Name(), err)
			}
			if document.Kind != "" {
				result = append(result, document)
			}
		}
	}
	return result
}

func findLabV2Document(documents []labV2Document, kind, name string) (labV2Document, bool) {
	for _, document := range documents {
		if document.Kind == kind && document.Metadata.Name == name {
			return document, true
		}
	}
	return labV2Document{}, false
}

func containerNames(containers []labV2Container) []string {
	names := make([]string, 0, len(containers))
	for _, container := range containers {
		names = append(names, container.Name)
	}
	sort.Strings(names)
	return names
}

func hasEnvironment(containers []labV2Container, name, value string) bool {
	for _, container := range containers {
		for _, environment := range container.Env {
			if environment.Name == name && environment.Value == value {
				return true
			}
		}
	}
	return false
}

func findContainer(containers []labV2Container, name string) (labV2Container, bool) {
	for _, container := range containers {
		if container.Name == name {
			return container, true
		}
	}
	return labV2Container{}, false
}

func hasVolumeMount(container labV2Container, name, mountPath string, readOnly bool) bool {
	for _, mount := range container.VolumeMounts {
		if mount.Name == name && mount.MountPath == mountPath && mount.ReadOnly == readOnly {
			return true
		}
	}
	return false
}

func hasVolumeMountNamed(container labV2Container, name string) bool {
	for _, mount := range container.VolumeMounts {
		if mount.Name == name {
			return true
		}
	}
	return false
}

func hasSecretVolume(pod labV2PodSpec, name, secretName string) bool {
	for _, volume := range pod.Volumes {
		if volume.Name == name && volume.Secret != nil && volume.Secret.SecretName == secretName {
			return true
		}
	}
	return false
}

func hasMemoryEmptyDirVolume(pod labV2PodSpec, name, sizeLimit string) bool {
	for _, volume := range pod.Volumes {
		if volume.Name == name && volume.EmptyDir != nil &&
			volume.EmptyDir.Medium == "Memory" && volume.EmptyDir.SizeLimit == sizeLimit {
			return true
		}
	}
	return false
}

func hasServiceAccountTokenVolume(pod labV2PodSpec, name string) bool {
	for _, volume := range pod.Volumes {
		if volume.Name != name || volume.Projected == nil {
			continue
		}
		token := false
		rootCA := false
		for _, source := range volume.Projected.Sources {
			if source.ServiceAccountToken != nil && source.ServiceAccountToken.Path == "token" &&
				source.ServiceAccountToken.ExpirationSeconds >= 600 &&
				source.ServiceAccountToken.ExpirationSeconds <= 3600 {
				token = true
			}
			if source.ConfigMap != nil && source.ConfigMap.Name == "kube-root-ca.crt" {
				for _, item := range source.ConfigMap.Items {
					if item.Key == "ca.crt" && item.Path == "ca.crt" {
						rootCA = true
					}
				}
			}
		}
		return token && rootCA
	}
	return false
}

func containsValue(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func labV2Directory(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve lab-v2 deployment directory")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", "deploy", "lab-v2"))
}
