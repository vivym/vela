package deploymentcontract

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const runnerScratchRoot = "/var/lib/vela/worker/scratch"

type workerAgentManifest struct {
	Kind string `yaml:"kind"`
	Spec struct {
		Template struct {
			Spec workerAgentPodSpec `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

type workerAgentPodSpec struct {
	HostPID               *bool                  `yaml:"hostPID"`
	HostNetwork           *bool                  `yaml:"hostNetwork"`
	ShareProcessNamespace *bool                  `yaml:"shareProcessNamespace"`
	InitContainers        []workerAgentContainer `yaml:"initContainers"`
	Containers            []workerAgentContainer `yaml:"containers"`
	Volumes               []workerAgentVolume    `yaml:"volumes"`
}

type workerAgentContainer struct {
	Name         string                   `yaml:"name"`
	Command      []string                 `yaml:"command"`
	Env          []workerAgentEnv         `yaml:"env"`
	VolumeMounts []workerAgentVolumeMount `yaml:"volumeMounts"`
}

type workerAgentEnv struct {
	Name      string                     `yaml:"name"`
	ValueFrom *workerAgentEnvValueSource `yaml:"valueFrom"`
}

type workerAgentEnvValueSource struct {
	ConfigMapKeyRef *workerAgentConfigMapKeySelector `yaml:"configMapKeyRef"`
	FieldRef        *workerAgentObjectFieldSelector  `yaml:"fieldRef"`
}

type workerAgentConfigMapKeySelector struct {
	Name string `yaml:"name"`
	Key  string `yaml:"key"`
}

type workerAgentObjectFieldSelector struct {
	APIVersion string `yaml:"apiVersion"`
	FieldPath  string `yaml:"fieldPath"`
}

type workerAgentVolumeMount struct {
	Name      string `yaml:"name"`
	MountPath string `yaml:"mountPath"`
	SubPath   string `yaml:"subPath"`
}

type workerAgentVolume struct {
	Name     string               `yaml:"name"`
	EmptyDir *workerAgentEmptyDir `yaml:"emptyDir"`
	HostPath *workerAgentHostPath `yaml:"hostPath"`
}

type workerAgentEmptyDir struct {
	Medium    string `yaml:"medium"`
	SizeLimit string `yaml:"sizeLimit"`
}

type workerAgentHostPath struct {
	Path string `yaml:"path"`
	Type string `yaml:"type"`
}

func TestWorkerAgentManifestExcludesRecoveryQuarantineFromRunnerMountNamespace(t *testing.T) {
	manifest := loadWorkerAgentManifest(t)
	if manifest.Kind != "DaemonSet" {
		t.Fatalf("worker manifest kind = %q", manifest.Kind)
	}
	pod := manifest.Spec.Template.Spec
	if pod.HostPID == nil || *pod.HostPID {
		t.Fatal("Worker Pod must explicitly disable the host PID namespace")
	}
	if pod.HostNetwork == nil || *pod.HostNetwork {
		t.Fatal("Worker Pod must explicitly disable the host network namespace")
	}
	if pod.ShareProcessNamespace == nil || *pod.ShareProcessNamespace {
		t.Fatal("Worker Pod must explicitly disable the shared process namespace")
	}

	agent := requireContainer(t, pod.Containers, "worker-agent")
	requireExactMount(t, agent, workerAgentVolumeMount{
		Name: "scratch", MountPath: runnerScratchRoot,
	})

	runner := requireContainer(t, pod.Containers, "h3-runner")
	requireExactMount(t, runner, workerAgentVolumeMount{
		Name: "runner-scratch-view", MountPath: runnerScratchRoot,
	})
	requireExactMount(t, runner, workerAgentVolumeMount{
		Name: "scratch", MountPath: runnerScratchRoot + "/runner-state", SubPath: "runner-state",
	})
	requireExactMount(t, runner, workerAgentVolumeMount{
		Name: "scratch", MountPath: runnerScratchRoot + "/outputs", SubPath: "outputs",
	})
	for _, mount := range runner.VolumeMounts {
		if mount.Name == "scratch" && mount.SubPath != "runner-state" && mount.SubPath != "outputs" {
			t.Fatalf("Runner received an unapproved scratch mount: %#v", mount)
		}
		if strings.HasPrefix(mount.MountPath, runnerScratchRoot+"/recovery") {
			t.Fatalf("Runner can see Worker Local Recovery State: %#v", mount)
		}
	}

	view := requireVolume(t, pod.Volumes, "runner-scratch-view")
	if view.EmptyDir == nil || view.HostPath != nil || view.EmptyDir.Medium != "Memory" ||
		view.EmptyDir.SizeLimit != "1Mi" {
		t.Fatalf("Runner scratch view volume = %#v", view)
	}
	scratch := requireVolume(t, pod.Volumes, "scratch")
	if scratch.HostPath == nil || scratch.EmptyDir != nil ||
		scratch.HostPath.Path != runnerScratchRoot || scratch.HostPath.Type != "Directory" {
		t.Fatalf("Worker scratch volume = %#v", scratch)
	}
	for _, volume := range pod.Volumes {
		if volume.Name != "scratch" && volume.HostPath != nil &&
			strings.HasPrefix(volume.HostPath.Path, runnerScratchRoot) {
			t.Fatalf("alternate volume exposes Worker scratch: %#v", volume)
		}
	}
	initializer := requireContainer(t, pod.InitContainers, "runner-socket-permissions")
	requireExactMount(t, initializer, workerAgentVolumeMount{
		Name: "runner-scratch-view", MountPath: runnerScratchRoot,
	})
	command := strings.Join(initializer.Command, "\n")
	for _, required := range []string{
		"mkdir -p /var/lib/vela/worker/scratch/runner-state /var/lib/vela/worker/scratch/outputs",
		"chmod 0700 /var/lib/vela/worker/scratch",
		"chown -R 10001:10001 /var/lib/vela/worker/scratch",
	} {
		if !strings.Contains(command, required) {
			t.Fatalf("scratch-view initializer omitted %q", required)
		}
	}
}

func TestWorkerAgentManifestBindsCertifiedOutputCleanupThroughput(t *testing.T) {
	manifest := loadWorkerAgentManifest(t)
	agent := requireContainer(t, manifest.Spec.Template.Spec.Containers, "worker-agent")
	requireConfigMapEnv(
		t, agent, "VELA_WORKER_OUTPUT_CLEANUP_MIN_BYTES_PER_SECOND",
		"vela-worker-runtime", "output-cleanup-min-bytes-per-second",
	)
}

func TestWorkerAgentManifestBindsNodeIdentityFromScheduledNode(t *testing.T) {
	manifest := loadWorkerAgentManifest(t)
	agent := requireContainer(t, manifest.Spec.Template.Spec.Containers, "worker-agent")
	for _, environment := range agent.Env {
		if environment.Name != "VELA_WORKER_NODE_IDENTITY" {
			continue
		}
		if environment.ValueFrom == nil || environment.ValueFrom.FieldRef == nil ||
			environment.ValueFrom.ConfigMapKeyRef != nil ||
			environment.ValueFrom.FieldRef.APIVersion != "v1" ||
			environment.ValueFrom.FieldRef.FieldPath != "spec.nodeName" {
			t.Fatalf("Worker node identity environment = %#v", environment)
		}
		return
	}
	t.Fatal("worker-agent is missing VELA_WORKER_NODE_IDENTITY")
}

func TestWorkerAgentManifestBindsRunnerOutputLimitToAttemptQuota(t *testing.T) {
	manifest := loadWorkerAgentManifest(t)
	agent := requireContainer(t, manifest.Spec.Template.Spec.Containers, "worker-agent")
	requireConfigMapEnv(
		t, agent, "VELA_WORKER_ATTEMPT_QUOTA_BYTES",
		"vela-worker-runtime", "attempt-quota-bytes",
	)
	runner := requireContainer(t, manifest.Spec.Template.Spec.Containers, "h3-runner")
	requireConfigMapEnv(
		t, runner, "VELA_RUNNER_MAX_OUTPUT_BYTES",
		"vela-worker-runtime", "attempt-quota-bytes",
	)
}

func requireConfigMapEnv(
	t *testing.T,
	container workerAgentContainer,
	envName string,
	configMapName string,
	key string,
) {
	t.Helper()
	for _, env := range container.Env {
		if env.Name != envName {
			continue
		}
		if env.ValueFrom == nil || env.ValueFrom.ConfigMapKeyRef == nil ||
			env.ValueFrom.ConfigMapKeyRef.Name != configMapName ||
			env.ValueFrom.ConfigMapKeyRef.Key != key {
			t.Fatalf("container %q env %q = %#v", container.Name, envName, env)
		}
		return
	}
	t.Fatalf("container %q is missing env %q", container.Name, envName)
}

func loadWorkerAgentManifest(t *testing.T) workerAgentManifest {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve deployment contract test path")
	}
	path := filepath.Join(filepath.Dir(currentFile), "..", "..", "deploy", "worker-agent", "daemonset.yaml")
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Worker Agent manifest: %v", err)
	}
	var manifest workerAgentManifest
	if err := yaml.Unmarshal(payload, &manifest); err != nil {
		t.Fatalf("parse Worker Agent manifest: %v", err)
	}
	return manifest
}

func requireContainer(
	t *testing.T,
	containers []workerAgentContainer,
	name string,
) workerAgentContainer {
	t.Helper()
	for _, container := range containers {
		if container.Name == name {
			return container
		}
	}
	t.Fatalf("container %q is missing", name)
	return workerAgentContainer{}
}

func requireExactMount(
	t *testing.T,
	container workerAgentContainer,
	want workerAgentVolumeMount,
) {
	t.Helper()
	for _, mount := range container.VolumeMounts {
		if mount == want {
			return
		}
	}
	t.Fatalf("container %q mounts = %#v, missing %#v", container.Name, container.VolumeMounts, want)
}

func requireVolume(
	t *testing.T,
	volumes []workerAgentVolume,
	name string,
) workerAgentVolume {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name {
			return volume
		}
	}
	t.Fatalf("volume %q is missing", name)
	return workerAgentVolume{}
}
