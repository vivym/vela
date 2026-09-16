package nodeagent

import (
	"slices"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/vivym/vela/internal/runtimelaunch"
)

func TestRuntimeRemoteCLIConfiguration(t *testing.T) {
	for _, scenario := range []string{"valid", "image-path-default", "environment-order", "missing-path", "wrong-path", "wrong-mode", "flag-alias", "extra-arg", "image-command", "environment-missing", "environment-extra", "environment-duplicate", "environment-hostname", "environment-path", "environment-home", "image-environment", "image-duplicate", "tty", "cwd", "image-cwd"} {
		t.Run(scenario, func(t *testing.T) {
			image := ocispec.ImageConfig{Entrypoint: []string{"/vela-model-runtime", "serve-remote", "--bootstrap-file", "/runtime-config/bootstrap.json"}}
			task := &specs.Process{Args: slices.Clone(image.Entrypoint), Env: []string{"PATH=" + runtimeRemoteCLIPath, "HOSTNAME=planned-pod", "HOME=/"}, Cwd: "/"}
			switch scenario {
			case "image-path-default":
				image.Env = []string{"PATH=" + runtimeRemoteCLIPath}
			case "environment-order":
				slices.Reverse(task.Env)
			case "missing-path":
				task.Args = task.Args[:3]
			case "wrong-path":
				task.Args[3] = "/other/bootstrap.json"
			case "wrong-mode":
				task.Args[1] = "serve"
			case "flag-alias":
				task.Args[2] = "-bootstrap-file"
			case "extra-arg":
				task.Args = append(task.Args, "--bootstrap-file", task.Args[3])
			case "image-command":
				image.Entrypoint[1] = "serve"
				task.Args = slices.Clone(image.Entrypoint)
			case "environment-missing":
				task.Env = task.Env[:1]
			case "environment-extra":
				task.Env = append(task.Env, "GODEBUG=asyncpreemptoff=1")
			case "environment-duplicate":
				task.Env[1] = task.Env[0]
			case "environment-hostname":
				task.Env[1] = "HOSTNAME=other-pod"
			case "environment-path":
				task.Env[0] = "PATH=/untrusted"
			case "environment-home":
				task.Env[2] = "HOME=/untrusted"
			case "image-environment":
				image.Env = []string{"LD_PRELOAD=/untrusted.so"}
			case "image-duplicate":
				image.Env = []string{task.Env[0], task.Env[0]}
			case "tty":
				task.Terminal = true
			case "cwd":
				task.Cwd = "/tmp"
			case "image-cwd":
				image.WorkingDir = "/tmp"
			}
			valid := scenario == "valid" || scenario == "image-path-default" || scenario == "environment-order"
			if err := checkRemoteCLIConfiguration(image, task, "/runtime-config/bootstrap.json", "planned-pod"); (err == nil) != valid {
				t.Fatalf("configuration acceptance=%v: %v", valid, err)
			}
		})
	}
}

func TestRuntimeRemoteCLIKubernetesEntrypoint(t *testing.T) {
	image := ocispec.ImageConfig{Entrypoint: []string{runtimelaunch.Entrypoint, "runtime"}, WorkingDir: "/"}
	task := &specs.Process{Args: slices.Clone(image.Entrypoint), Env: []string{"HOME=/", "HOSTNAME=planned-pod", "PATH=" + runtimeRemoteCLIPath}, Cwd: "/"}
	task.Env = append(task.Env, runtimelaunch.DisabledServiceEnvironment()...)
	if err := checkRemoteCLIConfiguration(image, task, runtimelaunch.Bootstrap, "planned-pod"); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{runtimelaunch.Entrypoint, "worker"}, {runtimelaunch.Entrypoint, "runtime", "extra"}, {"/other/wrapper", "runtime"}} {
		image.Entrypoint = args
		task.Args = slices.Clone(args)
		if err := checkRemoteCLIConfiguration(image, task, runtimelaunch.Bootstrap, "planned-pod"); err == nil {
			t.Fatal("accepted unapproved stub", args)
		}
	}
}

func TestRuntimeRemoteCLICDIEnvironmentRequiresIndependentExactPolicy(t *testing.T) {
	image := ocispec.ImageConfig{Entrypoint: []string{runtimelaunch.Entrypoint, "runtime"}, WorkingDir: "/"}
	base := []string{"HOME=/", "HOSTNAME=planned-pod", "PATH=" + runtimeRemoteCLIPath}
	base = append(base, runtimelaunch.DisabledServiceEnvironment()...)
	extra := []string{"NVIDIA_VISIBLE_DEVICES=void", "NVIDIA_CTK_LIBCUDA_DIR=/usr/lib/x86_64-linux-gnu"}
	task := &specs.Process{Args: slices.Clone(image.Entrypoint), Env: append(slices.Clone(base), extra...), Cwd: "/"}
	if err := checkRemoteCLIConfiguration(image, task, runtimelaunch.Bootstrap, "planned-pod"); err == nil {
		t.Fatal("unapproved CDI environment accepted")
	}
	if err := checkRemoteCLIConfigurationWithEnvironment(image, task, runtimelaunch.Bootstrap, "planned-pod", extra); err != nil {
		t.Fatal(err)
	}
	for _, changed := range [][]string{{"LD_PRELOAD=/untrusted.so"}, {extra[0], extra[0]}, {"NVIDIA_VISIBLE_DEVICES=all"}, {"NVIDIA_CTK_LIBCUDA_DIR=/untrusted"}} {
		task.Env = append(slices.Clone(base), changed...)
		if err := checkRemoteCLIConfigurationWithEnvironment(image, task, runtimelaunch.Bootstrap, "planned-pod", changed); err == nil {
			t.Fatal("policy broadened the remote CLI", changed)
		}
	}
}

func TestRuntimeRemoteCLIRejectsAmbientKubernetesEnvironment(t *testing.T) {
	for _, scenario := range []string{"endpoint", "missing-override", "extra-service", "duplicate-home", "duplicate-override", "loader", "inherited-cuda"} {
		t.Run(scenario, func(t *testing.T) {
			image := ocispec.ImageConfig{Entrypoint: []string{runtimelaunch.Entrypoint, "runtime"}, Env: []string{"PATH=" + runtimeRemoteCLIPath, "HOME=/"}}
			task := &specs.Process{Args: slices.Clone(image.Entrypoint), Env: append([]string{"PATH=" + runtimeRemoteCLIPath, "HOME=/", "HOSTNAME=planned-pod"}, runtimelaunch.DisabledServiceEnvironment()...), Cwd: "/"}
			switch scenario {
			case "endpoint":
				task.Env[3] += "10.43.0.1"
			case "missing-override":
				task.Env = task.Env[:len(task.Env)-1]
			case "extra-service":
				task.Env = append(task.Env, "ANOTHER_SERVICE_HOST=")
			case "duplicate-home":
				task.Env = append(task.Env, "HOME=/")
			case "duplicate-override":
				task.Env = append(task.Env, task.Env[3])
			case "loader":
				task.Env = append(task.Env, "LD_LIBRARY_PATH=/unapproved")
			case "inherited-cuda":
				image.Env = append(image.Env, "NVIDIA_VISIBLE_DEVICES=all")
			}
			if err := checkRemoteCLIConfiguration(image, task, runtimelaunch.Bootstrap, "planned-pod"); err == nil {
				t.Fatal("accepted ambient or duplicate environment")
			}
		})
	}
}
