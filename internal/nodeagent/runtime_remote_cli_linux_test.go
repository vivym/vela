package nodeagent

import (
	"slices"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/opencontainers/runtime-spec/specs-go"
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
