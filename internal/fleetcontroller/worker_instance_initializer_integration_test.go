//go:build integration

package fleetcontroller_test

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontroller"
	corev1 "k8s.io/api/core/v1"
)

const initializerTestImage = "docker.io/library/busybox@sha256:7a3ebe5bfd1a4a19797d20b0c0bb39d44393e9a03fd852c0865b0f540d868df0"

func TestWorkerInstanceInitializersExecuteOnFreshAndRetainedVolumes(t *testing.T) {
	bundle, err := fleetcontroller.BuildH3WorkerBundleActuation(h3BundleSpec())
	if err != nil {
		t.Fatal(err)
	}
	pods, _, err := fleetcontroller.MaterializeWorkerInstanceLaunchResources(bundle)
	if err != nil {
		t.Fatal(err)
	}
	pod := pods[0]
	volume := "vela-initializer-test-" + uuid.NewString()
	runInitializerDocker(t, "volume", "create", volume)
	t.Cleanup(func() { runInitializerDocker(t, "volume", "rm", volume) })

	// Docker volume subpaths preserve Linux ownership across separate containers.
	// Projected files are synthetic, root-owned 0400 values as in the Pod.
	setup := []string{"set -eu"}
	for _, v := range pod.Spec.Volumes {
		setup = append(setup, "mkdir -p /fixture/"+v.Name)
	}
	for name, files := range map[string][]string{
		"stage-worker-control-projected":       {"ca.crt", "tls.crt", "tls.key"},
		"stage-worker-authority-projected":     {"keyring.json"},
		"artifact-store-credentials-projected": {"access-key-id", "secret-access-key"},
		"artifact-store-ca-projected":          {"ca.crt"},
		"model-runtime-verifier-projected":     {"verifier-keyring.json"},
	} {
		for _, file := range files {
			path := "/fixture/" + name + "/" + file
			setup = append(setup, "printf '%s' test-fixture > "+path, "chmod 0400 "+path)
		}
	}
	runInitializerDocker(t, "run", "--rm", "--platform", "linux/amd64", "--network", "none", "--read-only",
		"--cap-drop", "ALL", "--security-opt", "no-new-privileges",
		"--mount", "type=volume,src="+volume+",dst=/fixture", initializerTestImage,
		"/bin/sh", "-ec", strings.Join(setup, "\n"))

	for pass := range 2 {
		for _, initializer := range pod.Spec.InitContainers {
			t.Run(fmt.Sprintf("pass-%d/%s", pass, initializer.Name), func(t *testing.T) {
				runMaterializedInitializer(t, volume, initializer)
			})
			if t.Failed() {
				return
			}
		}
		check := `set -eu
test "$(stat -c '%u:%g:%a' /fixture/scratch)" = 10001:10001:700
for directory in production-state inputs input-transfer-journal outputs materialization-journal model-runtime-epochs; do
  test "$(stat -c '%u:%g:%a' /fixture/scratch/$directory)" = 10001:10001:700
done
test ! -e /fixture/scratch/model-runtime
test "$(stat -c '%u:%g:%a' /fixture/model-runtime-private/launch.json)" = 10001:10001:400
test "$(stat -c '%u:%g:%a' /fixture/model-runtime-private/authority/verifier-keyring.json)" = 10001:10001:400
test "$(stat -c '%u:%g:%a' /fixture/stage-worker-private/authority/keyring.json)" = 10001:10001:400
test ! -e /fixture/model-runtime-private/authority/keyring.json
`
		if pass == 0 {
			check += `printf '%s' retained-history > /fixture/scratch/model-runtime-epochs/retained
chmod 0600 /fixture/scratch/model-runtime-epochs/retained
printf '%s' retained-input > /fixture/scratch/inputs/retained
chmod 0600 /fixture/scratch/inputs/retained
`
		} else {
			check += `test "$(cat /fixture/scratch/model-runtime-epochs/retained)" = retained-history
test "$(stat -c '%u:%g:%a' /fixture/scratch/model-runtime-epochs/retained)" = 10001:10001:600
test "$(cat /fixture/scratch/inputs/retained)" = retained-input
test "$(stat -c '%u:%g:%a' /fixture/scratch/inputs/retained)" = 10001:10001:600
`
		}
		runInitializerDocker(t, "run", "--rm", "--platform", "linux/amd64", "--network", "none", "--read-only",
			"--user", "10001:10001", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
			"--mount", "type=volume,src="+volume+",dst=/fixture", initializerTestImage, "/bin/sh", "-ec", check)
	}
}

func runMaterializedInitializer(t *testing.T, volume string, initializer corev1.Container) {
	t.Helper()
	security := initializer.SecurityContext
	if security == nil || security.RunAsUser == nil || security.RunAsGroup == nil ||
		security.AllowPrivilegeEscalation == nil || *security.AllowPrivilegeEscalation ||
		security.ReadOnlyRootFilesystem == nil || !*security.ReadOnlyRootFilesystem || security.Capabilities == nil {
		t.Fatal("initializer omitted its restricted security context")
	}
	arguments := []string{"run", "--rm", "--platform", "linux/amd64", "--network", "none", "--read-only",
		"--security-opt", "no-new-privileges", "--user",
		strconv.FormatInt(*security.RunAsUser, 10) + ":" + strconv.FormatInt(*security.RunAsGroup, 10)}
	for _, capability := range security.Capabilities.Drop {
		arguments = append(arguments, "--cap-drop", string(capability))
	}
	for _, capability := range security.Capabilities.Add {
		arguments = append(arguments, "--cap-add", string(capability))
	}
	for _, mount := range initializer.VolumeMounts {
		value := "type=volume,src=" + volume + ",dst=" + mount.MountPath + ",volume-subpath=" + mount.Name
		if mount.ReadOnly {
			value += ",readonly"
		}
		arguments = append(arguments, "--mount", value)
	}
	for _, variable := range initializer.Env {
		if variable.ValueFrom != nil {
			t.Fatalf("unresolved initializer environment %s", variable.Name)
		}
		arguments = append(arguments, "--env", variable.Name+"="+variable.Value)
	}
	arguments = append(arguments, initializerTestImage)
	arguments = append(arguments, initializer.Command...)
	arguments = append(arguments, initializer.Args...)
	runInitializerDocker(t, arguments...)
}

func runInitializerDocker(t *testing.T, arguments ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "docker", arguments...).CombinedOutput()
	if err != nil {
		t.Fatalf("initializer Docker command failed: %v\n%s", err, output)
	}
	return strings.TrimSpace(string(output))
}
