//go:build linux

package main

import (
	"crypto/sha256"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func boolPointer(value bool) *bool    { return &value }
func int64Pointer(value int64) *int64 { return &value }

func TestProductionCommandUsesApprovedImageEntrypoints(t *testing.T) {
	runtime, args, err := productionCommand(&corev1.Container{Name: "model-runtime", Args: []string{"serve"}})
	if err != nil || len(runtime) != 1 || runtime[0] != "/usr/local/bin/vela-model-runtime" || len(args) != 1 || args[0] != "serve" {
		t.Fatalf("runtime command=%v args=%v err=%v", runtime, args, err)
	}
	worker, _, err := productionCommand(&corev1.Container{Name: "stage-worker-agent"})
	if err != nil || len(worker) != 1 || worker[0] != "/usr/local/bin/vela-stage-worker-agent" {
		t.Fatalf("worker command=%v err=%v", worker, err)
	}
}

func TestProductionCommandRejectsUnknownEntrypoint(t *testing.T) {
	if _, _, err := productionCommand(&corev1.Container{Name: "unknown"}); err == nil {
		t.Fatal("unknown production container entrypoint was accepted")
	}
}

func TestProductionRuntimeBootstrapPathNormalizesOCIArgv(t *testing.T) {
	container := &corev1.Container{
		Name:    "model-runtime",
		Command: []string{"/usr/local/bin/vela-model-runtime"},
		Args:    []string{"serve-remote", "--bootstrap-file", "/run/vela-model-runtime-bootstrap/bootstrap.json"},
	}
	path, err := productionRuntimeBootstrapPath(container)
	if err != nil || path != container.Args[2] {
		t.Fatalf("bootstrap path=%q err=%v", path, err)
	}
	container.Command = nil
	if path, err := productionRuntimeBootstrapPath(container); err != nil || path != container.Args[2] {
		t.Fatalf("default entrypoint bootstrap path=%q err=%v", path, err)
	}
	container.Args = []string{"/usr/local/bin/vela-model-runtime", "serve-remote", "--bootstrap-file", container.Args[2]}
	if _, err := productionRuntimeBootstrapPath(container); err == nil {
		t.Fatal("argv[0] duplicated in Args was accepted")
	}
}

func TestLauncherStrictJSONRejectsDuplicateAndTrailingData(t *testing.T) {
	var request struct {
		Version int `json:"version"`
	}
	if err := strictJSON([]byte(`{"version":1,"version":1}`), &request); err == nil {
		t.Fatal("duplicate launcher request field was accepted")
	}
	if err := strictJSON([]byte(`{"version":1}{"version":2}`), &request); err == nil {
		t.Fatal("trailing launcher request JSON was accepted")
	}
	if err := strictJSON([]byte(`{"version":1,"unknown":true}`), &request); err == nil {
		t.Fatal("unknown launcher request field was accepted")
	}
}

func TestRequiredContainersRejectsDuplicateOrUnpinnedImages(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{
		{Name: "model-runtime", Image: "registry.example/runtime@sha256:" + strings.Repeat("a", 64)},
		{Name: "model-runtime", Image: "registry.example/runtime@sha256:" + strings.Repeat("a", 64)},
		{Name: "stage-worker-agent", Image: "registry.example/worker@sha256:" + strings.Repeat("b", 64)},
	}}}
	if _, _, err := requiredContainers(pod); err == nil {
		t.Fatal("duplicate Runtime container was accepted")
	}
	pod.Spec.Containers[1].Name = "other"
	pod.Spec.Containers[2].Image = "registry.example/worker:latest"
	if _, _, err := requiredContainers(pod); err == nil {
		t.Fatal("unpinned Worker image was accepted")
	}
}

func TestRequiredContainersRejectsAdditionalContainers(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{
		{Name: "model-runtime", Image: "registry.example/runtime@sha256:" + strings.Repeat("a", 64)},
		{Name: "stage-worker-agent", Image: "registry.example/worker@sha256:" + strings.Repeat("b", 64)},
		{Name: "sidecar", Image: "registry.example/sidecar@sha256:" + strings.Repeat("c", 64)},
	}}}
	if _, _, err := requiredContainers(pod); err == nil {
		t.Fatal("additional signed Pod container was accepted")
	}
}

func TestRequiredContainersAcceptsSupportedInitContainers(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{
		ShareProcessNamespace: boolPointer(false),
		SecurityContext:       &corev1.PodSecurityContext{RunAsUser: int64Pointer(10001), RunAsGroup: int64Pointer(10001), FSGroup: int64Pointer(10001)},
		InitContainers:        nil,
		Containers: []corev1.Container{
			{Name: "model-runtime", Image: "registry.example/runtime@sha256:" + strings.Repeat("a", 64)},
			{Name: "stage-worker-agent", Image: "registry.example/worker@sha256:" + strings.Repeat("b", 64)},
		},
	}}
	if _, _, err := requiredContainers(pod); err != nil {
		t.Fatalf("supported Pod without init containers rejected: %v", err)
	}
	pod.Spec.InitContainers = []corev1.Container{
		{Name: "stage-worker-private-materialization", Image: "registry.example/init@sha256:" + strings.Repeat("a", 64), Command: []string{"/bin/true"}},
		{Name: "model-runtime-private-materialization", Image: "registry.example/init@sha256:" + strings.Repeat("a", 64), Command: []string{"/bin/true"}},
	}
	if _, _, err := requiredContainers(pod); err != nil {
		t.Fatalf("supported init containers rejected: %v", err)
	}
}

func TestRuntimeStartupContainerSocketPreservesBasename(t *testing.T) {
	got, err := runtimeStartupContainerSocket("/run/vela/startup-smoke.sock")
	if err != nil || got != "/run/vela-node/startup-smoke.sock" {
		t.Fatalf("container startup socket=%q err=%v", got, err)
	}
	if _, err := runtimeStartupContainerSocket("relative.sock"); err == nil {
		t.Fatal("relative startup socket was accepted")
	}
}

func TestProductionObserverSocketpairIsCreatedWithCloseOnExec(t *testing.T) {
	node, child, err := newProductionObserverSocketpair()
	if err != nil {
		t.Fatal(err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(node.Close)
	defer func(cleanup func() error) { _ = cleanup() }(child.Close)
	for _, file := range []*os.File{node, child} {
		flags, err := unix.FcntlInt(file.Fd(), unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC == 0 {
			t.Fatalf("observer custody endpoint is not close-on-exec: flags=%x err=%v", flags, err)
		}
	}
}

func TestValidateProductionContainerRejectsUnsupportedFields(t *testing.T) {
	base := corev1.Container{Name: "model-runtime"}
	cases := []struct {
		name string
		edit func(*corev1.Container)
	}{
		{"working directory", func(c *corev1.Container) { c.WorkingDir = "/work" }},
		{"ports", func(c *corev1.Container) { c.Ports = []corev1.ContainerPort{{ContainerPort: 8080}} }},
		{"resource claims", func(c *corev1.Container) { c.Resources.Claims = []corev1.ResourceClaim{{Name: "gpu"}} }},
		{"lifecycle", func(c *corev1.Container) { c.Lifecycle = &corev1.Lifecycle{} }},
		{"volume subpath", func(c *corev1.Container) {
			c.VolumeMounts = []corev1.VolumeMount{{Name: "v", MountPath: "/v", SubPath: "x"}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			container := base
			tc.edit(&container)
			if err := validateProductionContainer(&container); err == nil {
				t.Fatal("unsupported container field was accepted")
			}
		})
	}
}

func TestProductionResourcesAndSecurityContextAreMapped(t *testing.T) {
	readOnly, noEscalation := true, false
	container := corev1.Container{
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("1Gi")},
		},
		SecurityContext: &corev1.SecurityContext{ReadOnlyRootFilesystem: &readOnly, AllowPrivilegeEscalation: &noEscalation},
	}
	resources, err := productionResources(container.Resources)
	if err != nil || resources.CpuShares != 1000 || resources.CpuQuota != 200000 || resources.MemoryLimitInBytes != 1<<30 {
		t.Fatalf("mapped resources=%+v err=%v", resources, err)
	}
	security, err := productionSecurityContext(container.SecurityContext, &runtimev1.NamespaceOption{}, 10001, 10001, false)
	if err != nil || !security.ReadonlyRootfs || !security.NoNewPrivs {
		t.Fatalf("mapped security=%+v err=%v", security, err)
	}
}

func TestLauncherPathAndVolumeRules(t *testing.T) {
	for _, path := range []string{"", "/", "relative", "/tmp/../vela"} {
		if canonicalAbsolute(path) {
			t.Fatalf("non-canonical path %q was accepted", path)
		}
	}
	for _, path := range []string{"../x", "a/../b", "/absolute", "."} {
		if safeRelativePath(path) {
			t.Fatalf("unsafe relative path %q was accepted", path)
		}
	}
	if !safeRelativePath("config/runtime.json") {
		t.Fatal("safe relative path was rejected")
	}
}

func TestLauncherRequestDigestUsesJSONBytes(t *testing.T) {
	pod, err := json.Marshal(corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "n"}})
	if err != nil || len(pod) == 0 {
		t.Fatalf("marshal Pod: %v", err)
	}
	request := launcherRequest{Version: protocolVersion, ExpectedPod: pod}
	request.ExpectedPodDigest = sha256.Sum256(pod)
	if request.ExpectedPodDigest == [sha256.Size]byte{} {
		t.Fatal("Pod digest unexpectedly empty")
	}
}

func TestImageDigestMatchesCRIForms(t *testing.T) {
	if !imageDigestMatches("sha256:"+strings.Repeat("a", 64), nil, "sha256:"+strings.Repeat("a", 64)) {
		t.Fatal("CRI image ID digest was rejected")
	}
	if !imageDigestMatches("", []string{"registry.example/runtime@sha256:" + strings.Repeat("b", 64)}, "sha256:"+strings.Repeat("b", 64)) {
		t.Fatal("repository digest form was rejected")
	}
	if imageDigestMatches("sha256:"+strings.Repeat("a", 64), nil, "sha256:"+strings.Repeat("b", 64)) {
		t.Fatal("unexpected image digest was accepted")
	}
}
