package fleetcontroller_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontroller"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestKubernetesActuatorMaterializesPerGPUH3WorkerInstances(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register core Kubernetes scheme: %v", err)
	}
	client := dynamicfake.NewSimpleDynamicClient(scheme)
	resources, err := fleetcontroller.NewKubernetesResources(client, "vela-system")
	if err != nil {
		t.Fatalf("create Kubernetes resources: %v", err)
	}
	actuator, err := fleetcontroller.NewWorkerInstanceActuator(resources)
	if err != nil {
		t.Fatalf("create WorkerInstance actuator: %v", err)
	}
	bundle, err := fleetcontroller.BuildH3WorkerBundleActuation(h3BundleSpec())
	if err != nil {
		t.Fatalf("build H3 WorkerBundle actuation: %v", err)
	}
	if len(bundle.WorkerInstances) != 8 {
		t.Fatalf("H3 WorkerInstance count = %d, want 8", len(bundle.WorkerInstances))
	}
	if got := bundle.WorkerInstances[0]; got.CapacitySlots != 1 ||
		len(got.ModelRuntimes) != 2 || got.ModelRuntimes[0].Component != "ENCODER" ||
		got.ModelRuntimes[1].Component != "VAE_DECODER" {
		t.Fatalf("AUX WorkerInstance = %#v, want one slot with Encoder/VAE", got)
	}
	for index, worker := range bundle.WorkerInstances[1:] {
		if worker.CapacitySlots != 1 || len(worker.Members) != 1 ||
			worker.Members[0].DeviceCount != 1 || len(worker.ModelRuntimes) != 1 ||
			worker.ModelRuntimes[0].Component != "DIT" {
			t.Fatalf("DiT WorkerInstance %d = %#v, want one member/card/runtime/slot", index, worker)
		}
	}

	result, err := actuator.Actuate(context.Background(), bundle)
	if err != nil || result.CreatedPods != 8 || result.Converged {
		t.Fatalf("actuate H3 WorkerBundle = %#v error=%v", result, err)
	}
	pods, err := client.Resource(schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "pods",
	}).Namespace("vela-system").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(pods.Items) != 8 {
		t.Fatalf("list H3 WorkerInstance Pods count=%d error=%v", len(pods.Items), err)
	}
	seenWorkers := make(map[string]struct{}, 8)
	var auxPod corev1.Pod
	for index := range pods.Items {
		var pod corev1.Pod
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
			pods.Items[index].Object,
			&pod,
		); err != nil {
			t.Fatalf("decode WorkerInstance Pod: %v", err)
		}
		workerID := pod.Labels["vela.ai/worker-instance-id"]
		if workerID == "" {
			t.Fatalf("WorkerInstance Pod %q has no WorkerInstance identity", pod.Name)
		}
		if _, duplicate := seenWorkers[workerID]; duplicate {
			t.Fatalf("H3 standard layout duplicated WorkerInstance %s", workerID)
		}
		seenWorkers[workerID] = struct{}{}
		runtimeContainer := requireContainer(t, pod.Spec.Containers, "model-runtime")
		gpu := runtimeContainer.Resources.Limits["nvidia.com/gpu"]
		if gpu.String() != "1" {
			t.Fatalf("WorkerInstance Pod %q GPU limit = %s, want 1", pod.Name,
				gpu.String())
		}
		if pod.Labels["vela.ai/worker-role"] == "aux" {
			auxPod = pod
		}
	}
	if auxPod.Name == "" {
		t.Fatal("H3 AUX WorkerInstance Pod is missing")
	}
	var runtimes []fleetcontroller.ModelRuntimeProcess
	if err := json.Unmarshal(
		[]byte(requireEnvironment(t, requireContainer(t, auxPod.Spec.Containers, "model-runtime"), "VELA_MODEL_RUNTIMES_JSON")),
		&runtimes,
	); err != nil || len(runtimes) != 2 || runtimes[0].Component != "ENCODER" ||
		runtimes[1].Component != "VAE_DECODER" {
		t.Fatalf("AUX runtime processes = %#v error=%v", runtimes, err)
	}

	replayed, err := actuator.Actuate(context.Background(), bundle)
	if err != nil || !replayed.Converged || replayed.CreatedPods != 0 {
		t.Fatalf("replay H3 WorkerBundle actuation = %#v error=%v", replayed, err)
	}
}

func TestKubernetesActuatorMaterializesMultiMemberAuthority(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register core Kubernetes scheme: %v", err)
	}
	client := dynamicfake.NewSimpleDynamicClient(scheme)
	resources, err := fleetcontroller.NewKubernetesResources(client, "vela-system")
	if err != nil {
		t.Fatalf("create Kubernetes resources: %v", err)
	}
	actuator, err := fleetcontroller.NewWorkerInstanceActuator(resources)
	if err != nil {
		t.Fatalf("create WorkerInstance actuator: %v", err)
	}
	workerID := uuid.MustParse("49300000-0000-0000-0000-000000000030")
	bundle := fleetcontroller.WorkerBundleActuation{
		SchemaVersion:          1,
		PlanRevisionID:         uuid.MustParse("49300000-0000-0000-0000-000000000001"),
		WorkerBundleID:         uuid.MustParse("49300000-0000-0000-0000-000000000002"),
		RevisionDigest:         digestString('a'),
		Namespace:              "vela-system",
		InitImage:              pinnedImage("busybox", 'b'),
		WorkerAgentImage:       pinnedImage("vela-worker-agent", 'c'),
		RuntimeImage:           pinnedImage("vela-stage-runtime", 'd'),
		ArtifactStoreTLSSecret: "artifact-store-ca-r1",
		WorkerRuntimeConfigMap: "worker-runtime-r1",
		WorkerControlTLSSecret: "worker-control-tls-r1",
		WorkerInstances: []fleetcontroller.WorkerInstanceActuation{{
			ID:                      workerID,
			InstanceEpoch:           1,
			WorkerProfileRevisionID: uuid.MustParse("49300000-0000-0000-0000-000000000031"),
			CapacityPoolID:          uuid.MustParse("49300000-0000-0000-0000-000000000032"),
			Role:                    "llm",
			CapacitySlots:           1,
			ModelRuntimes: []fleetcontroller.ModelRuntimeProcess{{
				Component: "LLM", ModelComponentRevision: "future-llm-v1",
				RuntimeIdentity: "future-llm-runtime-v1", Command: []string{"/opt/vela/bin/llm-runtime"},
			}},
			Members: []fleetcontroller.WorkerMemberActuation{
				{
					ID:  uuid.MustParse("49300000-0000-0000-0000-000000000033"),
					Key: "member-0", NodeIdentity: "llm-node-a", DeviceCount: 2,
				},
				{
					ID:  uuid.MustParse("49300000-0000-0000-0000-000000000034"),
					Key: "member-1", NodeIdentity: "llm-node-b", DeviceCount: 2,
				},
			},
		}},
	}
	result, err := actuator.Actuate(context.Background(), bundle)
	if err != nil || result.CreatedPods != 2 {
		t.Fatalf("actuate multi-member WorkerInstance = %#v error=%v", result, err)
	}
	pods, err := client.Resource(schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "pods",
	}).Namespace("vela-system").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(pods.Items) != 2 {
		t.Fatalf("multi-member Pod count=%d error=%v", len(pods.Items), err)
	}
	seenMembers := map[string]bool{}
	for _, object := range pods.Items {
		if object.GetLabels()["vela.ai/worker-instance-id"] != workerID.String() {
			t.Fatalf("member Pod %q WorkerInstance = %q", object.GetName(), object.GetLabels()["vela.ai/worker-instance-id"])
		}
		member := object.GetLabels()["vela.ai/worker-member-key"]
		seenMembers[member] = true
		var pod corev1.Pod
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &pod); err != nil {
			t.Fatalf("decode member Pod: %v", err)
		}
		gpu := requireContainer(t, pod.Spec.Containers, "model-runtime").Resources.Limits["nvidia.com/gpu"]
		if gpu.String() != "2" {
			t.Fatalf("member Pod %q GPU limit = %s, want 2", pod.Name, gpu.String())
		}
	}
	if !seenMembers["member-0"] || !seenMembers["member-1"] {
		t.Fatalf("multi-member Pods = %#v", seenMembers)
	}
}

func h3BundleSpec() fleetcontroller.H3WorkerBundleSpec {
	devices := [8]fleetcontroller.DeviceConstraint{}
	for index := range devices {
		devices[index] = fleetcontroller.DeviceConstraint{
			GPUUUID: "GPU-00000000-0000-0000-0000-00000000000" + string(rune('0'+index)),
			PCIBDF:  "0000:4" + string(rune('1'+index)) + ":00.0",
		}
	}
	return fleetcontroller.H3WorkerBundleSpec{
		SchemaVersion:  1,
		PlanRevisionID: uuid.MustParse("49300000-0000-0000-0000-000000000001"),
		WorkerBundleID: uuid.MustParse("49300000-0000-0000-0000-000000000002"),
		RevisionDigest: digestString('a'),
		Namespace:      "vela-system", NodeIdentity: "h3-node-01",
		AuxCapacityPoolID:          uuid.MustParse("49300000-0000-0000-0000-000000000003"),
		DiTCapacityPoolID:          uuid.MustParse("49300000-0000-0000-0000-000000000004"),
		AuxWorkerProfileRevisionID: uuid.MustParse("49300000-0000-0000-0000-000000000005"),
		DiTWorkerProfileRevisionID: uuid.MustParse("49300000-0000-0000-0000-000000000006"),
		InitImage:                  pinnedImage("busybox", 'b'),
		WorkerAgentImage:           pinnedImage("vela-worker-agent", 'c'),
		RuntimeImage:               pinnedImage("vela-h3-stage-runtime", 'd'),
		ArtifactStoreTLSSecret:     "artifact-store-ca-r1",
		WorkerRuntimeConfigMap:     "worker-runtime-r1",
		WorkerControlTLSSecret:     "worker-control-tls-r1",
		Devices:                    devices,
		Encoder: fleetcontroller.ModelRuntimeProcess{
			Component: "ENCODER", ModelComponentRevision: "h3-encoder-v1",
			RuntimeIdentity: "h3-encoder-runtime-v1", Command: []string{"/opt/vela/bin/h3-encoder"},
		},
		DiT: fleetcontroller.ModelRuntimeProcess{
			Component: "DIT", ModelComponentRevision: "h3-dit-v1",
			RuntimeIdentity: "h3-dit-runtime-v1", Command: []string{"/opt/vela/bin/h3-dit"},
		},
		VAEDecoder: fleetcontroller.ModelRuntimeProcess{
			Component: "VAE_DECODER", ModelComponentRevision: "h3-vae-decoder-v1",
			RuntimeIdentity: "h3-vae-decoder-runtime-v1", Command: []string{"/opt/vela/bin/h3-vae-decoder"},
		},
	}
}

func requireContainer(t *testing.T, containers []corev1.Container, name string) corev1.Container {
	t.Helper()
	for _, container := range containers {
		if container.Name == name {
			return container
		}
	}
	t.Fatalf("container %q is missing", name)
	return corev1.Container{}
}

func requireEnvironment(t *testing.T, container corev1.Container, name string) string {
	t.Helper()
	for _, variable := range container.Env {
		if variable.Name == name {
			return variable.Value
		}
	}
	t.Fatalf("container %q environment %q is missing", container.Name, name)
	return ""
}

func pinnedImage(name string, digit rune) string {
	return "ghcr.io/vivym/" + name + "@sha256:" + digestString(digit)
}

func digestString(digit rune) string {
	encoded := make([]rune, 64)
	for index := range encoded {
		encoded[index] = digit
	}
	return string(encoded)
}
