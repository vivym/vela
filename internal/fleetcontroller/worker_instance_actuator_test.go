package fleetcontroller_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/modelruntime"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestKubernetesActuatorMaterializesPerGPUH3WorkerInstances(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register core Kubernetes scheme: %v", err)
	}
	if err := resourcev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register resource Kubernetes scheme: %v", err)
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
	if got := bundle.WorkerInstances[0]; got.ModelRuntimes[0].CapacityPoolID == got.ModelRuntimes[1].CapacityPoolID {
		t.Fatalf(
			"AUX Encoder/VAE runtime routes share CapacityPool %s",
			got.ModelRuntimes[0].CapacityPoolID,
		)
	}
	for index, worker := range bundle.WorkerInstances[1:] {
		if worker.CapacitySlots != 1 || len(worker.Members) != 1 ||
			worker.Members[0].DeviceCount != 1 || len(worker.ModelRuntimes) != 1 ||
			worker.ModelRuntimes[0].Component != "DIT" {
			t.Fatalf("DiT WorkerInstance %d = %#v, want one member/card/runtime/slot", index, worker)
		}
	}

	result, err := actuator.Actuate(context.Background(), bundle)
	if err != nil || result.CreatedGPUClaims != 8 || result.CreatedPods != 8 ||
		result.CreatedMemberServices != 0 || result.CreatedMemberSecrets != 0 || result.Converged {
		t.Fatalf("actuate H3 WorkerBundle = %#v error=%v", result, err)
	}
	services, err := client.Resource(schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "services",
	}).Namespace("vela-system").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(services.Items) != 0 {
		t.Fatalf("single-member H3 Service count=%d error=%v", len(services.Items), err)
	}
	secrets, err := client.Resource(schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "secrets",
	}).Namespace("vela-system").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(secrets.Items) != 0 {
		t.Fatalf("single-member H3 derived Secret count=%d error=%v", len(secrets.Items), err)
	}
	pods, err := client.Resource(schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "pods",
	}).Namespace("vela-system").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(pods.Items) != 8 {
		t.Fatalf("list H3 WorkerInstance Pods count=%d error=%v", len(pods.Items), err)
	}
	seenWorkers := make(map[string]struct{}, 8)
	expectedMembers := make(map[string]fleetcontroller.WorkerMemberActuation, 8)
	expectedWorkers := make(map[string]fleetcontroller.WorkerInstanceActuation, 8)
	for _, worker := range bundle.WorkerInstances {
		expectedMembers[worker.ID.String()] = worker.Members[0]
		expectedWorkers[worker.ID.String()] = worker
	}
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
		stageAgent := requireContainer(t, pod.Spec.Containers, "stage-worker-agent")
		controlVolume := requireVolume(t, pod.Spec.Volumes, "stage-worker-control-projected")
		if controlVolume.Secret == nil || len(controlVolume.Secret.Items) != 3 ||
			controlVolume.Secret.Items[0].Key != "ca.crt" ||
			controlVolume.Secret.Items[1].Key != expectedMembers[workerID].ID.String()+".tls.crt" ||
			controlVolume.Secret.Items[1].Path != "tls.crt" ||
			controlVolume.Secret.Items[2].Key != expectedMembers[workerID].ID.String()+".tls.key" ||
			controlVolume.Secret.Items[2].Path != "tls.key" {
			t.Fatalf("WorkerInstance Pod %q control identity projection = %#v", pod.Name, controlVolume.Secret)
		}
		for _, variable := range stageAgent.Env {
			if strings.HasPrefix(variable.Name, "VELA_STAGE_WORKER_MEMBER_") {
				t.Fatalf("single-member Pod %q exposes member transport environment %q", pod.Name, variable.Name)
			}
		}
		if len(stageAgent.Ports) != 0 {
			t.Fatalf("single-member Pod %q exposes member transport ports %#v", pod.Name, stageAgent.Ports)
		}
		expectedMember := expectedMembers[workerID]
		if requireEnvironment(t, stageAgent, "VELA_WORKER_MEMBER_EPOCH") !=
			strconv.FormatInt(expectedMember.MemberEpoch, 10) {
			t.Fatalf("WorkerInstance Pod %q Stage Worker member epoch = %#v", pod.Name, stageAgent.Env)
		}
		var devices []struct {
			DeviceID    string `json:"device_id"`
			DeviceEpoch int64  `json:"device_epoch"`
		}
		if err := json.Unmarshal(
			[]byte(requireEnvironment(t, stageAgent, "VELA_STAGE_WORKER_DEVICES_JSON")),
			&devices,
		); err != nil || len(devices) != 1 ||
			devices[0].DeviceID != expectedMember.DeviceConstraints[0].DeviceID.String() ||
			devices[0].DeviceEpoch != expectedMember.DeviceConstraints[0].DeviceEpoch {
			t.Fatalf("WorkerInstance Pod %q Stage Worker devices = %#v error=%v", pod.Name, devices, err)
		}
		if requireEnvironment(t, stageAgent, "VELA_MODEL_RUNTIME_SOCKET") !=
			"/run/vela-model-runtime/private/runtime.sock" {
			t.Fatalf("WorkerInstance Pod %q Stage Worker ModelRuntime socket = %#v", pod.Name, stageAgent.Env)
		}
		runtimeContainer := requireContainer(t, pod.Spec.Containers, "model-runtime")
		if pod.Spec.TerminationGracePeriodSeconds == nil ||
			*pod.Spec.TerminationGracePeriodSeconds != 150 {
			t.Fatalf("WorkerInstance Pod %q termination grace = %v, want 150s", pod.Name, pod.Spec.TerminationGracePeriodSeconds)
		}
		if len(runtimeContainer.Command) != 0 {
			t.Fatalf("WorkerInstance Pod %q overrides the ModelRuntime image entrypoint: %v", pod.Name, runtimeContainer.Command)
		}
		for name, value := range map[string]string{
			"VELA_MODEL_RUNTIME_LAUNCH_MANIFEST_FILE":            "/etc/vela-model-runtime/private/launch.json",
			"VELA_MODEL_RUNTIME_AUTHORITY_VERIFIER_KEYRING_FILE": "/etc/vela-model-runtime/private/authority/verifier-keyring.json",
			"VELA_MODEL_RUNTIME_EPOCH_DIRECTORY":                 "/var/lib/vela/stage-worker/scratch/model-runtime-epochs",
			"VELA_MODEL_RUNTIME_SOCKET":                          "/run/vela-model-runtime/private/runtime.sock",
		} {
			if got := requireEnvironment(t, runtimeContainer, name); got != value {
				t.Fatalf("WorkerInstance Pod %q ModelRuntime environment %s = %q", pod.Name, name, got)
			}
		}
		requireConfigMapEnvironment(t, stageAgent, "VELA_MODEL_RUNTIME_STARTUP_TIMEOUT", "stage-worker-runtime-r1", "model-runtime-startup-timeout")
		requireConfigMapEnvironment(t, runtimeContainer, "VELA_MODEL_RUNTIME_CANCEL_TIMEOUT", "stage-worker-runtime-r1", "model-runtime-cancel-timeout")
		requireConfigMapEnvironment(t, runtimeContainer, "VELA_MODEL_RUNTIME_SHUTDOWN_TIMEOUT", "stage-worker-runtime-r1", "model-runtime-shutdown-timeout")
		initializer := requireContainer(t, pod.Spec.InitContainers, "model-runtime-private-materialization")
		var launch modelruntime.LaunchManifest
		if err := json.Unmarshal(
			[]byte(requireEnvironment(t, initializer, "VELA_MODEL_RUNTIME_LAUNCH_MANIFEST_JSON")),
			&launch,
		); err != nil {
			t.Fatalf("decode WorkerInstance Pod %q launch manifest: %v", pod.Name, err)
		}
		if _, err := launch.RuntimeBindings(); err != nil || launch.WorkerInstanceID != workerID ||
			launch.WorkerMemberID != expectedMember.ID.String() ||
			launch.DeviceSetDigest != expectedWorkers[workerID].DeviceSetDigest ||
			launch.MembershipDigest != expectedWorkers[workerID].MembershipDigest ||
			len(launch.LocalDevices) != 1 ||
			launch.LocalDevices[0].GPUUUID != expectedMember.DeviceConstraints[0].GPUUUID {
			t.Fatalf("WorkerInstance Pod %q launch manifest = %#v error=%v", pod.Name, launch, err)
		}
		for _, runtime := range launch.Runtimes {
			if runtime.ScratchRoot != "/var/lib/vela/stage-worker/scratch" ||
				runtime.InputRoot != "/var/lib/vela/stage-worker/scratch/inputs" ||
				runtime.OutputRoot != "/var/lib/vela/stage-worker/scratch/outputs" {
				t.Fatalf("WorkerInstance Pod %q runtime roots = %#v", pod.Name, runtime)
			}
		}
		if _, exists := runtimeContainer.Resources.Limits["nvidia.com/gpu"]; exists {
			t.Fatalf("WorkerInstance Pod %q retained generic nvidia.com/gpu allocation", pod.Name)
		}
		if len(pod.Spec.ResourceClaims) != 1 || len(runtimeContainer.Resources.Claims) != 1 ||
			pod.Spec.ResourceClaims[0].ResourceClaimTemplateName == nil ||
			runtimeContainer.Resources.Claims[0].Name != pod.Spec.ResourceClaims[0].Name {
			t.Fatalf("WorkerInstance Pod %q exact GPU claims = %#v/%#v", pod.Name,
				pod.Spec.ResourceClaims, runtimeContainer.Resources.Claims)
		}
		if pod.Labels["vela.ai/worker-role"] == "aux" {
			auxPod = pod
		}
	}
	if auxPod.Name == "" {
		t.Fatal("H3 AUX WorkerInstance Pod is missing")
	}
	claims, err := client.Resource(schema.GroupVersionResource{
		Group: "resource.k8s.io", Version: "v1", Resource: "resourceclaimtemplates",
	}).Namespace("vela-system").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(claims.Items) != 8 {
		t.Fatalf("list exact H3 GPU claims count=%d error=%v", len(claims.Items), err)
	}
	expectedConstraints := make(map[string]fleetcontroller.DeviceConstraint, 8)
	for _, worker := range bundle.WorkerInstances {
		expectedConstraints[worker.ID.String()] = worker.Members[0].DeviceConstraints[0]
	}
	for index := range claims.Items {
		var claim resourcev1.ResourceClaimTemplate
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
			claims.Items[index].Object,
			&claim,
		); err != nil {
			t.Fatalf("decode exact GPU claim: %v", err)
		}
		workerID := claim.Labels["vela.ai/worker-instance-id"]
		expected, exists := expectedConstraints[workerID]
		requests := claim.Spec.Spec.Devices.Requests
		if !exists || len(requests) != 1 || requests[0].Exactly == nil ||
			claim.Annotations["vela.ai/worker-node-identity"] != "h3-node-01" ||
			requests[0].Exactly.DeviceClassName != "gpu.nvidia.com" ||
			requests[0].Exactly.Count != 1 || len(requests[0].Exactly.Selectors) != 1 ||
			requests[0].Exactly.Selectors[0].CEL == nil {
			t.Fatalf("exact GPU claim %q = %#v", claim.Name, claim)
		}
		expression := requests[0].Exactly.Selectors[0].CEL.Expression
		if !strings.Contains(expression, "device.attributes['gpu.nvidia.com'].type == 'gpu'") ||
			!strings.Contains(expression, expected.GPUUUID) ||
			!strings.Contains(expression, expected.PCIBDF) {
			t.Fatalf("exact GPU claim %q selector %q misses %#v", claim.Name, expression, expected)
		}
	}
	var auxLaunch modelruntime.LaunchManifest
	if err := json.Unmarshal(
		[]byte(requireEnvironment(
			t, requireContainer(t, auxPod.Spec.InitContainers, "model-runtime-private-materialization"),
			"VELA_MODEL_RUNTIME_LAUNCH_MANIFEST_JSON",
		)),
		&auxLaunch,
	); err != nil || len(auxLaunch.Runtimes) != 2 || auxLaunch.Runtimes[0].Component != "ENCODER" ||
		auxLaunch.Runtimes[1].Component != "VAE_DECODER" ||
		auxLaunch.Runtimes[0].ModelResidencyID == auxLaunch.Runtimes[1].ModelResidencyID ||
		auxLaunch.Runtimes[0].ModelRuntimeEpochFloor == auxLaunch.Runtimes[1].ModelRuntimeEpochFloor {
		t.Fatalf("AUX launch runtimes = %#v error=%v", auxLaunch.Runtimes, err)
	}
	stageAgent := requireContainer(t, auxPod.Spec.Containers, "stage-worker-agent")
	for name, value := range map[string]string{
		"VELA_WORKER_TLS_CERT_FILE":                      "/etc/vela-stage-worker/private/control/tls.crt",
		"VELA_WORKER_TLS_KEY_FILE":                       "/etc/vela-stage-worker/private/control/tls.key",
		"VELA_WORKER_CONTROL_CA_FILE":                    "/etc/vela-stage-worker/private/control/ca.crt",
		"VELA_STAGE_WORKER_AUTHORITY_KEYRING_FILE":       "/etc/vela-stage-worker/private/authority/keyring.json",
		"VELA_ARTIFACT_S3_ACCESS_KEY_ID_FILE":            "/etc/vela-stage-worker/private/artifact/access-key-id",
		"VELA_ARTIFACT_S3_SECRET_ACCESS_KEY_FILE":        "/etc/vela-stage-worker/private/artifact/secret-access-key",
		"VELA_ARTIFACT_S3_CA_FILE":                       "/etc/vela-stage-worker/private/artifact/ca.crt",
		"VELA_STAGE_WORKER_PRODUCTION_STATE_ROOT":        "/var/lib/vela/stage-worker/scratch/production-state",
		"VELA_STAGE_WORKER_INPUT_ROOT":                   "/var/lib/vela/stage-worker/scratch/inputs",
		"VELA_STAGE_WORKER_INPUT_TRANSFER_JOURNAL_ROOT":  "/var/lib/vela/stage-worker/scratch/input-transfer-journal",
		"VELA_STAGE_WORKER_OUTPUT_ROOT":                  "/var/lib/vela/stage-worker/scratch/outputs",
		"VELA_STAGE_WORKER_MATERIALIZATION_JOURNAL_ROOT": "/var/lib/vela/stage-worker/scratch/materialization-journal",
		"VELA_MODEL_RUNTIME_SOCKET":                      "/run/vela-model-runtime/private/runtime.sock",
		"VELA_MODEL_RUNTIME_EXPECTED_UID":                "10001",
	} {
		if got := requireEnvironment(t, stageAgent, name); got != value {
			t.Fatalf("Stage Worker environment %s = %q, want %q", name, got, value)
		}
	}
	for name, key := range map[string]string{
		"VELA_WORKER_CONTROL_ADDRESS":                           "control-address",
		"VELA_WORKER_CONTROL_SERVER_NAME":                       "control-server-name",
		"VELA_STAGE_WORKER_AUTHORITY_ACTIVE_KEY_ID":             "authority-active-key-id",
		"VELA_STAGE_WORKER_CONNECTOR_REVISION_ID":               "connector-revision-id",
		"VELA_STAGE_WORKER_CAPACITY_TTL":                        "capacity-ttl",
		"VELA_STAGE_WORKER_HEARTBEAT_INTERVAL":                  "heartbeat-interval",
		"VELA_STAGE_WORKER_RETRY_MINIMUM":                       "retry-minimum",
		"VELA_STAGE_WORKER_RETRY_MAXIMUM":                       "retry-maximum",
		"VELA_STAGE_WORKER_MATERIALIZATION_JOURNAL_LIMIT":       "materialization-journal-limit",
		"VELA_ARTIFACT_S3_ENDPOINT":                             "artifact-s3-endpoint",
		"VELA_ARTIFACT_S3_REGION":                               "artifact-s3-region",
		"VELA_ARTIFACT_S3_BUCKET":                               "artifact-s3-bucket",
		"VELA_ARTIFACT_S3_PATH_STYLE":                           "artifact-s3-path-style",
		"VELA_ARTIFACT_S3_SIGNED_GET_TTL":                       "artifact-s3-signed-get-ttl",
		"VELA_STAGE_WORKER_SOURCE_LOSS_RETRY":                   "source-loss-retry",
		"VELA_STAGE_WORKER_SOURCE_LOSS_CONSUMED_RESOURCE_UNITS": "source-loss-consumed-resource-units",
	} {
		requireConfigMapEnvironment(t, stageAgent, name, "stage-worker-runtime-r1", key)
	}
	initializer := requireContainer(t, auxPod.Spec.InitContainers, "stage-worker-private-materialization")
	if initializer.SecurityContext == nil || initializer.SecurityContext.RunAsUser == nil ||
		*initializer.SecurityContext.RunAsUser != 0 {
		t.Fatalf("Stage Worker private materialization security = %#v", initializer.SecurityContext)
	}
	for _, name := range []string{
		"stage-worker-control-projected", "stage-worker-authority-projected",
		"artifact-store-credentials-projected", "artifact-store-ca-projected",
		"stage-worker-private",
	} {
		requireVolumeMount(t, initializer, name)
	}
	requireVolumeMount(t, stageAgent, "stage-worker-private")
	for _, name := range []string{
		"stage-worker-control-projected", "stage-worker-authority-projected",
		"artifact-store-credentials-projected", "artifact-store-ca-projected",
	} {
		if hasVolumeMount(stageAgent, name) {
			t.Fatalf("Stage Worker directly mounts projected Secret %q", name)
		}
	}
	if volume := requireVolume(t, auxPod.Spec.Volumes, "stage-worker-private"); volume.EmptyDir == nil || volume.EmptyDir.Medium != corev1.StorageMediumMemory {
		t.Fatalf("Stage Worker private volume = %#v", volume)
	}
	runtimeContainer := requireContainer(t, auxPod.Spec.Containers, "model-runtime")
	runtimeInitializer := requireContainer(t, auxPod.Spec.InitContainers, "model-runtime-private-materialization")
	for _, name := range []string{"model-runtime-verifier-projected", "model-runtime-private", "scratch"} {
		requireVolumeMount(t, runtimeInitializer, name)
	}
	for _, name := range []string{"model-runtime-private", "model-runtime-socket", "scratch", "model-weights"} {
		requireVolumeMount(t, runtimeContainer, name)
	}
	for _, forbidden := range []string{
		"stage-worker-control-projected", "stage-worker-authority-projected",
		"artifact-store-credentials-projected", "artifact-store-ca-projected", "stage-worker-private",
	} {
		if hasVolumeMount(runtimeContainer, forbidden) || hasVolumeMount(runtimeInitializer, forbidden) {
			t.Fatalf("ModelRuntime materialization mounts private Stage Worker authority %q", forbidden)
		}
	}
	if volume := requireVolume(t, auxPod.Spec.Volumes, "model-runtime-private"); volume.EmptyDir == nil || volume.EmptyDir.Medium != corev1.StorageMediumMemory {
		t.Fatalf("ModelRuntime private volume = %#v", volume)
	}

	replayed, err := actuator.Actuate(context.Background(), bundle)
	if err != nil || !replayed.Converged || replayed.CreatedGPUClaims != 0 ||
		replayed.CreatedPods != 0 || replayed.CreatedMemberServices != 0 ||
		replayed.CreatedMemberSecrets != 0 {
		t.Fatalf("replay H3 WorkerBundle actuation = %#v error=%v", replayed, err)
	}
}

func TestMaterializeIndependentCPUMediaWorkersWithoutGPUClaims(t *testing.T) {
	bundle := cpuMediaBundle(t)
	pods, claims, err := fleetcontroller.MaterializeWorkerInstanceLaunchResources(bundle)
	if err != nil {
		t.Fatalf("materialize CPU media WorkerInstances: %v", err)
	}
	if len(pods) != 3 || len(claims) != 0 {
		t.Fatalf("CPU media resources pods=%d GPU claims=%d", len(pods), len(claims))
	}
	wantCPU := map[string]string{
		"cpu-encode":    "4",
		"cpu-mux":       "2",
		"cpu-thumbnail": "2",
	}
	for _, pod := range pods {
		role := pod.Labels["vela.ai/worker-role"]
		if len(pod.Spec.ResourceClaims) != 0 {
			t.Fatalf("CPU media Pod %q has GPU claims: %#v", pod.Name, pod.Spec.ResourceClaims)
		}
		runtimeContainer := requireContainer(t, pod.Spec.Containers, "model-runtime")
		if len(runtimeContainer.Resources.Claims) != 0 ||
			runtimeContainer.Resources.Requests.Cpu().String() != wantCPU[role] {
			t.Fatalf("CPU media Pod %q runtime resources = %#v", pod.Name, runtimeContainer.Resources)
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.Name == "model-weights" {
				t.Fatalf("CPU media Pod %q mounts model weights", pod.Name)
			}
		}
		stageAgent := requireContainer(t, pod.Spec.Containers, "stage-worker-agent")
		var capacity map[string]int64
		if err := json.Unmarshal(
			[]byte(requireEnvironment(t, stageAgent, "VELA_STAGE_WORKER_CAPACITY_VECTOR_JSON")),
			&capacity,
		); err != nil || capacity["active_stage_slots"] != 1 || capacity["cpu_slot_count"] != 1 ||
			capacity["gpu_count"] != 0 {
			t.Fatalf("CPU media Pod %q capacity = %#v error=%v", pod.Name, capacity, err)
		}
		initializer := requireContainer(t, pod.Spec.InitContainers, "model-runtime-private-materialization")
		var launch modelruntime.LaunchManifest
		if err := json.Unmarshal(
			[]byte(requireEnvironment(t, initializer, "VELA_MODEL_RUNTIME_LAUNCH_MANIFEST_JSON")),
			&launch,
		); err != nil || len(launch.LocalDevices) != 1 ||
			launch.LocalDevices[0].ResourceClass != "CPU" || launch.LocalDevices[0].GPUUUID != "" ||
			len(launch.Runtimes) != 1 || launch.Runtimes[0].Component != "CPU_MEDIA" {
			t.Fatalf("CPU media Pod %q launch = %#v error=%v", pod.Name, launch, err)
		}
		if _, err := launch.RuntimeBindings(); err != nil {
			t.Fatalf("CPU media Pod %q launch authority: %v", pod.Name, err)
		}
	}
}

func TestKubernetesActuatorMaterializesMultiMemberAuthority(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register core Kubernetes scheme: %v", err)
	}
	if err := resourcev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register resource Kubernetes scheme: %v", err)
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
	memberIDs := []uuid.UUID{
		uuid.MustParse("49300000-0000-0000-0000-000000000033"),
		uuid.MustParse("49300000-0000-0000-0000-000000000034"),
	}
	memberPKIData, memberIdentityDigests := issueMemberPKISource(t, "vela-system", memberIDs)
	immutable := true
	memberPKISecret := corev1.Secret{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "vela-system", Name: "stage-worker-member-pki-r1",
		},
		Immutable: &immutable,
		Type:      corev1.SecretTypeOpaque,
		Data:      memberPKIData,
	}
	encodedMemberPKI, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&memberPKISecret)
	if err != nil {
		t.Fatalf("encode member PKI source Secret: %v", err)
	}
	sourceSecrets := client.Resource(schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "secrets",
	}).Namespace("vela-system")
	liveSource, err := sourceSecrets.Create(
		context.Background(),
		&unstructured.Unstructured{Object: encodedMemberPKI},
		metav1.CreateOptions{},
	)
	if err != nil {
		t.Fatalf("create member PKI source Secret: %v", err)
	}
	memberDevices := map[string][]fleetcontroller.DeviceConstraint{
		"member-0": {
			{DeviceID: uuid.MustParse("49300000-0000-0000-0000-000000000041"), DeviceEpoch: 11,
				GPUUUID: "GPU-00000000-0000-0000-0000-000000000001", PCIBDF: "0000:41:00.0"},
			{DeviceID: uuid.MustParse("49300000-0000-0000-0000-000000000042"), DeviceEpoch: 12,
				GPUUUID: "GPU-00000000-0000-0000-0000-000000000002", PCIBDF: "0000:42:00.0"},
		},
		"member-1": {
			{DeviceID: uuid.MustParse("49300000-0000-0000-0000-000000000043"), DeviceEpoch: 13,
				GPUUUID: "GPU-00000000-0000-0000-0000-000000000003", PCIBDF: "0000:41:00.0"},
			{DeviceID: uuid.MustParse("49300000-0000-0000-0000-000000000044"), DeviceEpoch: 14,
				GPUUUID: "GPU-00000000-0000-0000-0000-000000000004", PCIBDF: "0000:44:00.0"},
		},
	}
	bundle := fleetcontroller.WorkerBundleActuation{
		SchemaVersion:                  1,
		PlanRevisionID:                 uuid.MustParse("49300000-0000-0000-0000-000000000001"),
		WorkerBundleID:                 uuid.MustParse("49300000-0000-0000-0000-000000000002"),
		Namespace:                      "vela-system",
		InitImage:                      pinnedImage("busybox", 'b'),
		StageWorkerAgentImage:          pinnedImage("vela-stage-worker-agent", 'c'),
		RuntimeImage:                   pinnedImage("vela-stage-runtime", 'd'),
		StageWorkerConfigMap:           "stage-worker-runtime-r1",
		ModelRuntimeVerifierConfigMap:  "model-runtime-verifier-r1",
		StageWorkerControlTLSSecret:    "stage-worker-control-tls-r1",
		StageWorkerAuthoritySecret:     "stage-worker-authority-r1",
		ArtifactStoreCredentialsSecret: "artifact-store-credentials-r1",
		ArtifactStoreCASecret:          "artifact-store-ca-r1",
		StageWorkerMemberPKISecret:     "stage-worker-member-pki-r1",
		WorkerInstances: []fleetcontroller.WorkerInstanceActuation{{
			ID:                      workerID,
			InstanceEpoch:           1,
			WorkerProfileRevisionID: uuid.MustParse("49300000-0000-0000-0000-000000000031"),
			CapacityPoolID:          uuid.MustParse("49300000-0000-0000-0000-000000000032"),
			Role:                    "llm",
			CapacitySlots:           1,
			DeviceSetDigest:         strings.Repeat("1", 64),
			MembershipDigest:        strings.Repeat("2", 64),
			ModelRuntimes: []fleetcontroller.ModelRuntimeProcess{{
				ModelResidencyID:       uuid.MustParse("49300000-0000-0000-0000-000000000035"),
				CapacityPoolID:         uuid.MustParse("49300000-0000-0000-0000-000000000032"),
				StageProfileRevisionID: uuid.MustParse("49300000-0000-0000-0000-000000000036"),
				ModelRuntimeEpochFloor: 1,
				Component:              "LLM", ModelComponentRevision: "future-llm-v1",
				RuntimeIdentity: "future-llm-runtime-v1", Command: []string{"/opt/vela/bin/llm-runtime"},
				InitializationTimeout: "2h", ShutdownTimeout: "2m",
			}},
			Members: []fleetcontroller.WorkerMemberActuation{
				{
					ID: memberIDs[0], MemberEpoch: 21,
					Key: "member-0", NodeIdentity: "llm-node-a", ResourceClass: "GPU",
					IdentityDigest: memberIdentityDigests[memberIDs[0]],
					DeviceCount:    2, DeviceConstraints: memberDevices["member-0"],
				},
				{
					ID: memberIDs[1], MemberEpoch: 22,
					Key: "member-1", NodeIdentity: "llm-node-b", ResourceClass: "GPU",
					IdentityDigest: memberIdentityDigests[memberIDs[1]],
					DeviceCount:    2, DeviceConstraints: memberDevices["member-1"],
				},
			},
		}},
	}
	bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
	if err != nil {
		t.Fatalf("digest multi-member WorkerBundle actuation: %v", err)
	}
	mutableSource := liveSource.DeepCopy()
	if err := unstructured.SetNestedField(mutableSource.Object, false, "immutable"); err != nil {
		t.Fatalf("mark member PKI source mutable: %v", err)
	}
	if _, err := sourceSecrets.Update(context.Background(), mutableSource, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("store mutable member PKI source: %v", err)
	}
	if _, err := actuator.Actuate(context.Background(), bundle); err == nil ||
		!strings.Contains(err.Error(), "invalid or mutable") {
		t.Fatalf("mutable member PKI source error=%v", err)
	}
	if err := unstructured.SetNestedField(mutableSource.Object, true, "immutable"); err != nil {
		t.Fatalf("restore member PKI source immutability: %v", err)
	}
	if _, err := sourceSecrets.Update(context.Background(), mutableSource, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("store immutable member PKI source: %v", err)
	}
	validIdentityDigest := bundle.WorkerInstances[0].Members[0].IdentityDigest
	bundle.WorkerInstances[0].Members[0].IdentityDigest = strings.Repeat("a", 64)
	bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
	if err != nil {
		t.Fatalf("digest mismatched member identity actuation: %v", err)
	}
	if _, err := actuator.Actuate(context.Background(), bundle); err == nil ||
		!strings.Contains(err.Error(), "identity digest does not match") {
		t.Fatalf("mismatched member identity digest error=%v", err)
	}
	bundle.WorkerInstances[0].Members[0].IdentityDigest = validIdentityDigest
	bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
	if err != nil {
		t.Fatalf("restore multi-member WorkerBundle digest: %v", err)
	}
	result, err := actuator.Actuate(context.Background(), bundle)
	if err != nil || result.CreatedGPUClaims != 2 || result.CreatedPods != 2 ||
		result.CreatedMemberServices != 2 || result.CreatedMemberSecrets != 2 {
		t.Fatalf("actuate multi-member WorkerInstance = %#v error=%v", result, err)
	}
	rollout := fleetcontroller.ResidencyPlanRollout{
		ApprovedPlan: fleet.ApprovedResidencyPlan{
			SchemaVersion: 1, ID: bundle.PlanRevisionID, StableID: "llm-gang-r1", Revision: 1,
			ContentDigest: bundle.RevisionDigest, ApprovalEvidenceDigest: strings.Repeat("e", 64),
			ApprovedAt: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC), ApprovedBy: "fleet/operator-1",
			CapacityPools: []fleet.PlannedCapacityPool{{
				ID: bundle.WorkerInstances[0].CapacityPoolID, StableID: "llm-gang",
				StageProfileRevisionID: bundle.WorkerInstances[0].ModelRuntimes[0].StageProfileRevisionID,
				ResourceClass:          "GPU", SecurityClass: "INTERNAL", Region: "cn-shanghai",
				MaxReadyQueueDepth: 128,
			}},
			WorkerBundles: []fleet.PlannedWorkerBundle{{
				ID: bundle.WorkerBundleID, StableID: "llm-gang-bundle", DesiredGeneration: 1,
				LayoutDigest: bundle.RevisionDigest,
			}},
			WorkerInstances: []fleet.PlannedWorkerInstance{{
				ID:                      bundle.WorkerInstances[0].ID,
				WorkerProfileRevisionID: bundle.WorkerInstances[0].WorkerProfileRevisionID,
				CapacityPoolID:          bundle.WorkerInstances[0].CapacityPoolID,
				WorkerBundleID:          bundle.WorkerBundleID,
				DesiredMemberCount:      2,
				DesiredDeviceCount:      4,
				ModelRuntimeRoutes: []fleet.PlannedModelRuntimeRoute{{
					ModelResidencyID:       bundle.WorkerInstances[0].ModelRuntimes[0].ModelResidencyID,
					CapacityPoolID:         bundle.WorkerInstances[0].ModelRuntimes[0].CapacityPoolID,
					StageProfileRevisionID: bundle.WorkerInstances[0].ModelRuntimes[0].StageProfileRevisionID,
				}},
			}},
		},
		WorkerBundles: []fleetcontroller.WorkerBundleActuation{bundle},
	}
	validator, err := fleetcontroller.NewWorkerInstanceAdmissionValidator(
		[]fleetcontroller.ResidencyPlanRollout{rollout}, resources,
	)
	if err != nil {
		t.Fatalf("create multi-member admission validator: %v", err)
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
		runtimeContainer := requireContainer(t, pod.Spec.Containers, "model-runtime")
		stageAgent := requireContainer(t, pod.Spec.Containers, "stage-worker-agent")
		var capacity map[string]int64
		if err := json.Unmarshal(
			[]byte(requireEnvironment(t, stageAgent, "VELA_STAGE_WORKER_CAPACITY_VECTOR_JSON")),
			&capacity,
		); err != nil || capacity["active_stage_slots"] != 1 || capacity["gpu_count"] != 4 {
			t.Fatalf("member Pod %q gang capacity = %#v error=%v", pod.Name, capacity, err)
		}
		var endpoints []struct {
			WorkerMemberID string `json:"worker_member_id"`
			MemberEpoch    int64  `json:"member_epoch"`
			IdentityDigest string `json:"identity_digest"`
			Address        string `json:"address"`
			ServerName     string `json:"server_name"`
		}
		if err := json.Unmarshal(
			[]byte(requireEnvironment(t, stageAgent, "VELA_STAGE_WORKER_MEMBERS_JSON")),
			&endpoints,
		); err != nil || len(endpoints) != 2 || endpoints[0].WorkerMemberID != memberIDs[0].String() ||
			endpoints[1].WorkerMemberID != memberIDs[1].String() ||
			endpoints[0].Address == endpoints[1].Address ||
			endpoints[0].ServerName == endpoints[1].ServerName {
			t.Fatalf("member Pod %q endpoints = %#v error=%v", pod.Name, endpoints, err)
		}
		for name, expected := range map[string]string{
			"VELA_STAGE_WORKER_MEMBER_LISTEN_ADDRESS":       "0.0.0.0:7444",
			"VELA_STAGE_WORKER_MEMBER_CLIENT_TLS_CERT_FILE": "/etc/vela-stage-worker/private/member/client/tls.crt",
			"VELA_STAGE_WORKER_MEMBER_SERVER_TLS_CERT_FILE": "/etc/vela-stage-worker/private/member/server/tls.crt",
		} {
			if got := requireEnvironment(t, stageAgent, name); got != expected {
				t.Fatalf("member Pod %q environment %s=%q, want %q", pod.Name, name, got, expected)
			}
		}
		memberPKIVolume := requireVolume(t, pod.Spec.Volumes, "stage-worker-member-pki-projected")
		controlVolume := requireVolume(t, pod.Spec.Volumes, "stage-worker-control-projected")
		if len(stageAgent.Ports) != 1 || stageAgent.Ports[0].ContainerPort != 7444 ||
			memberPKIVolume.Secret == nil ||
			memberPKIVolume.Secret.SecretName == bundle.StageWorkerMemberPKISecret ||
			controlVolume.Secret == nil || len(controlVolume.Secret.Items) != 3 ||
			controlVolume.Secret.Items[0].Key != "ca.crt" ||
			controlVolume.Secret.Items[1].Key != object.GetLabels()["vela.ai/worker-member-id"]+".tls.crt" ||
			controlVolume.Secret.Items[1].Path != "tls.crt" ||
			controlVolume.Secret.Items[2].Key != object.GetLabels()["vela.ai/worker-member-id"]+".tls.key" ||
			controlVolume.Secret.Items[2].Path != "tls.key" {
			t.Fatalf("member Pod %q transport port/volume = %#v/%#v", pod.Name, stageAgent.Ports, pod.Spec.Volumes)
		}
		initializer := requireContainer(t, pod.Spec.InitContainers, "stage-worker-private-materialization")
		if len(initializer.Command) != 3 ||
			!strings.Contains(initializer.Command[2], "cp /projected/control/tls.crt /private/control/tls.crt") ||
			!strings.Contains(initializer.Command[2], "cp /projected/control/tls.key /private/control/tls.key") ||
			strings.Contains(initializer.Command[2], "cp /projected/member-pki/client.crt /private/control/tls.crt") {
			t.Fatalf("member Pod %q does not isolate control and peer identities", pod.Name)
		}
		if _, exists := runtimeContainer.Resources.Limits["nvidia.com/gpu"]; exists ||
			len(runtimeContainer.Resources.Claims) != 1 || len(pod.Spec.ResourceClaims) != 1 {
			t.Fatalf("member Pod %q GPU claim = %#v/%#v", pod.Name,
				pod.Spec.ResourceClaims, runtimeContainer.Resources.Claims)
		}
	}
	if !seenMembers["member-0"] || !seenMembers["member-1"] {
		t.Fatalf("multi-member Pods = %#v", seenMembers)
	}
	claims, err := client.Resource(schema.GroupVersionResource{
		Group: "resource.k8s.io", Version: "v1", Resource: "resourceclaimtemplates",
	}).Namespace("vela-system").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(claims.Items) != 2 {
		t.Fatalf("multi-member GPU claim count=%d error=%v", len(claims.Items), err)
	}
	for index := range claims.Items {
		var claim resourcev1.ResourceClaimTemplate
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
			claims.Items[index].Object,
			&claim,
		); err != nil {
			t.Fatalf("decode multi-member GPU claim: %v", err)
		}
		memberKey := claim.Labels["vela.ai/worker-member-key"]
		requests := claim.Spec.Spec.Devices.Requests
		expectedNode := map[string]string{"member-0": "llm-node-a", "member-1": "llm-node-b"}[memberKey]
		if len(memberDevices[memberKey]) != 2 || len(requests) != 2 ||
			claim.Annotations["vela.ai/worker-node-identity"] != expectedNode {
			t.Fatalf("member %q exact GPU requests = %#v", memberKey, requests)
		}
		for deviceIndex, expected := range memberDevices[memberKey] {
			expression := requests[deviceIndex].Exactly.Selectors[0].CEL.Expression
			if !strings.Contains(expression, expected.GPUUUID) ||
				!strings.Contains(expression, expected.PCIBDF) {
				t.Fatalf("member %q request %d selector %q misses %#v",
					memberKey, deviceIndex, expression, expected)
			}
		}
	}
	services, err := client.Resource(schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "services",
	}).Namespace("vela-system").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(services.Items) != 2 {
		t.Fatalf("multi-member Service count=%d error=%v", len(services.Items), err)
	}
	for _, serviceObject := range services.Items {
		var service corev1.Service
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(serviceObject.Object, &service); err != nil ||
			service.Spec.Type != corev1.ServiceTypeClusterIP || !service.Spec.PublishNotReadyAddresses ||
			service.Spec.SessionAffinity != corev1.ServiceAffinityNone ||
			service.Spec.InternalTrafficPolicy == nil ||
			*service.Spec.InternalTrafficPolicy != corev1.ServiceInternalTrafficPolicyCluster ||
			len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != 7444 {
			t.Fatalf("decode multi-member Service %q = %#v error=%v", serviceObject.GetName(), service, err)
		}
		if err := validator.ValidateProtectedServiceCreate(context.Background(), service); err != nil {
			t.Fatalf("validate exact member Service %q: %v", service.Name, err)
		}
	}
	secrets, err := client.Resource(schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "secrets",
	}).Namespace("vela-system").List(context.Background(), metav1.ListOptions{
		LabelSelector: "vela.ai/fleet-controller-owned=true",
	})
	if err != nil || len(secrets.Items) != 2 {
		t.Fatalf("derived member Secret count=%d error=%v", len(secrets.Items), err)
	}
	for _, secretObject := range secrets.Items {
		var secret corev1.Secret
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(secretObject.Object, &secret); err != nil ||
			secret.Immutable == nil || !*secret.Immutable || len(secret.Data) != 6 {
			t.Fatalf("decode derived member Secret %q = %#v error=%v", secretObject.GetName(), secret, err)
		}
		if err := validator.ValidateProtectedSecretCreate(context.Background(), secret); err != nil {
			t.Fatalf("validate exact member Secret %q: %v", secret.Name, err)
		}
	}
	replayed, err := actuator.Actuate(context.Background(), bundle)
	if err != nil || !replayed.Converged || replayed.CreatedGPUClaims != 0 || replayed.CreatedPods != 0 ||
		replayed.CreatedMemberServices != 0 || replayed.CreatedMemberSecrets != 0 {
		t.Fatalf("replay multi-member WorkerBundle actuation = %#v error=%v", replayed, err)
	}
	serviceResources := client.Resource(schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "services",
	}).Namespace("vela-system")
	originalService := services.Items[0].DeepCopy()
	var driftedService corev1.Service
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(originalService.Object, &driftedService); err != nil {
		t.Fatalf("decode member Service for drift: %v", err)
	}
	driftedService.Spec.Ports[0].Port++
	if err := validator.ValidateProtectedServiceCreate(
		context.Background(), driftedService,
	); !errors.Is(err, fleetcontroller.ErrProtectedResourceDrift) {
		t.Fatalf("drifted member Service admission error=%v, want ErrProtectedResourceDrift", err)
	}
	encodedDriftedService, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&driftedService)
	if err != nil {
		t.Fatalf("encode drifted member Service: %v", err)
	}
	if _, err := serviceResources.Update(
		context.Background(), &unstructured.Unstructured{Object: encodedDriftedService}, metav1.UpdateOptions{},
	); err != nil {
		t.Fatalf("store drifted member Service: %v", err)
	}
	if _, err := actuator.Actuate(context.Background(), bundle); !errors.Is(err, fleetcontroller.ErrProtectedResourceDrift) {
		t.Fatalf("drifted member Service error=%v, want ErrProtectedResourceDrift", err)
	}
	if _, err := serviceResources.Update(context.Background(), originalService, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("restore member Service: %v", err)
	}
	derivedSecrets := client.Resource(schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "secrets",
	}).Namespace("vela-system")
	var driftedSecret corev1.Secret
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(secrets.Items[0].Object, &driftedSecret); err != nil {
		t.Fatalf("decode member Secret for drift: %v", err)
	}
	driftedSecret.Data["client.crt"] = []byte("drifted")
	if err := validator.ValidateProtectedSecretCreate(
		context.Background(), driftedSecret,
	); !errors.Is(err, fleetcontroller.ErrProtectedResourceDrift) {
		t.Fatalf("drifted member Secret admission error=%v, want ErrProtectedResourceDrift", err)
	}
	encodedDriftedSecret, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&driftedSecret)
	if err != nil {
		t.Fatalf("encode drifted member Secret: %v", err)
	}
	if _, err := derivedSecrets.Update(
		context.Background(), &unstructured.Unstructured{Object: encodedDriftedSecret}, metav1.UpdateOptions{},
	); err != nil {
		t.Fatalf("store drifted member Secret: %v", err)
	}
	if _, err := actuator.Actuate(context.Background(), bundle); !errors.Is(err, fleetcontroller.ErrProtectedResourceDrift) {
		t.Fatalf("drifted member Secret error=%v, want ErrProtectedResourceDrift", err)
	}
}

func TestWorkerBundleActuationRejectsDuplicateDeviceAuthorityAndInvalidH3Shapes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fleetcontroller.WorkerBundleActuation)
	}{
		{
			name: "duplicate device id",
			mutate: func(bundle *fleetcontroller.WorkerBundleActuation) {
				bundle.WorkerInstances[1].Members[0].DeviceConstraints[0].DeviceID =
					bundle.WorkerInstances[0].Members[0].DeviceConstraints[0].DeviceID
			},
		},
		{
			name: "duplicate PCI BDF on one node",
			mutate: func(bundle *fleetcontroller.WorkerBundleActuation) {
				bundle.WorkerInstances[1].Members[0].DeviceConstraints[0].PCIBDF =
					bundle.WorkerInstances[0].Members[0].DeviceConstraints[0].PCIBDF
			},
		},
		{
			name: "duplicate model residency",
			mutate: func(bundle *fleetcontroller.WorkerBundleActuation) {
				bundle.WorkerInstances[1].ModelRuntimes[0].ModelResidencyID =
					bundle.WorkerInstances[0].ModelRuntimes[0].ModelResidencyID
			},
		},
		{
			name: "multi gpu dit",
			mutate: func(bundle *fleetcontroller.WorkerBundleActuation) {
				member := &bundle.WorkerInstances[1].Members[0]
				member.DeviceCount = 2
				member.DeviceConstraints = append(member.DeviceConstraints, fleetcontroller.DeviceConstraint{
					DeviceID:    uuid.MustParse("49300000-0000-0000-0000-000000000199"),
					DeviceEpoch: 1,
					GPUUUID:     "GPU-00000000-0000-0000-0000-000000000099",
					PCIBDF:      "0000:99:00.0",
				})
			},
		},
		{
			name: "shared slot exception on non aux role",
			mutate: func(bundle *fleetcontroller.WorkerBundleActuation) {
				bundle.WorkerInstances[0].Role = "custom"
			},
		},
		{
			name: "shared slot exception on single runtime",
			mutate: func(bundle *fleetcontroller.WorkerBundleActuation) {
				bundle.WorkerInstances[1].SharedSlotException = "H3_AUX_ENCODER_VAE"
			},
		},
		{
			name: "shared slot exception on CPU",
			mutate: func(bundle *fleetcontroller.WorkerBundleActuation) {
				member := &bundle.WorkerInstances[0].Members[0]
				member.ResourceClass = "CPU"
				member.DeviceConstraints[0].ResourceClass = "CPU"
				member.DeviceConstraints[0].GPUUUID = ""
				member.DeviceConstraints[0].PCIBDF = ""
			},
		},
		{
			name: "member identity does not bind member id",
			mutate: func(bundle *fleetcontroller.WorkerBundleActuation) {
				bundle.WorkerInstances[0].Members[0].IdentityDigest = strings.Repeat("f", 64)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bundle, err := fleetcontroller.BuildH3WorkerBundleActuation(h3BundleSpec())
			if err != nil {
				t.Fatalf("build H3 WorkerBundle actuation: %v", err)
			}
			test.mutate(&bundle)
			bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
			if err != nil {
				t.Fatalf("digest invalid WorkerBundle fixture: %v", err)
			}
			if err := fleetcontroller.ValidateWorkerBundleActuation(bundle); err == nil {
				t.Fatal("invalid WorkerBundle actuation was accepted")
			}
		})
	}
}

func TestWorkerBundleActuationRejectsUnapprovedCPUWorkerRole(t *testing.T) {
	bundle := cpuMediaBundle(t)
	bundle.WorkerInstances[0].Role = "cpu-custom"
	var err error
	bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
	if err != nil {
		t.Fatalf("digest invalid CPU WorkerBundle fixture: %v", err)
	}
	if err := fleetcontroller.ValidateWorkerBundleActuation(bundle); err == nil {
		t.Fatal("unapproved CPU Worker role was accepted")
	}
}

func TestKubernetesActuatorRejectsGPUOwnedByLiveClaimFromAnotherPlan(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register core Kubernetes scheme: %v", err)
	}
	if err := resourcev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register resource Kubernetes scheme: %v", err)
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
	first, err := fleetcontroller.BuildH3WorkerBundleActuation(h3BundleSpec())
	if err != nil {
		t.Fatalf("build first H3 WorkerBundle actuation: %v", err)
	}
	if _, err := actuator.Actuate(context.Background(), first); err != nil {
		t.Fatalf("actuate first H3 WorkerBundle: %v", err)
	}
	secondSpec := h3BundleSpec()
	secondSpec.PlanRevisionID = uuid.MustParse("49300000-0000-0000-0000-000000000201")
	secondSpec.WorkerBundleID = uuid.MustParse("49300000-0000-0000-0000-000000000202")
	second, err := fleetcontroller.BuildH3WorkerBundleActuation(secondSpec)
	if err != nil {
		t.Fatalf("build second H3 WorkerBundle actuation: %v", err)
	}
	if _, err := actuator.Actuate(context.Background(), second); !errors.Is(err, fleetcontroller.ErrProtectedResourceDrift) {
		t.Fatalf("actuate conflicting H3 WorkerBundle error=%v, want ErrProtectedResourceDrift", err)
	}
	claims, err := client.Resource(schema.GroupVersionResource{
		Group: "resource.k8s.io", Version: "v1", Resource: "resourceclaimtemplates",
	}).Namespace("vela-system").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(claims.Items) != 8 {
		t.Fatalf("live GPU claims after conflict count=%d error=%v, want 8", len(claims.Items), err)
	}
}

func TestKubernetesActuatorFailsClosedOnImmutablePodDrift(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register core Kubernetes scheme: %v", err)
	}
	if err := resourcev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register resource Kubernetes scheme: %v", err)
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
	if _, err := actuator.Actuate(context.Background(), bundle); err != nil {
		t.Fatalf("materialize H3 WorkerBundle: %v", err)
	}
	pods := client.Resource(schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "pods",
	}).Namespace("vela-system")
	live, err := pods.List(context.Background(), metav1.ListOptions{})
	if err != nil || len(live.Items) == 0 {
		t.Fatalf("list WorkerInstance Pods count=%d error=%v", len(live.Items), err)
	}
	drifted := live.Items[0].DeepCopy()
	drifted.SetAnnotations(map[string]string{
		"vela.ai/fleet-controller-owned": "true",
		"vela.ai/device-constraints":     `[]`,
		"vela.ai/actuation-revision":     digestString('f'),
	})
	if _, err := pods.Update(context.Background(), drifted, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("inject immutable Pod drift: %v", err)
	}
	if _, err := actuator.Actuate(context.Background(), bundle); !errors.Is(err, fleetcontroller.ErrProtectedResourceDrift) {
		t.Fatalf("drifted WorkerInstance Pod error=%v, want ErrProtectedResourceDrift", err)
	}
}

func TestKubernetesActuatorFailsClosedOnMissingOrDriftedExactGPUClaim(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, dynamic.ResourceInterface, unstructured.Unstructured)
	}{
		{
			name: "missing claim with live Pod",
			mutate: func(t *testing.T, claims dynamic.ResourceInterface, claim unstructured.Unstructured) {
				t.Helper()
				if err := claims.Delete(
					context.Background(), claim.GetName(), metav1.DeleteOptions{},
				); err != nil {
					t.Fatalf("delete exact GPU claim: %v", err)
				}
			},
		},
		{
			name: "drifted exact selector",
			mutate: func(t *testing.T, claims dynamic.ResourceInterface, claim unstructured.Unstructured) {
				t.Helper()
				var decoded resourcev1.ResourceClaimTemplate
				if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
					claim.Object,
					&decoded,
				); err != nil {
					t.Fatalf("decode exact GPU claim for drift: %v", err)
				}
				decoded.Spec.Spec.Devices.Requests[0].Exactly.Selectors[0].CEL.Expression =
					"device.attributes['gpu.nvidia.com'].type == 'gpu' && false"
				encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&decoded)
				if err != nil {
					t.Fatalf("encode exact GPU selector drift: %v", err)
				}
				if _, err := claims.Update(
					context.Background(), &unstructured.Unstructured{Object: encoded}, metav1.UpdateOptions{},
				); err != nil {
					t.Fatalf("store exact GPU selector drift: %v", err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			if err := corev1.AddToScheme(scheme); err != nil {
				t.Fatalf("register core Kubernetes scheme: %v", err)
			}
			if err := resourcev1.AddToScheme(scheme); err != nil {
				t.Fatalf("register resource Kubernetes scheme: %v", err)
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
			if _, err := actuator.Actuate(context.Background(), bundle); err != nil {
				t.Fatalf("materialize exact GPU claims: %v", err)
			}
			claims := client.Resource(schema.GroupVersionResource{
				Group: "resource.k8s.io", Version: "v1", Resource: "resourceclaimtemplates",
			}).Namespace("vela-system")
			live, err := claims.List(context.Background(), metav1.ListOptions{})
			if err != nil || len(live.Items) == 0 {
				t.Fatalf("list exact GPU claims count=%d error=%v", len(live.Items), err)
			}
			test.mutate(t, claims, live.Items[0])
			if _, err := actuator.Actuate(
				context.Background(), bundle,
			); !errors.Is(err, fleetcontroller.ErrProtectedResourceDrift) {
				t.Fatalf("reconcile invalid exact GPU claim error=%v, want ErrProtectedResourceDrift", err)
			}
		})
	}
}

func h3BundleSpec() fleetcontroller.H3WorkerBundleSpec {
	devices := [8]fleetcontroller.DeviceConstraint{}
	memberEpochs := [8]int64{}
	deviceSetDigests := [8]string{}
	membershipDigests := [8]string{}
	ditRuntimes := [7]fleetcontroller.ModelRuntimeProcess{}
	for index := range devices {
		devices[index] = fleetcontroller.DeviceConstraint{
			DeviceID: uuid.MustParse(
				"49300000-0000-0000-0000-0000000001" + fmt.Sprintf("%02d", index),
			),
			DeviceEpoch: int64(index + 11),
			GPUUUID:     "GPU-00000000-0000-0000-0000-00000000000" + string(rune('0'+index)),
			PCIBDF:      "0000:4" + string(rune('1'+index)) + ":00.0",
		}
		memberEpochs[index] = int64(index + 21)
		deviceSetDigests[index] = strings.Repeat(string(rune('1'+index)), 64)
		membershipDigests[index] = strings.Repeat(string("89abcdef"[index]), 64)
		if index > 0 {
			ditRuntimes[index-1] = fleetcontroller.ModelRuntimeProcess{
				ModelResidencyID: uuid.MustParse(
					"49300000-0000-0000-0000-0000000004" + fmt.Sprintf("%02d", index),
				),
				StageProfileRevisionID: uuid.MustParse("49300000-0000-0000-0000-000000000302"),
				ModelRuntimeEpochFloor: int64(index + 30),
				Component:              "DIT", ModelComponentRevision: "h3-dit-v1",
				RuntimeIdentity: "h3-dit-runtime-v1", Command: []string{"/opt/vela/bin/h3-dit"},
				InitializationTimeout: "2h", ShutdownTimeout: "2m",
			}
		}
	}
	return fleetcontroller.H3WorkerBundleSpec{
		SchemaVersion:  1,
		PlanRevisionID: uuid.MustParse("49300000-0000-0000-0000-000000000001"),
		WorkerBundleID: uuid.MustParse("49300000-0000-0000-0000-000000000002"),
		Namespace:      "vela-system", NodeIdentity: "h3-node-01",
		AuxCapacityPoolID:              uuid.MustParse("49300000-0000-0000-0000-000000000003"),
		VAEDecoderCapacityPoolID:       uuid.MustParse("49300000-0000-0000-0000-000000000007"),
		DiTCapacityPoolID:              uuid.MustParse("49300000-0000-0000-0000-000000000004"),
		AuxWorkerProfileRevisionID:     uuid.MustParse("49300000-0000-0000-0000-000000000005"),
		DiTWorkerProfileRevisionID:     uuid.MustParse("49300000-0000-0000-0000-000000000006"),
		InitImage:                      pinnedImage("busybox", 'b'),
		StageWorkerAgentImage:          pinnedImage("vela-stage-worker-agent", 'c'),
		RuntimeImage:                   pinnedImage("vela-h3-stage-runtime", 'd'),
		StageWorkerConfigMap:           "stage-worker-runtime-r1",
		ModelRuntimeVerifierConfigMap:  "model-runtime-verifier-r1",
		StageWorkerControlTLSSecret:    "stage-worker-control-tls-r1",
		StageWorkerAuthoritySecret:     "stage-worker-authority-r1",
		ArtifactStoreCredentialsSecret: "artifact-store-credentials-r1",
		ArtifactStoreCASecret:          "artifact-store-ca-r1",
		Devices:                        devices,
		MemberEpochs:                   memberEpochs,
		DeviceSetDigests:               deviceSetDigests,
		MembershipDigests:              membershipDigests,
		Encoder: fleetcontroller.ModelRuntimeProcess{
			ModelResidencyID:       uuid.MustParse("49300000-0000-0000-0000-000000000201"),
			StageProfileRevisionID: uuid.MustParse("49300000-0000-0000-0000-000000000301"),
			ModelRuntimeEpochFloor: 31,
			Component:              "ENCODER", ModelComponentRevision: "h3-encoder-v1",
			RuntimeIdentity: "h3-encoder-runtime-v1", Command: []string{"/opt/vela/bin/h3-encoder"},
			InitializationTimeout: "2h", ShutdownTimeout: "2m",
		},
		DiT: ditRuntimes,
		VAEDecoder: fleetcontroller.ModelRuntimeProcess{
			ModelResidencyID:       uuid.MustParse("49300000-0000-0000-0000-000000000209"),
			StageProfileRevisionID: uuid.MustParse("49300000-0000-0000-0000-000000000309"),
			ModelRuntimeEpochFloor: 39,
			Component:              "VAE_DECODER", ModelComponentRevision: "h3-vae-decoder-v1",
			RuntimeIdentity: "h3-vae-decoder-runtime-v1", Command: []string{"/opt/vela/bin/h3-vae-decoder"},
			InitializationTimeout: "2h", ShutdownTimeout: "2m",
		},
	}
}

func cpuMediaBundle(t *testing.T) fleetcontroller.WorkerBundleActuation {
	t.Helper()
	bundle, err := fleetcontroller.BuildH3WorkerBundleActuation(h3BundleSpec())
	if err != nil {
		t.Fatalf("build base WorkerBundle actuation: %v", err)
	}
	bundle.WorkerBundleID = uuid.MustParse("49300000-0000-0000-0000-000000000020")
	bundle.RuntimeImage = pinnedImage("vela-cpu-media-stage-runtime", 'e')
	bundle.WorkerInstances = nil
	roles := []struct {
		role        string
		profileID   string
		poolID      string
		stageID     string
		residencyID string
		digit       string
	}{
		{"cpu-encode", "49300000-0000-0000-0000-000000000021", "49300000-0000-0000-0000-000000000031", "49300000-0000-0000-0000-000000000041", "49300000-0000-0000-0000-000000000051", "a"},
		{"cpu-mux", "49300000-0000-0000-0000-000000000022", "49300000-0000-0000-0000-000000000032", "49300000-0000-0000-0000-000000000042", "49300000-0000-0000-0000-000000000052", "b"},
		{"cpu-thumbnail", "49300000-0000-0000-0000-000000000023", "49300000-0000-0000-0000-000000000033", "49300000-0000-0000-0000-000000000043", "49300000-0000-0000-0000-000000000053", "c"},
	}
	for index, spec := range roles {
		workerID := uuid.MustParse(fmt.Sprintf("49300000-0000-0000-0000-00000000006%d", index+1))
		bundle.WorkerInstances = append(bundle.WorkerInstances, fleetcontroller.WorkerInstanceActuation{
			ID: workerID, InstanceEpoch: 1,
			WorkerProfileRevisionID: uuid.MustParse(spec.profileID),
			CapacityPoolID:          uuid.MustParse(spec.poolID), Role: spec.role, CapacitySlots: 1,
			DeviceSetDigest:  strings.Repeat(spec.digit, 64),
			MembershipDigest: strings.Repeat(string(rune('d'+index)), 64),
			ModelRuntimes: []fleetcontroller.ModelRuntimeProcess{{
				ModelResidencyID:       uuid.MustParse(spec.residencyID),
				CapacityPoolID:         uuid.MustParse(spec.poolID),
				StageProfileRevisionID: uuid.MustParse(spec.stageID), ModelRuntimeEpochFloor: 1,
				Component: "CPU_MEDIA", ModelComponentRevision: "ffmpeg-" + spec.role + "-v1",
				RuntimeIdentity:       "h3-" + spec.role + "-runtime-v1",
				Command:               []string{"/opt/vela/bin/h3-cpu-media", "--role", spec.role},
				InitializationTimeout: "5m", ShutdownTimeout: "2m",
			}},
			Members: []fleetcontroller.WorkerMemberActuation{{
				ID:          uuid.MustParse(fmt.Sprintf("49300000-0000-0000-0000-00000000007%d", index+1)),
				MemberEpoch: 1, Key: "member-0", NodeIdentity: "h3-cpu-node-01",
				ResourceClass: "CPU", IdentityDigest: memberIdentityDigest(
					uuid.MustParse(fmt.Sprintf("49300000-0000-0000-0000-00000000007%d", index+1)),
				),
				DeviceCount: 1, DeviceConstraints: []fleetcontroller.DeviceConstraint{{
					DeviceID:    uuid.MustParse(fmt.Sprintf("49300000-0000-0000-0000-00000000008%d", index+1)),
					DeviceEpoch: 1, ResourceClass: "CPU",
				}},
			}},
		})
	}
	bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
	if err != nil {
		t.Fatalf("compute CPU media WorkerBundle digest: %v", err)
	}
	return bundle
}

func memberIdentityDigest(memberID uuid.UUID) string {
	digest := sha256.Sum256([]byte("spiffe://vela.internal/stage-worker/" + memberID.String()))
	return fmt.Sprintf("%x", digest)
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

func issueMemberPKISource(
	t *testing.T,
	namespace string,
	members []uuid.UUID,
) (map[string][]byte, map[uuid.UUID]string) {
	t.Helper()
	_, caKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate member PKI CA key: %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Vela member test CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caKey.Public(), caKey)
	if err != nil {
		t.Fatalf("issue member PKI CA: %v", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse member PKI CA: %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	data := make(map[string][]byte, len(members)*6)
	digests := make(map[uuid.UUID]string, len(members))
	for _, memberID := range members {
		identity, err := url.Parse("spiffe://vela.internal/stage-worker/" + memberID.String())
		if err != nil {
			t.Fatalf("parse member SPIFFE identity: %v", err)
		}
		digest := sha256.Sum256([]byte(identity.String()))
		digests[memberID] = fmt.Sprintf("%x", digest)
		clientCertificate, clientKey := issueMemberCertificate(
			t, ca, caKey, memberID.String()+"-client", nil, []*url.URL{identity},
			[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		)
		serverCertificate, serverKey := issueMemberCertificate(
			t, ca, caKey, memberID.String()+"-server", []string{"*." + namespace + ".svc"},
			[]*url.URL{identity}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		)
		prefix := memberID.String() + "."
		data[prefix+"client.crt"] = clientCertificate
		data[prefix+"client.key"] = clientKey
		data[prefix+"server-ca.crt"] = caPEM
		data[prefix+"server.crt"] = serverCertificate
		data[prefix+"server.key"] = serverKey
		data[prefix+"client-ca.crt"] = caPEM
	}
	return data, digests
}

func issueMemberCertificate(
	t *testing.T,
	ca *x509.Certificate,
	caKey ed25519.PrivateKey,
	commonName string,
	dnsNames []string,
	identities []*url.URL,
	usage []x509.ExtKeyUsage,
) ([]byte, []byte) {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate member certificate key: %v", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatalf("generate member certificate serial: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: commonName},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usage,
		DNSNames: dnsNames, URIs: identities,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, key.Public(), caKey)
	if err != nil {
		t.Fatalf("issue member certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal member certificate key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
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

func requireConfigMapEnvironment(
	t *testing.T,
	container corev1.Container,
	name string,
	configMap string,
	key string,
) {
	t.Helper()
	for _, variable := range container.Env {
		if variable.Name != name {
			continue
		}
		if variable.ValueFrom == nil || variable.ValueFrom.ConfigMapKeyRef == nil ||
			variable.ValueFrom.ConfigMapKeyRef.Name != configMap ||
			variable.ValueFrom.ConfigMapKeyRef.Key != key {
			t.Fatalf("container %q environment %q = %#v", container.Name, name, variable)
		}
		return
	}
	t.Fatalf("container %q environment %q is missing", container.Name, name)
}

func requireVolumeMount(t *testing.T, container corev1.Container, name string) corev1.VolumeMount {
	t.Helper()
	for _, mount := range container.VolumeMounts {
		if mount.Name == name {
			return mount
		}
	}
	t.Fatalf("container %q volume mount %q is missing", container.Name, name)
	return corev1.VolumeMount{}
}

func hasVolumeMount(container corev1.Container, name string) bool {
	for _, mount := range container.VolumeMounts {
		if mount.Name == name {
			return true
		}
	}
	return false
}

func requireVolume(t *testing.T, volumes []corev1.Volume, name string) corev1.VolumeSource {
	t.Helper()
	for _, volume := range volumes {
		if volume.Name == name {
			return volume.VolumeSource
		}
	}
	t.Fatalf("volume %q is missing", name)
	return corev1.VolumeSource{}
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
