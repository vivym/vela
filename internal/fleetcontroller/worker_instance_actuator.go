package fleetcontroller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	workerInstanceIDLabel       = "vela.ai/worker-instance-id"
	workerInstanceEpochLabel    = "vela.ai/worker-instance-epoch"
	workerMemberIDLabel         = "vela.ai/worker-member-id"
	workerMemberKeyLabel        = "vela.ai/worker-member-key"
	workerBundleIDLabel         = "vela.ai/worker-bundle-id"
	workerProfileRevisionLabel  = "vela.ai/worker-profile-revision-id"
	capacityPoolIDLabel         = "vela.ai/capacity-pool-id"
	residencyPlanRevisionLabel  = "vela.ai/residency-plan-revision-id"
	workerRoleLabel             = "vela.ai/worker-role"
	deviceConstraintsAnnotation = "vela.ai/device-constraints"
	actuationRevisionAnnotation = "vela.ai/actuation-revision"
	h3AUXSharedSlotException    = "H3_AUX_ENCODER_VAE"
)

var (
	actuatorGPUUUIDPattern = regexp.MustCompile(
		`^GPU-[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`,
	)
	actuatorPCIBDFPattern = regexp.MustCompile(`^[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}[.][0-7]$`)
)

type WorkerInstancePodResources interface {
	GetWorkerInstancePod(context.Context, ResourceKey) (corev1.Pod, error)
	CreateWorkerInstancePod(context.Context, corev1.Pod) error
}

type WorkerInstanceActuator struct {
	resources WorkerInstancePodResources
}

type WorkerBundleActuation struct {
	SchemaVersion          int
	PlanRevisionID         uuid.UUID
	WorkerBundleID         uuid.UUID
	RevisionDigest         string
	Namespace              string
	InitImage              string
	WorkerAgentImage       string
	RuntimeImage           string
	ArtifactStoreTLSSecret string
	WorkerRuntimeConfigMap string
	WorkerControlTLSSecret string
	WorkerInstances        []WorkerInstanceActuation
}

type WorkerInstanceActuation struct {
	ID                      uuid.UUID
	InstanceEpoch           int64
	WorkerProfileRevisionID uuid.UUID
	CapacityPoolID          uuid.UUID
	Role                    string
	CapacitySlots           int
	SharedSlotException     string
	ModelRuntimes           []ModelRuntimeProcess
	Members                 []WorkerMemberActuation
}

type WorkerMemberActuation struct {
	ID                uuid.UUID
	Key               string
	NodeIdentity      string
	ResourceClass     string
	DeviceCount       int
	DeviceConstraints []DeviceConstraint
}

type DeviceConstraint struct {
	GPUUUID string `json:"gpu_uuid"`
	PCIBDF  string `json:"pci_bdf"`
}

type ModelRuntimeProcess struct {
	Component              string   `json:"component"`
	ModelComponentRevision string   `json:"model_component_revision"`
	RuntimeIdentity        string   `json:"runtime_identity"`
	Command                []string `json:"command"`
}

type H3WorkerBundleSpec struct {
	SchemaVersion              int
	PlanRevisionID             uuid.UUID
	WorkerBundleID             uuid.UUID
	RevisionDigest             string
	Namespace                  string
	NodeIdentity               string
	AuxCapacityPoolID          uuid.UUID
	DiTCapacityPoolID          uuid.UUID
	AuxWorkerProfileRevisionID uuid.UUID
	DiTWorkerProfileRevisionID uuid.UUID
	InitImage                  string
	WorkerAgentImage           string
	RuntimeImage               string
	ArtifactStoreTLSSecret     string
	WorkerRuntimeConfigMap     string
	WorkerControlTLSSecret     string
	Devices                    [8]DeviceConstraint
	Encoder                    ModelRuntimeProcess
	DiT                        ModelRuntimeProcess
	VAEDecoder                 ModelRuntimeProcess
}

type WorkerInstanceActuationResult struct {
	CreatedPods int
	Converged   bool
}

func NewWorkerInstanceActuator(
	resources WorkerInstancePodResources,
) (*WorkerInstanceActuator, error) {
	if resources == nil {
		return nil, errors.New("WorkerInstance Kubernetes resources are required")
	}
	return &WorkerInstanceActuator{resources: resources}, nil
}

func BuildH3WorkerBundleActuation(spec H3WorkerBundleSpec) (WorkerBundleActuation, error) {
	if spec.SchemaVersion != 1 || spec.PlanRevisionID == uuid.Nil ||
		spec.WorkerBundleID == uuid.Nil || !validSHA256(spec.RevisionDigest) ||
		!validResourceName(spec.Namespace) || !validResourceName(spec.NodeIdentity) ||
		spec.AuxCapacityPoolID == uuid.Nil || spec.DiTCapacityPoolID == uuid.Nil ||
		spec.AuxWorkerProfileRevisionID == uuid.Nil || spec.DiTWorkerProfileRevisionID == uuid.Nil ||
		!validPinnedImage(spec.InitImage) || !validPinnedImage(spec.WorkerAgentImage) ||
		!validPinnedImage(spec.RuntimeImage) || !validResourceName(spec.ArtifactStoreTLSSecret) ||
		!validResourceName(spec.WorkerRuntimeConfigMap) ||
		!validResourceName(spec.WorkerControlTLSSecret) ||
		!validModelRuntimeProcess(spec.Encoder) || spec.Encoder.Component != "ENCODER" ||
		!validModelRuntimeProcess(spec.DiT) || spec.DiT.Component != "DIT" ||
		!validModelRuntimeProcess(spec.VAEDecoder) || spec.VAEDecoder.Component != "VAE_DECODER" {
		return WorkerBundleActuation{}, errors.New("certified H3 WorkerBundle specification is invalid")
	}
	for _, device := range spec.Devices {
		if !validDeviceConstraint(device) {
			return WorkerBundleActuation{}, errors.New("certified H3 WorkerBundle device constraint is invalid")
		}
	}
	bundle := WorkerBundleActuation{
		SchemaVersion: 1, PlanRevisionID: spec.PlanRevisionID,
		WorkerBundleID: spec.WorkerBundleID, RevisionDigest: spec.RevisionDigest,
		Namespace: spec.Namespace, InitImage: spec.InitImage,
		WorkerAgentImage: spec.WorkerAgentImage, RuntimeImage: spec.RuntimeImage,
		ArtifactStoreTLSSecret: spec.ArtifactStoreTLSSecret,
		WorkerRuntimeConfigMap: spec.WorkerRuntimeConfigMap,
		WorkerControlTLSSecret: spec.WorkerControlTLSSecret,
		WorkerInstances:        make([]WorkerInstanceActuation, 0, 8),
	}
	auxID := uuid.NewSHA1(spec.WorkerBundleID, []byte("h3/aux"))
	bundle.WorkerInstances = append(bundle.WorkerInstances, WorkerInstanceActuation{
		ID: auxID, InstanceEpoch: 1,
		WorkerProfileRevisionID: spec.AuxWorkerProfileRevisionID,
		CapacityPoolID:          spec.AuxCapacityPoolID, Role: "aux", CapacitySlots: 1,
		SharedSlotException: h3AUXSharedSlotException,
		ModelRuntimes:       []ModelRuntimeProcess{spec.Encoder, spec.VAEDecoder},
		Members:             []WorkerMemberActuation{h3Member(auxID, spec.NodeIdentity, spec.Devices[0])},
	})
	for index := range 7 {
		workerID := uuid.NewSHA1(spec.WorkerBundleID, []byte("h3/dit/"+strconv.Itoa(index)))
		bundle.WorkerInstances = append(bundle.WorkerInstances, WorkerInstanceActuation{
			ID: workerID, InstanceEpoch: 1,
			WorkerProfileRevisionID: spec.DiTWorkerProfileRevisionID,
			CapacityPoolID:          spec.DiTCapacityPoolID, Role: "dit", CapacitySlots: 1,
			ModelRuntimes: []ModelRuntimeProcess{spec.DiT},
			Members:       []WorkerMemberActuation{h3Member(workerID, spec.NodeIdentity, spec.Devices[index+1])},
		})
	}
	if err := ValidateWorkerBundleActuation(bundle); err != nil {
		return WorkerBundleActuation{}, err
	}
	return bundle, nil
}

func h3Member(
	workerID uuid.UUID,
	nodeIdentity string,
	device DeviceConstraint,
) WorkerMemberActuation {
	return WorkerMemberActuation{
		ID: uuid.NewSHA1(workerID, []byte("member-0")), Key: "member-0",
		NodeIdentity: nodeIdentity, ResourceClass: "GPU", DeviceCount: 1,
		DeviceConstraints: []DeviceConstraint{device},
	}
}

func (actuator *WorkerInstanceActuator) Actuate(
	ctx context.Context,
	bundle WorkerBundleActuation,
) (WorkerInstanceActuationResult, error) {
	if actuator == nil || actuator.resources == nil {
		return WorkerInstanceActuationResult{}, errors.New("WorkerInstance actuator is not configured")
	}
	if err := ValidateWorkerBundleActuation(bundle); err != nil {
		return WorkerInstanceActuationResult{}, err
	}
	desiredPods, err := materializeWorkerInstancePods(bundle)
	if err != nil {
		return WorkerInstanceActuationResult{}, err
	}
	missing := make([]corev1.Pod, 0, len(desiredPods))
	for _, desired := range desiredPods {
		live, err := actuator.resources.GetWorkerInstancePod(ctx, ResourceKey{
			Namespace: bundle.Namespace, Name: desired.Name,
		})
		if errors.Is(err, ErrResourceNotFound) {
			missing = append(missing, desired)
			continue
		}
		if err != nil {
			return WorkerInstanceActuationResult{}, fmt.Errorf("get WorkerInstance Pod %q: %w", desired.Name, err)
		}
		if !workerInstancePodMatches(live, desired) {
			return WorkerInstanceActuationResult{}, ErrProtectedResourceDrift
		}
	}
	for _, pod := range missing {
		if err := actuator.resources.CreateWorkerInstancePod(ctx, pod); err != nil {
			return WorkerInstanceActuationResult{}, fmt.Errorf("create WorkerInstance Pod %q: %w", pod.Name, err)
		}
	}
	return WorkerInstanceActuationResult{
		CreatedPods: len(missing), Converged: len(missing) == 0,
	}, nil
}

func ValidateWorkerBundleActuation(bundle WorkerBundleActuation) error {
	if bundle.SchemaVersion != 1 || bundle.PlanRevisionID == uuid.Nil ||
		bundle.WorkerBundleID == uuid.Nil || !validSHA256(bundle.RevisionDigest) ||
		!validResourceName(bundle.Namespace) || !validPinnedImage(bundle.InitImage) ||
		!validPinnedImage(bundle.WorkerAgentImage) || !validPinnedImage(bundle.RuntimeImage) ||
		!validResourceName(bundle.ArtifactStoreTLSSecret) ||
		!validResourceName(bundle.WorkerRuntimeConfigMap) ||
		!validResourceName(bundle.WorkerControlTLSSecret) ||
		len(bundle.WorkerInstances) == 0 || len(bundle.WorkerInstances) > 4096 {
		return errors.New("WorkerBundle actuation is invalid")
	}
	workers := make(map[uuid.UUID]struct{}, len(bundle.WorkerInstances))
	members := make(map[uuid.UUID]struct{})
	podNames := make(map[string]struct{})
	gpuUUIDs := make(map[string]struct{})
	pciBDFs := make(map[string]struct{})
	for _, worker := range bundle.WorkerInstances {
		if worker.ID == uuid.Nil || worker.InstanceEpoch <= 0 ||
			worker.WorkerProfileRevisionID == uuid.Nil || worker.CapacityPoolID == uuid.Nil ||
			!validWorkerRole(worker.Role) || worker.CapacitySlots <= 0 ||
			len(worker.ModelRuntimes) == 0 || len(worker.Members) == 0 || len(worker.Members) > 64 {
			return errors.New("WorkerInstance actuation is invalid")
		}
		if _, exists := workers[worker.ID]; exists {
			return errors.New("WorkerBundle actuation reuses a WorkerInstance id")
		}
		workers[worker.ID] = struct{}{}
		if len(worker.ModelRuntimes) > 1 && !validSharedSlotException(worker) {
			return errors.New("multi-model WorkerInstance is not an approved shared-slot exception")
		}
		for _, runtime := range worker.ModelRuntimes {
			if !validModelRuntimeProcess(runtime) {
				return errors.New("WorkerInstance ModelRuntime process is invalid")
			}
		}
		memberKeys := make(map[string]struct{}, len(worker.Members))
		for _, member := range worker.Members {
			if member.ID == uuid.Nil || !validMemberKey(member.Key) ||
				!validResourceName(member.NodeIdentity) || member.DeviceCount <= 0 || member.DeviceCount > 64 ||
				(member.ResourceClass != "" && member.ResourceClass != "GPU") ||
				(len(member.DeviceConstraints) != 0 && len(member.DeviceConstraints) != member.DeviceCount) {
				return errors.New("WorkerMember actuation is invalid")
			}
			if _, exists := members[member.ID]; exists {
				return errors.New("WorkerBundle actuation reuses a WorkerMember id")
			}
			if _, exists := memberKeys[member.Key]; exists {
				return errors.New("WorkerInstance actuation reuses a WorkerMember key")
			}
			members[member.ID] = struct{}{}
			memberKeys[member.Key] = struct{}{}
			podName := workerInstancePodName(worker.ID, member.Key)
			if _, exists := podNames[podName]; exists {
				return errors.New("WorkerBundle actuation reuses a Pod identity")
			}
			podNames[podName] = struct{}{}
			for _, constraint := range member.DeviceConstraints {
				if !validDeviceConstraint(constraint) {
					return errors.New("WorkerMember device constraint is invalid")
				}
				if _, exists := gpuUUIDs[constraint.GPUUUID]; exists {
					return errors.New("WorkerBundle actuation assigns one GPU to multiple WorkerInstances")
				}
				if _, exists := pciBDFs[constraint.PCIBDF]; exists {
					return errors.New("WorkerBundle actuation assigns one PCI device to multiple WorkerInstances")
				}
				gpuUUIDs[constraint.GPUUUID] = struct{}{}
				pciBDFs[constraint.PCIBDF] = struct{}{}
			}
		}
	}
	return nil
}

func validSharedSlotException(worker WorkerInstanceActuation) bool {
	return worker.SharedSlotException == h3AUXSharedSlotException && worker.CapacitySlots == 1 &&
		len(worker.ModelRuntimes) == 2 && worker.ModelRuntimes[0].Component == "ENCODER" &&
		worker.ModelRuntimes[1].Component == "VAE_DECODER" && len(worker.Members) == 1 &&
		worker.Members[0].DeviceCount == 1
}

func validModelRuntimeProcess(runtime ModelRuntimeProcess) bool {
	if runtime.Component != "ENCODER" && runtime.Component != "DIT" &&
		runtime.Component != "VAE_DECODER" && runtime.Component != "LLM" &&
		runtime.Component != "CPU_MEDIA" {
		return false
	}
	if !validBoundedText(runtime.ModelComponentRevision, 300) ||
		!validBoundedText(runtime.RuntimeIdentity, 500) || len(runtime.Command) == 0 ||
		len(runtime.Command) > 128 || !strings.HasPrefix(runtime.Command[0], "/") {
		return false
	}
	for _, argument := range runtime.Command {
		if !validBoundedText(argument, 4096) {
			return false
		}
	}
	return true
}

func validWorkerRole(role string) bool {
	return validMemberKey(role)
}

func validMemberKey(value string) bool {
	if len(value) == 0 || len(value) > 100 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, character := range value[1:] {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') &&
			character != '_' && character != '-' {
			return false
		}
	}
	return true
}

func validDeviceConstraint(constraint DeviceConstraint) bool {
	return actuatorGPUUUIDPattern.MatchString(constraint.GPUUUID) &&
		actuatorPCIBDFPattern.MatchString(constraint.PCIBDF)
}

func materializeWorkerInstancePods(bundle WorkerBundleActuation) ([]corev1.Pod, error) {
	pods := make([]corev1.Pod, 0)
	for _, worker := range bundle.WorkerInstances {
		for _, member := range worker.Members {
			pod, err := materializeWorkerInstancePod(bundle, worker, member)
			if err != nil {
				return nil, err
			}
			pods = append(pods, pod)
		}
	}
	sort.Slice(pods, func(left, right int) bool { return pods[left].Name < pods[right].Name })
	return pods, nil
}

func materializeWorkerInstancePod(
	bundle WorkerBundleActuation,
	worker WorkerInstanceActuation,
	member WorkerMemberActuation,
) (corev1.Pod, error) {
	constraints, err := json.Marshal(member.DeviceConstraints)
	if err != nil {
		return corev1.Pod{}, fmt.Errorf("encode WorkerMember device constraints: %w", err)
	}
	runtimes, err := json.Marshal(worker.ModelRuntimes)
	if err != nil {
		return corev1.Pod{}, fmt.Errorf("encode WorkerInstance ModelRuntimes: %w", err)
	}
	automount := false
	shareProcessNamespace := false
	runAsNonRoot := true
	runAsUser := int64(10001)
	runAsGroup := int64(10001)
	fsGroup := int64(10001)
	enableServiceLinks := true
	pod := corev1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: bundle.Namespace, Name: workerInstancePodName(worker.ID, member.Key),
			Labels: map[string]string{
				"app.kubernetes.io/name":    "vela-worker-instance",
				"app.kubernetes.io/part-of": "vela",
				protectedLabel:              "true",
				workerInstanceIDLabel:       worker.ID.String(),
				workerInstanceEpochLabel:    strconv.FormatInt(worker.InstanceEpoch, 10),
				workerMemberIDLabel:         member.ID.String(),
				workerMemberKeyLabel:        member.Key,
				workerBundleIDLabel:         bundle.WorkerBundleID.String(),
				workerProfileRevisionLabel:  worker.WorkerProfileRevisionID.String(),
				capacityPoolIDLabel:         worker.CapacityPoolID.String(),
				residencyPlanRevisionLabel:  bundle.PlanRevisionID.String(),
				workerRoleLabel:             worker.Role,
			},
			Annotations: map[string]string{
				"vela.ai/fleet-controller-owned": "true",
				deviceConstraintsAnnotation:      string(constraints),
				actuationRevisionAnnotation:      bundle.RevisionDigest,
			},
			Finalizers: []string{protectionFinalizer},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: "vela-worker", AutomountServiceAccountToken: &automount,
			ShareProcessNamespace:         &shareProcessNamespace,
			TerminationGracePeriodSeconds: int64Pointer(120),
			RestartPolicy:                 corev1.RestartPolicyAlways, DNSPolicy: corev1.DNSClusterFirst,
			SchedulerName: corev1.DefaultSchedulerName, EnableServiceLinks: &enableServiceLinks,
			NodeSelector: map[string]string{corev1.LabelHostname: member.NodeIdentity},
			Tolerations: []corev1.Toleration{{
				Key: "vela.ai/h3", Operator: corev1.TolerationOpEqual,
				Value: "true", Effect: corev1.TaintEffectNoSchedule,
			}},
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: &runAsNonRoot, RunAsUser: &runAsUser, RunAsGroup: &runAsGroup,
				FSGroup: &fsGroup, FSGroupChangePolicy: valuePointer(corev1.FSGroupChangeOnRootMismatch),
				SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			InitContainers: []corev1.Container{h3SocketInitializer(bundle.InitImage)},
			Containers: []corev1.Container{
				workerInstanceAgentContainer(bundle, worker, member),
				workerInstanceRuntimeContainer(bundle, worker, member, string(runtimes)),
			},
			Volumes: workerInstanceVolumes(bundle, worker, member),
		},
	}
	return pod, nil
}

func workerInstanceAgentContainer(
	bundle WorkerBundleActuation,
	worker WorkerInstanceActuation,
	member WorkerMemberActuation,
) corev1.Container {
	return withKubernetesContainerDefaults(corev1.Container{
		Name: "worker-agent", Image: bundle.WorkerAgentImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Env: []corev1.EnvVar{
			literalEnvironment("VELA_WORKER_INSTANCE_ID", worker.ID.String()),
			literalEnvironment("VELA_WORKER_INSTANCE_EPOCH", strconv.FormatInt(worker.InstanceEpoch, 10)),
			literalEnvironment("VELA_WORKER_MEMBER_ID", member.ID.String()),
			literalEnvironment("VELA_WORKER_MEMBER_KEY", member.Key),
			fieldEnvironment("VELA_WORKER_NODE_IDENTITY", "spec.nodeName"),
			configMapEnvironment("VELA_WORKER_CONTROL_ADDRESS", bundle.WorkerRuntimeConfigMap, "control-address"),
			configMapEnvironment("VELA_WORKER_CONTROL_SERVER_NAME", bundle.WorkerRuntimeConfigMap, "control-server-name"),
			literalEnvironment("VELA_WORKER_TLS_CERT_FILE", "/etc/vela-worker-tls/tls.crt"),
			literalEnvironment("VELA_WORKER_TLS_KEY_FILE", "/etc/vela-worker-tls/tls.key"),
			literalEnvironment("VELA_WORKER_CONTROL_CA_FILE", "/etc/vela-worker-tls/ca.crt"),
			literalEnvironment("VELA_WORKER_RUNNER_SOCKET", "/run/vela-runner/private/runner.sock"),
			literalEnvironment("VELA_WORKER_SCRATCH_ROOT", workerScratchRoot),
		},
		Resources: corev1.ResourceRequirements{
			Requests: resourceList(map[corev1.ResourceName]string{
				corev1.ResourceCPU: "1", corev1.ResourceMemory: "1Gi",
			}),
			Limits: resourceList(map[corev1.ResourceName]string{
				corev1.ResourceCPU: "2", corev1.ResourceMemory: "2Gi",
			}),
		},
		SecurityContext: unprivilegedContainerSecurityContext(),
		VolumeMounts: []corev1.VolumeMount{
			{Name: "runner-socket", MountPath: runnerSocketRoot},
			{Name: "scratch", MountPath: workerScratchRoot},
			{Name: "worker-control-tls", MountPath: "/etc/vela-worker-tls", ReadOnly: true},
			{Name: "artifact-store-tls", MountPath: "/etc/vela-artifact-store-tls", ReadOnly: true},
		},
	})
}

func workerInstanceRuntimeContainer(
	bundle WorkerBundleActuation,
	worker WorkerInstanceActuation,
	member WorkerMemberActuation,
	modelRuntimesJSON string,
) corev1.Container {
	gpuCount := strconv.Itoa(member.DeviceCount)
	return withKubernetesContainerDefaults(corev1.Container{
		Name: "model-runtime", Image: bundle.RuntimeImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"/opt/vela/bin/stage-runtime-supervisor"},
		Env: []corev1.EnvVar{
			literalEnvironment("VELA_WORKER_INSTANCE_ID", worker.ID.String()),
			literalEnvironment("VELA_WORKER_INSTANCE_EPOCH", strconv.FormatInt(worker.InstanceEpoch, 10)),
			literalEnvironment("VELA_WORKER_MEMBER_ID", member.ID.String()),
			literalEnvironment("VELA_WORKER_MEMBER_KEY", member.Key),
			literalEnvironment("VELA_MODEL_RUNTIMES_JSON", modelRuntimesJSON),
			literalEnvironment("VELA_CAPACITY_SLOTS", strconv.Itoa(worker.CapacitySlots)),
			literalEnvironment("VELA_RUNNER_SOCKET", "/run/vela-runner/private/runner.sock"),
			literalEnvironment("VELA_RUNNER_SCRATCH_ROOT", workerScratchRoot),
		},
		Resources: corev1.ResourceRequirements{
			Requests: resourceList(map[corev1.ResourceName]string{
				corev1.ResourceCPU: "4", corev1.ResourceMemory: "32Gi", "nvidia.com/gpu": gpuCount,
			}),
			Limits: resourceList(map[corev1.ResourceName]string{
				corev1.ResourceCPU: "16", corev1.ResourceMemory: "128Gi", "nvidia.com/gpu": gpuCount,
			}),
		},
		SecurityContext: unprivilegedContainerSecurityContext(),
		VolumeMounts: []corev1.VolumeMount{
			{Name: "runner-socket", MountPath: runnerSocketRoot},
			{Name: "scratch", MountPath: workerScratchRoot},
			{Name: "model-weights", MountPath: "/var/lib/vela/models", ReadOnly: true},
		},
	})
}

func workerInstanceVolumes(
	bundle WorkerBundleActuation,
	worker WorkerInstanceActuation,
	member WorkerMemberActuation,
) []corev1.Volume {
	mode0400 := int32(0o400)
	mode0440 := int32(0o440)
	scratchPath := "/var/lib/vela/worker-instances/" + worker.ID.String() + "/" + member.Key
	return []corev1.Volume{
		{Name: "runner-socket", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
			Medium: corev1.StorageMediumMemory, SizeLimit: quantityPointer("16Mi"),
		}}},
		{Name: "runner-scratch-view", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
			Medium: corev1.StorageMediumMemory, SizeLimit: quantityPointer("1Mi"),
		}}},
		{Name: "scratch", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
			Path: scratchPath, Type: valuePointer(corev1.HostPathDirectoryOrCreate),
		}}},
		{Name: "model-weights", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
			Path: "/var/lib/vela/models", Type: valuePointer(corev1.HostPathDirectory),
		}}},
		{Name: "worker-control-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: bundle.WorkerControlTLSSecret, DefaultMode: &mode0400,
		}}},
		{Name: "artifact-store-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: bundle.ArtifactStoreTLSSecret, DefaultMode: &mode0440,
			Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
		}}},
	}
}

func workerInstancePodName(workerID uuid.UUID, memberKey string) string {
	memberDigest := sha256.Sum256([]byte(memberKey))
	return "wi-" + strings.ReplaceAll(workerID.String(), "-", "") + "-" +
		hex.EncodeToString(memberDigest[:4])
}

func workerInstancePodMatches(live, desired corev1.Pod) bool {
	normalize := func(pod corev1.Pod) corev1.Pod {
		copy := *pod.DeepCopy()
		copy.ResourceVersion = ""
		copy.Generation = 0
		copy.UID = ""
		copy.CreationTimestamp = metav1.Time{}
		copy.ManagedFields = nil
		copy.Status = corev1.PodStatus{}
		return copy
	}
	return reflect.DeepEqual(normalize(live), normalize(desired))
}
