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
	"github.com/vivym/vela/internal/fleetcontract"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	workerInstanceIDLabel       = fleetcontract.WorkerInstanceIDLabel
	workerInstanceEpochLabel    = fleetcontract.WorkerInstanceEpochLabel
	workerMemberIDLabel         = fleetcontract.WorkerMemberIDLabel
	workerMemberKeyLabel        = fleetcontract.WorkerMemberKeyLabel
	workerBundleIDLabel         = fleetcontract.WorkerBundleIDLabel
	workerProfileRevisionLabel  = fleetcontract.WorkerProfileRevisionIDLabel
	capacityPoolIDLabel         = fleetcontract.CapacityPoolIDLabel
	residencyPlanRevisionLabel  = fleetcontract.ResidencyPlanRevisionIDLabel
	workerRoleLabel             = fleetcontract.WorkerRoleLabel
	deviceConstraintsAnnotation = fleetcontract.DeviceConstraintsAnnotation
	actuationRevisionAnnotation = fleetcontract.ActuationRevisionAnnotation
	h3AUXSharedSlotException    = "H3_AUX_ENCODER_VAE"
)

var (
	actuatorGPUUUIDPattern = regexp.MustCompile(
		`^GPU-[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`,
	)
	actuatorPCIBDFPattern = regexp.MustCompile(`^[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}[.][0-7]$`)
)

type WorkerInstancePodResources interface {
	GetWorkerInstanceGPUClaimTemplate(
		context.Context,
		ResourceKey,
	) (resourcev1.ResourceClaimTemplate, error)
	CreateWorkerInstanceGPUClaimTemplate(context.Context, resourcev1.ResourceClaimTemplate) error
	GetWorkerInstancePod(context.Context, ResourceKey) (corev1.Pod, error)
	CreateWorkerInstancePod(context.Context, corev1.Pod) error
}

type WorkerInstanceActuator struct {
	resources WorkerInstancePodResources
}

type WorkerBundleActuation struct {
	SchemaVersion          int                       `json:"schema_version"`
	PlanRevisionID         uuid.UUID                 `json:"plan_revision_id"`
	WorkerBundleID         uuid.UUID                 `json:"worker_bundle_id"`
	RevisionDigest         string                    `json:"revision_digest"`
	Namespace              string                    `json:"namespace"`
	InitImage              string                    `json:"init_image"`
	WorkerAgentImage       string                    `json:"worker_agent_image"`
	RuntimeImage           string                    `json:"runtime_image"`
	ArtifactStoreTLSSecret string                    `json:"artifact_store_tls_secret"`
	WorkerRuntimeConfigMap string                    `json:"worker_runtime_config_map"`
	WorkerControlTLSSecret string                    `json:"worker_control_tls_secret"`
	WorkerInstances        []WorkerInstanceActuation `json:"worker_instances"`
}

type WorkerInstanceActuation struct {
	ID                      uuid.UUID               `json:"id"`
	InstanceEpoch           int64                   `json:"instance_epoch"`
	WorkerProfileRevisionID uuid.UUID               `json:"worker_profile_revision_id"`
	CapacityPoolID          uuid.UUID               `json:"capacity_pool_id"`
	Role                    string                  `json:"role"`
	CapacitySlots           int                     `json:"capacity_slots"`
	SharedSlotException     string                  `json:"shared_slot_exception,omitempty"`
	ModelRuntimes           []ModelRuntimeProcess   `json:"model_runtimes"`
	Members                 []WorkerMemberActuation `json:"members"`
}

type WorkerMemberActuation struct {
	ID                uuid.UUID          `json:"id"`
	Key               string             `json:"key"`
	NodeIdentity      string             `json:"node_identity"`
	ResourceClass     string             `json:"resource_class"`
	DeviceCount       int                `json:"device_count"`
	DeviceConstraints []DeviceConstraint `json:"device_constraints,omitempty"`
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
	CreatedGPUClaims int
	CreatedPods      int
	Converged        bool
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
		spec.WorkerBundleID == uuid.Nil ||
		(spec.RevisionDigest != "" && !validSHA256(spec.RevisionDigest)) ||
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
	expectedDigest := spec.RevisionDigest
	bundle := WorkerBundleActuation{
		SchemaVersion: 1, PlanRevisionID: spec.PlanRevisionID,
		WorkerBundleID: spec.WorkerBundleID,
		Namespace:      spec.Namespace, InitImage: spec.InitImage,
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
	computedDigest, err := ComputeWorkerBundleActuationDigest(bundle)
	if err != nil {
		return WorkerBundleActuation{}, err
	}
	bundle.RevisionDigest = computedDigest
	if expectedDigest != "" && expectedDigest != bundle.RevisionDigest {
		return WorkerBundleActuation{}, errors.New("certified H3 WorkerBundle revision digest does not match its actuation content")
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
	desiredClaims, err := materializeWorkerInstanceGPUClaimTemplates(bundle)
	if err != nil {
		return WorkerInstanceActuationResult{}, err
	}
	desiredPods, err := materializeWorkerInstancePods(bundle)
	if err != nil {
		return WorkerInstanceActuationResult{}, err
	}
	missingClaims := make([]resourcev1.ResourceClaimTemplate, 0, len(desiredClaims))
	missingClaimNames := make(map[string]struct{}, len(desiredClaims))
	for _, desired := range desiredClaims {
		live, err := actuator.resources.GetWorkerInstanceGPUClaimTemplate(ctx, ResourceKey{
			Namespace: bundle.Namespace, Name: desired.Name,
		})
		if errors.Is(err, ErrResourceNotFound) {
			missingClaims = append(missingClaims, desired)
			missingClaimNames[desired.Name] = struct{}{}
			continue
		}
		if err != nil {
			return WorkerInstanceActuationResult{}, fmt.Errorf(
				"get WorkerInstance GPU ResourceClaimTemplate %q: %w", desired.Name, err,
			)
		}
		if !workerInstanceGPUClaimTemplateMatches(live, desired) {
			return WorkerInstanceActuationResult{}, ErrProtectedResourceDrift
		}
	}
	missingPods := make([]corev1.Pod, 0, len(desiredPods))
	for _, desired := range desiredPods {
		live, err := actuator.resources.GetWorkerInstancePod(ctx, ResourceKey{
			Namespace: bundle.Namespace, Name: desired.Name,
		})
		if errors.Is(err, ErrResourceNotFound) {
			missingPods = append(missingPods, desired)
			continue
		}
		if err != nil {
			return WorkerInstanceActuationResult{}, fmt.Errorf("get WorkerInstance Pod %q: %w", desired.Name, err)
		}
		if claimName := workerInstancePodGPUClaimTemplateName(desired); claimName != "" {
			if _, missing := missingClaimNames[claimName]; missing {
				return WorkerInstanceActuationResult{}, ErrProtectedResourceDrift
			}
		}
		if !workerInstancePodMatches(live, desired) {
			return WorkerInstanceActuationResult{}, ErrProtectedResourceDrift
		}
	}
	for _, claim := range missingClaims {
		if err := actuator.resources.CreateWorkerInstanceGPUClaimTemplate(ctx, claim); err != nil {
			return WorkerInstanceActuationResult{}, fmt.Errorf(
				"create WorkerInstance GPU ResourceClaimTemplate %q: %w", claim.Name, err,
			)
		}
	}
	for _, pod := range missingPods {
		if err := actuator.resources.CreateWorkerInstancePod(ctx, pod); err != nil {
			return WorkerInstanceActuationResult{}, fmt.Errorf("create WorkerInstance Pod %q: %w", pod.Name, err)
		}
	}
	return WorkerInstanceActuationResult{
		CreatedGPUClaims: len(missingClaims),
		CreatedPods:      len(missingPods),
		Converged:        len(missingClaims) == 0 && len(missingPods) == 0,
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
	digest, err := ComputeWorkerBundleActuationDigest(bundle)
	if err != nil || digest != bundle.RevisionDigest {
		return errors.New("WorkerBundle actuation revision digest does not match its content")
	}
	return nil
}

func ComputeWorkerBundleActuationDigest(bundle WorkerBundleActuation) (string, error) {
	canonical := cloneWorkerBundleActuation(bundle)
	canonical.RevisionDigest = ""
	encoded, err := json.Marshal(struct {
		Schema string                `json:"schema"`
		Bundle WorkerBundleActuation `json:"bundle"`
	}{
		Schema: "vela.worker-bundle-actuation/v1",
		Bundle: canonical,
	})
	if err != nil {
		return "", fmt.Errorf("encode canonical WorkerBundle actuation: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
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

func materializeWorkerInstanceGPUClaimTemplates(
	bundle WorkerBundleActuation,
) ([]resourcev1.ResourceClaimTemplate, error) {
	claims := make([]resourcev1.ResourceClaimTemplate, 0)
	for _, worker := range bundle.WorkerInstances {
		for _, member := range worker.Members {
			if len(member.DeviceConstraints) != member.DeviceCount {
				return nil, errors.New("WorkerMember exact GPU device constraints are incomplete")
			}
			requests := make([]resourcev1.DeviceRequest, 0, len(member.DeviceConstraints))
			for index, constraint := range member.DeviceConstraints {
				expression := "device.driver == 'gpu.nvidia.com'" +
					" && device.attributes['gpu.nvidia.com'].type == 'gpu'" +
					" && device.attributes['gpu.nvidia.com'].uuid == '" + constraint.GPUUUID + "'" +
					" && device.attributes['resource.kubernetes.io'].pciBusID == '" + constraint.PCIBDF + "'"
				requests = append(requests, resourcev1.DeviceRequest{
					Name: "gpu-" + strconv.Itoa(index),
					Exactly: &resourcev1.ExactDeviceRequest{
						DeviceClassName: "gpu.nvidia.com",
						Selectors: []resourcev1.DeviceSelector{{
							CEL: &resourcev1.CELDeviceSelector{Expression: expression},
						}},
						AllocationMode: resourcev1.DeviceAllocationModeExactCount,
						Count:          1,
					},
				})
			}
			labels := map[string]string{
				"app.kubernetes.io/name":    "vela-worker-instance-gpu",
				"app.kubernetes.io/part-of": "vela",
				workerInstanceIDLabel:       worker.ID.String(),
				workerInstanceEpochLabel:    strconv.FormatInt(worker.InstanceEpoch, 10),
				workerMemberIDLabel:         member.ID.String(),
				workerMemberKeyLabel:        member.Key,
				workerBundleIDLabel:         bundle.WorkerBundleID.String(),
				residencyPlanRevisionLabel:  bundle.PlanRevisionID.String(),
			}
			annotations := map[string]string{
				"vela.ai/fleet-controller-owned": "true",
				deviceConstraintsAnnotation:      mustEncodeDeviceConstraints(member.DeviceConstraints),
				actuationRevisionAnnotation:      bundle.RevisionDigest,
			}
			claims = append(claims, resourcev1.ResourceClaimTemplate{
				TypeMeta: metav1.TypeMeta{
					APIVersion: resourcev1.SchemeGroupVersion.String(),
					Kind:       "ResourceClaimTemplate",
				},
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   bundle.Namespace,
					Name:        workerInstanceGPUClaimTemplateName(worker.ID, member.Key),
					Labels:      labels,
					Annotations: annotations,
				},
				Spec: resourcev1.ResourceClaimTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels:      cloneStringMap(labels),
						Annotations: cloneStringMap(annotations),
					},
					Spec: resourcev1.ResourceClaimSpec{Devices: resourcev1.DeviceClaim{
						Requests: requests,
					}},
				},
			})
		}
	}
	sort.Slice(claims, func(left, right int) bool { return claims[left].Name < claims[right].Name })
	return claims, nil
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
			ResourceClaims: []corev1.PodResourceClaim{{
				Name: "gpu",
				ResourceClaimTemplateName: valuePointer(
					workerInstanceGPUClaimTemplateName(worker.ID, member.Key),
				),
			}},
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
				corev1.ResourceCPU: "4", corev1.ResourceMemory: "32Gi",
			}),
			Limits: resourceList(map[corev1.ResourceName]string{
				corev1.ResourceCPU: "16", corev1.ResourceMemory: "128Gi",
			}),
			Claims: []corev1.ResourceClaim{{Name: "gpu"}},
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

func workerInstanceGPUClaimTemplateName(workerID uuid.UUID, memberKey string) string {
	return workerInstancePodName(workerID, memberKey) + "-gpu"
}

func workerInstancePodGPUClaimTemplateName(pod corev1.Pod) string {
	if len(pod.Spec.ResourceClaims) != 1 ||
		pod.Spec.ResourceClaims[0].ResourceClaimTemplateName == nil {
		return ""
	}
	return *pod.Spec.ResourceClaims[0].ResourceClaimTemplateName
}

func workerInstanceGPUClaimTemplateMatches(
	live resourcev1.ResourceClaimTemplate,
	desired resourcev1.ResourceClaimTemplate,
) bool {
	normalize := func(claim resourcev1.ResourceClaimTemplate) resourcev1.ResourceClaimTemplate {
		copy := *claim.DeepCopy()
		copy.ResourceVersion = ""
		copy.Generation = 0
		copy.UID = ""
		copy.CreationTimestamp = metav1.Time{}
		copy.ManagedFields = nil
		return copy
	}
	return reflect.DeepEqual(normalize(live), normalize(desired))
}

func mustEncodeDeviceConstraints(constraints []DeviceConstraint) string {
	encoded, err := json.Marshal(constraints)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func cloneStringMap(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
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
