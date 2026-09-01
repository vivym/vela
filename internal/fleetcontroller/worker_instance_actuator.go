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
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontract"
	"github.com/vivym/vela/internal/modelruntime"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	workerInstanceIDLabel        = fleetcontract.WorkerInstanceIDLabel
	workerInstanceEpochLabel     = fleetcontract.WorkerInstanceEpochLabel
	workerMemberIDLabel          = fleetcontract.WorkerMemberIDLabel
	workerMemberKeyLabel         = fleetcontract.WorkerMemberKeyLabel
	workerBundleIDLabel          = fleetcontract.WorkerBundleIDLabel
	workerProfileRevisionLabel   = fleetcontract.WorkerProfileRevisionIDLabel
	capacityPoolIDLabel          = fleetcontract.CapacityPoolIDLabel
	residencyPlanRevisionLabel   = fleetcontract.ResidencyPlanRevisionIDLabel
	workerRoleLabel              = fleetcontract.WorkerRoleLabel
	deviceConstraintsAnnotation  = fleetcontract.DeviceConstraintsAnnotation
	workerNodeIdentityAnnotation = fleetcontract.WorkerNodeIdentityAnnotation
	actuationRevisionAnnotation  = fleetcontract.ActuationRevisionAnnotation
	h3AUXSharedSlotException     = "H3_AUX_ENCODER_VAE"
	stageWorkerScratchRoot       = "/var/lib/vela/stage-worker/scratch"
	modelRuntimeSocketRoot       = "/run/vela-model-runtime"
	modelRuntimeSocketPath       = modelRuntimeSocketRoot + "/private/runtime.sock"
	modelRuntimePrivateRoot      = "/etc/vela-model-runtime/private"
	modelRuntimeLaunchManifest   = modelRuntimePrivateRoot + "/launch.json"
	modelRuntimeVerifierKeyring  = modelRuntimePrivateRoot + "/authority/verifier-keyring.json"
	modelRuntimeEpochDirectory   = stageWorkerScratchRoot + "/model-runtime-epochs"
)

var (
	actuatorGPUUUIDPattern = regexp.MustCompile(
		`^GPU-[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`,
	)
	actuatorPCIBDFPattern = regexp.MustCompile(`^[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}[.][0-7]$`)
)

type WorkerInstancePodResources interface {
	ListWorkerInstanceGPUClaimTemplates(context.Context) ([]resourcev1.ResourceClaimTemplate, error)
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
	SchemaVersion                  int                       `json:"schema_version"`
	PlanRevisionID                 uuid.UUID                 `json:"plan_revision_id"`
	WorkerBundleID                 uuid.UUID                 `json:"worker_bundle_id"`
	RevisionDigest                 string                    `json:"revision_digest"`
	Namespace                      string                    `json:"namespace"`
	InitImage                      string                    `json:"init_image"`
	StageWorkerAgentImage          string                    `json:"stage_worker_agent_image"`
	RuntimeImage                   string                    `json:"runtime_image"`
	StageWorkerConfigMap           string                    `json:"stage_worker_config_map"`
	ModelRuntimeVerifierConfigMap  string                    `json:"model_runtime_verifier_config_map"`
	StageWorkerControlTLSSecret    string                    `json:"stage_worker_control_tls_secret"`
	StageWorkerAuthoritySecret     string                    `json:"stage_worker_authority_secret"`
	ArtifactStoreCredentialsSecret string                    `json:"artifact_store_credentials_secret"`
	ArtifactStoreCASecret          string                    `json:"artifact_store_ca_secret"`
	WorkerInstances                []WorkerInstanceActuation `json:"worker_instances"`
}

type WorkerInstanceActuation struct {
	ID                      uuid.UUID               `json:"id"`
	InstanceEpoch           int64                   `json:"instance_epoch"`
	WorkerProfileRevisionID uuid.UUID               `json:"worker_profile_revision_id"`
	CapacityPoolID          uuid.UUID               `json:"capacity_pool_id"`
	Role                    string                  `json:"role"`
	CapacitySlots           int                     `json:"capacity_slots"`
	SharedSlotException     string                  `json:"shared_slot_exception,omitempty"`
	DeviceSetDigest         string                  `json:"device_set_digest"`
	MembershipDigest        string                  `json:"membership_digest"`
	ModelRuntimes           []ModelRuntimeProcess   `json:"model_runtimes"`
	Members                 []WorkerMemberActuation `json:"members"`
}

type WorkerMemberActuation struct {
	ID                uuid.UUID          `json:"id"`
	MemberEpoch       int64              `json:"member_epoch"`
	Key               string             `json:"key"`
	NodeIdentity      string             `json:"node_identity"`
	ResourceClass     string             `json:"resource_class"`
	DeviceCount       int                `json:"device_count"`
	DeviceConstraints []DeviceConstraint `json:"device_constraints,omitempty"`
}

type DeviceConstraint struct {
	DeviceID    uuid.UUID `json:"device_id"`
	DeviceEpoch int64     `json:"device_epoch"`
	GPUUUID     string    `json:"gpu_uuid"`
	PCIBDF      string    `json:"pci_bdf"`
}

type ModelRuntimeProcess struct {
	ModelResidencyID       uuid.UUID `json:"model_residency_id"`
	StageProfileRevisionID uuid.UUID `json:"stage_profile_revision_id"`
	ModelRuntimeEpochFloor int64     `json:"model_runtime_epoch_floor"`
	Component              string    `json:"component"`
	ModelComponentRevision string    `json:"model_component_revision"`
	RuntimeIdentity        string    `json:"runtime_identity"`
	Command                []string  `json:"command"`
	Environment            []string  `json:"environment,omitempty"`
	InitializationTimeout  string    `json:"initialization_timeout"`
	ShutdownTimeout        string    `json:"shutdown_timeout"`
}

type H3WorkerBundleSpec struct {
	SchemaVersion                  int
	PlanRevisionID                 uuid.UUID
	WorkerBundleID                 uuid.UUID
	RevisionDigest                 string
	Namespace                      string
	NodeIdentity                   string
	AuxCapacityPoolID              uuid.UUID
	DiTCapacityPoolID              uuid.UUID
	AuxWorkerProfileRevisionID     uuid.UUID
	DiTWorkerProfileRevisionID     uuid.UUID
	InitImage                      string
	StageWorkerAgentImage          string
	RuntimeImage                   string
	StageWorkerConfigMap           string
	ModelRuntimeVerifierConfigMap  string
	StageWorkerControlTLSSecret    string
	StageWorkerAuthoritySecret     string
	ArtifactStoreCredentialsSecret string
	ArtifactStoreCASecret          string
	Devices                        [8]DeviceConstraint
	MemberEpochs                   [8]int64
	DeviceSetDigests               [8]string
	MembershipDigests              [8]string
	Encoder                        ModelRuntimeProcess
	DiT                            [7]ModelRuntimeProcess
	VAEDecoder                     ModelRuntimeProcess
}

type WorkerInstanceActuationResult struct {
	CreatedGPUClaims int
	CreatedPods      int
	Converged        bool
}

type deviceOwnership struct {
	deviceIDs map[uuid.UUID]struct{}
	gpuUUIDs  map[string]struct{}
	pciSlots  map[string]struct{}
}

func newDeviceOwnership() *deviceOwnership {
	return &deviceOwnership{
		deviceIDs: make(map[uuid.UUID]struct{}),
		gpuUUIDs:  make(map[string]struct{}),
		pciSlots:  make(map[string]struct{}),
	}
}

func (ownership *deviceOwnership) reserve(nodeIdentity string, constraint DeviceConstraint) error {
	if _, exists := ownership.deviceIDs[constraint.DeviceID]; exists {
		return errors.New("one Device authority is assigned more than once")
	}
	if _, exists := ownership.gpuUUIDs[constraint.GPUUUID]; exists {
		return errors.New("one GPU is assigned to multiple WorkerInstances")
	}
	pciSlot := nodeIdentity + "\x00" + constraint.PCIBDF
	if _, exists := ownership.pciSlots[pciSlot]; exists {
		return errors.New("one node PCI device is assigned to multiple WorkerInstances")
	}
	ownership.deviceIDs[constraint.DeviceID] = struct{}{}
	ownership.gpuUUIDs[constraint.GPUUUID] = struct{}{}
	ownership.pciSlots[pciSlot] = struct{}{}
	return nil
}

func (ownership *deviceOwnership) conflicts(nodeIdentity string, constraint DeviceConstraint) bool {
	_, deviceConflict := ownership.deviceIDs[constraint.DeviceID]
	_, gpuConflict := ownership.gpuUUIDs[constraint.GPUUUID]
	_, pciConflict := ownership.pciSlots[nodeIdentity+"\x00"+constraint.PCIBDF]
	return deviceConflict || gpuConflict || pciConflict
}

func reserveWorkerBundleDevices(ownership *deviceOwnership, bundle WorkerBundleActuation) error {
	for _, worker := range bundle.WorkerInstances {
		for _, member := range worker.Members {
			for _, constraint := range member.DeviceConstraints {
				if err := ownership.reserve(member.NodeIdentity, constraint); err != nil {
					return err
				}
			}
		}
	}
	return nil
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
		!validPinnedImage(spec.InitImage) || !validPinnedImage(spec.StageWorkerAgentImage) ||
		!validPinnedImage(spec.RuntimeImage) || !validResourceName(spec.StageWorkerConfigMap) ||
		!validResourceName(spec.ModelRuntimeVerifierConfigMap) ||
		!validResourceName(spec.StageWorkerControlTLSSecret) ||
		!validResourceName(spec.StageWorkerAuthoritySecret) ||
		!validResourceName(spec.ArtifactStoreCredentialsSecret) ||
		!validResourceName(spec.ArtifactStoreCASecret) ||
		!validModelRuntimeProcess(spec.Encoder) || spec.Encoder.Component != "ENCODER" ||
		!validModelRuntimeProcess(spec.VAEDecoder) || spec.VAEDecoder.Component != "VAE_DECODER" {
		return WorkerBundleActuation{}, errors.New("certified H3 WorkerBundle specification is invalid")
	}
	for index, device := range spec.Devices {
		if !validDeviceConstraint(device) || spec.MemberEpochs[index] <= 0 ||
			!validSHA256(spec.DeviceSetDigests[index]) || !validSHA256(spec.MembershipDigests[index]) {
			return WorkerBundleActuation{}, errors.New("certified H3 WorkerBundle device constraint is invalid")
		}
	}
	for _, runtime := range spec.DiT {
		if !validModelRuntimeProcess(runtime) || runtime.Component != "DIT" {
			return WorkerBundleActuation{}, errors.New("certified H3 DiT runtime specification is invalid")
		}
	}
	expectedDigest := spec.RevisionDigest
	bundle := WorkerBundleActuation{
		SchemaVersion: 1, PlanRevisionID: spec.PlanRevisionID,
		WorkerBundleID: spec.WorkerBundleID,
		Namespace:      spec.Namespace, InitImage: spec.InitImage,
		StageWorkerAgentImage: spec.StageWorkerAgentImage, RuntimeImage: spec.RuntimeImage,
		StageWorkerConfigMap:           spec.StageWorkerConfigMap,
		ModelRuntimeVerifierConfigMap:  spec.ModelRuntimeVerifierConfigMap,
		StageWorkerControlTLSSecret:    spec.StageWorkerControlTLSSecret,
		StageWorkerAuthoritySecret:     spec.StageWorkerAuthoritySecret,
		ArtifactStoreCredentialsSecret: spec.ArtifactStoreCredentialsSecret,
		ArtifactStoreCASecret:          spec.ArtifactStoreCASecret,
		WorkerInstances:                make([]WorkerInstanceActuation, 0, 8),
	}
	auxID := uuid.NewSHA1(spec.WorkerBundleID, []byte("h3/aux"))
	bundle.WorkerInstances = append(bundle.WorkerInstances, WorkerInstanceActuation{
		ID: auxID, InstanceEpoch: 1,
		WorkerProfileRevisionID: spec.AuxWorkerProfileRevisionID,
		CapacityPoolID:          spec.AuxCapacityPoolID, Role: "aux", CapacitySlots: 1,
		SharedSlotException: h3AUXSharedSlotException,
		DeviceSetDigest:     spec.DeviceSetDigests[0],
		MembershipDigest:    spec.MembershipDigests[0],
		ModelRuntimes:       []ModelRuntimeProcess{spec.Encoder, spec.VAEDecoder},
		Members: []WorkerMemberActuation{
			h3Member(auxID, spec.MemberEpochs[0], spec.NodeIdentity, spec.Devices[0]),
		},
	})
	for index := range 7 {
		workerID := uuid.NewSHA1(spec.WorkerBundleID, []byte("h3/dit/"+strconv.Itoa(index)))
		bundle.WorkerInstances = append(bundle.WorkerInstances, WorkerInstanceActuation{
			ID: workerID, InstanceEpoch: 1,
			WorkerProfileRevisionID: spec.DiTWorkerProfileRevisionID,
			CapacityPoolID:          spec.DiTCapacityPoolID, Role: "dit", CapacitySlots: 1,
			DeviceSetDigest:  spec.DeviceSetDigests[index+1],
			MembershipDigest: spec.MembershipDigests[index+1],
			ModelRuntimes:    []ModelRuntimeProcess{spec.DiT[index]},
			Members: []WorkerMemberActuation{
				h3Member(workerID, spec.MemberEpochs[index+1], spec.NodeIdentity, spec.Devices[index+1]),
			},
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
	memberEpoch int64,
	nodeIdentity string,
	device DeviceConstraint,
) WorkerMemberActuation {
	return WorkerMemberActuation{
		ID: uuid.NewSHA1(workerID, []byte("member-0")), MemberEpoch: memberEpoch, Key: "member-0",
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
	liveClaims, err := actuator.resources.ListWorkerInstanceGPUClaimTemplates(ctx)
	if err != nil {
		return WorkerInstanceActuationResult{}, fmt.Errorf("list WorkerInstance GPU ResourceClaimTemplates: %w", err)
	}
	if err := validateLiveGPUClaimOwnership(liveClaims, desiredClaims); err != nil {
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
		!validPinnedImage(bundle.StageWorkerAgentImage) || !validPinnedImage(bundle.RuntimeImage) ||
		!validResourceName(bundle.StageWorkerConfigMap) ||
		!validResourceName(bundle.ModelRuntimeVerifierConfigMap) ||
		!validResourceName(bundle.StageWorkerControlTLSSecret) ||
		!validResourceName(bundle.StageWorkerAuthoritySecret) ||
		!validResourceName(bundle.ArtifactStoreCredentialsSecret) ||
		!validResourceName(bundle.ArtifactStoreCASecret) ||
		len(bundle.WorkerInstances) == 0 || len(bundle.WorkerInstances) > 4096 {
		return errors.New("WorkerBundle actuation is invalid")
	}
	workers := make(map[uuid.UUID]struct{}, len(bundle.WorkerInstances))
	members := make(map[uuid.UUID]struct{})
	residencies := make(map[uuid.UUID]struct{})
	podNames := make(map[string]struct{})
	deviceOwnership := newDeviceOwnership()
	for _, worker := range bundle.WorkerInstances {
		if worker.ID == uuid.Nil || worker.InstanceEpoch <= 0 ||
			worker.WorkerProfileRevisionID == uuid.Nil || worker.CapacityPoolID == uuid.Nil ||
			!validWorkerRole(worker.Role) || worker.CapacitySlots <= 0 ||
			!validSHA256(worker.DeviceSetDigest) || !validSHA256(worker.MembershipDigest) ||
			len(worker.ModelRuntimes) == 0 || len(worker.Members) == 0 || len(worker.Members) > 64 {
			return errors.New("WorkerInstance actuation is invalid")
		}
		if _, exists := workers[worker.ID]; exists {
			return errors.New("WorkerBundle actuation reuses a WorkerInstance id")
		}
		workers[worker.ID] = struct{}{}
		if worker.SharedSlotException != "" && !validSharedSlotException(worker) {
			return errors.New("WorkerInstance shared-slot exception is invalid")
		}
		if len(worker.ModelRuntimes) > 1 && !validSharedSlotException(worker) {
			return errors.New("multi-model WorkerInstance is not an approved shared-slot exception")
		}
		if worker.Role == "aux" && !validSharedSlotException(worker) {
			return errors.New("H3 AUX WorkerInstance is not the approved shared-slot shape")
		}
		if worker.Role == "dit" && !validSingleGPUH3Worker(worker, "DIT") {
			return errors.New("H3 DiT WorkerInstance must own one GPU and one DiT runtime")
		}
		for _, runtime := range worker.ModelRuntimes {
			if !validModelRuntimeProcess(runtime) {
				return errors.New("WorkerInstance ModelRuntime process is invalid")
			}
			if _, exists := residencies[runtime.ModelResidencyID]; exists {
				return errors.New("WorkerBundle actuation reuses a ModelResidency id")
			}
			residencies[runtime.ModelResidencyID] = struct{}{}
		}
		memberKeys := make(map[string]struct{}, len(worker.Members))
		for _, member := range worker.Members {
			if member.ID == uuid.Nil || member.MemberEpoch <= 0 || !validMemberKey(member.Key) ||
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
				if err := deviceOwnership.reserve(member.NodeIdentity, constraint); err != nil {
					return fmt.Errorf("WorkerBundle actuation %w", err)
				}
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
	return worker.Role == "aux" && worker.SharedSlotException == h3AUXSharedSlotException &&
		worker.CapacitySlots == 1 &&
		len(worker.ModelRuntimes) == 2 && worker.ModelRuntimes[0].Component == "ENCODER" &&
		worker.ModelRuntimes[1].Component == "VAE_DECODER" && len(worker.Members) == 1 &&
		worker.Members[0].DeviceCount == 1
}

func validSingleGPUH3Worker(worker WorkerInstanceActuation, component string) bool {
	return worker.SharedSlotException == "" && worker.CapacitySlots == 1 &&
		len(worker.ModelRuntimes) == 1 && worker.ModelRuntimes[0].Component == component &&
		len(worker.Members) == 1 && worker.Members[0].DeviceCount == 1
}

func validModelRuntimeProcess(runtime ModelRuntimeProcess) bool {
	if runtime.Component != "ENCODER" && runtime.Component != "DIT" &&
		runtime.Component != "VAE_DECODER" && runtime.Component != "LLM" &&
		runtime.Component != "CPU_MEDIA" {
		return false
	}
	if runtime.ModelResidencyID == uuid.Nil || runtime.StageProfileRevisionID == uuid.Nil ||
		runtime.ModelRuntimeEpochFloor <= 0 || !validBoundedText(runtime.ModelComponentRevision, 300) ||
		!validBoundedText(runtime.RuntimeIdentity, 500) || len(runtime.Command) == 0 ||
		len(runtime.Command) > 128 || !strings.HasPrefix(runtime.Command[0], "/") {
		return false
	}
	for _, argument := range runtime.Command {
		if !validBoundedText(argument, 4096) {
			return false
		}
	}
	if len(runtime.Environment) > 128 {
		return false
	}
	seenEnvironment := make(map[string]struct{}, len(runtime.Environment))
	for _, entry := range runtime.Environment {
		name, _, found := strings.Cut(entry, "=")
		if !found || name == "" || len(entry) > 4096 || strings.ContainsAny(name, "\x00=") ||
			strings.ContainsRune(entry, '\x00') || name == "VELA_MODEL_DRIVER_PROTOCOL" {
			return false
		}
		if _, duplicate := seenEnvironment[name]; duplicate {
			return false
		}
		seenEnvironment[name] = struct{}{}
	}
	initializationTimeout, initializationErr := time.ParseDuration(runtime.InitializationTimeout)
	shutdownTimeout, shutdownErr := time.ParseDuration(runtime.ShutdownTimeout)
	if initializationErr != nil || initializationTimeout <= 0 || initializationTimeout > 24*time.Hour ||
		shutdownErr != nil || shutdownTimeout <= 0 || shutdownTimeout > 10*time.Minute {
		return false
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
	return constraint.DeviceID != uuid.Nil && constraint.DeviceEpoch > 0 &&
		actuatorGPUUUIDPattern.MatchString(constraint.GPUUUID) &&
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

func encodeModelRuntimeLaunchManifest(
	bundle WorkerBundleActuation,
	worker WorkerInstanceActuation,
	localMember WorkerMemberActuation,
) (string, error) {
	runtimeImageDigest, ok := strings.CutPrefix(
		bundle.RuntimeImage[strings.LastIndex(bundle.RuntimeImage, "@")+1:], "sha256:",
	)
	if !ok || !validSHA256(runtimeImageDigest) {
		return "", errors.New("ModelRuntime image digest is invalid")
	}
	manifest := modelruntime.LaunchManifest{
		SchemaVersion: 1, WorkerProfileRevisionID: worker.WorkerProfileRevisionID.String(),
		WorkerRole: worker.Role, CapacitySlots: worker.CapacitySlots,
		SharedSlotException: worker.SharedSlotException,
		WorkerInstanceID:    worker.ID.String(), WorkerInstanceEpoch: worker.InstanceEpoch,
		WorkerMemberID: localMember.ID.String(), WorkerMemberEpoch: localMember.MemberEpoch,
		DeviceSetDigest: worker.DeviceSetDigest, MembershipDigest: worker.MembershipDigest,
	}
	for _, member := range worker.Members {
		manifest.Members = append(manifest.Members, modelruntime.LaunchMemberEpoch{
			ID: member.ID.String(), Epoch: member.MemberEpoch,
		})
		for _, device := range member.DeviceConstraints {
			manifest.Devices = append(manifest.Devices, modelruntime.LaunchDeviceEpoch{
				ID: device.DeviceID.String(), Epoch: device.DeviceEpoch,
			})
		}
	}
	for _, device := range localMember.DeviceConstraints {
		manifest.LocalDevices = append(manifest.LocalDevices, modelruntime.DriverDevice{
			DeviceID: device.DeviceID.String(), DeviceEpoch: device.DeviceEpoch,
			GPUUUID: device.GPUUUID, PCIBDF: device.PCIBDF,
		})
	}
	for _, runtime := range worker.ModelRuntimes {
		scratchRoot := modelRuntimeScratchRoot(runtime.Component)
		manifest.Runtimes = append(manifest.Runtimes, modelruntime.LaunchRuntime{
			ModelResidencyID:       runtime.ModelResidencyID.String(),
			RuntimeIdentity:        runtime.RuntimeIdentity,
			StageProfileRevisionID: runtime.StageProfileRevisionID.String(),
			ModelRuntimeEpochFloor: runtime.ModelRuntimeEpochFloor,
			Component:              runtime.Component,
			ModelComponentRevision: runtime.ModelComponentRevision,
			RuntimeImageDigest:     runtimeImageDigest,
			Command:                append([]string(nil), runtime.Command...),
			Environment:            append([]string(nil), runtime.Environment...),
			ScratchRoot:            scratchRoot, OutputRoot: scratchRoot + "/outputs",
			InitializationTimeout: runtime.InitializationTimeout,
			ShutdownTimeout:       runtime.ShutdownTimeout,
		})
	}
	encoded, err := modelruntime.EncodeLaunchManifest(manifest)
	if err != nil {
		return "", fmt.Errorf("validate and encode ModelRuntime launch manifest: %w", err)
	}
	return string(encoded), nil
}

func modelRuntimeScratchRoot(component string) string {
	return stageWorkerScratchRoot + "/model-runtime/" + strings.ToLower(component)
}

func modelRuntimeTerminationGraceSeconds(worker WorkerInstanceActuation) int64 {
	maximum := time.Duration(0)
	for _, runtime := range worker.ModelRuntimes {
		shutdown, err := time.ParseDuration(runtime.ShutdownTimeout)
		if err == nil && shutdown > maximum {
			maximum = shutdown
		}
	}
	return int64((maximum + 30*time.Second + time.Second - 1) / time.Second)
}

func modelRuntimeDirectories(worker WorkerInstanceActuation) string {
	directories := []string{modelRuntimeEpochDirectory}
	for _, runtime := range worker.ModelRuntimes {
		scratchRoot := modelRuntimeScratchRoot(runtime.Component)
		directories = append(directories, scratchRoot, scratchRoot+"/outputs")
	}
	return strings.Join(directories, " ")
}

// MaterializeWorkerInstanceLaunchResources returns the exact Pod and DRA claim
// templates authorized by a validated WorkerBundle. It is shared with launch
// evidence collection so evidence is compared with the same contract used for
// actuation.
func MaterializeWorkerInstanceLaunchResources(
	bundle WorkerBundleActuation,
) ([]corev1.Pod, []resourcev1.ResourceClaimTemplate, error) {
	if err := ValidateWorkerBundleActuation(bundle); err != nil {
		return nil, nil, err
	}
	pods, err := materializeWorkerInstancePods(bundle)
	if err != nil {
		return nil, nil, err
	}
	claims, err := materializeWorkerInstanceGPUClaimTemplates(bundle)
	if err != nil {
		return nil, nil, err
	}
	return pods, claims, nil
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
				workerNodeIdentityAnnotation:     member.NodeIdentity,
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

func validateLiveGPUClaimOwnership(
	liveClaims []resourcev1.ResourceClaimTemplate,
	desiredClaims []resourcev1.ResourceClaimTemplate,
) error {
	desiredNames := make(map[string]struct{}, len(desiredClaims))
	desiredOwnership := newDeviceOwnership()
	for _, claim := range desiredClaims {
		desiredNames[claim.Name] = struct{}{}
		nodeIdentity, constraints, ok := gpuClaimOwnership(claim)
		if !ok {
			return ErrProtectedResourceDrift
		}
		for _, constraint := range constraints {
			if err := desiredOwnership.reserve(nodeIdentity, constraint); err != nil {
				return ErrProtectedResourceDrift
			}
		}
	}
	for _, claim := range liveClaims {
		if _, desired := desiredNames[claim.Name]; desired {
			continue
		}
		if claim.Annotations["vela.ai/fleet-controller-owned"] != "true" {
			continue
		}
		nodeIdentity, constraints, ok := gpuClaimOwnership(claim)
		if !ok {
			return ErrProtectedResourceDrift
		}
		for _, constraint := range constraints {
			if desiredOwnership.conflicts(nodeIdentity, constraint) {
				return ErrProtectedResourceDrift
			}
		}
	}
	return nil
}

func gpuClaimOwnership(claim resourcev1.ResourceClaimTemplate) (string, []DeviceConstraint, bool) {
	nodeIdentity := claim.Annotations[workerNodeIdentityAnnotation]
	if !validResourceName(nodeIdentity) {
		return "", nil, false
	}
	var constraints []DeviceConstraint
	if err := json.Unmarshal([]byte(claim.Annotations[deviceConstraintsAnnotation]), &constraints); err != nil ||
		len(constraints) == 0 || len(constraints) != len(claim.Spec.Spec.Devices.Requests) {
		return "", nil, false
	}
	for _, constraint := range constraints {
		if !validDeviceConstraint(constraint) {
			return "", nil, false
		}
	}
	return nodeIdentity, constraints, true
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
	launchManifest, err := encodeModelRuntimeLaunchManifest(bundle, worker, member)
	if err != nil {
		return corev1.Pod{}, err
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
			TerminationGracePeriodSeconds: int64Pointer(modelRuntimeTerminationGraceSeconds(worker)),
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
			InitContainers: []corev1.Container{
				stageWorkerPrivateInitializer(bundle.InitImage),
				modelRuntimePrivateInitializer(
					bundle.InitImage, launchManifest, modelRuntimeDirectories(worker),
				),
			},
			Containers: []corev1.Container{
				workerInstanceAgentContainer(bundle, worker, member),
				workerInstanceRuntimeContainer(bundle),
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
	devicesJSON := mustEncodeStageWorkerDevices(member.DeviceConstraints)
	capacityVectorJSON := mustEncodeCapacityVector(worker.CapacitySlots, member.DeviceCount)
	return withKubernetesContainerDefaults(corev1.Container{
		Name: "stage-worker-agent", Image: bundle.StageWorkerAgentImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Env: []corev1.EnvVar{
			literalEnvironment("VELA_WORKER_INSTANCE_ID", worker.ID.String()),
			literalEnvironment("VELA_WORKER_INSTANCE_EPOCH", strconv.FormatInt(worker.InstanceEpoch, 10)),
			literalEnvironment("VELA_WORKER_MEMBER_ID", member.ID.String()),
			literalEnvironment("VELA_WORKER_MEMBER_EPOCH", strconv.FormatInt(member.MemberEpoch, 10)),
			literalEnvironment("VELA_WORKER_MEMBER_KEY", member.Key),
			literalEnvironment("VELA_STAGE_WORKER_DEVICES_JSON", devicesJSON),
			literalEnvironment("VELA_STAGE_WORKER_CAPACITY_VECTOR_JSON", capacityVectorJSON),
			fieldEnvironment("VELA_WORKER_NODE_IDENTITY", "spec.nodeName"),
			configMapEnvironment("VELA_WORKER_CONTROL_ADDRESS", bundle.StageWorkerConfigMap, "control-address"),
			configMapEnvironment("VELA_WORKER_CONTROL_SERVER_NAME", bundle.StageWorkerConfigMap, "control-server-name"),
			literalEnvironment("VELA_WORKER_TLS_CERT_FILE", "/etc/vela-stage-worker/private/control/tls.crt"),
			literalEnvironment("VELA_WORKER_TLS_KEY_FILE", "/etc/vela-stage-worker/private/control/tls.key"),
			literalEnvironment("VELA_WORKER_CONTROL_CA_FILE", "/etc/vela-stage-worker/private/control/ca.crt"),
			literalEnvironment("VELA_MODEL_RUNTIME_SOCKET", modelRuntimeSocketPath),
			literalEnvironment("VELA_MODEL_RUNTIME_EXPECTED_UID", "10001"),
			literalEnvironment("VELA_WORKER_SCRATCH_ROOT", stageWorkerScratchRoot),
			literalEnvironment("VELA_STAGE_WORKER_PRODUCTION_STATE_ROOT", stageWorkerScratchRoot+"/production-state"),
			literalEnvironment("VELA_STAGE_WORKER_INPUT_ROOT", stageWorkerScratchRoot+"/inputs"),
			literalEnvironment("VELA_STAGE_WORKER_INPUT_TRANSFER_JOURNAL_ROOT", stageWorkerScratchRoot+"/input-transfer-journal"),
			literalEnvironment("VELA_STAGE_WORKER_OUTPUT_ROOT", stageWorkerScratchRoot+"/outputs"),
			literalEnvironment("VELA_STAGE_WORKER_MATERIALIZATION_JOURNAL_ROOT", stageWorkerScratchRoot+"/materialization-journal"),
			literalEnvironment("VELA_STAGE_WORKER_AUTHORITY_KEYRING_FILE", "/etc/vela-stage-worker/private/authority/keyring.json"),
			literalEnvironment("VELA_ARTIFACT_S3_ACCESS_KEY_ID_FILE", "/etc/vela-stage-worker/private/artifact/access-key-id"),
			literalEnvironment("VELA_ARTIFACT_S3_SECRET_ACCESS_KEY_FILE", "/etc/vela-stage-worker/private/artifact/secret-access-key"),
			literalEnvironment("VELA_ARTIFACT_S3_CA_FILE", "/etc/vela-stage-worker/private/artifact/ca.crt"),
			configMapEnvironment("VELA_STAGE_WORKER_AUTHORITY_ACTIVE_KEY_ID", bundle.StageWorkerConfigMap, "authority-active-key-id"),
			configMapEnvironment("VELA_STAGE_WORKER_CONNECTOR_REVISION_ID", bundle.StageWorkerConfigMap, "connector-revision-id"),
			configMapEnvironment("VELA_STAGE_WORKER_CAPACITY_TTL", bundle.StageWorkerConfigMap, "capacity-ttl"),
			configMapEnvironment("VELA_STAGE_WORKER_HEARTBEAT_INTERVAL", bundle.StageWorkerConfigMap, "heartbeat-interval"),
			configMapEnvironment("VELA_STAGE_WORKER_RETRY_MINIMUM", bundle.StageWorkerConfigMap, "retry-minimum"),
			configMapEnvironment("VELA_STAGE_WORKER_RETRY_MAXIMUM", bundle.StageWorkerConfigMap, "retry-maximum"),
			configMapEnvironment("VELA_STAGE_WORKER_MATERIALIZATION_JOURNAL_LIMIT", bundle.StageWorkerConfigMap, "materialization-journal-limit"),
			configMapEnvironment("VELA_ARTIFACT_S3_ENDPOINT", bundle.StageWorkerConfigMap, "artifact-s3-endpoint"),
			configMapEnvironment("VELA_ARTIFACT_S3_REGION", bundle.StageWorkerConfigMap, "artifact-s3-region"),
			configMapEnvironment("VELA_ARTIFACT_S3_BUCKET", bundle.StageWorkerConfigMap, "artifact-s3-bucket"),
			configMapEnvironment("VELA_ARTIFACT_S3_PATH_STYLE", bundle.StageWorkerConfigMap, "artifact-s3-path-style"),
			configMapEnvironment("VELA_ARTIFACT_S3_SIGNED_GET_TTL", bundle.StageWorkerConfigMap, "artifact-s3-signed-get-ttl"),
			configMapEnvironment("VELA_STAGE_WORKER_SOURCE_LOSS_RETRY", bundle.StageWorkerConfigMap, "source-loss-retry"),
			configMapEnvironment("VELA_STAGE_WORKER_SOURCE_LOSS_CONSUMED_RESOURCE_UNITS", bundle.StageWorkerConfigMap, "source-loss-consumed-resource-units"),
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
			{Name: "model-runtime-socket", MountPath: modelRuntimeSocketRoot},
			{Name: "scratch", MountPath: stageWorkerScratchRoot},
			{Name: "stage-worker-private", MountPath: "/etc/vela-stage-worker/private", ReadOnly: true},
		},
	})
}

func workerInstanceRuntimeContainer(
	bundle WorkerBundleActuation,
) corev1.Container {
	return withKubernetesContainerDefaults(corev1.Container{
		Name: "model-runtime", Image: bundle.RuntimeImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Env: []corev1.EnvVar{
			literalEnvironment("VELA_MODEL_RUNTIME_LAUNCH_MANIFEST_FILE", modelRuntimeLaunchManifest),
			literalEnvironment("VELA_MODEL_RUNTIME_AUTHORITY_VERIFIER_KEYRING_FILE", modelRuntimeVerifierKeyring),
			literalEnvironment("VELA_MODEL_RUNTIME_EPOCH_DIRECTORY", modelRuntimeEpochDirectory),
			literalEnvironment("VELA_MODEL_RUNTIME_SOCKET", modelRuntimeSocketPath),
			configMapEnvironment("VELA_MODEL_RUNTIME_CANCEL_TIMEOUT", bundle.StageWorkerConfigMap, "model-runtime-cancel-timeout"),
			configMapEnvironment("VELA_MODEL_RUNTIME_SHUTDOWN_TIMEOUT", bundle.StageWorkerConfigMap, "model-runtime-shutdown-timeout"),
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
			{Name: "model-runtime-socket", MountPath: modelRuntimeSocketRoot},
			{Name: "scratch", MountPath: stageWorkerScratchRoot},
			{Name: "model-runtime-private", MountPath: modelRuntimePrivateRoot, ReadOnly: true},
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
	scratchPath := "/var/lib/vela/worker-instances/" + worker.ID.String() + "/" + member.Key
	return []corev1.Volume{
		{Name: "model-runtime-socket", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
			Medium: corev1.StorageMediumMemory, SizeLimit: quantityPointer("16Mi"),
		}}},
		{Name: "scratch", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
			Path: scratchPath, Type: valuePointer(corev1.HostPathDirectoryOrCreate),
		}}},
		{Name: "model-weights", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
			Path: "/var/lib/vela/models", Type: valuePointer(corev1.HostPathDirectory),
		}}},
		{Name: "model-runtime-verifier-projected", VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{Name: bundle.ModelRuntimeVerifierConfigMap},
				DefaultMode:          &mode0400,
				Items:                []corev1.KeyToPath{{Key: "verifier-keyring.json", Path: "verifier-keyring.json"}},
			},
		}},
		{Name: "stage-worker-control-projected", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: bundle.StageWorkerControlTLSSecret, DefaultMode: &mode0400,
		}}},
		{Name: "stage-worker-authority-projected", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: bundle.StageWorkerAuthoritySecret, DefaultMode: &mode0400,
			Items: []corev1.KeyToPath{{Key: "keyring.json", Path: "keyring.json"}},
		}}},
		{Name: "artifact-store-credentials-projected", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: bundle.ArtifactStoreCredentialsSecret, DefaultMode: &mode0400,
			Items: []corev1.KeyToPath{
				{Key: "access-key-id", Path: "access-key-id"},
				{Key: "secret-access-key", Path: "secret-access-key"},
			},
		}}},
		{Name: "artifact-store-ca-projected", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: bundle.ArtifactStoreCASecret, DefaultMode: &mode0400,
			Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
		}}},
		{Name: "stage-worker-private", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
			Medium: corev1.StorageMediumMemory, SizeLimit: quantityPointer("1Mi"),
		}}},
		{Name: "model-runtime-private", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
			Medium: corev1.StorageMediumMemory, SizeLimit: quantityPointer("2Mi"),
		}}},
	}
}

func modelRuntimePrivateInitializer(image, launchManifest, directories string) corev1.Container {
	runAsNonRoot := false
	runAsUser := int64(0)
	runAsGroup := int64(0)
	allowPrivilegeEscalation := false
	readOnlyRootFilesystem := true
	return withKubernetesContainerDefaults(corev1.Container{
		Name: "model-runtime-private-materialization", Image: image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command: []string{"/bin/sh", "-ec", `mkdir -p /private/authority
for directory in ${VELA_MODEL_RUNTIME_DIRECTORIES}; do
  mkdir -p "${directory}"
done
printf '%s' "${VELA_MODEL_RUNTIME_LAUNCH_MANIFEST_JSON}" > /private/launch.json
cp /projected/model-runtime-authority/verifier-keyring.json /private/authority/verifier-keyring.json
test -f /private/launch.json
test -f /private/authority/verifier-keyring.json
chmod 0700 /private /private/authority
chmod 0400 /private/launch.json /private/authority/verifier-keyring.json
chmod 0700 /var/lib/vela/stage-worker/scratch/model-runtime-epochs
chmod 0700 /var/lib/vela/stage-worker/scratch/model-runtime/*
chown -R 10001:10001 /private /var/lib/vela/stage-worker/scratch/model-runtime-epochs /var/lib/vela/stage-worker/scratch/model-runtime
`},
		Env: []corev1.EnvVar{
			literalEnvironment("VELA_MODEL_RUNTIME_LAUNCH_MANIFEST_JSON", launchManifest),
			literalEnvironment("VELA_MODEL_RUNTIME_DIRECTORIES", directories),
		},
		Resources: corev1.ResourceRequirements{
			Requests: resourceList(map[corev1.ResourceName]string{
				corev1.ResourceCPU: "10m", corev1.ResourceMemory: "8Mi",
			}),
			Limits: resourceList(map[corev1.ResourceName]string{
				corev1.ResourceCPU: "100m", corev1.ResourceMemory: "16Mi",
			}),
		},
		SecurityContext: &corev1.SecurityContext{
			RunAsNonRoot: &runAsNonRoot, RunAsUser: &runAsUser, RunAsGroup: &runAsGroup,
			AllowPrivilegeEscalation: &allowPrivilegeEscalation,
			ReadOnlyRootFilesystem:   &readOnlyRootFilesystem,
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"}, Add: []corev1.Capability{"CHOWN"},
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "model-runtime-verifier-projected", MountPath: "/projected/model-runtime-authority", ReadOnly: true},
			{Name: "model-runtime-private", MountPath: "/private"},
			{Name: "scratch", MountPath: stageWorkerScratchRoot},
		},
	})
}

func stageWorkerPrivateInitializer(image string) corev1.Container {
	runAsNonRoot := false
	runAsUser := int64(0)
	runAsGroup := int64(0)
	allowPrivilegeEscalation := false
	readOnlyRootFilesystem := true
	return withKubernetesContainerDefaults(corev1.Container{
		Name: "stage-worker-private-materialization", Image: image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command: []string{"/bin/sh", "-ec", `mkdir -p /run/vela-model-runtime/private
mkdir -p /var/lib/vela/stage-worker/scratch/production-state
mkdir -p /var/lib/vela/stage-worker/scratch/inputs
mkdir -p /var/lib/vela/stage-worker/scratch/input-transfer-journal
mkdir -p /var/lib/vela/stage-worker/scratch/outputs
mkdir -p /var/lib/vela/stage-worker/scratch/materialization-journal
mkdir -p /private/control /private/authority /private/artifact
cp /projected/control/tls.crt /private/control/tls.crt
cp /projected/control/tls.key /private/control/tls.key
cp /projected/control/ca.crt /private/control/ca.crt
cp /projected/authority/keyring.json /private/authority/keyring.json
cp /projected/artifact-credentials/access-key-id /private/artifact/access-key-id
cp /projected/artifact-credentials/secret-access-key /private/artifact/secret-access-key
cp /projected/artifact-ca/ca.crt /private/artifact/ca.crt
chmod 0700 /run/vela-model-runtime /run/vela-model-runtime/private
chmod 0700 /var/lib/vela/stage-worker/scratch /var/lib/vela/stage-worker/scratch/*
chmod 0700 /private /private/control /private/authority /private/artifact
chmod 0400 /private/control/* /private/authority/* /private/artifact/*
chown 10001:10001 /run/vela-model-runtime /run/vela-model-runtime/private
chown -R 10001:10001 /var/lib/vela/stage-worker/scratch /private
`},
		Resources: corev1.ResourceRequirements{
			Requests: resourceList(map[corev1.ResourceName]string{
				corev1.ResourceCPU: "10m", corev1.ResourceMemory: "8Mi",
			}),
			Limits: resourceList(map[corev1.ResourceName]string{
				corev1.ResourceCPU: "100m", corev1.ResourceMemory: "16Mi",
			}),
		},
		SecurityContext: &corev1.SecurityContext{
			RunAsNonRoot: &runAsNonRoot, RunAsUser: &runAsUser, RunAsGroup: &runAsGroup,
			AllowPrivilegeEscalation: &allowPrivilegeEscalation,
			ReadOnlyRootFilesystem:   &readOnlyRootFilesystem,
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"}, Add: []corev1.Capability{"CHOWN"},
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "model-runtime-socket", MountPath: modelRuntimeSocketRoot},
			{Name: "scratch", MountPath: stageWorkerScratchRoot},
			{Name: "stage-worker-control-projected", MountPath: "/projected/control", ReadOnly: true},
			{Name: "stage-worker-authority-projected", MountPath: "/projected/authority", ReadOnly: true},
			{Name: "artifact-store-credentials-projected", MountPath: "/projected/artifact-credentials", ReadOnly: true},
			{Name: "artifact-store-ca-projected", MountPath: "/projected/artifact-ca", ReadOnly: true},
			{Name: "stage-worker-private", MountPath: "/private"},
		},
	})
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

// WorkerInstanceGPUClaimTemplateMatches reports whether a live claim template
// has the exact actuation-owned content of the desired template. Kubernetes
// server-assigned metadata is excluded from the comparison.
func WorkerInstanceGPUClaimTemplateMatches(
	live resourcev1.ResourceClaimTemplate,
	desired resourcev1.ResourceClaimTemplate,
) bool {
	return workerInstanceGPUClaimTemplateMatches(live, desired)
}

func mustEncodeDeviceConstraints(constraints []DeviceConstraint) string {
	encoded, err := json.Marshal(constraints)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func mustEncodeStageWorkerDevices(constraints []DeviceConstraint) string {
	devices := make([]struct {
		DeviceID    string `json:"device_id"`
		DeviceEpoch int64  `json:"device_epoch"`
	}, 0, len(constraints))
	for _, constraint := range constraints {
		devices = append(devices, struct {
			DeviceID    string `json:"device_id"`
			DeviceEpoch int64  `json:"device_epoch"`
		}{DeviceID: constraint.DeviceID.String(), DeviceEpoch: constraint.DeviceEpoch})
	}
	encoded, err := json.Marshal(devices)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func mustEncodeCapacityVector(capacitySlots, deviceCount int) string {
	encoded, err := json.Marshal(map[string]int64{
		"active_stage_slots": int64(capacitySlots),
		"gpu_count":          int64(deviceCount),
	})
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
	normalizedLive := normalize(live)
	normalizedDesired := normalize(desired)
	if normalizedDesired.Spec.NodeName == "" &&
		normalizedDesired.Spec.NodeSelector[corev1.LabelHostname] != "" &&
		normalizedLive.Spec.NodeName == normalizedDesired.Spec.NodeSelector[corev1.LabelHostname] {
		normalizedLive.Spec.NodeName = ""
	}
	return reflect.DeepEqual(normalizedLive, normalizedDesired)
}

// WorkerInstancePodMatches reports whether a live Pod has the exact
// actuation-owned content of the desired Pod. Kubernetes server-assigned
// metadata, status, and the scheduler's exact approved node binding are
// excluded from the comparison.
func WorkerInstancePodMatches(live, desired corev1.Pod) bool {
	return workerInstancePodMatches(live, desired)
}
