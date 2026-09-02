package h3preflight

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/h3launchevidence"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
)

const (
	SchemaVersion = 2
	MediaType     = "application/vnd.vela.h3-real-environment-preflight.v2+json"

	minimumPhysicalNodes = 3
	expectedAUXWorkers   = 1
	expectedDiTWorkers   = 7
)

type CheckID string

const (
	CheckReleaseBundle           CheckID = "release_bundle"
	CheckEvidenceRole            CheckID = "evidence_role"
	CheckKubernetesAPI           CheckID = "kubernetes_api"
	CheckExternalResourceBinding CheckID = "external_resource_binding"
	CheckH3DeploymentUnit        CheckID = "h3_deployment_unit"
	CheckCrossNodeRollout        CheckID = "cross_node_rollout"
	CheckSchedulableNodes        CheckID = "schedulable_nodes"
	CheckNVIDIADeviceClass       CheckID = "nvidia_device_class"
	CheckNVIDIAResourceSlice     CheckID = "nvidia_resource_slices"
	CheckGPUIdentityClosure      CheckID = "gpu_identity_closure"
)

type Status string

const (
	StatusPass Status = "PASS"
	StatusFail Status = "FAIL"
)

const (
	ReasonSatisfied                = "SATISFIED"
	ReasonRoleUnverified           = "EVIDENCE_ROLE_UNVERIFIED"
	ReasonKubernetesOffline        = "KUBERNETES_API_UNREACHABLE"
	ReasonKubernetesIdentity       = "KUBERNETES_IDENTITY_MISMATCH"
	ReasonExternalResourceMismatch = "EXTERNAL_RESOURCE_BINDING_MISMATCH"
	ReasonInvalidH3Unit            = "H3_DEPLOYMENT_UNIT_INVALID"
	ReasonCrossNodeAbsent          = "CROSS_NODE_ROLLOUT_ABSENT"
	ReasonInsufficientNodes        = "INSUFFICIENT_SCHEDULABLE_NODES"
	ReasonDeviceClassMissing       = "NVIDIA_DEVICE_CLASS_MISSING"
	ReasonSlicesMissing            = "NVIDIA_RESOURCE_SLICES_INCOMPLETE"
	ReasonGPUIdentityMismatch      = "GPU_IDENTITY_CLOSURE_MISMATCH"
)

type Request struct {
	ReleaseDigest          string
	ConfigurationRevision  string
	ValidationEnvironment  string
	KubernetesClusterUID   string
	KubernetesNamespaceUID string
	CheckedAt              time.Time
	Rollout                fleetcontroller.ResidencyPlanRollout
	ExternalResources      []h3launchevidence.ExternalResourceExpectation
}

type Observation struct {
	EvidenceRoleVerified   bool
	KubernetesVersion      string
	KubernetesClusterUID   string
	KubernetesNamespaceUID string
	Nodes                  []corev1.Node
	DeviceClasses          []resourcev1.DeviceClass
	ResourceSlices         []resourcev1.ResourceSlice
	ConfigMaps             []corev1.ConfigMap
	Secrets                []corev1.Secret
}

type Check struct {
	ID         CheckID `json:"id"`
	Status     Status  `json:"status"`
	ReasonCode string  `json:"reason_code"`
	Observed   int     `json:"observed,omitempty"`
	Required   int     `json:"required,omitempty"`
}

type Report struct {
	SchemaVersion           int                                         `json:"schema_version"`
	MediaType               string                                      `json:"media_type"`
	ReleaseDigest           string                                      `json:"release_digest"`
	ConfigurationRevision   string                                      `json:"configuration_revision"`
	ResidencyPlanRevisionID uuid.UUID                                   `json:"residency_plan_revision_id"`
	ValidationEnvironment   string                                      `json:"validation_environment"`
	KubernetesClusterUID    string                                      `json:"kubernetes_cluster_uid"`
	KubernetesNamespaceUID  string                                      `json:"kubernetes_namespace_uid"`
	CheckedAt               time.Time                                   `json:"checked_at"`
	Ready                   bool                                        `json:"ready"`
	Checks                  []Check                                     `json:"checks"`
	ExternalResources       []h3launchevidence.ExternalResourceEvidence `json:"external_resources,omitempty"`
}

type expectedInventory struct {
	nodes map[string]struct{}
	gpus  map[string]struct{}
	aux   int
	dit   int
	valid bool
}

func Evaluate(request Request, observation Observation) (Report, error) {
	if !validDigest(request.ReleaseDigest) || !validDigest(request.ConfigurationRevision) ||
		request.Rollout.ApprovedPlan.ID == uuid.Nil ||
		request.ValidationEnvironment == "" ||
		request.ValidationEnvironment != strings.TrimSpace(request.ValidationEnvironment) ||
		!validUID(request.KubernetesClusterUID) ||
		!validUID(request.KubernetesNamespaceUID) ||
		request.CheckedAt.IsZero() || request.CheckedAt.Location() != time.UTC {
		return Report{}, errors.New("H3 preflight request is invalid")
	}
	expected := expectedFromRollout(request.Rollout)
	report := Report{
		SchemaVersion: SchemaVersion, MediaType: MediaType,
		ReleaseDigest: request.ReleaseDigest, ConfigurationRevision: request.ConfigurationRevision,
		ResidencyPlanRevisionID: request.Rollout.ApprovedPlan.ID,
		ValidationEnvironment:   request.ValidationEnvironment, CheckedAt: request.CheckedAt,
		KubernetesClusterUID:   observation.KubernetesClusterUID,
		KubernetesNamespaceUID: observation.KubernetesNamespaceUID,
		Ready:                  true,
	}
	report.add(Check{ID: CheckReleaseBundle, Status: StatusPass, ReasonCode: ReasonSatisfied})
	externalResources, externalResourceErr := h3launchevidence.VerifyExternalResources(
		request.ExternalResources, observation.ConfigMaps, observation.Secrets,
	)
	if externalResourceErr != nil {
		report.add(Check{
			ID: CheckExternalResourceBinding, Status: StatusFail,
			ReasonCode: ReasonExternalResourceMismatch,
			Observed:   len(observation.ConfigMaps) + len(observation.Secrets),
			Required:   len(request.ExternalResources),
		})
	} else {
		report.ExternalResources = externalResources
		report.add(Check{
			ID: CheckExternalResourceBinding, Status: StatusPass, ReasonCode: ReasonSatisfied,
			Observed: len(externalResources), Required: len(request.ExternalResources),
		})
	}
	if observation.EvidenceRoleVerified {
		report.add(Check{ID: CheckEvidenceRole, Status: StatusPass, ReasonCode: ReasonSatisfied})
	} else {
		report.add(Check{ID: CheckEvidenceRole, Status: StatusFail, ReasonCode: ReasonRoleUnverified})
	}
	if strings.TrimSpace(observation.KubernetesVersion) == "" {
		report.add(Check{ID: CheckKubernetesAPI, Status: StatusFail, ReasonCode: ReasonKubernetesOffline})
	} else if observation.KubernetesClusterUID != request.KubernetesClusterUID ||
		observation.KubernetesNamespaceUID != request.KubernetesNamespaceUID {
		report.add(Check{ID: CheckKubernetesAPI, Status: StatusFail, ReasonCode: ReasonKubernetesIdentity})
	} else {
		report.add(Check{ID: CheckKubernetesAPI, Status: StatusPass, ReasonCode: ReasonSatisfied})
	}
	if expected.valid && expected.aux == expectedAUXWorkers && expected.dit == expectedDiTWorkers &&
		len(expected.gpus) == expectedAUXWorkers+expectedDiTWorkers {
		report.add(Check{
			ID: CheckH3DeploymentUnit, Status: StatusPass, ReasonCode: ReasonSatisfied,
			Observed: expected.aux + expected.dit, Required: expectedAUXWorkers + expectedDiTWorkers,
		})
	} else {
		report.add(Check{
			ID: CheckH3DeploymentUnit, Status: StatusFail, ReasonCode: ReasonInvalidH3Unit,
			Observed: expected.aux + expected.dit, Required: expectedAUXWorkers + expectedDiTWorkers,
		})
	}
	if len(expected.nodes) >= minimumPhysicalNodes {
		report.add(Check{
			ID: CheckCrossNodeRollout, Status: StatusPass, ReasonCode: ReasonSatisfied,
			Observed: len(expected.nodes), Required: minimumPhysicalNodes,
		})
	} else {
		report.add(Check{
			ID: CheckCrossNodeRollout, Status: StatusFail, ReasonCode: ReasonCrossNodeAbsent,
			Observed: len(expected.nodes), Required: minimumPhysicalNodes,
		})
	}
	readyNodes, nodeIdentityByName := schedulableReadyNodes(observation.Nodes)
	if len(readyNodes) >= minimumPhysicalNodes && containsAll(readyNodes, expected.nodes) {
		report.add(Check{
			ID: CheckSchedulableNodes, Status: StatusPass, ReasonCode: ReasonSatisfied,
			Observed: len(readyNodes), Required: minimumPhysicalNodes,
		})
	} else {
		report.add(Check{
			ID: CheckSchedulableNodes, Status: StatusFail, ReasonCode: ReasonInsufficientNodes,
			Observed: len(readyNodes), Required: minimumPhysicalNodes,
		})
	}
	if hasNVIDIADeviceClass(observation.DeviceClasses) {
		report.add(Check{ID: CheckNVIDIADeviceClass, Status: StatusPass, ReasonCode: ReasonSatisfied, Observed: 1, Required: 1})
	} else {
		report.add(Check{ID: CheckNVIDIADeviceClass, Status: StatusFail, ReasonCode: ReasonDeviceClassMissing, Required: 1})
	}
	observedGPUs, slicesValid := GPUsFromSlices(observation.ResourceSlices, nodeIdentityByName)
	if slicesValid {
		report.add(Check{
			ID: CheckNVIDIAResourceSlice, Status: StatusPass, ReasonCode: ReasonSatisfied,
			Observed: len(observation.ResourceSlices), Required: 1,
		})
	} else {
		report.add(Check{
			ID: CheckNVIDIAResourceSlice, Status: StatusFail, ReasonCode: ReasonSlicesMissing,
			Observed: len(observation.ResourceSlices), Required: 1,
		})
	}
	if expected.valid && len(expected.gpus) == 8 && containsAll(observedGPUs, expected.gpus) {
		report.add(Check{
			ID: CheckGPUIdentityClosure, Status: StatusPass, ReasonCode: ReasonSatisfied,
			Observed: len(observedGPUs), Required: len(expected.gpus),
		})
	} else {
		report.add(Check{
			ID: CheckGPUIdentityClosure, Status: StatusFail, ReasonCode: ReasonGPUIdentityMismatch,
			Observed: len(observedGPUs), Required: len(expected.gpus),
		})
	}
	return report, nil
}

func (report *Report) add(check Check) {
	report.Checks = append(report.Checks, check)
	if check.Status != StatusPass {
		report.Ready = false
	}
}

func expectedFromRollout(rollout fleetcontroller.ResidencyPlanRollout) expectedInventory {
	result := expectedInventory{
		nodes: make(map[string]struct{}), gpus: make(map[string]struct{}), valid: true,
	}
	seenWorkers := make(map[uuid.UUID]struct{})
	for _, bundle := range rollout.WorkerBundles {
		for _, worker := range bundle.WorkerInstances {
			if worker.ID == uuid.Nil {
				result.valid = false
			}
			if _, duplicate := seenWorkers[worker.ID]; duplicate {
				result.valid = false
			}
			seenWorkers[worker.ID] = struct{}{}
			if !singleGPUWorker(worker) {
				result.valid = false
			}
			switch {
			case isAUXWorker(worker):
				result.aux++
			case isDiTWorker(worker):
				result.dit++
			default:
				result.valid = false
			}
			for _, member := range worker.Members {
				if member.NodeIdentity == "" {
					result.valid = false
					continue
				}
				result.nodes[member.NodeIdentity] = struct{}{}
				for _, device := range member.DeviceConstraints {
					key := gpuKey(member.NodeIdentity, device.GPUUUID, device.PCIBDF)
					if key == "" {
						result.valid = false
						continue
					}
					if _, duplicate := result.gpus[key]; duplicate {
						result.valid = false
					}
					result.gpus[key] = struct{}{}
				}
			}
		}
	}
	return result
}

func singleGPUWorker(worker fleetcontroller.WorkerInstanceActuation) bool {
	return worker.CapacitySlots == 1 && len(worker.Members) == 1 &&
		worker.Members[0].ResourceClass == "GPU" && worker.Members[0].DeviceCount == 1 &&
		len(worker.Members[0].DeviceConstraints) == 1
}

func isAUXWorker(worker fleetcontroller.WorkerInstanceActuation) bool {
	if strings.ToLower(worker.Role) != "aux" || worker.SharedSlotException == "" ||
		len(worker.ModelRuntimes) != 2 || len(worker.ModelRuntimes[0].Command) != 1 ||
		len(worker.ModelRuntimes[1].Command) != 1 {
		return false
	}
	components := []string{worker.ModelRuntimes[0].Component, worker.ModelRuntimes[1].Component}
	sort.Strings(components)
	return components[0] == "ENCODER" && components[1] == "VAE_DECODER" &&
		((worker.ModelRuntimes[0].Component == "ENCODER" && worker.ModelRuntimes[0].Command[0] == "/opt/vela/bin/h3-encoder" &&
			worker.ModelRuntimes[1].Component == "VAE_DECODER" && worker.ModelRuntimes[1].Command[0] == "/opt/vela/bin/h3-vae-decoder") ||
			(worker.ModelRuntimes[1].Component == "ENCODER" && worker.ModelRuntimes[1].Command[0] == "/opt/vela/bin/h3-encoder" &&
				worker.ModelRuntimes[0].Component == "VAE_DECODER" && worker.ModelRuntimes[0].Command[0] == "/opt/vela/bin/h3-vae-decoder"))
}

func isDiTWorker(worker fleetcontroller.WorkerInstanceActuation) bool {
	return strings.ToLower(worker.Role) == "dit" && worker.SharedSlotException == "" &&
		len(worker.ModelRuntimes) == 1 && worker.ModelRuntimes[0].Component == "DIT" &&
		len(worker.ModelRuntimes[0].Command) == 1 && worker.ModelRuntimes[0].Command[0] == "/opt/vela/bin/h3-dit"
}

func schedulableReadyNodes(nodes []corev1.Node) (map[string]struct{}, map[string]string) {
	candidates := make(map[string]string)
	selectorCounts := make(map[string]int)
	for _, node := range nodes {
		if node.Name == "" || node.UID == "" || node.ResourceVersion == "" ||
			node.DeletionTimestamp != nil || node.Spec.Unschedulable ||
			node.Labels[corev1.LabelHostname] == "" || hasUntoleratedTaint(node.Spec.Taints) {
			continue
		}
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				selectorIdentity := node.Labels[corev1.LabelHostname]
				candidates[node.Name] = selectorIdentity
				selectorCounts[selectorIdentity]++
				break
			}
		}
	}
	selectors := make(map[string]struct{})
	nodeIdentityByName := make(map[string]string)
	for nodeName, selectorIdentity := range candidates {
		if selectorCounts[selectorIdentity] != 1 {
			continue
		}
		selectors[selectorIdentity] = struct{}{}
		nodeIdentityByName[nodeName] = selectorIdentity
	}
	return selectors, nodeIdentityByName
}

func hasUntoleratedTaint(taints []corev1.Taint) bool {
	for _, taint := range taints {
		switch taint.Effect {
		case corev1.TaintEffectNoSchedule, corev1.TaintEffectNoExecute:
			if taint.Key != "vela.ai/h3" || taint.Value != "true" ||
				taint.Effect != corev1.TaintEffectNoSchedule {
				return true
			}
		}
	}
	return false
}

func hasNVIDIADeviceClass(classes []resourcev1.DeviceClass) bool {
	count := 0
	for _, class := range classes {
		if class.Name == "gpu.nvidia.com" && class.UID != "" && class.ResourceVersion != "" &&
			class.DeletionTimestamp == nil {
			count++
		}
	}
	return count == 1
}

// GPUsFromSlices returns exact node, UUID, and PCI BDF identities from complete
// current NVIDIA DRA pools.
func GPUsFromSlices(
	slices []resourcev1.ResourceSlice,
	nodeIdentityByName map[string]string,
) (map[string]struct{}, bool) {
	result := make(map[string]struct{})
	if len(slices) == 0 {
		return result, false
	}
	pools := make(map[string][]resourcev1.ResourceSlice)
	latest := make(map[string]int64)
	for _, slice := range slices {
		if slice.Spec.Driver != "gpu.nvidia.com" {
			continue
		}
		if slice.Spec.Pool.Generation > latest[slice.Spec.Pool.Name] {
			latest[slice.Spec.Pool.Name] = slice.Spec.Pool.Generation
		}
		pools[slice.Spec.Pool.Name] = append(pools[slice.Spec.Pool.Name], slice)
	}
	valid := len(pools) > 0
	for poolName, poolSlices := range pools {
		generation := latest[poolName]
		current := make([]resourcev1.ResourceSlice, 0)
		expectedCount := int64(0)
		for _, slice := range poolSlices {
			if slice.Spec.Pool.Generation != generation {
				continue
			}
			if slice.UID == "" || slice.ResourceVersion == "" || slice.DeletionTimestamp != nil ||
				slice.Spec.Pool.ResourceSliceCount <= 0 ||
				(expectedCount != 0 && expectedCount != slice.Spec.Pool.ResourceSliceCount) {
				valid = false
				continue
			}
			expectedCount = slice.Spec.Pool.ResourceSliceCount
			current = append(current, slice)
		}
		if int64(len(current)) != expectedCount {
			valid = false
			continue
		}
		for _, slice := range current {
			for _, device := range slice.Spec.Devices {
				node := nodeIdentityByName[deviceNode(slice, device)]
				uuidValue := stringAttribute(device, "gpu.nvidia.com/uuid", "uuid")
				pciBDF := stringAttribute(device, "resource.kubernetes.io/pciBusID")
				key := gpuKey(node, uuidValue, pciBDF)
				if key == "" {
					valid = false
					continue
				}
				if _, duplicate := result[key]; duplicate {
					valid = false
				}
				result[key] = struct{}{}
			}
		}
	}
	return result, valid && len(result) > 0
}

func deviceNode(slice resourcev1.ResourceSlice, device resourcev1.Device) string {
	if device.NodeName != nil {
		return *device.NodeName
	}
	if slice.Spec.NodeName != nil {
		return *slice.Spec.NodeName
	}
	return ""
}

func stringAttribute(device resourcev1.Device, names ...resourcev1.QualifiedName) string {
	for _, name := range names {
		attribute, exists := device.Attributes[name]
		if exists && attribute.StringValue != nil {
			return *attribute.StringValue
		}
	}
	return ""
}

func gpuKey(node, gpuUUID, pciBDF string) string {
	if node == "" || !strings.HasPrefix(gpuUUID, "GPU-") || pciBDF == "" ||
		strings.ContainsAny(node+gpuUUID+pciBDF, "|\n\r\t") {
		return ""
	}
	return fmt.Sprintf("%s|%s|%s", node, gpuUUID, strings.ToLower(pciBDF))
}

func containsAll(actual, expected map[string]struct{}) bool {
	for value := range expected {
		if _, exists := actual[value]; !exists {
			return false
		}
	}
	return true
}

func validDigest(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") ||
		value == "sha256:"+strings.Repeat("0", 64) {
		return false
	}
	for _, character := range strings.TrimPrefix(value, "sha256:") {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func validUID(value string) bool {
	return value != "" && len(value) <= 200 && value == strings.TrimSpace(value) &&
		!strings.ContainsAny(value, "\x00\r\n")
}
