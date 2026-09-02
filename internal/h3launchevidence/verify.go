package h3launchevidence

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontroller"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
)

var (
	ErrInvalidLaunchEvidence = errors.New("invalid H3 launch evidence")
	sha256Pattern            = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	hexDigestPattern         = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

type desiredMember struct {
	bundle   fleetcontroller.WorkerBundleActuation
	worker   fleetcontroller.WorkerInstanceActuation
	member   fleetcontroller.WorkerMemberActuation
	pod      corev1.Pod
	template resourcev1.ResourceClaimTemplate
}

// Verify checks one atomic capture against the exact approved release rollout.
// It fails closed on missing, duplicate, stale, or mismatched authority.
func Verify(input Input) (Evidence, error) {
	if err := validateInputEnvelope(input); err != nil {
		return Evidence{}, err
	}
	desired, err := desiredMembers(input.Rollout)
	if err != nil {
		return Evidence{}, invalid("approved rollout: %v", err)
	}
	if len(input.Kubernetes.Pods) != len(desired) ||
		len(input.Kubernetes.ClaimTemplates) != len(desired) ||
		len(input.Kubernetes.Claims) != len(desired) ||
		len(input.Registry.Workers) != len(input.Rollout.ApprovedPlan.WorkerInstances) {
		return Evidence{}, invalid("live resource cardinality does not match the approved rollout")
	}
	pods, err := indexPods(input.Kubernetes.Pods)
	if err != nil {
		return Evidence{}, err
	}
	templates, err := indexTemplates(input.Kubernetes.ClaimTemplates)
	if err != nil {
		return Evidence{}, err
	}
	claims, err := indexClaims(input.Kubernetes.Claims)
	if err != nil {
		return Evidence{}, err
	}
	nodes, err := indexNodes(input.Kubernetes.Nodes)
	if err != nil {
		return Evidence{}, err
	}
	registryWorkers, err := indexRegistryWorkers(input.Registry.Workers)
	if err != nil {
		return Evidence{}, err
	}
	externalResources, err := VerifyExternalResources(
		input.ExternalResources,
		input.Kubernetes.ConfigMaps,
		input.Kubernetes.Secrets,
	)
	if err != nil {
		return Evidence{}, err
	}

	evidence := Evidence{
		SchemaVersion: SchemaVersion, MediaType: MediaType,
		ReleaseDigest: input.ReleaseDigest, ConfigurationRevision: input.ConfigurationRevision,
		ValidationEnvironment: input.ValidationEnvironment, CollectorIdentity: input.CollectorIdentity,
		CapturedAt: input.CapturedAt, KubernetesClusterUID: input.Kubernetes.ClusterUID,
		KubernetesNamespaceUID:  input.Kubernetes.NamespaceUID,
		RegistryDatabaseTime:    input.Registry.DatabaseTime,
		RegistryTransactionID:   input.Registry.TransactionID,
		RegistrySnapshotID:      input.Registry.SnapshotID,
		ResidencyPlanRevisionID: input.Rollout.ApprovedPlan.ID,
		ExternalResources:       externalResources,
		Workers:                 make([]WorkerEvidence, 0, len(registryWorkers)),
	}

	workerEvidenceIndexes := make(map[uuid.UUID]int, len(registryWorkers))
	for _, bundle := range input.Rollout.WorkerBundles {
		for _, worker := range bundle.WorkerInstances {
			registry, exists := registryWorkers[worker.ID]
			if !exists {
				return Evidence{}, invalid("WorkerInstance %s is missing from Fleet Registry", worker.ID)
			}
			verified, err := verifyRegistryWorker(input, bundle, worker, registry)
			if err != nil {
				return Evidence{}, err
			}
			evidence.Workers = append(evidence.Workers, verified)
			workerEvidenceIndexes[worker.ID] = len(evidence.Workers) - 1
		}
	}

	for _, expected := range desired {
		pod, exists := pods[expected.pod.Name]
		if !exists {
			return Evidence{}, invalid("Pod %q is missing", expected.pod.Name)
		}
		template, exists := templates[expected.template.Name]
		if !exists {
			return Evidence{}, invalid("ResourceClaimTemplate %q is missing", expected.template.Name)
		}
		memberEvidence, err := verifyKubernetesMember(
			input, expected, pod, template, claims, nodes,
			registryWorkers[expected.worker.ID],
		)
		if err != nil {
			return Evidence{}, err
		}
		workerIndex, exists := workerEvidenceIndexes[expected.worker.ID]
		if !exists {
			return Evidence{}, invalid("WorkerInstance %s has no verified evidence slot", expected.worker.ID)
		}
		evidence.Workers[workerIndex].Members = append(
			evidence.Workers[workerIndex].Members,
			memberEvidence,
		)
	}
	for index := range evidence.Workers {
		sort.Slice(evidence.Workers[index].Members, func(left, right int) bool {
			return evidence.Workers[index].Members[left].MemberKey < evidence.Workers[index].Members[right].MemberKey
		})
	}
	sort.Slice(evidence.Workers, func(left, right int) bool {
		return evidence.Workers[left].WorkerInstanceID.String() < evidence.Workers[right].WorkerInstanceID.String()
	})
	return evidence, nil
}

func validateInputEnvelope(input Input) error {
	if !sha256Pattern.MatchString(input.ReleaseDigest) ||
		!sha256Pattern.MatchString(input.ConfigurationRevision) ||
		!validText(input.ValidationEnvironment, 500) ||
		!validText(input.CollectorIdentity, 500) || input.CapturedAt.IsZero() ||
		!validText(input.Kubernetes.ClusterUID, 200) ||
		!validText(input.Kubernetes.NamespaceUID, 200) ||
		input.Registry.DatabaseTime.IsZero() ||
		input.Registry.DatabaseTime.After(input.CapturedAt) ||
		input.CapturedAt.Sub(input.Registry.DatabaseTime) > 5*time.Minute ||
		!validText(input.Registry.TransactionID, 100) ||
		!validText(input.Registry.SnapshotID, 200) {
		return invalid("release binding or live source provenance is incomplete")
	}
	if input.Rollout.ApprovedPlan.ApprovedAt.After(input.CapturedAt) {
		return invalid("ResidencyPlan approval is newer than the capture")
	}
	return nil
}

func desiredMembers(rollout fleetcontroller.ResidencyPlanRollout) ([]desiredMember, error) {
	if err := fleetcontroller.ValidateResidencyPlanRollout(rollout); err != nil {
		return nil, err
	}
	result := make([]desiredMember, 0)
	for _, bundle := range rollout.WorkerBundles {
		pods, templates, err := fleetcontroller.MaterializeWorkerInstanceLaunchResources(bundle)
		if err != nil {
			return nil, err
		}
		podByIdentity := make(map[string]corev1.Pod, len(pods))
		templateByIdentity := make(map[string]resourcev1.ResourceClaimTemplate, len(templates))
		for _, pod := range pods {
			key := pod.Labels["vela.ai/worker-instance-id"] + "\x00" + pod.Labels["vela.ai/worker-member-id"]
			podByIdentity[key] = pod
		}
		for _, template := range templates {
			key := template.Labels["vela.ai/worker-instance-id"] + "\x00" + template.Labels["vela.ai/worker-member-id"]
			templateByIdentity[key] = template
		}
		for _, worker := range bundle.WorkerInstances {
			for _, member := range worker.Members {
				key := worker.ID.String() + "\x00" + member.ID.String()
				pod, podExists := podByIdentity[key]
				template, templateExists := templateByIdentity[key]
				if !podExists || !templateExists {
					return nil, errors.New("materialized launch resources do not cover every WorkerMember")
				}
				result = append(result, desiredMember{
					bundle: bundle, worker: worker, member: member, pod: pod, template: template,
				})
			}
		}
	}
	return result, nil
}

func verifyRegistryWorker(
	input Input,
	bundle fleetcontroller.WorkerBundleActuation,
	worker fleetcontroller.WorkerInstanceActuation,
	registry RegistryWorker,
) (WorkerEvidence, error) {
	if registry.InstanceEpoch != worker.InstanceEpoch || registry.ControlSessionEpoch <= 0 ||
		registry.ResidencyPlanRevisionID != input.Rollout.ApprovedPlan.ID ||
		registry.WorkerBundleID != bundle.WorkerBundleID ||
		registry.WorkerProfileRevisionID != worker.WorkerProfileRevisionID ||
		registry.CapacityPoolID != worker.CapacityPoolID ||
		registry.Lifecycle != "READY" || registry.Reachability != "HEALTHY" ||
		registry.DeviceSetID == uuid.Nil || !hexDigestPattern.MatchString(registry.DeviceSetDigest) ||
		!hexDigestPattern.MatchString(registry.MembershipDigest) ||
		len(registry.Members) != len(worker.Members) ||
		len(registry.Residencies) != len(worker.ModelRuntimes) {
		return WorkerEvidence{}, invalid("WorkerInstance %s Fleet authority is missing, stale, or not READY", worker.ID)
	}
	memberByID := make(map[uuid.UUID]RegistryMember, len(registry.Members))
	for _, member := range registry.Members {
		if member.ID == uuid.Nil {
			return WorkerEvidence{}, invalid("WorkerInstance %s has an invalid Registry member", worker.ID)
		}
		if _, duplicate := memberByID[member.ID]; duplicate {
			return WorkerEvidence{}, invalid("WorkerInstance %s has duplicate Registry members", worker.ID)
		}
		memberByID[member.ID] = member
	}
	for _, expected := range worker.Members {
		member, exists := memberByID[expected.ID]
		if !exists || member.Key != expected.Key || member.MemberEpoch != expected.MemberEpoch ||
			member.NodeIdentity != expected.NodeIdentity || member.ComputeNodeID == uuid.Nil ||
			member.Readiness != "READY" || !hexDigestPattern.MatchString(member.DeviceSubsetDigest) ||
			!hexDigestPattern.MatchString(member.IdentityDigest) ||
			len(member.Devices) != len(expected.DeviceConstraints) {
			return WorkerEvidence{}, invalid("WorkerMember %s Fleet authority is missing, stale, or not READY", expected.ID)
		}
		deviceByID := make(map[uuid.UUID]RegistryDevice, len(member.Devices))
		for _, device := range member.Devices {
			if _, duplicate := deviceByID[device.ID]; duplicate {
				return WorkerEvidence{}, invalid("WorkerMember %s has duplicate Registry devices", expected.ID)
			}
			deviceByID[device.ID] = device
		}
		for _, constraint := range expected.DeviceConstraints {
			device, exists := deviceByID[constraint.DeviceID]
			if !exists || device.DeviceEpoch != constraint.DeviceEpoch ||
				device.ComputeNodeID != member.ComputeNodeID || device.NodeIdentity != expected.NodeIdentity ||
				device.NodeEpoch <= 0 || device.AgentSessionEpoch <= 0 ||
				device.GPUUUID != constraint.GPUUUID || device.PCIBDF != constraint.PCIBDF ||
				device.Health != "HEALTHY" ||
				!hexDigestPattern.MatchString(device.NodeAttestationDigest) ||
				!hexDigestPattern.MatchString(device.DeviceAttestationDigest) {
				return WorkerEvidence{}, invalid("GPU %s Registry authority is missing, stale, or unhealthy", constraint.DeviceID)
			}
		}
	}
	residencies := append([]RegistryResidency(nil), registry.Residencies...)
	for _, runtime := range worker.ModelRuntimes {
		matched := -1
		for index, residency := range residencies {
			if residency.ID == runtime.ModelResidencyID {
				if matched >= 0 {
					return WorkerEvidence{}, invalid("WorkerInstance %s has duplicate ModelResidency route", worker.ID)
				}
				matched = index
			}
		}
		if matched < 0 {
			return WorkerEvidence{}, invalid("WorkerInstance %s is missing ModelResidency for %s", worker.ID, runtime.Component)
		}
		residency := residencies[matched]
		if residency.CapacityPoolID != runtime.CapacityPoolID ||
			residency.StageProfileRevisionID != runtime.StageProfileRevisionID ||
			residency.ModelComponentRevision != runtime.ModelComponentRevision ||
			residency.RuntimeIdentity != runtime.RuntimeIdentity ||
			residency.RuntimeImageDigest != imageDigest(bundle.RuntimeImage) ||
			residency.ModelRuntimeEpoch <= 0 || residency.State != "READY" ||
			!hexDigestPattern.MatchString(residency.WarmupEvidenceDigest) ||
			!hexDigestPattern.MatchString(residency.CanaryEvidenceDigest) {
			return WorkerEvidence{}, invalid("WorkerInstance %s ModelResidency is stale or not READY", worker.ID)
		}
	}
	sort.Slice(residencies, func(left, right int) bool {
		return residencies[left].ID.String() < residencies[right].ID.String()
	})
	return WorkerEvidence{
		WorkerInstanceID: worker.ID, InstanceEpoch: registry.InstanceEpoch,
		ControlSessionEpoch: registry.ControlSessionEpoch, WorkerBundleID: bundle.WorkerBundleID,
		WorkerProfileRevision: worker.WorkerProfileRevisionID, CapacityPoolID: worker.CapacityPoolID,
		Role: worker.Role, DeviceSetID: registry.DeviceSetID,
		DeviceSetDigest: registry.DeviceSetDigest, MembershipDigest: registry.MembershipDigest,
		Members: make([]MemberEvidence, 0, len(worker.Members)), Residencies: residencies,
	}, nil
}

func verifyKubernetesMember(
	input Input,
	expected desiredMember,
	pod corev1.Pod,
	template resourcev1.ResourceClaimTemplate,
	claims map[string]resourcev1.ResourceClaim,
	nodes map[string]corev1.Node,
	registry RegistryWorker,
) (MemberEvidence, error) {
	if pod.UID == "" || pod.ResourceVersion == "" || pod.DeletionTimestamp != nil ||
		!fleetcontroller.WorkerInstancePodMatches(pod, expected.pod) ||
		pod.Spec.NodeName != expected.member.NodeIdentity || pod.Status.Phase != corev1.PodRunning ||
		!podReady(pod) {
		return MemberEvidence{}, invalid("Pod %q does not match exact READY actuation authority", pod.Name)
	}
	if template.UID == "" || template.ResourceVersion == "" || template.Generation <= 0 ||
		template.DeletionTimestamp != nil ||
		!fleetcontroller.WorkerInstanceGPUClaimTemplateMatches(template, expected.template) {
		return MemberEvidence{}, invalid("ResourceClaimTemplate %q does not match exact actuation authority", template.Name)
	}
	initStatus, err := exactContainerStatus(pod.Status.InitContainerStatuses, "stage-worker-private-materialization")
	if err != nil || initStatus.RestartCount != 0 || initStatus.State.Terminated == nil ||
		initStatus.State.Terminated.ExitCode != 0 ||
		!imageIDMatches(initStatus.ImageID, expected.bundle.InitImage) {
		return MemberEvidence{}, invalid("Pod %q init image did not complete with the pinned identity", pod.Name)
	}
	agentStatus, err := exactContainerStatus(pod.Status.ContainerStatuses, "stage-worker-agent")
	if err != nil || !runningReadyWithoutRestart(agentStatus) ||
		!imageIDMatches(agentStatus.ImageID, expected.bundle.StageWorkerAgentImage) {
		return MemberEvidence{}, invalid("Pod %q Stage Worker container is not restart-free READY with the pinned image", pod.Name)
	}
	runtimeStatus, err := exactContainerStatus(pod.Status.ContainerStatuses, "model-runtime")
	if err != nil || !runningReadyWithoutRestart(runtimeStatus) ||
		!imageIDMatches(runtimeStatus.ImageID, expected.bundle.RuntimeImage) {
		return MemberEvidence{}, invalid("Pod %q ModelRuntime container is not restart-free READY with the pinned image", pod.Name)
	}
	claimName, err := podClaimName(pod)
	if err != nil {
		return MemberEvidence{}, invalid("Pod %q DRA claim status: %v", pod.Name, err)
	}
	claim, exists := claims[claimName]
	if !exists || claim.UID == "" || claim.ResourceVersion == "" || claim.DeletionTimestamp != nil ||
		claim.CreationTimestamp.IsZero() || claim.CreationTimestamp.After(input.CapturedAt) ||
		!reflect.DeepEqual(claim.Spec, template.Spec.Spec) || claim.Status.Allocation == nil ||
		(claim.Status.Allocation.AllocationTimestamp != nil &&
			claim.Status.Allocation.AllocationTimestamp.After(input.CapturedAt)) ||
		len(claim.Status.ReservedFor) != 1 || claim.Status.ReservedFor[0].APIGroup != "" ||
		claim.Status.ReservedFor[0].Resource != "pods" || claim.Status.ReservedFor[0].Name != pod.Name ||
		claim.Status.ReservedFor[0].UID != pod.UID {
		return MemberEvidence{}, invalid("ResourceClaim %q is missing, unallocated, or not reserved for exact Pod UID", claimName)
	}
	node, exists := nodes[pod.Spec.NodeName]
	if !exists || node.UID == "" || node.ResourceVersion == "" || node.DeletionTimestamp != nil {
		return MemberEvidence{}, invalid("Pod %q node identity is missing or stale", pod.Name)
	}
	registryMember, registryDevice, err := exactRegistryMemberDevice(registry, expected.member)
	if err != nil {
		return MemberEvidence{}, err
	}
	allocations := claim.Status.Allocation.Devices.Results
	if len(allocations) != len(expected.member.DeviceConstraints) || len(allocations) != 1 {
		return MemberEvidence{}, invalid("ResourceClaim %q allocation does not contain the exact single GPU", claimName)
	}
	allocation := allocations[0]
	if allocation.Request != "gpu-0" || allocation.Driver != "gpu.nvidia.com" {
		return MemberEvidence{}, invalid("ResourceClaim %q allocation identity is invalid", claimName)
	}
	device, slice, err := allocatedDevice(input.Kubernetes.ResourceSlices, allocation, pod.Spec.NodeName)
	if err != nil {
		return MemberEvidence{}, invalid("ResourceClaim %q: %v", claimName, err)
	}
	constraint := expected.member.DeviceConstraints[0]
	gpuUUID := deviceAttribute(device, "gpu.nvidia.com/uuid")
	pciBDF := deviceAttribute(device, "resource.kubernetes.io/pciBusID")
	if gpuUUID != constraint.GPUUUID || pciBDF != constraint.PCIBDF ||
		gpuUUID != registryDevice.GPUUUID || pciBDF != registryDevice.PCIBDF ||
		registryMember.NodeIdentity != pod.Spec.NodeName {
		return MemberEvidence{}, invalid("DRA allocation, Fleet device authority, and Pod node do not identify one exact GPU")
	}
	return MemberEvidence{
		WorkerMemberID: expected.member.ID, MemberEpoch: expected.member.MemberEpoch,
		MemberKey: expected.member.Key, PodName: pod.Name, PodUID: string(pod.UID),
		PodResourceVersion: pod.ResourceVersion, NodeName: pod.Spec.NodeName,
		NodeUID: string(node.UID), NodeResourceVersion: node.ResourceVersion,
		ClaimTemplateName: template.Name, ClaimTemplateUID: string(template.UID),
		ClaimTemplateVersion: template.ResourceVersion, ResourceClaimName: claim.Name,
		ResourceClaimUID: string(claim.UID), ResourceClaimVersion: claim.ResourceVersion,
		DRAResourceSliceUID: string(slice.UID), DRAResourceSliceVersion: slice.ResourceVersion,
		DRADriver: allocation.Driver, DRAPool: allocation.Pool, DRADevice: allocation.Device,
		DeviceID: constraint.DeviceID, DeviceEpoch: constraint.DeviceEpoch,
		GPUUUID: gpuUUID, PCIBDF: pciBDF,
		StageWorkerImageID: agentStatus.ImageID, ModelRuntimeImageID: runtimeStatus.ImageID,
		StageWorkerRestartCount:  agentStatus.RestartCount,
		ModelRuntimeRestartCount: runtimeStatus.RestartCount,
	}, nil
}

func allocatedDevice(
	slices []resourcev1.ResourceSlice,
	allocation resourcev1.DeviceRequestAllocationResult,
	nodeName string,
) (resourcev1.Device, resourcev1.ResourceSlice, error) {
	maximumGeneration := int64(-1)
	for _, slice := range slices {
		if slice.Spec.Driver == allocation.Driver && slice.Spec.Pool.Name == allocation.Pool &&
			slice.Spec.Pool.Generation > maximumGeneration {
			maximumGeneration = slice.Spec.Pool.Generation
		}
	}
	if maximumGeneration < 0 {
		return resourcev1.Device{}, resourcev1.ResourceSlice{}, errors.New("allocated DRA pool has no ResourceSlice")
	}
	current := make([]resourcev1.ResourceSlice, 0)
	var expectedCount int64
	for _, slice := range slices {
		if slice.Spec.Driver != allocation.Driver || slice.Spec.Pool.Name != allocation.Pool ||
			slice.Spec.Pool.Generation != maximumGeneration {
			continue
		}
		if slice.UID == "" || slice.ResourceVersion == "" || slice.Spec.Pool.ResourceSliceCount <= 0 ||
			(expectedCount != 0 && expectedCount != slice.Spec.Pool.ResourceSliceCount) {
			return resourcev1.Device{}, resourcev1.ResourceSlice{}, errors.New("allocated DRA pool has invalid current ResourceSlice identity")
		}
		expectedCount = slice.Spec.Pool.ResourceSliceCount
		current = append(current, slice)
	}
	if int64(len(current)) != expectedCount {
		return resourcev1.Device{}, resourcev1.ResourceSlice{}, errors.New("allocated DRA pool ResourceSlice generation is incomplete")
	}
	var matched *resourcev1.Device
	var matchedSlice *resourcev1.ResourceSlice
	for sliceIndex := range current {
		slice := &current[sliceIndex]
		for deviceIndex := range slice.Spec.Devices {
			device := &slice.Spec.Devices[deviceIndex]
			if device.Name != allocation.Device {
				continue
			}
			resolvedNode := ""
			if slice.Spec.NodeName != nil {
				resolvedNode = *slice.Spec.NodeName
			} else if device.NodeName != nil {
				resolvedNode = *device.NodeName
			}
			if resolvedNode != nodeName || matched != nil {
				return resourcev1.Device{}, resourcev1.ResourceSlice{}, errors.New("allocated DRA device is duplicated or belongs to another node")
			}
			deviceCopy := *device.DeepCopy()
			sliceCopy := *slice.DeepCopy()
			matched, matchedSlice = &deviceCopy, &sliceCopy
		}
	}
	if matched == nil {
		return resourcev1.Device{}, resourcev1.ResourceSlice{}, errors.New("allocated DRA device is absent from the current ResourceSlice generation")
	}
	return *matched, *matchedSlice, nil
}

func exactRegistryMemberDevice(
	worker RegistryWorker,
	expected fleetcontroller.WorkerMemberActuation,
) (RegistryMember, RegistryDevice, error) {
	for _, member := range worker.Members {
		if member.ID != expected.ID {
			continue
		}
		if len(member.Devices) != 1 || member.Devices[0].ID != expected.DeviceConstraints[0].DeviceID {
			return RegistryMember{}, RegistryDevice{}, invalid("WorkerMember %s does not own its exact GPU", expected.ID)
		}
		return member, member.Devices[0], nil
	}
	return RegistryMember{}, RegistryDevice{}, invalid("WorkerMember %s is missing from Fleet Registry", expected.ID)
}

func podReady(pod corev1.Pod) bool {
	ready := 0
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			ready++
			if condition.Status != corev1.ConditionTrue {
				return false
			}
		}
	}
	return ready == 1
}

func exactContainerStatus(statuses []corev1.ContainerStatus, name string) (corev1.ContainerStatus, error) {
	var result corev1.ContainerStatus
	count := 0
	for _, status := range statuses {
		if status.Name == name {
			result = status
			count++
		}
	}
	if count != 1 {
		return corev1.ContainerStatus{}, fmt.Errorf("container %q status count is %d", name, count)
	}
	return result, nil
}

func runningReadyWithoutRestart(status corev1.ContainerStatus) bool {
	return status.Ready && status.RestartCount == 0 && status.State.Running != nil
}

func podClaimName(pod corev1.Pod) (string, error) {
	if len(pod.Spec.ResourceClaims) != 1 || pod.Spec.ResourceClaims[0].Name != "gpu" ||
		pod.Spec.ResourceClaims[0].ResourceClaimTemplateName == nil ||
		len(pod.Status.ResourceClaimStatuses) != 1 || pod.Status.ResourceClaimStatuses[0].Name != "gpu" ||
		pod.Status.ResourceClaimStatuses[0].ResourceClaimName == nil ||
		*pod.Status.ResourceClaimStatuses[0].ResourceClaimName == "" {
		return "", errors.New("exact generated gpu ResourceClaim is absent")
	}
	return *pod.Status.ResourceClaimStatuses[0].ResourceClaimName, nil
}

func imageIDMatches(imageID, expected string) bool {
	digest := imageDigest(expected)
	return digest != "" && strings.HasSuffix(imageID, digest) && strings.Contains(imageID, "@")
}

func imageDigest(image string) string {
	separator := strings.LastIndex(image, "@")
	if separator < 0 || !sha256Pattern.MatchString(image[separator+1:]) {
		return ""
	}
	return image[separator+1:]
}

func deviceAttribute(device resourcev1.Device, name resourcev1.QualifiedName) string {
	attribute, exists := device.Attributes[name]
	if !exists || attribute.StringValue == nil {
		return ""
	}
	return *attribute.StringValue
}

func indexPods(values []corev1.Pod) (map[string]corev1.Pod, error) {
	result := make(map[string]corev1.Pod, len(values))
	for _, value := range values {
		if value.Name == "" || value.Namespace == "" {
			return nil, invalid("Kubernetes Pod identity is incomplete")
		}
		if _, duplicate := result[value.Name]; duplicate {
			return nil, invalid("Kubernetes snapshot contains duplicate Pod %q", value.Name)
		}
		result[value.Name] = value
	}
	return result, nil
}

func indexTemplates(values []resourcev1.ResourceClaimTemplate) (map[string]resourcev1.ResourceClaimTemplate, error) {
	result := make(map[string]resourcev1.ResourceClaimTemplate, len(values))
	for _, value := range values {
		if value.Name == "" || value.Namespace == "" {
			return nil, invalid("Kubernetes ResourceClaimTemplate identity is incomplete")
		}
		if _, duplicate := result[value.Name]; duplicate {
			return nil, invalid("Kubernetes snapshot contains duplicate ResourceClaimTemplate %q", value.Name)
		}
		result[value.Name] = value
	}
	return result, nil
}

func indexClaims(values []resourcev1.ResourceClaim) (map[string]resourcev1.ResourceClaim, error) {
	result := make(map[string]resourcev1.ResourceClaim, len(values))
	for _, value := range values {
		if value.Name == "" || value.Namespace == "" {
			return nil, invalid("Kubernetes ResourceClaim identity is incomplete")
		}
		if _, duplicate := result[value.Name]; duplicate {
			return nil, invalid("Kubernetes snapshot contains duplicate ResourceClaim %q", value.Name)
		}
		result[value.Name] = value
	}
	return result, nil
}

func indexNodes(values []corev1.Node) (map[string]corev1.Node, error) {
	result := make(map[string]corev1.Node, len(values))
	for _, value := range values {
		if value.Name == "" {
			return nil, invalid("Kubernetes Node identity is incomplete")
		}
		if _, duplicate := result[value.Name]; duplicate {
			return nil, invalid("Kubernetes snapshot contains duplicate Node %q", value.Name)
		}
		result[value.Name] = value
	}
	return result, nil
}

func indexRegistryWorkers(values []RegistryWorker) (map[uuid.UUID]RegistryWorker, error) {
	result := make(map[uuid.UUID]RegistryWorker, len(values))
	for _, value := range values {
		if value.ID == uuid.Nil {
			return nil, invalid("Fleet Registry WorkerInstance identity is incomplete")
		}
		if _, duplicate := result[value.ID]; duplicate {
			return nil, invalid("Fleet Registry snapshot contains duplicate WorkerInstance %s", value.ID)
		}
		result[value.ID] = value
	}
	return result, nil
}

func validText(value string, maximum int) bool {
	return len(value) > 0 && len(value) <= maximum && strings.TrimSpace(value) == value &&
		strings.IndexFunc(value, func(character rune) bool { return character < 0x20 || character == 0x7f }) == -1
}

func invalid(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidLaunchEvidence, fmt.Sprintf(format, arguments...))
}
