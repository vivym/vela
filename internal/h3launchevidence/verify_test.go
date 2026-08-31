package h3launchevidence_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/h3launchevidence"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestVerifyProducesEvidenceOnlyForExactLiveLaunchBinding(t *testing.T) {
	input := exactLaunchInput(t)

	evidence, err := h3launchevidence.Verify(input)
	if err != nil {
		t.Fatalf("Verify exact live launch binding: %v", err)
	}
	if evidence.ReleaseDigest != input.ReleaseDigest ||
		evidence.ConfigurationRevision != input.ConfigurationRevision ||
		evidence.ResidencyPlanRevisionID != input.Rollout.ApprovedPlan.ID ||
		len(evidence.Workers) != 1 || len(evidence.Workers[0].Members) != 1 {
		t.Fatalf("launch evidence binding = %#v", evidence)
	}
	member := evidence.Workers[0].Members[0]
	if member.PodUID == "" || member.ResourceClaimUID == "" || member.GPUUUID == "" ||
		member.PCIBDF == "" || member.NodeUID == "" || member.ModelRuntimeImageID == "" {
		t.Fatalf("launch evidence omitted live identities: %#v", member)
	}
}

func TestVerifyAcceptsAllocationWithoutOptionalTimestamp(t *testing.T) {
	input := exactLaunchInput(t)
	input.Kubernetes.Claims[0].Status.Allocation.AllocationTimestamp = nil

	if _, err := h3launchevidence.Verify(input); err != nil {
		t.Fatalf("Verify allocation without optional timestamp: %v", err)
	}
}

func TestVerifyRejectsStaleOrInventedLaunchEvidence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*h3launchevidence.Input)
	}{
		{
			name: "stale WorkerInstance epoch",
			mutate: func(input *h3launchevidence.Input) {
				input.Registry.Workers[0].InstanceEpoch++
			},
		},
		{
			name: "stale WorkerMember epoch",
			mutate: func(input *h3launchevidence.Input) {
				input.Registry.Workers[0].Members[0].MemberEpoch++
			},
		},
		{
			name: "stale device epoch",
			mutate: func(input *h3launchevidence.Input) {
				input.Registry.Workers[0].Members[0].Devices[0].DeviceEpoch++
			},
		},
		{
			name: "DRA GPU UUID mismatch",
			mutate: func(input *h3launchevidence.Input) {
				value := "GPU-00000000-0000-0000-0000-000000000099"
				attribute := input.Kubernetes.ResourceSlices[0].Spec.Devices[0].Attributes["gpu.nvidia.com/uuid"]
				attribute.StringValue = &value
				input.Kubernetes.ResourceSlices[0].Spec.Devices[0].Attributes["gpu.nvidia.com/uuid"] = attribute
			},
		},
		{
			name: "runtime image ID mismatch",
			mutate: func(input *h3launchevidence.Input) {
				input.Kubernetes.Pods[0].Status.ContainerStatuses[1].ImageID =
					"docker-pullable://ghcr.io/vivym/vela-h3-stage-runtime@" + digest('0')
			},
		},
		{
			name: "runtime restarted",
			mutate: func(input *h3launchevidence.Input) {
				input.Kubernetes.Pods[0].Status.ContainerStatuses[1].RestartCount = 1
			},
		},
		{
			name: "ModelResidency not ready",
			mutate: func(input *h3launchevidence.Input) {
				input.Registry.Workers[0].Residencies[0].State = "WARMING"
			},
		},
		{
			name: "ResourceClaim reserved for another Pod incarnation",
			mutate: func(input *h3launchevidence.Input) {
				input.Kubernetes.Claims[0].Status.ReservedFor[0].UID = types.UID("invented-pod-uid")
			},
		},
		{
			name: "ResourceClaim created after capture",
			mutate: func(input *h3launchevidence.Input) {
				input.Kubernetes.Claims[0].CreationTimestamp = metav1.NewTime(input.CapturedAt.Add(time.Second))
			},
		},
		{
			name: "incomplete current ResourceSlice generation",
			mutate: func(input *h3launchevidence.Input) {
				input.Kubernetes.ResourceSlices[0].Spec.Pool.ResourceSliceCount = 2
			},
		},
		{
			name: "duplicate live Pod",
			mutate: func(input *h3launchevidence.Input) {
				input.Kubernetes.Pods = append(input.Kubernetes.Pods, input.Kubernetes.Pods[0])
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := exactLaunchInput(t)
			test.mutate(&input)
			if _, err := h3launchevidence.Verify(input); !errors.Is(err, h3launchevidence.ErrInvalidLaunchEvidence) {
				t.Fatalf("Verify error = %v, want ErrInvalidLaunchEvidence", err)
			}
		})
	}
}

func exactLaunchInput(t *testing.T) h3launchevidence.Input {
	t.Helper()
	now := time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)
	planID := uuid.MustParse("49320000-0000-0000-0000-000000000001")
	bundleID := uuid.MustParse("49320000-0000-0000-0000-000000000002")
	poolID := uuid.MustParse("49320000-0000-0000-0000-000000000003")
	workerID := uuid.MustParse("49320000-0000-0000-0000-000000000004")
	profileID := uuid.MustParse("49320000-0000-0000-0000-000000000005")
	memberID := uuid.MustParse("49320000-0000-0000-0000-000000000006")
	deviceID := uuid.MustParse("49320000-0000-0000-0000-000000000007")
	computeNodeID := uuid.MustParse("49320000-0000-0000-0000-000000000008")
	deviceSetID := uuid.MustParse("49320000-0000-0000-0000-000000000009")
	residencyID := uuid.MustParse("49320000-0000-0000-0000-00000000000a")
	runtimeDigest := digest('3')
	bundle := fleetcontroller.WorkerBundleActuation{
		SchemaVersion: 1, PlanRevisionID: planID, WorkerBundleID: bundleID,
		Namespace:                      "vela-system",
		InitImage:                      "docker.io/library/busybox@" + digest('1'),
		StageWorkerAgentImage:          "ghcr.io/vivym/vela-stage-worker-agent@" + digest('2'),
		RuntimeImage:                   "ghcr.io/vivym/vela-h3-stage-runtime@" + runtimeDigest,
		StageWorkerConfigMap:           "stage-worker-r1",
		StageWorkerControlTLSSecret:    "stage-worker-control-r1",
		StageWorkerAuthoritySecret:     "stage-worker-authority-r1",
		ArtifactStoreCredentialsSecret: "artifact-credentials-r1",
		ArtifactStoreCASecret:          "artifact-ca-r1",
		WorkerInstances: []fleetcontroller.WorkerInstanceActuation{{
			ID: workerID, InstanceEpoch: 7, WorkerProfileRevisionID: profileID,
			CapacityPoolID: poolID, Role: "dit", CapacitySlots: 1,
			ModelRuntimes: []fleetcontroller.ModelRuntimeProcess{{
				Component: "DIT", ModelComponentRevision: "h3-dit-r17",
				RuntimeIdentity: "h3-dit-runtime-r4", Command: []string{"/opt/vela/bin/h3-dit"},
			}},
			Members: []fleetcontroller.WorkerMemberActuation{{
				ID: memberID, MemberEpoch: 11, Key: "member-0", NodeIdentity: "gpu-node-01",
				ResourceClass: "GPU", DeviceCount: 1,
				DeviceConstraints: []fleetcontroller.DeviceConstraint{{
					DeviceID: deviceID, DeviceEpoch: 13,
					GPUUUID: "GPU-00000000-0000-0000-0000-000000000007",
					PCIBDF:  "0000:41:00.0",
				}},
			}},
		}},
	}
	var err error
	bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
	if err != nil {
		t.Fatalf("compute WorkerBundle digest: %v", err)
	}
	rollout := fleetcontroller.ResidencyPlanRollout{
		ApprovedPlan: fleet.ApprovedResidencyPlan{
			SchemaVersion: 1, ID: planID, StableID: "h3-production", Revision: 4,
			ContentDigest: digestHex('4'), ApprovalEvidenceDigest: digestHex('5'),
			ApprovedAt: now.Add(-time.Hour), ApprovedBy: "spiffe://vela/fleet-approver",
			CapacityPools: []fleet.PlannedCapacityPool{{
				ID: poolID, StableID: "h3-dit", StageProfileRevisionID: uuid.New(),
				ResourceClass: "GPU", SecurityClass: "INTERNAL", Region: "cn-north-1",
				MaxReadyQueueDepth: 16,
			}},
			WorkerBundles: []fleet.PlannedWorkerBundle{{
				ID: bundleID, StableID: "h3-bundle", DesiredGeneration: 4,
				LayoutDigest: bundle.RevisionDigest,
			}},
			WorkerInstances: []fleet.PlannedWorkerInstance{{
				ID: workerID, WorkerProfileRevisionID: profileID, CapacityPoolID: poolID,
				WorkerBundleID: bundleID, DesiredMemberCount: 1, DesiredDeviceCount: 1,
			}},
		},
		WorkerBundles: []fleetcontroller.WorkerBundleActuation{bundle},
	}
	pods, templates, err := fleetcontroller.MaterializeWorkerInstanceLaunchResources(bundle)
	if err != nil {
		t.Fatalf("materialize expected launch resources: %v", err)
	}
	pod := pods[0]
	pod.UID = types.UID("pod-uid-1")
	pod.ResourceVersion = "101"
	pod.CreationTimestamp = metav1.NewTime(now.Add(-10 * time.Minute))
	pod.Spec.NodeName = "gpu-node-01"
	pod.Status = corev1.PodStatus{
		Phase:      corev1.PodRunning,
		Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		InitContainerStatuses: []corev1.ContainerStatus{{
			Name: "stage-worker-private-materialization", Image: bundle.InitImage,
			ImageID: "docker-pullable://docker.io/library/busybox@" + digest('1'), Ready: true,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
		}},
		ContainerStatuses: []corev1.ContainerStatus{
			{Name: "stage-worker-agent", Image: bundle.StageWorkerAgentImage,
				ImageID: "docker-pullable://ghcr.io/vivym/vela-stage-worker-agent@" + digest('2'), Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now.Add(-9 * time.Minute))}}},
			{Name: "model-runtime", Image: bundle.RuntimeImage,
				ImageID: "docker-pullable://ghcr.io/vivym/vela-h3-stage-runtime@" + runtimeDigest, Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now.Add(-9 * time.Minute))}}},
		},
	}
	claimName := "generated-gpu-claim-1"
	pod.Status.ResourceClaimStatuses = []corev1.PodResourceClaimStatus{{
		Name: "gpu", ResourceClaimName: &claimName,
	}}
	template := templates[0]
	template.UID = types.UID("template-uid-1")
	template.ResourceVersion = "102"
	template.Generation = 1
	template.CreationTimestamp = metav1.NewTime(now.Add(-10 * time.Minute))
	claim := resourcev1.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: bundle.Namespace, Name: claimName, UID: types.UID("claim-uid-1"),
			ResourceVersion: "103", CreationTimestamp: metav1.NewTime(now.Add(-9 * time.Minute)),
		},
		Spec: template.Spec.Spec,
		Status: resourcev1.ResourceClaimStatus{
			ReservedFor: []resourcev1.ResourceClaimConsumerReference{{
				Resource: "pods", Name: pod.Name, UID: pod.UID,
			}},
			Allocation: &resourcev1.AllocationResult{
				AllocationTimestamp: timePointer(now.Add(-9 * time.Minute)),
				Devices: resourcev1.DeviceAllocationResult{Results: []resourcev1.DeviceRequestAllocationResult{{
					Request: "gpu-0", Driver: "gpu.nvidia.com", Pool: "gpu-node-01", Device: "gpu-0",
				}}},
			},
		},
	}
	nodeName := "gpu-node-01"
	resourceSlice := resourcev1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-node-01-r1", UID: types.UID("slice-uid-1"), ResourceVersion: "104"},
		Spec: resourcev1.ResourceSliceSpec{
			Driver: "gpu.nvidia.com", Pool: resourcev1.ResourcePool{Name: "gpu-node-01", Generation: 3, ResourceSliceCount: 1},
			NodeName: &nodeName,
			Devices: []resourcev1.Device{{
				Name: "gpu-0",
				Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					"gpu.nvidia.com/uuid":             {StringValue: stringPointer("GPU-00000000-0000-0000-0000-000000000007")},
					"resource.kubernetes.io/pciBusID": {StringValue: stringPointer("0000:41:00.0")},
				},
			}},
		},
	}

	return h3launchevidence.Input{
		ReleaseDigest: digest('a'), ConfigurationRevision: digest('b'),
		ValidationEnvironment: "h3-production-cn-north-1", CollectorIdentity: "spiffe://vela/launch-evidence/collector",
		CapturedAt: now, Rollout: rollout,
		Kubernetes: h3launchevidence.KubernetesSnapshot{
			ClusterUID: "cluster-uid-1", NamespaceUID: "namespace-uid-1",
			Pods: []corev1.Pod{pod}, ClaimTemplates: []resourcev1.ResourceClaimTemplate{template},
			Claims: []resourcev1.ResourceClaim{claim},
			Nodes: []corev1.Node{{ObjectMeta: metav1.ObjectMeta{
				Name: "gpu-node-01", UID: types.UID("node-uid-1"), ResourceVersion: "105",
			}}},
			ResourceSlices: []resourcev1.ResourceSlice{resourceSlice},
		},
		Registry: h3launchevidence.RegistrySnapshot{
			DatabaseTime: now, TransactionID: "42", SnapshotID: "00000003-0000001B-1",
			Workers: []h3launchevidence.RegistryWorker{{
				ID: workerID, InstanceEpoch: 7, ControlSessionEpoch: 17,
				ResidencyPlanRevisionID: planID, WorkerBundleID: bundleID,
				WorkerProfileRevisionID: profileID, CapacityPoolID: poolID,
				Lifecycle: "READY", Reachability: "HEALTHY", DeviceSetID: deviceSetID,
				DeviceSetDigest: digestHex('6'), MembershipDigest: digestHex('7'),
				Members: []h3launchevidence.RegistryMember{{
					ID: memberID, Key: "member-0", MemberEpoch: 11, ComputeNodeID: computeNodeID,
					NodeIdentity: "gpu-node-01", Readiness: "READY",
					DeviceSubsetDigest: digestHex('8'), IdentityDigest: digestHex('9'),
					Devices: []h3launchevidence.RegistryDevice{{
						ID: deviceID, DeviceEpoch: 13, ComputeNodeID: computeNodeID,
						NodeIdentity: "gpu-node-01", NodeEpoch: 19, AgentSessionEpoch: 23,
						GPUUUID: "GPU-00000000-0000-0000-0000-000000000007", PCIBDF: "0000:41:00.0",
						Health: "HEALTHY", NodeAttestationDigest: digestHex('c'), DeviceAttestationDigest: digestHex('d'),
					}},
				}},
				Residencies: []h3launchevidence.RegistryResidency{{
					ID: residencyID, ModelComponentRevision: "h3-dit-r17", RuntimeIdentity: "h3-dit-runtime-r4",
					RuntimeImageDigest: runtimeDigest, ModelRuntimeEpoch: 29, State: "READY",
					WarmupEvidenceDigest: digestHex('e'), CanaryEvidenceDigest: digestHex('f'),
				}},
			}},
		},
	}
}

func digest(character byte) string       { return "sha256:" + strings.Repeat(string(character), 64) }
func digestHex(character byte) string    { return strings.Repeat(string(character), 64) }
func stringPointer(value string) *string { return &value }
func timePointer(value time.Time) *metav1.Time {
	timestamp := metav1.NewTime(value)
	return &timestamp
}
