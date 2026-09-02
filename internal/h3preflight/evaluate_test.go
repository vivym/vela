package h3preflight

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/h3launchevidence"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestEvaluatePassesExactThreeNodeH3DeploymentUnit(t *testing.T) {
	request, observation := passingFixture()
	report, err := Evaluate(request, observation)
	if err != nil {
		t.Fatalf("evaluate H3 preflight: %v", err)
	}
	if !report.Ready || report.SchemaVersion != SchemaVersion || report.MediaType != MediaType ||
		report.ReleaseDigest != request.ReleaseDigest ||
		report.ConfigurationRevision != request.ConfigurationRevision ||
		report.KubernetesClusterUID != request.KubernetesClusterUID ||
		report.KubernetesNamespaceUID != request.KubernetesNamespaceUID ||
		report.ResidencyPlanRevisionID != request.Rollout.ApprovedPlan.ID ||
		len(report.ExternalResources) != 2 || len(report.Checks) != 10 {
		t.Fatalf("H3 preflight report = %#v", report)
	}
	for _, check := range report.Checks {
		if check.Status != StatusPass || check.ReasonCode != ReasonSatisfied {
			t.Fatalf("H3 preflight check = %#v", check)
		}
	}
}

func TestEvaluateRejectsUnboundExternalResource(t *testing.T) {
	request, observation := passingFixture()
	mutable := false
	observation.Secrets[0].Immutable = &mutable

	report, err := Evaluate(request, observation)
	if err != nil {
		t.Fatalf("evaluate H3 preflight: %v", err)
	}
	for _, check := range report.Checks {
		if check.ID == CheckExternalResourceBinding {
			if check.Status != StatusFail || check.ReasonCode != ReasonExternalResourceMismatch || report.Ready {
				t.Fatalf("external resource check = %#v ready=%t", check, report.Ready)
			}
			return
		}
	}
	t.Fatal("external resource binding check is absent")
}

func TestEvaluateRejectsDifferentKubernetesIdentity(t *testing.T) {
	request, observation := passingFixture()
	observation.KubernetesClusterUID = "another-cluster"
	report, err := Evaluate(request, observation)
	if err != nil {
		t.Fatalf("evaluate H3 preflight: %v", err)
	}
	for _, check := range report.Checks {
		if check.ID == CheckKubernetesAPI {
			if check.Status != StatusFail || check.ReasonCode != ReasonKubernetesIdentity {
				t.Fatalf("Kubernetes identity check = %#v", check)
			}
			return
		}
	}
	t.Fatal("Kubernetes identity check is absent")
}

func TestEvaluateRejectsNodeThatCannotScheduleWorkerPod(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*corev1.Node)
	}{
		{
			name: "hostname selector mismatch",
			mutate: func(node *corev1.Node) {
				node.Labels[corev1.LabelHostname] = "another-node"
			},
		},
		{
			name: "untolerated taint",
			mutate: func(node *corev1.Node) {
				node.Spec.Taints = []corev1.Taint{{
					Key: "maintenance", Value: "true", Effect: corev1.TaintEffectNoSchedule,
				}}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			request, observation := passingFixture()
			test.mutate(&observation.Nodes[0])
			report, err := Evaluate(request, observation)
			if err != nil {
				t.Fatalf("evaluate H3 preflight: %v", err)
			}
			for _, check := range report.Checks {
				if check.ID == CheckSchedulableNodes {
					if check.Status != StatusFail || check.ReasonCode != ReasonInsufficientNodes {
						t.Fatalf("schedulable node check = %#v", check)
					}
					return
				}
			}
			t.Fatal("schedulable node check is absent")
		})
	}
}

func TestEvaluateUsesHostnameSelectorIdentityInsteadOfNodeObjectName(t *testing.T) {
	request, observation := passingFixture()
	actualNodeName := "node-object-a"
	observation.Nodes[0].Name = actualNodeName
	observation.ResourceSlices[0].Spec.NodeName = &actualNodeName
	report, err := Evaluate(request, observation)
	if err != nil {
		t.Fatalf("evaluate H3 preflight: %v", err)
	}
	if !report.Ready {
		t.Fatalf("H3 preflight rejected a valid hostname selector mapping: %#v", report)
	}
}

func TestEvaluateFailsClosedForMissingGPUAndInsufficientNodes(t *testing.T) {
	request, observation := passingFixture()
	observation.Nodes = observation.Nodes[:2]
	observation.ResourceSlices = observation.ResourceSlices[:2]
	report, err := Evaluate(request, observation)
	if err != nil {
		t.Fatalf("evaluate H3 preflight: %v", err)
	}
	if report.Ready {
		t.Fatalf("H3 preflight passed incomplete inventory: %#v", report)
	}
	checks := make(map[CheckID]Check, len(report.Checks))
	for _, check := range report.Checks {
		checks[check.ID] = check
	}
	if checks[CheckSchedulableNodes].ReasonCode != ReasonInsufficientNodes ||
		checks[CheckGPUIdentityClosure].ReasonCode != ReasonGPUIdentityMismatch {
		t.Fatalf("incomplete H3 preflight checks = %#v", checks)
	}
}

func TestEvaluateFailsClosedForAUXRuntimeWithoutCommand(t *testing.T) {
	request, observation := passingFixture()
	request.Rollout.WorkerBundles[0].WorkerInstances[0].ModelRuntimes[0].Command = nil
	report, err := Evaluate(request, observation)
	if err != nil {
		t.Fatalf("evaluate H3 preflight: %v", err)
	}
	for _, check := range report.Checks {
		if check.ID == CheckH3DeploymentUnit {
			if check.Status != StatusFail || check.ReasonCode != ReasonInvalidH3Unit {
				t.Fatalf("invalid AUX deployment check = %#v", check)
			}
			return
		}
	}
	t.Fatal("H3 deployment unit check is absent")
}

func passingFixture() (Request, Observation) {
	planID := uuid.MustParse("49360000-0000-0000-0000-000000000001")
	bundle := fleetcontroller.WorkerBundleActuation{
		WorkerBundleID: uuid.MustParse("49360000-0000-0000-0000-000000000002"),
	}
	devicesByNode := map[string][]resourcev1.Device{}
	for index := 0; index < 8; index++ {
		node := []string{"node-a", "node-b", "node-c"}[index%3]
		gpuUUID := fmt.Sprintf("GPU-49360000-0000-0000-0000-%012d", index+1)
		pciBDF := fmt.Sprintf("0000:%02x:00.0", 0x40+index)
		memberID := uuid.MustParse(fmt.Sprintf("49360000-0000-0000-0001-%012d", index+1))
		identityDigest := sha256.Sum256([]byte("spiffe://vela.internal/stage-worker/" + memberID.String()))
		member := fleetcontroller.WorkerMemberActuation{
			ID:  memberID,
			Key: "member-0", NodeIdentity: node, ResourceClass: "GPU", DeviceCount: 1,
			IdentityDigest: fmt.Sprintf("%x", identityDigest),
			DeviceConstraints: []fleetcontroller.DeviceConstraint{{
				DeviceID:    uuid.MustParse(fmt.Sprintf("49360000-0000-0000-0002-%012d", index+1)),
				DeviceEpoch: 1, GPUUUID: gpuUUID, PCIBDF: pciBDF,
			}},
		}
		worker := fleetcontroller.WorkerInstanceActuation{
			ID:            uuid.MustParse(fmt.Sprintf("49360000-0000-0000-0003-%012d", index+1)),
			CapacitySlots: 1, Members: []fleetcontroller.WorkerMemberActuation{member},
		}
		if index == 0 {
			worker.Role = "aux"
			worker.SharedSlotException = "encoder-vae-one-slot"
			worker.ModelRuntimes = []fleetcontroller.ModelRuntimeProcess{
				{Component: "ENCODER", Command: []string{"/opt/vela/bin/h3-encoder"}},
				{Component: "VAE_DECODER", Command: []string{"/opt/vela/bin/h3-vae-decoder"}},
			}
		} else {
			worker.Role = "dit"
			worker.ModelRuntimes = []fleetcontroller.ModelRuntimeProcess{{
				Component: "DIT", Command: []string{"/opt/vela/bin/h3-dit"},
			}}
		}
		bundle.WorkerInstances = append(bundle.WorkerInstances, worker)
		devicesByNode[node] = append(devicesByNode[node], resourcev1.Device{
			Name: fmt.Sprintf("gpu-%d", index),
			Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
				"uuid":                            {StringValue: stringPointer(gpuUUID)},
				"resource.kubernetes.io/pciBusID": {StringValue: stringPointer(pciBDF)},
			},
		})
	}
	rollout := fleetcontroller.ResidencyPlanRollout{WorkerBundles: []fleetcontroller.WorkerBundleActuation{bundle}}
	rollout.ApprovedPlan.ID = planID
	nodes := make([]corev1.Node, 0, 3)
	slices := make([]resourcev1.ResourceSlice, 0, 3)
	for index, name := range []string{"node-a", "node-b", "node-c"} {
		nodes = append(nodes, corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, UID: types.UID("node-uid-" + name), ResourceVersion: "1",
				Labels: map[string]string{corev1.LabelHostname: name},
			},
			Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
				Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			}}},
		})
		nodeName := name
		slices = append(slices, resourcev1.ResourceSlice{
			ObjectMeta: metav1.ObjectMeta{Name: "nvidia-" + name, UID: types.UID("slice-uid-" + name), ResourceVersion: "1"},
			Spec: resourcev1.ResourceSliceSpec{
				Driver: "gpu.nvidia.com", NodeName: &nodeName,
				Pool:    resourcev1.ResourcePool{Name: "pool-" + name, Generation: int64(index + 1), ResourceSliceCount: 1},
				Devices: devicesByNode[name],
			},
		})
	}
	return Request{
			ReleaseDigest:          "sha256:" + fmt.Sprintf("%064x", 1),
			ConfigurationRevision:  "sha256:" + fmt.Sprintf("%064x", 2),
			ValidationEnvironment:  "h3-preflight-test",
			KubernetesClusterUID:   "cluster-uid-1",
			KubernetesNamespaceUID: "namespace-uid-1",
			CheckedAt:              time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC),
			Rollout:                rollout,
			ExternalResources: []h3launchevidence.ExternalResourceExpectation{
				{Kind: "ConfigMap", Namespace: "vela-system", Name: "stage-worker-r1", Revision: "sha256:daad8764fd188cdc56f959f8441c2ebd3cd905c10965addaea458677ec8c862a"},
				{Kind: "Secret", Namespace: "vela-system", Name: "stage-worker-control-r1", Revision: "sha256:dcb53c139109dbb8a16ff33bf429c09d7ca6ff3f7c7e53d2b6879911808c6a1c", RequiredKeys: []string{"ca.crt", "tls.crt", "tls.key"}},
			},
		}, Observation{
			EvidenceRoleVerified:   true,
			KubernetesVersion:      "v1.34.1",
			KubernetesClusterUID:   "cluster-uid-1",
			KubernetesNamespaceUID: "namespace-uid-1",
			Nodes:                  nodes,
			DeviceClasses: []resourcev1.DeviceClass{{
				ObjectMeta: metav1.ObjectMeta{Name: "gpu.nvidia.com", UID: "class-uid", ResourceVersion: "1"},
			}},
			ResourceSlices: slices,
			ConfigMaps: []corev1.ConfigMap{{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "vela-system", Name: "stage-worker-r1", UID: "config-uid", ResourceVersion: "1",
					Annotations: map[string]string{"vela.ai/release-revision": "sha256:daad8764fd188cdc56f959f8441c2ebd3cd905c10965addaea458677ec8c862a"},
				},
				Immutable: boolPointer(true), Data: map[string]string{"control-address": "vela-control.vela-system.svc:9445"},
			}},
			Secrets: []corev1.Secret{{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "vela-system", Name: "stage-worker-control-r1", UID: "secret-uid", ResourceVersion: "1",
					Annotations: map[string]string{"vela.ai/release-revision": "sha256:dcb53c139109dbb8a16ff33bf429c09d7ca6ff3f7c7e53d2b6879911808c6a1c"},
				},
				Immutable: boolPointer(true), Type: corev1.SecretTypeOpaque,
				Data: map[string][]byte{"ca.crt": []byte("ca"), "tls.crt": []byte("cert"), "tls.key": []byte("key")},
			}},
		}
}

func stringPointer(value string) *string { return &value }
func boolPointer(value bool) *bool       { return &value }
