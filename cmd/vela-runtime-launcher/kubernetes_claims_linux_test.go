//go:build linux

package main

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontract"
	"github.com/vivym/vela/internal/fleetcontroller"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestKubernetesGPUAllocationBindsExclusiveExactDevice(t *testing.T) {
	node, gpu, pci := "server-22", "GPU-c08137e5-6932-a25d-27d3-03506fbf924c", "0000:01:00.0"
	controller, shared := true, false
	constraints, _ := json.Marshal([]fleetcontroller.DeviceConstraint{{DeviceID: uuid.New(), DeviceEpoch: 1, GPUUUID: gpu, PCIBDF: pci}})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "vela-system", Name: "worker", UID: types.UID(uuid.NewString()), Annotations: map[string]string{fleetcontract.DeviceConstraintsAnnotation: string(constraints)}}, Spec: corev1.PodSpec{NodeName: node}}
	claim := &resourcev1.ResourceClaim{ObjectMeta: metav1.ObjectMeta{Namespace: pod.Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID, Controller: &controller}}}, Status: resourcev1.ResourceClaimStatus{
		ReservedFor: []resourcev1.ResourceClaimConsumerReference{{Resource: "pods", Name: pod.Name, UID: pod.UID}},
		Allocation:  &resourcev1.AllocationResult{Devices: resourcev1.DeviceAllocationResult{Results: []resourcev1.DeviceRequestAllocationResult{{Request: "gpu-0", Driver: "gpu.nvidia.com", Pool: node, Device: "gpu-2"}}}},
	}}
	slice := resourcev1.ResourceSlice{Spec: resourcev1.ResourceSliceSpec{Driver: "gpu.nvidia.com", Pool: resourcev1.ResourcePool{Name: node}, NodeName: &node, Devices: []resourcev1.Device{{Name: "gpu-2", AllowMultipleAllocations: &shared, Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{"uuid": {StringValue: &gpu}, "resource.kubernetes.io/pciBusID": {StringValue: &pci}}}}}}
	for _, scenario := range []string{"valid", "other-pod", "extra-consumer", "wrong-gpu", "wrong-pci", "shared-device", "duplicate-slice", "wrong-request", "wrong-node", "no-owner", "admin-access"} {
		t.Run(scenario, func(t *testing.T) {
			c, s := claim.DeepCopy(), slice.DeepCopy()
			ss := []resourcev1.ResourceSlice{*s}
			switch scenario {
			case "other-pod":
				c.Status.ReservedFor[0].UID = types.UID(uuid.NewString())
			case "extra-consumer":
				c.Status.ReservedFor = append(c.Status.ReservedFor, c.Status.ReservedFor[0])
			case "wrong-gpu":
				wrong := "GPU-unknown"
				ss[0].Spec.Devices[0].Attributes["uuid"] = resourcev1.DeviceAttribute{StringValue: &wrong}
			case "wrong-pci":
				wrong := "0000:02:00.0"
				ss[0].Spec.Devices[0].Attributes["resource.kubernetes.io/pciBusID"] = resourcev1.DeviceAttribute{StringValue: &wrong}
			case "shared-device":
				ss[0].Spec.Devices[0].AllowMultipleAllocations = &controller
			case "duplicate-slice":
				ss = append(ss, *s)
			case "wrong-request":
				c.Status.Allocation.Devices.Results[0].Request = "gpu-1"
			case "wrong-node":
				other := "server-23"
				ss[0].Spec.NodeName = &other
			case "no-owner":
				c.OwnerReferences = nil
			case "admin-access":
				c.Status.Allocation.Devices.Results[0].AdminAccess = &controller
			}
			if err := validateAllocatedDevices(pod, c, ss); (err == nil) != (scenario == "valid") {
				t.Fatalf("allocation acceptance mismatch: %v", err)
			}
		})
	}
}
