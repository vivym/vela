//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vivym/vela/internal/fleetcontract"
	"github.com/vivym/vela/internal/fleetcontroller"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (w *productionWorkload) validateKubernetesClaims(ctx context.Context, pod *corev1.Pod) error {
	if len(pod.Spec.ResourceClaims) == 0 {
		return nil // CPU media Worker; the signed template forbids extra claims.
	}
	if len(pod.Spec.ResourceClaims) != 1 || pod.Spec.ResourceClaims[0].Name != "gpu" ||
		len(pod.Status.ResourceClaimStatuses) != 1 || pod.Status.ResourceClaimStatuses[0].Name != "gpu" ||
		pod.Status.ResourceClaimStatuses[0].ResourceClaimName == nil {
		return errors.New("Pod lacks its exact generated GPU ResourceClaim")
	}
	claim, err := w.kube.ResourceV1().ResourceClaims(pod.Namespace).Get(ctx, *pod.Status.ResourceClaimStatuses[0].ResourceClaimName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	slices, err := w.kube.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{FieldSelector: "spec.nodeName=" + pod.Spec.NodeName})
	if err != nil {
		return err
	}
	return validateAllocatedDevices(pod, claim, slices.Items)
}

func validateAllocatedDevices(pod *corev1.Pod, claim *resourcev1.ResourceClaim, slices []resourcev1.ResourceSlice) error {
	var constraints []fleetcontroller.DeviceConstraint
	if json.Unmarshal([]byte(pod.Annotations[fleetcontract.DeviceConstraintsAnnotation]), &constraints) != nil || len(constraints) == 0 ||
		claim.Namespace != pod.Namespace || claim.DeletionTimestamp != nil || claim.Status.Allocation == nil ||
		len(claim.Status.Allocation.Devices.Results) != len(constraints) || len(claim.Status.ReservedFor) != 1 {
		return errors.New("GPU allocation does not match the signed device count or exclusive claim")
	}
	consumer := claim.Status.ReservedFor[0]
	if consumer.APIGroup != "" || consumer.Resource != "pods" || consumer.UID != pod.UID || consumer.Name != pod.Name {
		return errors.New("GPU claim belongs to another Pod incarnation")
	}
	owned := false
	for _, owner := range claim.OwnerReferences {
		if owner.APIVersion == "v1" && owner.Kind == "Pod" && owner.UID == pod.UID && owner.Name == pod.Name && owner.Controller != nil && *owner.Controller {
			owned = true
		}
	}
	if !owned {
		return errors.New("GPU ResourceClaim owner is not the live Pod")
	}
	seen := make(map[string]bool)
	for _, allocation := range claim.Status.Allocation.Devices.Results {
		if allocation.Driver != "gpu.nvidia.com" || allocation.AdminAccess != nil && *allocation.AdminAccess {
			return errors.New("GPU allocation uses an unapproved driver or admin access")
		}
		var match *resourcev1.Device
		for _, slice := range slices {
			if slice.Spec.Driver != allocation.Driver || slice.Spec.Pool.Name != allocation.Pool {
				continue
			}
			if slice.Spec.NodeName == nil || *slice.Spec.NodeName != pod.Spec.NodeName {
				return errors.New("GPU pool belongs to another node")
			}
			for _, device := range slice.Spec.Devices {
				if device.Name == allocation.Device {
					if match != nil {
						return errors.New("GPU identity is ambiguous across ResourceSlices")
					}
					copy := device
					match = &copy
				}
			}
		}
		if match == nil || match.AllowMultipleAllocations != nil && *match.AllowMultipleAllocations {
			return errors.New("GPU allocation is absent or shared")
		}
		gpu, pci := match.Attributes["uuid"].StringValue, match.Attributes["resource.kubernetes.io/pciBusID"].StringValue
		if gpu == nil {
			gpu = match.Attributes["gpu.nvidia.com/uuid"].StringValue
		}
		if gpu == nil || pci == nil || seen[*gpu] {
			return errors.New("GPU hardware identity is absent or duplicated")
		}
		found := false
		for index, constraint := range constraints {
			if constraint.GPUUUID == *gpu && constraint.PCIBDF == *pci && allocation.Request == fmt.Sprintf("gpu-%d", index) {
				found = true
			}
		}
		if !found {
			return errors.New("allocated GPU differs from the signed UUID or PCI address")
		}
		seen[*gpu] = true
	}
	return nil
}
