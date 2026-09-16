package fleetcontract

import corev1 "k8s.io/api/core/v1"

// RemovePodNetworkObservations removes only Calico's observed sandbox/IP
// metadata. These fields neither select a network nor authorize a workload;
// all other annotations remain part of the immutable Fleet template.
func RemovePodNetworkObservations(pod *corev1.Pod) {
	for _, key := range []string{"cni.projectcalico.org/containerID", "cni.projectcalico.org/podIP", "cni.projectcalico.org/podIPs"} {
		delete(pod.Annotations, key)
	}
	if len(pod.Annotations) == 0 {
		pod.Annotations = nil
	}
}
