package fleetadmission

import (
	"testing"

	"github.com/vivym/vela/internal/runtimelaunch"
	corev1 "k8s.io/api/core/v1"
)

func TestNodeCanOnlyReleaseItsOwnApprovedStartupGate(t *testing.T) {
	const prefix = "system:serviceaccount:vela-system:vela-node-"
	old := workerInstancePod(true)
	old.Spec.NodeName = ""
	old.Spec.NodeSelector = map[string]string{corev1.LabelHostname: "gpu-11"}
	old.Spec.RestartPolicy = corev1.RestartPolicyNever
	old.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: runtimelaunch.Gate}}
	old.Annotations = map[string]string{runtimelaunch.ProtocolAnnotation: runtimelaunch.Protocol}
	template := *old.DeepCopy()
	template.UID = ""
	handler, err := NewHandler(&recordingAuthorizer{}, Config{
		FleetUsername: fleetUsername, NodeUsernamePrefix: prefix, CreateValidator: exactPodValidator{wantPod: template},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"valid", "other-node", "ordinary-user", "delete", "image-change", "finalizer-removal", "different-uid", "different-node", "remaining-gate", "replay"} {
		t.Run(scenario, func(t *testing.T) {
			previous := old.DeepCopy()
			current := old.DeepCopy()
			current.Spec.SchedulingGates = nil
			actor, operation := prefix+"gpu-11", "UPDATE"
			switch scenario {
			case "other-node":
				actor = prefix + "gpu-12"
			case "ordinary-user":
				actor = "developer"
			case "delete":
				operation = "DELETE"
			case "image-change":
				current.Spec.Containers = []corev1.Container{{Name: "model-runtime", Image: "untrusted"}}
			case "finalizer-removal":
				current.Finalizers = nil
			case "different-uid":
				current.UID = "replacement"
			case "different-node":
				current.Spec.NodeSelector[corev1.LabelHostname] = "gpu-12"
			case "remaining-gate":
				current.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "other/gate"}}
			case "replay":
				previous.Spec.SchedulingGates = nil
			}
			response := serveAdmission(t, handler, review("node-gate", operation, actor, previous, current))
			if response.Response.Allowed != (scenario == "valid") {
				t.Fatalf("unexpected authorization: %+v", response.Response)
			}
		})
	}
}

func TestNetworkActorCanOnlyUpdatePodNetworkObservations(t *testing.T) {
	const network = "system:serviceaccount:kube-system:canal"
	old := workerInstancePod(true)
	old.Spec.NodeName = "gpu-11"
	old.Annotations = map[string]string{runtimelaunch.ProtocolAnnotation: runtimelaunch.Protocol}
	handler, err := NewHandler(&recordingAuthorizer{}, Config{FleetUsername: fleetUsername, NetworkUsername: network, CreateValidator: exactPodValidator{wantPod: old}})
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"valid", "ordinary-user", "image", "network-selection", "uid", "finalizer", "gate", "delete"} {
		t.Run(scenario, func(t *testing.T) {
			current := old.DeepCopy()
			current.Annotations["cni.projectcalico.org/containerID"] = "sandbox"
			current.Annotations["cni.projectcalico.org/podIP"] = "10.42.3.20/32"
			actor, operation := network, "UPDATE"
			switch scenario {
			case "ordinary-user":
				actor = "developer"
			case "image":
				current.Spec.Containers = []corev1.Container{{Name: "model-runtime", Image: "untrusted"}}
			case "network-selection":
				current.Annotations["k8s.v1.cni.cncf.io/networks"] = "unapproved"
			case "uid":
				current.UID = "replacement"
			case "finalizer":
				current.Finalizers = nil
			case "gate":
				current.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: runtimelaunch.Gate}}
			case "delete":
				operation = "DELETE"
			}
			response := serveAdmission(t, handler, review("network-metadata", operation, actor, old, current))
			if response.Response.Allowed != (scenario == "valid") {
				t.Fatalf("unexpected network authorization: %+v", response.Response)
			}
		})
	}
}
