package fleetcontroller_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetadmission"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/runtimelaunch"
	corev1 "k8s.io/api/core/v1"
)

type rejectStartupRetirement struct{}

func (rejectStartupRetirement) AuthorizeMutation(context.Context, fleet.MutationAuthorizationRequest) (fleet.MutationAuthorizationResult, error) {
	return fleet.MutationAuthorizationResult{}, errors.New("startup must not use retirement authority")
}

func TestKubernetesStartupGateUsesRealCreateValidator(t *testing.T) {
	bundle, err := fleetcontroller.BuildH3WorkerBundleActuation(h3BundleSpec())
	if err != nil {
		t.Fatal(err)
	}
	bundle.RuntimeLaunchProtocol = runtimelaunch.Protocol
	bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := fleetcontroller.NewWorkerInstancePodAdmissionValidator([]fleetcontroller.ResidencyPlanRollout{
		residencyPlanRolloutForBundle(bundle, "h3-gated", "h3-gated-node"),
	})
	if err != nil {
		t.Fatal(err)
	}
	pods, _, err := fleetcontroller.MaterializeWorkerInstanceLaunchResources(bundle)
	if err != nil {
		t.Fatal(err)
	}
	pod := pods[0]
	// Kubernetes allocates the UID before the validating CREATE webhook.
	pod.UID = "7786776e-2d06-4c9c-80a5-e8e9984680f8"
	if err := validator.ValidateProtectedPodCreate(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	prebound := pod.DeepCopy()
	prebound.Spec.NodeName = pod.Spec.NodeSelector[corev1.LabelHostname]
	if err := validator.ValidateProtectedPodCreate(t.Context(), *prebound); err == nil {
		t.Fatal("CREATE with a node binding bypassing the scheduling gate was accepted")
	}
	ungated := pod.DeepCopy()
	ungated.Spec.SchedulingGates = nil
	if err := validator.ValidateProtectedPodCreate(t.Context(), *ungated); err == nil {
		t.Fatal("CREATE without the Node startup gate was accepted")
	}
	const prefix = "system:serviceaccount:vela-system:vela-node-"
	handler, err := fleetadmission.NewHandler(rejectStartupRetirement{}, fleetadmission.Config{
		FleetUsername: "system:serviceaccount:vela-system:vela-fleet-controller", NodeUsernamePrefix: prefix, CreateValidator: validator,
	})
	if err != nil {
		t.Fatal(err)
	}
	pod.UID = "7786776e-2d06-4c9c-80a5-e8e9984680f8"
	pod.ResourceVersion = "42"
	current := pod.DeepCopy()
	current.Spec.SchedulingGates = nil
	wire, err := json.Marshal(map[string]any{
		"apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview",
		"request": map[string]any{
			"uid": "node-startup-gate", "operation": "UPDATE",
			"kind":      map[string]string{"group": "", "version": "v1", "kind": "Pod"},
			"resource":  map[string]string{"group": "", "version": "v1", "resource": "pods"},
			"userInfo":  map[string]string{"username": prefix + pod.Spec.NodeSelector[corev1.LabelHostname]},
			"oldObject": pod, "object": current,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("POST", "/", bytes.NewReader(wire)))
	var result struct {
		Response struct {
			Allowed bool `json:"allowed"`
		} `json:"response"`
	}
	if json.Unmarshal(response.Body.Bytes(), &result) != nil || !result.Response.Allowed {
		t.Fatalf("Node gate release rejected by actual template validator: %s", response.Body.String())
	}
}

func TestKubernetesStartupRenderingAndOnlyGateRemovalMatch(t *testing.T) {
	bundle, err := fleetcontroller.BuildH3WorkerBundleActuation(h3BundleSpec())
	if err != nil {
		t.Fatal(err)
	}
	bundle.RuntimeLaunchProtocol = runtimelaunch.Protocol
	bundle.ImagePullSecrets = []string{"vela-release-pull-v1"}
	bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
	if err != nil {
		t.Fatal(err)
	}
	pods, claims, err := fleetcontroller.MaterializeWorkerInstanceLaunchResources(bundle)
	if err != nil {
		t.Fatal(err)
	}
	repeated, _, err := fleetcontroller.MaterializeWorkerInstanceLaunchResources(bundle)
	if err != nil || !reflect.DeepEqual(repeated, pods) {
		t.Fatal("non-deterministic signed Pod materialization", err)
	}
	if len(claims) != 8 {
		t.Fatal("DRA claims lost")
	}
	for _, pod := range pods {
		if len(pod.Spec.ImagePullSecrets) != 1 || pod.Spec.ImagePullSecrets[0].Name != "vela-release-pull-v1" {
			t.Fatal("signed private-registry pull identity was lost")
		}
		if len(pod.Spec.SchedulingGates) != 1 || pod.Spec.SchedulingGates[0].Name != runtimelaunch.Gate || pod.Spec.RestartPolicy != corev1.RestartPolicyNever || *pod.Spec.EnableServiceLinks {
			t.Fatal("Pod can start without Node custody or inherit stale grant")
		}
		for _, container := range pod.Spec.Containers {
			if container.Name == "model-runtime" {
				if len(container.Command) != 0 || len(container.Args) != 0 || len(container.Env) != 8 {
					t.Fatal("Runtime must use approved image defaults and clean environment")
				}
				for i, item := range container.Env {
					if item.ValueFrom != nil || item.Name+"="+item.Value != runtimelaunch.DisabledServiceEnvironment()[i] {
						t.Fatal("Runtime can inherit kubelet API Service links")
					}
				}
			}
		}
		live := pod.DeepCopy()
		live.APIVersion, live.Kind = "", ""
		live.Spec.SchedulingGates = nil
		live.Annotations["cni.projectcalico.org/containerID"] = "sandbox"
		live.Annotations["cni.projectcalico.org/podIP"] = "10.42.3.20/32"
		if !fleetcontroller.WorkerInstancePodMatches(*live, pod) {
			t.Fatal("released gate was rejected")
		}
		live.Annotations["k8s.v1.cni.cncf.io/networks"] = "unapproved"
		if fleetcontroller.WorkerInstancePodMatches(*live, pod) {
			t.Fatal("network observation concealed a network selection mutation")
		}
		delete(live.Annotations, "k8s.v1.cni.cncf.io/networks")
		live.Spec.Containers[0].Image = "untrusted"
		if fleetcontroller.WorkerInstancePodMatches(*live, pod) {
			t.Fatal("gate removal concealed an image mutation")
		}
	}
}

func TestKubernetesStartupBindsRuntimeSocketToMemberWorkDirectory(t *testing.T) {
	bundle, err := fleetcontroller.BuildH3WorkerBundleActuation(h3BundleSpec())
	if err != nil {
		t.Fatal(err)
	}
	bundle.RuntimeLaunchProtocol = runtimelaunch.Protocol
	bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
	if err != nil {
		t.Fatal(err)
	}
	pods, _, err := fleetcontroller.MaterializeWorkerInstanceLaunchResources(bundle)
	if err != nil {
		t.Fatal(err)
	}
	for _, pod := range pods {
		member := pod.Labels["vela.ai/worker-member-id"]
		wantHost := "/var/lib/vela/worker-instances/" + pod.Labels["vela.ai/worker-instance-id"] + "/member-0/work"
		var socket *corev1.Volume
		for i := range pod.Spec.Volumes {
			if pod.Spec.Volumes[i].Name == "model-runtime-socket" {
				socket = &pod.Spec.Volumes[i]
			}
		}
		if socket == nil || socket.HostPath == nil || socket.HostPath.Path != wantHost || socket.EmptyDir != nil {
			t.Fatalf("member %s runtime socket volume = %#v, want HostPath %s", member, socket, wantHost)
		}
		for _, container := range pod.Spec.InitContainers {
			for _, mount := range container.VolumeMounts {
				if mount.Name == "model-runtime-socket" || mount.Name == "scratch" {
					t.Fatalf("initializer %s can mutate Node-owned storage through %s", container.Name, mount.Name)
				}
			}
			for _, command := range container.Command {
				if strings.Contains(command, "/run/vela-model-runtime") {
					t.Fatalf("initializer %s accesses an unmounted read-only Runtime directory", container.Name)
				}
			}
		}
		for _, container := range pod.Spec.Containers {
			for _, mount := range container.VolumeMounts {
				if mount.Name == "model-runtime-socket" && mount.MountPath != "/run/vela-model-runtime/private" {
					t.Fatalf("container %s runtime socket mount = %s", container.Name, mount.MountPath)
				}
			}
		}
	}
}
