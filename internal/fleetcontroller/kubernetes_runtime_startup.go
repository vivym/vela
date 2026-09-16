package fleetcontroller

import (
	"strings"

	"github.com/vivym/vela/internal/runtimelaunch"
	corev1 "k8s.io/api/core/v1"
)

// configureKubernetesRuntimeStartup is opt-in and part of the signed bundle
// digest. kubelet owns containers and DRA; Node owns the one-shot startup gate.
func configureKubernetesRuntimeStartup(pod *corev1.Pod, member WorkerMemberActuation) {
	root := runtimelaunch.MemberRoot(member.ID.String())
	pod.Annotations[runtimelaunch.ProtocolAnnotation] = runtimelaunch.Protocol
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: runtimelaunch.Gate}}
	pod.Spec.RestartPolicy = corev1.RestartPolicyNever
	pod.Spec.RuntimeClassName = valuePointer("vela-runc")
	pod.Spec.EnableServiceLinks = valuePointer(false)
	pod.Spec.Priority = valuePointer(int32(0))
	pod.Spec.PreemptionPolicy = valuePointer(corev1.PreemptLowerPriority)
	pod.Spec.DeprecatedServiceAccount = pod.Spec.ServiceAccountName
	pod.Spec.Tolerations = append(pod.Spec.Tolerations,
		corev1.Toleration{Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: valuePointer(int64(300))},
		corev1.Toleration{Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: valuePointer(int64(300))})
	for _, volume := range []struct{ name, path string }{
		{"node-startup", root}, {"node-bootstrap", root + "/runtime-bootstrap"},
		{"node-launch", root + "/launch"}, {"pidfd-broker", runtimelaunch.BrokerRoot},
	} {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: volume.name, VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: volume.path, Type: valuePointer(corev1.HostPathDirectory)},
		}})
	}
	// All persistent directories are initialized by Node before gate release.
	// Never let an init container adopt or recursively chown a journal tree.
	for i := range pod.Spec.InitContainers {
		container := &pod.Spec.InitContainers[i]
		mounts := container.VolumeMounts[:0]
		for _, mount := range container.VolumeMounts {
			if mount.Name != "scratch" && mount.Name != "model-runtime-socket" {
				mounts = append(mounts, mount)
			}
		}
		container.VolumeMounts = mounts
	}
	var storage string
	for i := range pod.Spec.Volumes {
		volume := &pod.Spec.Volumes[i]
		if volume.Name == "scratch" {
			storage = volume.HostPath.Path
			volume.HostPath.Path = storage + "/work"
			volume.HostPath.Type = valuePointer(corev1.HostPathDirectory)
		}
	}
	// The Runtime socket is created by the Node startup path on the host. It
	// must be the same per-member HostPath as the worker scratch work root;
	// an EmptyDir would isolate the socket inside the Pod mount namespace and
	// make Node Agent live evidence impossible.
	for i := range pod.Spec.Volumes {
		volume := &pod.Spec.Volumes[i]
		if volume.Name == "model-runtime-socket" {
			volume.VolumeSource = corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
				Path: storage + "/work", Type: valuePointer(corev1.HostPathDirectory),
			}}
		}
	}
	for _, child := range []string{"worker-admission", "inputs", "outputs"} {
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "durable-" + child, VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: storage + "/provision/scratch/" + child, Type: valuePointer(corev1.HostPathDirectory)},
		}})
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "h3-shared-memory", VolumeSource: corev1.VolumeSource{
		EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: quantityPointer("2Gi")},
	}})
	for i := range pod.Spec.Containers {
		container := &pod.Spec.Containers[i]
		for i := range container.VolumeMounts {
			if container.VolumeMounts[i].Name == "model-runtime-socket" {
				container.VolumeMounts[i].MountPath = modelRuntimeSocketRoot + "/private"
			}
		}
		for _, child := range []string{"inputs", "outputs"} {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "durable-" + child, MountPath: stageWorkerScratchRoot + "/" + child})
		}
		container.VolumeMounts = append(container.VolumeMounts,
			corev1.VolumeMount{Name: "node-launch", MountPath: runtimelaunch.OfferRoot, ReadOnly: true},
			corev1.VolumeMount{Name: "pidfd-broker", MountPath: runtimelaunch.BrokerRoot, ReadOnly: true})
		switch container.Name {
		case "model-runtime":
			container.Command, container.Args = nil, nil
			// HOME comes from the sealed image config. Repeating it in the Pod
			// can produce duplicate OCI entries that Go deduplicates at exec.
			container.Env = nil
			for _, item := range runtimelaunch.DisabledServiceEnvironment() {
				name, value, _ := strings.Cut(item, "=")
				container.Env = append(container.Env, literalEnvironment(name, value))
			}
			container.WorkingDir = "/"
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "h3-shared-memory", MountPath: "/dev/shm"})
			container.VolumeMounts = append(container.VolumeMounts,
				corev1.VolumeMount{Name: "node-startup", MountPath: "/run/vela-node", ReadOnly: true},
				corev1.VolumeMount{Name: "node-bootstrap", MountPath: "/run/vela-model-runtime-bootstrap", ReadOnly: true})
		case "stage-worker-agent":
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "durable-worker-admission", MountPath: stageWorkerScratchRoot + "/worker-admission"})
			container.Command = []string{runtimelaunch.Entrypoint}
			container.Args = []string{"worker"}
			container.VolumeMounts = append(container.VolumeMounts,
				corev1.VolumeMount{Name: "node-startup", MountPath: root, ReadOnly: true},
				corev1.VolumeMount{Name: "model-runtime-private", MountPath: modelRuntimePrivateRoot, ReadOnly: true})
			container.Env = append(container.Env,
				literalEnvironment("VELA_WORKER_JOURNAL_SOCKET", root+"/worker-journal.sock"),
				literalEnvironment("VELA_WORKER_JOURNAL_PIDFD_BROKER_SOCKET", runtimelaunch.BrokerSocket),
				literalEnvironment("VELA_STAGE_WORKER_LAUNCH_MANIFEST_FILE", modelRuntimeLaunchManifest),
				literalEnvironment("VELA_STAGE_WORKER_ASSIGNMENT_STATE_DIRECTORY", stageWorkerScratchRoot+"/worker-admission"),
				literalEnvironment("VELA_STAGE_WORKER_ASSIGNMENT_MAX_RECORDS", "32"),
				literalEnvironment("VELA_STAGE_WORKER_JOURNAL_BINDING_FILE", root+"/worker-bootstrap/binding.json"),
				literalEnvironment("VELA_STAGE_WORKER_JOURNAL_BINDING_VERIFIER_KEYRING_FILE", root+"/worker-bootstrap/verifier.json"))
		}
	}
}
