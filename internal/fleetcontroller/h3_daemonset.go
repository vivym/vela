package fleetcontroller

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	workerScratchRoot = "/var/lib/vela/worker/scratch"
	runnerSocketRoot  = "/run/vela-runner"
)

func h3WorkerPodTemplate(
	desired DesiredRevision,
	placement WorkerPlacement,
	selector map[string]string,
) corev1.PodTemplateSpec {
	labels := cloneMap(selector)
	nodeSelector := cloneMap(desired.NodeSelector)
	nodeSelector[corev1.LabelHostname] = placement.NodeIdentity
	labels["app.kubernetes.io/part-of"] = "vela"
	labels["vela.ai/worker-profile"] = "h3"
	labels[protectedLabel] = "true"
	labels[workerPoolLabel] = desired.WorkerPoolID.String()
	labels[fleetRevisionLabel] = desired.Revision
	automount := false
	shareProcessNamespace := false
	runAsNonRoot := true
	runAsUser := int64(10001)
	runAsGroup := int64(10001)
	fsGroup := int64(10001)
	enableServiceLinks := true
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
			Annotations: map[string]string{
				"vela.ai/fleet-controller-owned": "true",
			},
			Finalizers: []string{protectionFinalizer},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName:            "vela-worker",
			AutomountServiceAccountToken:  &automount,
			ShareProcessNamespace:         &shareProcessNamespace,
			TerminationGracePeriodSeconds: int64Pointer(120),
			RestartPolicy:                 corev1.RestartPolicyAlways,
			DNSPolicy:                     corev1.DNSClusterFirst,
			SchedulerName:                 corev1.DefaultSchedulerName,
			EnableServiceLinks:            &enableServiceLinks,
			NodeSelector:                  nodeSelector,
			SchedulingGates: []corev1.PodSchedulingGate{{
				Name: IdentityBindingSchedulingGate,
			}},
			Tolerations: []corev1.Toleration{{
				Key: "vela.ai/h3", Operator: corev1.TolerationOpEqual,
				Value: "true", Effect: corev1.TaintEffectNoSchedule,
			}},
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot:        &runAsNonRoot,
				RunAsUser:           &runAsUser,
				RunAsGroup:          &runAsGroup,
				FSGroup:             &fsGroup,
				FSGroupChangePolicy: valuePointer(corev1.FSGroupChangeOnRootMismatch),
				SeccompProfile:      &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
			},
			InitContainers: []corev1.Container{h3SocketInitializer(desired.InitImage)},
			Containers: []corev1.Container{
				h3WorkerAgentContainer(desired, placement),
				h3RunnerContainer(desired, placement),
			},
			Volumes: h3WorkerVolumes(desired, placement),
		},
	}
}

func h3SocketInitializer(image string) corev1.Container {
	runAsNonRoot := false
	runAsUser := int64(0)
	runAsGroup := int64(0)
	allowPrivilegeEscalation := false
	readOnlyRootFilesystem := true
	return withKubernetesContainerDefaults(corev1.Container{
		Name:            "runner-socket-permissions",
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command: []string{"/bin/sh", "-ec", `mkdir -p /run/vela-runner/private
mkdir -p /var/lib/vela/worker/scratch/runner-state /var/lib/vela/worker/scratch/outputs
chmod 0700 /run/vela-runner /run/vela-runner/private
chmod 0700 /var/lib/vela/worker/scratch /var/lib/vela/worker/scratch/runner-state /var/lib/vela/worker/scratch/outputs
chown 10001:10001 /run/vela-runner /run/vela-runner/private
chown -R 10001:10001 /var/lib/vela/worker/scratch
`},
		Resources: corev1.ResourceRequirements{
			Requests: resourceList(map[corev1.ResourceName]string{
				corev1.ResourceCPU: "10m", corev1.ResourceMemory: "8Mi",
			}),
			Limits: resourceList(map[corev1.ResourceName]string{
				corev1.ResourceCPU: "100m", corev1.ResourceMemory: "16Mi",
			}),
		},
		SecurityContext: &corev1.SecurityContext{
			RunAsNonRoot:             &runAsNonRoot,
			RunAsUser:                &runAsUser,
			RunAsGroup:               &runAsGroup,
			AllowPrivilegeEscalation: &allowPrivilegeEscalation,
			ReadOnlyRootFilesystem:   &readOnlyRootFilesystem,
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"}, Add: []corev1.Capability{"CHOWN"},
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "runner-socket", MountPath: runnerSocketRoot},
			{Name: "runner-scratch-view", MountPath: workerScratchRoot},
		},
	})
}

func h3WorkerAgentContainer(desired DesiredRevision, placement WorkerPlacement) corev1.Container {
	return withKubernetesContainerDefaults(corev1.Container{
		Name:            "worker-agent",
		Image:           desired.WorkerAgentImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Env: []corev1.EnvVar{
			fieldEnvironment("VELA_WORKER_ID", "metadata.labels['vela.ai/worker-id']"),
			fieldEnvironment("VELA_WORKER_EPOCH", "metadata.labels['vela.ai/worker-epoch']"),
			fieldEnvironment("VELA_WORKER_NODE_IDENTITY", "spec.nodeName"),
			configMapEnvironment("VELA_WORKER_CONTROL_ADDRESS", placement.WorkerRuntimeConfigMap, "control-address"),
			configMapEnvironment("VELA_WORKER_CONTROL_SERVER_NAME", placement.WorkerRuntimeConfigMap, "control-server-name"),
			literalEnvironment("VELA_WORKER_TLS_CERT_FILE", "/etc/vela-worker-tls/tls.crt"),
			literalEnvironment("VELA_WORKER_TLS_KEY_FILE", "/etc/vela-worker-tls/tls.key"),
			literalEnvironment("VELA_WORKER_CONTROL_CA_FILE", "/etc/vela-worker-tls/ca.crt"),
			literalEnvironment("VELA_WORKER_RUNNER_SOCKET", "/run/vela-runner/private/runner.sock"),
			literalEnvironment("VELA_WORKER_RUNNER_EXPECTED_UID", "10001"),
			literalEnvironment("VELA_WORKER_SCRATCH_ROOT", workerScratchRoot),
			literalEnvironment("VELA_WORKER_RECOVERY_ROOT", workerScratchRoot+"/recovery"),
			literalEnvironment("VELA_WORKER_OUTPUT_ROOT", workerScratchRoot+"/outputs"),
			literalEnvironment("VELA_WORKER_OUTPUT_OWNER_UID", "10001"),
			configMapEnvironment("VELA_WORKER_OUTPUT_CLEANUP_MIN_BYTES_PER_SECOND", placement.WorkerRuntimeConfigMap, "output-cleanup-min-bytes-per-second"),
			literalEnvironment("VELA_WORKER_INFERENCE_BACKEND_REVISION", desired.InferenceBackendRevision),
			configMapEnvironment("VELA_WORKER_ARTIFACT_STORE_HEALTH_URL", placement.WorkerRuntimeConfigMap, "artifact-store-health-url"),
			literalEnvironment("VELA_WORKER_ARTIFACT_STORE_CA_FILE", "/etc/vela-artifact-store-tls/ca.crt"),
			literalEnvironment("VELA_WORKER_ARTIFACT_STORE_PROBE_TIMEOUT", "2s"),
			literalEnvironment(
				"VELA_WORKER_CAPACITY_REPORT_INTERVAL",
				(desired.CapacityPolicy.ObservationMaxAge / 3).String(),
			),
			literalEnvironment("VELA_WORKER_SCRATCH_PROBE", "xfs-project-quota"),
			literalEnvironment("VELA_WORKER_HOST_QUOTA_SOCKET", "/run/vela-node-agent/worker-quota.sock"),
			literalEnvironment("VELA_WORKER_HOST_QUOTA_SOCKET_UID", "0"),
			literalEnvironment("VELA_WORKER_HOST_QUOTA_SOCKET_GID", "10001"),
			configMapEnvironment("VELA_WORKER_XFS_DEVICE", placement.WorkerRuntimeConfigMap, "xfs-device"),
			configMapEnvironment("VELA_WORKER_XFS_PROJECT_ID", placement.WorkerRuntimeConfigMap, "xfs-project-id"),
			configMapEnvironment("VELA_WORKER_ATTEMPT_QUOTA_BYTES", placement.WorkerRuntimeConfigMap, "attempt-quota-bytes"),
			configMapEnvironment("VELA_WORKER_MAX_ENTRY_BYTES", placement.WorkerRuntimeConfigMap, "max-entry-bytes"),
			configMapEnvironment("VELA_WORKER_MAX_ENTRIES", placement.WorkerRuntimeConfigMap, "max-entries"),
			configMapEnvironment("VELA_WORKER_HIGH_WATERMARK_BYTES", placement.WorkerRuntimeConfigMap, "high-watermark-bytes"),
			configMapEnvironment("VELA_WORKER_LOW_WATERMARK_BYTES", placement.WorkerRuntimeConfigMap, "low-watermark-bytes"),
			configMapEnvironment("VELA_WORKER_CRITICAL_FREE_BYTES", placement.WorkerRuntimeConfigMap, "critical-free-bytes"),
			literalEnvironment("VELA_WORKER_TERMINAL_RETENTION", "24h"),
			literalEnvironment("VELA_WORKER_ALLOW_DEVELOPMENT_HTTP_UPLOADS", "false"),
		},
		Resources: corev1.ResourceRequirements{
			Requests: resourceList(map[corev1.ResourceName]string{
				corev1.ResourceCPU: "1", corev1.ResourceMemory: "1Gi",
			}),
			Limits: resourceList(map[corev1.ResourceName]string{
				corev1.ResourceCPU: "2", corev1.ResourceMemory: "2Gi",
			}),
		},
		SecurityContext: unprivilegedContainerSecurityContext(),
		VolumeMounts: []corev1.VolumeMount{
			{Name: "runner-socket", MountPath: runnerSocketRoot},
			{Name: "worker-quota-socket", MountPath: "/run/vela-node-agent/worker-quota.sock"},
			{Name: "scratch", MountPath: workerScratchRoot},
			{Name: "worker-control-tls", MountPath: "/etc/vela-worker-tls", ReadOnly: true},
			{Name: "artifact-store-tls", MountPath: "/etc/vela-artifact-store-tls", ReadOnly: true},
		},
	})
}

func h3RunnerContainer(desired DesiredRevision, placement WorkerPlacement) corev1.Container {
	return withKubernetesContainerDefaults(corev1.Container{
		Name:            "h3-runner",
		Image:           desired.RunnerImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Env: []corev1.EnvVar{
			literalEnvironment("VELA_RUNNER_SOCKET", "/run/vela-runner/private/runner.sock"),
			literalEnvironment("VELA_RUNNER_SCRATCH_ROOT", workerScratchRoot),
			literalEnvironment("VELA_RUNNER_STATE_ROOT", workerScratchRoot+"/runner-state"),
			literalEnvironment("VELA_RUNNER_OUTPUT_ROOT", workerScratchRoot+"/outputs"),
			literalEnvironment("VELA_RUNNER_BACKEND_REVISION", desired.InferenceBackendRevision),
			literalEnvironment("VELA_RUNNER_BACKEND_COMMAND", "/opt/vela/bin/h3-backend"),
			literalEnvironment("VELA_RUNNER_BACKEND_ARGS_JSON", "[]"),
			literalEnvironment("VELA_RUNNER_PROFILES_FILE", "/etc/vela-runner/profiles/profiles.json"),
			literalEnvironment("VELA_RUNNER_GPU_ROLES_FILE", "/etc/vela-runner/gpu-roles/gpu-roles.json"),
			literalEnvironment("VELA_RUNNER_STOP_TIMEOUT", "15"),
			configMapEnvironment("VELA_RUNNER_MAX_OUTPUT_BYTES", placement.WorkerRuntimeConfigMap, "attempt-quota-bytes"),
		},
		Resources: corev1.ResourceRequirements{
			Requests: resourceList(map[corev1.ResourceName]string{
				corev1.ResourceCPU: "16", corev1.ResourceMemory: "128Gi", "nvidia.com/gpu": "8",
			}),
			Limits: resourceList(map[corev1.ResourceName]string{
				corev1.ResourceCPU: "32", corev1.ResourceMemory: "256Gi", "nvidia.com/gpu": "8",
			}),
		},
		SecurityContext: unprivilegedContainerSecurityContext(),
		VolumeMounts: []corev1.VolumeMount{
			{Name: "runner-socket", MountPath: runnerSocketRoot},
			{Name: "runner-scratch-view", MountPath: workerScratchRoot},
			{Name: "scratch", MountPath: workerScratchRoot + "/runner-state", SubPath: "runner-state"},
			{Name: "scratch", MountPath: workerScratchRoot + "/outputs", SubPath: "outputs"},
			{Name: "model-weights", MountPath: "/var/lib/vela/models", ReadOnly: true},
			{Name: "runner-profiles", MountPath: "/etc/vela-runner/profiles", ReadOnly: true},
			{Name: "runner-gpu-roles", MountPath: "/etc/vela-runner/gpu-roles", ReadOnly: true},
		},
	})
}

func withKubernetesContainerDefaults(container corev1.Container) corev1.Container {
	container.TerminationMessagePath = corev1.TerminationMessagePathDefault
	container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	return container
}

func h3WorkerVolumes(desired DesiredRevision, placement WorkerPlacement) []corev1.Volume {
	mode0400 := int32(0o400)
	mode0440 := int32(0o440)
	return []corev1.Volume{
		{Name: "runner-socket", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
			Medium: corev1.StorageMediumMemory, SizeLimit: quantityPointer("16Mi"),
		}}},
		{Name: "runner-scratch-view", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
			Medium: corev1.StorageMediumMemory, SizeLimit: quantityPointer("1Mi"),
		}}},
		{Name: "worker-quota-socket", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
			Path: "/run/vela-node-agent/worker-quota.sock", Type: valuePointer(corev1.HostPathSocket),
		}}},
		{Name: "scratch", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
			Path: workerScratchRoot, Type: valuePointer(corev1.HostPathDirectory),
		}}},
		{Name: "model-weights", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{
			Path: "/var/lib/vela/models", Type: valuePointer(corev1.HostPathDirectory),
		}}},
		{Name: "worker-control-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: placement.WorkerControlTLSSecret, DefaultMode: &mode0400,
		}}},
		{Name: "artifact-store-tls", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
			SecretName: desired.ArtifactStoreTLSSecret, DefaultMode: &mode0440,
			Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}},
		}}},
		{Name: "runner-profiles", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: placement.RunnerProfilesConfigMap},
			DefaultMode:          &mode0440, Items: []corev1.KeyToPath{{Key: "profiles.json", Path: "profiles.json"}},
		}}},
		{Name: "runner-gpu-roles", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: placement.RunnerGPURolesConfigMap},
			DefaultMode:          &mode0440, Items: []corev1.KeyToPath{{Key: "gpu-roles.json", Path: "gpu-roles.json"}},
		}}},
	}
}

func literalEnvironment(name, value string) corev1.EnvVar {
	return corev1.EnvVar{Name: name, Value: value}
}

func fieldEnvironment(name, fieldPath string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
			APIVersion: "v1", FieldPath: fieldPath,
		}},
	}
}

func configMapEnvironment(name, configMapName, key string) corev1.EnvVar {
	return corev1.EnvVar{
		Name: name,
		ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: configMapName}, Key: key,
		}},
	}
}

func unprivilegedContainerSecurityContext() *corev1.SecurityContext {
	allowPrivilegeEscalation := false
	readOnlyRootFilesystem := true
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: &allowPrivilegeEscalation,
		ReadOnlyRootFilesystem:   &readOnlyRootFilesystem,
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

func resourceList(values map[corev1.ResourceName]string) corev1.ResourceList {
	result := make(corev1.ResourceList, len(values))
	for name, value := range values {
		result[name] = resource.MustParse(value)
	}
	return result
}

func quantityPointer(value string) *resource.Quantity {
	quantity := resource.MustParse(value)
	return &quantity
}

func int64Pointer(value int64) *int64 { return &value }

func valuePointer[T any](value T) *T { return &value }
