package fleetcontroller

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func withKubernetesContainerDefaults(container corev1.Container) corev1.Container {
	container.TerminationMessagePath = corev1.TerminationMessagePathDefault
	container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	return container
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
