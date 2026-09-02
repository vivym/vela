package h3launchevidence_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/vivym/vela/internal/h3launchevidence"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
)

func TestCollectKubernetesReadsExactLiveObjects(t *testing.T) {
	input := exactLaunchInput(t)
	reader := &fakeKubernetesReader{
		namespaceUIDs: map[string]string{
			"kube-system": input.Kubernetes.ClusterUID,
			"vela-system": input.Kubernetes.NamespaceUID,
		},
		configMaps:     make(map[string]corev1.ConfigMap),
		secrets:        make(map[string]corev1.Secret),
		pods:           make(map[string]corev1.Pod),
		templates:      make(map[string]resourcev1.ResourceClaimTemplate),
		claims:         make(map[string]resourcev1.ResourceClaim),
		nodes:          make(map[string]corev1.Node),
		resourceSlices: input.Kubernetes.ResourceSlices,
	}
	for index := range input.Kubernetes.ConfigMaps {
		value := input.Kubernetes.ConfigMaps[index]
		reader.configMaps[namespacedKey(value.Namespace, value.Name)] = value
	}
	for index := range input.Kubernetes.Secrets {
		value := input.Kubernetes.Secrets[index]
		reader.secrets[namespacedKey(value.Namespace, value.Name)] = value
	}
	for index := range input.Kubernetes.Pods {
		pod := input.Kubernetes.Pods[index]
		reader.pods[namespacedKey(pod.Namespace, pod.Name)] = pod
	}
	for index := range input.Kubernetes.ClaimTemplates {
		template := input.Kubernetes.ClaimTemplates[index]
		reader.templates[namespacedKey(template.Namespace, template.Name)] = template
	}
	for index := range input.Kubernetes.Claims {
		claim := input.Kubernetes.Claims[index]
		reader.claims[namespacedKey(claim.Namespace, claim.Name)] = claim
	}
	for index := range input.Kubernetes.Nodes {
		node := input.Kubernetes.Nodes[index]
		reader.nodes[node.Name] = node
	}

	snapshot, err := h3launchevidence.CollectKubernetes(
		context.Background(),
		reader,
		input.Rollout,
		input.ExternalResources,
	)
	if err != nil {
		t.Fatalf("CollectKubernetes: %v", err)
	}
	if snapshot.ClusterUID != input.Kubernetes.ClusterUID ||
		snapshot.NamespaceUID != input.Kubernetes.NamespaceUID ||
		len(snapshot.ConfigMaps) != 1 || len(snapshot.Secrets) != 1 ||
		len(snapshot.Pods) != 1 || len(snapshot.ClaimTemplates) != 1 ||
		len(snapshot.Claims) != 1 || len(snapshot.Nodes) != 1 || len(snapshot.ResourceSlices) != 1 {
		t.Fatalf("Kubernetes snapshot = %#v", snapshot)
	}
}

type fakeKubernetesReader struct {
	namespaceUIDs  map[string]string
	configMaps     map[string]corev1.ConfigMap
	secrets        map[string]corev1.Secret
	pods           map[string]corev1.Pod
	templates      map[string]resourcev1.ResourceClaimTemplate
	claims         map[string]resourcev1.ResourceClaim
	nodes          map[string]corev1.Node
	resourceSlices []resourcev1.ResourceSlice
}

func (reader *fakeKubernetesReader) ConfigMap(
	_ context.Context,
	namespace,
	name string,
) (corev1.ConfigMap, error) {
	value, exists := reader.configMaps[namespacedKey(namespace, name)]
	if !exists {
		return corev1.ConfigMap{}, fmt.Errorf("ConfigMap %s/%s not found", namespace, name)
	}
	return *value.DeepCopy(), nil
}

func (reader *fakeKubernetesReader) Secret(
	_ context.Context,
	namespace,
	name string,
) (corev1.Secret, error) {
	value, exists := reader.secrets[namespacedKey(namespace, name)]
	if !exists {
		return corev1.Secret{}, fmt.Errorf("Secret %s/%s not found", namespace, name)
	}
	return *value.DeepCopy(), nil
}

func (reader *fakeKubernetesReader) NamespaceUID(_ context.Context, name string) (string, error) {
	uid := reader.namespaceUIDs[name]
	if uid == "" {
		return "", fmt.Errorf("namespace %s not found", name)
	}
	return uid, nil
}

func (reader *fakeKubernetesReader) Pod(_ context.Context, namespace, name string) (corev1.Pod, error) {
	value, exists := reader.pods[namespacedKey(namespace, name)]
	if !exists {
		return corev1.Pod{}, fmt.Errorf("Pod %s/%s not found", namespace, name)
	}
	return *value.DeepCopy(), nil
}

func (reader *fakeKubernetesReader) ClaimTemplate(
	_ context.Context,
	namespace,
	name string,
) (resourcev1.ResourceClaimTemplate, error) {
	value, exists := reader.templates[namespacedKey(namespace, name)]
	if !exists {
		return resourcev1.ResourceClaimTemplate{}, fmt.Errorf("ResourceClaimTemplate %s/%s not found", namespace, name)
	}
	return *value.DeepCopy(), nil
}

func (reader *fakeKubernetesReader) Claim(
	_ context.Context,
	namespace,
	name string,
) (resourcev1.ResourceClaim, error) {
	value, exists := reader.claims[namespacedKey(namespace, name)]
	if !exists {
		return resourcev1.ResourceClaim{}, fmt.Errorf("ResourceClaim %s/%s not found", namespace, name)
	}
	return *value.DeepCopy(), nil
}

func (reader *fakeKubernetesReader) Node(_ context.Context, name string) (corev1.Node, error) {
	value, exists := reader.nodes[name]
	if !exists {
		return corev1.Node{}, fmt.Errorf("node %s not found", name)
	}
	return *value.DeepCopy(), nil
}

func (reader *fakeKubernetesReader) ResourceSlices(
	_ context.Context,
	driver string,
) ([]resourcev1.ResourceSlice, error) {
	if driver != "gpu.nvidia.com" {
		return nil, fmt.Errorf("unexpected driver %s", driver)
	}
	result := make([]resourcev1.ResourceSlice, len(reader.resourceSlices))
	for index := range reader.resourceSlices {
		result[index] = *reader.resourceSlices[index].DeepCopy()
	}
	return result, nil
}

func namespacedKey(namespace, name string) string { return namespace + "/" + name }

var _ h3launchevidence.KubernetesReader = (*fakeKubernetesReader)(nil)
