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
		pods:           make(map[string]corev1.Pod),
		templates:      make(map[string]resourcev1.ResourceClaimTemplate),
		claims:         make(map[string]resourcev1.ResourceClaim),
		nodes:          make(map[string]corev1.Node),
		resourceSlices: input.Kubernetes.ResourceSlices,
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
	)
	if err != nil {
		t.Fatalf("CollectKubernetes: %v", err)
	}
	if snapshot.ClusterUID != input.Kubernetes.ClusterUID ||
		snapshot.NamespaceUID != input.Kubernetes.NamespaceUID ||
		len(snapshot.Pods) != 1 || len(snapshot.ClaimTemplates) != 1 ||
		len(snapshot.Claims) != 1 || len(snapshot.Nodes) != 1 || len(snapshot.ResourceSlices) != 1 {
		t.Fatalf("Kubernetes snapshot = %#v", snapshot)
	}
}

type fakeKubernetesReader struct {
	namespaceUIDs  map[string]string
	pods           map[string]corev1.Pod
	templates      map[string]resourcev1.ResourceClaimTemplate
	claims         map[string]resourcev1.ResourceClaim
	nodes          map[string]corev1.Node
	resourceSlices []resourcev1.ResourceSlice
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
		return corev1.Node{}, fmt.Errorf("Node %s not found", name)
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
