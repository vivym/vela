package h3launchevidence

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/vivym/vela/internal/fleetcontroller"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	resourceclient "k8s.io/client-go/kubernetes/typed/resource/v1"
)

// ExternalResourceReader is the minimum read-only API needed to resolve the
// release-bound ConfigMaps and Secrets shared by preflight and launch capture.
type ExternalResourceReader interface {
	ConfigMap(context.Context, string, string) (corev1.ConfigMap, error)
	Secret(context.Context, string, string) (corev1.Secret, error)
}

// KubernetesReader is the minimum read-only API needed for launch evidence.
type KubernetesReader interface {
	ExternalResourceReader
	NamespaceUID(context.Context, string) (string, error)
	Pod(context.Context, string, string) (corev1.Pod, error)
	ClaimTemplate(context.Context, string, string) (resourcev1.ResourceClaimTemplate, error)
	Claim(context.Context, string, string) (resourcev1.ResourceClaim, error)
	Node(context.Context, string) (corev1.Node, error)
	ResourceSlices(context.Context, string) ([]resourcev1.ResourceSlice, error)
}

// ExternalResourceSnapshot contains the complete live external-resource set.
// Callers never receive a partial snapshot after a read failure.
type ExternalResourceSnapshot struct {
	ConfigMaps []corev1.ConfigMap
	Secrets    []corev1.Secret
}

// ClientsetExternalResourceReader adapts the Kubernetes Core client to the
// narrow external-resource evidence boundary.
type ClientsetExternalResourceReader struct {
	core coreclient.CoreV1Interface
}

func NewClientsetExternalResourceReader(
	core coreclient.CoreV1Interface,
) (*ClientsetExternalResourceReader, error) {
	if core == nil {
		return nil, errors.New("kubernetes core client is required")
	}
	return &ClientsetExternalResourceReader{core: core}, nil
}

func (reader *ClientsetExternalResourceReader) ConfigMap(
	ctx context.Context,
	namespace,
	name string,
) (corev1.ConfigMap, error) {
	value, err := reader.core.ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return corev1.ConfigMap{}, err
	}
	return *value, nil
}

func (reader *ClientsetExternalResourceReader) Secret(
	ctx context.Context,
	namespace,
	name string,
) (corev1.Secret, error) {
	value, err := reader.core.Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return corev1.Secret{}, err
	}
	return *value, nil
}

type ClientsetKubernetesReader struct {
	*ClientsetExternalResourceReader
	resource resourceclient.ResourceV1Interface
}

func NewClientsetKubernetesReader(
	core coreclient.CoreV1Interface,
	resource resourceclient.ResourceV1Interface,
) (*ClientsetKubernetesReader, error) {
	if core == nil || resource == nil {
		return nil, errors.New("kubernetes core and resource clients are required")
	}
	external, err := NewClientsetExternalResourceReader(core)
	if err != nil {
		return nil, err
	}
	return &ClientsetKubernetesReader{
		ClientsetExternalResourceReader: external,
		resource:                        resource,
	}, nil
}

func (reader *ClientsetKubernetesReader) NamespaceUID(ctx context.Context, name string) (string, error) {
	namespace, err := reader.core.Namespaces().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	return string(namespace.UID), nil
}

func (reader *ClientsetKubernetesReader) Pod(
	ctx context.Context,
	namespace,
	name string,
) (corev1.Pod, error) {
	value, err := reader.core.Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return corev1.Pod{}, err
	}
	return *value, nil
}

func (reader *ClientsetKubernetesReader) ClaimTemplate(
	ctx context.Context,
	namespace,
	name string,
) (resourcev1.ResourceClaimTemplate, error) {
	value, err := reader.resource.ResourceClaimTemplates(namespace).
		Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return resourcev1.ResourceClaimTemplate{}, err
	}
	return *value, nil
}

func (reader *ClientsetKubernetesReader) Claim(
	ctx context.Context,
	namespace,
	name string,
) (resourcev1.ResourceClaim, error) {
	value, err := reader.resource.ResourceClaims(namespace).
		Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return resourcev1.ResourceClaim{}, err
	}
	return *value, nil
}

func (reader *ClientsetKubernetesReader) Node(ctx context.Context, name string) (corev1.Node, error) {
	value, err := reader.core.Nodes().Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return corev1.Node{}, err
	}
	return *value, nil
}

func (reader *ClientsetKubernetesReader) ResourceSlices(
	ctx context.Context,
	driver string,
) ([]resourcev1.ResourceSlice, error) {
	selector := fields.OneTermEqualSelector(resourcev1.ResourceSliceSelectorDriver, driver).String()
	values, err := reader.resource.ResourceSlices().List(
		ctx,
		metav1.ListOptions{FieldSelector: selector},
	)
	if err != nil {
		return nil, err
	}
	return values.Items, nil
}

// CollectExternalResources reads the complete canonical external-resource set.
// A missing or invalid declaration returns an error and an empty snapshot.
func CollectExternalResources(
	ctx context.Context,
	reader ExternalResourceReader,
	expectations []ExternalResourceExpectation,
) (ExternalResourceSnapshot, error) {
	if ctx == nil || reader == nil {
		return ExternalResourceSnapshot{}, errors.New("external resource reader and context are required")
	}
	snapshot := ExternalResourceSnapshot{
		ConfigMaps: make([]corev1.ConfigMap, 0),
		Secrets:    make([]corev1.Secret, 0),
	}
	for _, expected := range expectations {
		if err := validateExternalResourceExpectation(expected); err != nil {
			return ExternalResourceSnapshot{}, err
		}
		switch expected.Kind {
		case "ConfigMap":
			value, err := reader.ConfigMap(ctx, expected.Namespace, expected.Name)
			if err != nil {
				return ExternalResourceSnapshot{}, fmt.Errorf(
					"read release-bound ConfigMap %s/%s: %w", expected.Namespace, expected.Name, err,
				)
			}
			snapshot.ConfigMaps = append(snapshot.ConfigMaps, value)
		case "Secret":
			value, err := reader.Secret(ctx, expected.Namespace, expected.Name)
			if err != nil {
				return ExternalResourceSnapshot{}, fmt.Errorf(
					"read release-bound Secret %s/%s: %w", expected.Namespace, expected.Name, err,
				)
			}
			snapshot.Secrets = append(snapshot.Secrets, value)
		}
	}
	sort.Slice(snapshot.ConfigMaps, func(left, right int) bool {
		if snapshot.ConfigMaps[left].Namespace != snapshot.ConfigMaps[right].Namespace {
			return snapshot.ConfigMaps[left].Namespace < snapshot.ConfigMaps[right].Namespace
		}
		return snapshot.ConfigMaps[left].Name < snapshot.ConfigMaps[right].Name
	})
	sort.Slice(snapshot.Secrets, func(left, right int) bool {
		if snapshot.Secrets[left].Namespace != snapshot.Secrets[right].Namespace {
			return snapshot.Secrets[left].Namespace < snapshot.Secrets[right].Namespace
		}
		return snapshot.Secrets[left].Name < snapshot.Secrets[right].Name
	})
	return snapshot, nil
}

// CollectKubernetes resolves all desired object names from the approved
// rollout and reads their live identities from the Kubernetes API.
func CollectKubernetes(
	ctx context.Context,
	reader KubernetesReader,
	rollout fleetcontroller.ResidencyPlanRollout,
	externalResources []ExternalResourceExpectation,
) (KubernetesSnapshot, error) {
	if ctx == nil || reader == nil {
		return KubernetesSnapshot{}, errors.New("kubernetes evidence reader and context are required")
	}
	desired, err := desiredMembers(rollout)
	if err != nil {
		return KubernetesSnapshot{}, fmt.Errorf("resolve approved launch resources: %w", err)
	}
	if len(desired) == 0 {
		return KubernetesSnapshot{}, errors.New("approved rollout has no WorkerMembers")
	}
	namespace := desired[0].bundle.Namespace
	for _, expected := range desired[1:] {
		if expected.bundle.Namespace != namespace {
			return KubernetesSnapshot{}, errors.New("one launch evidence capture cannot span Kubernetes namespaces")
		}
	}
	clusterUID, err := reader.NamespaceUID(ctx, "kube-system")
	if err != nil {
		return KubernetesSnapshot{}, fmt.Errorf("read Kubernetes cluster identity: %w", err)
	}
	namespaceUID, err := reader.NamespaceUID(ctx, namespace)
	if err != nil {
		return KubernetesSnapshot{}, fmt.Errorf("read Fleet namespace identity: %w", err)
	}
	resourceSlices, err := reader.ResourceSlices(ctx, "gpu.nvidia.com")
	if err != nil {
		return KubernetesSnapshot{}, fmt.Errorf("list NVIDIA DRA ResourceSlices: %w", err)
	}
	snapshot := KubernetesSnapshot{
		ClusterUID: clusterUID, NamespaceUID: namespaceUID,
		Pods:           make([]corev1.Pod, 0, len(desired)),
		ClaimTemplates: make([]resourcev1.ResourceClaimTemplate, 0, len(desired)),
		Claims:         make([]resourcev1.ResourceClaim, 0, len(desired)),
		Nodes:          make([]corev1.Node, 0), ResourceSlices: resourceSlices,
	}
	externalSnapshot, err := CollectExternalResources(ctx, reader, externalResources)
	if err != nil {
		return KubernetesSnapshot{}, err
	}
	snapshot.ConfigMaps = externalSnapshot.ConfigMaps
	snapshot.Secrets = externalSnapshot.Secrets
	seenNodes := make(map[string]struct{})
	for _, expected := range desired {
		pod, err := reader.Pod(ctx, namespace, expected.pod.Name)
		if err != nil {
			return KubernetesSnapshot{}, fmt.Errorf("read WorkerMember Pod %s: %w", expected.pod.Name, err)
		}
		template, err := reader.ClaimTemplate(ctx, namespace, expected.template.Name)
		if err != nil {
			return KubernetesSnapshot{}, fmt.Errorf(
				"read WorkerMember ResourceClaimTemplate %s: %w",
				expected.template.Name,
				err,
			)
		}
		claimName, err := podClaimName(pod)
		if err != nil {
			return KubernetesSnapshot{}, fmt.Errorf("resolve Pod %s generated ResourceClaim: %w", pod.Name, err)
		}
		claim, err := reader.Claim(ctx, namespace, claimName)
		if err != nil {
			return KubernetesSnapshot{}, fmt.Errorf("read generated ResourceClaim %s: %w", claimName, err)
		}
		if pod.Spec.NodeName == "" {
			return KubernetesSnapshot{}, fmt.Errorf("Pod %s has no scheduled node", pod.Name)
		}
		if _, exists := seenNodes[pod.Spec.NodeName]; !exists {
			node, err := reader.Node(ctx, pod.Spec.NodeName)
			if err != nil {
				return KubernetesSnapshot{}, fmt.Errorf("read scheduled Node %s: %w", pod.Spec.NodeName, err)
			}
			snapshot.Nodes = append(snapshot.Nodes, node)
			seenNodes[pod.Spec.NodeName] = struct{}{}
		}
		snapshot.Pods = append(snapshot.Pods, pod)
		snapshot.ClaimTemplates = append(snapshot.ClaimTemplates, template)
		snapshot.Claims = append(snapshot.Claims, claim)
	}
	sort.Slice(snapshot.Pods, func(left, right int) bool { return snapshot.Pods[left].Name < snapshot.Pods[right].Name })
	sort.Slice(snapshot.ClaimTemplates, func(left, right int) bool {
		return snapshot.ClaimTemplates[left].Name < snapshot.ClaimTemplates[right].Name
	})
	sort.Slice(snapshot.Claims, func(left, right int) bool { return snapshot.Claims[left].Name < snapshot.Claims[right].Name })
	sort.Slice(snapshot.Nodes, func(left, right int) bool { return snapshot.Nodes[left].Name < snapshot.Nodes[right].Name })
	sort.Slice(snapshot.ResourceSlices, func(left, right int) bool {
		return snapshot.ResourceSlices[left].Name < snapshot.ResourceSlices[right].Name
	})
	return snapshot, nil
}
