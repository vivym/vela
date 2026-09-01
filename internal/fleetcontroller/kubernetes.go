package fleetcontroller

import (
	"context"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

var (
	podResource                   = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	resourceClaimTemplateResource = schema.GroupVersionResource{
		Group: "resource.k8s.io", Version: "v1", Resource: "resourceclaimtemplates",
	}
)

type ResourceKey struct {
	Namespace string
	Name      string
}

type KubernetesResources struct {
	client    dynamic.Interface
	namespace string
}

func NewKubernetesResources(
	client dynamic.Interface,
	namespace string,
) (*KubernetesResources, error) {
	if client == nil {
		return nil, errors.New("kubernetes dynamic client is required")
	}
	if !validResourceName(namespace) {
		return nil, errors.New("fleet Kubernetes namespace is invalid")
	}
	return &KubernetesResources{client: client, namespace: namespace}, nil
}

func (resources *KubernetesResources) GetWorkerInstancePod(
	ctx context.Context,
	key ResourceKey,
) (corev1.Pod, error) {
	if err := resources.validateKey(key); err != nil {
		return corev1.Pod{}, err
	}
	object, err := resources.client.Resource(podResource).
		Namespace(resources.namespace).
		Get(ctx, key.Name, metav1.GetOptions{})
	if err != nil {
		return corev1.Pod{}, mapKubernetesResourceError(err)
	}
	var pod corev1.Pod
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &pod); err != nil {
		return corev1.Pod{}, fmt.Errorf("decode WorkerInstance Pod: %w", err)
	}
	return pod, nil
}

func (resources *KubernetesResources) CreateWorkerInstancePod(
	ctx context.Context,
	pod corev1.Pod,
) error {
	if resources == nil || resources.client == nil || pod.APIVersion != "v1" ||
		pod.Kind != "Pod" || pod.Namespace != resources.namespace ||
		!validResourceName(pod.Name) || pod.Labels[protectedLabel] != "true" ||
		pod.Labels[workerInstanceIDLabel] == "" ||
		!containsString(pod.Finalizers, protectionFinalizer) {
		return errors.New("WorkerInstance Pod is invalid")
	}
	encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pod)
	if err != nil {
		return fmt.Errorf("encode WorkerInstance Pod: %w", err)
	}
	_, err = resources.client.Resource(podResource).
		Namespace(resources.namespace).
		Create(ctx, &unstructured.Unstructured{Object: encoded}, metav1.CreateOptions{})
	return mapKubernetesWriteError(err)
}

func (resources *KubernetesResources) GetWorkerInstanceGPUClaimTemplate(
	ctx context.Context,
	key ResourceKey,
) (resourcev1.ResourceClaimTemplate, error) {
	if err := resources.validateKey(key); err != nil {
		return resourcev1.ResourceClaimTemplate{}, err
	}
	object, err := resources.client.Resource(resourceClaimTemplateResource).
		Namespace(resources.namespace).
		Get(ctx, key.Name, metav1.GetOptions{})
	if err != nil {
		return resourcev1.ResourceClaimTemplate{}, mapKubernetesResourceError(err)
	}
	var claim resourcev1.ResourceClaimTemplate
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &claim); err != nil {
		return resourcev1.ResourceClaimTemplate{}, fmt.Errorf(
			"decode WorkerInstance GPU ResourceClaimTemplate: %w", err,
		)
	}
	return claim, nil
}

func (resources *KubernetesResources) ListWorkerInstanceGPUClaimTemplates(
	ctx context.Context,
) ([]resourcev1.ResourceClaimTemplate, error) {
	if resources == nil || resources.client == nil {
		return nil, errors.New("fleet Kubernetes resources are not configured")
	}
	objects, err := resources.client.Resource(resourceClaimTemplateResource).
		Namespace(resources.namespace).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, mapKubernetesResourceError(err)
	}
	claims := make([]resourcev1.ResourceClaimTemplate, 0, len(objects.Items))
	for index := range objects.Items {
		if objects.Items[index].GetAnnotations()["vela.ai/fleet-controller-owned"] != "true" {
			continue
		}
		var claim resourcev1.ResourceClaimTemplate
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(objects.Items[index].Object, &claim); err != nil {
			return nil, fmt.Errorf("decode WorkerInstance GPU ResourceClaimTemplate: %w", err)
		}
		claims = append(claims, claim)
	}
	sort.Slice(claims, func(left, right int) bool { return claims[left].Name < claims[right].Name })
	return claims, nil
}

func (resources *KubernetesResources) CreateWorkerInstanceGPUClaimTemplate(
	ctx context.Context,
	claim resourcev1.ResourceClaimTemplate,
) error {
	if resources == nil || resources.client == nil ||
		claim.APIVersion != resourcev1.SchemeGroupVersion.String() ||
		claim.Kind != "ResourceClaimTemplate" || claim.Namespace != resources.namespace ||
		!validResourceName(claim.Name) || claim.Labels[workerInstanceIDLabel] == "" ||
		claim.Labels[workerMemberIDLabel] == "" ||
		len(claim.Spec.Spec.Devices.Requests) == 0 {
		return errors.New("WorkerInstance GPU ResourceClaimTemplate is invalid")
	}
	encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&claim)
	if err != nil {
		return fmt.Errorf("encode WorkerInstance GPU ResourceClaimTemplate: %w", err)
	}
	_, err = resources.client.Resource(resourceClaimTemplateResource).
		Namespace(resources.namespace).
		Create(ctx, &unstructured.Unstructured{Object: encoded}, metav1.CreateOptions{})
	return mapKubernetesWriteError(err)
}

func (resources *KubernetesResources) validateKey(key ResourceKey) error {
	if resources == nil || resources.client == nil || key.Namespace != resources.namespace ||
		!validResourceName(key.Name) {
		return errors.New("fleet Kubernetes resource key is invalid")
	}
	return nil
}

func mapKubernetesResourceError(err error) error {
	if apierrors.IsNotFound(err) {
		return ErrResourceNotFound
	}
	return err
}

func mapKubernetesWriteError(err error) error {
	if err == nil {
		return nil
	}
	return mapKubernetesResourceError(err)
}
