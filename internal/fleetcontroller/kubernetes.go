package fleetcontroller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontract"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"
)

var (
	workerPoolResource = schema.GroupVersionResource{
		Group: "fleet.vela.ai", Version: "v1alpha1", Resource: "workerpools",
	}
	daemonSetResource = schema.GroupVersionResource{
		Group: "apps", Version: "v1", Resource: "daemonsets",
	}
	podResource                   = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
	resourceClaimTemplateResource = schema.GroupVersionResource{
		Group: "resource.k8s.io", Version: "v1", Resource: "resourceclaimtemplates",
	}
)

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

func (resources *KubernetesResources) GetWorkerPool(
	ctx context.Context,
	key ResourceKey,
) (WorkerPool, error) {
	if err := resources.validateKey(key); err != nil {
		return WorkerPool{}, err
	}
	object, err := resources.client.Resource(workerPoolResource).
		Namespace(resources.namespace).
		Get(ctx, key.Name, metav1.GetOptions{})
	if err != nil {
		return WorkerPool{}, mapKubernetesResourceError(err)
	}
	return workerPoolFromUnstructured(object)
}

func (resources *KubernetesResources) CreateWorkerPool(
	ctx context.Context,
	workerPool WorkerPool,
) error {
	if err := resources.validateMetadata(workerPool.Metadata); err != nil {
		return err
	}
	object, err := workerPoolToUnstructured(workerPool)
	if err != nil {
		return err
	}
	_, err = resources.client.Resource(workerPoolResource).
		Namespace(resources.namespace).
		Create(ctx, object, metav1.CreateOptions{})
	return mapKubernetesWriteError(err)
}

func (resources *KubernetesResources) GetDaemonSet(
	ctx context.Context,
	key ResourceKey,
) (DaemonSet, error) {
	if err := resources.validateKey(key); err != nil {
		return DaemonSet{}, err
	}
	object, err := resources.client.Resource(daemonSetResource).
		Namespace(resources.namespace).
		Get(ctx, key.Name, metav1.GetOptions{})
	if err != nil {
		return DaemonSet{}, mapKubernetesResourceError(err)
	}
	return daemonSetFromUnstructured(object)
}

func (resources *KubernetesResources) CreateDaemonSet(
	ctx context.Context,
	daemonSet DaemonSet,
) error {
	if err := resources.validateMetadata(daemonSet.Metadata); err != nil {
		return err
	}
	object, err := daemonSetToUnstructured(daemonSet)
	if err != nil {
		return err
	}
	_, err = resources.client.Resource(daemonSetResource).
		Namespace(resources.namespace).
		Create(ctx, object, metav1.CreateOptions{})
	return mapKubernetesWriteError(err)
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

func (resources *KubernetesResources) ValidateProtectedPodCreate(
	ctx context.Context,
	pod corev1.Pod,
) error {
	if ctx == nil || resources == nil || resources.client == nil ||
		pod.APIVersion != "v1" || pod.Kind != "Pod" || pod.UID != "" ||
		pod.Namespace != resources.namespace || !validResourceName(pod.Name) ||
		pod.Labels[protectedLabel] != "true" ||
		pod.Labels[workerIDLabel] != "" || pod.Labels[workerEpochLabel] != "" ||
		!containsString(pod.Finalizers, protectionFinalizer) || len(pod.OwnerReferences) != 1 {
		return ErrProtectedResourceDrift
	}
	workerPoolID, err := uuid.Parse(pod.Labels[workerPoolLabel])
	if err != nil || workerPoolID == uuid.Nil || !validSHA256(pod.Labels[fleetRevisionLabel]) {
		return ErrProtectedResourceDrift
	}
	owner := pod.OwnerReferences[0]
	if owner.APIVersion != "apps/v1" || owner.Kind != "DaemonSet" ||
		!validResourceName(owner.Name) || owner.UID == "" ||
		owner.Controller == nil || !*owner.Controller {
		return ErrProtectedResourceDrift
	}
	live, err := resources.client.Resource(daemonSetResource).
		Namespace(resources.namespace).
		Get(ctx, owner.Name, metav1.GetOptions{})
	if err != nil {
		return mapKubernetesResourceError(err)
	}
	var daemonSet appsv1.DaemonSet
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(live.Object, &daemonSet); err != nil {
		return fmt.Errorf("decode authoritative Fleet DaemonSet: %w", err)
	}
	if daemonSet.UID == "" || owner.UID != daemonSet.UID ||
		daemonSet.Namespace != resources.namespace || daemonSet.Name != owner.Name ||
		daemonSet.Spec.UpdateStrategy.Type != appsv1.OnDeleteDaemonSetStrategyType ||
		daemonSet.Labels[protectedLabel] != "true" ||
		daemonSet.Labels[workerPoolLabel] != workerPoolID.String() ||
		daemonSet.Labels[fleetRevisionLabel] != pod.Labels[fleetRevisionLabel] ||
		daemonSet.Spec.Template.Labels[protectedLabel] != "true" ||
		daemonSet.Spec.Template.Labels[workerPoolLabel] != workerPoolID.String() ||
		daemonSet.Spec.Template.Labels[fleetRevisionLabel] != pod.Labels[fleetRevisionLabel] ||
		!daemonSetSelectorMatchesTemplate(daemonSet.Spec.Selector, daemonSet.Spec.Template.Labels) ||
		!protectedPodMetadataMatchesTemplate(pod, daemonSet.Spec.Template) ||
		!protectedPodSpecMatchesTemplate(pod.Spec, daemonSet.Spec.Template.Spec) {
		return ErrProtectedResourceDrift
	}
	return nil
}

func daemonSetSelectorMatchesTemplate(selector *metav1.LabelSelector, labels map[string]string) bool {
	if selector == nil || len(selector.MatchLabels) == 0 || len(selector.MatchExpressions) != 0 {
		return false
	}
	for key, value := range selector.MatchLabels {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func protectedPodMetadataMatchesTemplate(pod corev1.Pod, template corev1.PodTemplateSpec) bool {
	if !reflect.DeepEqual(pod.Annotations, template.Annotations) ||
		!reflect.DeepEqual(pod.Finalizers, template.Finalizers) {
		return false
	}
	labels := make(map[string]string, len(pod.Labels))
	for key, value := range pod.Labels {
		labels[key] = value
	}
	for _, key := range []string{appsv1.ControllerRevisionHashLabelKey, "pod-template-generation"} {
		if value, present := labels[key]; present {
			if value == "" || strings.TrimSpace(value) != value || len(value) > 253 {
				return false
			}
			delete(labels, key)
		}
	}
	return reflect.DeepEqual(labels, template.Labels)
}

func protectedPodSpecMatchesTemplate(actual, expected corev1.PodSpec) bool {
	if expected.Affinity != nil || actual.NodeName != "" ||
		!exactDaemonSetTargetNodeAffinity(actual.Affinity) {
		return false
	}
	normalized := *actual.DeepCopy()
	normalized.Affinity = nil
	baseTolerations, ok := removeDaemonSetControllerTolerations(
		normalized.Tolerations,
		expected.Tolerations,
		expected.HostNetwork,
	)
	if !ok {
		return false
	}
	normalized.Tolerations = baseTolerations
	return reflect.DeepEqual(normalized, expected)
}

func exactDaemonSetTargetNodeAffinity(affinity *corev1.Affinity) bool {
	if affinity == nil || affinity.PodAffinity != nil || affinity.PodAntiAffinity != nil ||
		affinity.NodeAffinity == nil || len(affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 0 ||
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return false
	}
	terms := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 1 || len(terms[0].MatchExpressions) != 0 || len(terms[0].MatchFields) != 1 {
		return false
	}
	requirement := terms[0].MatchFields[0]
	return requirement.Key == "metadata.name" && requirement.Operator == corev1.NodeSelectorOpIn &&
		len(requirement.Values) == 1 && validResourceName(requirement.Values[0])
}

func removeDaemonSetControllerTolerations(
	actual []corev1.Toleration,
	expected []corev1.Toleration,
	hostNetwork bool,
) ([]corev1.Toleration, bool) {
	if len(actual) < len(expected) || !reflect.DeepEqual(actual[:len(expected)], expected) {
		return nil, false
	}
	allowed := map[string]corev1.Toleration{
		"node.kubernetes.io/not-ready|NoExecute": {
			Key: "node.kubernetes.io/not-ready", Operator: corev1.TolerationOpExists,
			Effect: corev1.TaintEffectNoExecute,
		},
		"node.kubernetes.io/unreachable|NoExecute": {
			Key: "node.kubernetes.io/unreachable", Operator: corev1.TolerationOpExists,
			Effect: corev1.TaintEffectNoExecute,
		},
		"node.kubernetes.io/disk-pressure|NoSchedule": {
			Key: "node.kubernetes.io/disk-pressure", Operator: corev1.TolerationOpExists,
			Effect: corev1.TaintEffectNoSchedule,
		},
		"node.kubernetes.io/memory-pressure|NoSchedule": {
			Key: "node.kubernetes.io/memory-pressure", Operator: corev1.TolerationOpExists,
			Effect: corev1.TaintEffectNoSchedule,
		},
		"node.kubernetes.io/pid-pressure|NoSchedule": {
			Key: "node.kubernetes.io/pid-pressure", Operator: corev1.TolerationOpExists,
			Effect: corev1.TaintEffectNoSchedule,
		},
		"node.kubernetes.io/unschedulable|NoSchedule": {
			Key: "node.kubernetes.io/unschedulable", Operator: corev1.TolerationOpExists,
			Effect: corev1.TaintEffectNoSchedule,
		},
	}
	if hostNetwork {
		allowed["node.kubernetes.io/network-unavailable|NoSchedule"] = corev1.Toleration{
			Key: "node.kubernetes.io/network-unavailable", Operator: corev1.TolerationOpExists,
			Effect: corev1.TaintEffectNoSchedule,
		}
	}
	seen := make(map[string]bool, len(actual)-len(expected))
	for _, toleration := range actual[len(expected):] {
		key := toleration.Key + "|" + string(toleration.Effect)
		allowedToleration, ok := allowed[key]
		if !ok || seen[key] || !reflect.DeepEqual(toleration, allowedToleration) {
			return nil, false
		}
		seen[key] = true
	}
	return append([]corev1.Toleration(nil), expected...), true
}

func (resources *KubernetesResources) BindWorkerIdentity(
	ctx context.Context,
	binding WorkerPodIdentityBinding,
) error {
	if err := resources.validateBinding(binding); err != nil {
		return err
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		pods := resources.client.Resource(podResource).Namespace(resources.namespace)
		object, err := pods.Get(ctx, binding.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		pod, err := workerPodFromUnstructured(object)
		if err != nil {
			return err
		}
		workerPoolID, nodeIdentity, err := pendingWorkerPodIdentity(pod)
		if err != nil || pod.KubernetesUID != binding.KubernetesUID ||
			workerPoolID != binding.WorkerPoolID || nodeIdentity != binding.NodeIdentity {
			return ErrWorkerIdentityConflict
		}
		labels := object.GetLabels()
		labels[workerIDLabel] = binding.WorkerID.String()
		labels[workerEpochLabel] = strconv.FormatInt(binding.WorkerEpoch, 10)
		object.SetLabels(labels)
		if err := unstructured.SetNestedSlice(
			object.Object,
			[]any{},
			"spec",
			"schedulingGates",
		); err != nil {
			return fmt.Errorf("remove Worker Pod identity scheduling gate: %w", err)
		}
		_, err = pods.Update(ctx, object, metav1.UpdateOptions{})
		return err
	})
}

func (resources *KubernetesResources) AttachDrainOperations(
	ctx context.Context,
	resource ProtectedResource,
	drainIDs []uuid.UUID,
) error {
	if err := resources.validateProtectedResource(resource); err != nil || len(drainIDs) == 0 {
		return errors.New("protected Fleet drain identity is invalid")
	}
	values := make([]string, 0, len(drainIDs))
	for _, drainID := range drainIDs {
		if drainID == uuid.Nil {
			return errors.New("protected Fleet drain identity is invalid")
		}
		values = append(values, drainID.String())
	}
	sort.Strings(values)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		client, err := resources.resourceClient(resource)
		if err != nil {
			return err
		}
		object, err := client.Get(ctx, resource.Name, metav1.GetOptions{})
		if err != nil {
			return mapKubernetesResourceError(err)
		}
		if string(object.GetUID()) != resource.KubernetesUID {
			return ErrProtectedResourceDrift
		}
		annotations := object.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		annotations[fleetcontract.DrainOperationIDsAnnotation] = strings.Join(values, ",")
		object.SetAnnotations(annotations)
		_, err = client.Update(ctx, object, metav1.UpdateOptions{})
		return mapKubernetesWriteError(err)
	})
}

func (resources *KubernetesResources) Delete(
	ctx context.Context,
	resource ProtectedResource,
) error {
	if err := resources.validateProtectedResource(resource); err != nil {
		return err
	}
	client, err := resources.resourceClient(resource)
	if err != nil {
		return err
	}
	uid := types.UID(resource.KubernetesUID)
	err = client.Delete(ctx, resource.Name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	})
	return mapKubernetesWriteError(err)
}

func (resources *KubernetesResources) RemoveFinalizer(
	ctx context.Context,
	resource ProtectedResource,
) error {
	if err := resources.validateProtectedResource(resource); err != nil {
		return err
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		client, err := resources.resourceClient(resource)
		if err != nil {
			return err
		}
		object, err := client.Get(ctx, resource.Name, metav1.GetOptions{})
		if err != nil {
			return mapKubernetesResourceError(err)
		}
		if string(object.GetUID()) != resource.KubernetesUID {
			return ErrProtectedResourceDrift
		}
		finalizers := object.GetFinalizers()
		filtered := make([]string, 0, len(finalizers))
		removed := false
		for _, finalizer := range finalizers {
			if finalizer == protectionFinalizer {
				removed = true
				continue
			}
			filtered = append(filtered, finalizer)
		}
		if !removed {
			return ErrProtectedResourceDrift
		}
		object.SetFinalizers(filtered)
		_, err = client.Update(ctx, object, metav1.UpdateOptions{})
		return mapKubernetesWriteError(err)
	})
}

func (resources *KubernetesResources) IsAbsent(
	ctx context.Context,
	resource ProtectedResource,
) (bool, error) {
	if err := resources.validateProtectedResource(resource); err != nil {
		return false, err
	}
	client, err := resources.resourceClient(resource)
	if err != nil {
		return false, err
	}
	object, err := client.Get(ctx, resource.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return string(object.GetUID()) != resource.KubernetesUID, nil
}

func (resources *KubernetesResources) ListPendingWorkerPods(
	ctx context.Context,
) ([]WorkerPod, error) {
	objects, err := resources.client.Resource(podResource).
		Namespace(resources.namespace).
		List(ctx, metav1.ListOptions{LabelSelector: protectedLabel + "=true"})
	if err != nil {
		return nil, err
	}
	pods := make([]WorkerPod, 0, len(objects.Items))
	for index := range objects.Items {
		pod, err := workerPodFromUnstructured(&objects.Items[index])
		if err != nil {
			return nil, err
		}
		if pod.Metadata.Labels[workerIDLabel] == "" &&
			containsString(pod.SchedulingGates, IdentityBindingSchedulingGate) {
			pods = append(pods, pod)
		}
	}
	sort.Slice(pods, func(left, right int) bool {
		return pods[left].Metadata.Name < pods[right].Metadata.Name
	})
	return pods, nil
}

func (resources *KubernetesResources) validateKey(key ResourceKey) error {
	if resources == nil || resources.client == nil || key.Namespace != resources.namespace ||
		!validResourceName(key.Name) {
		return errors.New("fleet Kubernetes resource key is invalid")
	}
	return nil
}

func (resources *KubernetesResources) validateMetadata(metadata Metadata) error {
	return resources.validateKey(ResourceKey{Namespace: metadata.Namespace, Name: metadata.Name})
}

func (resources *KubernetesResources) validateBinding(binding WorkerPodIdentityBinding) error {
	if resources == nil || resources.client == nil || binding.KubernetesUID == "" ||
		binding.Namespace != resources.namespace || !validResourceName(binding.Name) ||
		binding.WorkerID == uuid.Nil || binding.WorkerPoolID == uuid.Nil ||
		binding.WorkerEpoch <= 0 || !validResourceName(binding.NodeIdentity) {
		return ErrWorkerPodIdentityInvalid
	}
	return nil
}

func (resources *KubernetesResources) validateProtectedResource(resource ProtectedResource) error {
	if resources == nil || resources.client == nil || resource.KubernetesUID == "" ||
		resource.Namespace != resources.namespace || !validResourceName(resource.Name) ||
		resource.WorkerPoolID == uuid.Nil {
		return errors.New("protected Fleet Kubernetes resource is invalid")
	}
	if resource.Kind == ResourcePod && (resource.WorkerID == uuid.Nil || resource.WorkerEpoch <= 0) {
		return errors.New("protected Fleet Kubernetes resource is invalid")
	}
	if resource.Kind != ResourcePod && (resource.WorkerID != uuid.Nil || resource.WorkerEpoch != 0) {
		return errors.New("protected Fleet Kubernetes resource is invalid")
	}
	return nil
}

func (resources *KubernetesResources) resourceClient(
	resource ProtectedResource,
) (dynamic.ResourceInterface, error) {
	var gvr schema.GroupVersionResource
	switch resource.Kind {
	case ResourcePod:
		gvr = podResource
	case ResourceDaemonSet:
		gvr = daemonSetResource
	case ResourceWorkerPool:
		gvr = workerPoolResource
	default:
		return nil, errors.New("protected Fleet Kubernetes resource kind is invalid")
	}
	return resources.client.Resource(gvr).Namespace(resources.namespace), nil
}

func workerPoolToUnstructured(workerPool WorkerPool) (*unstructured.Unstructured, error) {
	if workerPool.Spec.Revision == "" || workerPool.Spec.WorkerProfile == "" ||
		len(workerPool.Spec.Placements) == 0 {
		return nil, errors.New("fleet WorkerPool is invalid")
	}
	placements := make([]any, 0, len(workerPool.Spec.Placements))
	for _, placement := range workerPool.Spec.Placements {
		placements = append(placements, map[string]any{
			"nodeIdentity":            placement.NodeIdentity,
			"daemonSetName":           placement.DaemonSetName,
			"workerRuntimeConfigMap":  placement.WorkerRuntimeConfigMap,
			"runnerProfilesConfigMap": placement.RunnerProfilesConfigMap,
			"runnerGPURolesConfigMap": placement.RunnerGPURolesConfigMap,
			"workerControlTLSSecret":  placement.WorkerControlTLSSecret,
		})
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "fleet.vela.ai/v1alpha1",
		"kind":       "WorkerPool",
		"metadata": map[string]any{
			"namespace":  workerPool.Metadata.Namespace,
			"name":       workerPool.Metadata.Name,
			"labels":     stringMapToAny(workerPool.Metadata.Labels),
			"finalizers": stringsToAny(workerPool.Metadata.Finalizers),
		},
		"spec": map[string]any{
			"revision":      workerPool.Spec.Revision,
			"workerProfile": workerPool.Spec.WorkerProfile,
			"nodeSelector":  stringMapToAny(workerPool.Spec.NodeSelector),
			"placements":    placements,
			"capacityPolicy": map[string]any{
				"workerHighWatermarkBytes": workerPool.Spec.CapacityPolicy.WorkerHighWatermarkBytes,
				"workerLowWatermarkBytes":  workerPool.Spec.CapacityPolicy.WorkerLowWatermarkBytes,
				"workerCriticalFreeBytes":  workerPool.Spec.CapacityPolicy.WorkerCriticalFreeBytes,
				"poolHighWatermarkBytes":   workerPool.Spec.CapacityPolicy.PoolHighWatermarkBytes,
				"poolLowWatermarkBytes":    workerPool.Spec.CapacityPolicy.PoolLowWatermarkBytes,
				"observationMaxAgeSeconds": int64(workerPool.Spec.CapacityPolicy.ObservationMaxAge / time.Second),
			},
		},
	}}, nil
}

func workerPoolFromUnstructured(object *unstructured.Unstructured) (WorkerPool, error) {
	revision, revisionOK, _ := unstructured.NestedString(object.Object, "spec", "revision")
	profile, profileOK, _ := unstructured.NestedString(object.Object, "spec", "workerProfile")
	nodeSelector, selectorOK, _ := unstructured.NestedStringMap(object.Object, "spec", "nodeSelector")
	encodedPlacements, placementsOK, _ := unstructured.NestedSlice(object.Object, "spec", "placements")
	placements := make([]WorkerPlacement, 0, len(encodedPlacements))
	for _, encodedPlacement := range encodedPlacements {
		placementMap, ok := encodedPlacement.(map[string]any)
		if !ok || len(placementMap) != 6 {
			return WorkerPool{}, errors.New("live Fleet WorkerPool placement shape is invalid")
		}
		placement := WorkerPlacement{}
		placement.NodeIdentity, ok = placementMap["nodeIdentity"].(string)
		if !ok {
			return WorkerPool{}, errors.New("live Fleet WorkerPool placement shape is invalid")
		}
		placement.DaemonSetName, ok = placementMap["daemonSetName"].(string)
		if !ok {
			return WorkerPool{}, errors.New("live Fleet WorkerPool placement shape is invalid")
		}
		placement.WorkerRuntimeConfigMap, ok = placementMap["workerRuntimeConfigMap"].(string)
		if !ok {
			return WorkerPool{}, errors.New("live Fleet WorkerPool placement shape is invalid")
		}
		placement.RunnerProfilesConfigMap, ok = placementMap["runnerProfilesConfigMap"].(string)
		if !ok {
			return WorkerPool{}, errors.New("live Fleet WorkerPool placement shape is invalid")
		}
		placement.RunnerGPURolesConfigMap, ok = placementMap["runnerGPURolesConfigMap"].(string)
		if !ok {
			return WorkerPool{}, errors.New("live Fleet WorkerPool placement shape is invalid")
		}
		placement.WorkerControlTLSSecret, ok = placementMap["workerControlTLSSecret"].(string)
		if !ok {
			return WorkerPool{}, errors.New("live Fleet WorkerPool placement shape is invalid")
		}
		placements = append(placements, placement)
	}
	workerHigh, workerHighOK, _ := unstructured.NestedInt64(
		object.Object, "spec", "capacityPolicy", "workerHighWatermarkBytes",
	)
	workerLow, workerLowOK, _ := unstructured.NestedInt64(
		object.Object, "spec", "capacityPolicy", "workerLowWatermarkBytes",
	)
	workerCritical, workerCriticalOK, _ := unstructured.NestedInt64(
		object.Object, "spec", "capacityPolicy", "workerCriticalFreeBytes",
	)
	poolHigh, poolHighOK, _ := unstructured.NestedInt64(
		object.Object, "spec", "capacityPolicy", "poolHighWatermarkBytes",
	)
	poolLow, poolLowOK, _ := unstructured.NestedInt64(
		object.Object, "spec", "capacityPolicy", "poolLowWatermarkBytes",
	)
	maxAgeSeconds, maxAgeOK, _ := unstructured.NestedInt64(
		object.Object, "spec", "capacityPolicy", "observationMaxAgeSeconds",
	)
	policy := CapacityPolicySpec{
		WorkerHighWatermarkBytes: workerHigh,
		WorkerLowWatermarkBytes:  workerLow,
		WorkerCriticalFreeBytes:  workerCritical,
		PoolHighWatermarkBytes:   poolHigh,
		PoolLowWatermarkBytes:    poolLow,
		ObservationMaxAge:        time.Duration(maxAgeSeconds) * time.Second,
	}
	if !revisionOK || !profileOK || !selectorOK || !placementsOK || len(placements) == 0 || !workerHighOK ||
		!workerLowOK || !workerCriticalOK || !poolHighOK || !poolLowOK || !maxAgeOK ||
		!validCapacityPolicySpec(policy) {
		return WorkerPool{}, errors.New("live Fleet WorkerPool shape is invalid")
	}
	return WorkerPool{
		Metadata: metadataFromUnstructured(object),
		Spec: WorkerPoolSpec{
			Revision: revision, WorkerProfile: profile,
			NodeSelector: nodeSelector, Placements: placements, CapacityPolicy: policy,
		},
	}, nil
}

func daemonSetToUnstructured(daemonSet DaemonSet) (*unstructured.Unstructured, error) {
	selector := map[string]string{}
	for key, value := range daemonSet.Selector {
		selector[key] = value
	}
	typed := &appsv1.DaemonSet{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "DaemonSet"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: daemonSet.Metadata.Namespace, Name: daemonSet.Metadata.Name,
			Labels:     cloneMap(daemonSet.Metadata.Labels),
			Finalizers: append([]string(nil), daemonSet.Metadata.Finalizers...),
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
				Type: appsv1.DaemonSetUpdateStrategyType(daemonSet.UpdateStrategy),
			},
			Template: *daemonSet.Template.DeepCopy(),
		},
	}
	encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(typed)
	if err != nil {
		return nil, fmt.Errorf("encode Fleet DaemonSet: %w", err)
	}
	return &unstructured.Unstructured{Object: encoded}, nil
}

func daemonSetFromUnstructured(object *unstructured.Unstructured) (DaemonSet, error) {
	var typed appsv1.DaemonSet
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &typed); err != nil {
		return DaemonSet{}, fmt.Errorf("decode live Fleet DaemonSet: %w", err)
	}
	return DaemonSet{
		Metadata:       metadataFromUnstructured(object),
		UpdateStrategy: string(typed.Spec.UpdateStrategy.Type),
		Selector:       cloneMap(typed.Spec.Selector.MatchLabels),
		Template:       *typed.Spec.Template.DeepCopy(),
	}, nil
}

func workerPodFromUnstructured(object *unstructured.Unstructured) (WorkerPod, error) {
	var typed corev1.Pod
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.Object, &typed); err != nil {
		return WorkerPod{}, fmt.Errorf("decode protected Worker Pod: %w", err)
	}
	pod := WorkerPod{
		KubernetesUID:        string(typed.UID),
		CreatedAt:            typed.CreationTimestamp.UTC(),
		Metadata:             metadataFromObjectMeta(typed.ObjectMeta),
		SchedulingGates:      []string{},
		RequiredNodeAffinity: []NodeSelectorTerm{},
	}
	for _, gate := range typed.Spec.SchedulingGates {
		pod.SchedulingGates = append(pod.SchedulingGates, gate.Name)
	}
	if typed.Spec.Affinity == nil || typed.Spec.Affinity.NodeAffinity == nil ||
		typed.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return pod, nil
	}
	for _, term := range typed.Spec.Affinity.NodeAffinity.
		RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		converted := NodeSelectorTerm{}
		for _, requirement := range term.MatchExpressions {
			converted.MatchExpressions = append(converted.MatchExpressions, NodeSelectorRequirement{
				Key: requirement.Key, Operator: string(requirement.Operator),
				Values: append([]string(nil), requirement.Values...),
			})
		}
		for _, requirement := range term.MatchFields {
			converted.MatchFields = append(converted.MatchFields, NodeSelectorRequirement{
				Key: requirement.Key, Operator: string(requirement.Operator),
				Values: append([]string(nil), requirement.Values...),
			})
		}
		pod.RequiredNodeAffinity = append(pod.RequiredNodeAffinity, converted)
	}
	return pod, nil
}

func metadataFromUnstructured(object *unstructured.Unstructured) Metadata {
	return Metadata{
		Namespace: object.GetNamespace(), Name: object.GetName(),
		Labels:     cloneMap(object.GetLabels()),
		Finalizers: append([]string(nil), object.GetFinalizers()...),
	}
}

func metadataFromObjectMeta(metadata metav1.ObjectMeta) Metadata {
	return Metadata{
		Namespace: metadata.Namespace, Name: metadata.Name,
		Labels:     cloneMap(metadata.Labels),
		Finalizers: append([]string(nil), metadata.Finalizers...),
	}
}

func stringMapToAny(values map[string]string) map[string]any {
	converted := make(map[string]any, len(values))
	for key, value := range values {
		converted[key] = value
	}
	return converted
}

func stringsToAny(values []string) []any {
	converted := make([]any, len(values))
	for index, value := range values {
		converted[index] = value
	}
	return converted
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
