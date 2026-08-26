package fleetcontroller_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

var podGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}

func TestKubernetesResourcesMaterializeLiveWorkerPoolAndGatedDaemonSet(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	resources, err := fleetcontroller.NewKubernetesResources(client, "vela-system")
	if err != nil {
		t.Fatalf("create Kubernetes Fleet resources: %v", err)
	}
	reconciler, err := fleetcontroller.NewReconciler(
		resources,
		&staticDrainReader{},
		&staticIdentityResolver{},
		&staticReadinessStarter{},
		&staticCapacityPolicyConfigurator{},
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}
	desired := desiredRevision()
	result, err := reconciler.Reconcile(context.Background(), desired)
	if err != nil || !result.WorkerPoolCreated || !result.DaemonSetCreated {
		t.Fatalf("materialize Kubernetes Fleet resources = %#v error=%v", result, err)
	}

	workerPool, err := client.Resource(schema.GroupVersionResource{
		Group: "fleet.vela.ai", Version: "v1alpha1", Resource: "workerpools",
	}).Namespace("vela-system").Get(
		context.Background(),
		desired.Name,
		metav1.GetOptions{},
	)
	if err != nil || workerPool.GetLabels()["vela.ai/fleet-revision"] != desired.Revision {
		t.Fatalf("live WorkerPool = %#v error=%v", workerPool, err)
	}
	daemonSet, err := client.Resource(schema.GroupVersionResource{
		Group: "apps", Version: "v1", Resource: "daemonsets",
	}).Namespace("vela-system").Get(
		context.Background(),
		desired.DaemonSetName,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get live DaemonSet: %v", err)
	}
	assertNestedString(t, daemonSet.Object, "OnDelete", "spec", "updateStrategy", "type")
	assertNestedString(
		t,
		daemonSet.Object,
		fleetcontroller.IdentityBindingSchedulingGate,
		"spec", "template", "spec", "schedulingGates", "0", "name",
	)
	assertNestedString(
		t,
		daemonSet.Object,
		"8",
		"spec", "template", "spec", "containers", "1", "resources", "limits", "nvidia.com/gpu",
	)
	assertContainerEnvFieldPath(
		t,
		daemonSet.Object,
		"worker-agent",
		"VELA_WORKER_ID",
		"metadata.labels['vela.ai/worker-id']",
	)
	assertContainerEnvFieldPath(
		t,
		daemonSet.Object,
		"worker-agent",
		"VELA_WORKER_NODE_IDENTITY",
		"spec.nodeName",
	)

	result, err = reconciler.Reconcile(context.Background(), desired)
	if err != nil || !result.Converged || result.WorkerPoolCreated || result.DaemonSetCreated {
		t.Fatalf("reconcile existing Kubernetes Fleet resources = %#v error=%v", result, err)
	}
}

func TestKubernetesReconcileAcceptsAPIServerDefaultedDaemonSet(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	resources, err := fleetcontroller.NewKubernetesResources(client, "vela-system")
	if err != nil {
		t.Fatalf("create Kubernetes Fleet resources: %v", err)
	}
	reconciler, err := fleetcontroller.NewReconciler(
		resources,
		&staticDrainReader{},
		&staticIdentityResolver{},
		&staticReadinessStarter{},
		&staticCapacityPolicyConfigurator{},
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}
	desired := desiredRevision()
	if _, err := reconciler.Reconcile(context.Background(), desired); err != nil {
		t.Fatalf("materialize Kubernetes Fleet resources: %v", err)
	}
	daemonSets := client.Resource(schema.GroupVersionResource{
		Group: "apps", Version: "v1", Resource: "daemonsets",
	}).Namespace(desired.Namespace)
	live, err := daemonSets.Get(
		context.Background(), desired.DaemonSetName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get materialized DaemonSet: %v", err)
	}
	var defaulted appsv1.DaemonSet
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(
		live.Object, &defaulted,
	); err != nil {
		t.Fatalf("decode materialized DaemonSet: %v", err)
	}
	defaulted.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyAlways
	defaulted.Spec.Template.Spec.DNSPolicy = corev1.DNSClusterFirst
	defaulted.Spec.Template.Spec.SchedulerName = corev1.DefaultSchedulerName
	enableServiceLinks := true
	defaulted.Spec.Template.Spec.EnableServiceLinks = &enableServiceLinks
	for index := range defaulted.Spec.Template.Spec.InitContainers {
		defaulted.Spec.Template.Spec.InitContainers[index].TerminationMessagePath =
			corev1.TerminationMessagePathDefault
		defaulted.Spec.Template.Spec.InitContainers[index].TerminationMessagePolicy =
			corev1.TerminationMessageReadFile
	}
	for index := range defaulted.Spec.Template.Spec.Containers {
		defaulted.Spec.Template.Spec.Containers[index].TerminationMessagePath =
			corev1.TerminationMessagePathDefault
		defaulted.Spec.Template.Spec.Containers[index].TerminationMessagePolicy =
			corev1.TerminationMessageReadFile
	}
	defaultedObject, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&defaulted)
	if err != nil {
		t.Fatalf("encode API-defaulted DaemonSet: %v", err)
	}
	if _, err := daemonSets.Update(
		context.Background(),
		&unstructured.Unstructured{Object: defaultedObject},
		metav1.UpdateOptions{},
	); err != nil {
		t.Fatalf("store API-defaulted DaemonSet: %v", err)
	}

	result, err := reconciler.Reconcile(context.Background(), desired)
	if err != nil || !result.Converged {
		t.Fatalf("reconcile API-defaulted DaemonSet = %#v error=%v", result, err)
	}
}

func TestProtectedPodCreateMatchesLiveMaterializedDaemonSetExactly(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	resources, err := fleetcontroller.NewKubernetesResources(client, "vela-system")
	if err != nil {
		t.Fatalf("create Kubernetes Fleet resources: %v", err)
	}
	reconciler, err := fleetcontroller.NewReconciler(
		resources,
		&staticDrainReader{},
		&staticIdentityResolver{},
		&staticReadinessStarter{},
		&staticCapacityPolicyConfigurator{},
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}
	desired := desiredRevision()
	if _, err := reconciler.Reconcile(context.Background(), desired); err != nil {
		t.Fatalf("materialize protected DaemonSet: %v", err)
	}
	daemonSetResource := schema.GroupVersionResource{
		Group: "apps", Version: "v1", Resource: "daemonsets",
	}
	live, err := client.Resource(daemonSetResource).Namespace(desired.Namespace).Get(
		context.Background(), desired.DaemonSetName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get materialized DaemonSet: %v", err)
	}
	live.SetUID(types.UID("kubernetes-daemonset-uid-1"))
	live, err = client.Resource(daemonSetResource).Namespace(desired.Namespace).Update(
		context.Background(), live, metav1.UpdateOptions{},
	)
	if err != nil {
		t.Fatalf("assign test DaemonSet UID: %v", err)
	}
	var daemonSet appsv1.DaemonSet
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(live.Object, &daemonSet); err != nil {
		t.Fatalf("decode materialized DaemonSet: %v", err)
	}
	controller := true
	pod := corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: *daemonSet.Spec.Template.ObjectMeta.DeepCopy(),
		Spec:       *daemonSet.Spec.Template.Spec.DeepCopy(),
	}
	pod.Namespace = desired.Namespace
	pod.Name = "h3-worker-node-1"
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "DaemonSet", Name: daemonSet.Name,
		UID: daemonSet.UID, Controller: &controller,
	}}
	pod.Spec.Affinity = exactTargetNodeAffinity("h3-node-01")
	if err := resources.ValidateProtectedPodCreate(context.Background(), pod); err != nil {
		t.Fatalf("validate materialized protected Pod: %v", err)
	}

	privileged := pod.DeepCopy()
	privileged.Spec.Containers[0].SecurityContext.Privileged = boolPointer(true)
	if err := resources.ValidateProtectedPodCreate(context.Background(), *privileged); err == nil {
		t.Fatal("privileged counterfeit protected Pod matched the materialized template")
	}
}

func exactTargetNodeAffinity(node string) *corev1.Affinity {
	return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchFields: []corev1.NodeSelectorRequirement{{
					Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{node},
				}},
			}},
		},
	}}
}

func boolPointer(value bool) *bool { return &value }

func TestKubernetesResourcesBindWorkerIdentityAtomicallyAfterUIDAndAffinityCheck(t *testing.T) {
	pod := pendingWorkerPodUnstructured()
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), pod)
	resources, err := fleetcontroller.NewKubernetesResources(client, "vela-system")
	if err != nil {
		t.Fatalf("create Kubernetes Fleet resources: %v", err)
	}
	binding := fleetcontroller.WorkerPodIdentityBinding{
		KubernetesUID: "stale-worker-pod-uid",
		Namespace:     "vela-system",
		Name:          "h3-worker-node-1",
		WorkerID:      uuid.MustParse("23000000-0000-0000-0000-000000000041"),
		WorkerPoolID:  uuid.MustParse("00000000-0000-0000-0000-000000000005"),
		WorkerEpoch:   20,
		NodeIdentity:  "h3-node-01",
	}
	if err := resources.BindWorkerIdentity(context.Background(), binding); !errors.Is(err, fleetcontroller.ErrWorkerIdentityConflict) {
		t.Fatalf("stale Worker Pod UID binding error = %v", err)
	}
	live, err := client.Resource(podGVR).Namespace("vela-system").Get(
		context.Background(),
		"h3-worker-node-1",
		metav1.GetOptions{},
	)
	if err != nil || live.GetLabels()["vela.ai/worker-id"] != "" {
		t.Fatalf("stale UID mutated Worker Pod = %#v error=%v", live, err)
	}
	gates, found, err := unstructured.NestedSlice(live.Object, "spec", "schedulingGates")
	if err != nil || !found || len(gates) != 1 {
		t.Fatalf("stale UID changed scheduling gates = %#v found=%t error=%v", gates, found, err)
	}

	binding.KubernetesUID = "kubernetes-worker-pod-uid-1"
	if err := resources.BindWorkerIdentity(context.Background(), binding); err != nil {
		t.Fatalf("bind current Worker Pod identity: %v", err)
	}
	live, err = client.Resource(podGVR).Namespace("vela-system").Get(
		context.Background(),
		"h3-worker-node-1",
		metav1.GetOptions{},
	)
	if err != nil || live.GetLabels()["vela.ai/worker-id"] != binding.WorkerID.String() ||
		live.GetLabels()["vela.ai/worker-epoch"] != "20" {
		t.Fatalf("bound Worker Pod labels = %#v error=%v", live.GetLabels(), err)
	}
	gates, found, err = unstructured.NestedSlice(live.Object, "spec", "schedulingGates")
	if err != nil || !found || len(gates) != 0 {
		t.Fatalf("bound Worker Pod scheduling gates = %#v found=%t error=%v", gates, found, err)
	}
	assertNestedString(
		t,
		live.Object,
		"h3-node-01",
		"spec", "affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution",
		"nodeSelectorTerms", "0", "matchFields", "0", "values", "0",
	)
}

func TestRuntimeConvergesDesiredResourcesAndPendingPodInOneCycle(t *testing.T) {
	failPodList := false
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{podGVR: "PodList"},
		pendingWorkerPodUnstructured(),
	)
	client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		if !failPodList {
			return false, nil, nil
		}
		return true, nil, errors.New("kubernetes API unavailable")
	})
	resources, err := fleetcontroller.NewKubernetesResources(client, "vela-system")
	if err != nil {
		t.Fatalf("create Kubernetes Fleet resources: %v", err)
	}
	resolver := &staticIdentityResolver{identity: fleet.WorkerIdentity{
		WorkerID:     uuid.MustParse("23000000-0000-0000-0000-000000000041"),
		WorkerPoolID: uuid.MustParse("00000000-0000-0000-0000-000000000005"),
		WorkerEpoch:  20,
		NodeIdentity: "h3-node-01",
	}}
	reconciler, err := fleetcontroller.NewReconciler(
		resources,
		&staticDrainReader{},
		resolver,
		successfulReadinessStarter(),
		&staticCapacityPolicyConfigurator{},
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}
	runtimeController, err := fleetcontroller.NewRuntime(
		resources,
		reconciler,
		fleetcontroller.RuntimeConfig{
			DesiredRevisions: []fleetcontroller.DesiredRevision{desiredRevision()},
			PollInterval:     time.Second,
		},
	)
	if err != nil {
		t.Fatalf("create Fleet runtime: %v", err)
	}
	if !runtimeController.Ready() {
		t.Fatal("Fleet admission runtime is not ready after successful construction")
	}
	if runtimeController.Converged() {
		t.Fatal("Fleet runtime reports convergence before its first complete cycle")
	}
	result, err := runtimeController.RunOnce(context.Background())
	if err != nil || result.DesiredRevisionsConverged != 1 || result.WorkerPodsBound != 1 {
		t.Fatalf("Fleet runtime cycle = %#v error=%v", result, err)
	}
	if !runtimeController.Ready() {
		t.Fatal("Fleet admission runtime is not ready after a complete successful cycle")
	}
	if !runtimeController.Converged() {
		t.Fatal("Fleet runtime did not report convergence after a complete successful cycle")
	}
	live, err := client.Resource(podGVR).Namespace("vela-system").Get(
		context.Background(),
		"h3-worker-node-1",
		metav1.GetOptions{},
	)
	if err != nil || live.GetLabels()["vela.ai/worker-id"] != resolver.identity.WorkerID.String() {
		t.Fatalf("runtime-bound Worker Pod = %#v error=%v", live, err)
	}

	result, err = runtimeController.RunOnce(context.Background())
	if err != nil || result.DesiredRevisionsConverged != 1 || result.WorkerPodsBound != 0 ||
		len(resolver.requests) != 1 {
		t.Fatalf("idempotent Fleet runtime cycle = %#v requests=%#v error=%v", result, resolver.requests, err)
	}
	failPodList = true
	if _, err := runtimeController.RunOnce(context.Background()); err == nil {
		t.Fatal("Fleet runtime cycle succeeded while Kubernetes Pod listing failed")
	}
	if !runtimeController.Ready() {
		t.Fatal("Fleet admission runtime stopped serving after a failed reconciliation cycle")
	}
	if runtimeController.Converged() {
		t.Fatal("Fleet runtime retained convergence after a failed cycle")
	}
}

func TestKubernetesRetirementMapsAlreadyAbsentResourcesForAuthoritativeReplay(t *testing.T) {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	resources, err := fleetcontroller.NewKubernetesResources(client, "vela-system")
	if err != nil {
		t.Fatalf("create Kubernetes Fleet resources: %v", err)
	}
	pod := protectedWorkerPod()
	drainID := uuid.MustParse("23000000-0000-0000-0000-000000000042")
	if err := resources.AttachDrainOperations(
		context.Background(), pod, []uuid.UUID{drainID},
	); !errors.Is(err, fleetcontroller.ErrResourceNotFound) {
		t.Fatalf("attach drain to absent protected Pod error = %v", err)
	}
	if err := resources.RemoveFinalizer(
		context.Background(), pod,
	); !errors.Is(err, fleetcontroller.ErrResourceNotFound) {
		t.Fatalf("remove finalizer from absent protected Pod error = %v", err)
	}
	absent, err := resources.IsAbsent(context.Background(), pod)
	if err != nil || !absent {
		t.Fatalf("observe absent protected Pod = %t error=%v", absent, err)
	}
}

func TestKubernetesRetirementObservesExactUIDAbsence(t *testing.T) {
	pod := protectedWorkerPod()
	for _, testCase := range []struct {
		name       string
		liveUID    string
		wantAbsent bool
	}{
		{name: "exact UID remains", liveUID: pod.KubernetesUID, wantAbsent: false},
		{name: "name was reused by replacement UID", liveUID: "replacement-worker-pod-uid", wantAbsent: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			live := pendingWorkerPodUnstructured()
			live.SetUID(types.UID(testCase.liveUID))
			client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), live)
			resources, err := fleetcontroller.NewKubernetesResources(client, "vela-system")
			if err != nil {
				t.Fatalf("create Kubernetes Fleet resources: %v", err)
			}
			absent, err := resources.IsAbsent(context.Background(), pod)
			if err != nil || absent != testCase.wantAbsent {
				t.Fatalf("exact protected Pod absence = %t error=%v, want %t", absent, err, testCase.wantAbsent)
			}
		})
	}
}

func pendingWorkerPodUnstructured() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"uid":               "kubernetes-worker-pod-uid-1",
			"namespace":         "vela-system",
			"name":              "h3-worker-node-1",
			"creationTimestamp": "2026-08-25T14:00:00Z",
			"labels": map[string]any{
				"vela.ai/fleet-protected": "true",
				"vela.ai/worker-pool-id":  "00000000-0000-0000-0000-000000000005",
				"vela.ai/fleet-revision":  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
			"finalizers": []any{"fleet.vela.ai/drain-protection"},
		},
		"spec": map[string]any{
			"schedulingGates": []any{map[string]any{
				"name": fleetcontroller.IdentityBindingSchedulingGate,
			}},
			"affinity": map[string]any{
				"nodeAffinity": map[string]any{
					"requiredDuringSchedulingIgnoredDuringExecution": map[string]any{
						"nodeSelectorTerms": []any{map[string]any{
							"matchFields": []any{map[string]any{
								"key":      "metadata.name",
								"operator": "In",
								"values":   []any{"h3-node-01"},
							}},
						}},
					},
				},
			},
		},
	}}
}

func assertContainerEnvFieldPath(
	t *testing.T,
	object map[string]any,
	containerName string,
	environmentName string,
	expected string,
) {
	t.Helper()
	containers, found, err := unstructured.NestedSlice(
		object,
		"spec", "template", "spec", "containers",
	)
	if err != nil || !found {
		t.Fatalf("read live Fleet containers: found=%t error=%v", found, err)
	}
	for _, value := range containers {
		container, ok := value.(map[string]any)
		if !ok || container["name"] != containerName {
			continue
		}
		environment, ok := container["env"].([]any)
		if !ok {
			t.Fatalf("container %q environment = %#v", containerName, container["env"])
		}
		for _, environmentValue := range environment {
			variable, ok := environmentValue.(map[string]any)
			if !ok || variable["name"] != environmentName {
				continue
			}
			assertNestedString(
				t,
				variable,
				expected,
				"valueFrom", "fieldRef", "fieldPath",
			)
			return
		}
		t.Fatalf("container %q environment %q is missing", containerName, environmentName)
	}
	t.Fatalf("container %q is missing", containerName)
}

func assertNestedString(
	t *testing.T,
	object map[string]any,
	expected string,
	fields ...string,
) {
	t.Helper()
	current := any(object)
	for _, field := range fields {
		switch typed := current.(type) {
		case map[string]any:
			current = typed[field]
		case []any:
			switch field {
			case "0":
				current = typed[0]
			case "1":
				current = typed[1]
			default:
				t.Fatalf("unsupported test list index %q", field)
			}
		default:
			t.Fatalf("nested path %v stopped at %T", fields, current)
		}
	}
	if current != expected {
		t.Fatalf("nested path %v = %#v, want %q", fields, current, expected)
	}
}
