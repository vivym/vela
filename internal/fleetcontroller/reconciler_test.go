package fleetcontroller_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	corev1 "k8s.io/api/core/v1"
)

func TestReconcileMaterializesImmutableProtectedWorkerPoolAndOnDeleteDaemonSet(t *testing.T) {
	operations := []string{}
	resources := &recordingResources{sharedOperations: &operations}
	capacity := &staticCapacityPolicyConfigurator{sharedOperations: &operations}
	reconciler, err := fleetcontroller.NewReconciler(
		resources,
		&staticDrainReader{},
		&staticIdentityResolver{},
		&staticReadinessStarter{},
		capacity,
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}
	desired := desiredRevision()
	desired.Placements = append(desired.Placements, fleetcontroller.WorkerPlacement{
		NodeIdentity: "h3-node-02", DaemonSetName: "h3-worker-pool-primary-node-02",
		WorkerRuntimeConfigMap:  "vela-worker-runtime-a2",
		RunnerProfilesConfigMap: "vela-runner-profiles-a2",
		RunnerGPURolesConfigMap: "vela-runner-gpu-roles-a2",
		WorkerControlTLSSecret:  "vela-worker-control-mtls-a2",
	})
	result, err := reconciler.Reconcile(context.Background(), desired)
	if err != nil {
		t.Fatalf("reconcile desired Fleet revision: %v", err)
	}
	if !result.WorkerPoolCreated || !result.DaemonSetCreated || result.Converged {
		t.Fatalf("first Fleet reconcile result = %#v", result)
	}
	if !reflect.DeepEqual(resources.operations, []string{
		"get-worker-pool", "create-worker-pool", "get-daemonset", "create-daemonset",
		"get-daemonset", "create-daemonset",
	}) {
		t.Fatalf("Fleet resource operations = %#v", resources.operations)
	}
	if !reflect.DeepEqual(operations, []string{
		"get-worker-pool", "create-worker-pool", "get-daemonset", "create-daemonset",
		"get-daemonset", "create-daemonset",
		"configure-capacity-policy",
	}) {
		t.Fatalf("Fleet reconciliation authority order = %#v", operations)
	}
	wantCapacityPolicy := fleet.CapacityPolicy{
		WorkerPoolID: desired.WorkerPoolID, Revision: desired.Revision,
		WorkerHighWatermarkBytes: desired.CapacityPolicy.WorkerHighWatermarkBytes,
		WorkerLowWatermarkBytes:  desired.CapacityPolicy.WorkerLowWatermarkBytes,
		WorkerCriticalFreeBytes:  desired.CapacityPolicy.WorkerCriticalFreeBytes,
		PoolHighWatermarkBytes:   desired.CapacityPolicy.PoolHighWatermarkBytes,
		PoolLowWatermarkBytes:    desired.CapacityPolicy.PoolLowWatermarkBytes,
		ObservationMaxAge:        desired.CapacityPolicy.ObservationMaxAge,
	}
	if !reflect.DeepEqual(capacity.policies, []fleet.CapacityPolicy{wantCapacityPolicy}) {
		t.Fatalf("configured Fleet capacity policies = %#v, want %#v", capacity.policies, wantCapacityPolicy)
	}
	workerPool := resources.createdWorkerPool
	assertProtectedMetadata(t, workerPool.Metadata, desired)
	if workerPool.Spec.Revision != desired.Revision || workerPool.Spec.WorkerProfile != "h3" ||
		!reflect.DeepEqual(workerPool.Spec.Placements, desired.Placements) ||
		!reflect.DeepEqual(workerPool.Spec.NodeSelector, desired.NodeSelector) ||
		!reflect.DeepEqual(workerPool.Spec.CapacityPolicy, desired.CapacityPolicy) {
		t.Fatalf("materialized WorkerPool = %#v", workerPool)
	}
	if len(resources.createdDaemonSets) != 2 {
		t.Fatalf("materialized DaemonSets = %#v", resources.createdDaemonSets)
	}
	daemonSet := resources.createdDaemonSets[0]
	placement := desired.Placements[0]
	assertProtectedMetadata(t, daemonSet.Metadata, desired)
	if daemonSet.UpdateStrategy != "OnDelete" ||
		daemonSet.Metadata.Name != placement.DaemonSetName ||
		daemonSet.Template.Labels["app.kubernetes.io/name"] != daemonSet.Selector["app.kubernetes.io/name"] ||
		daemonSet.Template.Labels["vela.ai/fleet-protected"] != "true" ||
		daemonSet.Template.Labels["vela.ai/worker-pool-id"] != desired.WorkerPoolID.String() ||
		daemonSet.Template.Labels["vela.ai/fleet-revision"] != desired.Revision ||
		!reflect.DeepEqual(daemonSet.Template.Finalizers, []string{"fleet.vela.ai/drain-protection"}) ||
		daemonSet.Template.Spec.NodeSelector["kubernetes.io/hostname"] != placement.NodeIdentity ||
		daemonSet.Template.Spec.NodeSelector["vela.ai/worker-pool"] != desired.NodeSelector["vela.ai/worker-pool"] ||
		!reflect.DeepEqual(
			daemonSet.Template.Spec.SchedulingGates,
			[]corev1.PodSchedulingGate{{Name: fleetcontroller.IdentityBindingSchedulingGate}},
		) ||
		!hasH3Toleration(daemonSet.Template.Spec.Tolerations) ||
		daemonSet.Template.Spec.AutomountServiceAccountToken == nil ||
		*daemonSet.Template.Spec.AutomountServiceAccountToken ||
		daemonSet.Template.Spec.TerminationGracePeriodSeconds == nil ||
		*daemonSet.Template.Spec.TerminationGracePeriodSeconds != 120 ||
		len(daemonSet.Template.Spec.InitContainers) != 1 ||
		len(daemonSet.Template.Spec.Volumes) != 9 {
		t.Fatalf("materialized DaemonSet placement = %#v", daemonSet)
	}
	if initializer := daemonSet.Template.Spec.InitContainers[0]; initializer.Image != desired.InitImage || initializer.Name != "runner-socket-permissions" {
		t.Fatalf("materialized H3 initializer = %#v", initializer)
	}
	runner := requireFleetContainer(t, daemonSet.Template.Spec.Containers, "h3-runner")
	runnerGPURequest := runner.Resources.Requests[corev1.ResourceName("nvidia.com/gpu")]
	runnerGPULimit := runner.Resources.Limits[corev1.ResourceName("nvidia.com/gpu")]
	if runner.Image != desired.RunnerImage ||
		runnerGPURequest.Value() != 8 || runnerGPULimit.Value() != 8 ||
		requireFleetEnvironment(t, runner, "VELA_RUNNER_BACKEND_REVISION").Value !=
			desired.InferenceBackendRevision {
		t.Fatalf("materialized H3 runner = %#v", runner)
	}
	agent := requireFleetContainer(t, daemonSet.Template.Spec.Containers, "worker-agent")
	_, agentRequestsGPU := agent.Resources.Requests[corev1.ResourceName("nvidia.com/gpu")]
	_, agentLimitsGPU := agent.Resources.Limits[corev1.ResourceName("nvidia.com/gpu")]
	workerIDEnvironment := requireFleetEnvironment(t, agent, "VELA_WORKER_ID")
	workerEpochEnvironment := requireFleetEnvironment(t, agent, "VELA_WORKER_EPOCH")
	nodeEnvironment := requireFleetEnvironment(t, agent, "VELA_WORKER_NODE_IDENTITY")
	runtimeEnvironment := requireFleetEnvironment(t, agent, "VELA_WORKER_CONTROL_ADDRESS")
	capacityReportInterval := requireFleetEnvironment(
		t, agent, "VELA_WORKER_CAPACITY_REPORT_INTERVAL",
	)
	if agent.Image != desired.WorkerAgentImage || agentRequestsGPU || agentLimitsGPU ||
		workerIDEnvironment.ValueFrom == nil || workerIDEnvironment.ValueFrom.FieldRef == nil ||
		workerIDEnvironment.ValueFrom.FieldRef.FieldPath != "metadata.labels['vela.ai/worker-id']" ||
		workerEpochEnvironment.ValueFrom == nil || workerEpochEnvironment.ValueFrom.FieldRef == nil ||
		workerEpochEnvironment.ValueFrom.FieldRef.FieldPath != "metadata.labels['vela.ai/worker-epoch']" ||
		nodeEnvironment.ValueFrom == nil || nodeEnvironment.ValueFrom.FieldRef == nil ||
		nodeEnvironment.ValueFrom.FieldRef.FieldPath != "spec.nodeName" ||
		runtimeEnvironment.ValueFrom == nil || runtimeEnvironment.ValueFrom.ConfigMapKeyRef == nil ||
		runtimeEnvironment.ValueFrom.ConfigMapKeyRef.Name != placement.WorkerRuntimeConfigMap ||
		capacityReportInterval.Value != "40s" {
		t.Fatalf("materialized Worker Agent = %#v", agent)
	}
	second := resources.createdDaemonSets[1]
	if second.Metadata.Name != desired.Placements[1].DaemonSetName ||
		second.Template.Spec.NodeSelector["kubernetes.io/hostname"] != desired.Placements[1].NodeIdentity ||
		requireFleetEnvironment(
			t,
			requireFleetContainer(t, second.Template.Spec.Containers, "worker-agent"),
			"VELA_WORKER_CONTROL_ADDRESS",
		).ValueFrom.ConfigMapKeyRef.Name != desired.Placements[1].WorkerRuntimeConfigMap {
		t.Fatalf("second materialized DaemonSet placement = %#v", second)
	}
}

func TestReconcileFailsClosedWhenAuthoritativeCapacityPolicyCannotBeConfigured(t *testing.T) {
	policyFailure := errors.New("capacity authority unavailable")
	operations := []string{}
	resources := &recordingResources{sharedOperations: &operations}
	capacity := &staticCapacityPolicyConfigurator{
		err: policyFailure, sharedOperations: &operations,
	}
	reconciler, err := fleetcontroller.NewReconciler(
		resources,
		&staticDrainReader{},
		&staticIdentityResolver{},
		&staticReadinessStarter{},
		capacity,
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}
	result, err := reconciler.Reconcile(context.Background(), desiredRevision())
	if !errors.Is(err, policyFailure) {
		t.Fatalf("capacity policy reconciliation error = %v", err)
	}
	if !result.WorkerPoolCreated || !result.DaemonSetCreated ||
		result.CapacityPolicyConfigured || result.Converged {
		t.Fatalf("failed capacity policy reconciliation result = %#v", result)
	}
	if operations[len(operations)-1] != "configure-capacity-policy" {
		t.Fatalf("failed capacity policy authority order = %#v", operations)
	}
}

func TestReconcileRejectsSameWorkerPoolNameWithDifferentImmutableRevision(t *testing.T) {
	desired := desiredRevision()
	existing := fleetcontroller.WorkerPool{
		Metadata: fleetcontroller.Metadata{
			Namespace: desired.Namespace,
			Name:      desired.Name,
			Labels: map[string]string{
				"vela.ai/fleet-protected": "true",
				"vela.ai/worker-pool-id":  desired.WorkerPoolID.String(),
				"vela.ai/fleet-revision":  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			},
			Finalizers: []string{"fleet.vela.ai/drain-protection"},
		},
		Spec: fleetcontroller.WorkerPoolSpec{
			Revision:      "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			WorkerProfile: "h3", NodeSelector: desired.NodeSelector,
			Placements: desired.Placements,
		},
	}
	resources := &recordingResources{workerPool: existing}
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
	_, err = reconciler.Reconcile(context.Background(), desired)
	if !errors.Is(err, fleetcontroller.ErrImmutableDesiredRevision) {
		t.Fatalf("immutable revision drift error = %v", err)
	}
	if !reflect.DeepEqual(resources.operations, []string{"get-worker-pool"}) {
		t.Fatalf("immutable revision drift operations = %#v", resources.operations)
	}
}

func TestRetireWorkerPodDoesNotMutateWithoutCompletedExactDrain(t *testing.T) {
	drainID := uuid.MustParse("23000000-0000-0000-0000-000000000042")
	pod := protectedWorkerPod()
	tests := []struct {
		name  string
		drain fleet.DrainResult
	}{
		{
			name: "still draining",
			drain: fleet.DrainResult{
				OperationID: drainID, State: fleet.DrainDraining,
				WorkerID: pod.WorkerID, WorkerEpoch: pod.WorkerEpoch,
			},
		},
		{
			name: "different Worker epoch",
			drain: fleet.DrainResult{
				OperationID: drainID, State: fleet.DrainComplete,
				WorkerID: pod.WorkerID, WorkerEpoch: pod.WorkerEpoch + 1,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resources := &recordingResources{}
			reconciler, err := fleetcontroller.NewReconciler(
				resources,
				&staticDrainReader{result: test.drain},
				&staticIdentityResolver{},
				&staticReadinessStarter{},
				&staticCapacityPolicyConfigurator{},
			)
			if err != nil {
				t.Fatalf("create Fleet reconciler: %v", err)
			}
			if err := reconciler.RetireWorkerPod(context.Background(), pod, drainID); err == nil {
				t.Fatal("undrained Worker Pod retirement succeeded")
			}
			if len(resources.operations) != 0 {
				t.Fatalf("undrained Worker Pod operations = %#v", resources.operations)
			}
		})
	}
}

func TestRetireWorkerPodRemovesFinalizerOnlyAfterAuthorizedDelete(t *testing.T) {
	drainID := uuid.MustParse("23000000-0000-0000-0000-000000000042")
	pod := protectedWorkerPod()
	drain := fleet.DrainResult{
		OperationID: drainID, State: fleet.DrainComplete,
		WorkerID: pod.WorkerID, WorkerEpoch: pod.WorkerEpoch,
		Deadline: time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC),
	}
	resources := &recordingResources{absent: true}
	reconciler, err := fleetcontroller.NewReconciler(
		resources,
		&staticDrainReader{result: drain, retirementAuthorized: true},
		&staticIdentityResolver{},
		&staticReadinessStarter{},
		&staticCapacityPolicyConfigurator{},
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}
	if err := reconciler.RetireWorkerPod(context.Background(), pod, drainID); err != nil {
		t.Fatalf("retire drained Worker Pod: %v", err)
	}
	if !reflect.DeepEqual(resources.operations, []string{
		"attach-drain-operations", "delete", "remove-finalizer", "observe-absence",
	}) {
		t.Fatalf("drained Worker Pod operations = %#v", resources.operations)
	}
	if !reflect.DeepEqual(resources.attachedDrainIDs, []uuid.UUID{drainID}) {
		t.Fatalf("attached DrainOperation ids = %#v", resources.attachedDrainIDs)
	}

	deleteFailure := errors.New("admission denied delete")
	resources = &recordingResources{deleteErr: deleteFailure}
	reconciler, err = fleetcontroller.NewReconciler(
		resources,
		&staticDrainReader{result: drain},
		&staticIdentityResolver{},
		&staticReadinessStarter{},
		&staticCapacityPolicyConfigurator{},
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}
	if err := reconciler.RetireWorkerPod(context.Background(), pod, drainID); !errors.Is(err, deleteFailure) {
		t.Fatalf("denied Worker Pod delete error = %v", err)
	}
	if !reflect.DeepEqual(resources.operations, []string{
		"attach-drain-operations", "delete",
	}) {
		t.Fatalf("denied Worker Pod delete operations = %#v", resources.operations)
	}
}

func TestRuntimeCompletesExactPoolDrainSetBeforeRetiringProtectedResources(t *testing.T) {
	deadline := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	workers := []fleetcontroller.WorkerRetirement{
		{
			OperationID: uuid.MustParse("23000000-0000-0000-0000-000000000081"),
			WorkerID:    uuid.MustParse("23000000-0000-0000-0000-000000000082"),
			WorkerEpoch: 8, PodName: "h3-worker-old-node-1",
			PodKubernetesUID: "kubernetes-old-worker-pod-uid-1",
		},
		{
			OperationID: uuid.MustParse("23000000-0000-0000-0000-000000000083"),
			WorkerID:    uuid.MustParse("23000000-0000-0000-0000-000000000084"),
			WorkerEpoch: 9, PodName: "h3-worker-old-node-2",
			PodKubernetesUID: "kubernetes-old-worker-pod-uid-2",
		},
	}
	plan := fleetcontroller.RetirementPlan{
		Revision:     "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		WorkerPoolID: uuid.MustParse("23000000-0000-0000-0000-000000000085"),
		Namespace:    "vela-system", WorkerPoolName: "h3-worker-pool-old",
		WorkerPoolKubernetesUID: "kubernetes-old-worker-pool-uid",
		Reason:                  "retire immutable H3 revision eeee", Deadline: deadline,
		Placements: []fleetcontroller.RetirementPlacement{
			{
				DaemonSetName: "h3-worker-pool-old-node-1", DaemonSetKubernetesUID: "kubernetes-old-daemonset-uid-1",
				Workers: workers[:1],
			},
			{
				DaemonSetName: "h3-worker-pool-old-node-2", DaemonSetKubernetesUID: "kubernetes-old-daemonset-uid-2",
				Workers: workers[1:],
			},
		},
	}
	resources := &recordingResources{}
	drains := &recordingDrainCoordinator{state: fleet.DrainDraining, retirementAuthorized: true}
	reconciler, err := fleetcontroller.NewReconciler(
		resources,
		drains,
		&staticIdentityResolver{},
		&staticReadinessStarter{},
		&staticCapacityPolicyConfigurator{},
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}
	runtimeController, err := fleetcontroller.NewRuntime(
		staticPendingWorkerPodLister{},
		reconciler,
		fleetcontroller.RuntimeConfig{
			DesiredRevisions: []fleetcontroller.DesiredRevision{desiredRevision()},
			RetirementPlans:  []fleetcontroller.RetirementPlan{plan},
			PollInterval:     time.Second,
		},
	)
	if err != nil {
		t.Fatalf("create Fleet runtime: %v", err)
	}

	result, err := runtimeController.RunOnce(context.Background())
	if err != nil || result.RetirementPlansPending != 1 ||
		result.RetirementPlansCompleted != 0 || result.WorkerPodsRetired != 0 {
		t.Fatalf("pending Fleet retirement = %#v error=%v", result, err)
	}
	if len(resources.deletedResources) != 0 || runtimeController.Converged() {
		t.Fatalf("pending drain mutated resources = %#v converged=%t", resources.deletedResources, runtimeController.Converged())
	}
	if len(drains.requests) != len(workers) || len(drains.reconciled) != len(workers) {
		t.Fatalf("pending drain requests=%#v reconciled=%#v", drains.requests, drains.reconciled)
	}
	for index, request := range drains.requests {
		if request.OperationID != workers[index].OperationID ||
			request.WorkerID != workers[index].WorkerID ||
			request.ExpectedEpoch != workers[index].WorkerEpoch ||
			request.Reason != plan.Reason || !request.Deadline.Equal(deadline) {
			t.Fatalf("drain request %d = %#v", index, request)
		}
	}

	drains.state = fleet.DrainComplete
	result, err = runtimeController.RunOnce(context.Background())
	if err != nil || result.RetirementPlansPending != 1 ||
		result.RetirementPlansCompleted != 0 || result.WorkerPodsRetired != 0 {
		t.Fatalf("terminating Fleet retirement = %#v error=%v", result, err)
	}
	if len(drains.completionRecords) != 0 || len(resources.absenceRequests) != 1 ||
		resources.absenceRequests[0].Kind != fleetcontroller.ResourceDaemonSet ||
		runtimeController.Converged() {
		t.Fatalf("terminating retirement receipts=%#v absence=%#v converged=%t",
			drains.completionRecords, resources.absenceRequests, runtimeController.Converged())
	}

	resources.absent = true
	result, err = runtimeController.RunOnce(context.Background())
	if err != nil || result.RetirementPlansPending != 0 ||
		result.RetirementPlansCompleted != 1 || result.WorkerPodsRetired != 2 {
		t.Fatalf("completed Fleet retirement = %#v error=%v", result, err)
	}
	wantDeleted := []fleetcontroller.ResourceKind{
		fleetcontroller.ResourceDaemonSet,
		fleetcontroller.ResourceDaemonSet,
		fleetcontroller.ResourcePod,
		fleetcontroller.ResourceDaemonSet,
		fleetcontroller.ResourcePod,
		fleetcontroller.ResourceWorkerPool,
	}
	if len(resources.deletedResources) != len(wantDeleted) {
		t.Fatalf("retired protected resources = %#v", resources.deletedResources)
	}
	for index, kind := range wantDeleted {
		if resources.deletedResources[index].Kind != kind {
			t.Fatalf("retired protected resource %d = %#v, want %s", index, resources.deletedResources[index], kind)
		}
	}
	if !runtimeController.Converged() {
		t.Fatal("Fleet runtime did not converge after exact retirement plan completed")
	}
	if len(drains.completionRecords) != 5 {
		t.Fatalf("Fleet retirement completion receipts = %#v", drains.completionRecords)
	}
	wantAttachedDrainIDs := map[string][]uuid.UUID{
		plan.Placements[0].DaemonSetName: {workers[0].OperationID},
		plan.Placements[1].DaemonSetName: {workers[1].OperationID},
		plan.WorkerPoolName:              {workers[0].OperationID, workers[1].OperationID},
	}
	for name, want := range wantAttachedDrainIDs {
		if got := resources.lastAttachedDrainIDs(name); !reflect.DeepEqual(got, want) {
			t.Fatalf("retirement drain IDs attached to %s = %#v, want %#v", name, got, want)
		}
	}
	if len(drains.requests) != 3*len(workers) || drains.requests[0] != drains.requests[2] ||
		drains.requests[1] != drains.requests[3] || drains.requests[0] != drains.requests[4] ||
		drains.requests[1] != drains.requests[5] {
		t.Fatalf("Fleet drain replay changed immutable requests = %#v", drains.requests)
	}
}

func TestMultiPlacementRetirementDoesNotMutateBeforeEveryDrainCompletes(t *testing.T) {
	plan := retirementPlanForValidation(1)
	second := plan.Placements[0]
	second.DaemonSetName = "h3-worker-pool-old-node-b"
	second.DaemonSetKubernetesUID = "kubernetes-daemonset-old-b-uid"
	second.Workers = []fleetcontroller.WorkerRetirement{{
		OperationID: uuid.MustParse("23000000-0000-0000-0000-0000000000a1"),
		WorkerID:    uuid.MustParse("23000000-0000-0000-0000-0000000000a2"),
		WorkerEpoch: 4, PodName: "h3-worker-old-node-b",
		PodKubernetesUID: "kubernetes-old-worker-pod-b-uid",
	}}
	plan.Placements = append(plan.Placements, second)
	resources := &recordingResources{}
	drains := &recordingDrainCoordinator{states: map[uuid.UUID]fleet.DrainState{
		plan.Placements[0].Workers[0].OperationID: fleet.DrainComplete,
		plan.Placements[1].Workers[0].OperationID: fleet.DrainDraining,
	}}
	reconciler, err := fleetcontroller.NewReconciler(
		resources, drains, &staticIdentityResolver{}, &staticReadinessStarter{},
		&staticCapacityPolicyConfigurator{},
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}
	result, err := reconciler.ReconcileRetirement(context.Background(), plan)
	if err != nil || !result.Pending || result.Completed {
		t.Fatalf("multi-placement retirement = %#v error=%v", result, err)
	}
	if len(resources.deletedResources) != 0 || len(resources.absenceRequests) != 0 {
		t.Fatalf("partial multi-placement drain mutated resources: deletes=%#v absence=%#v",
			resources.deletedResources, resources.absenceRequests)
	}
}

func TestMultiPlacementRetirementReplaysReceiptsBeforeDeletingWorkerPool(t *testing.T) {
	plan := retirementPlanForValidation(1)
	plan.Placements = append(plan.Placements, retirementPlanForValidation(2).Placements[0])
	first := plan.Placements[0]
	resources := &recordingResources{}
	drains := &recordingDrainCoordinator{
		state:                fleet.DrainComplete,
		retirementAuthorized: true,
		completionByTarget: map[string]bool{
			string(fleet.ProtectedDaemonSet) + "/" + first.DaemonSetKubernetesUID: true,
			string(fleet.ProtectedPod) + "/" + first.Workers[0].PodKubernetesUID:  true,
		},
	}
	reconciler, err := fleetcontroller.NewReconciler(
		resources, drains, &staticIdentityResolver{}, &staticReadinessStarter{},
		&staticCapacityPolicyConfigurator{},
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}
	result, err := reconciler.ReconcileRetirement(context.Background(), plan)
	if err != nil || !result.Pending || result.Completed {
		t.Fatalf("staged multi-placement retirement = %#v error=%v", result, err)
	}
	for _, resource := range append(
		append([]fleetcontroller.ProtectedResource(nil), resources.attachedResources...),
		resources.deletedResources...,
	) {
		if resource.Kind == fleetcontroller.ResourceWorkerPool {
			t.Fatalf("WorkerPool was mutated before every placement completed: %#v", resource)
		}
	}
	if len(resources.attachedResources) != 1 ||
		resources.attachedResources[0].Name != plan.Placements[1].DaemonSetName ||
		!reflect.DeepEqual(
			resources.attachedIDSets[0],
			[]uuid.UUID{plan.Placements[1].Workers[0].OperationID},
		) {
		t.Fatalf("staged retirement attachment = %#v / %#v",
			resources.attachedResources, resources.attachedIDSets)
	}
}

func TestRuntimeNeverRetiresProtectedResourcesForExpiredDrain(t *testing.T) {
	worker := fleetcontroller.WorkerRetirement{
		OperationID: uuid.MustParse("23000000-0000-0000-0000-000000000091"),
		WorkerID:    uuid.MustParse("23000000-0000-0000-0000-000000000092"),
		WorkerEpoch: 3, PodName: "h3-worker-expired",
		PodKubernetesUID: "kubernetes-expired-worker-pod-uid",
	}
	plan := fleetcontroller.RetirementPlan{
		Revision:     "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		WorkerPoolID: uuid.MustParse("23000000-0000-0000-0000-000000000093"),
		Namespace:    "vela-system", WorkerPoolName: "h3-worker-pool-expired",
		WorkerPoolKubernetesUID: "kubernetes-expired-worker-pool-uid",
		Reason:                  "expired routine retirement must fail closed",
		Deadline:                time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
		Placements: []fleetcontroller.RetirementPlacement{{
			DaemonSetName: "h3-worker-pool-expired", DaemonSetKubernetesUID: "kubernetes-expired-daemonset-uid",
			Workers: []fleetcontroller.WorkerRetirement{worker},
		}},
	}
	resources := &recordingResources{}
	reconciler, err := fleetcontroller.NewReconciler(
		resources,
		&recordingDrainCoordinator{state: fleet.DrainExpired},
		&staticIdentityResolver{},
		&staticReadinessStarter{},
		&staticCapacityPolicyConfigurator{},
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}
	runtimeController, err := fleetcontroller.NewRuntime(
		staticPendingWorkerPodLister{},
		reconciler,
		fleetcontroller.RuntimeConfig{
			DesiredRevisions: []fleetcontroller.DesiredRevision{desiredRevision()},
			RetirementPlans:  []fleetcontroller.RetirementPlan{plan},
			PollInterval:     time.Second,
		},
	)
	if err != nil {
		t.Fatalf("create Fleet runtime: %v", err)
	}
	if _, err := runtimeController.RunOnce(context.Background()); !errors.Is(err, fleetcontroller.ErrDrainExpired) {
		t.Fatalf("expired Fleet retirement error = %v", err)
	}
	if len(resources.deletedResources) != 0 || runtimeController.Converged() {
		t.Fatalf("expired drain mutated resources = %#v converged=%t", resources.deletedResources, runtimeController.Converged())
	}
}

func TestRuntimeNeverInfersRetirementFromAbsenceWithoutPersistedCompletion(t *testing.T) {
	worker := fleetcontroller.WorkerRetirement{
		OperationID:      uuid.MustParse("23000000-0000-0000-0000-0000000000a1"),
		WorkerID:         uuid.MustParse("23000000-0000-0000-0000-0000000000a2"),
		WorkerEpoch:      4,
		PodName:          "h3-worker-absent",
		PodKubernetesUID: "kubernetes-absent-worker-pod-uid",
	}
	plan := fleetcontroller.RetirementPlan{
		Revision:                "abababababababababababababababababababababababababababababababab",
		WorkerPoolID:            uuid.MustParse("23000000-0000-0000-0000-0000000000a3"),
		Namespace:               "vela-system",
		WorkerPoolName:          "h3-worker-pool-absent",
		WorkerPoolKubernetesUID: "kubernetes-absent-worker-pool-uid",
		Reason:                  "prove restart retirement from durable completion",
		Deadline:                time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
		Placements: []fleetcontroller.RetirementPlacement{{
			DaemonSetName: "h3-worker-pool-absent", DaemonSetKubernetesUID: "kubernetes-absent-daemonset-uid",
			Workers: []fleetcontroller.WorkerRetirement{worker},
		}},
	}
	resources := &recordingResources{attachErr: fleetcontroller.ErrResourceNotFound, absent: true}
	drains := &recordingDrainCoordinator{state: fleet.DrainComplete}
	reconciler, err := fleetcontroller.NewReconciler(
		resources,
		drains,
		&staticIdentityResolver{},
		&staticReadinessStarter{},
		&staticCapacityPolicyConfigurator{},
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}
	runtimeController, err := fleetcontroller.NewRuntime(
		staticPendingWorkerPodLister{},
		reconciler,
		fleetcontroller.RuntimeConfig{
			DesiredRevisions: []fleetcontroller.DesiredRevision{desiredRevision()},
			RetirementPlans:  []fleetcontroller.RetirementPlan{plan},
			PollInterval:     time.Second,
		},
	)
	if err != nil {
		t.Fatalf("create Fleet runtime: %v", err)
	}
	result, err := runtimeController.RunOnce(context.Background())
	if !errors.Is(err, fleetcontroller.ErrRetirementUnproven) ||
		result.RetirementPlansCompleted != 0 || runtimeController.Converged() {
		t.Fatalf("unproven absent retirement = %#v error=%v", result, err)
	}
	if len(drains.completionRequests) != 1 || len(drains.authorizationRequests) != 1 ||
		drains.completionRequests[0].ResourceKind != fleet.ProtectedDaemonSet ||
		drains.completionRequests[0].KubernetesUID != plan.Placements[0].DaemonSetKubernetesUID {
		t.Fatalf("absent retirement completion lookups = %#v", drains.completionRequests)
	}
	drains.retirementAuthorized = true
	completionFailure := errors.New("completion database unavailable")
	drains.completionErr = completionFailure
	result, err = runtimeController.RunOnce(context.Background())
	if !errors.Is(err, completionFailure) ||
		result.RetirementPlansCompleted != 0 || runtimeController.Converged() {
		t.Fatalf("unpersisted absent retirement = %#v error=%v", result, err)
	}
	if len(drains.completionRequests) != 2 ||
		drains.completionRequests[1].ResourceKind != fleet.ProtectedDaemonSet ||
		len(drains.authorizationRequests) != 2 || len(drains.completionRecords) != 1 {
		t.Fatalf("unpersisted absent retirement lookups = %#v authorization=%#v records=%#v",
			drains.completionRequests, drains.authorizationRequests, drains.completionRecords)
	}
	drains.completionErr = nil
	result, err = runtimeController.RunOnce(context.Background())
	if err != nil || result.RetirementPlansCompleted != 1 ||
		result.WorkerPodsRetired != 1 || !runtimeController.Converged() {
		t.Fatalf("completion-proven absent retirement = %#v error=%v", result, err)
	}
	if len(drains.completionRequests) != 5 || len(drains.completionRecords) != 4 ||
		drains.completionRequests[2].ResourceKind != fleet.ProtectedDaemonSet ||
		drains.completionRequests[3].ResourceKind != fleet.ProtectedPod ||
		drains.completionRequests[3].WorkerID != worker.WorkerID ||
		drains.completionRequests[4].ResourceKind != fleet.ProtectedWorkerPool {
		t.Fatalf("completion-proven absent retirement lookups = %#v", drains.completionRequests)
	}
	operationsAfterCompletion := countRetirementOperations(resources.operations)
	result, err = runtimeController.RunOnce(context.Background())
	if err != nil || result.RetirementPlansCompleted != 1 ||
		len(drains.completionRecords) != 4 ||
		countRetirementOperations(resources.operations) != operationsAfterCompletion {
		t.Fatalf("durable completion replay = %#v records=%#v operations=%#v error=%v",
			result, drains.completionRecords, resources.operations, err)
	}
}

func TestRuntimeReceiptsOldUIDAbsenceWithoutTouchingReplacement(t *testing.T) {
	plan := retirementPlanForValidation(1)
	resources := &recordingResources{
		attachErr: fleetcontroller.ErrProtectedResourceDrift,
		absent:    true,
	}
	drains := &recordingDrainCoordinator{
		state:                fleet.DrainComplete,
		retirementAuthorized: true,
	}
	reconciler, err := fleetcontroller.NewReconciler(
		resources,
		drains,
		&staticIdentityResolver{},
		&staticReadinessStarter{},
		&staticCapacityPolicyConfigurator{},
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}

	result, err := reconciler.ReconcileRetirement(context.Background(), plan)
	if err != nil || !result.Completed || result.Pending || result.WorkerPodsRetired != 1 {
		t.Fatalf("replacement-safe retirement = %#v error=%v", result, err)
	}
	if len(resources.deletedResources) != 0 {
		t.Fatalf("replacement resources were deleted: %#v", resources.deletedResources)
	}
	if len(drains.completionRecords) != 3 {
		t.Fatalf("old UID retirement completion receipts = %#v", drains.completionRecords)
	}
	for _, receipt := range drains.completionRecords {
		if receipt.KubernetesUID == "" {
			t.Fatalf("retirement completion omitted old Kubernetes UID: %#v", receipt)
		}
	}
}

func countRetirementOperations(operations []string) int {
	count := 0
	for _, operation := range operations {
		switch operation {
		case "attach-drain-operations", "delete", "remove-finalizer", "observe-absence":
			count++
		}
	}
	return count
}

func TestFleetConfigurationRejectsKubernetesNamesOutsideCRDContract(t *testing.T) {
	for _, invalidName := range []string{".pool", "pool.", "a..b", "worker-pool-placeholder"} {
		desired := desiredRevision()
		desired.Name = invalidName
		if err := fleetcontroller.ValidateDesiredRevision(desired); err == nil {
			t.Fatalf("invalid Kubernetes resource name %q was accepted", invalidName)
		}
	}
}

func TestFleetDesiredRevisionRejectsPlacementsOutsideBoundedLabelContract(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fleetcontroller.DesiredRevision)
	}{
		{
			name: "more than 1024 placements",
			mutate: func(desired *fleetcontroller.DesiredRevision) {
				placement := desired.Placements[0]
				desired.Placements = make([]fleetcontroller.WorkerPlacement, 1025)
				for index := range desired.Placements {
					desired.Placements[index] = placement
					desired.Placements[index].NodeIdentity = fmt.Sprintf("h3-node-%d", index)
					desired.Placements[index].DaemonSetName = fmt.Sprintf("h3-worker-%d", index)
					desired.Placements[index].WorkerRuntimeConfigMap = fmt.Sprintf("runtime-%d", index)
					desired.Placements[index].RunnerProfilesConfigMap = fmt.Sprintf("profiles-%d", index)
					desired.Placements[index].RunnerGPURolesConfigMap = fmt.Sprintf("gpu-roles-%d", index)
					desired.Placements[index].WorkerControlTLSSecret = fmt.Sprintf("worker-tls-%d", index)
				}
			},
		},
		{
			name: "node identity is a DNS subdomain but exceeds label value length",
			mutate: func(desired *fleetcontroller.DesiredRevision) {
				desired.Placements[0].NodeIdentity = strings.Repeat("a", 63) + ".b"
			},
		},
		{
			name: "DaemonSet name exceeds label value length",
			mutate: func(desired *fleetcontroller.DesiredRevision) {
				desired.Placements[0].DaemonSetName = strings.Repeat("a", 64)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			desired := desiredRevision()
			test.mutate(&desired)
			if err := fleetcontroller.ValidateDesiredRevision(desired); err == nil {
				t.Fatal("invalid placement contract was accepted")
			}
		})
	}
}

func TestFleetDesiredRevisionRejectsInvalidOrUnboundedPoolSelector(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fleetcontroller.DesiredRevision)
	}{
		{name: "invalid qualified label key", mutate: func(desired *fleetcontroller.DesiredRevision) {
			desired.NodeSelector["bad key"] = "value"
		}},
		{name: "invalid label value", mutate: func(desired *fleetcontroller.DesiredRevision) {
			desired.NodeSelector["vela.ai/rack"] = "bad/value"
		}},
		{name: "explicit empty hostname selector", mutate: func(desired *fleetcontroller.DesiredRevision) {
			desired.NodeSelector[corev1.LabelHostname] = ""
		}},
		{name: "more than 64 selector entries", mutate: func(desired *fleetcontroller.DesiredRevision) {
			for index := 0; index < 63; index++ {
				desired.NodeSelector[fmt.Sprintf("vela.ai/selector-%d", index)] = "value"
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			desired := desiredRevision()
			test.mutate(&desired)
			if err := fleetcontroller.ValidateDesiredRevision(desired); err == nil {
				t.Fatal("invalid pool selector was accepted")
			}
		})
	}
}

func TestFleetDesiredRevisionRejectsZeroAndTemplateReleaseInputs(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fleetcontroller.DesiredRevision)
	}{
		{name: "zero revision", mutate: func(desired *fleetcontroller.DesiredRevision) {
			desired.Revision = strings.Repeat("0", 64)
		}},
		{name: "zero init image digest", mutate: func(desired *fleetcontroller.DesiredRevision) {
			desired.InitImage = "docker.io/library/busybox@sha256:" + strings.Repeat("0", 64)
		}},
		{name: "tagged image", mutate: func(desired *fleetcontroller.DesiredRevision) {
			desired.RunnerImage = "ghcr.io/vivym/vela-h3-runner:r1@sha256:" + strings.Repeat("c", 64)
		}},
		{name: "uppercase image", mutate: func(desired *fleetcontroller.DesiredRevision) {
			desired.WorkerAgentImage = "ghcr.io/vivym/Vela-worker-agent@sha256:" + strings.Repeat("b", 64)
		}},
		{name: "template backend revision", mutate: func(desired *fleetcontroller.DesiredRevision) {
			desired.InferenceBackendRevision = "replace-with-approved-backend-revision"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			desired := desiredRevision()
			test.mutate(&desired)
			if err := fleetcontroller.ValidateDesiredRevision(desired); err == nil {
				t.Fatal("invalid production desired revision was accepted")
			}
		})
	}
}

func TestRetirementPlanRejectsMoreThanTransportMaximumWorkers(t *testing.T) {
	plan := retirementPlanForValidation(1)
	worker := plan.Placements[0].Workers[0]
	plan.Placements[0].Workers = make([]fleetcontroller.WorkerRetirement, 4097)
	for index := range plan.Placements[0].Workers {
		plan.Placements[0].Workers[index] = worker
	}
	if err := fleetcontroller.ValidateRetirementPlan(plan); err == nil ||
		err.Error() != "fleet retirement plan is invalid" {
		t.Fatalf("oversized retirement plan error = %v", err)
	}
}

func TestRetirementPlanRejectsMalformedOrAliasedPlacements(t *testing.T) {
	base := retirementPlanForValidation(1)
	second := retirementPlanForValidation(2).Placements[0]
	base.Placements = append(base.Placements, second)
	tests := []struct {
		name   string
		mutate func(*fleetcontroller.RetirementPlan)
	}{
		{name: "placement has no Workers", mutate: func(plan *fleetcontroller.RetirementPlan) {
			plan.Placements[1].Workers = nil
		}},
		{name: "duplicate DaemonSet name", mutate: func(plan *fleetcontroller.RetirementPlan) {
			plan.Placements[1].DaemonSetName = plan.Placements[0].DaemonSetName
		}},
		{name: "duplicate DaemonSet UID", mutate: func(plan *fleetcontroller.RetirementPlan) {
			plan.Placements[1].DaemonSetKubernetesUID = plan.Placements[0].DaemonSetKubernetesUID
		}},
		{name: "DaemonSet reuses WorkerPool UID", mutate: func(plan *fleetcontroller.RetirementPlan) {
			plan.Placements[1].DaemonSetKubernetesUID = plan.WorkerPoolKubernetesUID
		}},
		{name: "duplicate DrainOperation id", mutate: func(plan *fleetcontroller.RetirementPlan) {
			plan.Placements[1].Workers[0].OperationID = plan.Placements[0].Workers[0].OperationID
		}},
		{name: "duplicate Worker id", mutate: func(plan *fleetcontroller.RetirementPlan) {
			plan.Placements[1].Workers[0].WorkerID = plan.Placements[0].Workers[0].WorkerID
		}},
		{name: "duplicate Pod name", mutate: func(plan *fleetcontroller.RetirementPlan) {
			plan.Placements[1].Workers[0].PodName = plan.Placements[0].Workers[0].PodName
		}},
		{name: "Pod reuses DaemonSet UID", mutate: func(plan *fleetcontroller.RetirementPlan) {
			plan.Placements[1].Workers[0].PodKubernetesUID = plan.Placements[0].DaemonSetKubernetesUID
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := base
			plan.Placements = append([]fleetcontroller.RetirementPlacement(nil), base.Placements...)
			for index := range plan.Placements {
				plan.Placements[index].Workers = append(
					[]fleetcontroller.WorkerRetirement(nil), base.Placements[index].Workers...,
				)
			}
			test.mutate(&plan)
			if err := fleetcontroller.ValidateRetirementPlan(plan); err == nil {
				t.Fatal("invalid multi-placement retirement plan was accepted")
			}
		})
	}
}

func TestRuntimeConfigurationRejectsOverlappingDesiredAndRetirementIdentities(t *testing.T) {
	desired := desiredRevision()
	first := retirementPlanForValidation(1)
	second := retirementPlanForValidation(2)
	tests := []struct {
		name   string
		mutate func(*fleetcontroller.RetirementPlan, *fleetcontroller.RetirementPlan)
	}{
		{
			name: "desired WorkerPool id",
			mutate: func(first, _ *fleetcontroller.RetirementPlan) {
				first.WorkerPoolID = desired.WorkerPoolID
			},
		},
		{
			name: "desired WorkerPool name",
			mutate: func(first, _ *fleetcontroller.RetirementPlan) {
				first.WorkerPoolName = desired.Name
			},
		},
		{
			name: "desired DaemonSet name",
			mutate: func(first, _ *fleetcontroller.RetirementPlan) {
				first.Placements[0].DaemonSetName = desired.Placements[0].DaemonSetName
			},
		},
		{
			name: "retirement WorkerPool id",
			mutate: func(first, second *fleetcontroller.RetirementPlan) {
				second.WorkerPoolID = first.WorkerPoolID
			},
		},
		{
			name: "retirement WorkerPool UID",
			mutate: func(first, second *fleetcontroller.RetirementPlan) {
				second.WorkerPoolKubernetesUID = first.WorkerPoolKubernetesUID
			},
		},
		{
			name: "retirement DaemonSet UID",
			mutate: func(first, second *fleetcontroller.RetirementPlan) {
				second.Placements[0].DaemonSetKubernetesUID = first.Placements[0].DaemonSetKubernetesUID
			},
		},
		{
			name: "retirement DaemonSet name",
			mutate: func(first, second *fleetcontroller.RetirementPlan) {
				second.Placements[0].DaemonSetName = first.Placements[0].DaemonSetName
			},
		},
		{
			name: "retirement DaemonSet reuses Pod UID",
			mutate: func(first, second *fleetcontroller.RetirementPlan) {
				second.Placements[0].DaemonSetKubernetesUID = first.Placements[0].Workers[0].PodKubernetesUID
			},
		},
		{
			name: "retirement Worker id",
			mutate: func(first, second *fleetcontroller.RetirementPlan) {
				second.Placements[0].Workers[0].WorkerID = first.Placements[0].Workers[0].WorkerID
			},
		},
		{
			name: "retirement Pod name",
			mutate: func(first, second *fleetcontroller.RetirementPlan) {
				second.Placements[0].Workers[0].PodName = first.Placements[0].Workers[0].PodName
			},
		},
		{
			name: "retirement Pod UID",
			mutate: func(first, second *fleetcontroller.RetirementPlan) {
				second.Placements[0].Workers[0].PodKubernetesUID = first.Placements[0].Workers[0].PodKubernetesUID
			},
		},
		{
			name: "retirement DrainOperation id",
			mutate: func(first, second *fleetcontroller.RetirementPlan) {
				second.Placements[0].Workers[0].OperationID = first.Placements[0].Workers[0].OperationID
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			firstCopy := first
			firstCopy.Placements = append([]fleetcontroller.RetirementPlacement(nil), first.Placements...)
			firstCopy.Placements[0].Workers = append(
				[]fleetcontroller.WorkerRetirement(nil), first.Placements[0].Workers...,
			)
			secondCopy := second
			secondCopy.Placements = append([]fleetcontroller.RetirementPlacement(nil), second.Placements...)
			secondCopy.Placements[0].Workers = append(
				[]fleetcontroller.WorkerRetirement(nil), second.Placements[0].Workers...,
			)
			test.mutate(&firstCopy, &secondCopy)
			if err := fleetcontroller.ValidateRuntimeConfiguration(
				[]fleetcontroller.DesiredRevision{desired},
				[]fleetcontroller.RetirementPlan{firstCopy, secondCopy},
			); err == nil {
				t.Fatal("overlapping Fleet configuration was accepted")
			}
		})
	}
}

func TestBindWorkerPodIdentityUsesExactTargetNodeAndPool(t *testing.T) {
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000041")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	resources := &recordingResources{}
	resolver := &staticIdentityResolver{identity: fleet.WorkerIdentity{
		WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 20,
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
	pod := pendingWorkerPod()
	binding, err := reconciler.BindWorkerPodIdentity(context.Background(), pod, desiredRevision())
	if err != nil {
		t.Fatalf("bind Worker Pod identity: %v", err)
	}
	if !reflect.DeepEqual(resolver.requests, []fleet.WorkerIdentityRequest{{
		NodeIdentity: "h3-node-01", WorkerPoolID: poolID,
		KubernetesUID: pod.KubernetesUID, Namespace: pod.Metadata.Namespace,
		Name: pod.Metadata.Name,
	}}) {
		t.Fatalf("Worker identity resolution requests = %#v", resolver.requests)
	}
	if binding.WorkerID != workerID || binding.WorkerPoolID != poolID ||
		binding.WorkerEpoch != 20 || binding.NodeIdentity != "h3-node-01" ||
		binding.KubernetesUID != pod.KubernetesUID || binding.Namespace != pod.Metadata.Namespace ||
		binding.Name != pod.Metadata.Name {
		t.Fatalf("Worker Pod identity binding = %#v", binding)
	}
	if !reflect.DeepEqual(resources.bindings, []fleetcontroller.WorkerPodIdentityBinding{binding}) ||
		!reflect.DeepEqual(resources.operations, []string{"bind-worker-identity"}) {
		t.Fatalf("Worker Pod binding operations = %#v bindings=%#v", resources.operations, resources.bindings)
	}
}

func TestBindWorkerPodIdentityBeginsStableReadinessBeforeRemovingSchedulingGate(t *testing.T) {
	workerID := uuid.MustParse("23000000-0000-0000-0000-000000000041")
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	profileID := uuid.MustParse("23000000-0000-0000-0000-000000000043")
	cycleID := uuid.MustParse("f98bf4ac-94c4-5360-a476-cac78358adbd")
	createdAt := time.Date(2026, 8, 25, 14, 0, 0, 0, time.UTC)
	deadline := time.Date(2026, 8, 25, 14, 30, 0, 0, time.UTC)
	operations := []string{}
	resources := &recordingResources{sharedOperations: &operations}
	resolver := &staticIdentityResolver{
		identity: fleet.WorkerIdentity{
			WorkerID: workerID, WorkerPoolID: poolID, WorkerEpoch: 20,
			NodeIdentity: "h3-node-01",
		},
		sharedOperations: &operations,
	}
	readiness := &staticReadinessStarter{
		result: fleet.ReadinessResult{
			CycleID: cycleID, State: fleet.ReadinessChecking,
			NextCheck:       fleet.ReadinessIdentity,
			WorkerLifecycle: "WARMING", WorkerReachability: "SUSPECT",
		},
		sharedOperations: &operations,
	}
	reconciler, err := fleetcontroller.NewReconciler(
		resources,
		&staticDrainReader{},
		resolver,
		readiness,
		&staticCapacityPolicyConfigurator{},
	)
	if err != nil {
		t.Fatalf("create Fleet reconciler: %v", err)
	}
	pod := pendingWorkerPod()
	pod.CreatedAt = createdAt
	desired := desiredRevision()
	desired.ExecutionProfileRevisionID = profileID
	desired.InferenceBackendRevision = "sglang-h3-v3"
	desired.ReadinessTimeout = 30 * time.Minute

	binding, err := reconciler.BindWorkerPodIdentity(context.Background(), pod, desired)
	if err != nil {
		t.Fatalf("begin readiness and bind Worker Pod identity: %v", err)
	}
	if !reflect.DeepEqual(operations, []string{
		"resolve-worker-identity", "begin-readiness", "bind-worker-identity",
	}) {
		t.Fatalf("Worker readiness binding operations = %#v", operations)
	}
	if binding.WorkerID != workerID || len(readiness.requests) != 1 {
		t.Fatalf("Worker readiness binding = %#v requests=%#v", binding, readiness.requests)
	}
	request := readiness.requests[0]
	if request.CycleID != cycleID || request.WorkerID != workerID ||
		request.WorkerPoolID != poolID || request.WorkerEpoch != 20 ||
		request.NodeIdentity != "h3-node-01" ||
		request.ExecutionProfileRevisionID != profileID ||
		request.InferenceBackendRevision != "sglang-h3-v3" ||
		!request.Deadline.Equal(deadline) {
		t.Fatalf("stable Worker readiness request = %#v", request)
	}
}

func TestBindWorkerPodIdentityFailsClosedBeforePodMutation(t *testing.T) {
	poolID := uuid.MustParse("00000000-0000-0000-0000-000000000005")
	tests := []struct {
		name     string
		mutate   func(*fleetcontroller.WorkerPod)
		identity fleet.WorkerIdentity
	}{
		{
			name: "ambiguous target Node",
			mutate: func(pod *fleetcontroller.WorkerPod) {
				pod.RequiredNodeAffinity[0].MatchFields[0].Values = []string{"h3-node-01", "h3-node-02"}
			},
		},
		{
			name: "returned Node mismatch",
			identity: fleet.WorkerIdentity{
				WorkerID:     uuid.MustParse("23000000-0000-0000-0000-000000000041"),
				WorkerPoolID: poolID, WorkerEpoch: 20, NodeIdentity: "h3-node-02",
			},
		},
		{
			name: "returned pool mismatch",
			identity: fleet.WorkerIdentity{
				WorkerID:     uuid.MustParse("23000000-0000-0000-0000-000000000041"),
				WorkerPoolID: uuid.MustParse("00000000-0000-0000-0000-000000000105"),
				WorkerEpoch:  20, NodeIdentity: "h3-node-01",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resources := &recordingResources{}
			resolver := &staticIdentityResolver{identity: test.identity}
			reconciler, err := fleetcontroller.NewReconciler(
				resources,
				&staticDrainReader{},
				resolver,
				&staticReadinessStarter{},
				&staticCapacityPolicyConfigurator{},
			)
			if err != nil {
				t.Fatalf("create Fleet reconciler: %v", err)
			}
			pod := pendingWorkerPod()
			if test.mutate != nil {
				test.mutate(&pod)
			}
			if _, err := reconciler.BindWorkerPodIdentity(
				context.Background(), pod, desiredRevision(),
			); err == nil {
				t.Fatal("invalid Worker Pod identity binding succeeded")
			}
			if len(resources.operations) != 0 || len(resources.bindings) != 0 {
				t.Fatalf("invalid identity binding mutated Pod: %#v", resources)
			}
		})
	}
}

func desiredRevision() fleetcontroller.DesiredRevision {
	return fleetcontroller.DesiredRevision{
		WorkerPoolID:  uuid.MustParse("00000000-0000-0000-0000-000000000005"),
		Namespace:     "vela-system",
		Name:          "h3-worker-pool-primary",
		Revision:      "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		WorkerProfile: "h3",
		NodeSelector: map[string]string{
			"vela.ai/worker-profile": "h3",
			"vela.ai/worker-pool":    "launch",
		},
		InitImage:                  "docker.io/library/busybox@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		WorkerAgentImage:           "ghcr.io/vivym/vela-worker-agent@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		RunnerImage:                "ghcr.io/vivym/vela-h3-runner@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ArtifactStoreTLSSecret:     "vela-artifact-store-ca-a1",
		ExecutionProfileRevisionID: uuid.MustParse("23000000-0000-0000-0000-000000000043"),
		InferenceBackendRevision:   "sglang-h3-v3",
		ReadinessTimeout:           30 * time.Minute,
		CapacityPolicy: fleetcontroller.CapacityPolicySpec{
			WorkerHighWatermarkBytes: 800,
			WorkerLowWatermarkBytes:  400,
			WorkerCriticalFreeBytes:  100,
			PoolHighWatermarkBytes:   5600,
			PoolLowWatermarkBytes:    2800,
			ObservationMaxAge:        2 * time.Minute,
		},
		Placements: []fleetcontroller.WorkerPlacement{{
			NodeIdentity: "h3-node-01", DaemonSetName: "h3-worker-pool-primary-node-01",
			WorkerRuntimeConfigMap:  "vela-worker-runtime-a1",
			RunnerProfilesConfigMap: "vela-runner-profiles-a1",
			RunnerGPURolesConfigMap: "vela-runner-gpu-roles-a1",
			WorkerControlTLSSecret:  "vela-worker-control-mtls-a1",
		}},
	}
}

func retirementPlanForValidation(index int) fleetcontroller.RetirementPlan {
	if index == 1 {
		return fleetcontroller.RetirementPlan{
			Revision:     strings.Repeat("e", 64),
			WorkerPoolID: uuid.MustParse("23000000-0000-0000-0000-000000000085"),
			Namespace:    "vela-system", WorkerPoolName: "h3-worker-pool-old-a",
			WorkerPoolKubernetesUID: "kubernetes-worker-pool-old-a-uid",
			Reason:                  "retire old Fleet revision a", Deadline: time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC),
			Placements: []fleetcontroller.RetirementPlacement{{
				DaemonSetName: "h3-worker-pool-old-a", DaemonSetKubernetesUID: "kubernetes-daemonset-old-a-uid",
				Workers: []fleetcontroller.WorkerRetirement{{
					OperationID: uuid.MustParse("23000000-0000-0000-0000-000000000081"),
					WorkerID:    uuid.MustParse("23000000-0000-0000-0000-000000000082"),
					WorkerEpoch: 8, PodName: "h3-worker-old-a",
					PodKubernetesUID: "kubernetes-worker-pod-old-a-uid",
				}},
			}},
		}
	}
	return fleetcontroller.RetirementPlan{
		Revision:     strings.Repeat("f", 64),
		WorkerPoolID: uuid.MustParse("23000000-0000-0000-0000-000000000095"),
		Namespace:    "vela-system", WorkerPoolName: "h3-worker-pool-old-b",
		WorkerPoolKubernetesUID: "kubernetes-worker-pool-old-b-uid",
		Reason:                  "retire old Fleet revision b", Deadline: time.Date(2026, 8, 27, 13, 0, 0, 0, time.UTC),
		Placements: []fleetcontroller.RetirementPlacement{{
			DaemonSetName: "h3-worker-pool-old-b", DaemonSetKubernetesUID: "kubernetes-daemonset-old-b-uid",
			Workers: []fleetcontroller.WorkerRetirement{{
				OperationID: uuid.MustParse("23000000-0000-0000-0000-000000000091"),
				WorkerID:    uuid.MustParse("23000000-0000-0000-0000-000000000092"),
				WorkerEpoch: 9, PodName: "h3-worker-old-b",
				PodKubernetesUID: "kubernetes-worker-pod-old-b-uid",
			}},
		}},
	}
}

func protectedWorkerPod() fleetcontroller.ProtectedResource {
	return fleetcontroller.ProtectedResource{
		Kind:          fleetcontroller.ResourcePod,
		KubernetesUID: "kubernetes-worker-pod-uid-1",
		Namespace:     "vela-system",
		Name:          "h3-worker-node-1",
		WorkerPoolID:  uuid.MustParse("00000000-0000-0000-0000-000000000005"),
		WorkerID:      uuid.MustParse("23000000-0000-0000-0000-000000000041"),
		WorkerEpoch:   20,
	}
}

func pendingWorkerPod() fleetcontroller.WorkerPod {
	return fleetcontroller.WorkerPod{
		KubernetesUID: "kubernetes-worker-pod-uid-1",
		CreatedAt:     time.Date(2026, 8, 25, 14, 0, 0, 0, time.UTC),
		Metadata: fleetcontroller.Metadata{
			Namespace: "vela-system", Name: "h3-worker-node-1",
			Labels: map[string]string{
				"vela.ai/fleet-protected": "true",
				"vela.ai/worker-pool-id":  "00000000-0000-0000-0000-000000000005",
				"vela.ai/fleet-revision":  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
			Finalizers: []string{"fleet.vela.ai/drain-protection"},
		},
		SchedulingGates: []string{fleetcontroller.IdentityBindingSchedulingGate},
		RequiredNodeAffinity: []fleetcontroller.NodeSelectorTerm{{
			MatchFields: []fleetcontroller.NodeSelectorRequirement{{
				Key: "metadata.name", Operator: "In", Values: []string{"h3-node-01"},
			}},
		}},
	}
}

func assertProtectedMetadata(
	t *testing.T,
	metadata fleetcontroller.Metadata,
	desired fleetcontroller.DesiredRevision,
) {
	t.Helper()
	if metadata.Namespace != desired.Namespace || metadata.Name == "" ||
		metadata.Labels["vela.ai/fleet-protected"] != "true" ||
		metadata.Labels["vela.ai/worker-pool-id"] != desired.WorkerPoolID.String() ||
		metadata.Labels["vela.ai/fleet-revision"] != desired.Revision ||
		!reflect.DeepEqual(metadata.Finalizers, []string{"fleet.vela.ai/drain-protection"}) {
		t.Fatalf("protected Fleet metadata = %#v", metadata)
	}
}

func hasH3Toleration(tolerations []corev1.Toleration) bool {
	for _, toleration := range tolerations {
		if toleration.Key == "vela.ai/h3" && toleration.Operator == corev1.TolerationOpEqual &&
			toleration.Value == "true" && toleration.Effect == corev1.TaintEffectNoSchedule {
			return true
		}
	}
	return false
}

func requireFleetContainer(
	t *testing.T,
	containers []corev1.Container,
	name string,
) corev1.Container {
	t.Helper()
	for _, container := range containers {
		if container.Name == name {
			return container
		}
	}
	t.Fatalf("Fleet container %q is missing: %#v", name, containers)
	return corev1.Container{}
}

func requireFleetEnvironment(
	t *testing.T,
	container corev1.Container,
	name string,
) corev1.EnvVar {
	t.Helper()
	for _, environment := range container.Env {
		if environment.Name == name {
			return environment
		}
	}
	t.Fatalf("Fleet container %q environment %q is missing", container.Name, name)
	return corev1.EnvVar{}
}

type recordingResources struct {
	workerPool        fleetcontroller.WorkerPool
	daemonSet         fleetcontroller.DaemonSet
	daemonSets        map[string]fleetcontroller.DaemonSet
	createdWorkerPool fleetcontroller.WorkerPool
	createdDaemonSet  fleetcontroller.DaemonSet
	createdDaemonSets []fleetcontroller.DaemonSet
	attachedDrainIDs  []uuid.UUID
	attachedResources []fleetcontroller.ProtectedResource
	attachedIDSets    [][]uuid.UUID
	bindings          []fleetcontroller.WorkerPodIdentityBinding
	operations        []string
	sharedOperations  *[]string
	deleteErr         error
	attachErr         error
	absent            bool
	absenceErr        error
	absenceRequests   []fleetcontroller.ProtectedResource
	deletedResources  []fleetcontroller.ProtectedResource
}

func (resources *recordingResources) BindWorkerIdentity(
	_ context.Context,
	binding fleetcontroller.WorkerPodIdentityBinding,
) error {
	resources.operations = append(resources.operations, "bind-worker-identity")
	if resources.sharedOperations != nil {
		*resources.sharedOperations = append(*resources.sharedOperations, "bind-worker-identity")
	}
	resources.bindings = append(resources.bindings, binding)
	return nil
}

func (resources *recordingResources) GetWorkerPool(
	_ context.Context,
	_ fleetcontroller.ResourceKey,
) (fleetcontroller.WorkerPool, error) {
	resources.operations = append(resources.operations, "get-worker-pool")
	resources.recordSharedOperation("get-worker-pool")
	if resources.workerPool.Metadata.Name == "" {
		return fleetcontroller.WorkerPool{}, fleetcontroller.ErrResourceNotFound
	}
	return resources.workerPool, nil
}

func (resources *recordingResources) CreateWorkerPool(
	_ context.Context,
	workerPool fleetcontroller.WorkerPool,
) error {
	resources.operations = append(resources.operations, "create-worker-pool")
	resources.recordSharedOperation("create-worker-pool")
	resources.createdWorkerPool = workerPool
	return nil
}

func (resources *recordingResources) GetDaemonSet(
	_ context.Context,
	key fleetcontroller.ResourceKey,
) (fleetcontroller.DaemonSet, error) {
	resources.operations = append(resources.operations, "get-daemonset")
	resources.recordSharedOperation("get-daemonset")
	if daemonSet, present := resources.daemonSets[key.Name]; present {
		return daemonSet, nil
	}
	if resources.daemonSet.Metadata.Name == "" {
		return fleetcontroller.DaemonSet{}, fleetcontroller.ErrResourceNotFound
	}
	return resources.daemonSet, nil
}

func (resources *recordingResources) CreateDaemonSet(
	_ context.Context,
	daemonSet fleetcontroller.DaemonSet,
) error {
	resources.operations = append(resources.operations, "create-daemonset")
	resources.recordSharedOperation("create-daemonset")
	resources.createdDaemonSet = daemonSet
	resources.createdDaemonSets = append(resources.createdDaemonSets, daemonSet)
	return nil
}

func (resources *recordingResources) AttachDrainOperations(
	_ context.Context,
	resource fleetcontroller.ProtectedResource,
	drainIDs []uuid.UUID,
) error {
	resources.operations = append(resources.operations, "attach-drain-operations")
	resources.attachedDrainIDs = append([]uuid.UUID(nil), drainIDs...)
	resources.attachedResources = append(resources.attachedResources, resource)
	resources.attachedIDSets = append(
		resources.attachedIDSets, append([]uuid.UUID(nil), drainIDs...),
	)
	return resources.attachErr
}

func (resources *recordingResources) lastAttachedDrainIDs(name string) []uuid.UUID {
	for index := len(resources.attachedResources) - 1; index >= 0; index-- {
		if resources.attachedResources[index].Name == name {
			return resources.attachedIDSets[index]
		}
	}
	return nil
}

func (resources *recordingResources) Delete(
	_ context.Context,
	resource fleetcontroller.ProtectedResource,
) error {
	resources.operations = append(resources.operations, "delete")
	resources.deletedResources = append(resources.deletedResources, resource)
	return resources.deleteErr
}

func (resources *recordingResources) RemoveFinalizer(
	_ context.Context,
	_ fleetcontroller.ProtectedResource,
) error {
	resources.operations = append(resources.operations, "remove-finalizer")
	return nil
}

func (resources *recordingResources) IsAbsent(
	_ context.Context,
	resource fleetcontroller.ProtectedResource,
) (bool, error) {
	resources.operations = append(resources.operations, "observe-absence")
	resources.absenceRequests = append(resources.absenceRequests, resource)
	return resources.absent, resources.absenceErr
}

func (resources *recordingResources) recordSharedOperation(operation string) {
	if resources.sharedOperations != nil {
		*resources.sharedOperations = append(*resources.sharedOperations, operation)
	}
}

type staticDrainReader struct {
	result               fleet.DrainResult
	err                  error
	retirementAuthorized bool
	retirementCompleted  bool
	completionErr        error
}

type recordingDrainCoordinator struct {
	state                 fleet.DrainState
	states                map[uuid.UUID]fleet.DrainState
	requests              []fleet.DrainRequest
	reconciled            []uuid.UUID
	retirementAuthorized  bool
	retirementCompleted   bool
	completionErr         error
	completionRecords     []fleet.RetirementAuthorizationRequest
	completionByTarget    map[string]bool
	authorizationRequests []fleet.RetirementAuthorizationRequest
	completionRequests    []fleet.RetirementAuthorizationRequest
}

func (coordinator *recordingDrainCoordinator) GetDrain(
	_ context.Context,
	operationID uuid.UUID,
) (fleet.DrainResult, error) {
	for _, request := range coordinator.requests {
		if request.OperationID == operationID {
			return drainResultForRequest(request, coordinator.stateFor(operationID)), nil
		}
	}
	return fleet.DrainResult{}, fleetcontroller.ErrResourceNotFound
}

func (coordinator *recordingDrainCoordinator) RequestDrain(
	_ context.Context,
	request fleet.DrainRequest,
) (fleet.DrainResult, error) {
	coordinator.requests = append(coordinator.requests, request)
	return drainResultForRequest(request, coordinator.stateFor(request.OperationID)), nil
}

func (coordinator *recordingDrainCoordinator) ReconcileDrain(
	_ context.Context,
	operationID uuid.UUID,
) (fleet.DrainResult, error) {
	coordinator.reconciled = append(coordinator.reconciled, operationID)
	for index := len(coordinator.requests) - 1; index >= 0; index-- {
		if coordinator.requests[index].OperationID == operationID {
			return drainResultForRequest(
				coordinator.requests[index], coordinator.stateFor(operationID),
			), nil
		}
	}
	return fleet.DrainResult{}, fleetcontroller.ErrResourceNotFound
}

func (coordinator *recordingDrainCoordinator) stateFor(operationID uuid.UUID) fleet.DrainState {
	if state, exists := coordinator.states[operationID]; exists {
		return state
	}
	return coordinator.state
}

func (coordinator *recordingDrainCoordinator) HasRetirementAuthorization(
	_ context.Context,
	request fleet.RetirementAuthorizationRequest,
) (bool, error) {
	coordinator.authorizationRequests = append(coordinator.authorizationRequests, request)
	return coordinator.retirementAuthorized, nil
}

func (coordinator *recordingDrainCoordinator) HasRetirementCompletion(
	_ context.Context,
	request fleet.RetirementAuthorizationRequest,
) (bool, error) {
	coordinator.completionRequests = append(coordinator.completionRequests, request)
	return coordinator.retirementCompleted || coordinator.completionByTarget[retirementTargetKey(request)], nil
}

func (coordinator *recordingDrainCoordinator) RecordRetirementCompletion(
	_ context.Context,
	request fleet.RetirementAuthorizationRequest,
) (fleet.RetirementCompletionResult, error) {
	coordinator.completionRecords = append(coordinator.completionRecords, request)
	if coordinator.completionErr != nil {
		return fleet.RetirementCompletionResult{}, coordinator.completionErr
	}
	if coordinator.completionByTarget == nil {
		coordinator.completionByTarget = make(map[string]bool)
	}
	coordinator.completionByTarget[retirementTargetKey(request)] = true
	return fleet.RetirementCompletionResult{
		CompletedAt: time.Date(2026, 8, 27, 13, 0, 0, 0, time.UTC),
	}, nil
}

func retirementTargetKey(request fleet.RetirementAuthorizationRequest) string {
	return string(request.ResourceKind) + "/" + request.KubernetesUID
}

func drainResultForRequest(request fleet.DrainRequest, state fleet.DrainState) fleet.DrainResult {
	return fleet.DrainResult{
		OperationID: request.OperationID, State: state,
		WorkerID: request.WorkerID, WorkerEpoch: request.ExpectedEpoch,
		WorkerLifecycle: "DRAINING", WorkerReachability: "HEALTHY",
		Deadline: request.Deadline,
	}
}

type staticPendingWorkerPodLister struct{}

func (staticPendingWorkerPodLister) ListPendingWorkerPods(context.Context) ([]fleetcontroller.WorkerPod, error) {
	return nil, nil
}

type staticCapacityPolicyConfigurator struct {
	policies         []fleet.CapacityPolicy
	result           fleet.CapacityPolicyResult
	err              error
	sharedOperations *[]string
}

func (configurator *staticCapacityPolicyConfigurator) ConfigureCapacityPolicy(
	_ context.Context,
	policy fleet.CapacityPolicy,
) (fleet.CapacityPolicyResult, error) {
	if configurator.sharedOperations != nil {
		*configurator.sharedOperations = append(
			*configurator.sharedOperations,
			"configure-capacity-policy",
		)
	}
	configurator.policies = append(configurator.policies, policy)
	result := configurator.result
	if result.WorkerPoolID == uuid.Nil {
		result = fleet.CapacityPolicyResult{
			WorkerPoolID: policy.WorkerPoolID,
			Revision:     policy.Revision,
			Replayed:     len(configurator.policies) > 1,
		}
	}
	return result, configurator.err
}

type staticIdentityResolver struct {
	identity         fleet.WorkerIdentity
	err              error
	requests         []fleet.WorkerIdentityRequest
	sharedOperations *[]string
}

func (resolver *staticIdentityResolver) ResolveWorkerIdentity(
	_ context.Context,
	request fleet.WorkerIdentityRequest,
) (fleet.WorkerIdentity, error) {
	if resolver.sharedOperations != nil {
		*resolver.sharedOperations = append(*resolver.sharedOperations, "resolve-worker-identity")
	}
	resolver.requests = append(resolver.requests, request)
	return resolver.identity, resolver.err
}

type staticReadinessStarter struct {
	result           fleet.ReadinessResult
	err              error
	requests         []fleet.ReadinessRequest
	sharedOperations *[]string
}

func (starter *staticReadinessStarter) BeginReadiness(
	_ context.Context,
	request fleet.ReadinessRequest,
) (fleet.ReadinessResult, error) {
	if starter.sharedOperations != nil {
		*starter.sharedOperations = append(*starter.sharedOperations, "begin-readiness")
	}
	starter.requests = append(starter.requests, request)
	return starter.result, starter.err
}

func successfulReadinessStarter() *staticReadinessStarter {
	return &staticReadinessStarter{result: fleet.ReadinessResult{
		CycleID: uuid.MustParse("f98bf4ac-94c4-5360-a476-cac78358adbd"),
		State:   fleet.ReadinessChecking, NextCheck: fleet.ReadinessIdentity,
		WorkerLifecycle: "WARMING", WorkerReachability: "SUSPECT",
	}}
}

func (reader *staticDrainReader) GetDrain(
	_ context.Context,
	_ uuid.UUID,
) (fleet.DrainResult, error) {
	return reader.result, reader.err
}

func (reader *staticDrainReader) RequestDrain(
	_ context.Context,
	_ fleet.DrainRequest,
) (fleet.DrainResult, error) {
	return reader.result, reader.err
}

func (reader *staticDrainReader) ReconcileDrain(
	_ context.Context,
	_ uuid.UUID,
) (fleet.DrainResult, error) {
	return reader.result, reader.err
}

func (reader *staticDrainReader) HasRetirementAuthorization(
	context.Context,
	fleet.RetirementAuthorizationRequest,
) (bool, error) {
	return reader.retirementAuthorized, reader.err
}

func (reader *staticDrainReader) HasRetirementCompletion(
	context.Context,
	fleet.RetirementAuthorizationRequest,
) (bool, error) {
	return reader.retirementCompleted, reader.err
}

func (reader *staticDrainReader) RecordRetirementCompletion(
	context.Context,
	fleet.RetirementAuthorizationRequest,
) (fleet.RetirementCompletionResult, error) {
	if reader.completionErr != nil {
		return fleet.RetirementCompletionResult{}, reader.completionErr
	}
	return fleet.RetirementCompletionResult{
		CompletedAt: time.Date(2026, 8, 27, 13, 0, 0, 0, time.UTC),
	}, nil
}
