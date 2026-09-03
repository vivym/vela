package fleetcontroller_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestResidencyPlanRolloutAppliesAuthorityBeforeActuation(t *testing.T) {
	rollout := h3ResidencyPlanRollout(t)
	events := []string{}
	applier := &recordingResidencyPlanApplier{
		events: &events,
		result: fleet.ActuationPlan{
			PlanRevisionID:      rollout.ApprovedPlan.ID,
			WorkerInstanceCount: len(rollout.ApprovedPlan.WorkerInstances),
		},
	}
	actuator := &recordingWorkerBundleActuator{events: &events}
	controller, err := fleetcontroller.NewResidencyPlanRolloutController(applier, actuator)
	if err != nil {
		t.Fatalf("create ResidencyPlan rollout controller: %v", err)
	}
	result, err := controller.Reconcile(context.Background(), rollout)
	if err != nil {
		t.Fatalf("reconcile ResidencyPlan rollout: %v", err)
	}
	if len(events) != 2 || events[0] != "apply" || events[1] != "actuate" {
		t.Fatalf("ResidencyPlan rollout events=%v, want apply then actuate", events)
	}
	if result.PlanRevisionID != rollout.ApprovedPlan.ID ||
		result.WorkerInstances != 8 || result.CreatedPods != 8 || result.Converged {
		t.Fatalf("ResidencyPlan rollout result=%#v", result)
	}
}

func TestResidencyPlanRolloutBindsEveryModelRuntimeRoute(t *testing.T) {
	rollout := h3ResidencyPlanRollout(t)
	if len(rollout.ApprovedPlan.CapacityPools) != 3 {
		t.Fatalf("H3 CapacityPool count=%d, want Encoder/DiT/VAE", len(rollout.ApprovedPlan.CapacityPools))
	}
	aux := rollout.ApprovedPlan.WorkerInstances[0]
	if len(aux.ModelRuntimeRoutes) != 2 {
		t.Fatalf("AUX ModelRuntime route count=%d, want Encoder/VAE", len(aux.ModelRuntimeRoutes))
	}
	if aux.ModelRuntimeRoutes[0].CapacityPoolID == aux.ModelRuntimeRoutes[1].CapacityPoolID {
		t.Fatalf("AUX routes share CapacityPool %s", aux.ModelRuntimeRoutes[0].CapacityPoolID)
	}

	drifted := rollout
	drifted.ApprovedPlan.WorkerInstances[0].ModelRuntimeRoutes[1].CapacityPoolID =
		drifted.ApprovedPlan.WorkerInstances[0].ModelRuntimeRoutes[0].CapacityPoolID
	if err := fleetcontroller.ValidateResidencyPlanRollout(drifted); err == nil {
		t.Fatal("ResidencyPlan rollout accepted a VAE route under the Encoder CapacityPool")
	}
}

func TestResidencyPlanRolloutRejectsActuationOutsideApprovedAuthority(t *testing.T) {
	rollout := h3ResidencyPlanRollout(t)
	rollout.WorkerBundles[0].WorkerInstances[0].ID = uuid.New()
	applier := &recordingResidencyPlanApplier{}
	actuator := &recordingWorkerBundleActuator{}
	controller, err := fleetcontroller.NewResidencyPlanRolloutController(applier, actuator)
	if err != nil {
		t.Fatalf("create ResidencyPlan rollout controller: %v", err)
	}
	if _, err := controller.Reconcile(context.Background(), rollout); err == nil {
		t.Fatal("ResidencyPlan rollout accepted an unapproved WorkerInstance id")
	}
	if applier.calls != 0 || actuator.calls != 0 {
		t.Fatalf("invalid rollout reached authority or actuator: apply=%d actuate=%d", applier.calls, actuator.calls)
	}
}

func TestResidencyPlanRolloutRejectsTamperedActuationWithRetainedDigest(t *testing.T) {
	rollout := h3ResidencyPlanRollout(t)
	rollout.WorkerBundles[0].RuntimeImage = pinnedImage("tampered-runtime", 'f')
	applier := &recordingResidencyPlanApplier{}
	actuator := &recordingWorkerBundleActuator{}
	controller, err := fleetcontroller.NewResidencyPlanRolloutController(applier, actuator)
	if err != nil {
		t.Fatalf("create ResidencyPlan rollout controller: %v", err)
	}
	if _, err := controller.Reconcile(context.Background(), rollout); err == nil {
		t.Fatal("ResidencyPlan rollout accepted tampered actuation under a retained digest")
	}
	if applier.calls != 0 || actuator.calls != 0 {
		t.Fatalf("tampered rollout reached authority or actuator: apply=%d actuate=%d", applier.calls, actuator.calls)
	}
}

func TestResidencyPlanRolloutStopsBeforePodsWhenAuthorityApplyFails(t *testing.T) {
	rollout := h3ResidencyPlanRollout(t)
	applier := &recordingResidencyPlanApplier{err: errors.New("approval rejected")}
	actuator := &recordingWorkerBundleActuator{}
	controller, err := fleetcontroller.NewResidencyPlanRolloutController(applier, actuator)
	if err != nil {
		t.Fatalf("create ResidencyPlan rollout controller: %v", err)
	}
	if _, err := controller.Reconcile(context.Background(), rollout); err == nil {
		t.Fatal("ResidencyPlan rollout ignored authority failure")
	}
	if actuator.calls != 0 {
		t.Fatalf("authority failure created %d WorkerBundle actuations", actuator.calls)
	}
}

func TestRuntimeReconcilesTargetOnlyResidencyPlanWithoutLegacyWorkerPool(t *testing.T) {
	rollout := h3ResidencyPlanRollout(t)
	applier := &recordingResidencyPlanApplier{result: fleet.ActuationPlan{
		PlanRevisionID:      rollout.ApprovedPlan.ID,
		WorkerInstanceCount: len(rollout.ApprovedPlan.WorkerInstances),
	}}
	actuator := &recordingWorkerBundleActuator{converged: true}
	controller, err := fleetcontroller.NewResidencyPlanRolloutController(applier, actuator)
	if err != nil {
		t.Fatalf("create ResidencyPlan rollout controller: %v", err)
	}
	runtimeController, err := fleetcontroller.NewRuntime(fleetcontroller.RuntimeConfig{
		ResidencyPlanRollouts:    []fleetcontroller.ResidencyPlanRollout{rollout},
		WorkerInstanceController: controller,
		PollInterval:             time.Second,
	})
	if err != nil {
		t.Fatalf("create target-only Fleet runtime: %v", err)
	}
	result, err := runtimeController.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("reconcile target-only Fleet runtime: %v", err)
	}
	if result.ResidencyPlansConverged != 1 || result.WorkerInstancePodsCreated != 8 ||
		!runtimeController.Converged() {
		t.Fatalf("target-only Fleet runtime result=%#v converged=%t", result, runtimeController.Converged())
	}
}

func TestRuntimeTreatsEmptyDesiredStateAsReadyConvergedNoop(t *testing.T) {
	runtimeController, err := fleetcontroller.NewRuntime(fleetcontroller.RuntimeConfig{
		PollInterval: time.Second,
	})
	if err != nil {
		t.Fatalf("create empty Fleet runtime: %v", err)
	}
	if !runtimeController.Ready() || !runtimeController.Converged() {
		t.Fatalf(
			"empty Fleet runtime ready=%t converged=%t, want true/true",
			runtimeController.Ready(), runtimeController.Converged(),
		)
	}
	result, err := runtimeController.RunOnce(context.Background())
	if err != nil || result != (fleetcontroller.RuntimeResult{}) {
		t.Fatalf("empty Fleet runtime result=%#v error=%v", result, err)
	}
}

func TestWorkerInstancePodAdmissionRequiresExactApprovedPlanPod(t *testing.T) {
	rollout := h3ResidencyPlanRollout(t)
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register core Kubernetes scheme: %v", err)
	}
	if err := resourcev1.AddToScheme(scheme); err != nil {
		t.Fatalf("register resource Kubernetes scheme: %v", err)
	}
	client := dynamicfake.NewSimpleDynamicClient(scheme)
	resources, err := fleetcontroller.NewKubernetesResources(client, "vela-system")
	if err != nil {
		t.Fatalf("create Kubernetes resources: %v", err)
	}
	validator, err := fleetcontroller.NewWorkerInstancePodAdmissionValidator(
		[]fleetcontroller.ResidencyPlanRollout{rollout},
	)
	if err != nil {
		t.Fatalf("create WorkerInstance Pod admission validator: %v", err)
	}
	actuator, err := fleetcontroller.NewWorkerInstanceActuator(resources)
	if err != nil {
		t.Fatalf("create WorkerInstance actuator: %v", err)
	}
	if _, err := actuator.Actuate(context.Background(), rollout.WorkerBundles[0]); err != nil {
		t.Fatalf("materialize WorkerInstance Pods: %v", err)
	}
	objects, err := client.Resource(schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "pods",
	}).Namespace("vela-system").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(objects.Items) == 0 {
		t.Fatalf("list WorkerInstance Pods count=%d error=%v", len(objects.Items), err)
	}
	var pod corev1.Pod
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(objects.Items[0].Object, &pod); err != nil {
		t.Fatalf("decode WorkerInstance Pod: %v", err)
	}
	if err := validator.ValidateProtectedPodCreate(context.Background(), pod); err != nil {
		t.Fatalf("validate exact approved WorkerInstance Pod: %v", err)
	}
	drifted := *pod.DeepCopy()
	drifted.Spec.Containers[1].Image = pinnedImage("unapproved-runtime", 'f')
	if err := validator.ValidateProtectedPodCreate(context.Background(), drifted); !errors.Is(err, fleetcontroller.ErrProtectedResourceDrift) {
		t.Fatalf("unapproved WorkerInstance Pod drift error=%v", err)
	}
}

func TestDecodeResidencyPlanRolloutsRejectsUnknownAndCrossNamespaceInput(t *testing.T) {
	rollout := h3ResidencyPlanRollout(t)
	encoded, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"rollouts":       []fleetcontroller.ResidencyPlanRollout{rollout},
	})
	if err != nil {
		t.Fatalf("encode ResidencyPlan rollout input: %v", err)
	}
	decoded, err := fleetcontroller.DecodeResidencyPlanRollouts(encoded, "vela-system")
	if err != nil || len(decoded) != 1 || decoded[0].ApprovedPlan.ID != rollout.ApprovedPlan.ID {
		t.Fatalf("decode ResidencyPlan rollouts=%#v error=%v", decoded, err)
	}
	unknown := append(encoded[:len(encoded)-1], []byte(`,"unknown":true}`)...)
	if _, err := fleetcontroller.DecodeResidencyPlanRollouts(unknown, "vela-system"); err == nil {
		t.Fatal("ResidencyPlan rollout decoder accepted an unknown field")
	}
	rollout.WorkerBundles[0].Namespace = "other-system"
	encoded, err = json.Marshal(map[string]any{
		"schema_version": 1,
		"rollouts":       []fleetcontroller.ResidencyPlanRollout{rollout},
	})
	if err != nil {
		t.Fatalf("encode cross-namespace ResidencyPlan rollout input: %v", err)
	}
	if _, err := fleetcontroller.DecodeResidencyPlanRollouts(encoded, "vela-system"); err == nil {
		t.Fatal("ResidencyPlan rollout decoder accepted a cross-namespace actuation")
	}
}

func TestDecodeResidencyPlanRolloutsAcceptsExplicitEmptyDesiredState(t *testing.T) {
	decoded, err := fleetcontroller.DecodeResidencyPlanRollouts(
		[]byte(`{"schema_version":1,"rollouts":[]}`),
		"vela-system",
	)
	if err != nil || decoded == nil || len(decoded) != 0 {
		t.Fatalf("decode explicit empty ResidencyPlan rollouts=%#v error=%v", decoded, err)
	}
	for _, encoded := range [][]byte{
		[]byte(`{"schema_version":1}`),
		[]byte(`{"schema_version":1,"rollouts":null}`),
	} {
		if _, err := fleetcontroller.DecodeResidencyPlanRollouts(encoded, "vela-system"); err == nil {
			t.Fatalf("ResidencyPlan rollout decoder accepted implicit empty input %s", encoded)
		}
	}
	validator, err := fleetcontroller.NewWorkerInstancePodAdmissionValidator(decoded)
	if err != nil {
		t.Fatalf("create empty desired-state admission validator: %v", err)
	}
	if err := validator.ValidateProtectedPodCreate(context.Background(), corev1.Pod{}); !errors.Is(err, fleetcontroller.ErrProtectedResourceDrift) {
		t.Fatalf("empty desired-state validator error=%v", err)
	}
}

func TestResidencyPlanRolloutRejectsGPUReusedAcrossWorkerBundles(t *testing.T) {
	rollout := h3ResidencyPlanRollout(t)
	secondSpec := h3BundleSpec()
	secondSpec.WorkerBundleID = uuid.MustParse("49300000-0000-0000-0000-000000000202")
	secondBundle, err := fleetcontroller.BuildH3WorkerBundleActuation(secondSpec)
	if err != nil {
		t.Fatalf("build second H3 WorkerBundle: %v", err)
	}
	appendWorkerBundleAuthority(&rollout, secondBundle, "h3-node-02")
	if err := fleetcontroller.ValidateResidencyPlanRollout(rollout); err == nil {
		t.Fatal("ResidencyPlan rollout accepted one GPU in multiple WorkerBundles")
	}
}

func TestDecodeResidencyPlanRolloutsRejectsGPUReusedAcrossPlans(t *testing.T) {
	first := h3ResidencyPlanRollout(t)
	secondSpec := h3BundleSpec()
	secondSpec.PlanRevisionID = uuid.MustParse("49300000-0000-0000-0000-000000000201")
	secondSpec.WorkerBundleID = uuid.MustParse("49300000-0000-0000-0000-000000000202")
	secondBundle, err := fleetcontroller.BuildH3WorkerBundleActuation(secondSpec)
	if err != nil {
		t.Fatalf("build second H3 WorkerBundle: %v", err)
	}
	second := residencyPlanRolloutForBundle(secondBundle, "h3-stage-workers-r2", "h3-node-02")
	encoded, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"rollouts":       []fleetcontroller.ResidencyPlanRollout{first, second},
	})
	if err != nil {
		t.Fatalf("encode conflicting ResidencyPlan rollouts: %v", err)
	}
	if _, err := fleetcontroller.DecodeResidencyPlanRollouts(encoded, "vela-system"); err == nil {
		t.Fatal("ResidencyPlan rollout decoder accepted one GPU in multiple plans")
	}
}

func h3ResidencyPlanRollout(t *testing.T) fleetcontroller.ResidencyPlanRollout {
	t.Helper()
	bundle, err := fleetcontroller.BuildH3WorkerBundleActuation(h3BundleSpec())
	if err != nil {
		t.Fatalf("build H3 WorkerBundle: %v", err)
	}
	return residencyPlanRolloutForBundle(bundle, "h3-stage-workers", "h3-node-01")
}

func residencyPlanRolloutForBundle(
	bundle fleetcontroller.WorkerBundleActuation,
	stableID string,
	bundleStableID string,
) fleetcontroller.ResidencyPlanRollout {
	plan := fleet.ApprovedResidencyPlan{
		SchemaVersion: 1,
		ID:            bundle.PlanRevisionID, StableID: stableID, Revision: 1,
		ContentDigest: bundle.RevisionDigest, ApprovalEvidenceDigest: digestString('e'),
		ApprovedAt: h3RolloutApprovalTime, ApprovedBy: "fleet/operator-1",
		CapacityPools: []fleet.PlannedCapacityPool{
			{
				ID: bundle.WorkerInstances[0].CapacityPoolID, StableID: "h3-aux",
				StageProfileRevisionID: bundle.WorkerInstances[0].ModelRuntimes[0].StageProfileRevisionID,
				ResourceClass:          "GPU", SecurityClass: "INTERNAL", Region: "cn-shanghai",
				MaxReadyQueueDepth: 128,
			},
			{
				ID: bundle.WorkerInstances[1].CapacityPoolID, StableID: "h3-dit",
				StageProfileRevisionID: bundle.WorkerInstances[1].ModelRuntimes[0].StageProfileRevisionID,
				ResourceClass:          "GPU", SecurityClass: "INTERNAL", Region: "cn-shanghai",
				MaxReadyQueueDepth: 1024,
			},
			{
				ID: bundle.WorkerInstances[0].ModelRuntimes[1].CapacityPoolID, StableID: "h3-vae",
				StageProfileRevisionID: bundle.WorkerInstances[0].ModelRuntimes[1].StageProfileRevisionID,
				ResourceClass:          "GPU", SecurityClass: "INTERNAL", Region: "cn-shanghai",
				MaxReadyQueueDepth: 128,
			},
		},
		WorkerBundles: []fleet.PlannedWorkerBundle{{
			ID: bundle.WorkerBundleID, StableID: bundleStableID,
			DesiredGeneration: 1, LayoutDigest: bundle.RevisionDigest,
		}},
		WorkerInstances: make([]fleet.PlannedWorkerInstance, 0, len(bundle.WorkerInstances)),
	}
	for _, worker := range bundle.WorkerInstances {
		deviceCount := 0
		for _, member := range worker.Members {
			deviceCount += member.DeviceCount
		}
		plan.WorkerInstances = append(plan.WorkerInstances, fleet.PlannedWorkerInstance{
			ID: worker.ID, WorkerProfileRevisionID: worker.WorkerProfileRevisionID,
			CapacityPoolID: worker.CapacityPoolID, WorkerBundleID: bundle.WorkerBundleID,
			DesiredMemberCount: len(worker.Members), DesiredDeviceCount: deviceCount,
			ModelRuntimeRoutes: plannedRuntimeRoutes(worker.ModelRuntimes),
		})
	}
	return fleetcontroller.ResidencyPlanRollout{
		ApprovedPlan: plan, WorkerBundles: []fleetcontroller.WorkerBundleActuation{bundle},
	}
}

func plannedRuntimeRoutes(runtimes []fleetcontroller.ModelRuntimeProcess) []fleet.PlannedModelRuntimeRoute {
	routes := make([]fleet.PlannedModelRuntimeRoute, 0, len(runtimes))
	for _, runtime := range runtimes {
		routes = append(routes, fleet.PlannedModelRuntimeRoute{
			ModelResidencyID: runtime.ModelResidencyID, CapacityPoolID: runtime.CapacityPoolID,
			StageProfileRevisionID: runtime.StageProfileRevisionID,
		})
	}
	return routes
}

func appendWorkerBundleAuthority(
	rollout *fleetcontroller.ResidencyPlanRollout,
	bundle fleetcontroller.WorkerBundleActuation,
	stableID string,
) {
	rollout.ApprovedPlan.WorkerBundles = append(
		rollout.ApprovedPlan.WorkerBundles,
		fleet.PlannedWorkerBundle{
			ID: bundle.WorkerBundleID, StableID: stableID,
			DesiredGeneration: 1, LayoutDigest: bundle.RevisionDigest,
		},
	)
	for _, worker := range bundle.WorkerInstances {
		rollout.ApprovedPlan.WorkerInstances = append(
			rollout.ApprovedPlan.WorkerInstances,
			fleet.PlannedWorkerInstance{
				ID: worker.ID, WorkerProfileRevisionID: worker.WorkerProfileRevisionID,
				CapacityPoolID: worker.CapacityPoolID, WorkerBundleID: bundle.WorkerBundleID,
				DesiredMemberCount: len(worker.Members), DesiredDeviceCount: workerDeviceCountForTest(worker),
				ModelRuntimeRoutes: plannedRuntimeRoutes(worker.ModelRuntimes),
			},
		)
	}
	rollout.WorkerBundles = append(rollout.WorkerBundles, bundle)
}

func workerDeviceCountForTest(worker fleetcontroller.WorkerInstanceActuation) int {
	count := 0
	for _, member := range worker.Members {
		count += member.DeviceCount
	}
	return count
}

var h3RolloutApprovalTime = mustRolloutTime()

func mustRolloutTime() time.Time {
	return time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
}

type recordingResidencyPlanApplier struct {
	calls  int
	events *[]string
	result fleet.ActuationPlan
	err    error
}

func (applier *recordingResidencyPlanApplier) Apply(
	_ context.Context,
	_ fleet.ApprovedResidencyPlan,
) (fleet.ActuationPlan, error) {
	applier.calls++
	if applier.events != nil {
		*applier.events = append(*applier.events, "apply")
	}
	return applier.result, applier.err
}

type recordingWorkerBundleActuator struct {
	calls     int
	events    *[]string
	converged bool
}

func (actuator *recordingWorkerBundleActuator) Actuate(
	_ context.Context,
	bundle fleetcontroller.WorkerBundleActuation,
) (fleetcontroller.WorkerInstanceActuationResult, error) {
	actuator.calls++
	if actuator.events != nil {
		*actuator.events = append(*actuator.events, "actuate")
	}
	return fleetcontroller.WorkerInstanceActuationResult{
		CreatedPods: len(bundle.WorkerInstances),
		Converged:   actuator.converged,
	}, nil
}
