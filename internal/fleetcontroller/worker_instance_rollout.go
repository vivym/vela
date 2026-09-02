package fleetcontroller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
)

const MaximumResidencyPlanRolloutBytes = 4 << 20

type ResidencyPlanApplier interface {
	Apply(context.Context, fleet.ApprovedResidencyPlan) (fleet.ActuationPlan, error)
}

type WorkerBundleActuator interface {
	Actuate(context.Context, WorkerBundleActuation) (WorkerInstanceActuationResult, error)
}

type ResidencyPlanRollout struct {
	ApprovedPlan  fleet.ApprovedResidencyPlan `json:"approved_plan"`
	WorkerBundles []WorkerBundleActuation     `json:"worker_bundles"`
}

type residencyPlanRolloutInput struct {
	SchemaVersion int                    `json:"schema_version"`
	Rollouts      []ResidencyPlanRollout `json:"rollouts"`
}

type ResidencyPlanRolloutResult struct {
	PlanRevisionID  uuid.UUID
	WorkerInstances int
	CreatedPods     int
	Converged       bool
}

type ResidencyPlanRolloutController struct {
	applier  ResidencyPlanApplier
	actuator WorkerBundleActuator
}

func NewResidencyPlanRolloutController(
	applier ResidencyPlanApplier,
	actuator WorkerBundleActuator,
) (*ResidencyPlanRolloutController, error) {
	if applier == nil || actuator == nil {
		return nil, errors.New("ResidencyPlan rollout authority and actuator are required")
	}
	return &ResidencyPlanRolloutController{applier: applier, actuator: actuator}, nil
}

func DecodeResidencyPlanRollouts(encoded []byte, namespace string) ([]ResidencyPlanRollout, error) {
	if len(encoded) == 0 || len(encoded) > MaximumResidencyPlanRolloutBytes ||
		!validResourceName(namespace) {
		return nil, errors.New("ResidencyPlan rollout input is empty, too large, or has an invalid namespace")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var input residencyPlanRolloutInput
	if err := decoder.Decode(&input); err != nil {
		return nil, fmt.Errorf("decode ResidencyPlan rollout input: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("ResidencyPlan rollout input must contain exactly one JSON document")
	}
	if input.SchemaVersion != 1 || len(input.Rollouts) == 0 || len(input.Rollouts) > 4096 {
		return nil, errors.New("ResidencyPlan rollout input is invalid")
	}
	planIDs := make(map[uuid.UUID]struct{}, len(input.Rollouts))
	configuredDevices := newDeviceOwnership()
	for _, rollout := range input.Rollouts {
		if err := ValidateResidencyPlanRollout(rollout); err != nil {
			return nil, err
		}
		if _, exists := planIDs[rollout.ApprovedPlan.ID]; exists {
			return nil, errors.New("ResidencyPlan rollout input contains a duplicate plan revision")
		}
		planIDs[rollout.ApprovedPlan.ID] = struct{}{}
		for _, bundle := range rollout.WorkerBundles {
			if bundle.Namespace != namespace {
				return nil, errors.New("ResidencyPlan rollout namespace does not match Fleet namespace")
			}
			if err := reserveWorkerBundleDevices(configuredDevices, bundle); err != nil {
				return nil, fmt.Errorf("ResidencyPlan rollout input reuses device ownership across plans: %w", err)
			}
		}
	}
	return input.Rollouts, nil
}

func (controller *ResidencyPlanRolloutController) Reconcile(
	ctx context.Context,
	rollout ResidencyPlanRollout,
) (ResidencyPlanRolloutResult, error) {
	if controller == nil || controller.applier == nil || controller.actuator == nil {
		return ResidencyPlanRolloutResult{}, errors.New("ResidencyPlan rollout controller is not configured")
	}
	if err := ValidateResidencyPlanRollout(rollout); err != nil {
		return ResidencyPlanRolloutResult{}, err
	}
	authority, err := controller.applier.Apply(ctx, rollout.ApprovedPlan)
	if err != nil {
		return ResidencyPlanRolloutResult{}, fmt.Errorf("apply approved ResidencyPlan: %w", err)
	}
	if authority.PlanRevisionID != rollout.ApprovedPlan.ID ||
		authority.WorkerInstanceCount != len(rollout.ApprovedPlan.WorkerInstances) {
		return ResidencyPlanRolloutResult{}, errors.New("applied ResidencyPlan authority does not match desired rollout")
	}
	result := ResidencyPlanRolloutResult{
		PlanRevisionID:  authority.PlanRevisionID,
		WorkerInstances: authority.WorkerInstanceCount,
		Converged:       true,
	}
	for _, bundle := range rollout.WorkerBundles {
		actuation, err := controller.actuator.Actuate(ctx, bundle)
		if err != nil {
			return ResidencyPlanRolloutResult{}, fmt.Errorf(
				"actuate WorkerBundle %s: %w",
				bundle.WorkerBundleID,
				err,
			)
		}
		result.CreatedPods += actuation.CreatedPods
		result.Converged = result.Converged && actuation.Converged
	}
	return result, nil
}

func ValidateResidencyPlanRollout(rollout ResidencyPlanRollout) error {
	plan := rollout.ApprovedPlan
	if err := fleet.ValidateApprovedResidencyPlan(plan); err != nil {
		return fmt.Errorf("ResidencyPlan rollout authority is invalid: %w", err)
	}
	if len(rollout.WorkerBundles) == 0 {
		return errors.New("ResidencyPlan rollout is invalid")
	}
	plannedBundles := make(map[uuid.UUID]fleet.PlannedWorkerBundle, len(plan.WorkerBundles))
	for _, bundle := range plan.WorkerBundles {
		plannedBundles[bundle.ID] = bundle
	}
	plannedWorkers := make(map[uuid.UUID]fleet.PlannedWorkerInstance, len(plan.WorkerInstances))
	for _, worker := range plan.WorkerInstances {
		plannedWorkers[worker.ID] = worker
	}
	seenBundles := make(map[uuid.UUID]struct{}, len(rollout.WorkerBundles))
	seenWorkers := make(map[uuid.UUID]struct{}, len(plan.WorkerInstances))
	rolloutDevices := newDeviceOwnership()
	for _, bundle := range rollout.WorkerBundles {
		if err := ValidateWorkerBundleActuation(bundle); err != nil {
			return err
		}
		if bundle.PlanRevisionID != plan.ID {
			return errors.New("WorkerBundle actuation references a different ResidencyPlan")
		}
		plannedBundle, exists := plannedBundles[bundle.WorkerBundleID]
		if !exists || plannedBundle.LayoutDigest != bundle.RevisionDigest {
			return errors.New("WorkerBundle actuation does not match approved layout authority")
		}
		if _, duplicate := seenBundles[bundle.WorkerBundleID]; duplicate {
			return errors.New("ResidencyPlan rollout actuates one WorkerBundle more than once")
		}
		seenBundles[bundle.WorkerBundleID] = struct{}{}
		if err := reserveWorkerBundleDevices(rolloutDevices, bundle); err != nil {
			return fmt.Errorf("ResidencyPlan rollout reuses device ownership across WorkerBundles: %w", err)
		}
		for _, worker := range bundle.WorkerInstances {
			planned, exists := plannedWorkers[worker.ID]
			if !exists || planned.WorkerBundleID != bundle.WorkerBundleID ||
				planned.WorkerProfileRevisionID != worker.WorkerProfileRevisionID ||
				planned.CapacityPoolID != worker.CapacityPoolID ||
				planned.DesiredMemberCount != len(worker.Members) ||
				planned.DesiredDeviceCount != workerDeviceCount(worker) ||
				!modelRuntimeRoutesMatch(planned.ModelRuntimeRoutes, worker.ModelRuntimes) {
				return errors.New("WorkerInstance actuation does not match approved authority")
			}
			if _, duplicate := seenWorkers[worker.ID]; duplicate {
				return errors.New("ResidencyPlan rollout actuates one WorkerInstance more than once")
			}
			seenWorkers[worker.ID] = struct{}{}
		}
	}
	if len(seenBundles) != len(plannedBundles) || len(seenWorkers) != len(plannedWorkers) {
		return errors.New("ResidencyPlan rollout does not cover every approved Worker authority")
	}
	return nil
}

func modelRuntimeRoutesMatch(
	planned []fleet.PlannedModelRuntimeRoute,
	actuated []ModelRuntimeProcess,
) bool {
	if len(planned) != len(actuated) {
		return false
	}
	byResidency := make(map[uuid.UUID]fleet.PlannedModelRuntimeRoute, len(planned))
	for _, route := range planned {
		byResidency[route.ModelResidencyID] = route
	}
	for _, runtime := range actuated {
		route, exists := byResidency[runtime.ModelResidencyID]
		if !exists || route.CapacityPoolID != runtime.CapacityPoolID ||
			route.StageProfileRevisionID != runtime.StageProfileRevisionID {
			return false
		}
	}
	return true
}

func workerDeviceCount(worker WorkerInstanceActuation) int {
	count := 0
	for _, member := range worker.Members {
		count += member.DeviceCount
	}
	return count
}
