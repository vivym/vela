package fleetcontroller

import (
	"bytes"
	"errors"
	"io"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

const MaximumDesiredConfigurationBytes = 1 << 20

type desiredInput struct {
	APIVersion  string                 `yaml:"apiVersion"`
	Kind        string                 `yaml:"kind"`
	Revisions   []desiredRevisionInput `yaml:"revisions"`
	Retirements []retirementPlanInput  `yaml:"retirements"`
}

type desiredRevisionInput struct {
	WorkerPoolID               string              `yaml:"workerPoolID"`
	Name                       string              `yaml:"name"`
	Revision                   string              `yaml:"revision"`
	WorkerProfile              string              `yaml:"workerProfile"`
	NodeSelector               map[string]string   `yaml:"nodeSelector"`
	InitImage                  string              `yaml:"initImage"`
	WorkerAgentImage           string              `yaml:"workerAgentImage"`
	RunnerImage                string              `yaml:"runnerImage"`
	ArtifactStoreTLSSecret     string              `yaml:"artifactStoreTLSSecret"`
	ExecutionProfileRevisionID string              `yaml:"executionProfileRevisionID"`
	InferenceBackendRevision   string              `yaml:"inferenceBackendRevision"`
	ReadinessTimeout           string              `yaml:"readinessTimeout"`
	CapacityPolicy             capacityPolicyInput `yaml:"capacityPolicy"`
	Placements                 []placementInput    `yaml:"placements"`
}

type placementInput struct {
	NodeIdentity            string `yaml:"nodeIdentity"`
	DaemonSetName           string `yaml:"daemonSetName"`
	WorkerRuntimeConfigMap  string `yaml:"workerRuntimeConfigMap"`
	RunnerProfilesConfigMap string `yaml:"runnerProfilesConfigMap"`
	RunnerGPURolesConfigMap string `yaml:"runnerGPURolesConfigMap"`
	WorkerControlTLSSecret  string `yaml:"workerControlTLSSecret"`
}

type capacityPolicyInput struct {
	WorkerHighWatermarkBytes int64  `yaml:"workerHighWatermarkBytes"`
	WorkerLowWatermarkBytes  int64  `yaml:"workerLowWatermarkBytes"`
	WorkerCriticalFreeBytes  int64  `yaml:"workerCriticalFreeBytes"`
	PoolHighWatermarkBytes   int64  `yaml:"poolHighWatermarkBytes"`
	PoolLowWatermarkBytes    int64  `yaml:"poolLowWatermarkBytes"`
	ObservationMaxAge        string `yaml:"observationMaxAge"`
}

type retirementPlanInput struct {
	Revision                string                     `yaml:"revision"`
	WorkerPoolID            string                     `yaml:"workerPoolID"`
	WorkerPoolName          string                     `yaml:"workerPoolName"`
	WorkerPoolKubernetesUID string                     `yaml:"workerPoolKubernetesUID"`
	Reason                  string                     `yaml:"reason"`
	Deadline                string                     `yaml:"deadline"`
	Placements              []retirementPlacementInput `yaml:"placements"`
}

type retirementPlacementInput struct {
	DaemonSetName          string                  `yaml:"daemonSetName"`
	DaemonSetKubernetesUID string                  `yaml:"daemonSetKubernetesUID"`
	Workers                []workerRetirementInput `yaml:"workers"`
}

type workerRetirementInput struct {
	OperationID      string `yaml:"operationID"`
	WorkerID         string `yaml:"workerID"`
	WorkerEpoch      int64  `yaml:"workerEpoch"`
	PodName          string `yaml:"podName"`
	PodKubernetesUID string `yaml:"podKubernetesUID"`
}

func DecodeDesiredConfiguration(
	encoded []byte,
	namespace string,
) ([]DesiredRevision, []RetirementPlan, error) {
	if len(encoded) == 0 || len(encoded) > MaximumDesiredConfigurationBytes {
		return nil, nil, errors.New("fleet desired input is empty or too large")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(encoded))
	decoder.KnownFields(true)
	var input desiredInput
	if err := decoder.Decode(&input); err != nil {
		return nil, nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, nil, errors.New("fleet desired input must contain exactly one YAML document")
	}
	if input.APIVersion != "fleet.vela.ai/v1alpha1" ||
		input.Kind != "FleetDesiredRevisions" || len(input.Revisions) == 0 {
		return nil, nil, errors.New("fleet desired input apiVersion, kind, or revisions are invalid")
	}
	seenPools := map[uuid.UUID]struct{}{}
	seenNames := map[string]struct{}{}
	seenDaemonSets := map[string]struct{}{}
	revisions := make([]DesiredRevision, 0, len(input.Revisions))
	for _, encoded := range input.Revisions {
		workerPoolID, err := uuid.Parse(encoded.WorkerPoolID)
		if err != nil || workerPoolID == uuid.Nil {
			return nil, nil, errors.New("fleet desired revision WorkerPool id is invalid")
		}
		executionProfileRevisionID, err := uuid.Parse(encoded.ExecutionProfileRevisionID)
		if err != nil || executionProfileRevisionID == uuid.Nil {
			return nil, nil, errors.New("fleet desired revision ExecutionProfileRevision id is invalid")
		}
		readinessTimeout, err := time.ParseDuration(encoded.ReadinessTimeout)
		if err != nil {
			return nil, nil, errors.New("fleet desired revision readiness timeout is invalid")
		}
		observationMaxAge, err := time.ParseDuration(encoded.CapacityPolicy.ObservationMaxAge)
		if err != nil {
			return nil, nil, errors.New("fleet desired revision capacity observation age is invalid")
		}
		desired := DesiredRevision{
			WorkerPoolID: workerPoolID, Namespace: namespace, Name: encoded.Name,
			Revision: encoded.Revision, WorkerProfile: encoded.WorkerProfile,
			NodeSelector: encoded.NodeSelector,
			InitImage:    encoded.InitImage, WorkerAgentImage: encoded.WorkerAgentImage,
			RunnerImage:                encoded.RunnerImage,
			ArtifactStoreTLSSecret:     encoded.ArtifactStoreTLSSecret,
			ExecutionProfileRevisionID: executionProfileRevisionID,
			InferenceBackendRevision:   encoded.InferenceBackendRevision,
			ReadinessTimeout:           readinessTimeout,
			CapacityPolicy: CapacityPolicySpec{
				WorkerHighWatermarkBytes: encoded.CapacityPolicy.WorkerHighWatermarkBytes,
				WorkerLowWatermarkBytes:  encoded.CapacityPolicy.WorkerLowWatermarkBytes,
				WorkerCriticalFreeBytes:  encoded.CapacityPolicy.WorkerCriticalFreeBytes,
				PoolHighWatermarkBytes:   encoded.CapacityPolicy.PoolHighWatermarkBytes,
				PoolLowWatermarkBytes:    encoded.CapacityPolicy.PoolLowWatermarkBytes,
				ObservationMaxAge:        observationMaxAge,
			},
		}
		desired.Placements = make([]WorkerPlacement, 0, len(encoded.Placements))
		for _, placement := range encoded.Placements {
			desired.Placements = append(desired.Placements, WorkerPlacement{
				NodeIdentity: placement.NodeIdentity, DaemonSetName: placement.DaemonSetName,
				WorkerRuntimeConfigMap:  placement.WorkerRuntimeConfigMap,
				RunnerProfilesConfigMap: placement.RunnerProfilesConfigMap,
				RunnerGPURolesConfigMap: placement.RunnerGPURolesConfigMap,
				WorkerControlTLSSecret:  placement.WorkerControlTLSSecret,
			})
		}
		if err := ValidateDesiredRevision(desired); err != nil {
			return nil, nil, err
		}
		if _, ok := seenPools[workerPoolID]; ok {
			return nil, nil, errors.New("fleet desired input contains a duplicate WorkerPool id")
		}
		if _, ok := seenNames[desired.Name]; ok {
			return nil, nil, errors.New("fleet desired input contains a duplicate WorkerPool name")
		}
		for _, placement := range desired.Placements {
			if _, ok := seenDaemonSets[placement.DaemonSetName]; ok {
				return nil, nil, errors.New("fleet desired input contains a duplicate DaemonSet name")
			}
			seenDaemonSets[placement.DaemonSetName] = struct{}{}
		}
		seenPools[workerPoolID] = struct{}{}
		seenNames[desired.Name] = struct{}{}
		revisions = append(revisions, desired)
	}
	retirementPlans := make([]RetirementPlan, 0, len(input.Retirements))
	seenPlanRevisions := make(map[string]struct{}, len(input.Retirements))
	seenDrainOperations := make(map[uuid.UUID]struct{})
	for _, encoded := range input.Retirements {
		workerPoolID, err := uuid.Parse(encoded.WorkerPoolID)
		if err != nil || workerPoolID == uuid.Nil {
			return nil, nil, errors.New("fleet retirement plan WorkerPool id is invalid")
		}
		deadline, err := time.Parse(time.RFC3339, encoded.Deadline)
		if err != nil {
			return nil, nil, errors.New("fleet retirement plan deadline must be RFC3339")
		}
		plan := RetirementPlan{
			Revision: encoded.Revision, WorkerPoolID: workerPoolID, Namespace: namespace,
			WorkerPoolName:          encoded.WorkerPoolName,
			WorkerPoolKubernetesUID: encoded.WorkerPoolKubernetesUID,
			Reason:                  encoded.Reason, Deadline: deadline.UTC(),
			Placements: make([]RetirementPlacement, 0, len(encoded.Placements)),
		}
		for _, encodedPlacement := range encoded.Placements {
			placement := RetirementPlacement{
				DaemonSetName:          encodedPlacement.DaemonSetName,
				DaemonSetKubernetesUID: encodedPlacement.DaemonSetKubernetesUID,
				Workers:                make([]WorkerRetirement, 0, len(encodedPlacement.Workers)),
			}
			for _, encodedWorker := range encodedPlacement.Workers {
				operationID, operationErr := uuid.Parse(encodedWorker.OperationID)
				workerID, workerErr := uuid.Parse(encodedWorker.WorkerID)
				if operationErr != nil || operationID == uuid.Nil ||
					workerErr != nil || workerID == uuid.Nil {
					return nil, nil, errors.New("fleet retirement Worker identity is invalid")
				}
				if _, exists := seenDrainOperations[operationID]; exists {
					return nil, nil, errors.New("fleet desired input reuses a retirement DrainOperation id")
				}
				seenDrainOperations[operationID] = struct{}{}
				placement.Workers = append(placement.Workers, WorkerRetirement{
					OperationID: operationID, WorkerID: workerID,
					WorkerEpoch: encodedWorker.WorkerEpoch, PodName: encodedWorker.PodName,
					PodKubernetesUID: encodedWorker.PodKubernetesUID,
				})
			}
			plan.Placements = append(plan.Placements, placement)
		}
		if err := ValidateRetirementPlan(plan); err != nil {
			return nil, nil, err
		}
		if _, exists := seenPlanRevisions[plan.Revision]; exists {
			return nil, nil, errors.New("fleet desired input contains a duplicate retirement plan revision")
		}
		seenPlanRevisions[plan.Revision] = struct{}{}
		retirementPlans = append(retirementPlans, plan)
	}
	if err := ValidateRuntimeConfiguration(revisions, retirementPlans); err != nil {
		return nil, nil, err
	}
	return revisions, retirementPlans, nil
}
