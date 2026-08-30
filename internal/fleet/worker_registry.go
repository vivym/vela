package fleet

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type WorkerInstanceLifecycle string

const (
	WorkerInstanceProvisioning WorkerInstanceLifecycle = "PROVISIONING"
	WorkerInstanceWarming      WorkerInstanceLifecycle = "WARMING"
	WorkerInstanceReady        WorkerInstanceLifecycle = "READY"
	WorkerInstanceDraining     WorkerInstanceLifecycle = "DRAINING"
	WorkerInstanceFenced       WorkerInstanceLifecycle = "FENCED"
	WorkerInstanceRetired      WorkerInstanceLifecycle = "RETIRED"
)

type WorkerRegistryAndFleet interface {
	Propose(context.Context, ResidencyPlanInputs) (ResidencyProposal, error)
	Apply(context.Context, ApprovedResidencyPlan) (ActuationPlan, error)
	Observe(context.Context, WorkerInstanceEvidence) (WorkerInstanceDecision, error)
	AuthorityMatches(context.Context, WorkerInstanceAuthority) (bool, error)
	Drain(context.Context, WorkerInstanceDrainRequest) (WorkerInstanceTransition, error)
	Fence(context.Context, WorkerInstanceFenceRequest) (WorkerInstanceTransition, error)
}

type ResidencyPlanInputs struct {
	ProposalID       uuid.UUID
	InputDigest      []byte
	ConfidencePPM    int
	ExpiresAt        time.Time
	MinCapacity      map[string]int64
	DesiredCapacity  map[string]int64
	MaxCapacity      map[string]int64
	Cooldown         time.Duration
	BudgetMicroUnits int64
	ReasonCodes      []string
	ProposedBy       string
}

type ResidencyProposal struct {
	ID uuid.UUID
}

type ApprovedResidencyPlan struct {
	SchemaVersion          int                     `json:"schema_version"`
	ID                     uuid.UUID               `json:"id"`
	StableID               string                  `json:"stable_id"`
	Revision               int                     `json:"revision"`
	SourceProposalID       *uuid.UUID              `json:"source_proposal_id,omitempty"`
	ContentDigest          string                  `json:"content_digest"`
	ApprovalEvidenceDigest string                  `json:"approval_evidence_digest"`
	ApprovedAt             time.Time               `json:"approved_at"`
	ApprovedBy             string                  `json:"approved_by"`
	CapacityPools          []PlannedCapacityPool   `json:"capacity_pools"`
	WorkerBundles          []PlannedWorkerBundle   `json:"worker_bundles"`
	WorkerInstances        []PlannedWorkerInstance `json:"worker_instances"`
}

type PlannedCapacityPool struct {
	ID                     uuid.UUID `json:"id"`
	StableID               string    `json:"stable_id"`
	StageProfileRevisionID uuid.UUID `json:"stage_profile_revision_id"`
	ResourceClass          string    `json:"resource_class"`
	SecurityClass          string    `json:"security_class"`
	Region                 string    `json:"region"`
	MaxReadyQueueDepth     int       `json:"max_ready_queue_depth"`
}

type PlannedWorkerBundle struct {
	ID                uuid.UUID `json:"id"`
	StableID          string    `json:"stable_id"`
	DesiredGeneration int64     `json:"desired_generation"`
	LayoutDigest      string    `json:"layout_digest"`
}

type PlannedWorkerInstance struct {
	ID                      uuid.UUID `json:"id"`
	WorkerProfileRevisionID uuid.UUID `json:"worker_profile_revision_id"`
	CapacityPoolID          uuid.UUID `json:"capacity_pool_id"`
	WorkerBundleID          uuid.UUID `json:"worker_bundle_id"`
	DesiredMemberCount      int       `json:"desired_member_count"`
	DesiredDeviceCount      int       `json:"desired_device_count"`
}

type ActuationPlan struct {
	PlanRevisionID      uuid.UUID
	WorkerInstanceCount int
}

type WorkerInstanceEvidence struct {
	SchemaVersion       int                      `json:"schema_version"`
	WorkerInstanceID    uuid.UUID                `json:"worker_instance_id"`
	InstanceEpoch       int64                    `json:"instance_epoch"`
	ControlSessionEpoch int64                    `json:"control_session_epoch"`
	DeviceSet           WorkerDeviceSetEvidence  `json:"device_set"`
	Members             []WorkerMemberEvidence   `json:"members"`
	Residencies         []ModelResidencyEvidence `json:"residencies"`
	Capacity            WorkerCapacityEvidence   `json:"capacity"`
	ObservedAt          time.Time                `json:"observed_at"`
	ObservedBy          string                   `json:"observed_by"`
}

type WorkerDeviceSetEvidence struct {
	ID               uuid.UUID              `json:"id"`
	MembershipDigest string                 `json:"membership_digest"`
	TopologyDigest   string                 `json:"topology_digest"`
	Devices          []WorkerDeviceEvidence `json:"devices"`
}

type WorkerDeviceEvidence struct {
	ID                    uuid.UUID `json:"id"`
	ComputeNodeID         uuid.UUID `json:"compute_node_id"`
	NodeIdentity          string    `json:"node_identity"`
	Region                string    `json:"region"`
	NetworkDomain         string    `json:"network_domain"`
	FaultDomain           string    `json:"fault_domain"`
	NodeEpoch             int64     `json:"node_epoch"`
	AgentSessionEpoch     int64     `json:"agent_session_epoch"`
	NodeAttestationDigest string    `json:"node_attestation_digest"`
	Kind                  string    `json:"kind"`
	GPUUUID               string    `json:"gpu_uuid,omitempty"`
	PCIBDF                string    `json:"pci_bdf,omitempty"`
	DeviceEpoch           int64     `json:"device_epoch"`
	Ordinal               int       `json:"ordinal"`
	Health                string    `json:"health"`
	AttestationDigest     string    `json:"attestation_digest"`
}

type WorkerMemberEvidence struct {
	ID                 uuid.UUID   `json:"id"`
	MemberKey          string      `json:"member_key"`
	ComputeNodeID      uuid.UUID   `json:"compute_node_id"`
	WorkerBundleID     *uuid.UUID  `json:"worker_bundle_id,omitempty"`
	MemberEpoch        int64       `json:"member_epoch"`
	DeviceIDs          []uuid.UUID `json:"device_ids"`
	DeviceSubsetDigest string      `json:"device_subset_digest"`
	IdentityDigest     string      `json:"identity_digest"`
	Readiness          string      `json:"readiness"`
}

type ModelResidencyEvidence struct {
	ID                     uuid.UUID `json:"id"`
	ModelComponentRevision string    `json:"model_component_revision"`
	RuntimeIdentity        string    `json:"runtime_identity"`
	RuntimeImageDigest     string    `json:"runtime_image_digest"`
	ModelRuntimeEpoch      int64     `json:"model_runtime_epoch"`
	State                  string    `json:"state"`
	WarmupEvidenceDigest   string    `json:"warmup_evidence_digest"`
	CanaryEvidenceDigest   string    `json:"canary_evidence_digest"`
}

type WorkerCapacityEvidence struct {
	Sequence   int64            `json:"sequence"`
	Vector     map[string]int64 `json:"vector"`
	ObservedAt time.Time        `json:"observed_at"`
	ExpiresAt  time.Time        `json:"expires_at"`
}

type WorkerInstanceDecision struct {
	WorkerInstanceID    uuid.UUID
	InstanceEpoch       int64
	ControlSessionEpoch int64
	ModelRuntimeEpoch   int64
	Readiness           WorkerInstanceLifecycle
}

type WorkerInstanceAuthority struct {
	WorkerInstanceID  uuid.UUID
	InstanceEpoch     int64
	DeviceSetDigest   []byte
	MembershipDigest  []byte
	ModelResidencyID  uuid.UUID
	ModelRuntimeEpoch int64
}

type WorkerInstanceDrainRequest struct {
	WorkerInstanceID      uuid.UUID
	ExpectedInstanceEpoch int64
	Reason                string
	RequestedBy           string
}

type WorkerInstanceFenceRequest struct {
	WorkerInstanceID      uuid.UUID
	ExpectedInstanceEpoch int64
	Reason                string
	ObservedBy            string
}

type WorkerInstanceTransition struct {
	WorkerInstanceID uuid.UUID
	InstanceEpoch    int64
	Lifecycle        WorkerInstanceLifecycle
}

type residencyProposalPayload struct {
	SchemaVersion    int              `json:"schema_version"`
	ID               uuid.UUID        `json:"id"`
	InputDigest      string           `json:"input_digest"`
	ConfidencePPM    int              `json:"confidence_ppm"`
	ExpiresAt        time.Time        `json:"expires_at"`
	MinCapacity      map[string]int64 `json:"min_capacity"`
	DesiredCapacity  map[string]int64 `json:"desired_capacity"`
	MaxCapacity      map[string]int64 `json:"max_capacity"`
	CooldownSeconds  int64            `json:"cooldown_seconds"`
	BudgetMicroUnits int64            `json:"budget_micro_units"`
	ReasonCodes      []string         `json:"reason_codes"`
	ProposedBy       string           `json:"proposed_by"`
}

func (service *Service) Propose(
	ctx context.Context,
	inputs ResidencyPlanInputs,
) (ResidencyProposal, error) {
	if service == nil || service.registryPool == nil {
		return ResidencyProposal{}, errors.New("fleet service is not configured")
	}
	if err := validateResidencyPlanInputs(inputs); err != nil {
		return ResidencyProposal{}, &Failure{Code: FailureInvalid, Message: err.Error()}
	}
	payload, err := json.Marshal(residencyProposalPayload{
		SchemaVersion:    1,
		ID:               inputs.ProposalID,
		InputDigest:      hex.EncodeToString(inputs.InputDigest),
		ConfidencePPM:    inputs.ConfidencePPM,
		ExpiresAt:        inputs.ExpiresAt,
		MinCapacity:      inputs.MinCapacity,
		DesiredCapacity:  inputs.DesiredCapacity,
		MaxCapacity:      inputs.MaxCapacity,
		CooldownSeconds:  int64(inputs.Cooldown / time.Second),
		BudgetMicroUnits: inputs.BudgetMicroUnits,
		ReasonCodes:      inputs.ReasonCodes,
		ProposedBy:       inputs.ProposedBy,
	})
	if err != nil {
		return ResidencyProposal{}, &Failure{Code: FailureInvalid, Message: "encode ResidencyProposal: " + err.Error()}
	}
	var proposal ResidencyProposal
	if err := service.registryPool.QueryRow(ctx, `
		SELECT proposal_id FROM vela_record_residency_proposal($1::jsonb)
	`, payload).Scan(&proposal.ID); err != nil {
		return ResidencyProposal{}, mapDatabaseError("record ResidencyProposal", err)
	}
	return proposal, nil
}

func (service *Service) Apply(
	ctx context.Context,
	plan ApprovedResidencyPlan,
) (ActuationPlan, error) {
	if service == nil || service.registryPool == nil {
		return ActuationPlan{}, errors.New("fleet service is not configured")
	}
	if err := validateApprovedResidencyPlan(plan); err != nil {
		return ActuationPlan{}, &Failure{Code: FailureInvalid, Message: err.Error()}
	}
	payload, err := json.Marshal(plan)
	if err != nil {
		return ActuationPlan{}, &Failure{Code: FailureInvalid, Message: "encode ResidencyPlan: " + err.Error()}
	}
	var result ActuationPlan
	if err := service.registryPool.QueryRow(ctx, `
		SELECT plan_revision_id, worker_instance_count
		FROM vela_apply_residency_plan($1::jsonb)
	`, payload).Scan(&result.PlanRevisionID, &result.WorkerInstanceCount); err != nil {
		return ActuationPlan{}, mapDatabaseError("apply ResidencyPlan", err)
	}
	return result, nil
}

func (service *Service) Observe(
	ctx context.Context,
	evidence WorkerInstanceEvidence,
) (WorkerInstanceDecision, error) {
	if service == nil || service.registryPool == nil {
		return WorkerInstanceDecision{}, errors.New("fleet service is not configured")
	}
	if err := validateWorkerInstanceEvidence(evidence); err != nil {
		return WorkerInstanceDecision{}, &Failure{Code: FailureInvalid, Message: err.Error()}
	}
	payload, err := json.Marshal(evidence)
	if err != nil {
		return WorkerInstanceDecision{}, &Failure{Code: FailureInvalid, Message: "encode WorkerInstance evidence: " + err.Error()}
	}
	var decision WorkerInstanceDecision
	if err := service.registryPool.QueryRow(ctx, `
		SELECT worker_instance_id, instance_epoch, control_session_epoch,
			model_runtime_epoch, readiness
		FROM vela_observe_worker_instance($1::jsonb)
	`, payload).Scan(
		&decision.WorkerInstanceID,
		&decision.InstanceEpoch,
		&decision.ControlSessionEpoch,
		&decision.ModelRuntimeEpoch,
		&decision.Readiness,
	); err != nil {
		return WorkerInstanceDecision{}, mapDatabaseError("observe WorkerInstance", err)
	}
	return decision, nil
}

func (service *Service) AuthorityMatches(
	ctx context.Context,
	authority WorkerInstanceAuthority,
) (bool, error) {
	if service == nil || service.registryPool == nil {
		return false, errors.New("fleet service is not configured")
	}
	if authority.WorkerInstanceID == uuid.Nil || authority.InstanceEpoch <= 0 ||
		len(authority.DeviceSetDigest) != 32 || len(authority.MembershipDigest) != 32 ||
		authority.ModelResidencyID == uuid.Nil || authority.ModelRuntimeEpoch <= 0 {
		return false, &Failure{
			Code: FailureInvalid, Message: "WorkerInstance authority is invalid",
		}
	}
	var matches bool
	if err := service.registryPool.QueryRow(ctx, `
		SELECT vela_worker_instance_authority_matches($1, $2, $3, $4, $5, $6)
	`, authority.WorkerInstanceID, authority.InstanceEpoch,
		authority.DeviceSetDigest, authority.MembershipDigest,
		authority.ModelResidencyID, authority.ModelRuntimeEpoch).Scan(&matches); err != nil {
		return false, mapDatabaseError("match WorkerInstance authority", err)
	}
	return matches, nil
}

func (service *Service) Drain(
	ctx context.Context,
	request WorkerInstanceDrainRequest,
) (WorkerInstanceTransition, error) {
	if service == nil || service.registryPool == nil {
		return WorkerInstanceTransition{}, errors.New("fleet service is not configured")
	}
	if request.WorkerInstanceID == uuid.Nil || request.ExpectedInstanceEpoch <= 0 ||
		!validText(request.Reason, 500) || !validText(request.RequestedBy, 500) {
		return WorkerInstanceTransition{}, &Failure{
			Code: FailureInvalid, Message: "WorkerInstance drain request is invalid",
		}
	}
	result := WorkerInstanceTransition{WorkerInstanceID: request.WorkerInstanceID}
	if err := service.registryPool.QueryRow(ctx, `
		SELECT instance_epoch, lifecycle_state
		FROM vela_begin_worker_instance_drain($1, $2, $3, $4)
	`, request.WorkerInstanceID, request.ExpectedInstanceEpoch,
		request.Reason, request.RequestedBy).Scan(
		&result.InstanceEpoch,
		&result.Lifecycle,
	); err != nil {
		return WorkerInstanceTransition{}, mapDatabaseError("drain WorkerInstance", err)
	}
	return result, nil
}

func (service *Service) Fence(
	ctx context.Context,
	request WorkerInstanceFenceRequest,
) (WorkerInstanceTransition, error) {
	if service == nil || service.registryPool == nil {
		return WorkerInstanceTransition{}, errors.New("fleet service is not configured")
	}
	if request.WorkerInstanceID == uuid.Nil || request.ExpectedInstanceEpoch <= 0 ||
		!validText(request.Reason, 500) || !validText(request.ObservedBy, 500) {
		return WorkerInstanceTransition{}, &Failure{
			Code: FailureInvalid, Message: "WorkerInstance fence request is invalid",
		}
	}
	result := WorkerInstanceTransition{WorkerInstanceID: request.WorkerInstanceID}
	if err := service.registryPool.QueryRow(ctx, `
		SELECT instance_epoch, lifecycle_state
		FROM vela_fence_worker_instance($1, $2, $3, $4)
	`, request.WorkerInstanceID, request.ExpectedInstanceEpoch,
		request.Reason, request.ObservedBy).Scan(
		&result.InstanceEpoch,
		&result.Lifecycle,
	); err != nil {
		return WorkerInstanceTransition{}, mapDatabaseError("fence WorkerInstance", err)
	}
	return result, nil
}

func validateResidencyPlanInputs(inputs ResidencyPlanInputs) error {
	if inputs.ProposalID == uuid.Nil || len(inputs.InputDigest) != 32 ||
		inputs.ConfidencePPM < 0 || inputs.ConfidencePPM > 1_000_000 ||
		inputs.ExpiresAt.IsZero() || len(inputs.MinCapacity) == 0 ||
		len(inputs.DesiredCapacity) == 0 || len(inputs.MaxCapacity) == 0 ||
		inputs.Cooldown < time.Second || inputs.Cooldown%time.Second != 0 ||
		inputs.BudgetMicroUnits < 0 || len(inputs.ReasonCodes) == 0 ||
		!validText(inputs.ProposedBy, 500) {
		return errors.New("ResidencyProposal inputs are invalid")
	}
	for _, reason := range inputs.ReasonCodes {
		if !validText(reason, 100) {
			return errors.New("ResidencyProposal reason code is invalid")
		}
	}
	return nil
}

func validateApprovedResidencyPlan(plan ApprovedResidencyPlan) error {
	if plan.SchemaVersion != 1 || plan.ID == uuid.Nil || !validText(plan.StableID, 100) ||
		plan.Revision <= 0 || !validDigestHex(plan.ContentDigest) ||
		!validDigestHex(plan.ApprovalEvidenceDigest) || plan.ApprovedAt.IsZero() ||
		!validText(plan.ApprovedBy, 500) || len(plan.CapacityPools) == 0 ||
		len(plan.WorkerBundles) == 0 || len(plan.WorkerInstances) == 0 {
		return errors.New("approved ResidencyPlan is invalid")
	}
	return nil
}

func validateWorkerInstanceEvidence(evidence WorkerInstanceEvidence) error {
	if evidence.SchemaVersion != 1 || evidence.WorkerInstanceID == uuid.Nil ||
		evidence.InstanceEpoch <= 0 || evidence.ControlSessionEpoch <= 0 ||
		evidence.DeviceSet.ID == uuid.Nil || !validDigestHex(evidence.DeviceSet.MembershipDigest) ||
		!validDigestHex(evidence.DeviceSet.TopologyDigest) || len(evidence.DeviceSet.Devices) == 0 ||
		len(evidence.Members) == 0 || len(evidence.Residencies) == 0 ||
		evidence.Capacity.Sequence <= 0 || len(evidence.Capacity.Vector) == 0 ||
		evidence.ObservedAt.IsZero() || !validText(evidence.ObservedBy, 500) {
		return errors.New("WorkerInstance evidence is invalid")
	}
	seenDevices := make(map[uuid.UUID]struct{}, len(evidence.DeviceSet.Devices))
	for _, device := range evidence.DeviceSet.Devices {
		if device.ID == uuid.Nil || device.ComputeNodeID == uuid.Nil ||
			device.NodeEpoch <= 0 || device.AgentSessionEpoch <= 0 || device.DeviceEpoch <= 0 ||
			!validDigestHex(device.NodeAttestationDigest) ||
			!validDigestHex(device.AttestationDigest) {
			return errors.New("WorkerInstance device evidence is invalid")
		}
		if _, exists := seenDevices[device.ID]; exists {
			return errors.New("WorkerInstance device evidence contains a duplicate device")
		}
		seenDevices[device.ID] = struct{}{}
	}
	coveredDevices := make(map[uuid.UUID]struct{}, len(seenDevices))
	for _, member := range evidence.Members {
		if member.ID == uuid.Nil || member.ComputeNodeID == uuid.Nil || member.MemberEpoch <= 0 ||
			len(member.DeviceIDs) == 0 || !validDigestHex(member.DeviceSubsetDigest) ||
			!validDigestHex(member.IdentityDigest) {
			return errors.New("WorkerInstance member evidence is invalid")
		}
		for _, deviceID := range member.DeviceIDs {
			if _, exists := seenDevices[deviceID]; !exists {
				return errors.New("WorkerInstance member references an unknown device")
			}
			if _, exists := coveredDevices[deviceID]; exists {
				return errors.New("WorkerInstance device is assigned to multiple members")
			}
			coveredDevices[deviceID] = struct{}{}
		}
	}
	if len(coveredDevices) != len(seenDevices) {
		return errors.New("WorkerInstance member evidence does not cover every device")
	}
	return nil
}

func validDigestHex(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}
