package h3launchevidence

import (
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontroller"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
)

const (
	SchemaVersion = 3
	MediaType     = "application/vnd.vela.h3-launch-evidence.v3+json"
)

type ExternalResourceExpectation struct {
	Kind         string
	Namespace    string
	Name         string
	Revision     string
	RequiredKeys []string
}

// Input contains objects captured directly from the Kubernetes API and the
// authoritative Fleet database. It is intentionally not a JSON evidence
// format: the collector must obtain these values from the live systems.
type Input struct {
	ReleaseDigest         string
	ConfigurationRevision string
	ValidationEnvironment string
	CollectorIdentity     string
	CapturedAt            time.Time
	Rollout               fleetcontroller.ResidencyPlanRollout
	ExternalResources     []ExternalResourceExpectation
	Kubernetes            KubernetesSnapshot
	Registry              RegistrySnapshot
}

type KubernetesSnapshot struct {
	ClusterUID     string
	NamespaceUID   string
	ConfigMaps     []corev1.ConfigMap
	Secrets        []corev1.Secret
	Pods           []corev1.Pod
	ClaimTemplates []resourcev1.ResourceClaimTemplate
	Claims         []resourcev1.ResourceClaim
	Nodes          []corev1.Node
	ResourceSlices []resourcev1.ResourceSlice
}

type RegistrySnapshot struct {
	DatabaseTime  time.Time
	TransactionID string
	SnapshotID    string
	Workers       []RegistryWorker
}

type RegistryWorker struct {
	ID                      uuid.UUID
	InstanceEpoch           int64
	ControlSessionEpoch     int64
	ResidencyPlanRevisionID uuid.UUID
	WorkerBundleID          uuid.UUID
	WorkerProfileRevisionID uuid.UUID
	CapacityPoolID          uuid.UUID
	Lifecycle               string
	Reachability            string
	DeviceSetID             uuid.UUID
	DeviceSetDigest         string
	MembershipDigest        string
	Members                 []RegistryMember
	Residencies             []RegistryResidency
}

type RegistryMember struct {
	ID                 uuid.UUID
	Key                string
	MemberEpoch        int64
	ComputeNodeID      uuid.UUID
	NodeIdentity       string
	Readiness          string
	DeviceSubsetDigest string
	IdentityDigest     string
	Devices            []RegistryDevice
}

type RegistryDevice struct {
	ID                      uuid.UUID
	DeviceEpoch             int64
	ComputeNodeID           uuid.UUID
	NodeIdentity            string
	NodeEpoch               int64
	AgentSessionEpoch       int64
	GPUUUID                 string
	PCIBDF                  string
	Health                  string
	NodeAttestationDigest   string
	DeviceAttestationDigest string
}

type RegistryResidency struct {
	ID                     uuid.UUID `json:"id"`
	CapacityPoolID         uuid.UUID `json:"capacity_pool_id"`
	StageProfileRevisionID uuid.UUID `json:"stage_profile_revision_id"`
	ModelComponentRevision string    `json:"model_component_revision"`
	RuntimeIdentity        string    `json:"runtime_identity"`
	RuntimeImageDigest     string    `json:"runtime_image_digest"`
	ModelRuntimeEpoch      int64     `json:"model_runtime_epoch"`
	State                  string    `json:"state"`
	WarmupEvidenceDigest   string    `json:"warmup_evidence_digest"`
	CanaryEvidenceDigest   string    `json:"canary_evidence_digest"`
}

// Evidence is the canonical, sanitized result of a successful live
// verification. It is an input to a release evidence campaign, not a Launch
// Receipt by itself.
type Evidence struct {
	SchemaVersion           int                        `json:"schema_version"`
	MediaType               string                     `json:"media_type"`
	ReleaseDigest           string                     `json:"release_digest"`
	ConfigurationRevision   string                     `json:"configuration_revision"`
	ValidationEnvironment   string                     `json:"validation_environment"`
	CollectorIdentity       string                     `json:"collector_identity"`
	CapturedAt              time.Time                  `json:"captured_at"`
	KubernetesClusterUID    string                     `json:"kubernetes_cluster_uid"`
	KubernetesNamespaceUID  string                     `json:"kubernetes_namespace_uid"`
	RegistryDatabaseTime    time.Time                  `json:"registry_database_time"`
	RegistryTransactionID   string                     `json:"registry_transaction_id"`
	RegistrySnapshotID      string                     `json:"registry_snapshot_id"`
	ResidencyPlanRevisionID uuid.UUID                  `json:"residency_plan_revision_id"`
	ExternalResources       []ExternalResourceEvidence `json:"external_resources"`
	Workers                 []WorkerEvidence           `json:"workers"`
}

type ExternalResourceEvidence struct {
	Kind            string   `json:"kind"`
	Namespace       string   `json:"namespace"`
	Name            string   `json:"name"`
	Revision        string   `json:"revision"`
	RequiredKeys    []string `json:"required_keys,omitempty"`
	UID             string   `json:"uid"`
	ResourceVersion string   `json:"resource_version"`
	ContentDigest   string   `json:"content_digest"`
}

type WorkerEvidence struct {
	WorkerInstanceID      uuid.UUID           `json:"worker_instance_id"`
	InstanceEpoch         int64               `json:"instance_epoch"`
	ControlSessionEpoch   int64               `json:"control_session_epoch"`
	WorkerBundleID        uuid.UUID           `json:"worker_bundle_id"`
	WorkerProfileRevision uuid.UUID           `json:"worker_profile_revision_id"`
	CapacityPoolID        uuid.UUID           `json:"capacity_pool_id"`
	Role                  string              `json:"role"`
	DeviceSetID           uuid.UUID           `json:"device_set_id"`
	DeviceSetDigest       string              `json:"device_set_digest"`
	MembershipDigest      string              `json:"membership_digest"`
	Members               []MemberEvidence    `json:"members"`
	Residencies           []RegistryResidency `json:"residencies"`
}

type MemberEvidence struct {
	WorkerMemberID           uuid.UUID `json:"worker_member_id"`
	MemberEpoch              int64     `json:"member_epoch"`
	MemberKey                string    `json:"member_key"`
	PodName                  string    `json:"pod_name"`
	PodUID                   string    `json:"pod_uid"`
	PodResourceVersion       string    `json:"pod_resource_version"`
	NodeName                 string    `json:"node_name"`
	NodeUID                  string    `json:"node_uid"`
	NodeResourceVersion      string    `json:"node_resource_version"`
	ClaimTemplateName        string    `json:"claim_template_name"`
	ClaimTemplateUID         string    `json:"claim_template_uid"`
	ClaimTemplateVersion     string    `json:"claim_template_resource_version"`
	ResourceClaimName        string    `json:"resource_claim_name"`
	ResourceClaimUID         string    `json:"resource_claim_uid"`
	ResourceClaimVersion     string    `json:"resource_claim_resource_version"`
	DRAResourceSliceUID      string    `json:"dra_resource_slice_uid"`
	DRAResourceSliceVersion  string    `json:"dra_resource_slice_resource_version"`
	DRADriver                string    `json:"dra_driver"`
	DRAPool                  string    `json:"dra_pool"`
	DRADevice                string    `json:"dra_device"`
	DeviceID                 uuid.UUID `json:"device_id"`
	DeviceEpoch              int64     `json:"device_epoch"`
	GPUUUID                  string    `json:"gpu_uuid"`
	PCIBDF                   string    `json:"pci_bdf"`
	StageWorkerImageID       string    `json:"stage_worker_image_id"`
	ModelRuntimeImageID      string    `json:"model_runtime_image_id"`
	StageWorkerRestartCount  int32     `json:"stage_worker_restart_count"`
	ModelRuntimeRestartCount int32     `json:"model_runtime_restart_count"`
}
