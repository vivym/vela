package h3campaignevidence

import (
	"time"

	"github.com/google/uuid"
)

const (
	SchemaVersion = 1
	MediaType     = "application/vnd.vela.h3-campaign-evidence.v1+json"
)

type RunKind string

const (
	RunSameNode  RunKind = "SAME_NODE"
	RunCrossNode RunKind = "CROSS_NODE"
)

type EvidenceBinding struct {
	ReleaseDigest           string
	ConfigurationRevision   string
	ValidationEnvironment   string
	CollectorIdentity       string
	ResidencyPlanRevisionID uuid.UUID
	seal                    [32]byte
}

// Input is a live database snapshot. It is intentionally separate from the
// emitted JSON evidence so callers cannot substitute an operator-authored
// evidence document for authoritative rows.
type Input struct {
	EvidenceBinding
	CapturedAt          time.Time
	InitialDatabaseRead DatabaseReadProvenance
	FinalDatabaseRead   DatabaseReadProvenance
	Runs                []RunSnapshot
	CacheRun            CacheRunSnapshot
}

type Selection struct {
	SameNodeJobID  uuid.UUID
	CrossNodeJobID uuid.UUID
	CacheJobID     uuid.UUID
}

type DatabaseSnapshot struct {
	Provenance DatabaseReadProvenance
	Runs       []RunSnapshot
	CacheRun   CacheRunSnapshot
}

type DatabaseReadProvenance struct {
	DatabaseTime time.Time `json:"database_time"`
	BackendPID   string    `json:"backend_pid"`
	SnapshotID   string    `json:"snapshot_id"`
}

type CaptureRequest struct {
	EvidenceBinding
	Selection Selection
}

type RunSnapshot struct {
	Kind                     RunKind
	JobID                    uuid.UUID
	AttemptID                uuid.UUID
	AttemptFence             int64
	ExecutionGraphRevisionID uuid.UUID
	JobState                 string
	AttemptState             string
	GraphState               string
	Stages                   []StageSnapshot
	Transfers                []TransferSnapshot
	ArtifactSetCount         int64
	VisibleCompletionCount   int64
	ChargeCount              int64
}

type StageSnapshot struct {
	StageKey                 string
	State                    string
	SourceKind               string
	StageRunID               uuid.UUID
	StageAttemptID           uuid.UUID
	StageProfileRevisionID   uuid.UUID
	ArtifactID               uuid.UUID
	ObjectVersion            string
	OutputDigest             string
	StageInterfaceRevisionID uuid.UUID
	InputStageArtifactIDs    []uuid.UUID
	RootInputDigest          string
	NodeIdentity             string
	WorkerInstanceID         uuid.UUID
	WorkerInstanceEpoch      int64
	ResidencyPlanRevisionID  uuid.UUID
	WorkerMemberID           uuid.UUID
	MemberEpoch              int64
	DeviceID                 uuid.UUID
	DeviceEpoch              int64
	ModelResidencyID         uuid.UUID
	ModelRuntimeEpoch        int64
}

type TransferSnapshot struct {
	ID                             uuid.UUID
	SourceStageRunID               uuid.UUID
	DestinationStageRunID          uuid.UUID
	StageArtifactID                uuid.UUID
	DestinationWorkerInstanceID    uuid.UUID
	DestinationWorkerInstanceEpoch int64
	ConnectorRevisionID            uuid.UUID
	State                          string
}

type CacheRunSnapshot struct {
	OrganizationID         uuid.UUID
	ProjectID              uuid.UUID
	JobID                  uuid.UUID
	AttemptID              uuid.UUID
	AttemptFence           int64
	JobState               string
	AttemptState           string
	GraphState             string
	ArtifactSetCount       int64
	VisibleCompletionCount int64
	ChargeCount            int64
	Hits                   []CacheHitSnapshot
	PhysicalWorkers        []WorkerPlanSnapshot
}

type WorkerPlanSnapshot struct {
	StageKey                string
	WorkerInstanceID        uuid.UUID
	ResidencyPlanRevisionID uuid.UUID
}

type CacheHitSnapshot struct {
	StageKey                      string
	EntryID                       uuid.UUID
	EntryState                    string
	Scope                         string
	ScopeProjectID                uuid.UUID
	OrganizationID                uuid.UUID
	SourceProjectID               uuid.UUID
	SourceJobID                   uuid.UUID
	SourceStageRunID              uuid.UUID
	SourceResidencyPlanRevisionID uuid.UUID
	TargetJobID                   uuid.UUID
	TargetStageRunID              uuid.UUID
	StageArtifactID               uuid.UUID
	ExactObjectVersion            string
	ArtifactDigest                string
	CacheKeyDigest                string
	CachePolicyRevisionID         uuid.UUID
	ResultEquivalenceRevisionID   uuid.UUID
	ReferenceID                   uuid.UUID
	ReferenceState                string
	ReferenceAcquiredAt           time.Time
	ExecutionPinID                uuid.UUID
	PinKind                       string
	PinState                      string
	PinAcquiredAt                 time.Time
	OutputBindingSourceKind       string
	OutputBindingArtifactID       uuid.UUID
}

type Evidence struct {
	SchemaVersion           int                    `json:"schema_version"`
	MediaType               string                 `json:"media_type"`
	ReleaseDigest           string                 `json:"release_digest"`
	ConfigurationRevision   string                 `json:"configuration_revision"`
	ValidationEnvironment   string                 `json:"validation_environment"`
	CollectorIdentity       string                 `json:"collector_identity"`
	CapturedAt              time.Time              `json:"captured_at"`
	ResidencyPlanRevisionID uuid.UUID              `json:"residency_plan_revision_id"`
	InitialDatabaseRead     DatabaseReadProvenance `json:"initial_database_read"`
	FinalDatabaseRead       DatabaseReadProvenance `json:"final_database_read"`
	Runs                    []RunEvidence          `json:"runs"`
	CacheRun                CacheRunEvidence       `json:"cache_run"`
}

type RunEvidence struct {
	Kind                      RunKind     `json:"kind"`
	JobID                     uuid.UUID   `json:"job_id"`
	AttemptID                 uuid.UUID   `json:"attempt_id"`
	AttemptFence              int64       `json:"attempt_fence"`
	ExecutionGraphRevisionID  uuid.UUID   `json:"execution_graph_revision_id"`
	RootInputDigest           string      `json:"root_input_digest"`
	StageProfileRevisionIDs   []uuid.UUID `json:"stage_profile_revision_ids"`
	StageInterfaceRevisionIDs []uuid.UUID `json:"stage_interface_revision_ids"`
	ConnectorRevisionIDs      []uuid.UUID `json:"connector_revision_ids"`
	StageRunIDs               []uuid.UUID `json:"stage_run_ids"`
	NodeIdentities            []string    `json:"node_identities"`
	TransferTicketIDs         []uuid.UUID `json:"transfer_ticket_ids"`
	FinalOutputDigest         string      `json:"final_output_digest"`
}

type CacheRunEvidence struct {
	JobID        uuid.UUID          `json:"job_id"`
	AttemptID    uuid.UUID          `json:"attempt_id"`
	AttemptFence int64              `json:"attempt_fence"`
	Hits         []CacheHitEvidence `json:"hits"`
}

type CacheHitEvidence struct {
	StageKey                    string    `json:"stage_key"`
	EntryID                     uuid.UUID `json:"entry_id"`
	SourceJobID                 uuid.UUID `json:"source_job_id"`
	SourceStageRunID            uuid.UUID `json:"source_stage_run_id"`
	TargetStageRunID            uuid.UUID `json:"target_stage_run_id"`
	StageArtifactID             uuid.UUID `json:"stage_artifact_id"`
	ExactObjectVersion          string    `json:"exact_object_version"`
	ArtifactDigest              string    `json:"artifact_digest"`
	CachePolicyRevisionID       uuid.UUID `json:"cache_policy_revision_id"`
	ResultEquivalenceRevisionID uuid.UUID `json:"result_equivalence_revision_id"`
	ReferenceID                 uuid.UUID `json:"reference_id"`
	ExecutionPinID              uuid.UUID `json:"execution_pin_id"`
}
