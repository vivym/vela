package h3faultevidence

import (
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/productiongates"
)

const (
	SchemaVersion    = 1
	MaxManifestBytes = 1024 * 1024
	MaxReceiptBytes  = 4 * 1024 * 1024
)

type Scenario string

const (
	ScenarioProcessKill                     Scenario = "process-kill"
	ScenarioWorkerControlNetworkPartition   Scenario = "worker-control-network-partition"
	ScenarioNodeReboot                      Scenario = "node-reboot"
	ScenarioOutboxPostCommitCrash           Scenario = "outbox-post-commit-crash"
	ScenarioPublisherPrePubAckCrash         Scenario = "publisher-pre-puback-crash"
	ScenarioPublisherPostPubAckPreMarkCrash Scenario = "publisher-post-puback-pre-mark-crash"
	ScenarioConsumerPostDBPreAckCrash       Scenario = "consumer-post-db-pre-ack-crash"
	ScenarioAssignmentPostCommitPreResponse Scenario = "assignment-post-commit-pre-response-crash"
	ScenarioRetryBudgetExhaustion           Scenario = "retry-budget-exhaustion"
	ScenarioStaleFenceLateCompletion        Scenario = "stale-fence-late-completion"
)

func AllScenarios() []Scenario {
	return []Scenario{
		ScenarioProcessKill,
		ScenarioWorkerControlNetworkPartition,
		ScenarioNodeReboot,
		ScenarioOutboxPostCommitCrash,
		ScenarioPublisherPrePubAckCrash,
		ScenarioPublisherPostPubAckPreMarkCrash,
		ScenarioConsumerPostDBPreAckCrash,
		ScenarioAssignmentPostCommitPreResponse,
		ScenarioRetryBudgetExhaustion,
		ScenarioStaleFenceLateCompletion,
	}
}

type StaleAuthorityKind string

const (
	StaleMemberEpoch       StaleAuthorityKind = "MEMBER_EPOCH"
	StaleDeviceEpoch       StaleAuthorityKind = "DEVICE_EPOCH"
	StaleModelRuntimeEpoch StaleAuthorityKind = "MODEL_RUNTIME_EPOCH"
	StaleStageLease        StaleAuthorityKind = "STAGE_LEASE"
)

func AllStaleAuthorityKinds() []StaleAuthorityKind {
	return []StaleAuthorityKind{
		StaleMemberEpoch,
		StaleDeviceEpoch,
		StaleModelRuntimeEpoch,
		StaleStageLease,
	}
}

type Manifest struct {
	SchemaVersion         int                 `json:"schema_version"`
	ReleaseDigest         string              `json:"release_digest"`
	ConfigurationRevision string              `json:"configuration_revision"`
	ValidationEnvironment string              `json:"validation_environment"`
	Owner                 string              `json:"owner"`
	StartedAt             time.Time           `json:"started_at"`
	CompletedAt           time.Time           `json:"completed_at"`
	Scenarios             []ScenarioReference `json:"scenarios"`
}

type ScenarioReference struct {
	Scenario Scenario `json:"scenario"`
	Ref      string   `json:"ref"`
	Digest   string   `json:"digest"`
}

type ScenarioReceipt struct {
	SchemaVersion         int                   `json:"schema_version"`
	Scenario              Scenario              `json:"scenario"`
	ReleaseDigest         string                `json:"release_digest"`
	ConfigurationRevision string                `json:"configuration_revision"`
	ValidationEnvironment string                `json:"validation_environment"`
	Owner                 string                `json:"owner"`
	StartedAt             time.Time             `json:"started_at"`
	CompletedAt           time.Time             `json:"completed_at"`
	ExerciseID            uuid.UUID             `json:"exercise_id"`
	ControllerIdentity    string                `json:"controller_identity"`
	Target                FaultTarget           `json:"target"`
	AcceptedJobIDs        []uuid.UUID           `json:"accepted_job_ids"`
	AuthorityBefore       AuthorityObservation  `json:"authority_before"`
	AuthorityAfter        AuthorityObservation  `json:"authority_after"`
	RawEvents             []RawEventObservation `json:"raw_events"`
	Measurements          Measurements          `json:"measurements"`
	StaleAuthorityProbes  []StaleAuthorityProbe `json:"stale_authority_probes"`
}

type FaultTarget struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

type AuthorityObservation struct {
	CapturedAt             time.Time `json:"captured_at"`
	DatabaseSnapshotID     string    `json:"database_snapshot_id"`
	JobLedgerDigest        string    `json:"job_ledger_digest"`
	CompletionLedgerDigest string    `json:"completion_ledger_digest"`
	ChargeLedgerDigest     string    `json:"charge_ledger_digest"`
	AcceptedJobCount       int64     `json:"accepted_job_count"`
	VisibleCompletionCount int64     `json:"visible_completion_count"`
	ChargeCount            int64     `json:"charge_count"`
}

type RawEventObservation struct {
	EventID          uuid.UUID `json:"event_id"`
	AggregateType    string    `json:"aggregate_type"`
	AggregateID      uuid.UUID `json:"aggregate_id"`
	AggregateVersion int64     `json:"aggregate_version"`
	EventType        string    `json:"event_type"`
	PayloadDigest    string    `json:"payload_digest"`
	PublishedCount   int64     `json:"published_count"`
	ConsumedCount    int64     `json:"consumed_count"`
}

type Measurements struct {
	LostAcceptedJobCount            int64 `json:"lost_accepted_job_count"`
	DuplicateVisibleCompletionCount int64 `json:"duplicate_visible_completion_count"`
	DuplicateChargeCount            int64 `json:"duplicate_charge_count"`
	StaleAuthorityAcceptanceCount   int64 `json:"stale_authority_acceptance_count"`
}

type StaleAuthorityProbe struct {
	ID                       uuid.UUID          `json:"id"`
	Kind                     StaleAuthorityKind `json:"kind"`
	JobID                    uuid.UUID          `json:"job_id"`
	StageRunID               uuid.UUID          `json:"stage_run_id"`
	WorkerInstanceID         uuid.UUID          `json:"worker_instance_id"`
	PresentedAuthorityDigest string             `json:"presented_authority_digest"`
	CurrentAuthorityDigest   string             `json:"current_authority_digest"`
	Decision                 string             `json:"decision"`
	ReasonCode               string             `json:"reason_code"`
	RejectedAt               time.Time          `json:"rejected_at"`
}

type Campaign struct {
	Manifest Manifest
	Receipts map[Scenario]ScenarioReceipt
}

type Bundle struct {
	Evidence      productiongates.TypedEvidence
	EvidenceBytes []byte
	Artifacts     map[string]productiongates.TypedEvidenceArtifact
	ArtifactBytes map[string][]byte
}
