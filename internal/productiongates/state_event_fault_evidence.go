package productiongates

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/strictjson"
)

const (
	FaultArtifactScenarioMatrix       = "scenario-matrix"
	FaultArtifactAuthorityBeforeAfter = "authority-before-after"
	FaultArtifactRawEventPayloads     = "raw-event-payloads"
)

type StateEventFaultScenarioContract struct {
	TargetKinds      []string
	Action           string
	InjectionPoint   string
	TriggerEventType string
}

func StateEventFaultScenarioContractForID(id string) (StateEventFaultScenarioContract, bool) {
	contracts := map[string]StateEventFaultScenarioContract{
		"process-kill": {
			TargetKinds: []string{"CONTROL_PLANE_PROCESS", "WORKER_AGENT_PROCESS", "MODEL_RUNTIME_PROCESS"},
			Action:      "KILL_PROCESS", InjectionPoint: "PROCESS_RUNNING",
			TriggerEventType: "fault.process.killed",
		},
		"worker-control-network-partition": {
			TargetKinds: []string{"WORKER_CONTROL_LINK"}, Action: "PARTITION_NETWORK",
			InjectionPoint: "WORKER_CONTROL_CONNECTED", TriggerEventType: "fault.worker-control.partitioned",
		},
		"node-reboot": {
			TargetKinds: []string{"COMPUTE_NODE"}, Action: "REBOOT_NODE",
			InjectionPoint: "NODE_READY", TriggerEventType: "fault.node.reboot-started",
		},
		"outbox-post-commit-crash": {
			TargetKinds: []string{"OUTBOX_PUBLISHER_PROCESS"}, Action: "CRASH_PROCESS",
			InjectionPoint: "OUTBOX_COMMIT_PRE_PUBLISH", TriggerEventType: "fault.outbox.post-commit-crash",
		},
		"publisher-pre-puback-crash": {
			TargetKinds: []string{"OUTBOX_PUBLISHER_PROCESS"}, Action: "CRASH_PROCESS",
			InjectionPoint: "PUBLISH_PRE_PUBACK", TriggerEventType: "fault.publisher.pre-puback-crash",
		},
		"publisher-post-puback-pre-mark-crash": {
			TargetKinds: []string{"OUTBOX_PUBLISHER_PROCESS"}, Action: "CRASH_PROCESS",
			InjectionPoint: "PUBACK_PRE_PUBLISHED_MARK", TriggerEventType: "fault.publisher.post-puback-pre-mark-crash",
		},
		"consumer-post-db-pre-ack-crash": {
			TargetKinds: []string{"EVENT_CONSUMER_PROCESS"}, Action: "CRASH_PROCESS",
			InjectionPoint: "CONSUMER_DB_COMMIT_PRE_ACK", TriggerEventType: "fault.consumer.post-db-pre-ack-crash",
		},
		"stage-assignment-post-commit-pre-response-crash": {
			TargetKinds: []string{"STAGE_ASSIGNMENT_SERVICE"}, Action: "CRASH_PROCESS",
			InjectionPoint: "STAGE_ASSIGNMENT_COMMIT_PRE_RESPONSE", TriggerEventType: "fault.stage-assignment.post-commit-pre-response-crash",
		},
		"retry-budget-exhaustion": {
			TargetKinds: []string{"STAGE_RUN"}, Action: "EXHAUST_RETRY_BUDGET",
			InjectionPoint: "STAGE_RETRY_ELIGIBLE", TriggerEventType: "fault.stage.retry-budget-exhausted",
		},
		"stale-fence-late-completion": {
			TargetKinds: []string{"STAGE_AUTHORITY"}, Action: "SUBMIT_STALE_COMPLETION",
			InjectionPoint: "STAGE_AUTHORITY_FENCED", TriggerEventType: "fault.stage.stale-completion-submitted",
		},
	}
	contract, ok := contracts[id]
	return contract, ok
}

type StateEventFaultArtifact struct {
	SourceManifestDigest string                       `json:"source_manifest_digest"`
	Scenarios            []StateEventFaultScenario    `json:"scenarios,omitempty"`
	Authorities          []StateEventFaultAuthority   `json:"authorities,omitempty"`
	RawEventSets         []StateEventFaultRawEventSet `json:"raw_event_sets,omitempty"`
}

type StateEventFaultReceiptBinding struct {
	Scenario       string      `json:"scenario"`
	ReceiptRef     string      `json:"receipt_ref"`
	ReceiptDigest  string      `json:"receipt_digest"`
	ExerciseID     uuid.UUID   `json:"exercise_id"`
	StartedAt      time.Time   `json:"started_at"`
	CompletedAt    time.Time   `json:"completed_at"`
	AcceptedJobIDs []uuid.UUID `json:"accepted_job_ids"`
}

type StateEventFaultScenario struct {
	Source              StateEventFaultReceiptBinding       `json:"source"`
	ControllerIdentity  string                              `json:"controller_identity"`
	Target              StateEventFaultTarget               `json:"target"`
	FaultWindow         StateEventFaultWindow               `json:"fault_window"`
	MaintenanceApproval *StateEventFaultMaintenanceApproval `json:"maintenance_approval,omitempty"`
}

type StateEventFaultTarget struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

type StateEventFaultWindow struct {
	Action              string    `json:"action"`
	InjectionPoint      string    `json:"injection_point"`
	OpenedAt            time.Time `json:"opened_at"`
	TriggeredAt         time.Time `json:"triggered_at"`
	RecoveryConfirmedAt time.Time `json:"recovery_confirmed_at"`
	TriggerEventID      uuid.UUID `json:"trigger_event_id"`
}

type StateEventFaultMaintenanceApproval struct {
	Ref        string    `json:"ref"`
	Digest     string    `json:"digest"`
	ApprovedBy string    `json:"approved_by"`
	ApprovedAt time.Time `json:"approved_at"`
	Reason     string    `json:"reason"`
	TargetID   string    `json:"target_id"`
}

type StateEventFaultAuthority struct {
	Source       StateEventFaultReceiptBinding   `json:"source"`
	Before       StateEventAuthorityObservation  `json:"before"`
	After        StateEventAuthorityObservation  `json:"after"`
	Measurements StateEventFaultMeasurements     `json:"measurements"`
	StaleProbes  []StateEventStaleAuthorityProbe `json:"stale_authority_probes,omitempty"`
}

type StateEventAuthorityObservation struct {
	CapturedAt             time.Time `json:"captured_at"`
	DatabaseSnapshotID     string    `json:"database_snapshot_id"`
	JobLedgerDigest        string    `json:"job_ledger_digest"`
	CompletionLedgerDigest string    `json:"completion_ledger_digest"`
	ChargeLedgerDigest     string    `json:"charge_ledger_digest"`
	AcceptedJobCount       int64     `json:"accepted_job_count"`
	VisibleCompletionCount int64     `json:"visible_completion_count"`
	ChargeCount            int64     `json:"charge_count"`
}

type StateEventFaultMeasurements struct {
	LostAcceptedJobCount            int64 `json:"lost_accepted_job_count"`
	DuplicateVisibleCompletionCount int64 `json:"duplicate_visible_completion_count"`
	DuplicateChargeCount            int64 `json:"duplicate_charge_count"`
	StaleAuthorityAcceptanceCount   int64 `json:"stale_authority_acceptance_count"`
}

type StateEventStaleAuthorityProbe struct {
	ID                       uuid.UUID `json:"id"`
	Kind                     string    `json:"kind"`
	JobID                    uuid.UUID `json:"job_id"`
	StageRunID               uuid.UUID `json:"stage_run_id"`
	WorkerInstanceID         uuid.UUID `json:"worker_instance_id"`
	PresentedAuthorityDigest string    `json:"presented_authority_digest"`
	CurrentAuthorityDigest   string    `json:"current_authority_digest"`
	Decision                 string    `json:"decision"`
	ReasonCode               string    `json:"reason_code"`
	RejectedAt               time.Time `json:"rejected_at"`
}

type StateEventFaultRawEventSet struct {
	Source StateEventFaultReceiptBinding `json:"source"`
	Events []StateEventFaultRawEvent     `json:"events"`
}

type StateEventFaultRawEvent struct {
	EventID          uuid.UUID       `json:"event_id"`
	AggregateType    string          `json:"aggregate_type"`
	AggregateID      uuid.UUID       `json:"aggregate_id"`
	AggregateVersion int64           `json:"aggregate_version"`
	EventType        string          `json:"event_type"`
	PayloadDigest    string          `json:"payload_digest"`
	Payload          json.RawMessage `json:"payload"`
	PublishedCount   int64           `json:"published_count"`
	ConsumedCount    int64           `json:"consumed_count"`
}

func (artifact StateEventFaultArtifact) validate(kind string, startedAt, completedAt time.Time) error {
	if !validSHA256Digest(artifact.SourceManifestDigest) {
		return fmt.Errorf("source manifest digest is invalid")
	}
	switch kind {
	case FaultArtifactScenarioMatrix:
		if len(artifact.Scenarios) != 10 || len(artifact.Authorities) != 0 || len(artifact.RawEventSets) != 0 {
			return fmt.Errorf("scenario matrix payload is incomplete or mixed")
		}
		for _, scenario := range artifact.Scenarios {
			if err := scenario.validate(); err != nil {
				return err
			}
			if err := scenario.Source.validateWithin(startedAt, completedAt); err != nil {
				return err
			}
		}
	case FaultArtifactAuthorityBeforeAfter:
		if len(artifact.Authorities) != 10 || len(artifact.Scenarios) != 0 || len(artifact.RawEventSets) != 0 {
			return fmt.Errorf("authority payload is incomplete or mixed")
		}
		for _, authority := range artifact.Authorities {
			if err := authority.validate(); err != nil {
				return err
			}
			if err := authority.Source.validateWithin(startedAt, completedAt); err != nil {
				return err
			}
		}
	case FaultArtifactRawEventPayloads:
		if len(artifact.RawEventSets) != 10 || len(artifact.Scenarios) != 0 || len(artifact.Authorities) != 0 {
			return fmt.Errorf("raw event payload is incomplete or mixed")
		}
		for _, eventSet := range artifact.RawEventSets {
			if err := eventSet.validate(); err != nil {
				return err
			}
			if err := eventSet.Source.validateWithin(startedAt, completedAt); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unknown state/event fault artifact kind %q", kind)
	}
	return nil
}

func (binding StateEventFaultReceiptBinding) validateWithin(startedAt, completedAt time.Time) error {
	if binding.StartedAt.Before(startedAt) || binding.CompletedAt.After(completedAt) {
		return fmt.Errorf("scenario %s source time is outside the evidence window", binding.Scenario)
	}
	return nil
}

func (binding StateEventFaultReceiptBinding) validate() error {
	if _, ok := StateEventFaultScenarioContractForID(binding.Scenario); !ok ||
		!filepath.IsLocal(filepath.FromSlash(binding.ReceiptRef)) || !validSHA256Digest(binding.ReceiptDigest) ||
		binding.ExerciseID == uuid.Nil || binding.StartedAt.IsZero() ||
		!binding.StartedAt.Equal(binding.StartedAt.UTC()) || !binding.CompletedAt.Equal(binding.CompletedAt.UTC()) ||
		!binding.CompletedAt.After(binding.StartedAt) || len(binding.AcceptedJobIDs) == 0 ||
		len(binding.AcceptedJobIDs) > 10000 {
		return fmt.Errorf("fault receipt source binding is invalid")
	}
	seen := make(map[uuid.UUID]struct{}, len(binding.AcceptedJobIDs))
	for _, jobID := range binding.AcceptedJobIDs {
		if jobID == uuid.Nil {
			return fmt.Errorf("fault receipt source contains a nil Job id")
		}
		if _, duplicate := seen[jobID]; duplicate {
			return fmt.Errorf("fault receipt source contains a duplicate Job id")
		}
		seen[jobID] = struct{}{}
	}
	return nil
}

func (scenario StateEventFaultScenario) validate() error {
	if err := scenario.Source.validate(); err != nil {
		return err
	}
	contract, _ := StateEventFaultScenarioContractForID(scenario.Source.Scenario)
	if !validBoundedText(scenario.ControllerIdentity, 500) ||
		!validBoundedText(scenario.Target.ID, 500) || !containsString(contract.TargetKinds, scenario.Target.Kind) ||
		scenario.FaultWindow.Action != contract.Action ||
		scenario.FaultWindow.InjectionPoint != contract.InjectionPoint ||
		scenario.FaultWindow.TriggerEventID == uuid.Nil ||
		scenario.FaultWindow.OpenedAt.Before(scenario.Source.StartedAt) ||
		!scenario.FaultWindow.TriggeredAt.After(scenario.FaultWindow.OpenedAt) ||
		!scenario.FaultWindow.RecoveryConfirmedAt.After(scenario.FaultWindow.TriggeredAt) ||
		scenario.FaultWindow.RecoveryConfirmedAt.After(scenario.Source.CompletedAt) ||
		!scenario.FaultWindow.OpenedAt.Equal(scenario.FaultWindow.OpenedAt.UTC()) ||
		!scenario.FaultWindow.TriggeredAt.Equal(scenario.FaultWindow.TriggeredAt.UTC()) ||
		!scenario.FaultWindow.RecoveryConfirmedAt.Equal(scenario.FaultWindow.RecoveryConfirmedAt.UTC()) {
		return fmt.Errorf("scenario %s target or fault window is invalid", scenario.Source.Scenario)
	}
	if scenario.Target.Kind == "MODEL_RUNTIME_PROCESS" {
		if scenario.MaintenanceApproval == nil ||
			!validBoundedText(scenario.MaintenanceApproval.Ref, 2000) ||
			!filepath.IsLocal(filepath.FromSlash(scenario.MaintenanceApproval.Ref)) ||
			!validSHA256Digest(scenario.MaintenanceApproval.Digest) ||
			!validBoundedText(scenario.MaintenanceApproval.ApprovedBy, 500) ||
			!validBoundedText(scenario.MaintenanceApproval.Reason, 1000) ||
			scenario.MaintenanceApproval.TargetID != scenario.Target.ID ||
			scenario.MaintenanceApproval.ApprovedAt.After(scenario.FaultWindow.TriggeredAt) ||
			scenario.MaintenanceApproval.ApprovedAt.Before(scenario.Source.StartedAt) ||
			!scenario.MaintenanceApproval.ApprovedAt.Equal(scenario.MaintenanceApproval.ApprovedAt.UTC()) {
			return fmt.Errorf("ModelRuntime process exercise lacks exact maintenance approval")
		}
	} else if scenario.MaintenanceApproval != nil {
		return fmt.Errorf("scenario %s contains unexpected maintenance approval", scenario.Source.Scenario)
	}
	return nil
}

func (authority StateEventFaultAuthority) validate() error {
	if err := authority.Source.validate(); err != nil {
		return err
	}
	jobCount := int64(len(authority.Source.AcceptedJobIDs))
	if err := validateStateEventAuthorityObservation(authority.Source, authority.Before); err != nil {
		return err
	}
	if err := validateStateEventAuthorityObservation(authority.Source, authority.After); err != nil {
		return err
	}
	if authority.Before.CapturedAt.After(authority.After.CapturedAt) ||
		authority.Before.AcceptedJobCount != jobCount || authority.After.AcceptedJobCount != jobCount ||
		authority.Before.VisibleCompletionCount > jobCount || authority.Before.ChargeCount > jobCount ||
		authority.After.VisibleCompletionCount != jobCount || authority.After.ChargeCount != jobCount ||
		authority.After.VisibleCompletionCount < authority.Before.VisibleCompletionCount ||
		authority.After.ChargeCount < authority.Before.ChargeCount ||
		authority.Measurements != (StateEventFaultMeasurements{}) {
		return fmt.Errorf("scenario %s authority ledger does not reconcile", authority.Source.Scenario)
	}
	if authority.Before.VisibleCompletionCount != authority.After.VisibleCompletionCount &&
		authority.Before.CompletionLedgerDigest == authority.After.CompletionLedgerDigest ||
		authority.Before.ChargeCount != authority.After.ChargeCount &&
			authority.Before.ChargeLedgerDigest == authority.After.ChargeLedgerDigest {
		return fmt.Errorf("scenario %s authority count changed without a ledger transition", authority.Source.Scenario)
	}
	if authority.Source.Scenario == "stale-fence-late-completion" {
		if err := validateStateEventStaleProbes(authority.Source, authority.StaleProbes); err != nil {
			return err
		}
	} else if len(authority.StaleProbes) != 0 {
		return fmt.Errorf("scenario %s contains unexpected stale probes", authority.Source.Scenario)
	}
	return nil
}

func validateStateEventAuthorityObservation(
	binding StateEventFaultReceiptBinding,
	observation StateEventAuthorityObservation,
) error {
	if observation.CapturedAt.Before(binding.StartedAt) || observation.CapturedAt.After(binding.CompletedAt) ||
		!observation.CapturedAt.Equal(observation.CapturedAt.UTC()) ||
		!validBoundedText(observation.DatabaseSnapshotID, 500) ||
		!validSHA256Digest(observation.JobLedgerDigest) ||
		!validSHA256Digest(observation.CompletionLedgerDigest) ||
		!validSHA256Digest(observation.ChargeLedgerDigest) || observation.AcceptedJobCount < 0 ||
		observation.VisibleCompletionCount < 0 || observation.ChargeCount < 0 {
		return fmt.Errorf("scenario %s authority observation is invalid", binding.Scenario)
	}
	return nil
}

func validateStateEventStaleProbes(
	binding StateEventFaultReceiptBinding,
	probes []StateEventStaleAuthorityProbe,
) error {
	wanted := map[string]string{
		"MEMBER_EPOCH": "member-epoch-stale", "DEVICE_EPOCH": "device-epoch-stale",
		"MODEL_RUNTIME_EPOCH": "model-runtime-epoch-stale", "STAGE_LEASE": "stage-lease-stale",
	}
	if len(probes) != len(wanted) {
		return fmt.Errorf("stale-fence scenario does not contain all authority probes")
	}
	jobs := make(map[uuid.UUID]struct{}, len(binding.AcceptedJobIDs))
	for _, jobID := range binding.AcceptedJobIDs {
		jobs[jobID] = struct{}{}
	}
	seen := make(map[string]struct{}, len(probes))
	seenIDs := make(map[uuid.UUID]struct{}, len(probes))
	for _, probe := range probes {
		reason, known := wanted[probe.Kind]
		_, knownJob := jobs[probe.JobID]
		if !known || !knownJob || probe.ID == uuid.Nil || probe.StageRunID == uuid.Nil ||
			probe.WorkerInstanceID == uuid.Nil || !validSHA256Digest(probe.PresentedAuthorityDigest) ||
			!validSHA256Digest(probe.CurrentAuthorityDigest) ||
			probe.PresentedAuthorityDigest == probe.CurrentAuthorityDigest || probe.Decision != "REJECTED" ||
			probe.ReasonCode != reason || probe.RejectedAt.Before(binding.StartedAt) ||
			probe.RejectedAt.After(binding.CompletedAt) || !probe.RejectedAt.Equal(probe.RejectedAt.UTC()) {
			return fmt.Errorf("stale-authority probe is invalid")
		}
		if _, duplicate := seen[probe.Kind]; duplicate {
			return fmt.Errorf("duplicate stale-authority probe kind %s", probe.Kind)
		}
		if _, duplicate := seenIDs[probe.ID]; duplicate {
			return fmt.Errorf("duplicate stale-authority probe id")
		}
		seen[probe.Kind] = struct{}{}
		seenIDs[probe.ID] = struct{}{}
	}
	return nil
}

func (eventSet StateEventFaultRawEventSet) validate() error {
	if err := eventSet.Source.validate(); err != nil {
		return err
	}
	if len(eventSet.Events) == 0 || len(eventSet.Events) > 100000 {
		return fmt.Errorf("scenario %s raw event set is invalid", eventSet.Source.Scenario)
	}
	seen := make(map[uuid.UUID]struct{}, len(eventSet.Events))
	coveredJobs := make(map[uuid.UUID]struct{}, len(eventSet.Source.AcceptedJobIDs))
	jobs := make(map[uuid.UUID]struct{}, len(eventSet.Source.AcceptedJobIDs))
	for _, jobID := range eventSet.Source.AcceptedJobIDs {
		jobs[jobID] = struct{}{}
	}
	for _, event := range eventSet.Events {
		if event.EventID == uuid.Nil || event.AggregateID == uuid.Nil || event.AggregateVersion <= 0 ||
			!validBoundedText(event.AggregateType, 100) || !validBoundedText(event.EventType, 200) ||
			!validStateEventRawPayload(event.Payload, event.PayloadDigest) ||
			event.PublishedCount <= 0 || event.ConsumedCount <= 0 {
			return fmt.Errorf("scenario %s raw event is invalid", eventSet.Source.Scenario)
		}
		if _, duplicate := seen[event.EventID]; duplicate {
			return fmt.Errorf("scenario %s contains duplicate raw event identity", eventSet.Source.Scenario)
		}
		seen[event.EventID] = struct{}{}
		if event.AggregateType == "Job" {
			if _, accepted := jobs[event.AggregateID]; !accepted {
				return fmt.Errorf("scenario %s contains an unaccepted Job event", eventSet.Source.Scenario)
			}
			coveredJobs[event.AggregateID] = struct{}{}
		}
	}
	if len(coveredJobs) != len(jobs) {
		return fmt.Errorf("scenario %s raw events do not cover every Accepted Job", eventSet.Source.Scenario)
	}
	return nil
}

func validStateEventRawPayload(encoded json.RawMessage, expectedDigest string) bool {
	if len(encoded) == 0 || len(encoded) > MaxEvidenceArtifactBytes ||
		!validSHA256Digest(expectedDigest) || strictjson.RejectDuplicateKeys(encoded) != nil {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	var value any
	if err := decoder.Decode(&value); err != nil || value == nil {
		return false
	}
	var trailing any
	if !errors.Is(decoder.Decode(&trailing), io.EOF) {
		return false
	}
	var canonical bytes.Buffer
	if json.Compact(&canonical, encoded) != nil {
		return false
	}
	digest := sha256.Sum256(canonical.Bytes())
	return "sha256:"+hex.EncodeToString(digest[:]) == expectedDigest
}

func validateStateEventFaultArtifactSet(artifacts map[string]TypedEvidenceArtifact) error {
	matrix := artifacts[FaultArtifactScenarioMatrix].StateEventFault
	authorities := artifacts[FaultArtifactAuthorityBeforeAfter].StateEventFault
	events := artifacts[FaultArtifactRawEventPayloads].StateEventFault
	if matrix == nil || authorities == nil || events == nil ||
		matrix.SourceManifestDigest != authorities.SourceManifestDigest ||
		matrix.SourceManifestDigest != events.SourceManifestDigest {
		return fmt.Errorf("%w: state/event fault source manifest binding is inconsistent", ErrInvalidTypedEvidence)
	}
	authorityByScenario := make(map[string]StateEventFaultAuthority, len(authorities.Authorities))
	eventsByScenario := make(map[string]StateEventFaultRawEventSet, len(events.RawEventSets))
	for _, authority := range authorities.Authorities {
		if _, duplicate := authorityByScenario[authority.Source.Scenario]; duplicate {
			return fmt.Errorf("%w: duplicate authority scenario %s", ErrInvalidTypedEvidence, authority.Source.Scenario)
		}
		authorityByScenario[authority.Source.Scenario] = authority
	}
	globalEvents := make(map[uuid.UUID]string)
	for _, eventSet := range events.RawEventSets {
		if _, duplicate := eventsByScenario[eventSet.Source.Scenario]; duplicate {
			return fmt.Errorf("%w: duplicate raw-event scenario %s", ErrInvalidTypedEvidence, eventSet.Source.Scenario)
		}
		eventsByScenario[eventSet.Source.Scenario] = eventSet
		for _, event := range eventSet.Events {
			if prior, duplicate := globalEvents[event.EventID]; duplicate {
				return fmt.Errorf("%w: raw event %s occurs in scenarios %s and %s", ErrInvalidTypedEvidence, event.EventID, prior, eventSet.Source.Scenario)
			}
			globalEvents[event.EventID] = eventSet.Source.Scenario
		}
	}
	seenScenarios := make(map[string]struct{}, len(matrix.Scenarios))
	seenReceiptRefs := make(map[string]struct{}, len(matrix.Scenarios))
	seenExerciseIDs := make(map[uuid.UUID]struct{}, len(matrix.Scenarios))
	for _, scenario := range matrix.Scenarios {
		if _, duplicate := seenScenarios[scenario.Source.Scenario]; duplicate {
			return fmt.Errorf("%w: duplicate fault scenario %s", ErrInvalidTypedEvidence, scenario.Source.Scenario)
		}
		seenScenarios[scenario.Source.Scenario] = struct{}{}
		if _, duplicate := seenReceiptRefs[scenario.Source.ReceiptRef]; duplicate {
			return fmt.Errorf("%w: duplicate source receipt ref %s", ErrInvalidTypedEvidence, scenario.Source.ReceiptRef)
		}
		if _, duplicate := seenExerciseIDs[scenario.Source.ExerciseID]; duplicate {
			return fmt.Errorf("%w: duplicate fault Exercise id %s", ErrInvalidTypedEvidence, scenario.Source.ExerciseID)
		}
		seenReceiptRefs[scenario.Source.ReceiptRef] = struct{}{}
		seenExerciseIDs[scenario.Source.ExerciseID] = struct{}{}
		authority, authorityPresent := authorityByScenario[scenario.Source.Scenario]
		eventSet, eventsPresent := eventsByScenario[scenario.Source.Scenario]
		if !authorityPresent || !eventsPresent ||
			!reflect.DeepEqual(scenario.Source, authority.Source) ||
			!reflect.DeepEqual(scenario.Source, eventSet.Source) {
			return fmt.Errorf("%w: scenario %s source binding differs across artifacts", ErrInvalidTypedEvidence, scenario.Source.Scenario)
		}
		if authority.Before.CapturedAt.After(scenario.FaultWindow.OpenedAt) ||
			authority.After.CapturedAt.Before(scenario.FaultWindow.RecoveryConfirmedAt) {
			return fmt.Errorf("%w: scenario %s authority snapshots do not bracket the fault window", ErrInvalidTypedEvidence, scenario.Source.Scenario)
		}
		contract, _ := StateEventFaultScenarioContractForID(scenario.Source.Scenario)
		triggerFound := false
		for _, event := range eventSet.Events {
			if event.EventID == scenario.FaultWindow.TriggerEventID && event.EventType == contract.TriggerEventType &&
				event.AggregateType == "FaultExercise" && event.AggregateID == scenario.Source.ExerciseID {
				triggerFound = true
			}
		}
		if !triggerFound {
			return fmt.Errorf("%w: scenario %s trigger event is missing", ErrInvalidTypedEvidence, scenario.Source.Scenario)
		}
	}
	if len(seenScenarios) != 10 || len(authorityByScenario) != 10 || len(eventsByScenario) != 10 {
		return fmt.Errorf("%w: state/event fault scenario coverage is incomplete", ErrInvalidTypedEvidence)
	}
	return nil
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
