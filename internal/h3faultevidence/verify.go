package h3faultevidence

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/productiongates"
)

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func validateManifest(manifest Manifest) error {
	if manifest.SchemaVersion != SchemaVersion ||
		!digestPattern.MatchString(manifest.ReleaseDigest) ||
		!validText(manifest.ConfigurationRevision, 500) ||
		!validText(manifest.ValidationEnvironment, 500) ||
		!validText(manifest.Owner, 500) || manifest.StartedAt.IsZero() ||
		!manifest.StartedAt.Equal(manifest.StartedAt.UTC()) ||
		!manifest.CompletedAt.Equal(manifest.CompletedAt.UTC()) ||
		!manifest.CompletedAt.After(manifest.StartedAt) {
		return invalid("manifest binding or time window is invalid")
	}
	wanted := make(map[Scenario]struct{}, len(AllScenarios()))
	for _, scenario := range AllScenarios() {
		wanted[scenario] = struct{}{}
	}
	if len(manifest.Scenarios) != len(wanted) {
		return invalid("manifest must contain exactly %d scenarios", len(wanted))
	}
	seen := make(map[Scenario]struct{}, len(wanted))
	seenRefs := make(map[string]struct{}, len(wanted))
	for _, reference := range manifest.Scenarios {
		if _, known := wanted[reference.Scenario]; !known ||
			!filepath.IsLocal(filepath.FromSlash(reference.Ref)) ||
			!digestPattern.MatchString(reference.Digest) {
			return invalid("scenario reference is invalid")
		}
		if _, duplicate := seen[reference.Scenario]; duplicate {
			return invalid("duplicate scenario %s", reference.Scenario)
		}
		if _, duplicate := seenRefs[reference.Ref]; duplicate {
			return invalid("duplicate scenario receipt reference %s", reference.Ref)
		}
		seen[reference.Scenario] = struct{}{}
		seenRefs[reference.Ref] = struct{}{}
	}
	return nil
}

func validateScenarioReceipt(manifest Manifest, expected Scenario, receipt ScenarioReceipt) error {
	if receipt.SchemaVersion != SchemaVersion || receipt.Scenario != expected ||
		receipt.ReleaseDigest != manifest.ReleaseDigest ||
		receipt.ConfigurationRevision != manifest.ConfigurationRevision ||
		receipt.ValidationEnvironment != manifest.ValidationEnvironment ||
		receipt.Owner != manifest.Owner || receipt.ExerciseID == uuid.Nil ||
		!validText(receipt.ControllerIdentity, 500) ||
		!validText(receipt.Target.Kind, 100) || !validText(receipt.Target.ID, 500) ||
		receipt.StartedAt.Before(manifest.StartedAt) ||
		receipt.CompletedAt.After(manifest.CompletedAt) ||
		!receipt.StartedAt.Equal(receipt.StartedAt.UTC()) ||
		!receipt.CompletedAt.Equal(receipt.CompletedAt.UTC()) ||
		!receipt.CompletedAt.After(receipt.StartedAt) {
		return invalid("scenario %s binding or time window is invalid", expected)
	}
	if len(receipt.AcceptedJobIDs) == 0 || len(receipt.AcceptedJobIDs) > 10000 {
		return invalid("scenario %s Accepted Job inventory is invalid", expected)
	}
	if err := validateFaultExercise(expected, receipt); err != nil {
		return invalid("scenario %s fault exercise: %v", expected, err)
	}
	jobs := make(map[uuid.UUID]struct{}, len(receipt.AcceptedJobIDs))
	for _, jobID := range receipt.AcceptedJobIDs {
		if jobID == uuid.Nil {
			return invalid("scenario %s contains a nil Job id", expected)
		}
		if _, duplicate := jobs[jobID]; duplicate {
			return invalid("scenario %s contains a duplicate Job id", expected)
		}
		jobs[jobID] = struct{}{}
	}
	if err := validateAuthorityObservation(receipt, receipt.AuthorityBefore, true); err != nil {
		return invalid("scenario %s authority before: %v", expected, err)
	}
	if err := validateAuthorityObservation(receipt, receipt.AuthorityAfter, false); err != nil {
		return invalid("scenario %s authority after: %v", expected, err)
	}
	jobCount := int64(len(receipt.AcceptedJobIDs))
	if receipt.AuthorityBefore.AcceptedJobCount != jobCount ||
		receipt.AuthorityAfter.AcceptedJobCount != jobCount ||
		receipt.AuthorityAfter.VisibleCompletionCount != jobCount ||
		receipt.AuthorityAfter.ChargeCount != jobCount {
		return invalid("scenario %s authority ledgers do not reconcile Accepted Jobs", expected)
	}
	if receipt.AuthorityBefore.VisibleCompletionCount > jobCount ||
		receipt.AuthorityBefore.ChargeCount > jobCount ||
		receipt.AuthorityAfter.VisibleCompletionCount < receipt.AuthorityBefore.VisibleCompletionCount ||
		receipt.AuthorityAfter.ChargeCount < receipt.AuthorityBefore.ChargeCount {
		return invalid("scenario %s authority ledgers are not monotonic", expected)
	}
	if receipt.AuthorityBefore.CapturedAt.After(receipt.FaultWindow.OpenedAt) ||
		receipt.AuthorityAfter.CapturedAt.Before(receipt.FaultWindow.RecoveryConfirmedAt) {
		return invalid("scenario %s authority snapshots do not bracket the fault window", expected)
	}
	if receipt.AuthorityBefore.VisibleCompletionCount != receipt.AuthorityAfter.VisibleCompletionCount &&
		receipt.AuthorityBefore.CompletionLedgerDigest == receipt.AuthorityAfter.CompletionLedgerDigest ||
		receipt.AuthorityBefore.ChargeCount != receipt.AuthorityAfter.ChargeCount &&
			receipt.AuthorityBefore.ChargeLedgerDigest == receipt.AuthorityAfter.ChargeLedgerDigest {
		return invalid("scenario %s changed authority counts without changing ledger digests", expected)
	}
	if receipt.Measurements != (Measurements{}) {
		return invalid("scenario %s contains a nonzero failure measurement", expected)
	}
	if len(receipt.RawEvents) == 0 || len(receipt.RawEvents) > 100000 {
		return invalid("scenario %s raw event inventory is invalid", expected)
	}
	events := make(map[uuid.UUID]struct{}, len(receipt.RawEvents))
	coveredJobs := make(map[uuid.UUID]struct{}, len(jobs))
	triggerEventFound := false
	contract, _ := productiongates.StateEventFaultScenarioContractForID(string(expected))
	for _, event := range receipt.RawEvents {
		if event.EventID == uuid.Nil || event.AggregateID == uuid.Nil ||
			event.AggregateVersion <= 0 || !validText(event.AggregateType, 100) ||
			!validText(event.EventType, 200) || !validRawEventPayload(event.Payload, event.PayloadDigest) ||
			event.PublishedCount <= 0 || event.ConsumedCount <= 0 {
			return invalid("scenario %s contains an invalid raw event observation", expected)
		}
		if _, duplicate := events[event.EventID]; duplicate {
			return invalid("scenario %s contains duplicate raw event identity", expected)
		}
		events[event.EventID] = struct{}{}
		if event.AggregateType == "Job" {
			if _, accepted := jobs[event.AggregateID]; !accepted {
				return invalid("scenario %s contains a Job event outside the Accepted Job inventory", expected)
			}
			coveredJobs[event.AggregateID] = struct{}{}
		}
		if event.EventID == receipt.FaultWindow.TriggerEventID && event.EventType == contract.TriggerEventType &&
			event.AggregateType == "FaultExercise" && event.AggregateID == receipt.ExerciseID {
			triggerEventFound = true
		}
	}
	if len(coveredJobs) != len(jobs) {
		return invalid("scenario %s raw events do not cover every Accepted Job", expected)
	}
	if !triggerEventFound {
		return invalid("scenario %s raw events do not contain the exact fault trigger", expected)
	}
	if expected == ScenarioStaleFenceLateCompletion {
		if err := validateStaleAuthorityProbes(receipt, jobs); err != nil {
			return invalid("scenario %s: %v", expected, err)
		}
	} else if len(receipt.StaleAuthorityProbes) != 0 {
		return invalid("scenario %s contains unexpected stale-authority probes", expected)
	}
	return nil
}

func validRawEventPayload(encoded json.RawMessage, expectedDigest string) bool {
	if len(encoded) == 0 || len(encoded) > MaxReceiptBytes || !digestPattern.MatchString(expectedDigest) {
		return false
	}
	var payload any
	if decodeStrict(encoded, &payload) != nil || payload == nil {
		return false
	}
	var canonical bytes.Buffer
	if json.Compact(&canonical, encoded) != nil {
		return false
	}
	digest := sha256.Sum256(canonical.Bytes())
	return "sha256:"+hex.EncodeToString(digest[:]) == expectedDigest
}

func validateFaultExercise(expected Scenario, receipt ScenarioReceipt) error {
	contract, known := productiongates.StateEventFaultScenarioContractForID(string(expected))
	if !known || !contains(contract.TargetKinds, receipt.Target.Kind) ||
		receipt.FaultWindow.Action != contract.Action ||
		receipt.FaultWindow.InjectionPoint != contract.InjectionPoint ||
		receipt.FaultWindow.TriggerEventID == uuid.Nil ||
		receipt.FaultWindow.OpenedAt.Before(receipt.StartedAt) ||
		!receipt.FaultWindow.TriggeredAt.After(receipt.FaultWindow.OpenedAt) ||
		!receipt.FaultWindow.RecoveryConfirmedAt.After(receipt.FaultWindow.TriggeredAt) ||
		receipt.FaultWindow.RecoveryConfirmedAt.After(receipt.CompletedAt) ||
		!receipt.FaultWindow.OpenedAt.Equal(receipt.FaultWindow.OpenedAt.UTC()) ||
		!receipt.FaultWindow.TriggeredAt.Equal(receipt.FaultWindow.TriggeredAt.UTC()) ||
		!receipt.FaultWindow.RecoveryConfirmedAt.Equal(receipt.FaultWindow.RecoveryConfirmedAt.UTC()) {
		return fmt.Errorf("target, action, injection point, or time window is invalid")
	}
	if receipt.Target.Kind == "MODEL_RUNTIME_PROCESS" {
		approval := receipt.MaintenanceApproval
		if approval == nil || !filepath.IsLocal(filepath.FromSlash(approval.Ref)) ||
			!digestPattern.MatchString(approval.Digest) || !validText(approval.ApprovedBy, 500) ||
			!validText(approval.Reason, 1000) || approval.TargetID != receipt.Target.ID ||
			approval.ApprovedAt.Before(receipt.StartedAt) ||
			approval.ApprovedAt.After(receipt.FaultWindow.TriggeredAt) ||
			!approval.ApprovedAt.Equal(approval.ApprovedAt.UTC()) {
			return fmt.Errorf("ModelRuntime process target lacks exact maintenance approval")
		}
	} else if receipt.MaintenanceApproval != nil {
		return fmt.Errorf("maintenance approval is allowed only for a ModelRuntime process target")
	}
	return nil
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func validateAuthorityObservation(
	receipt ScenarioReceipt,
	observation AuthorityObservation,
	before bool,
) error {
	if observation.CapturedAt.IsZero() ||
		!observation.CapturedAt.Equal(observation.CapturedAt.UTC()) ||
		!validText(observation.DatabaseSnapshotID, 500) ||
		!digestPattern.MatchString(observation.JobLedgerDigest) ||
		!digestPattern.MatchString(observation.CompletionLedgerDigest) ||
		!digestPattern.MatchString(observation.ChargeLedgerDigest) ||
		observation.AcceptedJobCount < 0 || observation.VisibleCompletionCount < 0 ||
		observation.ChargeCount < 0 {
		return fmt.Errorf("snapshot provenance or ledger is invalid")
	}
	if before {
		if observation.CapturedAt.Before(receipt.StartedAt) ||
			observation.CapturedAt.After(receipt.CompletedAt) {
			return fmt.Errorf("snapshot time is outside the scenario")
		}
	} else if observation.CapturedAt.Before(receipt.AuthorityBefore.CapturedAt) ||
		observation.CapturedAt.After(receipt.CompletedAt) {
		return fmt.Errorf("snapshot time is outside the recovery window")
	}
	return nil
}

func validateStaleAuthorityProbes(
	receipt ScenarioReceipt,
	jobs map[uuid.UUID]struct{},
) error {
	wanted := map[StaleAuthorityKind]string{
		StaleMemberEpoch:       "member-epoch-stale",
		StaleDeviceEpoch:       "device-epoch-stale",
		StaleModelRuntimeEpoch: "model-runtime-epoch-stale",
		StaleStageLease:        "stage-lease-stale",
	}
	if len(receipt.StaleAuthorityProbes) != len(wanted) {
		return fmt.Errorf("all four stale-authority probe kinds are required")
	}
	seen := make(map[StaleAuthorityKind]struct{}, len(wanted))
	seenIDs := make(map[uuid.UUID]struct{}, len(wanted))
	for _, probe := range receipt.StaleAuthorityProbes {
		reason, known := wanted[probe.Kind]
		_, knownJob := jobs[probe.JobID]
		if !known || !knownJob || probe.ID == uuid.Nil || probe.StageRunID == uuid.Nil ||
			probe.WorkerInstanceID == uuid.Nil ||
			!digestPattern.MatchString(probe.PresentedAuthorityDigest) ||
			!digestPattern.MatchString(probe.CurrentAuthorityDigest) ||
			probe.PresentedAuthorityDigest == probe.CurrentAuthorityDigest ||
			probe.Decision != "REJECTED" || probe.ReasonCode != reason ||
			probe.RejectedAt.Before(receipt.StartedAt) ||
			probe.RejectedAt.After(receipt.CompletedAt) ||
			!probe.RejectedAt.Equal(probe.RejectedAt.UTC()) {
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

func (campaign Campaign) BuildBundle() (Bundle, error) {
	if err := validateManifest(campaign.Manifest); err != nil {
		return Bundle{}, err
	}
	if !digestPattern.MatchString(campaign.ManifestDigest) {
		return Bundle{}, invalid("source manifest digest is invalid")
	}
	if len(campaign.Receipts) != len(AllScenarios()) {
		return Bundle{}, invalid("loaded scenario receipt set is incomplete")
	}
	globalEvents := make(map[uuid.UUID]Scenario)
	for _, scenario := range AllScenarios() {
		receipt, present := campaign.Receipts[scenario]
		if !present {
			return Bundle{}, invalid("loaded scenario %s receipt is missing", scenario)
		}
		if err := validateScenarioReceipt(campaign.Manifest, scenario, receipt); err != nil {
			return Bundle{}, err
		}
		for _, event := range receipt.RawEvents {
			if prior, duplicate := globalEvents[event.EventID]; duplicate {
				return Bundle{}, invalid(
					"raw event %s occurs in scenarios %s and %s", event.EventID, prior, scenario,
				)
			}
			globalEvents[event.EventID] = scenario
		}
	}

	claims, err := projectCampaign(campaign)
	if err != nil {
		return Bundle{}, err
	}
	checks, measurements := claims.envelopeObservations()
	manifest := campaign.Manifest
	artifacts := claims.typedArtifacts(manifest, checks, measurements)
	artifactBytes := make(map[ArtifactKind][]byte, len(artifacts))
	references := make([]productiongates.EvidenceArtifact, 0, len(artifacts))
	verifiedArtifacts := make(map[string]productiongates.TypedEvidenceArtifact, len(artifacts))
	for _, item := range artifactContract {
		artifact := artifacts[item.Kind]
		encoded, err := json.MarshalIndent(artifact, "", "  ")
		if err != nil {
			return Bundle{}, invalid("encode %s artifact: %v", item.Kind, err)
		}
		encoded = append(encoded, '\n')
		digest := sha256.Sum256(encoded)
		artifactBytes[item.Kind] = encoded
		verifiedArtifacts[string(item.Kind)] = artifact
		references = append(references, productiongates.EvidenceArtifact{
			Kind: string(item.Kind), Ref: item.Ref, Digest: "sha256:" + hex.EncodeToString(digest[:]),
		})
	}
	contract, ok := productiongates.TypedEvidenceContractForGate(
		productiongates.GateStateEventFaultInjection,
	)
	if !ok {
		return Bundle{}, invalid("state-event fault gate contract is unavailable")
	}
	evidence := productiongates.TypedEvidence{
		SchemaVersion:    SchemaVersion,
		Gate:             productiongates.GateStateEventFaultInjection,
		CriteriaRevision: contract.CriteriaRevision,
		ReleaseDigest:    manifest.ReleaseDigest, ConfigurationRevision: manifest.ConfigurationRevision,
		ValidationEnvironment: manifest.ValidationEnvironment, Owner: manifest.Owner,
		StartedAt: manifest.StartedAt, CompletedAt: manifest.CompletedAt,
		Checks: checks, Measurements: measurements, Artifacts: references,
	}
	receipt := productiongates.Receipt{
		SchemaVersion: SchemaVersion, Gate: evidence.Gate,
		ReleaseDigest: evidence.ReleaseDigest, ConfigurationRevision: evidence.ConfigurationRevision,
		ValidationEnvironment: evidence.ValidationEnvironment, Result: productiongates.ResultPass,
		Owner: evidence.Owner, AcceptanceThreshold: evidence.AcceptanceThreshold(),
		ObservedResult: evidence.ObservedResult(), StartedAt: evidence.StartedAt,
		CompletedAt: evidence.CompletedAt, RecordedAt: evidence.CompletedAt.Add(time.Second),
		EvidenceRef:    "state-event-fault-injection.json",
		EvidenceDigest: "sha256:" + strings.Repeat("0", 64),
	}
	if err := evidence.Validate(receipt); err != nil {
		return Bundle{}, invalid("validate typed evidence: %v", err)
	}
	if err := productiongates.ValidateTypedEvidenceArtifacts(evidence, verifiedArtifacts); err != nil {
		return Bundle{}, invalid("validate typed artifacts: %v", err)
	}
	evidenceBytes, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		return Bundle{}, invalid("encode typed evidence: %v", err)
	}
	evidenceBytes = append(evidenceBytes, '\n')
	return Bundle{
		Evidence: evidence, EvidenceBytes: evidenceBytes,
		Artifacts: artifacts, ArtifactBytes: artifactBytes,
	}, nil
}

func typedArtifact(
	manifest Manifest,
	kind ArtifactKind,
	checks []productiongates.EvidenceCheck,
	measurements []productiongates.EvidenceMeasurement,
	fault *productiongates.StateEventFaultArtifact,
) productiongates.TypedEvidenceArtifact {
	return productiongates.TypedEvidenceArtifact{
		SchemaVersion: SchemaVersion, Gate: productiongates.GateStateEventFaultInjection,
		Kind: string(kind), ReleaseDigest: manifest.ReleaseDigest,
		ConfigurationRevision: manifest.ConfigurationRevision,
		ValidationEnvironment: manifest.ValidationEnvironment, Owner: manifest.Owner,
		StartedAt: manifest.StartedAt, CompletedAt: manifest.CompletedAt,
		Checks:          append([]productiongates.EvidenceCheck(nil), checks...),
		Measurements:    append([]productiongates.EvidenceMeasurement(nil), measurements...),
		StateEventFault: fault,
	}
}

func validText(value string, maximum int) bool {
	if value == "" || len(value) > maximum || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func SortedScenarios(receipts map[Scenario]ScenarioReceipt) []Scenario {
	scenarios := make([]Scenario, 0, len(receipts))
	for scenario := range receipts {
		scenarios = append(scenarios, scenario)
	}
	sort.Slice(scenarios, func(left, right int) bool { return scenarios[left] < scenarios[right] })
	return scenarios
}
