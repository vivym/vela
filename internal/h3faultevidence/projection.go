package h3faultevidence

import (
	"github.com/google/uuid"
	"github.com/vivym/vela/internal/productiongates"
)

type faultCampaignClaims struct {
	scenarioMatrix   productiongates.StateEventFaultArtifact
	authorityLedger  productiongates.StateEventFaultArtifact
	rawEventPayloads productiongates.StateEventFaultArtifact
}

func projectCampaign(campaign Campaign) (faultCampaignClaims, error) {
	references := make(map[Scenario]ScenarioReference, len(campaign.Manifest.Scenarios))
	for _, reference := range campaign.Manifest.Scenarios {
		references[reference.Scenario] = reference
	}
	claims := faultCampaignClaims{
		scenarioMatrix: productiongates.StateEventFaultArtifact{
			SourceManifestDigest: campaign.ManifestDigest,
			Scenarios:            make([]productiongates.StateEventFaultScenario, 0, len(AllScenarios())),
		},
		authorityLedger: productiongates.StateEventFaultArtifact{
			SourceManifestDigest: campaign.ManifestDigest,
			Authorities:          make([]productiongates.StateEventFaultAuthority, 0, len(AllScenarios())),
		},
		rawEventPayloads: productiongates.StateEventFaultArtifact{
			SourceManifestDigest: campaign.ManifestDigest,
			RawEventSets:         make([]productiongates.StateEventFaultRawEventSet, 0, len(AllScenarios())),
		},
	}
	for _, scenario := range AllScenarios() {
		receipt, receiptPresent := campaign.Receipts[scenario]
		reference, referencePresent := references[scenario]
		if !receiptPresent || !referencePresent {
			return faultCampaignClaims{}, invalid("scenario %s source projection is incomplete", scenario)
		}
		source := projectReceiptBinding(reference, receipt)
		claims.scenarioMatrix.Scenarios = append(
			claims.scenarioMatrix.Scenarios,
			productiongates.StateEventFaultScenario{
				Source: source, ControllerIdentity: receipt.ControllerIdentity,
				Target: productiongates.StateEventFaultTarget{
					Kind: receipt.Target.Kind, ID: receipt.Target.ID,
				},
				FaultWindow:         projectFaultWindow(receipt.FaultWindow),
				MaintenanceApproval: projectMaintenanceApproval(receipt.MaintenanceApproval),
			},
		)
		claims.authorityLedger.Authorities = append(
			claims.authorityLedger.Authorities,
			productiongates.StateEventFaultAuthority{
				Source: source, Before: projectAuthorityObservation(receipt.AuthorityBefore),
				After:        projectAuthorityObservation(receipt.AuthorityAfter),
				Measurements: projectMeasurements(receipt.Measurements),
				StaleProbes:  projectStaleProbes(receipt.StaleAuthorityProbes),
			},
		)
		claims.rawEventPayloads.RawEventSets = append(
			claims.rawEventPayloads.RawEventSets,
			productiongates.StateEventFaultRawEventSet{
				Source: source, Events: projectRawEvents(receipt.RawEvents),
			},
		)
	}
	return claims, nil
}

func (claims faultCampaignClaims) envelopeObservations() (
	[]productiongates.EvidenceCheck,
	[]productiongates.EvidenceMeasurement,
) {
	checks := make([]productiongates.EvidenceCheck, 0, len(claims.scenarioMatrix.Scenarios))
	var totals productiongates.StateEventFaultMeasurements
	for _, scenario := range claims.scenarioMatrix.Scenarios {
		checks = append(checks, productiongates.EvidenceCheck{ID: scenario.Source.Scenario, Passed: true})
	}
	for _, authority := range claims.authorityLedger.Authorities {
		totals.LostAcceptedJobCount += authority.Measurements.LostAcceptedJobCount
		totals.DuplicateVisibleCompletionCount += authority.Measurements.DuplicateVisibleCompletionCount
		totals.DuplicateChargeCount += authority.Measurements.DuplicateChargeCount
		totals.StaleAuthorityAcceptanceCount += authority.Measurements.StaleAuthorityAcceptanceCount
	}
	measurements := []productiongates.EvidenceMeasurement{
		{ID: "lost-accepted-job-count", Unit: "count", Comparator: productiongates.EvidenceEqual, Threshold: 0, Observed: totals.LostAcceptedJobCount},
		{ID: "duplicate-visible-completion-count", Unit: "count", Comparator: productiongates.EvidenceEqual, Threshold: 0, Observed: totals.DuplicateVisibleCompletionCount},
		{ID: "duplicate-charge-count", Unit: "count", Comparator: productiongates.EvidenceEqual, Threshold: 0, Observed: totals.DuplicateChargeCount},
		{ID: "stale-authority-acceptance-count", Unit: "count", Comparator: productiongates.EvidenceEqual, Threshold: 0, Observed: totals.StaleAuthorityAcceptanceCount},
	}
	return checks, measurements
}

func (claims faultCampaignClaims) typedArtifacts(
	manifest Manifest,
	checks []productiongates.EvidenceCheck,
	measurements []productiongates.EvidenceMeasurement,
) map[ArtifactKind]productiongates.TypedEvidenceArtifact {
	return map[ArtifactKind]productiongates.TypedEvidenceArtifact{
		ArtifactScenarioMatrix: typedArtifact(
			manifest, ArtifactScenarioMatrix, checks, nil, &claims.scenarioMatrix,
		),
		ArtifactAuthorityBeforeAfter: typedArtifact(
			manifest, ArtifactAuthorityBeforeAfter, nil, measurements, &claims.authorityLedger,
		),
		ArtifactRawEventPayloads: typedArtifact(
			manifest, ArtifactRawEventPayloads, nil, nil, &claims.rawEventPayloads,
		),
	}
}

func projectReceiptBinding(
	reference ScenarioReference,
	receipt ScenarioReceipt,
) productiongates.StateEventFaultReceiptBinding {
	return productiongates.StateEventFaultReceiptBinding{
		Scenario: string(receipt.Scenario), ReceiptRef: reference.Ref, ReceiptDigest: reference.Digest,
		ExerciseID: receipt.ExerciseID, StartedAt: receipt.StartedAt, CompletedAt: receipt.CompletedAt,
		AcceptedJobIDs: append([]uuid.UUID(nil), receipt.AcceptedJobIDs...),
	}
}

func projectFaultWindow(window FaultWindow) productiongates.StateEventFaultWindow {
	return productiongates.StateEventFaultWindow{
		Action: window.Action, InjectionPoint: window.InjectionPoint,
		OpenedAt: window.OpenedAt, TriggeredAt: window.TriggeredAt,
		RecoveryConfirmedAt: window.RecoveryConfirmedAt, TriggerEventID: window.TriggerEventID,
	}
}

func projectMaintenanceApproval(
	approval *MaintenanceApproval,
) *productiongates.StateEventFaultMaintenanceApproval {
	if approval == nil {
		return nil
	}
	return &productiongates.StateEventFaultMaintenanceApproval{
		Ref: approval.Ref, Digest: approval.Digest, ApprovedBy: approval.ApprovedBy,
		ApprovedAt: approval.ApprovedAt, Reason: approval.Reason, TargetID: approval.TargetID,
	}
}

func projectAuthorityObservation(
	observation AuthorityObservation,
) productiongates.StateEventAuthorityObservation {
	return productiongates.StateEventAuthorityObservation{
		CapturedAt: observation.CapturedAt, DatabaseSnapshotID: observation.DatabaseSnapshotID,
		JobLedgerDigest:        observation.JobLedgerDigest,
		CompletionLedgerDigest: observation.CompletionLedgerDigest,
		ChargeLedgerDigest:     observation.ChargeLedgerDigest,
		AcceptedJobCount:       observation.AcceptedJobCount,
		VisibleCompletionCount: observation.VisibleCompletionCount,
		ChargeCount:            observation.ChargeCount,
	}
}

func projectMeasurements(measurements Measurements) productiongates.StateEventFaultMeasurements {
	return productiongates.StateEventFaultMeasurements{
		LostAcceptedJobCount:            measurements.LostAcceptedJobCount,
		DuplicateVisibleCompletionCount: measurements.DuplicateVisibleCompletionCount,
		DuplicateChargeCount:            measurements.DuplicateChargeCount,
		StaleAuthorityAcceptanceCount:   measurements.StaleAuthorityAcceptanceCount,
	}
}

func projectStaleProbes(probes []StaleAuthorityProbe) []productiongates.StateEventStaleAuthorityProbe {
	projected := make([]productiongates.StateEventStaleAuthorityProbe, 0, len(probes))
	for _, probe := range probes {
		projected = append(projected, productiongates.StateEventStaleAuthorityProbe{
			ID: probe.ID, Kind: string(probe.Kind), JobID: probe.JobID,
			StageRunID: probe.StageRunID, WorkerInstanceID: probe.WorkerInstanceID,
			PresentedAuthorityDigest: probe.PresentedAuthorityDigest,
			CurrentAuthorityDigest:   probe.CurrentAuthorityDigest, Decision: probe.Decision,
			ReasonCode: probe.ReasonCode, RejectedAt: probe.RejectedAt,
		})
	}
	return projected
}

func projectRawEvents(events []RawEventObservation) []productiongates.StateEventFaultRawEvent {
	projected := make([]productiongates.StateEventFaultRawEvent, 0, len(events))
	for _, event := range events {
		projected = append(projected, productiongates.StateEventFaultRawEvent{
			EventID: event.EventID, AggregateType: event.AggregateType, AggregateID: event.AggregateID,
			AggregateVersion: event.AggregateVersion, EventType: event.EventType,
			PayloadDigest: event.PayloadDigest, Payload: append([]byte(nil), event.Payload...),
			PublishedCount: event.PublishedCount,
			ConsumedCount:  event.ConsumedCount,
		})
	}
	return projected
}
