package capacitysim

import (
	"errors"
	"sort"
)

func ProposeResidency(
	scenario ScenarioRevision,
	receipt SimulationReceipt,
) (ResidencyProposal, error) {
	if err := validateScenario(scenario); err != nil {
		return ResidencyProposal{}, err
	}
	if err := validateReceipt(receipt); err != nil || !receipt.Validation.Valid ||
		!receipt.Conservation.Valid {
		return ResidencyProposal{}, errors.New("SimulationReceipt is not valid planning evidence")
	}
	metrics := make(map[string]PoolMetrics, len(receipt.Pools))
	for _, metric := range receipt.Pools {
		metrics[metric.PoolID] = metric
	}
	pools := append([]ResidentPool(nil), scenario.Pools...)
	sort.Slice(pools, func(left, right int) bool { return pools[left].ID < pools[right].ID })
	proposal := ResidencyProposal{
		SchemaVersion: SchemaVersion, InputDigest: receipt.ReceiptDigest,
		AutoApply: false, ConfidencePPM: minimumConfidence(scenario),
		ExpiresOffsetNS:  scenario.Policy.ProposalExpiryNS,
		CooldownNS:       scenario.Policy.ProposalCooldownNS,
		BudgetMicroUnits: receipt.Cost.TotalMicroUnits,
		ReasonCodes:      []string{"FIXED_LAYOUT_REPLAY", "NO_HEALTHY_RESIDENCY_RELEASE"},
		UnresolvedRisks:  []string{"NO_PRODUCTION_SOAK", "NO_LAUNCH_RECEIPT"},
	}
	if scenario.Provenance.SourceKind != "MEASURED" {
		proposal.UnresolvedRisks = append(proposal.UnresolvedRisks, "NON_MEASURED_SCENARIO_INPUT")
	}
	for _, pool := range pools {
		desired := pool.WorkerCount
		metric := metrics[pool.ID]
		capacityNS := metric.ResidencyNS
		if capacityNS > 0 && metric.BusyNS*1_000_000/capacityNS >= 800_000 && desired < pool.MaxCount {
			desired++
			proposal.ReasonCodes = append(proposal.ReasonCodes, "HIGH_UTILIZATION:"+pool.ID)
		}
		proposal.Pools = append(proposal.Pools, ResidencyPoolProposal{
			PoolID: pool.ID, CurrentCount: pool.WorkerCount,
			MinCount: pool.MinCount, DesiredCount: desired, MaxCount: pool.MaxCount,
		})
	}
	sort.Strings(proposal.ReasonCodes)
	sort.Strings(proposal.UnresolvedRisks)
	return proposal, nil
}

func minimumConfidence(scenario ScenarioRevision) int {
	confidence := scenario.Provenance.ConfidencePPM
	if scenario.CostModel.Provenance.ConfidencePPM < confidence {
		confidence = scenario.CostModel.Provenance.ConfidencePPM
	}
	return confidence
}
