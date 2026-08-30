package capacitysim

import (
	"encoding/json"
	"fmt"
	"sort"
)

func Compare(
	baseline SimulationReceipt,
	candidate SimulationReceipt,
) (ReceiptComparison, error) {
	if err := validateReceipt(baseline); err != nil {
		return ReceiptComparison{}, fmt.Errorf("baseline: %w", err)
	}
	if err := validateReceipt(candidate); err != nil {
		return ReceiptComparison{}, fmt.Errorf("candidate: %w", err)
	}
	comparison := ReceiptComparison{
		SchemaVersion: SchemaVersion, BaselineReceiptDigest: baseline.ReceiptDigest,
		CandidateReceiptDigest: candidate.ReceiptDigest,
	}
	metrics := []struct {
		name      string
		baseline  int64
		candidate int64
	}{
		{"visible_completions", int64(baseline.Completion.VisibleCompletions), int64(candidate.Completion.VisibleCompletions)},
		{"success_rate_ppm", int64(baseline.Completion.SuccessRatePPM), int64(candidate.Completion.SuccessRatePPM)},
		{"throughput_per_second_ppm", baseline.Completion.ThroughputPerSecondPPM, candidate.Completion.ThroughputPerSecondPPM},
		{"latency_p50_ns", baseline.Latency.P50, candidate.Latency.P50},
		{"latency_p95_ns", baseline.Latency.P95, candidate.Latency.P95},
		{"latency_p99_ns", baseline.Latency.P99, candidate.Latency.P99},
		{"total_cost_micro_units", baseline.Cost.TotalMicroUnits, candidate.Cost.TotalMicroUnits},
		{"retry_waste_micro_units", baseline.Cost.RetryWasteMicroUnits, candidate.Cost.RetryWasteMicroUnits},
		{"cache_hits", int64(baseline.Cache.Hits), int64(candidate.Cache.Hits)},
		{"peak_buffer_bytes", baseline.Buffers.PeakBytes, candidate.Buffers.PeakBytes},
		{"peak_storage_bytes", baseline.Buffers.PeakStorageBytes, candidate.Buffers.PeakStorageBytes},
	}
	for _, metric := range metrics {
		comparison.Deltas = append(comparison.Deltas, MetricDelta{
			Metric: metric.name, BaselineValue: metric.baseline,
			CandidateValue: metric.candidate, Delta: metric.candidate - metric.baseline,
		})
	}
	baselineSources := evidenceSources(baseline.InputEvidence)
	candidateSources := evidenceSources(candidate.InputEvidence)
	paths := make(map[string]bool, len(baselineSources)+len(candidateSources))
	for path := range baselineSources {
		paths[path] = true
	}
	for path := range candidateSources {
		paths[path] = true
	}
	ordered := make([]string, 0, len(paths))
	for path := range paths {
		ordered = append(ordered, path)
	}
	sort.Strings(ordered)
	for _, path := range ordered {
		baselineKind := baselineSources[path]
		if baselineKind == "" {
			baselineKind = "MISSING"
		}
		candidateKind := candidateSources[path]
		if candidateKind == "" {
			candidateKind = "MISSING"
		}
		comparison.SourceClassifications = append(
			comparison.SourceClassifications,
			SourceClassificationComparison{
				Path: path, BaselineSourceKind: baselineKind,
				CandidateSourceKind: candidateKind,
			},
		)
	}
	comparison.ComparisonDigest = ""
	digest, err := digestValue(comparison)
	if err != nil {
		return ReceiptComparison{}, fmt.Errorf("digest comparison: %w", err)
	}
	comparison.ComparisonDigest = digest
	return comparison, nil
}

func EncodeComparison(comparison ReceiptComparison) ([]byte, error) {
	return json.Marshal(comparison)
}

func evidenceSources(evidence []InputEvidence) map[string]string {
	result := make(map[string]string, len(evidence))
	for _, item := range evidence {
		result[item.Path] = item.SourceKind
	}
	return result
}
