package capacitysim

import (
	"math"
	"math/big"
	"sort"
)

type stageMetricAccumulator struct {
	stageID           string
	profileRevision   string
	requestCohort     string
	queue             []int64
	transfer          []int64
	service           []int64
	materialization   []int64
	outputBytes       []int64
	starts            int
	seals             int
	completions       int
	retries           int
	failures          int
	cacheHits         int
	maximumQueueDepth int
}

func (metric *stageMetricAccumulator) receipt() StageMetrics {
	return StageMetrics{
		StageID: metric.stageID, ProfileRevision: metric.profileRevision,
		RequestCohort: metric.requestCohort,
		Queue:         durationStats(metric.queue), Transfer: durationStats(metric.transfer),
		Service:         durationStats(metric.service),
		Materialization: durationStats(metric.materialization),
		OutputBytes:     durationStats(metric.outputBytes), Starts: metric.starts,
		Seals: metric.seals, Completions: metric.completions, Retries: metric.retries,
		Failures: metric.failures, CacheHits: metric.cacheHits,
		MaximumQueueDepth: metric.maximumQueueDepth,
	}
}

func durationStats(values []int64) DurationStats {
	if len(values) == 0 {
		return DurationStats{}
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(left, right int) bool { return sorted[left] < sorted[right] })
	return DurationStats{
		Count: len(sorted), P50: percentile(sorted, 50), P95: percentile(sorted, 95),
		P99: percentile(sorted, 99), Max: sorted[len(sorted)-1],
	}
}

func percentile(sorted []int64, percent int) int64 {
	if len(sorted) == 0 {
		return 0
	}
	index := (percent*len(sorted) + 99) / 100
	if index <= 0 {
		index = 1
	}
	if index > len(sorted) {
		index = len(sorted)
	}
	return sorted[index-1]
}

func relativeErrorPPM(predicted, observed int64) int {
	if observed <= 0 {
		return 0
	}
	difference := predicted - observed
	if difference < 0 {
		difference = -difference
	}
	if difference > math.MaxInt64/1_000_000 {
		return 1_000_000_000
	}
	value := difference * 1_000_000 / observed
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(value)
}

func addReason(reasons map[string]int, reason string) {
	reasons[reason]++
}

func reasonCounts(reasons map[string]int) []ReasonCount {
	keys := make([]string, 0, len(reasons))
	for reason := range reasons {
		keys = append(keys, reason)
	}
	sort.Strings(keys)
	result := make([]ReasonCount, 0, len(keys))
	for _, reason := range keys {
		result = append(result, ReasonCount{Reason: reason, Count: reasons[reason]})
	}
	return result
}

func multiplyDivide(value, multiplier, divisor int64) int64 {
	if value <= 0 || multiplier <= 0 || divisor <= 0 {
		return 0
	}
	if value > math.MaxInt64/multiplier {
		return math.MaxInt64
	}
	return value * multiplier / divisor
}

func resourceTimeCost(bytes, durationNS, microUnitsPerGBSecond int64) int64 {
	if bytes <= 0 || durationNS <= 0 || microUnitsPerGBSecond <= 0 {
		return 0
	}
	value := new(big.Int).SetInt64(bytes)
	value.Mul(value, new(big.Int).SetInt64(durationNS))
	value.Mul(value, new(big.Int).SetInt64(microUnitsPerGBSecond))
	value.Div(value, new(big.Int).SetInt64(1_000_000_000_000_000_000))
	if !value.IsInt64() {
		return math.MaxInt64
	}
	return value.Int64()
}
