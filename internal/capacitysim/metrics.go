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
	var product big.Int
	addProduct(&product, value, multiplier)
	return divideQuantity(&product, divisor)
}

func addProduct(total *big.Int, factors ...int64) {
	product := big.NewInt(1)
	for _, factor := range factors {
		product.Mul(product, big.NewInt(factor))
	}
	total.Add(total, product)
}

func divideQuantity(quantity *big.Int, divisor int64) int64 {
	value := new(big.Int).Quo(quantity, big.NewInt(divisor))
	if !value.IsInt64() {
		return math.MaxInt64
	}
	return value.Int64()
}

func multiplyQuantityDivide(quantity *big.Int, multiplier, divisor int64) int64 {
	value := new(big.Int).Mul(quantity, big.NewInt(multiplier))
	return divideQuantity(value, divisor)
}

func boundedSum(values ...int64) int64 {
	var sum int64
	for _, value := range values {
		if value > math.MaxInt64-sum {
			return math.MaxInt64
		}
		sum += value
	}
	return sum
}

func transferPayloadNS(bytes, bytesPerSecond, scalePPM int64) int64 {
	var numerator, denominator big.Int
	addProduct(&numerator, bytes, 1_000_000_000, 1_000_000)
	addProduct(&denominator, bytesPerSecond, scalePPM)
	numerator.Add(&numerator, &denominator)
	numerator.Sub(&numerator, big.NewInt(1))
	numerator.Quo(&numerator, &denominator)
	if !numerator.IsInt64() {
		return math.MaxInt64
	}
	return numerator.Int64()
}
