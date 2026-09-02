package capacitysim_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/vivym/vela/internal/capacitysim"
)

func TestRandomizedSimulationPreservesBoundsAndConservation(t *testing.T) {
	random := rand.New(rand.NewSource(20260831))
	for trial := 0; trial < 128; trial++ {
		scenario, workload, calibration := fixedPipelineFixture()
		scenario.Seed = random.Uint64()
		scenario.WindowDurationNS = 100_000
		scenario.Policy.CacheEnabled = random.Intn(2) == 1
		scenario.Policy.MaxRetriesPerStage = random.Intn(3)
		scenario.Policy.CacheTTLNS = int64(200 + random.Intn(2_000))
		jobCount := 1 + random.Intn(12)
		scenario.Limits.MaxJobs = jobCount
		scenario.Limits.MaxEvents = 20_000
		scenario.Limits.MaxQueuePerStage = 1 + random.Intn(jobCount)
		scenario.Limits.MaxBufferItems = 1 + random.Intn(jobCount*len(scenario.Stages))
		scenario.Limits.MaxBufferBytes = int64(500 + random.Intn(4_000))
		scenario.Limits.MaxStorageBytes = int64(2_000 + random.Intn(16_000))
		scenario.Limits.MaxCacheEntries = 1 + random.Intn(6)
		scenario.Limits.MaxCacheBytes = int64(500 + random.Intn(4_000))

		for index := range scenario.Pools {
			scenario.Pools[index].WorkerCount = 1 + random.Intn(scenario.Pools[index].MaxCount)
			scenario.Pools[index].WarmReadyOffsetNS = int64(random.Intn(40))
		}
		for index := range calibration.StageModels {
			service := int64(5 + random.Intn(50))
			output := int64(10 + random.Intn(90))
			calibration.StageModels[index].ServiceTime = constantDistribution(service)
			calibration.StageModels[index].OutputBytes = constantDistribution(output)
			calibration.StageModels[index].FailureRatePPM = random.Intn(300_001)
		}

		workload.Records = make([]capacitysim.TraceRecord, 0, jobCount)
		arrivalOffset := int64(0)
		for index := 0; index < jobCount; index++ {
			arrivalOffset += int64(random.Intn(25))
			record := arrival(fmt.Sprintf("trial-%03d-job-%03d", trial, index), arrivalOffset)
			record.JobExpiryOffsetNS = 90_000
			record.CacheKeyCohort = fmt.Sprintf("equivalence-%d", random.Intn(4))
			record.ProjectCohort = fmt.Sprintf("project-%d", random.Intn(2))
			workload.Records = append(workload.Records, record)
		}

		receipt, err := capacitysim.Simulate(scenario, workload, calibration)
		if err != nil {
			t.Fatalf("trial %d Simulate: %v", trial, err)
		}
		assertSimulationInvariants(t, trial, scenario, receipt)
	}
}

func TestFixedServicePipelineMatchesAnalyticalCapacity(t *testing.T) {
	scenario, workload, calibration := singleStageAnalyticalFixture(20)
	const serviceNS int64 = 1_000
	calibration.StageModels[0].ServiceTime = constantDistribution(serviceNS)
	scenario.WindowDurationNS = 20_050
	for index := range workload.Records {
		workload.Records[index].JobExpiryOffsetNS = 20_049
	}

	receipt, err := capacitysim.Simulate(scenario, workload, calibration)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Completion.VisibleCompletions != 20 || receipt.Conservation.Unfinished != 0 {
		t.Fatalf("fixed-service completion = %#v conservation=%#v", receipt.Completion, receipt.Conservation)
	}
	analyticalThroughputPPM := int64(1_000_000_000_000_000 / serviceNS)
	errorPPM := relativeDifferencePPM(
		receipt.Completion.ThroughputPerSecondPPM,
		analyticalThroughputPPM,
	)
	if errorPPM > 5_000 {
		t.Fatalf(
			"fixed-service throughput = %d, analytical = %d, error = %d ppm",
			receipt.Completion.ThroughputPerSecondPPM,
			analyticalThroughputPPM,
			errorPPM,
		)
	}
	stage := receipt.Stages[0]
	if stage.Service.P50 != serviceNS || stage.Service.P99 != serviceNS ||
		receipt.Pools[0].BusyNS != 20*(serviceNS+2) {
		t.Fatalf("fixed-service accounting = stage %#v pool %#v", stage, receipt.Pools[0])
	}
}

func TestMM1LikePipelineStaysBelowAnalyticalUtilizationBound(t *testing.T) {
	const jobs = 256
	scenario, workload, calibration := singleStageAnalyticalFixture(jobs)
	model := &calibration.StageModels[0]
	model.ServiceTime = capacitysim.Distribution{
		P50: 693, P95: 2_996, P99: 4_605, HardMax: 10_000,
	}
	meanServiceNS := piecewiseDistributionMean(model.ServiceTime)
	arrivalIntervalNS := meanServiceNS * 2
	for index := range workload.Records {
		workload.Records[index].ArrivalOffsetNS = int64(index) * arrivalIntervalNS
		workload.Records[index].JobExpiryOffsetNS = int64(jobs+2) * arrivalIntervalNS
	}
	scenario.WindowDurationNS = int64(jobs+2) * arrivalIntervalNS

	receipt, err := capacitysim.Simulate(scenario, workload, calibration)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Conservation.VisibleCompletions != jobs || receipt.Conservation.Unfinished != 0 {
		t.Fatalf("M/M/1-like fixture did not drain: %#v", receipt.Conservation)
	}
	arrivalRatePPM := int64(1_000_000_000_000_000 / arrivalIntervalNS)
	serviceCapacityPPM := int64(1_000_000_000_000_000 / meanServiceNS)
	if arrivalRatePPM*2 != serviceCapacityPPM &&
		relativeDifferencePPM(arrivalRatePPM*2, serviceCapacityPPM) > 1 {
		t.Fatalf("fixture utilization is not 0.5: arrival=%d capacity=%d", arrivalRatePPM, serviceCapacityPPM)
	}
	if receipt.Completion.ThroughputPerSecondPPM > serviceCapacityPPM ||
		receipt.Stages[0].MaximumQueueDepth >= scenario.Limits.MaxQueuePerStage {
		t.Fatalf(
			"M/M/1-like analytical bound violated: completion=%#v stage=%#v capacity=%d",
			receipt.Completion, receipt.Stages[0], serviceCapacityPPM,
		)
	}
}

func singleStageAnalyticalFixture(jobCount int) (
	capacitysim.ScenarioRevision,
	capacitysim.WorkloadTrace,
	capacitysim.CalibrationBundle,
) {
	scenario, workload, calibration := fixedPipelineFixture()
	scenario.Stages = scenario.Stages[:1]
	scenario.Pools = scenario.Pools[:1]
	scenario.Pools[0].WorkerCount = 1
	scenario.Pools[0].WarmReadyOffsetNS = 0
	scenario.Policy.CacheEnabled = false
	scenario.Policy.MaxRetriesPerStage = 0
	scenario.Policy.FinalizationDurationNS = 1
	scenario.Limits.MaxJobs = jobCount
	scenario.Limits.MaxEvents = jobCount*10 + 100
	scenario.Limits.MaxQueuePerStage = jobCount
	scenario.Limits.MaxBufferItems = jobCount
	scenario.Limits.MaxBufferBytes = int64(jobCount * 1_000)
	scenario.Limits.MaxStorageBytes = int64(jobCount * 1_000)
	calibration.StageModels = calibration.StageModels[:1]
	calibration.StageModels[0].SealTime = constantDistribution(1)
	calibration.StageModels[0].MaterializationTime = constantDistribution(1)
	calibration.StageModels[0].OutputBytes = constantDistribution(1)
	calibration.StageModels[0].FailureRatePPM = 0
	calibration.ConnectorModels = nil
	workload.Records = make([]capacitysim.TraceRecord, 0, jobCount)
	for index := 0; index < jobCount; index++ {
		workload.Records = append(workload.Records, arrival(fmt.Sprintf("analytical-%03d", index), 0))
	}
	return scenario, workload, calibration
}

func assertSimulationInvariants(
	t *testing.T,
	trial int,
	scenario capacitysim.ScenarioRevision,
	receipt capacitysim.SimulationReceipt,
) {
	t.Helper()
	conservation := receipt.Conservation
	if !conservation.Valid || conservation.Arrivals != conservation.Accepted+conservation.Rejected ||
		conservation.Accepted != conservation.VisibleCompletions+conservation.Failed+
			conservation.Expired+conservation.Unfinished {
		t.Fatalf("trial %d conservation = %#v", trial, conservation)
	}
	if conservation.EventsProcessed < 0 || conservation.EventsProcessed > scenario.Limits.MaxEvents ||
		conservation.StorageBytes < 0 || conservation.StorageBytes > scenario.Limits.MaxStorageBytes {
		t.Fatalf("trial %d event/storage bounds = %#v", trial, conservation)
	}
	if receipt.Buffers.PeakItems < 0 || receipt.Buffers.PeakItems > scenario.Limits.MaxBufferItems ||
		receipt.Buffers.PeakBytes < 0 || receipt.Buffers.PeakBytes > scenario.Limits.MaxBufferBytes ||
		receipt.Buffers.PeakStorageBytes < 0 ||
		receipt.Buffers.PeakStorageBytes > scenario.Limits.MaxStorageBytes {
		t.Fatalf("trial %d buffer bounds = %#v", trial, receipt.Buffers)
	}
	if receipt.Cache.Entries < 0 || receipt.Cache.Entries > scenario.Limits.MaxCacheEntries ||
		receipt.Cache.Bytes < 0 || receipt.Cache.Bytes > scenario.Limits.MaxCacheBytes ||
		receipt.Cache.Pins != receipt.Cache.PinReleases {
		t.Fatalf("trial %d cache/pin bounds = %#v", trial, receipt.Cache)
	}
	if conservation.Unfinished == 0 && conservation.StorageBytes != receipt.Cache.Bytes {
		t.Fatalf(
			"trial %d terminal storage/cache conservation = %d/%d",
			trial, conservation.StorageBytes, receipt.Cache.Bytes,
		)
	}
	for _, stage := range receipt.Stages {
		if stage.MaximumQueueDepth < 0 || stage.MaximumQueueDepth > scenario.Limits.MaxQueuePerStage {
			t.Fatalf("trial %d stage queue bound = %#v", trial, stage)
		}
		for name, stats := range map[string]capacitysim.DurationStats{
			"queue": stage.Queue, "transfer": stage.Transfer, "service": stage.Service,
			"materialization": stage.Materialization, "output": stage.OutputBytes,
		} {
			if stats.Count < 0 || stats.P50 < 0 || stats.P95 < 0 || stats.P99 < 0 || stats.Max < 0 {
				t.Fatalf("trial %d stage %s has negative %s stats: %#v", trial, stage.StageID, name, stats)
			}
		}
	}
	for _, pool := range receipt.Pools {
		if pool.BusyNS < 0 || pool.IdleNS < 0 || pool.ResidencyNS < 0 ||
			pool.BusyNS > pool.ResidencyNS ||
			pool.BusyDeviceNS != pool.BusyNS*int64(pool.DeviceCount) {
			t.Fatalf("trial %d pool conservation = %#v", trial, pool)
		}
	}
	for name, value := range map[string]int64{
		"gpu": receipt.Cost.DirectGPUMicroUnits, "cpu": receipt.Cost.DirectCPUMicroUnits,
		"memory": receipt.Cost.MemoryMicroUnits, "scratch": receipt.Cost.ScratchMicroUnits,
		"direct":   receipt.Cost.DirectStageMicroUnits,
		"shared":   receipt.Cost.SharedResidencyMicroUnits,
		"transfer": receipt.Cost.TransferMicroUnits, "storage": receipt.Cost.StorageMicroUnits,
		"retry": receipt.Cost.RetryWasteMicroUnits, "cache": receipt.Cost.CacheMicroUnits,
		"total": receipt.Cost.TotalMicroUnits,
	} {
		if value < 0 {
			t.Fatalf("trial %d has negative %s cost: %#v", trial, name, receipt.Cost)
		}
	}
}

func constantDistribution(value int64) capacitysim.Distribution {
	return capacitysim.Distribution{P50: value, P95: value, P99: value, HardMax: value}
}

func piecewiseDistributionMean(distribution capacitysim.Distribution) int64 {
	weightedTwice := (1+distribution.P50)*50 +
		(distribution.P50+distribution.P95)*45 +
		(distribution.P95+distribution.P99)*4 +
		(distribution.P99 + distribution.HardMax)
	return weightedTwice / 200
}

func relativeDifferencePPM(actual, expected int64) int64 {
	if expected <= 0 {
		return 0
	}
	difference := actual - expected
	if difference < 0 {
		difference = -difference
	}
	return difference * 1_000_000 / expected
}
