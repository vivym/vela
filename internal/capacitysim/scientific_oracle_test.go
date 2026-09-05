package capacitysim_test

import (
	"math"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/capacitysim"
)

func TestIndependentOracleUsesAllWarmPools(t *testing.T) {
	for _, seed := range []uint64{0, 7, 982451653} {
		scenario, workload, calibration := singleStageAnalyticalFixture(2)
		scenario.Seed = seed
		other := scenario.Pools[0]
		other.ID += "-second"
		scenario.Pools = append(scenario.Pools, other)
		calibration.StageModels[0].ServiceTime = constantDistribution(10)
		receipt := simulateOracle(t, scenario, workload, calibration)
		if receipt.Latency.P99 != 13 || receipt.Stages[0].Queue.Max != 0 {
			t.Fatalf("two free workers must finish both jobs at 10 + 1 + 1 + 1 ns: latency=%#v stage=%#v", receipt.Latency, receipt.Stages[0])
		}
		for _, pool := range receipt.Pools {
			if pool.BusyNS != 12 {
				t.Fatalf("each pool must execute exactly one 12 ns stage: %#v", receipt.Pools)
			}
		}
	}
}

func TestIndependentOracleClipsBusyCostAtWindow(t *testing.T) {
	scenario, workload, calibration := singleStageAnalyticalFixture(1)
	scenario.WindowDurationNS = 500_000_000
	scenario.CostModel.GPUMicroUnitsPerSecond = 10_000
	workload.Records[0].JobExpiryOffsetNS = 2_000_000_000
	calibration.StageModels[0].ServiceTime = constantDistribution(1_000_000_000)
	receipt := simulateOracle(t, scenario, workload, calibration)
	if receipt.Pools[0].BusyNS != 500_000_000 || receipt.Pools[0].IdleNS != 0 ||
		receipt.Cost.DirectGPUMicroUnits != 5_000 || receipt.Conservation.Unfinished != 1 {
		t.Fatalf("half a GPU-second must be charged inside the window: pool=%#v cost=%#v conservation=%#v", receipt.Pools[0], receipt.Cost, receipt.Conservation)
	}
}

func TestIndependentOracleCountsUnservedQueueStarvation(t *testing.T) {
	scenario, workload, calibration := singleStageAnalyticalFixture(1)
	scenario.WindowDurationNS = 100
	scenario.Pools[0].WarmReadyOffsetNS = 200
	workload.Records[0].JobExpiryOffsetNS = 300
	receipt := simulateOracle(t, scenario, workload, calibration)
	if len(receipt.Fairness) != 1 || receipt.Fairness[0].MaximumStarvationNS != 100 || receipt.Fairness[0].AttainedServiceNS != 0 {
		t.Fatalf("a queued job has waited 100 ns even when it has never started: %#v", receipt.Fairness)
	}
}

func TestIndependentOracleThroughputDividesAfterWideMultiplication(t *testing.T) {
	const jobs = 10_000
	scenario, workload, calibration := singleStageAnalyticalFixture(jobs)
	scenario.WindowDurationNS = jobs * 4
	calibration.StageModels[0].ServiceTime = constantDistribution(1)
	for index := range workload.Records {
		workload.Records[index].ArrivalOffsetNS = int64(index * 4)
		workload.Records[index].JobExpiryOffsetNS = scenario.WindowDurationNS + 1
	}
	receipt := simulateOracle(t, scenario, workload, calibration)
	if receipt.Completion.VisibleCompletions != jobs || receipt.Completion.ThroughputPerSecondPPM != 250_000_000_000_000 {
		t.Fatalf("10,000 completions / 40,000 ns = 250,000,000 completions/s: %#v", receipt.Completion)
	}
}

func TestIndependentOracleTransferAndQueueAreDisjoint(t *testing.T) {
	scenario, workload, calibration := twoStageOracleFixture()
	calibration.StageModels[0].OutputBytes = constantDistribution(100)
	calibration.ConnectorModels[0].PayloadBytesPerSecond = 1_000_000_000
	receipt := simulateOracle(t, scenario, workload, calibration)
	downstream := stageOracle(t, receipt, scenario.Stages[1].ID)
	if downstream.Queue.Max != 0 || downstream.Transfer.P50 != 100 || receipt.Latency.P99 != 107 {
		t.Fatalf("3 ns root + 100 ns transfer + 0 ns queue + 3 ns downstream + 1 ns finalization: latency=%#v stage=%#v", receipt.Latency, downstream)
	}
}

func TestIndependentOracleTenGBTransferUsesBytesPerSecond(t *testing.T) {
	scenario, workload, calibration := twoStageOracleFixture()
	scenario.WindowDurationNS = 3_000_000_000
	scenario.Limits.MaxBufferBytes = 30_000_000_000
	scenario.Limits.MaxStorageBytes = 30_000_000_000
	workload.Records[0].JobExpiryOffsetNS = 3_000_000_001
	calibration.StageModels[0].OutputBytes = constantDistribution(10_000_000_000)
	calibration.ConnectorModels[0].PayloadBytesPerSecond = 10_000_000_000
	receipt := simulateOracle(t, scenario, workload, calibration)
	downstream := stageOracle(t, receipt, scenario.Stages[1].ID)
	if downstream.Transfer.P50 != 1_000_000_000 || receipt.Latency.P99 != 1_000_000_007 {
		t.Fatalf("10 GB / 10 GB/s must take 1 second: latency=%#v stage=%#v", receipt.Latency, downstream)
	}
}

func TestIndependentOracleTransferSensitivityKeepsFractionalBandwidth(t *testing.T) {
	scenario, workload, calibration := twoStageOracleFixture()
	scenario.WindowDurationNS = 3_000_000_000
	workload.Records[0].JobExpiryOffsetNS = 3_000_000_001
	calibration.ConnectorModels[0].PayloadBytesPerSecond = 1
	receipt := simulateOracle(t, scenario, workload, calibration)
	if receipt.TransferSensitivity[0].LatencyP99NS != 2_000_000_007 ||
		receipt.TransferSensitivity[2].LatencyP99NS != 500_000_007 {
		t.Fatalf("one byte at 0.5 and 2 bytes/s takes 2 and 0.5 seconds: %#v", receipt.TransferSensitivity)
	}
}

func TestIndependentOracleStorageIntegratesBeforeUnitConversion(t *testing.T) {
	scenario, workload, calibration := singleStageAnalyticalFixture(1)
	scenario.WindowDurationNS = 3_000_000_000
	scenario.Policy.FinalizationDurationNS = 2_000_000_000
	scenario.Limits.MaxBufferBytes = 20_000_000_000
	scenario.Limits.MaxStorageBytes = 20_000_000_000
	scenario.CostModel.StorageMicroUnitsPerGBSecond = 1
	workload.Records[0].JobExpiryOffsetNS = 3_000_000_001
	calibration.StageModels[0].ServiceTime = constantDistribution(1)
	calibration.StageModels[0].OutputBytes = constantDistribution(10_000_000_000)
	receipt := simulateOracle(t, scenario, workload, calibration)
	if receipt.Cost.StorageMicroUnits != 20 || receipt.Conservation.StorageBytes != 0 {
		t.Fatalf("10 GB retained for 2 seconds at 1 micro-unit/GB-second costs 20: %#v", receipt.Cost)
	}
}

func TestIndependentOracleCostMultipliesWithoutPrematureSaturation(t *testing.T) {
	scenario, workload, calibration := singleStageAnalyticalFixture(1)
	scenario.WindowDurationNS = 30_000_000_000
	scenario.CostModel.GPUMicroUnitsPerSecond = 1_000_000_000
	workload.Records[0].JobExpiryOffsetNS = 30_000_000_001
	calibration.StageModels[0].ServiceTime = constantDistribution(20_000_000_000)
	receipt := simulateOracle(t, scenario, workload, calibration)
	if receipt.Cost.DirectGPUMicroUnits != 20_000_000_002 {
		t.Fatalf("(20 s + 2 ns) * 1e9 micro-units/s: %#v", receipt.Cost)
	}
}

func TestIndependentOraclePinnedProducerCannotEvictLiveInput(t *testing.T) {
	scenario, workload, calibration := twoStageOracleFixture()
	scenario.WindowDurationNS = 500
	scenario.Pools[1].WarmReadyOffsetNS = 1_000
	scenario.Policy.CacheEnabled = true
	scenario.Policy.CacheTTLNS = 10_000
	scenario.Limits.MaxCacheEntries = 1
	scenario.Limits.MaxCacheBytes = 100
	calibration.StageModels[0].OutputBytes = constantDistribution(10)
	workload.Records = append(workload.Records, arrival("second-live-input", 10))
	for index := range workload.Records {
		workload.Records[index].CacheKeyCohort = workload.Records[index].TraceID
		workload.Records[index].JobExpiryOffsetNS = 2_000
	}
	receipt := simulateOracle(t, scenario, workload, calibration)
	if receipt.Conservation.StorageBytes != 20 || receipt.Cache.Evictions != 0 || receipt.Conservation.Unfinished != 2 {
		t.Fatalf("two live 10-byte inputs require 20 storage bytes despite one-entry cache quota: conservation=%#v cache=%#v", receipt.Conservation, receipt.Cache)
	}
}

func TestIndependentOracleMissingPredictionIsUnavailable(t *testing.T) {
	scenario, workload, calibration := singleStageAnalyticalFixture(1)
	scenario.Pools[0].WarmReadyOffsetNS = scenario.WindowDurationNS + 1
	workload.Records[0].ObservedStages = []capacitysim.ObservedStageTiming{{
		StageID: scenario.Stages[0].ID, ProfileRevision: scenario.Stages[0].ProfileRevision,
		RequestCohort: workload.Records[0].RequestCohort, ServiceNS: 10,
	}}
	receipt := simulateOracle(t, scenario, workload, calibration)
	if len(receipt.CalibrationErrors) != 1 || receipt.CalibrationErrors[0].Status != "NO_PREDICTED_SAMPLES" ||
		receipt.CalibrationErrors[0].StageID != scenario.Stages[0].ID {
		t.Fatalf("unexecuted observed stage has no prediction, not a zero-duration prediction: %#v", receipt.CalibrationErrors)
	}
}

func TestIndependentOracleExpiryReleasesQueueCredit(t *testing.T) {
	scenario, workload, calibration := singleStageAnalyticalFixture(3)
	scenario.Limits.MaxQueuePerStage = 1
	workload.Records[1].JobExpiryOffsetNS = 5
	workload.Records[2].ArrivalOffsetNS = 6
	calibration.StageModels[0].ServiceTime = constantDistribution(10)
	receipt := simulateOracle(t, scenario, workload, calibration)
	if receipt.Conservation.VisibleCompletions != 2 || receipt.Conservation.Expired != 1 || receipt.Conservation.Failed != 0 {
		t.Fatalf("expired queued job must release its queue credit before the third arrival: %#v", receipt.Conservation)
	}
}

func TestIndependentOracleCompletionReleasesAdmissionAtSameTimestamp(t *testing.T) {
	scenario, workload, calibration := singleStageAnalyticalFixture(2)
	scenario.Limits.MaxJobs = 1
	workload.Records[1].ArrivalOffsetNS = 4
	calibration.StageModels[0].ServiceTime = constantDistribution(1)
	receipt := simulateOracle(t, scenario, workload, calibration)
	if receipt.Conservation.VisibleCompletions != 2 || receipt.Conservation.Rejected != 0 {
		t.Fatalf("the first Job releases its slot at t=4 before the second arrival at t=4: %#v", receipt.Conservation)
	}
}

func TestIndependentOracleCountsFailedOrganizationService(t *testing.T) {
	scenario, workload, calibration := singleStageAnalyticalFixture(2)
	workload.Records[0].OrganizationCohort = "organization-a"
	workload.Records[1].OrganizationCohort = "organization-b"
	calibration.StageModels[0].ServiceTime = constantDistribution(10)
	calibration.StageModels[0].FailureRatePPM = 1_000_000
	receipt := simulateOracle(t, scenario, workload, calibration)
	if len(receipt.Fairness) != 2 {
		t.Fatalf("failed work still consumes organization service: %#v", receipt.Fairness)
	}
	for _, cohort := range receipt.Fairness {
		if cohort.AttainedServiceNS != 10 || cohort.ShareErrorPPM != 0 {
			t.Fatalf("each failing organization consumed 10 ns: %#v", receipt.Fairness)
		}
	}
}

func TestIndependentOracleProposalBindsExactScenario(t *testing.T) {
	scenario, workload, calibration := singleStageAnalyticalFixture(1)
	receipt := simulateOracle(t, scenario, workload, calibration)
	scenario.Pools[0].WorkerCount++
	if _, err := capacitysim.ProposeResidency(scenario, receipt); err == nil {
		t.Fatal("proposal accepted receipt from a different worker layout")
	}
}

func TestIndependentOracleProposalUsesLongWindowUtilization(t *testing.T) {
	scenario, workload, calibration := singleStageAnalyticalFixture(1)
	scenario.WindowDurationNS = 100_000_000_000_000
	workload.Records[0].JobExpiryOffsetNS = scenario.WindowDurationNS + 1
	calibration.StageModels[0].ServiceTime = constantDistribution(scenario.WindowDurationNS)
	receipt := simulateOracle(t, scenario, workload, calibration)
	proposal, err := capacitysim.ProposeResidency(scenario, receipt)
	if err != nil || proposal.Pools[0].DesiredCount != 2 {
		t.Fatalf("100%% utilized worker in a long window should propose an extra slot: %#v %v", proposal, err)
	}
}

func TestIndependentOracleRejectsUnrepresentableResourceTime(t *testing.T) {
	scenario, workload, calibration := singleStageAnalyticalFixture(1)
	scenario.WindowDurationNS = 365 * 24 * 60 * 60 * 1_000_000_000
	scenario.Pools[0].WorkerCount = 100_000
	scenario.Pools[0].MaxCount = 100_000
	if err := capacitysim.Validate(scenario, workload, calibration); err == nil {
		t.Fatal("one year times 100,000 workers does not fit the int64 nanosecond receipt")
	}
	scenario, workload, calibration = singleStageAnalyticalFixture(1)
	scenario.CostModel.GPUMicroUnitsPerSecond = math.MaxInt64
	scenario.WindowDurationNS = 2_000_000_000
	if _, err := capacitysim.Simulate(scenario, workload, calibration); err == nil || !strings.Contains(err.Error(), "receipt range") {
		t.Fatalf("unrepresentable cost must fail explicitly: %v", err)
	}
}

func TestIndependentOracleRejectsUnimplementedModelParameters(t *testing.T) {
	for _, parameter := range []string{"production-scheduler", "resource-multiplier", "object-operation-cost"} {
		t.Run(parameter, func(t *testing.T) {
			scenario, workload, calibration := twoStageOracleFixture()
			switch parameter {
			case "production-scheduler":
				scenario.Policy.SchedulerRevision = "stage-filter-fairness-score-pick-v1"
			case "resource-multiplier":
				scenario.Policy.JobResourceMultiplierPPM = 2_000_000
			case "object-operation-cost":
				calibration.ConnectorModels[0].ObjectReadMicroUnits = 1
			}
			if err := capacitysim.Validate(scenario, workload, calibration); err == nil {
				t.Fatal("an unimplemented parameter must not silently produce a model result")
			}
		})
	}
}

func twoStageOracleFixture() (capacitysim.ScenarioRevision, capacitysim.WorkloadTrace, capacitysim.CalibrationBundle) {
	scenario, workload, calibration := fixedPipelineFixture()
	scenario.Stages = scenario.Stages[:2]
	scenario.Pools = scenario.Pools[:2]
	calibration.StageModels = calibration.StageModels[:2]
	calibration.ConnectorModels = calibration.ConnectorModels[:1]
	for index := range scenario.Pools {
		scenario.Pools[index].WorkerCount = 1
		scenario.Pools[index].WarmReadyOffsetNS = 0
		calibration.StageModels[index].ServiceTime = constantDistribution(1)
		calibration.StageModels[index].SealTime = constantDistribution(1)
		calibration.StageModels[index].MaterializationTime = constantDistribution(1)
		calibration.StageModels[index].OutputBytes = constantDistribution(1)
		calibration.StageModels[index].FailureRatePPM = 0
	}
	calibration.ConnectorModels[0].SetupLatencyNS = 0
	calibration.ConnectorModels[0].FailureRatePPM = 0
	calibration.ConnectorModels[0].PayloadBytesPerSecond = 1_000_000_000
	scenario.Policy.CacheEnabled = false
	scenario.Policy.MaxRetriesPerStage = 0
	scenario.Policy.FinalizationDurationNS = 1
	workload.Records = workload.Records[:1]
	return scenario, workload, calibration
}

func simulateOracle(t *testing.T, scenario capacitysim.ScenarioRevision, workload capacitysim.WorkloadTrace, calibration capacitysim.CalibrationBundle) capacitysim.SimulationReceipt {
	t.Helper()
	receipt, err := capacitysim.Simulate(scenario, workload, calibration)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

func stageOracle(t *testing.T, receipt capacitysim.SimulationReceipt, id string) capacitysim.StageMetrics {
	t.Helper()
	for _, stage := range receipt.Stages {
		if stage.StageID == id {
			return stage
		}
	}
	t.Fatalf("missing stage %s", id)
	return capacitysim.StageMetrics{}
}
