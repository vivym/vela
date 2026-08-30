package capacitysim_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/capacitysim"
)

func TestStrictInputsRejectCustomerContentAndMissingBounds(t *testing.T) {
	scenario, _, calibration := fixedPipelineFixture()
	encodedScenario := []byte(`{
		"schema_version":1,
		"revision":"scenario-v1",
		"algorithm_revision":"capacity-sim-v1",
		"seed":7,
		"graph_revision":"h3-graph-v1",
		"window_duration_ns":1000,
		"prompt":"must never enter the simulator"
	}`)
	if _, err := capacitysim.DecodeScenario(encodedScenario); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("DecodeScenario content field error = %v", err)
	}

	scenario.Limits.MaxEvents = 0
	if err := capacitysim.Validate(scenario, capacitysim.WorkloadTrace{}, calibration); err == nil ||
		!strings.Contains(err.Error(), "max_events") {
		t.Fatalf("Validate unbounded events error = %v", err)
	}

	trace := []byte("{\"record_kind\":\"TRACE_HEADER\",\"schema_version\":1,\"revision\":\"trace-v1\"}\n" +
		"{\"record_kind\":\"ARRIVAL\",\"schema_version\":1,\"trace_id\":\"cohort-1\",\"arrival_offset_ns\":0,\"prompt\":\"secret\"}\n")
	if _, err := capacitysim.DecodeTraceNDJSON(trace); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("DecodeTraceNDJSON content field error = %v", err)
	}
}

func TestFixedPipelineIsDeterministicConservativeAndDeviceSetAtomic(t *testing.T) {
	scenario, workload, calibration := fixedPipelineFixture()
	first, err := capacitysim.Simulate(scenario, workload, calibration)
	if err != nil {
		t.Fatalf("Simulate first: %v", err)
	}
	second, err := capacitysim.Simulate(scenario, workload, calibration)
	if err != nil {
		t.Fatalf("Simulate second: %v", err)
	}
	firstBytes, err := capacitysim.EncodeReceipt(first)
	if err != nil {
		t.Fatalf("EncodeReceipt first: %v", err)
	}
	secondBytes, err := capacitysim.EncodeReceipt(second)
	if err != nil {
		t.Fatalf("EncodeReceipt second: %v", err)
	}
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatalf("identical inputs produced different receipts:\n%s\n%s", firstBytes, secondBytes)
	}
	if first.ReceiptDigest == "" || first.ReceiptDigest != second.ReceiptDigest {
		t.Fatalf("receipt digests = %q/%q", first.ReceiptDigest, second.ReceiptDigest)
	}
	if first.Conservation.Arrivals != 3 || first.Conservation.Accepted != 3 ||
		first.Conservation.VisibleCompletions != 3 || first.Conservation.Rejected != 0 ||
		first.Conservation.Failed != 0 || first.Conservation.Expired != 0 ||
		first.Conservation.Unfinished != 0 || !first.Conservation.Valid {
		t.Fatalf("conservation = %#v", first.Conservation)
	}
	if len(first.Pools) != 3 {
		t.Fatalf("pool metrics = %d, want 3", len(first.Pools))
	}
	for _, pool := range first.Pools {
		if pool.StageID == "dit" && (pool.DeviceCount != 1 || pool.BusyDeviceNS != pool.BusyNS) {
			t.Fatalf("H3 DiT pool did not consume one complete single-GPU DeviceSet: %#v", pool)
		}
		if pool.StageID == "future-llm" && pool.BusyDeviceNS != pool.BusyNS*4 {
			t.Fatalf("multi-GPU pool was rank-scheduled instead of DeviceSet-scheduled: %#v", pool)
		}
	}
	if len(first.TransferSensitivity) != 4 {
		t.Fatalf("transfer sensitivity cases = %d, want 4", len(first.TransferSensitivity))
	}
}

func TestCalibrationErrorRemainsPerStageProfileAndCohort(t *testing.T) {
	scenario, workload, calibration := fixedPipelineFixture()
	workload.Records[0].ObservedStages = []capacitysim.ObservedStageTiming{{
		StageID: "encoder", ProfileRevision: "encoder-profile-v1",
		RequestCohort: "short", ServiceNS: 12,
	}}
	receipt, err := capacitysim.Simulate(scenario, workload, calibration)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipt.CalibrationErrors) != 1 {
		t.Fatalf("calibration errors = %#v", receipt.CalibrationErrors)
	}
	error := receipt.CalibrationErrors[0]
	if error.StageID != "encoder" || error.ProfileRevision != "encoder-profile-v1" ||
		error.RequestCohort != "short" || error.SampleCount != 1 || error.Status != "AVAILABLE" {
		t.Fatalf("calibration error lost stage/profile/cohort identity: %#v", error)
	}
}

func TestResidencyProposalIsAdvisoryOnly(t *testing.T) {
	scenario, workload, calibration := fixedPipelineFixture()
	receipt, err := capacitysim.Simulate(scenario, workload, calibration)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := capacitysim.ProposeResidency(scenario, receipt)
	if err != nil {
		t.Fatal(err)
	}
	if proposal.AutoApply || proposal.InputDigest != receipt.ReceiptDigest ||
		proposal.ExpiresOffsetNS <= 0 || proposal.CooldownNS <= 0 ||
		len(proposal.Pools) != len(scenario.Pools) || len(proposal.ReasonCodes) == 0 {
		t.Fatalf("ResidencyProposal = %#v", proposal)
	}
}

func TestCacheReuseIsExactAndProjectScoped(t *testing.T) {
	scenario, workload, calibration := fixedPipelineFixture()
	workload.Records = []capacitysim.TraceRecord{
		arrival("first", 0), arrival("same-project", 500), arrival("other-project", 600),
	}
	for index := range workload.Records {
		workload.Records[index].CacheKeyCohort = "equivalent-request-v1"
		workload.Records[index].JobExpiryOffsetNS = 995
	}
	workload.Records[2].ProjectCohort = "different-project"
	receipt, err := capacitysim.Simulate(scenario, workload, calibration)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Cache.Hits != 3 || receipt.Cache.Pins != 3 ||
		receipt.Cache.PinReleases != 3 || receipt.Cache.AvoidedComputeCount != 3 {
		t.Fatalf("project-scoped exact cache metrics = %#v, want one three-stage reuse", receipt.Cache)
	}
	if receipt.Conservation.VisibleCompletions != 3 || !receipt.Conservation.Valid {
		t.Fatalf("cache conservation = %#v", receipt.Conservation)
	}
}

func TestFailureRetriesAreBoundedAndChargedAsWaste(t *testing.T) {
	scenario, workload, calibration := fixedPipelineFixture()
	scenario.Stages = scenario.Stages[:1]
	scenario.Pools = scenario.Pools[:1]
	workload.Records = workload.Records[:1]
	calibration.StageModels = calibration.StageModels[:1]
	calibration.ConnectorModels = nil
	calibration.StageModels[0].FailureRatePPM = 1_000_000
	receipt, err := capacitysim.Simulate(scenario, workload, calibration)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Conservation.Failed != 1 || receipt.Failures.StageFailures != 2 ||
		receipt.Failures.Retries != 1 || receipt.Failures.RetryWasteDeviceNS <= 0 ||
		len(receipt.Stages) != 1 || receipt.Stages[0].Starts != 2 ||
		receipt.Stages[0].Retries != 1 || receipt.Stages[0].Failures != 2 {
		t.Fatalf("bounded retry receipt = %#v stages=%#v", receipt.Failures, receipt.Stages)
	}
}

func TestTransferSensitivityIsMonotonicAndIncludesOutage(t *testing.T) {
	scenario, workload, calibration := fixedPipelineFixture()
	receipt, err := capacitysim.Simulate(scenario, workload, calibration)
	if err != nil {
		t.Fatal(err)
	}
	cases := make(map[string]capacitysim.SensitivityResult)
	for _, result := range receipt.TransferSensitivity {
		cases[result.Case] = result
	}
	if cases["TRANSFER_0_5X"].LatencyP99NS < cases["TRANSFER_2X"].LatencyP99NS {
		t.Fatalf("transfer sensitivity is reversed: %#v", cases)
	}
	if cases["TRANSFER_OUTAGE"].VisibleCompletions != 0 ||
		cases["TRANSFER_OUTAGE"].Failed != 3 {
		t.Fatalf("outage sensitivity = %#v", cases["TRANSFER_OUTAGE"])
	}
}

func TestValidationBindsRuntimeModelToCompleteDeviceSet(t *testing.T) {
	scenario, workload, calibration := fixedPipelineFixture()
	calibration.StageModels[2].GPUCount = 1
	if err := capacitysim.Validate(scenario, workload, calibration); err == nil ||
		!strings.Contains(err.Error(), "DeviceSet") {
		t.Fatalf("mismatched multi-GPU runtime model error = %v", err)
	}
	scenario.Stages[1].DeviceCount = 2
	if err := capacitysim.Validate(scenario, workload, calibration); err == nil ||
		!strings.Contains(err.Error(), "DiT") {
		t.Fatalf("multi-GPU H3 DiT validation error = %v", err)
	}
}

func fixedPipelineFixture() (
	capacitysim.ScenarioRevision,
	capacitysim.WorkloadTrace,
	capacitysim.CalibrationBundle,
) {
	provenance := capacitysim.Provenance{
		SourceKind: "SYNTHETIC", CollectionWindow: "fixture-window-v1",
		HardwareRevision: "fixture-gpu-v1", RuntimeRevision: "runtime-v1",
		ModelRevision: "minimax-h3-v1", ProfileRevision: "profiles-v1",
		Units: "ns-bytes", SampleCount: 100, FreshnessOffsetNS: 10_000,
		ConfidencePPM: 900_000, ContentDigest: strings.Repeat("a", 64),
	}
	scenario := capacitysim.ScenarioRevision{
		SchemaVersion: 1, Revision: "scenario-v1",
		AlgorithmRevision: capacitysim.AlgorithmRevision, Seed: 7,
		GraphRevision: "h3-graph-v1", WindowDurationNS: 1_000,
		Provenance: provenance,
		Stages: []capacitysim.StageSpec{
			{ID: "encoder", ProfileRevision: "encoder-profile-v1", ResourceKind: "GPU", DeviceCount: 1},
			{ID: "dit", ProfileRevision: "dit-profile-v1", ResourceKind: "GPU", DeviceCount: 1, Dependencies: []string{"encoder"}, ConnectorRevision: "rdma-v1"},
			{ID: "future-llm", ProfileRevision: "llm-profile-v1", ResourceKind: "GPU", DeviceCount: 4, Dependencies: []string{"dit"}, ConnectorRevision: "rdma-v1"},
		},
		Pools: []capacitysim.ResidentPool{
			{ID: "encoder-pool", StageID: "encoder", ProfileRevision: "encoder-profile-v1", WorkerCount: 1, DeviceCount: 1, NetworkDomain: "rack-a", FaultDomain: "node-a", ModelRevision: "h3-encoder-v1", MinCount: 1, MaxCount: 2},
			{ID: "dit-pool", StageID: "dit", ProfileRevision: "dit-profile-v1", WorkerCount: 1, DeviceCount: 1, NetworkDomain: "rack-b", FaultDomain: "node-b", ModelRevision: "h3-dit-v1", MinCount: 1, MaxCount: 8},
			{ID: "llm-pool", StageID: "future-llm", ProfileRevision: "llm-profile-v1", WorkerCount: 1, DeviceCount: 4, NetworkDomain: "rack-c", FaultDomain: "node-c", ModelRevision: "future-llm-v1", MinCount: 1, MaxCount: 2},
		},
		Limits: capacitysim.Limits{
			MaxJobs: 16, MaxEvents: 1_000, MaxQueuePerStage: 16,
			MaxBufferItems: 16, MaxBufferBytes: 1 << 20,
			MaxStorageBytes: 1 << 24, MaxCacheEntries: 16, MaxCacheBytes: 1 << 20,
		},
		Policy: capacitysim.Policy{
			SchedulerRevision: "scheduler-v1", CachePolicyRevision: "cache-v1",
			MaxRetriesPerStage: 1, CacheEnabled: true, CacheTTLNS: 10_000,
			FinalizationDurationNS: 5, ReleaseHealthyResidency: false,
			ProposalCooldownNS: 100, ProposalExpiryNS: 500,
		},
		CostModel: capacitysim.CostModelRevision{
			Revision: "cost-v1", GPUMicroUnitsPerSecond: 1_000_000,
			CPUMicroUnitsPerSecond: 100_000, NetworkMicroUnitsPerGB: 10_000,
			StorageMicroUnitsPerGBSecond: 1_000, SharedAllocationMethod: "DEVICE_TIME",
			Provenance: provenance,
		},
	}
	workload := capacitysim.WorkloadTrace{
		SchemaVersion: 1, Revision: "trace-v1", Provenance: provenance,
		Records: []capacitysim.TraceRecord{
			arrival("trace-a", 0), arrival("trace-b", 3), arrival("trace-c", 8),
		},
	}
	calibration := capacitysim.CalibrationBundle{
		SchemaVersion: 1, Revision: "calibration-v1", Provenance: provenance,
		StageModels: []capacitysim.StageRuntimeModel{
			stageModel("encoder", "encoder-profile-v1", 10, 100, provenance),
			stageModel("dit", "dit-profile-v1", 20, 200, provenance),
			stageModel("future-llm", "llm-profile-v1", 30, 300, provenance),
		},
		ConnectorModels: []capacitysim.ConnectorModel{{
			Revision: "rdma-v1", SetupLatencyNS: 2, PayloadBytesPerSecond: 1_000_000_000,
			ConcurrencyLimit: 2, FailureRatePPM: 0, Provenance: provenance,
		}},
	}
	calibration.StageModels[2].GPUCount = 4
	return scenario, workload, calibration
}

func arrival(id string, offset int64) capacitysim.TraceRecord {
	return capacitysim.TraceRecord{
		RecordKind: "ARRIVAL", SchemaVersion: 1, TraceID: id,
		ArrivalOffsetNS: offset, OrganizationCohort: "org-small",
		ProjectCohort: "project-batch", ServiceClassRevision: "standard-v1",
		GenerationPresetRevision: "balanced-v1", OutputSpec: "video-1080p-v1",
		RequestCohort: "short", JobExpiryOffsetNS: 900,
		EligibleGraphRevision: "h3-graph-v1", CachePolicyRevision: "cache-v1",
		CacheKeyCohort: id,
	}
}

func stageModel(
	stageID, profile string,
	serviceNS, outputBytes int64,
	provenance capacitysim.Provenance,
) capacitysim.StageRuntimeModel {
	return capacitysim.StageRuntimeModel{
		StageID: stageID, ProfileRevision: profile, RequestCohort: "short",
		ServiceTime:         capacitysim.Distribution{P50: serviceNS, P95: serviceNS, P99: serviceNS, HardMax: serviceNS},
		OutputBytes:         capacitysim.Distribution{P50: outputBytes, P95: outputBytes, P99: outputBytes, HardMax: outputBytes},
		SealTime:            capacitysim.Distribution{P50: 1, P95: 1, P99: 1, HardMax: 1},
		MaterializationTime: capacitysim.Distribution{P50: 1, P95: 1, P99: 1, HardMax: 1},
		RecoveryTime:        capacitysim.Distribution{P50: 2, P95: 2, P99: 2, HardMax: 2},
		FailureRatePPM:      0, GPUCount: 1, CPUMilli: 100, MemoryBytes: 1024,
		Provenance: provenance,
	}
}
