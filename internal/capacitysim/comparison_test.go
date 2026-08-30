package capacitysim_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/capacitysim"
)

func TestReceiptPreservesInputAuthorityAndFixedPrice(t *testing.T) {
	scenario, workload, calibration := fixedPipelineFixture()
	receipt, err := capacitysim.Simulate(scenario, workload, calibration)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipt.InputEvidence) != 9 {
		t.Fatalf("input evidence = %#v, want scenario/trace/calibration/cost/pricing/3 stages/connector", receipt.InputEvidence)
	}
	for _, evidence := range receipt.InputEvidence {
		if evidence.SourceKind != "SYNTHETIC" || evidence.ContentDigest == "" ||
			evidence.ConfidencePPM != 900_000 || evidence.SampleCount != 100 {
			t.Fatalf("input authority classification was lost: %#v", evidence)
		}
	}
	if receipt.Completion.VisibleCompletions != 3 ||
		receipt.Completion.SuccessRatePPM != 1_000_000 ||
		receipt.Completion.ThroughputPerSecondPPM <= 0 {
		t.Fatalf("completion metrics = %#v", receipt.Completion)
	}
	if len(receipt.PriceComparisons) != 1 ||
		receipt.PriceComparisons[0].FixedCustomerPriceMicroUnits != 3_000_000 ||
		receipt.PriceComparisons[0].PricingSnapshotRevision != "price-v1" {
		t.Fatalf("fixed price comparison = %#v", receipt.PriceComparisons)
	}
	if receipt.DynamicETA.Status != "NOT_SUPPLIED" {
		t.Fatalf("Dynamic ETA without model = %#v", receipt.DynamicETA)
	}
}

func TestCompareReportsDeltasWithoutSelectingAPlan(t *testing.T) {
	scenario, workload, calibration := fixedPipelineFixture()
	baseline, err := capacitysim.Simulate(scenario, workload, calibration)
	if err != nil {
		t.Fatal(err)
	}
	candidateScenario := scenario
	candidateScenario.Revision = "scenario-v2"
	candidateScenario.Pools = append([]capacitysim.ResidentPool(nil), scenario.Pools...)
	candidateScenario.Pools[1].WorkerCount = 2
	candidate, err := capacitysim.Simulate(candidateScenario, workload, calibration)
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := capacitysim.Compare(baseline, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if comparison.BaselineReceiptDigest != baseline.ReceiptDigest ||
		comparison.CandidateReceiptDigest != candidate.ReceiptDigest ||
		len(comparison.Deltas) < 4 || len(comparison.SourceClassifications) == 0 {
		t.Fatalf("comparison = %#v", comparison)
	}
	encoded, err := capacitysim.EncodeComparison(comparison)
	if err != nil {
		t.Fatal(err)
	}
	lower := bytes.ToLower(encoded)
	if bytes.Contains(lower, []byte("recommend")) || bytes.Contains(lower, []byte("selected")) ||
		bytes.Contains(lower, []byte("winner")) {
		t.Fatalf("compare selected a plan: %s", encoded)
	}
}

func TestDecodeReceiptRejectsTampering(t *testing.T) {
	scenario, workload, calibration := fixedPipelineFixture()
	receipt, err := capacitysim.Simulate(scenario, workload, calibration)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := capacitysim.EncodeReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatal(err)
	}
	value["window_duration_ns"] = float64(999)
	tampered, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capacitysim.DecodeReceipt(tampered); err == nil ||
		!strings.Contains(err.Error(), "digest") {
		t.Fatalf("tampered receipt error = %v", err)
	}
}
