package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/capacitysim"
)

func TestRunValidateRunAndCompare(t *testing.T) {
	directory := t.TempDir()
	scenarioPath, tracePath, calibrationPath := writeCapacitySimulatorFixture(t, directory)

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"validate", "--scenario", scenarioPath, "--trace", tracePath,
		"--calibration", calibrationPath,
	}, &stdout, &stderr)
	if code != 0 || stderr.Len() != 0 || !strings.Contains(stdout.String(), "PASS") {
		t.Fatalf("validate = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}

	receiptA := filepath.Join(directory, "receipt-a.json")
	receiptB := filepath.Join(directory, "receipt-b.json")
	proposalA := filepath.Join(directory, "proposal-a.json")
	proposalB := filepath.Join(directory, "proposal-b.json")
	for _, output := range []struct {
		receipt  string
		proposal string
	}{{receiptA, proposalA}, {receiptB, proposalB}} {
		stdout.Reset()
		stderr.Reset()
		code = run([]string{
			"run", "--scenario", scenarioPath, "--trace", tracePath,
			"--calibration", calibrationPath, "--out", output.receipt,
			"--proposal-out", output.proposal,
		}, &stdout, &stderr)
		if code != 0 || stderr.Len() != 0 || !strings.Contains(stdout.String(), "PASS") {
			t.Fatalf("run = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
		}
		assertOwnerOnlyFile(t, output.receipt)
		assertOwnerOnlyFile(t, output.proposal)
	}
	firstReceipt, err := os.ReadFile(receiptA)
	if err != nil {
		t.Fatal(err)
	}
	secondReceipt, err := os.ReadFile(receiptB)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstReceipt, secondReceipt) {
		t.Fatal("identical input bytes produced different receipt bytes")
	}
	if _, err := capacitysim.DecodeReceipt(firstReceipt); err != nil {
		t.Fatalf("decode CLI receipt: %v", err)
	}
	firstProposal, err := os.ReadFile(proposalA)
	if err != nil {
		t.Fatal(err)
	}
	secondProposal, err := os.ReadFile(proposalB)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstProposal, secondProposal) {
		t.Fatal("identical input bytes produced different proposal bytes")
	}
	var proposal capacitysim.ResidencyProposal
	if err := json.Unmarshal(firstProposal, &proposal); err != nil || proposal.AutoApply {
		t.Fatalf("CLI proposal = %#v error=%v", proposal, err)
	}

	comparisonPath := filepath.Join(directory, "comparison.json")
	stdout.Reset()
	stderr.Reset()
	code = run([]string{
		"compare", "--baseline", receiptA, "--candidate", receiptB,
		"--out", comparisonPath,
	}, &stdout, &stderr)
	if code != 0 || stderr.Len() != 0 || !strings.Contains(stdout.String(), "PASS") {
		t.Fatalf("compare = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
	comparison, err := os.ReadFile(comparisonPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(bytes.ToLower(comparison), []byte("recommend")) {
		t.Fatalf("compare selected a plan: %s", comparison)
	}
}

func TestRunFailsClosedWithoutOverwritingInputs(t *testing.T) {
	directory := t.TempDir()
	scenarioPath, tracePath, calibrationPath := writeCapacitySimulatorFixture(t, directory)
	original, err := os.ReadFile(scenarioPath)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"run", "--scenario", scenarioPath, "--trace", tracePath,
		"--calibration", calibrationPath, "--out", scenarioPath,
	}, &stdout, &stderr)
	if code != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "input") {
		t.Fatalf("protected output = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
	after, err := os.ReadFile(scenarioPath)
	if err != nil || !bytes.Equal(original, after) {
		t.Fatalf("scenario was overwritten: error=%v", err)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"run", "--unknown"}, &stdout, &stderr); code != 2 ||
		stdout.Len() != 0 || !strings.Contains(stderr.String(), "usage:") {
		t.Fatalf("unknown flag = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestProductionDependencyClosureHasNoActuationAuthority(t *testing.T) {
	command := exec.Command(
		"go", "list", "-deps", "-f", "{{.ImportPath}}", "./cmd/vela-capacity-sim",
	)
	command.Dir = filepath.Join("..", "..")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("list production dependencies: %v\n%s", err, output)
	}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "k8s.io/") ||
			strings.HasPrefix(dependency, "sigs.k8s.io/") {
			t.Fatalf("capacity simulator acquired Kubernetes dependency %q", dependency)
		}
		if strings.HasPrefix(dependency, "github.com/vivym/vela/") &&
			dependency != "github.com/vivym/vela/internal/capacitysim" &&
			dependency != "github.com/vivym/vela/cmd/vela-capacity-sim" {
			t.Fatalf("capacity simulator acquired runtime/actuation dependency %q", dependency)
		}
	}
}

func TestCheckedInSyntheticH3ExampleRemainsExecutable(t *testing.T) {
	repositoryRoot := filepath.Join("..", "..")
	exampleRoot := filepath.Join(repositoryRoot, "examples", "capacitysim", "h3-synthetic")
	outputRoot := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"run",
		"--scenario", filepath.Join(exampleRoot, "scenario.json"),
		"--trace", filepath.Join(exampleRoot, "trace.ndjson"),
		"--calibration", filepath.Join(exampleRoot, "calibration.json"),
		"--out", filepath.Join(outputRoot, "receipt.json"),
		"--proposal-out", filepath.Join(outputRoot, "proposal.json"),
	}, &stdout, &stderr)
	if code != 0 || stderr.Len() != 0 || !strings.Contains(stdout.String(), "PASS") {
		t.Fatalf("synthetic H3 example = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
}

func writeCapacitySimulatorFixture(t *testing.T, directory string) (string, string, string) {
	t.Helper()
	provenance := capacitysim.Provenance{
		SourceKind: "SYNTHETIC", CollectionWindow: "cli-fixture",
		HardwareRevision: "gpu-v1", RuntimeRevision: "runtime-v1",
		ModelRevision: "model-v1", ProfileRevision: "profile-v1",
		Units: "ns-bytes", SampleCount: 10, FreshnessOffsetNS: 10_000,
		ConfidencePPM: 800_000, ContentDigest: strings.Repeat("a", 64),
	}
	scenario := capacitysim.ScenarioRevision{
		SchemaVersion: 1, Revision: "scenario-v1",
		AlgorithmRevision: capacitysim.AlgorithmRevision, Seed: 1,
		GraphRevision: "graph-v1", WindowDurationNS: 1_000,
		Provenance: provenance,
		Stages: []capacitysim.StageSpec{{
			ID: "encoder", ProfileRevision: "encoder-v1", ResourceKind: "GPU", DeviceCount: 1,
		}},
		Pools: []capacitysim.ResidentPool{{
			ID: "encoder-pool", StageID: "encoder", ProfileRevision: "encoder-v1",
			WorkerCount: 1, DeviceCount: 1, NetworkDomain: "rack-a", FaultDomain: "node-a",
			ModelRevision: "encoder-model-v1", MinCount: 1, MaxCount: 2,
		}},
		Limits: capacitysim.Limits{
			MaxJobs: 4, MaxEvents: 100, MaxQueuePerStage: 4,
			MaxBufferItems: 4, MaxBufferBytes: 1 << 20,
			MaxStorageBytes: 1 << 20, MaxCacheEntries: 4, MaxCacheBytes: 1 << 20,
		},
		Policy: capacitysim.Policy{
			SchedulerRevision: "scheduler-v1", CachePolicyRevision: "cache-v1",
			MaxRetriesPerStage: 1, CacheEnabled: true, CacheTTLNS: 1_000,
			FinalizationDurationNS: 5, ProposalCooldownNS: 100, ProposalExpiryNS: 500,
		},
		CostModel: capacitysim.CostModelRevision{
			Revision: "cost-v1", GPUMicroUnitsPerSecond: 1_000_000,
			SharedAllocationMethod: "DEVICE_TIME", Provenance: provenance,
		},
		PricingSnapshots: []capacitysim.PricingSnapshot{{
			Revision: "price-v1", ServiceClassRevision: "standard-v1",
			GenerationPresetRevision: "balanced-v1", OutputSpec: "video-v1",
			PriceMicroUnits: 100, Provenance: provenance,
		}},
	}
	trace := capacitysim.WorkloadTrace{
		SchemaVersion: 1, Revision: "trace-v1", Provenance: provenance,
		Records: []capacitysim.TraceRecord{{
			RecordKind: "ARRIVAL", SchemaVersion: 1, TraceID: "trace-1",
			OrganizationCohort: "org-1", ProjectCohort: "project-1",
			ServiceClassRevision: "standard-v1", GenerationPresetRevision: "balanced-v1",
			OutputSpec: "video-v1", RequestCohort: "short", JobExpiryOffsetNS: 900,
			EligibleGraphRevision: "graph-v1", CachePolicyRevision: "cache-v1",
			CacheKeyCohort: "key-1",
		}},
	}
	calibration := capacitysim.CalibrationBundle{
		SchemaVersion: 1, Revision: "calibration-v1", Provenance: provenance,
		StageModels: []capacitysim.StageRuntimeModel{{
			StageID: "encoder", ProfileRevision: "encoder-v1", RequestCohort: "short",
			ServiceTime:         capacitysim.Distribution{P50: 10, P95: 10, P99: 10, HardMax: 10},
			OutputBytes:         capacitysim.Distribution{P50: 100, P95: 100, P99: 100, HardMax: 100},
			SealTime:            capacitysim.Distribution{P50: 1, P95: 1, P99: 1, HardMax: 1},
			MaterializationTime: capacitysim.Distribution{P50: 1, P95: 1, P99: 1, HardMax: 1},
			RecoveryTime:        capacitysim.Distribution{P50: 1, P95: 1, P99: 1, HardMax: 1},
			GPUCount:            1, MemoryBytes: 1024, Provenance: provenance,
		}},
	}
	scenarioBytes, err := capacitysim.EncodeScenario(scenario)
	if err != nil {
		t.Fatal(err)
	}
	traceBytes, err := capacitysim.EncodeTraceNDJSON(trace)
	if err != nil {
		t.Fatal(err)
	}
	calibrationBytes, err := capacitysim.EncodeCalibration(calibration)
	if err != nil {
		t.Fatal(err)
	}
	scenarioPath := filepath.Join(directory, "scenario.json")
	tracePath := filepath.Join(directory, "trace.ndjson")
	calibrationPath := filepath.Join(directory, "calibration.json")
	for path, content := range map[string][]byte{
		scenarioPath: scenarioBytes, tracePath: traceBytes, calibrationPath: calibrationBytes,
	} {
		if err := os.WriteFile(path, content, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return scenarioPath, tracePath, calibrationPath
}

func assertOwnerOnlyFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("%s mode = %o, want 600", path, info.Mode().Perm())
	}
}
