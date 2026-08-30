package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/vivym/vela/internal/capacitysim"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(arguments []string, stdout, stderr io.Writer) int {
	if len(arguments) == 0 {
		writeUsage(stderr)
		return 2
	}
	switch arguments[0] {
	case "validate":
		return runValidate(arguments[1:], stdout, stderr)
	case "run":
		return runSimulation(arguments[1:], stdout, stderr)
	case "compare":
		return runCompare(arguments[1:], stdout, stderr)
	default:
		writeUsage(stderr)
		return 2
	}
}

func runValidate(arguments []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	scenarioPath := flags.String("scenario", "", "scenario JSON")
	tracePath := flags.String("trace", "", "trace NDJSON")
	calibrationPath := flags.String("calibration", "", "calibration JSON")
	if flags.Parse(arguments) != nil || flags.NArg() != 0 ||
		*scenarioPath == "" || *tracePath == "" || *calibrationPath == "" {
		writeUsage(stderr)
		return 2
	}
	scenario, trace, calibration, err := loadInputs(*scenarioPath, *tracePath, *calibrationPath)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "validate capacity simulation inputs: %v\n", err)
		return 1
	}
	if err := capacitysim.Validate(scenario, trace, calibration); err != nil {
		_, _ = fmt.Fprintf(stderr, "validate capacity simulation inputs: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(
		stdout, "PASS scenario=%s trace=%s calibration=%s\n",
		scenario.Revision, trace.Revision, calibration.Revision,
	)
	return 0
}

func runSimulation(arguments []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	scenarioPath := flags.String("scenario", "", "scenario JSON")
	tracePath := flags.String("trace", "", "trace NDJSON")
	calibrationPath := flags.String("calibration", "", "calibration JSON")
	outputPath := flags.String("out", "", "SimulationReceipt JSON")
	proposalPath := flags.String("proposal-out", "", "advisory ResidencyProposal JSON")
	if flags.Parse(arguments) != nil || flags.NArg() != 0 ||
		*scenarioPath == "" || *tracePath == "" || *calibrationPath == "" ||
		*outputPath == "" {
		writeUsage(stderr)
		return 2
	}
	inputs := []string{*scenarioPath, *tracePath, *calibrationPath}
	outputs := []string{*outputPath}
	if *proposalPath != "" {
		outputs = append(outputs, *proposalPath)
	}
	if err := rejectInputOverwrite(inputs, outputs); err != nil {
		_, _ = fmt.Fprintf(stderr, "run capacity simulation: %v\n", err)
		return 1
	}
	scenario, trace, calibration, err := loadInputs(*scenarioPath, *tracePath, *calibrationPath)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "run capacity simulation: %v\n", err)
		return 1
	}
	receipt, err := capacitysim.Simulate(scenario, trace, calibration)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "run capacity simulation: %v\n", err)
		return 1
	}
	encodedReceipt, err := capacitysim.EncodeReceipt(receipt)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "run capacity simulation: encode receipt: %v\n", err)
		return 1
	}
	if err := writeAtomic(*outputPath, encodedReceipt); err != nil {
		_, _ = fmt.Fprintf(stderr, "run capacity simulation: write receipt: %v\n", err)
		return 1
	}
	if *proposalPath != "" {
		proposal, proposalErr := capacitysim.ProposeResidency(scenario, receipt)
		if proposalErr != nil {
			_, _ = fmt.Fprintf(stderr, "run capacity simulation: propose residency: %v\n", proposalErr)
			return 1
		}
		encodedProposal, encodeErr := capacitysim.EncodeProposal(proposal)
		if encodeErr != nil {
			_, _ = fmt.Fprintf(stderr, "run capacity simulation: encode proposal: %v\n", encodeErr)
			return 1
		}
		if err := writeAtomic(*proposalPath, encodedProposal); err != nil {
			_, _ = fmt.Fprintf(stderr, "run capacity simulation: write proposal: %v\n", err)
			return 1
		}
	}
	_, _ = fmt.Fprintf(stdout, "PASS receipt=%s output=%s\n", receipt.ReceiptDigest, *outputPath)
	return 0
}

func runCompare(arguments []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("compare", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	baselinePath := flags.String("baseline", "", "baseline SimulationReceipt JSON")
	candidatePath := flags.String("candidate", "", "candidate SimulationReceipt JSON")
	outputPath := flags.String("out", "", "ReceiptComparison JSON")
	if flags.Parse(arguments) != nil || flags.NArg() != 0 ||
		*baselinePath == "" || *candidatePath == "" || *outputPath == "" {
		writeUsage(stderr)
		return 2
	}
	if err := rejectInputOverwrite(
		[]string{*baselinePath, *candidatePath}, []string{*outputPath},
	); err != nil {
		_, _ = fmt.Fprintf(stderr, "compare capacity receipts: %v\n", err)
		return 1
	}
	baselineBytes, err := readBounded(*baselinePath)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "compare capacity receipts: read baseline: %v\n", err)
		return 1
	}
	candidateBytes, err := readBounded(*candidatePath)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "compare capacity receipts: read candidate: %v\n", err)
		return 1
	}
	baseline, err := capacitysim.DecodeReceipt(baselineBytes)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "compare capacity receipts: decode baseline: %v\n", err)
		return 1
	}
	candidate, err := capacitysim.DecodeReceipt(candidateBytes)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "compare capacity receipts: decode candidate: %v\n", err)
		return 1
	}
	comparison, err := capacitysim.Compare(baseline, candidate)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "compare capacity receipts: %v\n", err)
		return 1
	}
	encoded, err := capacitysim.EncodeComparison(comparison)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "compare capacity receipts: encode: %v\n", err)
		return 1
	}
	if err := writeAtomic(*outputPath, encoded); err != nil {
		_, _ = fmt.Fprintf(stderr, "compare capacity receipts: write: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(stdout, "PASS comparison=%s output=%s\n", comparison.ComparisonDigest, *outputPath)
	return 0
}

func loadInputs(
	scenarioPath string,
	tracePath string,
	calibrationPath string,
) (capacitysim.ScenarioRevision, capacitysim.WorkloadTrace, capacitysim.CalibrationBundle, error) {
	scenarioBytes, err := readBounded(scenarioPath)
	if err != nil {
		return capacitysim.ScenarioRevision{}, capacitysim.WorkloadTrace{}, capacitysim.CalibrationBundle{}, fmt.Errorf("read scenario: %w", err)
	}
	traceBytes, err := readBounded(tracePath)
	if err != nil {
		return capacitysim.ScenarioRevision{}, capacitysim.WorkloadTrace{}, capacitysim.CalibrationBundle{}, fmt.Errorf("read trace: %w", err)
	}
	calibrationBytes, err := readBounded(calibrationPath)
	if err != nil {
		return capacitysim.ScenarioRevision{}, capacitysim.WorkloadTrace{}, capacitysim.CalibrationBundle{}, fmt.Errorf("read calibration: %w", err)
	}
	scenario, err := capacitysim.DecodeScenario(scenarioBytes)
	if err != nil {
		return capacitysim.ScenarioRevision{}, capacitysim.WorkloadTrace{}, capacitysim.CalibrationBundle{}, err
	}
	trace, err := capacitysim.DecodeTraceNDJSON(traceBytes)
	if err != nil {
		return capacitysim.ScenarioRevision{}, capacitysim.WorkloadTrace{}, capacitysim.CalibrationBundle{}, err
	}
	calibration, err := capacitysim.DecodeCalibration(calibrationBytes)
	if err != nil {
		return capacitysim.ScenarioRevision{}, capacitysim.WorkloadTrace{}, capacitysim.CalibrationBundle{}, err
	}
	return scenario, trace, calibration, nil
}

func readBounded(path string) ([]byte, error) {
	information, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if information.Mode()&os.ModeSymlink != 0 || !information.Mode().IsRegular() ||
		information.Size() <= 0 || information.Size() > capacitysim.MaxInputBytes {
		return nil, fmt.Errorf("input must be a regular file in 1..%d bytes", capacitysim.MaxInputBytes)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	content, err := io.ReadAll(io.LimitReader(file, capacitysim.MaxInputBytes+1))
	if err != nil {
		return nil, err
	}
	if len(content) == 0 || len(content) > capacitysim.MaxInputBytes {
		return nil, fmt.Errorf("input changed while reading or exceeds %d bytes", capacitysim.MaxInputBytes)
	}
	return content, nil
}

func rejectInputOverwrite(inputs, outputs []string) error {
	canonicalInputs := make(map[string]bool, len(inputs))
	for _, path := range inputs {
		canonical, err := canonicalPath(path)
		if err != nil {
			return err
		}
		canonicalInputs[canonical] = true
	}
	seenOutputs := make(map[string]bool, len(outputs))
	for _, path := range outputs {
		canonical, err := canonicalPath(path)
		if err != nil {
			return err
		}
		if canonicalInputs[canonical] {
			return fmt.Errorf("output must not overwrite an input file")
		}
		if seenOutputs[canonical] {
			return errors.New("output paths must be distinct")
		}
		seenOutputs[canonical] = true
	}
	return nil
}

func canonicalPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err == nil {
		return resolved, nil
	}
	if !os.IsNotExist(err) {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(absolute)), nil
}

func writeAtomic(path string, content []byte) error {
	if len(content) == 0 || len(content) > capacitysim.MaxInputBytes {
		return errors.New("output size is invalid")
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".vela-capacity-sim-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(content); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}

func writeUsage(writer io.Writer) {
	_, _ = fmt.Fprintln(writer, "usage: vela-capacity-sim validate --scenario <scenario.json> --trace <trace.ndjson> --calibration <calibration.json>")
	_, _ = fmt.Fprintln(writer, "       vela-capacity-sim run --scenario <scenario.json> --trace <trace.ndjson> --calibration <calibration.json> --out <receipt.json> [--proposal-out <proposal.json>]")
	_, _ = fmt.Fprintln(writer, "       vela-capacity-sim compare --baseline <receipt.json> --candidate <receipt.json> --out <comparison.json>")
}
