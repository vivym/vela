package capacitysim

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

var boundedToken = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/+-]{0,199}$`)

func DecodeScenario(encoded []byte) (ScenarioRevision, error) {
	var scenario ScenarioRevision
	if err := decodeStrict(encoded, &scenario); err != nil {
		return ScenarioRevision{}, fmt.Errorf("decode scenario: %w", err)
	}
	if err := validateScenario(scenario); err != nil {
		return ScenarioRevision{}, err
	}
	return scenario, nil
}

func DecodeCalibration(encoded []byte) (CalibrationBundle, error) {
	var calibration CalibrationBundle
	if err := decodeStrict(encoded, &calibration); err != nil {
		return CalibrationBundle{}, fmt.Errorf("decode calibration: %w", err)
	}
	if err := validateCalibration(calibration); err != nil {
		return CalibrationBundle{}, err
	}
	return calibration, nil
}

func DecodeReceipt(encoded []byte) (SimulationReceipt, error) {
	var receipt SimulationReceipt
	if err := decodeStrict(encoded, &receipt); err != nil {
		return SimulationReceipt{}, fmt.Errorf("decode receipt: %w", err)
	}
	if err := validateReceipt(receipt); err != nil {
		return SimulationReceipt{}, err
	}
	return receipt, nil
}

func validateReceipt(receipt SimulationReceipt) error {
	if receipt.SchemaVersion != SchemaVersion || receipt.SimulatorRevision != AlgorithmRevision ||
		!validDigest(receipt.ReceiptDigest) {
		return errors.New("receipt identity is invalid")
	}
	want := receipt.ReceiptDigest
	receipt.ReceiptDigest = ""
	digest, err := digestValue(receipt)
	if err != nil || digest != want {
		return errors.New("receipt digest does not match content")
	}
	return nil
}

func DecodeTraceNDJSON(encoded []byte) (WorkloadTrace, error) {
	if len(encoded) == 0 || len(encoded) > MaxInputBytes {
		return WorkloadTrace{}, fmt.Errorf("trace bytes must be in 1..%d", MaxInputBytes)
	}
	scanner := bufio.NewScanner(bytes.NewReader(encoded))
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	var lines []TraceRecord
	for scanner.Scan() {
		if len(bytes.TrimSpace(scanner.Bytes())) == 0 {
			return WorkloadTrace{}, errors.New("trace contains an empty record")
		}
		var record TraceRecord
		if err := decodeStrict(scanner.Bytes(), &record); err != nil {
			return WorkloadTrace{}, fmt.Errorf("decode trace record %d: %w", len(lines)+1, err)
		}
		lines = append(lines, record)
		if len(lines) > MaxTraceRecords+1 {
			return WorkloadTrace{}, fmt.Errorf("trace exceeds %d records", MaxTraceRecords)
		}
	}
	if err := scanner.Err(); err != nil {
		return WorkloadTrace{}, fmt.Errorf("scan trace: %w", err)
	}
	if len(lines) < 2 || lines[0].RecordKind != "TRACE_HEADER" ||
		lines[0].Provenance == nil {
		return WorkloadTrace{}, errors.New("trace must start with a complete TRACE_HEADER")
	}
	trace := WorkloadTrace{
		SchemaVersion: lines[0].SchemaVersion,
		Revision:      lines[0].Revision,
		Provenance:    *lines[0].Provenance,
		Records:       append([]TraceRecord(nil), lines[1:]...),
	}
	if err := validateTrace(trace); err != nil {
		return WorkloadTrace{}, err
	}
	return trace, nil
}

func EncodeReceipt(receipt SimulationReceipt) ([]byte, error) {
	return json.Marshal(receipt)
}

func EncodeScenario(scenario ScenarioRevision) ([]byte, error) {
	return json.Marshal(scenario)
}

func EncodeCalibration(calibration CalibrationBundle) ([]byte, error) {
	return json.Marshal(calibration)
}

func EncodeTraceNDJSON(trace WorkloadTrace) ([]byte, error) {
	if err := validateTrace(trace); err != nil {
		return nil, err
	}
	var encoded bytes.Buffer
	header, err := json.Marshal(TraceRecord{
		RecordKind: "TRACE_HEADER", SchemaVersion: trace.SchemaVersion,
		Revision: trace.Revision, Provenance: &trace.Provenance,
	})
	if err != nil {
		return nil, err
	}
	encoded.Write(header)
	encoded.WriteByte('\n')
	for _, record := range trace.Records {
		line, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			return nil, marshalErr
		}
		encoded.Write(line)
		encoded.WriteByte('\n')
	}
	return encoded.Bytes(), nil
}

func EncodeProposal(proposal ResidencyProposal) ([]byte, error) {
	return json.Marshal(proposal)
}

func Validate(
	scenario ScenarioRevision,
	workload WorkloadTrace,
	calibration CalibrationBundle,
) error {
	if err := validateScenario(scenario); err != nil {
		return err
	}
	if err := validateTrace(workload); err != nil {
		return err
	}
	if err := validateCalibration(calibration); err != nil {
		return err
	}
	stageByID := make(map[string]StageSpec, len(scenario.Stages))
	for _, stage := range scenario.Stages {
		stageByID[stage.ID] = stage
	}
	connectors := make(map[string]bool, len(calibration.ConnectorModels))
	for _, connector := range calibration.ConnectorModels {
		connectors[connector.Revision] = true
	}
	for _, stage := range scenario.Stages {
		if len(stage.Dependencies) > 0 && !connectors[stage.ConnectorRevision] {
			return fmt.Errorf("stage %q has no exact connector model %q", stage.ID, stage.ConnectorRevision)
		}
	}
	models := make(map[string]bool, len(calibration.StageModels))
	for _, model := range calibration.StageModels {
		stage, ok := stageByID[model.StageID]
		if !ok {
			return fmt.Errorf("runtime model references unknown stage %q", model.StageID)
		}
		if stage.ProfileRevision != model.ProfileRevision ||
			stage.ResourceKind == "GPU" && model.GPUCount != stage.DeviceCount ||
			stage.ResourceKind == "CPU" && model.GPUCount != 0 {
			return fmt.Errorf(
				"runtime model %s/%s does not match the complete DeviceSet",
				model.StageID, model.ProfileRevision,
			)
		}
		models[model.StageID+"\x00"+model.ProfileRevision+"\x00"+model.RequestCohort] = true
	}
	pricing := make(map[string]bool, len(scenario.PricingSnapshots))
	for _, snapshot := range scenario.PricingSnapshots {
		pricing[pricingKey(
			snapshot.ServiceClassRevision,
			snapshot.GenerationPresetRevision,
			snapshot.OutputSpec,
		)] = true
	}
	for _, record := range workload.Records {
		if record.RecordKind == "ARRIVAL" {
			for _, stage := range stageByID {
				key := stage.ID + "\x00" + stage.ProfileRevision + "\x00" + record.RequestCohort
				if !models[key] {
					return fmt.Errorf(
						"trace %q has no exact stage/profile/cohort model for %s/%s/%s",
						record.TraceID, stage.ID, stage.ProfileRevision, record.RequestCohort,
					)
				}
			}
			if !pricing[pricingKey(
				record.ServiceClassRevision,
				record.GenerationPresetRevision,
				record.OutputSpec,
			)] {
				return fmt.Errorf("trace %q has no exact PricingSnapshot", record.TraceID)
			}
		}
		for _, observed := range record.ObservedStages {
			key := modelKey(observed.StageID, observed.ProfileRevision, observed.RequestCohort)
			if !models[key] {
				return fmt.Errorf("trace %q has no exact model for observed stage %s", record.TraceID, key)
			}
		}
	}
	return nil
}

func validateScenario(scenario ScenarioRevision) error {
	if scenario.SchemaVersion != SchemaVersion || !validToken(scenario.Revision) ||
		scenario.AlgorithmRevision != AlgorithmRevision || !validToken(scenario.GraphRevision) ||
		scenario.WindowDurationNS <= 0 || scenario.WindowDurationNS > 365*24*60*60*1_000_000_000 {
		return errors.New("scenario identity or window is invalid")
	}
	if err := validateProvenance(scenario.Provenance); err != nil {
		return fmt.Errorf("scenario provenance: %w", err)
	}
	if len(scenario.Stages) == 0 || len(scenario.Stages) > 128 ||
		len(scenario.Pools) == 0 || len(scenario.Pools) > 4096 {
		return errors.New("scenario stages or pools are unbounded")
	}
	limits := scenario.Limits
	if limits.MaxJobs <= 0 {
		return errors.New("limits.max_jobs must be bounded")
	}
	if limits.MaxEvents <= 0 {
		return errors.New("limits.max_events must be bounded")
	}
	if limits.MaxQueuePerStage <= 0 {
		return errors.New("limits.max_queue_per_stage must be bounded")
	}
	if limits.MaxBufferItems <= 0 || limits.MaxBufferBytes <= 0 {
		return errors.New("buffer count and bytes must be bounded")
	}
	if limits.MaxStorageBytes <= 0 {
		return errors.New("storage bytes must be bounded")
	}
	if limits.MaxCacheEntries <= 0 || limits.MaxCacheBytes <= 0 {
		return errors.New("cache count and bytes must be bounded")
	}
	if limits.MaxJobs > 10_000_000 || limits.MaxEvents > 100_000_000 ||
		limits.MaxQueuePerStage > 10_000_000 || limits.MaxBufferItems > 10_000_000 {
		return errors.New("scenario limits exceed simulator hard bounds")
	}
	policy := scenario.Policy
	if !validToken(policy.SchedulerRevision) || !validToken(policy.CachePolicyRevision) ||
		policy.MaxRetriesPerStage < 0 || policy.MaxRetriesPerStage > 100 ||
		policy.FinalizationDurationNS <= 0 || policy.ProposalCooldownNS <= 0 ||
		policy.ProposalExpiryNS <= policy.ProposalCooldownNS || policy.ReleaseHealthyResidency {
		return errors.New("scenario policy is invalid or permits healthy residency release")
	}
	if policy.CacheEnabled && policy.CacheTTLNS <= 0 {
		return errors.New("cache TTL must be bounded when cache is enabled")
	}
	if err := validateCostModel(scenario.CostModel); err != nil {
		return err
	}
	if len(scenario.PricingSnapshots) == 0 || len(scenario.PricingSnapshots) > 10_000 {
		return errors.New("pricing snapshot count is invalid")
	}
	pricingKeys := make(map[string]bool, len(scenario.PricingSnapshots))
	for _, snapshot := range scenario.PricingSnapshots {
		key := pricingKey(
			snapshot.ServiceClassRevision,
			snapshot.GenerationPresetRevision,
			snapshot.OutputSpec,
		)
		if !validToken(snapshot.Revision) || !validToken(snapshot.ServiceClassRevision) ||
			!validToken(snapshot.GenerationPresetRevision) || !validToken(snapshot.OutputSpec) ||
			snapshot.PriceMicroUnits < 0 || pricingKeys[key] {
			return fmt.Errorf("PricingSnapshot %q is invalid", snapshot.Revision)
		}
		if err := validateProvenance(snapshot.Provenance); err != nil {
			return fmt.Errorf("PricingSnapshot %q provenance: %w", snapshot.Revision, err)
		}
		pricingKeys[key] = true
	}
	stageByID := make(map[string]StageSpec, len(scenario.Stages))
	for _, stage := range scenario.Stages {
		if !validToken(stage.ID) || !validToken(stage.ProfileRevision) ||
			(stage.ResourceKind != "GPU" && stage.ResourceKind != "CPU") ||
			stage.DeviceCount <= 0 || stage.DeviceCount > 64 {
			return fmt.Errorf("stage %q is invalid", stage.ID)
		}
		if strings.EqualFold(stage.ID, "dit") && stage.DeviceCount != 1 {
			return errors.New("H3 DiT requires exactly one GPU per WorkerInstance")
		}
		if _, duplicate := stageByID[stage.ID]; duplicate {
			return fmt.Errorf("duplicate stage %q", stage.ID)
		}
		stageByID[stage.ID] = stage
	}
	for _, stage := range scenario.Stages {
		seen := make(map[string]bool, len(stage.Dependencies))
		for _, dependency := range stage.Dependencies {
			if dependency == stage.ID || seen[dependency] {
				return fmt.Errorf("stage %q has invalid dependency %q", stage.ID, dependency)
			}
			if _, ok := stageByID[dependency]; !ok {
				return fmt.Errorf("stage %q has unknown dependency %q", stage.ID, dependency)
			}
			seen[dependency] = true
		}
		if len(stage.Dependencies) > 0 && !validToken(stage.ConnectorRevision) {
			return fmt.Errorf("stage %q connector revision is invalid", stage.ID)
		}
	}
	if err := rejectCycle(scenario.Stages); err != nil {
		return err
	}
	poolsPerStage := make(map[string]int)
	poolIDs := make(map[string]bool)
	for _, pool := range scenario.Pools {
		stage, ok := stageByID[pool.StageID]
		if !validToken(pool.ID) || poolIDs[pool.ID] || !ok ||
			pool.ProfileRevision != stage.ProfileRevision || pool.DeviceCount != stage.DeviceCount ||
			pool.WorkerCount <= 0 || pool.WorkerCount > 100_000 ||
			!validToken(pool.NetworkDomain) || !validToken(pool.FaultDomain) ||
			!validToken(pool.ModelRevision) || pool.WarmReadyOffsetNS < 0 ||
			pool.MinCount <= 0 || pool.MinCount > pool.WorkerCount ||
			pool.WorkerCount > pool.MaxCount {
			return fmt.Errorf("resident pool %q is invalid", pool.ID)
		}
		poolIDs[pool.ID] = true
		poolsPerStage[pool.StageID]++
	}
	for stageID := range stageByID {
		if poolsPerStage[stageID] == 0 {
			return fmt.Errorf("stage %q has no resident pool", stageID)
		}
	}
	return nil
}

func validateTrace(trace WorkloadTrace) error {
	if trace.SchemaVersion != SchemaVersion || !validToken(trace.Revision) {
		return errors.New("trace identity is invalid")
	}
	if err := validateProvenance(trace.Provenance); err != nil {
		return fmt.Errorf("trace provenance: %w", err)
	}
	if len(trace.Records) == 0 || len(trace.Records) > MaxTraceRecords {
		return errors.New("trace record count is invalid")
	}
	seenArrivals := make(map[string]bool)
	for index, record := range trace.Records {
		if record.SchemaVersion != SchemaVersion || !validToken(record.TraceID) ||
			(record.RecordKind != "ARRIVAL" && record.RecordKind != "TERMINAL") {
			return fmt.Errorf("trace record %d identity is invalid", index)
		}
		switch record.RecordKind {
		case "ARRIVAL":
			if seenArrivals[record.TraceID] || record.ArrivalOffsetNS < 0 ||
				!validToken(record.OrganizationCohort) || !validToken(record.ProjectCohort) ||
				!validToken(record.ServiceClassRevision) ||
				!validToken(record.GenerationPresetRevision) || !validToken(record.OutputSpec) ||
				!validToken(record.RequestCohort) || record.JobExpiryOffsetNS <= record.ArrivalOffsetNS ||
				!validToken(record.EligibleGraphRevision) || !validToken(record.CachePolicyRevision) ||
				(record.CacheKeyCohort != "" && !validToken(record.CacheKeyCohort)) {
				return fmt.Errorf("arrival record %q is invalid", record.TraceID)
			}
			seenArrivals[record.TraceID] = true
		case "TERMINAL":
			if len(record.ObservedStages) == 0 && record.ObservedVisibleCompletionNS <= 0 {
				return fmt.Errorf("terminal record %q has no observations", record.TraceID)
			}
		}
		for _, observed := range record.ObservedStages {
			if !validToken(observed.StageID) || !validToken(observed.ProfileRevision) ||
				!validToken(observed.RequestCohort) || observed.ServiceNS <= 0 ||
				observed.QueueNS < 0 || observed.TransferNS < 0 ||
				observed.MaterializeNS < 0 || observed.OutputBytes < 0 {
				return fmt.Errorf("trace record %q has invalid stage observation", record.TraceID)
			}
		}
	}
	return nil
}

func validateCalibration(calibration CalibrationBundle) error {
	if calibration.SchemaVersion != SchemaVersion || !validToken(calibration.Revision) {
		return errors.New("calibration identity is invalid")
	}
	if err := validateProvenance(calibration.Provenance); err != nil {
		return fmt.Errorf("calibration provenance: %w", err)
	}
	if len(calibration.StageModels) == 0 || len(calibration.StageModels) > 100_000 ||
		len(calibration.ConnectorModels) > 10_000 {
		return errors.New("calibration model counts are invalid")
	}
	seenStages := make(map[string]bool)
	for _, model := range calibration.StageModels {
		key := model.StageID + "\x00" + model.ProfileRevision + "\x00" + model.RequestCohort
		if !validToken(model.StageID) || !validToken(model.ProfileRevision) ||
			!validToken(model.RequestCohort) || seenStages[key] ||
			!validDistribution(model.ServiceTime) || !validDistribution(model.OutputBytes) ||
			!validDistribution(model.SealTime) || !validDistribution(model.MaterializationTime) ||
			!validDistribution(model.RecoveryTime) || model.FailureRatePPM < 0 ||
			model.FailureRatePPM > 1_000_000 || model.GPUCount < 0 || model.GPUCount > 64 ||
			model.CPUMilli < 0 || model.MemoryBytes < 0 || model.ScratchBytes < 0 {
			return fmt.Errorf("stage runtime model %q is invalid", key)
		}
		if err := validateProvenance(model.Provenance); err != nil {
			return fmt.Errorf("stage runtime model %q provenance: %w", key, err)
		}
		seenStages[key] = true
	}
	seenConnectors := make(map[string]bool)
	for _, connector := range calibration.ConnectorModels {
		if !validToken(connector.Revision) || seenConnectors[connector.Revision] ||
			connector.SetupLatencyNS < 0 || connector.PayloadBytesPerSecond <= 0 ||
			connector.ConcurrencyLimit <= 0 || connector.ConcurrencyLimit > 1_000_000 ||
			connector.FailureRatePPM < 0 || connector.FailureRatePPM > 1_000_000 ||
			connector.ObjectReadMicroUnits < 0 || connector.ObjectWriteMicroUnits < 0 {
			return fmt.Errorf("connector model %q is invalid", connector.Revision)
		}
		if err := validateProvenance(connector.Provenance); err != nil {
			return fmt.Errorf("connector model %q provenance: %w", connector.Revision, err)
		}
		seenConnectors[connector.Revision] = true
	}
	return nil
}

func validateProvenance(provenance Provenance) error {
	switch provenance.SourceKind {
	case "MEASURED", "DERIVED", "ASSUMED", "SYNTHETIC":
	default:
		return errors.New("source_kind is invalid")
	}
	if !validToken(provenance.CollectionWindow) || !validToken(provenance.HardwareRevision) ||
		!validToken(provenance.RuntimeRevision) || !validToken(provenance.ModelRevision) ||
		!validToken(provenance.ProfileRevision) || !validToken(provenance.Units) ||
		(provenance.ConnectorRevision != "" && !validToken(provenance.ConnectorRevision)) ||
		provenance.SampleCount <= 0 || provenance.FreshnessOffsetNS <= 0 ||
		provenance.ConfidencePPM < 0 || provenance.ConfidencePPM > 1_000_000 ||
		!validDigest(provenance.ContentDigest) {
		return errors.New("provenance fields are invalid")
	}
	return nil
}

func validateCostModel(model CostModelRevision) error {
	if !validToken(model.Revision) || model.GPUMicroUnitsPerSecond < 0 ||
		model.CPUMicroUnitsPerSecond < 0 || model.NetworkMicroUnitsPerGB < 0 ||
		model.StorageMicroUnitsPerGBSecond < 0 || model.MemoryMicroUnitsPerGBSecond < 0 ||
		model.ScratchMicroUnitsPerGBSecond < 0 || !validToken(model.SharedAllocationMethod) {
		return errors.New("cost model is invalid")
	}
	if err := validateProvenance(model.Provenance); err != nil {
		return fmt.Errorf("cost model provenance: %w", err)
	}
	return nil
}

func validDistribution(distribution Distribution) bool {
	return distribution.P50 > 0 && distribution.P50 <= distribution.P95 &&
		distribution.P95 <= distribution.P99 && distribution.P99 <= distribution.HardMax
}

func rejectCycle(stages []StageSpec) error {
	dependencies := make(map[string][]string, len(stages))
	for _, stage := range stages {
		dependencies[stage.ID] = stage.Dependencies
	}
	state := make(map[string]uint8, len(stages))
	var visit func(string) error
	visit = func(stageID string) error {
		switch state[stageID] {
		case 1:
			return fmt.Errorf("stage graph contains a cycle at %q", stageID)
		case 2:
			return nil
		}
		state[stageID] = 1
		for _, dependency := range dependencies[stageID] {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		state[stageID] = 2
		return nil
	}
	ids := make([]string, 0, len(stages))
	for _, stage := range stages {
		ids = append(ids, stage.ID)
	}
	sort.Strings(ids)
	for _, stageID := range ids {
		if err := visit(stageID); err != nil {
			return err
		}
	}
	return nil
}

func decodeStrict(encoded []byte, destination any) error {
	if len(encoded) == 0 || len(encoded) > MaxInputBytes {
		return fmt.Errorf("input bytes must be in 1..%d", MaxInputBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("input contains multiple JSON values")
		}
		return err
	}
	return nil
}

func digestValue(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func validToken(value string) bool {
	return boundedToken.MatchString(value)
}

func pricingKey(serviceClass, preset, outputSpec string) string {
	return serviceClass + "\x00" + preset + "\x00" + outputSpec
}
