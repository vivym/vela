package capacitysim

import (
	"container/heap"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
)

type jobRuntime struct {
	record                TraceRecord
	terminal              bool
	terminalKind          string
	finalizationScheduled bool
	completedStages       map[string]bool
	consumedDependencies  map[string]bool
	attempts              map[string]int
	outputs               map[string]*stageOutput
}

type stageOutput struct {
	bytes              int64
	remainingConsumers int
	cacheKey           string
	cached             bool
	fromCache          bool
	cachePinHeld       bool
}

type queuedStage struct {
	jobID      string
	stageID    string
	poolID     string
	readyAtNS  int64
	transferNS int64
	attempt    int
	retry      bool
}

type poolRuntime struct {
	spec           ResidentPool
	warm           bool
	workers        []bool
	busyUntil      []int64
	busyNS         int64
	loadActuations int
}

type connectorRuntime struct {
	model     ConnectorModel
	available []int64
}

type cacheEntry struct {
	key        string
	stageID    string
	bytes      int64
	expiresAt  int64
	lastUsedAt int64
	pins       int
}

type simulationState struct {
	scenario            ScenarioRevision
	workload            WorkloadTrace
	calibration         CalibrationBundle
	events              eventQueue
	nextSequence        uint64
	eventsScheduled     int
	eventsProcessed     int
	nowNS               int64
	jobs                map[string]*jobRuntime
	activeJobs          int
	stages              map[string]StageSpec
	dependents          map[string][]string
	leaves              map[string]bool
	pools               map[string]*poolRuntime
	poolsByStage        map[string][]string
	stageModels         map[string]StageRuntimeModel
	connectors          map[string]*connectorRuntime
	queues              map[string][]queuedStage
	cache               map[string]*cacheEntry
	stageMetrics        map[string]*stageMetricAccumulator
	endToEnd            []int64
	admissionReasons    map[string]int
	accepted            int
	rejected            int
	visible             int
	failed              int
	expired             int
	storageBytes        int64
	storageByteNS       big.Int
	lastStorageAt       int64
	cacheByteNS         big.Int
	lastCacheAt         int64
	bufferItems         int
	bufferBytes         int64
	bufferMetrics       BufferMetrics
	cacheMetrics        CacheMetrics
	failureMetrics      FailureMetrics
	transferBytes       big.Int
	transferScalePPM    int64
	directGPUCost       big.Int
	directCPUCost       big.Int
	memoryCost          big.Int
	scratchCost         big.Int
	retryCost           big.Int
	organizationService map[string]int64
	organizationMaxWait map[string]int64
	dropped             []InputDisposition
}

func Simulate(
	scenario ScenarioRevision,
	workload WorkloadTrace,
	calibration CalibrationBundle,
) (SimulationReceipt, error) {
	if err := Validate(scenario, workload, calibration); err != nil {
		return SimulationReceipt{}, err
	}
	receipt, err := simulateCore(scenario, workload, calibration, 1_000_000)
	if err != nil {
		return SimulationReceipt{}, err
	}
	receipt.CalibrationErrors = calibrationErrors(workload, receipt.Stages)
	for _, sensitivity := range []struct {
		name   string
		scale  int
		outage bool
	}{
		{name: "TRANSFER_0_5X", scale: 500_000},
		{name: "TRANSFER_1X", scale: 1_000_000},
		{name: "TRANSFER_2X", scale: 2_000_000},
		{name: "TRANSFER_OUTAGE", scale: 0, outage: true},
	} {
		candidate := calibration
		candidate.ConnectorModels = append([]ConnectorModel(nil), calibration.ConnectorModels...)
		for index := range candidate.ConnectorModels {
			candidate.ConnectorModels[index].Outage = sensitivity.outage
		}
		candidateReceipt, runErr := simulateCore(scenario, workload, candidate, int64(sensitivity.scale))
		if runErr != nil {
			return SimulationReceipt{}, fmt.Errorf("simulate %s sensitivity: %w", sensitivity.name, runErr)
		}
		receipt.TransferSensitivity = append(receipt.TransferSensitivity, SensitivityResult{
			Case: sensitivity.name, ThroughputScalePPM: sensitivity.scale,
			VisibleCompletions: candidateReceipt.Conservation.VisibleCompletions,
			Failed:             candidateReceipt.Conservation.Failed,
			Expired:            candidateReceipt.Conservation.Expired,
			LatencyP99NS:       candidateReceipt.Latency.P99,
		})
	}
	receipt.ReceiptDigest = ""
	digest, err := digestValue(receipt)
	if err != nil {
		return SimulationReceipt{}, fmt.Errorf("digest SimulationReceipt: %w", err)
	}
	receipt.ReceiptDigest = digest
	return receipt, nil
}

func simulateCore(
	scenario ScenarioRevision,
	workload WorkloadTrace,
	calibration CalibrationBundle,
	transferScalePPM int64,
) (SimulationReceipt, error) {
	state := newSimulationState(scenario, workload, calibration)
	state.transferScalePPM = transferScalePPM
	if err := state.initialize(); err != nil {
		return SimulationReceipt{}, err
	}
	for state.events.Len() > 0 {
		event := heap.Pop(&state.events).(simulationEvent)
		if event.timeNS > scenario.WindowDurationNS {
			break
		}
		state.nowNS = event.timeNS
		state.eventsProcessed++
		if state.eventsProcessed > scenario.Limits.MaxEvents {
			return SimulationReceipt{}, errors.New("event processing exceeded limits.max_events")
		}
		if err := state.process(event); err != nil {
			return SimulationReceipt{}, err
		}
	}
	state.accountStorageUntil(scenario.WindowDurationNS)
	state.accountCacheUntil(scenario.WindowDurationNS)
	return state.receipt()
}

func newSimulationState(
	scenario ScenarioRevision,
	workload WorkloadTrace,
	calibration CalibrationBundle,
) *simulationState {
	return &simulationState{
		scenario: scenario, workload: workload, calibration: calibration,
		events: initializeEventQueue(), jobs: make(map[string]*jobRuntime),
		stages: make(map[string]StageSpec), dependents: make(map[string][]string),
		leaves: make(map[string]bool), pools: make(map[string]*poolRuntime),
		poolsByStage: make(map[string][]string), stageModels: make(map[string]StageRuntimeModel),
		connectors: make(map[string]*connectorRuntime), queues: make(map[string][]queuedStage),
		cache: make(map[string]*cacheEntry), stageMetrics: make(map[string]*stageMetricAccumulator),
		admissionReasons: make(map[string]int), organizationService: make(map[string]int64),
		organizationMaxWait: make(map[string]int64),
	}
}

func (state *simulationState) initialize() error {
	for _, stage := range state.scenario.Stages {
		state.stages[stage.ID] = stage
		state.leaves[stage.ID] = true
		for _, dependency := range stage.Dependencies {
			state.dependents[dependency] = append(state.dependents[dependency], stage.ID)
			state.leaves[dependency] = false
		}
	}
	for stageID := range state.dependents {
		sort.Strings(state.dependents[stageID])
	}
	pools := append([]ResidentPool(nil), state.scenario.Pools...)
	sort.Slice(pools, func(left, right int) bool { return pools[left].ID < pools[right].ID })
	for _, pool := range pools {
		runtime := &poolRuntime{
			spec: pool, warm: pool.WarmReadyOffsetNS == 0,
			workers: make([]bool, pool.WorkerCount), busyUntil: make([]int64, pool.WorkerCount),
		}
		state.pools[pool.ID] = runtime
		state.poolsByStage[pool.StageID] = append(state.poolsByStage[pool.StageID], pool.ID)
		if pool.WarmReadyOffsetNS > 0 {
			if err := state.schedule(simulationEvent{
				timeNS: pool.WarmReadyOffsetNS, kind: eventResidencyReady,
				entityKey: "pool/" + pool.ID, poolID: pool.ID,
			}); err != nil {
				return err
			}
		}
	}
	for stageID := range state.poolsByStage {
		sort.Strings(state.poolsByStage[stageID])
	}
	for _, model := range state.calibration.StageModels {
		state.stageModels[modelKey(model.StageID, model.ProfileRevision, model.RequestCohort)] = model
	}
	for _, model := range state.calibration.ConnectorModels {
		state.connectors[model.Revision] = &connectorRuntime{
			model: model, available: make([]int64, model.ConcurrencyLimit),
		}
	}
	records := append([]TraceRecord(nil), state.workload.Records...)
	sort.SliceStable(records, func(left, right int) bool {
		if records[left].ArrivalOffsetNS != records[right].ArrivalOffsetNS {
			return records[left].ArrivalOffsetNS < records[right].ArrivalOffsetNS
		}
		return records[left].TraceID < records[right].TraceID
	})
	for _, record := range records {
		if record.RecordKind != "ARRIVAL" {
			continue
		}
		if record.ArrivalOffsetNS > state.scenario.WindowDurationNS {
			state.rejected++
			addReason(state.admissionReasons, "ARRIVAL_OUTSIDE_WINDOW")
			state.dropped = append(state.dropped, InputDisposition{
				TraceID: record.TraceID, Reason: "ARRIVAL_OUTSIDE_WINDOW",
			})
			continue
		}
		if err := state.schedule(simulationEvent{
			timeNS: record.ArrivalOffsetNS, kind: eventArrival,
			entityKey: "job/" + record.TraceID, jobID: record.TraceID,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (state *simulationState) schedule(event simulationEvent) error {
	state.eventsScheduled++
	if state.eventsScheduled > state.scenario.Limits.MaxEvents {
		return errors.New("event scheduling exceeded limits.max_events")
	}
	event.priority = priorityFor(event.kind)
	event.sequence = state.nextSequence
	state.nextSequence++
	heap.Push(&state.events, event)
	return nil
}

func (state *simulationState) process(event simulationEvent) error {
	switch event.kind {
	case eventArrival:
		return state.processArrival(event)
	case eventExpiry:
		state.processExpiry(event)
	case eventResidencyReady:
		return state.processResidencyReady(event)
	case eventTransferComplete, eventStageReady, eventRetryReady:
		return state.processStageReady(event)
	case eventStageComplete:
		return state.processStageComplete(event)
	case eventFinalizationComplete:
		state.processFinalization(event)
	}
	return nil
}

func (state *simulationState) processArrival(event simulationEvent) error {
	record, ok := state.arrival(event.jobID)
	if !ok {
		return fmt.Errorf("arrival %q disappeared", event.jobID)
	}
	if state.activeJobs >= state.scenario.Limits.MaxJobs ||
		record.EligibleGraphRevision != state.scenario.GraphRevision ||
		record.CachePolicyRevision != state.scenario.Policy.CachePolicyRevision {
		state.rejected++
		reason := "ACTIVE_JOB_LIMIT"
		if record.EligibleGraphRevision != state.scenario.GraphRevision {
			reason = "GRAPH_REVISION_INELIGIBLE"
		} else if record.CachePolicyRevision != state.scenario.Policy.CachePolicyRevision {
			reason = "CACHE_POLICY_INELIGIBLE"
		}
		addReason(state.admissionReasons, reason)
		return nil
	}
	job := &jobRuntime{
		record: record, completedStages: make(map[string]bool), attempts: make(map[string]int),
		outputs: make(map[string]*stageOutput), consumedDependencies: make(map[string]bool),
	}
	state.jobs[record.TraceID] = job
	state.accepted++
	state.activeJobs++
	if _, exists := state.organizationService[record.OrganizationCohort]; !exists {
		state.organizationService[record.OrganizationCohort] = 0
	}
	if err := state.schedule(simulationEvent{
		timeNS: record.JobExpiryOffsetNS, kind: eventExpiry,
		entityKey: "job/" + record.TraceID, jobID: record.TraceID,
	}); err != nil {
		return err
	}
	rootIDs := make([]string, 0)
	for _, stage := range state.scenario.Stages {
		if len(stage.Dependencies) == 0 {
			rootIDs = append(rootIDs, stage.ID)
		}
	}
	sort.Strings(rootIDs)
	for _, stageID := range rootIDs {
		poolID := state.choosePool(stageID, state.nowNS)
		if err := state.schedule(simulationEvent{
			timeNS: state.nowNS, kind: eventStageReady,
			entityKey: record.TraceID + "/" + stageID, jobID: record.TraceID,
			stageID: stageID, poolID: poolID, readyAtNS: state.nowNS,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (state *simulationState) processExpiry(event simulationEvent) {
	job := state.jobs[event.jobID]
	if job == nil || job.terminal {
		return
	}
	state.terminalize(job, "EXPIRED")
	state.expired++
	state.failureMetrics.ExpiredJobs++
}

func (state *simulationState) processResidencyReady(event simulationEvent) error {
	pool := state.pools[event.poolID]
	if pool == nil {
		return fmt.Errorf("residency pool %q disappeared", event.poolID)
	}
	pool.warm = true
	pool.loadActuations++
	return state.dispatch(pool.spec.StageID)
}

func (state *simulationState) processStageReady(event simulationEvent) error {
	job := state.jobs[event.jobID]
	if job == nil || job.terminal || job.completedStages[event.stageID] {
		return nil
	}
	stage := state.stages[event.stageID]
	model := state.stageModel(stage, job.record.RequestCohort)
	metric := state.metric(stage, job.record.RequestCohort)
	if state.scenario.Policy.CacheEnabled && job.record.CacheKeyCohort != "" && !event.failed {
		key := scopedCacheKey(job.record, event.stageID)
		if entry := state.cache[key]; entry != nil {
			if entry.expiresAt <= state.nowNS && entry.pins == 0 {
				state.evictCacheEntry(entry)
			} else if entry.expiresAt > state.nowNS {
				entry.pins++
				entry.lastUsedAt = state.nowNS
				state.cacheMetrics.Hits++
				state.cacheMetrics.Pins++
				state.cacheMetrics.AvoidedComputeCount++
				state.cacheMetrics.SavedServiceNS += model.ServiceTime.P50
				metric.cacheHits++
				metric.transfer = append(metric.transfer, event.transferNS)
				if state.bufferItems+1 > state.scenario.Limits.MaxBufferItems ||
					entry.bytes > state.scenario.Limits.MaxBufferBytes ||
					state.bufferBytes > state.scenario.Limits.MaxBufferBytes-entry.bytes {
					entry.pins--
					state.cacheMetrics.PinReleases++
					state.bufferMetrics.BackpressureEvents++
					state.terminalize(job, "BUFFER_LIMIT")
					state.failed++
					return nil
				}
				return state.completeStageFromCache(job, stage, entry)
			}
		}
		state.cacheMetrics.Misses++
	}
	queue := state.queues[event.stageID]
	if len(queue) >= state.scenario.Limits.MaxQueuePerStage {
		state.bufferMetrics.BackpressureEvents++
		state.terminalize(job, "STAGE_QUEUE_LIMIT")
		state.failed++
		return nil
	}
	queue = append(queue, queuedStage{
		jobID: event.jobID, stageID: event.stageID, poolID: event.poolID,
		readyAtNS: event.readyAtNS, transferNS: event.transferNS,
		attempt: event.attempt, retry: event.kind == eventRetryReady,
	})
	state.queues[event.stageID] = queue
	if len(queue) > metric.maximumQueueDepth {
		metric.maximumQueueDepth = len(queue)
	}
	return state.dispatch(event.stageID)
}

func (state *simulationState) dispatch(stageID string) error {
	for {
		queue := state.queues[stageID]
		if len(queue) == 0 {
			return nil
		}
		index := state.pickQueued(queue)
		candidate := queue[index]
		// Pool capacity is selected at dispatch because equal-time READY events
		// have not reserved a worker and may all name the same initially idle pool.
		pool := state.pools[state.choosePool(stageID, state.nowNS)]
		if pool == nil || !pool.warm {
			return nil
		}
		worker := firstFreeWorker(pool)
		if worker < 0 {
			return nil
		}
		queue = append(queue[:index], queue[index+1:]...)
		state.queues[stageID] = queue
		job := state.jobs[candidate.jobID]
		if job == nil || job.terminal || job.completedStages[stageID] {
			continue
		}
		state.consumeDependencies(job, stageID)
		stage := state.stages[stageID]
		model := state.stageModel(stage, job.record.RequestCohort)
		attempt := job.attempts[stageID] + 1
		job.attempts[stageID] = attempt
		service := sampleDistribution(model.ServiceTime, state.scenario.Seed,
			candidate.jobID+"/"+stageID+fmt.Sprintf("/%d/service", attempt))
		seal := sampleDistribution(model.SealTime, state.scenario.Seed,
			candidate.jobID+"/"+stageID+fmt.Sprintf("/%d/seal", attempt))
		materialize := sampleDistribution(model.MaterializationTime, state.scenario.Seed,
			candidate.jobID+"/"+stageID+fmt.Sprintf("/%d/materialize", attempt))
		outputBytes := sampleDistribution(model.OutputBytes, state.scenario.Seed,
			candidate.jobID+"/"+stageID+fmt.Sprintf("/%d/output", attempt))
		duration := boundedSum(service, seal, materialize)
		pool.workers[worker] = true
		pool.busyUntil[worker] = boundedSum(state.nowNS, duration)
		observedDuration := min(duration, state.scenario.WindowDurationNS-state.nowNS)
		pool.busyNS += observedDuration
		state.accountDirectCost(observedDuration, model)
		state.organizationService[job.record.OrganizationCohort] += min(service, observedDuration)
		metric := state.metric(stage, job.record.RequestCohort)
		metric.starts++
		metric.queue = append(metric.queue, state.nowNS-candidate.readyAtNS)
		metric.transfer = append(metric.transfer, candidate.transferNS)
		metric.service = append(metric.service, service)
		metric.materialization = append(metric.materialization, boundedSum(seal, materialize))
		metric.outputBytes = append(metric.outputBytes, outputBytes)
		if candidate.retry {
			metric.retries++
		}
		wait := state.nowNS - candidate.readyAtNS
		if wait > state.organizationMaxWait[job.record.OrganizationCohort] {
			state.organizationMaxWait[job.record.OrganizationCohort] = wait
		}
		failed := deterministicRate(
			model.FailureRatePPM, state.scenario.Seed,
			candidate.jobID+"/"+stageID+fmt.Sprintf("/%d/failure", attempt),
		)
		if err := state.schedule(simulationEvent{
			timeNS: boundedSum(state.nowNS, duration), kind: eventStageComplete,
			entityKey: candidate.jobID + "/" + stageID, jobID: candidate.jobID,
			stageID: stageID, poolID: pool.spec.ID, workerIndex: worker,
			attempt: attempt, serviceNS: service, materializeNS: boundedSum(seal, materialize),
			outputBytes: outputBytes, failed: failed,
		}); err != nil {
			return err
		}
	}
}

func (state *simulationState) processStageComplete(event simulationEvent) error {
	pool := state.pools[event.poolID]
	if pool == nil || event.workerIndex < 0 || event.workerIndex >= len(pool.workers) {
		return errors.New("stage completion references an invalid complete DeviceSet")
	}
	duration := boundedSum(event.serviceNS, event.materializeNS)
	pool.workers[event.workerIndex] = false
	job := state.jobs[event.jobID]
	stage := state.stages[event.stageID]
	model := StageRuntimeModel{}
	if job != nil {
		model = state.stageModel(stage, job.record.RequestCohort)
	}
	if job == nil || job.terminal {
		return state.dispatch(event.stageID)
	}
	metric := state.metric(stage, job.record.RequestCohort)
	if event.failed {
		metric.failures++
		state.failureMetrics.StageFailures++
		waste := duration * int64(pool.spec.DeviceCount)
		state.failureMetrics.RetryWasteDeviceNS += waste
		if stage.ResourceKind == "GPU" {
			addProduct(&state.retryCost, waste, state.scenario.CostModel.GPUMicroUnitsPerSecond, 1_000)
		} else {
			addProduct(&state.retryCost, duration, model.CPUMilli, state.scenario.CostModel.CPUMicroUnitsPerSecond)
		}
		if event.attempt <= state.scenario.Policy.MaxRetriesPerStage {
			recovery := sampleDistribution(model.RecoveryTime, state.scenario.Seed,
				event.jobID+"/"+event.stageID+fmt.Sprintf("/%d/recovery", event.attempt))
			if boundedSum(state.nowNS, recovery) < job.record.JobExpiryOffsetNS {
				state.failureMetrics.Retries++
				if err := state.schedule(simulationEvent{
					timeNS: boundedSum(state.nowNS, recovery), kind: eventRetryReady,
					entityKey: event.jobID + "/" + event.stageID,
					jobID:     event.jobID, stageID: event.stageID, poolID: event.poolID,
					readyAtNS: boundedSum(state.nowNS, recovery), attempt: event.attempt,
				}); err != nil {
					return err
				}
				return state.dispatch(event.stageID)
			}
		}
		state.terminalize(job, "STAGE_FAILURE")
		state.failed++
		return state.dispatch(event.stageID)
	}
	metric.seals++
	metric.completions++
	if err := state.materializeOutput(job, stage, event.outputBytes); err != nil {
		state.terminalize(job, "STORAGE_LIMIT")
		state.failed++
		state.bufferMetrics.StorageBlockedJobs++
		return state.dispatch(event.stageID)
	}
	job.completedStages[event.stageID] = true
	if err := state.advance(job, stage); err != nil {
		return err
	}
	return state.dispatch(event.stageID)
}

func (state *simulationState) completeStageFromCache(
	job *jobRuntime,
	stage StageSpec,
	entry *cacheEntry,
) error {
	job.completedStages[stage.ID] = true
	state.consumeDependencies(job, stage.ID)
	job.outputs[stage.ID] = &stageOutput{
		bytes: entry.bytes, remainingConsumers: max(1, len(state.dependents[stage.ID])),
		cacheKey: entry.key, cached: true, fromCache: true, cachePinHeld: true,
	}
	state.addBuffer(entry.bytes)
	return state.advance(job, stage)
}

func (state *simulationState) materializeOutput(
	job *jobRuntime,
	stage StageSpec,
	bytes int64,
) error {
	if bytes <= 0 || bytes > state.scenario.Limits.MaxBufferBytes ||
		bytes > state.scenario.Limits.MaxStorageBytes ||
		state.bufferItems+1 > state.scenario.Limits.MaxBufferItems ||
		state.bufferBytes > state.scenario.Limits.MaxBufferBytes-bytes ||
		state.storageBytes > state.scenario.Limits.MaxStorageBytes-bytes {
		return errors.New("buffer or storage reservation exceeded")
	}
	state.changeStorage(bytes)
	output := &stageOutput{
		bytes: bytes, remainingConsumers: max(1, len(state.dependents[stage.ID])),
	}
	state.addBuffer(bytes)
	if state.scenario.Policy.CacheEnabled && job.record.CacheKeyCohort != "" {
		key := scopedCacheKey(job.record, stage.ID)
		_, alreadyCached := state.cache[key]
		if state.admitCache(key, stage.ID, bytes) && !alreadyCached {
			output.cacheKey = key
			output.cached = true
			output.cachePinHeld = true
			state.cache[key].pins++
		}
	}
	job.outputs[stage.ID] = output
	return nil
}

func (state *simulationState) advance(job *jobRuntime, stage StageSpec) error {
	dependents := state.dependents[stage.ID]
	for _, dependentID := range dependents {
		dependent := state.stages[dependentID]
		ready := true
		var payloadBytes int64
		for _, dependency := range dependent.Dependencies {
			if !job.completedStages[dependency] {
				ready = false
				break
			}
			payloadBytes += job.outputs[dependency].bytes
		}
		if !ready {
			continue
		}
		poolID := state.choosePool(dependentID, state.nowNS)
		transferNS, failed := state.reserveTransfer(
			dependent.ConnectorRevision, payloadBytes,
			job.record.TraceID+"/"+dependentID,
		)
		if failed {
			state.terminalize(job, "CONNECTOR_FAILURE")
			state.failed++
			return nil
		}
		if err := state.schedule(simulationEvent{
			timeNS: boundedSum(state.nowNS, transferNS), kind: eventTransferComplete,
			entityKey: job.record.TraceID + "/" + dependentID,
			jobID:     job.record.TraceID, stageID: dependentID, poolID: poolID,
			readyAtNS: boundedSum(state.nowNS, transferNS), transferNS: transferNS,
		}); err != nil {
			return err
		}
	}
	if state.allLeavesComplete(job) && !job.finalizationScheduled {
		job.finalizationScheduled = true
		return state.schedule(simulationEvent{
			timeNS: boundedSum(state.nowNS, state.scenario.Policy.FinalizationDurationNS),
			kind:   eventFinalizationComplete, entityKey: "job/" + job.record.TraceID,
			jobID: job.record.TraceID,
		})
	}
	return nil
}

func (state *simulationState) processFinalization(event simulationEvent) {
	job := state.jobs[event.jobID]
	if job == nil || job.terminal || !state.allLeavesComplete(job) {
		return
	}
	state.terminalize(job, "VISIBLE_COMPLETION")
	state.visible++
	state.endToEnd = append(state.endToEnd, state.nowNS-job.record.ArrivalOffsetNS)
}

func (state *simulationState) reserveTransfer(revision string, bytes int64, key string) (int64, bool) {
	connector := state.connectors[revision]
	if connector == nil || connector.model.Outage ||
		deterministicRate(connector.model.FailureRatePPM, state.scenario.Seed, key+"/failure") {
		return 0, true
	}
	index := 0
	for candidate := 1; candidate < len(connector.available); candidate++ {
		if connector.available[candidate] < connector.available[index] {
			index = candidate
		}
	}
	start := state.nowNS
	if connector.available[index] > start {
		start = connector.available[index]
	}
	payloadNS := transferPayloadNS(bytes, connector.model.PayloadBytesPerSecond, state.transferScalePPM)
	duration := boundedSum(connector.model.SetupLatencyNS, payloadNS)
	connector.available[index] = boundedSum(start, duration)
	addProduct(&state.transferBytes, bytes)
	return connector.available[index] - state.nowNS, false
}

func (state *simulationState) choosePool(stageID string, at int64) string {
	poolIDs := state.poolsByStage[stageID]
	best := poolIDs[0]
	bestAt := max(at, earliestPoolAvailability(state.pools[best]))
	for _, poolID := range poolIDs[1:] {
		candidateAt := max(at, earliestPoolAvailability(state.pools[poolID]))
		if candidateAt < bestAt || candidateAt == bestAt && poolID < best {
			best, bestAt = poolID, candidateAt
		}
	}
	return best
}

func (state *simulationState) pickQueued(queue []queuedStage) int {
	best := 0
	for index := 1; index < len(queue); index++ {
		left, right := queue[index], queue[best]
		leftJob, rightJob := state.jobs[left.jobID], state.jobs[right.jobID]
		if left.retry != right.retry {
			if left.retry {
				best = index
			}
			continue
		}
		leftService := state.organizationService[leftJob.record.OrganizationCohort]
		rightService := state.organizationService[rightJob.record.OrganizationCohort]
		if leftService != rightService {
			if leftService < rightService {
				best = index
			}
			continue
		}
		leftKey := leftJob.record.OrganizationCohort + "/" + leftJob.record.ServiceClassRevision +
			"/" + leftJob.record.ProjectCohort + "/" + left.jobID
		rightKey := rightJob.record.OrganizationCohort + "/" + rightJob.record.ServiceClassRevision +
			"/" + rightJob.record.ProjectCohort + "/" + right.jobID
		if left.readyAtNS < right.readyAtNS || left.readyAtNS == right.readyAtNS && leftKey < rightKey {
			best = index
		}
	}
	return best
}

func (state *simulationState) consumeDependencies(job *jobRuntime, stageID string) {
	if job.consumedDependencies[stageID] {
		return
	}
	job.consumedDependencies[stageID] = true
	for _, dependency := range state.stages[stageID].Dependencies {
		output := job.outputs[dependency]
		if output == nil || output.remainingConsumers <= 0 {
			continue
		}
		output.remainingConsumers--
		if output.remainingConsumers == 0 {
			state.removeBuffer(output.bytes)
		}
	}
}

func (state *simulationState) terminalize(job *jobRuntime, kind string) {
	if job.terminal {
		return
	}
	job.terminal = true
	job.terminalKind = kind
	state.activeJobs--
	for stageID, queue := range state.queues {
		live := queue[:0]
		for _, candidate := range queue {
			if candidate.jobID != job.record.TraceID {
				live = append(live, candidate)
			} else {
				cohort := job.record.OrganizationCohort
				state.organizationMaxWait[cohort] = max(state.organizationMaxWait[cohort], state.nowNS-candidate.readyAtNS)
			}
		}
		state.queues[stageID] = live
	}
	for _, output := range job.outputs {
		if output.remainingConsumers > 0 {
			state.removeBuffer(output.bytes)
			output.remainingConsumers = 0
		}
		state.releaseCachePin(output)
		if !output.cached && !output.fromCache {
			state.changeStorage(-output.bytes)
		}
	}
}

func (state *simulationState) releaseCachePin(output *stageOutput) {
	if output == nil || output.cacheKey == "" || !output.cachePinHeld {
		return
	}
	entry := state.cache[output.cacheKey]
	if entry != nil && entry.pins > 0 {
		entry.pins--
		if output.fromCache {
			state.cacheMetrics.PinReleases++
		}
	}
	output.cachePinHeld = false
}

func (state *simulationState) admitCache(key, stageID string, bytes int64) bool {
	if existing := state.cache[key]; existing != nil {
		existing.expiresAt = boundedSum(state.nowNS, state.scenario.Policy.CacheTTLNS)
		existing.lastUsedAt = state.nowNS
		return true
	}
	for len(state.cache)+1 > state.scenario.Limits.MaxCacheEntries ||
		state.cacheMetrics.Bytes > state.scenario.Limits.MaxCacheBytes-bytes {
		entry := state.oldestEvictableCacheEntry()
		if entry == nil {
			return false
		}
		state.evictCacheEntry(entry)
	}
	entry := &cacheEntry{
		key: key, stageID: stageID, bytes: bytes,
		expiresAt:  boundedSum(state.nowNS, state.scenario.Policy.CacheTTLNS),
		lastUsedAt: state.nowNS,
	}
	state.accountCacheUntil(state.nowNS)
	state.cache[key] = entry
	state.cacheMetrics.Entries = len(state.cache)
	state.cacheMetrics.Bytes += bytes
	return true
}

func (state *simulationState) oldestEvictableCacheEntry() *cacheEntry {
	var oldest *cacheEntry
	for _, entry := range state.cache {
		if entry.pins > 0 {
			continue
		}
		if oldest == nil || entry.lastUsedAt < oldest.lastUsedAt ||
			entry.lastUsedAt == oldest.lastUsedAt && entry.key < oldest.key {
			oldest = entry
		}
	}
	return oldest
}

func (state *simulationState) evictCacheEntry(entry *cacheEntry) {
	if entry == nil || entry.pins > 0 {
		return
	}
	state.accountCacheUntil(state.nowNS)
	delete(state.cache, entry.key)
	state.cacheMetrics.Evictions++
	state.cacheMetrics.Entries = len(state.cache)
	state.cacheMetrics.Bytes -= entry.bytes
	state.changeStorage(-entry.bytes)
}

func (state *simulationState) addBuffer(bytes int64) {
	state.bufferItems++
	state.bufferBytes += bytes
	if state.bufferItems > state.bufferMetrics.PeakItems {
		state.bufferMetrics.PeakItems = state.bufferItems
	}
	if state.bufferBytes > state.bufferMetrics.PeakBytes {
		state.bufferMetrics.PeakBytes = state.bufferBytes
	}
}

func (state *simulationState) removeBuffer(bytes int64) {
	state.bufferItems--
	state.bufferBytes -= bytes
}

func (state *simulationState) changeStorage(delta int64) {
	state.accountStorageUntil(state.nowNS)
	state.storageBytes += delta
	if state.storageBytes > state.bufferMetrics.PeakStorageBytes {
		state.bufferMetrics.PeakStorageBytes = state.storageBytes
	}
}

func (state *simulationState) accountStorageUntil(at int64) {
	if at <= state.lastStorageAt {
		return
	}
	duration := at - state.lastStorageAt
	addProduct(&state.storageByteNS, state.storageBytes, duration)
	state.lastStorageAt = at
}

func (state *simulationState) accountCacheUntil(at int64) {
	if at <= state.lastCacheAt {
		return
	}
	duration := at - state.lastCacheAt
	addProduct(&state.cacheByteNS, state.cacheMetrics.Bytes, duration)
	state.lastCacheAt = at
}

func (state *simulationState) accountDirectCost(
	duration int64,
	model StageRuntimeModel,
) {
	addProduct(&state.directGPUCost, duration, int64(model.GPUCount), state.scenario.CostModel.GPUMicroUnitsPerSecond)
	addProduct(&state.directCPUCost, duration, model.CPUMilli, state.scenario.CostModel.CPUMicroUnitsPerSecond)
	addProduct(&state.memoryCost, duration, model.MemoryBytes, state.scenario.CostModel.MemoryMicroUnitsPerGBSecond)
	addProduct(&state.scratchCost, duration, model.ScratchBytes, state.scenario.CostModel.ScratchMicroUnitsPerGBSecond)
}

func (state *simulationState) receipt() (SimulationReceipt, error) {
	scenarioDigest, err := digestValue(state.scenario)
	if err != nil {
		return SimulationReceipt{}, err
	}
	traceDigest, err := digestValue(state.workload)
	if err != nil {
		return SimulationReceipt{}, err
	}
	calibrationDigest, err := digestValue(state.calibration)
	if err != nil {
		return SimulationReceipt{}, err
	}
	unfinished := 0
	for _, job := range state.jobs {
		if !job.terminal {
			unfinished++
		}
	}
	arrivals := state.accepted + state.rejected
	conservation := ConservationChecks{
		Arrivals: arrivals, Accepted: state.accepted, Rejected: state.rejected,
		VisibleCompletions: state.visible, Failed: state.failed, Expired: state.expired,
		Unfinished: unfinished, EventsProcessed: state.eventsProcessed,
		StorageBytes: state.storageBytes,
	}
	conservation.Valid = conservation.Arrivals == conservation.Accepted+conservation.Rejected &&
		conservation.Accepted == conservation.VisibleCompletions+conservation.Failed+
			conservation.Expired+conservation.Unfinished && state.bufferItems >= 0 &&
		state.bufferBytes >= 0 && state.storageBytes >= 0 &&
		state.eventsProcessed <= state.scenario.Limits.MaxEvents && state.storageConserved()
	if !conservation.Valid {
		return SimulationReceipt{}, errors.New("simulation violated job, storage, buffer, or pin conservation")
	}
	stageMetrics := make([]StageMetrics, 0, len(state.stageMetrics))
	for _, metric := range state.stageMetrics {
		stageMetrics = append(stageMetrics, metric.receipt())
	}
	sort.Slice(stageMetrics, func(left, right int) bool {
		leftKey := modelKey(stageMetrics[left].StageID, stageMetrics[left].ProfileRevision, stageMetrics[left].RequestCohort)
		rightKey := modelKey(stageMetrics[right].StageID, stageMetrics[right].ProfileRevision, stageMetrics[right].RequestCohort)
		return leftKey < rightKey
	})
	poolMetrics, sharedCost := state.poolReceipts()
	transferCost := multiplyQuantityDivide(
		&state.transferBytes, state.scenario.CostModel.NetworkMicroUnitsPerGB, 1_000_000_000,
	)
	storageCost := multiplyQuantityDivide(
		&state.storageByteNS, state.scenario.CostModel.StorageMicroUnitsPerGBSecond,
		1_000_000_000_000_000_000,
	)
	cacheCost := multiplyQuantityDivide(
		&state.cacheByteNS, state.scenario.CostModel.StorageMicroUnitsPerGBSecond,
		1_000_000_000_000_000_000,
	)
	gpuCost := divideQuantity(&state.directGPUCost, 1_000_000_000)
	cpuCost := divideQuantity(&state.directCPUCost, 1_000_000_000_000)
	memoryCost := divideQuantity(&state.memoryCost, 1_000_000_000_000_000_000)
	scratchCost := divideQuantity(&state.scratchCost, 1_000_000_000_000_000_000)
	retryCost := divideQuantity(&state.retryCost, 1_000_000_000_000)
	directStageCost := boundedSum(gpuCost, cpuCost, memoryCost, scratchCost)
	totalCost := boundedSum(directStageCost, sharedCost, transferCost, storageCost)
	if totalCost == math.MaxInt64 || retryCost == math.MaxInt64 || cacheCost == math.MaxInt64 {
		return SimulationReceipt{}, errors.New("simulated cost exceeds the int64 receipt range")
	}
	cost := CostMetrics{
		DirectGPUMicroUnits:       gpuCost,
		DirectCPUMicroUnits:       cpuCost,
		MemoryMicroUnits:          memoryCost,
		ScratchMicroUnits:         scratchCost,
		DirectStageMicroUnits:     directStageCost,
		SharedResidencyMicroUnits: sharedCost,
		TransferMicroUnits:        transferCost, StorageMicroUnits: storageCost,
		RetryWasteMicroUnits: retryCost, CacheMicroUnits: cacheCost,
		TotalMicroUnits: totalCost,
	}
	if state.visible > 0 {
		cost.PerVisibleCompletionMicros = totalCost / int64(state.visible)
	}
	state.cacheMetrics.Entries = len(state.cache)
	completion := CompletionMetrics{VisibleCompletions: state.visible}
	if state.accepted > 0 {
		completion.SuccessRatePPM = state.visible * 1_000_000 / state.accepted
	}
	completion.ThroughputPerSecondPPM = multiplyDivide(
		int64(state.visible), 1_000_000_000_000_000, state.scenario.WindowDurationNS,
	)
	if completion.ThroughputPerSecondPPM == math.MaxInt64 {
		return SimulationReceipt{}, errors.New("simulated throughput exceeds the int64 receipt range")
	}
	receipt := SimulationReceipt{
		SchemaVersion: SchemaVersion, SimulatorRevision: AlgorithmRevision,
		Seed: state.scenario.Seed, ScenarioDigest: scenarioDigest, TraceDigest: traceDigest,
		CalibrationDigest: calibrationDigest, WindowDurationNS: state.scenario.WindowDurationNS,
		InputEvidence: state.inputEvidence(), Validation: ValidationResult{Valid: true, UnsupportedInputs: []string{
			"PRODUCTION_SCHEDULER_DECISION_REPLAY", "DOMAIN_DEPENDENT_PLACEMENT",
			"CORRELATED_NODE_FAILURES", "OBJECT_OPERATION_COST", "WARMUP_RESOURCE_COST",
		}},
		Admission: AdmissionMetrics{
			Accepted: state.accepted, Rejected: state.rejected,
			ReasonCounts: reasonCounts(state.admissionReasons),
		},
		Completion: completion, Conservation: conservation,
		Latency: durationStats(state.endToEnd), DynamicETA: DynamicETAError{Status: "NOT_SUPPLIED"},
		Stages: stageMetrics, Pools: poolMetrics, Buffers: state.bufferMetrics,
		Cache: state.cacheMetrics, Failures: state.failureMetrics, Cost: cost,
		PriceComparisons: state.priceComparisons(totalCost),
		Fairness:         state.fairnessReceipts(), DroppedInputs: state.dropped,
	}
	return receipt, nil
}

func (state *simulationState) storageConserved() bool {
	var expectedStorage, expectedBufferBytes int64
	var expectedBufferItems int
	expectedPins := make(map[string]int)
	for key, entry := range state.cache {
		expectedStorage = boundedSum(expectedStorage, entry.bytes)
		expectedPins[key] = 0
	}
	if expectedStorage != state.cacheMetrics.Bytes {
		return false
	}
	for _, job := range state.jobs {
		if job.terminal {
			continue
		}
		for _, output := range job.outputs {
			if !output.cached && !output.fromCache {
				expectedStorage = boundedSum(expectedStorage, output.bytes)
			}
			if output.remainingConsumers > 0 {
				expectedBufferItems++
				expectedBufferBytes = boundedSum(expectedBufferBytes, output.bytes)
			}
			if output.cachePinHeld {
				if state.cache[output.cacheKey] == nil {
					return false
				}
				expectedPins[output.cacheKey]++
			}
		}
	}
	for key, expected := range expectedPins {
		if state.cache[key].pins != expected {
			return false
		}
	}
	return expectedStorage == state.storageBytes && expectedBufferBytes == state.bufferBytes &&
		expectedBufferItems == state.bufferItems
}

func (state *simulationState) poolReceipts() ([]PoolMetrics, int64) {
	ids := make([]string, 0, len(state.pools))
	for poolID := range state.pools {
		ids = append(ids, poolID)
	}
	sort.Strings(ids)
	result := make([]PoolMetrics, 0, len(ids))
	var sharedCost big.Int
	for _, poolID := range ids {
		pool := state.pools[poolID]
		warmup := min(pool.spec.WarmReadyOffsetNS, state.scenario.WindowDurationNS) * int64(pool.spec.WorkerCount)
		residencyPerWorker := state.scenario.WindowDurationNS - min(pool.spec.WarmReadyOffsetNS, state.scenario.WindowDurationNS)
		residency := residencyPerWorker * int64(pool.spec.WorkerCount)
		idle := residency - pool.busyNS
		if idle < 0 {
			idle = 0
		}
		busyDeviceNS := pool.busyNS * int64(pool.spec.DeviceCount)
		idleDeviceNS := idle * int64(pool.spec.DeviceCount)
		if state.stages[pool.spec.StageID].ResourceKind == "GPU" {
			addProduct(&sharedCost, idleDeviceNS, state.scenario.CostModel.GPUMicroUnitsPerSecond)
		} else {
			addProduct(&sharedCost, idle, state.scenario.CostModel.CPUMicroUnitsPerSecond)
		}
		result = append(result, PoolMetrics{
			PoolID: poolID, StageID: pool.spec.StageID,
			WorkerCount: pool.spec.WorkerCount, DeviceCount: pool.spec.DeviceCount,
			BusyNS: pool.busyNS, BusyDeviceNS: busyDeviceNS, IdleNS: idle,
			WarmupNS: warmup, ResidencyNS: residency,
			LoadActuations: pool.loadActuations, ReleaseActuations: 0,
		})
	}
	return result, divideQuantity(&sharedCost, 1_000_000_000)
}

func (state *simulationState) fairnessReceipts() []FairnessMetrics {
	for _, queue := range state.queues {
		for _, candidate := range queue {
			cohort := state.jobs[candidate.jobID].record.OrganizationCohort
			state.organizationMaxWait[cohort] = max(state.organizationMaxWait[cohort], state.scenario.WindowDurationNS-candidate.readyAtNS)
		}
	}
	cohorts := make([]string, 0, len(state.organizationService))
	var total int64
	for cohort, service := range state.organizationService {
		cohorts = append(cohorts, cohort)
		total += service
	}
	sort.Strings(cohorts)
	result := make([]FairnessMetrics, 0, len(cohorts))
	for _, cohort := range cohorts {
		shareError := 0
		if total > 0 && len(cohorts) > 0 {
			attained := multiplyDivide(state.organizationService[cohort], 1_000_000, total)
			target := int64(1_000_000 / len(cohorts))
			difference := attained - target
			if difference < 0 {
				difference = -difference
			}
			shareError = int(difference)
		}
		result = append(result, FairnessMetrics{
			OrganizationCohort: cohort, AttainedServiceNS: state.organizationService[cohort],
			ShareErrorPPM: shareError, MaximumStarvationNS: state.organizationMaxWait[cohort],
		})
	}
	return result
}

func (state *simulationState) inputEvidence() []InputEvidence {
	type source struct {
		path       string
		provenance Provenance
	}
	sources := []source{
		{path: "scenario", provenance: state.scenario.Provenance},
		{path: "trace", provenance: state.workload.Provenance},
		{path: "calibration", provenance: state.calibration.Provenance},
		{path: "cost_model/" + state.scenario.CostModel.Revision, provenance: state.scenario.CostModel.Provenance},
	}
	for _, snapshot := range state.scenario.PricingSnapshots {
		sources = append(sources, source{
			path: "pricing_snapshots/" + snapshot.Revision, provenance: snapshot.Provenance,
		})
	}
	for _, model := range state.calibration.StageModels {
		sources = append(sources, source{
			path:       "stage_models/" + model.StageID + "/" + model.ProfileRevision + "/" + model.RequestCohort,
			provenance: model.Provenance,
		})
	}
	for _, connector := range state.calibration.ConnectorModels {
		sources = append(sources, source{
			path: "connector_models/" + connector.Revision, provenance: connector.Provenance,
		})
	}
	sort.Slice(sources, func(left, right int) bool { return sources[left].path < sources[right].path })
	result := make([]InputEvidence, 0, len(sources))
	for _, source := range sources {
		result = append(result, InputEvidence{
			Path: source.path, SourceKind: source.provenance.SourceKind,
			CollectionWindow: source.provenance.CollectionWindow,
			Units:            source.provenance.Units, SampleCount: source.provenance.SampleCount,
			FreshnessOffsetNS: source.provenance.FreshnessOffsetNS,
			ConfidencePPM:     source.provenance.ConfidencePPM,
			ContentDigest:     source.provenance.ContentDigest,
		})
	}
	return result
}

func (state *simulationState) priceComparisons(totalCost int64) []PriceComparison {
	snapshots := append([]PricingSnapshot(nil), state.scenario.PricingSnapshots...)
	sort.Slice(snapshots, func(left, right int) bool { return snapshots[left].Revision < snapshots[right].Revision })
	result := make([]PriceComparison, 0, len(snapshots))
	for _, snapshot := range snapshots {
		count := 0
		for _, job := range state.jobs {
			if job.record.ServiceClassRevision == snapshot.ServiceClassRevision &&
				job.record.GenerationPresetRevision == snapshot.GenerationPresetRevision &&
				job.record.OutputSpec == snapshot.OutputSpec {
				count++
			}
		}
		if count == 0 {
			continue
		}
		allocated := int64(0)
		if state.accepted > 0 {
			allocated = multiplyDivide(totalCost, int64(count), int64(state.accepted))
		}
		result = append(result, PriceComparison{
			PricingSnapshotRevision:  snapshot.Revision,
			ServiceClassRevision:     snapshot.ServiceClassRevision,
			GenerationPresetRevision: snapshot.GenerationPresetRevision,
			OutputSpec:               snapshot.OutputSpec, JobCount: count,
			FixedCustomerPriceMicroUnits: multiplyDivide(snapshot.PriceMicroUnits, int64(count), 1),
			AllocatedInternalMicroUnits:  allocated,
		})
	}
	return result
}

func (state *simulationState) metric(stage StageSpec, cohort string) *stageMetricAccumulator {
	key := modelKey(stage.ID, stage.ProfileRevision, cohort)
	metric := state.stageMetrics[key]
	if metric == nil {
		metric = &stageMetricAccumulator{
			stageID: stage.ID, profileRevision: stage.ProfileRevision, requestCohort: cohort,
		}
		state.stageMetrics[key] = metric
	}
	return metric
}

func (state *simulationState) stageModel(stage StageSpec, cohort string) StageRuntimeModel {
	return state.stageModels[modelKey(stage.ID, stage.ProfileRevision, cohort)]
}

func (state *simulationState) allLeavesComplete(job *jobRuntime) bool {
	for stageID, leaf := range state.leaves {
		if leaf && !job.completedStages[stageID] {
			return false
		}
	}
	return true
}

func (state *simulationState) arrival(traceID string) (TraceRecord, bool) {
	for _, record := range state.workload.Records {
		if record.RecordKind == "ARRIVAL" && record.TraceID == traceID {
			return record, true
		}
	}
	return TraceRecord{}, false
}

func calibrationErrors(workload WorkloadTrace, stages []StageMetrics) []CalibrationError {
	observed := make(map[string][]int64)
	identities := make(map[string]ObservedStageTiming)
	for _, record := range workload.Records {
		for _, stage := range record.ObservedStages {
			key := modelKey(stage.StageID, stage.ProfileRevision, stage.RequestCohort)
			observed[key] = append(observed[key], stage.ServiceNS)
			identities[key] = stage
		}
	}
	predicted := make(map[string]StageMetrics, len(stages))
	for _, stage := range stages {
		predicted[modelKey(stage.StageID, stage.ProfileRevision, stage.RequestCohort)] = stage
	}
	keys := make([]string, 0, len(observed))
	for key := range observed {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]CalibrationError, 0, len(keys))
	for _, key := range keys {
		values := observed[key]
		sort.Slice(values, func(left, right int) bool { return values[left] < values[right] })
		stage := predicted[key]
		observedP50 := percentile(values, 50)
		observedP95 := percentile(values, 95)
		observedP99 := percentile(values, 99)
		if stage.Service.Count == 0 {
			identity := identities[key]
			result = append(result, CalibrationError{
				StageID: identity.StageID, ProfileRevision: identity.ProfileRevision,
				RequestCohort: identity.RequestCohort, SampleCount: len(values),
				ObservedP50NS: observedP50, ObservedP95NS: observedP95, ObservedP99NS: observedP99,
				Status: "NO_PREDICTED_SAMPLES",
			})
			continue
		}
		result = append(result, CalibrationError{
			StageID: stage.StageID, ProfileRevision: stage.ProfileRevision,
			RequestCohort: stage.RequestCohort, SampleCount: len(values),
			PredictedP50NS: stage.Service.P50, ObservedP50NS: observedP50,
			AbsoluteErrorNS:  absoluteDifference(stage.Service.P50, observedP50),
			RelativeErrorPPM: relativeErrorPPM(stage.Service.P50, observedP50),
			PredictedP95NS:   stage.Service.P95, ObservedP95NS: observedP95,
			P95AbsoluteErrorNS:  absoluteDifference(stage.Service.P95, observedP95),
			P95RelativeErrorPPM: relativeErrorPPM(stage.Service.P95, observedP95),
			PredictedP99NS:      stage.Service.P99, ObservedP99NS: observedP99,
			P99AbsoluteErrorNS:  absoluteDifference(stage.Service.P99, observedP99),
			P99RelativeErrorPPM: relativeErrorPPM(stage.Service.P99, observedP99),
			Status:              "AVAILABLE",
		})
	}
	return result
}

func absoluteDifference(left, right int64) int64 {
	difference := left - right
	if difference < 0 {
		return -difference
	}
	return difference
}

func sampleDistribution(distribution Distribution, seed uint64, key string) int64 {
	if distribution.P50 == distribution.HardMax {
		return distribution.P50
	}
	value := deterministicMillion(seed, key)
	switch {
	case value < 500_000:
		return interpolate(1, distribution.P50, value, 500_000)
	case value < 950_000:
		return interpolate(distribution.P50, distribution.P95, value-500_000, 450_000)
	case value < 990_000:
		return interpolate(distribution.P95, distribution.P99, value-950_000, 40_000)
	default:
		return interpolate(distribution.P99, distribution.HardMax, value-990_000, 10_000)
	}
}

func deterministicRate(rate int, seed uint64, key string) bool {
	return rate > 0 && deterministicMillion(seed, key) < uint64(rate)
}

func deterministicMillion(seed uint64, key string) uint64 {
	payload := make([]byte, 8+len(key))
	binary.BigEndian.PutUint64(payload, seed)
	copy(payload[8:], key)
	digest := sha256.Sum256(payload)
	return binary.BigEndian.Uint64(digest[:8]) % 1_000_000
}

func interpolate(low, high int64, numerator uint64, denominator uint64) int64 {
	if high <= low || denominator == 0 {
		return low
	}
	return low + multiplyDivide(high-low, int64(numerator), int64(denominator))
}

func modelKey(stageID, profile, cohort string) string {
	return stageID + "\x00" + profile + "\x00" + cohort
}

func scopedCacheKey(record TraceRecord, stageID string) string {
	return record.OrganizationCohort + "\x00" + record.ProjectCohort + "\x00" +
		record.CacheKeyCohort + "\x00" + stageID
}

func firstFreeWorker(pool *poolRuntime) int {
	for index, busy := range pool.workers {
		if !busy {
			return index
		}
	}
	return -1
}

func earliestPoolAvailability(pool *poolRuntime) int64 {
	if !pool.warm {
		return pool.spec.WarmReadyOffsetNS
	}
	earliest := int64(math.MaxInt64)
	for index, busy := range pool.workers {
		if !busy {
			return 0
		}
		if pool.busyUntil[index] < earliest {
			earliest = pool.busyUntil[index]
		}
	}
	return earliest
}
