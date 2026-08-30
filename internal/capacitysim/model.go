package capacitysim

const (
	AlgorithmRevision = "capacity-sim-v1"
	SchemaVersion     = 1
	MaxInputBytes     = 64 << 20
	MaxTraceRecords   = 1_000_000
)

type Provenance struct {
	SourceKind        string `json:"source_kind"`
	CollectionWindow  string `json:"collection_window"`
	HardwareRevision  string `json:"hardware_revision"`
	RuntimeRevision   string `json:"runtime_revision"`
	ModelRevision     string `json:"model_revision"`
	ProfileRevision   string `json:"profile_revision"`
	ConnectorRevision string `json:"connector_revision,omitempty"`
	Units             string `json:"units"`
	SampleCount       int64  `json:"sample_count"`
	FreshnessOffsetNS int64  `json:"freshness_offset_ns"`
	ConfidencePPM     int    `json:"confidence_ppm"`
	ContentDigest     string `json:"content_digest"`
}

type ScenarioRevision struct {
	SchemaVersion     int               `json:"schema_version"`
	Revision          string            `json:"revision"`
	AlgorithmRevision string            `json:"algorithm_revision"`
	Seed              uint64            `json:"seed"`
	GraphRevision     string            `json:"graph_revision"`
	WindowDurationNS  int64             `json:"window_duration_ns"`
	Provenance        Provenance        `json:"provenance"`
	Stages            []StageSpec       `json:"stages"`
	Pools             []ResidentPool    `json:"pools"`
	Limits            Limits            `json:"limits"`
	Policy            Policy            `json:"policy"`
	CostModel         CostModelRevision `json:"cost_model"`
	PricingSnapshots  []PricingSnapshot `json:"pricing_snapshots"`
}

type StageSpec struct {
	ID                string   `json:"id"`
	ProfileRevision   string   `json:"profile_revision"`
	ResourceKind      string   `json:"resource_kind"`
	DeviceCount       int      `json:"device_count"`
	Dependencies      []string `json:"dependencies,omitempty"`
	ConnectorRevision string   `json:"connector_revision,omitempty"`
}

type ResidentPool struct {
	ID                string `json:"id"`
	StageID           string `json:"stage_id"`
	ProfileRevision   string `json:"profile_revision"`
	WorkerCount       int    `json:"worker_count"`
	DeviceCount       int    `json:"device_count"`
	NetworkDomain     string `json:"network_domain"`
	FaultDomain       string `json:"fault_domain"`
	ModelRevision     string `json:"model_revision"`
	WarmReadyOffsetNS int64  `json:"warm_ready_offset_ns"`
	MinCount          int    `json:"min_count"`
	MaxCount          int    `json:"max_count"`
}

type Limits struct {
	MaxJobs          int   `json:"max_jobs"`
	MaxEvents        int   `json:"max_events"`
	MaxQueuePerStage int   `json:"max_queue_per_stage"`
	MaxBufferItems   int   `json:"max_buffer_items"`
	MaxBufferBytes   int64 `json:"max_buffer_bytes"`
	MaxStorageBytes  int64 `json:"max_storage_bytes"`
	MaxCacheEntries  int   `json:"max_cache_entries"`
	MaxCacheBytes    int64 `json:"max_cache_bytes"`
}

type Policy struct {
	SchedulerRevision        string `json:"scheduler_revision"`
	CachePolicyRevision      string `json:"cache_policy_revision"`
	MaxRetriesPerStage       int    `json:"max_retries_per_stage"`
	CacheEnabled             bool   `json:"cache_enabled"`
	CacheTTLNS               int64  `json:"cache_ttl_ns"`
	FinalizationDurationNS   int64  `json:"finalization_duration_ns"`
	ReleaseHealthyResidency  bool   `json:"release_healthy_residency"`
	ProposalCooldownNS       int64  `json:"proposal_cooldown_ns"`
	ProposalExpiryNS         int64  `json:"proposal_expiry_ns"`
	JobResourceMultiplierPPM int    `json:"job_resource_multiplier_ppm,omitempty"`
}

type CostModelRevision struct {
	Revision                     string     `json:"revision"`
	GPUMicroUnitsPerSecond       int64      `json:"gpu_micro_units_per_second"`
	CPUMicroUnitsPerSecond       int64      `json:"cpu_micro_units_per_second"`
	NetworkMicroUnitsPerGB       int64      `json:"network_micro_units_per_gb"`
	StorageMicroUnitsPerGBSecond int64      `json:"storage_micro_units_per_gb_second"`
	MemoryMicroUnitsPerGBSecond  int64      `json:"memory_micro_units_per_gb_second"`
	ScratchMicroUnitsPerGBSecond int64      `json:"scratch_micro_units_per_gb_second"`
	SharedAllocationMethod       string     `json:"shared_allocation_method"`
	Provenance                   Provenance `json:"provenance"`
}

type PricingSnapshot struct {
	Revision                 string     `json:"revision"`
	ServiceClassRevision     string     `json:"service_class_revision"`
	GenerationPresetRevision string     `json:"generation_preset_revision"`
	OutputSpec               string     `json:"output_spec"`
	PriceMicroUnits          int64      `json:"price_micro_units"`
	Provenance               Provenance `json:"provenance"`
}

type WorkloadTrace struct {
	SchemaVersion int           `json:"schema_version"`
	Revision      string        `json:"revision"`
	Provenance    Provenance    `json:"provenance"`
	Records       []TraceRecord `json:"records"`
}

type TraceRecord struct {
	RecordKind                  string                `json:"record_kind"`
	SchemaVersion               int                   `json:"schema_version"`
	Revision                    string                `json:"revision,omitempty"`
	Provenance                  *Provenance           `json:"provenance,omitempty"`
	TraceID                     string                `json:"trace_id,omitempty"`
	ArrivalOffsetNS             int64                 `json:"arrival_offset_ns,omitempty"`
	OrganizationCohort          string                `json:"organization_cohort,omitempty"`
	ProjectCohort               string                `json:"project_cohort,omitempty"`
	ServiceClassRevision        string                `json:"service_class_revision,omitempty"`
	GenerationPresetRevision    string                `json:"generation_preset_revision,omitempty"`
	OutputSpec                  string                `json:"output_spec,omitempty"`
	RequestCohort               string                `json:"request_cohort,omitempty"`
	JobExpiryOffsetNS           int64                 `json:"job_expiry_offset_ns,omitempty"`
	EligibleGraphRevision       string                `json:"eligible_graph_revision,omitempty"`
	CachePolicyRevision         string                `json:"cache_policy_revision,omitempty"`
	CacheKeyCohort              string                `json:"cache_key_cohort,omitempty"`
	ObservedStages              []ObservedStageTiming `json:"observed_stages,omitempty"`
	ObservedFinalizationNS      int64                 `json:"observed_finalization_ns,omitempty"`
	ObservedVisibleCompletionNS int64                 `json:"observed_visible_completion_ns,omitempty"`
}

type ObservedStageTiming struct {
	StageID         string `json:"stage_id"`
	ProfileRevision string `json:"profile_revision"`
	RequestCohort   string `json:"request_cohort"`
	QueueNS         int64  `json:"queue_ns,omitempty"`
	TransferNS      int64  `json:"transfer_ns,omitempty"`
	ServiceNS       int64  `json:"service_ns"`
	MaterializeNS   int64  `json:"materialize_ns,omitempty"`
	OutputBytes     int64  `json:"output_bytes,omitempty"`
}

type CalibrationBundle struct {
	SchemaVersion   int                 `json:"schema_version"`
	Revision        string              `json:"revision"`
	Provenance      Provenance          `json:"provenance"`
	StageModels     []StageRuntimeModel `json:"stage_models"`
	ConnectorModels []ConnectorModel    `json:"connector_models"`
}

type Distribution struct {
	P50     int64 `json:"p50"`
	P95     int64 `json:"p95"`
	P99     int64 `json:"p99"`
	HardMax int64 `json:"hard_max"`
}

type StageRuntimeModel struct {
	StageID             string       `json:"stage_id"`
	ProfileRevision     string       `json:"profile_revision"`
	RequestCohort       string       `json:"request_cohort"`
	ServiceTime         Distribution `json:"service_time_ns"`
	OutputBytes         Distribution `json:"output_bytes"`
	SealTime            Distribution `json:"seal_time_ns"`
	MaterializationTime Distribution `json:"materialization_time_ns"`
	RecoveryTime        Distribution `json:"recovery_time_ns"`
	FailureRatePPM      int          `json:"failure_rate_ppm"`
	GPUCount            int          `json:"gpu_count"`
	CPUMilli            int64        `json:"cpu_milli"`
	MemoryBytes         int64        `json:"memory_bytes"`
	ScratchBytes        int64        `json:"scratch_bytes,omitempty"`
	Provenance          Provenance   `json:"provenance"`
}

type ConnectorModel struct {
	Revision              string     `json:"revision"`
	SetupLatencyNS        int64      `json:"setup_latency_ns"`
	PayloadBytesPerSecond int64      `json:"payload_bytes_per_second"`
	ConcurrencyLimit      int        `json:"concurrency_limit"`
	FailureRatePPM        int        `json:"failure_rate_ppm"`
	Outage                bool       `json:"outage"`
	ObjectReadMicroUnits  int64      `json:"object_read_micro_units,omitempty"`
	ObjectWriteMicroUnits int64      `json:"object_write_micro_units,omitempty"`
	Provenance            Provenance `json:"provenance"`
}

type SimulationReceipt struct {
	SchemaVersion       int                 `json:"schema_version"`
	SimulatorRevision   string              `json:"simulator_revision"`
	Seed                uint64              `json:"seed"`
	ScenarioDigest      string              `json:"scenario_digest"`
	TraceDigest         string              `json:"trace_digest"`
	CalibrationDigest   string              `json:"calibration_digest"`
	ReceiptDigest       string              `json:"receipt_digest"`
	WindowDurationNS    int64               `json:"window_duration_ns"`
	InputEvidence       []InputEvidence     `json:"input_evidence"`
	Validation          ValidationResult    `json:"validation"`
	Admission           AdmissionMetrics    `json:"admission"`
	Completion          CompletionMetrics   `json:"completion"`
	Conservation        ConservationChecks  `json:"conservation"`
	Latency             DurationStats       `json:"end_to_end_latency_ns"`
	DynamicETA          DynamicETAError     `json:"dynamic_eta"`
	Stages              []StageMetrics      `json:"stages"`
	Pools               []PoolMetrics       `json:"pools"`
	Buffers             BufferMetrics       `json:"buffers"`
	Cache               CacheMetrics        `json:"cache"`
	Failures            FailureMetrics      `json:"failures"`
	Cost                CostMetrics         `json:"cost"`
	PriceComparisons    []PriceComparison   `json:"price_comparisons"`
	Fairness            []FairnessMetrics   `json:"fairness"`
	CalibrationErrors   []CalibrationError  `json:"calibration_errors"`
	TransferSensitivity []SensitivityResult `json:"transfer_sensitivity"`
	DroppedInputs       []InputDisposition  `json:"dropped_inputs"`
}

type InputEvidence struct {
	Path              string `json:"path"`
	SourceKind        string `json:"source_kind"`
	CollectionWindow  string `json:"collection_window"`
	Units             string `json:"units"`
	SampleCount       int64  `json:"sample_count"`
	FreshnessOffsetNS int64  `json:"freshness_offset_ns"`
	ConfidencePPM     int    `json:"confidence_ppm"`
	ContentDigest     string `json:"content_digest"`
}

type ValidationResult struct {
	Valid             bool     `json:"valid"`
	UnsupportedInputs []string `json:"unsupported_inputs"`
}

type AdmissionMetrics struct {
	Accepted     int           `json:"accepted"`
	Rejected     int           `json:"rejected"`
	ReasonCounts []ReasonCount `json:"reason_counts"`
}

type CompletionMetrics struct {
	VisibleCompletions     int   `json:"visible_completions"`
	SuccessRatePPM         int   `json:"success_rate_ppm"`
	ThroughputPerSecondPPM int64 `json:"throughput_per_second_ppm"`
}

type DynamicETAError struct {
	Status         string `json:"status"`
	SampleCount    int    `json:"sample_count"`
	AbsoluteP50NS  int64  `json:"absolute_p50_ns"`
	AbsoluteP95NS  int64  `json:"absolute_p95_ns"`
	RelativeP95PPM int    `json:"relative_p95_ppm"`
}

type ReasonCount struct {
	Reason string `json:"reason"`
	Count  int    `json:"count"`
}

type ConservationChecks struct {
	Arrivals           int   `json:"arrivals"`
	Accepted           int   `json:"accepted"`
	Rejected           int   `json:"rejected"`
	VisibleCompletions int   `json:"visible_completions"`
	Failed             int   `json:"failed"`
	Expired            int   `json:"expired"`
	Unfinished         int   `json:"unfinished"`
	EventsProcessed    int   `json:"events_processed"`
	StorageBytes       int64 `json:"storage_bytes"`
	Valid              bool  `json:"valid"`
}

type DurationStats struct {
	Count int   `json:"count"`
	P50   int64 `json:"p50"`
	P95   int64 `json:"p95"`
	P99   int64 `json:"p99"`
	Max   int64 `json:"max"`
}

type StageMetrics struct {
	StageID           string        `json:"stage_id"`
	ProfileRevision   string        `json:"profile_revision"`
	RequestCohort     string        `json:"request_cohort"`
	Queue             DurationStats `json:"queue_ns"`
	Transfer          DurationStats `json:"transfer_ns"`
	Service           DurationStats `json:"service_ns"`
	Materialization   DurationStats `json:"materialization_ns"`
	OutputBytes       DurationStats `json:"output_bytes"`
	Starts            int           `json:"starts"`
	Seals             int           `json:"seals"`
	Completions       int           `json:"completions"`
	Retries           int           `json:"retries"`
	Failures          int           `json:"failures"`
	CacheHits         int           `json:"cache_hits"`
	MaximumQueueDepth int           `json:"maximum_queue_depth"`
}

type PoolMetrics struct {
	PoolID            string `json:"pool_id"`
	StageID           string `json:"stage_id"`
	WorkerCount       int    `json:"worker_count"`
	DeviceCount       int    `json:"device_count"`
	BusyNS            int64  `json:"busy_ns"`
	BusyDeviceNS      int64  `json:"busy_device_ns"`
	IdleNS            int64  `json:"idle_ns"`
	WarmupNS          int64  `json:"warmup_ns"`
	ResidencyNS       int64  `json:"residency_ns"`
	LoadActuations    int    `json:"load_actuations"`
	ReleaseActuations int    `json:"release_actuations"`
}

type BufferMetrics struct {
	PeakItems          int   `json:"peak_items"`
	PeakBytes          int64 `json:"peak_bytes"`
	PeakStorageBytes   int64 `json:"peak_storage_bytes"`
	BackpressureEvents int   `json:"backpressure_events"`
	StorageBlockedJobs int   `json:"storage_blocked_jobs"`
}

type CacheMetrics struct {
	Hits                int   `json:"hits"`
	Misses              int   `json:"misses"`
	Pins                int   `json:"pins"`
	PinReleases         int   `json:"pin_releases"`
	Evictions           int   `json:"evictions"`
	Entries             int   `json:"entries"`
	Bytes               int64 `json:"bytes"`
	SavedServiceNS      int64 `json:"saved_service_ns"`
	AvoidedComputeCount int   `json:"avoided_compute_count"`
}

type FailureMetrics struct {
	StageFailures      int   `json:"stage_failures"`
	Retries            int   `json:"retries"`
	RetryWasteDeviceNS int64 `json:"retry_waste_device_ns"`
	ExpiredJobs        int   `json:"expired_jobs"`
	CanceledJobs       int   `json:"canceled_jobs"`
	FinalizationFailed int   `json:"finalization_failed"`
}

type CostMetrics struct {
	DirectGPUMicroUnits        int64 `json:"direct_gpu_micro_units"`
	DirectCPUMicroUnits        int64 `json:"direct_cpu_micro_units"`
	MemoryMicroUnits           int64 `json:"memory_micro_units"`
	ScratchMicroUnits          int64 `json:"scratch_micro_units"`
	DirectStageMicroUnits      int64 `json:"direct_stage_micro_units"`
	SharedResidencyMicroUnits  int64 `json:"shared_residency_micro_units"`
	TransferMicroUnits         int64 `json:"transfer_micro_units"`
	StorageMicroUnits          int64 `json:"storage_micro_units"`
	RetryWasteMicroUnits       int64 `json:"retry_waste_micro_units"`
	CacheMicroUnits            int64 `json:"cache_micro_units"`
	TotalMicroUnits            int64 `json:"total_micro_units"`
	PerVisibleCompletionMicros int64 `json:"per_visible_completion_micro_units"`
}

type PriceComparison struct {
	PricingSnapshotRevision      string `json:"pricing_snapshot_revision"`
	ServiceClassRevision         string `json:"service_class_revision"`
	GenerationPresetRevision     string `json:"generation_preset_revision"`
	OutputSpec                   string `json:"output_spec"`
	JobCount                     int    `json:"job_count"`
	FixedCustomerPriceMicroUnits int64  `json:"fixed_customer_price_micro_units"`
	AllocatedInternalMicroUnits  int64  `json:"allocated_internal_micro_units"`
}

type FairnessMetrics struct {
	OrganizationCohort  string `json:"organization_cohort"`
	AttainedServiceNS   int64  `json:"attained_service_ns"`
	ShareErrorPPM       int    `json:"share_error_ppm"`
	MaximumStarvationNS int64  `json:"maximum_starvation_ns"`
}

type CalibrationError struct {
	StageID             string `json:"stage_id"`
	ProfileRevision     string `json:"profile_revision"`
	RequestCohort       string `json:"request_cohort"`
	SampleCount         int    `json:"sample_count"`
	PredictedP50NS      int64  `json:"predicted_p50_ns"`
	ObservedP50NS       int64  `json:"observed_p50_ns"`
	AbsoluteErrorNS     int64  `json:"absolute_error_ns"`
	RelativeErrorPPM    int    `json:"relative_error_ppm"`
	PredictedP95NS      int64  `json:"predicted_p95_ns"`
	ObservedP95NS       int64  `json:"observed_p95_ns"`
	P95AbsoluteErrorNS  int64  `json:"p95_absolute_error_ns"`
	P95RelativeErrorPPM int    `json:"p95_relative_error_ppm"`
	PredictedP99NS      int64  `json:"predicted_p99_ns"`
	ObservedP99NS       int64  `json:"observed_p99_ns"`
	P99AbsoluteErrorNS  int64  `json:"p99_absolute_error_ns"`
	P99RelativeErrorPPM int    `json:"p99_relative_error_ppm"`
	Status              string `json:"status"`
}

type SensitivityResult struct {
	Case               string `json:"case"`
	ThroughputScalePPM int    `json:"throughput_scale_ppm"`
	VisibleCompletions int    `json:"visible_completions"`
	Failed             int    `json:"failed"`
	Expired            int    `json:"expired"`
	LatencyP99NS       int64  `json:"latency_p99_ns"`
}

type InputDisposition struct {
	TraceID string `json:"trace_id"`
	Reason  string `json:"reason"`
}

type ResidencyProposal struct {
	SchemaVersion    int                     `json:"schema_version"`
	InputDigest      string                  `json:"input_digest"`
	AutoApply        bool                    `json:"auto_apply"`
	ConfidencePPM    int                     `json:"confidence_ppm"`
	ExpiresOffsetNS  int64                   `json:"expires_offset_ns"`
	CooldownNS       int64                   `json:"cooldown_ns"`
	BudgetMicroUnits int64                   `json:"budget_micro_units"`
	Pools            []ResidencyPoolProposal `json:"pools"`
	ReasonCodes      []string                `json:"reason_codes"`
	UnresolvedRisks  []string                `json:"unresolved_risks"`
}

type ResidencyPoolProposal struct {
	PoolID       string `json:"pool_id"`
	CurrentCount int    `json:"current_count"`
	MinCount     int    `json:"min_count"`
	DesiredCount int    `json:"desired_count"`
	MaxCount     int    `json:"max_count"`
}

type ReceiptComparison struct {
	SchemaVersion          int                              `json:"schema_version"`
	BaselineReceiptDigest  string                           `json:"baseline_receipt_digest"`
	CandidateReceiptDigest string                           `json:"candidate_receipt_digest"`
	ComparisonDigest       string                           `json:"comparison_digest"`
	Deltas                 []MetricDelta                    `json:"deltas"`
	SourceClassifications  []SourceClassificationComparison `json:"source_classifications"`
}

type MetricDelta struct {
	Metric         string `json:"metric"`
	BaselineValue  int64  `json:"baseline_value"`
	CandidateValue int64  `json:"candidate_value"`
	Delta          int64  `json:"delta"`
}

type SourceClassificationComparison struct {
	Path                string `json:"path"`
	BaselineSourceKind  string `json:"baseline_source_kind"`
	CandidateSourceKind string `json:"candidate_source_kind"`
}
