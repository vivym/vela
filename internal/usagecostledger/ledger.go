package usagecostledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var ErrReplayMismatch = errors.New("usage/cost replay does not match the recorded receipt")

type Attribution string

const (
	AttributionDirect         Attribution = "DIRECT"
	AttributionShared         Attribution = "SHARED"
	AttributionCounterfactual Attribution = "COUNTERFACTUAL"
)

type UsageClass string

const (
	UsageExecution           UsageClass = "EXECUTION"
	UsageResidency           UsageClass = "RESIDENCY"
	UsageLoadWarmup          UsageClass = "LOAD_WARMUP"
	UsageRetry               UsageClass = "RETRY"
	UsageCancellation        UsageClass = "CANCELLATION"
	UsageTransfer            UsageClass = "TRANSFER"
	UsageStorage             UsageClass = "STORAGE"
	UsageFinalization        UsageClass = "FINALIZATION"
	UsageDrain               UsageClass = "DRAIN"
	UsageFailedReconfigure   UsageClass = "FAILED_RECONFIGURATION"
	UsageMinimumWarmCapacity UsageClass = "MINIMUM_WARM_CAPACITY"
	UsageCacheAvoidedCompute UsageClass = "CACHE_AVOIDED_COMPUTE"
)

type ResourceKind string

const (
	ResourceGPUNanosecond   ResourceKind = "GPU_NANOSECOND"
	ResourceCPUNanosecond   ResourceKind = "CPU_NANOSECOND"
	ResourceByteNanosecond  ResourceKind = "BYTE_NANOSECOND"
	ResourceByte            ResourceKind = "BYTE"
	ResourceObjectOperation ResourceKind = "OBJECT_OPERATION"
)

type SourceKind string

const (
	SourceStageAttempt       SourceKind = "STAGE_ATTEMPT"
	SourceStageCache         SourceKind = "STAGE_CACHE"
	SourceTransferTicket     SourceKind = "TRANSFER_TICKET"
	SourceStorageReservation SourceKind = "STORAGE_RESERVATION"
	SourceFinalizationClaim  SourceKind = "FINALIZATION_CLAIM"
	SourceCapacityPool       SourceKind = "CAPACITY_POOL"
	SourceModelResidency     SourceKind = "MODEL_RESIDENCY"
	SourceResidencyOperation SourceKind = "RESIDENCY_OPERATION"
)

type RecordUsageCommand struct {
	ID                uuid.UUID
	SchemaVersion     int
	SourceKind        SourceKind
	SourceAuthorityID uuid.UUID
	ReceiptDigest     [32]byte
	Attribution       Attribution
	UsageClass        UsageClass
	ResourceKind      ResourceKind
	Quantity          int64
	OrganizationID    uuid.UUID
	ProjectID         uuid.UUID
	JobID             uuid.UUID
	AttemptID         uuid.UUID
	StageAttemptID    uuid.UUID
	CapacityPoolID    uuid.UUID
	IntervalStart     time.Time
	IntervalEnd       time.Time
	RecordedAt        time.Time
}

type UsageIdentity struct {
	ID       uuid.UUID
	Replayed bool
}

type ValueCommand struct {
	AllocationID        uuid.UUID
	UsageID             uuid.UUID
	CostModelRevisionID uuid.UUID
	ValuedAt            time.Time
}

type CostAllocation struct {
	ID                      uuid.UUID
	UsageID                 uuid.UUID
	CostModelRevisionID     uuid.UUID
	SupersedesAllocationID  uuid.UUID
	Attribution             Attribution
	ResourceKind            ResourceKind
	Quantity                int64
	RateNumeratorMicroUnits int64
	RateDenominatorUnits    int64
	CostMicroUnits          int64
	ValuedAt                time.Time
	Replayed                bool
}

type SummaryQuery struct {
	CostModelRevisionID uuid.UUID
	From                time.Time
	To                  time.Time
}

type SummaryBucket struct {
	Attribution         Attribution
	UsageClass          UsageClass
	ResourceKind        ResourceKind
	Quantity            int64
	CostMicroUnits      int64
	UsageRecordCount    int64
	UnvaluedRecordCount int64
}

type OperatorSummary struct {
	Buckets                             []SummaryBucket
	DirectCostMicroUnits                int64
	SharedCostMicroUnits                int64
	CounterfactualAvoidedCostMicroUnits int64
	UnvaluedRecordCount                 int64
}

type Backend interface {
	RecordUsage(context.Context, RecordUsageCommand) (UsageIdentity, error)
	Value(context.Context, ValueCommand) (CostAllocation, error)
	Summarize(context.Context, SummaryQuery) (OperatorSummary, error)
}

type UsageCostLedger struct {
	backend Backend
}

func New(backend Backend) (*UsageCostLedger, error) {
	if backend == nil {
		return nil, errors.New("Usage/Cost Ledger backend is required")
	}
	return &UsageCostLedger{backend: backend}, nil
}

func (ledger *UsageCostLedger) RecordUsage(
	ctx context.Context,
	command RecordUsageCommand,
) (UsageIdentity, error) {
	if ledger == nil || ledger.backend == nil {
		return UsageIdentity{}, errors.New("Usage/Cost Ledger is not configured")
	}
	if err := validateRecordUsage(ctx, command); err != nil {
		return UsageIdentity{}, err
	}
	return ledger.backend.RecordUsage(ctx, command)
}

func (ledger *UsageCostLedger) Value(
	ctx context.Context,
	command ValueCommand,
) (CostAllocation, error) {
	if ledger == nil || ledger.backend == nil {
		return CostAllocation{}, errors.New("Usage/Cost Ledger is not configured")
	}
	if ctx == nil || command.AllocationID == uuid.Nil || command.UsageID == uuid.Nil ||
		command.CostModelRevisionID == uuid.Nil || command.ValuedAt.IsZero() {
		return CostAllocation{}, errors.New("Usage/Cost valuation command is incomplete")
	}
	return ledger.backend.Value(ctx, command)
}

func (ledger *UsageCostLedger) Summarize(
	ctx context.Context,
	query SummaryQuery,
) (OperatorSummary, error) {
	if ledger == nil || ledger.backend == nil {
		return OperatorSummary{}, errors.New("Usage/Cost Ledger is not configured")
	}
	if ctx == nil || query.CostModelRevisionID == uuid.Nil || query.From.IsZero() ||
		query.To.IsZero() || !query.To.After(query.From) || query.To.Sub(query.From) > 366*24*time.Hour {
		return OperatorSummary{}, errors.New("Usage/Cost summary query is incomplete or unbounded")
	}
	summary, err := ledger.backend.Summarize(ctx, query)
	if err != nil {
		return OperatorSummary{}, err
	}
	summary.DirectCostMicroUnits = 0
	summary.SharedCostMicroUnits = 0
	summary.CounterfactualAvoidedCostMicroUnits = 0
	summary.UnvaluedRecordCount = 0
	for _, bucket := range summary.Buckets {
		if bucket.Quantity < 0 || bucket.CostMicroUnits < 0 || bucket.UsageRecordCount < 0 ||
			bucket.UnvaluedRecordCount < 0 {
			return OperatorSummary{}, errors.New("Usage/Cost summary contains a negative counter")
		}
		switch bucket.Attribution {
		case AttributionDirect:
			summary.DirectCostMicroUnits += bucket.CostMicroUnits
		case AttributionShared:
			summary.SharedCostMicroUnits += bucket.CostMicroUnits
		case AttributionCounterfactual:
			summary.CounterfactualAvoidedCostMicroUnits += bucket.CostMicroUnits
		default:
			return OperatorSummary{}, fmt.Errorf(
				"Usage/Cost summary contains unknown attribution %q", bucket.Attribution,
			)
		}
		summary.UnvaluedRecordCount += bucket.UnvaluedRecordCount
	}
	return summary, nil
}

func validateRecordUsage(ctx context.Context, command RecordUsageCommand) error {
	if ctx == nil || command.ID == uuid.Nil || command.SchemaVersion != 1 ||
		command.SourceAuthorityID == uuid.Nil || command.ReceiptDigest == ([32]byte{}) ||
		command.Quantity <= 0 || command.IntervalStart.IsZero() || command.IntervalEnd.IsZero() ||
		command.RecordedAt.IsZero() || command.IntervalEnd.Before(command.IntervalStart) ||
		command.RecordedAt.Before(command.IntervalEnd) {
		return errors.New("resource usage receipt is incomplete")
	}
	if !validSourceKind(command.SourceKind) || !validResourceKind(command.ResourceKind) ||
		!validUsageClass(command.UsageClass) {
		return errors.New("resource usage receipt contains an unsupported bounded value")
	}
	switch command.Attribution {
	case AttributionDirect:
		if command.OrganizationID == uuid.Nil || command.ProjectID == uuid.Nil ||
			command.JobID == uuid.Nil || command.AttemptID == uuid.Nil ||
			command.CapacityPoolID != uuid.Nil || command.UsageClass == UsageCacheAvoidedCompute {
			return errors.New("direct Resource Usage ancestry is invalid")
		}
		if command.SourceKind == SourceStageAttempt && command.StageAttemptID == uuid.Nil {
			return errors.New("StageAttempt usage requires StageAttempt ancestry")
		}
		if command.SourceKind == SourceStageAttempt &&
			command.SourceAuthorityID != command.StageAttemptID {
			return errors.New("StageAttempt source authority does not match its ancestry")
		}
		if !directSourceKind(command.SourceKind) {
			return errors.New("direct Resource Usage source is invalid")
		}
	case AttributionShared:
		if command.CapacityPoolID == uuid.Nil || command.OrganizationID != uuid.Nil ||
			command.ProjectID != uuid.Nil || command.JobID != uuid.Nil ||
			command.AttemptID != uuid.Nil || command.StageAttemptID != uuid.Nil ||
			!sharedUsageClass(command.UsageClass) {
			return errors.New("shared Resource Usage ancestry or class is invalid")
		}
		if command.SourceKind == SourceCapacityPool &&
			command.SourceAuthorityID != command.CapacityPoolID {
			return errors.New("CapacityPool source authority does not match its ancestry")
		}
		if !sharedSourceKind(command.SourceKind) {
			return errors.New("shared Resource Usage source is invalid")
		}
	case AttributionCounterfactual:
		if command.OrganizationID == uuid.Nil || command.ProjectID == uuid.Nil ||
			command.JobID == uuid.Nil || command.AttemptID == uuid.Nil ||
			command.StageAttemptID != uuid.Nil || command.CapacityPoolID != uuid.Nil ||
			command.UsageClass != UsageCacheAvoidedCompute || command.SourceKind != SourceStageCache {
			return errors.New("cache counterfactual ancestry is invalid")
		}
	default:
		return errors.New("resource usage attribution is unsupported")
	}
	return nil
}

func validSourceKind(value SourceKind) bool {
	switch value {
	case SourceStageAttempt, SourceStageCache, SourceTransferTicket,
		SourceStorageReservation, SourceFinalizationClaim, SourceCapacityPool,
		SourceModelResidency, SourceResidencyOperation:
		return true
	default:
		return false
	}
}

func validResourceKind(value ResourceKind) bool {
	switch value {
	case ResourceGPUNanosecond, ResourceCPUNanosecond, ResourceByteNanosecond,
		ResourceByte, ResourceObjectOperation:
		return true
	default:
		return false
	}
}

func validUsageClass(value UsageClass) bool {
	switch value {
	case UsageExecution, UsageResidency, UsageLoadWarmup, UsageRetry, UsageCancellation,
		UsageTransfer, UsageStorage, UsageFinalization, UsageDrain, UsageFailedReconfigure,
		UsageMinimumWarmCapacity, UsageCacheAvoidedCompute:
		return true
	default:
		return false
	}
}

func sharedUsageClass(value UsageClass) bool {
	switch value {
	case UsageResidency, UsageLoadWarmup, UsageDrain, UsageFailedReconfigure,
		UsageMinimumWarmCapacity:
		return true
	default:
		return false
	}
}

func directSourceKind(value SourceKind) bool {
	switch value {
	case SourceStageAttempt, SourceTransferTicket, SourceStorageReservation,
		SourceFinalizationClaim:
		return true
	default:
		return false
	}
}

func sharedSourceKind(value SourceKind) bool {
	switch value {
	case SourceCapacityPool, SourceModelResidency, SourceResidencyOperation:
		return true
	default:
		return false
	}
}
