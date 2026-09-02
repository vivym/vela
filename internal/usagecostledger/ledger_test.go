package usagecostledger

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRecordUsageReplayAndMismatch(t *testing.T) {
	backend := &memoryBackend{}
	ledger, err := New(backend)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	command := directUsageCommand()

	first, err := ledger.RecordUsage(context.Background(), command)
	if err != nil {
		t.Fatalf("RecordUsage: %v", err)
	}
	replayed, err := ledger.RecordUsage(context.Background(), command)
	if err != nil {
		t.Fatalf("replay RecordUsage: %v", err)
	}
	if first.ID != command.ID || first.Replayed || replayed.ID != first.ID || !replayed.Replayed {
		t.Fatalf("usage first=%#v replay=%#v", first, replayed)
	}

	changed := command
	changed.Quantity++
	if _, err := ledger.RecordUsage(context.Background(), changed); !errors.Is(err, ErrReplayMismatch) {
		t.Fatalf("changed replay error = %v, want ErrReplayMismatch", err)
	}
}

func TestValueAppendsRevaluationWithoutChangingUsage(t *testing.T) {
	backend := &memoryBackend{}
	ledger, err := New(backend)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	usage := directUsageCommand()
	if _, err := ledger.RecordUsage(context.Background(), usage); err != nil {
		t.Fatalf("RecordUsage: %v", err)
	}

	first, err := ledger.Value(context.Background(), ValueCommand{
		AllocationID: uuid.New(), UsageID: usage.ID,
		CostModelRevisionID: uuid.New(), ValuedAt: usage.RecordedAt.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	second, err := ledger.Value(context.Background(), ValueCommand{
		AllocationID: uuid.New(), UsageID: usage.ID,
		CostModelRevisionID: uuid.New(), ValuedAt: usage.RecordedAt.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatalf("revalue: %v", err)
	}
	if first.SupersedesAllocationID != uuid.Nil || second.SupersedesAllocationID != first.ID {
		t.Fatalf("allocation chain first=%#v second=%#v", first, second)
	}
	if got := backend.usage[usage.ID]; got != usage {
		t.Fatalf("revaluation changed usage: got %#v want %#v", got, usage)
	}
}

func TestSummarizeKeepsDirectSharedAndCounterfactualSeparate(t *testing.T) {
	backend := &memoryBackend{summary: OperatorSummary{Buckets: []SummaryBucket{
		{Attribution: AttributionDirect, CostMicroUnits: 100},
		{Attribution: AttributionShared, CostMicroUnits: 40},
		{Attribution: AttributionCounterfactual, CostMicroUnits: 70},
	}}}
	ledger, err := New(backend)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	modelID := uuid.New()
	summary, err := ledger.Summarize(context.Background(), SummaryQuery{
		CostModelRevisionID: modelID,
		From:                time.Unix(100, 0).UTC(), To: time.Unix(200, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("Summarize: %v", err)
	}
	if summary.DirectCostMicroUnits != 100 || summary.SharedCostMicroUnits != 40 ||
		summary.CounterfactualAvoidedCostMicroUnits != 70 {
		t.Fatalf("summary = %#v", summary)
	}
}

func TestUsageClassAndAncestryMatrix(t *testing.T) {
	direct := directUsageCommand()
	for _, usageClass := range []UsageClass{
		UsageExecution, UsageRetry, UsageCancellation,
	} {
		command := direct
		command.ID = uuid.New()
		command.UsageClass = usageClass
		command.ReceiptDigest = sha256.Sum256([]byte("direct/" + string(usageClass)))
		if err := validateRecordUsage(context.Background(), command); err != nil {
			t.Fatalf("validate direct %s usage: %v", usageClass, err)
		}
	}
	for _, test := range []struct {
		usageClass UsageClass
		sourceKind SourceKind
	}{
		{usageClass: UsageTransfer, sourceKind: SourceTransferTicket},
		{usageClass: UsageStorage, sourceKind: SourceStorageReservation},
		{usageClass: UsageFinalization, sourceKind: SourceFinalizationClaim},
	} {
		command := direct
		command.ID = uuid.New()
		command.UsageClass = test.usageClass
		command.SourceKind = test.sourceKind
		command.SourceAuthorityID = uuid.New()
		command.StageAttemptID = uuid.Nil
		command.ReceiptDigest = sha256.Sum256([]byte("direct/" + string(test.usageClass)))
		if err := validateRecordUsage(context.Background(), command); err != nil {
			t.Fatalf("validate direct %s usage: %v", test.usageClass, err)
		}
	}

	for _, usageClass := range []UsageClass{
		UsageResidency, UsageLoadWarmup, UsageDrain,
		UsageFailedReconfigure, UsageMinimumWarmCapacity,
	} {
		poolID := uuid.New()
		start := time.Unix(2_000, 0).UTC()
		command := RecordUsageCommand{
			ID: uuid.New(), SchemaVersion: 1,
			SourceKind: SourceCapacityPool, SourceAuthorityID: poolID,
			ReceiptDigest: sha256.Sum256([]byte("shared/" + string(usageClass))),
			Attribution:   AttributionShared, UsageClass: usageClass,
			ResourceKind: ResourceGPUNanosecond, Quantity: 1,
			CapacityPoolID: poolID, IntervalStart: start, IntervalEnd: start,
			RecordedAt: start,
		}
		if err := validateRecordUsage(context.Background(), command); err != nil {
			t.Fatalf("validate shared %s usage: %v", usageClass, err)
		}
	}

	counterfactual := direct
	counterfactual.ID = uuid.New()
	counterfactual.SourceKind = SourceStageCache
	counterfactual.SourceAuthorityID = uuid.New()
	counterfactual.ReceiptDigest = sha256.Sum256([]byte("cache/counterfactual"))
	counterfactual.Attribution = AttributionCounterfactual
	counterfactual.UsageClass = UsageCacheAvoidedCompute
	counterfactual.StageAttemptID = uuid.Nil
	if err := validateRecordUsage(context.Background(), counterfactual); err != nil {
		t.Fatalf("validate cache counterfactual: %v", err)
	}
}

func directUsageCommand() RecordUsageCommand {
	start := time.Unix(1_000, 0).UTC()
	stageAttemptID := uuid.New()
	return RecordUsageCommand{
		ID: uuid.New(), SchemaVersion: 1,
		SourceKind: SourceStageAttempt, SourceAuthorityID: stageAttemptID,
		ReceiptDigest: sha256.Sum256([]byte("signed-stage-runtime-receipt")),
		Attribution:   AttributionDirect, UsageClass: UsageExecution,
		ResourceKind: ResourceGPUNanosecond, Quantity: 8_000_000_000,
		OrganizationID: uuid.New(), ProjectID: uuid.New(), JobID: uuid.New(),
		AttemptID: uuid.New(), StageAttemptID: stageAttemptID,
		IntervalStart: start, IntervalEnd: start.Add(8 * time.Second),
		RecordedAt: start.Add(9 * time.Second),
	}
}

type memoryBackend struct {
	usage       map[uuid.UUID]RecordUsageCommand
	byIdentity  map[string]uuid.UUID
	allocations []CostAllocation
	summary     OperatorSummary
}

func (backend *memoryBackend) RecordUsage(
	_ context.Context,
	command RecordUsageCommand,
) (UsageIdentity, error) {
	if backend.usage == nil {
		backend.usage = make(map[uuid.UUID]RecordUsageCommand)
		backend.byIdentity = make(map[string]uuid.UUID)
	}
	key := string(command.SourceKind) + "/" + command.SourceAuthorityID.String() + "/" +
		string(command.ResourceKind) + "/" + string(command.ReceiptDigest[:])
	if existingID, ok := backend.byIdentity[key]; ok {
		if backend.usage[existingID] != command {
			return UsageIdentity{}, ErrReplayMismatch
		}
		return UsageIdentity{ID: existingID, Replayed: true}, nil
	}
	backend.usage[command.ID] = command
	backend.byIdentity[key] = command.ID
	return UsageIdentity{ID: command.ID}, nil
}

func (backend *memoryBackend) Value(
	_ context.Context,
	command ValueCommand,
) (CostAllocation, error) {
	allocation := CostAllocation{
		ID: command.AllocationID, UsageID: command.UsageID,
		CostModelRevisionID: command.CostModelRevisionID,
	}
	if len(backend.allocations) > 0 {
		allocation.SupersedesAllocationID = backend.allocations[len(backend.allocations)-1].ID
	}
	backend.allocations = append(backend.allocations, allocation)
	return allocation, nil
}

func (backend *memoryBackend) Summarize(
	_ context.Context,
	_ SummaryQuery,
) (OperatorSummary, error) {
	return backend.summary, nil
}
