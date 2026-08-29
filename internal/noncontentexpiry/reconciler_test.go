package noncontentexpiry

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestNewReconcilerRejectsInvalidConfig(t *testing.T) {
	database := &recordingDatabase{}
	valid := Config{
		InstanceID: "expiry-1", BatchSize: 10,
		ClaimTTL: 30 * time.Second, HeldRetry: time.Minute,
	}
	for _, test := range []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "empty instance", mutate: func(config *Config) { config.InstanceID = "" }},
		{name: "padded instance", mutate: func(config *Config) { config.InstanceID = " expiry" }},
		{name: "control instance", mutate: func(config *Config) { config.InstanceID = "expiry\n" }},
		{name: "zero batch", mutate: func(config *Config) { config.BatchSize = 0 }},
		{name: "large batch", mutate: func(config *Config) { config.BatchSize = 1001 }},
		{name: "fractional claim TTL", mutate: func(config *Config) { config.ClaimTTL = 1500 * time.Millisecond }},
		{name: "large claim TTL", mutate: func(config *Config) { config.ClaimTTL = time.Hour + time.Second }},
		{name: "zero held retry", mutate: func(config *Config) { config.HeldRetry = 0 }},
		{name: "large held retry", mutate: func(config *Config) { config.HeldRetry = 24*time.Hour + time.Second }},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := valid
			test.mutate(&config)
			if reconciler, err := newReconciler(database, config); err == nil || reconciler != nil {
				t.Fatalf("newReconciler() = %#v, %v", reconciler, err)
			}
		})
	}
}

func TestReconcileBatchRecordsExactOutcomesAndStopsAtNoWork(t *testing.T) {
	sourceID := uuid.New()
	receiptID := uuid.New()
	database := &recordingDatabase{rows: []*recordingRow{
		{values: []any{string(kindJobMetadata), sourceID, uuid.Nil}},
		{values: []any{string(outcomeExpired), &receiptID, int32(2)}},
		{values: []any{string(kindJobFinancial), sourceID, uuid.Nil}},
		{values: []any{string(outcomeHeld), (*uuid.UUID)(nil), int32(0)}},
		{err: pgx.ErrNoRows},
	}}
	reconciler, err := newReconciler(database, Config{
		InstanceID: "expiry-1", BatchSize: 10,
		ClaimTTL: 30 * time.Second, HeldRetry: time.Minute,
	})
	if err != nil {
		t.Fatalf("configure Reconciler: %v", err)
	}

	result, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || result != (Result{Claimed: 2, Expired: 1, Held: 1}) {
		t.Fatalf("ReconcileBatch() = %#v, %v", result, err)
	}
	if len(database.queries) != 5 ||
		!strings.Contains(database.queries[0], "vela_claim_non_content_expiry") ||
		!strings.Contains(database.queries[1], "vela_complete_non_content_expiry") {
		t.Fatalf("queries = %#v", database.queries)
	}
}

func TestReconcileBatchCountsStaleCompletion(t *testing.T) {
	database := &recordingDatabase{rows: []*recordingRow{
		{values: []any{string(kindOrganizationFinancial), uuid.New(), uuid.Nil}},
		{values: []any{string(outcomeStale), (*uuid.UUID)(nil), int32(0)}},
	}}
	reconciler, err := newReconciler(database, Config{
		InstanceID: "expiry-1", BatchSize: 1,
		ClaimTTL: time.Second, HeldRetry: time.Second,
	})
	if err != nil {
		t.Fatalf("configure Reconciler: %v", err)
	}
	result, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || result != (Result{Claimed: 1, Stale: 1}) {
		t.Fatalf("ReconcileBatch() = %#v, %v", result, err)
	}
}

func TestReconcileBatchFailsClosedOnInvalidAuthorityOrDatabaseError(t *testing.T) {
	for _, test := range []struct {
		name string
		rows []*recordingRow
	}{
		{name: "claim error", rows: []*recordingRow{{err: context.DeadlineExceeded}}},
		{name: "claim identity", rows: []*recordingRow{{values: []any{"UNKNOWN", uuid.New(), uuid.Nil}}}},
		{
			name: "completion error",
			rows: []*recordingRow{
				{values: []any{string(kindJobMetadata), uuid.New(), uuid.Nil}},
				{err: context.DeadlineExceeded},
			},
		},
		{
			name: "invalid expired result",
			rows: []*recordingRow{
				{values: []any{string(kindJobMetadata), uuid.New(), uuid.Nil}},
				{values: []any{string(outcomeExpired), (*uuid.UUID)(nil), int32(0)}},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			reconciler, err := newReconciler(&recordingDatabase{rows: test.rows}, Config{
				InstanceID: "expiry-1", BatchSize: 1,
				ClaimTTL: time.Second, HeldRetry: time.Second,
			})
			if err != nil {
				t.Fatalf("configure Reconciler: %v", err)
			}
			if _, err := reconciler.ReconcileBatch(context.Background()); err == nil {
				t.Fatal("ReconcileBatch() error = nil")
			}
		})
	}
}

type recordingDatabase struct {
	rows    []*recordingRow
	queries []string
}

func (database *recordingDatabase) QueryRow(
	_ context.Context,
	query string,
	arguments ...any,
) pgx.Row {
	database.queries = append(database.queries, query)
	row := database.rows[0]
	database.rows = database.rows[1:]
	if len(arguments) >= 2 {
		if generated, ok := arguments[1].(uuid.UUID); ok {
			for index, value := range row.values {
				if id, ok := value.(uuid.UUID); ok && id == uuid.Nil {
					row.values[index] = generated
				}
			}
		}
	}
	return row
}

type recordingRow struct {
	values []any
	err    error
}

func (row *recordingRow) Scan(destinations ...any) error {
	if row.err != nil {
		return row.err
	}
	if len(row.values) != len(destinations) {
		return errors.New("unexpected scan shape")
	}
	for index, value := range row.values {
		destination := reflect.ValueOf(destinations[index])
		if destination.Kind() != reflect.Pointer {
			return errors.New("scan destination is not a pointer")
		}
		destination.Elem().Set(reflect.ValueOf(value))
	}
	return nil
}
