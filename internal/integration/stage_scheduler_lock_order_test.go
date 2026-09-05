//go:build integration

package integration_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/stagescheduler"
)

func TestStageSchedulerSnapshotLocksPoolBeforeCounter(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "snapshot-pool-lock")
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SELECT id FROM capacity_pools WHERE id = $1 FOR UPDATE`,
		fixture.authority.CapacityPoolID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, captureErr := fixture.repository.Capture(ctx, fixture.authority, fixture.observation)
		result <- captureErr
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_stage_scheduler_login")
	// Admission holds the pool before its queue trigger updates the counter.
	// A blocked snapshot must not prevent that parent transaction from finishing.
	if _, err := tx.Exec(`SELECT capacity_pool_id FROM stage_capacity_pool_counters
		WHERE capacity_pool_id = $1 FOR UPDATE NOWAIT`, fixture.authority.CapacityPoolID); err != nil {
		t.Fatalf("snapshot locked the counter while waiting for its parent pool: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatalf("capture snapshot after parent transaction committed: %v", err)
	}
}

func TestStageSchedulerClaimLocksPoolBeforeCounter(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "claim-pool-lock")
	var claim stagescheduler.ClaimRequest
	stop := new(int)
	repository := &tamperingStageRepository{
		PostgresRepository: fixture.repository,
		tamper: func(request *stagescheduler.ClaimRequest) {
			claim = *request
			panic(stop)
		},
	}
	service, err := stagescheduler.NewService(repository, fixture.coordinator, stagescheduler.Config{
		SchedulerID: "claim-lock-order", ClaimTTL: 30 * time.Second,
		LeaseTTL: time.Minute, LocalDeadlineTTL: 50 * time.Second, SigningKeyID: "stage-authority-key-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recovered := recover(); recovered != stop {
				t.Fatalf("scheduler did not produce a claim: %v", recovered)
			}
		}()
		_, _, _ = service.Acquire(context.Background(), fixture.authority, fixture.observation)
	}()
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SELECT id FROM capacity_pools WHERE id = $1 FOR UPDATE`,
		fixture.authority.CapacityPoolID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, claimErr := fixture.repository.Claim(ctx, claim)
		result <- claimErr
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_stage_scheduler_login")
	if _, err := tx.Exec(`SELECT capacity_pool_id FROM stage_capacity_pool_counters
		WHERE capacity_pool_id = $1 FOR UPDATE NOWAIT`, fixture.authority.CapacityPoolID); err != nil {
		t.Fatalf("claim locked the counter while waiting for its parent pool: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatalf("claim after parent transaction committed: %v", err)
	}
}

func TestStageSchedulerSnapshotLockMigrationPreservesAuthority(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 79)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	var original string
	var originalOID uint32
	if err := database.Admin.QueryRow(`SELECT pg_get_functiondef(
		'vela_capture_stage_scheduler_snapshot(jsonb)'::regprocedure) || pg_get_functiondef(
		'vela_claim_stage_scheduler_decision(jsonb)'::regprocedure),
		'vela_capture_stage_scheduler_snapshot(jsonb)'::regprocedure::oid`).Scan(&original, &originalOID); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := goose.UpTo(database.Admin, migrations, 80); err != nil {
			t.Fatal(err)
		}
		var oid uint32
		var runtimeWrite, runtimeExecute, ownerLock bool
		if err := database.Admin.QueryRow(`SELECT
			'vela_capture_stage_scheduler_snapshot(jsonb)'::regprocedure::oid,
			has_column_privilege('vela_stage_scheduler', 'capacity_pools', 'id', 'UPDATE'),
			has_function_privilege('vela_stage_scheduler', 'vela_capture_stage_scheduler_snapshot(jsonb)', 'EXECUTE'),
			has_column_privilege('vela_stage_scheduler_owner', 'capacity_pools', 'id', 'UPDATE')`).
			Scan(&oid, &runtimeWrite, &runtimeExecute, &ownerLock); err != nil {
			t.Fatal(err)
		}
		if oid != originalOID || runtimeWrite || !runtimeExecute || !ownerLock {
			t.Fatalf("snapshot authority oid=%d runtimeWrite=%t runtimeExecute=%t ownerLock=%t",
				oid, runtimeWrite, runtimeExecute, ownerLock)
		}
		if err := goose.DownTo(database.Admin, migrations, 79); err != nil {
			t.Fatal(err)
		}
		var restored string
		if err := database.Admin.QueryRow(`SELECT pg_get_functiondef(
			'vela_capture_stage_scheduler_snapshot(jsonb)'::regprocedure) || pg_get_functiondef(
			'vela_claim_stage_scheduler_decision(jsonb)'::regprocedure),
			has_column_privilege('vela_stage_scheduler_owner', 'capacity_pools', 'id', 'UPDATE')`).
			Scan(&restored, &ownerLock); err != nil {
			t.Fatal(err)
		}
		if restored != original || ownerLock {
			t.Fatal("snapshot rollback did not restore the function and owner privilege")
		}
	}
}
