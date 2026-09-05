//go:build integration

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stagescheduler"
)

func TestStageSchedulerCommitLocksPoolBeforeCounter(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "commit-pool-lock")
	repository := &deferredStageClaimCommit{PostgresRepository: fixture.repository}
	service, err := stagescheduler.NewService(repository, fixture.coordinator, stagescheduler.Config{
		SchedulerID: "commit-lock-order", ClaimTTL: 30 * time.Second,
		LeaseTTL: time.Minute, LocalDeadlineTTL: 50 * time.Second, SigningKeyID: "stage-authority-key-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.Acquire(context.Background(), fixture.authority, fixture.observation); !errors.Is(err, errStageClaimCommitDeferred) {
		t.Fatalf("defer commit after actual claim and assignment: %v", err)
	}
	if repository.claimID == uuid.Nil || repository.stageAttemptID == uuid.Nil {
		t.Fatal("scheduler did not reach claim commit")
	}
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SELECT id FROM capacity_pools WHERE id = $1 FOR UPDATE`, fixture.authority.CapacityPoolID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	committed := make(chan error, 1)
	go func() {
		committed <- fixture.repository.Commit(ctx, repository.claimID, repository.stageAttemptID)
	}()
	waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_stage_scheduler_login")
	// A READY publication holds the parent CapacityPool before its queue
	// trigger updates the counter. Claim commit must wait before taking it.
	if _, err := tx.Exec(`SELECT capacity_pool_id FROM stage_capacity_pool_counters
		WHERE capacity_pool_id = $1 FOR UPDATE NOWAIT`, fixture.authority.CapacityPoolID); err != nil {
		t.Fatalf("claim commit locked the counter while waiting for its parent pool: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-committed; err != nil {
		t.Fatalf("commit after parent publication transaction: %v", err)
	}
	var state string
	var accounted bool
	var counterVersion int64
	if err := fixture.database.Admin.QueryRow(`SELECT claim.state, claim.fairness_accounted, counter.version
		FROM stage_scheduler_claims claim JOIN stage_capacity_pool_counters counter
		ON counter.capacity_pool_id = claim.capacity_pool_id WHERE claim.id = $1`, repository.claimID).
		Scan(&state, &accounted, &counterVersion); err != nil {
		t.Fatal(err)
	}
	if state != "COMMITTED" || !accounted {
		t.Fatalf("claim state=%s fairness_accounted=%t", state, accounted)
	}
	if err := fixture.repository.Commit(ctx, repository.claimID, repository.stageAttemptID); err != nil {
		t.Fatalf("exact claim commit replay: %v", err)
	}
	var replayVersion int64
	if err := fixture.database.Admin.QueryRow(`SELECT version FROM stage_capacity_pool_counters
		WHERE capacity_pool_id = $1`, fixture.authority.CapacityPoolID).Scan(&replayVersion); err != nil {
		t.Fatal(err)
	}
	if replayVersion != counterVersion {
		t.Fatal("exact replay repeated capacity/fairness accounting")
	}
}

func TestStageSchedulerClaimCleanupLocksPoolBeforeCounter(t *testing.T) {
	for _, operation := range []string{"abandon", "expire", "recover-assigned"} {
		t.Run(operation, func(t *testing.T) {
			fixture := newStageSchedulerFixture(t, "cleanup-pool-lock-"+operation)
			config := stagescheduler.Config{
				SchedulerID: "cleanup-lock-order", ClaimTTL: 2 * time.Second,
				LeaseTTL: time.Minute, LocalDeadlineTTL: 50 * time.Second, SigningKeyID: "stage-authority-key-v1",
			}
			var claimID uuid.UUID
			if operation == "recover-assigned" {
				repository := &deferredStageClaimCommit{PostgresRepository: fixture.repository}
				service, err := stagescheduler.NewService(repository, fixture.coordinator, config)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := service.Acquire(context.Background(), fixture.authority, fixture.observation); !errors.Is(err, errStageClaimCommitDeferred) {
					t.Fatalf("defer assigned claim commit: %v", err)
				}
				claimID = repository.claimID
			} else {
				service, err := stagescheduler.NewService(fixture.repository, panickingStageCoordinator{}, config)
				if err != nil {
					t.Fatal(err)
				}
				func() {
					defer func() {
						if recovered := recover(); recovered != "simulated StageScheduler process crash" {
							t.Fatalf("expected crash after durable claim, got %v", recovered)
						}
					}()
					_, _, _ = service.Acquire(context.Background(), fixture.authority, fixture.observation)
				}()
				if err := fixture.database.Admin.QueryRow(`SELECT id FROM stage_scheduler_claims
					WHERE stage_run_id = $1`, fixture.stageRunID).Scan(&claimID); err != nil {
					t.Fatal(err)
				}
			}
			if operation != "abandon" {
				var expiresAt time.Time
				if err := fixture.database.Admin.QueryRow(`SELECT claim_expires_at FROM stage_scheduler_claims
					WHERE id = $1`, claimID).Scan(&expiresAt); err != nil {
					t.Fatal(err)
				}
				waitStageExpiry(t, expiresAt)
			}
			tx, err := fixture.database.Admin.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.Exec(`SELECT id FROM capacity_pools WHERE id = $1 FOR UPDATE`, fixture.authority.CapacityPoolID); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				if operation == "abandon" {
					result <- fixture.repository.Abandon(ctx, claimID, stagescheduler.AbandonAssignmentRejected)
					return
				}
				processed, err := fixture.repository.ReconcileExpired(ctx, 10)
				if err == nil && processed != 1 {
					err = errors.New("expiry cleanup did not process exactly one durable claim")
				}
				result <- err
			}()
			waitForRoleDatabaseLock(t, fixture.database.Admin, "vela_stage_scheduler_login")
			if _, err := tx.Exec(`SELECT capacity_pool_id FROM stage_capacity_pool_counters
				WHERE capacity_pool_id = $1 FOR UPDATE NOWAIT`, fixture.authority.CapacityPoolID); err != nil {
				t.Fatalf("%s locked the counter while waiting for parent pool: %v", operation, err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := <-result; err != nil {
				t.Fatalf("%s after parent publication transaction: %v", operation, err)
			}
			var state string
			var accounted bool
			if err := fixture.database.Admin.QueryRow(`SELECT state, fairness_accounted
				FROM stage_scheduler_claims WHERE id = $1`, claimID).Scan(&state, &accounted); err != nil {
				t.Fatal(err)
			}
			want := map[string]string{"abandon": "ABANDONED", "expire": "EXPIRED", "recover-assigned": "COMMITTED"}[operation]
			if state != want || accounted != (operation == "recover-assigned") {
				t.Fatalf("cleanup state=%s fairness_accounted=%t", state, accounted)
			}
			if operation == "abandon" {
				if err := fixture.repository.Abandon(ctx, claimID, stagescheduler.AbandonAssignmentRejected); err != nil {
					t.Fatalf("abandon replay: %v", err)
				}
			} else if processed, err := fixture.repository.ReconcileExpired(ctx, 10); err != nil || processed != 0 {
				t.Fatalf("expiry replay processed=%d error=%v", processed, err)
			}
		})
	}
}

var errStageClaimCommitDeferred = errors.New("defer claim commit for independent lock interleaving")

type deferredStageClaimCommit struct {
	*stagescheduler.PostgresRepository
	claimID        uuid.UUID
	stageAttemptID uuid.UUID
}

func (repository *deferredStageClaimCommit) Commit(_ context.Context, claimID, stageAttemptID uuid.UUID) error {
	repository.claimID, repository.stageAttemptID = claimID, stageAttemptID
	return errStageClaimCommitDeferred
}
