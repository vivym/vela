//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/recovery"
)

func TestWorkerBootstrapAbandonmentFencesAndPermanentlyRejectsCompletion(t *testing.T) {
	database, service, request := newWorkerBootstrapFixture(t)
	claim, err := service.ClaimWorkerBootstrap(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	lookup := fleet.WorkerBootstrapLookup{RequestID: request.RequestID, NodeIdentity: claim.NodeIdentity, ActorIdentity: request.ActorIdentity}
	before, err := service.LookupWorkerBootstrap(t.Context(), lookup)
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"request", "node", "actor"} {
		wrong := lookup
		switch fault {
		case "request":
			wrong.RequestID = uuid.New()
		case "node":
			wrong.NodeIdentity = "another-node"
		case "actor":
			wrong.ActorIdentity = "another-agent"
		}
		result, err := service.AbandonWorkerBootstrap(t.Context(), wrong)
		assertFleetFailure(t, err, fleet.FailureNotFound)
		if !reflect.DeepEqual(result, fleet.WorkerBootstrapHistory{}) {
			t.Fatal("unauthorized abandonment exposed history")
		}
	}
	connection := recoveryConnection(t, database)
	operationID := uuid.New()
	if _, err := connection.Exec(t.Context(), `SELECT vela_close_recovery_admission($1)`, operationID); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Exec(t.Context(), `SELECT vela_seal_recovery_quiescence($1)`, operationID); err == nil {
		t.Fatal("pending first use was accepted as database quiescence")
	}
	first, err := service.AbandonWorkerBootstrap(t.Context(), lookup)
	if err != nil || first.Abandonment == nil || first.Abandonment.FencedInstanceEpoch != 2 || first.Abandonment.AbandonedAt.IsZero() ||
		first.Receipt != nil || !first.RecordedAt.IsZero() || !reflect.DeepEqual(first.Claim, before.Claim) {
		t.Fatalf("abandonment changed the claim or failed to terminate authority: %+v %v", first, err)
	}
	assertBootstrapWorkerState(t, database, request.WorkerInstanceID, "FENCED", 2)
	again, err := service.AbandonWorkerBootstrap(t.Context(), lookup)
	if err != nil || !reflect.DeepEqual(first, again) {
		t.Fatalf("abandonment replay changed its original outcome: %+v %v", again, err)
	}
	if _, err := service.RecordWorkerBootstrapReceipt(t.Context(), bootstrapReceipt(request)); err == nil {
		t.Fatal("abandoned claim accepted a delayed receipt")
	}
	if replay, err := service.ClaimWorkerBootstrap(t.Context(), request); err != nil || replay.Fresh {
		t.Fatalf("abandonment reissued first use: %+v %v", replay, err)
	}
	newRequest := request
	newRequest.RequestID = uuid.New()
	if _, err := service.ClaimWorkerBootstrap(t.Context(), newRequest); err == nil {
		t.Fatal("abandoned member acquired replacement initialization")
	}
	sealed, err := recovery.Quiesce(t.Context(), connection, operationID, time.Millisecond)
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil {
		t.Fatal(versionErr)
	}
	if err != nil || sealed.SchemaVersion != version || sealed.Inventory["worker_bootstrap_claims"] != 0 {
		t.Fatalf("permanently rejected claim prevented database quiescence: %+v %v", sealed, err)
	}
	for _, statement := range []string{"UPDATE worker_bootstrap_abandonments SET fenced_instance_epoch=3", "DELETE FROM worker_bootstrap_abandonments", "TRUNCATE worker_bootstrap_abandonments"} {
		if _, err := database.Admin.Exec(statement); err == nil || !strings.Contains(err.Error(), "immutable") {
			t.Fatalf("abandonment history was mutable: %s: %v", statement, err)
		}
	}
	role := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	receipt := bootstrapReceipt(request)
	if _, err := role.Exec(t.Context(), `SELECT vela_record_worker_bootstrap_receipt_v93($1,$2,$3,$4,$5,$6)`, receipt.RequestID,
		receipt.WorkerJournalID, receipt.WorkerScope, receipt.RuntimeJournalID, receipt.RuntimeScope, receipt.ActorIdentity); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("Fleet bypassed terminal outcome through old function: %v", err)
	}
	if err := goose.DownTo(database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), 93); err == nil || !strings.Contains(err.Error(), "abandonment history prohibits rollback") {
		t.Fatalf("terminal abandonment history rolled back: %v", err)
	}
}

func TestWorkerBootstrapAbandonmentRejectsRecordedAndObservedWorkers(t *testing.T) {
	for _, state := range []string{"recorded", "observed", "observed-fenced", "unobserved-fenced"} {
		t.Run(state, func(t *testing.T) {
			database, service, request := newWorkerBootstrapFixture(t)
			claim, err := service.ClaimWorkerBootstrap(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			if state == "recorded" {
				if _, err := service.RecordWorkerBootstrapReceipt(t.Context(), bootstrapReceipt(request)); err != nil {
					t.Fatal(err)
				}
			}
			if state == "observed" || state == "observed-fenced" {
				if _, err := service.Observe(t.Context(), workerRegistryEvidenceValue(t, request.WorkerInstanceID, 0x51)); err != nil {
					t.Fatal(err)
				}
			}
			if strings.HasSuffix(state, "-fenced") {
				if _, err := service.Fence(t.Context(), fleet.WorkerInstanceFenceRequest{WorkerInstanceID: request.WorkerInstanceID,
					ExpectedInstanceEpoch: 1, Reason: "test first-use fencing", ObservedBy: "fleet/test"}); err != nil {
					t.Fatal(err)
				}
			}
			result, err := service.AbandonWorkerBootstrap(t.Context(), fleet.WorkerBootstrapLookup{
				RequestID: request.RequestID, NodeIdentity: claim.NodeIdentity, ActorIdentity: request.ActorIdentity})
			if state == "unobserved-fenced" {
				if err != nil || result.Abandonment == nil || result.Abandonment.FencedInstanceEpoch != 2 {
					t.Fatalf("already fenced first use did not terminate: %+v %v", result, err)
				}
				assertBootstrapWorkerState(t, database, request.WorkerInstanceID, "FENCED", 2)
				_, err = service.Observe(t.Context(), workerRegistryEvidenceValue(t, request.WorkerInstanceID, 0x51))
				assertFleetFailure(t, err, fleet.FailureConflict)
			} else {
				assertFleetFailure(t, err, fleet.FailureConflict)
				var count int
				if err := database.Admin.QueryRow("SELECT count(*) FROM worker_bootstrap_abandonments").Scan(&count); err != nil || count != 0 {
					t.Fatalf("unsupported abandonment changed authority: %d %v", count, err)
				}
			}
		})
	}
}

func TestWorkerBootstrapAbandonmentSerializesWithReceipt(t *testing.T) {
	database, service, request := newWorkerBootstrapFixture(t)
	claim, err := service.ClaimWorkerBootstrap(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	type outcome struct {
		abandon bool
		err     error
	}
	results := make(chan outcome, 8)
	var group sync.WaitGroup
	for i := range 8 {
		group.Go(func() {
			<-start
			var err error
			if i%2 == 0 {
				_, err = service.AbandonWorkerBootstrap(ctx, fleet.WorkerBootstrapLookup{RequestID: request.RequestID, NodeIdentity: claim.NodeIdentity, ActorIdentity: request.ActorIdentity})
			} else {
				_, err = service.RecordWorkerBootstrapReceipt(ctx, bootstrapReceipt(request))
			}
			results <- outcome{abandon: i%2 == 0, err: err}
		})
	}
	close(start)
	group.Wait()
	close(results)
	var abandoned, recorded int
	for result := range results {
		if result.err != nil {
			assertFleetFailure(t, result.err, fleet.FailureConflict)
		} else if result.abandon {
			abandoned++
		} else {
			recorded++
		}
	}
	if (abandoned == 0) == (recorded == 0) {
		t.Fatalf("concurrent receipt and abandonment did not select one outcome: abandoned=%d recorded=%d", abandoned, recorded)
	}
	var count int
	if err := database.Admin.QueryRow(`SELECT (SELECT count(*) FROM worker_bootstrap_receipts) + (SELECT count(*) FROM worker_bootstrap_abandonments)`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("competing terminal outcomes were both persisted: %d %v", count, err)
	}
}

func TestWorkerBootstrapAbandonmentRequiresCommitQuorumAndSupportsEmptyRollback(t *testing.T) {
	database, ordinary, request := newWorkerBootstrapFixture(t)
	claim, err := ordinary.ClaimWorkerBootstrap(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	enableStageQuorumRequirement(t, database)
	guarded, err := fleet.NewService(newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password"))
	if err != nil {
		t.Fatal(err)
	}
	lookup := fleet.WorkerBootstrapLookup{RequestID: request.RequestID, NodeIdentity: claim.NodeIdentity, ActorIdentity: request.ActorIdentity}
	result, err := guarded.AbandonWorkerBootstrap(t.Context(), lookup)
	if err == nil || !strings.Contains(err.Error(), "synchronous replication quorum is unavailable") || !reflect.DeepEqual(result, fleet.WorkerBootstrapHistory{}) {
		t.Fatalf("uncommitted abandonment escaped as terminal: %+v %v", result, err)
	}
	assertBootstrapWorkerState(t, database, request.WorkerInstanceID, "PROVISIONING", 1)
	history, err := ordinary.LookupWorkerBootstrap(t.Context(), lookup)
	if err != nil || history.Abandonment != nil || history.Receipt != nil {
		t.Fatalf("failed abandonment left terminal history: %+v %v", history, err)
	}
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 93); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(database.Admin, migrations, 94); err != nil {
		t.Fatal(err)
	}
	if after, err := ordinary.LookupWorkerBootstrap(t.Context(), lookup); err != nil || !reflect.DeepEqual(history, after) {
		t.Fatalf("migration changed retained claim: %+v %v", after, err)
	}
}

func bootstrapReceipt(request fleet.WorkerBootstrapRequest) fleet.WorkerBootstrapReceipt {
	return fleet.WorkerBootstrapReceipt{RequestID: request.RequestID, ActorIdentity: request.ActorIdentity,
		WorkerJournalID: uuid.NewSHA1(request.RequestID, []byte("worker")), RuntimeJournalID: uuid.NewSHA1(request.RequestID, []byte("runtime")),
		WorkerScope: bytes.Repeat([]byte{1}, 32), RuntimeScope: bytes.Repeat([]byte{2}, 32)}
}

func assertBootstrapWorkerState(t *testing.T, database testDatabase, id uuid.UUID, state string, epoch int64) {
	t.Helper()
	var actualState string
	var actualEpoch int64
	if err := database.Admin.QueryRow("SELECT lifecycle_state, instance_epoch FROM worker_instances WHERE id=$1", id).Scan(&actualState, &actualEpoch); err != nil || actualState != state || actualEpoch != epoch {
		t.Fatalf("unexpected Worker state: %s %d %v", actualState, actualEpoch, err)
	}
}
