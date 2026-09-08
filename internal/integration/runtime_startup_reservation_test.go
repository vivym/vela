//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/recovery"
)

func runtimeStartupRequest(t *testing.T, service *fleet.Service, bootstrap fleet.WorkerBootstrapRequest) fleet.RuntimeStartupRequest {
	t.Helper()
	if claim, err := service.ClaimWorkerBootstrap(t.Context(), bootstrap); err != nil || !claim.Fresh {
		t.Fatalf("bootstrap claim: %+v %v", claim, err)
	}
	receipt := fleet.WorkerBootstrapReceipt{RequestID: bootstrap.RequestID, ActorIdentity: bootstrap.ActorIdentity,
		WorkerJournalID: uuid.New(), RuntimeJournalID: uuid.New(),
		WorkerScope: bytes.Repeat([]byte{0x31}, 32), RuntimeScope: bytes.Repeat([]byte{0x32}, 32)}
	if _, err := service.RecordWorkerBootstrapReceipt(t.Context(), receipt); err != nil {
		t.Fatal(err)
	}
	document, err := fleetcontroller.ParseWorkerBundleActuationManifest(bootstrap.BundleManifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := fleetcontroller.WorkerMemberLaunchManifest(document, bootstrap.WorkerInstanceID, bootstrap.WorkerMemberID)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := modelruntime.EncodeLaunchManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	bindings, err := modelruntime.RemoteStartupBindings(manifest)
	if err != nil {
		t.Fatal(err)
	}
	request := fleet.RuntimeStartupRequest{RequestID: uuid.New(), BootstrapRequestID: bootstrap.RequestID,
		NodeIdentity: "h3-node-01", ActorIdentity: bootstrap.ActorIdentity, RuntimeJournalID: receipt.RuntimeJournalID,
		RuntimeScope: receipt.RuntimeScope, IncarnationID: uuid.New(), LaunchDigest: sha256Bytes(wire),
		OwnerObservationDigest: sha256Bytes([]byte("fixture owner observation, not kernel attestation"))}
	for _, binding := range bindings {
		request.Epochs = append(request.Epochs, fleet.RuntimeStartupEpoch{ModelResidencyID: uuid.MustParse(binding.ModelResidencyID),
			ModelRuntimeIdentity: binding.ModelRuntimeIdentity, StageProfileRevisionID: uuid.MustParse(binding.StageProfileRevisionID),
			ModelRuntimeEpoch: binding.ModelRuntimeEpoch})
	}
	return request
}

func TestRuntimeStartupReservationConcurrentFirstUseAndHistory(t *testing.T) {
	database, service, bootstrap := newWorkerBootstrapFixture(t)
	request := runtimeStartupRequest(t, service, bootstrap)
	const contenders = 16
	results := make(chan fleet.RuntimeStartupReservation, contenders)
	errorsSeen := make(chan error, contenders)
	var group sync.WaitGroup
	for range contenders {
		group.Go(func() {
			result, err := service.ReserveRuntimeStartup(t.Context(), request)
			results <- result
			errorsSeen <- err
		})
	}
	group.Wait()
	close(results)
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatal(err)
		}
	}
	fresh := 0
	var timestamp time.Time
	for result := range results {
		if result.Fresh {
			fresh++
		}
		if timestamp.IsZero() {
			timestamp = result.ReservedAt
		}
		if !result.ReservedAt.Equal(timestamp) || !reflect.DeepEqual(result.RuntimeStartupRequest, request) {
			t.Fatalf("reservation identity changed: %+v", result)
		}
	}
	if fresh != 1 {
		t.Fatalf("concurrent fresh reservations = %d, want 1", fresh)
	}
	// Discard the first response and reconnect, as after client transport loss.
	// No wire-loss injection or same-process Node reconciliation is claimed here.
	restarted, err := fleet.NewService(newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password"))
	if err != nil {
		t.Fatal(err)
	}
	history, err := restarted.LookupRuntimeStartup(t.Context(), fleet.WorkerBootstrapLookup{
		RequestID: request.RequestID, NodeIdentity: request.NodeIdentity, ActorIdentity: request.ActorIdentity})
	if err != nil || history.Fresh || !history.ReservedAt.Equal(timestamp) || !reflect.DeepEqual(history.RuntimeStartupRequest, request) {
		t.Fatalf("lost response history minted authority or changed identity: %+v %v", history, err)
	}
	replay, err := restarted.ReserveRuntimeStartup(t.Context(), request)
	if err != nil || replay.Fresh {
		t.Fatalf("reconnected retry minted permission: %+v %v", replay, err)
	}
	for name, mutate := range map[string]func(*fleet.RuntimeStartupRequest){
		"new request": func(r *fleet.RuntimeStartupRequest) { r.RequestID = uuid.New() },
		"incarnation": func(r *fleet.RuntimeStartupRequest) { r.IncarnationID = uuid.New() },
		"owner":       func(r *fleet.RuntimeStartupRequest) { r.OwnerObservationDigest = sha256Bytes([]byte("replacement")) },
		"launch":      func(r *fleet.RuntimeStartupRequest) { r.LaunchDigest = sha256Bytes([]byte("changed launch")) },
		"journal":     func(r *fleet.RuntimeStartupRequest) { r.RuntimeJournalID = uuid.New() },
		"scope":       func(r *fleet.RuntimeStartupRequest) { r.RuntimeScope = sha256Bytes([]byte("changed scope")) },
		"epoch":       func(r *fleet.RuntimeStartupRequest) { r.Epochs[0].ModelRuntimeEpoch++ },
	} {
		t.Run(name, func(t *testing.T) {
			changed := request
			changed.Epochs = slices.Clone(request.Epochs)
			mutate(&changed)
			result, err := service.ReserveRuntimeStartup(t.Context(), changed)
			assertFleetFailure(t, err, fleet.FailureConflict)
			if result.Fresh {
				t.Fatal("changed request received fresh reservation")
			}
		})
	}
	for _, identity := range []fleet.WorkerBootstrapLookup{
		{RequestID: request.RequestID, NodeIdentity: "other-node", ActorIdentity: request.ActorIdentity},
		{RequestID: request.RequestID, NodeIdentity: request.NodeIdentity, ActorIdentity: "other-actor"},
		{RequestID: uuid.New(), NodeIdentity: request.NodeIdentity, ActorIdentity: request.ActorIdentity},
	} {
		_, err := service.LookupRuntimeStartup(t.Context(), identity)
		assertFleetFailure(t, err, fleet.FailureNotFound)
	}
	for _, command := range []string{"UPDATE runtime_startup_reservations SET reserved_at = clock_timestamp()",
		"DELETE FROM runtime_startup_reservations", "TRUNCATE runtime_startup_reservations"} {
		if _, err := database.Admin.Exec(command); err == nil || !strings.Contains(err.Error(), "immutable") {
			t.Fatalf("mutable reservation history: %s: %v", command, err)
		}
	}
	role := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	for _, command := range []string{"SELECT * FROM runtime_startup_reservations", "DELETE FROM runtime_startup_reservations",
		"SELECT vela_recovery_inventory_v94()"} {
		if _, err := role.Exec(t.Context(), command); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("Fleet bypassed restricted API: %s: %v", command, err)
		}
	}
	if err := goose.DownTo(database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), 94); err == nil ||
		!strings.Contains(err.Error(), "reservation history prohibits rollback") {
		t.Fatalf("reservation authority rolled back: %v", err)
	}
}

func TestRuntimeStartupReservationRejectsUnapprovedVector(t *testing.T) {
	_, service, bootstrap := newWorkerBootstrapFixture(t)
	request := runtimeStartupRequest(t, service, bootstrap)
	for name, mutate := range map[string]func(*fleet.RuntimeStartupRequest){
		"epoch":     func(r *fleet.RuntimeStartupRequest) { r.Epochs[0].ModelRuntimeEpoch++ },
		"residency": func(r *fleet.RuntimeStartupRequest) { r.Epochs[0].ModelResidencyID = uuid.New() },
		"profile":   func(r *fleet.RuntimeStartupRequest) { r.Epochs[0].StageProfileRevisionID = uuid.New() },
		"runtime":   func(r *fleet.RuntimeStartupRequest) { r.Epochs[0].ModelRuntimeIdentity = "unapproved" },
		"extra": func(r *fleet.RuntimeStartupRequest) {
			extra := r.Epochs[0]
			extra.ModelResidencyID = uuid.New()
			r.Epochs = append(r.Epochs, extra)
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := request
			changed.Epochs = slices.Clone(request.Epochs)
			mutate(&changed)
			_, err := service.ReserveRuntimeStartup(t.Context(), changed)
			assertFleetFailure(t, err, fleet.FailureConflict)
		})
	}
	if result, err := service.ReserveRuntimeStartup(t.Context(), request); err != nil || !result.Fresh {
		t.Fatalf("invalid proposals consumed first use: %+v %v", result, err)
	}
}

func TestRuntimeStartupReservationRejectsFencedAndChangedBundle(t *testing.T) {
	for _, state := range []string{"fenced", "observed", "bundle-retired", "bundle-digest"} {
		t.Run(state, func(t *testing.T) {
			database, service, bootstrap := newWorkerBootstrapFixture(t)
			request := runtimeStartupRequest(t, service, bootstrap)
			var err error
			switch state {
			case "fenced":
				_, err = service.Fence(t.Context(), fleet.WorkerInstanceFenceRequest{WorkerInstanceID: bootstrap.WorkerInstanceID,
					ExpectedInstanceEpoch: 1, Reason: "startup-test", ObservedBy: request.ActorIdentity})
			case "observed":
				_, err = database.Admin.Exec("UPDATE worker_instances SET observed_at=clock_timestamp() WHERE id=$1", bootstrap.WorkerInstanceID)
			case "bundle-retired":
				_, err = database.Admin.Exec("UPDATE worker_bundles SET lifecycle_state='RETIRED'")
			case "bundle-digest":
				_, err = database.Admin.Exec("UPDATE worker_bundles SET layout_digest=$1", sha256Bytes([]byte("different bundle")))
			}
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.ReserveRuntimeStartup(t.Context(), request)
			assertFleetFailure(t, err, fleet.FailureConflict)
		})
	}
}

func TestRuntimeStartupReservationRequiresCommittedQuorum(t *testing.T) {
	database, ordinary, bootstrap := newWorkerBootstrapFixture(t)
	request := runtimeStartupRequest(t, ordinary, bootstrap)
	enableStageQuorumRequirement(t, database)
	guarded, err := fleet.NewService(newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := guarded.ReserveRuntimeStartup(t.Context(), request)
	if err == nil || !strings.Contains(err.Error(), "synchronous replication quorum is unavailable") ||
		!reflect.DeepEqual(result, fleet.RuntimeStartupReservation{}) {
		t.Fatalf("uncommitted reservation escaped: %+v %v", result, err)
	}
	var count int
	if err := database.Admin.QueryRow("SELECT count(*) FROM runtime_startup_reservations").Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed commit retained reservation: %d %v", count, err)
	}
	if result, err := ordinary.ReserveRuntimeStartup(t.Context(), request); err != nil || !result.Fresh {
		t.Fatalf("rolled back proposal consumed first use: %+v %v", result, err)
	}
}

func TestRuntimeStartupReservationAUXVectorIsAtomic(t *testing.T) {
	database, service, bootstrap := newConfiguredWorkerBootstrapFixture(t, func(plan *fleet.ApprovedResidencyPlan, bundle *fleetcontroller.WorkerBundleActuation) {
		worker := &bundle.WorkerInstances[0]
		worker.Role, worker.SharedSlotException = "aux", "H3_AUX_ENCODER_VAE"
		encoder := &worker.ModelRuntimes[0]
		encoder.Component, encoder.RuntimeIdentity = "ENCODER", "cpu-encoder"
		encoder.StageProfileRevisionID = uuid.MustParse("49000000-0000-0000-0000-000000000040")
		encoder.ModelRuntimeEpochFloor = 7
		plan.CapacityPools[0].StageProfileRevisionID = encoder.StageProfileRevisionID
		plan.WorkerInstances[0].ModelRuntimeRoutes[0].StageProfileRevisionID = encoder.StageProfileRevisionID
		decoder := *encoder
		decoder.Component, decoder.RuntimeIdentity = "VAE_DECODER", "cpu-decoder"
		decoder.ModelResidencyID, decoder.CapacityPoolID = uuid.New(), uuid.New()
		decoder.StageProfileRevisionID = uuid.MustParse("49000000-0000-0000-0000-000000000042")
		decoder.ModelRuntimeEpochFloor = 19
		worker.ModelRuntimes = append(worker.ModelRuntimes, decoder)
		pool := plan.CapacityPools[0]
		pool.ID, pool.StableID, pool.StageProfileRevisionID = decoder.CapacityPoolID, "startup-vae-pool", decoder.StageProfileRevisionID
		plan.CapacityPools = append(plan.CapacityPools, pool)
		plan.WorkerInstances[0].ModelRuntimeRoutes = append(plan.WorkerInstances[0].ModelRuntimeRoutes,
			fleet.PlannedModelRuntimeRoute{ModelResidencyID: decoder.ModelResidencyID,
				CapacityPoolID: decoder.CapacityPoolID, StageProfileRevisionID: decoder.StageProfileRevisionID})
	})
	request := runtimeStartupRequest(t, service, bootstrap)
	if len(request.Epochs) != 2 || request.Epochs[0].ModelRuntimeEpoch != 8 || request.Epochs[1].ModelRuntimeEpoch != 20 {
		t.Fatalf("fixture lost independent AUX floors: %+v", request.Epochs)
	}
	partial := request
	partial.Epochs = request.Epochs[:1]
	if _, err := service.ReserveRuntimeStartup(t.Context(), partial); err == nil {
		t.Fatal("partial AUX reservation accepted")
	}
	wrong := request
	wrong.Epochs = slices.Clone(request.Epochs)
	wrong.Epochs[1].ModelRuntimeEpoch = wrong.Epochs[0].ModelRuntimeEpoch
	if _, err := service.ReserveRuntimeStartup(t.Context(), wrong); err == nil {
		t.Fatal("shared AUX epoch accepted")
	}
	var count int
	if err := database.Admin.QueryRow("SELECT count(*) FROM runtime_startup_reservations").Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial reservation consumed authority: %d %v", count, err)
	}
	result, err := service.ReserveRuntimeStartup(t.Context(), request)
	if err != nil || !result.Fresh || len(result.Epochs) != 2 {
		t.Fatalf("complete AUX vector: %+v %v", result, err)
	}
	slices.Reverse(request.Epochs)
	replay, err := service.ReserveRuntimeStartup(t.Context(), request)
	if err != nil || replay.Fresh || !reflect.DeepEqual(result.Epochs, replay.Epochs) {
		t.Fatalf("vector ordering changed reservation: %+v %v", replay, err)
	}
}

func TestRuntimeStartupReservationRequiresJournalReceipt(t *testing.T) {
	_, service, bootstrap := newWorkerBootstrapFixture(t)
	if _, err := service.ClaimWorkerBootstrap(t.Context(), bootstrap); err != nil {
		t.Fatal(err)
	}
	request := fleet.RuntimeStartupRequest{RequestID: uuid.New(), BootstrapRequestID: bootstrap.RequestID,
		NodeIdentity: "h3-node-01", ActorIdentity: bootstrap.ActorIdentity, RuntimeJournalID: uuid.New(), IncarnationID: uuid.New(),
		RuntimeScope: sha256Bytes([]byte("scope")), LaunchDigest: sha256Bytes([]byte("launch")),
		OwnerObservationDigest: sha256Bytes([]byte("owner")), Epochs: []fleet.RuntimeStartupEpoch{{
			ModelResidencyID: uuid.New(), StageProfileRevisionID: uuid.New(), ModelRuntimeIdentity: "fixture", ModelRuntimeEpoch: 2}}}
	_, err := service.ReserveRuntimeStartup(t.Context(), request)
	assertFleetFailure(t, err, fleet.FailureConflict)
	if _, err := service.AbandonWorkerBootstrap(t.Context(), fleet.WorkerBootstrapLookup{
		RequestID: bootstrap.RequestID, NodeIdentity: request.NodeIdentity, ActorIdentity: request.ActorIdentity}); err != nil {
		t.Fatal(err)
	}
	_, err = service.ReserveRuntimeStartup(t.Context(), request)
	assertFleetFailure(t, err, fleet.FailureConflict)
}

func TestRuntimeStartupReservationWaitsForConcurrentFence(t *testing.T) {
	database, service, bootstrap := newWorkerBootstrapFixture(t)
	request := runtimeStartupRequest(t, service, bootstrap)
	transaction, err := database.Admin.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = transaction.Rollback() }()
	if _, err := transaction.Exec("SELECT vela_fence_worker_instance($1,1,'startup-race','fixture')", bootstrap.WorkerInstanceID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := service.ReserveRuntimeStartup(ctx, request)
		result <- err
	}()
	// Observe actual PostgreSQL lock waiting before releasing the fence commit.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := database.Admin.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE usename='vela_fleet_login' AND wait_event_type='Lock'
			AND query LIKE '%vela_reserve_runtime_startup%')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("reservation never waited on the fencing transaction")
		case err := <-result:
			t.Fatalf("reservation escaped held fence: %v", err)
		case <-ticker.C:
		}
	}
	if err := transaction.Commit(); err != nil {
		t.Fatal(err)
	}
	assertFleetFailure(t, <-result, fleet.FailureConflict)
	var count int
	if err := database.Admin.QueryRow("SELECT count(*) FROM runtime_startup_reservations").Scan(&count); err != nil || count != 0 {
		t.Fatalf("fenced startup retained a reservation: %d %v", count, err)
	}
}

func TestRuntimeStartupReservationRecoveryGateAndEmptyMigration(t *testing.T) {
	database, service, bootstrap := newWorkerBootstrapFixture(t)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 94); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(database.Admin, migrations, 95); err != nil {
		t.Fatal(err)
	}
	request := runtimeStartupRequest(t, service, bootstrap)
	connection := recoveryConnection(t, database)
	operationID := uuid.New()
	closed, err := recovery.Quiesce(t.Context(), connection, operationID, time.Millisecond)
	if err != nil || closed.SchemaVersion != 95 || closed.Validate() != nil {
		t.Fatalf("empty reservation quiescence: %+v %v", closed, err)
	}
	if _, err := service.ReserveRuntimeStartup(t.Context(), request); err == nil || !strings.Contains(err.Error(), "closed for recovery") {
		t.Fatalf("startup passed closed recovery gate: %v", err)
	}
	if err := recovery.Reopen(t.Context(), connection, operationID, closed.Generation); err != nil {
		t.Fatal(err)
	}
	if result, err := service.ReserveRuntimeStartup(t.Context(), request); err != nil || !result.Fresh {
		t.Fatalf("reopened gate: %+v %v", result, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if _, err := recovery.Quiesce(ctx, connection, uuid.New(), time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unresolved reservation became quiescent: %v", err)
	}
	var count int
	if err := database.Admin.QueryRow("SELECT (vela_recovery_inventory()->>'runtime_startup_reservations')::int").Scan(&count); err != nil || count != 1 {
		t.Fatalf("missing startup recovery inventory: %d %v", count, err)
	}
	if result, err := service.ReserveRuntimeStartup(t.Context(), request); err != nil || result.Fresh {
		t.Fatalf("closed-gate replay changed history: %+v %v", result, err)
	}
}
