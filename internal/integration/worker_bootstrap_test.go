//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/recovery"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/workerbootstrap"
)

func TestLocalWorkerBootstrapPersistsActualJournalsWithRegistryAuthority(t *testing.T) {
	for _, lost := range []string{"receipt", "claim"} {
		t.Run(lost, func(t *testing.T) {
			database, service, request := newWorkerBootstrapFixture(t)
			config := localBootstrapConfig(t, request)
			transport := &bootstrapResponseLoss{service: service, lost: lost}
			if result, err := workerbootstrap.Prepare(t.Context(), config, transport); err == nil || result != (workerbootstrap.Result{}) {
				t.Fatalf("lost %s response exposed success: %+v %v", lost, result, err)
			}
			transport.lost = ""
			result, err := workerbootstrap.Prepare(t.Context(), config, transport)
			if lost == "claim" {
				if !errors.Is(err, workerbootstrap.ErrIncomplete) || result != (workerbootstrap.Result{}) || transport.claimCalls != 1 {
					t.Fatalf("lost committed claim reinitialized: %+v %v", result, err)
				}
				var count int
				if err := database.Admin.QueryRow("SELECT count(*) FROM worker_bootstrap_receipts").Scan(&count); err != nil || count != 0 {
					t.Fatalf("unprepared journals reported receipt: %d %v", count, err)
				}
				for _, name := range []string{"worker-admission", "runtime-admission", "inputs", "outputs"} {
					entries, err := os.ReadDir(filepath.Join(config.ScratchDirectory, name))
					if err != nil || len(entries) != 0 {
						t.Fatalf("lost claim retry initialized %s: %v", name, err)
					}
				}
				return
			}
			if err != nil || result.Worker.JournalID == uuid.Nil || result.Runtime.JournalID == uuid.Nil || transport.claimCalls != 1 {
				t.Fatalf("recover prepared pair: %+v %v", result, err)
			}
			var workerID, runtimeID uuid.UUID
			var workerScope, runtimeScope []byte
			var recordedAt time.Time
			if err := database.Admin.QueryRow(`SELECT worker_journal_id, worker_scope, runtime_journal_id, runtime_scope, recorded_at
				FROM worker_bootstrap_receipts WHERE request_id = $1`, result.RequestID).
				Scan(&workerID, &workerScope, &runtimeID, &runtimeScope, &recordedAt); err != nil {
				t.Fatal(err)
			}
			if workerID != result.Worker.JournalID || runtimeID != result.Runtime.JournalID ||
				!bytes.Equal(workerScope, result.Worker.Scope[:]) || !bytes.Equal(runtimeScope, result.Runtime.Scope[:]) || !recordedAt.Equal(result.RecordedAt) {
				t.Fatal("Registry receipt differs from actual recovered local pair")
			}
			again, err := workerbootstrap.Prepare(t.Context(), config, service)
			if err != nil || again != result {
				t.Fatalf("replacement provisioner changed durable result: %+v %v", again, err)
			}
		})
	}
}

type bootstrapResponseLoss struct {
	service    *fleet.Service
	lost       string
	claimCalls int
}

func (transport *bootstrapResponseLoss) ClaimWorkerBootstrap(ctx context.Context, request fleet.WorkerBootstrapRequest) (fleet.WorkerBootstrapClaim, error) {
	transport.claimCalls++
	claim, err := transport.service.ClaimWorkerBootstrap(ctx, request)
	if err == nil && transport.lost == "claim" {
		return fleet.WorkerBootstrapClaim{}, errors.New("committed claim response lost")
	}
	return claim, err
}

func (transport *bootstrapResponseLoss) RecordWorkerBootstrapReceipt(ctx context.Context, receipt fleet.WorkerBootstrapReceipt) (time.Time, error) {
	when, err := transport.service.RecordWorkerBootstrapReceipt(ctx, receipt)
	if err == nil && transport.lost == "receipt" {
		return time.Time{}, errors.New("committed receipt response lost")
	}
	return when, err
}

func localBootstrapConfig(t *testing.T, request fleet.WorkerBootstrapRequest) workerbootstrap.Config {
	t.Helper()
	var document struct {
		Schema string
		Bundle fleetcontroller.WorkerBundleActuation
	}
	if err := json.Unmarshal(request.BundleManifest, &document); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(request.BundleManifest)
	document.Bundle.RevisionDigest = hex.EncodeToString(digest[:])
	launch, err := fleetcontroller.WorkerMemberLaunchManifest(document.Bundle, request.WorkerInstanceID, request.WorkerMemberID)
	if err != nil {
		t.Fatal(err)
	}
	keyring, err := stageauthority.DeriveVerifierKeyring(map[string][]byte{"bootstrap-key": bytes.Repeat([]byte{3}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stageauthority.ClearKeyring(keyring) })
	validator, err := stageauthority.NewVerifier(keyring, nil)
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"bootstrap", "worker-admission", "runtime-admission", "inputs", "outputs"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return workerbootstrap.Config{Bundle: document.Bundle, Launch: launch, NodeIdentity: "h3-node-01", ActorIdentity: request.ActorIdentity,
		ScratchDirectory: root, MaxRecords: 4, Validator: validator}
}

func TestWorkerBootstrapConsumesFirstUseOnceAndPreservesReceipt(t *testing.T) {
	database, service, request := newWorkerBootstrapFixture(t)
	const callers = 8
	results := make(chan fleet.WorkerBootstrapClaim, callers)
	failures := make(chan error, callers)
	var group sync.WaitGroup
	for range callers {
		group.Go(func() {
			claim, err := service.ClaimWorkerBootstrap(t.Context(), request)
			results <- claim
			failures <- err
		})
	}
	group.Wait()
	close(results)
	close(failures)
	for err := range failures {
		if err != nil {
			t.Fatal(err)
		}
	}
	fresh := 0
	var claimedAt time.Time
	for claim := range results {
		if claim.Fresh {
			fresh++
		}
		if claim.RequestID != request.RequestID || claim.WorkerMemberID != request.WorkerMemberID ||
			claim.WorkerInstanceEpoch != 1 || claim.WorkerMemberEpoch != 1 || claim.NodeIdentity != "h3-node-01" ||
			!bytes.Equal(claim.BundleDigest, sha256Bytes(request.BundleManifest)) || claim.ClaimedAt.IsZero() {
			t.Fatalf("claim differs from approved bundle: %+v", claim)
		}
		if !claimedAt.IsZero() && !claimedAt.Equal(claim.ClaimedAt) {
			t.Fatal("claim replay replaced original timestamp")
		}
		claimedAt = claim.ClaimedAt
	}
	if fresh != 1 {
		t.Fatalf("concurrent first-use grants = %d, want 1", fresh)
	}

	// Simulate loss of the sole fresh response. Neither a new service instance
	// nor a new command UUID may turn the retained claim into another grant.
	replacement, err := fleet.NewService(newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password"))
	if err != nil {
		t.Fatal(err)
	}
	replay, err := replacement.ClaimWorkerBootstrap(t.Context(), request)
	if err != nil || replay.Fresh || !replay.ClaimedAt.Equal(claimedAt) {
		t.Fatalf("lost response granted initialization again: %+v %v", replay, err)
	}
	changed := request
	changed.RequestID = uuid.New()
	_, err = service.ClaimWorkerBootstrap(t.Context(), changed)
	assertFleetFailure(t, err, fleet.FailureConflict)
	changed = request
	changed.ActorIdentity = "fleet/other-provisioner"
	_, err = service.ClaimWorkerBootstrap(t.Context(), changed)
	assertFleetFailure(t, err, fleet.FailureConflict)

	receipt := fleet.WorkerBootstrapReceipt{
		RequestID: request.RequestID, WorkerJournalID: uuid.New(), RuntimeJournalID: uuid.New(),
		WorkerScope: bytes.Repeat([]byte{0x31}, 32), RuntimeScope: bytes.Repeat([]byte{0x32}, 32), ActorIdentity: request.ActorIdentity,
	}
	first, err := service.RecordWorkerBootstrapReceipt(t.Context(), receipt)
	if err != nil || first.IsZero() {
		t.Fatalf("record journal identities: %v", err)
	}
	second, err := replacement.RecordWorkerBootstrapReceipt(t.Context(), receipt)
	if err != nil || !first.Equal(second) {
		t.Fatalf("receipt replay changed original result: %v", err)
	}
	for _, fault := range []string{"journal", "scope", "actor"} {
		t.Run(fault, func(t *testing.T) {
			changed := receipt
			switch fault {
			case "journal":
				changed.RuntimeJournalID = uuid.New()
			case "scope":
				changed.WorkerScope = bytes.Repeat([]byte{0x33}, 32)
			case "actor":
				changed.ActorIdentity = "fleet/other-provisioner"
			}
			_, err := service.RecordWorkerBootstrapReceipt(t.Context(), changed)
			assertFleetFailure(t, err, fleet.FailureConflict)
		})
	}
	for _, table := range []string{"worker_bootstrap_manifests", "worker_bootstrap_claims", "worker_bootstrap_receipts"} {
		for _, statement := range []string{"DELETE FROM " + table, "TRUNCATE " + table + " CASCADE"} {
			if _, err := database.Admin.Exec(statement); err == nil || !strings.Contains(err.Error(), "immutable") {
				t.Fatalf("bootstrap history mutation was not rejected: %s: %v", statement, err)
			}
		}
	}
	if err := goose.DownTo(database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), 90); err == nil ||
		!strings.Contains(err.Error(), "bootstrap history prohibits rollback") {
		t.Fatalf("populated first-use history rolled back: %v", err)
	}
}

func TestWorkerBootstrapRejectsUnapprovedOrAlreadyObservedScope(t *testing.T) {
	database, service, request := newWorkerBootstrapFixture(t)
	for _, fault := range []string{"payload", "epoch", "member"} {
		t.Run(fault, func(t *testing.T) {
			changed := request
			switch fault {
			case "payload":
				changed.BundleManifest = bytes.Replace(request.BundleManifest, []byte("h3-node-01"), []byte("h3-node-02"), 1)
			case "epoch":
				changed.WorkerInstanceEpoch++
			case "member":
				changed.WorkerMemberID = uuid.New()
			}
			if result, err := service.ClaimWorkerBootstrap(t.Context(), changed); err == nil || result.Fresh {
				t.Fatalf("unapproved first-use claim: %+v %v", result, err)
			}
		})
	}
	var count int
	if err := database.Admin.QueryRow("SELECT count(*) FROM worker_bootstrap_claims").Scan(&count); err != nil || count != 0 {
		t.Fatalf("rejected requests consumed first use: %d %v", count, err)
	}
	internalPool := newRolePool(t, database.DSN, "vela_internal_login", "vela-internal-password")
	if _, err := internalPool.Exec(t.Context(), "SELECT * FROM vela_claim_worker_bootstrap($1, $2, $3, $4, $5, $6)",
		request.RequestID, request.WorkerInstanceID, request.WorkerInstanceEpoch, request.WorkerMemberID,
		request.BundleManifest, request.ActorIdentity); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("broad internal runtime obtained bootstrap authority: %v", err)
	}
	evidence := workerRegistryEvidenceValue(t, request.WorkerInstanceID, 0x51)
	if _, err := service.Observe(t.Context(), evidence); err != nil {
		t.Fatal(err)
	}
	_, err := service.ClaimWorkerBootstrap(t.Context(), request)
	assertFleetFailure(t, err, fleet.FailureConflict)
	if _, err := service.Fence(t.Context(), fleet.WorkerInstanceFenceRequest{
		WorkerInstanceID: request.WorkerInstanceID, ExpectedInstanceEpoch: 1, Reason: "bootstrap regression", ObservedBy: "fleet/test",
	}); err != nil {
		t.Fatal(err)
	}
	_, err = service.ClaimWorkerBootstrap(t.Context(), request)
	assertFleetFailure(t, err, fleet.FailureConflict)
}

func TestWorkerBootstrapEmptyMigrationDownUp(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 91)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 90); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(database.Admin, migrations, 91); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerBootstrapRequiresCommitQuorum(t *testing.T) {
	database, ordinary, request := newWorkerBootstrapFixture(t)
	enableStageQuorumRequirement(t, database)
	guarded, err := fleet.NewService(newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password"))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := guarded.ClaimWorkerBootstrap(t.Context(), request)
	if err == nil || claim.Fresh || !strings.Contains(err.Error(), "synchronous replication quorum is unavailable") {
		t.Fatalf("uncommitted claim escaped as permission: %+v %v", claim, err)
	}
	var count int
	if err := database.Admin.QueryRow("SELECT count(*) FROM worker_bootstrap_claims").Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed commit retained claim: %d %v", count, err)
	}
	// The preexisting connection retains the standalone test configuration.
	if claim, err := ordinary.ClaimWorkerBootstrap(t.Context(), request); err != nil || !claim.Fresh {
		t.Fatalf("rolled-back first use could not be claimed: %+v %v", claim, err)
	}
	receipt := fleet.WorkerBootstrapReceipt{RequestID: request.RequestID, WorkerJournalID: uuid.New(), RuntimeJournalID: uuid.New(),
		WorkerScope: bytes.Repeat([]byte{0x31}, 32), RuntimeScope: bytes.Repeat([]byte{0x32}, 32), ActorIdentity: request.ActorIdentity}
	if timestamp, err := guarded.RecordWorkerBootstrapReceipt(t.Context(), receipt); err == nil || !timestamp.IsZero() {
		t.Fatalf("uncommitted receipt escaped as durable: %v %v", timestamp, err)
	}
}

func TestWorkerBootstrapParticipatesInRecoveryQuiescence(t *testing.T) {
	database, service, request := newWorkerBootstrapFixture(t)
	connection := recoveryConnection(t, database)
	operationID := uuid.New()
	closed, err := recovery.Quiesce(t.Context(), connection, operationID, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ClaimWorkerBootstrap(t.Context(), request); err == nil || !strings.Contains(err.Error(), "closed for recovery") {
		t.Fatalf("closed recovery gate allowed first use: %v", err)
	}
	if err := recovery.Reopen(t.Context(), connection, operationID, closed.Generation); err != nil {
		t.Fatal(err)
	}
	if claim, err := service.ClaimWorkerBootstrap(t.Context(), request); err != nil || !claim.Fresh {
		t.Fatalf("claim after reopened gate: %+v %v", claim, err)
	}
	operationID = uuid.New()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := recovery.Quiesce(ctx, connection, operationID, time.Millisecond); err == nil {
		t.Fatal("unfinished first use was accepted as database quiescence")
	}
	if replay, err := service.ClaimWorkerBootstrap(t.Context(), request); err != nil || replay.Fresh {
		t.Fatalf("closed gate changed historical observation: %+v %v", replay, err)
	}
	receipt := fleet.WorkerBootstrapReceipt{RequestID: request.RequestID, WorkerJournalID: uuid.New(), RuntimeJournalID: uuid.New(),
		WorkerScope: bytes.Repeat([]byte{0x31}, 32), RuntimeScope: bytes.Repeat([]byte{0x32}, 32), ActorIdentity: request.ActorIdentity}
	if _, err := service.RecordWorkerBootstrapReceipt(t.Context(), receipt); err != nil {
		t.Fatal(err)
	}
	completed, err := recovery.Quiesce(t.Context(), connection, operationID, time.Millisecond)
	if err != nil || completed.SchemaVersion != 91 || completed.Inventory["worker_bootstrap_claims"] != 0 {
		t.Fatalf("completed first use did not release quiescence: %+v %v", completed, err)
	}
}

func newWorkerBootstrapFixture(t *testing.T) (testDatabase, *fleet.Service, fleet.WorkerBootstrapRequest) {
	t.Helper()
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	service, err := fleet.NewService(newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password"))
	if err != nil {
		t.Fatal(err)
	}
	var plan fleet.ApprovedResidencyPlan
	if err := json.Unmarshal(mustJSON(t, approvedResidencyPlanFixture(uuid.New())), &plan); err != nil {
		t.Fatal(err)
	}
	plan.SourceProposalID = nil
	worker := plan.WorkerInstances[0]
	memberID := uuid.NewSHA1(worker.ID, []byte("member-0"))
	identity := sha256.Sum256([]byte("spiffe://vela.internal/stage-worker/" + memberID.String()))
	bundle := fleetcontroller.WorkerBundleActuation{
		SchemaVersion: 2, PlanRevisionID: plan.ID, WorkerBundleID: plan.WorkerBundles[0].ID, Namespace: "vela-system",
		InitImage: "docker.io/library/busybox@sha256:" + digestHex(0x61), StageWorkerAgentImage: "ghcr.io/vela/worker@sha256:" + digestHex(0x62),
		RuntimeImage: "ghcr.io/vela/runtime@sha256:" + digestHex(0x63), StageWorkerConfigMap: "worker-config",
		ModelRuntimeVerifierConfigMap: "verifier", StageWorkerControlTLSSecret: "control-tls", StageWorkerAuthoritySecret: "authority",
		ArtifactStoreCredentialsSecret: "artifact-credentials", ArtifactStoreCASecret: "artifact-ca",
		WorkerInstances: []fleetcontroller.WorkerInstanceActuation{{
			ID: worker.ID, InstanceEpoch: 1, WorkerProfileRevisionID: worker.WorkerProfileRevisionID,
			CapacityPoolID: worker.CapacityPoolID, Role: "dit", CapacitySlots: 1,
			DeviceSetDigest: digestHex(0x64), MembershipDigest: digestHex(0x65),
			ModelRuntimes: []fleetcontroller.ModelRuntimeProcess{{
				ModelResidencyID: worker.ModelRuntimeRoutes[0].ModelResidencyID, CapacityPoolID: worker.CapacityPoolID,
				StageProfileRevisionID: worker.ModelRuntimeRoutes[0].StageProfileRevisionID, ModelRuntimeEpochFloor: 1,
				Component: "DIT", ModelComponentRevision: "cpu-test", RuntimeIdentity: "cpu-test-runtime",
				Command: []string{"/nonexistent-test-backend"}, InitializationTimeout: "1s", ShutdownTimeout: "1s",
			}},
			Members: []fleetcontroller.WorkerMemberActuation{{
				ID: memberID, MemberEpoch: 1, Key: "member-0", NodeIdentity: "h3-node-01", ResourceClass: "GPU", DeviceCount: 1,
				IdentityDigest: hex.EncodeToString(identity[:]), DeviceSubsetDigest: digestHex(0x66),
				DeviceConstraints: []fleetcontroller.DeviceConstraint{{DeviceID: uuid.MustParse(workerRegistryDeviceID), DeviceEpoch: 1,
					GPUUUID: "GPU-00000000-0000-0000-0000-000000000004", PCIBDF: "0000:41:00.0"}},
			}},
		}},
	}
	bundle.RevisionDigest, err = fleetcontroller.ComputeWorkerBundleActuationDigest(bundle)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := fleetcontroller.WorkerBundleActuationManifest(bundle)
	if err != nil {
		t.Fatal(err)
	}
	plan.WorkerBundles[0].LayoutDigest = bundle.RevisionDigest
	if err := fleetcontroller.ValidateResidencyPlanRollout(fleetcontroller.ResidencyPlanRollout{
		ApprovedPlan: plan, WorkerBundles: []fleetcontroller.WorkerBundleActuation{bundle},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Apply(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	return database, service, fleet.WorkerBootstrapRequest{
		RequestID: uuid.New(), WorkerInstanceID: worker.ID, WorkerInstanceEpoch: 1, WorkerMemberID: memberID,
		BundleManifest: manifest, ActorIdentity: "fleet/bootstrap-test",
	}
}

func sha256Bytes(value []byte) []byte {
	digest := sha256.Sum256(value)
	return digest[:]
}
