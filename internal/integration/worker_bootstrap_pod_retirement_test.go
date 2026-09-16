//go:build integration

package integration_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/runtimelaunch"
)

func TestWorkerBootstrapPodRetirementRequiresFencedExactManifest(t *testing.T) {
	database, service, bootstrap := newConfiguredWorkerBootstrapFixture(t, func(_ *fleet.ApprovedResidencyPlan, bundle *fleetcontroller.WorkerBundleActuation) {
		bundle.RuntimeLaunchProtocol = runtimelaunch.Protocol
	})
	if _, err := service.ClaimWorkerBootstrap(t.Context(), bootstrap); err != nil {
		t.Fatal(err)
	}
	bundle, err := fleetcontroller.ParseWorkerBundleActuationManifest(bootstrap.BundleManifest)
	if err != nil {
		t.Fatal(err)
	}
	request := fleet.MutationAuthorizationRequest{RequestUID: uuid.NewString(), ActorIdentity: "fleet/controller", Operation: fleet.MutationDelete,
		KubernetesUID: uuid.NewString(), Namespace: bundle.Namespace,
		Name:             "wi-" + strings.ReplaceAll(bootstrap.WorkerInstanceID.String(), "-", "") + "-ba3790e0",
		WorkerInstanceID: bootstrap.WorkerInstanceID, WorkerInstanceEpoch: bootstrap.WorkerInstanceEpoch,
		ResidencyPlanRevisionID: bundle.PlanRevisionID, WorkerBundleID: bundle.WorkerBundleID,
		WorkerMemberID: bootstrap.WorkerMemberID, RequestDigest: sha256Bytes([]byte("exact admission request"))}
	if _, err := service.AuthorizeMutation(t.Context(), request); err == nil {
		t.Fatal("unfenced bootstrap can retire Pod")
	}
	if _, err := service.RecordWorkerBootstrapReceipt(t.Context(), bootstrapReceipt(bootstrap)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Admin.Exec("SELECT * FROM vela_fence_worker_instance($1,1,'STARTUP_FAILED','fleet/controller')", bootstrap.WorkerInstanceID); err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"member", "bundle", "plan", "namespace", "name", "epoch"} {
		t.Run(fault, func(t *testing.T) {
			changed := request
			changed.RequestUID = uuid.NewString()
			switch fault {
			case "member":
				changed.WorkerMemberID = uuid.New()
			case "bundle":
				changed.WorkerBundleID = uuid.New()
			case "plan":
				changed.ResidencyPlanRevisionID = uuid.New()
			case "namespace":
				changed.Namespace = "other"
			case "name":
				changed.Name += "-other"
			case "epoch":
				changed.WorkerInstanceEpoch++
			}
			if _, err := service.AuthorizeMutation(t.Context(), changed); err == nil {
				t.Fatal("accepted conflicting bootstrap retirement")
			}
		})
	}
	// Hold a real, successful authorization uncommitted. Down must wait before
	// checking emptiness and then refuse to discard the committed receipt.
	mutation, err := database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mutation.Rollback() }()
	if _, err := mutation.Exec(`SELECT * FROM vela_authorize_worker_instance_pod_mutation(
		$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		request.RequestUID, request.ActorIdentity, request.Operation, request.KubernetesUID,
		request.Namespace, request.Name, request.WorkerInstanceID, request.WorkerInstanceEpoch,
		request.ResidencyPlanRevisionID, request.WorkerBundleID, request.WorkerMemberID, request.RequestDigest); err != nil {
		t.Fatal(err)
	}
	down := make(chan error, 1)
	go func() { down <- goose.DownTo(database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), 99) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err := database.Admin.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_locks
			WHERE relation='worker_bootstrap_pod_mutation_authorizations'::regclass
			AND mode='AccessExclusiveLock' AND NOT granted)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-down:
			t.Fatalf("rollback bypassed pending authorization: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("rollback did not acquire the audit table lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := mutation.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-down; err == nil || !strings.Contains(err.Error(), "Cannot discard pre-readiness Pod retirement history") {
		t.Fatalf("rollback lost concurrent retirement receipt: %v", err)
	}
	for _, operation := range []fleet.MutationOperation{fleet.MutationDelete, fleet.MutationRemoveFinalizer} {
		request.RequestUID, request.Operation = uuid.NewString(), operation
		first, err := service.AuthorizeMutation(t.Context(), request)
		if err != nil || !first.Authorized || first.Replayed {
			t.Fatalf("retirement: %+v %v", first, err)
		}
		again, err := service.AuthorizeMutation(t.Context(), request)
		if err != nil || !again.Authorized || !again.Replayed {
			t.Fatalf("replay: %+v %v", again, err)
		}
		changed := request
		changed.KubernetesUID = uuid.NewString()
		if _, err := service.AuthorizeMutation(t.Context(), changed); err == nil {
			t.Fatal("request UID replay changed Pod UID")
		}
	}
	var members, residencies, receipts int
	if err := database.Admin.QueryRow("SELECT (SELECT count(*) FROM worker_members),(SELECT count(*) FROM model_residencies),(SELECT count(*) FROM worker_bootstrap_pod_mutation_authorizations)").Scan(&members, &residencies, &receipts); err != nil {
		t.Fatal(err)
	}
	if members != 0 || residencies != 0 || receipts != 3 {
		t.Fatalf("invented observations or missing receipts: %d %d %d", members, residencies, receipts)
	}
	if _, err := database.Admin.Exec("DELETE FROM worker_bootstrap_pod_mutation_authorizations"); err == nil {
		t.Fatal("retirement history is mutable")
	}
	if err := goose.DownTo(database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), 99); err == nil {
		t.Fatal("downgrade discarded retirement history")
	}
}

func TestWorkerBootstrapPodRetirementRequiresReceiptAndPreservesEmptyRollback(t *testing.T) {
	database, service, request := newConfiguredWorkerBootstrapFixture(t, func(_ *fleet.ApprovedResidencyPlan, bundle *fleetcontroller.WorkerBundleActuation) {
		bundle.RuntimeLaunchProtocol = runtimelaunch.Protocol
	})
	if _, err := service.ClaimWorkerBootstrap(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Admin.Exec("SELECT * FROM vela_fence_worker_instance($1,1,'STARTUP_FAILED','fleet/controller')", request.WorkerInstanceID); err != nil {
		t.Fatal(err)
	}
	bundle, err := fleetcontroller.ParseWorkerBundleActuationManifest(request.BundleManifest)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.AuthorizeMutation(t.Context(), fleet.MutationAuthorizationRequest{RequestUID: uuid.NewString(), ActorIdentity: "fleet/controller", Operation: fleet.MutationDelete,
		KubernetesUID: uuid.NewString(), Namespace: bundle.Namespace, Name: "wi-" + strings.ReplaceAll(request.WorkerInstanceID.String(), "-", "") + "-ba3790e0",
		WorkerInstanceID: request.WorkerInstanceID, WorkerInstanceEpoch: 1, ResidencyPlanRevisionID: bundle.PlanRevisionID, WorkerBundleID: bundle.WorkerBundleID, WorkerMemberID: request.WorkerMemberID, RequestDigest: sha256Bytes([]byte("missing receipt"))})
	if err == nil {
		t.Fatal("retired without completed bootstrap")
	}
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 99); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(database.Admin, migrations, 100); err != nil {
		t.Fatal(err)
	}
}
