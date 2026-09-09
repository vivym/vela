package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
)

type startupActivationFixture struct {
	ledger      *RuntimeStartupLedger
	directory   string
	plan        *RuntimeLaunchPlan
	owner       *modelruntime.ExecutionJournalOwner
	endpoint    *JournalEndpoint
	grant       *JournalWriteGrant
	caller      *RuntimeCaller
	peers       journalEndpointFixture
	identity    modelruntime.ExecutionJournalIdentity
	floor       modelruntime.JournalCommand
	reservation RuntimeStartupReservationRecord
}

func newStartupActivationFixture(t *testing.T, existing ...*RuntimeStartupLedger) startupActivationFixture {
	t.Helper()
	_, config := newRemoteReservationFixture(t, false)
	return startupActivationFromReservation(t, config, existing...)
}

func startupActivationFromReservation(t *testing.T, config RuntimeStartupReservationConfig, existing ...*RuntimeStartupLedger) startupActivationFixture {
	t.Helper()
	var ledger *RuntimeStartupLedger
	var directory string
	if len(existing) == 1 {
		ledger, directory = existing[0], existing[0].path
	} else {
		ledger, directory = startupTestLedger(t)
	}
	reservation, err := ledger.ReserveRemote(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	peers := newJournalEndpointOwnerFixture(t, 0, nil, true, true)
	endpoint, err := NewReadOnlyJournalEndpoint(t.Context(), config.Journal, ledger.owners[reservation.JournalID], peers.worker.owner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	grant, err := IssueReservedJournalWriteGrant(t.Context(), endpoint, ledger.owners[reservation.JournalID], peers.worker.owner, reservation.OperationID, sha256.Sum256([]byte("fixture independently approved startup; not production authorization")), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var manifest modelruntime.LaunchManifest
	if err := json.Unmarshal(config.Plan.manifest, &manifest); err != nil {
		t.Fatal(err)
	}
	routes, err := modelruntime.RemoteStartupBindings(manifest)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := stageauthority.NewSigner(map[string][]byte{"authority": make([]byte, 32)})
	if err != nil {
		t.Fatal(err)
	}
	_, floor := journalEndpointCommands(t, manifest, routes[0], signer, time.Now(), "authority")
	return startupActivationFixture{ledger: ledger, directory: directory, plan: config.Plan, owner: config.Journal, endpoint: endpoint, grant: grant, peers: peers,
		identity: ledger.starts[reservation.JournalID].Remote.JournalIdentity,
		caller:   config.Caller,
		floor:    modelruntime.JournalCommand{SchemaVersion: 1, Floor: &modelruntime.JournalFloorCommand{Disposition: floor}}, reservation: reservation}
}

func (f startupActivationFixture) write(t *testing.T, allowed bool) {
	t.Helper()
	report := journalEndpointExchange(t, f.endpoint, f.peers.listener, f.peers.worker, f.identity, f.floor, false)
	if (report.Error == "") != allowed {
		t.Fatalf("write allowed=%v: %+v", allowed, report)
	}
}

func TestRuntimeStartupGrantActivationOrdersDurabilityAndWrites(t *testing.T) {
	f := newStartupActivationFixture(t)
	plain, err := IssueJournalWriteGrant(t.Context(), f.endpoint, f.ledger.owners[f.identity.JournalID], f.peers.worker.owner, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.endpoint.ActivateJournalWriteGrant(t.Context(), f.grant); err == nil {
		t.Fatal("startup grant bypassed ledger")
	}
	f.write(t, false)
	synced := false
	f.ledger.boundary = func(phase string) error {
		if phase == "after-sync" {
			synced = true
			// Runs while ledger is busy with append. Endpoint I/O must remain
			// responsive and read-only, and pre-issued ordinary grants cannot race it.
			f.write(t, false)
			if err := f.endpoint.ActivateJournalWriteGrant(t.Context(), plain); err == nil {
				t.Fatal("ordinary grant bypassed pending consumption")
			}
		}
		return nil
	}
	record, err := f.ledger.ActivateReservedJournalWriteGrant(t.Context(), f.plan, f.grant)
	if err != nil || !synced || record.OperationID != f.reservation.OperationID {
		t.Fatalf("activation: %+v %v synced=%v", record, err, synced)
	}
	if stored, err := f.ledger.InspectJournalGrantAttempt(t.Context(), f.identity.JournalID); err != nil || stored != record {
		t.Fatal("activated without exact durable consumption")
	}
	f.write(t, true)
	if status, err := f.owner.Status(t.Context()); err != nil || status.Floor != 1 {
		t.Fatalf("role write did not persist: %+v %v", status, err)
	}
	if _, err := f.ledger.ActivateReservedJournalWriteGrant(t.Context(), f.plan, f.grant); err == nil {
		t.Fatal("activation repeated")
	}
	if err := f.ledger.Close(); err != nil {
		t.Fatal(err)
	}
	f.write(t, false)
}

func TestRuntimeStartupGrantActivationRejectsHistoryAndMismatches(t *testing.T) {
	for _, scenario := range []string{"operation", "normal-grant", "wrong-runtime", "wrong-journal", "wrong-plan", "history", "reopened", "expired", "nil-grant"} {
		t.Run(scenario, func(t *testing.T) {
			f := newStartupActivationFixture(t)
			plan, grant := f.plan, f.grant
			switch scenario {
			case "operation":
				grant.operationID = uuid.New()
			case "normal-grant":
				var err error
				grant, err = IssueJournalWriteGrant(t.Context(), f.endpoint, f.ledger.owners[f.identity.JournalID], f.peers.worker.owner, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
			case "wrong-runtime":
				endpoint, err := NewReadOnlyJournalEndpoint(t.Context(), f.owner, f.peers.runtime.owner, f.peers.worker.owner)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = endpoint.Close() }()
				grant, err = IssueReservedJournalWriteGrant(t.Context(), endpoint, f.peers.runtime.owner, f.peers.worker.owner, f.reservation.OperationID, [32]byte{1}, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
			case "wrong-journal":
				var err error
				grant, err = IssueReservedJournalWriteGrant(t.Context(), f.peers.endpoint, f.peers.runtime.owner, f.peers.worker.owner, f.reservation.OperationID, [32]byte{1}, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
			case "wrong-plan":
				_, config := newRemoteReservationFixture(t, false)
				plan = config.Plan
			case "history":
				if _, err := f.ledger.ConsumeJournalGrantAttempt(t.Context(), f.identity.JournalID, f.reservation.OperationID, [32]byte{1}); err != nil {
					t.Fatal(err)
				}
			case "reopened":
				if err := f.ledger.Close(); err != nil {
					t.Fatal(err)
				}
				var err error
				f.ledger, err = OpenRuntimeStartupLedger(t.Context(), f.directory, "cpu-node", false)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = f.ledger.Close() }()
			case "expired":
				grant.expires = time.Now().Add(-time.Second)
			case "nil-grant":
				grant = nil
			}
			if result, err := f.ledger.ActivateReservedJournalWriteGrant(t.Context(), plan, grant); err == nil || result != (RuntimeStartupGrantAttempt{}) {
				t.Fatalf("invalid activation: %+v %v", result, err)
			}
			f.write(t, false)
		})
	}
}

func TestRuntimeStartupGrantActivationFailsClosedDuringPersistence(t *testing.T) {
	for _, scenario := range []string{"before-append", "after-append", "after-sync", "cancelled", "closed-endpoint", "worker-exited", "runtime-owner-lost", "journal-changed", "expired"} {
		t.Run(scenario, func(t *testing.T) {
			f := newStartupActivationFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			f.ledger.boundary = func(phase string) error {
				if phase == scenario {
					return errors.New("injected persistence failure")
				}
				if phase != "after-sync" {
					return nil
				}
				switch scenario {
				case "cancelled":
					cancel()
				case "closed-endpoint":
					return f.endpoint.Close()
				case "worker-exited":
					if err := f.peers.worker.process.Kill(); err != nil {
						return err
					}
					select {
					case <-f.peers.worker.done:
					case <-time.After(5 * time.Second):
						t.Fatal("worker did not exit")
					}
				case "runtime-owner-lost":
					return f.ledger.owners[f.identity.JournalID].Close()
				case "journal-changed":
					wire, err := modelruntime.EncodeJournalCommand(f.floor)
					if err != nil {
						return err
					}
					_, err = f.owner.Apply(t.Context(), modelruntime.JournalWorkerRole, wire)
					if err != nil {
						t.Fatalf("journal mutation injection failed: %v", err)
					}
					return nil
				case "expired":
					f.grant.expires = time.Now().Add(-time.Second)
				}
				return nil
			}
			if result, err := f.ledger.ActivateReservedJournalWriteGrant(ctx, f.plan, f.grant); err == nil || result != (RuntimeStartupGrantAttempt{}) {
				t.Fatalf("failed activation returned success: %+v %v", result, err)
			}
			if !f.endpoint.readOnly {
				t.Fatal("failure left writable route")
			}
			f.ledger.boundary = nil
			if _, err := f.ledger.ActivateReservedJournalWriteGrant(t.Context(), f.plan, f.grant); err == nil {
				t.Fatal("failed operation reactivated")
			}
			if err := f.endpoint.ActivateJournalWriteGrant(t.Context(), f.grant); err == nil {
				t.Fatal("failed coordinated grant activated directly")
			}
			if err := f.ledger.Close(); err != nil {
				t.Fatal(err)
			}
			recovered, err := OpenRuntimeStartupLedger(t.Context(), f.directory, "cpu-node", false)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = recovered.Close() }()
			_, err = recovered.InspectJournalGrantAttempt(t.Context(), f.identity.JournalID)
			if scenario == "before-append" && !errors.Is(err, os.ErrNotExist) || scenario != "before-append" && err != nil {
				t.Fatalf("unexpected recovered consumption: %v", err)
			}
			if _, err := recovered.ActivateReservedJournalWriteGrant(t.Context(), f.plan, f.grant); err == nil {
				t.Fatal("recovery activated failed attempt")
			}
		})
	}
}

func TestRuntimeStartupGrantActivationConcurrent(t *testing.T) {
	f := newStartupActivationFixture(t)
	var group sync.WaitGroup
	results := make(chan error, 8)
	for range 8 {
		group.Go(func() {
			_, err := f.ledger.ActivateReservedJournalWriteGrant(t.Context(), f.plan, f.grant)
			results <- err
		})
	}
	group.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful activations=%d", successes)
	}
	f.write(t, true)
	wire, err := os.ReadFile(filepath.Join(f.directory, runtimeStartupLedgerName))
	if err != nil || len(wire) == 0 {
		t.Fatal("missing durable ledger")
	}
}

func TestRuntimeStartupGrantActivationCloseRevokesBeforeBlockedAppend(t *testing.T) {
	active := newStartupActivationFixture(t)
	if _, err := active.ledger.ActivateReservedJournalWriteGrant(t.Context(), active.plan, active.grant); err != nil {
		t.Fatal(err)
	}
	active.write(t, true)
	pending := newStartupActivationFixture(t, active.ledger)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	active.ledger.boundary = func(phase string) error {
		if phase == "before-append" {
			close(entered)
			<-release
		}
		return nil
	}
	result := make(chan error, 1)
	go func() {
		_, err := active.ledger.ActivateReservedJournalWriteGrant(t.Context(), pending.plan, pending.grant)
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("pending startup did not enter append")
	}
	closed := make(chan error, 1)
	go func() { closed <- active.ledger.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		active.endpoint.mu.Lock()
		revoked := active.endpoint.runtime == nil && active.endpoint.worker == nil
		active.endpoint.mu.Unlock()
		if revoked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("route revocation waited behind ledger persistence")
		}
		time.Sleep(time.Millisecond)
	}
	active.write(t, false)
	unblock()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("pending grant activated after Close began")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pending activation did not join")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not join")
	}
	pending.write(t, false)
}
