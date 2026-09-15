//go:build linux

package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageauthority"
)

// TestRuntimeStartupCompositionDriver exercises one shared validation driver
// for the accepted path and the authority failures that must stop before a
// grant is usable.  The driver deliberately reuses the production
// reservation/orchestration methods; only Fleet and policy are disposable
// validation boundaries.
func TestRuntimeStartupCompositionDriver(t *testing.T) {
	t.Run("success-and-replay", func(t *testing.T) {
		fixture, reservationConfig, custody, ledger, ledgerDirectory, peers, authority := newCompositionDriver(t, compositionAuthorizationPolicy{})
		orchestration, record, err := authority.Prepare(t.Context(), fixture.caller)
		if err != nil {
			t.Fatal(err)
		}
		if orchestration == nil || record.OperationID == uuid.Nil {
			t.Fatalf("composition did not create one operation: orchestration=%v record=%+v", orchestration != nil, record)
		}
		defer orchestration.Close()

		// Use the orchestration's authenticated server path, including the
		// caller reply, rather than calling the coordinator directly.
		// Before that exchange the endpoint is read-only; prove that a paired
		// Worker cannot mutate its journal before the grant is consumed.
		endpoint := orchestration.coordinator.grant.endpoint
		manifest, err := fixture.plan.LaunchManifest()
		if err != nil {
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
		floorCommand := modelruntime.JournalCommand{SchemaVersion: 1, Floor: &modelruntime.JournalFloorCommand{Disposition: floor}}
		identity := modelruntime.ExecutionJournalIdentity{JournalID: fixture.request.JournalID, Scope: fixture.request.JournalScope}
		if reply := journalEndpointExchange(t, endpoint, peers.listener, peers.worker, identity, floorCommand, false); reply.Error == "" {
			t.Fatal("pre-grant Worker journal mutation succeeded")
		}
		if err := orchestration.ServeCaller(t.Context()); err != nil {
			t.Fatalf("backend startup exchange failed: %v", err)
		}
		receipt, err := orchestration.CompositionReceipt(t.Context())
		if err != nil || !receipt.Permit || receipt.Outcome != "permitted" {
			t.Fatalf("invalid permitted receipt: %+v %v", receipt, err)
		}
		if err := receipt.Verify(); err != nil {
			t.Fatal(err)
		}
		// Cross the process boundary as JSON before binding the receipt. This
		// catches a serializer that drops an authority field while preserving a
		// self-consistent in-memory value.
		receiptWire, err := json.Marshal(receipt)
		if err != nil {
			t.Fatal(err)
		}
		var replayed RuntimeStartupCompositionReceipt
		if err := json.Unmarshal(receiptWire, &replayed); err != nil {
			t.Fatal(err)
		}
		if err := replayed.Verify(); err != nil {
			t.Fatalf("replayed receipt failed self verification: %v", err)
		}
		reservationWire, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		reservationDigest := sha256.Sum256(reservationWire)
		requestWire, err := modelruntime.EncodeBackendStartupRequest(fixture.request)
		if err != nil {
			t.Fatal(err)
		}
		requestDigest := sha256.Sum256(requestWire)
		if err := replayed.VerifyBinding(record.OperationID, record.JournalID, requestDigest, reservationDigest, replayed.AuthorizationDigest); err != nil {
			t.Fatalf("receipt is not bound to independent records: %v", err)
		}
		// After Permit the same retained endpoint permits exactly one Worker
		// journal mutation and advances the durable floor once.
		if reply := journalEndpointExchange(t, endpoint, peers.listener, peers.worker, identity, floorCommand, false); reply.Error != "" {
			t.Fatalf("post-Permit Worker journal mutation failed: %+v", reply)
		}
		status, err := reservationConfig.Journal.Status(t.Context())
		if err != nil || status.Floor != 1 {
			t.Fatalf("Worker journal floor did not advance: %+v %v", status, err)
		}

		// The one-shot exchange must consume exactly one grant and a retry must
		// remain denied after the operation is closed.
		if _, err := ledger.InspectJournalGrantAttempt(t.Context(), record.JournalID); err != nil {
			t.Fatalf("missing durable grant attempt: %v", err)
		}
		if err := orchestration.Close(); err != nil {
			t.Fatal(err)
		}
		if retry, _, err := authority.Prepare(t.Context(), fixture.caller); err == nil || retry != nil {
			t.Fatalf("duplicate startup was accepted: orchestration=%v err=%v", retry != nil, err)
		}
		if _, err := ledger.InspectJournalGrantAttempt(t.Context(), record.JournalID); err != nil {
			t.Fatalf("grant history was lost after revoke: %v", err)
		}
		// Reopen the durable ledger as a simulated Node restart. Recovery may
		// inspect history, but it must not reconstruct the original pidfd or
		// issue another reservation.
		if err := ledger.Close(); err != nil {
			t.Fatal(err)
		}
		recovered, err := OpenRuntimeStartupLedger(t.Context(), ledgerDirectory, "cpu-node", false)
		if err != nil {
			t.Fatal(err)
		}
		defer recovered.Close()
		if _, err := recovered.ReserveRemote(t.Context(), reservationConfig); !errors.Is(err, ErrRuntimeStartupRecorded) && !errors.Is(err, modelruntime.ErrBackendStartupDenied) {
			t.Fatalf("Node restart reissued startup authority: %v", err)
		}
		_ = custody
	})

	t.Run("policy-response-loss", func(t *testing.T) {
		fixture, _, _, ledger, _, _, authority := newCompositionDriver(t, lossCompositionAuthorizationPolicy{})
		orchestration, record, err := authority.Prepare(t.Context(), fixture.caller)
		if err == nil || orchestration != nil || record.OperationID == uuid.Nil {
			t.Fatalf("policy loss crossed composition boundary: orchestration=%v record=%+v err=%v", orchestration != nil, record, err)
		}
		if _, err := ledger.InspectJournalGrantAttempt(t.Context(), fixture.request.JournalID); err == nil {
			t.Fatal("policy loss consumed a startup grant")
		}
		if _, err := ledger.InspectReservation(t.Context(), fixture.request.JournalID); err != nil {
			t.Fatalf("committed reservation was not retained for recovery: %v", err)
		}
	})

	t.Run("caller-replacement", func(t *testing.T) {
		fixture, _, _, ledger, _, _, authority := newCompositionDriver(t, compositionAuthorizationPolicy{})
		credentials, err := fixture.plan.CallerCredentials()
		if err != nil {
			t.Fatal(err)
		}
		replacementConnection, _, _ := runtimeCallerConfiguredConnection(t, "hold-after-disconnect", "unixpacket", fixture.caller.Payload(), credentials, true)
		replacement, err := ReceiveRuntimeCaller(t.Context(), replacementConnection, credentials)
		if err != nil {
			t.Fatal(err)
		}
		defer replacement.Close()
		orchestration, record, err := authority.Prepare(t.Context(), replacement)
		if err == nil || orchestration != nil || record.OperationID != uuid.Nil {
			t.Fatalf("caller replacement was accepted: orchestration=%v record=%+v err=%v", orchestration != nil, record, err)
		}
		if _, err := ledger.InspectReservation(t.Context(), fixture.request.JournalID); err == nil {
			t.Fatal("caller replacement created a reservation")
		}
	})

	t.Run("observer-loss", func(t *testing.T) {
		fixture, _, custody, ledger, _, _, authority := newCompositionDriver(t, compositionAuthorizationPolicy{})
		if err := custody.Revoke(); err != nil {
			t.Fatal(err)
		}
		orchestration, record, err := authority.Prepare(t.Context(), fixture.caller)
		if err == nil && orchestration != nil {
			defer orchestration.Close()
			if err := orchestration.ServeCaller(t.Context()); err == nil {
				t.Fatal("observer loss produced a Permit")
			}
			receipt, receiptErr := orchestration.CompositionReceipt(t.Context())
			if receiptErr == nil && receipt.Permit {
				t.Fatalf("observer loss produced permitted receipt: %+v", receipt)
			}
		}
		if record.OperationID != uuid.Nil {
			if _, err := ledger.InspectJournalGrantAttempt(t.Context(), fixture.request.JournalID); err == nil {
				t.Fatal("observer loss consumed a startup grant")
			}
		}
	})
}

func newCompositionDriver(t *testing.T, policy RuntimeStartupAuthorizationPolicy) (runtimeStartupFixture, RuntimeStartupReservationConfig, *RuntimeObserverCustody, *RuntimeStartupLedger, string, journalEndpointFixture, RuntimeStartupAuthority) {
	t.Helper()
	var custody *RuntimeObserverCustody
	fixture, reservationConfig := newRemoteReservationFixture(t, false, &custody)
	ledger, ledgerDirectory := startupTestLedger(t)
	peers := newJournalEndpointOwnerFixture(t, 0, nil, true, true)
	registry := reservationConfig.Registry.(*startupReservationRegistryFixture)
	registry.reserve = func(_ context.Context, request fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
		return fleet.RuntimeStartupReservation{RuntimeStartupRequest: request, Fresh: true, ReservedAt: time.Now().UTC(), PolicyAuthorization: []byte("fixture-authorization")}, nil
	}
	credentials, err := fixture.plan.CallerCredentials()
	if err != nil {
		t.Fatal(err)
	}
	authority, err := NewRuntimeStartupAuthority(RuntimeStartupAuthorityConfig{
		Ledger: ledger, Plan: fixture.plan, Pods: reservationConfig.Pods, Observer: reservationConfig.Observer,
		Custody: custody, Journal: reservationConfig.Journal, WorkerOwner: peers.worker.owner,
		Registry: registry, AuthorizationPolicy: policy,
		PolicyAuthorizationPublisher: func(context.Context, []byte) error { return nil },
		Credentials:                  []RuntimeCallerCredentials{credentials}, ObserverInterval: 50 * time.Millisecond,
		ObserverTimeout: 500 * time.Millisecond, ExchangeTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return fixture, reservationConfig, custody, ledger, ledgerDirectory, peers, authority
}
