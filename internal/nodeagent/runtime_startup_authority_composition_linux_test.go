//go:build linux

package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/runtimepolicy"
)

// compositionAuthorizationPolicy is deliberately test-only. It models the
// independent issuer boundary while keeping the reservation, grant and
// orchestration on the real Node composition path.
type compositionAuthorizationPolicy struct{}

type invalidCompositionAuthorizationPolicy struct{}

type lossCompositionAuthorizationPolicy struct{}

func (compositionAuthorizationPolicy) IssueRuntimeStartupAuthorization(_ context.Context, record RuntimeStartupReservationRecord) (RuntimeStartupAuthorizationEvidence, error) {
	reservationDigest, err := runtimepolicy.ReservationBindingDigest(record.OperationID, record.JournalID, record.RequestDigest, record.ReservedAt)
	if err != nil {
		return RuntimeStartupAuthorizationEvidence{}, err
	}
	now := time.Now().UTC()
	return RuntimeStartupAuthorizationEvidence{
		OperationID:       record.OperationID,
		JournalID:         record.JournalID,
		RequestDigest:     record.RequestDigest,
		ReservationDigest: reservationDigest,
		// Keep the fixture digest stable so the process-level harness can bind
		// its durable grant attempt to the same independently issued evidence.
		EvidenceDigest: sha256.Sum256([]byte("fixture startup authorization evidence; no permit")),
		IssuedAt:       now,
		ExpiresAt:      now.Add(time.Minute),
	}, nil
}

func (invalidCompositionAuthorizationPolicy) IssueRuntimeStartupAuthorization(context.Context, RuntimeStartupReservationRecord) (RuntimeStartupAuthorizationEvidence, error) {
	return RuntimeStartupAuthorizationEvidence{}, nil
}

func (lossCompositionAuthorizationPolicy) IssueRuntimeStartupAuthorization(context.Context, RuntimeStartupReservationRecord) (RuntimeStartupAuthorizationEvidence, error) {
	return RuntimeStartupAuthorizationEvidence{}, context.DeadlineExceeded
}

func TestRuntimeStartupAuthorityPrepareComposesReservationAndGrant(t *testing.T) {
	var custody *RuntimeObserverCustody
	fixture, reservationConfig := newRemoteReservationFixture(t, false, &custody)
	if custody == nil {
		t.Fatal("fixture did not create observer custody")
	}
	ledger, _ := startupTestLedger(t)
	peers := newJournalEndpointOwnerFixture(t, 0, nil, true, true)
	registry := reservationConfig.Registry.(*startupReservationRegistryFixture)
	registry.reserve = func(_ context.Context, request fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
		return fleet.RuntimeStartupReservation{
			RuntimeStartupRequest: request,
			Fresh:                 true,
			ReservedAt:            time.Now().UTC(),
			PolicyAuthorization:   []byte("temporary signed Fleet authorization"),
		}, nil
	}
	credentials, err := fixture.plan.CallerCredentials()
	if err != nil {
		t.Fatal(err)
	}
	authority, err := NewRuntimeStartupAuthority(RuntimeStartupAuthorityConfig{
		Ledger: ledger, Plan: fixture.plan, Pods: reservationConfig.Pods, Observer: reservationConfig.Observer,
		Custody: custody, Journal: reservationConfig.Journal, WorkerOwner: peers.worker.owner,
		Registry: registry, AuthorizationPolicy: compositionAuthorizationPolicy{},
		PolicyAuthorizationPublisher: func(context.Context, []byte) error { return nil },
		Credentials:                  []RuntimeCallerCredentials{credentials}, ObserverInterval: 50 * time.Millisecond,
		ObserverTimeout: 500 * time.Millisecond, ExchangeTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	orchestration, record, err := authority.Prepare(t.Context(), fixture.caller)
	if err != nil {
		t.Fatalf("compose authority: %v", err)
	}
	if orchestration == nil || record.OperationID == uuid.Nil || registry.calls != 1 {
		t.Fatalf("composition did not produce one reservation: orchestration=%v record=%+v calls=%d", orchestration != nil, record, registry.calls)
	}
	pending, err := orchestration.CompositionReceipt(t.Context())
	if err != nil || pending.Permit || pending.Outcome != "pending" || pending.Verify() != nil {
		t.Fatalf("pending composition receipt invalid: %+v err=%v", pending, err)
	}
	wire, err := orchestration.coordinator.HandleBackendStartupWithCaller(t.Context(), fixture.caller, fixture.request)
	var decision modelruntime.BackendStartupDecision
	if err != nil || json.Unmarshal(wire, &decision) != nil || !decision.Permit {
		t.Fatalf("backend Permit through composition failed: wire=%s err=%v", wire, err)
	}
	permitted, err := orchestration.CompositionReceipt(t.Context())
	if err != nil || !permitted.Permit || permitted.Outcome != "permitted" || permitted.Verify() != nil || permitted.OperationID != record.OperationID {
		t.Fatalf("permitted composition receipt invalid: %+v err=%v", permitted, err)
	}
	// A Permit is only useful as validation evidence when its typed receipt can
	// survive process boundaries. Persist the exact JSON bytes, replay them and
	// verify both the self-digest and the operation-bound authority material.
	receiptPath := filepath.Join(t.TempDir(), "runtime-startup-composition.json")
	receiptWire, err := json.Marshal(permitted)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(receiptPath, receiptWire, 0o600); err != nil {
		t.Fatal(err)
	}
	replayedWire, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	var replayed RuntimeStartupCompositionReceipt
	if err := json.Unmarshal(replayedWire, &replayed); err != nil {
		t.Fatal(err)
	}
	if err := replayed.Verify(); err != nil {
		t.Fatalf("persisted composition receipt failed self verification: %v", err)
	}
	// Bind the replay against the independently retained reservation record and
	// request, rather than feeding the receipt's own digests back as expected
	// values. This catches a receipt that is internally self-consistent but
	// belongs to a different operation or authority record.
	reservationWire, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	reservationDigest := sha256.Sum256(reservationWire)
	backendRequestWire, err := modelruntime.EncodeBackendStartupRequest(fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	backendRequestDigest := sha256.Sum256(backendRequestWire)
	if err := replayed.VerifyBinding(record.OperationID, record.JournalID, backendRequestDigest, reservationDigest, permitted.AuthorizationDigest); err != nil {
		t.Fatalf("persisted composition receipt lost operation binding: %v", err)
	}
	if err := orchestration.Close(); err != nil {
		t.Fatal(err)
	}
	revoked, err := orchestration.CompositionReceipt(t.Context())
	if err != nil || revoked.Permit || revoked.Outcome != "revoked" || revoked.Verify() != nil {
		t.Fatalf("revoked composition receipt invalid: %+v err=%v", revoked, err)
	}
	if retry, _, err := authority.Prepare(t.Context(), fixture.caller); err == nil || retry != nil {
		t.Fatalf("duplicate startup was accepted: orchestration=%v error=%v", retry != nil, err)
	}
}

func TestRuntimeStartupCompositionNodeRestartDoesNotReissueAuthority(t *testing.T) {
	var custody *RuntimeObserverCustody
	fixture, reservationConfig := newRemoteReservationFixture(t, false, &custody)
	ledger, directory := startupTestLedger(t)
	registry := reservationConfig.Registry.(*startupReservationRegistryFixture)
	registry.reserve = func(_ context.Context, request fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
		return fleet.RuntimeStartupReservation{RuntimeStartupRequest: request, Fresh: true, ReservedAt: time.Now().UTC(), PolicyAuthorization: []byte("temporary signed Fleet authorization")}, nil
	}
	credentials, err := fixture.plan.CallerCredentials()
	if err != nil {
		t.Fatal(err)
	}
	authority, err := NewRuntimeStartupAuthority(RuntimeStartupAuthorityConfig{
		Ledger: ledger, Plan: fixture.plan, Pods: reservationConfig.Pods, Observer: reservationConfig.Observer,
		Custody: custody, Journal: reservationConfig.Journal, WorkerOwner: newJournalEndpointOwnerFixture(t, 0, nil, true, true).worker.owner,
		Registry: registry, AuthorizationPolicy: compositionAuthorizationPolicy{},
		PolicyAuthorizationPublisher: func(context.Context, []byte) error { return nil },
		Credentials:                  []RuntimeCallerCredentials{credentials}, ObserverInterval: 50 * time.Millisecond,
		ObserverTimeout: 500 * time.Millisecond, ExchangeTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	orchestration, record, err := authority.Prepare(t.Context(), fixture.caller)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orchestration.coordinator.HandleBackendStartupWithCaller(t.Context(), fixture.caller, fixture.request); err != nil {
		t.Fatal(err)
	}
	if err := orchestration.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenRuntimeStartupLedger(t.Context(), directory, "cpu-node", false)
	if err != nil {
		t.Fatal(err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(recovered.Close)
	reservationConfig.RuntimeOwner = nil
	if retry, retryErr := recovered.ReserveRemote(t.Context(), reservationConfig); retryErr == nil || retryErr != ErrRuntimeStartupRecorded || retry.OperationID != uuid.Nil || registry.calls != 1 {
		t.Fatalf("Node restart reissued startup authority: retry=%+v err=%v calls=%d record=%+v", retry, retryErr, registry.calls, record)
	}
	if _, err := recovered.InspectJournalGrantAttempt(t.Context(), fixture.request.JournalID); err != nil {
		t.Fatalf("Node restart lost durable grant attempt: %v", err)
	}
}

func TestRuntimeStartupAuthorityCompositionFailsClosedOnPolicyOrFleetEvidence(t *testing.T) {
	for _, scenario := range []string{"invalid-policy-evidence", "missing-fleet-authorization"} {
		t.Run(scenario, func(t *testing.T) {
			var custody *RuntimeObserverCustody
			fixture, reservationConfig := newRemoteReservationFixture(t, false, &custody)
			ledger, _ := startupTestLedger(t)
			peers := newJournalEndpointOwnerFixture(t, 0, nil, true, true)
			registry := reservationConfig.Registry.(*startupReservationRegistryFixture)
			if scenario == "invalid-policy-evidence" {
				registry.reserve = func(_ context.Context, request fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
					return fleet.RuntimeStartupReservation{RuntimeStartupRequest: request, Fresh: true, ReservedAt: time.Now().UTC(), PolicyAuthorization: []byte("temporary signed Fleet authorization")}, nil
				}
			}
			credentials, err := fixture.plan.CallerCredentials()
			if err != nil {
				t.Fatal(err)
			}
			policy := RuntimeStartupAuthorizationPolicy(compositionAuthorizationPolicy{})
			if scenario == "invalid-policy-evidence" {
				policy = invalidCompositionAuthorizationPolicy{}
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
			orchestration, _, err := authority.Prepare(t.Context(), fixture.caller)
			if err == nil || orchestration != nil {
				t.Fatalf("invalid composition was accepted: orchestration=%v error=%v", orchestration != nil, err)
			}
		})
	}
}

func TestRuntimeStartupCompositionReceiptRejectsAuthorityGaps(t *testing.T) {
	var custody *RuntimeObserverCustody
	fixture, reservationConfig := newRemoteReservationFixture(t, false, &custody)
	ledger, _ := startupTestLedger(t)
	peers := newJournalEndpointOwnerFixture(t, 0, nil, true, true)
	registry := reservationConfig.Registry.(*startupReservationRegistryFixture)
	registry.reserve = func(_ context.Context, request fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
		return fleet.RuntimeStartupReservation{
			RuntimeStartupRequest: request,
			Fresh:                 true,
			ReservedAt:            time.Now().UTC(),
			PolicyAuthorization:   []byte("temporary signed Fleet authorization"),
		}, nil
	}
	credentials, err := fixture.plan.CallerCredentials()
	if err != nil {
		t.Fatal(err)
	}
	authority, err := NewRuntimeStartupAuthority(RuntimeStartupAuthorityConfig{
		Ledger: ledger, Plan: fixture.plan, Pods: reservationConfig.Pods, Observer: reservationConfig.Observer,
		Custody: custody, Journal: reservationConfig.Journal, WorkerOwner: peers.worker.owner,
		Registry: registry, AuthorizationPolicy: compositionAuthorizationPolicy{},
		PolicyAuthorizationPublisher: func(context.Context, []byte) error { return nil },
		Credentials:                  []RuntimeCallerCredentials{credentials}, ObserverInterval: 50 * time.Millisecond,
		ObserverTimeout: 500 * time.Millisecond, ExchangeTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	orchestration, _, err := authority.Prepare(t.Context(), fixture.caller)
	if err != nil {
		t.Fatal(err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(orchestration.Close)
	wire, err := orchestration.coordinator.HandleBackendStartupWithCaller(t.Context(), fixture.caller, fixture.request)
	if err != nil {
		t.Fatalf("backend Permit through composition failed: %v", err)
	}
	var decision modelruntime.BackendStartupDecision
	if err := json.Unmarshal(wire, &decision); err != nil || !decision.Permit {
		t.Fatalf("backend decision=%+v err=%v", decision, err)
	}
	receipt, err := orchestration.CompositionReceipt(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := receipt.Verify(); err != nil {
		t.Fatal(err)
	}
	if err := receipt.VerifyBinding(receipt.OperationID, receipt.JournalID, receipt.RequestDigest, receipt.ReservationDigest, receipt.AuthorizationDigest); err != nil {
		t.Fatalf("receipt binding verification failed: %v", err)
	}
	wrongOperation := uuid.New()
	if err := receipt.VerifyBinding(wrongOperation, receipt.JournalID, receipt.RequestDigest, receipt.ReservationDigest, receipt.AuthorizationDigest); err == nil {
		t.Fatal("receipt from another operation was accepted")
	}
	for _, mutate := range []func(*RuntimeStartupCompositionReceipt){
		func(r *RuntimeStartupCompositionReceipt) { r.ReservationDigest = [sha256.Size]byte{} },
		func(r *RuntimeStartupCompositionReceipt) { r.AuthorizationDigest = [sha256.Size]byte{} },
		func(r *RuntimeStartupCompositionReceipt) { r.RequestDigest = [sha256.Size]byte{} },
		func(r *RuntimeStartupCompositionReceipt) {
			r.Outcome = "unknown"
			r.ReceiptDigest = receiptDigest(*r)
		},
	} {
		invalid := receipt
		mutate(&invalid)
		invalid.ReceiptDigest = receiptDigest(invalid)
		if err := invalid.Verify(); err == nil {
			t.Fatalf("receipt with authority gap was accepted: %+v", invalid)
		}
	}
}

func TestRuntimeStartupCompositionReceiptFailsClosedOnIncompleteCoordinator(t *testing.T) {
	if receipt, err := (&RuntimeStartupOrchestration{coordinator: &RuntimeStartupCoordinator{}}).CompositionReceipt(t.Context()); receipt != (RuntimeStartupCompositionReceipt{}) || !errors.Is(err, ErrRuntimeStartupAuthority) {
		t.Fatalf("incomplete coordinator produced receipt=%+v err=%v", receipt, err)
	}
}

func TestRuntimeStartupAuthorityCompositionFailureMatrix(t *testing.T) {
	t.Run("policy-response-loss", func(t *testing.T) {
		var custody *RuntimeObserverCustody
		fixture, reservationConfig := newRemoteReservationFixture(t, false, &custody)
		ledger, _ := startupTestLedger(t)
		peers := newJournalEndpointOwnerFixture(t, 0, nil, true, true)
		registry := reservationConfig.Registry.(*startupReservationRegistryFixture)
		registry.reserve = func(_ context.Context, request fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
			return fleet.RuntimeStartupReservation{RuntimeStartupRequest: request, Fresh: true, ReservedAt: time.Now().UTC(), PolicyAuthorization: []byte("temporary signed Fleet authorization")}, nil
		}
		credentials, err := fixture.plan.CallerCredentials()
		if err != nil {
			t.Fatal(err)
		}
		authority, err := NewRuntimeStartupAuthority(RuntimeStartupAuthorityConfig{
			Ledger: ledger, Plan: fixture.plan, Pods: reservationConfig.Pods, Observer: reservationConfig.Observer,
			Custody: custody, Journal: reservationConfig.Journal, WorkerOwner: peers.worker.owner,
			Registry: reservationConfig.Registry, AuthorizationPolicy: lossCompositionAuthorizationPolicy{},
			PolicyAuthorizationPublisher: func(context.Context, []byte) error { return nil },
			Credentials:                  []RuntimeCallerCredentials{credentials}, ObserverInterval: 50 * time.Millisecond,
			ObserverTimeout: 500 * time.Millisecond, ExchangeTimeout: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		orchestration, record, err := authority.Prepare(t.Context(), fixture.caller)
		if err == nil || orchestration != nil || record.OperationID == uuid.Nil {
			t.Fatalf("policy response loss crossed authority boundary: orchestration=%v record=%+v err=%v", orchestration != nil, record, err)
		}
		if _, err := ledger.InspectJournalGrantAttempt(t.Context(), fixture.request.JournalID); err == nil {
			t.Fatal("policy response loss consumed a startup grant")
		}
		if reservation, inspectErr := ledger.InspectReservation(t.Context(), fixture.request.JournalID); inspectErr != nil || reservation.OperationID == uuid.Nil {
			t.Fatalf("policy response loss did not retain the committed reservation: %+v err=%v", reservation, inspectErr)
		}
	})

	t.Run("caller-replacement", func(t *testing.T) {
		var custody *RuntimeObserverCustody
		fixture, reservationConfig := newRemoteReservationFixture(t, false, &custody)
		ledger, _ := startupTestLedger(t)
		peers := newJournalEndpointOwnerFixture(t, 0, nil, true, true)
		credentials, err := fixture.plan.CallerCredentials()
		if err != nil {
			t.Fatal(err)
		}
		payload := fixture.caller.Payload()
		connection, _, _ := runtimeCallerConfiguredConnection(t, "hold-after-disconnect", "unixpacket", payload, credentials, true)
		replacement, err := ReceiveRuntimeCaller(t.Context(), connection, credentials)
		if err != nil {
			t.Fatal(err)
		}
		defer func(cleanup func() error) { _ = cleanup() }(replacement.Close)
		authority, err := NewRuntimeStartupAuthority(RuntimeStartupAuthorityConfig{
			Ledger: ledger, Plan: fixture.plan, Pods: reservationConfig.Pods, Observer: reservationConfig.Observer,
			Custody: custody, Journal: reservationConfig.Journal, WorkerOwner: peers.worker.owner,
			Registry: reservationConfig.Registry, AuthorizationPolicy: compositionAuthorizationPolicy{},
			PolicyAuthorizationPublisher: func(context.Context, []byte) error { return nil },
			Credentials:                  []RuntimeCallerCredentials{credentials}, ObserverInterval: 50 * time.Millisecond,
			ObserverTimeout: 500 * time.Millisecond, ExchangeTimeout: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if orchestration, record, err := authority.Prepare(t.Context(), replacement); err == nil || orchestration != nil || record.OperationID != uuid.Nil {
			t.Fatalf("caller replacement was accepted: orchestration=%v record=%+v err=%v", orchestration != nil, record, err)
		}
		if _, err := ledger.InspectReservation(t.Context(), fixture.request.JournalID); err == nil {
			t.Fatal("caller replacement created a reservation")
		}
	})

	t.Run("observer-loss", func(t *testing.T) {
		var custody *RuntimeObserverCustody
		fixture, reservationConfig := newRemoteReservationFixture(t, false, &custody)
		ledger, _ := startupTestLedger(t)
		peers := newJournalEndpointOwnerFixture(t, 0, nil, true, true)
		registry := reservationConfig.Registry.(*startupReservationRegistryFixture)
		registry.reserve = func(_ context.Context, request fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
			return fleet.RuntimeStartupReservation{RuntimeStartupRequest: request, Fresh: true, ReservedAt: time.Now().UTC(), PolicyAuthorization: []byte("temporary signed Fleet authorization")}, nil
		}
		credentials, err := fixture.plan.CallerCredentials()
		if err != nil {
			t.Fatal(err)
		}
		authority, err := NewRuntimeStartupAuthority(RuntimeStartupAuthorityConfig{
			Ledger: ledger, Plan: fixture.plan, Pods: reservationConfig.Pods, Observer: reservationConfig.Observer,
			Custody: custody, Journal: reservationConfig.Journal, WorkerOwner: peers.worker.owner,
			Registry: registry, AuthorizationPolicy: compositionAuthorizationPolicy{},
			PolicyAuthorizationPublisher: func(context.Context, []byte) error { return nil },
			Credentials:                  []RuntimeCallerCredentials{credentials}, ObserverInterval: 50 * time.Millisecond,
			ObserverTimeout: 500 * time.Millisecond, ExchangeTimeout: time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := custody.Revoke(); err != nil {
			t.Fatal(err)
		}
		orchestration, _, err := authority.Prepare(t.Context(), fixture.caller)
		if err != nil {
			if _, grantErr := ledger.InspectJournalGrantAttempt(t.Context(), fixture.request.JournalID); grantErr == nil {
				t.Fatal("observer loss consumed a startup grant before failing")
			}
			return
		}
		if orchestration == nil {
			t.Fatal("observer loss returned no orchestration")
		}
		defer func(cleanup func() error) { _ = cleanup() }(orchestration.Close)
		if err := orchestration.ServeCaller(t.Context()); err == nil {
			t.Fatal("observer loss produced a Permit")
		}
		receipt, receiptErr := orchestration.CompositionReceipt(t.Context())
		if receiptErr != nil || receipt.Verify() != nil || receipt.Permit {
			t.Fatalf("observer loss receipt is not failed closed: %+v err=%v", receipt, receiptErr)
		}
	})
}
