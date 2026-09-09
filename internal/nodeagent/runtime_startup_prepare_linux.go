package nodeagent

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"
)

// RemoteStartupOrchestrationConfig supplies the non-reconstructible Node
// objects for one startup. AuthorizationDigest identifies an independently
// checked policy result; this method only requires it to be non-zero and never
// treats the digest as proof. WorkerOwner must be the independently held owner
// of the paired Worker journal.
type RemoteStartupOrchestrationConfig struct {
	Reservation         RuntimeStartupReservationConfig
	WorkerOwner         *RuntimeNamespaceOwner
	Observer            *RuntimeObserverCustody
	AuthorizationDigest [sha256.Size]byte
	Credentials         []RuntimeCallerCredentials
	ObserverInterval    time.Duration
	ObserverTimeout     time.Duration
	ExchangeTimeout     time.Duration
}

// PrepareRemoteStartupOrchestration performs one exact Fleet reservation, then
// constructs the read-only endpoint, operation-bound grant and explicit startup
// composition root. It sends no backend Permit and does not create a socket.
// Any failure after reservation is fail-closed; callers must close returned
// resources and must not retry the same ledger opportunity.
func (ledger *RuntimeStartupLedger) PrepareRemoteStartupOrchestration(ctx context.Context, config RemoteStartupOrchestrationConfig) (*RuntimeStartupOrchestration, RuntimeStartupReservationRecord, error) {
	if ledger == nil || config.Reservation.Plan == nil || config.Reservation.Journal == nil || config.Reservation.Caller == nil || config.Reservation.Observer == nil || config.Reservation.Registry == nil || config.WorkerOwner == nil || config.Observer == nil || config.AuthorizationDigest == ([sha256.Size]byte{}) {
		return nil, RuntimeStartupReservationRecord{}, ErrRuntimeStartupLedger
	}
	record, err := ledger.ReserveRemote(ctx, config.Reservation)
	if err != nil {
		return nil, RuntimeStartupReservationRecord{}, err
	}
	runtimeOwner := ledger.owners[record.JournalID]
	if runtimeOwner == nil {
		return nil, record, ErrRuntimeNamespaceOwnerLost
	}
	endpoint, err := NewReadOnlyJournalEndpoint(ctx, config.Reservation.Journal, runtimeOwner, config.WorkerOwner)
	if err != nil {
		return nil, record, err
	}
	grant, err := IssueReservedJournalWriteGrant(ctx, endpoint, runtimeOwner, config.WorkerOwner, record.OperationID, config.AuthorizationDigest, time.Minute)
	if err != nil {
		_ = endpoint.Close()
		return nil, record, err
	}
	ledger.mu.Lock()
	startup, ok := ledger.starts[record.JournalID]
	ledger.mu.Unlock()
	if !ok {
		_ = endpoint.Close()
		return nil, record, ErrRuntimeStartupLedger
	}
	expected := startup.Request
	orchestration, err := NewRuntimeStartupOrchestration(RuntimeStartupOrchestrationConfig{Ledger: ledger, Plan: config.Reservation.Plan, ExpectedRequest: expected, Grant: grant, Observer: config.Observer, Credentials: config.Credentials, ObserverInterval: config.ObserverInterval, ObserverTimeout: config.ObserverTimeout, ExchangeTimeout: config.ExchangeTimeout})
	if err != nil {
		_ = endpoint.Close()
		return nil, record, errors.Join(err, ErrRuntimeStartupLedger)
	}
	return orchestration, record, nil
}
