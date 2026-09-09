package nodeagent

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"time"

	"github.com/vivym/vela/internal/runtimechannel"
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

func validateRemoteStartupOrchestrationConfig(config RemoteStartupOrchestrationConfig) error {
	if config.Reservation.Plan == nil || config.Reservation.Journal == nil || config.Reservation.Caller == nil || config.Reservation.Observer == nil || config.Reservation.Registry == nil || config.WorkerOwner == nil || config.Observer == nil || config.AuthorizationDigest == ([sha256.Size]byte{}) {
		return ErrRuntimeStartupLedger
	}
	if len(config.Credentials) == 0 || config.ExchangeTimeout <= 0 || config.ExchangeTimeout > runtimechannel.ExchangeTimeout || config.ObserverInterval <= 0 || config.ObserverInterval > time.Second || config.ObserverTimeout <= 0 || config.ObserverTimeout > 5*time.Second {
		return ErrRuntimeStartupLedger
	}
	for i, credential := range config.Credentials {
		if credential.UID == 0 || credential.GID == 0 || credential.UID == ^uint32(0) || credential.GID == ^uint32(0) || slices.Contains(config.Credentials[:i], credential) {
			return ErrRuntimeStartupLedger
		}
	}
	return nil
}

// PrepareRemoteStartupOrchestration performs one exact Fleet reservation, then
// constructs the read-only endpoint, operation-bound grant and explicit startup
// composition root. It sends no backend Permit and does not create a socket.
// Any failure after reservation is fail-closed; callers must close returned
// resources and must not retry the same ledger opportunity.
func (ledger *RuntimeStartupLedger) PrepareRemoteStartupOrchestration(ctx context.Context, config RemoteStartupOrchestrationConfig) (*RuntimeStartupOrchestration, RuntimeStartupReservationRecord, error) {
	if ledger == nil {
		return nil, RuntimeStartupReservationRecord{}, ErrRuntimeStartupLedger
	}
	if err := validateRemoteStartupOrchestrationConfig(config); err != nil {
		return nil, RuntimeStartupReservationRecord{}, err
	}
	var endpoint *JournalEndpoint
	transferred := false
	defer func() {
		if !transferred {
			if endpoint != nil {
				_ = endpoint.Close()
			}
			_ = config.Observer.Close()
		}
	}()
	record, err := ledger.ReserveRemote(ctx, config.Reservation)
	if err != nil {
		return nil, RuntimeStartupReservationRecord{}, err
	}
	ledger.mu.Lock()
	runtimeOwner := ledger.owners[record.JournalID]
	startup, ok := ledger.starts[record.JournalID]
	ledger.mu.Unlock()
	if runtimeOwner == nil {
		return nil, record, ErrRuntimeNamespaceOwnerLost
	}
	endpoint, err = NewReadOnlyJournalEndpoint(ctx, config.Reservation.Journal, runtimeOwner, config.WorkerOwner)
	if err != nil {
		return nil, record, err
	}
	grant, err := IssueReservedJournalWriteGrant(ctx, endpoint, runtimeOwner, config.WorkerOwner, record.OperationID, config.AuthorizationDigest, time.Minute)
	if err != nil {
		return nil, record, err
	}
	if !ok {
		return nil, record, ErrRuntimeStartupLedger
	}
	expected := startup.Request
	orchestration, err := NewRuntimeStartupOrchestration(RuntimeStartupOrchestrationConfig{Ledger: ledger, Plan: config.Reservation.Plan, ExpectedRequest: expected, Grant: grant, Observer: config.Observer, Credentials: config.Credentials, ObserverInterval: config.ObserverInterval, ObserverTimeout: config.ObserverTimeout, ExchangeTimeout: config.ExchangeTimeout})
	if err != nil {
		return nil, record, errors.Join(err, ErrRuntimeStartupLedger)
	}
	transferred = true
	return orchestration, record, nil
}
