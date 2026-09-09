package nodeagent

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
)

var ErrRuntimeStartupAuthority = errors.New("runtime startup authority source is incomplete or untrusted")

// RuntimeStartupAuthority is the Node-owned composition seam for one startup
// authority. Every field is an independently established source; this type has
// no defaults, history fallback or mock Permit path. A value is single-use in
// practice because Prepare consumes the ledger's reservation opportunity.
type RuntimeStartupAuthority struct {
	Ledger            *RuntimeStartupLedger
	Plan              *RuntimeLaunchPlan
	Pods              RuntimeLaunchPodReader
	Observer          *RuntimeContainerObserver
	Custody           *RuntimeObserverCustody
	Journal           *modelruntime.ExecutionJournalOwner
	WorkerOwner       *RuntimeNamespaceOwner
	Registry          RuntimeStartupRegistry
	AuthorizationHash [sha256.Size]byte
	Credentials       []RuntimeCallerCredentials
	ObserverInterval  time.Duration
	ObserverTimeout   time.Duration
	ExchangeTimeout   time.Duration
}

func (authority RuntimeStartupAuthority) validate(caller *RuntimeCaller) error {
	if authority.Ledger == nil || authority.Plan == nil || authority.Pods == nil || authority.Observer == nil || authority.Journal == nil || authority.WorkerOwner == nil || authority.Registry == nil || authority.Custody == nil || caller == nil || authority.AuthorizationHash == ([sha256.Size]byte{}) {
		return ErrRuntimeStartupAuthority
	}
	if len(authority.Credentials) == 0 || authority.ExchangeTimeout <= 0 || authority.ObserverInterval <= 0 || authority.ObserverInterval > time.Second || authority.ObserverTimeout <= 0 || authority.ObserverTimeout > 5*time.Second {
		return ErrRuntimeStartupAuthority
	}
	for i, credential := range authority.Credentials {
		if credential.UID == 0 || credential.GID == 0 || credential.UID == ^uint32(0) || credential.GID == ^uint32(0) || slices.Contains(authority.Credentials[:i], credential) {
			return ErrRuntimeStartupAuthority
		}
	}
	return nil
}

// Prepare authenticates the request already held by caller through the exact
// reservation/observation path and returns an orchestration whose ServeCaller
// consumes that same caller. It never parses a caller-supplied plan, creates a
// reservation from history, or fabricates authorization evidence.
func (authority RuntimeStartupAuthority) Prepare(ctx context.Context, caller *RuntimeCaller) (*RuntimeStartupOrchestration, RuntimeStartupReservationRecord, error) {
	if err := contextError(ctx); err != nil {
		return nil, RuntimeStartupReservationRecord{}, err
	}
	if err := authority.validate(caller); err != nil {
		return nil, RuntimeStartupReservationRecord{}, err
	}
	orchestration, record, err := authority.Ledger.PrepareRemoteStartupOrchestration(ctx, RemoteStartupOrchestrationConfig{
		Reservation: RuntimeStartupReservationConfig{
			Plan: authority.Plan, Pods: authority.Pods, Observer: authority.Observer,
			Caller: caller, Journal: authority.Journal, Registry: authority.Registry,
		},
		WorkerOwner: authority.WorkerOwner, Observer: authority.Custody,
		AuthorizationDigest: authority.AuthorizationHash, Credentials: authority.Credentials,
		ObserverInterval: authority.ObserverInterval, ObserverTimeout: authority.ObserverTimeout,
		ExchangeTimeout: authority.ExchangeTimeout,
	})
	if err != nil {
		return nil, record, err
	}
	return orchestration, record, nil
}
