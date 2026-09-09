package nodeagent

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/runtimechannel"
)

// RuntimeJournalObservation binds a trusted creation observer to one endpoint.
// It is live state, never recoverable authorization. The creator's policy and
// launch approval remain independent prerequisites. A failed check fences routes
// and requests original-process termination; it does not prove descendant exit.
// Close joins monitoring and closes the endpoint. Custody remains caller-owned
// so TargetExited can still be used before its owner closes it.
type RuntimeJournalObservation struct {
	custody           *RuntimeObserverCustody
	endpoint          *JournalEndpoint
	interval, timeout time.Duration
	ctx               context.Context
	cancel            context.CancelCauseFunc
	deadline          atomic.Pointer[time.Time]
	stopped           atomic.Bool
	stopOnce          sync.Once
	revokeErr         atomic.Pointer[error]
	done              chan struct{}
}

// ActivateObservedJournalWriteGrant owns monitoring from before consumption
// until Close or service cancellation. Callers must close the returned lifetime
// on uncertain Permit delivery. It neither issues a grant nor sends Permit.
// The interval must be in (0, 1s], timeout in (0, 5s]. A response expires no later
// than interval+timeout after its challenge began, including exchange queue time.
func (ledger *RuntimeStartupLedger) ActivateObservedJournalWriteGrant(ctx context.Context, plan *RuntimeLaunchPlan, grant *JournalWriteGrant, custody *RuntimeObserverCustody, interval, timeout time.Duration) (*RuntimeJournalObservation, RuntimeStartupGrantAttempt, error) {
	if err := contextError(ctx); err != nil {
		return nil, RuntimeStartupGrantAttempt{}, err
	}
	if ledger == nil || plan == nil || grant == nil || grant.endpoint == nil || grant.operationID == uuid.Nil || grant.authorizationDigest == ([sha256.Size]byte{}) || custody == nil || interval <= 0 || interval > time.Second || timeout <= 0 || timeout > 5*time.Second {
		return nil, RuntimeStartupGrantAttempt{}, ErrRuntimeObserverCustody
	}
	endpoint := grant.endpoint
	if endpoint.observation.Load() != nil {
		return nil, RuntimeStartupGrantAttempt{}, ErrRuntimeObserverCustody
	}
	// Match retained original handles; a numeric PID or unrelated healthy observer
	// cannot protect this route. No network exchange runs under the endpoint lock.
	// Never hold custody while waiting for journal Handle/Apply's endpoint lock.
	endpoint.mu.Lock()
	custody.mu.Lock()
	var err error
	if custody.revoked || !custody.started || custody.target == nil || endpoint.runtime == nil || !endpoint.readOnly || endpoint.grantPending || endpoint.grantUsed || endpoint.observation.Load() != nil {
		err = ErrRuntimeObserverCustody
	} else {
		err = runtimechannel.SameLiveProcess(int(custody.target.Fd()), int(endpoint.runtime.Fd()))
	}
	var observation *RuntimeJournalObservation
	if err == nil {
		lifetime, cancel := context.WithCancelCause(ctx)
		observation = &RuntimeJournalObservation{custody: custody, endpoint: endpoint, interval: interval, timeout: timeout, ctx: lifetime, cancel: cancel, done: make(chan struct{})}
		endpoint.observation.Store(observation)
	}
	custody.mu.Unlock()
	endpoint.mu.Unlock()
	if err != nil {
		return nil, RuntimeStartupGrantAttempt{}, err
	}
	go observation.run()
	record, err := ledger.ActivateReservedJournalWriteGrant(observation.ctx, plan, grant)
	if err != nil {
		return nil, RuntimeStartupGrantAttempt{}, errors.Join(err, observation.Close())
	}
	return observation, record, nil
}

func (observation *RuntimeJournalObservation) check(ctx context.Context) error {
	if observation.stopped.Load() || observation.expired() {
		return errors.Join(ErrRuntimeObserverCustody, observation.revoke(ErrRuntimeObserverCustody))
	}
	began := time.Now()
	check, cancel := context.WithTimeout(ctx, observation.timeout)
	defer cancel()
	if err := observation.custody.Check(check); err != nil {
		return errors.Join(err, observation.revoke(err))
	}
	deadline := began.Add(observation.interval + observation.timeout)
	// A late response may never revive an expired lifetime. CAS also prevents
	// concurrent activation/monitor checks from replacing a newer expiry.
	for {
		previous := observation.deadline.Load()
		if previous != nil && !time.Now().Before(*previous) {
			return errors.Join(ErrRuntimeObserverCustody, observation.revoke(ErrRuntimeObserverCustody))
		}
		if previous != nil && !previous.Before(deadline) {
			break
		}
		if observation.deadline.CompareAndSwap(previous, &deadline) {
			break
		}
	}
	if observation.stopped.Load() || observation.ctx.Err() != nil || !time.Now().Before(deadline) {
		return errors.Join(ErrRuntimeObserverCustody, observation.revoke(ErrRuntimeObserverCustody))
	}
	return nil
}

// valid is checked under the endpoint lock immediately before routing/activation.
// It never exchanges with the observer or waits for ledger/journal persistence.
func (observation *RuntimeJournalObservation) valid() bool {
	if observation == nil {
		return true
	}
	deadline := observation.deadline.Load()
	if deadline != nil && !time.Now().Before(*deadline) {
		// Only fence/cancel here; the monitor performs process revocation without
		// tying termination requests to endpoint/journal lock ownership.
		observation.stopped.Store(true)
		observation.cancel(ErrRuntimeObserverCustody)
	}
	return !observation.stopped.Load() && observation.ctx.Err() == nil && deadline != nil
}

func (observation *RuntimeJournalObservation) expired() bool {
	deadline := observation.deadline.Load()
	return deadline != nil && !time.Now().Before(*deadline)
}

func (observation *RuntimeJournalObservation) revoke(cause error) error {
	observation.stopOnce.Do(func() {
		// Fence first, without the endpoint lock: an in-flight journal Apply may be
		// blocked in fsync. Already admitted writes may still commit after revocation.
		observation.stopped.Store(true)
		observation.cancel(cause)
		if err := observation.custody.Revoke(); err != nil {
			observation.revokeErr.Store(&err)
		}
	})
	if err := observation.revokeErr.Load(); err != nil {
		return *err
	}
	return nil
}

func (observation *RuntimeJournalObservation) run() {
	defer close(observation.done)
	stop := context.AfterFunc(observation.ctx, func() { _ = observation.revoke(context.Cause(observation.ctx)) })
	defer stop()
	ticker := time.NewTicker(observation.interval)
	defer ticker.Stop()
	for {
		select {
		case <-observation.ctx.Done():
			_ = observation.revoke(context.Cause(observation.ctx))
			return
		case <-observation.custody.revokedSignal:
			_ = observation.revoke(ErrRuntimeObserverCustody)
			return
		case <-ticker.C:
			if err := observation.check(observation.ctx); err != nil {
				return
			}
		}
	}
}

func (observation *RuntimeJournalObservation) Close() error {
	if observation == nil {
		return nil
	}
	err := observation.revoke(ErrRuntimeObserverCustody)
	<-observation.done
	return errors.Join(err, observation.endpoint.Close())
}

// Done closes when the monitor has stopped and attempted original-process
// revocation. It does not assert process exit or completed journal I/O.
func (observation *RuntimeJournalObservation) Done() <-chan struct{} { return observation.done }

// Err preserves the first lifetime-ending cause, including failed asynchronous
// checks/revocation, for the owning Node service. Nil means no cancellation yet.
func (observation *RuntimeJournalObservation) Err() error {
	if err := observation.revokeErr.Load(); err != nil {
		return errors.Join(context.Cause(observation.ctx), *err)
	}
	return context.Cause(observation.ctx)
}
