package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/runtimechannel"
)

// ActivateReservedJournalWriteGrant joins durable consumption and the in-memory
// route transition. It accepts an independently issued, operation-bound grant,
// never an inspected RuntimeStartupGrantAttempt or reservation receipt. The
// startup authority issuer must already have established effective launch and
// execution continuity; this method does not replace those checks or send a
// backend Permit. Caller orchestration must revoke the endpoint on later lost
// replies, observer failure or cancellation of the running service.
//
// Lock order is ledger -> activation -> endpoint. The endpoint lock is NOT held across fsync,
// so reads and Close remain available. Once claimed, any error leaves that
// endpoint read-only/closed and permanently blocks alternative grant attempts.
// Closing the live ledger also closes every endpoint activated through it.
func (ledger *RuntimeStartupLedger) ActivateReservedJournalWriteGrant(ctx context.Context, plan *RuntimeLaunchPlan, grant *JournalWriteGrant) (RuntimeStartupGrantAttempt, error) {
	if err := contextError(ctx); err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	if ledger == nil || plan == nil || grant == nil || grant.endpoint == nil || grant.operationID == uuid.Nil || grant.authorizationDigest == ([sha256.Size]byte{}) {
		return RuntimeStartupGrantAttempt{}, ErrRuntimeObserverCustody
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if err := errors.Join(ledger.check(), context.Cause(ctx)); err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	ledger.activationMu.Lock()
	closing := ledger.activationClosed
	ledger.activationMu.Unlock()
	if closing {
		return RuntimeStartupGrantAttempt{}, ErrRuntimeStartupLedger
	}
	endpoint := grant.endpoint
	status, err := endpoint.owner.Status(ctx)
	if err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	startup, exists := ledger.starts[status.JournalID]
	if !exists || startup.Remote == nil || startup.OperationID != grant.operationID {
		return RuntimeStartupGrantAttempt{}, ErrRuntimeStartupLedger
	}
	request, err := modelruntime.EncodeBackendStartupRequest(startup.Request)
	if err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	if _, _, err := parseRuntimeStartupPlan(plan, request); err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	var manifest modelruntime.LaunchManifest
	if err := json.Unmarshal(plan.manifest, &manifest); err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	original, err := retainJournalProcess(ctx, ledger.owners[status.JournalID])
	if err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	defer func() { _ = original.Close() }()
	// Claim the endpoint without releasing any write route. In particular, an
	// ordinary grant issued before this claim cannot bypass durable consumption.
	endpoint.mu.Lock()
	if endpoint.runtime == nil || endpoint.worker == nil || !endpoint.readOnly || endpoint.grantUsed || endpoint.grantPending ||
		grant.nonce == ([32]byte{}) || !time.Now().Before(grant.expires) {
		endpoint.mu.Unlock()
		return RuntimeStartupGrantAttempt{}, ErrRuntimeObserverCustody
	}
	err = errors.Join(context.Cause(ctx), runtimechannel.SameLiveProcess(int(original.Fd()), int(endpoint.runtime.Fd())), runtimechannel.PollLivePIDFD(int(endpoint.worker.Fd())))
	if err == nil {
		endpoint.grantPending = true
	}
	endpoint.mu.Unlock()
	if err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	// Burn the supplied capability on success AND on any failure after claim.
	defer func() {
		endpoint.mu.Lock()
		grant.nonce = [32]byte{}
		endpoint.mu.Unlock()
	}()
	checkJournal := func() error {
		if observation := endpoint.observation.Load(); observation != nil {
			if err := observation.check(ctx); err != nil {
				return err
			}
		}
		// Loss of the ledger's custody during persistence cannot be masked by
		// the temporary duplicate retained earlier in this call.
		retained, err := retainJournalProcess(ctx, ledger.owners[startup.Request.JournalID])
		if err != nil {
			return err
		}
		_ = retained.Close()
		if err := endpoint.owner.RequireRootCustody(ctx); err != nil {
			return err
		}
		snapshot, err := endpoint.owner.InspectStartup(ctx, manifest, startup.Request)
		if err != nil {
			return err
		}
		current := snapshot.Status()
		if snapshot.Digest() != startup.Remote.JournalDigest || current.JournalID != startup.Remote.JournalIdentity.JournalID ||
			current.Scope != startup.Remote.JournalIdentity.Scope || current.Storage != startup.Remote.JournalIdentity.Storage {
			return ErrRuntimeStartupLedger
		}
		return nil
	}
	if err := checkJournal(); err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	record, err := ledger.consumeJournalGrantAttemptLocked(ctx, startup.Request.JournalID, grant.operationID, grant.authorizationDigest)
	if err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	if err := checkJournal(); err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	// Close can fence activation without waiting for this transaction's fsync.
	ledger.activationMu.Lock()
	defer ledger.activationMu.Unlock()
	if ledger.activationClosed {
		return RuntimeStartupGrantAttempt{}, ErrRuntimeStartupLedger
	}
	endpoint.mu.Lock()
	err = endpoint.activateJournalWriteGrantLocked(ctx, grant, true)
	endpoint.mu.Unlock()
	if err != nil {
		return RuntimeStartupGrantAttempt{}, err
	}
	if ledger.activatedEndpoints == nil {
		ledger.activatedEndpoints = make(map[uuid.UUID]*JournalEndpoint)
	}
	ledger.activatedEndpoints[record.JournalID] = endpoint
	return record, nil
}
