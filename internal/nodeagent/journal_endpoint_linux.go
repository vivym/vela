package nodeagent

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/runtimechannel"
	"golang.org/x/sys/unix"
)

// JournalEndpoint binds two independently approved original processes to one
// root-owned journal. It accepts no process, role, route or storage selector.
// Namespace-owner correlation alone is not Registry/Fleet startup approval;
// trusted Node orchestration must establish that authority before construction.
type JournalEndpoint struct {
	mu           sync.Mutex
	owner        *modelruntime.ExecutionJournalOwner
	runtime      *os.File
	worker       *os.File
	readOnly     bool
	grantUsed    bool
	grantPending bool
}

// JournalWriteGrant is an in-memory, single-use capability issued only by
// trusted Node orchestration after an external startup decision. It carries no
// serializable permit and cannot be reconstructed from a receipt or history.
type JournalWriteGrant struct {
	endpoint            *JournalEndpoint
	nonce               [32]byte
	expires             time.Time
	operationID         uuid.UUID
	authorizationDigest [sha256.Size]byte
}

// IssueJournalWriteGrant is the narrow trusted boundary between an external
// startup authorization and the journal route. The caller must retain custody
// of the same original processes; this function does not contact Fleet or issue
// authorization itself.
func IssueJournalWriteGrant(ctx context.Context, endpoint *JournalEndpoint, runtimeOwner, workerOwner *RuntimeNamespaceOwner, lifetime time.Duration) (*JournalWriteGrant, error) {
	if endpoint == nil || lifetime <= 0 || lifetime > 5*time.Minute || runtimeOwner == nil || workerOwner == nil {
		return nil, ErrRuntimeObserverCustody
	}
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if endpoint.runtime == nil || endpoint.worker == nil || !endpoint.readOnly || endpoint.grantUsed || endpoint.grantPending {
		return nil, ErrRuntimeObserverCustody
	}
	if err := errors.Join(contextError(ctx), runtimechannel.PollLivePIDFD(int(endpoint.runtime.Fd())), runtimechannel.PollLivePIDFD(int(endpoint.worker.Fd()))); err != nil {
		return nil, err
	}
	runtimeFD, err := retainJournalProcess(ctx, runtimeOwner)
	if err != nil {
		return nil, err
	}
	workerFD, err := retainJournalProcess(ctx, workerOwner)
	if err != nil {
		_ = runtimeFD.Close()
		return nil, err
	}
	defer func() { _ = runtimeFD.Close() }()
	defer func() { _ = workerFD.Close() }()
	if err := errors.Join(runtimechannel.SameLiveProcess(int(endpoint.runtime.Fd()), int(runtimeFD.Fd())), runtimechannel.SameLiveProcess(int(endpoint.worker.Fd()), int(workerFD.Fd()))); err != nil {
		return nil, err
	}
	grant := &JournalWriteGrant{endpoint: endpoint, expires: time.Now().Add(lifetime)}
	if _, err := rand.Read(grant.nonce[:]); err != nil {
		return nil, err
	}
	return grant, nil
}

// IssueReservedJournalWriteGrant binds independently established startup
// authority to an exact Fleet operation. Like IssueJournalWriteGrant, this is
// trusted Node orchestration, not an approval verifier: a digest alone is no
// authority. The resulting grant can ONLY be activated through the startup
// ledger, never through ActivateJournalWriteGrant or from serialized history.
func IssueReservedJournalWriteGrant(ctx context.Context, endpoint *JournalEndpoint, runtimeOwner, workerOwner *RuntimeNamespaceOwner, operationID uuid.UUID, authorizationDigest [sha256.Size]byte, lifetime time.Duration) (*JournalWriteGrant, error) {
	if operationID == uuid.Nil || authorizationDigest == ([sha256.Size]byte{}) {
		return nil, ErrRuntimeObserverCustody
	}
	grant, err := IssueJournalWriteGrant(ctx, endpoint, runtimeOwner, workerOwner, lifetime)
	if err != nil {
		return nil, err
	}
	grant.operationID, grant.authorizationDigest = operationID, authorizationDigest
	return grant, nil
}

// ActivateJournalWriteGrant consumes an unbound component grant after checking
// both original handles. Operation-bound startup grants require the ledger's
// ActivateReservedJournalWriteGrant; receipts and history cannot activate either.
func (endpoint *JournalEndpoint) ActivateJournalWriteGrant(ctx context.Context, grant *JournalWriteGrant) error {
	if endpoint == nil || grant == nil {
		return ErrRuntimeObserverCustody
	}
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	return endpoint.activateJournalWriteGrantLocked(ctx, grant, false)
}

func (endpoint *JournalEndpoint) activateJournalWriteGrantLocked(ctx context.Context, grant *JournalWriteGrant, reserved bool) error {
	if endpoint.runtime == nil || endpoint.worker == nil || grant.endpoint != endpoint || endpoint.grantUsed || !endpoint.readOnly || endpoint.grantPending != reserved || (grant.operationID != uuid.Nil) != reserved || !time.Now().Before(grant.expires) || grant.nonce == ([32]byte{}) {
		return ErrRuntimeObserverCustody
	}
	if err := errors.Join(contextError(ctx), runtimechannel.PollLivePIDFD(int(endpoint.runtime.Fd())), runtimechannel.PollLivePIDFD(int(endpoint.worker.Fd()))); err != nil {
		return err
	}
	endpoint.readOnly = false
	endpoint.grantUsed = true
	grant.nonce = [32]byte{}
	return nil
}

// NewJournalEndpoint retains independent pidfds. Closing the observations after
// construction does not lose the originals; closing this endpoint revokes both
// routes. It does not close the caller-owned journal.
// This writable constructor requires independently established authority; use
// NewReadOnlyJournalEndpoint for startup snapshot reads before a grant.
func NewJournalEndpoint(ctx context.Context, owner *modelruntime.ExecutionJournalOwner, runtimeOwner, workerOwner *RuntimeNamespaceOwner) (*JournalEndpoint, error) {
	return newJournalEndpoint(ctx, owner, runtimeOwner, workerOwner, false)
}

// NewReadOnlyJournalEndpoint enrolls original processes for snapshot reads only.
// Every mutation, including restrictive Worker floors, is rejected before Apply.
// A Fleet receipt, caller reply or restart cannot upgrade it. Only trusted Node
// orchestration can issue and activate an in-memory JournalWriteGrant after an
// independent startup authorization; this constructor supplies no such authority.
func NewReadOnlyJournalEndpoint(ctx context.Context, owner *modelruntime.ExecutionJournalOwner, runtimeOwner, workerOwner *RuntimeNamespaceOwner) (*JournalEndpoint, error) {
	return newJournalEndpoint(ctx, owner, runtimeOwner, workerOwner, true)
}

func newJournalEndpoint(ctx context.Context, owner *modelruntime.ExecutionJournalOwner, runtimeOwner, workerOwner *RuntimeNamespaceOwner, readOnly bool) (*JournalEndpoint, error) {
	if err := owner.RequireRootCustody(ctx); err != nil {
		return nil, err
	}
	endpoint := &JournalEndpoint{owner: owner, readOnly: readOnly}
	var err error
	endpoint.runtime, err = retainJournalProcess(ctx, runtimeOwner)
	if err != nil {
		return nil, err
	}
	endpoint.worker, err = retainJournalProcess(ctx, workerOwner)
	if err != nil {
		_ = endpoint.Close()
		return nil, err
	}
	if runtimechannel.SameLiveProcess(int(endpoint.runtime.Fd()), int(endpoint.worker.Fd())) == nil {
		_ = endpoint.Close()
		return nil, errors.New("journal Runtime and Worker roles require different original processes")
	}
	return endpoint, nil
}

func retainJournalProcess(ctx context.Context, owner *RuntimeNamespaceOwner) (*os.File, error) {
	if owner == nil {
		return nil, ErrRuntimeNamespaceOwnerLost
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err := errors.Join(contextError(ctx), owner.checkLocked()); err != nil {
		return nil, err
	}
	fd := int(owner.pidfd.Fd())
	if err := runtimechannel.SameLiveProcess(fd, fd); err != nil {
		return nil, err
	}
	copyFD, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(copyFD), "journal-original-process"), nil
}

// Handle accepts an already authenticated one-shot caller. The owning Node
// accept loop must bound concurrent handshakes before calling this method.
// Returning bytes does not imply the client received a durable acknowledgement.
func (endpoint *JournalEndpoint) Handle(ctx context.Context, caller *RuntimeCaller) ([]byte, error) {
	if endpoint == nil || caller == nil || ctx == nil {
		return nil, ErrRuntimeCallerIdentity
	}
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if endpoint.runtime == nil || endpoint.worker == nil {
		return nil, ErrRuntimeNamespaceOwnerLost
	}
	caller.mu.Lock()
	_, err := caller.inspectLocked(ctx)
	role := modelruntime.JournalCallerRole("")
	if err == nil {
		if runtimechannel.SameLiveProcess(int(endpoint.runtime.Fd()), int(caller.pidfd.Fd())) == nil {
			role = modelruntime.JournalRuntimeRole
		} else if runtimechannel.SameLiveProcess(int(endpoint.worker.Fd()), int(caller.pidfd.Fd())) == nil {
			role = modelruntime.JournalWorkerRole
		} else {
			err = ErrRuntimeCallerIdentity
		}
	}
	caller.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if err := endpoint.owner.RequireRootCustody(ctx); err != nil {
		return nil, err
	}
	payload := caller.Payload()
	command, err := modelruntime.ParseJournalCommand(payload)
	if err != nil {
		return nil, err
	}
	if command.Read != nil {
		page, err := endpoint.owner.Read(ctx, *command.Read)
		response := modelruntime.JournalEndpointResponse{SchemaVersion: 1, RequestDigest: sha256.Sum256(payload)}
		switch {
		case err == nil:
			response.Page = &page
		case errors.Is(err, modelruntime.ErrJournalChanged):
			response.Error = "CHANGED"
		case errors.Is(err, modelruntime.ErrExecutionStateRecovery) || context.Cause(ctx) != nil:
			response.Error = "UNCERTAIN"
		default:
			response.Error = "REJECTED"
		}
		return json.Marshal(response)
	}
	if endpoint.readOnly {
		return json.Marshal(modelruntime.JournalEndpointResponse{SchemaVersion: 1, RequestDigest: sha256.Sum256(payload), Error: "REJECTED"})
	}
	if role == modelruntime.JournalWorkerRole {
		// A signed floor only restricts future admission. New non-admission
		// evidence requires the originally approved Runtime to remain live;
		// process replacement must go through independent Node reconciliation.
		if command.Floor == nil && runtimechannel.SameLiveProcess(int(endpoint.runtime.Fd()), int(endpoint.runtime.Fd())) != nil {
			return nil, ErrRuntimeNamespaceOwnerLost
		}
	}
	receipt, err := endpoint.owner.Apply(ctx, role, payload)
	response := modelruntime.JournalEndpointResponse{SchemaVersion: 1, RequestDigest: sha256.Sum256(payload)}
	if err == nil {
		response.Receipt = &receipt
	} else if errors.Is(err, modelruntime.ErrExecutionStateRecovery) || context.Cause(ctx) != nil {
		response.Error = "UNCERTAIN"
	} else {
		response.Error = "REJECTED"
	}
	return json.Marshal(response)
}

func (endpoint *JournalEndpoint) Close() error {
	if endpoint == nil {
		return nil
	}
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	var err error
	if endpoint.runtime != nil {
		err = endpoint.runtime.Close()
		endpoint.runtime = nil
	}
	if endpoint.worker != nil {
		err = errors.Join(err, endpoint.worker.Close())
		endpoint.worker = nil
	}
	return err
}
