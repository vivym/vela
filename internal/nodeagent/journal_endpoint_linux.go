package nodeagent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"sync"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/runtimechannel"
	"golang.org/x/sys/unix"
)

// JournalEndpoint binds two independently approved original processes to one
// root-owned journal. It accepts no process, role, route or storage selector.
// Namespace-owner correlation alone is not Registry/Fleet startup approval;
// trusted Node orchestration must establish that authority before construction.
type JournalEndpoint struct {
	mu      sync.Mutex
	owner   *modelruntime.ExecutionJournalOwner
	runtime *os.File
	worker  *os.File
}

// NewJournalEndpoint retains independent pidfds. Closing the observations after
// construction does not lose the originals; closing this endpoint revokes both
// routes. It does not close the caller-owned journal.
func NewJournalEndpoint(ctx context.Context, owner *modelruntime.ExecutionJournalOwner, runtimeOwner, workerOwner *RuntimeNamespaceOwner) (*JournalEndpoint, error) {
	if err := owner.RequireRootCustody(ctx); err != nil {
		return nil, err
	}
	endpoint := &JournalEndpoint{owner: owner}
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
