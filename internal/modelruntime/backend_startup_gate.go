package modelruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/strictjson"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const maximumBackendStartupBytes = 4096

var (
	ErrBackendStartupGateRequired = errors.New("registry-bound backend first startup requires a Node gate")
	ErrBackendStartupDenied       = errors.New("node did not permit this exact backend startup intent")
)

// RuntimeBackendStartupGate is trusted server assembly, like BackendFactory.
// Returning nil permits only the current startup invocation to continue. The
// gate must independently authorize the declared intent; identity alone is not
// permission. Production assembly uses the authenticated Node socket client.
type RuntimeBackendStartupGate func(context.Context, BackendStartupRequest) error

// BackendStartupRequest is a declaration, not a journal-lock or launch proof.
// StartRuntimeServer derives it from its original held, Registry-bound journal
// after persisting the member-wide startup intent and before any factory.
type BackendStartupRequest struct {
	SchemaVersion         int               `json:"schema_version"`
	NodeIdentity          string            `json:"node_identity"`
	RegistryBindingDigest [sha256.Size]byte `json:"registry_binding_digest"`
	JournalID             uuid.UUID         `json:"journal_id"`
	JournalScope          [sha256.Size]byte `json:"journal_scope"`
	IncarnationID         uuid.UUID         `json:"incarnation_id"`
	LaunchDigest          [sha256.Size]byte `json:"launch_digest"`
	// Version 2 binds the path and exact canonical bytes consumed by the remote
	// CLI. These remain declarations until Node checks its independent history.
	BootstrapDigest [sha256.Size]byte `json:"bootstrap_digest,omitzero"`
	BootstrapPath   string            `json:"bootstrap_path,omitempty"`
}

// BackendStartupDecision is authenticated only by the surrounding Node channel.
// Encoding this value creates no authority. Permit requires an independent Node
// transaction and exact owner binding; this type implements neither of them.
type BackendStartupDecision struct {
	SchemaVersion int               `json:"schema_version"`
	RequestDigest [sha256.Size]byte `json:"request_digest"`
	Permit        bool              `json:"permit"`
}

func (request BackendStartupRequest) Validate() error {
	if !validDriverText(request.NodeIdentity, 253) ||
		request.RegistryBindingDigest == ([sha256.Size]byte{}) || request.JournalID == uuid.Nil ||
		request.JournalScope == ([sha256.Size]byte{}) || request.IncarnationID == uuid.Nil ||
		request.IncarnationID.Version() != 4 || request.IncarnationID.Variant() != uuid.RFC4122 || request.LaunchDigest == ([sha256.Size]byte{}) {
		return ErrBackendStartupDenied
	}
	switch request.SchemaVersion {
	case 1:
		if request.BootstrapDigest != ([sha256.Size]byte{}) || request.BootstrapPath != "" {
			return ErrBackendStartupDenied
		}
	case 2:
		if request.BootstrapDigest == ([sha256.Size]byte{}) || !validBootstrapConsumptionPath(request.BootstrapPath) {
			return ErrBackendStartupDenied
		}
	default:
		return ErrBackendStartupDenied
	}
	return nil
}

func validBootstrapConsumptionPath(path string) bool {
	return validDriverText(path, 2048) && filepath.IsAbs(path) && filepath.Clean(path) == path && path != "/"
}

func EncodeBackendStartupRequest(request BackendStartupRequest) ([]byte, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	wire, err := json.Marshal(request)
	if err != nil || len(wire) > maximumBackendStartupBytes {
		return nil, errors.Join(ErrBackendStartupDenied, err)
	}
	return wire, nil
}

func ParseBackendStartupRequest(document []byte) (BackendStartupRequest, error) {
	var request BackendStartupRequest
	if err := decodeBackendStartup(document, &request); err != nil {
		return BackendStartupRequest{}, err
	}
	if err := request.Validate(); err != nil {
		return BackendStartupRequest{}, err
	}
	return request, nil
}

func decodeBackendStartup(document []byte, value any) error {
	if len(document) == 0 || len(document) > maximumBackendStartupBytes {
		return ErrBackendStartupDenied
	}
	if err := strictjson.RejectDuplicateKeys(document); err != nil {
		return errors.Join(ErrBackendStartupDenied, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return errors.Join(ErrBackendStartupDenied, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrBackendStartupDenied
	}
	canonical, err := json.Marshal(value)
	if err != nil || !bytes.Equal(canonical, document) {
		return errors.Join(ErrBackendStartupDenied, err)
	}
	return nil
}

func (store *executionStateFile) authorizeBackendStartup(ctx context.Context, binding *velav1.WorkerBootstrapBinding, gate RuntimeBackendStartupGate) error {
	if gate == nil {
		return ErrBackendStartupGateRequired
	}
	if err := store.check(); err != nil {
		return err
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(binding)
	if err != nil {
		return err
	}
	lifecycle := store.state.BackendLifecycle
	if lifecycle == nil || lifecycle.State != BackendLifecycleUnresolved {
		return ErrBackendStartupDenied
	}
	request := BackendStartupRequest{SchemaVersion: 1, NodeIdentity: binding.GetClaim().GetNodeIdentity(),
		RegistryBindingDigest: sha256.Sum256(wire), JournalID: store.state.ID, JournalScope: store.state.Scope,
		IncarnationID: lifecycle.IncarnationID, LaunchDigest: lifecycle.LaunchDigest}
	if err := request.Validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	if err := gate(ctx, request); err != nil {
		return errors.Join(ErrBackendStartupDenied, err)
	}
	return errors.Join(store.check(), context.Cause(ctx))
}

// NewNodeBackendStartupGate creates the production client, not a Node issuer.
// Recovery-only startup never calls it and does not require a live Node socket.
func NewNodeBackendStartupGate(socketPath string) (RuntimeBackendStartupGate, error) {
	return NewNodeBackendStartupGateWithPIDFDBroker(socketPath, "")
}

// NewNodeBackendStartupGateWithPIDFDBroker supplies the optional host-side
// identity broker for legacy pidfds hidden by a nested PID namespace.
func NewNodeBackendStartupGateWithPIDFDBroker(socketPath, brokerSocket string) (RuntimeBackendStartupGate, error) {
	if !filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath || len(socketPath) > maxRuntimeSocketPathBytes || strings.ContainsRune(socketPath, 0) {
		return nil, errors.New("backend startup Node socket must be a canonical local path")
	}
	if brokerSocket != "" && (!filepath.IsAbs(brokerSocket) || filepath.Clean(brokerSocket) != brokerSocket || len(brokerSocket) > maxRuntimeSocketPathBytes || strings.ContainsRune(brokerSocket, 0)) {
		return nil, errors.New("pidfd broker socket must be a canonical local path")
	}
	return nodeBackendStartupGate(socketPath, brokerSocket), nil
}
