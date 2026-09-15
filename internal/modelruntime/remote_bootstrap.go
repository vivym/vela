package modelruntime

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/strictjson"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

const MaximumRemoteBootstrapBytes = 4 << 20

var ErrRemoteBootstrap = errors.New("remote Runtime bootstrap is not a complete trusted Node configuration")

// RemoteRuntimeBootstrap is published by trusted Node assembly for one original
// incarnation. It contains public verifier keys, never signing keys or a permit.
// The Runtime consumes one protected snapshot; no workload environment selects
// launch/key files, local journal paths, local epochs or an alternate authorizer.
type RemoteRuntimeBootstrap struct {
	SchemaVersion     int                      `json:"schema_version"`
	Manifest          LaunchManifest           `json:"manifest"`
	AuthorityKeys     map[string][]byte        `json:"authority_keys"`
	RegistryKeys      map[string][]byte        `json:"registry_keys"`
	RegistryBinding   []byte                   `json:"registry_binding"`
	Identity          ExecutionJournalIdentity `json:"identity"`
	Startup           BackendLifecycleStatus   `json:"startup"`
	JournalSocket     string                   `json:"journal_socket"`
	StartupSocket     string                   `json:"startup_socket"`
	RuntimeSocket     string                   `json:"runtime_socket"`
	PIDFDBrokerSocket string                   `json:"pidfd_broker_socket,omitempty"`
	JournalTimeout    time.Duration            `json:"journal_timeout"`
	CancelTimeout     time.Duration            `json:"cancel_timeout"`
	ShutdownTimeout   time.Duration            `json:"shutdown_timeout"`
}

func EncodeRemoteRuntimeBootstrap(config RemoteRuntimeBootstrap) ([]byte, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	wire, err := json.Marshal(config)
	if err != nil || len(wire) > MaximumRemoteBootstrapBytes {
		return nil, errors.Join(ErrRemoteBootstrap, err)
	}
	return wire, nil
}

func ParseRemoteRuntimeBootstrap(wire []byte) (RemoteRuntimeBootstrap, error) {
	var config RemoteRuntimeBootstrap
	if len(wire) == 0 || len(wire) > MaximumRemoteBootstrapBytes || strictjson.RejectDuplicateKeys(wire) != nil {
		return config, ErrRemoteBootstrap
	}
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&config) != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) {
		return RemoteRuntimeBootstrap{}, ErrRemoteBootstrap
	}
	canonical, err := EncodeRemoteRuntimeBootstrap(config)
	if err != nil || !bytes.Equal(canonical, wire) {
		return RemoteRuntimeBootstrap{}, errors.Join(ErrRemoteBootstrap, err)
	}
	return config, nil
}

func (config RemoteRuntimeBootstrap) validate() error {
	if config.SchemaVersion != 1 || config.JournalTimeout <= 0 || config.JournalTimeout > 45*time.Second ||
		config.CancelTimeout < time.Millisecond || config.CancelTimeout > time.Minute || config.ShutdownTimeout < time.Millisecond || config.ShutdownTimeout > 10*time.Minute ||
		len(config.AuthorityKeys) == 0 || len(config.AuthorityKeys) > 32 || len(config.RegistryKeys) == 0 || len(config.RegistryKeys) > 32 {
		return ErrRemoteBootstrap
	}
	for _, path := range []string{config.JournalSocket, config.StartupSocket, config.RuntimeSocket} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 100 || strings.ContainsRune(path, '\x00') || path == "/" {
			return ErrRemoteBootstrap
		}
	}
	if config.PIDFDBrokerSocket != "" && (!filepath.IsAbs(config.PIDFDBrokerSocket) || filepath.Clean(config.PIDFDBrokerSocket) != config.PIDFDBrokerSocket || len(config.PIDFDBrokerSocket) > 100 || strings.ContainsRune(config.PIDFDBrokerSocket, '\x00') || config.PIDFDBrokerSocket == "/") {
		return ErrRemoteBootstrap
	}
	if config.JournalSocket == config.StartupSocket || config.JournalSocket == config.RuntimeSocket || config.StartupSocket == config.RuntimeSocket {
		return ErrRemoteBootstrap
	}
	manifest, err := EncodeLaunchManifest(config.Manifest)
	if err != nil || !config.Identity.Storage.Valid() || config.Startup.State != BackendLifecycleUnresolved || config.Startup.LaunchDigest != sha256.Sum256(manifest) || config.Startup.RecordedAt.IsZero() || config.Startup.RecordedAt.Location() != time.UTC {
		return errors.Join(ErrRemoteBootstrap, err)
	}
	validator, err := stageauthority.NewVerifier(config.AuthorityKeys, time.Now)
	if err != nil {
		return errors.Join(ErrRemoteBootstrap, err)
	}
	scope, err := executionScopeForManifest(config.Manifest, validator)
	if err != nil {
		return errors.Join(ErrRemoteBootstrap, err)
	}
	digest, err := scope.digest()
	if err != nil || digest != config.Identity.Scope {
		return errors.Join(ErrRemoteBootstrap, err)
	}
	verifier, err := journalbinding.NewVerifier(config.RegistryKeys)
	if err != nil {
		return errors.Join(ErrRemoteBootstrap, err)
	}
	var binding velav1.WorkerBootstrapBinding
	if err := proto.Unmarshal(config.RegistryBinding, &binding); err != nil {
		return errors.Join(ErrRemoteBootstrap, err)
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(&binding)
	if err != nil || !bytes.Equal(canonical, config.RegistryBinding) {
		return errors.Join(ErrRemoteBootstrap, err)
	}
	if err := verifier.VerifyJournal(&binding, journalbinding.RuntimeJournal, journalbinding.Journal{
		WorkerInstanceID: config.Manifest.WorkerInstanceID, WorkerInstanceEpoch: config.Manifest.WorkerInstanceEpoch,
		WorkerMemberID: config.Manifest.WorkerMemberID, WorkerMemberEpoch: config.Manifest.WorkerMemberEpoch,
		JournalID: config.Identity.JournalID, Scope: config.Identity.Scope,
	}); err != nil {
		return errors.Join(ErrRemoteBootstrap, err)
	}
	request := BackendStartupRequest{SchemaVersion: 1, NodeIdentity: binding.GetClaim().GetNodeIdentity(), RegistryBindingDigest: sha256.Sum256(config.RegistryBinding),
		JournalID: config.Identity.JournalID, JournalScope: config.Identity.Scope, IncarnationID: config.Startup.IncarnationID, LaunchDigest: config.Startup.LaunchDigest}
	return request.Validate()
}
