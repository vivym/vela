package nodeagent

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"

	bootstrap "github.com/containerd/containerd/api/runtime/bootstrap/v1"
	"github.com/containerd/containerd/api/types/runc/options"
	"google.golang.org/protobuf/proto"
)

var ErrRuntimeTaskMechanism = errors.New("runtime task options or hooks do not match the supported launch policy")

func validateRuntimeTaskBootstrap(data []byte) error {
	var result bootstrap.BootstrapResult
	if err := json.Unmarshal(data, &result); err != nil || result.Version != 3 || result.Protocol != "ttrpc" || result.Address == "" ||
		len(result.Capabilities) != 0 || len(result.Metadata) != 0 {
		return errors.Join(ErrRuntimeTaskMechanism, err)
	}
	encoded, err := json.Marshal(&result)
	if err != nil || !bytes.Equal(data, encoded) {
		return errors.Join(ErrRuntimeTaskMechanism, err)
	}
	return nil
}

// RuntimeTaskRuntimePolicy is trusted Node deployment policy, never a workload
// request. All three paths must be explicit; resolving an empty binary through
// the shim's PATH is not an approved runtime selection.
type RuntimeTaskRuntimePolicy struct {
	ShimBinaryPath    string
	RuntimeBinaryPath string
	RuntimeStateRoot  string
	SystemdCgroup     bool
}

type RuntimeTaskFileObservation struct {
	Digest [sha256.Size]byte
	Bytes  int64
	Device uint64
	Inode  uint64
}

// RuntimeOptions returns an independent copy of the actual bundle options.
// The shim writes encoding/json, not protojson. Its canonical output also
// excludes duplicate/case-folded keys, unknown fields, nulls and extra values.
func (launch *RuntimeTaskLaunch) RuntimeOptions() (*options.Options, error) {
	if launch == nil {
		return nil, ErrRuntimeTaskMechanism
	}
	return parseRuntimeTaskOptions(launch.optionsEncoded)
}

func parseRuntimeTaskOptions(data []byte) (*options.Options, error) {
	if len(data) == 0 || len(data) > 64<<10 {
		return nil, ErrRuntimeTaskMechanism
	}
	var value options.Options
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, errors.Join(ErrRuntimeTaskMechanism, err)
	}
	encoded, err := json.Marshal(&value)
	if err != nil || !bytes.Equal(data, encoded) {
		return nil, errors.Join(ErrRuntimeTaskMechanism, err)
	}
	return &value, nil
}

// CheckRuntimeMechanism checks retained bytes, not mutable public observation
// fields. This is a content-policy prerequisite, NOT executable attestation,
// effective mount approval, current process authority or a startup grant.
// Only explicit runc/shim paths, explicit runc state, cgroup mode and otherwise
// default options are supported; all OCI hooks are rejected.
func (launch *RuntimeTaskLaunch) CheckRuntimeMechanism(policy RuntimeTaskRuntimePolicy) error {
	for _, path := range []string{policy.ShimBinaryPath, policy.RuntimeBinaryPath, policy.RuntimeStateRoot} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || len(path) > 4096 || strings.ContainsRune(path, '\x00') {
			return ErrRuntimeTaskMechanism
		}
	}
	value, err := launch.RuntimeOptions()
	if err != nil {
		return err
	}
	expected := &options.Options{BinaryName: policy.RuntimeBinaryPath, Root: policy.RuntimeStateRoot, SystemdCgroup: policy.SystemdCgroup}
	if !proto.Equal(value, expected) || string(launch.runtimeEncoded) != policy.RuntimeBinaryPath || string(launch.shimEncoded) != policy.ShimBinaryPath {
		return ErrRuntimeTaskMechanism
	}
	configuration, err := launch.Configuration()
	if err != nil || configuration.Hooks != nil {
		return errors.Join(ErrRuntimeTaskMechanism, err)
	}
	return nil
}

func runtimeTaskFileObservation(data []byte, identity runtimeTaskFileIdentity) RuntimeTaskFileObservation {
	return RuntimeTaskFileObservation{Digest: sha256.Sum256(data), Bytes: int64(len(data)), Device: identity.device, Inode: identity.inode}
}
