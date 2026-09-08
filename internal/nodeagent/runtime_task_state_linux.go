package nodeagent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
	"google.golang.org/grpc"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

var ErrRuntimeTaskState = errors.New("node state directory does not match the authenticated containerd filesystem view")

type runtimeContainerStatusReader interface {
	Status(context.Context, *runtimev1.StatusRequest, ...grpc.CallOption) (*runtimev1.StatusResponse, error)
}

// containerd v2.3.1 reports its resolved CRI plugin state path via Status.
// plugins/cri/runtime/plugin.go derives it from the daemon state directory,
// preserving the historical io.containerd.grpc.v1.cri suffix. This is daemon
// configuration, not the mutable spec stored on a container metadata object.
func runtimeDaemonStateDirectory(configuration string) (string, error) {
	if len(configuration) == 0 || len(configuration) > 1<<20 || strictjson.RejectDuplicateKeys([]byte(configuration)) != nil {
		return "", ErrRuntimeTaskState
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal([]byte(configuration), &document); err != nil {
		return "", errors.Join(ErrRuntimeTaskState, err)
	}
	var state string
	if err := json.Unmarshal(document["stateDir"], &state); err != nil || !filepath.IsAbs(state) || filepath.Clean(state) != state ||
		strings.ContainsRune(state, '\x00') || filepath.Base(state) != "io.containerd.grpc.v1.cri" || filepath.Dir(state) == "/" {
		return "", errors.Join(ErrRuntimeTaskState, err)
	}
	return filepath.Dir(state), nil
}

func (observer *RuntimeContainerObserver) openTaskBundle(ctx context.Context, stateDirectory string, target RuntimeContainerTarget) (*os.File, string, error) {
	if observer == nil || observer.daemon == nil || observer.check == nil {
		return nil, "", ErrRuntimeTaskState
	}
	reader, ok := observer.reader.(runtimeContainerStatusReader)
	if !ok {
		return nil, "", ErrRuntimeTaskState
	}
	if err := errors.Join(observer.check(), ctx.Err(), target.Validate()); err != nil {
		return nil, "", err
	}
	status, err := reader.Status(ctx, &runtimev1.StatusRequest{Verbose: true})
	if err != nil {
		return nil, "", err
	}
	daemonState, err := runtimeDaemonStateDirectory(status.GetInfo()["config"])
	if err != nil {
		return nil, "", err
	}
	configured, err := securefile.OpenTrustedDirectory(stateDirectory)
	if err != nil {
		return nil, "", errors.Join(ErrRuntimeTaskState, err)
	}
	defer func() { _ = configured.Close() }()
	actual, err := observer.daemon.openDirectory(daemonState)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = actual.Close() }()
	configuredInfo, configuredErr := configured.Stat()
	actualInfo, actualErr := actual.Stat()
	if err := errors.Join(configuredErr, actualErr); err != nil || !os.SameFile(configuredInfo, actualInfo) {
		return nil, "", errors.Join(ErrRuntimeTaskState, err)
	}
	// Read the bundle through the daemon's view as well. Comparing only the
	// state inode would not constrain different mounts below a Node-side alias.
	bundle, err := observer.daemon.openDirectory(filepath.Join(daemonState, "io.containerd.runtime.v2.task", "k8s.io", target.ContainerID))
	if err != nil {
		return nil, "", err
	}
	if err := errors.Join(observer.check(), ctx.Err()); err != nil {
		_ = bundle.Close()
		return nil, "", err
	}
	return bundle, daemonState, nil
}
