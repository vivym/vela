package nodeagent

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/vivym/vela/internal/runtimechannel"
)

// RuntimeImageLaunch retains the manifest-bound image configuration and its
// measured default entrypoint. It is an observation, not a release signature,
// live process approval, effective mount check or startup permission.
type RuntimeImageLaunch struct {
	executable RuntimeImageExecutableObservation
	encoded    []byte
}

// InspectLaunch derives the config digest and executable path from the exact
// trusted single-platform image manifest. A caller cannot nominate another
// executable that happens to be present in that image. No image is pulled.
func (observer *RuntimeImageObserver) InspectLaunch(ctx context.Context, manifestDigest string) (*RuntimeImageLaunch, error) {
	launch := &RuntimeImageLaunch{}
	observation, err := observer.inspectExecutable(ctx, RuntimeImageTarget{ManifestDigest: manifestDigest}, launch)
	if err != nil {
		return nil, err
	}
	launch.executable = observation
	return launch, nil
}

func (launch *RuntimeImageLaunch) Executable() RuntimeImageExecutableObservation {
	if launch == nil {
		return RuntimeImageExecutableObservation{}
	}
	return launch.executable
}

func (launch *RuntimeImageLaunch) Configuration() (ocispec.Image, error) {
	if launch == nil || len(launch.encoded) == 0 {
		return ocispec.Image{}, ErrRuntimeImage
	}
	var configuration ocispec.Image
	err := json.Unmarshal(launch.encoded, &configuration)
	return configuration, err
}

func runtimeImageDefaultArguments(configuration ocispec.ImageConfig) ([]string, error) {
	arguments := append(slices.Clone(configuration.Entrypoint), configuration.Cmd...)
	if len(arguments) == 0 || len(arguments) > 256 || !validRuntimeImagePath(arguments[0]) || arguments[0] == "/" {
		return nil, ErrRuntimeImage
	}
	for _, argument := range arguments {
		if len(argument) > 64<<10 || strings.ContainsRune(argument, '\x00') {
			return nil, ErrRuntimeImage
		}
	}
	return arguments, nil
}

func (observer *RuntimeImageObserver) sameDaemon(container *RuntimeContainerObserver) error {
	if observer == nil || observer.local == nil || observer.mountReader == nil || container == nil || container.daemon == nil ||
		observer.local.nodeIdentity != container.nodeIdentity {
		return ErrRuntimeImage
	}
	daemon, reader := container.daemon, observer.mountReader
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if err := errors.Join(daemon.checkLocked(), reader.checkLocked()); err != nil {
		return err
	}
	return runtimechannel.SameLiveProcess(int(daemon.pidfd.Fd()), int(reader.pidfd.Fd()))
}
