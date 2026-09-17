package main

import (
	"errors"
	"path/filepath"

	"github.com/vivym/vela/internal/modelruntime"
)

func newRuntimeJournalWriter(configuration config, launch *durableWorkerLaunch) (modelruntime.JournalCommandWriter, error) {
	if launch == nil || !filepath.IsAbs(configuration.workerJournalSocket) ||
		filepath.Clean(configuration.workerJournalSocket) != configuration.workerJournalSocket ||
		!filepath.IsAbs(configuration.workerJournalPIDFDBrokerSocket) ||
		filepath.Clean(configuration.workerJournalPIDFDBrokerSocket) != configuration.workerJournalPIDFDBrokerSocket {
		return nil, errors.New("runtime journal writer requires verified Node-owned journal transport")
	}
	identity, err := runtimeJournalIdentity(launch.admission.RegistryBinding)
	if err != nil {
		return nil, err
	}
	// Node publishes both authenticated journal endpoints in the same signed
	// member directory. The broker binds each exchange to this live Worker PID.
	return modelruntime.UnixRuntimeJournalTransport{
		Socket:            filepath.Join(filepath.Dir(configuration.workerJournalSocket), "runtime-journal.sock"),
		PIDFDBrokerSocket: configuration.workerJournalPIDFDBrokerSocket,
		Identity:          identity,
	}, nil
}
