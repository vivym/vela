//go:build !linux

package main

import (
	"errors"

	"github.com/vivym/vela/internal/modelruntime"
)

func newRuntimeJournalWriter(config, *durableWorkerLaunch) (modelruntime.JournalCommandWriter, error) {
	return nil, errors.New("Node-owned Runtime journal transport requires Linux")
}
