//go:build !darwin && !linux

package modelruntime

import (
	"errors"
	"os"
	"os/exec"
)

func configureDriverProcess(*exec.Cmd) error {
	return errors.New("ModelRuntime process-group teardown is supported only on Darwin and Linux")
}

func waitDriverProcessExit(*os.Process) error {
	return errors.New("ModelRuntime process exit observation is unsupported")
}

func killDriverProcessGroup(*os.Process) error {
	return errors.New("ModelRuntime process-group teardown is unsupported")
}
