//go:build !linux

package artifactvalidator

import "errors"

func NewProductionSandbox(SandboxConfig) (Sandbox, error) {
	return nil, errors.New("production Artifact inspection sandbox requires Linux")
}

func RunSandboxHelper([]string) error {
	return errors.New("artifact inspection sandbox helper requires Linux")
}
