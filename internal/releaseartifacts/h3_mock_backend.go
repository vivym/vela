package releaseartifacts

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// BuildH3MockBackend publishes a verified linux/amd64 mock backend context.
func BuildH3MockBackend(
	ctx context.Context,
	sourceRoot string,
	outputDirectory string,
) (string, error) {
	if ctx == nil {
		return "", errors.New("H3 mock backend build context is required")
	}
	sourceRoot, err := canonicalExistingDirectory(sourceRoot)
	if err != nil {
		return "", fmt.Errorf("resolve source root: %w", err)
	}
	outputDirectory, parent, err := canonicalNewOutputDirectory(outputDirectory)
	if err != nil {
		return "", fmt.Errorf("resolve output directory: %w", err)
	}
	candidate, err := os.MkdirTemp(parent, ".vela-h3-mock-backend-*")
	if err != nil {
		return "", fmt.Errorf("create H3 mock backend candidate: %w", err)
	}
	defer func() { _ = os.RemoveAll(candidate) }()
	if err := os.Chmod(candidate, 0o700); err != nil {
		return "", fmt.Errorf("protect H3 mock backend candidate: %w", err)
	}
	backendPath := filepath.Join(candidate, h3BackendArtifactName)
	command := exec.CommandContext(
		ctx,
		"go",
		"build",
		"-mod=readonly",
		"-trimpath",
		"-buildvcs=false",
		"-ldflags=-buildid= -s -w",
		"-o",
		backendPath,
		"./cmd/vela-h3-mock-backend",
	)
	command.Dir = sourceRoot
	command.Env = buildEnvironment(map[string]string{
		"CGO_ENABLED": "0",
		"GOARCH":      "amd64",
		"GOOS":        "linux",
	})
	if encoded, err := command.CombinedOutput(); err != nil {
		return "", fmt.Errorf(
			"build linux/amd64 H3 mock backend: %w: %s",
			err,
			strings.TrimSpace(string(encoded)),
		)
	}
	if err := os.Chmod(backendPath, 0o555); err != nil {
		return "", fmt.Errorf("set H3 mock backend mode: %w", err)
	}
	if err := syncFile(backendPath); err != nil {
		return "", fmt.Errorf("sync H3 mock backend: %w", err)
	}
	digest, _, err := digestFile(backendPath)
	if err != nil {
		return "", fmt.Errorf("digest H3 mock backend: %w", err)
	}
	if err := VerifyH3Backend(candidate, digest); err != nil {
		return "", fmt.Errorf("verify H3 mock backend candidate: %w", err)
	}
	if err := syncDirectory(candidate); err != nil {
		return "", fmt.Errorf("sync H3 mock backend candidate: %w", err)
	}
	if err := renameNoReplace(candidate, outputDirectory); err != nil {
		return "", fmt.Errorf("publish H3 mock backend: %w", err)
	}
	if err := syncDirectory(parent); err != nil {
		return "", fmt.Errorf("sync H3 mock backend parent: %w", err)
	}
	return digest, nil
}
