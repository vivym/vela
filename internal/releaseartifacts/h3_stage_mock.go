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

type H3StageMockRuntimeDigests struct {
	Encoder    string
	DiT        string
	VAEDecoder string
}

func BuildH3StageMockRuntime(
	ctx context.Context,
	sourceRoot string,
	outputDirectory string,
) (H3StageMockRuntimeDigests, error) {
	if ctx == nil {
		return H3StageMockRuntimeDigests{}, errors.New("H3 Stage mock build context is required")
	}
	sourceRoot, err := canonicalExistingDirectory(sourceRoot)
	if err != nil {
		return H3StageMockRuntimeDigests{}, fmt.Errorf("resolve source root: %w", err)
	}
	outputDirectory, parent, err := canonicalNewOutputDirectory(outputDirectory)
	if err != nil {
		return H3StageMockRuntimeDigests{}, fmt.Errorf("resolve output directory: %w", err)
	}
	candidate, err := os.MkdirTemp(parent, ".vela-h3-stage-mock-*")
	if err != nil {
		return H3StageMockRuntimeDigests{}, fmt.Errorf("create H3 Stage mock candidate: %w", err)
	}
	defer func() { _ = os.RemoveAll(candidate) }()
	if err := os.Chmod(candidate, 0o700); err != nil {
		return H3StageMockRuntimeDigests{}, fmt.Errorf("protect H3 Stage mock candidate: %w", err)
	}
	digestByName := make(map[string]string, len(h3RuntimeCommandNames))
	encoderPath := filepath.Join(candidate, h3RuntimeCommandNames[0])
	command := exec.CommandContext(
		ctx, "go", "build", "-mod=readonly", "-trimpath", "-buildvcs=false",
		"-ldflags=-buildid= -s -w", "-o", encoderPath, "./cmd/vela-h3-stage-mock",
	)
	command.Dir = sourceRoot
	command.Env = buildEnvironment(map[string]string{
		"CGO_ENABLED": "0", "GOARCH": "amd64", "GOOS": "linux",
	})
	if encoded, err := command.CombinedOutput(); err != nil {
		return H3StageMockRuntimeDigests{}, fmt.Errorf(
			"build H3 Stage mock runtime: %w: %s",
			err, strings.TrimSpace(string(encoded)),
		)
	}
	if err := os.Chmod(encoderPath, 0o555); err != nil {
		return H3StageMockRuntimeDigests{}, fmt.Errorf("set H3 Stage mock runtime mode: %w", err)
	}
	if err := syncFile(encoderPath); err != nil {
		return H3StageMockRuntimeDigests{}, fmt.Errorf("sync H3 Stage mock runtime: %w", err)
	}
	digest, _, err := digestFile(encoderPath)
	if err != nil {
		return H3StageMockRuntimeDigests{}, fmt.Errorf("digest H3 Stage mock runtime: %w", err)
	}
	digestByName[h3RuntimeCommandNames[0]] = digest
	for _, name := range h3RuntimeCommandNames[1:] {
		path := filepath.Join(candidate, name)
		if err := os.Link(encoderPath, path); err != nil {
			return H3StageMockRuntimeDigests{}, fmt.Errorf("publish %s mock command: %w", name, err)
		}
		digestByName[name] = digest
	}
	digests := H3StageMockRuntimeDigests{
		Encoder: digestByName["h3-encoder"], DiT: digestByName["h3-dit"],
		VAEDecoder: digestByName["h3-vae-decoder"],
	}
	if err := VerifyH3RuntimeCommands(
		candidate, digests.Encoder, digests.DiT, digests.VAEDecoder,
	); err != nil {
		return H3StageMockRuntimeDigests{}, fmt.Errorf("verify H3 Stage mock commands: %w", err)
	}
	if err := syncDirectory(candidate); err != nil {
		return H3StageMockRuntimeDigests{}, fmt.Errorf("sync H3 Stage mock candidate: %w", err)
	}
	if err := renameNoReplace(candidate, outputDirectory); err != nil {
		return H3StageMockRuntimeDigests{}, fmt.Errorf("publish H3 Stage mock commands: %w", err)
	}
	if err := syncDirectory(parent); err != nil {
		return H3StageMockRuntimeDigests{}, fmt.Errorf("sync H3 Stage mock parent: %w", err)
	}
	return digests, nil
}
