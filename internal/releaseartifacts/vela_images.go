package releaseartifacts

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/vivym/vela/internal/releasebundle"
)

type VelaImageBuildRequest struct {
	SourceRoot  string
	Revision    string
	ImagePrefix string
}

func PrintVelaImageBuild(ctx context.Context, request VelaImageBuildRequest) error {
	request, err := validateVelaImageBuildInputs(ctx, request)
	if err != nil {
		return err
	}
	return runVelaImageBake(ctx, request, true)
}

func BuildVelaImages(ctx context.Context, request VelaImageBuildRequest) error {
	request, err := validateVelaImageBuildInputs(ctx, request)
	if err != nil {
		return err
	}
	return runVelaImageBake(ctx, request, false)
}

func validateVelaImageBuildInputs(
	ctx context.Context,
	request VelaImageBuildRequest,
) (VelaImageBuildRequest, error) {
	if ctx == nil {
		return VelaImageBuildRequest{}, errors.New("vela image build context is required")
	}
	sourceRoot, err := canonicalExistingDirectory(request.SourceRoot)
	if err != nil {
		return VelaImageBuildRequest{}, fmt.Errorf("resolve source root: %w", err)
	}
	request.SourceRoot = sourceRoot
	if !releasebundle.ValidRevision(request.Revision) {
		return VelaImageBuildRequest{}, errors.New("release revision is invalid")
	}
	if request.ImagePrefix == "" || request.ImagePrefix != strings.TrimSpace(request.ImagePrefix) {
		return VelaImageBuildRequest{}, errors.New("release image prefix is required")
	}
	return request, nil
}

func runVelaImageBake(
	ctx context.Context,
	request VelaImageBuildRequest,
	printDefinition bool,
) error {
	arguments := []string{"buildx", "bake"}
	arguments = append(arguments, "--file", "docker-bake.hcl", "vela-all")
	if printDefinition {
		arguments = append(arguments, "--print", "--progress=quiet")
	} else {
		arguments = append(arguments, "--load")
	}
	return runVelaImageBakeCommand(ctx, request, arguments, "run Docker Buildx Bake")
}

func runVelaImageBakeCommand(
	ctx context.Context,
	request VelaImageBuildRequest,
	arguments []string,
	operation string,
) error {
	command := exec.CommandContext(ctx, "docker", arguments...)
	command.Dir = request.SourceRoot
	command.Env = buildEnvironment(map[string]string{
		"RELEASE_REVISION":     request.Revision,
		"RELEASE_IMAGE_PREFIX": request.ImagePrefix,
	})
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}
