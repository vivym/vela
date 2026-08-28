package releaseartifacts

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/vivym/vela/internal/releasebundle"
)

type VelaImageBuildRequest struct {
	SourceRoot     string
	Revision       string
	ImagePrefix    string
	BackendContext string
	BackendSHA256  string
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
	stagedContext, err := stageH3Backend(request.BackendContext, request.BackendSHA256)
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(stagedContext) }()
	request.BackendContext = stagedContext
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
	if err := VerifyH3Backend(request.BackendContext, request.BackendSHA256); err != nil {
		return VelaImageBuildRequest{}, err
	}
	return request, nil
}

func stageH3Backend(sourceContext, expectedSHA256 string) (string, error) {
	candidate, err := os.MkdirTemp("", ".vela-h3-backend-*")
	if err != nil {
		return "", fmt.Errorf("create private H3 backend context: %w", err)
	}
	cleanup := func(err error) (string, error) {
		_ = os.RemoveAll(candidate)
		return "", err
	}
	if err := os.Chmod(candidate, 0o700); err != nil {
		return cleanup(fmt.Errorf("protect private H3 backend context: %w", err))
	}
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return cleanup(fmt.Errorf("resolve private H3 backend context: %w", err))
	}
	candidate = resolved

	source, err := os.Open(filepath.Join(sourceContext, h3BackendArtifactName))
	if err != nil {
		return cleanup(fmt.Errorf("open H3 backend for staging: %w", err))
	}
	destinationPath := filepath.Join(candidate, h3BackendArtifactName)
	destination, err := os.OpenFile(destinationPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o555)
	if err != nil {
		_ = source.Close()
		return cleanup(fmt.Errorf("create staged H3 backend: %w", err))
	}
	_, copyErr := io.Copy(destination, source)
	sourceCloseErr := source.Close()
	destinationCloseErr := destination.Close()
	if copyErr != nil {
		return cleanup(fmt.Errorf("copy H3 backend: %w", copyErr))
	}
	if sourceCloseErr != nil {
		return cleanup(fmt.Errorf("close H3 backend source: %w", sourceCloseErr))
	}
	if destinationCloseErr != nil {
		return cleanup(fmt.Errorf("close staged H3 backend: %w", destinationCloseErr))
	}
	if err := os.Chmod(destinationPath, 0o555); err != nil {
		return cleanup(fmt.Errorf("set staged H3 backend mode: %w", err))
	}
	if err := syncFile(destinationPath); err != nil {
		return cleanup(fmt.Errorf("sync staged H3 backend: %w", err))
	}
	if err := syncDirectory(candidate); err != nil {
		return cleanup(fmt.Errorf("sync private H3 backend context: %w", err))
	}
	if err := VerifyH3Backend(candidate, expectedSHA256); err != nil {
		return cleanup(fmt.Errorf("verify staged H3 backend: %w", err))
	}
	return candidate, nil
}

func runVelaImageBake(
	ctx context.Context,
	request VelaImageBuildRequest,
	printDefinition bool,
) error {
	arguments := []string{"buildx", "bake"}
	if !printDefinition {
		arguments = append(arguments, "--allow=fs.read="+request.BackendContext)
	}
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
		"H3_BACKEND_CONTEXT":   request.BackendContext,
		"H3_BACKEND_SHA256":    request.BackendSHA256,
	})
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}
