package releaseartifacts

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/distribution/reference"
	"github.com/vivym/vela/internal/releasebundle"
)

type H3RuntimeComposition struct {
	BaseImage        string
	CommandContext   string
	EncoderSHA256    string
	DiTSHA256        string
	VAEDecoderSHA256 string
}

type VelaImageBuildRequest struct {
	SourceRoot  string
	Revision    string
	ImagePrefix string
	H3Runtime   H3RuntimeComposition
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
	if err := validatePinnedH3RuntimeBase(request.H3Runtime.BaseImage); err != nil {
		return VelaImageBuildRequest{}, err
	}
	commandContext, err := canonicalExistingDirectory(request.H3Runtime.CommandContext)
	if err != nil {
		return VelaImageBuildRequest{}, fmt.Errorf("resolve H3 runtime command context: %w", err)
	}
	request.H3Runtime.CommandContext = commandContext
	if err := VerifyH3RuntimeCommands(
		commandContext,
		request.H3Runtime.EncoderSHA256,
		request.H3Runtime.DiTSHA256,
		request.H3Runtime.VAEDecoderSHA256,
	); err != nil {
		return VelaImageBuildRequest{}, fmt.Errorf("verify H3 runtime commands: %w", err)
	}
	return request, nil
}

func validatePinnedH3RuntimeBase(image string) error {
	named, err := reference.ParseNormalizedNamed(image)
	if err != nil || named.String() != image {
		return errors.New("H3 runtime base must be a canonical image reference")
	}
	if _, tagged := named.(reference.NamedTagged); tagged {
		return errors.New("H3 runtime base must not contain a tag")
	}
	canonical, pinned := named.(reference.Canonical)
	if !pinned || canonical.Digest().Algorithm().String() != "sha256" ||
		len(canonical.Digest().Encoded()) != 64 ||
		canonical.Digest().Encoded() == strings.Repeat("0", 64) {
		return errors.New("H3 runtime base must be pinned by a nonzero SHA-256 digest")
	}
	return nil
}

func runVelaImageBake(
	ctx context.Context,
	request VelaImageBuildRequest,
	printDefinition bool,
) error {
	arguments := []string{
		"buildx", "bake",
		"--allow=fs.read=" + request.H3Runtime.CommandContext,
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
		"RELEASE_REVISION":           request.Revision,
		"RELEASE_IMAGE_PREFIX":       request.ImagePrefix,
		"H3_RUNTIME_BASE":            request.H3Runtime.BaseImage,
		"H3_RUNTIME_COMMAND_CONTEXT": request.H3Runtime.CommandContext,
		"H3_ENCODER_SHA256":          request.H3Runtime.EncoderSHA256,
		"H3_DIT_SHA256":              request.H3Runtime.DiTSHA256,
		"H3_VAE_DECODER_SHA256":      request.H3Runtime.VAEDecoderSHA256,
	})
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}
