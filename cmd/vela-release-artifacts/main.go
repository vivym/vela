package main

import (
	"context"
	"fmt"
	"os"

	"github.com/vivym/vela/internal/releaseartifacts"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(arguments []string) int {
	if len(arguments) == 3 && arguments[0] == "build-h3-mock-backend" {
		digest, err := releaseartifacts.BuildH3MockBackend(
			context.Background(), arguments[1], arguments[2],
		)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "build H3 mock backend: %v\n", err)
			return 1
		}
		_, _ = fmt.Fprintf(
			os.Stdout,
			"H3_BACKEND_CONTEXT=%s\nH3_BACKEND_SHA256=%s\n",
			arguments[2],
			digest,
		)
		return 0
	}
	if len(arguments) == 3 && arguments[0] == "build-h3-stage-mock-runtime" {
		digests, err := releaseartifacts.BuildH3StageMockRuntime(
			context.Background(), arguments[1], arguments[2],
		)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "build H3 Stage mock runtime: %v\n", err)
			return 1
		}
		_, _ = fmt.Fprintf(
			os.Stdout,
			"H3_RUNTIME_COMMAND_CONTEXT=%s\nH3_ENCODER_SHA256=%s\nH3_DIT_SHA256=%s\nH3_VAE_DECODER_SHA256=%s\n",
			arguments[2], digests.Encoder, digests.DiT, digests.VAEDecoder,
		)
		return 0
	}
	if len(arguments) == 4 && arguments[0] == "build-host-packages" {
		if err := releaseartifacts.BuildHostPackages(
			context.Background(), arguments[1], arguments[2], arguments[3],
		); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "build host packages: %v\n", err)
			return 1
		}
		_, _ = fmt.Fprintln(os.Stdout, arguments[3]+"/host-packages.json")
		return 0
	}
	if len(arguments) == 3 && arguments[0] == "verify-h3-backend" {
		if err := releaseartifacts.VerifyH3Backend(arguments[1], arguments[2]); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "verify H3 backend: %v\n", err)
			return 1
		}
		return 0
	}
	if len(arguments) == 5 && arguments[0] == "verify-h3-runtime-commands" {
		if err := releaseartifacts.VerifyH3RuntimeCommands(
			arguments[1], arguments[2], arguments[3], arguments[4],
		); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "verify H3 runtime commands: %v\n", err)
			return 1
		}
		return 0
	}
	if len(arguments) == 9 &&
		(arguments[0] == "print-vela-image-build" || arguments[0] == "build-vela-images") {
		request := velaImageBuildRequest(arguments)
		operation := releaseartifacts.BuildVelaImages
		if arguments[0] == "print-vela-image-build" {
			operation = releaseartifacts.PrintVelaImageBuild
		}
		if err := operation(context.Background(), request); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", arguments[0], err)
			return 1
		}
		return 0
	}
	if len(arguments) == 10 &&
		(arguments[0] == "build-vela-image-artifacts" || arguments[0] == "publish-vela-images") {
		request := releaseartifacts.VelaImageArtifactBuildRequest{
			VelaImageBuildRequest: velaImageBuildRequest(arguments),
			OutputDirectory:       arguments[9],
		}
		operation := releaseartifacts.BuildVelaImageArtifacts
		output := arguments[9] + "/vela-images.json"
		if arguments[0] == "publish-vela-images" {
			operation = releaseartifacts.PublishVelaImageArtifacts
			output = arguments[9] + "/vela-registry-publication.json"
		}
		if err := operation(context.Background(), request); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", arguments[0], err)
			return 1
		}
		_, _ = fmt.Fprintln(os.Stdout, output)
		return 0
	}
	_, _ = fmt.Fprintln(os.Stderr, "usage: vela-release-artifacts <build-h3-mock-backend|build-h3-stage-mock-runtime|build-host-packages|verify-h3-backend|verify-h3-runtime-commands|print-vela-image-build|build-vela-images|build-vela-image-artifacts|publish-vela-images> ...")
	return 2
}

func velaImageBuildRequest(arguments []string) releaseartifacts.VelaImageBuildRequest {
	return releaseartifacts.VelaImageBuildRequest{
		SourceRoot:  arguments[1],
		Revision:    arguments[2],
		ImagePrefix: arguments[3],
		H3Runtime: releaseartifacts.H3RuntimeComposition{
			BaseImage:        arguments[4],
			CommandContext:   arguments[5],
			EncoderSHA256:    arguments[6],
			DiTSHA256:        arguments[7],
			VAEDecoderSHA256: arguments[8],
		},
	}
}
