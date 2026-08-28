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
	if len(arguments) == 6 &&
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
	if len(arguments) == 7 && arguments[0] == "build-vela-image-artifacts" {
		request := releaseartifacts.VelaImageArtifactBuildRequest{
			VelaImageBuildRequest: velaImageBuildRequest(arguments),
			OutputDirectory:       arguments[6],
		}
		if err := releaseartifacts.BuildVelaImageArtifacts(context.Background(), request); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "build Vela image artifacts: %v\n", err)
			return 1
		}
		_, _ = fmt.Fprintln(os.Stdout, arguments[6]+"/vela-images.json")
		return 0
	}
	_, _ = fmt.Fprintln(os.Stderr, "usage: vela-release-artifacts <build-host-packages|verify-h3-backend|print-vela-image-build|build-vela-images|build-vela-image-artifacts> ...")
	return 2
}

func velaImageBuildRequest(arguments []string) releaseartifacts.VelaImageBuildRequest {
	return releaseartifacts.VelaImageBuildRequest{
		SourceRoot:     arguments[1],
		Revision:       arguments[2],
		ImagePrefix:    arguments[3],
		BackendContext: arguments[4],
		BackendSHA256:  arguments[5],
	}
}
