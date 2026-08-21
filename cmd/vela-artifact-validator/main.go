package main

import (
	"fmt"
	"os"

	"github.com/vivym/vela/internal/artifactvalidator"
)

var runArtifactSandboxHelper = artifactvalidator.RunSandboxHelper

func main() {
	if err := runArtifactSandboxHelper(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
