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
	if len(arguments) != 4 || arguments[0] != "build-host-packages" {
		_, _ = fmt.Fprintln(os.Stderr, "usage: vela-release-artifacts build-host-packages <source-root> <revision> <output-directory>")
		return 2
	}
	if err := releaseartifacts.BuildHostPackages(
		context.Background(), arguments[1], arguments[2], arguments[3],
	); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "build host packages: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintln(os.Stdout, arguments[3]+"/host-packages.json")
	return 0
}
