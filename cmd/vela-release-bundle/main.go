package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/vivym/vela/internal/releasebundle"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(arguments []string, stdout, stderr io.Writer) int {
	if len(arguments) == 2 && arguments[0] == "verify" {
		bundle, err := releasebundle.Load(arguments[1])
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "verify release bundle: %v\n", err)
			return 1
		}
		_, _ = fmt.Fprintf(stdout, "PASS release=%s configuration=%s\n", bundle.ReleaseDigest, bundle.ConfigurationRevision)
		return 0
	}
	if len(arguments) == 3 && arguments[0] == "build" {
		planDirectory, err := filepath.Abs(filepath.Dir(arguments[1]))
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "build release bundle: resolve plan directory: %v\n", err)
			return 1
		}
		outputDirectory, err := filepath.Abs(filepath.Dir(arguments[2]))
		if err != nil || outputDirectory != planDirectory {
			_, _ = fmt.Fprintln(stderr, "build release bundle: output must be in the build plan directory so artifact references remain rooted")
			return 1
		}
		bundle, encoded, err := releasebundle.Build(arguments[1])
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "build release bundle: %v\n", err)
			return 1
		}
		if err := writeAtomic(arguments[2], encoded); err != nil {
			_, _ = fmt.Fprintf(stderr, "build release bundle: write output: %v\n", err)
			return 1
		}
		_, _ = fmt.Fprintf(stdout, "PASS release=%s configuration=%s output=%s\n", bundle.ReleaseDigest, bundle.ConfigurationRevision, arguments[2])
		return 0
	}
	_, _ = fmt.Fprintln(stderr, "usage: vela-release-bundle build <bundle-plan.json> <release-bundle.json>")
	_, _ = fmt.Fprintln(stderr, "       vela-release-bundle verify <release-bundle.json>")
	return 2
}

func writeAtomic(path string, content []byte) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".vela-release-bundle-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(content); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}
