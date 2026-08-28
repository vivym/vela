package main

import (
	"fmt"
	"io"
	"os"

	"github.com/vivym/vela/internal/productiongates"
	"github.com/vivym/vela/internal/releasebundle"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(arguments []string, stdout, stderr io.Writer) int {
	if len(arguments) != 2 {
		_, _ = fmt.Fprintln(stderr, "usage: vela-verify-launch <release-bundle.json> <launch-receipts.json>")
		return 2
	}
	bundle, err := releasebundle.Load(arguments[0])
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "verify release bundle: %v\n", err)
		return 1
	}
	manifest, err := productiongates.LoadManifest(arguments[1])
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "verify launch receipts: %v\n", err)
		return 1
	}
	if err := validateBindings(bundle, manifest); err != nil {
		_, _ = fmt.Fprintf(stderr, "verify launch bindings: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(
		stdout,
		"PASS %d/%d release=%s configuration=%s manifest=%s\n",
		manifest.Evaluation.Pass,
		len(productiongates.AllGates()),
		manifest.ReleaseDigest,
		manifest.ConfigurationRevision,
		manifest.Digest,
	)
	return 0
}

func validateBindings(bundle releasebundle.Bundle, manifest productiongates.Manifest) error {
	if manifest.ReleaseDigest != bundle.ReleaseDigest ||
		manifest.ConfigurationRevision != bundle.ConfigurationRevision {
		return fmt.Errorf(
			"Launch Receipts bind release=%s configuration=%s, want release=%s configuration=%s",
			manifest.ReleaseDigest,
			manifest.ConfigurationRevision,
			bundle.ReleaseDigest,
			bundle.ConfigurationRevision,
		)
	}
	return nil
}
