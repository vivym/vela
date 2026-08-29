package main

import (
	"fmt"
	"io"
	"os"

	"github.com/vivym/vela/internal/productiongates"
	"github.com/vivym/vela/internal/releasebundle"
	"github.com/vivym/vela/internal/supplychain"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(arguments []string, stdout, stderr io.Writer) int {
	if len(arguments) != 4 {
		_, _ = fmt.Fprintln(stderr, "usage: vela-verify-launch <release-bundle.json> <supply-chain.json> <supply-chain-policy.json> <launch-receipts.json>")
		return 2
	}
	bundle, err := releasebundle.Load(arguments[0])
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "verify release bundle: %v\n", err)
		return 1
	}
	evidence, err := supplychain.Load(arguments[1], arguments[2], bundle)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "verify release supply chain: %v\n", err)
		return 1
	}
	manifest, err := productiongates.LoadManifest(arguments[3])
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "verify launch receipts: %v\n", err)
		return 1
	}
	if err := validateBindings(bundle, evidence, manifest); err != nil {
		_, _ = fmt.Fprintf(stderr, "verify launch bindings: %v\n", err)
		return 1
	}
	_, _ = fmt.Fprintf(
		stdout,
		"PASS %d/%d release=%s configuration=%s supply_chain=%s policy=%s manifest=%s\n",
		manifest.Evaluation.Pass,
		len(productiongates.AllGates()),
		manifest.ReleaseDigest,
		manifest.ConfigurationRevision,
		evidence.ManifestDigest,
		evidence.PolicyDigest,
		manifest.Digest,
	)
	return 0
}

func validateBindings(
	bundle releasebundle.Bundle,
	evidence supplychain.Evidence,
	manifest productiongates.Manifest,
) error {
	if evidence.ReleaseDigest != bundle.ReleaseDigest ||
		evidence.ConfigurationRevision != bundle.ConfigurationRevision {
		return fmt.Errorf(
			"supply-chain evidence binds release=%s configuration=%s, want release=%s configuration=%s",
			evidence.ReleaseDigest,
			evidence.ConfigurationRevision,
			bundle.ReleaseDigest,
			bundle.ConfigurationRevision,
		)
	}
	return manifest.ValidateBinding(bundle.ReleaseDigest, bundle.ConfigurationRevision)
}
