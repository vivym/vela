package main

import (
	"fmt"
	"io"
	"os"

	"github.com/vivym/vela/internal/productiongates"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(arguments []string, stdout, stderr io.Writer) int {
	if len(arguments) != 1 {
		_, _ = fmt.Fprintln(stderr, "usage: vela-verify-launch <launch-receipts.json>")
		return 2
	}
	manifest, err := productiongates.LoadManifest(arguments[0])
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "verify launch receipts: %v\n", err)
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
