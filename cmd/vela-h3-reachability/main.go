package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/vivym/vela/internal/legacyh3reachability"
	"github.com/vivym/vela/internal/releasebundle"
)

type result struct {
	SchemaVersion         int    `json:"schema_version"`
	Result                string `json:"result"`
	ReleaseDigest         string `json:"release_digest"`
	ConfigurationRevision string `json:"configuration_revision"`
	EvidencePath          string `json:"evidence_path"`
	EvidenceDigest        string `json:"evidence_digest"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, time.Now))
}

func run(arguments []string, stdout, stderr io.Writer, now func() time.Time) int {
	if len(arguments) == 0 || stdout == nil || stderr == nil || now == nil {
		writeUsage(stderr)
		return 2
	}
	switch arguments[0] {
	case "scan":
		return runScan(arguments[1:], stdout, stderr, now)
	case "verify":
		return runVerify(arguments[1:], stdout, stderr)
	default:
		writeUsage(stderr)
		return 2
	}
}

func runScan(arguments []string, stdout, stderr io.Writer, now func() time.Time) int {
	flags := flag.NewFlagSet("scan", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	root := flags.String("root", "", "source repository root")
	bundlePath := flags.String("release-bundle", "", "verified release bundle")
	sourceRevision := flags.String("source-revision", "", "source revision")
	observedBy := flags.String("observed-by", "", "evidence collector identity")
	output := flags.String("output", "", "new evidence file")
	if flags.Parse(arguments) != nil || flags.NArg() != 0 ||
		*root == "" || *bundlePath == "" || *sourceRevision == "" ||
		*observedBy == "" || *output == "" {
		writeUsage(stderr)
		return 2
	}
	bundle, err := releasebundle.Load(*bundlePath)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "load release bundle: %v\n", err)
		return 1
	}
	evidence, encoded, digest, err := legacyh3reachability.Scan(
		*root,
		bundle,
		*sourceRevision,
		*observedBy,
		now().UTC(),
	)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "scan Legacy H3 reachability: %v\n", err)
		return 1
	}
	if err := legacyh3reachability.Write(*output, encoded); err != nil {
		_, _ = fmt.Fprintf(stderr, "publish Legacy H3 reachability evidence: %v\n", err)
		return 1
	}
	if err := writeResult(stdout, result{
		SchemaVersion: evidence.SchemaVersion, Result: evidence.Result,
		ReleaseDigest: evidence.ReleaseDigest, ConfigurationRevision: evidence.ConfigurationRevision,
		EvidencePath: *output, EvidenceDigest: digest,
	}); err != nil {
		_, _ = fmt.Fprintf(stderr, "encode Legacy H3 reachability result: %v\n", err)
		return 1
	}
	if evidence.Result != legacyh3reachability.ResultPass {
		_, _ = fmt.Fprintln(stderr, "Legacy H3 remains reachable; evidence result is FAIL")
		return 1
	}
	return 0
}

func runVerify(arguments []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	bundlePath := flags.String("release-bundle", "", "verified release bundle")
	evidencePath := flags.String("evidence", "", "reachability evidence")
	if flags.Parse(arguments) != nil || flags.NArg() != 0 ||
		*bundlePath == "" || *evidencePath == "" {
		writeUsage(stderr)
		return 2
	}
	bundle, err := releasebundle.Load(*bundlePath)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "load release bundle: %v\n", err)
		return 1
	}
	evidence, _, digest, err := legacyh3reachability.Load(*evidencePath, bundle)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "verify Legacy H3 reachability evidence: %v\n", err)
		return 1
	}
	if err := writeResult(stdout, result{
		SchemaVersion: evidence.SchemaVersion, Result: evidence.Result,
		ReleaseDigest: evidence.ReleaseDigest, ConfigurationRevision: evidence.ConfigurationRevision,
		EvidencePath: *evidencePath, EvidenceDigest: digest,
	}); err != nil {
		_, _ = fmt.Fprintf(stderr, "encode Legacy H3 reachability result: %v\n", err)
		return 1
	}
	return 0
}

func writeResult(writer io.Writer, value result) error {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func writeUsage(writer io.Writer) {
	if writer == nil {
		return
	}
	_, _ = fmt.Fprintln(
		writer,
		"usage: vela-h3-reachability scan --root <repo> --release-bundle <release-bundle.json> --source-revision <revision> --observed-by <identity> --output <new-evidence.json>",
	)
	_, _ = fmt.Fprintln(
		writer,
		"       vela-h3-reachability verify --release-bundle <release-bundle.json> --evidence <evidence.json>",
	)
}
