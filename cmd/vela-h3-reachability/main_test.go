package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestRunRequiresExactCommandFlags(t *testing.T) {
	for _, arguments := range [][]string{
		nil,
		{"unknown"},
		{"scan", "--root", "."},
		{"verify", "--release-bundle", "release-bundle.json"},
	} {
		var stdout, stderr bytes.Buffer
		code := run(arguments, &stdout, &stderr, func() time.Time { return time.Time{} })
		if code != 2 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "usage:") {
			t.Fatalf(
				"run(%v) = code %d stdout %q stderr %q",
				arguments, code, stdout.String(), stderr.String(),
			)
		}
	}
}

func TestRunRejectsUnverifiedReleaseBundleBeforeWritingEvidence(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{
		"scan",
		"--root", ".",
		"--release-bundle", "missing.json",
		"--source-revision", "0123456789abcdef",
		"--observed-by", "test/reachability",
		"--output", "unused.json",
	}, &stdout, &stderr, time.Now)
	if code != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "load release bundle") {
		t.Fatalf("run = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
}
