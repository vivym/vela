package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/productiongates"
	"github.com/vivym/vela/internal/releasebundle"
)

func TestRunFailsClosedWithoutManifest(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(nil, &stdout, &stderr); code != 2 {
		t.Fatalf("run exit code = %d, want 2; stderr=%s", code, stderr.String())
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "usage:") {
		t.Fatalf("run output = stdout %q stderr %q", stdout.String(), stderr.String())
	}
}

func TestRunRejectsMissingManifest(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"/does/not/exist-bundle.json", "/does/not/exist-receipts.json"}, &stdout, &stderr); code != 1 {
		t.Fatalf("run exit code = %d, want 1; stderr=%s", code, stderr.String())
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "verify release bundle:") {
		t.Fatalf("run output = stdout %q stderr %q", stdout.String(), stderr.String())
	}
}

func TestValidateBindingsRequiresExactReleaseAndConfiguration(t *testing.T) {
	bundle := releasebundle.Bundle{ReleaseDigest: "sha256:" + strings.Repeat("a", 64), ConfigurationRevision: "sha256:" + strings.Repeat("b", 64)}
	manifest := productiongates.Manifest{ReleaseDigest: bundle.ReleaseDigest, ConfigurationRevision: bundle.ConfigurationRevision}
	if err := validateBindings(bundle, manifest); err != nil {
		t.Fatalf("exact bindings rejected: %v", err)
	}
	manifest.ReleaseDigest = "sha256:" + strings.Repeat("c", 64)
	if err := validateBindings(bundle, manifest); err == nil {
		t.Fatal("release mismatch was accepted")
	}
	manifest.ReleaseDigest = bundle.ReleaseDigest
	manifest.ConfigurationRevision = "sha256:" + strings.Repeat("d", 64)
	if err := validateBindings(bundle, manifest); err == nil {
		t.Fatal("configuration mismatch was accepted")
	}
}
