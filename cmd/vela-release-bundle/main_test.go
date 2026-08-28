package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunFailsClosedForInvalidInvocation(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(nil, &stdout, &stderr); code != 2 || stdout.Len() != 0 ||
		!strings.Contains(stderr.String(), "usage:") {
		t.Fatalf("run = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestRunRejectsMissingBundle(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"verify", "/does/not/exist.json"}, &stdout, &stderr); code != 1 ||
		stdout.Len() != 0 || !strings.Contains(stderr.String(), "verify release bundle:") {
		t.Fatalf("run = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestRunRejectsOutputOutsidePlanRoot(t *testing.T) {
	planDirectory := t.TempDir()
	outputDirectory := t.TempDir()
	plan := filepath.Join(planDirectory, "plan.json")
	if err := os.WriteFile(plan, []byte(`{"schema_version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"build", plan, filepath.Join(outputDirectory, "bundle.json")}, &stdout, &stderr); code != 1 ||
		stdout.Len() != 0 || !strings.Contains(stderr.String(), "artifact references remain rooted") {
		t.Fatalf("run = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestWriteAtomicReplacesCompleteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bundle.json")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeAtomic(path, []byte("new bundle")); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(path)
	if err != nil || string(content) != "new bundle" {
		t.Fatalf("atomic output = %q error=%v", content, err)
	}
}
