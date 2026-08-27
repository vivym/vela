package main

import (
	"bytes"
	"strings"
	"testing"
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
	if code := run([]string{"/does/not/exist.json"}, &stdout, &stderr); code != 1 {
		t.Fatalf("run exit code = %d, want 1; stderr=%s", code, stderr.String())
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "verify launch receipts:") {
		t.Fatalf("run output = stdout %q stderr %q", stdout.String(), stderr.String())
	}
}
