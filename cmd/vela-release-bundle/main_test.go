package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/releasebundle"
)

func TestRunFailsClosedForInvalidInvocation(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(nil, &stdout, &stderr); code != 2 || stdout.Len() != 0 ||
		!strings.Contains(stderr.String(), "usage:") {
		t.Fatalf("run = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestRunBuildRejectsProtectedOutput(t *testing.T) {
	directory := t.TempDir()
	plan := filepath.Join(directory, "plan.json")
	render := filepath.Join(directory, "render.yaml")
	packageArtifact := filepath.Join(directory, "node-agent.tar")
	if err := os.WriteFile(plan, []byte("plan"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(render, []byte("render"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(packageArtifact, []byte("package"), 0o600); err != nil {
		t.Fatal(err)
	}
	bundle := releasebundle.Bundle{ConfigurationManifest: releasebundle.ConfigurationManifest{
		FinalRenders: []releasebundle.NamedArtifact{{
			Name: "control-storage", Artifact: releasebundle.Artifact{Ref: "render.yaml"},
		}},
		Packages: []releasebundle.Package{{Name: "node-agent", Artifact: releasebundle.Artifact{Ref: "node-agent.tar"}}},
	}}
	stubReleaseBundle(t, bundle, []byte("bundle"), nil)
	for _, output := range []string{plan, render, packageArtifact} {
		var stdout, stderr bytes.Buffer
		if code := run([]string{"build", plan, output}, &stdout, &stderr); code != 1 ||
			stdout.Len() != 0 || !strings.Contains(stderr.String(), "must not overwrite") {
			t.Fatalf("run(%q) = code %d stdout %q stderr %q", output, code, stdout.String(), stderr.String())
		}
	}
	if content, err := os.ReadFile(plan); err != nil || string(content) != "plan" {
		t.Fatalf("plan was overwritten: %q error=%v", content, err)
	}
	if content, err := os.ReadFile(render); err != nil || string(content) != "render" {
		t.Fatalf("render was overwritten: %q error=%v", content, err)
	}
	if content, err := os.ReadFile(packageArtifact); err != nil || string(content) != "package" {
		t.Fatalf("package was overwritten: %q error=%v", content, err)
	}
}

func TestRunBuildLoadsWrittenBundleBeforePass(t *testing.T) {
	directory := t.TempDir()
	plan := filepath.Join(directory, "plan.json")
	output := filepath.Join(directory, "bundle.json")
	if err := os.WriteFile(plan, []byte("plan"), 0o600); err != nil {
		t.Fatal(err)
	}
	bundle := releasebundle.Bundle{
		ReleaseDigest:         "sha256:" + strings.Repeat("a", 64),
		ConfigurationRevision: "sha256:" + strings.Repeat("b", 64),
	}
	loaded := false
	priorBuild, priorLoad := buildBundle, loadBundle
	buildBundle = func(string) (releasebundle.Bundle, []byte, error) {
		return bundle, []byte("canonical bundle"), nil
	}
	loadBundle = func(path string) (releasebundle.Bundle, error) {
		loaded = true
		content, err := os.ReadFile(path)
		if err != nil || string(content) != "canonical bundle" {
			t.Fatalf("post-write bytes = %q error=%v", content, err)
		}
		return bundle, nil
	}
	t.Cleanup(func() { buildBundle, loadBundle = priorBuild, priorLoad })
	var stdout, stderr bytes.Buffer
	if code := run([]string{"build", plan, output}, &stdout, &stderr); code != 0 ||
		!loaded || !strings.Contains(stdout.String(), "PASS release=") || stderr.Len() != 0 {
		t.Fatalf("run = code %d loaded=%t stdout %q stderr %q", code, loaded, stdout.String(), stderr.String())
	}
}

func TestRunBuildRejectsPostWriteIdentityMismatch(t *testing.T) {
	directory := t.TempDir()
	plan := filepath.Join(directory, "plan.json")
	output := filepath.Join(directory, "bundle.json")
	if err := os.WriteFile(plan, []byte("plan"), 0o600); err != nil {
		t.Fatal(err)
	}
	built := releasebundle.Bundle{
		ReleaseDigest:         "sha256:" + strings.Repeat("a", 64),
		ConfigurationRevision: "sha256:" + strings.Repeat("b", 64),
	}
	verified := built
	verified.ReleaseDigest = "sha256:" + strings.Repeat("c", 64)
	priorBuild, priorLoad := buildBundle, loadBundle
	buildBundle = func(string) (releasebundle.Bundle, []byte, error) { return built, []byte("bundle"), nil }
	loadBundle = func(string) (releasebundle.Bundle, error) { return verified, nil }
	t.Cleanup(func() { buildBundle, loadBundle = priorBuild, priorLoad })
	var stdout, stderr bytes.Buffer
	if code := run([]string{"build", plan, output}, &stdout, &stderr); code != 1 ||
		stdout.Len() != 0 || !strings.Contains(stderr.String(), "post-write verification failed") {
		t.Fatalf("run = code %d stdout %q stderr %q", code, stdout.String(), stderr.String())
	}
}

func stubReleaseBundle(t *testing.T, bundle releasebundle.Bundle, encoded []byte, loadError error) {
	t.Helper()
	priorBuild, priorLoad := buildBundle, loadBundle
	buildBundle = func(string) (releasebundle.Bundle, []byte, error) { return bundle, encoded, nil }
	loadBundle = func(string) (releasebundle.Bundle, error) { return bundle, loadError }
	t.Cleanup(func() { buildBundle, loadBundle = priorBuild, priorLoad })
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
