package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

	"github.com/vivym/vela/internal/releasebundle"
)

var (
	buildBundle = releasebundle.Build
	loadBundle  = releasebundle.Load
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(arguments []string, stdout, stderr io.Writer) int {
	if len(arguments) == 2 && arguments[0] == "verify" {
		bundle, err := loadBundle(arguments[1])
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
		bundle, encoded, err := buildBundle(arguments[1])
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "build release bundle: %v\n", err)
			return 1
		}
		if err := rejectProtectedOutput(arguments[1], arguments[2], bundle); err != nil {
			_, _ = fmt.Fprintf(stderr, "build release bundle: %v\n", err)
			return 1
		}
		if err := writeVerifiedAtomic(arguments[2], encoded, bundle); err != nil {
			_, _ = fmt.Fprintf(stderr, "build release bundle: write verified output: %v\n", err)
			return 1
		}
		_, _ = fmt.Fprintf(stdout, "PASS release=%s configuration=%s output=%s\n", bundle.ReleaseDigest, bundle.ConfigurationRevision, arguments[2])
		return 0
	}
	_, _ = fmt.Fprintln(stderr, "usage: vela-release-bundle build <bundle-plan.json> <release-bundle.json>")
	_, _ = fmt.Fprintln(stderr, "       vela-release-bundle verify <release-bundle.json>")
	return 2
}

func rejectProtectedOutput(planPath, outputPath string, bundle releasebundle.Bundle) error {
	root := filepath.Dir(planPath)
	protected := []string{planPath}
	for _, reference := range bundleArtifactReferences(bundle) {
		protected = append(protected, filepath.Join(root, filepath.FromSlash(reference)))
	}
	outputCanonical, err := canonicalPath(outputPath)
	if err != nil {
		return fmt.Errorf("resolve output path: %w", err)
	}
	outputInformation, outputStatErr := os.Stat(outputPath)
	for _, path := range protected {
		canonical, resolveErr := canonicalPath(path)
		if resolveErr != nil {
			return fmt.Errorf("resolve protected path %q: %w", path, resolveErr)
		}
		if canonical == outputCanonical {
			return fmt.Errorf("output must not overwrite build plan or referenced artifact %q", path)
		}
		if outputStatErr == nil {
			information, statErr := os.Stat(path)
			if statErr != nil {
				return fmt.Errorf("stat protected path %q: %w", path, statErr)
			}
			if os.SameFile(outputInformation, information) {
				return fmt.Errorf("output must not alias build plan or referenced artifact %q", path)
			}
		}
	}
	return nil
}

func canonicalPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if resolved, resolveErr := filepath.EvalSymlinks(absolute); resolveErr == nil {
		return resolved, nil
	} else if !os.IsNotExist(resolveErr) {
		return "", resolveErr
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(absolute)), nil
}

func bundleArtifactReferences(bundle releasebundle.Bundle) []string {
	var references []string
	for _, render := range bundle.ConfigurationManifest.FinalRenders {
		references = append(references, render.Artifact.Ref)
	}
	references = append(references, bundle.ConfigurationManifest.NodeAgentUnit.Artifact.Ref)
	for _, item := range bundle.ConfigurationManifest.Packages {
		references = append(references, item.Contract.Ref, item.Artifact.Ref)
	}
	for _, item := range bundle.ConfigurationManifest.WorkerMaterializations {
		references = append(references, item.WorkerRuntime.Ref, item.RunnerProfiles.Ref, item.RunnerGPURoles.Ref)
	}
	for _, image := range bundle.OCIImages {
		references = append(references, image.Descriptor.Ref)
	}
	return references
}

func sameDerivedIdentity(left, right releasebundle.Bundle) bool {
	return left.ReleaseDigest == right.ReleaseDigest &&
		left.ConfigurationRevision == right.ConfigurationRevision &&
		reflect.DeepEqual(left.ReleaseDescriptor, right.ReleaseDescriptor) &&
		reflect.DeepEqual(left.ConfigurationManifest, right.ConfigurationManifest)
}

func writeVerifiedAtomic(path string, content []byte, expected releasebundle.Bundle) error {
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
	verified, err := loadBundle(temporaryPath)
	if err != nil {
		return fmt.Errorf("verify candidate: %w", err)
	}
	if !sameDerivedIdentity(verified, expected) {
		return fmt.Errorf("verify candidate: derived identity mismatch")
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open output directory for sync: %w", err)
	}
	defer func() { _ = directoryHandle.Close() }()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("sync output directory: %w", err)
	}
	return nil
}
