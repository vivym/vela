package releaseartifacts

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestRuntimeStartupPackageCandidateRejectsPublicationMutations(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err = filepath.Abs(filepath.Join(root, "../.."))
	if err != nil {
		t.Fatal(err)
	}
	revision := "validation-package-test"
	tempRoot, err := filepath.EvalSymlinks(os.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	testRoot, err := os.MkdirTemp(tempRoot, "vela-runtime-package-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(testRoot) })
	built := filepath.Join(testRoot, "runtime-startup-packages")
	if err := BuildRuntimeStartupPackages(context.Background(), root, revision, built); err != nil {
		t.Fatalf("build runtime startup package fixture: %v", err)
	}
	if err := VerifyRuntimeStartupPackages(built, revision); err != nil {
		t.Fatalf("published runtime startup package failed exported verification: %v", err)
	}
	if err := VerifyRuntimeStartupPackages(built, "other-revision"); err == nil {
		t.Fatal("package verifier accepted a different release revision")
	}

	mutate := func(t *testing.T, name string, change func(string) error) {
		t.Helper()
		candidate := filepath.Join(testRoot, name)
		if err := copyRuntimeStartupPackageDirectory(built, candidate); err != nil {
			t.Fatal(err)
		}
		if err := change(candidate); err != nil {
			t.Fatal(err)
		}
		if err := verifyRuntimeStartupPackageCandidate(candidate, revision); err == nil {
			t.Fatalf("mutated candidate %q was accepted", name)
		}
	}

	mutate(t, "unexpected-file", func(candidate string) error {
		return os.WriteFile(filepath.Join(candidate, "unexpected"), []byte("x"), 0o600)
	})
	mutate(t, "manifest-revision", func(candidate string) error {
		return os.WriteFile(filepath.Join(candidate, "runtime-startup-packages.json"), []byte(`{"schema_version":1,"revision":"other","packages":[]}`), 0o600)
	})
	mutate(t, "artifact-mode", func(candidate string) error {
		return os.Chmod(filepath.Join(candidate, "vela-runtime-launcher"), 0o644)
	})
	mutate(t, "artifact-symlink", func(candidate string) error {
		path := filepath.Join(candidate, "vela-runtime-launcher")
		if err := os.Remove(path); err != nil {
			return err
		}
		return os.Symlink(filepath.Join(candidate, "vela-pidfd-broker"), path)
	})
	mutate(t, "unit-content", func(candidate string) error {
		return os.WriteFile(filepath.Join(candidate, "pidfd-broker.service"), []byte("[Service]\nExecStart=/tmp/invalid\n"), 0o600)
	})
}

func copyRuntimeStartupPackageDirectory(source, destination string) error {
	if err := os.Mkdir(destination, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			return os.ErrInvalid
		}
		content, err := os.ReadFile(filepath.Join(source, entry.Name()))
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(destination, entry.Name()), content, info.Mode().Perm()); err != nil {
			return err
		}
	}
	return nil
}
