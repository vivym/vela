package releaseartifacts

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRenameNoReplacePreservesExistingDestination(t *testing.T) {
	temporary := t.TempDir()
	source := filepath.Join(temporary, "source")
	destination := filepath.Join(temporary, "destination")
	if err := os.Mkdir(source, 0o700); err != nil {
		t.Fatalf("create source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "source-file"), []byte("source\n"), 0o600); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	if err := os.Mkdir(destination, 0o700); err != nil {
		t.Fatalf("create destination: %v", err)
	}
	before, err := os.Stat(destination)
	if err != nil {
		t.Fatalf("stat destination: %v", err)
	}

	if err := renameNoReplace(source, destination); err == nil {
		t.Fatal("rename unexpectedly replaced an existing destination")
	}
	after, err := os.Stat(destination)
	if err != nil {
		t.Fatalf("stat destination after rename: %v", err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("rename changed the existing destination identity")
	}
	if _, err := os.Stat(filepath.Join(source, "source-file")); err != nil {
		t.Fatalf("source changed after rejected rename: %v", err)
	}
}
