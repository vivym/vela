package securefile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadRejectsSymlinkAndWritableFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "config.json")
	if err := os.WriteFile(path, []byte("{}"), 0o600); err != nil {
		t.Fatalf("write secure file: %v", err)
	}
	if content, err := Read(path, 1024, true); err != nil || string(content) != "{}" {
		t.Fatalf("Read secure file = %q error=%v", content, err)
	}
	link := filepath.Join(directory, "config-link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	if _, err := Read(link, 1024, true); err == nil {
		t.Fatal("symlink was accepted as a secure file")
	}
	if err := os.Chmod(path, 0o666); err != nil {
		t.Fatalf("relax secure file permissions: %v", err)
	}
	if _, err := Read(path, 1024, false); err == nil {
		t.Fatal("group/world-writable file was accepted")
	}
}

func TestValidateDirectoryRejectsSymlink(t *testing.T) {
	directory := t.TempDir()
	if err := ValidateDirectory(directory); err != nil {
		t.Fatalf("ValidateDirectory: %v", err)
	}
	link := filepath.Join(t.TempDir(), "state")
	if err := os.Symlink(directory, link); err != nil {
		t.Fatalf("create directory symlink: %v", err)
	}
	if err := ValidateDirectory(link); err == nil {
		t.Fatal("directory symlink was accepted")
	}
}

func TestOpenPrivateStateRejectsSymlink(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatalf("write private state target: %v", err)
	}
	link := filepath.Join(directory, "state.lock")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("create private state symlink: %v", err)
	}
	if file, err := OpenPrivateState(link); err == nil {
		_ = file.Close()
		t.Fatal("private state symlink was accepted")
	}
}

func TestValidateExecutableRejectsWritableOrLinkedBinary(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "helper")
	if err := os.WriteFile(path, []byte("helper"), 0o700); err != nil {
		t.Fatalf("write helper: %v", err)
	}
	if err := ValidateExecutable(path); err != nil {
		t.Fatalf("ValidateExecutable: %v", err)
	}
	if err := os.Chmod(path, 0o722); err != nil {
		t.Fatalf("relax helper permissions: %v", err)
	}
	if err := ValidateExecutable(path); err == nil {
		t.Fatal("group/world-writable helper was accepted")
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatalf("restore helper permissions: %v", err)
	}
	link := filepath.Join(directory, "helper-link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatalf("create helper symlink: %v", err)
	}
	if err := ValidateExecutable(link); err == nil {
		t.Fatal("helper symlink was accepted")
	}
}
