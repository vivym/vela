//go:build linux

package artifactvalidator

import (
	"context"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/workercontrol"
)

func TestOpenSandboxExecutablePinsInodeAndRejectsSymlink(t *testing.T) {
	sourcePath, err := os.Executable()
	if err != nil {
		t.Fatalf("resolve test executable: %v", err)
	}
	directory := t.TempDir()
	executablePath := filepath.Join(directory, "validator-helper")
	copyExecutable(t, sourcePath, executablePath)

	pinned, err := openSandboxExecutable(executablePath, false)
	if err != nil {
		t.Fatalf("openSandboxExecutable: %v", err)
	}
	t.Cleanup(func() { _ = pinned.Close() })
	originalPath := executablePath + ".original"
	if err := os.Rename(executablePath, originalPath); err != nil {
		t.Fatalf("rename pinned executable: %v", err)
	}
	copyExecutable(t, sourcePath, executablePath)
	pinnedInfo, err := pinned.Stat()
	if err != nil {
		t.Fatalf("stat pinned executable: %v", err)
	}
	originalInfo, err := os.Stat(originalPath)
	if err != nil {
		t.Fatalf("stat original executable: %v", err)
	}
	replacementInfo, err := os.Stat(executablePath)
	if err != nil {
		t.Fatalf("stat replacement executable: %v", err)
	}
	if !os.SameFile(pinnedInfo, originalInfo) || os.SameFile(pinnedInfo, replacementInfo) {
		t.Fatal("sandbox executable descriptor did not remain pinned to the validated inode")
	}

	symlinkPath := filepath.Join(directory, "validator-symlink")
	if err := os.Symlink(executablePath, symlinkPath); err != nil {
		t.Fatalf("create executable symlink: %v", err)
	}
	if file, err := openSandboxExecutable(symlinkPath, false); err == nil {
		_ = file.Close()
		t.Fatal("openSandboxExecutable accepted a symlink")
	}
}

func TestSandboxHelperArgumentsDoNotAcceptFFprobePath(t *testing.T) {
	root, err := parseSandboxHelperArguments([]string{"--root", "/tmp/vela-sandbox"})
	if err != nil || root != "/tmp/vela-sandbox" {
		t.Fatalf("parseSandboxHelperArguments = %q error=%v", root, err)
	}
	if _, err := parseSandboxHelperArguments([]string{
		"--ffprobe",
		"/replaceable/path/ffprobe",
		"--root",
		"/tmp/vela-sandbox",
	}); err == nil {
		t.Fatal("sandbox helper accepted a path-selected ffprobe")
	}
}

func TestProductionSandboxProbesPinnedVideoAndThumbnail(t *testing.T) {
	if os.Getenv("VELA_REQUIRE_PINNED_FFPROBE") != "1" {
		t.Skip("pinned production ffprobe is not required")
	}
	helperPath := os.Getenv("VELA_ARTIFACT_VALIDATOR_HELPER_PATH")
	if helperPath == "" {
		t.Fatal("VELA_ARTIFACT_VALIDATOR_HELPER_PATH is required")
	}
	ffprobePath, expectedVersion := pinnedFFprobe(t)
	sandbox, err := NewProductionSandbox(SandboxConfig{
		HelperPath:     helperPath,
		FFprobePath:    ffprobePath,
		RootDirectory:  t.TempDir(),
		MaxOutputBytes: 1024 * 1024,
		MaxStderrBytes: 64 * 1024,
	})
	if err != nil {
		t.Fatalf("NewProductionSandbox: %v", err)
	}
	tests := []struct {
		name          string
		fixture       string
		kind          workercontrol.ArtifactKind
		wantCodec     string
		wantContainer string
	}{
		{
			name:          "video.mp4",
			fixture:       strings.TrimSpace(h264MP4FixtureBase64),
			kind:          workercontrol.ArtifactKindVideo,
			wantCodec:     "h264",
			wantContainer: "mp4",
		},
		{
			name:          "thumbnail.webp",
			fixture:       webPFixtureBase64,
			kind:          workercontrol.ArtifactKindThumbnail,
			wantCodec:     "webp",
			wantContainer: "webp",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			media, err := base64.StdEncoding.DecodeString(test.fixture)
			if err != nil {
				t.Fatalf("decode fixed media fixture: %v", err)
			}
			path := filepath.Join(t.TempDir(), test.name)
			if err := os.WriteFile(path, media, 0o600); err != nil {
				t.Fatalf("write fixed media fixture: %v", err)
			}
			input, err := os.Open(path)
			if err != nil {
				t.Fatalf("open fixed media fixture: %v", err)
			}
			defer func() {
				if err := input.Close(); err != nil {
					t.Errorf("close fixed media fixture: %v", err)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			output, err := sandbox.Probe(ctx, input)
			if err != nil {
				t.Fatalf("probe through production sandbox: %v", err)
			}
			facts, err := parseFFprobeOutput(output, test.kind, expectedVersion)
			if err != nil {
				t.Fatalf("parse sandboxed ffprobe output: %v; output=%s", err, output)
			}
			if facts.Codec != test.wantCodec || facts.Container != test.wantContainer {
				t.Fatalf("sandboxed media facts = %#v", facts)
			}
		})
	}
}

func copyExecutable(t *testing.T, sourcePath string, destinationPath string) {
	t.Helper()
	source, err := os.Open(sourcePath)
	if err != nil {
		t.Fatalf("open source executable: %v", err)
	}
	defer func() {
		if err := source.Close(); err != nil {
			t.Errorf("close source executable: %v", err)
		}
	}()
	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o500)
	if err != nil {
		t.Fatalf("create copied executable: %v", err)
	}
	if _, err := io.Copy(destination, source); err != nil {
		_ = destination.Close()
		t.Fatalf("copy executable: %v", err)
	}
	if err := destination.Close(); err != nil {
		t.Fatalf("close copied executable: %v", err)
	}
}
