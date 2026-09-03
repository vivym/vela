//go:build linux

package artifactvalidator

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stagefinalization"
	"golang.org/x/sys/unix"
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

func TestPrepareSandboxRootRemovesHelperAndLocksPinnedFFprobe(t *testing.T) {
	root := filepath.Join(t.TempDir(), "sandbox")
	t.Cleanup(func() {
		if err := os.Chmod(root, 0o700); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Errorf("unlock sandbox root for TempDir cleanup: %v", err)
		}
	})
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "artifact-validator-helper"), []byte("helper"), 0o500); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "artifact-ffprobe"), []byte("ffprobe"), 0o500); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "artifact-input"), []byte("input"), 0o400); err != nil {
		t.Fatal(err)
	}
	if err := prepareSandboxRoot(root); err != nil {
		t.Fatalf("prepare sandbox root: %v", err)
	}
	entries, err := os.ReadDir(root)
	info, statErr := os.Stat(root)
	if err != nil || statErr != nil || len(entries) != 2 || info.Mode().Perm() != 0o500 {
		t.Fatalf("prepared sandbox root = entries %v info %v errors %v/%v", entries, info, err, statErr)
	}
}

func TestPrepareSandboxRootRejectsUnexpectedEntry(t *testing.T) {
	root := filepath.Join(t.TempDir(), "sandbox")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, file := range map[string]struct {
		content string
		mode    os.FileMode
	}{
		"artifact-validator-helper": {content: "helper", mode: 0o500},
		"artifact-ffprobe":          {content: "ffprobe", mode: 0o500},
		"artifact-input":            {content: "input", mode: 0o400},
		"unexpected":                {content: "must fail closed", mode: 0o500},
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(file.content), file.mode); err != nil {
			t.Fatal(err)
		}
	}
	if err := prepareSandboxRoot(root); err == nil {
		t.Fatal("prepareSandboxRoot accepted an unexpected directory entry")
	}
}

func TestLandlockFilesystemAccessMaskTracksKernelABI(t *testing.T) {
	base := landlockFilesystemAccessMask(1)
	if base&unix.LANDLOCK_ACCESS_FS_EXECUTE == 0 ||
		base&unix.LANDLOCK_ACCESS_FS_READ_FILE == 0 ||
		base&unix.LANDLOCK_ACCESS_FS_REFER != 0 ||
		base&unix.LANDLOCK_ACCESS_FS_TRUNCATE != 0 ||
		base&unix.LANDLOCK_ACCESS_FS_IOCTL_DEV != 0 {
		t.Fatalf("Landlock ABI 1 mask = %#x", base)
	}
	if mask := landlockFilesystemAccessMask(2); mask != base|unix.LANDLOCK_ACCESS_FS_REFER {
		t.Fatalf("Landlock ABI 2 mask = %#x", mask)
	}
	if mask := landlockFilesystemAccessMask(3); mask != base|unix.LANDLOCK_ACCESS_FS_REFER|unix.LANDLOCK_ACCESS_FS_TRUNCATE {
		t.Fatalf("Landlock ABI 3 mask = %#x", mask)
	}
	if mask := landlockFilesystemAccessMask(5); mask != base|unix.LANDLOCK_ACCESS_FS_REFER|
		unix.LANDLOCK_ACCESS_FS_TRUNCATE|unix.LANDLOCK_ACCESS_FS_IOCTL_DEV {
		t.Fatalf("Landlock ABI 5 mask = %#x", mask)
	}
}

func TestSandboxIDMapMustBeNarrowAndNonTranslating(t *testing.T) {
	if !sandboxIDMapMatches([]byte("10001 10001 1\n"), 10001) {
		t.Fatal("narrow non-translating identity map was rejected")
	}
	for _, mapping := range []string{
		"0 10001 1\n",
		"10001 10001 2\n",
		"10001 10002 1\n",
		"10001 10001 1\n20000 20000 1\n",
		"not-an-id 10001 1\n",
	} {
		if sandboxIDMapMatches([]byte(mapping), 10001) {
			t.Fatalf("unsafe identity map %q was accepted", mapping)
		}
	}
}

func TestLandlockRestrictsFilesystemToPinnedExecutable(t *testing.T) {
	const (
		childEnvironment  = "VELA_LANDLOCK_TEST_CHILD"
		deniedEnvironment = "VELA_LANDLOCK_TEST_DENIED_PATH"
	)
	if os.Getenv(childEnvironment) == "1" {
		runtime.LockOSThread()

		executable, err := os.Open(os.Args[0])
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = executable.Close() }()
		if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
			t.Fatalf("set no_new_privs: %v", err)
		}
		if err := restrictSandboxFilesystem(executable); err != nil {
			t.Fatalf("restrict filesystem: %v", err)
		}
		if _, err := os.ReadFile(os.Args[0]); err != nil {
			t.Fatalf("read allowed executable: %v", err)
		}
		if _, err := os.ReadFile(os.Getenv(deniedEnvironment)); !errors.Is(err, unix.EACCES) {
			t.Fatalf("read denied file error = %v", err)
		}
		return
	}
	if _, err := landlockCreateRuleset(nil, 0, unix.LANDLOCK_CREATE_RULESET_VERSION); err != nil {
		if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EOPNOTSUPP) {
			t.Skipf("Landlock is not supported by this Linux kernel: %v", err)
		}
		t.Fatalf("query Landlock ABI: %v", err)
	}
	deniedPath := filepath.Join(t.TempDir(), "denied")
	if err := os.WriteFile(deniedPath, []byte("must remain inaccessible"), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestLandlockRestrictsFilesystemToPinnedExecutable$")
	command.Env = append(os.Environ(), childEnvironment+"=1", deniedEnvironment+"="+deniedPath)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Landlock child failed: %v\n%s", err, output)
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
		kind          stagefinalization.ArtifactKind
		wantCodec     string
		wantContainer string
	}{
		{
			name:          "video.mp4",
			fixture:       strings.TrimSpace(h264MP4FixtureBase64),
			kind:          stagefinalization.ArtifactKindVideo,
			wantCodec:     "h264",
			wantContainer: "mp4",
		},
		{
			name:          "thumbnail.webp",
			fixture:       webPFixtureBase64,
			kind:          stagefinalization.ArtifactKindThumbnail,
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
