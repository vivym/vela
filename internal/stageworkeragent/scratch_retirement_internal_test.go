package stageworkeragent

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageartifact"
)

func TestBoundScratchRetirementCannotFollowReplacedStageDirectories(t *testing.T) {
	for _, replaced := range []string{"stage-runs parent", "stage run", "stage attempt", "output parent"} {
		t.Run(replaced, func(t *testing.T) {
			inputRoot, outputRoot := t.TempDir(), t.TempDir()
			payload := []byte("matching committed and protected bytes")
			runID, attemptID := uuid.NewString(), uuid.NewString()
			inputRelative := path.Join("stage-runs", runID)
			outputRelative := path.Join(attemptID, "nested")
			inputPath := filepath.Join(inputRoot, filepath.FromSlash(inputRelative), "input.bin")
			outputPath := filepath.Join(outputRoot, filepath.FromSlash(outputRelative), "output.bin")
			for _, file := range []string{inputPath, outputPath} {
				writeBoundScratchFixture(t, file, payload)
			}
			retirer, err := NewFilesystemScratchRetirer(inputRoot, outputRoot)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = retirer.Close() }()
			inputs, err := bindScratchDirectory(retirer.inputs, inputRelative)
			if err != nil {
				t.Fatal(err)
			}
			defer inputs.close()
			outputs, err := bindScratchDirectory(retirer.outputs, outputRelative)
			if err != nil {
				t.Fatal(err)
			}
			defer outputs.close()

			var replacedPath, protectedDirectory, payloadSuffix string
			switch replaced {
			case "stage-runs parent":
				replacedPath = filepath.Join(inputRoot, "stage-runs")
				protectedDirectory = filepath.Join(inputRoot, "protected-runs")
				payloadSuffix = filepath.Join(runID, "input.bin")
			case "stage run":
				replacedPath = filepath.Dir(inputPath)
				protectedDirectory = filepath.Join(inputRoot, "stage-runs", "protected-run")
				payloadSuffix = "input.bin"
			case "stage attempt":
				replacedPath = filepath.Join(outputRoot, attemptID)
				protectedDirectory = filepath.Join(outputRoot, "protected-attempt")
				payloadSuffix = filepath.Join("nested", "output.bin")
			case "output parent":
				replacedPath = filepath.Dir(outputPath)
				protectedDirectory = filepath.Join(outputRoot, attemptID, "protected-output")
				payloadSuffix = "output.bin"
			}
			protectedPath := filepath.Join(protectedDirectory, payloadSuffix)
			writeBoundScratchFixture(t, protectedPath, payload)
			moved := replacedPath + ".original"
			if err := os.Rename(replacedPath, moved); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(protectedDirectory, replacedPath); err != nil {
				t.Fatal(err)
			}
			manifest := stageartifact.LocalOutputManifestV1{PayloadSHA256: sha256.Sum256(payload), SizeBytes: int64(len(payload))}
			if err := retireBoundScratch(context.Background(), inputs, outputs, "output.bin", manifest); err != nil {
				t.Fatal(err)
			}
			if actual, err := os.ReadFile(protectedPath); err != nil || string(actual) != string(payload) {
				t.Fatalf("replaced Stage directory redirected retirement: bytes=%q error=%v", actual, err)
			}
			if _, err := os.Stat(filepath.Join(moved, payloadSuffix)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("bound original scratch remained: %v", err)
			}
			if info, err := os.Lstat(replacedPath); err != nil || info.Mode()&os.ModeSymlink == 0 {
				t.Fatalf("retirement changed replacement directory: %v", err)
			}
		})
	}
}

func writeBoundScratchFixture(t *testing.T, file string, payload []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(file), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, payload, 0o600); err != nil {
		t.Fatal(err)
	}
}
