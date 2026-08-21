package artifactvalidator

import (
	"context"
	_ "embed"
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/workercontrol"
)

//go:embed testdata/h264_16x16_1fps.mp4.b64
var h264MP4FixtureBase64 string

const webPFixtureBase64 = "UklGRiIAAABXRUJQVlA4IBYAAAAwAQCdASoBAAEADsD+JaQAA3AA/v3AgAA="

func TestPinnedFFprobeCommandProducesParseableWebPInspection(t *testing.T) {
	facts := probePinnedFFprobe(
		t, "thumbnail.webp", webPFixtureBase64, workercontrol.ArtifactKindThumbnail,
	)
	if facts.Width != 1 || facts.Height != 1 || facts.Codec != "webp" ||
		facts.Container != "webp" || facts.FrameCount != 1 {
		t.Fatalf("WebP media facts = %#v", facts)
	}
}

func TestPinnedFFprobeCommandProducesParseableH264MP4Inspection(t *testing.T) {
	facts := probePinnedFFprobe(
		t, "video.mp4", h264MP4FixtureBase64, workercontrol.ArtifactKindVideo,
	)
	if facts.Width != 16 || facts.Height != 16 || facts.Codec != "h264" ||
		facts.Container != "mp4" || facts.FrameCount != 1 || facts.DurationMillis != 1_000 ||
		facts.FrameRateMilli != 1_000 {
		t.Fatalf("MP4/H.264 media facts = %#v", facts)
	}
}

func probePinnedFFprobe(
	t *testing.T,
	name string,
	fixture string,
	kind workercontrol.ArtifactKind,
) mediaFacts {
	t.Helper()
	ffprobePath, expectedVersion := pinnedFFprobe(t)
	media, err := base64.StdEncoding.DecodeString(strings.TrimSpace(fixture))
	if err != nil {
		t.Fatalf("decode fixed %s fixture: %v", name, err)
	}
	inputPath := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(inputPath, media, 0o600); err != nil {
		t.Fatalf("write fixed %s fixture: %v", name, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, ffprobePath, ffprobeArguments(inputPath)...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run production ffprobe command: %v: %s", err, output)
	}
	facts, err := parseFFprobeOutput(output, kind, expectedVersion)
	if err != nil {
		t.Fatalf("parse production ffprobe output: %v; output=%s", err, output)
	}
	return facts
}

func pinnedFFprobe(t *testing.T) (string, string) {
	t.Helper()
	required := os.Getenv("VELA_REQUIRE_PINNED_FFPROBE") == "1"
	ffprobePath, err := exec.LookPath("ffprobe")
	if err != nil {
		if required {
			t.Fatal("pinned ffprobe is required but is not installed")
		}
		t.Skip("ffprobe is not installed")
	}
	versionOutput, err := exec.Command(ffprobePath, "-version").Output()
	if err != nil {
		t.Fatalf("read ffprobe version: %v", err)
	}
	const expectedVersion = "8.0.1"
	if !strings.HasPrefix(string(versionOutput), "ffprobe version "+expectedVersion+" ") {
		if required {
			t.Fatalf("ffprobe %s is required; version output=%s", expectedVersion, versionOutput)
		}
		t.Skipf("ffprobe %s is required for the production CLI conformance test", expectedVersion)
	}
	return ffprobePath, expectedVersion
}
