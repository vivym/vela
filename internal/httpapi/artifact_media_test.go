package httpapi

import (
	"testing"

	"github.com/vivym/vela/internal/artifactaccess"
)

func TestArtifactAPIReportsCompleteAudioVideoAndRequestedDuration(t *testing.T) {
	result := toAPIArtifactSet(artifactaccess.ArtifactSet{Artifacts: []artifactaccess.Artifact{{
		Kind: "VIDEO", DownloadURL: "https://example.invalid/full.mp4",
		Media: &artifactaccess.MediaMetadata{RequestedDurationMillis: 5000, DurationMillis: 5167, ContainerDurationMillis: 5175, FrameCount: 124, FrameRateMilli: 24000,
			Audio: &artifactaccess.AudioMetadata{Codec: "aac", SampleRate: 32000, Channels: 2, DurationMillis: 5175}},
	}}})
	m := result.Artifacts[0].Media
	if result.Artifacts[0].DownloadUrl != "https://example.invalid/full.mp4" || m == nil || m.RequestedDurationMilliseconds == nil || *m.RequestedDurationMilliseconds != 5000 || m.DurationMilliseconds != 5167 || m.ContainerDurationMilliseconds == nil || *m.ContainerDurationMilliseconds != 5175 || m.FrameCount != 124 || m.Audio == nil || m.Audio.DurationMilliseconds != 5175 {
		t.Fatalf("full output facts lost: %+v", m)
	}
	if toAPIArtifactMedia(nil) != nil {
		t.Fatal("invented media facts")
	}
}
