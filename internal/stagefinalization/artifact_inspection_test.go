package stagefinalization

import (
	"crypto/sha256"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestH3NativeMediaPreservesAlignedFramesAndFullAudio(t *testing.T) {
	for _, sample := range []struct{ requested, frames, videoMS, audioMS int32 }{
		{4000, 107, 4458, 4450}, {5000, 124, 5167, 5175},
		{10000, 243, 10125, 10125}, {15000, 362, 15083, 15075},
	} {
		request, err := applyArtifactInspectionExpectations(ArtifactInspectionRequest{
			Kind: ArtifactKindVideo, ArtifactID: uuid.New(), ObjectVersionID: "full-av-v1",
			ExpectedSizeBytes: 1000, ExpectedSHA256: sha256.Sum256([]byte("complete-output")), ExpectedContentType: "video/mp4",
		}, artifactInspectionExpectations{
			width: 1344, height: 768, durationMilliseconds: sample.requested,
			frameRateMilli: 24000, codec: "h264", container: "mp4", mediaContract: MediaContractH3NativeAV,
		})
		if err != nil {
			t.Fatal(err)
		}
		if request.ExpectedFrameCount != sample.frames || request.ExpectedDurationMillis != sample.videoMS || request.ExpectedAudioDurationMillis != sample.audioMS {
			t.Fatalf("duration %d: unexpected expectations: %+v", sample.requested, request)
		}
		inspection := ArtifactInspection{
			ObjectVersionID: request.ObjectVersionID, SizeBytes: request.ExpectedSizeBytes, SHA256: request.ExpectedSHA256,
			ContentType: "video/mp4", Width: 1344, Height: 768, DurationMillis: sample.videoMS,
			FrameRateMilli: 24000, FrameCount: sample.frames, Codec: "h264", Container: "mp4", ValidatorRevision: "h3-full-av-test",
			ContainerDurationMillis: max(sample.videoMS, sample.audioMS),
			Audio:                   &AudioInspection{Codec: "aac", Channels: 2, SampleRate: 32000, DurationMillis: sample.audioMS},
		}
		receipt, _, valid := validateArtifactInspection(request, inspection)
		if !valid {
			t.Fatal("complete native output rejected")
		}
		var decoded artifactValidationReceipt
		if err := json.Unmarshal(receipt, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.RequestedDurationMillis != sample.requested || decoded.DurationMillis != sample.videoMS || decoded.Audio.DurationMillis != sample.audioMS {
			t.Fatalf("receipt replaced actual durations: %s", receipt)
		}
		for _, mutation := range []struct {
			name  string
			apply func(*ArtifactInspection)
		}{
			{"trimmed video", func(i *ArtifactInspection) {
				i.FrameCount = sample.requested * 24 / 1000
				i.DurationMillis = sample.requested
			}},
			{"extra frames", func(i *ArtifactInspection) { i.FrameCount++ }},
			{"missing audio", func(i *ArtifactInspection) { i.Audio = nil }},
			{"shortened audio", func(i *ArtifactInspection) { i.Audio.DurationMillis-- }},
			{"long audio", func(i *ArtifactInspection) { i.Audio.DurationMillis += 33 }},
			{"wrong audio codec", func(i *ArtifactInspection) { i.Audio.Codec = "mp3" }},
			{"mono audio", func(i *ArtifactInspection) { i.Audio.Channels = 1 }},
			{"wrong sample rate", func(i *ArtifactInspection) { i.Audio.SampleRate = 48000 }},
			{"truncated container", func(i *ArtifactInspection) { i.ContainerDurationMillis = sample.requested }},
		} {
			i := inspection
			a := *inspection.Audio
			i.Audio = &a
			mutation.apply(&i)
			if _, _, ok := validateArtifactInspection(request, i); ok {
				t.Fatalf("accepted %s", mutation.name)
			}
		}
	}
}

func TestMediaContractOptInAndInvalidExpectations(t *testing.T) {
	base := artifactInspectionExpectations{width: 1344, height: 768, durationMilliseconds: 5000, frameRateMilli: 24000, codec: "h264", container: "mp4"}
	r, err := applyArtifactInspectionExpectations(ArtifactInspectionRequest{Kind: ArtifactKindVideo}, base)
	if err != nil || r.ExpectedFrameCount != 120 || r.ExpectedDurationMillis != 5000 || r.MediaContract != MediaContractExactVideo {
		t.Fatalf("legacy exact contract changed: %+v, %v", r, err)
	}
	for _, contract := range []MediaContract{"unknown", MediaContractH3NativeAV} {
		bad := base
		bad.mediaContract = contract
		bad.frameRateMilli = 30000
		if _, err := applyArtifactInspectionExpectations(ArtifactInspectionRequest{Kind: ArtifactKindVideo}, bad); err == nil {
			t.Fatal("accepted unknown contract or unsupported H3 frame rate")
		}
	}
	if validInspectedAudio(r, ArtifactInspection{Audio: &AudioInspection{Codec: "aac"}}) {
		t.Fatal("audio contract applied without opt-in")
	}
}
