package artifactvalidator

import (
	"encoding/json"
	"testing"

	"github.com/vivym/vela/internal/stagefinalization"
)

func TestH3ParserRequiresBothCompleteTracksAndRejectsExtraStreams(t *testing.T) {
	video := map[string]any{"codec_name": "h264", "codec_type": "video", "width": 1344, "height": 768, "avg_frame_rate": "24/1", "nb_frames": "124", "duration": "5.166667", "start_time": "0.000000"}
	audio := map[string]any{"codec_name": "aac", "codec_type": "audio", "sample_rate": "32000", "channels": 2, "duration": "5.175000", "start_time": "0.000000"}
	probe := func(streams []any, kind stagefinalization.ArtifactKind, contract stagefinalization.MediaContract) error {
		data, err := json.Marshal(map[string]any{"program_version": map[string]string{"version": "8.0.1"}, "streams": streams,
			"format": map[string]string{"format_name": "mp4", "duration": "5.175000", "size": "1000"}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = parseFFprobeOutputForContract(data, kind, "8.0.1", contract)
		return err
	}
	for _, streams := range [][]any{{video, audio}, {audio, video}} {
		if err := probe(streams, stagefinalization.ArtifactKindVideo, stagefinalization.MediaContractH3NativeAV); err != nil {
			t.Fatal(err)
		}
	}
	for _, streams := range [][]any{{video}, {audio}, {video, video}, {audio, audio}, {video, audio, audio}, {video, map[string]any{"codec_type": "subtitle"}}} {
		if err := probe(streams, stagefinalization.ArtifactKindVideo, stagefinalization.MediaContractH3NativeAV); err == nil {
			t.Fatalf("accepted invalid streams: %v", streams)
		}
	}
	for _, change := range []struct {
		field string
		value any
	}{
		{"codec_name", "mp3"}, {"channels", 1}, {"sample_rate", "48000"}, {"duration", "N/A"}, {"duration", "NaN"}, {"duration", "0"}, {"start_time", "1.000000"},
	} {
		bad := make(map[string]any)
		for k, v := range audio {
			bad[k] = v
		}
		bad[change.field] = change.value
		if err := probe([]any{video, bad}, stagefinalization.ArtifactKindVideo, stagefinalization.MediaContractH3NativeAV); err == nil {
			t.Fatalf("accepted invalid audio %s", change.field)
		}
	}
	if err := probe([]any{video, audio}, stagefinalization.ArtifactKindVideo, stagefinalization.MediaContractExactVideo); err == nil {
		t.Fatal("accepted audio without contract opt-in")
	}
	if err := probe([]any{video, audio}, stagefinalization.ArtifactKindThumbnail, stagefinalization.MediaContractH3NativeAV); err == nil {
		t.Fatal("accepted audio in thumbnail")
	}
}
