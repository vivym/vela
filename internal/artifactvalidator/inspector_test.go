package artifactvalidator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/workercontrol"
)

func TestInspectorReadsAndHashesExactVersionBeforeFixedFFprobe(t *testing.T) {
	media := []byte("fixed-exact-version-media-fixture")
	digest := sha256.Sum256(media)
	source := &recordingExactVersionSource{
		reader: &artifactstore.ExactVersionReader{
			ReadCloser: io.NopCloser(bytes.NewReader(media)),
			ObjectVersion: artifactstore.ObjectVersion{
				ObjectKey:   "artifacts/org/project/job/attempt/artifact/video.mp4",
				VersionID:   "version-0001",
				SizeBytes:   int64(len(media)),
				ContentType: "video/mp4",
			},
		},
	}
	sandbox := &recordingSandbox{
		output: []byte(`{
			"program_version":{"version":"8.0.1"},
			"streams":[{
				"codec_name":"h264",
				"codec_type":"video",
				"width":1280,
				"height":720,
				"avg_frame_rate":"30/1",
				"nb_frames":"120",
				"duration":"4.000000"
			}],
			"format":{
				"format_name":"mov,mp4,m4a,3gp,3g2,mj2",
				"duration":"4.000000",
				"size":"33"
			}
		}`),
	}
	inspector, err := NewInspector(source, sandbox, Config{
		MaxInputBytes:          1024,
		MaxProbeOutputBytes:    64 * 1024,
		Timeout:                time.Second,
		ExpectedFFprobeVersion: "8.0.1",
		ValidatorRevision:      "ffprobe-8.0.1-sandbox-v1",
		SpoolDirectory:         t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewInspector: %v", err)
	}
	request := workercontrol.ArtifactInspectionRequest{
		ArtifactID:             uuid.New(),
		UploadID:               uuid.New(),
		Kind:                   workercontrol.ArtifactKindVideo,
		Ordinal:                0,
		ObjectKey:              source.reader.ObjectKey,
		ObjectVersionID:        source.reader.VersionID,
		ExpectedSizeBytes:      int64(len(media)),
		ExpectedSHA256:         digest,
		ExpectedContentType:    "video/mp4",
		ExpectedWidth:          1280,
		ExpectedHeight:         720,
		ExpectedDurationMillis: 4000,
		ExpectedFrameRateMilli: 30000,
		ExpectedFrameCount:     120,
		ExpectedCodec:          "h264",
		ExpectedContainer:      "mp4",
	}
	inspection, err := inspector.Inspect(context.Background(), request)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if source.objectKey != request.ObjectKey || source.versionID != request.ObjectVersionID {
		t.Fatalf("exact-version read = %q/%q", source.objectKey, source.versionID)
	}
	if !bytes.Equal(sandbox.input, media) {
		t.Fatalf("sandbox input = %q, want exact version bytes", sandbox.input)
	}
	if inspection.ObjectVersionID != request.ObjectVersionID ||
		inspection.SizeBytes != int64(len(media)) || inspection.SHA256 != digest ||
		inspection.ContentType != "video/mp4" || inspection.Width != 1280 ||
		inspection.Height != 720 || inspection.DurationMillis != 4000 ||
		inspection.FrameRateMilli != 30000 || inspection.FrameCount != 120 ||
		inspection.Codec != "h264" || inspection.Container != "mp4" ||
		inspection.ValidatorRevision != "ffprobe-8.0.1-sandbox-v1" {
		t.Fatalf("ArtifactInspection = %#v", inspection)
	}
}

func TestInspectorRejectsChangedExactVersionBytesBeforeSandbox(t *testing.T) {
	media := []byte("changed-object-bytes")
	wantDigest := sha256.Sum256([]byte("committed-object-bytes"))
	source := &recordingExactVersionSource{
		reader: &artifactstore.ExactVersionReader{
			ReadCloser: io.NopCloser(bytes.NewReader(media)),
			ObjectVersion: artifactstore.ObjectVersion{
				ObjectKey:   "artifacts/org/project/job/attempt/artifact/video.mp4",
				VersionID:   "version-0001",
				SizeBytes:   int64(len(media)),
				ContentType: "video/mp4",
			},
		},
	}
	sandbox := &recordingSandbox{output: []byte(`{"not":"reachable"}`)}
	inspector := newTestInspector(t, source, sandbox, 64*1024)
	_, err := inspector.Inspect(context.Background(), workercontrol.ArtifactInspectionRequest{
		ArtifactID:          uuid.New(),
		UploadID:            uuid.New(),
		Kind:                workercontrol.ArtifactKindVideo,
		ObjectKey:           source.reader.ObjectKey,
		ObjectVersionID:     source.reader.VersionID,
		ExpectedSizeBytes:   int64(len(media)),
		ExpectedSHA256:      wantDigest,
		ExpectedContentType: "video/mp4",
	})
	if err == nil {
		t.Fatal("Inspect accepted changed exact-version bytes")
	}
	if sandbox.input != nil {
		t.Fatalf("sandbox received changed exact-version bytes %q", sandbox.input)
	}
}

func TestInspectorRejectsFFprobeVersionDriftAndOversizeOutput(t *testing.T) {
	for _, test := range []struct {
		name           string
		probeOutput    []byte
		maxOutputBytes int64
	}{
		{
			name: "Version drift",
			probeOutput: []byte(`{
				"program_version":{"version":"8.0.2"},
				"streams":[{"codec_name":"h264","codec_type":"video","width":1,"height":1,"avg_frame_rate":"1/1","nb_frames":"1","duration":"1.0"}],
				"format":{"format_name":"mp4","duration":"1.0","size":"5"}
			}`),
			maxOutputBytes: 64 * 1024,
		},
		{
			name:           "Output bound",
			probeOutput:    bytes.Repeat([]byte("x"), 65),
			maxOutputBytes: 64,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			media := []byte("media")
			digest := sha256.Sum256(media)
			source := &recordingExactVersionSource{
				reader: &artifactstore.ExactVersionReader{
					ReadCloser: io.NopCloser(bytes.NewReader(media)),
					ObjectVersion: artifactstore.ObjectVersion{
						ObjectKey:   "artifacts/org/project/job/attempt/artifact/video.mp4",
						VersionID:   "version-0001",
						SizeBytes:   int64(len(media)),
						ContentType: "video/mp4",
					},
				},
			}
			sandbox := &recordingSandbox{output: test.probeOutput}
			inspector := newTestInspector(t, source, sandbox, test.maxOutputBytes)
			_, err := inspector.Inspect(context.Background(), workercontrol.ArtifactInspectionRequest{
				ArtifactID:          uuid.New(),
				UploadID:            uuid.New(),
				Kind:                workercontrol.ArtifactKindVideo,
				ObjectKey:           source.reader.ObjectKey,
				ObjectVersionID:     source.reader.VersionID,
				ExpectedSizeBytes:   int64(len(media)),
				ExpectedSHA256:      digest,
				ExpectedContentType: "video/mp4",
			})
			if err == nil {
				t.Fatal("Inspect accepted invalid ffprobe output")
			}
		})
	}
}

func TestInspectorEnforcesProbeTimeout(t *testing.T) {
	media := []byte("media")
	digest := sha256.Sum256(media)
	source := &recordingExactVersionSource{
		reader: &artifactstore.ExactVersionReader{
			ReadCloser: io.NopCloser(bytes.NewReader(media)),
			ObjectVersion: artifactstore.ObjectVersion{
				ObjectKey:   "artifacts/org/project/job/attempt/artifact/video.mp4",
				VersionID:   "version-0001",
				SizeBytes:   int64(len(media)),
				ContentType: "video/mp4",
			},
		},
	}
	inspector, err := NewInspector(source, blockingSandbox{}, Config{
		MaxInputBytes:          1024,
		MaxProbeOutputBytes:    64 * 1024,
		Timeout:                time.Second,
		ExpectedFFprobeVersion: "8.0.1",
		ValidatorRevision:      "ffprobe-8.0.1-sandbox-v1",
		SpoolDirectory:         t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewInspector: %v", err)
	}
	startedAt := time.Now()
	_, err = inspector.Inspect(context.Background(), workercontrol.ArtifactInspectionRequest{
		ArtifactID:          uuid.New(),
		UploadID:            uuid.New(),
		Kind:                workercontrol.ArtifactKindVideo,
		ObjectKey:           source.reader.ObjectKey,
		ObjectVersionID:     source.reader.VersionID,
		ExpectedSizeBytes:   int64(len(media)),
		ExpectedSHA256:      digest,
		ExpectedContentType: "video/mp4",
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Inspect timeout error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(startedAt); elapsed < time.Second || elapsed > 2*time.Second {
		t.Fatalf("Inspect timeout elapsed = %s, want configured one-second bound", elapsed)
	}
}

func newTestInspector(
	t *testing.T,
	source ExactVersionSource,
	sandbox Sandbox,
	maxOutputBytes int64,
) *Inspector {
	t.Helper()
	inspector, err := NewInspector(source, sandbox, Config{
		MaxInputBytes:          1024,
		MaxProbeOutputBytes:    maxOutputBytes,
		Timeout:                time.Second,
		ExpectedFFprobeVersion: "8.0.1",
		ValidatorRevision:      "ffprobe-8.0.1-sandbox-v1",
		SpoolDirectory:         t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewInspector: %v", err)
	}
	return inspector
}

type recordingExactVersionSource struct {
	reader    *artifactstore.ExactVersionReader
	objectKey string
	versionID string
}

func (source *recordingExactVersionSource) ReadExactVersion(
	_ context.Context,
	objectKey string,
	versionID string,
) (*artifactstore.ExactVersionReader, error) {
	source.objectKey = objectKey
	source.versionID = versionID
	return source.reader, nil
}

type recordingSandbox struct {
	input  []byte
	output []byte
}

type blockingSandbox struct{}

func (blockingSandbox) Probe(ctx context.Context, _ *os.File) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (sandbox *recordingSandbox) Probe(_ context.Context, input *os.File) ([]byte, error) {
	if _, err := input.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	content, err := io.ReadAll(input)
	if err != nil {
		return nil, err
	}
	sandbox.input = content
	return sandbox.output, nil
}
