package artifactvalidator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/stagefinalization"
)

type ExactVersionSource interface {
	ReadExactVersion(context.Context, string, string) (*artifactstore.ExactVersionReader, error)
}

type Sandbox interface {
	Probe(context.Context, *os.File) ([]byte, error)
}

type Config struct {
	MaxInputBytes          int64
	MaxProbeOutputBytes    int64
	Timeout                time.Duration
	ExpectedFFprobeVersion string
	ValidatorRevision      string
	SpoolDirectory         string
}

type Inspector struct {
	source                 ExactVersionSource
	sandbox                Sandbox
	maxInputBytes          int64
	maxProbeOutputBytes    int64
	timeout                time.Duration
	expectedFFprobeVersion string
	validatorRevision      string
	spoolDirectory         string
}

var _ stagefinalization.ArtifactInspector = (*Inspector)(nil)

func NewInspector(source ExactVersionSource, sandbox Sandbox, config Config) (*Inspector, error) {
	if source == nil || sandbox == nil || config.MaxInputBytes <= 0 ||
		config.MaxProbeOutputBytes <= 0 || config.MaxProbeOutputBytes > 16*1024*1024 ||
		config.Timeout < time.Second || config.Timeout > 10*time.Minute ||
		!validFixedText(config.ExpectedFFprobeVersion, 100) ||
		!validFixedText(config.ValidatorRevision, 200) {
		return nil, errors.New("invalid Artifact inspector configuration")
	}
	spoolDirectory := filepath.Clean(config.SpoolDirectory)
	if !filepath.IsAbs(spoolDirectory) {
		return nil, errors.New("artifact inspector spool directory must be absolute")
	}
	info, err := os.Stat(spoolDirectory)
	if err != nil || !info.IsDir() {
		return nil, errors.New("artifact inspector spool directory is unavailable")
	}
	return &Inspector{
		source:                 source,
		sandbox:                sandbox,
		maxInputBytes:          config.MaxInputBytes,
		maxProbeOutputBytes:    config.MaxProbeOutputBytes,
		timeout:                config.Timeout,
		expectedFFprobeVersion: config.ExpectedFFprobeVersion,
		validatorRevision:      config.ValidatorRevision,
		spoolDirectory:         spoolDirectory,
	}, nil
}

func (inspector *Inspector) Inspect(
	ctx context.Context,
	request stagefinalization.ArtifactInspectionRequest,
) (stagefinalization.ArtifactInspection, error) {
	if inspector == nil || inspector.source == nil || inspector.sandbox == nil {
		return stagefinalization.ArtifactInspection{}, errors.New("artifact inspector is not configured")
	}
	if ctx == nil {
		return stagefinalization.ArtifactInspection{}, errors.New("artifact inspection context is required")
	}
	if request.ExpectedSizeBytes <= 0 || request.ExpectedSizeBytes > inspector.maxInputBytes ||
		request.ExpectedSHA256 == [sha256.Size]byte{} || request.ObjectKey == "" ||
		request.ObjectVersionID == "" || request.ExpectedContentType == "" {
		return stagefinalization.ArtifactInspection{}, errors.New("artifact inspection request exceeds configured bounds")
	}

	exact, err := inspector.source.ReadExactVersion(
		ctx,
		request.ObjectKey,
		request.ObjectVersionID,
	)
	if err != nil {
		return stagefinalization.ArtifactInspection{}, fmt.Errorf("read exact Artifact version: %w", err)
	}
	if exact == nil || exact.ReadCloser == nil {
		return stagefinalization.ArtifactInspection{}, errors.New("exact Artifact version reader is incomplete")
	}
	defer func() { _ = exact.Close() }()
	if exact.ObjectKey != request.ObjectKey || exact.VersionID != request.ObjectVersionID ||
		exact.SizeBytes != request.ExpectedSizeBytes ||
		exact.ContentType != request.ExpectedContentType {
		return stagefinalization.ArtifactInspection{}, errors.New("exact Artifact version metadata does not match publication evidence")
	}

	spool, err := os.CreateTemp(inspector.spoolDirectory, ".vela-artifact-inspection-*")
	if err != nil {
		return stagefinalization.ArtifactInspection{}, fmt.Errorf("create Artifact inspection spool: %w", err)
	}
	spoolPath := spool.Name()
	defer func() {
		_ = spool.Close()
		_ = os.Remove(spoolPath)
	}()
	if err := spool.Chmod(0o600); err != nil {
		return stagefinalization.ArtifactInspection{}, fmt.Errorf("restrict Artifact inspection spool: %w", err)
	}
	digest := sha256.New()
	written, err := io.Copy(
		io.MultiWriter(spool, digest),
		io.LimitReader(exact, inspector.maxInputBytes+1),
	)
	if err != nil {
		return stagefinalization.ArtifactInspection{}, fmt.Errorf("spool exact Artifact version: %w", err)
	}
	if written != request.ExpectedSizeBytes || written > inspector.maxInputBytes {
		return stagefinalization.ArtifactInspection{}, errors.New("exact Artifact version size does not match publication evidence")
	}
	var observedDigest [sha256.Size]byte
	copy(observedDigest[:], digest.Sum(nil))
	if observedDigest != request.ExpectedSHA256 {
		return stagefinalization.ArtifactInspection{}, errors.New("exact Artifact version SHA-256 does not match publication evidence")
	}
	if err := spool.Sync(); err != nil {
		return stagefinalization.ArtifactInspection{}, fmt.Errorf("sync Artifact inspection spool: %w", err)
	}
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return stagefinalization.ArtifactInspection{}, fmt.Errorf("rewind Artifact inspection spool: %w", err)
	}

	probeContext, cancel := context.WithTimeout(ctx, inspector.timeout)
	defer cancel()
	output, err := inspector.sandbox.Probe(probeContext, spool)
	if err != nil {
		return stagefinalization.ArtifactInspection{}, fmt.Errorf("run sandboxed ffprobe: %w", err)
	}
	if len(output) == 0 || int64(len(output)) > inspector.maxProbeOutputBytes {
		return stagefinalization.ArtifactInspection{}, errors.New("ffprobe output is absent or exceeds configured bounds")
	}
	media, err := parseFFprobeOutput(output, request.Kind, inspector.expectedFFprobeVersion)
	if err != nil {
		return stagefinalization.ArtifactInspection{}, err
	}
	if media.SizeBytes != request.ExpectedSizeBytes {
		return stagefinalization.ArtifactInspection{}, errors.New("ffprobe size does not match exact Artifact version")
	}
	return stagefinalization.ArtifactInspection{
		ObjectVersionID:   request.ObjectVersionID,
		SizeBytes:         written,
		SHA256:            observedDigest,
		ContentType:       exact.ContentType,
		Width:             media.Width,
		Height:            media.Height,
		DurationMillis:    media.DurationMillis,
		FrameRateMilli:    media.FrameRateMilli,
		FrameCount:        media.FrameCount,
		Codec:             media.Codec,
		Container:         media.Container,
		ValidatorRevision: inspector.validatorRevision,
	}, nil
}

type ffprobeDocument struct {
	ProgramVersion struct {
		Version string `json:"version"`
	} `json:"program_version"`
	Streams []struct {
		CodecName  string `json:"codec_name"`
		CodecType  string `json:"codec_type"`
		Width      int32  `json:"width"`
		Height     int32  `json:"height"`
		FrameRate  string `json:"avg_frame_rate"`
		FrameCount string `json:"nb_frames"`
		Duration   string `json:"duration"`
	} `json:"streams"`
	Format struct {
		FormatName string `json:"format_name"`
		Duration   string `json:"duration"`
		Size       string `json:"size"`
	} `json:"format"`
	Programs     []json.RawMessage `json:"programs"`
	StreamGroups []json.RawMessage `json:"stream_groups"`
}

type mediaFacts struct {
	SizeBytes      int64
	Width          int32
	Height         int32
	DurationMillis int32
	FrameRateMilli int32
	FrameCount     int32
	Codec          string
	Container      string
}

func parseFFprobeOutput(
	output []byte,
	kind stagefinalization.ArtifactKind,
	expectedVersion string,
) (mediaFacts, error) {
	decoder := json.NewDecoder(strings.NewReader(string(output)))
	decoder.DisallowUnknownFields()
	var document ffprobeDocument
	if err := decoder.Decode(&document); err != nil {
		return mediaFacts{}, fmt.Errorf("decode bounded ffprobe output: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return mediaFacts{}, errors.New("ffprobe output must contain one JSON document")
	}
	if document.ProgramVersion.Version != expectedVersion {
		return mediaFacts{}, errors.New("ffprobe version does not match configured revision")
	}
	if len(document.Streams) != 1 || document.Streams[0].CodecType != "video" {
		return mediaFacts{}, errors.New("artifact must contain exactly one video stream")
	}
	stream := document.Streams[0]
	if stream.Width <= 0 || stream.Height <= 0 || !validFixedText(stream.CodecName, 100) {
		return mediaFacts{}, errors.New("ffprobe video stream identity is incomplete")
	}
	sizeBytes, err := strconv.ParseInt(document.Format.Size, 10, 64)
	if err != nil || sizeBytes <= 0 {
		return mediaFacts{}, errors.New("ffprobe Artifact size is invalid")
	}
	if len(document.Programs) != 0 || len(document.StreamGroups) != 0 {
		return mediaFacts{}, errors.New("artifact contains unsupported media groups")
	}
	var durationMillis int32
	var frameRateMilli int32
	var frameCount64 int64
	if kind == stagefinalization.ArtifactKindThumbnail {
		frameCount64 = 1
		if stream.FrameCount != "" {
			parsedFrameCount, frameCountErr := strconv.ParseInt(stream.FrameCount, 10, 32)
			if frameCountErr != nil || parsedFrameCount != 1 {
				return mediaFacts{}, errors.New("ffprobe thumbnail frame count is invalid")
			}
		}
	} else {
		durationMillis, err = parseDurationMillis(stream.Duration)
		if err != nil {
			durationMillis, err = parseDurationMillis(document.Format.Duration)
		}
		if err != nil {
			return mediaFacts{}, err
		}
		frameRateMilli, err = parseFrameRateMilli(stream.FrameRate)
		if err != nil {
			return mediaFacts{}, err
		}
		frameCount64, err = strconv.ParseInt(stream.FrameCount, 10, 32)
		if err != nil || frameCount64 <= 0 {
			return mediaFacts{}, errors.New("ffprobe frame count is invalid")
		}
	}
	container, err := canonicalContainer(document.Format.FormatName, stream.CodecName, kind)
	if err != nil {
		return mediaFacts{}, err
	}
	return mediaFacts{
		SizeBytes:      sizeBytes,
		Width:          stream.Width,
		Height:         stream.Height,
		DurationMillis: durationMillis,
		FrameRateMilli: frameRateMilli,
		FrameCount:     int32(frameCount64),
		Codec:          stream.CodecName,
		Container:      container,
	}, nil
}

func parseDurationMillis(value string) (int32, error) {
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 0 ||
		seconds > float64(math.MaxInt32)/1000 {
		return 0, errors.New("ffprobe duration is invalid")
	}
	milliseconds := math.Round(seconds * 1000)
	if milliseconds < 0 || milliseconds > math.MaxInt32 {
		return 0, errors.New("ffprobe duration is invalid")
	}
	return int32(milliseconds), nil
}

func parseFrameRateMilli(value string) (int32, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return 0, errors.New("ffprobe frame rate is invalid")
	}
	numerator, numeratorErr := strconv.ParseInt(parts[0], 10, 64)
	denominator, denominatorErr := strconv.ParseInt(parts[1], 10, 64)
	if numeratorErr != nil || denominatorErr != nil || numerator <= 0 || denominator <= 0 ||
		numerator > math.MaxInt64/1000 {
		return 0, errors.New("ffprobe frame rate is invalid")
	}
	scaled := numerator * 1000
	rounded := (scaled + denominator/2) / denominator
	if rounded <= 0 || rounded > math.MaxInt32 {
		return 0, errors.New("ffprobe frame rate is invalid")
	}
	return int32(rounded), nil
}

func canonicalContainer(
	formatName string,
	codec string,
	kind stagefinalization.ArtifactKind,
) (string, error) {
	formats := strings.Split(formatName, ",")
	for _, format := range formats {
		if format == "mp4" {
			return "mp4", nil
		}
	}
	if kind == stagefinalization.ArtifactKindThumbnail && codec == "webp" {
		for _, format := range formats {
			if format == "webp" || format == "webp_pipe" || format == "image2" {
				return "webp", nil
			}
		}
	}
	return "", errors.New("ffprobe container is unsupported")
}

func validFixedText(value string, maxRunes int) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" {
		return false
	}
	count := 0
	for _, character := range value {
		if !unicode.IsPrint(character) {
			return false
		}
		count++
	}
	return count <= maxRunes
}
