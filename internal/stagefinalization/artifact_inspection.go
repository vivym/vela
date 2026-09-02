package stagefinalization

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"

	"github.com/google/uuid"
)

type ArtifactKind string

const (
	ArtifactKindVideo     ArtifactKind = "VIDEO"
	ArtifactKindThumbnail ArtifactKind = "THUMBNAIL"
)

type ArtifactInspectionRequest struct {
	ArtifactID             uuid.UUID
	UploadID               uuid.UUID
	Kind                   ArtifactKind
	Ordinal                int32
	ObjectKey              string
	ObjectVersionID        string
	ExpectedSizeBytes      int64
	ExpectedSHA256         [sha256.Size]byte
	ExpectedContentType    string
	ExpectedWidth          int32
	ExpectedHeight         int32
	ExpectedDurationMillis int32
	ExpectedFrameRateMilli int32
	ExpectedFrameCount     int32
	ExpectedCodec          string
	ExpectedContainer      string
}

type ArtifactInspection struct {
	ObjectVersionID   string
	SizeBytes         int64
	SHA256            [sha256.Size]byte
	ContentType       string
	Width             int32
	Height            int32
	DurationMillis    int32
	FrameRateMilli    int32
	FrameCount        int32
	Codec             string
	Container         string
	ValidatorRevision string
}

type ArtifactInspector interface {
	Inspect(context.Context, ArtifactInspectionRequest) (ArtifactInspection, error)
}

type artifactInspectionExpectations struct {
	width                int32
	height               int32
	durationMilliseconds int32
	frameRateMilli       int32
	codec                string
	container            string
}

func applyArtifactInspectionExpectations(
	request ArtifactInspectionRequest,
	expected artifactInspectionExpectations,
) (ArtifactInspectionRequest, error) {
	switch request.Kind {
	case ArtifactKindVideo:
		frameProduct := int64(expected.durationMilliseconds) * int64(expected.frameRateMilli)
		if frameProduct%1_000_000 != 0 || frameProduct/1_000_000 > math.MaxInt32 {
			return ArtifactInspectionRequest{}, errors.New("OutputSpec frame count is not integral")
		}
		request.ExpectedWidth = expected.width
		request.ExpectedHeight = expected.height
		request.ExpectedDurationMillis = expected.durationMilliseconds
		request.ExpectedFrameRateMilli = expected.frameRateMilli
		request.ExpectedFrameCount = int32(frameProduct / 1_000_000)
		request.ExpectedCodec = expected.codec
		request.ExpectedContainer = expected.container
	case ArtifactKindThumbnail:
		request.ExpectedWidth = 320
		request.ExpectedHeight = 180
		request.ExpectedFrameCount = 1
		request.ExpectedCodec = "webp"
		request.ExpectedContainer = "webp"
	default:
		return ArtifactInspectionRequest{}, errors.New("unsupported Artifact kind")
	}
	return request, nil
}

type artifactValidationReceipt struct {
	ArtifactID        string `json:"artifact_id"`
	ObjectKey         string `json:"object_key"`
	ObjectVersionID   string `json:"object_version_id"`
	SizeBytes         int64  `json:"size_bytes"`
	SHA256            string `json:"sha256"`
	ContentType       string `json:"content_type"`
	Width             int32  `json:"width"`
	Height            int32  `json:"height"`
	DurationMillis    int32  `json:"duration_milliseconds"`
	FrameRateMilli    int32  `json:"frame_rate_milli"`
	FrameCount        int32  `json:"frame_count"`
	Codec             string `json:"codec"`
	Container         string `json:"container"`
	ValidatorRevision string `json:"validator_revision"`
}

func validateArtifactInspection(
	request ArtifactInspectionRequest,
	inspection ArtifactInspection,
) ([]byte, [sha256.Size]byte, bool) {
	if inspection.ObjectVersionID != request.ObjectVersionID ||
		inspection.SizeBytes != request.ExpectedSizeBytes ||
		inspection.SHA256 != request.ExpectedSHA256 ||
		inspection.ContentType != request.ExpectedContentType ||
		inspection.Width != request.ExpectedWidth ||
		inspection.Height != request.ExpectedHeight ||
		inspection.DurationMillis != request.ExpectedDurationMillis ||
		inspection.FrameRateMilli != request.ExpectedFrameRateMilli ||
		inspection.FrameCount != request.ExpectedFrameCount ||
		inspection.Codec != request.ExpectedCodec ||
		inspection.Container != request.ExpectedContainer ||
		len(inspection.ValidatorRevision) == 0 || len(inspection.ValidatorRevision) > 200 ||
		strings.ContainsRune(inspection.ValidatorRevision, '\x00') {
		return nil, [sha256.Size]byte{}, false
	}
	receipt, err := json.Marshal(artifactValidationReceipt{
		ArtifactID: request.ArtifactID.String(), ObjectKey: request.ObjectKey,
		ObjectVersionID: inspection.ObjectVersionID, SizeBytes: inspection.SizeBytes,
		SHA256: hex.EncodeToString(inspection.SHA256[:]), ContentType: inspection.ContentType,
		Width: inspection.Width, Height: inspection.Height,
		DurationMillis: inspection.DurationMillis, FrameRateMilli: inspection.FrameRateMilli,
		FrameCount: inspection.FrameCount, Codec: inspection.Codec,
		Container: inspection.Container, ValidatorRevision: inspection.ValidatorRevision,
	})
	if err != nil {
		return nil, [sha256.Size]byte{}, false
	}
	return receipt, sha256.Sum256(receipt), true
}
