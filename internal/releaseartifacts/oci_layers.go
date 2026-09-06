package releaseartifacts

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	ocidigest "github.com/opencontainers/go-digest"
	ociv1 "github.com/opencontainers/image-spec/specs-go/v1"
)

const maximumOCIZstdWindowBytes = uint64(64 << 20)

func verifyOCIImageLayers(ctx context.Context, root *ociLayoutRoot, layers []ociv1.Descriptor, diffIDs []ocidigest.Digest, maximumExpanded int64) error {
	if ctx == nil || len(layers) == 0 || len(layers) > 4096 || len(layers) != len(diffIDs) ||
		maximumExpanded <= 0 || maximumExpanded > maximumOCIImageExpandedBytes {
		return errors.New("OCI image layers require an ordered DiffID and expanded byte budget")
	}
	totalLayerBytes, totalExpandedBytes := int64(0), int64(0)
	for index, layer := range layers {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !validOCIImageLayerMediaType(layer.MediaType) || layer.Size <= 0 ||
			layer.Size > maximumOCIImageLayoutBytes-totalLayerBytes {
			return errors.New("OCI image layer bytes exceed the bounded layout budget")
		}
		totalLayerBytes += layer.Size
		expanded, err := verifyOCIImageLayer(ctx, root, layer, diffIDs[index], maximumExpanded-totalExpandedBytes)
		if err != nil {
			return fmt.Errorf("verify OCI layer %d: %w", index, err)
		}
		totalExpandedBytes += expanded
	}
	return ctx.Err()
}

func verifyOCIImageLayer(ctx context.Context, root *ociLayoutRoot, descriptor ociv1.Descriptor, diffID ocidigest.Digest, maximumExpanded int64) (int64, error) {
	file, err := openVerifiedOCIBlob(root, descriptor, maximumOCIImageLayoutBytes)
	if err != nil {
		return 0, err
	}
	defer func() { _ = file.Close() }()
	return verifyOCIImageLayerStream(ctx, file, descriptor, diffID, maximumExpanded)
}

// Hash both representations during the same bounded read. An independently
// valid config/blob pair is not enough: DiffIDs bind the ordered decoded layers.
// This checks content identities, not tar extraction or executable provenance.
func verifyOCIImageLayerStream(ctx context.Context, input io.Reader, descriptor ociv1.Descriptor, diffID ocidigest.Digest, maximumExpanded int64) (int64, error) {
	if ctx == nil || input == nil || maximumExpanded <= 0 || maximumExpanded > maximumOCIImageExpandedBytes ||
		descriptor.Size <= 0 || descriptor.Size > maximumOCIImageLayoutBytes ||
		descriptor.Digest.Algorithm() != ocidigest.SHA256 || descriptor.Digest.Validate() != nil ||
		diffID.Algorithm() != ocidigest.SHA256 || diffID.Validate() != nil ||
		len(descriptor.URLs) != 0 || len(descriptor.Data) != 0 {
		return 0, errors.New("OCI layer validation requires bounded content and SHA-256 identities")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	compressedDigest := sha256.New()
	compressed := &io.LimitedReader{R: input, N: descriptor.Size + 1}
	encoded := &ociLayerContextReader{ctx: ctx, reader: io.TeeReader(compressed, compressedDigest)}
	decoded, err := decodeOCIImageLayer(encoded, descriptor.MediaType)
	if err != nil {
		return 0, fmt.Errorf("decode OCI layer: %w", err)
	}
	defer func() { _ = decoded.Close() }()
	uncompressedDigest := sha256.New()
	count, err := io.Copy(uncompressedDigest, io.LimitReader(
		&ociLayerContextReader{ctx: ctx, reader: decoded}, maximumExpanded+1,
	))
	if err != nil {
		return 0, fmt.Errorf("read uncompressed OCI layer: %w", err)
	}
	if count <= 0 || count > maximumExpanded {
		return 0, errors.New("OCI uncompressed layer exceeds the remaining expanded image budget or is empty")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if compressed.N != 1 || descriptor.Digest.String() != "sha256:"+hex.EncodeToString(compressedDigest.Sum(nil)) {
		return 0, errors.New("OCI blob digest or size does not match its descriptor")
	}
	if diffID.String() != "sha256:"+hex.EncodeToString(uncompressedDigest.Sum(nil)) {
		return 0, errors.New("OCI uncompressed layer digest does not match its ordered config DiffID")
	}
	return count, nil
}

func decodeOCIImageLayer(input io.Reader, mediaType string) (io.ReadCloser, error) {
	switch mediaType {
	case ociv1.MediaTypeImageLayer:
		return io.NopCloser(input), nil
	case ociv1.MediaTypeImageLayerGzip:
		return gzip.NewReader(input)
	case ociv1.MediaTypeImageLayerZstd:
		decoder, err := zstd.NewReader(input, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxMemory(maximumOCIZstdWindowBytes), zstd.WithDecoderMaxWindow(maximumOCIZstdWindowBytes))
		if err != nil {
			return nil, err
		}
		return decoder.IOReadCloser(), nil
	default:
		return nil, errors.New("unsupported OCI layer media type")
	}
}

type ociLayerContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *ociLayerContextReader) Read(output []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(output)
}
