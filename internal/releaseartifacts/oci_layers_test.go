package releaseartifacts

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	ocidigest "github.com/opencontainers/go-digest"
	ociv1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestOCIImageLayerContentIdentity(t *testing.T) {
	t.Parallel()
	archive := ociLayerArchive(t, "runtime fixture")
	for _, mediaType := range []string{ociv1.MediaTypeImageLayer, ociv1.MediaTypeImageLayerGzip, ociv1.MediaTypeImageLayerZstd} {
		t.Run(mediaType, func(t *testing.T) {
			encoded := ociLayerEncode(t, archive, mediaType)
			descriptor := ociLayerDescriptor(encoded, mediaType)
			diffID := ocidigest.FromBytes(archive)
			count, err := verifyOCIImageLayerStream(t.Context(), bytes.NewReader(encoded), descriptor, diffID, int64(len(archive)))
			if err != nil || count != int64(len(archive)) {
				t.Fatalf("matching layer at exact expanded budget: %d %v", count, err)
			}
			for _, scenario := range []string{"wrong-diff-id", "wrong-blob-digest", "short-size", "long-size", "truncated", "over-budget", "empty-budget", "url", "embedded-data", "unknown-codec"} {
				t.Run(scenario, func(t *testing.T) {
					input, changed, expected, maximum := encoded, descriptor, diffID, int64(len(archive))
					switch scenario {
					case "wrong-diff-id":
						expected = ocidigest.FromString("another layer")
					case "wrong-blob-digest":
						changed.Digest = ocidigest.FromString("another blob")
					case "short-size":
						changed.Size--
					case "long-size":
						changed.Size++
					case "truncated":
						input = encoded[:len(encoded)-1]
						changed = ociLayerDescriptor(input, mediaType)
					case "over-budget":
						maximum--
					case "empty-budget":
						maximum = 0
					case "url":
						changed.URLs = []string{"https://example.invalid/layer"}
					case "embedded-data":
						changed.Data = encoded
					case "unknown-codec":
						changed.MediaType = "application/octet-stream"
					}
					if count, err := verifyOCIImageLayerStream(t.Context(), bytes.NewReader(input), changed, expected, maximum); err == nil || count != 0 {
						t.Fatalf("invalid layer accepted: %d %v", count, err)
					}
				})
			}
		})
	}
}

func TestOCIImageLayerCompressionContract(t *testing.T) {
	t.Parallel()
	archive := ociLayerArchive(t, "compressed runtime fixture")
	for _, mediaType := range []string{ociv1.MediaTypeImageLayerGzip, ociv1.MediaTypeImageLayerZstd} {
		t.Run(mediaType, func(t *testing.T) {
			encoded := ociLayerEncode(t, archive, mediaType)
			corrupt := slices.Clone(encoded)
			corrupt[len(corrupt)-1] ^= 1
			for scenario, input := range map[string][]byte{
				"wrong-format":  archive,
				"bad-trailer":   corrupt,
				"trailing-junk": append(slices.Clone(encoded), []byte("not another frame")...),
				"empty-decoded": ociLayerEncode(t, nil, mediaType),
			} {
				t.Run(scenario, func(t *testing.T) {
					diffID := ocidigest.FromBytes(archive)
					if scenario == "empty-decoded" {
						diffID = ocidigest.FromBytes(nil)
					}
					if count, err := verifyOCIImageLayerStream(t.Context(), bytes.NewReader(input), ociLayerDescriptor(input, mediaType), diffID, 1<<20); err == nil || count != 0 {
						t.Fatalf("invalid compressed stream accepted: %d %v", count, err)
					}
				})
			}
			// DiffID includes every frame, including bytes after the tar terminator.
			joined := append(slices.Clone(encoded), encoded...)
			decoded := append(slices.Clone(archive), archive...)
			if count, err := verifyOCIImageLayerStream(t.Context(), bytes.NewReader(joined), ociLayerDescriptor(joined, mediaType), ocidigest.FromBytes(decoded), int64(len(decoded))); err != nil || count != int64(len(decoded)) {
				t.Fatalf("concatenated frame identity: %d %v", count, err)
			}
			if _, err := verifyOCIImageLayerStream(t.Context(), bytes.NewReader(joined), ociLayerDescriptor(joined, mediaType), ocidigest.FromBytes(archive), int64(len(decoded))); err == nil {
				t.Fatal("ignored bytes after first frame")
			}
		})
	}
}

func TestOCIImageLayerCancellationAndReadErrors(t *testing.T) {
	t.Parallel()
	archive := ociLayerArchive(t, "runtime fixture")
	descriptor, diffID := ociLayerDescriptor(archive, ociv1.MediaTypeImageLayer), ocidigest.FromBytes(archive)
	readFailure := errors.New("fixture read failure")
	for _, scenario := range []string{"nil-context", "canceled-before", "canceled-during", "read-failure"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			input := io.Reader(bytes.NewReader(archive))
			switch scenario {
			case "nil-context":
				ctx = nil
			case "canceled-before":
				cancel()
				input = ociLayerReaderFunc(func([]byte) (int, error) { t.Fatal("read after cancellation"); return 0, nil })
			case "canceled-during":
				input = ociLayerReaderFunc(func(output []byte) (int, error) { cancel(); return copy(output, archive), io.EOF })
			case "read-failure":
				input = ociLayerReaderFunc(func([]byte) (int, error) { return 0, readFailure })
			}
			count, err := verifyOCIImageLayerStream(ctx, input, descriptor, diffID, int64(len(archive)))
			if count != 0 || err == nil || strings.HasPrefix(scenario, "canceled-") && !errors.Is(err, context.Canceled) || scenario == "read-failure" && !errors.Is(err, readFailure) {
				t.Fatalf("invalid observation after interruption: %d %v", count, err)
			}
		})
	}
}

func TestOCIImageLayersOrderedIdentityAndSharedBudget(t *testing.T) {
	t.Parallel()
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(directory, "blobs", "sha256"), 0o700); err != nil {
		t.Fatal(err)
	}
	var layers []ociv1.Descriptor
	var diffIDs []ocidigest.Digest
	var total int64
	for i, mediaType := range []string{ociv1.MediaTypeImageLayer, ociv1.MediaTypeImageLayerGzip, ociv1.MediaTypeImageLayerZstd} {
		archive := ociLayerArchive(t, strings.Repeat("layer", i+1))
		encoded := ociLayerEncode(t, archive, mediaType)
		descriptor := ociLayerDescriptor(encoded, mediaType)
		if err := os.WriteFile(filepath.Join(directory, "blobs", "sha256", descriptor.Digest.Encoded()), encoded, 0o600); err != nil {
			t.Fatal(err)
		}
		layers, diffIDs = append(layers, descriptor), append(diffIDs, ocidigest.FromBytes(archive))
		total += int64(len(archive))
	}
	root, err := openOCILayoutRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	if err := verifyOCIImageLayers(t.Context(), root, layers, diffIDs, total); err != nil {
		t.Fatalf("mixed layers at exact shared budget: %v", err)
	}
	if err := verifyOCIImageLayers(t.Context(), root, layers, diffIDs, total-1); err == nil || !strings.Contains(err.Error(), "remaining expanded image budget") {
		t.Fatalf("per-layer sizes bypassed shared image budget: %v", err)
	}
	wrongOrder := slices.Clone(diffIDs)
	wrongOrder[0], wrongOrder[1] = wrongOrder[1], wrongOrder[0]
	if err := verifyOCIImageLayers(t.Context(), root, layers, wrongOrder, total); err == nil || !strings.Contains(err.Error(), "ordered config DiffID") {
		t.Fatalf("reordered config DiffIDs accepted: %v", err)
	}
	if err := verifyOCIImageLayers(t.Context(), root, layers, diffIDs[:1], total); err == nil {
		t.Fatal("mismatching DiffID count accepted")
	}
}

func ociLayerArchive(t *testing.T, content string) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := tar.NewWriter(&output)
	if err := writer.WriteHeader(&tar.Header{Name: "fixture.txt", Mode: 0o444, Size: int64(len(content))}); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(writer, content); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func ociLayerEncode(t *testing.T, content []byte, mediaType string) []byte {
	t.Helper()
	if mediaType == ociv1.MediaTypeImageLayer {
		return slices.Clone(content)
	}
	var output bytes.Buffer
	var writer io.WriteCloser
	if mediaType == ociv1.MediaTypeImageLayerGzip {
		writer = gzip.NewWriter(&output)
	} else {
		var err error
		writer, err = zstd.NewWriter(&output, zstd.WithEncoderConcurrency(1), zstd.WithZeroFrames(true))
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := writer.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func ociLayerDescriptor(content []byte, mediaType string) ociv1.Descriptor {
	return ociv1.Descriptor{MediaType: mediaType, Digest: ocidigest.FromBytes(content), Size: int64(len(content))}
}

type ociLayerReaderFunc func([]byte) (int, error)

func (read ociLayerReaderFunc) Read(output []byte) (int, error) { return read(output) }
