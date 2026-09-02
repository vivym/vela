package stageartifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

const maxLocalOutputManifestBytes = 1 << 20

var ErrLocalOutputSourceLost = errors.New("local StageArtifact source lost")

type LocalOutputLineageV1 struct {
	AttemptID              uuid.UUID `json:"attempt_id"`
	StageRunID             uuid.UUID `json:"stage_run_id"`
	StageAttemptID         uuid.UUID `json:"stage_attempt_id"`
	StageLeaseID           uuid.UUID `json:"stage_lease_id"`
	AttemptFence           int64     `json:"attempt_fence"`
	StageFence             int64     `json:"stage_fence"`
	StageProfileRevisionID uuid.UUID `json:"stage_profile_revision_id"`
}

type LocalOutputManifestV1 struct {
	SchemaVersion int                  `json:"schema_version"`
	OutputPort    string               `json:"output_port"`
	LocalLocator  string               `json:"local_locator"`
	ContentType   string               `json:"content_type"`
	PayloadSHA256 [sha256.Size]byte    `json:"-"`
	SizeBytes     int64                `json:"size_bytes"`
	Lineage       LocalOutputLineageV1 `json:"lineage"`
}

type localOutputManifestDocumentV1 struct {
	SchemaVersion int                  `json:"schema_version"`
	OutputPort    string               `json:"output_port"`
	LocalLocator  string               `json:"local_locator"`
	ContentType   string               `json:"content_type"`
	PayloadSHA256 string               `json:"payload_sha256"`
	SizeBytes     int64                `json:"size_bytes"`
	Lineage       LocalOutputLineageV1 `json:"lineage"`
}

func ParseLocalOutputManifestV1(document []byte) (LocalOutputManifestV1, error) {
	if len(document) == 0 || len(document) > maxLocalOutputManifestBytes || !utf8.Valid(document) {
		return LocalOutputManifestV1{}, errors.New("LocalOutputManifestV1 document is invalid")
	}
	decoder := json.NewDecoder(strings.NewReader(string(document)))
	decoder.DisallowUnknownFields()
	var decoded localOutputManifestDocumentV1
	if err := decoder.Decode(&decoded); err != nil {
		return LocalOutputManifestV1{}, fmt.Errorf("decode LocalOutputManifestV1: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return LocalOutputManifestV1{}, err
	}
	digest, err := decodeCanonicalSHA256(decoded.PayloadSHA256)
	if err != nil {
		return LocalOutputManifestV1{}, err
	}
	manifest := LocalOutputManifestV1{
		SchemaVersion: decoded.SchemaVersion,
		OutputPort:    decoded.OutputPort,
		LocalLocator:  decoded.LocalLocator,
		ContentType:   decoded.ContentType,
		PayloadSHA256: digest,
		SizeBytes:     decoded.SizeBytes,
		Lineage:       decoded.Lineage,
	}
	if err := validateLocalOutputManifestV1(manifest); err != nil {
		return LocalOutputManifestV1{}, err
	}
	return manifest, nil
}

func (manifest LocalOutputManifestV1) LineageDigest() ([sha256.Size]byte, error) {
	if err := validateLocalOutputManifestV1(manifest); err != nil {
		return [sha256.Size]byte{}, err
	}
	document, err := json.Marshal(manifest.Lineage)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("encode LocalOutputManifestV1 lineage: %w", err)
	}
	return sha256.Sum256(document), nil
}

func validateLocalOutputManifestV1(manifest LocalOutputManifestV1) error {
	if manifest.SchemaVersion != 1 || !validOutputPort(manifest.OutputPort) ||
		!validLocalLocator(manifest.LocalLocator) || manifest.SizeBytes <= 0 ||
		manifest.PayloadSHA256 == [sha256.Size]byte{} {
		return errors.New("LocalOutputManifestV1 output contract is invalid")
	}
	contentType := strings.TrimSpace(manifest.ContentType)
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil || contentType != manifest.ContentType || mediaType == "" || len(contentType) > 200 {
		return errors.New("LocalOutputManifestV1 content type is invalid")
	}
	lineage := manifest.Lineage
	if lineage.AttemptID == uuid.Nil || lineage.StageRunID == uuid.Nil ||
		lineage.StageAttemptID == uuid.Nil || lineage.StageLeaseID == uuid.Nil ||
		lineage.StageProfileRevisionID == uuid.Nil || lineage.AttemptFence <= 0 ||
		lineage.StageFence <= 0 {
		return errors.New("LocalOutputManifestV1 lineage is incomplete")
	}
	return nil
}

func validOutputPort(outputPort string) bool {
	if len(outputPort) == 0 || len(outputPort) > 100 {
		return false
	}
	for index, value := range []byte(outputPort) {
		if (value >= 'a' && value <= 'z') || (index > 0 && value >= '0' && value <= '9') ||
			(index > 0 && value == '_') {
			continue
		}
		return false
	}
	return true
}

func validLocalLocator(locator string) bool {
	if len(locator) == 0 || len(locator) > 1024 || strings.ContainsRune(locator, '\x00') ||
		strings.ContainsRune(locator, '\\') || strings.HasPrefix(locator, "/") ||
		path.Clean(locator) != locator || locator == "." ||
		strings.HasPrefix(locator, "../") {
		return false
	}
	return true
}

func decodeCanonicalSHA256(value string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if len(value) != hex.EncodedLen(sha256.Size) || strings.ToLower(value) != value {
		return digest, errors.New("LocalOutputManifestV1 payload digest is invalid")
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return digest, errors.New("LocalOutputManifestV1 payload digest is invalid")
	}
	copy(digest[:], decoded)
	if digest == [sha256.Size]byte{} {
		return digest, errors.New("LocalOutputManifestV1 payload digest is invalid")
	}
	return digest, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("LocalOutputManifestV1 contains trailing data")
	}
	return nil
}

type LocalOutputSource interface {
	Open(context.Context, LocalOutputManifestV1) (io.ReadCloser, error)
}

type FilesystemLocalOutputSource struct {
	root string
}

func NewFilesystemLocalOutputSource(root string) (*FilesystemLocalOutputSource, error) {
	root = strings.TrimSpace(root)
	if root == "" || !filepath.IsAbs(root) {
		return nil, errors.New("local output root must be an absolute path")
	}
	resolved, err := filepath.EvalSymlinks(filepath.Clean(root))
	if err != nil {
		return nil, fmt.Errorf("resolve local output root: %w", err)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return nil, errors.New("local output root is not a directory")
	}
	return &FilesystemLocalOutputSource{root: resolved}, nil
}

func (source *FilesystemLocalOutputSource) Open(
	ctx context.Context,
	manifest LocalOutputManifestV1,
) (io.ReadCloser, error) {
	if source == nil || source.root == "" || ctx == nil {
		return nil, errors.New("FilesystemLocalOutputSource is not configured")
	}
	if err := validateLocalOutputManifestV1(manifest); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	candidate := filepath.Join(source.root, filepath.FromSlash(manifest.LocalLocator))
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLocalOutputSourceLost, err)
	}
	relative, err := filepath.Rel(source.root, resolved)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, errors.New("local output locator escapes configured root")
	}
	file, err := os.Open(resolved)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLocalOutputSourceLost, err)
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != manifest.SizeBytes {
		_ = file.Close()
		return nil, ErrLocalOutputSourceLost
	}
	return file, nil
}
