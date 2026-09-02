package artifactstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

type VersionedStore interface {
	PutIfAbsent(
		context.Context,
		string,
		string,
		io.Reader,
		int64,
		[sha256.Size]byte,
	) (ObjectVersion, error)
	ReadExactVersion(context.Context, string, string) (*ExactVersionReader, error)
	DeleteExactVersion(context.Context, string, string) error
	ResolveCurrentVersion(context.Context, string) (ObjectVersion, bool, error)
}

var (
	_ VersionedStore = (*S3)(nil)
	_ VersionedStore = (*Local)(nil)
)

type Local struct {
	mu       sync.RWMutex
	sequence uint64
	current  map[string]string
	versions map[string]map[string]localObject
}

type localObject struct {
	metadata ObjectVersion
	payload  []byte
}

func NewLocal() *Local {
	return &Local{
		current:  make(map[string]string),
		versions: make(map[string]map[string]localObject),
	}
}

func (store *Local) PutIfAbsent(
	ctx context.Context,
	objectKey string,
	contentType string,
	body io.Reader,
	sizeBytes int64,
	digest [sha256.Size]byte,
) (ObjectVersion, error) {
	if store == nil || store.current == nil || store.versions == nil {
		return ObjectVersion{}, errors.New("local Artifact Store is not configured")
	}
	if err := validateObjectKey(objectKey); err != nil {
		return ObjectVersion{}, err
	}
	contentType = strings.TrimSpace(contentType)
	if ctx == nil || contentType == "" || len(contentType) > 200 ||
		strings.ContainsRune(contentType, '\x00') || body == nil || sizeBytes <= 0 ||
		digest == [sha256.Size]byte{} {
		return ObjectVersion{}, errors.New("invalid conditional Artifact object")
	}
	if err := ctx.Err(); err != nil {
		return ObjectVersion{}, err
	}
	payload, err := io.ReadAll(io.LimitReader(body, sizeBytes+1))
	if err != nil {
		return ObjectVersion{}, fmt.Errorf("read local Artifact payload: %w", err)
	}
	if int64(len(payload)) != sizeBytes || sha256.Sum256(payload) != digest {
		return ObjectVersion{}, errors.New("local Artifact payload does not match sealed size and digest")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.current[objectKey]; exists {
		return ObjectVersion{}, ErrObjectAlreadyExists
	}
	store.sequence++
	versionID := fmt.Sprintf("local-v%020d", store.sequence)
	metadata := ObjectVersion{
		ObjectKey:      objectKey,
		VersionID:      versionID,
		ETag:           hex.EncodeToString(digest[:]),
		SizeBytes:      sizeBytes,
		ContentType:    contentType,
		ChecksumSHA256: base64.StdEncoding.EncodeToString(digest[:]),
	}
	if store.versions[objectKey] == nil {
		store.versions[objectKey] = make(map[string]localObject)
	}
	store.versions[objectKey][versionID] = localObject{
		metadata: metadata,
		payload:  bytes.Clone(payload),
	}
	store.current[objectKey] = versionID
	return metadata, nil
}

func (store *Local) ReadExactVersion(
	ctx context.Context,
	objectKey string,
	versionID string,
) (*ExactVersionReader, error) {
	if store == nil || store.versions == nil {
		return nil, errors.New("local Artifact Store is not configured")
	}
	if ctx == nil {
		return nil, errors.New("local Artifact Store context is required")
	}
	if err := validateExactVersion(objectKey, versionID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.RLock()
	object, exists := store.versions[objectKey][versionID]
	store.mu.RUnlock()
	if !exists {
		return nil, ErrObjectVersionNotFound
	}
	return &ExactVersionReader{
		ReadCloser:    io.NopCloser(bytes.NewReader(bytes.Clone(object.payload))),
		ObjectVersion: object.metadata,
	}, nil
}

func (store *Local) DeleteExactVersion(
	ctx context.Context,
	objectKey string,
	versionID string,
) error {
	if store == nil || store.versions == nil || store.current == nil {
		return errors.New("local Artifact Store is not configured")
	}
	if ctx == nil {
		return errors.New("local Artifact Store context is required")
	}
	if err := validateExactVersion(objectKey, versionID); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	versions := store.versions[objectKey]
	if versions == nil {
		return nil
	}
	delete(versions, versionID)
	if store.current[objectKey] == versionID {
		delete(store.current, objectKey)
	}
	if len(versions) == 0 {
		delete(store.versions, objectKey)
	}
	return nil
}

func (store *Local) ResolveCurrentVersion(
	ctx context.Context,
	objectKey string,
) (ObjectVersion, bool, error) {
	if store == nil || store.versions == nil || store.current == nil {
		return ObjectVersion{}, false, errors.New("local Artifact Store is not configured")
	}
	if ctx == nil {
		return ObjectVersion{}, false, errors.New("local Artifact Store context is required")
	}
	if err := validateObjectKey(objectKey); err != nil {
		return ObjectVersion{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return ObjectVersion{}, false, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	versionID, exists := store.current[objectKey]
	if !exists {
		return ObjectVersion{}, false, nil
	}
	object, exists := store.versions[objectKey][versionID]
	if !exists {
		return ObjectVersion{}, false, errors.New("local Artifact Store current version is inconsistent")
	}
	return object.metadata, true, nil
}
