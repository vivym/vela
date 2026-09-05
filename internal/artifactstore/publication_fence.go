package artifactstore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
)

const publicationFenceBody = "vela-conditional-publication-fenced-v1\n"
const publicationFenceContentType = "application/vnd.vela.publication-fence"

func IsPublicationFence(object ObjectVersion) bool {
	digest := sha256.Sum256([]byte(publicationFenceBody))
	return object.VersionID != "" && object.ContentType == publicationFenceContentType &&
		object.SizeBytes == int64(len(publicationFenceBody)) &&
		object.ChecksumSHA256 == base64.StdEncoding.EncodeToString(digest[:])
}

// FenceConditionalPublication retains a non-content object at a retired key so a
// delayed conditional PUT cannot recreate Customer Content after deletion.
func FenceConditionalPublication(ctx context.Context, store VersionedStore, key string, size int64, digest [sha256.Size]byte) error {
	markerDigest := sha256.Sum256([]byte(publicationFenceBody))
	for range 3 {
		object, err := store.PutIfAbsent(ctx, key, publicationFenceContentType,
			strings.NewReader(publicationFenceBody), int64(len(publicationFenceBody)), markerDigest)
		if err == nil {
			if object.ObjectKey != key || !IsPublicationFence(object) {
				return errors.New("publication fence identity is invalid")
			}
			return nil
		}
		if !errors.Is(err, ErrObjectAlreadyExists) {
			return errors.New("publication fence conditional create did not complete")
		}
		object, exists, err := store.ResolveCurrentVersion(ctx, key)
		if err != nil {
			return errors.New("publication fence resolution did not complete")
		}
		if !exists {
			continue
		}
		if object.ObjectKey != key || object.VersionID == "" {
			return errors.New("publication fence current identity is invalid")
		}
		if IsPublicationFence(object) {
			return nil
		}
		if object.SizeBytes != size || object.ChecksumSHA256 != base64.StdEncoding.EncodeToString(digest[:]) {
			return errors.New("late publication differs from authorized content")
		}
		if err := store.DeleteExactVersion(ctx, key, object.VersionID); err != nil {
			return errors.New("late publication exact deletion did not complete")
		}
	}
	return errors.New("publication key is not yet fenced")
}
