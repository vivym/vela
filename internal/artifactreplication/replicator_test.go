package artifactreplication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"testing"

	"github.com/vivym/vela/internal/artifactstore"
)

func TestCopyReplicatesExactCommittedVersion(t *testing.T) {
	payload := []byte("immutable committed Artifact")
	digest := sha256.Sum256(payload)
	source := &recordingSourceStore{reader: exactReader(payload, "source-version")}
	backup := &recordingBackupStore{putResult: artifactstore.ObjectVersion{
		ObjectKey:      "artifacts/org/project/job/video.mp4",
		VersionID:      "backup-version",
		SizeBytes:      int64(len(payload)),
		ContentType:    "video/mp4",
		ChecksumSHA256: base64.StdEncoding.EncodeToString(digest[:]),
	}}
	replicator := &Replicator{source: source, backup: backup}

	result, code, err := replicator.copy(context.Background(), claim{
		objectKey:       "artifacts/org/project/job/video.mp4",
		objectVersionID: "source-version",
		sizeBytes:       int64(len(payload)),
		sha256:          digest,
		contentType:     "video/mp4",
	})
	if err != nil || code != "" || result.VersionID != "backup-version" {
		t.Fatalf("copy result = %#v, %q, %v", result, code, err)
	}
	if source.key != "artifacts/org/project/job/video.mp4" ||
		source.versionID != "source-version" {
		t.Fatalf("source identity = %q@%q", source.key, source.versionID)
	}
	if backup.putCalls != 1 || backup.headCalls != 0 ||
		!bytes.Equal(backup.body, payload) || backup.digest != digest {
		t.Fatalf("backup calls = %#v", backup)
	}
}

func TestCopyRecoversMatchingBackupAfterConditionalCreateResponseLoss(t *testing.T) {
	payload := []byte("already copied Artifact")
	digest := sha256.Sum256(payload)
	matching := artifactstore.ObjectVersion{
		ObjectKey:      "artifacts/org/project/job/video.mp4",
		VersionID:      "existing-backup-version",
		SizeBytes:      int64(len(payload)),
		ContentType:    "video/mp4",
		ChecksumSHA256: base64.StdEncoding.EncodeToString(digest[:]),
	}
	replicator := &Replicator{
		source: &recordingSourceStore{reader: exactReader(payload, "source-version")},
		backup: &recordingBackupStore{
			putErr: artifactstore.ErrObjectAlreadyExists,
			head:   matching,
		},
	}

	result, code, err := replicator.copy(context.Background(), claim{
		objectKey:       matching.ObjectKey,
		objectVersionID: "source-version",
		sizeBytes:       int64(len(payload)),
		sha256:          digest,
		contentType:     matching.ContentType,
	})
	if err != nil || code != "" || result != matching {
		t.Fatalf("recovered copy = %#v, %q, %v", result, code, err)
	}
}

func TestCopyRejectsConflictingBackupObject(t *testing.T) {
	payload := []byte("committed Artifact")
	digest := sha256.Sum256(payload)
	replicator := &Replicator{
		source: &recordingSourceStore{reader: exactReader(payload, "source-version")},
		backup: &recordingBackupStore{
			putErr: artifactstore.ErrObjectAlreadyExists,
			head: artifactstore.ObjectVersion{
				ObjectKey:      "artifacts/org/project/job/video.mp4",
				VersionID:      "conflicting-version",
				SizeBytes:      int64(len(payload)),
				ContentType:    "video/mp4",
				ChecksumSHA256: base64.StdEncoding.EncodeToString(sha256.New().Sum(nil)),
			},
		},
	}

	_, code, err := replicator.copy(context.Background(), claim{
		objectKey:       "artifacts/org/project/job/video.mp4",
		objectVersionID: "source-version",
		sizeBytes:       int64(len(payload)),
		sha256:          digest,
		contentType:     "video/mp4",
	})
	if err == nil || code != "BACKUP_CONFLICT" {
		t.Fatalf("conflicting copy = code %q error %v", code, err)
	}
}

func TestCopyRejectsSourceMetadataMismatchBeforeWriting(t *testing.T) {
	payload := []byte("committed Artifact")
	digest := sha256.Sum256(payload)
	reader := exactReader(payload, "source-version")
	reader.SizeBytes++
	backup := &recordingBackupStore{}
	replicator := &Replicator{
		source: &recordingSourceStore{reader: reader},
		backup: backup,
	}

	_, code, err := replicator.copy(context.Background(), claim{
		objectKey:       reader.ObjectKey,
		objectVersionID: reader.VersionID,
		sizeBytes:       int64(len(payload)),
		sha256:          digest,
		contentType:     reader.ContentType,
	})
	if err == nil || code != "SOURCE_IDENTITY_MISMATCH" || backup.putCalls != 0 {
		t.Fatalf("mismatched source = code %q error %v put calls %d", code, err, backup.putCalls)
	}
}

func TestCopyPreservesStorageErrors(t *testing.T) {
	payload := []byte("committed Artifact")
	digest := sha256.Sum256(payload)
	claimed := claim{
		objectKey:       "artifacts/org/project/job/video.mp4",
		objectVersionID: "source-version",
		sizeBytes:       int64(len(payload)),
		sha256:          digest,
		contentType:     "video/mp4",
	}
	for _, test := range []struct {
		name   string
		source SourceStore
		backup BackupStore
	}{
		{
			name:   "source read",
			source: &recordingSourceStore{err: context.DeadlineExceeded},
			backup: &recordingBackupStore{},
		},
		{
			name:   "backup write",
			source: &recordingSourceStore{reader: exactReader(payload, "source-version")},
			backup: &recordingBackupStore{putErr: context.DeadlineExceeded},
		},
		{
			name:   "backup response-loss head",
			source: &recordingSourceStore{reader: exactReader(payload, "source-version")},
			backup: &recordingBackupStore{
				putErr:  artifactstore.ErrObjectAlreadyExists,
				headErr: context.DeadlineExceeded,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			replicator := &Replicator{source: test.source, backup: test.backup}
			_, code, err := replicator.copy(context.Background(), claimed)
			if code != "STORAGE_OPERATION_FAILED" || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("copy storage failure = code %q error %v", code, err)
			}
		})
	}
}

type recordingSourceStore struct {
	reader    *artifactstore.ExactVersionReader
	err       error
	key       string
	versionID string
}

func (store *recordingSourceStore) ReadExactVersion(
	_ context.Context,
	key string,
	versionID string,
) (*artifactstore.ExactVersionReader, error) {
	store.key = key
	store.versionID = versionID
	if store.err != nil {
		return nil, store.err
	}
	return store.reader, nil
}

type recordingBackupStore struct {
	putResult artifactstore.ObjectVersion
	putErr    error
	head      artifactstore.ObjectVersion
	headErr   error
	putCalls  int
	headCalls int
	body      []byte
	digest    [sha256.Size]byte
}

func (store *recordingBackupStore) PutIfAbsent(
	_ context.Context,
	_ string,
	_ string,
	body io.Reader,
	_ int64,
	digest [sha256.Size]byte,
) (artifactstore.ObjectVersion, error) {
	store.putCalls++
	store.digest = digest
	store.body, _ = io.ReadAll(body)
	return store.putResult, store.putErr
}

func (store *recordingBackupStore) HeadCurrentVersion(
	context.Context,
	string,
) (artifactstore.ObjectVersion, error) {
	store.headCalls++
	return store.head, store.headErr
}

func exactReader(payload []byte, versionID string) *artifactstore.ExactVersionReader {
	return &artifactstore.ExactVersionReader{
		ReadCloser: io.NopCloser(bytes.NewReader(payload)),
		ObjectVersion: artifactstore.ObjectVersion{
			ObjectKey:   "artifacts/org/project/job/video.mp4",
			VersionID:   versionID,
			SizeBytes:   int64(len(payload)),
			ContentType: "video/mp4",
		},
	}
}
