package stageartifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/artifactstore"
)

func TestMaterializerRetriesL2WithoutReissuingComputeAuthority(t *testing.T) {
	now := time.Date(2026, time.August, 30, 10, 0, 0, 0, time.UTC)
	payload := []byte("durable encoder tensor")
	digest := sha256.Sum256(payload)
	lease := testMaterializationLease(now, digest, int64(len(payload)))
	store := &failingVersionedStore{
		VersionedStore: artifactstore.NewLocal(),
		failPuts:       1,
	}
	committer := &recordingCommitter{}
	materializer, err := NewMaterializer(store, committer, func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	_, err = materializer.Materialize(context.Background(), lease, bytes.NewReader(payload))
	if err == nil || committer.calls != 0 {
		t.Fatalf("first Materialize error=%v commit calls=%d", err, committer.calls)
	}
	artifact, err := materializer.Materialize(
		context.Background(), lease, bytes.NewReader(payload),
	)
	if err != nil {
		t.Fatalf("retry Materialize: %v", err)
	}
	if committer.calls != 1 || artifact.ID != lease.ArtifactID ||
		artifact.ObjectVersion == "" || artifact.SHA256 != digest {
		t.Fatalf("retry artifact=%#v commit calls=%d", artifact, committer.calls)
	}
	if committer.last.MaterializationLeaseID != lease.ID ||
		committer.last.ArtifactID != lease.ArtifactID {
		t.Fatalf("commit command = %#v", committer.last)
	}
}

func TestMaterializerReconcilesUploadBeforeDatabaseCommit(t *testing.T) {
	now := time.Date(2026, time.August, 30, 11, 0, 0, 0, time.UTC)
	payload := []byte("sealed dit latent")
	digest := sha256.Sum256(payload)
	lease := testMaterializationLease(now, digest, int64(len(payload)))
	store := artifactstore.NewLocal()
	committer := &recordingCommitter{failCommits: 1}
	materializer, err := NewMaterializer(store, committer, func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	_, err = materializer.Materialize(context.Background(), lease, bytes.NewReader(payload))
	if err == nil {
		t.Fatal("first Materialize succeeded despite injected database outage")
	}
	artifact, err := materializer.Materialize(
		context.Background(), lease, bytes.NewReader(payload),
	)
	if err != nil {
		t.Fatalf("reconcile Materialize: %v", err)
	}
	if committer.calls != 2 || artifact.ObjectVersion == "" {
		t.Fatalf("reconciled artifact=%#v commit calls=%d", artifact, committer.calls)
	}
}

func TestMaterializerFailsClosedOnConditionalPublicationConflict(t *testing.T) {
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	store := artifactstore.NewLocal()
	existing := []byte("another producer")
	existingDigest := sha256.Sum256(existing)
	lease := testMaterializationLease(now, sha256.Sum256([]byte("winner")), int64(len("winner")))
	if _, err := store.PutIfAbsent(
		context.Background(), lease.ObjectKey, lease.ContentType,
		bytes.NewReader(existing), int64(len(existing)), existingDigest,
	); err != nil {
		t.Fatalf("seed conflicting object: %v", err)
	}
	committer := &recordingCommitter{}
	materializer, err := NewMaterializer(store, committer, func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	_, err = materializer.Materialize(
		context.Background(), lease, bytes.NewReader([]byte("winner")),
	)
	if !errors.Is(err, ErrConditionalPublicationConflict) || committer.calls != 0 {
		t.Fatalf("Materialize conflict error=%v commit calls=%d", err, committer.calls)
	}
}

func TestMaterializerRejectsExpiredLeaseBeforeReadingSource(t *testing.T) {
	now := time.Date(2026, time.August, 30, 13, 0, 0, 0, time.UTC)
	payload := []byte("vae frames")
	digest := sha256.Sum256(payload)
	lease := testMaterializationLease(now.Add(-time.Hour), digest, int64(len(payload)))
	lease.ExpiresAt = now.Add(-time.Second)
	materializer, err := NewMaterializer(
		artifactstore.NewLocal(), &recordingCommitter{}, func() time.Time { return now },
	)
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	_, err = materializer.Materialize(context.Background(), lease, bytes.NewReader(payload))
	if !errors.Is(err, ErrMaterializationLeaseExpired) {
		t.Fatalf("Materialize expired error=%v", err)
	}
}

func testMaterializationLease(
	now time.Time,
	digest [sha256.Size]byte,
	size int64,
) MaterializationLease {
	return MaterializationLease{
		ID:          uuid.MustParse("49600000-0000-0000-0000-000000000001"),
		ArtifactID:  uuid.MustParse("49600000-0000-0000-0000-000000000002"),
		ObjectKey:   "artifacts/stage/org/project/attempt/stage/output.bin",
		ContentType: "application/octet-stream",
		SHA256:      digest,
		TokenDigest: sha256.Sum256([]byte("signed materialization authority")),
		SizeBytes:   size,
		IssuedAt:    now.Add(-time.Minute),
		ExpiresAt:   now.Add(time.Hour),
	}
}

type recordingCommitter struct {
	calls       int
	failCommits int
	last        CommitCommand
}

func (committer *recordingCommitter) Commit(
	_ context.Context,
	command CommitCommand,
) (Artifact, error) {
	committer.calls++
	committer.last = command
	if committer.failCommits > 0 {
		committer.failCommits--
		return Artifact{}, errors.New("injected database outage")
	}
	return Artifact{
		ID:            command.ArtifactID,
		ObjectKey:     command.ObjectKey,
		ObjectVersion: command.ObjectVersion,
		SHA256:        command.SHA256,
		SizeBytes:     command.SizeBytes,
		CommittedAt:   command.CommittedAt,
	}, nil
}

type failingVersionedStore struct {
	artifactstore.VersionedStore
	failPuts int
}

func (store *failingVersionedStore) PutIfAbsent(
	ctx context.Context,
	objectKey string,
	contentType string,
	body io.Reader,
	sizeBytes int64,
	digest [sha256.Size]byte,
) (artifactstore.ObjectVersion, error) {
	if store.failPuts > 0 {
		store.failPuts--
		return artifactstore.ObjectVersion{}, errors.New("injected L2 outage")
	}
	return store.VersionedStore.PutIfAbsent(
		ctx, objectKey, contentType, body, sizeBytes, digest,
	)
}
