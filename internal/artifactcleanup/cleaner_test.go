package artifactcleanup

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/vivym/vela/internal/artifactstore"
)

func TestCleanerAbortsOnlyOldUnrecordedMultipartUploads(t *testing.T) {
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	store := &recordingMultipartStore{uploads: []artifactstore.IncompleteMultipartUpload{
		{ObjectKey: "artifacts/org/project/job/attempt/old-orphan/video.mp4", UploadID: "old-orphan", InitiatedAt: now.Add(-20 * time.Minute)},
		{ObjectKey: "artifacts/org/project/job/attempt/recorded/video.mp4", UploadID: "recorded", InitiatedAt: now.Add(-15 * time.Minute)},
		{ObjectKey: "artifacts/org/project/job/attempt/young-orphan/video.mp4", UploadID: "young-orphan", InitiatedAt: now.Add(-time.Minute)},
	}}
	registry := &recordingRegistry{recorded: map[string]bool{
		"artifacts/org/project/job/attempt/recorded/video.mp4\x00recorded": true,
	}}
	cleaner, err := New(store, registry, Config{
		ObjectPrefix: "artifacts/",
		MinimumAge:   10 * time.Minute,
		MaxAborts:    10,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cleaner.now = func() time.Time { return now }

	result, err := cleaner.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.Listed != 3 || result.Eligible != 2 || result.Recorded != 1 || result.Aborted != 1 {
		t.Fatalf("cleanup result = %#v", result)
	}
	wantAborted := []artifactstore.MultipartUpload{{
		ObjectKey: "artifacts/org/project/job/attempt/old-orphan/video.mp4",
		UploadID:  "old-orphan",
	}}
	if !reflect.DeepEqual(store.aborted, wantAborted) {
		t.Fatalf("aborted uploads = %#v, want %#v", store.aborted, wantAborted)
	}
	if len(registry.lookups) != 2 {
		t.Fatalf("registry lookups = %#v, want only old sessions", registry.lookups)
	}
}

func TestCleanerFailsClosedBeforeAbortWhenRegistryLookupFails(t *testing.T) {
	now := time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC)
	store := &recordingMultipartStore{uploads: []artifactstore.IncompleteMultipartUpload{{
		ObjectKey:   "artifacts/org/project/job/attempt/unknown/video.mp4",
		UploadID:    "unknown",
		InitiatedAt: now.Add(-time.Hour),
	}}}
	registry := &recordingRegistry{err: errors.New("database unavailable")}
	cleaner, err := New(store, registry, Config{
		ObjectPrefix: "artifacts/",
		MinimumAge:   10 * time.Minute,
		MaxAborts:    10,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cleaner.now = func() time.Time { return now }

	_, err = cleaner.Reconcile(context.Background())
	if err == nil || !errors.Is(err, registry.err) {
		t.Fatalf("Reconcile error = %v, want registry failure", err)
	}
	if len(store.aborted) != 0 {
		t.Fatalf("cleanup aborted after uncertain registry state: %#v", store.aborted)
	}
}

type recordingMultipartStore struct {
	uploads []artifactstore.IncompleteMultipartUpload
	aborted []artifactstore.MultipartUpload
}

func (store *recordingMultipartStore) ListIncompleteMultipartUploads(
	context.Context,
	string,
) ([]artifactstore.IncompleteMultipartUpload, error) {
	return append([]artifactstore.IncompleteMultipartUpload(nil), store.uploads...), nil
}

func (store *recordingMultipartStore) AbortMultipartUpload(
	_ context.Context,
	upload artifactstore.MultipartUpload,
) error {
	store.aborted = append(store.aborted, upload)
	return nil
}

type recordingRegistry struct {
	recorded map[string]bool
	lookups  []string
	err      error
}

func (registry *recordingRegistry) IsMultipartUploadRecorded(
	_ context.Context,
	objectKey string,
	uploadID string,
) (bool, error) {
	lookup := objectKey + "\x00" + uploadID
	registry.lookups = append(registry.lookups, lookup)
	if registry.err != nil {
		return false, registry.err
	}
	return registry.recorded[lookup], nil
}
