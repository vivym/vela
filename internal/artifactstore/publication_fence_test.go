package artifactstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"sync"
	"testing"
)

func TestPublicationFenceClosesDelayedConditionalPut(t *testing.T) {
	for _, mode := range []string{"marker wins", "late content wins", "marker response loss"} {
		t.Run(mode, func(t *testing.T) {
			store := &delayedConditionalStore{Local: NewLocal(), started: make(chan struct{}), release: make(chan struct{}),
				finished: make(chan struct{}), releaseBeforeFence: mode == "late content wins", loseFenceResponse: mode == "marker response loss"}
			payload := []byte("Customer Content held by a remote PUT")
			digest := sha256.Sum256(payload)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				_, err := store.PutIfAbsent(ctx, "artifacts/deferred.bin", "application/octet-stream", bytes.NewReader(payload), int64(len(payload)), digest)
				done <- err
			}()
			<-store.started
			cancel()
			err := FenceConditionalPublication(context.Background(), store, "artifacts/deferred.bin", int64(len(payload)), digest)
			if mode == "marker response loss" {
				if err == nil {
					t.Fatal("missing simulated marker response loss")
				}
				err = FenceConditionalPublication(context.Background(), store, "artifacts/deferred.bin", int64(len(payload)), digest)
			}
			if err != nil {
				t.Fatal(err)
			}
			store.releaseOnce.Do(func() { close(store.release) })
			err = <-done
			if mode == "late content wins" {
				if err != nil {
					t.Fatalf("late PUT did not precede fence: %v", err)
				}
			} else if !errors.Is(err, ErrObjectAlreadyExists) {
				t.Fatalf("delayed PUT bypassed fence: %v", err)
			}
			marker, exists, err := store.ResolveCurrentVersion(context.Background(), "artifacts/deferred.bin")
			if err != nil || !exists || !IsPublicationFence(marker) {
				t.Fatalf("fence=%+v exists=%t err=%v", marker, exists, err)
			}
			if store.lateVersion != "" {
				if _, err := store.ReadExactVersion(context.Background(), "artifacts/deferred.bin", store.lateVersion); !errors.Is(err, ErrObjectVersionNotFound) {
					t.Fatalf("late exact content remains: %v", err)
				}
			}
			if err := FenceConditionalPublication(context.Background(), store, "artifacts/deferred.bin", int64(len(payload)), digest); err != nil {
				t.Fatal(err)
			}
			after, _, _ := store.ResolveCurrentVersion(context.Background(), "artifacts/deferred.bin")
			if after.VersionID != marker.VersionID {
				t.Fatal("cleanup replay replaced permanent fence")
			}
			if err := store.DeleteExactVersion(context.Background(), marker.ObjectKey, marker.VersionID); err != nil {
				t.Fatal(err)
			}
			after, _, _ = store.ResolveCurrentVersion(context.Background(), marker.ObjectKey)
			if after.VersionID != marker.VersionID {
				t.Fatal("exact-version cleanup removed permanent fence")
			}
		})
	}
}

type delayedConditionalStore struct {
	*Local
	started, release, finished            chan struct{}
	releaseOnce                           sync.Once
	releaseBeforeFence, loseFenceResponse bool
	lateVersion                           string
}

func (store *delayedConditionalStore) PutIfAbsent(ctx context.Context, key, contentType string, reader io.Reader, size int64, digest [sha256.Size]byte) (ObjectVersion, error) {
	if contentType != publicationFenceContentType {
		payload, err := io.ReadAll(reader)
		if err != nil {
			return ObjectVersion{}, err
		}
		close(store.started)
		<-store.release
		object, err := store.Local.PutIfAbsent(context.Background(), key, contentType, bytes.NewReader(payload), size, digest)
		store.lateVersion = object.VersionID
		close(store.finished)
		return object, err
	}
	if store.releaseBeforeFence {
		store.releaseOnce.Do(func() { close(store.release) })
		<-store.finished
	}
	object, err := store.Local.PutIfAbsent(ctx, key, contentType, reader, size, digest)
	if err == nil && store.loseFenceResponse {
		store.loseFenceResponse = false
		return ObjectVersion{}, errors.New("lost fence response")
	}
	return object, err
}
