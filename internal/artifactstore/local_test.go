package artifactstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"
)

func TestLocalPutIfAbsentPublishesOneImmutableExactVersion(t *testing.T) {
	store := NewLocal()
	payload := []byte("sealed encoder output")
	digest := sha256.Sum256(payload)

	version, err := store.PutIfAbsent(
		context.Background(),
		"artifacts/stage/org/project/attempt/encoder/output.bin",
		"application/octet-stream",
		bytes.NewReader(payload),
		int64(len(payload)),
		digest,
	)
	if err != nil {
		t.Fatalf("PutIfAbsent: %v", err)
	}
	if version.VersionID == "" || version.SizeBytes != int64(len(payload)) ||
		version.ChecksumSHA256 == "" {
		t.Fatalf("published version = %#v", version)
	}

	_, err = store.PutIfAbsent(
		context.Background(),
		version.ObjectKey,
		"application/octet-stream",
		bytes.NewReader(payload),
		int64(len(payload)),
		digest,
	)
	if !errors.Is(err, ErrObjectAlreadyExists) {
		t.Fatalf("second PutIfAbsent error = %v, want ErrObjectAlreadyExists", err)
	}

	reader, err := store.ReadExactVersion(
		context.Background(), version.ObjectKey, version.VersionID,
	)
	if err != nil {
		t.Fatalf("ReadExactVersion: %v", err)
	}
	defer func() { _ = reader.Close() }()
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read exact version: %v", err)
	}
	if !bytes.Equal(got, payload) || reader.VersionID != version.VersionID {
		t.Fatalf("exact version payload=%q metadata=%#v", got, reader.ObjectVersion)
	}
}

func TestLocalPutIfAbsentRejectsUnsealedPayload(t *testing.T) {
	store := NewLocal()
	payload := []byte("truncated")
	digest := sha256.Sum256([]byte("expected complete payload"))

	_, err := store.PutIfAbsent(
		context.Background(),
		"artifacts/stage/org/project/attempt/dit/output.bin",
		"application/octet-stream",
		bytes.NewReader(payload),
		int64(len(payload)),
		digest,
	)
	if err == nil {
		t.Fatal("PutIfAbsent accepted a payload with the wrong digest")
	}
	if _, _, resolveErr := store.ResolveCurrentVersion(
		context.Background(), "artifacts/stage/org/project/attempt/dit/output.bin",
	); resolveErr != nil {
		t.Fatalf("ResolveCurrentVersion after rejected publish: %v", resolveErr)
	}
}

func TestLocalDeleteExactVersionCannotDeleteAnotherVersion(t *testing.T) {
	store := NewLocal()
	payload := []byte("vae output")
	digest := sha256.Sum256(payload)
	version, err := store.PutIfAbsent(
		context.Background(),
		"artifacts/stage/org/project/attempt/vae/output.bin",
		"application/octet-stream",
		bytes.NewReader(payload),
		int64(len(payload)),
		digest,
	)
	if err != nil {
		t.Fatalf("PutIfAbsent: %v", err)
	}

	if err := store.DeleteExactVersion(
		context.Background(), version.ObjectKey, "another-version",
	); err != nil {
		t.Fatalf("DeleteExactVersion another version: %v", err)
	}
	reader, err := store.ReadExactVersion(
		context.Background(), version.ObjectKey, version.VersionID,
	)
	if err != nil {
		t.Fatalf("ReadExactVersion after unrelated delete: %v", err)
	}
	_ = reader.Close()

	if err := store.DeleteExactVersion(
		context.Background(), version.ObjectKey, version.VersionID,
	); err != nil {
		t.Fatalf("DeleteExactVersion: %v", err)
	}
	_, err = store.ReadExactVersion(context.Background(), version.ObjectKey, version.VersionID)
	if !errors.Is(err, ErrObjectVersionNotFound) {
		t.Fatalf("ReadExactVersion deleted error = %v, want ErrObjectVersionNotFound", err)
	}
}
