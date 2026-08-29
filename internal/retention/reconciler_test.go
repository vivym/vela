package retention

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/artifactstore"
)

func TestNewReconcilerRequiresBackupPoolAndStoreTogether(t *testing.T) {
	primaryPool := &pgxpool.Pool{}
	backupPool := &pgxpool.Pool{}
	primaryStore := noOpDeletionStore{}
	backupStore := noOpBackupDeletionStore{}
	config := ReconcilerConfig{
		InstanceID: "backup-pairing-test",
		BatchSize:  1,
		ClaimTTL:   time.Minute,
		RetryDelay: time.Minute,
	}

	config.BackupPool = backupPool
	if _, err := NewReconciler(primaryPool, primaryStore, config); err == nil {
		t.Fatal("NewReconciler accepted a backup pool without a backup store")
	}
	config.BackupPool = nil
	config.BackupStore = backupStore
	if _, err := NewReconciler(primaryPool, primaryStore, config); err == nil {
		t.Fatal("NewReconciler accepted a backup store without a backup pool")
	}
	config.BackupPool = backupPool
	if _, err := NewReconciler(primaryPool, primaryStore, config); err != nil {
		t.Fatalf("NewReconciler rejected paired backup dependencies: %v", err)
	}
}

type noOpDeletionStore struct{}

func (noOpDeletionStore) DeleteExactVersion(
	context.Context,
	string,
	string,
) error {
	return nil
}

func (noOpDeletionStore) ResolveCurrentVersion(
	context.Context,
	string,
) (artifactstore.ObjectVersion, bool, error) {
	return artifactstore.ObjectVersion{}, false, nil
}

func (noOpDeletionStore) ListIncompleteMultipartUploads(
	context.Context,
	string,
) ([]artifactstore.IncompleteMultipartUpload, error) {
	return nil, nil
}

func (noOpDeletionStore) AbortMultipartUpload(
	context.Context,
	artifactstore.MultipartUpload,
) error {
	return nil
}

type noOpBackupDeletionStore struct{}

func (noOpBackupDeletionStore) PurgeObjectVersions(
	context.Context,
	string,
) (artifactstore.ObjectVersionsPurgeResult, error) {
	return artifactstore.ObjectVersionsPurgeResult{}, nil
}
