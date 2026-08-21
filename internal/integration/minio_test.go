//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/vivym/vela/internal/artifactstore"
)

const (
	minIOAccessKey = "vela-minio-integration"
	minIOSecretKey = "vela-minio-integration-secret"
	minIORegion    = "us-east-1"
)

type minIOFixture struct {
	admin    *s3.Client
	store    *artifactstore.S3
	config   artifactstore.S3Config
	endpoint string
	bucket   string
}

func newMinIOFixture(t *testing.T, bucket string) *minIOFixture {
	t.Helper()
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "minio/minio:RELEASE.2025-04-22T22-12-26Z",
			ExposedPorts: []string{"9000/tcp"},
			Env: map[string]string{
				"MINIO_ROOT_USER":     minIOAccessKey,
				"MINIO_ROOT_PASSWORD": minIOSecretKey,
			},
			Cmd: []string{"server", "/data", "--address", ":9000"},
			WaitingFor: wait.ForHTTP("/minio/health/ready").
				WithPort("9000/tcp").
				WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start MinIO: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Errorf("terminate MinIO: %v", err)
		}
	})
	endpoint, err := container.Endpoint(ctx, "http")
	if err != nil {
		t.Fatalf("resolve MinIO endpoint: %v", err)
	}
	admin := newMinIOS3Client(endpoint, minIORegion, minIOAccessKey, minIOSecretKey)
	if _, err := admin.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatalf("create private Artifact bucket: %v", err)
	}
	config := artifactstore.S3Config{
		Endpoint:        endpoint,
		Region:          minIORegion,
		Bucket:          bucket,
		AccessKeyID:     minIOAccessKey,
		SecretAccessKey: minIOSecretKey,
		UsePathStyle:    true,
		SignedGETTTL:    15 * time.Minute,
	}
	store, err := artifactstore.NewS3(config)
	if err != nil {
		t.Fatalf("create S3 Artifact Store: %v", err)
	}
	return &minIOFixture{
		admin: admin, store: store, config: config, endpoint: endpoint, bucket: bucket,
	}
}

func (fixture *minIOFixture) enableVersioning(t *testing.T) {
	t.Helper()
	if _, err := fixture.admin.PutBucketVersioning(context.Background(), &s3.PutBucketVersioningInput{
		Bucket: aws.String(fixture.bucket),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	}); err != nil {
		t.Fatalf("enable Artifact bucket versioning: %v", err)
	}
}
