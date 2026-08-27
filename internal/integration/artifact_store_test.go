//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/vivym/vela/internal/artifactstore"
)

func TestS3ArtifactStoreResumesMultipartAndReadsExactPrivateVersion(t *testing.T) {
	ctx := context.Background()
	minio := newMinIOFixture(t, "vela-artifacts")
	admin := minio.admin
	store := minio.store
	config := minio.config
	endpoint := minio.endpoint
	bucket := minio.bucket
	if err := store.ValidateBucket(ctx); !errors.Is(err, artifactstore.ErrBucketVersioningRequired) {
		t.Fatalf("unversioned Artifact bucket validation error = %v", err)
	}
	minio.enableVersioning(t)
	publicPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::vela-artifacts/*"]}]}`
	if _, err := admin.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: aws.String(bucket),
		Policy: aws.String(publicPolicy),
	}); err != nil {
		t.Fatalf("make Artifact bucket public for negative preflight: %v", err)
	}
	if err := store.ValidateBucket(ctx); !errors.Is(err, artifactstore.ErrBucketNotPrivate) {
		t.Fatalf("public Artifact bucket validation error = %v", err)
	}
	if _, err := admin.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
		Bucket: aws.String(bucket),
	}); err != nil {
		t.Fatalf("remove public Artifact bucket policy: %v", err)
	}
	if err := store.ValidateBucket(ctx); err != nil {
		t.Fatalf("validate private versioned Artifact bucket: %v", err)
	}
	const objectKey = "artifacts/organization/project/job/attempt/artifact/video.mp4"
	session, err := store.CreateMultipartUpload(ctx, objectKey, "video/mp4")
	if err != nil || session.UploadID == "" || session.ObjectKey != objectKey {
		t.Fatalf("create multipart upload = %#v error=%v", session, err)
	}

	firstBytes := bytes.Repeat([]byte("a"), 5*1024*1024)
	firstDigest := sha256.Sum256(firstBytes)
	firstPart, err := store.UploadPart(
		ctx,
		session,
		1,
		bytes.NewReader(firstBytes),
		int64(len(firstBytes)),
		firstDigest,
	)
	if err != nil || firstPart.Number != 1 || firstPart.ETag == "" ||
		firstPart.SizeBytes != int64(len(firstBytes)) {
		t.Fatalf("upload first part = %#v error=%v", firstPart, err)
	}

	restarted, err := artifactstore.NewS3(config)
	if err != nil {
		t.Fatalf("restart S3 Artifact Store: %v", err)
	}
	resumedParts, err := restarted.ListParts(ctx, session)
	if err != nil || len(resumedParts) != 1 || resumedParts[0] != firstPart {
		t.Fatalf("resumed multipart parts = %#v error=%v, want %#v", resumedParts, err, firstPart)
	}
	orphan, err := restarted.CreateMultipartUpload(
		ctx,
		"artifacts/organization/project/job/attempt/orphan/video.mp4",
		"video/mp4",
	)
	if err != nil {
		t.Fatalf("create orphan multipart upload: %v", err)
	}
	const multipartPrefix = "artifacts/organization/project/job/attempt/"
	incomplete := waitForIncompleteMultipartUploads(
		t,
		ctx,
		restarted,
		multipartPrefix,
		func(uploads []artifactstore.IncompleteMultipartUpload) bool {
			return containsMultipartUpload(uploads, orphan) && containsMultipartUpload(uploads, session)
		},
	)
	if !containsMultipartUpload(incomplete, orphan) || !containsMultipartUpload(incomplete, session) {
		t.Fatalf("incomplete multipart uploads = %#v", incomplete)
	}
	if err := restarted.AbortMultipartUpload(ctx, orphan); err != nil {
		t.Fatalf("abort orphan multipart upload: %v", err)
	}
	if err := restarted.AbortMultipartUpload(ctx, orphan); err != nil {
		t.Fatalf("idempotently abort orphan multipart upload: %v", err)
	}
	incomplete = waitForIncompleteMultipartUploads(
		t,
		ctx,
		restarted,
		multipartPrefix,
		func(uploads []artifactstore.IncompleteMultipartUpload) bool {
			return !containsMultipartUpload(uploads, orphan) && containsMultipartUpload(uploads, session)
		},
	)
	if containsMultipartUpload(incomplete, orphan) || !containsMultipartUpload(incomplete, session) {
		t.Fatalf("incomplete multipart uploads after orphan abort = %#v", incomplete)
	}

	lastBytes := []byte("durable-final-part")
	lastDigest := sha256.Sum256(lastBytes)
	lastPart, err := restarted.UploadPart(
		ctx,
		session,
		2,
		bytes.NewReader(lastBytes),
		int64(len(lastBytes)),
		lastDigest,
	)
	if err != nil {
		t.Fatalf("upload final part: %v", err)
	}
	competingSession, err := restarted.CreateMultipartUpload(ctx, objectKey, "video/mp4")
	if err != nil {
		t.Fatalf("create competing multipart upload: %v", err)
	}
	competingBytes := []byte("competing-object-version")
	competingDigest := sha256.Sum256(competingBytes)
	competingPart, err := restarted.UploadPart(
		ctx,
		competingSession,
		1,
		bytes.NewReader(competingBytes),
		int64(len(competingBytes)),
		competingDigest,
	)
	if err != nil {
		t.Fatalf("upload competing multipart part: %v", err)
	}
	version, err := restarted.CompleteMultipartUpload(
		ctx,
		session,
		[]artifactstore.CompletedPart{firstPart, lastPart},
	)
	compositeDigest := sha256.New()
	_, _ = compositeDigest.Write(firstDigest[:])
	_, _ = compositeDigest.Write(lastDigest[:])
	wantCompositeChecksum := base64.StdEncoding.EncodeToString(compositeDigest.Sum(nil)) + "-2"
	if err != nil || version.VersionID == "" || version.ObjectKey != objectKey ||
		version.SizeBytes != int64(len(firstBytes)+len(lastBytes)) ||
		version.ChecksumSHA256 != wantCompositeChecksum {
		t.Fatalf("complete multipart upload = %#v error=%v", version, err)
	}
	currentVersion, err := restarted.HeadCurrentVersion(ctx, objectKey)
	if err != nil || currentVersion != version {
		t.Fatalf("current object version = %#v error=%v, want %#v", currentVersion, err, version)
	}
	if _, err := restarted.CompleteMultipartUpload(
		ctx,
		competingSession,
		[]artifactstore.CompletedPart{competingPart},
	); !errors.Is(err, artifactstore.ErrObjectAlreadyExists) {
		t.Fatalf("complete competing multipart upload error = %v", err)
	}

	exact, err := restarted.ReadExactVersion(ctx, objectKey, version.VersionID)
	if err != nil {
		t.Fatalf("read exact Artifact version: %v", err)
	}
	t.Cleanup(func() { _ = exact.Close() })
	content, err := io.ReadAll(exact)
	if err != nil {
		t.Fatalf("read exact Artifact content: %v", err)
	}
	wantContent := append(append([]byte(nil), firstBytes...), lastBytes...)
	if !bytes.Equal(content, wantContent) || exact.VersionID != version.VersionID ||
		exact.ContentType != "video/mp4" {
		t.Fatalf("exact Artifact read metadata = %#v content bytes=%d", exact.ObjectVersion, len(content))
	}
	_ = exact.Close()

	replacement := []byte("later-object-version-must-not-change-committed-read")
	replacementDigest := sha256.Sum256(replacement)
	replacementOutput, err := admin.PutObject(ctx, &s3.PutObjectInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String(objectKey),
		Body:           bytes.NewReader(replacement),
		ContentLength:  aws.Int64(int64(len(replacement))),
		ContentType:    aws.String("video/mp4"),
		ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(replacementDigest[:])),
	})
	if err != nil || replacementOutput.VersionId == nil ||
		*replacementOutput.VersionId == version.VersionID {
		t.Fatalf("write replacement object version = %#v error=%v", replacementOutput, err)
	}
	committedExact, err := restarted.ReadExactVersion(ctx, objectKey, version.VersionID)
	if err != nil {
		t.Fatalf("read committed exact version after overwrite: %v", err)
	}
	committedContent, readCommittedErr := io.ReadAll(committedExact)
	_ = committedExact.Close()
	if readCommittedErr != nil || !bytes.Equal(committedContent, wantContent) ||
		committedExact.VersionID != version.VersionID {
		t.Fatalf(
			"committed exact version after overwrite = %#v bytes=%d error=%v",
			committedExact.ObjectVersion,
			len(committedContent),
			readCommittedErr,
		)
	}

	signed, err := restarted.PresignExactVersion(ctx, objectKey, version.VersionID)
	if err != nil || signed.ExpiresAt.Sub(signed.IssuedAt) != 15*time.Minute {
		t.Fatalf("presign exact Artifact version = %#v error=%v", signed, err)
	}
	signedURL, err := url.Parse(signed.URL)
	if err != nil || signedURL.Query().Get("versionId") != version.VersionID {
		t.Fatalf("signed exact-version URL = %q error=%v", signed.URL, err)
	}
	response, err := http.Get(signed.URL)
	if err != nil {
		t.Fatalf("GET signed Artifact version: %v", err)
	}
	signedContent, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil || response.StatusCode != http.StatusOK || !bytes.Equal(signedContent, wantContent) {
		t.Fatalf("signed GET status=%d bytes=%d error=%v", response.StatusCode, len(signedContent), readErr)
	}

	manifest := []byte(`{"artifact_set":"immutable"}`)
	manifestDigest := sha256.Sum256(manifest)
	const manifestKey = "artifacts/organization/project/job/attempt/artifact/manifest.json"
	if _, err := restarted.PutIfAbsent(
		ctx,
		manifestKey,
		"application/json",
		bytes.NewReader(manifest),
		int64(len(manifest)),
		manifestDigest,
	); err != nil {
		t.Fatalf("conditionally create Artifact object: %v", err)
	}
	_, err = restarted.PutIfAbsent(
		ctx,
		manifestKey,
		"application/json",
		bytes.NewReader([]byte("replacement")),
		int64(len("replacement")),
		sha256.Sum256([]byte("replacement")),
	)
	if !errors.Is(err, artifactstore.ErrObjectAlreadyExists) {
		t.Fatalf("conditional Artifact overwrite error = %v, want ErrObjectAlreadyExists", err)
	}

	unsigned, err := http.Get(endpoint + "/" + bucket + "/" + objectKey)
	if err != nil {
		t.Fatalf("GET private Artifact without signature: %v", err)
	}
	_ = unsigned.Body.Close()
	if unsigned.StatusCode != http.StatusForbidden {
		t.Fatalf("private Artifact status = %d, want 403", unsigned.StatusCode)
	}
}

func TestS3ArtifactStorePurgesEveryBackupVersionAndDeleteMarker(t *testing.T) {
	ctx := context.Background()
	minio := newMinIOFixture(t, "vela-artifact-backup")
	minio.enableVersioning(t)
	const objectKey = "artifacts/organization/project/job/attempt/artifact/video.mp4"

	for _, body := range [][]byte{[]byte("first backup version"), []byte("second backup version")} {
		digest := sha256.Sum256(body)
		if _, err := minio.admin.PutObject(ctx, &s3.PutObjectInput{
			Bucket:         aws.String(minio.bucket),
			Key:            aws.String(objectKey),
			Body:           bytes.NewReader(body),
			ContentLength:  aws.Int64(int64(len(body))),
			ContentType:    aws.String("video/mp4"),
			ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(digest[:])),
		}); err != nil {
			t.Fatalf("write backup object version: %v", err)
		}
	}
	if _, err := minio.admin.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(minio.bucket),
		Key:    aws.String(objectKey),
	}); err != nil {
		t.Fatalf("create backup delete marker: %v", err)
	}

	result, err := minio.store.PurgeObjectVersions(ctx, objectKey)
	if err != nil || result.PurgedVersionCount != 3 {
		t.Fatalf("purge backup versions result=%#v error=%v, want 3 and nil", result, err)
	}
	versions, err := minio.admin.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(minio.bucket),
		Prefix: aws.String(objectKey),
	})
	if err != nil {
		t.Fatalf("list backup versions after purge: %v", err)
	}
	if len(versions.Versions) != 0 || len(versions.DeleteMarkers) != 0 {
		t.Fatalf(
			"backup versions after purge = versions %d delete markers %d, want 0 and 0",
			len(versions.Versions),
			len(versions.DeleteMarkers),
		)
	}
	result, err = minio.store.PurgeObjectVersions(ctx, objectKey)
	if err != nil || result.PurgedVersionCount != 0 {
		t.Fatalf("idempotent backup purge result=%#v error=%v, want 0 and nil", result, err)
	}
}

func containsMultipartUpload(
	uploads []artifactstore.IncompleteMultipartUpload,
	want artifactstore.MultipartUpload,
) bool {
	for _, upload := range uploads {
		if upload.ObjectKey == want.ObjectKey && upload.UploadID == want.UploadID {
			return true
		}
	}
	return false
}

func waitForIncompleteMultipartUploads(
	t *testing.T,
	ctx context.Context,
	store *artifactstore.S3,
	prefix string,
	ready func([]artifactstore.IncompleteMultipartUpload) bool,
) []artifactstore.IncompleteMultipartUpload {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last []artifactstore.IncompleteMultipartUpload
	for {
		uploads, err := store.ListIncompleteMultipartUploads(ctx, prefix)
		if err != nil {
			t.Fatalf("list incomplete multipart uploads: %v", err)
		}
		last = uploads
		if ready(uploads) || !time.Now().Before(deadline) {
			return last
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func newMinIOS3Client(endpoint, region, accessKey, secretKey string) *s3.Client {
	config := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	}
	return s3.NewFromConfig(config, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
}
