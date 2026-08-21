package artifactstore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestPresignedUploadPartBindsExactSessionSizeAndChecksum(t *testing.T) {
	store, err := NewS3(S3Config{
		Endpoint:        "https://s3.example.com",
		Region:          "us-test-1",
		Bucket:          "vela-artifacts",
		AccessKeyID:     "test-access-key",
		SecretAccessKey: "test-secret-key",
		UsePathStyle:    true,
		SignedGETTTL:    15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewS3: %v", err)
	}
	now := time.Date(2026, time.August, 22, 1, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	digest := sha256.Sum256([]byte("fixed Artifact multipart part"))
	signed, err := store.PresignUploadPart(
		context.Background(),
		MultipartUpload{
			ObjectKey:   "artifacts/org/project/job/attempt/artifact/video.mp4",
			UploadID:    "fixed-multipart-upload-id",
			ContentType: "video/mp4",
		},
		3,
		27,
		digest,
		now.Add(20*time.Minute),
	)
	if err != nil {
		t.Fatalf("PresignUploadPart: %v", err)
	}
	parsed, err := url.Parse(signed.URL)
	if err != nil {
		t.Fatalf("parse signed upload URL: %v", err)
	}
	wantChecksum := base64.StdEncoding.EncodeToString(digest[:])
	if signed.Method != http.MethodPut ||
		!signed.IssuedAt.Equal(now) || !signed.ExpiresAt.Equal(now.Add(MaxSignedUploadTTL)) ||
		parsed.Path != "/vela-artifacts/artifacts/org/project/job/attempt/artifact/video.mp4" ||
		parsed.Query().Get("uploadId") != "fixed-multipart-upload-id" ||
		parsed.Query().Get("partNumber") != "3" ||
		signed.Headers.Get("Content-Length") != "27" ||
		signed.Headers.Get("X-Amz-Checksum-Sha256") != wantChecksum {
		t.Fatalf("signed upload part = %#v URL=%s", signed, parsed)
	}
}

func TestBucketPrivacyPreflightRejectsAllowWithNotPrincipal(t *testing.T) {
	policy := []byte(`{
		"Version":"2012-10-17",
		"Statement":[{
			"Effect":"Allow",
			"NotPrincipal":{"AWS":"arn:aws:iam::123456789012:role/blocked-only"},
			"Action":"s3:GetObject",
			"Resource":"arn:aws:s3:::vela-artifacts/*"
		}]
	}`)
	public, err := policyAllowsPublicPrincipal(policy)
	if err != nil {
		t.Fatalf("parse bucket policy: %v", err)
	}
	if !public {
		t.Fatal("bucket privacy preflight accepted an Allow statement with NotPrincipal")
	}
}

func TestBucketPrivacyPreflightDoesNotTreatDenyNotPrincipalAsPublic(t *testing.T) {
	policy := []byte(`{
		"Version":"2012-10-17",
		"Statement":{
			"Effect":"Deny",
			"NotPrincipal":{"AWS":"arn:aws:iam::123456789012:role/vela-control"},
			"Action":"s3:GetObject",
			"Resource":"arn:aws:s3:::vela-artifacts/*"
		}
	}`)
	public, err := policyAllowsPublicPrincipal(policy)
	if err != nil {
		t.Fatalf("parse bucket policy: %v", err)
	}
	if public {
		t.Fatal("bucket privacy preflight rejected a Deny statement solely for NotPrincipal")
	}
}
