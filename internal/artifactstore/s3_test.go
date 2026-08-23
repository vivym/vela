package artifactstore

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestDeleteExactVersionBindsObjectKeyAndVersion(t *testing.T) {
	const (
		objectKey = "artifacts/org/project/job/attempt/artifact/video.mp4"
		versionID = "01JEXACTOBJECTVERSION"
	)
	requested := make(chan *http.Request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requested <- request.Clone(request.Context())
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	store, err := NewS3(S3Config{
		Endpoint:        server.URL,
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
	if err := store.DeleteExactVersion(context.Background(), objectKey, versionID); err != nil {
		t.Fatalf("DeleteExactVersion: %v", err)
	}
	select {
	case request := <-requested:
		if request.Method != http.MethodDelete ||
			request.URL.Path != "/vela-artifacts/"+objectKey ||
			request.URL.Query().Get("versionId") != versionID {
			t.Fatalf("delete request = %s %s", request.Method, request.URL.String())
		}
	case <-time.After(time.Second):
		t.Fatal("DeleteExactVersion sent no S3 request")
	}
}

func TestDeleteExactVersionRejectsMissingIdentity(t *testing.T) {
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
	for _, test := range []struct {
		name      string
		objectKey string
		versionID string
	}{
		{name: "missing key", versionID: "version-1"},
		{name: "missing version", objectKey: "artifacts/org/project/job/video.mp4"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := store.DeleteExactVersion(
				context.Background(), test.objectKey, test.versionID,
			); err == nil {
				t.Fatal("DeleteExactVersion accepted an incomplete object identity")
			}
		})
	}
}

func TestDeleteExactVersionTreatsNoSuchVersionAsAlreadyAbsent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodDelete || request.URL.Query().Get("versionId") == "" {
			t.Fatalf("delete absent request = %s %s", request.Method, request.URL.String())
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchVersion</Code><Message>absent</Message></Error>`))
	}))
	t.Cleanup(server.Close)
	store, err := NewS3(S3Config{
		Endpoint:        server.URL,
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
	if err := store.DeleteExactVersion(
		context.Background(),
		"artifacts/org/project/job/video.mp4",
		"absent-version",
	); err != nil {
		t.Fatalf("DeleteExactVersion absent version: %v", err)
	}
}

func TestResolveCurrentVersionReturnsExactMetadata(t *testing.T) {
	const objectKey = "artifacts/org/project/job/video.mp4"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodHead ||
			request.URL.Path != "/vela-artifacts/"+objectKey {
			t.Fatalf("resolve request = %s %s", request.Method, request.URL.String())
		}
		w.Header().Set("Content-Length", "4096")
		w.Header().Set("Content-Type", "video/mp4")
		w.Header().Set("ETag", `"resolved-etag"`)
		w.Header().Set("x-amz-version-id", "resolved-version")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	store, err := NewS3(S3Config{
		Endpoint:        server.URL,
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
	version, exists, err := store.ResolveCurrentVersion(context.Background(), objectKey)
	if err != nil {
		t.Fatalf("ResolveCurrentVersion: %v", err)
	}
	if !exists || version.ObjectKey != objectKey || version.VersionID != "resolved-version" ||
		version.ETag != `"resolved-etag"` || version.SizeBytes != 4096 ||
		version.ContentType != "video/mp4" {
		t.Fatalf("resolved current version = exists %t %#v", exists, version)
	}
}

func TestResolveCurrentVersionTreatsNoSuchKeyAsAbsent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodHead {
			t.Fatalf("resolve absent method = %s", request.Method)
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	store, err := NewS3(S3Config{
		Endpoint:        server.URL,
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
	version, exists, err := store.ResolveCurrentVersion(
		context.Background(), "artifacts/org/project/job/absent.mp4",
	)
	if err != nil || exists || version != (ObjectVersion{}) {
		t.Fatalf("ResolveCurrentVersion absent = exists %t %#v error=%v", exists, version, err)
	}
}

func TestListIncompleteMultipartUploadsBindsObjectPrefix(t *testing.T) {
	const objectPrefix = "artifacts/org/project/job/"
	requested := make(chan *http.Request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requested <- request.Clone(request.Context())
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>vela-artifacts</Bucket>
  <KeyMarker></KeyMarker>
  <UploadIdMarker></UploadIdMarker>
  <NextKeyMarker></NextKeyMarker>
  <NextUploadIdMarker></NextUploadIdMarker>
  <Delimiter></Delimiter>
  <Prefix>artifacts/org/project/job/</Prefix>
  <IsTruncated>false</IsTruncated>
  <Upload>
    <Key>artifacts/org/project/job/attempt/video.mp4</Key>
    <UploadId>fixed-upload-id</UploadId>
    <Initiated>2026-08-24T01:02:03Z</Initiated>
  </Upload>
</ListMultipartUploadsResult>`))
	}))
	t.Cleanup(server.Close)
	store, err := NewS3(S3Config{
		Endpoint:        server.URL,
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
	uploads, err := store.ListIncompleteMultipartUploads(context.Background(), objectPrefix)
	if err != nil {
		t.Fatalf("ListIncompleteMultipartUploads: %v", err)
	}
	if len(uploads) != 1 || uploads[0].ObjectKey != objectPrefix+"attempt/video.mp4" ||
		uploads[0].UploadID != "fixed-upload-id" {
		t.Fatalf("incomplete multipart uploads = %#v", uploads)
	}
	request := <-requested
	if request.URL.Query().Get("prefix") != objectPrefix {
		t.Fatalf("ListMultipartUploads prefix = %q, want %q", request.URL.Query().Get("prefix"), objectPrefix)
	}
}

func TestListIncompleteMultipartUploadsFallsBackToBoundedKeyRange(t *testing.T) {
	const objectPrefix = "artifacts/org/project/job/"
	requested := make(chan *http.Request, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requested <- request.Clone(request.Context())
		w.Header().Set("Content-Type", "application/xml")
		if request.URL.Query().Get("prefix") != "" {
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>vela-artifacts</Bucket>
  <IsTruncated>false</IsTruncated>
</ListMultipartUploadsResult>`))
			return
		}
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>vela-artifacts</Bucket>
  <IsTruncated>false</IsTruncated>
  <Upload>
    <Key>artifacts/org/project/job/orphan/video.mp4</Key>
    <UploadId>matched-upload-id</UploadId>
    <Initiated>2026-08-24T01:02:03Z</Initiated>
  </Upload>
  <Upload>
    <Key>artifacts/org/project/job0/unrelated.mp4</Key>
    <UploadId>unrelated-upload-id</UploadId>
    <Initiated>2026-08-24T01:02:04Z</Initiated>
  </Upload>
</ListMultipartUploadsResult>`))
	}))
	t.Cleanup(server.Close)
	store, err := NewS3(S3Config{
		Endpoint:        server.URL,
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
	uploads, err := store.ListIncompleteMultipartUploads(context.Background(), objectPrefix)
	if err != nil {
		t.Fatalf("ListIncompleteMultipartUploads: %v", err)
	}
	if len(uploads) != 1 || uploads[0].ObjectKey != objectPrefix+"orphan/video.mp4" ||
		uploads[0].UploadID != "matched-upload-id" {
		t.Fatalf("bounded incomplete multipart uploads = %#v", uploads)
	}
	standard := <-requested
	fallback := <-requested
	if standard.URL.Query().Get("prefix") != objectPrefix ||
		fallback.URL.Query().Get("prefix") != "" ||
		fallback.URL.Query().Get("key-marker") != objectPrefix {
		t.Fatalf("multipart listing requests = %s then %s", standard.URL.RawQuery, fallback.URL.RawQuery)
	}
}

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
