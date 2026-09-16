package artifactstore

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func TestPublicDownloadEndpointKeepsStorageTrafficInternal(t *testing.T) {
	requests := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		writePublicationTestMetadata(w, r.URL.Query().Get("versionId"), false)
	}))
	t.Cleanup(backend.Close)
	store, err := NewS3(S3Config{
		Endpoint: backend.URL, DownloadEndpoint: "https://gateway.example:30443",
		Region: "us-east-1", Bucket: "vela-artifacts", UsePathStyle: true,
		AccessKeyID: "test-access", SecretAccessKey: "test-secret", SignedGETTTL: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	const key = "artifacts/org/project/job/attempt/artifact/video.mp4"
	if _, err := store.headExactVersion(context.Background(), key, "version-1"); err != nil {
		t.Fatal(err)
	}
	if requests == 0 {
		t.Fatal("object metadata was not read from the internal endpoint")
	}
	signed, err := store.PresignExactVersion(context.Background(), key, "version-1")
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(signed.URL)
	if err != nil || u.Scheme != "https" || u.Host != "gateway.example:30443" ||
		u.Path != "/vela-artifacts/"+key || u.Query().Get("versionId") != "version-1" ||
		u.Query().Get("X-Amz-SignedHeaders") != "host" || u.Query().Get("X-Amz-Signature") == "" {
		t.Fatal("download did not bind the public origin and exact object version")
	}
}

func TestPublicDownloadEndpointRejectsUnsafeOrigins(t *testing.T) {
	for _, endpoint := range []string{
		"http://gateway.example", "https://user:password@gateway.example",
		"https://gateway.example/prefix", "https://gateway.example?x=1",
		"https://gateway.example#fragment", " https://gateway.example",
	} {
		t.Run(endpoint, func(t *testing.T) {
			_, err := NewS3(S3Config{
				Endpoint: "http://minio.internal:9000", DownloadEndpoint: endpoint,
				Region: "us-east-1", Bucket: "vela-artifacts", UsePathStyle: true,
				AccessKeyID: "test-access", SecretAccessKey: "test-secret", SignedGETTTL: time.Minute,
			})
			if err == nil {
				t.Fatal("accepted an unsafe public download origin")
			}
		})
	}
}
