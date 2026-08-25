package workeragent

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/vivym/vela/internal/workertransport"
)

func TestHTTPArtifactPartUploaderPutsOnlyTheChecksumBoundPayload(t *testing.T) {
	payload := []byte("presigned-artifact-part")
	digest := sha256.Sum256(payload)
	checksum := base64.StdEncoding.EncodeToString(digest[:])
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read upload body: %v", err)
		}
		if request.Method != http.MethodPut || string(body) != string(payload) ||
			request.ContentLength != int64(len(payload)) ||
			request.Header.Get("X-Amz-Checksum-Sha256") != checksum {
			t.Errorf("Artifact PUT = %s length=%d checksum=%q body=%q", request.Method, request.ContentLength, request.Header.Get("X-Amz-Checksum-Sha256"), body)
		}
		response.Header().Set("ETag", `"etag-1"`)
		response.Header().Set("X-Amz-Checksum-Sha256", checksum)
		response.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	now := time.Date(2026, 8, 25, 11, 0, 0, 0, time.UTC)
	uploader, err := NewHTTPArtifactPartUploader(HTTPArtifactPartUploaderConfig{
		AllowHTTP: true, Timeout: time.Second, Clock: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewHTTPArtifactPartUploader: %v", err)
	}
	signed := workertransport.SignedArtifactUploadPart{
		Number: 1, SizeBytes: int64(len(payload)), SHA256: digest, URL: server.URL,
		RequiredHeaders: map[string]string{
			"Content-Length":        strconv.FormatInt(int64(len(payload)), 10),
			"X-Amz-Checksum-Sha256": checksum,
		},
		ExpiresAt: now.Add(time.Minute),
	}

	completed, err := uploader.Upload(context.Background(), signed, payload)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if completed.Number != 1 || completed.ETag != `"etag-1"` ||
		completed.SizeBytes != int64(len(payload)) || completed.ChecksumSHA256 != checksum {
		t.Fatalf("completed part = %#v", completed)
	}
}
