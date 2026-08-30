package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestSmokeSubmitsPollsAndVerifiesArtifacts(t *testing.T) {
	video := []byte("mock-video")
	thumbnail := []byte("mock-thumbnail")
	videoDigest := sha256.Sum256(video)
	thumbnailDigest := sha256.Sum256(thumbnail)
	var polls atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer vla_fixture.secret" && request.URL.Path != "/objects/video" && request.URL.Path != "/objects/thumbnail" {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/v1/projects/84000000-0000-0000-0000-000000000002/jobs":
			response.WriteHeader(http.StatusAccepted)
			writeJob(t, response, "QUEUED")
		case request.Method == http.MethodGet && request.URL.Path == "/v1/projects/84000000-0000-0000-0000-000000000002/jobs/85000000-0000-0000-0000-000000000001":
			if polls.Add(1) == 1 {
				writeJob(t, response, "RUNNING")
			} else {
				writeJob(t, response, "SUCCEEDED")
			}
		case request.Method == http.MethodGet && request.URL.Path == "/v1/projects/84000000-0000-0000-0000-000000000002/jobs/85000000-0000-0000-0000-000000000001/artifacts":
			_ = json.NewEncoder(response).Encode(map[string]any{
				"artifact_set_id": "85000000-0000-0000-0000-000000000002",
				"job_id":          "85000000-0000-0000-0000-000000000001",
				"committed_at":    time.Now().UTC(), "retention_expires_at": time.Now().Add(time.Hour).UTC(),
				"artifacts": []map[string]any{
					artifactJSON(server.URL+"/objects/video", "VIDEO", video, hex.EncodeToString(videoDigest[:]), 1),
					artifactJSON(server.URL+"/objects/thumbnail", "THUMBNAIL", thumbnail, hex.EncodeToString(thumbnailDigest[:]), 2),
				},
			})
		case request.URL.Path == "/objects/video":
			_, _ = response.Write(video)
		case request.URL.Path == "/objects/thumbnail":
			_, _ = response.Write(thumbnail)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	credential := filepath.Join(t.TempDir(), "credential")
	if err := os.WriteFile(credential, []byte("vla_fixture.secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	receipt, err := smoke(t.Context(), options{
		baseURL: server.URL, projectID: defaultProjectID, credentialFile: credential,
		pollInterval: time.Millisecond,
	}, server.Client())
	if err != nil {
		t.Fatalf("smoke: %v", err)
	}
	if receipt.Status != "LAB VERIFIED" || receipt.FinalState != "SUCCEEDED" || receipt.ArtifactCount != 2 {
		t.Fatalf("receipt = %#v", receipt)
	}
}

func writeJob(t *testing.T, response http.ResponseWriter, state string) {
	t.Helper()
	now := time.Now().UTC()
	if err := json.NewEncoder(response).Encode(map[string]any{
		"attempts_started": 0, "created_at": now, "job_expires_at": now.Add(time.Hour),
		"job_id": "85000000-0000-0000-0000-000000000001", "project_id": defaultProjectID,
		"state":   state,
		"pricing": map[string]any{"currency": "CNY", "quantity": 1, "quoted_amount_minor": 1, "unit_amount_minor": 1},
	}); err != nil {
		t.Fatal(err)
	}
}

func artifactJSON(downloadURL, kind string, content []byte, digest string, ordinal int) map[string]any {
	return map[string]any{
		"artifact_id":  fmt.Sprintf("85000000-0000-0000-0000-%012d", ordinal+10),
		"content_type": "application/octet-stream", "download_url": downloadURL,
		"download_url_expires_at": time.Now().Add(time.Hour).UTC(), "kind": kind,
		"object_version_id": fmt.Sprintf("version-%d", ordinal), "ordinal": ordinal - 1,
		"sha256": digest, "size_bytes": len(content),
	}
}
