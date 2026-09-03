package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	api "github.com/vivym/vela/api/gen"
)

func TestRunSubmitsPollsAndVerifiesCommittedArtifacts(t *testing.T) {
	projectID := uuid.MustParse("84000000-0000-0000-0000-000000000002")
	jobID := uuid.MustParse("84000000-0000-0000-0000-000000000701")
	artifactSetID := uuid.MustParse("84000000-0000-0000-0000-000000000702")
	video := []byte("verified-video")
	thumbnail := []byte("verified-thumbnail")
	videoDigest := sha256.Sum256(video)
	thumbnailDigest := sha256.Sum256(thumbnail)
	getJobCalls := 0

	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasPrefix(request.URL.Path, "/v1/") {
			if request.Header.Get("Authorization") != "Bearer vla_test.secret" ||
				request.Header.Get("Accept") != "application/json" {
				t.Fatalf("API request headers = %#v", request.Header)
			}
		} else if request.Header.Get("Authorization") != "" {
			t.Fatalf("bearer leaked to Artifact download: %#v", request.Header)
		}
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/v1/projects/"+projectID.String()+"/jobs":
			if request.Header.Get("Idempotency-Key") == "" || request.Header.Get("Content-Type") != "application/json" {
				t.Fatalf("submit headers = %#v", request.Header)
			}
			var submitted api.SubmitJobRequest
			if err := json.NewDecoder(request.Body).Decode(&submitted); err != nil {
				t.Fatalf("decode submit request: %v", err)
			}
			if submitted.Model != "h3-mock" || submitted.GenerationPreset != "balanced" ||
				submitted.OutputSpec != "mock-video-1080p-5s-24fps" ||
				submitted.ServiceClass != "standard" || submitted.GenerationCount != 1 {
				t.Fatalf("submit request = %#v", submitted)
			}
			writer.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(writer).Encode(api.Job{
				JobId: jobID, ProjectId: projectID, State: api.JobStateQUEUED,
			})
		case request.Method == http.MethodGet && request.URL.Path == "/v1/projects/"+projectID.String()+"/jobs/"+jobID.String():
			getJobCalls++
			state := api.JobStateRUNNING
			if getJobCalls > 1 {
				state = api.JobStateSUCCEEDED
			}
			_ = json.NewEncoder(writer).Encode(api.Job{
				JobId: jobID, ProjectId: projectID, State: state,
			})
		case request.Method == http.MethodGet && request.URL.Path == "/v1/projects/"+projectID.String()+"/jobs/"+jobID.String()+"/artifacts":
			_ = json.NewEncoder(writer).Encode(api.ArtifactSet{
				ArtifactSetId: artifactSetID, JobId: jobID,
				CommittedAt: time.Now().UTC(), RetentionExpiresAt: time.Now().UTC().Add(time.Hour),
				Artifacts: []api.ArtifactDownload{
					artifactDownload(uuid.New(), api.VIDEO, "video/mp4", server.URL+"/objects/video", video, videoDigest),
					artifactDownload(uuid.New(), api.THUMBNAIL, "image/webp", server.URL+"/objects/thumbnail", thumbnail, thumbnailDigest),
				},
			})
		case request.Method == http.MethodGet && request.URL.Path == "/objects/video":
			_, _ = writer.Write(video)
		case request.Method == http.MethodGet && request.URL.Path == "/objects/thumbnail":
			_, _ = writer.Write(thumbnail)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	credentialFile := filepath.Join(t.TempDir(), "bearer-credential")
	if err := os.WriteFile(credentialFile, []byte("vla_test.secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err := run(context.Background(), []string{
		"--api-url", server.URL,
		"--credential-file", credentialFile,
		"--root-ca-file", "",
		"--poll-interval", "1ms",
		"--timeout", "2s",
	}, &output)
	if err != nil {
		t.Fatalf("run smoke: %v", err)
	}
	if getJobCalls != 2 {
		t.Fatalf("Get Job calls = %d, want 2", getJobCalls)
	}
	var receipt struct {
		SchemaVersion          int      `json:"schema_version"`
		Status                 string   `json:"status"`
		Environment            string   `json:"environment"`
		ProductionGateEvidence bool     `json:"production_gate_evidence"`
		JobID                  string   `json:"job_id"`
		FinalState             string   `json:"final_state"`
		ArtifactSetID          string   `json:"artifact_set_id"`
		ArtifactCount          int      `json:"artifact_count"`
		ArtifactKinds          []string `json:"artifact_kinds"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &receipt); err != nil {
		t.Fatalf("decode receipt %q: %v", output.String(), err)
	}
	if receipt.SchemaVersion != 1 || receipt.Status != "LAB VERIFIED" ||
		receipt.Environment != "non-production-lab" || receipt.ProductionGateEvidence ||
		receipt.JobID != jobID.String() || receipt.FinalState != "SUCCEEDED" ||
		receipt.ArtifactSetID != artifactSetID.String() || receipt.ArtifactCount != 2 ||
		strings.Join(receipt.ArtifactKinds, ",") != "THUMBNAIL,VIDEO" {
		t.Fatalf("receipt = %#v", receipt)
	}
}

func artifactDownload(
	id uuid.UUID,
	kind api.ArtifactDownloadKind,
	contentType string,
	url string,
	payload []byte,
	digest [sha256.Size]byte,
) api.ArtifactDownload {
	return api.ArtifactDownload{
		ArtifactId: id, Kind: kind, Ordinal: 0, ObjectVersionId: "version-1",
		SizeBytes: int64(len(payload)), Sha256: hex.EncodeToString(digest[:]),
		ContentType: contentType, DownloadUrl: url,
		DownloadUrlExpiresAt: time.Now().UTC().Add(time.Minute),
	}
}
