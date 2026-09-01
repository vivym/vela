package h3campaignrunner

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	api "github.com/vivym/vela/api/gen"
)

func TestHTTPClientSubmitsAndReadsJobWithoutLeakingBearer(t *testing.T) {
	projectID := uuid.MustParse("49350000-0000-0000-0000-000000000201")
	jobID := uuid.MustParse("49350000-0000-0000-0000-000000000202")
	request := validManifest().Request
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, incoming *http.Request) {
		calls++
		if incoming.Header.Get("Authorization") != "Bearer secret-token" ||
			incoming.Header.Get("Accept") != "application/json" {
			t.Fatalf("request headers = %#v", incoming.Header)
		}
		if calls == 1 {
			if incoming.Method != http.MethodPost ||
				incoming.URL.Path != "/v1/projects/"+projectID.String()+"/jobs" ||
				incoming.Header.Get("Idempotency-Key") != "campaign-key" {
				t.Fatalf("submit request = %s %s %#v", incoming.Method, incoming.URL, incoming.Header)
			}
			var actual api.SubmitJobRequest
			if err := json.NewDecoder(incoming.Body).Decode(&actual); err != nil || actual != request {
				t.Fatalf("submit body = %#v error=%v", actual, err)
			}
			writer.WriteHeader(http.StatusAccepted)
		} else {
			if incoming.Method != http.MethodGet ||
				incoming.URL.Path != "/v1/projects/"+projectID.String()+"/jobs/"+jobID.String() {
				t.Fatalf("get request = %s %s", incoming.Method, incoming.URL)
			}
		}
		_ = json.NewEncoder(writer).Encode(api.Job{
			JobId: jobID, ProjectId: projectID, State: api.JobStateSUCCEEDED,
		})
	}))
	defer server.Close()
	client, err := NewHTTPClient(server.URL, "secret-token", server.Client())
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	job, err := client.SubmitJob(context.Background(), projectID, "campaign-key", request)
	if err != nil || uuid.UUID(job.JobId) != jobID {
		t.Fatalf("SubmitJob = %#v error=%v", job, err)
	}
	job, err = client.GetJob(context.Background(), projectID, jobID)
	if err != nil || job.State != api.JobStateSUCCEEDED || calls != 2 {
		t.Fatalf("GetJob = %#v calls=%d error=%v", job, calls, err)
	}
}

func TestHTTPClientRejectsRedirectAndUnsafeConfiguration(t *testing.T) {
	for _, baseURL := range []string{"http://example.com", "https://user@example.com", "not-a-url"} {
		if _, err := NewHTTPClient(baseURL, "token", nil); err == nil {
			t.Fatalf("unsafe base URL %q accepted", baseURL)
		}
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, "https://example.com", http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	client, err := NewHTTPClient(redirect.URL, "token", redirect.Client())
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	_, err = client.SubmitJob(
		context.Background(), validManifest().ProjectID, "campaign-key", validManifest().Request,
	)
	if err == nil || !strings.Contains(err.Error(), "HTTP 307") {
		t.Fatalf("redirect error = %v", err)
	}
}

func TestHTTPClientRejectsAmbiguousResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusAccepted)
		_, _ = writer.Write([]byte(`{"job_id":"49350000-0000-0000-0000-000000000202","job_id":"49350000-0000-0000-0000-000000000203"}`))
	}))
	defer server.Close()
	client, err := NewHTTPClient(server.URL, "token", server.Client())
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	_, err = client.SubmitJob(
		context.Background(), validManifest().ProjectID, "campaign-key", validManifest().Request,
	)
	if err == nil || !strings.Contains(err.Error(), "duplicate JSON key") {
		t.Fatalf("ambiguous response error = %v", err)
	}
}
