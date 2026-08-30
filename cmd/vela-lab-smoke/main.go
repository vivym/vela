package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	api "github.com/vivym/vela/api/gen"
)

const (
	defaultBaseURL        = "http://vela-lab-control.vela-lab.svc:8080"
	defaultProjectID      = "84000000-0000-0000-0000-000000000002"
	defaultCredentialFile = "/etc/vela-lab-smoke/bearer-credential"
	defaultTimeout        = 5 * time.Minute
	maximumResponseBytes  = 1 << 20
	maximumArtifactBytes  = 2 << 30
)

type options struct {
	baseURL        string
	projectID      string
	credentialFile string
	timeout        time.Duration
	pollInterval   time.Duration
}

type smokeReceipt struct {
	Status        string   `json:"status"`
	JobID         string   `json:"job_id"`
	FinalState    string   `json:"final_state"`
	ArtifactCount int      `json:"artifact_count"`
	ArtifactKinds []string `json:"artifact_kinds"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "vela lab smoke failed: %v\n", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	flags := flag.NewFlagSet("vela-lab-smoke", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	configuration := options{}
	flags.StringVar(&configuration.baseURL, "base-url", defaultBaseURL, "Vela control HTTP base URL")
	flags.StringVar(&configuration.projectID, "project-id", defaultProjectID, "lab Project UUID")
	flags.StringVar(&configuration.credentialFile, "credential-file", defaultCredentialFile, "bearer credential file")
	flags.DurationVar(&configuration.timeout, "timeout", defaultTimeout, "end-to-end timeout")
	flags.DurationVar(&configuration.pollInterval, "poll-interval", time.Second, "Job poll interval")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	ctx, cancel := context.WithTimeout(context.Background(), configuration.timeout)
	defer cancel()
	receipt, err := smoke(ctx, configuration, &http.Client{Timeout: 30 * time.Second})
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintln(os.Stdout, string(encoded))
	return nil
}

func smoke(ctx context.Context, configuration options, client *http.Client) (smokeReceipt, error) {
	if ctx == nil || client == nil {
		return smokeReceipt{}, errors.New("smoke dependencies are required")
	}
	base, err := url.Parse(strings.TrimRight(configuration.baseURL, "/"))
	if err != nil || base.Scheme != "http" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return smokeReceipt{}, errors.New("--base-url must be a plain lab HTTP origin")
	}
	projectID, err := uuid.Parse(configuration.projectID)
	if err != nil || projectID == uuid.Nil {
		return smokeReceipt{}, errors.New("--project-id must be a UUID")
	}
	if configuration.pollInterval <= 0 || configuration.pollInterval > 10*time.Second {
		return smokeReceipt{}, errors.New("--poll-interval must be in (0, 10s]")
	}
	credentialBytes, err := os.ReadFile(configuration.credentialFile)
	if err != nil {
		return smokeReceipt{}, fmt.Errorf("read smoke credential: %w", err)
	}
	credential := strings.TrimSpace(string(credentialBytes))
	clear(credentialBytes)
	if !strings.HasPrefix(credential, "vla_") || len(credential) > 1024 {
		return smokeReceipt{}, errors.New("smoke credential is invalid")
	}
	requestBody := api.SubmitJobRequest{
		Model: "h3-mock", GenerationPreset: api.Balanced,
		ServiceClass: api.Standard, OutputSpec: "mock-video-1080p-5s-24fps",
		GenerationCount: 1, Prompt: "non-production lab end-to-end smoke",
	}
	encodedRequest, err := json.Marshal(requestBody)
	if err != nil {
		return smokeReceipt{}, err
	}
	jobsURL := fmt.Sprintf("%s/v1/projects/%s/jobs", strings.TrimRight(configuration.baseURL, "/"), projectID)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, jobsURL, bytes.NewReader(encodedRequest))
	if err != nil {
		return smokeReceipt{}, err
	}
	request.Header.Set("Authorization", "Bearer "+credential)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "lab-smoke-"+uuid.NewString())
	var job api.Job
	if err := requestJSON(client, request, http.StatusAccepted, &job); err != nil {
		return smokeReceipt{}, fmt.Errorf("submit mock Job: %w", err)
	}
	jobID := uuid.UUID(job.JobId)
	if jobID == uuid.Nil || uuid.UUID(job.ProjectId) != projectID {
		return smokeReceipt{}, errors.New("submitted Job identity is invalid")
	}
	for job.State != api.JobStateSUCCEEDED {
		switch job.State {
		case api.JobStateFAILED, api.JobStateCANCELED:
			return smokeReceipt{}, fmt.Errorf("mock Job entered terminal state %s", job.State)
		}
		select {
		case <-ctx.Done():
			return smokeReceipt{}, fmt.Errorf("wait for mock Job %s: %w", jobID, ctx.Err())
		case <-time.After(configuration.pollInterval):
		}
		jobURL := fmt.Sprintf("%s/%s", jobsURL, jobID)
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, jobURL, nil)
		if err != nil {
			return smokeReceipt{}, err
		}
		request.Header.Set("Authorization", "Bearer "+credential)
		if err := requestJSON(client, request, http.StatusOK, &job); err != nil {
			return smokeReceipt{}, fmt.Errorf("poll mock Job: %w", err)
		}
	}
	artifactsURL := fmt.Sprintf("%s/%s/artifacts", jobsURL, jobID)
	request, err = http.NewRequestWithContext(ctx, http.MethodGet, artifactsURL, nil)
	if err != nil {
		return smokeReceipt{}, err
	}
	request.Header.Set("Authorization", "Bearer "+credential)
	var artifactSet api.ArtifactSet
	if err := requestJSON(client, request, http.StatusOK, &artifactSet); err != nil {
		return smokeReceipt{}, fmt.Errorf("get committed ArtifactSet: %w", err)
	}
	if uuid.UUID(artifactSet.JobId) != jobID || len(artifactSet.Artifacts) != 2 {
		return smokeReceipt{}, errors.New("committed ArtifactSet does not contain the two mock outputs")
	}
	kinds := make([]string, 0, len(artifactSet.Artifacts))
	seenKinds := make(map[string]bool, len(artifactSet.Artifacts))
	for _, artifact := range artifactSet.Artifacts {
		kind := string(artifact.Kind)
		if seenKinds[kind] || (kind != "VIDEO" && kind != "THUMBNAIL") {
			return smokeReceipt{}, fmt.Errorf("unexpected Artifact kind %q", kind)
		}
		seenKinds[kind] = true
		if err := verifyArtifact(ctx, client, artifact); err != nil {
			return smokeReceipt{}, err
		}
		kinds = append(kinds, kind)
	}
	if !seenKinds["VIDEO"] || !seenKinds["THUMBNAIL"] {
		return smokeReceipt{}, errors.New("mock ArtifactSet is missing VIDEO or THUMBNAIL")
	}
	return smokeReceipt{
		Status: "LAB VERIFIED", JobID: jobID.String(), FinalState: string(job.State),
		ArtifactCount: len(artifactSet.Artifacts), ArtifactKinds: kinds,
	}, nil
}

func requestJSON(client *http.Client, request *http.Request, expectedStatus int, target any) error {
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	content, err := io.ReadAll(io.LimitReader(response.Body, maximumResponseBytes+1))
	if err != nil {
		return err
	}
	if len(content) > maximumResponseBytes {
		return errors.New("HTTP response exceeds the lab bound")
	}
	if response.StatusCode != expectedStatus {
		return fmt.Errorf("HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(content)))
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("HTTP response contains trailing JSON")
	}
	return nil
}

func verifyArtifact(ctx context.Context, client *http.Client, artifact api.ArtifactDownload) error {
	if artifact.SizeBytes <= 0 || artifact.SizeBytes > maximumArtifactBytes ||
		len(artifact.Sha256) != sha256.Size*2 || artifact.DownloadUrl == "" ||
		artifact.ObjectVersionId == "" {
		return fmt.Errorf("artifact %s metadata is invalid", artifact.ArtifactId)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, artifact.DownloadUrl, nil)
	if err != nil {
		return fmt.Errorf("create Artifact download request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("download Artifact %s: %w", artifact.ArtifactId, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("download Artifact %s returned HTTP %d", artifact.ArtifactId, response.StatusCode)
	}
	digest := sha256.New()
	written, err := io.Copy(digest, io.LimitReader(response.Body, maximumArtifactBytes+1))
	if err != nil {
		return fmt.Errorf("hash Artifact %s: %w", artifact.ArtifactId, err)
	}
	if written != artifact.SizeBytes || written > maximumArtifactBytes ||
		hex.EncodeToString(digest.Sum(nil)) != artifact.Sha256 {
		return fmt.Errorf("artifact %s size or digest does not match Visible Completion", artifact.ArtifactId)
	}
	return nil
}

func clear(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
