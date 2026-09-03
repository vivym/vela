package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	api "github.com/vivym/vela/api/gen"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
)

const (
	defaultAPIURL         = "http://vela-lab-control.vela-lab-v2.svc:8080"
	defaultCredentialFile = "/etc/vela-lab-smoke/bearer-credential"
	defaultRootCAFile     = "/etc/vela-lab-smoke/ca.crt"
	defaultProjectID      = "84000000-0000-0000-0000-000000000002"
	maximumResponseBytes  = 1 << 20
	maximumCredentialSize = 4096
)

type options struct {
	apiURL         string
	credentialFile string
	rootCAFile     string
	projectID      uuid.UUID
	pollInterval   time.Duration
	timeout        time.Duration
}

type smokeClient struct {
	baseURL *url.URL
	token   string
	client  *http.Client
}

type receipt struct {
	SchemaVersion          int               `json:"schema_version"`
	Status                 string            `json:"status"`
	Environment            string            `json:"environment"`
	ProductionGateEvidence bool              `json:"production_gate_evidence"`
	JobID                  string            `json:"job_id"`
	FinalState             string            `json:"final_state"`
	ArtifactSetID          string            `json:"artifact_set_id"`
	ArtifactCount          int               `json:"artifact_count"`
	ArtifactKinds          []string          `json:"artifact_kinds"`
	Artifacts              []receiptArtifact `json:"artifacts"`
}

type receiptArtifact struct {
	Kind      string `json:"kind"`
	Ordinal   int32  `json:"ordinal"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "vela lab smoke failed: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, arguments []string, output io.Writer) error {
	if ctx == nil || output == nil {
		return errors.New("lab smoke context and output are required")
	}
	configuration, err := parseOptions(arguments)
	if err != nil {
		return err
	}
	client, err := newSmokeClient(configuration)
	if err != nil {
		return err
	}
	runContext, cancel := context.WithTimeout(ctx, configuration.timeout)
	defer cancel()

	job, err := client.submitJob(runContext, configuration.projectID)
	if err != nil {
		return err
	}
	job, err = client.waitForTerminal(
		runContext, configuration.projectID, job, configuration.pollInterval,
	)
	if err != nil {
		return err
	}
	artifacts, err := client.getArtifacts(
		runContext, configuration.projectID, uuid.UUID(job.JobId),
	)
	if err != nil {
		return err
	}
	verified, err := client.verifyArtifacts(runContext, uuid.UUID(job.JobId), artifacts)
	if err != nil {
		return err
	}
	kinds := make([]string, 0, len(verified))
	for _, artifact := range verified {
		kinds = append(kinds, artifact.Kind)
	}
	sort.Strings(kinds)
	encoded := receipt{
		SchemaVersion: 1, Status: "LAB VERIFIED", Environment: "non-production-lab",
		ProductionGateEvidence: false, JobID: job.JobId.String(), FinalState: string(job.State),
		ArtifactSetID: artifacts.ArtifactSetId.String(), ArtifactCount: len(verified),
		ArtifactKinds: kinds, Artifacts: verified,
	}
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(encoded); err != nil {
		return fmt.Errorf("write lab smoke receipt: %w", err)
	}
	return nil
}

func parseOptions(arguments []string) (options, error) {
	flags := flag.NewFlagSet("vela-lab-smoke", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var configuration options
	var projectID string
	flags.StringVar(&configuration.apiURL, "api-url", defaultAPIURL, "Vela API base URL")
	flags.StringVar(&configuration.credentialFile, "credential-file", defaultCredentialFile, "bearer credential file")
	flags.StringVar(&configuration.rootCAFile, "root-ca-file", defaultRootCAFile, "lab Root CA file")
	flags.StringVar(&projectID, "project-id", defaultProjectID, "lab Project id")
	flags.DurationVar(&configuration.pollInterval, "poll-interval", time.Second, "Job polling interval")
	flags.DurationVar(&configuration.timeout, "timeout", 6*time.Minute, "overall smoke timeout")
	if err := flags.Parse(arguments); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 {
		return options{}, errors.New("unexpected positional arguments")
	}
	parsedProjectID, err := uuid.Parse(projectID)
	if err != nil || parsedProjectID == uuid.Nil {
		return options{}, errors.New("--project-id must be a non-zero UUID")
	}
	configuration.projectID = parsedProjectID
	if configuration.pollInterval <= 0 || configuration.pollInterval > time.Minute ||
		configuration.timeout <= configuration.pollInterval || configuration.timeout > 15*time.Minute {
		return options{}, errors.New("lab smoke polling interval or timeout is invalid")
	}
	return configuration, nil
}

func newSmokeClient(configuration options) (*smokeClient, error) {
	baseURL, err := parseEndpoint(configuration.apiURL, true)
	if err != nil {
		return nil, fmt.Errorf("parse lab smoke API URL: %w", err)
	}
	credential, err := securefile.Read(configuration.credentialFile, maximumCredentialSize, false)
	if err != nil {
		return nil, fmt.Errorf("read lab smoke bearer credential: %w", err)
	}
	token := strings.TrimSpace(string(credential))
	clear(credential)
	if token == "" || strings.TrimSpace(token) != token || strings.ContainsAny(token, "\x00\r\n") {
		return nil, errors.New("lab smoke bearer credential is invalid")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS13}
	if configuration.rootCAFile != "" {
		rootPEM, readErr := securefile.Read(configuration.rootCAFile, maximumResponseBytes, false)
		if readErr != nil {
			return nil, fmt.Errorf("read lab smoke Root CA: %w", readErr)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(rootPEM) {
			clear(rootPEM)
			return nil, errors.New("lab smoke Root CA contains no certificates")
		}
		clear(rootPEM)
		transport.TLSClientConfig.RootCAs = roots
	}
	httpClient := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &smokeClient{baseURL: baseURL, token: token, client: httpClient}, nil
}

func (client *smokeClient) submitJob(ctx context.Context, projectID uuid.UUID) (api.Job, error) {
	request := api.SubmitJobRequest{
		Model: "h3-mock", GenerationPreset: api.Balanced, ServiceClass: api.Standard,
		OutputSpec: "mock-video-1080p-5s-24fps", GenerationCount: 1,
		Prompt: "Vela non-production lab Stage graph smoke",
	}
	body, err := json.Marshal(request)
	if err != nil {
		return api.Job{}, fmt.Errorf("encode lab smoke Job: %w", err)
	}
	randomKey := make([]byte, 16)
	if _, err := rand.Read(randomKey); err != nil {
		return api.Job{}, fmt.Errorf("generate lab smoke idempotency key: %w", err)
	}
	jobURL := client.baseURL.JoinPath("v1", "projects", projectID.String(), "jobs")
	var job api.Job
	if err := client.doAPIJSON(
		ctx, http.MethodPost, jobURL, "vela-lab-smoke-"+hex.EncodeToString(randomKey), body,
		http.StatusAccepted, &job,
	); err != nil {
		return api.Job{}, fmt.Errorf("submit lab smoke Job: %w", err)
	}
	if err := validateJob(job, projectID, uuid.Nil); err != nil {
		return api.Job{}, fmt.Errorf("submit lab smoke Job: %w", err)
	}
	return job, nil
}

func (client *smokeClient) waitForTerminal(
	ctx context.Context,
	projectID uuid.UUID,
	job api.Job,
	pollInterval time.Duration,
) (api.Job, error) {
	jobID := uuid.UUID(job.JobId)
	for {
		switch job.State {
		case api.JobStateSUCCEEDED:
			return job, nil
		case api.JobStateFAILED, api.JobStateCANCELED:
			return api.Job{}, fmt.Errorf("lab smoke Job %s reached terminal state %s", jobID, job.State)
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return api.Job{}, fmt.Errorf("wait for lab smoke Job %s: %w", jobID, ctx.Err())
		case <-timer.C:
		}
		jobURL := client.baseURL.JoinPath("v1", "projects", projectID.String(), "jobs", jobID.String())
		var refreshed api.Job
		if err := client.doAPIJSON(ctx, http.MethodGet, jobURL, "", nil, http.StatusOK, &refreshed); err != nil {
			return api.Job{}, fmt.Errorf("read lab smoke Job %s: %w", jobID, err)
		}
		if err := validateJob(refreshed, projectID, jobID); err != nil {
			return api.Job{}, fmt.Errorf("read lab smoke Job %s: %w", jobID, err)
		}
		job = refreshed
	}
}

func (client *smokeClient) getArtifacts(
	ctx context.Context,
	projectID uuid.UUID,
	jobID uuid.UUID,
) (api.ArtifactSet, error) {
	artifactURL := client.baseURL.JoinPath(
		"v1", "projects", projectID.String(), "jobs", jobID.String(), "artifacts",
	)
	var result api.ArtifactSet
	if err := client.doAPIJSON(ctx, http.MethodGet, artifactURL, "", nil, http.StatusOK, &result); err != nil {
		return api.ArtifactSet{}, fmt.Errorf("read lab smoke ArtifactSet: %w", err)
	}
	return result, nil
}

func (client *smokeClient) verifyArtifacts(
	ctx context.Context,
	jobID uuid.UUID,
	set api.ArtifactSet,
) ([]receiptArtifact, error) {
	if uuid.UUID(set.ArtifactSetId) == uuid.Nil || uuid.UUID(set.JobId) != jobID || len(set.Artifacts) != 2 {
		return nil, errors.New("lab smoke ArtifactSet identity or cardinality is invalid")
	}
	seen := make(map[api.ArtifactDownloadKind]bool, len(set.Artifacts))
	verified := make([]receiptArtifact, 0, len(set.Artifacts))
	for _, artifact := range set.Artifacts {
		wantContentType := map[api.ArtifactDownloadKind]string{
			api.VIDEO: "video/mp4", api.THUMBNAIL: "image/webp",
		}[artifact.Kind]
		if wantContentType == "" || seen[artifact.Kind] || uuid.UUID(artifact.ArtifactId) == uuid.Nil ||
			artifact.Ordinal != 0 || artifact.ObjectVersionId == "" || artifact.SizeBytes <= 0 ||
			artifact.ContentType != wantContentType || len(artifact.Sha256) != sha256.Size*2 ||
			artifact.DownloadUrlExpiresAt.Before(time.Now()) {
			return nil, fmt.Errorf("lab smoke %s Artifact metadata is invalid", artifact.Kind)
		}
		expectedDigest, err := hex.DecodeString(artifact.Sha256)
		if err != nil || hex.EncodeToString(expectedDigest) != artifact.Sha256 {
			return nil, fmt.Errorf("lab smoke %s Artifact SHA-256 is invalid", artifact.Kind)
		}
		if err := client.verifyDownload(ctx, artifact, expectedDigest); err != nil {
			return nil, err
		}
		seen[artifact.Kind] = true
		verified = append(verified, receiptArtifact{
			Kind: string(artifact.Kind), Ordinal: artifact.Ordinal,
			SizeBytes: artifact.SizeBytes, SHA256: artifact.Sha256,
		})
	}
	if !seen[api.VIDEO] || !seen[api.THUMBNAIL] {
		return nil, errors.New("lab smoke ArtifactSet is incomplete")
	}
	sort.Slice(verified, func(left, right int) bool { return verified[left].Kind < verified[right].Kind })
	return verified, nil
}

func (client *smokeClient) verifyDownload(
	ctx context.Context,
	artifact api.ArtifactDownload,
	expectedDigest []byte,
) error {
	downloadURL, err := parseEndpoint(artifact.DownloadUrl, false)
	if err != nil {
		return fmt.Errorf("parse lab smoke %s download URL: %w", artifact.Kind, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, downloadURL.String(), nil)
	if err != nil {
		return fmt.Errorf("build lab smoke %s download: %w", artifact.Kind, err)
	}
	response, err := client.client.Do(request)
	if err != nil {
		return fmt.Errorf("download lab smoke %s Artifact: %w", artifact.Kind, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("download lab smoke %s Artifact returned HTTP %d", artifact.Kind, response.StatusCode)
	}
	hash := sha256.New()
	read, err := io.Copy(hash, io.LimitReader(response.Body, artifact.SizeBytes+1))
	if err != nil {
		return fmt.Errorf("read lab smoke %s Artifact: %w", artifact.Kind, err)
	}
	if read != artifact.SizeBytes || !bytes.Equal(hash.Sum(nil), expectedDigest) {
		return fmt.Errorf("lab smoke %s Artifact payload does not match committed size and SHA-256", artifact.Kind)
	}
	return nil
}

func (client *smokeClient) doAPIJSON(
	ctx context.Context,
	method string,
	requestURL *url.URL,
	idempotencyKey string,
	body []byte,
	wantStatus int,
	target any,
) error {
	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+client.token)
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := client.client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, maximumResponseBytes+1))
	if err != nil {
		return err
	}
	if len(encoded) > maximumResponseBytes {
		return errors.New("vela API response exceeds size bound")
	}
	if response.StatusCode != wantStatus {
		var apiError api.Error
		if decodeStrictJSON(encoded, &apiError) == nil && apiError.Code != "" {
			return fmt.Errorf("vela API returned HTTP %d (%s): %s", response.StatusCode, apiError.Code, apiError.Message)
		}
		return fmt.Errorf("vela API returned HTTP %d", response.StatusCode)
	}
	if err := decodeStrictJSON(encoded, target); err != nil {
		return fmt.Errorf("decode Vela API response: %w", err)
	}
	return nil
}

func validateJob(job api.Job, projectID, expectedJobID uuid.UUID) error {
	jobID := uuid.UUID(job.JobId)
	if jobID == uuid.Nil || uuid.UUID(job.ProjectId) != projectID || !job.State.Valid() ||
		expectedJobID != uuid.Nil && jobID != expectedJobID {
		return errors.New("vela API returned an invalid lab smoke Job identity")
	}
	return nil
}

func decodeStrictJSON(encoded []byte, target any) error {
	if len(encoded) == 0 {
		return errors.New("JSON response is empty")
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("JSON response contains trailing data")
	}
	return nil
}

func parseEndpoint(value string, apiEndpoint bool) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" || parsed.User != nil ||
		parsed.Fragment != "" || apiEndpoint && (parsed.RawQuery != "" ||
		(parsed.Path != "" && parsed.Path != "/")) {
		return nil, errors.New("endpoint must be a canonical absolute URL")
	}
	loopback := strings.EqualFold(parsed.Hostname(), "localhost")
	if address := net.ParseIP(parsed.Hostname()); address != nil {
		loopback = address.IsLoopback()
	}
	validHTTP := parsed.Scheme == "http" && (loopback || apiEndpoint && parsed.Hostname() == "vela-lab-control.vela-lab-v2.svc")
	if parsed.Scheme != "https" && !validHTTP {
		return nil, errors.New("endpoint must use HTTPS or an approved lab HTTP address")
	}
	return parsed, nil
}

func clear(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
