package h3campaignrunner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"
	api "github.com/vivym/vela/api/gen"
	"github.com/vivym/vela/internal/strictjson"
)

const maximumResponseBytes = 1 << 20

type HTTPClient struct {
	baseURL *url.URL
	token   string
	client  *http.Client
}

func NewHTTPClient(baseURL, bearerToken string, client *http.Client) (*HTTPClient, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || !parsed.IsAbs() || parsed.Host == "" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Scheme != "https" &&
			(parsed.Scheme != "http" || !isLoopbackHost(parsed.Hostname()))) {
		return nil, errors.New("h3 campaign API URL must be HTTPS or loopback HTTP")
	}
	if strings.TrimSpace(bearerToken) != bearerToken || bearerToken == "" ||
		strings.ContainsAny(bearerToken, "\x00\r\n") {
		return nil, errors.New("h3 campaign API bearer token is invalid")
	}
	if client == nil {
		client = &http.Client{}
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &HTTPClient{baseURL: parsed, token: bearerToken, client: &copyClient}, nil
}

func (client *HTTPClient) SubmitJob(
	ctx context.Context,
	projectID uuid.UUID,
	idempotencyKey string,
	request api.SubmitJobRequest,
) (api.Job, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return api.Job{}, fmt.Errorf("encode H3 campaign Job request: %w", err)
	}
	return client.doJobRequest(
		ctx, http.MethodPost, projectID, uuid.Nil, idempotencyKey, encoded, http.StatusAccepted,
	)
}

func (client *HTTPClient) GetJob(
	ctx context.Context,
	projectID uuid.UUID,
	jobID uuid.UUID,
) (api.Job, error) {
	return client.doJobRequest(
		ctx, http.MethodGet, projectID, jobID, "", nil, http.StatusOK,
	)
}

func (client *HTTPClient) doJobRequest(
	ctx context.Context,
	method string,
	projectID uuid.UUID,
	jobID uuid.UUID,
	idempotencyKey string,
	body []byte,
	wantStatus int,
) (api.Job, error) {
	if ctx == nil || client == nil || client.baseURL == nil || client.client == nil ||
		projectID == uuid.Nil || (method == http.MethodGet && jobID == uuid.Nil) {
		return api.Job{}, errors.New("h3 campaign HTTP request is invalid")
	}
	requestURL := client.baseURL.JoinPath("v1", "projects", projectID.String(), "jobs")
	if jobID != uuid.Nil {
		requestURL = requestURL.JoinPath(jobID.String())
	}
	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), bytes.NewReader(body))
	if err != nil {
		return api.Job{}, fmt.Errorf("build H3 campaign API request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+client.token)
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := client.client.Do(request)
	if err != nil {
		return api.Job{}, fmt.Errorf("call H3 campaign API: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, maximumResponseBytes+1))
	if err != nil {
		return api.Job{}, fmt.Errorf("read H3 campaign API response: %w", err)
	}
	if len(encoded) > maximumResponseBytes {
		return api.Job{}, errors.New("h3 campaign API response exceeds size bound")
	}
	if response.StatusCode != wantStatus {
		return api.Job{}, decodeAPIError(response.StatusCode, encoded)
	}
	var job api.Job
	if err := decodeStrictJSON(encoded, &job); err != nil {
		return api.Job{}, fmt.Errorf("decode H3 campaign Job response: %w", err)
	}
	return job, nil
}

func decodeAPIError(statusCode int, encoded []byte) error {
	var response api.Error
	if err := decodeStrictJSON(encoded, &response); err == nil && response.Code != "" {
		return fmt.Errorf("vela API returned HTTP %d (%s): %s", statusCode, response.Code, response.Message)
	}
	return fmt.Errorf("vela API returned HTTP %d", statusCode)
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

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
