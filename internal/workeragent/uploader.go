package workeragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workertransport"
)

type HTTPArtifactPartUploaderConfig struct {
	AllowHTTP bool
	Timeout   time.Duration
	Clock     func() time.Time
}

type HTTPArtifactPartUploader struct {
	client    *http.Client
	allowHTTP bool
	clock     func() time.Time
}

func NewHTTPArtifactPartUploader(
	config HTTPArtifactPartUploaderConfig,
) (*HTTPArtifactPartUploader, error) {
	if config.Timeout < time.Second || config.Timeout > 10*time.Minute {
		return nil, errors.New("artifact part upload timeout must be between one second and ten minutes")
	}
	clock := config.Clock
	if clock == nil {
		clock = time.Now
	}
	return &HTTPArtifactPartUploader{
		client: &http.Client{
			Timeout: config.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		allowHTTP: config.AllowHTTP,
		clock:     clock,
	}, nil
}

func (uploader *HTTPArtifactPartUploader) Upload(
	ctx context.Context,
	signed workertransport.SignedArtifactUploadPart,
	payload []byte,
) (workercontrol.ArtifactUploadPart, error) {
	if uploader == nil || uploader.client == nil || uploader.clock == nil || ctx == nil {
		return workercontrol.ArtifactUploadPart{}, errors.New("artifact part uploader is not configured")
	}
	digest := sha256.Sum256(payload)
	checksum := base64.StdEncoding.EncodeToString(digest[:])
	parsedURL, err := url.Parse(signed.URL)
	if err != nil || parsedURL.Host == "" || parsedURL.User != nil || parsedURL.Fragment != "" ||
		(parsedURL.Scheme != "https" && (!uploader.allowHTTP || parsedURL.Scheme != "http")) ||
		signed.Number <= 0 || signed.Number > 10_000 || signed.SizeBytes != int64(len(payload)) ||
		signed.SizeBytes <= 0 || signed.SHA256 != digest || !signed.ExpiresAt.After(uploader.clock()) {
		return workercontrol.ArtifactUploadPart{}, errors.New("signed Artifact upload part is invalid or expired")
	}
	headers, err := validatedUploadHeaders(signed.RequiredHeaders, signed.SizeBytes, checksum)
	if err != nil {
		return workercontrol.ArtifactUploadPart{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, signed.URL, bytes.NewReader(payload))
	if err != nil {
		return workercontrol.ArtifactUploadPart{}, fmt.Errorf("create signed Artifact PUT: %w", err)
	}
	request.ContentLength = signed.SizeBytes
	for name, values := range headers {
		if name == "Content-Length" {
			continue
		}
		request.Header[name] = append([]string(nil), values...)
	}
	response, err := uploader.client.Do(request)
	if err != nil {
		return workercontrol.ArtifactUploadPart{}, fmt.Errorf("execute signed Artifact PUT: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, 4097))
	if readErr != nil {
		return workercontrol.ArtifactUploadPart{}, fmt.Errorf("read Artifact PUT response: %w", readErr)
	}
	if len(body) > 4096 {
		return workercontrol.ArtifactUploadPart{}, errors.New("artifact PUT response is too large")
	}
	if response.StatusCode != http.StatusOK {
		return workercontrol.ArtifactUploadPart{}, fmt.Errorf("artifact PUT returned %s", response.Status)
	}
	etag := response.Header.Get("ETag")
	if etag == "" || len(etag) > 1000 || strings.ContainsRune(etag, '\x00') {
		return workercontrol.ArtifactUploadPart{}, errors.New("artifact PUT response omitted a valid ETag")
	}
	if responseChecksum := response.Header.Get("X-Amz-Checksum-Sha256"); responseChecksum != "" && responseChecksum != checksum {
		return workercontrol.ArtifactUploadPart{}, errors.New("artifact PUT response checksum does not match")
	}
	return workercontrol.ArtifactUploadPart{
		Number: signed.Number, ETag: etag, SizeBytes: signed.SizeBytes,
		ChecksumSHA256: checksum,
	}, nil
}

func validatedUploadHeaders(
	required map[string]string,
	sizeBytes int64,
	checksum string,
) (http.Header, error) {
	if len(required) == 0 || len(required) > 32 {
		return nil, errors.New("signed Artifact upload headers are invalid")
	}
	forbidden := map[string]struct{}{
		"Authorization": {}, "Cookie": {}, "Host": {}, "Proxy-Authorization": {},
		"Connection": {}, "Transfer-Encoding": {}, "Upgrade": {},
	}
	headers := make(http.Header, len(required))
	for name, value := range required {
		canonical := http.CanonicalHeaderKey(strings.TrimSpace(name))
		if canonical == "" || value == "" || strings.ContainsRune(value, '\x00') {
			return nil, errors.New("signed Artifact upload headers are invalid")
		}
		if _, blocked := forbidden[canonical]; blocked {
			return nil, errors.New("signed Artifact upload headers contain a forbidden field")
		}
		if _, duplicate := headers[canonical]; duplicate {
			return nil, errors.New("signed Artifact upload headers contain a duplicate field")
		}
		headers.Set(canonical, value)
	}
	if headers.Get("Content-Length") != strconv.FormatInt(sizeBytes, 10) ||
		headers.Get("X-Amz-Checksum-Sha256") != checksum {
		return nil, errors.New("signed Artifact upload headers do not bind the payload")
	}
	return headers, nil
}

var _ ArtifactPartUploader = (*HTTPArtifactPartUploader)(nil)
