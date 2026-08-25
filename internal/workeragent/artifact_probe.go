package workeragent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/vivym/vela/internal/securefile"
)

const maxArtifactStoreCABytes = 4 << 20

type HTTPArtifactStoreProbeConfig struct {
	URL       string
	CAFile    string
	Timeout   time.Duration
	AllowHTTP bool
}

type HTTPArtifactStoreProbe struct {
	client *http.Client
	url    string
}

func NewHTTPArtifactStoreProbe(config HTTPArtifactStoreProbeConfig) (*HTTPArtifactStoreProbe, error) {
	parsed, err := url.Parse(config.URL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" ||
		parsed.RawQuery != "" ||
		(parsed.Scheme != "https" && (!config.AllowHTTP || parsed.Scheme != "http")) {
		return nil, errors.New("artifact-store health URL is invalid")
	}
	if config.Timeout < 10*time.Millisecond || config.Timeout > 30*time.Second {
		return nil, errors.New("artifact-store probe timeout must be between 10ms and 30s")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	if parsed.Scheme == "https" {
		caPEM, readErr := securefile.Read(config.CAFile, maxArtifactStoreCABytes, false)
		if readErr != nil {
			return nil, errors.New("read Artifact-store health CA file")
		}
		rootCAs := x509.NewCertPool()
		if !rootCAs.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("artifact-store health CA contains no certificates")
		}
		transport.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS13,
			RootCAs:    rootCAs,
		}
	} else if config.CAFile != "" {
		return nil, errors.New("development HTTP Artifact-store probe must not configure a CA file")
	}
	return &HTTPArtifactStoreProbe{
		client: &http.Client{
			Transport: transport,
			Timeout:   config.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		url: parsed.String(),
	}, nil
}

func (probe *HTTPArtifactStoreProbe) Reachable(ctx context.Context) bool {
	if probe == nil || probe.client == nil || probe.url == "" || ctx == nil {
		return false
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, probe.url, nil)
	if err != nil {
		return false
	}
	response, err := probe.client.Do(request)
	if err != nil {
		return false
	}
	defer func() { _ = response.Body.Close() }()
	return response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices
}
