package identity

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

var ErrInvalidOIDCToken = errors.New("invalid human OIDC token")

type OIDCIdentity struct {
	Issuer    string
	Subject   string
	ExpiresAt time.Time
}

type OIDCTokenVerifier interface {
	Verify(context.Context, string) (OIDCIdentity, error)
}

type OIDCVerifierConfig struct {
	Issuer     string
	Audience   string
	JWKSURL    string
	HTTPClient *http.Client
}

type RemoteOIDCTokenVerifier struct {
	verifier   *oidc.IDTokenVerifier
	httpClient *http.Client
}

func NewRemoteOIDCTokenVerifier(config OIDCVerifierConfig) (*RemoteOIDCTokenVerifier, error) {
	issuer, err := validateOIDCEndpoint("issuer", config.Issuer, false)
	if err != nil {
		return nil, err
	}
	jwksURL, err := validateOIDCEndpoint("JWKS URL", config.JWKSURL, true)
	if err != nil {
		return nil, err
	}
	audience := strings.TrimSpace(config.Audience)
	if audience == "" || len(audience) > 500 {
		return nil, errors.New("OIDC audience is required and must not exceed 500 bytes")
	}
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	httpClient = oidcHTTPSOnlyClient(httpClient)
	keyContext := oidc.ClientContext(context.Background(), httpClient)
	keySet := oidc.NewRemoteKeySet(keyContext, jwksURL)
	return &RemoteOIDCTokenVerifier{
		verifier: oidc.NewVerifier(issuer, keySet, &oidc.Config{
			ClientID:             audience,
			SupportedSigningAlgs: []string{oidc.RS256},
		}),
		httpClient: httpClient,
	}, nil
}

func oidcHTTPSOnlyClient(client *http.Client) *http.Client {
	secured := *client
	upstreamCheckRedirect := client.CheckRedirect
	secured.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if !strings.EqualFold(request.URL.Scheme, "https") {
			return errors.New("OIDC endpoint redirect must remain HTTPS")
		}
		if upstreamCheckRedirect != nil {
			return upstreamCheckRedirect(request, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	return &secured
}

func (v *RemoteOIDCTokenVerifier) Verify(
	ctx context.Context,
	rawToken string,
) (OIDCIdentity, error) {
	if v == nil || v.verifier == nil || v.httpClient == nil || ctx == nil || rawToken == "" {
		return OIDCIdentity{}, ErrInvalidOIDCToken
	}
	verified, err := v.verifier.Verify(oidc.ClientContext(ctx, v.httpClient), rawToken)
	if err != nil {
		return OIDCIdentity{}, fmt.Errorf("%w: %v", ErrInvalidOIDCToken, err)
	}
	if verified.Issuer == "" || verified.Subject == "" || len(verified.Subject) > 500 ||
		verified.Expiry.IsZero() {
		return OIDCIdentity{}, ErrInvalidOIDCToken
	}
	return OIDCIdentity{
		Issuer: verified.Issuer, Subject: verified.Subject, ExpiresAt: verified.Expiry.UTC(),
	}, nil
}

func validateOIDCEndpoint(label, raw string, allowQuery bool) (string, error) {
	if strings.TrimSpace(raw) != raw || raw == "" || len(raw) > 2048 {
		return "", fmt.Errorf("OIDC %s is invalid", label)
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil ||
		parsed.Fragment != "" || (!allowQuery && parsed.RawQuery != "") {
		return "", fmt.Errorf("OIDC %s must be an absolute HTTPS URL", label)
	}
	return parsed.String(), nil
}
