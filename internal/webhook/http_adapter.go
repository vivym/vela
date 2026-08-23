package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const maxWebhookResponseBytes = 64 * 1024

var forbiddenWebhookPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/128"),
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("100:0:0:1::/64"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("3fff::/20"),
	netip.MustParsePrefix("5f00::/16"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("fec0::/10"),
	netip.MustParsePrefix("ff00::/8"),
}

// IANA reserves 2001::/23 by default but assigns these globally reachable exceptions.
var publicWebhookExceptionPrefixes = []netip.Prefix{
	netip.MustParsePrefix("2001:1::1/128"),
	netip.MustParsePrefix("2001:1::2/128"),
	netip.MustParsePrefix("2001:1::3/128"),
	netip.MustParsePrefix("2001:3::/32"),
	netip.MustParsePrefix("2001:4:112::/48"),
	netip.MustParsePrefix("2001:20::/28"),
	netip.MustParsePrefix("2001:30::/28"),
}

type DeliveryRequest struct {
	Endpoint       string
	SubscriptionID uuid.UUID
	DeliveryID     uuid.UUID
	EventID        uuid.UUID
	ClaimedAt      time.Time
	Payload        []byte
	Secrets        [][]byte
}

type DeliveryAdapter interface {
	Deliver(context.Context, DeliveryRequest) (int, error)
}

type AddressResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type ContextDialer interface {
	DialContext(context.Context, string, string) (net.Conn, error)
}

type PublicAddressDialer struct {
	resolver AddressResolver
	dialer   ContextDialer
}

func NewPublicAddressDialer(
	resolver AddressResolver,
	dialer ContextDialer,
) (*PublicAddressDialer, error) {
	if resolver == nil {
		return nil, errors.New("webhook DNS resolver is required")
	}
	if dialer == nil {
		return nil, errors.New("webhook network dialer is required")
	}
	return &PublicAddressDialer{resolver: resolver, dialer: dialer}, nil
}

func (d *PublicAddressDialer) DialContext(
	ctx context.Context,
	network, address string,
) (net.Conn, error) {
	if d == nil || d.resolver == nil || d.dialer == nil {
		return nil, errors.New("webhook public-address dialer is not configured")
	}
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, errors.New("webhook endpoint requires a TCP connection")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || port == "" {
		return nil, errors.New("webhook endpoint dial address is invalid")
	}

	addresses := make([]netip.Addr, 0, 1)
	if literal, parseErr := netip.ParseAddr(host); parseErr == nil {
		addresses = append(addresses, literal)
	} else {
		addresses, err = resolvePublicWebhookAddresses(ctx, d.resolver, host)
		if err != nil {
			return nil, err
		}
	}
	for _, resolved := range addresses {
		if !isPublicWebhookNetIP(resolved) {
			return nil, errors.New("webhook endpoint resolved to a non-public address")
		}
	}

	var dialErrors []error
	for _, resolved := range addresses {
		connection, dialErr := d.dialer.DialContext(
			ctx,
			network,
			net.JoinHostPort(resolved.Unmap().String(), port),
		)
		if dialErr == nil {
			return connection, nil
		}
		dialErrors = append(dialErrors, dialErr)
		if ctx.Err() != nil {
			break
		}
	}
	return nil, errors.Join(dialErrors...)
}

func NewProductionHTTPClient(timeout time.Duration) (*http.Client, error) {
	if timeout <= 0 {
		return nil, errors.New("webhook HTTP timeout must be positive")
	}
	publicDialer, err := NewPublicAddressDialer(
		net.DefaultResolver,
		&net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second},
	)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = publicDialer.DialContext
	transport.ResponseHeaderTimeout = timeout
	transport.MaxResponseHeaderBytes = maxWebhookResponseBytes
	return &http.Client{Transport: transport, Timeout: timeout}, nil
}

type HTTPAdapter struct {
	client *http.Client
}

func NewHTTPAdapter(client *http.Client) (*HTTPAdapter, error) {
	if client == nil {
		return nil, errors.New("webhook HTTP client is required")
	}
	configured := *client
	configured.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &HTTPAdapter{client: &configured}, nil
}

func (a *HTTPAdapter) Deliver(ctx context.Context, delivery DeliveryRequest) (int, error) {
	if a == nil || a.client == nil {
		return 0, errors.New("webhook HTTP adapter is not configured")
	}
	if err := ValidateEndpoint(delivery.Endpoint); err != nil {
		return 0, err
	}
	if delivery.SubscriptionID == uuid.Nil || delivery.DeliveryID == uuid.Nil ||
		delivery.EventID == uuid.Nil {
		return 0, errors.New("webhook delivery identity is incomplete")
	}
	if delivery.ClaimedAt.IsZero() || len(delivery.Payload) == 0 {
		return 0, errors.New("webhook delivery timestamp and payload are required")
	}
	if len(delivery.Secrets) < 1 || len(delivery.Secrets) > 2 {
		return 0, errors.New("webhook delivery requires one or two signing secrets")
	}

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		delivery.Endpoint,
		strings.NewReader(string(delivery.Payload)),
	)
	if err != nil {
		return 0, fmt.Errorf("create webhook request: %w", err)
	}
	timestamp := strconv.FormatInt(delivery.ClaimedAt.Unix(), 10)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Vela-Webhook-Id", delivery.SubscriptionID.String())
	request.Header.Set("Vela-Delivery-Id", delivery.DeliveryID.String())
	request.Header.Set("Vela-Event-Id", delivery.EventID.String())
	request.Header.Set("Vela-Timestamp", timestamp)
	request.Header.Set(
		"Vela-Signature",
		signatureHeader(delivery.Secrets, timestamp, delivery.EventID, delivery.Payload),
	)

	response, err := a.client.Do(request)
	if err != nil {
		return 0, fmt.Errorf("send webhook request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	responseBytes, readErr := io.CopyN(io.Discard, response.Body, maxWebhookResponseBytes+1)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return response.StatusCode, fmt.Errorf("read webhook response: %w", readErr)
	}
	if responseBytes > maxWebhookResponseBytes {
		return response.StatusCode, errors.New("webhook response exceeds configured byte limit")
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return response.StatusCode, fmt.Errorf("webhook endpoint returned HTTP %d", response.StatusCode)
	}
	return response.StatusCode, nil
}

func ValidateEndpoint(endpoint string) error {
	parsed, err := url.ParseRequestURI(endpoint)
	if err != nil {
		return errors.New("webhook endpoint is not a valid absolute URL")
	}
	if parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" {
		return errors.New("webhook endpoint must use HTTPS with an explicit host")
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return errors.New("webhook endpoint cannot contain userinfo or a fragment")
	}
	hostname := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if hostname == "localhost" || strings.HasSuffix(hostname, ".localhost") {
		return errors.New("webhook endpoint cannot target localhost")
	}
	if address, parseErr := netip.ParseAddr(hostname); parseErr == nil &&
		!isPublicWebhookNetIP(address) {
		return errors.New("webhook endpoint cannot target a non-public address")
	}
	return nil
}

func validateEndpointForRegistration(
	ctx context.Context,
	endpoint string,
	resolver AddressResolver,
) error {
	if err := ValidateEndpoint(endpoint); err != nil {
		return err
	}
	parsed, _ := url.ParseRequestURI(endpoint)
	hostname := strings.TrimSuffix(strings.ToLower(parsed.Hostname()), ".")
	if _, err := netip.ParseAddr(hostname); err == nil {
		return nil
	}
	_, err := resolvePublicWebhookAddresses(ctx, resolver, hostname)
	return err
}

func resolvePublicWebhookAddresses(
	ctx context.Context,
	resolver AddressResolver,
	hostname string,
) ([]netip.Addr, error) {
	if resolver == nil {
		return nil, errors.New("webhook DNS resolver is required")
	}
	addresses, err := resolver.LookupNetIP(ctx, "ip", hostname)
	if err != nil {
		return nil, fmt.Errorf("resolve webhook endpoint: %w", err)
	}
	if len(addresses) == 0 {
		return nil, errors.New("webhook endpoint resolved to no addresses")
	}
	for _, resolved := range addresses {
		if !isPublicWebhookNetIP(resolved) {
			return nil, errors.New("webhook endpoint resolved to a non-public address")
		}
	}
	return addresses, nil
}

func signatureHeader(secrets [][]byte, timestamp string, eventID uuid.UUID, payload []byte) string {
	signed := make([]byte, 0, len(timestamp)+1+36+1+len(payload))
	signed = append(signed, timestamp...)
	signed = append(signed, '.')
	signed = append(signed, eventID.String()...)
	signed = append(signed, '.')
	signed = append(signed, payload...)

	signatures := make([]string, 0, len(secrets))
	for _, secret := range secrets {
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write(signed)
		signatures = append(signatures, "v1="+hex.EncodeToString(mac.Sum(nil)))
	}
	return strings.Join(signatures, ",")
}

func isPublicWebhookNetIP(parsed netip.Addr) bool {
	if !parsed.IsValid() || parsed.Zone() != "" {
		return false
	}
	parsed = parsed.Unmap()
	if !parsed.IsGlobalUnicast() || parsed.IsPrivate() || parsed.IsLoopback() ||
		parsed.IsLinkLocalUnicast() || parsed.IsMulticast() || parsed.IsUnspecified() {
		return false
	}
	for _, prefix := range publicWebhookExceptionPrefixes {
		if prefix.Contains(parsed) {
			return true
		}
	}
	for _, prefix := range forbiddenWebhookPrefixes {
		if prefix.Contains(parsed) {
			return false
		}
	}
	return true
}
