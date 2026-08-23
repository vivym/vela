package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestHTTPAdapterSignsExactPayloadWithOverlappingSecrets(t *testing.T) {
	subscriptionID := uuid.MustParse("10000000-0000-0000-0000-000000000001")
	deliveryID := uuid.MustParse("10000000-0000-0000-0000-000000000002")
	eventID := uuid.MustParse("10000000-0000-0000-0000-000000000003")
	claimedAt := time.Date(2026, 8, 23, 11, 22, 33, 0, time.UTC)
	payload := []byte(`{"schema_version":1,"event_id":"10000000-0000-0000-0000-000000000003"}`)
	secrets := [][]byte{
		[]byte("vwhsec_current-secret"),
		[]byte("vwhsec_previous-secret"),
	}

	transport := &recordingWebhookTransport{}
	adapter, err := NewHTTPAdapter(&http.Client{Transport: transport})
	if err != nil {
		t.Fatalf("configure HTTP adapter: %v", err)
	}
	statusCode, err := adapter.Deliver(context.Background(), DeliveryRequest{
		Endpoint:       "https://hooks.example.com/vela",
		SubscriptionID: subscriptionID,
		DeliveryID:     deliveryID,
		EventID:        eventID,
		ClaimedAt:      claimedAt,
		Payload:        payload,
		Secrets:        secrets,
	})
	if err != nil {
		t.Fatalf("deliver webhook: %v", err)
	}
	if statusCode != http.StatusNoContent {
		t.Fatalf("status code = %d, want 204", statusCode)
	}
	request := transport.request
	if request == nil {
		t.Fatal("HTTP request was not sent")
	}
	requestBody, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatalf("read request body: %v", err)
	}
	if string(requestBody) != string(payload) || request.Method != http.MethodPost ||
		request.URL.String() != "https://hooks.example.com/vela" ||
		request.Header.Get("Content-Type") != "application/json" ||
		request.Header.Get("Vela-Webhook-Id") != subscriptionID.String() ||
		request.Header.Get("Vela-Delivery-Id") != deliveryID.String() ||
		request.Header.Get("Vela-Event-Id") != eventID.String() ||
		request.Header.Get("Vela-Timestamp") != "1787484153" {
		t.Fatalf("request = %#v body=%s", request, requestBody)
	}

	signedPayload := "1787484153." + eventID.String() + "." + string(payload)
	wantSignatures := make([]string, 0, len(secrets))
	for _, secret := range secrets {
		mac := hmac.New(sha256.New, secret)
		_, _ = mac.Write([]byte(signedPayload))
		wantSignatures = append(wantSignatures, "v1="+hex.EncodeToString(mac.Sum(nil)))
	}
	if got, want := request.Header.Get("Vela-Signature"), strings.Join(wantSignatures, ","); got != want {
		t.Fatalf("Vela-Signature = %q, want %q", got, want)
	}
}

func TestHTTPAdapterRejectsOversizedSuccessfulResponse(t *testing.T) {
	transport := &staticWebhookResponseTransport{
		statusCode: http.StatusNoContent,
		body:       strings.Repeat("x", maxWebhookResponseBytes+1),
	}
	adapter, err := NewHTTPAdapter(&http.Client{Transport: transport})
	if err != nil {
		t.Fatalf("configure HTTP adapter: %v", err)
	}
	statusCode, err := adapter.Deliver(context.Background(), DeliveryRequest{
		Endpoint:       "https://hooks.example.com/oversized-response",
		SubscriptionID: uuid.MustParse("20000000-0000-0000-0000-000000000001"),
		DeliveryID:     uuid.MustParse("20000000-0000-0000-0000-000000000002"),
		EventID:        uuid.MustParse("20000000-0000-0000-0000-000000000003"),
		ClaimedAt:      time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC),
		Payload:        []byte(`{"schema_version":1}`),
		Secrets:        [][]byte{[]byte("vwhsec_response-boundary")},
	})
	if statusCode != http.StatusNoContent || err == nil ||
		!strings.Contains(err.Error(), "response exceeds") {
		t.Fatalf("oversized response status=%d error=%v", statusCode, err)
	}
}

func TestValidateEndpointRejectsEveryNonPublicAddressClass(t *testing.T) {
	for _, endpoint := range []string{
		"https://127.0.0.1/hook",
		"https://10.0.0.1/hook",
		"https://100.64.0.1/hook",
		"https://169.254.169.254/latest/meta-data",
		"https://192.0.2.1/hook",
		"https://198.18.0.1/hook",
		"https://198.51.100.1/hook",
		"https://203.0.113.1/hook",
		"https://192.88.99.1/hook",
		"https://[::1]/hook",
		"https://[fe80::1%25en0]/hook",
		"https://[64:ff9b:1::1]/hook",
		"https://[100:0:0:1::1]/hook",
		"https://[2001:2::1]/hook",
		"https://[2001:5::1]/hook",
		"https://[fc00::1]/hook",
		"https://[fec0::1]/hook",
		"https://[2001:db8::1]/hook",
		"https://[2002::1]/hook",
		"https://[3fff::1]/hook",
		"https://[5f00::1]/hook",
	} {
		t.Run(endpoint, func(t *testing.T) {
			if err := ValidateEndpoint(endpoint); err == nil {
				t.Fatalf("ValidateEndpoint(%q) succeeded", endpoint)
			}
		})
	}
}

func TestValidateEndpointAcceptsGloballyReachableIANAExceptions(t *testing.T) {
	for _, endpoint := range []string{
		"https://[2001:1::1]/hook",
		"https://[2001:1::2]/hook",
		"https://[2001:1::3]/hook",
		"https://[2001:3::1]/hook",
		"https://[2001:4:112::1]/hook",
		"https://[2001:20::1]/hook",
		"https://[2001:30::1]/hook",
	} {
		t.Run(endpoint, func(t *testing.T) {
			if err := ValidateEndpoint(endpoint); err != nil {
				t.Fatalf("ValidateEndpoint(%q) error = %v", endpoint, err)
			}
		})
	}
}

func TestPublicAddressDialerFailsClosedBeforeConnectingToMixedResolution(t *testing.T) {
	resolver := &staticAddressResolver{addresses: []netip.Addr{
		netip.MustParseAddr("93.184.216.34"),
		netip.MustParseAddr("10.20.30.40"),
	}}
	dialer := &recordingContextDialer{err: errors.New("dial sentinel")}
	publicDialer, err := NewPublicAddressDialer(resolver, dialer)
	if err != nil {
		t.Fatalf("configure public-address dialer: %v", err)
	}
	connection, err := publicDialer.DialContext(context.Background(), "tcp", "hooks.example.com:443")
	if connection != nil {
		_ = connection.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "non-public") {
		t.Fatalf("DialContext error = %v, want non-public rejection", err)
	}
	if dialer.calls != 0 {
		t.Fatalf("underlying dialer calls = %d, want 0", dialer.calls)
	}
}

func TestPublicAddressDialerConnectsOnlyToValidatedResolvedAddress(t *testing.T) {
	resolver := &staticAddressResolver{addresses: []netip.Addr{
		netip.MustParseAddr("93.184.216.34"),
	}}
	dialer := &recordingContextDialer{err: errors.New("dial sentinel")}
	publicDialer, err := NewPublicAddressDialer(resolver, dialer)
	if err != nil {
		t.Fatalf("configure public-address dialer: %v", err)
	}
	connection, err := publicDialer.DialContext(context.Background(), "tcp", "hooks.example.com:443")
	if connection != nil {
		_ = connection.Close()
	}
	if !errors.Is(err, dialer.err) {
		t.Fatalf("DialContext error = %v, want dial sentinel", err)
	}
	if dialer.calls != 1 || dialer.address != "93.184.216.34:443" {
		t.Fatalf("underlying dial = calls %d address %q", dialer.calls, dialer.address)
	}
}

type recordingWebhookTransport struct {
	request *http.Request
}

type staticWebhookResponseTransport struct {
	statusCode int
	body       string
}

type staticAddressResolver struct {
	addresses []netip.Addr
	err       error
}

func (r *staticAddressResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return append([]netip.Addr(nil), r.addresses...), r.err
}

type recordingContextDialer struct {
	calls   int
	address string
	err     error
}

func (d *recordingContextDialer) DialContext(
	_ context.Context,
	_, address string,
) (net.Conn, error) {
	d.calls++
	d.address = address
	return nil, d.err
}

func (t *recordingWebhookTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.request = request
	return &http.Response{
		StatusCode: http.StatusNoContent,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("ignored")),
		Request:    request,
	}, nil
}

func (t *staticWebhookResponseTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: t.statusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(t.body)),
		Request:    request,
	}, nil
}
