package billingexport_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/billingexport"
)

func TestHTTPAdapterFailsClosedOnConfigurationAndResponseDrift(t *testing.T) {
	validClient := &http.Client{
		Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("invalid HTTP Invoice adapter configuration reached transport")
			return nil, nil
		}),
		Timeout: time.Second,
	}
	for _, test := range []struct {
		name   string
		client *http.Client
		config billingexport.HTTPConfig
	}{
		{
			name:   "HTTP endpoint",
			client: validClient,
			config: billingexport.HTTPConfig{Endpoint: "http://finance.example/lines", BearerToken: "token"},
		},
		{
			name:   "endpoint credentials",
			client: validClient,
			config: billingexport.HTTPConfig{Endpoint: "https://user@finance.example/lines", BearerToken: "token"},
		},
		{
			name:   "endpoint query",
			client: validClient,
			config: billingexport.HTTPConfig{Endpoint: "https://finance.example/lines?mode=test", BearerToken: "token"},
		},
		{
			name:   "unbounded client",
			client: &http.Client{},
			config: billingexport.HTTPConfig{Endpoint: "https://finance.example/lines", BearerToken: "token"},
		},
		{
			name:   "excessive timeout",
			client: &http.Client{Timeout: time.Minute + time.Second},
			config: billingexport.HTTPConfig{Endpoint: "https://finance.example/lines", BearerToken: "token"},
		},
		{
			name:   "multiline token",
			client: validClient,
			config: billingexport.HTTPConfig{Endpoint: "https://finance.example/lines", BearerToken: "token\nnext"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := billingexport.NewHTTPAdapter(test.client, test.config); err == nil {
				t.Fatal("invalid HTTP Invoice adapter configuration was accepted")
			}
		})
	}

	responses := []struct {
		name        string
		status      int
		contentType string
		body        string
	}{
		{name: "redirect", status: http.StatusTemporaryRedirect, contentType: "application/json", body: `{}`},
		{name: "server error", status: http.StatusServiceUnavailable, contentType: "application/json", body: `{}`},
		{name: "wrong media type", status: http.StatusCreated, contentType: "text/html", body: `{}`},
		{name: "unknown receipt field", status: http.StatusCreated, contentType: "application/json", body: `{"invoice_reference":"invoice","line_reference":"line","drift":true}`},
		{name: "missing line reference", status: http.StatusCreated, contentType: "application/json", body: `{"invoice_reference":"invoice","line_reference":""}`},
		{
			name:        "oversized trailing document",
			status:      http.StatusCreated,
			contentType: "application/json",
			body: `{"invoice_reference":"invoice","line_reference":"line"}` +
				strings.Repeat(" ", 64*1024) + `{"second":true}`,
		},
	}
	for _, test := range responses {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			adapter, err := billingexport.NewHTTPAdapter(
				&http.Client{
					Timeout: time.Second,
					Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
						calls++
						return &http.Response{
							StatusCode: test.status,
							Header:     http.Header{"Content-Type": []string{test.contentType}},
							Body:       io.NopCloser(strings.NewReader(test.body)),
							Request:    request,
						}, nil
					}),
				},
				billingexport.HTTPConfig{
					Endpoint:    "https://finance.example/v1/invoice-lines",
					BearerToken: "token",
				},
			)
			if err != nil {
				t.Fatalf("configure response-drift adapter: %v", err)
			}
			if _, err := adapter.ExportLine(context.Background(), validHTTPLine()); err == nil {
				t.Fatal("response drift was accepted")
			}
			if calls != 1 {
				t.Fatalf("response-drift transport calls = %d, want 1", calls)
			}
		})
	}
}

func TestHTTPAdapterExportsLineWithChargeIDIdempotency(t *testing.T) {
	chargeID := uuid.MustParse("00000000-0000-0000-0000-000000000901")
	postedAt := time.Date(
		2026,
		time.August,
		23,
		10,
		30,
		45,
		123000000,
		time.FixedZone("Asia/Shanghai", 8*60*60),
	)
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost ||
			request.URL.String() != "https://finance.example/v1/invoice-lines" {
			t.Fatalf("Invoice request target = %s %s", request.Method, request.URL)
		}
		if request.Header.Get("Authorization") != "Bearer test-finance-token" ||
			request.Header.Get("Idempotency-Key") != chargeID.String() ||
			request.Header.Get("Content-Type") != "application/json" ||
			request.Header.Get("Accept") != "application/json" {
			t.Fatalf("Invoice request headers = %#v", request.Header)
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatalf("read Invoice request body: %v", err)
		}
		const expected = `{"charge_id":"00000000-0000-0000-0000-000000000901","organization_id":"00000000-0000-0000-0000-000000000001","project_id":"00000000-0000-0000-0000-000000000002","job_id":"00000000-0000-0000-0000-000000000903","reason":"VISIBLE_COMPLETION","amount_minor":1250,"currency":"CNY","posted_at":"2026-08-23T02:30:45.123Z"}`
		if string(body) != expected+"\n" {
			t.Fatalf("Invoice request body = %s", body)
		}
		return &http.Response{
			StatusCode: http.StatusCreated,
			Header: http.Header{
				"Content-Type": []string{"application/json"},
			},
			Body: io.NopCloser(strings.NewReader(
				`{"invoice_reference":"invoice-2026-08-org-1","line_reference":"line-901"}`,
			)),
			Request: request,
		}, nil
	})
	adapter, err := billingexport.NewHTTPAdapter(
		&http.Client{Transport: transport, Timeout: 5 * time.Second},
		billingexport.HTTPConfig{
			Endpoint:    "https://finance.example/v1/invoice-lines",
			BearerToken: "test-finance-token",
		},
	)
	if err != nil {
		t.Fatalf("configure HTTP Invoice adapter: %v", err)
	}

	receipt, err := adapter.ExportLine(context.Background(), billingexport.Line{
		ChargeID:       chargeID,
		IdempotencyKey: chargeID.String(),
		OrganizationID: uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		ProjectID:      uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		JobID:          uuid.MustParse("00000000-0000-0000-0000-000000000903"),
		Reason:         "VISIBLE_COMPLETION",
		AmountMinor:    1250,
		Currency:       "CNY",
		PostedAt:       postedAt,
	})
	if err != nil {
		t.Fatalf("export HTTP Invoice line: %v", err)
	}
	if receipt.InvoiceReference != "invoice-2026-08-org-1" ||
		receipt.LineReference != "line-901" {
		t.Fatalf("HTTP Invoice receipt = %#v", receipt)
	}
}

func validHTTPLine() billingexport.Line {
	chargeID := uuid.MustParse("00000000-0000-0000-0000-000000000901")
	return billingexport.Line{
		ChargeID:       chargeID,
		IdempotencyKey: chargeID.String(),
		OrganizationID: uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		ProjectID:      uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		JobID:          uuid.MustParse("00000000-0000-0000-0000-000000000903"),
		Reason:         "VISIBLE_COMPLETION",
		AmountMinor:    1250,
		Currency:       "CNY",
		PostedAt:       time.Date(2026, time.August, 23, 2, 30, 45, 123000000, time.UTC),
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}
