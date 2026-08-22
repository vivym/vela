package billingexport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

const maxInvoiceResponseBytes = 64 * 1024

var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

type HTTPConfig struct {
	Endpoint    string
	BearerToken string
}

type HTTPAdapter struct {
	client      *http.Client
	endpoint    string
	bearerToken string
}

type httpLine struct {
	ChargeID       string    `json:"charge_id"`
	OrganizationID string    `json:"organization_id"`
	ProjectID      string    `json:"project_id"`
	JobID          string    `json:"job_id"`
	Reason         string    `json:"reason"`
	AmountMinor    int64     `json:"amount_minor"`
	Currency       string    `json:"currency"`
	PostedAt       time.Time `json:"posted_at"`
}

type httpReceipt struct {
	InvoiceReference string `json:"invoice_reference"`
	LineReference    string `json:"line_reference"`
}

func NewHTTPAdapter(client *http.Client, config HTTPConfig) (*HTTPAdapter, error) {
	if client == nil {
		return nil, errors.New("HTTP Invoice adapter client is required")
	}
	if client.Timeout <= 0 || client.Timeout > time.Minute {
		return nil, errors.New("HTTP Invoice adapter client timeout must be in (0, 1m]")
	}
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.User != nil ||
		endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, errors.New("HTTP Invoice adapter endpoint must be an HTTPS URL without credentials, query, or fragment")
	}
	if config.BearerToken == "" || len(config.BearerToken) > 8192 ||
		strings.TrimSpace(config.BearerToken) != config.BearerToken ||
		strings.ContainsAny(config.BearerToken, "\x00\r\n") {
		return nil, errors.New("HTTP Invoice adapter bearer token is empty or invalid")
	}
	boundedClient := *client
	boundedClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &HTTPAdapter{
		client:      &boundedClient,
		endpoint:    endpoint.String(),
		bearerToken: config.BearerToken,
	}, nil
}

func (a *HTTPAdapter) ExportLine(ctx context.Context, line Line) (Receipt, error) {
	if a == nil || a.client == nil {
		return Receipt{}, errors.New("HTTP Invoice adapter is not configured")
	}
	if err := validateLine(line); err != nil {
		return Receipt{}, err
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(httpLine{
		ChargeID:       line.ChargeID.String(),
		OrganizationID: line.OrganizationID.String(),
		ProjectID:      line.ProjectID.String(),
		JobID:          line.JobID.String(),
		Reason:         line.Reason,
		AmountMinor:    line.AmountMinor,
		Currency:       line.Currency,
		PostedAt:       line.PostedAt.UTC(),
	}); err != nil {
		return Receipt{}, fmt.Errorf("encode Invoice line: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, &body)
	if err != nil {
		return Receipt{}, fmt.Errorf("create Invoice export request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+a.bearerToken)
	request.Header.Set("Idempotency-Key", line.IdempotencyKey)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	response, err := a.client.Do(request)
	if err != nil {
		return Receipt{}, fmt.Errorf("send Invoice export request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxInvoiceResponseBytes))
		return Receipt{}, fmt.Errorf("invoice export returned HTTP %d", response.StatusCode)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return Receipt{}, errors.New("invoice export response is not application/json")
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxInvoiceResponseBytes+1))
	if err != nil {
		return Receipt{}, fmt.Errorf("read Invoice export receipt: %w", err)
	}
	if len(responseBody) > maxInvoiceResponseBytes {
		return Receipt{}, errors.New("invoice export response exceeds the size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(responseBody))
	decoder.DisallowUnknownFields()
	var external httpReceipt
	if err := decoder.Decode(&external); err != nil {
		return Receipt{}, fmt.Errorf("decode Invoice export receipt: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Receipt{}, errors.New("invoice export response must contain one JSON document")
	}
	receipt := Receipt(external)
	if err := validateReceipt(receipt); err != nil {
		return Receipt{}, err
	}
	return receipt, nil
}

func validateLine(line Line) error {
	if line.ChargeID == uuid.Nil || line.OrganizationID == uuid.Nil || line.ProjectID == uuid.Nil ||
		line.JobID == uuid.Nil || line.IdempotencyKey != line.ChargeID.String() {
		return errors.New("invoice line identity or idempotency key is invalid")
	}
	if line.AmountMinor < 0 || !currencyPattern.MatchString(line.Currency) || line.PostedAt.IsZero() {
		return errors.New("invoice line amount, currency, or posting time is invalid")
	}
	if line.Reason != "VISIBLE_COMPLETION" && line.Reason != "CUSTOMER_CANCELLATION" {
		return errors.New("invoice line Charge reason is invalid")
	}
	return nil
}
