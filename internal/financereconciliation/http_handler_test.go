package financereconciliation

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestHTTPHandlerAcceptsAuthenticatedStrictReconciliation(t *testing.T) {
	identityURI := mustParseTestURL(t, "spiffe://finance.internal/reconciliation/primary")
	postedAt := time.Date(2026, 8, 24, 14, 0, 0, 0, time.UTC)
	applier := &recordingApplier{
		identity: Identity{
			PrincipalID:    uuid.MustParse("00000000-0000-0000-0000-000000001901"),
			StableID:       "primary-finance-reconciliation",
			TLSURIIdentity: identityURI.String(),
		},
		result: Result{
			RecordID:                 uuid.MustParse("00000000-0000-0000-0000-000000001902"),
			OrganizationID:           uuid.MustParse("00000000-0000-0000-0000-000000001903"),
			Kind:                     KindSettlementPosted,
			Currency:                 "CNY",
			ContractCreditLimitMinor: 100000,
			UnsettledPostedMinor:     750,
			AccountVersion:           9,
			PostedAt:                 postedAt,
		},
	}
	handler, err := NewHTTPHandler(applier)
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	body := []byte(`{
		"idempotency_key":"settlement-http-1901",
		"source_sequence":1,
		"organization_id":"00000000-0000-0000-0000-000000001903",
		"kind":"SETTLEMENT_POSTED",
		"currency":"CNY",
		"settlement_minor":500,
		"external_reference":"payment-http-1901",
		"effective_at":"2026-08-24T12:00:00Z"
	}`)
	request := httptest.NewRequest(
		http.MethodPost,
		"https://finance-listener.internal/internal/v1/finance/reconciliations",
		bytes.NewReader(body),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.TLS = verifiedTestTLSState(identityURI)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusCreated || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("Finance HTTP response = %d %s; body=%s", response.Code, response.Header(), response.Body.String())
	}
	if len(applier.requests) != 1 {
		t.Fatalf("Finance HTTP Apply calls = %d, want 1", len(applier.requests))
	}
	applied := applier.requests[0]
	if applied.IdempotencyKey != "settlement-http-1901" || applied.SourceSequence != 1 ||
		applied.OrganizationID != applier.result.OrganizationID ||
		applied.SettlementMinor == nil || *applied.SettlementMinor != 500 ||
		!applied.EffectiveAt.Equal(time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("Finance HTTP Apply request = %#v", applied)
	}
	var decoded struct {
		RecordID                 string    `json:"record_id"`
		Replayed                 bool      `json:"replayed"`
		Kind                     string    `json:"kind"`
		OrganizationID           string    `json:"organization_id"`
		Currency                 string    `json:"currency"`
		ContractCreditLimitMinor int64     `json:"contract_credit_limit_minor"`
		UnsettledPostedMinor     int64     `json:"unsettled_posted_minor"`
		AccountVersion           int64     `json:"account_version"`
		PostedAt                 time.Time `json:"posted_at"`
	}
	decoder := json.NewDecoder(response.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("decode Finance HTTP response: %v", err)
	}
	if decoded.RecordID != applier.result.RecordID.String() || decoded.Replayed ||
		decoded.Kind != string(KindSettlementPosted) ||
		decoded.OrganizationID != applier.result.OrganizationID.String() ||
		decoded.Currency != "CNY" || decoded.ContractCreditLimitMinor != 100000 ||
		decoded.UnsettledPostedMinor != 750 || decoded.AccountVersion != 9 ||
		!decoded.PostedAt.Equal(postedAt) {
		t.Fatalf("Finance HTTP response body = %#v", decoded)
	}
}

func TestHTTPHandlerRejectsUnauthenticatedOrMalformedRequestsBeforeApply(t *testing.T) {
	expectedURI := mustParseTestURL(t, "spiffe://finance.internal/reconciliation/primary")
	otherURI := mustParseTestURL(t, "spiffe://finance.internal/reconciliation/other")
	applier := &recordingApplier{
		identity: Identity{
			PrincipalID:    uuid.MustParse("00000000-0000-0000-0000-000000001901"),
			StableID:       "primary-finance-reconciliation",
			TLSURIIdentity: expectedURI.String(),
		},
	}
	handler, err := NewHTTPHandler(applier)
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	validBody := `{
		"idempotency_key":"settlement-http-negative",
		"source_sequence":1,
		"organization_id":"00000000-0000-0000-0000-000000001903",
		"kind":"SETTLEMENT_POSTED",
		"currency":"CNY",
		"settlement_minor":500,
		"external_reference":"payment-http-negative",
		"effective_at":"2026-08-24T12:00:00Z"
	}`
	tests := []struct {
		name        string
		method      string
		path        string
		contentType string
		body        string
		tlsState    *tls.ConnectionState
		wantStatus  int
	}{
		{
			name: "missing TLS identity", method: http.MethodPost, path: ReconciliationPath,
			contentType: "application/json", body: validBody, wantStatus: http.StatusUnauthorized,
		},
		{
			name: "wrong TLS URI", method: http.MethodPost, path: ReconciliationPath,
			contentType: "application/json", body: validBody,
			tlsState: verifiedTestTLSState(otherURI), wantStatus: http.StatusUnauthorized,
		},
		{
			name: "additional TLS URI", method: http.MethodPost, path: ReconciliationPath,
			contentType: "application/json", body: validBody,
			tlsState:   verifiedTestTLSStateWithURIs(expectedURI, otherURI),
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "wrong path", method: http.MethodPost, path: "/internal/v1/finance/other",
			contentType: "application/json", body: validBody,
			tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusNotFound,
		},
		{
			name: "wrong method", method: http.MethodGet, path: ReconciliationPath,
			contentType: "application/json", body: validBody,
			tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name: "wrong media type", method: http.MethodPost, path: ReconciliationPath,
			contentType: "text/plain", body: validBody,
			tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusUnsupportedMediaType,
		},
		{
			name: "unknown JSON field", method: http.MethodPost, path: ReconciliationPath,
			contentType: "application/json", body: strings.Replace(
				validBody, "\n\t}", ",\n\t\t\"secret\":\"forged\"\n\t}", 1,
			),
			tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusBadRequest,
		},
		{
			name: "null non-kind amount", method: http.MethodPost, path: ReconciliationPath,
			contentType: "application/json", body: strings.Replace(
				validBody, "\"settlement_minor\":500,", "\"settlement_minor\":500,\n\t\t\"credit_adjustment_minor\":null,", 1,
			),
			tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusBadRequest,
		},
		{
			name: "multiple JSON documents", method: http.MethodPost, path: ReconciliationPath,
			contentType: "application/json", body: validBody + `{}`,
			tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusBadRequest,
		},
		{
			name: "oversized body", method: http.MethodPost, path: ReconciliationPath,
			contentType: "application/json", body: string(bytes.Repeat([]byte("x"), maxRequestBytes+1)),
			tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid Organization UUID", method: http.MethodPost, path: ReconciliationPath,
			contentType: "application/json", body: string(bytes.ReplaceAll(
				[]byte(validBody),
				[]byte("00000000-0000-0000-0000-000000001903"),
				[]byte("not-a-uuid"),
			)),
			tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusBadRequest,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			beforeCalls := len(applier.requests)
			request := httptest.NewRequest(
				test.method,
				"https://finance-listener.internal"+test.path,
				bytes.NewBufferString(test.body),
			)
			request.Header.Set("Content-Type", test.contentType)
			request.TLS = test.tlsState
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.wantStatus ||
				response.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("rejected Finance HTTP response = %d %s; body=%s", response.Code, response.Header(), response.Body.String())
			}
			if len(applier.requests) != beforeCalls {
				t.Fatalf("rejected Finance HTTP request reached Apply: %d -> %d", beforeCalls, len(applier.requests))
			}
		})
	}
}

func TestHTTPHandlerMapsApplyFailuresWithoutLeakingInternalErrors(t *testing.T) {
	expectedURI := mustParseTestURL(t, "spiffe://finance.internal/reconciliation/primary")
	body := []byte(`{
		"idempotency_key":"settlement-http-error",
		"source_sequence":1,
		"organization_id":"00000000-0000-0000-0000-000000001903",
		"kind":"SETTLEMENT_POSTED",
		"currency":"CNY",
		"settlement_minor":500,
		"external_reference":"payment-http-error",
		"effective_at":"2026-08-24T12:00:00Z"
	}`)
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{name: "invalid", err: &Failure{Code: FailureInvalid, Message: "invalid input"}, wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "unauthorized", err: &Failure{Code: FailureUnauthorized, Message: "inactive identity"}, wantStatus: http.StatusUnauthorized, wantCode: "unauthorized"},
		{name: "not found", err: &Failure{Code: FailureNotFound, Message: "unknown Organization"}, wantStatus: http.StatusNotFound, wantCode: "not_found"},
		{name: "conflict", err: &Failure{Code: FailureConflict, Message: "sequence conflict"}, wantStatus: http.StatusConflict, wantCode: "conflict"},
		{name: "internal", err: errors.New("postgres://secret@database/internal detail"), wantStatus: http.StatusServiceUnavailable, wantCode: "unavailable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			applier := &recordingApplier{
				identity: Identity{
					PrincipalID: uuid.MustParse("00000000-0000-0000-0000-000000001901"),
					StableID:    "primary-finance-reconciliation", TLSURIIdentity: expectedURI.String(),
				},
				err: test.err,
			}
			handler, err := NewHTTPHandler(applier)
			if err != nil {
				t.Fatalf("NewHTTPHandler: %v", err)
			}
			request := httptest.NewRequest(
				http.MethodPost,
				"https://finance-listener.internal"+ReconciliationPath,
				bytes.NewReader(body),
			)
			request.Header.Set("Content-Type", "application/json")
			request.TLS = verifiedTestTLSState(expectedURI)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus ||
				!bytes.Contains(response.Body.Bytes(), []byte(`"code":"`+test.wantCode+`"`)) ||
				bytes.Contains(response.Body.Bytes(), []byte("secret")) ||
				bytes.Contains(response.Body.Bytes(), []byte("database")) {
				t.Fatalf("Finance HTTP failure = %d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

type recordingApplier struct {
	identity Identity
	result   Result
	err      error
	requests []Request
}

func (a *recordingApplier) Identity() Identity {
	return a.identity
}

func (a *recordingApplier) Apply(_ context.Context, request Request) (Result, error) {
	a.requests = append(a.requests, request)
	return a.result, a.err
}

func mustParseTestURL(t *testing.T, value string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(value)
	if err != nil {
		t.Fatalf("parse test URL: %v", err)
	}
	return parsed
}

func verifiedTestTLSState(identity *url.URL) *tls.ConnectionState {
	return verifiedTestTLSStateWithURIs(identity)
}

func verifiedTestTLSStateWithURIs(identities ...*url.URL) *tls.ConnectionState {
	certificate := &x509.Certificate{URIs: identities}
	return &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{certificate},
		VerifiedChains:   [][]*x509.Certificate{{certificate}},
	}
}
