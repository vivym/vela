package legalhold

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

func TestHTTPHandlerAcceptsAuthenticatedStrictLegalHoldPlacement(t *testing.T) {
	identityURI := mustParseTestURL(t, "spiffe://compliance.internal/legal-holds/primary")
	recordedAt := time.Date(2026, 8, 27, 14, 0, 0, 0, time.UTC)
	holdID := uuid.MustParse("00000000-0000-0000-0000-000000003301")
	organizationID := uuid.MustParse("00000000-0000-0000-0000-000000003302")
	projectID := uuid.MustParse("00000000-0000-0000-0000-000000003303")
	jobID := uuid.MustParse("00000000-0000-0000-0000-000000003304")
	applier := &recordingApplier{
		identity: Identity{
			PrincipalID:    uuid.MustParse("00000000-0000-0000-0000-000000003305"),
			StableID:       "primary-compliance",
			TLSURIIdentity: identityURI.String(),
		},
		result: Result{
			EventID:       uuid.MustParse("00000000-0000-0000-0000-000000003306"),
			HoldID:        holdID,
			State:         StateActive,
			Scope:         ScopeJob,
			RecordClasses: []RecordClass{RecordClassMetadata, RecordClassFinancial},
			RecordedAt:    recordedAt,
		},
	}
	handler, err := NewHTTPHandler(applier)
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	body := []byte(`{
		"idempotency_key":"legal-hold-http-3301",
		"source_sequence":1,
		"hold_id":"00000000-0000-0000-0000-000000003301",
		"kind":"HOLD_PLACED",
		"scope":"JOB",
		"organization_id":"00000000-0000-0000-0000-000000003302",
		"project_id":"00000000-0000-0000-0000-000000003303",
		"job_id":"00000000-0000-0000-0000-000000003304",
		"record_classes":["FINANCIAL","METADATA"],
		"reason_code":"LITIGATION",
		"external_reference":"matter-3301/place",
		"effective_at":"2026-08-27T12:00:00Z"
	}`)
	request := httptest.NewRequest(
		http.MethodPost,
		"https://compliance.internal"+EventPath,
		bytes.NewReader(body),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.TLS = verifiedTestTLSState(identityURI)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusCreated || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("Compliance HTTP response = %d %s; body=%s", response.Code, response.Header(), response.Body.String())
	}
	if len(applier.requests) != 1 {
		t.Fatalf("Compliance HTTP Apply calls = %d, want 1", len(applier.requests))
	}
	applied := applier.requests[0]
	if applied.IdempotencyKey != "legal-hold-http-3301" || applied.SourceSequence != 1 ||
		applied.HoldID != holdID || applied.Kind != KindHoldPlaced || applied.Scope != ScopeJob ||
		applied.OrganizationID != organizationID || applied.ProjectID == nil || *applied.ProjectID != projectID ||
		applied.JobID == nil || *applied.JobID != jobID ||
		len(applied.RecordClasses) != 2 || applied.RecordClasses[0] != RecordClassMetadata ||
		applied.RecordClasses[1] != RecordClassFinancial ||
		!applied.EffectiveAt.Equal(time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("Compliance HTTP Apply request = %#v", applied)
	}
	var decoded struct {
		EventID       string        `json:"event_id"`
		Replayed      bool          `json:"replayed"`
		HoldID        string        `json:"hold_id"`
		State         State         `json:"state"`
		Scope         Scope         `json:"scope"`
		RecordClasses []RecordClass `json:"record_classes"`
		RecordedAt    time.Time     `json:"recorded_at"`
		ReleasedAt    *time.Time    `json:"released_at"`
	}
	decoder := json.NewDecoder(response.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("decode Compliance HTTP response: %v", err)
	}
	if decoded.EventID != applier.result.EventID.String() || decoded.Replayed ||
		decoded.HoldID != holdID.String() || decoded.State != StateActive ||
		decoded.Scope != ScopeJob || len(decoded.RecordClasses) != 2 ||
		!decoded.RecordedAt.Equal(recordedAt) || decoded.ReleasedAt != nil {
		t.Fatalf("Compliance HTTP response body = %#v", decoded)
	}
}

func TestHTTPHandlerAcceptsStrictLegalHoldReleaseWithoutPlacementFields(t *testing.T) {
	identityURI := mustParseTestURL(t, "spiffe://compliance.internal/legal-holds/primary")
	releasedAt := time.Date(2026, 8, 27, 16, 0, 0, 0, time.UTC)
	applier := &recordingApplier{
		identity: Identity{
			PrincipalID:    uuid.MustParse("00000000-0000-0000-0000-000000003305"),
			StableID:       "primary-compliance",
			TLSURIIdentity: identityURI.String(),
		},
		result: Result{
			EventID:       uuid.MustParse("00000000-0000-0000-0000-000000003307"),
			HoldID:        uuid.MustParse("00000000-0000-0000-0000-000000003301"),
			State:         StateReleased,
			Scope:         ScopeJob,
			RecordClasses: []RecordClass{RecordClassMetadata},
			RecordedAt:    releasedAt,
			ReleasedAt:    &releasedAt,
		},
	}
	handler, err := NewHTTPHandler(applier)
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"https://compliance.internal"+EventPath,
		bytes.NewBufferString(`{
			"idempotency_key":"legal-hold-release-http-3301",
			"source_sequence":2,
			"hold_id":"00000000-0000-0000-0000-000000003301",
			"kind":"HOLD_RELEASED",
			"reason_code":"ORDER_LIFTED",
			"external_reference":"matter-3301/release",
			"effective_at":"2026-08-27T15:00:00Z"
		}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.TLS = verifiedTestTLSState(identityURI)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("Compliance release HTTP response = %d body=%s", response.Code, response.Body.String())
	}
	if len(applier.requests) != 1 {
		t.Fatalf("Compliance release Apply calls = %d, want 1", len(applier.requests))
	}
	applied := applier.requests[0]
	if applied.Kind != KindHoldReleased || applied.OrganizationID != uuid.Nil ||
		applied.ProjectID != nil || applied.JobID != nil || len(applied.RecordClasses) != 0 {
		t.Fatalf("Compliance release Apply request = %#v", applied)
	}
}

func TestHTTPHandlerRejectsUnauthenticatedOrMalformedEventsBeforeApply(t *testing.T) {
	expectedURI := mustParseTestURL(t, "spiffe://compliance.internal/legal-holds/primary")
	otherURI := mustParseTestURL(t, "spiffe://compliance.internal/legal-holds/other")
	applier := &recordingApplier{identity: Identity{
		PrincipalID: uuid.MustParse("00000000-0000-0000-0000-000000003305"),
		StableID:    "primary-compliance", TLSURIIdentity: expectedURI.String(),
	}}
	handler, err := NewHTTPHandler(applier)
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	validBody := `{
		"idempotency_key":"legal-hold-http-negative",
		"source_sequence":1,
		"hold_id":"00000000-0000-0000-0000-000000003301",
		"kind":"HOLD_PLACED",
		"scope":"ORGANIZATION",
		"organization_id":"00000000-0000-0000-0000-000000003302",
		"record_classes":["METADATA"],
		"reason_code":"LITIGATION",
		"external_reference":"matter-negative/place",
		"effective_at":"2026-08-27T12:00:00Z"
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
		{name: "missing TLS identity", method: http.MethodPost, path: EventPath, contentType: "application/json", body: validBody, wantStatus: http.StatusUnauthorized},
		{name: "wrong TLS URI", method: http.MethodPost, path: EventPath, contentType: "application/json", body: validBody, tlsState: verifiedTestTLSState(otherURI), wantStatus: http.StatusUnauthorized},
		{name: "additional TLS URI", method: http.MethodPost, path: EventPath, contentType: "application/json", body: validBody, tlsState: verifiedTestTLSStateWithURIs(expectedURI, otherURI), wantStatus: http.StatusUnauthorized},
		{name: "wrong path", method: http.MethodPost, path: "/internal/v1/compliance/other", contentType: "application/json", body: validBody, tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusNotFound},
		{name: "wrong method", method: http.MethodGet, path: EventPath, contentType: "application/json", body: validBody, tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusMethodNotAllowed},
		{name: "wrong media type", method: http.MethodPost, path: EventPath, contentType: "text/plain", body: validBody, tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusUnsupportedMediaType},
		{name: "unknown JSON field", method: http.MethodPost, path: EventPath, contentType: "application/json", body: strings.Replace(validBody, "\n\t}", ",\n\t\t\"secret\":\"forged\"\n\t}", 1), tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusBadRequest},
		{name: "unknown kind", method: http.MethodPost, path: EventPath, contentType: "application/json", body: strings.Replace(validBody, "HOLD_PLACED", "UNKNOWN", 1), tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusBadRequest},
		{name: "placement contains null optional field", method: http.MethodPost, path: EventPath, contentType: "application/json", body: strings.Replace(validBody, `"record_classes"`, `"project_id":null,"record_classes"`, 1), tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusBadRequest},
		{name: "release contains null placement field", method: http.MethodPost, path: EventPath, contentType: "application/json", body: `{"idempotency_key":"release-null","source_sequence":2,"hold_id":"00000000-0000-0000-0000-000000003301","kind":"HOLD_RELEASED","scope":null,"reason_code":"ORDER_LIFTED","external_reference":"matter/release","effective_at":"2026-08-27T13:00:00Z"}`, tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusBadRequest},
		{name: "duplicate record class", method: http.MethodPost, path: EventPath, contentType: "application/json", body: strings.Replace(validBody, `["METADATA"]`, `["METADATA","METADATA"]`, 1), tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusBadRequest},
		{name: "invalid Organization UUID", method: http.MethodPost, path: EventPath, contentType: "application/json", body: strings.Replace(validBody, "00000000-0000-0000-0000-000000003302", "not-a-uuid", 1), tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusBadRequest},
		{name: "noncanonical hold UUID", method: http.MethodPost, path: EventPath, contentType: "application/json", body: strings.Replace(validBody, "00000000-0000-0000-0000-000000003301", "00000000000000000000000000003301", 1), tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusBadRequest},
		{name: "noncanonical Organization UUID", method: http.MethodPost, path: EventPath, contentType: "application/json", body: strings.Replace(validBody, "00000000-0000-0000-0000-000000003302", "{00000000-0000-0000-0000-000000003302}", 1), tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusBadRequest},
		{name: "multiple documents", method: http.MethodPost, path: EventPath, contentType: "application/json", body: validBody + `{}`, tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusBadRequest},
		{name: "oversized body", method: http.MethodPost, path: EventPath, contentType: "application/json", body: string(bytes.Repeat([]byte("x"), maxRequestBytes+1)), tlsState: verifiedTestTLSState(expectedURI), wantStatus: http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			beforeCalls := len(applier.requests)
			request := httptest.NewRequest(test.method, "https://compliance.internal"+test.path, bytes.NewBufferString(test.body))
			request.Header.Set("Content-Type", test.contentType)
			request.TLS = test.tlsState
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus || response.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("rejected Compliance HTTP response = %d %s body=%s", response.Code, response.Header(), response.Body.String())
			}
			if len(applier.requests) != beforeCalls {
				t.Fatalf("rejected Compliance HTTP request reached Apply: %d -> %d", beforeCalls, len(applier.requests))
			}
		})
	}
}

func TestHTTPHandlerMapsApplyFailuresWithoutLeakingInternalErrors(t *testing.T) {
	identityURI := mustParseTestURL(t, "spiffe://compliance.internal/legal-holds/primary")
	body := []byte(`{
		"idempotency_key":"legal-hold-http-error",
		"source_sequence":1,
		"hold_id":"00000000-0000-0000-0000-000000003301",
		"kind":"HOLD_PLACED",
		"scope":"ORGANIZATION",
		"organization_id":"00000000-0000-0000-0000-000000003302",
		"record_classes":["METADATA"],
		"reason_code":"LITIGATION",
		"external_reference":"matter-error/place",
		"effective_at":"2026-08-27T12:00:00Z"
	}`)
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{name: "invalid", err: &Failure{Code: FailureInvalid, Message: "invalid input"}, wantStatus: http.StatusBadRequest, wantCode: "invalid_request"},
		{name: "unauthorized", err: &Failure{Code: FailureUnauthorized, Message: "inactive identity"}, wantStatus: http.StatusUnauthorized, wantCode: "unauthorized"},
		{name: "not found", err: &Failure{Code: FailureNotFound, Message: "unknown target"}, wantStatus: http.StatusNotFound, wantCode: "not_found"},
		{name: "conflict", err: &Failure{Code: FailureConflict, Message: "sequence conflict"}, wantStatus: http.StatusConflict, wantCode: "conflict"},
		{name: "internal", err: errors.New("postgres://secret@database/internal detail"), wantStatus: http.StatusServiceUnavailable, wantCode: "unavailable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			applier := &recordingApplier{
				identity: Identity{
					PrincipalID: uuid.MustParse("00000000-0000-0000-0000-000000003305"),
					StableID:    "primary-compliance", TLSURIIdentity: identityURI.String(),
				},
				err: test.err,
			}
			handler, err := NewHTTPHandler(applier)
			if err != nil {
				t.Fatalf("NewHTTPHandler: %v", err)
			}
			request := httptest.NewRequest(http.MethodPost, "https://compliance.internal"+EventPath, bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.TLS = verifiedTestTLSState(identityURI)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus ||
				!bytes.Contains(response.Body.Bytes(), []byte(`"code":"`+test.wantCode+`"`)) ||
				bytes.Contains(response.Body.Bytes(), []byte("secret")) ||
				bytes.Contains(response.Body.Bytes(), []byte("database")) {
				t.Fatalf("Compliance HTTP failure = %d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestHTTPHandlerRejectsInvalidApplyResults(t *testing.T) {
	identityURI := mustParseTestURL(t, "spiffe://compliance.internal/legal-holds/primary")
	body := []byte(`{
		"idempotency_key":"legal-hold-invalid-result",
		"source_sequence":1,
		"hold_id":"00000000-0000-0000-0000-000000003301",
		"kind":"HOLD_PLACED",
		"scope":"ORGANIZATION",
		"organization_id":"00000000-0000-0000-0000-000000003302",
		"record_classes":["METADATA"],
		"reason_code":"LITIGATION",
		"external_reference":"matter-invalid-result/place",
		"effective_at":"2026-08-27T12:00:00Z"
	}`)
	validResult := Result{
		EventID:       uuid.MustParse("00000000-0000-0000-0000-000000003306"),
		HoldID:        uuid.MustParse("00000000-0000-0000-0000-000000003301"),
		State:         StateActive,
		Scope:         ScopeOrganization,
		RecordClasses: []RecordClass{RecordClassMetadata},
		RecordedAt:    time.Date(2026, 8, 27, 14, 0, 0, 0, time.UTC),
	}
	tests := []struct {
		name   string
		mutate func(*Result)
	}{
		{name: "unknown state", mutate: func(result *Result) { result.State = State("UNKNOWN") }},
		{name: "unknown scope", mutate: func(result *Result) { result.Scope = Scope("TENANT") }},
		{name: "empty classes", mutate: func(result *Result) { result.RecordClasses = nil }},
		{name: "noncanonical classes", mutate: func(result *Result) {
			result.RecordClasses = []RecordClass{RecordClassFinancial, RecordClassMetadata}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := validResult
			test.mutate(&result)
			applier := &recordingApplier{
				identity: Identity{
					PrincipalID: uuid.MustParse("00000000-0000-0000-0000-000000003305"),
					StableID:    "primary-compliance", TLSURIIdentity: identityURI.String(),
				},
				result: result,
			}
			handler, err := NewHTTPHandler(applier)
			if err != nil {
				t.Fatalf("NewHTTPHandler: %v", err)
			}
			request := httptest.NewRequest(http.MethodPost, "https://compliance.internal"+EventPath, bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.TLS = verifiedTestTLSState(identityURI)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != http.StatusServiceUnavailable ||
				!bytes.Contains(response.Body.Bytes(), []byte(`"code":"unavailable"`)) {
				t.Fatalf("invalid Apply result response = %d body=%s", response.Code, response.Body.String())
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
