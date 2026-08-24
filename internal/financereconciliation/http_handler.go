package financereconciliation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/google/uuid"
)

const (
	ReconciliationPath = "/internal/v1/finance/reconciliations"
	maxRequestBytes    = 64 * 1024
)

type Applier interface {
	Identity() Identity
	Apply(context.Context, Request) (Result, error)
}

type HTTPHandler struct {
	applier             Applier
	expectedTLSIdentity string
}

type httpRequest struct {
	IdempotencyKey           string     `json:"idempotency_key"`
	SourceSequence           int64      `json:"source_sequence"`
	OrganizationID           string     `json:"organization_id"`
	Kind                     Kind       `json:"kind"`
	Currency                 string     `json:"currency"`
	SettlementMinor          *int64     `json:"settlement_minor,omitempty"`
	CreditAdjustmentMinor    *int64     `json:"credit_adjustment_minor,omitempty"`
	ContractCreditLimitMinor *int64     `json:"contract_credit_limit_minor,omitempty"`
	ExternalReference        string     `json:"external_reference"`
	EffectiveAt              *time.Time `json:"effective_at"`
}

type httpResult struct {
	RecordID                 string    `json:"record_id"`
	Replayed                 bool      `json:"replayed"`
	OrganizationID           string    `json:"organization_id"`
	Kind                     Kind      `json:"kind"`
	Currency                 string    `json:"currency"`
	ContractCreditLimitMinor int64     `json:"contract_credit_limit_minor"`
	UnsettledPostedMinor     int64     `json:"unsettled_posted_minor"`
	AccountVersion           int64     `json:"account_version"`
	PostedAt                 time.Time `json:"posted_at"`
}

type httpFailure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NewHTTPHandler(applier Applier) (*HTTPHandler, error) {
	if applier == nil {
		return nil, errors.New("finance reconciliation applier is required")
	}
	identity := applier.Identity()
	if identity.PrincipalID == uuid.Nil || !validBoundedText(identity.StableID, 200) ||
		!validSPIFFEIdentity(identity.TLSURIIdentity) {
		return nil, errors.New("finance reconciliation HTTP identity is invalid")
	}
	return &HTTPHandler{applier: applier, expectedTLSIdentity: identity.TLSURIIdentity}, nil
}

func (h *HTTPHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != ReconciliationPath {
		writeHTTPFailure(writer, http.StatusNotFound, "not_found", "resource is not found")
		return
	}
	if h == nil || h.applier == nil || !h.authenticated(request) {
		writeHTTPFailure(
			writer,
			http.StatusUnauthorized,
			"unauthorized",
			"authenticated Finance Reconciliation identity is required",
		)
		return
	}
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		writeHTTPFailure(writer, http.StatusMethodNotAllowed, "method_not_allowed", "method is not allowed")
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeHTTPFailure(
			writer,
			http.StatusUnsupportedMediaType,
			"unsupported_media_type",
			"Content-Type must be application/json",
		)
		return
	}
	if request.ContentLength > maxRequestBytes {
		writeHTTPFailure(writer, http.StatusBadRequest, "invalid_request", "request body is too large")
		return
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, maxRequestBytes+1))
	if err != nil || len(body) == 0 || len(body) > maxRequestBytes {
		writeHTTPFailure(writer, http.StatusBadRequest, "invalid_request", "request body is invalid")
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var input httpRequest
	if err := decoder.Decode(&input); err != nil {
		writeHTTPFailure(writer, http.StatusBadRequest, "invalid_request", "request JSON is invalid")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeHTTPFailure(writer, http.StatusBadRequest, "invalid_request", "request must contain one JSON document")
		return
	}
	var rawFields map[string]json.RawMessage
	if err := json.Unmarshal(body, &rawFields); err != nil {
		writeHTTPFailure(writer, http.StatusBadRequest, "invalid_request", "request JSON is invalid")
		return
	}
	for _, field := range []string{"settlement_minor", "credit_adjustment_minor", "contract_credit_limit_minor"} {
		if value, present := rawFields[field]; present && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			writeHTTPFailure(writer, http.StatusBadRequest, "invalid_request", "request fields are invalid")
			return
		}
	}
	organizationID, err := uuidFromString(input.OrganizationID)
	if err != nil || input.EffectiveAt == nil {
		writeHTTPFailure(writer, http.StatusBadRequest, "invalid_request", "request fields are invalid")
		return
	}
	result, err := h.applier.Apply(request.Context(), Request{
		IdempotencyKey:           input.IdempotencyKey,
		SourceSequence:           input.SourceSequence,
		OrganizationID:           organizationID,
		Kind:                     input.Kind,
		Currency:                 input.Currency,
		SettlementMinor:          input.SettlementMinor,
		CreditAdjustmentMinor:    input.CreditAdjustmentMinor,
		ContractCreditLimitMinor: input.ContractCreditLimitMinor,
		ExternalReference:        input.ExternalReference,
		EffectiveAt:              input.EffectiveAt.UTC(),
	})
	if err != nil {
		writeApplyFailure(writer, err)
		return
	}
	if result.RecordID == uuid.Nil || result.OrganizationID == uuid.Nil || result.PostedAt.IsZero() ||
		result.AccountVersion <= 0 {
		writeHTTPFailure(writer, http.StatusServiceUnavailable, "unavailable", "reconciliation result is unavailable")
		return
	}
	status := http.StatusCreated
	if result.Replayed {
		status = http.StatusOK
	}
	writeJSON(writer, status, httpResult{
		RecordID:                 result.RecordID.String(),
		Replayed:                 result.Replayed,
		OrganizationID:           result.OrganizationID.String(),
		Kind:                     result.Kind,
		Currency:                 result.Currency,
		ContractCreditLimitMinor: result.ContractCreditLimitMinor,
		UnsettledPostedMinor:     result.UnsettledPostedMinor,
		AccountVersion:           result.AccountVersion,
		PostedAt:                 result.PostedAt.UTC(),
	})
}

func (h *HTTPHandler) authenticated(request *http.Request) bool {
	if request == nil || request.TLS == nil || len(request.TLS.PeerCertificates) == 0 ||
		len(request.TLS.VerifiedChains) == 0 {
		return false
	}
	leaf := request.TLS.PeerCertificates[0]
	if len(leaf.URIs) != 1 || leaf.URIs[0].String() != h.expectedTLSIdentity {
		return false
	}
	for _, chain := range request.TLS.VerifiedChains {
		if len(chain) > 0 && chain[0].Equal(leaf) {
			return true
		}
	}
	return false
}

func writeApplyFailure(writer http.ResponseWriter, err error) {
	var failure *Failure
	if errors.As(err, &failure) {
		switch failure.Code {
		case FailureInvalid:
			writeHTTPFailure(writer, http.StatusBadRequest, string(failure.Code), failure.Message)
		case FailureUnauthorized:
			writeHTTPFailure(writer, http.StatusUnauthorized, string(failure.Code), failure.Message)
		case FailureNotFound:
			writeHTTPFailure(writer, http.StatusNotFound, string(failure.Code), failure.Message)
		case FailureConflict:
			writeHTTPFailure(writer, http.StatusConflict, string(failure.Code), failure.Message)
		default:
			writeHTTPFailure(writer, http.StatusServiceUnavailable, "unavailable", "reconciliation is unavailable")
		}
		return
	}
	writeHTTPFailure(writer, http.StatusServiceUnavailable, "unavailable", "reconciliation is unavailable")
}

func writeHTTPFailure(writer http.ResponseWriter, status int, code, message string) {
	writeJSON(writer, status, httpFailure{Code: code, Message: message})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func uuidFromString(value string) (uuid.UUID, error) {
	return uuid.Parse(value)
}
