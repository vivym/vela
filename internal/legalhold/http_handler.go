package legalhold

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/privilegedlistener"
)

const (
	EventPath       = "/internal/v1/compliance/legal-hold-events"
	maxRequestBytes = 64 * 1024
)

type HTTPHandler struct {
	applier             Applier
	expectedTLSIdentity string
}

type httpRequest struct {
	IdempotencyKey    string        `json:"idempotency_key"`
	SourceSequence    int64         `json:"source_sequence"`
	HoldID            string        `json:"hold_id"`
	Kind              Kind          `json:"kind"`
	Scope             Scope         `json:"scope,omitempty"`
	OrganizationID    string        `json:"organization_id,omitempty"`
	ProjectID         *string       `json:"project_id,omitempty"`
	JobID             *string       `json:"job_id,omitempty"`
	RecordClasses     []RecordClass `json:"record_classes,omitempty"`
	ReasonCode        string        `json:"reason_code"`
	ExternalReference string        `json:"external_reference"`
	EffectiveAt       *time.Time    `json:"effective_at"`
}

type httpResult struct {
	EventID       string        `json:"event_id"`
	Replayed      bool          `json:"replayed"`
	HoldID        string        `json:"hold_id"`
	State         State         `json:"state"`
	Scope         Scope         `json:"scope"`
	RecordClasses []RecordClass `json:"record_classes"`
	RecordedAt    time.Time     `json:"recorded_at"`
	ReleasedAt    *time.Time    `json:"released_at"`
}

type httpFailure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NewHTTPHandler(applier Applier) (*HTTPHandler, error) {
	if applier == nil {
		return nil, errors.New("legal hold applier is required")
	}
	identity := applier.Identity()
	if identity.PrincipalID == uuid.Nil || !validBoundedText(identity.StableID, 200) ||
		!validURIIdentity(identity.TLSURIIdentity) {
		return nil, errors.New("legal hold HTTP identity is invalid")
	}
	return &HTTPHandler{applier: applier, expectedTLSIdentity: identity.TLSURIIdentity}, nil
}

func (h *HTTPHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != EventPath {
		writeHTTPFailure(writer, http.StatusNotFound, "not_found", "resource is not found")
		return
	}
	if h == nil || h.applier == nil || !h.authenticated(request) {
		writeHTTPFailure(writer, http.StatusUnauthorized, "unauthorized", "authenticated Compliance identity is required")
		return
	}
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		writeHTTPFailure(writer, http.StatusMethodNotAllowed, "method_not_allowed", "method is not allowed")
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeHTTPFailure(writer, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
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
	holdID, ok := canonicalUUID(input.HoldID)
	if !ok || input.EffectiveAt == nil {
		writeHTTPFailure(writer, http.StatusBadRequest, "invalid_request", "request fields are invalid")
		return
	}
	var organizationID uuid.UUID
	var projectID, jobID *uuid.UUID
	var classes []RecordClass
	switch input.Kind {
	case KindHoldPlaced:
		for _, field := range []string{"project_id", "job_id"} {
			if raw, present := rawFields[field]; present && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				writeHTTPFailure(writer, http.StatusBadRequest, "invalid_request", "request fields are invalid")
				return
			}
		}
		organizationID, ok = canonicalUUID(input.OrganizationID)
		if !ok {
			writeHTTPFailure(writer, http.StatusBadRequest, "invalid_request", "request fields are invalid")
			return
		}
		var ok bool
		projectID, ok = optionalUUID(input.ProjectID)
		if !ok {
			writeHTTPFailure(writer, http.StatusBadRequest, "invalid_request", "request fields are invalid")
			return
		}
		jobID, ok = optionalUUID(input.JobID)
		if !ok {
			writeHTTPFailure(writer, http.StatusBadRequest, "invalid_request", "request fields are invalid")
			return
		}
		classes, ok = canonicalRecordClasses(input.RecordClasses)
		if !ok {
			writeHTTPFailure(writer, http.StatusBadRequest, "invalid_request", "request fields are invalid")
			return
		}
	case KindHoldReleased:
		for _, field := range []string{"scope", "organization_id", "project_id", "job_id", "record_classes"} {
			if _, present := rawFields[field]; present {
				writeHTTPFailure(writer, http.StatusBadRequest, "invalid_request", "request fields are invalid")
				return
			}
		}
	default:
		writeHTTPFailure(writer, http.StatusBadRequest, "invalid_request", "request fields are invalid")
		return
	}
	result, err := h.applier.Apply(request.Context(), Request{
		IdempotencyKey: input.IdempotencyKey, SourceSequence: input.SourceSequence,
		HoldID: holdID, Kind: input.Kind, Scope: input.Scope,
		OrganizationID: organizationID, ProjectID: projectID, JobID: jobID,
		RecordClasses: classes, ReasonCode: input.ReasonCode,
		ExternalReference: input.ExternalReference, EffectiveAt: input.EffectiveAt.UTC(),
	})
	if err != nil {
		writeApplyFailure(writer, err)
		return
	}
	if !validHTTPResult(result) {
		writeHTTPFailure(writer, http.StatusServiceUnavailable, "unavailable", "Legal Hold result is unavailable")
		return
	}
	status := http.StatusCreated
	if result.Replayed {
		status = http.StatusOK
	}
	privilegedlistener.WriteJSON(writer, status, httpResult{
		EventID: result.EventID.String(), Replayed: result.Replayed,
		HoldID: result.HoldID.String(), State: result.State, Scope: result.Scope,
		RecordClasses: result.RecordClasses, RecordedAt: result.RecordedAt.UTC(),
		ReleasedAt: utcTimePointer(result.ReleasedAt),
	})
}

func validHTTPResult(result Result) bool {
	if result.EventID == uuid.Nil || result.HoldID == uuid.Nil || result.RecordedAt.IsZero() {
		return false
	}
	switch result.State {
	case StateActive, StateReleased:
	default:
		return false
	}
	switch result.Scope {
	case ScopeOrganization, ScopeProject, ScopeJob:
	default:
		return false
	}
	canonical, ok := canonicalRecordClasses(result.RecordClasses)
	if !ok || len(canonical) != len(result.RecordClasses) {
		return false
	}
	for index := range canonical {
		if canonical[index] != result.RecordClasses[index] {
			return false
		}
	}
	return true
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
			writeHTTPFailure(writer, http.StatusServiceUnavailable, "unavailable", "Legal Hold is unavailable")
		}
		return
	}
	writeHTTPFailure(writer, http.StatusServiceUnavailable, "unavailable", "Legal Hold is unavailable")
}

func (h *HTTPHandler) authenticated(request *http.Request) bool {
	return privilegedlistener.AuthenticatedExactURI(request, h.expectedTLSIdentity)
}

func optionalUUID(value *string) (*uuid.UUID, bool) {
	if value == nil {
		return nil, true
	}
	parsed, ok := canonicalUUID(*value)
	return &parsed, ok
}

func canonicalUUID(value string) (uuid.UUID, bool) {
	parsed, err := uuid.Parse(value)
	return parsed, err == nil && parsed.String() == value
}

func utcTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	utc := value.UTC()
	return &utc
}

func writeHTTPFailure(writer http.ResponseWriter, status int, code, message string) {
	privilegedlistener.WriteJSON(writer, status, httpFailure{Code: code, Message: message})
}
