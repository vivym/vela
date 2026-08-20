package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	oapimiddleware "github.com/oapi-codegen/nethttp-middleware"
	api "github.com/vivym/vela/api/gen"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/identity"
)

const maxRequestBodyBytes = 1 << 20

type Config struct {
	Authenticator *identity.Authenticator
	Admission     *admission.Service
}

type server struct {
	authenticator *identity.Authenticator
	admission     *admission.Service
}

type principalContextKey struct{}

func NewHandler(config Config) (http.Handler, error) {
	if config.Authenticator == nil {
		return nil, errors.New("missing HTTP API authenticator")
	}
	if config.Admission == nil {
		return nil, errors.New("missing HTTP API Admission service")
	}
	openAPI, err := api.GetSpec()
	if err != nil {
		return nil, fmt.Errorf("load embedded OpenAPI contract: %w", err)
	}
	openAPI.Servers = nil
	openAPI.Security = nil

	implementation := &server{
		authenticator: config.Authenticator,
		admission:     config.Admission,
	}
	strict := api.NewStrictHandlerWithOptions(implementation, nil, api.StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, _ error) {
			writeError(w, http.StatusBadRequest, "invalid_request", "request does not match the API contract")
		},
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			slog.ErrorContext(r.Context(), "HTTP request failed", "error", err)
			writeError(w, http.StatusInternalServerError, "internal_error", "request could not be completed")
		},
	})

	router := chi.NewRouter()
	router.Use(implementation.limitRequestBody)
	router.Use(implementation.authenticate)
	router.Use(oapimiddleware.OapiRequestValidatorWithOptions(openAPI, &oapimiddleware.Options{
		ErrorHandler: func(w http.ResponseWriter, _ string, _ int) {
			writeError(w, http.StatusBadRequest, "invalid_request", "request does not match the API contract")
		},
	}))
	return api.HandlerFromMux(strict, router), nil
}

func (s *server) SubmitJob(
	ctx context.Context,
	request api.SubmitJobRequestObject,
) (api.SubmitJobResponseObject, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return api.SubmitJob401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: "valid Service Principal credential is required",
			},
		}, nil
	}
	if !principal.HasScope(identity.ScopeJobsSubmit) {
		return api.SubmitJob403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "credential does not have jobs:submit scope",
			},
		}, nil
	}
	if request.Body == nil {
		return submitBadRequest("request body is required"), nil
	}

	var clientMetadata json.RawMessage
	if request.Body.ClientMetadata != nil {
		clientMetadata = append(clientMetadata, (*request.Body.ClientMetadata)...)
	}
	job, err := s.admission.Submit(ctx, principal, request.ProjectId, request.Params.IdempotencyKey, admission.Request{
		Model:            request.Body.Model,
		GenerationPreset: string(request.Body.GenerationPreset),
		ServiceClass:     string(request.Body.ServiceClass),
		OutputSpec:       request.Body.OutputSpec,
		GenerationCount:  int32(request.Body.GenerationCount),
		Prompt:           request.Body.Prompt,
		ClientMetadata:   clientMetadata,
	})
	if err != nil {
		return submitFailure(err)
	}
	return api.SubmitJob202JSONResponse(toAPIJob(job)), nil
}

func (s *server) GetJob(
	ctx context.Context,
	request api.GetJobRequestObject,
) (api.GetJobResponseObject, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return api.GetJob401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: "valid Service Principal credential is required",
			},
		}, nil
	}
	if !principal.HasScope(identity.ScopeJobsRead) {
		return api.GetJob403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "credential does not have jobs:read scope",
			},
		}, nil
	}
	job, err := s.admission.Get(ctx, principal, request.ProjectId, request.JobId)
	if err != nil {
		var failure *admission.Failure
		if errors.As(err, &failure) {
			switch failure.Code {
			case admission.FailureCodeForbidden:
				return api.GetJob403JSONResponse{
					ForbiddenJSONResponse: api.ForbiddenJSONResponse{
						Code: string(failure.Code), Message: failure.Message,
					},
				}, nil
			case admission.FailureCodeNotFound:
				return api.GetJob404JSONResponse{
					Code: string(failure.Code), Message: failure.Message,
				}, nil
			}
		}
		return nil, err
	}
	return api.GetJob200JSONResponse(toAPIJob(job)), nil
}

func (s *server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Fields(r.Header.Get("Authorization"))
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			writeError(w, http.StatusUnauthorized, "unauthorized", "valid Service Principal credential is required")
			return
		}
		principal, err := s.authenticator.Authenticate(r.Context(), parts[1])
		if errors.Is(err, identity.ErrInvalidCredential) {
			writeError(w, http.StatusUnauthorized, "unauthorized", "valid Service Principal credential is required")
			return
		}
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, "identity_unavailable", "credential verification is unavailable")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalContextKey{}, principal)))
	})
}

func (s *server) limitRequestBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
		}
		next.ServeHTTP(w, r)
	})
}

func principalFromContext(ctx context.Context) (identity.Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(identity.Principal)
	return principal, ok
}

func submitFailure(err error) (api.SubmitJobResponseObject, error) {
	var failure *admission.Failure
	if !errors.As(err, &failure) {
		return nil, err
	}
	response := api.Error{Code: string(failure.Code), Message: failure.Message}
	switch failure.Code {
	case admission.FailureCodeInvalidRequest, admission.FailureCodeInvalidSKU:
		return api.SubmitJob400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case admission.FailureCodeCreditLimitExceeded:
		return api.SubmitJob402JSONResponse(response), nil
	case admission.FailureCodeForbidden, admission.FailureCodeOrganizationInactive:
		return api.SubmitJob403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case admission.FailureCodeIdempotencyConflict:
		return api.SubmitJob409JSONResponse(response), nil
	case admission.FailureCodeProjectLimitExceeded:
		retryAfter := failure.RetryAfter
		return api.SubmitJob429JSONResponse{
			Body: response, Headers: api.SubmitJob429ResponseHeaders{RetryAfter: &retryAfter},
		}, nil
	case admission.FailureCodeCapacityUnavailable:
		retryAfter := failure.RetryAfter
		return api.SubmitJob503JSONResponse{
			Body: response, Headers: api.SubmitJob503ResponseHeaders{RetryAfter: &retryAfter},
		}, nil
	default:
		return nil, err
	}
}

func submitBadRequest(message string) api.SubmitJob400JSONResponse {
	return api.SubmitJob400JSONResponse{
		BadRequestJSONResponse: api.BadRequestJSONResponse{
			Code: "invalid_request", Message: message,
		},
	}
}

func toAPIJob(job admission.Job) api.Job {
	view := api.Job{
		AttemptsStarted: int(job.AttemptsStarted),
		CreatedAt:       job.CreatedAt,
		JobExpiresAt:    job.JobExpiresAt,
		JobId:           job.ID,
		ProjectId:       job.ProjectID,
		State:           api.JobState(job.State),
		Pricing: api.PricingSnapshot{
			RateCardRevisionId: job.PricingSnapshot.RateCardRevisionID,
			RateLineId:         job.PricingSnapshot.RateLineID,
			UnitAmountMinor:    job.PricingSnapshot.UnitAmountMinor,
			Quantity:           int(job.PricingSnapshot.Quantity),
			QuotedAmountMinor:  job.PricingSnapshot.QuotedAmountMinor,
			Currency:           job.PricingSnapshot.Currency,
		},
	}
	if job.Phase != nil {
		phase := api.ExecutionPhase(*job.Phase)
		view.Phase = &phase
	}
	view.PhaseProgress = job.PhaseProgress
	view.NextRetryAt = job.NextRetryAt
	view.EstimatedFinishAt = job.EstimatedFinishAt
	view.ProgressUpdatedAt = job.ProgressUpdatedAt
	return view
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(api.Error{Code: code, Message: message})
}
