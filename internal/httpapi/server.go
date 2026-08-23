package httpapi

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	oapimiddleware "github.com/oapi-codegen/nethttp-middleware"
	api "github.com/vivym/vela/api/gen"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/artifactaccess"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/webhook"
)

const (
	maxRequestBodyBytes                 = 1 << 20
	authenticationFailureMessage        = "valid bearer credential is required"
	serviceAuthenticationFailureMessage = "valid Service Principal credential is required"
)

type Config struct {
	Authenticator *identity.Authenticator
	Admission     *admission.Service
	Cancellation  *cancellation.Service
	Artifacts     *artifactaccess.Service
	Webhooks      *webhook.Service
}

type server struct {
	authenticator *identity.Authenticator
	admission     *admission.Service
	cancellation  *cancellation.Service
	artifacts     *artifactaccess.Service
	webhooks      *webhook.Service
}

type principalContextKey struct{}

func NewHandler(config Config) (http.Handler, error) {
	if config.Authenticator == nil {
		return nil, errors.New("missing HTTP API authenticator")
	}
	if config.Admission == nil {
		return nil, errors.New("missing HTTP API Admission service")
	}
	if config.Cancellation == nil {
		return nil, errors.New("missing HTTP API cancellation service")
	}
	if config.Artifacts == nil {
		return nil, errors.New("missing HTTP API Artifact access service")
	}
	if config.Webhooks == nil {
		return nil, errors.New("missing HTTP API webhook service")
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
		cancellation:  config.Cancellation,
		artifacts:     config.Artifacts,
		webhooks:      config.Webhooks,
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
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	principal, projectAuthorized := principalForProjectRequest(principal, request.ProjectId)
	if !projectAuthorized || !principal.HasScope(identity.ScopeJobsSubmit) {
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
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	principal, projectAuthorized := principalForProjectRequest(principal, request.ProjectId)
	if !projectAuthorized || !principal.HasScope(identity.ScopeJobsRead) {
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

func (s *server) CancelJob(
	ctx context.Context,
	request api.CancelJobRequestObject,
) (api.CancelJobResponseObject, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return api.CancelJob401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	principal, projectAuthorized := principalForProjectRequest(principal, request.ProjectId)
	if !projectAuthorized || !principal.HasScope(identity.ScopeJobsCancel) {
		return api.CancelJob403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "credential does not have jobs:cancel scope",
			},
		}, nil
	}
	result, err := s.cancellation.Cancel(ctx, principal, request.ProjectId, request.JobId)
	if err != nil {
		var failure *cancellation.Failure
		if errors.As(err, &failure) {
			switch failure.Code {
			case cancellation.FailureUnauthorized:
				return api.CancelJob401JSONResponse{
					UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
						Code: string(failure.Code), Message: failure.Message,
					},
				}, nil
			case cancellation.FailureForbidden:
				return api.CancelJob403JSONResponse{
					ForbiddenJSONResponse: api.ForbiddenJSONResponse{
						Code: string(failure.Code), Message: failure.Message,
					},
				}, nil
			case cancellation.FailureNotFound:
				return api.CancelJob404JSONResponse{
					Code: string(failure.Code), Message: failure.Message,
				}, nil
			}
		}
		return nil, err
	}
	return api.CancelJob200JSONResponse(toAPICancelResult(result)), nil
}

func (s *server) GetJobArtifacts(
	ctx context.Context,
	request api.GetJobArtifactsRequestObject,
) (api.GetJobArtifactsResponseObject, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return api.GetJobArtifacts401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	principal, projectAuthorized := principalForProjectRequest(principal, request.ProjectId)
	if !projectAuthorized || !principal.HasScope(identity.ScopeArtifactsRead) {
		return api.GetJobArtifacts403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "credential does not have artifacts:read scope",
			},
		}, nil
	}
	artifactSet, err := s.artifacts.Get(ctx, principal, request.ProjectId, request.JobId)
	if err != nil {
		var failure *artifactaccess.Failure
		if errors.As(err, &failure) {
			switch failure.Code {
			case artifactaccess.FailureUnauthorized:
				return api.GetJobArtifacts401JSONResponse{
					UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
						Code: string(failure.Code), Message: failure.Message,
					},
				}, nil
			case artifactaccess.FailureForbidden:
				return api.GetJobArtifacts403JSONResponse{
					ForbiddenJSONResponse: api.ForbiddenJSONResponse{
						Code: string(failure.Code), Message: failure.Message,
					},
				}, nil
			case artifactaccess.FailureNotFound:
				return api.GetJobArtifacts404JSONResponse{
					Code: string(failure.Code), Message: failure.Message,
				}, nil
			}
		}
		return nil, err
	}
	return api.GetJobArtifacts200JSONResponse(toAPIArtifactSet(artifactSet)), nil
}

func (s *server) CreateWebhookSubscription(
	ctx context.Context,
	request api.CreateWebhookSubscriptionRequestObject,
) (api.CreateWebhookSubscriptionResponseObject, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return api.CreateWebhookSubscription401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	principal, projectAuthorized := principalForProjectRequest(principal, request.ProjectId)
	if !projectAuthorized || !principal.HasScope(identity.ScopeWebhooksManage) {
		return api.CreateWebhookSubscription403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "credential does not have webhooks:manage scope",
			},
		}, nil
	}
	if request.Body == nil {
		return webhookCreateBadRequest("request body is required"), nil
	}
	eventTypes := make([]webhook.EventType, len(request.Body.EventTypes))
	for index, eventType := range request.Body.EventTypes {
		eventTypes[index] = webhook.EventType(eventType)
	}
	created, err := s.webhooks.Create(ctx, principal, request.ProjectId, webhook.CreateRequest{
		Endpoint: request.Body.Endpoint, EventTypes: eventTypes,
	})
	if err != nil {
		return webhookCreateFailure(err)
	}
	response := toAPICreatedWebhookSubscription(created)
	return api.CreateWebhookSubscription201JSONResponse(response), nil
}

func (s *server) ListWebhookSubscriptions(
	ctx context.Context,
	request api.ListWebhookSubscriptionsRequestObject,
) (api.ListWebhookSubscriptionsResponseObject, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return api.ListWebhookSubscriptions401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	principal, projectAuthorized := principalForProjectRequest(principal, request.ProjectId)
	if !projectAuthorized || !principal.HasScope(identity.ScopeWebhooksRead) {
		return api.ListWebhookSubscriptions403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "credential does not have webhooks:read scope",
			},
		}, nil
	}
	limit := int32(100)
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}
	subscriptions, err := s.webhooks.List(ctx, principal, request.ProjectId, limit)
	if err != nil {
		return webhookListFailure(err)
	}
	response := api.WebhookSubscriptionList{
		Subscriptions: make([]api.WebhookSubscription, len(subscriptions)),
	}
	for index, subscription := range subscriptions {
		response.Subscriptions[index] = toAPIWebhookSubscription(subscription)
	}
	return api.ListWebhookSubscriptions200JSONResponse(response), nil
}

func (s *server) RotateWebhookSubscriptionSecret(
	ctx context.Context,
	request api.RotateWebhookSubscriptionSecretRequestObject,
) (api.RotateWebhookSubscriptionSecretResponseObject, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return api.RotateWebhookSubscriptionSecret401JSONResponse{
			UnauthorizedJSONResponse: webhookUnauthorizedResponse(),
		}, nil
	}
	principal, projectAuthorized := principalForProjectRequest(principal, request.ProjectId)
	if !projectAuthorized || !principal.HasScope(identity.ScopeWebhooksManage) {
		return api.RotateWebhookSubscriptionSecret403JSONResponse{
			ForbiddenJSONResponse: webhookManageForbiddenResponse(),
		}, nil
	}
	rotated, err := s.webhooks.RotateSecret(ctx, principal, request.ProjectId, request.SubscriptionId)
	if err != nil {
		return webhookRotateFailure(err)
	}
	return api.RotateWebhookSubscriptionSecret200JSONResponse(
		toAPIRotatedWebhookSubscription(rotated),
	), nil
}

func (s *server) DisableWebhookSubscription(
	ctx context.Context,
	request api.DisableWebhookSubscriptionRequestObject,
) (api.DisableWebhookSubscriptionResponseObject, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return api.DisableWebhookSubscription401JSONResponse{
			UnauthorizedJSONResponse: webhookUnauthorizedResponse(),
		}, nil
	}
	principal, projectAuthorized := principalForProjectRequest(principal, request.ProjectId)
	if !projectAuthorized || !principal.HasScope(identity.ScopeWebhooksManage) {
		return api.DisableWebhookSubscription403JSONResponse{
			ForbiddenJSONResponse: webhookManageForbiddenResponse(),
		}, nil
	}
	subscription, err := s.webhooks.Disable(ctx, principal, request.ProjectId, request.SubscriptionId)
	if err != nil {
		return webhookDisableFailure(err)
	}
	return api.DisableWebhookSubscription200JSONResponse(
		toAPIWebhookSubscription(subscription),
	), nil
}

func (s *server) ListWebhookDeliveries(
	ctx context.Context,
	request api.ListWebhookDeliveriesRequestObject,
) (api.ListWebhookDeliveriesResponseObject, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return api.ListWebhookDeliveries401JSONResponse{
			UnauthorizedJSONResponse: webhookUnauthorizedResponse(),
		}, nil
	}
	principal, projectAuthorized := principalForProjectRequest(principal, request.ProjectId)
	if !projectAuthorized || !principal.HasScope(identity.ScopeWebhooksRead) {
		return api.ListWebhookDeliveries403JSONResponse{
			ForbiddenJSONResponse: webhookReadForbiddenResponse(),
		}, nil
	}
	limit := int32(100)
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}
	deliveries, err := s.webhooks.ListDeliveries(
		ctx,
		principal,
		request.ProjectId,
		request.SubscriptionId,
		limit,
	)
	if err != nil {
		return webhookDeliveryListFailure(err)
	}
	response := api.WebhookDeliveryList{Deliveries: make([]api.WebhookDelivery, len(deliveries))}
	for index, delivery := range deliveries {
		response.Deliveries[index] = toAPIWebhookDelivery(delivery)
	}
	return api.ListWebhookDeliveries200JSONResponse(response), nil
}

func (s *server) ReplayWebhookDelivery(
	ctx context.Context,
	request api.ReplayWebhookDeliveryRequestObject,
) (api.ReplayWebhookDeliveryResponseObject, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return api.ReplayWebhookDelivery401JSONResponse{
			UnauthorizedJSONResponse: webhookUnauthorizedResponse(),
		}, nil
	}
	principal, projectAuthorized := principalForProjectRequest(principal, request.ProjectId)
	if !projectAuthorized || !principal.HasScope(identity.ScopeWebhooksManage) {
		return api.ReplayWebhookDelivery403JSONResponse{
			ForbiddenJSONResponse: webhookManageForbiddenResponse(),
		}, nil
	}
	delivery, err := s.webhooks.Replay(
		ctx,
		principal,
		request.ProjectId,
		request.SubscriptionId,
		request.DeliveryId,
	)
	if err != nil {
		return webhookReplayFailure(err)
	}
	return api.ReplayWebhookDelivery200JSONResponse(toAPIWebhookDelivery(delivery)), nil
}

func (s *server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Fields(r.Header.Get("Authorization"))
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			writeError(w, http.StatusUnauthorized, "unauthorized", authenticationFailureMessage)
			return
		}
		principal, err := s.authenticator.Authenticate(r.Context(), parts[1])
		if errors.Is(err, identity.ErrInvalidCredential) {
			message := authenticationFailureMessage
			if strings.HasPrefix(parts[1], "vla_") {
				message = serviceAuthenticationFailureMessage
			}
			writeError(w, http.StatusUnauthorized, "unauthorized", message)
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

func principalForProjectRequest(
	principal identity.Principal,
	projectID uuid.UUID,
) (identity.Principal, bool) {
	// Released Service APIs leave Project mismatch handling to each domain service.
	if principal.Kind == identity.PrincipalKindService {
		return principal, true
	}
	return principal.ForProject(projectID)
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

func webhookCreateFailure(err error) (api.CreateWebhookSubscriptionResponseObject, error) {
	var failure *webhook.Failure
	if !errors.As(err, &failure) {
		return nil, err
	}
	response := api.Error{Code: string(failure.Code), Message: failure.Message}
	switch failure.Code {
	case webhook.FailureUnauthorized:
		return api.CreateWebhookSubscription401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case webhook.FailureForbidden:
		return api.CreateWebhookSubscription403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case webhook.FailureInvalidRequest:
		return api.CreateWebhookSubscription400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	default:
		return nil, err
	}
}

func webhookCreateBadRequest(message string) api.CreateWebhookSubscription400JSONResponse {
	return api.CreateWebhookSubscription400JSONResponse{
		BadRequestJSONResponse: api.BadRequestJSONResponse{
			Code: "invalid_request", Message: message,
		},
	}
}

func webhookListFailure(err error) (api.ListWebhookSubscriptionsResponseObject, error) {
	var failure *webhook.Failure
	if !errors.As(err, &failure) {
		return nil, err
	}
	response := api.Error{Code: string(failure.Code), Message: failure.Message}
	switch failure.Code {
	case webhook.FailureUnauthorized:
		return api.ListWebhookSubscriptions401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case webhook.FailureForbidden:
		return api.ListWebhookSubscriptions403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case webhook.FailureInvalidRequest:
		return api.ListWebhookSubscriptions400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	default:
		return nil, err
	}
}

func webhookRotateFailure(err error) (api.RotateWebhookSubscriptionSecretResponseObject, error) {
	failure, response, ok := webhookFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case webhook.FailureUnauthorized:
		return api.RotateWebhookSubscriptionSecret401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case webhook.FailureForbidden:
		return api.RotateWebhookSubscriptionSecret403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case webhook.FailureInvalidRequest:
		return api.RotateWebhookSubscriptionSecret400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case webhook.FailureNotFound:
		return api.RotateWebhookSubscriptionSecret404JSONResponse(response), nil
	case webhook.FailureConflict:
		return api.RotateWebhookSubscriptionSecret409JSONResponse(response), nil
	default:
		return nil, err
	}
}

func webhookDisableFailure(err error) (api.DisableWebhookSubscriptionResponseObject, error) {
	failure, response, ok := webhookFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case webhook.FailureUnauthorized:
		return api.DisableWebhookSubscription401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case webhook.FailureForbidden:
		return api.DisableWebhookSubscription403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case webhook.FailureNotFound:
		return api.DisableWebhookSubscription404JSONResponse(response), nil
	default:
		return nil, err
	}
}

func webhookDeliveryListFailure(err error) (api.ListWebhookDeliveriesResponseObject, error) {
	failure, response, ok := webhookFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case webhook.FailureUnauthorized:
		return api.ListWebhookDeliveries401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case webhook.FailureForbidden:
		return api.ListWebhookDeliveries403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case webhook.FailureInvalidRequest:
		return api.ListWebhookDeliveries400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case webhook.FailureNotFound:
		return api.ListWebhookDeliveries404JSONResponse(response), nil
	default:
		return nil, err
	}
}

func webhookReplayFailure(err error) (api.ReplayWebhookDeliveryResponseObject, error) {
	failure, response, ok := webhookFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case webhook.FailureUnauthorized:
		return api.ReplayWebhookDelivery401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case webhook.FailureForbidden:
		return api.ReplayWebhookDelivery403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case webhook.FailureInvalidRequest:
		return api.ReplayWebhookDelivery400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case webhook.FailureNotFound:
		return api.ReplayWebhookDelivery404JSONResponse(response), nil
	case webhook.FailureConflict:
		return api.ReplayWebhookDelivery409JSONResponse(response), nil
	default:
		return nil, err
	}
}

func webhookFailureResponse(err error) (*webhook.Failure, api.Error, bool) {
	var failure *webhook.Failure
	if !errors.As(err, &failure) {
		return nil, api.Error{}, false
	}
	return failure, api.Error{Code: string(failure.Code), Message: failure.Message}, true
}

func webhookUnauthorizedResponse() api.UnauthorizedJSONResponse {
	return api.UnauthorizedJSONResponse{
		Code: "unauthorized", Message: authenticationFailureMessage,
	}
}

func webhookManageForbiddenResponse() api.ForbiddenJSONResponse {
	return api.ForbiddenJSONResponse{
		Code: "forbidden", Message: "credential does not have webhooks:manage scope",
	}
}

func webhookReadForbiddenResponse() api.ForbiddenJSONResponse {
	return api.ForbiddenJSONResponse{
		Code: "forbidden", Message: "credential does not have webhooks:read scope",
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

func toAPICancelResult(result cancellation.Result) api.CancelResult {
	response := api.CancelResult{
		Billable:       result.Billable,
		CancellationId: result.CancellationID,
		DecidedAt:      result.DecidedAt,
		Decision:       api.CancelDecision(result.Decision),
		JobId:          result.JobID,
		JobVersion:     result.JobVersion,
		State:          api.JobState(result.State),
	}
	if result.Charge != nil {
		response.Charge = &api.Charge{
			AmountMinor: result.Charge.AmountMinor,
			ChargeId:    result.Charge.ID,
			Currency:    result.Charge.Currency,
			PostedAt:    result.Charge.PostedAt,
			Reason:      api.ChargeReason(result.Charge.Reason),
		}
	}
	return response
}

func toAPIArtifactSet(artifactSet artifactaccess.ArtifactSet) api.ArtifactSet {
	artifacts := make([]api.ArtifactDownload, len(artifactSet.Artifacts))
	for index, artifact := range artifactSet.Artifacts {
		artifacts[index] = api.ArtifactDownload{
			ArtifactId:           artifact.ID,
			ContentType:          artifact.ContentType,
			DownloadUrl:          artifact.DownloadURL,
			DownloadUrlExpiresAt: artifact.DownloadURLExpiresAt,
			Kind:                 api.ArtifactDownloadKind(artifact.Kind),
			ObjectVersionId:      artifact.ObjectVersionID,
			Ordinal:              artifact.Ordinal,
			Sha256:               hex.EncodeToString(artifact.SHA256[:]),
			SizeBytes:            artifact.SizeBytes,
		}
	}
	return api.ArtifactSet{
		ArtifactSetId:      artifactSet.ID,
		Artifacts:          artifacts,
		CommittedAt:        artifactSet.CommittedAt,
		JobId:              artifactSet.JobID,
		RetentionExpiresAt: artifactSet.RetentionExpiresAt,
	}
}

func toAPIWebhookSubscription(subscription webhook.Subscription) api.WebhookSubscription {
	eventTypes := make([]api.WebhookEventType, len(subscription.EventTypes))
	for index, eventType := range subscription.EventTypes {
		eventTypes[index] = api.WebhookEventType(eventType)
	}
	return api.WebhookSubscription{
		CreatedAt:      subscription.CreatedAt,
		DisabledAt:     subscription.DisabledAt,
		Endpoint:       subscription.Endpoint,
		EventTypes:     eventTypes,
		ProjectId:      subscription.ProjectID,
		SecretRevision: subscription.SecretRevision,
		State:          api.WebhookSubscriptionState(subscription.State),
		SubscriptionId: subscription.ID,
	}
}

func toAPICreatedWebhookSubscription(created webhook.CreatedSubscription) api.CreatedWebhookSubscription {
	subscription := toAPIWebhookSubscription(created.Subscription)
	return api.CreatedWebhookSubscription{
		CreatedAt:      subscription.CreatedAt,
		Endpoint:       subscription.Endpoint,
		EventTypes:     subscription.EventTypes,
		ProjectId:      subscription.ProjectId,
		SecretRevision: subscription.SecretRevision,
		SigningSecret:  created.SigningSecret,
		State:          subscription.State,
		SubscriptionId: subscription.SubscriptionId,
	}
}

func toAPIRotatedWebhookSubscription(rotated webhook.RotatedSubscription) api.RotatedWebhookSubscription {
	subscription := toAPIWebhookSubscription(rotated.Subscription)
	return api.RotatedWebhookSubscription{
		CreatedAt:                subscription.CreatedAt,
		Endpoint:                 subscription.Endpoint,
		EventTypes:               subscription.EventTypes,
		PreviousSecretValidUntil: rotated.PreviousSecretValidUntil,
		ProjectId:                subscription.ProjectId,
		SecretRevision:           subscription.SecretRevision,
		SigningSecret:            rotated.SigningSecret,
		State:                    subscription.State,
		SubscriptionId:           subscription.SubscriptionId,
	}
}

func toAPIWebhookDelivery(delivery webhook.Delivery) api.WebhookDelivery {
	return api.WebhookDelivery{
		Attempts:        delivery.Attempts,
		CreatedAt:       delivery.CreatedAt,
		DeadLetteredAt:  delivery.DeadLetteredAt,
		DeliveredAt:     delivery.DeliveredAt,
		DeliveryId:      delivery.ID,
		EventId:         delivery.EventID,
		EventType:       api.WebhookEventType(delivery.EventType),
		Generation:      delivery.Generation,
		JobId:           delivery.JobID,
		JobVersion:      delivery.JobVersion,
		LastAttemptAt:   delivery.LastAttemptAt,
		LastHttpStatus:  delivery.LastHTTPStatus,
		RetryDeadlineAt: delivery.RetryDeadlineAt,
		State:           api.WebhookDeliveryState(delivery.State),
		UpdatedAt:       delivery.UpdatedAt,
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(api.Error{Code: code, Message: message})
}
