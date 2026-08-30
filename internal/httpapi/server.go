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
	"github.com/vivym/vela/internal/breakglass"
	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/debugdump"
	"github.com/vivym/vela/internal/identity"
	"github.com/vivym/vela/internal/organizationreporting"
	"github.com/vivym/vela/internal/remediation"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/webhook"
)

const (
	maxRequestBodyBytes                  = 1 << 20
	authenticationFailureMessage         = "valid bearer credential is required"
	serviceAuthenticationFailureMessage  = "valid Service Principal credential is required"
	platformAuthenticationFailureMessage = "valid Platform Operator credential is required"
	breakGlassServiceUnavailableMessage  = "Break-glass dependency is unavailable"
	remediationServiceUnavailableMessage = "Remediation dependency is unavailable"
	platformBreakGlassPathPrefix         = "/v1/platform/break-glass/"
	platformRemediationPathPrefix        = "/v1/platform/remediation/"
)

type Config struct {
	Observer               func(http.Handler) http.Handler
	Authenticator          *identity.Authenticator
	PlatformAuthenticator  *breakglass.Authenticator
	BreakGlass             *breakglass.Service
	Remediation            *remediation.Service
	IdentityAdministration *identity.AdministrationService
	OrganizationReporting  *organizationreporting.Service
	Retention              *retention.Service
	DebugDumps             *debugdump.Service
	Admission              *admission.Service
	Cancellation           *cancellation.Service
	StageGraphCancellation stageGraphCanceler
	Artifacts              *artifactaccess.Service
	Webhooks               *webhook.Service
}

type server struct {
	authenticator          *identity.Authenticator
	platformAuthenticator  *breakglass.Authenticator
	breakGlass             *breakglass.Service
	remediation            *remediation.Service
	identityAdministration *identity.AdministrationService
	organizationReporting  *organizationreporting.Service
	retention              *retention.Service
	debugDumps             *debugdump.Service
	admission              *admission.Service
	cancellation           *cancellation.Service
	stageGraphCancellation stageGraphCanceler
	artifacts              *artifactaccess.Service
	webhooks               *webhook.Service
}

type stageGraphCanceler interface {
	Cancel(
		context.Context,
		identity.Principal,
		uuid.UUID,
		uuid.UUID,
	) (cancellation.Result, bool, error)
}

type principalContextKey struct{}
type platformOperatorContextKey struct{}

func NewHandler(config Config) (http.Handler, error) {
	if config.Authenticator == nil {
		return nil, errors.New("missing HTTP API authenticator")
	}
	if config.IdentityAdministration == nil {
		return nil, errors.New("missing HTTP API identity Administration service")
	}
	if config.OrganizationReporting == nil {
		return nil, errors.New("missing HTTP API Organization reporting service")
	}
	if config.Retention == nil {
		return nil, errors.New("missing HTTP API retention service")
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
		authenticator:          config.Authenticator,
		platformAuthenticator:  config.PlatformAuthenticator,
		breakGlass:             config.BreakGlass,
		remediation:            config.Remediation,
		identityAdministration: config.IdentityAdministration,
		organizationReporting:  config.OrganizationReporting,
		retention:              config.Retention,
		debugDumps:             config.DebugDumps,
		admission:              config.Admission,
		cancellation:           config.Cancellation,
		stageGraphCancellation: config.StageGraphCancellation,
		artifacts:              config.Artifacts,
		webhooks:               config.Webhooks,
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
	if config.Observer != nil {
		router.Use(config.Observer)
	}
	router.Use(implementation.limitRequestBody)
	router.Use(implementation.authenticate)
	router.Use(oapimiddleware.OapiRequestValidatorWithOptions(openAPI, &oapimiddleware.Options{
		ErrorHandler: func(w http.ResponseWriter, _ string, _ int) {
			writeError(w, http.StatusBadRequest, "invalid_request", "request does not match the API contract")
		},
	}))
	return api.HandlerFromMux(strict, router), nil
}

func (s *server) CreateRemediationOperation(
	ctx context.Context,
	request api.CreateRemediationOperationRequestObject,
) (api.CreateRemediationOperationResponseObject, error) {
	operator, ok := platformOperatorFromContext(ctx)
	if !ok {
		return api.CreateRemediationOperation401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: platformAuthenticationFailureMessage,
			},
		}, nil
	}
	if s.remediation == nil {
		return api.CreateRemediationOperation503JSONResponse{
			ServiceUnavailableJSONResponse: remediationUnavailableResponse(),
		}, nil
	}
	if request.Body == nil {
		return api.CreateRemediationOperation400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse{
				Code: "invalid_request", Message: "remediation request body is required",
			},
		}, nil
	}
	evidence, err := hex.DecodeString(request.Body.EvidenceSha256)
	if err != nil {
		return api.CreateRemediationOperation400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse{
				Code: "invalid_request", Message: "remediation evidence digest is invalid",
			},
		}, nil
	}
	created, err := s.remediation.Request(ctx, remediation.Request{
		OperationID:           uuid.UUID(request.Body.OperationId),
		WorkerID:              uuid.UUID(request.Body.WorkerId),
		WorkerEpoch:           request.Body.WorkerEpoch,
		NodeIdentity:          request.Body.NodeIdentity,
		DeviceIdentity:        request.Body.GpuUuid,
		FailureClass:          request.Body.FailureClass,
		EvidenceDigest:        evidence,
		CertificationRevision: request.Body.CertificationRevision,
		ActionLevel:           remediation.ActionLevel(request.Body.ActionLevel),
		IdempotencyKey:        string(request.Params.IdempotencyKey),
		RequestedBy:           remediationOperatorIdentity(operator),
	})
	if err != nil {
		return createRemediationOperationFailure(err)
	}
	operation, err := s.remediation.Get(ctx, created.OperationID)
	if err != nil {
		return createRemediationOperationFailure(err)
	}
	projection := toAPIRemediationOperation(operation)
	if created.Replayed {
		return api.CreateRemediationOperation200JSONResponse(projection), nil
	}
	return api.CreateRemediationOperation201JSONResponse(projection), nil
}

func (s *server) GetRemediationOperation(
	ctx context.Context,
	request api.GetRemediationOperationRequestObject,
) (api.GetRemediationOperationResponseObject, error) {
	if _, ok := platformOperatorFromContext(ctx); !ok {
		return api.GetRemediationOperation401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: platformAuthenticationFailureMessage,
			},
		}, nil
	}
	if s.remediation == nil {
		return api.GetRemediationOperation503JSONResponse{
			ServiceUnavailableJSONResponse: remediationUnavailableResponse(),
		}, nil
	}
	operation, err := s.remediation.Get(ctx, uuid.UUID(request.RemediationOperationId))
	if err != nil {
		return getRemediationOperationFailure(err)
	}
	return api.GetRemediationOperation200JSONResponse(toAPIRemediationOperation(operation)), nil
}

func (s *server) ApproveRemediationOperation(
	ctx context.Context,
	request api.ApproveRemediationOperationRequestObject,
) (api.ApproveRemediationOperationResponseObject, error) {
	operator, ok := platformOperatorFromContext(ctx)
	if !ok {
		return api.ApproveRemediationOperation401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: platformAuthenticationFailureMessage,
			},
		}, nil
	}
	if s.remediation == nil {
		return api.ApproveRemediationOperation503JSONResponse{
			ServiceUnavailableJSONResponse: remediationUnavailableResponse(),
		}, nil
	}
	operationID := uuid.UUID(request.RemediationOperationId)
	if _, err := s.remediation.Approve(ctx, operationID, remediationOperatorIdentity(operator)); err != nil {
		return approveRemediationOperationFailure(err)
	}
	operation, err := s.remediation.Get(ctx, operationID)
	if err != nil {
		return approveRemediationOperationFailure(err)
	}
	return api.ApproveRemediationOperation200JSONResponse(toAPIRemediationOperation(operation)), nil
}

func (s *server) StartRemediationOperation(
	ctx context.Context,
	request api.StartRemediationOperationRequestObject,
) (api.StartRemediationOperationResponseObject, error) {
	operator, ok := platformOperatorFromContext(ctx)
	if !ok {
		return api.StartRemediationOperation401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: platformAuthenticationFailureMessage,
			},
		}, nil
	}
	if s.remediation == nil {
		return api.StartRemediationOperation503JSONResponse{
			ServiceUnavailableJSONResponse: remediationUnavailableResponse(),
		}, nil
	}
	operationID := uuid.UUID(request.RemediationOperationId)
	operation, err := s.remediation.Get(ctx, operationID)
	if err != nil {
		return startRemediationOperationFailure(err)
	}
	if _, err := s.remediation.Start(
		ctx,
		operationID,
		operation.WorkerID,
		operation.WorkerEpoch,
		remediationOperatorIdentity(operator),
	); err != nil {
		return startRemediationOperationFailure(err)
	}
	operation, err = s.remediation.Get(ctx, operationID)
	if err != nil {
		return startRemediationOperationFailure(err)
	}
	return api.StartRemediationOperation200JSONResponse(toAPIRemediationOperation(operation)), nil
}

func remediationOperatorIdentity(operator breakglass.Operator) string {
	return "platform-operator/" + operator.ID.String()
}

func remediationUnavailableResponse() api.ServiceUnavailableJSONResponse {
	return api.ServiceUnavailableJSONResponse{
		Code: "service_unavailable", Message: remediationServiceUnavailableMessage,
	}
}

func (s *server) CreateBreakGlassRequest(
	ctx context.Context,
	request api.CreateBreakGlassRequestRequestObject,
) (api.CreateBreakGlassRequestResponseObject, error) {
	operator, ok := platformOperatorFromContext(ctx)
	if !ok {
		return api.CreateBreakGlassRequest401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: platformAuthenticationFailureMessage,
			},
		}, nil
	}
	if request.Body == nil {
		return api.CreateBreakGlassRequest400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse{
				Code: "invalid_request", Message: "request body is required",
			},
		}, nil
	}
	if s.breakGlass == nil {
		return nil, errors.New("service for Break-glass Access is not configured")
	}
	scopes := make([]breakglass.Scope, len(request.Body.Scopes))
	for index, scope := range request.Body.Scopes {
		scopes[index] = breakglass.Scope(scope)
	}
	created, replayed, err := s.breakGlass.Request(
		ctx,
		operator,
		string(request.Params.IdempotencyKey),
		breakglass.RequestInput{
			Target: breakglass.Target{
				OrganizationID: request.Body.OrganizationId,
				ProjectID:      request.Body.ProjectId,
				JobID:          request.Body.JobId,
			},
			Scopes:                   scopes,
			ReasonCode:               breakglass.ReasonCode(request.Body.ReasonCode),
			TicketReference:          request.Body.TicketReference,
			RequestedDurationSeconds: request.Body.RequestedDurationSeconds,
		},
	)
	if err != nil {
		return createBreakGlassRequestFailure(err)
	}
	response := toAPIBreakGlassRequest(created)
	if replayed {
		return api.CreateBreakGlassRequest200JSONResponse(response), nil
	}
	return api.CreateBreakGlassRequest201JSONResponse(response), nil
}

func (s *server) GetBreakGlassRequest(
	ctx context.Context,
	request api.GetBreakGlassRequestRequestObject,
) (api.GetBreakGlassRequestResponseObject, error) {
	operator, ok := platformOperatorFromContext(ctx)
	if !ok {
		return api.GetBreakGlassRequest401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: platformAuthenticationFailureMessage,
			},
		}, nil
	}
	if s.breakGlass == nil {
		return nil, errors.New("service for Break-glass Access is not configured")
	}
	result, err := s.breakGlass.GetRequest(ctx, operator, request.BreakGlassRequestId)
	if err != nil {
		return getBreakGlassRequestFailure(err)
	}
	return api.GetBreakGlassRequest200JSONResponse(toAPIBreakGlassRequest(result)), nil
}

func (s *server) ApproveBreakGlassRequest(
	ctx context.Context,
	request api.ApproveBreakGlassRequestRequestObject,
) (api.ApproveBreakGlassRequestResponseObject, error) {
	operator, ok := platformOperatorFromContext(ctx)
	if !ok {
		return api.ApproveBreakGlassRequest401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: platformAuthenticationFailureMessage,
			},
		}, nil
	}
	if s.breakGlass == nil {
		return nil, errors.New("service for Break-glass Access is not configured")
	}
	result, err := s.breakGlass.Approve(ctx, operator, request.BreakGlassRequestId)
	if err != nil {
		return approveBreakGlassRequestFailure(err)
	}
	return api.ApproveBreakGlassRequest200JSONResponse(toAPIBreakGlassRequest(result)), nil
}

func (s *server) RevokeBreakGlassGrant(
	ctx context.Context,
	request api.RevokeBreakGlassGrantRequestObject,
) (api.RevokeBreakGlassGrantResponseObject, error) {
	operator, ok := platformOperatorFromContext(ctx)
	if !ok {
		return api.RevokeBreakGlassGrant401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: platformAuthenticationFailureMessage,
			},
		}, nil
	}
	if s.breakGlass == nil {
		return nil, errors.New("service for Break-glass Access is not configured")
	}
	result, err := s.breakGlass.Revoke(ctx, operator, request.BreakGlassGrantId)
	if err != nil {
		return revokeBreakGlassGrantFailure(err)
	}
	return api.RevokeBreakGlassGrant200JSONResponse(toAPIBreakGlassRequest(result)), nil
}

func (s *server) GetBreakGlassRequestContent(
	ctx context.Context,
	request api.GetBreakGlassRequestContentRequestObject,
) (api.GetBreakGlassRequestContentResponseObject, error) {
	operator, ok := platformOperatorFromContext(ctx)
	if !ok {
		return api.GetBreakGlassRequestContent401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: platformAuthenticationFailureMessage,
			},
		}, nil
	}
	if s.breakGlass == nil {
		return nil, errors.New("service for Break-glass Access is not configured")
	}
	content, err := s.breakGlass.ReadRequestContent(ctx, operator, request.BreakGlassGrantId)
	if err != nil {
		return getBreakGlassRequestContentFailure(err)
	}
	return api.GetBreakGlassRequestContent200JSONResponse{
		OrganizationId: content.OrganizationID,
		ProjectId:      content.ProjectID,
		JobId:          content.JobID,
		RequestContent: content.RequestContent,
	}, nil
}

func (s *server) GetBreakGlassArtifacts(
	ctx context.Context,
	request api.GetBreakGlassArtifactsRequestObject,
) (api.GetBreakGlassArtifactsResponseObject, error) {
	operator, ok := platformOperatorFromContext(ctx)
	if !ok {
		return api.GetBreakGlassArtifacts401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: platformAuthenticationFailureMessage,
			},
		}, nil
	}
	if s.breakGlass == nil {
		return nil, errors.New("service for Break-glass Access is not configured")
	}
	artifacts, err := s.breakGlass.GetArtifacts(ctx, operator, request.BreakGlassGrantId)
	if err != nil {
		return getBreakGlassArtifactsFailure(err)
	}
	return api.GetBreakGlassArtifacts200JSONResponse(toAPIBreakGlassArtifactSet(artifacts)), nil
}

func (s *server) GetProjectRetentionPolicy(
	ctx context.Context,
	request api.GetProjectRetentionPolicyRequestObject,
) (api.GetProjectRetentionPolicyResponseObject, error) {
	principal, status := retentionPolicyAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeRetentionPolicyManage,
	)
	if status == http.StatusUnauthorized {
		return api.GetProjectRetentionPolicy401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.GetProjectRetentionPolicy403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human ProjectAdmin authorization is required",
			},
		}, nil
	}
	if status == http.StatusNotFound {
		return api.GetProjectRetentionPolicy404JSONResponse{
			Code: "not_found", Message: "Project is not visible",
		}, nil
	}
	policy, err := s.retention.GetProjectPolicy(ctx, principal, request.ProjectId)
	if err != nil {
		return getProjectRetentionPolicyFailure(err)
	}
	return api.GetProjectRetentionPolicy200JSONResponse(toAPIProjectRetentionPolicy(policy)), nil
}

func (s *server) SetProjectRetentionPolicy(
	ctx context.Context,
	request api.SetProjectRetentionPolicyRequestObject,
) (api.SetProjectRetentionPolicyResponseObject, error) {
	principal, status := retentionPolicyAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeRetentionPolicyManage,
	)
	if status == http.StatusUnauthorized {
		return api.SetProjectRetentionPolicy401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.SetProjectRetentionPolicy403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human ProjectAdmin authorization is required",
			},
		}, nil
	}
	if status == http.StatusNotFound {
		return api.SetProjectRetentionPolicy404JSONResponse{
			Code: "not_found", Message: "Project is not visible",
		}, nil
	}
	if request.Body == nil {
		return api.SetProjectRetentionPolicy400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse{
				Code: "invalid_request", Message: "request body is required",
			},
		}, nil
	}
	policy, err := s.retention.SetProjectPolicy(
		ctx,
		principal,
		request.ProjectId,
		int32(request.Body.ArtifactRetentionDays),
	)
	if err != nil {
		return setProjectRetentionPolicyFailure(err)
	}
	return api.SetProjectRetentionPolicy200JSONResponse(toAPIProjectRetentionPolicy(policy)), nil
}

func (s *server) AuthorizeDebugDump(
	ctx context.Context,
	request api.AuthorizeDebugDumpRequestObject,
) (api.AuthorizeDebugDumpResponseObject, error) {
	principal, status := retentionPolicyAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeDebugDumpsManage,
	)
	if status == http.StatusUnauthorized {
		return api.AuthorizeDebugDump401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.AuthorizeDebugDump403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human ProjectAdmin debug dump authority is required",
			},
		}, nil
	}
	if status == http.StatusNotFound {
		return api.AuthorizeDebugDump404JSONResponse{
			Code: "not_found", Message: "Project or Job is not visible",
		}, nil
	}
	if s.debugDumps == nil {
		return api.AuthorizeDebugDump503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse{
				Code: "service_unavailable", Message: "Debug dump dependency is unavailable",
			},
		}, nil
	}
	if request.Body == nil {
		return api.AuthorizeDebugDump400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse{
				Code: "invalid_request", Message: "debug dump authorization body is required",
			},
		}, nil
	}
	authorization, err := s.debugDumps.Authorize(
		ctx,
		principal,
		request.ProjectId,
		request.JobId,
		string(request.Params.IdempotencyKey),
		debugdump.Purpose(request.Body.Purpose),
	)
	if err != nil {
		return authorizeDebugDumpFailure(err)
	}
	response := toAPIDebugDumpAuthorization(authorization)
	if authorization.Replayed {
		return api.AuthorizeDebugDump200JSONResponse(response), nil
	}
	return api.AuthorizeDebugDump201JSONResponse(response), nil
}

func (s *server) GetDebugDumpAuthorization(
	ctx context.Context,
	request api.GetDebugDumpAuthorizationRequestObject,
) (api.GetDebugDumpAuthorizationResponseObject, error) {
	principal, status := retentionPolicyAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeDebugDumpsManage,
	)
	if status == http.StatusUnauthorized {
		return api.GetDebugDumpAuthorization401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.GetDebugDumpAuthorization403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human ProjectAdmin debug dump authority is required",
			},
		}, nil
	}
	if status == http.StatusNotFound {
		return api.GetDebugDumpAuthorization404JSONResponse{
			Code: "not_found", Message: "debug dump authorization is not visible",
		}, nil
	}
	if s.debugDumps == nil {
		return api.GetDebugDumpAuthorization503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse{
				Code: "service_unavailable", Message: "Debug dump dependency is unavailable",
			},
		}, nil
	}
	authorization, err := s.debugDumps.GetAuthorization(
		ctx,
		principal,
		request.ProjectId,
		request.JobId,
		request.DebugDumpAuthorizationId,
	)
	if err != nil {
		return getDebugDumpAuthorizationFailure(err)
	}
	return api.GetDebugDumpAuthorization200JSONResponse(
		toAPIDebugDumpAuthorization(authorization),
	), nil
}

func (s *server) RevokeDebugDumpAuthorization(
	ctx context.Context,
	request api.RevokeDebugDumpAuthorizationRequestObject,
) (api.RevokeDebugDumpAuthorizationResponseObject, error) {
	principal, status := retentionPolicyAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeDebugDumpsManage,
	)
	if status == http.StatusUnauthorized {
		return api.RevokeDebugDumpAuthorization401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.RevokeDebugDumpAuthorization403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human ProjectAdmin debug dump authority is required",
			},
		}, nil
	}
	if status == http.StatusNotFound {
		return api.RevokeDebugDumpAuthorization404JSONResponse{
			Code: "not_found", Message: "debug dump authorization is not visible",
		}, nil
	}
	if s.debugDumps == nil {
		return api.RevokeDebugDumpAuthorization503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse{
				Code: "service_unavailable", Message: "Debug dump dependency is unavailable",
			},
		}, nil
	}
	authorization, err := s.debugDumps.RevokeAuthorization(
		ctx,
		principal,
		request.ProjectId,
		request.JobId,
		request.DebugDumpAuthorizationId,
		string(request.Params.IdempotencyKey),
	)
	if err != nil {
		return revokeDebugDumpAuthorizationFailure(err)
	}
	return api.RevokeDebugDumpAuthorization200JSONResponse(
		toAPIDebugDumpAuthorization(authorization),
	), nil
}

func (s *server) ListDebugDumps(
	ctx context.Context,
	request api.ListDebugDumpsRequestObject,
) (api.ListDebugDumpsResponseObject, error) {
	principal, status := retentionPolicyAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeDebugDumpsManage,
	)
	if status == http.StatusUnauthorized {
		return api.ListDebugDumps401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.ListDebugDumps403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human ProjectAdmin debug dump authority is required",
			},
		}, nil
	}
	if status == http.StatusNotFound {
		return api.ListDebugDumps404JSONResponse{
			Code: "not_found", Message: "debug dump authorization is not visible",
		}, nil
	}
	if s.debugDumps == nil {
		return api.ListDebugDumps503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse{
				Code: "service_unavailable", Message: "Debug dump dependency is unavailable",
			},
		}, nil
	}
	dumps, err := s.debugDumps.ListDumps(
		ctx,
		principal,
		request.ProjectId,
		request.JobId,
		request.DebugDumpAuthorizationId,
	)
	if err != nil {
		return listDebugDumpsFailure(err)
	}
	response := api.DebugDumpList{Dumps: make([]api.DebugDump, len(dumps))}
	for index := range dumps {
		response.Dumps[index] = toAPIDebugDump(dumps[index])
	}
	return api.ListDebugDumps200JSONResponse(response), nil
}

func (s *server) ReadDebugDump(
	ctx context.Context,
	request api.ReadDebugDumpRequestObject,
) (api.ReadDebugDumpResponseObject, error) {
	principal, status := retentionPolicyAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeDebugDumpsManage,
	)
	if status == http.StatusUnauthorized {
		return api.ReadDebugDump401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.ReadDebugDump403JSONResponse{
			Code: "forbidden", Message: "Human ProjectAdmin debug dump authority is required",
		}, nil
	}
	if status == http.StatusNotFound {
		return api.ReadDebugDump404JSONResponse{
			Code: "not_found", Message: "debug dump target is not visible",
		}, nil
	}
	if s.debugDumps == nil {
		return api.ReadDebugDump503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse{
				Code: "service_unavailable", Message: "Debug dump dependency is unavailable",
			},
		}, nil
	}
	download, err := s.debugDumps.ReadDump(
		ctx,
		principal,
		request.ProjectId,
		request.JobId,
		request.DebugDumpAuthorizationId,
		request.DebugDumpId,
	)
	if err != nil {
		return readDebugDumpFailure(err)
	}
	return api.ReadDebugDump200JSONResponse(toAPIDebugDumpDownload(download)), nil
}

func (s *server) AcceptContentDeletionRequest(
	ctx context.Context,
	request api.AcceptContentDeletionRequestRequestObject,
) (api.AcceptContentDeletionRequestResponseObject, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return api.AcceptContentDeletionRequest401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	principal, projectAuthorized := principalForProjectRequest(principal, request.ProjectId)
	if !projectAuthorized {
		return api.AcceptContentDeletionRequest404JSONResponse{
			Code: "not_found", Message: "Project or Job is not visible",
		}, nil
	}
	if !principal.HasScope(identity.ScopeContentDeletionManage) {
		return api.AcceptContentDeletionRequest403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Content Deletion authority is required",
			},
		}, nil
	}
	deletion, err := s.retention.AcceptContentDeletion(
		ctx,
		principal,
		request.ProjectId,
		request.JobId,
		request.Params.IdempotencyKey,
	)
	if err != nil {
		return acceptContentDeletionRequestFailure(err)
	}
	return api.AcceptContentDeletionRequest202JSONResponse(
		toAPIContentDeletionRequest(deletion),
	), nil
}

func (s *server) GetContentDeletionRequest(
	ctx context.Context,
	request api.GetContentDeletionRequestRequestObject,
) (api.GetContentDeletionRequestResponseObject, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return api.GetContentDeletionRequest401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	principal, projectAuthorized := principalForProjectRequest(principal, request.ProjectId)
	if !projectAuthorized {
		return api.GetContentDeletionRequest404JSONResponse{
			Code: "not_found", Message: "Content Deletion request is not visible",
		}, nil
	}
	if !principal.HasScope(identity.ScopeContentDeletionManage) {
		return api.GetContentDeletionRequest403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Content Deletion authority is required",
			},
		}, nil
	}
	deletion, err := s.retention.GetContentDeletion(
		ctx,
		principal,
		request.ProjectId,
		request.ContentDeletionRequestId,
	)
	if err != nil {
		return getContentDeletionRequestFailure(err)
	}
	return api.GetContentDeletionRequest200JSONResponse(
		toAPIContentDeletionRequestStatus(deletion),
	), nil
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
	var result cancellation.Result
	var err error
	handled := false
	if s.stageGraphCancellation != nil {
		result, handled, err = s.stageGraphCancellation.Cancel(
			ctx, principal, request.ProjectId, request.JobId,
		)
	}
	if !handled && err == nil {
		result, err = s.cancellation.Cancel(ctx, principal, request.ProjectId, request.JobId)
	}
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

func (s *server) GetOrganizationCreditSummary(
	ctx context.Context,
	request api.GetOrganizationCreditSummaryRequestObject,
) (api.GetOrganizationCreditSummaryResponseObject, error) {
	principal, status := organizationIdentityAdministrationPrincipal(
		ctx, request.OrganizationId, identity.ScopeOrganizationBillingRead,
	)
	if status == http.StatusUnauthorized {
		return api.GetOrganizationCreditSummary401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.GetOrganizationCreditSummary403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human Organization billing authorization is required",
			},
		}, nil
	}
	summary, err := s.organizationReporting.GetCreditSummary(
		ctx, principal, request.OrganizationId,
	)
	if err != nil {
		failure, response, ok := organizationReportingFailureResponse(err)
		if !ok {
			return nil, err
		}
		switch failure.Code {
		case organizationreporting.FailureUnauthorized:
			return api.GetOrganizationCreditSummary401JSONResponse{
				UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
			}, nil
		case organizationreporting.FailureForbidden:
			return api.GetOrganizationCreditSummary403JSONResponse{
				ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
			}, nil
		case organizationreporting.FailureInvalid:
			return api.GetOrganizationCreditSummary400JSONResponse{
				BadRequestJSONResponse: api.BadRequestJSONResponse(response),
			}, nil
		case organizationreporting.FailureNotFound:
			return api.GetOrganizationCreditSummary404JSONResponse(response), nil
		default:
			return nil, err
		}
	}
	return api.GetOrganizationCreditSummary200JSONResponse(
		toAPIOrganizationCreditSummary(summary),
	), nil
}

func (s *server) ListOrganizationCharges(
	ctx context.Context,
	request api.ListOrganizationChargesRequestObject,
) (api.ListOrganizationChargesResponseObject, error) {
	principal, status := organizationIdentityAdministrationPrincipal(
		ctx, request.OrganizationId, identity.ScopeOrganizationBillingRead,
	)
	if status == http.StatusUnauthorized {
		return api.ListOrganizationCharges401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.ListOrganizationCharges403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human Organization billing authorization is required",
			},
		}, nil
	}
	limit := int32(100)
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}
	charges, err := s.organizationReporting.ListCharges(
		ctx, principal, request.OrganizationId, limit,
	)
	if err != nil {
		failure, response, ok := organizationReportingFailureResponse(err)
		if !ok {
			return nil, err
		}
		switch failure.Code {
		case organizationreporting.FailureUnauthorized:
			return api.ListOrganizationCharges401JSONResponse{
				UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
			}, nil
		case organizationreporting.FailureForbidden:
			return api.ListOrganizationCharges403JSONResponse{
				ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
			}, nil
		case organizationreporting.FailureInvalid:
			return api.ListOrganizationCharges400JSONResponse{
				BadRequestJSONResponse: api.BadRequestJSONResponse(response),
			}, nil
		default:
			return nil, err
		}
	}
	response := api.OrganizationChargeList{Charges: make([]api.OrganizationCharge, len(charges))}
	for index, charge := range charges {
		response.Charges[index] = toAPIOrganizationCharge(charge)
	}
	return api.ListOrganizationCharges200JSONResponse(response), nil
}

func (s *server) CreateSettlementContact(
	ctx context.Context,
	request api.CreateSettlementContactRequestObject,
) (api.CreateSettlementContactResponseObject, error) {
	principal, status := organizationIdentityAdministrationPrincipal(
		ctx, request.OrganizationId, identity.ScopeOrganizationBillingContactsManage,
	)
	if status == http.StatusUnauthorized {
		return api.CreateSettlementContact401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.CreateSettlementContact403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human settlement-contact authorization is required",
			},
		}, nil
	}
	if request.Body == nil {
		return api.CreateSettlementContact400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse{
				Code: "invalid_request", Message: "request body is required",
			},
		}, nil
	}
	contact, err := s.organizationReporting.CreateSettlementContact(
		ctx,
		principal,
		request.OrganizationId,
		organizationreporting.CreateSettlementContactRequest{
			DisplayName: request.Body.DisplayName,
			Email:       request.Body.Email,
		},
	)
	if err != nil {
		failure, response, ok := organizationReportingFailureResponse(err)
		if !ok {
			return nil, err
		}
		switch failure.Code {
		case organizationreporting.FailureUnauthorized:
			return api.CreateSettlementContact401JSONResponse{
				UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
			}, nil
		case organizationreporting.FailureForbidden:
			return api.CreateSettlementContact403JSONResponse{
				ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
			}, nil
		case organizationreporting.FailureInvalid:
			return api.CreateSettlementContact400JSONResponse{
				BadRequestJSONResponse: api.BadRequestJSONResponse(response),
			}, nil
		default:
			return nil, err
		}
	}
	return api.CreateSettlementContact201JSONResponse(toAPISettlementContact(contact)), nil
}

func (s *server) ListSettlementContacts(
	ctx context.Context,
	request api.ListSettlementContactsRequestObject,
) (api.ListSettlementContactsResponseObject, error) {
	principal, status := organizationIdentityAdministrationPrincipal(
		ctx, request.OrganizationId, identity.ScopeOrganizationBillingContactsRead,
	)
	if status == http.StatusUnauthorized {
		return api.ListSettlementContacts401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.ListSettlementContacts403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human settlement-contact authorization is required",
			},
		}, nil
	}
	limit := int32(100)
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}
	contacts, err := s.organizationReporting.ListSettlementContacts(
		ctx, principal, request.OrganizationId, limit,
	)
	if err != nil {
		failure, response, ok := organizationReportingFailureResponse(err)
		if !ok {
			return nil, err
		}
		switch failure.Code {
		case organizationreporting.FailureUnauthorized:
			return api.ListSettlementContacts401JSONResponse{
				UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
			}, nil
		case organizationreporting.FailureForbidden:
			return api.ListSettlementContacts403JSONResponse{
				ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
			}, nil
		case organizationreporting.FailureInvalid:
			return api.ListSettlementContacts400JSONResponse{
				BadRequestJSONResponse: api.BadRequestJSONResponse(response),
			}, nil
		default:
			return nil, err
		}
	}
	response := api.SettlementContactList{Contacts: make([]api.SettlementContact, len(contacts))}
	for index, contact := range contacts {
		response.Contacts[index] = toAPISettlementContact(contact)
	}
	return api.ListSettlementContacts200JSONResponse(response), nil
}

func (s *server) DisableSettlementContact(
	ctx context.Context,
	request api.DisableSettlementContactRequestObject,
) (api.DisableSettlementContactResponseObject, error) {
	principal, status := organizationIdentityAdministrationPrincipal(
		ctx, request.OrganizationId, identity.ScopeOrganizationBillingContactsManage,
	)
	if status == http.StatusUnauthorized {
		return api.DisableSettlementContact401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.DisableSettlementContact403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human settlement-contact authorization is required",
			},
		}, nil
	}
	contact, err := s.organizationReporting.DisableSettlementContact(
		ctx, principal, request.OrganizationId, request.ContactId,
	)
	if err != nil {
		failure, response, ok := organizationReportingFailureResponse(err)
		if !ok {
			return nil, err
		}
		switch failure.Code {
		case organizationreporting.FailureUnauthorized:
			return api.DisableSettlementContact401JSONResponse{
				UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
			}, nil
		case organizationreporting.FailureForbidden:
			return api.DisableSettlementContact403JSONResponse{
				ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
			}, nil
		case organizationreporting.FailureInvalid:
			return api.DisableSettlementContact400JSONResponse{
				BadRequestJSONResponse: api.BadRequestJSONResponse(response),
			}, nil
		case organizationreporting.FailureNotFound:
			return api.DisableSettlementContact404JSONResponse(response), nil
		default:
			return nil, err
		}
	}
	return api.DisableSettlementContact200JSONResponse(toAPISettlementContact(contact)), nil
}

func (s *server) GetOrganizationUsage(
	ctx context.Context,
	request api.GetOrganizationUsageRequestObject,
) (api.GetOrganizationUsageResponseObject, error) {
	principal, status := organizationIdentityAdministrationPrincipal(
		ctx, request.OrganizationId, identity.ScopeOrganizationUsageRead,
	)
	if status == http.StatusUnauthorized {
		return api.GetOrganizationUsage401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.GetOrganizationUsage403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human Organization usage authorization is required",
			},
		}, nil
	}
	usage, err := s.organizationReporting.GetUsage(
		ctx, principal, request.OrganizationId, request.Params.From, request.Params.To,
	)
	if err != nil {
		failure, response, ok := organizationReportingFailureResponse(err)
		if !ok {
			return nil, err
		}
		switch failure.Code {
		case organizationreporting.FailureUnauthorized:
			return api.GetOrganizationUsage401JSONResponse{
				UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
			}, nil
		case organizationreporting.FailureForbidden:
			return api.GetOrganizationUsage403JSONResponse{
				ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
			}, nil
		case organizationreporting.FailureInvalid:
			return api.GetOrganizationUsage400JSONResponse{
				BadRequestJSONResponse: api.BadRequestJSONResponse(response),
			}, nil
		default:
			return nil, err
		}
	}
	return api.GetOrganizationUsage200JSONResponse(toAPIOrganizationUsage(usage)), nil
}

func (s *server) ListOrganizationAuditEvents(
	ctx context.Context,
	request api.ListOrganizationAuditEventsRequestObject,
) (api.ListOrganizationAuditEventsResponseObject, error) {
	principal, status := organizationIdentityAdministrationPrincipal(
		ctx, request.OrganizationId, identity.ScopeOrganizationAuditRead,
	)
	if status == http.StatusUnauthorized {
		return api.ListOrganizationAuditEvents401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.ListOrganizationAuditEvents403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human Organization audit authorization is required",
			},
		}, nil
	}
	limit := int32(100)
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}
	events, err := s.organizationReporting.ListAuditEvents(
		ctx, principal, request.OrganizationId, limit,
	)
	if err != nil {
		failure, response, ok := organizationReportingFailureResponse(err)
		if !ok {
			return nil, err
		}
		switch failure.Code {
		case organizationreporting.FailureUnauthorized:
			return api.ListOrganizationAuditEvents401JSONResponse{
				UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
			}, nil
		case organizationreporting.FailureForbidden:
			return api.ListOrganizationAuditEvents403JSONResponse{
				ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
			}, nil
		case organizationreporting.FailureInvalid:
			return api.ListOrganizationAuditEvents400JSONResponse{
				BadRequestJSONResponse: api.BadRequestJSONResponse(response),
			}, nil
		default:
			return nil, err
		}
	}
	response := api.OrganizationAuditEventList{
		Events: make([]api.OrganizationAuditEvent, len(events)),
	}
	for index, event := range events {
		response.Events[index] = toAPIOrganizationAuditEvent(event)
	}
	return api.ListOrganizationAuditEvents200JSONResponse(response), nil
}

func (s *server) CreateHumanMember(
	ctx context.Context,
	request api.CreateHumanMemberRequestObject,
) (api.CreateHumanMemberResponseObject, error) {
	principal, status := organizationIdentityAdministrationPrincipal(
		ctx, request.OrganizationId, identity.ScopeOrganizationMembersManage,
	)
	if status == http.StatusUnauthorized {
		return api.CreateHumanMember401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.CreateHumanMember403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human OrganizationOwner authorization is required",
			},
		}, nil
	}
	if request.Body == nil {
		return api.CreateHumanMember400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse{
				Code: "invalid_request", Message: "request body is required",
			},
		}, nil
	}
	displayName := ""
	if request.Body.DisplayName != nil {
		displayName = *request.Body.DisplayName
	}
	member, err := s.identityAdministration.CreateHumanMember(
		ctx,
		principal,
		request.OrganizationId,
		identity.CreateHumanMemberRequest{
			OIDCSubject: request.Body.OidcSubject,
			DisplayName: displayName,
		},
	)
	if err != nil {
		return createHumanMemberFailure(err)
	}
	return api.CreateHumanMember201JSONResponse(toAPIHumanMember(member)), nil
}

func (s *server) ListHumanMembers(
	ctx context.Context,
	request api.ListHumanMembersRequestObject,
) (api.ListHumanMembersResponseObject, error) {
	principal, status := organizationIdentityAdministrationPrincipal(
		ctx, request.OrganizationId, identity.ScopeOrganizationMembersRead,
	)
	if status == http.StatusUnauthorized {
		return api.ListHumanMembers401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.ListHumanMembers403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human OrganizationOwner authorization is required",
			},
		}, nil
	}
	limit := int32(100)
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}
	members, err := s.identityAdministration.ListHumanMembers(
		ctx, principal, request.OrganizationId, limit,
	)
	if err != nil {
		return listHumanMembersFailure(err)
	}
	response := api.HumanMemberList{Members: make([]api.OrganizationMember, len(members))}
	for index, member := range members {
		response.Members[index] = toAPIOrganizationMember(member)
	}
	return api.ListHumanMembers200JSONResponse(response), nil
}

func (s *server) DisableHumanMember(
	ctx context.Context,
	request api.DisableHumanMemberRequestObject,
) (api.DisableHumanMemberResponseObject, error) {
	principal, status := organizationIdentityAdministrationPrincipal(
		ctx, request.OrganizationId, identity.ScopeOrganizationMembersManage,
	)
	if status == http.StatusUnauthorized {
		return api.DisableHumanMember401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.DisableHumanMember403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human OrganizationOwner authorization is required",
			},
		}, nil
	}
	member, err := s.identityAdministration.DisableHumanMember(
		ctx, principal, request.OrganizationId, request.PrincipalId,
	)
	if err != nil {
		return disableHumanMemberFailure(err)
	}
	return api.DisableHumanMember200JSONResponse(toAPIHumanMember(member)), nil
}

func (s *server) AssignOrganizationRole(
	ctx context.Context,
	request api.AssignOrganizationRoleRequestObject,
) (api.AssignOrganizationRoleResponseObject, error) {
	principal, status := organizationIdentityAdministrationPrincipal(
		ctx, request.OrganizationId, identity.ScopeOrganizationMembersManage,
	)
	if status == http.StatusUnauthorized {
		return api.AssignOrganizationRole401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.AssignOrganizationRole403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human OrganizationOwner authorization is required",
			},
		}, nil
	}
	if request.Body == nil {
		return api.AssignOrganizationRole400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse{
				Code: "invalid_request", Message: "request body is required",
			},
		}, nil
	}
	assignment, err := s.identityAdministration.AssignOrganizationRole(
		ctx,
		principal,
		request.OrganizationId,
		request.PrincipalId,
		identity.OrganizationRole(request.Body.Role),
	)
	if err != nil {
		return assignOrganizationRoleFailure(err)
	}
	return api.AssignOrganizationRole200JSONResponse(
		toAPIOrganizationRoleAssignment(assignment),
	), nil
}

func (s *server) RevokeOrganizationRole(
	ctx context.Context,
	request api.RevokeOrganizationRoleRequestObject,
) (api.RevokeOrganizationRoleResponseObject, error) {
	principal, status := organizationIdentityAdministrationPrincipal(
		ctx, request.OrganizationId, identity.ScopeOrganizationMembersManage,
	)
	if status == http.StatusUnauthorized {
		return api.RevokeOrganizationRole401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.RevokeOrganizationRole403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human OrganizationOwner authorization is required",
			},
		}, nil
	}
	assignment, err := s.identityAdministration.RevokeOrganizationRole(
		ctx,
		principal,
		request.OrganizationId,
		request.PrincipalId,
		identity.OrganizationRole(request.Role),
	)
	if err != nil {
		return revokeOrganizationRoleFailure(err)
	}
	return api.RevokeOrganizationRole200JSONResponse(
		toAPIOrganizationRoleAssignment(assignment),
	), nil
}

func (s *server) ListProjectMembers(
	ctx context.Context,
	request api.ListProjectMembersRequestObject,
) (api.ListProjectMembersResponseObject, error) {
	principal, status := projectMembershipAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeProjectMembersRead,
	)
	if status == http.StatusUnauthorized {
		return api.ListProjectMembers401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.ListProjectMembers403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code:    "forbidden",
				Message: "Human OrganizationOwner or ProjectAdmin authorization is required",
			},
		}, nil
	}
	limit := int32(100)
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}
	members, err := s.identityAdministration.ListProjectMembers(
		ctx, principal, request.ProjectId, limit,
	)
	if err != nil {
		return listProjectMembersFailure(err)
	}
	response := api.ProjectMemberList{Members: make([]api.ProjectMember, len(members))}
	for index, member := range members {
		response.Members[index] = toAPIProjectMember(member)
	}
	return api.ListProjectMembers200JSONResponse(response), nil
}

func (s *server) AssignProjectRole(
	ctx context.Context,
	request api.AssignProjectRoleRequestObject,
) (api.AssignProjectRoleResponseObject, error) {
	principal, status := projectMembershipAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeProjectMembersManage,
	)
	if status == http.StatusUnauthorized {
		return api.AssignProjectRole401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.AssignProjectRole403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code:    "forbidden",
				Message: "Human OrganizationOwner or ProjectAdmin authorization is required",
			},
		}, nil
	}
	if request.Body == nil {
		return api.AssignProjectRole400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse{
				Code: "invalid_request", Message: "request body is required",
			},
		}, nil
	}
	assignment, err := s.identityAdministration.AssignProjectRole(
		ctx,
		principal,
		request.ProjectId,
		request.PrincipalId,
		identity.ProjectRole(request.Body.Role),
	)
	if err != nil {
		return assignProjectRoleFailure(err)
	}
	return api.AssignProjectRole200JSONResponse(toAPIProjectRoleAssignment(assignment)), nil
}

func (s *server) RevokeProjectRole(
	ctx context.Context,
	request api.RevokeProjectRoleRequestObject,
) (api.RevokeProjectRoleResponseObject, error) {
	principal, status := projectMembershipAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeProjectMembersManage,
	)
	if status == http.StatusUnauthorized {
		return api.RevokeProjectRole401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.RevokeProjectRole403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code:    "forbidden",
				Message: "Human OrganizationOwner or ProjectAdmin authorization is required",
			},
		}, nil
	}
	assignment, err := s.identityAdministration.RevokeProjectRole(
		ctx,
		principal,
		request.ProjectId,
		request.PrincipalId,
		identity.ProjectRole(request.Role),
	)
	if err != nil {
		return revokeProjectRoleFailure(err)
	}
	return api.RevokeProjectRole200JSONResponse(toAPIProjectRoleAssignment(assignment)), nil
}

func (s *server) CreateServicePrincipal(
	ctx context.Context,
	request api.CreateServicePrincipalRequestObject,
) (api.CreateServicePrincipalResponseObject, error) {
	principal, status := identityAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeServicePrincipalsManage,
	)
	if status == http.StatusUnauthorized {
		return api.CreateServicePrincipal401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.CreateServicePrincipal403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human ProjectAdmin authorization is required",
			},
		}, nil
	}
	if request.Body == nil {
		return api.CreateServicePrincipal400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse{
				Code: "invalid_request", Message: "request body is required",
			},
		}, nil
	}
	created, err := s.identityAdministration.CreateServicePrincipal(
		ctx,
		principal,
		request.ProjectId,
		identity.CreateServicePrincipalRequest{DisplayName: request.Body.DisplayName},
	)
	if err != nil {
		return createServicePrincipalFailure(err)
	}
	return api.CreateServicePrincipal201JSONResponse(toAPIServicePrincipal(created)), nil
}

func (s *server) ListServicePrincipals(
	ctx context.Context,
	request api.ListServicePrincipalsRequestObject,
) (api.ListServicePrincipalsResponseObject, error) {
	principal, status := identityAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeServicePrincipalsRead,
	)
	if status == http.StatusUnauthorized {
		return api.ListServicePrincipals401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.ListServicePrincipals403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human ProjectAdmin authorization is required",
			},
		}, nil
	}
	limit := int32(100)
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}
	principals, err := s.identityAdministration.ListServicePrincipals(
		ctx, principal, request.ProjectId, limit,
	)
	if err != nil {
		return listServicePrincipalsFailure(err)
	}
	response := api.ServicePrincipalList{
		ServicePrincipals: make([]api.ServicePrincipal, len(principals)),
	}
	for index, servicePrincipal := range principals {
		response.ServicePrincipals[index] = toAPIServicePrincipal(servicePrincipal)
	}
	return api.ListServicePrincipals200JSONResponse(response), nil
}

func (s *server) DisableServicePrincipal(
	ctx context.Context,
	request api.DisableServicePrincipalRequestObject,
) (api.DisableServicePrincipalResponseObject, error) {
	principal, status := identityAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeServicePrincipalsManage,
	)
	if status == http.StatusUnauthorized {
		return api.DisableServicePrincipal401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.DisableServicePrincipal403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human ProjectAdmin authorization is required",
			},
		}, nil
	}
	disabled, err := s.identityAdministration.DisableServicePrincipal(
		ctx, principal, request.ProjectId, request.ServicePrincipalId,
	)
	if err != nil {
		return disableServicePrincipalFailure(err)
	}
	return api.DisableServicePrincipal200JSONResponse(toAPIServicePrincipal(disabled)), nil
}

func (s *server) IssueServiceCredential(
	ctx context.Context,
	request api.IssueServiceCredentialRequestObject,
) (api.IssueServiceCredentialResponseObject, error) {
	principal, status := identityAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeServicePrincipalsManage,
	)
	if status == http.StatusUnauthorized {
		return api.IssueServiceCredential401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.IssueServiceCredential403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human ProjectAdmin authorization is required",
			},
		}, nil
	}
	if request.Body == nil {
		return api.IssueServiceCredential400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse{
				Code: "invalid_request", Message: "request body is required",
			},
		}, nil
	}
	scopes := make([]string, len(request.Body.Scopes))
	for index, scope := range request.Body.Scopes {
		scopes[index] = string(scope)
	}
	issued, err := s.identityAdministration.IssueCredential(
		ctx,
		principal,
		request.ProjectId,
		request.ServicePrincipalId,
		identity.IssueCredentialRequest{Scopes: scopes, ExpiresAt: request.Body.ExpiresAt},
	)
	if err != nil {
		return issueServiceCredentialFailure(err)
	}
	return api.IssueServiceCredential201JSONResponse(toAPIIssuedServiceCredential(issued)), nil
}

func (s *server) ListServiceCredentials(
	ctx context.Context,
	request api.ListServiceCredentialsRequestObject,
) (api.ListServiceCredentialsResponseObject, error) {
	principal, status := identityAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeServicePrincipalsRead,
	)
	if status == http.StatusUnauthorized {
		return api.ListServiceCredentials401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.ListServiceCredentials403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human ProjectAdmin authorization is required",
			},
		}, nil
	}
	limit := int32(100)
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}
	credentials, err := s.identityAdministration.ListCredentials(
		ctx, principal, request.ProjectId, request.ServicePrincipalId, limit,
	)
	if err != nil {
		return listServiceCredentialsFailure(err)
	}
	response := api.ServiceCredentialList{
		Credentials: make([]api.ServiceCredential, len(credentials)),
	}
	for index, credential := range credentials {
		response.Credentials[index] = toAPIServiceCredential(credential)
	}
	return api.ListServiceCredentials200JSONResponse(response), nil
}

func (s *server) RevokeServiceCredential(
	ctx context.Context,
	request api.RevokeServiceCredentialRequestObject,
) (api.RevokeServiceCredentialResponseObject, error) {
	principal, status := identityAdministrationPrincipal(
		ctx, request.ProjectId, identity.ScopeServicePrincipalsManage,
	)
	if status == http.StatusUnauthorized {
		return api.RevokeServiceCredential401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: "unauthorized", Message: authenticationFailureMessage,
			},
		}, nil
	}
	if status == http.StatusForbidden {
		return api.RevokeServiceCredential403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: "forbidden", Message: "Human ProjectAdmin authorization is required",
			},
		}, nil
	}
	revoked, err := s.identityAdministration.RevokeCredential(
		ctx,
		principal,
		request.ProjectId,
		request.ServicePrincipalId,
		request.CredentialId,
	)
	if err != nil {
		return revokeServiceCredentialFailure(err)
	}
	return api.RevokeServiceCredential200JSONResponse(toAPIServiceCredential(revoked)), nil
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
		if strings.HasPrefix(r.URL.Path, platformBreakGlassPathPrefix) ||
			strings.HasPrefix(r.URL.Path, platformRemediationPathPrefix) {
			if s.platformAuthenticator == nil {
				writeError(
					w,
					http.StatusServiceUnavailable,
					"identity_unavailable",
					"Platform Operator verification is unavailable",
				)
				return
			}
			operator, err := s.platformAuthenticator.Authenticate(r.Context(), parts[1])
			if errors.Is(err, breakglass.ErrInvalidOperatorCredential) {
				writeError(
					w,
					http.StatusUnauthorized,
					"unauthorized",
					platformAuthenticationFailureMessage,
				)
				return
			}
			if err != nil {
				writeError(
					w,
					http.StatusServiceUnavailable,
					"identity_unavailable",
					"Platform Operator verification is unavailable",
				)
				return
			}
			next.ServeHTTP(
				w,
				r.WithContext(context.WithValue(
					r.Context(), platformOperatorContextKey{}, operator,
				)),
			)
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

func platformOperatorFromContext(ctx context.Context) (breakglass.Operator, bool) {
	operator, ok := ctx.Value(platformOperatorContextKey{}).(breakglass.Operator)
	return operator, ok
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

func identityAdministrationPrincipal(
	ctx context.Context,
	projectID uuid.UUID,
	requiredScope string,
) (identity.Principal, int) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return identity.Principal{}, http.StatusUnauthorized
	}
	principal, ok = principalForProjectRequest(principal, projectID)
	if !ok || principal.Kind != identity.PrincipalKindHuman || !principal.HasScope(requiredScope) {
		return identity.Principal{}, http.StatusForbidden
	}
	return principal, http.StatusOK
}

func retentionPolicyAdministrationPrincipal(
	ctx context.Context,
	projectID uuid.UUID,
	requiredScope string,
) (identity.Principal, int) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return identity.Principal{}, http.StatusUnauthorized
	}
	principal, ok = principal.ForProject(projectID)
	if !ok {
		return identity.Principal{}, http.StatusNotFound
	}
	if principal.Kind != identity.PrincipalKindHuman || !principal.HasScope(requiredScope) {
		return identity.Principal{}, http.StatusForbidden
	}
	return principal, http.StatusOK
}

func organizationIdentityAdministrationPrincipal(
	ctx context.Context,
	organizationID uuid.UUID,
	requiredScope string,
) (identity.Principal, int) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return identity.Principal{}, http.StatusUnauthorized
	}
	principal, ok = principal.ForOrganization(organizationID)
	if !ok || principal.Kind != identity.PrincipalKindHuman || !principal.HasScope(requiredScope) {
		return identity.Principal{}, http.StatusForbidden
	}
	return principal, http.StatusOK
}

func projectMembershipAdministrationPrincipal(
	ctx context.Context,
	projectID uuid.UUID,
	requiredScope string,
) (identity.Principal, int) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return identity.Principal{}, http.StatusUnauthorized
	}
	if principal.Kind != identity.PrincipalKindHuman {
		return identity.Principal{}, http.StatusForbidden
	}
	if projectPrincipal, selected := principal.ForProject(projectID); selected &&
		projectPrincipal.HasScope(requiredScope) {
		return projectPrincipal, http.StatusOK
	}
	if organizationPrincipal, selected := principal.ForOrganization(principal.OrganizationID); selected &&
		organizationPrincipal.HasScope(requiredScope) {
		return organizationPrincipal, http.StatusOK
	}
	return identity.Principal{}, http.StatusForbidden
}

type remediationHTTPFailure struct {
	status  int
	code    string
	message string
}

func classifyRemediationFailure(err error) (remediationHTTPFailure, bool) {
	var failure *remediation.Failure
	if !errors.As(err, &failure) {
		return remediationHTTPFailure{}, false
	}
	switch failure.Code {
	case remediation.FailureInvalid:
		return remediationHTTPFailure{status: http.StatusBadRequest, code: "invalid_request", message: failure.Message}, true
	case remediation.FailureNotFound:
		return remediationHTTPFailure{status: http.StatusNotFound, code: "not_found", message: failure.Message}, true
	case remediation.FailureConflict, remediation.FailureUncertified, remediation.FailureExecution:
		return remediationHTTPFailure{status: http.StatusConflict, code: "conflict", message: failure.Message}, true
	case remediation.FailureUnavailable:
		return remediationHTTPFailure{
			status: http.StatusServiceUnavailable, code: "service_unavailable",
			message: remediationServiceUnavailableMessage,
		}, true
	default:
		return remediationHTTPFailure{}, false
	}
}

func createRemediationOperationFailure(err error) (api.CreateRemediationOperationResponseObject, error) {
	failure, ok := classifyRemediationFailure(err)
	if !ok {
		return nil, err
	}
	switch failure.status {
	case http.StatusBadRequest:
		return api.CreateRemediationOperation400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse{
			Code: failure.code, Message: failure.message,
		}}, nil
	case http.StatusNotFound:
		return api.CreateRemediationOperation404JSONResponse{Code: failure.code, Message: failure.message}, nil
	case http.StatusConflict:
		return api.CreateRemediationOperation409JSONResponse{Code: failure.code, Message: failure.message}, nil
	case http.StatusServiceUnavailable:
		return api.CreateRemediationOperation503JSONResponse{ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse{
			Code: failure.code, Message: failure.message,
		}}, nil
	default:
		return nil, err
	}
}

func getRemediationOperationFailure(err error) (api.GetRemediationOperationResponseObject, error) {
	failure, ok := classifyRemediationFailure(err)
	if !ok {
		return nil, err
	}
	switch failure.status {
	case http.StatusNotFound:
		return api.GetRemediationOperation404JSONResponse{Code: failure.code, Message: failure.message}, nil
	case http.StatusServiceUnavailable:
		return api.GetRemediationOperation503JSONResponse{ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse{
			Code: failure.code, Message: failure.message,
		}}, nil
	default:
		return nil, err
	}
}

func approveRemediationOperationFailure(err error) (api.ApproveRemediationOperationResponseObject, error) {
	failure, ok := classifyRemediationFailure(err)
	if !ok {
		return nil, err
	}
	switch failure.status {
	case http.StatusNotFound:
		return api.ApproveRemediationOperation404JSONResponse{Code: failure.code, Message: failure.message}, nil
	case http.StatusBadRequest, http.StatusConflict:
		return api.ApproveRemediationOperation409JSONResponse{Code: "conflict", Message: failure.message}, nil
	case http.StatusServiceUnavailable:
		return api.ApproveRemediationOperation503JSONResponse{ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse{
			Code: failure.code, Message: failure.message,
		}}, nil
	default:
		return nil, err
	}
}

func startRemediationOperationFailure(err error) (api.StartRemediationOperationResponseObject, error) {
	failure, ok := classifyRemediationFailure(err)
	if !ok {
		return nil, err
	}
	switch failure.status {
	case http.StatusNotFound:
		return api.StartRemediationOperation404JSONResponse{Code: failure.code, Message: failure.message}, nil
	case http.StatusBadRequest, http.StatusConflict:
		return api.StartRemediationOperation409JSONResponse{Code: "conflict", Message: failure.message}, nil
	case http.StatusServiceUnavailable:
		return api.StartRemediationOperation503JSONResponse{ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse{
			Code: failure.code, Message: failure.message,
		}}, nil
	default:
		return nil, err
	}
}

type breakGlassHTTPFailure struct {
	status  int
	code    string
	message string
}

func classifyBreakGlassFailure(err error) (breakGlassHTTPFailure, bool) {
	var failure *breakglass.Failure
	if !errors.As(err, &failure) {
		return breakGlassHTTPFailure{}, false
	}
	switch failure.Code {
	case breakglass.FailureInvalid:
		return breakGlassHTTPFailure{
			status: http.StatusBadRequest, code: "invalid_request", message: failure.Message,
		}, true
	case breakglass.FailureUnauthorized:
		return breakGlassHTTPFailure{
			status: http.StatusUnauthorized, code: "unauthorized",
			message: platformAuthenticationFailureMessage,
		}, true
	case breakglass.FailureForbidden:
		return breakGlassHTTPFailure{
			status: http.StatusForbidden, code: "forbidden", message: failure.Message,
		}, true
	case breakglass.FailureNotFound:
		return breakGlassHTTPFailure{
			status: http.StatusNotFound, code: "not_found", message: failure.Message,
		}, true
	case breakglass.FailureConflict:
		return breakGlassHTTPFailure{
			status: http.StatusConflict, code: "conflict", message: failure.Message,
		}, true
	case breakglass.FailureUnavailable:
		return breakGlassHTTPFailure{
			status: http.StatusServiceUnavailable, code: "service_unavailable",
			message: breakGlassServiceUnavailableMessage,
		}, true
	default:
		return breakGlassHTTPFailure{}, false
	}
}

func createBreakGlassRequestFailure(
	err error,
) (api.CreateBreakGlassRequestResponseObject, error) {
	failure, ok := classifyBreakGlassFailure(err)
	if !ok {
		return nil, err
	}
	switch failure.status {
	case http.StatusBadRequest:
		return api.CreateBreakGlassRequest400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	case http.StatusUnauthorized:
		return api.CreateBreakGlassRequest401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	case http.StatusNotFound:
		return api.CreateBreakGlassRequest404JSONResponse{
			Code: failure.code, Message: failure.message,
		}, nil
	case http.StatusConflict:
		return api.CreateBreakGlassRequest409JSONResponse{
			Code: failure.code, Message: failure.message,
		}, nil
	case http.StatusServiceUnavailable:
		return api.CreateBreakGlassRequest503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	default:
		return nil, err
	}
}

func getBreakGlassRequestFailure(
	err error,
) (api.GetBreakGlassRequestResponseObject, error) {
	failure, ok := classifyBreakGlassFailure(err)
	if !ok {
		return nil, err
	}
	switch failure.status {
	case http.StatusUnauthorized:
		return api.GetBreakGlassRequest401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	case http.StatusNotFound:
		return api.GetBreakGlassRequest404JSONResponse{
			Code: failure.code, Message: failure.message,
		}, nil
	case http.StatusServiceUnavailable:
		return api.GetBreakGlassRequest503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	default:
		return nil, err
	}
}

func approveBreakGlassRequestFailure(
	err error,
) (api.ApproveBreakGlassRequestResponseObject, error) {
	failure, ok := classifyBreakGlassFailure(err)
	if !ok {
		return nil, err
	}
	switch failure.status {
	case http.StatusUnauthorized:
		return api.ApproveBreakGlassRequest401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	case http.StatusForbidden:
		return api.ApproveBreakGlassRequest403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	case http.StatusNotFound:
		return api.ApproveBreakGlassRequest404JSONResponse{
			Code: failure.code, Message: failure.message,
		}, nil
	case http.StatusConflict:
		return api.ApproveBreakGlassRequest409JSONResponse{
			Code: failure.code, Message: failure.message,
		}, nil
	case http.StatusServiceUnavailable:
		return api.ApproveBreakGlassRequest503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	default:
		return nil, err
	}
}

func revokeBreakGlassGrantFailure(
	err error,
) (api.RevokeBreakGlassGrantResponseObject, error) {
	failure, ok := classifyBreakGlassFailure(err)
	if !ok {
		return nil, err
	}
	switch failure.status {
	case http.StatusUnauthorized:
		return api.RevokeBreakGlassGrant401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	case http.StatusForbidden:
		return api.RevokeBreakGlassGrant403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	case http.StatusNotFound:
		return api.RevokeBreakGlassGrant404JSONResponse{
			Code: failure.code, Message: failure.message,
		}, nil
	case http.StatusServiceUnavailable:
		return api.RevokeBreakGlassGrant503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	default:
		return nil, err
	}
}

func getBreakGlassRequestContentFailure(
	err error,
) (api.GetBreakGlassRequestContentResponseObject, error) {
	failure, ok := classifyBreakGlassFailure(err)
	if !ok {
		return nil, err
	}
	switch failure.status {
	case http.StatusUnauthorized:
		return api.GetBreakGlassRequestContent401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	case http.StatusForbidden:
		return api.GetBreakGlassRequestContent403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	case http.StatusNotFound:
		return api.GetBreakGlassRequestContent404JSONResponse{
			Code: failure.code, Message: failure.message,
		}, nil
	case http.StatusServiceUnavailable:
		return api.GetBreakGlassRequestContent503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	default:
		return nil, err
	}
}

func getBreakGlassArtifactsFailure(
	err error,
) (api.GetBreakGlassArtifactsResponseObject, error) {
	failure, ok := classifyBreakGlassFailure(err)
	if !ok {
		return nil, err
	}
	switch failure.status {
	case http.StatusUnauthorized:
		return api.GetBreakGlassArtifacts401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	case http.StatusForbidden:
		return api.GetBreakGlassArtifacts403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	case http.StatusNotFound:
		return api.GetBreakGlassArtifacts404JSONResponse{
			Code: failure.code, Message: failure.message,
		}, nil
	case http.StatusServiceUnavailable:
		return api.GetBreakGlassArtifacts503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse{
				Code: failure.code, Message: failure.message,
			},
		}, nil
	default:
		return nil, err
	}
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

func createServicePrincipalFailure(
	err error,
) (api.CreateServicePrincipalResponseObject, error) {
	failure, response, ok := identityAdministrationFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case identity.AdministrationFailureUnauthorized:
		return api.CreateServicePrincipal401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case identity.AdministrationFailureForbidden:
		return api.CreateServicePrincipal403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case identity.AdministrationFailureInvalidRequest:
		return api.CreateServicePrincipal400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case identity.AdministrationFailureConflict:
		return api.CreateServicePrincipal409JSONResponse(response), nil
	default:
		return nil, err
	}
}

func createHumanMemberFailure(
	err error,
) (api.CreateHumanMemberResponseObject, error) {
	failure, response, ok := identityAdministrationFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case identity.AdministrationFailureUnauthorized:
		return api.CreateHumanMember401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case identity.AdministrationFailureForbidden:
		return api.CreateHumanMember403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case identity.AdministrationFailureInvalidRequest:
		return api.CreateHumanMember400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case identity.AdministrationFailureConflict:
		return api.CreateHumanMember409JSONResponse(response), nil
	default:
		return nil, err
	}
}

func listHumanMembersFailure(
	err error,
) (api.ListHumanMembersResponseObject, error) {
	failure, response, ok := identityAdministrationFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case identity.AdministrationFailureUnauthorized:
		return api.ListHumanMembers401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case identity.AdministrationFailureForbidden:
		return api.ListHumanMembers403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case identity.AdministrationFailureInvalidRequest:
		return api.ListHumanMembers400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	default:
		return nil, err
	}
}

func disableHumanMemberFailure(
	err error,
) (api.DisableHumanMemberResponseObject, error) {
	failure, response, ok := identityAdministrationFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case identity.AdministrationFailureUnauthorized:
		return api.DisableHumanMember401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case identity.AdministrationFailureForbidden:
		return api.DisableHumanMember403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case identity.AdministrationFailureInvalidRequest:
		return api.DisableHumanMember400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case identity.AdministrationFailureNotFound:
		return api.DisableHumanMember404JSONResponse(response), nil
	case identity.AdministrationFailureConflict:
		return api.DisableHumanMember409JSONResponse(response), nil
	default:
		return nil, err
	}
}

func assignOrganizationRoleFailure(
	err error,
) (api.AssignOrganizationRoleResponseObject, error) {
	failure, response, ok := identityAdministrationFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case identity.AdministrationFailureUnauthorized:
		return api.AssignOrganizationRole401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case identity.AdministrationFailureForbidden:
		return api.AssignOrganizationRole403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case identity.AdministrationFailureInvalidRequest:
		return api.AssignOrganizationRole400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case identity.AdministrationFailureNotFound:
		return api.AssignOrganizationRole404JSONResponse(response), nil
	default:
		return nil, err
	}
}

func revokeOrganizationRoleFailure(
	err error,
) (api.RevokeOrganizationRoleResponseObject, error) {
	failure, response, ok := identityAdministrationFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case identity.AdministrationFailureUnauthorized:
		return api.RevokeOrganizationRole401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case identity.AdministrationFailureForbidden:
		return api.RevokeOrganizationRole403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case identity.AdministrationFailureInvalidRequest:
		return api.RevokeOrganizationRole400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case identity.AdministrationFailureNotFound:
		return api.RevokeOrganizationRole404JSONResponse(response), nil
	case identity.AdministrationFailureConflict:
		return api.RevokeOrganizationRole409JSONResponse(response), nil
	default:
		return nil, err
	}
}

func listProjectMembersFailure(
	err error,
) (api.ListProjectMembersResponseObject, error) {
	failure, response, ok := identityAdministrationFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case identity.AdministrationFailureUnauthorized:
		return api.ListProjectMembers401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case identity.AdministrationFailureForbidden:
		return api.ListProjectMembers403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case identity.AdministrationFailureInvalidRequest:
		return api.ListProjectMembers400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case identity.AdministrationFailureNotFound:
		return api.ListProjectMembers404JSONResponse(response), nil
	default:
		return nil, err
	}
}

func assignProjectRoleFailure(
	err error,
) (api.AssignProjectRoleResponseObject, error) {
	failure, response, ok := identityAdministrationFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case identity.AdministrationFailureUnauthorized:
		return api.AssignProjectRole401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case identity.AdministrationFailureForbidden:
		return api.AssignProjectRole403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case identity.AdministrationFailureInvalidRequest:
		return api.AssignProjectRole400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case identity.AdministrationFailureNotFound:
		return api.AssignProjectRole404JSONResponse(response), nil
	default:
		return nil, err
	}
}

func revokeProjectRoleFailure(
	err error,
) (api.RevokeProjectRoleResponseObject, error) {
	failure, response, ok := identityAdministrationFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case identity.AdministrationFailureUnauthorized:
		return api.RevokeProjectRole401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case identity.AdministrationFailureForbidden:
		return api.RevokeProjectRole403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case identity.AdministrationFailureInvalidRequest:
		return api.RevokeProjectRole400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case identity.AdministrationFailureNotFound:
		return api.RevokeProjectRole404JSONResponse(response), nil
	default:
		return nil, err
	}
}

func listServicePrincipalsFailure(
	err error,
) (api.ListServicePrincipalsResponseObject, error) {
	failure, response, ok := identityAdministrationFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case identity.AdministrationFailureUnauthorized:
		return api.ListServicePrincipals401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case identity.AdministrationFailureForbidden:
		return api.ListServicePrincipals403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case identity.AdministrationFailureInvalidRequest:
		return api.ListServicePrincipals400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	default:
		return nil, err
	}
}

func disableServicePrincipalFailure(
	err error,
) (api.DisableServicePrincipalResponseObject, error) {
	failure, response, ok := identityAdministrationFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case identity.AdministrationFailureUnauthorized:
		return api.DisableServicePrincipal401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case identity.AdministrationFailureForbidden:
		return api.DisableServicePrincipal403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case identity.AdministrationFailureInvalidRequest:
		return api.DisableServicePrincipal400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case identity.AdministrationFailureNotFound:
		return api.DisableServicePrincipal404JSONResponse(response), nil
	default:
		return nil, err
	}
}

func issueServiceCredentialFailure(
	err error,
) (api.IssueServiceCredentialResponseObject, error) {
	failure, response, ok := identityAdministrationFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case identity.AdministrationFailureUnauthorized:
		return api.IssueServiceCredential401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case identity.AdministrationFailureForbidden:
		return api.IssueServiceCredential403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case identity.AdministrationFailureInvalidRequest:
		return api.IssueServiceCredential400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case identity.AdministrationFailureNotFound:
		return api.IssueServiceCredential404JSONResponse(response), nil
	case identity.AdministrationFailureConflict:
		return api.IssueServiceCredential409JSONResponse(response), nil
	default:
		return nil, err
	}
}

func listServiceCredentialsFailure(
	err error,
) (api.ListServiceCredentialsResponseObject, error) {
	failure, response, ok := identityAdministrationFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case identity.AdministrationFailureUnauthorized:
		return api.ListServiceCredentials401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case identity.AdministrationFailureForbidden:
		return api.ListServiceCredentials403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case identity.AdministrationFailureInvalidRequest:
		return api.ListServiceCredentials400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case identity.AdministrationFailureNotFound:
		return api.ListServiceCredentials404JSONResponse(response), nil
	default:
		return nil, err
	}
}

func revokeServiceCredentialFailure(
	err error,
) (api.RevokeServiceCredentialResponseObject, error) {
	failure, response, ok := identityAdministrationFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case identity.AdministrationFailureUnauthorized:
		return api.RevokeServiceCredential401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case identity.AdministrationFailureForbidden:
		return api.RevokeServiceCredential403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case identity.AdministrationFailureInvalidRequest:
		return api.RevokeServiceCredential400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case identity.AdministrationFailureNotFound:
		return api.RevokeServiceCredential404JSONResponse(response), nil
	default:
		return nil, err
	}
}

func identityAdministrationFailureResponse(
	err error,
) (*identity.AdministrationFailure, api.Error, bool) {
	var failure *identity.AdministrationFailure
	if !errors.As(err, &failure) {
		return nil, api.Error{}, false
	}
	return failure, api.Error{Code: string(failure.Code), Message: failure.Message}, true
}

func getProjectRetentionPolicyFailure(
	err error,
) (api.GetProjectRetentionPolicyResponseObject, error) {
	failure, response, ok := retentionFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case retention.FailureUnauthorized:
		return api.GetProjectRetentionPolicy401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case retention.FailureForbidden:
		return api.GetProjectRetentionPolicy403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case retention.FailureNotFound:
		return api.GetProjectRetentionPolicy404JSONResponse(response), nil
	default:
		return nil, err
	}
}

func setProjectRetentionPolicyFailure(
	err error,
) (api.SetProjectRetentionPolicyResponseObject, error) {
	failure, response, ok := retentionFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case retention.FailureUnauthorized:
		return api.SetProjectRetentionPolicy401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case retention.FailureForbidden:
		return api.SetProjectRetentionPolicy403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case retention.FailureInvalid:
		return api.SetProjectRetentionPolicy400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case retention.FailureNotFound:
		return api.SetProjectRetentionPolicy404JSONResponse(response), nil
	default:
		return nil, err
	}
}

func retentionFailureResponse(err error) (*retention.Failure, api.Error, bool) {
	var failure *retention.Failure
	if !errors.As(err, &failure) {
		return nil, api.Error{}, false
	}
	return failure, api.Error{Code: string(failure.Code), Message: failure.Message}, true
}

func authorizeDebugDumpFailure(
	err error,
) (api.AuthorizeDebugDumpResponseObject, error) {
	var failure *debugdump.Failure
	if !errors.As(err, &failure) {
		return nil, err
	}
	response := api.Error{Code: string(failure.Code), Message: failure.Message}
	switch failure.Code {
	case debugdump.FailureUnauthorized:
		return api.AuthorizeDebugDump401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case debugdump.FailureForbidden:
		return api.AuthorizeDebugDump403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case debugdump.FailureInvalid:
		return api.AuthorizeDebugDump400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case debugdump.FailureNotFound:
		return api.AuthorizeDebugDump404JSONResponse(response), nil
	case debugdump.FailureConflict:
		return api.AuthorizeDebugDump409JSONResponse(response), nil
	case debugdump.FailureUnavailable:
		return api.AuthorizeDebugDump503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse(response),
		}, nil
	default:
		return nil, err
	}
}

func getDebugDumpAuthorizationFailure(
	err error,
) (api.GetDebugDumpAuthorizationResponseObject, error) {
	var failure *debugdump.Failure
	if !errors.As(err, &failure) {
		return nil, err
	}
	response := api.Error{Code: string(failure.Code), Message: failure.Message}
	switch failure.Code {
	case debugdump.FailureUnauthorized:
		return api.GetDebugDumpAuthorization401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case debugdump.FailureForbidden:
		return api.GetDebugDumpAuthorization403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case debugdump.FailureNotFound, debugdump.FailureInvalid:
		return api.GetDebugDumpAuthorization404JSONResponse(response), nil
	case debugdump.FailureUnavailable:
		return api.GetDebugDumpAuthorization503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse(response),
		}, nil
	default:
		return nil, err
	}
}

func revokeDebugDumpAuthorizationFailure(
	err error,
) (api.RevokeDebugDumpAuthorizationResponseObject, error) {
	var failure *debugdump.Failure
	if !errors.As(err, &failure) {
		return nil, err
	}
	response := api.Error{Code: string(failure.Code), Message: failure.Message}
	switch failure.Code {
	case debugdump.FailureUnauthorized:
		return api.RevokeDebugDumpAuthorization401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case debugdump.FailureForbidden:
		return api.RevokeDebugDumpAuthorization403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case debugdump.FailureNotFound, debugdump.FailureInvalid:
		return api.RevokeDebugDumpAuthorization404JSONResponse(response), nil
	case debugdump.FailureConflict:
		return api.RevokeDebugDumpAuthorization409JSONResponse(response), nil
	case debugdump.FailureUnavailable:
		return api.RevokeDebugDumpAuthorization503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse(response),
		}, nil
	default:
		return nil, err
	}
}

func listDebugDumpsFailure(err error) (api.ListDebugDumpsResponseObject, error) {
	var failure *debugdump.Failure
	if !errors.As(err, &failure) {
		return nil, err
	}
	response := api.Error{Code: string(failure.Code), Message: failure.Message}
	switch failure.Code {
	case debugdump.FailureUnauthorized:
		return api.ListDebugDumps401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case debugdump.FailureForbidden:
		return api.ListDebugDumps403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case debugdump.FailureInvalid:
		return api.ListDebugDumps400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case debugdump.FailureNotFound:
		return api.ListDebugDumps404JSONResponse(response), nil
	case debugdump.FailureUnavailable:
		return api.ListDebugDumps503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse(response),
		}, nil
	default:
		return nil, err
	}
}

func readDebugDumpFailure(err error) (api.ReadDebugDumpResponseObject, error) {
	var failure *debugdump.Failure
	if !errors.As(err, &failure) {
		return nil, err
	}
	response := api.Error{Code: string(failure.Code), Message: failure.Message}
	switch failure.Code {
	case debugdump.FailureUnauthorized:
		return api.ReadDebugDump401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case debugdump.FailureForbidden:
		return api.ReadDebugDump403JSONResponse(response), nil
	case debugdump.FailureInvalid:
		return api.ReadDebugDump400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case debugdump.FailureNotFound:
		return api.ReadDebugDump404JSONResponse(response), nil
	case debugdump.FailureUnavailable:
		return api.ReadDebugDump503JSONResponse{
			ServiceUnavailableJSONResponse: api.ServiceUnavailableJSONResponse(response),
		}, nil
	default:
		return nil, err
	}
}

func acceptContentDeletionRequestFailure(
	err error,
) (api.AcceptContentDeletionRequestResponseObject, error) {
	failure, response, ok := retentionFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case retention.FailureUnauthorized:
		return api.AcceptContentDeletionRequest401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case retention.FailureForbidden:
		return api.AcceptContentDeletionRequest403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case retention.FailureInvalid:
		return api.AcceptContentDeletionRequest400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case retention.FailureNotFound:
		return api.AcceptContentDeletionRequest404JSONResponse(response), nil
	case retention.FailureConflict:
		return api.AcceptContentDeletionRequest409JSONResponse(response), nil
	default:
		return nil, err
	}
}

func getContentDeletionRequestFailure(
	err error,
) (api.GetContentDeletionRequestResponseObject, error) {
	failure, response, ok := retentionFailureResponse(err)
	if !ok {
		return nil, err
	}
	switch failure.Code {
	case retention.FailureUnauthorized:
		return api.GetContentDeletionRequest401JSONResponse{
			UnauthorizedJSONResponse: api.UnauthorizedJSONResponse(response),
		}, nil
	case retention.FailureForbidden:
		return api.GetContentDeletionRequest403JSONResponse{
			ForbiddenJSONResponse: api.ForbiddenJSONResponse(response),
		}, nil
	case retention.FailureInvalid:
		return api.GetContentDeletionRequest400JSONResponse{
			BadRequestJSONResponse: api.BadRequestJSONResponse(response),
		}, nil
	case retention.FailureNotFound:
		return api.GetContentDeletionRequest404JSONResponse(response), nil
	default:
		return nil, err
	}
}

func organizationReportingFailureResponse(
	err error,
) (*organizationreporting.Failure, api.Error, bool) {
	var failure *organizationreporting.Failure
	if !errors.As(err, &failure) {
		return nil, api.Error{}, false
	}
	return failure, api.Error{Code: string(failure.Code), Message: failure.Message}, true
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

func toAPIOrganizationCreditSummary(
	summary organizationreporting.CreditSummary,
) api.OrganizationCreditSummary {
	return api.OrganizationCreditSummary{
		AvailableMinor:           summary.AvailableMinor,
		ContractCreditLimitMinor: summary.ContractCreditLimitMinor,
		Currency:                 summary.Currency,
		LedgerVersion:            summary.Version,
		OrganizationId:           summary.OrganizationID,
		ReservedMinor:            summary.ReservedMinor,
		UnsettledPostedMinor:     summary.UnsettledPostedMinor,
		UpdatedAt:                summary.UpdatedAt,
	}
}

func toAPIOrganizationCharge(charge organizationreporting.Charge) api.OrganizationCharge {
	return api.OrganizationCharge{
		AmountMinor:      charge.AmountMinor,
		ChargeId:         charge.ChargeID,
		Currency:         charge.Currency,
		ExportedAt:       charge.ExportedAt,
		InvoiceReference: charge.InvoiceReference,
		JobId:            charge.JobID,
		LineReference:    charge.LineReference,
		PostedAt:         charge.PostedAt,
		ProjectId:        charge.ProjectID,
		Reason:           api.ChargeReason(charge.Reason),
	}
}

func toAPISettlementContact(
	contact organizationreporting.SettlementContact,
) api.SettlementContact {
	return api.SettlementContact{
		ContactId:             contact.ID,
		CreatedAt:             contact.CreatedAt,
		CreatedByPrincipalId:  contact.CreatedByPrincipalID,
		DisabledAt:            contact.DisabledAt,
		DisabledByPrincipalId: contact.DisabledByPrincipalID,
		DisplayName:           contact.DisplayName,
		Email:                 contact.Email,
		OrganizationId:        contact.OrganizationID,
	}
}

func toAPIUsageAggregate(usage organizationreporting.UsageAggregate) api.UsageAggregate {
	return api.UsageAggregate{
		AssignedJobs:            usage.AssignedJobs,
		CanceledJobs:            usage.CanceledJobs,
		CancelingJobs:           usage.CancelingJobs,
		FailedJobs:              usage.FailedJobs,
		FinalizingJobs:          usage.FinalizingJobs,
		PostedChargeAmountMinor: usage.PostedChargeAmountMinor,
		QueuedJobs:              usage.QueuedJobs,
		QuotedAmountMinor:       usage.QuotedAmountMinor,
		RetryWaitJobs:           usage.RetryWaitJobs,
		RunningJobs:             usage.RunningJobs,
		SucceededJobs:           usage.SucceededJobs,
		TotalJobs:               usage.TotalJobs,
	}
}

func toAPIOrganizationUsage(usage organizationreporting.UsageSummary) api.OrganizationUsage {
	projects := make([]api.ProjectUsage, len(usage.Projects))
	for index, project := range usage.Projects {
		projects[index] = api.ProjectUsage{
			ProjectId: project.ProjectID,
			Usage:     toAPIUsageAggregate(project.UsageAggregate),
		}
	}
	return api.OrganizationUsage{
		Currency:       usage.Currency,
		From:           usage.From,
		OrganizationId: usage.OrganizationID,
		Projects:       projects,
		To:             usage.To,
		Total:          toAPIUsageAggregate(usage.Total),
	}
}

func toAPIOrganizationAuditEvent(
	event organizationreporting.AuditEvent,
) api.OrganizationAuditEvent {
	var scope *api.BreakGlassScope
	if event.Scope != nil {
		value := api.BreakGlassScope(*event.Scope)
		scope = &value
	}
	var actorPrincipalID, actorSessionID *uuid.UUID
	if event.ActorPrincipalID != uuid.Nil {
		value := event.ActorPrincipalID
		actorPrincipalID = &value
	}
	if event.ActorSessionID != uuid.Nil {
		value := event.ActorSessionID
		actorSessionID = &value
	}
	return api.OrganizationAuditEvent{
		Action:           api.OrganizationAuditEventAction(event.Action),
		ActorPrincipalId: actorPrincipalID,
		ActorSessionId:   actorSessionID,
		CreatedAt:        event.CreatedAt,
		EventId:          event.EventID,
		OutcomeCode:      event.OutcomeCode,
		ProjectId:        event.ProjectID,
		Scope:            scope,
		Source:           api.OrganizationAuditEventSource(event.Source),
		TargetId:         event.TargetID,
		TargetKind:       api.OrganizationAuditEventTargetKind(event.TargetKind),
	}
}

func toAPIRemediationOperation(operation remediation.Operation) api.RemediationOperation {
	result := api.RemediationOperation{
		OperationId: uuid.UUID(operation.ID), WorkerId: uuid.UUID(operation.WorkerID),
		WorkerEpoch: operation.WorkerEpoch, NodeIdentity: operation.NodeIdentity,
		GpuUuid: operation.DeviceIdentity, FailureClass: operation.FailureClass,
		EvidenceSha256:        hex.EncodeToString(operation.EvidenceDigest),
		CertificationRevision: operation.CertificationRevision,
		ActionLevel:           api.RemediationActionLevel(operation.ActionLevel),
		State:                 api.RemediationOperationState(operation.State),
		RequestedBy:           operation.RequestedBy, RequestedAt: operation.RequestedAt,
		DeadlineAt: operation.DeadlineAt,
		StartedAt:  operation.StartedAt, FinishedAt: operation.FinishedAt,
		ApprovedAt: operation.ApprovedAt,
	}
	if operation.ResultCode != "" {
		result.ResultCode = &operation.ResultCode
	}
	if operation.ResultDetail != "" {
		result.ResultDetail = &operation.ResultDetail
	}
	if len(operation.PostcheckDigest) > 0 {
		value := hex.EncodeToString(operation.PostcheckDigest)
		result.PostcheckSha256 = &value
	}
	if operation.FirstApprover != "" {
		result.FirstApprover = &operation.FirstApprover
	}
	if operation.SecondApprover != "" {
		result.SecondApprover = &operation.SecondApprover
	}
	return result
}

func toAPIBreakGlassRequest(request breakglass.Request) api.BreakGlassRequest {
	scopes := make([]api.BreakGlassScope, len(request.Scopes))
	for index, scope := range request.Scopes {
		scopes[index] = api.BreakGlassScope(scope)
	}
	return api.BreakGlassRequest{
		ApprovalDeadlineAt:       request.ApprovalDeadlineAt,
		ApprovedAt:               request.ApprovedAt,
		ApproverOperatorId:       request.ApproverOperatorID,
		ExpiresAt:                request.ExpiresAt,
		GrantId:                  request.GrantID,
		JobId:                    request.JobID,
		OrganizationId:           request.OrganizationID,
		ProjectId:                request.ProjectID,
		ReasonCode:               api.BreakGlassReasonCode(request.ReasonCode),
		RequestId:                request.ID,
		RequestedAt:              request.RequestedAt,
		RequestedDurationSeconds: request.RequestedDurationSeconds,
		RequesterOperatorId:      request.RequesterOperatorID,
		RevokedAt:                request.RevokedAt,
		RevokedByOperatorId:      request.RevokedByOperatorID,
		Scopes:                   scopes,
		State:                    api.BreakGlassState(request.State),
		TicketReference:          request.TicketReference,
	}
}

func toAPIBreakGlassArtifactSet(artifactSet breakglass.ArtifactSet) api.BreakGlassArtifactSet {
	artifacts := make([]api.BreakGlassArtifact, len(artifactSet.Artifacts))
	for index, artifact := range artifactSet.Artifacts {
		artifacts[index] = api.BreakGlassArtifact{
			ArtifactId:           artifact.ID,
			ContentType:          artifact.ContentType,
			DownloadUrl:          artifact.DownloadURL,
			DownloadUrlExpiresAt: artifact.DownloadURLExpiresAt,
			Kind:                 api.BreakGlassArtifactKind(artifact.Kind),
			Ordinal:              artifact.Ordinal,
			Sha256:               hex.EncodeToString(artifact.SHA256[:]),
			SizeBytes:            artifact.SizeBytes,
		}
	}
	return api.BreakGlassArtifactSet{
		ArtifactSetId:      artifactSet.ID,
		Artifacts:          artifacts,
		CommittedAt:        artifactSet.CommittedAt,
		JobId:              artifactSet.JobID,
		OrganizationId:     artifactSet.OrganizationID,
		ProjectId:          artifactSet.ProjectID,
		RetentionExpiresAt: artifactSet.RetentionExpiresAt,
	}
}

func toAPIProjectRetentionPolicy(policy retention.Policy) api.ProjectRetentionPolicy {
	return api.ProjectRetentionPolicy{
		ProjectId:                       policy.ProjectID,
		PolicyRevisionId:                policy.PolicyRevisionID,
		StableId:                        policy.StableID,
		ArtifactRetentionDays:           api.ProjectRetentionPolicyArtifactRetentionDays(policy.ArtifactRetentionDays),
		RequestContentRetentionDays:     policy.RequestContentRetentionDays,
		IncompleteContentRetentionHours: policy.IncompleteContentRetentionHours,
		ScratchRetentionHours:           policy.ScratchRetentionHours,
		DebugRetentionHours:             policy.DebugRetentionHours,
		MetadataRetentionDays:           policy.MetadataRetentionDays,
		FinancialRetentionDays:          policy.FinancialRetentionDays,
		SelectedAt:                      policy.SelectedAt,
	}
}

func toAPIDebugDumpAuthorization(
	authorization debugdump.Authorization,
) api.DebugDumpAuthorization {
	return api.DebugDumpAuthorization{
		AuthorizationId: authorization.ID,
		OrganizationId:  authorization.OrganizationID,
		ProjectId:       authorization.ProjectID,
		JobId:           authorization.JobID,
		Purpose:         api.DebugDumpPurpose(authorization.Purpose),
		AuthorizedAt:    authorization.AuthorizedAt,
		ExpiresAt:       authorization.ExpiresAt,
		RevokedAt:       authorization.RevokedAt,
	}
}

func toAPIDebugDump(dump debugdump.Dump) api.DebugDump {
	return api.DebugDump{
		AttemptId:       dump.AttemptID,
		AuthorizationId: dump.AuthorizationID,
		ContentType:     api.DebugDumpContentType(dump.ContentType),
		CreatedAt:       dump.CreatedAt,
		DebugDumpId:     dump.ID,
		DeletedAt:       dump.DeletedAt,
		ExpiresAt:       dump.ExpiresAt,
		Sha256:          hex.EncodeToString(dump.SHA256[:]),
		SizeBytes:       dump.SizeBytes,
		State:           api.DebugDumpState(dump.State),
		UploadedAt:      dump.UploadedAt,
	}
}

func toAPIDebugDumpDownload(download debugdump.Download) api.DebugDumpDownload {
	return api.DebugDumpDownload{
		AuthorizationId:      download.AuthorizationID,
		ContentType:          api.DebugDumpDownloadContentType(download.ContentType),
		DebugDumpId:          download.ID,
		DownloadUrl:          download.DownloadURL,
		DownloadUrlExpiresAt: download.DownloadURLExpiresAt,
		ExpiresAt:            download.ExpiresAt,
		Sha256:               hex.EncodeToString(download.SHA256[:]),
		SizeBytes:            download.SizeBytes,
	}
}

func toAPIContentDeletionRequest(
	deletion retention.DeletionRequest,
) api.ContentDeletionRequest {
	return api.ContentDeletionRequest{
		CompletedAt: deletion.CompletedAt,
		DeadlineAt:  deletion.DeadlineAt,
		JobId:       deletion.JobID,
		Overdue:     deletion.Overdue,
		ProjectId:   deletion.ProjectID,
		RequestId:   deletion.RequestID,
		RequestedAt: deletion.RequestedAt,
		State:       api.ContentDeletionRequestState(deletion.State),
	}
}

func toAPIContentDeletionRequestStatus(
	deletion retention.DeletionRequest,
) api.ContentDeletionRequestStatus {
	return api.ContentDeletionRequestStatus{
		CompletedAt:          deletion.CompletedAt,
		CompletedTargetCount: deletion.CompletedTargetCount,
		DeadlineAt:           deletion.DeadlineAt,
		JobId:                deletion.JobID,
		LastErrorCode:        deletion.LastErrorCode,
		LastErrorMessage:     deletion.LastErrorMessage,
		Overdue:              deletion.Overdue,
		ProjectId:            deletion.ProjectID,
		RequestId:            deletion.RequestID,
		RequestedAt:          deletion.RequestedAt,
		RetryingTargetCount:  deletion.RetryingTargetCount,
		State:                api.ContentDeletionRequestStatusState(deletion.State),
		TargetCount:          deletion.TargetCount,
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

func toAPIServicePrincipal(principal identity.ServicePrincipal) api.ServicePrincipal {
	return api.ServicePrincipal{
		CreatedAt:          principal.CreatedAt,
		DisabledAt:         principal.DisabledAt,
		DisplayName:        principal.DisplayName,
		ProjectId:          principal.ProjectID,
		ServicePrincipalId: principal.ID,
	}
}

func toAPIHumanMember(member identity.HumanMember) api.HumanMember {
	return api.HumanMember{
		CreatedAt:      member.CreatedAt,
		DisabledAt:     member.DisabledAt,
		DisplayName:    member.DisplayName,
		OidcIssuer:     member.OIDCIssuer,
		OidcSubject:    member.OIDCSubject,
		OrganizationId: member.OrganizationID,
		PrincipalId:    member.ID,
	}
}

func toAPIOrganizationMember(member identity.OrganizationMember) api.OrganizationMember {
	roles := make([]api.OrganizationRole, len(member.Roles))
	for index, role := range member.Roles {
		roles[index] = api.OrganizationRole(role)
	}
	return api.OrganizationMember{
		CreatedAt:         member.CreatedAt,
		DisabledAt:        member.DisabledAt,
		DisplayName:       member.DisplayName,
		OidcIssuer:        member.OIDCIssuer,
		OidcSubject:       member.OIDCSubject,
		OrganizationId:    member.OrganizationID,
		OrganizationRoles: roles,
		PrincipalId:       member.ID,
	}
}

func toAPIOrganizationRoleAssignment(
	assignment identity.OrganizationRoleAssignment,
) api.OrganizationRoleAssignment {
	var assignedByPrincipalID *uuid.UUID
	if assignment.AssignedByPrincipalID != uuid.Nil {
		value := assignment.AssignedByPrincipalID
		assignedByPrincipalID = &value
	}
	return api.OrganizationRoleAssignment{
		Active:                assignment.Active,
		AssignedAt:            assignment.AssignedAt,
		AssignedByPrincipalId: assignedByPrincipalID,
		OrganizationId:        assignment.OrganizationID,
		PrincipalId:           assignment.PrincipalID,
		Role:                  api.OrganizationRole(assignment.Role),
	}
}

func toAPIProjectMember(member identity.ProjectMember) api.ProjectMember {
	roles := make([]api.ProjectRole, len(member.Roles))
	for index, role := range member.Roles {
		roles[index] = api.ProjectRole(role)
	}
	return api.ProjectMember{
		DisabledAt:     member.DisabledAt,
		DisplayName:    member.DisplayName,
		OrganizationId: member.OrganizationID,
		PrincipalId:    member.ID,
		ProjectId:      member.ProjectID,
		ProjectRoles:   roles,
	}
}

func toAPIProjectRoleAssignment(
	assignment identity.ProjectRoleAssignment,
) api.ProjectRoleAssignment {
	var assignedByPrincipalID *uuid.UUID
	if assignment.AssignedByPrincipalID != uuid.Nil {
		value := assignment.AssignedByPrincipalID
		assignedByPrincipalID = &value
	}
	return api.ProjectRoleAssignment{
		Active:                assignment.Active,
		AssignedAt:            assignment.AssignedAt,
		AssignedByPrincipalId: assignedByPrincipalID,
		OrganizationId:        assignment.OrganizationID,
		PrincipalId:           assignment.PrincipalID,
		ProjectId:             assignment.ProjectID,
		Role:                  api.ProjectRole(assignment.Role),
	}
}

func toAPIServiceCredential(credential identity.Credential) api.ServiceCredential {
	scopes := make([]api.ServiceCredentialScope, len(credential.Scopes))
	for index, scope := range credential.Scopes {
		scopes[index] = api.ServiceCredentialScope(scope)
	}
	return api.ServiceCredential{
		CreatedAt:          credential.CreatedAt,
		CredentialId:       credential.ID,
		ExpiresAt:          credential.ExpiresAt,
		ProjectId:          credential.ProjectID,
		RevokedAt:          credential.RevokedAt,
		Scopes:             scopes,
		ServicePrincipalId: credential.ServicePrincipalID,
	}
}

func toAPIIssuedServiceCredential(issued identity.IssuedCredential) api.IssuedServiceCredential {
	credential := toAPIServiceCredential(issued.Credential)
	return api.IssuedServiceCredential{
		BearerCredential:   issued.BearerCredential,
		CreatedAt:          credential.CreatedAt,
		CredentialId:       credential.CredentialId,
		ExpiresAt:          credential.ExpiresAt,
		ProjectId:          credential.ProjectId,
		RevokedAt:          credential.RevokedAt,
		Scopes:             credential.Scopes,
		ServicePrincipalId: credential.ServicePrincipalId,
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
