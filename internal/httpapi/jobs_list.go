package httpapi

import (
	"context"
	"errors"

	api "github.com/vivym/vela/api/gen"
	"github.com/vivym/vela/internal/admission"
	"github.com/vivym/vela/internal/identity"
)

func (s *server) ListJobs(ctx context.Context, request api.ListJobsRequestObject) (api.ListJobsResponseObject, error) {
	principal, ok := principalFromContext(ctx)
	if !ok {
		return api.ListJobs401JSONResponse{UnauthorizedJSONResponse: api.UnauthorizedJSONResponse{Code: "unauthorized", Message: authenticationFailureMessage}}, nil
	}
	principal, authorized := principalForProjectRequest(principal, request.ProjectId)
	if !authorized || !principal.HasScope(identity.ScopeJobsRead) {
		return api.ListJobs403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse{Code: "forbidden", Message: "credential does not have jobs:read scope"}}, nil
	}
	options := admission.ListOptions{Limit: 50}
	if request.Params.Limit != nil {
		options.Limit = *request.Params.Limit
	}
	if request.Params.Active != nil {
		options.Active = *request.Params.Active
	}
	if request.Params.State != nil {
		options.State = admission.JobState(*request.Params.State)
	}
	if request.Params.Cursor != nil {
		options.Cursor = *request.Params.Cursor
	}
	list, err := s.admission.List(ctx, principal, request.ProjectId, options)
	if err != nil {
		var f *admission.Failure
		if errors.As(err, &f) {
			switch f.Code {
			case admission.FailureCodeInvalidRequest:
				return api.ListJobs400JSONResponse{BadRequestJSONResponse: api.BadRequestJSONResponse{Code: string(f.Code), Message: f.Message}}, nil
			case admission.FailureCodeForbidden:
				return api.ListJobs403JSONResponse{ForbiddenJSONResponse: api.ForbiddenJSONResponse{Code: string(f.Code), Message: f.Message}}, nil
			}
		}
		return nil, err
	}
	response := api.JobList{Jobs: make([]api.Job, len(list.Jobs))}
	for i, job := range list.Jobs {
		response.Jobs[i] = toAPIJob(job)
	}
	if list.NextCursor != "" {
		response.NextCursor = &list.NextCursor
	}
	return api.ListJobs200JSONResponse(response), nil
}
