package admission

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/identity"
	store "github.com/vivym/vela/internal/store/sqlc"
)

type ListOptions struct {
	Active bool
	State  JobState
	Limit  int
	Cursor string
}

type JobList struct {
	Jobs       []Job
	NextCursor string
}

// A cursor is a position, not an authorization token. Every page independently
// authenticates and applies the database request context and Project predicates.
type jobCursor struct {
	Version        int       `json:"v"`
	OrganizationID uuid.UUID `json:"organization_id"`
	ProjectID      uuid.UUID `json:"project_id"`
	Active         bool      `json:"active"`
	State          JobState  `json:"state"`
	CreatedAt      time.Time `json:"created_at"`
	ID             uuid.UUID `json:"id"`
}

func parseJobCursor(options ListOptions, organizationID, projectID uuid.UUID) (jobCursor, error) {
	invalid := func() (jobCursor, error) {
		return jobCursor{}, failure(FailureCodeInvalidRequest, "invalid Job list filters or cursor", 0)
	}
	if options.Limit < 1 || options.Limit > 100 {
		return invalid()
	}
	switch options.State {
	case "", JobStateQueued, JobStateAssigned, JobStateRunning, JobStateFinalizing, JobStateRetryWait, JobStateCanceling:
	case JobStateSucceeded, JobStateFailed, JobStateCanceled:
		if options.Active {
			return invalid()
		}
	default:
		return invalid()
	}
	expected := jobCursor{Version: 1, OrganizationID: organizationID, ProjectID: projectID, Active: options.Active, State: options.State}
	if options.Cursor == "" {
		return expected, nil
	}
	if len(options.Cursor) > 1024 {
		return invalid()
	}
	wire, err := base64.RawURLEncoding.DecodeString(options.Cursor)
	if err != nil {
		return invalid()
	}
	var cursor jobCursor
	d := json.NewDecoder(bytes.NewReader(wire))
	d.DisallowUnknownFields()
	if d.Decode(&cursor) != nil || !errors.Is(d.Decode(new(any)), io.EOF) {
		return invalid()
	}
	if cursor.Version != 1 || cursor.OrganizationID != organizationID || cursor.ProjectID != projectID || cursor.Active != options.Active || cursor.State != options.State || cursor.CreatedAt.IsZero() || cursor.CreatedAt.Year() < 1 || cursor.CreatedAt.Year() > 9999 || cursor.ID == uuid.Nil {
		return invalid()
	}
	return cursor, nil
}

func (s *Service) List(ctx context.Context, principal identity.Principal, projectID uuid.UUID, options ListOptions) (JobList, error) {
	if s == nil || s.pool == nil {
		return JobList{}, errors.New("admission service is not configured")
	}
	if principal.ProjectID != projectID {
		return JobList{}, failure(FailureCodeForbidden, "credential is not authorized for this Project", 0)
	}
	if options.Limit == 0 {
		options.Limit = 50
	}
	cursor, err := parseJobCursor(options, principal.OrganizationID, projectID)
	if err != nil {
		return JobList{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return JobList{}, fmt.Errorf("begin Job list: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	requestContext, err := establishRequestContext(ctx, queries, principal, projectID, identity.ScopeJobsRead)
	if err != nil {
		return JobList{}, err
	}
	if s.roleObserver != nil {
		s.roleObserver.ObserveRequestRole(ctx, veladb.RequestRoleObservation{Surface: veladb.RequestRoleSurfaceJobRead, DatabaseLogin: requestContext.DatabaseLogin, DatabaseRole: veladb.RoleRequest})
	}
	before := pgtype.Timestamptz{}
	if options.Cursor != "" {
		before = pgtype.Timestamptz{Time: cursor.CreatedAt, Valid: true}
	}
	rows, err := queries.ListJobs(ctx, store.ListJobsParams{OrganizationID: principal.OrganizationID, ProjectID: projectID, Active: options.Active, StateFilter: string(options.State), BeforeCreatedAt: before, BeforeID: cursor.ID, PageSize: int32(options.Limit + 1)})
	if err != nil {
		return JobList{}, fmt.Errorf("list Jobs: %w", err)
	}
	more := len(rows) > options.Limit
	if more {
		rows = rows[:options.Limit]
	}
	result := JobList{Jobs: make([]Job, 0, len(rows))}
	for _, row := range rows {
		job, err := jobFromGetRow(store.GetJobRow(row))
		if err != nil {
			return JobList{}, err
		}
		result.Jobs = append(result.Jobs, job)
	}
	if more {
		last := result.Jobs[len(result.Jobs)-1]
		cursor.CreatedAt = last.CreatedAt
		cursor.ID = last.ID
		wire, err := json.Marshal(cursor)
		if err != nil {
			return JobList{}, err
		}
		result.NextCursor = base64.RawURLEncoding.EncodeToString(wire)
	}
	if err := tx.Commit(ctx); err != nil {
		return JobList{}, fmt.Errorf("commit Job list: %w", err)
	}
	return result, nil
}
