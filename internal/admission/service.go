package admission

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/execution"
	"github.com/vivym/vela/internal/identity"
	store "github.com/vivym/vela/internal/store/sqlc"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultCapacityRetryAfter = 30

var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)

type Request struct {
	Model            string          `json:"model"`
	GenerationPreset string          `json:"generation_preset"`
	ServiceClass     string          `json:"service_class"`
	OutputSpec       string          `json:"output_spec"`
	GenerationCount  int32           `json:"generation_count"`
	Prompt           string          `json:"prompt"`
	ClientMetadata   json.RawMessage `json:"client_metadata,omitempty"`
}

type PricingSnapshot struct {
	RateCardRevisionID uuid.UUID
	RateLineID         uuid.UUID
	UnitAmountMinor    int64
	Quantity           int32
	QuotedAmountMinor  int64
	Currency           string
}

type JobState string

const (
	JobStateQueued     JobState = "QUEUED"
	JobStateAssigned   JobState = "ASSIGNED"
	JobStateRunning    JobState = "RUNNING"
	JobStateFinalizing JobState = "FINALIZING"
	JobStateRetryWait  JobState = "RETRY_WAIT"
	JobStateCanceling  JobState = "CANCELING"
	JobStateSucceeded  JobState = "SUCCEEDED"
	JobStateFailed     JobState = "FAILED"
	JobStateCanceled   JobState = "CANCELED"
)

type Job struct {
	ID                uuid.UUID
	ProjectID         uuid.UUID
	State             JobState
	Phase             *execution.Phase
	PhaseProgress     *float64
	AttemptsStarted   int32
	NextRetryAt       *time.Time
	EstimatedFinishAt *time.Time
	ProgressUpdatedAt *time.Time
	PricingSnapshot   PricingSnapshot
	JobExpiresAt      time.Time
	CreatedAt         time.Time
}

type FailureCode string

const (
	FailureCodeInvalidRequest       FailureCode = "invalid_request"
	FailureCodeInvalidSKU           FailureCode = "invalid_sku"
	FailureCodeCreditLimitExceeded  FailureCode = "credit_limit_exceeded"
	FailureCodeForbidden            FailureCode = "forbidden"
	FailureCodeOrganizationInactive FailureCode = "organization_inactive"
	FailureCodeIdempotencyConflict  FailureCode = "idempotency_conflict"
	FailureCodeProjectLimitExceeded FailureCode = "project_limit_exceeded"
	FailureCodeCapacityUnavailable  FailureCode = "capacity_unavailable"
	FailureCodeNotFound             FailureCode = "not_found"
)

type Failure struct {
	Code       FailureCode
	Message    string
	RetryAfter int
}

func (e *Failure) Error() string {
	return string(e.Code) + ": " + e.Message
}

type Service struct {
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

func (s *Service) Submit(
	ctx context.Context,
	principal identity.Principal,
	projectID uuid.UUID,
	idempotencyKey string,
	request Request,
) (Job, error) {
	if s == nil || s.pool == nil {
		return Job{}, errors.New("admission service is not configured")
	}
	if principal.ProjectID != projectID {
		return Job{}, failure(FailureCodeForbidden, "credential is not authorized for this Project", 0)
	}
	requestContent, requestHash, err := canonicalRequest(request, idempotencyKey)
	if err != nil {
		return Job{}, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Job{}, fmt.Errorf("begin Admission transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)

	requestContext, err := establishRequestContext(
		ctx,
		queries,
		principal,
		projectID,
		identity.ScopeJobsSubmit,
	)
	if err != nil {
		return Job{}, err
	}
	if _, err := queries.LockIdempotencyKey(ctx, store.LockIdempotencyKeyParams{
		ProjectID:      projectID.String(),
		IdempotencyKey: idempotencyKey,
	}); err != nil {
		return Job{}, fmt.Errorf("lock Idempotency-Key: %w", err)
	}

	existing, err := queries.GetIdempotencyResult(ctx, store.GetIdempotencyResultParams{
		OrganizationID: principal.OrganizationID,
		ProjectID:      projectID,
		IdempotencyKey: idempotencyKey,
	})
	if err == nil {
		if !bytes.Equal(existing.RequestHash, requestHash[:]) {
			return Job{}, failure(
				FailureCodeIdempotencyConflict,
				"Idempotency-Key was already accepted with different request content",
				0,
			)
		}
		row, getErr := queries.GetJob(ctx, store.GetJobParams{
			OrganizationID: principal.OrganizationID,
			ProjectID:      projectID,
			JobID:          existing.JobID,
		})
		if getErr != nil {
			return Job{}, fmt.Errorf("load idempotent Job: %w", getErr)
		}
		job, mapErr := jobFromGetRow(row)
		if mapErr != nil {
			return Job{}, mapErr
		}
		if err := tx.Commit(ctx); err != nil {
			return Job{}, fmt.Errorf("commit idempotent Admission replay: %w", err)
		}
		return job, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, fmt.Errorf("read Idempotency-Key result: %w", err)
	}

	sku, err := queries.ResolveActiveSKU(ctx, store.ResolveActiveSKUParams{
		Model:            request.Model,
		GenerationPreset: request.GenerationPreset,
		ServiceClass:     request.ServiceClass,
		OutputSpec:       request.OutputSpec,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, failure(FailureCodeInvalidSKU, "no ACTIVE certified Rate Card line matches the request", 0)
	}
	if err != nil {
		return Job{}, fmt.Errorf("resolve ACTIVE SKU: %w", err)
	}
	route, err := queries.ResolveStageJobExecutionRoute(ctx, store.ResolveStageJobExecutionRouteParams{
		OrganizationID:  principal.OrganizationID,
		ProjectID:       projectID,
		ModelRevisionID: sku.ModelRevisionID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, failure(
			FailureCodeCapacityUnavailable,
			"no active Stage execution route is available",
			defaultCapacityRetryAfter,
		)
	}
	if err != nil {
		return Job{}, fmt.Errorf("resolve Stage Job execution route: %w", err)
	}

	project, err := queries.LockProjectForAdmission(ctx, store.LockProjectForAdmissionParams{
		OrganizationID: principal.OrganizationID,
		ProjectID:      projectID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, failure(FailureCodeForbidden, "Project is not available to this credential", 0)
	}
	if err != nil {
		return Job{}, fmt.Errorf("lock Project Admission counters: %w", err)
	}
	if project.OrganizationStatus != "ACTIVE" {
		return Job{}, failure(FailureCodeOrganizationInactive, "Customer Organization is not active", 0)
	}
	if project.QueuedCount >= project.QueuedLimit {
		return Job{}, failure(
			FailureCodeProjectLimitExceeded,
			"Project Admission limit is exhausted",
			int(project.RetryAfterSeconds),
		)
	}

	if route.StageCutoverRevisionID == uuid.Nil ||
		route.ExecutionGraphRevisionID == uuid.Nil ||
		route.ExecutionProfileRevisionID == uuid.Nil ||
		route.ReservedStorageBytes <= 0 {
		return Job{}, errors.New("resolved Stage execution route is incomplete")
	}
	capacity, capacityErr := queries.LockStageGraphReadyCapacityPath(
		ctx,
		store.LockStageGraphReadyCapacityPathParams{
			ExecutionGraphRevisionID:   route.ExecutionGraphRevisionID,
			ExecutionProfileRevisionID: route.ExecutionProfileRevisionID,
		},
	)
	if capacityErr != nil {
		return Job{}, fmt.Errorf("lock Stage graph READY capacity path: %w", capacityErr)
	}
	if !capacity.Ready {
		return Job{}, failure(
			FailureCodeCapacityUnavailable,
			"no complete READY Stage capacity path is available",
			int(capacity.RetryAfterSeconds),
		)
	}

	quotedAmount, ok := checkedMultiply(sku.UnitAmountMinor, int64(request.GenerationCount))
	if !ok {
		return Job{}, errors.New("resolved SKU quote overflows integer minor units")
	}
	maxTotalComputeSeconds, ok := computeBudgetSeconds(
		sku.CertifiedP95ComputeSeconds,
		sku.MaxTotalComputeMultiplierMilli,
	)
	if !ok {
		return Job{}, errors.New("resolved Retry Budget overflows seconds")
	}
	jobLifetimeSeconds, ok := deriveJobLifetimeSeconds(
		sku.QueueRetryAllowanceSeconds,
		maxTotalComputeSeconds,
		sku.MaxAttempts,
		sku.MaxFinalizationSecondsPerAttempt,
	)
	if !ok {
		return Job{}, errors.New("resolved Job Expiry policy overflows seconds")
	}
	requestContentRetentionSeconds := int64(project.RequestContentRetentionDays) * 24 * 60 * 60
	if requestContentRetentionSeconds <= jobLifetimeSeconds {
		return Job{}, errors.New("retention policy expires request content before Job Expiry")
	}

	credit, err := queries.LockCreditAccount(ctx, principal.OrganizationID)
	if err != nil {
		return Job{}, fmt.Errorf("lock Contract Credit account: %w", err)
	}
	if credit.Currency != sku.Currency {
		return Job{}, failure(FailureCodeInvalidSKU, "SKU currency does not match Contract Credit account", 0)
	}
	availableCredit := credit.ContractCreditLimitMinor - credit.UnsettledPostedMinor - credit.ReservedMinor
	if availableCredit < quotedAmount {
		return Job{}, failure(
			FailureCodeCreditLimitExceeded,
			"Contract Credit Limit is insufficient for the quoted amount",
			0,
		)
	}

	if rows, updateErr := queries.IncrementProjectQueued(ctx, store.IncrementProjectQueuedParams{
		OrganizationID: principal.OrganizationID,
		ProjectID:      projectID,
	}); updateErr != nil || rows != 1 {
		if updateErr != nil {
			return Job{}, fmt.Errorf("increment Project queue counter: %w", updateErr)
		}
		return Job{}, failure(FailureCodeProjectLimitExceeded, "Project Admission limit is exhausted", int(project.RetryAfterSeconds))
	}
	if rows, updateErr := queries.ReserveOrganizationCredit(ctx, store.ReserveOrganizationCreditParams{
		AmountMinor:    quotedAmount,
		OrganizationID: principal.OrganizationID,
		Currency:       sku.Currency,
	}); updateErr != nil || rows != 1 {
		if updateErr != nil {
			return Job{}, fmt.Errorf("reserve Contract Credit: %w", updateErr)
		}
		return Job{}, failure(FailureCodeCreditLimitExceeded, "Contract Credit Limit is insufficient for the quoted amount", 0)
	}

	jobID := uuid.New()
	err = queries.InsertJob(ctx, store.InsertJobParams{
		ID:                                        jobID,
		OrganizationID:                            principal.OrganizationID,
		ProjectID:                                 projectID,
		CreatedByPrincipalID:                      principal.PrincipalID,
		ModelRevisionID:                           sku.ModelRevisionID,
		GenerationPresetRevisionID:                sku.GenerationPresetRevisionID,
		ServiceClassRevisionID:                    sku.ServiceClassRevisionID,
		OutputSpecID:                              sku.OutputSpecID,
		StageCutoverRevisionID:                    route.StageCutoverRevisionID,
		ExecutionGraphRevisionID:                  route.ExecutionGraphRevisionID,
		StageExecutionProfileRevisionID:           route.ExecutionProfileRevisionID,
		RequestHash:                               requestHash[:],
		RequestContent:                            requestContent,
		RetentionPolicyRevisionID:                 project.RetentionPolicyRevisionID,
		RetentionArtifactDays:                     project.ArtifactRetentionDays,
		RetentionRequestContentDays:               project.RequestContentRetentionDays,
		RetentionIncompleteContentHours:           project.IncompleteContentRetentionHours,
		RetentionScratchHours:                     project.ScratchRetentionHours,
		RetentionDebugHours:                       project.DebugRetentionHours,
		RetentionMetadataDays:                     project.MetadataRetentionDays,
		RetentionFinancialDays:                    project.FinancialRetentionDays,
		PricingRateCardRevisionID:                 sku.RateCardRevisionID,
		PricingRateLineID:                         sku.RateLineID,
		PricingUnitAmountMinor:                    sku.UnitAmountMinor,
		PricingQuantity:                           request.GenerationCount,
		PricingQuotedAmountMinor:                  quotedAmount,
		PricingCurrency:                           sku.Currency,
		ExecutionMaxAttempts:                      sku.MaxAttempts,
		ExecutionMaxTotalComputeSeconds:           maxTotalComputeSeconds,
		ExecutionMaxFinalizationSecondsPerAttempt: sku.MaxFinalizationSecondsPerAttempt,
		ExecutionRetryBackoffPolicy:               sku.RetryBackoffPolicy,
		ExecutionRetryableFailureClasses:          sku.RetryableFailureClasses,
		ExecutionCircuitBreakerPolicy:             sku.CircuitBreakerPolicy,
		ExecutionCircuitFingerprintWindowSeconds:  sku.CircuitFingerprintWindowSeconds,
		ExecutionCircuitMinDistinctHealthyWorkers: sku.CircuitMinDistinctHealthyWorkers,
		JobLifetimeSeconds:                        jobLifetimeSeconds,
	})
	if err != nil {
		return Job{}, fmt.Errorf("insert Accepted Job: %w", err)
	}
	if err := queries.InsertRetryRuntimeState(ctx, store.InsertRetryRuntimeStateParams{
		JobID:          jobID,
		OrganizationID: principal.OrganizationID,
		ProjectID:      projectID,
	}); err != nil {
		return Job{}, fmt.Errorf("insert RetryRuntimeState: %w", err)
	}
	if err := queries.InsertCreditReservation(ctx, store.InsertCreditReservationParams{
		ID:             uuid.New(),
		OrganizationID: principal.OrganizationID,
		ProjectID:      projectID,
		JobID:          jobID,
		AmountMinor:    quotedAmount,
		Currency:       sku.Currency,
	}); err != nil {
		return Job{}, fmt.Errorf("insert Credit Reservation: %w", err)
	}
	instantiation, instantiateErr := queries.InstantiateAdmittedStageGraph(
		ctx,
		store.InstantiateAdmittedStageGraphParams{
			OrganizationID: principal.OrganizationID,
			ProjectID:      projectID,
			JobID:          jobID,
		},
	)
	if instantiateErr != nil {
		var postgresError *pgconn.PgError
		if errors.As(instantiateErr, &postgresError) &&
			(postgresError.ConstraintName == "stage_graph_ready_capacity_unavailable" ||
				postgresError.ConstraintName == "stage_ready_queue_depth_exceeded") {
			return Job{}, failure(
				FailureCodeCapacityUnavailable,
				"no complete READY Stage capacity path is available",
				defaultCapacityRetryAfter,
			)
		}
		return Job{}, fmt.Errorf("instantiate Accepted Stage graph: %w", instantiateErr)
	}
	aggregateVersion := instantiation.AggregateVersion
	if err := queries.InsertIdempotencyResult(ctx, store.InsertIdempotencyResultParams{
		OrganizationID: principal.OrganizationID,
		ProjectID:      projectID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    requestHash[:],
		JobID:          jobID,
	}); err != nil {
		return Job{}, fmt.Errorf("insert Idempotency-Key result: %w", err)
	}

	eventID := uuid.New()
	event := &velav1.EventEnvelope{
		EventId:          eventID.String(),
		AggregateType:    "Job",
		AggregateId:      jobID.String(),
		AggregateVersion: uint64(aggregateVersion),
		EventType:        "job.ready",
		SchemaVersion:    1,
		OccurredAt:       timestamppb.New(requestContext.TransactionTime.Time),
		Payload: &velav1.EventEnvelope_JobReady{JobReady: &velav1.JobReady{
			OrganizationId:             principal.OrganizationID.String(),
			ProjectId:                  projectID.String(),
			JobId:                      jobID.String(),
			ModelRevisionId:            sku.ModelRevisionID.String(),
			GenerationPresetRevisionId: sku.GenerationPresetRevisionID.String(),
			ServiceClassRevisionId:     sku.ServiceClassRevisionID.String(),
			OutputSpecId:               sku.OutputSpecID.String(),
		}},
	}
	payload, err := proto.Marshal(event)
	if err != nil {
		return Job{}, fmt.Errorf("marshal job.ready event: %w", err)
	}
	if err := queries.InsertOutboxEvent(ctx, store.InsertOutboxEventParams{
		EventID:          eventID,
		OrganizationID:   principal.OrganizationID,
		ProjectID:        projectID,
		JobID:            jobID,
		AggregateVersion: aggregateVersion,
		Payload:          payload,
		OccurredAt:       requestContext.TransactionTime,
	}); err != nil {
		return Job{}, fmt.Errorf("insert job.ready Outbox event: %w", err)
	}

	row, err := queries.GetJob(ctx, store.GetJobParams{
		OrganizationID: principal.OrganizationID,
		ProjectID:      projectID,
		JobID:          jobID,
	})
	if err != nil {
		return Job{}, fmt.Errorf("load newly Accepted Job: %w", err)
	}
	job, err := jobFromGetRow(row)
	if err != nil {
		return Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, fmt.Errorf("commit Admission: %w", err)
	}
	return job, nil
}

func (s *Service) Get(
	ctx context.Context,
	principal identity.Principal,
	projectID uuid.UUID,
	jobID uuid.UUID,
) (Job, error) {
	if s == nil || s.pool == nil {
		return Job{}, errors.New("admission service is not configured")
	}
	if principal.ProjectID != projectID {
		return Job{}, failure(FailureCodeNotFound, "Job is not visible in this Project", 0)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, fmt.Errorf("begin Job read transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)
	_, err = establishRequestContext(ctx, queries, principal, projectID, identity.ScopeJobsRead)
	if err != nil {
		return Job{}, err
	}
	row, err := queries.GetJob(ctx, store.GetJobParams{
		OrganizationID: principal.OrganizationID,
		ProjectID:      projectID,
		JobID:          jobID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, failure(FailureCodeNotFound, "Job is not visible in this Project", 0)
	}
	if err != nil {
		return Job{}, fmt.Errorf("get Job: %w", err)
	}
	job, err := jobFromGetRow(row)
	if err != nil {
		return Job{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, fmt.Errorf("commit Job read: %w", err)
	}
	return job, nil
}

func establishRequestContext(
	ctx context.Context,
	queries *store.Queries,
	principal identity.Principal,
	projectID uuid.UUID,
	requiredScope string,
) (store.SetRequestContextRow, error) {
	credentialProof := principal.RequestContextProof()
	defer clear(credentialProof)
	requestContext, err := queries.SetRequestContext(ctx, store.SetRequestContextParams{
		CredentialID:    principal.CredentialID,
		CredentialProof: credentialProof,
		RequiredScope:   requiredScope,
	})
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "28000" {
			return store.SetRequestContextRow{}, failure(
				FailureCodeForbidden,
				"credential is no longer authorized for this operation",
				0,
			)
		}
		return store.SetRequestContextRow{}, fmt.Errorf("set request identity context: %w", err)
	}
	if requestContext.OrganizationID != principal.OrganizationID ||
		requestContext.ProjectID != projectID ||
		requestContext.PrincipalID != principal.PrincipalID {
		return store.SetRequestContextRow{}, errors.New(
			"database request identity does not match authenticated Principal",
		)
	}
	if !requestContext.TransactionTime.Valid {
		return store.SetRequestContextRow{}, errors.New("transaction time is unavailable from PostgreSQL")
	}
	return requestContext, nil
}

func canonicalRequest(request Request, idempotencyKey string) ([]byte, [sha256.Size]byte, error) {
	if len(idempotencyKey) == 0 || len(idempotencyKey) > 128 || !idempotencyKeyPattern.MatchString(idempotencyKey) {
		return nil, [sha256.Size]byte{}, failure(FailureCodeInvalidRequest, "Idempotency-Key is invalid", 0)
	}
	if request.Model == "" || len(request.Model) > 100 || request.OutputSpec == "" || len(request.OutputSpec) > 100 {
		return nil, [sha256.Size]byte{}, failure(FailureCodeInvalidRequest, "model and output_spec are required", 0)
	}
	if request.GenerationPreset != "quality" && request.GenerationPreset != "balanced" && request.GenerationPreset != "fast" {
		return nil, [sha256.Size]byte{}, failure(FailureCodeInvalidRequest, "generation_preset is invalid", 0)
	}
	if request.ServiceClass != "standard" {
		return nil, [sha256.Size]byte{}, failure(FailureCodeInvalidRequest, "service_class is invalid", 0)
	}
	if request.GenerationCount < 1 || request.GenerationCount > 16 {
		return nil, [sha256.Size]byte{}, failure(FailureCodeInvalidRequest, "generation_count must be between 1 and 16", 0)
	}
	promptLength := utf8.RuneCountInString(request.Prompt)
	if promptLength < 1 || promptLength > 20000 {
		return nil, [sha256.Size]byte{}, failure(FailureCodeInvalidRequest, "prompt length is invalid", 0)
	}
	if len(request.ClientMetadata) > 0 {
		metadata, err := canonicalJSONObject(request.ClientMetadata)
		if err != nil {
			return nil, [sha256.Size]byte{}, failure(FailureCodeInvalidRequest, "client_metadata must be a JSON object", 0)
		}
		request.ClientMetadata = metadata
	}
	content, err := json.Marshal(request)
	if err != nil {
		return nil, [sha256.Size]byte{}, failure(FailureCodeInvalidRequest, "client_metadata cannot be encoded", 0)
	}
	return content, sha256.Sum256(content), nil
}

func canonicalJSONObject(raw json.RawMessage) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("client metadata has trailing JSON values")
	}
	return json.Marshal(value)
}

func checkedMultiply(left, right int64) (int64, bool) {
	if left < 0 || right < 0 || (right != 0 && left > math.MaxInt64/right) {
		return 0, false
	}
	return left * right, true
}

func computeBudgetSeconds(p95Seconds, multiplierMilli int32) (int64, bool) {
	product, ok := checkedMultiply(int64(p95Seconds), int64(multiplierMilli))
	if !ok || product > math.MaxInt64-999 {
		return 0, false
	}
	seconds := (product + 999) / 1000
	return seconds, seconds > 0
}

func deriveJobLifetimeSeconds(
	queueAndRetryAllowanceSeconds int32,
	maxTotalComputeSeconds int64,
	maxAttempts int32,
	maxFinalizationSecondsPerAttempt int32,
) (int64, bool) {
	if queueAndRetryAllowanceSeconds <= 0 || maxTotalComputeSeconds <= 0 || maxAttempts <= 0 ||
		maxFinalizationSecondsPerAttempt <= 0 {
		return 0, false
	}
	finalizationSeconds, ok := checkedMultiply(int64(maxAttempts), int64(maxFinalizationSecondsPerAttempt))
	if !ok {
		return 0, false
	}
	lifetimeSeconds, ok := checkedAdd(int64(queueAndRetryAllowanceSeconds), maxTotalComputeSeconds)
	if !ok {
		return 0, false
	}
	return checkedAdd(lifetimeSeconds, finalizationSeconds)
}

func checkedAdd(left, right int64) (int64, bool) {
	if left < 0 || right < 0 || left > math.MaxInt64-right {
		return 0, false
	}
	return left + right, true
}

func jobFromGetRow(row store.GetJobRow) (Job, error) {
	if !row.JobExpiresAt.Valid || !row.CreatedAt.Valid {
		return Job{}, errors.New("Job record has invalid timestamps")
	}
	return Job{
		ID:                row.ID,
		ProjectID:         row.ProjectID,
		State:             JobState(row.State),
		Phase:             row.ExecutionPhase,
		PhaseProgress:     row.PhaseProgress,
		AttemptsStarted:   row.AttemptsStarted,
		NextRetryAt:       nullableTime(row.NextRetryAt),
		EstimatedFinishAt: nullableTime(row.EstimatedFinishAt),
		ProgressUpdatedAt: nullableTime(row.ProgressUpdatedAt),
		PricingSnapshot: PricingSnapshot{
			RateCardRevisionID: row.PricingRateCardRevisionID,
			RateLineID:         row.PricingRateLineID,
			UnitAmountMinor:    row.PricingUnitAmountMinor,
			Quantity:           row.PricingQuantity,
			QuotedAmountMinor:  row.PricingQuotedAmountMinor,
			Currency:           row.PricingCurrency,
		},
		JobExpiresAt: row.JobExpiresAt.Time,
		CreatedAt:    row.CreatedAt.Time,
	}, nil
}

func nullableTime(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	instant := value.Time
	return &instant
}

func failure(code FailureCode, message string, retryAfter int) *Failure {
	return &Failure{Code: code, Message: message, RetryAfter: retryAfter}
}
