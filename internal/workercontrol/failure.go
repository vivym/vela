package workercontrol

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	store "github.com/vivym/vela/internal/store/sqlc"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	failureClassPattern       = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,99}$`)
	failureFingerprintPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$`)
)

type RetryDisposition string

const (
	RetryDispositionRetryWait          RetryDisposition = "RETRY_WAIT"
	RetryDispositionFailed             RetryDisposition = "FAILED"
	RetryDispositionRejectedStaleLease RetryDisposition = "REJECTED_STALE_LEASE"
)

type AttemptTerminalState string

const (
	FailedAttempt AttemptTerminalState = "FAILED"
	LostAttempt   AttemptTerminalState = "LOST"
)

type FailureObservation struct {
	FailureClass             string   `json:"failure_class"`
	FailureFingerprint       string   `json:"failure_fingerprint"`
	ErrorSummary             string   `json:"error_summary"`
	BackendStage             string   `json:"backend_stage"`
	GPUUUIDs                 []string `json:"gpu_uuids"`
	InferenceBackendRevision string   `json:"inference_backend_revision"`
	RetryRecommended         bool     `json:"retry_recommended"`
	WorkerReusable           bool     `json:"worker_reusable"`
}

type RetryDecision struct {
	Disposition                RetryDisposition
	FailureClass               string
	AttemptID                  uuid.UUID
	JobID                      uuid.UUID
	AttemptState               AttemptTerminalState
	AttemptComputeSeconds      int64
	TotalComputeSeconds        int64
	AttemptFinalizationSeconds int64
	TotalFinalizationSeconds   int64
	NextRetryAt                *time.Time
	JobFence                   int64
	JobVersion                 int64
	DecidedAt                  time.Time
}

type normalizedFailureObservation struct {
	FailureClass             string   `json:"failure_class"`
	FailureFingerprint       string   `json:"failure_fingerprint"`
	ErrorSummary             string   `json:"error_summary"`
	BackendStage             string   `json:"backend_stage"`
	GPUUUIDs                 []string `json:"gpu_uuids"`
	InferenceBackendRevision string   `json:"inference_backend_revision"`
	RetryRecommended         bool     `json:"retry_recommended"`
	WorkerReusable           bool     `json:"worker_reusable"`
}

type retryBackoffPolicy struct {
	Kind           string `json:"kind"`
	InitialSeconds int64  `json:"initial_seconds"`
	MaxSeconds     int64  `json:"max_seconds"`
}

type excludedWorkerRecord struct {
	WorkerID    uuid.UUID `json:"worker_id"`
	WorkerEpoch int64     `json:"worker_epoch"`
	Reason      string    `json:"reason"`
	ExpiresAt   time.Time `json:"expires_at"`
}

type failureFingerprintRecord struct {
	Fingerprint  string    `json:"fingerprint"`
	FailureClass string    `json:"failure_class"`
	AttemptID    uuid.UUID `json:"attempt_id"`
	ObservedAt   time.Time `json:"observed_at"`
}

func (s *Service) Fail(
	ctx context.Context,
	worker AuthenticatedWorker,
	credentials LeaseCredentials,
	observation FailureObservation,
) (RetryDecision, error) {
	if s == nil || s.pool == nil {
		return RetryDecision{}, errors.New("worker coordinator is not configured")
	}
	if worker.ID == uuid.Nil || credentials.AttemptID == uuid.Nil ||
		credentials.WorkerEpoch <= 0 || credentials.Fence <= 0 || credentials.Token == "" {
		return rejectedStaleFailure(), nil
	}
	normalized, requestHash, err := normalizeFailureObservation(observation)
	if err != nil {
		return RetryDecision{}, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return RetryDecision{}, fmt.Errorf("begin Fail transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	queries := store.New(tx)

	workerRow, err := queries.LockWorkerAuthority(ctx, worker.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedStaleFailure(), nil
	}
	if err != nil {
		return RetryDecision{}, fmt.Errorf("lock Worker for Fail: %w", err)
	}
	if err := queries.LockExecutionLeaseWrites(ctx); err != nil {
		return RetryDecision{}, fmt.Errorf("lock execution Lease writes: %w", err)
	}
	authority, err := queries.LockFailureAuthority(ctx, credentials.AttemptID)
	if errors.Is(err, pgx.ErrNoRows) {
		return rejectedStaleFailure(), nil
	}
	if err != nil {
		return RetryDecision{}, fmt.Errorf("lock Fail authority: %w", err)
	}
	if !authority.WorkerID.Valid || authority.WorkerID.UUID != worker.ID ||
		authority.WorkerEpoch == nil || *authority.WorkerEpoch != credentials.WorkerEpoch ||
		authority.AttemptFence != credentials.Fence ||
		authority.LeaseOwnerKind != store.LeaseOwnerKindWORKER ||
		authority.LeaseOwnerID != workerRow.SpiffeID ||
		(authority.LeasePhase != store.LeasePhaseEXECUTION &&
			authority.LeasePhase != store.LeasePhaseFINALIZATION) ||
		(authority.LeasePhase == store.LeasePhaseFINALIZATION &&
			normalized.BackendStage != "finalization") {
		return rejectedStaleFailure(), nil
	}
	presentedDigest := sha256.Sum256([]byte(credentials.Token))
	if !hmac.Equal(presentedDigest[:], authority.TokenDigest) {
		return rejectedStaleFailure(), nil
	}

	stored, decisionErr := queries.GetExecutionFailureDecision(
		ctx,
		uuid.NullUUID{UUID: credentials.AttemptID, Valid: true},
	)
	if decisionErr == nil {
		if !hmac.Equal(requestHash[:], stored.RequestHash) {
			return rejectedStaleFailure(), nil
		}
		decision, convertErr := retryDecisionFromStored(stored)
		if convertErr != nil {
			return RetryDecision{}, convertErr
		}
		if err := tx.Commit(ctx); err != nil {
			return RetryDecision{}, fmt.Errorf("commit Fail replay: %w", err)
		}
		return decision, nil
	}
	if !errors.Is(decisionErr, pgx.ErrNoRows) {
		return RetryDecision{}, fmt.Errorf("read durable RetryDecision: %w", decisionErr)
	}
	if workerRow.Epoch != credentials.WorkerEpoch {
		return rejectedStaleFailure(), nil
	}

	decidedAt, err := postgresTime(ctx, queries)
	if err != nil {
		return RetryDecision{}, err
	}
	executionActive := authority.LeasePhase == store.LeasePhaseEXECUTION &&
		(authority.AttemptState == store.AttemptStateASSIGNED ||
			authority.AttemptState == store.AttemptStateRUNNING) &&
		(authority.JobState == store.JobStateASSIGNED || authority.JobState == store.JobStateRUNNING)
	finalizationActive := authority.LeasePhase == store.LeasePhaseFINALIZATION &&
		authority.AttemptState == store.AttemptStateFINALIZING &&
		authority.JobState == store.JobStateFINALIZING
	if authority.LeaseRevokedAt.Valid || authority.CurrentFence != credentials.Fence ||
		!authority.LeaseExpiresAt.Valid || !authority.LeaseExpiresAt.Time.After(decidedAt) ||
		!authority.JobExpiresAt.Valid || !authority.JobExpiresAt.Time.After(decidedAt) ||
		(!executionActive && !finalizationActive) {
		return rejectedStaleFailure(), nil
	}

	decision, err := s.applyAttemptFailure(
		ctx,
		queries,
		workerRow,
		authority,
		normalized,
		requestHash,
		decidedAt,
		attemptFailureTransition{
			Source:           store.ExecutionFailureSourceWORKERREPORTED,
			AttemptState:     store.AttemptStateFAILED,
			AllowRetry:       true,
			WorkerTransition: workerFailureReported,
		},
	)
	if err != nil {
		return RetryDecision{}, err
	}

	commitTime, err := postgresTime(ctx, queries)
	if err != nil {
		return RetryDecision{}, err
	}
	if !authority.LeaseExpiresAt.Time.After(commitTime) || !authority.JobExpiresAt.Time.After(commitTime) {
		return rejectedStaleFailure(), nil
	}
	if err := tx.Commit(ctx); err != nil {
		return RetryDecision{}, fmt.Errorf("commit Fail: %w", err)
	}
	return decision, nil
}

type legacyFailureBinding struct {
	workerID                   uuid.UUID
	workerEpoch                int64
	profileCertificationID     uuid.UUID
	executionProfileRevisionID uuid.UUID
}

func requireLegacyFailureBinding(authority store.LockFailureAuthorityRow) (legacyFailureBinding, error) {
	if !authority.WorkerID.Valid || authority.WorkerEpoch == nil ||
		!authority.ProfileCertificationID.Valid || !authority.ExecutionProfileRevisionID.Valid {
		return legacyFailureBinding{}, errors.New("legacy failure authority is missing Worker or Profile binding")
	}
	return legacyFailureBinding{
		workerID:                   authority.WorkerID.UUID,
		workerEpoch:                *authority.WorkerEpoch,
		profileCertificationID:     authority.ProfileCertificationID.UUID,
		executionProfileRevisionID: authority.ExecutionProfileRevisionID.UUID,
	}, nil
}

func normalizeFailureObservation(
	observation FailureObservation,
) (normalizedFailureObservation, [sha256.Size]byte, error) {
	if !failureClassPattern.MatchString(observation.FailureClass) ||
		!failureFingerprintPattern.MatchString(observation.FailureFingerprint) ||
		!validPrintableFailureText(observation.ErrorSummary, 2000) ||
		!validPrintableFailureText(observation.BackendStage, 100) ||
		!validPrintableFailureText(observation.InferenceBackendRevision, 200) ||
		len(observation.GPUUUIDs) > 8 {
		return normalizedFailureObservation{}, [sha256.Size]byte{}, errors.New("invalid FailureObservation")
	}
	gpuUUIDs := append([]string{}, observation.GPUUUIDs...)
	for _, gpuUUID := range gpuUUIDs {
		if !validPrintableFailureText(gpuUUID, 100) {
			return normalizedFailureObservation{}, [sha256.Size]byte{}, errors.New("invalid FailureObservation GPU UUID")
		}
	}
	sort.Strings(gpuUUIDs)
	for i := 1; i < len(gpuUUIDs); i++ {
		if gpuUUIDs[i] == gpuUUIDs[i-1] {
			return normalizedFailureObservation{}, [sha256.Size]byte{}, errors.New("FailureObservation GPU UUIDs must be distinct")
		}
	}
	normalized := normalizedFailureObservation{
		FailureClass:             observation.FailureClass,
		FailureFingerprint:       observation.FailureFingerprint,
		ErrorSummary:             observation.ErrorSummary,
		BackendStage:             observation.BackendStage,
		GPUUUIDs:                 gpuUUIDs,
		InferenceBackendRevision: observation.InferenceBackendRevision,
		RetryRecommended:         observation.RetryRecommended,
		WorkerReusable:           observation.WorkerReusable,
	}
	canonical, err := json.Marshal(normalized)
	if err != nil {
		return normalizedFailureObservation{}, [sha256.Size]byte{}, fmt.Errorf("canonicalize FailureObservation: %w", err)
	}
	return normalized, sha256.Sum256(canonical), nil
}

func validPrintableFailureText(value string, maxRunes int) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" {
		return false
	}
	count := 0
	for _, character := range value {
		if !unicode.IsPrint(character) {
			return false
		}
		count++
		if count > maxRunes {
			return false
		}
	}
	return count > 0
}

func retryTime(
	authority store.LockFailureAuthorityRow,
	observation normalizedFailureObservation,
	totalComputeSeconds int64,
	decidedAt time.Time,
) (*time.Time, bool) {
	if !observation.RetryRecommended ||
		!containsFailureClass(authority.ExecutionRetryableFailureClasses, observation.FailureClass) ||
		authority.AttemptsStarted >= authority.ExecutionMaxAttempts ||
		totalComputeSeconds >= authority.ExecutionMaxTotalComputeSeconds {
		return nil, false
	}
	backoff, err := parseRetryBackoff(authority.ExecutionRetryBackoffPolicy, authority.AttemptsStarted)
	if err != nil {
		return nil, false
	}
	nextRetryAt := decidedAt.Add(backoff)
	if !nextRetryAt.Before(authority.JobExpiresAt.Time) {
		return nil, false
	}
	return &nextRetryAt, true
}

func parseRetryBackoff(raw []byte, attemptsStarted int32) (time.Duration, error) {
	var policy retryBackoffPolicy
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&policy); err != nil {
		return 0, fmt.Errorf("parse Retry backoff policy: %w", err)
	}
	if policy.Kind != "exponential" || policy.InitialSeconds <= 0 ||
		policy.MaxSeconds < policy.InitialSeconds || attemptsStarted <= 0 ||
		policy.MaxSeconds > int64(math.MaxInt64/time.Second) {
		return 0, errors.New("invalid exponential Retry backoff policy")
	}
	seconds := policy.InitialSeconds
	for attempt := int32(1); attempt < attemptsStarted && seconds < policy.MaxSeconds; attempt++ {
		if seconds > policy.MaxSeconds/2 {
			seconds = policy.MaxSeconds
		} else {
			seconds *= 2
		}
	}
	if seconds > policy.MaxSeconds {
		seconds = policy.MaxSeconds
	}
	return time.Duration(seconds) * time.Second, nil
}

func attemptComputeSeconds(authority store.LockFailureAuthorityRow, decidedAt time.Time) (int64, error) {
	if authority.AttemptState == store.AttemptStateASSIGNED {
		if authority.AttemptStartedAt.Valid {
			return 0, errors.New("assigned Attempt has a start time")
		}
		return 0, nil
	}
	if authority.AttemptState != store.AttemptStateRUNNING &&
		authority.AttemptState != store.AttemptStateFINALIZING {
		return 0, errors.New("active Attempt state is inconsistent with its start time")
	}
	if !authority.AttemptStartedAt.Valid {
		return 0, errors.New("active Attempt state is inconsistent with its start time")
	}
	endedAt := decidedAt
	if authority.AttemptState == store.AttemptStateFINALIZING {
		if !authority.FinalizationStartedAt.Valid || !authority.FinalizationDeadlineAt.Valid {
			return 0, errors.New("FINALIZING Attempt has incomplete finalization timestamps")
		}
		endedAt = authority.FinalizationStartedAt.Time
	} else if authority.LeaseExpiresAt.Time.Before(endedAt) {
		endedAt = authority.LeaseExpiresAt.Time
	}
	if authority.JobExpiresAt.Time.Before(endedAt) {
		endedAt = authority.JobExpiresAt.Time
	}
	return conservativeElapsedSeconds(
		authority.AttemptStartedAt.Time,
		endedAt,
		"attempt compute interval",
	)
}

func attemptFinalizationSeconds(authority store.LockFailureAuthorityRow, decidedAt time.Time) (int64, error) {
	if authority.AttemptState != store.AttemptStateFINALIZING {
		if authority.FinalizationStartedAt.Valid || authority.FinalizationDeadlineAt.Valid {
			return 0, errors.New("pre-finalization Attempt has finalization timestamps")
		}
		return 0, nil
	}
	if !authority.FinalizationStartedAt.Valid || !authority.FinalizationDeadlineAt.Valid {
		return 0, errors.New("FINALIZING Attempt has incomplete finalization timestamps")
	}
	endedAt := decidedAt
	if authority.FinalizationDeadlineAt.Time.Before(endedAt) {
		endedAt = authority.FinalizationDeadlineAt.Time
	}
	if authority.JobExpiresAt.Time.Before(endedAt) {
		endedAt = authority.JobExpiresAt.Time
	}
	return conservativeElapsedSeconds(
		authority.FinalizationStartedAt.Time,
		endedAt,
		"attempt finalization interval",
	)
}

func conservativeElapsedSeconds(startedAt time.Time, endedAt time.Time, intervalName string) (int64, error) {
	if endedAt.Before(startedAt) {
		return 0, fmt.Errorf("%s has inconsistent timestamps", intervalName)
	}
	endSeconds := endedAt.Unix()
	startSeconds := startedAt.Unix()
	if startSeconds < 0 && endSeconds > math.MaxInt64+startSeconds {
		return 0, fmt.Errorf("%s overflows seconds", intervalName)
	}
	seconds := endSeconds - startSeconds
	if endedAt.Nanosecond() > startedAt.Nanosecond() {
		if seconds == math.MaxInt64 {
			return 0, fmt.Errorf("%s overflows seconds", intervalName)
		}
		seconds++
	}
	return seconds, nil
}

func containsFailureClass(classes []string, target string) bool {
	for _, class := range classes {
		if class == target {
			return true
		}
	}
	return false
}

func updateWorkerAfterReportedFailure(
	ctx context.Context,
	queries *store.Queries,
	worker store.LockWorkerAuthorityRow,
	reusable bool,
	decidedAt pgtype.Timestamptz,
) error {
	var rows int64
	var err error
	if reusable && worker.ReachabilityCondition == store.WorkerReachabilityConditionHEALTHY {
		rows, err = queries.MarkWorkerReusableAfterFailure(ctx, store.MarkWorkerReusableAfterFailureParams{
			DecidedAt:   decidedAt,
			WorkerID:    worker.ID,
			WorkerEpoch: worker.Epoch,
		})
	} else {
		rows, err = queries.MarkWorkerDrainingAfterFailure(ctx, store.MarkWorkerDrainingAfterFailureParams{
			DecidedAt:   decidedAt,
			WorkerID:    worker.ID,
			WorkerEpoch: worker.Epoch,
		})
	}
	if err != nil || rows != 1 {
		return changedRowsError("transition Worker after Fail", rows, err)
	}
	return nil
}

func appendExcludedWorker(raw []byte, record excludedWorkerRecord) ([]byte, error) {
	var records []excludedWorkerRecord
	if err := json.Unmarshal(raw, &records); err != nil {
		return nil, fmt.Errorf("decode RetryRuntimeState excluded Workers: %w", err)
	}
	records = append(records, record)
	encoded, err := json.Marshal(records)
	if err != nil {
		return nil, fmt.Errorf("encode RetryRuntimeState excluded Workers: %w", err)
	}
	return encoded, nil
}

func workerExcludedForJob(raw []byte, workerID uuid.UUID, now time.Time) (bool, error) {
	var records []excludedWorkerRecord
	if err := json.Unmarshal(raw, &records); err != nil {
		return false, fmt.Errorf("decode RetryRuntimeState excluded Workers: %w", err)
	}
	for _, record := range records {
		if record.WorkerID == workerID && record.ExpiresAt.After(now) {
			return true, nil
		}
	}
	return false, nil
}

func appendFailureFingerprint(raw []byte, record failureFingerprintRecord) ([]byte, error) {
	var records []failureFingerprintRecord
	if err := json.Unmarshal(raw, &records); err != nil {
		return nil, fmt.Errorf("decode RetryRuntimeState failure fingerprints: %w", err)
	}
	records = append(records, record)
	encoded, err := json.Marshal(records)
	if err != nil {
		return nil, fmt.Errorf("encode RetryRuntimeState failure fingerprints: %w", err)
	}
	return encoded, nil
}

func insertRetryWaitEvent(
	ctx context.Context,
	queries *store.Queries,
	authority store.LockFailureAuthorityRow,
	observation normalizedFailureObservation,
	attemptComputeSeconds int64,
	totalComputeSeconds int64,
	attemptFinalizationSeconds int64,
	totalFinalizationSeconds int64,
	jobFence int64,
	jobVersion int64,
	nextRetryAt time.Time,
	decidedAt time.Time,
) error {
	eventID := uuid.New()
	event := &velav1.EventEnvelope{
		EventId:          eventID.String(),
		AggregateType:    "Job",
		AggregateId:      authority.JobID.String(),
		AggregateVersion: uint64(jobVersion),
		EventType:        "job.retry_wait",
		SchemaVersion:    1,
		OccurredAt:       timestamppb.New(decidedAt),
		Payload: &velav1.EventEnvelope_JobRetryWait{JobRetryWait: &velav1.JobRetryWait{
			OrganizationId:             authority.OrganizationID.String(),
			ProjectId:                  authority.ProjectID.String(),
			JobId:                      authority.JobID.String(),
			AttemptId:                  authority.AttemptID.String(),
			AttemptNumber:              uint32(authority.AttemptNumber),
			AttemptFence:               uint64(authority.AttemptFence),
			JobFence:                   uint64(jobFence),
			FailureClass:               observation.FailureClass,
			AttemptComputeSeconds:      uint64(attemptComputeSeconds),
			TotalComputeSeconds:        uint64(totalComputeSeconds),
			AttemptFinalizationSeconds: uint64(attemptFinalizationSeconds),
			TotalFinalizationSeconds:   uint64(totalFinalizationSeconds),
			NextRetryAt:                timestamppb.New(nextRetryAt),
			DecidedAt:                  timestamppb.New(decidedAt),
		}},
	}
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal job.retry_wait event: %w", err)
	}
	if err := queries.InsertRetryWaitOutboxEvent(ctx, store.InsertRetryWaitOutboxEventParams{
		EventID:        eventID,
		OrganizationID: authority.OrganizationID,
		ProjectID:      authority.ProjectID,
		JobID:          authority.JobID,
		JobVersion:     jobVersion,
		Payload:        payload,
		OccurredAt:     pgtype.Timestamptz{Time: decidedAt, Valid: true},
	}); err != nil {
		return fmt.Errorf("insert job.retry_wait Outbox event: %w", err)
	}
	return nil
}

func insertJobFailedEvent(
	ctx context.Context,
	queries *store.Queries,
	authority store.LockFailureAuthorityRow,
	failureClass string,
	attemptState store.AttemptState,
	attemptComputeSeconds int64,
	totalComputeSeconds int64,
	attemptFinalizationSeconds int64,
	totalFinalizationSeconds int64,
	jobFence int64,
	jobVersion int64,
	decidedAt time.Time,
) error {
	eventID := uuid.New()
	event := &velav1.EventEnvelope{
		EventId:          eventID.String(),
		AggregateType:    "Job",
		AggregateId:      authority.JobID.String(),
		AggregateVersion: uint64(jobVersion),
		EventType:        "job.failed",
		SchemaVersion:    1,
		OccurredAt:       timestamppb.New(decidedAt),
		Payload: &velav1.EventEnvelope_JobFailed{JobFailed: &velav1.JobFailed{
			OrganizationId:             authority.OrganizationID.String(),
			ProjectId:                  authority.ProjectID.String(),
			JobId:                      authority.JobID.String(),
			AttemptId:                  authority.AttemptID.String(),
			AttemptNumber:              uint32(authority.AttemptNumber),
			AttemptFence:               uint64(authority.AttemptFence),
			JobFence:                   uint64(jobFence),
			FailureClass:               failureClass,
			AttemptState:               string(attemptState),
			AttemptComputeSeconds:      uint64(attemptComputeSeconds),
			TotalComputeSeconds:        uint64(totalComputeSeconds),
			AttemptFinalizationSeconds: uint64(attemptFinalizationSeconds),
			TotalFinalizationSeconds:   uint64(totalFinalizationSeconds),
			DecidedAt:                  timestamppb.New(decidedAt),
		}},
	}
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal job.failed event: %w", err)
	}
	if err := queries.InsertJobFailedOutboxEvent(ctx, store.InsertJobFailedOutboxEventParams{
		EventID:        eventID,
		OrganizationID: authority.OrganizationID,
		ProjectID:      authority.ProjectID,
		JobID:          authority.JobID,
		JobVersion:     jobVersion,
		Payload:        payload,
		OccurredAt:     pgtype.Timestamptz{Time: decidedAt, Valid: true},
	}); err != nil {
		return fmt.Errorf("insert job.failed Outbox event: %w", err)
	}
	return nil
}

func retryDecisionFromStored(row store.GetExecutionFailureDecisionRow) (RetryDecision, error) {
	if !row.AttemptID.Valid || row.AttemptState == nil || !row.DecidedAt.Valid {
		return RetryDecision{}, errors.New("stored RetryDecision is incomplete")
	}
	disposition := RetryDisposition(row.Disposition)
	if disposition != RetryDispositionRetryWait && disposition != RetryDispositionFailed {
		return RetryDecision{}, errors.New("stored RetryDecision disposition is invalid")
	}
	attemptState := AttemptTerminalState(*row.AttemptState)
	if attemptState != FailedAttempt && attemptState != LostAttempt {
		return RetryDecision{}, errors.New("stored RetryDecision Attempt state is invalid")
	}
	var nextRetryAt *time.Time
	if row.NextRetryAt.Valid {
		value := row.NextRetryAt.Time
		nextRetryAt = &value
	}
	return RetryDecision{
		Disposition:                disposition,
		FailureClass:               row.FailureClass,
		AttemptID:                  row.AttemptID.UUID,
		JobID:                      row.JobID,
		AttemptState:               attemptState,
		AttemptComputeSeconds:      row.AttemptComputeSeconds,
		TotalComputeSeconds:        row.TotalComputeSeconds,
		AttemptFinalizationSeconds: row.AttemptFinalizationSeconds,
		TotalFinalizationSeconds:   row.TotalFinalizationSeconds,
		NextRetryAt:                nextRetryAt,
		JobFence:                   row.JobFence,
		JobVersion:                 row.JobVersion,
		DecidedAt:                  row.DecidedAt.Time,
	}, nil
}

func rejectedStaleFailure() RetryDecision {
	return RetryDecision{Disposition: RetryDispositionRejectedStaleLease}
}

func mustMarshalJSON(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}
