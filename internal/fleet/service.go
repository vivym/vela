package fleet

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CapacityState string

const (
	CapacityAdmittable         CapacityState = "ADMITTABLE"
	CapacityScratchPressured   CapacityState = "SCRATCH_PRESSURED"
	CapacityScratchCritical    CapacityState = "SCRATCH_CRITICAL"
	CapacityStorageUnavailable CapacityState = "STORAGE_UNAVAILABLE"
	CapacityMultipleBlockers   CapacityState = "MULTIPLE_BLOCKERS"
)

type ScratchWatermarkState string

const (
	ScratchWatermarkNormal    ScratchWatermarkState = "NORMAL"
	ScratchWatermarkPressured ScratchWatermarkState = "PRESSURED"
	ScratchWatermarkCritical  ScratchWatermarkState = "CRITICAL"
)

type ReadinessState string

const (
	ReadinessChecking ReadinessState = "CHECKING"
	ReadinessReady    ReadinessState = "READY"
	ReadinessFailed   ReadinessState = "FAILED"
	ReadinessExpired  ReadinessState = "EXPIRED"
)

type ReadinessCheck string

const (
	ReadinessIdentity         ReadinessCheck = "IDENTITY"
	ReadinessDevice           ReadinessCheck = "DEVICE"
	ReadinessInferenceBackend ReadinessCheck = "INFERENCE_BACKEND"
	ReadinessModelWarmup      ReadinessCheck = "MODEL_WARMUP"
	ReadinessCanary           ReadinessCheck = "CANARY"
)

type DrainState string

const (
	DrainDraining DrainState = "DRAINING"
	DrainComplete DrainState = "COMPLETE"
	DrainExpired  DrainState = "EXPIRED"
)

type ProtectedResourceKind string

const (
	ProtectedPod        ProtectedResourceKind = "POD"
	ProtectedDaemonSet  ProtectedResourceKind = "DAEMONSET"
	ProtectedWorkerPool ProtectedResourceKind = "WORKER_POOL"
)

type MutationOperation string

const (
	MutationDelete          MutationOperation = "DELETE"
	MutationPatchSelector   MutationOperation = "PATCH_SELECTOR"
	MutationPatchImage      MutationOperation = "PATCH_IMAGE"
	MutationRemoveFinalizer MutationOperation = "REMOVE_FINALIZER"
)

type FailureCode string

const (
	FailureInvalid  FailureCode = "invalid"
	FailureConflict FailureCode = "conflict"
	FailureNotFound FailureCode = "not_found"
)

type Failure struct {
	Code    FailureCode
	Message string
}

func (failure *Failure) Error() string {
	if failure == nil {
		return ""
	}
	return string(failure.Code) + ": " + failure.Message
}

type CapacityObservation struct {
	WorkerID               uuid.UUID
	WorkerPoolID           uuid.UUID
	WorkerEpoch            int64
	Sequence               int64
	ObservedAt             time.Time
	WatermarkState         ScratchWatermarkState
	TotalBytes             int64
	FreeBytes              int64
	HighWatermarkBytes     int64
	LowWatermarkBytes      int64
	CriticalFreeBytes      int64
	ArtifactStoreReachable bool
	ObservedBy             string
}

type CapacityPolicy struct {
	WorkerPoolID             uuid.UUID
	Revision                 string
	WorkerHighWatermarkBytes int64
	WorkerLowWatermarkBytes  int64
	WorkerCriticalFreeBytes  int64
	PoolHighWatermarkBytes   int64
	PoolLowWatermarkBytes    int64
	ObservationMaxAge        time.Duration
	ConfiguredBy             string
}

type CapacityPolicyResult struct {
	WorkerPoolID uuid.UUID
	Revision     string
	Replayed     bool
}

type WorkerIdentityRequest struct {
	NodeIdentity  string
	WorkerPoolID  uuid.UUID
	KubernetesUID string
	Namespace     string
	Name          string
}

type WorkerIdentity struct {
	WorkerID     uuid.UUID
	WorkerPoolID uuid.UUID
	WorkerEpoch  int64
	NodeIdentity string
}

type CapacityResult struct {
	WorkerPoolID            uuid.UUID
	Replayed                bool
	WorkerState             CapacityState
	PoolState               CapacityState
	WorkerAssignmentAllowed bool
	PoolReadinessAllowed    bool
	PoolAssignmentAllowed   bool
}

type ReadinessRequest struct {
	CycleID                    uuid.UUID
	WorkerID                   uuid.UUID
	WorkerPoolID               uuid.UUID
	WorkerEpoch                int64
	NodeIdentity               string
	ExecutionProfileRevisionID uuid.UUID
	InferenceBackendRevision   string
	RequestedBy                string
	Deadline                   time.Time
}

type ReadinessEvidence struct {
	CycleID        uuid.UUID
	Check          ReadinessCheck
	Passed         bool
	EvidenceDigest []byte
	ObservedBy     string
}

type ReadinessResult struct {
	CycleID            uuid.UUID
	Replayed           bool
	State              ReadinessState
	NextCheck          ReadinessCheck
	WorkerLifecycle    string
	WorkerReachability string
}

type ReadinessWork struct {
	Available                  bool
	CycleID                    uuid.UUID
	Check                      ReadinessCheck
	WorkerID                   uuid.UUID
	WorkerPoolID               uuid.UUID
	WorkerEpoch                int64
	NodeIdentity               string
	ExecutionProfileRevisionID uuid.UUID
	InferenceBackendRevision   string
	Deadline                   time.Time
}

type DrainRequest struct {
	OperationID   uuid.UUID
	WorkerID      uuid.UUID
	ExpectedEpoch int64
	Reason        string
	Deadline      time.Time
	RequestedBy   string
}

type DrainResult struct {
	OperationID        uuid.UUID
	Replayed           bool
	State              DrainState
	WorkerID           uuid.UUID
	WorkerEpoch        int64
	WorkerLifecycle    string
	WorkerReachability string
	Deadline           time.Time
}

type MutationAuthorizationRequest struct {
	RequestUID              string
	ActorIdentity           string
	ResourceKind            ProtectedResourceKind
	Operation               MutationOperation
	KubernetesUID           string
	Namespace               string
	Name                    string
	WorkerPoolID            uuid.UUID
	WorkerID                uuid.UUID
	WorkerEpoch             int64
	WorkerInstanceID        uuid.UUID
	WorkerInstanceEpoch     int64
	ResidencyPlanRevisionID uuid.UUID
	WorkerBundleID          uuid.UUID
	WorkerMemberID          uuid.UUID
	DrainOperationIDs       []uuid.UUID
	RequestDigest           []byte
}

type MutationAuthorizationResult struct {
	RequestUID string
	Replayed   bool
	Authorized bool
}

type RetirementAuthorizationRequest struct {
	ResourceKind      ProtectedResourceKind
	KubernetesUID     string
	Namespace         string
	Name              string
	WorkerPoolID      uuid.UUID
	WorkerID          uuid.UUID
	WorkerEpoch       int64
	DrainOperationIDs []uuid.UUID
}

type RetirementCompletionResult struct {
	Replayed    bool
	CompletedAt time.Time
}

type RetirementCompletionRequest struct {
	RetirementAuthorizationRequest
	ObservedBy string
}

type Service struct {
	pool         *pgxpool.Pool
	registryPool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool, registryPools ...*pgxpool.Pool) (*Service, error) {
	if pool == nil {
		return nil, errors.New("fleet database pool is required")
	}
	registryPool := pool
	if len(registryPools) > 1 {
		return nil, errors.New("at most one Worker Registry database pool is allowed")
	}
	if len(registryPools) == 1 {
		if registryPools[0] == nil {
			return nil, errors.New("Worker Registry database pool is required")
		}
		registryPool = registryPools[0]
	}
	return &Service{pool: pool, registryPool: registryPool}, nil
}

func (service *Service) ResolveWorkerIdentity(
	ctx context.Context,
	request WorkerIdentityRequest,
) (WorkerIdentity, error) {
	if service == nil || service.pool == nil {
		return WorkerIdentity{}, errors.New("fleet service is not configured")
	}
	if !validText(request.NodeIdentity, 500) || request.WorkerPoolID == uuid.Nil ||
		!validText(request.KubernetesUID, 128) || !validText(request.Namespace, 253) ||
		!validText(request.Name, 253) {
		return WorkerIdentity{}, &Failure{
			Code: FailureInvalid, Message: "Fleet Worker identity request is invalid",
		}
	}
	var identity WorkerIdentity
	err := service.pool.QueryRow(ctx, `
		SELECT worker_id, worker_pool_id, worker_epoch, node_identity
		FROM vela_resolve_worker_identity($1, $2, $3, $4, $5)
	`, request.NodeIdentity, request.WorkerPoolID, request.KubernetesUID,
		request.Namespace, request.Name).Scan(
		&identity.WorkerID,
		&identity.WorkerPoolID,
		&identity.WorkerEpoch,
		&identity.NodeIdentity,
	)
	if err != nil {
		return WorkerIdentity{}, mapDatabaseError("resolve Fleet Worker identity", err)
	}
	return identity, nil
}

func (service *Service) ObserveCapacity(
	ctx context.Context,
	observation CapacityObservation,
) (CapacityResult, error) {
	if service == nil || service.pool == nil {
		return CapacityResult{}, errors.New("fleet service is not configured")
	}
	if err := validateCapacityObservation(observation); err != nil {
		return CapacityResult{}, &Failure{Code: FailureInvalid, Message: err.Error()}
	}
	var result CapacityResult
	err := service.pool.QueryRow(ctx, `
		SELECT worker_pool_id, replayed, worker_state, pool_state,
			worker_assignment_allowed, pool_readiness_allowed, pool_assignment_allowed
			FROM vela_observe_worker_capacity(
					$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
				)
			`, observation.WorkerID, observation.WorkerPoolID, observation.WorkerEpoch,
		observation.Sequence, observation.ObservedAt, observation.WatermarkState,
		observation.TotalBytes, observation.FreeBytes,
		observation.HighWatermarkBytes, observation.LowWatermarkBytes,
		observation.CriticalFreeBytes, observation.ArtifactStoreReachable,
		observation.ObservedBy,
	).Scan(
		&result.WorkerPoolID, &result.Replayed, &result.WorkerState, &result.PoolState,
		&result.WorkerAssignmentAllowed, &result.PoolReadinessAllowed,
		&result.PoolAssignmentAllowed,
	)
	if err != nil {
		return CapacityResult{}, mapDatabaseError("record Fleet capacity observation", err)
	}
	return result, nil
}

func (service *Service) ConfigureCapacityPolicy(
	ctx context.Context,
	policy CapacityPolicy,
) (CapacityPolicyResult, error) {
	if service == nil || service.pool == nil {
		return CapacityPolicyResult{}, errors.New("fleet service is not configured")
	}
	if err := validateCapacityPolicy(policy); err != nil {
		return CapacityPolicyResult{}, &Failure{Code: FailureInvalid, Message: err.Error()}
	}
	var result CapacityPolicyResult
	err := service.pool.QueryRow(ctx, `
		SELECT worker_pool_id, revision, replayed
		FROM vela_configure_worker_pool_capacity(
			$1, $2, $3, $4, $5, $6, $7, $8, $9
		)
	`, policy.WorkerPoolID, policy.Revision, policy.WorkerHighWatermarkBytes,
		policy.WorkerLowWatermarkBytes, policy.WorkerCriticalFreeBytes,
		policy.PoolHighWatermarkBytes, policy.PoolLowWatermarkBytes,
		int64(policy.ObservationMaxAge/time.Second), policy.ConfiguredBy,
	).Scan(&result.WorkerPoolID, &result.Revision, &result.Replayed)
	if err != nil {
		return CapacityPolicyResult{}, mapDatabaseError("configure Fleet capacity policy", err)
	}
	return result, nil
}

func (service *Service) GetPoolCapacity(
	ctx context.Context,
	workerPoolID uuid.UUID,
) (CapacityResult, error) {
	if service == nil || service.pool == nil {
		return CapacityResult{}, errors.New("fleet service is not configured")
	}
	if workerPoolID == uuid.Nil {
		return CapacityResult{}, &Failure{Code: FailureInvalid, Message: "Worker pool id is required"}
	}
	result := CapacityResult{WorkerPoolID: workerPoolID}
	err := service.pool.QueryRow(ctx, `
		SELECT worker_pool_id, pool_state, pool_assignment_allowed
		FROM vela_get_worker_pool_capacity($1)
	`, workerPoolID).Scan(
		&result.WorkerPoolID, &result.PoolState, &result.PoolAssignmentAllowed,
	)
	if err != nil {
		return CapacityResult{}, mapDatabaseError("get Worker pool capacity", err)
	}
	return result, nil
}

func (service *Service) BeginReadiness(
	ctx context.Context,
	request ReadinessRequest,
) (ReadinessResult, error) {
	if service == nil || service.pool == nil {
		return ReadinessResult{}, errors.New("fleet service is not configured")
	}
	if err := validateReadinessRequest(request); err != nil {
		return ReadinessResult{}, &Failure{Code: FailureInvalid, Message: err.Error()}
	}
	result, err := scanReadinessResult(service.pool.QueryRow(ctx, `
		SELECT cycle_id, replayed, readiness_state, next_check,
			worker_lifecycle_state, worker_reachability_condition
		FROM vela_begin_worker_readiness($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, request.CycleID, request.WorkerID, request.WorkerPoolID, request.WorkerEpoch,
		request.NodeIdentity, request.ExecutionProfileRevisionID,
		request.InferenceBackendRevision, request.RequestedBy, request.Deadline,
	), true)
	if err != nil {
		return ReadinessResult{}, mapDatabaseError("begin Worker readiness", err)
	}
	return result, nil
}

func (service *Service) ReportReadiness(
	ctx context.Context,
	evidence ReadinessEvidence,
) (ReadinessResult, error) {
	if service == nil || service.pool == nil {
		return ReadinessResult{}, errors.New("fleet service is not configured")
	}
	if err := validateReadinessEvidence(evidence); err != nil {
		return ReadinessResult{}, &Failure{Code: FailureInvalid, Message: err.Error()}
	}
	result, err := scanReadinessResult(service.pool.QueryRow(ctx, `
		SELECT cycle_id, replayed, readiness_state, next_check,
			worker_lifecycle_state, worker_reachability_condition
		FROM vela_report_worker_readiness($1, $2, $3, $4, $5)
	`, evidence.CycleID, evidence.Check, evidence.Passed,
		evidence.EvidenceDigest, evidence.ObservedBy,
	), true)
	if err != nil {
		return ReadinessResult{}, mapDatabaseError("report Worker readiness", err)
	}
	return result, nil
}

func (service *Service) GetReadiness(
	ctx context.Context,
	cycleID uuid.UUID,
) (ReadinessResult, error) {
	if service == nil || service.pool == nil {
		return ReadinessResult{}, errors.New("fleet service is not configured")
	}
	if cycleID == uuid.Nil {
		return ReadinessResult{}, &Failure{Code: FailureInvalid, Message: "Readiness cycle id is required"}
	}
	result, err := scanReadinessResult(service.pool.QueryRow(ctx, `
		SELECT cycle_id, readiness_state, next_check,
			worker_lifecycle_state, worker_reachability_condition
		FROM vela_get_worker_readiness($1)
	`, cycleID), false)
	if err != nil {
		return ReadinessResult{}, mapDatabaseError("get Worker readiness", err)
	}
	return result, nil
}

func (service *Service) GetWorkerReadinessWork(
	ctx context.Context,
	workerID uuid.UUID,
	workerEpoch int64,
) (ReadinessWork, error) {
	if service == nil || service.pool == nil {
		return ReadinessWork{}, errors.New("fleet service is not configured")
	}
	if workerID == uuid.Nil || workerEpoch <= 0 {
		return ReadinessWork{}, &Failure{
			Code: FailureInvalid, Message: "Worker readiness work identity is invalid",
		}
	}
	work := ReadinessWork{Available: true}
	err := service.pool.QueryRow(ctx, `
		SELECT cycle_id, next_check, worker_id, worker_pool_id, worker_epoch,
			node_identity, execution_profile_revision_id,
			inference_backend_revision, deadline_at
		FROM vela_get_worker_readiness_work($1, $2)
	`, workerID, workerEpoch).Scan(
		&work.CycleID, &work.Check, &work.WorkerID, &work.WorkerPoolID,
		&work.WorkerEpoch, &work.NodeIdentity, &work.ExecutionProfileRevisionID,
		&work.InferenceBackendRevision, &work.Deadline,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ReadinessWork{}, nil
	}
	if err != nil {
		return ReadinessWork{}, mapDatabaseError("get Worker readiness work", err)
	}
	return work, nil
}

func (service *Service) RequestDrain(
	ctx context.Context,
	request DrainRequest,
) (DrainResult, error) {
	if service == nil || service.pool == nil {
		return DrainResult{}, errors.New("fleet service is not configured")
	}
	if err := validateDrainRequest(request); err != nil {
		return DrainResult{}, &Failure{Code: FailureInvalid, Message: err.Error()}
	}
	result, err := scanDrainResult(service.pool.QueryRow(ctx, `
		SELECT operation_id, replayed, drain_state, worker_id, worker_epoch,
			worker_lifecycle_state, worker_reachability_condition, deadline_at
		FROM vela_request_worker_drain($1, $2, $3, $4, $5, $6)
	`, request.OperationID, request.WorkerID, request.ExpectedEpoch, request.Reason,
		request.Deadline, request.RequestedBy,
	), true)
	if err != nil {
		return DrainResult{}, mapDatabaseError("request Worker drain", err)
	}
	return result, nil
}

func (service *Service) GetDrain(
	ctx context.Context,
	operationID uuid.UUID,
) (DrainResult, error) {
	if service == nil || service.pool == nil {
		return DrainResult{}, errors.New("fleet service is not configured")
	}
	if operationID == uuid.Nil {
		return DrainResult{}, &Failure{Code: FailureInvalid, Message: "Drain operation id is required"}
	}
	result, err := scanDrainResult(service.pool.QueryRow(ctx, `
		SELECT operation_id, drain_state, worker_id, worker_epoch,
			worker_lifecycle_state, worker_reachability_condition, deadline_at
		FROM vela_get_worker_drain($1)
	`, operationID), false)
	if err != nil {
		return DrainResult{}, mapDatabaseError("get Worker drain", err)
	}
	return result, nil
}

func (service *Service) ReconcileDrain(
	ctx context.Context,
	operationID uuid.UUID,
	reconciledBy string,
) (DrainResult, error) {
	if service == nil || service.pool == nil {
		return DrainResult{}, errors.New("fleet service is not configured")
	}
	if operationID == uuid.Nil || !validText(reconciledBy, 500) {
		return DrainResult{}, &Failure{
			Code: FailureInvalid, Message: "Worker drain reconciliation request is invalid",
		}
	}
	result, err := scanDrainResult(service.pool.QueryRow(ctx, `
		SELECT operation_id, replayed, drain_state, worker_id, worker_epoch,
			worker_lifecycle_state, worker_reachability_condition, deadline_at
		FROM vela_reconcile_worker_drain($1, $2)
	`, operationID, reconciledBy), true)
	if err != nil {
		return DrainResult{}, mapDatabaseError("reconcile Worker drain", err)
	}
	return result, nil
}

func (service *Service) AuthorizeMutation(
	ctx context.Context,
	request MutationAuthorizationRequest,
) (MutationAuthorizationResult, error) {
	if service == nil || service.pool == nil {
		return MutationAuthorizationResult{}, errors.New("fleet service is not configured")
	}
	if err := validateMutationAuthorizationRequest(request); err != nil {
		return MutationAuthorizationResult{}, &Failure{Code: FailureInvalid, Message: err.Error()}
	}
	var result MutationAuthorizationResult
	if request.WorkerInstanceID != uuid.Nil {
		if service.registryPool == nil {
			return MutationAuthorizationResult{}, errors.New("Worker Registry service is not configured")
		}
		err := service.registryPool.QueryRow(ctx, `
			SELECT request_uid, replayed, authorized
			FROM vela_authorize_worker_instance_pod_mutation(
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
			)
		`, request.RequestUID, request.ActorIdentity, request.Operation,
			request.KubernetesUID, request.Namespace, request.Name,
			request.WorkerInstanceID, request.WorkerInstanceEpoch,
			request.ResidencyPlanRevisionID, request.WorkerBundleID,
			request.WorkerMemberID, request.RequestDigest,
		).Scan(&result.RequestUID, &result.Replayed, &result.Authorized)
		if err != nil {
			return MutationAuthorizationResult{}, mapDatabaseError(
				"authorize WorkerInstance Pod mutation", err,
			)
		}
		return result, nil
	}
	err := service.pool.QueryRow(ctx, `
		SELECT request_uid, replayed, authorized
		FROM vela_authorize_fleet_mutation(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		)
	`, request.RequestUID, request.ActorIdentity, request.ResourceKind, request.Operation,
		request.KubernetesUID, request.Namespace, request.Name, request.WorkerPoolID,
		nullableMutationWorkerID(request), nullableMutationWorkerEpoch(request),
		request.DrainOperationIDs, request.RequestDigest,
	).Scan(&result.RequestUID, &result.Replayed, &result.Authorized)
	if err != nil {
		return MutationAuthorizationResult{}, mapDatabaseError("authorize Fleet mutation", err)
	}
	return result, nil
}

func (service *Service) HasRetirementAuthorization(
	ctx context.Context,
	request RetirementAuthorizationRequest,
) (bool, error) {
	if service == nil || service.pool == nil {
		return false, errors.New("fleet service is not configured")
	}
	if err := validateRetirementAuthorizationRequest(request); err != nil {
		return false, &Failure{Code: FailureInvalid, Message: err.Error()}
	}
	var authorized bool
	err := service.pool.QueryRow(ctx, `
		SELECT vela_has_fleet_retirement_authorization(
			$1, $2, $3, $4, $5, $6, $7, $8
		)
	`, request.ResourceKind, request.KubernetesUID, request.Namespace, request.Name,
		request.WorkerPoolID, nullableRetirementWorkerID(request),
		nullableRetirementWorkerEpoch(request), request.DrainOperationIDs,
	).Scan(&authorized)
	if err != nil {
		return false, mapDatabaseError("check Fleet retirement authorization", err)
	}
	return authorized, nil
}

func (service *Service) RecordRetirementCompletion(
	ctx context.Context,
	request RetirementCompletionRequest,
) (RetirementCompletionResult, error) {
	if service == nil || service.pool == nil {
		return RetirementCompletionResult{}, errors.New("fleet service is not configured")
	}
	if err := validateRetirementAuthorizationRequest(request.RetirementAuthorizationRequest); err != nil ||
		!validText(request.ObservedBy, 500) {
		return RetirementCompletionResult{}, &Failure{
			Code: FailureInvalid, Message: "fleet retirement completion request is invalid",
		}
	}
	var result RetirementCompletionResult
	err := service.pool.QueryRow(ctx, `
		SELECT replayed, recorded_at
		FROM vela_record_fleet_retirement_completion(
			$1, $2, $3, $4, $5, $6, $7, $8, $9
		)
	`, request.ResourceKind, request.KubernetesUID, request.Namespace, request.Name,
		request.WorkerPoolID, nullableRetirementWorkerID(request.RetirementAuthorizationRequest),
		nullableRetirementWorkerEpoch(request.RetirementAuthorizationRequest),
		request.DrainOperationIDs, request.ObservedBy,
	).Scan(&result.Replayed, &result.CompletedAt)
	if err != nil {
		return RetirementCompletionResult{}, mapDatabaseError("record Fleet retirement completion", err)
	}
	return result, nil
}

func (service *Service) HasRetirementCompletion(
	ctx context.Context,
	request RetirementAuthorizationRequest,
) (bool, error) {
	if service == nil || service.pool == nil {
		return false, errors.New("fleet service is not configured")
	}
	if err := validateRetirementAuthorizationRequest(request); err != nil {
		return false, &Failure{Code: FailureInvalid, Message: err.Error()}
	}
	var completed bool
	err := service.pool.QueryRow(ctx, `
		SELECT vela_has_fleet_retirement_completion(
			$1, $2, $3, $4, $5, $6, $7, $8
		)
	`, request.ResourceKind, request.KubernetesUID, request.Namespace, request.Name,
		request.WorkerPoolID, nullableRetirementWorkerID(request),
		nullableRetirementWorkerEpoch(request), request.DrainOperationIDs,
	).Scan(&completed)
	if err != nil {
		return false, mapDatabaseError("check Fleet retirement completion", err)
	}
	return completed, nil
}

func validateCapacityObservation(observation CapacityObservation) error {
	if observation.WorkerID == uuid.Nil || observation.WorkerPoolID == uuid.Nil ||
		observation.WorkerEpoch <= 0 || observation.Sequence <= 0 ||
		observation.ObservedAt.IsZero() ||
		!validScratchWatermarkState(observation.WatermarkState) ||
		observation.TotalBytes <= 0 || observation.FreeBytes < 0 ||
		observation.FreeBytes > observation.TotalBytes ||
		observation.HighWatermarkBytes <= 0 ||
		observation.HighWatermarkBytes >= observation.TotalBytes ||
		observation.LowWatermarkBytes < 0 ||
		observation.LowWatermarkBytes >= observation.HighWatermarkBytes ||
		observation.CriticalFreeBytes < 0 ||
		observation.CriticalFreeBytes >= observation.TotalBytes ||
		!validText(observation.ObservedBy, 500) {
		return errors.New("fleet capacity observation is invalid")
	}
	return nil
}

func validScratchWatermarkState(state ScratchWatermarkState) bool {
	switch state {
	case ScratchWatermarkNormal, ScratchWatermarkPressured, ScratchWatermarkCritical:
		return true
	default:
		return false
	}
}

func validateCapacityPolicy(policy CapacityPolicy) error {
	if policy.WorkerPoolID == uuid.Nil || len(policy.Revision) != 64 ||
		strings.Trim(policy.Revision, "0123456789abcdef") != "" ||
		policy.WorkerHighWatermarkBytes <= 0 || policy.WorkerLowWatermarkBytes < 0 ||
		policy.WorkerLowWatermarkBytes >= policy.WorkerHighWatermarkBytes ||
		policy.WorkerCriticalFreeBytes < 0 || policy.PoolHighWatermarkBytes <= 0 ||
		policy.PoolLowWatermarkBytes < 0 ||
		policy.PoolLowWatermarkBytes >= policy.PoolHighWatermarkBytes ||
		policy.ObservationMaxAge < 10*time.Second || policy.ObservationMaxAge > 10*time.Minute ||
		policy.ObservationMaxAge%time.Second != 0 || !validText(policy.ConfiguredBy, 500) {
		return errors.New("fleet capacity policy is invalid")
	}
	return nil
}

func validateReadinessRequest(request ReadinessRequest) error {
	if request.CycleID == uuid.Nil || request.WorkerID == uuid.Nil ||
		request.WorkerPoolID == uuid.Nil || request.WorkerEpoch <= 0 ||
		request.ExecutionProfileRevisionID == uuid.Nil || request.Deadline.IsZero() ||
		!validText(request.NodeIdentity, 500) ||
		!validText(request.InferenceBackendRevision, 200) ||
		!validText(request.RequestedBy, 500) {
		return errors.New("worker readiness request is invalid")
	}
	return nil
}

func validateReadinessEvidence(evidence ReadinessEvidence) error {
	if evidence.CycleID == uuid.Nil || !validReadinessCheck(evidence.Check) ||
		len(evidence.EvidenceDigest) != 32 || !validText(evidence.ObservedBy, 500) {
		return errors.New("worker readiness evidence is invalid")
	}
	return nil
}

func validateDrainRequest(request DrainRequest) error {
	if request.OperationID == uuid.Nil || request.WorkerID == uuid.Nil ||
		request.ExpectedEpoch <= 0 || request.Deadline.IsZero() ||
		!validText(request.Reason, 1000) || !validText(request.RequestedBy, 500) {
		return errors.New("worker drain request is invalid")
	}
	return nil
}

func validateMutationAuthorizationRequest(request MutationAuthorizationRequest) error {
	if !validText(request.RequestUID, 200) || !validText(request.ActorIdentity, 500) ||
		!validProtectedResourceKind(request.ResourceKind) ||
		!validMutationOperation(request.Operation) ||
		!validText(request.KubernetesUID, 200) || !validText(request.Namespace, 253) ||
		!validText(request.Name, 253) || len(request.DrainOperationIDs) > 4096 ||
		len(request.RequestDigest) != 32 {
		return errors.New("fleet mutation authorization request is invalid")
	}
	for _, operationID := range request.DrainOperationIDs {
		if operationID == uuid.Nil {
			return errors.New("fleet mutation authorization request is invalid")
		}
	}
	workerInstanceMutation := request.WorkerInstanceID != uuid.Nil || request.WorkerInstanceEpoch != 0 ||
		request.ResidencyPlanRevisionID != uuid.Nil || request.WorkerBundleID != uuid.Nil ||
		request.WorkerMemberID != uuid.Nil
	if workerInstanceMutation {
		if request.ResourceKind != ProtectedPod || request.WorkerInstanceID == uuid.Nil ||
			request.WorkerInstanceEpoch <= 0 || request.ResidencyPlanRevisionID == uuid.Nil ||
			request.WorkerBundleID == uuid.Nil || request.WorkerMemberID == uuid.Nil ||
			request.WorkerPoolID != uuid.Nil || request.WorkerID != uuid.Nil || request.WorkerEpoch != 0 ||
			len(request.DrainOperationIDs) != 0 {
			return errors.New("fleet mutation authorization request is invalid")
		}
		return nil
	}
	if request.WorkerPoolID == uuid.Nil || len(request.DrainOperationIDs) == 0 {
		return errors.New("fleet mutation authorization request is invalid")
	}
	if request.ResourceKind == ProtectedPod {
		if request.WorkerID == uuid.Nil || request.WorkerEpoch <= 0 ||
			len(request.DrainOperationIDs) != 1 {
			return errors.New("fleet mutation authorization request is invalid")
		}
	} else if request.WorkerID != uuid.Nil || request.WorkerEpoch != 0 {
		return errors.New("fleet mutation authorization request is invalid")
	}
	return nil
}

func validateRetirementAuthorizationRequest(request RetirementAuthorizationRequest) error {
	if !validProtectedResourceKind(request.ResourceKind) ||
		!validText(request.KubernetesUID, 200) || !validText(request.Namespace, 253) ||
		!validText(request.Name, 253) || request.WorkerPoolID == uuid.Nil ||
		len(request.DrainOperationIDs) == 0 || len(request.DrainOperationIDs) > 4096 {
		return errors.New("fleet retirement authorization request is invalid")
	}
	seenDrainOperations := make(map[uuid.UUID]struct{}, len(request.DrainOperationIDs))
	for _, operationID := range request.DrainOperationIDs {
		if operationID == uuid.Nil {
			return errors.New("fleet retirement authorization request is invalid")
		}
		if _, exists := seenDrainOperations[operationID]; exists {
			return errors.New("fleet retirement authorization request is invalid")
		}
		seenDrainOperations[operationID] = struct{}{}
	}
	if request.ResourceKind == ProtectedPod {
		if request.WorkerID == uuid.Nil || request.WorkerEpoch <= 0 ||
			len(request.DrainOperationIDs) != 1 {
			return errors.New("fleet retirement authorization request is invalid")
		}
	} else if request.WorkerID != uuid.Nil || request.WorkerEpoch != 0 {
		return errors.New("fleet retirement authorization request is invalid")
	}
	return nil
}

func validProtectedResourceKind(kind ProtectedResourceKind) bool {
	return kind == ProtectedPod || kind == ProtectedDaemonSet || kind == ProtectedWorkerPool
}

func validMutationOperation(operation MutationOperation) bool {
	switch operation {
	case MutationDelete, MutationPatchSelector, MutationPatchImage, MutationRemoveFinalizer:
		return true
	default:
		return false
	}
}

func nullableMutationWorkerID(request MutationAuthorizationRequest) any {
	if request.WorkerID == uuid.Nil {
		return nil
	}
	return request.WorkerID
}

func nullableMutationWorkerEpoch(request MutationAuthorizationRequest) any {
	if request.WorkerEpoch == 0 {
		return nil
	}
	return request.WorkerEpoch
}

func nullableRetirementWorkerID(request RetirementAuthorizationRequest) any {
	if request.WorkerID == uuid.Nil {
		return nil
	}
	return request.WorkerID
}

func nullableRetirementWorkerEpoch(request RetirementAuthorizationRequest) any {
	if request.WorkerEpoch == 0 {
		return nil
	}
	return request.WorkerEpoch
}

func validReadinessCheck(check ReadinessCheck) bool {
	switch check {
	case ReadinessIdentity, ReadinessDevice, ReadinessInferenceBackend,
		ReadinessModelWarmup, ReadinessCanary:
		return true
	default:
		return false
	}
}

func scanReadinessResult(row pgx.Row, includesReplay bool) (ReadinessResult, error) {
	var result ReadinessResult
	var nextCheck *string
	var err error
	if includesReplay {
		err = row.Scan(
			&result.CycleID, &result.Replayed, &result.State, &nextCheck,
			&result.WorkerLifecycle, &result.WorkerReachability,
		)
	} else {
		err = row.Scan(
			&result.CycleID, &result.State, &nextCheck,
			&result.WorkerLifecycle, &result.WorkerReachability,
		)
	}
	if err != nil {
		return ReadinessResult{}, err
	}
	if nextCheck != nil {
		result.NextCheck = ReadinessCheck(*nextCheck)
	}
	return result, nil
}

func scanDrainResult(row pgx.Row, includesReplay bool) (DrainResult, error) {
	var result DrainResult
	var err error
	if includesReplay {
		err = row.Scan(
			&result.OperationID, &result.Replayed, &result.State, &result.WorkerID,
			&result.WorkerEpoch, &result.WorkerLifecycle, &result.WorkerReachability,
			&result.Deadline,
		)
	} else {
		err = row.Scan(
			&result.OperationID, &result.State, &result.WorkerID, &result.WorkerEpoch,
			&result.WorkerLifecycle, &result.WorkerReachability, &result.Deadline,
		)
	}
	if err != nil {
		return DrainResult{}, err
	}
	return result, nil
}

func mapDatabaseError(operation string, err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return &Failure{Code: FailureNotFound, Message: operation + " target does not exist"}
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		switch postgresError.Code {
		case "22023", "23502", "23514":
			return &Failure{Code: FailureInvalid, Message: postgresError.Message}
		case "P0002":
			return &Failure{Code: FailureNotFound, Message: postgresError.Message}
		case "55000", "23505", "23503":
			return &Failure{Code: FailureConflict, Message: postgresError.Message}
		}
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func validText(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, '\x00')
}
