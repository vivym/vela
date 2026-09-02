package fleet

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MutationOperation string

const (
	MutationDelete          MutationOperation = "DELETE"
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

type MutationAuthorizationRequest struct {
	RequestUID              string
	ActorIdentity           string
	Operation               MutationOperation
	KubernetesUID           string
	Namespace               string
	Name                    string
	WorkerInstanceID        uuid.UUID
	WorkerInstanceEpoch     int64
	ResidencyPlanRevisionID uuid.UUID
	WorkerBundleID          uuid.UUID
	WorkerMemberID          uuid.UUID
	RequestDigest           []byte
}

type MutationAuthorizationResult struct {
	RequestUID string
	Replayed   bool
	Authorized bool
}

type Service struct {
	registryPool *pgxpool.Pool
}

func NewService(registryPool *pgxpool.Pool) (*Service, error) {
	if registryPool == nil {
		return nil, errors.New("worker Registry database pool is required")
	}
	return &Service{registryPool: registryPool}, nil
}

func (service *Service) AuthorizeMutation(
	ctx context.Context,
	request MutationAuthorizationRequest,
) (MutationAuthorizationResult, error) {
	if service == nil || service.registryPool == nil {
		return MutationAuthorizationResult{}, errors.New("fleet service is not configured")
	}
	if err := validateMutationAuthorizationRequest(request); err != nil {
		return MutationAuthorizationResult{}, &Failure{Code: FailureInvalid, Message: err.Error()}
	}
	var result MutationAuthorizationResult
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

func validateMutationAuthorizationRequest(request MutationAuthorizationRequest) error {
	if !validText(request.RequestUID, 200) || !validText(request.ActorIdentity, 500) ||
		(request.Operation != MutationDelete && request.Operation != MutationRemoveFinalizer) ||
		!validText(request.KubernetesUID, 200) || !validText(request.Namespace, 253) ||
		!validText(request.Name, 253) || request.WorkerInstanceID == uuid.Nil ||
		request.WorkerInstanceEpoch <= 0 || request.ResidencyPlanRevisionID == uuid.Nil ||
		request.WorkerBundleID == uuid.Nil || request.WorkerMemberID == uuid.Nil ||
		len(request.RequestDigest) != 32 {
		return errors.New("WorkerInstance Pod mutation authorization request is invalid")
	}
	return nil
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
