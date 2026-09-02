package stageworkercontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

type PostgresWorkerEvidenceBackend struct {
	pool *pgxpool.Pool
}

func NewPostgresWorkerEvidenceBackend(pool *pgxpool.Pool) (*PostgresWorkerEvidenceBackend, error) {
	if pool == nil {
		return nil, errors.New("stage worker evidence database pool is required")
	}
	return &PostgresWorkerEvidenceBackend{pool: pool}, nil
}

func (backend *PostgresWorkerEvidenceBackend) RegisterWorkerEvidence(
	ctx context.Context,
	command CommandContext,
	request *velav1.RegisterWorkerEvidenceRequest,
) (ReadinessResult, error) {
	if backend == nil || backend.pool == nil || ctx == nil {
		return ReadinessResult{}, errors.New("PostgreSQL Stage Worker evidence backend is not configured")
	}
	identity := request.GetRuntimeIdentity()
	workerID := parseUUID(identity.GetWorkerInstanceId())
	residencyID := parseUUID(identity.GetModelResidencyId())
	profileID := parseUUID(identity.GetStageProfileRevisionId())
	memberID := parseUUID(identity.GetWorkerMemberId())
	if command.CommandID == uuid.Nil || command.ControlSessionEpoch <= 0 ||
		strings.TrimSpace(command.Identity.SPIFFEID) == "" || identity == nil ||
		workerID == uuid.Nil || residencyID == uuid.Nil || profileID == uuid.Nil ||
		memberID == uuid.Nil || identity.GetWorkerInstanceEpoch() <= 0 ||
		identity.GetModelRuntimeEpoch() <= 0 || identity.GetWorkerMemberEpoch() <= 0 ||
		identity.GetRuntimeIdentity() == "" || request.GetCapacityObservationSequence() <= 0 ||
		len(identity.GetDeviceSetDigest()) != sha256.Size ||
		len(identity.GetMembershipDigest()) != sha256.Size ||
		len(request.GetReadinessEvidence()) == 0 || len(request.GetDevices()) == 0 ||
		len(request.GetDevices()) > 64 || len(request.GetMembers()) != 1 {
		return ReadinessResult{}, errors.New("stage worker registration evidence is incomplete")
	}
	devices, err := registrationDevices(request.GetDevices())
	if err != nil {
		return ReadinessResult{}, err
	}
	members, err := registrationMembers(request.GetMembers(), identity.GetModelRuntimeEpoch())
	if err != nil {
		return ReadinessResult{}, err
	}
	spiffeDigest := sha256.Sum256([]byte(command.Identity.SPIFFEID))
	readinessDigest := sha256.Sum256(request.GetReadinessEvidence())
	payload, err := json.Marshal(map[string]any{
		"schema_version":                1,
		"worker_instance_id":            workerID,
		"worker_instance_epoch":         identity.GetWorkerInstanceEpoch(),
		"control_session_epoch":         command.ControlSessionEpoch,
		"device_set_digest":             hex.EncodeToString(identity.GetDeviceSetDigest()),
		"membership_digest":             hex.EncodeToString(identity.GetMembershipDigest()),
		"model_residency_id":            residencyID,
		"runtime_identity":              identity.GetRuntimeIdentity(),
		"model_runtime_epoch":           identity.GetModelRuntimeEpoch(),
		"stage_profile_revision_id":     profileID,
		"worker_member_id":              memberID,
		"worker_member_epoch":           identity.GetWorkerMemberEpoch(),
		"capacity_observation_sequence": request.GetCapacityObservationSequence(),
		"spiffe_id_digest":              hex.EncodeToString(spiffeDigest[:]),
		"readiness_evidence_digest":     hex.EncodeToString(readinessDigest[:]),
		"devices":                       devices,
		"members":                       members,
	})
	if err != nil {
		return ReadinessResult{}, fmt.Errorf("encode Stage Worker registration evidence: %w", err)
	}
	return backend.readRegistrationReadiness(ctx, payload)
}

func (backend *PostgresWorkerEvidenceBackend) ReportCapacityObservation(
	ctx context.Context,
	command CommandContext,
	request *velav1.ReportStageCapacityObservationRequest,
) (ReadinessResult, error) {
	if backend == nil || backend.pool == nil || ctx == nil {
		return ReadinessResult{}, errors.New("PostgreSQL Stage Worker evidence backend is not configured")
	}
	workerID := parseUUID(request.GetWorkerInstanceId())
	if command.CommandID == uuid.Nil || command.ControlSessionEpoch <= 0 ||
		strings.TrimSpace(command.Identity.SPIFFEID) == "" || request == nil ||
		workerID == uuid.Nil || request.GetWorkerInstanceEpoch() <= 0 ||
		request.GetObservationSequence() <= 0 || !validCapacityVector(request.GetCapacityVector()) ||
		!validTimestamp(request.GetObservedAt()) || !validTimestamp(request.GetExpiresAt()) ||
		!request.GetExpiresAt().AsTime().After(request.GetObservedAt().AsTime()) {
		return ReadinessResult{}, errors.New("stage worker capacity observation is incomplete")
	}
	spiffeDigest := sha256.Sum256([]byte(command.Identity.SPIFFEID))
	payload, err := json.Marshal(map[string]any{
		"schema_version":        1,
		"worker_instance_id":    workerID,
		"worker_instance_epoch": request.GetWorkerInstanceEpoch(),
		"control_session_epoch": command.ControlSessionEpoch,
		"observation_sequence":  request.GetObservationSequence(),
		"capacity_vector":       request.GetCapacityVector(),
		"observed_at":           request.GetObservedAt().AsTime().UTC().Format(timeFormat),
		"expires_at":            request.GetExpiresAt().AsTime().UTC().Format(timeFormat),
		"spiffe_id_digest":      hex.EncodeToString(spiffeDigest[:]),
	})
	if err != nil {
		return ReadinessResult{}, fmt.Errorf("encode Stage Worker capacity observation: %w", err)
	}
	return backend.readReadiness(ctx, "vela_verify_stage_capacity_observation", payload)
}

const timeFormat = "2006-01-02T15:04:05.999999999Z07:00"

func (backend *PostgresWorkerEvidenceBackend) readReadiness(
	ctx context.Context,
	functionName string,
	payload []byte,
) (ReadinessResult, error) {
	var result ReadinessResult
	query := fmt.Sprintf(
		"SELECT worker_instance_id, worker_instance_epoch, ready, reason FROM %s($1::jsonb)",
		functionName,
	)
	if err := backend.pool.QueryRow(ctx, query, payload).Scan(
		&result.WorkerInstanceID,
		&result.WorkerInstanceEpoch,
		&result.Ready,
		&result.Reason,
	); err != nil {
		return ReadinessResult{}, fmt.Errorf("verify durable Stage Worker evidence: %w", err)
	}
	if result.WorkerInstanceID == uuid.Nil || result.WorkerInstanceEpoch <= 0 ||
		strings.TrimSpace(result.Reason) == "" || len(result.Reason) > maxControlDetailBytes {
		return ReadinessResult{}, errors.New("durable Stage Worker readiness result is malformed")
	}
	return result, nil
}

func (backend *PostgresWorkerEvidenceBackend) readRegistrationReadiness(
	ctx context.Context,
	payload []byte,
) (ReadinessResult, error) {
	var result ReadinessResult
	var leaderMemberID string
	if err := backend.pool.QueryRow(ctx, `
		SELECT worker_instance_id, worker_instance_epoch, ready, reason,
		       COALESCE(barrier_generation, 0),
		       COALESCE(leader_worker_member_id::text, '')
		FROM vela_register_stage_worker_runtime($1::jsonb)
	`, payload).Scan(
		&result.WorkerInstanceID, &result.WorkerInstanceEpoch,
		&result.Ready, &result.Reason,
		&result.ModelRuntimeBarrierGeneration, &leaderMemberID,
	); err != nil {
		return ReadinessResult{}, fmt.Errorf("register durable ModelRuntime barrier: %w", err)
	}
	if leaderMemberID != "" {
		parsed, err := uuid.Parse(leaderMemberID)
		if err != nil {
			return ReadinessResult{}, errors.New("durable ModelRuntime barrier leader is malformed")
		}
		result.LeaderWorkerMemberID = parsed
	}
	if result.WorkerInstanceID == uuid.Nil || result.WorkerInstanceEpoch <= 0 ||
		strings.TrimSpace(result.Reason) == "" ||
		(result.Ready && (result.ModelRuntimeBarrierGeneration <= 0 ||
			result.LeaderWorkerMemberID == uuid.Nil)) {
		return ReadinessResult{}, errors.New("durable ModelRuntime barrier result is malformed")
	}
	return result, nil
}

func registrationDevices(
	values []*velav1.StageAuthorityDeviceEpoch,
) ([]map[string]any, error) {
	seen := make(map[uuid.UUID]struct{}, len(values))
	devices := make([]map[string]any, 0, len(values))
	for _, value := range values {
		id := parseUUID(value.GetDeviceId())
		if value == nil || id == uuid.Nil || value.GetDeviceEpoch() <= 0 {
			return nil, errors.New("stage worker registration device evidence is invalid")
		}
		if _, duplicated := seen[id]; duplicated {
			return nil, errors.New("stage worker registration device evidence is duplicated")
		}
		seen[id] = struct{}{}
		devices = append(devices, map[string]any{
			"device_id": id, "device_epoch": value.GetDeviceEpoch(),
		})
	}
	return devices, nil
}

func registrationMembers(
	values []*velav1.StageAuthorityMemberEpoch,
	modelRuntimeEpoch int64,
) ([]map[string]any, error) {
	seen := make(map[uuid.UUID]struct{}, len(values))
	members := make([]map[string]any, 0, len(values))
	for _, value := range values {
		id := parseUUID(value.GetWorkerMemberId())
		if value == nil || id == uuid.Nil || value.GetMemberEpoch() <= 0 ||
			value.GetModelRuntimeEpoch() != modelRuntimeEpoch {
			return nil, errors.New("stage worker registration member evidence is invalid")
		}
		if _, duplicated := seen[id]; duplicated {
			return nil, errors.New("stage worker registration member evidence is duplicated")
		}
		seen[id] = struct{}{}
		members = append(members, map[string]any{
			"worker_member_id": id, "member_epoch": value.GetMemberEpoch(),
			"model_runtime_epoch": value.GetModelRuntimeEpoch(),
		})
	}
	return members, nil
}

var _ WorkerEvidenceOperations = (*PostgresWorkerEvidenceBackend)(nil)
