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
	"github.com/jackc/pgx/v5"
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
		identity.GetRuntimeIdentity() == "" || request.GetCapacityObservationSequence() < 0 ||
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
	payloadFields := map[string]any{
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
	}
	var payload []byte
	stageCapacity := stageWorkerOwnsCapacity(
		request.GetCapacityObservationSequence(), identity.GetWorkerInstanceEpoch(),
	)
	durableSessionEpoch := int64(0)
	if stageCapacity {
		// Persist the stream epoch independently so a rejected operation cannot roll it back.
		payloadFields["stage_worker_control_reconnect"] = true
		payload, err = json.Marshal(payloadFields)
		if err != nil {
			return ReadinessResult{}, fmt.Errorf("encode Stage Worker control reconnect: %w", err)
		}
		durableSessionEpoch, err = ensureControlSession(
			ctx, backend.pool, payload, workerID, identity.GetWorkerInstanceEpoch(),
		)
		if err != nil {
			return ReadinessResult{}, err
		}
		delete(payloadFields, "stage_worker_control_reconnect")
		payloadFields["control_session_epoch"] = durableSessionEpoch
	}
	payload, err = json.Marshal(payloadFields)
	if err != nil {
		return ReadinessResult{}, fmt.Errorf("encode synchronized Stage Worker registration evidence: %w", err)
	}
	result, err := readRegistrationReadiness(ctx, backend.pool, payload)
	if stageCapacity {
		result.ControlSessionEpoch = durableSessionEpoch
	}
	return result, err
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
	payloadFields := map[string]any{
		"schema_version":        1,
		"worker_instance_id":    workerID,
		"worker_instance_epoch": request.GetWorkerInstanceEpoch(),
		"control_session_epoch": command.ControlSessionEpoch,
		"observation_sequence":  request.GetObservationSequence(),
		"capacity_vector":       request.GetCapacityVector(),
		"observed_at":           request.GetObservedAt().AsTime().UTC().Format(timeFormat),
		"expires_at":            request.GetExpiresAt().AsTime().UTC().Format(timeFormat),
		"spiffe_id_digest":      hex.EncodeToString(spiffeDigest[:]),
	}
	var payload []byte
	var err error
	stageCapacity := stageWorkerOwnsCapacity(
		request.GetObservationSequence(), request.GetWorkerInstanceEpoch(),
	)
	durableSessionEpoch := int64(0)
	if stageCapacity {
		// Persist the stream epoch independently so a rejected operation cannot roll it back.
		payloadFields["stage_worker_control_reconnect"] = true
		payload, err = json.Marshal(payloadFields)
		if err != nil {
			return ReadinessResult{}, fmt.Errorf("encode Stage Worker control reconnect: %w", err)
		}
		durableSessionEpoch, err = ensureControlSession(
			ctx, backend.pool, payload, workerID, request.GetWorkerInstanceEpoch(),
		)
		if err != nil {
			return ReadinessResult{}, err
		}
		delete(payloadFields, "stage_worker_control_reconnect")
		payloadFields["control_session_epoch"] = durableSessionEpoch
	}
	payload, err = json.Marshal(payloadFields)
	if err != nil {
		return ReadinessResult{}, fmt.Errorf("encode synchronized Stage Worker capacity observation: %w", err)
	}
	result, err := readReadiness(ctx, backend.pool, "vela_verify_stage_capacity_observation", payload)
	if !stageCapacity || err != nil {
		return result, err
	}
	var envelope struct {
		Reason                      string `json:"reason"`
		CapacityObservationSequence int64  `json:"capacity_observation_sequence"`
	}
	if json.Unmarshal([]byte(result.Reason), &envelope) != nil ||
		strings.TrimSpace(envelope.Reason) == "" || envelope.CapacityObservationSequence <= 0 {
		return ReadinessResult{}, errors.New("durable Stage Worker capacity result is malformed")
	}
	result.Reason = envelope.Reason
	result.ControlSessionEpoch = durableSessionEpoch
	result.CapacityObservationSequence = envelope.CapacityObservationSequence
	return result, nil
}

const timeFormat = "2006-01-02T15:04:05.999999999Z07:00"

type evidenceQueryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func ensureControlSession(
	ctx context.Context,
	database evidenceQueryRower,
	payload []byte,
	workerID uuid.UUID,
	workerEpoch int64,
) (int64, error) {
	var durableWorkerID uuid.UUID
	var durableWorkerEpoch, durableSessionEpoch int64
	if err := database.QueryRow(ctx, `
		SELECT worker_instance_id, worker_instance_epoch, COALESCE(barrier_generation, 0)
		FROM vela_register_stage_worker_runtime($1::jsonb)
	`, payload).Scan(
		&durableWorkerID, &durableWorkerEpoch, &durableSessionEpoch,
	); err != nil {
		return 0, fmt.Errorf("reconnect durable Stage Worker control session: %w", err)
	}
	if durableWorkerID != workerID || durableWorkerEpoch != workerEpoch || durableSessionEpoch <= 0 {
		return 0, errors.New("durable Stage Worker control session result is malformed")
	}
	return durableSessionEpoch, nil
}

func readReadiness(
	ctx context.Context,
	database evidenceQueryRower,
	functionName string,
	payload []byte,
) (ReadinessResult, error) {
	var result ReadinessResult
	query := fmt.Sprintf(
		"SELECT worker_instance_id, worker_instance_epoch, ready, reason FROM %s($1::jsonb)",
		functionName,
	)
	if err := database.QueryRow(ctx, query, payload).Scan(
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

func readRegistrationReadiness(
	ctx context.Context,
	database evidenceQueryRower,
	payload []byte,
) (ReadinessResult, error) {
	var result ReadinessResult
	var leaderMemberID string
	if err := database.QueryRow(ctx, `
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

func stageWorkerOwnsCapacity(sequence, workerEpoch int64) bool {
	if sequence == 0 {
		return true
	}
	if workerEpoch <= 0 || workerEpoch > int64(^uint64(0)>>33) {
		return false
	}
	base := workerEpoch << 32
	return sequence > base && sequence <= base+int64(^uint32(0))
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
