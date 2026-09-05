package stageworkercontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const renewalClockStep = time.Microsecond

type PostgresExecutionConfig struct {
	ActiveSigningKeyID string
	AuthorityTTL       time.Duration
	LocalDeadlineTTL   time.Duration
	MaxClockSkew       time.Duration
	Now                func() time.Time
}

type PostgresExecutionBackend struct {
	pool   *pgxpool.Pool
	signer *stageauthority.Signer
	config PostgresExecutionConfig
}

func NewPostgresExecutionBackend(
	pool *pgxpool.Pool,
	signer *stageauthority.Signer,
	config PostgresExecutionConfig,
) (*PostgresExecutionBackend, error) {
	config.ActiveSigningKeyID = strings.TrimSpace(config.ActiveSigningKeyID)
	if pool == nil || signer == nil || config.ActiveSigningKeyID == "" ||
		len(config.ActiveSigningKeyID) > 100 || config.AuthorityTTL <= 0 ||
		config.AuthorityTTL > 7*24*time.Hour || config.LocalDeadlineTTL <= 0 ||
		config.LocalDeadlineTTL > config.AuthorityTTL ||
		config.AuthorityTTL%renewalClockStep != 0 ||
		config.LocalDeadlineTTL%renewalClockStep != 0 || config.MaxClockSkew < 0 ||
		config.MaxClockSkew > time.Minute {
		return nil, errors.New("PostgreSQL Stage Worker execution configuration is invalid")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &PostgresExecutionBackend{pool: pool, signer: signer, config: config}, nil
}

func (backend *PostgresExecutionBackend) StartStage(
	ctx context.Context,
	command CommandContext,
	request *velav1.StartStageRequest,
	authorities VerifiedAuthorities,
) (CommandResult, error) {
	current, err := executionStageAuthority(ctx, command, request.GetAuthority(), authorities)
	if err != nil {
		return CommandResult{}, err
	}
	startedAt, err := validExecutionEventTime(
		request.GetStartedAt(), current.Authority, backend.now(), backend.config.MaxClockSkew,
	)
	if err != nil {
		return CommandResult{}, err
	}
	renewed, err := backend.renew(current.Authority, current.Authority.GetStageVersion()+1, startedAt)
	if err != nil {
		return CommandResult{}, err
	}
	payload, err := executionCommandPayload(command, current, renewed)
	if err != nil {
		return CommandResult{}, err
	}
	payload["command_kind"] = "START"
	payload["started_at"] = startedAt.Format(time.RFC3339Nano)
	return backend.execute(ctx, "vela_start_stage_worker_command", payload, current, renewed)
}

func (backend *PostgresExecutionBackend) HeartbeatStage(
	ctx context.Context,
	command CommandContext,
	request *velav1.HeartbeatStageRequest,
	authorities VerifiedAuthorities,
) (CommandResult, error) {
	current, err := executionStageAuthority(ctx, command, request.GetAuthority(), authorities)
	if err != nil {
		return CommandResult{}, err
	}
	observedAt, err := validExecutionEventTime(
		request.GetObservedAt(), current.Authority, backend.now(), backend.config.MaxClockSkew,
	)
	if err != nil {
		return CommandResult{}, err
	}
	if request.GetSequence() <= 0 || request.GetRuntimeState() ==
		velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED ||
		len(request.GetBoundedStatusJson()) == 0 ||
		len(request.GetBoundedStatusJson()) > maxBoundedStatusBytes ||
		!json.Valid(request.GetBoundedStatusJson()) ||
		(request.GetLocalReceiptId() == "") != (len(request.GetLocalReceiptDigest()) == 0) ||
		(len(request.GetLocalReceiptDigest()) != 0 &&
			len(request.GetLocalReceiptDigest()) != sha256.Size) {
		return CommandResult{}, errors.New("stage worker Heartbeat evidence is invalid")
	}
	renewed, err := backend.renew(
		current.Authority, current.Authority.GetStageVersion(), observedAt,
	)
	if err != nil {
		return CommandResult{}, err
	}
	payload, err := executionCommandPayload(command, current, renewed)
	if err != nil {
		return CommandResult{}, err
	}
	payload["command_kind"] = "HEARTBEAT"
	payload["sequence"] = request.GetSequence()
	payload["runtime_state"] = strings.TrimPrefix(
		request.GetRuntimeState().String(), "MODEL_RUNTIME_EXECUTION_STATE_",
	)
	payload["bounded_status"] = json.RawMessage(request.GetBoundedStatusJson())
	if request.GetLocalReceiptId() == "" {
		payload["local_receipt_id"] = nil
		payload["local_receipt_digest"] = nil
	} else {
		payload["local_receipt_id"] = request.GetLocalReceiptId()
		payload["local_receipt_digest"] = hex.EncodeToString(request.GetLocalReceiptDigest())
	}
	payload["observed_at"] = observedAt.Format(time.RFC3339Nano)
	return backend.execute(ctx, "vela_heartbeat_stage_worker_command", payload, current, renewed)
}

func (backend *PostgresExecutionBackend) renew(
	current *velav1.StageAuthority,
	stageVersion int64,
	eventAt time.Time,
) (*velav1.StageAuthority, error) {
	if backend == nil || backend.pool == nil || backend.signer == nil || backend.config.Now == nil {
		return nil, errors.New("PostgreSQL Stage Worker execution backend is not configured")
	}
	if current == nil || stageVersion < current.GetStageVersion() {
		return nil, errors.New("stage worker renewal Stage version is invalid")
	}
	issuedAt := eventAt.UTC().Truncate(renewalClockStep)
	minimumIssuedAt := current.GetIssuedAt().AsTime().UTC().
		Truncate(renewalClockStep).Add(renewalClockStep)
	if issuedAt.Before(minimumIssuedAt) {
		issuedAt = minimumIssuedAt
	}
	expiresAt := issuedAt.Add(backend.config.AuthorityTTL)
	minimumExpiresAt := current.GetExpiresAt().AsTime().UTC().
		Truncate(renewalClockStep).Add(renewalClockStep)
	if expiresAt.Before(minimumExpiresAt) {
		expiresAt = minimumExpiresAt
	}
	renewed := proto.Clone(current).(*velav1.StageAuthority)
	renewed.StageVersion = stageVersion
	renewed.SigningKeyId = backend.config.ActiveSigningKeyID
	renewed.IssuedAt = timestamppb.New(issuedAt)
	renewed.ExpiresAt = timestamppb.New(expiresAt)
	renewed.MonotonicValidFor = durationpb.New(backend.config.LocalDeadlineTTL)
	renewed.Signature = nil
	signed, err := backend.signer.Sign(renewed)
	if err != nil {
		return nil, fmt.Errorf("sign renewed StageAuthority: %w", err)
	}
	if err := stageauthority.ValidateRenewal(current, signed); err != nil {
		return nil, fmt.Errorf("validate renewed StageAuthority: %w", err)
	}
	return signed, nil
}

func (backend *PostgresExecutionBackend) execute(
	ctx context.Context,
	functionName string,
	payload map[string]any,
	current stageauthority.Verified,
	proposed *velav1.StageAuthority,
) (CommandResult, error) {
	if backend == nil || backend.pool == nil || ctx == nil {
		return CommandResult{}, errors.New("PostgreSQL Stage Worker execution backend is not configured")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return CommandResult{}, fmt.Errorf("encode Stage Worker execution command: %w", err)
	}
	var returnedWire []byte
	var stageVersion int64
	var replayed bool
	query := fmt.Sprintf(
		"SELECT renewed_authority, stage_version, replayed FROM %s($1::jsonb)",
		functionName,
	)
	err = backend.pool.QueryRow(ctx, query, encoded).Scan(
		&returnedWire, &stageVersion, &replayed,
	)
	if err != nil {
		if negative, mapped := mapExecutionCommandError(err); mapped {
			return negative, nil
		}
		return CommandResult{}, fmt.Errorf("persist Stage Worker execution command: %w", err)
	}
	returned := &velav1.StageAuthority{}
	if err := proto.Unmarshal(returnedWire, returned); err != nil {
		return CommandResult{}, errors.New("durable Stage Worker renewal is malformed")
	}
	if stageVersion != returned.GetStageVersion() || !proto.Equal(returned, proposed) {
		return CommandResult{}, errors.New("durable Stage Worker renewal changed identity")
	}
	if err := stageauthority.ValidateRenewal(current.Authority, returned); err != nil {
		return CommandResult{}, errors.New("durable Stage Worker renewal violates its execution contract")
	}
	digest, err := stageauthority.Digest(returned)
	if err != nil || digest == ([sha256.Size]byte{}) {
		return CommandResult{}, errors.New("durable Stage Worker renewal digest is invalid")
	}
	decision := velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED
	if replayed {
		decision = velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED
	}
	return CommandResult{Decision: decision, RenewedAuthority: returned}, nil
}

func executionStageAuthority(
	ctx context.Context,
	command CommandContext,
	requestAuthority *velav1.StageAuthority,
	authorities VerifiedAuthorities,
) (stageauthority.Verified, error) {
	if ctx == nil || command.CommandID == uuid.Nil || command.ControlSessionEpoch <= 0 ||
		strings.TrimSpace(command.Identity.SPIFFEID) == "" || authorities.Stage == nil ||
		authorities.Materialization != nil || authorities.Stage.Authority == nil ||
		requestAuthority == nil || !proto.Equal(requestAuthority, authorities.Stage.Authority) {
		return stageauthority.Verified{}, errors.New("stage worker execution authority is incomplete")
	}
	digest, err := stageauthority.Digest(authorities.Stage.Authority)
	if err != nil || digest != authorities.Stage.Digest {
		return stageauthority.Verified{}, errors.New("stage worker execution authority digest is inconsistent")
	}
	return *authorities.Stage, nil
}

func validExecutionEventTime(
	value *timestamppb.Timestamp,
	authority *velav1.StageAuthority,
	now time.Time,
	maxClockSkew time.Duration,
) (time.Time, error) {
	if value == nil || value.CheckValid() != nil || authority == nil ||
		authority.GetIssuedAt() == nil || authority.GetExpiresAt() == nil {
		return time.Time{}, errors.New("stage worker execution event time is invalid")
	}
	eventAt := value.AsTime().UTC()
	issuedAt := authority.GetIssuedAt().AsTime().UTC()
	if eventAt.Add(maxClockSkew).Before(issuedAt) ||
		!eventAt.Before(authority.GetExpiresAt().AsTime().UTC()) ||
		eventAt.After(now.UTC().Add(maxClockSkew)) {
		return time.Time{}, errors.New("stage worker execution event is outside active authority")
	}
	if eventAt.Before(issuedAt) {
		eventAt = issuedAt
	}
	return eventAt.Truncate(renewalClockStep), nil
}

func executionCommandPayload(
	command CommandContext,
	current stageauthority.Verified,
	renewed *velav1.StageAuthority,
) (map[string]any, error) {
	authority := current.Authority
	if authority == nil || renewed == nil ||
		authority.GetModelRuntimeBarrierGeneration() <= 0 {
		return nil, errors.New("stage worker execution authority binding is incomplete")
	}
	currentWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(authority)
	if err != nil {
		return nil, fmt.Errorf("encode current StageAuthority: %w", err)
	}
	renewedWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(renewed)
	if err != nil {
		return nil, fmt.Errorf("encode renewed StageAuthority: %w", err)
	}
	renewedDigest, err := stageauthority.Digest(renewed)
	if err != nil {
		return nil, err
	}
	leaseTokenDigest := sha256.Sum256(authority.GetLeaseToken())
	localDeadlineAt := renewed.GetIssuedAt().AsTime().UTC().Add(
		renewed.GetMonotonicValidFor().AsDuration(),
	)
	return map[string]any{
		"schema_version":                1,
		"command_id":                    command.CommandID,
		"current_authority_digest":      hex.EncodeToString(current.Digest[:]),
		"current_authority":             hex.EncodeToString(currentWire),
		"renewed_authority_digest":      hex.EncodeToString(renewedDigest[:]),
		"renewed_authority":             hex.EncodeToString(renewedWire),
		"attempt_id":                    authority.GetAttemptId(),
		"stage_run_id":                  authority.GetStageRunId(),
		"stage_attempt_id":              authority.GetStageAttemptId(),
		"stage_allocation_id":           authority.GetStageAllocationId(),
		"stage_lease_id":                authority.GetStageLeaseId(),
		"expected_attempt_fence":        authority.GetAttemptFence(),
		"expected_stage_fence":          authority.GetStageFence(),
		"expected_stage_version":        authority.GetStageVersion(),
		"renewed_stage_version":         renewed.GetStageVersion(),
		"worker_instance_id":            authority.GetWorkerInstanceId(),
		"worker_instance_epoch":         authority.GetWorkerInstanceEpoch(),
		"device_set_digest":             hex.EncodeToString(authority.GetDeviceSetDigest()),
		"membership_digest":             hex.EncodeToString(authority.GetMembershipDigest()),
		"model_residency_id":            authority.GetModelResidencyId(),
		"model_runtime_epoch":           authority.GetModelRuntimeBarrierGeneration(),
		"capacity_observation_sequence": authority.GetCapacityObservationSequence(),
		"capacity_vector":               authority.GetCapacityVector(),
		"lease_token_digest":            hex.EncodeToString(leaseTokenDigest[:]),
		"execution_nonce":               hex.EncodeToString(authority.GetExecutionNonce()),
		"control_session_epoch":         command.ControlSessionEpoch,
		"renewed_signing_key_id":        renewed.GetSigningKeyId(),
		"renewed_issued_at":             renewed.GetIssuedAt().AsTime().UTC().Format(time.RFC3339Nano),
		"renewed_expires_at":            renewed.GetExpiresAt().AsTime().UTC().Format(time.RFC3339Nano),
		"renewed_local_deadline_at":     localDeadlineAt.Format(time.RFC3339Nano),
	}, nil
}

func mapExecutionCommandError(err error) (CommandResult, bool) {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return CommandResult{}, false
	}
	if postgresError.Code == "40001" || strings.HasSuffix(postgresError.ConstraintName, "_stale") {
		return CommandResult{
			Decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE,
			Detail:   "Stage Worker execution authority changed",
		}, true
	}
	if postgresError.ConstraintName == "stage_worker_command_replay_mismatch" ||
		strings.HasSuffix(postgresError.ConstraintName, "_invalid") {
		return CommandResult{
			Decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED,
			Detail:   "Stage Worker execution command was rejected",
		}, true
	}
	return CommandResult{}, false
}

func (backend *PostgresExecutionBackend) now() time.Time {
	if backend == nil || backend.config.Now == nil {
		return time.Time{}
	}
	return backend.config.Now().UTC()
}
