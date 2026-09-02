package stageworkercontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

type PostgresReattachmentBackend struct {
	pool *pgxpool.Pool
}

func NewPostgresReattachmentBackend(pool *pgxpool.Pool) (*PostgresReattachmentBackend, error) {
	if pool == nil {
		return nil, errors.New("stage worker reattachment database pool is required")
	}
	return &PostgresReattachmentBackend{pool: pool}, nil
}

func (backend *PostgresReattachmentBackend) ReattachStage(
	ctx context.Context,
	command CommandContext,
	request *velav1.ReattachStageRequest,
	authorities VerifiedAuthorities,
) (CommandResult, error) {
	if backend == nil || backend.pool == nil || ctx == nil {
		return CommandResult{}, errors.New("PostgreSQL Stage Worker reattachment backend is not configured")
	}
	current, err := executionStageAuthority(ctx, command, request.GetAuthority(), authorities)
	if err != nil {
		return CommandResult{}, err
	}
	authority := current.Authority
	if authority.GetModelRuntimeBarrierGeneration() <= 0 {
		return CommandResult{}, errors.New("stage worker authority has no ModelRuntime barrier generation")
	}
	leaseTokenDigest := sha256.Sum256(authority.GetLeaseToken())
	spiffeDigest := sha256.Sum256([]byte(command.Identity.SPIFFEID))
	payload := map[string]any{
		"schema_version":                1,
		"command_kind":                  "REATTACH",
		"command_id":                    command.CommandID,
		"attempt_id":                    authority.GetAttemptId(),
		"stage_run_id":                  authority.GetStageRunId(),
		"stage_attempt_id":              authority.GetStageAttemptId(),
		"stage_allocation_id":           authority.GetStageAllocationId(),
		"stage_lease_id":                authority.GetStageLeaseId(),
		"expected_attempt_fence":        authority.GetAttemptFence(),
		"expected_stage_fence":          authority.GetStageFence(),
		"expected_stage_version":        authority.GetStageVersion(),
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
		"spiffe_id_digest":              hex.EncodeToString(spiffeDigest[:]),
		"current_authority_digest":      hex.EncodeToString(current.Digest[:]),
		"observed_runtime_state":        runtimeStateName(request.GetObservedRuntimeState()),
	}
	if request.GetLocalReceiptId() == "" {
		payload["local_receipt_id"] = nil
		payload["local_receipt_digest"] = nil
	} else {
		payload["local_receipt_id"] = request.GetLocalReceiptId()
		payload["local_receipt_digest"] = hex.EncodeToString(request.GetLocalReceiptDigest())
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return CommandResult{}, fmt.Errorf("encode Stage Worker Reattach command: %w", err)
	}
	var stageVersion int64
	var replayed bool
	err = backend.pool.QueryRow(ctx, `
		SELECT stage_version, replayed
		FROM vela_reattach_stage_worker_command($1::jsonb)
	`, encoded).Scan(&stageVersion, &replayed)
	if err != nil {
		if negative, mapped := mapOperationCommandError(err); mapped {
			return negative, nil
		}
		return CommandResult{}, fmt.Errorf("persist Stage Worker Reattach command: %w", err)
	}
	if stageVersion != authority.GetStageVersion() {
		return CommandResult{}, errors.New("durable Stage Worker Reattach changed Stage version")
	}
	return acceptedCommandResult(replayed), nil
}

func runtimeStateName(state velav1.ModelRuntimeExecutionState) string {
	const prefix = "MODEL_RUNTIME_EXECUTION_STATE_"
	name := state.String()
	if len(name) >= len(prefix) && name[:len(prefix)] == prefix {
		return name[len(prefix):]
	}
	return name
}

var _ ReattachmentOperations = (*PostgresReattachmentBackend)(nil)
