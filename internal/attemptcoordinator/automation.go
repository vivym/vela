package attemptcoordinator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	store "github.com/vivym/vela/internal/store/sqlc"
)

type automationConfig struct {
	instanceID   string
	claimSeconds int
	retrySeconds int
	batchSize    int
}

type instantiationClaim struct {
	InstantiateCommand
	reclaimed bool
}

func newAutomationConfig(config AutomationConfig) (automationConfig, error) {
	if config.InstanceID == "" || len(config.InstanceID) > 200 ||
		strings.TrimSpace(config.InstanceID) != config.InstanceID ||
		strings.IndexFunc(config.InstanceID, unicode.IsControl) >= 0 {
		return automationConfig{}, errors.New("AttemptCoordinator automation instance id is invalid")
	}
	claimSeconds, ok := automationDurationSeconds(config.ClaimTTL, false, 300)
	if !ok {
		return automationConfig{}, errors.New(
			"AttemptCoordinator automation claim TTL must be in (0, 5m]",
		)
	}
	retrySeconds, ok := automationDurationSeconds(config.RetryDelay, true, 86400)
	if !ok {
		return automationConfig{}, errors.New(
			"AttemptCoordinator automation retry delay must be in [0, 24h]",
		)
	}
	if config.BatchSize < 1 || config.BatchSize > 1000 {
		return automationConfig{}, errors.New(
			"AttemptCoordinator automation batch size must be between 1 and 1000",
		)
	}
	return automationConfig{
		instanceID:   config.InstanceID,
		claimSeconds: claimSeconds,
		retrySeconds: retrySeconds,
		batchSize:    config.BatchSize,
	}, nil
}

func (service *Service) RunCycle(ctx context.Context) (AutomationResult, error) {
	if service == nil || service.pool == nil || service.automation == nil {
		return AutomationResult{}, errors.New("AttemptCoordinator automation is not configured")
	}
	result := AutomationResult{}
	reconciled, discarded, err := service.reconcileInstantiations(
		ctx,
		service.automation.batchSize,
	)
	if err != nil {
		return result, err
	}
	result.Reconciled = reconciled
	result.Discarded = discarded

	claims, claimToken, err := service.claimInstantiations(ctx, *service.automation)
	if err != nil {
		return result, err
	}
	result.Claims = len(claims)
	var cycleErrors []error
	for _, claim := range claims {
		if claim.reclaimed {
			result.Reclaimed++
		}
		handle, instantiateErr := service.Instantiate(ctx, claim.InstantiateCommand)
		if instantiateErr != nil {
			releaseErr := service.releaseInstantiation(
				ctx,
				claim.JobID,
				claimToken,
				service.automation.retrySeconds,
				instantiateErr,
			)
			cycleErrors = append(cycleErrors, errors.Join(instantiateErr, releaseErr))
			continue
		}
		if handle.Replayed {
			result.Replayed++
		} else {
			result.Instantiated++
		}
		completed, completeErr := service.completeInstantiation(
			ctx,
			claim.JobID,
			claimToken,
			claim.CommandID,
			handle.SnapshotID,
			handle.AttemptID,
		)
		if completeErr != nil {
			cycleErrors = append(cycleErrors, completeErr)
			continue
		}
		if !completed {
			cycleErrors = append(cycleErrors, fmt.Errorf(
				"complete Stage graph instantiation for Job %s: claim is stale",
				claim.JobID,
			))
		}
	}

	transitions, reconcileErr := service.Reconcile(ctx, service.automation.batchSize)
	result.StageTransitions = len(transitions)
	if reconcileErr != nil {
		cycleErrors = append(cycleErrors, reconcileErr)
	}
	return result, errors.Join(cycleErrors...)
}

func (service *Service) reconcileInstantiations(
	ctx context.Context,
	limit int,
) (int, int, error) {
	rows, err := service.pool.Query(ctx, `
		SELECT job_id, state::text, reason
		FROM vela_reconcile_stage_graph_instantiations($1)
	`, limit)
	if err != nil {
		return 0, 0, fmt.Errorf("reconcile Stage graph instantiations: %w", err)
	}
	defer rows.Close()
	completed := 0
	discarded := 0
	for rows.Next() {
		var jobID uuid.UUID
		var state store.StageGraphInstantiationState
		var reason string
		if err := rows.Scan(&jobID, &state, &reason); err != nil {
			return 0, 0, fmt.Errorf("scan reconciled Stage graph instantiation: %w", err)
		}
		switch state {
		case store.StageGraphInstantiationStateCOMPLETED:
			completed++
		case store.StageGraphInstantiationStateDISCARDED:
			discarded++
		default:
			return 0, 0, fmt.Errorf(
				"reconcile Stage graph instantiation %s returned state %q for reason %q",
				jobID,
				state,
				reason,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("read reconciled Stage graph instantiations: %w", err)
	}
	return completed, discarded, nil
}

func (service *Service) claimInstantiations(
	ctx context.Context,
	config automationConfig,
) ([]instantiationClaim, uuid.UUID, error) {
	claimToken := uuid.New()
	rows, err := service.pool.Query(ctx, `
		SELECT
			job_id,
			command_id,
			expected_job_version,
			expected_job_fence,
			execution_graph_snapshot_id,
			execution_graph_revision_id,
			execution_profile_revision_id,
			attempt_id,
			storage_reservation_id,
			reserved_storage_bytes,
			reclaimed
		FROM vela_claim_stage_graph_instantiations($1, $2, $3, $4)
	`, config.instanceID, claimToken, config.claimSeconds, config.batchSize)
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("claim Stage graph instantiations: %w", err)
	}
	defer rows.Close()
	claims := make([]instantiationClaim, 0)
	for rows.Next() {
		var claim instantiationClaim
		if err := rows.Scan(
			&claim.JobID,
			&claim.CommandID,
			&claim.ExpectedJobVersion,
			&claim.ExpectedJobFence,
			&claim.ExecutionGraphSnapshotID,
			&claim.ExecutionGraphRevisionID,
			&claim.ExecutionProfileRevisionID,
			&claim.AttemptID,
			&claim.StorageReservationID,
			&claim.ReservedStorageBytes,
			&claim.reclaimed,
		); err != nil {
			return nil, uuid.Nil, fmt.Errorf("scan Stage graph instantiation claim: %w", err)
		}
		claims = append(claims, claim)
	}
	if err := rows.Err(); err != nil {
		return nil, uuid.Nil, fmt.Errorf("read Stage graph instantiation claims: %w", err)
	}
	return claims, claimToken, nil
}

func (service *Service) completeInstantiation(
	ctx context.Context,
	jobID uuid.UUID,
	claimToken uuid.UUID,
	commandID uuid.UUID,
	snapshotID uuid.UUID,
	attemptID uuid.UUID,
) (bool, error) {
	var completed bool
	err := service.pool.QueryRow(ctx, `
		SELECT vela_complete_stage_graph_instantiation($1, $2, $3, $4, $5)
	`, jobID, claimToken, commandID, snapshotID, attemptID).Scan(&completed)
	if err != nil {
		return false, fmt.Errorf("complete Stage graph instantiation for Job %s: %w", jobID, err)
	}
	return completed, nil
}

func (service *Service) releaseInstantiation(
	ctx context.Context,
	jobID uuid.UUID,
	claimToken uuid.UUID,
	retrySeconds int,
	workErr error,
) error {
	var released bool
	err := service.pool.QueryRow(ctx, `
		SELECT vela_release_stage_graph_instantiation($1, $2, $3, $4)
	`, jobID, claimToken, retrySeconds, databaseErrorText(workErr)).Scan(&released)
	if err != nil {
		return fmt.Errorf("release Stage graph instantiation for Job %s: %w", jobID, err)
	}
	if !released {
		return fmt.Errorf("release Stage graph instantiation for Job %s: claim is stale", jobID)
	}
	return nil
}

func automationDurationSeconds(duration time.Duration, allowZero bool, maximum int) (int, bool) {
	if duration < 0 || (!allowZero && duration == 0) {
		return 0, false
	}
	seconds := duration / time.Second
	if duration%time.Second != 0 {
		seconds++
	}
	if seconds > time.Duration(maximum) || seconds > time.Duration(math.MaxInt32) {
		return 0, false
	}
	return int(seconds), true
}

func databaseErrorText(err error) string {
	message := "Stage graph instantiation failed"
	if err != nil {
		message = err.Error()
	}
	message = strings.Map(func(character rune) rune {
		if unicode.IsControl(character) {
			return ' '
		}
		return character
	}, message)
	message = strings.TrimSpace(message)
	if message == "" {
		message = "Stage graph instantiation failed"
	}
	for len(message) > 2000 {
		_, size := utf8.DecodeLastRuneInString(message)
		message = message[:len(message)-size]
	}
	return message
}
