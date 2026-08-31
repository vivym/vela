package h3campaignevidence

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresReader struct {
	pool *pgxpool.Pool
}

func NewPostgresReader(pool *pgxpool.Pool) (*PostgresReader, error) {
	if pool == nil {
		return nil, errors.New("H3 campaign database pool is required")
	}
	return &PostgresReader{pool: pool}, nil
}

// Capture rebuilds campaign evidence only from committed database authority.
// A read-only repeatable-read transaction gives every related row one stable
// snapshot and retains enough provenance to audit that database view later.
func (reader *PostgresReader) Capture(
	ctx context.Context,
	selection Selection,
) (snapshot DatabaseSnapshot, returnedError error) {
	if ctx == nil || reader == nil || reader.pool == nil || !validSelection(selection) {
		return DatabaseSnapshot{}, errors.New("H3 campaign database capture input is invalid")
	}
	tx, err := reader.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return DatabaseSnapshot{}, fmt.Errorf("begin H3 campaign evidence transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil &&
			!errors.Is(rollbackErr, pgx.ErrTxClosed) && returnedError == nil {
			returnedError = fmt.Errorf("rollback H3 campaign evidence transaction: %w", rollbackErr)
		}
	}()
	if err := tx.QueryRow(ctx, `
		SELECT clock_timestamp(), pg_backend_pid()::text, pg_current_snapshot()::text
	`).Scan(
		&snapshot.Provenance.DatabaseTime,
		&snapshot.Provenance.BackendPID,
		&snapshot.Provenance.SnapshotID,
	); err != nil {
		return DatabaseSnapshot{}, fmt.Errorf("read H3 campaign snapshot identity: %w", err)
	}

	same, err := capturePhysicalRun(ctx, tx, RunSameNode, selection.SameNodeJobID)
	if err != nil {
		return DatabaseSnapshot{}, err
	}
	cross, err := capturePhysicalRun(ctx, tx, RunCrossNode, selection.CrossNodeJobID)
	if err != nil {
		return DatabaseSnapshot{}, err
	}
	cacheRun, err := captureCacheRun(ctx, tx, selection.CacheJobID)
	if err != nil {
		return DatabaseSnapshot{}, err
	}
	snapshot.Runs = []RunSnapshot{same, cross}
	snapshot.CacheRun = cacheRun
	if err := tx.Commit(ctx); err != nil {
		return DatabaseSnapshot{}, fmt.Errorf("commit read-only H3 campaign evidence transaction: %w", err)
	}
	return snapshot, nil
}

func capturePhysicalRun(
	ctx context.Context,
	tx pgx.Tx,
	kind RunKind,
	jobID uuid.UUID,
) (RunSnapshot, error) {
	var run RunSnapshot
	run.Kind = kind
	if err := tx.QueryRow(ctx, `
		SELECT job.id,
		       attempt.id,
		       attempt.fence,
		       (SELECT stage.execution_graph_revision_id
		        FROM stage_runs AS stage
		        WHERE stage.attempt_id = attempt.id
		        ORDER BY stage.id
		        LIMIT 1),
		       job.state::text,
		       attempt.state::text,
		       attempt.graph_state::text,
		       (SELECT count(*) FROM artifact_sets WHERE job_id = job.id),
		       (SELECT count(*) FROM visible_completions WHERE job_id = job.id),
		       (SELECT count(*) FROM charges WHERE job_id = job.id)
		FROM visible_completions AS completion
		JOIN jobs AS job ON job.id = completion.job_id
		JOIN attempts AS attempt ON attempt.id = completion.attempt_id
		WHERE job.id = $1
	`, jobID).Scan(
		&run.JobID,
		&run.AttemptID,
		&run.AttemptFence,
		&run.ExecutionGraphRevisionID,
		&run.JobState,
		&run.AttemptState,
		&run.GraphState,
		&run.ArtifactSetCount,
		&run.VisibleCompletionCount,
		&run.ChargeCount,
	); err != nil {
		return RunSnapshot{}, fmt.Errorf("read %s H3 campaign Job %s: %w", kind, jobID, err)
	}

	stages, err := capturePhysicalStages(ctx, tx, run.AttemptID)
	if err != nil {
		return RunSnapshot{}, fmt.Errorf("read %s H3 campaign stages: %w", kind, err)
	}
	transfers, err := capturePhysicalTransfers(ctx, tx, run.AttemptID)
	if err != nil {
		return RunSnapshot{}, fmt.Errorf("read %s H3 campaign transfers: %w", kind, err)
	}
	run.Stages = stages
	run.Transfers = transfers
	return run, nil
}

func capturePhysicalStages(
	ctx context.Context,
	tx pgx.Tx,
	attemptID uuid.UUID,
) ([]StageSnapshot, error) {
	rows, err := tx.Query(ctx, `
		SELECT run.stage_key,
		       run.state::text,
		       binding.source_kind::text,
		       run.id,
		       physical.id,
		       physical.selected_stage_profile_revision_id,
		       artifact.id,
		       artifact.object_version,
		       'sha256:' || encode(artifact.sha256, 'hex'),
		       artifact.stage_interface_revision_id,
		       node.node_identity,
		       allocation.worker_instance_id,
		       allocation.worker_instance_epoch,
		       worker.residency_plan_revision_id,
		       member.id,
		       member.member_epoch,
		       device.id,
		       device.device_epoch,
		       allocation.model_residency_id,
		       allocation.model_runtime_epoch
		FROM stage_runs AS run
		JOIN stage_run_output_bindings AS binding
		  ON binding.stage_run_id = run.id
		JOIN stage_artifacts AS artifact
		  ON artifact.id = binding.stage_artifact_id
		JOIN stage_attempts AS physical
		  ON physical.id = artifact.producer_stage_attempt_id
		 AND physical.stage_run_id = artifact.producer_stage_run_id
		JOIN stage_allocations AS allocation
		  ON allocation.stage_attempt_id = physical.id
		JOIN worker_instances AS worker
		  ON worker.id = allocation.worker_instance_id
		 AND worker.instance_epoch = allocation.worker_instance_epoch
		JOIN worker_members AS member
		  ON member.worker_instance_id = allocation.worker_instance_id
		 AND member.worker_instance_epoch = allocation.worker_instance_epoch
		JOIN worker_member_devices AS membership
		  ON membership.worker_instance_id = member.worker_instance_id
		 AND membership.worker_member_id = member.id
		JOIN devices AS device ON device.id = membership.device_id
		JOIN compute_nodes AS node ON node.id = device.compute_node_id
		WHERE run.attempt_id = $1
		ORDER BY CASE run.stage_key
			WHEN 'encoder' THEN 1 WHEN 'dit' THEN 2 WHEN 'vae' THEN 3 ELSE 4 END,
			member.member_key, device.id
	`, attemptID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	stages := make([]StageSnapshot, 0, 3)
	for rows.Next() {
		var stage StageSnapshot
		if err := rows.Scan(
			&stage.StageKey,
			&stage.State,
			&stage.SourceKind,
			&stage.StageRunID,
			&stage.StageAttemptID,
			&stage.StageProfileRevisionID,
			&stage.ArtifactID,
			&stage.ObjectVersion,
			&stage.OutputDigest,
			&stage.StageInterfaceRevisionID,
			&stage.NodeIdentity,
			&stage.WorkerInstanceID,
			&stage.WorkerInstanceEpoch,
			&stage.ResidencyPlanRevisionID,
			&stage.WorkerMemberID,
			&stage.MemberEpoch,
			&stage.DeviceID,
			&stage.DeviceEpoch,
			&stage.ModelResidencyID,
			&stage.ModelRuntimeEpoch,
		); err != nil {
			return nil, err
		}
		stages = append(stages, stage)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	for index := range stages {
		if err := captureStageLineage(ctx, tx, &stages[index]); err != nil {
			return nil, fmt.Errorf(
				"read StageArtifact %s lineage: %w", stages[index].ArtifactID, err,
			)
		}
	}
	return stages, nil
}

func captureStageLineage(ctx context.Context, tx pgx.Tx, stage *StageSnapshot) error {
	rows, err := tx.Query(ctx, `
		SELECT input_stage_artifact_id,
		       CASE WHEN root_input_digest IS NULL THEN ''
		            ELSE 'sha256:' || encode(root_input_digest, 'hex') END
		FROM stage_artifact_inputs
		WHERE stage_artifact_id = $1
		ORDER BY input_ordinal
	`, stage.ArtifactID)
	if err != nil {
		return err
	}
	defer rows.Close()
	rootInputs := 0
	lineageRows := 0
	for rows.Next() {
		var input *uuid.UUID
		var rootDigest string
		if err := rows.Scan(&input, &rootDigest); err != nil {
			return err
		}
		lineageRows++
		if input != nil {
			stage.InputStageArtifactIDs = append(stage.InputStageArtifactIDs, *input)
		}
		if rootDigest != "" {
			rootInputs++
			stage.RootInputDigest = rootDigest
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	if rootInputs > 1 {
		return errors.New("StageArtifact contains multiple root-input lineage rows")
	}
	if lineageRows == 0 {
		if err := tx.QueryRow(ctx, `
			SELECT 'sha256:' || encode(job.request_hash, 'hex')
			FROM stage_artifacts AS artifact
			JOIN jobs AS job ON job.id = artifact.job_id
			WHERE artifact.id = $1
			  AND NOT EXISTS (
				SELECT 1
				FROM stage_dependencies AS dependency
				WHERE dependency.destination_stage_run_id = artifact.producer_stage_run_id
			  )
		`, stage.ArtifactID).Scan(&stage.RootInputDigest); err != nil &&
			!errors.Is(err, pgx.ErrNoRows) {
			return err
		}
	}
	return nil
}

func capturePhysicalTransfers(
	ctx context.Context,
	tx pgx.Tx,
	attemptID uuid.UUID,
) ([]TransferSnapshot, error) {
	rows, err := tx.Query(ctx, `
		SELECT ticket.id,
		       artifact.producer_stage_run_id,
		       destination.id,
		       ticket.stage_artifact_id,
		       ticket.destination_worker_instance_id,
		       ticket.destination_worker_instance_epoch,
		       ticket.connector_revision_id,
		       ticket.state::text
		FROM transfer_tickets AS ticket
		JOIN stage_artifacts AS artifact ON artifact.id = ticket.stage_artifact_id
		JOIN stage_artifact_pins AS pin ON pin.id = ticket.stage_artifact_pin_id
		JOIN stage_runs AS destination ON destination.id = pin.owner_stage_run_id
		WHERE artifact.attempt_id = $1
		  AND destination.attempt_id = $1
		ORDER BY ticket.issued_at, ticket.id
	`, attemptID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	transfers := make([]TransferSnapshot, 0, 2)
	for rows.Next() {
		var transfer TransferSnapshot
		if err := rows.Scan(
			&transfer.ID,
			&transfer.SourceStageRunID,
			&transfer.DestinationStageRunID,
			&transfer.StageArtifactID,
			&transfer.DestinationWorkerInstanceID,
			&transfer.DestinationWorkerInstanceEpoch,
			&transfer.ConnectorRevisionID,
			&transfer.State,
		); err != nil {
			return nil, err
		}
		transfers = append(transfers, transfer)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return transfers, nil
}

func captureCacheRun(
	ctx context.Context,
	tx pgx.Tx,
	jobID uuid.UUID,
) (CacheRunSnapshot, error) {
	var run CacheRunSnapshot
	if err := tx.QueryRow(ctx, `
		SELECT job.organization_id,
		       job.project_id,
		       job.id,
		       attempt.id,
		       attempt.fence,
		       job.state::text,
		       attempt.state::text,
		       attempt.graph_state::text,
		       (SELECT count(*) FROM artifact_sets WHERE job_id = job.id),
		       (SELECT count(*) FROM visible_completions WHERE job_id = job.id),
		       (SELECT count(*) FROM charges WHERE job_id = job.id)
		FROM visible_completions AS completion
		JOIN jobs AS job ON job.id = completion.job_id
		JOIN attempts AS attempt ON attempt.id = completion.attempt_id
		WHERE job.id = $1
	`, jobID).Scan(
		&run.OrganizationID,
		&run.ProjectID,
		&run.JobID,
		&run.AttemptID,
		&run.AttemptFence,
		&run.JobState,
		&run.AttemptState,
		&run.GraphState,
		&run.ArtifactSetCount,
		&run.VisibleCompletionCount,
		&run.ChargeCount,
	); err != nil {
		return CacheRunSnapshot{}, fmt.Errorf("read exact-cache H3 campaign Job %s: %w", jobID, err)
	}

	rows, err := tx.Query(ctx, `
		SELECT target.stage_key,
		       entry.id,
		       entry.state::text,
		       entry.scope::text,
		       COALESCE(entry.scope_project_id,
		                '00000000-0000-0000-0000-000000000000'::uuid),
		       entry.organization_id,
		       entry.source_project_id,
		       entry.source_job_id,
		       entry.source_stage_run_id,
		       source_worker.residency_plan_revision_id,
		       reference.owner_job_id,
		       reference.owner_stage_run_id,
		       entry.stage_artifact_id,
		       entry.exact_object_version,
		       'sha256:' || encode(entry.sha256, 'hex'),
		       'sha256:' || encode(entry.cache_key_digest, 'hex'),
		       entry.cache_policy_revision_id,
		       entry.result_equivalence_revision_id,
		       reference.id,
		       reference.state::text,
		       reference.acquired_at,
		       pin.id,
		       pin.pin_kind::text,
		       pin.state::text,
		       pin.acquired_at,
		       binding.source_kind::text,
		       binding.stage_artifact_id
		FROM stage_run_output_bindings AS binding
		JOIN stage_runs AS target ON target.id = binding.stage_run_id
		JOIN stage_cache_references AS reference
		  ON reference.id = binding.stage_cache_reference_id
		JOIN stage_cache_entries AS entry
		  ON entry.id = reference.stage_cache_entry_id
		JOIN stage_artifacts AS source_artifact
		  ON source_artifact.id = entry.stage_artifact_id
		 AND source_artifact.job_id = entry.source_job_id
		 AND source_artifact.producer_stage_run_id = entry.source_stage_run_id
		JOIN stage_attempts AS source_attempt
		  ON source_attempt.id = source_artifact.producer_stage_attempt_id
		 AND source_attempt.stage_run_id = source_artifact.producer_stage_run_id
		JOIN stage_allocations AS source_allocation
		  ON source_allocation.stage_attempt_id = source_attempt.id
		JOIN worker_instances AS source_worker
		  ON source_worker.id = source_allocation.worker_instance_id
		 AND source_worker.instance_epoch = source_allocation.worker_instance_epoch
		JOIN stage_artifact_pins AS pin
		  ON pin.id = reference.execution_pin_id
		WHERE binding.job_id = $1
		  AND binding.attempt_id = $2
		  AND binding.source_kind = 'EXACT_CACHE'
		ORDER BY CASE target.stage_key
			WHEN 'encoder' THEN 1 WHEN 'dit' THEN 2 ELSE 3 END, target.id
	`, run.JobID, run.AttemptID)
	if err != nil {
		return CacheRunSnapshot{}, fmt.Errorf("read exact-cache H3 campaign hits: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var hit CacheHitSnapshot
		if err := rows.Scan(
			&hit.StageKey,
			&hit.EntryID,
			&hit.EntryState,
			&hit.Scope,
			&hit.ScopeProjectID,
			&hit.OrganizationID,
			&hit.SourceProjectID,
			&hit.SourceJobID,
			&hit.SourceStageRunID,
			&hit.SourceResidencyPlanRevisionID,
			&hit.TargetJobID,
			&hit.TargetStageRunID,
			&hit.StageArtifactID,
			&hit.ExactObjectVersion,
			&hit.ArtifactDigest,
			&hit.CacheKeyDigest,
			&hit.CachePolicyRevisionID,
			&hit.ResultEquivalenceRevisionID,
			&hit.ReferenceID,
			&hit.ReferenceState,
			&hit.ReferenceAcquiredAt,
			&hit.ExecutionPinID,
			&hit.PinKind,
			&hit.PinState,
			&hit.PinAcquiredAt,
			&hit.OutputBindingSourceKind,
			&hit.OutputBindingArtifactID,
		); err != nil {
			return CacheRunSnapshot{}, fmt.Errorf("decode exact-cache H3 campaign hit: %w", err)
		}
		run.Hits = append(run.Hits, hit)
	}
	if err := rows.Err(); err != nil {
		return CacheRunSnapshot{}, fmt.Errorf("iterate exact-cache H3 campaign hits: %w", err)
	}
	workers, err := tx.Query(ctx, `
		SELECT run.stage_key,
		       allocation.worker_instance_id,
		       worker.residency_plan_revision_id
		FROM stage_runs AS run
		JOIN stage_attempts AS physical
		  ON physical.stage_run_id = run.id
		JOIN stage_allocations AS allocation
		  ON allocation.stage_attempt_id = physical.id
		JOIN worker_instances AS worker
		  ON worker.id = allocation.worker_instance_id
		 AND worker.instance_epoch = allocation.worker_instance_epoch
		WHERE run.attempt_id = $1
		ORDER BY run.stage_key, allocation.worker_instance_id
	`, run.AttemptID)
	if err != nil {
		return CacheRunSnapshot{}, fmt.Errorf("read exact-cache H3 campaign physical Workers: %w", err)
	}
	defer workers.Close()
	for workers.Next() {
		var worker WorkerPlanSnapshot
		if err := workers.Scan(
			&worker.StageKey,
			&worker.WorkerInstanceID,
			&worker.ResidencyPlanRevisionID,
		); err != nil {
			return CacheRunSnapshot{}, fmt.Errorf("decode exact-cache physical Worker: %w", err)
		}
		run.PhysicalWorkers = append(run.PhysicalWorkers, worker)
	}
	if err := workers.Err(); err != nil {
		return CacheRunSnapshot{}, fmt.Errorf("iterate exact-cache physical Workers: %w", err)
	}
	return run, nil
}

var _ DatabaseReader = (*PostgresReader)(nil)
