package h3launchevidence

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RegistryReader interface {
	Capture(context.Context, uuid.UUID) (RegistrySnapshot, error)
}

type PostgresRegistryReader struct {
	pool *pgxpool.Pool
}

func NewPostgresRegistryReader(pool *pgxpool.Pool) (*PostgresRegistryReader, error) {
	if pool == nil {
		return nil, errors.New("fleet registry database pool is required")
	}
	return &PostgresRegistryReader{pool: pool}, nil
}

// Capture reads one ResidencyPlan's complete current Worker authority inside a
// repeatable-read, read-only transaction. The transaction and snapshot IDs are
// retained so an evidence review can correlate the exact database view.
func (reader *PostgresRegistryReader) Capture(
	ctx context.Context,
	planRevisionID uuid.UUID,
) (snapshot RegistrySnapshot, returnedError error) {
	if ctx == nil || reader == nil || reader.pool == nil || planRevisionID == uuid.Nil {
		return RegistrySnapshot{}, errors.New("fleet registry capture input is invalid")
	}
	tx, err := reader.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return RegistrySnapshot{}, fmt.Errorf("begin Fleet Registry evidence transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) && returnedError == nil {
			returnedError = fmt.Errorf("rollback Fleet Registry evidence transaction: %w", rollbackErr)
		}
	}()
	if err := tx.QueryRow(ctx, `
		SELECT clock_timestamp(), pg_backend_pid()::text, pg_current_snapshot()::text
	`).Scan(&snapshot.DatabaseTime, &snapshot.TransactionID, &snapshot.SnapshotID); err != nil {
		return RegistrySnapshot{}, fmt.Errorf("read Fleet Registry snapshot identity: %w", err)
	}
	workers, err := tx.Query(ctx, `
		SELECT worker.id,
		       worker.instance_epoch,
		       worker.control_session_epoch,
		       worker.residency_plan_revision_id,
		       worker.worker_bundle_id,
		       worker.worker_profile_revision_id,
		       worker.capacity_pool_id,
		       worker.lifecycle_state::text,
		       worker.reachability_state::text,
		       COALESCE(worker.device_set_id, '00000000-0000-0000-0000-000000000000'::uuid),
		       COALESCE(encode(worker.device_set_digest, 'hex'), ''),
		       COALESCE(encode(worker.membership_digest, 'hex'), '')
		FROM worker_instances AS worker
		WHERE worker.residency_plan_revision_id = $1
		ORDER BY worker.id
	`, planRevisionID)
	if err != nil {
		return RegistrySnapshot{}, fmt.Errorf("read Fleet Registry WorkerInstances: %w", err)
	}
	workerRows := make([]RegistryWorker, 0)
	for workers.Next() {
		var worker RegistryWorker
		if err := workers.Scan(
			&worker.ID,
			&worker.InstanceEpoch,
			&worker.ControlSessionEpoch,
			&worker.ResidencyPlanRevisionID,
			&worker.WorkerBundleID,
			&worker.WorkerProfileRevisionID,
			&worker.CapacityPoolID,
			&worker.Lifecycle,
			&worker.Reachability,
			&worker.DeviceSetID,
			&worker.DeviceSetDigest,
			&worker.MembershipDigest,
		); err != nil {
			return RegistrySnapshot{}, fmt.Errorf("decode Fleet Registry WorkerInstance: %w", err)
		}
		workerRows = append(workerRows, worker)
	}
	if err := workers.Err(); err != nil {
		workers.Close()
		return RegistrySnapshot{}, fmt.Errorf("iterate Fleet Registry WorkerInstances: %w", err)
	}
	workers.Close()
	if len(workerRows) == 0 {
		return RegistrySnapshot{}, errors.New("fleet registry has no WorkerInstances for the approved ResidencyPlan")
	}
	for _, worker := range workerRows {
		worker.Members, err = captureRegistryMembers(ctx, tx, worker.ID, worker.InstanceEpoch)
		if err != nil {
			return RegistrySnapshot{}, err
		}
		worker.Residencies, err = captureRegistryResidencies(ctx, tx, worker.ID, worker.InstanceEpoch)
		if err != nil {
			return RegistrySnapshot{}, err
		}
		snapshot.Workers = append(snapshot.Workers, worker)
	}
	if err := tx.Commit(ctx); err != nil {
		return RegistrySnapshot{}, fmt.Errorf("commit read-only Fleet Registry evidence transaction: %w", err)
	}
	return snapshot, nil
}

func captureRegistryMembers(
	ctx context.Context,
	tx pgx.Tx,
	workerID uuid.UUID,
	instanceEpoch int64,
) ([]RegistryMember, error) {
	rows, err := tx.Query(ctx, `
		SELECT member.id,
		       member.member_key,
		       member.member_epoch,
		       member.compute_node_id,
		       node.node_identity,
		       member.readiness::text,
		       encode(member.device_subset_digest, 'hex'),
		       encode(member.identity_digest, 'hex')
		FROM worker_members AS member
		JOIN compute_nodes AS node ON node.id = member.compute_node_id
		WHERE member.worker_instance_id = $1
		  AND member.worker_instance_epoch = $2
		ORDER BY member.member_key, member.id
	`, workerID, instanceEpoch)
	if err != nil {
		return nil, fmt.Errorf("read Fleet Registry WorkerMembers for %s: %w", workerID, err)
	}
	members := make([]RegistryMember, 0)
	for rows.Next() {
		var member RegistryMember
		if err := rows.Scan(
			&member.ID,
			&member.Key,
			&member.MemberEpoch,
			&member.ComputeNodeID,
			&member.NodeIdentity,
			&member.Readiness,
			&member.DeviceSubsetDigest,
			&member.IdentityDigest,
		); err != nil {
			return nil, fmt.Errorf("decode Fleet Registry WorkerMember for %s: %w", workerID, err)
		}
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate Fleet Registry WorkerMembers for %s: %w", workerID, err)
	}
	rows.Close()
	for index := range members {
		members[index].Devices, err = captureRegistryDevices(ctx, tx, workerID, members[index].ID)
		if err != nil {
			return nil, err
		}
	}
	return members, nil
}

func captureRegistryDevices(
	ctx context.Context,
	tx pgx.Tx,
	workerID,
	memberID uuid.UUID,
) ([]RegistryDevice, error) {
	rows, err := tx.Query(ctx, `
		SELECT device.id,
		       device.device_epoch,
		       device.compute_node_id,
		       node.node_identity,
		       node.lifecycle_epoch,
		       node.agent_session_epoch,
		       device.gpu_uuid,
		       device.pci_bdf,
		       device.health::text,
		       encode(node.attestation_digest, 'hex'),
		       encode(device.attestation_digest, 'hex')
		FROM worker_member_devices AS membership
		JOIN devices AS device ON device.id = membership.device_id
		JOIN compute_nodes AS node ON node.id = device.compute_node_id
		WHERE membership.worker_instance_id = $1
		  AND membership.worker_member_id = $2
		ORDER BY device.id
	`, workerID, memberID)
	if err != nil {
		return nil, fmt.Errorf("read Fleet Registry devices for WorkerMember %s: %w", memberID, err)
	}
	defer rows.Close()
	devices := make([]RegistryDevice, 0)
	for rows.Next() {
		var device RegistryDevice
		if err := rows.Scan(
			&device.ID,
			&device.DeviceEpoch,
			&device.ComputeNodeID,
			&device.NodeIdentity,
			&device.NodeEpoch,
			&device.AgentSessionEpoch,
			&device.GPUUUID,
			&device.PCIBDF,
			&device.Health,
			&device.NodeAttestationDigest,
			&device.DeviceAttestationDigest,
		); err != nil {
			return nil, fmt.Errorf("decode Fleet Registry device for WorkerMember %s: %w", memberID, err)
		}
		devices = append(devices, device)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Fleet Registry devices for WorkerMember %s: %w", memberID, err)
	}
	return devices, nil
}

func captureRegistryResidencies(
	ctx context.Context,
	tx pgx.Tx,
	workerID uuid.UUID,
	instanceEpoch int64,
) ([]RegistryResidency, error) {
	rows, err := tx.Query(ctx, `
		SELECT residency.id,
		       residency.model_component_revision,
		       residency.runtime_identity,
		       residency.runtime_image_digest,
		       residency.model_runtime_epoch,
		       residency.state::text,
		       encode(residency.warmup_evidence_digest, 'hex'),
		       encode(residency.canary_evidence_digest, 'hex')
		FROM model_residencies AS residency
		WHERE residency.worker_instance_id = $1
		  AND residency.worker_instance_epoch = $2
		  AND residency.released_at IS NULL
		ORDER BY residency.id
	`, workerID, instanceEpoch)
	if err != nil {
		return nil, fmt.Errorf("read Fleet Registry ModelResidencies for %s: %w", workerID, err)
	}
	defer rows.Close()
	residencies := make([]RegistryResidency, 0)
	for rows.Next() {
		var residency RegistryResidency
		if err := rows.Scan(
			&residency.ID,
			&residency.ModelComponentRevision,
			&residency.RuntimeIdentity,
			&residency.RuntimeImageDigest,
			&residency.ModelRuntimeEpoch,
			&residency.State,
			&residency.WarmupEvidenceDigest,
			&residency.CanaryEvidenceDigest,
		); err != nil {
			return nil, fmt.Errorf("decode Fleet Registry ModelResidency for %s: %w", workerID, err)
		}
		residencies = append(residencies, residency)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Fleet Registry ModelResidencies for %s: %w", workerID, err)
	}
	return residencies, nil
}

var _ RegistryReader = (*PostgresRegistryReader)(nil)
