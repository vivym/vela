package stageworkercontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

type PostgresAuthorizer struct {
	pool *pgxpool.Pool
}

type durableStageAuthority struct {
	jobID                         uuid.UUID
	attemptID                     uuid.UUID
	attemptFence                  int64
	attemptState                  string
	stageRunID                    uuid.UUID
	stageFence                    int64
	stageVersion                  int64
	stageRunState                 string
	stageAttemptID                uuid.UUID
	stageAttemptState             string
	stageProfileID                uuid.UUID
	stageAllocationID             uuid.UUID
	allocationState               string
	capacityVector                map[string]int64
	stageLeaseID                  uuid.UUID
	leaseState                    string
	workerInstanceID              uuid.UUID
	workerInstanceEpoch           int64
	deviceSetDigest               []byte
	membershipDigest              []byte
	modelResidencyID              uuid.UUID
	modelRuntimeBarrierGeneration int64
	tokenDigest                   []byte
	signingKeyID                  string
	executionNonce                []byte
	issuedAt                      time.Time
	expiresAt                     time.Time
	localDeadlineAt               time.Time
	controlSessionEpoch           int64
	workerLifecycle               string
	workerReachability            string
	runtimeIdentity               string
	residencyState                string
	capacityObservationOK         bool
	members                       []durableMember
	devices                       []durableDevice
}

type durableMember struct {
	id                uuid.UUID
	epoch             int64
	identityDigest    []byte
	readiness         string
	modelRuntimeEpoch int64
}

type durableDevice struct {
	id    uuid.UUID
	epoch int64
}

func NewPostgresAuthorizer(pool *pgxpool.Pool) (*PostgresAuthorizer, error) {
	if pool == nil {
		return nil, errors.New("stage worker durable authority database pool is required")
	}
	return &PostgresAuthorizer{pool: pool}, nil
}

func (authorizer *PostgresAuthorizer) IsActive(
	ctx context.Context,
	identity stageworkertransport.Identity,
	sessionEpoch int64,
	operation Operation,
	verified stageauthority.Verified,
) (bool, error) {
	if authorizer == nil || authorizer.pool == nil {
		return false, errors.New("stage worker durable authorizer is not configured")
	}
	if ctx == nil || identity.SPIFFEID == "" || sessionEpoch <= 0 ||
		verified.Authority == nil || verified.Digest == ([sha256.Size]byte{}) ||
		!operationRequiresStageAuthority(operation) {
		return false, errors.New("stage worker durable authority check is incomplete")
	}
	digest, err := stageauthority.Digest(verified.Authority)
	if err != nil || digest != verified.Digest {
		return false, errors.New("verified StageAuthority digest is inconsistent")
	}
	leaseID, err := uuid.Parse(verified.Authority.GetStageLeaseId())
	if err != nil {
		return false, nil
	}
	tx, err := authorizer.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return false, fmt.Errorf("begin StageAuthority snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	snapshot, err := readDurableStageAuthority(ctx, tx, leaseID, verified.Authority.GetCapacityObservationSequence())
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit StageAuthority snapshot: %w", err)
	}
	return matchesDurableStageAuthority(
		snapshot, identity, sessionEpoch, operation, verified.Authority,
	), nil
}

func readDurableStageAuthority(
	ctx context.Context,
	tx pgx.Tx,
	leaseID uuid.UUID,
	observationSequence int64,
) (durableStageAuthority, error) {
	var snapshot durableStageAuthority
	var capacityJSON, ignoredMembersJSON, devicesJSON []byte
	err := tx.QueryRow(ctx, `
		SELECT * FROM vela_read_stage_authority_snapshot($1, $2)
	`, leaseID, observationSequence).Scan(
		&snapshot.jobID, &snapshot.attemptID, &snapshot.attemptFence, &snapshot.attemptState,
		&snapshot.stageRunID, &snapshot.stageFence, &snapshot.stageVersion, &snapshot.stageRunState,
		&snapshot.stageAttemptID, &snapshot.stageAttemptState, &snapshot.stageProfileID,
		&snapshot.stageAllocationID, &snapshot.allocationState, &capacityJSON,
		&snapshot.stageLeaseID, &snapshot.leaseState,
		&snapshot.workerInstanceID, &snapshot.workerInstanceEpoch,
		&snapshot.deviceSetDigest, &snapshot.membershipDigest,
		&snapshot.modelResidencyID, &snapshot.modelRuntimeBarrierGeneration,
		&snapshot.tokenDigest, &snapshot.signingKeyID, &snapshot.executionNonce,
		&snapshot.issuedAt, &snapshot.expiresAt, &snapshot.localDeadlineAt,
		&snapshot.controlSessionEpoch, &snapshot.workerLifecycle,
		&snapshot.workerReachability, &snapshot.runtimeIdentity,
		&snapshot.residencyState, &snapshot.capacityObservationOK,
		&ignoredMembersJSON, &devicesJSON,
	)
	if err != nil {
		return durableStageAuthority{}, fmt.Errorf("read durable StageAuthority: %w", err)
	}
	if err := json.Unmarshal(capacityJSON, &snapshot.capacityVector); err != nil {
		return durableStageAuthority{}, fmt.Errorf("decode durable Stage capacity: %w", err)
	}
	var membersJSON []byte
	if err := tx.QueryRow(ctx, `
		SELECT members FROM vela_read_stage_authority_member_epochs($1)
	`, leaseID).Scan(&membersJSON); err != nil {
		return durableStageAuthority{}, fmt.Errorf("read durable ModelRuntime member epochs: %w", err)
	}
	var encodedMembers []struct {
		ID                uuid.UUID `json:"id"`
		Epoch             int64     `json:"epoch"`
		IdentityDigest    string    `json:"identity_digest"`
		Readiness         string    `json:"readiness"`
		ModelRuntimeEpoch int64     `json:"model_runtime_epoch"`
	}
	if err := json.Unmarshal(membersJSON, &encodedMembers); err != nil {
		return durableStageAuthority{}, fmt.Errorf("decode durable WorkerMembers: %w", err)
	}
	for _, encoded := range encodedMembers {
		identityDigest, err := hex.DecodeString(encoded.IdentityDigest)
		if err != nil || len(identityDigest) != sha256.Size || encoded.ModelRuntimeEpoch <= 0 {
			return durableStageAuthority{}, errors.New("durable WorkerMember identity digest is malformed")
		}
		snapshot.members = append(snapshot.members, durableMember{
			id: encoded.ID, epoch: encoded.Epoch,
			identityDigest: identityDigest, readiness: encoded.Readiness,
			modelRuntimeEpoch: encoded.ModelRuntimeEpoch,
		})
	}
	var encodedDevices []struct {
		ID    uuid.UUID `json:"id"`
		Epoch int64     `json:"epoch"`
	}
	if err := json.Unmarshal(devicesJSON, &encodedDevices); err != nil {
		return durableStageAuthority{}, fmt.Errorf("decode durable Devices: %w", err)
	}
	for _, encoded := range encodedDevices {
		snapshot.devices = append(snapshot.devices, durableDevice{
			id: encoded.ID, epoch: encoded.Epoch,
		})
	}
	return snapshot, nil
}

func matchesDurableStageAuthority(
	snapshot durableStageAuthority,
	identity stageworkertransport.Identity,
	sessionEpoch int64,
	operation Operation,
	authority *velav1.StageAuthority,
) bool {
	if len(authority.GetMembers()) == 0 {
		return false
	}
	jobID, jobErr := uuid.Parse(authority.GetJobId())
	attemptID, attemptErr := uuid.Parse(authority.GetAttemptId())
	runID, runErr := uuid.Parse(authority.GetStageRunId())
	physicalID, physicalErr := uuid.Parse(authority.GetStageAttemptId())
	allocationID, allocationErr := uuid.Parse(authority.GetStageAllocationId())
	leaseID, leaseErr := uuid.Parse(authority.GetStageLeaseId())
	workerID, workerErr := uuid.Parse(authority.GetWorkerInstanceId())
	residencyID, residencyErr := uuid.Parse(authority.GetModelResidencyId())
	profileID, profileErr := uuid.Parse(authority.GetStageProfileRevisionId())
	if errors.Join(
		jobErr, attemptErr, runErr, physicalErr, allocationErr, leaseErr,
		workerErr, residencyErr, profileErr,
	) != nil {
		return false
	}
	leaseTokenDigest := sha256.Sum256(authority.GetLeaseToken())
	localDeadline := authority.GetIssuedAt().AsTime().UTC().Add(authority.GetMonotonicValidFor().AsDuration())
	if snapshot.jobID != jobID || snapshot.attemptID != attemptID || snapshot.stageRunID != runID ||
		snapshot.stageAttemptID != physicalID || snapshot.stageAllocationID != allocationID ||
		snapshot.stageLeaseID != leaseID || snapshot.workerInstanceID != workerID ||
		snapshot.modelResidencyID != residencyID || snapshot.stageProfileID != profileID ||
		snapshot.attemptFence != authority.GetAttemptFence() ||
		snapshot.stageFence != authority.GetStageFence() ||
		snapshot.stageVersion != authority.GetStageVersion() ||
		snapshot.workerInstanceEpoch != authority.GetWorkerInstanceEpoch() ||
		snapshot.modelRuntimeBarrierGeneration != authority.GetModelRuntimeBarrierGeneration() ||
		snapshot.modelRuntimeBarrierGeneration <= 0 || snapshot.controlSessionEpoch != sessionEpoch ||
		!bytes.Equal(snapshot.deviceSetDigest, authority.GetDeviceSetDigest()) ||
		!bytes.Equal(snapshot.membershipDigest, authority.GetMembershipDigest()) ||
		!bytes.Equal(snapshot.tokenDigest, leaseTokenDigest[:]) ||
		!bytes.Equal(snapshot.executionNonce, authority.GetExecutionNonce()) ||
		snapshot.signingKeyID != authority.GetSigningKeyId() ||
		snapshot.runtimeIdentity != authority.GetModelRuntimeIdentity() ||
		!maps.Equal(snapshot.capacityVector, authority.GetCapacityVector()) ||
		!snapshot.issuedAt.Equal(authority.GetIssuedAt().AsTime()) ||
		!snapshot.expiresAt.Equal(authority.GetExpiresAt().AsTime()) ||
		!snapshot.localDeadlineAt.Equal(localDeadline) || !snapshot.capacityObservationOK ||
		snapshot.leaseState != "ACTIVE" || snapshot.allocationState != "ALLOCATED" ||
		snapshot.workerLifecycle != "READY" || snapshot.workerReachability != "CONNECTED" ||
		snapshot.residencyState != "READY" ||
		!activeOperationState(operation, snapshot) {
		return false
	}
	if !matchesMembers(snapshot.members, authority.GetMembers(), identity.SPIFFEID) ||
		!matchesDevices(snapshot.devices, authority.GetDevices()) {
		return false
	}
	return true
}

func matchesMembers(
	durable []durableMember,
	signed []*velav1.StageAuthorityMemberEpoch,
	spiffeID string,
) bool {
	if len(durable) == 0 || len(durable) != len(signed) {
		return false
	}
	spiffeDigest := sha256.Sum256([]byte(spiffeID))
	identityMatched := false
	canonical := slices.Clone(signed)
	slices.SortFunc(canonical, func(left, right *velav1.StageAuthorityMemberEpoch) int {
		return bytes.Compare([]byte(left.GetWorkerMemberId()), []byte(right.GetWorkerMemberId()))
	})
	for index, member := range durable {
		if canonical[index] == nil || canonical[index].GetWorkerMemberId() != member.id.String() ||
			canonical[index].GetMemberEpoch() != member.epoch ||
			canonical[index].GetModelRuntimeEpoch() != member.modelRuntimeEpoch ||
			!bytes.Equal(canonical[index].GetIdentityDigest(), member.identityDigest) ||
			member.readiness != "READY" {
			return false
		}
		identityMatched = identityMatched || bytes.Equal(member.identityDigest, spiffeDigest[:])
	}
	return identityMatched
}

func matchesDevices(
	durable []durableDevice,
	signed []*velav1.StageAuthorityDeviceEpoch,
) bool {
	if len(durable) == 0 || len(durable) != len(signed) {
		return false
	}
	canonical := slices.Clone(signed)
	slices.SortFunc(canonical, func(left, right *velav1.StageAuthorityDeviceEpoch) int {
		return bytes.Compare([]byte(left.GetDeviceId()), []byte(right.GetDeviceId()))
	})
	for index, device := range durable {
		if canonical[index] == nil || canonical[index].GetDeviceId() != device.id.String() ||
			canonical[index].GetDeviceEpoch() != device.epoch {
			return false
		}
	}
	return true
}

func activeOperationState(operation Operation, snapshot durableStageAuthority) bool {
	descriptor, ok := descriptorForOperation(operation)
	return ok && descriptor.activeState != nil && descriptor.activeState(snapshot)
}

func operationRequiresStageAuthority(operation Operation) bool {
	descriptor, ok := descriptorForOperation(operation)
	return ok && descriptor.authority == operationAuthorityStage
}

var _ Authorizer = (*PostgresAuthorizer)(nil)
