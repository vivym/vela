package stageworkercontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// TerminalStageHistory contains authenticated historical facts, not permission to
// execute or delete files. Original assignment content never leaves this reader.
type TerminalStageHistory struct {
	OrganizationID          uuid.UUID             `json:"organization_id"`
	ProjectID               uuid.UUID             `json:"project_id"`
	JobID                   uuid.UUID             `json:"job_id"`
	AttemptID               uuid.UUID             `json:"attempt_id"`
	StageRunID              uuid.UUID             `json:"stage_run_id"`
	TerminalState           string                `json:"terminal_state"`
	StageFence              int64                 `json:"stage_fence"`
	StageVersion            int64                 `json:"stage_version"`
	WorkerInstanceID        uuid.UUID             `json:"worker_instance_id"`
	WorkerInstanceEpoch     int64                 `json:"worker_instance_epoch"`
	WorkerMemberID          uuid.UUID             `json:"worker_member_id"`
	ControlSessionEpoch     int64                 `json:"control_session_epoch"`
	DeviceSetDigest         [sha256.Size]byte     `json:"device_set_digest"`
	MembershipDigest        [sha256.Size]byte     `json:"membership_digest"`
	Devices                 []TerminalStageDevice `json:"devices"`
	OriginalAuthorityDigest [sha256.Size]byte     `json:"-"`
	Cutoff                  int64                 `json:"cutoff"`
	Allocations             []TerminalAllocation  `json:"allocations"`
	ObservedAt              time.Time             `json:"observed_at"`
}

type TerminalStageDevice struct {
	ID    uuid.UUID `json:"device_id"`
	Epoch int64     `json:"device_epoch"`
}

type TerminalAllocation struct {
	StageAttemptID         uuid.UUID         `json:"stage_attempt_id"`
	StageAllocationID      uuid.UUID         `json:"stage_allocation_id"`
	StageLeaseID           uuid.UUID         `json:"stage_lease_id"`
	ExecutionSequence      int64             `json:"execution_sequence"`
	ExecutionNonce         [sha256.Size]byte `json:"execution_nonce"`
	ModelResidencyID       uuid.UUID         `json:"model_residency_id"`
	ModelRuntimeIdentity   string            `json:"model_runtime_identity"`
	BarrierGeneration      int64             `json:"barrier_generation"`
	StageProfileRevisionID uuid.UUID         `json:"stage_profile_revision_id"`
	Members                []TerminalMember  `json:"members"`
}

type TerminalMember struct {
	WorkerMemberID     uuid.UUID         `json:"worker_member_id"`
	MemberEpoch        int64             `json:"member_epoch"`
	ModelRuntimeEpoch  int64             `json:"model_runtime_epoch"`
	IdentityDigest     [sha256.Size]byte `json:"identity_digest"`
	DeviceSubsetDigest [sha256.Size]byte `json:"device_subset_digest"`
}

type PostgresTerminalHistoryReader struct {
	pool      *pgxpool.Pool
	validator *stageauthority.Validator
}

func NewPostgresTerminalHistoryReader(pool *pgxpool.Pool, validator *stageauthority.Validator) (*PostgresTerminalHistoryReader, error) {
	if pool == nil || validator == nil {
		return nil, errors.New("terminal Stage history database and signature validator are required")
	}
	return &PostgresTerminalHistoryReader{pool: pool, validator: validator}, nil
}

// Read accepts expired historical authority, but never future-issued authority.
// A nil history with no error means that complete matching history is unavailable.
func (reader *PostgresTerminalHistoryReader) Read(
	ctx context.Context, command CommandContext, authority *velav1.StageAuthority, acquireID uuid.UUID,
) (*TerminalStageHistory, error) {
	if reader == nil || reader.pool == nil || reader.validator == nil || ctx == nil ||
		command.Identity.SPIFFEID == "" || command.ControlSessionEpoch <= 0 {
		return nil, errors.New("terminal Stage history reader or caller is incomplete")
	}
	verified, err := reader.validator.ValidateEnvelopeForReplay(authority, 0)
	if err != nil {
		return nil, fmt.Errorf("authenticate terminal Stage history: %w", err)
	}
	authority = verified.Authority
	if authority.GetSchemaVersion() != stageauthority.SchemaVersionV2 {
		return nil, nil
	}
	tokenDigest := sha256.Sum256(authority.GetLeaseToken())
	spiffeDigest := sha256.Sum256([]byte(command.Identity.SPIFFEID))
	request := map[string]any{
		"schema_version": 1, "job_id": authority.GetJobId(), "attempt_id": authority.GetAttemptId(),
		"stage_run_id": authority.GetStageRunId(), "stage_attempt_id": authority.GetStageAttemptId(),
		"stage_allocation_id": authority.GetStageAllocationId(), "stage_lease_id": authority.GetStageLeaseId(),
		"worker_instance_id": authority.GetWorkerInstanceId(), "worker_instance_epoch": authority.GetWorkerInstanceEpoch(),
		"control_session_epoch": command.ControlSessionEpoch, "execution_sequence": authority.GetExecutionSequence(),
		"authority_digest": hex.EncodeToString(verified.Digest[:]), "token_digest": hex.EncodeToString(tokenDigest[:]),
		"spiffe_id_digest": hex.EncodeToString(spiffeDigest[:]), "model_residency_id": authority.GetModelResidencyId(),
		"model_runtime_barrier_generation": authority.GetModelRuntimeBarrierGeneration(),
		"stage_profile_revision_id":        authority.GetStageProfileRevisionId(),
		"capacity_observation_sequence":    authority.GetCapacityObservationSequence(),
	}
	if acquireID != uuid.Nil {
		request["acquire_command_id"] = acquireID
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode terminal Stage history request: %w", err)
	}
	tx, err := reader.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin terminal Stage history snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var raw []byte
	if err := tx.QueryRow(ctx, `SELECT vela_read_stage_terminal_history($1::jsonb)`, payload).Scan(&raw); err != nil {
		return nil, fmt.Errorf("read terminal Stage history: %w", err)
	}
	if len(raw) > 12<<20 {
		return nil, errors.New("terminal Stage history exceeds its bounded format")
	}
	defer clear(raw)
	var row terminalHistoryRow
	if err := json.Unmarshal(raw, &row); err != nil {
		return nil, errors.New("terminal Stage history has an invalid format")
	}
	if row.SchemaVersion != 1 {
		return nil, errors.New("terminal Stage history schema is unsupported")
	}
	if !row.Eligible {
		return nil, nil
	}
	if row.Reason != "HISTORY_COMPLETE" {
		return nil, errors.New("terminal Stage history lacks a complete history result")
	}
	stored, err := decodeTerminalStoredAuthority(row.AssignmentWire, row.RenewalWire)
	if err != nil {
		return nil, err
	}
	recorded, err := reader.validator.ValidateEnvelopeForReplay(stored, 0)
	if err != nil || recorded.Digest != verified.Digest {
		return nil, nil
	}
	history, err := row.decode()
	if err != nil {
		return nil, err
	}
	if !history.matches(command, authority, spiffeDigest) {
		return nil, nil
	}
	history.OriginalAuthorityDigest = verified.Digest
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit terminal Stage history snapshot: %w", err)
	}
	return history, nil
}

// Shadow only SQL's hex fields; the returned facts contain fixed-size digests.
type terminalHistoryRow struct {
	TerminalStageHistory
	SchemaVersion    int                     `json:"schema_version"`
	Eligible         bool                    `json:"eligible"`
	Reason           string                  `json:"reason"`
	DeviceSetDigest  string                  `json:"device_set_digest"`
	MembershipDigest string                  `json:"membership_digest"`
	Allocations      []terminalAllocationRow `json:"allocations"`
	AssignmentWire   string                  `json:"assignment_wire"`
	RenewalWire      string                  `json:"renewal_wire"`
}

type terminalAllocationRow struct {
	TerminalAllocation
	ExecutionNonce string              `json:"execution_nonce"`
	Members        []terminalMemberRow `json:"members"`
}

type terminalMemberRow struct {
	TerminalMember
	IdentityDigest     string `json:"identity_digest"`
	DeviceSubsetDigest string `json:"device_subset_digest"`
}

func decodeTerminalStoredAuthority(assignmentWire, renewalWire string) (*velav1.StageAuthority, error) {
	if (assignmentWire == "") == (renewalWire == "") {
		return nil, errors.New("terminal Stage history has ambiguous original evidence")
	}
	encoded := renewalWire
	if assignmentWire != "" {
		encoded = assignmentWire
	}
	if len(encoded) > 8<<20 {
		return nil, errors.New("terminal Stage original evidence exceeds its bounded format")
	}
	wire, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, errors.New("terminal Stage original evidence has an invalid encoding")
	}
	defer clear(wire)
	if assignmentWire != "" {
		var assignment velav1.StageAssignment
		if err := proto.Unmarshal(wire, &assignment); err != nil {
			return nil, errors.New("terminal Stage original assignment cannot be decoded")
		}
		return assignment.GetAuthority(), nil
	}
	var authority velav1.StageAuthority
	if err := proto.Unmarshal(wire, &authority); err != nil {
		return nil, errors.New("terminal Stage original renewal cannot be decoded")
	}
	return &authority, nil
}

func (row terminalHistoryRow) decode() (*TerminalStageHistory, error) {
	history := row.TerminalStageHistory
	if !decodeTerminalDigest(row.DeviceSetDigest, &history.DeviceSetDigest) ||
		!decodeTerminalDigest(row.MembershipDigest, &history.MembershipDigest) {
		return nil, errors.New("terminal Stage history topology digest is invalid")
	}
	for _, allocationRow := range row.Allocations {
		allocation := allocationRow.TerminalAllocation
		if !decodeTerminalDigest(allocationRow.ExecutionNonce, &allocation.ExecutionNonce) {
			return nil, errors.New("terminal Stage history allocation nonce is invalid")
		}
		for _, memberRow := range allocationRow.Members {
			member := memberRow.TerminalMember
			if !decodeTerminalDigest(memberRow.IdentityDigest, &member.IdentityDigest) ||
				!decodeTerminalDigest(memberRow.DeviceSubsetDigest, &member.DeviceSubsetDigest) {
				return nil, errors.New("terminal Stage history member digest is invalid")
			}
			allocation.Members = append(allocation.Members, member)
		}
		history.Allocations = append(history.Allocations, allocation)
	}
	return &history, nil
}

func decodeTerminalDigest(encoded string, digest *[sha256.Size]byte) bool {
	if len(encoded) != hex.EncodedLen(len(digest)) {
		return false
	}
	_, err := hex.Decode(digest[:], []byte(encoded))
	return err == nil
}

func (history *TerminalStageHistory) matches(command CommandContext, authority *velav1.StageAuthority, spiffeDigest [sha256.Size]byte) bool {
	if history.OrganizationID == uuid.Nil || history.ProjectID == uuid.Nil || history.ObservedAt.IsZero() ||
		history.JobID != uuid.MustParse(authority.GetJobId()) || history.AttemptID != uuid.MustParse(authority.GetAttemptId()) ||
		history.StageRunID != uuid.MustParse(authority.GetStageRunId()) || history.WorkerInstanceID != uuid.MustParse(authority.GetWorkerInstanceId()) ||
		history.WorkerInstanceEpoch != authority.GetWorkerInstanceEpoch() || history.ControlSessionEpoch != command.ControlSessionEpoch ||
		history.StageFence < authority.GetStageFence() || history.StageVersion <= authority.GetStageVersion() ||
		!bytes.Equal(history.DeviceSetDigest[:], authority.GetDeviceSetDigest()) ||
		!bytes.Equal(history.MembershipDigest[:], authority.GetMembershipDigest()) {
		return false
	}
	switch history.TerminalState {
	case "SUCCEEDED", "FAILED", "CANCELED":
	default:
		return false
	}
	if len(history.Devices) != len(authority.GetDevices()) || len(history.Devices) == 0 || len(history.Devices) > 64 ||
		len(history.Allocations) == 0 || len(history.Allocations) > 256 {
		return false
	}
	devices := make(map[uuid.UUID]int64, len(authority.GetDevices()))
	for _, device := range authority.GetDevices() {
		devices[uuid.MustParse(device.GetDeviceId())] = device.GetDeviceEpoch()
	}
	for _, device := range history.Devices {
		if epoch, ok := devices[device.ID]; !ok || epoch != device.Epoch {
			return false
		}
		delete(devices, device.ID)
	}
	members := make(map[uuid.UUID]*velav1.StageAuthorityMemberEpoch, len(authority.GetMembers()))
	for _, member := range authority.GetMembers() {
		members[uuid.MustParse(member.GetWorkerMemberId())] = member
	}
	leader := members[history.WorkerMemberID]
	if leader == nil || !bytes.Equal(leader.GetIdentityDigest(), spiffeDigest[:]) {
		return false
	}
	var previous int64
	matched := false
	for _, allocation := range history.Allocations {
		if allocation.StageAttemptID == uuid.Nil || allocation.StageAllocationID == uuid.Nil || allocation.StageLeaseID == uuid.Nil ||
			allocation.ModelResidencyID == uuid.Nil || allocation.StageProfileRevisionID == uuid.Nil || allocation.ModelRuntimeIdentity == "" ||
			allocation.BarrierGeneration <= 0 || allocation.ExecutionSequence <= previous || len(allocation.Members) != len(members) || len(members) > 64 {
			return false
		}
		previous = allocation.ExecutionSequence
		original := allocation.StageAllocationID == uuid.MustParse(authority.GetStageAllocationId())
		seen := make(map[uuid.UUID]bool, len(allocation.Members))
		for _, member := range allocation.Members {
			expected := members[member.WorkerMemberID]
			if expected == nil || seen[member.WorkerMemberID] || member.MemberEpoch != expected.GetMemberEpoch() || member.ModelRuntimeEpoch <= 0 ||
				!bytes.Equal(member.IdentityDigest[:], expected.GetIdentityDigest()) ||
				(original && member.ModelRuntimeEpoch != expected.GetModelRuntimeEpoch()) {
				return false
			}
			seen[member.WorkerMemberID] = true
		}
		if original {
			if allocation.StageAttemptID != uuid.MustParse(authority.GetStageAttemptId()) || allocation.StageLeaseID != uuid.MustParse(authority.GetStageLeaseId()) ||
				allocation.ExecutionSequence != authority.GetExecutionSequence() || !bytes.Equal(allocation.ExecutionNonce[:], authority.GetExecutionNonce()) ||
				allocation.ModelResidencyID != uuid.MustParse(authority.GetModelResidencyId()) || allocation.ModelRuntimeIdentity != authority.GetModelRuntimeIdentity() ||
				allocation.BarrierGeneration != authority.GetModelRuntimeBarrierGeneration() || allocation.StageProfileRevisionID != uuid.MustParse(authority.GetStageProfileRevisionId()) {
				return false
			}
			matched = true
		}
	}
	return matched && history.Cutoff == previous
}
