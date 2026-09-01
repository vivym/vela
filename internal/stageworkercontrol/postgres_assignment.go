package stageworkercontrol

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageassignment"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stagescheduler"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type AssignmentScheduler interface {
	AcquireIdentified(
		context.Context,
		stagescheduler.WorkerAuthority,
		stagescheduler.CapacityObservation,
		stagescheduler.AssignmentIdentity,
	) (stagescheduler.Assignment, bool, error)
}

type AssignmentTransferTicketIssuer interface {
	Issue(
		context.Context,
		stageartifact.IssueTransferRequest,
	) (stageartifact.SignedTransferTicket, error)
}

type PostgresAssignmentConfig struct {
	Pool               *pgxpool.Pool
	Scheduler          AssignmentScheduler
	AuthoritySigner    *stageauthority.Signer
	TransferTickets    AssignmentTransferTicketIssuer
	IdentityKey        []byte
	NoWorkRetry        time.Duration
	MemberStartTimeout time.Duration
	TransferTicketTTL  time.Duration
}

type PostgresAssignmentBackend struct {
	pool               *pgxpool.Pool
	scheduler          AssignmentScheduler
	authoritySigner    *stageauthority.Signer
	transferTickets    AssignmentTransferTicketIssuer
	identityKey        []byte
	noWorkRetry        time.Duration
	memberStartTimeout time.Duration
	transferTicketTTL  time.Duration
}

func NewPostgresAssignmentBackend(
	config PostgresAssignmentConfig,
) (*PostgresAssignmentBackend, error) {
	if config.Pool == nil || config.Scheduler == nil || config.AuthoritySigner == nil ||
		config.TransferTickets == nil || len(config.IdentityKey) < sha256.Size {
		return nil, errors.New("Stage Worker assignment backend configuration is incomplete")
	}
	if config.NoWorkRetry <= 0 || config.NoWorkRetry > maxNoWorkRetry ||
		config.MemberStartTimeout <= 0 || config.MemberStartTimeout > 10*time.Minute ||
		config.TransferTicketTTL <= 0 || config.TransferTicketTTL > 15*time.Minute {
		return nil, errors.New("Stage Worker assignment backend deadlines are invalid")
	}
	return &PostgresAssignmentBackend{
		pool: config.Pool, scheduler: config.Scheduler,
		authoritySigner: config.AuthoritySigner, transferTickets: config.TransferTickets,
		identityKey: bytes.Clone(config.IdentityKey), noWorkRetry: config.NoWorkRetry,
		memberStartTimeout: config.MemberStartTimeout,
		transferTicketTTL:  config.TransferTicketTTL,
	}, nil
}

func (backend *PostgresAssignmentBackend) AcquireStage(
	ctx context.Context,
	command CommandContext,
	request *velav1.AcquireStageRequest,
) (AcquireResult, error) {
	if backend == nil || backend.pool == nil || backend.scheduler == nil ||
		backend.authoritySigner == nil || backend.transferTickets == nil || ctx == nil {
		return AcquireResult{}, errors.New("PostgreSQL Stage Worker assignment backend is not configured")
	}
	workerID := parseUUID(request.GetWorkerInstanceId())
	residencyID := parseUUID(request.GetModelResidencyId())
	profileID := parseUUID(request.GetStageProfileRevisionId())
	if command.CommandID == uuid.Nil || command.ControlSessionEpoch <= 0 ||
		strings.TrimSpace(command.Identity.SPIFFEID) == "" || request == nil ||
		workerID == uuid.Nil || residencyID == uuid.Nil || profileID == uuid.Nil ||
		request.GetWorkerInstanceEpoch() <= 0 ||
		request.GetCapacityObservationSequence() <= 0 || request.GetModelRuntimeEpoch() <= 0 {
		return AcquireResult{}, errors.New("Stage Worker acquire authority is incomplete")
	}
	spiffeDigest := sha256.Sum256([]byte(command.Identity.SPIFFEID))
	payload, err := json.Marshal(map[string]any{
		"schema_version":                1,
		"command_id":                    command.CommandID,
		"worker_instance_id":            workerID,
		"worker_instance_epoch":         request.GetWorkerInstanceEpoch(),
		"control_session_epoch":         command.ControlSessionEpoch,
		"capacity_observation_sequence": request.GetCapacityObservationSequence(),
		"model_residency_id":            residencyID,
		"model_runtime_epoch":           request.GetModelRuntimeEpoch(),
		"stage_profile_revision_id":     profileID,
		"spiffe_id_digest":              hex.EncodeToString(spiffeDigest[:]),
	})
	if err != nil {
		return AcquireResult{}, fmt.Errorf("encode Stage Worker acquire intent: %w", err)
	}
	begin, err := backend.begin(ctx, payload)
	if err != nil {
		return AcquireResult{}, err
	}
	if begin.complete {
		return decodeDurableAcquireResult(begin.kind, begin.wire, begin.retryAfterMS, begin.detail)
	}
	authority, decision, detail, err := backend.readWorkerAuthority(ctx, command.CommandID)
	if err != nil {
		return AcquireResult{}, err
	}
	if decision != "AUTHORIZED" {
		return backend.completeNegative(ctx, command.CommandID, decision, detail)
	}
	identity := stagescheduler.AssignmentIdentity{
		ClaimID:           backend.deriveUUID("claim", command.CommandID),
		DecisionID:        backend.deriveUUID("decision", command.CommandID),
		CommandID:         backend.deriveUUID("scheduler-command", command.CommandID),
		StageAttemptID:    backend.deriveUUID("stage-attempt", command.CommandID),
		StageAllocationID: backend.deriveUUID("stage-allocation", command.CommandID),
		StageLeaseID:      backend.deriveUUID("stage-lease", command.CommandID),
		LeaseToken:        backend.deriveSecret("lease-token", command.CommandID),
		ExecutionNonce:    backend.deriveSecret("execution-nonce", command.CommandID),
		IssuedAt:          begin.requestedAt,
	}
	scheduled, assigned, err := backend.scheduler.AcquireIdentified(
		ctx,
		stagescheduler.WorkerAuthority{
			CapacityPoolID: authority.CapacityPoolID, StageProfileRevisionID: authority.StageProfileRevisionID,
			WorkerInstanceID: authority.WorkerInstanceID, WorkerInstanceEpoch: authority.WorkerInstanceEpoch,
			DeviceSetDigest: authority.DeviceSetDigest, MembershipDigest: authority.MembershipDigest,
			ModelResidencyID: authority.ModelResidencyID, ModelRuntimeEpoch: authority.ModelRuntimeEpoch,
			CapacityVector: authority.CapacityVector,
		},
		stagescheduler.CapacityObservation{Sequence: authority.CapacityObservationSequence},
		identity,
	)
	if err != nil {
		if kind, detail, ok := classifyAcquireFailure(err); ok {
			return backend.completeNegative(ctx, command.CommandID, kind, detail)
		}
		return AcquireResult{}, fmt.Errorf("schedule Stage Worker assignment: %w", err)
	}
	if !assigned {
		return backend.complete(
			ctx, command.CommandID, "NO_WORK", nil,
			backend.noWorkRetry.Milliseconds(), "",
		)
	}
	if scheduled.ClaimID != identity.ClaimID || scheduled.StageAttemptID != identity.StageAttemptID ||
		scheduled.StageAllocationID != identity.StageAllocationID ||
		scheduled.StageLeaseID != identity.StageLeaseID || scheduled.LeaseToken != identity.LeaseToken {
		return AcquireResult{}, errors.New("StageScheduler returned mismatched identified assignment")
	}
	execution, err := backend.readExecution(ctx, command.CommandID, scheduled.ClaimID)
	if err != nil {
		if kind, detail, ok := classifyAcquireFailure(err); ok {
			return backend.completeNegative(ctx, command.CommandID, kind, detail)
		}
		return AcquireResult{}, err
	}
	assignment, err := backend.buildAssignment(ctx, command.CommandID, identity, execution)
	if err != nil {
		return AcquireResult{}, err
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(assignment)
	if err != nil {
		return AcquireResult{}, fmt.Errorf("encode durable StageAssignment: %w", err)
	}
	return backend.complete(ctx, command.CommandID, "ASSIGNMENT", wire, 0, "")
}

type beginAcquireResult struct {
	requestedAt  time.Time
	kind         string
	wire         []byte
	retryAfterMS int64
	detail       string
	complete     bool
}

func (backend *PostgresAssignmentBackend) begin(
	ctx context.Context,
	payload []byte,
) (beginAcquireResult, error) {
	var result beginAcquireResult
	var kind, detail sql.NullString
	var retry sql.NullInt64
	if err := backend.pool.QueryRow(ctx, `
		SELECT requested_at, result_kind, assignment_wire, retry_after_ms, detail
		FROM vela_begin_stage_worker_acquire($1::jsonb)
	`, payload).Scan(
		&result.requestedAt, &kind, &result.wire, &retry, &detail,
	); err != nil {
		return beginAcquireResult{}, fmt.Errorf("begin durable Stage Worker acquire: %w", err)
	}
	result.kind = kind.String
	result.retryAfterMS = retry.Int64
	result.detail = detail.String
	result.complete = kind.Valid
	return result, nil
}

type acquireAuthoritySnapshot struct {
	CapacityPoolID                uuid.UUID         `json:"capacity_pool_id"`
	StageProfileRevisionID        uuid.UUID         `json:"stage_profile_revision_id"`
	WorkerInstanceID              uuid.UUID         `json:"worker_instance_id"`
	WorkerInstanceEpoch           int64             `json:"worker_instance_epoch"`
	DeviceSetDigestHex            string            `json:"device_set_digest"`
	MembershipDigestHex           string            `json:"membership_digest"`
	ModelResidencyID              uuid.UUID         `json:"model_residency_id"`
	ModelRuntimeIdentity          string            `json:"model_runtime_identity"`
	ModelRuntimeEpoch             int64             `json:"model_runtime_epoch"`
	ModelRuntimeBarrierGeneration int64             `json:"model_runtime_barrier_generation"`
	CapacityObservationSequence   int64             `json:"capacity_observation_sequence"`
	CapacityVector                map[string]int64  `json:"capacity_vector"`
	Members                       []authorityMember `json:"members"`
	Devices                       []authorityDevice `json:"devices"`
	DeviceSetDigest               []byte            `json:"-"`
	MembershipDigest              []byte            `json:"-"`
}

type authorityMember struct {
	WorkerMemberID    uuid.UUID `json:"worker_member_id"`
	MemberEpoch       int64     `json:"member_epoch"`
	ModelRuntimeEpoch int64     `json:"model_runtime_epoch"`
	IdentityDigestHex string    `json:"identity_digest"`
	IdentityDigest    []byte    `json:"-"`
}

type authorityDevice struct {
	DeviceID    uuid.UUID `json:"device_id"`
	DeviceEpoch int64     `json:"device_epoch"`
}

func (backend *PostgresAssignmentBackend) readWorkerAuthority(
	ctx context.Context,
	commandID uuid.UUID,
) (acquireAuthoritySnapshot, string, string, error) {
	var decision, reason string
	var encoded []byte
	if err := backend.pool.QueryRow(ctx, `
		SELECT decision, reason, authority
		FROM vela_read_stage_worker_acquire_authority($1)
	`, commandID).Scan(&decision, &reason, &encoded); err != nil {
		return acquireAuthoritySnapshot{}, "", "", fmt.Errorf(
			"read durable Stage Worker authority: %w", err,
		)
	}
	if decision != "AUTHORIZED" {
		if decision != "STALE" && decision != "REJECTED" || strings.TrimSpace(reason) == "" {
			return acquireAuthoritySnapshot{}, "", "", errors.New(
				"durable Stage Worker authority decision is malformed",
			)
		}
		return acquireAuthoritySnapshot{}, decision, reason, nil
	}
	var authority acquireAuthoritySnapshot
	if err := json.Unmarshal(encoded, &authority); err != nil {
		return acquireAuthoritySnapshot{}, "", "", fmt.Errorf(
			"decode durable Stage Worker authority: %w", err,
		)
	}
	if err := decodeAuthorityMemberDigests(authority.Members); err != nil {
		return acquireAuthoritySnapshot{}, "", "", err
	}
	deviceDigest, err := hex.DecodeString(authority.DeviceSetDigestHex)
	if err != nil {
		return acquireAuthoritySnapshot{}, "", "", errors.New("Stage Worker device digest is malformed")
	}
	membershipDigest, err := hex.DecodeString(authority.MembershipDigestHex)
	if err != nil {
		return acquireAuthoritySnapshot{}, "", "", errors.New("Stage Worker membership digest is malformed")
	}
	authority.DeviceSetDigest = deviceDigest
	authority.MembershipDigest = membershipDigest
	if err := validateAcquireAuthority(authority); err != nil {
		return acquireAuthoritySnapshot{}, "", "", err
	}
	return authority, decision, reason, nil
}

type assignmentInputSnapshot struct {
	StageArtifactID          uuid.UUID `json:"stage_artifact_id"`
	ObjectVersion            string    `json:"object_version"`
	SHA256Hex                string    `json:"sha256"`
	SizeBytes                int64     `json:"size_bytes"`
	StageInterfaceRevisionID uuid.UUID `json:"stage_interface_revision_id"`
	ArtifactExpiresAt        time.Time `json:"artifact_expires_at"`
	PinID                    uuid.UUID `json:"pin_id"`
	ConnectorRevisionID      uuid.UUID `json:"connector_revision_id"`
}

type assignmentExecutionSnapshot struct {
	JobID                         uuid.UUID                 `json:"job_id"`
	AttemptID                     uuid.UUID                 `json:"attempt_id"`
	AttemptFence                  int64                     `json:"attempt_fence"`
	StageRunID                    uuid.UUID                 `json:"stage_run_id"`
	StageFence                    int64                     `json:"stage_fence"`
	StageVersion                  int64                     `json:"stage_version"`
	StageAttemptID                uuid.UUID                 `json:"stage_attempt_id"`
	StageAllocationID             uuid.UUID                 `json:"stage_allocation_id"`
	StageLeaseID                  uuid.UUID                 `json:"stage_lease_id"`
	StageProfileRevisionID        uuid.UUID                 `json:"stage_profile_revision_id"`
	WorkerInstanceID              uuid.UUID                 `json:"worker_instance_id"`
	WorkerInstanceEpoch           int64                     `json:"worker_instance_epoch"`
	DeviceSetDigestHex            string                    `json:"device_set_digest"`
	MembershipDigestHex           string                    `json:"membership_digest"`
	ModelResidencyID              uuid.UUID                 `json:"model_residency_id"`
	ModelRuntimeIdentity          string                    `json:"model_runtime_identity"`
	ModelRuntimeEpoch             int64                     `json:"model_runtime_epoch"`
	ModelRuntimeBarrierGeneration int64                     `json:"model_runtime_barrier_generation"`
	CapacityObservationSequence   int64                     `json:"capacity_observation_sequence"`
	CapacityVector                map[string]int64          `json:"capacity_vector"`
	LeaseTokenDigestHex           string                    `json:"lease_token_digest"`
	ExecutionNonceHex             string                    `json:"execution_nonce"`
	SigningKeyID                  string                    `json:"signing_key_id"`
	IssuedAt                      time.Time                 `json:"issued_at"`
	ExpiresAt                     time.Time                 `json:"expires_at"`
	LocalDeadlineAt               time.Time                 `json:"local_deadline_at"`
	Parameters                    json.RawMessage           `json:"parameters"`
	ExpectedOutputManifest        json.RawMessage           `json:"expected_output_manifest"`
	Members                       []authorityMember         `json:"members"`
	Devices                       []authorityDevice         `json:"devices"`
	Inputs                        []assignmentInputSnapshot `json:"inputs"`
}

func (backend *PostgresAssignmentBackend) readExecution(
	ctx context.Context,
	commandID uuid.UUID,
	claimID uuid.UUID,
) (assignmentExecutionSnapshot, error) {
	var encoded []byte
	if err := backend.pool.QueryRow(ctx, `
		SELECT snapshot FROM vela_read_stage_assignment_execution($1, $2)
	`, commandID, claimID).Scan(&encoded); err != nil {
		return assignmentExecutionSnapshot{}, fmt.Errorf(
			"read durable StageAssignment execution: %w", err,
		)
	}
	var snapshot assignmentExecutionSnapshot
	if err := json.Unmarshal(encoded, &snapshot); err != nil {
		return assignmentExecutionSnapshot{}, fmt.Errorf(
			"decode durable StageAssignment execution: %w", err,
		)
	}
	if err := decodeAuthorityMemberDigests(snapshot.Members); err != nil {
		return assignmentExecutionSnapshot{}, err
	}
	if err := validateExecutionSnapshot(snapshot); err != nil {
		return assignmentExecutionSnapshot{}, err
	}
	return snapshot, nil
}

func (backend *PostgresAssignmentBackend) buildAssignment(
	ctx context.Context,
	commandID uuid.UUID,
	identity stagescheduler.AssignmentIdentity,
	snapshot assignmentExecutionSnapshot,
) (*velav1.StageAssignment, error) {
	if snapshot.StageAttemptID != identity.StageAttemptID ||
		snapshot.StageAllocationID != identity.StageAllocationID ||
		snapshot.StageLeaseID != identity.StageLeaseID || !snapshot.IssuedAt.Equal(identity.IssuedAt) {
		return nil, errors.New("durable StageAssignment identity changed")
	}
	wantTokenDigest, err := hex.DecodeString(snapshot.LeaseTokenDigestHex)
	if err != nil {
		return nil, errors.New("durable StageLease token digest is malformed")
	}
	actualTokenDigest := sha256.Sum256(identity.LeaseToken[:])
	if !bytes.Equal(wantTokenDigest, actualTokenDigest[:]) {
		return nil, errors.New("durable StageLease token does not match acquire identity")
	}
	executionNonce, err := hex.DecodeString(snapshot.ExecutionNonceHex)
	if err != nil || !bytes.Equal(executionNonce, identity.ExecutionNonce[:]) {
		return nil, errors.New("durable StageLease execution nonce does not match acquire identity")
	}
	parameters, err := compactJSON(snapshot.Parameters)
	if err != nil {
		return nil, fmt.Errorf("canonicalize StageExecutionSpec parameters: %w", err)
	}
	manifest, err := compactJSON(snapshot.ExpectedOutputManifest)
	if err != nil {
		return nil, fmt.Errorf("canonicalize StageExecutionSpec output manifest: %w", err)
	}
	spec := &velav1.StageExecutionSpec{
		ParametersJson: parameters, ExpectedOutputManifestJson: manifest,
		Inputs: make([]*velav1.StageInputArtifact, 0, len(snapshot.Inputs)),
	}
	for _, input := range snapshot.Inputs {
		digest, decodeErr := hex.DecodeString(input.SHA256Hex)
		if decodeErr != nil || len(digest) != sha256.Size {
			return nil, errors.New("StageAssignment input digest is malformed")
		}
		spec.Inputs = append(spec.Inputs, &velav1.StageInputArtifact{
			StageArtifactId: input.StageArtifactID.String(), ObjectVersion: input.ObjectVersion,
			Sha256: digest, SizeBytes: input.SizeBytes,
			StageInterfaceRevisionId: input.StageInterfaceRevisionID.String(),
		})
	}
	specDigest, err := stageauthority.ExecutionSpecDigest(spec)
	if err != nil {
		return nil, err
	}
	deviceDigest, _ := hex.DecodeString(snapshot.DeviceSetDigestHex)
	membershipDigest, _ := hex.DecodeString(snapshot.MembershipDigestHex)
	members := make([]*velav1.StageAuthorityMemberEpoch, 0, len(snapshot.Members))
	requiredMembers := make([]string, 0, len(snapshot.Members))
	for _, member := range snapshot.Members {
		members = append(members, &velav1.StageAuthorityMemberEpoch{
			WorkerMemberId: member.WorkerMemberID.String(), MemberEpoch: member.MemberEpoch,
			ModelRuntimeEpoch: member.ModelRuntimeEpoch,
			IdentityDigest:    bytes.Clone(member.IdentityDigest),
		})
		requiredMembers = append(requiredMembers, member.WorkerMemberID.String())
	}
	devices := make([]*velav1.StageAuthorityDeviceEpoch, 0, len(snapshot.Devices))
	for _, device := range snapshot.Devices {
		devices = append(devices, &velav1.StageAuthorityDeviceEpoch{
			DeviceId: device.DeviceID.String(), DeviceEpoch: device.DeviceEpoch,
		})
	}
	authority, err := backend.authoritySigner.Sign(&velav1.StageAuthority{
		SchemaVersion: stageauthority.SchemaVersionV1,
		JobId:         snapshot.JobID.String(), AttemptId: snapshot.AttemptID.String(),
		StageRunId: snapshot.StageRunID.String(), StageAttemptId: snapshot.StageAttemptID.String(),
		StageAllocationId: snapshot.StageAllocationID.String(), StageLeaseId: snapshot.StageLeaseID.String(),
		AttemptFence: snapshot.AttemptFence, StageFence: snapshot.StageFence, StageVersion: snapshot.StageVersion,
		WorkerInstanceId: snapshot.WorkerInstanceID.String(), WorkerInstanceEpoch: snapshot.WorkerInstanceEpoch,
		DeviceSetDigest: deviceDigest, Devices: devices,
		MembershipDigest: membershipDigest, Members: members,
		ModelResidencyId: snapshot.ModelResidencyID.String(), ModelRuntimeIdentity: snapshot.ModelRuntimeIdentity,
		ModelRuntimeBarrierGeneration: snapshot.ModelRuntimeBarrierGeneration,
		StageProfileRevisionId:        snapshot.StageProfileRevisionID.String(),
		CapacityObservationSequence:   snapshot.CapacityObservationSequence,
		CapacityVector:                snapshot.CapacityVector, LeaseToken: identity.LeaseToken[:],
		ExecutionNonce: executionNonce, SigningKeyId: snapshot.SigningKeyID,
		IssuedAt: timestamppb.New(snapshot.IssuedAt), ExpiresAt: timestamppb.New(snapshot.ExpiresAt),
		MonotonicValidFor:   durationpb.New(snapshot.LocalDeadlineAt.Sub(snapshot.IssuedAt)),
		ExecutionSpecDigest: specDigest[:],
	})
	if err != nil {
		return nil, fmt.Errorf("sign StageAssignment authority: %w", err)
	}
	assignment := &velav1.StageAssignment{
		Authority: authority, ExecutionSpec: spec,
		RequiredWorkerMemberIds: requiredMembers,
		MemberStartTimeout:      durationpb.New(backend.memberStartTimeout),
		InputTransferTickets:    make([]*velav1.StageInputTransferTicket, 0, len(snapshot.Inputs)),
	}
	for _, input := range snapshot.Inputs {
		ticketExpiry := minTime(
			snapshot.IssuedAt.Add(backend.transferTicketTTL),
			snapshot.ExpiresAt,
			input.ArtifactExpiresAt.Add(-time.Microsecond),
		)
		if !ticketExpiry.After(snapshot.IssuedAt) {
			return nil, errors.New("StageAssignment input TransferTicket deadline is stale")
		}
		issued, issueErr := backend.transferTickets.Issue(ctx, stageartifact.IssueTransferRequest{
			CommandID:  backend.deriveUUID("transfer-command/"+input.StageArtifactID.String(), commandID),
			TicketID:   backend.deriveUUID("transfer-ticket/"+input.StageArtifactID.String(), commandID),
			ArtifactID: input.StageArtifactID, PinID: input.PinID,
			SigningKeyID: snapshot.SigningKeyID,
			Destination: stageartifact.TransferDestination{
				WorkerInstanceID:    snapshot.WorkerInstanceID,
				WorkerInstanceEpoch: snapshot.WorkerInstanceEpoch,
				ModelResidencyID:    snapshot.ModelResidencyID,
				ModelRuntimeEpoch:   snapshot.ModelRuntimeBarrierGeneration,
				ConnectorRevisionID: input.ConnectorRevisionID,
			},
			IssuedAt: snapshot.IssuedAt, ExpiresAt: ticketExpiry,
		})
		if issueErr != nil {
			return nil, fmt.Errorf("issue exact StageAssignment TransferTicket: %w", issueErr)
		}
		assignment.InputTransferTickets = append(
			assignment.InputTransferTickets,
			&velav1.StageInputTransferTicket{
				StageArtifactId: input.StageArtifactID.String(),
				ObjectVersion:   input.ObjectVersion, TransferTicket: issued.Token,
			},
		)
	}
	if _, err := stageassignment.Validate(assignment); err != nil {
		return nil, fmt.Errorf("validate constructed StageAssignment: %w", err)
	}
	return assignment, nil
}

func (backend *PostgresAssignmentBackend) completeNegative(
	ctx context.Context,
	commandID uuid.UUID,
	kind string,
	detail string,
) (AcquireResult, error) {
	if kind != "STALE" && kind != "REJECTED" {
		return AcquireResult{}, errors.New("Stage Worker acquire negative decision is invalid")
	}
	return backend.complete(ctx, commandID, kind, nil, 0, detail)
}

func (backend *PostgresAssignmentBackend) complete(
	ctx context.Context,
	commandID uuid.UUID,
	kind string,
	wire []byte,
	retryAfterMS int64,
	detail string,
) (AcquireResult, error) {
	payload := map[string]any{
		"schema_version": 1, "command_id": commandID, "result_kind": kind,
	}
	if len(wire) > 0 {
		payload["assignment_wire"] = hex.EncodeToString(wire)
	}
	if retryAfterMS > 0 {
		payload["retry_after_ms"] = retryAfterMS
	}
	if detail != "" {
		payload["detail"] = detail
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return AcquireResult{}, fmt.Errorf("encode Stage Worker acquire result: %w", err)
	}
	var returnedKind string
	var returnedWire []byte
	var returnedRetry sql.NullInt64
	var returnedDetail sql.NullString
	if err := backend.pool.QueryRow(ctx, `
		SELECT result_kind, assignment_wire, retry_after_ms, detail
		FROM vela_complete_stage_worker_acquire($1::jsonb)
	`, encoded).Scan(
		&returnedKind, &returnedWire, &returnedRetry, &returnedDetail,
	); err != nil {
		return AcquireResult{}, fmt.Errorf("complete durable Stage Worker acquire: %w", err)
	}
	return decodeDurableAcquireResult(
		returnedKind, returnedWire, returnedRetry.Int64, returnedDetail.String,
	)
}

func decodeDurableAcquireResult(
	kind string,
	wire []byte,
	retryAfterMS int64,
	detail string,
) (AcquireResult, error) {
	switch kind {
	case "ASSIGNMENT":
		var assignment velav1.StageAssignment
		if len(wire) == 0 || proto.Unmarshal(wire, &assignment) != nil {
			return AcquireResult{}, errors.New("durable StageAssignment wire is malformed")
		}
		if _, err := stageassignment.Validate(&assignment); err != nil {
			return AcquireResult{}, errors.New("durable StageAssignment contract is malformed")
		}
		return AcquireResult{Assignment: &assignment}, nil
	case "NO_WORK":
		retry := time.Duration(retryAfterMS) * time.Millisecond
		if retry <= 0 || retry > maxNoWorkRetry {
			return AcquireResult{}, errors.New("durable Stage Worker no-work retry is malformed")
		}
		return AcquireResult{RetryAfter: retry}, nil
	case "STALE", "REJECTED":
		decision := velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE
		if kind == "REJECTED" {
			decision = velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED
		}
		if strings.TrimSpace(detail) == "" {
			return AcquireResult{}, errors.New("durable Stage Worker rejection is malformed")
		}
		return AcquireResult{Command: &CommandResult{Decision: decision, Detail: detail}}, nil
	default:
		return AcquireResult{}, errors.New("durable Stage Worker acquire result kind is unsupported")
	}
}

func validateAcquireAuthority(authority acquireAuthoritySnapshot) error {
	if authority.CapacityPoolID == uuid.Nil || authority.StageProfileRevisionID == uuid.Nil ||
		authority.WorkerInstanceID == uuid.Nil || authority.ModelResidencyID == uuid.Nil ||
		authority.WorkerInstanceEpoch <= 0 || authority.ModelRuntimeEpoch <= 0 ||
		authority.ModelRuntimeBarrierGeneration <= 0 ||
		authority.ModelRuntimeBarrierGeneration != authority.ModelRuntimeEpoch ||
		authority.CapacityObservationSequence <= 0 || len(authority.DeviceSetDigest) != sha256.Size ||
		len(authority.MembershipDigest) != sha256.Size || len(authority.CapacityVector) == 0 ||
		len(authority.Members) == 0 || len(authority.Devices) == 0 ||
		strings.TrimSpace(authority.ModelRuntimeIdentity) == "" {
		return errors.New("durable Stage Worker authority is incomplete")
	}
	for _, member := range authority.Members {
		if len(member.IdentityDigest) != sha256.Size {
			return errors.New("durable Stage Worker member identity digest is malformed")
		}
	}
	return nil
}

func validateExecutionSnapshot(snapshot assignmentExecutionSnapshot) error {
	if snapshot.JobID == uuid.Nil || snapshot.AttemptID == uuid.Nil || snapshot.StageRunID == uuid.Nil ||
		snapshot.StageAttemptID == uuid.Nil || snapshot.StageAllocationID == uuid.Nil ||
		snapshot.StageLeaseID == uuid.Nil || snapshot.StageProfileRevisionID == uuid.Nil ||
		snapshot.WorkerInstanceID == uuid.Nil || snapshot.ModelResidencyID == uuid.Nil ||
		snapshot.AttemptFence <= 0 || snapshot.StageFence <= 0 || snapshot.StageVersion <= 0 ||
		snapshot.WorkerInstanceEpoch <= 0 || snapshot.ModelRuntimeEpoch <= 0 ||
		snapshot.ModelRuntimeBarrierGeneration <= 0 ||
		snapshot.ModelRuntimeBarrierGeneration != snapshot.ModelRuntimeEpoch ||
		snapshot.CapacityObservationSequence <= 0 || len(snapshot.CapacityVector) == 0 ||
		len(snapshot.Members) == 0 || len(snapshot.Devices) == 0 ||
		strings.TrimSpace(snapshot.SigningKeyID) == "" ||
		snapshot.IssuedAt.IsZero() || !snapshot.ExpiresAt.After(snapshot.IssuedAt) ||
		!snapshot.LocalDeadlineAt.After(snapshot.IssuedAt) ||
		snapshot.LocalDeadlineAt.After(snapshot.ExpiresAt) ||
		len(snapshot.Parameters) == 0 || len(snapshot.ExpectedOutputManifest) == 0 {
		return errors.New("durable StageAssignment execution snapshot is incomplete")
	}
	for _, member := range snapshot.Members {
		if len(member.IdentityDigest) != sha256.Size {
			return errors.New("durable StageAssignment member identity digest is malformed")
		}
	}
	return nil
}

func decodeAuthorityMemberDigests(members []authorityMember) error {
	for index := range members {
		digest, err := hex.DecodeString(members[index].IdentityDigestHex)
		if err != nil || len(digest) != sha256.Size {
			return errors.New("durable Stage Worker member identity digest is malformed")
		}
		members[index].IdentityDigest = digest
	}
	return nil
}

func classifyAcquireFailure(err error) (string, string, bool) {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return "", "", false
	}
	constraint := postgresError.ConstraintName
	if strings.Contains(constraint, "replay_mismatch") || strings.HasSuffix(constraint, "_invalid") {
		return "REJECTED", "Stage Worker acquire was rejected", true
	}
	if strings.Contains(constraint, "stale") ||
		constraint == "stage_scheduler_activation_stopped" ||
		constraint == "stage_scheduler_assignment_rejected" {
		return "STALE", "Stage Worker acquire authority is stale", true
	}
	return "", "", false
}

func (backend *PostgresAssignmentBackend) deriveUUID(label string, commandID uuid.UUID) uuid.UUID {
	material := backend.deriveMaterial(label, commandID)
	material[6] = (material[6] & 0x0f) | 0x50
	material[8] = (material[8] & 0x3f) | 0x80
	derived, _ := uuid.FromBytes(material[:16])
	return derived
}

func (backend *PostgresAssignmentBackend) deriveSecret(
	label string,
	commandID uuid.UUID,
) [sha256.Size]byte {
	return backend.deriveMaterial(label, commandID)
}

func (backend *PostgresAssignmentBackend) deriveMaterial(
	label string,
	commandID uuid.UUID,
) [sha256.Size]byte {
	mac := hmac.New(sha256.New, backend.identityKey)
	_, _ = mac.Write([]byte("vela/stage-worker-acquire/" + label + "\x00"))
	_, _ = mac.Write(commandID[:])
	var material [sha256.Size]byte
	copy(material[:], mac.Sum(nil))
	return material
}

func compactJSON(value []byte) ([]byte, error) {
	var compact bytes.Buffer
	if err := json.Compact(&compact, value); err != nil {
		return nil, err
	}
	return compact.Bytes(), nil
}

func minTime(values ...time.Time) time.Time {
	minimum := values[0]
	for _, value := range values[1:] {
		if value.Before(minimum) {
			minimum = value
		}
	}
	return minimum
}

var _ AssignmentOperations = (*PostgresAssignmentBackend)(nil)
