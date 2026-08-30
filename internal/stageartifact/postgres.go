package stageartifact

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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageworkertransport"
)

type SealCommand struct {
	CommandID              uuid.UUID
	AttemptID              uuid.UUID
	StageRunID             uuid.UUID
	StageAttemptID         uuid.UUID
	StageAllocationID      uuid.UUID
	StageLeaseID           uuid.UUID
	ExpectedAttemptFence   int64
	ExpectedStageFence     int64
	ExpectedStageVersion   int64
	OutputPort             string
	LocalReceiptID         string
	LocalReceiptDigest     [sha256.Size]byte
	ManifestSHA256         [sha256.Size]byte
	SHA256                 [sha256.Size]byte
	LineageDigest          [sha256.Size]byte
	TokenDigest            [sha256.Size]byte
	SizeBytes              int64
	ArtifactID             uuid.UUID
	MaterializationLeaseID uuid.UUID
	ObjectKey              string
	ContentType            string
	SealedAt               time.Time
	LeaseExpiresAt         time.Time
}

type PostgresRepository struct {
	pool *pgxpool.Pool
}

type SourceLostCommand struct {
	CommandID              uuid.UUID
	MaterializationLeaseID uuid.UUID
	TokenDigest            [sha256.Size]byte
	FailureFingerprint     [sha256.Size]byte
	ConsumedResourceUnits  int64
	LostAt                 time.Time
	RetryAt                time.Time
}

type SourceLostDecision struct {
	StageRunID   uuid.UUID
	State        string
	StageFence   int64
	StageVersion int64
	Replayed     bool
}

func NewPostgresRepository(pool *pgxpool.Pool) (*PostgresRepository, error) {
	if pool == nil {
		return nil, errors.New("StageArtifact database pool is required")
	}
	return &PostgresRepository{pool: pool}, nil
}

func (repository *PostgresRepository) IsActive(
	ctx context.Context,
	identity stageworkertransport.Identity,
	sessionEpoch int64,
	verified materializationauthority.Verified,
) (bool, error) {
	if repository == nil || repository.pool == nil {
		return false, errors.New("StageArtifact repository is not configured")
	}
	if ctx == nil || identity.SPIFFEID == "" || sessionEpoch <= 0 ||
		verified.Authority == nil || verified.Digest == [sha256.Size]byte{} {
		return false, errors.New("MaterializationAuthority check is incomplete")
	}
	digest, err := materializationauthority.Digest(verified.Authority)
	if err != nil || digest != verified.Digest {
		return false, errors.New("verified MaterializationAuthority digest is inconsistent")
	}
	spiffeDigest := sha256.Sum256([]byte(identity.SPIFFEID))
	if !bytes.Equal(spiffeDigest[:], verified.Authority.GetSourceSpiffeIdDigest()) {
		return false, nil
	}
	leaseID, err := uuid.Parse(verified.Authority.GetStageMaterializationLeaseId())
	if err != nil {
		return false, nil
	}
	artifactID, err := uuid.Parse(verified.Authority.GetStageArtifactId())
	if err != nil {
		return false, nil
	}
	workerID, err := uuid.Parse(verified.Authority.GetSourceWorkerInstanceId())
	if err != nil {
		return false, nil
	}
	memberID, err := uuid.Parse(verified.Authority.GetSourceWorkerMemberId())
	if err != nil {
		return false, nil
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version":               1,
		"materialization_lease_id":     leaseID,
		"artifact_id":                  artifactID,
		"token_digest":                 hex.EncodeToString(digest[:]),
		"source_worker_instance_id":    workerID,
		"source_worker_instance_epoch": verified.Authority.GetSourceWorkerInstanceEpoch(),
		"source_worker_member_id":      memberID,
		"source_worker_member_epoch":   verified.Authority.GetSourceWorkerMemberEpoch(),
		"control_session_epoch":        sessionEpoch,
	})
	if err != nil {
		return false, fmt.Errorf("encode MaterializationAuthority check: %w", err)
	}
	var active bool
	if err := repository.pool.QueryRow(ctx, `
		SELECT vela_is_stage_materialization_authority_active($1::jsonb)
	`, payload).Scan(&active); err != nil {
		return false, fmt.Errorf("check active MaterializationAuthority: %w", err)
	}
	return active, nil
}

func (repository *PostgresRepository) Seal(
	ctx context.Context,
	command SealCommand,
) (MaterializationLease, error) {
	if repository == nil || repository.pool == nil {
		return MaterializationLease{}, errors.New("StageArtifact repository is not configured")
	}
	if err := validateSealCommand(command); err != nil {
		return MaterializationLease{}, err
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version":           1,
		"command_id":               command.CommandID,
		"attempt_id":               command.AttemptID,
		"stage_run_id":             command.StageRunID,
		"stage_attempt_id":         command.StageAttemptID,
		"stage_allocation_id":      command.StageAllocationID,
		"stage_lease_id":           command.StageLeaseID,
		"expected_attempt_fence":   command.ExpectedAttemptFence,
		"expected_stage_fence":     command.ExpectedStageFence,
		"expected_stage_version":   command.ExpectedStageVersion,
		"output_port":              command.OutputPort,
		"local_receipt_id":         command.LocalReceiptID,
		"local_receipt_digest":     hex.EncodeToString(command.LocalReceiptDigest[:]),
		"manifest_sha256":          hex.EncodeToString(command.ManifestSHA256[:]),
		"sha256":                   hex.EncodeToString(command.SHA256[:]),
		"lineage_digest":           hex.EncodeToString(command.LineageDigest[:]),
		"token_digest":             hex.EncodeToString(command.TokenDigest[:]),
		"size_bytes":               command.SizeBytes,
		"artifact_id":              command.ArtifactID,
		"materialization_lease_id": command.MaterializationLeaseID,
		"object_key":               command.ObjectKey,
		"content_type":             command.ContentType,
		"sealed_at":                command.SealedAt.UTC().Format(time.RFC3339Nano),
		"lease_expires_at":         command.LeaseExpiresAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return MaterializationLease{}, fmt.Errorf("encode StageArtifact seal command: %w", err)
	}
	var lease MaterializationLease
	var digest []byte
	var replayed bool
	err = repository.pool.QueryRow(ctx, `
		SELECT materialization_lease_id, artifact_id, object_key, content_type,
		       expected_sha256, expected_size_bytes, issued_at, expires_at, replayed
		FROM vela_seal_stage_output($1::jsonb)
	`, payload).Scan(
		&lease.ID,
		&lease.ArtifactID,
		&lease.ObjectKey,
		&lease.ContentType,
		&digest,
		&lease.SizeBytes,
		&lease.IssuedAt,
		&lease.ExpiresAt,
		&replayed,
	)
	if err != nil {
		return MaterializationLease{}, fmt.Errorf("seal StageArtifact output: %w", err)
	}
	if len(digest) != sha256.Size {
		return MaterializationLease{}, errors.New("sealed StageMaterializationLease digest is malformed")
	}
	copy(lease.SHA256[:], digest)
	lease.TokenDigest = command.TokenDigest
	return lease, nil
}

func (repository *PostgresRepository) Commit(
	ctx context.Context,
	command CommitCommand,
) (Artifact, error) {
	if repository == nil || repository.pool == nil {
		return Artifact{}, errors.New("StageArtifact repository is not configured")
	}
	if err := validateCommitCommand(command); err != nil {
		return Artifact{}, err
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version":           1,
		"command_id":               command.CommandID,
		"progress_receipt_id":      command.ProgressReceiptID,
		"materialization_lease_id": command.MaterializationLeaseID,
		"artifact_id":              command.ArtifactID,
		"object_key":               command.ObjectKey,
		"object_version":           command.ObjectVersion,
		"sha256":                   hex.EncodeToString(command.SHA256[:]),
		"size_bytes":               command.SizeBytes,
		"token_digest":             hex.EncodeToString(command.TokenDigest[:]),
		"committed_at":             command.CommittedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return Artifact{}, fmt.Errorf("encode StageArtifact commit command: %w", err)
	}
	var artifact Artifact
	var digest []byte
	var replayed bool
	err = repository.pool.QueryRow(ctx, `
		SELECT artifact_id, object_key, object_version, sha256,
		       size_bytes, committed_at, replayed
		FROM vela_commit_stage_artifact($1::jsonb)
	`, payload).Scan(
		&artifact.ID,
		&artifact.ObjectKey,
		&artifact.ObjectVersion,
		&digest,
		&artifact.SizeBytes,
		&artifact.CommittedAt,
		&replayed,
	)
	if err != nil {
		return Artifact{}, fmt.Errorf("commit StageArtifact: %w", err)
	}
	if len(digest) != sha256.Size {
		return Artifact{}, errors.New("committed StageArtifact digest is malformed")
	}
	copy(artifact.SHA256[:], digest)
	return artifact, nil
}

func (repository *PostgresRepository) FailSourceLost(
	ctx context.Context,
	command SourceLostCommand,
) (SourceLostDecision, error) {
	if repository == nil || repository.pool == nil {
		return SourceLostDecision{}, errors.New("StageArtifact repository is not configured")
	}
	if command.CommandID == uuid.Nil || command.MaterializationLeaseID == uuid.Nil ||
		command.TokenDigest == [sha256.Size]byte{} ||
		command.FailureFingerprint == [sha256.Size]byte{} ||
		command.ConsumedResourceUnits <= 0 || command.LostAt.IsZero() ||
		!command.RetryAt.After(command.LostAt) {
		return SourceLostDecision{}, errors.New("StageArtifact source-loss authority is incomplete")
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version":           1,
		"command_id":               command.CommandID,
		"materialization_lease_id": command.MaterializationLeaseID,
		"token_digest":             hex.EncodeToString(command.TokenDigest[:]),
		"failure_fingerprint":      hex.EncodeToString(command.FailureFingerprint[:]),
		"consumed_resource_units":  command.ConsumedResourceUnits,
		"lost_at":                  command.LostAt.UTC().Format(time.RFC3339Nano),
		"retry_at":                 command.RetryAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return SourceLostDecision{}, fmt.Errorf("encode StageArtifact source-loss command: %w", err)
	}
	var decision SourceLostDecision
	err = repository.pool.QueryRow(ctx, `
		SELECT stage_run_id, stage_state, stage_fence, stage_version, replayed
		FROM vela_fail_stage_materialization_source($1::jsonb)
	`, payload).Scan(
		&decision.StageRunID,
		&decision.State,
		&decision.StageFence,
		&decision.StageVersion,
		&decision.Replayed,
	)
	if err != nil {
		return SourceLostDecision{}, fmt.Errorf("fail lost StageArtifact source: %w", err)
	}
	return decision, nil
}

func validateSealCommand(command SealCommand) error {
	if command.CommandID == uuid.Nil || command.AttemptID == uuid.Nil ||
		command.StageRunID == uuid.Nil || command.StageAttemptID == uuid.Nil ||
		command.StageAllocationID == uuid.Nil || command.StageLeaseID == uuid.Nil ||
		command.ArtifactID == uuid.Nil || command.MaterializationLeaseID == uuid.Nil {
		return errors.New("StageArtifact seal identities are required")
	}
	if command.ExpectedAttemptFence <= 0 || command.ExpectedStageFence <= 0 ||
		command.ExpectedStageVersion <= 0 || command.OutputPort == "" ||
		len(command.OutputPort) > 100 || command.LocalReceiptID == "" ||
		len(command.LocalReceiptID) > 1000 || command.SizeBytes <= 0 ||
		command.ObjectKey == "" || command.ContentType == "" {
		return errors.New("StageArtifact seal authority or output is incomplete")
	}
	if command.LocalReceiptDigest == [sha256.Size]byte{} ||
		command.ManifestSHA256 == [sha256.Size]byte{} ||
		command.SHA256 == [sha256.Size]byte{} || command.LineageDigest == [sha256.Size]byte{} ||
		command.TokenDigest == [sha256.Size]byte{} {
		return errors.New("StageArtifact seal integrity evidence is incomplete")
	}
	if command.SealedAt.IsZero() || !command.LeaseExpiresAt.After(command.SealedAt) {
		return errors.New("StageArtifact seal deadline is invalid")
	}
	return nil
}

func validateCommitCommand(command CommitCommand) error {
	if command.CommandID == uuid.Nil || command.ProgressReceiptID == uuid.Nil ||
		command.MaterializationLeaseID == uuid.Nil || command.ArtifactID == uuid.Nil ||
		command.ObjectKey == "" || command.ObjectVersion == "" ||
		command.SHA256 == [sha256.Size]byte{} || command.SizeBytes <= 0 ||
		command.TokenDigest == [sha256.Size]byte{} ||
		command.CommittedAt.IsZero() {
		return errors.New("StageArtifact commit authority is incomplete")
	}
	return nil
}

func (repository *PostgresRepository) Issue(
	ctx context.Context,
	command IssueTransferCommand,
) (IssuedTransferTicket, error) {
	if repository == nil || repository.pool == nil {
		return IssuedTransferTicket{}, errors.New("StageArtifact repository is not configured")
	}
	request := command.IssueTransferRequest
	if request.CommandID == uuid.Nil || request.TicketID == uuid.Nil ||
		request.ArtifactID == uuid.Nil || request.PinID == uuid.Nil ||
		command.TokenDigest == [sha256.Size]byte{} ||
		validateTransferTicketClaims(TransferTicketClaims{
			TicketID: request.TicketID, Destination: request.Destination,
			IssuedAt: request.IssuedAt, ExpiresAt: request.ExpiresAt,
		}) != nil {
		return IssuedTransferTicket{}, errors.New("TransferTicket issue authority is incomplete")
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version":                    1,
		"command_id":                        request.CommandID,
		"ticket_id":                         request.TicketID,
		"artifact_id":                       request.ArtifactID,
		"pin_id":                            request.PinID,
		"destination_worker_instance_id":    request.Destination.WorkerInstanceID,
		"destination_worker_instance_epoch": request.Destination.WorkerInstanceEpoch,
		"destination_model_residency_id":    request.Destination.ModelResidencyID,
		"destination_model_runtime_epoch":   request.Destination.ModelRuntimeEpoch,
		"connector_revision_id":             request.Destination.ConnectorRevisionID,
		"token_digest":                      hex.EncodeToString(command.TokenDigest[:]),
		"issued_at":                         request.IssuedAt.UTC().Format(time.RFC3339Nano),
		"expires_at":                        request.ExpiresAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return IssuedTransferTicket{}, fmt.Errorf("encode TransferTicket issue command: %w", err)
	}
	var issued IssuedTransferTicket
	err = repository.pool.QueryRow(ctx, `
		SELECT ticket_id, expires_at, replayed
		FROM vela_issue_stage_transfer_ticket($1::jsonb)
	`, payload).Scan(&issued.TicketID, &issued.ExpiresAt, &issued.Replayed)
	if err != nil {
		return IssuedTransferTicket{}, fmt.Errorf("issue TransferTicket: %w", err)
	}
	return issued, nil
}

func (repository *PostgresRepository) Resolve(
	ctx context.Context,
	command ResolveTransferCommand,
) (TransferDescriptor, error) {
	if repository == nil || repository.pool == nil {
		return TransferDescriptor{}, errors.New("StageArtifact repository is not configured")
	}
	if command.TicketID == uuid.Nil || command.TokenDigest == [sha256.Size]byte{} ||
		command.ResolvedAt.IsZero() || command.Destination.WorkerInstanceID == uuid.Nil ||
		command.Destination.WorkerInstanceEpoch <= 0 ||
		command.Destination.ModelResidencyID == uuid.Nil ||
		command.Destination.ModelRuntimeEpoch <= 0 ||
		command.Destination.ConnectorRevisionID == uuid.Nil {
		return TransferDescriptor{}, errors.New("TransferTicket resolve authority is incomplete")
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version":                    1,
		"ticket_id":                         command.TicketID,
		"token_digest":                      hex.EncodeToString(command.TokenDigest[:]),
		"destination_worker_instance_id":    command.Destination.WorkerInstanceID,
		"destination_worker_instance_epoch": command.Destination.WorkerInstanceEpoch,
		"destination_model_residency_id":    command.Destination.ModelResidencyID,
		"destination_model_runtime_epoch":   command.Destination.ModelRuntimeEpoch,
		"connector_revision_id":             command.Destination.ConnectorRevisionID,
		"resolved_at":                       command.ResolvedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return TransferDescriptor{}, fmt.Errorf("encode TransferTicket resolve command: %w", err)
	}
	var descriptor TransferDescriptor
	var digest []byte
	err = repository.pool.QueryRow(ctx, `
		SELECT ticket_id, artifact_id, object_key, object_version,
		       sha256, size_bytes, content_type
		FROM vela_resolve_stage_transfer_ticket($1::jsonb)
	`, payload).Scan(
		&descriptor.TicketID,
		&descriptor.ArtifactID,
		&descriptor.ObjectKey,
		&descriptor.ObjectVersion,
		&digest,
		&descriptor.SizeBytes,
		&descriptor.ContentType,
	)
	if err != nil {
		return TransferDescriptor{}, fmt.Errorf("resolve TransferTicket: %w", err)
	}
	if len(digest) != sha256.Size {
		return TransferDescriptor{}, errors.New("resolved TransferTicket digest is malformed")
	}
	copy(descriptor.SHA256[:], digest)
	return descriptor, nil
}

func (repository *PostgresRepository) Consume(
	ctx context.Context,
	command ConsumeTransferCommand,
) error {
	if repository == nil || repository.pool == nil {
		return errors.New("StageArtifact repository is not configured")
	}
	if command.CommandID == uuid.Nil || command.TicketID == uuid.Nil ||
		command.TokenDigest == [sha256.Size]byte{} ||
		command.OutcomeDigest == [sha256.Size]byte{} || command.ConsumedAt.IsZero() {
		return errors.New("TransferTicket consume authority is incomplete")
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version": 1,
		"command_id":     command.CommandID,
		"ticket_id":      command.TicketID,
		"token_digest":   hex.EncodeToString(command.TokenDigest[:]),
		"outcome_digest": hex.EncodeToString(command.OutcomeDigest[:]),
		"consumed_at":    command.ConsumedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return fmt.Errorf("encode TransferTicket consume command: %w", err)
	}
	var ticketID uuid.UUID
	var consumedAt time.Time
	var replayed bool
	err = repository.pool.QueryRow(ctx, `
		SELECT ticket_id, consumed_at, replayed
		FROM vela_consume_stage_transfer_ticket($1::jsonb)
	`, payload).Scan(&ticketID, &consumedAt, &replayed)
	if err != nil {
		return fmt.Errorf("consume TransferTicket: %w", err)
	}
	if ticketID != command.TicketID || !consumedAt.Equal(command.ConsumedAt) {
		return errors.New("consumed TransferTicket identity is mismatched")
	}
	return nil
}
