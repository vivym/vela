package stagecache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	pool *pgxpool.Pool
}

type ProjectControlCommand struct {
	OrganizationID        uuid.UUID
	ProjectID             uuid.UUID
	CachePolicyRevisionID uuid.UUID
	Enabled               bool
	MaxEntries            int
	MaxBytes              int64
	UpdatedAt             time.Time
}

type OrganizationAuthorizationCommand struct {
	OrganizationID        uuid.UUID
	CachePolicyRevisionID uuid.UUID
	Enabled               bool
	MaxEntries            int
	MaxBytes              int64
	UpdatedAt             time.Time
}

type ControlDecision struct {
	Version int64
	Enabled bool
}

type AdmitCommand struct {
	CommandID                   uuid.UUID
	EntryID                     uuid.UUID
	ArtifactID                  uuid.UUID
	CachePolicyRevisionID       uuid.UUID
	StageProfileRevisionID      uuid.UUID
	ResultEquivalenceRevisionID uuid.UUID
	Scope                       Scope
	StageKey                    string
	CacheKeyDigest              Digest
	ExpectedSavedComputeMinor   int64
	CarryCostMinor              int64
	AdmittedAt                  time.Time
	ExpiresAt                   time.Time
}

type AdmitDecision struct {
	EntryID       uuid.UUID
	ArtifactID    uuid.UUID
	ObjectVersion string
	ExpiresAt     time.Time
	Replayed      bool
	Deduplicated  bool
}

type HitCommand struct {
	CommandID              uuid.UUID
	EntryID                uuid.UUID
	PinID                  uuid.UUID
	AttemptID              uuid.UUID
	StageRunID             uuid.UUID
	StageProfileRevisionID uuid.UUID
	ExpectedOrganizationID uuid.UUID
	ExpectedProjectID      uuid.UUID
	ExpectedAttemptFence   int64
	ExpectedStageFence     int64
	ExpectedStageVersion   int64
	ProgressReceiptID      uuid.UUID
	CacheKeyDigest         Digest
	HitAt                  time.Time
}

type HitDecision struct {
	EntryID       uuid.UUID
	ArtifactID    uuid.UUID
	PinID         uuid.UUID
	ObjectVersion string
	SHA256        Digest
	SizeBytes     int64
	StageRunID    uuid.UUID
	StageState    string
	StageFence    int64
	StageVersion  int64
	Replayed      bool
}

type ReleasePinCommand struct {
	CommandID       uuid.UUID
	PinID           uuid.UUID
	OwnerJobID      uuid.UUID
	OwnerStageRunID uuid.UUID
	ReleaseReason   string
	ReleasedAt      time.Time
}

type PinReleaseDecision struct {
	PinID      uuid.UUID
	ReleasedAt time.Time
	Replayed   bool
}

type DeletionCommand struct {
	CommandID      uuid.UUID
	OrganizationID uuid.UUID
	ProjectID      uuid.UUID
	SourceJobID    uuid.UUID
	RequestedAt    time.Time
}

type DeletionDecision struct {
	DeletedCount int64
	BlockedCount int64
	Replayed     bool
}

func NewPostgresRepository(pool *pgxpool.Pool) (*PostgresRepository, error) {
	if pool == nil {
		return nil, errors.New("stage cache database pool is required")
	}
	return &PostgresRepository{pool: pool}, nil
}

func (repository *PostgresRepository) SetProjectControl(
	ctx context.Context,
	command ProjectControlCommand,
) (ControlDecision, error) {
	if err := repository.validate(); err != nil {
		return ControlDecision{}, err
	}
	if ctx == nil || command.OrganizationID == uuid.Nil || command.ProjectID == uuid.Nil ||
		command.CachePolicyRevisionID == uuid.Nil || command.MaxEntries <= 0 ||
		command.MaxBytes <= 0 || command.UpdatedAt.IsZero() {
		return ControlDecision{}, errors.New("project stage cache control is incomplete")
	}
	payload, err := json.Marshal(map[string]any{
		"organization_id":          command.OrganizationID,
		"project_id":               command.ProjectID,
		"cache_policy_revision_id": command.CachePolicyRevisionID,
		"enabled":                  command.Enabled,
		"max_entries":              command.MaxEntries,
		"max_bytes":                command.MaxBytes,
		"updated_at":               command.UpdatedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return ControlDecision{}, fmt.Errorf("encode Project Stage Cache control: %w", err)
	}
	var decision ControlDecision
	if err := repository.pool.QueryRow(ctx, `
		SELECT version, enabled
		FROM vela_set_project_stage_cache_control($1::jsonb)
	`, payload).Scan(&decision.Version, &decision.Enabled); err != nil {
		return ControlDecision{}, fmt.Errorf("set Project Stage Cache control: %w", err)
	}
	return decision, nil
}

func (repository *PostgresRepository) AuthorizeOrganization(
	ctx context.Context,
	command OrganizationAuthorizationCommand,
) (ControlDecision, error) {
	if err := repository.validate(); err != nil {
		return ControlDecision{}, err
	}
	if ctx == nil || command.OrganizationID == uuid.Nil ||
		command.CachePolicyRevisionID == uuid.Nil || command.MaxEntries <= 0 ||
		command.MaxBytes <= 0 || command.UpdatedAt.IsZero() {
		return ControlDecision{}, errors.New("organization stage cache authorization is incomplete")
	}
	payload, err := json.Marshal(map[string]any{
		"organization_id":          command.OrganizationID,
		"cache_policy_revision_id": command.CachePolicyRevisionID,
		"enabled":                  command.Enabled,
		"max_entries":              command.MaxEntries,
		"max_bytes":                command.MaxBytes,
		"updated_at":               command.UpdatedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return ControlDecision{}, fmt.Errorf("encode Organization Stage Cache authorization: %w", err)
	}
	var decision ControlDecision
	if err := repository.pool.QueryRow(ctx, `
		SELECT version, enabled
		FROM vela_set_organization_stage_cache_authorization($1::jsonb)
	`, payload).Scan(&decision.Version, &decision.Enabled); err != nil {
		return ControlDecision{}, fmt.Errorf("set Organization Stage Cache authorization: %w", err)
	}
	return decision, nil
}

func (repository *PostgresRepository) Admit(
	ctx context.Context,
	command AdmitCommand,
) (AdmitDecision, error) {
	return repository.admit(ctx, command, false)
}

func (repository *PostgresRepository) AdmitH3Exact(
	ctx context.Context,
	command AdmitCommand,
) (AdmitDecision, error) {
	return repository.admit(ctx, command, true)
}

func (repository *PostgresRepository) admit(
	ctx context.Context,
	command AdmitCommand,
	h3Exact bool,
) (AdmitDecision, error) {
	if err := repository.validate(); err != nil {
		return AdmitDecision{}, err
	}
	if ctx == nil || command.CommandID == uuid.Nil || command.EntryID == uuid.Nil ||
		command.ArtifactID == uuid.Nil || command.CachePolicyRevisionID == uuid.Nil ||
		command.StageProfileRevisionID == uuid.Nil ||
		command.ResultEquivalenceRevisionID == uuid.Nil ||
		(command.Scope != ScopeProject && command.Scope != ScopeOrganization) ||
		command.StageKey == "" || len(command.StageKey) > 100 ||
		command.CacheKeyDigest == (Digest{}) ||
		command.ExpectedSavedComputeMinor < 0 || command.CarryCostMinor < 0 ||
		command.AdmittedAt.IsZero() || !command.ExpiresAt.After(command.AdmittedAt) {
		return AdmitDecision{}, errors.New("stage cache admission is incomplete")
	}
	payload, err := json.Marshal(map[string]any{
		"command_id":                     command.CommandID,
		"entry_id":                       command.EntryID,
		"artifact_id":                    command.ArtifactID,
		"cache_policy_revision_id":       command.CachePolicyRevisionID,
		"stage_profile_revision_id":      command.StageProfileRevisionID,
		"result_equivalence_revision_id": command.ResultEquivalenceRevisionID,
		"scope":                          command.Scope,
		"stage_key":                      command.StageKey,
		"cache_key_digest":               hex.EncodeToString(command.CacheKeyDigest[:]),
		"expected_saved_compute_minor":   command.ExpectedSavedComputeMinor,
		"carry_cost_minor":               command.CarryCostMinor,
		"admitted_at":                    command.AdmittedAt.UTC().Format(time.RFC3339Nano),
		"expires_at":                     command.ExpiresAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return AdmitDecision{}, fmt.Errorf("encode Stage Cache admission: %w", err)
	}
	var decision AdmitDecision
	query := `
			SELECT entry_id, artifact_id, object_version, expires_at, replayed, deduplicated
			FROM vela_admit_stage_cache_entry($1::jsonb)
		`
	if h3Exact {
		query = `
			SELECT entry_id, artifact_id, object_version, expires_at, replayed, deduplicated
			FROM vela_admit_h3_exact_cache_entry($1::jsonb)
		`
	}
	if err := repository.pool.QueryRow(ctx, query, payload).Scan(
		&decision.EntryID,
		&decision.ArtifactID,
		&decision.ObjectVersion,
		&decision.ExpiresAt,
		&decision.Replayed,
		&decision.Deduplicated,
	); err != nil {
		return AdmitDecision{}, fmt.Errorf("admit Stage Cache entry: %w", err)
	}
	return decision, nil
}

func (repository *PostgresRepository) Hit(
	ctx context.Context,
	command HitCommand,
) (HitDecision, error) {
	if err := repository.validate(); err != nil {
		return HitDecision{}, err
	}
	if ctx == nil || command.CommandID == uuid.Nil || command.EntryID == uuid.Nil ||
		command.PinID == uuid.Nil || command.AttemptID == uuid.Nil ||
		command.StageRunID == uuid.Nil || command.StageProfileRevisionID == uuid.Nil ||
		command.ExpectedOrganizationID == uuid.Nil || command.ExpectedProjectID == uuid.Nil ||
		command.ExpectedAttemptFence <= 0 || command.ExpectedStageFence <= 0 ||
		command.ExpectedStageVersion <= 0 || command.ProgressReceiptID == uuid.Nil ||
		command.CacheKeyDigest == (Digest{}) || command.HitAt.IsZero() {
		return HitDecision{}, errors.New("stage cache hit is incomplete")
	}
	payload, err := json.Marshal(map[string]any{
		"command_id":                command.CommandID,
		"entry_id":                  command.EntryID,
		"pin_id":                    command.PinID,
		"attempt_id":                command.AttemptID,
		"stage_run_id":              command.StageRunID,
		"stage_profile_revision_id": command.StageProfileRevisionID,
		"expected_organization_id":  command.ExpectedOrganizationID,
		"expected_project_id":       command.ExpectedProjectID,
		"expected_attempt_fence":    command.ExpectedAttemptFence,
		"expected_stage_fence":      command.ExpectedStageFence,
		"expected_stage_version":    command.ExpectedStageVersion,
		"progress_receipt_id":       command.ProgressReceiptID,
		"cache_key_digest":          hex.EncodeToString(command.CacheKeyDigest[:]),
		"hit_at":                    command.HitAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return HitDecision{}, fmt.Errorf("encode Stage Cache hit: %w", err)
	}
	var decision HitDecision
	var digest []byte
	if err := repository.pool.QueryRow(ctx, `
		SELECT entry_id, artifact_id, pin_id, object_version, sha256,
		       size_bytes, stage_run_id, stage_state, stage_fence,
		       stage_version, replayed
		FROM vela_hit_stage_cache($1::jsonb)
	`, payload).Scan(
		&decision.EntryID,
		&decision.ArtifactID,
		&decision.PinID,
		&decision.ObjectVersion,
		&digest,
		&decision.SizeBytes,
		&decision.StageRunID,
		&decision.StageState,
		&decision.StageFence,
		&decision.StageVersion,
		&decision.Replayed,
	); err != nil {
		return HitDecision{}, fmt.Errorf("hit Stage Cache entry: %w", err)
	}
	if len(digest) != sha256.Size {
		return HitDecision{}, errors.New("stage cache hit digest is malformed")
	}
	copy(decision.SHA256[:], digest)
	return decision, nil
}

func (repository *PostgresRepository) ReleaseExecutionPin(
	ctx context.Context,
	command ReleasePinCommand,
) (PinReleaseDecision, error) {
	if err := repository.validate(); err != nil {
		return PinReleaseDecision{}, err
	}
	if ctx == nil || command.CommandID == uuid.Nil || command.PinID == uuid.Nil ||
		command.OwnerJobID == uuid.Nil || command.OwnerStageRunID == uuid.Nil ||
		command.ReleaseReason == "" || len(command.ReleaseReason) > 500 ||
		command.ReleasedAt.IsZero() {
		return PinReleaseDecision{}, errors.New("stage cache ExecutionPin release is incomplete")
	}
	payload, err := json.Marshal(map[string]any{
		"command_id":         command.CommandID,
		"pin_id":             command.PinID,
		"owner_job_id":       command.OwnerJobID,
		"owner_stage_run_id": command.OwnerStageRunID,
		"release_reason":     command.ReleaseReason,
		"released_at":        command.ReleasedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return PinReleaseDecision{}, fmt.Errorf("encode Stage Cache ExecutionPin release: %w", err)
	}
	var decision PinReleaseDecision
	if err := repository.pool.QueryRow(ctx, `
		SELECT pin_id, released_at, replayed
		FROM vela_release_stage_cache_execution_pin($1::jsonb)
	`, payload).Scan(
		&decision.PinID,
		&decision.ReleasedAt,
		&decision.Replayed,
	); err != nil {
		return PinReleaseDecision{}, fmt.Errorf("release Stage Cache ExecutionPin: %w", err)
	}
	return decision, nil
}

func (repository *PostgresRepository) RequestDeletion(
	ctx context.Context,
	command DeletionCommand,
) (DeletionDecision, error) {
	if err := repository.validate(); err != nil {
		return DeletionDecision{}, err
	}
	if ctx == nil || command.CommandID == uuid.Nil || command.OrganizationID == uuid.Nil ||
		command.ProjectID == uuid.Nil || command.SourceJobID == uuid.Nil ||
		command.RequestedAt.IsZero() {
		return DeletionDecision{}, errors.New("stage cache deletion is incomplete")
	}
	payload, err := json.Marshal(map[string]any{
		"command_id":      command.CommandID,
		"organization_id": command.OrganizationID,
		"project_id":      command.ProjectID,
		"source_job_id":   command.SourceJobID,
		"requested_at":    command.RequestedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return DeletionDecision{}, fmt.Errorf("encode Stage Cache deletion: %w", err)
	}
	var decision DeletionDecision
	if err := repository.pool.QueryRow(ctx, `
		SELECT deleted_count, blocked_count, replayed
		FROM vela_request_stage_cache_deletion($1::jsonb)
	`, payload).Scan(
		&decision.DeletedCount,
		&decision.BlockedCount,
		&decision.Replayed,
	); err != nil {
		return DeletionDecision{}, fmt.Errorf("request Stage Cache deletion: %w", err)
	}
	return decision, nil
}

func (repository *PostgresRepository) ReconcileDeletions(
	ctx context.Context,
	observedAt time.Time,
	limit int,
) (int64, error) {
	if err := repository.validate(); err != nil {
		return 0, err
	}
	if ctx == nil || observedAt.IsZero() || limit < 1 || limit > 1000 {
		return 0, errors.New("stage cache deletion reconciliation is incomplete")
	}
	var changed int64
	if err := repository.pool.QueryRow(ctx, `
		SELECT vela_reconcile_stage_cache_deletions($1, $2)
	`, observedAt.UTC(), limit).Scan(&changed); err != nil {
		return 0, fmt.Errorf("reconcile Stage Cache deletions: %w", err)
	}
	return changed, nil
}

func (repository *PostgresRepository) validate() error {
	if repository == nil || repository.pool == nil {
		return errors.New("stage cache repository is not configured")
	}
	return nil
}
