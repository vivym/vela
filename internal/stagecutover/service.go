package stagecutover

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/legacyh3reachability"
	"github.com/vivym/vela/internal/releasebundle"
)

type Service struct {
	pool *pgxpool.Pool
}

type CaptureInventoryRequest struct {
	SnapshotID uuid.UUID `json:"snapshot_id"`
	ObservedBy string    `json:"observed_by"`
}

type InventoryResult struct {
	SnapshotID                    uuid.UUID `json:"snapshot_id"`
	CutoverRevisionID             uuid.UUID `json:"cutover_revision_id"`
	ObservedAt                    time.Time `json:"observed_at"`
	ObservedBy                    string    `json:"observed_by"`
	NonterminalJobs               int64     `json:"nonterminal_jobs"`
	NonterminalAttempts           int64     `json:"nonterminal_attempts"`
	ActiveExecutionLeases         int64     `json:"active_execution_leases"`
	ActiveFinalizationLeases      int64     `json:"active_finalization_leases"`
	ActiveArtifactUploads         int64     `json:"active_artifact_uploads"`
	UnpublishedOutboxEvents       int64     `json:"unpublished_outbox_events"`
	RetainedPublishedOutboxEvents int64     `json:"retained_published_outbox_events"`
	SchedulerInboxBacklog         int64     `json:"scheduler_inbox_backlog"`
	RetryRecoveryBacklog          int64     `json:"retry_recovery_backlog"`
	TotalCount                    int64     `json:"total_count"`
	ContentDigest                 string    `json:"content_digest"`
}

type ExternalBacklog struct {
	WorkerLocalRecovery int64 `json:"worker_local_recovery"`
	NMinusOneDeployment int64 `json:"n_minus_one_deployment"`
	Scheduler           int64 `json:"scheduler"`
	Event               int64 `json:"event"`
	Artifact            int64 `json:"artifact"`
}

type RecordExternalDrainEvidenceRequest struct {
	EvidenceID             uuid.UUID       `json:"evidence_id"`
	Backlog                ExternalBacklog `json:"backlog"`
	EvidenceManifestDigest string          `json:"evidence_manifest_digest"`
	ObservedBy             string          `json:"observed_by"`
}

type ExternalDrainEvidenceResult struct {
	EvidenceID    uuid.UUID `json:"evidence_id"`
	TotalCount    int64     `json:"total_count"`
	ContentDigest string    `json:"content_digest"`
	Replayed      bool      `json:"replayed"`
}

type SealZeroBacklogRequest struct {
	ReceiptID               uuid.UUID `json:"receipt_id"`
	StartInventoryID        uuid.UUID `json:"start_inventory_id"`
	EndInventoryID          uuid.UUID `json:"end_inventory_id"`
	StartExternalEvidenceID uuid.UUID `json:"start_external_evidence_id"`
	EndExternalEvidenceID   uuid.UUID `json:"end_external_evidence_id"`
	SealedBy                string    `json:"sealed_by"`
}

type ZeroBacklogReceiptResult struct {
	ReceiptID       uuid.UUID `json:"receipt_id"`
	WindowStartedAt time.Time `json:"window_started_at"`
	WindowEndedAt   time.Time `json:"window_ended_at"`
	ContentDigest   string    `json:"content_digest"`
	Replayed        bool      `json:"replayed"`
}

type PrepareLegacyH3ContractionRequest struct {
	ZeroBacklogReceiptID uuid.UUID `json:"zero_backlog_receipt_id"`
	PreparedBy           string    `json:"prepared_by"`
}

type LegacyH3ContractionPreparationResult struct {
	ZeroBacklogReceiptID uuid.UUID `json:"zero_backlog_receipt_id"`
	CutoverRevisionID    uuid.UUID `json:"cutover_revision_id"`
	PreparedAt           time.Time `json:"prepared_at"`
	ArchiveDigest        string    `json:"archive_digest"`
	ContentDigest        string    `json:"content_digest"`
	Replayed             bool      `json:"replayed"`
}

type AuthorizeLegacyH3ContractionRequest struct {
	ZeroBacklogReceiptID     uuid.UUID `json:"zero_backlog_receipt_id"`
	LaunchManifestDigest     string    `json:"launch_manifest_digest"`
	ReleaseBundlePath        string    `json:"release_bundle_path"`
	ReachabilityEvidencePath string    `json:"reachability_evidence_path"`
	AuthorizedBy             string    `json:"authorized_by"`
}

type LegacyH3ContractionAuthorizationResult struct {
	ZeroBacklogReceiptID       uuid.UUID `json:"zero_backlog_receipt_id"`
	CutoverRevisionID          uuid.UUID `json:"cutover_revision_id"`
	LaunchManifestDigest       string    `json:"launch_manifest_digest"`
	ReleaseDigest              string    `json:"release_digest"`
	ConfigurationRevision      string    `json:"configuration_revision"`
	SourceRevision             string    `json:"source_revision"`
	ReachabilityEvidenceDigest string    `json:"reachability_evidence_digest"`
	AuthorizedAt               time.Time `json:"authorized_at"`
	ContentDigest              string    `json:"content_digest"`
	Replayed                   bool      `json:"replayed"`
}

func New(ctx context.Context, pool *pgxpool.Pool) (*Service, error) {
	if pool == nil {
		return nil, errors.New("stage cutover database pool is required")
	}
	if err := veladb.VerifyRole(ctx, pool, veladb.RoleCatalogPromotion); err != nil {
		return nil, fmt.Errorf("verify Stage cutover Catalog Promotion role: %w", err)
	}
	return &Service{pool: pool}, nil
}

func (service *Service) CaptureInventory(
	ctx context.Context,
	request CaptureInventoryRequest,
) (InventoryResult, error) {
	var result InventoryResult
	var contentDigest []byte
	err := service.pool.QueryRow(ctx, `
		SELECT id, cutover_revision_id, observed_at, observed_by,
		       nonterminal_jobs, nonterminal_attempts,
		       active_execution_leases, active_finalization_leases,
		       active_artifact_uploads, unpublished_outbox_events,
		       retained_published_outbox_events, scheduler_inbox_backlog,
		       retry_recovery_backlog, total_count, content_digest
		FROM vela_capture_legacy_authority_inventory($1, $2)
	`, request.SnapshotID, request.ObservedBy).Scan(
		&result.SnapshotID,
		&result.CutoverRevisionID,
		&result.ObservedAt,
		&result.ObservedBy,
		&result.NonterminalJobs,
		&result.NonterminalAttempts,
		&result.ActiveExecutionLeases,
		&result.ActiveFinalizationLeases,
		&result.ActiveArtifactUploads,
		&result.UnpublishedOutboxEvents,
		&result.RetainedPublishedOutboxEvents,
		&result.SchedulerInboxBacklog,
		&result.RetryRecoveryBacklog,
		&result.TotalCount,
		&contentDigest,
	)
	if err != nil {
		return InventoryResult{}, fmt.Errorf("capture legacy authority inventory: %w", err)
	}
	result.ContentDigest = hex.EncodeToString(contentDigest)
	return result, nil
}

func (service *Service) RecordExternalDrainEvidence(
	ctx context.Context,
	request RecordExternalDrainEvidenceRequest,
) (ExternalDrainEvidenceResult, error) {
	manifestDigest, err := decodeSHA256(request.EvidenceManifestDigest)
	if err != nil {
		return ExternalDrainEvidenceResult{}, fmt.Errorf("decode external drain evidence manifest digest: %w", err)
	}
	var result ExternalDrainEvidenceResult
	var contentDigest []byte
	err = service.pool.QueryRow(ctx, `
		SELECT evidence_id, total_count, content_digest, replayed
		FROM vela_record_stage_cutover_external_drain_evidence(
			$1, $2, $3, $4, $5, $6, $7, $8
		)
	`,
		request.EvidenceID,
		request.Backlog.WorkerLocalRecovery,
		request.Backlog.NMinusOneDeployment,
		request.Backlog.Scheduler,
		request.Backlog.Event,
		request.Backlog.Artifact,
		manifestDigest,
		request.ObservedBy,
	).Scan(
		&result.EvidenceID,
		&result.TotalCount,
		&contentDigest,
		&result.Replayed,
	)
	if err != nil {
		return ExternalDrainEvidenceResult{}, fmt.Errorf("record external Stage cutover drain evidence: %w", err)
	}
	result.ContentDigest = hex.EncodeToString(contentDigest)
	return result, nil
}

func (service *Service) SealZeroBacklog(
	ctx context.Context,
	request SealZeroBacklogRequest,
) (ZeroBacklogReceiptResult, error) {
	var result ZeroBacklogReceiptResult
	var contentDigest []byte
	err := service.pool.QueryRow(ctx, `
		SELECT receipt_id, window_started_at, window_ended_at,
		       content_digest, replayed
		FROM vela_seal_stage_cutover_zero_backlog($1, $2, $3, $4, $5, $6)
	`,
		request.ReceiptID,
		request.StartInventoryID,
		request.EndInventoryID,
		request.StartExternalEvidenceID,
		request.EndExternalEvidenceID,
		request.SealedBy,
	).Scan(
		&result.ReceiptID,
		&result.WindowStartedAt,
		&result.WindowEndedAt,
		&contentDigest,
		&result.Replayed,
	)
	if err != nil {
		return ZeroBacklogReceiptResult{}, fmt.Errorf("seal Stage cutover zero backlog: %w", err)
	}
	result.ContentDigest = hex.EncodeToString(contentDigest)
	return result, nil
}

func (service *Service) PrepareLegacyH3Contraction(
	ctx context.Context,
	request PrepareLegacyH3ContractionRequest,
) (LegacyH3ContractionPreparationResult, error) {
	var result LegacyH3ContractionPreparationResult
	var archiveDigest, contentDigest []byte
	err := service.pool.QueryRow(ctx, `
		SELECT zero_backlog_receipt_id, cutover_revision_id, prepared_at,
		       archive_digest, content_digest, replayed
		FROM vela_prepare_legacy_h3_contraction($1, $2)
	`, request.ZeroBacklogReceiptID, request.PreparedBy).Scan(
		&result.ZeroBacklogReceiptID,
		&result.CutoverRevisionID,
		&result.PreparedAt,
		&archiveDigest,
		&contentDigest,
		&result.Replayed,
	)
	if err != nil {
		return LegacyH3ContractionPreparationResult{}, fmt.Errorf(
			"prepare Legacy H3 contraction: %w",
			err,
		)
	}
	result.ArchiveDigest = hex.EncodeToString(archiveDigest)
	result.ContentDigest = hex.EncodeToString(contentDigest)
	return result, nil
}

func (service *Service) AuthorizeLegacyH3Contraction(
	ctx context.Context,
	request AuthorizeLegacyH3ContractionRequest,
) (LegacyH3ContractionAuthorizationResult, error) {
	bundle, err := releasebundle.Load(request.ReleaseBundlePath)
	if err != nil {
		return LegacyH3ContractionAuthorizationResult{}, fmt.Errorf(
			"load contraction release bundle: %w",
			err,
		)
	}
	_, reachabilityBytes, _, err := legacyh3reachability.Load(
		request.ReachabilityEvidencePath,
		bundle,
	)
	if err != nil {
		return LegacyH3ContractionAuthorizationResult{}, fmt.Errorf(
			"load release-bound Legacy H3 reachability evidence: %w",
			err,
		)
	}
	launchManifestDigest, err := decodeSHA256(request.LaunchManifestDigest)
	if err != nil {
		return LegacyH3ContractionAuthorizationResult{}, fmt.Errorf(
			"decode Launch manifest digest: %w",
			err,
		)
	}
	releaseDigest, err := decodeSHA256(strings.TrimPrefix(bundle.ReleaseDigest, "sha256:"))
	if err != nil {
		return LegacyH3ContractionAuthorizationResult{}, fmt.Errorf(
			"decode release bundle digest: %w",
			err,
		)
	}
	configurationManifest, err := json.Marshal(bundle.ConfigurationManifest)
	if err != nil {
		return LegacyH3ContractionAuthorizationResult{}, fmt.Errorf(
			"encode release configuration manifest: %w",
			err,
		)
	}
	var result LegacyH3ContractionAuthorizationResult
	var returnedLaunchManifestDigest, returnedReleaseDigest, returnedReachabilityDigest []byte
	var contentDigest []byte
	err = service.pool.QueryRow(ctx, `
		SELECT zero_backlog_receipt_id, cutover_revision_id,
		       launch_manifest_digest, release_digest,
		       configuration_revision, source_revision,
		       reachability_evidence_digest,
		       authorized_at, content_digest, replayed
		FROM vela_authorize_legacy_h3_contraction($1, $2, $3, $4, $5, $6, $7)
	`, request.ZeroBacklogReceiptID, launchManifestDigest, releaseDigest,
		bundle.ConfigurationRevision, configurationManifest,
		reachabilityBytes, request.AuthorizedBy).Scan(
		&result.ZeroBacklogReceiptID,
		&result.CutoverRevisionID,
		&returnedLaunchManifestDigest,
		&returnedReleaseDigest,
		&result.ConfigurationRevision,
		&result.SourceRevision,
		&returnedReachabilityDigest,
		&result.AuthorizedAt,
		&contentDigest,
		&result.Replayed,
	)
	if err != nil {
		return LegacyH3ContractionAuthorizationResult{}, fmt.Errorf(
			"authorize Legacy H3 contraction: %w",
			err,
		)
	}
	result.LaunchManifestDigest = hex.EncodeToString(returnedLaunchManifestDigest)
	result.ReleaseDigest = hex.EncodeToString(returnedReleaseDigest)
	result.ReachabilityEvidenceDigest = hex.EncodeToString(returnedReachabilityDigest)
	result.ContentDigest = hex.EncodeToString(contentDigest)
	return result, nil
}

func decodeSHA256(value string) ([]byte, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		return nil, errors.New("SHA-256 digest must be 64 hexadecimal characters")
	}
	return decoded, nil
}
