package catalogpromotion

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/productiongates"
)

var receiptNamespace = uuid.MustParse("8e45d190-25fa-5ac1-90fd-6d342778ce51")

type Service struct {
	pool *pgxpool.Pool
}

type Result struct {
	ManifestDigest  string
	ReleaseDigest   string
	ReceiptIDs      map[productiongates.Gate]uuid.UUID
	ProtocolVersion int
}

func New(ctx context.Context, pool *pgxpool.Pool) (*Service, error) {
	if pool == nil {
		return nil, errors.New("catalog promotion database pool is required")
	}
	if err := veladb.VerifyRole(ctx, pool, veladb.RoleCatalogPromotion); err != nil {
		return nil, fmt.Errorf("verify Catalog Promotion database role: %w", err)
	}
	return &Service{pool: pool}, nil
}

func (service *Service) Apply(ctx context.Context, planPath string) (Result, error) {
	plan, err := LoadPlan(planPath)
	if err != nil {
		return Result{}, err
	}
	manifest, err := productiongates.LoadManifestWithin(filepath.Dir(planPath), plan.ManifestRef)
	if err != nil {
		return Result{}, fmt.Errorf("load Launch Receipt manifest: %w", err)
	}
	manifestDigest, err := decodeDigest(manifest.Digest)
	if err != nil {
		return Result{}, err
	}
	releaseDigest, err := decodeDigest(manifest.ReleaseDigest)
	if err != nil {
		return Result{}, err
	}

	transaction, err := service.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Result{}, fmt.Errorf("begin Catalog Promotion transaction: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()
	receiptIDs := make(map[productiongates.Gate]uuid.UUID, len(manifest.Receipts))
	for _, receipt := range manifest.Receipts {
		receiptID := receiptID(manifest.Digest, receipt.Gate)
		evidenceDigest, err := decodeDigest(receipt.EvidenceDigest)
		if err != nil {
			return Result{}, err
		}
		var returnedID uuid.UUID
		var replayed bool
		err = transaction.QueryRow(ctx, `
			SELECT receipt_id, replayed
			FROM vela_record_production_gate_receipt(
				$1, $2, $3, $4, $5, $6, $7, $8,
				$9, $10, $11, $12, $13, $14, $15, $16
			)
		`,
			receiptID,
			receipt.SchemaVersion,
			string(receipt.Gate),
			releaseDigest,
			receipt.ConfigurationRevision,
			receipt.ValidationEnvironment,
			string(receipt.Result),
			receipt.Owner,
			receipt.AcceptanceThreshold,
			receipt.ObservedResult,
			receipt.EvidenceRef,
			evidenceDigest,
			manifestDigest,
			receipt.StartedAt,
			receipt.CompletedAt,
			receipt.RecordedAt,
		).Scan(&returnedID, &replayed)
		if err != nil {
			return Result{}, fmt.Errorf("record %s Launch Receipt: %w", receipt.Gate, err)
		}
		if returnedID != receiptID {
			return Result{}, fmt.Errorf("record %s Launch Receipt returned mismatched identity", receipt.Gate)
		}
		receiptIDs[receipt.Gate] = receiptID
	}
	var sealed bool
	var receiptCount int
	if err := transaction.QueryRow(ctx, `
		SELECT sealed, receipt_count FROM vela_seal_production_gate_manifest($1)
	`, manifestDigest).Scan(&sealed, &receiptCount); err != nil {
		return Result{}, fmt.Errorf("seal Production Gate manifest: %w", err)
	}
	if !sealed || receiptCount != len(productiongates.AllGates()) {
		return Result{}, errors.New("sealed Production Gate manifest returned incomplete receipt count")
	}
	presetReceiptID := receiptIDs[productiongates.GatePresetCertification]
	for _, promotion := range plan.Certifications {
		if err := promoteCertification(ctx, transaction, promotion, presetReceiptID); err != nil {
			return Result{}, err
		}
	}
	for _, promotion := range plan.RateCards {
		if err := promoteRateCard(ctx, transaction, promotion, presetReceiptID); err != nil {
			return Result{}, err
		}
	}
	var protocolVersion int
	var mode string
	var replayed bool
	if err := transaction.QueryRow(ctx, `
		SELECT protocol_version, mode::text, replayed
		FROM vela_enable_evidenced_catalog($1)
	`, presetReceiptID).Scan(&protocolVersion, &mode, &replayed); err != nil {
		return Result{}, fmt.Errorf("enable evidenced Catalog: %w", err)
	}
	if mode != "EVIDENCED" || protocolVersion != 2 {
		return Result{}, fmt.Errorf("enable evidenced Catalog returned protocol %d mode %q", protocolVersion, mode)
	}
	if err := transaction.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("commit Catalog Promotion transaction: %w", err)
	}
	return Result{
		ManifestDigest:  manifest.Digest,
		ReleaseDigest:   manifest.ReleaseDigest,
		ReceiptIDs:      receiptIDs,
		ProtocolVersion: protocolVersion,
	}, nil
}

func promoteCertification(
	ctx context.Context,
	transaction pgx.Tx,
	promotion CertificationPromotion,
	receiptID uuid.UUID,
) error {
	var evidenceID uuid.UUID
	var replayed bool
	var state string
	if err := transaction.QueryRow(ctx, `
		SELECT evidence_id, replayed, certification_state::text
		FROM vela_promote_profile_certification(
			$1, $2, $3, $4, $5, $6, $7, $8, $9,
			$10, $11, $12, $13, $14, $15, $16, $17, $18
		)
	`,
		promotion.EvidenceID,
		promotion.ProfileCertificationID,
		promotion.InferenceBackendRevisionID,
		promotion.HardwareDriverBaseline,
		promotion.BenchmarkCorpusRevision,
		promotion.QualityThresholdPPM,
		promotion.QualityObservedPPM,
		promotion.SuccessRateThresholdPPM,
		promotion.SuccessRateObservedPPM,
		promotion.P50Milliseconds,
		promotion.P95ThresholdMilliseconds,
		promotion.P95ObservedMilliseconds,
		promotion.CostThresholdMinor,
		promotion.CostObservedMinor,
		promotion.CostCurrency,
		promotion.ConfidenceThresholdPPM,
		promotion.ConfidenceObservedPPM,
		receiptID,
	).Scan(&evidenceID, &replayed, &state); err != nil {
		return fmt.Errorf("promote ProfileCertification %s: %w", promotion.ProfileCertificationID, err)
	}
	if evidenceID != promotion.EvidenceID || state != "ACTIVE" {
		return fmt.Errorf("promote ProfileCertification %s returned mismatched result", promotion.ProfileCertificationID)
	}
	return nil
}

func promoteRateCard(
	ctx context.Context,
	transaction pgx.Tx,
	promotion RateCardPromotion,
	receiptID uuid.UUID,
) error {
	var bindingID uuid.UUID
	var replayed bool
	var state string
	if err := transaction.QueryRow(ctx, `
		SELECT binding_id, replayed, rate_card_state::text
		FROM vela_promote_rate_card($1, $2, $3)
	`, promotion.BindingID, promotion.RateCardRevisionID, receiptID).Scan(
		&bindingID, &replayed, &state,
	); err != nil {
		return fmt.Errorf("promote RateCardRevision %s: %w", promotion.RateCardRevisionID, err)
	}
	if bindingID != promotion.BindingID || state != "ACTIVE" {
		return fmt.Errorf("promote RateCardRevision %s returned mismatched result", promotion.RateCardRevisionID)
	}
	return nil
}

func receiptID(manifestDigest string, gate productiongates.Gate) uuid.UUID {
	return uuid.NewSHA1(receiptNamespace, []byte(manifestDigest+"\x00"+string(gate)))
}

func decodeDigest(value string) ([]byte, error) {
	if !strings.HasPrefix(value, "sha256:") {
		return nil, fmt.Errorf("invalid SHA-256 digest %q", value)
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	if err != nil || len(decoded) != 32 {
		return nil, fmt.Errorf("invalid SHA-256 digest %q", value)
	}
	return decoded, nil
}
