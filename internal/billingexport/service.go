package billingexport

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Line struct {
	ChargeID       uuid.UUID
	IdempotencyKey string
	OrganizationID uuid.UUID
	ProjectID      uuid.UUID
	JobID          uuid.UUID
	Reason         string
	AmountMinor    int64
	Currency       string
	PostedAt       time.Time
}

type Receipt struct {
	InvoiceReference string
	LineReference    string
}

type Adapter interface {
	ExportLine(context.Context, Line) (Receipt, error)
}

type Config struct {
	ExporterID string
	BatchSize  int32
	ClaimTTL   time.Duration
	RetryDelay time.Duration
}

type BatchResult struct {
	Claimed  int
	Exported int
}

type claimedLine struct {
	ChargeID       uuid.UUID `db:"charge_id"`
	OrganizationID uuid.UUID `db:"organization_id"`
	ProjectID      uuid.UUID `db:"project_id"`
	JobID          uuid.UUID `db:"job_id"`
	Reason         string    `db:"reason"`
	AmountMinor    int64     `db:"amount_minor"`
	Currency       string    `db:"currency"`
	PostedAt       time.Time `db:"posted_at"`
}

type Service struct {
	pool              *pgxpool.Pool
	adapter           Adapter
	exporterID        string
	batchSize         int32
	claimSeconds      int32
	retryAfterSeconds int32
}

func NewService(pool *pgxpool.Pool, adapter Adapter, config Config) (*Service, error) {
	if pool == nil {
		return nil, errors.New("invoice exporter database pool is required")
	}
	if adapter == nil {
		return nil, errors.New("invoice exporter adapter is required")
	}
	if strings.TrimSpace(config.ExporterID) != config.ExporterID || config.ExporterID == "" ||
		len(config.ExporterID) > 200 {
		return nil, errors.New("invoice exporter id must contain 1 to 200 unpadded characters")
	}
	if config.BatchSize < 1 || config.BatchSize > 1000 {
		return nil, errors.New("invoice exporter batch size must be between 1 and 1000")
	}
	claimSeconds, ok := supportedDurationSeconds(config.ClaimTTL, false)
	if !ok || claimSeconds > 300 {
		return nil, errors.New("invoice exporter claim TTL must be in (0, 5m]")
	}
	retryAfterSeconds, ok := supportedDurationSeconds(config.RetryDelay, true)
	if !ok || retryAfterSeconds > 3600 {
		return nil, errors.New("invoice exporter retry delay must be in [0, 1h]")
	}
	return &Service{
		pool:              pool,
		adapter:           adapter,
		exporterID:        config.ExporterID,
		batchSize:         config.BatchSize,
		claimSeconds:      claimSeconds,
		retryAfterSeconds: retryAfterSeconds,
	}, nil
}

func (s *Service) ExportBatch(ctx context.Context) (BatchResult, error) {
	if s == nil || s.pool == nil || s.adapter == nil {
		return BatchResult{}, errors.New("invoice exporter is not configured")
	}
	var result BatchResult
	var exportErrors []error
	for result.Claimed < int(s.batchSize) {
		claimToken := uuid.New()
		export, found, err := s.claimNext(ctx, claimToken)
		if err != nil {
			exportErrors = append(exportErrors, err)
			break
		}
		if !found {
			break
		}
		result.Claimed++
		line := Line{
			ChargeID:       export.ChargeID,
			IdempotencyKey: export.ChargeID.String(),
			OrganizationID: export.OrganizationID,
			ProjectID:      export.ProjectID,
			JobID:          export.JobID,
			Reason:         export.Reason,
			AmountMinor:    export.AmountMinor,
			Currency:       export.Currency,
			PostedAt:       export.PostedAt,
		}
		receipt, exportErr := s.adapter.ExportLine(ctx, line)
		if exportErr != nil {
			exportErrors = append(exportErrors, s.markFailed(ctx, export.ChargeID, claimToken, exportErr))
			break
		}
		if err := validateReceipt(receipt); err != nil {
			exportErrors = append(exportErrors, s.markFailed(ctx, export.ChargeID, claimToken, err))
			break
		}
		var marked bool
		markErr := s.pool.QueryRow(ctx, `
			SELECT vela_mark_invoice_exported($1, $2, $3, $4, $5)
		`,
			uuid.New(),
			export.ChargeID,
			claimToken,
			receipt.InvoiceReference,
			receipt.LineReference,
		).Scan(&marked)
		if markErr != nil {
			exportErrors = append(exportErrors, fmt.Errorf("record Invoice export receipt for Charge %s: %w", export.ChargeID, markErr))
			break
		}
		if !marked {
			exportErrors = append(exportErrors, fmt.Errorf("invoice export claim for Charge %s is stale after remote success", export.ChargeID))
			break
		}
		result.Exported++
	}
	return result, errors.Join(exportErrors...)
}

func (s *Service) claimNext(ctx context.Context, claimToken uuid.UUID) (claimedLine, bool, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			charge_id,
			organization_id,
			project_id,
			job_id,
			reason,
			amount_minor,
			currency,
			posted_at
		FROM vela_claim_invoice_exports($1, $2, $3, $4)
	`, s.exporterID, claimToken, s.claimSeconds, 1)
	if err != nil {
		return claimedLine{}, false, fmt.Errorf("claim next Invoice export: %w", err)
	}
	claimed, err := pgx.CollectRows(rows, pgx.RowToStructByName[claimedLine])
	if err != nil {
		return claimedLine{}, false, fmt.Errorf("read next claimed Invoice export: %w", err)
	}
	if len(claimed) == 0 {
		return claimedLine{}, false, nil
	}
	if len(claimed) != 1 {
		return claimedLine{}, false, fmt.Errorf("claim next Invoice export returned %d rows", len(claimed))
	}
	return claimed[0], true, nil
}

func (s *Service) markFailed(
	ctx context.Context,
	chargeID uuid.UUID,
	claimToken uuid.UUID,
	exportErr error,
) error {
	var marked bool
	err := s.pool.QueryRow(ctx, `
		SELECT vela_mark_invoice_export_failed($1, $2, $3, $4)
	`, chargeID, claimToken, s.retryAfterSeconds, exportErr.Error()).Scan(&marked)
	if err != nil {
		return fmt.Errorf("export Invoice line for Charge %s: %v; release claim: %w", chargeID, exportErr, err)
	}
	if !marked {
		return fmt.Errorf("export Invoice line for Charge %s: %v; claim is stale", chargeID, exportErr)
	}
	return fmt.Errorf("export Invoice line for Charge %s: %w", chargeID, exportErr)
}

func validateReceipt(receipt Receipt) error {
	if strings.TrimSpace(receipt.InvoiceReference) != receipt.InvoiceReference ||
		receipt.InvoiceReference == "" || len(receipt.InvoiceReference) > 500 {
		return errors.New("invoice adapter receipt has an invalid Invoice reference")
	}
	if strings.TrimSpace(receipt.LineReference) != receipt.LineReference ||
		receipt.LineReference == "" || len(receipt.LineReference) > 500 {
		return errors.New("invoice adapter receipt has an invalid line reference")
	}
	return nil
}

func supportedDurationSeconds(duration time.Duration, allowZero bool) (int32, bool) {
	if duration < 0 || (!allowZero && duration == 0) {
		return 0, false
	}
	seconds := duration / time.Second
	if duration%time.Second != 0 {
		seconds++
	}
	if seconds > time.Duration(math.MaxInt32) {
		return 0, false
	}
	return int32(seconds), true
}
