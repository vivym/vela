package telemetry

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type SLOMetric struct {
	ModelRevisionID            string
	GenerationPreset           string
	GenerationPresetRevisionID string
	ServiceClassRevisionID     string
	OutputSpecID               string
	GenerationCount            int
	Result                     string
	P95TargetMilliseconds      float64
	SuccessTargetPPM           float64
	P95Milliseconds            float64
	SuccessLowerBoundPPM       float64
	EligibleCount              float64
	SucceededCount             float64
	FailedCount                float64
	CustomerCanceledCount      float64
	OpenCount                  float64
	SealedAt                   time.Time
	HasReport                  bool
}

type SLOReportReader interface {
	LatestSLOReports(context.Context) ([]SLOMetric, error)
}

type PostgresSLOReportReader struct {
	pool *pgxpool.Pool
}

func NewPostgresSLOReportReader(pool *pgxpool.Pool) *PostgresSLOReportReader {
	return &PostgresSLOReportReader{pool: pool}
}

func (reader *PostgresSLOReportReader) LatestSLOReports(ctx context.Context) ([]SLOMetric, error) {
	if reader == nil || reader.pool == nil {
		return nil, fmt.Errorf("statistical SLO report database is unavailable")
	}
	rows, err := reader.pool.Query(ctx, `
		WITH saleable_contracts AS (
			SELECT DISTINCT
				line.model_revision_id,
				line.generation_preset_revision_id,
				line.service_class_revision_id,
				line.output_spec_id,
				generation.generation_count
			FROM rate_card_revisions AS rate_card
			JOIN rate_card_lines AS line
			  ON line.rate_card_revision_id = rate_card.id
			CROSS JOIN generate_series(1, 16) AS generation(generation_count)
			WHERE rate_card.state = 'ACTIVE'
		)
		SELECT
			saleable.model_revision_id::text,
			preset.stable_id,
			saleable.generation_preset_revision_id::text,
			saleable.service_class_revision_id::text,
			saleable.output_spec_id::text,
			saleable.generation_count,
			COALESCE(report.result::text, 'MISSING'),
			COALESCE(contract.p95_target_milliseconds, 0)::double precision,
			COALESCE(contract.success_target_ppm, 0)::double precision,
			COALESCE(report.p95_milliseconds, 0)::double precision,
			COALESCE(report.success_lower_bound_ppm, 0)::double precision,
			COALESCE(report.eligible_count, 0)::double precision,
			COALESCE(report.succeeded_count, 0)::double precision,
			COALESCE(report.failed_count, 0)::double precision,
			COALESCE(report.customer_canceled_count, 0)::double precision,
			COALESCE(report.open_count, 0)::double precision,
			COALESCE(report.sealed_at, to_timestamp(0)),
			report.id IS NOT NULL
		FROM saleable_contracts AS saleable
		JOIN generation_preset_revisions AS preset
		  ON preset.id = saleable.generation_preset_revision_id
		LEFT JOIN statistical_slo_contract_revisions AS contract
		  ON contract.model_revision_id = saleable.model_revision_id
		 AND contract.generation_preset_revision_id = saleable.generation_preset_revision_id
		 AND contract.service_class_revision_id = saleable.service_class_revision_id
		 AND contract.output_spec_id = saleable.output_spec_id
		 AND contract.generation_count = saleable.generation_count
		LEFT JOIN LATERAL (
			SELECT candidate.*
			FROM slo_measurement_reports AS candidate
			WHERE candidate.contract_revision_id = contract.id
			ORDER BY candidate.window_end DESC
			LIMIT 1
		) AS report ON true
		ORDER BY saleable.model_revision_id, saleable.generation_preset_revision_id,
			saleable.service_class_revision_id, saleable.output_spec_id,
			saleable.generation_count
	`)
	if err != nil {
		return nil, fmt.Errorf("query latest statistical SLO reports: %w", err)
	}
	defer rows.Close()
	metrics := make([]SLOMetric, 0)
	for rows.Next() {
		var metric SLOMetric
		if err := rows.Scan(
			&metric.ModelRevisionID,
			&metric.GenerationPreset,
			&metric.GenerationPresetRevisionID,
			&metric.ServiceClassRevisionID,
			&metric.OutputSpecID,
			&metric.GenerationCount,
			&metric.Result,
			&metric.P95TargetMilliseconds,
			&metric.SuccessTargetPPM,
			&metric.P95Milliseconds,
			&metric.SuccessLowerBoundPPM,
			&metric.EligibleCount,
			&metric.SucceededCount,
			&metric.FailedCount,
			&metric.CustomerCanceledCount,
			&metric.OpenCount,
			&metric.SealedAt,
			&metric.HasReport,
		); err != nil {
			return nil, fmt.Errorf("scan latest statistical SLO report: %w", err)
		}
		metrics = append(metrics, metric)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate latest statistical SLO reports: %w", err)
	}
	return metrics, nil
}

type SLOCollector struct {
	reader            SLOReportReader
	p95               *prometheus.Desc
	p95Target         *prometheus.Desc
	successLowerBound *prometheus.Desc
	successTarget     *prometheus.Desc
	eligible          *prometheus.Desc
	succeeded         *prometheus.Desc
	failed            *prometheus.Desc
	customerCanceled  *prometheus.Desc
	open              *prometheus.Desc
	sealedAt          *prometheus.Desc
	coverage          *prometheus.Desc
	scrapeSuccess     *prometheus.Desc
}

func NewSLOCollector(reader SLOReportReader) *SLOCollector {
	contractLabels := []string{
		"model_revision", "generation_preset", "generation_preset_revision",
		"service_class_revision", "output_spec", "generation_count",
	}
	reportLabels := append(append([]string(nil), contractLabels...), "result")
	return &SLOCollector{
		reader: reader,
		p95: prometheus.NewDesc(
			"vela_slo_report_p95_milliseconds",
			"Latest sealed monthly QUEUED-to-Visible-Completion p95.", reportLabels, nil,
		),
		p95Target: prometheus.NewDesc(
			"vela_slo_target_p95_milliseconds",
			"Immutable QUEUED-to-Visible-Completion p95 target.", reportLabels, nil,
		),
		successLowerBound: prometheus.NewDesc(
			"vela_slo_report_success_lower_bound_ratio",
			"Latest sealed monthly one-sided Wilson success-rate lower bound.", reportLabels, nil,
		),
		successTarget: prometheus.NewDesc(
			"vela_slo_target_success_ratio",
			"Immutable statistical Job success-rate target.", reportLabels, nil,
		),
		eligible: prometheus.NewDesc(
			"vela_slo_report_eligible_jobs",
			"Eligible Jobs in the latest sealed monthly cohort.", reportLabels, nil,
		),
		succeeded: prometheus.NewDesc(
			"vela_slo_report_succeeded_jobs",
			"Succeeded Jobs in the latest sealed monthly cohort.", reportLabels, nil,
		),
		failed: prometheus.NewDesc(
			"vela_slo_report_failed_jobs",
			"Failed Jobs in the latest sealed monthly cohort.", reportLabels, nil,
		),
		customerCanceled: prometheus.NewDesc(
			"vela_slo_report_customer_canceled_jobs",
			"Customer-canceled Jobs excluded from the latest sealed monthly success denominator.", reportLabels, nil,
		),
		open: prometheus.NewDesc(
			"vela_slo_report_open_jobs",
			"Open Jobs in the latest sealed monthly cohort.", reportLabels, nil,
		),
		sealedAt: prometheus.NewDesc(
			"vela_slo_report_sealed_timestamp_seconds",
			"Unix timestamp of the latest sealed monthly report.", reportLabels, nil,
		),
		coverage: prometheus.NewDesc(
			"vela_slo_contract_report_coverage",
			"Whether an exact statistical SLO contract has a sealed monthly report.", contractLabels, nil,
		),
		scrapeSuccess: prometheus.NewDesc(
			"vela_slo_report_exporter_last_scrape_success",
			"Whether the PostgreSQL-authoritative SLO report export succeeded.", nil, nil,
		),
	}
}

func (collector *SLOCollector) Describe(output chan<- *prometheus.Desc) {
	output <- collector.p95
	output <- collector.p95Target
	output <- collector.successLowerBound
	output <- collector.successTarget
	output <- collector.eligible
	output <- collector.succeeded
	output <- collector.failed
	output <- collector.customerCanceled
	output <- collector.open
	output <- collector.sealedAt
	output <- collector.coverage
	output <- collector.scrapeSuccess
}

func (collector *SLOCollector) Collect(output chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reports, err := collector.reader.LatestSLOReports(ctx)
	if err != nil {
		output <- prometheus.MustNewConstMetric(collector.scrapeSuccess, prometheus.GaugeValue, 0)
		return
	}
	output <- prometheus.MustNewConstMetric(collector.scrapeSuccess, prometheus.GaugeValue, 1)
	for _, report := range reports {
		contractLabels := []string{
			report.ModelRevisionID,
			report.GenerationPreset,
			report.GenerationPresetRevisionID,
			report.ServiceClassRevisionID,
			report.OutputSpecID,
			strconv.Itoa(report.GenerationCount),
		}
		coverage := 0.0
		if report.HasReport {
			coverage = 1
		}
		output <- prometheus.MustNewConstMetric(collector.coverage, prometheus.GaugeValue, coverage, contractLabels...)
		if !report.HasReport {
			continue
		}
		labels := append(contractLabels, report.Result)
		output <- prometheus.MustNewConstMetric(collector.p95, prometheus.GaugeValue, report.P95Milliseconds, labels...)
		output <- prometheus.MustNewConstMetric(collector.p95Target, prometheus.GaugeValue, report.P95TargetMilliseconds, labels...)
		output <- prometheus.MustNewConstMetric(
			collector.successLowerBound,
			prometheus.GaugeValue,
			report.SuccessLowerBoundPPM/1_000_000,
			labels...,
		)
		output <- prometheus.MustNewConstMetric(
			collector.successTarget,
			prometheus.GaugeValue,
			report.SuccessTargetPPM/1_000_000,
			labels...,
		)
		output <- prometheus.MustNewConstMetric(collector.eligible, prometheus.GaugeValue, report.EligibleCount, labels...)
		output <- prometheus.MustNewConstMetric(collector.succeeded, prometheus.GaugeValue, report.SucceededCount, labels...)
		output <- prometheus.MustNewConstMetric(collector.failed, prometheus.GaugeValue, report.FailedCount, labels...)
		output <- prometheus.MustNewConstMetric(collector.customerCanceled, prometheus.GaugeValue, report.CustomerCanceledCount, labels...)
		output <- prometheus.MustNewConstMetric(collector.open, prometheus.GaugeValue, report.OpenCount, labels...)
		output <- prometheus.MustNewConstMetric(collector.sealedAt, prometheus.GaugeValue, float64(report.SealedAt.Unix()), labels...)
	}
}
