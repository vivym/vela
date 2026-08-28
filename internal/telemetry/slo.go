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
		SELECT DISTINCT ON (contract.id)
			contract.model_revision_id::text,
			preset.stable_id,
			contract.generation_preset_revision_id::text,
			contract.service_class_revision_id::text,
			contract.output_spec_id::text,
			contract.generation_count,
			report.result::text,
			contract.p95_target_milliseconds::double precision,
			contract.success_target_ppm::double precision,
			COALESCE(report.p95_milliseconds, 0)::double precision,
			report.success_lower_bound_ppm::double precision,
			report.eligible_count::double precision,
			report.succeeded_count::double precision,
			report.failed_count::double precision,
			report.customer_canceled_count::double precision,
			report.open_count::double precision,
			report.sealed_at
		FROM slo_measurement_reports AS report
		JOIN statistical_slo_contract_revisions AS contract
		  ON contract.id = report.contract_revision_id
		JOIN generation_preset_revisions AS preset
		  ON preset.id = contract.generation_preset_revision_id
		ORDER BY contract.id, report.window_end DESC
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
	scrapeSuccess     *prometheus.Desc
}

func NewSLOCollector(reader SLOReportReader) *SLOCollector {
	labels := []string{
		"model_revision", "generation_preset", "generation_preset_revision",
		"service_class_revision", "output_spec", "generation_count", "result",
	}
	return &SLOCollector{
		reader: reader,
		p95: prometheus.NewDesc(
			"vela_slo_report_p95_milliseconds",
			"Latest sealed monthly QUEUED-to-Visible-Completion p95.", labels, nil,
		),
		p95Target: prometheus.NewDesc(
			"vela_slo_target_p95_milliseconds",
			"Immutable QUEUED-to-Visible-Completion p95 target.", labels, nil,
		),
		successLowerBound: prometheus.NewDesc(
			"vela_slo_report_success_lower_bound_ratio",
			"Latest sealed monthly one-sided Wilson success-rate lower bound.", labels, nil,
		),
		successTarget: prometheus.NewDesc(
			"vela_slo_target_success_ratio",
			"Immutable statistical Job success-rate target.", labels, nil,
		),
		eligible: prometheus.NewDesc(
			"vela_slo_report_eligible_jobs",
			"Eligible Jobs in the latest sealed monthly cohort.", labels, nil,
		),
		succeeded: prometheus.NewDesc(
			"vela_slo_report_succeeded_jobs",
			"Succeeded Jobs in the latest sealed monthly cohort.", labels, nil,
		),
		failed: prometheus.NewDesc(
			"vela_slo_report_failed_jobs",
			"Failed Jobs in the latest sealed monthly cohort.", labels, nil,
		),
		customerCanceled: prometheus.NewDesc(
			"vela_slo_report_customer_canceled_jobs",
			"Customer-canceled Jobs excluded from the latest sealed monthly success denominator.", labels, nil,
		),
		open: prometheus.NewDesc(
			"vela_slo_report_open_jobs",
			"Open Jobs in the latest sealed monthly cohort.", labels, nil,
		),
		sealedAt: prometheus.NewDesc(
			"vela_slo_report_sealed_timestamp_seconds",
			"Unix timestamp of the latest sealed monthly report.", labels, nil,
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
		labels := []string{
			report.ModelRevisionID,
			report.GenerationPreset,
			report.GenerationPresetRevisionID,
			report.ServiceClassRevisionID,
			report.OutputSpecID,
			strconv.Itoa(report.GenerationCount),
			report.Result,
		}
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
