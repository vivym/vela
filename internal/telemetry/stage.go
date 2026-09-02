package telemetry

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type StageStateCount struct {
	StageKind string
	State     string
	Count     float64
}

type StageValue struct {
	StageKind string
	Value     float64
}

type StateCount struct {
	State string
	Count float64
}

type StageSnapshot struct {
	RunStates                      []StageStateCount
	ReadyOldestAgeSeconds          []StageValue
	TransferStates                 []StateCount
	TransferActiveOldestAgeSeconds float64
	HasActiveTransfers             bool
	CacheStates                    []StageStateCount
	ResidencyStates                []StageStateCount
}

type StageSnapshotReader interface {
	LatestStageSnapshot(context.Context) (StageSnapshot, error)
}

type PostgresStageSnapshotReader struct {
	pool *pgxpool.Pool
}

func NewPostgresStageSnapshotReader(pool *pgxpool.Pool) *PostgresStageSnapshotReader {
	return &PostgresStageSnapshotReader{pool: pool}
}

func (reader *PostgresStageSnapshotReader) LatestStageSnapshot(ctx context.Context) (StageSnapshot, error) {
	if reader == nil || reader.pool == nil {
		return StageSnapshot{}, fmt.Errorf("stage telemetry database is unavailable")
	}
	rows, err := reader.pool.Query(ctx, `
		WITH run_states AS (
			SELECT definition.stage_kind, run.state::text AS state,
			       count(*)::double precision AS value
			FROM stage_runs AS run
			JOIN stage_definition_revisions AS definition
			  ON definition.id = run.stage_definition_revision_id
			GROUP BY definition.stage_kind, run.state
		), ready_oldest AS (
			SELECT definition.stage_kind, ''::text AS state,
			       extract(epoch FROM statement_timestamp() - min(run.updated_at))::double precision AS value
			FROM stage_runs AS run
			JOIN stage_definition_revisions AS definition
			  ON definition.id = run.stage_definition_revision_id
			WHERE run.state = 'READY'
			GROUP BY definition.stage_kind
		), transfer_states AS (
			SELECT ''::text AS stage_kind, ticket.state::text AS state,
			       count(*)::double precision AS value
			FROM transfer_tickets AS ticket
			GROUP BY ticket.state
		), transfer_active_oldest AS (
			SELECT ''::text AS stage_kind, ''::text AS state,
			       extract(epoch FROM statement_timestamp() - min(ticket.issued_at))::double precision AS value
			FROM transfer_tickets AS ticket
			WHERE ticket.state = 'ACTIVE'
			HAVING count(*) > 0
		), cache_states AS (
			SELECT definition.stage_kind, entry.state::text AS state,
			       count(*)::double precision AS value
			FROM stage_cache_entries AS entry
			JOIN stage_profile_revisions AS profile
			  ON profile.id = entry.stage_profile_revision_id
			JOIN stage_definition_revisions AS definition
			  ON definition.id = profile.stage_definition_revision_id
			GROUP BY definition.stage_kind, entry.state
		), residency_states AS (
			SELECT definition.stage_kind, residency.state::text AS state,
			       count(*)::double precision AS value
			FROM model_residencies AS residency
			JOIN worker_instances AS worker
			  ON worker.id = residency.worker_instance_id
			JOIN capacity_pools AS pool ON pool.id = worker.capacity_pool_id
			JOIN stage_profile_revisions AS profile
			  ON profile.id = pool.stage_profile_revision_id
			JOIN stage_definition_revisions AS definition
			  ON definition.id = profile.stage_definition_revision_id
			GROUP BY definition.stage_kind, residency.state
		)
		SELECT 'RUN_STATE', stage_kind, state, value FROM run_states
		UNION ALL
		SELECT 'READY_OLDEST', stage_kind, state, value FROM ready_oldest
		UNION ALL
		SELECT 'TRANSFER_STATE', stage_kind, state, value FROM transfer_states
		UNION ALL
		SELECT 'TRANSFER_ACTIVE_OLDEST', stage_kind, state, value FROM transfer_active_oldest
		UNION ALL
		SELECT 'CACHE_STATE', stage_kind, state, value FROM cache_states
		UNION ALL
		SELECT 'RESIDENCY_STATE', stage_kind, state, value FROM residency_states
		ORDER BY 1, 2, 3
	`)
	if err != nil {
		return StageSnapshot{}, fmt.Errorf("query Stage telemetry snapshot: %w", err)
	}
	defer rows.Close()

	var snapshot StageSnapshot
	for rows.Next() {
		var family, stageKind, state string
		var value float64
		if err := rows.Scan(&family, &stageKind, &state, &value); err != nil {
			return StageSnapshot{}, fmt.Errorf("scan Stage telemetry snapshot: %w", err)
		}
		switch family {
		case "RUN_STATE":
			snapshot.RunStates = append(snapshot.RunStates, StageStateCount{
				StageKind: stageKind, State: state, Count: value,
			})
		case "READY_OLDEST":
			snapshot.ReadyOldestAgeSeconds = append(snapshot.ReadyOldestAgeSeconds, StageValue{
				StageKind: stageKind, Value: value,
			})
		case "TRANSFER_STATE":
			snapshot.TransferStates = append(snapshot.TransferStates, StateCount{State: state, Count: value})
		case "TRANSFER_ACTIVE_OLDEST":
			snapshot.TransferActiveOldestAgeSeconds = value
			snapshot.HasActiveTransfers = true
		case "CACHE_STATE":
			snapshot.CacheStates = append(snapshot.CacheStates, StageStateCount{
				StageKind: stageKind, State: state, Count: value,
			})
		case "RESIDENCY_STATE":
			snapshot.ResidencyStates = append(snapshot.ResidencyStates, StageStateCount{
				StageKind: stageKind, State: state, Count: value,
			})
		default:
			return StageSnapshot{}, fmt.Errorf("unknown Stage telemetry family %q", family)
		}
	}
	if err := rows.Err(); err != nil {
		return StageSnapshot{}, fmt.Errorf("iterate Stage telemetry snapshot: %w", err)
	}
	return snapshot, nil
}

type StageCollector struct {
	reader          StageSnapshotReader
	runStates       *prometheus.Desc
	readyOldestAge  *prometheus.Desc
	transferStates  *prometheus.Desc
	transferOldest  *prometheus.Desc
	cacheStates     *prometheus.Desc
	residencyStates *prometheus.Desc
	scrapeSuccess   *prometheus.Desc
}

func NewStageCollector(reader StageSnapshotReader) *StageCollector {
	stageStateLabels := []string{"stage_kind", "state"}
	return &StageCollector{
		reader: reader,
		runStates: prometheus.NewDesc(
			"vela_stage_run_state_count",
			"PostgreSQL-authoritative StageRun count by bounded stage kind and state.",
			stageStateLabels,
			nil,
		),
		readyOldestAge: prometheus.NewDesc(
			"vela_stage_ready_oldest_age_seconds",
			"Age in seconds of the oldest READY StageRun by bounded stage kind.",
			[]string{"stage_kind"},
			nil,
		),
		transferStates: prometheus.NewDesc(
			"vela_stage_transfer_ticket_state_count",
			"PostgreSQL-authoritative TransferTicket count by state.",
			[]string{"state"},
			nil,
		),
		transferOldest: prometheus.NewDesc(
			"vela_stage_transfer_active_oldest_age_seconds",
			"Age in seconds of the oldest ACTIVE Stage TransferTicket.",
			nil,
			nil,
		),
		cacheStates: prometheus.NewDesc(
			"vela_stage_cache_entry_state_count",
			"PostgreSQL-authoritative exact Stage cache entry count by bounded stage kind and state.",
			stageStateLabels,
			nil,
		),
		residencyStates: prometheus.NewDesc(
			"vela_stage_model_residency_state_count",
			"PostgreSQL-authoritative ModelResidency count by bounded stage kind and state.",
			stageStateLabels,
			nil,
		),
		scrapeSuccess: prometheus.NewDesc(
			"vela_stage_authority_exporter_last_scrape_success",
			"Whether the PostgreSQL-authoritative Stage telemetry export succeeded.",
			nil,
			nil,
		),
	}
}

func (collector *StageCollector) Describe(output chan<- *prometheus.Desc) {
	output <- collector.runStates
	output <- collector.readyOldestAge
	output <- collector.transferStates
	output <- collector.transferOldest
	output <- collector.cacheStates
	output <- collector.residencyStates
	output <- collector.scrapeSuccess
}

func (collector *StageCollector) Collect(output chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	snapshot, err := collector.reader.LatestStageSnapshot(ctx)
	if err != nil {
		output <- prometheus.MustNewConstMetric(collector.scrapeSuccess, prometheus.GaugeValue, 0)
		return
	}
	output <- prometheus.MustNewConstMetric(collector.scrapeSuccess, prometheus.GaugeValue, 1)
	for _, metric := range snapshot.RunStates {
		output <- prometheus.MustNewConstMetric(
			collector.runStates, prometheus.GaugeValue, metric.Count, metric.StageKind, metric.State,
		)
	}
	for _, metric := range snapshot.ReadyOldestAgeSeconds {
		output <- prometheus.MustNewConstMetric(
			collector.readyOldestAge, prometheus.GaugeValue, metric.Value, metric.StageKind,
		)
	}
	for _, metric := range snapshot.TransferStates {
		output <- prometheus.MustNewConstMetric(
			collector.transferStates, prometheus.GaugeValue, metric.Count, metric.State,
		)
	}
	if snapshot.HasActiveTransfers {
		output <- prometheus.MustNewConstMetric(
			collector.transferOldest,
			prometheus.GaugeValue,
			snapshot.TransferActiveOldestAgeSeconds,
		)
	}
	for _, metric := range snapshot.CacheStates {
		output <- prometheus.MustNewConstMetric(
			collector.cacheStates, prometheus.GaugeValue, metric.Count, metric.StageKind, metric.State,
		)
	}
	for _, metric := range snapshot.ResidencyStates {
		output <- prometheus.MustNewConstMetric(
			collector.residencyStates, prometheus.GaugeValue, metric.Count, metric.StageKind, metric.State,
		)
	}
}
